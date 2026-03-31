use crate::engine::{DeferredTransactionSideEffects, Engine, TransactionBackendAdapter};
use crate::filesystem::runtime::merge_filesystem_transaction_state;
use crate::live_state::{
    bootstrap_public_surface_registry_with_pending_transaction_view, public_read_execution_mode,
};
use crate::read::contracts::PublicReadExecutionMode;
use crate::sql::catalog::SurfaceRegistry;
use crate::sql::executor::execution_program::ExecutionContext;
use crate::sql::executor::{
    apply_public_surface_registry_mutations, prepared_execution_mutates_public_surface_registry,
    public_surface_registry_mutations, CompiledExecution, PreparedPublicWrite,
};
use crate::sql::physical_plan::PublicWriteExecutionPartition;
use crate::transaction::PendingTransactionView;
use crate::{LixBackendTransaction, LixError};

use super::compile::SqlBufferedWriteCommand;
use super::{CompiledExecutionRoute, SqlExecutionOutcome};
use crate::transaction::commands::{
    BufferedWriteCommandMetadata, BufferedWriteExecutionResult, BufferedWriteExecutionRoute,
};
use crate::transaction::contracts::TransactionCommitOutcome;
use crate::version::GLOBAL_VERSION_ID;

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const REGISTERED_SCHEMA_BOOTSTRAP_TABLE: &str = "lix_internal_registered_schema_bootstrap";

pub(super) fn command_metadata(
    command: &SqlBufferedWriteCommand,
) -> Result<BufferedWriteCommandMetadata, LixError> {
    let route = match command.compiled.route() {
        CompiledExecutionRoute::Explain(_) => BufferedWriteExecutionRoute::Other,
        CompiledExecutionRoute::Internal(_) => BufferedWriteExecutionRoute::Internal,
        CompiledExecutionRoute::PublicRead(public_read)
            if matches!(
                public_read_execution_mode(public_read),
                PublicReadExecutionMode::Committed(_)
            ) =>
        {
            BufferedWriteExecutionRoute::PublicReadCommitted
        }
        CompiledExecutionRoute::PublicRead(_)
        | CompiledExecutionRoute::PlannedWriteDelta(_)
        | CompiledExecutionRoute::PublicWriteNoop => BufferedWriteExecutionRoute::Other,
    };

    Ok(BufferedWriteCommandMetadata {
        route,
        has_materialization_plan: command.compiled.has_materialization_plan(),
        planned_write_delta: command
            .compiled
            .is_bufferable_write(&command.statement)
            .then(|| command.compiled.planned_write_delta().cloned())
            .flatten(),
        registry_mutated_during_planning: command.registry_mutated_during_planning,
    })
}

pub(super) fn apply_buffered_write_planning_effects(
    command: &SqlBufferedWriteCommand,
    context: &mut ExecutionContext,
) -> Result<(), LixError> {
    apply_execution_planning_effects(command.compiled.execution(), context)
}

pub(super) async fn refresh_public_surface_registry_from_pending_transaction_view(
    transaction: &mut dyn LixBackendTransaction,
    pending_transaction_view: Option<&PendingTransactionView>,
    context: &mut ExecutionContext,
) -> Result<(), LixError> {
    let backend = TransactionBackendAdapter::new(transaction);
    context.public_surface_registry =
        bootstrap_public_surface_registry_with_pending_transaction_view(
            &backend,
            pending_transaction_view,
        )
        .await?;
    context.bump_public_surface_registry_generation();
    Ok(())
}

pub(crate) async fn mirror_public_registered_schema_bootstrap_rows(
    transaction: &mut dyn LixBackendTransaction,
    applied_output: &crate::commit::CreateCommitAppliedOutput,
) -> Result<(), LixError> {
    for row in &applied_output.canonical_output.changes {
        if row.schema_key != REGISTERED_SCHEMA_KEY {
            continue;
        }

        let snapshot_sql = row
            .snapshot_content
            .as_ref()
            .map(|value| format!("'{}'", crate::sql::common::text::escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let metadata_sql = row
            .metadata
            .as_ref()
            .map(|value| format!("'{}'", crate::sql::common::text::escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let writer_key_sql = "NULL".to_string();
        let is_tombstone = if row.snapshot_content.is_some() { 0 } else { 1 };

        let sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, snapshot_content, change_id, metadata, writer_key, is_tombstone, created_at, updated_at\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', true, '{plugin_key}', {snapshot_content}, '{change_id}', {metadata}, {writer_key}, {is_tombstone}, '{created_at}', '{updated_at}'\
             ) ON CONFLICT (entity_id, file_id, version_id, untracked) DO UPDATE SET \
             schema_key = excluded.schema_key, \
             schema_version = excluded.schema_version, \
             global = excluded.global, \
             plugin_key = excluded.plugin_key, \
             snapshot_content = excluded.snapshot_content, \
             change_id = excluded.change_id, \
             metadata = excluded.metadata, \
             writer_key = excluded.writer_key, \
             is_tombstone = excluded.is_tombstone, \
            updated_at = excluded.updated_at",
            table = REGISTERED_SCHEMA_BOOTSTRAP_TABLE,
            entity_id = crate::sql::common::text::escape_sql_string(&row.entity_id),
            schema_key = crate::sql::common::text::escape_sql_string(&row.schema_key),
            schema_version = crate::sql::common::text::escape_sql_string(&row.schema_version),
            file_id = crate::sql::common::text::escape_sql_string(&row.file_id),
            version_id = crate::sql::common::text::escape_sql_string(GLOBAL_VERSION_ID),
            plugin_key = crate::sql::common::text::escape_sql_string(&row.plugin_key),
            snapshot_content = snapshot_sql,
            change_id = crate::sql::common::text::escape_sql_string(&row.id),
            metadata = metadata_sql,
            writer_key = writer_key_sql,
            is_tombstone = is_tombstone,
            created_at = crate::sql::common::text::escape_sql_string(&row.created_at),
            updated_at = crate::sql::common::text::escape_sql_string(&row.created_at),
        );

        transaction.execute(&sql, &[]).await?;
    }

    Ok(())
}

pub(super) async fn complete_sql_command_execution(
    engine: &Engine,
    transaction: &mut dyn LixBackendTransaction,
    command: &SqlBufferedWriteCommand,
    execution: SqlExecutionOutcome,
    context: &mut ExecutionContext,
    deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
    skip_side_effect_collection: bool,
) -> Result<BufferedWriteExecutionResult, LixError> {
    let mut commit_outcome = TransactionCommitOutcome::default();
    let clear_pending_public_commit_session = execution.plan_effects_override.is_none()
        && !matches!(
            command.statement,
            sqlparser::ast::Statement::Query(_) | sqlparser::ast::Statement::Explain { .. }
        );

    if let Some(public_write) = command.compiled.execution().public_write() {
        let mut mutations = public_surface_registry_mutations(public_write)?;
        if apply_public_surface_registry_mutations(
            &mut context.public_surface_registry,
            &mut mutations,
        )? {
            context.bump_public_surface_registry_generation();
            commit_outcome.refresh_public_surface_registry = true;
        }
    } else if prepared_execution_mutates_public_surface_registry(command.compiled.execution())? {
        let backend = TransactionBackendAdapter::new(transaction);
        context.public_surface_registry = SurfaceRegistry::bootstrap_with_backend(&backend).await?;
        context.bump_public_surface_registry_generation();
        commit_outcome.refresh_public_surface_registry = true;
    }

    let active_effects = execution
        .plan_effects_override
        .as_ref()
        .unwrap_or(&command.compiled.execution().effects);

    if let Some(version_id) = &active_effects.session_delta.next_active_version_id {
        context.active_version_id = version_id.clone();
    }
    if let Some(active_account_ids) = &active_effects.session_delta.next_active_account_ids {
        context.active_account_ids = active_account_ids.clone();
    }

    let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
    state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
    commit_outcome.invalidate_deterministic_settings_cache = engine
        .should_invalidate_deterministic_settings_cache(
            command
                .compiled
                .execution()
                .internal_execution()
                .map(|internal| internal.mutations.as_slice())
                .unwrap_or(&[]),
            &state_commit_stream_changes,
        );
    commit_outcome.invalidate_installed_plugins_cache = execution.plugin_changes_committed;
    commit_outcome
        .state_commit_stream_changes
        .extend(state_commit_stream_changes);

    let write_handled_by_planned_write = command.compiled.planned_write_delta().is_some();

    if write_handled_by_planned_write {
    } else if skip_side_effect_collection && deferred_side_effects.is_none() {
    } else if let Some(deferred) = deferred_side_effects {
        merge_filesystem_transaction_state(
            &mut deferred.filesystem_state,
            &command.compiled.execution().intent.filesystem_state,
        );
    } else {
        let filesystem_payload_changes_already_committed =
            public_write_filesystem_payload_changes_already_committed(command.compiled.execution());
        let binary_blob_writes =
            crate::filesystem::runtime::binary_blob_writes_from_filesystem_state(
                &command.compiled.execution().intent.filesystem_state,
            );
        if !filesystem_payload_changes_already_committed {
            engine
                .persist_binary_blob_writes_in_transaction(transaction, &binary_blob_writes)
                .await
                .map_err(|error| LixError {
                    code: error.code,
                    description: format!(
                        "transaction pending filesystem payload persistence failed: {}",
                        error.description
                    ),
                })?;
        }
        let filesystem_finalization = if filesystem_payload_changes_already_committed {
            None
        } else {
            Some(
                engine
                    .compile_filesystem_finalization_from_state_in_transaction(
                        transaction,
                        &command.compiled.execution().intent.filesystem_state,
                        context.options.writer_key.as_deref(),
                        command
                            .compiled
                            .execution()
                            .internal_execution()
                            .map(|internal| internal.mutations.as_slice())
                            .unwrap_or(&[]),
                    )
                    .await
                    .map_err(|error| LixError {
                        code: error.code,
                        description: format!(
                            "transaction filesystem finalization compilation failed: {}",
                            error.description
                        ),
                    })?,
            )
        };
        if let Some(filesystem_finalization) = filesystem_finalization.as_ref() {
            engine
                .persist_filesystem_payload_domain_changes_in_transaction(
                    transaction,
                    &filesystem_finalization.payload_domain_changes(),
                )
                .await
                .map_err(|error| LixError {
                    code: error.code,
                    description: format!(
                        "transaction tracked filesystem side-effect persistence failed: {}",
                        error.description
                    ),
                })?;
        }
        if filesystem_finalization
            .as_ref()
            .is_some_and(|compiled| compiled.should_run_gc)
        {
            engine
                .garbage_collect_unreachable_binary_cas_in_transaction(transaction)
                .await
                .map_err(|error| LixError {
                    code: error.code,
                    description: format!(
                        "transaction binary CAS garbage collection failed: {}",
                        error.description
                    ),
                })?;
        }
    }

    if !write_handled_by_planned_write {
        engine
            .persist_runtime_sequence_in_transaction(
                transaction,
                command.compiled.execution().runtime_state.settings(),
                command.compiled.execution().runtime_state.provider(),
            )
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "transaction runtime-sequence persistence failed: {}",
                    error.description
                ),
            })?;
    }

    Ok(BufferedWriteExecutionResult {
        public_result: execution.public_result,
        clear_pending_public_commit_session,
        commit_outcome,
    })
}

fn public_write_filesystem_payload_changes_already_committed(prepared: &CompiledExecution) -> bool {
    let Some(public_write) = prepared.public_write() else {
        return false;
    };
    matches!(
        public_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    ) && public_write.materialization().is_some_and(|execution| {
        execution
            .partitions
            .iter()
            .any(|partition| matches!(partition, PublicWriteExecutionPartition::Tracked(_)))
    })
}

fn apply_execution_planning_effects(
    execution: &CompiledExecution,
    context: &mut ExecutionContext,
) -> Result<(), LixError> {
    if let Some(public_write) = execution.public_write() {
        let mut mutations = public_surface_registry_mutations(public_write)?;
        if apply_public_surface_registry_mutations(
            &mut context.public_surface_registry,
            &mut mutations,
        )? {
            context.bump_public_surface_registry_generation();
        }
        if let Some(next_active_version_id) =
            public_write_execution_next_active_version_id(public_write)
        {
            context.active_version_id = next_active_version_id;
        }
    } else if let Some(version_id) = &execution.effects.session_delta.next_active_version_id {
        context.active_version_id = version_id.clone();
    }
    Ok(())
}

fn public_write_execution_next_active_version_id(
    public_write: &PreparedPublicWrite,
) -> Option<String> {
    public_write.materialization().and_then(|execution| {
        execution
            .partitions
            .iter()
            .rev()
            .find_map(|partition| match partition {
                PublicWriteExecutionPartition::Tracked(tracked) => tracked
                    .semantic_effects
                    .session_delta
                    .next_active_version_id
                    .clone(),
                PublicWriteExecutionPartition::Untracked(untracked) => untracked
                    .semantic_effects
                    .session_delta
                    .next_active_version_id
                    .clone(),
            })
    })
}
