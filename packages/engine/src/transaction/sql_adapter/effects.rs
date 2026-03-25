use crate::engine::{DeferredTransactionSideEffects, Engine, TransactionBackendAdapter};
use crate::sql::execution::execution_program::ExecutionContext;
use crate::sql::execution::runtime_effects::merge_filesystem_transaction_state;
use crate::sql::execution::shared_path::{
    self, prepared_execution_mutates_public_surface_registry, PendingTransactionView,
    PreparedPublicReadTransactionMode,
};
use crate::sql::public::catalog::SurfaceRegistry;
use crate::sql::public::runtime::{
    apply_public_surface_registry_mutations, public_surface_registry_mutations,
    PublicWriteExecutionPartition,
};
use crate::{LixBackendTransaction, LixError};

use super::compile::SqlBufferedWriteCommand;
use super::{CompiledExecution, CompiledExecutionRoute, SqlExecutionOutcome};
use crate::transaction::commands::{
    BufferedWriteCommandMetadata, BufferedWriteExecutionResult, BufferedWriteExecutionRoute,
};
use crate::transaction::contracts::TransactionCommitOutcome;

pub(super) fn command_metadata(
    command: &SqlBufferedWriteCommand,
) -> Result<BufferedWriteCommandMetadata, LixError> {
    let route = match command.compiled.route() {
        CompiledExecutionRoute::Internal(_) => BufferedWriteExecutionRoute::Internal,
        CompiledExecutionRoute::PublicRead(public_read)
            if matches!(
                shared_path::prepared_public_read_transaction_mode(public_read),
                PreparedPublicReadTransactionMode::MaterializedState
            ) =>
        {
            BufferedWriteExecutionRoute::PublicReadMaterializedState
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
    apply_execution_planning_effects(
        command.compiled.execution(),
        &mut context.public_surface_registry,
        &mut context.public_surface_registry_generation,
        &mut context.active_version_id,
    )
}

pub(super) async fn refresh_public_surface_registry_from_pending_transaction_view(
    transaction: &mut dyn LixBackendTransaction,
    pending_transaction_view: Option<&PendingTransactionView>,
    context: &mut ExecutionContext,
) -> Result<(), LixError> {
    let backend = TransactionBackendAdapter::new(transaction);
    context.public_surface_registry =
        shared_path::bootstrap_public_surface_registry_with_pending_transaction_view(
            &backend,
            pending_transaction_view,
        )
        .await?;
    context.bump_public_surface_registry_generation();
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

    if let Some(version_id) = &active_effects.next_active_version_id {
        context.active_version_id = version_id.clone();
    }

    let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
    state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
    commit_outcome.invalidate_deterministic_settings_cache =
        engine.should_invalidate_deterministic_settings_cache(
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
            shared_path::public_write_filesystem_payload_changes_already_committed(
                command.compiled.execution(),
            );
        let binary_blob_writes =
            crate::sql::execution::runtime_effects::binary_blob_writes_from_filesystem_state(
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
                command.compiled.execution().settings,
                command.compiled.execution().sequence_start,
                &command.compiled.execution().functions,
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

fn apply_execution_planning_effects(
    execution: &CompiledExecution,
    public_surface_registry: &mut SurfaceRegistry,
    public_surface_registry_generation: &mut u64,
    active_version_id: &mut String,
) -> Result<(), LixError> {
    if let Some(public_write) = execution.public_write() {
        let mut mutations = public_surface_registry_mutations(public_write)?;
        if apply_public_surface_registry_mutations(public_surface_registry, &mut mutations)? {
            *public_surface_registry_generation += 1;
        }
        if let Some(next_active_version_id) =
            public_write_execution_next_active_version_id(public_write)
        {
            *active_version_id = next_active_version_id;
        }
    } else if let Some(version_id) = &execution.effects.next_active_version_id {
        *active_version_id = version_id.clone();
    }
    Ok(())
}

fn public_write_execution_next_active_version_id(
    public_write: &crate::sql::public::runtime::PreparedPublicWrite,
) -> Option<String> {
    public_write.materialization().and_then(|execution| {
        execution
            .partitions
            .iter()
            .rev()
            .find_map(|partition| match partition {
                PublicWriteExecutionPartition::Tracked(tracked) => {
                    tracked.semantic_effects.next_active_version_id.clone()
                }
                PublicWriteExecutionPartition::Untracked(untracked) => {
                    untracked.semantic_effects.next_active_version_id.clone()
                }
            })
    })
}
