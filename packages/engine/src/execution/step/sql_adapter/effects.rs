use crate::catalog::{
    builtin_catalog_compiler_facade, CatalogCompilerApi, CatalogWriteTargetKind,
    FilesystemRelationKind, ResolvedRelation,
};
use crate::contracts::should_invalidate_deterministic_settings_cache;
use crate::contracts::{
    PlannedFilesystemState, PreparedPublicWriteExecutionPartition, PreparedWriteStatement,
};
use crate::execution::step::BufferedWriteExecutionInput;
use crate::transaction::{
    binary_blob_writes_from_filesystem_state,
    compile_filesystem_finalization_from_state_in_transaction,
    filesystem_transaction_state_from_planned, merge_filesystem_transaction_state,
    persist_filesystem_payload_changes_in_transaction, BufferedWriteCommandMetadata,
    BufferedWriteExecutionResult, BufferedWriteFlushClass, BufferedWriteSessionEffects,
    DeferredCommitEffects, TransactionCommitOutcome, WriteCommand, WriteExecutionContext,
    WritePath,
};
use crate::{LixBackendTransaction, LixError};

use super::runtime::WriteExecutionOutcome;

pub(crate) fn command_metadata(
    step: &WriteCommand,
) -> Result<BufferedWriteCommandMetadata, LixError> {
    let flush_class = match step.path() {
        WritePath::ExplainOnly => BufferedWriteFlushClass::NoPreFlush,
        WritePath::DirectWrite(_) => BufferedWriteFlushClass::DirectWrite,
        WritePath::CommittedRead(_) => BufferedWriteFlushClass::CommittedRead,
        WritePath::PendingRead(_) | WritePath::BufferedDelta(_) | WritePath::NoopWrite => {
            BufferedWriteFlushClass::NoPreFlush
        }
    };

    Ok(BufferedWriteCommandMetadata {
        flush_class,
        has_materialization_plan: step.has_materialization_plan(),
        transaction_write_delta: step
            .is_bufferable_write()
            .then(|| step.transaction_write_delta().cloned())
            .flatten(),
        registry_mutated_during_planning: !step.prepared().public_surface_registry_effect.is_none(),
    })
}

pub(crate) async fn complete_sql_command_execution(
    execution_context: &dyn WriteExecutionContext,
    transaction: &mut dyn LixBackendTransaction,
    step: &WriteCommand,
    write_outcome: WriteExecutionOutcome,
    execution_input: &BufferedWriteExecutionInput,
    deferred_commit_effects: Option<&mut DeferredCommitEffects>,
    skip_side_effect_collection: bool,
) -> Result<BufferedWriteExecutionResult, LixError> {
    let mut commit_outcome = TransactionCommitOutcome::default();
    let clear_pending_commit_state = write_outcome.plan_effects_override.is_none()
        && !matches!(
            step.statement_kind(),
            crate::contracts::PreparedWriteStatementKind::Query
                | crate::contracts::PreparedWriteStatementKind::Explain
        );

    let default_effects = step
        .prepared()
        .direct_write()
        .map(|internal| internal.effects.clone())
        .unwrap_or_default();
    let active_effects = write_outcome
        .plan_effects_override
        .as_ref()
        .unwrap_or(&default_effects);
    let session_effects = BufferedWriteSessionEffects {
        session_delta: active_effects.session_delta.clone(),
        public_surface_registry_effect: step.prepared().public_surface_registry_effect.clone(),
    };
    commit_outcome.refresh_public_surface_registry =
        !session_effects.public_surface_registry_effect.is_none();

    let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
    state_commit_stream_changes.extend(write_outcome.state_commit_stream_changes.clone());
    commit_outcome.invalidate_deterministic_settings_cache =
        should_invalidate_deterministic_settings_cache(
            step.prepared()
                .direct_write()
                .map(|internal| internal.mutations.as_slice())
                .unwrap_or(&[]),
            &state_commit_stream_changes,
        );
    commit_outcome.invalidate_installed_plugins_cache = write_outcome.plugin_changes_committed;
    commit_outcome
        .state_commit_stream_changes
        .extend(state_commit_stream_changes);

    let write_handled_by_planned_write = step.transaction_write_delta().is_some();

    if write_handled_by_planned_write {
    } else if skip_side_effect_collection && deferred_commit_effects.is_none() {
    } else if let Some(deferred) = deferred_commit_effects {
        let filesystem_state =
            filesystem_transaction_state_from_planned(&prepared_filesystem_state(step.prepared()));
        merge_filesystem_transaction_state(&mut deferred.filesystem_state, &filesystem_state);
    } else {
        let filesystem_payload_changes_already_committed =
            public_write_filesystem_payload_changes_already_committed(step.prepared());
        let filesystem_state =
            filesystem_transaction_state_from_planned(&prepared_filesystem_state(step.prepared()));
        let binary_blob_writes = binary_blob_writes_from_filesystem_state(&filesystem_state);
        if !filesystem_payload_changes_already_committed {
            execution_context
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
                compile_filesystem_finalization_from_state_in_transaction(
                    transaction,
                    &filesystem_state,
                    execution_input.writer_key(),
                    step.prepared()
                        .direct_write()
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
            persist_filesystem_payload_changes_in_transaction(
                transaction,
                &filesystem_finalization.payload_changes(),
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
            execution_context
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
        execution_context
            .persist_runtime_sequence_in_transaction(
                transaction,
                step.function_bindings().provider(),
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
        public_result: write_outcome.public_result,
        clear_pending_commit_state,
        session_effects,
        commit_outcome,
    })
}

fn prepared_filesystem_state(prepared: &PreparedWriteStatement) -> PlannedFilesystemState {
    if let Some(public_write) = prepared.public_write() {
        public_write
            .contract
            .resolved_write_plan
            .as_ref()
            .map(|resolved| resolved.filesystem_state())
            .unwrap_or_default()
    } else if let Some(internal) = prepared.direct_write() {
        internal.filesystem_state.clone()
    } else {
        PlannedFilesystemState::default()
    }
}

fn public_write_filesystem_payload_changes_already_committed(
    prepared: &PreparedWriteStatement,
) -> bool {
    let Some(public_write) = prepared.public_write() else {
        return false;
    };
    is_catalog_filesystem_file_surface(&public_write.contract.target)
        && public_write.materialization().is_some_and(|execution| {
            execution.partitions.iter().any(|partition| {
                matches!(partition, PreparedPublicWriteExecutionPartition::Tracked(_))
            })
        })
}

fn is_catalog_filesystem_file_surface(target: &ResolvedRelation) -> bool {
    builtin_catalog_compiler_facade()
        .write_surface_semantics(target)
        .ok()
        .flatten()
        .is_some_and(|semantics| {
            semantics.target_kind == CatalogWriteTargetKind::Filesystem
                && semantics.filesystem_kind == Some(FilesystemRelationKind::File)
        })
}
