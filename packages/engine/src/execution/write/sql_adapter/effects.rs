use crate::contracts::artifacts::{
    PlannedFilesystemState, PreparedPublicWriteExecutionPartition, PreparedWriteStep,
    PublicReadExecutionMode,
};
use crate::contracts::state_commit_stream::should_invalidate_deterministic_settings_cache;
use crate::execution::write::buffered::{
    BufferedWriteCommandMetadata, BufferedWriteExecutionResult, BufferedWriteExecutionRoute,
    BufferedWriteSessionEffects,
};
use crate::execution::write::filesystem::runtime::{
    compile_filesystem_finalization_from_state_in_transaction, merge_filesystem_transaction_state,
    persist_filesystem_payload_domain_changes_in_transaction,
};
use crate::execution::write::filesystem::state::filesystem_transaction_state_from_planned;
use crate::execution::write::{
    BufferedWriteExecutionInput, DeferredTransactionSideEffects, TransactionCommitOutcome,
    WriteExecutionBindings,
};
use crate::{LixBackendTransaction, LixError};

use super::runtime::{
    PreparedWriteExecutionRoute, PreparedWriteExecutionStep, SqlExecutionOutcome,
};

pub(crate) fn command_metadata(
    step: &PreparedWriteExecutionStep,
) -> Result<BufferedWriteCommandMetadata, LixError> {
    let route = match step.route() {
        PreparedWriteExecutionRoute::Explain => BufferedWriteExecutionRoute::Other,
        PreparedWriteExecutionRoute::Internal(_) => BufferedWriteExecutionRoute::Internal,
        PreparedWriteExecutionRoute::PublicRead(public_read)
            if matches!(
                public_read.contract.execution_mode(),
                PublicReadExecutionMode::Committed(_)
            ) =>
        {
            BufferedWriteExecutionRoute::PublicReadCommitted
        }
        PreparedWriteExecutionRoute::PublicRead(_)
        | PreparedWriteExecutionRoute::PlannedWriteDelta(_)
        | PreparedWriteExecutionRoute::PublicWriteNoop => BufferedWriteExecutionRoute::Other,
    };

    Ok(BufferedWriteCommandMetadata {
        route,
        has_materialization_plan: step.has_materialization_plan(),
        planned_write_delta: step
            .is_bufferable_write()
            .then(|| step.planned_write_delta().cloned())
            .flatten(),
        registry_mutated_during_planning: !step.prepared().public_surface_registry_effect.is_none(),
    })
}

pub(crate) async fn complete_sql_command_execution(
    bindings: &dyn WriteExecutionBindings,
    transaction: &mut dyn LixBackendTransaction,
    step: &PreparedWriteExecutionStep,
    execution: SqlExecutionOutcome,
    execution_input: &BufferedWriteExecutionInput,
    deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
    skip_side_effect_collection: bool,
) -> Result<BufferedWriteExecutionResult, LixError> {
    let mut commit_outcome = TransactionCommitOutcome::default();
    let clear_pending_public_commit_session = execution.plan_effects_override.is_none()
        && !matches!(
            step.statement_kind(),
            crate::contracts::artifacts::PreparedWriteStatementKind::Query
                | crate::contracts::artifacts::PreparedWriteStatementKind::Explain
        );

    let default_effects = step
        .prepared()
        .internal_write()
        .map(|internal| internal.effects.clone())
        .unwrap_or_default();
    let active_effects = execution
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
    state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
    commit_outcome.invalidate_deterministic_settings_cache =
        should_invalidate_deterministic_settings_cache(
            step.prepared()
                .internal_write()
                .map(|internal| internal.mutations.as_slice())
                .unwrap_or(&[]),
            &state_commit_stream_changes,
        );
    commit_outcome.invalidate_installed_plugins_cache = execution.plugin_changes_committed;
    commit_outcome
        .state_commit_stream_changes
        .extend(state_commit_stream_changes);

    let write_handled_by_planned_write = step.planned_write_delta().is_some();

    if write_handled_by_planned_write {
    } else if skip_side_effect_collection && deferred_side_effects.is_none() {
    } else if let Some(deferred) = deferred_side_effects {
        let filesystem_state =
            filesystem_transaction_state_from_planned(&prepared_filesystem_state(step.prepared()));
        merge_filesystem_transaction_state(&mut deferred.filesystem_state, &filesystem_state);
    } else {
        let filesystem_payload_changes_already_committed =
            public_write_filesystem_payload_changes_already_committed(step.prepared());
        let filesystem_state =
            filesystem_transaction_state_from_planned(&prepared_filesystem_state(step.prepared()));
        let binary_blob_writes =
            crate::execution::write::filesystem::runtime::binary_blob_writes_from_filesystem_state(
                &filesystem_state,
            );
        if !filesystem_payload_changes_already_committed {
            bindings
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
                        .internal_write()
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
            persist_filesystem_payload_domain_changes_in_transaction(
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
            bindings
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
        bindings
            .persist_runtime_sequence_in_transaction(transaction, step.runtime_state().functions())
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
        session_effects,
        commit_outcome,
    })
}

fn prepared_filesystem_state(prepared: &PreparedWriteStep) -> PlannedFilesystemState {
    if let Some(public_write) = prepared.public_write() {
        public_write
            .contract
            .resolved_write_plan
            .as_ref()
            .map(|resolved| resolved.filesystem_state())
            .unwrap_or_default()
    } else if let Some(internal) = prepared.internal_write() {
        internal.filesystem_state.clone()
    } else {
        PlannedFilesystemState::default()
    }
}

fn public_write_filesystem_payload_changes_already_committed(prepared: &PreparedWriteStep) -> bool {
    let Some(public_write) = prepared.public_write() else {
        return false;
    };
    matches!(
        public_write.contract.target.descriptor.public_name.as_str(),
        "lix_file" | "lix_file_by_version"
    ) && public_write.materialization().is_some_and(|execution| {
        execution
            .partitions
            .iter()
            .any(|partition| matches!(partition, PreparedPublicWriteExecutionPartition::Tracked(_)))
    })
}
