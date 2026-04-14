use crate::transaction::pipeline::{
    execute_direct_execution_with_transaction, WriteExecutionOutcome,
};
use crate::transaction::FilesystemPayloadChange;
use crate::transaction::{
    build_filesystem_payload_changes_insert,
    compile_filesystem_finalization_from_state_in_transaction,
    filesystem_transaction_state_from_planned, PlannedDirectWriteUnit, WriteExecutionContext,
};
use crate::{LixBackendTransaction, LixError};

use super::registered_schema_mirror::mirror_registered_schema_mutations_in_transaction;

pub(crate) async fn execute_direct_transaction_write_unit_with_transaction(
    execution_context: &dyn WriteExecutionContext,
    transaction: &mut dyn LixBackendTransaction,
    plan: &PlannedDirectWriteUnit,
) -> Result<Option<WriteExecutionOutcome>, LixError> {
    let mut write_outcome = execute_direct_execution_with_transaction(
        transaction,
        &plan.execution,
        plan.result_contract,
        plan.function_bindings.provider(),
        plan.execution.writer_key.as_deref(),
    )
    .await
    .map_err(LixError::from)?;
    mirror_registered_schema_mutations_in_transaction(transaction, &plan.execution.mutations)
        .await?;

    let filesystem_state =
        filesystem_transaction_state_from_planned(&plan.execution.filesystem_state);
    let filesystem_finalization = compile_filesystem_finalization_from_state_in_transaction(
        transaction,
        &filesystem_state,
        plan.execution.writer_key.as_deref(),
        &plan.execution.mutations,
    )
    .await?;
    if !filesystem_finalization.binary_blob_writes.is_empty() {
        execution_context
            .persist_binary_blob_writes_in_transaction(
                transaction,
                &filesystem_finalization.binary_blob_writes,
            )
            .await?;
    }
    persist_filesystem_payload_changes_direct(
        transaction,
        &filesystem_finalization.payload_changes(),
    )
    .await?;
    if filesystem_finalization.should_run_gc {
        execution_context
            .garbage_collect_unreachable_binary_cas_in_transaction(transaction)
            .await?;
    }

    execution_context
        .persist_runtime_sequence_in_transaction(transaction, plan.function_bindings.provider())
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "direct write runtime-sequence persistence failed inside write txn: {}",
                error.description
            ),
        })?;

    if write_outcome.plan_effects_override.is_none() {
        write_outcome.plan_effects_override = Some(plan.execution.effects.clone());
    }

    Ok(Some(write_outcome))
}

async fn persist_filesystem_payload_changes_direct(
    transaction: &mut dyn LixBackendTransaction,
    changes: &[FilesystemPayloadChange],
) -> Result<(), LixError> {
    let tracked = changes
        .iter()
        .filter(|change| !change.untracked)
        .cloned()
        .collect::<Vec<_>>();
    if !tracked.is_empty() {
        let (sql, params) = build_filesystem_payload_changes_insert(&tracked, false);
        transaction.execute(&sql, &params).await?;
    }

    let untracked = changes
        .iter()
        .filter(|change| change.untracked)
        .cloned()
        .collect::<Vec<_>>();
    if !untracked.is_empty() {
        let (sql, params) = build_filesystem_payload_changes_insert(&untracked, true);
        transaction.execute(&sql, &params).await?;
    }

    Ok(())
}
