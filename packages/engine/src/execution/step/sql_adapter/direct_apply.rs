use crate::contracts::FilesystemPayloadChange;
use crate::transaction::{
    build_filesystem_payload_changes_insert,
    compile_filesystem_finalization_from_state_in_transaction,
    filesystem_transaction_state_from_planned, PlannedDirectWriteUnit, WriteExecutionHost,
};
use crate::{LixBackendTransaction, LixError};

use super::runtime::{execute_direct_execution_with_transaction, WriteExecutionOutcome};

pub(crate) async fn run_direct_write_txn_with_transaction(
    host: &dyn WriteExecutionHost,
    transaction: &mut dyn LixBackendTransaction,
    plan: &PlannedDirectWriteUnit,
) -> Result<Option<WriteExecutionOutcome>, LixError> {
    let mut write_outcome = execute_direct_execution_with_transaction(
        transaction,
        &plan.execution,
        plan.result_contract,
        plan.runtime_state.functions(),
        plan.execution.writer_key.as_deref(),
    )
    .await
    .map_err(LixError::from)?;

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
        host.persist_binary_blob_writes_in_transaction(
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
        host.garbage_collect_unreachable_binary_cas_in_transaction(transaction)
            .await?;
    }

    host.persist_runtime_sequence_in_transaction(transaction, plan.runtime_state.functions())
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
