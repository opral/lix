use crate::contracts::artifacts::FilesystemPayloadDomainChange;
use crate::engine::Engine;
use crate::filesystem::runtime::build_filesystem_payload_domain_changes_insert;
use crate::{LixBackendTransaction, LixError};

use super::planned_write::PlannedInternalWriteUnit;
use super::runtime::{execute_internal_execution_with_transaction, SqlExecutionOutcome};

pub(super) async fn run_internal_write_txn_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixBackendTransaction,
    plan: &PlannedInternalWriteUnit,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    let mut execution = execute_internal_execution_with_transaction(
        transaction,
        &plan.execution,
        plan.result_contract,
        plan.runtime_state.provider(),
        plan.writer_key.as_deref(),
    )
    .await
    .map_err(LixError::from)?;

    let filesystem_finalization = engine
        .compile_filesystem_finalization_from_state_in_transaction(
            transaction,
            &plan.filesystem_state,
            plan.writer_key.as_deref(),
            &plan.execution.mutations,
        )
        .await?;
    if !filesystem_finalization.binary_blob_writes.is_empty() {
        engine
            .persist_binary_blob_writes_in_transaction(
                transaction,
                &filesystem_finalization.binary_blob_writes,
            )
            .await?;
    }
    persist_filesystem_payload_domain_changes_direct(
        transaction,
        &filesystem_finalization.payload_domain_changes(),
    )
    .await?;
    if filesystem_finalization.should_run_gc {
        engine
            .garbage_collect_unreachable_binary_cas_in_transaction(transaction)
            .await?;
    }

    engine
        .persist_runtime_sequence_in_transaction(
            transaction,
            plan.runtime_state.settings(),
            plan.runtime_state.provider(),
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "internal write runtime-sequence persistence failed inside write txn: {}",
                error.description
            ),
        })?;

    if execution.plan_effects_override.is_none() {
        execution.plan_effects_override = Some(plan.effects.clone());
    }

    Ok(Some(execution))
}

async fn persist_filesystem_payload_domain_changes_direct(
    transaction: &mut dyn LixBackendTransaction,
    changes: &[FilesystemPayloadDomainChange],
) -> Result<(), LixError> {
    let tracked = changes
        .iter()
        .filter(|change| !change.untracked)
        .cloned()
        .collect::<Vec<_>>();
    if !tracked.is_empty() {
        let (sql, params) = build_filesystem_payload_domain_changes_insert(&tracked, false);
        transaction.execute(&sql, &params).await?;
    }

    let untracked = changes
        .iter()
        .filter(|change| change.untracked)
        .cloned()
        .collect::<Vec<_>>();
    if !untracked.is_empty() {
        let (sql, params) = build_filesystem_payload_domain_changes_insert(&untracked, true);
        transaction.execute(&sql, &params).await?;
    }

    Ok(())
}
