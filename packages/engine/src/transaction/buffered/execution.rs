use crate::contracts::{PendingCommitState, PlanEffects};
use crate::transaction::WriteExecutionContext;
use crate::{LixBackendTransaction, LixError};

use super::{
    direct_apply::execute_direct_transaction_write_unit_with_transaction,
    tracked_apply::execute_public_tracked_transaction_write_unit_with_transaction,
    untracked_apply::execute_public_untracked_transaction_write_unit_with_transaction,
    TransactionWriteDelta, TransactionWriteUnit,
};
use crate::transaction::pipeline::{empty_public_write_execution_outcome, WriteExecutionOutcome};

impl TransactionWriteDelta {
    pub(crate) async fn execute(
        &self,
        execution_context: &dyn WriteExecutionContext,
        transaction: &mut dyn LixBackendTransaction,
        mut pending_commit_state: Option<&mut Option<PendingCommitState>>,
    ) -> Result<WriteExecutionOutcome, LixError> {
        let mut combined_write_outcome = None;

        for unit in &self.materialization_plan().units {
            let write_outcome = match unit {
                TransactionWriteUnit::PublicTracked(tracked) => {
                    execute_public_tracked_transaction_write_unit_with_transaction(
                        execution_context,
                        transaction,
                        tracked,
                        pending_commit_state.as_deref_mut(),
                    )
                    .await?
                }
                TransactionWriteUnit::PublicUntracked(untracked) => {
                    execute_public_untracked_transaction_write_unit_with_transaction(
                        execution_context,
                        transaction,
                        untracked,
                    )
                    .await?
                }
                TransactionWriteUnit::Direct(direct) => {
                    execute_direct_transaction_write_unit_with_transaction(
                        execution_context,
                        transaction,
                        direct,
                    )
                    .await?
                }
            };

            if let Some(write_outcome) = write_outcome {
                merge_write_execution_outcome(&mut combined_write_outcome, write_outcome);
            }
        }

        Ok(combined_write_outcome.unwrap_or_else(empty_public_write_execution_outcome))
    }
}

fn merge_write_execution_outcome(
    combined_write_outcome: &mut Option<WriteExecutionOutcome>,
    write_outcome: WriteExecutionOutcome,
) {
    let Some(existing_write_outcome) = combined_write_outcome.as_mut() else {
        *combined_write_outcome = Some(write_outcome);
        return;
    };

    existing_write_outcome
        .direct_write_file_cache_targets
        .extend(write_outcome.direct_write_file_cache_targets);
    existing_write_outcome.plugin_changes_committed |= write_outcome.plugin_changes_committed;
    existing_write_outcome
        .state_commit_stream_changes
        .extend(write_outcome.state_commit_stream_changes);
    existing_write_outcome.observe_tick_emitted |= write_outcome.observe_tick_emitted;
    merge_plan_effects_override(
        &mut existing_write_outcome.plan_effects_override,
        write_outcome.plan_effects_override,
    );
}

fn merge_plan_effects_override(existing: &mut Option<PlanEffects>, next: Option<PlanEffects>) {
    match (existing, next) {
        (_, None) => {}
        (slot @ None, Some(next)) => {
            *slot = Some(next);
        }
        (Some(current), Some(next)) => {
            current
                .state_commit_stream_changes
                .extend(next.state_commit_stream_changes);
            current
                .file_cache_refresh_targets
                .extend(next.file_cache_refresh_targets);
            current.session_delta.merge(next.session_delta);
        }
    }
}
