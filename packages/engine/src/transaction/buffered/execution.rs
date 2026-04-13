use crate::contracts::{PendingPublicCommitSession, PlanEffects};
use crate::execution::step::{
    empty_public_write_execution_outcome, execute_internal_transaction_write_unit_with_transaction,
    execute_public_tracked_transaction_write_unit_with_transaction,
    execute_public_untracked_transaction_write_unit_with_transaction, SqlExecutionOutcome,
};
use crate::transaction::WriteExecutionBindings;
use crate::{LixBackendTransaction, LixError};

use super::{TransactionWriteDelta, TransactionWriteUnit};

impl TransactionWriteDelta {
    pub(crate) async fn execute(
        &self,
        bindings: &dyn WriteExecutionBindings,
        transaction: &mut dyn LixBackendTransaction,
        mut pending_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
    ) -> Result<SqlExecutionOutcome, LixError> {
        let mut combined = None;

        for unit in &self.materialization_plan().units {
            let outcome = match unit {
                TransactionWriteUnit::PublicTracked(tracked) => {
                    execute_public_tracked_transaction_write_unit_with_transaction(
                        bindings,
                        transaction,
                        tracked,
                        pending_commit_session.as_deref_mut(),
                    )
                    .await?
                }
                TransactionWriteUnit::PublicUntracked(untracked) => {
                    execute_public_untracked_transaction_write_unit_with_transaction(
                        bindings,
                        transaction,
                        untracked,
                    )
                    .await?
                }
                TransactionWriteUnit::Internal(internal) => {
                    execute_internal_transaction_write_unit_with_transaction(
                        bindings,
                        transaction,
                        internal,
                    )
                    .await?
                }
            };

            if let Some(outcome) = outcome {
                merge_sql_execution_outcome(&mut combined, outcome);
            }
        }

        Ok(combined.unwrap_or_else(empty_public_write_execution_outcome))
    }
}

fn merge_sql_execution_outcome(
    combined: &mut Option<SqlExecutionOutcome>,
    outcome: SqlExecutionOutcome,
) {
    let Some(existing) = combined.as_mut() else {
        *combined = Some(outcome);
        return;
    };

    existing
        .internal_write_file_cache_targets
        .extend(outcome.internal_write_file_cache_targets);
    existing.plugin_changes_committed |= outcome.plugin_changes_committed;
    existing
        .state_commit_stream_changes
        .extend(outcome.state_commit_stream_changes);
    existing.observe_tick_emitted |= outcome.observe_tick_emitted;
    merge_plan_effects_override(
        &mut existing.plan_effects_override,
        outcome.plan_effects_override,
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
