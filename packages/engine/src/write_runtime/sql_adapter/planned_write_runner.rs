use crate::contracts::artifacts::PlanEffects;
use crate::engine::Engine;
use crate::write_runtime::commit::PendingPublicCommitSession;
use crate::{LixBackendTransaction, LixError};

use super::internal_apply::run_internal_write_txn_with_transaction;
use super::planned_write::{PlannedWriteDelta, PlannedWriteUnit};
use super::runtime::{empty_public_write_execution_outcome, SqlExecutionOutcome};
use super::tracked_apply::run_public_tracked_append_txn_with_transaction;
use super::untracked_apply::run_public_untracked_write_txn_with_transaction;

pub(crate) async fn execute_planned_write_delta(
    engine: &Engine,
    transaction: &mut dyn LixBackendTransaction,
    delta: &PlannedWriteDelta,
    mut pending_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
) -> Result<SqlExecutionOutcome, LixError> {
    let mut combined = None;

    for unit in &delta.materialization_plan().units {
        let outcome = match unit {
            PlannedWriteUnit::PublicTracked(tracked) => {
                run_public_tracked_append_txn_with_transaction(
                    transaction,
                    tracked,
                    pending_commit_session.as_deref_mut(),
                )
                .await?
            }
            PlannedWriteUnit::PublicUntracked(untracked) => {
                run_public_untracked_write_txn_with_transaction(engine, transaction, untracked)
                    .await?
            }
            PlannedWriteUnit::Internal(internal) => {
                run_internal_write_txn_with_transaction(engine, transaction, internal).await?
            }
            PlannedWriteUnit::WorkspaceWriterKey(workspace_writer_key) => {
                let mut backend = crate::runtime::TransactionBackendAdapter::new(transaction);
                crate::annotations::writer_key::apply_workspace_writer_key_annotations_with_executor(
                    &mut backend,
                    &workspace_writer_key.annotations,
                )
                .await?;
                None
            }
        };

        if let Some(outcome) = outcome {
            merge_sql_execution_outcome(&mut combined, outcome);
        }
    }

    Ok(combined.unwrap_or_else(empty_public_write_execution_outcome))
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
