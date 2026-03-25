use crate::engine::Engine;
use crate::sql::execution::execution_program::{ExecutionContext, SqlExecutionOutcome};
use crate::sql::execution::shared_path::{PendingPublicCommitSession, PendingTransactionView};
use crate::LixBackendTransaction;
use crate::LixError;

use super::write_plan::{BufferedWriteJournal, PlannedWriteDelta};
use super::write_runner::execute_planned_write_delta;

#[derive(Default)]
pub(crate) struct BufferedWriteState {
    journal: BufferedWriteJournal,
    pending_public_commit_session: Option<PendingPublicCommitSession>,
}

impl BufferedWriteState {
    pub(crate) fn journal_is_empty(&self) -> bool {
        self.journal.is_empty()
    }

    pub(crate) fn pending_transaction_view(
        &self,
    ) -> Result<Option<PendingTransactionView>, LixError> {
        self.journal.pending_transaction_view()
    }

    pub(crate) fn can_stage_delta(&self, delta: &PlannedWriteDelta) -> Result<bool, LixError> {
        self.journal.can_stage_delta(delta)
    }

    pub(crate) fn stage_delta(&mut self, delta: PlannedWriteDelta) -> Result<(), LixError> {
        self.journal.stage_delta(delta)
    }

    pub(crate) fn clear_pending_public_commit_session(&mut self) {
        self.pending_public_commit_session = None;
    }

    pub(crate) fn pending_public_commit_session_mut(
        &mut self,
    ) -> &mut Option<PendingPublicCommitSession> {
        &mut self.pending_public_commit_session
    }

    pub(crate) async fn flush(
        &mut self,
        transaction: &mut dyn LixBackendTransaction,
        engine: &Engine,
        context: &mut ExecutionContext,
    ) -> Result<(), LixError> {
        let Some(delta) = self.journal.take_staged_delta() else {
            return Ok(());
        };
        let mut pending_public_commit_session = self.pending_public_commit_session.take();
        let execution = execute_planned_write_delta(
            engine,
            transaction,
            &delta,
            Some(&mut pending_public_commit_session),
        )
        .await?;
        self.pending_public_commit_session = pending_public_commit_session;
        apply_buffered_write_execution_outcome(engine, context, execution);
        Ok(())
    }

    pub(crate) async fn prepare_commit(
        &mut self,
        transaction: &mut dyn LixBackendTransaction,
        engine: &Engine,
        context: &mut ExecutionContext,
    ) -> Result<(), LixError> {
        let active_version_before_flush = context.active_version_id.clone();
        self.flush(transaction, engine, context).await?;
        if context.active_version_id != active_version_before_flush {
            context.active_version_changed = true;
        }
        if !context.observe_tick_already_emitted
            && !context.pending_state_commit_stream_changes.is_empty()
        {
            engine
                .append_observe_tick_in_transaction(
                    transaction,
                    context.options.writer_key.as_deref(),
                )
                .await?;
        }
        Ok(())
    }
}

fn apply_buffered_write_execution_outcome(
    engine: &Engine,
    context: &mut ExecutionContext,
    execution: SqlExecutionOutcome,
) {
    let active_effects = execution
        .plan_effects_override
        .as_ref()
        .cloned()
        .unwrap_or_default();
    if let Some(version_id) = &active_effects.next_active_version_id {
        context.active_version_id = version_id.clone();
    }
    let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
    state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
    engine.maybe_invalidate_deterministic_settings_cache(&[], &state_commit_stream_changes);
    context
        .pending_state_commit_stream_changes
        .extend(state_commit_stream_changes);
    context.observe_tick_already_emitted |= execution.observe_tick_emitted;
}
