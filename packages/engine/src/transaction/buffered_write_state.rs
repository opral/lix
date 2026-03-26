use crate::engine::Engine;
use crate::LixBackendTransaction;
use crate::LixError;

use super::contracts::TransactionCommitOutcome;
use super::coordinator::apply_schema_registrations_in_transaction;
use super::sql_adapter::{
    ExecutionContext, PendingPublicCommitSession, PendingTransactionView, SqlExecutionOutcome,
};
use super::write_plan::{BufferedWriteJournal, PlannedWriteDelta};
use super::write_runner::execute_planned_write_delta;

#[derive(Default)]
pub(crate) struct BufferedWriteState {
    journal: BufferedWriteJournal,
    pending_public_commit_session: Option<PendingPublicCommitSession>,
    commit_outcome: TransactionCommitOutcome,
    observe_tick_emitted: bool,
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

    pub(crate) fn commit_outcome(&self) -> TransactionCommitOutcome {
        self.commit_outcome.clone()
    }

    pub(crate) fn commit_outcome_mut(&mut self) -> &mut TransactionCommitOutcome {
        &mut self.commit_outcome
    }

    pub(crate) fn mark_public_surface_registry_refresh_pending(&mut self) {
        self.commit_outcome.refresh_public_surface_registry = true;
    }

    pub(crate) fn mark_installed_plugins_cache_invalidation_pending(&mut self) {
        self.commit_outcome.invalidate_installed_plugins_cache = true;
    }

    pub(crate) fn record_state_commit_stream_changes(
        &mut self,
        changes: Vec<crate::state::stream::StateCommitStreamChange>,
    ) {
        self.commit_outcome
            .state_commit_stream_changes
            .extend(changes);
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
        apply_schema_registrations_in_transaction(transaction, delta.schema_registrations())
            .await?;
        let mut pending_public_commit_session = self.pending_public_commit_session.take();
        let execution = execute_planned_write_delta(
            engine,
            transaction,
            &delta,
            Some(&mut pending_public_commit_session),
        )
        .await?;
        self.pending_public_commit_session = pending_public_commit_session;
        apply_buffered_write_execution_outcome(self, engine, context, execution);
        Ok(())
    }

    pub(crate) async fn prepare_commit(
        &mut self,
        transaction: &mut dyn LixBackendTransaction,
        engine: &Engine,
        context: &mut ExecutionContext,
    ) -> Result<(), LixError> {
        self.flush(transaction, engine, context).await?;
        if !self.observe_tick_emitted && !self.commit_outcome.state_commit_stream_changes.is_empty()
        {
            engine
                .append_observe_tick_in_transaction(
                    transaction,
                    context.options.writer_key.as_deref(),
                )
                .await?;
            self.observe_tick_emitted = true;
        }
        Ok(())
    }
}

fn apply_buffered_write_execution_outcome(
    state: &mut BufferedWriteState,
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
    if let Some(active_account_ids) = &active_effects.next_active_account_ids {
        context.active_account_ids = active_account_ids.clone();
    }
    let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
    state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
    state.commit_outcome.merge(TransactionCommitOutcome {
        next_active_account_ids: active_effects.next_active_account_ids.clone(),
        invalidate_deterministic_settings_cache: engine
            .should_invalidate_deterministic_settings_cache(&[], &state_commit_stream_changes),
        invalidate_installed_plugins_cache: execution.plugin_changes_committed,
        state_commit_stream_changes,
        ..TransactionCommitOutcome::default()
    });
    state.observe_tick_emitted |= execution.observe_tick_emitted;
}
