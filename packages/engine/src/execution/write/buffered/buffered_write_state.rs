use super::BufferedWriteJournal;
use super::PlannedWriteDelta;
use crate::contracts::StateCommitStreamChange;
use crate::execution::write::overlay::PendingTransactionView;
use crate::execution::write::TransactionCommitOutcome;
use crate::LixError;

#[derive(Default)]
pub(crate) struct BufferedWriteState {
    journal: BufferedWriteJournal,
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
        changes: Vec<StateCommitStreamChange>,
    ) {
        self.commit_outcome
            .state_commit_stream_changes
            .extend(changes);
    }

    pub(crate) fn take_staged_delta(&mut self) -> Option<PlannedWriteDelta> {
        self.journal.take_staged_delta()
    }

    pub(crate) fn observe_tick_emitted(&self) -> bool {
        self.observe_tick_emitted
    }

    pub(crate) fn mark_observe_tick_emitted(&mut self) {
        self.observe_tick_emitted = true;
    }

    pub(crate) fn absorb_observe_tick_emitted(&mut self, emitted: bool) {
        self.observe_tick_emitted |= emitted;
    }
}
