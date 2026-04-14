//! Buffered write transaction helpers.
//!
//! This module owns buffered write commit orchestration, rollback, and staged
//! journal flushing over a single backend transaction.

use crate::canonical::CanonicalCommitReceipt;
use crate::streams::{should_invalidate_deterministic_settings_cache, StateCommitStreamChange};
use crate::transaction::buffered::{
    apply_schema_registrations_in_transaction, BufferedWriteState, TransactionCoordinator,
    TransactionWriteDelta,
};
use crate::transaction::pipeline::WriteExecutionOutcome;
use crate::transaction::{
    append_observe_tick_in_transaction, BufferedWriteExecutionInput, PendingCommitState,
    PendingWriteOverlay, PreparedWriteStatementStager, TransactionCommitOutcome,
    WriteExecutionContext,
};
use crate::{LixBackendTransaction, LixError};

pub(crate) struct BufferedWriteTransaction<'a> {
    coordinator: TransactionCoordinator<'a>,
    buffered_write_state: BufferedWriteState,
    pending_commit_state: Option<PendingCommitState>,
    latest_canonical_commit_receipt: Option<CanonicalCommitReceipt>,
}

pub(crate) struct BorrowedBufferedWriteTransaction<'tx> {
    backend_txn: &'tx mut dyn LixBackendTransaction,
    buffered_write_state: BufferedWriteState,
    pending_commit_state: Option<PendingCommitState>,
}

impl<'a> BufferedWriteTransaction<'a> {
    pub(crate) fn new(backend_txn: Box<dyn LixBackendTransaction + 'a>) -> Self {
        Self {
            coordinator: TransactionCoordinator::new(backend_txn),
            buffered_write_state: BufferedWriteState::default(),
            pending_commit_state: None,
            latest_canonical_commit_receipt: None,
        }
    }

    pub(crate) async fn commit(
        mut self,
        execution_context: &dyn WriteExecutionContext,
        mut execution_input: BufferedWriteExecutionInput,
    ) -> Result<TransactionCommitOutcome, LixError> {
        let initial_active_version_id = execution_input.active_version_id().to_string();
        let initial_active_account_ids = execution_input.active_account_ids().to_vec();
        self.prepare_buffered_write_commit(execution_context, &mut execution_input)
            .await?;
        let mut outcome = self.buffered_write_state.commit_outcome();
        if execution_input.active_version_id() != initial_active_version_id {
            outcome.session_delta.next_active_version_id =
                Some(execution_input.active_version_id().to_string());
        }
        if execution_input.active_account_ids() != initial_active_account_ids {
            outcome.session_delta.next_active_account_ids =
                Some(execution_input.active_account_ids().to_vec());
        }
        self.finalize_live_state_for_commit().await?;
        self.coordinator.commit().await?;
        Ok(outcome)
    }

    pub(crate) async fn rollback(mut self) -> Result<(), LixError> {
        self.coordinator.rollback().await
    }

    pub(crate) fn backend_transaction_mut(
        &mut self,
    ) -> Result<&mut dyn LixBackendTransaction, LixError> {
        self.coordinator.backend_transaction_mut()
    }

    pub(crate) fn buffered_write_journal_is_empty(&self) -> bool {
        self.buffered_write_state.journal_is_empty()
    }

    pub(crate) fn buffered_write_pending_write_overlay(
        &self,
    ) -> Result<Option<PendingWriteOverlay>, LixError> {
        self.buffered_write_state.pending_write_overlay()
    }

    pub(crate) fn can_stage_transaction_write_delta(
        &self,
        delta: &TransactionWriteDelta,
    ) -> Result<bool, LixError> {
        self.buffered_write_state.can_stage_delta(delta)
    }

    pub(crate) fn stage_transaction_write_delta(
        &mut self,
        delta: TransactionWriteDelta,
    ) -> Result<(), LixError> {
        self.buffered_write_state.stage_delta(delta)
    }

    pub(crate) fn clear_pending_commit_state(&mut self) {
        self.pending_commit_state = None;
    }

    pub(crate) fn pending_commit_state_mut(&mut self) -> &mut Option<PendingCommitState> {
        &mut self.pending_commit_state
    }

    pub(crate) fn buffered_write_commit_outcome_mut(&mut self) -> &mut TransactionCommitOutcome {
        self.buffered_write_state.commit_outcome_mut()
    }

    pub(crate) async fn flush_journal(
        &mut self,
        execution_context: &dyn WriteExecutionContext,
        execution_input: &mut BufferedWriteExecutionInput,
    ) -> Result<(), LixError> {
        let Some(delta) = self.buffered_write_state.take_staged_delta() else {
            return Ok(());
        };
        let transaction = self.coordinator.backend_transaction_mut()?;
        apply_schema_registrations_in_transaction(transaction, delta.schema_registrations())
            .await?;
        let write_outcome = delta
            .execute(
                execution_context,
                transaction,
                Some(&mut self.pending_commit_state),
            )
            .await?;
        apply_buffered_write_execution_outcome(
            &mut self.buffered_write_state,
            &mut self.latest_canonical_commit_receipt,
            execution_input,
            write_outcome,
        );
        Ok(())
    }

    pub(crate) async fn prepare_buffered_write_commit(
        &mut self,
        execution_context: &dyn WriteExecutionContext,
        execution_input: &mut BufferedWriteExecutionInput,
    ) -> Result<(), LixError> {
        self.flush_journal(execution_context, execution_input)
            .await?;
        if !self.buffered_write_state.observe_tick_emitted()
            && !self
                .buffered_write_state
                .commit_outcome()
                .state_commit_stream_changes
                .is_empty()
        {
            append_observe_tick_in_transaction(
                self.coordinator.backend_transaction_mut()?,
                execution_input.writer_key(),
            )
            .await?;
            self.buffered_write_state.mark_observe_tick_emitted();
        }
        Ok(())
    }

    pub(crate) fn mark_public_surface_registry_refresh_pending(&mut self) {
        self.buffered_write_state
            .mark_public_surface_registry_refresh_pending();
    }

    pub(crate) fn mark_installed_plugins_cache_invalidation_pending(&mut self) {
        self.buffered_write_state
            .mark_installed_plugins_cache_invalidation_pending();
    }

    pub(crate) fn record_state_commit_stream_changes(
        &mut self,
        changes: Vec<StateCommitStreamChange>,
    ) {
        self.buffered_write_state
            .record_state_commit_stream_changes(changes);
    }

    pub(crate) fn record_canonical_commit_receipt(&mut self, receipt: CanonicalCommitReceipt) {
        record_latest_canonical_commit_receipt(&mut self.latest_canonical_commit_receipt, receipt);
    }

    async fn finalize_live_state_for_commit(&mut self) -> Result<(), LixError> {
        self.coordinator
            .advance_live_state_replay_boundary_for_commit(
                self.latest_canonical_commit_receipt.as_ref(),
            )
            .await
    }
}

impl PreparedWriteStatementStager for BufferedWriteTransaction<'_> {
    fn mark_public_surface_registry_refresh_pending(&mut self) {
        Self::mark_public_surface_registry_refresh_pending(self);
    }

    fn stage_transaction_write_delta(
        &mut self,
        delta: TransactionWriteDelta,
    ) -> Result<(), LixError> {
        Self::stage_transaction_write_delta(self, delta)
    }
}

impl<'tx> BorrowedBufferedWriteTransaction<'tx> {
    pub(crate) fn new(backend_txn: &'tx mut dyn LixBackendTransaction) -> Self {
        Self {
            backend_txn,
            buffered_write_state: BufferedWriteState::default(),
            pending_commit_state: None,
        }
    }

    pub(crate) fn backend_transaction_mut(&mut self) -> &mut dyn LixBackendTransaction {
        &mut *self.backend_txn
    }

    pub(crate) fn buffered_write_journal_is_empty(&self) -> bool {
        self.buffered_write_state.journal_is_empty()
    }

    pub(crate) fn buffered_write_pending_write_overlay(
        &self,
    ) -> Result<Option<PendingWriteOverlay>, LixError> {
        self.buffered_write_state.pending_write_overlay()
    }

    pub(crate) fn can_stage_transaction_write_delta(
        &self,
        delta: &TransactionWriteDelta,
    ) -> Result<bool, LixError> {
        self.buffered_write_state.can_stage_delta(delta)
    }

    pub(crate) fn stage_transaction_write_delta(
        &mut self,
        delta: TransactionWriteDelta,
    ) -> Result<(), LixError> {
        self.buffered_write_state.stage_delta(delta)
    }

    pub(crate) fn clear_pending_commit_state(&mut self) {
        self.pending_commit_state = None;
    }

    pub(crate) fn pending_commit_state_mut(&mut self) -> &mut Option<PendingCommitState> {
        &mut self.pending_commit_state
    }

    pub(crate) async fn flush_journal(
        &mut self,
        execution_context: &dyn WriteExecutionContext,
        execution_input: &mut BufferedWriteExecutionInput,
    ) -> Result<(), LixError> {
        let Some(delta) = self.buffered_write_state.take_staged_delta() else {
            return Ok(());
        };
        let mut latest_canonical_commit_receipt = None;
        apply_schema_registrations_in_transaction(self.backend_txn, delta.schema_registrations())
            .await?;
        let write_outcome = delta
            .execute(
                execution_context,
                self.backend_txn,
                Some(&mut self.pending_commit_state),
            )
            .await?;
        apply_buffered_write_execution_outcome(
            &mut self.buffered_write_state,
            &mut latest_canonical_commit_receipt,
            execution_input,
            write_outcome,
        );
        Ok(())
    }

    pub(crate) fn buffered_write_commit_outcome_mut(&mut self) -> &mut TransactionCommitOutcome {
        self.buffered_write_state.commit_outcome_mut()
    }

    pub(crate) fn mark_public_surface_registry_refresh_pending(&mut self) {
        self.buffered_write_state
            .mark_public_surface_registry_refresh_pending();
    }

    pub(crate) fn mark_installed_plugins_cache_invalidation_pending(&mut self) {
        self.buffered_write_state
            .mark_installed_plugins_cache_invalidation_pending();
    }
}

impl PreparedWriteStatementStager for BorrowedBufferedWriteTransaction<'_> {
    fn mark_public_surface_registry_refresh_pending(&mut self) {
        self.buffered_write_state
            .mark_public_surface_registry_refresh_pending();
    }

    fn stage_transaction_write_delta(
        &mut self,
        delta: TransactionWriteDelta,
    ) -> Result<(), LixError> {
        Self::stage_transaction_write_delta(self, delta)
    }
}

fn record_latest_canonical_commit_receipt(
    slot: &mut Option<CanonicalCommitReceipt>,
    receipt: CanonicalCommitReceipt,
) {
    let should_replace = slot
        .as_ref()
        .is_none_or(|current| receipt.replay_cursor.is_newer_than(&current.replay_cursor));
    if should_replace {
        *slot = Some(receipt);
    }
}

fn apply_buffered_write_execution_outcome(
    state: &mut BufferedWriteState,
    latest_canonical_commit_receipt: &mut Option<CanonicalCommitReceipt>,
    execution_input: &mut BufferedWriteExecutionInput,
    write_outcome: WriteExecutionOutcome,
) {
    let active_effects = write_outcome
        .plan_effects_override
        .as_ref()
        .cloned()
        .unwrap_or_default();
    execution_input.apply_session_delta(&active_effects.session_delta);
    let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
    state_commit_stream_changes.extend(write_outcome.state_commit_stream_changes.clone());
    state.commit_outcome_mut().merge(TransactionCommitOutcome {
        session_delta: active_effects.session_delta.clone(),
        invalidate_deterministic_settings_cache: should_invalidate_deterministic_settings_cache(
            &[],
            &state_commit_stream_changes,
        ),
        invalidate_installed_plugins_cache: write_outcome.plugin_changes_committed,
        state_commit_stream_changes,
        ..TransactionCommitOutcome::default()
    });
    if let Some(receipt) = write_outcome.canonical_commit_receipt {
        record_latest_canonical_commit_receipt(latest_canonical_commit_receipt, receipt);
    }
    state.absorb_observe_tick_emitted(write_outcome.observe_tick_emitted);
}
