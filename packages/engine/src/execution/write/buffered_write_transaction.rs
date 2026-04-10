use crate::contracts::should_invalidate_deterministic_settings_cache;
use crate::contracts::{
    CanonicalCommitReceipt, PendingPublicCommitSession, StateCommitStreamChange,
};
use crate::execution::write::buffered::{
    apply_schema_registrations_in_transaction, BufferedWriteState, PlannedWriteDelta,
    TransactionCoordinator,
};
use crate::execution::write::{
    append_observe_tick_in_transaction, execute_planned_write_delta, BufferedWriteExecutionInput,
    PendingTransactionView, PreparedWriteStepStager, SqlExecutionOutcome, TransactionCommitOutcome,
    WriteExecutionBindings,
};
use crate::{LixBackendTransaction, LixError};

pub(crate) struct BufferedWriteTransaction<'a> {
    coordinator: TransactionCoordinator<'a>,
    buffered_write_state: BufferedWriteState,
    pending_public_commit_session: Option<PendingPublicCommitSession>,
    latest_canonical_commit_receipt: Option<CanonicalCommitReceipt>,
}

pub(crate) struct BorrowedBufferedWriteTransaction<'tx> {
    backend_txn: &'tx mut dyn LixBackendTransaction,
    buffered_write_state: BufferedWriteState,
    pending_public_commit_session: Option<PendingPublicCommitSession>,
}

impl<'a> BufferedWriteTransaction<'a> {
    pub(crate) fn new(backend_txn: Box<dyn LixBackendTransaction + 'a>) -> Self {
        Self {
            coordinator: TransactionCoordinator::new(backend_txn),
            buffered_write_state: BufferedWriteState::default(),
            pending_public_commit_session: None,
            latest_canonical_commit_receipt: None,
        }
    }

    pub(crate) async fn commit_buffered_write(
        mut self,
        bindings: &dyn WriteExecutionBindings,
        mut execution_input: BufferedWriteExecutionInput,
    ) -> Result<TransactionCommitOutcome, LixError> {
        let initial_active_version_id = execution_input.active_version_id().to_string();
        let initial_active_account_ids = execution_input.active_account_ids().to_vec();
        self.prepare_buffered_write_commit(bindings, &mut execution_input)
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

    pub(crate) async fn rollback_buffered_write(mut self) -> Result<(), LixError> {
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

    pub(crate) fn buffered_write_pending_transaction_view(
        &self,
    ) -> Result<Option<PendingTransactionView>, LixError> {
        self.buffered_write_state.pending_transaction_view()
    }

    pub(crate) fn can_stage_planned_write_delta(
        &self,
        delta: &PlannedWriteDelta,
    ) -> Result<bool, LixError> {
        self.buffered_write_state.can_stage_delta(delta)
    }

    pub(crate) fn stage_planned_write_delta(
        &mut self,
        delta: PlannedWriteDelta,
    ) -> Result<(), LixError> {
        self.buffered_write_state.stage_delta(delta)
    }

    pub(crate) fn clear_pending_public_commit_session(&mut self) {
        self.pending_public_commit_session = None;
    }

    pub(crate) fn pending_public_commit_session_mut(
        &mut self,
    ) -> &mut Option<PendingPublicCommitSession> {
        &mut self.pending_public_commit_session
    }

    pub(crate) fn buffered_write_commit_outcome_mut(&mut self) -> &mut TransactionCommitOutcome {
        self.buffered_write_state.commit_outcome_mut()
    }

    pub(crate) async fn flush_buffered_write_journal(
        &mut self,
        bindings: &dyn WriteExecutionBindings,
        execution_input: &mut BufferedWriteExecutionInput,
    ) -> Result<(), LixError> {
        let Some(delta) = self.buffered_write_state.take_staged_delta() else {
            return Ok(());
        };
        let transaction = self.coordinator.backend_transaction_mut()?;
        apply_schema_registrations_in_transaction(transaction, delta.schema_registrations())
            .await?;
        let execution = execute_planned_write_delta(
            bindings,
            transaction,
            &delta,
            Some(&mut self.pending_public_commit_session),
        )
        .await?;
        apply_buffered_write_execution_outcome(
            &mut self.buffered_write_state,
            &mut self.latest_canonical_commit_receipt,
            execution_input,
            execution,
        );
        Ok(())
    }

    pub(crate) async fn prepare_buffered_write_commit(
        &mut self,
        bindings: &dyn WriteExecutionBindings,
        execution_input: &mut BufferedWriteExecutionInput,
    ) -> Result<(), LixError> {
        self.flush_buffered_write_journal(bindings, execution_input)
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

impl PreparedWriteStepStager for BufferedWriteTransaction<'_> {
    fn mark_public_surface_registry_refresh_pending(&mut self) {
        Self::mark_public_surface_registry_refresh_pending(self);
    }

    fn stage_planned_write_delta(&mut self, delta: PlannedWriteDelta) -> Result<(), LixError> {
        Self::stage_planned_write_delta(self, delta)
    }
}

impl<'tx> BorrowedBufferedWriteTransaction<'tx> {
    pub(crate) fn new(backend_txn: &'tx mut dyn LixBackendTransaction) -> Self {
        Self {
            backend_txn,
            buffered_write_state: BufferedWriteState::default(),
            pending_public_commit_session: None,
        }
    }

    pub(crate) fn backend_transaction_mut(&mut self) -> &mut dyn LixBackendTransaction {
        &mut *self.backend_txn
    }

    pub(crate) fn buffered_write_journal_is_empty(&self) -> bool {
        self.buffered_write_state.journal_is_empty()
    }

    pub(crate) fn buffered_write_pending_transaction_view(
        &self,
    ) -> Result<Option<PendingTransactionView>, LixError> {
        self.buffered_write_state.pending_transaction_view()
    }

    pub(crate) fn can_stage_planned_write_delta(
        &self,
        delta: &PlannedWriteDelta,
    ) -> Result<bool, LixError> {
        self.buffered_write_state.can_stage_delta(delta)
    }

    pub(crate) fn stage_planned_write_delta(
        &mut self,
        delta: PlannedWriteDelta,
    ) -> Result<(), LixError> {
        self.buffered_write_state.stage_delta(delta)
    }

    pub(crate) fn clear_pending_public_commit_session(&mut self) {
        self.pending_public_commit_session = None;
    }

    pub(crate) fn pending_public_commit_session_mut(
        &mut self,
    ) -> &mut Option<PendingPublicCommitSession> {
        &mut self.pending_public_commit_session
    }

    pub(crate) async fn flush_buffered_write_journal(
        &mut self,
        bindings: &dyn WriteExecutionBindings,
        execution_input: &mut BufferedWriteExecutionInput,
    ) -> Result<(), LixError> {
        let Some(delta) = self.buffered_write_state.take_staged_delta() else {
            return Ok(());
        };
        let mut latest_canonical_commit_receipt = None;
        apply_schema_registrations_in_transaction(self.backend_txn, delta.schema_registrations())
            .await?;
        let execution = execute_planned_write_delta(
            bindings,
            self.backend_txn,
            &delta,
            Some(&mut self.pending_public_commit_session),
        )
        .await?;
        apply_buffered_write_execution_outcome(
            &mut self.buffered_write_state,
            &mut latest_canonical_commit_receipt,
            execution_input,
            execution,
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

impl PreparedWriteStepStager for BorrowedBufferedWriteTransaction<'_> {
    fn mark_public_surface_registry_refresh_pending(&mut self) {
        self.buffered_write_state
            .mark_public_surface_registry_refresh_pending();
    }

    fn stage_planned_write_delta(&mut self, delta: PlannedWriteDelta) -> Result<(), LixError> {
        Self::stage_planned_write_delta(self, delta)
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
    execution: SqlExecutionOutcome,
) {
    let active_effects = execution
        .plan_effects_override
        .as_ref()
        .cloned()
        .unwrap_or_default();
    execution_input.apply_session_delta(&active_effects.session_delta);
    let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
    state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
    state.commit_outcome_mut().merge(TransactionCommitOutcome {
        session_delta: active_effects.session_delta.clone(),
        invalidate_deterministic_settings_cache: should_invalidate_deterministic_settings_cache(
            &[],
            &state_commit_stream_changes,
        ),
        invalidate_installed_plugins_cache: execution.plugin_changes_committed,
        state_commit_stream_changes,
        ..TransactionCommitOutcome::default()
    });
    if let Some(receipt) = execution.canonical_commit_receipt {
        record_latest_canonical_commit_receipt(latest_canonical_commit_receipt, receipt);
    }
    state.absorb_observe_tick_emitted(execution.observe_tick_emitted);
}
