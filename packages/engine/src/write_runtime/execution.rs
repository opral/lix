use crate::contracts::artifacts::{SchemaRegistration, StateCommitStreamChange};
use crate::write_runtime::buffered::{
    BufferedWriteState, LiveStateWriteState, TransactionCoordinator,
};
use crate::write_runtime::commit::{CanonicalCommitReceipt, PendingPublicCommitSession};
use crate::write_runtime::overlay::PendingTransactionView;
use crate::{LixBackendTransaction, LixError, ReplayCursor};

use super::{
    BufferedWriteExecutionInput, CommitOutcome, ReadContext, TransactionCommitOutcome,
    TransactionDelta, TransactionJournal,
};
use crate::write_runtime::PlannedWriteDelta;

pub struct WriteTransaction<'a> {
    coordinator: TransactionCoordinator<'a>,
    live_state_write_state: Option<LiveStateWriteState<'a>>,
    buffered_write_state: Option<BufferedWriteState>,
}

pub(crate) struct BorrowedWriteTransaction<'tx> {
    backend_txn: &'tx mut dyn LixBackendTransaction,
    buffered_write_state: BufferedWriteState,
}

impl<'a> WriteTransaction<'a> {
    pub fn new(
        backend_txn: Box<dyn LixBackendTransaction + 'a>,
        read_context: ReadContext<'a>,
    ) -> Self {
        Self {
            coordinator: TransactionCoordinator::new(backend_txn),
            live_state_write_state: Some(LiveStateWriteState::new(read_context)),
            buffered_write_state: None,
        }
    }

    pub(crate) fn new_buffered_write(backend_txn: Box<dyn LixBackendTransaction + 'a>) -> Self {
        Self {
            coordinator: TransactionCoordinator::new(backend_txn),
            live_state_write_state: None,
            buffered_write_state: Some(BufferedWriteState::default()),
        }
    }

    pub fn journal(&self) -> &TransactionJournal {
        self.live_state_write_state()
            .expect("journal() only applies to the live-state write state")
            .journal()
    }

    pub fn stage(&mut self, delta: TransactionDelta) -> Result<(), LixError> {
        self.live_state_write_state_mut()?.stage(delta)
    }

    pub fn register_schema(
        &mut self,
        registration: impl Into<SchemaRegistration>,
    ) -> Result<(), LixError> {
        if let Some(write_state) = self.live_state_write_state.as_ref() {
            if write_state.is_executed() {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "cannot register schema after execute()",
                ));
            }
        }
        self.coordinator.register_schema(registration)
    }

    pub async fn execute(&mut self) -> Result<(), LixError> {
        let coordinator = &mut self.coordinator;
        let write_state = self.live_state_write_state.as_mut().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "operation requires a live-state write state",
            )
        })?;
        write_state.execute(coordinator).await
    }

    pub async fn finalize_live_state(&mut self) -> Result<ReplayCursor, LixError> {
        self.coordinator.finalize_live_state().await
    }

    pub(crate) async fn finalize_live_state_for_commit(&mut self) -> Result<(), LixError> {
        let receipt = self
            .buffered_write_state
            .as_ref()
            .and_then(BufferedWriteState::latest_canonical_commit_receipt);
        self.coordinator
            .advance_live_state_replay_boundary_for_commit(receipt)
            .await
    }

    pub async fn commit(mut self) -> Result<CommitOutcome, LixError> {
        self.execute().await?;
        let outcome = self.live_state_write_state()?.outcome();
        self.coordinator.commit().await?;
        Ok(outcome)
    }

    pub(crate) async fn commit_buffered_write(
        mut self,
        mut execution_input: BufferedWriteExecutionInput,
    ) -> Result<TransactionCommitOutcome, LixError> {
        let initial_active_version_id = execution_input.active_version_id().to_string();
        let initial_active_account_ids = execution_input.active_account_ids().to_vec();
        self.prepare_buffered_write_commit(&mut execution_input)
            .await?;
        let mut outcome = self
            .buffered_write_state()
            .map(BufferedWriteState::commit_outcome)
            .unwrap_or_default();
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

    pub async fn rollback(mut self) -> Result<(), LixError> {
        self.coordinator.rollback().await
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
        self.buffered_write_state()
            .expect("buffered_write_journal_is_empty only applies to the buffered write state")
            .journal_is_empty()
    }

    pub(crate) fn buffered_write_pending_transaction_view(
        &self,
    ) -> Result<Option<PendingTransactionView>, LixError> {
        self.buffered_write_state()?.pending_transaction_view()
    }

    pub(crate) fn can_stage_planned_write_delta(
        &self,
        delta: &PlannedWriteDelta,
    ) -> Result<bool, LixError> {
        self.buffered_write_state()?.can_stage_delta(delta)
    }

    pub(crate) fn stage_planned_write_delta(
        &mut self,
        delta: PlannedWriteDelta,
    ) -> Result<(), LixError> {
        self.buffered_write_state_mut()?.stage_delta(delta)
    }

    pub(crate) fn clear_pending_public_commit_session(&mut self) {
        if let Some(write_state) = self.buffered_write_state.as_mut() {
            write_state.clear_pending_public_commit_session();
        }
    }

    pub(crate) fn pending_public_commit_session_mut(
        &mut self,
    ) -> &mut Option<PendingPublicCommitSession> {
        self.buffered_write_state_mut()
            .expect("pending_public_commit_session_mut only applies to the buffered write state")
            .pending_public_commit_session_mut()
    }

    pub(crate) fn buffered_write_commit_outcome_mut(&mut self) -> &mut TransactionCommitOutcome {
        self.buffered_write_state_mut()
            .expect("buffered_write_commit_outcome_mut only applies to the buffered write state")
            .commit_outcome_mut()
    }

    pub(crate) async fn flush_buffered_write_journal(
        &mut self,
        execution_input: &mut BufferedWriteExecutionInput,
    ) -> Result<(), LixError> {
        self.coordinator.register_staged_schemas().await?;
        let transaction = self.coordinator.backend_transaction_mut()?;
        let write_state = self.buffered_write_state.as_mut().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "operation requires a buffered write state",
            )
        })?;
        write_state.flush(transaction, execution_input).await
    }

    pub(crate) async fn prepare_buffered_write_commit(
        &mut self,
        execution_input: &mut BufferedWriteExecutionInput,
    ) -> Result<(), LixError> {
        self.coordinator.register_staged_schemas().await?;
        let transaction = self.coordinator.backend_transaction_mut()?;
        let write_state = self.buffered_write_state.as_mut().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "operation requires a buffered write state",
            )
        })?;
        write_state
            .prepare_commit(transaction, execution_input)
            .await
    }

    pub(crate) fn mark_public_surface_registry_refresh_pending(&mut self) {
        if let Some(write_state) = self.buffered_write_state.as_mut() {
            write_state.mark_public_surface_registry_refresh_pending();
        }
    }

    pub(crate) fn mark_installed_plugins_cache_invalidation_pending(&mut self) {
        if let Some(write_state) = self.buffered_write_state.as_mut() {
            write_state.mark_installed_plugins_cache_invalidation_pending();
        }
    }

    pub(crate) fn record_state_commit_stream_changes(
        &mut self,
        changes: Vec<StateCommitStreamChange>,
    ) {
        if let Some(write_state) = self.buffered_write_state.as_mut() {
            write_state.record_state_commit_stream_changes(changes);
        }
    }

    pub(crate) fn record_canonical_commit_receipt(&mut self, receipt: CanonicalCommitReceipt) {
        if let Some(write_state) = self.buffered_write_state.as_mut() {
            write_state.record_canonical_commit_receipt(receipt);
        }
    }

    fn live_state_write_state(&self) -> Result<&LiveStateWriteState<'a>, LixError> {
        self.live_state_write_state.as_ref().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "operation requires a live-state write state",
            )
        })
    }

    fn live_state_write_state_mut(&mut self) -> Result<&mut LiveStateWriteState<'a>, LixError> {
        self.live_state_write_state.as_mut().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "operation requires a live-state write state",
            )
        })
    }

    fn buffered_write_state(&self) -> Result<&BufferedWriteState, LixError> {
        self.buffered_write_state.as_ref().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "operation requires a buffered write state",
            )
        })
    }

    fn buffered_write_state_mut(&mut self) -> Result<&mut BufferedWriteState, LixError> {
        self.buffered_write_state.as_mut().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "operation requires a buffered write state",
            )
        })
    }
}

impl<'tx> BorrowedWriteTransaction<'tx> {
    pub(crate) fn new(backend_txn: &'tx mut dyn LixBackendTransaction) -> Self {
        Self {
            backend_txn,
            buffered_write_state: BufferedWriteState::default(),
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
        self.buffered_write_state
            .clear_pending_public_commit_session();
    }

    pub(crate) fn pending_public_commit_session_mut(
        &mut self,
    ) -> &mut Option<PendingPublicCommitSession> {
        self.buffered_write_state
            .pending_public_commit_session_mut()
    }

    pub(crate) async fn flush_buffered_write_journal(
        &mut self,
        execution_input: &mut BufferedWriteExecutionInput,
    ) -> Result<(), LixError> {
        let buffered_write_state = &mut self.buffered_write_state;
        let backend_txn = &mut *self.backend_txn;
        buffered_write_state
            .flush(backend_txn, execution_input)
            .await
    }

    pub(crate) fn buffered_write_commit_outcome_mut(&mut self) -> &mut TransactionCommitOutcome {
        self.buffered_write_state.commit_outcome_mut()
    }

    pub(crate) fn mark_installed_plugins_cache_invalidation_pending(&mut self) {
        self.buffered_write_state
            .mark_installed_plugins_cache_invalidation_pending();
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::cell::Cell;
    use std::collections::{BTreeMap, BTreeSet};

    use crate::annotations::writer_key::WorkspaceWriterKeyReadView;
    use crate::contracts::traits::{TrackedReadView, TrackedTombstoneView, UntrackedReadView};
    use crate::live_state::shared::identity::RowIdentity;
    use crate::live_state::tracked::{
        BatchTrackedRowRequest, TrackedRow, TrackedScanRequest, TrackedTombstoneMarker,
        TrackedWriteOperation, TrackedWriteRow,
    };
    use crate::live_state::untracked::{
        BatchUntrackedRowRequest, UntrackedRow, UntrackedScanRequest, UntrackedWriteOperation,
        UntrackedWriteRow,
    };
    use crate::write_runtime::buffered::prepare_materialization_plan;

    use super::*;

    #[derive(Default)]
    struct CountingTrackedView {
        scans: Cell<usize>,
    }

    #[derive(Default)]
    struct CountingUntrackedView {
        scans: Cell<usize>,
    }

    struct EmptyTombstones;
    struct EmptyWorkspaceWriterKeys;

    #[async_trait(?Send)]
    impl TrackedReadView for CountingTrackedView {
        async fn load_exact_rows(
            &self,
            _request: &BatchTrackedRowRequest,
        ) -> Result<Vec<TrackedRow>, LixError> {
            Ok(Vec::new())
        }

        async fn scan_rows(
            &self,
            _request: &TrackedScanRequest,
        ) -> Result<Vec<TrackedRow>, LixError> {
            self.scans.set(self.scans.get() + 1);
            Ok(Vec::new())
        }
    }

    #[async_trait(?Send)]
    impl UntrackedReadView for CountingUntrackedView {
        async fn load_exact_rows(
            &self,
            _request: &BatchUntrackedRowRequest,
        ) -> Result<Vec<UntrackedRow>, LixError> {
            Ok(Vec::new())
        }

        async fn scan_rows(
            &self,
            _request: &UntrackedScanRequest,
        ) -> Result<Vec<UntrackedRow>, LixError> {
            self.scans.set(self.scans.get() + 1);
            Ok(Vec::new())
        }
    }

    #[async_trait(?Send)]
    impl TrackedTombstoneView for EmptyTombstones {
        async fn scan_tombstones(
            &self,
            _request: &TrackedScanRequest,
        ) -> Result<Vec<TrackedTombstoneMarker>, LixError> {
            Ok(Vec::new())
        }
    }

    #[async_trait(?Send)]
    impl WorkspaceWriterKeyReadView for EmptyWorkspaceWriterKeys {
        async fn load_annotation(
            &self,
            _row_identity: &RowIdentity,
        ) -> Result<Option<String>, LixError> {
            Ok(None)
        }

        async fn load_annotations(
            &self,
            row_identities: &BTreeSet<RowIdentity>,
        ) -> Result<BTreeMap<RowIdentity, Option<String>>, LixError> {
            Ok(row_identities
                .iter()
                .cloned()
                .map(|row_identity| (row_identity, None))
                .collect())
        }
    }

    #[tokio::test]
    async fn prepare_materialization_plan_scans_each_partition_once() {
        let tracked = CountingTrackedView::default();
        let untracked = CountingUntrackedView::default();
        let tombstones = EmptyTombstones;
        let writer_keys = EmptyWorkspaceWriterKeys;
        let read_context = ReadContext::new(&tracked, &untracked, &writer_keys)
            .with_tracked_tombstones(&tombstones);
        let mut journal = TransactionJournal::default();
        journal
            .stage(TransactionDelta {
                tracked_writes: vec![
                    TrackedWriteRow {
                        entity_id: "edge-1".to_string(),
                        schema_key: "lix_commit_edge".to_string(),
                        schema_version: "1".to_string(),
                        file_id: "lix".to_string(),
                        version_id: "main".to_string(),
                        global: false,
                        plugin_key: "lix".to_string(),
                        metadata: None,
                        change_id: "change-1".to_string(),
                        writer_key: None,
                        snapshot_content: Some("{\"child_id\":\"c1\"}".to_string()),
                        created_at: Some("2026-03-24T00:00:00Z".to_string()),
                        updated_at: "2026-03-24T00:00:00Z".to_string(),
                        operation: TrackedWriteOperation::Upsert,
                    },
                    TrackedWriteRow {
                        entity_id: "edge-2".to_string(),
                        schema_key: "lix_commit_edge".to_string(),
                        schema_version: "1".to_string(),
                        file_id: "lix".to_string(),
                        version_id: "main".to_string(),
                        global: false,
                        plugin_key: "lix".to_string(),
                        metadata: None,
                        change_id: "change-2".to_string(),
                        writer_key: None,
                        snapshot_content: Some("{\"child_id\":\"c2\"}".to_string()),
                        created_at: Some("2026-03-24T00:00:00Z".to_string()),
                        updated_at: "2026-03-24T00:00:00Z".to_string(),
                        operation: TrackedWriteOperation::Upsert,
                    },
                ],
                untracked_writes: vec![UntrackedWriteRow {
                    entity_id: "main".to_string(),
                    schema_key: "lix_version_ref".to_string(),
                    schema_version: "1".to_string(),
                    file_id: "lix".to_string(),
                    version_id: "global".to_string(),
                    global: true,
                    plugin_key: "lix".to_string(),
                    metadata: None,
                    writer_key: None,
                    snapshot_content: Some("{\"commit_id\":\"commit-1\"}".to_string()),
                    created_at: Some("2026-03-24T00:00:00Z".to_string()),
                    updated_at: "2026-03-24T00:00:00Z".to_string(),
                    operation: UntrackedWriteOperation::Upsert,
                }],
            })
            .expect("journal stage should succeed");

        let plan = prepare_materialization_plan(&read_context, &journal)
            .await
            .expect("preflight should succeed");

        assert_eq!(tracked.scans.get(), 3);
        assert_eq!(untracked.scans.get(), 3);
        assert_eq!(plan.units.len(), 2);
    }

    #[tokio::test]
    async fn journal_rejects_cross_storage_identity_conflicts() {
        let mut journal = TransactionJournal::default();
        journal
            .stage(TransactionDelta {
                tracked_writes: vec![TrackedWriteRow {
                    entity_id: "row-1".to_string(),
                    schema_key: "lix_commit_edge".to_string(),
                    schema_version: "1".to_string(),
                    file_id: "lix".to_string(),
                    version_id: "main".to_string(),
                    global: false,
                    plugin_key: "lix".to_string(),
                    metadata: None,
                    change_id: "change-1".to_string(),
                    writer_key: None,
                    snapshot_content: Some("{\"child_id\":\"c1\"}".to_string()),
                    created_at: Some("2026-03-24T00:00:00Z".to_string()),
                    updated_at: "2026-03-24T00:00:00Z".to_string(),
                    operation: TrackedWriteOperation::Upsert,
                }],
                untracked_writes: Vec::new(),
            })
            .expect("first stage should succeed");

        let error = journal
            .stage(TransactionDelta {
                tracked_writes: Vec::new(),
                untracked_writes: vec![UntrackedWriteRow {
                    entity_id: "row-1".to_string(),
                    schema_key: "lix_commit_edge".to_string(),
                    schema_version: "1".to_string(),
                    file_id: "lix".to_string(),
                    version_id: "main".to_string(),
                    global: false,
                    plugin_key: "lix".to_string(),
                    metadata: None,
                    writer_key: None,
                    snapshot_content: Some("{}".to_string()),
                    created_at: Some("2026-03-24T00:00:00Z".to_string()),
                    updated_at: "2026-03-24T00:00:00Z".to_string(),
                    operation: UntrackedWriteOperation::Upsert,
                }],
            })
            .expect_err("cross-storage conflict should be rejected");

        assert!(error
            .description
            .contains("cannot stage conflicting tracked and untracked identities"));
    }

    #[test]
    fn transaction_journal_coalesces_last_write_wins() {
        let mut journal = TransactionJournal::default();
        journal
            .stage(TransactionDelta {
                tracked_writes: vec![
                    TrackedWriteRow {
                        entity_id: "edge-1".to_string(),
                        schema_key: "lix_commit_edge".to_string(),
                        schema_version: "1".to_string(),
                        file_id: "lix".to_string(),
                        version_id: "main".to_string(),
                        global: false,
                        plugin_key: "lix".to_string(),
                        metadata: None,
                        change_id: "change-1".to_string(),
                        writer_key: None,
                        snapshot_content: Some("{\"child_id\":\"c1\"}".to_string()),
                        created_at: Some("2026-03-24T00:00:00Z".to_string()),
                        updated_at: "2026-03-24T00:00:00Z".to_string(),
                        operation: TrackedWriteOperation::Upsert,
                    },
                    TrackedWriteRow {
                        entity_id: "edge-1".to_string(),
                        schema_key: "lix_commit_edge".to_string(),
                        schema_version: "1".to_string(),
                        file_id: "lix".to_string(),
                        version_id: "main".to_string(),
                        global: false,
                        plugin_key: "lix".to_string(),
                        metadata: None,
                        change_id: "change-2".to_string(),
                        writer_key: None,
                        snapshot_content: None,
                        created_at: Some("2026-03-24T00:00:00Z".to_string()),
                        updated_at: "2026-03-24T01:00:00Z".to_string(),
                        operation: TrackedWriteOperation::Tombstone,
                    },
                ],
                untracked_writes: Vec::new(),
            })
            .expect("journal stage should succeed");

        let aggregated = journal.aggregated_delta();
        assert_eq!(aggregated.tracked_writes.len(), 1);
        assert_eq!(
            aggregated.tracked_writes[0].operation,
            TrackedWriteOperation::Tombstone
        );
    }
}
