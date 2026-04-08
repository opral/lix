use crate::contracts::artifacts::SchemaRegistration;
use crate::execution::write::buffered::{LiveStateWriteState, TransactionCoordinator};
use crate::{LixBackendTransaction, LixError, ReplayCursor};

use super::{CommitOutcome, ReadContext, TransactionDelta, TransactionJournal};

pub struct WriteTransaction<'a> {
    coordinator: TransactionCoordinator<'a>,
    live_state_write_state: Option<LiveStateWriteState<'a>>,
}

impl<'a> WriteTransaction<'a> {
    pub fn new(
        backend_txn: Box<dyn LixBackendTransaction + 'a>,
        read_context: ReadContext<'a>,
    ) -> Self {
        Self {
            coordinator: TransactionCoordinator::new(backend_txn),
            live_state_write_state: Some(LiveStateWriteState::new(read_context)),
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

    pub async fn commit(mut self) -> Result<CommitOutcome, LixError> {
        self.execute().await?;
        let outcome = self.live_state_write_state()?.outcome();
        self.coordinator.commit().await?;
        Ok(outcome)
    }

    pub async fn rollback(mut self) -> Result<(), LixError> {
        self.coordinator.rollback().await
    }

    pub(crate) fn backend_transaction_mut(
        &mut self,
    ) -> Result<&mut dyn LixBackendTransaction, LixError> {
        self.coordinator.backend_transaction_mut()
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
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::cell::Cell;
    use std::collections::{BTreeMap, BTreeSet};

    use crate::contracts::traits::{TrackedReadView, TrackedTombstoneView, UntrackedReadView};
    use crate::contracts::traits::WorkspaceWriterKeyReadView;
    use crate::live_state::shared::identity::RowIdentity;
    use crate::live_state::tracked::{
        BatchTrackedRowRequest, TrackedRow, TrackedScanRequest, TrackedTombstoneMarker,
        TrackedWriteOperation, TrackedWriteRow,
    };
    use crate::live_state::untracked::{
        BatchUntrackedRowRequest, UntrackedRow, UntrackedScanRequest, UntrackedWriteOperation,
        UntrackedWriteRow,
    };
    use crate::execution::write::buffered::prepare_materialization_plan;

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
