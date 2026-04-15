use crate::live_state::SchemaRegistration;
use crate::transaction::buffered::{LiveStateWriteState, TransactionCoordinator};
use crate::{LixBackendTransaction, LixError};

use super::{CommitOutcome, OverlayReadContext, TransactionDelta, TransactionJournal};

pub struct LiveStateWriteTransaction<'a> {
    coordinator: TransactionCoordinator<'a>,
    live_state_write_state: Option<LiveStateWriteState<'a>>,
}

impl<'a> LiveStateWriteTransaction<'a> {
    pub fn new(
        backend_txn: Box<dyn LixBackendTransaction + 'a>,
        read_context: OverlayReadContext<'a>,
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

    pub async fn commit(mut self) -> Result<CommitOutcome, LixError> {
        self.execute().await?;
        let outcome = self.live_state_write_state()?.outcome();
        self.coordinator.commit().await?;
        Ok(outcome)
    }

    pub async fn rollback(mut self) -> Result<(), LixError> {
        self.coordinator.rollback().await
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

    use crate::live_state::tracked::{
        BatchTrackedRowRequest, TrackedRow, TrackedScanRequest, TrackedTombstoneMarker,
    };
    use crate::live_state::untracked::{
        BatchUntrackedRowRequest, UntrackedRow, UntrackedScanRequest,
    };
    use crate::live_state::RowIdentity;
    use crate::live_state::WriterKeyReadView;
    use crate::live_state::{
        LiveWriteOperation, LiveWriteRow, TrackedReadView, TrackedTombstoneView, UntrackedReadView,
    };
    use crate::transaction::buffered::prepare_materialization_plan;

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
    struct EmptyWriterKeys;

    fn live_write_row(
        schema_key: &str,
        entity_id: &str,
        version_id: &str,
        change_id: &str,
        snapshot_content: Option<&str>,
        untracked: bool,
        operation: LiveWriteOperation,
    ) -> LiveWriteRow {
        LiveWriteRow {
            entity_id: entity_id.to_string(),
            schema_key: schema_key.to_string(),
            schema_version: "1".to_string(),
            file_id: "lix".to_string(),
            version_id: version_id.to_string(),
            global: version_id == "global",
            untracked,
            plugin_key: "lix".to_string(),
            metadata: None,
            change_id: change_id.to_string(),
            writer_key: None,
            snapshot_content: snapshot_content.map(ToString::to_string),
            created_at: Some("2026-03-24T00:00:00Z".to_string()),
            updated_at: if matches!(operation, LiveWriteOperation::Tombstone) {
                "2026-03-24T01:00:00Z".to_string()
            } else {
                "2026-03-24T00:00:00Z".to_string()
            },
            operation,
        }
    }

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
    impl WriterKeyReadView for EmptyWriterKeys {
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
        let writer_keys = EmptyWriterKeys;
        let read_context = OverlayReadContext::new(&tracked, &untracked, &writer_keys)
            .with_tracked_tombstones(&tombstones);
        let mut journal = TransactionJournal::default();
        journal
            .stage(TransactionDelta {
                writes: vec![
                    live_write_row(
                        "lix_commit_edge",
                        "edge-1",
                        "main",
                        "change-1",
                        Some("{\"child_id\":\"c1\"}"),
                        false,
                        LiveWriteOperation::Upsert,
                    ),
                    live_write_row(
                        "lix_commit_edge",
                        "edge-2",
                        "main",
                        "change-2",
                        Some("{\"child_id\":\"c2\"}"),
                        false,
                        LiveWriteOperation::Upsert,
                    ),
                    live_write_row(
                        "lix_version_ref",
                        "main",
                        "global",
                        "change-untracked-1",
                        Some("{\"commit_id\":\"commit-1\"}"),
                        true,
                        LiveWriteOperation::Upsert,
                    ),
                ],
            })
            .expect("journal stage should succeed");

        let plan = prepare_materialization_plan(&read_context, &journal)
            .await
            .expect("preflight should succeed");

        assert_eq!(tracked.scans.get(), 3);
        assert_eq!(untracked.scans.get(), 3);
        assert_eq!(
            plan.writes.iter().filter(|write| !write.untracked).count(),
            2
        );
        assert_eq!(
            plan.writes.iter().filter(|write| write.untracked).count(),
            1
        );
    }

    #[tokio::test]
    async fn journal_rejects_cross_storage_identity_conflicts() {
        let mut journal = TransactionJournal::default();
        journal
            .stage(TransactionDelta {
                writes: vec![live_write_row(
                    "lix_commit_edge",
                    "row-1",
                    "main",
                    "change-1",
                    Some("{\"child_id\":\"c1\"}"),
                    false,
                    LiveWriteOperation::Upsert,
                )],
            })
            .expect("first stage should succeed");

        let error = journal
            .stage(TransactionDelta {
                writes: vec![live_write_row(
                    "lix_commit_edge",
                    "row-1",
                    "main",
                    "change-untracked-conflict",
                    Some("{}"),
                    true,
                    LiveWriteOperation::Upsert,
                )],
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
                writes: vec![
                    live_write_row(
                        "lix_commit_edge",
                        "edge-1",
                        "main",
                        "change-1",
                        Some("{\"child_id\":\"c1\"}"),
                        false,
                        LiveWriteOperation::Upsert,
                    ),
                    live_write_row(
                        "lix_commit_edge",
                        "edge-1",
                        "main",
                        "change-2",
                        None,
                        false,
                        LiveWriteOperation::Tombstone,
                    ),
                ],
            })
            .expect("journal stage should succeed");

        let aggregated = journal.aggregated_delta();
        assert_eq!(aggregated.writes.len(), 1);
        assert_eq!(
            aggregated.writes[0].operation,
            LiveWriteOperation::Tombstone
        );
    }
}
