use std::collections::{BTreeMap, BTreeSet};

use crate::live_state::effective::{resolve_effective_rows, EffectiveRowsRequest};
use crate::live_state::shared::query::entity_id_in_constraint;
use crate::live_state::{CanonicalWatermark, SchemaRegistration};
use crate::{LixError, LixTransaction};

use super::contracts::{CommitOutcome, TransactionDelta, TransactionJournal};
use super::read_context::ReadContext;
use super::write_plan::TxnMaterializationPlan;
use super::write_runner::run_materialization_plan;

pub struct WriteTransaction<'a> {
    backend_txn: Option<Box<dyn LixTransaction + 'a>>,
    read_context: ReadContext<'a>,
    journal: TransactionJournal,
    registered_schemas: BTreeMap<String, SchemaRegistration>,
    outcome: CommitOutcome,
    executed: bool,
}

impl<'a> WriteTransaction<'a> {
    pub fn new(backend_txn: Box<dyn LixTransaction + 'a>, read_context: ReadContext<'a>) -> Self {
        Self {
            backend_txn: Some(backend_txn),
            read_context,
            journal: TransactionJournal::default(),
            registered_schemas: BTreeMap::new(),
            outcome: CommitOutcome::default(),
            executed: false,
        }
    }

    pub fn journal(&self) -> &TransactionJournal {
        &self.journal
    }

    pub fn stage(&mut self, delta: TransactionDelta) -> Result<(), LixError> {
        if self.executed {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "cannot stage new transaction work after execute()",
            ));
        }
        self.ensure_active()?;
        self.journal.stage(delta)
    }

    pub fn register_schema(
        &mut self,
        registration: impl Into<SchemaRegistration>,
    ) -> Result<(), LixError> {
        if self.executed {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "cannot register schema after execute()",
            ));
        }
        self.ensure_active()?;
        let registration = registration.into();
        self.registered_schemas
            .insert(registration.schema_key.clone(), registration);
        Ok(())
    }

    pub async fn execute(&mut self) -> Result<(), LixError> {
        self.ensure_active()?;
        if self.executed {
            return Ok(());
        }

        let transaction = self.backend_txn.as_deref_mut().ok_or_else(inactive_error)?;
        for registration in self.registered_schemas.values() {
            crate::live_state::register_schema_in_transaction(transaction, registration.clone())
                .await?;
        }
        let plan = prepare_materialization_plan(&self.read_context, &self.journal).await?;
        self.outcome
            .merge(run_materialization_plan(transaction, &plan).await?);
        self.executed = true;
        Ok(())
    }

    pub async fn finalize_live_state(&mut self) -> Result<CanonicalWatermark, LixError> {
        self.ensure_active()?;
        let transaction = self.backend_txn.as_deref_mut().ok_or_else(inactive_error)?;
        crate::live_state::finalize_commit_in_transaction(transaction).await
    }

    pub async fn commit(mut self) -> Result<CommitOutcome, LixError> {
        self.execute().await?;
        let transaction = self.backend_txn.take().ok_or_else(inactive_error)?;
        transaction.commit().await?;
        Ok(self.outcome)
    }

    pub async fn rollback(mut self) -> Result<(), LixError> {
        let transaction = self.backend_txn.take().ok_or_else(inactive_error)?;
        transaction.rollback().await
    }

    fn ensure_active(&self) -> Result<(), LixError> {
        if self.backend_txn.is_none() {
            return Err(inactive_error());
        }
        Ok(())
    }
}

pub(crate) async fn prepare_materialization_plan(
    read_context: &ReadContext<'_>,
    journal: &TransactionJournal,
) -> Result<TxnMaterializationPlan, LixError> {
    let Some(pending) = journal.mutation_journal().pending_txn_participants() else {
        return Ok(TxnMaterializationPlan::default());
    };
    let Some(plan) = journal.mutation_journal().materialization_plan() else {
        return Ok(TxnMaterializationPlan::default());
    };

    let pending_context = read_context.with_pending(pending);
    let effective_context = pending_context.effective_state_context();

    for ((schema_key, version_id), entity_ids) in grouped_entities(&journal.aggregated_delta()) {
        let constraints = if entity_ids.is_empty() {
            Vec::new()
        } else {
            vec![entity_id_in_constraint(entity_ids.into_iter().collect::<Vec<_>>())]
        };
        let _ = resolve_effective_rows(
            &EffectiveRowsRequest {
                schema_key,
                version_id,
                constraints,
                required_columns: Vec::new(),
                include_global: true,
                include_untracked: true,
                include_tombstones: true,
            },
            &effective_context,
        )
        .await?;
    }

    Ok(plan.clone())
}

fn grouped_entities(delta: &TransactionDelta) -> BTreeMap<(String, String), BTreeSet<String>> {
    let mut grouped = BTreeMap::<(String, String), BTreeSet<String>>::new();
    for row in &delta.tracked_writes {
        grouped
            .entry((row.schema_key.clone(), row.version_id.clone()))
            .or_default()
            .insert(row.entity_id.clone());
    }
    for row in &delta.untracked_writes {
        grouped
            .entry((row.schema_key.clone(), row.version_id.clone()))
            .or_default()
            .insert(row.entity_id.clone());
    }
    grouped
}

fn inactive_error() -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", "transaction is no longer active")
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use async_trait::async_trait;

    use crate::live_state::tracked::{
        BatchTrackedRowRequest, ExactTrackedRowRequest, TrackedReadView, TrackedRow,
        TrackedScanRequest, TrackedTombstoneMarker, TrackedTombstoneView,
        TrackedWriteOperation, TrackedWriteRow,
    };
    use crate::live_state::untracked::{
        BatchUntrackedRowRequest, ExactUntrackedRowRequest, UntrackedReadView, UntrackedRow,
        UntrackedScanRequest, UntrackedWriteOperation, UntrackedWriteRow,
    };

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

    #[async_trait(?Send)]
    impl TrackedReadView for CountingTrackedView {
        async fn load_exact_row(
            &self,
            _request: &ExactTrackedRowRequest,
        ) -> Result<Option<TrackedRow>, LixError> {
            Ok(None)
        }

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
        async fn load_exact_row(
            &self,
            _request: &ExactUntrackedRowRequest,
        ) -> Result<Option<UntrackedRow>, LixError> {
            Ok(None)
        }

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
        async fn load_exact_tombstone(
            &self,
            _request: &ExactTrackedRowRequest,
        ) -> Result<Option<TrackedTombstoneMarker>, LixError> {
            Ok(None)
        }

        async fn scan_tombstones(
            &self,
            _request: &TrackedScanRequest,
        ) -> Result<Vec<TrackedTombstoneMarker>, LixError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn prepare_materialization_plan_scans_each_partition_once() {
        let tracked = CountingTrackedView::default();
        let untracked = CountingUntrackedView::default();
        let tombstones = EmptyTombstones;
        let read_context = ReadContext::new(&tracked, &untracked).with_tracked_tombstones(&tombstones);
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
