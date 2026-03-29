use std::collections::{BTreeMap, BTreeSet};

use crate::canonical::receipt::UpdatedVersionRef;
use crate::canonical::{CanonicalCommitReceipt, CanonicalWatermark};
use crate::live_state::shared::identity::RowIdentity;
use crate::live_state::untracked::{
    UntrackedWriteBatch, UntrackedWriteOperation, UntrackedWriteParticipant, UntrackedWriteRow,
};
use crate::live_state::{
    LiveStateMode, LiveStateProjectionStatus, LiveStateRebuildDebugMode, LiveStateRebuildRequest,
    LiveStateRebuildScope,
};
use crate::version::{
    version_ref_file_id, version_ref_plugin_key, version_ref_schema_key,
    version_ref_schema_version, version_ref_snapshot_content, version_ref_storage_version_id,
};
use crate::{LixBackend, LixBackendTransaction, LixError, TransactionMode};

const MAX_LIVE_STATE_DELTA_MERGE_PASSES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DerivedProjectionId {
    LiveState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionReplayMode {
    Uninitialized,
    Bootstrapping,
    Ready,
    NeedsRebuild,
    Rebuilding,
}

impl From<LiveStateMode> for ProjectionReplayMode {
    fn from(value: LiveStateMode) -> Self {
        match value {
            LiveStateMode::Uninitialized => Self::Uninitialized,
            LiveStateMode::Bootstrapping => Self::Bootstrapping,
            LiveStateMode::Ready => Self::Ready,
            LiveStateMode::NeedsRebuild => Self::NeedsRebuild,
            LiveStateMode::Rebuilding => Self::Rebuilding,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedProjectionStatus {
    pub projection: DerivedProjectionId,
    pub mode: ProjectionReplayMode,
    pub applied_watermark: Option<CanonicalWatermark>,
    pub latest_canonical_watermark: Option<CanonicalWatermark>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionStatus {
    pub projections: Vec<DerivedProjectionStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProjectionCatchUpOutcome {
    AlreadyApplied,
    RebuiltToTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionCatchUpReport {
    pub(crate) projection: DerivedProjectionId,
    pub(crate) outcome: ProjectionCatchUpOutcome,
    pub(crate) starting_watermark: Option<CanonicalWatermark>,
    pub(crate) target_watermark: CanonicalWatermark,
    pub(crate) final_watermark: Option<CanonicalWatermark>,
    pub(crate) full_rebuild_passes: usize,
    pub(crate) delta_merge_passes: usize,
}

pub(crate) async fn projection_status(
    backend: &dyn LixBackend,
) -> Result<ProjectionStatus, LixError> {
    Ok(ProjectionStatus {
        projections: vec![derived_projection_status_from_live_state(
            crate::live_state::load_projection_status_with_backend(backend).await?,
        )],
    })
}

pub(crate) async fn apply_canonical_receipt_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    receipt: &CanonicalCommitReceipt,
) -> Result<(), LixError> {
    crate::live_state::advance_commit_replay_boundary_to_watermark_in_transaction(
        transaction,
        &receipt.canonical_watermark,
    )
    .await
}

pub(crate) async fn apply_commit_projections_best_effort_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    receipt: &CanonicalCommitReceipt,
    tracked_writer_key_hints: &BTreeMap<RowIdentity, Option<String>>,
) -> Result<(), LixError> {
    apply_legacy_version_ref_compat_mirrors_best_effort_in_transaction(
        transaction,
        &receipt.updated_version_refs,
    )
    .await;

    if receipt.affected_versions.is_empty() {
        return Ok(());
    }

    if crate::live_state::require_ready_in_transaction(transaction)
        .await
        .is_err()
    {
        crate::live_state::mark_needs_rebuild_at_canonical_watermark_in_transaction(
            transaction,
            &receipt.canonical_watermark,
        )
        .await?;
        return Ok(());
    }

    let rebuild_request = LiveStateRebuildRequest {
        scope: LiveStateRebuildScope::Versions(receipt.affected_versions.iter().cloned().collect()),
        debug: LiveStateRebuildDebugMode::Off,
        debug_row_limit: 0,
    };
    if let Err(_projection_error) =
        crate::live_state::rebuild_scope_with_writer_key_hints_in_transaction(
            transaction,
            &rebuild_request,
            tracked_writer_key_hints,
        )
        .await
    {
        crate::live_state::mark_needs_rebuild_at_canonical_watermark_in_transaction(
            transaction,
            &receipt.canonical_watermark,
        )
        .await?;
    }

    Ok(())
}

async fn apply_legacy_version_ref_compat_mirrors_best_effort_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_ref_updates: &[UpdatedVersionRef],
) {
    if let Err(_mirror_error) =
        apply_legacy_version_ref_compat_mirrors_in_transaction(transaction, version_ref_updates)
            .await
    {
        // Legacy compatibility mirrors are disposable derived state kept only
        // for older read paths. They must not block canonical commit
        // durability, live-state projection readiness, or semantic rebuild
        // correctness.
    }
}

pub(crate) async fn mark_live_state_projection_ready_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<CanonicalWatermark, LixError> {
    crate::live_state::finalize_commit_in_transaction(transaction).await
}

pub(crate) async fn mark_live_state_projection_ready_with_backend(
    backend: &dyn LixBackend,
    watermark: &CanonicalWatermark,
) -> Result<(), LixError> {
    crate::live_state::mark_ready_with_backend(backend, watermark).await
}

pub(crate) async fn catch_up_live_state_to_watermark(
    backend: &dyn LixBackend,
    target: &CanonicalWatermark,
) -> Result<ProjectionCatchUpReport, LixError> {
    let starting_status = crate::live_state::load_projection_status_with_backend(backend).await?;
    let starting_watermark = starting_status.applied_watermark.clone();
    let mut requested_target = target.clone();
    if let Some(latest) = starting_status.latest_canonical_watermark.as_ref() {
        if latest.is_newer_than(&requested_target) {
            requested_target = latest.clone();
        }
    }

    let mut applied_watermark = starting_status.applied_watermark;
    let mut current_target = requested_target.clone();
    let mut full_rebuild_passes = 0usize;
    let mut delta_merge_passes = 0usize;

    for _ in 0..=MAX_LIVE_STATE_DELTA_MERGE_PASSES {
        if watermark_at_or_ahead_of_target(applied_watermark.as_ref(), &current_target) {
            let latest = crate::live_state::load_latest_canonical_watermark(backend).await?;
            match latest {
                Some(latest) if latest.is_newer_than(&current_target) => {
                    current_target = latest;
                    continue;
                }
                _ => {
                    return Ok(ProjectionCatchUpReport {
                        projection: DerivedProjectionId::LiveState,
                        outcome: if full_rebuild_passes == 0 && delta_merge_passes == 0 {
                            ProjectionCatchUpOutcome::AlreadyApplied
                        } else {
                            ProjectionCatchUpOutcome::RebuiltToTarget
                        },
                        starting_watermark,
                        target_watermark: current_target,
                        final_watermark: applied_watermark,
                        full_rebuild_passes,
                        delta_merge_passes,
                    });
                }
            }
        }

        let scope = match applied_watermark.as_ref() {
            Some(applied) => {
                match resolve_incremental_live_state_scope(backend, applied, &current_target)
                    .await?
                {
                    Some(version_ids) => {
                        delta_merge_passes += 1;
                        LiveStateRebuildScope::Versions(version_ids)
                    }
                    _ => {
                        full_rebuild_passes += 1;
                        LiveStateRebuildScope::Full
                    }
                }
            }
            None => {
                full_rebuild_passes += 1;
                LiveStateRebuildScope::Full
            }
        };

        apply_live_state_replay_scope_to_watermark(backend, &scope, &current_target).await?;

        let projection_status =
            crate::live_state::load_projection_status_with_backend(backend).await?;
        if !watermark_at_or_ahead_of_target(
            projection_status.applied_watermark.as_ref(),
            &current_target,
        ) {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "live_state catch-up finished without reaching target watermark '{}@{}'",
                    current_target.change_id, current_target.created_at
                ),
            ));
        }
        applied_watermark = projection_status.applied_watermark;
        if let Some(latest) = projection_status.latest_canonical_watermark {
            if latest.is_newer_than(&current_target) {
                current_target = latest;
            }
        }
    }

    Err(LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!(
            "live_state catch-up exceeded {} delta-merge passes without converging",
            MAX_LIVE_STATE_DELTA_MERGE_PASSES
        ),
    ))
}

async fn resolve_incremental_live_state_scope(
    backend: &dyn LixBackend,
    older_exclusive: &CanonicalWatermark,
    newer_inclusive: &CanonicalWatermark,
) -> Result<Option<BTreeSet<String>>, LixError> {
    if let Some(version_ids) =
        crate::canonical::refs::load_committed_version_ids_updated_between_watermarks_with_backend(
            backend,
            older_exclusive,
            newer_inclusive,
        )
        .await?
    {
        if !version_ids.is_empty() {
            return Ok(Some(version_ids));
        }
    }

    let mut executor = backend;
    let version_ids = crate::canonical::roots::load_all_version_head_commit_ids(&mut executor)
        .await?
        .into_iter()
        .map(|row| row.version_id)
        .collect::<BTreeSet<_>>();
    if version_ids.is_empty() {
        return Ok(None);
    }
    Ok(Some(version_ids))
}

fn derived_projection_status_from_live_state(
    status: LiveStateProjectionStatus,
) -> DerivedProjectionStatus {
    DerivedProjectionStatus {
        projection: DerivedProjectionId::LiveState,
        mode: status.mode.into(),
        applied_watermark: status.applied_watermark,
        latest_canonical_watermark: status.latest_canonical_watermark,
    }
}

fn watermark_at_or_ahead_of_target(
    applied: Option<&CanonicalWatermark>,
    target: &CanonicalWatermark,
) -> bool {
    applied.is_some_and(|applied| !target.is_newer_than(applied))
}

async fn apply_live_state_replay_scope_to_watermark(
    backend: &dyn LixBackend,
    scope: &LiveStateRebuildScope,
    target: &CanonicalWatermark,
) -> Result<(), LixError> {
    let mut transaction = backend.begin_transaction(TransactionMode::Write).await?;
    let apply_result = crate::live_state::rebuild_scope_in_transaction(
        transaction.as_mut(),
        &LiveStateRebuildRequest {
            scope: scope.clone(),
            debug: LiveStateRebuildDebugMode::Off,
            debug_row_limit: 0,
        },
    )
    .await;

    if let Err(error) = apply_result {
        let _ = transaction.rollback().await;
        return Err(error);
    }

    if let Err(error) = crate::live_state::mark_ready_at_canonical_watermark_in_transaction(
        transaction.as_mut(),
        target,
    )
    .await
    {
        let _ = transaction.rollback().await;
        return Err(error);
    }

    transaction.commit().await
}

async fn apply_legacy_version_ref_compat_mirrors_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_ref_updates: &[UpdatedVersionRef],
) -> Result<(), LixError> {
    if version_ref_updates.is_empty() {
        return Ok(());
    }
    let batch: UntrackedWriteBatch = version_ref_updates
        .iter()
        .map(legacy_compat_version_ref_mirror_write_row_from_update)
        .collect();
    UntrackedWriteParticipant::apply_write_batch(transaction, &batch).await
}

/// Legacy compatibility mirror row for older live-table readers.
///
/// Canonical committed version refs are read from canonical change history, not
/// from this untracked derived row shape.
pub(crate) fn legacy_compat_version_ref_mirror_write_row(
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> UntrackedWriteRow {
    UntrackedWriteRow {
        entity_id: version_id.to_string(),
        schema_key: version_ref_schema_key().to_string(),
        schema_version: version_ref_schema_version().to_string(),
        file_id: version_ref_file_id().to_string(),
        version_id: version_ref_storage_version_id().to_string(),
        global: true,
        plugin_key: version_ref_plugin_key().to_string(),
        metadata: None,
        writer_key: None,
        snapshot_content: Some(version_ref_snapshot_content(version_id, commit_id)),
        created_at: Some(timestamp.to_string()),
        updated_at: timestamp.to_string(),
        operation: UntrackedWriteOperation::Upsert,
    }
}

fn legacy_compat_version_ref_mirror_write_row_from_update(
    update: &UpdatedVersionRef,
) -> UntrackedWriteRow {
    legacy_compat_version_ref_mirror_write_row(
        update.version_id.as_str(),
        &update.commit_id,
        &update.created_at,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        apply_canonical_receipt_in_transaction,
        apply_commit_projections_best_effort_in_transaction, catch_up_live_state_to_watermark,
        projection_status, DerivedProjectionId, ProjectionCatchUpOutcome, ProjectionReplayMode,
        UpdatedVersionRef,
    };
    use crate::canonical::{CanonicalCommitReceipt, CanonicalWatermark};
    use crate::live_state::{LiveStateMode, LIVE_STATE_SCHEMA_EPOCH};
    use crate::test_support::boot_test_engine;
    use crate::{CreateVersionOptions, VersionId};
    use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, TransactionMode, Value};
    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeBackend {
        status_row: Option<Vec<Value>>,
        latest_watermark: Option<(i64, String, String)>,
        executed_sql: Mutex<Vec<String>>,
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> crate::SqlDialect {
            crate::SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.executed_sql.lock().unwrap().push(sql.to_string());
            if sql.contains("WITH status AS")
                && sql.contains("lix_internal_live_state_status")
                && sql.contains("lix_internal_change")
            {
                let mut row = self.status_row.clone().unwrap_or_else(|| {
                    vec![
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                    ]
                });
                match &self.latest_watermark {
                    Some((ordinal, id, created_at)) => {
                        row.push(Value::Integer(*ordinal));
                        row.push(Value::Text(id.clone()));
                        row.push(Value::Text(created_at.clone()));
                    }
                    None => {
                        row.push(Value::Null);
                        row.push(Value::Null);
                        row.push(Value::Null);
                    }
                }
                return Ok(QueryResult {
                    rows: vec![row],
                    columns: vec![
                        "mode".to_string(),
                        "latest_change_ordinal".to_string(),
                        "latest_change_id".to_string(),
                        "latest_change_created_at".to_string(),
                        "schema_epoch".to_string(),
                        "change_ordinal".to_string(),
                        "id".to_string(),
                        "created_at".to_string(),
                    ],
                });
            }
            if sql.contains("FROM lix_internal_live_state_status") {
                return Ok(QueryResult {
                    rows: self.status_row.clone().into_iter().collect(),
                    columns: vec![
                        "mode".to_string(),
                        "latest_change_ordinal".to_string(),
                        "latest_change_id".to_string(),
                        "latest_change_created_at".to_string(),
                        "schema_epoch".to_string(),
                    ],
                });
            }
            if sql.contains("FROM lix_internal_change")
                && sql.contains("ORDER BY change_ordinal DESC")
            {
                return Ok(QueryResult {
                    rows: self
                        .latest_watermark
                        .clone()
                        .into_iter()
                        .map(|(ordinal, id, created_at)| {
                            vec![
                                Value::Integer(ordinal),
                                Value::Text(id),
                                Value::Text(created_at),
                            ]
                        })
                        .collect(),
                    columns: vec![
                        "change_ordinal".to_string(),
                        "id".to_string(),
                        "created_at".to_string(),
                    ],
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            _mode: TransactionMode,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "transactions not used in fake backend",
            ))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "begin_savepoint not supported in test backend",
            ))
        }
    }

    struct FakeTransaction {
        status_row: Option<Vec<Value>>,
        latest_watermark: Option<(i64, String, String)>,
        executed_sql: Vec<String>,
        fail_legacy_version_ref_compat_mirror: bool,
    }

    #[async_trait(?Send)]
    impl LixBackendTransaction for FakeTransaction {
        fn dialect(&self) -> crate::SqlDialect {
            crate::SqlDialect::Sqlite
        }

        fn mode(&self) -> TransactionMode {
            TransactionMode::Write
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.executed_sql.push(sql.to_string());
            if self.fail_legacy_version_ref_compat_mirror
                && sql.contains("lix_version_ref")
                && sql.contains("INSERT INTO")
            {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "legacy version-ref compat mirror failed",
                ));
            }
            if sql.contains("WITH status AS")
                && sql.contains("lix_internal_live_state_status")
                && sql.contains("lix_internal_change")
            {
                let mut row = self.status_row.clone().unwrap_or_else(|| {
                    vec![
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                    ]
                });
                match &self.latest_watermark {
                    Some((ordinal, id, created_at)) => {
                        row.push(Value::Integer(*ordinal));
                        row.push(Value::Text(id.clone()));
                        row.push(Value::Text(created_at.clone()));
                    }
                    None => {
                        row.push(Value::Null);
                        row.push(Value::Null);
                        row.push(Value::Null);
                    }
                }
                return Ok(QueryResult {
                    rows: vec![row],
                    columns: vec![
                        "mode".to_string(),
                        "latest_change_ordinal".to_string(),
                        "latest_change_id".to_string(),
                        "latest_change_created_at".to_string(),
                        "schema_epoch".to_string(),
                        "change_ordinal".to_string(),
                        "id".to_string(),
                        "created_at".to_string(),
                    ],
                });
            }
            if sql.contains("FROM lix_internal_live_state_status") {
                return Ok(QueryResult {
                    rows: self.status_row.clone().into_iter().collect(),
                    columns: vec![
                        "mode".to_string(),
                        "latest_change_ordinal".to_string(),
                        "latest_change_id".to_string(),
                        "latest_change_created_at".to_string(),
                        "schema_epoch".to_string(),
                    ],
                });
            }
            if sql.contains("FROM lix_internal_change")
                && sql.contains("ORDER BY change_ordinal DESC")
            {
                return Ok(QueryResult {
                    rows: self
                        .latest_watermark
                        .clone()
                        .into_iter()
                        .map(|(ordinal, id, created_at)| {
                            vec![
                                Value::Integer(ordinal),
                                Value::Text(id),
                                Value::Text(created_at),
                            ]
                        })
                        .collect(),
                    columns: vec![
                        "change_ordinal".to_string(),
                        "id".to_string(),
                        "created_at".to_string(),
                    ],
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn projection_status_reports_live_state_applied_watermark() {
        let backend = FakeBackend {
            status_row: Some(vec![
                Value::Text("ready".to_string()),
                Value::Integer(1),
                Value::Text("change-1".to_string()),
                Value::Text("2026-03-15T01:02:02Z".to_string()),
                Value::Text(LIVE_STATE_SCHEMA_EPOCH.to_string()),
            ]),
            latest_watermark: Some((
                2,
                "change-2".to_string(),
                "2026-03-15T01:02:03Z".to_string(),
            )),
            executed_sql: Mutex::new(Vec::new()),
        };

        let status = projection_status(&backend)
            .await
            .expect("projection status should load");
        assert_eq!(status.projections.len(), 1);
        let live_state = &status.projections[0];
        assert_eq!(live_state.projection, DerivedProjectionId::LiveState);
        assert_eq!(live_state.mode, ProjectionReplayMode::Ready);
        assert_eq!(
            live_state.applied_watermark,
            Some(CanonicalWatermark {
                change_ordinal: 1,
                change_id: "change-1".to_string(),
                created_at: "2026-03-15T01:02:02Z".to_string(),
            })
        );
        assert_eq!(
            live_state.latest_canonical_watermark,
            Some(CanonicalWatermark {
                change_ordinal: 2,
                change_id: "change-2".to_string(),
                created_at: "2026-03-15T01:02:03Z".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn apply_canonical_receipt_routes_commit_boundary_through_projection_layer() {
        let mut transaction = FakeTransaction {
            status_row: Some(vec![
                Value::Text("ready".to_string()),
                Value::Integer(1),
                Value::Text("change-1".to_string()),
                Value::Text("2026-03-15T01:02:02Z".to_string()),
                Value::Text(LIVE_STATE_SCHEMA_EPOCH.to_string()),
            ]),
            latest_watermark: Some((
                1,
                "change-1".to_string(),
                "2026-03-15T01:02:02Z".to_string(),
            )),
            executed_sql: Vec::new(),
            fail_legacy_version_ref_compat_mirror: false,
        };
        let receipt = CanonicalCommitReceipt {
            commit_id: "commit-2".to_string(),
            canonical_watermark: CanonicalWatermark {
                change_ordinal: 2,
                change_id: "change-2".to_string(),
                created_at: "2026-03-15T01:02:03Z".to_string(),
            },
            updated_version_refs: Vec::new(),
            affected_versions: vec!["main".to_string()],
        };

        apply_canonical_receipt_in_transaction(&mut transaction, &receipt)
            .await
            .expect("projection layer should apply canonical receipt");

        assert!(transaction.executed_sql.iter().any(|sql| {
            sql.contains("INSERT INTO lix_internal_live_state_status ")
                && sql.contains("'ready'")
                && sql.contains("'change-2'")
        }));
    }

    #[tokio::test]
    async fn legacy_version_ref_compat_mirror_failures_do_not_block_projection_application() {
        let mut transaction = FakeTransaction {
            status_row: None,
            latest_watermark: None,
            executed_sql: Vec::new(),
            fail_legacy_version_ref_compat_mirror: true,
        };
        let receipt = CanonicalCommitReceipt {
            commit_id: "commit-2".to_string(),
            canonical_watermark: CanonicalWatermark {
                change_ordinal: 2,
                change_id: "change-2".to_string(),
                created_at: "2026-03-15T01:02:03Z".to_string(),
            },
            updated_version_refs: vec![UpdatedVersionRef {
                version_id: VersionId::new("main").expect("valid version id"),
                commit_id: "commit-2".to_string(),
                created_at: "2026-03-15T01:02:03Z".to_string(),
            }],
            affected_versions: Vec::new(),
        };

        apply_commit_projections_best_effort_in_transaction(
            &mut transaction,
            &receipt,
            &BTreeMap::new(),
        )
        .await
        .expect("legacy compat mirror failures should not block projection application");

        assert!(transaction
            .executed_sql
            .iter()
            .any(|sql| sql.contains("lix_version_ref")));
    }

    #[tokio::test]
    async fn catch_up_is_noop_when_target_watermark_is_already_applied() {
        let backend = FakeBackend {
            status_row: Some(vec![
                Value::Text("ready".to_string()),
                Value::Integer(2),
                Value::Text("change-2".to_string()),
                Value::Text("2026-03-15T01:02:03Z".to_string()),
                Value::Text(LIVE_STATE_SCHEMA_EPOCH.to_string()),
            ]),
            latest_watermark: Some((
                2,
                "change-2".to_string(),
                "2026-03-15T01:02:03Z".to_string(),
            )),
            executed_sql: Mutex::new(Vec::new()),
        };
        let target = CanonicalWatermark {
            change_ordinal: 2,
            change_id: "change-2".to_string(),
            created_at: "2026-03-15T01:02:03Z".to_string(),
        };

        let report = catch_up_live_state_to_watermark(&backend, &target)
            .await
            .expect("up-to-date catch-up should not rebuild");
        assert_eq!(report.projection, DerivedProjectionId::LiveState);
        assert_eq!(report.outcome, ProjectionCatchUpOutcome::AlreadyApplied);
        assert_eq!(report.final_watermark, Some(target));
        assert_eq!(report.full_rebuild_passes, 0);
        assert_eq!(report.delta_merge_passes, 0);
    }

    #[test]
    fn catch_up_merges_newer_canonical_version_ref_deltas_without_full_rebuild() {
        run_checkpoint_test(|| async {
            let (backend, _engine, session) = boot_test_engine()
                .await
                .expect("boot test engine should succeed");

            session
                .create_version(CreateVersionOptions {
                    id: Some("version-b".to_string()),
                    name: Some("version-b".to_string()),
                    ..CreateVersionOptions::default()
                })
                .await
                .expect("create version-b should succeed");
            let watermark_before_delta =
                crate::live_state::load_latest_canonical_watermark(&backend)
                    .await
                    .expect("canonical watermark lookup should succeed")
                    .expect("version-b creation should produce a canonical watermark");

            session
                .create_version(CreateVersionOptions {
                    id: Some("version-c".to_string()),
                    name: Some("version-c".to_string()),
                    ..CreateVersionOptions::default()
                })
                .await
                .expect("create version-c should succeed");
            let latest_watermark = crate::live_state::load_latest_canonical_watermark(&backend)
                .await
                .expect("latest canonical watermark lookup should succeed")
                .expect("version-c creation should produce a canonical watermark");

            let mut transaction = backend
                .begin_transaction(TransactionMode::Write)
                .await
                .expect("staleness transaction should open");
            crate::live_state::mark_needs_rebuild_at_canonical_watermark_in_transaction(
                transaction.as_mut(),
                &watermark_before_delta,
            )
            .await
            .expect("marking live_state stale should succeed");
            transaction
                .commit()
                .await
                .expect("staleness transaction should commit");

            let report = catch_up_live_state_to_watermark(&backend, &watermark_before_delta)
                .await
                .expect("catch-up should merge newer canonical deltas");

            let projection_status =
                crate::live_state::load_projection_status_with_backend(&backend)
                    .await
                    .expect("projection status should load after catch-up");
            assert_eq!(projection_status.mode, LiveStateMode::Ready);
            assert_eq!(
                projection_status.applied_watermark,
                Some(latest_watermark.clone())
            );
            assert_eq!(report.projection, DerivedProjectionId::LiveState);
            assert_eq!(report.outcome, ProjectionCatchUpOutcome::RebuiltToTarget);
            assert_eq!(report.final_watermark, Some(latest_watermark.clone()));
            assert_eq!(report.target_watermark, latest_watermark);
            assert_eq!(report.full_rebuild_passes, 0);
            assert_eq!(report.delta_merge_passes, 1);
        });
    }

    fn run_checkpoint_test<Factory, Future>(factory: Factory)
    where
        Factory: FnOnce() -> Future + Send + 'static,
        Future: std::future::Future<Output = ()> + 'static,
    {
        std::thread::Builder::new()
            .name("live-state-projection-test".to_string())
            .stack_size(64 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("live_state projection test runtime should build")
                    .block_on(factory());
            })
            .expect("live_state projection test thread should spawn")
            .join()
            .expect("live_state projection test thread should join");
    }
}
