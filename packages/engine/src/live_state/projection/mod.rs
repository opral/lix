//! Projection replay orchestration for derived query surfaces.
//!
//! Replay status tracked here is operational and replica-local. Replay cursors
//! describe how this engine instance resumes scanning local canonical storage.
//! Freshness for committed-state projections is defined by committed frontiers,
//! not by local append position.
//!
//! If this state is lost, the engine may need to rescan or rebuild projections.
//! Canonical meaning remains recoverable from `canonical/*`.

pub(crate) mod replay;
pub(crate) mod status;

use std::collections::{BTreeMap, BTreeSet};

use crate::canonical::receipt::UpdatedVersionRef;
use crate::canonical::CanonicalCommitReceipt;
use crate::errors::classification::is_missing_relation_error;
use crate::live_state::shared::identity::RowIdentity;
use crate::live_state::untracked::{
    UntrackedWriteBatch, UntrackedWriteOperation, UntrackedWriteParticipant, UntrackedWriteRow,
};
use crate::live_state::ReplayCursor;
use crate::live_state::{
    LiveStateMode, LiveStateProjectionStatus, LiveStateRebuildDebugMode, LiveStateRebuildRequest,
    LiveStateRebuildScope,
};
use crate::version::{
    version_ref_file_id, version_ref_plugin_key, version_ref_schema_key,
    version_ref_schema_version, version_ref_snapshot_content, version_ref_storage_version_id,
};
use crate::{
    CommittedVersionFrontier, LixBackend, LixBackendTransaction, LixError, TransactionMode, Value,
};

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
    /// Local replay cursor used to resume canonical scans for this projection.
    pub applied_cursor: Option<ReplayCursor>,
    /// Current local canonical storage head observed by this engine instance.
    pub latest_cursor: Option<ReplayCursor>,
    /// Semantic frontier currently served by this projection.
    pub applied_committed_frontier: Option<CommittedVersionFrontier>,
    /// Current committed frontier resolved from canonical refs.
    pub current_committed_frontier: CommittedVersionFrontier,
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
    pub(crate) starting_cursor: Option<ReplayCursor>,
    pub(crate) target_cursor: Option<ReplayCursor>,
    pub(crate) final_cursor: Option<ReplayCursor>,
    pub(crate) starting_frontier: Option<CommittedVersionFrontier>,
    pub(crate) target_frontier: CommittedVersionFrontier,
    pub(crate) final_frontier: Option<CommittedVersionFrontier>,
    pub(crate) full_rebuild_passes: usize,
    pub(crate) delta_merge_passes: usize,
}

pub(crate) async fn projection_status(
    backend: &dyn LixBackend,
) -> Result<ProjectionStatus, LixError> {
    Ok(ProjectionStatus {
        projections: vec![derived_projection_status_from_live_state(
            status::load_live_state_projection_status_with_backend(backend).await?,
        )],
    })
}

pub(crate) async fn apply_canonical_receipt_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    receipt: &CanonicalCommitReceipt,
) -> Result<(), LixError> {
    replay::advance_live_state_projection_replay_boundary_to_cursor_in_transaction(
        transaction,
        &receipt.replay_cursor,
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
        replay::mark_live_state_projection_needs_rebuild_at_replay_cursor_in_transaction(
            transaction,
            &receipt.replay_cursor,
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
        replay::mark_live_state_projection_needs_rebuild_at_replay_cursor_in_transaction(
            transaction,
            &receipt.replay_cursor,
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
) -> Result<ReplayCursor, LixError> {
    crate::live_state::finalize_commit_in_transaction(transaction).await
}

pub(crate) async fn mark_live_state_projection_ready_with_backend(
    backend: &dyn LixBackend,
    cursor: &ReplayCursor,
) -> Result<(), LixError> {
    replay::mark_live_state_projection_ready_with_backend(backend, cursor).await
}

pub(crate) async fn catch_up_live_state_to_current_frontier(
    backend: &dyn LixBackend,
) -> Result<ProjectionCatchUpReport, LixError> {
    let starting_status = status::load_live_state_projection_status_with_backend(backend).await?;
    let starting_cursor = starting_status.applied_cursor.clone();
    let starting_frontier = starting_status.applied_committed_frontier.clone();
    let mut full_rebuild_passes = 0usize;
    let mut delta_merge_passes = 0usize;

    for _ in 0..=MAX_LIVE_STATE_DELTA_MERGE_PASSES {
        let projection_status =
            status::load_live_state_projection_status_with_backend(backend).await?;
        let target_cursor = projection_status.latest_cursor.clone();
        let target_frontier = projection_status.current_committed_frontier.clone();
        if live_state_projection_serves_current_frontier(&projection_status) {
            return Ok(ProjectionCatchUpReport {
                projection: DerivedProjectionId::LiveState,
                outcome: if full_rebuild_passes == 0 && delta_merge_passes == 0 {
                    ProjectionCatchUpOutcome::AlreadyApplied
                } else {
                    ProjectionCatchUpOutcome::RebuiltToTarget
                },
                starting_cursor,
                target_cursor,
                final_cursor: projection_status.applied_cursor,
                starting_frontier,
                target_frontier,
                final_frontier: projection_status.applied_committed_frontier,
                full_rebuild_passes,
                delta_merge_passes,
            });
        }

        let scope = match (
            projection_status.applied_cursor.as_ref(),
            projection_status.latest_cursor.as_ref(),
        ) {
            (Some(applied), Some(target_cursor)) if target_cursor.is_newer_than(applied) => {
                match resolve_incremental_live_state_scope(backend, applied, target_cursor).await? {
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
            _ => {
                full_rebuild_passes += 1;
                LiveStateRebuildScope::Full
            }
        };

        apply_live_state_replay_scope_to_cursor(
            backend,
            &scope,
            projection_status.latest_cursor.as_ref(),
        )
        .await?;
    }

    Err(LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!(
            "live_state catch-up exceeded {} delta-merge passes without converging on the current committed frontier",
            MAX_LIVE_STATE_DELTA_MERGE_PASSES
        ),
    ))
}

async fn resolve_incremental_live_state_scope(
    backend: &dyn LixBackend,
    older_exclusive: &ReplayCursor,
    newer_inclusive: &ReplayCursor,
) -> Result<Option<BTreeSet<String>>, LixError> {
    if let Some(version_ids) = load_version_ids_with_ref_changes_between_cursors_with_backend(
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

async fn load_version_ids_with_ref_changes_between_cursors_with_backend(
    backend: &dyn LixBackend,
    older_exclusive: &ReplayCursor,
    newer_inclusive: &ReplayCursor,
) -> Result<Option<BTreeSet<String>>, LixError> {
    if !newer_inclusive.is_newer_than(older_exclusive) {
        return Ok(Some(BTreeSet::new()));
    }

    let sql = format!(
        "SELECT DISTINCT c.entity_id \
         FROM lix_internal_change c \
         WHERE c.schema_key = '{schema_key}' \
           AND c.schema_version = '{schema_version}' \
           AND c.file_id = '{file_id}' \
           AND c.plugin_key = '{plugin_key}' \
           AND (\
             c.created_at > '{older_created_at}' \
             OR (c.created_at = '{older_created_at}' AND c.id > '{older_change_id}')\
           ) \
           AND (\
             c.created_at < '{newer_created_at}' \
             OR (c.created_at = '{newer_created_at}' AND c.id <= '{newer_change_id}')\
           ) \
         ORDER BY c.entity_id ASC",
        schema_key = escape_sql_string(version_ref_schema_key()),
        schema_version = escape_sql_string(version_ref_schema_version()),
        file_id = escape_sql_string(version_ref_file_id()),
        plugin_key = escape_sql_string(version_ref_plugin_key()),
        older_created_at = escape_sql_string(&older_exclusive.created_at),
        older_change_id = escape_sql_string(&older_exclusive.change_id),
        newer_created_at = escape_sql_string(&newer_inclusive.created_at),
        newer_change_id = escape_sql_string(&newer_inclusive.change_id),
    );

    let result = match backend.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(error) if is_missing_relation_error(&error) => return Ok(None),
        Err(error) => return Err(error),
    };

    let mut version_ids = BTreeSet::new();
    for row in &result.rows {
        match row.first() {
            Some(Value::Text(version_id)) if !version_id.is_empty() => {
                version_ids.insert(version_id.clone());
            }
            Some(Value::Null) | None => {}
            Some(other) => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("version ref replay query returned non-text entity_id: {other:?}"),
                ));
            }
        }
    }

    Ok(Some(version_ids))
}

fn derived_projection_status_from_live_state(
    status: LiveStateProjectionStatus,
) -> DerivedProjectionStatus {
    DerivedProjectionStatus {
        projection: DerivedProjectionId::LiveState,
        mode: status.mode.into(),
        applied_cursor: status.applied_cursor,
        latest_cursor: status.latest_cursor,
        applied_committed_frontier: status.applied_committed_frontier,
        current_committed_frontier: status.current_committed_frontier,
    }
}

fn live_state_projection_serves_current_frontier(status: &LiveStateProjectionStatus) -> bool {
    status.mode == LiveStateMode::Ready
        && status.applied_committed_frontier.as_ref() == Some(&status.current_committed_frontier)
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

async fn apply_live_state_replay_scope_to_cursor(
    backend: &dyn LixBackend,
    scope: &LiveStateRebuildScope,
    target: Option<&ReplayCursor>,
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

    let ready_result = match target {
        Some(target) => {
            replay::mark_live_state_projection_ready_at_replay_cursor_in_transaction(
                transaction.as_mut(),
                target,
            )
            .await
        }
        None => {
            replay::mark_live_state_projection_ready_without_replay_cursor_in_transaction(
                transaction.as_mut(),
            )
            .await
        }
    };

    if let Err(error) = ready_result {
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
        apply_commit_projections_best_effort_in_transaction,
        catch_up_live_state_to_current_frontier,
        projection_status, replay, status, DerivedProjectionId, ProjectionCatchUpOutcome,
        ProjectionReplayMode, UpdatedVersionRef,
    };
    use crate::canonical::CanonicalCommitReceipt;
    use crate::live_state::ReplayCursor;
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
        latest_cursor: Option<(String, String)>,
        version_heads: BTreeMap<String, String>,
        executed_sql: Mutex<Vec<String>>,
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> crate::SqlDialect {
            crate::SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.executed_sql.lock().unwrap().push(sql.to_string());
            if is_committed_version_frontier_query(sql) {
                return Ok(QueryResult {
                    rows: version_ref_rows(&self.version_heads),
                    columns: vec!["version_id".to_string(), "commit_id".to_string()],
                });
            }
            if sql.contains("FROM lix_internal_live_state_status") {
                return Ok(QueryResult {
                    rows: self.status_row.clone().into_iter().collect(),
                    columns: vec![
                        "mode".to_string(),
                        "latest_change_id".to_string(),
                        "latest_change_created_at".to_string(),
                        "schema_epoch".to_string(),
                        "applied_committed_frontier".to_string(),
                    ],
                });
            }
            if sql.contains("FROM lix_internal_change")
                && sql.contains("ORDER BY created_at DESC, id DESC")
            {
                return Ok(QueryResult {
                    rows: self
                        .latest_cursor
                        .clone()
                        .into_iter()
                        .map(|(id, created_at)| vec![Value::Text(id), Value::Text(created_at)])
                        .collect(),
                    columns: vec!["id".to_string(), "created_at".to_string()],
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
        latest_cursor: Option<(String, String)>,
        version_heads: BTreeMap<String, String>,
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
            if is_committed_version_frontier_query(sql) {
                return Ok(QueryResult {
                    rows: version_ref_rows(&self.version_heads),
                    columns: vec!["version_id".to_string(), "commit_id".to_string()],
                });
            }
            if sql.contains("FROM lix_internal_live_state_status") {
                return Ok(QueryResult {
                    rows: self.status_row.clone().into_iter().collect(),
                    columns: vec![
                        "mode".to_string(),
                        "latest_change_id".to_string(),
                        "latest_change_created_at".to_string(),
                        "schema_epoch".to_string(),
                        "applied_committed_frontier".to_string(),
                    ],
                });
            }
            if sql.contains("FROM lix_internal_change")
                && sql.contains("ORDER BY created_at DESC, id DESC")
            {
                return Ok(QueryResult {
                    rows: self
                        .latest_cursor
                        .clone()
                        .into_iter()
                        .map(|(id, created_at)| vec![Value::Text(id), Value::Text(created_at)])
                        .collect(),
                    columns: vec!["id".to_string(), "created_at".to_string()],
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

    fn is_committed_version_frontier_query(sql: &str) -> bool {
        sql.contains("version_ref_facts")
            && sql.contains("current_refs")
            && sql.contains("SELECT version_id, commit_id")
    }

    fn version_ref_rows(version_heads: &BTreeMap<String, String>) -> Vec<Vec<Value>> {
        version_heads
            .iter()
            .map(|(version_id, commit_id)| {
                vec![
                    Value::Text(version_id.clone()),
                    Value::Text(commit_id.clone()),
                ]
            })
            .collect()
    }

    fn frontier_json(entries: &[(&str, &str)]) -> String {
        crate::CommittedVersionFrontier {
            version_heads: entries
                .iter()
                .map(|(version_id, commit_id)| {
                    ((*version_id).to_string(), (*commit_id).to_string())
                })
                .collect(),
        }
        .to_json_string()
    }

    #[tokio::test]
    async fn projection_status_reports_live_state_applied_cursor() {
        let backend = FakeBackend {
            status_row: Some(vec![
                Value::Text("ready".to_string()),
                Value::Text("change-1".to_string()),
                Value::Text("2026-03-15T01:02:02Z".to_string()),
                Value::Text(LIVE_STATE_SCHEMA_EPOCH.to_string()),
                Value::Text(frontier_json(&[("main", "commit-1")])),
            ]),
            latest_cursor: Some(("change-2".to_string(), "2026-03-15T01:02:03Z".to_string())),
            version_heads: BTreeMap::from([("main".to_string(), "commit-1".to_string())]),
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
            live_state.applied_cursor,
            Some(ReplayCursor::new("change-1", "2026-03-15T01:02:02Z"))
        );
        assert_eq!(
            live_state.latest_cursor,
            Some(ReplayCursor::new("change-2", "2026-03-15T01:02:03Z"))
        );
        assert_eq!(
            live_state.applied_committed_frontier,
            Some(crate::CommittedVersionFrontier {
                version_heads: BTreeMap::from([("main".to_string(), "commit-1".to_string())]),
            })
        );
        assert_eq!(
            live_state.current_committed_frontier,
            crate::CommittedVersionFrontier {
                version_heads: BTreeMap::from([("main".to_string(), "commit-1".to_string())]),
            }
        );
    }

    #[tokio::test]
    async fn apply_canonical_receipt_routes_commit_boundary_through_projection_layer() {
        let mut transaction = FakeTransaction {
            status_row: Some(vec![
                Value::Text("ready".to_string()),
                Value::Text("change-1".to_string()),
                Value::Text("2026-03-15T01:02:02Z".to_string()),
                Value::Text(LIVE_STATE_SCHEMA_EPOCH.to_string()),
                Value::Text(frontier_json(&[("main", "commit-1")])),
            ]),
            latest_cursor: Some(("change-1".to_string(), "2026-03-15T01:02:02Z".to_string())),
            version_heads: BTreeMap::from([("main".to_string(), "commit-2".to_string())]),
            executed_sql: Vec::new(),
            fail_legacy_version_ref_compat_mirror: false,
        };
        let receipt = CanonicalCommitReceipt {
            commit_id: "commit-2".to_string(),
            replay_cursor: ReplayCursor::new("change-2", "2026-03-15T01:02:03Z"),
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
                && sql.contains("commit-2")
        }));
    }

    #[tokio::test]
    async fn legacy_version_ref_compat_mirror_failures_do_not_block_projection_application() {
        let mut transaction = FakeTransaction {
            status_row: None,
            latest_cursor: None,
            version_heads: BTreeMap::new(),
            executed_sql: Vec::new(),
            fail_legacy_version_ref_compat_mirror: true,
        };
        let receipt = CanonicalCommitReceipt {
            commit_id: "commit-2".to_string(),
            replay_cursor: ReplayCursor::new("change-2", "2026-03-15T01:02:03Z"),
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
    async fn catch_up_is_noop_when_frontier_is_already_applied() {
        let backend = FakeBackend {
            status_row: Some(vec![
                Value::Text("ready".to_string()),
                Value::Text("change-1".to_string()),
                Value::Text("2026-03-15T01:02:02Z".to_string()),
                Value::Text(LIVE_STATE_SCHEMA_EPOCH.to_string()),
                Value::Text(frontier_json(&[("main", "commit-2")])),
            ]),
            latest_cursor: Some(("change-2".to_string(), "2026-03-15T01:02:03Z".to_string())),
            version_heads: BTreeMap::from([("main".to_string(), "commit-2".to_string())]),
            executed_sql: Mutex::new(Vec::new()),
        };

        let report = catch_up_live_state_to_current_frontier(&backend)
            .await
            .expect("up-to-date frontier catch-up should not rebuild");
        assert_eq!(report.projection, DerivedProjectionId::LiveState);
        assert_eq!(report.outcome, ProjectionCatchUpOutcome::AlreadyApplied);
        assert_eq!(
            report.final_cursor,
            Some(ReplayCursor::new("change-1", "2026-03-15T01:02:02Z"))
        );
        assert_eq!(
            report.target_cursor,
            Some(ReplayCursor::new("change-2", "2026-03-15T01:02:03Z"))
        );
        assert_eq!(
            report.final_frontier,
            Some(crate::CommittedVersionFrontier {
                version_heads: BTreeMap::from([("main".to_string(), "commit-2".to_string())]),
            })
        );
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
            let cursor_before_delta =
                replay::load_latest_live_state_replay_cursor_with_backend(&backend)
                    .await
                    .expect("latest replay cursor lookup should succeed")
                    .expect("version-b creation should produce a replay cursor");

            session
                .create_version(CreateVersionOptions {
                    id: Some("version-c".to_string()),
                    name: Some("version-c".to_string()),
                    ..CreateVersionOptions::default()
                })
                .await
                .expect("create version-c should succeed");
            let latest_cursor = replay::load_latest_live_state_replay_cursor_with_backend(&backend)
                .await
                .expect("latest replay cursor lookup should succeed")
                .expect("version-c creation should produce a replay cursor");

            let mut transaction = backend
                .begin_transaction(TransactionMode::Write)
                .await
                .expect("staleness transaction should open");
            replay::mark_live_state_projection_needs_rebuild_at_replay_cursor_in_transaction(
                transaction.as_mut(),
                &cursor_before_delta,
            )
            .await
            .expect("marking live_state stale should succeed");
            transaction
                .commit()
                .await
                .expect("staleness transaction should commit");

            let report = catch_up_live_state_to_current_frontier(&backend)
                .await
                .expect("catch-up should merge newer replay deltas");

            let projection_status =
                status::load_live_state_projection_status_with_backend(&backend)
                    .await
                    .expect("projection status should load after catch-up");
            assert_eq!(projection_status.mode, LiveStateMode::Ready);
            assert_eq!(
                projection_status.applied_cursor,
                Some(latest_cursor.clone())
            );
            assert_eq!(report.projection, DerivedProjectionId::LiveState);
            assert_eq!(report.outcome, ProjectionCatchUpOutcome::RebuiltToTarget);
            assert_eq!(report.final_cursor, Some(latest_cursor.clone()));
            assert_eq!(report.target_cursor, Some(latest_cursor));
            assert_eq!(report.full_rebuild_passes, 0);
            assert_eq!(report.delta_merge_passes, 1);
        });
    }

    #[test]
    fn catch_up_full_rebuilds_when_projection_replay_state_is_lost() {
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

            replay::mark_live_state_projection_replay_state_lost_with_backend(&backend)
                .await
                .expect("marking projection replay state lost should succeed");

            let report = catch_up_live_state_to_current_frontier(&backend)
                .await
                .expect("catch-up should recover from lost replay state");

            let projection_status =
                status::load_live_state_projection_status_with_backend(&backend)
                    .await
                    .expect("projection status should load after recovery");
            assert_eq!(projection_status.mode, LiveStateMode::Ready);
            assert!(
                projection_status.applied_cursor.is_some(),
                "full rebuild recovery should restamp a replay cursor"
            );
            assert_eq!(report.outcome, ProjectionCatchUpOutcome::RebuiltToTarget);
            assert_eq!(report.full_rebuild_passes, 1);
            assert_eq!(report.delta_merge_passes, 0);
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
