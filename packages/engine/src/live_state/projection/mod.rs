//! Projection replay orchestration for derived query surfaces.
//!
//! Replay status tracked here is operational and replica-local. Replay cursors
//! describe how this engine instance resumes scanning local canonical storage.
//! Freshness for committed-state projections is defined by committed frontiers,
//! not by local append position.
//!
//! If this state is lost, the engine may need to rescan or rebuild projections.
//! Canonical meaning remains recoverable from `canonical/*`.

pub(crate) mod dispatch;
pub(crate) mod hydration;
pub(crate) mod replay;
pub(crate) mod status;

use crate::live_state::store::LiveStateTransactionRef;
use crate::live_state::{
    CanonicalCommitProjectionReceipt, LiveStateMode, LiveStateProjectionStatus, ReplayCursor,
};
use crate::schema::LixVersionRef;
use crate::version::CommittedVersionFrontier;
use crate::LixError;

#[cfg(test)]
use std::collections::BTreeSet;

#[cfg(test)]
use crate::live_state::store::LiveStateBackendRef;
#[cfg(test)]
use crate::live_state::store_sql::begin_write_transaction;
#[cfg(test)]
use crate::live_state::{
    LiveStateRebuildDebugMode, LiveStateRebuildRequest, LiveStateRebuildScope,
};

#[cfg(test)]
const MAX_LIVE_STATE_DELTA_MERGE_PASSES: usize = 16;

fn version_ref_schema_key() -> String {
    crate::version::version_ref_schema_key().to_string()
}

fn version_ref_schema_version() -> String {
    crate::version::version_ref_schema_version().to_string()
}

fn version_ref_file_id() -> Option<String> {
    crate::version::version_ref_file_id().map(str::to_string)
}

fn version_ref_plugin_key() -> Option<String> {
    crate::version::version_ref_plugin_key().map(str::to_string)
}

fn version_ref_storage_version_id() -> String {
    crate::version::version_ref_storage_version_id().to_string()
}

fn version_ref_snapshot_content(version_id: &str, commit_id: &str) -> String {
    serde_json::to_string(&LixVersionRef {
        id: version_id.to_string(),
        commit_id: commit_id.to_string(),
    })
    .expect("lix_version_ref snapshot serialization must succeed")
}

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
    /// Current committed frontier resolved from replica-local version heads.
    pub current_committed_frontier: CommittedVersionFrontier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionStatus {
    pub projections: Vec<DerivedProjectionStatus>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProjectionCatchUpOutcome {
    AlreadyApplied,
    RebuiltToTarget,
}

#[cfg(test)]
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

pub(crate) fn projection_status_from_live_state(
    status: LiveStateProjectionStatus,
) -> ProjectionStatus {
    ProjectionStatus {
        projections: vec![derived_projection_status_from_live_state(status)],
    }
}

pub(crate) async fn apply_canonical_receipt_in_transaction(
    transaction: LiveStateTransactionRef<'_>,
    receipt: &CanonicalCommitProjectionReceipt,
) -> Result<(), LixError> {
    replay::advance_live_state_projection_replay_boundary_to_cursor_in_transaction(
        transaction,
        &receipt.replay_cursor,
    )
    .await
}

pub(crate) async fn mark_live_state_projection_ready_in_transaction(
    transaction: LiveStateTransactionRef<'_>,
) -> Result<ReplayCursor, LixError> {
    crate::live_state::mark_live_state_ready_at_latest_replay_cursor_in_transaction(transaction)
        .await
}

#[cfg(test)]
pub(crate) async fn catch_up_live_state_to_current_frontier(
    backend: LiveStateBackendRef<'_>,
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

        if live_state_projection_needs_replay_recovery(&projection_status) {
            recover_live_state_projection_replay_state(
                backend,
                projection_status.latest_cursor.as_ref(),
            )
            .await?;
            continue;
        }

        let scope = match changed_version_ids_between_frontiers(
            projection_status.applied_committed_frontier.as_ref(),
            &projection_status.current_committed_frontier,
        ) {
            Some(version_ids) if !version_ids.is_empty() => {
                delta_merge_passes += 1;
                LiveStateRebuildScope::Versions(version_ids)
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

#[cfg(test)]
fn live_state_projection_needs_replay_recovery(status: &LiveStateProjectionStatus) -> bool {
    status.mode == LiveStateMode::NeedsRebuild
        && status.applied_committed_frontier.as_ref() == Some(&status.current_committed_frontier)
}

#[cfg(test)]
fn changed_version_ids_between_frontiers(
    applied_frontier: Option<&CommittedVersionFrontier>,
    current_frontier: &CommittedVersionFrontier,
) -> Option<BTreeSet<String>> {
    let applied_frontier = applied_frontier?;
    let version_ids = applied_frontier
        .version_heads
        .keys()
        .chain(current_frontier.version_heads.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    Some(
        version_ids
            .into_iter()
            .filter(|version_id| {
                applied_frontier.version_heads.get(version_id)
                    != current_frontier.version_heads.get(version_id)
            })
            .collect(),
    )
}

#[cfg(test)]
async fn recover_live_state_projection_replay_state(
    backend: LiveStateBackendRef<'_>,
    target: Option<&ReplayCursor>,
) -> Result<(), LixError> {
    let mut transaction = begin_write_transaction(backend).await?;
    let recovery_result = match target {
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

    if let Err(error) = recovery_result {
        let _ = transaction.rollback().await;
        return Err(error);
    }

    transaction.commit().await
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

#[cfg(test)]
fn live_state_projection_serves_current_frontier(status: &LiveStateProjectionStatus) -> bool {
    status.mode == LiveStateMode::Ready
        && status.applied_committed_frontier.as_ref() == Some(&status.current_committed_frontier)
}

#[cfg(test)]
async fn apply_live_state_replay_scope_to_cursor(
    backend: LiveStateBackendRef<'_>,
    scope: &LiveStateRebuildScope,
    target: Option<&ReplayCursor>,
) -> Result<(), LixError> {
    let mut transaction = begin_write_transaction(backend).await?;
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

#[cfg(test)]
mod tests {
    use super::{
        apply_canonical_receipt_in_transaction, catch_up_live_state_to_current_frontier, replay,
        status, DerivedProjectionId, ProjectionCatchUpOutcome, ProjectionReplayMode,
    };
    use crate::canonical::CanonicalCommitReceipt;
    use crate::live_state::{CanonicalCommitProjectionReceipt, LiveStateMode, ReplayCursor};
    use crate::test_support::{
        boot_test_engine, init_test_backend_core, seed_canonical_change_row,
        seed_live_state_status_row, seed_local_version_head, CanonicalChangeSeed,
        TestSqliteBackend,
    };
    use crate::CommittedVersionFrontier;
    use crate::CreateVersionOptions;
    use std::collections::BTreeMap;

    async fn init_projection_backend() -> TestSqliteBackend {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");
        backend
    }

    async fn seed_latest_replay_cursor(
        backend: &TestSqliteBackend,
        change_id: &str,
        created_at: &str,
    ) {
        seed_canonical_change_row(
            backend,
            CanonicalChangeSeed {
                id: change_id,
                entity_id: "cursor-entity",
                schema_key: "lix_key_value",
                schema_version: "1",
                file_id: None,
                plugin_key: None,
                snapshot_id: "no-content",
                snapshot_content: None,
                metadata: None,
                created_at,
            },
        )
        .await
        .expect("latest replay cursor row should seed");
    }

    fn frontier(entries: &[(&str, &str)]) -> CommittedVersionFrontier {
        CommittedVersionFrontier {
            version_heads: entries
                .iter()
                .map(|(version_id, commit_id)| {
                    ((*version_id).to_string(), (*commit_id).to_string())
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn projection_status_reports_live_state_applied_cursor() {
        let backend = init_projection_backend().await;
        seed_local_version_head(&backend, "main", "commit-1", "2026-03-15T01:02:02Z")
            .await
            .expect("local version head should seed");
        seed_latest_replay_cursor(&backend, "change-2", "2026-03-15T01:02:03Z").await;
        seed_live_state_status_row(
            &backend,
            LiveStateMode::Ready,
            Some(&ReplayCursor::new("change-1", "2026-03-15T01:02:02Z")),
            Some(&frontier(&[("main", "commit-1")])),
            "2026-03-15T01:02:03Z",
        )
        .await
        .expect("status row should seed");

        let status = crate::live_state::projection_status(&backend)
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
        let backend = init_projection_backend().await;
        seed_local_version_head(&backend, "main", "commit-2", "2026-03-15T01:02:03Z")
            .await
            .expect("local version head should seed");
        seed_live_state_status_row(
            &backend,
            LiveStateMode::Ready,
            Some(&ReplayCursor::new("change-1", "2026-03-15T01:02:02Z")),
            Some(&frontier(&[("main", "commit-1")])),
            "2026-03-15T01:02:02Z",
        )
        .await
        .expect("status row should seed");
        backend.clear_query_log();
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should begin");
        let receipt = CanonicalCommitProjectionReceipt::new(
            CanonicalCommitReceipt {
                commit_id: "commit-2".to_string(),
                updated_version_refs: Vec::new(),
                affected_versions: vec!["main".to_string()],
            },
            ReplayCursor::new("change-2", "2026-03-15T01:02:03Z"),
        );

        apply_canonical_receipt_in_transaction(transaction.as_mut(), &receipt)
            .await
            .expect("projection layer should apply canonical receipt");

        assert!(backend.executed_sql().iter().any(|sql| {
            sql.contains("INSERT INTO lix_internal_live_state_status ")
                && sql.contains("'ready'")
                && sql.contains("'change-2'")
                && sql.contains("commit-2")
        }));
        transaction
            .rollback()
            .await
            .expect("transaction rollback should succeed");
    }

    #[tokio::test]
    async fn catch_up_is_noop_when_frontier_is_already_applied() {
        let backend = init_projection_backend().await;
        seed_local_version_head(&backend, "main", "commit-2", "2026-03-15T01:02:03Z")
            .await
            .expect("local version head should seed");
        seed_latest_replay_cursor(&backend, "change-2", "2026-03-15T01:02:03Z").await;
        seed_live_state_status_row(
            &backend,
            LiveStateMode::Ready,
            Some(&ReplayCursor::new("change-1", "2026-03-15T01:02:02Z")),
            Some(&frontier(&[("main", "commit-2")])),
            "2026-03-15T01:02:03Z",
        )
        .await
        .expect("status row should seed");
        backend.clear_query_log();

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
        assert!(
            backend.executed_sql().iter().all(|sql| !sql
                .to_ascii_lowercase()
                .contains("insert into lix_internal_live_state_status")),
            "already-applied catch-up should not rewrite projection status"
        );
    }

    #[test]
    fn catch_up_restamps_replay_state_without_rebuild_when_frontier_is_current() {
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
                .begin_write_transaction()
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
                .expect("catch-up should restamp replay state without rebuilding");

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
            assert_eq!(report.outcome, ProjectionCatchUpOutcome::AlreadyApplied);
            assert_eq!(report.final_cursor, Some(latest_cursor.clone()));
            assert_eq!(report.target_cursor, Some(latest_cursor));
            assert_eq!(report.full_rebuild_passes, 0);
            assert_eq!(report.delta_merge_passes, 0);
        });
    }

    #[test]
    fn catch_up_rebuilds_only_changed_frontier_versions_without_full_rebuild() {
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
            let frontier_before_delta =
                status::load_live_state_projection_status_with_backend(&backend)
                    .await
                    .expect("projection status should load after version-b")
                    .current_committed_frontier;
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

            seed_live_state_status_row(
                &backend,
                LiveStateMode::NeedsRebuild,
                Some(&cursor_before_delta),
                Some(&frontier_before_delta),
                "2026-03-15T01:02:03Z",
            )
            .await
            .expect("status row should seed");

            let report = catch_up_live_state_to_current_frontier(&backend)
                .await
                .expect("catch-up should rebuild only changed frontier versions");

            let projection_status =
                status::load_live_state_projection_status_with_backend(&backend)
                    .await
                    .expect("projection status should load after catch-up");
            assert_eq!(projection_status.mode, LiveStateMode::Ready);
            assert_eq!(
                projection_status.applied_cursor,
                Some(latest_cursor.clone())
            );
            assert_eq!(
                projection_status.applied_committed_frontier,
                Some(projection_status.current_committed_frontier.clone())
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
