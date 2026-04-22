#![allow(dead_code)]

use crate::common::is_missing_relation_error;
use crate::live_state::store::{
    LiveStateLifecycleAdminStore, LiveStateLifecycleReadStore, LiveStateLifecycleWriteStore,
};
use crate::live_state::ReplayCursor;
use crate::live_state::{LiveStateMode, LiveStateProjectionStatus};
use crate::version::CommittedVersionFrontier;
use crate::{LixError, QueryResult, Value};

pub(crate) const LIVE_STATE_SCHEMA_EPOCH: &str = "1";
pub(crate) const LIVE_STATE_STATUS_TABLE: &str = "lix_internal_live_state_status";
const LIVE_STATE_STATUS_SINGLETON_ID: i64 = 1;

pub(crate) const LIVE_STATE_STATUS_CREATE_TABLE_SQL: &str =
    "CREATE TABLE IF NOT EXISTS lix_internal_live_state_status (\
     singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),\
     mode TEXT NOT NULL,\
     latest_change_id TEXT,\
     latest_change_created_at TEXT,\
     applied_committed_frontier TEXT,\
     schema_epoch TEXT NOT NULL,\
     updated_at TEXT NOT NULL\
     )";

pub(crate) const LIVE_STATE_STATUS_SEED_ROW_SQL: &str = "INSERT INTO lix_internal_live_state_status (\
     singleton_id, mode, latest_change_id, latest_change_created_at, applied_committed_frontier, schema_epoch, updated_at\
     ) \
     SELECT 1, 'uninitialized', NULL, NULL, NULL, '1', '1970-01-01T00:00:00Z' \
     WHERE NOT EXISTS (\
       SELECT 1 FROM lix_internal_live_state_status WHERE singleton_id = 1\
     )";

impl LiveStateMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Uninitialized => "uninitialized",
            Self::Bootstrapping => "bootstrapping",
            Self::Ready => "ready",
            Self::NeedsRebuild => "needs_rebuild",
            Self::Rebuilding => "rebuilding",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "uninitialized" => Some(Self::Uninitialized),
            "bootstrapping" => Some(Self::Bootstrapping),
            "ready" => Some(Self::Ready),
            "needs_rebuild" => Some(Self::NeedsRebuild),
            "rebuilding" => Some(Self::Rebuilding),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveStateStatusRow {
    pub(crate) mode: LiveStateMode,
    pub(crate) schema_epoch: String,
    pub(crate) replay_cursor: Option<ReplayCursor>,
    pub(crate) applied_committed_frontier: Option<CommittedVersionFrontier>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveStateSnapshot {
    pub(crate) status: Option<LiveStateStatusRow>,
    pub(crate) latest_replay_cursor: Option<ReplayCursor>,
    pub(crate) current_committed_frontier: CommittedVersionFrontier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveStateReadiness {
    Uninitialized,
    Ready,
    NeedsRebuild,
}

pub(crate) fn evaluate_live_state_transaction_eligibility(
    snapshot: &LiveStateSnapshot,
) -> LiveStateReadiness {
    let Some(status) = snapshot.status.as_ref() else {
        return if snapshot.current_committed_frontier.is_empty() {
            LiveStateReadiness::Uninitialized
        } else {
            LiveStateReadiness::NeedsRebuild
        };
    };

    match status.mode {
        LiveStateMode::Uninitialized => {
            if snapshot.current_committed_frontier.is_empty() {
                LiveStateReadiness::Uninitialized
            } else {
                LiveStateReadiness::NeedsRebuild
            }
        }
        LiveStateMode::Bootstrapping | LiveStateMode::Rebuilding | LiveStateMode::NeedsRebuild => {
            LiveStateReadiness::NeedsRebuild
        }
        // Inside an open write transaction the canonical change head may advance
        // before the transaction stamps the live-state replay cursor at commit
        // time. Transaction eligibility therefore validates owner state and
        // schema epoch, not cursor equality.
        LiveStateMode::Ready => {
            if status.schema_epoch == LIVE_STATE_SCHEMA_EPOCH {
                LiveStateReadiness::Ready
            } else {
                LiveStateReadiness::NeedsRebuild
            }
        }
    }
}

pub(crate) fn evaluate_live_state_snapshot(snapshot: &LiveStateSnapshot) -> LiveStateReadiness {
    let Some(status) = snapshot.status.as_ref() else {
        return if snapshot.current_committed_frontier.is_empty() {
            LiveStateReadiness::Uninitialized
        } else {
            LiveStateReadiness::NeedsRebuild
        };
    };

    match status.mode {
        LiveStateMode::Uninitialized => {
            if snapshot.current_committed_frontier.is_empty() {
                LiveStateReadiness::Uninitialized
            } else {
                LiveStateReadiness::NeedsRebuild
            }
        }
        LiveStateMode::Bootstrapping | LiveStateMode::Rebuilding | LiveStateMode::NeedsRebuild => {
            LiveStateReadiness::NeedsRebuild
        }
        LiveStateMode::Ready => {
            let ready = status.schema_epoch == LIVE_STATE_SCHEMA_EPOCH
                && status.applied_committed_frontier.as_ref()
                    == Some(&snapshot.current_committed_frontier);
            if ready {
                LiveStateReadiness::Ready
            } else {
                LiveStateReadiness::NeedsRebuild
            }
        }
    }
}

pub(crate) async fn load_live_state_snapshot(
    store: &impl LiveStateLifecycleReadStore,
) -> Result<LiveStateSnapshot, LixError> {
    store.load_live_state_snapshot().await
}

pub(crate) async fn load_projection_status(
    store: &impl LiveStateLifecycleReadStore,
) -> Result<LiveStateProjectionStatus, LixError> {
    Ok(projection_status_from_snapshot(
        store.load_live_state_snapshot().await?,
    ))
}

pub async fn init(store: &impl LiveStateLifecycleAdminStore) -> Result<(), LixError> {
    store.init_live_state_status_storage().await
}

pub async fn require_ready(store: &impl LiveStateLifecycleReadStore) -> Result<(), LixError> {
    let snapshot = load_live_state_snapshot(store).await?;
    match evaluate_live_state_snapshot(&snapshot) {
        LiveStateReadiness::Ready => Ok(()),
        LiveStateReadiness::Uninitialized => Err(crate::common::not_initialized_error()),
        LiveStateReadiness::NeedsRebuild => Err(crate::common::live_state_not_ready_error()),
    }
}

pub(crate) async fn require_ready_in_transaction(
    store: &mut impl LiveStateLifecycleWriteStore,
) -> Result<(), LixError> {
    let snapshot = store.load_live_state_snapshot().await?;
    match evaluate_live_state_transaction_eligibility(&snapshot) {
        LiveStateReadiness::Ready => Ok(()),
        LiveStateReadiness::Uninitialized => Err(crate::common::not_initialized_error()),
        LiveStateReadiness::NeedsRebuild => Err(crate::common::live_state_not_ready_error()),
    }
}

pub(crate) async fn mark_live_state_ready_at_latest_replay_cursor_in_transaction(
    store: &mut impl LiveStateLifecycleWriteStore,
) -> Result<ReplayCursor, LixError> {
    let cursor = store.load_latest_replay_cursor().await?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "live_state::mark_live_state_ready_at_latest_replay_cursor expected a replay cursor",
        )
    })?;
    let frontier = store.load_current_committed_frontier().await?;
    store.mark_live_state_ready(&cursor, &frontier).await?;
    store
        .stamp_live_state_durable_consumer_cursor(&cursor)
        .await?;
    Ok(cursor)
}

pub(crate) async fn mark_live_state_ready_at_replay_cursor_in_transaction(
    store: &mut impl LiveStateLifecycleWriteStore,
    cursor: &ReplayCursor,
) -> Result<(), LixError> {
    let frontier = store.load_current_committed_frontier().await?;
    store.mark_live_state_ready(cursor, &frontier).await?;
    store
        .stamp_live_state_durable_consumer_cursor(cursor)
        .await?;
    Ok(())
}

pub(crate) async fn mark_live_state_ready_without_replay_cursor_in_transaction(
    store: &mut impl LiveStateLifecycleWriteStore,
) -> Result<(), LixError> {
    let frontier = store.load_current_committed_frontier().await?;
    store
        .mark_live_state_ready_without_cursor(&frontier)
        .await?;
    store.clear_live_state_durable_consumer_cursor().await?;
    Ok(())
}

pub(crate) async fn advance_commit_replay_boundary_in_transaction(
    store: &mut impl LiveStateLifecycleWriteStore,
) -> Result<(), LixError> {
    let cursor = store.load_latest_replay_cursor().await?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "live_state::advance_commit_replay_boundary expected a replay cursor",
        )
    })?;
    advance_commit_replay_boundary_to_cursor_in_transaction(store, &cursor).await
}

pub(crate) async fn advance_commit_replay_boundary_to_cursor_in_transaction(
    store: &mut impl LiveStateLifecycleWriteStore,
    cursor: &ReplayCursor,
) -> Result<(), LixError> {
    store.ensure_live_state_status_row().await?;

    let snapshot = store.load_live_state_snapshot().await?;
    let mode = match snapshot.status.as_ref().map(|status| status.mode) {
        Some(LiveStateMode::Ready) => LiveStateMode::Ready,
        _ => LiveStateMode::NeedsRebuild,
    };
    let applied_frontier = if mode == LiveStateMode::Ready {
        Some(snapshot.current_committed_frontier.clone())
    } else {
        snapshot
            .status
            .as_ref()
            .and_then(|status| status.applied_committed_frontier.clone())
    };

    store
        .mark_live_state_mode_with_cursor_and_frontier(mode, cursor, applied_frontier.as_ref())
        .await?;
    store
        .stamp_live_state_durable_consumer_cursor(cursor)
        .await?;
    Ok(())
}

pub(crate) async fn mark_needs_rebuild_at_latest_replay_cursor_in_transaction(
    store: &mut impl LiveStateLifecycleWriteStore,
) -> Result<ReplayCursor, LixError> {
    let cursor = store.load_latest_replay_cursor().await?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "live_state::mark_needs_rebuild expected a replay cursor",
        )
    })?;
    mark_needs_rebuild_at_replay_cursor_in_transaction(store, &cursor).await?;
    Ok(cursor)
}

pub(crate) async fn mark_needs_rebuild_at_replay_cursor_in_transaction(
    store: &mut impl LiveStateLifecycleWriteStore,
    cursor: &ReplayCursor,
) -> Result<(), LixError> {
    store.ensure_live_state_status_row().await?;
    let applied_frontier = store.load_current_applied_frontier().await?;
    store
        .mark_live_state_mode_with_cursor_and_frontier(
            LiveStateMode::NeedsRebuild,
            cursor,
            applied_frontier.as_ref(),
        )
        .await?;
    store
        .stamp_live_state_durable_consumer_cursor(cursor)
        .await?;
    Ok(())
}

pub(crate) async fn try_claim_live_state_bootstrap_in_transaction(
    store: &mut impl LiveStateLifecycleWriteStore,
) -> Result<bool, LixError> {
    store.try_claim_live_state_bootstrap().await
}

pub(crate) async fn mark_live_state_mode_in_transaction(
    store: &mut impl LiveStateLifecycleWriteStore,
    mode: LiveStateMode,
) -> Result<(), LixError> {
    store.mark_live_state_mode(mode).await
}

pub(crate) async fn mark_live_state_mode(
    store: &impl LiveStateLifecycleAdminStore,
    mode: LiveStateMode,
) -> Result<(), LixError> {
    store.mark_live_state_mode(mode).await
}

pub(crate) async fn mark_live_state_ready(
    store: &impl LiveStateLifecycleAdminStore,
    cursor: &ReplayCursor,
) -> Result<(), LixError> {
    let frontier = store.load_current_committed_frontier().await?;
    store.mark_live_state_ready(cursor, &frontier).await?;
    store
        .stamp_live_state_durable_consumer_cursor(cursor)
        .await?;
    Ok(())
}

pub(crate) async fn load_live_state_mode(
    store: &impl LiveStateLifecycleReadStore,
) -> Result<LiveStateMode, LixError> {
    store.load_live_state_mode().await
}

pub(crate) async fn try_claim_live_state_bootstrap(
    store: &impl LiveStateLifecycleAdminStore,
) -> Result<bool, LixError> {
    store.try_claim_live_state_bootstrap().await
}

pub(crate) async fn load_latest_replay_cursor(
    store: &impl LiveStateLifecycleReadStore,
) -> Result<Option<ReplayCursor>, LixError> {
    store.load_latest_replay_cursor().await
}

pub(crate) fn parse_latest_replay_cursor(
    result: &QueryResult,
) -> Result<Option<ReplayCursor>, LixError> {
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    Ok(Some(ReplayCursor::new(
        text_value(row.first(), "lix_internal_change.id")?,
        text_value(row.get(1), "lix_internal_change.created_at")?,
    )))
}

pub(crate) fn build_set_live_state_mode_sql(mode: LiveStateMode) -> String {
    build_upsert_live_state_status_sql(mode, None, None)
}

pub(crate) fn build_mark_live_state_ready_sql(
    cursor: &ReplayCursor,
    frontier: &CommittedVersionFrontier,
) -> String {
    build_set_live_state_mode_with_cursor_and_frontier_sql(
        LiveStateMode::Ready,
        cursor,
        Some(frontier),
    )
}

pub(crate) fn build_mark_live_state_ready_without_cursor_sql(
    frontier: &CommittedVersionFrontier,
) -> String {
    build_upsert_live_state_status_sql(LiveStateMode::Ready, None, Some(frontier))
}

pub(crate) fn build_set_live_state_mode_with_cursor_sql(
    mode: LiveStateMode,
    cursor: &ReplayCursor,
) -> String {
    build_set_live_state_mode_with_cursor_and_frontier_sql(mode, cursor, None)
}

pub(crate) fn build_set_live_state_mode_with_cursor_and_frontier_sql(
    mode: LiveStateMode,
    cursor: &ReplayCursor,
    frontier: Option<&CommittedVersionFrontier>,
) -> String {
    build_upsert_live_state_status_sql(mode, Some(cursor), frontier)
}

fn build_upsert_live_state_status_sql(
    mode: LiveStateMode,
    cursor: Option<&ReplayCursor>,
    frontier: Option<&CommittedVersionFrontier>,
) -> String {
    let latest_change_id_sql = cursor
        .map(|value| format!("'{}'", escape_sql_string(&value.change_id)))
        .unwrap_or_else(|| "NULL".to_string());
    let latest_change_created_at_sql = cursor
        .map(|value| format!("'{}'", escape_sql_string(&value.created_at)))
        .unwrap_or_else(|| "NULL".to_string());
    let applied_frontier_sql = frontier
        .map(|frontier| format!("'{}'", escape_sql_string(&frontier.to_json_string())))
        .unwrap_or_else(|| "NULL".to_string());
    format!(
        "INSERT INTO {table} (\
         singleton_id, mode, latest_change_id, latest_change_created_at, applied_committed_frontier, schema_epoch, updated_at\
         ) VALUES (\
         {singleton_id}, '{mode}', {change_id}, {created_at}, {frontier}, '{schema_epoch}', CURRENT_TIMESTAMP\
         ) ON CONFLICT (singleton_id) DO UPDATE SET \
         mode = excluded.mode, \
         latest_change_id = excluded.latest_change_id, \
         latest_change_created_at = excluded.latest_change_created_at, \
         applied_committed_frontier = excluded.applied_committed_frontier, \
         schema_epoch = excluded.schema_epoch, \
         updated_at = excluded.updated_at",
        table = LIVE_STATE_STATUS_TABLE,
        singleton_id = LIVE_STATE_STATUS_SINGLETON_ID,
        mode = mode.as_str(),
        change_id = latest_change_id_sql,
        created_at = latest_change_created_at_sql,
        frontier = applied_frontier_sql,
        schema_epoch = LIVE_STATE_SCHEMA_EPOCH,
    )
}

pub(crate) fn parse_nullable_live_state_status_result(
    result: Result<QueryResult, LixError>,
) -> Result<Option<LiveStateStatusRow>, LixError> {
    let result = match result {
        Ok(result) => result,
        Err(error) if is_missing_relation_error(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    Ok(Some(live_state_status_row_from_values(row)?))
}

pub(crate) fn projection_status_from_snapshot(
    snapshot: LiveStateSnapshot,
) -> LiveStateProjectionStatus {
    let readiness = evaluate_live_state_snapshot(&snapshot);
    let LiveStateSnapshot {
        status,
        latest_replay_cursor,
        current_committed_frontier,
    } = snapshot;
    let (raw_mode, applied_cursor, applied_committed_frontier) = match status {
        Some(status) => (
            status.mode,
            status.replay_cursor,
            status.applied_committed_frontier,
        ),
        None => (LiveStateMode::Uninitialized, None, None),
    };
    let mode = match readiness {
        LiveStateReadiness::Ready => LiveStateMode::Ready,
        LiveStateReadiness::Uninitialized => LiveStateMode::Uninitialized,
        LiveStateReadiness::NeedsRebuild => match raw_mode {
            LiveStateMode::Bootstrapping => LiveStateMode::Bootstrapping,
            LiveStateMode::Rebuilding => LiveStateMode::Rebuilding,
            _ => LiveStateMode::NeedsRebuild,
        },
    };
    LiveStateProjectionStatus {
        mode,
        applied_cursor,
        latest_cursor: latest_replay_cursor,
        applied_committed_frontier,
        current_committed_frontier,
    }
}

pub(crate) fn live_state_status_row_from_values(
    row: &[Value],
) -> Result<LiveStateStatusRow, LixError> {
    let mode_text = text_value(row.first(), "lix_internal_live_state_status.mode")?;
    let Some(mode) = LiveStateMode::parse(&mode_text) else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("invalid live state mode '{mode_text}'"),
        ));
    };
    let latest_change_id = optional_text_value(row.get(1))?;
    let latest_change_created_at = optional_text_value(row.get(2))?;
    let replay_cursor = match (latest_change_id, latest_change_created_at) {
        (Some(change_id), Some(created_at)) => Some(ReplayCursor::new(change_id, created_at)),
        (None, None) => None,
        _ => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "live state replay cursor is partially populated",
            ));
        }
    };

    Ok(LiveStateStatusRow {
        mode,
        schema_epoch: text_value(row.get(3), "lix_internal_live_state_status.schema_epoch")?,
        replay_cursor,
        applied_committed_frontier: parse_nullable_committed_frontier(row.get(4))?,
    })
}

pub(crate) fn default_live_state_status() -> LiveStateStatusRow {
    LiveStateStatusRow {
        mode: LiveStateMode::Uninitialized,
        schema_epoch: LIVE_STATE_SCHEMA_EPOCH.to_string(),
        replay_cursor: None,
        applied_committed_frontier: None,
    }
}

pub(crate) fn default_live_state_snapshot() -> LiveStateSnapshot {
    LiveStateSnapshot {
        status: None,
        latest_replay_cursor: None,
        current_committed_frontier: CommittedVersionFrontier::default(),
    }
}

fn parse_nullable_replay_cursor(
    change_id: Option<&Value>,
    created_at: Option<&Value>,
) -> Result<Option<ReplayCursor>, LixError> {
    match (
        optional_text_value(change_id)?,
        optional_text_value(created_at)?,
    ) {
        (Some(change_id), Some(created_at)) => Ok(Some(ReplayCursor::new(change_id, created_at))),
        (None, None) => Ok(None),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "latest replay cursor is partially populated",
        )),
    }
}

fn parse_nullable_committed_frontier(
    value: Option<&Value>,
) -> Result<Option<CommittedVersionFrontier>, LixError> {
    match optional_text_value(value)? {
        Some(value) => Ok(Some(CommittedVersionFrontier::from_json_str(&value)?)),
        None => Ok(None),
    }
}

fn text_value(value: Option<&Value>, field: &str) -> Result<String, LixError> {
    match value {
        Some(Value::Text(text)) if !text.is_empty() => Ok(text.clone()),
        Some(Value::Text(_)) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("{field} is empty"),
        )),
        Some(Value::Integer(number)) => Ok(number.to_string()),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("expected text-like value for {field}, got {other:?}"),
        )),
        None => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("{field} is missing"),
        )),
    }
}

fn optional_text_value(value: Option<&Value>) -> Result<Option<String>, LixError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Text(text)) => Ok(Some(text.clone())),
        Some(Value::Integer(number)) => Ok(Some(number.to_string())),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("expected nullable text-like live state field, got {other:?}"),
        )),
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::{
        append_untracked_change_visibility_rows, CanonicalUntrackedVisibilityKind,
        CanonicalUntrackedVisibilityWrite,
    };
    use crate::live_state::store_sql::SqlLiveStateStore;
    use crate::streams::{
        load_durable_state_commit_consumer_cursors, load_durable_state_commit_low_watermark,
        upsert_durable_state_commit_consumer_cursor_in_transaction, DurableStateCommitCursor,
        LIVE_STATE_DURABLE_CONSUMER_KEY,
    };
    use crate::test_support::{
        init_test_backend_core, seed_canonical_change_row, seed_live_state_status_row,
        seed_local_version_head, CanonicalChangeSeed, TestSqliteBackend,
    };
    use crate::version::GLOBAL_VERSION_ID;

    async fn init_lifecycle_backend() -> TestSqliteBackend {
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
        untracked: bool,
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

        if untracked {
            let mut transaction = backend
                .begin_write_transaction()
                .await
                .expect("latest replay cursor visibility transaction should begin");
            append_untracked_change_visibility_rows(
                transaction.as_mut(),
                &[CanonicalUntrackedVisibilityWrite {
                    id: format!("visibility:{change_id}"),
                    change_id: change_id.to_string(),
                    version_id: GLOBAL_VERSION_ID.to_string(),
                    visibility_kind: CanonicalUntrackedVisibilityKind::Global,
                    entity_id: "cursor-entity".try_into().expect("valid entity id"),
                    schema_key: "lix_key_value".try_into().expect("valid schema key"),
                    file_id: None,
                    created_at: created_at.to_string(),
                }],
            )
            .await
            .expect("latest replay cursor visibility row should seed");
            transaction
                .commit()
                .await
                .expect("latest replay cursor visibility transaction should commit");
        }
    }

    async fn delete_status_row(backend: &TestSqliteBackend) {
        crate::live_state::store_sql::execute_query_with_backend(
            backend,
            "DELETE FROM lix_internal_live_state_status WHERE singleton_id = 1",
            &[],
        )
        .await
        .expect("status row should delete");
    }

    fn frontier_json(entries: &[(&str, &str)]) -> String {
        CommittedVersionFrontier {
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
    async fn readiness_is_uninitialized_without_canonical_state() {
        let backend = init_lifecycle_backend().await;
        assert_eq!(
            evaluate_live_state_snapshot(
                &load_live_state_snapshot(&SqlLiveStateStore::from_backend(&backend))
                    .await
                    .unwrap(),
            ),
            LiveStateReadiness::Uninitialized
        );
    }

    #[tokio::test]
    async fn readiness_is_ready_when_applied_frontier_matches_current_frontier() {
        let backend = init_lifecycle_backend().await;
        seed_local_version_head(&backend, "main", "commit-2", "2026-03-15T01:02:03Z")
            .await
            .expect("local version head should seed");
        seed_latest_replay_cursor(&backend, "change-3", "2026-03-15T01:02:04Z", false).await;
        seed_live_state_status_row(
            &backend,
            LiveStateMode::Ready,
            Some(&ReplayCursor::new("change-1", "2026-03-15T01:02:02Z")),
            Some(
                &CommittedVersionFrontier::from_json_str(&frontier_json(&[("main", "commit-2")]))
                    .expect("frontier json should parse"),
            ),
            "2026-03-15T01:02:04Z",
        )
        .await
        .expect("status row should seed");

        assert_eq!(
            evaluate_live_state_snapshot(
                &load_live_state_snapshot(&SqlLiveStateStore::from_backend(&backend))
                    .await
                    .unwrap(),
            ),
            LiveStateReadiness::Ready
        );
    }

    #[tokio::test]
    async fn readiness_mismatch_is_observed_without_mutating_status() {
        let backend = init_lifecycle_backend().await;
        seed_local_version_head(&backend, "main", "commit-2", "2026-03-15T01:02:03Z")
            .await
            .expect("local version head should seed");
        seed_latest_replay_cursor(&backend, "change-2", "2026-03-15T01:02:03Z", false).await;
        seed_live_state_status_row(
            &backend,
            LiveStateMode::Ready,
            Some(&ReplayCursor::new("change-1", "2026-03-15T01:02:02Z")),
            Some(
                &CommittedVersionFrontier::from_json_str(&frontier_json(&[("main", "commit-1")]))
                    .expect("frontier json should parse"),
            ),
            "2026-03-15T01:02:03Z",
        )
        .await
        .expect("status row should seed");
        backend.clear_query_log();

        let snapshot = load_live_state_snapshot(&SqlLiveStateStore::from_backend(&backend))
            .await
            .unwrap();
        assert_eq!(
            evaluate_live_state_snapshot(&snapshot),
            LiveStateReadiness::NeedsRebuild
        );

        let executed_sql = backend.executed_sql();
        assert!(
            executed_sql.iter().all(|sql| !sql
                .to_ascii_lowercase()
                .contains("insert into lix_internal_live_state_status")),
            "observer path must not mutate live-state status"
        );
        assert!(
            executed_sql.iter().all(|sql| !sql
                .to_ascii_lowercase()
                .contains("update lix_internal_live_state_status")),
            "observer path must not mutate live-state status"
        );
    }

    #[tokio::test]
    async fn transaction_ready_check_rejects_needs_rebuild() {
        let backend = init_lifecycle_backend().await;
        seed_live_state_status_row(
            &backend,
            LiveStateMode::NeedsRebuild,
            None,
            None,
            "2026-03-15T01:02:03Z",
        )
        .await
        .expect("status row should seed");
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should begin");

        let error = require_ready_in_transaction(&mut SqlLiveStateStore::from_transaction(
            transaction.as_mut(),
        ))
        .await
        .expect_err("needs_rebuild should fail");
        assert_eq!(
            error.code,
            crate::common::ErrorCode::LiveStateNotReady.as_str()
        );
        transaction
            .rollback()
            .await
            .expect("transaction rollback should succeed");
    }

    #[tokio::test]
    async fn readiness_without_status_but_with_replayed_canonical_state_requires_rebuild() {
        let backend = init_lifecycle_backend().await;
        delete_status_row(&backend).await;
        seed_local_version_head(&backend, "main", "commit-2", "2026-03-15T01:02:03Z")
            .await
            .expect("local version head should seed");
        seed_latest_replay_cursor(&backend, "change-2", "2026-03-15T01:02:03Z", false).await;

        let snapshot = load_live_state_snapshot(&SqlLiveStateStore::from_backend(&backend))
            .await
            .unwrap();
        assert_eq!(
            evaluate_live_state_snapshot(&snapshot),
            LiveStateReadiness::NeedsRebuild
        );
    }

    #[tokio::test]
    async fn latest_replay_cursor_includes_untracked_journal_rows() {
        let backend = init_lifecycle_backend().await;
        seed_latest_replay_cursor(&backend, "change-tracked", "2026-03-15T01:02:02Z", false).await;
        seed_latest_replay_cursor(&backend, "change-untracked", "2026-03-15T01:02:03Z", true).await;

        let latest = load_latest_replay_cursor(&SqlLiveStateStore::from_backend(&backend))
            .await
            .expect("latest replay cursor should load")
            .expect("latest replay cursor should exist");

        assert_eq!(
            latest,
            ReplayCursor::new("change-untracked", "2026-03-15T01:02:03Z")
        );
    }

    #[tokio::test]
    async fn transaction_ready_check_allows_inflight_cursor_drift() {
        let backend = init_lifecycle_backend().await;
        seed_local_version_head(&backend, "main", "commit-2", "2026-03-15T01:02:03Z")
            .await
            .expect("local version head should seed");
        seed_latest_replay_cursor(&backend, "change-2", "2026-03-15T01:02:03Z", false).await;
        seed_live_state_status_row(
            &backend,
            LiveStateMode::Ready,
            Some(&ReplayCursor::new("change-1", "2026-03-15T01:02:02Z")),
            Some(
                &CommittedVersionFrontier::from_json_str(&frontier_json(&[("main", "commit-1")]))
                    .expect("frontier json should parse"),
            ),
            "2026-03-15T01:02:03Z",
        )
        .await
        .expect("status row should seed");
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should begin");

        require_ready_in_transaction(&mut SqlLiveStateStore::from_transaction(
            transaction.as_mut(),
        ))
        .await
        .expect("inflight cursor drift inside transaction should be allowed");
        transaction
            .rollback()
            .await
            .expect("transaction rollback should succeed");
    }

    #[tokio::test]
    async fn commit_replay_boundary_marks_ready_when_live_state_is_ready() {
        let backend = init_lifecycle_backend().await;
        seed_local_version_head(&backend, "main", "commit-2", "2026-03-15T01:02:03Z")
            .await
            .expect("local version head should seed");
        seed_latest_replay_cursor(&backend, "change-2", "2026-03-15T01:02:03Z", false).await;
        seed_live_state_status_row(
            &backend,
            LiveStateMode::Ready,
            Some(&ReplayCursor::new("change-1", "2026-03-15T01:02:02Z")),
            Some(
                &CommittedVersionFrontier::from_json_str(&frontier_json(&[("main", "commit-1")]))
                    .expect("frontier json should parse"),
            ),
            "2026-03-15T01:02:03Z",
        )
        .await
        .expect("status row should seed");
        backend.clear_query_log();
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should begin");

        advance_commit_replay_boundary_in_transaction(&mut SqlLiveStateStore::from_transaction(
            transaction.as_mut(),
        ))
        .await
        .expect("replay-boundary update should succeed");
        assert!(
            backend.executed_sql().iter().any(|sql| sql
                .contains("INSERT INTO lix_internal_live_state_status ")
                && sql.contains("'ready'")
                && sql.contains("'change-2'")
                && sql.contains("commit-2")),
            "ready live-state status should advance to the latest replay cursor",
        );
        transaction
            .rollback()
            .await
            .expect("transaction rollback should succeed");
    }

    #[tokio::test]
    async fn live_state_ready_marks_durable_state_commit_consumer_cursor() {
        let backend = init_lifecycle_backend().await;
        seed_local_version_head(&backend, "main", "commit-2", "2026-03-15T01:02:03Z")
            .await
            .expect("local version head should seed");
        seed_latest_replay_cursor(&backend, "change-2", "2026-03-15T01:02:03Z", false).await;
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should begin");

        mark_live_state_ready_at_latest_replay_cursor_in_transaction(
            &mut SqlLiveStateStore::from_transaction(transaction.as_mut()),
        )
        .await
        .expect("ready stamp should succeed");
        transaction
            .commit()
            .await
            .expect("transaction commit should succeed");

        let cursors = load_durable_state_commit_consumer_cursors(&backend)
            .await
            .expect("consumer cursors should load");
        assert_eq!(cursors.len(), 1);
        assert_eq!(cursors[0].consumer_key, LIVE_STATE_DURABLE_CONSUMER_KEY);
        assert_eq!(
            cursors[0].cursor,
            DurableStateCommitCursor {
                change_id: "change-2".to_string(),
                created_at: "2026-03-15T01:02:03Z".to_string(),
                visibility_append_seq: 0,
            }
        );

        let low_watermark = load_durable_state_commit_low_watermark(&backend)
            .await
            .expect("low watermark should load")
            .expect("low watermark should exist");
        assert_eq!(low_watermark, cursors[0].cursor);
    }

    #[tokio::test]
    async fn live_state_ready_without_replay_cursor_clears_durable_consumer_cursor() {
        let backend = init_lifecycle_backend().await;
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should begin");

        upsert_durable_state_commit_consumer_cursor_in_transaction(
            transaction.as_mut(),
            LIVE_STATE_DURABLE_CONSUMER_KEY,
            &DurableStateCommitCursor {
                change_id: "change-1".to_string(),
                created_at: "2026-03-15T01:02:02Z".to_string(),
                visibility_append_seq: 0,
            },
        )
        .await
        .expect("seed durable consumer cursor should succeed");
        mark_live_state_ready_without_replay_cursor_in_transaction(
            &mut SqlLiveStateStore::from_transaction(transaction.as_mut()),
        )
        .await
        .expect("ready without cursor should succeed");
        transaction
            .commit()
            .await
            .expect("transaction commit should succeed");

        let cursors = load_durable_state_commit_consumer_cursors(&backend)
            .await
            .expect("consumer cursors should load");
        assert!(cursors.is_empty());
        assert!(
            load_durable_state_commit_low_watermark(&backend)
                .await
                .expect("low watermark should load")
                .is_none(),
            "clearing the only durable consumer cursor should clear the low watermark"
        );
    }

    #[tokio::test]
    async fn commit_replay_boundary_marks_needs_rebuild_when_live_state_is_not_ready() {
        let backend = init_lifecycle_backend().await;
        seed_local_version_head(&backend, "main", "commit-2", "2026-03-15T01:02:03Z")
            .await
            .expect("local version head should seed");
        seed_latest_replay_cursor(&backend, "change-2", "2026-03-15T01:02:03Z", false).await;
        seed_live_state_status_row(
            &backend,
            LiveStateMode::NeedsRebuild,
            Some(&ReplayCursor::new("change-1", "2026-03-15T01:02:02Z")),
            Some(
                &CommittedVersionFrontier::from_json_str(&frontier_json(&[("main", "commit-1")]))
                    .expect("frontier json should parse"),
            ),
            "2026-03-15T01:02:03Z",
        )
        .await
        .expect("status row should seed");
        backend.clear_query_log();
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should begin");

        advance_commit_replay_boundary_in_transaction(&mut SqlLiveStateStore::from_transaction(
            transaction.as_mut(),
        ))
        .await
        .expect("replay-boundary update should succeed");
        assert!(
            backend.executed_sql().iter().any(|sql| sql
                .contains("INSERT INTO lix_internal_live_state_status ")
                && sql.contains("'needs_rebuild'")
                && sql.contains("'change-2'")
                && sql.contains("commit-1")),
            "non-ready live-state status should keep a durable replay boundary instead of claiming readiness",
        );
        transaction
            .rollback()
            .await
            .expect("transaction rollback should succeed");
    }
}
