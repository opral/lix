use crate::backend::{KvStore, KvWriter};
use crate::engine2::commit_graph::CommitGraphContext;
use crate::engine2::commit_graph::CommitGraphEntity;
use crate::engine2::tracked_state::{
    TrackedStateContext, TrackedStateDeleteRequest, TrackedStateFilter, TrackedStateRow,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

/// Summary of a tracked-state rebuild operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TrackedStateRebuildReport {
    pub(crate) deleted_rows: usize,
    pub(crate) written_rows: usize,
}

/// Rebuilds tracked-state rows for one version from a commit graph head.
///
/// The caller provides both stores explicitly so rebuilds can read from the
/// desired KV snapshot and write through the desired transaction.
pub(super) async fn rebuild_version_state<R, W>(
    tracked_state: &TrackedStateContext,
    commit_graph: &CommitGraphContext,
    read_store: R,
    write_store: W,
    version_id: &str,
    head_commit_id: &str,
) -> Result<TrackedStateRebuildReport, LixError>
where
    R: KvStore,
    W: KvWriter,
{
    let entities = commit_graph
        .reader(read_store)
        .entities_at(head_commit_id)
        .await?;
    let rows = rows_from_entities(version_id, entities)?;
    let written_rows = rows.len();

    let mut writer = tracked_state.writer(write_store);
    let deleted_rows = writer
        .delete_rows(&TrackedStateDeleteRequest {
            filter: TrackedStateFilter {
                version_ids: vec![version_id.to_string()],
                include_tombstones: true,
                ..Default::default()
            },
        })
        .await?;
    writer.write_rows(&rows).await?;

    Ok(TrackedStateRebuildReport {
        deleted_rows,
        written_rows,
    })
}

/// Converts commit-graph entities into tracked-state rows for one version.
///
/// The commit graph owns history resolution. This function only maps the
/// effective canonical entities into the storage row shape tracked_state serves.
pub(crate) fn rows_from_entities(
    version_id: &str,
    entities: Vec<CommitGraphEntity>,
) -> Result<Vec<TrackedStateRow>, LixError> {
    Ok(entities
        .into_iter()
        .map(|entity| tracked_row_from_entity(version_id, entity))
        .collect())
}

fn tracked_row_from_entity(version_id: &str, entity: CommitGraphEntity) -> TrackedStateRow {
    let CommitGraphEntity {
        change,
        source_commit_id,
        created_at,
        updated_at,
        ..
    } = entity;
    TrackedStateRow {
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        file_id: change.file_id,
        plugin_key: change.plugin_key,
        snapshot_content: change.snapshot_content,
        metadata: change.metadata,
        schema_version: change.schema_version,
        created_at,
        updated_at,
        global: version_id == GLOBAL_VERSION_ID,
        change_id: change.id,
        commit_id: source_commit_id,
        version_id: version_id.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
    use crate::engine2::changelog::CanonicalChange;
    use crate::engine2::changelog::ChangelogContext;
    use crate::engine2::commit_graph::CommitGraphContext;
    use crate::engine2::tracked_state::{TrackedStateFilter, TrackedStateScanRequest};
    use serde_json::json;

    #[test]
    fn rows_from_entities_converts_normal_entity() {
        let rows = rows_from_entities("version-a", vec![entity("change-1", Some("{}"))])
            .expect("conversion should succeed");

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.entity_id, "entity-1");
        assert_eq!(row.schema_key, "test_schema");
        assert_eq!(row.file_id.as_deref(), Some("file-1"));
        assert_eq!(row.plugin_key.as_deref(), Some("plugin-1"));
        assert_eq!(row.snapshot_content.as_deref(), Some("{}"));
        assert_eq!(row.metadata.as_deref(), Some("{\"m\":1}"));
        assert_eq!(row.schema_version, "1");
        assert_eq!(row.change_id, "change-1");
        assert_eq!(row.commit_id, "commit-1");
        assert_eq!(row.version_id, "version-a");
        assert!(!row.global);
    }

    #[test]
    fn rows_from_entities_preserves_tombstones() {
        let rows = rows_from_entities("version-a", vec![entity("change-1", None)])
            .expect("conversion should succeed");

        assert_eq!(rows[0].snapshot_content, None);
    }

    #[test]
    fn rows_from_entities_uses_commit_graph_timestamps() {
        let rows = rows_from_entities("version-a", vec![entity("change-1", Some("{}"))])
            .expect("conversion should succeed");

        assert_eq!(rows[0].created_at, "2026-01-01T00:00:00Z");
        assert_eq!(rows[0].updated_at, "2026-01-02T00:00:00Z");
    }

    #[test]
    fn rows_from_entities_marks_global_for_global_version_only() {
        let global_rows =
            rows_from_entities(GLOBAL_VERSION_ID, vec![entity("change-1", Some("{}"))])
                .expect("global conversion should succeed");
        let version_rows = rows_from_entities("version-a", vec![entity("change-2", Some("{}"))])
            .expect("version conversion should succeed");

        assert!(global_rows[0].global);
        assert!(!version_rows[0].global);
    }

    #[tokio::test]
    async fn rebuild_version_state_writes_rows_from_commit_graph() {
        let backend = Arc::new(UnitTestBackend::new());
        let tracked_state = TrackedStateContext::new();
        let changelog = ChangelogContext::new();
        let commit_graph = CommitGraphContext::new(changelog);
        append_changes(
            Arc::clone(&backend),
            &[
                entity_change("change-1", "entity-1", "test_schema", Some("{}")),
                commit_change("commit-1-change", "commit-1", &["change-1"], &[]),
            ],
        )
        .await;

        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let report = rebuild_version_state(
            &tracked_state,
            &commit_graph,
            Arc::clone(&backend),
            tx.as_mut(),
            "version-a",
            "commit-1",
        )
        .await
        .expect("rebuild should succeed");
        tx.commit().await.expect("transaction should commit");

        assert_eq!(
            report,
            TrackedStateRebuildReport {
                deleted_rows: 0,
                written_rows: 2,
            }
        );
        let rows = scan_version_rows(&tracked_state, Arc::clone(&backend), "version-a").await;
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .any(|row| row.schema_key == "test_schema" && row.entity_id == "entity-1"));
        assert!(rows
            .iter()
            .any(|row| row.schema_key == "lix_commit" && row.entity_id == "commit-1"));
    }

    #[tokio::test]
    async fn rebuild_version_state_deletes_stale_rows_for_target_version_only() {
        let backend = Arc::new(UnitTestBackend::new());
        let tracked_state = TrackedStateContext::new();
        let changelog = ChangelogContext::new();
        let commit_graph = CommitGraphContext::new(changelog);
        append_changes(
            Arc::clone(&backend),
            &[
                entity_change("change-new", "entity-new", "test_schema", Some("{}")),
                commit_change("commit-1-change", "commit-1", &["change-new"], &[]),
            ],
        )
        .await;
        seed_tracked_rows(
            &tracked_state,
            Arc::clone(&backend),
            &[
                stale_row("version-a", "stale-target"),
                stale_row("version-b", "stale-other"),
            ],
        )
        .await;

        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let report = rebuild_version_state(
            &tracked_state,
            &commit_graph,
            Arc::clone(&backend),
            tx.as_mut(),
            "version-a",
            "commit-1",
        )
        .await
        .expect("rebuild should succeed");
        tx.commit().await.expect("transaction should commit");

        assert_eq!(report.deleted_rows, 1);
        assert_eq!(report.written_rows, 2);

        let version_a_rows =
            scan_version_rows(&tracked_state, Arc::clone(&backend), "version-a").await;
        assert!(!version_a_rows
            .iter()
            .any(|row| row.entity_id == "stale-target"));
        assert!(version_a_rows
            .iter()
            .any(|row| row.entity_id == "entity-new"));

        let version_b_rows =
            scan_version_rows(&tracked_state, Arc::clone(&backend), "version-b").await;
        assert_eq!(version_b_rows.len(), 1);
        assert_eq!(version_b_rows[0].entity_id, "stale-other");
    }

    #[tokio::test]
    async fn rebuild_version_state_uses_latest_change_across_commits() {
        let backend = Arc::new(UnitTestBackend::new());
        let tracked_state = TrackedStateContext::new();
        let changelog = ChangelogContext::new();
        let commit_graph = CommitGraphContext::new(changelog);
        append_changes(
            Arc::clone(&backend),
            &[
                entity_change_at(
                    "change-old",
                    "entity-1",
                    "test_schema",
                    Some("{\"value\":\"old\"}"),
                    "2026-01-01T00:00:00Z",
                ),
                entity_change_at(
                    "change-new",
                    "entity-1",
                    "test_schema",
                    Some("{\"value\":\"new\"}"),
                    "2026-01-02T00:00:00Z",
                ),
                commit_change("commit-root-change", "commit-root", &["change-old"], &[]),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-new"],
                    &["commit-root"],
                ),
            ],
        )
        .await;

        rebuild_version(
            &tracked_state,
            &commit_graph,
            Arc::clone(&backend),
            "version-a",
            "commit-head",
        )
        .await;

        let rows = scan_version_rows(&tracked_state, Arc::clone(&backend), "version-a").await;
        let row = rows
            .iter()
            .find(|row| row.schema_key == "test_schema" && row.entity_id == "entity-1")
            .expect("rebuilt entity row should exist");
        assert_eq!(row.snapshot_content.as_deref(), Some("{\"value\":\"new\"}"));
        assert_eq!(row.change_id, "change-new");
        assert_eq!(row.commit_id, "commit-head");
        assert_eq!(row.created_at, "2026-01-01T00:00:00Z");
        assert_eq!(row.updated_at, "2026-01-02T00:00:00Z");
    }

    #[tokio::test]
    async fn rebuild_version_state_preserves_tombstone_winner() {
        let backend = Arc::new(UnitTestBackend::new());
        let tracked_state = TrackedStateContext::new();
        let changelog = ChangelogContext::new();
        let commit_graph = CommitGraphContext::new(changelog);
        append_changes(
            Arc::clone(&backend),
            &[
                entity_change_at(
                    "change-created",
                    "entity-1",
                    "test_schema",
                    Some("{\"value\":\"created\"}"),
                    "2026-01-01T00:00:00Z",
                ),
                entity_change_at(
                    "change-deleted",
                    "entity-1",
                    "test_schema",
                    None,
                    "2026-01-02T00:00:00Z",
                ),
                commit_change(
                    "commit-root-change",
                    "commit-root",
                    &["change-created"],
                    &[],
                ),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-deleted"],
                    &["commit-root"],
                ),
            ],
        )
        .await;

        rebuild_version(
            &tracked_state,
            &commit_graph,
            Arc::clone(&backend),
            "version-a",
            "commit-head",
        )
        .await;

        let rows = scan_version_rows(&tracked_state, Arc::clone(&backend), "version-a").await;
        let row = rows
            .iter()
            .find(|row| row.schema_key == "test_schema" && row.entity_id == "entity-1")
            .expect("rebuilt tombstone row should exist");
        assert_eq!(row.snapshot_content, None);
        assert_eq!(row.change_id, "change-deleted");
        assert_eq!(row.commit_id, "commit-head");
    }

    #[tokio::test]
    async fn rebuild_version_state_marks_rows_global_for_global_version() {
        let backend = Arc::new(UnitTestBackend::new());
        let tracked_state = TrackedStateContext::new();
        let changelog = ChangelogContext::new();
        let commit_graph = CommitGraphContext::new(changelog);
        append_changes(
            Arc::clone(&backend),
            &[
                entity_change("change-1", "entity-1", "test_schema", Some("{}")),
                commit_change("commit-1-change", "commit-1", &["change-1"], &[]),
            ],
        )
        .await;

        rebuild_version(
            &tracked_state,
            &commit_graph,
            Arc::clone(&backend),
            GLOBAL_VERSION_ID,
            "commit-1",
        )
        .await;

        let rows = scan_version_rows(&tracked_state, Arc::clone(&backend), GLOBAL_VERSION_ID).await;
        assert!(!rows.is_empty());
        assert!(rows.iter().all(|row| row.global));
        assert!(rows
            .iter()
            .any(|row| row.schema_key == "test_schema" && row.entity_id == "entity-1"));
    }

    fn entity(change_id: &str, snapshot_content: Option<&str>) -> CommitGraphEntity {
        CommitGraphEntity {
            change: CanonicalChange {
                id: change_id.to_string(),
                entity_id: "entity-1".to_string(),
                schema_key: "test_schema".to_string(),
                schema_version: "1".to_string(),
                file_id: Some("file-1".to_string()),
                plugin_key: Some("plugin-1".to_string()),
                snapshot_content: snapshot_content.map(str::to_string),
                metadata: Some("{\"m\":1}".to_string()),
                created_at: "ignored-change-created-at".to_string(),
            },
            source_commit_id: "commit-1".to_string(),
            depth: 0,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-02T00:00:00Z".to_string(),
        }
    }

    async fn append_changes(backend: Arc<UnitTestBackend>, changes: &[CanonicalChange]) {
        let changelog = ChangelogContext::new();
        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        changelog
            .writer(tx.as_mut())
            .append_changes(changes)
            .await
            .expect("changes should append");
        tx.commit().await.expect("transaction should commit");
    }

    async fn seed_tracked_rows(
        tracked_state: &TrackedStateContext,
        backend: Arc<UnitTestBackend>,
        rows: &[TrackedStateRow],
    ) {
        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        tracked_state
            .writer(tx.as_mut())
            .write_rows(rows)
            .await
            .expect("rows should seed");
        tx.commit().await.expect("transaction should commit");
    }

    async fn rebuild_version(
        tracked_state: &TrackedStateContext,
        commit_graph: &CommitGraphContext,
        backend: Arc<UnitTestBackend>,
        version_id: &str,
        head_commit_id: &str,
    ) -> TrackedStateRebuildReport {
        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let report = rebuild_version_state(
            tracked_state,
            commit_graph,
            Arc::clone(&backend),
            tx.as_mut(),
            version_id,
            head_commit_id,
        )
        .await
        .expect("rebuild should succeed");
        tx.commit().await.expect("transaction should commit");
        report
    }

    async fn scan_version_rows(
        tracked_state: &TrackedStateContext,
        backend: Arc<UnitTestBackend>,
        version_id: &str,
    ) -> Vec<TrackedStateRow> {
        tracked_state
            .reader(backend)
            .scan_rows(&TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    version_ids: vec![version_id.to_string()],
                    include_tombstones: true,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("tracked rows should scan")
    }

    fn entity_change(
        change_id: &str,
        entity_id: &str,
        schema_key: &str,
        snapshot_content: Option<&str>,
    ) -> CanonicalChange {
        entity_change_at(
            change_id,
            entity_id,
            schema_key,
            snapshot_content,
            "2026-01-01T00:00:00Z",
        )
    }

    fn entity_change_at(
        change_id: &str,
        entity_id: &str,
        schema_key: &str,
        snapshot_content: Option<&str>,
        created_at: &str,
    ) -> CanonicalChange {
        CanonicalChange {
            id: change_id.to_string(),
            entity_id: entity_id.to_string(),
            schema_key: schema_key.to_string(),
            schema_version: "1".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: snapshot_content.map(str::to_string),
            metadata: None,
            created_at: created_at.to_string(),
        }
    }

    fn commit_change(
        change_id: &str,
        commit_id: &str,
        change_ids: &[&str],
        parent_commit_ids: &[&str],
    ) -> CanonicalChange {
        CanonicalChange {
            id: change_id.to_string(),
            entity_id: commit_id.to_string(),
            schema_key: "lix_commit".to_string(),
            schema_version: "1".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(
                serde_json::to_string(&json!({
                    "id": commit_id,
                    "change_ids": change_ids,
                    "parent_commit_ids": parent_commit_ids,
                }))
                .expect("commit snapshot should serialize"),
            ),
            metadata: None,
            created_at: "2026-01-02T00:00:00Z".to_string(),
        }
    }

    fn stale_row(version_id: &str, entity_id: &str) -> TrackedStateRow {
        TrackedStateRow {
            entity_id: entity_id.to_string(),
            schema_key: "test_schema".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some("{}".to_string()),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == GLOBAL_VERSION_ID,
            change_id: format!("change-{entity_id}"),
            commit_id: format!("commit-{version_id}"),
            version_id: version_id.to_string(),
        }
    }
}
