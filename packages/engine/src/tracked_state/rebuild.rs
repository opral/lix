use crate::commit_graph::CommitGraphContext;
use crate::commit_graph::CommitGraphEntity;
use crate::storage::{StorageReader, StorageWriteSet};
use crate::tracked_state::{TrackedStateContext, TrackedStateRow};
use crate::LixError;

/// Summary of a tracked-state rebuild operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TrackedStateRebuildReport {
    pub(crate) written_rows: usize,
}

/// Rebuilds tracked-state rows at one commit from the commit graph.
///
/// The caller provides the read stores and owns the transaction write set.
pub(super) async fn rebuild_state_at_commit<R, S>(
    tracked_state: &TrackedStateContext,
    commit_graph: &CommitGraphContext,
    read_store: R,
    tracked_store: &mut S,
    writes: &mut StorageWriteSet,
    head_commit_id: &str,
) -> Result<TrackedStateRebuildReport, LixError>
where
    R: StorageReader,
    S: StorageReader + ?Sized,
{
    let entities = commit_graph
        .reader(read_store)
        .entities_at(head_commit_id)
        .await?;
    let rows = rows_from_entities(entities);
    let written_rows = rows.len();

    tracked_state
        .writer()
        .stage_root(
            tracked_store,
            writes,
            head_commit_id,
            None,
            rows.iter().map(|row| row.as_ref()),
        )
        .await?;

    Ok(TrackedStateRebuildReport { written_rows })
}

/// Converts commit-graph entities into root-local tracked-state rows.
///
/// The commit graph owns history resolution. This function only maps the
/// effective canonical entities into the storage row shape tracked_state serves.
pub(crate) fn rows_from_entities(entities: Vec<CommitGraphEntity>) -> Vec<TrackedStateRow> {
    entities
        .into_iter()
        .filter(|entity| !is_commit_graph_fact(&entity.change.schema_key))
        .map(tracked_row_from_entity)
        .collect()
}

fn tracked_row_from_entity(entity: CommitGraphEntity) -> TrackedStateRow {
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
        snapshot_ref: change.snapshot_ref,
        metadata_ref: change.metadata_ref,
        created_at,
        updated_at,
        change_id: change.id,
        commit_id: source_commit_id,
    }
}

fn is_commit_graph_fact(schema_key: &str) -> bool {
    schema_key == "lix_commit"
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use crate::backend::testing::UnitTestBackend;
    use crate::commit_graph::CommitGraphContext;
    use crate::commit_store::{Change, ChangeBorrowed, CommitDraftBorrowed, CommitStoreContext};
    use crate::json_store::JsonStoreContext;
    use crate::storage::{StorageContext, StorageWriteSet};
    use crate::tracked_state::{
        MaterializedTrackedStateRow, TrackedStateFilter, TrackedStateScanRequest,
    };

    #[test]
    fn rows_from_entities_converts_normal_entity() {
        let rows = rows_from_entities(vec![entity("change-1", Some("{}"))]);

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(
            row.entity_id,
            crate::entity_identity::EntityIdentity::single("entity-1")
        );
        assert_eq!(row.schema_key, "test_schema");
        assert_eq!(row.file_id.as_deref(), Some("file-1"));
        assert!(row.snapshot_ref.is_some());
        assert!(row.metadata_ref.is_some());
        assert_eq!(row.change_id, "change-1");
        assert_eq!(row.commit_id, "commit-1");
    }

    #[test]
    fn rows_from_entities_preserves_tombstones() {
        let rows = rows_from_entities(vec![entity("change-1", None)]);

        assert_eq!(rows[0].snapshot_ref, None);
    }

    #[test]
    fn rows_from_entities_uses_commit_graph_timestamps() {
        let rows = rows_from_entities(vec![entity("change-1", Some("{}"))]);

        assert_eq!(rows[0].created_at, "2026-01-01T00:00:00Z");
        assert_eq!(rows[0].updated_at, "2026-01-02T00:00:00Z");
    }

    #[test]
    fn rows_from_entities_is_root_local_and_version_independent() {
        let rows = rows_from_entities(vec![entity("change-1", Some("{}"))]);

        assert_eq!(
            rows[0].entity_id,
            crate::entity_identity::EntityIdentity::single("entity-1")
        );
        assert_eq!(rows[0].schema_key, "test_schema");
    }

    #[test]
    fn rows_from_entities_excludes_commit_graph_facts() {
        let rows = rows_from_entities(vec![commit_entity("commit-1")]);

        assert_eq!(rows.len(), 0);
    }

    #[tokio::test]
    async fn rebuild_state_at_commit_writes_rows_from_commit_graph() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let commit_graph = CommitGraphContext::new();
        append_changes(
            storage.clone(),
            &[
                entity_change("change-1", "entity-1", "test_schema", Some("{}")),
                commit_change("commit-1-change", "commit-1", &["change-1"], &[]),
            ],
        )
        .await;

        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        let report = rebuild_state_at_commit(
            &tracked_state,
            &commit_graph,
            storage.clone(),
            tx.as_mut(),
            &mut writes,
            "commit-1",
        )
        .await
        .expect("rebuild should succeed");
        writes
            .apply(&mut tx.as_mut())
            .await
            .expect("rebuild writes should apply");
        tx.commit().await.expect("transaction should commit");

        assert_eq!(report, TrackedStateRebuildReport { written_rows: 1 });
        let rows = scan_rows_at_commit(&tracked_state, storage.clone(), "commit-1").await;
        assert_eq!(rows.len(), 1);
        assert!(rows.iter().any(|row| row.schema_key == "test_schema"
            && row.entity_id == crate::entity_identity::EntityIdentity::single("entity-1")));
    }

    #[tokio::test]
    async fn rebuild_state_at_commit_writes_replacement_root_for_head_commit() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let commit_graph = CommitGraphContext::new();
        append_changes(
            storage.clone(),
            &[
                entity_change("change-new", "entity-new", "test_schema", Some("{}")),
                commit_change("commit-1-change", "commit-1", &["change-new"], &[]),
            ],
        )
        .await;
        seed_tracked_root(
            &tracked_state,
            storage.clone(),
            "stale-commit",
            &[stale_row("version-b", "stale-other")],
        )
        .await;

        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        let report = rebuild_state_at_commit(
            &tracked_state,
            &commit_graph,
            storage.clone(),
            tx.as_mut(),
            &mut writes,
            "commit-1",
        )
        .await
        .expect("rebuild should succeed");
        writes
            .apply(&mut tx.as_mut())
            .await
            .expect("rebuild writes should apply");
        tx.commit().await.expect("transaction should commit");

        assert_eq!(report.written_rows, 1);

        let version_a_rows = scan_rows_at_commit(&tracked_state, storage.clone(), "commit-1").await;
        assert!(!version_a_rows
            .iter()
            .any(|row| row.entity_id
                == crate::entity_identity::EntityIdentity::single("stale-target")));
        assert!(version_a_rows.iter().any(
            |row| row.entity_id == crate::entity_identity::EntityIdentity::single("entity-new")
        ));

        let version_b_rows =
            scan_rows_at_commit(&tracked_state, storage.clone(), "stale-commit").await;
        assert_eq!(version_b_rows.len(), 1);
        assert_eq!(
            version_b_rows[0].entity_id,
            crate::entity_identity::EntityIdentity::single("stale-other")
        );
    }

    #[tokio::test]
    async fn rebuild_state_at_commit_is_content_address_deterministic() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let commit_graph = CommitGraphContext::new();
        append_changes(
            storage.clone(),
            &[
                entity_change("change-1", "entity-1", "test_schema", Some("{\"v\":1}")),
                entity_change("change-2", "entity-2", "test_schema", Some("{\"v\":2}")),
                commit_change(
                    "commit-1-change",
                    "commit-1",
                    &["change-1", "change-2"],
                    &[],
                ),
            ],
        )
        .await;

        rebuild_state_at_commit_for_test(
            &tracked_state,
            &commit_graph,
            storage.clone(),
            "commit-1",
        )
        .await;
        let first_root = load_root(&tracked_state, storage.clone(), "commit-1").await;
        delete_root(&tracked_state, storage.clone(), "commit-1").await;
        rebuild_state_at_commit_for_test(
            &tracked_state,
            &commit_graph,
            storage.clone(),
            "commit-1",
        )
        .await;
        let second_root = load_root(&tracked_state, storage.clone(), "commit-1").await;

        assert_eq!(
            first_root, second_root,
            "rebuilding the same changelog head should produce the same prolly root"
        );
    }

    #[tokio::test]
    async fn rebuild_state_at_commit_uses_latest_change_across_commits() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let commit_graph = CommitGraphContext::new();
        append_changes(
            storage.clone(),
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

        rebuild_state_at_commit_for_test(
            &tracked_state,
            &commit_graph,
            storage.clone(),
            "commit-head",
        )
        .await;

        let rows = scan_rows_at_commit(&tracked_state, storage.clone(), "commit-head").await;
        let row = rows
            .iter()
            .find(|row| {
                row.schema_key == "test_schema"
                    && row.entity_id == crate::entity_identity::EntityIdentity::single("entity-1")
            })
            .expect("rebuilt entity row should exist");
        assert_eq!(row.snapshot_content.as_deref(), Some("{\"value\":\"new\"}"));
        assert_eq!(row.change_id, "change-new");
        assert_eq!(row.commit_id, "commit-head");
        assert_eq!(row.created_at, "2026-01-01T00:00:00Z");
        assert_eq!(row.updated_at, "2026-01-02T00:00:00Z");
    }

    #[tokio::test]
    async fn rebuild_state_at_commit_preserves_tombstone_winner() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let commit_graph = CommitGraphContext::new();
        append_changes(
            storage.clone(),
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

        rebuild_state_at_commit_for_test(
            &tracked_state,
            &commit_graph,
            storage.clone(),
            "commit-head",
        )
        .await;

        let rows = scan_rows_at_commit(&tracked_state, storage.clone(), "commit-head").await;
        let row = rows
            .iter()
            .find(|row| {
                row.schema_key == "test_schema"
                    && row.entity_id == crate::entity_identity::EntityIdentity::single("entity-1")
            })
            .expect("rebuilt tombstone row should exist");
        assert_eq!(row.snapshot_content, None);
        assert_eq!(row.change_id, "change-deleted");
        assert_eq!(row.commit_id, "commit-head");
    }

    #[tokio::test]
    async fn rebuild_state_at_commit_can_rebuild_global_commit_state() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let commit_graph = CommitGraphContext::new();
        append_changes(
            storage.clone(),
            &[
                entity_change("change-1", "entity-1", "test_schema", Some("{}")),
                commit_change("commit-1-change", "commit-1", &["change-1"], &[]),
            ],
        )
        .await;

        rebuild_state_at_commit_for_test(
            &tracked_state,
            &commit_graph,
            storage.clone(),
            "commit-1",
        )
        .await;

        let rows = scan_rows_at_commit(&tracked_state, storage.clone(), "commit-1").await;
        assert!(!rows.is_empty());
        assert!(rows.iter().any(|row| row.schema_key == "test_schema"
            && row.entity_id == crate::entity_identity::EntityIdentity::single("entity-1")));
    }

    #[tokio::test]
    async fn rebuilding_one_commit_state_does_not_rewrite_another_commit_root() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let commit_graph = CommitGraphContext::new();
        append_changes(
            storage.clone(),
            &[
                entity_change("change-global", "entity-global", "test_schema", Some("{}")),
                entity_change("change-main", "entity-main", "test_schema", Some("{}")),
                commit_change(
                    "commit-global-change",
                    "commit-global",
                    &["change-global"],
                    &[],
                ),
                commit_change("commit-main-change", "commit-main", &["change-main"], &[]),
            ],
        )
        .await;

        rebuild_state_at_commit_for_test(
            &tracked_state,
            &commit_graph,
            storage.clone(),
            "commit-global",
        )
        .await;
        let global_root_before = load_root(&tracked_state, storage.clone(), "commit-global").await;

        rebuild_state_at_commit_for_test(
            &tracked_state,
            &commit_graph,
            storage.clone(),
            "commit-main",
        )
        .await;
        let global_root_after = load_root(&tracked_state, storage.clone(), "commit-global").await;

        assert_eq!(
            global_root_after, global_root_before,
            "rebuilding one commit state must not rewrite another commit root"
        );
        let main_rows = scan_rows_at_commit(&tracked_state, storage.clone(), "commit-main").await;
        assert_eq!(main_rows.len(), 1);
        assert_eq!(
            main_rows[0].entity_id,
            crate::entity_identity::EntityIdentity::single("entity-main")
        );
    }

    fn entity(change_id: &str, snapshot_content: Option<&str>) -> CommitGraphEntity {
        CommitGraphEntity {
            change: Change {
                id: change_id.to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
                schema_key: "test_schema".to_string(),
                file_id: Some("file-1".to_string()),
                snapshot_ref: snapshot_content.map(|content| {
                    crate::json_store::JsonRef::from_hash(blake3::hash(content.as_bytes()))
                }),
                metadata_ref: Some(crate::json_store::JsonRef::from_hash(blake3::hash(
                    br#"{"m":1}"#,
                ))),
                created_at: "ignored-change-created-at".to_string(),
            },
            source_commit_id: "commit-1".to_string(),
            depth: 0,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-02T00:00:00Z".to_string(),
        }
    }

    fn commit_entity(commit_id: &str) -> CommitGraphEntity {
        let fixture = commit_change(
            &format!("{commit_id}-change"),
            commit_id,
            &["change-1"],
            &[],
        );
        CommitGraphEntity {
            change: fixture.change,
            source_commit_id: commit_id.to_string(),
            depth: 0,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    #[derive(Clone)]
    struct TestChange {
        change: Change,
        snapshot_content: Option<String>,
        commit_change_ids: Vec<String>,
        parent_commit_ids: Vec<String>,
    }

    impl TestChange {
        fn commit(
            change_id: &str,
            commit_id: &str,
            change_ids: &[&str],
            parent_commit_ids: &[&str],
        ) -> Self {
            Self {
                change: Change {
                    id: change_id.to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single(commit_id),
                    schema_key: "lix_commit".to_string(),
                    file_id: None,
                    snapshot_ref: None,
                    metadata_ref: None,
                    created_at: "2026-01-02T00:00:00Z".to_string(),
                },
                snapshot_content: None,
                commit_change_ids: change_ids.iter().map(|id| id.to_string()).collect(),
                parent_commit_ids: parent_commit_ids.iter().map(|id| id.to_string()).collect(),
            }
        }

        fn entity(
            change_id: &str,
            entity_id: &str,
            schema_key: &str,
            snapshot_content: Option<&str>,
            created_at: &str,
        ) -> Self {
            Self {
                change: Change {
                    id: change_id.to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single(entity_id),
                    schema_key: schema_key.to_string(),
                    file_id: None,
                    snapshot_ref: None,
                    metadata_ref: None,
                    created_at: created_at.to_string(),
                },
                snapshot_content: snapshot_content.map(str::to_string),
                commit_change_ids: Vec::new(),
                parent_commit_ids: Vec::new(),
            }
        }

        fn is_commit(&self) -> bool {
            self.change.schema_key == "lix_commit"
        }
    }

    async fn append_changes(storage: StorageContext, changes: &[TestChange]) {
        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        let mut json_writer = JsonStoreContext::new().writer();
        let canonical_changes = changes
            .iter()
            .filter(|change| !change.is_commit())
            .map(|change| {
                let mut canonical = change.change.clone();
                if let Some(snapshot_content) = change.snapshot_content.as_deref() {
                    canonical.snapshot_ref = Some(
                        json_writer
                            .prepare_json(crate::json_store::NormalizedJson::from_arc_unchecked(
                                Arc::from(snapshot_content),
                            ))
                            .expect("fixture JSON should stage"),
                    );
                }
                canonical
            })
            .collect::<Vec<_>>();
        json_writer.flush_into(&mut writes);
        let changes_by_id = canonical_changes
            .iter()
            .map(|change| (change.id.as_str(), change))
            .collect::<BTreeMap<_, _>>();
        let mut authored_change_ids = BTreeSet::new();
        let commit_store = CommitStoreContext::new();
        for change in changes.iter().filter(|change| change.is_commit()) {
            let commit_id = change
                .change
                .entity_id
                .as_single_string()
                .expect("commit fixture should have id")
                .to_string();
            let author_account_ids = Vec::new();
            let commit = CommitDraftBorrowed {
                id: &commit_id,
                change_id: &change.change.id,
                parent_ids: &change.parent_commit_ids,
                author_account_ids: &author_account_ids,
                created_at: &change.change.created_at,
            };
            let mut authored_changes = Vec::new();
            let mut adopted_changes = Vec::new();
            for change_id in change.commit_change_ids.iter().cloned() {
                let change = changes_by_id
                    .get(change_id.as_str())
                    .expect("commit fixture member change should exist");
                if authored_change_ids.insert(change_id) {
                    authored_changes.push(change_borrowed_from_canonical(change.as_ref()));
                } else {
                    adopted_changes.push(change_borrowed_from_canonical(change.as_ref()));
                }
            }
            commit_store
                .writer(tx.as_mut(), &mut writes)
                .stage_commit_draft(commit, authored_changes, adopted_changes)
                .await
                .expect("commit-store fixture should append");
        }
        writes
            .apply(&mut tx.as_mut())
            .await
            .expect("writes should apply");
        tx.commit().await.expect("transaction should commit");
    }

    fn change_borrowed_from_canonical<'a>(change: ChangeBorrowed<'a>) -> ChangeBorrowed<'a> {
        ChangeBorrowed {
            id: change.id,
            entity_id: change.entity_id,
            schema_key: change.schema_key,
            file_id: change.file_id,
            snapshot_ref: change.snapshot_ref,
            metadata_ref: change.metadata_ref,
            created_at: change.created_at,
        }
    }

    async fn seed_tracked_root(
        tracked_state: &TrackedStateContext,
        storage: StorageContext,
        commit_id: &str,
        rows: &[MaterializedTrackedStateRow],
    ) {
        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        {
            let mut json_writer = JsonStoreContext::new().writer();
            let canonical_rows = rows
                .iter()
                .map(|row| {
                    crate::test_support::tracked_state_row_from_materialized(
                        &mut writes,
                        &mut json_writer,
                        row,
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .expect("rows should canonicalize");
            tracked_state
                .writer()
                .stage_root(
                    &mut tx.as_mut(),
                    &mut writes,
                    commit_id,
                    None,
                    canonical_rows.iter().map(|row| row.as_ref()),
                )
                .await
                .expect("rows should seed");
        }
        writes
            .apply(&mut tx.as_mut())
            .await
            .expect("rows should apply");
        tx.commit().await.expect("transaction should commit");
    }

    async fn rebuild_state_at_commit_for_test(
        tracked_state: &TrackedStateContext,
        commit_graph: &CommitGraphContext,
        storage: StorageContext,
        head_commit_id: &str,
    ) -> TrackedStateRebuildReport {
        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        let report = rebuild_state_at_commit(
            tracked_state,
            commit_graph,
            storage.clone(),
            tx.as_mut(),
            &mut writes,
            head_commit_id,
        )
        .await
        .expect("rebuild should succeed");
        writes
            .apply(&mut tx.as_mut())
            .await
            .expect("rebuild writes should apply");
        tx.commit().await.expect("transaction should commit");
        report
    }

    async fn scan_rows_at_commit(
        tracked_state: &TrackedStateContext,
        storage: StorageContext,
        commit_id: &str,
    ) -> Vec<MaterializedTrackedStateRow> {
        tracked_state
            .reader(storage)
            .scan_rows_at_commit(
                commit_id,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        include_tombstones: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("tracked rows should scan")
    }

    async fn load_root(
        tracked_state: &TrackedStateContext,
        storage: StorageContext,
        commit_id: &str,
    ) -> crate::tracked_state::tree_types::TrackedStateRootId {
        let mut reader = tracked_state.reader(storage);
        reader
            .load_root_for_test(commit_id)
            .await
            .expect("root load should succeed")
            .expect("root should exist")
    }

    async fn delete_root(
        tracked_state: &TrackedStateContext,
        storage: StorageContext,
        commit_id: &str,
    ) {
        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        tracked_state
            .writer()
            .stage_delete_root_for_rebuild(&mut writes, commit_id);
        writes
            .apply(&mut tx.as_mut())
            .await
            .expect("root delete should apply");
        tx.commit().await.expect("transaction should commit");
    }

    fn entity_change(
        change_id: &str,
        entity_id: &str,
        schema_key: &str,
        snapshot_content: Option<&str>,
    ) -> TestChange {
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
    ) -> TestChange {
        TestChange::entity(
            change_id,
            entity_id,
            schema_key,
            snapshot_content,
            created_at,
        )
    }

    fn commit_change(
        change_id: &str,
        commit_id: &str,
        change_ids: &[&str],
        parent_commit_ids: &[&str],
    ) -> TestChange {
        TestChange::commit(change_id, commit_id, change_ids, parent_commit_ids)
    }

    fn stale_row(version_id: &str, entity_id: &str) -> MaterializedTrackedStateRow {
        MaterializedTrackedStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single(entity_id),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot_content: Some("{}".to_string()),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            change_id: format!("change-{entity_id}"),
            commit_id: format!("commit-{version_id}"),
        }
    }
}
