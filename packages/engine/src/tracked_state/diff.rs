use crate::entity_identity::EntityIdentity;
use crate::tracked_state::types::TrackedStateTreeScanRequest;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateFilter, TrackedStateStoreReader,
};
use crate::LixError;

/// Filter for comparing two tracked-state commit roots.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TrackedStateDiffRequest {
    pub(crate) filter: TrackedStateFilter,
}

/// Changed tracked-state rows between two commit roots.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TrackedStateDiff {
    pub(crate) entries: Vec<TrackedStateDiffEntry>,
}

/// One changed identity between two commit roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateDiffEntry {
    pub(crate) identity: TrackedStateDiffIdentity,
    pub(crate) kind: TrackedStateDiffKind,
    /// Raw row in the left root.
    ///
    /// This can be a tombstone. Callers that need user-visible semantics
    /// should use `visible_before()` instead of inspecting this directly.
    pub(crate) before: Option<MaterializedTrackedStateRow>,
    /// Raw row in the right root.
    ///
    /// This can be a tombstone. Keeping the raw tombstone is what lets merge
    /// apply deletes without reloading the source root.
    pub(crate) after: Option<MaterializedTrackedStateRow>,
}

/// Root-local tracked-state identity.
///
/// Entity identity used by merge/diff logic.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TrackedStateDiffIdentity {
    pub(crate) schema_key: String,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) file_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrackedStateDiffKind {
    Added,
    Modified,
    Removed,
}

/// Diffs two tracked-state commit roots.
///
pub(crate) async fn diff_commits<S>(
    reader: &mut TrackedStateStoreReader<S>,
    left_commit_id: &str,
    right_commit_id: &str,
    request: &TrackedStateDiffRequest,
) -> Result<TrackedStateDiff, LixError>
where
    S: crate::storage::StorageRead + Send + Sync,
{
    let scan_request = scan_request_for_diff(request);
    let tree_entries = reader
        .diff_tree_entries_at_commits(left_commit_id, right_commit_id, &scan_request)
        .await?;
    let mut before_entries = Vec::new();
    let mut after_entries = Vec::new();
    let mut pending_entries = Vec::with_capacity(tree_entries.len());
    for tree_entry in tree_entries {
        let before_index = tree_entry.before.map(|entry| {
            let index = before_entries.len();
            before_entries.push(entry);
            index
        });
        let after_index = tree_entry.after.map(|entry| {
            let index = after_entries.len();
            after_entries.push(entry);
            index
        });
        pending_entries.push(PendingDiffEntry {
            before_index,
            after_index,
        });
    }

    let before_rows = reader.materialize_tree_values(before_entries).await?;
    let after_rows = reader.materialize_tree_values(after_entries).await?;
    let mut entries = Vec::new();
    for pending_entry in pending_entries {
        let before = materialized_row_at(pending_entry.before_index, &before_rows)?;
        let after = materialized_row_at(pending_entry.after_index, &after_rows)?;
        let identity = match before.as_ref().or(after.as_ref()) {
            Some(row) => TrackedStateDiffIdentity::from_row(row)?,
            None => continue,
        };
        let Some(entry) = classify_diff(identity, before, after) else {
            continue;
        };
        entries.push(entry);
    }

    Ok(TrackedStateDiff { entries })
}

fn materialized_row_at(
    index: Option<usize>,
    rows: &[MaterializedTrackedStateRow],
) -> Result<Option<MaterializedTrackedStateRow>, LixError> {
    let Some(index) = index else {
        return Ok(None);
    };
    rows.get(index).cloned().map(Some).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state diff materialization returned fewer rows than planned",
        )
    })
}

struct PendingDiffEntry {
    before_index: Option<usize>,
    after_index: Option<usize>,
}

fn scan_request_for_diff(request: &TrackedStateDiffRequest) -> TrackedStateTreeScanRequest {
    let mut filter = request.filter.clone();
    filter.include_tombstones = true;
    TrackedStateTreeScanRequest {
        schema_keys: filter.schema_keys,
        entity_ids: filter.entity_ids,
        file_ids: filter.file_ids,
        include_tombstones: true,
        limit: None,
    }
}

fn classify_diff(
    identity: TrackedStateDiffIdentity,
    before: Option<MaterializedTrackedStateRow>,
    after: Option<MaterializedTrackedStateRow>,
) -> Option<TrackedStateDiffEntry> {
    match (is_live_row(before.as_ref()), is_live_row(after.as_ref())) {
        (None, None) => None,
        (None, Some(_)) => Some(TrackedStateDiffEntry {
            identity,
            kind: TrackedStateDiffKind::Added,
            before,
            after,
        }),
        (Some(_), None) => Some(TrackedStateDiffEntry {
            identity,
            kind: TrackedStateDiffKind::Removed,
            before,
            after,
        }),
        (Some(before), Some(after)) if tracked_row_payload_eq(before, after) => None,
        (Some(_), Some(_)) => Some(TrackedStateDiffEntry {
            identity,
            kind: TrackedStateDiffKind::Modified,
            before,
            after,
        }),
    }
}

fn is_live_row(row: Option<&MaterializedTrackedStateRow>) -> Option<&MaterializedTrackedStateRow> {
    row.filter(|row| row.snapshot_content.is_some())
}

fn tracked_row_payload_eq(
    left: &MaterializedTrackedStateRow,
    right: &MaterializedTrackedStateRow,
) -> bool {
    left.snapshot_content == right.snapshot_content && left.metadata == right.metadata
}

impl TrackedStateDiffIdentity {
    fn from_row(row: &MaterializedTrackedStateRow) -> Result<Self, LixError> {
        Ok(Self {
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        })
    }
}

impl TrackedStateDiffEntry {
    #[cfg(test)]
    pub(crate) fn before_is_live(&self) -> bool {
        self.visible_before().is_some()
    }

    #[cfg(test)]
    pub(crate) fn after_is_live(&self) -> bool {
        self.visible_after().is_some()
    }

    #[cfg(test)]
    pub(crate) fn visible_before(&self) -> Option<&MaterializedTrackedStateRow> {
        self.before
            .as_ref()
            .filter(|row| row.snapshot_content.is_some())
    }

    #[cfg(test)]
    pub(crate) fn visible_after(&self) -> Option<&MaterializedTrackedStateRow> {
        self.after
            .as_ref()
            .filter(|row| row.snapshot_content.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageContext;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};
    use crate::tracked_state::TrackedStateContext;
    use crate::NullableKeyFilter;

    #[tokio::test]
    async fn diff_commits_reports_added_rows() {
        let (storage, tracked_state) = seed_roots(&[], &[row("entity-a", None, "after")]).await;

        let diff = diff(&storage, &tracked_state).await;

        assert_eq!(
            kinds(&diff),
            vec![("entity-a".to_string(), TrackedStateDiffKind::Added)]
        );
        assert!(diff.entries[0].before.is_none());
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .map(|row| row.change_id.as_str()),
            Some("after")
        );
        assert!(!diff.entries[0].before_is_live());
        assert!(diff.entries[0].after_is_live());
    }

    #[tokio::test]
    async fn diff_commits_reports_removed_rows_when_right_side_is_absent() {
        let (storage, tracked_state) = seed_roots(&[row("entity-a", None, "before")], &[]).await;

        let diff = diff(&storage, &tracked_state).await;

        assert_eq!(
            kinds(&diff),
            vec![("entity-a".to_string(), TrackedStateDiffKind::Removed)]
        );
        assert_eq!(
            diff.entries[0]
                .before
                .as_ref()
                .map(|row| row.change_id.as_str()),
            Some("before")
        );
        assert!(diff.entries[0].after.is_none());
        assert!(diff.entries[0].before_is_live());
        assert!(!diff.entries[0].after_is_live());
    }

    #[tokio::test]
    async fn diff_commits_reports_removed_rows_when_right_side_is_tombstone() {
        let (storage, tracked_state) = seed_roots(
            &[row("entity-a", None, "before")],
            &[tombstone("entity-a", None, "delete")],
        )
        .await;

        let diff = diff(&storage, &tracked_state).await;

        assert_eq!(
            kinds(&diff),
            vec![("entity-a".to_string(), TrackedStateDiffKind::Removed)]
        );
        let entry = &diff.entries[0];
        assert_eq!(
            entry.after.as_ref().map(|row| row.change_id.as_str()),
            Some("delete")
        );
        assert!(
            entry
                .after
                .as_ref()
                .is_some_and(|row| row.snapshot_content.is_none()),
            "removed diff should preserve the right-side tombstone for merge"
        );
        assert!(entry.before_is_live());
        assert!(!entry.after_is_live());
    }

    #[tokio::test]
    async fn diff_commits_reports_added_rows_when_left_side_is_tombstone() {
        let (storage, tracked_state) = seed_roots(
            &[tombstone("entity-a", None, "delete")],
            &[row("entity-a", None, "after")],
        )
        .await;

        let diff = diff(&storage, &tracked_state).await;

        assert_eq!(
            kinds(&diff),
            vec![("entity-a".to_string(), TrackedStateDiffKind::Added)]
        );
        let entry = &diff.entries[0];
        assert_eq!(
            entry.before.as_ref().map(|row| row.change_id.as_str()),
            Some("delete")
        );
        assert!(
            entry
                .before
                .as_ref()
                .is_some_and(|row| row.snapshot_content.is_none()),
            "added diff should preserve the left-side tombstone for merge"
        );
        assert!(!entry.before_is_live());
        assert!(entry.after_is_live());
    }

    #[tokio::test]
    async fn diff_commits_reports_modified_rows_for_changed_payload() {
        let (storage, tracked_state) = seed_roots(
            &[row_with_value("entity-a", None, "before", "one")],
            &[row_with_value("entity-a", None, "after", "two")],
        )
        .await;

        let diff = diff(&storage, &tracked_state).await;

        assert_eq!(
            kinds(&diff),
            vec![("entity-a".to_string(), TrackedStateDiffKind::Modified)]
        );
        assert!(diff.entries[0].before_is_live());
        assert!(diff.entries[0].after_is_live());
    }

    #[tokio::test]
    async fn diff_commits_omits_unchanged_rows_even_when_metadata_differs_only_by_commit() {
        let (storage, tracked_state) = seed_roots(
            &[row_with_value("entity-a", None, "before", "same")],
            &[row_with_value("entity-a", None, "after", "same")],
        )
        .await;

        let diff = diff(&storage, &tracked_state).await;

        assert!(diff.entries.is_empty());
    }

    #[tokio::test]
    async fn diff_commits_distinguishes_same_entity_with_different_file_id() {
        let (storage, tracked_state) = seed_roots(
            &[row("entity-a", Some("file-a"), "before-a")],
            &[
                row("entity-a", Some("file-a"), "before-a"),
                row("entity-a", Some("file-b"), "after-b"),
            ],
        )
        .await;

        let diff = diff(&storage, &tracked_state).await;

        assert_eq!(diff.entries.len(), 1);
        assert_eq!(diff.entries[0].identity.file_id.as_deref(), Some("file-b"));
        assert_eq!(diff.entries[0].kind, TrackedStateDiffKind::Added);
    }

    #[tokio::test]
    async fn diff_commits_filters_by_schema_entity_and_file_id() {
        let (storage, tracked_state) = seed_roots(
            &[],
            &[
                row_with_schema("entity-a", Some("file-a"), "schema-a", "change-a"),
                row_with_schema("entity-b", Some("file-b"), "schema-b", "change-b"),
            ],
        )
        .await;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = tracked_state.reader(read);
        let diff = reader
            .diff_commits(
                "left",
                "right",
                &TrackedStateDiffRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema-b".to_string()],
                        entity_ids: vec![crate::entity_identity::EntityIdentity::single(
                            "entity-b",
                        )],
                        file_ids: vec![NullableKeyFilter::Value("file-b".to_string())],
                        ..Default::default()
                    },
                },
            )
            .await
            .expect("diff should load");

        assert_eq!(
            kinds(&diff),
            vec![("entity-b".to_string(), TrackedStateDiffKind::Added)]
        );
    }

    #[tokio::test]
    async fn diff_commits_between_delta_parent_and_child_reports_suffix_rows() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        write_root_for_test(
            &read,
            &mut writes,
            &tracked_state,
            "parent",
            None,
            &[
                row_with_value("entity-a", None, "parent-a", "before"),
                row_with_value("entity-b", None, "parent-b", "same"),
            ],
        )
        .await
        .expect("parent should write");
        write_root_for_test(
            &read,
            &mut writes,
            &tracked_state,
            "child",
            Some("parent"),
            &[row_with_value("entity-a", None, "child-a", "after")],
        )
        .await
        .expect("child should write");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let diff = tracked_state
            .reader(read)
            .diff_commits("parent", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("diff should load");

        assert_eq!(
            kinds(&diff),
            vec![("entity-a".to_string(), TrackedStateDiffKind::Modified)]
        );
        assert_eq!(
            diff.entries[0]
                .before
                .as_ref()
                .and_then(|row| row.snapshot_content.as_deref()),
            Some("{\"value\":\"before\"}")
        );
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .and_then(|row| row.snapshot_content.as_deref()),
            Some("{\"value\":\"after\"}")
        );
    }

    #[tokio::test]
    async fn diff_commits_between_delta_child_and_parent_reports_reverse_suffix_rows() {
        let (storage, tracked_state) = seed_parent_child_delta(
            &[
                row_with_value("entity-a", None, "parent-a", "before"),
                row_with_value("entity-b", None, "parent-b", "same"),
            ],
            &[row_with_value("entity-a", None, "child-a", "after")],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let diff = tracked_state
            .reader(read)
            .diff_commits("child", "parent", &TrackedStateDiffRequest::default())
            .await
            .expect("diff should load");

        assert_eq!(
            kinds(&diff),
            vec![("entity-a".to_string(), TrackedStateDiffKind::Modified)]
        );
        assert_eq!(
            diff.entries[0]
                .before
                .as_ref()
                .and_then(|row| row.snapshot_content.as_deref()),
            Some("{\"value\":\"after\"}")
        );
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .and_then(|row| row.snapshot_content.as_deref()),
            Some("{\"value\":\"before\"}")
        );
    }

    #[tokio::test]
    async fn diff_commits_between_delta_parent_and_child_preserves_suffix_tombstones() {
        let (storage, tracked_state) = seed_parent_child_delta(
            &[
                row_with_value("entity-a", None, "parent-a", "before"),
                row_with_value("entity-b", None, "parent-b", "same"),
            ],
            &[tombstone("entity-a", None, "child-delete")],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let diff = tracked_state
            .reader(read)
            .diff_commits("parent", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("diff should load");

        assert_eq!(
            kinds(&diff),
            vec![("entity-a".to_string(), TrackedStateDiffKind::Removed)]
        );
        assert!(diff.entries[0].before_is_live());
        assert!(!diff.entries[0].after_is_live());
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .map(|row| row.change_id.as_str()),
            Some("child-delete")
        );
    }

    async fn diff(
        storage: &StorageContext,
        tracked_state: &TrackedStateContext,
    ) -> TrackedStateDiff {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        tracked_state
            .reader(read)
            .diff_commits("left", "right", &TrackedStateDiffRequest::default())
            .await
            .expect("diff should load")
    }

    async fn seed_roots(
        left_rows: &[MaterializedTrackedStateRow],
        right_rows: &[MaterializedTrackedStateRow],
    ) -> (StorageContext, TrackedStateContext) {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        write_root_for_test(&read, &mut writes, &tracked_state, "left", None, left_rows)
            .await
            .expect("left root should write");
        write_root_for_test(
            &read,
            &mut writes,
            &tracked_state,
            "right",
            None,
            right_rows,
        )
        .await
        .expect("right root should write");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should commit");
        (storage, tracked_state)
    }

    async fn seed_parent_child_delta(
        parent_rows: &[MaterializedTrackedStateRow],
        child_rows: &[MaterializedTrackedStateRow],
    ) -> (StorageContext, TrackedStateContext) {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        write_root_for_test(
            &read,
            &mut writes,
            &tracked_state,
            "parent",
            None,
            parent_rows,
        )
        .await
        .expect("parent should write");
        write_root_for_test(
            &read,
            &mut writes,
            &tracked_state,
            "child",
            Some("parent"),
            child_rows,
        )
        .await
        .expect("child should write");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should commit");
        (storage, tracked_state)
    }

    async fn write_root_for_test(
        read: &(impl crate::storage::StorageRead + Send + Sync + ?Sized),
        writes: &mut crate::storage::StorageWriteSet,
        tracked_state: &TrackedStateContext,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        rows: &[MaterializedTrackedStateRow],
    ) -> Result<(), LixError> {
        crate::test_support::stage_tracked_root_from_materialized(
            read,
            writes,
            tracked_state,
            commit_id,
            parent_commit_id,
            rows,
        )
        .await
    }

    fn kinds(diff: &TrackedStateDiff) -> Vec<(String, TrackedStateDiffKind)> {
        diff.entries
            .iter()
            .map(|entry| {
                (
                    entry
                        .identity
                        .entity_id
                        .as_single_string_owned()
                        .expect("identity"),
                    entry.kind,
                )
            })
            .collect()
    }

    fn tombstone(
        entity_id: &str,
        file_id: Option<&str>,
        change_id: &str,
    ) -> MaterializedTrackedStateRow {
        let mut row = row(entity_id, file_id, change_id);
        row.snapshot_content = None;
        row.deleted = true;
        row
    }

    fn row(entity_id: &str, file_id: Option<&str>, change_id: &str) -> MaterializedTrackedStateRow {
        row_with_schema(entity_id, file_id, "test_schema", change_id)
    }

    fn row_with_schema(
        entity_id: &str,
        file_id: Option<&str>,
        schema_key: &str,
        change_id: &str,
    ) -> MaterializedTrackedStateRow {
        row_with_schema_and_value(entity_id, file_id, schema_key, change_id, "value")
    }

    fn row_with_value(
        entity_id: &str,
        file_id: Option<&str>,
        change_id: &str,
        value: &str,
    ) -> MaterializedTrackedStateRow {
        row_with_schema_and_value(entity_id, file_id, "test_schema", change_id, value)
    }

    fn row_with_schema_and_value(
        entity_id: &str,
        file_id: Option<&str>,
        schema_key: &str,
        change_id: &str,
        value: &str,
    ) -> MaterializedTrackedStateRow {
        MaterializedTrackedStateRow {
            entity_id: EntityIdentity::single(entity_id),
            schema_key: schema_key.to_string(),
            file_id: file_id.map(str::to_string),
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            change_id: change_id.to_string(),
            commit_id: change_id.replace("change", "commit"),
        }
    }
}
