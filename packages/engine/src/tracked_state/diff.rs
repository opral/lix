use crate::changelog::{ChangeLocator, SegmentObjectLocation};
use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::tracked_state::types::{
    TrackedStateIndexValue, TrackedStateKey, TrackedStateTreeScanRequest,
};
use crate::tracked_state::{TrackedStateFilter, TrackedStateStoreReader};
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
    pub(crate) before: Option<TrackedStateDiffRow>,
    /// Raw row in the right root.
    ///
    /// This can be a tombstone. Keeping the raw tombstone is what lets merge
    /// apply deletes without reloading the source root.
    pub(crate) after: Option<TrackedStateDiffRow>,
}

/// Payload-light tracked-state row carried by diff and merge planning.
///
/// This deliberately stores JSON refs, not JSON payload strings. Diff can
/// compare and report rows from tracked-state tree values without hydrating
/// snapshot or metadata bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateDiffRow {
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) deleted: bool,
    pub(crate) snapshot_ref: Option<JsonRef>,
    pub(crate) metadata_ref: Option<JsonRef>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) change_id: String,
    pub(crate) commit_id: String,
    pub(crate) change_location: SegmentObjectLocation,
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
    let mut entries = Vec::with_capacity(tree_entries.len());
    for tree_entry in tree_entries {
        let before = tree_entry
            .before
            .map(|(key, value)| TrackedStateDiffRow::from_tree_entry(key, value));
        let after = tree_entry
            .after
            .map(|(key, value)| TrackedStateDiffRow::from_tree_entry(key, value));
        let identity = match before.as_ref().or(after.as_ref()) {
            Some(row) => TrackedStateDiffIdentity::from(row),
            None => continue,
        };
        let Some(entry) = classify_diff(identity, before, after) else {
            continue;
        };
        entries.push(entry);
    }

    Ok(TrackedStateDiff { entries })
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
    before: Option<TrackedStateDiffRow>,
    after: Option<TrackedStateDiffRow>,
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

fn is_live_row(row: Option<&TrackedStateDiffRow>) -> Option<&TrackedStateDiffRow> {
    row.filter(|row| !row.deleted)
}

fn tracked_row_payload_eq(left: &TrackedStateDiffRow, right: &TrackedStateDiffRow) -> bool {
    left.snapshot_ref == right.snapshot_ref && left.metadata_ref == right.metadata_ref
}

impl TrackedStateDiffIdentity {
    fn from(row: &TrackedStateDiffRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }
}

impl TrackedStateDiffRow {
    fn from_tree_entry(key: TrackedStateKey, value: TrackedStateIndexValue) -> Self {
        Self {
            entity_id: key.entity_id,
            schema_key: key.schema_key,
            file_id: key.file_id,
            deleted: value.deleted,
            snapshot_ref: value.snapshot_ref,
            metadata_ref: value.metadata_ref,
            created_at: value.created_at,
            updated_at: value.updated_at,
            change_id: value.change_locator.change_id,
            commit_id: value.change_locator.commit_id,
            change_location: value.change_locator.location,
        }
    }

    pub(crate) fn into_index_entry(self) -> (TrackedStateKey, TrackedStateIndexValue) {
        (
            TrackedStateKey {
                schema_key: self.schema_key,
                file_id: self.file_id,
                entity_id: self.entity_id,
            },
            TrackedStateIndexValue {
                change_locator: ChangeLocator {
                    change_id: self.change_id,
                    commit_id: self.commit_id,
                    location: self.change_location,
                },
                deleted: self.deleted,
                snapshot_ref: self.snapshot_ref,
                metadata_ref: self.metadata_ref,
                created_at: self.created_at,
                updated_at: self.updated_at,
            },
        )
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
    pub(crate) fn visible_before(&self) -> Option<&TrackedStateDiffRow> {
        self.before.as_ref().filter(|row| !row.deleted)
    }

    #[cfg(test)]
    pub(crate) fn visible_after(&self) -> Option<&TrackedStateDiffRow> {
        self.after.as_ref().filter(|row| !row.deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageContext;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};
    use crate::tracked_state::{MaterializedTrackedStateRow, TrackedStateContext};
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
            entry.after.as_ref().is_some_and(|row| row.deleted),
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
            entry.before.as_ref().is_some_and(|row| row.deleted),
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
        let (storage, tracked_state) = seed_parent_child_delta(
            &[row("entity-a", Some("file-a"), "before-a")],
            &[row("entity-a", Some("file-b"), "after-b")],
        )
        .await;

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let diff = tracked_state
            .reader(read)
            .diff_commits("parent", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("diff should load");

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
        let mut read = storage
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
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        write_root_for_test(
            &mut read,
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
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("parent writes should commit");
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("child read should open");
        let mut writes = storage.new_write_set();
        write_root_for_test(
            &mut read,
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

        let mut read = storage
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
        assert_ne!(
            diff.entries[0]
                .before
                .as_ref()
                .and_then(|row| row.snapshot_ref.as_ref()),
            diff.entries[0]
                .after
                .as_ref()
                .and_then(|row| row.snapshot_ref.as_ref())
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

        let mut read = storage
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
        assert_ne!(
            diff.entries[0]
                .before
                .as_ref()
                .and_then(|row| row.snapshot_ref.as_ref()),
            diff.entries[0]
                .after
                .as_ref()
                .and_then(|row| row.snapshot_ref.as_ref())
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

        let mut read = storage
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
        let mut read = storage
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
        write_root_committed_for_test(&storage, &tracked_state, "left", None, left_rows)
            .await
            .expect("left root should write");
        write_root_committed_for_test(&storage, &tracked_state, "right", None, right_rows)
            .await
            .expect("right root should write");
        (storage, tracked_state)
    }

    async fn seed_parent_child_delta(
        parent_rows: &[MaterializedTrackedStateRow],
        child_rows: &[MaterializedTrackedStateRow],
    ) -> (StorageContext, TrackedStateContext) {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "parent", None, parent_rows)
            .await
            .expect("parent should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            child_rows,
        )
        .await
        .expect("child should write");
        (storage, tracked_state)
    }

    async fn write_root_committed_for_test(
        storage: &StorageContext,
        tracked_state: &TrackedStateContext,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        rows: &[MaterializedTrackedStateRow],
    ) -> Result<(), LixError> {
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        write_root_for_test(
            &mut read,
            &mut writes,
            tracked_state,
            commit_id,
            parent_commit_id,
            rows,
        )
        .await?;
        storage.commit_write_set(writes, StorageWriteOptions::default())?;
        Ok(())
    }

    async fn write_root_for_test(
        read: &mut (impl crate::storage::StorageRead + Send + Sync + ?Sized),
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
