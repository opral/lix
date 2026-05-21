use crate::entity_pk::EntityPk;
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
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) deleted: bool,
    pub(crate) snapshot_ref: Option<JsonRef>,
    pub(crate) metadata_ref: Option<JsonRef>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) change_id: String,
    pub(crate) commit_id: String,
}

/// Root-local tracked-state identity.
///
/// Entity pk used by merge/diff logic.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TrackedStateDiffIdentity {
    pub(crate) schema_key: String,
    pub(crate) entity_pk: EntityPk,
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
    diff_commits_with_validation(reader, left_commit_id, right_commit_id, request, true, true).await
}

pub(crate) async fn diff_commits_with_validation<S>(
    reader: &mut TrackedStateStoreReader<S>,
    left_commit_id: &str,
    right_commit_id: &str,
    request: &TrackedStateDiffRequest,
    validate_left_root: bool,
    validate_right_root: bool,
) -> Result<TrackedStateDiff, LixError>
where
    S: crate::storage::StorageRead + Send + Sync,
{
    let scan_request = scan_request_for_diff(request);
    let tree_entries = reader
        .diff_tree_entries_at_commits(left_commit_id, right_commit_id, &scan_request)
        .await?;
    if validate_left_root {
        reader
            .validate_tree_rows_at_commit_against_changelog(left_commit_id, &scan_request)
            .await?;
    }
    if validate_right_root && left_commit_id != right_commit_id {
        reader
            .validate_tree_rows_at_commit_against_changelog(right_commit_id, &scan_request)
            .await?;
    }
    let mut raw_rows = Vec::with_capacity(tree_entries.len());
    for tree_entry in tree_entries.into_iter() {
        let before = tree_entry
            .before
            .map(|(key, value)| TrackedStateDiffRow::from_tree_entry(key, value));
        let after = tree_entry
            .after
            .map(|(key, value)| TrackedStateDiffRow::from_tree_entry(key, value));
        raw_rows.push((before, after));
    }

    let mut entries = Vec::with_capacity(raw_rows.len());
    for (before, after) in raw_rows {
        let identity = match before.as_ref().or(after.as_ref()) {
            Some(row) => TrackedStateDiffIdentity::from(row),
            None => continue,
        };
        if identity.schema_key == "lix_commit" {
            continue;
        }
        let Some(entry) = classify_diff(identity, before, after) else {
            continue;
        };
        entries.push(entry);
    }

    let diff = TrackedStateDiff { entries };
    Ok(diff)
}

fn scan_request_for_diff(request: &TrackedStateDiffRequest) -> TrackedStateTreeScanRequest {
    let mut filter = request.filter.clone();
    filter.include_tombstones = true;
    TrackedStateTreeScanRequest {
        schema_keys: filter.schema_keys,
        entity_pks: filter.entity_pks,
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
            entity_pk: row.entity_pk.clone(),
            file_id: row.file_id.clone(),
        }
    }
}

impl TrackedStateDiffRow {
    pub(crate) fn from_tree_entry(key: TrackedStateKey, value: TrackedStateIndexValue) -> Self {
        Self {
            entity_pk: key.entity_pk,
            schema_key: key.schema_key,
            file_id: key.file_id,
            deleted: value.deleted,
            snapshot_ref: value.snapshot_ref,
            metadata_ref: value.metadata_ref,
            created_at: value.created_at,
            updated_at: value.updated_at,
            change_id: value.change_id,
            commit_id: value.commit_id,
        }
    }

    pub(crate) fn into_index_entry(self) -> (TrackedStateKey, TrackedStateIndexValue) {
        (
            TrackedStateKey {
                schema_key: self.schema_key,
                file_id: self.file_id,
                entity_pk: self.entity_pk,
            },
            TrackedStateIndexValue {
                change_id: self.change_id,
                commit_id: self.commit_id,
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
    use crate::tracked_state::types::{
        TrackedStateCommitRoot, TrackedStateCommitRootParent, TrackedStateMutation,
        TrackedStateRootId,
    };
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

        let read = storage
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
                        entity_pks: vec![crate::entity_pk::EntityPk::single("entity-b")],
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
    async fn diff_validation_rejects_row_identity_that_does_not_match_changelog_change() {
        let (storage, tracked_state) = seed_roots(&[], &[row("entity-a", None, "after")]).await;
        let mut diff = diff(&storage, &tracked_state).await;
        diff.entries[0].after.as_mut().expect("after row").entity_pk =
            EntityPk::single("entity-corrupt");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .validate_diff_rows_for_commits_against_changelog(&[(
                diff.entries[0].after.as_ref().expect("after row"),
                "right",
            )])
            .await
            .expect_err("identity drift must be rejected");

        assert!(
            error
                .message
                .contains("does not match changelog change identity")
                || error.message.contains("changelog commit"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_validation_rejects_missing_changelog_change() {
        let (storage, tracked_state) = seed_roots(&[], &[row("entity-a", None, "after")]).await;
        let mut diff = diff(&storage, &tracked_state).await;
        diff.entries[0].after.as_mut().expect("after row").change_id = "missing-change".to_string();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .validate_diff_rows_for_commits_against_changelog(&[(
                diff.entries[0].after.as_ref().expect("after row"),
                "right",
            )])
            .await
            .expect_err("missing change must be rejected");

        assert!(
            error.message.contains("missing changelog change"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_validation_rejects_forged_updated_at() {
        let (storage, tracked_state) = seed_roots(&[], &[row("entity-a", None, "after")]).await;
        let mut diff = diff(&storage, &tracked_state).await;
        diff.entries[0]
            .after
            .as_mut()
            .expect("after row")
            .updated_at = "2026-01-02T00:00:00Z".to_string();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .validate_diff_rows_for_commits_against_changelog(&[(
                diff.entries[0].after.as_ref().expect("after row"),
                "right",
            )])
            .await
            .expect_err("forged updated_at must be rejected");

        assert!(
            error.message.contains("updated_at does not match"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_validation_rejects_forged_created_at() {
        let (storage, tracked_state) = seed_roots(&[], &[row("entity-a", None, "after")]).await;
        let mut diff = diff(&storage, &tracked_state).await;
        diff.entries[0]
            .after
            .as_mut()
            .expect("after row")
            .created_at = "2025-12-31T00:00:00Z".to_string();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .validate_diff_rows_for_commits_against_changelog(&[(
                diff.entries[0].after.as_ref().expect("after row"),
                "right",
            )])
            .await
            .expect_err("forged created_at must be rejected");

        assert!(
            error.message.contains("created_at"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_rejects_update_with_arbitrary_forged_created_at() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row_with_times(
                "entity-a",
                None,
                "parent-change",
                "old",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
            )],
        )
        .await
        .expect("parent root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[row_with_times(
                "entity-a",
                None,
                "child-change",
                "new",
                "2026-01-02T00:00:00Z",
                "2026-01-02T00:00:00Z",
            )],
        )
        .await
        .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let valid_diff = tracked_state
            .reader(read)
            .diff_commits("left", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("valid update should load");
        let row = valid_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("child row should appear");
        let (key, mut value) = row.into_index_entry();
        value.created_at = "2026-01-03T00:00:00Z".to_string();
        let parent_commit_row =
            commit_root_row_entry("parent", "parent:commit", "2026-01-01T00:00:00Z");
        let commit_row = commit_root_row_entry("child", "child:commit", "2026-01-02T00:00:00Z");
        stage_corrupt_commit_root(
            &storage,
            "child",
            vec![(key, value), parent_commit_row, commit_row],
            vec![TrackedStateCommitRootParent {
                commit_id: "parent".to_string(),
                root_id: tracked_state_root_id(&storage, "parent").await,
            }],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "child", &TrackedStateDiffRequest::default())
            .await
            .expect_err("arbitrary forged created_at must be rejected");

        assert!(
            error.message.contains("created_at"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_validates_same_payload_rows_before_classification_drops_them() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "left",
            None,
            &[row_with_value("entity-a", None, "left-a", "same")],
        )
        .await
        .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "right-valid",
            None,
            &[row_with_value("entity-b", None, "right-b", "same")],
        )
        .await
        .expect("right changelog should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let valid_diff = tracked_state
            .reader(read)
            .diff_commits("left", "right-valid", &TrackedStateDiffRequest::default())
            .await
            .expect("valid diff should load");
        let source_row = valid_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("right row should appear in valid diff");
        let (_source_key, source_value) = source_row.into_index_entry();
        let corrupt_key = TrackedStateKey {
            schema_key: "test_schema".to_string(),
            file_id: None,
            entity_pk: EntityPk::single("entity-a"),
        };
        let result = {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let mut writes = storage.new_write_set();
            let result = crate::tracked_state::tree::TrackedStateTree::new()
                .apply_mutations(
                    &mut read,
                    &mut writes,
                    None,
                    vec![TrackedStateMutation::put_encoded(
                        crate::tracked_state::codec::encode_key(&corrupt_key),
                        crate::tracked_state::codec::encode_value(&source_value),
                    )],
                    Some("right-corrupt"),
                )
                .await
                .expect("corrupt root should write");
            crate::tracked_state::storage::stage_commit_root(
                &mut writes,
                &TrackedStateCommitRoot {
                    commit_id: "right-corrupt".to_string(),
                    root_id: result.root_id.clone(),
                    parent_roots: Vec::new(),
                    changed_key_count: 1,
                    row_count_estimate: result.row_count as u64,
                    tree_height: result.tree_height as u32,
                    primary_chunk_count: result.chunk_count as u64,
                    primary_chunk_bytes: result.chunk_bytes as u64,
                },
            )
            .expect("metadata should encode");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("corrupt root should commit");
            result
        };
        assert_eq!(result.row_count, 1);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "right-corrupt", &TrackedStateDiffRequest::default())
            .await
            .expect_err("raw same-payload corruption must be rejected before classification");

        assert!(
            error
                .message
                .contains("does not match changelog change identity")
                || error.message.contains("changelog commit"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_rejects_stale_ancestor_row_that_is_not_root_winner() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row_with_value("entity-a", None, "parent-change", "old")],
        )
        .await
        .expect("parent root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[row_with_value("entity-a", None, "child-change", "new")],
        )
        .await
        .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let parent_diff = tracked_state
            .reader(read)
            .diff_commits("left", "parent", &TrackedStateDiffRequest::default())
            .await
            .expect("parent diff should load");
        let stale_row = parent_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("parent row should appear");
        let (stale_key, stale_value) = stale_row.into_index_entry();
        stage_corrupt_commit_root(
            &storage,
            "child",
            vec![(stale_key, stale_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: "parent".to_string(),
                root_id: tracked_state_root_id(&storage, "parent").await,
            }],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "child", &TrackedStateDiffRequest::default())
            .await
            .expect_err("stale ancestor winner must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_rejects_valid_change_from_unreachable_commit_root() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "unrelated",
            None,
            &[row_with_value(
                "entity-a",
                None,
                "unrelated-change",
                "value",
            )],
        )
        .await
        .expect("unrelated changelog should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let unrelated_diff = tracked_state
            .reader(read)
            .diff_commits("left", "unrelated", &TrackedStateDiffRequest::default())
            .await
            .expect("valid unrelated diff should load");
        let source_row = unrelated_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("unrelated row should appear in valid diff");
        let (source_key, source_value) = source_row.into_index_entry();

        let result = {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_empty_changelog_commit(
                &mut read,
                &mut writes,
                "right-corrupt",
                None,
            )
            .await
            .expect("empty right changelog should write");
            let result = crate::tracked_state::tree::TrackedStateTree::new()
                .apply_mutations(
                    &mut read,
                    &mut writes,
                    None,
                    vec![TrackedStateMutation::put_encoded(
                        crate::tracked_state::codec::encode_key(&source_key),
                        crate::tracked_state::codec::encode_value(&source_value),
                    )],
                    Some("right-corrupt"),
                )
                .await
                .expect("corrupt root should write");
            crate::tracked_state::storage::stage_commit_root(
                &mut writes,
                &TrackedStateCommitRoot {
                    commit_id: "right-corrupt".to_string(),
                    root_id: result.root_id.clone(),
                    parent_roots: Vec::new(),
                    changed_key_count: 1,
                    row_count_estimate: result.row_count as u64,
                    tree_height: result.tree_height as u32,
                    primary_chunk_count: result.chunk_count as u64,
                    primary_chunk_bytes: result.chunk_bytes as u64,
                },
            )
            .expect("metadata should encode");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("corrupt root should commit");
            result
        };
        assert_eq!(result.row_count, 1);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "right-corrupt", &TrackedStateDiffRequest::default())
            .await
            .expect_err("unreachable valid change must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_rejects_second_parent_row_without_commit_root_proof() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(&storage, &tracked_state, "target", None, &[])
            .await
            .expect("target root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "source",
            None,
            &[row_with_value("entity-a", None, "source-change", "value")],
        )
        .await
        .expect("source root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let source_diff = tracked_state
            .reader(read)
            .diff_commits("left", "source", &TrackedStateDiffRequest::default())
            .await
            .expect("source diff should load");
        let source_row = source_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("source row should appear");
        let (source_key, source_value) = source_row.into_index_entry();

        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_empty_changelog_commit_with_parents(
                &mut read,
                &mut writes,
                "merge",
                &["target".to_string(), "source".to_string()],
            )
            .await
            .expect("merge changelog should write");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("merge changelog should commit");
        }
        stage_corrupt_commit_root(
            &storage,
            "merge",
            vec![(source_key, source_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: "target".to_string(),
                root_id: tracked_state_root_id(&storage, "target").await,
            }],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "merge", &TrackedStateDiffRequest::default())
            .await
            .expect_err("second-parent row without commit-root proof must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_rejects_second_parent_row_with_forged_commit_root_parent() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(&storage, &tracked_state, "target", None, &[])
            .await
            .expect("target root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "source",
            None,
            &[row_with_value("entity-a", None, "source-change", "value")],
        )
        .await
        .expect("source root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let source_diff = tracked_state
            .reader(read)
            .diff_commits("left", "source", &TrackedStateDiffRequest::default())
            .await
            .expect("source diff should load");
        let source_row = source_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("source row should appear");
        let (source_key, source_value) = source_row.into_index_entry();

        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_empty_changelog_commit_with_parents(
                &mut read,
                &mut writes,
                "merge",
                &["target".to_string(), "source".to_string()],
            )
            .await
            .expect("merge changelog should write");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("merge changelog should commit");
        }
        stage_corrupt_commit_root(
            &storage,
            "merge",
            vec![(source_key, source_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: "source".to_string(),
                root_id: tracked_state_root_id(&storage, "source").await,
            }],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "merge", &TrackedStateDiffRequest::default())
            .await
            .expect_err("forged source parent must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_rejects_unrelated_row_with_forged_commit_root_parent() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "source",
            None,
            &[row_with_value("entity-a", None, "source-change", "value")],
        )
        .await
        .expect("source root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let source_diff = tracked_state
            .reader(read)
            .diff_commits("left", "source", &TrackedStateDiffRequest::default())
            .await
            .expect("source diff should load");
        let source_row = source_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("source row should appear");
        let (source_key, source_value) = source_row.into_index_entry();

        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_empty_changelog_commit(
                &mut read,
                &mut writes,
                "right-corrupt",
                None,
            )
            .await
            .expect("empty right changelog should write");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("right changelog should commit");
        }
        stage_corrupt_commit_root(
            &storage,
            "right-corrupt",
            vec![(source_key, source_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: "source".to_string(),
                root_id: tracked_state_root_id(&storage, "source").await,
            }],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "right-corrupt", &TrackedStateDiffRequest::default())
            .await
            .expect_err("forged unrelated parent must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_rejects_forged_parent_metadata_even_for_current_winner_rows() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(&storage, &tracked_state, "target", None, &[])
            .await
            .expect("target root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "source",
            None,
            &[row_with_value("entity-b", None, "source-b", "source")],
        )
        .await
        .expect("source root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("target"),
            &[row_with_value("entity-a", None, "child-a", "current")],
        )
        .await
        .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let child_diff = tracked_state
            .reader(read)
            .diff_commits("left", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("child diff should load");
        let child_row = child_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("child row should appear");
        let (child_key, child_value) = child_row.into_index_entry();

        stage_corrupt_commit_root(
            &storage,
            "child",
            vec![(child_key, child_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: "source".to_string(),
                root_id: tracked_state_root_id(&storage, "source").await,
            }],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "child", &TrackedStateDiffRequest::default())
            .await
            .expect_err("current winner root metadata must still be validated");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_rejects_stale_grandparent_row_with_forged_commit_root_parent() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "grandparent",
            None,
            &[row_with_value("entity-a", None, "grandparent-a", "old")],
        )
        .await
        .expect("grandparent root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "parent",
            Some("grandparent"),
            &[row_with_value("entity-a", None, "parent-a", "new")],
        )
        .await
        .expect("parent root should write");
        write_root_committed_for_test(&storage, &tracked_state, "child", Some("parent"), &[])
            .await
            .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let stale_diff = tracked_state
            .reader(read)
            .diff_commits("left", "grandparent", &TrackedStateDiffRequest::default())
            .await
            .expect("grandparent diff should load");
        let stale_row = stale_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("grandparent row should appear");
        let (stale_key, stale_value) = stale_row.into_index_entry();

        stage_corrupt_commit_root(
            &storage,
            "child",
            vec![(stale_key, stale_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: "grandparent".to_string(),
                root_id: tracked_state_root_id(&storage, "grandparent").await,
            }],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "child", &TrackedStateDiffRequest::default())
            .await
            .expect_err("forged grandparent parent must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_allows_rows_reachable_through_parent_commit() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row_with_value("entity-a", None, "parent-change", "value")],
        )
        .await
        .expect("parent root should write");
        write_root_committed_for_test(&storage, &tracked_state, "child", Some("parent"), &[])
            .await
            .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let diff = tracked_state
            .reader(read)
            .diff_commits("left", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("ancestor-reachable row should validate");

        assert_eq!(
            kinds(&diff),
            vec![("entity-a".to_string(), TrackedStateDiffKind::Added)]
        );
    }

    #[tokio::test]
    async fn diff_commits_allows_source_update_with_source_created_at() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "target", None, &[])
            .await
            .expect("target root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "source-add",
            None,
            &[row_with_times(
                "entity-a",
                None,
                "source-add-a",
                "old",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
            )],
        )
        .await
        .expect("source add root should write");
        let source_update = row_with_times(
            "entity-a",
            None,
            "source-update-a",
            "new",
            "2026-01-01T00:00:00Z",
            "2026-01-02T00:00:00Z",
        );
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "source-update",
            Some("source-add"),
            std::slice::from_ref(&source_update),
        )
        .await
        .expect("source update root should write");
        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_tracked_root_from_materialized_with_parents(
                &mut read,
                &mut writes,
                &tracked_state,
                "merge",
                &["target".to_string(), "source-update".to_string()],
                Some("target"),
                std::slice::from_ref(&source_update),
            )
            .await
            .expect("merge root should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("merge root should commit");
        }

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let diff = tracked_state
            .reader(read)
            .diff_commits("target", "merge", &TrackedStateDiffRequest::default())
            .await
            .expect("source update should validate");

        assert_eq!(
            kinds(&diff),
            vec![("entity-a".to_string(), TrackedStateDiffKind::Added)]
        );
        let row = diff.entries[0].after.as_ref().expect("after row");
        assert_eq!(row.created_at, "2026-01-01T00:00:00Z");
        assert_eq!(row.updated_at, "2026-01-02T00:00:00Z");
        assert_eq!(row.change_id, "source-update-a");
    }

    #[tokio::test]
    async fn diff_commits_rejects_omitted_inherited_row_even_when_diff_is_non_empty() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row_with_value("entity-a", None, "parent-a", "inherited")],
        )
        .await
        .expect("parent root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[row_with_value("entity-b", None, "child-b", "unrelated")],
        )
        .await
        .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let valid_diff = tracked_state
            .reader(read)
            .diff_commits("parent", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("valid child diff should load");
        let unrelated_row = valid_diff
            .entries
            .iter()
            .find_map(|entry| {
                entry
                    .after
                    .as_ref()
                    .filter(|row| row.change_id == "child-b")
                    .cloned()
            })
            .expect("unrelated child row should appear");
        let (unrelated_key, unrelated_value) = unrelated_row.into_index_entry();
        stage_corrupt_commit_root(
            &storage,
            "child",
            vec![(unrelated_key, unrelated_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: "parent".to_string(),
                root_id: tracked_state_root_id(&storage, "parent").await,
            }],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("parent", "child", &TrackedStateDiffRequest::default())
            .await
            .expect_err("omitted inherited row must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_rejects_omitted_updated_row_even_when_diff_is_non_empty() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row_with_value("entity-a", None, "parent-a", "old")],
        )
        .await
        .expect("parent root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[
                row_with_value("entity-a", None, "child-a", "new"),
                row_with_value("entity-b", None, "child-b", "unrelated"),
            ],
        )
        .await
        .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let valid_diff = tracked_state
            .reader(read)
            .diff_commits("parent", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("valid child diff should load");
        let unrelated_row = valid_diff
            .entries
            .iter()
            .find_map(|entry| {
                entry
                    .after
                    .as_ref()
                    .filter(|row| row.change_id == "child-b")
                    .cloned()
            })
            .expect("unrelated child row should appear");
        let (unrelated_key, unrelated_value) = unrelated_row.into_index_entry();
        stage_corrupt_commit_root(
            &storage,
            "child",
            vec![(unrelated_key, unrelated_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: "parent".to_string(),
                root_id: tracked_state_root_id(&storage, "parent").await,
            }],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("parent", "child", &TrackedStateDiffRequest::default())
            .await
            .expect_err("omitted updated row must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_rejects_shared_omitted_row_even_when_diff_is_non_empty() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row_with_value("entity-a", None, "parent-a", "shared")],
        )
        .await
        .expect("parent root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "left",
            Some("parent"),
            &[row_with_value("entity-b", None, "left-b", "left")],
        )
        .await
        .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "right",
            Some("parent"),
            &[row_with_value("entity-c", None, "right-c", "right")],
        )
        .await
        .expect("right root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let left_diff = tracked_state
            .reader(read)
            .diff_commits("parent", "left", &TrackedStateDiffRequest::default())
            .await
            .expect("left diff should load");
        let left_row = left_diff
            .entries
            .iter()
            .find_map(|entry| {
                entry
                    .after
                    .as_ref()
                    .filter(|row| row.change_id == "left-b")
                    .cloned()
            })
            .expect("left row should appear");
        let (left_key, left_value) = left_row.into_index_entry();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let right_diff = tracked_state
            .reader(read)
            .diff_commits("parent", "right", &TrackedStateDiffRequest::default())
            .await
            .expect("right diff should load");
        let right_row = right_diff
            .entries
            .iter()
            .find_map(|entry| {
                entry
                    .after
                    .as_ref()
                    .filter(|row| row.change_id == "right-c")
                    .cloned()
            })
            .expect("right row should appear");
        let (right_key, right_value) = right_row.into_index_entry();
        stage_corrupt_commit_root(
            &storage,
            "left",
            vec![(left_key, left_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: "parent".to_string(),
                root_id: tracked_state_root_id(&storage, "parent").await,
            }],
        )
        .await;
        stage_corrupt_commit_root(
            &storage,
            "right",
            vec![(right_key, right_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: "parent".to_string(),
                root_id: tracked_state_root_id(&storage, "parent").await,
            }],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "right", &TrackedStateDiffRequest::default())
            .await
            .expect_err("shared hidden omission must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_validates_roots_even_when_tree_diff_is_empty() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "source",
            None,
            &[row_with_value("entity-a", None, "source-change", "value")],
        )
        .await
        .expect("source root should write");
        write_root_committed_for_test(&storage, &tracked_state, "left-corrupt", None, &[])
            .await
            .expect("left changelog should write");
        write_root_committed_for_test(&storage, &tracked_state, "right-corrupt", None, &[])
            .await
            .expect("right changelog should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let source_diff = tracked_state
            .reader(read)
            .diff_commits(
                "left-corrupt",
                "source",
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("source diff should load");
        let source_row = source_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("source row should appear");
        let (source_key, source_value) = source_row.into_index_entry();

        stage_corrupt_commit_root(
            &storage,
            "left-corrupt",
            vec![(source_key.clone(), source_value.clone())],
            Vec::new(),
        )
        .await;
        stage_corrupt_commit_root(
            &storage,
            "right-corrupt",
            vec![(source_key, source_value)],
            Vec::new(),
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits(
                "left-corrupt",
                "right-corrupt",
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect_err("identical corrupt roots must still be validated");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
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

    async fn tracked_state_root_id(
        storage: &StorageContext,
        commit_id: &str,
    ) -> TrackedStateRootId {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        crate::tracked_state::storage::load_root(&read, commit_id)
            .await
            .expect("root should load")
            .expect("root should exist")
    }

    async fn stage_corrupt_commit_root(
        storage: &StorageContext,
        commit_id: &str,
        entries: Vec<(TrackedStateKey, TrackedStateIndexValue)>,
        parent_roots: Vec<TrackedStateCommitRootParent>,
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mutations = entries
            .into_iter()
            .map(|(key, value)| {
                TrackedStateMutation::put_encoded(
                    crate::tracked_state::codec::encode_key(&key),
                    crate::tracked_state::codec::encode_value(&value),
                )
            })
            .collect::<Vec<_>>();
        let changed_key_count = mutations.len() as u64;
        let result = crate::tracked_state::tree::TrackedStateTree::new()
            .apply_mutations(&read, &mut writes, None, mutations, Some(commit_id))
            .await
            .expect("corrupt root should write");
        crate::tracked_state::storage::stage_commit_root(
            &mut writes,
            &TrackedStateCommitRoot {
                commit_id: commit_id.to_string(),
                root_id: result.root_id,
                parent_roots,
                changed_key_count,
                row_count_estimate: result.row_count as u64,
                tree_height: result.tree_height as u32,
                primary_chunk_count: result.chunk_count as u64,
                primary_chunk_bytes: result.chunk_bytes as u64,
            },
        )
        .expect("metadata should encode");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("corrupt root should commit");
    }

    fn kinds(diff: &TrackedStateDiff) -> Vec<(String, TrackedStateDiffKind)> {
        diff.entries
            .iter()
            .map(|entry| {
                (
                    entry
                        .identity
                        .entity_pk
                        .as_single_string_owned()
                        .expect("identity"),
                    entry.kind,
                )
            })
            .collect()
    }

    fn is_commit_root_validation_error(error: &LixError) -> bool {
        error.message.contains("not the first-parent winner")
            || error.message.contains("does not match parent root")
            || error
                .message
                .contains("does not match changelog first-parent winners")
            || error.message.contains("contains non-winner identity")
            || error.message.contains("but changelog first parent is")
            || error
                .message
                .contains("nearest available first-parent root")
            || error.message.contains("references unexpected parent")
            || error.message.contains("missing changelog winner")
            || error.message.contains("has change")
            || error.message.contains("omits current changelog change")
            || error.message.contains("omits inherited identity")
            || error
                .message
                .contains("does not preserve inherited identity")
            || error.message.contains("but changelog winner is")
    }

    fn commit_root_row_entry(
        commit_id: &str,
        change_id: &str,
        created_at: &str,
    ) -> (TrackedStateKey, TrackedStateIndexValue) {
        (
            TrackedStateKey {
                schema_key: "lix_commit".to_string(),
                file_id: None,
                entity_pk: EntityPk::single(commit_id),
            },
            TrackedStateIndexValue {
                change_id: change_id.to_string(),
                commit_id: commit_id.to_string(),
                deleted: false,
                snapshot_ref: Some(JsonRef::for_content(
                    format!("{{\"id\":\"{commit_id}\"}}").as_bytes(),
                )),
                metadata_ref: None,
                created_at: created_at.to_string(),
                updated_at: created_at.to_string(),
            },
        )
    }

    fn tombstone(
        entity_pk: &str,
        file_id: Option<&str>,
        change_id: &str,
    ) -> MaterializedTrackedStateRow {
        let mut row = row(entity_pk, file_id, change_id);
        row.snapshot_content = None;
        row.deleted = true;
        row
    }

    fn row(entity_pk: &str, file_id: Option<&str>, change_id: &str) -> MaterializedTrackedStateRow {
        row_with_schema(entity_pk, file_id, "test_schema", change_id)
    }

    fn row_with_schema(
        entity_pk: &str,
        file_id: Option<&str>,
        schema_key: &str,
        change_id: &str,
    ) -> MaterializedTrackedStateRow {
        row_with_schema_and_value(entity_pk, file_id, schema_key, change_id, "value")
    }

    fn row_with_value(
        entity_pk: &str,
        file_id: Option<&str>,
        change_id: &str,
        value: &str,
    ) -> MaterializedTrackedStateRow {
        row_with_schema_and_value(entity_pk, file_id, "test_schema", change_id, value)
    }

    fn row_with_times(
        entity_pk: &str,
        file_id: Option<&str>,
        change_id: &str,
        value: &str,
        created_at: &str,
        updated_at: &str,
    ) -> MaterializedTrackedStateRow {
        let mut row = row_with_value(entity_pk, file_id, change_id, value);
        row.created_at = created_at.to_string();
        row.updated_at = updated_at.to_string();
        row
    }

    fn row_with_schema_and_value(
        entity_pk: &str,
        file_id: Option<&str>,
        schema_key: &str,
        change_id: &str,
        value: &str,
    ) -> MaterializedTrackedStateRow {
        MaterializedTrackedStateRow {
            entity_pk: EntityPk::single(entity_pk),
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
