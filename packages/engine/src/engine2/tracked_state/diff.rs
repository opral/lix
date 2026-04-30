use std::collections::{BTreeMap, BTreeSet};

use crate::engine2::entity_identity::EntityIdentity;
use crate::engine2::tracked_state::{
    TrackedStateFilter, TrackedStateRow, TrackedStateScanRequest, TrackedStateStoreReader,
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
    pub(crate) before: Option<TrackedStateRow>,
    /// Raw row in the right root.
    ///
    /// This can be a tombstone. Keeping the raw tombstone is what lets merge
    /// apply deletes without reloading the source root.
    pub(crate) after: Option<TrackedStateRow>,
}

/// Root-local tracked-state identity.
///
/// `plugin_key` is intentionally excluded. It is payload metadata for an
/// entity, not part of the entity identity used by merge/diff logic.
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
/// This first implementation scans both roots with tombstones included and
/// merge-joins the keyed rows. It deliberately mirrors the shape of prolly-tree
/// diffs (`before`, `after`, `Added | Modified | Removed`) so we can later
/// replace the internals with a chunk-skipping cursor diff without changing
/// merge code.
pub(crate) async fn diff_commits<S>(
    reader: &mut TrackedStateStoreReader<S>,
    left_commit_id: &str,
    right_commit_id: &str,
    request: &TrackedStateDiffRequest,
) -> Result<TrackedStateDiff, LixError>
where
    S: crate::backend::KvStore,
{
    let scan_request = scan_request_for_diff(request);
    let left_rows = keyed_rows(
        reader
            .scan_rows_at_commit(left_commit_id, &scan_request)
            .await?,
    )?;
    let right_rows = keyed_rows(
        reader
            .scan_rows_at_commit(right_commit_id, &scan_request)
            .await?,
    )?;
    let identities = left_rows
        .keys()
        .chain(right_rows.keys())
        .cloned()
        .collect::<BTreeSet<_>>();

    let mut entries = Vec::new();
    for identity in identities {
        let before = left_rows.get(&identity);
        let after = right_rows.get(&identity);
        let Some(entry) = classify_diff(identity, before, after) else {
            continue;
        };
        entries.push(entry);
    }

    Ok(TrackedStateDiff { entries })
}

fn scan_request_for_diff(request: &TrackedStateDiffRequest) -> TrackedStateScanRequest {
    let mut filter = request.filter.clone();
    filter.include_tombstones = true;
    TrackedStateScanRequest {
        filter,
        projection: Default::default(),
        limit: None,
    }
}

fn keyed_rows(
    rows: Vec<TrackedStateRow>,
) -> Result<BTreeMap<TrackedStateDiffIdentity, TrackedStateRow>, LixError> {
    let mut keyed = BTreeMap::new();
    for row in rows {
        keyed.insert(TrackedStateDiffIdentity::from_row(&row)?, row);
    }
    Ok(keyed)
}

fn classify_diff(
    identity: TrackedStateDiffIdentity,
    before: Option<&TrackedStateRow>,
    after: Option<&TrackedStateRow>,
) -> Option<TrackedStateDiffEntry> {
    match (is_live_row(before), is_live_row(after)) {
        (None, None) => None,
        (None, Some(_)) => Some(TrackedStateDiffEntry {
            identity,
            kind: TrackedStateDiffKind::Added,
            before: before.cloned(),
            after: after.cloned(),
        }),
        (Some(_), None) => Some(TrackedStateDiffEntry {
            identity,
            kind: TrackedStateDiffKind::Removed,
            before: before.cloned(),
            after: after.cloned(),
        }),
        (Some(before), Some(after)) if tracked_row_payload_eq(before, after) => None,
        (Some(before), Some(after)) => Some(TrackedStateDiffEntry {
            identity,
            kind: TrackedStateDiffKind::Modified,
            before: Some(before.clone()),
            after: Some(after.clone()),
        }),
    }
}

fn is_live_row(row: Option<&TrackedStateRow>) -> Option<&TrackedStateRow> {
    row.filter(|row| row.snapshot_content.is_some())
}

fn tracked_row_payload_eq(left: &TrackedStateRow, right: &TrackedStateRow) -> bool {
    left.plugin_key == right.plugin_key
        && left.snapshot_content == right.snapshot_content
        && left.metadata == right.metadata
        && left.schema_version == right.schema_version
}

impl TrackedStateDiffIdentity {
    fn from_row(row: &TrackedStateRow) -> Result<Self, LixError> {
        Ok(Self {
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        })
    }
}

impl TrackedStateDiffEntry {
    pub(crate) fn before_is_live(&self) -> bool {
        self.visible_before().is_some()
    }

    pub(crate) fn after_is_live(&self) -> bool {
        self.visible_after().is_some()
    }

    pub(crate) fn visible_before(&self) -> Option<&TrackedStateRow> {
        self.before
            .as_ref()
            .filter(|row| row.snapshot_content.is_some())
    }

    pub(crate) fn visible_after(&self) -> Option<&TrackedStateRow> {
        self.after
            .as_ref()
            .filter(|row| row.snapshot_content.is_some())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
    use crate::engine2::tracked_state::TrackedStateContext;
    use crate::NullableKeyFilter;

    #[tokio::test]
    async fn diff_commits_reports_added_rows() {
        let (backend, tracked_state) = seed_roots(&[], &[row("entity-a", None, "after")]).await;

        let diff = diff(&backend, &tracked_state).await;

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
        let (backend, tracked_state) = seed_roots(&[row("entity-a", None, "before")], &[]).await;

        let diff = diff(&backend, &tracked_state).await;

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
        let (backend, tracked_state) = seed_roots(
            &[row("entity-a", None, "before")],
            &[tombstone("entity-a", None, "delete")],
        )
        .await;

        let diff = diff(&backend, &tracked_state).await;

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
        let (backend, tracked_state) = seed_roots(
            &[tombstone("entity-a", None, "delete")],
            &[row("entity-a", None, "after")],
        )
        .await;

        let diff = diff(&backend, &tracked_state).await;

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
        let (backend, tracked_state) = seed_roots(
            &[row_with_value("entity-a", None, "before", "one")],
            &[row_with_value("entity-a", None, "after", "two")],
        )
        .await;

        let diff = diff(&backend, &tracked_state).await;

        assert_eq!(
            kinds(&diff),
            vec![("entity-a".to_string(), TrackedStateDiffKind::Modified)]
        );
        assert!(diff.entries[0].before_is_live());
        assert!(diff.entries[0].after_is_live());
    }

    #[tokio::test]
    async fn diff_commits_omits_unchanged_rows_even_when_metadata_differs_only_by_commit() {
        let (backend, tracked_state) = seed_roots(
            &[row_with_value("entity-a", None, "before", "same")],
            &[row_with_value("entity-a", None, "after", "same")],
        )
        .await;

        let diff = diff(&backend, &tracked_state).await;

        assert!(diff.entries.is_empty());
    }

    #[tokio::test]
    async fn diff_commits_distinguishes_same_entity_with_different_file_id() {
        let (backend, tracked_state) = seed_roots(
            &[row("entity-a", Some("file-a"), "before-a")],
            &[
                row("entity-a", Some("file-a"), "before-a"),
                row("entity-a", Some("file-b"), "after-b"),
            ],
        )
        .await;

        let diff = diff(&backend, &tracked_state).await;

        assert_eq!(diff.entries.len(), 1);
        assert_eq!(diff.entries[0].identity.file_id.as_deref(), Some("file-b"));
        assert_eq!(diff.entries[0].kind, TrackedStateDiffKind::Added);
    }

    #[tokio::test]
    async fn diff_commits_filters_by_schema_entity_and_file_id() {
        let (backend, tracked_state) = seed_roots(
            &[],
            &[
                row_with_schema("entity-a", Some("file-a"), "schema-a", "change-a"),
                row_with_schema("entity-b", Some("file-b"), "schema-b", "change-b"),
            ],
        )
        .await;
        let mut reader = tracked_state.reader(Arc::clone(&backend));
        let diff = reader
            .diff_commits(
                "left",
                "right",
                &TrackedStateDiffRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema-b".to_string()],
                        entity_ids: vec![crate::engine2::entity_identity::EntityIdentity::single(
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

    async fn diff(
        backend: &Arc<UnitTestBackend>,
        tracked_state: &TrackedStateContext,
    ) -> TrackedStateDiff {
        tracked_state
            .reader(Arc::clone(backend))
            .diff_commits("left", "right", &TrackedStateDiffRequest::default())
            .await
            .expect("diff should load")
    }

    async fn seed_roots(
        left_rows: &[TrackedStateRow],
        right_rows: &[TrackedStateRow],
    ) -> (Arc<UnitTestBackend>, TrackedStateContext) {
        let backend = Arc::new(UnitTestBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        tracked_state
            .writer(tx.as_mut())
            .write_root("left", None, left_rows)
            .await
            .expect("left root should write");
        tracked_state
            .writer(tx.as_mut())
            .write_root("right", None, right_rows)
            .await
            .expect("right root should write");
        tx.commit().await.expect("transaction should commit");
        (backend, tracked_state)
    }

    fn kinds(diff: &TrackedStateDiff) -> Vec<(String, TrackedStateDiffKind)> {
        diff.entries
            .iter()
            .map(|entry| {
                (
                    entry.identity.entity_id.as_string().expect("identity"),
                    entry.kind,
                )
            })
            .collect()
    }

    fn tombstone(entity_id: &str, file_id: Option<&str>, change_id: &str) -> TrackedStateRow {
        let mut row = row(entity_id, file_id, change_id);
        row.snapshot_content = None;
        row
    }

    fn row(entity_id: &str, file_id: Option<&str>, change_id: &str) -> TrackedStateRow {
        row_with_schema(entity_id, file_id, "test_schema", change_id)
    }

    fn row_with_schema(
        entity_id: &str,
        file_id: Option<&str>,
        schema_key: &str,
        change_id: &str,
    ) -> TrackedStateRow {
        row_with_schema_and_value(entity_id, file_id, schema_key, change_id, "value")
    }

    fn row_with_value(
        entity_id: &str,
        file_id: Option<&str>,
        change_id: &str,
        value: &str,
    ) -> TrackedStateRow {
        row_with_schema_and_value(entity_id, file_id, "test_schema", change_id, value)
    }

    fn row_with_schema_and_value(
        entity_id: &str,
        file_id: Option<&str>,
        schema_key: &str,
        change_id: &str,
        value: &str,
    ) -> TrackedStateRow {
        TrackedStateRow {
            entity_id: EntityIdentity::single(entity_id),
            schema_key: schema_key.to_string(),
            file_id: file_id.map(str::to_string),
            plugin_key: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            change_id: change_id.to_string(),
            commit_id: change_id.replace("change", "commit"),
        }
    }
}
