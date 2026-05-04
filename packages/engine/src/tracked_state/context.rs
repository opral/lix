use crate::commit_graph::CommitGraphContext;
use crate::storage::{StorageReader, StorageWriter};
use crate::tracked_state::by_file_index::ByFileIndex;
use crate::tracked_state::diff::{diff_commits, TrackedStateDiff, TrackedStateDiffRequest};
use crate::tracked_state::merge::{self, TrackedStateMergePlan};
use crate::tracked_state::rebuild::TrackedStateRebuildReport;
use crate::tracked_state::snapshot_store::SnapshotStore;
use crate::tracked_state::storage;
use crate::tracked_state::tree::TrackedStateTree;
use crate::tracked_state::tree_types::{
    TrackedStateKey, TrackedStateMutation, TrackedStateTreeScanRequest, TrackedStateValue,
};
use crate::tracked_state::{TrackedStateRow, TrackedStateRowRequest, TrackedStateScanRequest};
use crate::LixError;

/// Factory for rebuildable tracked-state readers and writers.
///
/// Tracked state is stored as content-addressed roots. Version refs
/// choose which commit/root to read; this context only owns root operations.
#[derive(Clone)]
pub(crate) struct TrackedStateContext {
    tree: TrackedStateTree,
    snapshot_store: SnapshotStore,
}

impl TrackedStateContext {
    pub(crate) fn new() -> Self {
        Self {
            tree: TrackedStateTree::new(),
            snapshot_store: SnapshotStore::new(),
        }
    }

    #[cfg(feature = "storage-benches")]
    pub(crate) fn with_max_inline_encoded_value_bytes_for_bench(
        max_inline_encoded_value_bytes: usize,
    ) -> Self {
        Self {
            tree: TrackedStateTree::new(),
            snapshot_store: SnapshotStore::with_max_inline_encoded_value_bytes(
                max_inline_encoded_value_bytes,
            ),
        }
    }

    /// Creates a commit-id-addressed tracked-state reader.
    pub(crate) fn reader<S>(&self, store: S) -> TrackedStateStoreReader<S>
    where
        S: StorageReader,
    {
        TrackedStateStoreReader {
            store,
            tree: self.tree.clone(),
        }
    }

    /// Creates a tracked-state writer over a caller-provided KV writer.
    pub(crate) fn writer<S>(&self, store: S) -> TrackedStateWriter<S>
    where
        S: StorageWriter,
    {
        TrackedStateWriter {
            store,
            tree: self.tree.clone(),
            snapshot_store: self.snapshot_store,
        }
    }

    /// Rebuilds tracked state at one commit from commit-graph entities.
    pub(crate) async fn rebuild_state_at_commit<R, W>(
        &self,
        commit_graph: &CommitGraphContext,
        read_store: R,
        write_store: W,
        head_commit_id: &str,
    ) -> Result<TrackedStateRebuildReport, LixError>
    where
        R: StorageReader,
        W: StorageWriter,
    {
        crate::tracked_state::rebuild::rebuild_state_at_commit(
            self,
            commit_graph,
            read_store,
            write_store,
            head_commit_id,
        )
        .await
    }
}

/// Store-backed tracked-state reader created by `TrackedStateContext`.
pub(crate) struct TrackedStateStoreReader<S> {
    store: S,
    tree: TrackedStateTree,
}

impl<S> TrackedStateStoreReader<S>
where
    S: StorageReader,
{
    pub(crate) async fn scan_rows_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<TrackedStateRow>, LixError> {
        let Some(root_id) = self.tree.load_root(&mut self.store, commit_id).await? else {
            return Ok(Vec::new());
        };
        let rows = if ByFileIndex::should_use(request) {
            let Some(by_file_root_id) =
                storage::load_by_file_root(&mut self.store, commit_id).await?
            else {
                return Ok(Vec::new());
            };
            self.scan_rows_at_commit_by_file_index(&root_id, &by_file_root_id, request)
                .await?
        } else {
            let rows = self
                .tree
                .scan(
                    &mut self.store,
                    &root_id,
                    &tree_scan_request_from_tracked(request),
                )
                .await?;
            SnapshotStore::resolve_rows(&mut self.store, rows, scan_needs_snapshot_content(request))
                .await?
        };
        let needs_snapshot_content = scan_needs_snapshot_content(request);
        Ok(rows
            .into_iter()
            .map(|(key, mut value)| {
                if !needs_snapshot_content {
                    value = value.without_snapshot_content();
                }
                value.into_row(key)
            })
            .collect())
    }

    pub(crate) async fn load_row_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateRowRequest,
    ) -> Result<Option<TrackedStateRow>, LixError> {
        let key = tracked_key_from_request(request)?;
        let Some(root_id) = self.tree.load_root(&mut self.store, commit_id).await? else {
            return Ok(None);
        };
        let row = self
            .tree
            .get(&mut self.store, &root_id, &key)
            .await?
            .map(|value| async {
                let value = SnapshotStore::resolve_value(&mut self.store, value).await?;
                Ok::<_, LixError>(value.into_row(key))
            });
        match row {
            Some(row) => row.await.map(Some),
            None => Ok(None),
        }
    }

    pub(crate) async fn diff_commits(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStateDiffRequest,
    ) -> Result<TrackedStateDiff, LixError> {
        diff_commits(self, left_commit_id, right_commit_id, request).await
    }

    pub(crate) async fn diff_tree_entries_at_commits(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<Vec<crate::tracked_state::tree_types::TrackedStateTreeDiffEntry>, LixError> {
        let left_root = self.tree.load_root(&mut self.store, left_commit_id).await?;
        let right_root = self
            .tree
            .load_root(&mut self.store, right_commit_id)
            .await?;
        let entries = self
            .tree
            .diff(
                &mut self.store,
                left_root.as_ref(),
                right_root.as_ref(),
                request,
            )
            .await?;
        let mut resolved = Vec::with_capacity(entries.len());
        for entry in entries {
            resolved.push(
                crate::tracked_state::tree_types::TrackedStateTreeDiffEntry {
                    before: match entry.before {
                        Some((key, value)) => Some((
                            key,
                            SnapshotStore::resolve_value(&mut self.store, value).await?,
                        )),
                        None => None,
                    },
                    after: match entry.after {
                        Some((key, value)) => Some((
                            key,
                            SnapshotStore::resolve_value(&mut self.store, value).await?,
                        )),
                        None => None,
                    },
                },
            );
        }
        Ok(resolved)
    }

    async fn scan_rows_at_commit_by_file_index(
        &mut self,
        primary_root_id: &crate::tracked_state::tree_types::TrackedStateRootId,
        by_file_root_id: &crate::tracked_state::tree_types::TrackedStateRootId,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<(TrackedStateKey, TrackedStateValue)>, LixError> {
        let by_file_request = ByFileIndex::scan_request_from_tracked(request);
        let index_match_count = self
            .tree
            .count_matching_keys(&mut self.store, by_file_root_id, &by_file_request)
            .await?;
        let primary_row_count = self
            .tree
            .row_count(&mut self.store, primary_root_id)
            .await?;
        if index_match_count * 20 > primary_row_count {
            let rows = self
                .tree
                .scan(
                    &mut self.store,
                    primary_root_id,
                    &tree_scan_request_from_tracked(request),
                )
                .await?;
            return SnapshotStore::resolve_rows(
                &mut self.store,
                rows,
                scan_needs_snapshot_content(request),
            )
            .await;
        }
        let index_rows = self
            .tree
            .scan(&mut self.store, by_file_root_id, &by_file_request)
            .await?;
        let mut rows = Vec::new();
        let tree_request = tree_scan_request_from_tracked(request);
        let needs_snapshot_content = scan_needs_snapshot_content(request);
        if needs_snapshot_content {
            let mut primary_keys = Vec::with_capacity(index_rows.len());
            for (index_key, _) in index_rows {
                if let Some(primary_key) = ByFileIndex::primary_key_from_index_key(index_key) {
                    primary_keys.push(primary_key);
                }
            }
            let primary_values = self
                .tree
                .get_many(&mut self.store, primary_root_id, &primary_keys)
                .await?;
            for (primary_key, value) in primary_keys.into_iter().zip(primary_values) {
                if request.limit.is_some_and(|limit| rows.len() >= limit) {
                    break;
                }
                let Some(value) = value else {
                    continue;
                };
                if !tree_request.matches(&primary_key, &value) {
                    continue;
                }
                rows.push((
                    primary_key,
                    SnapshotStore::resolve_value(&mut self.store, value).await?,
                ));
            }
            return Ok(rows);
        }

        for (index_key, index_value) in index_rows {
            if request.limit.is_some_and(|limit| rows.len() >= limit) {
                break;
            }
            let Some(primary_key) = ByFileIndex::primary_key_from_index_key(index_key) else {
                continue;
            };
            let value = index_value;
            if tree_request.matches(&primary_key, &value) {
                rows.push((primary_key, value));
            }
        }
        Ok(rows)
    }

    /// Plans a three-way merge by diffing both heads against the same base.
    ///
    /// `target_commit_id` is the destination root that should keep its own
    /// changes. `source_commit_id` is the incoming root whose non-conflicting
    /// changes should be applied.
    pub(crate) async fn plan_merge(
        &mut self,
        base_commit_id: &str,
        target_commit_id: &str,
        source_commit_id: &str,
        request: &TrackedStateDiffRequest,
    ) -> Result<TrackedStateMergePlan, LixError> {
        let target_diff = self
            .diff_commits(base_commit_id, target_commit_id, request)
            .await?;
        let source_diff = self
            .diff_commits(base_commit_id, source_commit_id, request)
            .await?;
        merge::plan_merge(&target_diff, &source_diff)
    }

    #[cfg(test)]
    pub(crate) async fn load_root_for_test(
        &mut self,
        commit_id: &str,
    ) -> Result<Option<crate::tracked_state::tree_types::TrackedStateRootId>, LixError> {
        self.tree.load_root(&mut self.store, commit_id).await
    }
}

/// Writer for rebuildable tracked-state roots.
pub(crate) struct TrackedStateWriter<S> {
    store: S,
    tree: TrackedStateTree,
    snapshot_store: SnapshotStore,
}

impl<S> TrackedStateWriter<S>
where
    S: StorageWriter,
{
    /// Writes one root for `commit_id` from the provided row set.
    ///
    /// `parent_commit_id` is the tracked-state root to layer mutations on top
    /// of. Rebuild passes `None` because it has already materialized the full
    /// entity set for the requested head.
    pub(crate) async fn write_root(
        &mut self,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        rows: &[TrackedStateRow],
    ) -> Result<TrackedStateWriteReceipt, LixError> {
        let base_root = match parent_commit_id {
            Some(parent_commit_id) => {
                let Some(root) = self
                    .tree
                    .load_root(&mut self.store, parent_commit_id)
                    .await?
                else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "tracked-state parent root for commit '{parent_commit_id}' is missing"
                        ),
                    ));
                };
                Some(root)
            }
            None => None,
        };
        let mut stored_rows = Vec::with_capacity(rows.len());
        let mut mutations = Vec::with_capacity(rows.len());
        for row in rows {
            let stored_value = self
                .snapshot_store
                .store_value(&mut self.store, TrackedStateValue::from_row(row))
                .await?;
            mutations.push(TrackedStateMutation::put(
                TrackedStateKey::from_row(row),
                stored_value.clone(),
            ));
            stored_rows.push((row, stored_value));
        }
        let result = self
            .tree
            .apply_mutations(
                &mut self.store,
                base_root.as_ref(),
                mutations,
                Some(commit_id),
            )
            .await?;

        let by_file_base_root = match parent_commit_id {
            Some(parent_commit_id) => storage::load_by_file_root(&mut self.store, parent_commit_id)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "tracked-state by-file parent root for commit '{parent_commit_id}' is missing"
                        ),
                    )
                })
                .map(Some)?,
            None => None,
        };
        let mut by_file_mutations = Vec::with_capacity(rows.len());
        for (row, stored_value) in &stored_rows {
            by_file_mutations.push(TrackedStateMutation::put(
                ByFileIndex::key_from_row(row),
                ByFileIndex::header_value_from_primary(stored_value),
            ));
        }
        let by_file_result = self
            .tree
            .apply_mutations(
                &mut self.store,
                by_file_base_root.as_ref(),
                by_file_mutations,
                None,
            )
            .await?;
        storage::store_by_file_root(&mut self.store, commit_id, &by_file_result.root_id).await?;
        Ok(TrackedStateWriteReceipt {
            commit_id: commit_id.to_string(),
            row_count: result.row_count,
        })
    }

    /// Deletes the root pointer for one commit.
    ///
    /// This is intentionally root-scoped, not row-scoped. It is useful for
    /// rebuild/corruption tests where the changelog remains authoritative and
    /// the tracked-state projection must be recreated from the commit id.
    #[cfg(test)]
    pub(crate) async fn delete_root_for_rebuild(
        &mut self,
        commit_id: &str,
    ) -> Result<(), LixError> {
        storage::delete_root(&mut self.store, commit_id).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateWriteReceipt {
    pub(crate) commit_id: String,
    pub(crate) row_count: usize,
}

fn tree_scan_request_from_tracked(
    request: &TrackedStateScanRequest,
) -> TrackedStateTreeScanRequest {
    TrackedStateTreeScanRequest {
        schema_keys: request.filter.schema_keys.clone(),
        entity_ids: request.filter.entity_ids.clone(),
        file_ids: request.filter.file_ids.clone(),
        include_tombstones: request.filter.include_tombstones,
        limit: request.limit,
    }
}

fn scan_needs_snapshot_content(request: &TrackedStateScanRequest) -> bool {
    request.projection.columns.is_empty()
        || request
            .projection
            .columns
            .iter()
            .any(|column| column == "snapshot_content")
}

fn tracked_key_from_request(request: &TrackedStateRowRequest) -> Result<TrackedStateKey, LixError> {
    let file_id = match &request.file_id {
        crate::NullableKeyFilter::Null => None,
        crate::NullableKeyFilter::Value(value) => Some(value.clone()),
        crate::NullableKeyFilter::Any => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state tree exact lookup requires a concrete file_id filter",
            ))
        }
    };
    Ok(TrackedStateKey {
        schema_key: request.schema_key.clone(),
        file_id,
        entity_id: request.entity_id.clone(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, Backend};
    use crate::storage::StorageContext;
    use crate::tracked_state::snapshot_store::DEFAULT_MAX_INLINE_ENCODED_VALUE_BYTES;
    use crate::NullableKeyFilter;

    #[tokio::test]
    async fn write_root_rejects_missing_parent_root() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let tracked_state = TrackedStateContext::new();
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        let error = tracked_state
            .writer(transaction.as_mut())
            .write_root(
                "commit-child",
                Some("missing-parent"),
                &[row("entity-child", "change-child", "commit-child")],
            )
            .await
            .expect_err("parent root must exist when parent_commit_id is provided");

        assert!(
            error.message.contains("parent root") && error.message.contains("missing-parent"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn plan_merge_from_roots_applies_source_only_change() {
        let (storage, tracked_state) = seed_merge_roots(
            &[row_with_value("entity-a", "change-base", "base", "base")],
            &[row_with_value("entity-a", "change-base", "base", "base")],
            &[row_with_value(
                "entity-a",
                "change-source",
                "source",
                "source",
            )],
        )
        .await;

        let plan = tracked_state
            .reader(storage.clone())
            .plan_merge(
                "base",
                "target",
                "source",
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("merge should plan");

        assert_eq!(merge_patch_ids(&plan), vec!["entity-a"]);
        assert!(plan.conflicts.is_empty());
    }

    #[tokio::test]
    async fn plan_merge_from_roots_keeps_target_only_change() {
        let (storage, tracked_state) = seed_merge_roots(
            &[row("entity-a", "change-base", "base")],
            &[row("entity-a", "change-target", "target")],
            &[row("entity-a", "change-base", "base")],
        )
        .await;

        let plan = tracked_state
            .reader(storage.clone())
            .plan_merge(
                "base",
                "target",
                "source",
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("merge should plan");

        assert!(plan.patches.is_empty());
        assert!(plan.conflicts.is_empty());
    }

    #[tokio::test]
    async fn plan_merge_from_roots_reports_divergent_modification_conflict() {
        let (storage, tracked_state) = seed_merge_roots(
            &[row_with_value("entity-a", "change-base", "base", "base")],
            &[row_with_value(
                "entity-a",
                "change-target",
                "target",
                "target",
            )],
            &[row_with_value(
                "entity-a",
                "change-source",
                "source",
                "source",
            )],
        )
        .await;

        let plan = tracked_state
            .reader(storage.clone())
            .plan_merge(
                "base",
                "target",
                "source",
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("merge should plan");

        assert!(plan.patches.is_empty());
        assert_eq!(merge_conflict_ids(&plan), vec!["entity-a"]);
    }

    #[tokio::test]
    async fn plan_merge_from_roots_applies_source_tombstone() {
        let (storage, tracked_state) = seed_merge_roots(
            &[row("entity-a", "change-base", "base")],
            &[row("entity-a", "change-base", "base")],
            &[tombstone("entity-a", "change-source-delete", "source")],
        )
        .await;

        let plan = tracked_state
            .reader(storage.clone())
            .plan_merge(
                "base",
                "target",
                "source",
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("merge should plan");

        assert_eq!(merge_patch_ids(&plan), vec!["entity-a"]);
        assert_eq!(plan.patches[0].projected_row().snapshot_content, None);
        assert_eq!(plan.patches[0].change_id(), "change-source-delete");
    }

    #[tokio::test]
    async fn scan_rows_by_file_uses_file_index_shape() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let mut file_a = row("entity-a", "change-a", "commit-1");
        file_a.file_id = Some("file-a.json".to_string());
        let mut file_b = row("entity-b", "change-b", "commit-1");
        file_b.file_id = Some("file-b.json".to_string());

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        tracked_state
            .writer(transaction.as_mut())
            .write_root("commit-1", None, &[file_a, file_b])
            .await
            .expect("root should write");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let rows = tracked_state
            .reader(storage.clone())
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        file_ids: vec![NullableKeyFilter::Value("file-a.json".to_string())],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("file scan should read through index");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].entity_id.as_string().expect("entity id"),
            "entity-a"
        );
        assert_eq!(rows[0].file_id.as_deref(), Some("file-a.json"));
    }

    #[tokio::test]
    async fn by_file_header_index_fetches_primary_payload_only_when_requested() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let mut row = row("entity-a", "change-a", "commit-1");
        row.file_id = Some("file-a.json".to_string());
        let expected_snapshot = row.snapshot_content.clone();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        tracked_state
            .writer(transaction.as_mut())
            .write_root("commit-1", None, std::slice::from_ref(&row))
            .await
            .expect("root should write");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut reader = tracked_state.reader(storage.clone());
        let header_rows = reader
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        file_ids: vec![NullableKeyFilter::Value("file-a.json".to_string())],
                        ..Default::default()
                    },
                    projection: crate::tracked_state::TrackedStateProjection {
                        columns: vec!["entity_id".to_string()],
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("header scan should read through by-file index");
        let full_rows = reader
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        file_ids: vec![NullableKeyFilter::Value("file-a.json".to_string())],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("full scan should fetch primary payload");

        assert_eq!(header_rows[0].snapshot_content, None);
        assert_eq!(full_rows[0].snapshot_content, expected_snapshot);
    }

    #[tokio::test]
    async fn by_file_header_index_filters_tombstones_without_payload_sentinel() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let mut live = row("entity-live", "change-live", "commit-1");
        live.file_id = Some("file-a.json".to_string());
        let mut deleted = tombstone("entity-deleted", "change-delete", "commit-1");
        deleted.file_id = Some("file-a.json".to_string());

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        tracked_state
            .writer(transaction.as_mut())
            .write_root("commit-1", None, &[live, deleted])
            .await
            .expect("root should write");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let rows = tracked_state
            .reader(storage.clone())
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        file_ids: vec![NullableKeyFilter::Value("file-a.json".to_string())],
                        ..Default::default()
                    },
                    projection: crate::tracked_state::TrackedStateProjection {
                        columns: vec!["entity_id".to_string()],
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("file scan should read through index");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].entity_id.as_string().expect("entity id"),
            "entity-live"
        );
    }

    #[tokio::test]
    async fn reads_resolve_large_snapshot_refs() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let large_value = "x".repeat(DEFAULT_MAX_INLINE_ENCODED_VALUE_BYTES + 512);
        let row = row_with_value("entity-a", "change-a", "commit-1", &large_value);

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        tracked_state
            .writer(transaction.as_mut())
            .write_root("commit-1", None, std::slice::from_ref(&row))
            .await
            .expect("root should write");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut reader = tracked_state.reader(storage.clone());
        let loaded = reader
            .load_row_at_commit(
                "commit-1",
                &TrackedStateRowRequest {
                    schema_key: row.schema_key.clone(),
                    entity_id: row.entity_id.clone(),
                    file_id: NullableKeyFilter::Null,
                },
            )
            .await
            .expect("row should load")
            .expect("row should exist");
        let scanned = reader
            .scan_rows_at_commit("commit-1", &TrackedStateScanRequest::default())
            .await
            .expect("rows should scan");

        assert_eq!(loaded.snapshot_content, row.snapshot_content);
        assert_eq!(scanned[0].snapshot_content, row.snapshot_content);
    }

    #[tokio::test]
    async fn projected_scans_do_not_return_snapshot_refs_when_snapshot_content_is_omitted() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let large_value = "x".repeat(DEFAULT_MAX_INLINE_ENCODED_VALUE_BYTES + 512);
        let row = row_with_value("entity-a", "change-a", "commit-1", &large_value);

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        tracked_state
            .writer(transaction.as_mut())
            .write_root("commit-1", None, std::slice::from_ref(&row))
            .await
            .expect("root should write");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let rows = tracked_state
            .reader(storage.clone())
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    projection: crate::tracked_state::TrackedStateProjection {
                        columns: vec!["entity_id".to_string()],
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("rows should scan");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].snapshot_content, None);
    }

    async fn seed_merge_roots(
        base_rows: &[TrackedStateRow],
        target_rows: &[TrackedStateRow],
        source_rows: &[TrackedStateRow],
    ) -> (StorageContext, TrackedStateContext) {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        tracked_state
            .writer(transaction.as_mut())
            .write_root("base", None, base_rows)
            .await
            .expect("base root should write");
        tracked_state
            .writer(transaction.as_mut())
            .write_root("target", None, target_rows)
            .await
            .expect("target root should write");
        tracked_state
            .writer(transaction.as_mut())
            .write_root("source", None, source_rows)
            .await
            .expect("source root should write");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
        (storage, tracked_state)
    }

    fn merge_patch_ids(plan: &TrackedStateMergePlan) -> Vec<String> {
        plan.patches
            .iter()
            .map(|entry| entry.identity().entity_id.as_string().expect("identity"))
            .collect()
    }

    fn merge_conflict_ids(plan: &TrackedStateMergePlan) -> Vec<String> {
        plan.conflicts
            .iter()
            .map(|entry| entry.identity.entity_id.as_string().expect("identity"))
            .collect()
    }

    fn tombstone(entity_id: &str, change_id: &str, commit_id: &str) -> TrackedStateRow {
        let mut row = row(entity_id, change_id, commit_id);
        row.snapshot_content = None;
        row
    }

    fn row(entity_id: &str, change_id: &str, commit_id: &str) -> TrackedStateRow {
        row_with_value(entity_id, change_id, commit_id, "value")
    }

    fn row_with_value(
        entity_id: &str,
        change_id: &str,
        commit_id: &str,
        value: &str,
    ) -> TrackedStateRow {
        TrackedStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single(entity_id),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            change_id: change_id.to_string(),
            commit_id: commit_id.to_string(),
        }
    }
}
