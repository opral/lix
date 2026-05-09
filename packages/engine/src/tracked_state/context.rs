use crate::commit_store::CommitStoreContext;
use crate::storage::{StorageReader, StorageWriteSet};
use crate::tracked_state::by_file_index::ByFileIndex;
use crate::tracked_state::codec::{encode_key_ref, encode_value_ref};
use crate::tracked_state::diff::{diff_commits, TrackedStateDiff, TrackedStateDiffRequest};
use crate::tracked_state::materialize_index_entries;
use crate::tracked_state::merge::{self, TrackedStateMergePlan};
use crate::tracked_state::storage;
use crate::tracked_state::tree::TrackedStateTree;
use crate::tracked_state::types::{
    TrackedStateIndexValue, TrackedStateKey, TrackedStateKeyRef, TrackedStateMutation,
    TrackedStateTreeScanRequest,
};
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateDeltaRef, TrackedStateRowRequest,
    TrackedStateScanRequest,
};
use crate::LixError;

/// Factory for rebuildable tracked-state readers and writers.
///
/// Tracked state is stored as content-addressed roots. Version refs
/// choose which commit/root to read; this context only owns root operations.
#[derive(Clone)]
pub(crate) struct TrackedStateContext {
    tree: TrackedStateTree,
    commit_store: CommitStoreContext,
}

impl TrackedStateContext {
    pub(crate) fn new() -> Self {
        Self {
            tree: TrackedStateTree::new(),
            commit_store: CommitStoreContext::new(),
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
            commit_store: self.commit_store,
        }
    }

    /// Creates a tracked-state writer over a caller-owned transaction and write set.
    pub(crate) fn writer<'a, S>(
        &'a self,
        store: &'a mut S,
        writes: &'a mut StorageWriteSet,
    ) -> TrackedStateWriter<'a, S>
    where
        S: StorageReader + ?Sized,
    {
        TrackedStateWriter {
            tree: self.tree.clone(),
            store,
            writes,
        }
    }

    /// Rebuilds tracked state at one commit from commit_store facts.
    pub(crate) async fn rebuild_at_commit<S>(
        &self,
        store: &mut S,
        writes: &mut StorageWriteSet,
        commit_store: &CommitStoreContext,
        commit_id: &str,
    ) -> Result<TrackedStateWriteReport, LixError>
    where
        S: StorageReader + ?Sized,
    {
        crate::tracked_state::rebuild::rebuild_at_commit(
            self,
            store,
            writes,
            commit_store,
            commit_id,
        )
        .await
    }
}

/// Store-backed tracked-state reader created by `TrackedStateContext`.
pub(crate) struct TrackedStateStoreReader<S> {
    store: S,
    tree: TrackedStateTree,
    commit_store: CommitStoreContext,
}

impl<S> TrackedStateStoreReader<S>
where
    S: StorageReader,
{
    pub(crate) async fn scan_rows_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<MaterializedTrackedStateRow>, LixError> {
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
            rows
        };
        let projection = crate::tracked_state::TrackedMaterializationProjection::from_columns(
            &request.projection.columns,
        );
        let mut rows =
            materialize_index_entries(&mut self.store, &self.commit_store, rows, &projection)
                .await?;
        if !request.filter.include_tombstones {
            rows.retain(|row| !row.deleted);
        }
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    pub(crate) async fn load_rows_at_commit(
        &mut self,
        commit_id: &str,
        requests: &[TrackedStateRowRequest],
    ) -> Result<Vec<Option<MaterializedTrackedStateRow>>, LixError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let keys = requests
            .iter()
            .map(tracked_key_from_request)
            .collect::<Result<Vec<_>, _>>()?;
        let Some(root_id) = self.tree.load_root(&mut self.store, commit_id).await? else {
            return Ok(vec![None; requests.len()]);
        };
        let values = self.tree.get_many(&mut self.store, &root_id, &keys).await?;
        let mut entry_indices = Vec::new();
        let mut entries = Vec::new();
        for (index, (key, value)) in keys.into_iter().zip(values).enumerate() {
            if let Some(value) = value {
                entry_indices.push(index);
                entries.push((key, value));
            }
        }
        let materialized = materialize_index_entries(
            &mut self.store,
            &self.commit_store,
            entries,
            &crate::tracked_state::TrackedMaterializationProjection::full(),
        )
        .await?;
        let mut rows = vec![None; requests.len()];
        for (index, row) in entry_indices.into_iter().zip(materialized) {
            rows[index] = Some(row);
        }
        Ok(rows)
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
    ) -> Result<Vec<crate::tracked_state::types::TrackedStateTreeDiffEntry>, LixError> {
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
        Ok(entries)
    }

    pub(crate) async fn materialize_tree_value(
        &mut self,
        key: TrackedStateKey,
        value: TrackedStateIndexValue,
    ) -> Result<MaterializedTrackedStateRow, LixError> {
        let mut rows = materialize_index_entries(
            &mut self.store,
            &self.commit_store,
            vec![(key, value)],
            &crate::tracked_state::TrackedMaterializationProjection::full(),
        )
        .await?;
        rows.pop().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state materialization returned no row for one index entry",
            )
        })
    }

    async fn scan_rows_at_commit_by_file_index(
        &mut self,
        primary_root_id: &crate::tracked_state::types::TrackedStateRootId,
        by_file_root_id: &crate::tracked_state::types::TrackedStateRootId,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
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
            return Ok(rows);
        }
        let index_rows = self
            .tree
            .scan(&mut self.store, by_file_root_id, &by_file_request)
            .await?;
        let mut rows = Vec::new();
        let tree_request = tree_scan_request_from_tracked(request);
        let needs_payloads = scan_needs_json_payloads(request);
        if needs_payloads {
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
                rows.push((primary_key, value));
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
    #[allow(dead_code)]
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
}

/// Writer for commit-store-backed tracked-state projection roots.
pub(crate) struct TrackedStateWriter<'a, S: ?Sized> {
    tree: TrackedStateTree,
    store: &'a mut S,
    writes: &'a mut StorageWriteSet,
}

impl<S> TrackedStateWriter<'_, S>
where
    S: StorageReader + ?Sized,
{
    /// Stages one tracked-state projection delta for `commit_id`.
    pub(crate) async fn stage_delta<'a, I>(
        &mut self,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        deltas: I,
    ) -> Result<TrackedStateWriteReport, LixError>
    where
        I: IntoIterator<Item = TrackedStateDeltaRef<'a>>,
    {
        let deltas = deltas.into_iter().collect::<Vec<_>>();
        let base_root = match parent_commit_id {
            Some(parent_commit_id) => {
                let Some(root) = self.tree.load_root(self.store, parent_commit_id).await? else {
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
        let mut mutations = Vec::with_capacity(deltas.len());
        let mut by_file_mutations = Vec::with_capacity(deltas.len());
        for delta in &deltas {
            let key = TrackedStateKeyRef {
                schema_key: delta.change.schema_key,
                file_id: delta.change.file_id,
                entity_id: delta.change.entity_id,
            };
            let value = crate::tracked_state::types::TrackedStateIndexValueRef {
                change_locator: delta.locator,
                created_at: delta.created_at,
                updated_at: delta.updated_at,
            };
            mutations.push(TrackedStateMutation::put_encoded(
                encode_key_ref(key),
                encode_value_ref(value),
            ));
            by_file_mutations.push(TrackedStateMutation::put_encoded(
                ByFileIndex::encode_key_ref(key),
                ByFileIndex::encode_header_value_ref(value),
            ));
        }
        let result = self
            .tree
            .apply_mutations(
                self.store,
                self.writes,
                base_root.as_ref(),
                mutations,
                Some(commit_id),
            )
            .await?;

        let by_file_base_root = match parent_commit_id {
            Some(parent_commit_id) => storage::load_by_file_root(self.store, parent_commit_id)
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
        let by_file_result = self
            .tree
            .apply_mutations(
                self.store,
                self.writes,
                by_file_base_root.as_ref(),
                by_file_mutations,
                None,
            )
            .await?;
        storage::stage_by_file_root(self.writes, commit_id, &by_file_result.root_id);
        Ok(TrackedStateWriteReport {
            commit_id: commit_id.to_string(),
            changed_rows: deltas.len(),
            primary_chunk_puts: result.chunk_count,
            by_file_chunk_puts: by_file_result.chunk_count,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateWriteReport {
    pub(crate) commit_id: String,
    pub(crate) changed_rows: usize,
    pub(crate) primary_chunk_puts: usize,
    pub(crate) by_file_chunk_puts: usize,
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

fn scan_needs_json_payloads(request: &TrackedStateScanRequest) -> bool {
    if request.projection.columns.is_empty() {
        return true;
    }
    request
        .projection
        .columns
        .iter()
        .any(|column| column == "snapshot_content" || column == "metadata")
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
    use crate::storage::{StorageContext, StorageWriteTransaction};
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

        let error = write_root_for_test(
            transaction.as_mut(),
            &tracked_state,
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
        write_root_for_test(
            transaction.as_mut(),
            &tracked_state,
            "commit-1",
            None,
            &[file_a, file_b],
        )
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
            rows[0]
                .entity_id
                .as_single_string_owned()
                .expect("entity id"),
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
        write_root_for_test(
            transaction.as_mut(),
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
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
        write_root_for_test(
            transaction.as_mut(),
            &tracked_state,
            "commit-1",
            None,
            &[live, deleted],
        )
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
            rows[0]
                .entity_id
                .as_single_string_owned()
                .expect("entity id"),
            "entity-live"
        );
    }

    #[tokio::test]
    async fn reads_resolve_json_snapshot_refs() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let large_value = "x".repeat(1536);
        let row = row_with_value("entity-a", "change-a", "commit-1", &large_value);

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_root_for_test(
            transaction.as_mut(),
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
        .await
        .expect("root should write");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut reader = tracked_state.reader(storage.clone());
        let loaded = reader
            .load_rows_at_commit(
                "commit-1",
                &[TrackedStateRowRequest {
                    schema_key: row.schema_key.clone(),
                    entity_id: row.entity_id.clone(),
                    file_id: NullableKeyFilter::Null,
                }],
            )
            .await
            .expect("row should load")
            .pop()
            .flatten()
            .expect("row should exist");
        let scanned = reader
            .scan_rows_at_commit("commit-1", &TrackedStateScanRequest::default())
            .await
            .expect("rows should scan");

        assert_eq!(loaded.snapshot_content, row.snapshot_content);
        assert_eq!(scanned[0].snapshot_content, row.snapshot_content);
    }

    #[tokio::test]
    async fn projection_cache_uses_seen_updated_at_not_change_created_at() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let mut row = row("entity-a", "change-a", "commit-1");
        row.created_at = "2026-01-01T00:00:00Z".to_string();
        row.updated_at = "2026-01-02T00:00:00Z".to_string();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_root_for_test(
            transaction.as_mut(),
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
        .await
        .expect("root should write");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let loaded = tracked_state
            .reader(storage.clone())
            .load_rows_at_commit(
                "commit-1",
                &[TrackedStateRowRequest {
                    schema_key: row.schema_key.clone(),
                    entity_id: row.entity_id.clone(),
                    file_id: NullableKeyFilter::Null,
                }],
            )
            .await
            .expect("row should load")
            .pop()
            .flatten()
            .expect("row should exist");

        assert_eq!(loaded.created_at, "2026-01-01T00:00:00Z");
        assert_eq!(loaded.updated_at, "2026-01-02T00:00:00Z");
    }

    #[tokio::test]
    async fn projected_scans_do_not_materialize_snapshot_when_snapshot_content_is_omitted() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let large_value = "x".repeat(1536);
        let row = row_with_value("entity-a", "change-a", "commit-1", &large_value);

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_root_for_test(
            transaction.as_mut(),
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
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
        base_rows: &[MaterializedTrackedStateRow],
        target_rows: &[MaterializedTrackedStateRow],
        source_rows: &[MaterializedTrackedStateRow],
    ) -> (StorageContext, TrackedStateContext) {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let tracked_state = TrackedStateContext::new();
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_root_for_test(
            transaction.as_mut(),
            &tracked_state,
            "base",
            None,
            base_rows,
        )
        .await
        .expect("base root should write");
        write_root_for_test(
            transaction.as_mut(),
            &tracked_state,
            "target",
            None,
            target_rows,
        )
        .await
        .expect("target root should write");
        write_root_for_test(
            transaction.as_mut(),
            &tracked_state,
            "source",
            None,
            source_rows,
        )
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
            .map(|entry| {
                entry
                    .identity()
                    .entity_id
                    .as_single_string_owned()
                    .expect("identity")
            })
            .collect()
    }

    fn merge_conflict_ids(plan: &TrackedStateMergePlan) -> Vec<String> {
        plan.conflicts
            .iter()
            .map(|entry| {
                entry
                    .identity
                    .entity_id
                    .as_single_string_owned()
                    .expect("identity")
            })
            .collect()
    }

    async fn write_root_for_test(
        transaction: &mut dyn StorageWriteTransaction,
        tracked_state: &TrackedStateContext,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        rows: &[MaterializedTrackedStateRow],
    ) -> Result<(), LixError> {
        crate::test_support::stage_tracked_root_from_materialized(
            transaction,
            tracked_state,
            commit_id,
            parent_commit_id,
            rows,
        )
        .await
    }

    fn tombstone(entity_id: &str, change_id: &str, commit_id: &str) -> MaterializedTrackedStateRow {
        let mut row = row(entity_id, change_id, commit_id);
        row.snapshot_content = None;
        row
    }

    fn row(entity_id: &str, change_id: &str, commit_id: &str) -> MaterializedTrackedStateRow {
        row_with_value(entity_id, change_id, commit_id, "value")
    }

    fn row_with_value(
        entity_id: &str,
        change_id: &str,
        commit_id: &str,
        value: &str,
    ) -> MaterializedTrackedStateRow {
        MaterializedTrackedStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single(entity_id),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            change_id: change_id.to_string(),
            commit_id: commit_id.to_string(),
        }
    }
}
