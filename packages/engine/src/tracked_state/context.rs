use std::collections::{BTreeMap, BTreeSet};

use crate::commit_store::CommitStoreContext;
use crate::storage::{StorageRead, StorageWriteSet};
use crate::tracked_state::by_file_index::ByFileIndex;
use crate::tracked_state::codec::{encode_key_ref, encode_value_ref};
use crate::tracked_state::diff::{diff_commits, TrackedStateDiff, TrackedStateDiffRequest};
use crate::tracked_state::materialize_index_entries;
use crate::tracked_state::merge::{self, TrackedStateMergePlan};
use crate::tracked_state::storage;
use crate::tracked_state::storage::DeltaJsonPackIndexesRef;
use crate::tracked_state::tree::TrackedStateTree;
use crate::tracked_state::types::{
    TrackedStateIndexValue, TrackedStateKey, TrackedStateKeyRef, TrackedStateMutation,
    TrackedStateTreeDiffEntry, TrackedStateTreeScanRequest,
};
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateDeltaRef, TrackedStateRowRequest,
    TrackedStateScanRequest,
};
use crate::LixError;

/// Factory for tracked-state readers, delta writers, and projection-root materializers.
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
        S: StorageRead + Send + Sync,
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
        store: &'a S,
        writes: &'a mut StorageWriteSet,
    ) -> TrackedStateWriter<'a, S>
    where
        S: StorageRead + Send + Sync + ?Sized,
    {
        TrackedStateWriter {
            tree: self.tree.clone(),
            store,
            writes,
        }
    }

    /// Creates an explicit tracked-state projection-root materializer.
    ///
    /// Normal commits should use `writer(...).stage_delta(...)`. Materializing a
    /// projection root is a caller-chosen maintenance/read-acceleration step.
    pub(crate) fn materializer<'a, S>(
        &'a self,
        store: &'a S,
        writes: &'a mut StorageWriteSet,
        commit_store: &'a CommitStoreContext,
    ) -> TrackedStateMaterializer<'a, S>
    where
        S: StorageRead + Send + Sync + ?Sized,
    {
        TrackedStateMaterializer {
            tracked_state: self,
            store,
            writes,
            commit_store,
        }
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
    S: StorageRead + Send + Sync,
{
    pub(crate) async fn scan_rows_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<MaterializedTrackedStateRow>, LixError> {
        let root_id = self.tree.load_root(&mut self.store, commit_id).await?;
        let rows = if let Some(root_id) = root_id {
            if ByFileIndex::should_use(request) {
                if let Some(by_file_root_id) =
                    storage::load_by_file_root(&mut self.store, commit_id).await?
                {
                    self.scan_rows_at_commit_by_file_index(&root_id, &by_file_root_id, request)
                        .await?
                } else {
                    self.tree
                        .scan(
                            &mut self.store,
                            &root_id,
                            &tree_scan_request_from_tracked(request),
                        )
                        .await?
                }
            } else {
                self.tree
                    .scan(
                        &mut self.store,
                        &root_id,
                        &tree_scan_request_from_tracked(request),
                    )
                    .await?
            }
        } else {
            self.projection_entries_at_commit(commit_id, &tree_scan_request_from_tracked(request))
                .await?
        };
        let projection = crate::tracked_state::TrackedMaterializationProjection::from_columns(
            &request.projection.columns,
        );
        let mut rows = materialize_index_entries(&mut self.store, rows, &projection).await?;
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
        let values = self
            .projection_values_at_commit_for_keys(commit_id, &keys)
            .await?;
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
        if !self.projection_has_pending_deltas(left_commit_id).await?
            && !self.projection_has_pending_deltas(right_commit_id).await?
            && self.projection_root_exists(left_commit_id).await?
            && self.projection_root_exists(right_commit_id).await?
        {
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
            return Ok(entries);
        }

        if let Some(entries) = self
            .diff_pending_delta_suffix(left_commit_id, right_commit_id, request)
            .await?
        {
            return Ok(entries);
        }

        let left = self
            .projection_entries_at_commit(left_commit_id, request)
            .await?
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let right = self
            .projection_entries_at_commit(right_commit_id, request)
            .await?
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let keys = left
            .keys()
            .chain(right.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        let entries = keys
            .into_iter()
            .filter_map(|key| {
                let before = left.get(&key).cloned().map(|value| (key.clone(), value));
                let after = right.get(&key).cloned().map(|value| (key, value));
                if before == after {
                    None
                } else {
                    Some(TrackedStateTreeDiffEntry { before, after })
                }
            })
            .collect();
        Ok(entries)
    }

    async fn diff_pending_delta_suffix(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<Option<Vec<TrackedStateTreeDiffEntry>>, LixError> {
        let left_delta_ids = self
            .delta_commit_ids_since_projection_root(left_commit_id)
            .await?;
        let right_delta_ids = self
            .delta_commit_ids_since_projection_root(right_commit_id)
            .await?;
        let left_base_commit_id = self
            .projection_base_commit_id(left_commit_id, &left_delta_ids)
            .await?;
        let right_base_commit_id = self
            .projection_base_commit_id(right_commit_id, &right_delta_ids)
            .await?;
        if left_base_commit_id != right_base_commit_id {
            return Ok(None);
        }

        if right_delta_ids.starts_with(&left_delta_ids) {
            let suffix = &right_delta_ids[left_delta_ids.len()..];
            return self
                .diff_pending_delta_suffix_from_base(left_commit_id, suffix, request, true)
                .await
                .map(Some);
        }

        if left_delta_ids.starts_with(&right_delta_ids) {
            let suffix = &left_delta_ids[right_delta_ids.len()..];
            return self
                .diff_pending_delta_suffix_from_base(right_commit_id, suffix, request, false)
                .await
                .map(Some);
        }

        Ok(None)
    }

    async fn diff_pending_delta_suffix_from_base(
        &mut self,
        base_commit_id: &str,
        suffix_commit_ids: &[String],
        request: &TrackedStateTreeScanRequest,
        suffix_is_after: bool,
    ) -> Result<Vec<TrackedStateTreeDiffEntry>, LixError> {
        if suffix_commit_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut changed = BTreeMap::<TrackedStateKey, TrackedStateIndexValue>::new();
        for commit_id in suffix_commit_ids {
            let Some(delta_entries) = storage::load_delta_pack(&mut self.store, commit_id).await?
            else {
                continue;
            };
            for delta in delta_entries {
                if request.matches_key(&delta.key) {
                    changed.insert(delta.key, delta.value);
                }
            }
        }

        if changed.is_empty() {
            return Ok(Vec::new());
        }

        let keys = changed.keys().cloned().collect::<Vec<_>>();
        let base_values = self
            .projection_values_at_commit_for_keys(base_commit_id, &keys)
            .await?;
        let entries = keys
            .into_iter()
            .zip(base_values)
            .filter_map(|(key, base_value)| {
                let changed_value = changed.get(&key).cloned();
                let (before_value, after_value) = if suffix_is_after {
                    (base_value, changed_value)
                } else {
                    (changed_value, base_value)
                };
                if before_value == after_value {
                    return None;
                }
                Some(TrackedStateTreeDiffEntry {
                    before: before_value.map(|value| (key.clone(), value)),
                    after: after_value.map(|value| (key, value)),
                })
            })
            .collect();
        Ok(entries)
    }

    pub(crate) async fn materialize_tree_values(
        &mut self,
        entries: Vec<(TrackedStateKey, TrackedStateIndexValue)>,
    ) -> Result<Vec<MaterializedTrackedStateRow>, LixError> {
        materialize_index_entries(
            &mut self.store,
            entries,
            &crate::tracked_state::TrackedMaterializationProjection::full(),
        )
        .await
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

    async fn projection_root_exists(&mut self, commit_id: &str) -> Result<bool, LixError> {
        Ok(self
            .tree
            .load_root(&mut self.store, commit_id)
            .await?
            .is_some())
    }

    async fn projection_has_pending_deltas(&mut self, commit_id: &str) -> Result<bool, LixError> {
        Ok(!self
            .delta_commit_ids_since_projection_root(commit_id)
            .await?
            .is_empty())
    }

    async fn projection_entries_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<Vec<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
        let delta_commit_ids = self
            .delta_commit_ids_since_projection_root(commit_id)
            .await?;
        let base_commit_id = self
            .projection_base_commit_id(commit_id, &delta_commit_ids)
            .await?;
        if base_commit_id.is_none() && delta_commit_ids.len() == 1 {
            return self
                .single_delta_pack_entries(&delta_commit_ids[0], request)
                .await;
        }
        let mut entries = if let Some(base_commit_id) = base_commit_id {
            let root_id = self
                .tree
                .load_root(&mut self.store, &base_commit_id)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "tracked_state projection base root '{base_commit_id}' disappeared"
                        ),
                    )
                })?;
            self.tree
                .scan(&mut self.store, &root_id, request)
                .await?
                .into_iter()
                .collect::<BTreeMap<_, _>>()
        } else {
            BTreeMap::new()
        };
        self.apply_delta_packs_to_entries(&delta_commit_ids, Some(request), &mut entries)
            .await?;
        Ok(entries.into_iter().collect())
    }

    async fn single_delta_pack_entries(
        &mut self,
        commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<Vec<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
        let Some(delta_entries) = storage::load_delta_pack(&mut self.store, commit_id).await?
        else {
            return Ok(Vec::new());
        };
        let mut rows = delta_entries
            .into_iter()
            .enumerate()
            .filter_map(|(ordinal, delta)| {
                request
                    .matches_key(&delta.key)
                    .then_some((ordinal, delta.key, delta.value))
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)));

        let mut out = Vec::new();
        let mut rows = rows.into_iter().peekable();
        while let Some((_, key, mut value)) = rows.next() {
            while rows.peek().is_some_and(|(_, next_key, _)| next_key == &key) {
                let (_, _, next_value) = rows
                    .next()
                    .expect("peek confirmed duplicate delta entry exists");
                value = next_value;
            }
            if !request.include_tombstones && value.deleted {
                continue;
            }
            out.push((key, value));
        }
        Ok(out)
    }

    async fn projection_values_at_commit_for_keys(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKey],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        let delta_commit_ids = self
            .delta_commit_ids_since_projection_root(commit_id)
            .await?;
        let base_commit_id = self
            .projection_base_commit_id(commit_id, &delta_commit_ids)
            .await?;
        let mut entries = if let Some(base_commit_id) = base_commit_id {
            let root_id = self
                .tree
                .load_root(&mut self.store, &base_commit_id)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "tracked_state projection base root '{base_commit_id}' disappeared"
                        ),
                    )
                })?;
            let values = self.tree.get_many(&mut self.store, &root_id, keys).await?;
            keys.iter()
                .cloned()
                .zip(values)
                .filter_map(|(key, value)| value.map(|value| (key, value)))
                .collect::<BTreeMap<_, _>>()
        } else {
            BTreeMap::new()
        };
        let key_filter = keys.iter().cloned().collect::<BTreeSet<_>>();
        self.apply_delta_packs_to_entries_for_keys(&delta_commit_ids, &key_filter, &mut entries)
            .await?;
        Ok(keys.iter().map(|key| entries.get(key).cloned()).collect())
    }

    async fn projection_base_commit_id(
        &mut self,
        commit_id: &str,
        delta_commit_ids: &[String],
    ) -> Result<Option<String>, LixError> {
        if delta_commit_ids.is_empty() {
            return Ok(if self.projection_root_exists(commit_id).await? {
                Some(commit_id.to_string())
            } else {
                None
            });
        }
        let Some(first_delta_commit_id) = delta_commit_ids.first() else {
            return Ok(None);
        };
        let commit = self
            .commit_store
            .load_commit_from(&mut self.store, first_delta_commit_id)
            .await?
            .ok_or_else(|| missing_commit_error(first_delta_commit_id))?;
        let Some(parent_id) = commit.parent_ids.first() else {
            return Ok(None);
        };
        Ok(if self.projection_root_exists(parent_id).await? {
            Some(parent_id.clone())
        } else {
            None
        })
    }

    async fn delta_commit_ids_since_projection_root(
        &mut self,
        commit_id: &str,
    ) -> Result<Vec<String>, LixError> {
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        let mut current = Some(commit_id.to_string());
        while let Some(current_id) = current {
            if !seen.insert(current_id.clone()) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("tracked_state projection found first-parent cycle at '{current_id}'"),
                ));
            }
            if self
                .tree
                .load_root(&mut self.store, &current_id)
                .await?
                .is_some()
            {
                break;
            }
            if storage::delta_pack_exists(&mut self.store, &current_id).await? {
                out.push(current_id.clone());
            }
            let commit = self
                .commit_store
                .load_commit_from(&mut self.store, &current_id)
                .await?
                .ok_or_else(|| missing_commit_error(&current_id))?;
            current = commit.parent_ids.first().cloned();
        }
        out.reverse();
        Ok(out)
    }

    async fn apply_delta_packs_to_entries(
        &mut self,
        commit_ids: &[String],
        request: Option<&TrackedStateTreeScanRequest>,
        entries: &mut BTreeMap<TrackedStateKey, TrackedStateIndexValue>,
    ) -> Result<(), LixError> {
        for commit_id in commit_ids {
            let Some(delta_entries) = storage::load_delta_pack(&mut self.store, commit_id).await?
            else {
                continue;
            };
            for delta in delta_entries {
                if let Some(request) = request {
                    if !request.matches_key(&delta.key) {
                        continue;
                    }
                    if !request.include_tombstones && delta.value.deleted {
                        entries.remove(&delta.key);
                        continue;
                    }
                    entries.insert(delta.key, delta.value);
                } else {
                    entries.insert(delta.key, delta.value);
                }
            }
        }
        Ok(())
    }

    async fn apply_delta_packs_to_entries_for_keys(
        &mut self,
        commit_ids: &[String],
        keys: &BTreeSet<TrackedStateKey>,
        entries: &mut BTreeMap<TrackedStateKey, TrackedStateIndexValue>,
    ) -> Result<(), LixError> {
        for commit_id in commit_ids {
            let Some(delta_entries) = storage::load_delta_pack(&mut self.store, commit_id).await?
            else {
                continue;
            };
            for delta in delta_entries {
                if keys.contains(&delta.key) {
                    entries.insert(delta.key, delta.value);
                }
            }
        }
        Ok(())
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
    store: &'a S,
    writes: &'a mut StorageWriteSet,
}

/// Explicit projection-root materializer created by `TrackedStateContext`.
pub(crate) struct TrackedStateMaterializer<'a, S: ?Sized> {
    pub(super) tracked_state: &'a TrackedStateContext,
    pub(super) store: &'a S,
    pub(super) writes: &'a mut StorageWriteSet,
    pub(super) commit_store: &'a CommitStoreContext,
}

impl<S> TrackedStateMaterializer<'_, S>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    pub(crate) async fn materialize_root_at(
        &mut self,
        commit_id: &str,
    ) -> Result<TrackedStateWriteReport, LixError> {
        crate::tracked_state::materializer::materialize_root_at(self, commit_id).await
    }
}

impl<S> TrackedStateWriter<'_, S>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    /// Stages one tracked-state projection delta for `commit_id`.
    pub(crate) async fn stage_delta(
        &mut self,
        commit_id: &str,
        _parent_commit_id: Option<&str>,
        deltas: &[TrackedStateDeltaRef<'_>],
    ) -> Result<TrackedStateWriteReport, LixError> {
        storage::stage_delta_pack_refs(self.writes, commit_id, deltas)?;
        Ok(TrackedStateWriteReport {
            commit_id: commit_id.to_string(),
            changed_rows: deltas.len(),
            primary_chunk_puts: 0,
            by_file_chunk_puts: 0,
        })
    }

    pub(crate) async fn stage_delta_with_json_pack_indexes(
        &mut self,
        commit_id: &str,
        _parent_commit_id: Option<&str>,
        deltas: &[TrackedStateDeltaRef<'_>],
        json_pack_indexes: DeltaJsonPackIndexesRef<'_>,
    ) -> Result<TrackedStateWriteReport, LixError> {
        storage::stage_delta_pack_refs_with_json_pack_indexes(
            self.writes,
            commit_id,
            deltas,
            json_pack_indexes,
        )?;
        Ok(TrackedStateWriteReport {
            commit_id: commit_id.to_string(),
            changed_rows: deltas.len(),
            primary_chunk_puts: 0,
            by_file_chunk_puts: 0,
        })
    }

    pub(crate) async fn stage_projection_root<'a, I>(
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
        for delta in &deltas {
            let key = TrackedStateKeyRef {
                schema_key: delta.change.schema_key,
                file_id: delta.change.file_id,
                entity_id: delta.change.entity_id,
            };
            let value = crate::tracked_state::types::TrackedStateIndexValueRef {
                change_locator: delta.locator,
                deleted: delta.change.snapshot_ref.is_none(),
                snapshot_ref: delta.change.snapshot_ref,
                metadata_ref: delta.change.metadata_ref,
                created_at: delta.created_at,
                updated_at: delta.updated_at,
            };
            mutations.push(TrackedStateMutation::put_encoded(
                encode_key_ref(key),
                encode_value_ref(value),
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
            Some(parent_commit_id) => {
                storage::load_by_file_root(self.store, parent_commit_id).await?
            }
            None => None,
        };
        let concrete_file_deltas = deltas
            .iter()
            .filter(|delta| delta.change.file_id.is_some())
            .collect::<Vec<_>>();
        let by_file_chunk_puts = if concrete_file_deltas.is_empty() {
            if let Some(by_file_base_root) = by_file_base_root.as_ref() {
                storage::stage_by_file_root(self.writes, commit_id, by_file_base_root);
            }
            0
        } else {
            let mut by_file_mutations = Vec::with_capacity(concrete_file_deltas.len());
            for delta in concrete_file_deltas {
                let key = TrackedStateKeyRef {
                    schema_key: delta.change.schema_key,
                    file_id: delta.change.file_id,
                    entity_id: delta.change.entity_id,
                };
                let header_value = crate::tracked_state::types::TrackedStateIndexValueRef {
                    change_locator: delta.locator,
                    deleted: delta.change.snapshot_ref.is_none(),
                    snapshot_ref: None,
                    metadata_ref: None,
                    created_at: delta.created_at,
                    updated_at: delta.updated_at,
                };
                by_file_mutations.push(TrackedStateMutation::put_encoded(
                    ByFileIndex::encode_key_ref(key),
                    ByFileIndex::encode_header_value_ref(header_value),
                ));
            }
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
            by_file_result.chunk_count
        };
        Ok(TrackedStateWriteReport {
            commit_id: commit_id.to_string(),
            changed_rows: deltas.len(),
            primary_chunk_puts: result.chunk_count,
            by_file_chunk_puts,
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

fn missing_commit_error(commit_id: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state projection references missing commit '{commit_id}'"),
    )
}

fn tree_scan_request_from_tracked(
    request: &TrackedStateScanRequest,
) -> TrackedStateTreeScanRequest {
    TrackedStateTreeScanRequest {
        schema_keys: request.filter.schema_keys.clone(),
        entity_ids: request.filter.entity_ids.clone(),
        file_ids: request.filter.file_ids.clone(),
        include_tombstones: request.filter.include_tombstones,
        // User limits belong above delta overlay and tombstone visibility.
        // Pushing them into the physical tree can stop on rows that are later
        // hidden, returning too few live rows.
        limit: None,
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
    use super::*;
    use crate::storage::StorageContext;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};
    use crate::NullableKeyFilter;

    #[tokio::test]
    async fn stage_delta_does_not_require_parent_projection_root() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();

        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-child",
            Some("missing-parent"),
            &[row("entity-child", "change-child", "commit-child")],
        )
        .await
        .expect("delta pack staging should not require a parent projection root");
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
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
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
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
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
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
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
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut file_a = row("entity-a", "change-a", "commit-1");
        file_a.file_id = Some("file-a.json".to_string());
        let mut file_b = row("entity-b", "change-b", "commit-1");
        file_b.file_id = Some("file-b.json".to_string());
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            &[file_a, file_b],
        )
        .await
        .expect("root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut row = row("entity-a", "change-a", "commit-1");
        row.file_id = Some("file-a.json".to_string());
        let expected_snapshot = row.snapshot_content.clone();
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
        .await
        .expect("root should write");

        let mut reader = tracked_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
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
    async fn null_file_rows_do_not_stage_by_file_index() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let row = row("entity-a", "change-a", "commit-1");
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
        .await
        .expect("root should write");

        let by_file_root = storage::load_by_file_root(
            &storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
            "commit-1",
        )
        .await
        .expect("by-file root lookup should load");
        assert!(by_file_root.is_none());

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        file_ids: vec![NullableKeyFilter::Null],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("null file scan should fall back to primary tree");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .entity_id
                .as_single_string_owned()
                .expect("entity id"),
            "entity-a"
        );
    }

    #[tokio::test]
    async fn mixed_null_and_concrete_file_scan_uses_primary_tree() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let null_row = row("entity-null", "change-null", "commit-1");
        let mut file_row = row("entity-file", "change-file", "commit-2");
        file_row.file_id = Some("file-a.json".to_string());
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&null_row),
        )
        .await
        .expect("parent root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-2",
            Some("commit-1"),
            std::slice::from_ref(&file_row),
        )
        .await
        .expect("child root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .scan_rows_at_commit(
                "commit-2",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        file_ids: vec![
                            NullableKeyFilter::Null,
                            NullableKeyFilter::Value("file-a.json".to_string()),
                        ],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("mixed scan should use primary tree");

        let mut entity_ids = rows
            .iter()
            .map(|row| row.entity_id.as_single_string_owned().expect("entity id"))
            .collect::<Vec<_>>();
        entity_ids.sort();
        assert_eq!(entity_ids, vec!["entity-file", "entity-null"]);
    }

    #[tokio::test]
    async fn by_file_header_index_filters_tombstones_without_payload_sentinel() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut live = row("entity-live", "change-live", "commit-1");
        live.file_id = Some("file-a.json".to_string());
        let mut deleted = tombstone("entity-deleted", "change-delete", "commit-1");
        deleted.file_id = Some("file-a.json".to_string());
        write_root_for_test(&storage, &tracked_state, "commit-1", None, &[live, deleted])
            .await
            .expect("root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
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
    async fn pending_tombstone_delta_hides_materialized_base_row() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let base = row("entity-a", "change-base", "base");
        let delete = tombstone("entity-a", "change-delete", "child");
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            std::slice::from_ref(&base),
        )
        .await
        .expect("base delta should write");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        tracked_state
            .materializer(&read, &mut writes, &CommitStoreContext::new())
            .materialize_root_at("base")
            .await
            .expect("base projection root should materialize");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("materialized base should commit");
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("base"),
            std::slice::from_ref(&delete),
        )
        .await
        .expect("child tombstone delta should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .scan_rows_at_commit("child", &TrackedStateScanRequest::default())
            .await
            .expect("child scan should apply pending tombstone over base root");

        assert!(rows.is_empty(), "pending tombstone must hide base row");
    }

    #[tokio::test]
    async fn single_delta_pack_scan_keeps_last_delta_for_duplicate_key() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            &[
                row_with_value("entity-a", "change-a1", "commit-1", "first"),
                row_with_value("entity-b", "change-b", "commit-1", "middle"),
                row_with_value("entity-a", "change-a2", "commit-1", "second"),
                tombstone("entity-c", "change-c1", "commit-1"),
            ],
        )
        .await
        .expect("delta pack should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .scan_rows_at_commit("commit-1", &TrackedStateScanRequest::default())
            .await
            .expect("single delta pack should scan");

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter()
                .map(|row| (
                    row.entity_id.as_single_string_owned().expect("entity id"),
                    row.snapshot_content.clone()
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    "entity-a".to_string(),
                    Some("{\"value\":\"second\"}".to_string())
                ),
                (
                    "entity-b".to_string(),
                    Some("{\"value\":\"middle\"}".to_string())
                ),
            ]
        );
    }

    #[tokio::test]
    async fn scan_limit_applies_after_tombstone_visibility() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            &[
                tombstone("entity-a", "change-delete", "commit-1"),
                row("entity-b", "change-live", "commit-1"),
            ],
        )
        .await
        .expect("root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    limit: Some(1),
                    ..Default::default()
                },
            )
            .await
            .expect("limited scan should apply visibility before limit");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .entity_id
                .as_single_string_owned()
                .expect("entity id"),
            "entity-b"
        );
    }

    #[tokio::test]
    async fn by_file_scan_limit_applies_after_tombstone_visibility() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut deleted = tombstone("entity-a", "change-delete", "commit-1");
        deleted.file_id = Some("file-a.json".to_string());
        let mut live = row("entity-b", "change-live", "commit-1");
        live.file_id = Some("file-a.json".to_string());
        write_root_for_test(&storage, &tracked_state, "commit-1", None, &[deleted, live])
            .await
            .expect("root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
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
                    limit: Some(1),
                },
            )
            .await
            .expect("limited by-file scan should apply visibility before limit");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .entity_id
                .as_single_string_owned()
                .expect("entity id"),
            "entity-b"
        );
    }

    #[tokio::test]
    async fn reads_resolve_json_snapshot_refs() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let large_value = "x".repeat(1536);
        let row = row_with_value("entity-a", "change-a", "commit-1", &large_value);
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
        .await
        .expect("root should write");

        let mut reader = tracked_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut row = row("entity-a", "change-a", "commit-1");
        row.created_at = "2026-01-01T00:00:00Z".to_string();
        row.updated_at = "2026-01-02T00:00:00Z".to_string();
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
        .await
        .expect("root should write");

        let loaded = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let large_value = "x".repeat(1536);
        let row = row_with_value("entity-a", "change-a", "commit-1", &large_value);
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
        .await
        .expect("root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(&storage, &tracked_state, "base", None, base_rows)
            .await
            .expect("base root should write");
        write_root_for_test(&storage, &tracked_state, "target", None, target_rows)
            .await
            .expect("target root should write");
        write_root_for_test(&storage, &tracked_state, "source", None, source_rows)
            .await
            .expect("source root should write");
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
        storage: &StorageContext,
        tracked_state: &TrackedStateContext,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        rows: &[MaterializedTrackedStateRow],
    ) -> Result<(), LixError> {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        crate::test_support::stage_tracked_root_from_materialized(
            &read,
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
