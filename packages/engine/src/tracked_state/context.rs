use std::collections::{BTreeMap, HashMap, HashSet};

use crate::changelog::{
    ChangelogContext, CommitLoadEntry, CommitLoadRequest, CommitProjection, CommitVisibilityMode,
    StateRowIdentity,
};
use crate::common::{CanonicalSchemaKey, EntityId, FileId};
use crate::entity_identity::EntityIdentity;
use crate::storage::{StorageRead, StorageWriteSet};
use crate::tracked_state::by_file_index::ByFileIndex;
use crate::tracked_state::codec::{encode_key_ref, encode_value_ref};
use crate::tracked_state::diff::{
    diff_commits, diff_commits_with_validation, TrackedStateDiff, TrackedStateDiffRequest,
    TrackedStateDiffRow,
};
use crate::tracked_state::materialize_rows_from_index_entries;
use crate::tracked_state::merge::{self, TrackedStateMergePlan};
use crate::tracked_state::storage;
use crate::tracked_state::tree::TrackedStateTree;
use crate::tracked_state::types::{
    TrackedStateIndexValue, TrackedStateKey, TrackedStateKeyRef, TrackedStateMutation,
    TrackedStateProjectionMetadata, TrackedStateProjectionParent, TrackedStateRootId,
    TrackedStateTreeScanRequest,
};
use crate::tracked_state::TrackedStateRowRequest;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateDeltaRef, TrackedStateScanRequest,
};
use crate::LixError;

/// Factory for tracked-state readers, root writers, and projection-root rebuilders.
///
/// Tracked state is stored as content-addressed roots. Version refs
/// choose which commit/root to read; this context only owns root operations.
#[derive(Clone)]
pub(crate) struct TrackedStateContext {
    tree: TrackedStateTree,
}

impl TrackedStateContext {
    pub(crate) fn new() -> Self {
        Self {
            tree: TrackedStateTree::new(),
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
            chunk_overlay: storage::TrackedStateChunkOverlay::new(),
            staged_by_file_roots: BTreeMap::new(),
            staged_roots: BTreeMap::new(),
            tree: self.tree.clone(),
            store,
            writes,
        }
    }

    /// Creates an explicit tracked-state projection-root rebuilder.
    ///
    /// Normal commits stage projection roots directly. This rebuilder reconstructs
    /// a missing root from changelog facts as an explicit maintenance path.
    pub(crate) fn root_rebuilder<'a, S>(
        &'a self,
        store: &'a S,
        writes: &'a mut StorageWriteSet,
    ) -> TrackedStateRootRebuilder<'a, S>
    where
        S: StorageRead + Send + Sync + ?Sized,
    {
        TrackedStateRootRebuilder {
            tracked_state: self,
            store,
            writes,
        }
    }
}

/// Store-backed tracked-state reader created by `TrackedStateContext`.
pub(crate) struct TrackedStateStoreReader<S> {
    store: S,
    tree: TrackedStateTree,
}

#[allow(dead_code)]
struct DiffProjectionValidationCache {
    requested_identities: HashSet<StateRowIdentity>,
    visible_commit_winners: HashMap<String, HashMap<StateRowIdentity, String>>,
    projection_metadata: HashMap<String, TrackedStateProjectionMetadata>,
    projection_roots: HashMap<String, TrackedStateRootId>,
    tree_values: HashMap<(TrackedStateRootId, TrackedStateKey), Option<TrackedStateIndexValue>>,
    changelog_first_parents: HashMap<String, Option<String>>,
}

impl DiffProjectionValidationCache {
    fn new(requested_identities: HashSet<StateRowIdentity>) -> Self {
        Self {
            requested_identities,
            visible_commit_winners: HashMap::new(),
            projection_metadata: HashMap::new(),
            projection_roots: HashMap::new(),
            tree_values: HashMap::new(),
            changelog_first_parents: HashMap::new(),
        }
    }
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
        let rows = if let Some(root_id) = self.tree.load_root(&mut self.store, commit_id).await? {
            if ByFileIndex::should_use(request) && !request.filter.schema_keys.is_empty() {
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
            self.scan_rows_at_commit_from_changelog(commit_id, request)
                .await?
        };
        let projection =
            crate::tracked_state::TrackedRowProjection::from_columns(&request.projection.columns);
        let mut rows =
            materialize_rows_from_index_entries(&mut self.store, rows, &projection).await?;
        if !request.filter.include_tombstones {
            rows.retain(|row| !row.deleted);
        }
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    async fn scan_rows_at_commit_from_changelog(
        &mut self,
        commit_id: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
        let input =
            crate::tracked_state::projection_root_rebuild::build_projection_root_rebuild_input(
                &self.store,
                commit_id,
            )
            .await?;
        let tree_request = tree_scan_request_from_tracked(request);
        let mut rows = Vec::with_capacity(input.deltas.len());
        for delta in input.deltas {
            let key = TrackedStateKey {
                schema_key: delta.change.schema_key,
                file_id: delta.change.file_id,
                entity_id: delta.change.entity_id,
            };
            let value = TrackedStateIndexValue {
                change_locator: delta.locator,
                deleted: delta.change.snapshot_ref.is_none(),
                snapshot_ref: delta.change.snapshot_ref,
                metadata_ref: delta.change.metadata_ref,
                created_at: delta.created_at,
                updated_at: delta.updated_at,
            };
            if tree_request.matches(&key, &value) {
                rows.push((key, value));
            }
        }
        Ok(rows)
    }

    #[cfg(any(test, feature = "storage-benches"))]
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
            .projection_values_at_commit_for_keys_allow_rebuild(commit_id, &keys)
            .await?;
        let mut entry_indices = Vec::new();
        let mut entries = Vec::new();
        for (index, (key, value)) in keys.into_iter().zip(values).enumerate() {
            if let Some(value) = value {
                entry_indices.push(index);
                entries.push((key, value));
            }
        }
        let materialized = materialize_rows_from_index_entries(
            &mut self.store,
            entries,
            &crate::tracked_state::TrackedRowProjection::full(),
        )
        .await?;
        let mut rows = vec![None; requests.len()];
        for (index, row) in entry_indices.into_iter().zip(materialized) {
            rows[index] = Some(row);
        }
        Ok(rows)
    }

    pub(crate) async fn load_index_entries_at_commit(
        &mut self,
        commit_id: &str,
        requests: &[TrackedStateRowRequest],
    ) -> Result<Vec<Option<TrackedStateDiffRow>>, LixError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let keys = requests
            .iter()
            .map(tracked_key_from_request)
            .collect::<Result<Vec<_>, _>>()?;
        let root_id = self.load_ensured_root(commit_id).await?;
        let values = self.tree.get_many(&mut self.store, &root_id, &keys).await?;
        Ok(keys
            .into_iter()
            .zip(values)
            .map(|(key, value)| value.map(|value| TrackedStateDiffRow::from_tree_entry(key, value)))
            .collect())
    }

    pub(crate) async fn diff_commits(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStateDiffRequest,
    ) -> Result<TrackedStateDiff, LixError> {
        diff_commits(self, left_commit_id, right_commit_id, request).await
    }

    pub(crate) async fn diff_commits_with_validation(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStateDiffRequest,
        validate_left_root: bool,
        validate_right_root: bool,
    ) -> Result<TrackedStateDiff, LixError> {
        diff_commits_with_validation(
            self,
            left_commit_id,
            right_commit_id,
            request,
            validate_left_root,
            validate_right_root,
        )
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn validate_diff_rows_for_commits_against_changelog(
        &mut self,
        rows: &[(&TrackedStateDiffRow, &str)],
    ) -> Result<(), LixError> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut change_ids = rows
            .iter()
            .map(|(row, _)| row.change_id.clone())
            .collect::<Vec<_>>();
        change_ids.sort();
        change_ids.dedup();

        let mut changelog_reader = ChangelogContext::new().reader(&mut self.store);
        let loaded_changes = changelog_reader
            .load_physical_segment_changes(&change_ids)
            .await?;
        let mut changes = HashMap::new();
        for (change_id, loaded) in change_ids.into_iter().zip(loaded_changes) {
            let Some((location, change)) = loaded else {
                return Err(LixError::unknown(format!(
                    "tracked-state diff row references missing changelog change '{change_id}'"
                )));
            };
            changes.insert(change_id, (location, change));
        }

        let requested_identities = rows
            .iter()
            .map(|(row, _)| state_row_identity_from_diff_row(row))
            .collect::<Result<HashSet<_>, _>>()?;
        let mut validation_cache = DiffProjectionValidationCache::new(requested_identities);
        for (row, expected_commit_id) in rows {
            validate_diff_row_against_changelog(row, &changes)?;
            let change_created_at = changes
                .get(&row.change_id)
                .map(|(_, change)| change.created_at.as_str())
                .ok_or_else(|| {
                    LixError::unknown(format!(
                        "tracked-state diff row references missing changelog change '{}'",
                        row.change_id
                    ))
                })?;
            self.validate_diff_row_projection_membership(
                row,
                expected_commit_id,
                change_created_at,
                &mut validation_cache,
            )
            .await?;
        }
        Ok(())
    }

    async fn validate_diff_rows_physical_against_changelog(
        &mut self,
        rows: &[&TrackedStateDiffRow],
    ) -> Result<(), LixError> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut change_ids = rows
            .iter()
            .map(|row| row.change_id.clone())
            .collect::<Vec<_>>();
        change_ids.sort();
        change_ids.dedup();

        let mut changelog_reader = ChangelogContext::new().reader(&mut self.store);
        let loaded_changes = changelog_reader
            .load_physical_segment_changes(&change_ids)
            .await?;
        let mut changes = HashMap::new();
        for (change_id, loaded) in change_ids.into_iter().zip(loaded_changes) {
            let Some((location, change)) = loaded else {
                return Err(LixError::unknown(format!(
                    "tracked-state diff row references missing changelog change '{change_id}'"
                )));
            };
            changes.insert(change_id, (location, change));
        }

        for row in rows {
            validate_diff_row_against_changelog(row, &changes)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    async fn validate_diff_row_projection_membership(
        &mut self,
        row: &TrackedStateDiffRow,
        root_commit_id: &str,
        change_created_at: &str,
        cache: &mut DiffProjectionValidationCache,
    ) -> Result<(), LixError> {
        let identity = state_row_identity_from_diff_row(row)?;
        let key = TrackedStateKey {
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            entity_id: row.entity_id.clone(),
        };
        let root_metadata = self
            .load_cached_projection_metadata(root_commit_id, cache)
            .await?;
        self.validate_projection_parent_matches_changelog(root_commit_id, &root_metadata, cache)
            .await?;
        let (_, row_value) = row.clone().into_index_entry();
        let mut current_commit_id = root_commit_id.to_string();
        let mut seen = HashSet::new();
        loop {
            if !seen.insert(current_commit_id.clone()) {
                return Err(LixError::unknown(format!(
                    "tracked-state projection parent chain contains cycle at commit '{current_commit_id}'"
                )));
            }

            let winners = self
                .load_cached_visible_commit_winners(&current_commit_id, cache)
                .await?;
            if let Some(winner_change_id) = winners.get(&identity) {
                if winner_change_id != &row.change_id {
                    return Err(LixError::unknown(format!(
                        "tracked-state diff row references changelog change '{}' that is not the first-parent winner for commit '{}' and identity {:?}",
                        row.change_id, root_commit_id, identity
                    )));
                }
                self.validate_diff_row_created_at(
                    row,
                    &key,
                    &current_commit_id,
                    change_created_at,
                    cache,
                )
                .await?;
                return Ok(());
            }

            let metadata = self
                .load_cached_projection_metadata(&current_commit_id, cache)
                .await?;
            self.validate_projection_parent_matches_changelog(&current_commit_id, &metadata, cache)
                .await?;
            let Some(parent) = metadata.parent_roots.first() else {
                return Err(LixError::unknown(format!(
                    "tracked-state diff row references changelog change '{}' that is not the first-parent winner for commit '{}' and identity {:?}",
                    row.change_id, root_commit_id, identity
                )));
            };
            let parent_value = self
                .load_cached_tree_value(&parent.root_id, &key, cache)
                .await?;
            if parent_value.as_ref() != Some(&row_value) {
                return Err(LixError::unknown(format!(
                    "tracked-state projection row for commit '{}' does not match parent root '{}' for inherited identity {:?}",
                    root_commit_id, parent.commit_id, identity
                )));
            }
            current_commit_id = parent.commit_id.clone();
        }
    }

    async fn validate_projection_parent_matches_changelog(
        &mut self,
        commit_id: &str,
        metadata: &TrackedStateProjectionMetadata,
        cache: &mut DiffProjectionValidationCache,
    ) -> Result<(), LixError> {
        let changelog_first_parent = self
            .load_cached_visible_changelog_first_parent(commit_id, cache)
            .await?;
        let expected_parent = match changelog_first_parent.as_deref() {
            Some(first_parent_id) => {
                self.nearest_available_projection_parent(first_parent_id, cache)
                    .await?
            }
            None => None,
        };
        match (expected_parent, metadata.parent_roots.first()) {
            (None, None) => Ok(()),
            (Some((expected_parent_id, expected_root)), Some(parent))
                if parent.commit_id == expected_parent_id && parent.root_id == expected_root =>
            {
                Ok(())
            }
            (Some((expected_parent_id, expected_root)), Some(parent))
                if parent.commit_id == expected_parent_id =>
            {
                let _ = expected_root;
                Err(LixError::unknown(format!(
                    "tracked-state projection metadata for commit '{}' references stale root for projection parent '{}'",
                    commit_id, expected_parent_id
                )))
            }
            (Some((expected_parent_id, _)), Some(parent)) => Err(LixError::unknown(format!(
                "tracked-state projection metadata for commit '{}' references parent '{}' but nearest available first-parent root is '{}'",
                commit_id, parent.commit_id, expected_parent_id
            ))),
            (Some((expected_parent_id, _)), None) => Err(LixError::unknown(format!(
                "tracked-state projection metadata for commit '{}' is missing projection parent '{}'",
                commit_id, expected_parent_id
            ))),
            (None, Some(parent)) => Err(LixError::unknown(format!(
                "tracked-state projection metadata for root commit '{}' references unexpected parent '{}'",
                commit_id, parent.commit_id
            ))),
        }
    }

    async fn nearest_available_projection_parent(
        &mut self,
        start_commit_id: &str,
        cache: &mut DiffProjectionValidationCache,
    ) -> Result<Option<(String, TrackedStateRootId)>, LixError> {
        let mut current = Some(start_commit_id.to_string());
        let mut seen = HashSet::new();
        while let Some(commit_id) = current {
            if !seen.insert(commit_id.clone()) {
                return Err(LixError::unknown(format!(
                    "tracked-state projection parent chain contains cycle at commit '{commit_id}'"
                )));
            }
            if let Some(root_id) = self
                .load_cached_projection_root_optional(&commit_id, cache)
                .await?
            {
                return Ok(Some((commit_id, root_id)));
            }
            current = self
                .load_cached_visible_changelog_first_parent(&commit_id, cache)
                .await?;
        }
        Ok(None)
    }

    #[allow(dead_code)]
    async fn load_cached_visible_commit_winners(
        &mut self,
        commit_id: &str,
        cache: &mut DiffProjectionValidationCache,
    ) -> Result<HashMap<StateRowIdentity, String>, LixError> {
        if let Some(winners) = cache.visible_commit_winners.get(commit_id) {
            return Ok(winners.clone());
        }
        let identities = cache
            .requested_identities
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let mut changelog_reader = ChangelogContext::new().reader(&mut self.store);
        let winners = changelog_reader
            .load_visible_commit_winners(commit_id, &identities)
            .await?;
        cache
            .visible_commit_winners
            .insert(commit_id.to_string(), winners.clone());
        Ok(winners)
    }

    async fn load_cached_projection_metadata(
        &mut self,
        commit_id: &str,
        cache: &mut DiffProjectionValidationCache,
    ) -> Result<TrackedStateProjectionMetadata, LixError> {
        self.load_cached_projection_metadata_optional(commit_id, cache)
            .await?
            .ok_or_else(|| missing_projection_root_error(commit_id))
    }

    async fn load_cached_projection_metadata_optional(
        &mut self,
        commit_id: &str,
        cache: &mut DiffProjectionValidationCache,
    ) -> Result<Option<TrackedStateProjectionMetadata>, LixError> {
        if let Some(metadata) = cache.projection_metadata.get(commit_id) {
            return Ok(Some(metadata.clone()));
        }
        let Some(metadata) = storage::load_projection_metadata(&mut self.store, commit_id).await?
        else {
            return Ok(None);
        };
        cache
            .projection_metadata
            .insert(commit_id.to_string(), metadata.clone());
        Ok(Some(metadata))
    }

    async fn load_cached_projection_root_optional(
        &mut self,
        commit_id: &str,
        cache: &mut DiffProjectionValidationCache,
    ) -> Result<Option<TrackedStateRootId>, LixError> {
        if let Some(root_id) = cache.projection_roots.get(commit_id) {
            return Ok(Some(root_id.clone()));
        }
        let root_id = storage::load_root(&self.store, commit_id).await?;
        if let Some(root_id) = &root_id {
            cache
                .projection_roots
                .insert(commit_id.to_string(), root_id.clone());
        }
        Ok(root_id)
    }

    #[allow(dead_code)]
    async fn load_cached_tree_value(
        &mut self,
        root_id: &TrackedStateRootId,
        key: &TrackedStateKey,
        cache: &mut DiffProjectionValidationCache,
    ) -> Result<Option<TrackedStateIndexValue>, LixError> {
        let cache_key = (root_id.clone(), key.clone());
        if let Some(value) = cache.tree_values.get(&cache_key) {
            return Ok(value.clone());
        }
        let value = self
            .tree
            .get_many(&mut self.store, root_id, std::slice::from_ref(key))
            .await?
            .into_iter()
            .next()
            .flatten();
        cache.tree_values.insert(cache_key, value.clone());
        Ok(value)
    }

    async fn load_cached_visible_changelog_first_parent(
        &mut self,
        commit_id: &str,
        cache: &mut DiffProjectionValidationCache,
    ) -> Result<Option<String>, LixError> {
        if let Some(parent_id) = cache.changelog_first_parents.get(commit_id) {
            return Ok(parent_id.clone());
        }
        let commit_ids = [commit_id.to_string()];
        let mut changelog_reader = ChangelogContext::new().reader(&mut self.store);
        let batch = changelog_reader
            .load_commits(CommitLoadRequest {
                commit_ids: &commit_ids,
                projection: CommitProjection::Header,
                visibility: CommitVisibilityMode::RequireVisible,
            })
            .await?;
        let Some(entry) = batch.entries.into_iter().next().flatten() else {
            return Err(LixError::unknown(format!(
                "visible changelog commit '{commit_id}' is missing while validating tracked-state projection metadata"
            )));
        };
        let CommitLoadEntry::Header(header) = entry else {
            return Err(LixError::unknown(format!(
                "visible changelog commit '{commit_id}' did not return a header projection"
            )));
        };
        let parent_id = header.parent_commit_ids.first().cloned();
        cache
            .changelog_first_parents
            .insert(commit_id.to_string(), parent_id.clone());
        Ok(parent_id)
    }

    async fn validate_diff_row_created_at(
        &mut self,
        row: &TrackedStateDiffRow,
        key: &TrackedStateKey,
        commit_id: &str,
        change_created_at: &str,
        cache: &mut DiffProjectionValidationCache,
    ) -> Result<(), LixError> {
        if row.created_at == change_created_at {
            return Ok(());
        }
        let metadata = self
            .load_cached_projection_metadata(commit_id, cache)
            .await?;
        self.validate_projection_parent_matches_changelog(commit_id, &metadata, cache)
            .await?;
        let Some(parent) = metadata.parent_roots.first() else {
            return Err(LixError::unknown(format!(
                "tracked-state diff row for change '{}' created_at '{}' does not match changelog change '{}'",
                row.change_id, row.created_at, change_created_at
            )));
        };
        let parent_value = self
            .load_cached_tree_value(&parent.root_id, key, cache)
            .await?;
        if parent_value
            .as_ref()
            .is_some_and(|value| value.created_at == row.created_at)
        {
            return Ok(());
        }
        if row.commit_id != commit_id {
            if let Some(source_created_at) =
                self.load_parent_created_at_for_row_commit(row, key, cache)
                    .await?
            {
                if source_created_at == row.created_at {
                    return Ok(());
                }
            }
        }
        Err(LixError::unknown(format!(
            "tracked-state diff row for change '{}' created_at '{}' does not match changelog change '{}' or parent projection",
            row.change_id, row.created_at, change_created_at
        )))
    }

    async fn load_parent_created_at_for_row_commit(
        &mut self,
        row: &TrackedStateDiffRow,
        key: &TrackedStateKey,
        cache: &mut DiffProjectionValidationCache,
    ) -> Result<Option<String>, LixError> {
        let Some(metadata) = self
            .load_cached_projection_metadata_optional(&row.commit_id, cache)
            .await?
        else {
            return Ok(None);
        };
        self.validate_projection_parent_matches_changelog(&row.commit_id, &metadata, cache)
            .await?;
        let Some(parent) = metadata.parent_roots.first() else {
            return Ok(None);
        };
        let parent_value = self
            .load_cached_tree_value(&parent.root_id, key, cache)
            .await?;
        Ok(parent_value.map(|value| value.created_at))
    }

    pub(crate) async fn validate_tree_rows_at_commit_against_changelog(
        &mut self,
        commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<(), LixError> {
        let root = self.load_ensured_root(commit_id).await?;
        let rows = self.tree.scan(&mut self.store, &root, request).await?;
        let rows = rows
            .into_iter()
            .map(|(key, value)| TrackedStateDiffRow::from_tree_entry(key, value))
            .collect::<Vec<_>>();
        let mut validation_cache = DiffProjectionValidationCache::new(HashSet::new());
        let metadata = self
            .load_cached_projection_metadata(commit_id, &mut validation_cache)
            .await?;
        self.validate_projection_parent_matches_changelog(
            commit_id,
            &metadata,
            &mut validation_cache,
        )
        .await?;
        let mut changelog_reader = ChangelogContext::new().reader(&mut self.store);
        let winner_facts = changelog_reader
            .load_first_parent_winner_facts_matching_visible_commit(commit_id, |identity| {
                state_row_identity_matches_tree_request(identity, request)
            })
            .await?;
        let mut expected_identities = HashSet::new();
        for (identity, fact) in &winner_facts {
            if !fact.deleted || request.include_tombstones {
                expected_identities.insert(identity.clone());
            }
        }
        let mut actual_identities = HashSet::new();
        for row in &rows {
            let identity = state_row_identity_from_diff_row(row)?;
            let fact = winner_facts.get(&identity).ok_or_else(|| {
                LixError::unknown(format!(
                    "tracked-state projection root for commit '{commit_id}' contains non-winner identity {:?}",
                    identity
                ))
            })?;
            if fact.change_id != row.change_id {
                return Err(LixError::unknown(format!(
                    "tracked-state projection root for commit '{}' has change '{}' but changelog first-parent winner is '{}'",
                    commit_id, row.change_id, fact.change_id
                )));
            }
            if fact.created_at != row.created_at && fact.updated_at != row.created_at {
                let (key, _) = row.clone().into_index_entry();
                self.validate_diff_row_created_at(
                    row,
                    &key,
                    &fact.commit_id,
                    &fact.created_at,
                    &mut validation_cache,
                )
                .await
                    .map_err(|_| {
                        LixError::unknown(format!(
                            "tracked-state projection root for commit '{}' has created_at '{}' for change '{}' but changelog first-parent created_at is '{}'",
                            commit_id, row.created_at, row.change_id, fact.created_at
                        ))
                    })?;
            }
            if fact.updated_at != row.updated_at {
                return Err(LixError::unknown(format!(
                    "tracked-state projection root for commit '{}' has updated_at '{}' for change '{}' but changelog first-parent updated_at is '{}'",
                    commit_id, row.updated_at, row.change_id, fact.updated_at
                )));
            }
            actual_identities.insert(identity);
        }
        if actual_identities != expected_identities {
            return Err(LixError::unknown(format!(
                "tracked-state projection root for commit '{commit_id}' does not match changelog first-parent winners"
            )));
        }
        let row_refs = rows.iter().collect::<Vec<_>>();
        self.validate_diff_rows_physical_against_changelog(&row_refs)
            .await
    }

    pub(crate) async fn diff_tree_entries_at_commits(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<Vec<crate::tracked_state::types::TrackedStateTreeDiffEntry>, LixError> {
        let left_root = self.load_ensured_root(left_commit_id).await?;
        let right_root = self.load_ensured_root(right_commit_id).await?;
        self.tree
            .diff(
                &mut self.store,
                Some(&left_root),
                Some(&right_root),
                request,
            )
            .await
    }

    async fn scan_rows_at_commit_by_file_index(
        &mut self,
        primary_root_id: &crate::tracked_state::types::TrackedStateRootId,
        by_file_root_id: &crate::tracked_state::types::TrackedStateRootId,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
        if request.filter.schema_keys.is_empty() {
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

        let by_file_request = ByFileIndex::scan_request_from_tracked(request);
        let index_match_count = self
            .tree
            .count_matching_keys(&mut self.store, by_file_root_id, &by_file_request)
            .await?;
        let primary_row_count = self
            .tree
            .row_count(&mut self.store, primary_root_id)
            .await?;
        if index_match_count > primary_row_count / 20 {
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
        let primary_rows = self
            .tree
            .scan(
                &mut self.store,
                primary_root_id,
                &tree_scan_request_from_tracked(request),
            )
            .await?;
        let index_rows = self
            .tree
            .scan(&mut self.store, by_file_root_id, &by_file_request)
            .await?;
        if primary_rows.len() != index_rows.len() {
            return Err(LixError::unknown(format!(
                "tracked-state by-file index row count {} does not match primary projection row count {}",
                index_rows.len(),
                primary_rows.len()
            )));
        }

        let mut primary_by_key =
            HashMap::<TrackedStateKey, TrackedStateIndexValue>::with_capacity(primary_rows.len());
        for (key, value) in primary_rows {
            primary_by_key.insert(key, value);
        }
        let mut rows = Vec::new();
        for (index_key, index_value) in index_rows {
            let Some(primary_key) = ByFileIndex::primary_key_from_index_key(index_key) else {
                return Err(LixError::unknown(format!(
                    "tracked-state by-file index contains malformed primary key mapping"
                )));
            };
            let Some(value) = primary_by_key.remove(&primary_key) else {
                return Err(LixError::unknown(format!(
                    "tracked-state by-file index references row {:?} outside primary projection",
                    primary_key
                )));
            };
            validate_by_file_index_value_matches_primary(&primary_key, &index_value, &value)?;
            rows.push((primary_key, value));
        }
        if !primary_by_key.is_empty() {
            return Err(LixError::unknown(
                "tracked-state by-file index is missing primary projection rows",
            ));
        }
        Ok(rows)
    }

    async fn load_ensured_root(
        &mut self,
        commit_id: &str,
    ) -> Result<crate::tracked_state::types::TrackedStateRootId, LixError> {
        self.tree
            .load_root(&mut self.store, commit_id)
            .await?
            .ok_or_else(|| missing_projection_root_error(commit_id))
    }

    #[cfg(any(test, feature = "storage-benches"))]
    async fn projection_values_at_commit_for_keys_allow_rebuild(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKey],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        if let Some(root_id) = self.tree.load_root(&mut self.store, commit_id).await? {
            return self.tree.get_many(&mut self.store, &root_id, keys).await;
        }
        let input =
            crate::tracked_state::projection_root_rebuild::build_incremental_projection_root_rebuild_input(
                &self.store,
                commit_id,
            )
            .await?;
        let mut values = keys
            .iter()
            .cloned()
            .map(|key| (key, None))
            .collect::<BTreeMap<_, Option<TrackedStateIndexValue>>>();
        for delta in input.deltas {
            let key = TrackedStateKey {
                schema_key: delta.change.schema_key,
                file_id: delta.change.file_id,
                entity_id: delta.change.entity_id,
            };
            if values.contains_key(&key) {
                values.insert(
                    key,
                    Some(TrackedStateIndexValue {
                        change_locator: delta.locator,
                        deleted: delta.change.snapshot_ref.is_none(),
                        snapshot_ref: delta.change.snapshot_ref,
                        metadata_ref: delta.change.metadata_ref,
                        created_at: delta.created_at,
                        updated_at: delta.updated_at,
                    }),
                );
            }
        }
        Ok(keys
            .iter()
            .map(|key| values.get(key).cloned().unwrap_or(None))
            .collect())
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

/// Writer for changelog-backed tracked-state projection roots.
pub(crate) struct TrackedStateWriter<'a, S: ?Sized> {
    chunk_overlay: storage::TrackedStateChunkOverlay,
    staged_by_file_roots: BTreeMap<String, crate::tracked_state::types::TrackedStateRootId>,
    staged_roots: BTreeMap<String, crate::tracked_state::types::TrackedStateRootId>,
    tree: TrackedStateTree,
    store: &'a S,
    writes: &'a mut StorageWriteSet,
}

/// Explicit projection-root rebuilder created by `TrackedStateContext`.
pub(crate) struct TrackedStateRootRebuilder<'a, S: ?Sized> {
    pub(super) tracked_state: &'a TrackedStateContext,
    pub(super) store: &'a S,
    pub(super) writes: &'a mut StorageWriteSet,
}

impl<S> TrackedStateRootRebuilder<'_, S>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    pub(crate) async fn ensure_projection_root(
        &mut self,
        commit_id: &str,
    ) -> Result<crate::tracked_state::projection_root_rebuild::ProjectionRootEnsureReport, LixError>
    {
        crate::tracked_state::projection_root_rebuild::ensure_projection_root(self, commit_id).await
    }

    pub(crate) async fn rebuild_projection_root_at(
        &mut self,
        commit_id: &str,
    ) -> Result<TrackedStateWriteReport, LixError> {
        crate::tracked_state::projection_root_rebuild::rebuild_projection_root_at(self, commit_id)
            .await
    }
}

impl<S> TrackedStateWriter<'_, S>
where
    S: StorageRead + Send + Sync + ?Sized,
{
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
                let root = match self.staged_roots.get(parent_commit_id) {
                    Some(root) => Some(root.clone()),
                    None => self.tree.load_root(self.store, parent_commit_id).await?,
                };
                let Some(root) = root else {
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
            .apply_mutations_with_overlay(
                self.store,
                self.writes,
                &mut self.chunk_overlay,
                base_root.as_ref(),
                mutations,
                Some(commit_id),
            )
            .await?;
        self.staged_roots
            .insert(commit_id.to_string(), result.root_id.clone());
        storage::stage_projection_metadata(
            self.writes,
            &TrackedStateProjectionMetadata {
                commit_id: commit_id.to_string(),
                root_id: result.root_id.clone(),
                parent_roots: parent_commit_id
                    .zip(base_root.as_ref())
                    .map(|(parent_commit_id, root_id)| {
                        vec![TrackedStateProjectionParent {
                            commit_id: parent_commit_id.to_string(),
                            root_id: root_id.clone(),
                        }]
                    })
                    .unwrap_or_default(),
                changed_key_count: u64::try_from(deltas.len()).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state projection changed key count exceeds u64",
                    )
                })?,
                row_count_estimate: u64::try_from(result.row_count).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state projection row count exceeds u64",
                    )
                })?,
                tree_height: u32::try_from(result.tree_height).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state projection tree height exceeds u32",
                    )
                })?,
                primary_chunk_count: u64::try_from(result.chunk_count).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state projection chunk count exceeds u64",
                    )
                })?,
                primary_chunk_bytes: u64::try_from(result.chunk_bytes).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state projection chunk bytes exceeds u64",
                    )
                })?,
            },
        )?;

        let by_file_base_root = match parent_commit_id {
            Some(parent_commit_id) => match self.staged_by_file_roots.get(parent_commit_id) {
                Some(root) => Some(root.clone()),
                None => storage::load_by_file_root(self.store, parent_commit_id).await?,
            },
            None => None,
        };
        let concrete_file_deltas = deltas
            .iter()
            .filter(|delta| delta.change.file_id.is_some())
            .collect::<Vec<_>>();
        let by_file_chunk_puts = if concrete_file_deltas.is_empty() {
            if let Some(by_file_base_root) = by_file_base_root.as_ref() {
                storage::stage_by_file_root(self.writes, commit_id, by_file_base_root);
                self.staged_by_file_roots
                    .insert(commit_id.to_string(), by_file_base_root.clone());
            }
            0
        } else if parent_commit_id.is_some() && by_file_base_root.is_none() {
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
                .apply_mutations_with_overlay(
                    self.store,
                    self.writes,
                    &mut self.chunk_overlay,
                    by_file_base_root.as_ref(),
                    by_file_mutations,
                    None,
                )
                .await?;
            storage::stage_by_file_root(self.writes, commit_id, &by_file_result.root_id);
            self.staged_by_file_roots
                .insert(commit_id.to_string(), by_file_result.root_id.clone());
            by_file_result.chunk_count
        };
        Ok(TrackedStateWriteReport {
            commit_id: commit_id.to_string(),
            root_id: result.root_id,
            changed_rows: deltas.len(),
            primary_chunk_puts: result.chunk_count,
            by_file_chunk_puts,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateWriteReport {
    pub(crate) commit_id: String,
    pub(crate) root_id: TrackedStateRootId,
    pub(crate) changed_rows: usize,
    pub(crate) primary_chunk_puts: usize,
    pub(crate) by_file_chunk_puts: usize,
}

fn missing_projection_root_error(commit_id: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "tracked_state projection root is missing for commit '{commit_id}'; call ensure_projection_root before structural diff"
        ),
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

fn validate_by_file_index_value_matches_primary(
    key: &TrackedStateKey,
    index_value: &TrackedStateIndexValue,
    primary_value: &TrackedStateIndexValue,
) -> Result<(), LixError> {
    if index_value.snapshot_ref.is_some() || index_value.metadata_ref.is_some() {
        return Err(LixError::unknown(format!(
            "tracked-state by-file index value contains payload refs for {:?}",
            key
        )));
    }
    if index_value.change_locator != primary_value.change_locator
        || index_value.deleted != primary_value.deleted
        || index_value.created_at != primary_value.created_at
        || index_value.updated_at != primary_value.updated_at
    {
        return Err(LixError::unknown(format!(
            "tracked-state by-file index value does not match primary projection for {:?}",
            key
        )));
    }
    Ok(())
}

fn validate_diff_row_against_changelog(
    row: &TrackedStateDiffRow,
    changes: &HashMap<
        String,
        (
            crate::changelog::SegmentObjectLocation,
            crate::changelog::SegmentChange,
        ),
    >,
) -> Result<(), LixError> {
    let Some((location, change)) = changes.get(&row.change_id) else {
        return Err(LixError::unknown(format!(
            "tracked-state diff row references missing changelog change '{}'",
            row.change_id
        )));
    };
    if row.change_location != *location {
        return Err(LixError::unknown(format!(
            "tracked-state diff row for change '{}' has stale changelog locator",
            row.change_id
        )));
    }
    if change.authored_commit_id.as_deref() != Some(row.commit_id.as_str()) {
        return Err(LixError::unknown(format!(
            "tracked-state diff row for change '{}' has commit_id '{}' but changelog authored_commit_id is {:?}",
            row.change_id, row.commit_id, change.authored_commit_id
        )));
    }
    if change.schema_key != row.schema_key
        || change.file_id != row.file_id
        || change.entity_id != row.entity_id
    {
        return Err(LixError::unknown(format!(
            "tracked-state diff row for change '{}' does not match changelog change identity",
            row.change_id
        )));
    }
    if row.deleted != change.snapshot_ref.is_none() {
        return Err(LixError::unknown(format!(
            "tracked-state diff row for change '{}' deleted flag does not match changelog snapshot",
            row.change_id
        )));
    }
    if row.snapshot_ref != change.snapshot_ref || row.metadata_ref != change.metadata_ref {
        return Err(LixError::unknown(format!(
            "tracked-state diff row for change '{}' payload refs do not match changelog change",
            row.change_id
        )));
    }
    if row.updated_at != change.created_at {
        return Err(LixError::unknown(format!(
            "tracked-state diff row for change '{}' updated_at does not match changelog change timestamp",
            row.change_id
        )));
    }
    Ok(())
}

fn state_row_identity_from_diff_row(
    row: &TrackedStateDiffRow,
) -> Result<StateRowIdentity, LixError> {
    Ok(StateRowIdentity {
        schema_key: CanonicalSchemaKey::new(row.schema_key.clone())?,
        file_id: FileId::new(
            row.file_id
                .clone()
                .unwrap_or_else(|| "__global__".to_string()),
        )?,
        entity_id: EntityId::new(row.entity_id.as_json_array_text()?)?,
    })
}

fn state_row_identity_matches_tree_request(
    identity: &StateRowIdentity,
    request: &TrackedStateTreeScanRequest,
) -> Result<bool, LixError> {
    if !request.schema_keys.is_empty()
        && !request
            .schema_keys
            .iter()
            .any(|schema_key| schema_key == identity.schema_key.as_str())
    {
        return Ok(false);
    }
    if !request.file_ids.is_empty()
        && !request.file_ids.iter().any(|filter| match filter {
            crate::NullableKeyFilter::Null => identity.file_id.as_str() == "__global__",
            crate::NullableKeyFilter::Value(value) => identity.file_id.as_str() == value,
            crate::NullableKeyFilter::Any => true,
        })
    {
        return Ok(false);
    }
    if !request.entity_ids.is_empty() {
        let entity_id =
            EntityIdentity::from_json_array_text(identity.entity_id.as_str()).map_err(|error| {
                LixError::unknown(format!(
                    "tracked-state changelog winner identity contains invalid entity_id: {error}"
                ))
            })?;
        if !request
            .entity_ids
            .iter()
            .any(|requested| requested == &entity_id)
        {
            return Ok(false);
        }
    }
    Ok(true)
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
    use crate::tracked_state::codec::{
        ChildSummary, PendingChunkWrite, encode_internal_node, hash_bytes,
    };
    use crate::tracked_state::types::TRACKED_STATE_HASH_BYTES;
    use crate::NullableKeyFilter;

    #[tokio::test]
    async fn stage_projection_root_requires_parent_projection_root() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .expect("parent read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_empty_changelog_commit(
                &mut read,
                &mut writes,
                "missing-parent",
                None,
            )
            .await
            .expect("parent changelog commit should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("parent changelog commit should commit");
        }

        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-child",
            Some("missing-parent"),
            &[row("entity-child", "change-child", "commit-child")],
        )
        .await
        .expect_err("root staging should require a parent projection root");
    }

    #[tokio::test]
    async fn stage_projection_root_writes_projection_metadata() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row("entity-a", "change-parent", "parent")],
        )
        .await
        .expect("parent root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[
                row("entity-a", "change-child-a", "child"),
                row("entity-b", "change-child-b", "child"),
            ],
        )
        .await
        .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let parent_root = storage::load_root(&read, "parent")
            .await
            .expect("parent root should load")
            .expect("parent root should exist");
        let child_root = storage::load_root(&read, "child")
            .await
            .expect("child root should load")
            .expect("child root should exist");
        let metadata = storage::load_projection_metadata(&read, "child")
            .await
            .expect("metadata should load")
            .expect("metadata should exist");

        assert_eq!(metadata.commit_id, "child");
        assert_eq!(metadata.root_id, child_root);
        assert_eq!(metadata.parent_roots.len(), 1);
        assert_eq!(metadata.parent_roots[0].commit_id, "parent");
        assert_eq!(metadata.parent_roots[0].root_id, parent_root);
        assert_eq!(metadata.changed_key_count, 2);
        assert_eq!(metadata.row_count_estimate, 2);
        assert!(metadata.tree_height >= 1);
        assert!(metadata.primary_chunk_count >= 1);
        assert!(metadata.primary_chunk_bytes > 0);
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
        assert!(plan.patches[0].projected_row().deleted);
        assert_eq!(plan.patches[0].change_id(), "change-source-delete");
    }

    #[tokio::test]
    async fn ensure_projection_root_repairs_missing_child_root_from_nearest_parent() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            &[row_with_value("entity-a", "change-base", "base", "base")],
        )
        .await
        .expect("base root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("base"),
            &[row_with_value("entity-a", "change-child", "child", "child")],
        )
        .await
        .expect("child root should write");
        {
            let mut writes = storage.new_write_set();
            writes.delete(
                storage::TRACKED_STATE_PROJECTION_SPACE,
                crate::storage::StorageKey(bytes::Bytes::copy_from_slice(b"child")),
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("child projection delete should commit");
        }

        tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &TrackedStateDiffRequest::default())
            .await
            .expect_err("diff should require durable roots before repair");

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let report = tracked_state
            .root_rebuilder(&mut read, &mut writes)
            .ensure_projection_root("child")
            .await
            .expect("child root should repair");
        assert!(report.repaired);
        assert_eq!(report.parent_commit_id.as_deref(), Some("base"));
        assert_eq!(report.replayed_commits, 1);
        assert_eq!(report.replayed_changes, 1);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("repaired root should commit");

        let diff = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("diff should use repaired root");

        assert_eq!(diff.entries.len(), 1);
        assert_eq!(
            diff.entries[0].kind,
            crate::tracked_state::TrackedStateDiffKind::Modified
        );
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .map(|row| row.change_id.as_str()),
            Some("change-child")
        );
    }

    #[tokio::test]
    async fn diff_allows_repaired_root_parented_to_nearest_available_ancestor_root() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            &[row_with_value("entity-a", "change-base", "base", "base")],
        )
        .await
        .expect("base root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "middle",
            Some("base"),
            &[row_with_value(
                "entity-a",
                "change-middle",
                "middle",
                "middle",
            )],
        )
        .await
        .expect("middle root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("middle"),
            &[row_with_value("entity-a", "change-child", "child", "child")],
        )
        .await
        .expect("child root should write");
        {
            let mut writes = storage.new_write_set();
            for commit_id in ["middle", "child"] {
                writes.delete(
                    storage::TRACKED_STATE_PROJECTION_SPACE,
                    crate::storage::StorageKey(bytes::Bytes::copy_from_slice(commit_id.as_bytes())),
                );
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("projection deletes should commit");
        }

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let report = tracked_state
            .root_rebuilder(&mut read, &mut writes)
            .ensure_projection_root("child")
            .await
            .expect("child root should repair");
        assert!(report.repaired);
        assert_eq!(report.parent_commit_id.as_deref(), Some("base"));
        assert_eq!(report.replayed_commits, 2);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("repaired root should commit");

        let diff = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("diff should accept repaired nearest-ancestor parent metadata");

        assert_eq!(diff.entries.len(), 1);
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .map(|row| row.change_id.as_str()),
            Some("change-child")
        );
    }

    #[tokio::test]
    async fn scan_rows_by_file_uses_file_index_shape() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut file_a = row("entity-a", "change-a", "commit-1");
        file_a.file_id = Some("file-a.json".to_string());
        let mut file_b = row("entity-b", "change-b", "commit-1");
        file_b.file_id = Some("file-b.json".to_string());
        let mut rows = vec![file_a, file_b];
        for index in 0..25 {
            let mut row = row(
                &format!("entity-padding-{index}"),
                &format!("change-padding-{index}"),
                "commit-1",
            );
            row.file_id = Some("file-b.json".to_string());
            rows.push(row);
        }
        write_root_for_test(&storage, &tracked_state, "commit-1", None, &rows)
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
                        schema_keys: vec!["test_schema".to_string()],
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
    async fn file_only_by_file_scan_falls_back_to_primary_even_with_existing_by_file_root() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut primary_row = row("entity-a", "change-a", "commit-1");
        primary_row.file_id = Some("file-a.json".to_string());
        let mut other_row = row("entity-b", "change-b", "commit-1");
        other_row.file_id = Some("file-b.json".to_string());
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            &[primary_row.clone(), other_row],
        )
        .await
        .expect("root should write");

        let key = TrackedStateKey {
            schema_key: primary_row.schema_key.clone(),
            file_id: primary_row.file_id.clone(),
            entity_id: primary_row.entity_id.clone(),
        };
        let primary_root = storage::load_root(
            &storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
            "commit-1",
        )
        .await
        .expect("primary root should load")
        .expect("primary root should exist");
        let index_value = TrackedStateTree::new()
            .get(
                &storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
                &primary_root,
                &key,
            )
            .await
            .expect("primary value should load")
            .expect("primary value should exist");
        assert!(
            index_value.snapshot_ref.is_some(),
            "test setup needs a payload ref to prove the by-file root is bypassed"
        );
        write_by_file_entries_for_test(&storage, "commit-1", &[(key, index_value)])
            .await
            .expect("payload-bearing by-file root should write");

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
            .expect("file-only scan should use the primary fallback");

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
    async fn by_file_header_index_fetches_primary_payload_only_when_requested() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut primary_row = row("entity-a", "change-a", "commit-1");
        primary_row.file_id = Some("file-a.json".to_string());
        let expected_snapshot = primary_row.snapshot_content.clone();
        let mut rows = vec![primary_row];
        for index in 0..25 {
            let mut row = row(
                &format!("entity-padding-{index}"),
                &format!("change-padding-{index}"),
                "commit-1",
            );
            row.file_id = Some("file-b.json".to_string());
            rows.push(row);
        }
        write_root_for_test(&storage, &tracked_state, "commit-1", None, &rows)
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
                        schema_keys: vec!["test_schema".to_string()],
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
                        schema_keys: vec!["test_schema".to_string()],
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
    async fn by_file_header_index_rejects_value_corruption_against_primary_root() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut primary_row = row("entity-a", "change-a", "commit-1");
        primary_row.file_id = Some("file-a.json".to_string());
        let mut corrupt_index_row = row("entity-a", "change-bad", "commit-bad");
        corrupt_index_row.file_id = Some("file-a.json".to_string());
        let mut primary_rows = vec![primary_row.clone()];
        for index in 0..25 {
            let mut row = row(
                &format!("entity-padding-{index}"),
                &format!("change-padding-{index}"),
                "commit-1",
            );
            row.file_id = Some("file-b.json".to_string());
            primary_rows.push(row);
        }
        write_root_for_test(&storage, &tracked_state, "commit-1", None, &primary_rows)
            .await
            .expect("primary root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-bad",
            None,
            std::slice::from_ref(&corrupt_index_row),
        )
        .await
        .expect("corrupt by-file donor root should write");

        let corrupt_by_file_root = storage::load_by_file_root(
            &storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
            "commit-bad",
        )
        .await
        .expect("corrupt by-file root should load")
        .expect("corrupt by-file root should exist");
        let mut writes = storage.new_write_set();
        storage::stage_by_file_root(&mut writes, "commit-1", &corrupt_by_file_root);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("corrupt by-file root should commit");

        let error = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_string()],
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
            .expect_err("by-file value corruption should be rejected");

        assert!(
            error
                .to_string()
                .contains("by-file index value does not match primary projection"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn by_file_header_index_rejects_missing_primary_projection_rows() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut indexed_row = row("entity-indexed", "change-indexed", "commit-1");
        indexed_row.file_id = Some("file-a.json".to_string());
        let mut omitted_row = row("entity-omitted", "change-omitted", "commit-1");
        omitted_row.file_id = Some("file-a.json".to_string());
        let mut primary_rows = vec![indexed_row.clone(), omitted_row];
        for index in 0..25 {
            let mut row = row(
                &format!("entity-padding-{index}"),
                &format!("change-padding-{index}"),
                "commit-1",
            );
            row.file_id = Some("file-b.json".to_string());
            primary_rows.push(row);
        }
        write_root_for_test(&storage, &tracked_state, "commit-1", None, &primary_rows)
            .await
            .expect("primary root should write");
        write_by_file_root_for_test(&storage, "commit-1", &[indexed_row])
            .await
            .expect("partial by-file root should write");

        let error = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_string()],
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
            .expect_err("by-file omission should be rejected");

        assert!(
            error.to_string().contains(
                "by-file index row count 1 does not match primary projection row count 2"
            ),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn by_file_scan_fallback_heuristic_handles_corrupt_large_index_count() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut primary_row = row("entity-a", "change-a", "commit-1");
        primary_row.file_id = Some("file-a.json".to_string());
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&primary_row),
        )
        .await
        .expect("primary root should write");

        let encoded_index_key = ByFileIndex::encode_key_ref(TrackedStateKeyRef {
            schema_key: &primary_row.schema_key,
            file_id: primary_row.file_id.as_deref(),
            entity_id: &primary_row.entity_id,
        });
        stage_corrupt_by_file_internal_root(
            &storage,
            "commit-1",
            ChildSummary {
                first_key: encoded_index_key.clone(),
                last_key: encoded_index_key,
                child_hash: [7; TRACKED_STATE_HASH_BYTES],
                subtree_count: u64::MAX,
            },
        );

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
                        schema_keys: vec!["test_schema".to_string()],
                        file_ids: vec![NullableKeyFilter::Value("file-a.json".to_string())],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("large by-file count should choose primary fallback without overflow");

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
    async fn by_file_header_index_rejects_payload_refs_in_index_values() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut primary_row = row("entity-a", "change-a", "commit-1");
        primary_row.file_id = Some("file-a.json".to_string());
        let mut primary_rows = vec![primary_row.clone()];
        for index in 0..25 {
            let mut row = row(
                &format!("entity-padding-{index}"),
                &format!("change-padding-{index}"),
                "commit-1",
            );
            row.file_id = Some("file-b.json".to_string());
            primary_rows.push(row);
        }
        write_root_for_test(&storage, &tracked_state, "commit-1", None, &primary_rows)
            .await
            .expect("primary root should write");

        let key = TrackedStateKey {
            schema_key: primary_row.schema_key.clone(),
            file_id: primary_row.file_id.clone(),
            entity_id: primary_row.entity_id.clone(),
        };
        let primary_root = storage::load_root(
            &storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
            "commit-1",
        )
        .await
        .expect("primary root should load")
        .expect("primary root should exist");
        let index_value = TrackedStateTree::new()
            .get(
                &storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
                &primary_root,
                &key,
            )
            .await
            .expect("primary value should load")
            .expect("primary value should exist");
        assert!(
            index_value.snapshot_ref.is_some(),
            "test setup needs a payload ref to forge into the by-file index"
        );
        write_by_file_entries_for_test(&storage, "commit-1", &[(key, index_value)])
            .await
            .expect("payload-bearing by-file root should write");

        let error = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_string()],
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
            .expect_err("payload-bearing by-file value should be rejected");

        assert!(
            error
                .to_string()
                .contains("by-file index value contains payload refs"),
            "unexpected error: {error}"
        );
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
        let mut rows = vec![live, deleted];
        for index in 0..25 {
            let mut row = row(
                &format!("entity-padding-{index}"),
                &format!("change-padding-{index}"),
                "commit-1",
            );
            row.file_id = Some("file-b.json".to_string());
            rows.push(row);
        }
        write_root_for_test(&storage, &tracked_state, "commit-1", None, &rows)
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
                        schema_keys: vec!["test_schema".to_string()],
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
    async fn child_does_not_stage_partial_by_file_index_without_parent_by_file_root() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut base = row("entity-base", "change-base", "base");
        base.file_id = Some("file-a.json".to_string());
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            std::slice::from_ref(&base),
        )
        .await
        .expect("base root should write");
        {
            let mut writes = storage.new_write_set();
            writes.delete(
                storage::TRACKED_STATE_BY_FILE_ROOT_SPACE,
                crate::storage::StorageKey(bytes::Bytes::copy_from_slice(b"base")),
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("base root should commit");
        }

        let mut child = row("entity-child", "change-child", "child");
        child.file_id = Some("file-b.json".to_string());
        write_root_for_test(&storage, &tracked_state, "child", Some("base"), &[child])
            .await
            .expect("child root should write");

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        assert!(
            storage::load_by_file_root(&mut read, "child")
                .await
                .expect("by-file root read should succeed")
                .is_none(),
            "child must not publish a partial by-file root"
        );
        let rows = tracked_state
            .reader(read)
            .scan_rows_at_commit(
                "child",
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
            .expect("file scan should fall back to primary root");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .entity_id
                .as_single_string_owned()
                .expect("entity id"),
            "entity-base"
        );
    }

    #[tokio::test]
    async fn child_root_tombstone_hides_materialized_base_row() {
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
        .expect("base root should write");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_projection_root_at("base")
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
        .expect("child tombstone root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .scan_rows_at_commit("child", &TrackedStateScanRequest::default())
            .await
            .expect("child scan should apply tombstone over base root");

        assert!(rows.is_empty(), "pending tombstone must hide base row");
    }

    #[tokio::test]
    async fn root_scan_keeps_last_mutation_for_duplicate_key() {
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
        .expect("root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .scan_rows_at_commit("commit-1", &TrackedStateScanRequest::default())
            .await
            .expect("root should scan");

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
        let mut rows = vec![deleted, live];
        for index in 0..25 {
            let mut row = row(
                &format!("entity-padding-{index}"),
                &format!("change-padding-{index}"),
                "commit-1",
            );
            row.file_id = Some("file-b.json".to_string());
            rows.push(row);
        }
        write_root_for_test(&storage, &tracked_state, "commit-1", None, &rows)
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
                        schema_keys: vec!["test_schema".to_string()],
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

    #[tokio::test]
    async fn validate_tree_rows_rejects_corrupt_updated_at() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row("entity-a", "change-parent", "parent")],
        )
        .await
        .expect("parent root should write");
        let mut child_row = row_with_value("entity-a", "change-child", "child", "child");
        child_row.updated_at = "2026-02-01T00:00:00Z".to_string();
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            std::slice::from_ref(&child_row),
        )
        .await
        .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let child_root = storage::load_root(&read, "child")
            .await
            .expect("child root should load")
            .expect("child root should exist");
        let child_metadata = storage::load_projection_metadata(&read, "child")
            .await
            .expect("child metadata should load")
            .expect("child metadata should exist");
        let key = TrackedStateKey {
            schema_key: child_row.schema_key.clone(),
            file_id: child_row.file_id.clone(),
            entity_id: child_row.entity_id.clone(),
        };
        let mut corrupt_value = TrackedStateTree::new()
            .get(&read, &child_root, &key)
            .await
            .expect("child value should load")
            .expect("child value should exist");
        corrupt_value.updated_at = "1999-01-01T00:00:00Z".to_string();
        drop(read);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let result = TrackedStateTree::new()
            .apply_mutations(
                &read,
                &mut writes,
                None,
                vec![TrackedStateMutation::put_encoded(
                    crate::tracked_state::codec::encode_key(&key),
                    crate::tracked_state::codec::encode_value(&corrupt_value),
                )],
                Some("child"),
            )
            .await
            .expect("corrupt child root should write");
        storage::stage_projection_metadata(
            &mut writes,
            &TrackedStateProjectionMetadata {
                commit_id: "child".to_string(),
                root_id: result.root_id,
                parent_roots: child_metadata.parent_roots,
                changed_key_count: child_metadata.changed_key_count,
                row_count_estimate: result.row_count as u64,
                tree_height: result.tree_height as u32,
                primary_chunk_count: result.chunk_count as u64,
                primary_chunk_bytes: result.chunk_bytes as u64,
            },
        )
        .expect("corrupt child metadata should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("corrupt child root should commit");

        let mut reader = tracked_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let error = reader
            .validate_tree_rows_at_commit_against_changelog(
                "child",
                &TrackedStateTreeScanRequest::default(),
            )
            .await
            .expect_err("corrupt updated_at must be rejected");

        assert!(error.message.contains("has updated_at"));
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
        write_root_for_test(
            &storage,
            &tracked_state,
            "target",
            Some("base"),
            target_rows,
        )
        .await
        .expect("target root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "source",
            Some("base"),
            source_rows,
        )
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
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        crate::test_support::stage_tracked_root_from_materialized(
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

    async fn write_by_file_root_for_test(
        storage: &StorageContext,
        commit_id: &str,
        rows: &[MaterializedTrackedStateRow],
    ) -> Result<(), LixError> {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let primary_root = storage::load_root(&read, commit_id)
            .await?
            .ok_or_else(|| missing_projection_root_error(commit_id))?;
        let tree = TrackedStateTree::new();
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let key = TrackedStateKey {
                schema_key: row.schema_key.clone(),
                file_id: row.file_id.clone(),
                entity_id: row.entity_id.clone(),
            };
            let mut value = tree
                .get(&read, &primary_root, &key)
                .await?
                .ok_or_else(|| LixError::unknown("test row is missing from primary root"))?;
            value.snapshot_ref = None;
            value.metadata_ref = None;
            entries.push((key, value));
        }
        write_by_file_entries_for_test(storage, commit_id, &entries).await
    }

    async fn write_by_file_entries_for_test(
        storage: &StorageContext,
        commit_id: &str,
        entries: &[(TrackedStateKey, TrackedStateIndexValue)],
    ) -> Result<(), LixError> {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut mutations = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            if key.file_id.is_none() {
                return Err(LixError::unknown(
                    "test by-file index entry requires a concrete file_id",
                ));
            }
            mutations.push(TrackedStateMutation::put_encoded(
                ByFileIndex::encode_key_ref(TrackedStateKeyRef {
                    schema_key: &key.schema_key,
                    file_id: key.file_id.as_deref(),
                    entity_id: &key.entity_id,
                }),
                crate::tracked_state::codec::encode_value(value),
            ));
        }
        let mut writes = storage.new_write_set();
        let result = TrackedStateTree::new()
            .apply_mutations(&read, &mut writes, None, mutations, None)
            .await?;
        storage::stage_by_file_root(&mut writes, commit_id, &result.root_id);
        storage.commit_write_set(writes, StorageWriteOptions::default())?;
        Ok(())
    }

    fn stage_corrupt_by_file_internal_root(
        storage: &StorageContext<InMemoryStorageBackend>,
        commit_id: &str,
        child: ChildSummary,
    ) {
        let node = encode_internal_node(&[child]);
        let hash = hash_bytes(&node);
        let mut writes = storage.new_write_set();
        storage::stage_chunks(&mut writes, &[PendingChunkWrite { hash, data: node }]);
        storage::stage_by_file_root(&mut writes, commit_id, &TrackedStateRootId::new(hash));
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("corrupt by-file root should commit");
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
