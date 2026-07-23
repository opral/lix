#![allow(
    clippy::cast_possible_truncation,
    clippy::clone_on_copy,
    clippy::match_same_arms,
    clippy::needless_pass_by_ref_mut,
    clippy::redundant_closure_for_method_calls,
    clippy::unnecessary_mut_passed,
    clippy::unnecessary_wraps
)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::changelog::{ChangeId, ChangeRecordProjection};
use crate::changelog::{
    ChangeLoadRequest, ChangeRecord, ChangelogContext, ChangelogReader, CommitId, CommitLoadEntry,
    CommitLoadRequest, CommitProjection,
};
use crate::entity_pk::EntityPk;
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};
use crate::tracked_state::codec::{encode_key_ref, encode_value_ref};
use crate::tracked_state::diff::{
    TrackedStateDiff, TrackedStateDiffRequest, TrackedStateDiffRow, diff_commits,
};
use crate::tracked_state::materialize_rows_from_index_entries;
#[cfg(test)]
use crate::tracked_state::merge::{self, TrackedStateMergePlan};
use crate::tracked_state::storage;
use crate::tracked_state::tree::TrackedStateTree;
use crate::tracked_state::types::{
    TrackedStateCommitRoot, TrackedStateCommitRootParent, TrackedStateIndexValue, TrackedStateKey,
    TrackedStateKeyRef, TrackedStateMutation, TrackedStateRootId, TrackedStateTreeScanRequest,
};
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateDeltaRef, TrackedStateScanRequest,
};
use crate::{LixError, NullableKeyFilter};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TrackedStateIdentity {
    schema_key: String,
    file_id: Option<String>,
    entity_pk: EntityPk,
}

/// Factory for tracked-state readers, root writers, and commit-root rebuilders.
///
/// Tracked state is stored as content-addressed roots. Branch refs
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
        S: StorageAdapterRead,
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
        S: StorageAdapterRead + ?Sized,
    {
        TrackedStateWriter {
            chunk_overlay: storage::TrackedStateChunkOverlay::new(),
            staged_roots: BTreeMap::new(),
            tree: self.tree.clone(),
            store,
            writes,
        }
    }

    /// Creates an explicit tracked-state commit-root rebuilder.
    ///
    /// Normal commits stage commit roots directly. This rebuilder reconstructs
    /// a missing root from changelog facts as an explicit maintenance path.
    pub(crate) fn root_rebuilder<'a, S>(
        &'a self,
        store: &'a S,
        writes: &'a mut StorageWriteSet,
    ) -> TrackedStateRootRebuilder<'a, S>
    where
        S: StorageAdapterRead + ?Sized,
    {
        let _ = self;
        TrackedStateRootRebuilder { store, writes }
    }

    pub(crate) async fn reachable_tree_chunk_hashes(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        roots: impl IntoIterator<Item = TrackedStateRootId>,
    ) -> Result<BTreeSet<[u8; crate::tracked_state::types::TRACKED_STATE_HASH_BYTES]>, LixError>
    {
        self.tree.reachable_chunk_hashes(store, roots).await
    }
}

/// Store-backed tracked-state reader created by `TrackedStateContext`.
pub(crate) struct TrackedStateStoreReader<S> {
    store: S,
    tree: TrackedStateTree,
}

struct DiffCommitRootValidationCache {
    commit_ref_winners: HashMap<String, HashMap<TrackedStateIdentity, ChangeId>>,
    commit_root_metadata: HashMap<String, TrackedStateCommitRoot>,
    commit_roots: HashMap<String, TrackedStateRootId>,
    tree_values: HashMap<(TrackedStateRootId, TrackedStateKey), Option<TrackedStateIndexValue>>,
    changelog_first_parents: HashMap<String, Option<CommitId>>,
}

impl DiffCommitRootValidationCache {
    fn new() -> Self {
        Self {
            commit_ref_winners: HashMap::new(),
            commit_root_metadata: HashMap::new(),
            commit_roots: HashMap::new(),
            tree_values: HashMap::new(),
            changelog_first_parents: HashMap::new(),
        }
    }
}

impl<S> TrackedStateStoreReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) async fn scan_rows_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<MaterializedTrackedStateRow>, LixError> {
        let Some(root_id) = self.tree.load_root(&self.store, commit_id).await? else {
            return Err(missing_commit_root_error(commit_id));
        };
        let rows = self
            .tree
            .scan(
                &self.store,
                &root_id,
                &tree_scan_request_from_tracked(request),
            )
            .await?;
        let materialization = ChangeRecordProjection::from_columns(&request.read_columns.columns);
        let mut rows =
            materialize_rows_from_index_entries(&self.store, rows, &materialization).await?;
        if !request.filter.include_tombstones {
            rows.retain(|row| !row.deleted);
        }
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    pub(crate) async fn load_projected_rows_at_commit(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<Vec<Option<MaterializedTrackedStateRow>>, LixError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut output_indices = BTreeMap::<TrackedStateKey, Vec<usize>>::new();
        for (index, key) in keys.iter().cloned().enumerate() {
            output_indices.entry(key).or_default().push(index);
        }
        let unique_keys = output_indices.keys().cloned().collect::<Vec<_>>();
        let values = self
            .commit_root_values_for_keys(commit_id, &unique_keys)
            .await?;
        let mut entries = Vec::new();
        for (key, value) in unique_keys.into_iter().zip(values) {
            if let Some(value) = value {
                entries.push((key, value));
            }
        }
        let materialized =
            materialize_rows_from_index_entries(&self.store, entries, projection).await?;
        let mut rows = vec![None; keys.len()];
        for row in materialized {
            let key = TrackedStateKey {
                schema_key: row.schema_key.clone(),
                entity_pk: row.entity_pk.clone(),
                file_id: row.file_id.clone(),
            };
            if let Some(indices) = output_indices.get(&key) {
                for &index in indices {
                    rows[index] = Some(row.clone());
                }
            }
        }
        Ok(rows)
    }

    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) async fn load_rows_at_commit(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKey],
    ) -> Result<Vec<Option<MaterializedTrackedStateRow>>, LixError> {
        self.load_projected_rows_at_commit(commit_id, keys, &ChangeRecordProjection::full())
            .await
    }

    pub(crate) async fn diff_commits(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStateDiffRequest,
    ) -> Result<TrackedStateDiff, LixError> {
        diff_commits(self, left_commit_id, right_commit_id, request).await
    }

    pub(crate) async fn validate_diff_rows_for_commits_against_changelog(
        &mut self,
        rows: &[(&TrackedStateDiffRow, &str)],
    ) -> Result<(), LixError> {
        let row_refs = rows.iter().map(|(row, _)| *row).collect::<Vec<_>>();
        let changes = self.load_and_validate_diff_row_changes(&row_refs).await?;
        let mut validation_cache = DiffCommitRootValidationCache::new();
        for (row, expected_commit_id) in rows {
            let change_created_at = changes
                .get(&row.change_id)
                .map(|change| change.created_at)
                .ok_or_else(|| {
                    LixError::unknown(format!(
                        "tracked-state diff row references missing changelog change '{}'",
                        row.change_id
                    ))
                })?;
            self.validate_diff_row_commit_root_membership(
                row,
                expected_commit_id,
                change_created_at,
                &mut validation_cache,
            )
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn validate_diff_rows_and_load_payloads(
        &mut self,
        rows: &[&TrackedStateDiffRow],
    ) -> Result<
        HashMap<ChangeId, (crate::json_store::JsonSlot, crate::json_store::JsonSlot)>,
        LixError,
    > {
        let changes = self.load_and_validate_diff_row_changes(rows).await?;
        Ok(changes
            .into_iter()
            .map(|(change_id, change)| (change_id, (change.snapshot, change.metadata)))
            .collect())
    }

    async fn load_and_validate_diff_row_changes(
        &mut self,
        rows: &[&TrackedStateDiffRow],
    ) -> Result<HashMap<ChangeId, ChangeRecord>, LixError> {
        if rows.is_empty() {
            return Ok(HashMap::new());
        }

        let mut change_ids = rows.iter().map(|row| row.change_id).collect::<Vec<_>>();
        change_ids.sort();
        change_ids.dedup();

        let mut changelog_reader = ChangelogContext::new().reader(&mut self.store);
        let loaded_changes = changelog_reader
            .load_changes(ChangeLoadRequest {
                change_ids: &change_ids,
            })
            .await?;
        let mut changes = HashMap::new();
        for (change_id, loaded) in change_ids.into_iter().zip(loaded_changes.entries) {
            let Some(change) = loaded else {
                return Err(LixError::unknown(format!(
                    "tracked-state diff row references missing changelog change '{change_id}'"
                )));
            };
            changes.insert(change_id, change);
        }
        for row in rows {
            validate_diff_row_against_changelog(row, &changes)?;
        }
        Ok(changes)
    }

    async fn validate_diff_row_commit_root_membership(
        &mut self,
        row: &TrackedStateDiffRow,
        root_commit_id: &str,
        change_created_at: crate::common::LixTimestamp,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<(), LixError> {
        let identity = tracked_state_identity_from_diff_row(row)?;
        let key = TrackedStateKey {
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            entity_pk: row.entity_pk.clone(),
        };
        let root_metadata = self
            .load_cached_commit_root_metadata(root_commit_id, cache)
            .await?;
        self.validate_commit_root_parent_matches_changelog(root_commit_id, &root_metadata, cache)
            .await?;
        let (_, row_value) = row.clone().into_index_entry();
        let mut current_commit_id = root_commit_id.to_string();
        let mut seen = HashSet::new();
        loop {
            if !seen.insert(current_commit_id.clone()) {
                return Err(LixError::unknown(format!(
                    "tracked-state commit-root parent chain contains cycle at commit '{current_commit_id}'"
                )));
            }

            let winner_change_id = self
                .load_cached_commit_ref_winner(&current_commit_id, &identity, cache)
                .await?;
            if let Some(winner_change_id) = winner_change_id {
                if winner_change_id != row.change_id {
                    return Err(LixError::unknown(format!(
                        "tracked-state diff row references changelog change '{}' that is not the first-parent winner for commit '{}' and identity {:?}",
                        row.change_id, root_commit_id, identity
                    )));
                }
                self.validate_diff_row_created_at(row, &key, &current_commit_id, change_created_at)
                    .await?;
                return Ok(());
            }

            let metadata = self
                .load_cached_commit_root_metadata(&current_commit_id, cache)
                .await?;
            self.validate_commit_root_parent_matches_changelog(
                &current_commit_id,
                &metadata,
                cache,
            )
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
                    "tracked-state commit-root row for commit '{}' does not match parent root '{}' for inherited identity {:?}",
                    root_commit_id, parent.commit_id, identity
                )));
            }
            current_commit_id = parent.commit_id.to_string();
        }
    }

    async fn validate_commit_root_parent_matches_changelog(
        &mut self,
        commit_id: &str,
        metadata: &TrackedStateCommitRoot,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<(), LixError> {
        if metadata.parent_roots.len() > 1 {
            return Err(LixError::unknown(format!(
                "tracked-state commit-root metadata for commit '{commit_id}' has more than one first-parent root"
            )));
        }
        let changelog_first_parent = self
            .load_cached_changelog_first_parent(commit_id, cache)
            .await?;
        let expected_parent = match changelog_first_parent {
            Some(first_parent_id) => {
                self.nearest_available_commit_root_parent(&first_parent_id.to_string(), cache)
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
                    "tracked-state commit-root metadata for commit '{commit_id}' references stale root for commit-root parent '{expected_parent_id}'"
                )))
            }
            (Some((expected_parent_id, _)), Some(parent)) => Err(LixError::unknown(format!(
                "tracked-state commit-root metadata for commit '{}' references parent '{}' but nearest available first-parent root is '{}'",
                commit_id, parent.commit_id, expected_parent_id
            ))),
            (Some((expected_parent_id, _)), None) => Err(LixError::unknown(format!(
                "tracked-state commit-root metadata for commit '{commit_id}' is missing commit-root parent '{expected_parent_id}'"
            ))),
            (None, Some(parent)) => Err(LixError::unknown(format!(
                "tracked-state commit-root metadata for root commit '{}' references unexpected parent '{}'",
                commit_id, parent.commit_id
            ))),
        }
    }

    async fn nearest_available_commit_root_parent(
        &mut self,
        start_commit_id: &str,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<Option<(String, TrackedStateRootId)>, LixError> {
        let mut current = Some(start_commit_id.to_string());
        let mut seen = HashSet::new();
        while let Some(commit_id) = current {
            if !seen.insert(commit_id.clone()) {
                return Err(LixError::unknown(format!(
                    "tracked-state commit-root parent chain contains cycle at commit '{commit_id}'"
                )));
            }
            if let Some(root_id) = self
                .load_cached_commit_root_optional(&commit_id, cache)
                .await?
            {
                return Ok(Some((commit_id, root_id)));
            }
            current = self
                .load_cached_changelog_first_parent(&commit_id, cache)
                .await?
                .map(|id| id.to_string());
        }
        Ok(None)
    }

    async fn load_cached_commit_ref_winners(
        &mut self,
        commit_id: &str,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<HashMap<TrackedStateIdentity, ChangeId>, LixError> {
        self.ensure_cached_commit_ref_winners(commit_id, cache)
            .await?;
        Ok(cache
            .commit_ref_winners
            .get(commit_id)
            .cloned()
            .expect("commit-ref winners should be cached after loading"))
    }

    async fn load_cached_commit_ref_winner(
        &mut self,
        commit_id: &str,
        identity: &TrackedStateIdentity,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<Option<ChangeId>, LixError> {
        self.ensure_cached_commit_ref_winners(commit_id, cache)
            .await?;
        Ok(cache
            .commit_ref_winners
            .get(commit_id)
            .and_then(|winners| winners.get(identity))
            .copied())
    }

    async fn ensure_cached_commit_ref_winners(
        &mut self,
        commit_id: &str,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<(), LixError> {
        if cache.commit_ref_winners.contains_key(commit_id) {
            return Ok(());
        }
        let commit_ids = [CommitId::parse_lix(
            commit_id,
            "commit-ref winner commit_id",
        )?];
        let mut changelog_reader = ChangelogContext::new().reader(&mut self.store);
        let batch = changelog_reader
            .load_commits(CommitLoadRequest {
                commit_ids: &commit_ids,
                projection: CommitProjection::Full,
            })
            .await?;
        let Some(entry) = batch.entries.into_iter().next().flatten() else {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' is missing while validating tracked-state commit-root rows"
            )));
        };
        let CommitLoadEntry::Full {
            change_ref_chunks: chunks,
            ..
        } = entry
        else {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' did not return full commit"
            )));
        };
        let mut winners = HashMap::new();
        // Ref chunks carry change ids only; row identities live in the
        // change records, batch point-read here.
        let change_ids = chunks
            .into_iter()
            .flat_map(|chunk| chunk.entries)
            .collect::<Vec<_>>();
        let changes = changelog_reader
            .load_changes(ChangeLoadRequest {
                change_ids: &change_ids,
            })
            .await?;
        for (change_id, change) in change_ids.iter().zip(changes.entries) {
            let Some(change) = change else {
                return Err(LixError::unknown(format!(
                    "changelog commit '{commit_id}' references change '{change_id}' that is missing from the changelog"
                )));
            };
            winners.insert(
                TrackedStateIdentity {
                    schema_key: change.schema_key,
                    file_id: change.file_id,
                    entity_pk: change.entity_pk,
                },
                *change_id,
            );
        }
        cache
            .commit_ref_winners
            .insert(commit_id.to_string(), winners);
        Ok(())
    }

    async fn load_cached_commit_root_metadata(
        &mut self,
        commit_id: &str,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<TrackedStateCommitRoot, LixError> {
        if let Some(metadata) = cache.commit_root_metadata.get(commit_id) {
            return Ok(metadata.clone());
        }
        let metadata = storage::load_commit_root(&self.store, commit_id)
            .await?
            .ok_or_else(|| missing_commit_root_error(commit_id))?;
        cache
            .commit_root_metadata
            .insert(commit_id.to_string(), metadata.clone());
        Ok(metadata)
    }

    async fn load_cached_commit_root_optional(
        &mut self,
        commit_id: &str,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<Option<TrackedStateRootId>, LixError> {
        if let Some(root_id) = cache.commit_roots.get(commit_id) {
            return Ok(Some(root_id.clone()));
        }
        let root_id = storage::load_root(&self.store, commit_id).await?;
        if let Some(root_id) = &root_id {
            cache
                .commit_roots
                .insert(commit_id.to_string(), root_id.clone());
        }
        Ok(root_id)
    }

    async fn load_cached_tree_value(
        &mut self,
        root_id: &TrackedStateRootId,
        key: &TrackedStateKey,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<Option<TrackedStateIndexValue>, LixError> {
        let cache_key = (root_id.clone(), key.clone());
        if let Some(value) = cache.tree_values.get(&cache_key) {
            return Ok(value.clone());
        }
        let value = self
            .tree
            .get_many(&self.store, root_id, std::slice::from_ref(key))
            .await?
            .into_iter()
            .next()
            .flatten();
        cache.tree_values.insert(cache_key, value.clone());
        Ok(value)
    }

    async fn load_cached_changelog_first_parent(
        &mut self,
        commit_id: &str,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<Option<CommitId>, LixError> {
        if let Some(parent_id) = cache.changelog_first_parents.get(commit_id) {
            return Ok(*parent_id);
        }
        let commit_ids = [CommitId::parse_lix(
            commit_id,
            "changelog first parent commit_id",
        )?];
        let mut changelog_reader = ChangelogContext::new().reader(&mut self.store);
        let batch = changelog_reader
            .load_commits(CommitLoadRequest {
                commit_ids: &commit_ids,
                projection: CommitProjection::Record,
            })
            .await?;
        let Some(entry) = batch.entries.into_iter().next().flatten() else {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' is missing while validating tracked-state commit-root metadata"
            )));
        };
        let CommitLoadEntry::Record(record) = entry else {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' did not return a commit record"
            )));
        };
        let parent_id = record.parent_commit_ids.first().copied();
        cache
            .changelog_first_parents
            .insert(commit_id.to_string(), parent_id);
        Ok(parent_id)
    }

    async fn validate_diff_row_created_at(
        &mut self,
        row: &TrackedStateDiffRow,
        key: &TrackedStateKey,
        commit_id: &str,
        change_created_at: crate::common::LixTimestamp,
    ) -> Result<(), LixError> {
        let mut expected_created_at = change_created_at;
        let Some(metadata) = storage::load_commit_root(&self.store, commit_id).await? else {
            return Err(missing_commit_root_error(commit_id));
        };
        if let Some(parent) = metadata.parent_roots.first() {
            let parent_value = self
                .tree
                .get_many(&self.store, &parent.root_id, std::slice::from_ref(key))
                .await?
                .into_iter()
                .next()
                .flatten();
            if let Some(parent_value) = parent_value {
                expected_created_at = parent_value.created_at();
            }
        }
        if expected_created_at == change_created_at {
            if let Some(merge_parent_created_at) = self
                .load_merge_parent_created_at_for_row(commit_id, row, key)
                .await?
            {
                expected_created_at = merge_parent_created_at;
            }
        }
        if expected_created_at == change_created_at && row.commit_id != commit_id {
            if let Some(source_created_at) =
                self.load_parent_created_at_for_row_commit(row, key).await?
            {
                expected_created_at = source_created_at;
            }
        }
        if row.created_at == expected_created_at {
            return Ok(());
        }
        Err(LixError::unknown(format!(
            "tracked-state diff row for change '{}' created_at '{}' does not match first ancestry timestamp '{}'",
            row.change_id, row.created_at, expected_created_at
        )))
    }

    async fn load_merge_parent_created_at_for_row(
        &mut self,
        commit_id: &str,
        row: &TrackedStateDiffRow,
        key: &TrackedStateKey,
    ) -> Result<Option<crate::common::LixTimestamp>, LixError> {
        let commit_ids = [CommitId::parse_lix(commit_id, "merge parent commit_id")?];
        let mut changelog_reader = ChangelogContext::new().reader(&mut self.store);
        let batch = changelog_reader
            .load_commits(CommitLoadRequest {
                commit_ids: &commit_ids,
                projection: CommitProjection::Record,
            })
            .await?;
        let Some(CommitLoadEntry::Record(commit)) = batch.entries.into_iter().next().flatten()
        else {
            return Ok(None);
        };
        for parent_id in commit.parent_commit_ids.iter().skip(1) {
            let Some(parent_root) = storage::load_root(&self.store, &parent_id.to_string()).await?
            else {
                continue;
            };
            let parent_value = self
                .tree
                .get_many(&self.store, &parent_root, std::slice::from_ref(key))
                .await?
                .into_iter()
                .next()
                .flatten();
            if let Some(parent_value) = parent_value {
                if parent_value.change_id == row.change_id {
                    return Ok(Some(parent_value.created_at()));
                }
            }
        }
        Ok(None)
    }

    async fn load_parent_created_at_for_row_commit(
        &mut self,
        row: &TrackedStateDiffRow,
        key: &TrackedStateKey,
    ) -> Result<Option<crate::common::LixTimestamp>, LixError> {
        let row_commit_id = row.commit_id.to_string();
        let Some(metadata) = storage::load_commit_root(&self.store, &row_commit_id).await? else {
            return Ok(None);
        };
        let Some(parent) = metadata.parent_roots.first() else {
            return Ok(None);
        };
        let parent_value = self
            .tree
            .get_many(&self.store, &parent.root_id, std::slice::from_ref(key))
            .await?
            .into_iter()
            .next()
            .flatten();
        Ok(parent_value.map(|value| value.created_at()))
    }

    /// Runs the full O(total rows) tracked-root coverage audit.
    ///
    /// Normal diff validates root metadata and changed rows only. Maintenance
    /// and repair tooling can call this when it deliberately needs fsck-level
    /// assurance for every unchanged row too.
    pub(crate) async fn validate_commit_root_against_changelog(
        &mut self,
        commit_id: &str,
    ) -> Result<(), LixError> {
        self.validate_tree_rows_at_commit_against_changelog(
            commit_id,
            &TrackedStateTreeScanRequest::default(),
        )
        .await
    }

    async fn validate_tree_rows_at_commit_against_changelog(
        &mut self,
        commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<(), LixError> {
        let mut validation_cache = DiffCommitRootValidationCache::new();
        let metadata = self
            .load_cached_commit_root_metadata(commit_id, &mut validation_cache)
            .await?;
        self.validate_commit_root_parent_matches_changelog(
            commit_id,
            &metadata,
            &mut validation_cache,
        )
        .await?;
        let root = metadata.root_id;
        let rows = self.tree.scan(&self.store, &root, request).await?;
        self.validate_commit_root_coverage(commit_id, request, &rows)
            .await?;
        let rows = rows
            .into_iter()
            .map(|(key, value)| TrackedStateDiffRow::from_tree_entry(key, value))
            .collect::<Vec<_>>();
        let row_refs = rows.iter().map(|row| (row, commit_id)).collect::<Vec<_>>();
        self.validate_diff_rows_for_commits_against_changelog(&row_refs)
            .await
    }

    async fn validate_commit_root_coverage(
        &mut self,
        commit_id: &str,
        request: &TrackedStateTreeScanRequest,
        rows: &[(TrackedStateKey, TrackedStateIndexValue)],
    ) -> Result<(), LixError> {
        let row_map = rows
            .iter()
            .map(|(key, value)| (tracked_state_identity_from_key(key), value))
            .collect::<HashMap<_, _>>();
        let mut cache = DiffCommitRootValidationCache::new();
        let winners = self
            .load_cached_commit_ref_winners(commit_id, &mut cache)
            .await?;
        for (identity, change_id) in &winners {
            if !tracked_state_identity_matches_tree_request(identity, request) {
                continue;
            }
            let Some(value) = row_map.get(identity) else {
                return Err(LixError::unknown(format!(
                    "tracked-state commit-root for commit '{commit_id}' omits current changelog change '{change_id}' for identity {identity:?}"
                )));
            };
            if &value.change_id != change_id {
                return Err(LixError::unknown(format!(
                    "tracked-state commit-root for commit '{commit_id}' stores change '{}' but changelog winner is '{}' for identity {:?}",
                    value.change_id, change_id, identity
                )));
            }
        }

        let metadata = self
            .load_cached_commit_root_metadata(commit_id, &mut cache)
            .await?;
        let Some(parent) = metadata.parent_roots.first() else {
            return Ok(());
        };
        let parent_rows = self
            .tree
            .scan(&self.store, &parent.root_id, request)
            .await?;
        for (parent_key, parent_value) in parent_rows {
            let identity = tracked_state_identity_from_key(&parent_key);
            if winners.contains_key(&identity) {
                continue;
            }
            let Some(value) = row_map.get(&identity) else {
                return Err(LixError::unknown(format!(
                    "tracked-state commit-root for commit '{commit_id}' omits inherited identity {:?} from parent '{}'",
                    identity, parent.commit_id
                )));
            };
            if *value != &parent_value {
                return Err(LixError::unknown(format!(
                    "tracked-state commit-root for commit '{commit_id}' does not preserve inherited identity {:?} from parent '{}'",
                    identity, parent.commit_id
                )));
            }
        }
        Ok(())
    }

    /// Batched payload-slot load for diff's cross-change equality fallback.
    pub(crate) async fn load_change_payloads(
        &mut self,
        change_ids: &[ChangeId],
    ) -> Result<
        HashMap<ChangeId, (crate::json_store::JsonSlot, crate::json_store::JsonSlot)>,
        LixError,
    > {
        let records =
            crate::changelog::load_change_records(&self.store, change_ids.iter().copied()).await?;
        Ok(records
            .into_iter()
            .map(|(change_id, record)| (change_id, (record.snapshot, record.metadata)))
            .collect())
    }

    pub(crate) async fn diff_tree_entries_at_commits(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<Vec<crate::tracked_state::types::TrackedStateTreeDiffEntry>, LixError> {
        let mut cache = DiffCommitRootValidationCache::new();
        let left_root = self
            .load_validated_diff_root(left_commit_id, &mut cache)
            .await?;
        let right_root = if left_commit_id == right_commit_id {
            left_root.clone()
        } else {
            self.load_validated_diff_root(right_commit_id, &mut cache)
                .await?
        };
        self.tree
            .diff(&self.store, Some(&left_root), Some(&right_root), request)
            .await
    }

    async fn load_validated_diff_root(
        &mut self,
        commit_id: &str,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<TrackedStateRootId, LixError> {
        let metadata = self
            .load_cached_commit_root_metadata(commit_id, cache)
            .await?;
        self.validate_commit_root_parent_matches_changelog(commit_id, &metadata, cache)
            .await?;
        Ok(metadata.root_id)
    }

    async fn load_ensured_root(&mut self, commit_id: &str) -> Result<TrackedStateRootId, LixError> {
        self.tree
            .load_root(&self.store, commit_id)
            .await?
            .ok_or_else(|| missing_commit_root_error(commit_id))
    }

    async fn commit_root_values_for_keys(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKey],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        let root_id = self.load_ensured_root(commit_id).await?;
        self.tree.get_many(&self.store, &root_id, keys).await
    }

    /// Plans a three-way merge by diffing both heads against the same base.
    ///
    /// `target_commit_id` is the destination root that should keep its own
    /// changes. `source_commit_id` is the incoming root whose non-conflicting
    /// changes should be applied.
    #[cfg(test)]
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
        let fallback_ids = merge::merge_payload_fallback_ids(&target_diff, &source_diff);
        let payloads = self.load_change_payloads(&fallback_ids).await?;
        merge::plan_merge(&target_diff, &source_diff, &payloads)
    }
}

/// Writer for changelog-backed tracked-state commit roots.
pub(crate) struct TrackedStateWriter<'a, S: ?Sized> {
    chunk_overlay: storage::TrackedStateChunkOverlay,
    staged_roots: BTreeMap<String, TrackedStateCommitRoot>,
    tree: TrackedStateTree,
    store: &'a S,
    writes: &'a mut StorageWriteSet,
}

/// Explicit commit-root rebuilder created by `TrackedStateContext`.
pub(crate) struct TrackedStateRootRebuilder<'a, S: ?Sized> {
    pub(super) store: &'a S,
    pub(super) writes: &'a mut StorageWriteSet,
}

impl<S> TrackedStateRootRebuilder<'_, S>
where
    S: StorageAdapterRead + ?Sized,
{
    pub(crate) async fn rebuild_commit_root_at(
        &mut self,
        commit_id: &str,
    ) -> Result<TrackedStateWriteReport, LixError> {
        crate::tracked_state::commit_root_rebuild::rebuild_commit_root_at(self, commit_id).await
    }
}

impl<S> TrackedStateWriter<'_, S>
where
    S: StorageAdapterRead + ?Sized,
{
    pub(crate) async fn validate_staged_commit_root_against_changelog(
        &self,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let read = storage::TrackedStateStagedRead::new(
            self.store,
            self.staged_roots.values(),
            &self.chunk_overlay,
        )?;
        TrackedStateContext::new()
            .reader(read)
            .validate_commit_root_against_changelog(commit_id)
            .await
    }

    pub(crate) async fn stage_commit_root<'a, I>(
        &mut self,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        deltas: I,
    ) -> Result<TrackedStateWriteReport, LixError>
    where
        I: IntoIterator<Item = TrackedStateDeltaRef<'a>>,
    {
        let deltas = deltas.into_iter().collect::<Vec<_>>();
        let typed_commit_id =
            CommitId::parse_lix(commit_id, "tracked-state commit root commit_id")?;
        let typed_parent_commit_id = parent_commit_id
            .map(|id| CommitId::parse_lix(id, "tracked-state parent commit_id"))
            .transpose()?;
        let parent_metadata = match parent_commit_id {
            Some(parent_commit_id) => {
                let metadata = match self.staged_roots.get(parent_commit_id) {
                    Some(metadata) => Some(metadata.clone()),
                    None => storage::load_commit_root(self.store, parent_commit_id).await?,
                };
                let Some(metadata) = metadata else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "tracked-state parent root for commit '{parent_commit_id}' is missing"
                        ),
                    ));
                };
                Some(metadata)
            }
            None => None,
        };
        let base_root = parent_metadata
            .as_ref()
            .map(|metadata| metadata.root_id.clone());
        if deltas.is_empty()
            && let Some(parent_metadata) = parent_metadata.as_ref()
        {
            let root_id = parent_metadata.root_id.clone();
            let metadata = TrackedStateCommitRoot {
                commit_id: typed_commit_id,
                root_id: root_id.clone(),
                parent_roots: vec![TrackedStateCommitRootParent {
                    commit_id: typed_parent_commit_id.expect("parent metadata requires parent id"),
                    root_id: root_id.clone(),
                }],
                changed_key_count: 0,
                row_count_estimate: parent_metadata.row_count_estimate,
                tree_height: parent_metadata.tree_height,
                primary_chunk_count: 0,
                primary_chunk_bytes: 0,
            };
            storage::stage_commit_root(self.writes, &metadata)?;
            self.staged_roots.insert(commit_id.to_string(), metadata);
            return Ok(TrackedStateWriteReport {
                commit_id: typed_commit_id,
                root_id,
                changed_rows: 0,
                primary_chunk_puts: 0,
            });
        }
        let parent_values = if let Some(base_root) = base_root.as_ref() {
            let keys = deltas
                .iter()
                .map(|delta| TrackedStateKey {
                    schema_key: delta.schema_key.to_string(),
                    file_id: delta.file_id.map(str::to_string),
                    entity_pk: delta.entity_pk.clone(),
                })
                .collect::<Vec<_>>();
            self.tree.get_many(self.store, base_root, &keys).await?
        } else {
            vec![None; deltas.len()]
        };
        let mut mutations = Vec::with_capacity(deltas.len());
        for (delta, parent_value) in deltas.iter().zip(parent_values.iter()) {
            let parent_created_at = parent_value.as_ref().map(|value| value.created_at());
            let created_at = parent_created_at.unwrap_or(delta.created_at);
            let key = TrackedStateKeyRef {
                schema_key: delta.schema_key,
                file_id: delta.file_id,
                entity_pk: delta.entity_pk,
            };
            let value = crate::tracked_state::types::TrackedStateIndexValueRef {
                change_id: delta.change_id,
                commit_id: delta.commit_id,
                deleted: delta.deleted,

                created_at,
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
        let metadata = TrackedStateCommitRoot {
            commit_id: typed_commit_id,
            root_id: result.root_id.clone(),
            parent_roots: typed_parent_commit_id
                .zip(base_root.as_ref())
                .map(|(parent_commit_id, root_id)| {
                    vec![TrackedStateCommitRootParent {
                        commit_id: parent_commit_id,
                        root_id: root_id.clone(),
                    }]
                })
                .unwrap_or_default(),
            changed_key_count: u64::try_from(deltas.len()).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_root changed key count exceeds u64",
                )
            })?,
            row_count_estimate: u64::try_from(result.row_count).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_root row count exceeds u64",
                )
            })?,
            tree_height: u32::try_from(result.tree_height).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_root tree height exceeds u32",
                )
            })?,
            primary_chunk_count: u64::try_from(result.chunk_count).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_root chunk count exceeds u64",
                )
            })?,
            primary_chunk_bytes: u64::try_from(result.chunk_bytes).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_root chunk bytes exceeds u64",
                )
            })?,
        };
        storage::stage_commit_root(self.writes, &metadata)?;
        self.staged_roots.insert(commit_id.to_string(), metadata);

        Ok(TrackedStateWriteReport {
            commit_id: typed_commit_id,
            root_id: result.root_id,
            changed_rows: deltas.len(),
            primary_chunk_puts: result.chunk_count,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateWriteReport {
    pub(crate) commit_id: CommitId,
    pub(crate) root_id: TrackedStateRootId,
    pub(crate) changed_rows: usize,
    pub(crate) primary_chunk_puts: usize,
}

fn missing_commit_root_error(commit_id: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "tracked_state commit_root is missing for commit '{commit_id}'; run explicit commit_root rebuild before structural diff"
        ),
    )
}

fn tree_scan_request_from_tracked(
    request: &TrackedStateScanRequest,
) -> TrackedStateTreeScanRequest {
    TrackedStateTreeScanRequest {
        schema_keys: request.filter.schema_keys.clone(),
        entity_pks: request.filter.entity_pks.clone(),
        file_ids: request.filter.file_ids.clone(),
        include_tombstones: request.filter.include_tombstones,
        // User limits belong above delta overlay and tombstone visibility.
        // Pushing them into the physical tree can stop on rows that are later
        // hidden, returning too few live rows.
        limit: None,
    }
}

fn validate_diff_row_against_changelog(
    row: &TrackedStateDiffRow,
    changes: &HashMap<ChangeId, ChangeRecord>,
) -> Result<(), LixError> {
    let Some(change) = changes.get(&row.change_id) else {
        return Err(LixError::unknown(format!(
            "tracked-state diff row references missing changelog change '{}'",
            row.change_id
        )));
    };
    if change.schema_key != row.schema_key
        || change.file_id != row.file_id
        || change.entity_pk != row.entity_pk
    {
        return Err(LixError::unknown(format!(
            "tracked-state diff row for change '{}' does not match changelog change identity",
            row.change_id
        )));
    }
    if row.deleted != change.snapshot.is_none() {
        return Err(LixError::unknown(format!(
            "tracked-state diff row for change '{}' deleted flag does not match changelog snapshot",
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

fn tracked_state_identity_from_diff_row(
    row: &TrackedStateDiffRow,
) -> Result<TrackedStateIdentity, LixError> {
    Ok(TrackedStateIdentity {
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        entity_pk: row.entity_pk.clone(),
    })
}

fn tracked_state_identity_from_key(key: &TrackedStateKey) -> TrackedStateIdentity {
    TrackedStateIdentity {
        schema_key: key.schema_key.clone(),
        file_id: key.file_id.clone(),
        entity_pk: key.entity_pk.clone(),
    }
}

fn tracked_state_identity_matches_tree_request(
    identity: &TrackedStateIdentity,
    request: &TrackedStateTreeScanRequest,
) -> bool {
    if !request.schema_keys.is_empty() && !request.schema_keys.contains(&identity.schema_key) {
        return false;
    }
    if !request.entity_pks.is_empty() && !request.entity_pks.contains(&identity.entity_pk) {
        return false;
    }
    nullable_key_filter_allows(&request.file_ids, identity.file_id.as_deref())
}

fn nullable_key_filter_allows(filters: &[NullableKeyFilter<String>], value: Option<&str>) -> bool {
    filters.is_empty()
        || filters.iter().any(|filter| match (filter, value) {
            (NullableKeyFilter::Any, _) => true,
            (NullableKeyFilter::Null, None) => true,
            (NullableKeyFilter::Value(expected), Some(value)) => expected == value,
            _ => false,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NullableKeyFilter;
    use crate::changelog::CommitRecord;
    use crate::storage_adapter::StorageAdapter;
    use crate::storage_adapter::{Memory, StorageReadOptions, StorageWriteOptions};

    fn commit_root_key(label: &str) -> crate::storage_adapter::StorageKey {
        crate::storage_adapter::StorageKey(bytes::Bytes::copy_from_slice(
            CommitId::for_test_label(label).as_uuid().as_bytes(),
        ))
    }

    fn change_id(label: &str) -> String {
        ChangeId::for_test_label(label).to_string()
    }

    #[tokio::test]
    async fn stage_commit_root_requires_parent_commit_root() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .await
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
                .await
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
        .expect_err("root staging should require a parent commit root");
    }

    #[tokio::test]
    async fn stage_commit_root_writes_commit_root_metadata() {
        let storage = StorageAdapter::new(Memory::new());
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
            .await
            .expect("read should open");
        let parent_root = storage::load_root(&read, "parent")
            .await
            .expect("parent root should load")
            .expect("parent root should exist");
        let child_root = storage::load_root(&read, "child")
            .await
            .expect("child root should load")
            .expect("child root should exist");
        let metadata = storage::load_commit_root(&read, "child")
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
    async fn staged_root_audit_failure_does_not_publish_replacement() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-a",
            None,
            &[row("entity-a", "change-a", "commit-a")],
        )
        .await
        .expect("committed root should write");
        let original_root = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("original-root read should open");
            storage::load_root(&read, "commit-a")
                .await
                .expect("original root should load")
                .expect("original root should exist")
        };

        {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("staged-root read should open");
            let mut writes = storage.new_write_set();
            let mut writer = tracked_state.writer(&read, &mut writes);
            let replacement = writer
                .stage_commit_root(
                    "commit-a",
                    None,
                    std::iter::empty::<TrackedStateDeltaRef<'_>>(),
                )
                .await
                .expect("invalid replacement should stage before audit");
            assert_ne!(replacement.root_id, original_root);

            let error = writer
                .validate_staged_commit_root_against_changelog("commit-a")
                .await
                .expect_err("audit must reject a root that omits the changelog winner");
            assert!(
                error.message.contains("omits current changelog change"),
                "unexpected error: {error}"
            );
            // Dropping the failed staged write set is the publication fence.
        }

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verification read should open");
        assert_eq!(
            storage::load_root(&read, "commit-a")
                .await
                .expect("published root should load"),
            Some(original_root)
        );
    }

    #[tokio::test]
    async fn stage_empty_commit_root_reuses_parent_without_tree_chunks() {
        let storage = StorageAdapter::new(Memory::new());
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

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let parent_metadata = storage::load_commit_root(&read, "parent")
            .await
            .expect("parent metadata should load")
            .expect("parent metadata should exist");
        let mut writes = storage.new_write_set();
        let report = tracked_state
            .writer(&mut read, &mut writes)
            .stage_commit_root("empty-child", Some("parent"), [])
            .await
            .expect("empty child root should stage");

        assert_eq!(report.changed_rows, 0);
        assert_eq!(report.primary_chunk_puts, 0);
        assert_eq!(report.root_id, parent_metadata.root_id);

        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("empty child root should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should reopen");
        let child_metadata = storage::load_commit_root(&read, "empty-child")
            .await
            .expect("child metadata should load")
            .expect("child metadata should exist");

        assert_eq!(child_metadata.root_id, parent_metadata.root_id);
        assert_eq!(child_metadata.changed_key_count, 0);
        assert_eq!(
            child_metadata.row_count_estimate,
            parent_metadata.row_count_estimate
        );
        assert_eq!(child_metadata.tree_height, parent_metadata.tree_height);
        assert_eq!(child_metadata.primary_chunk_count, 0);
        assert_eq!(child_metadata.primary_chunk_bytes, 0);
        assert_eq!(child_metadata.parent_roots.len(), 1);
        assert_eq!(child_metadata.parent_roots[0].commit_id, "parent");
        assert_eq!(
            child_metadata.parent_roots[0].root_id,
            parent_metadata.root_id
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
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
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

        assert_eq!(merge_pick_ids(&plan), vec!["entity-a"]);
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
                    .await
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

        assert!(plan.picks.is_empty());
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
                    .await
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

        assert!(plan.picks.is_empty());
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
                    .await
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

        assert_eq!(merge_pick_ids(&plan), vec!["entity-a"]);
        assert!(plan.picks[0].source_row().deleted);
        assert_eq!(
            plan.picks[0].source_change_id(),
            change_id("change-source-delete")
        );
    }

    #[tokio::test]
    async fn explicit_rebuild_repairs_missing_child_root_from_nearest_parent() {
        let storage = StorageAdapter::new(Memory::new());
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
                storage::TRACKED_STATE_COMMIT_ROOT_SPACE,
                commit_root_key("child"),
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("child commit_root delete should commit");
        }

        tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &test_schema_diff_request())
            .await
            .expect_err("diff should require durable roots before repair");

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        tracked_state
            .root_rebuilder(&mut read, &mut writes)
            .rebuild_commit_root_at("child")
            .await
            .expect("child root should repair");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("repaired root should commit");

        let diff = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &test_schema_diff_request())
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
                .map(|row| row.change_id.to_string()),
            Some(change_id("change-child"))
        );
    }

    #[tokio::test]
    async fn diff_allows_repaired_root_with_rebuilt_ancestor_chain() {
        let storage = StorageAdapter::new(Memory::new());
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
                    storage::TRACKED_STATE_COMMIT_ROOT_SPACE,
                    commit_root_key(commit_id),
                );
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("commit_root deletes should commit");
        }

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        tracked_state
            .root_rebuilder(&mut read, &mut writes)
            .rebuild_commit_root_at("child")
            .await
            .expect("child root should repair");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("repaired root should commit");

        let diff = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &test_schema_diff_request())
            .await
            .expect("diff should accept repaired nearest-ancestor parent metadata");

        assert_eq!(diff.entries.len(), 1);
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .map(|row| row.change_id.to_string()),
            Some(change_id("change-child"))
        );
    }

    #[tokio::test]
    async fn explicit_rebuild_repairs_missing_ancestor_chain() {
        let storage = StorageAdapter::new(Memory::new());
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
                    storage::TRACKED_STATE_COMMIT_ROOT_SPACE,
                    commit_root_key(commit_id),
                );
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("commit_root deletes should commit");
        }

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at("child")
            .await
            .expect("explicit rebuild should repair missing ancestor chain");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("repaired roots should commit");

        let diff = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &test_schema_diff_request())
            .await
            .expect("diff should accept explicitly rebuilt chain");

        assert_eq!(diff.entries.len(), 1);
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .map(|row| row.change_id.to_string()),
            Some(change_id("change-child"))
        );
    }

    #[tokio::test]
    async fn explicit_rebuild_errors_on_first_parent_cycle() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_empty_changelog_commit(
                &mut read,
                &mut writes,
                "commit-a",
                None,
            )
            .await
            .expect("commit-a should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("commit-a should commit");
        }
        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_empty_changelog_commit_with_parents(
                &mut read,
                &mut writes,
                "commit-b",
                &["commit-a".to_string()],
            )
            .await
            .expect("commit-b should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("commit-b should commit");
        }
        {
            let mut writes = storage.new_write_set();
            let commit_a = CommitId::for_test_label("commit-a");
            let commit_b = CommitId::for_test_label("commit-b");
            writes.put(
                crate::changelog::COMMIT_SPACE,
                crate::storage_adapter::StorageKey(bytes::Bytes::copy_from_slice(
                    commit_a.as_uuid().as_bytes(),
                )),
                crate::changelog::encode_commit_record(&CommitRecord {
                    format_version: 1,
                    commit_id: commit_a,
                    parent_commit_ids: vec![commit_b],
                    change_id: ChangeId::for_test_label("commit-a:commit"),
                    author_account_ids: Vec::new(),
                    created_at: crate::common::LixTimestamp::expect_parse(
                        "created_at",
                        "1970-01-01T00:00:00.000Z",
                    ),
                })
                .expect("corrupt cycle commit should encode"),
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("cycle corruption should commit");
        }

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let error = tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at("commit-a")
            .await
            .expect_err("first-parent cycle should not rebuild forever");

        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(
            error.message.contains("first-parent cycle"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[tokio::test]
    async fn explicit_rebuild_repairs_missing_head_root_chunk() {
        let storage = StorageAdapter::new(Memory::new());
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
        delete_root_chunk_for_test(&storage, "child").await;

        tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &test_schema_diff_request())
            .await
            .expect_err("diff should fail before missing root chunk repair");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at("child")
            .await
            .expect("child root chunk should repair");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("repaired root should commit");

        let diff = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &test_schema_diff_request())
            .await
            .expect("diff should use repaired root chunk");

        assert_eq!(diff.entries.len(), 1);
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .map(|row| row.change_id.to_string()),
            Some(change_id("change-child"))
        );
    }

    #[tokio::test]
    async fn explicit_rebuild_repairs_corrupt_head_root_chunk() {
        let storage = StorageAdapter::new(Memory::new());
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
        corrupt_root_chunk_for_test(&storage, "child").await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at("child")
            .await
            .expect("corrupt child root chunk should repair");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("repaired root should commit");

        let diff = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &test_schema_diff_request())
            .await
            .expect("diff should use repaired root chunk");

        assert_eq!(diff.entries.len(), 1);
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .map(|row| row.change_id.to_string()),
            Some(change_id("change-child"))
        );
    }

    #[tokio::test]
    async fn explicit_rebuild_repairs_stale_root_missing_inherited_row() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let inherited = row_with_value("entity-a", "change-base", "base", "base");
        let child = row_with_value("entity-b", "change-child", "child", "child");
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            std::slice::from_ref(&inherited),
        )
        .await
        .expect("base root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("base"),
            std::slice::from_ref(&child),
        )
        .await
        .expect("child root should write");
        overwrite_root_with_rows_for_test(&storage, "child", std::slice::from_ref(&child)).await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at("child")
            .await
            .expect("stale child root should repair");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("repaired root should commit");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_rows_at_commit("child", &test_schema_scan_request())
            .await
            .expect("repaired child root should scan");
        assert_eq!(
            rows.iter()
                .map(|row| row.change_id.to_string())
                .collect::<Vec<_>>(),
            vec![change_id("change-base"), change_id("change-child")]
        );
    }

    #[tokio::test]
    async fn scan_rows_filters_by_file() {
        let storage = StorageAdapter::new(Memory::new());
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
                    .await
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
            .expect("file scan should use primary root");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .entity_pk
                .as_single_string_owned()
                .expect("entity pk"),
            "entity-a"
        );
        assert_eq!(rows[0].file_id.as_deref(), Some("file-a.json"));
    }

    #[tokio::test]
    async fn file_filtered_header_scan_fetches_primary_payload_only_when_requested() {
        let storage = StorageAdapter::new(Memory::new());
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
                .await
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
                    read_columns: crate::tracked_state::TrackedStateReadColumns {
                        columns: vec!["entity_pk".to_string()],
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("header scan should use primary root");
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
    async fn null_file_rows_match_null_file_filter() {
        let storage = StorageAdapter::new(Memory::new());
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

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_string()],
                        file_ids: vec![NullableKeyFilter::Null],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("null file scan should use primary tree");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .entity_pk
                .as_single_string_owned()
                .expect("entity pk"),
            "entity-a"
        );
    }

    #[tokio::test]
    async fn mixed_null_and_concrete_file_scan_uses_primary_tree() {
        let storage = StorageAdapter::new(Memory::new());
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
                    .await
                    .expect("read should open"),
            )
            .scan_rows_at_commit(
                "commit-2",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_string()],
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

        let mut entity_pks = rows
            .iter()
            .map(|row| row.entity_pk.as_single_string_owned().expect("entity pk"))
            .collect::<Vec<_>>();
        entity_pks.sort();
        assert_eq!(entity_pks, vec!["entity-file", "entity-null"]);
    }

    #[tokio::test]
    async fn file_filtered_header_scan_filters_tombstones_without_payload_sentinel() {
        let storage = StorageAdapter::new(Memory::new());
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
                    .await
                    .expect("read should open"),
            )
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        file_ids: vec![NullableKeyFilter::Value("file-a.json".to_string())],
                        ..Default::default()
                    },
                    read_columns: crate::tracked_state::TrackedStateReadColumns {
                        columns: vec!["entity_pk".to_string()],
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("file scan should use primary root");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .entity_pk
                .as_single_string_owned()
                .expect("entity pk"),
            "entity-live"
        );
    }

    #[tokio::test]
    async fn child_root_tombstone_hides_materialized_base_row() {
        let storage = StorageAdapter::new(Memory::new());
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
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at("base")
            .await
            .expect("base commit root should materialize");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
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
                    .await
                    .expect("read should open"),
            )
            .scan_rows_at_commit("child", &test_schema_scan_request())
            .await
            .expect("child scan should apply tombstone over base root");

        assert!(rows.is_empty(), "pending tombstone must hide base row");
    }

    #[tokio::test]
    async fn root_scan_keeps_last_mutation_for_duplicate_key() {
        let storage = StorageAdapter::new(Memory::new());
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
                    .await
                    .expect("read should open"),
            )
            .scan_rows_at_commit("commit-1", &test_schema_scan_request())
            .await
            .expect("root should scan");

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter()
                .map(|row| (
                    row.entity_pk.as_single_string_owned().expect("entity pk"),
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
        let storage = StorageAdapter::new(Memory::new());
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
                    .await
                    .expect("read should open"),
            )
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_string()],
                        ..Default::default()
                    },
                    limit: Some(1),
                    ..Default::default()
                },
            )
            .await
            .expect("limited scan should apply visibility before limit");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .entity_pk
                .as_single_string_owned()
                .expect("entity pk"),
            "entity-b"
        );
    }

    #[tokio::test]
    async fn file_filtered_scan_limit_applies_after_tombstone_visibility() {
        let storage = StorageAdapter::new(Memory::new());
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
                    .await
                    .expect("read should open"),
            )
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        file_ids: vec![NullableKeyFilter::Value("file-a.json".to_string())],
                        ..Default::default()
                    },
                    read_columns: crate::tracked_state::TrackedStateReadColumns {
                        columns: vec!["entity_pk".to_string()],
                    },
                    limit: Some(1),
                },
            )
            .await
            .expect("limited file scan should apply visibility before limit");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .entity_pk
                .as_single_string_owned()
                .expect("entity pk"),
            "entity-b"
        );
    }

    #[tokio::test]
    async fn reads_resolve_large_payload_refs_via_change_records() {
        let storage = StorageAdapter::new(Memory::new());
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
                .await
                .expect("read should open"),
        );
        let loaded = reader
            .load_rows_at_commit(
                "commit-1",
                &[TrackedStateKey {
                    schema_key: row.schema_key.clone(),
                    entity_pk: row.entity_pk.clone(),
                    file_id: None,
                }],
            )
            .await
            .expect("row should load")
            .pop()
            .flatten()
            .expect("row should exist");
        let scanned = reader
            .scan_rows_at_commit("commit-1", &test_schema_scan_request())
            .await
            .expect("rows should scan");

        assert_eq!(loaded.snapshot_content, row.snapshot_content);
        assert_eq!(scanned[0].snapshot_content, row.snapshot_content);
    }

    #[tokio::test]
    async fn missing_change_record_for_live_row_errors_clearly() {
        let storage = StorageAdapter::new(Memory::new());
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

        // Violate the GC contract: delete the change record while a live
        // tree row still references its change id.
        let mut writes = storage.new_write_set();
        writes.delete(
            crate::changelog::CHANGE_SPACE,
            crate::storage_adapter::StorageKey(bytes::Bytes::from(crate::changelog::change_key(
                row.change_id,
            ))),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("delete should commit");

        let mut reader = tracked_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let error = reader
            .scan_rows_at_commit("commit-1", &test_schema_scan_request())
            .await
            .expect_err("materialization must reject a dangling change id");
        assert!(
            error.message.contains("missing from the changelog"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn inline_threshold_boundary_routes_payloads_deterministically() {
        // 256 bytes inlines into the change record; 257 takes the
        // json_store ref path. Both must read back identically.
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        // row_with_value wraps values as {"value":"<v>"} (12 framing bytes);
        // size the inner strings so the stored payloads land exactly at the
        // threshold and one byte over.
        let rows = [
            row_with_value("entity-at", "change-at", "commit-1", &"a".repeat(256 - 12)),
            row_with_value(
                "entity-over",
                "change-over",
                "commit-1",
                &"b".repeat(257 - 12),
            ),
        ];
        let at_threshold = rows[0].snapshot_content.clone().expect("payload");
        let over_threshold = rows[1].snapshot_content.clone().expect("payload");
        assert_eq!(at_threshold.len(), 256);
        assert_eq!(over_threshold.len(), 257);
        write_root_for_test(&storage, &tracked_state, "commit-1", None, &rows)
            .await
            .expect("root should write");

        let mut reader = tracked_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let scanned = reader
            .scan_rows_at_commit("commit-1", &test_schema_scan_request())
            .await
            .expect("rows should scan");
        let by_pk = |pk: &str| {
            scanned
                .iter()
                .find(|row| row.entity_pk.parts.first().map(String::as_str) == Some(pk))
                .expect("row should exist")
                .snapshot_content
                .clone()
        };
        assert_eq!(by_pk("entity-at").as_deref(), Some(at_threshold.as_str()));
        assert_eq!(
            by_pk("entity-over").as_deref(),
            Some(over_threshold.as_str())
        );
    }

    #[tokio::test]
    async fn commit_root_cache_uses_seen_updated_at_not_change_created_at() {
        let storage = StorageAdapter::new(Memory::new());
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
                    .await
                    .expect("read should open"),
            )
            .load_rows_at_commit(
                "commit-1",
                &[TrackedStateKey {
                    schema_key: row.schema_key.clone(),
                    entity_pk: row.entity_pk.clone(),
                    file_id: None,
                }],
            )
            .await
            .expect("row should load")
            .pop()
            .flatten()
            .expect("row should exist");

        assert_eq!(loaded.created_at, "2026-01-01T00:00:00.000Z");
        assert_eq!(loaded.updated_at, "2026-01-02T00:00:00.000Z");
    }

    #[tokio::test]
    async fn updates_preserve_first_visible_created_at_across_rebuild() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let mut parent = row("entity-a", "change-parent", "parent");
        parent.created_at = "2026-01-01T00:00:00Z".to_string();
        parent.updated_at = "2026-01-01T00:00:00Z".to_string();
        write_root_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            std::slice::from_ref(&parent),
        )
        .await
        .expect("parent root should write");

        let mut child = row("entity-a", "change-child", "child");
        child.created_at = "2026-01-02T00:00:00Z".to_string();
        child.updated_at = "2026-01-03T00:00:00Z".to_string();
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            std::slice::from_ref(&child),
        )
        .await
        .expect("child root should write");

        let key = TrackedStateKey {
            schema_key: child.schema_key.clone(),
            file_id: child.file_id.clone(),
            entity_pk: child.entity_pk.clone(),
        };
        let loaded = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_rows_at_commit("child", std::slice::from_ref(&key))
            .await
            .expect("child row should load")
            .pop()
            .flatten()
            .expect("child row should exist");
        assert_eq!(loaded.created_at, "2026-01-01T00:00:00.000Z");
        assert_eq!(loaded.updated_at, "2026-01-03T00:00:00.000Z");

        {
            let mut writes = storage.new_write_set();
            writes.delete(
                storage::TRACKED_STATE_COMMIT_ROOT_SPACE,
                commit_root_key("child"),
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("child root delete should commit");
        }
        {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut writes = storage.new_write_set();
            tracked_state
                .root_rebuilder(&read, &mut writes)
                .rebuild_commit_root_at("child")
                .await
                .expect("child root should rebuild");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("rebuilt child root should commit");
        }

        let rebuilt = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_rows_at_commit("child", &[key])
            .await
            .expect("rebuilt child row should load")
            .pop()
            .flatten()
            .expect("rebuilt child row should exist");
        assert_eq!(rebuilt.created_at, "2026-01-01T00:00:00.000Z");
        assert_eq!(rebuilt.updated_at, "2026-01-03T00:00:00.000Z");
    }

    #[tokio::test]
    async fn selected_column_scans_do_not_materialize_snapshot_when_snapshot_content_is_omitted() {
        let storage = StorageAdapter::new(Memory::new());
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
                    .await
                    .expect("read should open"),
            )
            .scan_rows_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_string()],
                        ..Default::default()
                    },
                    read_columns: crate::tracked_state::TrackedStateReadColumns {
                        columns: vec!["entity_pk".to_string()],
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
    ) -> (StorageAdapter, TrackedStateContext) {
        let storage = StorageAdapter::new(Memory::new());
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

    fn merge_pick_ids(plan: &TrackedStateMergePlan) -> Vec<String> {
        plan.picks
            .iter()
            .map(|entry| {
                entry
                    .identity()
                    .entity_pk
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
                    .entity_pk
                    .as_single_string_owned()
                    .expect("identity")
            })
            .collect()
    }

    async fn write_root_for_test(
        storage: &StorageAdapter,
        tracked_state: &TrackedStateContext,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        rows: &[MaterializedTrackedStateRow],
    ) -> Result<(), LixError> {
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
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
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await?;
        Ok(())
    }

    async fn delete_root_chunk_for_test(storage: &StorageAdapter, commit_id: &str) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let root_id = storage::load_root(&read, commit_id)
            .await
            .expect("root metadata should load")
            .expect("root metadata should exist");
        let mut writes = storage.new_write_set();
        writes.delete(
            storage::TRACKED_STATE_TREE_CHUNK_SPACE,
            crate::storage_adapter::StorageKey(bytes::Bytes::copy_from_slice(root_id.as_bytes())),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("root chunk delete should commit");
    }

    async fn corrupt_root_chunk_for_test(storage: &StorageAdapter, commit_id: &str) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let root_id = storage::load_root(&read, commit_id)
            .await
            .expect("root metadata should load")
            .expect("root metadata should exist");
        let mut writes = storage.new_write_set();
        writes.put(
            storage::TRACKED_STATE_TREE_CHUNK_SPACE,
            crate::storage_adapter::StorageKey(bytes::Bytes::copy_from_slice(root_id.as_bytes())),
            b"corrupt tracked-state root chunk".as_slice(),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("root chunk corruption should commit");
    }

    async fn overwrite_root_with_rows_for_test(
        storage: &StorageAdapter,
        commit_id: &str,
        rows: &[MaterializedTrackedStateRow],
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mutations = rows
            .iter()
            .map(|row| {
                let key = TrackedStateKey {
                    schema_key: row.schema_key.clone(),
                    file_id: row.file_id.clone(),
                    entity_pk: row.entity_pk.clone(),
                };
                let value = TrackedStateIndexValue {
                    change_id: row.change_id.clone(),
                    commit_id: row.commit_id.clone(),
                    deleted: row.deleted,
                    created_at: crate::common::LixTimestamp::expect_parse(
                        "created_at",
                        &row.created_at,
                    ),
                    updated_at: crate::common::LixTimestamp::expect_parse(
                        "updated_at",
                        &row.updated_at,
                    ),
                };
                TrackedStateMutation::put_encoded(
                    crate::tracked_state::codec::encode_key(&key),
                    crate::tracked_state::codec::encode_value(&value),
                )
            })
            .collect::<Vec<_>>();
        let result = TrackedStateTree::new()
            .apply_mutations(&read, &mut writes, None, mutations, Some(commit_id))
            .await
            .expect("stale root should write");
        storage::stage_commit_root(
            &mut writes,
            &TrackedStateCommitRoot {
                commit_id: CommitId::for_test_label(commit_id),
                root_id: result.root_id,
                parent_roots: Vec::new(),
                changed_key_count: rows.len() as u64,
                row_count_estimate: result.row_count as u64,
                tree_height: result.tree_height as u32,
                primary_chunk_count: result.chunk_count as u64,
                primary_chunk_bytes: result.chunk_bytes as u64,
            },
        )
        .expect("stale metadata should encode");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("stale root overwrite should commit");
    }

    fn test_schema_scan_request() -> TrackedStateScanRequest {
        TrackedStateScanRequest {
            filter: crate::tracked_state::TrackedStateFilter {
                schema_keys: vec!["test_schema".to_string()],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn test_schema_diff_request() -> TrackedStateDiffRequest {
        TrackedStateDiffRequest {
            filter: crate::tracked_state::TrackedStateFilter {
                schema_keys: vec!["test_schema".to_string()],
                ..Default::default()
            },
        }
    }

    fn tombstone(entity_pk: &str, change_id: &str, commit_id: &str) -> MaterializedTrackedStateRow {
        let mut row = row(entity_pk, change_id, commit_id);
        row.snapshot_content = None;
        row
    }

    fn row(entity_pk: &str, change_id: &str, commit_id: &str) -> MaterializedTrackedStateRow {
        row_with_value(entity_pk, change_id, commit_id, "value")
    }

    fn row_with_value(
        entity_pk: &str,
        change_id: &str,
        commit_id: &str,
        value: &str,
    ) -> MaterializedTrackedStateRow {
        MaterializedTrackedStateRow {
            entity_pk: EntityPk::single(entity_pk),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            change_id: ChangeId::for_test_label(change_id),
            commit_id: CommitId::for_test_label(commit_id),
        }
    }
}
