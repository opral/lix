#![allow(
    clippy::cast_possible_truncation,
    clippy::clone_on_copy,
    clippy::match_same_arms,
    clippy::needless_pass_by_ref_mut,
    clippy::redundant_closure,
    clippy::unnecessary_wraps,
    clippy::unused_self
)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;

use async_trait::async_trait;
use bytes::Bytes;

#[cfg(test)]
use super::codec::encode_commit_record;
use super::codec::{
    append_change_record, append_commit_record, append_transaction_change_record,
    decode_change_record,
};
use super::store::{
    CHANGE_SPACE, COMMIT_CHANGE_ID_INDEX_FORMAT_KEY, COMMIT_CHANGE_ID_INDEX_FORMAT_VALUE,
    COMMIT_CHANGE_ID_SPACE, COMMIT_SPACE, change_id_from_key, change_key,
    commit_change_id_index_format_key, commit_change_id_key, commit_id_from_key, commit_key,
};
#[cfg(test)]
use super::store::{commit_change_id_index_format_value, commit_change_id_value};
use crate::changelog::{
    ChangeId, ChangeLoadBatch, ChangeLoadRequest, ChangeRecord, ChangeScanBatch, ChangeScanRequest,
    ChangelogAppend, ChangelogReader, ChangelogWriter, CommitId, CommitLoadBatch,
    CommitLoadRequest, CommitRecord, CommitScanBatch, CommitScanRequest,
    TransactionChangelogAppend,
};
use crate::changelog::{GcPlan, GcRoot};
use crate::json_store::{JsonRef, JsonSlot, JsonSlotRef, JsonStoreContext};
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    BufferRange, EncodedMutationBatch, EncodedPut, PointReadPlan, ScanPlan, StorageAdapter,
    StorageAdapterRead, StorageCoreProjection, StorageGetManyRequest, StorageGetOptions,
    StorageKey, StoragePrefix, StorageProjectedValue, StorageReadOptions, StorageScanOptions,
    StorageSpace, StorageWriteSet,
};
use crate::{LixError, storage_codec};

const SCAN_PAGE_LIMIT: usize = 1024;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ChangelogContext;

impl ChangelogContext {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn reader<S>(&self, store: S) -> ChangelogStoreReader<S>
    where
        S: ChangelogStorageRead,
    {
        ChangelogStoreReader { store }
    }

    pub(crate) fn writer<'a, S>(
        &self,
        store: &'a mut S,
        writes: &'a mut StorageWriteSet,
    ) -> ChangelogStoreWriter<'a, S>
    where
        S: ChangelogStorageRead + ?Sized,
    {
        ChangelogStoreWriter {
            store,
            writes,
            staged_commits: HashMap::new(),
            staged_changes: HashMap::new(),
            staged_change_deletes: HashSet::new(),
        }
    }
}

pub(crate) struct ChangelogStoreReader<S> {
    store: S,
}

pub(crate) struct ChangelogStoreWriter<'a, S: ?Sized> {
    store: &'a mut S,
    writes: &'a mut StorageWriteSet,
    staged_commits: HashMap<CommitId, CommitRecord>,
    staged_changes: HashMap<ChangeId, ChangeRecord>,
    staged_change_deletes: HashSet<ChangeId>,
}

struct EncodedChangelogBatch {
    key_bytes: Vec<u8>,
    value_bytes: Vec<u8>,
    puts: Vec<EncodedPut>,
}

impl EncodedChangelogBatch {
    fn with_capacity(puts: usize, key_bytes: usize, value_bytes: usize) -> Self {
        Self {
            key_bytes: Vec::with_capacity(key_bytes),
            value_bytes: Vec::with_capacity(value_bytes),
            puts: Vec::with_capacity(puts),
        }
    }

    fn try_put(
        &mut self,
        key: &[u8],
        encode_value: impl FnOnce(&mut Vec<u8>) -> Result<std::ops::Range<usize>, LixError>,
    ) -> Result<(), LixError> {
        let value = encode_value(&mut self.value_bytes)?;
        self.put_range(key, value);
        Ok(())
    }

    fn put(&mut self, key: &[u8], value: &[u8]) {
        let start = self.value_bytes.len();
        self.value_bytes.extend_from_slice(value);
        self.put_range(key, start..self.value_bytes.len());
    }

    fn put_range(&mut self, key: &[u8], value: std::ops::Range<usize>) {
        let key_start = self.key_bytes.len();
        self.key_bytes.extend_from_slice(key);
        self.puts.push(EncodedPut {
            key: BufferRange::new(key_start, self.key_bytes.len() - key_start),
            value: BufferRange::new(value.start, value.end - value.start),
        });
    }

    fn stage(self, writes: &mut StorageWriteSet, space: StorageSpace) {
        if self.puts.is_empty() {
            return;
        }
        let batch = EncodedMutationBatch::try_new(
            Bytes::from(self.key_bytes),
            Bytes::from(self.value_bytes),
            self.puts,
            Vec::new(),
        )
        .expect("changelog ranges originate in the supplied encoded buffers");
        writes.stage_encoded_batch(space, batch);
    }
}

fn transaction_change_value_capacity(
    change: &crate::changelog::TransactionChangeRecordRef<'_>,
) -> usize {
    96usize
        .saturating_add(change.schema_key.len())
        .saturating_add(change.entity_pk.estimated_heap_bytes())
        .saturating_add(change.file_id.map_or(0, str::len))
        .saturating_add(change.origin_key.map_or(0, str::len))
        .saturating_add(json_slot_ref_value_capacity(change.snapshot))
        .saturating_add(json_slot_ref_value_capacity(change.metadata))
}

fn change_value_capacity(change: &ChangeRecord) -> usize {
    let change = crate::changelog::TransactionChangeRecordRef::from(change);
    transaction_change_value_capacity(&change)
}

fn json_slot_ref_value_capacity(slot: JsonSlotRef<'_>) -> usize {
    match slot {
        JsonSlotRef::None => 1,
        JsonSlotRef::Ref(_) => 33,
        JsonSlotRef::Inline(json) => json.len().saturating_add(9),
    }
}

fn commit_value_capacity(commit: &CommitRecord) -> usize {
    96usize
        .saturating_add(commit.parent_commit_ids.len().saturating_mul(16))
        .saturating_add(
            commit
                .author_account_ids
                .iter()
                .map(String::len)
                .sum::<usize>(),
        )
}

#[derive(Debug)]
pub(crate) struct ChangelogScanPage {
    pub(super) keys: Vec<Vec<u8>>,
    pub(super) values: Vec<Vec<u8>>,
    pub(super) resume_after: Option<Vec<u8>>,
}

#[async_trait]
pub(crate) trait ChangelogStorageRead {
    async fn changelog_get_many(
        &mut self,
        space: StorageSpace,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, LixError>;

    async fn changelog_get_many_batch(
        &mut self,
        requests: Vec<(StorageSpace, Vec<Vec<u8>>)>,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, LixError>;

    async fn changelog_scan(
        &mut self,
        space: StorageSpace,
        prefix: Vec<u8>,
        after: Option<Vec<u8>>,
        limit: usize,
        projection: StorageCoreProjection,
    ) -> Result<ChangelogScanPage, LixError>;
}

#[async_trait]
impl<T> ChangelogStorageRead for T
where
    T: StorageAdapterRead + Send,
{
    async fn changelog_get_many(
        &mut self,
        space: StorageSpace,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, LixError> {
        native_get_many(self, space, keys).await
    }

    async fn changelog_get_many_batch(
        &mut self,
        requests: Vec<(StorageSpace, Vec<Vec<u8>>)>,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, LixError> {
        native_get_many_batch(self, requests).await
    }

    async fn changelog_scan(
        &mut self,
        space: StorageSpace,
        prefix: Vec<u8>,
        after: Option<Vec<u8>>,
        limit: usize,
        projection: StorageCoreProjection,
    ) -> Result<ChangelogScanPage, LixError> {
        native_scan(self, space, prefix, after, limit, projection).await
    }
}

#[async_trait]
impl<StorageImpl> ChangelogStorageRead for StorageAdapter<StorageImpl>
where
    StorageImpl: Storage + Send,
{
    async fn changelog_get_many(
        &mut self,
        space: StorageSpace,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, LixError> {
        let mut read = self.begin_read(StorageReadOptions::default()).await?;
        native_get_many(&mut read, space, keys).await
    }

    async fn changelog_get_many_batch(
        &mut self,
        requests: Vec<(StorageSpace, Vec<Vec<u8>>)>,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, LixError> {
        let mut read = self.begin_read(StorageReadOptions::default()).await?;
        native_get_many_batch(&mut read, requests).await
    }

    async fn changelog_scan(
        &mut self,
        space: StorageSpace,
        prefix: Vec<u8>,
        after: Option<Vec<u8>>,
        limit: usize,
        projection: StorageCoreProjection,
    ) -> Result<ChangelogScanPage, LixError> {
        let mut read = self.begin_read(StorageReadOptions::default()).await?;
        native_scan(&mut read, space, prefix, after, limit, projection).await
    }
}

#[async_trait]
impl<S> ChangelogReader for ChangelogStoreReader<S>
where
    S: ChangelogStorageRead + Send,
{
    async fn load_commits(
        &mut self,
        request: CommitLoadRequest<'_>,
    ) -> Result<CommitLoadBatch, LixError> {
        load_commits_from_store(&mut self.store, request).await
    }

    async fn scan_commits(
        &mut self,
        request: CommitScanRequest<'_>,
    ) -> Result<CommitScanBatch, LixError> {
        scan_commits_from_store(&mut self.store, request).await
    }

    async fn load_changes(
        &mut self,
        request: ChangeLoadRequest<'_>,
    ) -> Result<ChangeLoadBatch, LixError> {
        load_changes_from_store(&mut self.store, request).await
    }

    async fn scan_changes(
        &mut self,
        request: ChangeScanRequest<'_>,
    ) -> Result<ChangeScanBatch, LixError> {
        scan_changes_from_store(&mut self.store, request).await
    }
}

#[async_trait]
impl<S> ChangelogReader for ChangelogStoreWriter<'_, S>
where
    S: ChangelogStorageRead + Send + ?Sized,
{
    async fn load_commits(
        &mut self,
        request: CommitLoadRequest<'_>,
    ) -> Result<CommitLoadBatch, LixError> {
        let stored = load_commits_from_store(self.store, request).await?;
        let entries = request
            .commit_ids
            .iter()
            .zip(stored.entries)
            .map(|(commit_id, stored)| {
                if let Some(record) = self.staged_commits.get(commit_id) {
                    return Some(record.clone());
                }
                stored
            })
            .collect();
        Ok(CommitLoadBatch { entries })
    }

    async fn scan_commits(
        &mut self,
        request: CommitScanRequest<'_>,
    ) -> Result<CommitScanBatch, LixError> {
        let mut batch = scan_commits_from_store(self.store, request).await?;
        let mut staged = self
            .staged_commits
            .values()
            .filter(|commit| {
                request
                    .start_after
                    .map(|start_after| commit.commit_id.to_string().as_str() > start_after)
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        staged.sort_by_key(|left| left.commit_id);
        batch.entries.extend(staged);
        batch.entries.sort_by_key(|commit| commit.commit_id);
        let limit = request.limit.unwrap_or(usize::MAX);
        if batch.entries.len() > limit {
            batch.entries.truncate(limit);
            batch.next_start_after = batch.entries.last().map(|commit| commit.commit_id);
        }
        Ok(batch)
    }

    async fn load_changes(
        &mut self,
        request: ChangeLoadRequest<'_>,
    ) -> Result<ChangeLoadBatch, LixError> {
        let stored = load_changes_from_store(self.store, request).await?;
        let entries = request
            .change_ids
            .iter()
            .zip(stored.entries)
            .map(|(change_id, stored)| self.staged_changes.get(change_id).cloned().or(stored))
            .collect();
        Ok(ChangeLoadBatch { entries })
    }

    async fn scan_changes(
        &mut self,
        request: ChangeScanRequest<'_>,
    ) -> Result<ChangeScanBatch, LixError> {
        let mut batch = scan_changes_from_store(self.store, request).await?;
        let mut staged = self
            .staged_changes
            .values()
            .filter(|change| {
                request
                    .start_after
                    .map(|start_after| change.change_id.to_string().as_str() > start_after)
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        staged.sort_by_key(|left| left.change_id);
        batch.entries.extend(staged);
        batch.entries.sort_by_key(|left| left.change_id);
        batch
            .entries
            .dedup_by(|left, right| left.change_id == right.change_id);
        let limit = request.limit.unwrap_or(usize::MAX);
        if batch.entries.len() > limit {
            batch.entries.truncate(limit);
            batch.next_start_after = batch.entries.last().map(|change| change.change_id);
        }
        Ok(batch)
    }
}

#[async_trait]
impl<S> ChangelogWriter for ChangelogStoreWriter<'_, S>
where
    S: ChangelogStorageRead + Send + ?Sized,
{
    async fn stage_append(&mut self, append: ChangelogAppend) -> Result<(), LixError> {
        self.ensure_changelog_mutation_is_allowed()?;
        let stage_commit_change_id_index_format = self.validate_append(&append).await?;
        self.stage_append_records(append, stage_commit_change_id_index_format)
    }

    async fn stage_delete_standalone_changes(
        &mut self,
        change_ids: &[ChangeId],
    ) -> Result<(), LixError> {
        self.ensure_changelog_mutation_is_allowed()?;
        let change_ids = change_ids.iter().copied().collect::<HashSet<_>>();
        for change_id in &change_ids {
            if self.staged_changes.contains_key(change_id) {
                return Err(LixError::unknown(format!(
                    "cannot delete changelog change '{change_id}' because it was staged in the same transaction"
                )));
            }
        }
        for change_id in change_ids {
            if self.staged_change_deletes.insert(change_id) {
                self.writes.delete(CHANGE_SPACE, change_key(change_id));
            }
        }
        Ok(())
    }
}

impl<S> ChangelogStoreWriter<'_, S>
where
    S: ChangelogStorageRead + Send + ?Sized,
{
    /// Stages a terminal append assembled from already-prepared transaction rows.
    ///
    /// The transaction owns ID generation, parent selection, and change-ref
    /// construction. This path has no read-your-writes overlay: its writer is
    /// dropped immediately after the append, so retaining a second owned copy
    /// of every row would only add allocation and drop work. Direct changelog
    /// callers continue to use `stage_append`.
    pub(crate) fn stage_transaction_append(
        &mut self,
        append: TransactionChangelogAppend<'_>,
    ) -> Result<(), LixError> {
        self.ensure_changelog_mutation_is_allowed()?;
        let TransactionChangelogAppend { commits, changes } = append;
        let mut change_batch = EncodedChangelogBatch::with_capacity(
            changes.len(),
            changes.len() * 16,
            changes.iter().map(transaction_change_value_capacity).sum(),
        );
        let mut commit_batch = EncodedChangelogBatch::with_capacity(
            commits.len(),
            commits.len() * 16,
            commits.iter().map(commit_value_capacity).sum(),
        );
        let mut commit_change_id_batch = EncodedChangelogBatch::with_capacity(
            commits.len(),
            commits.len() * 16,
            commits.len() * 16,
        );

        for change in &changes {
            change_batch.try_put(change.change_id.as_uuid().as_bytes(), |bytes| {
                append_transaction_change_record(bytes, change)
            })?;
        }
        for commit in &commits {
            commit_batch.try_put(commit.commit_id.as_uuid().as_bytes(), |bytes| {
                append_commit_record(bytes, commit)
            })?;
            commit_change_id_batch.put(
                commit.change_id.as_uuid().as_bytes(),
                commit.commit_id.as_uuid().as_bytes(),
            );
        }
        change_batch.stage(self.writes, CHANGE_SPACE);
        commit_batch.stage(self.writes, COMMIT_SPACE);
        commit_change_id_batch.stage(self.writes, COMMIT_CHANGE_ID_SPACE);
        Ok(())
    }

    fn stage_append_records(
        &mut self,
        append: ChangelogAppend,
        stage_commit_change_id_index_format: bool,
    ) -> Result<(), LixError> {
        let ChangelogAppend { commits, changes } = append;
        let mut change_batch = EncodedChangelogBatch::with_capacity(
            changes.len(),
            changes.len() * 16,
            changes.iter().map(change_value_capacity).sum(),
        );
        let mut commit_batch = EncodedChangelogBatch::with_capacity(
            commits.len(),
            commits.len() * 16,
            commits.iter().map(commit_value_capacity).sum(),
        );
        let reverse_count = commits.len() + usize::from(stage_commit_change_id_index_format);
        let mut commit_change_id_batch = EncodedChangelogBatch::with_capacity(
            reverse_count,
            commits.len() * 16 + COMMIT_CHANGE_ID_INDEX_FORMAT_KEY.len(),
            commits.len() * 16 + COMMIT_CHANGE_ID_INDEX_FORMAT_VALUE.len(),
        );
        if stage_commit_change_id_index_format {
            commit_change_id_batch.put(
                COMMIT_CHANGE_ID_INDEX_FORMAT_KEY,
                COMMIT_CHANGE_ID_INDEX_FORMAT_VALUE,
            );
        }
        for change in &changes {
            change_batch.try_put(change.change_id.as_uuid().as_bytes(), |bytes| {
                append_change_record(bytes, change)
            })?;
        }
        for commit in &commits {
            commit_batch.try_put(commit.commit_id.as_uuid().as_bytes(), |bytes| {
                append_commit_record(bytes, commit)
            })?;
            commit_change_id_batch.put(
                commit.change_id.as_uuid().as_bytes(),
                commit.commit_id.as_uuid().as_bytes(),
            );
        }
        change_batch.stage(self.writes, CHANGE_SPACE);
        commit_batch.stage(self.writes, COMMIT_SPACE);
        commit_change_id_batch.stage(self.writes, COMMIT_CHANGE_ID_SPACE);

        self.staged_changes.reserve(changes.len());
        self.staged_commits.reserve(commits.len());
        for change in changes {
            self.staged_changes.insert(change.change_id, change);
        }
        for commit in commits {
            self.staged_commits.insert(commit.commit_id, commit);
        }
        Ok(())
    }
    fn ensure_changelog_mutation_is_allowed(&self) -> Result<(), LixError> {
        if !self.writes.changelog_gc_is_sealed() {
            return Ok(());
        }
        Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "cannot stage changelog mutations after garbage collection in the same transaction",
        ))
    }

    async fn validate_append(&mut self, append: &ChangelogAppend) -> Result<bool, LixError> {
        validate_unique(
            append.commits.iter().map(|commit| commit.commit_id),
            "commit_id",
        )?;
        validate_unique(
            append.changes.iter().map(|change| change.change_id),
            "change_id",
        )?;
        validate_unique(
            append.commits.iter().map(|commit| commit.change_id),
            "commit change_id",
        )?;

        let append_commit_ids = append
            .commits
            .iter()
            .map(|commit| commit.commit_id)
            .collect::<HashSet<_>>();
        let append_changes = append
            .changes
            .iter()
            .map(|change| (change.change_id, change))
            .collect::<HashMap<_, _>>();

        if let Some(change_id) = append_changes
            .keys()
            .find(|change_id| self.staged_change_deletes.contains(change_id))
        {
            return Err(LixError::unknown(format!(
                "cannot append changelog change '{change_id}' because it was deleted in the same transaction"
            )));
        }

        let stage_commit_change_id_index_format = self
            .reject_existing_id_collisions(append, &append_commit_ids, &append_changes)
            .await?;
        self.validate_parent_commits(append, &append_commit_ids)
            .await?;

        Ok(stage_commit_change_id_index_format)
    }

    async fn reject_existing_id_collisions(
        &mut self,
        append: &ChangelogAppend,
        append_commit_ids: &HashSet<CommitId>,
        append_changes: &HashMap<ChangeId, &ChangeRecord>,
    ) -> Result<bool, LixError> {
        let commit_ids = append_commit_ids.iter().copied().collect::<Vec<_>>();
        let commit_keys = commit_ids
            .iter()
            .map(|commit_id| commit_key(*commit_id))
            .collect::<Vec<_>>();
        let commit_change_ids = append
            .commits
            .iter()
            .map(|commit| commit.change_id)
            .collect::<Vec<_>>();
        let append_change_ids = append_changes.keys().copied().collect::<Vec<_>>();
        let change_keys = append_change_ids
            .iter()
            .chain(&commit_change_ids)
            .map(|change_id| change_key(*change_id))
            .collect::<Vec<_>>();
        let index_format_key = commit_change_id_index_format_key();
        let index_format_is_staged = self
            .writes
            .contains_put(COMMIT_CHANGE_ID_SPACE, &index_format_key);
        let mut index_keys = Vec::with_capacity(commit_change_ids.len() + 1);
        index_keys.push(index_format_key);
        index_keys.extend(
            commit_change_ids
                .iter()
                .map(|change_id| commit_change_id_key(*change_id)),
        );
        let mut batches = self
            .store
            .changelog_get_many_batch(vec![
                (COMMIT_SPACE, commit_keys),
                (CHANGE_SPACE, change_keys),
                (COMMIT_CHANGE_ID_SPACE, index_keys),
            ])
            .await?
            .into_iter();
        let existing_commits = batches
            .next()
            .expect("commit validation batch was requested");
        let existing_changes = batches
            .next()
            .expect("change validation batch was requested");
        let mut index_values = batches
            .next()
            .expect("commit change-id validation batch was requested")
            .into_iter();
        let unexpected_batch = batches.next();
        debug_assert!(unexpected_batch.is_none());
        for (commit_id, found) in commit_ids.iter().zip(existing_commits) {
            if found.is_some() || self.staged_commits.contains_key(commit_id) {
                return Err(LixError::unknown(format!(
                    "changelog commit '{commit_id}' already exists"
                )));
            }
        }
        let (existing_append_changes, existing_commit_changes) =
            existing_changes.split_at(append_change_ids.len());
        for (change_id, found) in append_change_ids.iter().zip(existing_append_changes) {
            if found.is_some() || self.staged_changes.contains_key(change_id) {
                return Err(LixError::unknown(format!(
                    "changelog change '{change_id}' already exists"
                )));
            }
        }
        if append.commits.is_empty() {
            return Ok(false);
        }
        for ((commit, change_id), existing_change) in append
            .commits
            .iter()
            .zip(commit_change_ids.iter())
            .zip(existing_commit_changes)
        {
            if append_changes.contains_key(change_id)
                || existing_change.is_some()
                || self.staged_changes.contains_key(change_id)
                || self
                    .staged_commits
                    .values()
                    .any(|staged| staged.change_id == *change_id)
            {
                return Err(LixError::unknown(format!(
                    "changelog commit '{}' derived change_id '{}' collides with an existing change id",
                    commit.commit_id, commit.change_id
                )));
            }
        }
        let stored_format = index_values
            .next()
            .expect("commit change-id index format key was requested");
        let stage_commit_change_id_index_format = match stored_format {
            Some(value) if value.as_slice() == COMMIT_CHANGE_ID_INDEX_FORMAT_VALUE => false,
            Some(_) => {
                return Err(LixError::unknown(
                    "changelog commit_change_id index has an unsupported format; recreate the repository",
                ));
            }
            None if index_format_is_staged => false,
            None => {
                let existing_commits = self
                    .store
                    .changelog_scan(
                        COMMIT_SPACE,
                        Vec::new(),
                        None,
                        1,
                        StorageCoreProjection::KeyOnly,
                    )
                    .await?;
                if !existing_commits.keys.is_empty() {
                    return Err(LixError::unknown(
                        "changelog commit_change_id index is missing for an existing repository; recreate the repository before appending commits",
                    ));
                }
                true
            }
        };
        for (change_id, existing_commit) in commit_change_ids.iter().zip(index_values) {
            if existing_commit.is_some() {
                return Err(LixError::unknown(format!(
                    "changelog commit derived change_id '{change_id}' already exists"
                )));
            }
        }
        Ok(stage_commit_change_id_index_format)
    }

    async fn validate_parent_commits(
        &mut self,
        append: &ChangelogAppend,
        append_commit_ids: &HashSet<CommitId>,
    ) -> Result<(), LixError> {
        let parent_ids = append
            .commits
            .iter()
            .flat_map(|commit| commit.parent_commit_ids.iter().copied())
            .filter(|parent_id| !append_commit_ids.contains(parent_id))
            .collect::<HashSet<_>>();
        let keys = parent_ids
            .iter()
            .map(|id| commit_key(*id))
            .collect::<Vec<_>>();
        for (parent_id, found) in parent_ids
            .iter()
            .zip(get_many(self.store, COMMIT_SPACE, keys).await?)
        {
            if found.is_none() && !self.staged_commits.contains_key(parent_id) {
                return Err(LixError::unknown(format!(
                    "changelog parent commit '{parent_id}' does not exist"
                )));
            }
        }
        Ok(())
    }
}

async fn load_commits_from_store(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    request: CommitLoadRequest<'_>,
) -> Result<CommitLoadBatch, LixError> {
    let keys = request
        .commit_ids
        .iter()
        .map(|commit_id| commit_key(*commit_id))
        .collect::<Vec<_>>();
    let commit_values = get_many(store, COMMIT_SPACE, keys).await?;
    let mut entries = Vec::with_capacity(request.commit_ids.len());
    for (_commit_id, value) in request.commit_ids.iter().zip(commit_values) {
        let Some(value) = value else {
            entries.push(None);
            continue;
        };
        let record = storage_codec::decode("commit record", &value)?;
        entries.push(Some(record));
    }
    Ok(CommitLoadBatch { entries })
}

async fn scan_commits_from_store(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    request: CommitScanRequest<'_>,
) -> Result<CommitScanBatch, LixError> {
    let limit = request.limit.unwrap_or(SCAN_PAGE_LIMIT);
    if limit == 0 {
        return Ok(CommitScanBatch {
            entries: Vec::new(),
            next_start_after: request
                .start_after
                .map(|id| CommitId::parse_lix(id, "commit scan start_after"))
                .transpose()?,
        });
    }
    let page = store
        .changelog_scan(
            COMMIT_SPACE,
            Vec::new(),
            request
                .start_after
                .map(|id| CommitId::parse_lix(id, "commit scan start_after").map(commit_key))
                .transpose()?,
            limit,
            StorageCoreProjection::FullValue,
        )
        .await?;
    let mut entries = Vec::with_capacity(page.values.len());
    for (key, value) in page.keys.iter().zip(page.values.iter()) {
        let record: CommitRecord = storage_codec::decode("commit record", value)?;
        if key.as_slice() != commit_key(record.commit_id).as_slice() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "changelog commit scan key does not match decoded commit_id '{}'",
                    record.commit_id
                ),
            ));
        }
        entries.push(record);
    }
    let next_start_after = page
        .resume_after
        .map(|key| commit_id_from_key(&key))
        .transpose()?;
    Ok(CommitScanBatch {
        entries,
        next_start_after,
    })
}

async fn load_changes_from_store(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    request: ChangeLoadRequest<'_>,
) -> Result<ChangeLoadBatch, LixError> {
    let keys = request
        .change_ids
        .iter()
        .map(|change_id| change_key(*change_id))
        .collect::<Vec<_>>();
    let entries = get_many(store, CHANGE_SPACE, keys)
        .await?
        .into_iter()
        .zip(request.change_ids.iter())
        .map(|(value, change_id)| {
            value
                .as_deref()
                .map(|value| decode_change_record(value, *change_id))
                .transpose()
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    Ok(ChangeLoadBatch { entries })
}

async fn scan_changes_from_store(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    request: ChangeScanRequest<'_>,
) -> Result<ChangeScanBatch, LixError> {
    let limit = request.limit.unwrap_or(SCAN_PAGE_LIMIT);
    if limit == 0 {
        return Ok(ChangeScanBatch {
            entries: Vec::new(),
            next_start_after: request
                .start_after
                .map(|id| ChangeId::parse_lix(id, "change scan start_after"))
                .transpose()?,
        });
    }
    let page = store
        .changelog_scan(
            CHANGE_SPACE,
            Vec::new(),
            request
                .start_after
                .map(|id| ChangeId::parse_lix(id, "change scan start_after").map(change_key))
                .transpose()?,
            limit,
            StorageCoreProjection::FullValue,
        )
        .await?;
    let mut entries = Vec::with_capacity(page.values.len());
    for (key, value) in page.keys.iter().zip(page.values.iter()) {
        // change_id lives in the key; the stored value omits it.
        let change_id = change_id_from_key(key)?;
        entries.push(decode_change_record(value, change_id)?);
    }
    let next_start_after = page
        .resume_after
        .map(|key| change_id_from_key(&key))
        .transpose()?;
    Ok(ChangeScanBatch {
        entries,
        next_start_after,
    })
}

fn validate_unique<T>(values: impl IntoIterator<Item = T>, label: &str) -> Result<(), LixError>
where
    T: fmt::Display,
{
    let mut seen = HashSet::new();
    for value in values {
        if !seen.insert(value.to_string()) {
            return Err(LixError::unknown(format!(
                "changelog append contains duplicate {label} '{value}'"
            )));
        }
    }
    Ok(())
}

async fn get_many(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    space: StorageSpace,
    keys: Vec<Vec<u8>>,
) -> Result<Vec<Option<Vec<u8>>>, LixError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    store.changelog_get_many(space, keys).await
}

async fn native_get_many<R>(
    read: &mut R,
    space: StorageSpace,
    keys: Vec<Vec<u8>>,
) -> Result<Vec<Option<Vec<u8>>>, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let keys = keys
        .into_iter()
        .map(|key| StorageKey(Bytes::from(key)))
        .collect::<Vec<_>>();
    let result = PointReadPlan::new(space, &keys)
        .materialize(read, StorageGetOptions::default())
        .await?;
    Ok(result
        .value
        .into_iter()
        .map(|value| match value {
            Some(StorageProjectedValue::FullValue(bytes)) => Some(bytes.to_vec()),
            Some(StorageProjectedValue::KeyOnly) => Some(Vec::new()),
            None => None,
        })
        .collect())
}

async fn native_get_many_batch<R>(
    read: &mut R,
    requests: Vec<(StorageSpace, Vec<Vec<u8>>)>,
) -> Result<Vec<Vec<Option<Vec<u8>>>>, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let owned_requests = requests
        .into_iter()
        .map(|(space, keys)| {
            (
                space,
                keys.into_iter()
                    .map(|key| StorageKey(Bytes::from(key)))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    let requests = owned_requests
        .iter()
        .map(|(space, keys)| StorageGetManyRequest {
            space: *space,
            keys,
            opts: StorageGetOptions::default(),
        })
        .collect::<Vec<_>>();
    let mut values = read.get_many(&requests).await?.values.into_iter();
    Ok(requests
        .iter()
        .map(|request| {
            values
                .by_ref()
                .take(request.keys.len())
                .map(|value| match value {
                    Some(StorageProjectedValue::FullValue(bytes)) => Some(bytes.to_vec()),
                    Some(StorageProjectedValue::KeyOnly) => Some(Vec::new()),
                    None => None,
                })
                .collect()
        })
        .collect())
}

async fn native_scan<R>(
    read: &mut R,
    space: StorageSpace,
    prefix: Vec<u8>,
    after: Option<Vec<u8>>,
    limit: usize,
    projection: StorageCoreProjection,
) -> Result<ChangelogScanPage, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let after_key = after.map(|key| StorageKey(Bytes::from(key)));
    let opts = StorageScanOptions {
        projection,
        limit_rows: limit,
        resume_after: after_key,
    };
    let chunk = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::from(prefix),
        },
    )
    .collect(read, opts)
    .await?
    .value;
    let has_more = chunk.has_more;
    let mut keys = Vec::with_capacity(chunk.entries.len());
    let mut values = Vec::with_capacity(chunk.entries.len());
    for entry in chunk.entries {
        keys.push(entry.key.0.to_vec());
        if let StorageProjectedValue::FullValue(bytes) = entry.value {
            values.push(bytes.to_vec());
        }
    }
    let resume_after = has_more.then(|| keys.last().cloned()).flatten();
    Ok(ChangelogScanPage {
        keys,
        values,
        resume_after,
    })
}
