#![allow(
    clippy::cast_possible_truncation,
    clippy::clone_on_copy,
    clippy::match_same_arms,
    clippy::needless_pass_by_ref_mut,
    clippy::redundant_closure,
    clippy::unnecessary_wraps,
    clippy::unused_self
)]

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::Bound;

use async_trait::async_trait;
use bytes::Bytes;

use super::codec::{
    append_change_record, append_commit_record, append_transaction_change_record,
    decode_change_record,
};
use super::store::{
    CHANGE_SPACE, COMMIT_SPACE, change_id_from_key, change_key, commit_id_from_key, commit_key,
};
use crate::changelog::{
    ChangeId, ChangeLoadBatch, ChangeLoadRequest, ChangeRecord, ChangeScanBatch, ChangeScanRequest,
    ChangelogAppend, ChangelogReader, ChangelogWriter, CommitId, CommitLoadBatch,
    CommitLoadRequest, CommitRecord, CommitScanBatch, CommitScanRequest,
    TransactionChangelogAppend,
};
use crate::json_store::JsonSlotRef;
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    BufferRange, EncodedMutationBatch, EncodedPut, PointReadPlan, StorageAdapter,
    StorageAdapterRead, StorageBeginScanOptions, StorageCoreProjection, StorageGetManyRequest,
    StorageGetOptions, StorageKey, StoragePrefix, StorageProjectedValue, StorageReadOptions,
    StorageSpace, StorageWriteSet, exact_get_many,
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
        .saturating_add(change.row_pk.estimated_heap_bytes())
        .saturating_add(change.file_id.map_or(0, str::len))
        .saturating_add(change.origin_key.map_or(0, str::len))
        .saturating_add(json_slot_ref_value_capacity(change.snapshot))
        .saturating_add(json_slot_ref_value_capacity(change.metadata))
}

fn change_value_capacity(change: &ChangeRecord) -> Result<usize, LixError> {
    let change = crate::changelog::TransactionChangeRecordRef::try_from(change)?;
    Ok(transaction_change_value_capacity(&change))
}

fn json_slot_ref_value_capacity(slot: JsonSlotRef<'_>) -> usize {
    match slot {
        JsonSlotRef::None => 1,
        JsonSlotRef::Ref(_) => 33,
        JsonSlotRef::Inline(json) => json.len().saturating_add(9),
    }
}

fn commit_value_capacity(commit: &CommitRecord) -> usize {
    113usize.saturating_add(commit.parent_commit_ids.len().saturating_mul(16))
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
    async fn load_commits<'a>(
        &mut self,
        request: CommitLoadRequest<'a>,
    ) -> Result<CommitLoadBatch<'a>, LixError> {
        load_commits_from_store(&mut self.store, request).await
    }

    async fn scan_commits(
        &mut self,
        request: CommitScanRequest<'_>,
    ) -> Result<CommitScanBatch, LixError> {
        scan_commits_from_store(&mut self.store, request).await
    }

    async fn load_changes<'a>(
        &mut self,
        request: ChangeLoadRequest<'a>,
    ) -> Result<ChangeLoadBatch<'a>, LixError> {
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
    async fn load_commits<'a>(
        &mut self,
        request: CommitLoadRequest<'a>,
    ) -> Result<CommitLoadBatch<'a>, LixError> {
        let stored = load_commits_from_store(self.store, request).await?;
        let entries = stored
            .into_iter()
            .map(|(commit_id, stored)| {
                if let Some(record) = self.staged_commits.get(commit_id) {
                    return Some(record.clone());
                }
                stored
            })
            .collect();
        CommitLoadBatch::try_new("changelog commit overlay", request.commit_ids, entries)
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

    async fn load_changes<'a>(
        &mut self,
        request: ChangeLoadRequest<'a>,
    ) -> Result<ChangeLoadBatch<'a>, LixError> {
        let stored = load_changes_from_store(self.store, request).await?;
        let entries = stored
            .into_iter()
            .map(|(change_id, stored)| self.staged_changes.get(change_id).cloned().or(stored))
            .collect();
        ChangeLoadBatch::try_new("changelog change overlay", request.change_ids, entries)
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
        self.validate_append(&append).await?;
        self.stage_append_records(append)
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
        for change in &changes {
            change_batch.try_put(change.change_id.as_uuid().as_bytes(), |bytes| {
                append_transaction_change_record(bytes, change)
            })?;
        }
        for commit in &commits {
            commit_batch.try_put(commit.commit_id.as_uuid().as_bytes(), |bytes| {
                append_commit_record(bytes, commit)
            })?;
        }
        change_batch.stage(self.writes, CHANGE_SPACE);
        commit_batch.stage(self.writes, COMMIT_SPACE);
        Ok(())
    }

    fn stage_append_records(&mut self, append: ChangelogAppend) -> Result<(), LixError> {
        let ChangelogAppend { commits, changes } = append;
        let change_value_bytes = changes
            .iter()
            .map(change_value_capacity)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum();
        let mut change_batch = EncodedChangelogBatch::with_capacity(
            changes.len(),
            changes.len() * 16,
            change_value_bytes,
        );
        let mut commit_batch = EncodedChangelogBatch::with_capacity(
            commits.len(),
            commits.len() * 16,
            commits.iter().map(commit_value_capacity).sum(),
        );
        for change in &changes {
            change_batch.try_put(change.change_id.as_uuid().as_bytes(), |bytes| {
                append_change_record(bytes, change)
            })?;
        }
        for commit in &commits {
            commit_batch.try_put(commit.commit_id.as_uuid().as_bytes(), |bytes| {
                append_commit_record(bytes, commit)
            })?;
        }
        change_batch.stage(self.writes, CHANGE_SPACE);
        commit_batch.stage(self.writes, COMMIT_SPACE);

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

    async fn validate_append(&mut self, append: &ChangelogAppend) -> Result<(), LixError> {
        validate_unique(
            append.commits.iter().map(|commit| commit.commit_id),
            "commit_id",
        )?;
        validate_unique(
            append.changes.iter().map(|change| change.change_id),
            "change_id",
        )?;
        // A commit's change id is a bijection of its commit id, so the
        // `commit_id` uniqueness check above already covers it. That bijection
        // is only collision-free against the commit's own packed member changes
        // while the low 32 bits stay reserved, so require it here rather than
        // trusting every caller to have minted the id through
        // `CommitId::with_change_address_space`.
        for commit in &append.commits {
            if commit.commit_id.as_uuid().as_bytes()[12..] != [0; 4] {
                return Err(LixError::unknown(format!(
                    "changelog commit '{}' does not reserve its change address space",
                    commit.commit_id
                )));
            }
        }

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

        self.reject_existing_id_collisions(append, &append_commit_ids, &append_changes)
            .await?;
        self.validate_parent_commits(append, &append_commit_ids)
            .await?;

        Ok(())
    }

    /// Rejects an append whose ids already exist.
    ///
    /// A commit's own change id is `commit_id` at ordinal zero of its change
    /// address space (`CommitId::commit_change_id`), so "this commit's change
    /// id is taken" is the same question as "this commit id is taken" — the
    /// `COMMIT_SPACE` probe below answers both. The only independent risk left
    /// is a standalone change that already occupies that address, which the
    /// `CHANGE_SPACE` probe covers. The dedicated reverse-index space this
    /// used to consult is retired.
    async fn reject_existing_id_collisions(
        &mut self,
        append: &ChangelogAppend,
        append_commit_ids: &HashSet<CommitId>,
        append_changes: &HashMap<ChangeId, &ChangeRecord>,
    ) -> Result<(), LixError> {
        let commit_ids = append_commit_ids.iter().copied().collect::<Vec<_>>();
        let commit_keys = commit_ids
            .iter()
            .map(|commit_id| commit_key(*commit_id))
            .collect::<Vec<_>>();
        let commit_change_ids = append
            .commits
            .iter()
            .map(CommitRecord::change_id)
            .collect::<Vec<_>>();
        let append_change_ids = append_changes.keys().copied().collect::<Vec<_>>();
        let change_keys = append_change_ids
            .iter()
            .chain(&commit_change_ids)
            .map(|change_id| change_key(*change_id))
            .collect::<Vec<_>>();
        let mut batches = self
            .store
            .changelog_get_many_batch(vec![
                (COMMIT_SPACE, commit_keys),
                (CHANGE_SPACE, change_keys),
            ])
            .await?
            .into_iter();
        let existing_commits = batches
            .next()
            .expect("commit validation batch was requested");
        let existing_changes = batches
            .next()
            .expect("change validation batch was requested");
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
        for ((commit, change_id), existing_change) in append
            .commits
            .iter()
            .zip(commit_change_ids.iter())
            .zip(existing_commit_changes)
        {
            if append_changes.contains_key(change_id)
                || existing_change.is_some()
                || self.staged_changes.contains_key(change_id)
            {
                return Err(LixError::unknown(format!(
                    "changelog commit '{}' derived change_id '{}' collides with an existing change id",
                    commit.commit_id, change_id
                )));
            }
        }
        Ok(())
    }

    async fn validate_parent_commits(
        &mut self,
        append: &ChangelogAppend,
        append_commit_ids: &HashSet<CommitId>,
    ) -> Result<(), LixError> {
        let mut parent_ids = append
            .commits
            .iter()
            .flat_map(|commit| commit.parent_commit_ids.iter().copied())
            .filter(|parent_id| !append_commit_ids.contains(parent_id))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        parent_ids.sort_unstable();
        let keys = parent_ids
            .iter()
            .map(|id| commit_key(*id))
            .collect::<Vec<_>>();
        let mut parent_generations = HashMap::<CommitId, u64>::new();
        for (parent_id, found) in parent_ids
            .iter()
            .zip(get_many(self.store, COMMIT_SPACE, keys).await?)
        {
            let generation = match found {
                Some(bytes) => {
                    let record: CommitRecord = storage_codec::decode("commit record", &bytes)?;
                    record.generation
                }
                None => self
                    .staged_commits
                    .get(parent_id)
                    .map(|commit| commit.generation)
                    .ok_or_else(|| {
                        LixError::unknown(format!(
                            "changelog parent commit '{parent_id}' does not exist"
                        ))
                    })?,
            };
            parent_generations.insert(*parent_id, generation);
        }
        let append_generations = append
            .commits
            .iter()
            .map(|commit| (commit.commit_id, commit.generation))
            .collect::<HashMap<_, _>>();
        for commit in &append.commits {
            let expected_generation = commit
                .parent_commit_ids
                .iter()
                .map(|parent_id| {
                    append_generations
                        .get(parent_id)
                        .or_else(|| parent_generations.get(parent_id))
                        .copied()
                        .expect("every parent generation was resolved")
                })
                .max()
                .map_or(Ok(0), |generation| {
                    generation
                        .checked_add(1)
                        .ok_or_else(|| LixError::unknown("commit generation exceeds u64"))
                })?;
            if commit.generation != expected_generation {
                return Err(LixError::unknown(format!(
                    "changelog commit '{}' has generation {}, expected {expected_generation}",
                    commit.commit_id, commit.generation
                )));
            }
        }
        Ok(())
    }
}

async fn load_commits_from_store<'a>(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    request: CommitLoadRequest<'a>,
) -> Result<CommitLoadBatch<'a>, LixError> {
    let keys = request
        .commit_ids
        .iter()
        .map(|commit_id| commit_key(*commit_id))
        .collect::<Vec<_>>();
    let commit_values = get_many(store, COMMIT_SPACE, keys).await?;
    let mut entries = Vec::with_capacity(request.commit_ids.len());
    for value in commit_values {
        let Some(value) = value else {
            entries.push(None);
            continue;
        };
        let record = storage_codec::decode("commit record", &value)?;
        entries.push(Some(record));
    }
    CommitLoadBatch::try_new("changelog commit", request.commit_ids, entries)
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

async fn load_changes_from_store<'a>(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    request: ChangeLoadRequest<'a>,
) -> Result<ChangeLoadBatch<'a>, LixError> {
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
    ChangeLoadBatch::try_new("changelog change", request.change_ids, entries)
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
    let mut values = exact_get_many(read, &requests).await?.values.into_iter();
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
    let mut range = StoragePrefix {
        bytes: Bytes::from(prefix),
    }
    .to_range()?;
    if let Some(after) = after {
        let after = StorageKey(Bytes::from(after));
        range.lower = match range.lower {
            Bound::Included(lower) if lower > after => Bound::Included(lower),
            Bound::Excluded(lower) if lower >= after => Bound::Excluded(lower),
            _ => Bound::Excluded(after),
        };
    }
    let mut cursor = read
        .begin_scan(
            space,
            range,
            StorageBeginScanOptions {
                projection,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    let (chunk, has_more) = cursor.next_page(limit).await?.into_parts();
    let mut keys = Vec::with_capacity(chunk.len());
    let mut values = Vec::with_capacity(chunk.len());
    for entry in chunk {
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
