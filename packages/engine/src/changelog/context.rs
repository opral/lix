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

use async_trait::async_trait;
use bytes::Bytes;

use super::codec::{
    decode_change_record, decode_commit_change_ref_chunk, encode_change_record,
    encode_commit_change_ref_chunk, encode_commit_record,
};
use super::store::{
    CHANGE_SPACE, COMMIT_CHANGE_REF_CHUNK_SPACE, COMMIT_SPACE, change_id_from_key, change_key,
    commit_change_ref_chunk_key, commit_change_ref_chunk_prefix, commit_id_from_key, commit_key,
};
use crate::changelog::{
    ChangeId, ChangeLoadBatch, ChangeLoadRequest, ChangeRecord, ChangeScanBatch, ChangeScanRequest,
    ChangelogAppend, ChangelogReader, ChangelogWriter, CommitChangeRefChunk, CommitChangeRefSet,
    CommitId, CommitLoadBatch, CommitLoadEntry, CommitLoadRequest, CommitProjection, CommitRecord,
    CommitScanBatch, CommitScanRequest,
};
#[cfg(feature = "storage-benches")]
use crate::changelog::{GcPlan, GcRoot};
use crate::json_store::JsonSlot;
use crate::storage::StorageBackend;
use crate::storage::{
    PointReadPlan, ScanPlan, StorageContext, StorageCoreProjection, StorageGetOptions, StorageKey,
    StoragePrefix, StorageProjectedValue, StorageRead, StorageReadOptions, StorageScanOptions,
    StorageSpace, StorageWriteSet,
};
use crate::{LixError, storage_codec};

pub(super) const COMMIT_CHANGE_REF_CHUNK_FORMAT_VERSION: u32 = 1;
const COMMIT_CHANGE_REF_CHUNK_MAX_ENTRIES: usize = 2048;
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
            staged_commit_change_ref_chunks: HashMap::new(),
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
    staged_commit_change_ref_chunks: HashMap<CommitId, Vec<CommitChangeRefChunk>>,
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
    T: StorageRead + Send,
{
    async fn changelog_get_many(
        &mut self,
        space: StorageSpace,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, LixError> {
        native_get_many(self, space, keys).await
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
impl<B> ChangelogStorageRead for StorageContext<B>
where
    B: StorageBackend + Send,
{
    async fn changelog_get_many(
        &mut self,
        space: StorageSpace,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, LixError> {
        let mut read = self.begin_read(StorageReadOptions::default()).await?;
        native_get_many(&mut read, space, keys).await
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
    #[cfg(feature = "storage-benches")]
    async fn plan_gc(&mut self, roots: &[GcRoot]) -> Result<GcPlan, LixError> {
        Ok(empty_gc_plan(roots))
    }

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
    #[cfg(feature = "storage-benches")]
    async fn plan_gc(&mut self, roots: &[GcRoot]) -> Result<GcPlan, LixError> {
        Ok(empty_gc_plan(roots))
    }

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
                    return Some(project_commit_entry(
                        request.projection,
                        record.clone(),
                        self.staged_commit_change_ref_chunks
                            .get(commit_id)
                            .cloned()
                            .unwrap_or_default(),
                    ));
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
        for commit in staged {
            batch.entries.push(project_commit_entry(
                request.projection,
                commit.clone(),
                self.staged_commit_change_ref_chunks
                    .get(&commit.commit_id)
                    .cloned()
                    .unwrap_or_default(),
            ));
        }
        batch.entries.sort_by_key(|left| commit_entry_id(left));
        let limit = request.limit.unwrap_or(usize::MAX);
        if batch.entries.len() > limit {
            batch.entries.truncate(limit);
            batch.next_start_after = batch.entries.last().and_then(commit_entry_id);
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
        self.validate_append(&append).await?;

        for change in append.changes {
            self.writes.put(
                CHANGE_SPACE,
                change_key(change.change_id),
                encode_change_record(&change)?,
            );
            self.staged_changes.insert(change.change_id, change);
        }

        let chunks = chunk_commit_change_refs(append.commit_change_refs);
        for commit in append.commits {
            self.writes.put(
                COMMIT_SPACE,
                commit_key(commit.commit_id),
                encode_commit_record(&commit)?,
            );
            self.staged_commits.insert(commit.commit_id, commit);
        }

        for (commit_id, commit_chunks) in chunks {
            for (chunk_no, chunk) in commit_chunks.iter().enumerate() {
                self.writes.put(
                    COMMIT_CHANGE_REF_CHUNK_SPACE,
                    commit_change_ref_chunk_key(commit_id, chunk_no as u32),
                    encode_commit_change_ref_chunk(chunk)?,
                );
            }
            self.staged_commit_change_ref_chunks
                .insert(commit_id, commit_chunks);
        }

        Ok(())
    }

    async fn stage_delete_standalone_changes(
        &mut self,
        change_ids: &[ChangeId],
    ) -> Result<(), LixError> {
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

    #[cfg(feature = "storage-benches")]
    async fn collect_garbage(&mut self, roots: &[GcRoot]) -> Result<GcPlan, LixError> {
        Ok(empty_gc_plan(roots))
    }
}

impl<S> ChangelogStoreWriter<'_, S>
where
    S: ChangelogStorageRead + Send + ?Sized,
{
    async fn validate_append(&mut self, append: &ChangelogAppend) -> Result<(), LixError> {
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
        validate_unique(
            append.commit_change_refs.iter().map(|refs| refs.commit_id),
            "commit change ref commit_id",
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

        if let Some(change_id) = append
            .commit_change_refs
            .iter()
            .flat_map(|refs| refs.entries.iter())
            .find(|change_id| self.staged_change_deletes.contains(change_id))
        {
            return Err(LixError::unknown(format!(
                "cannot retain changelog change '{change_id}' in a commit because it was deleted in the same transaction"
            )));
        }

        if let Some(change_id) = append_changes
            .keys()
            .find(|change_id| self.staged_change_deletes.contains(change_id))
        {
            return Err(LixError::unknown(format!(
                "cannot append changelog change '{change_id}' because it was deleted in the same transaction"
            )));
        }

        self.reject_existing_commits(&append_commit_ids).await?;
        self.reject_existing_changes(append_changes.keys().copied())
            .await?;
        self.reject_commit_change_id_collisions(append, &append_changes)
            .await?;
        self.validate_parent_commits(append, &append_commit_ids)
            .await?;

        for commit in &append.commits {
            if !append
                .commit_change_refs
                .iter()
                .any(|refs| refs.commit_id == commit.commit_id)
            {
                return Err(LixError::unknown(format!(
                    "changelog commit '{}' is missing commit change refs",
                    commit.commit_id
                )));
            }
        }

        for refs in &append.commit_change_refs {
            if !append_commit_ids.contains(&refs.commit_id) {
                return Err(LixError::unknown(format!(
                    "changelog commit change refs target missing staged commit '{}'",
                    refs.commit_id
                )));
            }
            self.validate_change_refs(refs, &append_changes).await?;
        }

        Ok(())
    }

    async fn reject_commit_change_id_collisions(
        &mut self,
        append: &ChangelogAppend,
        append_changes: &HashMap<ChangeId, &ChangeRecord>,
    ) -> Result<(), LixError> {
        if append.commits.is_empty() {
            return Ok(());
        }
        for commit in &append.commits {
            if append_changes.contains_key(&commit.change_id)
                || self.change_exists(&commit.change_id).await?
                || self
                    .staged_commits
                    .values()
                    .any(|staged| staged.change_id == commit.change_id)
            {
                return Err(LixError::unknown(format!(
                    "changelog commit '{}' derived change_id '{}' collides with an existing change id",
                    commit.commit_id, commit.change_id
                )));
            }
        }
        let mut start_after = None::<String>;
        loop {
            let batch = scan_commits_from_store(
                self.store,
                CommitScanRequest {
                    start_after: start_after.as_deref(),
                    limit: Some(SCAN_PAGE_LIMIT),
                    projection: CommitProjection::Record,
                },
            )
            .await?;
            for entry in batch.entries {
                let CommitLoadEntry::Record(record) = entry else {
                    continue;
                };
                if append
                    .commits
                    .iter()
                    .any(|commit| commit.change_id == record.change_id)
                {
                    return Err(LixError::unknown(format!(
                        "changelog commit derived change_id '{}' already exists",
                        record.change_id
                    )));
                }
            }
            let Some(next) = batch.next_start_after else {
                break;
            };
            start_after = Some(next.to_string());
        }
        Ok(())
    }

    async fn reject_existing_commits(
        &mut self,
        commit_ids: &HashSet<CommitId>,
    ) -> Result<(), LixError> {
        let keys = commit_ids
            .iter()
            .map(|id| commit_key(*id))
            .collect::<Vec<_>>();
        for (commit_id, found) in commit_ids
            .iter()
            .zip(get_many(self.store, COMMIT_SPACE, keys).await?)
        {
            if found.is_some() || self.staged_commits.contains_key(commit_id) {
                return Err(LixError::unknown(format!(
                    "changelog commit '{commit_id}' already exists"
                )));
            }
        }
        Ok(())
    }

    async fn reject_existing_changes(
        &mut self,
        change_ids: impl IntoIterator<Item = ChangeId>,
    ) -> Result<(), LixError> {
        let change_ids = change_ids.into_iter().collect::<Vec<_>>();
        let keys = change_ids
            .iter()
            .map(|id| change_key(*id))
            .collect::<Vec<_>>();
        for (change_id, found) in change_ids
            .iter()
            .zip(get_many(self.store, CHANGE_SPACE, keys).await?)
        {
            if found.is_some() || self.staged_changes.contains_key(change_id) {
                return Err(LixError::unknown(format!(
                    "changelog change '{change_id}' already exists"
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

    /// Every ref must resolve to a change record (in this append, already
    /// staged, or stored), and no two refs in one commit may target the same
    /// (schema_key, file_id, entity_pk) identity.
    async fn validate_change_refs(
        &mut self,
        refs: &CommitChangeRefSet,
        append_changes: &HashMap<ChangeId, &ChangeRecord>,
    ) -> Result<(), LixError> {
        let missing_from_append = refs
            .entries
            .iter()
            .filter(|change_id| !append_changes.contains_key(change_id))
            .copied()
            .collect::<HashSet<_>>();
        let stored = self
            .load_stored_changes(missing_from_append.iter().copied())
            .await?;

        let mut seen_identities = HashSet::new();
        for change_id in &refs.entries {
            let change = append_changes
                .get(change_id)
                .copied()
                .or_else(|| self.staged_changes.get(change_id))
                .or_else(|| stored.get(change_id))
                .ok_or_else(|| {
                    LixError::unknown(format!(
                        "changelog commit '{}' references missing change '{}'",
                        refs.commit_id, change_id
                    ))
                })?;
            let identity = (
                change.schema_key.as_str(),
                change.file_id.as_deref(),
                &change.entity_pk,
            );
            if !seen_identities.insert(identity) {
                return Err(LixError::unknown(format!(
                    "changelog commit '{}' has duplicate change ref key",
                    refs.commit_id
                )));
            }
        }
        Ok(())
    }

    async fn load_stored_changes(
        &mut self,
        change_ids: impl IntoIterator<Item = ChangeId>,
    ) -> Result<HashMap<ChangeId, ChangeRecord>, LixError> {
        let change_ids = change_ids.into_iter().collect::<Vec<_>>();
        let keys = change_ids
            .iter()
            .map(|id| change_key(*id))
            .collect::<Vec<_>>();
        let values = get_many(self.store, CHANGE_SPACE, keys).await?;
        let mut out = HashMap::new();
        for (change_id, value) in change_ids.into_iter().zip(values) {
            if let Some(value) = value {
                out.insert(change_id, decode_change_record(&value, change_id)?);
            }
        }
        Ok(out)
    }

    async fn change_exists(&mut self, change_id: &ChangeId) -> Result<bool, LixError> {
        if self.staged_changes.contains_key(change_id) {
            return Ok(true);
        }
        Ok(get_one(self.store, CHANGE_SPACE, change_key(*change_id))
            .await?
            .is_some())
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
    for (commit_id, value) in request.commit_ids.iter().zip(commit_values) {
        let Some(value) = value else {
            entries.push(None);
            continue;
        };
        let record = storage_codec::decode("commit record", &value)?;
        let chunks = match request.projection {
            CommitProjection::Record => Vec::new(),
            CommitProjection::Full => load_commit_change_ref_chunks(store, commit_id).await?,
        };
        entries.push(Some(project_commit_entry(
            request.projection,
            record,
            chunks,
        )));
    }
    Ok(CommitLoadBatch { entries })
}

async fn scan_commits_from_store(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    request: CommitScanRequest<'_>,
) -> Result<CommitScanBatch, LixError> {
    if request.projection != CommitProjection::Record {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "changelog scan_commits currently supports CommitProjection::Record only",
        ));
    }
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
        entries.push(CommitLoadEntry::Record(record));
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

async fn load_commit_change_ref_chunks(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    commit_id: &CommitId,
) -> Result<Vec<CommitChangeRefChunk>, LixError> {
    let prefix = commit_change_ref_chunk_prefix(*commit_id);
    let mut after = None;
    let mut chunks = Vec::new();
    loop {
        let page = store
            .changelog_scan(
                COMMIT_CHANGE_REF_CHUNK_SPACE,
                prefix.clone(),
                after,
                SCAN_PAGE_LIMIT,
                StorageCoreProjection::FullValue,
            )
            .await?;
        for value in page.values {
            chunks.push(decode_commit_change_ref_chunk(&value, *commit_id)?);
        }
        let Some(resume_after) = page.resume_after else {
            break;
        };
        after = Some(resume_after);
    }
    Ok(chunks)
}

fn project_commit_entry(
    projection: CommitProjection,
    record: CommitRecord,
    change_ref_chunks: Vec<CommitChangeRefChunk>,
) -> CommitLoadEntry {
    match projection {
        CommitProjection::Record => CommitLoadEntry::Record(record),
        CommitProjection::Full => CommitLoadEntry::Full {
            record,
            change_ref_chunks,
        },
    }
}

fn commit_entry_id(entry: &CommitLoadEntry) -> Option<CommitId> {
    match entry {
        CommitLoadEntry::Record(record) => Some(record.commit_id),
        CommitLoadEntry::Full { record, .. } => Some(record.commit_id),
    }
}

fn chunk_commit_change_refs(
    refs: Vec<CommitChangeRefSet>,
) -> HashMap<CommitId, Vec<CommitChangeRefChunk>> {
    refs.into_iter()
        .map(|refs| {
            let commit_id = refs.commit_id;
            (
                commit_id,
                chunk_one_commit_change_refs(refs, COMMIT_CHANGE_REF_CHUNK_MAX_ENTRIES),
            )
        })
        .collect()
}

/// Each entry is a fixed 16 raw bytes on the wire, so chunking is a plain
/// fixed-capacity split over the change ids, sorted ascending for
/// deterministic output.
fn chunk_one_commit_change_refs(
    mut refs: CommitChangeRefSet,
    max_entries: usize,
) -> Vec<CommitChangeRefChunk> {
    refs.entries.sort_unstable();
    if refs.entries.is_empty() {
        return vec![commit_change_ref_chunk(refs.commit_id, Vec::new())];
    }
    refs.entries
        .chunks(max_entries)
        .map(|entries| commit_change_ref_chunk(refs.commit_id, entries.to_vec()))
        .collect()
}

fn commit_change_ref_chunk(commit_id: CommitId, entries: Vec<ChangeId>) -> CommitChangeRefChunk {
    CommitChangeRefChunk {
        format_version: COMMIT_CHANGE_REF_CHUNK_FORMAT_VERSION,
        commit_id,
        entries,
    }
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

#[cfg(feature = "storage-benches")]
fn empty_gc_plan(roots: &[GcRoot]) -> GcPlan {
    GcPlan {
        roots: roots.to_vec(),
        ..GcPlan::default()
    }
}

async fn get_one(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    space: StorageSpace,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, LixError> {
    Ok(get_many(store, space, vec![key])
        .await?
        .into_iter()
        .next()
        .flatten())
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
    R: StorageRead + ?Sized,
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

async fn native_scan<R>(
    read: &mut R,
    space: StorageSpace,
    prefix: Vec<u8>,
    after: Option<Vec<u8>>,
    limit: usize,
    projection: StorageCoreProjection,
) -> Result<ChangelogScanPage, LixError>
where
    R: StorageRead + ?Sized,
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

#[cfg(test)]
mod tests {
    use crate::changelog::test_support::{changelog_test_context, test_append, test_change_record};
    use crate::changelog::{
        ChangeId, ChangeLoadRequest, ChangeRecord, ChangeScanRequest, ChangelogAppend,
        ChangelogReader, ChangelogWriter, CommitId, CommitLoadEntry, CommitLoadRequest,
        CommitProjection, CommitScanRequest,
    };
    use crate::entity_pk::EntityPk;

    use super::*;

    fn ts(value: &str) -> crate::common::LixTimestamp {
        crate::common::LixTimestamp::expect_parse("timestamp", value)
    }

    fn commit_id(label: &str) -> String {
        CommitId::for_test_label(label).to_string()
    }

    fn change_id(label: &str) -> String {
        ChangeId::for_test_label(label).to_string()
    }

    fn change_ids<const N: usize>(labels: [&str; N]) -> Vec<String> {
        labels.into_iter().map(change_id).collect()
    }

    fn sorted_commit_ids<const N: usize>(labels: [&str; N]) -> Vec<String> {
        let mut ids = labels.into_iter().map(commit_id).collect::<Vec<_>>();
        ids.sort();
        ids
    }

    fn sorted_change_ids<const N: usize>(labels: [&str; N]) -> Vec<String> {
        let mut ids = labels.into_iter().map(change_id).collect::<Vec<_>>();
        ids.sort();
        ids
    }

    #[test]
    fn chunk_one_commit_change_refs_sorts_and_splits_by_entry_count() {
        let refs = CommitChangeRefSet {
            commit_id: CommitId::for_test_label("commit-1"),
            entries: (0..5)
                .rev()
                .map(|index| ChangeId::for_test_label(&format!("change-{index}")))
                .collect(),
        };

        let chunks = chunk_one_commit_change_refs(refs, 2);

        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.entries.len())
                .collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
        let flattened = chunks
            .iter()
            .flat_map(|chunk| chunk.entries.iter().copied())
            .collect::<Vec<_>>();
        let mut sorted = flattened.clone();
        sorted.sort_unstable();
        assert_eq!(flattened, sorted, "entries must be sorted ascending");
    }

    #[test]
    fn chunk_one_commit_change_refs_keeps_empty_commits_addressable() {
        let refs = CommitChangeRefSet {
            commit_id: CommitId::for_test_label("commit-1"),
            entries: Vec::new(),
        };
        let chunks = chunk_one_commit_change_refs(refs, 2048);
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].entries.is_empty());
    }

    #[tokio::test]
    async fn stage_append_writes_direct_records_and_change_ref_chunks() {
        let (context, storage) = changelog_test_context();
        let append = test_append();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_append(append).await.unwrap();
        }
        let stats = writes.apply(&mut *transaction).await.unwrap();
        assert_eq!(stats.staged_puts, 3);
        transaction.commit().await.unwrap();

        let mut read = storage.begin_read_transaction().await.unwrap();
        let mut reader = context.reader(&mut *read);
        let commits = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &[CommitId::for_test_label("commit-1")],
                projection: CommitProjection::Full,
            })
            .await
            .unwrap();
        let Some(CommitLoadEntry::Full {
            record,
            change_ref_chunks,
        }) = commits.entries.into_iter().next().flatten()
        else {
            panic!("expected full commit entry");
        };
        assert_eq!(record.commit_id, "commit-1");
        assert_eq!(record.change_id, "commit-row-change-1");
        assert_eq!(change_ref_chunks.len(), 1);
        assert_eq!(
            change_ref_chunks[0]
                .entries
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            change_ids(["change-1"])
        );

        let changes = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &[
                    ChangeId::for_test_label("change-1"),
                    ChangeId::for_test_label("missing"),
                ],
            })
            .await
            .unwrap();
        assert_eq!(changes.entries[0].as_ref().unwrap().schema_key, "message");
        assert!(changes.entries[1].is_none());
    }

    #[tokio::test]
    async fn stage_delete_standalone_changes_removes_change() {
        let (context, storage) = changelog_test_context();
        let change_id = ChangeId::for_test_label("standalone-change");
        let change = ChangeRecord {
            change_id,
            ..test_change_record()
        };

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .stage_append(ChangelogAppend {
                    commits: Vec::new(),
                    changes: vec![change],
                    commit_change_refs: Vec::new(),
                })
                .await
                .unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .stage_delete_standalone_changes(&[change_id])
                .await
                .unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut read = storage.begin_read_transaction().await.unwrap();
        let mut reader = context.reader(&mut *read);
        let changes = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &[change_id],
            })
            .await
            .unwrap();
        assert_eq!(changes.entries, vec![None]);
    }

    #[tokio::test]
    async fn stage_delete_standalone_changes_rejects_change_appended_in_same_transaction() {
        let (context, storage) = changelog_test_context();
        let change_id = ChangeId::for_test_label("new-change");
        let change = ChangeRecord {
            change_id,
            ..test_change_record()
        };

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .stage_append(ChangelogAppend {
                    commits: Vec::new(),
                    changes: vec![change],
                    commit_change_refs: Vec::new(),
                })
                .await
                .unwrap();
            writer
                .stage_delete_standalone_changes(&[change_id])
                .await
                .unwrap_err()
        };
        assert!(error.message.contains("staged in the same transaction"));
    }

    #[tokio::test]
    async fn stage_append_rejects_ref_to_change_deleted_in_same_transaction() {
        let (context, storage) = changelog_test_context();
        let change_id = ChangeId::for_test_label("standalone-promoted-change");

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            context
                .writer(&mut *transaction, &mut writes)
                .stage_append(ChangelogAppend {
                    changes: vec![ChangeRecord {
                        change_id,
                        ..test_change_record()
                    }],
                    ..ChangelogAppend::default()
                })
                .await
                .unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut append = test_append();
        append.changes.clear();
        append.commit_change_refs[0].entries = vec![change_id];
        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .stage_delete_standalone_changes(&[change_id])
                .await
                .unwrap();
            writer.stage_append(append).await.unwrap_err()
        };
        assert!(error.message.contains("retain"));
        assert!(error.message.contains("deleted in the same transaction"));
    }

    #[tokio::test]
    async fn stage_append_rejects_duplicate_ref_identities() {
        // Two different change records targeting the same
        // (schema_key, file_id, entity_pk) must not land in one commit.
        let (context, storage) = changelog_test_context();
        let mut append = test_append();
        append.changes.push(ChangeRecord {
            change_id: ChangeId::for_test_label("change-dup"),
            ..test_change_record()
        });
        append.commit_change_refs[0]
            .entries
            .push(ChangeId::for_test_label("change-dup"));

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_append(append).await.unwrap_err()
        };
        assert!(
            error.message.contains("duplicate change ref key"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn stage_append_rejects_commit_missing_change_refs() {
        let (context, storage) = changelog_test_context();
        let mut append = test_append();
        append.commit_change_refs.clear();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_append(append).await.unwrap_err()
        };
        assert!(
            error.message.contains("is missing commit change refs"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn stage_append_rejects_commit_change_id_colliding_with_change_record() {
        let (context, storage) = changelog_test_context();
        let mut append = test_append();
        append.changes[0].change_id = append.commits[0].change_id.clone();
        append.commit_change_refs[0].entries[0] = append.commits[0].change_id.clone();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_append(append).await.unwrap_err()
        };
        assert!(
            error
                .message
                .contains("collides with an existing change id"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn stage_append_splits_large_ref_sets_at_the_entry_cap() {
        // Pins COMMIT_CHANGE_REF_CHUNK_MAX_ENTRIES end-to-end: one commit
        // with 2049 refs must persist as chunks of [2048, 1] whose
        // concatenation is globally sorted and gap-free.
        let (context, storage) = changelog_test_context();
        let mut append = test_append();
        append.changes.clear();
        append.commit_change_refs[0].entries.clear();
        for index in 0..=COMMIT_CHANGE_REF_CHUNK_MAX_ENTRIES {
            let change_id = ChangeId::for_test_label(&format!("bulk-change-{index:05}"));
            append.changes.push(ChangeRecord {
                change_id,
                entity_pk: EntityPk::single(format!("bulk-entity-{index:05}")),
                ..test_change_record()
            });
            append.commit_change_refs[0].entries.push(change_id);
        }

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_append(append).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut read = storage.begin_read_transaction().await.unwrap();
        let mut reader = context.reader(&mut *read);
        let commits = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &[CommitId::for_test_label("commit-1")],
                projection: CommitProjection::Full,
            })
            .await
            .unwrap();
        let Some(CommitLoadEntry::Full {
            change_ref_chunks, ..
        }) = commits.entries.into_iter().next().flatten()
        else {
            panic!("expected full commit entry");
        };
        assert_eq!(
            change_ref_chunks
                .iter()
                .map(|chunk| chunk.entries.len())
                .collect::<Vec<_>>(),
            vec![COMMIT_CHANGE_REF_CHUNK_MAX_ENTRIES, 1]
        );
        let flattened = change_ref_chunks
            .iter()
            .flat_map(|chunk| chunk.entries.iter().copied())
            .collect::<Vec<_>>();
        let mut sorted = flattened.clone();
        sorted.sort_unstable();
        assert_eq!(flattened, sorted, "entries must be globally sorted");
        assert_eq!(
            flattened.len(),
            COMMIT_CHANGE_REF_CHUNK_MAX_ENTRIES + 1,
            "no entry may be lost across the chunk boundary"
        );
    }

    #[tokio::test]
    async fn stage_append_sorts_commit_change_refs_by_canonical_key() {
        let (context, storage) = changelog_test_context();
        let mut append = test_append();
        append.changes.push(ChangeRecord {
            format_version: 1,
            change_id: ChangeId::for_test_label("change-0"),
            schema_key: "alpha".to_string(),
            entity_pk: EntityPk::single("entity-0"),
            file_id: None,
            snapshot: JsonSlot::None,
            metadata: JsonSlot::None,
            created_at: ts("2026-05-12T00:00:00Z"),
            origin_key: None,
        });
        append.commit_change_refs[0]
            .entries
            .insert(0, ChangeId::for_test_label("change-0"));
        append.commit_change_refs[0].entries.swap(0, 1);

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_append(append).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut read = storage.begin_read_transaction().await.unwrap();
        let mut reader = context.reader(&mut *read);
        let commits = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &[CommitId::for_test_label("commit-1")],
                projection: CommitProjection::Full,
            })
            .await
            .unwrap();
        let Some(CommitLoadEntry::Full {
            change_ref_chunks, ..
        }) = commits.entries.into_iter().next().flatten()
        else {
            panic!("expected full commit entry");
        };
        assert_eq!(
            change_ref_chunks[0]
                .entries
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            change_ids(["change-0", "change-1"])
        );
    }

    #[tokio::test]
    async fn scan_commits_reads_direct_commit_records_in_key_order() {
        let (context, storage) = changelog_test_context();
        let mut first = test_append();
        first.commits[0].commit_id = CommitId::for_test_label("commit-b");
        first.commits[0].change_id = ChangeId::for_test_label("commit-b-row-change");
        first.commit_change_refs[0].commit_id = CommitId::for_test_label("commit-b");

        let mut second = test_append();
        second.commits[0].commit_id = CommitId::for_test_label("commit-a");
        second.commits[0].change_id = ChangeId::for_test_label("commit-a-row-change");
        second.changes[0].change_id = ChangeId::for_test_label("change-a");
        second.commit_change_refs[0].commit_id = CommitId::for_test_label("commit-a");
        second.commit_change_refs[0].entries[0] = ChangeId::for_test_label("change-a");

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_append(first).await.unwrap();
            writer.stage_append(second).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut read = storage.begin_read_transaction().await.unwrap();
        let mut reader = context.reader(&mut *read);
        let expected_ids = sorted_commit_ids(["commit-b", "commit-a"]);
        let scan = reader
            .scan_commits(CommitScanRequest {
                start_after: None,
                limit: Some(1),
                projection: CommitProjection::Record,
            })
            .await
            .unwrap();
        let next_start_after = scan.next_start_after.map(|commit_id| commit_id.to_string());
        assert_eq!(scan.entries.len(), 1);
        assert_eq!(next_start_after.as_deref(), Some(expected_ids[0].as_str()));
        let CommitLoadEntry::Record(record) = &scan.entries[0] else {
            panic!("expected record projection");
        };
        assert_eq!(record.commit_id.to_string(), expected_ids[0]);

        let next = reader
            .scan_commits(CommitScanRequest {
                start_after: next_start_after.as_deref(),
                limit: Some(10),
                projection: CommitProjection::Record,
            })
            .await
            .unwrap();
        let ids = next
            .entries
            .iter()
            .map(|entry| {
                let CommitLoadEntry::Record(record) = entry else {
                    panic!("expected record projection");
                };
                record.commit_id.to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, expected_ids[1..].to_vec());
        assert_eq!(next.next_start_after, None);
    }

    #[tokio::test]
    async fn scan_changes_reads_direct_change_records_in_key_order() {
        let (context, storage) = changelog_test_context();
        let mut first = test_append();
        first.commits[0].commit_id = CommitId::for_test_label("commit-b");
        first.commits[0].change_id = ChangeId::for_test_label("commit-b-row-change");
        first.changes[0].change_id = ChangeId::for_test_label("change-b");
        first.commit_change_refs[0].commit_id = CommitId::for_test_label("commit-b");
        first.commit_change_refs[0].entries[0] = ChangeId::for_test_label("change-b");

        let mut second = test_append();
        second.commits[0].commit_id = CommitId::for_test_label("commit-a");
        second.commits[0].change_id = ChangeId::for_test_label("commit-a-row-change");
        second.changes[0].change_id = ChangeId::for_test_label("change-a");
        second.commit_change_refs[0].commit_id = CommitId::for_test_label("commit-a");
        second.commit_change_refs[0].entries[0] = ChangeId::for_test_label("change-a");

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_append(first).await.unwrap();
            writer.stage_append(second).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut read = storage.begin_read_transaction().await.unwrap();
        let mut reader = context.reader(&mut *read);
        let expected_ids = sorted_change_ids(["change-b", "change-a"]);
        let scan = reader
            .scan_changes(ChangeScanRequest {
                start_after: None,
                limit: Some(1),
            })
            .await
            .unwrap();
        let next_start_after = scan.next_start_after.map(|change_id| change_id.to_string());
        assert_eq!(scan.entries.len(), 1);
        assert_eq!(scan.entries[0].change_id.to_string(), expected_ids[0]);
        assert_eq!(next_start_after.as_deref(), Some(expected_ids[0].as_str()));

        let next = reader
            .scan_changes(ChangeScanRequest {
                start_after: next_start_after.as_deref(),
                limit: Some(10),
            })
            .await
            .unwrap();
        let ids = next
            .entries
            .iter()
            .map(|change| change.change_id.to_string())
            .collect::<Vec<_>>();
        assert_eq!(ids, expected_ids[1..].to_vec());
        assert_eq!(next.next_start_after, None);
    }

    #[tokio::test]
    async fn scan_changes_pages_all_direct_change_records_without_gaps() {
        let (context, storage) = changelog_test_context();
        let changes = (0..2_500)
            .map(|index| ChangeRecord {
                format_version: 1,
                change_id: ChangeId::for_test_label(&format!("change-{index:04}")),
                schema_key: "message".to_string(),
                entity_pk: EntityPk::single(format!("entity-{index:04}")),
                file_id: None,
                snapshot: JsonSlot::None,
                metadata: JsonSlot::None,
                created_at: ts("2026-05-20T00:00:00Z"),
                origin_key: None,
            })
            .collect::<Vec<_>>();
        let mut expected_ids = changes
            .iter()
            .map(|change| change.change_id.clone())
            .collect::<Vec<_>>();
        expected_ids.sort();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .stage_append(ChangelogAppend {
                    commits: Vec::new(),
                    changes,
                    commit_change_refs: Vec::new(),
                })
                .await
                .unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut read = storage.begin_read_transaction().await.unwrap();
        let mut reader = context.reader(&mut *read);
        let mut start_after = None::<String>;
        let mut scanned_ids = Vec::new();
        let mut page_sizes = Vec::new();
        loop {
            let page = reader
                .scan_changes(ChangeScanRequest {
                    start_after: start_after.as_deref(),
                    limit: Some(1_024),
                })
                .await
                .unwrap();
            page_sizes.push(page.entries.len());
            scanned_ids.extend(page.entries.into_iter().map(|change| change.change_id));
            let Some(next_start_after) = page.next_start_after else {
                break;
            };
            start_after = Some(next_start_after.to_string());
        }

        assert_eq!(page_sizes, vec![1_024, 1_024, 452]);
        assert_eq!(scanned_ids, expected_ids);
    }
}
