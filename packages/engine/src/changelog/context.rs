use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use bytes::Bytes;

use super::codec::{
    decode_change_record, decode_commit_change_ref_chunk, encode_change_record,
    encode_commit_change_ref_chunk, encode_commit_record,
};
use super::store::{
    change_key, commit_change_ref_chunk_key, commit_change_ref_chunk_prefix, commit_key,
    CHANGE_SPACE, COMMIT_CHANGE_REF_CHUNK_SPACE, COMMIT_SPACE,
};
use crate::changelog::{
    ChangeLoadBatch, ChangeLoadRequest, ChangeRecord, ChangeScanBatch, ChangeScanRequest,
    ChangelogAppend, ChangelogReader, ChangelogWriter, CommitChangeRef, CommitChangeRefChunk,
    CommitChangeRefSet, CommitLoadBatch, CommitLoadEntry, CommitLoadRequest, CommitProjection,
    CommitRecord, CommitScanBatch, CommitScanRequest, GcPlan, GcRoot,
};
use crate::storage::{
    PointReadPlan, ScanPlan, StorageBackend, StorageContext, StorageCoreProjection,
    StorageGetOptions, StorageKey, StoragePrefix, StorageProjectedValue, StorageRead,
    StorageReadOptions, StorageScanOptions, StorageSpace, StorageWriteSet,
};
use crate::{storage_codec, LixError};

const COMMIT_CHANGE_REF_CHUNK_FORMAT_VERSION: u32 = 1;
const COMMIT_CHANGE_REF_CHUNK_TARGET_BYTES: usize = 64 * 1024;
const COMMIT_CHANGE_REF_CHUNK_MAX_BYTES: usize = 128 * 1024;
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
    staged_commits: HashMap<String, CommitRecord>,
    staged_changes: HashMap<String, ChangeRecord>,
    staged_commit_change_ref_chunks: HashMap<String, Vec<CommitChangeRefChunk>>,
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
        native_get_many(self, space, keys)
    }

    async fn changelog_scan(
        &mut self,
        space: StorageSpace,
        prefix: Vec<u8>,
        after: Option<Vec<u8>>,
        limit: usize,
        projection: StorageCoreProjection,
    ) -> Result<ChangelogScanPage, LixError> {
        native_scan(self, space, prefix, after, limit, projection)
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
        let mut read = self.begin_read(StorageReadOptions::default())?;
        native_get_many(&mut read, space, keys)
    }

    async fn changelog_scan(
        &mut self,
        space: StorageSpace,
        prefix: Vec<u8>,
        after: Option<Vec<u8>>,
        limit: usize,
        projection: StorageCoreProjection,
    ) -> Result<ChangelogScanPage, LixError> {
        let mut read = self.begin_read(StorageReadOptions::default())?;
        native_scan(&mut read, space, prefix, after, limit, projection)
    }
}

#[async_trait]
impl<S> ChangelogReader for ChangelogStoreReader<S>
where
    S: ChangelogStorageRead + Send,
{
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
            .zip(stored.entries.into_iter())
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
                    .map(|start_after| commit.commit_id.as_str() > start_after)
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        staged.sort_by(|left, right| left.commit_id.cmp(&right.commit_id));
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
        batch.entries.sort_by(|left, right| {
            commit_entry_id(left)
                .unwrap_or_default()
                .cmp(commit_entry_id(right).unwrap_or_default())
        });
        let limit = request.limit.unwrap_or(usize::MAX);
        if batch.entries.len() > limit {
            batch.entries.truncate(limit);
            batch.next_start_after = batch
                .entries
                .last()
                .and_then(commit_entry_id)
                .map(str::to_string);
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
            .zip(stored.entries.into_iter())
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
                    .map(|start_after| change.change_id.as_str() > start_after)
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        staged.sort_by(|left, right| left.change_id.cmp(&right.change_id));
        batch.entries.extend(staged);
        batch
            .entries
            .sort_by(|left, right| left.change_id.cmp(&right.change_id));
        batch
            .entries
            .dedup_by(|left, right| left.change_id == right.change_id);
        let limit = request.limit.unwrap_or(usize::MAX);
        if batch.entries.len() > limit {
            batch.entries.truncate(limit);
            batch.next_start_after = batch.entries.last().map(|change| change.change_id.clone());
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
                change_key(&change.change_id),
                encode_change_record(&change)?,
            );
            self.staged_changes.insert(change.change_id.clone(), change);
        }

        let chunks = chunk_commit_change_refs(append.commit_change_refs)?;
        for commit in append.commits {
            self.writes.put(
                COMMIT_SPACE,
                commit_key(&commit.commit_id),
                encode_commit_record(&commit)?,
            );
            self.staged_commits.insert(commit.commit_id.clone(), commit);
        }

        for (commit_id, commit_chunks) in chunks {
            for (chunk_no, chunk) in commit_chunks.iter().enumerate() {
                self.writes.put(
                    COMMIT_CHANGE_REF_CHUNK_SPACE,
                    commit_change_ref_chunk_key(&commit_id, chunk_no as u32),
                    encode_commit_change_ref_chunk(chunk)?,
                );
            }
            self.staged_commit_change_ref_chunks
                .insert(commit_id, commit_chunks);
        }

        Ok(())
    }

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
            append
                .commits
                .iter()
                .map(|commit| commit.commit_id.as_str()),
            "commit_id",
        )?;
        validate_unique(
            append
                .changes
                .iter()
                .map(|change| change.change_id.as_str()),
            "change_id",
        )?;
        validate_unique(
            append
                .commits
                .iter()
                .map(|commit| commit.change_id.as_str()),
            "commit change_id",
        )?;
        validate_unique(
            append
                .commit_change_refs
                .iter()
                .map(|refs| refs.commit_id.as_str()),
            "commit change ref commit_id",
        )?;

        let append_commit_ids = append
            .commits
            .iter()
            .map(|commit| commit.commit_id.as_str())
            .collect::<HashSet<_>>();
        let append_changes = append
            .changes
            .iter()
            .map(|change| (change.change_id.as_str(), change))
            .collect::<HashMap<_, _>>();

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
            if !append_commit_ids.contains(refs.commit_id.as_str()) {
                return Err(LixError::unknown(format!(
                    "changelog commit change refs target missing staged commit '{}'",
                    refs.commit_id
                )));
            }
            validate_unique_ref_keys(&refs.entries, &refs.commit_id)?;
            self.validate_change_refs(refs, &append_changes).await?;
        }

        Ok(())
    }

    async fn reject_commit_change_id_collisions(
        &mut self,
        append: &ChangelogAppend,
        append_changes: &HashMap<&str, &ChangeRecord>,
    ) -> Result<(), LixError> {
        for commit in &append.commits {
            if append_changes.contains_key(commit.change_id.as_str())
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
            start_after = Some(next);
        }
        Ok(())
    }

    async fn reject_existing_commits<'a>(
        &mut self,
        commit_ids: &HashSet<&'a str>,
    ) -> Result<(), LixError> {
        let keys = commit_ids
            .iter()
            .map(|commit_id| commit_key(commit_id))
            .collect::<Vec<_>>();
        for (commit_id, found) in commit_ids
            .iter()
            .zip(get_many(self.store, COMMIT_SPACE, keys).await?)
        {
            if found.is_some() || self.staged_commits.contains_key(*commit_id) {
                return Err(LixError::unknown(format!(
                    "changelog commit '{}' already exists",
                    commit_id
                )));
            }
        }
        Ok(())
    }

    async fn reject_existing_changes<'a>(
        &mut self,
        change_ids: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), LixError> {
        let change_ids = change_ids.into_iter().collect::<Vec<_>>();
        let keys = change_ids
            .iter()
            .map(|change_id| change_key(change_id))
            .collect::<Vec<_>>();
        for (change_id, found) in change_ids
            .iter()
            .zip(get_many(self.store, CHANGE_SPACE, keys).await?)
        {
            if found.is_some() || self.staged_changes.contains_key(*change_id) {
                return Err(LixError::unknown(format!(
                    "changelog change '{}' already exists",
                    change_id
                )));
            }
        }
        Ok(())
    }

    async fn validate_parent_commits(
        &mut self,
        append: &ChangelogAppend,
        append_commit_ids: &HashSet<&str>,
    ) -> Result<(), LixError> {
        let parent_ids = append
            .commits
            .iter()
            .flat_map(|commit| commit.parent_commit_ids.iter().map(String::as_str))
            .filter(|parent_id| !append_commit_ids.contains(parent_id))
            .collect::<HashSet<_>>();
        let keys = parent_ids
            .iter()
            .map(|parent_id| commit_key(parent_id))
            .collect::<Vec<_>>();
        for (parent_id, found) in parent_ids
            .iter()
            .zip(get_many(self.store, COMMIT_SPACE, keys).await?)
        {
            if found.is_none() && !self.staged_commits.contains_key(*parent_id) {
                return Err(LixError::unknown(format!(
                    "changelog parent commit '{}' does not exist",
                    parent_id
                )));
            }
        }
        Ok(())
    }

    async fn validate_change_refs(
        &mut self,
        refs: &CommitChangeRefSet,
        append_changes: &HashMap<&str, &ChangeRecord>,
    ) -> Result<(), LixError> {
        let missing_from_append = refs
            .entries
            .iter()
            .filter(|entry| !append_changes.contains_key(entry.change_id.as_str()))
            .map(|entry| entry.change_id.as_str())
            .collect::<HashSet<_>>();
        let stored = self
            .load_stored_changes(missing_from_append.iter().copied())
            .await?;

        for entry in &refs.entries {
            let change = append_changes
                .get(entry.change_id.as_str())
                .copied()
                .or_else(|| self.staged_changes.get(&entry.change_id))
                .or_else(|| stored.get(entry.change_id.as_str()))
                .ok_or_else(|| {
                    LixError::unknown(format!(
                        "changelog commit '{}' references missing change '{}'",
                        refs.commit_id, entry.change_id
                    ))
                })?;
            validate_ref_matches_change(&refs.commit_id, entry, change)?;
        }
        Ok(())
    }

    async fn load_stored_changes<'a>(
        &mut self,
        change_ids: impl IntoIterator<Item = &'a str>,
    ) -> Result<HashMap<String, ChangeRecord>, LixError> {
        let change_ids = change_ids.into_iter().collect::<Vec<_>>();
        let keys = change_ids
            .iter()
            .map(|change_id| change_key(change_id))
            .collect::<Vec<_>>();
        let values = get_many(self.store, CHANGE_SPACE, keys).await?;
        let mut out = HashMap::new();
        for (change_id, value) in change_ids.into_iter().zip(values) {
            if let Some(value) = value {
                out.insert(change_id.to_string(), decode_change_record(&value)?);
            }
        }
        Ok(out)
    }

    async fn change_exists(&mut self, change_id: &str) -> Result<bool, LixError> {
        if self.staged_changes.contains_key(change_id) {
            return Ok(true);
        }
        Ok(get_one(self.store, CHANGE_SPACE, change_key(change_id))
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
        .map(|commit_id| commit_key(commit_id))
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
            CommitProjection::ChangeRefs | CommitProjection::Full => {
                load_commit_change_ref_chunks(store, commit_id).await?
            }
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
            next_start_after: request.start_after.map(str::to_string),
        });
    }
    let page = store
        .changelog_scan(
            COMMIT_SPACE,
            Vec::new(),
            request.start_after.map(commit_key),
            limit,
            StorageCoreProjection::FullValue,
        )
        .await?;
    let mut entries = Vec::with_capacity(page.values.len());
    for (key, value) in page.keys.iter().zip(page.values.iter()) {
        let record: CommitRecord = storage_codec::decode("commit record", value)?;
        if key.as_slice() != commit_key(&record.commit_id).as_slice() {
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
        .map(|key| {
            String::from_utf8(key).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("changelog commit scan resume key is not UTF-8: {error}"),
                )
            })
        })
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
        .map(|change_id| change_key(change_id))
        .collect::<Vec<_>>();
    let entries = get_many(store, CHANGE_SPACE, keys)
        .await?
        .into_iter()
        .map(|value| value.as_deref().map(decode_change_record).transpose())
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
            next_start_after: request.start_after.map(str::to_string),
        });
    }
    let page = store
        .changelog_scan(
            CHANGE_SPACE,
            Vec::new(),
            request.start_after.map(change_key),
            limit,
            StorageCoreProjection::FullValue,
        )
        .await?;
    let mut entries = Vec::with_capacity(page.values.len());
    for (key, value) in page.keys.iter().zip(page.values.iter()) {
        let record = decode_change_record(value)?;
        if key.as_slice() != change_key(&record.change_id).as_slice() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "changelog change scan key does not match decoded change_id '{}'",
                    record.change_id
                ),
            ));
        }
        entries.push(record);
    }
    let next_start_after = page
        .resume_after
        .map(|key| {
            String::from_utf8(key).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("changelog change scan resume key is not UTF-8: {error}"),
                )
            })
        })
        .transpose()?;
    Ok(ChangeScanBatch {
        entries,
        next_start_after,
    })
}

async fn load_commit_change_ref_chunks(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    commit_id: &str,
) -> Result<Vec<CommitChangeRefChunk>, LixError> {
    let prefix = commit_change_ref_chunk_prefix(commit_id);
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
            chunks.push(decode_commit_change_ref_chunk(&value, commit_id)?);
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
        CommitProjection::ChangeRefs => CommitLoadEntry::ChangeRefs(change_ref_chunks),
        CommitProjection::Full => CommitLoadEntry::Full {
            record,
            change_ref_chunks,
        },
    }
}

fn commit_entry_id(entry: &CommitLoadEntry) -> Option<&str> {
    match entry {
        CommitLoadEntry::Record(record) => Some(&record.commit_id),
        CommitLoadEntry::Full { record, .. } => Some(&record.commit_id),
        CommitLoadEntry::ChangeRefs(chunks) => chunks.first().map(|chunk| chunk.commit_id.as_str()),
    }
}

fn chunk_commit_change_refs(
    refs: Vec<CommitChangeRefSet>,
) -> Result<HashMap<String, Vec<CommitChangeRefChunk>>, LixError> {
    refs.into_iter()
        .map(|refs| {
            let commit_id = refs.commit_id.clone();
            Ok((
                commit_id,
                chunk_one_commit_change_refs(
                    refs,
                    COMMIT_CHANGE_REF_CHUNK_TARGET_BYTES,
                    COMMIT_CHANGE_REF_CHUNK_MAX_BYTES,
                    COMMIT_CHANGE_REF_CHUNK_MAX_ENTRIES,
                )?,
            ))
        })
        .collect()
}

fn chunk_one_commit_change_refs(
    mut refs: CommitChangeRefSet,
    target_bytes: usize,
    max_bytes: usize,
    max_entries: usize,
) -> Result<Vec<CommitChangeRefChunk>, LixError> {
    refs.entries.sort_by(|left, right| {
        (
            left.schema_key.as_str(),
            left.file_id.as_deref(),
            &left.entity_pk,
            left.change_id.as_str(),
        )
            .cmp(&(
                right.schema_key.as_str(),
                right.file_id.as_deref(),
                &right.entity_pk,
                right.change_id.as_str(),
            ))
    });

    let mut chunks = Vec::new();
    let mut builder = CommitChangeRefChunkBuilder::new(refs.commit_id.clone());
    for entry in refs.entries {
        let candidate_size = builder.estimated_size_after(&entry);
        if !builder.is_empty()
            && (builder.len() >= max_entries
                || builder.estimated_size() >= target_bytes
                || candidate_size > max_bytes)
        {
            chunks.push(builder.finish()?);
            builder = CommitChangeRefChunkBuilder::new(refs.commit_id.clone());
        }

        builder.push(entry);
        validate_commit_change_ref_chunk_size(&builder, max_bytes)?;
    }

    if !builder.is_empty() {
        chunks.push(builder.finish()?);
    }
    Ok(chunks)
}

fn commit_change_ref_chunk(commit_id: &str, entries: Vec<CommitChangeRef>) -> CommitChangeRefChunk {
    CommitChangeRefChunk {
        format_version: COMMIT_CHANGE_REF_CHUNK_FORMAT_VERSION,
        commit_id: commit_id.to_string(),
        entries,
    }
}

fn validate_commit_change_ref_chunk_size(
    builder: &CommitChangeRefChunkBuilder,
    max_bytes: usize,
) -> Result<(), LixError> {
    let size = builder.estimated_size();
    if size > max_bytes {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "single changelog commit_change_ref_chunk entry for commit '{}' exceeds {max_bytes} bytes",
                builder.commit_id
            ),
        ));
    }
    Ok(())
}

struct CommitChangeRefChunkBuilder {
    commit_id: String,
    entries: Vec<CommitChangeRef>,
    schema_keys: HashSet<String>,
    file_ids: HashSet<Option<String>>,
    estimated_size: usize,
}

impl CommitChangeRefChunkBuilder {
    fn new(commit_id: String) -> Self {
        Self {
            commit_id,
            entries: Vec::new(),
            schema_keys: HashSet::new(),
            file_ids: HashSet::new(),
            estimated_size: commit_change_ref_chunk_fixed_size(),
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn estimated_size(&self) -> usize {
        self.estimated_size
    }

    fn estimated_size_after(&self, entry: &CommitChangeRef) -> usize {
        self.estimated_size + self.incremental_size(entry)
    }

    fn push(&mut self, entry: CommitChangeRef) {
        self.estimated_size += self.incremental_size(&entry);
        self.schema_keys.insert(entry.schema_key.clone());
        self.file_ids.insert(entry.file_id.clone());
        self.entries.push(entry);
    }

    fn incremental_size(&self, entry: &CommitChangeRef) -> usize {
        let schema_dictionary_bytes = if self.schema_keys.contains(&entry.schema_key) {
            0
        } else {
            encoded_str_size(&entry.schema_key)
        };
        let file_dictionary_bytes = if self.file_ids.contains(&entry.file_id) {
            0
        } else {
            encoded_optional_str_size(entry.file_id.as_deref())
        };
        schema_dictionary_bytes
            + file_dictionary_bytes
            + encoded_commit_change_ref_entry_size(entry)
    }

    fn finish(self) -> Result<CommitChangeRefChunk, LixError> {
        Ok(commit_change_ref_chunk(&self.commit_id, self.entries))
    }
}

fn commit_change_ref_chunk_fixed_size() -> usize {
    5 // magic
        + 4 // format_version
        + 4 // schema dictionary length
        + 4 // file dictionary length
        + 4 // entry count
}

fn encoded_commit_change_ref_entry_size(entry: &CommitChangeRef) -> usize {
    2 // schema index
        + 2 // file index
        + encoded_entity_pk_compact_size(&entry.entity_pk)
        + encoded_str_size(&entry.change_id)
}

fn encoded_entity_pk_compact_size(identity: &crate::entity_pk::EntityPk) -> usize {
    if identity.parts.len() == 1 {
        1 + encoded_str_size(&identity.parts[0])
    } else {
        1 + 4
            + identity
                .parts
                .iter()
                .map(|part| encoded_str_size(part))
                .sum::<usize>()
    }
}

fn encoded_optional_str_size(value: Option<&str>) -> usize {
    1 + value.map(encoded_str_size).unwrap_or(0)
}

fn encoded_str_size(value: &str) -> usize {
    4 + value.len()
}

fn validate_unique<'a>(
    values: impl IntoIterator<Item = &'a str>,
    label: &str,
) -> Result<(), LixError> {
    let mut seen = HashSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(LixError::unknown(format!(
                "changelog append contains duplicate {label} '{value}'"
            )));
        }
    }
    Ok(())
}

fn validate_unique_ref_keys(entries: &[CommitChangeRef], commit_id: &str) -> Result<(), LixError> {
    let mut seen = HashSet::new();
    for entry in entries {
        let key = (
            entry.schema_key.as_str(),
            entry.file_id.as_deref(),
            &entry.entity_pk,
        );
        if !seen.insert(key) {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' has duplicate change ref key"
            )));
        }
    }
    Ok(())
}

fn validate_ref_matches_change(
    commit_id: &str,
    entry: &CommitChangeRef,
    change: &ChangeRecord,
) -> Result<(), LixError> {
    if entry.schema_key != change.schema_key
        || entry.file_id != change.file_id
        || entry.entity_pk != change.entity_pk
    {
        return Err(LixError::unknown(format!(
            "changelog commit '{}' change ref '{}' does not match referenced ChangeRecord key",
            commit_id, entry.change_id
        )));
    }
    Ok(())
}

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

fn native_get_many<R>(
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
    let result =
        PointReadPlan::new(space, &keys).materialize(read, StorageGetOptions::default())?;
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

fn native_scan<R>(
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
        resume_after: after_key.as_ref(),
    };
    let chunk = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::from(prefix),
        },
    )
    .collect(read, opts)?
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
    use crate::changelog::test_support::{changelog_test_context, test_append};
    use crate::changelog::{
        ChangeLoadRequest, ChangeRecord, ChangeScanRequest, ChangelogAppend, ChangelogReader,
        ChangelogWriter, CommitLoadEntry, CommitLoadRequest, CommitProjection, CommitScanRequest,
    };
    use crate::entity_pk::EntityPk;

    use super::*;

    fn ts(value: &str) -> crate::common::LixTimestamp {
        crate::common::LixTimestamp::expect_parse("timestamp", value)
    }

    fn test_change_ref(entity: &str, change_id: &str) -> CommitChangeRef {
        CommitChangeRef {
            schema_key: "message".to_string(),
            file_id: None,
            entity_pk: EntityPk::single(entity.to_string()),
            change_id: change_id.to_string(),
        }
    }

    #[test]
    fn chunk_one_commit_change_refs_splits_by_encoded_size() {
        let refs = CommitChangeRefSet {
            commit_id: "commit-1".to_string(),
            entries: (0..8)
                .map(|index| {
                    test_change_ref(
                        &format!("entity-{index:04}-{}", "x".repeat(24)),
                        &format!("change-{index:04}-{}", "y".repeat(24)),
                    )
                })
                .collect(),
        };

        let chunks = chunk_one_commit_change_refs(refs, 180, 260, 2048)
            .expect("refs should chunk under small test limit");

        assert!(chunks.len() > 1);
        assert!(chunks
            .iter()
            .all(|chunk| encode_commit_change_ref_chunk(chunk).unwrap().len() <= 260));
        assert_eq!(
            chunks
                .iter()
                .flat_map(|chunk| chunk.entries.iter())
                .map(|entry| entry.change_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "change-0000-yyyyyyyyyyyyyyyyyyyyyyyy",
                "change-0001-yyyyyyyyyyyyyyyyyyyyyyyy",
                "change-0002-yyyyyyyyyyyyyyyyyyyyyyyy",
                "change-0003-yyyyyyyyyyyyyyyyyyyyyyyy",
                "change-0004-yyyyyyyyyyyyyyyyyyyyyyyy",
                "change-0005-yyyyyyyyyyyyyyyyyyyyyyyy",
                "change-0006-yyyyyyyyyyyyyyyyyyyyyyyy",
                "change-0007-yyyyyyyyyyyyyyyyyyyyyyyy",
            ]
        );
    }

    #[test]
    fn chunk_one_commit_change_refs_splits_by_entry_count() {
        let refs = CommitChangeRefSet {
            commit_id: "commit-1".to_string(),
            entries: (0..5)
                .map(|index| {
                    test_change_ref(&format!("entity-{index}"), &format!("change-{index}"))
                })
                .collect(),
        };

        let chunks = chunk_one_commit_change_refs(refs, usize::MAX, usize::MAX, 2)
            .expect("refs should chunk by entry cap");

        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.entries.len())
                .collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
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
                commit_ids: &["commit-1".to_string()],
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
                .map(|entry| entry.change_id.as_str())
                .collect::<Vec<_>>(),
            vec!["change-1"]
        );

        let changes = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string(), "missing".to_string()],
            })
            .await
            .unwrap();
        assert_eq!(changes.entries[0].as_ref().unwrap().schema_key, "message");
        assert!(changes.entries[1].is_none());
    }

    #[tokio::test]
    async fn stage_append_rejects_ref_key_mismatch() {
        let (context, storage) = changelog_test_context();
        let mut append = test_append();
        append.commit_change_refs[0].entries[0].schema_key = "wrong".to_string();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_append(append).await.unwrap_err()
        };
        assert!(
            error
                .message
                .contains("does not match referenced ChangeRecord key"),
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
        append.commit_change_refs[0].entries[0].change_id = append.commits[0].change_id.clone();

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
    async fn stage_append_sorts_commit_change_refs_by_canonical_key() {
        let (context, storage) = changelog_test_context();
        let mut append = test_append();
        append.changes.push(ChangeRecord {
            format_version: 1,
            change_id: "change-0".to_string(),
            schema_key: "alpha".to_string(),
            entity_pk: EntityPk::single("entity-0"),
            file_id: None,
            snapshot_ref: None,
            metadata_ref: None,
            created_at: ts("2026-05-12T00:00:00Z"),
        });
        append.commit_change_refs[0].entries.insert(
            0,
            crate::changelog::CommitChangeRef {
                schema_key: "alpha".to_string(),
                file_id: None,
                entity_pk: EntityPk::single("entity-0"),
                change_id: "change-0".to_string(),
            },
        );
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
                commit_ids: &["commit-1".to_string()],
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
                .map(|entry| entry.change_id.as_str())
                .collect::<Vec<_>>(),
            vec!["change-0", "change-1"]
        );
    }

    #[tokio::test]
    async fn scan_commits_reads_direct_commit_records_in_key_order() {
        let (context, storage) = changelog_test_context();
        let mut first = test_append();
        first.commits[0].commit_id = "commit-b".to_string();
        first.commits[0].change_id = "commit-b-row-change".to_string();
        first.commit_change_refs[0].commit_id = "commit-b".to_string();

        let mut second = test_append();
        second.commits[0].commit_id = "commit-a".to_string();
        second.commits[0].change_id = "commit-a-row-change".to_string();
        second.changes[0].change_id = "change-a".to_string();
        second.commit_change_refs[0].commit_id = "commit-a".to_string();
        second.commit_change_refs[0].entries[0].change_id = "change-a".to_string();

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
        let scan = reader
            .scan_commits(CommitScanRequest {
                start_after: None,
                limit: Some(1),
                projection: CommitProjection::Record,
            })
            .await
            .unwrap();
        assert_eq!(scan.entries.len(), 1);
        assert_eq!(scan.next_start_after.as_deref(), Some("commit-a"));
        let CommitLoadEntry::Record(record) = &scan.entries[0] else {
            panic!("expected record projection");
        };
        assert_eq!(record.commit_id, "commit-a");

        let next = reader
            .scan_commits(CommitScanRequest {
                start_after: scan.next_start_after.as_deref(),
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
                record.commit_id.as_str()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["commit-b"]);
        assert_eq!(next.next_start_after, None);
    }

    #[tokio::test]
    async fn scan_changes_reads_direct_change_records_in_key_order() {
        let (context, storage) = changelog_test_context();
        let mut first = test_append();
        first.commits[0].commit_id = "commit-b".to_string();
        first.commits[0].change_id = "commit-b-row-change".to_string();
        first.changes[0].change_id = "change-b".to_string();
        first.commit_change_refs[0].commit_id = "commit-b".to_string();
        first.commit_change_refs[0].entries[0].change_id = "change-b".to_string();

        let mut second = test_append();
        second.commits[0].commit_id = "commit-a".to_string();
        second.commits[0].change_id = "commit-a-row-change".to_string();
        second.changes[0].change_id = "change-a".to_string();
        second.commit_change_refs[0].commit_id = "commit-a".to_string();
        second.commit_change_refs[0].entries[0].change_id = "change-a".to_string();

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
        let scan = reader
            .scan_changes(ChangeScanRequest {
                start_after: None,
                limit: Some(1),
            })
            .await
            .unwrap();
        assert_eq!(scan.entries.len(), 1);
        assert_eq!(scan.entries[0].change_id, "change-a");
        assert_eq!(scan.next_start_after.as_deref(), Some("change-a"));

        let next = reader
            .scan_changes(ChangeScanRequest {
                start_after: scan.next_start_after.as_deref(),
                limit: Some(10),
            })
            .await
            .unwrap();
        let ids = next
            .entries
            .iter()
            .map(|change| change.change_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["change-b"]);
        assert_eq!(next.next_start_after, None);
    }

    #[tokio::test]
    async fn scan_changes_pages_all_direct_change_records_without_gaps() {
        let (context, storage) = changelog_test_context();
        let changes = (0..2_500)
            .map(|index| ChangeRecord {
                format_version: 1,
                change_id: format!("change-{index:04}"),
                schema_key: "message".to_string(),
                entity_pk: EntityPk::single(format!("entity-{index:04}")),
                file_id: None,
                snapshot_ref: None,
                metadata_ref: None,
                created_at: ts("2026-05-20T00:00:00Z"),
            })
            .collect::<Vec<_>>();
        let expected_ids = changes
            .iter()
            .map(|change| change.change_id.clone())
            .collect::<Vec<_>>();

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
            start_after = Some(next_start_after);
        }

        assert_eq!(page_sizes, vec![1_024, 1_024, 452]);
        assert_eq!(scanned_ids, expected_ids);
    }
}
