use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use async_trait::async_trait;
use bytes::Bytes;

use super::by_change_index::by_change_entries_for_segments;
use super::by_change_membership_index::by_change_membership_entries_for_segments;
use super::by_commit_index::{by_commit_entries_for_segment, by_commit_entries_for_segments};
use super::segment::{
    canonicalize_segment, directory_change_location, directory_commit_location, segment_change,
    segment_commit, validate_change_checksum, validate_change_location, validate_commit_checksum,
    validate_commit_location, validate_segment_shape, validate_stage_segment_shape,
    DecodedSegmentIndex,
};
use super::store::{
    by_change_index_value, by_change_key, by_change_membership_commit_id_from_key,
    by_change_membership_index_value, by_change_membership_key, by_change_membership_prefix,
    by_commit_index_value, by_commit_key, commit_visibility_key, commit_visibility_value,
    segment_key, segment_value, visible_change_proof_key, visible_change_proof_value,
    BY_CHANGE_INDEX_SPACE, BY_CHANGE_MEMBERSHIP_INDEX_SPACE, BY_COMMIT_INDEX_SPACE,
    COMMIT_VISIBILITY_SPACE, SEGMENT_SPACE, VISIBLE_CHANGE_PROOF_SPACE,
};
use crate::changelog::{
    decode_by_change_entry, decode_by_commit_entry, decode_commit_visibility, decode_segment,
    decode_segment_change, decode_segment_commit, segment_commit_membership_contains_any,
    view_segment_directory,
};
use crate::changelog::{
    ByChangeEntry, ByCommitEntry, Change, ChangeLoadBatch, ChangeLoadEntry, ChangeLoadRequest,
    ChangeProjection, ChangeVisibilityMode, CommitLoadBatch, CommitLoadEntry, CommitLoadRequest,
    CommitProjection, CommitVisibility, CommitVisibilityMode, GcPlan, GcRoot, MembershipRole,
    RebuildIndexStats, Segment, SegmentChange, SegmentCommit, SegmentObjectLocation,
    SegmentStageReport, StateRowIdentity,
};
use crate::common::{CanonicalSchemaKey, EntityId, FileId};
use crate::storage::{
    PointReadPlan, ScanPlan, StorageBackend, StorageContext, StorageCoreProjection,
    StorageGetOptions, StorageKey, StorageKeyRange, StoragePrefix, StorageProjectedValue,
    StorageRead, StorageReadOptions, StorageScanOptions, StorageSpace, StorageWriteSet,
};
use crate::LixError;

/// Factory for changelog readers and writers.
///
/// The changelog owns durable commit/change truth and commit publication.
/// Callers choose the transaction/read boundary by supplying a storage reader
/// and transaction-local write set.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ChangelogContext;

impl ChangelogContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a reader over a caller-provided storage snapshot or transaction.
    pub(crate) fn reader<S>(&self, store: S) -> ChangelogStoreReader<S>
    where
        S: ChangelogStorageRead,
    {
        ChangelogStoreReader { store }
    }

    /// Creates a writer over read visibility and a pending write set.
    ///
    /// Changelog writes stage bytes into `writes`; the caller applies the write
    /// set to choose the atomic commit/rollback boundary.
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
            staged_segments: HashMap::new(),
            staged_commits: HashMap::new(),
            staged_changes: HashSet::new(),
            staged_generations: HashMap::new(),
            staged_publications: HashSet::new(),
            staged_visible_change_proofs: HashSet::new(),
        }
    }
}

/// Store-backed changelog reader created by [`ChangelogContext`].
pub(crate) struct ChangelogStoreReader<S> {
    store: S,
}

#[derive(Debug)]
pub(crate) struct ChangelogScanPage {
    pub(super) keys: Vec<Vec<u8>>,
    pub(super) values: Vec<Vec<u8>>,
    pub(super) resume_after: Option<Vec<u8>>,
}

impl ChangelogScanPage {
    pub(super) fn len(&self) -> usize {
        self.keys.len()
    }

    pub(super) fn key(&self, index: usize) -> Option<&[u8]> {
        self.keys.get(index).map(Vec::as_slice)
    }

    pub(super) fn value(&self, index: usize) -> Option<&[u8]> {
        self.values.get(index).map(Vec::as_slice)
    }
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

struct SegmentByteIndex {
    bytes: Vec<u8>,
    segment_id: String,
    commit_locations: HashMap<String, SegmentObjectLocation>,
    change_locations: HashMap<String, SegmentObjectLocation>,
}

impl SegmentByteIndex {
    fn decode(bytes: Vec<u8>) -> Result<Self, LixError> {
        let view = view_segment_directory(&bytes)?;
        let segment_id = view.segment_id.to_string();
        let commit_locations = view
            .directory_commits
            .iter()
            .map(|entry| {
                (
                    entry.id.to_string(),
                    SegmentObjectLocation {
                        segment_id: entry.location.segment_id.to_string(),
                        offset: entry.location.offset,
                        len: entry.location.len,
                        checksum: entry.location.checksum.to_string(),
                    },
                )
            })
            .collect();
        let change_locations = view
            .directory_changes
            .iter()
            .map(|entry| {
                (
                    entry.id.to_string(),
                    SegmentObjectLocation {
                        segment_id: entry.location.segment_id.to_string(),
                        offset: entry.location.offset,
                        len: entry.location.len,
                        checksum: entry.location.checksum.to_string(),
                    },
                )
            })
            .collect();
        Ok(Self {
            bytes,
            segment_id,
            commit_locations,
            change_locations,
        })
    }

    fn load_commit(
        &self,
        location: &SegmentObjectLocation,
        commit_id: &str,
    ) -> Result<SegmentCommit, LixError> {
        let expected = self.commit_locations.get(commit_id).ok_or_else(|| {
            LixError::unknown(format!(
                "changelog by_commit entry for '{commit_id}' points to segment '{}' without that commit",
                self.segment_id
            ))
        })?;
        if location != expected {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' locator does not match segment directory"
            )));
        }
        let bytes = self.object_bytes(location, "commit", commit_id)?;
        let commit = decode_segment_commit(bytes)?;
        if commit.header.id != commit_id {
            return Err(LixError::unknown(format!(
                "changelog commit locator for '{commit_id}' decoded commit '{}'",
                commit.header.id
            )));
        }
        validate_commit_checksum(&location.checksum, commit_id, &commit)?;
        Ok(commit)
    }

    fn prove_commit_membership(
        &self,
        location: &SegmentObjectLocation,
        commit_id: &str,
        requested_change_ids: &HashSet<String>,
    ) -> Result<Vec<String>, LixError> {
        let expected = self.commit_locations.get(commit_id).ok_or_else(|| {
            LixError::unknown(format!(
                "changelog by_commit entry for '{commit_id}' points to segment '{}' without that commit",
                self.segment_id
            ))
        })?;
        if location != expected {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' locator does not match segment directory"
            )));
        }
        let bytes = self.object_bytes(location, "commit", commit_id)?;
        segment_commit_membership_contains_any(
            bytes,
            commit_id,
            &location.checksum,
            requested_change_ids,
        )
    }

    fn load_change(
        &self,
        location: &SegmentObjectLocation,
        change_id: &str,
    ) -> Result<SegmentChange, LixError> {
        self.validate_change_location(location, change_id)?;
        let bytes = self.object_bytes(location, "change", change_id)?;
        let change = decode_segment_change(bytes)?;
        if change.id != change_id {
            return Err(LixError::unknown(format!(
                "changelog change locator for '{change_id}' decoded change '{}'",
                change.id
            )));
        }
        Ok(change)
    }

    fn validate_change_location(
        &self,
        location: &SegmentObjectLocation,
        change_id: &str,
    ) -> Result<(), LixError> {
        let expected = self.change_locations.get(change_id).ok_or_else(|| {
            LixError::unknown(format!(
                "changelog by_change entry for '{change_id}' points to segment '{}' without that change",
                self.segment_id
            ))
        })?;
        if location != expected {
            return Err(LixError::unknown(format!(
                "changelog change '{change_id}' locator does not match segment directory"
            )));
        }
        self.object_bytes(location, "change", change_id)?;
        Ok(())
    }

    fn object_bytes(
        &self,
        location: &SegmentObjectLocation,
        kind: &str,
        id: &str,
    ) -> Result<&[u8], LixError> {
        if location.segment_id != self.segment_id {
            return Err(LixError::unknown(format!(
                "changelog {kind} '{id}' locator points to segment '{}' but loaded '{}'",
                location.segment_id, self.segment_id
            )));
        }
        let start = usize::try_from(location.offset).map_err(|_| {
            LixError::unknown(format!(
                "changelog {kind} '{id}' locator offset does not fit usize"
            ))
        })?;
        let len = usize::try_from(location.len).map_err(|_| {
            LixError::unknown(format!(
                "changelog {kind} '{id}' locator len does not fit usize"
            ))
        })?;
        let end = start.checked_add(len).ok_or_else(|| {
            LixError::unknown(format!("changelog {kind} '{id}' locator range overflows"))
        })?;
        self.bytes.get(start..end).ok_or_else(|| {
            LixError::unknown(format!(
                "changelog {kind} '{id}' locator range is outside segment '{}'",
                self.segment_id
            ))
        })
    }
}

#[derive(Default)]
struct SourceParentFacts {
    reachable_memberships: HashSet<String>,
    first_parent_winners: HashMap<StateRowIdentity, String>,
}

impl<S> ChangelogStoreReader<S>
where
    S: ChangelogStorageRead,
{
    pub(crate) async fn plan_gc(&mut self, roots: &[GcRoot]) -> Result<GcPlan, LixError> {
        super::gc::plan_gc(&mut self.store, roots).await
    }

    pub(crate) async fn load_commits(
        &mut self,
        request: CommitLoadRequest<'_>,
    ) -> Result<CommitLoadBatch, LixError> {
        let entries = match request.visibility {
            CommitVisibilityMode::RequireVisible => {
                self.load_visible_commit_entries(request.commit_ids, request.projection)
                    .await?
            }
            CommitVisibilityMode::PhysicalOnly => {
                self.load_physical_commit_entries(request.commit_ids, request.projection)
                    .await?
            }
        };
        Ok(CommitLoadBatch { entries })
    }

    pub(crate) async fn scan_commit_visibilities(
        &mut self,
    ) -> Result<Vec<CommitVisibility>, LixError> {
        scan_commit_visibilities_from_store(&mut self.store).await
    }

    async fn load_visible_commit_entries(
        &mut self,
        commit_ids: &[String],
        projection: CommitProjection,
    ) -> Result<Vec<Option<CommitLoadEntry>>, LixError> {
        let visibilities = self.load_commit_visibility_many(commit_ids).await?;
        let mut segment_ids = Vec::new();
        for visibility in visibilities.iter().flatten() {
            push_unique(&mut segment_ids, visibility.location.segment_id.clone());
        }
        let segments = self.load_segment_byte_indexes_by_id(&segment_ids).await?;
        let mut entries = Vec::with_capacity(commit_ids.len());
        for (commit_id, visibility) in commit_ids.iter().zip(visibilities.iter()) {
            let Some(visibility) = visibility else {
                entries.push(None);
                continue;
            };
            if visibility.commit_id != *commit_id {
                return Err(LixError::unknown(format!(
                    "commit_visibility key for '{commit_id}' contains commit_id '{}'",
                    visibility.commit_id
                )));
            }
            let Some(segment) = segments.get(&visibility.location.segment_id) else {
                return Err(LixError::unknown(format!(
                    "visible changelog commit '{commit_id}' points to missing segment '{}'",
                    visibility.location.segment_id
                )));
            };
            let commit = segment.load_commit(&visibility.location, commit_id)?;
            validate_commit_checksum(&visibility.checksum, commit_id, &commit)?;
            entries.push(Some(project_segment_commit(&commit, projection)));
        }
        Ok(entries)
    }

    async fn load_physical_commit_entries(
        &mut self,
        commit_ids: &[String],
        projection: CommitProjection,
    ) -> Result<Vec<Option<CommitLoadEntry>>, LixError> {
        let by_commit_entries = self.load_by_commit_many(commit_ids).await?;
        let mut segment_ids = Vec::new();
        for entry in by_commit_entries.iter().flatten() {
            push_unique(&mut segment_ids, entry.location.segment_id.clone());
        }
        let segments = self.load_segment_byte_indexes_by_id(&segment_ids).await?;
        let mut entries = Vec::with_capacity(commit_ids.len());
        for (commit_id, by_commit) in commit_ids.iter().zip(by_commit_entries.iter()) {
            let Some(by_commit) = by_commit else {
                entries.push(None);
                continue;
            };
            if by_commit.commit_id != *commit_id {
                return Err(LixError::unknown(format!(
                    "by_commit key for '{commit_id}' contains commit_id '{}'",
                    by_commit.commit_id
                )));
            }
            let Some(segment) = segments.get(&by_commit.location.segment_id) else {
                return Err(LixError::unknown(format!(
                    "changelog by_commit entry for '{commit_id}' points to missing segment '{}'",
                    by_commit.location.segment_id
                )));
            };
            let commit = segment.load_commit(&by_commit.location, commit_id)?;
            entries.push(Some(project_segment_commit(&commit, projection)));
        }
        Ok(entries)
    }

    pub(crate) async fn load_changes(
        &mut self,
        request: ChangeLoadRequest<'_>,
    ) -> Result<ChangeLoadBatch, LixError> {
        let entries = match request.visibility {
            ChangeVisibilityMode::PhysicalOnly => {
                self.load_physical_change_entries(request.change_ids, request.projection)
                    .await?
            }
            ChangeVisibilityMode::RequireReachableFromVisibleCommit => {
                self.load_visible_change_entries(request.change_ids, request.projection)
                    .await?
            }
        };
        Ok(ChangeLoadBatch { entries })
    }

    async fn load_physical_change_entries(
        &mut self,
        change_ids: &[String],
        projection: ChangeProjection,
    ) -> Result<Vec<Option<ChangeLoadEntry>>, LixError> {
        let by_change_entries = self.load_by_change_many(change_ids).await?;
        let mut segment_ids = Vec::new();
        for entry in by_change_entries.iter().flatten() {
            push_unique(&mut segment_ids, entry.location.segment_id.clone());
        }
        let segments = self.load_segment_byte_indexes_by_id(&segment_ids).await?;
        let mut entries = Vec::with_capacity(change_ids.len());
        for (change_id, by_change) in change_ids.iter().zip(by_change_entries.iter()) {
            let Some(by_change) = by_change else {
                entries.push(None);
                continue;
            };
            if by_change.change_id != *change_id {
                return Err(LixError::unknown(format!(
                    "by_change key for '{change_id}' contains change_id '{}'",
                    by_change.change_id
                )));
            }
            let Some(segment) = segments.get(&by_change.location.segment_id) else {
                return Err(LixError::unknown(format!(
                    "changelog by_change entry for '{change_id}' points to missing segment '{}'",
                    by_change.location.segment_id
                )));
            };
            if projection == ChangeProjection::PhysicalLocation {
                let change = segment.load_change(&by_change.location, change_id)?;
                validate_change_checksum(&by_change.location.checksum, change_id, &change)?;
                entries.push(Some(ChangeLoadEntry::PhysicalLocation(
                    by_change.location.clone(),
                )));
                continue;
            }
            let change = segment.load_change(&by_change.location, change_id)?;
            validate_change_checksum(&by_change.location.checksum, change_id, &change)?;
            entries.push(Some(project_change_with_location(
                by_change.location.clone(),
                &change,
                projection,
            )));
        }
        Ok(entries)
    }

    async fn load_visible_change_entries(
        &mut self,
        change_ids: &[String],
        projection: ChangeProjection,
    ) -> Result<Vec<Option<ChangeLoadEntry>>, LixError> {
        let by_change_entries = self.load_by_change_many(change_ids).await?;
        let mut segment_ids = Vec::new();
        for entry in by_change_entries.iter().flatten() {
            push_unique(&mut segment_ids, entry.location.segment_id.clone());
        }
        let segments = self.load_segment_byte_indexes_by_id(&segment_ids).await?;
        let visible_change_ids = self.prove_visible_changes(change_ids).await?;
        let mut entries = Vec::with_capacity(change_ids.len());
        for (change_id, by_change) in change_ids.iter().zip(by_change_entries.iter()) {
            if !visible_change_ids.contains(change_id) {
                entries.push(None);
                continue;
            }
            let physical = if let Some(by_change) = by_change {
                if by_change.change_id != *change_id {
                    return Err(LixError::unknown(format!(
                        "by_change key for '{change_id}' contains change_id '{}'",
                        by_change.change_id
                    )));
                }
                if let Some(segment) = segments.get(&by_change.location.segment_id) {
                    if projection == ChangeProjection::PhysicalLocation {
                        let change = segment.load_change(&by_change.location, change_id)?;
                        validate_change_checksum(&by_change.location.checksum, change_id, &change)?;
                        entries.push(Some(ChangeLoadEntry::PhysicalLocation(
                            by_change.location.clone(),
                        )));
                        continue;
                    }
                    let change = segment.load_change(&by_change.location, change_id)?;
                    validate_change_checksum(&by_change.location.checksum, change_id, &change)?;
                    Some((by_change.location.clone(), change))
                } else {
                    return Err(LixError::unknown(format!(
                        "changelog by_change entry for visible change '{change_id}' points to missing segment '{}'",
                        by_change.location.segment_id
                    )));
                }
            } else {
                self.find_segment_change(change_id).await?
            };
            let Some((location, change)) = physical else {
                return Err(LixError::unknown(format!(
                    "visible changelog change '{change_id}' is referenced by a visible commit but no physical change exists"
                )));
            };
            entries.push(Some(project_change_with_location(
                location, &change, projection,
            )));
        }
        Ok(entries)
    }

    async fn load_commit_visibility(
        &mut self,
        commit_id: &str,
    ) -> Result<Option<CommitVisibility>, LixError> {
        get_one(
            &mut self.store,
            COMMIT_VISIBILITY_SPACE,
            commit_visibility_key(commit_id),
        )
        .await?
        .map(|bytes| {
            let visibility = decode_commit_visibility(&bytes)?;
            if visibility.commit_id != commit_id {
                return Err(LixError::unknown(format!(
                    "commit_visibility key for '{commit_id}' contains commit_id '{}'",
                    visibility.commit_id
                )));
            }
            Ok(visibility)
        })
        .transpose()
    }

    async fn load_by_commit(&mut self, commit_id: &str) -> Result<Option<ByCommitEntry>, LixError> {
        get_one(
            &mut self.store,
            BY_COMMIT_INDEX_SPACE,
            by_commit_key(commit_id),
        )
        .await?
        .map(|bytes| {
            let entry = decode_by_commit_entry(&bytes)?;
            if entry.commit_id != commit_id {
                return Err(LixError::unknown(format!(
                    "by_commit key for '{commit_id}' contains commit_id '{}'",
                    entry.commit_id
                )));
            }
            Ok(entry)
        })
        .transpose()
    }

    async fn load_by_change(&mut self, change_id: &str) -> Result<Option<ByChangeEntry>, LixError> {
        get_one(
            &mut self.store,
            BY_CHANGE_INDEX_SPACE,
            by_change_key(change_id),
        )
        .await?
        .map(|bytes| {
            let entry = decode_by_change_entry(&bytes)?;
            if entry.change_id != change_id {
                return Err(LixError::unknown(format!(
                    "by_change key for '{change_id}' contains change_id '{}'",
                    entry.change_id
                )));
            }
            Ok(entry)
        })
        .transpose()
    }

    async fn load_commit_visibility_many(
        &mut self,
        commit_ids: &[String],
    ) -> Result<Vec<Option<CommitVisibility>>, LixError> {
        let values = get_many(
            &mut self.store,
            COMMIT_VISIBILITY_SPACE,
            commit_ids
                .iter()
                .map(|commit_id| commit_visibility_key(commit_id))
                .collect(),
        )
        .await?;
        values
            .into_iter()
            .zip(commit_ids.iter())
            .map(|value| {
                let (value, commit_id) = value;
                value
                    .map(|bytes| {
                        let visibility = decode_commit_visibility(&bytes)?;
                        if visibility.commit_id != *commit_id {
                            return Err(LixError::unknown(format!(
                                "commit_visibility key for '{commit_id}' contains commit_id '{}'",
                                visibility.commit_id
                            )));
                        }
                        Ok(visibility)
                    })
                    .transpose()
            })
            .collect()
    }

    async fn load_visible_change_proofs_many(
        &mut self,
        change_ids: &[String],
    ) -> Result<Vec<Option<CommitVisibility>>, LixError> {
        let values = get_many(
            &mut self.store,
            VISIBLE_CHANGE_PROOF_SPACE,
            change_ids
                .iter()
                .map(|change_id| visible_change_proof_key(change_id))
                .collect(),
        )
        .await?;
        values
            .into_iter()
            .map(|value| {
                value
                    .map(|bytes| decode_commit_visibility(&bytes))
                    .transpose()
            })
            .collect()
    }

    async fn load_by_commit_many(
        &mut self,
        commit_ids: &[String],
    ) -> Result<Vec<Option<ByCommitEntry>>, LixError> {
        let values = get_many(
            &mut self.store,
            BY_COMMIT_INDEX_SPACE,
            commit_ids
                .iter()
                .map(|commit_id| by_commit_key(commit_id))
                .collect(),
        )
        .await?;
        values
            .into_iter()
            .zip(commit_ids.iter())
            .map(|value| {
                let (value, commit_id) = value;
                value
                    .map(|bytes| {
                        let entry = decode_by_commit_entry(&bytes)?;
                        if entry.commit_id != *commit_id {
                            return Err(LixError::unknown(format!(
                                "by_commit key for '{commit_id}' contains commit_id '{}'",
                                entry.commit_id
                            )));
                        }
                        Ok(entry)
                    })
                    .transpose()
            })
            .collect()
    }

    async fn load_by_change_many(
        &mut self,
        change_ids: &[String],
    ) -> Result<Vec<Option<ByChangeEntry>>, LixError> {
        let values = get_many(
            &mut self.store,
            BY_CHANGE_INDEX_SPACE,
            change_ids
                .iter()
                .map(|change_id| by_change_key(change_id))
                .collect(),
        )
        .await?;
        values
            .into_iter()
            .zip(change_ids.iter())
            .map(|value| {
                let (value, change_id) = value;
                value
                    .map(|bytes| {
                        let entry = decode_by_change_entry(&bytes)?;
                        if entry.change_id != *change_id {
                            return Err(LixError::unknown(format!(
                                "by_change key for '{change_id}' contains change_id '{}'",
                                entry.change_id
                            )));
                        }
                        Ok(entry)
                    })
                    .transpose()
            })
            .collect()
    }

    async fn load_segment_indexes_by_id(
        &mut self,
        segment_ids: &[String],
    ) -> Result<HashMap<String, DecodedSegmentIndex>, LixError> {
        let values = get_many(
            &mut self.store,
            SEGMENT_SPACE,
            segment_ids
                .iter()
                .map(|segment_id| segment_key(segment_id))
                .collect(),
        )
        .await?;
        let mut out = HashMap::new();
        for (segment_id, value) in segment_ids.iter().zip(values.into_iter()) {
            if let Some(bytes) = value {
                out.insert(segment_id.clone(), DecodedSegmentIndex::decode(&bytes)?);
            }
        }
        Ok(out)
    }

    async fn load_segment_byte_index(
        &mut self,
        segment_id: &str,
    ) -> Result<Option<SegmentByteIndex>, LixError> {
        let Some(bytes) = get_one(&mut self.store, SEGMENT_SPACE, segment_key(segment_id)).await?
        else {
            return Ok(None);
        };
        Ok(Some(SegmentByteIndex::decode(bytes)?))
    }

    async fn load_segment_byte_indexes_by_id(
        &mut self,
        segment_ids: &[String],
    ) -> Result<HashMap<String, SegmentByteIndex>, LixError> {
        let values = get_many(
            &mut self.store,
            SEGMENT_SPACE,
            segment_ids
                .iter()
                .map(|segment_id| segment_key(segment_id))
                .collect(),
        )
        .await?;
        let mut out = HashMap::new();
        for (segment_id, value) in segment_ids.iter().zip(values.into_iter()) {
            if let Some(bytes) = value {
                out.insert(segment_id.clone(), SegmentByteIndex::decode(bytes)?);
            }
        }
        Ok(out)
    }

    async fn load_change_membership_candidates(
        &mut self,
        change_id: &str,
    ) -> Result<Vec<String>, LixError> {
        let prefix = by_change_membership_prefix(change_id);
        let mut after = None;
        let mut out = Vec::new();
        loop {
            let page = self
                .store
                .changelog_scan(
                    BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
                    prefix.clone(),
                    after,
                    256,
                    StorageCoreProjection::KeyOnly,
                )
                .await?;
            for index in 0..page.keys.len() {
                let Some(key) = page.keys.get(index) else {
                    continue;
                };
                if let Some(commit_id) = by_change_membership_commit_id_from_key(change_id, key)? {
                    out.push(commit_id);
                }
            }
            let Some(next_after) = page.resume_after else {
                break;
            };
            after = Some(next_after);
        }
        Ok(out)
    }

    async fn prove_visible_changes(
        &mut self,
        change_ids: &[String],
    ) -> Result<HashSet<String>, LixError> {
        let requested: HashSet<String> = change_ids.iter().cloned().collect();
        if requested.is_empty() {
            return Ok(HashSet::new());
        }

        let (mut proven, mut checked_commits) = self
            .prove_visible_changes_from_native_proofs(change_ids)
            .await?;
        if proven.len() == requested.len() {
            return Ok(proven);
        }

        let mut candidate_changes_by_commit: HashMap<String, HashSet<String>> = HashMap::new();
        for change_id in requested.difference(&proven) {
            for commit_id in self.load_change_membership_candidates(change_id).await? {
                candidate_changes_by_commit
                    .entry(commit_id)
                    .or_default()
                    .insert(change_id.clone());
            }
        }

        for (commit_id, candidate_change_ids) in candidate_changes_by_commit {
            if checked_commits.insert(commit_id.clone()) {
                self.prove_visible_changes_from_commit(
                    &commit_id,
                    &candidate_change_ids,
                    &mut proven,
                )
                .await?;
            }
            if proven.len() == requested.len() {
                return Ok(proven);
            }
        }

        let remaining = requested
            .difference(&proven)
            .cloned()
            .collect::<HashSet<_>>();
        if remaining.is_empty() {
            return Ok(proven);
        }
        self.scan_visible_commits_for_changes(&remaining, checked_commits, &mut proven)
            .await?;
        Ok(proven)
    }

    async fn prove_visible_changes_from_native_proofs(
        &mut self,
        change_ids: &[String],
    ) -> Result<(HashSet<String>, HashSet<String>), LixError> {
        let proofs = self.load_visible_change_proofs_many(change_ids).await?;
        let mut changes_by_commit: HashMap<String, HashSet<String>> = HashMap::new();
        let mut proof_by_commit = HashMap::new();
        for (change_id, proof) in change_ids.iter().zip(proofs.into_iter()) {
            let Some(proof) = proof else {
                continue;
            };
            proof_by_commit
                .entry(proof.commit_id.clone())
                .or_insert_with(|| proof.clone());
            changes_by_commit
                .entry(proof.commit_id)
                .or_default()
                .insert(change_id.clone());
        }
        if changes_by_commit.is_empty() {
            return Ok((HashSet::new(), HashSet::new()));
        }

        let commit_ids = changes_by_commit.keys().cloned().collect::<Vec<_>>();
        let current_visibilities = self.load_commit_visibility_many(&commit_ids).await?;
        let mut segment_ids = Vec::new();
        let mut usable = Vec::new();
        for (commit_id, current) in commit_ids.iter().zip(current_visibilities.into_iter()) {
            let Some(current) = current else {
                continue;
            };
            let Some(proof) = proof_by_commit.get(commit_id) else {
                continue;
            };
            if proof.location != current.location || proof.checksum != current.checksum {
                continue;
            }
            push_unique(&mut segment_ids, current.location.segment_id.clone());
            usable.push(current);
        }

        let segments = self.load_segment_byte_indexes_by_id(&segment_ids).await?;
        let mut proven = HashSet::new();
        let mut checked_commits = HashSet::new();
        for visibility in usable {
            checked_commits.insert(visibility.commit_id.clone());
            let Some(requested_change_ids) = changes_by_commit.get(&visibility.commit_id) else {
                continue;
            };
            let Some(segment) = segments.get(&visibility.location.segment_id) else {
                continue;
            };
            if visibility.checksum != visibility.location.checksum {
                continue;
            }
            for change_id in segment.prove_commit_membership(
                &visibility.location,
                &visibility.commit_id,
                requested_change_ids,
            )? {
                proven.insert(change_id);
            }
        }
        Ok((proven, checked_commits))
    }

    async fn load_segment(&mut self, segment_id: &str) -> Result<Option<Segment>, LixError> {
        let Some(bytes) = get_one(&mut self.store, SEGMENT_SPACE, segment_key(segment_id)).await?
        else {
            return Ok(None);
        };
        let segment = decode_segment(&bytes)?;
        validate_segment_shape(&segment)?;
        Ok(Some(segment))
    }

    async fn scan_all_segments(&mut self) -> Result<Vec<Segment>, LixError> {
        let mut after = None;
        let mut segments = Vec::new();
        loop {
            let page = self
                .store
                .changelog_scan(
                    SEGMENT_SPACE,
                    Vec::new(),
                    after,
                    64,
                    StorageCoreProjection::FullValue,
                )
                .await?;
            for index in 0..page.len() {
                let Some(bytes) = page.value(index) else {
                    continue;
                };
                let segment = decode_segment(bytes)?;
                validate_segment_shape(&segment)?;
                segments.push(segment);
            }
            let Some(next_after) = page.resume_after else {
                break;
            };
            after = Some(next_after);
        }
        Ok(segments)
    }

    async fn load_visible_commit_entry(
        &mut self,
        commit_id: &str,
        projection: CommitProjection,
    ) -> Result<Option<CommitLoadEntry>, LixError> {
        let Some(visibility) = self.load_commit_visibility(commit_id).await? else {
            return Ok(None);
        };
        let Some(segment) = self.load_segment(&visibility.location.segment_id).await? else {
            return Err(LixError::unknown(format!(
                "visible changelog commit '{commit_id}' points to missing segment '{}'",
                visibility.location.segment_id
            )));
        };
        let Some(commit) = segment_commit(&segment, commit_id) else {
            return Err(LixError::unknown(format!(
                "visible changelog commit '{commit_id}' was not found in segment '{}'",
                segment.header.segment_id
            )));
        };
        validate_commit_location(&visibility.location, &segment, commit_id)?;
        validate_commit_checksum(&visibility.checksum, commit_id, commit)?;
        Ok(Some(project_segment_commit(commit, projection)))
    }

    async fn load_physical_commit_entry(
        &mut self,
        commit_id: &str,
        projection: CommitProjection,
    ) -> Result<Option<CommitLoadEntry>, LixError> {
        let Some(entry) = self.load_by_commit(commit_id).await? else {
            return Ok(None);
        };
        let Some(segment) = self.load_segment(&entry.location.segment_id).await? else {
            return Err(LixError::unknown(format!(
                "changelog by_commit entry for '{commit_id}' points to missing segment '{}'",
                entry.location.segment_id
            )));
        };
        let Some(commit) = segment_commit(&segment, commit_id) else {
            return Err(LixError::unknown(format!(
                "changelog by_commit entry for '{commit_id}' points to segment '{}' without that commit",
                segment.header.segment_id
            )));
        };
        validate_commit_location(&entry.location, &segment, commit_id)?;
        Ok(Some(project_segment_commit(commit, projection)))
    }

    async fn load_physical_change_entry(
        &mut self,
        change_id: &str,
        projection: ChangeProjection,
    ) -> Result<Option<ChangeLoadEntry>, LixError> {
        let Some((location, change)) = self.load_physical_segment_change(change_id).await? else {
            return Ok(None);
        };
        if projection == ChangeProjection::PhysicalLocation {
            return Ok(Some(ChangeLoadEntry::PhysicalLocation(location)));
        }
        Ok(Some(project_segment_change(&change, projection)))
    }

    async fn load_visible_change_entry(
        &mut self,
        change_id: &str,
        projection: ChangeProjection,
    ) -> Result<Option<ChangeLoadEntry>, LixError> {
        if !self.visible_membership_contains_change(change_id).await? {
            return Ok(None);
        }
        let Some((location, change)) = self.find_segment_change(change_id).await? else {
            return Err(LixError::unknown(format!(
                "visible changelog change '{change_id}' is referenced by a visible commit but no physical change exists"
            )));
        };
        if projection == ChangeProjection::PhysicalLocation {
            return Ok(Some(ChangeLoadEntry::PhysicalLocation(location)));
        }
        Ok(Some(project_segment_change(&change, projection)))
    }

    async fn load_physical_segment_change(
        &mut self,
        change_id: &str,
    ) -> Result<Option<(SegmentObjectLocation, SegmentChange)>, LixError> {
        let Some(entry) = self.load_by_change(change_id).await? else {
            return Ok(None);
        };
        let Some(segment) = self.load_segment(&entry.location.segment_id).await? else {
            return Err(LixError::unknown(format!(
                "changelog by_change entry for '{change_id}' points to missing segment '{}'",
                entry.location.segment_id
            )));
        };
        let Some(change) = segment_change(&segment, change_id) else {
            return Err(LixError::unknown(format!(
                "changelog by_change entry for '{change_id}' points to segment '{}' without that change",
                segment.header.segment_id
            )));
        };
        validate_change_location(&entry.location, &segment, change_id)?;
        validate_change_checksum(&entry.location.checksum, change_id, change)?;
        Ok(Some((entry.location, change.clone())))
    }

    async fn find_segment_change(
        &mut self,
        change_id: &str,
    ) -> Result<Option<(SegmentObjectLocation, SegmentChange)>, LixError> {
        match self.load_physical_segment_change(change_id).await {
            Ok(Some(found)) => Ok(Some(found)),
            Ok(None) => self.scan_segments_for_change(change_id).await,
            Err(error) => Err(error),
        }
    }

    async fn scan_segments_for_change(
        &mut self,
        change_id: &str,
    ) -> Result<Option<(SegmentObjectLocation, SegmentChange)>, LixError> {
        let mut after = None;
        loop {
            let page = self
                .store
                .changelog_scan(
                    SEGMENT_SPACE,
                    Vec::new(),
                    after,
                    64,
                    StorageCoreProjection::FullValue,
                )
                .await?;
            for index in 0..page.len() {
                let Some(bytes) = page.value(index) else {
                    continue;
                };
                let segment = decode_segment(bytes)?;
                validate_segment_shape(&segment)?;
                if let Some(change) = segment_change(&segment, change_id) {
                    let location = directory_change_location(&segment, change_id)?;
                    validate_change_checksum(&location.checksum, change_id, change)?;
                    return Ok(Some((location, change.clone())));
                }
            }
            let Some(next_after) = page.resume_after else {
                break;
            };
            after = Some(next_after);
        }
        Ok(None)
    }

    async fn visible_membership_contains_change(
        &mut self,
        change_id: &str,
    ) -> Result<bool, LixError> {
        let candidates = self.load_change_membership_candidates(change_id).await?;
        let mut checked = HashSet::new();
        for commit_id in candidates {
            checked.insert(commit_id.clone());
            if self
                .visible_commit_membership_contains_change(&commit_id, change_id)
                .await?
            {
                return Ok(true);
            }
        }
        self.scan_visible_commits_for_change(change_id, checked)
            .await
    }

    async fn visible_commit_membership_contains_change(
        &mut self,
        commit_id: &str,
        change_id: &str,
    ) -> Result<bool, LixError> {
        let mut proven = HashSet::new();
        self.prove_visible_changes_from_commit(
            commit_id,
            &HashSet::from([change_id.to_string()]),
            &mut proven,
        )
        .await?;
        Ok(proven.contains(change_id))
    }

    async fn prove_visible_changes_from_commit(
        &mut self,
        commit_id: &str,
        requested_change_ids: &HashSet<String>,
        proven: &mut HashSet<String>,
    ) -> Result<(), LixError> {
        let Some(visibility) = self.load_commit_visibility(commit_id).await? else {
            return Ok(());
        };
        let Some(segment) = self
            .load_segment_byte_index(&visibility.location.segment_id)
            .await?
        else {
            return Err(LixError::unknown(format!(
                "visible changelog commit '{commit_id}' points to missing segment '{}'",
                visibility.location.segment_id
            )));
        };
        if visibility.checksum != visibility.location.checksum {
            return Err(LixError::unknown(format!(
                "visible changelog commit '{commit_id}' checksum does not match physical locator checksum"
            )));
        }
        for change_id in segment.prove_commit_membership(
            &visibility.location,
            commit_id,
            requested_change_ids,
        )? {
            proven.insert(change_id);
        }
        Ok(())
    }

    async fn scan_visible_commits_for_change(
        &mut self,
        change_id: &str,
        mut checked: HashSet<String>,
    ) -> Result<bool, LixError> {
        let mut after = None;
        loop {
            let page = self
                .store
                .changelog_scan(
                    COMMIT_VISIBILITY_SPACE,
                    Vec::new(),
                    after,
                    256,
                    StorageCoreProjection::KeyOnly,
                )
                .await?;
            for index in 0..page.keys.len() {
                let Some(key) = page.keys.get(index) else {
                    continue;
                };
                let commit_id = std::str::from_utf8(key).map_err(|error| {
                    LixError::unknown(format!(
                        "changelog commit_visibility key contains invalid UTF-8: {error}"
                    ))
                })?;
                if checked.insert(commit_id.to_string())
                    && self
                        .visible_commit_membership_contains_change(commit_id, change_id)
                        .await?
                {
                    return Ok(true);
                }
            }
            let Some(next_after) = page.resume_after else {
                break;
            };
            after = Some(next_after);
        }
        Ok(false)
    }

    async fn scan_visible_commits_for_changes(
        &mut self,
        requested_change_ids: &HashSet<String>,
        mut checked: HashSet<String>,
        proven: &mut HashSet<String>,
    ) -> Result<(), LixError> {
        let mut after = None;
        loop {
            let page = self
                .store
                .changelog_scan(
                    COMMIT_VISIBILITY_SPACE,
                    Vec::new(),
                    after,
                    256,
                    StorageCoreProjection::KeyOnly,
                )
                .await?;
            for index in 0..page.keys.len() {
                let Some(key) = page.keys.get(index) else {
                    continue;
                };
                let commit_id = std::str::from_utf8(key).map_err(|error| {
                    LixError::unknown(format!(
                        "changelog commit_visibility key contains invalid UTF-8: {error}"
                    ))
                })?;
                if checked.insert(commit_id.to_string()) {
                    self.prove_visible_changes_from_commit(commit_id, requested_change_ids, proven)
                        .await?;
                    if requested_change_ids
                        .iter()
                        .all(|change_id| proven.contains(change_id))
                    {
                        return Ok(());
                    }
                }
            }
            let Some(next_after) = page.resume_after else {
                break;
            };
            after = Some(next_after);
        }
        Ok(())
    }
}

/// Changelog writer over a transaction-local storage write set.
pub(crate) struct ChangelogStoreWriter<'a, S: ?Sized> {
    store: &'a mut S,
    writes: &'a mut StorageWriteSet,
    staged_segments: HashMap<String, Segment>,
    staged_commits: HashMap<String, SegmentObjectLocation>,
    staged_changes: HashSet<String>,
    staged_generations: HashMap<String, u64>,
    staged_publications: HashSet<String>,
    staged_visible_change_proofs: HashSet<String>,
}

impl<S> ChangelogStoreWriter<'_, S>
where
    S: ChangelogStorageRead + ?Sized,
{
    pub(crate) async fn stage_segment(
        &mut self,
        segment: Segment,
    ) -> Result<SegmentStageReport, LixError> {
        let segment = canonicalize_segment(segment)?;
        validate_stage_segment_shape(&segment)?;
        self.validate_stage_adopted_membership_provenance(&segment)
            .await?;
        self.reject_duplicate_logical_ids(&segment).await?;
        let segment_id = segment.header.segment_id.clone();
        let report = SegmentStageReport {
            segment_id: segment_id.clone(),
            commit_locations: segment.directory.commits.clone(),
            change_locations: segment.directory.changes.clone(),
        };
        self.writes.put(
            SEGMENT_SPACE,
            segment_key(&segment_id),
            segment_value(&segment)?,
        );
        self.staged_segments
            .insert(segment_id.clone(), segment.clone());

        let external_generations = self.external_generations_for_segment(&segment).await?;
        let by_commit_entries = by_commit_entries_for_segment(&segment, &external_generations)?;
        for entry in by_commit_entries {
            self.writes.put(
                BY_COMMIT_INDEX_SPACE,
                by_commit_key(&entry.commit_id),
                by_commit_index_value(&entry)?,
            );
            self.staged_commits
                .insert(entry.commit_id.clone(), entry.location.clone());
            self.staged_generations
                .insert(entry.commit_id.clone(), entry.generation);
        }

        for entry in by_change_membership_entries_for_segments(std::slice::from_ref(&segment)) {
            self.writes.put(
                BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
                by_change_membership_key(&entry.change_id, &entry.commit_id),
                by_change_membership_index_value(),
            );
        }

        for entry in by_change_entries_for_segments(std::slice::from_ref(&segment))? {
            self.writes.put(
                BY_CHANGE_INDEX_SPACE,
                by_change_key(&entry.change_id),
                by_change_index_value(&entry)?,
            );
            self.staged_changes.insert(entry.change_id.clone());
        }

        Ok(report)
    }

    async fn reject_duplicate_logical_ids(&mut self, segment: &Segment) -> Result<(), LixError> {
        let commit_ids = segment
            .commits
            .iter()
            .map(|commit| commit.header.id.as_str())
            .collect::<HashSet<_>>();
        let change_ids = segment
            .changes
            .iter()
            .map(|change| change.id.as_str())
            .collect::<HashSet<_>>();

        for commit in &segment.commits {
            if self.staged_commits.contains_key(&commit.header.id) {
                return Err(LixError::unknown(format!(
                    "changelog commit '{}' already exists in another segment",
                    commit.header.id
                )));
            }
        }
        for change in &segment.changes {
            if self.staged_changes.contains(&change.id) {
                return Err(LixError::unknown(format!(
                    "changelog change '{}' already exists in another segment",
                    change.id
                )));
            }
        }

        for existing_segment in self.scan_all_segments().await? {
            if existing_segment.header.segment_id == segment.header.segment_id {
                return Err(LixError::unknown(format!(
                    "changelog segment '{}' already exists",
                    segment.header.segment_id
                )));
            }
            for commit in &existing_segment.commits {
                if commit_ids.contains(commit.header.id.as_str()) {
                    return Err(LixError::unknown(format!(
                        "changelog commit '{}' already exists in another segment",
                        commit.header.id
                    )));
                }
            }
            for change in &existing_segment.changes {
                if change_ids.contains(change.id.as_str()) {
                    return Err(LixError::unknown(format!(
                        "changelog change '{}' already exists in another segment",
                        change.id
                    )));
                }
            }
        }
        Ok(())
    }

    async fn external_generations_for_segment(
        &mut self,
        segment: &Segment,
    ) -> Result<HashMap<String, u64>, LixError> {
        let local_commit_ids = segment
            .commits
            .iter()
            .map(|commit| commit.header.id.as_str())
            .collect::<HashSet<_>>();
        let mut out = self.staged_generations.clone();
        let mut external_parent_ids = HashSet::new();
        for commit in &segment.commits {
            for parent_id in &commit.header.parent_commit_ids {
                if !local_commit_ids.contains(parent_id.as_str())
                    && !out.contains_key(parent_id.as_str())
                {
                    external_parent_ids.insert(parent_id.clone());
                }
            }
        }
        if external_parent_ids.is_empty() {
            return Ok(out);
        }

        let mut segments = self.scan_all_segments().await?;
        for staged in self.staged_segments.values() {
            if !segments
                .iter()
                .any(|segment| segment.header.segment_id == staged.header.segment_id)
            {
                segments.push(staged.clone());
            }
        }
        let entries = by_commit_entries_for_segments(&segments)?;
        for entry in entries {
            if external_parent_ids.contains(&entry.commit_id) {
                out.insert(entry.commit_id, entry.generation);
            }
        }
        Ok(out)
    }

    pub(crate) async fn stage_publish_commit(&mut self, commit_id: &str) -> Result<(), LixError> {
        let location = if let Some(location) = self.staged_commits.get(commit_id).cloned() {
            location
        } else {
            self.load_stored_commit_location_from_segment_truth(commit_id)
                .await?
        };
        self.stage_publish_commit_at_location(commit_id, location)
            .await
    }

    async fn load_stored_commit_location_from_segment_truth(
        &mut self,
        commit_id: &str,
    ) -> Result<SegmentObjectLocation, LixError> {
        let mut after = None;
        let mut found = None::<SegmentObjectLocation>;
        loop {
            let page = self
                .store
                .changelog_scan(
                    SEGMENT_SPACE,
                    Vec::new(),
                    after,
                    64,
                    StorageCoreProjection::FullValue,
                )
                .await?;
            for index in 0..page.len() {
                let Some(bytes) = page.value(index) else {
                    continue;
                };
                let segment = DecodedSegmentIndex::decode(bytes)?;
                let Some(location) = segment.commit_location(commit_id) else {
                    continue;
                };
                if let Some(existing) = &found {
                    return Err(LixError::unknown(format!(
                        "cannot publish changelog commit '{commit_id}' because multiple segments contain it: '{}' and '{}'",
                        existing.segment_id, location.segment_id
                    )));
                }
                found = Some(location.clone());
            }
            let Some(next_after) = page.resume_after else {
                break;
            };
            after = Some(next_after);
        }
        found.ok_or_else(|| {
            LixError::unknown(format!(
                "cannot publish changelog commit '{commit_id}' because no segment contains it"
            ))
        })
    }

    pub(crate) async fn stage_publish_commit_at_location(
        &mut self,
        commit_id: &str,
        location: SegmentObjectLocation,
    ) -> Result<(), LixError> {
        let commit = self.load_publish_commit(commit_id, &location).await?;
        self.validate_publish_membership_closure(&commit).await?;
        self.validate_publish_parents(&commit).await?;
        let visibility = CommitVisibility {
            commit_id: commit_id.to_string(),
            checksum: location.checksum.clone(),
            location,
        };
        self.writes.put(
            COMMIT_VISIBILITY_SPACE,
            commit_visibility_key(&visibility.commit_id),
            commit_visibility_value(&visibility)?,
        );
        for membership in &commit.body.membership {
            if self
                .staged_visible_change_proofs
                .insert(membership.member_change_id.clone())
            {
                self.writes.put(
                    VISIBLE_CHANGE_PROOF_SPACE,
                    visible_change_proof_key(&membership.member_change_id),
                    visible_change_proof_value(&visibility)?,
                );
            }
        }
        self.staged_publications.insert(commit_id.to_string());
        Ok(())
    }

    async fn load_publish_commit(
        &mut self,
        commit_id: &str,
        location: &SegmentObjectLocation,
    ) -> Result<SegmentCommit, LixError> {
        if let Some(segment) = self.staged_segments.get(&location.segment_id) {
            let Some(commit) = segment_commit(segment, commit_id) else {
                return Err(LixError::unknown(format!(
                    "cannot publish changelog commit '{commit_id}' because staged segment '{}' does not contain it",
                    location.segment_id
                )));
            };
            validate_commit_location(location, segment, commit_id)?;
            validate_commit_checksum(&location.checksum, commit_id, commit)?;
            return Ok(commit.clone());
        }
        let Some(bytes) = get_one(
            &mut *self.store,
            SEGMENT_SPACE,
            segment_key(&location.segment_id),
        )
        .await?
        else {
            return Err(LixError::unknown(format!(
                "cannot publish changelog commit '{commit_id}' because segment '{}' is missing",
                location.segment_id
            )));
        };
        let segment = DecodedSegmentIndex::decode(&bytes)?;
        let Some(commit) = segment.commit(commit_id)? else {
            return Err(LixError::unknown(format!(
                "cannot publish changelog commit '{commit_id}' because segment '{}' does not contain it",
                location.segment_id
            )));
        };
        segment.validate_commit_location(location, commit_id)?;
        validate_commit_checksum(&location.checksum, commit_id, &commit)?;
        Ok(commit)
    }

    async fn validate_publish_parents(&mut self, commit: &SegmentCommit) -> Result<(), LixError> {
        for parent_id in &commit.header.parent_commit_ids {
            if self.staged_publications.contains(parent_id) {
                continue;
            }
            if self.stored_commit_visibility_is_valid(parent_id).await? {
                continue;
            }
            return Err(LixError::unknown(format!(
                "cannot publish changelog commit '{}' because parent commit '{}' is not visible or staged for publication",
                commit.header.id, parent_id
            )));
        }
        Ok(())
    }

    async fn validate_publish_membership_closure(
        &mut self,
        commit: &SegmentCommit,
    ) -> Result<(), LixError> {
        let member_change_ids = commit
            .body
            .membership
            .iter()
            .map(|membership| membership.member_change_id.clone())
            .collect::<HashSet<_>>();
        let changes = self.resolve_publish_changes(&member_change_ids).await?;
        let mut source_parent_facts = HashMap::<String, SourceParentFacts>::new();

        for membership in &commit.body.membership {
            let Some(change) = changes.get(&membership.member_change_id) else {
                continue;
            };
            match membership.role {
                MembershipRole::Authored => {
                    if change.authored_commit_id.as_deref() != Some(commit.header.id.as_str()) {
                        return Err(LixError::unknown(format!(
                            "cannot publish changelog commit '{}' because authored membership change '{}' belongs to authored_commit_id {:?}",
                            commit.header.id,
                            membership.member_change_id,
                            change.authored_commit_id
                        )));
                    }
                }
                MembershipRole::Adopted => {
                    if change.authored_commit_id.as_deref() == Some(commit.header.id.as_str()) {
                        return Err(LixError::unknown(format!(
                            "cannot publish changelog commit '{}' because adopted membership change '{}' is authored by the same commit",
                            commit.header.id, membership.member_change_id
                        )));
                    }
                    let Some(source_parent_ordinal) = membership.source_parent_ordinal else {
                        return Err(LixError::unknown(format!(
                            "cannot publish changelog commit '{}' because adopted membership change '{}' is missing source_parent_ordinal",
                            commit.header.id, membership.member_change_id
                        )));
                    };
                    let Some(parent_id) = commit
                        .header
                        .parent_commit_ids
                        .get(source_parent_ordinal as usize)
                    else {
                        return Err(LixError::unknown(format!(
                            "cannot publish changelog commit '{}' because adopted membership change '{}' source_parent_ordinal {} is out of bounds",
                            commit.header.id, membership.member_change_id, source_parent_ordinal
                        )));
                    };
                    if !source_parent_facts.contains_key(parent_id) {
                        let facts = self.source_parent_facts(parent_id).await?;
                        source_parent_facts.insert(parent_id.clone(), facts);
                    }
                    let facts = source_parent_facts
                        .get(parent_id)
                        .expect("source parent facts should be cached");
                    if !facts
                        .reachable_memberships
                        .contains(&membership.member_change_id)
                    {
                        return Err(LixError::unknown(format!(
                            "cannot publish changelog commit '{}' because adopted membership change '{}' is not reachable from source parent '{}'",
                            commit.header.id, membership.member_change_id, parent_id
                        )));
                    }
                    let identity = state_row_identity_for_change(change)?;
                    if facts.first_parent_winners.get(&identity)
                        != Some(&membership.member_change_id)
                    {
                        return Err(LixError::unknown(format!(
                            "cannot publish changelog commit '{}' because adopted membership change '{}' is not the source parent '{}' winner for {:?}",
                            commit.header.id, membership.member_change_id, parent_id, identity
                        )));
                    }
                }
            }
        }

        for (identity, change_id) in &commit.directory.state_row_identities {
            let Some(change) = changes.get(change_id) else {
                return Err(LixError::unknown(format!(
                    "cannot publish changelog commit '{}' because StateRowIdentity winner references non-member change '{}'",
                    commit.header.id, change_id
                )));
            };
            let actual = state_row_identity_for_change(change)?;
            if &actual != identity {
                return Err(LixError::unknown(format!(
                    "cannot publish changelog commit '{}' because StateRowIdentity winner for change '{}' does not match changelog.change (expected {:?}, actual {:?})",
                    commit.header.id, change_id, identity, actual
                )));
            }
        }
        Ok(())
    }

    async fn validate_stage_adopted_membership_provenance(
        &mut self,
        segment: &Segment,
    ) -> Result<(), LixError> {
        let adopted_change_ids = segment
            .commits
            .iter()
            .flat_map(|commit| {
                commit
                    .body
                    .membership
                    .iter()
                    .filter(|membership| membership.role == MembershipRole::Adopted)
                    .map(|membership| membership.member_change_id.clone())
            })
            .collect::<HashSet<_>>();
        if adopted_change_ids.is_empty() {
            return Ok(());
        }

        let mut changes = segment
            .changes
            .iter()
            .filter(|change| adopted_change_ids.contains(&change.id))
            .map(|change| (change.id.clone(), change.clone()))
            .collect::<HashMap<_, _>>();
        let missing = adopted_change_ids
            .difference(&changes.keys().cloned().collect::<HashSet<_>>())
            .cloned()
            .collect::<HashSet<_>>();
        if !missing.is_empty() {
            changes.extend(self.resolve_publish_changes(&missing).await?);
        }

        let mut source_parent_facts = HashMap::<String, SourceParentFacts>::new();

        for commit in &segment.commits {
            for membership in &commit.body.membership {
                if membership.role != MembershipRole::Adopted {
                    continue;
                }
                let source_parent_ordinal =
                    membership.source_parent_ordinal.ok_or_else(|| {
                        LixError::unknown(format!(
                            "cannot stage changelog commit '{}' because adopted membership change '{}' is missing source_parent_ordinal",
                            commit.header.id, membership.member_change_id
                        ))
                    })?;
                let parent_id = commit
                    .header
                    .parent_commit_ids
                    .get(source_parent_ordinal as usize)
                    .ok_or_else(|| {
                        LixError::unknown(format!(
                            "cannot stage changelog commit '{}' because adopted membership change '{}' source_parent_ordinal {} is out of bounds",
                            commit.header.id, membership.member_change_id, source_parent_ordinal
                        ))
                    })?;
                let Some(change) = changes.get(&membership.member_change_id) else {
                    return Err(LixError::unknown(format!(
                        "cannot stage changelog commit '{}' because adopted membership change '{}' has no changelog.change",
                        commit.header.id, membership.member_change_id
                    )));
                };
                if change.authored_commit_id.as_deref() == Some(commit.header.id.as_str()) {
                    return Err(LixError::unknown(format!(
                        "cannot stage changelog commit '{}' because adopted membership change '{}' is authored by the same commit",
                        commit.header.id, membership.member_change_id
                    )));
                }
                if !source_parent_facts.contains_key(parent_id) {
                    let facts = self
                        .source_parent_facts_in_segment(parent_id, segment)
                        .await?;
                    source_parent_facts.insert(parent_id.clone(), facts);
                }
                let facts = source_parent_facts
                    .get(parent_id)
                    .expect("source parent facts should be cached");
                if !facts
                    .reachable_memberships
                    .contains(&membership.member_change_id)
                {
                    return Err(LixError::unknown(format!(
                        "cannot stage changelog commit '{}' because adopted membership change '{}' is not reachable from source parent '{}'",
                        commit.header.id, membership.member_change_id, parent_id
                    )));
                }
                let identity = state_row_identity_for_change(change)?;
                if facts.first_parent_winners.get(&identity) != Some(&membership.member_change_id) {
                    return Err(LixError::unknown(format!(
                        "cannot stage changelog commit '{}' because adopted membership change '{}' is not the source parent '{}' winner for {:?}",
                        commit.header.id, membership.member_change_id, parent_id, identity
                    )));
                }
            }
        }
        Ok(())
    }

    async fn source_parent_facts(
        &mut self,
        root_commit_id: &str,
    ) -> Result<SourceParentFacts, LixError> {
        let mut facts = SourceParentFacts::default();
        let mut stack = vec![root_commit_id.to_string()];
        let mut visited = HashSet::new();
        while let Some(commit_id) = stack.pop() {
            if !visited.insert(commit_id.clone()) {
                continue;
            }
            let Some(commit) = self.load_published_or_staged_commit(&commit_id).await? else {
                continue;
            };
            facts.reachable_memberships.extend(
                commit
                    .body
                    .membership
                    .iter()
                    .map(|membership| membership.member_change_id.clone()),
            );
            stack.extend(commit.header.parent_commit_ids);
        }

        let mut next_commit_id = Some(root_commit_id.to_string());
        let mut visited = HashSet::new();
        while let Some(commit_id) = next_commit_id.take() {
            if !visited.insert(commit_id.clone()) {
                return Err(LixError::unknown(format!(
                    "cannot resolve source parent facts because first-parent history contains parent cycle at commit '{commit_id}'"
                )));
            }
            let Some(commit) = self.load_published_or_staged_commit(&commit_id).await? else {
                break;
            };
            for (identity, change_id) in &commit.directory.state_row_identities {
                facts
                    .first_parent_winners
                    .entry(identity.clone())
                    .or_insert_with(|| change_id.clone());
            }
            next_commit_id = commit.header.parent_commit_ids.first().cloned();
        }
        Ok(facts)
    }

    async fn source_parent_facts_in_segment(
        &mut self,
        root_commit_id: &str,
        segment: &Segment,
    ) -> Result<SourceParentFacts, LixError> {
        let mut facts = SourceParentFacts::default();
        let mut stack = vec![root_commit_id.to_string()];
        let mut visited = HashSet::new();
        while let Some(commit_id) = stack.pop() {
            if !visited.insert(commit_id.clone()) {
                continue;
            }
            let Some(commit) = self
                .load_published_staged_or_segment_commit(&commit_id, Some(segment))
                .await?
            else {
                continue;
            };
            facts.reachable_memberships.extend(
                commit
                    .body
                    .membership
                    .iter()
                    .map(|membership| membership.member_change_id.clone()),
            );
            stack.extend(commit.header.parent_commit_ids);
        }

        let mut next_commit_id = Some(root_commit_id.to_string());
        let mut visited = HashSet::new();
        while let Some(commit_id) = next_commit_id.take() {
            if !visited.insert(commit_id.clone()) {
                return Err(LixError::unknown(format!(
                    "cannot resolve source parent facts because first-parent history contains parent cycle at commit '{commit_id}'"
                )));
            }
            let Some(commit) = self
                .load_published_staged_or_segment_commit(&commit_id, Some(segment))
                .await?
            else {
                break;
            };
            for (identity, change_id) in &commit.directory.state_row_identities {
                facts
                    .first_parent_winners
                    .entry(identity.clone())
                    .or_insert_with(|| change_id.clone());
            }
            next_commit_id = commit.header.parent_commit_ids.first().cloned();
        }
        Ok(facts)
    }

    async fn commit_history_contains_membership(
        &mut self,
        root_commit_id: &str,
        change_id: &str,
    ) -> Result<bool, LixError> {
        self.commit_history_contains_membership_with_loader(root_commit_id, change_id, None)
            .await
    }

    async fn commit_history_contains_membership_in_segment(
        &mut self,
        root_commit_id: &str,
        change_id: &str,
        segment: &Segment,
    ) -> Result<bool, LixError> {
        self.commit_history_contains_membership_with_loader(
            root_commit_id,
            change_id,
            Some(segment),
        )
        .await
    }

    async fn commit_history_contains_membership_with_loader(
        &mut self,
        root_commit_id: &str,
        change_id: &str,
        segment: Option<&Segment>,
    ) -> Result<bool, LixError> {
        let mut stack = vec![root_commit_id.to_string()];
        let mut visited = HashSet::new();
        while let Some(commit_id) = stack.pop() {
            if !visited.insert(commit_id.clone()) {
                continue;
            }
            let Some(commit) = self
                .load_published_staged_or_segment_commit(&commit_id, segment)
                .await?
            else {
                continue;
            };
            if commit
                .body
                .membership
                .iter()
                .any(|membership| membership.member_change_id == change_id)
            {
                return Ok(true);
            }
            stack.extend(commit.header.parent_commit_ids);
        }
        Ok(false)
    }

    async fn commit_history_projects_state_row(
        &mut self,
        root_commit_id: &str,
        identity: &StateRowIdentity,
        change_id: &str,
    ) -> Result<bool, LixError> {
        self.commit_history_projects_state_row_with_loader(
            root_commit_id,
            identity,
            change_id,
            None,
        )
        .await
    }

    async fn commit_history_projects_state_row_in_segment(
        &mut self,
        root_commit_id: &str,
        identity: &StateRowIdentity,
        change_id: &str,
        segment: &Segment,
    ) -> Result<bool, LixError> {
        self.commit_history_projects_state_row_with_loader(
            root_commit_id,
            identity,
            change_id,
            Some(segment),
        )
        .await
    }

    async fn commit_history_projects_state_row_with_loader(
        &mut self,
        root_commit_id: &str,
        identity: &StateRowIdentity,
        change_id: &str,
        segment: Option<&Segment>,
    ) -> Result<bool, LixError> {
        let mut next_commit_id = Some(root_commit_id.to_string());
        let mut visited = HashSet::new();
        while let Some(commit_id) = next_commit_id.take() {
            if !visited.insert(commit_id.clone()) {
                return Err(LixError::unknown(format!(
                    "cannot resolve StateRowIdentity winner for {:?} because first-parent history contains cycle at commit '{}'",
                    identity, commit_id
                )));
            }
            let Some(commit) = self
                .load_published_staged_or_segment_commit(&commit_id, segment)
                .await?
            else {
                return Ok(false);
            };
            if let Some((_, winner_change_id)) = commit
                .directory
                .state_row_identities
                .iter()
                .find(|(candidate, _)| candidate == identity)
            {
                return Ok(winner_change_id == change_id);
            }
            next_commit_id = commit.header.parent_commit_ids.first().cloned();
        }
        Ok(false)
    }

    async fn load_published_staged_or_segment_commit(
        &mut self,
        commit_id: &str,
        segment: Option<&Segment>,
    ) -> Result<Option<SegmentCommit>, LixError> {
        if let Some(commit) = segment.and_then(|segment| {
            segment
                .commits
                .iter()
                .find(|commit| commit.header.id == commit_id)
        }) {
            return Ok(Some(commit.clone()));
        }
        if let Some(commit) = self.load_published_or_staged_commit(commit_id).await? {
            return Ok(Some(commit));
        }
        self.find_segment_commit(commit_id).await
    }

    async fn load_published_or_staged_commit(
        &mut self,
        commit_id: &str,
    ) -> Result<Option<SegmentCommit>, LixError> {
        if let Some(location) = self.staged_commits.get(commit_id).cloned() {
            return self
                .load_publish_commit(commit_id, &location)
                .await
                .map(Some);
        }
        let Some(visibility) = get_one(
            &mut *self.store,
            COMMIT_VISIBILITY_SPACE,
            commit_visibility_key(commit_id),
        )
        .await?
        .map(|bytes| decode_commit_visibility(&bytes))
        .transpose()?
        else {
            return Ok(None);
        };
        self.load_publish_commit(commit_id, &visibility.location)
            .await
            .map(Some)
    }

    async fn find_segment_commit(
        &mut self,
        commit_id: &str,
    ) -> Result<Option<SegmentCommit>, LixError> {
        let Some(entry) = get_one(
            &mut *self.store,
            BY_COMMIT_INDEX_SPACE,
            by_commit_key(commit_id),
        )
        .await?
        .map(|bytes| decode_by_commit_entry(&bytes))
        .transpose()?
        else {
            return self.scan_segments_for_commit(commit_id).await;
        };
        if entry.commit_id != commit_id {
            return Err(LixError::unknown(format!(
                "by_commit key for '{commit_id}' contains commit_id '{}'",
                entry.commit_id
            )));
        }
        let Some(segment) = self
            .load_segment_byte_index_for_writer(&entry.location.segment_id)
            .await?
        else {
            return self.scan_segments_for_commit(commit_id).await;
        };
        match segment.load_commit(&entry.location, commit_id) {
            Ok(commit) => {
                if validate_commit_checksum(&entry.location.checksum, commit_id, &commit).is_ok() {
                    Ok(Some(commit))
                } else {
                    self.scan_segments_for_commit(commit_id).await
                }
            }
            Err(_) => self.scan_segments_for_commit(commit_id).await,
        }
    }

    async fn load_segment_byte_index_for_writer(
        &mut self,
        segment_id: &str,
    ) -> Result<Option<SegmentByteIndex>, LixError> {
        let Some(bytes) = get_one(&mut *self.store, SEGMENT_SPACE, segment_key(segment_id)).await?
        else {
            return Ok(None);
        };
        Ok(Some(SegmentByteIndex::decode(bytes)?))
    }

    async fn scan_segments_for_commit(
        &mut self,
        commit_id: &str,
    ) -> Result<Option<SegmentCommit>, LixError> {
        for segment in self.scan_all_segments().await? {
            let Some(commit) = segment_commit(&segment, commit_id) else {
                continue;
            };
            let location = directory_commit_location(&segment, commit_id)?;
            validate_commit_checksum(&location.checksum, commit_id, commit)?;
            return Ok(Some(commit.clone()));
        }
        Ok(None)
    }

    async fn resolve_publish_changes(
        &mut self,
        change_ids: &HashSet<String>,
    ) -> Result<HashMap<String, SegmentChange>, LixError> {
        let mut found = HashMap::new();
        for segment in self.staged_segments.values() {
            for change in &segment.changes {
                if change_ids.contains(&change.id) {
                    if found.insert(change.id.clone(), change.clone()).is_some() {
                        return Err(LixError::unknown(format!(
                            "cannot publish changelog change '{}' because it appears in multiple staged/stored segments",
                            change.id
                        )));
                    }
                }
            }
        }

        let mut remaining = change_ids
            .difference(&found.keys().cloned().collect::<HashSet<_>>())
            .cloned()
            .collect::<Vec<_>>();
        if !remaining.is_empty() {
            self.resolve_publish_changes_from_by_change(&remaining, &mut found)
                .await?;
            remaining.retain(|change_id| !found.contains_key(change_id));
        }
        if !remaining.is_empty() {
            self.resolve_publish_changes_by_segment_scan(&remaining, &mut found)
                .await?;
        }

        for change_id in change_ids {
            if !found.contains_key(change_id) {
                return Err(LixError::unknown(format!(
                    "cannot publish changelog change '{change_id}' because no changelog.change exists"
                )));
            }
        }
        Ok(found)
    }

    async fn resolve_publish_changes_from_by_change(
        &mut self,
        change_ids: &[String],
        found: &mut HashMap<String, SegmentChange>,
    ) -> Result<(), LixError> {
        let values = get_many(
            &mut *self.store,
            BY_CHANGE_INDEX_SPACE,
            change_ids
                .iter()
                .map(|change_id| by_change_key(change_id))
                .collect(),
        )
        .await?;
        let mut entries = Vec::new();
        let mut segment_ids = Vec::new();
        for (change_id, value) in change_ids.iter().zip(values.into_iter()) {
            let Some(bytes) = value else {
                continue;
            };
            let entry = decode_by_change_entry(&bytes)?;
            if entry.change_id != *change_id {
                return Err(LixError::unknown(format!(
                    "by_change key for '{change_id}' contains change_id '{}'",
                    entry.change_id
                )));
            }
            push_unique(&mut segment_ids, entry.location.segment_id.clone());
            entries.push(entry);
        }

        let segment_values = get_many(
            &mut *self.store,
            SEGMENT_SPACE,
            segment_ids
                .iter()
                .map(|segment_id| segment_key(segment_id))
                .collect(),
        )
        .await?;
        let mut segments = HashMap::new();
        for (segment_id, value) in segment_ids.iter().zip(segment_values.into_iter()) {
            if let Some(bytes) = value {
                segments.insert(segment_id.clone(), SegmentByteIndex::decode(bytes)?);
            }
        }

        for entry in entries {
            let Some(segment) = segments.get(&entry.location.segment_id) else {
                continue;
            };
            let change = segment.load_change(&entry.location, &entry.change_id)?;
            validate_change_checksum(&entry.location.checksum, &entry.change_id, &change)?;
            if found.insert(entry.change_id.clone(), change).is_some() {
                return Err(LixError::unknown(format!(
                    "cannot publish changelog change '{}' because it appears in multiple staged/stored segments",
                    entry.change_id
                )));
            }
        }
        Ok(())
    }

    async fn resolve_publish_changes_by_segment_scan(
        &mut self,
        change_ids: &[String],
        found: &mut HashMap<String, SegmentChange>,
    ) -> Result<(), LixError> {
        let unresolved = change_ids.iter().cloned().collect::<HashSet<_>>();
        for segment in self.scan_all_segments().await? {
            for change in &segment.changes {
                if unresolved.contains(&change.id) {
                    if found.insert(change.id.clone(), change.clone()).is_some() {
                        return Err(LixError::unknown(format!(
                            "cannot publish changelog change '{}' because it appears in multiple staged/stored segments",
                            change.id
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    async fn resolve_publish_change(&mut self, change_id: &str) -> Result<SegmentChange, LixError> {
        let mut found = None;
        for segment in self.staged_segments.values() {
            if let Some(change) = segment_change(segment, change_id) {
                if found.is_some() {
                    return Err(LixError::unknown(format!(
                        "cannot publish changelog change '{change_id}' because it appears in multiple staged/stored segments"
                    )));
                }
                found = Some(change.clone());
            }
        }
        for segment in self.scan_all_segments().await? {
            if let Some(change) = segment_change(&segment, change_id) {
                if found.is_some() {
                    return Err(LixError::unknown(format!(
                        "cannot publish changelog change '{change_id}' because it appears in multiple staged/stored segments"
                    )));
                }
                found = Some(change.clone());
            }
        }
        found.ok_or_else(|| {
            LixError::unknown(format!(
                "cannot publish changelog change '{change_id}' because no changelog.change exists"
            ))
        })
    }

    async fn stored_commit_visibility_is_valid(
        &mut self,
        commit_id: &str,
    ) -> Result<bool, LixError> {
        let Some(bytes) = get_one(
            &mut *self.store,
            COMMIT_VISIBILITY_SPACE,
            commit_visibility_key(commit_id),
        )
        .await?
        else {
            return Ok(false);
        };
        let visibility = decode_commit_visibility(&bytes)?;
        if visibility.commit_id != commit_id {
            return Err(LixError::unknown(format!(
                "commit_visibility key for '{commit_id}' contains commit_id '{}'",
                visibility.commit_id
            )));
        }
        let commit = self
            .load_publish_commit(commit_id, &visibility.location)
            .await?;
        validate_commit_checksum(&visibility.checksum, commit_id, &commit)?;
        Ok(true)
    }

    pub(crate) async fn collect_garbage(&mut self, roots: &[GcRoot]) -> Result<GcPlan, LixError> {
        super::gc::collect_garbage(&mut *self.store, self.writes, roots).await
    }

    pub(crate) async fn stage_gc_sweep(&mut self, plan: &GcPlan) -> Result<(), LixError> {
        super::gc::stage_gc_sweep(self.writes, plan)
    }

    pub(crate) async fn rebuild_mandatory_indexes(
        &mut self,
    ) -> Result<RebuildIndexStats, LixError> {
        let segments = self.scan_all_segments().await?;
        let stats = self
            .stage_by_commit_index_rebuild(&segments)
            .await?
            .combine(self.stage_by_change_index_rebuild(&segments).await?)
            .combine(
                self.stage_by_change_membership_index_rebuild(&segments)
                    .await?,
            )
            .combine(self.stage_visible_change_proof_rebuild(&segments).await?);
        Ok(stats)
    }

    pub(crate) async fn rebuild_by_commit_index(&mut self) -> Result<RebuildIndexStats, LixError> {
        let segments = self.scan_all_segments().await?;
        self.stage_by_commit_index_rebuild(&segments).await
    }

    pub(crate) async fn rebuild_by_change_index(&mut self) -> Result<RebuildIndexStats, LixError> {
        let segments = self.scan_all_segments().await?;
        self.stage_by_change_index_rebuild(&segments).await
    }

    pub(crate) async fn rebuild_by_change_membership_index(
        &mut self,
    ) -> Result<RebuildIndexStats, LixError> {
        let segments = self.scan_all_segments().await?;
        self.stage_by_change_membership_index_rebuild(&segments)
            .await
    }

    async fn scan_all_segments(&mut self) -> Result<Vec<Segment>, LixError> {
        let mut after = None;
        let mut segments = Vec::new();
        loop {
            let page = self
                .store
                .changelog_scan(
                    SEGMENT_SPACE,
                    Vec::new(),
                    after,
                    64,
                    StorageCoreProjection::FullValue,
                )
                .await?;
            for index in 0..page.len() {
                let Some(bytes) = page.value(index) else {
                    continue;
                };
                let segment = decode_segment(bytes)?;
                validate_segment_shape(&segment)?;
                segments.push(segment);
            }
            let Some(next_after) = page.resume_after else {
                break;
            };
            after = Some(next_after);
        }
        Ok(segments)
    }

    async fn stage_by_commit_index_rebuild(
        &mut self,
        segments: &[Segment],
    ) -> Result<RebuildIndexStats, LixError> {
        let entries = by_commit_entries_for_segments(segments)?;
        let mut expected_rows = HashMap::new();
        for entry in &entries {
            expected_rows.insert(
                by_commit_key(&entry.commit_id),
                by_commit_index_value(entry)?,
            );
        }
        let stats = self
            .stage_index_rebuild(BY_COMMIT_INDEX_SPACE, &expected_rows)
            .await?;
        for entry in entries {
            self.staged_commits
                .insert(entry.commit_id.clone(), entry.location.clone());
            self.staged_generations
                .insert(entry.commit_id.clone(), entry.generation);
        }
        Ok(stats)
    }

    async fn stage_by_change_index_rebuild(
        &mut self,
        segments: &[Segment],
    ) -> Result<RebuildIndexStats, LixError> {
        let entries = by_change_entries_for_segments(segments)?;
        let mut expected_rows = HashMap::new();
        for entry in &entries {
            expected_rows.insert(
                by_change_key(&entry.change_id),
                by_change_index_value(entry)?,
            );
        }
        self.stage_index_rebuild(BY_CHANGE_INDEX_SPACE, &expected_rows)
            .await
    }

    async fn stage_by_change_membership_index_rebuild(
        &mut self,
        segments: &[Segment],
    ) -> Result<RebuildIndexStats, LixError> {
        let entries = by_change_membership_entries_for_segments(segments);
        let mut expected_rows = HashMap::new();
        for entry in &entries {
            expected_rows.insert(
                by_change_membership_key(&entry.change_id, &entry.commit_id),
                by_change_membership_index_value(),
            );
        }
        self.stage_index_rebuild(BY_CHANGE_MEMBERSHIP_INDEX_SPACE, &expected_rows)
            .await
    }

    async fn stage_visible_change_proof_rebuild(
        &mut self,
        segments: &[Segment],
    ) -> Result<RebuildIndexStats, LixError> {
        let mut segments_by_id = HashMap::new();
        for segment in segments {
            segments_by_id.insert(segment.header.segment_id.as_str(), segment);
        }

        let mut expected_rows = HashMap::new();
        for visibility in self.scan_commit_visibilities().await? {
            let Some(segment) = segments_by_id.get(visibility.location.segment_id.as_str()) else {
                continue;
            };
            let Some(commit) = segment_commit(segment, &visibility.commit_id) else {
                continue;
            };
            validate_commit_location(&visibility.location, segment, &visibility.commit_id)?;
            validate_commit_checksum(&visibility.checksum, &visibility.commit_id, commit)?;
            for membership in &commit.body.membership {
                expected_rows.insert(
                    visible_change_proof_key(&membership.member_change_id),
                    visible_change_proof_value(&visibility)?,
                );
            }
        }
        self.stage_index_rebuild(VISIBLE_CHANGE_PROOF_SPACE, &expected_rows)
            .await
    }

    pub(crate) async fn scan_commit_visibilities(
        &mut self,
    ) -> Result<Vec<CommitVisibility>, LixError> {
        scan_commit_visibilities_from_store(self.store).await
    }

    async fn stage_index_rebuild(
        &mut self,
        space: StorageSpace,
        expected_rows: &HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<RebuildIndexStats, LixError> {
        let mut after = None;
        let mut seen = HashSet::new();
        let mut deleted = 0;
        let mut unchanged = 0;
        let mut put = 0;
        loop {
            let page = self
                .store
                .changelog_scan(
                    space,
                    Vec::new(),
                    after,
                    256,
                    StorageCoreProjection::FullValue,
                )
                .await?;
            for index in 0..page.len() {
                let Some(key) = page.key(index) else {
                    continue;
                };
                let Some(value) = page.value(index) else {
                    return Err(LixError::unknown(format!(
                        "changelog index space '{}' returned a key without a value",
                        space.name
                    )));
                };
                if let Some(expected_value) = expected_rows.get(key) {
                    seen.insert(key.to_vec());
                    if expected_value.as_slice() == value {
                        unchanged += 1;
                    } else {
                        self.writes.put(space, key.to_vec(), expected_value.clone());
                        put += 1;
                    }
                } else {
                    self.writes.delete(space, key.to_vec());
                    deleted += 1;
                }
            }
            let Some(next_after) = page.resume_after else {
                break;
            };
            after = Some(next_after);
        }
        for (key, value) in expected_rows {
            if !seen.contains(key) {
                self.writes.put(space, key.clone(), value.clone());
                put += 1;
            }
        }
        Ok(RebuildIndexStats {
            expected: expected_rows.len(),
            put,
            deleted,
            unchanged,
        })
    }
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn project_segment_commit(commit: &SegmentCommit, projection: CommitProjection) -> CommitLoadEntry {
    match projection {
        CommitProjection::Header => CommitLoadEntry::Header(commit.header.clone()),
        CommitProjection::Body => CommitLoadEntry::Body(commit.body.clone()),
        CommitProjection::Full => CommitLoadEntry::Full {
            header: commit.header.clone(),
            body: commit.body.clone(),
        },
    }
}

fn project_segment_change(change: &SegmentChange, projection: ChangeProjection) -> ChangeLoadEntry {
    match projection {
        ChangeProjection::Logical => ChangeLoadEntry::Logical(Change {
            id: change.id.clone(),
            authored_commit_id: change.authored_commit_id.clone(),
            entity_id: change.entity_id.clone(),
            schema_key: change.schema_key.clone(),
            file_id: change.file_id.clone(),
            snapshot_ref: change.snapshot_ref,
            metadata_ref: change.metadata_ref,
            created_at: change.created_at.clone(),
        }),
        ChangeProjection::Segment => ChangeLoadEntry::Segment(change.clone()),
        ChangeProjection::PhysicalLocation => {
            unreachable!("physical location projection is handled before segment hydration")
        }
    }
}

fn project_change_with_location(
    location: SegmentObjectLocation,
    change: &SegmentChange,
    projection: ChangeProjection,
) -> ChangeLoadEntry {
    match projection {
        ChangeProjection::PhysicalLocation => ChangeLoadEntry::PhysicalLocation(location),
        ChangeProjection::Logical | ChangeProjection::Segment => {
            project_segment_change(change, projection)
        }
    }
}

fn state_row_identity_for_change(change: &SegmentChange) -> Result<StateRowIdentity, LixError> {
    let file_id = change.file_id.as_deref().unwrap_or("__global__");
    Ok(StateRowIdentity {
        schema_key: CanonicalSchemaKey::new(change.schema_key.clone())?,
        file_id: FileId::new(file_id.to_string())?,
        entity_id: EntityId::new(change.entity_id.as_json_array_text()?)?,
    })
}

async fn scan_commit_visibilities_from_store(
    store: &mut (impl ChangelogStorageRead + ?Sized),
) -> Result<Vec<CommitVisibility>, LixError> {
    let mut after = None;
    let mut visibilities = Vec::new();
    loop {
        let page = store
            .changelog_scan(
                COMMIT_VISIBILITY_SPACE,
                Vec::new(),
                after,
                256,
                StorageCoreProjection::FullValue,
            )
            .await?;
        for index in 0..page.len() {
            let Some(key) = page.key(index) else {
                continue;
            };
            let commit_id = std::str::from_utf8(key).map_err(|error| {
                LixError::unknown(format!(
                    "changelog commit_visibility key contains invalid UTF-8: {error}"
                ))
            })?;
            let Some(value) = page.value(index) else {
                return Err(LixError::unknown(
                    "changelog commit_visibility scan returned key without value".to_string(),
                ));
            };
            let visibility = decode_commit_visibility(value)?;
            if visibility.commit_id != commit_id {
                return Err(LixError::unknown(format!(
                    "commit_visibility key for '{commit_id}' contains commit_id '{}'",
                    visibility.commit_id
                )));
            }
            visibilities.push(visibility);
        }
        let Some(next_after) = page.resume_after else {
            break;
        };
        after = Some(next_after);
    }
    Ok(visibilities)
}

fn changelog_not_implemented(operation: &str) -> LixError {
    LixError::unknown(format!(
        "changelog operation '{operation}' is not implemented yet"
    ))
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
    use crate::changelog::test_support::*;
    use crate::changelog::{
        decode_by_change_entry, decode_by_commit_entry, decode_commit_visibility, decode_segment,
        CommitBody, CommitHeader, MembershipRecord, MembershipRole, SegmentCommit,
        SegmentCommitDirectory,
    };
    use crate::entity_identity::EntityIdentity;
    use crate::storage::StorageWriteSet;

    use super::*;

    #[tokio::test]
    async fn stage_segment_stages_segment_and_rebuildable_indexes() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();
        let expected_visibility = commit_visibility_from_segment(&segment, "commit-1");

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
        }
        let stats = writes.apply(&mut *transaction).await.unwrap();
        assert_eq!(stats.staged_puts, 6);
        transaction.commit().await.unwrap();

        let result = read_test_value_groups(
            &storage,
            vec![
                (SEGMENT_SPACE, vec![segment_key("segment-1")]),
                (BY_COMMIT_INDEX_SPACE, vec![by_commit_key("commit-1")]),
                (BY_CHANGE_INDEX_SPACE, vec![by_change_key("change-1")]),
                (
                    BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
                    vec![by_change_membership_key("change-1", "commit-1")],
                ),
                (
                    COMMIT_VISIBILITY_SPACE,
                    vec![commit_visibility_key("commit-1")],
                ),
                (
                    VISIBLE_CHANGE_PROOF_SPACE,
                    vec![visible_change_proof_key("change-1")],
                ),
            ],
        );

        let segment_bytes = result[0][0].as_deref().unwrap();
        assert_eq!(decode_segment(segment_bytes).unwrap(), segment);

        let by_commit_bytes = result[1][0].as_deref().unwrap();
        let by_commit = decode_by_commit_entry(by_commit_bytes).unwrap();
        assert_eq!(by_commit.commit_id, "commit-1");
        assert_eq!(by_commit.location.segment_id, "segment-1");

        let by_change_bytes = result[2][0].as_deref().unwrap();
        let by_change = decode_by_change_entry(by_change_bytes).unwrap();
        assert_eq!(by_change.change_id, "change-1");
        assert_eq!(by_change.location.segment_id, "segment-1");

        assert_eq!(result[3][0].as_deref(), Some([].as_slice()));

        let visibility_bytes = result[4][0].as_deref().unwrap();
        assert_eq!(
            decode_commit_visibility(visibility_bytes).unwrap(),
            expected_visibility
        );
        let visible_proof_bytes = result[5][0].as_deref().unwrap();
        assert_eq!(
            decode_commit_visibility(visible_proof_bytes).unwrap(),
            expected_visibility
        );
    }

    #[tokio::test]
    async fn physical_locator_helpers_load_commits_changes_visibility_and_segments() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();
        let expected_visibility = commit_visibility_from_segment(&segment, "commit-1");

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage.clone());
        assert_eq!(
            reader.load_commit_visibility("commit-1").await.unwrap(),
            Some(expected_visibility)
        );
        assert_eq!(
            reader
                .load_by_commit("commit-1")
                .await
                .unwrap()
                .unwrap()
                .commit_id,
            "commit-1"
        );
        assert_eq!(
            reader
                .load_by_change("change-1")
                .await
                .unwrap()
                .unwrap()
                .change_id,
            "change-1"
        );
        assert_eq!(
            reader.load_segment("segment-1").await.unwrap(),
            Some(segment)
        );
        assert_eq!(
            reader.load_commit_visibility("missing").await.unwrap(),
            None
        );
        assert_eq!(reader.load_by_commit("missing").await.unwrap(), None);
        assert_eq!(reader.load_by_change("missing").await.unwrap(), None);
        assert_eq!(reader.load_segment("missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn load_commits_require_visible_returns_none_until_commit_is_published() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage.clone());
        let batch = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &["commit-1".to_string()],
                projection: CommitProjection::Full,
                visibility: CommitVisibilityMode::RequireVisible,
            })
            .await
            .unwrap();
        assert_eq!(batch.entries, vec![None]);

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_publish_commit("commit-1").await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage.clone());
        let batch = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &["commit-1".to_string()],
                projection: CommitProjection::Full,
                visibility: CommitVisibilityMode::RequireVisible,
            })
            .await
            .unwrap();
        assert_eq!(
            batch.entries,
            vec![Some(CommitLoadEntry::Full {
                header: segment.commits[0].header.clone(),
                body: segment.commits[0].body.clone(),
            })]
        );
    }

    #[tokio::test]
    async fn load_commits_require_visible_honors_projection() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        for (projection, expected) in [
            (
                CommitProjection::Header,
                CommitLoadEntry::Header(segment.commits[0].header.clone()),
            ),
            (
                CommitProjection::Body,
                CommitLoadEntry::Body(segment.commits[0].body.clone()),
            ),
            (
                CommitProjection::Full,
                CommitLoadEntry::Full {
                    header: segment.commits[0].header.clone(),
                    body: segment.commits[0].body.clone(),
                },
            ),
        ] {
            let mut reader = context.reader(storage.clone());
            let batch = reader
                .load_commits(CommitLoadRequest {
                    commit_ids: &["commit-1".to_string()],
                    projection,
                    visibility: CommitVisibilityMode::RequireVisible,
                })
                .await
                .unwrap();
            assert_eq!(batch.entries, vec![Some(expected)]);
        }
    }

    #[tokio::test]
    async fn load_commits_physical_only_loads_unpublished_segment() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage.clone());
        let batch = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &["commit-1".to_string()],
                projection: CommitProjection::Full,
                visibility: CommitVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();

        assert_eq!(
            batch.entries,
            vec![Some(CommitLoadEntry::Full {
                header: segment.commits[0].header.clone(),
                body: segment.commits[0].body.clone(),
            })]
        );
    }

    #[tokio::test]
    async fn load_commits_physical_only_returns_none_without_by_commit() {
        let (context, storage) = changelog_test_context();

        let mut reader = context.reader(storage);
        let batch = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &["missing".to_string()],
                projection: CommitProjection::Header,
                visibility: CommitVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();

        assert_eq!(batch.entries, vec![None]);
    }

    #[tokio::test]
    async fn load_commits_physical_only_returns_none_when_locator_index_is_missing_for_existing_commit(
    ) {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.delete(BY_COMMIT_INDEX_SPACE, by_commit_key("commit-1"));
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage);
        let batch = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &["commit-1".to_string()],
                projection: CommitProjection::Header,
                visibility: CommitVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();

        assert_eq!(batch.entries, vec![None]);
    }

    #[tokio::test]
    async fn load_commits_physical_only_errors_when_by_commit_value_is_corrupt() {
        let (context, storage) = changelog_test_context();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            BY_COMMIT_INDEX_SPACE,
            by_commit_key("commit-1"),
            b"not a by_commit entry".to_vec(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage);
        let error = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &["commit-1".to_string()],
                projection: CommitProjection::Header,
                visibility: CommitVisibilityMode::PhysicalOnly,
            })
            .await
            .expect_err("corrupt by_commit locator value should error");

        assert!(
            error
                .to_string()
                .contains("failed to decode changelog by_commit entry"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn load_commits_physical_only_errors_when_by_commit_points_to_missing_segment() {
        let (context, storage) = changelog_test_context();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            BY_COMMIT_INDEX_SPACE,
            by_commit_key("commit-1"),
            by_commit_index_value(&ByCommitEntry {
                commit_id: "commit-1".to_string(),
                location: location("missing-segment", 0, 0, "missing"),
                parent_commit_ids: Vec::new(),
                generation: 0,
            })
            .unwrap(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage);
        let error = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &["commit-1".to_string()],
                projection: CommitProjection::Header,
                visibility: CommitVisibilityMode::PhysicalOnly,
            })
            .await
            .expect_err("missing physical segment should error");

        assert!(
            error
                .to_string()
                .contains("points to missing segment 'missing-segment'"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn load_changes_physical_only_loads_unpublished_segment() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage);
        let batch = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();

        assert_eq!(
            batch.entries,
            vec![Some(ChangeLoadEntry::Segment(segment.changes[0].clone()))]
        );
    }

    #[tokio::test]
    async fn load_changes_physical_only_returns_none_without_by_change() {
        let (context, storage) = changelog_test_context();

        let mut reader = context.reader(storage);
        let batch = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["missing".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();

        assert_eq!(batch.entries, vec![None]);
    }

    #[tokio::test]
    async fn load_changes_physical_only_returns_none_when_locator_index_is_missing_for_existing_change(
    ) {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.delete(BY_CHANGE_INDEX_SPACE, by_change_key("change-1"));
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage);
        let batch = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();

        assert_eq!(batch.entries, vec![None]);
    }

    #[tokio::test]
    async fn load_changes_physical_only_errors_when_by_change_value_is_corrupt() {
        let (context, storage) = changelog_test_context();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            BY_CHANGE_INDEX_SPACE,
            by_change_key("change-1"),
            b"not a by_change entry".to_vec(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage);
        let error = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::PhysicalOnly,
            })
            .await
            .expect_err("corrupt by_change locator value should error");

        assert!(
            error
                .to_string()
                .contains("failed to decode changelog by_change entry"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn load_changes_physical_only_errors_when_by_change_points_to_missing_segment() {
        let (context, storage) = changelog_test_context();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            BY_CHANGE_INDEX_SPACE,
            by_change_key("change-1"),
            by_change_index_value(&ByChangeEntry {
                change_id: "change-1".to_string(),
                location: location("missing-segment", 0, 0, "missing"),
            })
            .unwrap(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage);
        let error = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::PhysicalOnly,
            })
            .await
            .expect_err("missing physical segment should error");

        assert!(
            error
                .to_string()
                .contains("points to missing segment 'missing-segment'"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn load_changes_physical_only_honors_projection() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        for (projection, expected) in [
            (
                ChangeProjection::Logical,
                ChangeLoadEntry::Logical(Change {
                    id: segment.changes[0].id.clone(),
                    authored_commit_id: segment.changes[0].authored_commit_id.clone(),
                    entity_id: segment.changes[0].entity_id.clone(),
                    schema_key: segment.changes[0].schema_key.clone(),
                    file_id: segment.changes[0].file_id.clone(),
                    snapshot_ref: segment.changes[0].snapshot_ref,
                    metadata_ref: segment.changes[0].metadata_ref,
                    created_at: segment.changes[0].created_at.clone(),
                }),
            ),
            (
                ChangeProjection::Segment,
                ChangeLoadEntry::Segment(segment.changes[0].clone()),
            ),
            (
                ChangeProjection::PhysicalLocation,
                ChangeLoadEntry::PhysicalLocation(
                    directory_change_location(&segment, "change-1").unwrap(),
                ),
            ),
        ] {
            let mut reader = context.reader(storage.clone());
            let batch = reader
                .load_changes(ChangeLoadRequest {
                    change_ids: &["change-1".to_string()],
                    projection,
                    visibility: ChangeVisibilityMode::PhysicalOnly,
                })
                .await
                .unwrap();
            assert_eq!(batch.entries, vec![Some(expected)]);
        }
    }

    #[tokio::test]
    async fn load_changes_visible_returns_none_until_commit_is_published_while_physical_succeeds() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage.clone());
        let visible = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await
            .unwrap();
        assert_eq!(visible.entries, vec![None]);

        let mut reader = context.reader(storage);
        let physical = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();
        assert_eq!(
            physical.entries,
            vec![Some(ChangeLoadEntry::Segment(segment.changes[0].clone()))]
        );
    }

    #[tokio::test]
    async fn load_commits_visible_ignores_missing_by_commit_locator_index() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.delete(BY_COMMIT_INDEX_SPACE, by_commit_key("commit-1"));
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage);
        let batch = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &["commit-1".to_string()],
                projection: CommitProjection::Header,
                visibility: CommitVisibilityMode::RequireVisible,
            })
            .await
            .unwrap();

        assert_eq!(
            batch.entries,
            vec![Some(CommitLoadEntry::Header(
                segment.commits[0].header.clone()
            ))]
        );
    }

    #[tokio::test]
    async fn load_changes_visible_succeeds_when_published_commit_membership_contains_change() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage.clone());
        let batch = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Logical,
                visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await
            .unwrap();

        assert_eq!(
            batch.entries,
            vec![Some(ChangeLoadEntry::Logical(Change {
                id: "change-1".to_string(),
                authored_commit_id: Some("commit-1".to_string()),
                entity_id: EntityIdentity::single("entity-1"),
                schema_key: "message".to_string(),
                file_id: Some("file-1".to_string()),
                snapshot_ref: None,
                metadata_ref: None,
                created_at: "2026-05-12T00:00:00Z".to_string(),
            }))]
        );

        let mut reader = context.reader(storage);
        let batch = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::PhysicalLocation,
                visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await
            .unwrap();

        assert_eq!(
            batch.entries,
            vec![Some(ChangeLoadEntry::PhysicalLocation(
                directory_change_location(&segment, "change-1").unwrap()
            ))]
        );
    }

    #[tokio::test]
    async fn load_changes_visible_falls_back_when_locator_indexes_are_stale() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.delete(BY_CHANGE_INDEX_SPACE, by_change_key("change-1"));
        writes.delete(
            BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
            by_change_membership_key("change-1", "commit-1"),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage);
        let batch = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await
            .unwrap();

        assert_eq!(
            batch.entries,
            vec![Some(ChangeLoadEntry::Segment(segment.changes[0].clone()))]
        );
    }

    #[tokio::test]
    async fn load_changes_visible_errors_when_by_change_value_is_corrupt() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            BY_CHANGE_INDEX_SPACE,
            by_change_key("change-1"),
            b"not a by_change entry".to_vec(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage);
        let error = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await
            .expect_err("visible read should error on corrupt locator value");

        assert!(
            error
                .to_string()
                .contains("failed to decode changelog by_change entry"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn rebuild_mandatory_indexes_repairs_deleted_locator_indexes() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.delete(BY_COMMIT_INDEX_SPACE, by_commit_key("commit-1"));
        writes.delete(BY_CHANGE_INDEX_SPACE, by_change_key("change-1"));
        writes.delete(
            BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
            by_change_membership_key("change-1", "commit-1"),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let stats = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.rebuild_mandatory_indexes().await.unwrap()
        };
        assert_eq!(
            stats,
            RebuildIndexStats {
                expected: 4,
                put: 3,
                deleted: 0,
                unchanged: 1
            }
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage.clone());
        let commits = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &["commit-1".to_string()],
                projection: CommitProjection::Header,
                visibility: CommitVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();
        assert!(matches!(
            commits.entries.as_slice(),
            [Some(CommitLoadEntry::Header(_))]
        ));

        let mut reader = context.reader(storage.clone());
        let changes = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await
            .unwrap();
        assert_eq!(
            changes.entries,
            vec![Some(ChangeLoadEntry::Segment(segment.changes[0].clone()))]
        );
    }

    #[tokio::test]
    async fn rebuild_indexes_physical_facts_without_publishing_visibility() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.delete(BY_COMMIT_INDEX_SPACE, by_commit_key("commit-1"));
        writes.delete(BY_CHANGE_INDEX_SPACE, by_change_key("change-1"));
        writes.delete(
            BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
            by_change_membership_key("change-1", "commit-1"),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let stats = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.rebuild_mandatory_indexes().await.unwrap()
        };
        assert_eq!(
            stats,
            RebuildIndexStats {
                expected: 3,
                put: 3,
                deleted: 0,
                unchanged: 0
            }
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage.clone());
        let physical = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();
        assert_eq!(
            physical.entries,
            vec![Some(ChangeLoadEntry::Segment(segment.changes[0].clone()))]
        );

        let mut reader = context.reader(storage);
        let visible = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await
            .unwrap();
        assert_eq!(visible.entries, vec![None]);
    }

    #[tokio::test]
    async fn rebuild_mandatory_indexes_deletes_stale_index_rows() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment).await.unwrap();
        }
        stage_stale_mandatory_index_rows(&mut writes);
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let stats = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.rebuild_mandatory_indexes().await.unwrap()
        };
        assert_eq!(
            stats,
            RebuildIndexStats {
                expected: 3,
                put: 0,
                deleted: 3,
                unchanged: 3
            }
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert_stale_mandatory_index_rows_deleted(&storage).await;
    }

    #[tokio::test]
    async fn individual_rebuild_apis_delete_stale_index_rows() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment).await.unwrap();
        }
        stage_stale_mandatory_index_rows(&mut writes);
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let (by_commit, by_change, by_change_membership) = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            (
                writer.rebuild_by_commit_index().await.unwrap(),
                writer.rebuild_by_change_index().await.unwrap(),
                writer.rebuild_by_change_membership_index().await.unwrap(),
            )
        };
        assert_eq!(
            by_commit,
            RebuildIndexStats {
                expected: 1,
                put: 0,
                deleted: 1,
                unchanged: 1
            }
        );
        assert_eq!(
            by_change,
            RebuildIndexStats {
                expected: 1,
                put: 0,
                deleted: 1,
                unchanged: 1
            }
        );
        assert_eq!(
            by_change_membership,
            RebuildIndexStats {
                expected: 1,
                put: 0,
                deleted: 1,
                unchanged: 1
            }
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert_stale_mandatory_index_rows_deleted(&storage).await;
    }

    #[tokio::test]
    async fn rebuild_mandatory_indexes_overwrites_corrupt_index_values() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            BY_COMMIT_INDEX_SPACE,
            by_commit_key("commit-1"),
            b"not a by_commit value".to_vec(),
        );
        writes.put(
            BY_CHANGE_INDEX_SPACE,
            by_change_key("change-1"),
            b"not a by_change value".to_vec(),
        );
        writes.put(
            BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
            by_change_membership_key("change-1", "commit-1"),
            b"not empty".to_vec(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let stats = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.rebuild_mandatory_indexes().await.unwrap()
        };
        assert_eq!(
            stats,
            RebuildIndexStats {
                expected: 3,
                put: 3,
                deleted: 0,
                unchanged: 0
            }
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert_mandatory_index_rows_match_segment(&storage, &segment).await;
    }

    #[tokio::test]
    async fn rebuild_mandatory_indexes_overwrites_wrong_locator_values() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            BY_COMMIT_INDEX_SPACE,
            by_commit_key("commit-1"),
            by_commit_index_value(&ByCommitEntry {
                commit_id: "commit-1".to_string(),
                location: location("wrong-segment", 9, 9, "wrong-checksum"),
                parent_commit_ids: Vec::new(),
                generation: 99,
            })
            .unwrap(),
        );
        writes.put(
            BY_CHANGE_INDEX_SPACE,
            by_change_key("change-1"),
            by_change_index_value(&ByChangeEntry {
                change_id: "change-1".to_string(),
                location: location("wrong-segment", 9, 9, "wrong-checksum"),
            })
            .unwrap(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let stats = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.rebuild_mandatory_indexes().await.unwrap()
        };
        assert_eq!(
            stats,
            RebuildIndexStats {
                expected: 3,
                put: 2,
                deleted: 0,
                unchanged: 1
            }
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert_mandatory_index_rows_match_segment(&storage, &segment).await;
    }

    #[tokio::test]
    async fn stage_segment_rejects_self_inconsistent_segment() {
        let (context, storage) = changelog_test_context();
        let mut segment = test_segment();
        segment.changes.push(segment.changes[0].clone());

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let mut writer = context.writer(&mut *transaction, &mut writes);
        let error = writer
            .stage_segment(segment)
            .await
            .expect_err("inconsistent segment should be rejected");

        assert!(
            error.to_string().contains("duplicate change"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn stage_segment_rejects_duplicate_commit_id_across_segments() {
        let (context, storage) = changelog_test_context();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(test_segment()).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.delete(BY_COMMIT_INDEX_SPACE, by_commit_key("commit-1"));
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut duplicate = test_segment();
        duplicate.header.segment_id = "segment-2".to_string();
        let duplicate = canonicalize_segment(duplicate).unwrap();
        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let mut writer = context.writer(&mut *transaction, &mut writes);
        let error = writer
            .stage_segment(duplicate)
            .await
            .expect_err("duplicate commit id across segments must be rejected");

        assert!(
            error.message.contains("commit 'commit-1' already exists"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn stage_segment_rejects_duplicate_change_id_across_segments() {
        let (context, storage) = changelog_test_context();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(test_segment()).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.delete(BY_CHANGE_INDEX_SPACE, by_change_key("change-1"));
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut duplicate = test_segment();
        duplicate.header.segment_id = "segment-2".to_string();
        duplicate.commits[0].header.id = "commit-2".to_string();
        duplicate.commits[0].header.derivable_change_id = "derived-change-2".to_string();
        duplicate.changes[0].authored_commit_id = Some("commit-2".to_string());
        let duplicate = canonicalize_segment(duplicate).unwrap();
        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let mut writer = context.writer(&mut *transaction, &mut writes);
        let error = writer
            .stage_segment(duplicate)
            .await
            .expect_err("duplicate change id across segments must be rejected");

        assert!(
            error.message.contains("change 'change-1' already exists"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn stage_segment_computes_by_commit_generations_from_parent_edges() {
        let (context, storage) = changelog_test_context();
        let segment = two_commit_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let result = read_test_value_groups(
            &storage,
            vec![(
                BY_COMMIT_INDEX_SPACE,
                vec![by_commit_key("commit-1"), by_commit_key("commit-2")],
            )],
        );
        let commit_1 = decode_by_commit_entry(result[0][0].as_deref().unwrap()).unwrap();
        let commit_2 = decode_by_commit_entry(result[0][1].as_deref().unwrap()).unwrap();
        assert_eq!(commit_1.generation, 0);
        assert_eq!(commit_2.generation, 1);
    }

    #[tokio::test]
    async fn stage_segment_rejects_missing_parent_generation() {
        let (context, storage) = changelog_test_context();
        let mut segment = test_segment();
        segment.commits[0]
            .header
            .parent_commit_ids
            .push("missing-parent".to_string());
        let segment = canonicalize_segment(segment).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let mut writer = context.writer(&mut *transaction, &mut writes);
        let error = writer
            .stage_segment(segment)
            .await
            .expect_err("missing parent generation should be rejected");

        assert!(
            error
                .to_string()
                .contains("parent commit 'missing-parent' is missing from changelog segments"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn stage_segment_derives_external_parent_generation_from_segment_truth() {
        let (context, storage) = changelog_test_context();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(test_segment()).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.delete(BY_COMMIT_INDEX_SPACE, by_commit_key("commit-1"));
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut child = test_segment();
        child.header.segment_id = "segment-2".to_string();
        child.commits[0].header.id = "commit-2".to_string();
        child.commits[0].header.parent_commit_ids = vec!["commit-1".to_string()];
        child.commits[0].header.derivable_change_id = "derived-change-2".to_string();
        child.commits[0].body.membership[0].member_change_id = "change-2".to_string();
        child.commits[0].directory.state_row_identities[0].1 = "change-2".to_string();
        child.commits[0].directory.membership_ordinals[0].0 = "change-2".to_string();
        child.changes[0].id = "change-2".to_string();
        child.changes[0].authored_commit_id = Some("commit-2".to_string());
        let child = canonicalize_segment(child).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(child).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let result = read_test_value_groups(
            &storage,
            vec![(
                BY_COMMIT_INDEX_SPACE,
                vec![by_commit_key("commit-1"), by_commit_key("commit-2")],
            )],
        );
        assert!(
            result[0][0].is_none(),
            "test setup should leave the parent by_commit index missing"
        );
        let commit_2 = decode_by_commit_entry(result[0][1].as_deref().unwrap()).unwrap();
        assert_eq!(commit_2.generation, 1);
    }

    #[tokio::test]
    async fn stage_segment_rejects_same_segment_parent_cycle() {
        let (context, storage) = changelog_test_context();
        let mut segment = two_commit_segment();
        segment.commits[0]
            .header
            .parent_commit_ids
            .push("commit-2".to_string());
        let segment = canonicalize_segment(segment).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let mut writer = context.writer(&mut *transaction, &mut writes);
        let error = writer
            .stage_segment(segment)
            .await
            .expect_err("same-segment parent cycle should be rejected");

        assert!(
            error.to_string().contains("parent cycle"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn rebuild_by_commit_index_rejects_missing_parent_generation() {
        let (context, storage) = changelog_test_context();
        let mut segment = test_segment();
        segment.commits[0]
            .header
            .parent_commit_ids
            .push("missing-parent".to_string());
        let segment = canonicalize_segment(segment).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            SEGMENT_SPACE,
            segment_key("segment-1"),
            segment_value(&segment).unwrap(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .rebuild_by_commit_index()
                .await
                .expect_err("missing parent generation should abort rebuild")
        };

        assert!(
        error.to_string().contains(
            "cannot rebuild by_commit generation because parent commit 'missing-parent' is missing"
        ),
        "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn rebuild_by_commit_index_rejects_stored_parent_cycle() {
        let (context, storage) = changelog_test_context();
        let mut segment = two_commit_segment();
        segment.commits[0]
            .header
            .parent_commit_ids
            .push("commit-2".to_string());
        let segment = canonicalize_segment(segment).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            SEGMENT_SPACE,
            segment_key("segment-1"),
            segment_value(&segment).unwrap(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .rebuild_by_commit_index()
                .await
                .expect_err("stored parent cycle should abort rebuild")
        };

        assert!(
            error.to_string().contains("parent cycle"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn load_commits_visible_rejects_mismatched_commit_visibility_locator() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
        }
        let mut bad_visibility = commit_visibility_from_segment(&segment, "commit-1");
        bad_visibility.location.offset = bad_visibility.location.offset.saturating_add(999);
        writes.put(
            COMMIT_VISIBILITY_SPACE,
            commit_visibility_key("commit-1"),
            commit_visibility_value(&bad_visibility).unwrap(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage);
        let error = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &["commit-1".to_string()],
                projection: CommitProjection::Header,
                visibility: CommitVisibilityMode::RequireVisible,
            })
            .await
            .expect_err("mismatched commit visibility locator should error");

        assert!(
            error.to_string().contains("locator does not match"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn stage_publish_commit_ignores_stale_by_commit_locator() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            BY_COMMIT_INDEX_SPACE,
            by_commit_key("commit-1"),
            by_commit_index_value(&ByCommitEntry {
                commit_id: "commit-1".to_string(),
                location: location("missing-segment", 0, 0, "checksum"),
                parent_commit_ids: Vec::new(),
                generation: 0,
            })
            .unwrap(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let mut writer = context.writer(&mut *transaction, &mut writes);
        writer.stage_publish_commit("commit-1").await.unwrap();
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let result = read_test_value_groups(
            &storage,
            vec![(
                COMMIT_VISIBILITY_SPACE,
                vec![commit_visibility_key("commit-1")],
            )],
        );
        let visibility = decode_commit_visibility(result[0][0].as_deref().unwrap()).unwrap();
        assert_eq!(visibility.location.segment_id, "segment-1");
    }

    #[tokio::test]
    async fn stage_publish_commit_rejects_child_when_parent_is_not_visible_or_staged() {
        let (context, storage) = changelog_test_context();
        let segment = two_commit_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment).await.unwrap();
            writer
                .stage_publish_commit("commit-2")
                .await
                .expect_err("child publication must require visible or staged parent")
        };

        assert!(
            error
                .message
                .contains("parent commit 'commit-1' is not visible or staged"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn stage_publish_commit_rejects_membership_without_changelog_change() {
        let (context, storage) = changelog_test_context();
        let mut segment = test_segment();
        segment.changes.clear();
        segment.directory.changes.clear();
        let segment = canonicalize_segment(segment).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .stage_segment(segment)
                .await
                .expect_err("staging must prove membership changes exist")
        };

        assert!(
            error
                .message
                .contains("authored membership references missing change 'change-1'"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn stage_publish_commit_rejects_state_row_identity_mismatch() {
        let (context, storage) = changelog_test_context();
        let mut segment = test_segment();
        segment.changes[0].entity_id = EntityIdentity::single("other-entity");
        let segment = canonicalize_segment(segment).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .stage_segment(segment)
                .await
                .expect_err("staging must prove StateRowIdentity matches the change")
        };

        assert!(
            error.message.contains("does not match changelog.change"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn stage_publish_commit_rejects_authored_membership_for_existing_change() {
        let (context, storage) = changelog_test_context();
        let mut segment = two_commit_segment();
        segment.commits[1].body.membership[0].role = MembershipRole::Authored;
        segment.commits[1].body.membership[0].source_parent_ordinal = None;
        let segment = canonicalize_segment(segment).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .stage_segment(segment)
                .await
                .expect_err("staging must prove authored membership owns its change")
        };

        assert!(
            error.message.contains(
                "authored membership change 'change-1' has mismatched authored_commit_id"
            ),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn stage_segment_rejects_cross_segment_adopted_membership_authored_by_same_commit() {
        let (context, storage) = changelog_test_context();
        let mut change_segment = test_segment();
        change_segment.header.segment_id = "change-segment".to_string();
        change_segment.commits.clear();
        change_segment.directory.commits.clear();
        change_segment.changes[0].authored_commit_id = Some("commit-2".to_string());
        let change_segment = canonicalize_segment(change_segment).unwrap();

        let mut adopt_segment = test_segment();
        adopt_segment.header.segment_id = "adopt-segment".to_string();
        adopt_segment.changes.clear();
        adopt_segment.directory.changes.clear();
        adopt_segment.commits[0].header.id = "commit-2".to_string();
        adopt_segment.commits[0].header.parent_commit_ids = vec!["missing-parent".to_string()];
        adopt_segment.commits[0].body.membership[0].role = MembershipRole::Adopted;
        adopt_segment.commits[0].body.membership[0].source_parent_ordinal = Some(0);
        let adopt_segment = canonicalize_segment(adopt_segment).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(change_segment).await.unwrap();
            writer
                .stage_segment(adopt_segment)
                .await
                .expect_err("staging must reject self-authored adopted membership")
        };

        assert!(
            error.message.contains("is authored by the same commit"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn stage_segment_accepts_adopted_membership_from_unpublished_stored_parent() {
        let (context, storage) = changelog_test_context();
        let parent_segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(parent_segment).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut adopt_segment = test_segment();
        adopt_segment.header.segment_id = "adopt-segment".to_string();
        adopt_segment.changes.clear();
        adopt_segment.directory.changes.clear();
        adopt_segment.commits[0].header.id = "commit-2".to_string();
        adopt_segment.commits[0].header.parent_commit_ids = vec!["commit-1".to_string()];
        adopt_segment.commits[0].body.membership[0].role = MembershipRole::Adopted;
        adopt_segment.commits[0].body.membership[0].source_parent_ordinal = Some(0);
        let adopt_segment = canonicalize_segment(adopt_segment).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(adopt_segment).await.unwrap();
        }
    }

    #[tokio::test]
    async fn stage_segment_accepts_adopted_membership_from_physical_parent_when_by_commit_is_stale()
    {
        let (context, storage) = changelog_test_context();
        let parent_segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(parent_segment).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            BY_COMMIT_INDEX_SPACE,
            by_commit_key("commit-1"),
            by_commit_index_value(&ByCommitEntry {
                commit_id: "commit-1".to_string(),
                location: SegmentObjectLocation {
                    segment_id: "missing-segment".to_string(),
                    offset: 0,
                    len: 0,
                    checksum: "stale-checksum".to_string(),
                },
                parent_commit_ids: Vec::new(),
                generation: 0,
            })
            .unwrap(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut adopt_segment = test_segment();
        adopt_segment.header.segment_id = "adopt-segment".to_string();
        adopt_segment.changes.clear();
        adopt_segment.directory.changes.clear();
        adopt_segment.commits[0].header.id = "commit-2".to_string();
        adopt_segment.commits[0].header.parent_commit_ids = vec!["commit-1".to_string()];
        adopt_segment.commits[0].body.membership[0].role = MembershipRole::Adopted;
        adopt_segment.commits[0].body.membership[0].source_parent_ordinal = Some(0);
        let adopt_segment = canonicalize_segment(adopt_segment).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(adopt_segment).await.unwrap();
        }
    }

    #[tokio::test]
    async fn stage_publish_commit_accepts_parent_staged_in_same_write_set() {
        let (context, storage) = changelog_test_context();
        let segment = two_commit_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
            writer.stage_publish_commit("commit-2").await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage);
        let commits = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &["commit-1".to_string(), "commit-2".to_string()],
                projection: CommitProjection::Header,
                visibility: CommitVisibilityMode::RequireVisible,
            })
            .await
            .unwrap();

        assert_eq!(commits.entries.len(), 2);
        assert!(commits.entries.iter().all(Option::is_some));
    }

    #[tokio::test]
    async fn stage_publish_commit_accepts_adopted_membership_reachable_through_source_parent_history(
    ) {
        let (context, storage) = changelog_test_context();
        let mut segment = two_commit_segment();
        segment.commits.push(SegmentCommit {
            header: CommitHeader {
                id: "commit-3".to_string(),
                parent_commit_ids: vec!["commit-2".to_string()],
                derivable_change_id: "derived-change-3".to_string(),
                author_account_ids: vec!["account-3".to_string()],
                created_at: "2026-05-12T00:02:00Z".to_string(),
                membership_count: 1,
            },
            body: CommitBody {
                membership: vec![MembershipRecord {
                    member_change_id: "change-1".to_string(),
                    role: MembershipRole::Adopted,
                    source_parent_ordinal: Some(0),
                }],
            },
            directory: SegmentCommitDirectory {
                state_row_identities: vec![(
                    state_row_identity("message", "file-1", "entity-1"),
                    "change-1".to_string(),
                )],
                membership_ordinals: vec![("change-1".to_string(), 0)],
            },
            checksum: String::new(),
        });
        let segment = canonicalize_segment(segment).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
            writer.stage_publish_commit("commit-2").await.unwrap();
            writer.stage_publish_commit("commit-3").await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();
    }

    #[tokio::test]
    async fn stage_publish_commit_rejects_adopted_membership_not_reachable_from_source_parent() {
        let (context, storage) = changelog_test_context();
        let mut segment = test_segment();
        segment.commits.push(SegmentCommit {
            header: CommitHeader {
                id: "commit-other-parent".to_string(),
                parent_commit_ids: Vec::new(),
                derivable_change_id: "derived-change-other".to_string(),
                author_account_ids: vec!["account-2".to_string()],
                created_at: "2026-05-12T00:01:00Z".to_string(),
                membership_count: 0,
            },
            body: CommitBody {
                membership: Vec::new(),
            },
            directory: SegmentCommitDirectory {
                state_row_identities: Vec::new(),
                membership_ordinals: Vec::new(),
            },
            checksum: String::new(),
        });
        segment.commits.push(SegmentCommit {
            header: CommitHeader {
                id: "commit-3".to_string(),
                parent_commit_ids: vec!["commit-other-parent".to_string(), "commit-1".to_string()],
                derivable_change_id: "derived-change-3".to_string(),
                author_account_ids: vec!["account-3".to_string()],
                created_at: "2026-05-12T00:02:00Z".to_string(),
                membership_count: 1,
            },
            body: CommitBody {
                membership: vec![MembershipRecord {
                    member_change_id: "change-1".to_string(),
                    role: MembershipRole::Adopted,
                    source_parent_ordinal: Some(0),
                }],
            },
            directory: SegmentCommitDirectory {
                state_row_identities: vec![(
                    state_row_identity("message", "file-1", "entity-1"),
                    "change-1".to_string(),
                )],
                membership_ordinals: vec![("change-1".to_string(), 0)],
            },
            checksum: String::new(),
        });
        let segment = canonicalize_segment(segment).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .stage_segment(segment)
                .await
                .expect_err("staging must prove adopted change reaches through source parent")
        };

        assert!(
            error
                .message
                .contains("adopted membership change 'change-1' is not reachable"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn stage_publish_commit_rejects_adopted_membership_when_source_parent_winner_differs() {
        let (context, storage) = changelog_test_context();
        let mut segment = test_segment();
        let mut change = segment.changes[0].clone();
        change.id = "change-2".to_string();
        change.authored_commit_id = Some("commit-2".to_string());
        change.created_at = "2026-05-12T00:01:00Z".to_string();
        segment.changes.push(change);
        segment.directory.changes.push((
            "change-2".to_string(),
            location("segment-1", 70, 40, "change-2-checksum"),
        ));
        segment.commits.push(SegmentCommit {
            header: CommitHeader {
                id: "commit-2".to_string(),
                parent_commit_ids: vec!["commit-1".to_string()],
                derivable_change_id: "derived-change-2".to_string(),
                author_account_ids: vec!["account-2".to_string()],
                created_at: "2026-05-12T00:01:00Z".to_string(),
                membership_count: 1,
            },
            body: CommitBody {
                membership: vec![MembershipRecord {
                    member_change_id: "change-2".to_string(),
                    role: MembershipRole::Authored,
                    source_parent_ordinal: None,
                }],
            },
            directory: SegmentCommitDirectory {
                state_row_identities: vec![(
                    state_row_identity("message", "file-1", "entity-1"),
                    "change-2".to_string(),
                )],
                membership_ordinals: vec![("change-2".to_string(), 0)],
            },
            checksum: String::new(),
        });
        segment.commits.push(SegmentCommit {
            header: CommitHeader {
                id: "commit-3".to_string(),
                parent_commit_ids: vec!["commit-2".to_string()],
                derivable_change_id: "derived-change-3".to_string(),
                author_account_ids: vec!["account-3".to_string()],
                created_at: "2026-05-12T00:02:00Z".to_string(),
                membership_count: 1,
            },
            body: CommitBody {
                membership: vec![MembershipRecord {
                    member_change_id: "change-1".to_string(),
                    role: MembershipRole::Adopted,
                    source_parent_ordinal: Some(0),
                }],
            },
            directory: SegmentCommitDirectory {
                state_row_identities: vec![(
                    state_row_identity("message", "file-1", "entity-1"),
                    "change-1".to_string(),
                )],
                membership_ordinals: vec![("change-1".to_string(), 0)],
            },
            checksum: String::new(),
        });
        let segment = canonicalize_segment(segment).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .stage_segment(segment)
                .await
                .expect_err("staging must prove adopted change is the source winner")
        };

        assert!(
            error
                .message
                .contains("adopted membership change 'change-1' is not the source parent"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn stage_publish_commit_uses_first_parent_projection_for_source_parent_winner() {
        let (context, storage) = changelog_test_context();
        let mut segment = test_segment();
        let mut source_change = segment.changes[0].clone();
        source_change.id = "source-change".to_string();
        source_change.authored_commit_id = Some("source-commit".to_string());
        source_change.created_at = "2026-05-12T00:01:00Z".to_string();
        segment.changes.push(source_change);
        let mut target_change = segment.changes[0].clone();
        target_change.id = "target-change".to_string();
        target_change.authored_commit_id = Some("target-commit".to_string());
        target_change.created_at = "2026-05-12T00:01:00Z".to_string();
        segment.changes.push(target_change);
        segment.directory.changes.push((
            "source-change".to_string(),
            location("segment-1", 70, 40, "source-change-checksum"),
        ));
        segment.directory.changes.push((
            "target-change".to_string(),
            location("segment-1", 110, 40, "target-change-checksum"),
        ));
        segment.commits.push(SegmentCommit {
            header: CommitHeader {
                id: "target-commit".to_string(),
                parent_commit_ids: Vec::new(),
                derivable_change_id: "derived-target".to_string(),
                author_account_ids: vec!["account-target".to_string()],
                created_at: "2026-05-12T00:01:00Z".to_string(),
                membership_count: 1,
            },
            body: CommitBody {
                membership: vec![MembershipRecord {
                    member_change_id: "target-change".to_string(),
                    role: MembershipRole::Authored,
                    source_parent_ordinal: None,
                }],
            },
            directory: SegmentCommitDirectory {
                state_row_identities: vec![(
                    state_row_identity("message", "file-1", "entity-1"),
                    "target-change".to_string(),
                )],
                membership_ordinals: vec![("target-change".to_string(), 0)],
            },
            checksum: String::new(),
        });
        segment.commits.push(SegmentCommit {
            header: CommitHeader {
                id: "source-commit".to_string(),
                parent_commit_ids: Vec::new(),
                derivable_change_id: "derived-source".to_string(),
                author_account_ids: vec!["account-source".to_string()],
                created_at: "2026-05-12T00:01:00Z".to_string(),
                membership_count: 1,
            },
            body: CommitBody {
                membership: vec![MembershipRecord {
                    member_change_id: "source-change".to_string(),
                    role: MembershipRole::Authored,
                    source_parent_ordinal: None,
                }],
            },
            directory: SegmentCommitDirectory {
                state_row_identities: vec![(
                    state_row_identity("message", "file-1", "entity-1"),
                    "source-change".to_string(),
                )],
                membership_ordinals: vec![("source-change".to_string(), 0)],
            },
            checksum: String::new(),
        });
        segment.commits.push(SegmentCommit {
            header: CommitHeader {
                id: "merge-parent".to_string(),
                parent_commit_ids: vec!["target-commit".to_string(), "source-commit".to_string()],
                derivable_change_id: "derived-merge-parent".to_string(),
                author_account_ids: vec!["account-merge".to_string()],
                created_at: "2026-05-12T00:02:00Z".to_string(),
                membership_count: 1,
            },
            body: CommitBody {
                membership: vec![MembershipRecord {
                    member_change_id: "target-change".to_string(),
                    role: MembershipRole::Adopted,
                    source_parent_ordinal: Some(0),
                }],
            },
            directory: SegmentCommitDirectory {
                state_row_identities: vec![(
                    state_row_identity("message", "file-1", "entity-1"),
                    "target-change".to_string(),
                )],
                membership_ordinals: vec![("target-change".to_string(), 0)],
            },
            checksum: String::new(),
        });
        segment.commits.push(SegmentCommit {
            header: CommitHeader {
                id: "adopting-commit".to_string(),
                parent_commit_ids: vec!["merge-parent".to_string()],
                derivable_change_id: "derived-adopting".to_string(),
                author_account_ids: vec!["account-adopting".to_string()],
                created_at: "2026-05-12T00:03:00Z".to_string(),
                membership_count: 1,
            },
            body: CommitBody {
                membership: vec![MembershipRecord {
                    member_change_id: "source-change".to_string(),
                    role: MembershipRole::Adopted,
                    source_parent_ordinal: Some(0),
                }],
            },
            directory: SegmentCommitDirectory {
                state_row_identities: vec![(
                    state_row_identity("message", "file-1", "entity-1"),
                    "source-change".to_string(),
                )],
                membership_ordinals: vec![("source-change".to_string(), 0)],
            },
            checksum: String::new(),
        });
        let segment = canonicalize_segment(segment).unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .stage_segment(segment)
                .await
                .expect_err("staging source parent winner must use first-parent projection")
        };

        assert!(
            error
                .message
                .contains("adopted membership change 'source-change' is not the source parent"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn load_changes_visible_can_be_proven_by_adopting_commit_membership() {
        let (context, storage) = changelog_test_context();
        let mut segment = test_segment();
        segment.commits.push(SegmentCommit {
            header: CommitHeader {
                id: "commit-2".to_string(),
                parent_commit_ids: vec!["commit-1".to_string()],
                derivable_change_id: "derived-change-2".to_string(),
                author_account_ids: vec!["account-2".to_string()],
                created_at: "2026-05-12T00:01:00Z".to_string(),
                membership_count: 1,
            },
            body: CommitBody {
                membership: vec![MembershipRecord {
                    member_change_id: "change-1".to_string(),
                    role: MembershipRole::Adopted,
                    source_parent_ordinal: Some(0),
                }],
            },
            directory: SegmentCommitDirectory {
                state_row_identities: vec![(
                    state_row_identity("message", "file-1", "entity-1"),
                    "change-1".to_string(),
                )],
                membership_ordinals: vec![("change-1".to_string(), 0)],
            },
            checksum: "commit-2-checksum".to_string(),
        });
        segment.directory.commits.push((
            "commit-2".to_string(),
            location("segment-1", 50, 20, "commit-2-checksum"),
        ));
        segment.header.commit_count = 2;

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
            writer.stage_publish_commit("commit-2").await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage);
        let batch = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await
            .unwrap();

        assert_eq!(
            batch.entries,
            vec![Some(ChangeLoadEntry::Segment(segment.changes[0].clone()))]
        );
    }

    #[tokio::test]
    async fn load_changes_visible_returns_none_when_visible_commit_membership_omits_change() {
        let (context, storage) = changelog_test_context();
        let mut segment = test_segment();
        segment.commits[0].body.membership.clear();
        segment.commits[0].header.membership_count = 0;
        segment.commits[0].directory.state_row_identities.clear();
        segment.commits[0].directory.membership_ordinals.clear();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage.clone());
        let visible = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await
            .unwrap();
        assert_eq!(visible.entries, vec![None]);

        let mut reader = context.reader(storage);
        let physical = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();
        assert_eq!(
            physical.entries,
            vec![Some(ChangeLoadEntry::Segment(segment.changes[0].clone()))]
        );
    }

    #[tokio::test]
    async fn load_changes_visible_errors_when_visible_membership_has_no_physical_change() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut corrupt_segment = segment;
        corrupt_segment.changes.clear();
        corrupt_segment.directory.changes.clear();
        let corrupt_segment = canonicalize_segment(corrupt_segment).unwrap();
        write_raw_segment(&storage, &corrupt_segment).await;

        let mut reader = context.reader(storage);
        let error = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await
            .expect_err("visible membership without physical change must be corruption");

        assert!(
            error.message.contains("without that change")
                || error
                    .message
                    .contains("locator does not match segment directory")
                || error
                    .message
                    .contains("references missing authored change 'change-1'"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn load_changes_rejects_mutated_change_body_with_stale_checksum() {
        let (context, storage) = changelog_test_context();
        let segment = test_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment.clone()).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut corrupted = segment;
        corrupted.changes[0].created_at = "2026-05-12T00:00:01Z".to_string();
        write_raw_segment(&storage, &corrupted).await;

        let mut reader = context.reader(storage);
        let error = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::PhysicalOnly,
            })
            .await
            .expect_err("stale checksum must reject mutated change body");

        assert!(
            error.message.contains("canonical checksum"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn load_commits_physical_only_decodes_same_segment_once_for_batch() {
        let (context, storage) = changelog_test_context();
        let segment = two_commit_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let (reader_store, segment_gets) = counting_reader(storage);
        let mut reader = context.reader(reader_store);
        let batch = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &["commit-1".to_string(), "commit-2".to_string()],
                projection: CommitProjection::Header,
                visibility: CommitVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();

        assert_eq!(batch.entries.len(), 2);
        assert!(batch.entries.iter().all(Option::is_some));
        assert_eq!(segment_gets.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn load_changes_physical_only_decodes_same_segment_once_for_batch() {
        let (context, storage) = changelog_test_context();
        let segment = two_change_segment();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment).await.unwrap();
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let (reader_store, segment_gets) = counting_reader(storage);
        let mut reader = context.reader(reader_store);
        let batch = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &["change-1".to_string(), "change-2".to_string()],
                projection: ChangeProjection::PhysicalLocation,
                visibility: ChangeVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();

        assert_eq!(batch.entries.len(), 2);
        assert!(batch.entries.iter().all(Option::is_some));
        assert_eq!(segment_gets.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
