//! Materialized serving state for one tracked branch head.
//!
//! Commit roots are sparse historical checkpoints. This table is the durable,
//! generation-keyed serving state for one branch head, letting the normal
//! live-state path range scan rows and hydrate JSON directly without replaying
//! changelog history. A marker binds a generation to the branch ref's commit.
//! Any mismatch is a direct-plane miss and callers take the historical fallback.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use std::sync::Arc;

use bytes::Bytes;

use crate::LixError;
use crate::NullableKeyFilter;
use crate::branch::{BranchHeadControl, BranchHeadControlContext};
use crate::changelog::{ChangeId, ChangeRecordProjection, CommitId};
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::json_store::{
    JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonSlot, JsonSlotRef, JsonStoreContext,
};
use crate::live_state::MaterializedLiveStateRow;
use crate::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StoragePrefix,
    StorageProjectedValue, StorageReadEntry, StorageScanOptions, StorageSpace, StorageSpaceId,
    StorageValue, StorageWriteSet,
};
use crate::storage_codec;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateDiff, TrackedStateDiffEntry, TrackedStateDiffIdentity,
    TrackedStateDiffKind, TrackedStateDiffRequest, TrackedStateDiffRow, TrackedStateFilter,
    TrackedStateKey, TrackedStateScanRequest,
};

// v6 makes the durable tracked head authoritative for normal current reads.
// A physical record owns every file-backed member of one logical entity PK.
// Public entity reads know `(branch, schema, entity_pk)` but intentionally do
// not invent a `file_id`; keeping those members together lets that common
// lookup be a RocksDB point get rather than a prefix scan. Repositories use a
// protocol gate, so predecessor bytes are never interpreted as v6 groups.
pub(crate) const TRACKED_HEAD_GROUP_NAMESPACE: &str = "live_state.tracked_head_group.v6";
pub(crate) const TRACKED_HEAD_MEMBER_NAMESPACE: &str = "live_state.tracked_head_member.v6";
pub(crate) const TRACKED_HEAD_MARKER_NAMESPACE: &str = "live_state.tracked_head_marker.v6";
pub(crate) const TRACKED_HEAD_GROUP_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_0012), TRACKED_HEAD_GROUP_NAMESPACE);
/// File-id projection for explicit file-backed identities.
///
/// The group value remains authoritative for normal logical-PK reads. This
/// narrow projection avoids turning `file_id = ?` reads into an unbounded
/// group-value fetch when a logical PK has many file members. Its physical
/// order is `(branch, generation, schema, file_id, entity_pk)`, so both an
/// exact full identity and a schema-scoped file-id scan avoid unpacking
/// unrelated entity groups.
pub(crate) const TRACKED_HEAD_MEMBER_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_0013), TRACKED_HEAD_MEMBER_NAMESPACE);
pub(crate) const TRACKED_HEAD_MARKER_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_0014), TRACKED_HEAD_MARKER_NAMESPACE);

/// Sparse enumeration index for the first-before summaries co-located in v6
/// head groups. It contains no row payload: the authoritative current row
/// and its immutable baseline remain one physical group record.
pub(crate) const TRACKED_WORKING_DIFF_GROUP_NAMESPACE: &str =
    "live_state.tracked_working_diff_group.v2";
pub(crate) const TRACKED_WORKING_DIFF_MARKER_NAMESPACE: &str =
    "live_state.tracked_working_diff_marker.v2";
pub(crate) const TRACKED_WORKING_DIFF_GROUP_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_0016),
    TRACKED_WORKING_DIFF_GROUP_NAMESPACE,
);
pub(crate) const TRACKED_WORKING_DIFF_MARKER_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_0018),
    TRACKED_WORKING_DIFF_MARKER_NAMESPACE,
);

/// Immutable manifest for the currently readable generation of a branch.
///
/// A new generation is used after a branch ref moves away from the parent of
/// a normal commit. Old rows can remain in storage: they are unreachable
/// without this marker and therefore cannot affect serving reads.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct TrackedHeadMarker {
    head_commit_id: CommitId,
    generation: CommitId,
    /// Checkpoint whose first-before baselines live in this generation.
    /// `None` means the head is still a correct serving projection but cannot
    /// answer a checkpoint-relative direct diff.
    #[musli(with = storage_codec::option)]
    working_diff_checkpoint_commit_id: Option<CommitId>,
}

/// The current serving generation plus the checkpoint epoch to which its
/// first-before summaries are bound. This is a read-side observation only;
/// the private marker remains the durable wire record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TrackedHeadMarkerInfo {
    pub(crate) generation: CommitId,
    pub(crate) working_diff_checkpoint_commit_id: Option<CommitId>,
}

/// The active checkpoint epoch for the sparse working-diff indexes.
///
/// `None` is the freshly published checkpoint state: it is known empty until
/// the first ordinary child bootstraps a complete v6 serving generation.
/// Once initialized, the generation is immutable for serial normal commits;
/// the tracked-head marker and branch control publish the moving head.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct TrackedWorkingDiffEpoch {
    pub(crate) checkpoint_commit_id: CommitId,
    // Musli's built-in packed Option encoding is terminal-field oriented.
    // This explicit bool-prefixed representation keeps `coverage` readable
    // when a freshly published checkpoint has no generation yet.
    #[musli(with = storage_codec::option)]
    pub(crate) generation: Option<CommitId>,
    pub(crate) coverage: WorkingDiffIndexCoverage,
}

/// A tiny atomic coverage proof for the current checkpoint's group index.
///
/// Count catches loss or duplication; the XOR of BLAKE3 key hashes also
/// catches a same-count replacement without adding another read structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct WorkingDiffIndexCoverage {
    group_count: u64,
    group_key_xor: JsonRef,
}

impl Default for WorkingDiffIndexCoverage {
    fn default() -> Self {
        Self {
            group_count: 0,
            group_key_xor: JsonRef::from_hash_bytes([0; JSON_REF_BYTES]),
        }
    }
}

impl WorkingDiffIndexCoverage {
    fn add_encoded_group_key(&mut self, key: &[u8]) -> Option<()> {
        self.group_count = self.group_count.checked_add(1)?;
        let hash = blake3::hash(key);
        let mut group_key_xor = *self.group_key_xor.as_hash_array();
        for (target, source) in group_key_xor.iter_mut().zip(hash.as_bytes()) {
            *target ^= source;
        }
        self.group_key_xor = JsonRef::from_hash_bytes(group_key_xor);
        Some(())
    }
}

/// A checkpoint-relative direct diff assembled from the current v6 head.
/// This is internal plumbing for SQL working-change and checkpoint compaction;
/// the public API remains the existing tracked-state diff representation.
pub(crate) struct TrackedWorkingDiff {
    pub(crate) checkpoint_commit_id: CommitId,
    pub(crate) diff: TrackedStateDiff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkingDiffBaseline {
    Clean,
    Absent(CommitId),
    Present {
        checkpoint_commit_id: CommitId,
        version: WorkingDiffVersion,
    },
}

impl WorkingDiffBaseline {
    fn is_for_checkpoint(self, checkpoint_commit_id: Option<CommitId>) -> bool {
        match (self, checkpoint_commit_id) {
            (
                Self::Absent(baseline_checkpoint_id)
                | Self::Present {
                    checkpoint_commit_id: baseline_checkpoint_id,
                    ..
                },
                Some(checkpoint_commit_id),
            ) => baseline_checkpoint_id == checkpoint_commit_id,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkingDiffVersion {
    change_id: ChangeId,
    commit_id: CommitId,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: WorkingDiffSlotFingerprint,
    metadata: WorkingDiffSlotFingerprint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkingDiffSlotFingerprint {
    kind: u8,
    hash: [u8; JSON_REF_BYTES],
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, musli::Encode, musli::Decode)]
#[musli(packed)]
struct HeadIdentity {
    branch_id: String,
    generation: CommitId,
    schema_key: String,
    entity_pk: EntityPk,
    #[musli(with = storage_codec::option)]
    file_id: Option<String>,
}

/// The physical v6 key for all current members of one logical entity PK.
///
/// `file_id` is deliberately not a key part. It remains part of the packed
/// group value so a tombstone or a file-backed variant affects only its own
/// full row identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct HeadGroupIdentity {
    branch_id: String,
    generation: CommitId,
    schema_key: String,
    entity_pk: EntityPk,
}

/// The portion of a head-row key that varies within one branch generation.
///
/// A full table scan already constrains `branch_id` and `generation` in the
/// RocksDB prefix. Keeping that immutable scope out of every decoded row
/// avoids parsing and allocating the same two key parts 10,000 times.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct HeadRowIdentity {
    schema_key: String,
    entity_pk: EntityPk,
    file_id: Option<String>,
}

impl HeadIdentity {
    fn into_row_identity(self) -> HeadRowIdentity {
        HeadRowIdentity {
            schema_key: self.schema_key,
            entity_pk: self.entity_pk,
            file_id: self.file_id,
        }
    }

    fn group_identity(&self) -> HeadGroupIdentity {
        HeadGroupIdentity {
            branch_id: self.branch_id.clone(),
            generation: self.generation,
            schema_key: self.schema_key.clone(),
            entity_pk: self.entity_pk.clone(),
        }
    }
}

/// Write-side representation of a v3 head row.
///
/// This exists only while a transaction is being staged. Read-side code uses
/// [`HeadValueView`], which parses the fixed header directly from RocksDB's
/// returned bytes and never builds this allocation-heavy representation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HeadValue {
    change_id: ChangeId,
    commit_id: CommitId,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: JsonSlot,
    metadata: JsonSlot,
}

impl HeadValue {
    fn as_ref(&self) -> HeadValueRef<'_> {
        HeadValueRef {
            change_id: self.change_id,
            commit_id: self.commit_id,
            deleted: self.deleted,
            created_at: self.created_at,
            updated_at: self.updated_at,
            snapshot: self.snapshot.as_ref_slot(),
            metadata: self.metadata.as_ref_slot(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HeadValueRef<'a> {
    change_id: ChangeId,
    commit_id: CommitId,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: JsonSlotRef<'a>,
    metadata: JsonSlotRef<'a>,
}

#[derive(Debug, Clone, Copy, musli::Encode)]
#[musli(packed)]
struct BranchRef<'a> {
    branch_id: &'a str,
}

#[derive(Debug, Clone, musli::Encode, musli::Decode)]
#[musli(packed)]
struct BranchRefKey {
    branch_id: String,
}

/// Zero-copy normal tracked mutation staged into a head generation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedHeadDeltaRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) entity_pk: &'a EntityPk,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) snapshot: JsonSlotRef<'a>,
    pub(crate) metadata: JsonSlotRef<'a>,
}

impl<'a> TrackedHeadDeltaRef<'a> {
    fn value_ref(&self, created_at: LixTimestamp) -> HeadValueRef<'a> {
        HeadValueRef {
            change_id: self.change_id,
            commit_id: self.commit_id,
            deleted: self.deleted,
            created_at,
            updated_at: self.updated_at,
            snapshot: if self.deleted {
                JsonSlotRef::None
            } else {
                self.snapshot
            },
            metadata: if self.deleted {
                JsonSlotRef::None
            } else {
                self.metadata
            },
        }
    }
}

/// Factory for tracked-head readers and writers.
#[derive(Clone, Copy, Default)]
pub(crate) struct TrackedHeadContext;

impl TrackedHeadContext {
    pub(crate) fn new() -> Self {
        Self
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn reader<S>(&self, store: S) -> TrackedHeadStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        TrackedHeadStoreReader { store }
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn writer<'a, S>(
        &'a self,
        store: &'a S,
        writes: &'a mut StorageWriteSet,
    ) -> TrackedHeadWriter<'a, S>
    where
        S: StorageAdapterRead + ?Sized,
    {
        TrackedHeadWriter { store, writes }
    }
}

/// Direct materializer for the current tracked branch generation.
pub(crate) struct TrackedHeadStoreReader<S> {
    store: S,
}

impl<S> TrackedHeadStoreReader<S>
where
    S: StorageAdapterRead,
{
    /// v6 control-plane variant of [`Self::scan_live_rows_if_current`].
    ///
    /// The direct branch control and the v6 marker are published in one
    /// storage commit. Requiring both the head and generation to agree makes
    /// a partially rebuilt or stale group projection a clean historical
    /// fallback rather than visible current state.
    pub(crate) async fn scan_live_rows_if_control_current(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        request: &TrackedStateScanRequest,
    ) -> Result<Option<Vec<MaterializedLiveStateRow>>, LixError> {
        self.scan_live_rows_for_marker(
            branch_id,
            self.marker_if_control_current(branch_id, control).await?,
            request,
        )
        .await
    }

    /// Returns `None` when this branch has no projection for the canonical
    /// branch ref. That is a direct-plane miss, not empty tracked state.
    #[cfg(test)]
    pub(crate) async fn scan_live_rows_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<Option<Vec<MaterializedLiveStateRow>>, LixError> {
        self.scan_live_rows_for_marker(
            branch_id,
            self.marker_if_current(branch_id, expected_head).await?,
            request,
        )
        .await
    }

    async fn scan_live_rows_for_marker(
        &self,
        branch_id: &str,
        marker: Option<TrackedHeadMarker>,
        request: &TrackedStateScanRequest,
    ) -> Result<Option<Vec<MaterializedLiveStateRow>>, LixError> {
        let Some(marker) = marker else {
            return Ok(None);
        };
        let entries = scan_entries(
            &self.store,
            branch_id,
            marker.generation,
            &request.filter,
            None,
        )
        .await?;
        let projection = ChangeRecordProjection::from_columns(&request.read_columns.columns);
        let mut rows =
            materialize_live_entries(&self.store, entries, projection, branch_id).await?;
        if !request.filter.include_tombstones {
            rows.retain(|row| !row.deleted);
        }
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(Some(rows))
    }

    /// Like the immutable-root point batch, preserves input cardinality and
    /// returns tombstones for the visibility layer to resolve.
    #[cfg(test)]
    pub(crate) async fn load_projected_live_rows_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<Option<Vec<Option<MaterializedLiveStateRow>>>, LixError> {
        self.load_projected_live_rows_for_marker(
            branch_id,
            self.marker_if_current(branch_id, expected_head).await?,
            keys,
            projection,
        )
        .await
    }

    async fn load_projected_live_rows_for_marker(
        &self,
        branch_id: &str,
        marker: Option<TrackedHeadMarker>,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<Option<Vec<Option<MaterializedLiveStateRow>>>, LixError> {
        if keys.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let Some(marker) = marker else {
            return Ok(None);
        };

        let mut output_indices = BTreeMap::<HeadIdentity, Vec<usize>>::new();
        for (index, key) in keys.iter().enumerate() {
            output_indices
                .entry(HeadIdentity {
                    branch_id: branch_id.to_string(),
                    generation: marker.generation,
                    schema_key: key.schema_key.clone(),
                    entity_pk: key.entity_pk.clone(),
                    file_id: key.file_id.clone(),
                })
                .or_default()
                .push(index);
        }
        let groups = output_indices
            .keys()
            .filter(|identity| identity.file_id.is_none())
            .map(HeadIdentity::group_identity)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let member_identities = output_indices
            .keys()
            .filter(|identity| identity.file_id.is_some())
            .cloned()
            .collect::<Vec<_>>();
        let values = load_group_bytes(&self.store, &groups).await?;
        let mut entries = Vec::new();
        for (group, value) in groups.into_iter().zip(values) {
            let Some(value) = value else {
                continue;
            };
            for member in decode_head_group_members(&value)? {
                let identity = HeadIdentity {
                    branch_id: group.branch_id.clone(),
                    generation: group.generation,
                    schema_key: group.schema_key.clone(),
                    entity_pk: group.entity_pk.clone(),
                    file_id: member.file_id,
                };
                if output_indices.contains_key(&identity) {
                    entries.push((identity.into_row_identity(), member.value));
                }
            }
        }
        let member_values = load_member_bytes(&self.store, &member_identities).await?;
        for (identity, value) in member_identities.into_iter().zip(member_values) {
            if let Some(value) = value {
                entries.push((identity.into_row_identity(), value));
            }
        }
        let rows = materialize_live_entries(&self.store, entries, *projection, branch_id).await?;
        let rows_by_identity = rows
            .into_iter()
            .map(|row| {
                (
                    HeadRowIdentity {
                        schema_key: row.schema_key.clone(),
                        entity_pk: row.entity_pk.clone(),
                        file_id: row.file_id.clone(),
                    },
                    row,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut output = vec![None; keys.len()];
        for (identity, indices) in output_indices {
            if let Some(row) = rows_by_identity.get(&identity.into_row_identity()) {
                for index in indices {
                    output[index] = Some(row.clone());
                }
            }
        }
        Ok(Some(output))
    }

    /// v6 control-plane variant of
    /// [`Self::load_projected_live_rows_if_current`].
    pub(crate) async fn load_projected_live_rows_if_control_current(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<Option<Vec<Option<MaterializedLiveStateRow>>>, LixError> {
        self.load_projected_live_rows_for_marker(
            branch_id,
            self.marker_if_control_current(branch_id, control).await?,
            keys,
            projection,
        )
        .await
    }

    /// Returns the durable serving generation exactly when the marker is
    /// bound to `expected_head`. Commit staging passes this value directly to
    /// the writer so a serial child needs one marker point read, not two.
    #[cfg(test)]
    pub(crate) async fn generation_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
    ) -> Result<Option<CommitId>, LixError> {
        Ok(self
            .marker_if_current(branch_id, expected_head)
            .await?
            .map(|marker| marker.generation))
    }

    /// Returns the durable serving generation and working-diff epoch binding
    /// only when both are atomically bound to the observed branch control.
    pub(crate) async fn marker_info_if_control_current(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
    ) -> Result<Option<TrackedHeadMarkerInfo>, LixError> {
        Ok(self
            .marker_if_control_current(branch_id, control)
            .await?
            .map(|marker| TrackedHeadMarkerInfo {
                generation: marker.generation,
                working_diff_checkpoint_commit_id: marker.working_diff_checkpoint_commit_id,
            }))
    }

    /// Loads the checkpoint epoch marker without treating it as visibility.
    /// Commit staging combines it with a coherent v6 branch-control
    /// observation before deciding whether it can preserve first-before
    /// summaries.
    pub(crate) async fn working_diff_epoch(
        &self,
        branch_id: &str,
    ) -> Result<Option<TrackedWorkingDiffEpoch>, LixError> {
        load_tracked_working_diff_epoch(&self.store, branch_id).await
    }

    /// Returns an O(ever-dirty groups since checkpoint) tracked diff only when the sparse
    /// working-diff epoch and the current v6 serving generation agree. Any
    /// missing, stale, or malformed auxiliary record returns `None`, leaving
    /// the canonical historical replay path authoritative.
    pub(crate) async fn working_diff_if_control_current(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        request: &TrackedStateDiffRequest,
    ) -> Result<Option<TrackedWorkingDiff>, LixError> {
        // The marker is an accelerator, not a source of visibility. A
        // malformed or unavailable auxiliary marker selects canonical replay
        // instead of making a valid SQL query fail.
        let Ok(Some(epoch)) = self.working_diff_epoch(branch_id).await else {
            return Ok(None);
        };
        let Some(generation) = epoch.generation else {
            return Ok(
                (epoch.checkpoint_commit_id == control.head_commit_id).then_some(
                    TrackedWorkingDiff {
                        checkpoint_commit_id: epoch.checkpoint_commit_id,
                        diff: TrackedStateDiff::default(),
                    },
                ),
            );
        };
        let Ok(Some(marker)) = self.marker_if_control_current(branch_id, control).await else {
            return Ok(None);
        };
        if generation != control.generation
            || marker.working_diff_checkpoint_commit_id != Some(epoch.checkpoint_commit_id)
        {
            return Ok(None);
        }
        let Some(entries) = working_diff_entries_from_current_head(
            &self.store,
            branch_id,
            epoch.checkpoint_commit_id,
            generation,
            epoch.coverage,
            &request.filter,
        )
        .await?
        else {
            return Ok(None);
        };
        Ok(Some(TrackedWorkingDiff {
            checkpoint_commit_id: epoch.checkpoint_commit_id,
            diff: TrackedStateDiff { entries },
        }))
    }

    #[cfg(test)]
    async fn marker_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
    ) -> Result<Option<TrackedHeadMarker>, LixError> {
        let expected_head = CommitId::parse_lix(expected_head, "tracked-head expected commit")?;
        let marker = load_marker(&self.store, branch_id).await?;
        Ok(marker.filter(|marker| marker.head_commit_id == expected_head))
    }

    async fn marker_if_control_current(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
    ) -> Result<Option<TrackedHeadMarker>, LixError> {
        let marker = load_marker(&self.store, branch_id).await?;
        Ok(marker.filter(|marker| {
            marker.head_commit_id == control.head_commit_id
                && marker.generation == control.generation
        }))
    }
}

/// Writer for an atomic branch-head projection update.
pub(crate) struct TrackedHeadWriter<'a, S: ?Sized> {
    store: &'a S,
    writes: &'a mut StorageWriteSet,
}

impl<S> TrackedHeadWriter<'_, S>
where
    S: StorageAdapterRead + ?Sized,
{
    /// Incrementally updates a matching parent generation, or creates a fresh
    /// generation from a caller-provided parent snapshot. The latter is used
    /// after branch movement and for old repositories which predate this
    /// serving table.
    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) async fn stage_commit(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[TrackedHeadDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
    ) -> Result<CommitId, LixError> {
        let mut working_diff_coverage = WorkingDiffIndexCoverage::default();
        self.stage_commit_with_working_diff(
            branch_id,
            parent_generation,
            new_head,
            deltas,
            absence_guards,
            parent_rows,
            None,
            None,
            &mut working_diff_coverage,
        )
        .await
    }

    /// Internal variant used only when an active checkpoint epoch makes the
    /// v6 group baseline authoritative for `lix_working_change`.
    pub(crate) async fn stage_commit_with_working_diff(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[TrackedHeadDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
        working_diff_marker_checkpoint_commit_id: Option<CommitId>,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        working_diff_coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<CommitId, LixError> {
        let matches_parent = parent_generation.is_some();
        let generation = parent_generation.unwrap_or(new_head);
        let marker = TrackedHeadMarker {
            head_commit_id: new_head,
            generation,
            working_diff_checkpoint_commit_id: working_diff_marker_checkpoint_commit_id,
        };
        // Preflight the only fallible publication bytes before staging an
        // incremental generation in the caller's write set. A failed writer
        // must not leave any same-generation group mutation behind.
        let marker_key = marker_key(branch_id)?;
        let marker_value = storage_codec::encode("tracked-head marker", &marker)?;

        // Sorting borrowed deltas establishes both the exact-mutation
        // uniqueness check and the group order needed for a streaming merge.
        // The normal matching-generation path must never reconstruct every
        // v6 member in an owned BTreeMap just to change one member.
        let mut sorted_deltas = deltas.iter().collect::<Vec<_>>();
        sorted_deltas.sort_unstable_by(|left, right| compare_head_deltas(left, right));
        for pair in sorted_deltas.windows(2) {
            if compare_head_deltas(pair[0], pair[1]) == Ordering::Equal {
                return Err(tracked_head_duplicate_delta_error(pair[1]));
            }
        }

        if matches_parent {
            let groups = group_sorted_head_deltas(branch_id, generation, &sorted_deltas);
            let previous_groups =
                load_group_bytes(self.store, groups.iter().map(|group| &group.identity)).await?;

            self.writes
                .reserve_space(TRACKED_HEAD_GROUP_SPACE, groups.len(), 0);
            self.writes.reserve_space(
                TRACKED_HEAD_MEMBER_SPACE,
                sorted_deltas
                    .iter()
                    .filter(|delta| delta.file_id.is_some())
                    .count(),
                0,
            );
            let mut next_groups = Vec::new();
            next_groups
                .try_reserve(groups.len())
                .map_err(|_| head_group_error("cannot reserve staged group outputs"))?;
            for (group, previous) in groups.iter().zip(previous_groups) {
                next_groups.push(encode_incremental_group(
                    previous.as_deref(),
                    &sorted_deltas[group.deltas.clone()],
                    absence_guards,
                    working_diff_capture_checkpoint_commit_id,
                )?);
            }
            for (group, next) in groups.iter().zip(next_groups) {
                let became_dirty_group = next.became_dirty_group;
                stage_put_group_bytes(self.writes, &group.identity, next.bytes);
                for (file_id, value) in next.explicit_member_projections {
                    stage_put_file_member_bytes(self.writes, &group.identity, file_id, &value);
                }
                if became_dirty_group {
                    stage_put_working_diff_group_index(
                        self.writes,
                        working_diff_coverage,
                        working_diff_capture_checkpoint_commit_id.expect(
                            "a newly dirty group requires an active working-diff checkpoint",
                        ),
                        &group.identity,
                    )?;
                }
            }
        } else {
            stage_bootstrap_groups(
                self.writes,
                branch_id,
                generation,
                &sorted_deltas,
                absence_guards,
                parent_rows.unwrap_or_default(),
                working_diff_capture_checkpoint_commit_id,
                working_diff_coverage,
            )?;
        }
        stage_marker_encoded(self.writes, marker_key, marker_value);
        Ok(generation)
    }
}

/// One contiguous range of sorted mutations for a physical v6 group.
///
/// The range borrows the transaction's deltas, while the group identity is
/// owned once. This is intentionally not a map of decoded members: matching
/// generation commits merge the encoded old group directly into its next
/// value.
struct HeadGroupMutation {
    identity: HeadGroupIdentity,
    deltas: Range<usize>,
}

/// Fully validated replacement bytes for one matching-generation group.
///
/// Keeping these short-lived outputs until every group has validated makes
/// `stage_commit` all-or-nothing for the existing serving generation without
/// rebuilding decoded member maps.
struct EncodedHeadGroup<'a> {
    bytes: Vec<u8>,
    explicit_member_projections: Vec<(&'a str, Vec<u8>)>,
    became_dirty_group: bool,
}

fn compare_head_deltas(
    left: &TrackedHeadDeltaRef<'_>,
    right: &TrackedHeadDeltaRef<'_>,
) -> Ordering {
    left.schema_key
        .cmp(right.schema_key)
        .then_with(|| left.entity_pk.cmp(right.entity_pk))
        .then_with(|| left.file_id.cmp(&right.file_id))
}

fn same_head_group(left: &TrackedHeadDeltaRef<'_>, right: &TrackedHeadDeltaRef<'_>) -> bool {
    left.schema_key == right.schema_key && left.entity_pk == right.entity_pk
}

fn tracked_head_duplicate_delta_error(delta: &TrackedHeadDeltaRef<'_>) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "tracked-head commit contains duplicate mutation for schema '{}' entity_pk '{:?}' file_id '{:?}'",
            delta.schema_key, delta.entity_pk, delta.file_id
        ),
    )
}

fn group_sorted_head_deltas(
    branch_id: &str,
    generation: CommitId,
    deltas: &[&TrackedHeadDeltaRef<'_>],
) -> Vec<HeadGroupMutation> {
    let mut groups = Vec::new();
    let mut start = 0;
    while start < deltas.len() {
        let first = deltas[start];
        let mut end = start + 1;
        while end < deltas.len() && same_head_group(first, deltas[end]) {
            end += 1;
        }
        groups.push(HeadGroupMutation {
            identity: HeadGroupIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: first.schema_key.to_string(),
                entity_pk: first.entity_pk.clone(),
            },
            deltas: start..end,
        });
        start = end;
    }
    groups
}

/// Builds a fresh v6 generation after a branch movement.
///
/// This path necessarily starts from materialized parent rows, so it retains
/// the simple owned-map implementation. Ordinary commits keep their parent
/// generation and use [`encode_incremental_group`] instead.
fn stage_bootstrap_groups(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
    deltas: &[&TrackedHeadDeltaRef<'_>],
    absence_guards: &BTreeSet<TrackedStateKey>,
    parent_rows: Vec<MaterializedTrackedStateRow>,
    working_diff_checkpoint_commit_id: Option<CommitId>,
    working_diff_coverage: &mut WorkingDiffIndexCoverage,
) -> Result<(), LixError> {
    let mut groups = BTreeMap::<HeadGroupIdentity, BTreeMap<Option<String>, Vec<u8>>>::new();
    let mut baselines =
        BTreeMap::<HeadGroupIdentity, BTreeMap<Option<String>, WorkingDiffBaseline>>::new();
    for row in parent_rows {
        let MaterializedTrackedStateRow {
            entity_pk,
            schema_key,
            file_id,
            snapshot_content,
            metadata,
            deleted,
            created_at,
            updated_at,
            change_id,
            commit_id,
        } = row;
        let key = TrackedStateKey {
            schema_key: schema_key.clone(),
            entity_pk: entity_pk.clone(),
            file_id: file_id.clone(),
        };
        if absence_guards.contains(&key) && !deleted {
            return Err(tracked_head_duplicate_insert_error(&key));
        }
        let identity = HeadGroupIdentity {
            branch_id: branch_id.to_string(),
            generation,
            schema_key,
            entity_pk,
        };
        let value = HeadValue {
            change_id,
            commit_id,
            deleted,
            created_at: LixTimestamp::expect_parse("tracked-head parent created_at", &created_at),
            updated_at: LixTimestamp::expect_parse("tracked-head parent updated_at", &updated_at),
            snapshot: snapshot_content
                .as_deref()
                .map_or(JsonSlot::None, JsonSlot::from_json),
            metadata: metadata
                .as_deref()
                .map_or(JsonSlot::None, JsonSlot::from_json),
        };
        let members = groups.entry(identity.clone()).or_default();
        if members
            .insert(file_id.clone(), encode_head_value(&value.as_ref())?)
            .is_some()
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked-head bootstrap contains duplicate full row identity",
            ));
        }
        baselines
            .entry(identity)
            .or_default()
            .insert(file_id, WorkingDiffBaseline::Clean);
    }

    let mut dirty_groups = BTreeSet::new();
    for delta in deltas {
        let group = HeadGroupIdentity {
            branch_id: branch_id.to_string(),
            generation,
            schema_key: delta.schema_key.to_string(),
            entity_pk: delta.entity_pk.clone(),
        };
        let file_id = delta.file_id.map(str::to_string);
        let members = groups.entry(group.clone()).or_default();
        let baseline_members = baselines.entry(group.clone()).or_default();
        let baseline = match members.get(&file_id) {
            Some(existing) => {
                let existing = decode_head_value(existing)?;
                reject_guarded_live_member(absence_guards, delta, existing)?;
                working_diff_baseline_for_existing_value(
                    existing,
                    working_diff_checkpoint_commit_id,
                )
            }
            None => working_diff_baseline_for_absent_delta(working_diff_checkpoint_commit_id),
        };
        let created_at = match members.get(&file_id) {
            Some(existing) => decode_head_value(existing)?.created_at,
            None => delta.created_at,
        };
        members.insert(
            file_id.clone(),
            encode_head_value(&delta.value_ref(created_at))?,
        );
        baseline_members.insert(file_id.clone(), baseline);
        if working_diff_checkpoint_commit_id.is_some() {
            dirty_groups.insert(group.clone());
        }
    }

    writes.reserve_space(TRACKED_HEAD_GROUP_SPACE, groups.len(), 0);
    let explicit_member_count = groups
        .values()
        .map(|members| members.keys().filter(|file_id| file_id.is_some()).count())
        .sum();
    writes.reserve_space(TRACKED_HEAD_MEMBER_SPACE, explicit_member_count, 0);
    for (identity, members) in groups {
        let member_baselines = baselines
            .get(&identity)
            .ok_or_else(|| head_group_error("bootstrap group is missing working-diff baselines"))?;
        stage_put_group_members_with_baselines(writes, &identity, &members, member_baselines)?;
        for (file_id, value) in &members {
            if file_id.is_some() {
                stage_put_member_bytes(writes, &identity, file_id.as_deref(), value)?;
            }
        }
    }
    if let Some(checkpoint_commit_id) = working_diff_checkpoint_commit_id {
        for group in &dirty_groups {
            stage_put_working_diff_group_index(
                writes,
                working_diff_coverage,
                checkpoint_commit_id,
                group,
            )?;
        }
    }
    Ok(())
}

/// Merges one existing v6 group and its sorted mutations without decoding the
/// whole group into owned member values. Every old member is still parsed and
/// validated before it is copied, so an unrelated malformed sibling cannot be
/// silently republished.
fn encode_incremental_group<'a>(
    previous: Option<&[u8]>,
    deltas: &[&TrackedHeadDeltaRef<'a>],
    absence_guards: &BTreeSet<TrackedStateKey>,
    working_diff_checkpoint_commit_id: Option<CommitId>,
) -> Result<EncodedHeadGroup<'a>, LixError> {
    debug_assert!(!deltas.is_empty());
    debug_assert!(
        deltas
            .windows(2)
            .all(|pair| compare_head_deltas(pair[0], pair[1]) == Ordering::Less)
    );

    let mut cursor = previous.map(HeadGroupMembers::new).transpose()?;
    let mut current = next_head_group_member(&mut cursor)?;
    let mut encoded = Vec::new();
    encoded
        .try_reserve(previous.map_or(HEAD_GROUP_HEADER_BYTES, <[u8]>::len))
        .map_err(|_| head_group_error("cannot reserve group value bytes"))?;
    encoded.push(HEAD_GROUP_VALUE_VERSION);
    encoded.extend_from_slice(&[0; 4]);
    let mut member_count = 0usize;
    let mut previous_has_current_dirty_member = false;
    let mut became_current_dirty_member = false;
    let mut explicit_member_projections = Vec::new();
    explicit_member_projections
        .try_reserve(
            deltas
                .iter()
                .filter(|delta| delta.file_id.is_some())
                .count(),
        )
        .map_err(|_| head_group_error("cannot reserve explicit member projections"))?;

    for delta in deltas {
        loop {
            let Some(member) = current else {
                append_delta_head_group_member(
                    &mut encoded,
                    delta,
                    delta.created_at,
                    working_diff_baseline_for_absent_delta(working_diff_checkpoint_commit_id),
                    &mut explicit_member_projections,
                )?;
                became_current_dirty_member |= working_diff_checkpoint_commit_id.is_some();
                increment_head_group_member_count(&mut member_count)?;
                break;
            };
            match member.file_id.cmp(&delta.file_id) {
                Ordering::Less => {
                    previous_has_current_dirty_member |= member
                        .baseline
                        .is_for_checkpoint(working_diff_checkpoint_commit_id);
                    append_head_group_member(
                        &mut encoded,
                        member.file_id,
                        member.value,
                        member.baseline,
                    )?;
                    increment_head_group_member_count(&mut member_count)?;
                    current = next_head_group_member(&mut cursor)?;
                }
                Ordering::Equal => {
                    reject_guarded_live_member(absence_guards, delta, member.head)?;
                    let (baseline, became_dirty) = working_diff_baseline_for_existing_delta(
                        member.baseline,
                        member.head,
                        working_diff_checkpoint_commit_id,
                    );
                    previous_has_current_dirty_member |= member
                        .baseline
                        .is_for_checkpoint(working_diff_checkpoint_commit_id);
                    append_delta_head_group_member(
                        &mut encoded,
                        delta,
                        member.head.created_at,
                        baseline,
                        &mut explicit_member_projections,
                    )?;
                    if became_dirty {
                        became_current_dirty_member = true;
                    }
                    increment_head_group_member_count(&mut member_count)?;
                    current = next_head_group_member(&mut cursor)?;
                    break;
                }
                Ordering::Greater => {
                    append_delta_head_group_member(
                        &mut encoded,
                        delta,
                        delta.created_at,
                        working_diff_baseline_for_absent_delta(working_diff_checkpoint_commit_id),
                        &mut explicit_member_projections,
                    )?;
                    became_current_dirty_member |= working_diff_checkpoint_commit_id.is_some();
                    increment_head_group_member_count(&mut member_count)?;
                    break;
                }
            }
        }
    }
    while let Some(member) = current {
        previous_has_current_dirty_member |= member
            .baseline
            .is_for_checkpoint(working_diff_checkpoint_commit_id);
        append_head_group_member(&mut encoded, member.file_id, member.value, member.baseline)?;
        increment_head_group_member_count(&mut member_count)?;
        current = next_head_group_member(&mut cursor)?;
    }

    let member_count =
        u32::try_from(member_count).map_err(|_| head_group_error("member count exceeds u32"))?;
    encoded[1..HEAD_GROUP_HEADER_BYTES].copy_from_slice(&member_count.to_be_bytes());
    Ok(EncodedHeadGroup {
        bytes: encoded,
        explicit_member_projections,
        became_dirty_group: working_diff_checkpoint_commit_id.is_some()
            && !previous_has_current_dirty_member
            && became_current_dirty_member,
    })
}

fn working_diff_baseline_for_absent_delta(
    checkpoint_commit_id: Option<CommitId>,
) -> WorkingDiffBaseline {
    checkpoint_commit_id.map_or(WorkingDiffBaseline::Clean, WorkingDiffBaseline::Absent)
}

fn working_diff_baseline_for_existing_value(
    prior_value: HeadValueView<'_>,
    checkpoint_commit_id: Option<CommitId>,
) -> WorkingDiffBaseline {
    checkpoint_commit_id.map_or(WorkingDiffBaseline::Clean, |checkpoint_commit_id| {
        WorkingDiffBaseline::Present {
            checkpoint_commit_id,
            version: prior_value.working_diff_version(),
        }
    })
}

fn working_diff_baseline_for_existing_delta(
    prior: WorkingDiffBaseline,
    prior_value: HeadValueView<'_>,
    checkpoint_commit_id: Option<CommitId>,
) -> (WorkingDiffBaseline, bool) {
    match checkpoint_commit_id {
        Some(checkpoint_commit_id) if !prior.is_for_checkpoint(Some(checkpoint_commit_id)) => (
            WorkingDiffBaseline::Present {
                checkpoint_commit_id,
                version: prior_value.working_diff_version(),
            },
            true,
        ),
        _ => (prior, false),
    }
}

fn increment_head_group_member_count(member_count: &mut usize) -> Result<(), LixError> {
    *member_count = member_count
        .checked_add(1)
        .ok_or_else(|| head_group_error("member count overflow"))?;
    Ok(())
}

fn next_head_group_member<'a>(
    cursor: &mut Option<HeadGroupMembers<'a>>,
) -> Result<Option<HeadGroupMemberView<'a>>, LixError> {
    cursor
        .as_mut()
        .map_or(Ok(None), HeadGroupMembers::next_member)
}

fn reject_guarded_live_member(
    absence_guards: &BTreeSet<TrackedStateKey>,
    delta: &TrackedHeadDeltaRef<'_>,
    existing: HeadValueView<'_>,
) -> Result<(), LixError> {
    if absence_guards.is_empty() || existing.deleted {
        return Ok(());
    }
    let key = TrackedStateKey {
        schema_key: delta.schema_key.to_string(),
        entity_pk: delta.entity_pk.clone(),
        file_id: delta.file_id.map(str::to_string),
    };
    if absence_guards.contains(&key) {
        return Err(tracked_head_duplicate_insert_error(&key));
    }
    Ok(())
}

fn append_delta_head_group_member<'a>(
    encoded: &mut Vec<u8>,
    delta: &TrackedHeadDeltaRef<'a>,
    created_at: LixTimestamp,
    baseline: WorkingDiffBaseline,
    explicit_member_projections: &mut Vec<(&'a str, Vec<u8>)>,
) -> Result<(), LixError> {
    let value = encode_head_value(&delta.value_ref(created_at))?;
    append_head_group_member(encoded, delta.file_id, &value, baseline)?;
    // The group is authoritative. An unchanged explicit projection already
    // belongs to this generation, so only rewrite projections whose member
    // changed in this commit.
    if let Some(file_id) = delta.file_id {
        explicit_member_projections.push((file_id, value));
    }
    Ok(())
}

fn tracked_head_duplicate_insert_error(key: &TrackedStateKey) -> LixError {
    let entity_pk = key
        .entity_pk
        .as_json_array_text()
        .unwrap_or_else(|_| "<invalid entity_pk>".to_string());
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "primary-key constraint violation on schema '{}': INSERT would duplicate entity_pk '{entity_pk}'",
            key.schema_key
        ),
    )
}

async fn load_marker(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
) -> Result<Option<TrackedHeadMarker>, LixError> {
    let key = marker_key(branch_id)?;
    let result = PointReadPlan::new(TRACKED_HEAD_MARKER_SPACE, &[StorageKey(Bytes::from(key))])
        .materialize(store, StorageGetOptions::default())
        .await?;
    result
        .value
        .into_iter()
        .next()
        .flatten()
        .map(decode_marker_value)
        .transpose()
}

/// Loads packed v6 groups without materializing their members.
async fn load_group_bytes<'a>(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: impl IntoIterator<Item = &'a HeadGroupIdentity>,
) -> Result<Vec<Option<Bytes>>, LixError> {
    let keys = identities
        .into_iter()
        .map(|identity| StorageKey(Bytes::from(encode_group_key(identity))))
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let result = PointReadPlan::new(TRACKED_HEAD_GROUP_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    result
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

/// Loads the explicit-file member projection. Its physical access pattern is
/// intentionally the same single-key point lookup as the prior member layout so file-id queries
/// do not become proportional to the size of their logical PK group.
async fn load_member_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &[HeadIdentity],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    debug_assert!(identities.iter().all(|identity| identity.file_id.is_some()));
    let keys = identities
        .iter()
        .map(|identity| StorageKey(Bytes::from(encode_member_key(identity))))
        .collect::<Vec<_>>();
    let result = PointReadPlan::new(TRACKED_HEAD_MEMBER_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    result
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

async fn scan_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
    limit: Option<usize>,
) -> Result<Vec<(HeadRowIdentity, Bytes)>, LixError> {
    if let Some(identities) = exact_explicit_member_identities(branch_id, generation, filter) {
        let values = load_member_bytes(store, &identities).await?;
        return Ok(identities
            .into_iter()
            .zip(values)
            .filter_map(|(identity, value)| {
                value.map(|value| (identity.into_row_identity(), value))
            })
            .take(limit.unwrap_or(usize::MAX))
            .collect());
    }
    if let Some(prefixes) = explicit_member_scan_prefixes(branch_id, generation, filter) {
        let mut rows = scan_explicit_member_entries(store, prefixes, filter).await?;
        // Member projection keys are ordered by `file_id` before `entity_pk`.
        // Restore the public logical order when callers request multiple file
        // ids; the group route below is already ordered that way.
        rows.sort_by(|(left, _), (right, _)| left.cmp(right));
        rows.dedup_by(|(left, _), (right, _)| left == right);
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
        return Ok(rows);
    }
    if let Some(groups) = exact_group_identities(branch_id, generation, filter) {
        let values = load_group_bytes(store, &groups).await?;
        let mut rows = Vec::new();
        for (group, value) in groups.into_iter().zip(values) {
            let Some(value) = value else {
                continue;
            };
            extend_group_entries(&mut rows, group, value, filter, limit)?;
            if limit.is_some_and(|limit| rows.len() >= limit) {
                return Ok(rows);
            }
        }
        return Ok(rows);
    }

    let scope = encode_scope_prefix(branch_id, generation);
    let mut prefixes = scan_prefixes(&scope, filter);
    prefixes.sort();
    prefixes.dedup();
    let mut rows = Vec::new();
    for prefix in prefixes {
        let plan = ScanPlan::prefix(
            TRACKED_HEAD_GROUP_SPACE,
            StoragePrefix {
                bytes: Bytes::from(prefix),
            },
        );
        let mut resume_after = None;
        loop {
            let remaining = limit.map(|limit| limit.saturating_sub(rows.len()));
            if matches!(remaining, Some(0)) {
                return Ok(rows);
            }
            let page = plan
                .collect(
                    store,
                    StorageScanOptions {
                        resume_after: resume_after.clone(),
                        limit_rows: remaining
                            .unwrap_or_else(|| StorageScanOptions::default().limit_rows),
                        ..StorageScanOptions::default()
                    },
                )
                .await?;
            resume_after = page.value.entries.last().map(|entry| entry.key.clone());
            for entry in page.value.entries {
                let identity = decode_group_key_in_scope(entry.key.0.as_ref(), &scope)?;
                extend_group_entries(
                    &mut rows,
                    HeadGroupIdentity {
                        branch_id: branch_id.to_string(),
                        generation,
                        schema_key: identity.schema_key,
                        entity_pk: identity.entity_pk,
                    },
                    full_value_bytes(entry.value)?,
                    filter,
                    limit,
                )?;
                if limit.is_some_and(|limit| rows.len() >= limit) {
                    return Ok(rows);
                }
            }
            if !page.value.has_more || resume_after.is_none() {
                break;
            }
        }
    }
    Ok(rows)
}

/// Reads the checkpoint's sparse dirty-group index and resolves before images
/// from authoritative v6 group values. The index is auxiliary: its complete
/// key coverage is verified before any result is returned, so a bad record is
/// a fallback (`None`), never an empty diff.
async fn working_diff_entries_from_current_head(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    checkpoint_commit_id: CommitId,
    generation: CommitId,
    expected_coverage: WorkingDiffIndexCoverage,
    filter: &TrackedStateFilter,
) -> Result<Option<Vec<TrackedStateDiffEntry>>, LixError> {
    let (groups, require_current_dirty_member) =
        match exact_group_identities(branch_id, generation, filter) {
            Some(groups) => (groups, false),
            None => {
                let Some(groups) = scan_working_diff_group_index(
                    store,
                    branch_id,
                    checkpoint_commit_id,
                    generation,
                    expected_coverage,
                    filter,
                )
                .await?
                else {
                    return Ok(None);
                };
                (groups, true)
            }
        };

    let values = load_group_bytes(store, &groups).await?;
    let mut dirty_rows = Vec::new();
    for (group, value) in groups.into_iter().zip(values) {
        let Some(value) = value else {
            return Ok(None);
        };
        let Ok(members) = decode_head_group_members(&value) else {
            return Ok(None);
        };
        let mut has_current_dirty_member = false;
        for member in members {
            if !member
                .baseline
                .is_for_checkpoint(Some(checkpoint_commit_id))
            {
                continue;
            }
            has_current_dirty_member = true;
            let identity = HeadRowIdentity {
                schema_key: group.schema_key.clone(),
                entity_pk: group.entity_pk.clone(),
                file_id: member.file_id,
            };
            if matches_filter(&identity, filter) {
                let after = match decode_head_value(&member.value) {
                    Ok(value) => value.working_diff_version(),
                    Err(_) => return Ok(None),
                };
                dirty_rows.push((identity, member.baseline, after));
            }
        }
        if require_current_dirty_member && !has_current_dirty_member {
            return Ok(None);
        }
    }

    Ok(Some(
        dirty_rows
            .into_iter()
            .filter_map(|(identity, baseline, after)| {
                classify_working_diff_entry(identity, baseline, after)
            })
            .collect(),
    ))
}

async fn scan_working_diff_group_index(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    checkpoint_commit_id: CommitId,
    generation: CommitId,
    expected_coverage: WorkingDiffIndexCoverage,
    filter: &TrackedStateFilter,
) -> Result<Option<Vec<HeadGroupIdentity>>, LixError> {
    let scope = encode_working_diff_scope_prefix(branch_id, checkpoint_commit_id, generation);
    let plan = ScanPlan::prefix(
        TRACKED_WORKING_DIFF_GROUP_SPACE,
        StoragePrefix {
            bytes: Bytes::from(scope.clone()),
        },
    );
    let mut actual_coverage = WorkingDiffIndexCoverage::default();
    let mut found = Vec::new();
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            if !is_working_diff_index_value(&entry.value)
                || actual_coverage
                    .add_encoded_group_key(entry.key.0.as_ref())
                    .is_none()
            {
                return Ok(None);
            }
            let Ok(identity) = decode_working_diff_group_key_in_scope(entry.key.0.as_ref(), &scope)
            else {
                return Ok(None);
            };
            if matches_group_filter(&identity, filter) {
                found.push(HeadGroupIdentity {
                    branch_id: branch_id.to_string(),
                    generation,
                    schema_key: identity.schema_key,
                    entity_pk: identity.entity_pk,
                });
            }
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    if actual_coverage != expected_coverage {
        return Ok(None);
    }
    found.sort();
    found.dedup();
    Ok(Some(found))
}

fn is_working_diff_index_value(value: &StorageProjectedValue) -> bool {
    matches!(value, StorageProjectedValue::FullValue(bytes) if bytes.as_ref() == WORKING_DIFF_INDEX_VALUE)
}

fn classify_working_diff_entry(
    identity: HeadRowIdentity,
    baseline: WorkingDiffBaseline,
    after_version: WorkingDiffVersion,
) -> Option<TrackedStateDiffEntry> {
    let before_version = match baseline {
        WorkingDiffBaseline::Clean => return None,
        WorkingDiffBaseline::Absent(_) => None,
        WorkingDiffBaseline::Present { version, .. } => Some(version),
    };
    let before = before_version.map(|version| version.into_diff_row(&identity));
    let after = after_version.into_diff_row(&identity);
    let identity = TrackedStateDiffIdentity {
        schema_key: identity.schema_key,
        entity_pk: identity.entity_pk,
        file_id: identity.file_id,
    };
    match (
        before.as_ref().filter(|row| !row.deleted),
        (!after.deleted).then_some(&after),
    ) {
        (None, None) => None,
        (None, Some(_)) => Some(TrackedStateDiffEntry {
            identity,
            kind: TrackedStateDiffKind::Added,
            before,
            after: Some(after),
        }),
        (Some(_), None) => Some(TrackedStateDiffEntry {
            identity,
            kind: TrackedStateDiffKind::Removed,
            before,
            after: Some(after),
        }),
        (Some(_), Some(_))
            if before_version.is_some_and(|version| version.payload_eq(after_version)) =>
        {
            None
        }
        (Some(_), Some(_)) => Some(TrackedStateDiffEntry {
            identity,
            kind: TrackedStateDiffKind::Modified,
            before,
            after: Some(after),
        }),
    }
}

fn matches_group_filter(identity: &HeadRowIdentity, filter: &TrackedStateFilter) -> bool {
    (filter.schema_keys.is_empty() || filter.schema_keys.contains(&identity.schema_key))
        && (filter.entity_pks.is_empty() || filter.entity_pks.contains(&identity.entity_pk))
}

fn exact_group_identities(
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Option<Vec<HeadGroupIdentity>> {
    if filter.schema_keys.is_empty() || filter.entity_pks.is_empty() {
        return None;
    }
    let mut identities = Vec::with_capacity(filter.schema_keys.len() * filter.entity_pks.len());
    for schema_key in &filter.schema_keys {
        for entity_pk in &filter.entity_pks {
            identities.push(HeadGroupIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: schema_key.clone(),
                entity_pk: entity_pk.clone(),
            });
        }
    }
    identities.sort();
    identities.dedup();
    Some(identities)
}

fn exact_explicit_member_identities(
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Option<Vec<HeadIdentity>> {
    exact_group_identities(branch_id, generation, filter)?;
    if filter.file_ids.is_empty()
        || filter
            .file_ids
            .iter()
            .any(|file_id| !matches!(file_id, NullableKeyFilter::Value(_)))
    {
        return None;
    }
    let mut identities = Vec::with_capacity(
        filter.schema_keys.len() * filter.entity_pks.len() * filter.file_ids.len(),
    );
    for schema_key in &filter.schema_keys {
        for entity_pk in &filter.entity_pks {
            for file_id in &filter.file_ids {
                let NullableKeyFilter::Value(file_id) = file_id else {
                    unreachable!("explicit member filter checked above");
                };
                identities.push(HeadIdentity {
                    branch_id: branch_id.to_string(),
                    generation,
                    schema_key: schema_key.clone(),
                    entity_pk: entity_pk.clone(),
                    file_id: Some(file_id.clone()),
                });
            }
        }
    }
    identities.sort();
    identities.dedup();
    Some(identities)
}

/// A schema-scoped `file_id = ?` lookup cannot use the grouped primary
/// serving record without decoding every logical PK in that schema. Explicit
/// member rows use a file-id-first suffix solely for this access pattern.
///
/// Exact `(schema, entity_pk, file_id)` requests take the point-read route
/// above. If the schema is unknown, no useful member-space prefix exists and
/// we correctly retain the general group scan fallback.
fn explicit_member_scan_prefixes(
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Option<Vec<Vec<u8>>> {
    if filter.schema_keys.is_empty()
        || filter.file_ids.is_empty()
        || !filter.entity_pks.is_empty()
        || filter
            .file_ids
            .iter()
            .any(|file_id| !matches!(file_id, NullableKeyFilter::Value(_)))
    {
        return None;
    }
    let mut prefixes = Vec::with_capacity(filter.schema_keys.len() * filter.file_ids.len());
    for schema_key in &filter.schema_keys {
        for file_id in &filter.file_ids {
            let NullableKeyFilter::Value(file_id) = file_id else {
                unreachable!("explicit member scan filter checked above");
            };
            let mut prefix = encode_scope_prefix(branch_id, generation);
            write_key_string(&mut prefix, schema_key, KEY_PART_FINAL);
            write_file_id(&mut prefix, Some(file_id));
            prefixes.push(prefix);
        }
    }
    prefixes.sort();
    prefixes.dedup();
    Some(prefixes)
}

async fn scan_explicit_member_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    prefixes: Vec<Vec<u8>>,
    filter: &TrackedStateFilter,
) -> Result<Vec<(HeadRowIdentity, Bytes)>, LixError> {
    let mut rows = Vec::new();
    for prefix in prefixes {
        let plan = ScanPlan::prefix(
            TRACKED_HEAD_MEMBER_SPACE,
            StoragePrefix {
                bytes: Bytes::from(prefix),
            },
        );
        let mut resume_after = None;
        loop {
            let page = plan
                .collect(
                    store,
                    StorageScanOptions {
                        resume_after: resume_after.clone(),
                        ..StorageScanOptions::default()
                    },
                )
                .await?;
            resume_after = page.value.entries.last().map(|entry| entry.key.clone());
            for entry in page.value.entries {
                let identity = decode_member_key(entry.key.0.as_ref())?.into_row_identity();
                if matches_filter(&identity, filter) {
                    rows.push((identity, full_value_bytes(entry.value)?));
                }
            }
            if !page.value.has_more || resume_after.is_none() {
                break;
            }
        }
    }
    Ok(rows)
}

fn scan_prefixes(scope: &[u8], filter: &TrackedStateFilter) -> Vec<Vec<u8>> {
    if filter.schema_keys.is_empty() {
        return vec![scope.to_vec()];
    }
    let mut prefixes = Vec::new();
    for schema_key in &filter.schema_keys {
        let mut schema_prefix = scope.to_vec();
        write_key_string(&mut schema_prefix, schema_key, KEY_PART_FINAL);
        if filter.entity_pks.is_empty() {
            prefixes.push(schema_prefix);
            continue;
        }
        for entity_pk in &filter.entity_pks {
            let mut entity_prefix = schema_prefix.clone();
            write_entity_pk(&mut entity_prefix, entity_pk);
            prefixes.push(entity_prefix);
        }
    }
    prefixes
}

fn extend_group_entries(
    rows: &mut Vec<(HeadRowIdentity, Bytes)>,
    group: HeadGroupIdentity,
    value: Bytes,
    filter: &TrackedStateFilter,
    limit: Option<usize>,
) -> Result<(), LixError> {
    for member in decode_head_group_members(&value)? {
        let identity = HeadRowIdentity {
            schema_key: group.schema_key.clone(),
            entity_pk: group.entity_pk.clone(),
            file_id: member.file_id,
        };
        if matches_filter(&identity, filter) {
            rows.push((identity, member.value));
            if limit.is_some_and(|limit| rows.len() >= limit) {
                break;
            }
        }
    }
    Ok(())
}

fn matches_filter(identity: &HeadRowIdentity, filter: &TrackedStateFilter) -> bool {
    (filter.schema_keys.is_empty() || filter.schema_keys.contains(&identity.schema_key))
        && (filter.entity_pks.is_empty() || filter.entity_pks.contains(&identity.entity_pk))
        && (filter.file_ids.is_empty()
            || filter.file_ids.iter().any(|filter| match filter {
                NullableKeyFilter::Any => true,
                NullableKeyFilter::Null => identity.file_id.is_none(),
                NullableKeyFilter::Value(value) => identity.file_id.as_ref() == Some(value),
            }))
}

#[cfg(test)]
fn stage_marker(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    marker: &TrackedHeadMarker,
) -> Result<(), LixError> {
    stage_marker_encoded(
        writes,
        marker_key(branch_id)?,
        storage_codec::encode("tracked-head marker", marker)?,
    );
    Ok(())
}

fn stage_marker_encoded(writes: &mut StorageWriteSet, key: Vec<u8>, value: Vec<u8>) {
    writes.put(
        TRACKED_HEAD_MARKER_SPACE,
        StorageKey(Bytes::from(key)),
        StorageValue {
            bytes: Bytes::from(value),
        },
    );
}

/// Publishes the checkpoint epoch that owns the sparse working-diff indexes.
/// The surrounding branch-control CAS makes this marker, the v6 head groups,
/// and the current branch head one atomic visibility boundary.
pub(crate) fn stage_tracked_working_diff_epoch(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    epoch: TrackedWorkingDiffEpoch,
) -> Result<(), LixError> {
    writes.put(
        TRACKED_WORKING_DIFF_MARKER_SPACE,
        StorageKey(Bytes::from(marker_key(branch_id)?)),
        StorageValue {
            bytes: Bytes::from(storage_codec::encode(
                "tracked working-diff marker",
                &epoch,
            )?),
        },
    );
    Ok(())
}

async fn load_tracked_working_diff_epoch(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
) -> Result<Option<TrackedWorkingDiffEpoch>, LixError> {
    let result = PointReadPlan::new(
        TRACKED_WORKING_DIFF_MARKER_SPACE,
        &[StorageKey(Bytes::from(marker_key(branch_id)?))],
    )
    .materialize(store, StorageGetOptions::default())
    .await?;
    result
        .value
        .into_iter()
        .next()
        .flatten()
        .map(|value| {
            let StorageProjectedValue::FullValue(bytes) = value else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked working-diff marker read unexpectedly omitted its value",
                ));
            };
            storage_codec::decode("tracked working-diff marker", &bytes)
        })
        .transpose()
}

/// Reclaims sparse dirty-index records outside every currently published
/// checkpoint epoch. This deliberately runs only from repository GC: a
/// checkpoint reset is O(1), while old index prefixes are unreachable as soon
/// as its marker commits.
pub(crate) async fn stage_collect_stale_working_diff_indexes<S>(
    store: &S,
    writes: &mut StorageWriteSet,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone,
{
    let controls = BranchHeadControlContext::new()
        .reader(store.clone())
        .scan()
        .await?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let active = stage_active_working_diff_scopes(store, writes, &controls).await?;
    stage_collect_stale_working_diff_space(
        store,
        writes,
        TRACKED_WORKING_DIFF_GROUP_SPACE,
        |entry| {
            is_working_diff_index_value(&entry.value)
                && decode_working_diff_group_key(entry.key.0.as_ref()).is_ok_and(
                    |(checkpoint_commit_id, identity)| {
                        active.get(&identity.branch_id).is_some_and(|scope| {
                            scope.checkpoint_commit_id == checkpoint_commit_id
                                && scope.generation == identity.generation
                        })
                    },
                )
        },
    )
    .await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ActiveWorkingDiffScope {
    checkpoint_commit_id: CommitId,
    generation: CommitId,
}

/// Validates and keeps only auxiliary epochs that are presently bound by the
/// authoritative branch control and tracked-head marker. Broken auxiliary
/// bytes are reclaimed here rather than turning background GC into a retry
/// loop; normal readers already select canonical replay for the same cases.
async fn stage_active_working_diff_scopes<S>(
    store: &S,
    writes: &mut StorageWriteSet,
    controls: &BTreeMap<String, BranchHeadControl>,
) -> Result<BTreeMap<String, ActiveWorkingDiffScope>, LixError>
where
    S: StorageAdapterRead + Clone,
{
    let plan = ScanPlan::prefix(
        TRACKED_WORKING_DIFF_MARKER_SPACE,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut active = BTreeMap::new();
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            let key: BranchRefKey = match storage_codec::decode(
                "tracked working-diff marker key",
                entry.key.0.as_ref(),
            ) {
                Ok(key) => key,
                Err(_) => {
                    writes.delete(TRACKED_WORKING_DIFF_MARKER_SPACE, entry.key);
                    continue;
                }
            };
            let StorageProjectedValue::FullValue(bytes) = entry.value else {
                writes.delete(TRACKED_WORKING_DIFF_MARKER_SPACE, entry.key);
                continue;
            };
            let epoch: TrackedWorkingDiffEpoch =
                match storage_codec::decode("tracked working-diff marker", &bytes) {
                    Ok(epoch) => epoch,
                    Err(_) => {
                        writes.delete(TRACKED_WORKING_DIFF_MARKER_SPACE, entry.key);
                        continue;
                    }
                };
            let Some(control) = controls.get(&key.branch_id).copied() else {
                writes.delete(TRACKED_WORKING_DIFF_MARKER_SPACE, entry.key);
                continue;
            };
            let valid = match epoch.generation {
                // A checkpoint reset is known empty and deliberately leaves
                // the prior serving group generation intact until its first
                // ordinary child captures a new baseline.
                None => epoch.checkpoint_commit_id == control.head_commit_id,
                Some(generation) if generation == control.generation => {
                    matches!(
                        TrackedHeadContext::new()
                            .reader(store.clone())
                            .marker_info_if_control_current(&key.branch_id, control)
                            .await,
                        Ok(Some(marker))
                            if marker.working_diff_checkpoint_commit_id
                                == Some(epoch.checkpoint_commit_id)
                    )
                }
                Some(_) => false,
            };
            if !valid || active.contains_key(&key.branch_id) {
                writes.delete(TRACKED_WORKING_DIFF_MARKER_SPACE, entry.key);
                continue;
            }
            if let Some(generation) = epoch.generation {
                active.insert(
                    key.branch_id,
                    ActiveWorkingDiffScope {
                        checkpoint_commit_id: epoch.checkpoint_commit_id,
                        generation,
                    },
                );
            }
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(active)
}

async fn stage_collect_stale_working_diff_space(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    space: StorageSpace,
    is_active: impl Fn(&StorageReadEntry) -> bool,
) -> Result<(), LixError> {
    let plan = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            if !is_active(&entry) {
                writes.delete(space, entry.key);
            }
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
fn stage_put(
    writes: &mut StorageWriteSet,
    identity: &HeadIdentity,
    value: &HeadValue,
) -> Result<(), LixError> {
    stage_put_ref(writes, identity, &value.as_ref())
}

#[cfg(test)]
fn stage_put_ref(
    writes: &mut StorageWriteSet,
    identity: &HeadIdentity,
    value: &HeadValueRef<'_>,
) -> Result<(), LixError> {
    let mut members = BTreeMap::new();
    members.insert(identity.file_id.clone(), encode_head_value(value)?);
    stage_put_group_members(writes, &identity.group_identity(), &members)?;
    if identity.file_id.is_some() {
        stage_put_member_bytes(
            writes,
            &identity.group_identity(),
            identity.file_id.as_deref(),
            &members[&identity.file_id],
        )?;
    }
    Ok(())
}

#[cfg(test)]
fn stage_put_group_members(
    writes: &mut StorageWriteSet,
    identity: &HeadGroupIdentity,
    members: &BTreeMap<Option<String>, Vec<u8>>,
) -> Result<(), LixError> {
    stage_put_group_bytes(writes, identity, encode_head_group_members(members)?);
    Ok(())
}

fn stage_put_group_members_with_baselines(
    writes: &mut StorageWriteSet,
    identity: &HeadGroupIdentity,
    members: &BTreeMap<Option<String>, Vec<u8>>,
    baselines: &BTreeMap<Option<String>, WorkingDiffBaseline>,
) -> Result<(), LixError> {
    stage_put_group_bytes(
        writes,
        identity,
        encode_head_group_members_with_baselines(members, baselines)?,
    );
    Ok(())
}

fn stage_put_group_bytes(
    writes: &mut StorageWriteSet,
    identity: &HeadGroupIdentity,
    bytes: Vec<u8>,
) {
    writes.put(
        TRACKED_HEAD_GROUP_SPACE,
        StorageKey(Bytes::from(encode_group_key(identity))),
        StorageValue {
            bytes: Bytes::from(bytes),
        },
    );
}

const WORKING_DIFF_INDEX_VALUE: &[u8] = b"\x01";

fn stage_put_working_diff_group_index(
    writes: &mut StorageWriteSet,
    coverage: &mut WorkingDiffIndexCoverage,
    checkpoint_commit_id: CommitId,
    identity: &HeadGroupIdentity,
) -> Result<(), LixError> {
    let key = encode_working_diff_group_key(checkpoint_commit_id, identity);
    coverage
        .add_encoded_group_key(&key)
        .ok_or_else(|| head_group_error("working-diff group index count exceeds u64"))?;
    writes.put(
        TRACKED_WORKING_DIFF_GROUP_SPACE,
        StorageKey(Bytes::from(key)),
        StorageValue {
            bytes: Bytes::from_static(WORKING_DIFF_INDEX_VALUE),
        },
    );
    Ok(())
}

fn stage_put_member_bytes(
    writes: &mut StorageWriteSet,
    group: &HeadGroupIdentity,
    file_id: Option<&str>,
    value: &[u8],
) -> Result<(), LixError> {
    let Some(file_id) = file_id else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked-head explicit member projection requires a file_id",
        ));
    };
    stage_put_file_member_bytes(writes, group, file_id, value);
    Ok(())
}

fn stage_put_file_member_bytes(
    writes: &mut StorageWriteSet,
    group: &HeadGroupIdentity,
    file_id: &str,
    value: &[u8],
) {
    let identity = HeadIdentity {
        branch_id: group.branch_id.clone(),
        generation: group.generation,
        schema_key: group.schema_key.clone(),
        entity_pk: group.entity_pk.clone(),
        file_id: Some(file_id.to_string()),
    };
    writes.put(
        TRACKED_HEAD_MEMBER_SPACE,
        StorageKey(Bytes::from(encode_member_key(&identity))),
        StorageValue {
            bytes: Bytes::copy_from_slice(value),
        },
    );
}

fn marker_key(branch_id: &str) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("tracked-head marker key", &BranchRef { branch_id })
}

fn encode_group_key(identity: &HeadGroupIdentity) -> Vec<u8> {
    let mut out = encode_scope_prefix(&identity.branch_id, identity.generation);
    write_key_string(&mut out, &identity.schema_key, KEY_PART_FINAL);
    write_entity_pk(&mut out, &identity.entity_pk);
    out
}

fn encode_working_diff_scope_prefix(
    branch_id: &str,
    checkpoint_commit_id: CommitId,
    generation: CommitId,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(branch_id.len() + 2 + 2 * GENERATION_BYTES);
    write_key_string(&mut out, branch_id, KEY_PART_FINAL);
    out.extend_from_slice(checkpoint_commit_id.as_uuid().as_bytes());
    out.extend_from_slice(generation.as_uuid().as_bytes());
    out
}

fn encode_working_diff_group_key(
    checkpoint_commit_id: CommitId,
    identity: &HeadGroupIdentity,
) -> Vec<u8> {
    let mut out = encode_working_diff_scope_prefix(
        &identity.branch_id,
        checkpoint_commit_id,
        identity.generation,
    );
    write_key_string(&mut out, &identity.schema_key, KEY_PART_FINAL);
    write_entity_pk(&mut out, &identity.entity_pk);
    out
}

fn encode_member_key(identity: &HeadIdentity) -> Vec<u8> {
    debug_assert!(identity.file_id.is_some());
    let mut out = encode_scope_prefix(&identity.branch_id, identity.generation);
    write_key_string(&mut out, &identity.schema_key, KEY_PART_FINAL);
    write_file_id(&mut out, identity.file_id.as_deref());
    write_entity_pk(&mut out, &identity.entity_pk);
    out
}

#[cfg(test)]
fn decode_group_key(bytes: &[u8]) -> Result<HeadGroupIdentity, LixError> {
    let mut offset = 0usize;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error("branch id has an invalid terminator"));
    }
    let generation = read_generation(bytes, &mut offset)?;
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error("schema key has an invalid terminator"));
    }
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("group key has trailing bytes"));
    }
    Ok(HeadGroupIdentity {
        branch_id,
        generation,
        schema_key,
        entity_pk,
    })
}

fn decode_working_diff_group_key(bytes: &[u8]) -> Result<(CommitId, HeadGroupIdentity), LixError> {
    let mut offset = 0usize;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error("branch id has an invalid terminator"));
    }
    let checkpoint_commit_id = read_generation(bytes, &mut offset)?;
    let generation = read_generation(bytes, &mut offset)?;
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error("schema key has an invalid terminator"));
    }
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("working-diff group key has trailing bytes"));
    }
    Ok((
        checkpoint_commit_id,
        HeadGroupIdentity {
            branch_id,
            generation,
            schema_key,
            entity_pk,
        },
    ))
}

fn decode_member_key(bytes: &[u8]) -> Result<HeadIdentity, LixError> {
    let mut offset = 0usize;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error("branch id has an invalid terminator"));
    }
    let generation = read_generation(bytes, &mut offset)?;
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error("schema key has an invalid terminator"));
    }
    let file_id = read_file_id(bytes, &mut offset)?;
    if file_id.is_none() {
        return Err(key_codec_error("member key must contain a file id"));
    }
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("member key has trailing bytes"));
    }
    Ok(HeadIdentity {
        branch_id,
        generation,
        schema_key,
        entity_pk,
        file_id,
    })
}

/// Decodes only the mutable suffix of a group key from a prefix-scoped scan.
///
/// `ScanPlan::prefix` already constrains the branch and generation. We still
/// verify the fixed scope before parsing the suffix so a malformed storage key
/// cannot be interpreted as a row from the wrong generation.
fn decode_group_key_in_scope(bytes: &[u8], scope: &[u8]) -> Result<HeadRowIdentity, LixError> {
    if !bytes.starts_with(scope) {
        return Err(key_codec_error(
            "does not begin with the scanned branch-generation scope",
        ));
    }
    let mut offset = scope.len();
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error("schema key has an invalid terminator"));
    }
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("group key has trailing bytes"));
    }
    Ok(HeadRowIdentity {
        schema_key,
        entity_pk,
        file_id: None,
    })
}

fn decode_working_diff_group_key_in_scope(
    bytes: &[u8],
    scope: &[u8],
) -> Result<HeadRowIdentity, LixError> {
    if !bytes.starts_with(scope) {
        return Err(key_codec_error(
            "does not begin with the scanned working-diff checkpoint-generation scope",
        ));
    }
    let mut offset = scope.len();
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error("schema key has an invalid terminator"));
    }
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("working-diff group key has trailing bytes"));
    }
    Ok(HeadRowIdentity {
        schema_key,
        entity_pk,
        file_id: None,
    })
}

const KEY_ESCAPE: u8 = 0xff;
const KEY_PART_FINAL: u8 = 0x00;
const KEY_PART_MORE: u8 = 0x01;
const FILE_ID_NONE: u8 = 0x00;
const FILE_ID_SOME: u8 = 0x01;
const GENERATION_BYTES: usize = 16;

/// Order-preserving tracked-head key encoding.
///
/// The head table is the normal read serving index, so its storage ordering is
/// also the visible row ordering: `(branch, generation, schema, entity,
/// file)`. Musli's storage encoding is excellent for values and structural
/// prefixes, but length-prefixed strings do not preserve lexical order. This
/// codec retains exact prefix scans while making every table scan already
/// ordered and duplicate-free for one branch generation.
fn encode_scope_prefix(branch_id: &str, generation: CommitId) -> Vec<u8> {
    let mut out = Vec::with_capacity(branch_id.len() + 2 + GENERATION_BYTES);
    write_key_string(&mut out, branch_id, KEY_PART_FINAL);
    out.extend_from_slice(generation.as_uuid().as_bytes());
    out
}

fn write_entity_pk(out: &mut Vec<u8>, entity_pk: &EntityPk) {
    debug_assert!(
        !entity_pk.parts.is_empty(),
        "tracked-head entity primary keys must be non-empty"
    );
    for (index, part) in entity_pk.parts.iter().enumerate() {
        let terminator = if index + 1 == entity_pk.parts.len() {
            KEY_PART_FINAL
        } else {
            KEY_PART_MORE
        };
        write_key_string(out, part, terminator);
    }
}

fn write_file_id(out: &mut Vec<u8>, file_id: Option<&str>) {
    match file_id {
        None => out.push(FILE_ID_NONE),
        Some(file_id) => {
            out.push(FILE_ID_SOME);
            write_key_string(out, file_id, KEY_PART_FINAL);
        }
    }
}

fn write_key_string(out: &mut Vec<u8>, value: &str, terminator: u8) {
    for &byte in value.as_bytes() {
        if byte == KEY_PART_FINAL {
            out.extend_from_slice(&[KEY_PART_FINAL, KEY_ESCAPE]);
        } else {
            out.push(byte);
        }
    }
    out.extend_from_slice(&[KEY_PART_FINAL, terminator]);
}

fn read_generation(bytes: &[u8], offset: &mut usize) -> Result<CommitId, LixError> {
    let end = offset
        .checked_add(GENERATION_BYTES)
        .ok_or_else(|| key_codec_error("generation offset overflow"))?;
    let generation = bytes
        .get(*offset..end)
        .ok_or_else(|| key_codec_error("is truncated before generation"))?;
    let mut uuid = [0; GENERATION_BYTES];
    uuid.copy_from_slice(generation);
    *offset = end;
    Ok(CommitId::new(uuid::Uuid::from_bytes(uuid)))
}

fn read_entity_pk(bytes: &[u8], offset: &mut usize) -> Result<EntityPk, LixError> {
    let mut parts = Vec::new();
    loop {
        let (part, terminator) = read_key_string(bytes, offset, "entity primary key")?;
        parts.push(part);
        match terminator {
            KEY_PART_FINAL => break,
            KEY_PART_MORE => {}
            _ => {
                return Err(key_codec_error(
                    "entity primary key has an invalid terminator",
                ));
            }
        }
    }
    EntityPk::from_parts(parts).map_err(|error| {
        key_codec_error(&format!("contains an invalid entity primary key: {error}"))
    })
}

fn read_file_id(bytes: &[u8], offset: &mut usize) -> Result<Option<String>, LixError> {
    let tag = *bytes
        .get(*offset)
        .ok_or_else(|| key_codec_error("is truncated before file id"))?;
    *offset += 1;
    match tag {
        FILE_ID_NONE => Ok(None),
        FILE_ID_SOME => {
            let (file_id, terminator) = read_key_string(bytes, offset, "file id")?;
            if terminator != KEY_PART_FINAL {
                return Err(key_codec_error("file id has an invalid terminator"));
            }
            Ok(Some(file_id))
        }
        _ => Err(key_codec_error("has an invalid file id tag")),
    }
}

fn read_key_string(
    bytes: &[u8],
    offset: &mut usize,
    field: &str,
) -> Result<(String, u8), LixError> {
    let start = *offset;
    let mut cursor = start;
    // The normal generated IDs do not contain the escaped NUL byte. Decode
    // that common case directly from the RocksDB key instead of first growing
    // a temporary `Vec<u8>` one byte at a time.
    loop {
        let byte = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated in {field}")))?;
        cursor += 1;
        if byte != KEY_PART_FINAL {
            continue;
        }
        let terminator = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated after {field}")))?;
        cursor += 1;
        if terminator != KEY_ESCAPE {
            let value = std::str::from_utf8(&bytes[start..cursor - 2])
                .map_err(|error| key_codec_error(&format!("{field} is not UTF-8: {error}")))?;
            *offset = cursor;
            return Ok((value.to_owned(), terminator));
        }
        break;
    }

    // Escaped NUL bytes are rare but remain fully supported. Seed the owned
    // buffer with the prefix before the first escape, then decode the rest.
    let mut out = Vec::with_capacity(cursor.saturating_sub(start) + 16);
    out.extend_from_slice(&bytes[start..cursor - 2]);
    out.push(KEY_PART_FINAL);
    loop {
        let byte = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated in {field}")))?;
        cursor += 1;
        if byte != KEY_PART_FINAL {
            out.push(byte);
            continue;
        }
        let terminator = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated after {field}")))?;
        cursor += 1;
        if terminator == KEY_ESCAPE {
            out.push(KEY_PART_FINAL);
            continue;
        }
        let value = String::from_utf8(out).map_err(|error| {
            key_codec_error(&format!("{field} is not UTF-8: {}", error.utf8_error()))
        })?;
        *offset = cursor;
        return Ok((value, terminator));
    }
}

fn key_codec_error(message: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("invalid tracked-head row key: {message}"),
    )
}

fn decode_marker_value(value: StorageProjectedValue) -> Result<TrackedHeadMarker, LixError> {
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked-head marker read unexpectedly omitted its value",
        ));
    };
    storage_codec::decode("tracked-head marker", &bytes)
}

/// v6 packs all file-backed members of one logical entity PK into one
/// canonical current-state group. The individual member payload is the proven
/// fixed-header v3 value below; only the outer framing is new.
///
/// ```text
///  0      group format version (1)
///  1..5   member count (big endian u32)
///  repeated:
///    0      file-id tag (0 = none, 1 = UTF-8 string)
///    1..5   file-id byte length when tag = 1 (big endian u32)
///    ...    file-id UTF-8 bytes when tag = 1
///    ...    v3 member byte length (big endian u32)
///    ...    v3 member bytes
/// ```
///
/// Members are strictly sorted by Rust's `Option<String>` ordering. This
/// makes scans deterministic and, more importantly, rejects silently
/// duplicated full identities at the storage boundary.
const HEAD_GROUP_VALUE_VERSION: u8 = 3;
const HEAD_GROUP_HEADER_BYTES: usize = 5;
const WORKING_DIFF_BASELINE_CLEAN: u8 = 0;
const WORKING_DIFF_BASELINE_ABSENT: u8 = 1;
const WORKING_DIFF_BASELINE_PRESENT: u8 = 2;
const WORKING_DIFF_SLOT_NONE: u8 = 0;
const WORKING_DIFF_SLOT_REF: u8 = 1;
const WORKING_DIFF_SLOT_INLINE: u8 = 2;
const WORKING_DIFF_VERSION_BYTES: usize =
    16 + 16 + 1 + 8 + 8 + 1 + JSON_REF_BYTES + 1 + JSON_REF_BYTES;

struct HeadGroupMemberBytes {
    file_id: Option<String>,
    value: Bytes,
    baseline: WorkingDiffBaseline,
}

/// A validated, borrowed member from a v6 group value.
///
/// The incremental writer uses this cursor to copy untouched member bytes
/// directly into the next group while retaining the old fixed-header value
/// needed to preserve `created_at` on a changed identity.
#[derive(Clone, Copy)]
struct HeadGroupMemberView<'a> {
    file_id: Option<&'a str>,
    value: &'a [u8],
    head: HeadValueView<'a>,
    baseline: WorkingDiffBaseline,
}

/// Streaming parser for the authoritative v6 group bytes.
///
/// Unlike the reader-side decoder, this does not allocate a `String`,
/// `Bytes`, or map for every member. It still enforces the complete wire
/// contract while every member is consumed, including untouched siblings.
struct HeadGroupMembers<'a> {
    bytes: &'a [u8],
    offset: usize,
    remaining: usize,
    prior_file_id: Option<Option<&'a str>>,
}

impl<'a> HeadGroupMembers<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, LixError> {
        if bytes.len() < HEAD_GROUP_HEADER_BYTES {
            return Err(head_group_error("value is shorter than the fixed header"));
        }
        if bytes[0] != HEAD_GROUP_VALUE_VERSION {
            return Err(head_group_error(&format!(
                "unsupported group format version {}",
                bytes[0]
            )));
        }
        let remaining = usize::try_from(read_u32(&bytes[1..5], "group member count")?)
            .map_err(|_| head_group_error("member count exceeds usize"))?;
        Ok(Self {
            bytes,
            offset: HEAD_GROUP_HEADER_BYTES,
            remaining,
            prior_file_id: None,
        })
    }

    fn next_member(&mut self) -> Result<Option<HeadGroupMemberView<'a>>, LixError> {
        if self.remaining == 0 {
            if self.offset != self.bytes.len() {
                return Err(head_group_error("has trailing bytes"));
            }
            return Ok(None);
        }

        let tag = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| head_group_error("is truncated before member file id"))?;
        self.offset += 1;
        let file_id = match tag {
            FILE_ID_NONE => None,
            FILE_ID_SOME => {
                let file_id_len =
                    read_group_u32(self.bytes, &mut self.offset, "member file-id length")?;
                let file_id_end = self
                    .offset
                    .checked_add(file_id_len)
                    .ok_or_else(|| head_group_error("member file-id length overflow"))?;
                let file_id = self
                    .bytes
                    .get(self.offset..file_id_end)
                    .ok_or_else(|| head_group_error("is truncated in member file id"))?;
                self.offset = file_id_end;
                Some(std::str::from_utf8(file_id).map_err(|error| {
                    head_group_error(&format!("member file id is not UTF-8: {error}"))
                })?)
            }
            _ => return Err(head_group_error("has an invalid member file-id tag")),
        };
        if self.prior_file_id.is_some_and(|prior| prior >= file_id) {
            return Err(head_group_error(
                "members are not strictly ordered by file id",
            ));
        }

        let value_len = read_group_u32(self.bytes, &mut self.offset, "member value length")?;
        let value_end = self
            .offset
            .checked_add(value_len)
            .ok_or_else(|| head_group_error("member value length overflow"))?;
        let value = self
            .bytes
            .get(self.offset..value_end)
            .ok_or_else(|| head_group_error("is truncated in member value"))?;
        let head = decode_head_value(value)?;
        self.offset = value_end;
        let baseline = decode_working_diff_baseline(self.bytes, &mut self.offset)?;
        self.remaining -= 1;
        self.prior_file_id = Some(file_id);
        Ok(Some(HeadGroupMemberView {
            file_id,
            value,
            head,
            baseline,
        }))
    }
}

fn append_head_group_member(
    encoded: &mut Vec<u8>,
    file_id: Option<&str>,
    value: &[u8],
    baseline: WorkingDiffBaseline,
) -> Result<(), LixError> {
    let file_id_bytes = match file_id {
        None => 0,
        Some(file_id) => {
            u32::try_from(file_id.len()).map_err(|_| head_group_error("file id exceeds u32"))?;
            4usize
                .checked_add(file_id.len())
                .ok_or_else(|| head_group_error("group value length overflow"))?
        }
    };
    u32::try_from(value.len()).map_err(|_| head_group_error("member value exceeds u32"))?;
    let member_len = 1usize
        .checked_add(file_id_bytes)
        .and_then(|length| length.checked_add(4))
        .and_then(|length| length.checked_add(value.len()))
        .and_then(|length| length.checked_add(working_diff_baseline_len(baseline)))
        .ok_or_else(|| head_group_error("group value length overflow"))?;
    encoded
        .len()
        .checked_add(member_len)
        .ok_or_else(|| head_group_error("group value length overflow"))?;
    encoded
        .try_reserve(member_len)
        .map_err(|_| head_group_error("cannot reserve group value bytes"))?;

    match file_id {
        None => encoded.push(FILE_ID_NONE),
        Some(file_id) => {
            encoded.push(FILE_ID_SOME);
            let file_id_len = u32::try_from(file_id.len())
                .map_err(|_| head_group_error("file id exceeds u32"))?;
            encoded.extend_from_slice(&file_id_len.to_be_bytes());
            encoded.extend_from_slice(file_id.as_bytes());
        }
    }
    let value_len =
        u32::try_from(value.len()).map_err(|_| head_group_error("member value exceeds u32"))?;
    encoded.extend_from_slice(&value_len.to_be_bytes());
    encoded.extend_from_slice(value);
    encode_working_diff_baseline(encoded, baseline);
    Ok(())
}

fn working_diff_baseline_len(baseline: WorkingDiffBaseline) -> usize {
    match baseline {
        WorkingDiffBaseline::Clean => 1,
        WorkingDiffBaseline::Absent(_) => 1 + UUID_BYTES,
        WorkingDiffBaseline::Present { .. } => 1 + UUID_BYTES + WORKING_DIFF_VERSION_BYTES,
    }
}

fn encode_working_diff_baseline(encoded: &mut Vec<u8>, baseline: WorkingDiffBaseline) {
    match baseline {
        WorkingDiffBaseline::Clean => encoded.push(WORKING_DIFF_BASELINE_CLEAN),
        WorkingDiffBaseline::Absent(checkpoint_commit_id) => {
            encoded.push(WORKING_DIFF_BASELINE_ABSENT);
            encoded.extend_from_slice(checkpoint_commit_id.as_uuid().as_bytes());
        }
        WorkingDiffBaseline::Present {
            checkpoint_commit_id,
            version,
        } => {
            encoded.push(WORKING_DIFF_BASELINE_PRESENT);
            encoded.extend_from_slice(checkpoint_commit_id.as_uuid().as_bytes());
            encode_working_diff_version(encoded, version);
        }
    }
}

fn encode_working_diff_version(encoded: &mut Vec<u8>, version: WorkingDiffVersion) {
    encoded.extend_from_slice(version.change_id.as_uuid().as_bytes());
    encoded.extend_from_slice(version.commit_id.as_uuid().as_bytes());
    encoded.push(u8::from(version.deleted));
    encoded.extend_from_slice(&version.created_at.packed().to_be_bytes());
    encoded.extend_from_slice(&version.updated_at.packed().to_be_bytes());
    encode_working_diff_slot(encoded, version.snapshot);
    encode_working_diff_slot(encoded, version.metadata);
}

fn encode_working_diff_slot(encoded: &mut Vec<u8>, slot: WorkingDiffSlotFingerprint) {
    encoded.push(slot.kind);
    encoded.extend_from_slice(&slot.hash);
}

fn decode_working_diff_baseline(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<WorkingDiffBaseline, LixError> {
    let tag = *bytes
        .get(*offset)
        .ok_or_else(|| head_group_error("is truncated before working-diff baseline"))?;
    *offset += 1;
    match tag {
        WORKING_DIFF_BASELINE_CLEAN => Ok(WorkingDiffBaseline::Clean),
        WORKING_DIFF_BASELINE_ABSENT => Ok(WorkingDiffBaseline::Absent(CommitId::new(
            uuid_from_working_diff_bytes(
                take_working_diff_bytes(bytes, offset, UUID_BYTES)?,
                "baseline checkpoint id",
            )?,
        ))),
        WORKING_DIFF_BASELINE_PRESENT => {
            let checkpoint_commit_id = CommitId::new(uuid_from_working_diff_bytes(
                take_working_diff_bytes(bytes, offset, UUID_BYTES)?,
                "baseline checkpoint id",
            )?);
            let version = decode_working_diff_version(bytes, offset)?;
            Ok(WorkingDiffBaseline::Present {
                checkpoint_commit_id,
                version,
            })
        }
        _ => Err(head_group_error("has an invalid working-diff baseline tag")),
    }
}

fn decode_working_diff_version(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<WorkingDiffVersion, LixError> {
    let payload = take_working_diff_bytes(bytes, offset, WORKING_DIFF_VERSION_BYTES)?;
    let mut field_offset = 0usize;
    let change_id = ChangeId::new(uuid_from_working_diff_bytes(
        take_working_diff_bytes(payload, &mut field_offset, UUID_BYTES)?,
        "change id",
    )?);
    let commit_id = CommitId::new(uuid_from_working_diff_bytes(
        take_working_diff_bytes(payload, &mut field_offset, UUID_BYTES)?,
        "commit id",
    )?);
    let deleted = match *take_working_diff_bytes(payload, &mut field_offset, 1)?
        .first()
        .ok_or_else(|| head_group_error("working-diff deletion flag is missing"))?
    {
        0 => false,
        1 => true,
        _ => return Err(head_group_error("working-diff deletion flag is invalid")),
    };
    let created_at = LixTimestamp::from_packed(read_u64(
        take_working_diff_bytes(payload, &mut field_offset, 8)?,
        "working-diff created_at",
    )?)
    .map_err(|error| head_group_error(&format!("invalid working-diff created_at: {error}")))?;
    let updated_at = LixTimestamp::from_packed(read_u64(
        take_working_diff_bytes(payload, &mut field_offset, 8)?,
        "working-diff updated_at",
    )?)
    .map_err(|error| head_group_error(&format!("invalid working-diff updated_at: {error}")))?;
    let snapshot = decode_working_diff_slot(payload, &mut field_offset, "snapshot")?;
    let metadata = decode_working_diff_slot(payload, &mut field_offset, "metadata")?;
    debug_assert_eq!(field_offset, WORKING_DIFF_VERSION_BYTES);
    Ok(WorkingDiffVersion {
        change_id,
        commit_id,
        deleted,
        created_at,
        updated_at,
        snapshot,
        metadata,
    })
}

fn decode_working_diff_slot(
    bytes: &[u8],
    offset: &mut usize,
    field: &str,
) -> Result<WorkingDiffSlotFingerprint, LixError> {
    let kind = *take_working_diff_bytes(bytes, offset, 1)?
        .first()
        .ok_or_else(|| head_group_error(&format!("working-diff {field} kind is missing")))?;
    if !matches!(
        kind,
        WORKING_DIFF_SLOT_NONE | WORKING_DIFF_SLOT_REF | WORKING_DIFF_SLOT_INLINE
    ) {
        return Err(head_group_error(&format!(
            "working-diff {field} slot kind is invalid"
        )));
    }
    let hash: [u8; JSON_REF_BYTES] = take_working_diff_bytes(bytes, offset, JSON_REF_BYTES)?
        .try_into()
        .map_err(|_| head_group_error(&format!("working-diff {field} hash is invalid")))?;
    if kind == WORKING_DIFF_SLOT_NONE && hash != [0; JSON_REF_BYTES] {
        return Err(head_group_error(&format!(
            "working-diff {field} none slot must have a zero hash"
        )));
    }
    Ok(WorkingDiffSlotFingerprint { kind, hash })
}

fn take_working_diff_bytes<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    length: usize,
) -> Result<&'a [u8], LixError> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| head_group_error("working-diff value offset overflow"))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| head_group_error("is truncated in working-diff value"))?;
    *offset = end;
    Ok(value)
}

fn uuid_from_working_diff_bytes(bytes: &[u8], field: &str) -> Result<uuid::Uuid, LixError> {
    let bytes: [u8; UUID_BYTES] = bytes
        .try_into()
        .map_err(|_| head_group_error(&format!("working-diff {field} has invalid width")))?;
    Ok(uuid::Uuid::from_bytes(bytes))
}

#[cfg(test)]
fn encode_head_group_members(
    members: &BTreeMap<Option<String>, Vec<u8>>,
) -> Result<Vec<u8>, LixError> {
    let baselines = members
        .keys()
        .cloned()
        .map(|file_id| (file_id, WorkingDiffBaseline::Clean))
        .collect::<BTreeMap<_, _>>();
    encode_head_group_members_with_baselines(members, &baselines)
}

fn encode_head_group_members_with_baselines(
    members: &BTreeMap<Option<String>, Vec<u8>>,
    baselines: &BTreeMap<Option<String>, WorkingDiffBaseline>,
) -> Result<Vec<u8>, LixError> {
    let member_count =
        u32::try_from(members.len()).map_err(|_| head_group_error("member count exceeds u32"))?;
    let payload_len = members.iter().try_fold(0usize, |total, (file_id, value)| {
        let baseline = baselines
            .get(file_id)
            .copied()
            .ok_or_else(|| head_group_error("is missing a working-diff baseline for one member"))?;
        let file_id_len = file_id.as_ref().map_or(0, String::len);
        u32::try_from(file_id_len).map_err(|_| head_group_error("file id exceeds u32"))?;
        let file_id_bytes = if file_id.is_some() {
            4usize
                .checked_add(file_id_len)
                .ok_or_else(|| head_group_error("group value length overflow"))?
        } else {
            0
        };
        u32::try_from(value.len()).map_err(|_| head_group_error("member value exceeds u32"))?;
        decode_head_value(value)?;
        let member_len = 1usize
            .checked_add(file_id_bytes)
            .and_then(|length| length.checked_add(4))
            .and_then(|length| length.checked_add(value.len()))
            .and_then(|length| length.checked_add(working_diff_baseline_len(baseline)))
            .ok_or_else(|| head_group_error("group value length overflow"))?;
        total
            .checked_add(member_len)
            .ok_or_else(|| head_group_error("group value length overflow"))
    })?;
    let capacity = HEAD_GROUP_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| head_group_error("group value length overflow"))?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.push(HEAD_GROUP_VALUE_VERSION);
    encoded.extend_from_slice(&member_count.to_be_bytes());
    for (file_id, value) in members {
        let baseline = baselines
            .get(file_id)
            .copied()
            .ok_or_else(|| head_group_error("is missing a working-diff baseline for one member"))?;
        match file_id {
            None => encoded.push(FILE_ID_NONE),
            Some(file_id) => {
                encoded.push(FILE_ID_SOME);
                let file_id_len = u32::try_from(file_id.len())
                    .map_err(|_| head_group_error("file id exceeds u32"))?;
                encoded.extend_from_slice(&file_id_len.to_be_bytes());
                encoded.extend_from_slice(file_id.as_bytes());
            }
        }
        let value_len =
            u32::try_from(value.len()).map_err(|_| head_group_error("member value exceeds u32"))?;
        encoded.extend_from_slice(&value_len.to_be_bytes());
        encoded.extend_from_slice(value);
        encode_working_diff_baseline(&mut encoded, baseline);
    }
    debug_assert_eq!(encoded.len(), capacity);
    Ok(encoded)
}

fn decode_head_group_members(bytes: &[u8]) -> Result<Vec<HeadGroupMemberBytes>, LixError> {
    if bytes.len() < HEAD_GROUP_HEADER_BYTES {
        return Err(head_group_error("value is shorter than the fixed header"));
    }
    if bytes[0] != HEAD_GROUP_VALUE_VERSION {
        return Err(head_group_error(&format!(
            "unsupported group format version {}",
            bytes[0]
        )));
    }
    let member_count = usize::try_from(read_u32(&bytes[1..5], "group member count")?)
        .map_err(|_| head_group_error("member count exceeds usize"))?;
    let mut offset = HEAD_GROUP_HEADER_BYTES;
    let mut prior_file_id = None::<Option<String>>;
    let mut members = Vec::with_capacity(member_count);
    for _ in 0..member_count {
        let tag = *bytes
            .get(offset)
            .ok_or_else(|| head_group_error("is truncated before member file id"))?;
        offset += 1;
        let file_id = match tag {
            FILE_ID_NONE => None,
            FILE_ID_SOME => {
                let file_id_len = read_group_u32(bytes, &mut offset, "member file-id length")?;
                let file_id_end = offset
                    .checked_add(file_id_len)
                    .ok_or_else(|| head_group_error("member file-id length overflow"))?;
                let file_id = bytes
                    .get(offset..file_id_end)
                    .ok_or_else(|| head_group_error("is truncated in member file id"))?;
                offset = file_id_end;
                Some(
                    std::str::from_utf8(file_id)
                        .map_err(|error| {
                            head_group_error(&format!("member file id is not UTF-8: {error}"))
                        })?
                        .to_string(),
                )
            }
            _ => return Err(head_group_error("has an invalid member file-id tag")),
        };
        if prior_file_id
            .as_ref()
            .is_some_and(|prior| prior >= &file_id)
        {
            return Err(head_group_error(
                "members are not strictly ordered by file id",
            ));
        }
        let value_len = read_group_u32(bytes, &mut offset, "member value length")?;
        let value_end = offset
            .checked_add(value_len)
            .ok_or_else(|| head_group_error("member value length overflow"))?;
        let value = bytes
            .get(offset..value_end)
            .ok_or_else(|| head_group_error("is truncated in member value"))?;
        decode_head_value(value)?;
        offset = value_end;
        let baseline = decode_working_diff_baseline(bytes, &mut offset)?;
        prior_file_id = Some(file_id.clone());
        members.push(HeadGroupMemberBytes {
            file_id,
            value: Bytes::copy_from_slice(value),
            baseline,
        });
    }
    if offset != bytes.len() {
        return Err(head_group_error("has trailing bytes"));
    }
    Ok(members)
}

fn read_group_u32(bytes: &[u8], offset: &mut usize, field: &str) -> Result<usize, LixError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| head_group_error(&format!("{field} offset overflow")))?;
    let value = read_u32(
        bytes
            .get(*offset..end)
            .ok_or_else(|| head_group_error(&format!("is truncated before {field}")))?,
        field,
    )?;
    *offset = end;
    usize::try_from(value).map_err(|_| head_group_error(&format!("{field} exceeds usize")))
}

fn head_group_error(message: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("invalid tracked-head v6 group: {message}"),
    )
}

/// v3 head values are intentionally a small, fixed-header wire record rather
/// than a general Musli struct. The normal read path needs only these fields,
/// and decoding a Musli `JsonSlot` first allocated an intermediate value for
/// every row before it was copied into a live-state row.
///
/// ```text
///  0      format version (3)
///  1      deleted + snapshot/metadata kinds
///  2..18  change UUID
/// 18..34  commit UUID
/// 34..42  created_at packed timestamp (big endian)
/// 42..50  updated_at packed timestamp (big endian)
/// 50..54  snapshot payload byte length (big endian u32)
/// 54..58  metadata payload byte length (big endian u32)
/// 58..    snapshot payload, then metadata payload
/// ```
///
/// Slot payloads are either inline UTF-8 JSON or a fixed 32-byte `JsonRef`.
/// This makes parsing bounded and lets the scan path build the final
/// `MaterializedLiveStateRow` in one pass.
const HEAD_VALUE_VERSION: u8 = 3;
const HEAD_VALUE_HEADER_BYTES: usize = 58;
const HEAD_VALUE_DELETED: u8 = 0b0000_0001;
const HEAD_VALUE_SNAPSHOT_SHIFT: u8 = 1;
const HEAD_VALUE_METADATA_SHIFT: u8 = 3;
const HEAD_VALUE_SLOT_MASK: u8 = 0b11;
const HEAD_VALUE_ALLOWED_FLAGS: u8 = HEAD_VALUE_DELETED
    | (HEAD_VALUE_SLOT_MASK << HEAD_VALUE_SNAPSHOT_SHIFT)
    | (HEAD_VALUE_SLOT_MASK << HEAD_VALUE_METADATA_SHIFT);
const HEAD_SLOT_NONE: u8 = 0;
const HEAD_SLOT_REF: u8 = 1;
const HEAD_SLOT_INLINE: u8 = 2;
const UUID_BYTES: usize = 16;
const JSON_REF_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeadSlotView<'a> {
    None,
    Ref(JsonRef),
    Inline(&'a str),
}

#[derive(Debug, Clone, Copy)]
struct HeadValueView<'a> {
    change_id: ChangeId,
    commit_id: CommitId,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: HeadSlotView<'a>,
    metadata: HeadSlotView<'a>,
}

impl HeadValueView<'_> {
    fn working_diff_version(self) -> WorkingDiffVersion {
        WorkingDiffVersion {
            change_id: self.change_id,
            commit_id: self.commit_id,
            deleted: self.deleted,
            created_at: self.created_at,
            updated_at: self.updated_at,
            snapshot: working_diff_slot_fingerprint(self.snapshot),
            metadata: working_diff_slot_fingerprint(self.metadata),
        }
    }
}

impl WorkingDiffVersion {
    fn payload_eq(self, other: Self) -> bool {
        // Keep the accelerator's net-change classification identical to the
        // canonical tracked diff: a shared change record is intrinsically the
        // same payload, otherwise compare the two stored payload slots.
        self.change_id == other.change_id
            || (self.snapshot == other.snapshot && self.metadata == other.metadata)
    }

    fn into_diff_row(self, identity: &HeadRowIdentity) -> TrackedStateDiffRow {
        TrackedStateDiffRow {
            entity_pk: identity.entity_pk.clone(),
            schema_key: identity.schema_key.clone(),
            file_id: identity.file_id.clone(),
            deleted: self.deleted,
            created_at: self.created_at,
            updated_at: self.updated_at,
            change_id: self.change_id,
            commit_id: self.commit_id,
        }
    }
}

fn working_diff_slot_fingerprint(slot: HeadSlotView<'_>) -> WorkingDiffSlotFingerprint {
    match slot {
        HeadSlotView::None => WorkingDiffSlotFingerprint {
            kind: WORKING_DIFF_SLOT_NONE,
            hash: [0; JSON_REF_BYTES],
        },
        HeadSlotView::Ref(json_ref) => WorkingDiffSlotFingerprint {
            kind: WORKING_DIFF_SLOT_REF,
            hash: *json_ref.as_hash_array(),
        },
        HeadSlotView::Inline(json) => WorkingDiffSlotFingerprint {
            kind: WORKING_DIFF_SLOT_INLINE,
            hash: *JsonRef::for_content(json.as_bytes()).as_hash_array(),
        },
    }
}

fn encode_head_value(value: &HeadValueRef<'_>) -> Result<Vec<u8>, LixError> {
    let snapshot_kind = encoded_slot_kind(value.snapshot);
    let metadata_kind = encoded_slot_kind(value.metadata);
    if value.deleted && (snapshot_kind != HEAD_SLOT_NONE || metadata_kind != HEAD_SLOT_NONE) {
        return Err(head_value_error(
            "deleted tracked-head rows must not carry JSON payloads",
        ));
    }
    let snapshot_len = encoded_slot_len(value.snapshot);
    let metadata_len = encoded_slot_len(value.metadata);
    let capacity = HEAD_VALUE_HEADER_BYTES
        .checked_add(snapshot_len)
        .and_then(|bytes| bytes.checked_add(metadata_len))
        .ok_or_else(|| head_value_error("encoded row length overflow"))?;
    let mut bytes = Vec::with_capacity(capacity);
    bytes.push(HEAD_VALUE_VERSION);
    let mut flags = if value.deleted { HEAD_VALUE_DELETED } else { 0 };
    flags |= snapshot_kind << HEAD_VALUE_SNAPSHOT_SHIFT;
    flags |= metadata_kind << HEAD_VALUE_METADATA_SHIFT;
    bytes.push(flags);
    bytes.extend_from_slice(value.change_id.as_uuid().as_bytes());
    bytes.extend_from_slice(value.commit_id.as_uuid().as_bytes());
    bytes.extend_from_slice(&value.created_at.packed().to_be_bytes());
    bytes.extend_from_slice(&value.updated_at.packed().to_be_bytes());
    bytes.extend_from_slice(
        &u32::try_from(snapshot_len)
            .map_err(|_| head_value_error("snapshot payload exceeds v3 u32 limit"))?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(
        &u32::try_from(metadata_len)
            .map_err(|_| head_value_error("metadata payload exceeds v3 u32 limit"))?
            .to_be_bytes(),
    );
    append_slot_payload(&mut bytes, value.snapshot);
    append_slot_payload(&mut bytes, value.metadata);
    debug_assert_eq!(bytes.len(), capacity);
    Ok(bytes)
}

fn encoded_slot_kind(slot: JsonSlotRef<'_>) -> u8 {
    match slot {
        JsonSlotRef::None => HEAD_SLOT_NONE,
        JsonSlotRef::Ref(_) => HEAD_SLOT_REF,
        JsonSlotRef::Inline(_) => HEAD_SLOT_INLINE,
    }
}

fn encoded_slot_len(slot: JsonSlotRef<'_>) -> usize {
    match slot {
        JsonSlotRef::None => 0,
        JsonSlotRef::Ref(_) => JSON_REF_BYTES,
        JsonSlotRef::Inline(json) => json.len(),
    }
}

fn append_slot_payload(bytes: &mut Vec<u8>, slot: JsonSlotRef<'_>) {
    match slot {
        JsonSlotRef::None => {}
        JsonSlotRef::Ref(json_ref) => bytes.extend_from_slice(json_ref.as_hash_bytes()),
        JsonSlotRef::Inline(json) => bytes.extend_from_slice(json.as_bytes()),
    }
}

fn full_value_bytes(value: StorageProjectedValue) -> Result<Bytes, LixError> {
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(head_value_error(
            "tracked-head row read unexpectedly omitted its value",
        ));
    };
    Ok(bytes)
}

fn decode_head_value(bytes: &[u8]) -> Result<HeadValueView<'_>, LixError> {
    if bytes.len() < HEAD_VALUE_HEADER_BYTES {
        return Err(head_value_error("row is shorter than the v3 fixed header"));
    }
    if bytes[0] != HEAD_VALUE_VERSION {
        return Err(head_value_error(&format!(
            "unsupported row format version {}",
            bytes[0]
        )));
    }
    let flags = bytes[1];
    if flags & !HEAD_VALUE_ALLOWED_FLAGS != 0 {
        return Err(head_value_error("row has unknown v3 flag bits"));
    }
    let snapshot_kind = (flags >> HEAD_VALUE_SNAPSHOT_SHIFT) & HEAD_VALUE_SLOT_MASK;
    let metadata_kind = (flags >> HEAD_VALUE_METADATA_SHIFT) & HEAD_VALUE_SLOT_MASK;
    let change_id = ChangeId::new(uuid_from_head_bytes(&bytes[2..18], "change id")?);
    let commit_id = CommitId::new(uuid_from_head_bytes(&bytes[18..34], "commit id")?);
    let created_at = LixTimestamp::from_packed(read_u64(&bytes[34..42], "created_at")?)
        .map_err(|error| head_value_error(&format!("invalid created_at: {error}")))?;
    let updated_at = LixTimestamp::from_packed(read_u64(&bytes[42..50], "updated_at")?)
        .map_err(|error| head_value_error(&format!("invalid updated_at: {error}")))?;
    let snapshot_len = usize::try_from(read_u32(&bytes[50..54], "snapshot length")?)
        .map_err(|_| head_value_error("snapshot length exceeds usize"))?;
    let metadata_len = usize::try_from(read_u32(&bytes[54..58], "metadata length")?)
        .map_err(|_| head_value_error("metadata length exceeds usize"))?;
    let snapshot_end = HEAD_VALUE_HEADER_BYTES
        .checked_add(snapshot_len)
        .ok_or_else(|| head_value_error("snapshot payload length overflow"))?;
    let metadata_end = snapshot_end
        .checked_add(metadata_len)
        .ok_or_else(|| head_value_error("metadata payload length overflow"))?;
    if metadata_end != bytes.len() {
        return Err(head_value_error(
            "row payload lengths do not match the buffer",
        ));
    }
    let snapshot = decode_slot(
        snapshot_kind,
        &bytes[HEAD_VALUE_HEADER_BYTES..snapshot_end],
        "snapshot",
    )?;
    let metadata = decode_slot(
        metadata_kind,
        &bytes[snapshot_end..metadata_end],
        "metadata",
    )?;
    let deleted = flags & HEAD_VALUE_DELETED != 0;
    if deleted && (snapshot != HeadSlotView::None || metadata != HeadSlotView::None) {
        return Err(head_value_error(
            "deleted tracked-head rows must not carry JSON payloads",
        ));
    }
    Ok(HeadValueView {
        change_id,
        commit_id,
        deleted,
        created_at,
        updated_at,
        snapshot,
        metadata,
    })
}

fn uuid_from_head_bytes(bytes: &[u8], field: &str) -> Result<uuid::Uuid, LixError> {
    let bytes: [u8; UUID_BYTES] = bytes.try_into().map_err(|_| {
        head_value_error(&format!(
            "{field} must have {UUID_BYTES} bytes in the v3 header"
        ))
    })?;
    Ok(uuid::Uuid::from_bytes(bytes))
}

fn read_u64(bytes: &[u8], field: &str) -> Result<u64, LixError> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| head_value_error(&format!("{field} has an invalid fixed-header width")))?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_u32(bytes: &[u8], field: &str) -> Result<u32, LixError> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| head_value_error(&format!("{field} has an invalid fixed-header width")))?;
    Ok(u32::from_be_bytes(bytes))
}

fn decode_slot<'a>(kind: u8, bytes: &'a [u8], field: &str) -> Result<HeadSlotView<'a>, LixError> {
    match kind {
        HEAD_SLOT_NONE if bytes.is_empty() => Ok(HeadSlotView::None),
        HEAD_SLOT_NONE => Err(head_value_error(&format!(
            "{field} none slot must have an empty payload"
        ))),
        HEAD_SLOT_REF if bytes.len() == JSON_REF_BYTES => {
            let hash: [u8; JSON_REF_BYTES] = bytes.try_into().map_err(|_| {
                head_value_error(&format!(
                    "{field} ref payload must have {JSON_REF_BYTES} bytes"
                ))
            })?;
            Ok(HeadSlotView::Ref(JsonRef::from_hash_bytes(hash)))
        }
        HEAD_SLOT_REF => Err(head_value_error(&format!(
            "{field} ref payload must have {JSON_REF_BYTES} bytes"
        ))),
        HEAD_SLOT_INLINE => std::str::from_utf8(bytes)
            .map(HeadSlotView::Inline)
            .map_err(|error| {
                head_value_error(&format!("{field} inline payload is not UTF-8: {error}"))
            }),
        _ => Err(head_value_error(&format!(
            "{field} has an unknown slot kind {kind}"
        ))),
    }
}

fn head_value_error(message: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("invalid tracked-head v3 row: {message}"),
    )
}

#[derive(Clone, Copy)]
enum DeferredJsonField {
    Snapshot,
    Metadata,
}

struct DeferredJson {
    row_index: usize,
    field: DeferredJsonField,
    json_ref: JsonRef,
}

/// Builds serving rows directly from a v3 wire value. The only allocations
/// here are the final `String` fields and identities which the public row type
/// requires; there is no `HeadValue`/`MaterializedTrackedStateRow` staging
/// layer to drop after each scan.
async fn materialize_live_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    entries: Vec<(HeadRowIdentity, Bytes)>,
    projection: ChangeRecordProjection,
    branch_id: &str,
) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
    let branch_id = Arc::<str>::from(branch_id);
    let global = branch_id.as_ref() == crate::GLOBAL_BRANCH_ID;
    let mut json_refs = Vec::new();
    let mut deferred = Vec::new();
    let mut rows = Vec::with_capacity(entries.len());
    for (identity, bytes) in entries {
        let value = decode_head_value(&bytes)?;
        let row_index = rows.len();
        let snapshot_content = materialize_live_slot(
            !value.deleted && projection.snapshot_content,
            value.snapshot,
            &mut json_refs,
            &mut deferred,
            row_index,
            DeferredJsonField::Snapshot,
        );
        let metadata = materialize_live_slot(
            !value.deleted && projection.metadata,
            value.metadata,
            &mut json_refs,
            &mut deferred,
            row_index,
            DeferredJsonField::Metadata,
        );
        rows.push(MaterializedLiveStateRow {
            entity_pk: identity.entity_pk,
            schema_key: identity.schema_key,
            file_id: identity.file_id,
            snapshot_content,
            metadata,
            deleted: value.deleted,
            created_at: value.created_at,
            updated_at: value.updated_at,
            global,
            change_id: Some(value.change_id),
            commit_id: Some(value.commit_id),
            untracked: false,
            branch_id: Arc::clone(&branch_id),
        });
    }
    if json_refs.is_empty() {
        return Ok(rows);
    }
    let mut json_values = JsonStoreContext::new()
        .load_bytes_many(
            store,
            JsonLoadRequestRef {
                refs: &json_refs,
                scope: JsonReadScopeRef::OutOfBand,
            },
        )
        .await?
        .into_values();
    for (index, deferred) in deferred.into_iter().enumerate() {
        let bytes = json_values
            .get_mut(index)
            .ok_or_else(|| head_value_error("lost an out-of-band JSON value index"))?
            .take()
            .ok_or_else(|| {
                head_value_error(&format!(
                    "row is missing JSON payload '{}'",
                    deferred.json_ref.to_hex()
                ))
            })?;
        let json = String::from_utf8(bytes).map_err(|error| {
            head_value_error(&format!("out-of-band JSON payload is not UTF-8: {error}"))
        })?;
        let row = rows
            .get_mut(deferred.row_index)
            .ok_or_else(|| head_value_error("lost an out-of-band JSON row index"))?;
        match deferred.field {
            DeferredJsonField::Snapshot => row.snapshot_content = Some(json),
            DeferredJsonField::Metadata => row.metadata = Some(json),
        }
    }
    Ok(rows)
}

fn materialize_live_slot(
    include: bool,
    slot: HeadSlotView<'_>,
    json_refs: &mut Vec<JsonRef>,
    deferred: &mut Vec<DeferredJson>,
    row_index: usize,
    field: DeferredJsonField,
) -> Option<String> {
    if !include {
        return None;
    }
    match slot {
        HeadSlotView::None => None,
        HeadSlotView::Inline(json) => Some(json.to_string()),
        HeadSlotView::Ref(json_ref) => {
            json_refs.push(json_ref);
            deferred.push(DeferredJson {
                row_index,
                field,
                json_ref,
            });
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::{BranchHeadControl, stage_branch_head_control};
    use crate::json_store::{JsonWritePlacementRef, NormalizedJsonRef};
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    fn ts(value: &str) -> LixTimestamp {
        LixTimestamp::expect_parse("test timestamp", value)
    }

    fn identity(branch_id: &str, generation: CommitId, entity: &str) -> HeadIdentity {
        HeadIdentity {
            branch_id: branch_id.to_string(),
            generation,
            schema_key: "schema".to_string(),
            entity_pk: EntityPk::single(entity),
            file_id: None,
        }
    }

    fn head_value(change: &str, commit_id: CommitId) -> HeadValue {
        HeadValue {
            change_id: ChangeId::for_test_label(change),
            commit_id,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            snapshot: JsonSlot::from_json("{\"value\":true}"),
            metadata: JsonSlot::None,
        }
    }

    #[test]
    fn v3_value_codec_roundtrips_fixed_header_inline_and_ref_slots() {
        let snapshot_ref = JsonRef::from_hash_bytes([7; JSON_REF_BYTES]);
        let value = HeadValueRef {
            change_id: ChangeId::for_test_label("change"),
            commit_id: CommitId::for_test_label("commit"),
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-02T00:00:00Z"),
            snapshot: JsonSlotRef::Inline("{\"snapshot\":true}"),
            metadata: JsonSlotRef::Ref(&snapshot_ref),
        };

        let bytes = encode_head_value(&value).expect("encode v3 row");
        assert_eq!(bytes[0], HEAD_VALUE_VERSION);
        assert_eq!(
            bytes.len(),
            HEAD_VALUE_HEADER_BYTES + "{\"snapshot\":true}".len() + JSON_REF_BYTES
        );
        let decoded = decode_head_value(&bytes).expect("decode v3 row");
        assert_eq!(decoded.change_id, value.change_id);
        assert_eq!(decoded.commit_id, value.commit_id);
        assert_eq!(decoded.created_at, value.created_at);
        assert_eq!(decoded.updated_at, value.updated_at);
        assert_eq!(
            decoded.snapshot,
            HeadSlotView::Inline("{\"snapshot\":true}")
        );
        assert_eq!(decoded.metadata, HeadSlotView::Ref(snapshot_ref));
    }

    #[test]
    fn v6_group_codec_roundtrips_sorted_members_and_rejects_corruption() {
        let mut members = BTreeMap::new();
        members.insert(
            None,
            encode_head_value(&head_value("none", CommitId::for_test_label("head")).as_ref())
                .expect("encode none member"),
        );
        members.insert(
            Some("file-a".to_string()),
            encode_head_value(&head_value("file-a", CommitId::for_test_label("head")).as_ref())
                .expect("encode file-a member"),
        );
        members.insert(
            Some("file-b".to_string()),
            encode_head_value(&head_value("file-b", CommitId::for_test_label("head")).as_ref())
                .expect("encode file-b member"),
        );

        let encoded = encode_head_group_members(&members).expect("encode group");
        let decoded = decode_head_group_members(&encoded).expect("decode group");
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].file_id, None);
        assert_eq!(decoded[1].file_id.as_deref(), Some("file-a"));
        assert_eq!(decoded[2].file_id.as_deref(), Some("file-b"));
        assert_eq!(
            decode_head_value(&decoded[2].value)
                .expect("member should preserve v3 payload")
                .change_id,
            ChangeId::for_test_label("file-b")
        );

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(decode_head_group_members(&trailing).is_err());

        let mut bad_version = encoded;
        bad_version[0] = HEAD_GROUP_VALUE_VERSION + 1;
        assert!(decode_head_group_members(&bad_version).is_err());
    }

    #[test]
    fn working_diff_payload_equality_matches_canonical_change_identity_semantics() {
        let slot = WorkingDiffSlotFingerprint {
            kind: WORKING_DIFF_SLOT_INLINE,
            hash: [1; JSON_REF_BYTES],
        };
        let other_slot = WorkingDiffSlotFingerprint {
            kind: WORKING_DIFF_SLOT_INLINE,
            hash: [2; JSON_REF_BYTES],
        };
        let baseline = WorkingDiffVersion {
            change_id: ChangeId::for_test_label("same-change"),
            commit_id: CommitId::for_test_label("baseline-commit"),
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            snapshot: slot,
            metadata: slot,
        };
        let same_change = WorkingDiffVersion {
            snapshot: other_slot,
            metadata: other_slot,
            ..baseline
        };
        let same_payload = WorkingDiffVersion {
            change_id: ChangeId::for_test_label("different-change"),
            ..baseline
        };
        let different_payload = WorkingDiffVersion {
            change_id: ChangeId::for_test_label("different-change"),
            snapshot: other_slot,
            ..baseline
        };

        assert!(baseline.payload_eq(same_change));
        assert!(baseline.payload_eq(same_payload));
        assert!(!baseline.payload_eq(different_payload));
    }

    #[tokio::test]
    async fn working_diff_restarts_after_a_noop_checkpoint_and_verifies_coverage() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let checkpoint = CommitId::for_test_label("checkpoint");
        let first_head = CommitId::for_test_label("first-head");
        let no_op_checkpoint = CommitId::for_test_label("no-op-checkpoint");
        let second_head = CommitId::for_test_label("second-head");
        let entity_pk = EntityPk::single("row");
        let control = |head_commit_id| BranchHeadControl {
            head_commit_id,
            generation: checkpoint,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            ref_change_id: ChangeId::for_test_label("branch-ref"),
        };

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open initial write read");
        let mut writes = StorageWriteSet::new();
        let mut initial_coverage = WorkingDiffIndexCoverage::default();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit_with_working_diff(
                branch_id,
                None,
                checkpoint,
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label("initial-change"),
                    commit_id: checkpoint,
                    deleted: false,
                    created_at: ts("2026-01-01T00:00:00Z"),
                    updated_at: ts("2026-01-01T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"value\":\"one\"}"),
                    metadata: JsonSlotRef::None,
                }],
                &BTreeSet::new(),
                None,
                Some(checkpoint),
                None,
                &mut initial_coverage,
            )
            .await
            .expect("stage clean checkpoint head");
        assert_eq!(initial_coverage, WorkingDiffIndexCoverage::default());
        stage_tracked_working_diff_epoch(
            &mut writes,
            branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: checkpoint,
                generation: Some(checkpoint),
                coverage: initial_coverage,
            },
        )
        .expect("stage initial epoch");
        stage_branch_head_control(&mut writes, branch_id, control(checkpoint))
            .expect("stage initial control");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit initial head");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open first update read");
        let mut writes = StorageWriteSet::new();
        let mut first_coverage = WorkingDiffIndexCoverage::default();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit_with_working_diff(
                branch_id,
                Some(checkpoint),
                first_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label("first-change"),
                    commit_id: first_head,
                    deleted: false,
                    created_at: ts("2026-01-01T00:00:00Z"),
                    updated_at: ts("2026-01-02T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"value\":\"two\"}"),
                    metadata: JsonSlotRef::None,
                }],
                &BTreeSet::new(),
                None,
                Some(checkpoint),
                Some(checkpoint),
                &mut first_coverage,
            )
            .await
            .expect("stage first update");
        stage_tracked_working_diff_epoch(
            &mut writes,
            branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: checkpoint,
                generation: Some(checkpoint),
                coverage: first_coverage,
            },
        )
        .expect("stage first epoch");
        stage_branch_head_control(&mut writes, branch_id, control(first_head))
            .expect("stage first control");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit first update");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open first direct read");
        let first_diff = TrackedHeadContext::new()
            .reader(read)
            .working_diff_if_control_current(
                branch_id,
                control(first_head),
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("first direct diff should read")
            .expect("first direct diff should be current");
        assert_eq!(first_diff.checkpoint_commit_id, checkpoint);
        assert_eq!(first_diff.diff.entries.len(), 1);
        assert_eq!(
            first_diff.diff.entries[0].kind,
            TrackedStateDiffKind::Modified
        );
        assert_eq!(
            first_diff.diff.entries[0]
                .before
                .as_ref()
                .expect("modified diff has before row")
                .change_id,
            ChangeId::for_test_label("initial-change")
        );

        // A net-zero checkpoint keeps the existing serving generation but
        // starts a new diff epoch. Old first-before summaries stay harmless:
        // the next mutation tags its own checkpoint and gets a new index key.
        let mut writes = StorageWriteSet::new();
        stage_marker(
            &mut writes,
            branch_id,
            &TrackedHeadMarker {
                head_commit_id: no_op_checkpoint,
                generation: checkpoint,
                working_diff_checkpoint_commit_id: Some(checkpoint),
            },
        )
        .expect("stage no-op checkpoint head marker");
        stage_tracked_working_diff_epoch(
            &mut writes,
            branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: no_op_checkpoint,
                generation: None,
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )
        .expect("reset no-op checkpoint epoch");
        stage_branch_head_control(&mut writes, branch_id, control(no_op_checkpoint))
            .expect("stage no-op checkpoint control");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit no-op checkpoint");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open no-op checkpoint epoch read");
        assert_eq!(
            TrackedHeadContext::new()
                .reader(read)
                .working_diff_epoch(branch_id)
                .await
                .expect("no-op checkpoint epoch should decode"),
            Some(TrackedWorkingDiffEpoch {
                checkpoint_commit_id: no_op_checkpoint,
                generation: None,
                coverage: WorkingDiffIndexCoverage::default(),
            })
        );
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open no-op checkpoint read");
        let empty_diff = TrackedHeadContext::new()
            .reader(read)
            .working_diff_if_control_current(
                branch_id,
                control(no_op_checkpoint),
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("no-op checkpoint direct diff should read")
            .expect("no-op checkpoint direct diff should be known empty");
        assert!(empty_diff.diff.entries.is_empty());

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open second update read");
        let mut writes = StorageWriteSet::new();
        let mut second_coverage = WorkingDiffIndexCoverage::default();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit_with_working_diff(
                branch_id,
                Some(checkpoint),
                second_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label("second-change"),
                    commit_id: second_head,
                    deleted: false,
                    created_at: ts("2026-01-01T00:00:00Z"),
                    updated_at: ts("2026-01-03T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"value\":\"three\"}"),
                    metadata: JsonSlotRef::None,
                }],
                &BTreeSet::new(),
                None,
                Some(no_op_checkpoint),
                Some(no_op_checkpoint),
                &mut second_coverage,
            )
            .await
            .expect("stage second update");
        stage_tracked_working_diff_epoch(
            &mut writes,
            branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: no_op_checkpoint,
                generation: Some(checkpoint),
                coverage: second_coverage,
            },
        )
        .expect("stage second epoch");
        stage_branch_head_control(&mut writes, branch_id, control(second_head))
            .expect("stage second control");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit second update");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open second direct read");
        let second_diff = TrackedHeadContext::new()
            .reader(read)
            .working_diff_if_control_current(
                branch_id,
                control(second_head),
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("second direct diff should read")
            .expect("second direct diff should be current");
        assert_eq!(second_diff.checkpoint_commit_id, no_op_checkpoint);
        assert_eq!(second_diff.diff.entries.len(), 1);
        assert_eq!(
            second_diff.diff.entries[0].kind,
            TrackedStateDiffKind::Modified
        );
        assert_eq!(
            second_diff.diff.entries[0]
                .before
                .as_ref()
                .expect("modified diff has before row")
                .change_id,
            ChangeId::for_test_label("first-change")
        );

        let mut writes = StorageWriteSet::new();
        stage_tracked_working_diff_epoch(
            &mut writes,
            branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: no_op_checkpoint,
                generation: Some(checkpoint),
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )
        .expect("stage bad coverage epoch");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit bad coverage epoch");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open bad coverage read");
        assert!(
            TrackedHeadContext::new()
                .reader(read)
                .working_diff_if_control_current(
                    branch_id,
                    control(second_head),
                    &TrackedStateDiffRequest::default(),
                )
                .await
                .expect("bad coverage should not error")
                .is_none(),
            "bad index coverage must select canonical replay"
        );
    }

    #[tokio::test]
    async fn direct_live_materializer_honors_projection_and_batches_out_of_band_refs() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("generation");
        let head = CommitId::for_test_label("head");
        let long_metadata = format!("\"{}\"", "x".repeat(300));
        let mut writes = StorageWriteSet::new();
        let mut json_writer = JsonStoreContext::new().writer();
        let refs = json_writer
            .stage_batch(
                &mut writes,
                JsonWritePlacementRef::OutOfBand,
                [NormalizedJsonRef::new(&long_metadata)],
            )
            .expect("stage out-of-band metadata");
        let metadata_ref = refs[0];
        let row_identity = identity(branch_id, generation, "row");
        stage_put(
            &mut writes,
            &row_identity,
            &HeadValue {
                change_id: ChangeId::for_test_label("change"),
                commit_id: head,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-02T00:00:00Z"),
                snapshot: JsonSlot::from_json("{\"snapshot\":true}"),
                metadata: JsonSlot::Ref(metadata_ref),
            },
        )
        .expect("stage v3 row");
        stage_marker(
            &mut writes,
            branch_id,
            &TrackedHeadMarker {
                head_commit_id: head,
                generation,
                working_diff_checkpoint_commit_id: None,
            },
        )
        .expect("stage v3 marker");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit v3 head");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open projection read");
        let metadata_only = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &head.to_string(),
                &TrackedStateScanRequest {
                    read_columns: crate::tracked_state::TrackedStateReadColumns {
                        columns: vec!["metadata".to_string()],
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("scan v3 head")
            .expect("matching marker");
        assert_eq!(metadata_only.len(), 1);
        assert_eq!(metadata_only[0].snapshot_content, None);
        assert_eq!(
            metadata_only[0].metadata.as_deref(),
            Some(long_metadata.as_str())
        );
        assert_eq!(metadata_only[0].branch_id.as_ref(), branch_id);
        assert!(!metadata_only[0].global);
        assert!(!metadata_only[0].untracked);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open point read");
        let keys = vec![
            TrackedStateKey {
                schema_key: "schema".to_string(),
                entity_pk: EntityPk::single("row"),
                file_id: None,
            },
            TrackedStateKey {
                schema_key: "schema".to_string(),
                entity_pk: EntityPk::single("row"),
                file_id: None,
            },
        ];
        let rows = TrackedHeadContext::new()
            .reader(read)
            .load_projected_live_rows_if_current(
                branch_id,
                &head.to_string(),
                &keys,
                &ChangeRecordProjection::full(),
            )
            .await
            .expect("point read v3 head")
            .expect("matching marker");
        assert_eq!(rows.len(), 2);
        for row in rows.into_iter().flatten() {
            assert_eq!(row.snapshot_content.as_deref(), Some("{\"snapshot\":true}"));
            assert_eq!(row.metadata.as_deref(), Some(long_metadata.as_str()));
            assert_eq!(row.change_id, Some(ChangeId::for_test_label("change")));
            assert_eq!(row.commit_id, Some(head));
        }
    }

    #[tokio::test]
    async fn explicit_file_id_reads_use_single_member_projection() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let head = CommitId::for_test_label("head");
        let control = BranchHeadControl {
            head_commit_id: head,
            generation: head,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            ref_change_id: ChangeId::for_test_label("branch-ref"),
        };
        let entity_pk = EntityPk::single("row");
        let second_entity_pk = EntityPk::single("row-2");
        let deltas = [
            TrackedHeadDeltaRef {
                schema_key: "schema",
                file_id: None,
                entity_pk: &entity_pk,
                change_id: ChangeId::for_test_label("none"),
                commit_id: head,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                snapshot: JsonSlotRef::Inline("{\"value\":\"none\"}"),
                metadata: JsonSlotRef::None,
            },
            TrackedHeadDeltaRef {
                schema_key: "schema",
                file_id: Some("file-a"),
                entity_pk: &entity_pk,
                change_id: ChangeId::for_test_label("file-a"),
                commit_id: head,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                snapshot: JsonSlotRef::Inline("{\"value\":\"a\"}"),
                metadata: JsonSlotRef::None,
            },
            TrackedHeadDeltaRef {
                schema_key: "schema",
                file_id: Some("file-b"),
                entity_pk: &entity_pk,
                change_id: ChangeId::for_test_label("file-b"),
                commit_id: head,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                snapshot: JsonSlotRef::Inline("{\"value\":\"b\"}"),
                metadata: JsonSlotRef::None,
            },
            TrackedHeadDeltaRef {
                schema_key: "schema",
                file_id: Some("file-b"),
                entity_pk: &second_entity_pk,
                change_id: ChangeId::for_test_label("second-file-b"),
                commit_id: head,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                snapshot: JsonSlotRef::Inline("{\"value\":\"second-b\"}"),
                metadata: JsonSlotRef::None,
            },
        ];
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open write read");
        let mut writes = StorageWriteSet::new();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(branch_id, None, head, &deltas, &BTreeSet::new(), None)
            .await
            .expect("stage grouped head");
        stage_branch_head_control(&mut writes, branch_id, control)
            .expect("stage matching v6 control");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit grouped head");

        let group = HeadGroupIdentity {
            branch_id: branch_id.to_string(),
            generation: head,
            schema_key: "schema".to_string(),
            entity_pk: entity_pk.clone(),
        };
        let second_group = HeadGroupIdentity {
            branch_id: branch_id.to_string(),
            generation: head,
            schema_key: "schema".to_string(),
            entity_pk: second_entity_pk.clone(),
        };
        let explicit_member = HeadIdentity {
            branch_id: branch_id.to_string(),
            generation: head,
            schema_key: "schema".to_string(),
            entity_pk: entity_pk.clone(),
            file_id: Some("file-b".to_string()),
        };
        let member_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open member verification read");
        let member = PointReadPlan::new(
            TRACKED_HEAD_MEMBER_SPACE,
            &[StorageKey(Bytes::from(encode_member_key(&explicit_member)))],
        )
        .materialize(&member_read, StorageGetOptions::default())
        .await
        .expect("member projection should load")
        .value
        .into_iter()
        .next()
        .flatten();
        assert!(
            member.is_some(),
            "explicit file member needs a point record"
        );
        drop(member_read);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open logical PK read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &head.to_string(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema".to_string()],
                        entity_pks: vec![entity_pk.clone()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("logical PK read should execute")
            .expect("marker should match");
        assert_eq!(
            rows.iter()
                .map(|row| row.file_id.as_deref())
                .collect::<Vec<_>>(),
            vec![None, Some("file-a"), Some("file-b")]
        );

        // Remove only the group to prove the exact-file read never needs to
        // fetch/parse sibling members. This is intentionally an impossible
        // committed state in production; it validates the physical route.
        let mut writes = StorageWriteSet::new();
        writes.delete(
            TRACKED_HEAD_GROUP_SPACE,
            StorageKey(Bytes::from(encode_group_key(&group))),
        );
        writes.delete(
            TRACKED_HEAD_GROUP_SPACE,
            StorageKey(Bytes::from(encode_group_key(&second_group))),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("remove group for route proof");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open filtered file scan");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &head.to_string(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema".to_string()],
                        entity_pks: vec![entity_pk.clone()],
                        file_ids: vec![NullableKeyFilter::Value("file-b".to_string())],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("filtered file scan should execute")
            .expect("marker should match");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].file_id.as_deref(), Some("file-b"));

        // A schema-scoped `file_id = ?` query also stays on the member
        // projection. This is the access pattern used by filesystem-backed
        // entity scans, where the entity PK is not known before the query.
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open file-id scan");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &head.to_string(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema".to_string()],
                        file_ids: vec![NullableKeyFilter::Value("file-b".to_string())],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("file-id scan should execute")
            .expect("marker should match");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter()
                .map(|row| row.entity_pk.as_single_string().expect("single key"))
                .collect::<Vec<_>>(),
            vec!["row", "row-2"]
        );
        assert!(
            rows.iter()
                .all(|row| row.file_id.as_deref() == Some("file-b"))
        );

        // The v6 control plane only validates the generation marker. Exact
        // file identity and schema-scoped file-id reads must still route to
        // the v6 member projection even when every backing group is absent.
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open v6 file-id scan");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_control_current(
                branch_id,
                control,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema".to_string()],
                        file_ids: vec![NullableKeyFilter::Value("file-b".to_string())],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("v6 file-id scan should execute")
            .expect("matching v6 control and marker");
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .all(|row| row.file_id.as_deref() == Some("file-b"))
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open explicit file read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .load_projected_live_rows_if_current(
                branch_id,
                &head.to_string(),
                &[TrackedStateKey {
                    schema_key: "schema".to_string(),
                    entity_pk,
                    file_id: Some("file-b".to_string()),
                }],
                &ChangeRecordProjection::full(),
            )
            .await
            .expect("exact file read should execute")
            .expect("marker should match");
        assert_eq!(rows.len(), 1);
        let row = rows[0].as_ref().expect("explicit member should resolve");
        assert_eq!(row.file_id.as_deref(), Some("file-b"));
        assert_eq!(row.snapshot_content.as_deref(), Some("{\"value\":\"b\"}"));

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open v6 exact file read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .load_projected_live_rows_if_control_current(
                branch_id,
                control,
                &[TrackedStateKey {
                    schema_key: "schema".to_string(),
                    entity_pk: EntityPk::single("row"),
                    file_id: Some("file-b".to_string()),
                }],
                &ChangeRecordProjection::full(),
            )
            .await
            .expect("v6 exact file read should execute")
            .expect("matching v6 control and marker");
        assert_eq!(rows.len(), 1);
        let row = rows[0]
            .as_ref()
            .expect("v6 explicit member should resolve without its group");
        assert_eq!(row.file_id.as_deref(), Some("file-b"));
        assert_eq!(row.snapshot_content.as_deref(), Some("{\"value\":\"b\"}"));
    }

    #[test]
    fn group_and_member_keys_roundtrip_and_preserve_logical_order() {
        let generation = CommitId::for_test_label("generation");
        let strings = ["", "\0", "a", "a\0", "a\u{1}", "z", "é"];
        let mut groups = Vec::new();
        for schema_key in strings {
            for entity_first in strings {
                for entity_pk in [
                    EntityPk::single(entity_first),
                    EntityPk::from_parts(vec![entity_first.to_string(), "tail".to_string()])
                        .expect("tuple entity key should be valid"),
                ] {
                    groups.push(HeadGroupIdentity {
                        branch_id: "branch\0name".to_string(),
                        generation,
                        schema_key: schema_key.to_string(),
                        entity_pk: entity_pk.clone(),
                    });
                }
            }
        }
        groups.sort();
        groups.dedup();

        for identity in &groups {
            let encoded = encode_group_key(identity);
            assert_eq!(
                decode_group_key(&encoded).expect("group key should decode"),
                *identity
            );
            let scope = encode_scope_prefix(&identity.branch_id, identity.generation);
            assert_eq!(
                decode_group_key_in_scope(&encoded, &scope)
                    .expect("scope-decoded row key should decode"),
                HeadRowIdentity {
                    schema_key: identity.schema_key.clone(),
                    entity_pk: identity.entity_pk.clone(),
                    file_id: None,
                }
            );
        }

        let mut by_encoded = groups
            .iter()
            .cloned()
            .map(|identity| (encode_group_key(&identity), identity))
            .collect::<Vec<_>>();
        by_encoded.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            by_encoded
                .iter()
                .map(|(_, identity)| identity)
                .collect::<Vec<_>>(),
            groups.iter().collect::<Vec<_>>()
        );
        for (index, (encoded, _)) in by_encoded.iter().enumerate() {
            for (other_index, (other, _)) in by_encoded.iter().enumerate() {
                if index != other_index {
                    assert!(
                        !other.starts_with(encoded),
                        "complete row key {index} prefixes row key {other_index}"
                    );
                }
            }
        }

        let member = HeadIdentity {
            branch_id: "branch\0name".to_string(),
            generation,
            schema_key: "schema".to_string(),
            entity_pk: EntityPk::single("entity"),
            file_id: Some("file\0id".to_string()),
        };
        assert_eq!(
            decode_member_key(&encode_member_key(&member)).expect("member key should decode"),
            member
        );
    }

    #[tokio::test]
    async fn head_scan_is_logically_ordered_and_unique() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("generation");
        let head = CommitId::for_test_label("head");
        let identities = vec![
            HeadIdentity {
                branch_id: "branch".to_string(),
                generation,
                schema_key: "schema-z".to_string(),
                entity_pk: EntityPk::single("entity-a"),
                file_id: None,
            },
            HeadIdentity {
                branch_id: "branch".to_string(),
                generation,
                schema_key: "schema-a".to_string(),
                entity_pk: EntityPk::single("entity-z"),
                file_id: Some("file-a".to_string()),
            },
            HeadIdentity {
                branch_id: "branch".to_string(),
                generation,
                schema_key: "schema-a".to_string(),
                entity_pk: EntityPk::single("entity-a"),
                file_id: None,
            },
        ];
        let mut expected = identities.clone();
        expected.sort();

        let mut writes = StorageWriteSet::new();
        for (index, identity) in identities.iter().rev().enumerate() {
            stage_put(
                &mut writes,
                identity,
                &head_value(&format!("change-{index}"), head),
            )
            .expect("stage row");
        }
        stage_marker(
            &mut writes,
            "branch",
            &TrackedHeadMarker {
                head_commit_id: head,
                generation,
                working_diff_checkpoint_commit_id: None,
            },
        )
        .expect("stage marker");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit head table");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                "branch",
                &head.to_string(),
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan")
            .expect("marker should match");
        assert_eq!(rows.len(), expected.len());
        assert!(
            Arc::ptr_eq(&rows[0].branch_id, &rows[1].branch_id),
            "one head scan should share its branch allocation across rows"
        );
        assert_eq!(
            rows.into_iter()
                .map(|row| (row.schema_key, row.entity_pk, row.file_id))
                .collect::<Vec<_>>(),
            expected
                .into_iter()
                .map(|identity| (identity.schema_key, identity.entity_pk, identity.file_id))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn marker_gates_generations_and_rows_roundtrip() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("generation");
        let head = CommitId::for_test_label("head");
        let identity = identity("branch", generation, "row");
        let value = HeadValue {
            change_id: ChangeId::for_test_label("change"),
            commit_id: head,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:01Z"),
            snapshot: JsonSlot::from_json("{\"id\":\"row\"}"),
            metadata: JsonSlot::None,
        };
        let mut writes = StorageWriteSet::new();
        stage_put(&mut writes, &identity, &value).expect("stage row");
        stage_marker(
            &mut writes,
            "branch",
            &TrackedHeadMarker {
                head_commit_id: head,
                generation,
                working_diff_checkpoint_commit_id: None,
            },
        )
        .expect("stage marker");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit table");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                "branch",
                &head.to_string(),
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan")
            .expect("matching marker");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"id\":\"row\"}")
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open mismatch read");
        assert!(
            TrackedHeadContext::new()
                .reader(read)
                .scan_live_rows_if_current(
                    "branch",
                    &CommitId::for_test_label("other").to_string(),
                    &TrackedStateScanRequest::default(),
                )
                .await
                .expect("scan mismatch")
                .is_none()
        );
    }

    #[tokio::test]
    async fn incremental_commit_preserves_first_created_at() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let entity_pk = EntityPk::single("row");
        let first_head = CommitId::for_test_label("first-head");
        let second_head = CommitId::for_test_label("second-head");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open first read");
        let mut writes = StorageWriteSet::new();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                None,
                first_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label("first-change"),
                    commit_id: first_head,
                    deleted: false,
                    created_at: ts("2026-01-01T00:00:00Z"),
                    updated_at: ts("2026-01-01T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"value\":1}"),
                    metadata: JsonSlotRef::None,
                }],
                &BTreeSet::new(),
                None,
            )
            .await
            .expect("stage first head");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit first head");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open second read");
        let mut writes = StorageWriteSet::new();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                Some(first_head),
                second_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label("second-change"),
                    commit_id: second_head,
                    deleted: false,
                    created_at: ts("2026-01-02T00:00:00Z"),
                    updated_at: ts("2026-01-02T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"value\":2}"),
                    metadata: JsonSlotRef::None,
                }],
                &BTreeSet::new(),
                None,
            )
            .await
            .expect("stage second head");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit second head");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open verify read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &second_head.to_string(),
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan second head")
            .expect("matching marker");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].created_at, ts("2026-01-01T00:00:00Z"));
        assert_eq!(rows[0].updated_at, ts("2026-01-02T00:00:00Z"));
        assert_eq!(rows[0].snapshot_content.as_deref(), Some("{\"value\":2}"));
    }

    #[tokio::test]
    async fn incremental_group_merge_preserves_untouched_members_and_file_projections() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("first-head");
        let second_head = CommitId::for_test_label("second-head");
        let entity_pk = EntityPk::single("row");
        let group = HeadGroupIdentity {
            branch_id: branch_id.to_string(),
            generation,
            schema_key: "schema".to_string(),
            entity_pk: entity_pk.clone(),
        };

        let mut members = BTreeMap::new();
        members.insert(
            None,
            encode_head_value(&head_value("none-first", generation).as_ref())
                .expect("encode none member"),
        );
        members.insert(
            Some("file-a".to_string()),
            encode_head_value(&head_value("file-a-first", generation).as_ref())
                .expect("encode file-a member"),
        );
        members.insert(
            Some("file-b".to_string()),
            encode_head_value(&head_value("file-b-first", generation).as_ref())
                .expect("encode file-b member"),
        );
        let mut writes = StorageWriteSet::new();
        stage_put_group_members(&mut writes, &group, &members).expect("stage initial group");
        for (file_id, value) in &members {
            if file_id.is_some() {
                stage_put_member_bytes(&mut writes, &group, file_id.as_deref(), value)
                    .expect("stage initial explicit member");
            }
        }
        stage_marker(
            &mut writes,
            branch_id,
            &TrackedHeadMarker {
                head_commit_id: generation,
                generation,
                working_diff_checkpoint_commit_id: None,
            },
        )
        .expect("stage initial marker");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit initial group");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open incremental read");
        let mut writes = StorageWriteSet::new();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                Some(generation),
                second_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: Some("file-a"),
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label("file-a-second"),
                    commit_id: second_head,
                    deleted: false,
                    created_at: ts("2026-01-02T00:00:00Z"),
                    updated_at: ts("2026-01-02T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"value\":2}"),
                    metadata: JsonSlotRef::None,
                }],
                &BTreeSet::new(),
                None,
            )
            .await
            .expect("stage streamed update");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit streamed update");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open logical verification read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &second_head.to_string(),
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan streamed group")
            .expect("matching marker");
        assert_eq!(rows.len(), 3);
        let none = rows
            .iter()
            .find(|row| row.file_id.is_none())
            .expect("none member remains");
        let file_a = rows
            .iter()
            .find(|row| row.file_id.as_deref() == Some("file-a"))
            .expect("changed member remains");
        let file_b = rows
            .iter()
            .find(|row| row.file_id.as_deref() == Some("file-b"))
            .expect("untouched member remains");
        assert_eq!(none.change_id, Some(ChangeId::for_test_label("none-first")));
        assert_eq!(
            file_a.change_id,
            Some(ChangeId::for_test_label("file-a-second"))
        );
        assert_eq!(
            file_b.change_id,
            Some(ChangeId::for_test_label("file-b-first"))
        );
        assert_eq!(file_a.created_at, ts("2026-01-01T00:00:00Z"));
        assert_eq!(file_a.updated_at, ts("2026-01-02T00:00:00Z"));

        let identities = ["file-a", "file-b"].map(|file_id| HeadIdentity {
            branch_id: branch_id.to_string(),
            generation,
            schema_key: "schema".to_string(),
            entity_pk: entity_pk.clone(),
            file_id: Some(file_id.to_string()),
        });
        let keys = identities
            .iter()
            .map(|identity| StorageKey(Bytes::from(encode_member_key(identity))))
            .collect::<Vec<_>>();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open projection verification read");
        let projections = PointReadPlan::new(TRACKED_HEAD_MEMBER_SPACE, &keys)
            .materialize(&read, StorageGetOptions::default())
            .await
            .expect("load explicit member projections")
            .value;
        for (identity, projection) in identities.into_iter().zip(projections) {
            let value = full_value_bytes(projection.expect("projection remains present"))
                .expect("projection has full bytes");
            let value = decode_head_value(&value).expect("projection decodes");
            let expected_change = if identity.file_id.as_deref() == Some("file-a") {
                ChangeId::for_test_label("file-a-second")
            } else {
                ChangeId::for_test_label("file-b-first")
            };
            assert_eq!(value.change_id, expected_change);
        }
    }

    #[tokio::test]
    async fn incremental_group_rejects_corrupt_untouched_member_before_staging_publication() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("first-head");
        let second_head = CommitId::for_test_label("second-head");
        let entity_pk = EntityPk::single("row");
        let group = HeadGroupIdentity {
            branch_id: branch_id.to_string(),
            generation,
            schema_key: "schema".to_string(),
            entity_pk: entity_pk.clone(),
        };
        let mut members = BTreeMap::new();
        members.insert(
            None,
            encode_head_value(&head_value("none", generation).as_ref())
                .expect("encode none member"),
        );
        members.insert(
            Some("file-a".to_string()),
            encode_head_value(&head_value("file-a", generation).as_ref())
                .expect("encode file-a member"),
        );
        members.insert(
            Some("file-b".to_string()),
            encode_head_value(&head_value("file-b", generation).as_ref())
                .expect("encode file-b member"),
        );
        let mut corrupt = encode_head_group_members(&members).expect("encode corrupt base group");
        corrupt.push(0);
        let mut initial_writes = StorageWriteSet::new();
        initial_writes.put(
            TRACKED_HEAD_GROUP_SPACE,
            StorageKey(Bytes::from(encode_group_key(&group))),
            StorageValue {
                bytes: Bytes::from(corrupt),
            },
        );
        storage
            .commit_write_set(initial_writes, StorageWriteOptions::default())
            .await
            .expect("commit corrupt group fixture");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open incremental read");
        let mut writes = StorageWriteSet::new();
        let error = TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                Some(generation),
                second_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: Some("file-a"),
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label("file-a-second"),
                    commit_id: second_head,
                    deleted: false,
                    created_at: ts("2026-01-02T00:00:00Z"),
                    updated_at: ts("2026-01-02T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"value\":2}"),
                    metadata: JsonSlotRef::None,
                }],
                &BTreeSet::new(),
                None,
            )
            .await
            .expect_err("corrupt untouched member must reject the whole group");
        assert!(error.message.contains("trailing bytes"));
        assert_eq!(writes.stats().staged_puts, 0);
        assert_eq!(writes.stats().staged_deletes, 0);
    }

    #[tokio::test]
    async fn incremental_singleton_insert_rejects_existing_live_row() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("first-head");
        let second_head = CommitId::for_test_label("second-head");
        let entity_pk = EntityPk::single("row");
        let identity = identity(branch_id, generation, "row");

        let mut writes = StorageWriteSet::new();
        stage_put(
            &mut writes,
            &identity,
            &head_value("first-change", generation),
        )
        .expect("stage existing live row");
        stage_marker(
            &mut writes,
            branch_id,
            &TrackedHeadMarker {
                head_commit_id: generation,
                generation,
                working_diff_checkpoint_commit_id: None,
            },
        )
        .expect("stage existing head marker");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit existing head");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open singleton insert read");
        let mut writes = StorageWriteSet::new();
        let absence_guards = BTreeSet::from([TrackedStateKey {
            schema_key: "schema".to_string(),
            entity_pk: entity_pk.clone(),
            file_id: None,
        }]);
        let error = TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                Some(generation),
                second_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label("second-change"),
                    commit_id: second_head,
                    deleted: false,
                    created_at: ts("2026-01-02T00:00:00Z"),
                    updated_at: ts("2026-01-02T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"value\":2}"),
                    metadata: JsonSlotRef::None,
                }],
                &absence_guards,
                None,
            )
            .await
            .expect_err("singleton INSERT must reject an existing live row");
        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn incremental_guarded_insert_resurrects_tombstone_with_first_created_at() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("first-head");
        let second_head = CommitId::for_test_label("second-head");
        let entity_pk = EntityPk::single("row");
        let identity = identity(branch_id, generation, "row");

        let mut tombstone = head_value("first-delete", generation);
        tombstone.deleted = true;
        tombstone.updated_at = ts("2026-01-02T00:00:00Z");
        tombstone.snapshot = JsonSlot::None;
        tombstone.metadata = JsonSlot::None;
        let mut writes = StorageWriteSet::new();
        stage_put(&mut writes, &identity, &tombstone).expect("stage existing tombstone");
        stage_marker(
            &mut writes,
            branch_id,
            &TrackedHeadMarker {
                head_commit_id: generation,
                generation,
                working_diff_checkpoint_commit_id: None,
            },
        )
        .expect("stage existing tombstone marker");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit existing tombstone");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open guarded resurrection read");
        let mut writes = StorageWriteSet::new();
        let absence_guards = BTreeSet::from([TrackedStateKey {
            schema_key: "schema".to_string(),
            entity_pk: entity_pk.clone(),
            file_id: None,
        }]);
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                Some(generation),
                second_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label("second-insert"),
                    commit_id: second_head,
                    deleted: false,
                    created_at: ts("2026-01-03T00:00:00Z"),
                    updated_at: ts("2026-01-03T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"value\":2}"),
                    metadata: JsonSlotRef::None,
                }],
                &absence_guards,
                None,
            )
            .await
            .expect("guarded INSERT may resurrect a tombstone");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit guarded resurrection");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open resurrection verification read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &second_head.to_string(),
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan resurrected row")
            .expect("matching marker");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].created_at, ts("2026-01-01T00:00:00Z"));
        assert_eq!(rows[0].updated_at, ts("2026-01-03T00:00:00Z"));
        assert_eq!(
            rows[0].change_id,
            Some(ChangeId::for_test_label("second-insert"))
        );
        assert_eq!(rows[0].snapshot_content.as_deref(), Some("{\"value\":2}"));
    }

    #[tokio::test]
    async fn bootstrap_overlays_parent_identity_without_duplicate_write() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let entity_pk = EntityPk::single("row");
        let parent_head = CommitId::for_test_label("parent-head");
        let child_head = CommitId::for_test_label("child-head");
        let parent_rows = vec![MaterializedTrackedStateRow {
            entity_pk: entity_pk.clone(),
            schema_key: "schema".to_string(),
            file_id: None,
            snapshot_content: Some("{\"value\":1}".to_string()),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00.000Z".to_string(),
            updated_at: "2026-01-01T00:00:00.000Z".to_string(),
            change_id: ChangeId::for_test_label("parent-change"),
            commit_id: parent_head,
        }];
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open read");
        let mut writes = StorageWriteSet::new();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                None,
                child_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label("child-change"),
                    commit_id: child_head,
                    deleted: false,
                    created_at: ts("2026-01-02T00:00:00Z"),
                    updated_at: ts("2026-01-02T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"value\":2}"),
                    metadata: JsonSlotRef::None,
                }],
                &BTreeSet::new(),
                Some(parent_rows),
            )
            .await
            .expect("stage bootstrap");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("overlapping bootstrap must commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open verify read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &child_head.to_string(),
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan child head")
            .expect("matching marker");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].change_id,
            Some(ChangeId::for_test_label("child-change"))
        );
        assert_eq!(rows[0].created_at, ts("2026-01-01T00:00:00Z"));
        assert_eq!(rows[0].snapshot_content.as_deref(), Some("{\"value\":2}"));
    }

    #[tokio::test]
    async fn working_diff_gc_reclaims_malformed_auxiliary_records() {
        let storage = StorageAdapter::new(Memory::new());
        let malformed_epoch_key = StorageKey(Bytes::from_static(b"not-a-working-diff-marker"));
        let malformed_index_key = StorageKey(Bytes::from_static(b"not-a-working-diff-index"));
        let mut writes = StorageWriteSet::new();
        writes.put(
            TRACKED_WORKING_DIFF_MARKER_SPACE,
            malformed_epoch_key.clone(),
            StorageValue {
                bytes: Bytes::from_static(b"not-a-working-diff-epoch"),
            },
        );
        writes.put(
            TRACKED_WORKING_DIFF_GROUP_SPACE,
            malformed_index_key.clone(),
            StorageValue {
                bytes: Bytes::from_static(WORKING_DIFF_INDEX_VALUE),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit malformed auxiliary records");

        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open GC read"),
        );
        let mut gc_writes = StorageWriteSet::new();
        stage_collect_stale_working_diff_indexes(&read, &mut gc_writes)
            .await
            .expect("GC must discard malformed auxiliary records");
        drop(read);
        storage
            .commit_write_set(gc_writes, StorageWriteOptions::default())
            .await
            .expect("commit GC auxiliary cleanup");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open cleanup verification read");
        for (space, key) in [
            (TRACKED_WORKING_DIFF_MARKER_SPACE, malformed_epoch_key),
            (TRACKED_WORKING_DIFF_GROUP_SPACE, malformed_index_key),
        ] {
            let value = PointReadPlan::new(space, &[key])
                .materialize(&read, StorageGetOptions::default())
                .await
                .expect("read cleaned auxiliary key")
                .value
                .into_iter()
                .next()
                .flatten();
            assert!(value.is_none(), "malformed auxiliary key must be reclaimed");
        }
    }

    #[tokio::test]
    async fn working_diff_gc_keeps_only_marker_bound_active_scopes() {
        let storage = StorageAdapter::new(Memory::new());
        let timestamp = ts("2026-01-01T00:00:00Z");
        let active_generation = CommitId::for_test_label("active-generation");
        let active_checkpoint = CommitId::for_test_label("active-checkpoint");
        let active_group = HeadGroupIdentity {
            branch_id: "active".to_string(),
            generation: active_generation,
            schema_key: "schema".to_string(),
            entity_pk: EntityPk::single("active-row"),
        };
        let stale_generation = CommitId::for_test_label("stale-generation");
        let stale_checkpoint = CommitId::for_test_label("stale-checkpoint");
        let stale_group = HeadGroupIdentity {
            branch_id: "stale".to_string(),
            generation: stale_generation,
            schema_key: "schema".to_string(),
            entity_pk: EntityPk::single("stale-row"),
        };
        let orphan_generation = CommitId::for_test_label("orphan-generation");
        let orphan_checkpoint = CommitId::for_test_label("orphan-checkpoint");
        let orphan_group = HeadGroupIdentity {
            branch_id: "deleted".to_string(),
            generation: orphan_generation,
            schema_key: "schema".to_string(),
            entity_pk: EntityPk::single("orphan-row"),
        };

        let mut writes = StorageWriteSet::new();
        let active_control = BranchHeadControl {
            head_commit_id: active_generation,
            generation: active_generation,
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id: ChangeId::for_test_label("active-ref"),
        };
        stage_marker(
            &mut writes,
            "active",
            &TrackedHeadMarker {
                head_commit_id: active_generation,
                generation: active_generation,
                working_diff_checkpoint_commit_id: Some(active_checkpoint),
            },
        )
        .expect("stage active marker");
        stage_tracked_working_diff_epoch(
            &mut writes,
            "active",
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: active_checkpoint,
                generation: Some(active_generation),
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )
        .expect("stage active epoch");
        stage_branch_head_control(&mut writes, "active", active_control)
            .expect("stage active control");

        let stale_control = BranchHeadControl {
            head_commit_id: stale_generation,
            generation: stale_generation,
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id: ChangeId::for_test_label("stale-ref"),
        };
        stage_marker(
            &mut writes,
            "stale",
            &TrackedHeadMarker {
                head_commit_id: stale_generation,
                generation: stale_generation,
                working_diff_checkpoint_commit_id: Some(CommitId::for_test_label("wrong")),
            },
        )
        .expect("stage stale marker");
        stage_tracked_working_diff_epoch(
            &mut writes,
            "stale",
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: stale_checkpoint,
                generation: Some(stale_generation),
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )
        .expect("stage stale epoch");
        stage_branch_head_control(&mut writes, "stale", stale_control)
            .expect("stage stale control");

        stage_tracked_working_diff_epoch(
            &mut writes,
            "deleted",
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: orphan_checkpoint,
                generation: Some(orphan_generation),
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )
        .expect("stage orphan epoch");

        let mut coverage = WorkingDiffIndexCoverage::default();
        for (checkpoint, group) in [
            (active_checkpoint, &active_group),
            (stale_checkpoint, &stale_group),
            (orphan_checkpoint, &orphan_group),
        ] {
            stage_put_working_diff_group_index(&mut writes, &mut coverage, checkpoint, group)
                .expect("stage working-diff index");
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit working-diff GC fixture");

        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open working-diff GC read"),
        );
        let mut gc_writes = StorageWriteSet::new();
        stage_collect_stale_working_diff_indexes(&read, &mut gc_writes)
            .await
            .expect("stage working-diff GC");
        drop(read);
        storage
            .commit_write_set(gc_writes, StorageWriteOptions::default())
            .await
            .expect("commit working-diff GC");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open working-diff GC verification read");
        let active_epoch = PointReadPlan::new(
            TRACKED_WORKING_DIFF_MARKER_SPACE,
            &[StorageKey(Bytes::from(
                marker_key("active").expect("active marker key"),
            ))],
        )
        .materialize(&read, StorageGetOptions::default())
        .await
        .expect("read active epoch")
        .value
        .into_iter()
        .next()
        .flatten();
        assert!(active_epoch.is_some(), "active epoch must survive GC");

        for (branch_id, checkpoint, group) in [
            ("stale", stale_checkpoint, &stale_group),
            ("deleted", orphan_checkpoint, &orphan_group),
        ] {
            let epoch = PointReadPlan::new(
                TRACKED_WORKING_DIFF_MARKER_SPACE,
                &[StorageKey(Bytes::from(
                    marker_key(branch_id).expect("marker key"),
                ))],
            )
            .materialize(&read, StorageGetOptions::default())
            .await
            .expect("read stale epoch")
            .value
            .into_iter()
            .next()
            .flatten();
            assert!(epoch.is_none(), "inactive epoch must be reclaimed");
            let index = PointReadPlan::new(
                TRACKED_WORKING_DIFF_GROUP_SPACE,
                &[StorageKey(Bytes::from(encode_working_diff_group_key(
                    checkpoint, group,
                )))],
            )
            .materialize(&read, StorageGetOptions::default())
            .await
            .expect("read stale index")
            .value
            .into_iter()
            .next()
            .flatten();
            assert!(index.is_none(), "inactive index must be reclaimed");
        }
        let active_index = PointReadPlan::new(
            TRACKED_WORKING_DIFF_GROUP_SPACE,
            &[StorageKey(Bytes::from(encode_working_diff_group_key(
                active_checkpoint,
                &active_group,
            )))],
        )
        .materialize(&read, StorageGetOptions::default())
        .await
        .expect("read active index")
        .value
        .into_iter()
        .next()
        .flatten();
        assert!(active_index.is_some(), "active index must survive GC");
    }
}
