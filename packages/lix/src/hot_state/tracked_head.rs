//! Unified, materialized live state for one branch head.
//!
//! The V21 hot state has one authoritative file-first row per full identity
//! plus one conservative file-membership marker per schema. Each row is tagged
//! `tracked` or `untracked`: tracked mutations also enter history, while
//! untracked mutations exist only in this serving plane. Normal reads consult
//! this single row index rather than merging a tracked snapshot with an
//! untracked overlay.

mod hot;
#[cfg(test)]
pub(crate) use hot::hot_decode_row_pk_probe;

pub(crate) use crate::hot_state::HotStateReadDomain;
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) use hot::{
    BROAD_CANONICAL_CREATED_AT_HITS, BROAD_CANONICAL_CREATED_AT_KEYS,
    BROAD_CANONICAL_CREATED_AT_LOOKUPS, COMPACTED_TOMBSTONE_CANDIDATES,
    COMPACTED_TOMBSTONE_COMPACTED, COMPACTED_TOMBSTONE_OFFERED, COMPACTED_TOMBSTONE_ROUTES,
    HOT_SCAN_DECODED_ENTRIES, HOT_SCAN_MATCHED_ENTRIES, HOT_SCAN_TOMBSTONE_ENTRIES,
    INTERVAL_LOCAL_TOMBSTONE_CANDIDATES, INTERVAL_LOCAL_TOMBSTONE_ELIDED,
    INTERVAL_LOCAL_TOMBSTONE_OFFERED, INTERVAL_LOCAL_TOMBSTONE_ROUTES,
};
#[cfg(test)]
pub(crate) use hot::WORKING_DIFF_PATH_HITS;
#[cfg(test)]
pub(crate) use hot::hot_generation_scope_prefix;
pub(crate) use hot::{
    RootBaseBatchCache,
    CERTIFIED_ROW_BATCH_MANIFEST_SPACE, CERTIFIED_ROW_BATCH_PAGE_SPACE,
    CERTIFIED_ROW_BATCH_SPACE, COLLECTION_CONTROL_SPACE, CertifiedRowBatchFileRef,
    DIFF_SPACE, DeferredFreshHotPlan, DeferredFreshHotRowRef, DeferredFreshHotRows,
    RowColumnarOverlayRow, FILE_SPACE, HotIndexEntry, HotIndexValue, HotStateTransactionCache,
    HotTrackedSnapshot, INDEX_SPACE, PACKED_CURRENT_BASE_CONTROL_SPACE, PACKED_CURRENT_BASE_SPACE,
    PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE, PackedIdentityMembership, ROOT_CURRENT_BASE_SPACE,
    ROW_SPACE, load_certified_rows_at_commit, materialize_certified_root_rows,
    scan_certified_history_rows, stage_certified_row_batches, stage_hot_index_entries,
    stage_retire_hot_generation,
};

/// Stable physical address of a row in an immutable columnar base.
///
/// The owner commit is part of the address so consumers can fail closed when
/// a stale coordinate is presented against a different base.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ColumnarBaseCoordinate {
    pub(crate) base_commit_id: CommitId,
    pub(crate) group_index: u32,
    pub(crate) row_index: u32,
}

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use smallvec::SmallVec;

use crate::LixError;
use crate::NullableKeyFilter;
#[cfg(test)]
use crate::branch::stage_branch_head_control;
use crate::branch::{BranchHeadControl, BranchHeadControlContext, BranchHeadTrackedReachability};
use crate::changelog::{ChangeId, ChangeRecordProjection, CommitId};
use crate::common::{LixTimestamp, SharedStr};
use crate::row_pk::RowPk;
use crate::hot_state::{
    MaterializedHotStateBatch, MaterializedHotStateBatchBuilder, MaterializedHotStateExactBatch,
    MaterializedHotStateRow, MaterializedHotStateRowRef,
};
use crate::json_store::{
    JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonSlot, JsonSlotRef, JsonStoreContext,
};
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageBeginScanOptions, StorageCoreProjection,
    StorageGetOptions, StorageKey, StoragePrefix, StorageProjectedValue, StorageSpace,
    StorageSpaceId, StorageValue, StorageWriteSet, ValueSemantics,
};
use crate::storage_codec;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateDiff, TrackedStateDiffEntry, TrackedStateDiffIdentity,
    TrackedStateDiffKind, TrackedStateDiffRequest, TrackedStateDiffRow, TrackedStateFilter,
    TrackedStateKey, TrackedStateKeyRef, TrackedStateScanRequest,
};

pub(crate) const TRACKED_WORKING_DIFF_MARKER_NAMESPACE: &str = "hot_state.diff_marker.v16";
pub(crate) const TRACKED_WORKING_DIFF_MARKER_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_001e),
    TRACKED_WORKING_DIFF_MARKER_NAMESPACE,
    ValueSemantics::Mutable,
);

/// The active checkpoint epoch for the sparse working-diff indexes.
///
/// A current protocol marker always names the complete hot generation that
/// owns it. Older marker encodings are rejected as malformed auxiliary data,
/// which selects canonical diff replay and lets GC reclaim them.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct TrackedWorkingDiffEpoch {
    pub(crate) checkpoint_commit_id: CommitId,
    pub(crate) generation: CommitId,
    pub(crate) coverage: WorkingDiffIndexCoverage,
}

/// A tiny atomic coverage proof for the current checkpoint's sparse row index.
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

    fn remove_encoded_group_key(&mut self, key: &[u8]) -> Option<()> {
        self.group_count = self.group_count.checked_sub(1)?;
        let mut group_key_xor = *self.group_key_xor.as_hash_array();
        for (target, source) in group_key_xor.iter_mut().zip(blake3::hash(key).as_bytes()) {
            *target ^= source;
        }
        self.group_key_xor = JsonRef::from_hash_bytes(group_key_xor);
        Some(())
    }
}

/// A checkpoint-relative direct diff assembled from the current-state
/// generation.
/// This is internal plumbing for SQL working-diff and checkpoint compaction;
/// the public API remains the existing tracked-state diff representation.
pub(crate) struct TrackedWorkingDiff {
    pub(crate) checkpoint_commit_id: CommitId,
    pub(crate) diff: TrackedStateDiff,
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

/// Checkpoint-relative state carried by the authoritative hot row.
///
/// The hot diff index is deliberately only a sparse enumeration aid.  The
/// row itself owns the first-before image, so a normal tracked write can
/// decide whether it is the first mutation from the primary row it already
/// loaded for validation.  That eliminates a second point-read batch against
/// the diff index from the CRUD write path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkingDiffBaseline {
    /// No active checkpoint owns this generation.
    Disabled,
    /// This tracked row was present when the active checkpoint was published
    /// and has not changed since.
    Clean,
    /// The first mutation after the checkpoint created this identity.
    BeforeAbsent { checkpoint_commit_id: CommitId },
    /// The first mutation after the checkpoint replaced this tracked value.
    BeforePresent {
        checkpoint_commit_id: CommitId,
        version: WorkingDiffVersion,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkingDiffSlotFingerprint {
    kind: u8,
    hash: [u8; JSON_REF_BYTES],
}

const WORKING_DIFF_SLOT_NONE: u8 = 0;
const WORKING_DIFF_SLOT_REF: u8 = 1;
const WORKING_DIFF_SLOT_INLINE: u8 = 2;
/// The before image is identified by its change id, but its payload slot was
/// not materialized where the baseline was captured.
///
/// Root current bases are read under `ChangeRecordProjection::identity_only()`
/// so the write path pays no payload I/O to capture a baseline. The reader
/// hydrates the referenced change record only for the one question the change
/// id cannot answer on its own: whether two distinct changes carry the same
/// payload.
const WORKING_DIFF_SLOT_UNRESOLVED: u8 = 3;
const WORKING_DIFF_VERSION_BYTES: usize =
    16 + 16 + 1 + 8 + 8 + 1 + JSON_REF_BYTES + 1 + JSON_REF_BYTES;
const WORKING_DIFF_CHECKPOINT_BYTES: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, musli::Encode, musli::Decode)]
#[musli(packed)]
struct HeadIdentity {
    branch_id: String,
    generation: CommitId,
    schema_key: String,
    row_pk: RowPk,
    #[musli(with = storage_codec::option)]
    file_id: Option<String>,
}

/// The portion of a head-row key that varies within one branch generation.
///
/// A full table scan already constrains `branch_id` and `generation` in the
/// RocksDB prefix. Keeping that immutable scope out of every decoded row
/// avoids parsing and allocating the same two key parts 10,000 times.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct HeadRowIdentity {
    schema_key: String,
    row_pk: RowPk,
    file_id: Option<String>,
}

impl HeadIdentity {
    fn into_row_identity(self) -> HeadRowIdentity {
        HeadRowIdentity {
            schema_key: self.schema_key,
            row_pk: self.row_pk,
            file_id: self.file_id,
        }
    }
}

/// Test-only owned representation of a hot current-state row.
///
/// This exists only while a transaction is being staged. Read-side code uses
/// [`HeadValueView`], which parses the fixed header directly from RocksDB's
/// returned bytes and never builds this allocation-heavy representation.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct HeadValue {
    change_id: Option<ChangeId>,
    commit_id: Option<CommitId>,
    untracked: bool,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: JsonSlot,
    metadata: JsonSlot,
    columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
}

#[cfg(test)]
impl HeadValue {
    fn as_ref(&self) -> HeadValueRef<'_> {
        HeadValueRef {
            change_id: self.change_id,
            commit_id: self.commit_id,
            untracked: self.untracked,
            deleted: self.deleted,
            created_at: self.created_at,
            updated_at: self.updated_at,
            snapshot: self.snapshot.as_ref_slot(),
            metadata: self.metadata.as_ref_slot(),
            columnar_base_coordinate: self.columnar_base_coordinate,
            working_diff_baseline: WorkingDiffBaseline::Disabled,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HeadValueRef<'a> {
    change_id: Option<ChangeId>,
    commit_id: Option<CommitId>,
    untracked: bool,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: JsonSlotRef<'a>,
    metadata: JsonSlotRef<'a>,
    columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
    working_diff_baseline: WorkingDiffBaseline,
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

/// Zero-copy tracked mutation staged into a current-state generation.
///
/// This narrow convenience type keeps historical writers explicit. Normal
/// serving publication converts it to [`CurrentStateDeltaRef`], which is also
/// able to carry history-free untracked mutations.
#[cfg(any(test, feature = "storage-benches"))]
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedHeadDeltaRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) row_pk: &'a RowPk,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) snapshot: JsonSlotRef<'a>,
    pub(crate) metadata: JsonSlotRef<'a>,
}

#[cfg(any(test, feature = "storage-benches"))]
impl<'a> TrackedHeadDeltaRef<'a> {
    fn as_current(&self) -> CurrentStateDeltaRef<'a> {
        CurrentStateDeltaRef {
            schema_key: self.schema_key,
            file_id: self.file_id,
            row_pk: self.row_pk,
            change_id: Some(self.change_id),
            commit_id: Some(self.commit_id),
            untracked: false,
            deleted: self.deleted,
            created_at: self.created_at,
            updated_at: self.updated_at,
            snapshot: self.snapshot,
            metadata: self.metadata,
            columnar_base_coordinate: None,
        }
    }
}

/// One mutation of the authoritative current serving state.
///
/// `tracked` mutations have both IDs and may create tombstones. `untracked`
/// mutations have neither ID; deletion removes the member physically. This
/// is deliberately the single write representation for the hot state plane,
/// so callers never stage a separate untracked overlay.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CurrentStateDeltaRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) row_pk: &'a RowPk,
    pub(crate) change_id: Option<ChangeId>,
    pub(crate) commit_id: Option<CommitId>,
    pub(crate) untracked: bool,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) snapshot: JsonSlotRef<'a>,
    pub(crate) metadata: JsonSlotRef<'a>,
    pub(crate) columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
}

/// Durable exact-read evidence aligned with a transaction delta.
///
/// The branch-control CAS protects this predecessor through publication. A
/// writer may therefore reuse its encoded HOT value instead of issuing the
/// same primary and packed-base reads again during commit materialization.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CertifiedCurrentStatePredecessorRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) row_pk: &'a RowPk,
    pub(crate) value: &'a CertifiedCurrentStatePredecessor,
}

#[derive(Debug, Clone)]
pub(crate) enum CertifiedCurrentStatePredecessor {
    Encoded(Bytes),
    Packed(PackedHeadValue),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PackedHeadValue {
    change_id: ChangeId,
    commit_id: CommitId,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    working_diff_baseline: PackedWorkingDiffBaseline,
    columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
}

/// Checkpoint-relative position of a current-state base row that is served
/// without a branch-local hot row.
///
/// The two bases are not interchangeable and must not share one encoding. A
/// *packed* current base is a collection published **inside** the active
/// working interval, so its rows were absent at the checkpoint. A *root*
/// current base is the referenced head itself, so its rows **are** the
/// checkpoint state. Collapsing both onto "has an active checkpoint id" made
/// the first branch-local mutation of a checkpointed identity look like a
/// creation.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PackedWorkingDiffBaseline {
    /// No active checkpoint owns this generation.
    Disabled,
    /// Published inside the active working interval: absent at the checkpoint.
    AbsentAtCheckpoint { checkpoint_commit_id: CommitId },
    /// Served from the referenced root current base: present at the active
    /// checkpoint and unchanged since.
    CleanAtCheckpoint,
}

impl<'a> CurrentStateDeltaRef<'a> {
    fn value_ref(
        &self,
        created_at: LixTimestamp,
        working_diff_baseline: WorkingDiffBaseline,
    ) -> HeadValueRef<'a> {
        HeadValueRef {
            change_id: self.change_id,
            commit_id: self.commit_id,
            untracked: self.untracked,
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
            columnar_base_coordinate: self.columnar_base_coordinate,
            working_diff_baseline,
        }
    }

    /// Enforces the untracked-row identity invariant at head staging.
    ///
    /// Untracked rows carry identity but no history: a real, non-nil
    /// `change_id` so every row is addressable, and an absent `commit_id`
    /// because they are not members of the commit graph.
    ///
    /// This check lives here, at head staging, rather than upstream at the
    /// prepared-row funnel, and that placement is load-bearing. An experiment
    /// on this branch hard-errored inside
    /// `transaction::commit::current_state_delta_from_state_row` — the
    /// prepared-row funnel — to test whether every untracked row reaches the
    /// head through it. It does not. The probe never fired, while 169 staging
    /// failures showed **four production lanes that build untracked deltas
    /// outside that funnel entirely**: repository init (`init::plan`'s seed
    /// rows), `current_state_delta_from_engine_row`,
    /// `functions::state::stage_sequence`, and `hot::stage_hot_bootstrap`.
    /// Each already had a real id in hand and was discarding it.
    ///
    /// So the id cannot be guaranteed by any single upstream site. Head
    /// staging is the one place every lane converges, which makes this the
    /// only sound place to enforce it. Do not "simplify" this by moving the
    /// check upstream to the funnel — that model was tested and is false.
    fn validate(self) -> Result<(), LixError> {
        let untracked_id_is_usable = self
            .change_id
            .is_some_and(|change_id| !change_id.as_uuid().is_nil());
        match (self.untracked, self.change_id, self.commit_id) {
            (false, Some(_), Some(_)) => Ok(()),
            (true, _, None) if untracked_id_is_usable => Ok(()),
            (false, _, _) => Err(head_value_error(
                "tracked current-state mutation must carry change_id and commit_id",
            )),
            (true, _, _) => Err(head_value_error(
                "untracked current-state mutation must carry a non-nil change_id and no commit_id",
            )),
        }
    }

    fn physically_deletes(self) -> bool {
        self.untracked && self.deleted
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
    pub(crate) fn reader<S>(&self, store: S) -> hot::HotStateStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        hot::HotStateStoreReader {
            store,
            transaction_cache: None,
            root_base_cache: None,
        }
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn transaction_reader<S>(
        &self,
        store: S,
        cache: Arc<HotStateTransactionCache>,
    ) -> hot::HotStateStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        hot::HotStateStoreReader {
            store,
            transaction_cache: Some(cache),
            root_base_cache: None,
        }
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn writer<'a, S>(
        &'a self,
        store: &'a S,
        writes: &'a mut StorageWriteSet,
    ) -> hot::HotStateWriter<'a, S>
    where
        S: StorageAdapterRead + ?Sized,
    {
        // `transaction_global_schema_keys: None` is the safe default: it
        // disables serving-view tombstone compaction. Only a caller holding
        // the transaction's complete prepared inputs may relax it, through
        // `HotStateWriter::with_transaction_global_schema_keys`.
        hot::HotStateWriter {
            store,
            writes,
            transaction_global_schema_keys: None,
        }
    }
}

impl<S> hot::HotStateWriter<'_, S>
where
    S: StorageAdapterRead + ?Sized,
{
    /// Stages history-free untracked deltas into the branch's one serving
    /// generation.
    ///
    /// Untracked rows share the generation with tracked rows and are separated
    /// only by their per-row flag, so this is an ordinary in-place hot
    /// mutation: no new generation UUID, no republished snapshot, no commit.
    /// The caller still advances `current_state_revision` so the branch
    /// control CAS remains a real write fence.
    ///
    /// Untracked deltas never produce working-diff records (their baseline is
    /// always `Disabled`), so the checkpoint context and the coverage counter
    /// are deliberately not threaded through here.
    pub(crate) async fn stage_untracked_current_state(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
    ) -> Result<(), LixError> {
        if deltas.is_empty() {
            return Ok(());
        }
        let mut coverage = WorkingDiffIndexCoverage::default();
        // Boxed for the same reason the tracked staging call sites are: this
        // sits on the commit path, and leaving the future inline pushes
        // auto-trait resolution over the recursion limit for callers that
        // require `Send`.
        Box::pin(self.stage_current_state_with_working_diff(
            branch_id,
            Some(generation),
            generation,
            deltas,
            absence_guards,
            None,
            None,
            None,
            &mut coverage,
        ))
        .await?;
        Ok(())
    }
}

fn current_state_duplicate_delta_error(delta: &CurrentStateDeltaRef<'_>) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "current-state commit contains duplicate mutation for schema '{}' row_pk '{:?}' file_id '{:?}'",
            delta.schema_key, delta.row_pk, delta.file_id
        ),
    )
}

fn reject_guarded_live_member(
    absence_guards: &BTreeSet<TrackedStateKey>,
    delta: &CurrentStateDeltaRef<'_>,
    existing: HeadValueView<'_>,
) -> Result<(), LixError> {
    if absence_guards.is_empty() || existing.deleted {
        return Ok(());
    }
    let key = TrackedStateKey {
        schema_key: delta.schema_key.to_string(),
        row_pk: delta.row_pk.clone(),
        file_id: delta.file_id.map(str::to_string),
    };
    if absence_guards.contains(&key) {
        return Err(tracked_head_duplicate_insert_error(&key));
    }
    Ok(())
}

/// Checks a sorted zero-copy INSERT guard selection.
///
/// Normal transaction publication carries INSERT intent as prepared-row
/// ordinals. Lowering those ordinals to borrowed key views avoids allocating
/// one owned key and one tree node per mutation merely to test the matching
/// current-state delta.
fn reject_borrowed_guarded_live_member(
    absence_guards: &[TrackedStateKeyRef<'_>],
    delta: &CurrentStateDeltaRef<'_>,
    existing: HeadValueView<'_>,
) -> Result<(), LixError> {
    if absence_guards.is_empty() || existing.deleted {
        return Ok(());
    }
    let guarded = absence_guards
        .binary_search_by(|guard| {
            guard
                .schema_key
                .cmp(delta.schema_key)
                .then_with(|| guard.row_pk.cmp(delta.row_pk))
                .then_with(|| guard.file_id.cmp(&delta.file_id))
        })
        .is_ok();
    if guarded {
        return Err(tracked_head_duplicate_insert_error_ref(
            delta.schema_key,
            delta.row_pk,
        ));
    }
    Ok(())
}

/// Retention is an identity property, not a mutable value column. An UPDATE
/// is planned against the current row and therefore preserves it; an INSERT
/// finding an existing identity is rejected by `absence_guards` above. This
/// additional fence makes an accidental tracked↔untracked promotion fail
/// closed even on an internal write path that did not originate in SQL.
fn reject_retention_change(
    delta: &CurrentStateDeltaRef<'_>,
    existing: HeadValueView<'_>,
) -> Result<(), LixError> {
    // A tracked tombstone is still the durable identity owner. Letting an
    // untracked member overwrite it would erase the tracked checkpoint
    // baseline and make a later diff silently miss the removal. Retention is
    // therefore immutable while any physical member exists; untracked delete
    // removes its member entirely, after which a new tracked insert is a new
    // identity and cannot affect historical diff state.
    if existing.untracked != delta.untracked {
        if existing.untracked {
            return Err(LixError::new(
                LixError::CODE_UNIQUE,
                format!(
                    "cannot insert tracked row in schema '{}' row_pk {:?}: a canonical untracked row already exists; delete it first",
                    delta.schema_key, delta.row_pk,
                ),
            ));
        }
        return Err(LixError::new(
            LixError::CODE_UNIQUE,
            format!(
                "cannot change retention for existing current-state row in schema '{}' row_pk {:?}; delete it before inserting it as {}",
                delta.schema_key,
                delta.row_pk,
                if delta.untracked {
                    "untracked"
                } else {
                    "tracked"
                },
            ),
        ));
    }
    Ok(())
}

fn tracked_head_duplicate_insert_error(key: &TrackedStateKey) -> LixError {
    tracked_head_duplicate_insert_error_ref(&key.schema_key, &key.row_pk)
}

fn tracked_head_duplicate_insert_error_ref(schema_key: &str, row_pk: &RowPk) -> LixError {
    let row_pk = row_pk
        .as_json_array_text()
        .unwrap_or_else(|_| "<invalid row_pk>".to_string());
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "primary-key constraint violation on schema '{}': INSERT would duplicate row_pk '{row_pk}'",
            schema_key
        ),
    )
}

fn matches_filter(identity: &HeadRowIdentity, filter: &TrackedStateFilter) -> bool {
    (filter.schema_keys.is_empty() || filter.schema_keys.contains(&identity.schema_key))
        && (filter.row_pks.is_empty() || filter.row_pks.contains(&identity.row_pk))
        && matches_file_filter(identity.file_id.as_ref(), &filter.file_ids)
}

fn matches_file_filter(file_id: Option<&String>, filters: &[NullableKeyFilter<String>]) -> bool {
    filters.is_empty()
        || filters.iter().any(|filter| match filter {
            NullableKeyFilter::Any => true,
            NullableKeyFilter::Null => file_id.is_none(),
            NullableKeyFilter::Value(value) => file_id == Some(value),
        })
}

#[cfg(test)]
fn stage_test_current_control(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    head_commit_id: CommitId,
    generation: CommitId,
    working_diff_checkpoint_commit_id: Option<CommitId>,
) -> Result<(), LixError> {
    let timestamp = LixTimestamp::expect_parse(
        "tracked-head test control timestamp",
        "2026-01-01T00:00:00Z",
    );
    stage_branch_head_control(
        writes,
        branch_id,
        BranchHeadControl {
            head_commit_id,
            tracked_generation: generation,
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id,
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id: ChangeId::for_test_label("tracked-head-test-control"),
            accelerator_root_set_digest:
                crate::tracked_state::accelerator_root_set_digest(None)?,
        },
    )
}

/// Publishes the checkpoint epoch that owns the sparse working-diff indexes.
/// The surrounding branch-control CAS makes this marker, the current hot rows,
/// and the current branch head one atomic visibility boundary.
pub(crate) fn stage_tracked_working_diff_epoch(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    epoch: TrackedWorkingDiffEpoch,
) -> Result<(), LixError> {
    writes.put(
        TRACKED_WORKING_DIFF_MARKER_SPACE,
        StorageKey(Bytes::from(working_diff_marker_key(branch_id)?)),
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
        &[StorageKey(Bytes::from(working_diff_marker_key(branch_id)?))],
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
    hot::stage_collect_stale_hot_diff_records(store, writes, &active).await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ActiveWorkingDiffScope {
    checkpoint_commit_id: CommitId,
    generation: CommitId,
}

/// Validates and keeps only auxiliary epochs that are presently bound by the
/// authoritative branch control. Broken auxiliary bytes are reclaimed here
/// rather than turning background GC into a retry loop; normal readers already
/// select canonical replay for the same cases.
async fn stage_active_working_diff_scopes<S>(
    store: &S,
    writes: &mut StorageWriteSet,
    controls: &BTreeMap<String, BranchHeadControl>,
) -> Result<BTreeMap<String, ActiveWorkingDiffScope>, LixError>
where
    S: StorageAdapterRead + Clone,
{
    let range = StoragePrefix {
        bytes: Bytes::new(),
    }
    .to_range()?;
    let mut active = BTreeMap::new();
    let mut cursor = store
        .begin_scan(
            TRACKED_WORKING_DIFF_MARKER_SPACE,
            range,
            StorageBeginScanOptions::default(),
        )
        .await?;
    loop {
        let (page, page_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in page {
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
            let valid = epoch.generation == control.tracked_generation
                && control.working_diff_checkpoint_commit_id == Some(epoch.checkpoint_commit_id);
            if !valid || active.contains_key(&key.branch_id) {
                writes.delete(TRACKED_WORKING_DIFF_MARKER_SPACE, entry.key);
                continue;
            }
            active.insert(
                key.branch_id,
                ActiveWorkingDiffScope {
                    checkpoint_commit_id: epoch.checkpoint_commit_id,
                    generation: epoch.generation,
                },
            );
        }
        if !page_has_more {
            break;
        }
    }
    Ok(active)
}

#[cfg(test)]
fn stage_put(
    writes: &mut StorageWriteSet,
    identity: &HeadIdentity,
    value: &HeadValue,
) -> Result<(), LixError> {
    hot::stage_test_hot_value(writes, identity, value)
}

fn working_diff_marker_key(branch_id: &str) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("tracked working-diff marker key", &BranchRef { branch_id })
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

// The key byte format is shared with the tracked-state tree and lives in one
// place; see `crate::order_preserving_key`. Re-exported so this module's
// children keep resolving these names through `use super::*`.
pub(crate) use crate::order_preserving_key::*;

const GENERATION_BYTES: usize = 16;

/// Order-preserving tracked-head key encoding.
///
/// The head table is the normal read serving index, so its storage ordering is
/// also the visible row ordering: `(branch, generation, schema, file,
/// row)` - see `encode_hot_row_key_parts`, which writes the file id before
/// the row primary key. Musli's storage encoding is excellent for values and structural
/// prefixes, but length-prefixed strings do not preserve lexical order. This
/// codec retains exact prefix scans while making every table scan already
/// ordered and duplicate-free for one branch generation.
fn encode_scope_prefix(branch_id: &str, generation: CommitId) -> Vec<u8> {
    let mut out = Vec::with_capacity(branch_id.len() + 2 + GENERATION_BYTES);
    write_key_string(&mut out, branch_id, KEY_PART_FINAL);
    out.extend_from_slice(generation.as_uuid().as_bytes());
    out
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

/// Test-only shim so the shared codec's three-way differential can drive this
/// plane's decoder. See `crate::order_preserving_key::tests`.
#[cfg(test)]
pub(crate) fn head_decode_row_pk_probe(bytes: &[u8]) -> Option<(RowPk, usize)> {
    let mut offset = 0usize;
    read_row_pk(bytes, &mut offset)
        .ok()
        .map(|row_pk| (row_pk, offset))
}

fn read_row_pk(bytes: &[u8], offset: &mut usize) -> Result<RowPk, LixError> {
    let version = bytes
        .get(*offset)
        .copied()
        .ok_or_else(|| key_codec_error("is truncated before row primary key version"))?;
    *offset += 1;
    if version != ROW_PK_CODEC_V1 {
        return Err(key_codec_error(&format!(
            "has unsupported row primary key codec version {version}"
        )));
    }
    let mut components = SmallVec::new();
    loop {
        let (part, terminator) = read_row_pk_part(bytes, offset)?;
        components.push(part);
        match terminator {
            KEY_PART_FINAL => break,
            KEY_PART_MORE => {}
            _ => {
                return Err(key_codec_error(
                    "row primary key has an invalid terminator",
                ));
            }
        }
    }
    RowPk::from_components(components).map_err(|error| {
        key_codec_error(&format!("contains an invalid row primary key: {error}"))
    })
}

fn read_row_pk_part(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<(crate::row_pk::RowPkComponent, u8), LixError> {
    let tag = bytes
        .get(*offset)
        .copied()
        .ok_or_else(|| key_codec_error("is truncated before row primary key part tag"))?;
    *offset += 1;
    match tag {
        ROW_PK_STRING => {
            let (value, terminator) = read_key_string(bytes, offset, "row primary key")?;
            Ok((
                crate::row_pk::RowPkComponent::String(value.into()),
                terminator,
            ))
        }
        ROW_PK_BYTES => {
            let (value, terminator) = read_key_bytes(bytes, offset, "row primary key bytes")?;
            Ok((
                crate::row_pk::RowPkComponent::Bytes(value.into()),
                terminator,
            ))
        }
        ROW_PK_UUID => {
            let uuid_end = offset
                .checked_add(ROW_PK_UUID_BYTES)
                .ok_or_else(|| key_codec_error("UUIDv7 row primary key offset overflow"))?;
            let uuid_bytes: [u8; 16] = bytes
                .get(*offset..uuid_end)
                .ok_or_else(|| key_codec_error("is truncated in UUIDv7 row primary key"))?
                .try_into()
                .expect("UUIDv7 slice has fixed length");
            let terminator = bytes
                .get(uuid_end)
                .copied()
                .ok_or_else(|| key_codec_error("is truncated after UUIDv7 row primary key"))?;
            if !is_key_part_terminator(terminator) {
                return Err(key_codec_error(
                    "UUIDv7 row primary key has an invalid terminator",
                ));
            }
            *offset = uuid_end + 1;
            Ok((
                crate::row_pk::RowPkComponent::Uuid(uuid_bytes),
                terminator,
            ))
        }
        ROW_PK_INTEGER => {
            let integer_end = offset
                .checked_add(ROW_PK_INTEGER_BYTES)
                .ok_or_else(|| key_codec_error("integer row primary key offset overflow"))?;
            let ordered = u64::from_be_bytes(
                bytes
                    .get(*offset..integer_end)
                    .ok_or_else(|| key_codec_error("is truncated in integer row primary key"))?
                    .try_into()
                    .expect("integer slice has fixed length"),
            );
            let terminator = bytes
                .get(integer_end)
                .copied()
                .ok_or_else(|| key_codec_error("is truncated after integer row primary key"))?;
            if !is_key_part_terminator(terminator) {
                return Err(key_codec_error(
                    "integer row primary key has an invalid terminator",
                ));
            }
            *offset = integer_end + 1;
            Ok((
                crate::row_pk::RowPkComponent::Integer(i64_from_ordered_integer(ordered)),
                terminator,
            ))
        }
        _ => Err(key_codec_error(
            "has an unknown row primary key part tag",
        )),
    }
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
    let (value, terminator) = read_key_bytes(bytes, offset, field)?;
    let value = String::from_utf8(value).map_err(|error| {
        key_codec_error(&format!("{field} is not UTF-8: {}", error.utf8_error()))
    })?;
    Ok((value, terminator))
}

/// Maps the shared scanner's structured error into this plane's vocabulary, so
/// unifying the scanner did not unify the error text.
fn head_key_part_error(error: KeyPartError, field: &str) -> LixError {
    match error {
        KeyPartError::Truncated => key_codec_error(&format!("is truncated in {field}")),
        KeyPartError::EscapeTruncated => key_codec_error(&format!("is truncated after {field}")),
        KeyPartError::UnknownEscape(_) => {
            key_codec_error(&format!("{field} has an invalid terminator"))
        }
    }
}

fn read_key_bytes(
    bytes: &[u8],
    offset: &mut usize,
    field: &str,
) -> Result<(Vec<u8>, u8), LixError> {
    let part = scan_key_part(bytes, *offset).map_err(|error| head_key_part_error(error, field))?;
    *offset = part.end;
    let value = match part.value {
        ScannedKeyValue::Verbatim(range) => bytes[range].to_vec(),
        ScannedKeyValue::Unescaped(value) => value,
    };
    Ok((value, part.terminator))
}

fn key_codec_error(message: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("invalid tracked-head row key: {message}"),
    )
}

/// Fixed-width fingerprints embedded in the V5 hot-row checkpoint baseline.
///
/// The sparse hot-diff index stores keys only.  The corresponding authoritative
/// hot row stores the first tracked before-image, which keeps the accelerator
/// compact without requiring a second write-path point read.
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

fn decode_working_diff_checkpoint(bytes: &[u8], offset: &mut usize) -> Result<CommitId, LixError> {
    Ok(CommitId::new(uuid_from_working_diff_bytes(
        take_working_diff_bytes(bytes, offset, WORKING_DIFF_CHECKPOINT_BYTES)?,
        "checkpoint commit id",
    )?))
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
        .ok_or_else(|| working_diff_error("deletion flag is missing"))?
    {
        0 => false,
        1 => true,
        _ => return Err(working_diff_error("deletion flag is invalid")),
    };
    let created_at = LixTimestamp::from_packed(read_u64(
        take_working_diff_bytes(payload, &mut field_offset, 8)?,
        "created_at",
    )?)
    .map_err(|error| working_diff_error(&format!("invalid created_at: {error}")))?;
    let updated_at = LixTimestamp::from_packed(read_u64(
        take_working_diff_bytes(payload, &mut field_offset, 8)?,
        "updated_at",
    )?)
    .map_err(|error| working_diff_error(&format!("invalid updated_at: {error}")))?;
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
        .ok_or_else(|| working_diff_error(&format!("{field} kind is missing")))?;
    if !matches!(
        kind,
        WORKING_DIFF_SLOT_NONE
            | WORKING_DIFF_SLOT_REF
            | WORKING_DIFF_SLOT_INLINE
            | WORKING_DIFF_SLOT_UNRESOLVED
    ) {
        return Err(working_diff_error(&format!("{field} slot kind is invalid")));
    }
    let hash: [u8; JSON_REF_BYTES] = take_working_diff_bytes(bytes, offset, JSON_REF_BYTES)?
        .try_into()
        .map_err(|_| working_diff_error(&format!("{field} hash is invalid")))?;
    if matches!(kind, WORKING_DIFF_SLOT_NONE | WORKING_DIFF_SLOT_UNRESOLVED)
        && hash != [0; JSON_REF_BYTES]
    {
        return Err(working_diff_error(&format!(
            "{field} slot kind must have a zero hash"
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
        .ok_or_else(|| working_diff_error("value offset overflow"))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| working_diff_error("value is truncated"))?;
    *offset = end;
    Ok(value)
}

fn uuid_from_working_diff_bytes(bytes: &[u8], field: &str) -> Result<uuid::Uuid, LixError> {
    let bytes: [u8; UUID_BYTES] = bytes
        .try_into()
        .map_err(|_| working_diff_error(&format!("{field} has invalid width")))?;
    Ok(uuid::Uuid::from_bytes(bytes))
}

fn working_diff_error(message: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("invalid hot working-diff version: {message}"),
    )
}

/// Current-state values are intentionally a small, fixed-header wire record rather
/// than a general Musli struct. The normal read path needs only these fields,
/// and decoding a Musli `JsonSlot` first allocated an intermediate value for
/// every row before it was copied into a live-state row.
///
/// ```text
///  0      format version (9)
///  1      deleted + untracked + snapshot/metadata kinds + diff baseline kind
///  2..18  change UUID
/// 18..34  commit UUID
/// 34..42  created_at packed timestamp (big endian)
/// 42..50  updated_at packed timestamp (big endian)
/// 50..54  snapshot payload byte length (big endian u32)
/// 54..58  metadata payload byte length (big endian u32)
/// 58      columnar base-coordinate presence (0 or 1)
/// 59..    snapshot payload, metadata payload, then an optional fixed
///          checkpoint before-image, then an optional 24-byte base coordinate
/// ```
///
/// Slot payloads are either inline UTF-8 JSON, a fixed 32-byte `JsonRef`, or
/// that same fingerprint followed by inline JSON for dirty working-diff rows.
/// Persisting fingerprints only while a row is dirty keeps repeated diff
/// classification independent of payload size without taxing checkpointed
/// current state.
const HEAD_VALUE_VERSION: u8 = 9;
const HEAD_VALUE_HEADER_BYTES: usize = 59;
const COLUMNAR_BASE_COORDINATE_BYTES: usize = 16 + 4 + 4;
const HEAD_VALUE_DELETED: u8 = 0b0000_0001;
const HEAD_VALUE_SNAPSHOT_SHIFT: u8 = 1;
const HEAD_VALUE_METADATA_SHIFT: u8 = 3;
const HEAD_VALUE_UNTRACKED: u8 = 0b0010_0000;
const HEAD_VALUE_WORKING_DIFF_SHIFT: u8 = 6;
const HEAD_VALUE_SLOT_MASK: u8 = 0b11;
const HEAD_VALUE_WORKING_DIFF_MASK: u8 = 0b11;
const HEAD_SLOT_NONE: u8 = 0;
const HEAD_SLOT_REF: u8 = 1;
const HEAD_SLOT_INLINE: u8 = 2;
const HEAD_SLOT_INLINE_FINGERPRINTED: u8 = 3;
const HEAD_WORKING_DIFF_DISABLED: u8 = 0;
const HEAD_WORKING_DIFF_CLEAN: u8 = 1;
const HEAD_WORKING_DIFF_BEFORE_ABSENT: u8 = 2;
const HEAD_WORKING_DIFF_BEFORE_PRESENT: u8 = 3;
const UUID_BYTES: usize = 16;
const JSON_REF_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeadSlotView<'a> {
    None,
    Ref(JsonRef),
    Inline(&'a str),
    InlineFingerprinted { json_ref: JsonRef, json: &'a str },
}

#[derive(Debug, Clone, Copy)]
struct HeadValueView<'a> {
    change_id: Option<ChangeId>,
    commit_id: Option<CommitId>,
    untracked: bool,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: HeadSlotView<'a>,
    metadata: HeadSlotView<'a>,
    columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
    working_diff_baseline: WorkingDiffBaseline,
    /// False when the JSON slots above are placeholders because this view was
    /// synthesized from a current-state base that was read without payload
    /// projection. Baselines captured from such a view record their payload
    /// slots as unresolved and are hydrated by the reader on demand.
    payload_slots_materialized: bool,
}

impl CertifiedCurrentStatePredecessor {
    pub(crate) fn created_at(&self) -> Result<LixTimestamp, LixError> {
        Ok(self.view()?.created_at)
    }

    fn view(&self) -> Result<HeadValueView<'_>, LixError> {
        match self {
            Self::Encoded(bytes) => decode_head_value(bytes),
            Self::Packed(value) => Ok(HeadValueView {
                change_id: Some(value.change_id),
                commit_id: Some(value.commit_id),
                untracked: false,
                deleted: value.deleted,
                created_at: value.created_at,
                updated_at: value.updated_at,
                // Current-state bases are read without payload projection, so
                // these slots are placeholders. The change id above is the
                // reference the reader hydrates from when it needs the payload.
                snapshot: HeadSlotView::None,
                metadata: HeadSlotView::None,
                payload_slots_materialized: false,
                columnar_base_coordinate: value.columnar_base_coordinate,
                working_diff_baseline: match value.working_diff_baseline {
                    PackedWorkingDiffBaseline::Disabled => WorkingDiffBaseline::Disabled,
                    PackedWorkingDiffBaseline::AbsentAtCheckpoint {
                        checkpoint_commit_id,
                    } => WorkingDiffBaseline::BeforeAbsent {
                        checkpoint_commit_id,
                    },
                    PackedWorkingDiffBaseline::CleanAtCheckpoint => WorkingDiffBaseline::Clean,
                },
            }),
        }
    }
}

impl HeadValueView<'_> {
    fn working_diff_version(self) -> Option<WorkingDiffVersion> {
        Some(WorkingDiffVersion {
            change_id: self.change_id?,
            commit_id: self.commit_id?,
            deleted: self.deleted,
            created_at: self.created_at,
            updated_at: self.updated_at,
            snapshot: self.working_diff_slot(self.snapshot),
            metadata: self.working_diff_slot(self.metadata),
        })
    }

    fn working_diff_slot(self, slot: HeadSlotView<'_>) -> WorkingDiffSlotFingerprint {
        if self.payload_slots_materialized {
            working_diff_slot_fingerprint(slot)
        } else {
            WorkingDiffSlotFingerprint::unresolved()
        }
    }
}

impl WorkingDiffSlotFingerprint {
    fn unresolved() -> Self {
        Self {
            kind: WORKING_DIFF_SLOT_UNRESOLVED,
            hash: [0; JSON_REF_BYTES],
        }
    }

    fn none() -> Self {
        Self {
            kind: WORKING_DIFF_SLOT_NONE,
            hash: [0; JSON_REF_BYTES],
        }
    }
}

fn working_diff_checkpoint_owner(baseline: WorkingDiffBaseline) -> Option<CommitId> {
    match baseline {
        WorkingDiffBaseline::BeforeAbsent {
            checkpoint_commit_id,
        }
        | WorkingDiffBaseline::BeforePresent {
            checkpoint_commit_id,
            ..
        } => Some(checkpoint_commit_id),
        WorkingDiffBaseline::Disabled | WorkingDiffBaseline::Clean => None,
    }
}

fn effective_hot_commit_id(
    value: HeadValueView<'_>,
    active_checkpoint_commit_id: Option<CommitId>,
) -> Option<CommitId> {
    let commit_id = value.commit_id?;
    match (
        active_checkpoint_commit_id,
        working_diff_checkpoint_owner(value.working_diff_baseline),
    ) {
        (Some(active), Some(owner)) if owner != active => Some(active),
        _ => Some(commit_id),
    }
}

/// Returns the checkpoint-canonical creation timestamp without rewriting the
/// current row when an epoch rotates.
///
/// A row first created in the previous interval carries `BeforeAbsent` and is
/// canonicalized by checkpoint history to its change timestamp. Rows that
/// existed at the checkpoint retain their original creation timestamp.
fn effective_hot_created_at(
    value: HeadValueView<'_>,
    active_checkpoint_commit_id: Option<CommitId>,
) -> LixTimestamp {
    match (
        active_checkpoint_commit_id,
        value.working_diff_baseline,
    ) {
        (
            Some(active),
            WorkingDiffBaseline::BeforeAbsent {
                checkpoint_commit_id,
            },
        ) if checkpoint_commit_id != active => value.updated_at,
        _ => value.created_at,
    }
}

/// Whether two working-diff versions carry the same payload.
///
/// `Unresolved` is deliberately a distinct answer rather than a default. An
/// accelerator over the canonical diff may answer correctly or decline, but it
/// must never answer confidently and wrongly, so a caller that has not
/// hydrated an unresolved before image cannot accidentally read it as
/// "different".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkingDiffPayloadEquality {
    Equal,
    Different,
    Unresolved,
}

impl WorkingDiffVersion {
    /// True when this version's payload slots were never materialized, so only
    /// its change id is known. Resolve with `resolve_payload_slots` before
    /// classifying.
    fn payload_is_unresolved(self) -> bool {
        self.snapshot.kind == WORKING_DIFF_SLOT_UNRESOLVED
            || self.metadata.kind == WORKING_DIFF_SLOT_UNRESOLVED
    }

    fn resolve_payload_slots(
        &mut self,
        snapshot: WorkingDiffSlotFingerprint,
        metadata: WorkingDiffSlotFingerprint,
    ) {
        self.snapshot = snapshot;
        self.metadata = metadata;
    }

    fn payload_equality(self, other: Self) -> WorkingDiffPayloadEquality {
        // Keep the accelerator's net-change classification identical to the
        // canonical tracked diff: a shared change record is intrinsically the
        // same payload, otherwise compare the two stored payload slots.
        if self.change_id == other.change_id {
            return WorkingDiffPayloadEquality::Equal;
        }
        if self.payload_is_unresolved() || other.payload_is_unresolved() {
            return WorkingDiffPayloadEquality::Unresolved;
        }
        if self.snapshot == other.snapshot && self.metadata == other.metadata {
            WorkingDiffPayloadEquality::Equal
        } else {
            WorkingDiffPayloadEquality::Different
        }
    }

    fn into_diff_row(self, identity: TrackedStateDiffIdentity) -> TrackedStateDiffRow {
        TrackedStateDiffRow {
            identity,
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
        HeadSlotView::InlineFingerprinted { json_ref, .. } => WorkingDiffSlotFingerprint {
            kind: WORKING_DIFF_SLOT_INLINE,
            hash: *json_ref.as_hash_array(),
        },
    }
}

#[derive(Debug, Clone, Copy)]
enum HeadSlotEncode<'a> {
    None,
    Ref(JsonRef),
    Inline {
        json_ref: Option<JsonRef>,
        json: &'a str,
    },
}

impl<'a> From<JsonSlotRef<'a>> for HeadSlotEncode<'a> {
    fn from(value: JsonSlotRef<'a>) -> Self {
        match value {
            JsonSlotRef::None => Self::None,
            JsonSlotRef::Ref(value) => Self::Ref(*value),
            JsonSlotRef::Inline(json) => Self::Inline {
                json_ref: None,
                json,
            },
        }
    }
}

impl<'a> From<HeadSlotView<'a>> for HeadSlotEncode<'a> {
    fn from(value: HeadSlotView<'a>) -> Self {
        match value {
            HeadSlotView::None => Self::None,
            HeadSlotView::Ref(value) => Self::Ref(value),
            HeadSlotView::Inline(json) => Self::Inline {
                json_ref: None,
                json,
            },
            HeadSlotView::InlineFingerprinted { json_ref, json } => Self::Inline {
                json_ref: Some(json_ref),
                json,
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HeadValueEncode<'a> {
    change_id: Option<ChangeId>,
    commit_id: Option<CommitId>,
    untracked: bool,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: HeadSlotEncode<'a>,
    metadata: HeadSlotEncode<'a>,
    columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
    working_diff_baseline: WorkingDiffBaseline,
}

fn encode_head_value(value: &HeadValueRef<'_>) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    append_head_value(&mut bytes, value)?;
    Ok(bytes)
}

fn append_head_value(
    bytes: &mut Vec<u8>,
    value: &HeadValueRef<'_>,
) -> Result<std::ops::Range<usize>, LixError> {
    append_head_value_parts(
        bytes,
        HeadValueEncode {
            change_id: value.change_id,
            commit_id: value.commit_id,
            untracked: value.untracked,
            deleted: value.deleted,
            created_at: value.created_at,
            updated_at: value.updated_at,
            snapshot: value.snapshot.into(),
            metadata: value.metadata.into(),
            columnar_base_coordinate: value.columnar_base_coordinate,
            working_diff_baseline: value.working_diff_baseline,
        },
    )
}

fn reencode_head_value_with_baseline(
    value: HeadValueView<'_>,
    working_diff_baseline: WorkingDiffBaseline,
) -> Result<Vec<u8>, LixError> {
    encode_head_value_parts(HeadValueEncode {
        change_id: value.change_id,
        commit_id: value.commit_id,
        untracked: value.untracked,
        deleted: value.deleted,
        created_at: value.created_at,
        updated_at: value.updated_at,
        snapshot: value.snapshot.into(),
        metadata: value.metadata.into(),
        columnar_base_coordinate: value.columnar_base_coordinate,
        working_diff_baseline,
    })
}

fn encode_head_value_parts(value: HeadValueEncode<'_>) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    append_head_value_parts(&mut bytes, value)?;
    Ok(bytes)
}

fn append_head_value_parts(
    bytes: &mut Vec<u8>,
    value: HeadValueEncode<'_>,
) -> Result<std::ops::Range<usize>, LixError> {
    let fingerprint_inline = matches!(
        value.working_diff_baseline,
        WorkingDiffBaseline::BeforeAbsent { .. } | WorkingDiffBaseline::BeforePresent { .. }
    );
    let snapshot_kind = encoded_slot_kind(value.snapshot, fingerprint_inline);
    let metadata_kind = encoded_slot_kind(value.metadata, fingerprint_inline);
    if value.deleted && (snapshot_kind != HEAD_SLOT_NONE || metadata_kind != HEAD_SLOT_NONE) {
        return Err(head_value_error(
            "deleted current-state rows must not carry JSON payloads",
        ));
    }
    // Encode-side half of the untracked identity invariant documented on
    // `CurrentStateDeltaRef::validate`. Rejecting a *nil* id and not merely an
    // absent one is the point: the certified lanes default the per-row slot to
    // `ChangeId::default()`, which is `Some` but all zeroes, so an `is_some()`
    // check would let a row of 16 zero bytes reach the head and only fail
    // later, on read, in `decode_head_value`.
    let untracked_id_is_usable = value
        .change_id
        .is_some_and(|change_id| !change_id.as_uuid().is_nil());
    match (
        value.untracked,
        value.change_id,
        value.commit_id,
        value.deleted,
    ) {
        (false, Some(_), Some(_), _) => {}
        (true, _, None, false) if untracked_id_is_usable => {}
        (true, _, _, true) => {
            return Err(head_value_error(
                "untracked current-state rows must be deleted physically",
            ));
        }
        (false, _, _, _) => {
            return Err(head_value_error(
                "tracked current-state rows must carry change_id and commit_id",
            ));
        }
        (true, _, _, false) => {
            return Err(head_value_error(
                "untracked current-state rows must carry a non-nil change_id and no commit_id",
            ));
        }
    }
    if value.untracked && value.working_diff_baseline != WorkingDiffBaseline::Disabled {
        return Err(head_value_error(
            "untracked current-state rows must not carry a working-diff baseline",
        ));
    }
    if value.untracked && value.columnar_base_coordinate.is_some() {
        return Err(head_value_error(
            "untracked current-state rows must not carry an columnar base coordinate",
        ));
    }
    if value
        .columnar_base_coordinate
        .is_some_and(|coordinate| coordinate.base_commit_id == CommitId::default())
    {
        return Err(head_value_error(
            "columnar base coordinate must carry a non-nil owner commit",
        ));
    }
    let snapshot_len = encoded_slot_len(value.snapshot, fingerprint_inline);
    let metadata_len = encoded_slot_len(value.metadata, fingerprint_inline);
    let capacity = HEAD_VALUE_HEADER_BYTES
        .checked_add(snapshot_len)
        .and_then(|bytes| bytes.checked_add(metadata_len))
        .and_then(|bytes| {
            bytes.checked_add(match value.working_diff_baseline {
                WorkingDiffBaseline::BeforePresent { .. } => {
                    WORKING_DIFF_CHECKPOINT_BYTES + WORKING_DIFF_VERSION_BYTES
                }
                WorkingDiffBaseline::BeforeAbsent { .. } => WORKING_DIFF_CHECKPOINT_BYTES,
                WorkingDiffBaseline::Disabled | WorkingDiffBaseline::Clean => 0,
            })
        })
        .and_then(|bytes| {
            bytes.checked_add(
                value
                    .columnar_base_coordinate
                    .map_or(0, |_| COLUMNAR_BASE_COORDINATE_BYTES),
            )
        })
        .ok_or_else(|| head_value_error("encoded row length overflow"))?;
    let start = bytes.len();
    bytes.reserve(capacity);
    bytes.push(HEAD_VALUE_VERSION);
    let mut flags = if value.deleted { HEAD_VALUE_DELETED } else { 0 };
    if value.untracked {
        flags |= HEAD_VALUE_UNTRACKED;
    }
    flags |= snapshot_kind << HEAD_VALUE_SNAPSHOT_SHIFT;
    flags |= metadata_kind << HEAD_VALUE_METADATA_SHIFT;
    flags |= encode_working_diff_baseline_tag(value.working_diff_baseline)
        << HEAD_VALUE_WORKING_DIFF_SHIFT;
    bytes.push(flags);
    bytes.extend_from_slice(value.change_id.unwrap_or_default().as_uuid().as_bytes());
    bytes.extend_from_slice(value.commit_id.unwrap_or_default().as_uuid().as_bytes());
    bytes.extend_from_slice(&value.created_at.packed().to_be_bytes());
    bytes.extend_from_slice(&value.updated_at.packed().to_be_bytes());
    bytes.extend_from_slice(
        &u32::try_from(snapshot_len)
            .map_err(|_| head_value_error("snapshot payload exceeds v8 u32 limit"))?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(
        &u32::try_from(metadata_len)
            .map_err(|_| head_value_error("metadata payload exceeds v8 u32 limit"))?
            .to_be_bytes(),
    );
    bytes.push(u8::from(value.columnar_base_coordinate.is_some()));
    append_slot_payload(bytes, value.snapshot, fingerprint_inline);
    append_slot_payload(bytes, value.metadata, fingerprint_inline);
    match value.working_diff_baseline {
        WorkingDiffBaseline::BeforeAbsent {
            checkpoint_commit_id,
        } => bytes.extend_from_slice(checkpoint_commit_id.as_uuid().as_bytes()),
        WorkingDiffBaseline::BeforePresent {
            checkpoint_commit_id,
            version,
        } => {
            bytes.extend_from_slice(checkpoint_commit_id.as_uuid().as_bytes());
            encode_working_diff_version(bytes, version);
        }
        WorkingDiffBaseline::Disabled | WorkingDiffBaseline::Clean => {}
    }
    if let Some(coordinate) = value.columnar_base_coordinate {
        bytes.extend_from_slice(coordinate.base_commit_id.as_uuid().as_bytes());
        bytes.extend_from_slice(&coordinate.group_index.to_be_bytes());
        bytes.extend_from_slice(&coordinate.row_index.to_be_bytes());
    }
    debug_assert_eq!(bytes.len() - start, capacity);
    Ok(start..bytes.len())
}

fn encode_working_diff_baseline_tag(baseline: WorkingDiffBaseline) -> u8 {
    match baseline {
        WorkingDiffBaseline::Disabled => HEAD_WORKING_DIFF_DISABLED,
        WorkingDiffBaseline::Clean => HEAD_WORKING_DIFF_CLEAN,
        WorkingDiffBaseline::BeforeAbsent { .. } => HEAD_WORKING_DIFF_BEFORE_ABSENT,
        WorkingDiffBaseline::BeforePresent { .. } => HEAD_WORKING_DIFF_BEFORE_PRESENT,
    }
}

fn encoded_slot_kind(slot: HeadSlotEncode<'_>, fingerprint_inline: bool) -> u8 {
    match slot {
        HeadSlotEncode::None => HEAD_SLOT_NONE,
        HeadSlotEncode::Ref(_) => HEAD_SLOT_REF,
        HeadSlotEncode::Inline { .. } if fingerprint_inline => HEAD_SLOT_INLINE_FINGERPRINTED,
        HeadSlotEncode::Inline { .. } => HEAD_SLOT_INLINE,
    }
}

fn encoded_slot_len(slot: HeadSlotEncode<'_>, fingerprint_inline: bool) -> usize {
    match slot {
        HeadSlotEncode::None => 0,
        HeadSlotEncode::Ref(_) => JSON_REF_BYTES,
        HeadSlotEncode::Inline { json, .. } if fingerprint_inline => {
            JSON_REF_BYTES.saturating_add(json.len())
        }
        HeadSlotEncode::Inline { json, .. } => json.len(),
    }
}

fn append_slot_payload(bytes: &mut Vec<u8>, slot: HeadSlotEncode<'_>, fingerprint_inline: bool) {
    match slot {
        HeadSlotEncode::None => {}
        HeadSlotEncode::Ref(json_ref) => bytes.extend_from_slice(json_ref.as_hash_bytes()),
        HeadSlotEncode::Inline { json_ref, json } if fingerprint_inline => {
            let json_ref = json_ref.unwrap_or_else(|| JsonRef::for_content(json.as_bytes()));
            bytes.extend_from_slice(json_ref.as_hash_bytes());
            bytes.extend_from_slice(json.as_bytes());
        }
        HeadSlotEncode::Inline { json, .. } => bytes.extend_from_slice(json.as_bytes()),
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
        return Err(head_value_error("row is shorter than the v8 fixed header"));
    }
    if bytes[0] != HEAD_VALUE_VERSION {
        return Err(head_value_error(&format!(
            "unsupported row format version {}",
            bytes[0]
        )));
    }
    let flags = bytes[1];
    let snapshot_kind = (flags >> HEAD_VALUE_SNAPSHOT_SHIFT) & HEAD_VALUE_SLOT_MASK;
    let metadata_kind = (flags >> HEAD_VALUE_METADATA_SHIFT) & HEAD_VALUE_SLOT_MASK;
    let change_uuid = uuid_from_head_bytes(&bytes[2..18], "change id")?;
    let commit_uuid = uuid_from_head_bytes(&bytes[18..34], "commit id")?;
    let created_at = LixTimestamp::from_packed(read_u64(&bytes[34..42], "created_at")?)
        .map_err(|error| head_value_error(&format!("invalid created_at: {error}")))?;
    let updated_at = LixTimestamp::from_packed(read_u64(&bytes[42..50], "updated_at")?)
        .map_err(|error| head_value_error(&format!("invalid updated_at: {error}")))?;
    let snapshot_len = usize::try_from(read_u32(&bytes[50..54], "snapshot length")?)
        .map_err(|_| head_value_error("snapshot length exceeds usize"))?;
    let metadata_len = usize::try_from(read_u32(&bytes[54..58], "metadata length")?)
        .map_err(|_| head_value_error("metadata length exceeds usize"))?;
    let has_columnar_base_coordinate = match bytes[58] {
        0 => false,
        1 => true,
        _ => return Err(head_value_error("invalid columnar base-coordinate tag")),
    };
    let snapshot_end = HEAD_VALUE_HEADER_BYTES
        .checked_add(snapshot_len)
        .ok_or_else(|| head_value_error("snapshot payload length overflow"))?;
    let metadata_end = snapshot_end
        .checked_add(metadata_len)
        .ok_or_else(|| head_value_error("metadata payload length overflow"))?;
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
    let baseline_tag = (flags >> HEAD_VALUE_WORKING_DIFF_SHIFT) & HEAD_VALUE_WORKING_DIFF_MASK;
    let mut baseline_offset = metadata_end;
    let working_diff_baseline = match baseline_tag {
        HEAD_WORKING_DIFF_DISABLED => WorkingDiffBaseline::Disabled,
        HEAD_WORKING_DIFF_CLEAN => WorkingDiffBaseline::Clean,
        HEAD_WORKING_DIFF_BEFORE_ABSENT => WorkingDiffBaseline::BeforeAbsent {
            checkpoint_commit_id: decode_working_diff_checkpoint(bytes, &mut baseline_offset)?,
        },
        HEAD_WORKING_DIFF_BEFORE_PRESENT => WorkingDiffBaseline::BeforePresent {
            checkpoint_commit_id: decode_working_diff_checkpoint(bytes, &mut baseline_offset)?,
            version: decode_working_diff_version(bytes, &mut baseline_offset)?,
        },
        _ => unreachable!("two-bit working-diff baseline tag is exhaustive"),
    };
    let columnar_base_coordinate = if has_columnar_base_coordinate {
        let base_commit_id = CommitId::new(uuid_from_head_bytes(
            take_head_bytes(
                bytes,
                &mut baseline_offset,
                UUID_BYTES,
                "columnar base commit id",
            )?,
            "columnar base commit id",
        )?);
        if base_commit_id == CommitId::default() {
            return Err(head_value_error(
                "columnar base coordinate has a nil owner commit",
            ));
        }
        let group_index = read_u32(
            take_head_bytes(bytes, &mut baseline_offset, 4, "columnar base group index")?,
            "columnar base group index",
        )?;
        let row_index = read_u32(
            take_head_bytes(bytes, &mut baseline_offset, 4, "columnar base row index")?,
            "columnar base row index",
        )?;
        Some(ColumnarBaseCoordinate {
            base_commit_id,
            group_index,
            row_index,
        })
    } else {
        None
    };
    if baseline_offset != bytes.len() {
        return Err(head_value_error(
            "row payload lengths do not match the buffer",
        ));
    }
    let deleted = flags & HEAD_VALUE_DELETED != 0;
    let untracked = flags & HEAD_VALUE_UNTRACKED != 0;
    if deleted && (snapshot != HeadSlotView::None || metadata != HeadSlotView::None) {
        return Err(head_value_error(
            "deleted current-state rows must not carry JSON payloads",
        ));
    }
    let (change_id, commit_id) = if untracked {
        if deleted {
            return Err(head_value_error(
                "untracked current-state rows must be deleted physically",
            ));
        }
        if change_uuid == uuid::Uuid::nil() || commit_uuid != uuid::Uuid::nil() {
            return Err(head_value_error(
                "untracked current-state rows must use a non-nil change id and a nil commit id",
            ));
        }
        if working_diff_baseline != WorkingDiffBaseline::Disabled {
            return Err(head_value_error(
                "untracked current-state rows must not carry a working-diff baseline",
            ));
        }
        if columnar_base_coordinate.is_some() {
            return Err(head_value_error(
                "untracked current-state rows must not carry an columnar base coordinate",
            ));
        }
        (Some(ChangeId::new(change_uuid)), None)
    } else {
        if change_uuid == uuid::Uuid::nil() || commit_uuid == uuid::Uuid::nil() {
            return Err(head_value_error(
                "tracked current-state rows must use non-nil change and commit ids",
            ));
        }
        (
            Some(ChangeId::new(change_uuid)),
            Some(CommitId::new(commit_uuid)),
        )
    };
    Ok(HeadValueView {
        change_id,
        commit_id,
        untracked,
        deleted,
        created_at,
        updated_at,
        snapshot,
        metadata,
        payload_slots_materialized: true,
        columnar_base_coordinate,
        working_diff_baseline,
    })
}

fn take_head_bytes<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    length: usize,
    field: &str,
) -> Result<&'a [u8], LixError> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| head_value_error(format!("{field} offset overflow")))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| head_value_error(format!("{field} is truncated")))?;
    *offset = end;
    Ok(value)
}

fn uuid_from_head_bytes(bytes: &[u8], field: &str) -> Result<uuid::Uuid, LixError> {
    let bytes: [u8; UUID_BYTES] = bytes.try_into().map_err(|_| {
        head_value_error(&format!(
            "{field} must have {UUID_BYTES} bytes in the v8 header"
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
        HEAD_SLOT_INLINE_FINGERPRINTED if bytes.len() >= JSON_REF_BYTES => {
            let (hash, json) = bytes.split_at(JSON_REF_BYTES);
            let hash: [u8; JSON_REF_BYTES] = hash.try_into().map_err(|_| {
                head_value_error(&format!(
                    "{field} inline fingerprint must have {JSON_REF_BYTES} bytes"
                ))
            })?;
            std::str::from_utf8(json)
                .map(|json| HeadSlotView::InlineFingerprinted {
                    json_ref: JsonRef::from_hash_bytes(hash),
                    json,
                })
                .map_err(|error| {
                    head_value_error(&format!("{field} inline payload is not UTF-8: {error}"))
                })
        }
        HEAD_SLOT_INLINE_FINGERPRINTED => Err(head_value_error(&format!(
            "{field} inline payload is shorter than its {JSON_REF_BYTES}-byte fingerprint"
        ))),
        _ => Err(head_value_error(&format!(
            "{field} has an unknown slot kind {kind}"
        ))),
    }
}

fn head_value_error(message: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("invalid hot live-state row: {message}"),
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

trait LiveMaterializationIdentity {
    #[allow(clippy::too_many_arguments)]
    fn push_materialized(
        self,
        rows: &mut MaterializedHotStateBatchBuilder,
        snapshot_content: Option<SharedStr>,
        metadata: Option<SharedStr>,
        deleted: bool,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
        global: bool,
        change_id: Option<ChangeId>,
        commit_id: Option<CommitId>,
        untracked: bool,
        branch_id: &str,
    );
}

impl LiveMaterializationIdentity for HeadRowIdentity {
    fn push_materialized(
        self,
        rows: &mut MaterializedHotStateBatchBuilder,
        snapshot_content: Option<SharedStr>,
        metadata: Option<SharedStr>,
        deleted: bool,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
        global: bool,
        change_id: Option<ChangeId>,
        commit_id: Option<CommitId>,
        untracked: bool,
        branch_id: &str,
    ) {
        rows.push_materialized(
            self.row_pk,
            self.schema_key,
            self.file_id,
            snapshot_content,
            metadata,
            deleted,
            created_at,
            updated_at,
            global,
            change_id,
            commit_id,
            untracked,
            branch_id,
        );
    }
}

impl LiveMaterializationIdentity for TrackedStateKeyRef<'_> {
    fn push_materialized(
        self,
        rows: &mut MaterializedHotStateBatchBuilder,
        snapshot_content: Option<SharedStr>,
        metadata: Option<SharedStr>,
        deleted: bool,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
        global: bool,
        change_id: Option<ChangeId>,
        commit_id: Option<CommitId>,
        untracked: bool,
        branch_id: &str,
    ) {
        rows.push_materialized_ref(
            self.row_pk,
            self.schema_key,
            self.file_id,
            snapshot_content,
            metadata,
            deleted,
            created_at,
            updated_at,
            global,
            change_id,
            commit_id,
            untracked,
            branch_id,
        );
    }
}

/// Builds serving rows directly from a V5 hot-row value. Inline JSON remains a
/// range over the immutable head-value buffer, while out-of-band JSON retains
/// the `JsonStore` buffer. There is no per-row payload `String` or intermediate
/// `HeadValue`/`MaterializedTrackedStateRow` staging layer.
async fn materialize_live_entries<I>(
    store: &(impl StorageAdapterRead + ?Sized),
    entries: Vec<(I, Bytes)>,
    projection: ChangeRecordProjection,
    branch_id: &str,
    active_checkpoint_commit_id: Option<CommitId>,
) -> Result<MaterializedHotStateBatch, LixError>
where
    I: LiveMaterializationIdentity,
{
    let global = branch_id == crate::GLOBAL_BRANCH_ID;
    let mut json_refs = Vec::new();
    let mut deferred = Vec::new();
    let mut rows = MaterializedHotStateBatchBuilder::with_capacity(entries.len());
    for (identity, bytes) in entries {
        let value = decode_head_value(&bytes)?;
        let row_index = rows.len();
        let snapshot_content = materialize_live_slot(
            !value.deleted && projection.snapshot_content,
            &bytes,
            value.snapshot,
            &mut json_refs,
            &mut deferred,
            row_index,
            DeferredJsonField::Snapshot,
        );
        let metadata = materialize_live_slot(
            !value.deleted && projection.metadata,
            &bytes,
            value.metadata,
            &mut json_refs,
            &mut deferred,
            row_index,
            DeferredJsonField::Metadata,
        );
        identity.push_materialized(
            &mut rows,
            snapshot_content,
            metadata,
            value.deleted,
            effective_hot_created_at(value, active_checkpoint_commit_id),
            value.updated_at,
            global,
            value.change_id,
            effective_hot_commit_id(value, active_checkpoint_commit_id),
            value.untracked,
            branch_id,
        );
        if let Some(coordinate) = value.columnar_base_coordinate {
            rows.set_columnar_base_coordinate(row_index, coordinate);
        }
        rows.set_durable_predecessor(row_index, CertifiedCurrentStatePredecessor::Encoded(bytes));
    }
    if json_refs.is_empty() {
        return Ok(rows.finish());
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
        let json = SharedStr::from_utf8(bytes).map_err(|error| {
            head_value_error(&format!("out-of-band JSON payload is not UTF-8: {error}"))
        })?;
        match deferred.field {
            DeferredJsonField::Snapshot => {
                rows.set_snapshot_content(deferred.row_index, json);
            }
            DeferredJsonField::Metadata => rows.set_metadata(deferred.row_index, json),
        }
    }
    Ok(rows.finish())
}

fn materialize_live_slot(
    include: bool,
    owner: &Bytes,
    slot: HeadSlotView<'_>,
    json_refs: &mut Vec<JsonRef>,
    deferred: &mut Vec<DeferredJson>,
    row_index: usize,
    field: DeferredJsonField,
) -> Option<SharedStr> {
    if !include {
        return None;
    }
    match slot {
        HeadSlotView::None => None,
        HeadSlotView::Inline(json) | HeadSlotView::InlineFingerprinted { json, .. } => {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_hot_scan_value_handle_clone();
            Some(
                SharedStr::from_utf8_slice(owner.clone(), json)
                    .expect("decoded inline JSON points into its head-value buffer"),
            )
        }
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

    async fn scan_test_space(
        read: &(impl StorageAdapterRead + ?Sized),
        space: StorageSpace,
    ) -> Vec<crate::storage_adapter::StorageReadEntry> {
        let range = StoragePrefix {
            bytes: Bytes::new(),
        }
        .to_range()
        .expect("valid empty prefix");
        let mut cursor = read
            .begin_scan(space, range, StorageBeginScanOptions::default())
            .await
            .expect("begin test scan");
        cursor.collect_all().await.expect("read test scan page")
    }

    fn ts(value: &str) -> LixTimestamp {
        LixTimestamp::expect_parse("test timestamp", value)
    }

    fn identity(branch_id: &str, generation: CommitId, row: &str) -> HeadIdentity {
        HeadIdentity {
            branch_id: branch_id.to_string(),
            generation,
            schema_key: "schema".to_string(),
            row_pk: RowPk::single(row),
            file_id: None,
        }
    }

    fn head_value(change: &str, commit_id: CommitId) -> HeadValue {
        HeadValue {
            change_id: Some(ChangeId::for_test_label(change)),
            commit_id: Some(commit_id),
            untracked: false,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            snapshot: JsonSlot::from_json("{\"value\":true}"),
            metadata: JsonSlot::None,
            columnar_base_coordinate: None,
        }
    }

    fn working_diff_control(
        head_commit_id: CommitId,
        generation: CommitId,
        checkpoint_commit_id: CommitId,
    ) -> BranchHeadControl {
        BranchHeadControl {
            head_commit_id,
            tracked_generation: generation,
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id: Some(checkpoint_commit_id),
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            ref_change_id: ChangeId::for_test_label("working-diff-branch-ref"),
            accelerator_root_set_digest:
                crate::tracked_state::accelerator_root_set_digest(None)
                    .expect("empty accelerator selection should hash"),
        }
    }

    fn working_diff_delta<'a>(
        row_pk: &'a RowPk,
        file_id: Option<&'a str>,
        change: &str,
        commit_id: CommitId,
        deleted: bool,
        snapshot: &'a str,
        metadata: Option<&'a str>,
        updated_at: &str,
    ) -> TrackedHeadDeltaRef<'a> {
        TrackedHeadDeltaRef {
            schema_key: "schema",
            file_id,
            row_pk,
            change_id: ChangeId::for_test_label(change),
            commit_id,
            deleted,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts(updated_at),
            snapshot: JsonSlotRef::Inline(snapshot),
            metadata: metadata.map_or(JsonSlotRef::None, JsonSlotRef::Inline),
        }
    }

    async fn publish_working_diff_commit(
        storage: &StorageAdapter<Memory>,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        head_commit_id: CommitId,
        deltas: &[TrackedHeadDeltaRef<'_>],
        checkpoint_commit_id: CommitId,
        coverage: &mut WorkingDiffIndexCoverage,
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open working-diff write read");
        let mut writes = StorageWriteSet::new();
        let generation = TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit_with_working_diff(
                branch_id,
                parent_generation,
                head_commit_id,
                deltas,
                &BTreeSet::new(),
                None,
                Some(checkpoint_commit_id),
                coverage,
            )
            .await
            .expect("stage working-diff current state");
        assert_eq!(generation, parent_generation.unwrap_or(head_commit_id));
        stage_tracked_working_diff_epoch(
            &mut writes,
            branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id,
                generation,
                coverage: *coverage,
            },
        )
        .expect("stage working-diff epoch");
        stage_branch_head_control(
            &mut writes,
            branch_id,
            working_diff_control(head_commit_id, generation, checkpoint_commit_id),
        )
        .expect("stage working-diff control");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit working-diff current state");
    }

    async fn read_working_diff(
        storage: &StorageAdapter<Memory>,
        branch_id: &str,
        head_commit_id: CommitId,
        generation: CommitId,
        checkpoint_commit_id: CommitId,
        request: &TrackedStateDiffRequest,
    ) -> TrackedWorkingDiff {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open working-diff read");
        TrackedHeadContext::new()
            .reader(read)
            .working_diff_for_control(
                branch_id,
                working_diff_control(head_commit_id, generation, checkpoint_commit_id),
                request,
            )
            .await
            .expect("read working diff")
            .expect("working diff must be current")
    }

    #[test]
    fn v8_value_codec_roundtrips_clean_inline_ref_and_base_coordinate() {
        let snapshot_ref = JsonRef::from_hash_bytes([7; JSON_REF_BYTES]);
        let columnar_base_coordinate = ColumnarBaseCoordinate {
            base_commit_id: CommitId::for_test_label("columnar-base"),
            group_index: 17,
            row_index: 42,
        };
        let value = HeadValueRef {
            change_id: Some(ChangeId::for_test_label("change")),
            commit_id: Some(CommitId::for_test_label("commit")),
            untracked: false,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-02T00:00:00Z"),
            snapshot: JsonSlotRef::Inline("{\"snapshot\":true}"),
            metadata: JsonSlotRef::Ref(&snapshot_ref),
            columnar_base_coordinate: Some(columnar_base_coordinate),
            working_diff_baseline: WorkingDiffBaseline::Disabled,
        };

        let bytes = encode_head_value(&value).expect("encode v8 row");
        assert_eq!(bytes[0], HEAD_VALUE_VERSION);
        assert_eq!(
            bytes.len(),
            HEAD_VALUE_HEADER_BYTES
                + "{\"snapshot\":true}".len()
                + JSON_REF_BYTES
                + COLUMNAR_BASE_COORDINATE_BYTES
        );
        let decoded = decode_head_value(&bytes).expect("decode v8 row");
        assert_eq!(decoded.change_id, value.change_id);
        assert_eq!(decoded.commit_id, value.commit_id);
        assert_eq!(decoded.created_at, value.created_at);
        assert_eq!(decoded.updated_at, value.updated_at);
        assert_eq!(
            decoded.columnar_base_coordinate,
            Some(columnar_base_coordinate)
        );
        assert_eq!(
            decoded.snapshot,
            HeadSlotView::Inline("{\"snapshot\":true}")
        );
        assert_eq!(decoded.metadata, HeadSlotView::Ref(snapshot_ref));
    }

    #[test]
    fn materialized_inline_fields_share_the_head_value_buffer() {
        let value = HeadValueRef {
            change_id: Some(ChangeId::for_test_label("shared-fields-change")),
            commit_id: Some(CommitId::for_test_label("shared-fields-commit")),
            untracked: false,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-02T00:00:00Z"),
            snapshot: JsonSlotRef::Inline("{\"snapshot\":true}"),
            metadata: JsonSlotRef::Inline("{\"source\":\"test\"}"),
            columnar_base_coordinate: None,
            working_diff_baseline: WorkingDiffBaseline::Disabled,
        };
        let bytes = Bytes::from(encode_head_value(&value).expect("encode v8 row"));
        let decoded = decode_head_value(&bytes).expect("decode v8 row");
        let mut json_refs = Vec::new();
        let mut deferred = Vec::new();
        let snapshot = materialize_live_slot(
            true,
            &bytes,
            decoded.snapshot,
            &mut json_refs,
            &mut deferred,
            0,
            DeferredJsonField::Snapshot,
        )
        .expect("snapshot view");
        let metadata = materialize_live_slot(
            true,
            &bytes,
            decoded.metadata,
            &mut json_refs,
            &mut deferred,
            0,
            DeferredJsonField::Metadata,
        )
        .expect("metadata view");

        assert!(snapshot.shares_buffer_with(&metadata));
        assert_eq!(snapshot.retained_buffer_len(), bytes.len());
        assert_eq!(metadata.retained_buffer_len(), bytes.len());
        assert!(json_refs.is_empty());
        assert!(deferred.is_empty());
    }

    #[test]
    fn v8_value_codec_embeds_a_checkpoint_owned_tracked_first_before_baseline() {
        let checkpoint_commit_id = CommitId::for_test_label("baseline-checkpoint");
        let baseline = WorkingDiffVersion {
            change_id: ChangeId::for_test_label("before-change"),
            commit_id: CommitId::for_test_label("before-commit"),
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:01Z"),
            snapshot: WorkingDiffSlotFingerprint {
                kind: WORKING_DIFF_SLOT_INLINE,
                hash: [3; JSON_REF_BYTES],
            },
            metadata: WorkingDiffSlotFingerprint {
                kind: WORKING_DIFF_SLOT_NONE,
                hash: [0; JSON_REF_BYTES],
            },
        };
        let value = HeadValueRef {
            change_id: Some(ChangeId::for_test_label("current-change")),
            commit_id: Some(CommitId::for_test_label("current-commit")),
            untracked: false,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-02T00:00:00Z"),
            snapshot: JsonSlotRef::Inline("{\"current\":true}"),
            metadata: JsonSlotRef::None,
            columnar_base_coordinate: None,
            working_diff_baseline: WorkingDiffBaseline::BeforePresent {
                checkpoint_commit_id,
                version: baseline,
            },
        };

        let bytes = encode_head_value(&value).expect("encode v8 row with baseline");
        assert_eq!(
            bytes.len(),
            HEAD_VALUE_HEADER_BYTES
                + JSON_REF_BYTES
                + "{\"current\":true}".len()
                + WORKING_DIFF_CHECKPOINT_BYTES
                + WORKING_DIFF_VERSION_BYTES
        );
        let decoded = decode_head_value(&bytes).expect("decode v8 row with baseline");
        assert_eq!(
            decoded.working_diff_baseline,
            WorkingDiffBaseline::BeforePresent {
                checkpoint_commit_id,
                version: baseline,
            }
        );
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

        assert_eq!(
            baseline.payload_equality(same_change),
            WorkingDiffPayloadEquality::Equal
        );
        assert_eq!(
            baseline.payload_equality(same_payload),
            WorkingDiffPayloadEquality::Equal
        );
        assert_eq!(
            baseline.payload_equality(different_payload),
            WorkingDiffPayloadEquality::Different
        );

        // A baseline captured by reference must never be read as "different".
        let unresolved = WorkingDiffVersion {
            change_id: ChangeId::for_test_label("root-change"),
            snapshot: WorkingDiffSlotFingerprint::unresolved(),
            metadata: WorkingDiffSlotFingerprint::unresolved(),
            ..baseline
        };
        assert_eq!(
            unresolved.payload_equality(different_payload),
            WorkingDiffPayloadEquality::Unresolved
        );
        assert_eq!(
            unresolved.payload_equality(WorkingDiffVersion {
                change_id: ChangeId::for_test_label("root-change"),
                ..different_payload
            }),
            WorkingDiffPayloadEquality::Equal,
            "a shared change record resolves without hydration"
        );
    }

    #[tokio::test]
    async fn working_diff_absent_delete_then_reinsert_is_added() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let checkpoint = CommitId::for_test_label("checkpoint");
        let delete_head = CommitId::for_test_label("delete");
        let reinsert_head = CommitId::for_test_label("reinsert");
        let row_pk = RowPk::single("row");
        let mut coverage = WorkingDiffIndexCoverage::default();

        publish_working_diff_commit(
            &storage,
            branch_id,
            None,
            checkpoint,
            &[],
            checkpoint,
            &mut coverage,
        )
        .await;

        let delete = [working_diff_delta(
            &row_pk,
            None,
            "delete-absent",
            delete_head,
            true,
            "{\"ignored\":true}",
            None,
            "2026-01-02T00:00:00Z",
        )];
        publish_working_diff_commit(
            &storage,
            branch_id,
            Some(checkpoint),
            delete_head,
            &delete,
            checkpoint,
            &mut coverage,
        )
        .await;
        let after_delete = read_working_diff(
            &storage,
            branch_id,
            delete_head,
            checkpoint,
            checkpoint,
            &TrackedStateDiffRequest::default(),
        )
        .await;
        assert!(
            after_delete.diff.entries.is_empty(),
            "deleting an identity that was absent at the checkpoint is net empty"
        );

        let reinsert = [working_diff_delta(
            &row_pk,
            None,
            "reinsert",
            reinsert_head,
            false,
            "{\"value\":\"present\"}",
            None,
            "2026-01-03T00:00:00Z",
        )];
        publish_working_diff_commit(
            &storage,
            branch_id,
            Some(checkpoint),
            reinsert_head,
            &reinsert,
            checkpoint,
            &mut coverage,
        )
        .await;

        let diff = read_working_diff(
            &storage,
            branch_id,
            reinsert_head,
            checkpoint,
            checkpoint,
            &TrackedStateDiffRequest::default(),
        )
        .await;
        assert_eq!(coverage.group_count, 1, "one identity stays dirty");
        assert_eq!(diff.checkpoint_commit_id, checkpoint);
        assert_eq!(diff.diff.entries.len(), 1);
        let entry = &diff.diff.entries[0];
        assert_eq!(entry.kind, TrackedStateDiffKind::Added);
        assert!(entry.before.is_none());
        assert_eq!(
            entry
                .after
                .as_ref()
                .expect("added entry has an after row")
                .change_id,
            ChangeId::for_test_label("reinsert")
        );
    }

    #[tokio::test]
    async fn working_diff_reads_mixed_direct_and_segmented_hot_indexes() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "mixed-hot-diff";
        let checkpoint = CommitId::for_test_label("mixed-checkpoint");
        let direct_head = CommitId::for_test_label("mixed-direct");
        let segmented_head = CommitId::for_test_label("mixed-segmented");
        let mut coverage = WorkingDiffIndexCoverage::default();

        publish_working_diff_commit(
            &storage,
            branch_id,
            None,
            checkpoint,
            &[],
            checkpoint,
            &mut coverage,
        )
        .await;

        let direct_rows = (0..32)
            .map(|index| RowPk::single(format!("direct-{index:03}")))
            .collect::<Vec<_>>();
        let direct = direct_rows
            .iter()
            .enumerate()
            .map(|(index, row_pk)| {
                working_diff_delta(
                    row_pk,
                    None,
                    &format!("direct-{index}"),
                    direct_head,
                    false,
                    "{\"value\":\"direct\"}",
                    None,
                    "2026-01-02T00:00:00Z",
                )
            })
            .collect::<Vec<_>>();
        publish_working_diff_commit(
            &storage,
            branch_id,
            Some(checkpoint),
            direct_head,
            &direct,
            checkpoint,
            &mut coverage,
        )
        .await;

        let segmented_rows = (0..96)
            .map(|index| RowPk::single(format!("segmented-{index:03}")))
            .collect::<Vec<_>>();
        let segmented = segmented_rows
            .iter()
            .enumerate()
            .map(|(index, row_pk)| {
                working_diff_delta(
                    row_pk,
                    None,
                    &format!("segmented-{index}"),
                    segmented_head,
                    false,
                    "{\"value\":\"segmented\"}",
                    None,
                    "2026-01-03T00:00:00Z",
                )
            })
            .collect::<Vec<_>>();
        publish_working_diff_commit(
            &storage,
            branch_id,
            Some(checkpoint),
            segmented_head,
            &segmented,
            checkpoint,
            &mut coverage,
        )
        .await;

        let diff = read_working_diff(
            &storage,
            branch_id,
            segmented_head,
            checkpoint,
            checkpoint,
            &TrackedStateDiffRequest::default(),
        )
        .await;
        assert_eq!(coverage.group_count, 128);
        assert_eq!(diff.diff.entries.len(), 128);
        assert!(
            diff.diff
                .entries
                .iter()
                .all(|entry| entry.kind == TrackedStateDiffKind::Added)
        );
    }

    #[tokio::test]
    async fn working_diff_clean_delete_then_restore_is_net_empty() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let checkpoint = CommitId::for_test_label("checkpoint");
        let delete_head = CommitId::for_test_label("delete");
        let restore_head = CommitId::for_test_label("restore");
        let row_pk = RowPk::single("row");
        let mut coverage = WorkingDiffIndexCoverage::default();

        let initial = [working_diff_delta(
            &row_pk,
            None,
            "initial",
            checkpoint,
            false,
            "{\"value\":\"one\"}",
            None,
            "2026-01-01T00:00:00Z",
        )];
        publish_working_diff_commit(
            &storage,
            branch_id,
            None,
            checkpoint,
            &initial,
            checkpoint,
            &mut coverage,
        )
        .await;

        let delete = [working_diff_delta(
            &row_pk,
            None,
            "delete",
            delete_head,
            true,
            "{\"ignored\":true}",
            None,
            "2026-01-02T00:00:00Z",
        )];
        publish_working_diff_commit(
            &storage,
            branch_id,
            Some(checkpoint),
            delete_head,
            &delete,
            checkpoint,
            &mut coverage,
        )
        .await;

        let restore = [working_diff_delta(
            &row_pk,
            None,
            "restore",
            restore_head,
            false,
            "{\"value\":\"one\"}",
            None,
            "2026-01-03T00:00:00Z",
        )];
        publish_working_diff_commit(
            &storage,
            branch_id,
            Some(checkpoint),
            restore_head,
            &restore,
            checkpoint,
            &mut coverage,
        )
        .await;

        let diff = read_working_diff(
            &storage,
            branch_id,
            restore_head,
            checkpoint,
            checkpoint,
            &TrackedStateDiffRequest::default(),
        )
        .await;
        assert_eq!(
            coverage.group_count, 1,
            "the restore must not duplicate the dirty key"
        );
        assert!(
            diff.diff.entries.is_empty(),
            "matching checkpoint payloads are net empty even after a tombstone"
        );
    }

    #[tokio::test]
    async fn working_diff_metadata_only_mutation_is_modified() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let checkpoint = CommitId::for_test_label("checkpoint");
        let update_head = CommitId::for_test_label("metadata-update");
        let row_pk = RowPk::single("row");
        let mut coverage = WorkingDiffIndexCoverage::default();

        let initial = [working_diff_delta(
            &row_pk,
            None,
            "initial",
            checkpoint,
            false,
            "{\"value\":\"same\"}",
            Some("{\"label\":\"before\"}"),
            "2026-01-01T00:00:00Z",
        )];
        publish_working_diff_commit(
            &storage,
            branch_id,
            None,
            checkpoint,
            &initial,
            checkpoint,
            &mut coverage,
        )
        .await;

        let update = [working_diff_delta(
            &row_pk,
            None,
            "metadata-update",
            update_head,
            false,
            "{\"value\":\"same\"}",
            Some("{\"label\":\"after\"}"),
            "2026-01-02T00:00:00Z",
        )];
        publish_working_diff_commit(
            &storage,
            branch_id,
            Some(checkpoint),
            update_head,
            &update,
            checkpoint,
            &mut coverage,
        )
        .await;

        let diff = read_working_diff(
            &storage,
            branch_id,
            update_head,
            checkpoint,
            checkpoint,
            &TrackedStateDiffRequest::default(),
        )
        .await;
        assert_eq!(diff.diff.entries.len(), 1);
        let entry = &diff.diff.entries[0];
        assert_eq!(entry.kind, TrackedStateDiffKind::Modified);
        assert_eq!(
            entry
                .before
                .as_ref()
                .expect("modified entry has a before row")
                .change_id,
            ChangeId::for_test_label("initial")
        );
        assert_eq!(
            entry
                .after
                .as_ref()
                .expect("modified entry has an after row")
                .change_id,
            ChangeId::for_test_label("metadata-update")
        );
    }

    #[tokio::test]
    async fn working_diff_file_id_mutation_after_checkpoint_is_modified() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let checkpoint = CommitId::for_test_label("checkpoint");
        let update_head = CommitId::for_test_label("file-update");
        let row_pk = RowPk::single("row");
        let file_id = "files/app.json";
        let mut coverage = WorkingDiffIndexCoverage::default();

        let initial = [working_diff_delta(
            &row_pk,
            Some(file_id),
            "initial-file",
            checkpoint,
            false,
            "{\"value\":\"before\"}",
            None,
            "2026-01-01T00:00:00Z",
        )];
        publish_working_diff_commit(
            &storage,
            branch_id,
            None,
            checkpoint,
            &initial,
            checkpoint,
            &mut coverage,
        )
        .await;

        let update = [working_diff_delta(
            &row_pk,
            Some(file_id),
            "updated-file",
            update_head,
            false,
            "{\"value\":\"after\"}",
            None,
            "2026-01-02T00:00:00Z",
        )];
        publish_working_diff_commit(
            &storage,
            branch_id,
            Some(checkpoint),
            update_head,
            &update,
            checkpoint,
            &mut coverage,
        )
        .await;

        let request = TrackedStateDiffRequest {
            filter: TrackedStateFilter {
                schema_keys: vec!["schema".to_string()],
                row_pks: vec![row_pk.clone()],
                file_ids: vec![NullableKeyFilter::Value(file_id.to_string())],
                ..Default::default()
            },
            ..Default::default()
        };
        let diff = read_working_diff(
            &storage,
            branch_id,
            update_head,
            checkpoint,
            checkpoint,
            &request,
        )
        .await;
        assert_eq!(diff.diff.entries.len(), 1);
        let entry = &diff.diff.entries[0];
        assert_eq!(entry.kind, TrackedStateDiffKind::Modified);
        assert_eq!(entry.identity.row_pk(), &row_pk);
        assert_eq!(entry.identity.file_id(), Some(file_id));
        assert_eq!(
            entry
                .before
                .as_ref()
                .expect("modified file entry has a before row")
                .change_id,
            ChangeId::for_test_label("initial-file")
        );
        assert_eq!(
            entry
                .after
                .as_ref()
                .expect("modified file entry has an after row")
                .change_id,
            ChangeId::for_test_label("updated-file")
        );
    }

    #[tokio::test]
    async fn working_diff_restarts_after_a_checkpoint_generation_and_verifies_coverage() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let checkpoint = CommitId::for_test_label("checkpoint");
        let first_head = CommitId::for_test_label("first-head");
        let no_op_checkpoint = CommitId::for_test_label("no-op-checkpoint");
        let second_head = CommitId::for_test_label("second-head");
        let row_pk = RowPk::single("row");
        let control = |head_commit_id, generation, checkpoint_commit_id| BranchHeadControl {
            head_commit_id,
            tracked_generation: generation,
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id: Some(checkpoint_commit_id),
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            ref_change_id: ChangeId::for_test_label("branch-ref"),
            accelerator_root_set_digest:
                crate::tracked_state::accelerator_root_set_digest(None)
                    .expect("empty accelerator selection should hash"),
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
                    row_pk: &row_pk,
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
                generation: checkpoint,
                coverage: initial_coverage,
            },
        )
        .expect("stage initial epoch");
        stage_branch_head_control(
            &mut writes,
            branch_id,
            control(checkpoint, checkpoint, checkpoint),
        )
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
                    row_pk: &row_pk,
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
                &mut first_coverage,
            )
            .await
            .expect("stage first update");
        stage_tracked_working_diff_epoch(
            &mut writes,
            branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: checkpoint,
                generation: checkpoint,
                coverage: first_coverage,
            },
        )
        .expect("stage first epoch");
        stage_branch_head_control(
            &mut writes,
            branch_id,
            control(first_head, checkpoint, checkpoint),
        )
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
            .working_diff_for_control(
                branch_id,
                control(first_head, checkpoint, checkpoint),
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

        // A checkpoint that selects the already-authoritative immutable
        // change still has to clear its row-local dirty baseline. Leaving the
        // stale before-image physically attached would make retained current
        // state grow with every checkpoint interval.
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open no-op checkpoint write read");
        let mut writes = StorageWriteSet::new();
        let mut no_op_coverage = WorkingDiffIndexCoverage::default();
        let selected = TrackedHeadDeltaRef {
            schema_key: "schema",
            file_id: None,
            row_pk: &row_pk,
            change_id: ChangeId::for_test_label("first-change"),
            commit_id: first_head,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-02T00:00:00Z"),
            snapshot: JsonSlotRef::Inline("{\"value\":\"two\"}"),
            metadata: JsonSlotRef::None,
        };
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_checkpoint_current_state(
                branch_id,
                checkpoint,
                no_op_checkpoint,
                &[selected.as_current()],
                &BTreeSet::new(),
                no_op_checkpoint,
                &mut no_op_coverage,
            )
            .await
            .expect("stage no-op checkpoint publication");
        assert_eq!(no_op_coverage, WorkingDiffIndexCoverage::default());
        assert!(
            writes.contains_put(
                ROW_SPACE,
                &hot::encode_hot_row_key(&HeadIdentity {
                    branch_id: branch_id.to_owned(),
                    generation: checkpoint,
                    schema_key: "schema".to_owned(),
                    row_pk: row_pk.clone(),
                    file_id: None,
                }),
            ),
            "an identical dirty selected change must be rewritten clean"
        );
        stage_tracked_working_diff_epoch(
            &mut writes,
            branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: no_op_checkpoint,
                generation: checkpoint,
                coverage: no_op_coverage,
            },
        )
        .expect("reset no-op checkpoint epoch");
        stage_branch_head_control(
            &mut writes,
            branch_id,
            control(no_op_checkpoint, checkpoint, no_op_checkpoint),
        )
        .expect("stage no-op checkpoint control");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit no-op checkpoint");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open no-op checkpoint epoch read");
        let hot_key = hot::encode_hot_row_key(&HeadIdentity {
            branch_id: branch_id.to_owned(),
            generation: checkpoint,
            schema_key: "schema".to_owned(),
            row_pk: row_pk.clone(),
            file_id: None,
        });
        let value = PointReadPlan::new(ROW_SPACE, &[StorageKey(Bytes::from(hot_key))])
            .materialize(&read, StorageGetOptions::default())
            .await
            .expect("read cleaned checkpoint HOT row")
            .value
            .into_iter()
            .next()
            .flatten()
            .expect("cleaned checkpoint HOT row exists");
        let StorageProjectedValue::FullValue(value) = value else {
            panic!("cleaned checkpoint HOT row unexpectedly omitted its value");
        };
        assert_eq!(
            decode_head_value(&value)
                .expect("cleaned checkpoint HOT value decodes")
                .working_diff_baseline,
            WorkingDiffBaseline::Clean,
        );
        assert_eq!(
            TrackedHeadContext::new()
                .reader(read)
                .working_diff_epoch(branch_id)
                .await
                .expect("no-op checkpoint epoch should decode"),
            Some(TrackedWorkingDiffEpoch {
                checkpoint_commit_id: no_op_checkpoint,
                generation: checkpoint,
                coverage: WorkingDiffIndexCoverage::default(),
            })
        );
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open no-op checkpoint read");
        let empty_diff = TrackedHeadContext::new()
            .reader(read)
            .working_diff_for_control(
                branch_id,
                control(no_op_checkpoint, checkpoint, no_op_checkpoint),
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("no-op checkpoint direct diff should read")
            .expect("no-op checkpoint direct diff should be known empty");
        assert!(empty_diff.diff.entries.is_empty());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact no-op checkpoint read");
        let exact_empty_diff = TrackedHeadContext::new()
            .reader(read)
            .working_diff_for_control(
                branch_id,
                control(no_op_checkpoint, checkpoint, no_op_checkpoint),
                &TrackedStateDiffRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema".to_owned()],
                        row_pks: vec![row_pk.clone()],
                        ..TrackedStateFilter::default()
                    },
                    ..TrackedStateDiffRequest::default()
                },
            )
            .await
            .expect("exact no-op checkpoint diff should read")
            .expect("exact no-op checkpoint diff should be current");
        assert!(
            exact_empty_diff.diff.entries.is_empty(),
            "finite diff reads must ignore a baseline owned by the prior checkpoint"
        );

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
                    row_pk: &row_pk,
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
                &mut second_coverage,
            )
            .await
            .expect("stage second update");
        stage_tracked_working_diff_epoch(
            &mut writes,
            branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: no_op_checkpoint,
                generation: checkpoint,
                coverage: second_coverage,
            },
        )
        .expect("stage second epoch");
        stage_branch_head_control(
            &mut writes,
            branch_id,
            control(second_head, checkpoint, no_op_checkpoint),
        )
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
            .working_diff_for_control(
                branch_id,
                control(second_head, checkpoint, no_op_checkpoint),
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

        // An auxiliary epoch with the right physical generation but a
        // different checkpoint must never become a direct diff result. In
        // particular, an exact-PK request must not turn that stale epoch into
        // a plausible empty diff.
        let stale_checkpoint = CommitId::for_test_label("stale-checkpoint");
        let mut writes = StorageWriteSet::new();
        stage_tracked_working_diff_epoch(
            &mut writes,
            branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: stale_checkpoint,
                generation: checkpoint,
                coverage: second_coverage,
            },
        )
        .expect("stage same-generation stale epoch");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit same-generation stale epoch");
        for request in [
            TrackedStateDiffRequest::default(),
            TrackedStateDiffRequest {
                filter: TrackedStateFilter {
                    row_pks: vec![row_pk.clone()],
                    ..Default::default()
                },
                ..Default::default()
            },
        ] {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open stale epoch direct read");
            assert!(
                TrackedHeadContext::new()
                    .reader(read)
                    .working_diff_for_control(
                        branch_id,
                        control(second_head, checkpoint, no_op_checkpoint),
                        &request,
                    )
                    .await
                    .expect("stale epoch should not error")
                    .is_none(),
                "a same-generation stale epoch must select canonical replay"
            );
        }

        let mut writes = StorageWriteSet::new();
        stage_tracked_working_diff_epoch(
            &mut writes,
            branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: no_op_checkpoint,
                generation: checkpoint,
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
                .working_diff_for_control(
                    branch_id,
                    control(second_head, checkpoint, no_op_checkpoint),
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
        let control = BranchHeadControl {
            head_commit_id: head,
            tracked_generation: generation,
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id: None,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-02T00:00:00Z"),
            ref_change_id: ChangeId::for_test_label("branch-ref"),
            accelerator_root_set_digest:
                crate::tracked_state::accelerator_root_set_digest(None)
                    .expect("empty accelerator selection should hash"),
        };
        let snapshot_content = r#"{"snapshot":true}"#;
        let long_metadata = format!("\"{}\"", "x".repeat(300));
        let mut writes = StorageWriteSet::new();
        let mut json_writer = JsonStoreContext::new().writer();
        let refs = json_writer
            .stage_batch(
                &mut writes,
                JsonWritePlacementRef::OutOfBand,
                [
                    NormalizedJsonRef::new(snapshot_content),
                    NormalizedJsonRef::new(&long_metadata),
                ],
            )
            .expect("stage out-of-band JSON");
        let snapshot_ref = refs[0];
        let metadata_ref = refs[1];
        let row_identity = identity(branch_id, generation, "row");
        stage_put(
            &mut writes,
            &row_identity,
            &HeadValue {
                change_id: Some(ChangeId::for_test_label("change")),
                commit_id: Some(head),
                untracked: false,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-02T00:00:00Z"),
                snapshot: JsonSlot::Ref(snapshot_ref),
                metadata: JsonSlot::Ref(metadata_ref),
                columnar_base_coordinate: None,
            },
        )
        .expect("stage v3 row");
        stage_put(
            &mut writes,
            &identity(branch_id, generation, "deleted"),
            &HeadValue {
                change_id: Some(ChangeId::for_test_label("deleted-change")),
                commit_id: Some(head),
                untracked: false,
                deleted: true,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-02T00:00:00Z"),
                snapshot: JsonSlot::None,
                metadata: JsonSlot::None,
                columnar_base_coordinate: None,
            },
        )
        .expect("stage tombstone member");
        stage_branch_head_control(&mut writes, branch_id, control)
            .expect("stage matching branch control");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit v3 head");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open direct row snapshot read");
        let exact_row_pks = vec![
            RowPk::single("missing"),
            RowPk::single("row"),
            RowPk::single("row"),
        ];
        let snapshots = TrackedHeadContext::new()
            .reader(read)
            .scan_row_snapshots(branch_id, control, "schema", &exact_row_pks, None)
            .await
            .expect("direct row snapshots should read");
        assert_eq!(snapshots.len(), 1, "tombstone must not reach SQL rows");
        assert_eq!(
            snapshots[0]
                .as_deref()
                .and_then(|snapshot| std::str::from_utf8(snapshot).ok()),
            Some(snapshot_content),
            "out-of-band snapshot must be hydrated before Arrow decoding"
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open limited direct row snapshot read");
        let snapshots = TrackedHeadContext::new()
            .reader(read)
            .scan_row_snapshots(branch_id, control, "schema", &[], Some(1))
            .await
            .expect("limited direct row snapshots should read");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(
            snapshots[0]
                .as_deref()
                .and_then(|snapshot| std::str::from_utf8(snapshot).ok()),
            Some(snapshot_content)
        );

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
                row_pk: RowPk::single("row"),
                file_id: None,
            },
            TrackedStateKey {
                schema_key: "schema".to_string(),
                row_pk: RowPk::single("row"),
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
            assert_eq!(row.snapshot_content.as_deref(), Some(snapshot_content));
            assert_eq!(row.metadata.as_deref(), Some(long_metadata.as_str()));
            assert_eq!(row.change_id, Some(ChangeId::for_test_label("change")));
            assert_eq!(row.commit_id, Some(head));
        }
    }

    #[tokio::test]
    async fn row_snapshot_scan_restores_logical_primary_key_order() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("generation");
        let head = CommitId::for_test_label("head");
        let control = BranchHeadControl {
            head_commit_id: head,
            tracked_generation: generation,
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id: None,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            ref_change_id: ChangeId::for_test_label("branch-ref"),
            accelerator_root_set_digest:
                crate::tracked_state::accelerator_root_set_digest(None)
                    .expect("empty accelerator selection should hash"),
        };
        let mut writes = StorageWriteSet::new();
        for (row, file_id) in [("a", "z-file"), ("b", "a-file")] {
            let identity = HeadIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: "schema".to_string(),
                row_pk: RowPk::single(row),
                file_id: Some(file_id.to_string()),
            };
            stage_put(
                &mut writes,
                &identity,
                &HeadValue {
                    change_id: Some(ChangeId::for_test_label(row)),
                    commit_id: Some(head),
                    untracked: false,
                    deleted: false,
                    created_at: ts("2026-01-01T00:00:00Z"),
                    updated_at: ts("2026-01-01T00:00:00Z"),
                    snapshot: JsonSlot::from_json(&format!(r#"{{"row":"{row}"}}"#)),
                    metadata: JsonSlot::None,
                    columnar_base_coordinate: None,
                },
            )
            .expect("stage file-backed row");
        }
        stage_branch_head_control(&mut writes, branch_id, control)
            .expect("stage matching branch control");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit file-backed rows");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open row snapshot read");
        let snapshots = TrackedHeadContext::new()
            .reader(read)
            .scan_row_snapshots(branch_id, control, "schema", &[], None)
            .await
            .expect("scan snapshots");
        let snapshots = snapshots
            .into_iter()
            .map(|snapshot| {
                String::from_utf8(snapshot.expect("row has a snapshot").to_vec())
                    .expect("snapshot is UTF-8")
            })
            .collect::<Vec<_>>();
        assert_eq!(snapshots, [r#"{"row":"a"}"#, r#"{"row":"b"}"#]);
    }

    #[tokio::test]
    async fn file_id_reads_use_file_first_primary_values() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let head = CommitId::for_test_label("head");
        let control = BranchHeadControl {
            head_commit_id: head,
            tracked_generation: head,
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id: None,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            ref_change_id: ChangeId::for_test_label("branch-ref"),
            accelerator_root_set_digest:
                crate::tracked_state::accelerator_root_set_digest(None)
                    .expect("empty accelerator selection should hash"),
        };
        let row_pk = RowPk::single("row");
        let second_row_pk = RowPk::single("row-2");
        let deltas = [
            TrackedHeadDeltaRef {
                schema_key: "schema",
                file_id: None,
                row_pk: &row_pk,
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
                file_id: Some("01920000-0000-7000-8000-0000000000a2"),
                row_pk: &row_pk,
                change_id: ChangeId::for_test_label("01920000-0000-7000-8000-0000000000a2"),
                commit_id: head,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                snapshot: JsonSlotRef::Inline("{\"value\":\"a\"}"),
                metadata: JsonSlotRef::None,
            },
            TrackedHeadDeltaRef {
                schema_key: "schema",
                file_id: Some("01920000-0000-7000-8000-0000000000b2"),
                row_pk: &row_pk,
                change_id: ChangeId::for_test_label("01920000-0000-7000-8000-0000000000b2"),
                commit_id: head,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                snapshot: JsonSlotRef::Inline("{\"value\":\"b\"}"),
                metadata: JsonSlotRef::None,
            },
            TrackedHeadDeltaRef {
                schema_key: "schema",
                file_id: Some("01920000-0000-7000-8000-0000000000b2"),
                row_pk: &second_row_pk,
                change_id: ChangeId::for_test_label("second-01920000-0000-7000-8000-0000000000b2"),
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
            .expect("stage hot head");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit hot head");

        let projection_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open file schema marker verification read");
        let projection_rows = scan_test_space(&projection_read, FILE_SPACE).await;
        assert_eq!(
            projection_rows.len(),
            1,
            "file rows share one conservative schema marker"
        );
        drop(projection_read);

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
                        row_pks: vec![row_pk.clone()],
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
            vec![
                None,
                Some("01920000-0000-7000-8000-0000000000a2"),
                Some("01920000-0000-7000-8000-0000000000b2")
            ]
        );

        // A null-file predicate selects only the null-file hot row.
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open null file-id read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &head.to_string(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema".to_string()],
                        row_pks: vec![row_pk.clone()],
                        file_ids: vec![NullableKeyFilter::Null],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("null file-id read should execute")
            .expect("marker should match");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].file_id, None);

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
                        row_pks: vec![row_pk.clone()],
                        file_ids: vec![NullableKeyFilter::Value(
                            "01920000-0000-7000-8000-0000000000b2".to_string(),
                        )],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("filtered file scan should execute")
            .expect("marker should match");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].file_id.as_deref(),
            Some("01920000-0000-7000-8000-0000000000b2")
        );

        // A schema-scoped `file_id = $1` query reads the hydrated file-first
        // primary range directly. This is the access pattern used by
        // filesystem-backed row scans, where the row PK is not known
        // before the query.
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
                        file_ids: vec![NullableKeyFilter::Value(
                            "01920000-0000-7000-8000-0000000000b2".to_string(),
                        )],
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
                .map(|row| row.row_pk.as_single_string().expect("single key"))
                .collect::<Vec<_>>(),
            vec!["row", "row-2"]
        );
        assert!(
            rows.iter()
                .all(|row| row.file_id.as_deref() == Some("01920000-0000-7000-8000-0000000000b2"))
        );

        // The branch control validates the published hot generation. Exact
        // file identity and schema-scoped file-id reads route through the V18
        // file-first primary index.
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open control-gated file-id scan");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_batch_for_retention(
                branch_id,
                control,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema".to_string()],
                        file_ids: vec![NullableKeyFilter::Value(
                            "01920000-0000-7000-8000-0000000000b2".to_string(),
                        )],
                        ..Default::default()
                    },
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("control-bound file-id scan should execute");
        let rows = rows.into_rows();
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .all(|row| row.file_id.as_deref() == Some("01920000-0000-7000-8000-0000000000b2"))
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
                    row_pk,
                    file_id: Some("01920000-0000-7000-8000-0000000000b2".to_string()),
                }],
                &ChangeRecordProjection::full(),
            )
            .await
            .expect("exact file read should execute")
            .expect("marker should match");
        assert_eq!(rows.len(), 1);
        let row = rows[0].as_ref().expect("explicit file row should resolve");
        assert_eq!(
            row.file_id.as_deref(),
            Some("01920000-0000-7000-8000-0000000000b2")
        );
        assert_eq!(row.snapshot_content.as_deref(), Some("{\"value\":\"b\"}"));

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open control-gated exact file read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .load_projected_live_rows(
                branch_id,
                control,
                &[TrackedStateKey {
                    schema_key: "schema".to_string(),
                    row_pk: RowPk::single("row"),
                    file_id: Some("01920000-0000-7000-8000-0000000000b2".to_string()),
                }],
                &ChangeRecordProjection::full(),
            )
            .await
            .expect("control-bound exact file read should execute");
        assert_eq!(rows.len(), 1);
        let row = rows[0]
            .as_ref()
            .expect("explicit file row should resolve through its projection");
        assert_eq!(
            row.file_id.as_deref(),
            Some("01920000-0000-7000-8000-0000000000b2")
        );
        assert_eq!(row.snapshot_content.as_deref(), Some("{\"value\":\"b\"}"));
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
                row_pk: RowPk::single("row-a"),
                file_id: None,
            },
            HeadIdentity {
                branch_id: "branch".to_string(),
                generation,
                schema_key: "schema-a".to_string(),
                row_pk: RowPk::single("row-z"),
                file_id: Some("01920000-0000-7000-8000-0000000000a2".to_string()),
            },
            HeadIdentity {
                branch_id: "branch".to_string(),
                generation,
                schema_key: "schema-a".to_string(),
                row_pk: RowPk::single("row-a"),
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
        stage_test_current_control(&mut writes, "branch", head, generation, None)
            .expect("stage current control");
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
                .map(|row| (row.schema_key, row.row_pk, row.file_id))
                .collect::<Vec<_>>(),
            expected
                .into_iter()
                .map(|identity| (identity.schema_key, identity.row_pk, identity.file_id))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn branch_control_gates_generations_and_rows_roundtrip() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("generation");
        let head = CommitId::for_test_label("head");
        let identity = identity("branch", generation, "row");
        let value = HeadValue {
            change_id: Some(ChangeId::for_test_label("change")),
            commit_id: Some(head),
            untracked: false,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:01Z"),
            snapshot: JsonSlot::from_json("{\"id\":\"row\"}"),
            metadata: JsonSlot::None,
            columnar_base_coordinate: None,
        };
        let mut writes = StorageWriteSet::new();
        stage_put(&mut writes, &identity, &value).expect("stage row");
        stage_test_current_control(&mut writes, "branch", head, generation, None)
            .expect("stage current control");
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
        let row_pk = RowPk::single("row");
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
                    row_pk: &row_pk,
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
                    row_pk: &row_pk,
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
    async fn incremental_row_update_preserves_siblings_and_file_schema_marker() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("first-head");
        let second_head = CommitId::for_test_label("second-head");
        let row_pk = RowPk::single("row");

        let mut writes = StorageWriteSet::new();
        for (file_id, change_id) in [
            (None, "none-first"),
            (
                Some("01920000-0000-7000-8000-0000000000a2"),
                "01920000-0000-7000-8000-0000000000a2-first",
            ),
            (
                Some("01920000-0000-7000-8000-0000000000b2"),
                "01920000-0000-7000-8000-0000000000b2-first",
            ),
        ] {
            stage_put(
                &mut writes,
                &HeadIdentity {
                    branch_id: branch_id.to_string(),
                    generation,
                    schema_key: "schema".to_string(),
                    row_pk: row_pk.clone(),
                    file_id: file_id.map(str::to_string),
                },
                &head_value(change_id, generation),
            )
            .expect("stage initial hot row");
        }
        stage_test_current_control(&mut writes, branch_id, generation, generation, None)
            .expect("stage initial current control");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit initial hot rows");

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
                    file_id: Some("01920000-0000-7000-8000-0000000000a2"),
                    row_pk: &row_pk,
                    change_id: ChangeId::for_test_label(
                        "01920000-0000-7000-8000-0000000000a2-second",
                    ),
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
            .expect("stage direct row update");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit direct row update");

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
            .expect("scan hot rows")
            .expect("matching marker");
        assert_eq!(rows.len(), 3);
        let none = rows
            .iter()
            .find(|row| row.file_id.is_none())
            .expect("null-file row remains");
        let file_a = rows
            .iter()
            .find(|row| row.file_id.as_deref() == Some("01920000-0000-7000-8000-0000000000a2"))
            .expect("changed file row remains");
        let file_b = rows
            .iter()
            .find(|row| row.file_id.as_deref() == Some("01920000-0000-7000-8000-0000000000b2"))
            .expect("untouched file row remains");
        assert_eq!(none.change_id, Some(ChangeId::for_test_label("none-first")));
        assert_eq!(
            file_a.change_id,
            Some(ChangeId::for_test_label(
                "01920000-0000-7000-8000-0000000000a2-second"
            ))
        );
        assert_eq!(
            file_b.change_id,
            Some(ChangeId::for_test_label(
                "01920000-0000-7000-8000-0000000000b2-first"
            ))
        );
        assert_eq!(file_a.created_at, ts("2026-01-01T00:00:00Z"));
        assert_eq!(file_a.updated_at, ts("2026-01-02T00:00:00Z"));

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open file-schema marker verification read");
        let projections = scan_test_space(&read, FILE_SPACE).await;
        assert_eq!(projections.len(), 1);
        assert!(
            projections.into_iter().all(|projection| {
                full_value_bytes(projection.value).is_ok_and(|bytes| bytes.is_empty())
            }),
            "schema membership markers remain key-only"
        );
    }

    #[tokio::test]
    async fn incremental_row_update_does_not_decode_unrelated_hot_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("first-head");
        let second_head = CommitId::for_test_label("second-head");
        let row_pk = RowPk::single("row");
        let unrelated_pk = RowPk::single("unrelated");

        let mut initial_writes = StorageWriteSet::new();
        stage_put(
            &mut initial_writes,
            &HeadIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: "schema".to_string(),
                row_pk: row_pk.clone(),
                file_id: Some("01920000-0000-7000-8000-0000000000a2".to_string()),
            },
            &head_value("01920000-0000-7000-8000-0000000000a2-first", generation),
        )
        .expect("stage target hot row");
        stage_put(
            &mut initial_writes,
            &HeadIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: "schema".to_string(),
                row_pk: unrelated_pk,
                file_id: None,
            },
            &head_value("unrelated", generation),
        )
        .expect("stage unrelated hot row");
        stage_test_current_control(&mut initial_writes, branch_id, generation, generation, None)
            .expect("stage initial control");
        storage
            .commit_write_set(initial_writes, StorageWriteOptions::default())
            .await
            .expect("commit initial hot rows");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open corruption fixture read");
        let unrelated_key = scan_test_space(&read, ROW_SPACE)
            .await
            .into_iter()
            .find_map(|entry| {
                let value = full_value_bytes(entry.value).ok()?;
                let value = decode_head_value(&value).ok()?;
                (value.change_id == Some(ChangeId::for_test_label("unrelated")))
                    .then_some(entry.key)
            })
            .expect("find unrelated row key");
        drop(read);
        let mut corrupt_writes = StorageWriteSet::new();
        corrupt_writes.put(
            ROW_SPACE,
            unrelated_key,
            StorageValue {
                bytes: Bytes::from_static(b"corrupt unrelated hot row"),
            },
        );
        storage
            .commit_write_set(corrupt_writes, StorageWriteOptions::default())
            .await
            .expect("commit corrupt unrelated row");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open targeted incremental read");
        let mut writes = StorageWriteSet::new();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                Some(generation),
                second_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: Some("01920000-0000-7000-8000-0000000000a2"),
                    row_pk: &row_pk,
                    change_id: ChangeId::for_test_label(
                        "01920000-0000-7000-8000-0000000000a2-second",
                    ),
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
            .expect("unrelated corrupt row must not block a direct update");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit direct hot-row update");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact file verification read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .load_projected_live_rows_if_current(
                branch_id,
                &second_head.to_string(),
                &[TrackedStateKey {
                    schema_key: "schema".to_string(),
                    row_pk,
                    file_id: Some("01920000-0000-7000-8000-0000000000a2".to_string()),
                }],
                &ChangeRecordProjection::full(),
            )
            .await
            .expect("exact file read should execute")
            .expect("matching current control");
        assert_eq!(
            rows[0]
                .as_ref()
                .expect("target file row survives")
                .change_id,
            Some(ChangeId::for_test_label(
                "01920000-0000-7000-8000-0000000000a2-second"
            ))
        );
    }

    #[tokio::test]
    async fn incremental_singleton_insert_rejects_existing_live_row() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("first-head");
        let second_head = CommitId::for_test_label("second-head");
        let row_pk = RowPk::single("row");
        let identity = identity(branch_id, generation, "row");

        let mut writes = StorageWriteSet::new();
        stage_put(
            &mut writes,
            &identity,
            &head_value("first-change", generation),
        )
        .expect("stage existing live row");
        stage_test_current_control(&mut writes, branch_id, generation, generation, None)
            .expect("stage existing current control");
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
            row_pk: row_pk.clone(),
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
                    row_pk: &row_pk,
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
    async fn incremental_file_descriptor_delete_cascades_without_resurrection() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let first_head = CommitId::for_test_label("file-cascade-first");
        let delete_head = CommitId::for_test_label("file-cascade-delete");
        let recreate_head = CommitId::for_test_label("file-cascade-recreate");
        let file_pk = RowPk::single("file-a");
        let file_row_pk = RowPk::single("file-row");
        let unrelated_pk = RowPk::single("unrelated-row");

        let mut writes = StorageWriteSet::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open initial file-cascade read");
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                None,
                first_head,
                &[
                    TrackedHeadDeltaRef {
                        schema_key: "lix_file_descriptor",
                        file_id: None,
                        row_pk: &file_pk,
                        change_id: ChangeId::for_test_label("file-create"),
                        commit_id: first_head,
                        deleted: false,
                        created_at: ts("2026-01-01T00:00:00Z"),
                        updated_at: ts("2026-01-01T00:00:00Z"),
                        snapshot: JsonSlotRef::Inline("{\"name\":\"a\"}"),
                        metadata: JsonSlotRef::None,
                    },
                    TrackedHeadDeltaRef {
                        schema_key: "semantic",
                        file_id: Some("file-a"),
                        row_pk: &file_row_pk,
                        change_id: ChangeId::for_test_label("file-row-create"),
                        commit_id: first_head,
                        deleted: false,
                        created_at: ts("2026-01-01T00:00:00Z"),
                        updated_at: ts("2026-01-01T00:00:00Z"),
                        snapshot: JsonSlotRef::Inline("{\"value\":1}"),
                        metadata: JsonSlotRef::None,
                    },
                    TrackedHeadDeltaRef {
                        schema_key: "semantic",
                        file_id: Some("file-b"),
                        row_pk: &unrelated_pk,
                        change_id: ChangeId::for_test_label("unrelated-create"),
                        commit_id: first_head,
                        deleted: false,
                        created_at: ts("2026-01-01T00:00:00Z"),
                        updated_at: ts("2026-01-01T00:00:00Z"),
                        snapshot: JsonSlotRef::Inline("{\"value\":2}"),
                        metadata: JsonSlotRef::None,
                    },
                ],
                &BTreeSet::new(),
                None,
            )
            .await
            .expect("stage initial file state");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit initial file state");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open file-delete read");
        let mut writes = StorageWriteSet::new();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                Some(first_head),
                delete_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "lix_file_descriptor",
                    file_id: None,
                    row_pk: &file_pk,
                    change_id: ChangeId::for_test_label("file-delete"),
                    commit_id: delete_head,
                    deleted: true,
                    created_at: ts("2026-01-01T00:00:00Z"),
                    updated_at: ts("2026-01-02T00:00:00Z"),
                    snapshot: JsonSlotRef::None,
                    metadata: JsonSlotRef::None,
                }],
                &BTreeSet::new(),
                None,
            )
            .await
            .expect("stage cascading file delete");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit cascading file delete");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open cascade verification read");
        let mut including_tombstones = TrackedStateScanRequest::default();
        including_tombstones.filter.include_tombstones = true;
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(branch_id, &delete_head.to_string(), &including_tombstones)
            .await
            .expect("scan cascaded state")
            .expect("matching delete head");
        let cascaded = rows
            .iter()
            .find(|row| row.row_pk == file_row_pk)
            .expect("file-scoped row remains as a visibility tombstone");
        assert!(cascaded.deleted);
        assert_eq!(
            cascaded.change_id,
            Some(ChangeId::for_test_label("file-delete"))
        );
        assert!(
            rows.iter()
                .any(|row| row.row_pk == unrelated_pk && !row.deleted),
            "unrelated file state must remain live"
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open file-recreate read");
        let mut writes = StorageWriteSet::new();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                Some(first_head),
                recreate_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "lix_file_descriptor",
                    file_id: None,
                    row_pk: &file_pk,
                    change_id: ChangeId::for_test_label("file-recreate"),
                    commit_id: recreate_head,
                    deleted: false,
                    created_at: ts("2026-01-03T00:00:00Z"),
                    updated_at: ts("2026-01-03T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"name\":\"a\"}"),
                    metadata: JsonSlotRef::None,
                }],
                &BTreeSet::new(),
                None,
            )
            .await
            .expect("stage file recreation");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit file recreation");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open recreation verification read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &recreate_head.to_string(),
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan recreated state")
            .expect("matching recreate head");
        assert!(
            rows.iter().all(|row| row.row_pk != file_row_pk),
            "recreating a file descriptor must not resurrect old scoped state"
        );
    }

    #[tokio::test]
    async fn incremental_guarded_insert_resurrects_tombstone_with_first_created_at() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("first-head");
        let second_head = CommitId::for_test_label("second-head");
        let row_pk = RowPk::single("row");
        let identity = identity(branch_id, generation, "row");

        let mut tombstone = head_value("first-delete", generation);
        tombstone.deleted = true;
        tombstone.updated_at = ts("2026-01-02T00:00:00Z");
        tombstone.snapshot = JsonSlot::None;
        tombstone.metadata = JsonSlot::None;
        let mut writes = StorageWriteSet::new();
        stage_put(&mut writes, &identity, &tombstone).expect("stage existing tombstone");
        stage_test_current_control(&mut writes, branch_id, generation, generation, None)
            .expect("stage existing current control");
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
            row_pk: row_pk.clone(),
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
                    row_pk: &row_pk,
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
        let row_pk = RowPk::single("row");
        let parent_head = CommitId::for_test_label("parent-head");
        let child_head = CommitId::for_test_label("child-head");
        let parent_rows = vec![MaterializedTrackedStateRow {
            row_pk: row_pk.clone(),
            schema_key: "schema".to_string(),
            file_id: None,
            snapshot_content: Some("{\"value\":1}".into()),
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
                    row_pk: &row_pk,
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
            DIFF_SPACE,
            malformed_index_key.clone(),
            StorageValue {
                bytes: Bytes::from_static(b"not-a-hot-diff-before-image"),
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
            (DIFF_SPACE, malformed_index_key),
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
}
