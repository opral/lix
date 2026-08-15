use crate::NullableKeyFilter;
use crate::changelog::{ChangeId, CommitId};
use crate::common::{LixTimestamp, SharedStr};
use crate::row_pk::RowPk;
use bytes::Bytes;

pub(crate) const TRACKED_STATE_HASH_BYTES: usize = 32;
pub(crate) const COMMIT_STATE_MAX_REPLAY_DEPTH: u16 = 32;
pub(crate) const COMMIT_STATE_MAX_REPLAY_BYTES: u64 = 256 * 1024 * 1024;

/// Content-addressed root id for one tracked-state commit-root tree.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, musli::Encode, musli::Decode)]
pub(crate) struct TrackedStateRootId(#[musli(bytes)] [u8; TRACKED_STATE_HASH_BYTES]);

impl TrackedStateRootId {
    pub(crate) fn new(bytes: [u8; TRACKED_STATE_HASH_BYTES]) -> Self {
        Self(bytes)
    }

    pub(crate) fn as_bytes(&self) -> &[u8; TRACKED_STATE_HASH_BYTES] {
        &self.0
    }
}

/// Root-independent tracked row primary key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TrackedStateKey {
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) row_pk: RowPk,
}

/// Zero-copy view of primary tracked-state key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TrackedStateKeyRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) row_pk: &'a RowPk,
}

/// Zero-copy tracked-state commit-root delta prepared from changelog facts.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateDeltaRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) row_pk: &'a RowPk,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
}

/// Physical location of a row snapshot in an immutable columnar base.
///
/// Commit deltas carry this coordinate alongside their authoritative payload,
/// allowing exact identity lookups to reconcile an overlay row with its base
/// row without reading a second index. The commit id owns the referenced base
/// layout; group and row ordinals are intentionally fixed-width so the packed
/// commit-delta sidecar remains compact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct TrackedStateBaseCoordinate {
    pub(crate) base_commit_id: CommitId,
    pub(crate) group_index: u32,
    pub(crate) row_index: u32,
}

/// Payload-bearing immutable commit member.
///
/// Root mutation logic consumes only [`TrackedStateDeltaRef`]. Packed commit
/// storage additionally requires the complete authoritative payload; there is
/// no optional/non-authoritative representation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateCommitDeltaRef<'a> {
    pub(crate) delta: TrackedStateDeltaRef<'a>,
    pub(crate) snapshot: crate::json_store::JsonSlotRef<'a>,
    pub(crate) metadata: crate::json_store::JsonSlotRef<'a>,
    pub(crate) origin_key: Option<&'a str>,
    pub(crate) base_coordinate: Option<TrackedStateBaseCoordinate>,
    pub(crate) authored: bool,
}

/// Typed complete-replacement member produced directly by the transaction
/// journal's dominant one-column string identity lane. Invalid mutation
/// states (delete, selected-source payload, origin override) are deliberately
/// unrepresentable, so immutable replacement parts can be sealed without
/// constructing an `RowPk` per row.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateSingleStringReplacementRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) row_pk: &'a str,
    pub(crate) commit_id: CommitId,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) snapshot: crate::json_store::JsonSlotRef<'a>,
    pub(crate) metadata: crate::json_store::JsonSlotRef<'a>,
}

/// One ordered tracked-root mutation with its insert-collision contract.
///
/// Bulk commit assembly keeps this zero-copy form until it has compared the
/// mutation with the parent leaf. That lets the common full-batch path retain
/// only one incoming key/value at a time instead of a second `Vec` plus a
/// cloned absence-guard set.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateRootMutationRef<'a> {
    pub(crate) delta: TrackedStateDeltaRef<'a>,
    pub(crate) require_absence: bool,
}

/// Value stored in tracked-state commit-root trees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateIndexValue {
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
}

impl TrackedStateIndexValue {
    pub(crate) fn created_at(&self) -> LixTimestamp {
        self.created_at
    }

    pub(crate) fn updated_at(&self) -> LixTimestamp {
        self.updated_at
    }

    pub(crate) fn deleted(&self) -> bool {
        self.deleted
    }
}

/// Zero-copy view of a tracked-state commit-root value.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateIndexValueRef {
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
}

/// Durable tracked-state root metadata for one commit.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct TrackedStateCommitRoot {
    pub(crate) commit_id: CommitId,
    pub(crate) root_id: TrackedStateRootId,
    pub(crate) parent_roots: Vec<TrackedStateCommitRootParent>,
    pub(crate) changed_key_count: u64,
    pub(crate) row_count_estimate: u64,
    pub(crate) tree_height: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct TrackedStateCommitRootParent {
    pub(crate) commit_id: CommitId,
    pub(crate) root_id: TrackedStateRootId,
}

/// Bounded first-parent replay work carried by a commit's canonical mutation
/// interval.
///
/// Zero debt means the manifest's snapshot root is the canonical serving
/// layout. Nonzero debt means readers reconstruct state from the bounded
/// interval and the manifest must not publish a snapshot root.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitStateReplayDebt {
    pub(crate) depth: u16,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
}

/// Key bounds for one existing commit-addressed mutation segment.
///
/// Segment slot order is durable because directly addressable `ChangeId`s
/// encode that slot and the row ordinal. Snapshot compaction may replace the
/// optional root, but must never reorder these entries.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitStateMutationPart {
    #[musli(bytes)]
    pub(crate) first_key: Vec<u8>,
    #[musli(bytes)]
    pub(crate) last_key: Vec<u8>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) replacement_part: Option<StoredReplacementPart>,
}

/// One lossless row-columnar generation used directly as authored history.
///
/// The row-group manifest binds every column digest. Uniform lifecycle and
/// origin metadata remain in the commit authority instead of being repeated
/// in every row. Direct change addresses retain their established 512-row
/// logical slots and translate to these larger physical groups by ordinal.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct ColumnarMutationPartSet {
    pub(crate) owner_commit_id: [u8; 16],
    pub(crate) row_group_set_id: [u8; 16],
    pub(crate) manifest_digest: [u8; 32],
    pub(crate) schema_key: String,
    pub(crate) row_count: u32,
    pub(crate) group_row_counts: Vec<u32>,
    #[musli(bytes)]
    pub(crate) first_key: Vec<u8>,
    #[musli(bytes)]
    pub(crate) last_key: Vec<u8>,
    pub(crate) page_first_keys: Vec<Vec<u8>>,
    pub(crate) page_last_keys: Vec<Vec<u8>>,
    pub(crate) uniform_created_at: LixTimestamp,
    pub(crate) uniform_updated_at: LixTimestamp,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) origin_key: Option<String>,
}

/// One collection partition replaced by a certified immutable generation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitDeltaReplacementScope {
    pub(crate) schema_key: String,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) file_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitDeltaLifecycleSummary {
    pub(crate) scope: CommitDeltaReplacementScope,
    pub(crate) ordered_identity_digest: [u8; 32],
    pub(crate) uniform_created_at: LixTimestamp,
}

/// Durable certificate binding a replacement generation to its owner and
/// immutable replacement-part directory.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct StoredCommitDeltaReplacementGeneration {
    pub(crate) owner_commit_id: [u8; 16],
    pub(crate) scope: CommitDeltaReplacementScope,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) fallback_commit_id: Option<[u8; 16]>,
    pub(crate) integrity_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct StoredReplacementPartsAuthority {
    pub(crate) directory_digest: [u8; 32],
    pub(crate) uniform_updated_at: LixTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct StoredReplacementPart {
    pub(crate) content_digest: [u8; 32],
    pub(crate) owner_commit_id: [u8; 16],
    pub(crate) first_address: u32,
    pub(crate) uniform_created_at: LixTimestamp,
    pub(crate) uniform_updated_at: LixTimestamp,
}

/// One immutable post-image range in a committed current-state partition.
///
/// This is deliberately distinct from [`CommitStateMutationPart`]. Mutation
/// parts describe what a commit authored; current-state parts describe the
/// strictly ordered, non-overlapping state that readers may serve directly.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CurrentStatePartDescriptor {
    #[musli(bytes)]
    pub(crate) first_key: Vec<u8>,
    #[musli(bytes)]
    pub(crate) last_key: Vec<u8>,
    pub(crate) content_digest: [u8; 32],
    /// Which physical source serves this range, plus that source's own
    /// addressing fields.
    pub(crate) source: CurrentStatePartSource,
    /// First physical row selected from the source part. Descriptor slicing
    /// allows sparse deletes and updates to retain untouched source bytes.
    pub(crate) source_row_offset: u16,
    pub(crate) row_count: u16,
    /// True only for structural slices and authored islands introduced by a
    /// sparse rewrite. Canonical encodes clear this bit, making compaction
    /// self-stabilizing without guessing from physical row density.
    pub(crate) fragmented: bool,
}

/// Physical source of one current-state part, with the addressing fields that
/// source actually uses.
///
/// This was previously a `source_kind: u8` discriminator beside the union of
/// every kind's fields, so each locator carried the other kinds' fields pinned
/// to zero and a hand-written validator re-proved that pinning on every
/// decode. Per-variant fields make those combinations unrepresentable instead
/// of merely rejected, and stop the unused fields from being encoded at all.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, musli::Encode, musli::Decode)]
pub(crate) enum CurrentStatePartSource {
    /// An immutable complete-replacement mutation part owned by one commit.
    Replacement(ReplacementPartSource),
    /// A native content-addressed current-state data part. The part's own rows
    /// carry per-row authorship, so the locator has no uniform timestamps and
    /// no owning commit; reachability is proved by the refs summary instead.
    NativeDataPart {
        /// Digest of the part's compact JSON-reference summary.
        payload_refs_digest: [u8; 32],
    },
    /// One authenticated page in a canonical row-group set.
    ColumnarPage(ColumnarPageSource),
}

/// Addressing for a replacement-part source.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct ReplacementPartSource {
    pub(crate) owner_commit_id: [u8; 16],
    /// Replacement segment index within the owner commit.
    pub(crate) part_index: u32,
    pub(crate) uniform_created_at: LixTimestamp,
    pub(crate) uniform_updated_at: LixTimestamp,
}

/// Addressing for one page of a canonical row-group set.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct ColumnarPageSource {
    /// Physical immutable row-group-set id.
    pub(crate) source_id: [u8; 16],
    pub(crate) owner_commit_id: [u8; 16],
    /// Row-group index within the set.
    pub(crate) part_index: u32,
    /// Page index inside `part_index`.
    pub(crate) source_page_index: u16,
    pub(crate) uniform_created_at: LixTimestamp,
    pub(crate) uniform_updated_at: LixTimestamp,
}

/// Manifest-attested root of the unified scope/part serving tree.
///
/// The generic tree owns only authenticated physical routing. These fields
/// bind one result root to the physical serving base and certified mutation
/// authority that produced it. Graph ancestry remains an independent semantic
/// relationship and is not exposed to the tree.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CurrentStateScopedRangeRoot {
    pub(crate) tree: super::scoped_range::ScopedRangeRoot,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) serving_base_commit_id: Option<CommitId>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) serving_base_root_id: Option<[u8; 32]>,
    pub(crate) transition_digest: [u8; 32],
}

/// Cumulative negative-membership certificate for collection scopes.
///
/// A complete filter may have false positives, but never false negatives: a
/// missing schema-family bit therefore proves that no effective graph-parent
/// or selected-source lineage authored any scope for that schema. Coarsening
/// file-scoped collections to their schema avoids cardinality-driven
/// saturation while remaining conservative. Incomplete filters fail closed
/// and carry no bits.
#[derive(Debug, Clone, Default, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitStateTouchedScopeFilter {
    pub(crate) complete: bool,
    #[musli(bytes)]
    pub(crate) bits: Vec<u8>,
}

/// Point-addressable immutable mutation inventory owned by one commit.
///
/// The fields intentionally mirror the existing commit-delta directory. This
/// lets the hard-cut manifest become authoritative without changing the
/// bounded LXCD16 segment and payload-sidecar codec in the same step.
#[derive(Debug, Clone, Default, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitStateMutationInventory {
    /// Certified whole-source alias authority. This is never used for a
    /// finite exact selection: when present, the selected source supplies the
    /// complete inherited state and authored members overlay it during replay.
    #[musli(with = crate::storage_codec::option)]
    pub(crate) selected_source_commit_id: Option<[u8; 16]>,
    pub(crate) member_count: u32,
    pub(crate) selection_fingerprint: [u8; 32],
    /// Exact row counts for every directly addressable part. Empty means the
    /// generic locator-indexed layout.
    pub(crate) direct_part_row_counts: Vec<u16>,
    /// Compact physical identities for a complete replacement. Range bounds
    /// live in the rebuildable current-state directory; history can always
    /// recover them by decoding these immutable parts.
    pub(crate) replacement_part_digests: Vec<[u8; 32]>,
    /// Authoritative collection-replacement scope. A miss within this scope
    /// cannot fall through to an older first-parent generation.
    #[musli(with = crate::storage_codec::option)]
    pub(crate) single_partition: Option<CommitDeltaReplacementScope>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) lifecycle_summary: Option<CommitDeltaLifecycleSummary>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) replacement_generation: Option<StoredCommitDeltaReplacementGeneration>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) replacement_parts: Option<StoredReplacementPartsAuthority>,
    /// Lossless typed post-images that are both the authored mutation payload
    /// and the serving columnar source. When present, legacy LXCD parts are
    /// forbidden rather than retained as a compatibility copy.
    #[musli(with = crate::storage_codec::option)]
    pub(crate) columnar_parts: Option<ColumnarMutationPartSet>,
    /// Tiny commits retain their only part inline so an exact history lookup
    /// remains one backend point read.
    #[musli(bytes)]
    pub(crate) inline_part: Vec<u8>,
    pub(crate) parts: Vec<CommitStateMutationPart>,
}

impl CommitStateMutationInventory {
    pub(crate) fn selected_source_commit_id(&self) -> Option<CommitId> {
        self.selected_source_commit_id
            .map(|bytes| CommitId::new(uuid::Uuid::from_bytes(bytes)))
    }

    pub(crate) fn part_count(&self) -> usize {
        self.columnar_parts
            .as_ref()
            .map_or(0, |parts| parts.group_row_counts.len())
            + usize::from(!self.inline_part.is_empty())
            + if self.replacement_part_digests.is_empty() {
                self.parts.len()
            } else {
                self.replacement_part_digests.len()
            }
    }

    /// Whether this local inventory can contain a finite selected member whose
    /// payload owner must be resolved through the canonical change locator.
    ///
    /// A selected-source alias is whole-source authority, not a finite
    /// selection. A non-empty direct-address inventory proves every member
    /// owns its encoded slot, and columnar parts are authored history by
    /// contract. Only the remaining mixed/generic layouts require decoding
    /// local members to distinguish authored rows from finite selections.
    pub(crate) fn may_contain_finite_selected_members(&self) -> bool {
        self.member_count != 0
            && self.selected_source_commit_id.is_none()
            && self.direct_part_row_counts.is_empty()
            && self.columnar_parts.is_none()
    }
}

/// Immutable physical authority for one tracked commit.
///
/// Compact topology projections, point locators, current-state HOT rows, and
/// snapshot tree chunks remain rebuildable serving indexes. None may carry
/// semantic commit facts, which belong exclusively to `changelog.commit`.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitStateManifest {
    pub(crate) commit_id: CommitId,
    /// Physical decode dictionary for authored mutation rows. This is not
    /// commit-account authority: it remains with retained immutable payloads
    /// even if GC removes the semantic commit projection.
    pub(crate) change_account_id: String,
    pub(crate) replay_debt: CommitStateReplayDebt,
    pub(crate) mutations: CommitStateMutationInventory,
    pub(crate) touched_scope_filter: CommitStateTouchedScopeFilter,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) current_state_scoped_ranges: Option<Box<CurrentStateScopedRangeRoot>>,
    /// Canonical snapshot metadata when this commit was published as a root
    /// fence. The tree chunks are rebuildable by content hash; this immutable
    /// pointer is the authority that permits readers to serve them.
    #[musli(with = crate::storage_codec::option)]
    pub(crate) snapshot_root: Option<Box<TrackedStateCommitRoot>>,
}

/// Materialized tracked-state commit-root row.
///
/// Tracked rows are the serving state that can be rebuilt from changelog facts.
/// They intentionally do not carry an `untracked` flag: commit roots contain
/// tracked history only. Mutable untracked rows share the current-state
/// projection with tracked rows, but never enter a commit root or changelog.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MaterializedTrackedStateRow {
    pub(crate) row_pk: RowPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<SharedStr>,
    pub(crate) metadata: Option<SharedStr>,
    pub(crate) deleted: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
}

/// Identity-centered filter for tracked-state scans.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct TrackedStateFilter {
    #[serde(default)]
    pub(crate) schema_keys: Vec<String>,
    #[serde(default)]
    pub(crate) row_pks: Vec<RowPk>,
    #[serde(default)]
    pub(crate) row_pk_lower: Option<RowPkRangeBound>,
    #[serde(default)]
    pub(crate) row_pk_upper: Option<RowPkRangeBound>,
    #[serde(default)]
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    #[serde(default)]
    pub(crate) include_tombstones: bool,
}

/// One canonical bound over the typed primary-key ordering.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RowPkRangeBound {
    pub(crate) row_pk: RowPk,
    pub(crate) inclusive: bool,
}

impl TrackedStateFilter {
    pub(crate) fn matches_row_pk(&self, row_pk: &RowPk) -> bool {
        (self.row_pks.is_empty() || self.row_pks.contains(row_pk))
            && row_pk_satisfies_bounds(
                row_pk,
                self.row_pk_lower.as_ref(),
                self.row_pk_upper.as_ref(),
            )
    }
}

pub(crate) fn row_pk_satisfies_bounds(
    row_pk: &RowPk,
    lower: Option<&RowPkRangeBound>,
    upper: Option<&RowPkRangeBound>,
) -> bool {
    lower.is_none_or(|bound| {
        row_pk > &bound.row_pk || (bound.inclusive && row_pk == &bound.row_pk)
    }) && upper.is_none_or(|bound| {
        row_pk < &bound.row_pk || (bound.inclusive && row_pk == &bound.row_pk)
    })
}

/// Requested property set for a tracked-state scan.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct TrackedStateReadColumns {
    #[serde(default)]
    pub(crate) columns: Vec<String>,
}

/// Scan request for tracked-state commit roots.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct TrackedStateScanRequest {
    #[serde(default)]
    pub(crate) filter: TrackedStateFilter,
    #[serde(default)]
    pub(crate) read_columns: TrackedStateReadColumns,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TrackedStateMutation {
    pub(crate) encoded_key: Bytes,
    pub(crate) encoded_value: Bytes,
}

impl TrackedStateMutation {
    #[cfg(test)]
    pub(crate) fn put_encoded(encoded_key: Vec<u8>, encoded_value: Vec<u8>) -> Self {
        Self {
            encoded_key: Bytes::from(encoded_key),
            encoded_value: Bytes::from(encoded_value),
        }
    }

    pub(crate) fn from_shared(encoded_key: Bytes, encoded_value: Bytes) -> Self {
        Self {
            encoded_key,
            encoded_value,
        }
    }
}

/// An encoded tracked-root mutation batch.
///
/// Every key is a slice of one immutable key arena and every value is a slice
/// of one immutable value arena. The row vector therefore carries descriptors
/// only; moving it through diff, merge, root planning, and tree materialization
/// never clones row-owned payload buffers.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TrackedStateMutationBatch {
    mutations: Vec<TrackedStateMutation>,
}

impl TrackedStateMutationBatch {
    pub(crate) fn from_shared(mutations: Vec<TrackedStateMutation>) -> Self {
        Self { mutations }
    }

    pub(crate) fn len(&self) -> usize {
        self.mutations.len()
    }

    pub(crate) fn first_encoded_key(&self) -> Option<&[u8]> {
        self.mutations
            .first()
            .map(|mutation| mutation.encoded_key.as_ref())
    }

    #[cfg(test)]
    pub(crate) fn as_slice(&self) -> &[TrackedStateMutation] {
        &self.mutations
    }

    pub(crate) fn into_mutations(self) -> Vec<TrackedStateMutation> {
        self.mutations
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateTreeScanRequest {
    pub(crate) schema_keys: Vec<String>,
    pub(crate) row_pks: Vec<RowPk>,
    pub(crate) row_pk_lower: Option<RowPkRangeBound>,
    pub(crate) row_pk_upper: Option<RowPkRangeBound>,
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    pub(crate) include_tombstones: bool,
    pub(crate) limit: Option<usize>,
}

impl Default for TrackedStateTreeScanRequest {
    fn default() -> Self {
        Self {
            schema_keys: Vec::new(),
            row_pks: Vec::new(),
            row_pk_lower: None,
            row_pk_upper: None,
            file_ids: Vec::new(),
            include_tombstones: true,
            limit: None,
        }
    }
}

impl TrackedStateTreeScanRequest {
    pub(crate) fn matches(&self, key: &TrackedStateKey, value: &TrackedStateIndexValue) -> bool {
        self.matches_ref(
            TrackedStateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                row_pk: &key.row_pk,
            },
            value,
        )
    }

    pub(crate) fn matches_ref(
        &self,
        key: TrackedStateKeyRef<'_>,
        value: &TrackedStateIndexValue,
    ) -> bool {
        if !self.include_tombstones && value.deleted {
            return false;
        }
        self.matches_key_ref(key)
    }

    pub(crate) fn matches_key_ref(&self, key: TrackedStateKeyRef<'_>) -> bool {
        if !self.schema_keys.is_empty()
            && !self
                .schema_keys
                .iter()
                .any(|schema_key| schema_key == key.schema_key)
        {
            return false;
        }
        if !self.row_pks.is_empty() && !self.row_pks.contains(key.row_pk) {
            return false;
        }
        if !row_pk_satisfies_bounds(
            key.row_pk,
            self.row_pk_lower.as_ref(),
            self.row_pk_upper.as_ref(),
        ) {
            return false;
        }
        if !self.file_ids.is_empty()
            && !self.file_ids.iter().any(|filter| match filter {
                NullableKeyFilter::Any => true,
                NullableKeyFilter::Null => key.file_id.is_none(),
                NullableKeyFilter::Value(value) => key.file_id == Some(value.as_str()),
            })
        {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateApplyResult {
    pub(crate) root_id: TrackedStateRootId,
    pub(crate) row_count: usize,
    pub(crate) tree_height: usize,
    pub(crate) chunk_count: usize,
    pub(crate) chunk_bytes: usize,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateTreeDiffEntry {
    /// Identity column shared by both sides of a modified row.
    ///
    /// Tree ordering already proves that a modified entry has the same
    /// encoded key on both sides. Keeping one decoded key avoids decoding and
    /// allocating the schema/file/row identity twice before diff and merge
    /// immediately re-share it.
    pub(crate) key: TrackedStateKey,
    pub(crate) before: Option<TrackedStateIndexValue>,
    pub(crate) after: Option<TrackedStateIndexValue>,
}
