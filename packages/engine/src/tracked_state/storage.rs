#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cmp_owned
)]

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::ops::{Bound, Deref, Range};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};

use crate::changelog::CommitId;
use crate::common::SharedStr;
use crate::storage_adapter::{
    BufferRange, EncodedMutationBatch, EncodedPut, PointReadPlan, ScanPlan, StorageAdapterRead,
    StorageCoreProjection, StorageError, StorageGetManyRequest, StorageGetManyResult,
    StorageGetOptions, StorageKey, StorageKeyRange, StorageProjectedValue, StorageScanChunk,
    StorageScanOptions, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
    exact_get_many,
};
use crate::tracked_state::codec::{
    DecodedLeafNodeRef, DecodedNodeRef, EncodedLeafEntry, EncodedLeafEntryRef, PendingChunkBatch,
    TrackedStateMutationBatchBuilder, decode_key, decode_key_shared, decode_node_ref, decode_value,
    encode_key_ref, encode_key_ref_into, encode_leaf_node_refs, encode_schema_key_prefix,
    encode_single_string_key_ref_into, encode_value_ref,
};
pub(crate) use crate::tracked_state::types::{
    CommitDeltaLifecycleSummary, CommitDeltaReplacementScope,
};
use crate::tracked_state::types::{
    CommitStateManifest, CommitStateMutationInventory, CommitStateMutationPart,
    CurrentStatePartDescriptor, StoredCommitDeltaReplacementGeneration, StoredReplacementPart,
    StoredReplacementPartsAuthority, TRACKED_STATE_HASH_BYTES, TrackedStateBaseCoordinate,
    TrackedStateCommitDeltaRef, TrackedStateCommitRoot, TrackedStateIndexValue,
    TrackedStateIndexValueRef, TrackedStateKey, TrackedStateKeyRef, TrackedStateRootId,
    TrackedStateSingleStringReplacementRef,
};
use crate::{LixError, storage_codec};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};

pub(crate) const TRACKED_STATE_TREE_CHUNK_NAMESPACE: &str = "tracked_state.tree_chunk";
pub(crate) const TRACKED_STATE_COMMIT_DELTA_SEGMENT_NAMESPACE: &str =
    "tracked_state.commit_delta_segment.v6";
pub(crate) const TRACKED_STATE_CHANGE_LOCATOR_NAMESPACE: &str = "tracked_state.change_locator.v2";
pub(crate) const TRACKED_STATE_COMMIT_STATE_MANIFEST_NAMESPACE: &str =
    "tracked_state.commit_state_manifest.v1";
pub(crate) const TRACKED_STATE_COMMIT_STATE_SEAL_NAMESPACE: &str =
    "tracked_state.commit_state_semantic_authority.v1";
const MIN_CURRENT_STATE_CATALOG_POINT_READS: u16 = 4;
pub(crate) const TRACKED_STATE_TREE_CHUNK_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0004_0001),
    TRACKED_STATE_TREE_CHUNK_NAMESPACE,
);
pub(crate) const TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE: StorageSpace = StorageSpace::immutable(
    StorageSpaceId(0x0004_001a),
    TRACKED_STATE_COMMIT_DELTA_SEGMENT_NAMESPACE,
);
/// Keep every high-volume packed-history plane below the live-row spaces
/// (`0x0004_001b..=0x0004_001d`). Backends order the space prefix first, so a
/// locator above those spaces makes each mixed manifest/locator SST overlap
/// unrelated live-state point reads.
pub(crate) const TRACKED_STATE_CHANGE_LOCATOR_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0004_0018),
    TRACKED_STATE_CHANGE_LOCATOR_NAMESPACE,
);
/// Hard-cut tracked commit authority.
///
/// Current repositories publish this one manifest per commit, including
/// commits with no tracked mutations. The former topology, delta-directory,
/// and root authority spaces are not part of the current protocol.
pub(crate) const TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0004_002b),
    TRACKED_STATE_COMMIT_STATE_MANIFEST_NAMESPACE,
);
pub(crate) const TRACKED_STATE_COMMIT_STATE_SEAL_SPACE: StorageSpace = StorageSpace::immutable(
    StorageSpaceId(0x0004_002e),
    TRACKED_STATE_COMMIT_STATE_SEAL_NAMESPACE,
);

const COMMIT_DELTA_SEGMENT_MAX_ROWS: usize = 512;
const GENERIC_COMMIT_DELTA_SEGMENT_MAX_ROWS: usize = 128;
const GENERIC_COMMIT_DELTA_SEGMENT_TARGET_BYTES: usize = 28 * 1024;
const ORDERED_COMMIT_DELTA_SEGMENT_TARGET_BYTES: usize = 64 * 1024;
// Version 14 makes every ordinary commit member self-contained and complete
// replacements authoritative through their immutable part manifest. The
// payload-less certified-reference encoding is intentionally rejected.
const COMMIT_DELTA_FORMAT_MAGIC: &[u8] = b"LXCD14";
const COMMIT_STATE_SEMANTIC_AUTHORITY_MAGIC: &[u8] = b"LXSA1";
const COMMIT_STATE_MANIFEST_FORMAT_MAGIC: &[u8] = b"LXCS3";
const COMMIT_DELTA_PAYLOAD_OFFSET_BYTES: usize = size_of::<u32>();
#[cfg(not(test))]
const COMMIT_DELTA_MAX_SIDECAR_BYTES: usize = 64 * 1024 * 1024;
#[cfg(test)]
const COMMIT_DELTA_MAX_SIDECAR_BYTES: usize = 1024 * 1024;
const COMMIT_DELTA_SIDECAR_RAW: u8 = 0;
const COMMIT_DELTA_SIDECAR_ZSTD: u8 = 1;
/// Every entry is an authored inline snapshot with empty metadata and origin
/// columns. The indexed body stores raw JSON ranges without per-row Musli
/// envelopes.
const COMMIT_DELTA_SIDECAR_AUTHORED_INLINE_RAW: u8 = 3;
const COMMIT_DELTA_SIDECAR_AUTHORED_INLINE_ZSTD: u8 = 4;
// Tiny history records are faster and usually smaller once stored raw: the
// zstd frame/header and compressor call cannot amortize over a point write.
const COMMIT_DELTA_MIN_COMPRESS_BYTES: usize = 1024;
const DECODED_COMMIT_DELTA_CACHE_MAX_BYTES: usize = 8 * 1024 * 1024;
const DECODED_COMMIT_DELTA_CACHE_MAX_ENTRIES: usize = 8;
const DECODED_COMMIT_DELTA_CACHE_ADMISSION_ENTRIES: usize = 32;
const DECODED_COMMIT_DELTA_CACHE_MAX_POINT_KEYS: usize = 16;
const TRANSACTION_COMMIT_DELTA_POINT_CACHE_MAX_BYTES: usize = 2 * 1024 * 1024;
const TRANSACTION_COMMIT_DELTA_POINT_CACHE_MAX_ENTRIES: usize = 2;

enum CommitDeltaSegmentEncodeError {
    SidecarTooLarge,
    Codec(LixError),
}

impl CommitDeltaSegmentEncodeError {
    fn into_lix_error(self) -> LixError {
        match self {
            Self::SidecarTooLarge => LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta sidecar exceeds the format limit",
            ),
            Self::Codec(error) => error,
        }
    }
}

#[derive(Clone, Copy, musli::Encode)]
#[musli(packed)]
struct CommitDeltaPayloadRef<'a> {
    #[musli(with = crate::json_store::json_slot_storage_ref)]
    snapshot: crate::json_store::JsonSlotRef<'a>,
    #[musli(with = crate::json_store::json_slot_storage_ref)]
    metadata: crate::json_store::JsonSlotRef<'a>,
    #[musli(with = storage_codec::option)]
    origin_key: Option<&'a str>,
    #[musli(with = storage_codec::option)]
    base_coordinate: Option<TrackedStateBaseCoordinate>,
    authored: bool,
}

#[derive(Clone, Copy, musli::Encode)]
#[musli(packed)]
struct CommitDeltaAuthoredPayloadRef<'a> {
    #[musli(with = crate::json_store::json_slot_storage_ref)]
    snapshot: crate::json_store::JsonSlotRef<'a>,
    #[musli(with = crate::json_store::json_slot_storage_ref)]
    metadata: crate::json_store::JsonSlotRef<'a>,
    #[musli(with = storage_codec::option)]
    origin_key: Option<&'a str>,
    #[musli(with = storage_codec::option)]
    base_coordinate: Option<TrackedStateBaseCoordinate>,
}

const COMMIT_DELTA_PAYLOAD_AUTHORED: u8 = 0;
const COMMIT_DELTA_PAYLOAD_SELECTED_REF: u8 = 1;
const COMMIT_DELTA_PAYLOAD_SELECTED_TOMBSTONE: u8 = 2;

#[derive(Debug, musli::Decode)]
#[musli(packed)]
struct CommitDeltaAuthoredPayload {
    #[musli(with = crate::json_store::json_slot_storage)]
    snapshot: crate::json_store::JsonSlot,
    #[musli(with = crate::json_store::json_slot_storage)]
    metadata: crate::json_store::JsonSlot,
    #[musli(with = storage_codec::option)]
    origin_key: Option<String>,
    #[musli(with = storage_codec::option)]
    base_coordinate: Option<TrackedStateBaseCoordinate>,
}

#[derive(Debug)]
enum CommitDeltaPayload {
    Authored(CommitDeltaAuthoredPayload),
    SelectedRef(Option<TrackedStateBaseCoordinate>),
    SelectedTombstone(Option<TrackedStateBaseCoordinate>),
}

#[cfg(test)]
impl CommitDeltaPayload {
    fn authored_payload(&self) -> &CommitDeltaAuthoredPayload {
        let Self::Authored(payload) = self else {
            panic!("test expected an authored commit-delta payload");
        };
        payload
    }
}

/// Fixed-width directory over independently encoded payload records.
///
/// A pair of equal offsets means that the corresponding identity has no
/// authoritative payload. Non-empty ranges contain exactly one musli-encoded
/// [`CommitDeltaPayload`], so a point lookup decodes only the requested row
/// instead of reconstructing every payload in the segment.
#[derive(Debug)]
struct CommitDeltaPayloadIndex<S> {
    sidecar: S,
    offsets: Range<usize>,
    payload_start: usize,
    entry_count: usize,
    layout: CommitDeltaPayloadLayout,
}

#[derive(Debug, Clone, Copy)]
enum CommitDeltaPayloadLayout {
    Indexed,
    AuthoredInline,
}

type CommitDeltaPayloadIndexRef<'a> = CommitDeltaPayloadIndex<Cow<'a, [u8]>>;
type OwnedCommitDeltaPayloadIndex = CommitDeltaPayloadIndex<Bytes>;

fn replacement_payload_error(message: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state replacement {message}"),
    )
}

impl<S> CommitDeltaPayloadIndex<S>
where
    S: AsRef<[u8]>,
{
    #[cfg(test)]
    fn len(&self) -> usize {
        self.entry_count
    }

    fn decode(&self, entry_index: usize) -> Result<CommitDeltaPayload, LixError> {
        if entry_index >= self.entry_count {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta payload index is out of bounds",
            ));
        }
        match self.layout {
            CommitDeltaPayloadLayout::AuthoredInline => {
                let payload = self.payload_range(entry_index)?;
                if payload.is_empty() {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state commit_delta member is missing its authoritative payload",
                    ));
                }
                let json = std::str::from_utf8(payload).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state inline commit_delta payload is not UTF-8",
                    )
                })?;
                return Ok(CommitDeltaPayload::Authored(CommitDeltaAuthoredPayload {
                    snapshot: crate::json_store::JsonSlot::Inline(json.into()),
                    metadata: crate::json_store::JsonSlot::None,
                    origin_key: None,
                    base_coordinate: None,
                }));
            }
            CommitDeltaPayloadLayout::Indexed => {}
        }
        let range = self.payload_range(entry_index)?;
        if range.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta member is missing its authoritative payload",
            ));
        }
        let (&tag, payload) = range.split_first().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta member has an empty payload record",
            )
        })?;
        match tag {
            COMMIT_DELTA_PAYLOAD_AUTHORED => {
                let payload = storage_codec::decode(
                    "tracked_state indexed authored commit_delta payload",
                    payload,
                )?;
                Ok(CommitDeltaPayload::Authored(payload))
            }
            COMMIT_DELTA_PAYLOAD_SELECTED_REF => Ok(CommitDeltaPayload::SelectedRef(
                decode_optional_base_coordinate(payload)?,
            )),
            COMMIT_DELTA_PAYLOAD_SELECTED_TOMBSTONE => Ok(CommitDeltaPayload::SelectedTombstone(
                decode_optional_base_coordinate(payload)?,
            )),
            _ => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta member has an invalid payload tag",
            )),
        }
    }

    fn resident_bytes(&self) -> usize {
        size_of::<Self>() + self.sidecar.as_ref().len()
    }

    fn payload_range(&self, entry_index: usize) -> Result<&[u8], LixError> {
        if entry_index >= self.entry_count {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta payload index is out of bounds",
            ));
        }
        let start = self.offset(entry_index)?;
        let end = self.offset(entry_index + 1)?;
        self.sidecar
            .as_ref()
            .get(self.payload_start + start..self.payload_start + end)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_delta payload range is out of bounds",
                )
            })
    }

    fn offset(&self, offset_index: usize) -> Result<usize, LixError> {
        let byte_start = offset_index
            .checked_mul(COMMIT_DELTA_PAYLOAD_OFFSET_BYTES)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_delta payload directory overflows",
                )
            })?;
        let bytes = self
            .sidecar
            .as_ref()
            .get(
                self.offsets.start + byte_start
                    ..self.offsets.start + byte_start + COMMIT_DELTA_PAYLOAD_OFFSET_BYTES,
            )
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_delta payload directory is truncated",
                )
            })?;
        Ok(usize::try_from(u32::from_be_bytes(
            bytes.try_into().expect("fixed payload offset"),
        ))
        .expect("u32 fits usize"))
    }
}

fn decode_optional_base_coordinate(
    payload: &[u8],
) -> Result<Option<TrackedStateBaseCoordinate>, LixError> {
    if payload.is_empty() {
        Ok(None)
    } else {
        storage_codec::decode("tracked_state commit_delta base coordinate", payload).map(Some)
    }
}

impl CommitDeltaPayloadIndexRef<'_> {
    fn into_owned(self) -> OwnedCommitDeltaPayloadIndex {
        OwnedCommitDeltaPayloadIndex {
            sidecar: match self.sidecar {
                Cow::Borrowed(bytes) => Bytes::copy_from_slice(bytes),
                Cow::Owned(bytes) => Bytes::from(bytes),
            },
            offsets: self.offsets,
            payload_start: self.payload_start,
            entry_count: self.entry_count,
            layout: self.layout,
        }
    }
}

pub(crate) struct LoadedCommitDeltaEntry {
    pub(crate) value: TrackedStateIndexValue,
    pub(crate) change_record: crate::changelog::ChangeRecord,
    pub(crate) base_coordinate: Option<TrackedStateBaseCoordinate>,
    selected_ref: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CommitDeltaChangeLocator {
    pub(crate) change_id: crate::changelog::ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) segment_index: u32,
    pub(crate) ordinal: u16,
}

pub(crate) struct AddressableCommitDeltaStage {
    pub(crate) locators: Vec<CommitDeltaChangeLocator>,
    /// Final ids by input delta ordinal. Non-addressable entries retain the
    /// nil sentinel and never require a second per-row index.
    pub(crate) assigned_change_ids: Vec<crate::changelog::ChangeId>,
    mutation_inventory: CommitStateMutationInventory,
}

impl AddressableCommitDeltaStage {
    pub(crate) fn mutation_inventory(&self) -> &CommitStateMutationInventory {
        &self.mutation_inventory
    }
}

/// Compact assignment map for an already ordered, fully addressable commit.
///
/// Full 512-row segments derive their direct addresses from the row ordinal.
/// Byte-limited irregular segments retain one packed u32 address per row. Both
/// shapes avoid retaining one UUID per row while the prepared batch and backend
/// write batch are simultaneously live.
#[derive(Debug, Clone)]
pub(crate) struct OrderedAddressableCommitDeltaStage {
    commit_id: CommitId,
    change_addresses: OrderedChangeAddresses,
    row_count: usize,
    mutation_inventory: CommitStateMutationInventory,
}

#[derive(Debug, Clone)]
enum OrderedChangeAddresses {
    Dense,
    Packed(Vec<u32>),
}

impl OrderedAddressableCommitDeltaStage {
    pub(crate) fn mutation_inventory(&self) -> &CommitStateMutationInventory {
        &self.mutation_inventory
    }
    pub(crate) fn assigned_change_ids(
        &self,
    ) -> impl Iterator<Item = crate::changelog::ChangeId> + '_ {
        (0..self.row_count).map(|row_index| {
            self.change_id_at(row_index)
                .expect("ordered change assignment covers every row")
        })
    }

    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn change_id_at(&self, row_index: usize) -> Option<crate::changelog::ChangeId> {
        if row_index >= self.row_count {
            return None;
        }
        let packed = match &self.change_addresses {
            OrderedChangeAddresses::Dense => u32::try_from(row_index)
                .expect("ordered commit-delta row index fits direct address space")
                .checked_add(1)
                .expect("ordered commit-delta address fits direct address space"),
            OrderedChangeAddresses::Packed(addresses) => addresses[row_index],
        };
        Some(change_id_from_packed_address(self.commit_id, packed))
    }

    #[cfg(test)]
    pub(crate) fn for_test_dense(commit_id: CommitId, row_count: usize) -> Self {
        Self {
            commit_id,
            change_addresses: OrderedChangeAddresses::Dense,
            row_count,
            mutation_inventory: CommitStateMutationInventory::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitDeltaMember {
    pub(crate) key: TrackedStateKey,
    pub(crate) value: TrackedStateIndexValue,
    pub(crate) change: crate::changelog::ChangeRecord,
    pub(crate) segment_index: u32,
    pub(crate) ordinal: u32,
    pub(crate) authored: bool,
    pub(crate) base_coordinate: Option<TrackedStateBaseCoordinate>,
    selected_tombstone: bool,
}

impl CommitDeltaMember {
    #[cfg(feature = "storage-benches")]
    pub(crate) fn is_selected_payload_ref(&self) -> bool {
        !self.authored && !self.selected_tombstone
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitDeltaInventoryEntry {
    pub(crate) members: Vec<CommitDeltaMember>,
    pub(crate) segment_count: usize,
    physical_segment_keys: Vec<Vec<u8>>,
    pub(crate) selected_source_commit_id: Option<CommitId>,
    pub(crate) authority: CommitStateTopologyProjection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitStateTopologyProjection {
    pub(crate) generation: u64,
    pub(crate) parent_commit_ids: Vec<CommitId>,
    pub(crate) commit_change_id: crate::changelog::ChangeId,
    pub(crate) author_account_ids: Vec<String>,
    pub(crate) created_at: crate::common::LixTimestamp,
    pub(crate) replay_debt: crate::tracked_state::types::CommitStateReplayDebt,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CommitDeltaInventory {
    pub(crate) commits: BTreeMap<CommitId, CommitDeltaInventoryEntry>,
}

struct CommitDeltaPlane {
    manifests: BTreeMap<CommitId, CommitDeltaManifest>,
    authorities: BTreeMap<CommitId, CommitStateTopologyProjection>,
    segments: BTreeMap<CommitId, BTreeMap<usize, Bytes>>,
    segment_keys: BTreeMap<CommitId, BTreeMap<usize, Bytes>>,
}

// Version the root metadata independently of storage backends. Version 3 is a
// hard cut for derived commit rows, prefix-friendly keys, and compact tree
// nodes. Reject older roots before their differently ordered state can be
// inherited or traversed.

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct CommitDeltaManifest {
    /// Complete selected state borrowed from one ordinary source commit.
    /// Local inline/external segments are a disjoint authored overlay.
    #[musli(with = storage_codec::option)]
    selected_source_commit_id: Option<[u8; 16]>,
    member_count: u32,
    selection_fingerprint: [u8; 32],
    /// Exact dense address inventory for ordered, fully addressable commits.
    /// An empty column denotes the generic unordered/mixed-address layout.
    direct_segment_row_counts: Vec<u16>,
    /// Collection partitions for which this commit is an authoritative base
    /// generation. A miss in one of these scopes must not fall through to a
    /// first-parent run. This persists the SQL executor's complete-replacement
    /// certificate without synthesizing one tombstone per predecessor row.
    #[musli(with = storage_codec::option)]
    single_partition: Option<CommitDeltaReplacementScope>,
    #[musli(with = storage_codec::option)]
    lifecycle_summary: Option<CommitDeltaLifecycleSummary>,
    #[musli(with = storage_codec::option)]
    replacement_generation: Option<StoredCommitDeltaReplacementGeneration>,
    #[musli(with = storage_codec::option)]
    replacement_parts: Option<StoredReplacementPartsAuthority>,
    /// A complete leaf payload for a commit that fits in one segment. Keeping
    /// it in the directory preserves the one-record shape of tiny commits;
    /// larger commits use the indexed segment list below.
    #[musli(bytes)]
    inline_segment: Vec<u8>,
    segments: Vec<CommitDeltaSegmentBounds>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitDeltaReplacementGeneration {
    pub(crate) scope: CommitDeltaReplacementScope,
    pub(crate) fallback_commit_id: Option<CommitId>,
    pub(crate) lifecycle_summary: CommitDeltaLifecycleSummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitDeltaReplayMetadata {
    pub(crate) member_count: u32,
    pub(crate) single_partition: Option<CommitDeltaReplacementScope>,
    pub(crate) lifecycle_summary: Option<CommitDeltaLifecycleSummary>,
    pub(crate) replacement_generation: Option<CommitDeltaReplacementGeneration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommitDeltaSelectionCertificate {
    pub(crate) member_count: u32,
    pub(crate) selection_fingerprint: [u8; 32],
    pub(crate) direct_segment_row_counts: Vec<u16>,
    pub(crate) selected_source_commit_id: Option<CommitId>,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct CommitDeltaSegmentBounds {
    #[musli(bytes)]
    first_key: Vec<u8>,
    #[musli(bytes)]
    last_key: Vec<u8>,
    #[musli(with = storage_codec::option)]
    replacement_part: Option<StoredReplacementPart>,
}

fn commit_state_inventory_from_delta_manifest(
    manifest: &CommitDeltaManifest,
) -> CommitStateMutationInventory {
    CommitStateMutationInventory {
        selected_source_commit_id: manifest.selected_source_commit_id,
        member_count: manifest.member_count,
        selection_fingerprint: manifest.selection_fingerprint,
        direct_part_row_counts: manifest.direct_segment_row_counts.clone(),
        replacement_part_digests: manifest
            .segments
            .iter()
            .filter_map(|part| {
                part.replacement_part
                    .as_ref()
                    .map(|part| part.content_digest)
            })
            .collect(),
        single_partition: manifest.single_partition.clone(),
        lifecycle_summary: manifest.lifecycle_summary.clone(),
        replacement_generation: manifest.replacement_generation.clone(),
        replacement_parts: manifest.replacement_parts.clone(),
        inline_part: manifest.inline_segment.clone(),
        parts: manifest
            .segments
            .iter()
            .map(|part| CommitStateMutationPart {
                first_key: part.first_key.clone(),
                last_key: part.last_key.clone(),
                replacement_part: part.replacement_part.clone(),
            })
            .collect(),
    }
}

fn commit_delta_manifest_from_commit_state(manifest: &CommitStateManifest) -> CommitDeltaManifest {
    commit_delta_manifest_from_inventory(&manifest.mutations)
}

/// Returns the exact collection scopes touched by a sealed mutation inventory.
/// `None` means bounds or implicit cascades may affect another scope, in which
/// case an inherited serving catalog must be discarded fail-closed.
pub(crate) fn commit_state_inventory_exact_touched_scopes(
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<Option<Vec<CommitDeltaReplacementScope>>, LixError> {
    if inventory.selected_source_commit_id.is_some() {
        return Ok(None);
    }
    if inventory.member_count == 0 {
        return Ok(Some(Vec::new()));
    }
    if !inventory.replacement_part_digests.is_empty() {
        return Ok(inventory.single_partition.clone().map(|scope| vec![scope]));
    }

    let mut scopes = BTreeSet::new();
    for part in &inventory.parts {
        let first = decode_key(&part.first_key)?;
        let last = decode_key(&part.last_key)?;
        if first.schema_key != last.schema_key || first.file_id != last.file_id {
            return Ok(None);
        }
        if first.schema_key == "lix_file_descriptor" {
            return Ok(None);
        }
        scopes.insert(CommitDeltaReplacementScope {
            schema_key: first.schema_key,
            file_id: first.file_id,
        });
    }
    if !inventory.inline_part.is_empty() {
        let leaf = decode_commit_delta_segment(&inventory.inline_part, None, commit_id)?;
        let mut has_cascade = false;
        visit_commit_delta_leaf(&leaf, commit_id, |_, encoded_key, _| {
            let key = decode_key(encoded_key)?;
            if key.schema_key == "lix_file_descriptor" {
                has_cascade = true;
            } else {
                scopes.insert(CommitDeltaReplacementScope {
                    schema_key: key.schema_key,
                    file_id: key.file_id,
                });
            }
            Ok(())
        })?;
        if has_cascade {
            return Ok(None);
        }
    }
    Ok(Some(scopes.into_iter().collect()))
}

async fn expanded_commit_delta_manifest_from_commit_state(
    store: &(impl StorageAdapterRead + ?Sized),
    manifest: &CommitStateManifest,
) -> Result<CommitDeltaManifest, LixError> {
    let mut expanded = commit_delta_manifest_from_commit_state(manifest);
    if manifest.mutations.replacement_part_digests.is_empty() {
        return Ok(expanded);
    }
    let fresh_set = fresh_replacement_current_state_part_set(manifest)?;
    let descriptors = match fresh_set.as_ref() {
        Some(set) => match super::current_state_part::load_current_state_part_descriptors(
            store,
            &set.directory,
        )
        .await
        {
            Ok(descriptors)
                if descriptors.len() == manifest.mutations.replacement_part_digests.len()
                    && descriptors
                        .iter()
                        .zip(&manifest.mutations.replacement_part_digests)
                        .all(|(descriptor, digest)| descriptor.content_digest == *digest) =>
            {
                Some(descriptors)
            }
            Ok(_) | Err(_) => {
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_crud_current_state_directory_recovery();
                None
            }
        },
        None => None,
    };
    expanded.segments = match descriptors {
        Some(descriptors) => descriptors
            .into_iter()
            .map(|descriptor| CommitDeltaSegmentBounds {
                first_key: descriptor.first_key,
                last_key: descriptor.last_key,
                replacement_part: Some(StoredReplacementPart {
                    content_digest: descriptor.content_digest,
                    owner_commit_id: descriptor.owner_commit_id,
                    first_address: descriptor.part_index.saturating_mul(
                        u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS)
                            .expect("replacement row bound fits u32"),
                    ),
                    uniform_created_at: descriptor.uniform_created_at,
                    uniform_updated_at: descriptor.uniform_updated_at,
                }),
            })
            .collect(),
        None => recover_replacement_segment_bounds(store, manifest).await?,
    };
    Ok(expanded)
}

fn fresh_replacement_current_state_part_set(
    manifest: &CommitStateManifest,
) -> Result<Option<crate::tracked_state::types::CurrentStatePartSet>, LixError> {
    let (Some(_root), Some(generation), Some(_authority), Some(anchor)) = (
        manifest.current_state_catalog.as_ref(),
        manifest.mutations.replacement_generation.as_ref(),
        manifest.mutations.replacement_parts.as_ref(),
        manifest.current_state_coverage_anchor.as_ref(),
    ) else {
        return Ok(None);
    };
    let set = crate::tracked_state::types::CurrentStatePartSet {
        scope: anchor.scope.clone(),
        generation_integrity_digest: anchor.generation_integrity_digest,
        state_lineage_digest: anchor.state_lineage_digest,
        directory: anchor.directory.clone(),
    };
    if set.generation_integrity_digest != generation.integrity_digest
        || set.scope != anchor.scope
        || set.generation_integrity_digest != anchor.generation_integrity_digest
        || set.state_lineage_digest != anchor.state_lineage_digest
        || set.directory != anchor.directory
        || set.directory.row_count != u64::from(manifest.mutations.member_count)
        || set.directory.part_count as usize != manifest.mutations.part_count()
        || set.directory.root_id == [0; 32]
        || set.directory.tree_height == 0
    {
        return Err(replacement_payload_error(
            "current-state catalog entry disagrees with its replacement authority",
        ));
    }
    Ok(Some(set))
}

/// Resolves one exact authenticated serving partition from an immutable
/// published commit manifest. The sealed content-addressed catalog root binds
/// every exact-scope entry; absence means the scope is not completely covered.
pub(crate) async fn load_complete_current_state_part_set_from_published_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &PublishedCommitStateManifest,
    scope: &CommitDeltaReplacementScope,
) -> Result<Option<crate::tracked_state::types::CurrentStatePartSet>, LixError> {
    if let Some(fresh) = fresh_replacement_current_state_part_set(state)?
        && fresh.scope == *scope
    {
        return Ok(Some(fresh));
    }
    let Some(root) = state.current_state_catalog.as_deref() else {
        return Ok(None);
    };
    let Some(entry) =
        super::current_state_part::load_current_state_catalog_entry(store, root, scope).await?
    else {
        return Ok(None);
    };
    Ok(Some(entry))
}

/// Resolves a point batch from authoritative complete collection entries in
/// the persistent catalog. `None` means at least one scope is uncovered and
/// canonical first-parent replay remains responsible for the whole batch.
async fn load_complete_current_state_values_encoded_inner(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &CommitStateManifest,
    encoded_keys: &[Bytes],
    validate_publication: bool,
    prefer_recent_replay: bool,
) -> Result<Option<Vec<Option<TrackedStateIndexValue>>>, LixError> {
    if encoded_keys.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let Some(root) = state.current_state_catalog.as_ref() else {
        return Ok(None);
    };
    if state.replay_debt.depth < MIN_CURRENT_STATE_CATALOG_POINT_READS {
        // Even the smallest covered lookup reads a catalog node, a directory
        // node, and one immutable part. Canonical replay is cheaper while its
        // bounded manifest interval is shorter.
        return Ok(None);
    }
    let mut scope_keys = BTreeMap::<CommitDeltaReplacementScope, Vec<usize>>::new();
    for (index, encoded_key) in encoded_keys.iter().enumerate() {
        let key = decode_key_shared(encoded_key.clone())?;
        scope_keys
            .entry(CommitDeltaReplacementScope {
                schema_key: key.schema_key.to_string(),
                file_id: key.file_id.map(|file_id| file_id.to_string()),
            })
            .or_default()
            .push(index);
    }
    let Some(_touched_scopes) =
        commit_state_inventory_exact_touched_scopes(state.commit_id, &state.mutations)?
    else {
        return Ok(None);
    };
    if prefer_recent_replay && current_inventory_may_contain_any_key(state, encoded_keys)? {
        return Ok(None);
    }
    if state
        .current_state_coverage_anchor
        .as_ref()
        .is_some_and(|anchor| scope_keys.keys().all(|scope| scope == &anchor.scope))
    {
        // The current manifest's complete-replacement authority already has a
        // direct bounded-part route for this scope. Prefer it over resolving
        // the same directory through the inherited-serving catalog.
        return Ok(None);
    }

    let scoped_keys = scope_keys.into_iter().collect::<Vec<_>>();
    let scopes = scoped_keys
        .iter()
        .map(|(scope, _)| scope.clone())
        .collect::<Vec<_>>();
    let loaded_sets = super::current_state_part::load_current_state_catalog_entries_for_scopes(
        store, root, &scopes,
    )
    .await?;
    if validate_publication {
        validate_current_state_catalog_requested_scopes(store, state, &scopes, &loaded_sets)
            .await?;
    }
    let Some(sets) = loaded_sets
        .into_iter()
        .zip(scoped_keys)
        .map(|(set, (_, key_indices))| set.map(|set| (set, key_indices)))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };

    let mut routed = BTreeMap::<
        (u8, [u8; 16], u32, [u8; 32], u16),
        (CurrentStatePartDescriptor, Vec<usize>),
    >::new();
    let mut values = vec![None; encoded_keys.len()];
    let directory_keys = sets
        .iter()
        .map(|(_, key_indices)| {
            key_indices
                .iter()
                .map(|&index| encoded_keys[index].clone())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let directory_requests = sets
        .iter()
        .zip(&directory_keys)
        .map(|((set, _), keys)| (&set.directory, keys.as_slice()))
        .collect::<Vec<_>>();
    let directory_routes =
        super::current_state_part::route_current_state_part_sets(store, &directory_requests)
            .await?;
    for ((_, key_indices), routes) in sets.into_iter().zip(directory_routes) {
        for (output_index, descriptor) in key_indices.into_iter().zip(routes) {
            let Some(descriptor) = descriptor else {
                continue;
            };
            routed
                .entry((
                    descriptor.source_kind,
                    descriptor.owner_commit_id,
                    descriptor.part_index,
                    descriptor.content_digest,
                    descriptor.source_row_offset,
                ))
                .or_insert_with(|| (descriptor, Vec::new()))
                .1
                .push(output_index);
        }
    }

    let replacement = routed
        .iter()
        .filter(|(_, (descriptor, _))| descriptor.source_kind == 0)
        .collect::<Vec<_>>();
    let storage_keys = replacement
        .iter()
        .map(|(_, (descriptor, _))| {
            let owner = CommitId::new(uuid::Uuid::from_bytes(descriptor.owner_commit_id));
            let mut key = commit_delta_segment_key(owner, descriptor.part_index as usize)?;
            key.extend_from_slice(&descriptor.content_digest);
            Ok(StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let loaded = PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &storage_keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    for ((_, (descriptor, output_indices)), value) in replacement.into_iter().zip(loaded.value) {
        let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
            replacement_payload_error("current-state directory references a missing part")
        })?;
        let owner = CommitId::new(uuid::Uuid::from_bytes(descriptor.owner_commit_id));
        let decoded = crate::tracked_state::replacement_part::decode_replacement_part(
            &descriptor.content_digest,
            &bytes,
        )?;
        for output_index in output_indices {
            let Some(found) = decoded.find(&encoded_keys[*output_index])? else {
                continue;
            };
            let physical_ordinal = u32::from(found.ordinal);
            let slice_start = u32::from(descriptor.source_row_offset);
            if physical_ordinal < slice_start
                || physical_ordinal >= slice_start + u32::from(descriptor.row_count)
            {
                return Err(replacement_payload_error(
                    "replacement current-state route escaped its source slice",
                ));
            }
            let packed = descriptor
                .part_index
                .checked_mul(
                    u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS).expect("row bound fits u32"),
                )
                .and_then(|base| base.checked_add(physical_ordinal))
                .and_then(|address| address.checked_add(1))
                .ok_or_else(|| replacement_payload_error("replacement address overflows"))?;
            values[*output_index] = Some(TrackedStateIndexValue {
                change_id: change_id_from_packed_address(owner, packed),
                commit_id: owner,
                deleted: false,
                created_at: descriptor.uniform_created_at,
                updated_at: descriptor.uniform_updated_at,
            });
        }
    }
    let native = routed
        .iter()
        .filter(|(_, (descriptor, _))| descriptor.source_kind == 1)
        .collect::<Vec<_>>();
    let native_keys = native
        .iter()
        .map(|(_, (descriptor, _))| StorageKey(Bytes::copy_from_slice(&descriptor.content_digest)))
        .collect::<Vec<_>>();
    let loaded = PointReadPlan::new(
        crate::tracked_state::current_state_data_part::CURRENT_STATE_DATA_PART_SPACE,
        &native_keys,
    )
    .materialize(store, StorageGetOptions::default())
    .await?;
    for ((_, (descriptor, output_indices)), value) in native.into_iter().zip(loaded.value) {
        let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
            replacement_payload_error("current-state directory references a missing native part")
        })?;
        let rows = crate::tracked_state::current_state_data_part::decode_current_state_data_part(
            &descriptor.content_digest,
            &bytes,
        )?;
        let start = usize::from(descriptor.source_row_offset);
        let end = start + usize::from(descriptor.row_count);
        let rows = rows.get(start..end).ok_or_else(|| {
            replacement_payload_error("native current-state source slice is out of bounds")
        })?;
        for output_index in output_indices {
            if let Ok(index) = rows.binary_search_by(|row| {
                row.encoded_key
                    .as_slice()
                    .cmp(encoded_keys[*output_index].as_ref())
            }) {
                values[*output_index] = Some(rows[index].value.clone());
            }
        }
    }
    Ok(Some(values))
}

#[cfg(test)]
pub(crate) async fn load_complete_current_state_values_encoded(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &CommitStateManifest,
    encoded_keys: &[Bytes],
) -> Result<Option<Vec<Option<TrackedStateIndexValue>>>, LixError> {
    load_complete_current_state_values_encoded_inner(store, state, encoded_keys, true, true).await
}

/// Uses a manifest returned by [`load_commit_state_manifest`] or
/// [`load_commit_state_manifests`]. The opaque handle proves that the mutable
/// manifest matched its immutable semantic authority in the caller's coherent
/// read.
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) async fn load_complete_current_state_values_from_published_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &PublishedCommitStateManifest,
    encoded_keys: &[Bytes],
) -> Result<Option<Vec<Option<TrackedStateIndexValue>>>, LixError> {
    load_complete_current_state_values_encoded_inner(
        store,
        &state.manifest,
        encoded_keys,
        false,
        false,
    )
    .await
}

/// Tries the serving catalog from a one-read replay manifest. The inner cost
/// gates run before publication validation, so shallow OLTP replay retains its
/// original one-manifest read while any catalog hit is authenticated first.
pub(crate) async fn load_complete_current_state_values_from_replay_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    encoded_keys: &[Bytes],
) -> Result<Option<Vec<Option<TrackedStateIndexValue>>>, LixError> {
    // `load_point_replay_commit_state` decoded this exact manifest from the
    // immutable semantic-authority seal, including its certified catalog root.
    // Re-reading the mutable manifest would add point I/O without
    // strengthening that authority.
    load_complete_current_state_values_encoded_inner(store, state, encoded_keys, false, true).await
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) async fn load_complete_current_state_values_from_published_replay_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &PublishedCommitStateManifest,
    encoded_keys: &[Bytes],
) -> Result<Option<Vec<Option<TrackedStateIndexValue>>>, LixError> {
    load_complete_current_state_values_encoded_inner(store, state, encoded_keys, false, true).await
}

fn current_inventory_may_contain_any_key(
    state: &CommitStateManifest,
    encoded_keys: &[Bytes],
) -> Result<bool, LixError> {
    let manifest = commit_delta_manifest_from_inventory(&state.mutations);
    if let Some(inline) = manifest.inline_segment() {
        let leaf = decode_commit_delta_segment(inline, None, state.commit_id)?;
        for encoded_key in encoded_keys {
            if find_commit_delta_entry_index(&leaf, encoded_key)?.is_some() {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    Ok(encoded_keys.iter().any(|encoded_key| {
        manifest.segments.iter().any(|segment| {
            segment.first_key.as_slice() <= encoded_key.as_ref()
                && encoded_key.as_ref() <= segment.last_key.as_slice()
        })
    }))
}

pub(crate) async fn validate_current_state_catalog_requested_scopes(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &CommitStateManifest,
    _scopes: &[CommitDeltaReplacementScope],
    _current_entries: &[Option<crate::tracked_state::types::CurrentStatePartSet>],
) -> Result<(), LixError> {
    if load_commit_state_manifest(store, state.commit_id)
        .await?
        .as_ref()
        != Some(state)
    {
        return Err(replacement_payload_error(
            "current-state catalog was not loaded from its immutable commit authority",
        ));
    }
    Ok(())
}

pub(crate) fn validate_current_state_catalog_parent_manifest(
    state: &CommitStateManifest,
    parent: Option<&CommitStateManifest>,
) -> Result<(), LixError> {
    let Some(root) = state.current_state_catalog.as_ref() else {
        return Ok(());
    };
    let expected_parent_root = parent
        .and_then(|parent| parent.current_state_catalog.as_ref())
        .map(|parent| parent.root_id);
    if root.parent_root_id != expected_parent_root {
        return Err(replacement_payload_error(
            "current-state catalog transition disagrees with its graph parent",
        ));
    }
    Ok(())
}

async fn recover_replacement_segment_bounds(
    store: &(impl StorageAdapterRead + ?Sized),
    manifest: &CommitStateManifest,
) -> Result<Vec<CommitDeltaSegmentBounds>, LixError> {
    let generation = manifest
        .mutations
        .replacement_generation
        .as_ref()
        .ok_or_else(|| {
            replacement_payload_error("compact part inventory omitted its generation")
        })?;
    let lifecycle = manifest
        .mutations
        .lifecycle_summary
        .as_ref()
        .ok_or_else(|| {
            replacement_payload_error("compact part inventory omitted lifecycle metadata")
        })?;
    let authority = manifest
        .mutations
        .replacement_parts
        .as_ref()
        .ok_or_else(|| {
            replacement_payload_error("compact part inventory omitted part authority")
        })?;
    let keys = manifest
        .mutations
        .replacement_part_digests
        .iter()
        .enumerate()
        .map(|(index, digest)| {
            let mut key = commit_delta_segment_key(manifest.commit_id, index)?;
            key.extend_from_slice(digest);
            Ok(StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let values = PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    manifest
        .mutations
        .replacement_part_digests
        .iter()
        .copied()
        .zip(values.value)
        .enumerate()
        .map(|(index, (content_digest, value))| {
            let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
                replacement_payload_error("compact inventory references a missing part")
            })?;
            let decoded = crate::tracked_state::replacement_part::decode_replacement_part(
                &content_digest,
                &bytes,
            )?;
            let first_key = decoded
                .first_key()
                .ok_or_else(|| replacement_payload_error("replacement part is empty"))?
                .to_vec();
            let last_key = decoded
                .last_key()
                .ok_or_else(|| replacement_payload_error("replacement part is empty"))?
                .to_vec();
            Ok(CommitDeltaSegmentBounds {
                first_key,
                last_key,
                replacement_part: Some(StoredReplacementPart {
                    content_digest,
                    owner_commit_id: generation.owner_commit_id,
                    first_address: u32::try_from(index)
                        .expect("replacement part index fits u32")
                        .saturating_mul(
                            u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS)
                                .expect("replacement row bound fits u32"),
                        ),
                    uniform_created_at: lifecycle.uniform_created_at,
                    uniform_updated_at: authority.uniform_updated_at,
                }),
            })
        })
        .collect()
}

fn commit_delta_manifest_from_inventory(
    inventory: &CommitStateMutationInventory,
) -> CommitDeltaManifest {
    let replacement_parts = inventory
        .parts
        .is_empty()
        .then_some(inventory.replacement_part_digests.as_slice())
        .unwrap_or_default()
        .iter()
        .enumerate()
        .map(|(index, &content_digest)| CommitDeltaSegmentBounds {
            first_key: Vec::new(),
            last_key: Vec::new(),
            replacement_part: inventory
                .replacement_generation
                .as_ref()
                .zip(
                    inventory
                        .lifecycle_summary
                        .as_ref()
                        .zip(inventory.replacement_parts.as_ref()),
                )
                .map(
                    |(generation, (lifecycle, authority))| StoredReplacementPart {
                        content_digest,
                        owner_commit_id: generation.owner_commit_id,
                        first_address: u32::try_from(index)
                            .expect("replacement part index fits u32")
                            .saturating_mul(
                                u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS)
                                    .expect("row limit fits u32"),
                            ),
                        uniform_created_at: lifecycle.uniform_created_at,
                        uniform_updated_at: authority.uniform_updated_at,
                    },
                ),
        });
    CommitDeltaManifest {
        selected_source_commit_id: inventory.selected_source_commit_id,
        member_count: inventory.member_count,
        selection_fingerprint: inventory.selection_fingerprint,
        direct_segment_row_counts: inventory.direct_part_row_counts.clone(),
        single_partition: inventory.single_partition.clone(),
        lifecycle_summary: inventory.lifecycle_summary.clone(),
        replacement_generation: inventory.replacement_generation.clone(),
        replacement_parts: inventory.replacement_parts.clone(),
        inline_segment: inventory.inline_part.clone(),
        segments: inventory
            .parts
            .iter()
            .map(|part| CommitDeltaSegmentBounds {
                first_key: part.first_key.clone(),
                last_key: part.last_key.clone(),
                replacement_part: part.replacement_part.clone(),
            })
            .chain(replacement_parts)
            .collect(),
    }
}

#[derive(Debug)]
struct DecodedCommitDeltaSegment {
    leaf: DecodedLeafNodeRef,
    payloads: OwnedCommitDeltaPayloadIndex,
    resident_bytes: usize,
}

#[derive(Debug)]
struct DecodedCommitDeltaCacheEntry {
    digest: [u8; 32],
    encoded: Bytes,
    /// Replacement-part bytes are content-addressed independently of their
    /// publishing commit, while decoding synthesizes commit ids and lifecycle
    /// timestamps from these bounds. Cache that semantic binding with the
    /// bytes so identical post-images in consecutive commits cannot reuse a
    /// leaf decoded for the previous owner.
    expected_bounds: Option<CommitDeltaSegmentBounds>,
    decoded: Arc<DecodedCommitDeltaSegment>,
}

impl DecodedCommitDeltaCacheEntry {
    fn resident_bytes(&self) -> usize {
        size_of::<Self>()
            + self.encoded.len()
            + self
                .expected_bounds
                .as_ref()
                .map_or(0, |bounds| bounds.first_key.len() + bounds.last_key.len())
            + self.decoded.resident_bytes
    }
}

#[derive(Debug, Default)]
struct DecodedCommitDeltaCache {
    entries: VecDeque<DecodedCommitDeltaCacheEntry>,
    recent_misses: VecDeque<([u8; 32], usize)>,
    resident_bytes: usize,
}

impl DecodedCommitDeltaCache {
    fn get(
        &mut self,
        digest: [u8; 32],
        bytes: &[u8],
        expected_bounds: Option<&CommitDeltaSegmentBounds>,
    ) -> Result<Option<Arc<DecodedCommitDeltaSegment>>, LixError> {
        let Some(position) = self.entries.iter().position(|entry| {
            entry.digest == digest
                && entry.encoded.as_ref() == bytes
                && entry.expected_bounds.as_ref() == expected_bounds
        }) else {
            return Ok(None);
        };
        validate_decoded_commit_delta_bounds(
            &self.entries[position].decoded.leaf,
            expected_bounds,
        )?;
        let entry = self
            .entries
            .remove(position)
            .expect("located decoded commit-delta cache entry");
        let decoded = Arc::clone(&entry.decoded);
        self.entries.push_back(entry);
        Ok(Some(decoded))
    }

    fn insert(
        &mut self,
        digest: [u8; 32],
        encoded: Bytes,
        expected_bounds: Option<CommitDeltaSegmentBounds>,
        decoded: Arc<DecodedCommitDeltaSegment>,
    ) {
        let entry = DecodedCommitDeltaCacheEntry {
            digest,
            encoded,
            expected_bounds,
            decoded,
        };
        let entry_bytes = entry.resident_bytes();
        if entry_bytes > DECODED_COMMIT_DELTA_CACHE_MAX_BYTES {
            return;
        }
        if let Some(position) = self.entries.iter().position(|existing| {
            existing.digest == entry.digest
                && existing.encoded == entry.encoded
                && existing.expected_bounds == entry.expected_bounds
        }) {
            let previous = self
                .entries
                .remove(position)
                .expect("located raced decoded commit-delta cache entry");
            self.resident_bytes = self
                .resident_bytes
                .saturating_sub(previous.resident_bytes());
        }
        self.resident_bytes = self.resident_bytes.saturating_add(entry_bytes);
        self.entries.push_back(entry);
        while self.resident_bytes > DECODED_COMMIT_DELTA_CACHE_MAX_BYTES
            || self.entries.len() > DECODED_COMMIT_DELTA_CACHE_MAX_ENTRIES
        {
            let evicted = self
                .entries
                .pop_front()
                .expect("over-budget decoded commit-delta cache is non-empty");
            self.resident_bytes = self.resident_bytes.saturating_sub(evicted.resident_bytes());
        }
    }

    /// Admit an immutable block only after a second observation. Cold point
    /// reads otherwise pay the hash but retain neither encoded nor decoded
    /// bytes, while transaction update loops promote their shared base block.
    fn should_admit(&mut self, digest: [u8; 32], encoded_len: usize) -> bool {
        if let Some(position) = self
            .recent_misses
            .iter()
            .position(|candidate| *candidate == (digest, encoded_len))
        {
            self.recent_misses.remove(position);
            return true;
        }
        self.recent_misses.push_back((digest, encoded_len));
        while self.recent_misses.len() > DECODED_COMMIT_DELTA_CACHE_ADMISSION_ENTRIES {
            self.recent_misses.pop_front();
        }
        false
    }
}

fn decoded_commit_delta_cache() -> &'static Mutex<DecodedCommitDeltaCache> {
    static CACHE: OnceLock<Mutex<DecodedCommitDeltaCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(DecodedCommitDeltaCache::default()))
}

/// Bounded immutable-address cache for repeated point reads in one storage
/// snapshot. Unlike the process-wide content cache, this may trust
/// `(commit_id, segment_index)` because a transaction cannot cross repositories
/// or observe a rewritten value at that address.
#[derive(Default)]
pub(crate) struct CommitDeltaPointReadCache {
    inner: Mutex<CommitDeltaPointReadCacheInner>,
}

/// Transaction-owned cursor for a monotonically scanned immutable generation.
/// The manifest and current decoded segment stay outside the shared cache
/// mutex; an ordered UPDATE therefore touches the LRU only when it crosses a
/// physical part boundary instead of twice for every row.
pub(crate) struct CommitDeltaLiveMembershipCursor {
    commit_id: CommitId,
    manifest: Option<Arc<CommitDeltaManifest>>,
    segment_index: Option<usize>,
    segment: Option<Arc<DecodedCommitDeltaSegment>>,
}

#[derive(Default)]
struct CommitDeltaPointReadCacheInner {
    manifests: VecDeque<(CommitId, Arc<CommitDeltaManifest>)>,
    segments: VecDeque<((CommitId, usize), Arc<DecodedCommitDeltaSegment>)>,
    recent_segment_misses: VecDeque<(CommitId, usize)>,
    segment_resident_bytes: usize,
}

impl CommitDeltaPointReadCache {
    pub(crate) fn live_membership_cursor(
        &self,
        commit_id: CommitId,
    ) -> CommitDeltaLiveMembershipCursor {
        CommitDeltaLiveMembershipCursor {
            commit_id,
            manifest: None,
            segment_index: None,
            segment: None,
        }
    }
}

impl CommitDeltaLiveMembershipCursor {
    /// Resolves one key through a monotonically consumed immutable generation.
    ///
    /// The shared point cache is only an opportunistic source. An ordered
    /// transaction must not fall back to the generic live-state point reader
    /// merely because the next physical part has not been observed twice yet:
    /// load that one bounded part directly and retain it until the cursor
    /// crosses the next range boundary.
    pub(crate) async fn live_member(
        &mut self,
        store: &(impl StorageAdapterRead + ?Sized),
        cache: &CommitDeltaPointReadCache,
        encoded_key: &[u8],
    ) -> Result<Option<bool>, LixError> {
        if self.manifest.is_none() {
            self.manifest =
                load_commit_delta_manifest_cached(store, self.commit_id, Some(cache)).await?;
        }
        let Some(manifest) = self.manifest.as_ref() else {
            return Ok(None);
        };
        if manifest.selected_source_commit_id.is_some() {
            return Ok(None);
        }
        let segment_index = if manifest.inline_segment().is_some() {
            0
        } else {
            let Some(segment_index) = commit_delta_segment_for_key(manifest, encoded_key) else {
                return Ok(Some(false));
            };
            segment_index
        };
        if self.segment_index != Some(segment_index) || self.segment.is_none() {
            self.segment = cache.segment(
                self.commit_id,
                segment_index,
                manifest.segments.get(segment_index),
            )?;
            if self.segment.is_none() {
                let decoded = if let Some(inline_segment) = manifest.inline_segment() {
                    decode_owned_commit_delta_segment(inline_segment, None)?
                } else {
                    let bounds = manifest.segments.get(segment_index).ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "tracked_state commit_delta manifest for commit '{}' has no segment {segment_index}",
                                self.commit_id
                            ),
                        )
                    })?;
                    let storage_key =
                        commit_delta_segment_key_for_bounds(self.commit_id, segment_index, bounds)?;
                    let loaded = PointReadPlan::new(
                        TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
                        &[StorageKey(Bytes::from(storage_key))],
                    )
                    .materialize(store, StorageGetOptions::default())
                    .await?;
                    let bytes = loaded
                        .value
                        .into_iter()
                        .next()
                        .flatten()
                        .and_then(full_value_bytes)
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "tracked_state commit_delta manifest for commit '{}' references missing segment {segment_index}",
                                    self.commit_id
                                ),
                            )
                        })?;
                    decode_owned_commit_delta_segment(&bytes, Some(bounds))?
                };
                self.segment = Some(decoded);
            }
            self.segment_index = Some(segment_index);
        }
        let segment = self
            .segment
            .as_ref()
            .expect("membership cursor loaded its current immutable part");
        Ok(Some(
            find_commit_delta_value(&segment.leaf, encoded_key, self.commit_id)?
                .is_some_and(|value| !value.deleted),
        ))
    }
}

impl CommitDeltaPointReadCache {
    fn should_admit_segment(
        &self,
        commit_id: CommitId,
        segment_index: usize,
    ) -> Result<bool, LixError> {
        let address = (commit_id, segment_index);
        let mut cache = self.inner.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "transaction commit-delta point cache lock is poisoned",
            )
        })?;
        if let Some(position) = cache
            .recent_segment_misses
            .iter()
            .position(|candidate| *candidate == address)
        {
            cache.recent_segment_misses.remove(position);
            return Ok(true);
        }
        cache.recent_segment_misses.push_back(address);
        while cache.recent_segment_misses.len() > DECODED_COMMIT_DELTA_CACHE_ADMISSION_ENTRIES {
            cache.recent_segment_misses.pop_front();
        }
        Ok(false)
    }

    fn manifest(&self, commit_id: CommitId) -> Result<Option<Arc<CommitDeltaManifest>>, LixError> {
        let mut cache = self.inner.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "transaction commit-delta point cache lock is poisoned",
            )
        })?;
        let Some(position) = cache
            .manifests
            .iter()
            .position(|(cached_commit_id, _)| *cached_commit_id == commit_id)
        else {
            return Ok(None);
        };
        let entry = cache
            .manifests
            .remove(position)
            .expect("located transaction commit-delta manifest cache entry");
        let manifest = Arc::clone(&entry.1);
        cache.manifests.push_back(entry);
        Ok(Some(manifest))
    }

    fn remember_manifest(
        &self,
        commit_id: CommitId,
        manifest: Arc<CommitDeltaManifest>,
    ) -> Result<(), LixError> {
        let mut cache = self.inner.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "transaction commit-delta point cache lock is poisoned",
            )
        })?;
        if let Some(position) = cache
            .manifests
            .iter()
            .position(|(cached_commit_id, _)| *cached_commit_id == commit_id)
        {
            cache.manifests.remove(position);
        }
        cache.manifests.push_back((commit_id, manifest));
        while cache.manifests.len() > DECODED_COMMIT_DELTA_CACHE_MAX_ENTRIES {
            cache.manifests.pop_front();
        }
        Ok(())
    }

    fn segment(
        &self,
        commit_id: CommitId,
        segment_index: usize,
        expected_bounds: Option<&CommitDeltaSegmentBounds>,
    ) -> Result<Option<Arc<DecodedCommitDeltaSegment>>, LixError> {
        let mut cache = self.inner.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "transaction commit-delta point cache lock is poisoned",
            )
        })?;
        let Some(position) = cache
            .segments
            .iter()
            .position(|(address, _)| *address == (commit_id, segment_index))
        else {
            return Ok(None);
        };
        validate_decoded_commit_delta_bounds(&cache.segments[position].1.leaf, expected_bounds)?;
        let entry = cache
            .segments
            .remove(position)
            .expect("located transaction commit-delta segment cache entry");
        let decoded = Arc::clone(&entry.1);
        cache.segments.push_back(entry);
        Ok(Some(decoded))
    }

    fn remember_segment(
        &self,
        commit_id: CommitId,
        segment_index: usize,
        decoded: Arc<DecodedCommitDeltaSegment>,
    ) -> Result<(), LixError> {
        if decoded.resident_bytes > TRANSACTION_COMMIT_DELTA_POINT_CACHE_MAX_BYTES {
            return Ok(());
        }
        let mut cache = self.inner.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "transaction commit-delta point cache lock is poisoned",
            )
        })?;
        if let Some(position) = cache
            .segments
            .iter()
            .position(|(address, _)| *address == (commit_id, segment_index))
        {
            let previous = cache
                .segments
                .remove(position)
                .expect("located transaction commit-delta segment cache entry");
            cache.segment_resident_bytes = cache
                .segment_resident_bytes
                .saturating_sub(previous.1.resident_bytes);
        }
        cache.segment_resident_bytes = cache
            .segment_resident_bytes
            .saturating_add(decoded.resident_bytes);
        cache
            .segments
            .push_back(((commit_id, segment_index), decoded));
        while cache.segment_resident_bytes > TRANSACTION_COMMIT_DELTA_POINT_CACHE_MAX_BYTES
            || cache.segments.len() > TRANSACTION_COMMIT_DELTA_POINT_CACHE_MAX_ENTRIES
        {
            let evicted = cache
                .segments
                .pop_front()
                .expect("over-budget transaction commit-delta cache is non-empty");
            cache.segment_resident_bytes = cache
                .segment_resident_bytes
                .saturating_sub(evicted.1.resident_bytes);
        }
        Ok(())
    }
}

enum PointReadCommitDeltaManifest {
    Owned(CommitDeltaManifest),
    Cached(Arc<CommitDeltaManifest>),
}

impl Deref for PointReadCommitDeltaManifest {
    type Target = CommitDeltaManifest;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(manifest) => manifest,
            Self::Cached(manifest) => manifest,
        }
    }
}

const COMMIT_DELTA_SMALL_STRING_DICTIONARY_LIMIT: usize = 32;

/// Arena-backed decoded mutations from one immutable commit.
///
/// Segment decoders reconstruct keys and compact values into one `Bytes`
/// arena per selected segment. Rows retain only compact arena/dictionary
/// ordinals plus the typed entity key; repeated schema and file metadata is
/// stored once for the whole scan.
#[derive(Debug, Default)]
pub(crate) struct DecodedCommitDeltaBatch {
    arenas: Vec<DecodedLeafNodeRef>,
    schema_keys: Vec<SharedStr>,
    file_ids: Vec<SharedStr>,
    rows: Vec<DecodedCommitDeltaRow>,
    values: Vec<TrackedStateIndexValue>,
}

#[derive(Debug)]
struct DecodedCommitDeltaRow {
    arena_ordinal: u32,
    entry_ordinal: u16,
    schema_key_ordinal: u32,
    /// `u32::MAX` is the null file-id sentinel.
    file_id_ordinal: u32,
    entity_pk: crate::entity_pk::EntityPk,
}

#[derive(Clone, Copy)]
pub(crate) struct DecodedCommitDeltaRowRef<'a> {
    batch: &'a DecodedCommitDeltaBatch,
    ordinal: usize,
}

impl DecodedCommitDeltaBatch {
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = DecodedCommitDeltaRowRef<'_>> + '_ {
        (0..self.rows.len()).map(|ordinal| DecodedCommitDeltaRowRef {
            batch: self,
            ordinal,
        })
    }

    #[cfg(test)]
    fn arena_count(&self) -> usize {
        self.arenas.len()
    }

    #[cfg(test)]
    fn schema_dictionary_len(&self) -> usize {
        self.schema_keys.len()
    }

    #[cfg(test)]
    fn file_dictionary_len(&self) -> usize {
        self.file_ids.len()
    }
}

impl<'a> DecodedCommitDeltaRowRef<'a> {
    pub(crate) fn key_ref(self) -> TrackedStateKeyRef<'a> {
        let row = &self.batch.rows[self.ordinal];
        TrackedStateKeyRef {
            schema_key: self.batch.schema_keys[row.schema_key_ordinal as usize].as_str(),
            file_id: (row.file_id_ordinal != u32::MAX)
                .then(|| self.batch.file_ids[row.file_id_ordinal as usize].as_str()),
            entity_pk: &row.entity_pk,
        }
    }

    pub(crate) fn value(self) -> &'a TrackedStateIndexValue {
        &self.batch.values[self.ordinal]
    }

    /// Returns a zero-copy view retaining the selected segment arena.
    #[cfg(test)]
    pub(crate) fn encoded_key(&self) -> Bytes {
        let row = &self.batch.rows[self.ordinal];
        self.batch.arenas[row.arena_ordinal as usize]
            .entry_owned(row.entry_ordinal as usize)
            .expect("decoded commit-delta row references an existing leaf entry")
            .key
    }

    /// Returns the encoded identity directly from its decoded segment arena.
    ///
    /// First-parent diff flattens these slices into one interval-wide arena,
    /// so it does not need a `Bytes` clone for every discovered mutation.
    pub(crate) fn encoded_key_ref(&self) -> &[u8] {
        let row = &self.batch.rows[self.ordinal];
        self.batch.arenas[row.arena_ordinal as usize]
            .key(row.entry_ordinal as usize)
            .expect("decoded commit-delta key lookup cannot fail")
            .expect("decoded commit-delta row references an existing leaf entry")
    }
}

struct CommitDeltaStringInterner {
    values: Vec<SharedStr>,
    ordinals: Option<HashMap<SharedStr, u32>>,
}

impl CommitDeltaStringInterner {
    fn new(expected_cardinality: usize) -> Self {
        Self {
            values: Vec::with_capacity(
                expected_cardinality.min(COMMIT_DELTA_SMALL_STRING_DICTIONARY_LIMIT),
            ),
            ordinals: None,
        }
    }

    fn intern(&mut self, value: SharedStr) -> Result<u32, LixError> {
        if let Some(ordinals) = &self.ordinals {
            if let Some(&ordinal) = ordinals.get(&value) {
                return Ok(ordinal);
            }
        } else if let Some(ordinal) = self.values.iter().position(|candidate| candidate == &value) {
            return Ok(ordinal as u32);
        }

        let ordinal = u32::try_from(self.values.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta string dictionary exceeds u32",
            )
        })?;
        if self.ordinals.is_none()
            && self.values.len() == COMMIT_DELTA_SMALL_STRING_DICTIONARY_LIMIT
        {
            let mut ordinals = HashMap::with_capacity(self.values.len().saturating_mul(2));
            for (ordinal, existing) in self.values.iter().enumerate() {
                ordinals.insert(existing.clone(), ordinal as u32);
            }
            self.ordinals = Some(ordinals);
        }
        if let Some(ordinals) = &mut self.ordinals {
            ordinals.insert(value.clone(), ordinal);
        }
        self.values.push(value);
        Ok(ordinal)
    }
}

struct DecodedCommitDeltaBatchBuilder {
    arenas: Vec<DecodedLeafNodeRef>,
    schema_keys: CommitDeltaStringInterner,
    file_ids: CommitDeltaStringInterner,
    rows: Vec<DecodedCommitDeltaRow>,
    values: Vec<TrackedStateIndexValue>,
}

impl DecodedCommitDeltaBatchBuilder {
    fn with_capacity(row_capacity: usize, arena_capacity: usize) -> Self {
        Self {
            arenas: Vec::with_capacity(arena_capacity),
            schema_keys: CommitDeltaStringInterner::new(row_capacity),
            file_ids: CommitDeltaStringInterner::new(row_capacity),
            rows: Vec::with_capacity(row_capacity),
            values: Vec::with_capacity(row_capacity),
        }
    }

    fn push_leaf(
        &mut self,
        leaf: DecodedLeafNodeRef,
        commit_id: CommitId,
        requested_schemas: &BTreeSet<&str>,
    ) -> Result<(), LixError> {
        let arena_ordinal = u32::try_from(self.arenas.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta scan has too many segment arenas",
            )
        })?;
        let first_row = self.rows.len();
        visit_commit_delta_leaf(&leaf, commit_id, |entry_index, _encoded_key, value| {
            let key = decode_key_shared(
                leaf.entry_owned(entry_index)
                    .expect("visited commit-delta leaf entry exists")
                    .key,
            )?;
            if !requested_schemas.is_empty() && !requested_schemas.contains(key.schema_key.as_str())
            {
                return Ok(());
            }
            let entry_ordinal = u16::try_from(entry_index).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_delta segment exceeds u16 row ordinals",
                )
            })?;
            let schema_key_ordinal = self.schema_keys.intern(key.schema_key)?;
            let file_id_ordinal = key
                .file_id
                .map_or(Ok(u32::MAX), |file_id| self.file_ids.intern(file_id))?;
            self.rows.push(DecodedCommitDeltaRow {
                arena_ordinal,
                entry_ordinal,
                schema_key_ordinal,
                file_id_ordinal,
                entity_pk: key.entity_pk,
            });
            self.values.push(value);
            Ok(())
        })?;
        if self.rows.len() != first_row {
            self.arenas.push(leaf);
        }
        Ok(())
    }

    fn finish(self) -> DecodedCommitDeltaBatch {
        DecodedCommitDeltaBatch {
            arenas: self.arenas,
            schema_keys: self.schema_keys.values,
            file_ids: self.file_ids.values,
            rows: self.rows,
            values: self.values,
        }
    }
}

async fn get_one(
    store: &(impl StorageAdapterRead + ?Sized),
    space: StorageSpace,
    key: Vec<u8>,
) -> Result<Option<Bytes>, LixError> {
    let result = PointReadPlan::new(space, &[StorageKey(Bytes::from(key))])
        .materialize(store, StorageGetOptions::default())
        .await?;
    Ok(result
        .value
        .into_iter()
        .next()
        .flatten()
        .and_then(full_value_bytes))
}

pub(crate) async fn load_root(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: &str,
) -> Result<Option<TrackedStateRootId>, LixError> {
    Ok(load_authoritative_commit_root(store, commit_id)
        .await?
        .map(|metadata| metadata.root_id))
}

/// Resolves snapshot metadata only through the hard-cut commit authority.
///
/// Tree chunks are content addressed; the commit-state manifest is the only
/// durable mapping from a commit to a snapshot root.
pub(crate) async fn load_authoritative_commit_root(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: &str,
) -> Result<Option<TrackedStateCommitRoot>, LixError> {
    let commit_id = CommitId::parse_lix(commit_id, "tracked-state authoritative root lookup")?;
    Ok(load_commit_state_manifest(store, commit_id)
        .await?
        .and_then(|manifest| manifest.snapshot_root))
}

fn commit_delta_manifest_key(commit_id: CommitId) -> Vec<u8> {
    commit_id.as_uuid().as_bytes().to_vec()
}

fn commit_state_manifest_key(commit_id: CommitId) -> Vec<u8> {
    commit_id.as_uuid().as_bytes().to_vec()
}

fn commit_state_semantic_seal(manifest: &CommitStateManifest) -> Result<Vec<u8>, LixError> {
    // Retain the root published with the commit so one-read point replay can
    // stop at its original baseline. A later rebuilt root may change only the
    // mutable manifest; semantic comparison intentionally ignores that field.
    let payload = storage_codec::encode("commit-state semantic authority", manifest)?;
    let digest = blake3::Hasher::new_derive_key("lix.commit-state.semantic-authority.v1")
        .update(&payload)
        .finalize();
    let mut encoded = Vec::with_capacity(
        COMMIT_STATE_SEMANTIC_AUTHORITY_MAGIC.len() + TRACKED_STATE_HASH_BYTES + payload.len(),
    );
    encoded.extend_from_slice(COMMIT_STATE_SEMANTIC_AUTHORITY_MAGIC);
    encoded.extend_from_slice(digest.as_bytes());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn decode_commit_state_semantic_authority(bytes: &[u8]) -> Result<CommitStateManifest, LixError> {
    let payload = bytes
        .strip_prefix(COMMIT_STATE_SEMANTIC_AUTHORITY_MAGIC)
        .ok_or_else(|| {
            replacement_payload_error("commit-state semantic authority has an unsupported format")
        })?;
    let (expected_digest, payload) = payload
        .split_at_checked(TRACKED_STATE_HASH_BYTES)
        .ok_or_else(|| replacement_payload_error("commit-state semantic authority is truncated"))?;
    let actual_digest = blake3::Hasher::new_derive_key("lix.commit-state.semantic-authority.v1")
        .update(payload)
        .finalize();
    if expected_digest != actual_digest.as_bytes() {
        return Err(replacement_payload_error(
            "commit-state semantic authority digest is invalid",
        ));
    }
    let manifest = storage_codec::decode("commit-state semantic authority", payload)?;
    // The immutable seal authenticates bytes, while local validation proves
    // that those bytes describe a structurally valid serving authority. This
    // is what lets one-read point replay trust a catalog leaf without loading
    // the historical complete-replacement manifest that first created it.
    validate_commit_state_manifest(&manifest)?;
    Ok(manifest)
}

fn validate_commit_state_semantic_seal(
    manifest: &CommitStateManifest,
    seal: &[u8],
) -> Result<(), LixError> {
    let authority = decode_commit_state_semantic_authority(seal)?;
    validate_commit_state_semantic_projection(manifest, &authority)
}

fn validate_commit_state_semantic_projection(
    manifest: &CommitStateManifest,
    authority: &CommitStateManifest,
) -> Result<(), LixError> {
    let mut manifest_semantics = manifest.clone();
    manifest_semantics.snapshot_root = None;
    let mut authority_semantics = authority.clone();
    authority_semantics.snapshot_root = None;
    if authority_semantics != manifest_semantics {
        return Err(replacement_payload_error(
            "commit-state manifest disagrees with its immutable semantic authority",
        ));
    }
    Ok(())
}

/// Commit authority loaded from the mutable manifest plane after its
/// immutable semantic authority was verified. Only this opaque handle may bypass
/// publication-time catalog re-derivation.
#[derive(Clone, Debug)]
pub(crate) struct PublishedCommitStateManifest {
    manifest: CommitStateManifest,
}

/// One-read immutable semantic authority used by point replay. The wrapper
/// prevents a freely constructed manifest from claiming authoritative catalog
/// coverage without passing seal and structural validation.
#[derive(Clone, Debug)]
pub(crate) struct AuthenticatedReplayCommitStateManifest {
    manifest: CommitStateManifest,
}

/// Same-write-set authority produced only after semantic publication. This lets a
/// later commit in one atomic transaction consume its parent catalog without
/// treating a freely constructible manifest as provenance.
pub(crate) struct StagedCommitStateManifest {
    manifest: CommitStateManifest,
    write_set_id: u64,
}

impl Deref for PublishedCommitStateManifest {
    type Target = CommitStateManifest;

    fn deref(&self) -> &Self::Target {
        &self.manifest
    }
}

impl Deref for AuthenticatedReplayCommitStateManifest {
    type Target = CommitStateManifest;

    fn deref(&self) -> &Self::Target {
        &self.manifest
    }
}

impl Deref for StagedCommitStateManifest {
    type Target = CommitStateManifest;

    fn deref(&self) -> &Self::Target {
        &self.manifest
    }
}

pub(crate) async fn stage_current_state_catalog_from_published_parent(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    parent: Option<&PublishedCommitStateManifest>,
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<super::current_state_part::CertifiedCurrentStateCatalogPublication, LixError> {
    super::current_state_part::stage_current_state_catalog(
        store,
        writes,
        parent.map(|parent| &parent.manifest),
        commit_id,
        inventory,
    )
    .await
}

pub(crate) async fn stage_current_state_catalog_from_staged_parent(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    parent: &StagedCommitStateManifest,
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<super::current_state_part::CertifiedCurrentStateCatalogPublication, LixError> {
    if parent.write_set_id != writes.identity() {
        return Err(replacement_payload_error(
            "staged catalog parent belongs to a different storage write set",
        ));
    }
    super::current_state_part::stage_current_state_catalog(
        store,
        writes,
        Some(&parent.manifest),
        commit_id,
        inventory,
    )
    .await
}

/// Rewrites one exactly scoped current-state partition from its certified
/// parent post-image and the current commit's sealed mutation parts. Untouched
/// parent descriptors are reused byte-for-byte; only intersecting bounded
/// parts and insertion gaps become native current-state data parts.
pub(crate) async fn stage_sparse_current_state_part_set(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    parent: &crate::tracked_state::types::CurrentStatePartSet,
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<Option<crate::tracked_state::types::CurrentStatePartSet>, LixError> {
    use crate::tracked_state::current_state_data_part::{
        CURRENT_STATE_DATA_PART_REFS_SPACE, CURRENT_STATE_DATA_PART_SPACE, CurrentStateDataRow,
        decode_current_state_data_part, encode_bounded_current_state_data_parts,
    };

    let staged_segments = staged_commit_delta_segment_bytes(writes, commit_id, inventory)?;
    let members = staged_commit_delta_members(store, commit_id, inventory, staged_segments).await?;
    if members.iter().any(|member| {
        member.key.schema_key == parent.scope.schema_key
            && member.key.file_id == parent.scope.file_id
            && !member.value.deleted
            && member.change.snapshot == crate::json_store::JsonSlot::None
    }) {
        // Selected/reference rows may intentionally omit their payload from
        // the local mutation part. Until that authority is available through
        // the same read-your-writes view, drop only this rebuildable serving
        // entry and leave canonical replay responsible for the scope.
        return Ok(None);
    }
    let mut mutations = BTreeMap::<Vec<u8>, Option<CurrentStateDataRow>>::new();
    for member in members {
        if member.key.schema_key != parent.scope.schema_key
            || member.key.file_id != parent.scope.file_id
        {
            continue;
        }
        let encoded_key = crate::tracked_state::codec::encode_key(&member.key);
        let row = (!member.value.deleted).then(|| CurrentStateDataRow {
            encoded_key: encoded_key.clone(),
            value: member.value,
            snapshot: member.change.snapshot,
            metadata: member.change.metadata,
        });
        if mutations.insert(encoded_key, row).is_some() {
            return Err(replacement_payload_error(
                "sparse current-state rewrite contains a duplicate identity",
            ));
        }
    }
    if mutations.is_empty() {
        return Ok(Some(parent.clone()));
    }

    let mut mutation_rows = mutations.into_iter().map(Some).collect::<Vec<_>>();
    let directory = if parent.directory.part_count == 0 {
        let rows = mutation_rows
            .into_iter()
            .flatten()
            .filter_map(|(_, row)| row)
            .collect::<Vec<_>>();
        let mut output = Vec::new();
        stage_native_current_state_rows(writes, &rows, &mut output)?;
        super::current_state_part::stage_current_state_part_directory(writes, &output)?
    } else {
        let encoded_keys = mutation_rows
            .iter()
            .map(|entry| {
                Bytes::copy_from_slice(
                    &entry
                        .as_ref()
                        .expect("sparse mutation has not been assigned")
                        .0,
                )
            })
            .collect::<Vec<_>>();
        let splice = super::current_state_part::plan_current_state_part_directory_splice(
            store,
            writes,
            &parent.directory,
            &encoded_keys,
        )
        .await?;
        let mut replacements = Vec::with_capacity(splice.leaf_count());
        for leaf_index in 0..splice.leaf_count() {
            let leaf_key_indices = splice.leaf_key_indices(leaf_index).to_vec();
            let leaf_parts = splice.leaf_parts(leaf_index).to_vec();
            let mut pending = leaf_key_indices
                .iter()
                .map(|&key_index| {
                    mutation_rows[key_index]
                        .take()
                        .expect("each sparse mutation is assigned to one leaf")
                })
                .collect::<Vec<_>>()
                .into_iter()
                .peekable();
            let mut output = Vec::with_capacity(leaf_parts.len() + leaf_key_indices.len());
            for descriptor in &leaf_parts {
                let mut gap = Vec::new();
                while pending
                    .peek()
                    .is_some_and(|(key, _)| key.as_slice() < descriptor.first_key.as_slice())
                {
                    let (_, row) = pending.next().expect("peeked sparse mutation");
                    if let Some(row) = row {
                        gap.push(row);
                    }
                }
                stage_native_current_state_rows(writes, &gap, &mut output)?;

                let touches_descriptor = pending
                    .peek()
                    .is_some_and(|(key, _)| key.as_slice() <= descriptor.last_key.as_slice());
                if !touches_descriptor {
                    output.push(descriptor.clone());
                    continue;
                }
                let mut rows = load_current_state_descriptor_rows(store, writes, descriptor)
                    .await?
                    .into_iter()
                    .map(|row| (row.encoded_key.clone(), row))
                    .collect::<BTreeMap<_, _>>();
                while pending
                    .peek()
                    .is_some_and(|(key, _)| key.as_slice() <= descriptor.last_key.as_slice())
                {
                    let (key, row) = pending.next().expect("peeked sparse mutation");
                    match row {
                        Some(mut row) => {
                            if let Some(previous) = rows.get(&key) {
                                row.value.created_at = previous.value.created_at;
                            }
                            rows.insert(key, row);
                        }
                        None => {
                            rows.remove(&key);
                        }
                    }
                }
                stage_native_current_state_rows(
                    writes,
                    &rows.into_values().collect::<Vec<_>>(),
                    &mut output,
                )?;
            }
            let tail = pending
                .filter_map(|(_, row)| row)
                .collect::<Vec<CurrentStateDataRow>>();
            stage_native_current_state_rows(writes, &tail, &mut output)?;
            replacements.push(output);
        }
        if mutation_rows.iter().any(Option::is_some) {
            return Err(replacement_payload_error(
                "sparse current-state splice did not assign every mutation",
            ));
        }
        super::current_state_part::stage_current_state_part_directory_splice(
            store,
            writes,
            splice,
            replacements,
        )
        .await?
    };
    let inventory_bytes =
        storage_codec::encode("sparse current-state lineage inventory", inventory)?;
    let state_lineage_digest =
        *blake3::Hasher::new_derive_key("lix sparse current-state lineage v2")
            .update(&parent.state_lineage_digest)
            .update(commit_id.as_uuid().as_bytes())
            .update(&(inventory_bytes.len() as u64).to_be_bytes())
            .update(&inventory_bytes)
            .update(&directory.root_id)
            .update(&directory.directory_digest)
            .finalize()
            .as_bytes();
    let rewritten = crate::tracked_state::types::CurrentStatePartSet {
        scope: parent.scope.clone(),
        generation_integrity_digest: parent.generation_integrity_digest,
        state_lineage_digest,
        directory,
    };

    fn stage_native_current_state_rows(
        writes: &mut StorageWriteSet,
        rows: &[CurrentStateDataRow],
        output: &mut Vec<CurrentStatePartDescriptor>,
    ) -> Result<(), LixError> {
        if rows.is_empty() {
            return Ok(());
        }
        let timestamp = crate::common::LixTimestamp::from_unix_millis_utc_lossy(0);
        for part in encode_bounded_current_state_data_parts(rows)? {
            stage_content_addressed_current_state_bytes(
                writes,
                CURRENT_STATE_DATA_PART_SPACE,
                part.digest,
                &part.bytes,
            )?;
            stage_content_addressed_current_state_bytes(
                writes,
                CURRENT_STATE_DATA_PART_REFS_SPACE,
                part.digest,
                &part.refs_bytes,
            )?;
            output.push(CurrentStatePartDescriptor {
                first_key: part.first_key,
                last_key: part.last_key,
                content_digest: part.digest,
                payload_refs_digest: part.refs_digest,
                source_kind: 1,
                owner_commit_id: [0; 16],
                part_index: 0,
                source_row_offset: 0,
                row_count: part.row_count,
                uniform_created_at: timestamp,
                uniform_updated_at: timestamp,
            });
        }
        Ok(())
    }

    fn stage_content_addressed_current_state_bytes(
        writes: &mut StorageWriteSet,
        space: StorageSpace,
        digest: [u8; 32],
        bytes: &[u8],
    ) -> Result<(), LixError> {
        if let Some(staged) = writes.staged_value(space, &digest) {
            if staged.as_ref() != bytes {
                return Err(replacement_payload_error(
                    "current-state content digest has conflicting staged bytes",
                ));
            }
            return Ok(());
        }
        writes.put(space, key(digest.to_vec()), value(bytes.to_vec()));
        Ok(())
    }

    async fn load_current_state_descriptor_rows(
        store: &(impl StorageAdapterRead + ?Sized),
        writes: &mut StorageWriteSet,
        descriptor: &CurrentStatePartDescriptor,
    ) -> Result<Vec<CurrentStateDataRow>, LixError> {
        let rows = match descriptor.source_kind {
            0 => {
                let owner = CommitId::new(uuid::Uuid::from_bytes(descriptor.owner_commit_id));
                let mut physical_key =
                    commit_delta_segment_key(owner, descriptor.part_index as usize)?;
                physical_key.extend_from_slice(&descriptor.content_digest);
                let staged =
                    writes.staged_value(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &physical_key);
                let bytes = if let Some(bytes) = staged {
                    bytes
                } else {
                    get_one(
                        store,
                        TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
                        physical_key,
                    )
                    .await?
                    .ok_or_else(|| {
                        replacement_payload_error("current-state source part is missing")
                    })?
                };
                let decoded = crate::tracked_state::replacement_part::decode_replacement_part(
                    &descriptor.content_digest,
                    &bytes,
                )?;
                let start = usize::from(descriptor.source_row_offset);
                let end = start + usize::from(descriptor.row_count);
                if end > decoded.len() {
                    return Err(replacement_payload_error(
                        "current-state source slice is out of bounds",
                    ));
                }
                (start..end)
                    .map(|ordinal| {
                        let encoded_key = decoded.key(ordinal)?.ok_or_else(|| {
                            replacement_payload_error("replacement source omitted a key")
                        })?;
                        let packed = descriptor
                            .part_index
                            .checked_mul(
                                u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS)
                                    .expect("row bound fits u32"),
                            )
                            .and_then(|base| {
                                base.checked_add(u32::try_from(ordinal).expect("ordinal fits u32"))
                            })
                            .and_then(|address| address.checked_add(1))
                            .ok_or_else(|| {
                                replacement_payload_error("replacement source address overflows")
                            })?;
                        Ok(CurrentStateDataRow {
                            encoded_key: encoded_key.to_vec(),
                            value: TrackedStateIndexValue {
                                change_id: change_id_from_packed_address(owner, packed),
                                commit_id: owner,
                                deleted: false,
                                created_at: descriptor.uniform_created_at,
                                updated_at: descriptor.uniform_updated_at,
                            },
                            snapshot: owned_json_slot(decoded.snapshot(ordinal)?.ok_or_else(
                                || replacement_payload_error("replacement source omitted snapshot"),
                            )?),
                            metadata: owned_json_slot(decoded.metadata(ordinal)?.ok_or_else(
                                || replacement_payload_error("replacement source omitted metadata"),
                            )?),
                        })
                    })
                    .collect::<Result<Vec<_>, LixError>>()?
            }
            1 => {
                let physical_key = descriptor.content_digest.to_vec();
                let staged = writes.staged_value(CURRENT_STATE_DATA_PART_SPACE, &physical_key);
                let bytes = if let Some(bytes) = staged {
                    bytes
                } else {
                    get_one(store, CURRENT_STATE_DATA_PART_SPACE, physical_key)
                        .await?
                        .ok_or_else(|| {
                            replacement_payload_error("native current-state part is missing")
                        })?
                };
                let decoded = decode_current_state_data_part(&descriptor.content_digest, &bytes)?;
                let start = usize::from(descriptor.source_row_offset);
                let end = start + usize::from(descriptor.row_count);
                decoded
                    .get(start..end)
                    .ok_or_else(|| {
                        replacement_payload_error("native current-state slice is out of bounds")
                    })?
                    .to_vec()
            }
            _ => {
                return Err(replacement_payload_error(
                    "current-state descriptor has unknown source",
                ));
            }
        };
        if rows.first().map(|row| row.encoded_key.as_slice())
            != Some(descriptor.first_key.as_slice())
            || rows.last().map(|row| row.encoded_key.as_slice())
                != Some(descriptor.last_key.as_slice())
        {
            return Err(replacement_payload_error(
                "current-state source slice disagrees with descriptor bounds",
            ));
        }
        Ok(rows)
    }

    fn owned_json_slot(slot: crate::json_store::JsonSlotRef<'_>) -> crate::json_store::JsonSlot {
        match slot {
            crate::json_store::JsonSlotRef::None => crate::json_store::JsonSlot::None,
            crate::json_store::JsonSlotRef::Ref(reference) => {
                crate::json_store::JsonSlot::Ref(reference.clone())
            }
            crate::json_store::JsonSlotRef::Inline(json) => {
                crate::json_store::JsonSlot::Inline(json.into())
            }
        }
    }
    Ok(Some(rewritten))
}

fn staged_commit_delta_segment_bytes(
    writes: &StorageWriteSet,
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<Vec<Option<Bytes>>, LixError> {
    let manifest = commit_delta_manifest_from_inventory(inventory);
    manifest
        .segments
        .iter()
        .enumerate()
        .map(|(segment_index, bounds)| {
            let physical_key =
                commit_delta_segment_key_for_bounds(commit_id, segment_index, bounds)?;
            Ok(writes.staged_value(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &physical_key))
        })
        .collect()
}

async fn staged_commit_delta_members(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
    staged_segments: Vec<Option<Bytes>>,
) -> Result<Vec<CommitDeltaMember>, LixError> {
    let manifest = commit_delta_manifest_from_inventory(inventory);
    let mut members = Vec::with_capacity(inventory.member_count as usize);
    if let Some(inline) = manifest.inline_segment() {
        collect_strict_commit_delta_members(inline, None, commit_id, 0, &mut members)?;
    } else {
        for ((segment_index, bounds), staged) in
            manifest.segments.iter().enumerate().zip(staged_segments)
        {
            let physical_key =
                commit_delta_segment_key_for_bounds(commit_id, segment_index, bounds)?;
            let bytes = if let Some(bytes) = staged {
                bytes
            } else {
                get_one(
                    store,
                    TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
                    physical_key,
                )
                .await?
                .ok_or_else(|| {
                    replacement_payload_error("sparse rewrite cannot read its mutation part")
                })?
            };
            collect_strict_commit_delta_members(
                &bytes,
                Some(bounds),
                commit_id,
                u32::try_from(segment_index).expect("segment index fits u32"),
                &mut members,
            )?;
        }
    }
    validate_commit_delta_member_order_and_ids(commit_id, &members)?;
    Ok(members)
}

fn commit_delta_segment_key(
    commit_id: CommitId,
    segment_index: usize,
) -> Result<Vec<u8>, LixError> {
    let segment_index = u32::try_from(segment_index).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta has too many packed segments",
        )
    })?;
    let mut encoded = commit_delta_manifest_key(commit_id);
    encoded.extend_from_slice(&segment_index.to_be_bytes());
    Ok(encoded)
}

fn commit_delta_segment_key_for_bounds(
    commit_id: CommitId,
    segment_index: usize,
    bounds: &CommitDeltaSegmentBounds,
) -> Result<Vec<u8>, LixError> {
    let mut encoded = commit_delta_segment_key(commit_id, segment_index)?;
    if let Some(part) = bounds.replacement_part.as_ref() {
        encoded.extend_from_slice(&part.content_digest);
    }
    Ok(encoded)
}

/// Loads the hard-cut semantic authority for one tracked commit.
pub(crate) async fn load_commit_state_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<CommitStateManifest>, LixError> {
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_crud_sealed_manifest_load();
    let key = StorageKey(Bytes::from(commit_state_manifest_key(commit_id)));
    let requests = [
        StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            keys: std::slice::from_ref(&key),
            opts: StorageGetOptions::default(),
        },
        StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_STATE_SEAL_SPACE,
            keys: std::slice::from_ref(&key),
            opts: StorageGetOptions::default(),
        },
    ];
    let mut values = exact_get_many(store, &requests).await?.values.into_iter();
    let manifest_bytes = values.next().flatten().and_then(full_value_bytes);
    let seal = values.next().flatten().and_then(full_value_bytes);
    let Some(bytes) = manifest_bytes else {
        if seal.is_some() {
            return Err(replacement_payload_error(
                "commit-state semantic authority has no mutable manifest",
            ));
        }
        return Ok(None);
    };
    let seal = seal.ok_or_else(|| {
        replacement_payload_error("commit-state manifest omitted its immutable semantic authority")
    })?;
    let manifest = decode_commit_state_manifest(&bytes)?;
    if manifest.commit_id != commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_state_manifest key for commit '{commit_id}' contains manifest for commit '{}'",
                manifest.commit_id
            ),
        ));
    }
    validate_commit_state_semantic_seal(&manifest, &seal)?;
    Ok(Some(manifest))
}

pub(crate) async fn load_published_commit_state_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<PublishedCommitStateManifest>, LixError> {
    Ok(load_commit_state_manifest(store, commit_id)
        .await?
        .map(|manifest| PublishedCommitStateManifest { manifest }))
}

/// Loads the replay projection after verifying immutable semantic authority.
pub(crate) async fn load_replay_commit_state_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<CommitStateManifest>, LixError> {
    Ok(load_point_replay_commit_state(store, commit_id)
        .await?
        .map(|state| state.manifest))
}

pub(crate) async fn load_point_replay_commit_state(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<AuthenticatedReplayCommitStateManifest>, LixError> {
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_crud_replay_manifest_load();
    let Some(authority) = get_one(
        store,
        TRACKED_STATE_COMMIT_STATE_SEAL_SPACE,
        commit_state_manifest_key(commit_id),
    )
    .await?
    else {
        return Ok(None);
    };
    let state = decode_commit_state_semantic_authority(&authority)?;
    if state.commit_id != commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state semantic authority key for commit '{commit_id}' contains authority for commit '{}'",
                state.commit_id
            ),
        ));
    }
    Ok(Some(AuthenticatedReplayCommitStateManifest {
        manifest: state,
    }))
}

/// Bulk-loads commit authorities in request order.
pub(crate) async fn load_commit_state_manifests(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_ids: &[CommitId],
) -> Result<Vec<Option<CommitStateManifest>>, LixError> {
    let keys = commit_ids
        .iter()
        .map(|commit_id| StorageKey(Bytes::from(commit_state_manifest_key(*commit_id))))
        .collect::<Vec<_>>();
    let requests = [
        StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            keys: &keys,
            opts: StorageGetOptions::default(),
        },
        StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_STATE_SEAL_SPACE,
            keys: &keys,
            opts: StorageGetOptions::default(),
        },
    ];
    let mut values = exact_get_many(store, &requests).await?.values.into_iter();
    let manifests = values.by_ref().take(keys.len()).collect::<Vec<_>>();
    let seals = values.collect::<Vec<_>>();
    commit_ids
        .iter()
        .copied()
        .zip(manifests.into_iter().zip(seals))
        .map(|(commit_id, (manifest_value, seal_value))| {
            let manifest_bytes = manifest_value.and_then(full_value_bytes);
            let seal = seal_value.and_then(full_value_bytes);
            let Some(bytes) = manifest_bytes else {
                if seal.is_some() {
                    return Err(replacement_payload_error(
                        "commit-state semantic authority has no mutable manifest",
                    ));
                }
                return Ok(None);
            };
            let seal = seal.ok_or_else(|| {
                replacement_payload_error(
                    "commit-state manifest omitted its immutable semantic authority",
                )
            })?;
            let manifest = decode_commit_state_manifest(&bytes)?;
            if manifest.commit_id != commit_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state commit-state batch key for commit '{commit_id}' contains manifest for commit '{}'",
                        manifest.commit_id
                    ),
                ));
            }
            validate_commit_state_semantic_seal(&manifest, &seal)?;
            Ok(Some(manifest))
        })
        .collect()
}

/// Loads deliberately malformed authority without semantic validation so a
/// corruption test can replace one forged record with another.
#[cfg(test)]
pub(crate) async fn load_unchecked_commit_state_manifest_for_test(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<CommitStateManifest>, LixError> {
    let Some(bytes) = get_one(
        store,
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        commit_state_manifest_key(commit_id),
    )
    .await?
    else {
        return Ok(None);
    };
    let payload = bytes
        .strip_prefix(COMMIT_STATE_MANIFEST_FORMAT_MAGIC)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state corrupt-test commit_state_manifest has an unsupported format",
            )
        })?;
    let manifest = storage_codec::decode("tracked_state commit_state_manifest", payload)?;
    Ok(Some(manifest))
}

async fn load_commit_delta_manifests(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_ids: &[CommitId],
) -> Result<Vec<Option<CommitDeltaManifest>>, LixError> {
    let states = load_commit_state_manifests(store, commit_ids).await?;
    let mut manifests = Vec::with_capacity(states.len());
    for state in states {
        manifests.push(match state {
            Some(state) => {
                Some(expanded_commit_delta_manifest_from_commit_state(store, &state).await?)
            }
            None => None,
        });
    }
    Ok(manifests)
}

/// Stages one complete commit authority record.
///
/// Callers must invoke this only after the immutable mutation inventory and
/// optional snapshot metadata are final. Publishing a partially populated
/// manifest and patching it later would make an intermediate representation
/// authoritative to read-your-writes consumers.
fn stage_commit_state_manifest_bytes(
    writes: &mut StorageWriteSet,
    manifest: &CommitStateManifest,
) -> Result<(), LixError> {
    let encoded = encode_commit_state_manifest(manifest)?;
    let seal = commit_state_semantic_seal(manifest)?;
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_crud_commit_state_manifest_bytes(encoded.len());
    writes.put(
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        key(commit_state_manifest_key(manifest.commit_id)),
        value(encoded),
    );
    writes.put(
        TRACKED_STATE_COMMIT_STATE_SEAL_SPACE,
        key(commit_state_manifest_key(manifest.commit_id)),
        value(seal.to_vec()),
    );
    Ok(())
}

/// Stages authority that carries no serving catalog. Catalog-bearing
/// authorities require the opaque canonical transition proof returned by
/// `stage_current_state_catalog`.
pub(crate) fn stage_commit_state_manifest(
    writes: &mut StorageWriteSet,
    manifest: &CommitStateManifest,
) -> Result<(), LixError> {
    if manifest.current_state_catalog.is_some() || manifest.current_state_coverage_anchor.is_some()
    {
        return Err(replacement_payload_error(
            "current-state catalogs require canonical publication certification",
        ));
    }
    stage_commit_state_manifest_bytes(writes, manifest)
}

pub(crate) fn stage_commit_state_manifest_with_handle(
    writes: &mut StorageWriteSet,
    manifest: &CommitStateManifest,
) -> Result<StagedCommitStateManifest, LixError> {
    stage_commit_state_manifest(writes, manifest)?;
    Ok(StagedCommitStateManifest {
        manifest: manifest.clone(),
        write_set_id: writes.identity(),
    })
}

/// Publishes a catalog only when the exact opaque proof from its canonical
/// staging transition accompanies the manifest.
pub(crate) fn stage_certified_commit_state_manifest(
    writes: &mut StorageWriteSet,
    manifest: &CommitStateManifest,
    publication: &super::current_state_part::CertifiedCurrentStateCatalogPublication,
) -> Result<(), LixError> {
    if writes.identity() != publication.write_set_id() {
        return Err(replacement_payload_error(
            "catalog publication proof belongs to a different storage write set",
        ));
    }
    let (root, anchor) = publication.parts();
    let declared_parent_id = match manifest.parent_commit_ids.as_slice() {
        [] => None,
        [parent_id] => Some(*parent_id),
        _ => {
            return Err(replacement_payload_error(
                "certified current-state catalogs cannot have multiple graph parents",
            ));
        }
    };
    if declared_parent_id != publication.parent_commit_id() {
        return Err(replacement_payload_error(
            "commit manifest graph parent disagrees with its catalog publication proof",
        ));
    }
    if manifest.current_state_catalog != root || manifest.current_state_coverage_anchor != anchor {
        return Err(replacement_payload_error(
            "commit manifest disagrees with its canonical catalog publication proof",
        ));
    }
    stage_commit_state_manifest_bytes(writes, manifest)
}

pub(crate) fn stage_certified_commit_state_manifest_with_handle(
    writes: &mut StorageWriteSet,
    manifest: &CommitStateManifest,
    publication: &super::current_state_part::CertifiedCurrentStateCatalogPublication,
) -> Result<StagedCommitStateManifest, LixError> {
    stage_certified_commit_state_manifest(writes, manifest, publication)?;
    Ok(StagedCommitStateManifest {
        manifest: manifest.clone(),
        write_set_id: writes.identity(),
    })
}

/// Updates only the rebuildable snapshot accelerator of an already sealed
/// authority. All semantic fields come from the opaque published handle and
/// therefore retain the exact same immutable authority.
pub(crate) fn stage_commit_state_snapshot_root_update(
    writes: &mut StorageWriteSet,
    published: &PublishedCommitStateManifest,
    snapshot_root: Option<TrackedStateCommitRoot>,
) -> Result<(), LixError> {
    let mut manifest = published.manifest.clone();
    manifest.snapshot_root = snapshot_root;
    let encoded = encode_commit_state_manifest(&manifest)?;
    writes.put(
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        key(commit_state_manifest_key(manifest.commit_id)),
        value(encoded),
    );
    Ok(())
}

/// Stages deliberately malformed authority for corruption tests. Production
/// publication must always use [`stage_commit_state_manifest`].
#[cfg(test)]
pub(crate) fn stage_unchecked_commit_state_manifest_for_test(
    writes: &mut StorageWriteSet,
    manifest: &CommitStateManifest,
) -> Result<(), LixError> {
    let payload = storage_codec::encode("tracked_state commit_state_manifest", manifest)?;
    let mut encoded = Vec::with_capacity(COMMIT_STATE_MANIFEST_FORMAT_MAGIC.len() + payload.len());
    encoded.extend_from_slice(COMMIT_STATE_MANIFEST_FORMAT_MAGIC);
    encoded.extend_from_slice(&payload);
    writes.put(
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        key(commit_state_manifest_key(manifest.commit_id)),
        value(encoded),
    );
    Ok(())
}

/// Replaces both mutable manifest bytes and the normally immutable seal for a
/// corruption/test-fixture scenario. Production code cannot compile a call to
/// this bypass.
#[cfg(test)]
pub(crate) fn stage_resealed_commit_state_manifest_for_test(
    writes: &mut StorageWriteSet,
    manifest: &CommitStateManifest,
) -> Result<(), LixError> {
    let encoded = encode_commit_state_manifest(manifest)?;
    let seal = commit_state_semantic_seal(manifest)?;
    writes.put(
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        key(commit_state_manifest_key(manifest.commit_id)),
        value(encoded),
    );
    writes.put(
        StorageSpace::mutable(
            TRACKED_STATE_COMMIT_STATE_SEAL_SPACE.id,
            TRACKED_STATE_COMMIT_STATE_SEAL_NAMESPACE,
        ),
        key(commit_state_manifest_key(manifest.commit_id)),
        value(seal.to_vec()),
    );
    Ok(())
}

/// Stages all tracked mutations for one immutable commit as bounded, sorted
/// front-coded segments plus one tiny directory. A full commit no longer
/// writes one backend key for every affected identity.
/// Stages bounded immutable mutation parts and returns the exact inventory
/// that the caller must publish in the commit-state authority.
pub(crate) fn stage_commit_deltas_for_commit_state(
    writes: &mut StorageWriteSet,
    deltas: &[TrackedStateCommitDeltaRef<'_>],
) -> Result<AddressableCommitDeltaStage, LixError> {
    stage_commit_deltas_inner(writes, deltas, None, None)
}

pub(crate) fn stage_addressable_commit_deltas(
    writes: &mut StorageWriteSet,
    deltas: &[TrackedStateCommitDeltaRef<'_>],
    addressable: &[bool],
) -> Result<AddressableCommitDeltaStage, LixError> {
    if addressable.len() != deltas.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state addressability column does not match commit deltas",
        ));
    }
    stage_commit_deltas_inner(writes, deltas, Some(addressable), None)
}

pub(crate) fn stage_addressable_commit_deltas_with_selected_source(
    writes: &mut StorageWriteSet,
    deltas: &[TrackedStateCommitDeltaRef<'_>],
    addressable: &[bool],
    selected_source_commit_id: CommitId,
) -> Result<AddressableCommitDeltaStage, LixError> {
    if addressable.len() != deltas.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state addressability column does not match commit deltas",
        ));
    }
    stage_commit_deltas_inner(
        writes,
        deltas,
        Some(addressable),
        Some(selected_source_commit_id),
    )
}

/// Streams an already ordered, fully addressable commit into bounded segments.
///
/// The generic path below supports arbitrary input order and mixed locator
/// policies, so it must retain transaction-wide encoded arenas, payload
/// descriptors, sort tuples, and UUID assignments. Certified SQL creates are
/// already strictly ordered and every row receives a direct address. For that
/// shape, keeping only one candidate segment plus the compact manifest removes
/// the dominant peak-memory overlap before backend commit.
pub(crate) fn stage_ordered_addressable_commit_deltas<'a, I>(
    writes: &mut StorageWriteSet,
    deltas: I,
    order_certified: bool,
    publish_lifecycle_summary: bool,
) -> Result<Option<OrderedAddressableCommitDeltaStage>, LixError>
where
    I: ExactSizeIterator<Item = Result<TrackedStateCommitDeltaRef<'a>, LixError>> + Clone,
{
    let row_count = deltas.len();
    let mut probe = deltas.clone();
    let Some(first) = probe.next().transpose()? else {
        return Ok(Some(OrderedAddressableCommitDeltaStage {
            commit_id: CommitId::default(),
            change_addresses: OrderedChangeAddresses::Dense,
            row_count: 0,
            mutation_inventory: CommitStateMutationInventory::default(),
        }));
    };
    let commit_id = first.delta.commit_id;
    let mut previous_key = TrackedStateKeyRef {
        schema_key: first.delta.schema_key,
        file_id: first.delta.file_id,
        entity_pk: first.delta.entity_pk,
    };
    if !order_certified {
        for delta in probe {
            let delta = delta?;
            if delta.delta.commit_id != commit_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state cannot pack deltas from different commits together",
                ));
            }
            let key = TrackedStateKeyRef {
                schema_key: delta.delta.schema_key,
                file_id: delta.delta.file_id,
                entity_pk: delta.delta.entity_pk,
            };
            if compare_tracked_state_key_refs(previous_key, key) != std::cmp::Ordering::Less {
                return Ok(None);
            }
            previous_key = key;
        }
    }

    let lifecycle_summary = if publish_lifecycle_summary {
        lifecycle_summary_for_ordered_deltas(deltas.clone())?
    } else {
        None
    };
    let mut compressor = None;
    let mut source = deltas;
    let mut pending = VecDeque::with_capacity(COMMIT_DELTA_SEGMENT_MAX_ROWS);
    let mut first_segment = None::<(CommitDeltaSegmentBounds, Vec<u8>)>;
    let mut manifest = CommitDeltaManifest {
        selected_source_commit_id: None,
        member_count: u32::try_from(row_count).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta member count exceeds the manifest format",
            )
        })?,
        selection_fingerprint: [0; 32],
        direct_segment_row_counts: Vec::with_capacity(
            row_count.div_ceil(COMMIT_DELTA_SEGMENT_MAX_ROWS),
        ),
        single_partition: None,
        lifecycle_summary,
        replacement_generation: None,
        replacement_parts: None,
        inline_segment: Vec::new(),
        segments: Vec::with_capacity(row_count.div_ceil(COMMIT_DELTA_SEGMENT_MAX_ROWS)),
    };
    let mut segment_row_counts = Vec::with_capacity(manifest.segments.capacity());
    while !pending.is_empty() || source.len() > 0 {
        while pending.len() < COMMIT_DELTA_SEGMENT_MAX_ROWS {
            let Some(delta) = source.next() else {
                break;
            };
            pending.push_back(delta?);
        }
        let segment_index = segment_row_counts.len();
        let mut candidate_len = pending.len();
        let (bounds, encoded) = loop {
            match encode_ordered_addressable_commit_delta_segment(
                commit_id,
                segment_index,
                pending.iter().take(candidate_len).copied(),
                candidate_len,
                &mut compressor,
            ) {
                Ok((bounds, encoded))
                    if encoded.len() <= ORDERED_COMMIT_DELTA_SEGMENT_TARGET_BYTES
                        || candidate_len == 1 =>
                {
                    break (bounds, encoded);
                }
                Ok(_) | Err(CommitDeltaSegmentEncodeError::SidecarTooLarge)
                    if candidate_len > 1 =>
                {
                    candidate_len = candidate_len.div_ceil(2);
                }
                Err(error) => return Err(error.into_lix_error()),
                Ok(_) => unreachable!("single-row segment exits through the guarded success arm"),
            }
        };
        let row_count_u16 =
            u16::try_from(candidate_len).expect("commit-delta segment row count fits u16");
        manifest.direct_segment_row_counts.push(row_count_u16);
        segment_row_counts.push(row_count_u16);
        for _ in 0..candidate_len {
            pending.pop_front();
        }

        if segment_index == 0 {
            first_segment = Some((bounds, encoded));
            continue;
        }
        if segment_index == 1 {
            writes.reserve_space(
                TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
                row_count.div_ceil(COMMIT_DELTA_SEGMENT_MAX_ROWS),
                0,
            );
            let (first_bounds, first_encoded) = first_segment
                .take()
                .expect("the second segment follows one retained first segment");
            manifest.segments.push(first_bounds);
            writes.put(
                TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
                key(commit_delta_segment_key(commit_id, 0)?),
                value(first_encoded),
            );
        }
        manifest.segments.push(bounds);
        writes.put(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            key(commit_delta_segment_key(commit_id, segment_index)?),
            value(encoded),
        );
    }

    manifest.single_partition = if segment_row_counts.len() == 1 {
        let (bounds, _) = first_segment
            .as_ref()
            .expect("one ordered segment remains available for partition certification");
        single_partition_from_bounds(&bounds.first_key, &bounds.last_key)?
    } else {
        let first = manifest
            .segments
            .first()
            .expect("multi-segment ordered commit has first bounds");
        let last = manifest
            .segments
            .last()
            .expect("multi-segment ordered commit has last bounds");
        single_partition_from_bounds(&first.first_key, &last.last_key)?
    };
    if segment_row_counts.len() == 1 {
        let (_, inline_segment) = first_segment
            .take()
            .expect("one ordered segment remains inline in its manifest");
        manifest.inline_segment = inline_segment;
    }
    let dense_addresses = segment_row_counts
        .iter()
        .take(segment_row_counts.len().saturating_sub(1))
        .all(|&count| usize::from(count) == COMMIT_DELTA_SEGMENT_MAX_ROWS);
    let change_addresses = if dense_addresses {
        OrderedChangeAddresses::Dense
    } else {
        let mut packed = Vec::with_capacity(row_count);
        for (segment_index, &segment_rows) in segment_row_counts.iter().enumerate() {
            let segment_base = u32::try_from(segment_index)
                .expect("ordered commit-delta segment index fits u32")
                .checked_mul(
                    u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS)
                        .expect("segment row limit fits u32"),
                )
                .expect("ordered commit-delta segment base fits direct address space");
            packed.extend((0..segment_rows).map(|ordinal| {
                segment_base
                    .checked_add(u32::from(ordinal))
                    .and_then(|address| address.checked_add(1))
                    .expect("ordered commit-delta address fits u32")
            }));
        }
        debug_assert_eq!(packed.len(), row_count);
        OrderedChangeAddresses::Packed(packed)
    };
    Ok(Some(OrderedAddressableCommitDeltaStage {
        commit_id,
        change_addresses,
        row_count,
        mutation_inventory: commit_state_inventory_from_delta_manifest(&manifest),
    }))
}

/// Publishes a complete replacement as compact immutable identity parts.
/// Parts own identity routing and JSON authority: small values are inline and
/// large values remain content-addressed references into the JSON store.
pub(crate) struct ReplacementPartInput<'a> {
    key: Vec<u8>,
    commit_id: CommitId,
    created_at: crate::common::LixTimestamp,
    updated_at: crate::common::LixTimestamp,
    snapshot: crate::json_store::JsonSlotRef<'a>,
    metadata: crate::json_store::JsonSlotRef<'a>,
}

pub(crate) trait ReplacementPartInputRef<'a>: Copy {
    fn into_replacement_part_input(self) -> Result<ReplacementPartInput<'a>, LixError>;
}

impl<'a> ReplacementPartInputRef<'a> for TrackedStateCommitDeltaRef<'a> {
    fn into_replacement_part_input(self) -> Result<ReplacementPartInput<'a>, LixError> {
        if self.delta.deleted || !self.authored || self.origin_key.is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state replacement member violates immutable replacement invariants",
            ));
        }
        Ok(ReplacementPartInput {
            key: encode_key_ref(TrackedStateKeyRef {
                schema_key: self.delta.schema_key,
                file_id: self.delta.file_id,
                entity_pk: self.delta.entity_pk,
            }),
            commit_id: self.delta.commit_id,
            created_at: self.delta.created_at,
            updated_at: self.delta.updated_at,
            snapshot: self.snapshot,
            metadata: self.metadata,
        })
    }
}

impl<'a> ReplacementPartInputRef<'a> for TrackedStateSingleStringReplacementRef<'a> {
    fn into_replacement_part_input(self) -> Result<ReplacementPartInput<'a>, LixError> {
        let mut key = Vec::with_capacity(
            self.schema_key.len() + self.file_id.map_or(0, str::len) + self.entity_pk.len() + 8,
        );
        encode_single_string_key_ref_into(&mut key, self.schema_key, self.file_id, self.entity_pk);
        Ok(ReplacementPartInput {
            key,
            commit_id: self.commit_id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            snapshot: self.snapshot,
            metadata: self.metadata,
        })
    }
}

pub(crate) fn stage_ordered_addressable_replacement_parts<'a, I, R>(
    writes: &mut StorageWriteSet,
    deltas: I,
    generation: &CommitDeltaReplacementGeneration,
) -> Result<OrderedAddressableCommitDeltaStage, LixError>
where
    I: ExactSizeIterator<Item = Result<R, LixError>>,
    R: ReplacementPartInputRef<'a>,
{
    struct BorrowedRow<'a> {
        key: Vec<u8>,
        snapshot: crate::json_store::JsonSlotRef<'a>,
        metadata: crate::json_store::JsonSlotRef<'a>,
    }

    let row_count = deltas.len();
    if row_count == 0 {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state replacement generation cannot be empty",
        ));
    }
    let _span = tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.materialization.commit_delta.replacement_parts",
        row_count
    )
    .entered();
    let mut commit_id = None;
    let mut uniform_updated_at = None;
    let mut previous_key = Vec::new();
    let mut pending = Vec::with_capacity(COMMIT_DELTA_SEGMENT_MAX_ROWS);
    let mut parts = Vec::with_capacity(row_count.div_ceil(COMMIT_DELTA_SEGMENT_MAX_ROWS));
    let mut compressor = None;
    for delta in deltas {
        let delta = delta?.into_replacement_part_input()?;
        if delta.created_at != generation.lifecycle_summary.uniform_created_at {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state replacement member violates immutable replacement invariants",
            ));
        }
        if matches!(delta.snapshot, crate::json_store::JsonSlotRef::None) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state replacement member has no canonical snapshot",
            ));
        }
        if commit_id
            .replace(delta.commit_id)
            .is_some_and(|owner| owner != delta.commit_id)
            || uniform_updated_at
                .replace(delta.updated_at)
                .is_some_and(|timestamp| timestamp != delta.updated_at)
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state replacement members have nonuniform owner or timestamp",
            ));
        }
        let key = delta.key;
        if !previous_key.is_empty() && previous_key >= key {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state replacement members are not in canonical identity order",
            ));
        }
        previous_key.clear();
        previous_key.extend_from_slice(&key);
        pending.push(BorrowedRow {
            key,
            snapshot: delta.snapshot,
            metadata: delta.metadata,
        });
        if pending.len() == COMMIT_DELTA_SEGMENT_MAX_ROWS {
            encode_replacement_part_prefix(&mut pending, &mut parts, &mut compressor)?;
        }
    }
    let commit_id = commit_id.expect("non-empty replacement has an owner");
    addressable_change_id(commit_id, 0, 0)?;
    let uniform_updated_at = uniform_updated_at.expect("non-empty replacement has a timestamp");

    while !pending.is_empty() {
        encode_replacement_part_prefix(&mut pending, &mut parts, &mut compressor)?;
    }

    fn encode_replacement_part_prefix(
        pending: &mut Vec<BorrowedRow<'_>>,
        parts: &mut Vec<crate::tracked_state::replacement_part::EncodedReplacementPart>,
        compressor: &mut Option<crate::compression::ZstdLevel1Compressor>,
    ) -> Result<(), LixError> {
        let mut candidate_len = pending.len().min(COMMIT_DELTA_SEGMENT_MAX_ROWS);
        let encoded = loop {
            let refs = pending[..candidate_len]
                .iter()
                .map(
                    |row| crate::tracked_state::replacement_part::ReplacementPartRowRef {
                        encoded_key: &row.key,
                        snapshot: row.snapshot,
                        metadata: row.metadata,
                    },
                )
                .collect::<Vec<_>>();
            match crate::tracked_state::replacement_part::encode_replacement_part_with_compressor(
                &refs, compressor,
            ) {
                Ok(encoded)
                    if encoded.bytes().len()
                        <= crate::tracked_state::replacement_part::REPLACEMENT_PART_TARGET_BYTES
                        || candidate_len == 1 =>
                {
                    break encoded;
                }
                Ok(_) => candidate_len = candidate_len.div_ceil(2),
                Err(_) if candidate_len > 1 => candidate_len = candidate_len.div_ceil(2),
                Err(error) => return Err(error),
            }
        };
        parts.push(encoded);
        pending.drain(..candidate_len);
        Ok(())
    }

    let mut first_ordinal = 0u32;
    let directory_entries = parts
        .iter()
        .map(|part| {
            let entry = part.directory_entry(first_ordinal);
            first_ordinal = first_ordinal
                .checked_add(u32::from(part.row_count()))
                .expect("replacement row count fits u32");
            entry
        })
        .collect::<Vec<_>>();
    let directory = crate::tracked_state::replacement_part::ReplacementPartDirectory::try_new(
        directory_entries,
        u32::try_from(row_count).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state replacement row count exceeds u32",
            )
        })?,
    )?;
    let authority = StoredReplacementPartsAuthority {
        directory_digest: directory.digest()?,
        uniform_updated_at,
    };
    let mut manifest = CommitDeltaManifest {
        selected_source_commit_id: None,
        member_count: u32::try_from(row_count).expect("replacement row count was bounded"),
        selection_fingerprint: [0; 32],
        direct_segment_row_counts: Vec::with_capacity(parts.len()),
        single_partition: Some(generation.scope.clone()),
        lifecycle_summary: Some(generation.lifecycle_summary.clone()),
        replacement_generation: Some(stored_replacement_generation(
            commit_id, generation, &authority,
        )),
        replacement_parts: Some(authority),
        inline_segment: Vec::new(),
        segments: Vec::with_capacity(parts.len()),
    };
    writes.reserve_space(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, parts.len(), 0);
    let mut first_address = 0u32;
    for (segment_index, part) in parts.into_iter().enumerate() {
        manifest.direct_segment_row_counts.push(part.row_count());
        let bounds = CommitDeltaSegmentBounds {
            first_key: part.first_key().to_vec(),
            last_key: part.last_key().to_vec(),
            replacement_part: Some(StoredReplacementPart {
                content_digest: *part.digest(),
                owner_commit_id: *commit_id.as_uuid().as_bytes(),
                first_address,
                uniform_created_at: generation.lifecycle_summary.uniform_created_at,
                uniform_updated_at,
            }),
        };
        let physical_key = commit_delta_segment_key_for_bounds(commit_id, segment_index, &bounds)?;
        manifest.segments.push(bounds);
        writes.put(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            key(physical_key),
            value(part.bytes().to_vec()),
        );
        first_address = u32::try_from(segment_index + 1)
            .expect("replacement segment count fits u32")
            .checked_mul(u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS).expect("row limit fits u32"))
            .expect("replacement direct address fits u32");
    }
    let dense = manifest
        .direct_segment_row_counts
        .iter()
        .take(manifest.direct_segment_row_counts.len().saturating_sub(1))
        .all(|&count| usize::from(count) == COMMIT_DELTA_SEGMENT_MAX_ROWS);
    let change_addresses = if dense {
        OrderedChangeAddresses::Dense
    } else {
        let mut packed = Vec::with_capacity(row_count);
        for (segment_index, &rows) in manifest.direct_segment_row_counts.iter().enumerate() {
            let base = u32::try_from(segment_index)
                .expect("replacement segment index fits u32")
                .checked_mul(
                    u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS).expect("row limit fits u32"),
                )
                .expect("replacement direct address fits u32");
            packed.extend((0..rows).map(|ordinal| base + u32::from(ordinal) + 1));
        }
        OrderedChangeAddresses::Packed(packed)
    };
    Ok(OrderedAddressableCommitDeltaStage {
        commit_id,
        change_addresses,
        row_count,
        mutation_inventory: commit_state_inventory_from_delta_manifest(&manifest),
    })
}

fn lifecycle_summary_for_ordered_deltas<'a, I>(
    deltas: I,
) -> Result<Option<CommitDeltaLifecycleSummary>, LixError>
where
    I: Iterator<Item = Result<TrackedStateCommitDeltaRef<'a>, LixError>>,
{
    let mut hasher = blake3::Hasher::new();
    let mut scope = None::<(&'a str, Option<&'a str>)>;
    let mut uniform_created_at = None;
    for delta in deltas {
        let delta = delta?;
        if delta.delta.deleted {
            return Ok(None);
        }
        let candidate_scope = (delta.delta.schema_key, delta.delta.file_id);
        if scope.is_some_and(|scope| scope != candidate_scope) {
            return Ok(None);
        }
        scope.get_or_insert(candidate_scope);
        if uniform_created_at.is_some_and(|created_at| created_at != delta.delta.created_at) {
            return Ok(None);
        }
        uniform_created_at.get_or_insert(delta.delta.created_at);
        let Ok(identity) = delta.delta.entity_pk.as_single_string() else {
            return Ok(None);
        };
        hasher.update(&(identity.len() as u64).to_le_bytes());
        hasher.update(identity.as_bytes());
    }
    Ok(scope
        .zip(uniform_created_at)
        .map(
            |((schema_key, file_id), uniform_created_at)| CommitDeltaLifecycleSummary {
                scope: CommitDeltaReplacementScope {
                    schema_key: schema_key.to_string(),
                    file_id: file_id.map(str::to_string),
                },
                ordered_identity_digest: *hasher.finalize().as_bytes(),
                uniform_created_at,
            },
        ))
}

fn stored_replacement_generation(
    owner_commit_id: CommitId,
    generation: &CommitDeltaReplacementGeneration,
    replacement_parts: &StoredReplacementPartsAuthority,
) -> StoredCommitDeltaReplacementGeneration {
    let mut stored = StoredCommitDeltaReplacementGeneration {
        owner_commit_id: *owner_commit_id.as_uuid().as_bytes(),
        scope: generation.scope.clone(),
        fallback_commit_id: generation
            .fallback_commit_id
            .map(|commit_id| *commit_id.as_uuid().as_bytes()),
        integrity_digest: [0; 32],
    };
    stored.integrity_digest = replacement_generation_integrity_digest(
        &stored,
        &generation.lifecycle_summary,
        replacement_parts,
    );
    stored
}

pub(crate) fn replacement_generation_integrity_digest(
    generation: &StoredCommitDeltaReplacementGeneration,
    lifecycle_summary: &CommitDeltaLifecycleSummary,
    replacement_parts: &StoredReplacementPartsAuthority,
) -> [u8; 32] {
    let mut digest =
        blake3::Hasher::new_derive_key("lix tracked-state replacement generation certificate v2");
    digest.update(&generation.owner_commit_id);
    digest.update(&(generation.scope.schema_key.len() as u64).to_be_bytes());
    digest.update(generation.scope.schema_key.as_bytes());
    match generation.scope.file_id.as_deref() {
        Some(file_id) => {
            digest.update(&[1]);
            digest.update(&(file_id.len() as u64).to_be_bytes());
            digest.update(file_id.as_bytes());
        }
        None => {
            digest.update(&[0]);
        }
    }
    match generation.fallback_commit_id {
        Some(fallback_commit_id) => {
            digest.update(&[1]);
            digest.update(&fallback_commit_id);
        }
        None => {
            digest.update(&[0]);
        }
    }
    digest.update(&lifecycle_summary.ordered_identity_digest);
    digest.update(&lifecycle_summary.uniform_created_at.packed().to_be_bytes());
    digest.update(&replacement_parts.directory_digest);
    digest.update(&replacement_parts.uniform_updated_at.packed().to_be_bytes());
    *digest.finalize().as_bytes()
}

fn compare_tracked_state_key_refs(
    left: TrackedStateKeyRef<'_>,
    right: TrackedStateKeyRef<'_>,
) -> std::cmp::Ordering {
    left.schema_key
        .cmp(right.schema_key)
        .then_with(|| left.file_id.cmp(&right.file_id))
        .then_with(|| left.entity_pk.cmp(right.entity_pk))
}

fn single_partition_for_entries(
    entries: &[EncodedLeafEntry],
) -> Result<Option<CommitDeltaReplacementScope>, LixError> {
    let Some(first) = entries.first() else {
        return Ok(None);
    };
    let last = entries.last().expect("non-empty entries have a last key");
    single_partition_from_bounds(&first.key, &last.key)
}

fn single_partition_from_bounds(
    first_key: &[u8],
    last_key: &[u8],
) -> Result<Option<CommitDeltaReplacementScope>, LixError> {
    let first = decode_key(first_key)?;
    let last = decode_key(last_key)?;
    Ok(
        (first.schema_key == last.schema_key && first.file_id == last.file_id).then_some(
            CommitDeltaReplacementScope {
                schema_key: first.schema_key,
                file_id: first.file_id,
            },
        ),
    )
}

fn encode_ordered_addressable_commit_delta_segment<'a>(
    commit_id: CommitId,
    segment_index: usize,
    deltas: impl Iterator<Item = TrackedStateCommitDeltaRef<'a>>,
    row_count: usize,
    compressor: &mut Option<crate::compression::ZstdLevel1Compressor>,
) -> Result<(CommitDeltaSegmentBounds, Vec<u8>), CommitDeltaSegmentEncodeError> {
    let mut entries = TrackedStateMutationBatchBuilder::with_row_capacity(row_count);
    let mut payloads = Vec::with_capacity(row_count);
    for (ordinal, delta) in deltas.enumerate() {
        let change_id = addressable_change_id(commit_id, segment_index, ordinal)
            .map_err(CommitDeltaSegmentEncodeError::Codec)?;
        entries.push(
            TrackedStateKeyRef {
                schema_key: delta.delta.schema_key,
                file_id: delta.delta.file_id,
                entity_pk: delta.delta.entity_pk,
            },
            TrackedStateIndexValueRef {
                change_id,
                commit_id,
                deleted: delta.delta.deleted,
                created_at: delta.delta.created_at,
                updated_at: delta.delta.updated_at,
            },
        );
        payloads.push(CommitDeltaPayloadRef {
            snapshot: delta.snapshot,
            metadata: delta.metadata,
            origin_key: delta.origin_key,
            base_coordinate: delta.base_coordinate,
            authored: delta.authored,
        });
    }
    entries.with_entry_refs(|entries| {
        let bounds = CommitDeltaSegmentBounds {
            first_key: entries
                .first()
                .expect("ordered commit-delta segment is nonempty")
                .key
                .to_vec(),
            last_key: entries
                .last()
                .expect("ordered commit-delta segment is nonempty")
                .key
                .to_vec(),
            replacement_part: None,
        };
        let encoded =
            try_encode_commit_delta_segment_with_payload_refs(entries, &payloads, compressor)?;
        Ok((bounds, encoded))
    })
}

fn stage_commit_deltas_inner(
    writes: &mut StorageWriteSet,
    deltas: &[TrackedStateCommitDeltaRef<'_>],
    addressable: Option<&[bool]>,
    selected_source_commit_id: Option<CommitId>,
) -> Result<AddressableCommitDeltaStage, LixError> {
    let Some(&commit_id) = deltas.first().map(|delta| &delta.delta.commit_id) else {
        return Ok(AddressableCommitDeltaStage {
            locators: Vec::new(),
            assigned_change_ids: Vec::new(),
            mutation_inventory: CommitStateMutationInventory::default(),
        });
    };
    let mut entries = TrackedStateMutationBatchBuilder::with_row_capacity(deltas.len());
    let mut payloads = Vec::with_capacity(deltas.len());
    for delta in deltas {
        if delta.delta.commit_id != commit_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state cannot pack deltas from different commits together",
            ));
        }
        entries.push(
            TrackedStateKeyRef {
                schema_key: delta.delta.schema_key,
                file_id: delta.delta.file_id,
                entity_pk: delta.delta.entity_pk,
            },
            TrackedStateIndexValueRef {
                change_id: delta.delta.change_id,
                commit_id: delta.delta.commit_id,
                deleted: delta.delta.deleted,
                created_at: delta.delta.created_at,
                updated_at: delta.delta.updated_at,
            },
        );
        payloads.push(CommitDeltaPayloadRef {
            snapshot: delta.snapshot,
            metadata: delta.metadata,
            origin_key: delta.origin_key,
            base_coordinate: delta.base_coordinate,
            authored: delta.authored,
        });
    }
    let mutations = entries
        .finish()
        .into_mutations()
        .into_iter()
        .collect::<Vec<_>>();
    let mut pending = mutations
        .into_iter()
        .zip(payloads)
        .enumerate()
        .map(|(index, (mutation, payload))| {
            (
                EncodedLeafEntry {
                    key: mutation.encoded_key,
                    value: mutation.encoded_value,
                },
                payload,
                addressable.is_some_and(|column| column[index]),
                index,
            )
        })
        .collect::<Vec<_>>();
    if !pending
        .windows(2)
        .all(|pair| pair[0].0.key <= pair[1].0.key)
    {
        pending.sort_unstable_by(|left, right| left.0.key.cmp(&right.0.key));
    }
    let mut entries = Vec::with_capacity(pending.len());
    let mut payloads = Vec::with_capacity(pending.len());
    let mut addressable = Vec::with_capacity(pending.len());
    let mut source_indices = Vec::with_capacity(pending.len());
    for (entry, payload, direct, source_index) in pending {
        entries.push(entry);
        payloads.push(payload);
        addressable.push(direct);
        source_indices.push(source_index);
    }
    if entries.windows(2).any(|pair| pair[0].key == pair[1].key) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_delta for commit '{commit_id}' contains duplicate identities"
            ),
        ));
    }

    let mut encoded_segments = Vec::new();
    let mut assigned_change_ids = vec![crate::changelog::ChangeId::default(); deltas.len()];
    let mut segment_start = 0usize;
    let mut sidecar_compressor = None;
    while segment_start < entries.len() {
        let mut segment_end =
            (segment_start + GENERIC_COMMIT_DELTA_SEGMENT_MAX_ROWS).min(entries.len());
        let segment_index = encoded_segments.len();
        let (encoded, assigned_entries, segment_assignments) = loop {
            let mut candidate = entries[segment_start..segment_end].to_vec();
            let mut candidate_assignments = Vec::new();
            for (ordinal, entry) in candidate.iter_mut().enumerate() {
                let source_index = segment_start + ordinal;
                if !addressable[source_index] {
                    continue;
                }
                let value = decode_value(&entry.value)?;
                let change_id = addressable_change_id(commit_id, segment_index, ordinal)?;
                entry.value = Bytes::from(encode_value_ref(TrackedStateIndexValueRef {
                    change_id,
                    commit_id: value.commit_id,
                    deleted: value.deleted,
                    created_at: value.created_at,
                    updated_at: value.updated_at,
                }));
                candidate_assignments.push((source_indices[source_index], change_id));
            }
            match try_encode_commit_delta_segment_with_payloads(
                &candidate,
                &payloads[segment_start..segment_end],
                &mut sidecar_compressor,
            ) {
                Ok(encoded)
                    if encoded.len() <= GENERIC_COMMIT_DELTA_SEGMENT_TARGET_BYTES
                        || segment_end - segment_start == 1 =>
                {
                    break (encoded, candidate, candidate_assignments);
                }
                Ok(_) | Err(CommitDeltaSegmentEncodeError::SidecarTooLarge)
                    if segment_end - segment_start > 1 =>
                {
                    segment_end = segment_start + (segment_end - segment_start).div_ceil(2);
                }
                Err(error) => return Err(error.into_lix_error()),
                Ok(_) => unreachable!("single-row segment exits through the guarded success arm"),
            }
        };
        entries[segment_start..segment_end].clone_from_slice(&assigned_entries);
        for (source_index, change_id) in segment_assignments {
            assigned_change_ids[source_index] = change_id;
        }
        encoded_segments.push((segment_start..segment_end, encoded));
        segment_start = segment_end;
    }
    let member_count = u32::try_from(entries.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta member count exceeds the manifest format",
        )
    })?;
    let selection_fingerprint = selection_fingerprint(entries.iter().map(|entry| {
        let value = decode_value(&entry.value)
            .expect("staged commit-delta values were encoded by the trusted builder");
        (
            entry.key.as_ref(),
            value.change_id,
            value.deleted,
            value.created_at,
            value.updated_at,
        )
    }));
    // A direct-address inventory is valid only when every row owns the slot
    // encoded into its assigned ChangeId. Mixed batches need locator fallback
    // for all rows because the inventory cannot describe per-row ownership;
    // selected-source aliases likewise cannot certify only their local part.
    let direct_segment_row_counts =
        if selected_source_commit_id.is_none() && addressable.iter().all(|&direct| direct) {
            encoded_segments
                .iter()
                .map(|(range, _)| {
                    u16::try_from(range.len()).expect("commit-delta segment row count fits u16")
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
    let has_dense_address_inventory = !direct_segment_row_counts.is_empty();
    let segment_count = encoded_segments.len();
    if segment_count == 1 {
        let (_, inline_segment) = encoded_segments
            .pop()
            .expect("non-empty commit delta has one encoded segment");
        let manifest = CommitDeltaManifest {
            selected_source_commit_id: selected_source_commit_id
                .map(|commit_id| *commit_id.as_uuid().as_bytes()),
            member_count,
            selection_fingerprint,
            direct_segment_row_counts,
            single_partition: single_partition_for_entries(&entries)?,
            lifecycle_summary: None,
            replacement_generation: None,
            replacement_parts: None,
            inline_segment,
            segments: Vec::new(),
        };
        let locators = commit_delta_change_locators(commit_id, 0, &entries)?
            .into_iter()
            .enumerate()
            .filter_map(|(index, locator)| (!addressable[index]).then_some(locator))
            .collect();
        return Ok(AddressableCommitDeltaStage {
            locators,
            assigned_change_ids,
            mutation_inventory: commit_state_inventory_from_delta_manifest(&manifest),
        });
    }
    writes.reserve_space(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, segment_count, 0);
    let mut manifest = CommitDeltaManifest {
        selected_source_commit_id: selected_source_commit_id
            .map(|commit_id| *commit_id.as_uuid().as_bytes()),
        member_count,
        selection_fingerprint,
        direct_segment_row_counts,
        single_partition: single_partition_for_entries(&entries)?,
        lifecycle_summary: None,
        replacement_generation: None,
        replacement_parts: None,
        inline_segment: Vec::new(),
        segments: Vec::with_capacity(segment_count),
    };
    let mut locators = Vec::with_capacity(entries.len());
    for (segment_index, (range, encoded)) in encoded_segments.into_iter().enumerate() {
        let segment_entries = &entries[range];
        let first_key = segment_entries
            .first()
            .expect("non-empty packed commit-delta segment")
            .key
            .to_vec();
        let last_key = segment_entries
            .last()
            .expect("non-empty packed commit-delta segment")
            .key
            .to_vec();
        manifest.segments.push(CommitDeltaSegmentBounds {
            first_key,
            last_key,
            replacement_part: None,
        });
        writes.put(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            key(commit_delta_segment_key(commit_id, segment_index)?),
            value(encoded),
        );
        if !has_dense_address_inventory {
            locators.extend(commit_delta_change_locators(
                commit_id,
                segment_index,
                segment_entries,
            )?);
        }
    }
    Ok(AddressableCommitDeltaStage {
        locators,
        assigned_change_ids,
        mutation_inventory: commit_state_inventory_from_delta_manifest(&manifest),
    })
}

fn addressable_change_id(
    commit_id: CommitId,
    segment_index: usize,
    ordinal: usize,
) -> Result<crate::changelog::ChangeId, LixError> {
    if commit_id.as_uuid().as_bytes()[12..] != [0; 4] {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state addressable commit id has no reserved change address space",
        ));
    }
    let segment = u32::try_from(segment_index).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta segment index exceeds direct address space",
        )
    })?;
    let ordinal = u16::try_from(ordinal).expect("commit-delta segment row count fits u16");
    let packed = segment
        .checked_mul(
            u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS).expect("segment row limit fits u32"),
        )
        .and_then(|value| value.checked_add(u32::from(ordinal)))
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta address exceeds u32",
            )
        })?;
    Ok(change_id_from_packed_address(commit_id, packed))
}

fn change_id_from_packed_address(commit_id: CommitId, packed: u32) -> crate::changelog::ChangeId {
    let mut bytes = *commit_id.as_uuid().as_bytes();
    bytes[12..].copy_from_slice(&packed.to_be_bytes());
    crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(bytes))
}

fn commit_delta_change_locators(
    commit_id: CommitId,
    segment_index: usize,
    entries: &[EncodedLeafEntry],
) -> Result<Vec<CommitDeltaChangeLocator>, LixError> {
    let segment_index = u32::try_from(segment_index).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta locator segment index exceeds u32",
        )
    })?;
    entries
        .iter()
        .enumerate()
        .map(|(ordinal, entry)| {
            let ordinal = u16::try_from(ordinal).expect("commit-delta segment row count fits u16");
            let change_id = decode_value(&entry.value)?.change_id;
            Ok(CommitDeltaChangeLocator {
                change_id,
                commit_id,
                segment_index,
                ordinal,
            })
        })
        .collect()
}

pub(crate) fn stage_change_locators(
    writes: &mut StorageWriteSet,
    locators: &[CommitDeltaChangeLocator],
) {
    writes.reserve_space(TRACKED_STATE_CHANGE_LOCATOR_SPACE, locators.len(), 0);
    for locator in locators {
        let encoded = encode_change_locator(*locator);
        writes.put(
            TRACKED_STATE_CHANGE_LOCATOR_SPACE,
            key(locator.change_id.as_uuid().as_bytes().to_vec()),
            value(encoded),
        );
    }
}

pub(crate) fn stage_delete_change_locators(
    writes: &mut StorageWriteSet,
    change_ids: impl IntoIterator<Item = crate::changelog::ChangeId>,
) {
    writes.delete_batch(
        TRACKED_STATE_CHANGE_LOCATOR_SPACE,
        change_ids
            .into_iter()
            .map(|change_id| change_id.as_uuid().as_bytes().to_vec()),
    );
}

pub(crate) async fn load_change_record_by_id(
    store: &(impl StorageAdapterRead + ?Sized),
    change_id: crate::changelog::ChangeId,
) -> Result<Option<crate::changelog::ChangeRecord>, LixError> {
    let mut direct_error = None;
    if let Some(locator) = direct_change_locator(change_id)
        && let Some(commit_state) = load_commit_state_manifest(store, locator.commit_id).await?
        && direct_change_locator_in_commit_state(&commit_state, change_id) == Some(locator)
    {
        let mutation_directory =
            expanded_commit_delta_manifest_from_commit_state(store, &commit_state).await?;
        match try_load_change_record_at_locator_in_manifest(store, locator, &mutation_directory)
            .await
        {
            Ok(Some(record)) => return Ok(Some(record)),
            Ok(None) => {}
            Err(error) => direct_error = Some(error),
        }
    }
    if let Some(locator) = load_change_locator_by_id(store, change_id).await? {
        return load_change_record_at_locator(store, locator)
            .await
            .map(Some);
    }
    direct_error.map_or(Ok(None), Err)
}

async fn load_canonical_change_locator(
    store: &(impl StorageAdapterRead + ?Sized),
    change_id: crate::changelog::ChangeId,
) -> Result<Option<CommitDeltaChangeLocator>, LixError> {
    let mut direct_error = None;
    if let Some(locator) = direct_change_locator(change_id)
        && let Some(commit_state) = load_commit_state_manifest(store, locator.commit_id).await?
        && direct_change_locator_in_commit_state(&commit_state, change_id) == Some(locator)
    {
        let mutation_directory =
            expanded_commit_delta_manifest_from_commit_state(store, &commit_state).await?;
        match try_load_change_record_at_locator_in_manifest(store, locator, &mutation_directory)
            .await
        {
            Ok(Some(_)) => return Ok(Some(locator)),
            Ok(None) => {}
            Err(error) => direct_error = Some(error),
        }
    }
    if let Some(locator) = load_change_locator_by_id(store, change_id).await? {
        return Ok(Some(locator));
    }
    direct_error.map_or(Ok(None), Err)
}

pub(crate) fn direct_change_locator(
    change_id: crate::changelog::ChangeId,
) -> Option<CommitDeltaChangeLocator> {
    let mut commit_bytes = *change_id.as_uuid().as_bytes();
    let packed = u32::from_be_bytes(commit_bytes[12..].try_into().expect("four address bytes"));
    let packed = packed.checked_sub(1)?;
    let segment_row_limit =
        u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS).expect("segment row limit fits u32");
    let ordinal = u16::try_from(packed % segment_row_limit).ok()?;
    if usize::from(ordinal) >= COMMIT_DELTA_SEGMENT_MAX_ROWS {
        return None;
    }
    commit_bytes[12..].fill(0);
    Some(CommitDeltaChangeLocator {
        change_id,
        commit_id: CommitId::new(uuid::Uuid::from_bytes(commit_bytes)),
        segment_index: packed / segment_row_limit,
        ordinal,
    })
}

/// Resolves a direct `ChangeId` only when its encoded slot is present in the
/// authoritative commit-state inventory.
///
/// This is the hard-cut point route: physical part slots remain stable even
/// when the optional snapshot root is rebuilt or compacted.
pub(crate) fn direct_change_locator_in_commit_state(
    manifest: &CommitStateManifest,
    change_id: crate::changelog::ChangeId,
) -> Option<CommitDeltaChangeLocator> {
    let locator = direct_change_locator(change_id)?;
    if locator.commit_id != manifest.commit_id {
        return None;
    }
    let rows = manifest
        .mutations
        .direct_part_row_counts
        .get(usize::try_from(locator.segment_index).ok()?)?;
    (locator.ordinal < *rows).then_some(locator)
}

async fn load_change_locator_by_id(
    store: &(impl StorageAdapterRead + ?Sized),
    change_id: crate::changelog::ChangeId,
) -> Result<Option<CommitDeltaChangeLocator>, LixError> {
    let locator_key = StorageKey(Bytes::copy_from_slice(change_id.as_uuid().as_bytes()));
    let locator = PointReadPlan::new(
        TRACKED_STATE_CHANGE_LOCATOR_SPACE,
        std::slice::from_ref(&locator_key),
    )
    .materialize(store, StorageGetOptions::default())
    .await?
    .value
    .into_iter()
    .next()
    .flatten()
    .and_then(full_value_bytes);
    let Some(locator) = locator else {
        return Ok(None);
    };
    decode_change_locator(change_id, &locator).map(Some)
}

async fn load_change_records_by_ids(
    store: &(impl StorageAdapterRead + ?Sized),
    change_ids: &[crate::changelog::ChangeId],
) -> Result<Vec<crate::changelog::ChangeRecord>, LixError> {
    if change_ids.is_empty() {
        return Ok(Vec::new());
    }
    // Fresh changes use their commit-delta coordinates as their IDs and
    // intentionally have no locator rows. Keep the legacy locator batch below
    // for wholly explicit IDs; mixed/direct batches must validate the direct
    // candidate and retain the existing explicit-locator fallback.
    if let Some(direct_locators) = change_ids
        .iter()
        .copied()
        .map(direct_change_locator)
        .collect::<Option<Vec<_>>>()
    {
        // Certified commits encode their authoritative coordinates directly
        // in every change ID. Resolve the whole selection as one physical
        // batch so a large segment is read and decoded once rather than once
        // per selected change. Address-shaped explicit IDs are rare and keep
        // the established locator fallback below if candidate validation
        // rejects the direct route.
        if let Ok(records) = load_change_records_at_locators(store, &direct_locators).await {
            return Ok(records);
        }
    }
    if change_ids
        .iter()
        .copied()
        .any(|change_id| direct_change_locator(change_id).is_some())
    {
        return stream::iter(change_ids.iter().copied())
            .map(|change_id| async move {
                load_change_record_by_id_boxed(store, change_id)
                    .await?
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "tracked_state selected change '{change_id}' has no authoritative locator"
                            ),
                        )
                    })
            })
            .buffered(64)
            .try_collect()
            .await;
    }
    let locator_keys = change_ids
        .iter()
        .map(|change_id| StorageKey(Bytes::copy_from_slice(change_id.as_uuid().as_bytes())))
        .collect::<Vec<_>>();
    let locator_values = PointReadPlan::new(TRACKED_STATE_CHANGE_LOCATOR_SPACE, &locator_keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    let locators = change_ids
        .iter()
        .copied()
        .zip(locator_values.value)
        .map(|(change_id, value)| {
            let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state selected change '{change_id}' has no authoritative locator"
                    ),
                )
            })?;
            decode_change_locator(change_id, &bytes)
        })
        .collect::<Result<Vec<_>, _>>()?;
    load_change_records_at_locators(store, &locators).await
}

fn load_change_record_by_id_boxed<'a, S>(
    store: &'a S,
    change_id: crate::changelog::ChangeId,
) -> Pin<
    Box<dyn Future<Output = Result<Option<crate::changelog::ChangeRecord>, LixError>> + Send + 'a>,
>
where
    S: StorageAdapterRead + ?Sized + 'a,
{
    Box::pin(load_change_record_by_id(store, change_id))
}

async fn load_change_records_at_locators(
    store: &(impl StorageAdapterRead + ?Sized),
    locators: &[CommitDeltaChangeLocator],
) -> Result<Vec<crate::changelog::ChangeRecord>, LixError> {
    let commit_ids = locators
        .iter()
        .map(|locator| locator.commit_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let manifest_values = load_commit_delta_manifests(store, &commit_ids).await?;
    let manifests = commit_ids
        .into_iter()
        .zip(manifest_values)
        .map(|(commit_id, value)| {
            let manifest = value.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state selected change references missing commit '{commit_id}'"
                    ),
                )
            })?;
            Ok((commit_id, manifest))
        })
        .collect::<Result<BTreeMap<_, _>, LixError>>()?;

    let routes = locators
        .iter()
        .filter_map(|locator| {
            let manifest = &manifests[&locator.commit_id];
            manifest.inline_segment().is_none().then_some((
                locator.commit_id,
                usize::try_from(locator.segment_index).expect("u32 fits usize"),
            ))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let segment_keys = routes
        .iter()
        .map(|&(commit_id, segment_index)| {
            let bounds = manifests[&commit_id].segments.get(segment_index).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state selected change references out-of-range segment {segment_index} of commit '{commit_id}'"
                    ),
                )
            })?;
            commit_delta_segment_key_for_bounds(
                commit_id,
                segment_index,
                bounds,
            )
            .map(|key| StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let segment_values =
        PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &segment_keys)
            .materialize(store, StorageGetOptions::default())
            .await?;
    let segments = routes
        .into_iter()
        .zip(segment_values.value)
        .map(|(route, value)| {
            value
                .and_then(full_value_bytes)
                .map(|bytes| (route, bytes))
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "tracked_state selected change references absent segment {} of commit '{}'",
                            route.1, route.0
                        ),
                    )
                })
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;

    let mut locator_groups = BTreeMap::<(CommitId, usize), Vec<usize>>::new();
    for (locator_index, locator) in locators.iter().enumerate() {
        locator_groups
            .entry((
                locator.commit_id,
                usize::try_from(locator.segment_index).expect("u32 fits usize"),
            ))
            .or_default()
            .push(locator_index);
    }
    let mut loaded = (0..locators.len()).map(|_| None).collect::<Vec<_>>();
    for ((commit_id, segment_index), locator_indices) in locator_groups {
        let manifest = &manifests[&commit_id];
        let (bytes, bounds) = if let Some(inline) = manifest.inline_segment() {
            if segment_index != 0 {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state selected change references a nonzero inline segment",
                ));
            }
            (inline, None)
        } else {
            let bounds = manifest.segments.get(segment_index).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state selected change references an undeclared segment",
                )
            })?;
            (segments[&(commit_id, segment_index)].as_ref(), Some(bounds))
        };
        let (leaf, payloads) = decode_commit_delta_with_payloads(bytes, bounds)?;
        for locator_index in locator_indices {
            let locator = locators[locator_index];
            loaded[locator_index] = Some(decode_change_at_locator_from_decoded(
                &leaf, &payloads, locator,
            )?);
        }
    }
    loaded
        .into_iter()
        .map(|entry| {
            entry.map(|entry| entry.change_record).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state authoritative change unexpectedly disappeared",
                )
            })
        })
        .collect()
}

fn decode_change_at_locator(
    segment: &[u8],
    bounds: Option<&CommitDeltaSegmentBounds>,
    locator: CommitDeltaChangeLocator,
) -> Result<LoadedCommitDeltaEntry, LixError> {
    let (leaf, payloads) = decode_commit_delta_with_payloads(segment, bounds)?;
    decode_change_at_locator_from_decoded(&leaf, &payloads, locator)
}

fn decode_change_at_locator_from_decoded<S>(
    leaf: &DecodedLeafNodeRef,
    payloads: &CommitDeltaPayloadIndex<S>,
    locator: CommitDeltaChangeLocator,
) -> Result<LoadedCommitDeltaEntry, LixError>
where
    S: AsRef<[u8]>,
{
    let change_id = locator.change_id;
    let ordinal = usize::from(locator.ordinal);
    let entry = leaf.entry(ordinal)?.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state change locator for '{change_id}' references absent ordinal {}",
                locator.ordinal
            ),
        )
    })?;
    let value = decode_value(entry.value)?;
    if value.change_id != change_id || value.commit_id != locator.commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("tracked_state change locator for '{change_id}' points to the wrong row"),
        ));
    }
    let key = decode_key(entry.key)?;
    let (snapshot, metadata, origin_key, base_coordinate) = match payloads.decode(ordinal)? {
        CommitDeltaPayload::Authored(payload) => (
            payload.snapshot,
            payload.metadata,
            payload.origin_key,
            payload.base_coordinate,
        ),
        CommitDeltaPayload::SelectedRef(_) | CommitDeltaPayload::SelectedTombstone(_) => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state authoritative change locator for '{change_id}' points to a selected row"
                ),
            ));
        }
    };
    let updated_at = value.updated_at;
    Ok(LoadedCommitDeltaEntry {
        value,
        change_record: crate::changelog::ChangeRecord {
            format_version: 2,
            change_id,
            schema_key: key.schema_key,
            entity_pk: key.entity_pk,
            file_id: key.file_id,
            snapshot,
            metadata,
            created_at: updated_at,
            origin_key,
        },
        base_coordinate,
        selected_ref: false,
    })
}

async fn load_change_record_at_locator(
    store: &(impl StorageAdapterRead + ?Sized),
    locator: CommitDeltaChangeLocator,
) -> Result<crate::changelog::ChangeRecord, LixError> {
    let change_id = locator.change_id;
    let Some(manifest) = load_commit_delta_manifest(store, locator.commit_id).await? else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state change locator for '{change_id}' references missing commit '{}'",
                locator.commit_id
            ),
        ));
    };
    load_change_record_at_locator_in_manifest(store, locator, &manifest).await
}

async fn load_change_record_at_locator_in_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    locator: CommitDeltaChangeLocator,
    manifest: &CommitDeltaManifest,
) -> Result<crate::changelog::ChangeRecord, LixError> {
    let change_id = locator.change_id;
    try_load_change_record_at_locator_in_manifest(store, locator, manifest)
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("tracked_state change locator for '{change_id}' points to the wrong row"),
            )
        })
}

async fn try_load_change_record_at_locator_in_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    locator: CommitDeltaChangeLocator,
    manifest: &CommitDeltaManifest,
) -> Result<Option<crate::changelog::ChangeRecord>, LixError> {
    let change_id = locator.change_id;
    let (segment, bounds) = if let Some(inline) = manifest.inline_segment() {
        if locator.segment_index != 0 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state change locator for '{change_id}' references segment {} of an inline commit",
                    locator.segment_index
                ),
            ));
        }
        (Bytes::copy_from_slice(inline), None)
    } else {
        let segment_index = usize::try_from(locator.segment_index).expect("u32 fits usize");
        let bounds = manifest.segments.get(segment_index).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state change locator for '{change_id}' references missing segment {}",
                    locator.segment_index
                ),
            )
        })?;
        let segment_key = StorageKey(Bytes::from(commit_delta_segment_key_for_bounds(
            locator.commit_id,
            segment_index,
            bounds,
        )?));
        let segment = PointReadPlan::new(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            std::slice::from_ref(&segment_key),
        )
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .next()
        .flatten()
        .and_then(full_value_bytes)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state change locator for '{change_id}' references absent segment {}",
                    locator.segment_index
                ),
            )
        })?;
        (segment, Some(bounds))
    };
    Ok(Some(
        decode_change_at_locator(&segment, bounds, locator)?.change_record,
    ))
}

pub(crate) fn decode_change_locator(
    change_id: crate::changelog::ChangeId,
    bytes: &[u8],
) -> Result<CommitDeltaChangeLocator, LixError> {
    let mut cursor = 0usize;
    let encoding = *bytes.get(cursor).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("tracked_state change locator for '{change_id}' is truncated"),
        )
    })?;
    cursor += 1;
    let commit_id = match encoding {
        0 => {
            let encoded_delta = decode_locator_varint(bytes, &mut cursor)
                .ok_or_else(|| invalid_change_locator(change_id, "has an invalid commit delta"))?;
            let delta = ((encoded_delta >> 1) as i64) ^ -((encoded_delta & 1) as i64);
            let change = change_id.as_uuid().as_u128();
            let commit = if delta >= 0 {
                change.checked_add(delta as u128)
            } else {
                change.checked_sub(u128::from(delta.unsigned_abs()))
            }
            .ok_or_else(|| invalid_change_locator(change_id, "commit delta overflows"))?;
            uuid::Uuid::from_u128(commit)
        }
        1 => {
            let common_prefix = usize::from(*bytes.get(cursor).ok_or_else(|| {
                invalid_change_locator(change_id, "is missing its commit prefix length")
            })?);
            cursor += 1;
            if common_prefix > 16 || bytes.len() - cursor < 16 - common_prefix {
                return Err(invalid_change_locator(
                    change_id,
                    "has an invalid commit id",
                ));
            }
            let suffix_end = cursor + 16 - common_prefix;
            let mut commit_id = *change_id.as_uuid().as_bytes();
            commit_id[common_prefix..].copy_from_slice(&bytes[cursor..suffix_end]);
            cursor = suffix_end;
            uuid::Uuid::from_bytes(commit_id)
        }
        _ => {
            return Err(invalid_change_locator(
                change_id,
                "has an unsupported encoding",
            ));
        }
    };
    let packed_ordinal = decode_locator_varint(bytes, &mut cursor)
        .filter(|_| cursor == bytes.len())
        .ok_or_else(|| invalid_change_locator(change_id, "has an invalid ordinal"))?;
    let segment_index = u32::try_from(packed_ordinal / COMMIT_DELTA_SEGMENT_MAX_ROWS as u64)
        .map_err(|_| invalid_change_locator(change_id, "has an invalid segment"))?;
    let ordinal = u16::try_from(packed_ordinal % COMMIT_DELTA_SEGMENT_MAX_ROWS as u64)
        .expect("segment remainder fits u16");
    Ok(CommitDeltaChangeLocator {
        change_id,
        commit_id: CommitId::new(commit_id),
        segment_index,
        ordinal,
    })
}

fn encode_change_locator(locator: CommitDeltaChangeLocator) -> Vec<u8> {
    let packed_ordinal = u64::from(locator.segment_index)
        * u64::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS).expect("segment row limit fits u64")
        + u64::from(locator.ordinal);
    let change_uuid = locator.change_id.as_uuid();
    let commit_uuid = locator.commit_id.as_uuid();
    let numeric_delta = if commit_uuid.as_u128() >= change_uuid.as_u128() {
        i128::try_from(commit_uuid.as_u128() - change_uuid.as_u128()).ok()
    } else {
        i128::try_from(change_uuid.as_u128() - commit_uuid.as_u128())
            .ok()
            .map(|delta| -delta)
    };
    if let Some(delta) = numeric_delta.and_then(|delta| i64::try_from(delta).ok()) {
        let mut encoded = Vec::with_capacity(12);
        encoded.push(0);
        let zigzag = ((delta << 1) ^ (delta >> 63)) as u64;
        encode_locator_varint(zigzag, &mut encoded);
        encode_locator_varint(packed_ordinal, &mut encoded);
        return encoded;
    }
    let change_id = change_uuid.as_bytes();
    let commit_id = commit_uuid.as_bytes();
    let common_prefix = change_id
        .iter()
        .zip(commit_id)
        .position(|(change, commit)| change != commit)
        .unwrap_or(16);
    let mut encoded = Vec::with_capacity(19);
    encoded.push(1);
    encoded.push(u8::try_from(common_prefix).expect("UUID prefix length fits u8"));
    encoded.extend_from_slice(&commit_id[common_prefix..]);
    encode_locator_varint(packed_ordinal, &mut encoded);
    encoded
}

fn invalid_change_locator(change_id: crate::changelog::ChangeId, reason: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state change locator for '{change_id}' {reason}"),
    )
}

fn encode_locator_varint(mut value: u64, encoded: &mut Vec<u8>) {
    while value >= 0x80 {
        encoded.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
}

fn decode_locator_varint(bytes: &[u8], cursor: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..=63).step_by(7) {
        let byte = *bytes.get(*cursor)?;
        *cursor += 1;
        if shift == 63 && byte > 1 {
            return None;
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

/// Loads commit deltas by encoded key for first-parent batch replay.
///
/// Callers may pass `Bytes` slices that retain decoded commit-delta arenas, so
/// replay does not need to allocate schema/file strings merely to perform a
/// point lookup.
#[cfg(test)]
pub(crate) async fn load_commit_delta_values_encoded(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    encoded_keys: &[Bytes],
) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
    load_commit_delta_values_encoded_with_cache(store, commit_id, encoded_keys, None).await
}

pub(crate) async fn load_commit_delta_values_encoded_with_cache(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    encoded_keys: &[Bytes],
    point_cache: Option<&CommitDeltaPointReadCache>,
) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
    let cached_manifest = point_cache
        .map(|cache| cache.manifest(commit_id))
        .transpose()?
        .flatten();
    let manifest = match cached_manifest {
        Some(manifest) => manifest,
        None => {
            let Some(state) = load_replay_commit_state_manifest(store, commit_id).await? else {
                return Ok(vec![None; encoded_keys.len()]);
            };
            if !state.mutations.replacement_part_digests.is_empty() {
                return load_compact_replacement_values_encoded(
                    store,
                    commit_id,
                    encoded_keys,
                    &state,
                )
                .await;
            }
            let manifest =
                Arc::new(expanded_commit_delta_manifest_from_commit_state(store, &state).await?);
            if let Some(point_cache) = point_cache {
                point_cache.remember_manifest(commit_id, Arc::clone(&manifest))?;
            }
            manifest
        }
    };
    let Some(source_commit_id) = manifest.selected_source_commit_id() else {
        return load_local_commit_delta_values_encoded(store, commit_id, encoded_keys, &manifest)
            .await;
    };
    if load_replay_commit_state_manifest(store, source_commit_id)
        .await?
        .is_none()
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state selected-source commit '{commit_id}' references missing authority '{source_commit_id}'"
            ),
        ));
    }
    let mut values =
        match load_commit_delta_manifest_cached(store, source_commit_id, point_cache).await? {
            Some(source_manifest) => {
                load_local_commit_delta_values_encoded(
                    store,
                    source_commit_id,
                    encoded_keys,
                    &source_manifest,
                )
                .await?
            }
            None => vec![None; encoded_keys.len()],
        };
    for value in values.iter_mut().flatten() {
        value.commit_id = commit_id;
    }
    let local =
        load_local_commit_delta_values_encoded(store, commit_id, encoded_keys, &manifest).await?;
    for (value, local) in values.iter_mut().zip(local) {
        if local.is_some() {
            *value = local;
        }
    }
    Ok(values)
}

/// Replays deltas from the one-read manifest already validated against the
/// changelog projection. Compact replacements route directly through their
/// immutable parts; ordinary manifests seed and reuse the point cache.
pub(crate) async fn load_commit_delta_values_encoded_from_replay_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &CommitStateManifest,
    encoded_keys: &[Bytes],
    point_cache: &CommitDeltaPointReadCache,
) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
    if !state.mutations.replacement_part_digests.is_empty() {
        return load_compact_replacement_values_encoded(
            store,
            state.commit_id,
            encoded_keys,
            state,
        )
        .await;
    }
    if point_cache.manifest(state.commit_id)?.is_none() {
        point_cache.remember_manifest(
            state.commit_id,
            Arc::new(commit_delta_manifest_from_commit_state(state)),
        )?;
    }
    load_commit_delta_values_encoded_with_cache(
        store,
        state.commit_id,
        encoded_keys,
        Some(point_cache),
    )
    .await
}

async fn load_compact_replacement_values_encoded(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    encoded_keys: &[Bytes],
    state: &CommitStateManifest,
) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
    if encoded_keys.is_empty() {
        return Ok(Vec::new());
    }
    let Some(set) = fresh_replacement_current_state_part_set(state)? else {
        let expanded = expanded_commit_delta_manifest_from_commit_state(store, state).await?;
        return load_local_commit_delta_values_encoded(store, commit_id, encoded_keys, &expanded)
            .await;
    };
    let generation = state
        .mutations
        .replacement_generation
        .as_ref()
        .ok_or_else(|| replacement_payload_error("compact state omitted its generation"))?;
    let lifecycle =
        state.mutations.lifecycle_summary.as_ref().ok_or_else(|| {
            replacement_payload_error("compact state omitted lifecycle authority")
        })?;
    let authority = state
        .mutations
        .replacement_parts
        .as_ref()
        .ok_or_else(|| replacement_payload_error("compact state omitted part authority"))?;
    let routes = match super::current_state_part::route_current_state_parts(
        store,
        &set.directory,
        encoded_keys,
    )
    .await
    {
        Ok(routes) => routes,
        Err(_) => {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_crud_current_state_directory_recovery();
            let expanded = expanded_commit_delta_manifest_from_commit_state(store, state).await?;
            return load_local_commit_delta_values_encoded(
                store,
                commit_id,
                encoded_keys,
                &expanded,
            )
            .await;
        }
    };
    let mut grouped = BTreeMap::<u32, (CurrentStatePartDescriptor, Vec<usize>)>::new();
    for (output_index, descriptor) in routes.into_iter().enumerate() {
        let Some(descriptor) = descriptor else {
            continue;
        };
        let part_index = descriptor.part_index as usize;
        let expected_digest = state
            .mutations
            .replacement_part_digests
            .get(part_index)
            .copied()
            .or_else(|| {
                state
                    .mutations
                    .parts
                    .get(part_index)
                    .and_then(|part| part.replacement_part.as_ref())
                    .map(|part| part.content_digest)
            })
            .ok_or_else(|| replacement_payload_error("directory part index is out of bounds"))?;
        if descriptor.content_digest != expected_digest
            || state
                .mutations
                .direct_part_row_counts
                .get(descriptor.part_index as usize)
                != Some(&descriptor.row_count)
            || descriptor.owner_commit_id != generation.owner_commit_id
            || descriptor.uniform_created_at != lifecycle.uniform_created_at
            || descriptor.uniform_updated_at != authority.uniform_updated_at
        {
            return Err(replacement_payload_error(
                "directory descriptor disagrees with compact mutation authority",
            ));
        }
        grouped
            .entry(descriptor.part_index)
            .or_insert_with(|| (descriptor, Vec::new()))
            .1
            .push(output_index);
    }
    let storage_keys = grouped
        .values()
        .map(|(descriptor, _)| {
            let mut key = commit_delta_segment_key(commit_id, descriptor.part_index as usize)?;
            key.extend_from_slice(&descriptor.content_digest);
            Ok(StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let loaded = PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &storage_keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    let mut values = vec![None; encoded_keys.len()];
    for ((_, (descriptor, output_indices)), value) in grouped.into_iter().zip(loaded.value) {
        let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
            replacement_payload_error("directory references a missing immutable state part")
        })?;
        let bounds = CommitDeltaSegmentBounds {
            first_key: descriptor.first_key,
            last_key: descriptor.last_key,
            replacement_part: Some(StoredReplacementPart {
                content_digest: descriptor.content_digest,
                owner_commit_id: descriptor.owner_commit_id,
                first_address: descriptor.part_index.saturating_mul(
                    u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS)
                        .expect("replacement row bound fits u32"),
                ),
                uniform_created_at: descriptor.uniform_created_at,
                uniform_updated_at: descriptor.uniform_updated_at,
            }),
        };
        let leaf = decode_commit_delta_segment(&bytes, Some(&bounds), commit_id)?;
        for output_index in output_indices {
            values[output_index] =
                find_commit_delta_value(&leaf, &encoded_keys[output_index], commit_id)?;
        }
    }
    Ok(values)
}

async fn load_local_commit_delta_values_encoded(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    encoded_keys: &[Bytes],
    manifest: &CommitDeltaManifest,
) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
    if encoded_keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut values = vec![None; encoded_keys.len()];
    if let Some(inline_segment) = manifest.inline_segment() {
        let leaf = decode_commit_delta_segment(inline_segment, None, commit_id)?;
        for (output_index, encoded_key) in encoded_keys.iter().enumerate() {
            values[output_index] = find_commit_delta_value(&leaf, encoded_key, commit_id)?;
        }
        return Ok(values);
    }
    // Keep one dense lookup column instead of one tree node and one owned
    // vector per touched segment. The key bytes remain in the caller's shared
    // arena; rows retain only their output ordinal.
    let mut lookups = Vec::<(usize, usize)>::with_capacity(encoded_keys.len());
    for (output_index, encoded_key) in encoded_keys.iter().enumerate() {
        if let Some(segment_index) = commit_delta_segment_for_key(manifest, encoded_key) {
            lookups.push((segment_index, output_index));
        }
    }
    if lookups.is_empty() {
        return Ok(values);
    }
    lookups.sort_unstable();
    let segment_count = 1 + lookups
        .windows(2)
        .filter(|pair| pair[0].0 != pair[1].0)
        .count();
    let mut segment_ranges = Vec::with_capacity(segment_count);
    let mut offset = 0;
    while offset < lookups.len() {
        let segment_index = lookups[offset].0;
        let mut end = offset + 1;
        while end < lookups.len() && lookups[end].0 == segment_index {
            end += 1;
        }
        segment_ranges.push((segment_index, offset, end));
        offset = end;
    }
    let storage_keys = segment_ranges
        .iter()
        .map(|&(segment_index, _, _)| {
            commit_delta_segment_key_for_bounds(
                commit_id,
                segment_index,
                &manifest.segments[segment_index],
            )
            .map(|key| StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let result = PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &storage_keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    for ((segment_index, start, end), value) in segment_ranges.into_iter().zip(result.value) {
        let bytes = value
            .and_then(full_value_bytes)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state commit_delta manifest for commit '{commit_id}' references missing segment {segment_index}"
                    ),
                )
            })?;
        let leaf = decode_commit_delta_segment(
            &bytes,
            Some(&manifest.segments[segment_index]),
            commit_id,
        )?;
        for &(_, output_index) in &lookups[start..end] {
            values[output_index] =
                find_commit_delta_value(&leaf, &encoded_keys[output_index], commit_id)?;
        }
    }
    Ok(values)
}

/// Loads authoritative change records for exact identities in one physical
/// commit delta. This is the payload counterpart to
/// [`load_commit_delta_values`]: callers already know the owning commit from
/// the endpoint index value, so no global changelog or delta-space scan is
/// necessary.
pub(crate) async fn load_commit_delta_change_records(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    keys: &[TrackedStateKey],
) -> Result<Vec<Option<crate::changelog::ChangeRecord>>, LixError> {
    let requests = keys
        .iter()
        .cloned()
        .map(|key| (commit_id, key))
        .collect::<Vec<_>>();
    Ok(load_owned_commit_delta_entries(store, &requests)
        .await?
        .into_iter()
        .map(|entry| entry.map(|entry| entry.change_record))
        .collect())
}

/// Loads every tracked member of one physical commit delta.
///
/// A known commit without a manifest is an empty commit. A present manifest is
/// authoritative: every identity must carry its payload in the same packed
/// record. Selected cascade tombstones may share their source change id; live
/// and authored rows remain unique by change id.
pub(crate) async fn load_commit_delta_members_with_payloads(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Vec<CommitDeltaMember>, LixError> {
    Ok(
        load_commit_delta_members_with_payloads_for_schemas(store, commit_id, &[], usize::MAX)
            .await?
            .expect("unbounded commit-delta payload scan cannot exceed its segment limit"),
    )
}

/// Loads tracked members and payloads for only the requested schemas.
///
/// Segment bounds route around unrelated schema ranges before payload-sidecar
/// decoding. An empty schema list retains the full inventory behavior used by
/// history, rebuild, and lifecycle callers.
pub(crate) async fn load_commit_delta_members_with_payloads_for_schemas(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    schema_keys: &[String],
    max_segment_count: usize,
) -> Result<Option<Vec<CommitDeltaMember>>, LixError> {
    let Some(manifest) = load_commit_delta_manifest(store, commit_id).await? else {
        return Ok(Some(Vec::new()));
    };
    let requested_schemas = schema_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let local_segment_count = if manifest.inline_segment().is_some() {
        1
    } else {
        commit_delta_segment_count_for_schemas_up_to(
            &manifest,
            &requested_schemas,
            max_segment_count,
        )
    };
    if local_segment_count > max_segment_count {
        return Ok(None);
    }
    let source = if let Some(source_commit_id) = manifest.selected_source_commit_id() {
        let source_manifest = load_commit_delta_manifest(store, source_commit_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "selected-source commit delta '{}' references missing source '{}'",
                        commit_id, source_commit_id
                    ),
                )
            })?;
        let source_segment_count = if source_manifest.inline_segment().is_some() {
            1
        } else {
            commit_delta_segment_count_for_schemas_up_to(
                &source_manifest,
                &requested_schemas,
                max_segment_count.saturating_sub(local_segment_count),
            )
        };
        if local_segment_count.saturating_add(source_segment_count) > max_segment_count {
            return Ok(None);
        }
        Some((source_commit_id, source_manifest))
    } else {
        None
    };
    let mut local =
        load_commit_delta_members_from_manifest(store, commit_id, &manifest, schema_keys).await?;
    let Some((source_commit_id, source_manifest)) = source else {
        return Ok(Some(local));
    };
    if source_manifest.selected_source_commit_id().is_some() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "selected-source commit delta chains are unsupported",
        ));
    }
    let mut members = load_commit_delta_members_from_manifest(
        store,
        source_commit_id,
        &source_manifest,
        schema_keys,
    )
    .await?;
    for member in &mut members {
        member.value.commit_id = commit_id;
        member.authored = false;
        member.selected_tombstone = member.value.deleted;
    }
    members.append(&mut local);
    members.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    if members.windows(2).any(|pair| pair[0].key == pair[1].key) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("selected-source commit delta '{commit_id}' has overlapping local rows"),
        ));
    }
    Ok(Some(members))
}

async fn load_commit_delta_members_from_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    manifest: &CommitDeltaManifest,
    schema_keys: &[String],
) -> Result<Vec<CommitDeltaMember>, LixError> {
    let requested_schemas = schema_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut members = Vec::new();
    if let Some(inline_segment) = manifest.inline_segment() {
        collect_strict_commit_delta_members(inline_segment, None, commit_id, 0, &mut members)?;
    } else {
        let segment_indices = commit_delta_segments_for_schemas(manifest, &requested_schemas);
        let segment_keys = segment_indices
            .iter()
            .map(|&segment_index| {
                commit_delta_segment_key_for_bounds(
                    commit_id,
                    segment_index,
                    &manifest.segments[segment_index],
                )
                .map(|key| StorageKey(Bytes::from(key)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let segments = PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &segment_keys)
            .materialize(store, StorageGetOptions::default())
            .await?;
        for (segment_index, value) in segment_indices.into_iter().zip(segments.value) {
            let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state commit_delta manifest for commit '{commit_id}' references missing segment {segment_index}"
                    ),
                )
            })?;
            collect_strict_commit_delta_members(
                &bytes,
                Some(&manifest.segments[segment_index]),
                commit_id,
                u32::try_from(segment_index).expect("segment index fits u32"),
                &mut members,
            )?;
        }
    }
    if !requested_schemas.is_empty() {
        members.retain(|member| requested_schemas.contains(member.key.schema_key.as_str()));
    }
    hydrate_selected_members(store, &mut members).await?;
    validate_commit_delta_member_order_and_ids(commit_id, &members)?;
    Ok(members)
}

async fn hydrate_selected_members(
    store: &(impl StorageAdapterRead + ?Sized),
    members: &mut [CommitDeltaMember],
) -> Result<(), LixError> {
    let selected = members
        .iter()
        .enumerate()
        .filter(|(_, member)| !member.authored && !member.selected_tombstone)
        .map(|(index, member)| (index, member.value.change_id))
        .collect::<Vec<_>>();
    let change_ids = selected
        .iter()
        .map(|(_, change_id)| *change_id)
        .collect::<Vec<_>>();
    let canonical = load_change_records_by_ids(store, &change_ids).await?;
    for ((index, _), change_record) in selected.into_iter().zip(canonical) {
        let member = &mut members[index];
        if change_record.schema_key != member.key.schema_key
            || change_record.file_id != member.key.file_id
            || change_record.entity_pk != member.key.entity_pk
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state selected change '{}' references canonical authority for a different identity",
                    member.value.change_id
                ),
            ));
        }
        member.change.snapshot = change_record.snapshot;
        member.change.metadata = change_record.metadata;
        member.change.origin_key = change_record.origin_key;
    }
    Ok(())
}

pub(crate) async fn scan_commit_delta_members(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Vec<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
    let batch = scan_commit_delta_values(store, commit_id, &[]).await?;
    let mut members = Vec::with_capacity(batch.len());
    for row in batch.iter() {
        let key = row.key_ref();
        let key = TrackedStateKey {
            schema_key: key.schema_key.to_owned(),
            file_id: key.file_id.map(str::to_owned),
            entity_pk: key.entity_pk.clone(),
        };
        if members.last().is_some_and(|(previous, _)| previous >= &key) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit_delta for commit '{commit_id}' is not strictly ordered across segments"
                ),
            ));
        }
        let value = row.value().clone();
        members.push((key, value));
    }
    Ok(members)
}

/// Public commit membership is deterministic by change id, independent of the
/// physical identity order used by the packed delta.
#[cfg(test)]
pub(crate) async fn load_commit_delta_change_ids(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Vec<crate::changelog::ChangeId>, LixError> {
    let mut change_ids = scan_commit_delta_members(store, commit_id)
        .await?
        .into_iter()
        .map(|(_, value)| value.change_id)
        .collect::<Vec<_>>();
    change_ids.sort_unstable();
    change_ids.dedup();
    Ok(change_ids)
}

/// Loads exact tracked-state entries from their known physical commit owners.
///
/// All owner manifests are read in one point batch and all routed segments in
/// a second. Each selected segment is decoded once for both its index values
/// and payload sidecar, preserving request order without topology replay.
pub(crate) async fn load_owned_commit_delta_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    requests: &[(CommitId, TrackedStateKey)],
) -> Result<Vec<Option<LoadedCommitDeltaEntry>>, LixError> {
    let mut output = load_local_owned_commit_delta_entries(store, requests).await?;
    let owner_commit_ids = requests
        .iter()
        .enumerate()
        .filter_map(|(request_index, (commit_id, _))| {
            output[request_index].is_none().then_some(*commit_id)
        })
        .collect::<BTreeSet<_>>();
    let owner_commit_ids = owner_commit_ids.into_iter().collect::<Vec<_>>();
    let manifest_values = load_commit_delta_manifests(store, &owner_commit_ids).await?;
    let mut owner_manifests = BTreeMap::new();
    for (commit_id, manifest) in owner_commit_ids.into_iter().zip(manifest_values) {
        let Some(manifest) = manifest else {
            continue;
        };
        owner_manifests.insert(commit_id, manifest);
    }
    let mut source_requests = Vec::new();
    let mut source_outputs = Vec::new();
    let mut source_owner_commits = Vec::new();
    for (request_index, (commit_id, key)) in requests.iter().enumerate() {
        if output[request_index].is_some() {
            continue;
        }
        let Some(manifest) = owner_manifests.get(commit_id) else {
            continue;
        };
        let Some(source_commit_id) = manifest.selected_source_commit_id() else {
            continue;
        };
        source_outputs.push(request_index);
        source_owner_commits.push(*commit_id);
        source_requests.push((source_commit_id, key.clone()));
    }
    let selected = load_local_owned_commit_delta_entries(store, &source_requests).await?;
    for ((request_index, owner_commit_id), entry) in source_outputs
        .into_iter()
        .zip(source_owner_commits)
        .zip(selected)
    {
        if let Some(mut entry) = entry {
            entry.value.commit_id = owner_commit_id;
            entry.selected_ref = true;
            output[request_index] = Some(entry);
        }
    }
    Ok(output)
}

/// Loads one ordered exact-key batch without first owning a second copy of
/// every identity. Dense current-state reads have this shape and normally
/// resolve every key from the physical owner; unusual selected-source or
/// missing-key cases fall back to the general loader.
pub(crate) async fn load_owned_commit_delta_entries_one_ordered_ref(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    keys: &[TrackedStateKeyRef<'_>],
    point_cache: Option<&CommitDeltaPointReadCache>,
) -> Result<Vec<Option<LoadedCommitDeltaEntry>>, LixError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let strictly_ordered = keys.windows(2).all(|pair| {
        (pair[0].schema_key, pair[0].file_id, pair[0].entity_pk)
            < (pair[1].schema_key, pair[1].file_id, pair[1].entity_pk)
    });
    if strictly_ordered {
        let output =
            load_local_owned_commit_delta_entries_one_ordered(store, commit_id, keys, point_cache)
                .await?;
        if output.iter().all(Option::is_some) {
            return Ok(output);
        }
    }
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_crud_ordered_delta_fallback();
    let requests = keys
        .iter()
        .map(|key| {
            (
                commit_id,
                TrackedStateKey {
                    schema_key: key.schema_key.to_owned(),
                    file_id: key.file_id.map(str::to_owned),
                    entity_pk: key.entity_pk.clone(),
                },
            )
        })
        .collect::<Vec<_>>();
    load_owned_commit_delta_entries(store, &requests).await
}

async fn load_local_owned_commit_delta_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    requests: &[(CommitId, TrackedStateKey)],
) -> Result<Vec<Option<LoadedCommitDeltaEntry>>, LixError> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    if requests
        .windows(2)
        .all(|pair| pair[0].0 == pair[1].0 && pair[0].1 < pair[1].1)
    {
        let keys = requests
            .iter()
            .map(|(_, key)| TrackedStateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            })
            .collect::<Vec<_>>();
        return Box::pin(load_local_owned_commit_delta_entries_one_ordered(
            store,
            requests[0].0,
            &keys,
            None,
        ))
        .await;
    }

    let mut request_indices_by_commit = BTreeMap::<CommitId, Vec<usize>>::new();
    for (request_index, (commit_id, _)) in requests.iter().enumerate() {
        request_indices_by_commit
            .entry(*commit_id)
            .or_default()
            .push(request_index);
    }

    let commit_ids = request_indices_by_commit
        .keys()
        .copied()
        .collect::<Vec<_>>();
    let manifest_values = load_commit_delta_manifests(store, &commit_ids).await?;

    let mut output = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
    let mut segmented_manifests = BTreeMap::<CommitId, CommitDeltaManifest>::new();
    let mut lookups_by_segment = BTreeMap::<(CommitId, usize), Vec<(usize, Vec<u8>)>>::new();

    for (commit_id, manifest) in commit_ids.into_iter().zip(manifest_values) {
        let Some(manifest) = manifest else {
            continue;
        };
        let request_indices = request_indices_by_commit
            .get(&commit_id)
            .expect("manifest commit came from the requested commit set");
        if let Some(inline_segment) = manifest.inline_segment() {
            let (leaf, payloads) = decode_commit_delta_with_payloads(inline_segment, None)?;
            for &request_index in request_indices {
                let encoded_key = encoded_commit_delta_lookup_key(&requests[request_index].1);
                output[request_index] =
                    find_loaded_commit_delta_entry(&leaf, &payloads, &encoded_key, commit_id)?;
            }
            continue;
        }

        for &request_index in request_indices {
            let encoded_key = encoded_commit_delta_lookup_key(&requests[request_index].1);
            if let Some(segment_index) = commit_delta_segment_for_key(&manifest, &encoded_key) {
                lookups_by_segment
                    .entry((commit_id, segment_index))
                    .or_default()
                    .push((request_index, encoded_key));
            }
        }
        segmented_manifests.insert(commit_id, manifest);
    }

    if lookups_by_segment.is_empty() {
        hydrate_selected_loaded_entries(store, &mut output).await?;
        return Ok(output);
    }

    let segment_routes = lookups_by_segment.keys().copied().collect::<Vec<_>>();
    let segment_keys = segment_routes
        .iter()
        .map(|&(commit_id, segment_index)| {
            commit_delta_segment_key_for_bounds(
                commit_id,
                segment_index,
                &segmented_manifests[&commit_id].segments[segment_index],
            )
            .map(|key| StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let segment_values =
        PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &segment_keys)
            .materialize(store, StorageGetOptions::default())
            .await?;

    for ((commit_id, segment_index), segment_value) in
        segment_routes.into_iter().zip(segment_values.value)
    {
        let bytes = segment_value.and_then(full_value_bytes).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit_delta manifest for commit '{commit_id}' references missing segment {segment_index}"
                ),
            )
        })?;
        let manifest = segmented_manifests.get(&commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit_delta lost the manifest for routed commit '{commit_id}'"
                ),
            )
        })?;
        let bounds = manifest.segments.get(segment_index).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit_delta manifest for commit '{commit_id}' has no segment {segment_index}"
                ),
            )
        })?;
        let (leaf, payloads) = decode_commit_delta_with_payloads(&bytes, Some(bounds))?;
        let lookups = lookups_by_segment
            .remove(&(commit_id, segment_index))
            .expect("read segment came from the routed lookup set");
        for (request_index, encoded_key) in lookups {
            output[request_index] =
                find_loaded_commit_delta_entry(&leaf, &payloads, &encoded_key, commit_id)?;
        }
    }
    hydrate_selected_loaded_entries(store, &mut output).await?;
    Ok(output)
}

async fn load_compact_replacement_entries_one_ordered(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    keys: &[TrackedStateKeyRef<'_>],
    state: &CommitStateManifest,
) -> Result<Vec<Option<LoadedCommitDeltaEntry>>, LixError> {
    let Some(set) = fresh_replacement_current_state_part_set(state)? else {
        return Err(replacement_payload_error(
            "compact replacement omitted its direct current-state route",
        ));
    };
    let generation = state
        .mutations
        .replacement_generation
        .as_ref()
        .ok_or_else(|| replacement_payload_error("compact state omitted its generation"))?;
    let lifecycle =
        state.mutations.lifecycle_summary.as_ref().ok_or_else(|| {
            replacement_payload_error("compact state omitted lifecycle authority")
        })?;
    let authority = state
        .mutations
        .replacement_parts
        .as_ref()
        .ok_or_else(|| replacement_payload_error("compact state omitted part authority"))?;
    let encoded_keys = keys
        .iter()
        .map(|&key| Bytes::from(encode_key_ref(key)))
        .collect::<Vec<_>>();
    let routes =
        super::current_state_part::route_current_state_parts(store, &set.directory, &encoded_keys)
            .await?;
    let mut grouped = BTreeMap::<u32, (CurrentStatePartDescriptor, Vec<usize>)>::new();
    for (output_index, descriptor) in routes.into_iter().enumerate() {
        let Some(descriptor) = descriptor else {
            continue;
        };
        let expected_digest = state
            .mutations
            .replacement_part_digests
            .get(descriptor.part_index as usize)
            .ok_or_else(|| replacement_payload_error("directory part index is out of bounds"))?;
        if descriptor.content_digest != *expected_digest
            || state
                .mutations
                .direct_part_row_counts
                .get(descriptor.part_index as usize)
                != Some(&descriptor.row_count)
            || descriptor.owner_commit_id != generation.owner_commit_id
            || descriptor.uniform_created_at != lifecycle.uniform_created_at
            || descriptor.uniform_updated_at != authority.uniform_updated_at
        {
            return Err(replacement_payload_error(
                "directory descriptor disagrees with compact mutation authority",
            ));
        }
        grouped
            .entry(descriptor.part_index)
            .or_insert_with(|| (descriptor, Vec::new()))
            .1
            .push(output_index);
    }
    let storage_keys = grouped
        .values()
        .map(|(descriptor, _)| {
            let mut key = commit_delta_segment_key(commit_id, descriptor.part_index as usize)?;
            key.extend_from_slice(&descriptor.content_digest);
            Ok(StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let loaded = PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &storage_keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    let mut output = (0..keys.len()).map(|_| None).collect::<Vec<_>>();
    for ((_, (descriptor, output_indices)), value) in grouped.into_iter().zip(loaded.value) {
        let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
            replacement_payload_error("directory references a missing immutable state part")
        })?;
        let bounds = CommitDeltaSegmentBounds {
            first_key: descriptor.first_key,
            last_key: descriptor.last_key,
            replacement_part: Some(StoredReplacementPart {
                content_digest: descriptor.content_digest,
                owner_commit_id: descriptor.owner_commit_id,
                first_address: descriptor.part_index.saturating_mul(
                    u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS)
                        .expect("replacement row bound fits u32"),
                ),
                uniform_created_at: descriptor.uniform_created_at,
                uniform_updated_at: descriptor.uniform_updated_at,
            }),
        };
        let (leaf, payloads) = decode_commit_delta_with_payloads(&bytes, Some(&bounds))?;
        for output_index in output_indices {
            output[output_index] = find_loaded_commit_delta_entry(
                &leaf,
                &payloads,
                &encoded_keys[output_index],
                commit_id,
            )?;
        }
    }
    hydrate_selected_loaded_entries(store, &mut output).await?;
    Ok(output)
}

async fn load_inventory_part_entries_one_ordered(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    keys: &[TrackedStateKeyRef<'_>],
    state: &CommitStateManifest,
) -> Result<Vec<Option<LoadedCommitDeltaEntry>>, LixError> {
    let inventory = &state.mutations;
    if !inventory_parts_support_direct_route(inventory) {
        return Err(replacement_payload_error(
            "ordered mutation inventory has invalid part bounds",
        ));
    }
    let mut encoded_keys = Vec::new();
    let mut grouped = BTreeMap::<usize, Vec<(usize, Range<usize>)>>::new();
    for (output_index, &key) in keys.iter().enumerate() {
        let encoded = encode_key_ref_into(&mut encoded_keys, key);
        let key_bytes = &encoded_keys[encoded.clone()];
        let mut lower = 0usize;
        let mut upper = inventory.parts.len();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if inventory.parts[middle].first_key.as_slice() <= key_bytes {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        let Some(part_index) = lower.checked_sub(1) else {
            continue;
        };
        if key_bytes <= inventory.parts[part_index].last_key.as_slice() {
            grouped
                .entry(part_index)
                .or_default()
                .push((output_index, encoded));
        }
    }
    let storage_keys = grouped
        .keys()
        .map(|&part_index| {
            let bounds = &inventory.parts[part_index];
            commit_delta_segment_key_for_bounds(
                commit_id,
                part_index,
                &CommitDeltaSegmentBounds {
                    first_key: bounds.first_key.clone(),
                    last_key: bounds.last_key.clone(),
                    replacement_part: bounds.replacement_part.clone(),
                },
            )
            .map(|key| StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let loaded = PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &storage_keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    let mut output = (0..keys.len()).map(|_| None).collect::<Vec<_>>();
    for ((part_index, requests), value) in grouped.into_iter().zip(loaded.value) {
        let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
            replacement_payload_error("mutation inventory references a missing immutable part")
        })?;
        let part = &inventory.parts[part_index];
        let bounds = CommitDeltaSegmentBounds {
            first_key: part.first_key.clone(),
            last_key: part.last_key.clone(),
            replacement_part: part.replacement_part.clone(),
        };
        let (leaf, payloads) = decode_commit_delta_with_payloads(&bytes, Some(&bounds))?;
        for (output_index, encoded) in requests {
            output[output_index] = find_loaded_commit_delta_entry(
                &leaf,
                &payloads,
                &encoded_keys[encoded],
                commit_id,
            )?;
        }
    }
    Ok(output)
}

fn inventory_parts_support_direct_route(inventory: &CommitStateMutationInventory) -> bool {
    !(inventory.selected_source_commit_id.is_some()
        || !inventory.inline_part.is_empty()
        || inventory.parts.is_empty()
        || inventory.parts.len() != inventory.direct_part_row_counts.len()
        || inventory.parts.iter().any(|part| {
            part.first_key.is_empty() || part.last_key.is_empty() || part.first_key > part.last_key
        })
        || inventory
            .parts
            .windows(2)
            .any(|pair| pair[0].last_key >= pair[1].first_key))
}

/// Fast path for one physical owner and an already ordered exact-key batch.
///
/// Dense current-state replacement reads naturally have this shape. Route the
/// monotonic key stream through the manifest once instead of allocating a
/// commit map, a segment B-tree, and one lookup vector per segment.
async fn load_local_owned_commit_delta_entries_one_ordered(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    keys: &[TrackedStateKeyRef<'_>],
    point_cache: Option<&CommitDeltaPointReadCache>,
) -> Result<Vec<Option<LoadedCommitDeltaEntry>>, LixError> {
    let manifest = match point_cache
        .map(|cache| cache.manifest(commit_id))
        .transpose()?
        .flatten()
    {
        Some(manifest) => PointReadCommitDeltaManifest::Cached(manifest),
        None => {
            let Some(state) = load_point_replay_commit_state(store, commit_id).await? else {
                return Ok((0..keys.len()).map(|_| None).collect());
            };
            if state.mutations.replacement_generation.is_some()
                && state.current_state_coverage_anchor.is_some()
            {
                let output =
                    load_compact_replacement_entries_one_ordered(store, commit_id, keys, &state)
                        .await?;
                remember_expanded_point_manifest(store, &state, point_cache).await?;
                return Ok(output);
            }
            if keys.len() <= DECODED_COMMIT_DELTA_CACHE_MAX_POINT_KEYS
                && inventory_parts_support_direct_route(&state.mutations)
            {
                let output =
                    load_inventory_part_entries_one_ordered(store, commit_id, keys, &state).await?;
                remember_expanded_point_manifest(store, &state, point_cache).await?;
                return Ok(output);
            }
            let manifest = expanded_commit_delta_manifest_from_commit_state(store, &state).await?;
            validate_commit_delta_manifest(&manifest)?;
            if manifest
                .replacement_generation
                .as_ref()
                .is_some_and(|generation| {
                    generation.owner_commit_id != *commit_id.as_uuid().as_bytes()
                })
            {
                return Err(replacement_payload_error(
                    "replacement generation does not belong to its commit",
                ));
            }
            match point_cache {
                Some(cache) => {
                    let manifest = Arc::new(manifest);
                    cache.remember_manifest(commit_id, Arc::clone(&manifest))?;
                    PointReadCommitDeltaManifest::Cached(manifest)
                }
                None => PointReadCommitDeltaManifest::Owned(manifest),
            }
        }
    };
    let mut output = (0..keys.len()).map(|_| None).collect::<Vec<_>>();
    if let Some(inline_segment) = manifest.inline_segment() {
        if keys.len() <= DECODED_COMMIT_DELTA_CACHE_MAX_POINT_KEYS
            && let Some(cache) = point_cache
        {
            let decoded = match cache.segment(commit_id, 0, None)? {
                Some(decoded) => decoded,
                None => {
                    if !cache.should_admit_segment(commit_id, 0)? {
                        let (leaf, payloads) =
                            decode_commit_delta_with_payloads(inline_segment, None)?;
                        for (request_index, &key) in keys.iter().enumerate() {
                            let encoded_key = encode_key_ref(key);
                            output[request_index] = find_loaded_commit_delta_entry(
                                &leaf,
                                &payloads,
                                &encoded_key,
                                commit_id,
                            )?;
                        }
                        hydrate_selected_loaded_entries(store, &mut output).await?;
                        return Ok(output);
                    }
                    let decoded = decode_owned_commit_delta_segment(inline_segment, None)?;
                    cache.remember_segment(commit_id, 0, Arc::clone(&decoded))?;
                    decoded
                }
            };
            for (request_index, &key) in keys.iter().enumerate() {
                let encoded_key = encode_key_ref(key);
                output[request_index] = find_loaded_commit_delta_entry(
                    &decoded.leaf,
                    &decoded.payloads,
                    &encoded_key,
                    commit_id,
                )?;
            }
            hydrate_selected_loaded_entries(store, &mut output).await?;
            return Ok(output);
        }
        if keys.len() <= DECODED_COMMIT_DELTA_CACHE_MAX_POINT_KEYS {
            if let Some(decoded) = decode_commit_delta_with_payloads_cached(inline_segment, None)? {
                for (request_index, &key) in keys.iter().enumerate() {
                    let encoded_key = encode_key_ref(key);
                    output[request_index] = find_loaded_commit_delta_entry(
                        &decoded.leaf,
                        &decoded.payloads,
                        &encoded_key,
                        commit_id,
                    )?;
                }
                hydrate_selected_loaded_entries(store, &mut output).await?;
                return Ok(output);
            }
            let (leaf, payloads) = decode_commit_delta_with_payloads(inline_segment, None)?;
            for (request_index, &key) in keys.iter().enumerate() {
                let encoded_key = encode_key_ref(key);
                output[request_index] =
                    find_loaded_commit_delta_entry(&leaf, &payloads, &encoded_key, commit_id)?;
            }
            hydrate_selected_loaded_entries(store, &mut output).await?;
            return Ok(output);
        }
        let (leaf, payloads) = decode_commit_delta_with_payloads(inline_segment, None)?;
        for (request_index, &key) in keys.iter().enumerate() {
            let encoded_key = encode_key_ref(key);
            output[request_index] =
                find_loaded_commit_delta_entry(&leaf, &payloads, &encoded_key, commit_id)?;
        }
        hydrate_selected_loaded_entries(store, &mut output).await?;
        return Ok(output);
    }

    let mut encoded_keys = Vec::new();
    let mut routed = Vec::<(usize, usize, Range<usize>)>::with_capacity(keys.len());
    let mut segment_indices = Vec::new();
    for (request_index, &key) in keys.iter().enumerate() {
        let encoded_key = encode_key_ref_into(&mut encoded_keys, key);
        let Some(segment_index) =
            commit_delta_segment_for_key(&manifest, &encoded_keys[encoded_key.clone()])
        else {
            continue;
        };
        if segment_indices.last().copied() != Some(segment_index) {
            debug_assert!(
                segment_indices
                    .last()
                    .is_none_or(|previous| *previous < segment_index),
                "ordered commit-delta keys must route monotonically"
            );
            segment_indices.push(segment_index);
        }
        routed.push((request_index, segment_index, encoded_key));
    }
    if keys.len() <= DECODED_COMMIT_DELTA_CACHE_MAX_POINT_KEYS
        && let Some(cache) = point_cache
    {
        let mut decoded_segments = Vec::with_capacity(segment_indices.len());
        let mut missing_indices = Vec::new();
        let mut missing_keys = Vec::new();
        for &segment_index in &segment_indices {
            let bounds = manifest.segments.get(segment_index).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state commit_delta manifest for commit '{commit_id}' has no segment {segment_index}"
                    ),
                )
            })?;
            match cache.segment(commit_id, segment_index, Some(bounds))? {
                Some(decoded) => decoded_segments.push(Some(decoded)),
                None => {
                    decoded_segments.push(None);
                    missing_indices.push(segment_index);
                    missing_keys.push(StorageKey(Bytes::from(
                        commit_delta_segment_key_for_bounds(commit_id, segment_index, bounds)?,
                    )));
                }
            }
        }
        let missing_values =
            PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &missing_keys)
                .materialize(store, StorageGetOptions::default())
                .await?;
        let mut missing = missing_indices.into_iter().zip(missing_values.value);
        let mut routed = routed.into_iter().peekable();
        for (segment_position, segment_index) in segment_indices.into_iter().enumerate() {
            let decoded = match decoded_segments[segment_position].take() {
                Some(decoded) => decoded,
                None => {
                    let (missing_index, value) = missing
                        .next()
                        .expect("every uncached commit-delta segment has a point-read result");
                    debug_assert_eq!(missing_index, segment_index);
                    let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "tracked_state commit_delta manifest for commit '{commit_id}' references missing segment {segment_index}"
                            ),
                        )
                    })?;
                    let bounds = manifest.segments.get(segment_index).ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "tracked_state commit_delta manifest for commit '{commit_id}' has no segment {segment_index}"
                            ),
                        )
                    })?;
                    if !cache.should_admit_segment(commit_id, segment_index)? {
                        let (leaf, payloads) =
                            decode_commit_delta_with_payloads(&bytes, Some(bounds))?;
                        while routed
                            .peek()
                            .is_some_and(|(_, routed_segment, _)| *routed_segment == segment_index)
                        {
                            let (request_index, _, encoded_key) = routed
                                .next()
                                .expect("peeked routed lookup remains available");
                            output[request_index] = find_loaded_commit_delta_entry(
                                &leaf,
                                &payloads,
                                &encoded_keys[encoded_key],
                                commit_id,
                            )?;
                        }
                        continue;
                    }
                    let decoded = decode_owned_commit_delta_segment(&bytes, Some(bounds))?;
                    cache.remember_segment(commit_id, segment_index, Arc::clone(&decoded))?;
                    decoded
                }
            };
            while routed
                .peek()
                .is_some_and(|(_, routed_segment, _)| *routed_segment == segment_index)
            {
                let (request_index, _, encoded_key) = routed
                    .next()
                    .expect("peeked routed lookup remains available");
                output[request_index] = find_loaded_commit_delta_entry(
                    &decoded.leaf,
                    &decoded.payloads,
                    &encoded_keys[encoded_key],
                    commit_id,
                )?;
            }
        }
        debug_assert!(missing.next().is_none());
        hydrate_selected_loaded_entries(store, &mut output).await?;
        return Ok(output);
    }
    let segment_keys = segment_indices
        .iter()
        .map(|&segment_index| {
            commit_delta_segment_key_for_bounds(
                commit_id,
                segment_index,
                &manifest.segments[segment_index],
            )
            .map(|key| StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let segment_values =
        PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &segment_keys)
            .materialize(store, StorageGetOptions::default())
            .await?;
    let mut routed = routed.into_iter().peekable();
    for (segment_index, segment_value) in segment_indices.into_iter().zip(segment_values.value) {
        let bytes = segment_value.and_then(full_value_bytes).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit_delta manifest for commit '{commit_id}' references missing segment {segment_index}"
                ),
            )
        })?;
        let bounds = manifest.segments.get(segment_index).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit_delta manifest for commit '{commit_id}' has no segment {segment_index}"
                ),
            )
        })?;
        if keys.len() <= DECODED_COMMIT_DELTA_CACHE_MAX_POINT_KEYS {
            let decoded = decode_commit_delta_with_payloads_cached(&bytes, Some(bounds))?;
            let (leaf, payloads) = if let Some(decoded) = decoded.as_ref() {
                (&decoded.leaf, &decoded.payloads)
            } else {
                let (leaf, payloads) = decode_commit_delta_with_payloads(&bytes, Some(bounds))?;
                while routed
                    .peek()
                    .is_some_and(|(_, routed_segment, _)| *routed_segment == segment_index)
                {
                    let (request_index, _, encoded_key) = routed
                        .next()
                        .expect("peeked routed lookup remains available");
                    output[request_index] = find_loaded_commit_delta_entry(
                        &leaf,
                        &payloads,
                        &encoded_keys[encoded_key],
                        commit_id,
                    )?;
                }
                continue;
            };
            while routed
                .peek()
                .is_some_and(|(_, routed_segment, _)| *routed_segment == segment_index)
            {
                let (request_index, _, encoded_key) = routed
                    .next()
                    .expect("peeked routed lookup remains available");
                output[request_index] = find_loaded_commit_delta_entry(
                    leaf,
                    payloads,
                    &encoded_keys[encoded_key],
                    commit_id,
                )?;
            }
            continue;
        }
        let (leaf, payloads) = decode_commit_delta_with_payloads(&bytes, Some(bounds))?;
        let mut leaf_index = 0usize;
        while routed
            .peek()
            .is_some_and(|(_, routed_segment, _)| *routed_segment == segment_index)
        {
            let (request_index, _, encoded_key) = routed
                .next()
                .expect("peeked routed lookup remains available");
            let encoded_key = &encoded_keys[encoded_key];
            while leaf_index < leaf.len()
                && leaf.key(leaf_index)?.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state packed commit_delta leaf has a missing key",
                    )
                })? < encoded_key
            {
                leaf_index += 1;
            }
            let Some(leaf_key) = leaf.key(leaf_index)? else {
                continue;
            };
            if leaf_key == encoded_key {
                output[request_index] = Some(load_commit_delta_entry_at_index(
                    &leaf, &payloads, leaf_index, commit_id,
                )?);
                leaf_index += 1;
            }
        }
    }
    hydrate_selected_loaded_entries(store, &mut output).await?;
    Ok(output)
}

async fn remember_expanded_point_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &CommitStateManifest,
    point_cache: Option<&CommitDeltaPointReadCache>,
) -> Result<(), LixError> {
    let Some(point_cache) = point_cache else {
        return Ok(());
    };
    let manifest = expanded_commit_delta_manifest_from_commit_state(store, state).await?;
    validate_commit_delta_manifest(&manifest)?;
    point_cache.remember_manifest(state.commit_id, Arc::new(manifest))
}

async fn hydrate_selected_loaded_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    entries: &mut [Option<LoadedCommitDeltaEntry>],
) -> Result<(), LixError> {
    let selected = entries
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| {
            entry
                .as_ref()
                .filter(|entry| entry.selected_ref)
                .map(|entry| (index, entry.change_record.change_id))
        })
        .collect::<Vec<_>>();
    let change_ids = selected
        .iter()
        .map(|(_, change_id)| *change_id)
        .collect::<Vec<_>>();
    let canonical = load_change_records_by_ids(store, &change_ids).await?;
    for ((index, _), change_record) in selected.into_iter().zip(canonical) {
        let entry = entries[index]
            .as_mut()
            .expect("selected entry came from the output batch");
        if entry.change_record.schema_key != change_record.schema_key
            || entry.change_record.file_id != change_record.file_id
            || entry.change_record.entity_pk != change_record.entity_pk
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state selected change '{}' references canonical authority for a different identity",
                    entry.change_record.change_id
                ),
            ));
        }
        entry.change_record.snapshot = change_record.snapshot;
        entry.change_record.metadata = change_record.metadata;
        entry.change_record.origin_key = change_record.origin_key;
        entry.selected_ref = false;
    }
    Ok(())
}

/// Scans only the mutations in one commit that belong to one of the requested
/// schemas. This is the partial-key counterpart to
/// [`load_commit_delta_values`]: it avoids hydrating unrelated changelog
/// changes when a history provider knows the schema but not every identity.
pub(crate) async fn scan_commit_delta_values(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    schema_keys: &[String],
) -> Result<DecodedCommitDeltaBatch, LixError> {
    scan_commit_delta_values_with_cache(store, commit_id, schema_keys, None).await
}

pub(crate) async fn scan_commit_delta_values_with_cache(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    schema_keys: &[String],
    point_cache: Option<&CommitDeltaPointReadCache>,
) -> Result<DecodedCommitDeltaBatch, LixError> {
    let Some(manifest) = load_commit_delta_manifest_cached(store, commit_id, point_cache).await?
    else {
        return Ok(DecodedCommitDeltaBatch::default());
    };
    let Some(source_commit_id) = manifest.selected_source_commit_id() else {
        return scan_local_commit_delta_values(store, commit_id, schema_keys, &manifest).await;
    };
    let source = match load_commit_delta_manifest_cached(store, source_commit_id, point_cache)
        .await?
    {
        Some(source_manifest) => {
            scan_local_commit_delta_values(store, source_commit_id, schema_keys, &source_manifest)
                .await?
        }
        None => DecodedCommitDeltaBatch::default(),
    };
    let local = scan_local_commit_delta_values(store, commit_id, schema_keys, &manifest).await?;
    merge_selected_source_batches(source, local, commit_id)
}

async fn scan_local_commit_delta_values(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    schema_keys: &[String],
    manifest: &CommitDeltaManifest,
) -> Result<DecodedCommitDeltaBatch, LixError> {
    let requested_schemas = schema_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if let Some(inline_segment) = manifest.inline_segment() {
        let leaf = decode_commit_delta_leaf(inline_segment, None)?;
        let mut batch = DecodedCommitDeltaBatchBuilder::with_capacity(leaf.len(), 1);
        batch.push_leaf(leaf, commit_id, &requested_schemas)?;
        return Ok(batch.finish());
    }
    let segment_indices = commit_delta_segments_for_schemas(manifest, &requested_schemas);
    if segment_indices.is_empty() {
        return Ok(DecodedCommitDeltaBatch::default());
    }
    let storage_keys = segment_indices
        .iter()
        .map(|&segment_index| {
            commit_delta_segment_key_for_bounds(
                commit_id,
                segment_index,
                &manifest.segments[segment_index],
            )
            .map(|key| StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let segments = PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &storage_keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    let mut batch = DecodedCommitDeltaBatchBuilder::with_capacity(
        segment_indices
            .len()
            .saturating_mul(COMMIT_DELTA_SEGMENT_MAX_ROWS),
        segment_indices.len(),
    );
    for (segment_index, value) in segment_indices.into_iter().zip(segments.value) {
        let bytes = value
            .and_then(full_value_bytes)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state commit_delta manifest for commit '{commit_id}' references missing segment {segment_index}"
                    ),
                )
            })?;
        let leaf = decode_commit_delta_leaf(&bytes, Some(&manifest.segments[segment_index]))?;
        batch.push_leaf(leaf, commit_id, &requested_schemas)?;
    }
    Ok(batch.finish())
}

fn merge_selected_source_batches(
    mut source: DecodedCommitDeltaBatch,
    mut local: DecodedCommitDeltaBatch,
    commit_id: CommitId,
) -> Result<DecodedCommitDeltaBatch, LixError> {
    let arena_offset = u32::try_from(source.arenas.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "selected-source commit delta has too many arenas",
        )
    })?;
    let schema_offset = u32::try_from(source.schema_keys.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "selected-source commit delta has too many schema keys",
        )
    })?;
    let file_offset = u32::try_from(source.file_ids.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "selected-source commit delta has too many file ids",
        )
    })?;
    let mut entries = BTreeMap::<Vec<u8>, (DecodedCommitDeltaRow, TrackedStateIndexValue)>::new();
    for (row, mut value) in source.rows.drain(..).zip(source.values.drain(..)) {
        let key = source.arenas[row.arena_ordinal as usize]
            .key(row.entry_ordinal as usize)?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "selected-source commit delta references a missing source key",
                )
            })?
            .to_vec();
        value.commit_id = commit_id;
        entries.insert(key, (row, value));
    }
    for (mut row, value) in local.rows.drain(..).zip(local.values.drain(..)) {
        let key = local.arenas[row.arena_ordinal as usize]
            .key(row.entry_ordinal as usize)?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "selected-source commit delta references a missing local key",
                )
            })?
            .to_vec();
        row.arena_ordinal = row.arena_ordinal.checked_add(arena_offset).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "commit delta arena ordinal overflow",
            )
        })?;
        row.schema_key_ordinal = row
            .schema_key_ordinal
            .checked_add(schema_offset)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "commit delta schema ordinal overflow",
                )
            })?;
        if row.file_id_ordinal != u32::MAX {
            row.file_id_ordinal =
                row.file_id_ordinal
                    .checked_add(file_offset)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "commit delta file ordinal overflow",
                        )
                    })?;
        }
        entries.insert(key, (row, value));
    }
    source.arenas.append(&mut local.arenas);
    source.schema_keys.append(&mut local.schema_keys);
    source.file_ids.append(&mut local.file_ids);
    for (_, (row, value)) in entries {
        source.rows.push(row);
        source.values.push(value);
    }
    Ok(source)
}

/// Answers schema membership from the packed commit directory.
///
/// Segmented commits are decided from manifest bounds without reading any
/// payload page. Tiny inline commits inspect at most one bounded leaf.
pub(crate) async fn commit_delta_contains_schema(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    schema_key: &str,
) -> Result<bool, LixError> {
    let Some(manifest) = load_commit_delta_manifest(store, commit_id).await? else {
        return Ok(false);
    };
    if local_commit_delta_contains_schema(store, commit_id, schema_key).await? {
        return Ok(true);
    }
    let Some(source_commit_id) = manifest.selected_source_commit_id() else {
        return Ok(false);
    };
    local_commit_delta_contains_schema(store, source_commit_id, schema_key).await
}

async fn local_commit_delta_contains_schema(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    schema_key: &str,
) -> Result<bool, LixError> {
    let Some(manifest) = load_commit_delta_manifest(store, commit_id).await? else {
        return Ok(false);
    };
    if let Some(inline_segment) = manifest.inline_segment() {
        let leaf = decode_commit_delta_leaf(inline_segment, None)?;
        let mut found = false;
        visit_commit_delta_leaf(&leaf, commit_id, |entry_index, _, _| {
            let key = decode_key_shared(
                leaf.entry_owned(entry_index)
                    .expect("visited commit-delta leaf entry exists")
                    .key,
            )?;
            found |= key.schema_key.as_str() == schema_key;
            Ok(())
        })?;
        return Ok(found);
    }
    let requested = BTreeSet::from([schema_key]);
    Ok(!commit_delta_segments_for_schemas(&manifest, &requested).is_empty())
}

/// Scans every authoritative tracked change packed into immutable commit
/// deltas, deduplicating checkpoint and merge selections by change id.
///
/// `lix_change` is an unscoped durable-fact surface, so this is its packed
/// tracked counterpart to the point-addressed CHANGE_SPACE scan.
pub(crate) async fn scan_change_records_from_commit_deltas(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<Vec<crate::changelog::ChangeRecord>, LixError> {
    let mut records = Vec::new();
    visit_change_records_from_commit_deltas(store, |record| {
        records.push(record);
        Ok(())
    })
    .await?;
    records.sort_unstable_by_key(|record| record.change_id);
    Ok(records)
}

/// Visits canonical packed changes while retaining memory proportional to one
/// storage page plus one logical commit, never total repository history.
///
/// Merge/checkpoint selections carry an explicit non-authored marker. Those
/// uncommon duplicates validate against the immutable locator before being
/// skipped; authored rows require no secondary read.
pub(crate) async fn visit_change_records_from_commit_deltas(
    store: &(impl StorageAdapterRead + ?Sized),
    mut visit: impl FnMut(crate::changelog::ChangeRecord) -> Result<(), LixError>,
) -> Result<usize, LixError> {
    let plan = ScanPlan::range(
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        StorageKeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
    );
    let mut resume_after = None;
    let mut emitted = 0usize;
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    limit_rows: crate::storage_adapter::MAX_SCAN_PAGE_ROWS,
                    resume_after,
                },
            )
            .await?;
        let seal_keys = page
            .value
            .entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<Vec<_>>();
        let seal_request = [StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_STATE_SEAL_SPACE,
            keys: &seal_keys,
            opts: StorageGetOptions::default(),
        }];
        let seals = exact_get_many(store, &seal_request).await?.values;
        for (entry, seal) in page.value.entries.iter().zip(seals) {
            if entry.key.0.len() != 16 {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_delta manifest key is not a 16-byte commit id",
                ));
            }
            let StorageProjectedValue::FullValue(bytes) = &entry.value else {
                unreachable!("full commit-delta scan returned a key-only row");
            };
            let commit_id = commit_id_from_delta_key(&entry.key)?;
            let state = decode_commit_state_manifest(bytes)?;
            if state.commit_id != commit_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit-state scan key and manifest commit disagree",
                ));
            }
            let seal = seal.and_then(full_value_bytes).ok_or_else(|| {
                replacement_payload_error(
                    "commit-state scan found a manifest without its immutable semantic authority",
                )
            })?;
            validate_commit_state_semantic_seal(&state, &seal)?;
            let manifest = expanded_commit_delta_manifest_from_commit_state(store, &state).await?;
            let members =
                load_commit_delta_members_from_manifest(store, commit_id, &manifest, &[]).await?;
            for member in members {
                if member.authored {
                    visit(member.change)?;
                    emitted += 1;
                } else if is_payload_free_selected_tombstone(&member) {
                    // Cascade tombstones preserve identity history but do not
                    // introduce another public changelog fact.
                    continue;
                } else {
                    let locator = load_canonical_change_locator(store, member.change.change_id)
                        .await?
                        .ok_or_else(|| {
                            invalid_change_locator(
                                member.change.change_id,
                                "does not resolve to a canonical record",
                            )
                        })?;
                    if locator.commit_id == member.value.commit_id
                        && locator.segment_index == member.segment_index
                        && u32::from(locator.ordinal) == member.ordinal
                    {
                        visit(member.change)?;
                        emitted += 1;
                        continue;
                    }
                    let canonical = load_change_record_at_locator(store, locator).await?;
                    if canonical != member.change {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "tracked_state change '{}' has conflicting authoritative packed payloads",
                                member.change.change_id
                            ),
                        ));
                    }
                }
            }
        }
        if !page.value.has_more {
            break;
        }
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
    }
    validate_no_orphan_commit_delta_segments(store).await?;
    Ok(emitted)
}

async fn validate_no_orphan_commit_delta_segments(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<(), LixError> {
    let plan = ScanPlan::range(
        TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
        StorageKeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
    );
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    projection: StorageCoreProjection::KeyOnly,
                    limit_rows: crate::storage_adapter::MAX_SCAN_PAGE_ROWS,
                    resume_after,
                },
            )
            .await?;
        if page.value.entries.is_empty() {
            break;
        }
        let mut commit_ids = Vec::new();
        for entry in &page.value.entries {
            if entry.key.0.len() != 20 && entry.key.0.len() != 52 {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_delta segment key is not a physical segment address",
                ));
            }
            let commit_id = commit_id_from_delta_key(&entry.key)?;
            if commit_ids.last() != Some(&commit_id) {
                commit_ids.push(commit_id);
            }
        }
        let manifests = load_commit_delta_manifests(store, &commit_ids).await?;
        let manifests = commit_ids
            .into_iter()
            .zip(manifests)
            .collect::<BTreeMap<_, _>>();
        for entry in &page.value.entries {
            let commit_id = commit_id_from_delta_key(&entry.key)?;
            let segment_index = usize::try_from(u32::from_be_bytes(
                entry.key.0[16..20]
                    .try_into()
                    .expect("segment suffix length checked"),
            ))
            .expect("u32 fits usize");
            let Some(Some(manifest)) = manifests.get(&commit_id) else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state commit_delta inventory found orphan segments for commit '{commit_id}'"
                    ),
                ));
            };
            if manifest.inline_segment().is_some() || segment_index >= manifest.segments.len() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state commit_delta for commit '{commit_id}' has an undeclared segment {segment_index}"
                    ),
                ));
            }
            if entry.key.0.as_ref()
                != commit_delta_segment_key_for_bounds(
                    commit_id,
                    segment_index,
                    &manifest.segments[segment_index],
                )?
                .as_slice()
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_delta physical segment key does not match its manifest",
                ));
            }
        }
        if !page.value.has_more {
            break;
        }
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
    }
    Ok(())
}

/// Inventories the complete packed commit-delta plane in one manifest scan and
/// one segment scan. This is the repository-GC correctness boundary: no
/// manifest may reference a missing/extra segment and no segment may exist
/// without its manifest.
pub(crate) async fn scan_commit_delta_inventory(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<CommitDeltaInventory, LixError> {
    let CommitDeltaPlane {
        manifests,
        mut authorities,
        mut segments,
        mut segment_keys,
    } = scan_commit_delta_plane(store).await?;
    let mut inventory = CommitDeltaInventory::default();
    for (&commit_id, manifest) in &manifests {
        let physical_segments = segments.remove(&commit_id).unwrap_or_default();
        let physical_segment_keys = segment_keys.remove(&commit_id).unwrap_or_default();
        let segment_count = manifest.segments.len();
        let mut members = Vec::new();
        if let Some(inline_segment) = manifest.inline_segment() {
            if !physical_segments.is_empty() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state inline commit_delta for commit '{commit_id}' has external segments"
                    ),
                ));
            }
            collect_strict_commit_delta_members(inline_segment, None, commit_id, 0, &mut members)?;
        } else {
            validate_physical_commit_delta_segments(
                commit_id,
                &manifest,
                &physical_segments,
                &physical_segment_keys,
            )?;
            for (segment_index, bounds) in manifest.segments.iter().enumerate() {
                collect_strict_commit_delta_members(
                    &physical_segments[&segment_index],
                    Some(bounds),
                    commit_id,
                    u32::try_from(segment_index).expect("segment index fits u32"),
                    &mut members,
                )?;
            }
        }
        hydrate_selected_members(store, &mut members).await?;
        validate_commit_delta_member_order_and_ids(commit_id, &members)?;
        inventory.commits.insert(
            commit_id,
            CommitDeltaInventoryEntry {
                members,
                segment_count,
                physical_segment_keys: manifest
                    .segments
                    .iter()
                    .enumerate()
                    .map(|(segment_index, bounds)| {
                        commit_delta_segment_key_for_bounds(commit_id, segment_index, bounds)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                selected_source_commit_id: manifest.selected_source_commit_id(),
                authority: authorities
                    .remove(&commit_id)
                    .expect("every decoded mutation manifest has topology authority"),
            },
        );
    }
    for (&commit_id, manifest) in &manifests {
        let Some(source_commit_id) = manifest.selected_source_commit_id() else {
            continue;
        };
        if manifests
            .get(&source_commit_id)
            .is_some_and(|source| source.selected_source_commit_id().is_some())
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "selected-source commit delta chains are unsupported",
            ));
        }
        let mut selected = inventory
            .commits
            .get(&source_commit_id)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "selected-source commit delta '{commit_id}' references missing source '{source_commit_id}'"
                    ),
                )
            })?
            .members
            .clone();
        for member in &mut selected {
            member.value.commit_id = commit_id;
            member.authored = false;
            member.selected_tombstone = member.value.deleted;
        }
        let local = inventory
            .commits
            .get_mut(&commit_id)
            .expect("alias manifest was inventoried locally");
        selected.append(&mut local.members);
        selected.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        if selected.windows(2).any(|pair| pair[0].key == pair[1].key) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("selected-source commit delta '{commit_id}' has overlapping local rows"),
            ));
        }
        local.members = selected;
    }
    let mut authoritative_changes =
        BTreeMap::<crate::changelog::ChangeId, crate::changelog::ChangeRecord>::new();
    for entry in inventory.commits.values() {
        for member in &entry.members {
            if is_payload_free_selected_tombstone(member) {
                continue;
            }
            if let Some(existing) =
                authoritative_changes.insert(member.change.change_id, member.change.clone())
                && existing != member.change
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state change '{}' has conflicting authoritative packed payloads",
                        member.change.change_id
                    ),
                ));
            }
        }
    }
    debug_assert!(segments.is_empty());
    debug_assert!(segment_keys.is_empty());
    debug_assert!(authorities.is_empty());
    Ok(inventory)
}

async fn scan_commit_delta_plane(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<CommitDeltaPlane, LixError> {
    let commit_state_rows =
        scan_full_space(store, TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE).await?;
    let commit_state_seals = scan_full_space(store, TRACKED_STATE_COMMIT_STATE_SEAL_SPACE).await?;
    let segment_rows = scan_full_space(store, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE).await?;

    if commit_state_rows.len() != commit_state_seals.len()
        || commit_state_rows
            .iter()
            .zip(&commit_state_seals)
            .any(|((manifest_key, _), (seal_key, _))| manifest_key != seal_key)
    {
        return Err(replacement_payload_error(
            "commit-state inventory found an orphan manifest or semantic authority",
        ));
    }

    let mut manifests = BTreeMap::<CommitId, CommitDeltaManifest>::new();
    let mut authorities = BTreeMap::<CommitId, CommitStateTopologyProjection>::new();
    for ((key, bytes), (_, seal)) in commit_state_rows.into_iter().zip(commit_state_seals) {
        if key.0.len() != 16 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_state_manifest key is not a 16-byte commit id",
            ));
        }
        let commit_id = commit_id_from_delta_key(&key)?;
        let manifest = decode_commit_state_manifest(&bytes)?;
        if manifest.commit_id != commit_id || manifests.contains_key(&commit_id) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit-state inventory found duplicate or mismatched manifest for commit '{commit_id}'"
                ),
            ));
        }
        validate_commit_state_semantic_seal(&manifest, &seal)?;
        authorities.insert(
            commit_id,
            CommitStateTopologyProjection {
                generation: manifest.generation,
                parent_commit_ids: manifest.parent_commit_ids.clone(),
                commit_change_id: manifest.commit_change_id,
                author_account_ids: manifest.author_account_ids.clone(),
                created_at: manifest.created_at,
                replay_debt: manifest.replay_debt,
            },
        );
        manifests.insert(
            commit_id,
            expanded_commit_delta_manifest_from_commit_state(store, &manifest).await?,
        );
    }

    let mut segments = BTreeMap::<CommitId, BTreeMap<usize, Bytes>>::new();
    let mut segment_keys = BTreeMap::<CommitId, BTreeMap<usize, Bytes>>::new();
    for (key, bytes) in segment_rows {
        if key.0.len() != 20 && key.0.len() != 52 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta segment key is not a physical segment address",
            ));
        }
        let commit_id = commit_id_from_delta_key(&key)?;
        let segment_index = usize::try_from(u32::from_be_bytes(
            key.0[16..20]
                .try_into()
                .expect("commit-delta segment suffix length checked"),
        ))
        .expect("u32 fits usize");
        segment_keys
            .entry(commit_id)
            .or_default()
            .insert(segment_index, key.0.clone());
        if segments
            .entry(commit_id)
            .or_default()
            .insert(segment_index, bytes)
            .is_some()
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit_delta inventory found duplicate segment {segment_index} for commit '{commit_id}'"
                ),
            ));
        }
    }

    if let Some(commit_id) = segments
        .keys()
        .find(|commit_id| !manifests.contains_key(commit_id))
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_delta inventory found orphan segments for commit '{commit_id}'"
            ),
        ));
    }

    Ok(CommitDeltaPlane {
        manifests,
        authorities,
        segments,
        segment_keys,
    })
}

fn validate_physical_commit_delta_segments(
    commit_id: CommitId,
    manifest: &CommitDeltaManifest,
    physical_segments: &BTreeMap<usize, Bytes>,
    physical_segment_keys: &BTreeMap<usize, Bytes>,
) -> Result<(), LixError> {
    if physical_segments.len() != manifest.segments.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_delta for commit '{commit_id}' has {} physical segments but its manifest declares {}",
                physical_segments.len(),
                manifest.segments.len(),
            ),
        ));
    }
    if let Some(segment_index) =
        (0..manifest.segments.len()).find(|index| !physical_segments.contains_key(index))
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_delta for commit '{commit_id}' is missing segment {segment_index}"
            ),
        ));
    }
    for (segment_index, bounds) in manifest.segments.iter().enumerate() {
        if physical_segment_keys.get(&segment_index).is_none_or(|key| {
            match commit_delta_segment_key_for_bounds(commit_id, segment_index, bounds) {
                Ok(expected) => key.as_ref() != expected.as_slice(),
                Err(_) => true,
            }
        }) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta physical segment key does not match its manifest",
            ));
        }
    }
    Ok(())
}

pub(crate) fn stage_delete_commit_delta_inventory_entry(
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
    entry: &CommitDeltaInventoryEntry,
) -> Result<(), LixError> {
    writes.delete(
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        key(commit_state_manifest_key(commit_id)),
    );
    writes.delete(
        TRACKED_STATE_COMMIT_STATE_SEAL_SPACE,
        key(commit_state_manifest_key(commit_id)),
    );
    for segment_key in &entry.physical_segment_keys {
        writes.delete(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            key(segment_key.clone()),
        );
    }
    Ok(())
}

async fn scan_full_space(
    store: &(impl StorageAdapterRead + ?Sized),
    space: StorageSpace,
) -> Result<Vec<(StorageKey, Bytes)>, LixError> {
    let plan = ScanPlan::range(
        space,
        StorageKeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
    );
    let mut resume_after = None;
    let mut rows = Vec::new();
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    limit_rows: crate::storage_adapter::MAX_SCAN_PAGE_ROWS,
                    resume_after,
                },
            )
            .await?;
        for entry in &page.value.entries {
            let StorageProjectedValue::FullValue(bytes) = &entry.value else {
                unreachable!("full commit-delta scan returned a key-only row");
            };
            rows.push((entry.key.clone(), bytes.clone()));
        }
        if !page.value.has_more {
            break;
        }
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
    }
    Ok(rows)
}

fn commit_id_from_delta_key(key: &StorageKey) -> Result<CommitId, LixError> {
    let bytes = key.0.as_ref();
    let commit_bytes = bytes.get(..16).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta key is shorter than a commit id",
        )
    })?;
    Ok(CommitId::new(
        uuid::Uuid::from_slice(commit_bytes).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("tracked_state commit_delta key has invalid commit id: {error}"),
            )
        })?,
    ))
}

fn collect_strict_commit_delta_members(
    bytes: &[u8],
    expected_bounds: Option<&CommitDeltaSegmentBounds>,
    expected_commit_id: CommitId,
    segment_index: u32,
    members: &mut Vec<CommitDeltaMember>,
) -> Result<(), LixError> {
    let (leaf, payloads) = decode_commit_delta_with_payloads(bytes, expected_bounds)?;
    visit_commit_delta_leaf(&leaf, expected_commit_id, |_, _, _| Ok(()))?;
    for entry_index in 0..leaf.len() {
        let entry = leaf.entry(entry_index)?.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state packed commit_delta leaf has a missing entry",
            )
        })?;
        let value = decode_value(entry.value)?;
        if value.commit_id != expected_commit_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state packed commit_delta for commit '{expected_commit_id}' contains an entry for commit '{}'",
                    value.commit_id
                ),
            ));
        }
        let payload = payloads.decode(entry_index)?;
        let key = decode_key(entry.key)?;
        let (snapshot, metadata, origin_key, base_coordinate, authored, selected_tombstone) =
            match payload {
                CommitDeltaPayload::Authored(payload) => (
                    payload.snapshot,
                    payload.metadata,
                    payload.origin_key,
                    payload.base_coordinate,
                    true,
                    false,
                ),
                CommitDeltaPayload::SelectedRef(base_coordinate) => (
                    crate::json_store::JsonSlot::None,
                    crate::json_store::JsonSlot::None,
                    None,
                    base_coordinate,
                    false,
                    false,
                ),
                CommitDeltaPayload::SelectedTombstone(base_coordinate) => (
                    crate::json_store::JsonSlot::None,
                    crate::json_store::JsonSlot::None,
                    None,
                    base_coordinate,
                    false,
                    true,
                ),
            };
        let change = crate::changelog::ChangeRecord {
            format_version: 2,
            change_id: value.change_id,
            schema_key: key.schema_key.clone(),
            entity_pk: key.entity_pk.clone(),
            file_id: key.file_id.clone(),
            snapshot,
            metadata,
            created_at: value.updated_at,
            origin_key,
        };
        members.push(CommitDeltaMember {
            key,
            value,
            change,
            segment_index,
            ordinal: u32::try_from(entry_index).expect("segment ordinal fits u32"),
            authored,
            base_coordinate,
            selected_tombstone,
        });
    }
    Ok(())
}

fn validate_commit_delta_member_order_and_ids(
    commit_id: CommitId,
    members: &[CommitDeltaMember],
) -> Result<(), LixError> {
    if members.windows(2).any(|pair| pair[0].key >= pair[1].key) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_delta for commit '{commit_id}' is not strictly ordered across segments"
            ),
        ));
    }
    let mut change_ids = BTreeMap::new();
    for member in members {
        if let Some(previous_is_selected_tombstone) = change_ids.insert(
            member.value.change_id,
            is_payload_free_selected_tombstone(member),
        ) && !(previous_is_selected_tombstone && is_payload_free_selected_tombstone(member))
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit_delta for commit '{commit_id}' contains duplicate change id '{}'",
                    member.value.change_id
                ),
            ));
        }
    }
    Ok(())
}

fn is_payload_free_selected_tombstone(member: &CommitDeltaMember) -> bool {
    member.selected_tombstone
}

fn selection_fingerprint<'a>(
    members: impl IntoIterator<
        Item = (
            &'a [u8],
            crate::changelog::ChangeId,
            bool,
            crate::common::LixTimestamp,
            crate::common::LixTimestamp,
        ),
    >,
) -> [u8; 32] {
    let mut fingerprint = [0_u8; 32];
    for (key, change_id, deleted, created_at, updated_at) in members {
        let mut member = blake3::Hasher::new();
        member.update(b"lix.commit_delta.selection.v2");
        member.update(&(key.len() as u64).to_be_bytes());
        member.update(key);
        member.update(change_id.as_uuid().as_bytes());
        member.update(&[u8::from(deleted)]);
        member.update(&created_at.packed().to_be_bytes());
        member.update(&updated_at.packed().to_be_bytes());
        for (target, source) in fingerprint.iter_mut().zip(member.finalize().as_bytes()) {
            *target ^= source;
        }
    }
    fingerprint
}

pub(crate) fn selected_change_selection_fingerprint<'a>(
    members: impl IntoIterator<
        Item = (
            &'a [u8],
            crate::changelog::ChangeId,
            bool,
            crate::common::LixTimestamp,
            crate::common::LixTimestamp,
        ),
    >,
) -> [u8; 32] {
    selection_fingerprint(members)
}

pub(crate) async fn load_commit_delta_selection_certificate(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<CommitDeltaSelectionCertificate>, LixError> {
    Ok(load_commit_state_manifest(store, commit_id)
        .await?
        .map(|manifest| CommitDeltaSelectionCertificate {
            member_count: manifest.mutations.member_count,
            selection_fingerprint: manifest.mutations.selection_fingerprint,
            selected_source_commit_id: manifest.mutations.selected_source_commit_id(),
            direct_segment_row_counts: manifest.mutations.direct_part_row_counts,
        }))
}

pub(crate) async fn load_commit_delta_replay_metadata(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<CommitDeltaReplayMetadata>, LixError> {
    load_commit_delta_replay_metadata_with_cache(store, commit_id, None).await
}

pub(crate) async fn load_commit_delta_replay_metadata_with_cache(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    point_cache: Option<&CommitDeltaPointReadCache>,
) -> Result<Option<CommitDeltaReplayMetadata>, LixError> {
    if let Some(manifest) = point_cache
        .map(|cache| cache.manifest(commit_id))
        .transpose()?
        .flatten()
    {
        return Ok(Some(commit_delta_replay_metadata(&manifest)));
    }
    Ok(load_replay_commit_state_manifest(store, commit_id)
        .await?
        .map(|manifest| commit_delta_replay_metadata_from_inventory(&manifest.mutations)))
}

/// Seeds the snapshot-local delta cache from the seal-verified replay manifest
/// already read by point traversal, avoiding a second authority read.
pub(crate) fn seed_commit_delta_point_cache_from_replay_manifest(
    state: &CommitStateManifest,
    point_cache: &CommitDeltaPointReadCache,
) -> Result<CommitDeltaReplayMetadata, LixError> {
    if let Some(manifest) = point_cache.manifest(state.commit_id)? {
        return Ok(commit_delta_replay_metadata(&manifest));
    }
    let metadata = commit_delta_replay_metadata_from_inventory(&state.mutations);
    if state.mutations.replacement_part_digests.is_empty() {
        point_cache.remember_manifest(
            state.commit_id,
            Arc::new(commit_delta_manifest_from_commit_state(state)),
        )?;
    }
    Ok(metadata)
}

fn commit_delta_replay_metadata(manifest: &CommitDeltaManifest) -> CommitDeltaReplayMetadata {
    let lifecycle_summary = manifest.lifecycle_summary.clone();
    CommitDeltaReplayMetadata {
        member_count: manifest.member_count,
        single_partition: manifest.single_partition.clone(),
        lifecycle_summary: lifecycle_summary.clone(),
        replacement_generation: manifest.replacement_generation.as_ref().map(|generation| {
            CommitDeltaReplacementGeneration {
                scope: generation.scope.clone(),
                fallback_commit_id: generation
                    .fallback_commit_id
                    .map(|bytes| CommitId::new(uuid::Uuid::from_bytes(bytes))),
                lifecycle_summary: lifecycle_summary
                    .expect("validated replacement generation has lifecycle metadata"),
            }
        }),
    }
}

fn commit_delta_replay_metadata_from_inventory(
    inventory: &CommitStateMutationInventory,
) -> CommitDeltaReplayMetadata {
    let lifecycle_summary = inventory.lifecycle_summary.clone();
    CommitDeltaReplayMetadata {
        member_count: inventory.member_count,
        single_partition: inventory.single_partition.clone(),
        lifecycle_summary: lifecycle_summary.clone(),
        replacement_generation: inventory
            .replacement_generation
            .as_ref()
            .zip(lifecycle_summary)
            .map(
                |(generation, lifecycle_summary)| CommitDeltaReplacementGeneration {
                    scope: generation.scope.clone(),
                    fallback_commit_id: generation
                        .fallback_commit_id
                        .map(|bytes| CommitId::new(uuid::Uuid::from_bytes(bytes))),
                    lifecycle_summary,
                },
            ),
    }
}
async fn load_commit_delta_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<CommitDeltaManifest>, LixError> {
    let Some(state) = load_point_replay_commit_state(store, commit_id).await? else {
        return Ok(None);
    };
    let manifest = expanded_commit_delta_manifest_from_commit_state(store, &state).await?;
    validate_commit_delta_manifest(&manifest)?;
    if manifest
        .replacement_generation
        .as_ref()
        .is_some_and(|generation| generation.owner_commit_id != *commit_id.as_uuid().as_bytes())
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("tracked_state replacement generation does not belong to commit '{commit_id}'"),
        ));
    }
    Ok(Some(manifest))
}

async fn load_commit_delta_manifest_cached(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    point_cache: Option<&CommitDeltaPointReadCache>,
) -> Result<Option<Arc<CommitDeltaManifest>>, LixError> {
    if let Some(manifest) = point_cache
        .map(|cache| cache.manifest(commit_id))
        .transpose()?
        .flatten()
    {
        return Ok(Some(manifest));
    }
    let Some(manifest) = load_commit_delta_manifest(store, commit_id).await? else {
        return Ok(None);
    };
    let manifest = Arc::new(manifest);
    if let Some(point_cache) = point_cache {
        point_cache.remember_manifest(commit_id, Arc::clone(&manifest))?;
    }
    Ok(Some(manifest))
}

fn validate_commit_delta_manifest(manifest: &CommitDeltaManifest) -> Result<(), LixError> {
    // Every protocol commit has a commit-state manifest, including commits
    // whose only contribution is topology. Its canonical empty mutation
    // inventory is a valid delta with no physical segment to read.
    if manifest.member_count == 0
        && manifest.selected_source_commit_id.is_none()
        && manifest.direct_segment_row_counts.is_empty()
        && manifest.single_partition.is_none()
        && manifest.lifecycle_summary.is_none()
        && manifest.replacement_generation.is_none()
        && manifest.replacement_parts.is_none()
        && manifest.inline_segment.is_empty()
        && manifest.segments.is_empty()
    {
        return Ok(());
    }
    if manifest
        .single_partition
        .as_ref()
        .is_some_and(|scope| scope.schema_key.is_empty())
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta manifest has an invalid single-partition certificate",
        ));
    }
    if let Some(summary) = manifest.lifecycle_summary.as_ref()
        && (summary.scope.schema_key.is_empty()
            || manifest.single_partition.as_ref() != Some(&summary.scope)
            || manifest.member_count == 0
            || manifest.selected_source_commit_id.is_some()
            || manifest.direct_segment_row_counts.is_empty())
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta manifest has invalid lifecycle metadata",
        ));
    }
    if let Some(generation) = manifest.replacement_generation.as_ref()
        // Replacement authority is independent of whether its sole compact
        // mutation part is inline or addressed through the external directory.
        && (generation.scope.schema_key.is_empty()
            || generation.owner_commit_id == [0; 16]
            || manifest.single_partition.as_ref() != Some(&generation.scope)
            || manifest
                .lifecycle_summary
                .as_ref()
                .map(|summary| &summary.scope)
                != Some(&generation.scope)
            || manifest.member_count == 0
            || manifest.selected_source_commit_id.is_some()
            || manifest.direct_segment_row_counts.is_empty()
            || !manifest.inline_segment.is_empty()
            || manifest.replacement_parts.is_none()
            || manifest.lifecycle_summary.as_ref().is_none_or(|summary| {
                generation.integrity_digest
                    != replacement_generation_integrity_digest(
                        generation,
                        summary,
                        manifest
                            .replacement_parts
                            .as_ref()
                            .expect("replacement generation requires immutable parts"),
                    )
            }))
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta manifest has an invalid replacement generation",
        ));
    }
    match manifest.replacement_parts.as_ref() {
        Some(authority) => {
            if manifest.replacement_generation.is_none()
                || manifest.inline_segment().is_some()
                || manifest.segments.is_empty()
                || manifest
                    .segments
                    .iter()
                    .any(|bounds| bounds.replacement_part.is_none())
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state replacement-part authority has an invalid manifest shape",
                ));
            }
            let directory = replacement_directory_from_manifest(manifest)?;
            if directory.digest()? != authority.directory_digest {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state replacement-part directory digest mismatch",
                ));
            }
            let generation = manifest
                .replacement_generation
                .as_ref()
                .expect("replacement-part authority requires a generation");
            if manifest.segments.iter().enumerate().any(|(index, bounds)| {
                bounds.replacement_part.as_ref().is_none_or(|part| {
                    part.owner_commit_id != generation.owner_commit_id
                        || part.uniform_created_at
                            != manifest
                                .lifecycle_summary
                                .as_ref()
                                .expect("replacement generation has lifecycle summary")
                                .uniform_created_at
                        || part.first_address
                            != u32::try_from(index)
                                .expect("replacement part index fits u32")
                                .saturating_mul(
                                    u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS)
                                        .expect("replacement row bound fits u32"),
                                )
                        || part.uniform_updated_at != authority.uniform_updated_at
                })
            }) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state replacement-part authority does not match its owner generation",
                ));
            }
        }
        None => {
            if manifest
                .segments
                .iter()
                .any(|bounds| bounds.replacement_part.is_some())
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_delta has unauthorised replacement parts",
                ));
            }
        }
    }
    if !manifest.direct_segment_row_counts.is_empty() {
        if manifest.selected_source_commit_id.is_some()
            || manifest
                .direct_segment_row_counts
                .iter()
                .any(|&count| count == 0 || usize::from(count) > COMMIT_DELTA_SEGMENT_MAX_ROWS)
            || manifest
                .direct_segment_row_counts
                .iter()
                .map(|&count| u64::from(count))
                .sum::<u64>()
                != u64::from(manifest.member_count)
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta manifest has an invalid dense address inventory",
            ));
        }
        let expected_segment_count = if manifest.inline_segment.is_empty() {
            manifest.segments.len()
        } else {
            1
        };
        if manifest.direct_segment_row_counts.len() != expected_segment_count {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta dense address inventory does not match its segments",
            ));
        }
    }
    if !manifest.inline_segment.is_empty() {
        if !manifest.segments.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta manifest mixes inline and indexed segments",
            ));
        }
        let leaf = decode_commit_delta_leaf(&manifest.inline_segment, None)?;
        let actual_single_partition = match (leaf.first_key(), leaf.last_key()) {
            (Some(first), Some(last)) => single_partition_from_bounds(first, last)?,
            (None, None) => None,
            _ => unreachable!("a decoded leaf has both first and last keys or neither"),
        };
        if manifest.single_partition != actual_single_partition {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state inline commit_delta partition certificate does not match its rows",
            ));
        }
        return Ok(());
    }
    if manifest.segments.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta manifest has no segments",
        ));
    }
    let mut previous_last: Option<&[u8]> = None;
    for bounds in &manifest.segments {
        if bounds.first_key.is_empty() || bounds.last_key.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta manifest has an empty segment bound",
            ));
        }
        if bounds.first_key > bounds.last_key {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta manifest has inverted segment bounds",
            ));
        }
        if previous_last.is_some_and(|previous_last| previous_last >= bounds.first_key.as_slice()) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta manifest has overlapping segment bounds",
            ));
        }
        previous_last = Some(&bounds.last_key);
    }
    let actual_single_partition = single_partition_from_bounds(
        &manifest
            .segments
            .first()
            .expect("non-empty manifest segments have first bounds")
            .first_key,
        &manifest
            .segments
            .last()
            .expect("non-empty manifest segments have last bounds")
            .last_key,
    )?;
    if manifest.single_partition != actual_single_partition {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta single-partition certificate does not match segment bounds",
        ));
    }
    Ok(())
}

fn replacement_directory_from_manifest(
    manifest: &CommitDeltaManifest,
) -> Result<crate::tracked_state::replacement_part::ReplacementPartDirectory, LixError> {
    let mut first_ordinal = 0u32;
    let entries = manifest
        .segments
        .iter()
        .zip(&manifest.direct_segment_row_counts)
        .map(|(bounds, &row_count)| {
            let replacement = bounds.replacement_part.as_ref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state replacement directory contains a generic segment",
                )
            })?;
            let entry = crate::tracked_state::replacement_part::ReplacementPartDirectoryEntry::new(
                replacement.content_digest,
                &bounds.first_key,
                &bounds.last_key,
                first_ordinal,
                row_count,
            );
            first_ordinal = first_ordinal
                .checked_add(u32::from(row_count))
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state replacement directory ordinal overflows",
                    )
                })?;
            Ok(entry)
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    crate::tracked_state::replacement_part::ReplacementPartDirectory::try_new(
        entries,
        manifest.member_count,
    )
}

impl CommitDeltaManifest {
    fn selected_source_commit_id(&self) -> Option<CommitId> {
        self.selected_source_commit_id
            .map(|bytes| CommitId::new(uuid::Uuid::from_bytes(bytes)))
    }

    fn inline_segment(&self) -> Option<&[u8]> {
        (!self.inline_segment.is_empty()).then_some(self.inline_segment.as_slice())
    }
}

fn commit_delta_segment_for_key(manifest: &CommitDeltaManifest, key: &[u8]) -> Option<usize> {
    let mut lower = 0usize;
    let mut upper = manifest.segments.len();
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        if manifest.segments[middle].first_key.as_slice() <= key {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    let segment_index = lower.checked_sub(1)?;
    (key <= manifest.segments[segment_index].last_key.as_slice()).then_some(segment_index)
}

fn encoded_commit_delta_lookup_key(key: &TrackedStateKey) -> Vec<u8> {
    encode_key_ref(TrackedStateKeyRef {
        schema_key: &key.schema_key,
        file_id: key.file_id.as_deref(),
        entity_pk: &key.entity_pk,
    })
}

fn commit_delta_segments_for_schemas(
    manifest: &CommitDeltaManifest,
    schema_keys: &BTreeSet<&str>,
) -> Vec<usize> {
    if schema_keys.is_empty() {
        return (0..manifest.segments.len()).collect();
    }
    manifest
        .segments
        .iter()
        .enumerate()
        .filter_map(|(segment_index, bounds)| {
            schema_keys
                .iter()
                .copied()
                .any(|schema_key| commit_delta_segment_overlaps_schema(bounds, schema_key))
                .then_some(segment_index)
        })
        .collect()
}

fn commit_delta_segment_count_for_schemas_up_to(
    manifest: &CommitDeltaManifest,
    schema_keys: &BTreeSet<&str>,
    limit: usize,
) -> usize {
    if schema_keys.is_empty() {
        return manifest.segments.len().min(limit.saturating_add(1));
    }
    manifest
        .segments
        .iter()
        .filter(|bounds| {
            schema_keys
                .iter()
                .copied()
                .any(|schema_key| commit_delta_segment_overlaps_schema(bounds, schema_key))
        })
        .take(limit.saturating_add(1))
        .count()
}

fn commit_delta_segment_overlaps_schema(
    bounds: &CommitDeltaSegmentBounds,
    schema_key: &str,
) -> bool {
    let schema_prefix = encode_schema_key_prefix(schema_key);
    let Some(schema_end) = prefix_successor(&schema_prefix) else {
        return true;
    };
    bounds.last_key.as_slice() >= schema_prefix.as_slice()
        && bounds.first_key.as_slice() < schema_end.as_slice()
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    while let Some(last) = successor.last_mut() {
        if *last != u8::MAX {
            *last += 1;
            return Some(successor);
        }
        successor.pop();
    }
    None
}

#[cfg(test)]
fn encode_commit_delta_segment(entries: &[EncodedLeafEntry]) -> Vec<u8> {
    let payloads = vec![
        CommitDeltaPayloadRef {
            snapshot: crate::json_store::JsonSlotRef::None,
            metadata: crate::json_store::JsonSlotRef::None,
            origin_key: None,
            base_coordinate: None,
            authored: true,
        };
        entries.len()
    ];
    encode_commit_delta_segment_with_payloads(entries, &payloads)
}

fn try_encode_commit_delta_segment_with_payloads(
    entries: &[EncodedLeafEntry],
    payloads: &[CommitDeltaPayloadRef<'_>],
    compressor: &mut Option<crate::compression::ZstdLevel1Compressor>,
) -> Result<Vec<u8>, CommitDeltaSegmentEncodeError> {
    let entries = entries
        .iter()
        .map(EncodedLeafEntry::as_ref)
        .collect::<Vec<_>>();
    try_encode_commit_delta_segment_with_payload_refs(&entries, payloads, compressor)
}

fn try_encode_commit_delta_segment_with_payload_refs(
    entries: &[EncodedLeafEntryRef<'_>],
    payloads: &[CommitDeltaPayloadRef<'_>],
    compressor: &mut Option<crate::compression::ZstdLevel1Compressor>,
) -> Result<Vec<u8>, CommitDeltaSegmentEncodeError> {
    encode_commit_delta_segment_layout(entries, payloads, Some(compressor))
}

fn encode_commit_delta_segment_layout(
    entries: &[EncodedLeafEntryRef<'_>],
    payloads: &[CommitDeltaPayloadRef<'_>],
    compressor: Option<&mut Option<crate::compression::ZstdLevel1Compressor>>,
) -> Result<Vec<u8>, CommitDeltaSegmentEncodeError> {
    debug_assert_eq!(entries.len(), payloads.len());
    let leaf = encode_leaf_node_refs(entries);
    let authored_inline = payloads.iter().all(|payload| {
        payload.authored
            && matches!(payload.snapshot, crate::json_store::JsonSlotRef::Inline(_))
            && matches!(payload.metadata, crate::json_store::JsonSlotRef::None)
            && payload.origin_key.is_none()
            && payload.base_coordinate.is_none()
    });
    if authored_inline {
        let entry_count = u32::try_from(entries.len()).expect("commit-delta entry count fits u32");
        let directory_bytes = (payloads.len() + 1)
            .checked_mul(COMMIT_DELTA_PAYLOAD_OFFSET_BYTES)
            .ok_or(CommitDeltaSegmentEncodeError::SidecarTooLarge)?;
        let payload_bytes = payloads.iter().try_fold(0usize, |total, payload| {
            let crate::json_store::JsonSlotRef::Inline(json) = payload.snapshot else {
                unreachable!("authored inline sidecar shape was checked")
            };
            total
                .checked_add(json.len())
                .ok_or(CommitDeltaSegmentEncodeError::SidecarTooLarge)
        })?;
        let sidecar_len = 4usize
            .checked_add(directory_bytes)
            .and_then(|len| len.checked_add(payload_bytes))
            .ok_or(CommitDeltaSegmentEncodeError::SidecarTooLarge)?;
        if sidecar_len > COMMIT_DELTA_MAX_SIDECAR_BYTES {
            return Err(CommitDeltaSegmentEncodeError::SidecarTooLarge);
        }
        let mut sidecar = Vec::with_capacity(sidecar_len);
        sidecar.extend_from_slice(&entry_count.to_be_bytes());
        let mut offset = 0usize;
        for payload in payloads {
            sidecar.extend_from_slice(
                &u32::try_from(offset)
                    .expect("commit-delta payload sidecar fits u32")
                    .to_be_bytes(),
            );
            let crate::json_store::JsonSlotRef::Inline(json) = payload.snapshot else {
                unreachable!("authored inline sidecar shape was checked")
            };
            offset += json.len();
        }
        sidecar.extend_from_slice(
            &u32::try_from(offset)
                .expect("commit-delta payload sidecar fits u32")
                .to_be_bytes(),
        );
        for payload in payloads {
            let crate::json_store::JsonSlotRef::Inline(json) = payload.snapshot else {
                unreachable!("authored inline sidecar shape was checked")
            };
            sidecar.extend_from_slice(json.as_bytes());
        }
        return finish_commit_delta_segment_with_sidecar(
            leaf,
            sidecar,
            compressor,
            COMMIT_DELTA_SIDECAR_AUTHORED_INLINE_RAW,
            COMMIT_DELTA_SIDECAR_AUTHORED_INLINE_ZSTD,
        );
    }
    let mut payload_offsets = Vec::with_capacity(payloads.len() + 1);
    let mut payload_bytes = Vec::new();
    for (entry, payload) in entries.iter().zip(payloads) {
        payload_offsets.push(
            u32::try_from(payload_bytes.len()).expect("commit-delta payload sidecar fits u32"),
        );
        if payload.authored {
            payload_bytes.push(COMMIT_DELTA_PAYLOAD_AUTHORED);
            let authored = CommitDeltaAuthoredPayloadRef {
                snapshot: payload.snapshot,
                metadata: payload.metadata,
                origin_key: payload.origin_key,
                base_coordinate: payload.base_coordinate,
            };
            storage_codec::append(
                "tracked_state indexed authored commit_delta payload",
                &mut payload_bytes,
                &authored,
            )
            .map_err(CommitDeltaSegmentEncodeError::Codec)?;
        } else {
            let value = decode_value(&entry.value)
                .expect("commit-delta entries were encoded by the mutation builder");
            let payload_free_tombstone = value.deleted
                && matches!(payload.snapshot, crate::json_store::JsonSlotRef::None)
                && matches!(payload.metadata, crate::json_store::JsonSlotRef::None)
                && payload.origin_key.is_none();
            payload_bytes.push(if payload_free_tombstone {
                COMMIT_DELTA_PAYLOAD_SELECTED_TOMBSTONE
            } else {
                COMMIT_DELTA_PAYLOAD_SELECTED_REF
            });
            if let Some(base_coordinate) = payload.base_coordinate {
                storage_codec::append(
                    "tracked_state commit_delta base coordinate",
                    &mut payload_bytes,
                    &base_coordinate,
                )
                .map_err(CommitDeltaSegmentEncodeError::Codec)?;
            }
        }
    }
    payload_offsets
        .push(u32::try_from(payload_bytes.len()).expect("commit-delta payload sidecar fits u32"));
    let entry_count = u32::try_from(entries.len()).expect("commit-delta entry count fits u32");
    let directory_bytes = payload_offsets.len() * COMMIT_DELTA_PAYLOAD_OFFSET_BYTES;
    let sidecar_len = 4usize
        .checked_add(directory_bytes)
        .and_then(|len| len.checked_add(payload_bytes.len()))
        .ok_or(CommitDeltaSegmentEncodeError::SidecarTooLarge)?;
    if sidecar_len > COMMIT_DELTA_MAX_SIDECAR_BYTES {
        return Err(CommitDeltaSegmentEncodeError::SidecarTooLarge);
    }
    let mut sidecar = Vec::with_capacity(sidecar_len);
    sidecar.extend_from_slice(&entry_count.to_be_bytes());
    for offset in payload_offsets {
        sidecar.extend_from_slice(&offset.to_be_bytes());
    }
    sidecar.extend_from_slice(&payload_bytes);
    finish_commit_delta_segment_with_sidecar(
        leaf,
        sidecar,
        compressor,
        COMMIT_DELTA_SIDECAR_RAW,
        COMMIT_DELTA_SIDECAR_ZSTD,
    )
}

fn finish_commit_delta_segment_with_sidecar(
    leaf: Vec<u8>,
    sidecar: Vec<u8>,
    compressor: Option<&mut Option<crate::compression::ZstdLevel1Compressor>>,
    raw_encoding: u8,
    zstd_encoding: u8,
) -> Result<Vec<u8>, CommitDeltaSegmentEncodeError> {
    let compressed = if sidecar.len() >= COMMIT_DELTA_MIN_COMPRESS_BYTES {
        if let Some(compressor) = compressor {
            if compressor.is_none() {
                *compressor = Some(
                    crate::compression::ZstdLevel1Compressor::new().map_err(|error| {
                        CommitDeltaSegmentEncodeError::Codec(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "tracked_state commit_delta compressor initialization failed: {error}"
                            ),
                        ))
                    })?,
                );
            }
            Some(
                compressor
                    .as_mut()
                    .expect("compressor was initialized")
                    .compress(&sidecar)
                    .map_err(|error| {
                        CommitDeltaSegmentEncodeError::Codec(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "tracked_state commit_delta sidecar compression failed: {error}"
                            ),
                        ))
                    })?,
            )
        } else {
            None
        }
    } else {
        None
    };
    let (sidecar_encoding, stored_sidecar) = match compressed.as_deref() {
        Some(compressed) if compressed.len() < sidecar.len() => (zstd_encoding, compressed),
        _ => (raw_encoding, sidecar.as_slice()),
    };
    let leaf_len = u32::try_from(leaf.len()).expect("commit-delta leaf fits u32");
    let mut encoded = Vec::with_capacity(
        COMMIT_DELTA_FORMAT_MAGIC.len() + 4 + leaf.len() + 1 + 4 + stored_sidecar.len(),
    );
    encoded.extend_from_slice(COMMIT_DELTA_FORMAT_MAGIC);
    encoded.extend_from_slice(&leaf_len.to_be_bytes());
    encoded.extend_from_slice(&leaf);
    encoded.push(sidecar_encoding);
    encoded.extend_from_slice(
        &u32::try_from(sidecar.len())
            .expect("bounded commit-delta sidecar fits u32")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(stored_sidecar);
    Ok(encoded)
}

#[cfg(test)]
fn encode_commit_delta_segment_with_payloads(
    entries: &[EncodedLeafEntry],
    payloads: &[CommitDeltaPayloadRef<'_>],
) -> Vec<u8> {
    let mut compressor = None;
    try_encode_commit_delta_segment_with_payloads(entries, payloads, &mut compressor)
        .map_err(CommitDeltaSegmentEncodeError::into_lix_error)
        .expect("test commit-delta segment should encode")
}

#[cfg(test)]
fn encode_commit_delta_segment_with_raw_sidecar(
    entries: &[EncodedLeafEntry],
    payloads: &[CommitDeltaPayloadRef<'_>],
) -> Vec<u8> {
    let entries = entries
        .iter()
        .map(EncodedLeafEntry::as_ref)
        .collect::<Vec<_>>();
    encode_commit_delta_segment_layout(&entries, payloads, None)
        .map_err(CommitDeltaSegmentEncodeError::into_lix_error)
        .expect("test raw commit-delta segment should encode")
}

fn decode_commit_delta_leaf(
    bytes: &[u8],
    expected_bounds: Option<&CommitDeltaSegmentBounds>,
) -> Result<DecodedLeafNodeRef, LixError> {
    if let Some(bounds) = expected_bounds
        && bounds.replacement_part.is_some()
    {
        return decode_replacement_part_as_commit_delta(bytes, bounds).map(|(leaf, _)| leaf);
    }
    let (leaf_bytes, _) = split_commit_delta_segment(bytes)?;
    let leaf = match decode_node_ref(leaf_bytes)? {
        DecodedNodeRef::Leaf(leaf) => leaf,
        DecodedNodeRef::Internal(_) => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta segment contains an internal tree node",
            ));
        }
    };
    if leaf.len() == 0 || leaf.len() > COMMIT_DELTA_SEGMENT_MAX_ROWS {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta segment has an invalid entry count",
        ));
    }
    if let Some(expected_bounds) = expected_bounds
        && (leaf.first_key() != Some(expected_bounds.first_key.as_slice())
            || leaf.last_key() != Some(expected_bounds.last_key.as_slice()))
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta segment does not match its manifest bounds",
        ));
    }
    Ok(leaf)
}

fn split_commit_delta_segment(bytes: &[u8]) -> Result<(&[u8], &[u8]), LixError> {
    let Some(body) = bytes.strip_prefix(COMMIT_DELTA_FORMAT_MAGIC) else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta segment has an unsupported format; recreate the repository",
        ));
    };
    let (leaf_len, body) = body.split_at_checked(4).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta segment has a truncated leaf length",
        )
    })?;
    let leaf_len = usize::try_from(u32::from_be_bytes(
        leaf_len.try_into().expect("fixed leaf length"),
    ))
    .expect("u32 fits usize");
    let (leaf_bytes, payload_bytes) = body.split_at_checked(leaf_len).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta segment has a truncated leaf",
        )
    })?;
    Ok((leaf_bytes, payload_bytes))
}

fn decode_commit_delta_with_payloads<'a>(
    bytes: &'a [u8],
    expected_bounds: Option<&CommitDeltaSegmentBounds>,
) -> Result<(DecodedLeafNodeRef, CommitDeltaPayloadIndexRef<'a>), LixError> {
    if let Some(bounds) = expected_bounds
        && bounds.replacement_part.is_some()
    {
        let (leaf, payloads) = decode_replacement_part_as_commit_delta(bytes, bounds)?;
        let entry_count = leaf.len();
        let directory_len = entry_count
            .checked_add(1)
            .and_then(|count| count.checked_mul(4))
            .expect("bounded replacement payload directory fits usize");
        return Ok((
            leaf,
            CommitDeltaPayloadIndexRef {
                sidecar: Cow::Owned(payloads),
                offsets: 0..directory_len,
                payload_start: directory_len,
                entry_count,
                layout: CommitDeltaPayloadLayout::Indexed,
            },
        ));
    }
    let (_, encoded_sidecar) = split_commit_delta_segment(bytes)?;
    let leaf = decode_commit_delta_leaf(bytes, expected_bounds)?;
    let (&encoding, encoded_sidecar) = encoded_sidecar.split_first().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta sidecar is missing its encoding",
        )
    })?;
    let (uncompressed_len, encoded_sidecar) =
        encoded_sidecar.split_at_checked(4).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta sidecar has a truncated length",
            )
        })?;
    let uncompressed_len = usize::try_from(u32::from_be_bytes(
        uncompressed_len.try_into().expect("fixed sidecar length"),
    ))
    .expect("u32 fits usize");
    if uncompressed_len == 0 || uncompressed_len > COMMIT_DELTA_MAX_SIDECAR_BYTES {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta sidecar has an invalid uncompressed length",
        ));
    }
    let layout = match encoding {
        COMMIT_DELTA_SIDECAR_RAW | COMMIT_DELTA_SIDECAR_ZSTD => CommitDeltaPayloadLayout::Indexed,
        COMMIT_DELTA_SIDECAR_AUTHORED_INLINE_RAW | COMMIT_DELTA_SIDECAR_AUTHORED_INLINE_ZSTD => {
            CommitDeltaPayloadLayout::AuthoredInline
        }
        _ => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta sidecar has an unsupported encoding",
            ));
        }
    };
    let sidecar = match encoding {
        COMMIT_DELTA_SIDECAR_RAW | COMMIT_DELTA_SIDECAR_AUTHORED_INLINE_RAW
            if encoded_sidecar.len() == uncompressed_len =>
        {
            Cow::Borrowed(encoded_sidecar)
        }
        COMMIT_DELTA_SIDECAR_RAW | COMMIT_DELTA_SIDECAR_AUTHORED_INLINE_RAW => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state raw commit_delta sidecar length does not match its header",
            ));
        }
        COMMIT_DELTA_SIDECAR_ZSTD | COMMIT_DELTA_SIDECAR_AUTHORED_INLINE_ZSTD => {
            let decoded = crate::compression::decompress_zstd(encoded_sidecar, uncompressed_len)
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "tracked_state compressed commit_delta sidecar failed to decode: {error}"
                        ),
                    )
                })?;
            if decoded.len() != uncompressed_len {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state compressed commit_delta sidecar length does not match its header",
                ));
            }
            Cow::Owned(decoded)
        }
        _ => unreachable!("commit-delta sidecar encoding was classified above"),
    };
    let sidecar_bytes = sidecar.as_ref();
    let (entry_count, sidecar_body) = sidecar_bytes.split_at_checked(4).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta payload index has a truncated entry count",
        )
    })?;
    let entry_count = usize::try_from(u32::from_be_bytes(
        entry_count.try_into().expect("fixed payload entry count"),
    ))
    .expect("u32 fits usize");
    if entry_count != leaf.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta payload count does not match its identity count",
        ));
    }
    let offset_count = entry_count.checked_add(1).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta payload directory overflows",
        )
    })?;
    let directory_len = offset_count
        .checked_mul(COMMIT_DELTA_PAYLOAD_OFFSET_BYTES)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta payload directory overflows",
            )
        })?;
    if sidecar_body.len() < directory_len {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta payload directory is truncated",
        ));
    }
    let offsets = 4..4 + directory_len;
    let payload_start = offsets.end;
    let payload_bytes_len = sidecar_bytes.len() - payload_start;
    let index = CommitDeltaPayloadIndexRef {
        sidecar,
        offsets,
        payload_start,
        entry_count,
        layout,
    };
    if index.offset(0)? != 0 {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta payload directory does not start at zero",
        ));
    }
    let mut previous = 0usize;
    for offset_index in 1..=entry_count {
        let offset = index.offset(offset_index)?;
        if offset < previous {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta payload offsets are not ordered",
            ));
        }
        if offset > payload_bytes_len {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta payload offset is out of bounds",
            ));
        }
        previous = offset;
    }
    if previous != payload_bytes_len {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta payload directory does not cover its sidecar",
        ));
    }
    Ok((leaf, index))
}

fn decode_replacement_part_as_commit_delta(
    bytes: &[u8],
    bounds: &CommitDeltaSegmentBounds,
) -> Result<(DecodedLeafNodeRef, Vec<u8>), LixError> {
    let replacement = bounds.replacement_part.as_ref().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state replacement segment is missing its physical metadata",
        )
    })?;
    let decoded = crate::tracked_state::replacement_part::decode_replacement_part(
        &replacement.content_digest,
        bytes,
    )?;
    if decoded.first_key() != Some(bounds.first_key.as_slice())
        || decoded.last_key() != Some(bounds.last_key.as_slice())
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state replacement part does not match its manifest bounds",
        ));
    }
    let owner_commit_id = CommitId::new(uuid::Uuid::from_bytes(replacement.owner_commit_id));
    let mut values = Vec::with_capacity(decoded.len());
    let mut payload_offsets = Vec::with_capacity(decoded.len() + 1);
    let mut payload_bytes = Vec::new();
    for ordinal in 0..decoded.len() {
        let packed = replacement
            .first_address
            .checked_add(u32::try_from(ordinal).expect("replacement part ordinal fits u32"))
            .and_then(|address| address.checked_add(1))
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state replacement change address overflows",
                )
            })?;
        values.push(encode_value_ref(TrackedStateIndexValueRef {
            change_id: change_id_from_packed_address(owner_commit_id, packed),
            commit_id: owner_commit_id,
            deleted: false,
            created_at: replacement.uniform_created_at,
            updated_at: replacement.uniform_updated_at,
        }));
        payload_offsets.push(
            u32::try_from(payload_bytes.len())
                .map_err(|_| replacement_payload_error("payload directory exceeds u32"))?,
        );
        let snapshot = decoded
            .snapshot(ordinal)?
            .ok_or_else(|| replacement_payload_error("part omitted a snapshot"))?;
        let metadata = decoded
            .metadata(ordinal)?
            .ok_or_else(|| replacement_payload_error("part omitted metadata authority"))?;
        payload_bytes.push(COMMIT_DELTA_PAYLOAD_AUTHORED);
        storage_codec::append(
            "tracked_state replacement authored payload",
            &mut payload_bytes,
            &CommitDeltaAuthoredPayloadRef {
                snapshot,
                metadata,
                origin_key: None,
                base_coordinate: None,
            },
        )?;
    }
    let entries = (0..decoded.len())
        .map(|ordinal| {
            Ok(EncodedLeafEntryRef {
                key: decoded.key(ordinal)?.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state replacement part omitted a key",
                    )
                })?,
                value: &values[ordinal],
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let encoded_leaf = encode_leaf_node_refs(&entries);
    let DecodedNodeRef::Leaf(leaf) = decode_node_ref(&encoded_leaf)? else {
        unreachable!("replacement leaf encoder returns a leaf")
    };
    payload_offsets.push(
        u32::try_from(payload_bytes.len())
            .map_err(|_| replacement_payload_error("payload directory exceeds u32"))?,
    );
    let mut sidecar = Vec::with_capacity(payload_offsets.len() * 4 + payload_bytes.len());
    for offset in payload_offsets {
        sidecar.extend_from_slice(&offset.to_be_bytes());
    }
    sidecar.extend_from_slice(&payload_bytes);
    Ok((leaf, sidecar))
}

fn validate_decoded_commit_delta_bounds(
    leaf: &DecodedLeafNodeRef,
    expected_bounds: Option<&CommitDeltaSegmentBounds>,
) -> Result<(), LixError> {
    if let Some(expected_bounds) = expected_bounds
        && (leaf.first_key() != Some(expected_bounds.first_key.as_slice())
            || leaf.last_key() != Some(expected_bounds.last_key.as_slice()))
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta segment does not match its manifest bounds",
        ));
    }
    Ok(())
}

fn decode_commit_delta_with_payloads_cached(
    bytes: &[u8],
    expected_bounds: Option<&CommitDeltaSegmentBounds>,
) -> Result<Option<Arc<DecodedCommitDeltaSegment>>, LixError> {
    let digest = *blake3::hash(bytes).as_bytes();
    let should_admit = {
        let mut cache = decoded_commit_delta_cache()
            .lock()
            .expect("decoded commit-delta cache lock poisoned");
        if let Some(decoded) = cache.get(digest, bytes, expected_bounds)? {
            return Ok(Some(decoded));
        }
        cache.should_admit(digest, bytes.len())
    };
    if !should_admit {
        return Ok(None);
    }

    let decoded = decode_owned_commit_delta_segment(bytes, expected_bounds)?;
    decoded_commit_delta_cache()
        .lock()
        .expect("decoded commit-delta cache lock poisoned")
        .insert(
            digest,
            Bytes::copy_from_slice(bytes),
            expected_bounds.cloned(),
            Arc::clone(&decoded),
        );
    Ok(Some(decoded))
}

fn decode_owned_commit_delta_segment(
    bytes: &[u8],
    expected_bounds: Option<&CommitDeltaSegmentBounds>,
) -> Result<Arc<DecodedCommitDeltaSegment>, LixError> {
    let (leaf, payloads) = decode_commit_delta_with_payloads(bytes, expected_bounds)?;
    let payloads = payloads.into_owned();
    let resident_bytes =
        size_of::<DecodedCommitDeltaSegment>() + leaf.resident_bytes() + payloads.resident_bytes();
    Ok(Arc::new(DecodedCommitDeltaSegment {
        leaf,
        payloads,
        resident_bytes,
    }))
}

fn decode_commit_delta_segment(
    bytes: &[u8],
    expected_bounds: Option<&CommitDeltaSegmentBounds>,
    expected_commit_id: CommitId,
) -> Result<DecodedLeafNodeRef, LixError> {
    let leaf = decode_commit_delta_leaf(bytes, expected_bounds)?;
    visit_commit_delta_leaf(&leaf, expected_commit_id, |_, _, _| Ok(()))?;
    Ok(leaf)
}

/// Visits each packed delta exactly once while validating the immutable leaf
/// contract. Scan callers decode the key and retain matching values in
/// the same pass; point callers use the no-op visitor before their binary
/// search, preserving eager corruption detection.
fn visit_commit_delta_leaf(
    leaf: &DecodedLeafNodeRef,
    expected_commit_id: CommitId,
    mut visit: impl FnMut(usize, &[u8], TrackedStateIndexValue) -> Result<(), LixError>,
) -> Result<(), LixError> {
    let mut previous_key: Option<&[u8]> = None;
    for entry_index in 0..leaf.len() {
        let entry = leaf.entry(entry_index)?.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state packed commit_delta leaf has a missing entry",
            )
        })?;
        if previous_key.is_some_and(|previous_key| previous_key >= entry.key) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta segment keys are not strictly ordered",
            ));
        }
        let value = decode_value(entry.value)?;
        if value.commit_id != expected_commit_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state packed commit_delta for commit '{expected_commit_id}' contains an entry for commit '{}'",
                    value.commit_id
                ),
            ));
        }
        visit(entry_index, entry.key, value)?;
        previous_key = Some(entry.key);
    }
    Ok(())
}

fn find_commit_delta_value(
    leaf: &DecodedLeafNodeRef,
    target_key: &[u8],
    expected_commit_id: CommitId,
) -> Result<Option<TrackedStateIndexValue>, LixError> {
    let Some(index) = find_commit_delta_entry_index(leaf, target_key)? else {
        return Ok(None);
    };
    let entry = leaf.entry(index)?.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state packed commit_delta leaf has a missing entry",
        )
    })?;
    let value = decode_value(entry.value)?;
    if value.commit_id != expected_commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state packed commit_delta for commit '{expected_commit_id}' contains an entry for commit '{}'",
                value.commit_id
            ),
        ));
    }
    Ok(Some(value))
}

fn find_loaded_commit_delta_entry<S>(
    leaf: &DecodedLeafNodeRef,
    payloads: &CommitDeltaPayloadIndex<S>,
    target_key: &[u8],
    expected_commit_id: CommitId,
) -> Result<Option<LoadedCommitDeltaEntry>, LixError>
where
    S: AsRef<[u8]>,
{
    let Some(index) = find_commit_delta_entry_index(leaf, target_key)? else {
        return Ok(None);
    };
    Ok(Some(load_commit_delta_entry_at_index(
        leaf,
        payloads,
        index,
        expected_commit_id,
    )?))
}

fn load_commit_delta_entry_at_index<S>(
    leaf: &DecodedLeafNodeRef,
    payloads: &CommitDeltaPayloadIndex<S>,
    index: usize,
    expected_commit_id: CommitId,
) -> Result<LoadedCommitDeltaEntry, LixError>
where
    S: AsRef<[u8]>,
{
    let entry = leaf.entry(index)?.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state packed commit_delta leaf has a missing entry",
        )
    })?;
    let value = decode_value(entry.value)?;
    if value.commit_id != expected_commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta payload has the wrong physical commit id",
        ));
    }
    let payload = payloads.decode(index)?;
    let key = decode_key(entry.key)?;
    let (snapshot, metadata, origin_key, base_coordinate, selected_ref) = match payload {
        CommitDeltaPayload::Authored(payload) => (
            payload.snapshot,
            payload.metadata,
            payload.origin_key,
            payload.base_coordinate,
            false,
        ),
        CommitDeltaPayload::SelectedRef(base_coordinate) => (
            crate::json_store::JsonSlot::None,
            crate::json_store::JsonSlot::None,
            None,
            base_coordinate,
            true,
        ),
        CommitDeltaPayload::SelectedTombstone(base_coordinate) => (
            crate::json_store::JsonSlot::None,
            crate::json_store::JsonSlot::None,
            None,
            base_coordinate,
            false,
        ),
    };
    let change_record = crate::changelog::ChangeRecord {
        format_version: 2,
        change_id: value.change_id,
        schema_key: key.schema_key,
        entity_pk: key.entity_pk,
        file_id: key.file_id,
        snapshot,
        metadata,
        created_at: value.updated_at,
        origin_key,
    };
    Ok(LoadedCommitDeltaEntry {
        value,
        change_record,
        base_coordinate,
        selected_ref,
    })
}

fn find_commit_delta_entry_index(
    leaf: &DecodedLeafNodeRef,
    target_key: &[u8],
) -> Result<Option<usize>, LixError> {
    let mut lower = 0usize;
    let mut upper = leaf.len();
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        let key = leaf.key(middle)?.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state packed commit_delta leaf has a missing key",
            )
        })?;
        if key < target_key {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    let Some(entry) = leaf.entry(lower)? else {
        return Ok(None);
    };
    if entry.key != target_key {
        return Ok(None);
    }
    Ok(Some(lower))
}

pub(crate) async fn read_chunk(
    store: &(impl StorageAdapterRead + ?Sized),
    hash: &[u8; TRACKED_STATE_HASH_BYTES],
) -> Result<Option<Bytes>, LixError> {
    get_one(store, TRACKED_STATE_TREE_CHUNK_SPACE, hash.to_vec()).await
}

pub(crate) fn verify_chunk_hash(
    expected: &[u8; TRACKED_STATE_HASH_BYTES],
    bytes: &[u8],
) -> Result<(), LixError> {
    let actual = crate::tracked_state::codec::hash_bytes(bytes);
    if &actual != expected {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state chunk hash mismatch",
        ));
    }
    Ok(())
}

pub(crate) fn debug_verify_chunk_hash(
    expected: &[u8; TRACKED_STATE_HASH_BYTES],
    bytes: &[u8],
) -> Result<(), LixError> {
    if cfg!(debug_assertions) {
        verify_chunk_hash(expected, bytes)?;
    }
    Ok(())
}

#[derive(Debug, Default)]
pub(crate) struct TrackedStateChunkOverlay {
    chunks: HashMap<[u8; TRACKED_STATE_HASH_BYTES], Bytes>,
}

impl TrackedStateChunkOverlay {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn staged_chunk(&self, hash: &[u8; TRACKED_STATE_HASH_BYTES]) -> Option<&[u8]> {
        self.chunks.get(hash).map(AsRef::as_ref)
    }

    fn staged_chunk_bytes(&self, hash: &[u8; TRACKED_STATE_HASH_BYTES]) -> Option<Bytes> {
        self.chunks.get(hash).cloned()
    }

    pub(crate) fn stage_chunks(
        &mut self,
        writes: &mut StorageWriteSet,
        chunks: &PendingChunkBatch,
    ) {
        if chunks.is_empty() {
            return;
        }
        let mut key_arena =
            Vec::with_capacity(chunks.len().saturating_mul(TRACKED_STATE_HASH_BYTES));
        let mut puts = Vec::with_capacity(chunks.len());
        for chunk in chunks.chunks() {
            let key_start = key_arena.len();
            key_arena.extend_from_slice(&chunk.hash);
            puts.push(EncodedPut {
                key: BufferRange::new(key_start, TRACKED_STATE_HASH_BYTES),
                value: BufferRange::new(chunk.data_start, chunk.data_len),
            });
            self.chunks.insert(chunk.hash, chunks.chunk_data(*chunk));
        }
        let batch = EncodedMutationBatch::try_new(
            Bytes::from(key_arena),
            chunks.data().clone(),
            puts,
            Vec::new(),
        )
        .expect("tracked-state chunk batch descriptors must match their arenas");
        writes.stage_content_addressed_encoded_batch(TRACKED_STATE_TREE_CHUNK_SPACE, batch);
    }
}

/// Point-read overlay used to audit rebuilt roots before their write set is
/// published. Changelog reads fall through to the coherent base snapshot;
/// content-addressed tree chunks staged by the root writer are visible here.
#[derive(Debug)]
pub(crate) struct TrackedStateStagedRead<'a, S: ?Sized> {
    store: &'a S,
    chunks: &'a TrackedStateChunkOverlay,
    commit_states: HashMap<Vec<u8>, Bytes>,
    commit_state_seals: HashMap<Vec<u8>, Bytes>,
}

impl<'a, S> TrackedStateStagedRead<'a, S>
where
    S: StorageAdapterRead + ?Sized,
{
    pub(crate) fn new(store: &'a S, chunks: &'a TrackedStateChunkOverlay) -> Self {
        Self {
            store,
            chunks,
            commit_states: HashMap::new(),
            commit_state_seals: HashMap::new(),
        }
    }

    pub(crate) fn with_commit_state_manifests(
        store: &'a S,
        chunks: &'a TrackedStateChunkOverlay,
        manifests: impl IntoIterator<Item = CommitStateManifest>,
    ) -> Result<Self, LixError> {
        let manifests = manifests.into_iter().collect::<Vec<_>>();
        let commit_states = manifests
            .iter()
            .map(|manifest| {
                Ok((
                    commit_state_manifest_key(manifest.commit_id),
                    Bytes::from(encode_commit_state_manifest(manifest)?),
                ))
            })
            .collect::<Result<HashMap<_, _>, LixError>>()?;
        let commit_state_seals = manifests
            .iter()
            .map(|manifest| {
                Ok((
                    commit_state_manifest_key(manifest.commit_id),
                    Bytes::copy_from_slice(&commit_state_semantic_seal(manifest)?),
                ))
            })
            .collect::<Result<HashMap<_, _>, LixError>>()?;
        Ok(Self {
            store,
            chunks,
            commit_states,
            commit_state_seals,
        })
    }

    fn staged_bytes(&self, space: StorageSpaceId, key: &StorageKey) -> Option<Bytes> {
        if space == TRACKED_STATE_TREE_CHUNK_SPACE.id {
            let key = <&[u8; TRACKED_STATE_HASH_BYTES]>::try_from(key.0.as_ref()).ok()?;
            return self.chunks.staged_chunk_bytes(key);
        }
        if space == TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE.id {
            return self.commit_states.get(key.0.as_ref()).cloned();
        }
        if space == TRACKED_STATE_COMMIT_STATE_SEAL_SPACE.id {
            return self.commit_state_seals.get(key.0.as_ref()).cloned();
        }
        None
    }
}

impl<S> StorageAdapterRead for TrackedStateStagedRead<'_, S>
where
    S: StorageAdapterRead + ?Sized,
{
    async fn get_many(
        &self,
        requests: &[StorageGetManyRequest<'_>],
    ) -> Result<StorageGetManyResult, StorageError> {
        let mut result = exact_get_many(self.store, requests).await?;
        let mut slots = result.values.iter_mut();
        for request in requests {
            for (key, slot) in request.keys.iter().zip(slots.by_ref()) {
                let Some(bytes) = self.staged_bytes(request.space.id, key) else {
                    continue;
                };
                *slot = Some(match request.opts.projection {
                    StorageCoreProjection::KeyOnly => StorageProjectedValue::KeyOnly,
                    StorageCoreProjection::FullValue => StorageProjectedValue::FullValue(bytes),
                });
            }
        }
        Ok(result)
    }

    async fn scan(
        &self,
        space: StorageSpace,
        range: StorageKeyRange,
        opts: StorageScanOptions,
    ) -> Result<StorageScanChunk, StorageError> {
        if space == TRACKED_STATE_TREE_CHUNK_SPACE {
            return Err(StorageError::Io(
                "tracked-state staged audit supports point reads only for overlay spaces"
                    .to_string(),
            ));
        }
        self.store.scan(space, range, opts).await
    }
}

fn key(bytes: Vec<u8>) -> StorageKey {
    StorageKey(Bytes::from(bytes))
}

fn value(bytes: Vec<u8>) -> StorageValue {
    StorageValue {
        bytes: Bytes::from(bytes),
    }
}

fn full_value_bytes(value: StorageProjectedValue) -> Option<Bytes> {
    match value {
        StorageProjectedValue::FullValue(bytes) => Some(bytes),
        StorageProjectedValue::KeyOnly => None,
    }
}

fn encode_commit_state_manifest(manifest: &CommitStateManifest) -> Result<Vec<u8>, LixError> {
    validate_commit_state_manifest(manifest)?;
    let payload = storage_codec::encode("tracked_state commit_state_manifest", manifest)?;
    let mut encoded = Vec::with_capacity(COMMIT_STATE_MANIFEST_FORMAT_MAGIC.len() + payload.len());
    encoded.extend_from_slice(COMMIT_STATE_MANIFEST_FORMAT_MAGIC);
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn decode_commit_state_manifest(bytes: &[u8]) -> Result<CommitStateManifest, LixError> {
    decode_commit_state_manifest_with_catalog_attestation(bytes, true)
}

fn decode_commit_state_manifest_with_catalog_attestation(
    bytes: &[u8],
    validate_catalog_attestation: bool,
) -> Result<CommitStateManifest, LixError> {
    let Some(payload) = bytes.strip_prefix(COMMIT_STATE_MANIFEST_FORMAT_MAGIC) else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest has an unsupported format; recreate the repository",
        ));
    };
    let manifest = storage_codec::decode("tracked_state commit_state_manifest", payload)?;
    validate_commit_state_manifest_inner(&manifest, validate_catalog_attestation)?;
    Ok(manifest)
}

fn validate_commit_state_manifest(manifest: &CommitStateManifest) -> Result<(), LixError> {
    validate_commit_state_manifest_inner(manifest, true)
}

fn validate_commit_state_manifest_inner(
    manifest: &CommitStateManifest,
    validate_catalog_attestation: bool,
) -> Result<(), LixError> {
    let mut parents = BTreeSet::new();
    for parent in &manifest.parent_commit_ids {
        if *parent == manifest.commit_id || !parents.insert(*parent) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_state_manifest has a self or duplicate parent",
            ));
        }
    }

    match &manifest.snapshot_root {
        Some(root) => {
            if root.commit_id != manifest.commit_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_state_manifest snapshot belongs to another commit",
                ));
            }
            if root.parent_roots.first().map(|parent| parent.commit_id)
                != manifest.parent_commit_ids.first().copied()
                || root
                    .parent_roots
                    .iter()
                    .any(|parent| !parents.contains(&parent.commit_id))
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_state_manifest snapshot ancestry disagrees with commit topology",
                ));
            }
        }
        None if manifest.replay_debt.depth == 0 => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state rootless commit_state_manifest has zero replay depth",
            ));
        }
        None => {}
    }
    if manifest.replay_debt.depth == 0
        && (manifest.replay_debt.rows != 0 || manifest.replay_debt.bytes != 0)
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest has replay work at zero depth",
        ));
    }
    if manifest.replay_debt.depth > crate::tracked_state::COMMIT_STATE_MAX_REPLAY_DEPTH
        || manifest.replay_debt.bytes > crate::tracked_state::COMMIT_STATE_MAX_REPLAY_BYTES
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest replay debt exceeds the protocol bound",
        ));
    }

    validate_commit_state_mutation_inventory(manifest.commit_id, &manifest.mutations)?;
    validate_current_state_catalog(manifest, validate_catalog_attestation)
}

fn validate_current_state_catalog(
    manifest: &CommitStateManifest,
    validate_catalog_attestation: bool,
) -> Result<(), LixError> {
    if manifest.current_state_catalog.as_ref().is_some_and(|root| {
        root.root_id == [0; 32]
            || root.entry_count == 0
            || root.transition_digest == [0; 32]
            || manifest.parent_commit_ids.len() > 1
            || manifest.mutations.selected_source_commit_id.is_some()
    }) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest has an invalid current-state catalog root",
        ));
    }
    if validate_catalog_attestation && let Some(root) = manifest.current_state_catalog.as_ref() {
        let expected = super::current_state_part::current_state_catalog_transition_digest(
            manifest.commit_id,
            root.parent_root_id,
            &manifest.mutations,
            root.root_id,
            root.entry_count,
        )?;
        if root.transition_digest != expected
            || (manifest.parent_commit_ids.is_empty() && root.parent_root_id.is_some())
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state current-state catalog transition is not bound to this commit",
            ));
        }
    }
    if manifest.current_state_catalog.is_none() && manifest.current_state_coverage_anchor.is_some()
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest has a coverage anchor without a catalog",
        ));
    }
    if let Some(anchor) = manifest.current_state_coverage_anchor.as_ref() {
        let (Some(generation), Some(_authority)) = (
            manifest.mutations.replacement_generation.as_ref(),
            manifest.mutations.replacement_parts.as_ref(),
        ) else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state current-state coverage anchor has no replacement authority",
            ));
        };
        if anchor.scope != generation.scope
            || anchor.generation_integrity_digest != generation.integrity_digest
            || anchor.directory.row_count != u64::from(manifest.mutations.member_count)
            || anchor.directory.part_count as usize != manifest.mutations.part_count()
            || anchor.directory.root_id == [0; 32]
            || anchor.directory.tree_height == 0
            || anchor.state_lineage_digest
                != super::current_state_part::fresh_current_state_lineage_digest(
                    manifest.commit_id,
                    generation.integrity_digest,
                    &anchor.directory,
                )
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state current-state coverage anchor disagrees with replacement authority",
            ));
        }
    }
    Ok(())
}

fn validate_commit_state_mutation_inventory(
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<(), LixError> {
    if inventory.selected_source_commit_id() == Some(commit_id) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest selects itself as a mutation source",
        ));
    }
    if !inventory.inline_part.is_empty()
        && (!inventory.parts.is_empty() || !inventory.replacement_part_digests.is_empty())
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest mixes inline and external mutation parts",
        ));
    }
    if inventory.parts.iter().any(|part| {
        part.first_key.is_empty() || part.last_key.is_empty() || part.first_key > part.last_key
    }) || inventory
        .parts
        .windows(2)
        .any(|pair| pair[0].last_key >= pair[1].first_key)
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest has invalid or overlapping mutation-part bounds",
        ));
    }
    if !inventory.replacement_part_digests.is_empty()
        && (!inventory.parts.is_empty()
            || inventory.replacement_parts.is_none()
            || inventory.replacement_generation.is_none()
            || inventory.replacement_part_digests.contains(&[0; 32]))
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest has an invalid compact replacement-part inventory",
        ));
    }
    if !inventory.replacement_part_digests.is_empty() {
        validate_compact_replacement_inventory(commit_id, inventory)?;
    }
    if inventory.member_count > 0
        && inventory.part_count() == 0
        && inventory.selected_source_commit_id.is_none()
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest has members without mutation parts",
        ));
    }
    if !inventory.direct_part_row_counts.is_empty() {
        let direct_rows = &inventory.direct_part_row_counts;
        if direct_rows.len() != inventory.part_count()
            || direct_rows
                .iter()
                .any(|&rows| rows == 0 || usize::from(rows) > COMMIT_DELTA_SEGMENT_MAX_ROWS)
            || direct_rows.iter().map(|&rows| u64::from(rows)).sum::<u64>()
                != u64::from(inventory.member_count)
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_state_manifest has an invalid direct mutation address inventory",
            ));
        }
    }
    let is_empty = inventory.member_count == 0
        && inventory.selected_source_commit_id.is_none()
        && inventory.direct_part_row_counts.is_empty()
        && inventory.single_partition.is_none()
        && inventory.lifecycle_summary.is_none()
        && inventory.replacement_generation.is_none()
        && inventory.replacement_parts.is_none()
        && inventory.replacement_part_digests.is_empty()
        && inventory.inline_part.is_empty()
        && inventory.parts.is_empty();
    if !is_empty {
        if inventory.replacement_part_digests.is_empty() {
            validate_commit_delta_manifest(&commit_delta_manifest_from_inventory(inventory))?;
        }
        if inventory
            .replacement_generation
            .as_ref()
            .is_some_and(|generation| generation.owner_commit_id != *commit_id.as_uuid().as_bytes())
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state replacement generation does not belong to its commit-state authority",
            ));
        }
    }
    Ok(())
}

fn validate_compact_replacement_inventory(
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<(), LixError> {
    let invalid = || {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest has an invalid compact replacement authority",
        )
    };
    let generation = inventory
        .replacement_generation
        .as_ref()
        .ok_or_else(invalid)?;
    let lifecycle = inventory.lifecycle_summary.as_ref().ok_or_else(invalid)?;
    let authority = inventory.replacement_parts.as_ref().ok_or_else(invalid)?;
    let scope = inventory.single_partition.as_ref().ok_or_else(invalid)?;
    if inventory.selected_source_commit_id.is_some()
        || inventory.member_count == 0
        || scope.schema_key.is_empty()
        || generation.scope != *scope
        || lifecycle.scope != *scope
        || generation.owner_commit_id != *commit_id.as_uuid().as_bytes()
        || generation.owner_commit_id == [0; 16]
        || generation.integrity_digest
            != replacement_generation_integrity_digest(generation, lifecycle, authority)
        || !inventory.inline_part.is_empty()
        || !inventory.parts.is_empty()
        || inventory.replacement_part_digests.len() != inventory.direct_part_row_counts.len()
        || inventory
            .direct_part_row_counts
            .iter()
            .any(|&count| count == 0 || usize::from(count) > COMMIT_DELTA_SEGMENT_MAX_ROWS)
        || inventory
            .direct_part_row_counts
            .iter()
            .map(|&count| u64::from(count))
            .sum::<u64>()
            != u64::from(inventory.member_count)
    {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::future::Future;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;

    use crate::LixError;
    use crate::binary_cas::kv::{
        BINARY_CAS_CHUNK_PRESENCE_SPACE, BINARY_CAS_CHUNK_SPACE, BINARY_CAS_MANIFEST_CHUNK_SPACE,
        BINARY_CAS_MANIFEST_SPACE,
    };
    use crate::branch::BRANCH_HEAD_CONTROL_SPACE;
    use crate::changelog::{
        CHANGE_SPACE, COMMIT_CHANGE_ID_SPACE, COMMIT_SPACE, ChangeId, CommitId,
    };
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::gc::{CHECKPOINT_GC_STATE_SPACE, CHECKPOINT_RECOVERY_REF_SPACE};
    use crate::init::REPOSITORY_PROTOCOL_SPACE;
    use crate::json_store::{UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE, store::JSON_SPACE};
    use crate::live_state::{
        HOT_DIFF_SPACE, HOT_FILE_SPACE, HOT_ROW_SPACE, TRACKED_WORKING_DIFF_MARKER_SPACE,
    };
    use crate::storage_adapter::{
        Memory, StorageAdapter, StorageKey, StorageReadOptions, StorageSpace, StorageWriteOptions,
        StorageWriteSet,
    };
    use crate::tracked_state::codec::{
        EncodedLeafEntry, PendingChunk, PendingChunkBatch, TrackedStateKeyBatchBuilder,
        encode_key_ref, encode_value_ref, hash_bytes,
    };
    use crate::tracked_state::current_state_part::{
        CURRENT_STATE_PART_DIRECTORY_SPACE, load_current_state_catalog_entry,
        route_current_state_part, stage_fresh_current_state_catalog,
    };
    use crate::tracked_state::types::{
        CommitStateManifest, CommitStateMutationInventory, CommitStateReplayDebt,
        TrackedStateBaseCoordinate, TrackedStateCommitDeltaRef, TrackedStateCommitRoot,
        TrackedStateDeltaRef, TrackedStateIndexValue, TrackedStateIndexValueRef, TrackedStateKey,
        TrackedStateKeyRef, TrackedStateRootId,
    };

    use super::{
        COMMIT_DELTA_FORMAT_MAGIC, COMMIT_STATE_MANIFEST_FORMAT_MAGIC, CommitDeltaChangeLocator,
        CommitDeltaManifest, CommitDeltaPayloadRef, DecodedCommitDeltaBatch,
        DecodedCommitDeltaCache, DecodedCommitDeltaSegment, GENERIC_COMMIT_DELTA_SEGMENT_MAX_ROWS,
        TRACKED_STATE_CHANGE_LOCATOR_SPACE, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
        TRACKED_STATE_TREE_CHUNK_SPACE, TrackedStateChunkOverlay,
        decode_commit_delta_with_payloads, decode_commit_state_manifest,
        direct_change_locator_in_commit_state, encode_commit_delta_segment,
        encode_commit_delta_segment_with_payloads, encode_commit_delta_segment_with_raw_sidecar,
        encode_commit_state_manifest, key, load_change_record_by_id, load_commit_delta_change_ids,
        load_commit_delta_change_records, load_commit_delta_members_with_payloads,
        load_commit_delta_values_encoded, load_commit_state_manifest,
        load_owned_commit_delta_entries, scan_change_records_from_commit_deltas,
        scan_commit_delta_inventory, scan_commit_delta_members, scan_commit_delta_values,
        stage_change_locators, stage_commit_state_manifest,
        stage_delete_commit_delta_inventory_entry, value,
    };

    struct SealCountingRead<R> {
        inner: R,
        manifest_requests: std::sync::Arc<AtomicUsize>,
        seal_requests: std::sync::Arc<AtomicUsize>,
    }

    impl<R> crate::storage_adapter::StorageAdapterRead for SealCountingRead<R>
    where
        R: crate::storage_adapter::StorageAdapterRead,
    {
        fn snapshot_cache_key(&self) -> Option<u128> {
            self.inner.snapshot_cache_key()
        }

        fn get_many(
            &self,
            requests: &[crate::storage::GetManyRequest<'_>],
        ) -> impl Future<
            Output = Result<crate::storage::GetManyResult, crate::storage::StorageError>,
        > + Send {
            for request in requests {
                if request.space == super::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE {
                    self.manifest_requests.fetch_add(1, Ordering::Relaxed);
                }
                if request.space == super::TRACKED_STATE_COMMIT_STATE_SEAL_SPACE {
                    self.seal_requests.fetch_add(1, Ordering::Relaxed);
                }
            }
            self.inner.get_many(requests)
        }

        fn scan(
            &self,
            space: StorageSpace,
            range: crate::storage::KeyRange,
            opts: crate::storage::ScanOptions,
        ) -> impl Future<Output = Result<crate::storage::ScanChunk, crate::storage::StorageError>> + Send
        {
            self.inner.scan(space, range, opts)
        }
    }

    fn fixture_commit_state_manifest(
        commit_id: CommitId,
        mutations: CommitStateMutationInventory,
    ) -> CommitStateManifest {
        CommitStateManifest {
            commit_id,
            generation: 0,
            parent_commit_ids: Vec::new(),
            commit_change_id: ChangeId::for_test_label(&format!("{commit_id}:commit")),
            author_account_ids: Vec::new(),
            created_at: LixTimestamp::from_unix_millis_utc_lossy(0),
            replay_debt: CommitStateReplayDebt {
                depth: 1,
                rows: u64::from(mutations.member_count),
                bytes: u64::from(mutations.member_count),
            },
            mutations,
            current_state_catalog: None,
            current_state_coverage_anchor: None,
            snapshot_root: None,
        }
    }

    fn stage_fixture_manifest(
        writes: &mut StorageWriteSet,
        commit_id: CommitId,
        mutations: &CommitStateMutationInventory,
    ) -> Result<(), LixError> {
        let mut compact_mutations = mutations.clone();
        let mut manifest = fixture_commit_state_manifest(commit_id, compact_mutations.clone());
        let publication =
            stage_fresh_current_state_catalog(writes, commit_id, &mut compact_mutations)?;
        let (catalog, anchor) = publication.parts();
        manifest.current_state_catalog = catalog;
        manifest.current_state_coverage_anchor = anchor;
        if manifest.current_state_catalog.is_some() {
            compact_mutations.parts.clear();
            manifest.mutations = compact_mutations;
        }
        super::stage_certified_commit_state_manifest(writes, &manifest, &publication)
    }

    fn stage_commit_deltas(
        writes: &mut StorageWriteSet,
        deltas: &[TrackedStateCommitDeltaRef<'_>],
    ) -> Result<Vec<CommitDeltaChangeLocator>, LixError> {
        let staged = super::stage_commit_deltas_for_commit_state(writes, deltas)?;
        let commit_id = deltas
            .first()
            .map(|delta| delta.delta.commit_id)
            .unwrap_or_default();
        stage_fixture_manifest(writes, commit_id, staged.mutation_inventory())?;
        Ok(staged.locators)
    }

    fn stage_addressable_commit_deltas(
        writes: &mut StorageWriteSet,
        deltas: &[TrackedStateCommitDeltaRef<'_>],
        addressable: &[bool],
    ) -> Result<super::AddressableCommitDeltaStage, LixError> {
        let staged = super::stage_addressable_commit_deltas(writes, deltas, addressable)?;
        let commit_id = deltas
            .first()
            .map(|delta| delta.delta.commit_id)
            .unwrap_or_default();
        let mutations = fixture_addressable_inventory(&staged);
        stage_fixture_manifest(writes, commit_id, &mutations)?;
        Ok(staged)
    }

    fn stage_addressable_commit_deltas_with_selected_source(
        writes: &mut StorageWriteSet,
        deltas: &[TrackedStateCommitDeltaRef<'_>],
        addressable: &[bool],
        selected_source_commit_id: CommitId,
    ) -> Result<super::AddressableCommitDeltaStage, LixError> {
        let staged = super::stage_addressable_commit_deltas_with_selected_source(
            writes,
            deltas,
            addressable,
            selected_source_commit_id,
        )?;
        let commit_id = deltas
            .first()
            .map(|delta| delta.delta.commit_id)
            .unwrap_or_default();
        let mutations = fixture_addressable_inventory(&staged);
        stage_fixture_manifest(writes, commit_id, &mutations)?;
        Ok(staged)
    }

    fn fixture_addressable_inventory(
        staged: &super::AddressableCommitDeltaStage,
    ) -> CommitStateMutationInventory {
        let mut mutations = staged.mutation_inventory().clone();
        if mutations.direct_part_row_counts.is_empty()
            && staged
                .assigned_change_ids
                .iter()
                .any(|change_id| *change_id != ChangeId::default())
        {
            let mut row_counts = vec![0_u16; mutations.part_count()];
            for locator in staged.locators.iter().copied().chain(
                staged
                    .assigned_change_ids
                    .iter()
                    .copied()
                    .filter(|change_id| *change_id != ChangeId::default())
                    .filter_map(super::direct_change_locator),
            ) {
                let rows = &mut row_counts[locator.segment_index as usize];
                *rows = (*rows).max(locator.ordinal + 1);
            }
            mutations.direct_part_row_counts = row_counts;
        }
        mutations
    }

    fn stage_ordered_addressable_commit_deltas<'a, I>(
        writes: &mut StorageWriteSet,
        deltas: I,
        order_certified: bool,
    ) -> Result<Option<super::OrderedAddressableCommitDeltaStage>, LixError>
    where
        I: ExactSizeIterator<Item = Result<TrackedStateCommitDeltaRef<'a>, LixError>> + Clone,
    {
        let staged =
            super::stage_ordered_addressable_commit_deltas(writes, deltas, order_certified, false)?;
        if let Some(staged) = &staged {
            stage_fixture_manifest(writes, staged.commit_id, staged.mutation_inventory())?;
        }
        Ok(staged)
    }

    #[test]
    fn decoded_commit_delta_point_cache_reuses_an_immutable_segment() {
        let commit_id = CommitId::for_test_label("decoded-point-cache");
        let fixtures = packed_commit_delta_fixtures()
            .into_iter()
            .take(2)
            .collect::<Vec<_>>();
        let entries = fixtures
            .iter()
            .map(|fixture| EncodedLeafEntry {
                key: encode_key_ref(TrackedStateKeyRef {
                    schema_key: &fixture.schema_key,
                    file_id: fixture.file_id.as_deref(),
                    entity_pk: &fixture.entity_pk,
                })
                .into(),
                value: encode_value_ref(TrackedStateIndexValueRef {
                    change_id: fixture.change_id,
                    commit_id,
                    deleted: fixture.deleted,
                    created_at: fixture.created_at,
                    updated_at: fixture.updated_at,
                })
                .into(),
            })
            .collect::<Vec<_>>();
        let encoded = encode_commit_delta_segment(&entries);
        let (leaf, payloads) =
            decode_commit_delta_with_payloads(&encoded, None).expect("decode point-cache segment");
        let payloads = payloads.into_owned();
        let decoded = std::sync::Arc::new(DecodedCommitDeltaSegment {
            resident_bytes: leaf.resident_bytes() + payloads.resident_bytes(),
            leaf,
            payloads,
        });
        let digest = *blake3::hash(&encoded).as_bytes();
        let mut cache = DecodedCommitDeltaCache::default();
        assert!(
            !cache.should_admit(digest, encoded.len()),
            "a one-off point read should not retain a decoded block"
        );
        assert!(
            cache.should_admit(digest, encoded.len()),
            "a repeated point read should promote its decoded block"
        );
        cache.insert(
            digest,
            encoded.clone().into(),
            None,
            std::sync::Arc::clone(&decoded),
        );
        let reused = cache
            .get(digest, &encoded, None)
            .expect("read point-cache entry")
            .expect("point-cache entry should exist");
        assert!(
            std::sync::Arc::ptr_eq(&decoded, &reused),
            "the same immutable bytes should reuse one decoded block"
        );
        assert_eq!(cache.entries.len(), 1);
        assert!(cache.resident_bytes > 0);
        assert!(cache.resident_bytes <= super::DECODED_COMMIT_DELTA_CACHE_MAX_BYTES);

        let transaction_cache = super::CommitDeltaPointReadCache::default();
        for index in 0..=super::TRANSACTION_COMMIT_DELTA_POINT_CACHE_MAX_ENTRIES {
            transaction_cache
                .remember_segment(
                    CommitId::for_test_label(&format!("transaction-point-cache-{index}")),
                    index,
                    std::sync::Arc::clone(&decoded),
                )
                .expect("remember transaction-addressed decoded segment");
        }
        let transaction_cache = transaction_cache.inner.lock().unwrap();
        assert!(
            transaction_cache.segments.len()
                <= super::TRANSACTION_COMMIT_DELTA_POINT_CACHE_MAX_ENTRIES
        );
        assert!(
            transaction_cache.segment_resident_bytes
                <= super::TRANSACTION_COMMIT_DELTA_POINT_CACHE_MAX_BYTES
        );
    }

    #[derive(Clone)]
    struct CommitDeltaFixture {
        schema_key: String,
        file_id: Option<String>,
        entity_pk: EntityPk,
        change_id: ChangeId,
        deleted: bool,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
    }

    impl CommitDeltaFixture {
        fn key(&self) -> TrackedStateKey {
            TrackedStateKey {
                schema_key: self.schema_key.clone(),
                file_id: self.file_id.clone(),
                entity_pk: self.entity_pk.clone(),
            }
        }

        fn value(&self, commit_id: CommitId) -> TrackedStateIndexValue {
            TrackedStateIndexValue {
                change_id: self.change_id,
                commit_id,
                deleted: self.deleted,
                created_at: self.created_at,
                updated_at: self.updated_at,
            }
        }
    }

    async fn load_commit_delta_values_for_test(
        store: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
        commit_id: CommitId,
        keys: &[TrackedStateKey],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        let mut encoded_keys = TrackedStateKeyBatchBuilder::with_row_capacity(keys.len());
        for key in keys {
            encoded_keys.push(TrackedStateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            });
        }
        load_commit_delta_values_encoded(store, commit_id, &encoded_keys.finish()).await
    }

    fn packed_commit_delta_fixtures() -> Vec<CommitDeltaFixture> {
        (0..300)
            .map(|index| CommitDeltaFixture {
                schema_key: if index % 2 == 0 {
                    "alpha".to_string()
                } else {
                    "beta".to_string()
                },
                file_id: None,
                entity_pk: EntityPk::single(format!("entity-{index:04}")),
                change_id: ChangeId::for_test_label(&format!("packed-delta-change-{index}")),
                deleted: index % 7 == 0,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(index.into()),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy((index + 1).into()),
            })
            .collect()
    }

    fn commit_delta_refs(
        commit_id: CommitId,
        fixtures: &[CommitDeltaFixture],
    ) -> Vec<TrackedStateCommitDeltaRef<'_>> {
        fixtures
            .iter()
            .map(|fixture| {
                commit_delta_ref(
                    commit_id,
                    fixture,
                    crate::json_store::JsonSlotRef::None,
                    crate::json_store::JsonSlotRef::None,
                    None,
                )
            })
            .collect()
    }

    fn commit_delta_ref<'a>(
        commit_id: CommitId,
        fixture: &'a CommitDeltaFixture,
        snapshot: crate::json_store::JsonSlotRef<'a>,
        metadata: crate::json_store::JsonSlotRef<'a>,
        origin_key: Option<&'a str>,
    ) -> TrackedStateCommitDeltaRef<'a> {
        TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: &fixture.schema_key,
                file_id: fixture.file_id.as_deref(),
                entity_pk: &fixture.entity_pk,
                change_id: fixture.change_id,
                commit_id,
                deleted: fixture.deleted,
                created_at: fixture.created_at,
                updated_at: fixture.updated_at,
            },
            snapshot,
            metadata,
            origin_key,
            base_coordinate: None,
            authored: true,
        }
    }

    #[test]
    fn packed_history_spaces_do_not_span_the_live_state_key_range() {
        const FIRST_LIVE_STATE_SPACE: [u8; 4] = 0x0004_001b_u32.to_be_bytes();
        for space in [
            TRACKED_STATE_CHANGE_LOCATOR_SPACE,
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
        ] {
            assert!(
                space.physical_prefix() < FIRST_LIVE_STATE_SPACE,
                "{space} would make packed-history SSTs overlap live-state keys"
            );
        }
    }

    #[tokio::test]
    async fn change_locator_loads_inline_and_segmented_records_by_id() {
        for (label, fixtures) in [
            (
                "inline",
                packed_commit_delta_fixtures()
                    .into_iter()
                    .take(3)
                    .collect::<Vec<_>>(),
            ),
            ("segmented", packed_commit_delta_fixtures()),
        ] {
            let storage = StorageAdapter::new(Memory::new());
            let commit_id = CommitId::for_test_label(&format!("{label}-locator-commit"));
            let deltas = commit_delta_refs(commit_id, &fixtures);
            let mut writes = storage.new_write_set();
            let locators =
                stage_commit_deltas(&mut writes, &deltas).expect("locator delta should stage");
            stage_change_locators(&mut writes, &locators);
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("locator delta should commit");
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("locator read should open");
            let expected = &fixtures[fixtures.len() / 2];
            let loaded = load_change_record_by_id(&read, expected.change_id)
                .await
                .expect("exact locator read should succeed")
                .expect("exact locator should find the change");
            assert_eq!(loaded.change_id, expected.change_id);
            assert_eq!(loaded.schema_key, expected.schema_key);
            assert_eq!(loaded.entity_pk, expected.entity_pk);
            assert_eq!(loaded.file_id, expected.file_id);
            assert_eq!(loaded.created_at, expected.updated_at);
            assert!(
                load_change_record_by_id(
                    &read,
                    ChangeId::for_test_label(&format!("{label}-missing-change"))
                )
                .await
                .expect("missing exact locator read should succeed")
                .is_none()
            );
        }
    }

    #[tokio::test]
    async fn addressable_change_loads_without_a_per_change_locator() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_1234_5678_9abc,
        ));
        let fixtures = packed_commit_delta_fixtures();
        let deltas = commit_delta_refs(commit_id, &fixtures);
        let mut writes = storage.new_write_set();
        let staged =
            stage_addressable_commit_deltas(&mut writes, &deltas, &vec![true; deltas.len()])
                .expect("addressable deltas should stage");
        assert!(staged.locators.is_empty());
        assert!(
            staged
                .assigned_change_ids
                .iter()
                .all(|change_id| *change_id != ChangeId::default())
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("addressable deltas should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("addressable read should open");
        let source_index = fixtures.len() / 2;
        let change_id = staged.assigned_change_ids[source_index];
        let authority = load_commit_state_manifest(&read, commit_id)
            .await
            .expect("commit-state authority should load")
            .expect("commit-state authority should exist");
        assert_eq!(authority.mutations, fixture_addressable_inventory(&staged));
        assert_eq!(
            direct_change_locator_in_commit_state(&authority, change_id),
            super::direct_change_locator(change_id),
            "the authoritative inventory must retain the assigned direct slot"
        );
        assert!(
            super::load_change_locator_by_id(&read, change_id)
                .await
                .expect("locator absence should read")
                .is_none()
        );
        let loaded = load_change_record_by_id(&read, change_id)
            .await
            .expect("direct address should read")
            .expect("direct address should resolve");
        assert_eq!(loaded.change_id, change_id);
        assert_eq!(loaded.schema_key, fixtures[source_index].schema_key);
        assert_eq!(loaded.entity_pk, fixtures[source_index].entity_pk);
        let batch = super::load_change_records_by_ids(&read, &[change_id])
            .await
            .expect("direct address batch should read");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0], loaded);
    }

    #[tokio::test]
    async fn ordered_addressable_commit_delta_streams_segment_assignments() {
        let storage = StorageAdapter::new(Memory::new());
        let generic_storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_5678_0000_0000,
        ));
        let mut fixtures = packed_commit_delta_fixtures();
        fixtures.sort_unstable_by_key(CommitDeltaFixture::key);
        let deltas = fixtures
            .iter()
            .map(|fixture| {
                commit_delta_ref(
                    commit_id,
                    fixture,
                    if fixture.deleted {
                        crate::json_store::JsonSlotRef::None
                    } else {
                        crate::json_store::JsonSlotRef::Inline(r#"{"streamed":true}"#)
                    },
                    crate::json_store::JsonSlotRef::None,
                    None,
                )
            })
            .collect::<Vec<_>>();
        let mut writes = storage.new_write_set();
        let staged = stage_ordered_addressable_commit_deltas(
            &mut writes,
            deltas.iter().copied().map(Ok::<_, LixError>),
            false,
        )
        .expect("ordered addressable deltas should stage")
        .expect("sorted deltas should use the streaming route");
        assert_eq!(staged.row_count(), fixtures.len());
        let assigned = staged.assigned_change_ids().collect::<Vec<_>>();
        assert_eq!(assigned.len(), fixtures.len());
        assert!(
            assigned
                .iter()
                .all(|change_id| *change_id != ChangeId::default())
        );
        let mut generic_writes = generic_storage.new_write_set();
        let generic = stage_addressable_commit_deltas(
            &mut generic_writes,
            &deltas,
            &vec![true; deltas.len()],
        )
        .expect("generic addressable deltas should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("streamed addressable deltas should commit");
        generic_storage
            .commit_write_set(generic_writes, StorageWriteOptions::default())
            .await
            .expect("generic addressable deltas should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("streamed addressable read should open");
        let certificate = super::load_commit_delta_selection_certificate(&read, commit_id)
            .await
            .expect("dense selection certificate should load")
            .expect("ordered commit should have a selection certificate");
        assert!(!certificate.direct_segment_row_counts.is_empty());
        assert_eq!(
            certificate
                .direct_segment_row_counts
                .iter()
                .map(|&count| usize::from(count))
                .sum::<usize>(),
            fixtures.len()
        );
        assert_eq!(certificate.selection_fingerprint, [0; 32]);
        assert!(
            super::load_commit_delta_replay_metadata(&read, commit_id)
                .await
                .expect("replay metadata should load")
                .expect("ordered commit has replay metadata")
                .replacement_generation
                .is_none()
        );
        let generic_read = generic_storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("generic addressable read should open");
        for source_index in [0, 127, 128, fixtures.len() - 1] {
            let loaded = load_change_record_by_id(&read, assigned[source_index])
                .await
                .expect("direct streamed address should read")
                .expect("direct streamed address should resolve");
            assert_eq!(loaded.change_id, assigned[source_index]);
            assert_eq!(loaded.schema_key, fixtures[source_index].schema_key);
            assert_eq!(loaded.entity_pk, fixtures[source_index].entity_pk);
            assert_eq!(loaded.snapshot.is_none(), fixtures[source_index].deleted);
            let generic_loaded =
                load_change_record_by_id(&generic_read, generic.assigned_change_ids[source_index])
                    .await
                    .expect("direct generic address should read")
                    .expect("direct generic address should resolve");
            assert_eq!(generic_loaded.schema_key, loaded.schema_key);
            assert_eq!(generic_loaded.entity_pk, loaded.entity_pk);
            assert_eq!(generic_loaded.file_id, loaded.file_id);
            assert_eq!(generic_loaded.snapshot, loaded.snapshot);
            assert_eq!(generic_loaded.metadata, loaded.metadata);
            assert_eq!(generic_loaded.created_at, loaded.created_at);
            assert_eq!(generic_loaded.origin_key, loaded.origin_key);
        }
    }

    #[tokio::test]
    async fn irregular_ordered_segment_addresses_match_the_manifest() {
        use std::fmt::Write as _;

        const ROW_COUNT: usize = 700;
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_9876_0000_0000,
        ));
        let fixtures = (0..ROW_COUNT)
            .map(|index| CommitDeltaFixture {
                schema_key: "irregular".to_string(),
                file_id: None,
                entity_pk: EntityPk::single(format!("entity-{index:04}")),
                change_id: ChangeId::for_test_label(&format!("irregular-change-{index}")),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(index as i64),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(index as i64 + 1),
            })
            .collect::<Vec<_>>();
        let snapshots = (0..ROW_COUNT)
            .map(|index| {
                let mut state = (index as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
                let mut payload = String::with_capacity(1_024);
                for _ in 0..64 {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    write!(&mut payload, "{state:016x}").expect("write deterministic payload");
                }
                format!(r#"{{"payload":"{payload}"}}"#)
            })
            .collect::<Vec<_>>();
        let deltas = fixtures
            .iter()
            .zip(&snapshots)
            .map(|(fixture, snapshot)| {
                commit_delta_ref(
                    commit_id,
                    fixture,
                    crate::json_store::JsonSlotRef::Inline(snapshot),
                    crate::json_store::JsonSlotRef::None,
                    None,
                )
            })
            .collect::<Vec<_>>();
        let mut writes = storage.new_write_set();
        let staged = super::stage_ordered_addressable_commit_deltas(
            &mut writes,
            deltas.iter().copied().map(Ok::<_, LixError>),
            true,
            false,
        )
        .expect("irregular ordered deltas should stage")
        .expect("certified ordered deltas should use the streaming route");
        stage_fixture_manifest(&mut writes, staged.commit_id, staged.mutation_inventory())
            .expect("commit-state authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("irregular ordered deltas should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("irregular ordered read should open");
        let certificate = super::load_commit_delta_selection_certificate(&read, commit_id)
            .await
            .expect("irregular selection certificate should load")
            .expect("irregular ordered commit should have a certificate");
        assert!(
            super::load_commit_delta_replay_metadata(&read, commit_id)
                .await
                .expect("replay metadata should load")
                .expect("ordered commit has replay metadata")
                .replacement_generation
                .is_none()
        );
        assert!(
            certificate
                .direct_segment_row_counts
                .iter()
                .take(certificate.direct_segment_row_counts.len() - 1)
                .any(|&count| usize::from(count) < super::COMMIT_DELTA_SEGMENT_MAX_ROWS),
            "payload bytes should force a non-final segment below the row limit"
        );
        let assigned = staged.assigned_change_ids().collect::<Vec<_>>();
        assert_eq!(assigned.len(), ROW_COUNT);
        assert_eq!(staged.change_id_at(ROW_COUNT), None);

        let mut row_start = 0usize;
        for (segment_index, &segment_rows) in
            certificate.direct_segment_row_counts.iter().enumerate()
        {
            for row_index in [row_start, row_start + usize::from(segment_rows) - 1] {
                assert_eq!(staged.change_id_at(row_index), Some(assigned[row_index]));
                let locator = super::direct_change_locator(assigned[row_index])
                    .expect("assigned id should carry a direct locator");
                assert_eq!(locator.segment_index, segment_index as u32);
                assert_eq!(locator.ordinal as usize, row_index - row_start);
                let loaded = load_change_record_by_id(&read, assigned[row_index])
                    .await
                    .expect("irregular direct address should read")
                    .expect("irregular direct address should resolve");
                assert_eq!(loaded.entity_pk, fixtures[row_index].entity_pk);
            }
            row_start += usize::from(segment_rows);
        }
        assert_eq!(row_start, ROW_COUNT);
    }

    #[tokio::test]
    async fn complete_replacement_parts_are_bounded_content_addressed_and_replay_exactly() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_0000_0000_7000_8000_0000_0000_0000,
        ));
        let created_at = LixTimestamp::from_unix_millis_utc_lossy(11);
        let updated_at = LixTimestamp::from_unix_millis_utc_lossy(22);
        let fixtures = (0..1_025)
            .map(|index| CommitDeltaFixture {
                schema_key: "alpha".to_string(),
                file_id: None,
                entity_pk: EntityPk::single(format!("entity-{index:05}")),
                change_id: ChangeId::for_test_label(&format!("ignored-{index}")),
                deleted: false,
                created_at,
                updated_at,
            })
            .collect::<Vec<_>>();
        let mut deltas = commit_delta_refs(commit_id, &fixtures);
        for (index, delta) in deltas.iter_mut().enumerate() {
            delta.snapshot = crate::json_store::JsonSlotRef::Inline("{}");
            delta.base_coordinate = Some(TrackedStateBaseCoordinate {
                base_commit_id: commit_id,
                group_index: u32::try_from(index / 257).expect("fixture group fits u32"),
                row_index: u32::try_from(index % 257).expect("fixture row fits u32"),
            });
        }
        let generation = super::CommitDeltaReplacementGeneration {
            scope: super::CommitDeltaReplacementScope {
                schema_key: "alpha".to_string(),
                file_id: None,
            },
            fallback_commit_id: None,
            lifecycle_summary: super::CommitDeltaLifecycleSummary {
                scope: super::CommitDeltaReplacementScope {
                    schema_key: "alpha".to_string(),
                    file_id: None,
                },
                ordered_identity_digest: [3; 32],
                uniform_created_at: created_at,
            },
        };
        let mut writes = storage.new_write_set();
        let staged = super::stage_ordered_addressable_replacement_parts(
            &mut writes,
            deltas.iter().copied().map(Ok),
            &generation,
        )
        .expect("replacement parts should stage");
        let mut borrowed_writes = storage.new_write_set();
        let borrowed_staged = super::stage_ordered_addressable_replacement_parts(
            &mut borrowed_writes,
            fixtures.iter().map(|fixture| {
                Ok(
                    crate::tracked_state::TrackedStateSingleStringReplacementRef {
                        schema_key: &fixture.schema_key,
                        file_id: fixture.file_id.as_deref(),
                        entity_pk: fixture
                            .entity_pk
                            .as_single_string()
                            .expect("fixture identity is one string"),
                        commit_id,
                        created_at: fixture.created_at,
                        updated_at: fixture.updated_at,
                        snapshot: crate::json_store::JsonSlotRef::Inline("{}"),
                        metadata: crate::json_store::JsonSlotRef::None,
                    },
                )
            }),
            &generation,
        )
        .expect("borrowed replacement parts should stage");
        assert_eq!(borrowed_staged.commit_id, staged.commit_id);
        assert_eq!(borrowed_staged.row_count(), staged.row_count());
        assert_eq!(
            borrowed_staged.mutation_inventory(),
            staged.mutation_inventory()
        );
        assert_eq!(
            borrowed_staged.assigned_change_ids().collect::<Vec<_>>(),
            staged.assigned_change_ids().collect::<Vec<_>>()
        );
        assert_eq!(
            format!("{borrowed_writes:?}"),
            format!("{writes:?}"),
            "borrowed and generic replacement staging must be byte-identical"
        );
        stage_fixture_manifest(&mut writes, staged.commit_id, staged.mutation_inventory())
            .expect("replacement commit-state authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("replacement parts should commit atomically");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let authority = load_commit_state_manifest(&read, commit_id)
            .await
            .expect("replacement authority should load")
            .expect("replacement authority should exist");
        let state_set = load_current_state_catalog_entry(
            &read,
            authority
                .current_state_catalog
                .as_ref()
                .expect("replacement should publish a current-state catalog"),
            &authority
                .current_state_coverage_anchor
                .as_ref()
                .expect("replacement should publish a coverage anchor")
                .scope,
        )
        .await
        .expect("replacement catalog should load")
        .expect("replacement should publish a current-state part set");
        let published = super::load_published_commit_state_manifest(&read, commit_id)
            .await
            .expect("published replacement authority should load")
            .expect("published replacement authority should exist");
        assert_eq!(
            super::load_complete_current_state_part_set_from_published_manifest(
                &read,
                &published,
                &state_set.scope,
            )
            .await
            .expect("published exact scope should authenticate"),
            Some(state_set.clone()),
        );
        let directory_root = state_set.directory.clone();
        assert_eq!(state_set.directory.part_count, 3);
        let route_key = fixtures[777].key();
        let routed = route_current_state_part(
            &read,
            &state_set.directory,
            &encode_key_ref(TrackedStateKeyRef {
                schema_key: &route_key.schema_key,
                file_id: route_key.file_id.as_deref(),
                entity_pk: &route_key.entity_pk,
            }),
        )
        .await
        .expect("current-state route should load")
        .expect("replacement identity should be covered");
        assert_eq!(routed.part_index, 1);
        let physical = super::scan_full_space(&read, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE)
            .await
            .expect("replacement parts should scan");
        assert_eq!(physical.len(), 3);
        assert!(physical.iter().all(|(key, bytes)| {
            key.0.len() == 52
                && (bytes.starts_with(b"LXRPI003") || bytes.starts_with(b"LXRPZ003"))
                && bytes.len() <= 64 * 1024
        }));

        let keys = [
            fixtures[0].key(),
            fixtures[777].key(),
            fixtures[1_024].key(),
        ];
        let values = load_commit_delta_values_for_test(&read, commit_id, &keys)
            .await
            .expect("replacement point replay should load");
        assert!(values.iter().all(Option::is_some));
        let point_cache = super::CommitDeltaPointReadCache::default();
        let mut membership = point_cache.live_membership_cursor(commit_id);
        let mut encoded_key = Vec::new();
        for fixture_index in [0, 777, 1_024] {
            encoded_key.clear();
            crate::tracked_state::encode_single_string_key_ref_into(
                &mut encoded_key,
                "alpha",
                None,
                fixtures[fixture_index]
                    .entity_pk
                    .as_single_string()
                    .expect("fixture identity is one string"),
            );
            assert_eq!(
                membership
                    .live_member(&read, &point_cache, &encoded_key)
                    .await
                    .expect("cold monotonic membership should load its bounded part"),
                Some(true)
            );
        }
        let change_id = staged
            .change_id_at(777)
            .expect("replacement row has a direct address");
        let change_ids = load_commit_delta_change_ids(&read, commit_id)
            .await
            .expect("replacement addresses should scan without hydrating payloads");
        assert_eq!(change_ids[777], change_id);
        assert_eq!(
            load_change_record_by_id(&read, change_id)
                .await
                .expect("direct replacement history should load")
                .expect("direct replacement history should exist")
                .entity_pk,
            fixtures[777].entity_pk,
        );

        drop(read);
        let mut deletes = storage.new_write_set();
        deletes.delete(
            CURRENT_STATE_PART_DIRECTORY_SPACE,
            StorageKey(Bytes::copy_from_slice(&directory_root.root_id)),
        );
        storage
            .commit_write_set(deletes, StorageWriteOptions::default())
            .await
            .expect("serving-directory corruption should stage");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("fallback read should open");
        let values = load_commit_delta_values_for_test(&read, commit_id, &keys)
            .await
            .expect("missing serving directory should rebuild from history parts");
        assert!(values.iter().all(Option::is_some));
        assert!(
            load_change_record_by_id(&read, change_id)
                .await
                .expect("historical authority should survive directory loss")
                .is_some()
        );
    }

    #[tokio::test]
    async fn persistent_catalog_reuses_untouched_scope_and_rewrites_touched_scope() {
        let storage = StorageAdapter::new(Memory::new());
        let parent_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_2000_0000_7000_8000_0000_0000_0000,
        ));
        let child_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_2000_0000_7000_8000_0001_0000_0000,
        ));
        let created_at = LixTimestamp::from_unix_millis_utc_lossy(11);
        let updated_at = LixTimestamp::from_unix_millis_utc_lossy(22);
        let alpha = (0..3)
            .map(|index| CommitDeltaFixture {
                schema_key: "alpha".to_string(),
                file_id: None,
                entity_pk: EntityPk::single(format!("entity-{index}")),
                change_id: ChangeId::for_test_label(&format!("catalog-alpha-{index}")),
                deleted: false,
                created_at,
                updated_at,
            })
            .collect::<Vec<_>>();
        let mut parent_deltas = commit_delta_refs(parent_id, &alpha);
        for delta in &mut parent_deltas {
            delta.snapshot = crate::json_store::JsonSlotRef::Inline("{}");
        }
        let scope = super::CommitDeltaReplacementScope {
            schema_key: "alpha".to_string(),
            file_id: None,
        };
        let generation = super::CommitDeltaReplacementGeneration {
            scope: scope.clone(),
            fallback_commit_id: None,
            lifecycle_summary: super::CommitDeltaLifecycleSummary {
                scope: scope.clone(),
                ordered_identity_digest: [7; 32],
                uniform_created_at: created_at,
            },
        };
        let mut writes = storage.new_write_set();
        let parent_stage = super::stage_ordered_addressable_replacement_parts(
            &mut writes,
            parent_deltas.iter().copied().map(Ok),
            &generation,
        )
        .expect("parent replacement should stage");
        stage_fixture_manifest(&mut writes, parent_id, parent_stage.mutation_inventory())
            .expect("parent manifest should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("parent replacement should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("parent read should open");
        let parent = super::load_published_commit_state_manifest(&read, parent_id)
            .await
            .expect("parent manifest should load")
            .expect("parent manifest should exist");
        let parent_root = parent
            .current_state_catalog
            .clone()
            .expect("parent should publish a catalog");
        let selected_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_2000_0000_7000_8000_0000_1000_0000,
        ));
        let mut selected_delta = commit_delta_refs(selected_id, std::slice::from_ref(&alpha[0]));
        selected_delta[0].delta.change_id = parent_stage
            .change_id_at(0)
            .expect("replacement row should have an address");
        selected_delta[0].authored = false;
        let mut selected_writes = storage.new_write_set();
        let selected_stage = super::stage_ordered_addressable_commit_deltas(
            &mut selected_writes,
            selected_delta.iter().copied().map(Ok),
            true,
            false,
        )
        .expect("selected mutation should stage")
        .expect("selected mutation should be addressable");
        let selected_publication = super::stage_current_state_catalog_from_published_parent(
            &read,
            &mut selected_writes,
            Some(&parent),
            selected_id,
            selected_stage.mutation_inventory(),
        )
        .await
        .expect("omitted selected payload must not fail commit publication");
        assert!(
            selected_publication.parts().0.is_none(),
            "only the unsupported serving entry should be dropped"
        );
        let beta = CommitDeltaFixture {
            schema_key: "beta".to_string(),
            file_id: None,
            entity_pk: EntityPk::single("other"),
            change_id: ChangeId::for_test_label("catalog-beta-child"),
            deleted: false,
            created_at,
            updated_at,
        };
        let child_deltas = commit_delta_refs(child_id, std::slice::from_ref(&beta));
        let mut child_writes = storage.new_write_set();
        let child_stage = super::stage_ordered_addressable_commit_deltas(
            &mut child_writes,
            child_deltas.iter().copied().map(Ok),
            true,
            false,
        )
        .expect("child mutation should stage")
        .expect("child mutation should be addressable");
        let mut child_inventory = child_stage.mutation_inventory().clone();
        let child_publication = super::stage_current_state_catalog_from_published_parent(
            &read,
            &mut child_writes,
            Some(&parent),
            child_id,
            &mut child_inventory,
        )
        .await
        .expect("child catalog should path-copy");
        let (child_catalog, child_anchor) = child_publication.parts();
        let child_catalog = child_catalog.expect("child should retain inherited catalog");
        assert_eq!(child_catalog.root_id, parent_root.root_id);
        assert_eq!(child_catalog.parent_root_id, Some(parent_root.root_id));
        assert!(child_anchor.is_none());
        let mut child = fixture_commit_state_manifest(child_id, child_inventory);
        child.parent_commit_ids = vec![parent_id];
        child.replay_debt.depth = super::MIN_CURRENT_STATE_CATALOG_POINT_READS;
        child.current_state_catalog = Some(child_catalog);
        let mut cross_wired_child = child.clone();
        cross_wired_child.parent_commit_ids = vec![CommitId::for_test_label("foreign-parent")];
        assert!(
            super::stage_certified_commit_state_manifest(
                &mut child_writes,
                &cross_wired_child,
                &child_publication,
            )
            .is_err(),
            "catalog proof must bind the exact graph parent commit"
        );
        let mut foreign_writes = storage.new_write_set();
        assert!(
            super::stage_certified_commit_state_manifest(
                &mut foreign_writes,
                &child,
                &child_publication,
            )
            .is_err(),
            "catalog proof must bind the write set containing its physical nodes"
        );
        super::stage_certified_commit_state_manifest(&mut child_writes, &child, &child_publication)
            .expect("child manifest should stage");
        drop(read);
        storage
            .commit_write_set(child_writes, StorageWriteOptions::default())
            .await
            .expect("child should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("child read should open");
        let child = super::load_published_commit_state_manifest(&read, child_id)
            .await
            .expect("child manifest should load")
            .expect("child manifest should exist");
        let mut encoded = TrackedStateKeyBatchBuilder::with_row_capacity(2);
        for key in [
            alpha[1].key(),
            TrackedStateKey {
                schema_key: "alpha".to_string(),
                file_id: None,
                entity_pk: EntityPk::single("missing"),
            },
        ] {
            encoded.push(TrackedStateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            });
        }
        let resolved =
            super::load_complete_current_state_values_encoded(&read, &child, &encoded.finish())
                .await
                .expect("catalog points should resolve")
                .expect("alpha is covered");
        assert!(resolved[0].is_some());
        assert!(resolved[1].is_none(), "covered miss must be authoritative");

        let mut forged = (*child).clone();
        forged.current_state_catalog = Some(parent_root.clone());
        let mut forged_writes = storage.new_write_set();
        assert!(
            stage_commit_state_manifest(&mut forged_writes, &forged).is_err(),
            "an ancestor catalog root must not be cross-wired into a descendant manifest"
        );

        let touching_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_2000_0000_7000_8000_0002_0000_0000,
        ));
        let mut touching_deltas = commit_delta_refs(touching_id, std::slice::from_ref(&alpha[0]));
        touching_deltas[0].delta.created_at = LixTimestamp::from_unix_millis_utc_lossy(999);
        touching_deltas[0].snapshot = crate::json_store::JsonSlotRef::Inline("{\"updated\":true}");
        let mut touching_writes = storage.new_write_set();
        let touching_stage = super::stage_ordered_addressable_commit_deltas(
            &mut touching_writes,
            touching_deltas.iter().copied().map(Ok),
            true,
            false,
        )
        .expect("same-scope mutation should stage")
        .expect("same-scope mutation should be addressable");
        let mut touching_inventory = touching_stage.mutation_inventory().clone();
        let touching_publication = super::stage_current_state_catalog_from_published_parent(
            &read,
            &mut touching_writes,
            Some(&child),
            touching_id,
            &mut touching_inventory,
        )
        .await
        .expect("same-scope sparse rewrite should succeed");
        let (touching_catalog, _) = touching_publication.parts();
        let touching_catalog = touching_catalog.expect("touched scope should remain served");
        assert_ne!(
            touching_catalog.root_id,
            child
                .current_state_catalog
                .as_ref()
                .expect("child catalog exists")
                .root_id,
            "touched scope must publish a new post-image catalog"
        );
        let mut touching = fixture_commit_state_manifest(touching_id, touching_inventory.clone());
        touching.parent_commit_ids = vec![child_id];
        touching.replay_debt.depth = super::MIN_CURRENT_STATE_CATALOG_POINT_READS;
        touching.current_state_catalog = Some(touching_catalog.clone());
        let staged_touching = super::stage_certified_commit_state_manifest_with_handle(
            &mut touching_writes,
            &touching,
            &touching_publication,
        )
        .expect("sparse post-image manifest should stage");
        let staged_child_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_2000_0000_7000_8000_0002_1000_0000,
        ));
        let mut staged_child_deltas =
            commit_delta_refs(staged_child_id, std::slice::from_ref(&alpha[2]));
        staged_child_deltas[0].snapshot =
            crate::json_store::JsonSlotRef::Inline("{\"staged_child\":true}");
        let staged_child_stage = super::stage_ordered_addressable_commit_deltas(
            &mut touching_writes,
            staged_child_deltas.iter().copied().map(Ok),
            true,
            false,
        )
        .expect("staged child mutation should stage")
        .expect("staged child mutation should be addressable");
        let staged_child_inventory = staged_child_stage.mutation_inventory().clone();
        let staged_child_publication = super::stage_current_state_catalog_from_staged_parent(
            &read,
            &mut touching_writes,
            &staged_touching,
            staged_child_id,
            &staged_child_inventory,
        )
        .await
        .expect("staged child should read parent catalog, directory, and data parts");
        let (staged_child_catalog, _) = staged_child_publication.parts();
        let mut staged_child =
            fixture_commit_state_manifest(staged_child_id, staged_child_inventory);
        staged_child.parent_commit_ids = vec![touching_id];
        staged_child.replay_debt.depth = super::MIN_CURRENT_STATE_CATALOG_POINT_READS;
        staged_child.current_state_catalog = staged_child_catalog;
        let staged_staged_child = super::stage_certified_commit_state_manifest_with_handle(
            &mut touching_writes,
            &staged_child,
            &staged_child_publication,
        )
        .expect("staged child manifest should stage in the same write set");
        let absent_delete_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_2000_0000_7000_8000_0002_2000_0000,
        ));
        let absent_delete = CommitDeltaFixture {
            schema_key: "alpha".to_string(),
            file_id: None,
            entity_pk: EntityPk::single("absent-staged-child-key"),
            change_id: ChangeId::for_test_label("catalog-staged-child-absent-delete"),
            deleted: true,
            created_at,
            updated_at,
        };
        let absent_delete_deltas =
            commit_delta_refs(absent_delete_id, std::slice::from_ref(&absent_delete));
        let absent_delete_stage = super::stage_ordered_addressable_commit_deltas(
            &mut touching_writes,
            absent_delete_deltas.iter().copied().map(Ok),
            true,
            false,
        )
        .expect("absent delete should stage")
        .expect("absent delete should be addressable");
        let absent_delete_inventory = absent_delete_stage.mutation_inventory().clone();
        let absent_delete_publication = super::stage_current_state_catalog_from_staged_parent(
            &read,
            &mut touching_writes,
            &staged_staged_child,
            absent_delete_id,
            &absent_delete_inventory,
        )
        .await
        .expect("absent delete should reuse identical staged current-state parts");
        let (absent_delete_catalog, _) = absent_delete_publication.parts();
        let mut absent_delete_manifest =
            fixture_commit_state_manifest(absent_delete_id, absent_delete_inventory);
        absent_delete_manifest.parent_commit_ids = vec![staged_child_id];
        absent_delete_manifest.replay_debt.depth = super::MIN_CURRENT_STATE_CATALOG_POINT_READS;
        absent_delete_manifest.current_state_catalog = absent_delete_catalog;
        super::stage_certified_commit_state_manifest(
            &mut touching_writes,
            &absent_delete_manifest,
            &absent_delete_publication,
        )
        .expect("absent-delete child manifest should coalesce in the same write set");
        touching_writes
            .validate()
            .expect("two staged children must retain one immutable data-part put per digest");
        drop(read);
        storage
            .commit_write_set(touching_writes, StorageWriteOptions::default())
            .await
            .expect("sparse post-image should commit atomically");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("sparse post-image read should open");
        let published_touching = super::load_published_commit_state_manifest(&read, touching_id)
            .await
            .expect("sparse manifest should load")
            .expect("sparse manifest should exist");
        let updated_key = Bytes::from(encode_key_ref(TrackedStateKeyRef {
            schema_key: &alpha[0].schema_key,
            file_id: alpha[0].file_id.as_deref(),
            entity_pk: &alpha[0].entity_pk,
        }));
        assert!(
            super::load_complete_current_state_values_from_published_replay_manifest(
                &read,
                &published_touching,
                std::slice::from_ref(&updated_key),
            )
            .await
            .expect("hot-key replay preference should resolve")
            .is_none(),
            "a key authored by the head must retain the cheaper OLTP replay path"
        );
        let inherited_manifest_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let inherited_seal_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let inherited_read = SealCountingRead {
            inner: storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("inherited catalog read should open"),
            manifest_requests: std::sync::Arc::clone(&inherited_manifest_requests),
            seal_requests: std::sync::Arc::clone(&inherited_seal_requests),
        };
        let updated = super::load_complete_current_state_values_from_published_manifest(
            &inherited_read,
            &published_touching,
            std::slice::from_ref(&updated_key),
        )
        .await
        .expect("sparse point should resolve")
        .expect("sparse scope should remain covered");
        assert_eq!(
            updated[0].as_ref().map(|value| value.commit_id),
            Some(touching_id)
        );
        assert_eq!(
            updated[0].as_ref().map(|value| value.created_at),
            Some(created_at),
            "ordinary updates must retain the insertion lifecycle timestamp"
        );
        assert_eq!(inherited_manifest_requests.load(Ordering::Relaxed), 0);
        assert_eq!(inherited_seal_requests.load(Ordering::Relaxed), 0);

        let replay_state = super::load_point_replay_commit_state(&inherited_read, touching_id)
            .await
            .expect("one-read replay authority should authenticate")
            .expect("sparse replay authority should exist");
        assert_eq!(inherited_manifest_requests.load(Ordering::Relaxed), 0);
        assert_eq!(inherited_seal_requests.load(Ordering::Relaxed), 1);
        inherited_manifest_requests.store(0, Ordering::Relaxed);
        inherited_seal_requests.store(0, Ordering::Relaxed);
        let cold_key = Bytes::from(encode_key_ref(TrackedStateKeyRef {
            schema_key: &alpha[1].schema_key,
            file_id: alpha[1].file_id.as_deref(),
            entity_pk: &alpha[1].entity_pk,
        }));
        let cold = super::load_complete_current_state_values_from_replay_manifest(
            &inherited_read,
            &replay_state,
            std::slice::from_ref(&cold_key),
        )
        .await
        .expect("authenticated inherited catalog point should resolve")
        .expect("inherited scope should remain covered");
        assert!(cold[0].is_some());
        assert_eq!(inherited_manifest_requests.load(Ordering::Relaxed), 0);
        assert_eq!(inherited_seal_requests.load(Ordering::Relaxed), 0);

        let mut forged_root = parent_root;
        forged_root.parent_root_id = child
            .current_state_catalog
            .as_ref()
            .map(|root| root.root_id);
        forged_root.transition_digest =
            crate::tracked_state::current_state_part::current_state_catalog_transition_digest(
                touching_id,
                forged_root.parent_root_id,
                &touching_inventory,
                forged_root.root_id,
                forged_root.entry_count,
            )
            .expect("forged transition digest should compute");
        let mut forged_touching = fixture_commit_state_manifest(touching_id, touching_inventory);
        forged_touching.parent_commit_ids = vec![child_id];
        forged_touching.current_state_catalog = Some(forged_root);
        let mut forged_writes = storage.new_write_set();
        assert!(
            stage_commit_state_manifest(&mut forged_writes, &forged_touching).is_err(),
            "catalog authority cannot bypass canonical publication certification"
        );
        assert!(
            super::stage_certified_commit_state_manifest(
                &mut forged_writes,
                &forged_touching,
                &touching_publication,
            )
            .is_err(),
            "the canonical touched-scope proof must reject a stale catalog root"
        );
        let forged_entry = load_current_state_catalog_entry(
            &read,
            forged_touching
                .current_state_catalog
                .as_ref()
                .expect("forged catalog exists"),
            &scope,
        )
        .await
        .expect("forged stale scope should be structurally readable");
        assert!(forged_entry.is_some());
        assert!(
            crate::tracked_state::current_state_part::validate_current_state_catalog_transition_root(
                &read,
                &forged_touching,
                Some(&child),
            )
            .await
            .is_err(),
            "recomputed attestation must not authorize a stale touched scope"
        );

        let insert_delete_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_2000_0000_7000_8000_0003_0000_0000,
        ));
        let delete = CommitDeltaFixture {
            change_id: ChangeId::for_test_label("catalog-alpha-delete"),
            deleted: true,
            ..alpha[1].clone()
        };
        let insert = CommitDeltaFixture {
            schema_key: "alpha".to_string(),
            file_id: None,
            entity_pk: EntityPk::single("entity-inserted"),
            change_id: ChangeId::for_test_label("catalog-alpha-insert"),
            deleted: false,
            created_at,
            updated_at,
        };
        let sparse_fixtures = [delete, insert];
        let mut sparse_deltas = commit_delta_refs(insert_delete_id, &sparse_fixtures);
        sparse_deltas[1].snapshot = crate::json_store::JsonSlotRef::Inline("{\"inserted\":true}");
        let mut sparse_writes = storage.new_write_set();
        let sparse_stage = super::stage_ordered_addressable_commit_deltas(
            &mut sparse_writes,
            sparse_deltas.iter().copied().map(Ok),
            true,
            false,
        )
        .expect("insert/delete mutation should stage")
        .expect("insert/delete mutation should be addressable");
        let mut sparse_inventory = sparse_stage.mutation_inventory().clone();
        let sparse_publication = super::stage_current_state_catalog_from_published_parent(
            &read,
            &mut sparse_writes,
            Some(&published_touching),
            insert_delete_id,
            &mut sparse_inventory,
        )
        .await
        .expect("insert/delete post-image should stage");
        let (sparse_catalog, _) = sparse_publication.parts();
        let mut sparse = fixture_commit_state_manifest(insert_delete_id, sparse_inventory);
        sparse.parent_commit_ids = vec![touching_id];
        sparse.replay_debt.depth = super::MIN_CURRENT_STATE_CATALOG_POINT_READS;
        sparse.current_state_catalog = sparse_catalog;
        super::stage_certified_commit_state_manifest(
            &mut sparse_writes,
            &sparse,
            &sparse_publication,
        )
        .expect("insert/delete manifest should stage");
        drop(read);
        storage
            .commit_write_set(sparse_writes, StorageWriteOptions::default())
            .await
            .expect("insert/delete post-image should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("insert/delete read should open");
        let published_sparse = super::load_published_commit_state_manifest(&read, insert_delete_id)
            .await
            .expect("insert/delete manifest should load")
            .expect("insert/delete manifest should exist");
        let mut keys = TrackedStateKeyBatchBuilder::with_row_capacity(4);
        for key in [
            alpha[0].key(),
            alpha[1].key(),
            sparse_fixtures[1].key(),
            TrackedStateKey {
                schema_key: "alpha".to_string(),
                file_id: None,
                entity_pk: EntityPk::single("authoritative-miss"),
            },
        ] {
            keys.push(TrackedStateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            });
        }
        let resolved = super::load_complete_current_state_values_from_published_manifest(
            &read,
            &published_sparse,
            &keys.finish(),
        )
        .await
        .expect("insert/delete points should resolve")
        .expect("insert/delete scope should remain covered");
        assert!(resolved[0].is_some(), "untouched row must survive");
        assert!(resolved[1].is_none(), "delete must be authoritative");
        assert_eq!(
            resolved[2].as_ref().map(|value| value.commit_id),
            Some(insert_delete_id)
        );
        assert!(resolved[3].is_none(), "exact miss must be authoritative");
    }

    #[tokio::test]
    async fn address_shaped_explicit_change_id_falls_back_to_its_locator() {
        let storage = StorageAdapter::new(Memory::new());
        let addressable_commit_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_1234_0000_0000,
        ));
        let address_target = packed_commit_delta_fixtures()
            .into_iter()
            .next()
            .expect("fixture should exist");
        let mut writes = storage.new_write_set();
        let target_deltas =
            commit_delta_refs(addressable_commit_id, std::slice::from_ref(&address_target));
        let target_staged = stage_addressable_commit_deltas(&mut writes, &target_deltas, &[false])
            .expect("non-addressable target should stage");
        stage_change_locators(&mut writes, &target_staged.locators);

        let explicit_change_id =
            super::addressable_change_id(addressable_commit_id, 0, 0).expect("valid direct shape");
        let explicit_commit_id = CommitId::for_test_label("address-shaped-explicit-commit");
        let mut explicit = packed_commit_delta_fixtures()
            .into_iter()
            .nth(1)
            .expect("second fixture should exist");
        explicit.change_id = explicit_change_id;
        let explicit_deltas =
            commit_delta_refs(explicit_commit_id, std::slice::from_ref(&explicit));
        let explicit_staged =
            stage_addressable_commit_deltas(&mut writes, &explicit_deltas, &[false])
                .expect("explicit change should stage with a locator");
        assert_eq!(explicit_staged.locators.len(), 1);
        stage_change_locators(&mut writes, &explicit_staged.locators);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("collision fixture should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("collision read should open");
        let loaded = load_change_record_by_id(&read, explicit_change_id)
            .await
            .expect("explicit collision read should succeed")
            .expect("explicit collision should resolve through its locator");
        assert_eq!(loaded.change_id, explicit_change_id);
        assert_eq!(loaded.schema_key, explicit.schema_key);
        assert_eq!(loaded.entity_pk, explicit.entity_pk);
        assert_ne!(loaded.entity_pk, address_target.entity_pk);
        let batch = super::load_change_records_by_ids(&read, &[explicit_change_id])
            .await
            .expect("explicit collision batch should fall back to its locator");
        assert_eq!(batch, vec![loaded]);
        let canonical = super::load_canonical_change_locator(&read, explicit_change_id)
            .await
            .expect("canonical locator read should succeed")
            .expect("explicit collision should retain a canonical locator");
        assert_eq!(canonical.commit_id, explicit_commit_id);
    }

    #[tokio::test]
    async fn out_of_range_address_shaped_explicit_id_falls_back_to_its_locator() {
        let storage = StorageAdapter::new(Memory::new());
        let addressable_commit_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_5678_0000_0000,
        ));
        let address_target = packed_commit_delta_fixtures()
            .into_iter()
            .next()
            .expect("fixture should exist");
        let mut writes = storage.new_write_set();
        let target_deltas =
            commit_delta_refs(addressable_commit_id, std::slice::from_ref(&address_target));
        let target_staged = stage_addressable_commit_deltas(&mut writes, &target_deltas, &[false])
            .expect("inline target should stage");
        stage_change_locators(&mut writes, &target_staged.locators);

        let explicit_change_id = super::addressable_change_id(addressable_commit_id, 1, 0)
            .expect("out-of-range segment should retain a valid direct shape");
        let explicit_commit_id = CommitId::for_test_label("out-of-range-explicit-commit");
        let mut explicit = packed_commit_delta_fixtures()
            .into_iter()
            .nth(1)
            .expect("second fixture should exist");
        explicit.change_id = explicit_change_id;
        let explicit_deltas =
            commit_delta_refs(explicit_commit_id, std::slice::from_ref(&explicit));
        let explicit_staged =
            stage_addressable_commit_deltas(&mut writes, &explicit_deltas, &[false])
                .expect("out-of-range explicit change should stage");
        assert_eq!(explicit_staged.locators.len(), 1);
        stage_change_locators(&mut writes, &explicit_staged.locators);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("out-of-range collision fixture should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("collision read should open");
        let loaded = load_change_record_by_id(&read, explicit_change_id)
            .await
            .expect("out-of-range explicit read should succeed")
            .expect("out-of-range explicit id should resolve through its locator");
        assert_eq!(loaded.change_id, explicit_change_id);
        assert_eq!(loaded.schema_key, explicit.schema_key);
        assert_eq!(loaded.entity_pk, explicit.entity_pk);
        assert_eq!(
            super::load_change_records_by_ids(&read, &[explicit_change_id])
                .await
                .expect("out-of-range batch should fall back to its locator"),
            vec![loaded],
        );
        let canonical = super::load_canonical_change_locator(&read, explicit_change_id)
            .await
            .expect("canonical locator read should succeed")
            .expect("out-of-range explicit id should retain a canonical locator");
        assert_eq!(canonical.commit_id, explicit_commit_id);
    }

    #[test]
    fn change_locator_codec_compacts_sequential_ids_and_round_trips_fallback_ids() {
        let sequential = CommitDeltaChangeLocator {
            change_id: ChangeId::new(uuid::Uuid::from_u128(
                0x0192_0000_0000_7000_8000_0000_0000_0101,
            )),
            commit_id: CommitId::new(uuid::Uuid::from_u128(
                0x0192_0000_0000_7000_8000_0000_0000_0100,
            )),
            segment_index: 2,
            ordinal: 7,
        };
        let encoded = super::encode_change_locator(sequential);
        assert_eq!(encoded.len(), 4);
        assert_eq!(
            super::decode_change_locator(sequential.change_id, &encoded).expect("decode locator"),
            sequential
        );

        let fallback = CommitDeltaChangeLocator {
            change_id: ChangeId::new(uuid::Uuid::from_u128(u128::MAX)),
            commit_id: CommitId::new(uuid::Uuid::from_u128(1)),
            segment_index: u32::MAX,
            ordinal: 127,
        };
        let encoded = super::encode_change_locator(fallback);
        assert_eq!(
            super::decode_change_locator(fallback.change_id, &encoded).expect("decode locator"),
            fallback
        );
    }

    #[tokio::test]
    async fn commit_local_authority_sorts_public_ids_and_treats_missing_manifest_as_empty() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("commit-local-authority");
        let mut fixtures = packed_commit_delta_fixtures()
            .into_iter()
            .take(3)
            .collect::<Vec<_>>();
        fixtures[0].change_id =
            ChangeId::parse("00000000-0000-0000-0000-000000000003").expect("valid change id");
        fixtures[1].change_id =
            ChangeId::parse("00000000-0000-0000-0000-000000000001").expect("valid change id");
        fixtures[2].change_id =
            ChangeId::parse("00000000-0000-0000-0000-000000000002").expect("valid change id");
        let deltas = commit_delta_refs(commit_id, &fixtures);
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("commit members should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit members should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let expected_ids = vec![
            fixtures[1].change_id,
            fixtures[2].change_id,
            fixtures[0].change_id,
        ];
        assert_eq!(
            load_commit_delta_change_ids(&read, commit_id)
                .await
                .expect("public membership should load"),
            expected_ids,
            "public membership is ordered by change id, not physical identity"
        );
        let members = scan_commit_delta_members(&read, commit_id)
            .await
            .expect("physical members should scan");
        assert!(members.windows(2).all(|pair| pair[0].0 < pair[1].0));
        assert!(
            load_commit_delta_change_ids(
                &read,
                CommitId::for_test_label("known-empty-without-manifest"),
            )
            .await
            .expect("a known empty commit has no manifest")
            .is_empty()
        );

        let topology_only_commit_id = CommitId::for_test_label("topology-only-authority");
        let mut writes = storage.new_write_set();
        stage_fixture_manifest(
            &mut writes,
            topology_only_commit_id,
            &CommitStateMutationInventory::default(),
        )
        .expect("topology-only commit-state authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("topology-only commit-state authority should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert!(
            load_commit_delta_change_ids(&read, topology_only_commit_id)
                .await
                .expect("topology-only authority has an empty mutation inventory")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn exact_packed_lookup_round_trips_columnar_base_coordinates() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("base-coordinate-owner");
        let base_commit_id = CommitId::for_test_label("base-coordinate-layout");
        let fixtures = packed_commit_delta_fixtures()
            .into_iter()
            .take(2)
            .collect::<Vec<_>>();
        let mut deltas = commit_delta_refs(commit_id, &fixtures);
        let coordinates = [
            TrackedStateBaseCoordinate {
                base_commit_id,
                group_index: 7,
                row_index: 41,
            },
            TrackedStateBaseCoordinate {
                base_commit_id,
                group_index: u32::MAX,
                row_index: u32::MAX,
            },
        ];
        deltas[0].base_coordinate = Some(coordinates[0]);
        deltas[1].base_coordinate = Some(coordinates[1]);

        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("coordinated deltas should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("coordinated deltas should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("coordinate read should open");
        let requests = fixtures
            .iter()
            .map(|fixture| (commit_id, fixture.key()))
            .collect::<Vec<_>>();
        let loaded = load_owned_commit_delta_entries(&read, &requests)
            .await
            .expect("exact coordinated lookup should succeed");
        assert_eq!(loaded.len(), coordinates.len());
        for (entry, coordinate) in loaded.into_iter().zip(coordinates) {
            assert_eq!(
                entry.expect("coordinated row should exist").base_coordinate,
                Some(coordinate)
            );
        }
    }

    #[tokio::test]
    async fn public_membership_dedupes_ids_but_payload_authority_rejects_conflicts() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("duplicate-change-id");
        let mut fixtures = packed_commit_delta_fixtures()
            .into_iter()
            .take(2)
            .collect::<Vec<_>>();
        fixtures[1].change_id = fixtures[0].change_id;
        let deltas = commit_delta_refs(commit_id, &fixtures);
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("distinct identities should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("corrupt duplicate-id fixture should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            load_commit_delta_change_ids(&read, commit_id)
                .await
                .expect("public commit membership should load"),
            vec![fixtures[0].change_id]
        );
        let error = scan_commit_delta_inventory(&read)
            .await
            .expect_err("global authority must reject duplicate change ids");
        assert!(error.to_string().contains("contains duplicate change id"));
        let error = scan_change_records_from_commit_deltas(&read)
            .await
            .expect_err("streaming authority must reject duplicate change ids");
        assert!(error.to_string().contains("contains duplicate change id"));
    }

    #[tokio::test]
    async fn selected_tombstones_may_share_one_source_change_id() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("duplicate-selected-tombstone");
        let mut fixtures = packed_commit_delta_fixtures()
            .into_iter()
            .take(2)
            .collect::<Vec<_>>();
        fixtures[0].deleted = true;
        fixtures[1].deleted = true;
        fixtures[1].change_id = fixtures[0].change_id;
        let mut deltas = commit_delta_refs(commit_id, &fixtures);
        for delta in &mut deltas {
            delta.authored = false;
        }
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("selected tombstones should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("selected tombstones should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            load_commit_delta_members_with_payloads(&read, commit_id)
                .await
                .expect("selected tombstones should remain identity-addressable")
                .len(),
            2
        );
        assert!(
            scan_change_records_from_commit_deltas(&read)
                .await
                .expect("selected tombstones should not conflict with public authority")
                .is_empty()
        );
        scan_commit_delta_inventory(&read)
            .await
            .expect("selected tombstones should be valid inventory members");
    }

    #[tokio::test]
    async fn global_inventory_rejects_orphan_and_noncontiguous_segments() {
        let orphan_storage = StorageAdapter::new(Memory::new());
        let orphan_commit = CommitId::for_test_label("orphan-segment");
        let orphan_fixture = packed_commit_delta_fixtures()
            .into_iter()
            .next()
            .expect("fixture should exist");
        let orphan_entry = EncodedLeafEntry {
            key: encode_key_ref(TrackedStateKeyRef {
                schema_key: &orphan_fixture.schema_key,
                file_id: orphan_fixture.file_id.as_deref(),
                entity_pk: &orphan_fixture.entity_pk,
            })
            .into(),
            value: encode_value_ref(TrackedStateIndexValueRef {
                change_id: orphan_fixture.change_id,
                commit_id: orphan_commit,
                deleted: orphan_fixture.deleted,
                created_at: orphan_fixture.created_at,
                updated_at: orphan_fixture.updated_at,
            })
            .into(),
        };
        let orphan_bytes = encode_commit_delta_segment_with_payloads(
            &[orphan_entry],
            &[CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::None,
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: true,
            }],
        );
        let mut writes = orphan_storage.new_write_set();
        writes.put(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            key(super::commit_delta_segment_key(orphan_commit, 0)
                .expect("segment key should encode")),
            value(orphan_bytes),
        );
        orphan_storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("orphan segment should commit");
        let read = orphan_storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("orphan read should open");
        let error = scan_commit_delta_inventory(&read)
            .await
            .expect_err("orphan segments must fail inventory");
        assert!(error.to_string().contains("found orphan segments"));
        let error = scan_change_records_from_commit_deltas(&read)
            .await
            .expect_err("orphan segments must fail streaming scan");
        assert!(error.to_string().contains("found orphan segments"));

        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("noncontiguous-segments");
        let fixtures = packed_commit_delta_fixtures();
        let deltas = commit_delta_refs(commit_id, &fixtures);
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("segmented commit should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("segmented commit should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("segment read should open");
        let original = super::get_one(
            &read,
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            super::commit_delta_segment_key(commit_id, 1).expect("segment key should encode"),
        )
        .await
        .expect("segment should load")
        .expect("middle segment should exist");
        drop(read);
        let mut writes = storage.new_write_set();
        writes.delete(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            key(super::commit_delta_segment_key(commit_id, 1).expect("segment key should encode")),
        );
        writes.put(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            key(super::commit_delta_segment_key(commit_id, 99).expect("segment key should encode")),
            value(original.to_vec()),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("noncontiguous segment fixture should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("inventory read should open");
        let error = scan_commit_delta_inventory(&read)
            .await
            .expect_err("noncontiguous physical suffixes must fail inventory");
        assert!(error.to_string().contains("missing segment 1"));
        let error = scan_change_records_from_commit_deltas(&read)
            .await
            .expect_err("noncontiguous physical suffixes must fail streaming scan");
        assert!(error.to_string().contains("missing segment 1"));
    }

    #[tokio::test]
    async fn global_inventory_hydrates_selected_payloads_from_canonical_authority() {
        let storage = StorageAdapter::new(Memory::new());
        let fixture = packed_commit_delta_fixtures()
            .into_iter()
            .next()
            .expect("fixture should exist");
        let first_commit = CommitId::for_test_label("shared-authority-first");
        let second_commit = CommitId::for_test_label("shared-authority-second");
        let shared_snapshot = r#"{"shared":true}"#;
        let first = commit_delta_ref(
            first_commit,
            &fixture,
            crate::json_store::JsonSlotRef::Inline(shared_snapshot),
            crate::json_store::JsonSlotRef::None,
            None,
        );
        let mut second = commit_delta_ref(
            second_commit,
            &fixture,
            crate::json_store::JsonSlotRef::Inline(shared_snapshot),
            crate::json_store::JsonSlotRef::None,
            None,
        );
        second.authored = false;
        let mut writes = storage.new_write_set();
        let locators =
            stage_commit_deltas(&mut writes, &[first]).expect("first owner should stage");
        stage_change_locators(&mut writes, &locators);
        stage_commit_deltas(&mut writes, &[second]).expect("second owner should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("shared authority should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("shared authority read should open");
        let selected_manifest = super::load_commit_delta_manifest(&read, second_commit)
            .await
            .expect("selected manifest should load")
            .expect("selected manifest should exist");
        let (_, selected_payloads) = decode_commit_delta_with_payloads(
            selected_manifest
                .inline_segment()
                .expect("one selected row should stay inline"),
            None,
        )
        .expect("selected segment should decode");
        assert_eq!(
            selected_payloads
                .payload_range(0)
                .expect("selected payload range"),
            &[super::COMMIT_DELTA_PAYLOAD_SELECTED_REF],
            "selected rows must persist only the canonical-reference tag"
        );
        assert_eq!(
            scan_commit_delta_inventory(&read)
                .await
                .expect("identical selected payload may be shared")
                .commits
                .len(),
            2
        );
        let changes = scan_change_records_from_commit_deltas(&read)
            .await
            .expect("streaming scan should deduplicate identical shared authority");
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].change_id, fixture.change_id);
        drop(read);

        let conflicting_commit = CommitId::for_test_label("shared-authority-conflict");
        let mut conflicting = commit_delta_ref(
            conflicting_commit,
            &fixture,
            crate::json_store::JsonSlotRef::Inline(r#"{"shared":false}"#),
            crate::json_store::JsonSlotRef::None,
            None,
        );
        conflicting.authored = false;
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &[conflicting]).expect("conflicting owner should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("conflicting authority fixture should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("conflicting authority read should open");
        let inventory = scan_commit_delta_inventory(&read)
            .await
            .expect("selected wire rows must ignore repeated caller payload bytes");
        assert_eq!(inventory.commits.len(), 3);
        let changes = scan_change_records_from_commit_deltas(&read)
            .await
            .expect("streaming scan must hydrate selected rows from canonical authority");
        assert_eq!(changes.len(), 1);
        assert_eq!(
            changes[0].snapshot,
            crate::json_store::JsonSlot::Inline(shared_snapshot.into())
        );
        assert_selected_direct_address_hydrates_inventory_gc_and_root_rebuild_paths().await;
    }

    async fn assert_selected_direct_address_hydrates_inventory_gc_and_root_rebuild_paths() {
        let storage = StorageAdapter::new(Memory::new());
        let mut fixture = packed_commit_delta_fixtures()
            .into_iter()
            .find(|fixture| !fixture.deleted)
            .expect("fixture should exist");
        let owner_commit = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_4321_0000_0000,
        ));
        let selected_commit = CommitId::for_test_label("selected-direct-address");
        let snapshot = r#"{"direct":true}"#;
        let owner = commit_delta_ref(
            owner_commit,
            &fixture,
            crate::json_store::JsonSlotRef::Inline(snapshot),
            crate::json_store::JsonSlotRef::None,
            None,
        );
        let mut writes = storage.new_write_set();
        let staged = stage_addressable_commit_deltas(&mut writes, &[owner], &[true])
            .expect("direct owner should stage");
        assert!(staged.locators.is_empty());
        fixture.change_id = staged.assigned_change_ids[0];

        let mut selected = commit_delta_ref(
            selected_commit,
            &fixture,
            crate::json_store::JsonSlotRef::None,
            crate::json_store::JsonSlotRef::None,
            None,
        );
        selected.authored = false;
        let selected_locators =
            stage_commit_deltas(&mut writes, &[selected]).expect("selected row should stage");
        assert_eq!(selected_locators.len(), 1);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("direct owner and selected reference should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("selected direct-address read should open");
        assert!(
            super::load_change_locator_by_id(&read, fixture.change_id)
                .await
                .expect("locator absence should read")
                .is_none(),
            "ordinary direct-addressed owners intentionally persist no locator row"
        );
        let owner = load_change_record_by_id(&read, fixture.change_id)
            .await
            .expect("direct owner read should succeed")
            .expect("direct owner should exist");
        assert_eq!(
            owner.snapshot,
            crate::json_store::JsonSlot::Inline(snapshot.into())
        );
        let selected_members = load_commit_delta_members_with_payloads(&read, selected_commit)
            .await
            .expect("root rebuild should hydrate the selected direct-address payload");
        assert_eq!(selected_members.len(), 1);
        assert_eq!(
            selected_members[0].change.snapshot,
            crate::json_store::JsonSlot::Inline(snapshot.into())
        );
        let inventory = scan_commit_delta_inventory(&read)
            .await
            .expect("GC inventory should hydrate the selected direct-address payload");
        assert_eq!(inventory.commits.len(), 2);
        assert_eq!(
            inventory.commits[&selected_commit].members[0]
                .change
                .snapshot,
            crate::json_store::JsonSlot::Inline(snapshot.into())
        );
        let changes = scan_change_records_from_commit_deltas(&read)
            .await
            .expect("canonical history should deduplicate selected direct authority");
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].change_id, fixture.change_id);
    }

    #[tokio::test]
    async fn selected_source_alias_preserves_exact_members_inventory_and_canonical_history() {
        let storage = StorageAdapter::new(Memory::new());
        let source_commit = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_7777_0000_0000,
        ));
        let alias_commit = CommitId::for_test_label("selected-source-alias");
        let mut fixtures = packed_commit_delta_fixtures()
            .into_iter()
            .take(3)
            .collect::<Vec<_>>();

        let mut writes = storage.new_write_set();
        let source_deltas = commit_delta_refs(source_commit, &fixtures[..2]);
        let source_stage = stage_addressable_commit_deltas(
            &mut writes,
            &source_deltas,
            &vec![true; source_deltas.len()],
        )
        .expect("source commit should stage direct addresses");
        drop(source_deltas);
        for (fixture, change_id) in fixtures[..2]
            .iter_mut()
            .zip(source_stage.assigned_change_ids)
        {
            fixture.change_id = change_id;
        }

        let overlay = commit_delta_refs(alias_commit, &fixtures[2..]);
        stage_addressable_commit_deltas_with_selected_source(
            &mut writes,
            &overlay,
            &[false],
            source_commit,
        )
        .expect("source alias should stage one disjoint local overlay");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("source and alias commits should publish atomically");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("source alias read should open");
        let keys = fixtures
            .iter()
            .map(CommitDeltaFixture::key)
            .collect::<Vec<_>>();
        let values = load_commit_delta_values_for_test(&read, alias_commit, &keys)
            .await
            .expect("alias exact values should load");
        assert!(values.iter().all(Option::is_some));
        assert!(
            values
                .iter()
                .flatten()
                .all(|value| value.commit_id == alias_commit)
        );

        let missing_key = TrackedStateKey {
            schema_key: fixtures[0].schema_key.clone(),
            file_id: fixtures[0].file_id.clone(),
            entity_pk: EntityPk::single("missing-cascade-member"),
        };
        let missing_requests = (0..2_048)
            .map(|_| (alias_commit, missing_key.clone()))
            .collect::<Vec<_>>();
        let missing = load_owned_commit_delta_entries(&read, &missing_requests)
            .await
            .expect("alias misses should remain available to cascade fallback");
        assert!(missing.iter().all(Option::is_none));

        let members = load_commit_delta_members_with_payloads(&read, alias_commit)
            .await
            .expect("alias root-rebuild members should load");
        assert_eq!(members.len(), 3);
        assert_eq!(members.iter().filter(|member| member.authored).count(), 1);
        assert_eq!(members.iter().filter(|member| !member.authored).count(), 2);

        let inventory = scan_commit_delta_inventory(&read)
            .await
            .expect("alias GC inventory should load");
        assert_eq!(
            inventory.commits[&alias_commit].selected_source_commit_id,
            Some(source_commit)
        );
        assert_eq!(inventory.commits[&alias_commit].members.len(), 3);
        let canonical = scan_change_records_from_commit_deltas(&read)
            .await
            .expect("alias history should retain one canonical source authority");
        assert_eq!(canonical.len(), 3);
    }

    fn decoded_commit_delta_rows(
        batch: &DecodedCommitDeltaBatch,
    ) -> Vec<(TrackedStateKey, TrackedStateIndexValue)> {
        batch
            .iter()
            .map(|row| {
                let key = row.key_ref();
                (
                    TrackedStateKey {
                        schema_key: key.schema_key.to_owned(),
                        file_id: key.file_id.map(str::to_owned),
                        entity_pk: key.entity_pk.clone(),
                    },
                    row.value().clone(),
                )
            })
            .collect()
    }

    #[test]
    fn large_chunk_batch_stages_two_shared_arenas() {
        let chunk_count = 4_096;
        let mut data_arena = Vec::with_capacity(chunk_count * 64);
        let mut descriptors = Vec::with_capacity(chunk_count);
        for index in 0..chunk_count {
            let data_start = data_arena.len();
            data_arena.extend_from_slice(&(index as u64).to_be_bytes());
            data_arena.resize(data_start + 64, (index % 251) as u8);
            descriptors.push(PendingChunk {
                hash: hash_bytes(&data_arena[data_start..data_start + 64]),
                data_start,
                data_len: 64,
            });
        }
        let chunks = PendingChunkBatch::from_parts(Bytes::from(data_arena), descriptors);
        let mut writes = StorageWriteSet::new();
        let mut overlay = TrackedStateChunkOverlay::new();
        overlay.stage_chunks(&mut writes, &chunks);

        let arena = writes.arena_stats();
        assert_eq!(arena.put_descriptors, chunk_count);
        assert_eq!(arena.key_shared_buffers, 1);
        assert_eq!(arena.value_shared_buffers, 1);
        assert_eq!(arena.key_inline_allocations, 0);
        assert_eq!(arena.value_inline_allocations, 0);

        let first_chunk = chunks.chunks()[0];
        let first = overlay
            .staged_chunk(&first_chunk.hash)
            .expect("first staged chunk");
        let arena_start = first.as_ptr() as usize;
        for chunk in chunks.chunks() {
            let staged = overlay
                .staged_chunk(&chunk.hash)
                .expect("every chunk should be retained by the overlay");
            assert_eq!(
                staged.as_ptr() as usize,
                arena_start + chunk.data_start,
                "overlay chunks must be slices of one contiguous value arena"
            );
            assert_eq!(staged, chunks.chunk_bytes(*chunk));
        }
    }

    #[tokio::test]
    async fn packed_commit_deltas_preserve_point_and_schema_replay() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("packed-delta-commit");
        let fixtures = packed_commit_delta_fixtures();
        let deltas = fixtures
            .iter()
            .map(|fixture| {
                commit_delta_ref(
                    commit_id,
                    fixture,
                    crate::json_store::JsonSlotRef::None,
                    crate::json_store::JsonSlotRef::None,
                    None,
                )
            })
            .collect::<Vec<_>>();
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("packed deltas should stage");
        assert_eq!(
            writes.stats().staged_puts,
            5,
            "generic history keeps 300 compact rows in three read-friendly segments plus its commit-state authority and immutable seal"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("packed deltas should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let missing = TrackedStateKey {
            schema_key: "alpha".to_string(),
            file_id: None,
            entity_pk: EntityPk::single("not-present"),
        };
        let point_keys = vec![
            fixtures[0].key(),
            fixtures[255].key(),
            missing,
            fixtures[0].key(),
        ];
        let point_values = load_commit_delta_values_for_test(&read, commit_id, &point_keys)
            .await
            .expect("point replay should load packed deltas");
        assert_eq!(
            point_values,
            vec![
                Some(fixtures[0].value(commit_id)),
                Some(fixtures[255].value(commit_id)),
                None,
                Some(fixtures[0].value(commit_id)),
            ]
        );

        let alpha = scan_commit_delta_values(&read, commit_id, &["alpha".to_string()])
            .await
            .expect("schema replay should scan packed deltas");
        assert_eq!(alpha.len(), 150);
        assert!(alpha.iter().all(|row| row.key_ref().schema_key == "alpha"));
        let alpha_keys = alpha
            .iter()
            .map(|row| row.encoded_key())
            .collect::<Vec<_>>();
        assert!(alpha_keys.windows(2).all(|pair| pair[0] < pair[1]));

        let all = scan_commit_delta_values(&read, commit_id, &[])
            .await
            .expect("unconstrained replay should scan packed deltas");
        assert_eq!(all.len(), fixtures.len());
        let all_keys = all.iter().map(|row| row.encoded_key()).collect::<Vec<_>>();
        assert!(all_keys.windows(2).all(|pair| pair[0] < pair[1]));

        let alpha_members = super::load_commit_delta_members_with_payloads_for_schemas(
            &read,
            commit_id,
            &["alpha".to_string()],
            usize::MAX,
        )
        .await
        .expect("schema-routed payload scan should load packed deltas")
        .expect("unbounded schema-routed payload scan should be accepted");
        assert_eq!(alpha_members.len(), 150);
        assert!(
            alpha_members
                .iter()
                .all(|member| member.key.schema_key == "alpha")
        );
        assert!(
            alpha_members
                .windows(2)
                .all(|pair| pair[0].key < pair[1].key)
        );
        assert!(
            super::load_commit_delta_members_with_payloads_for_schemas(
                &read,
                commit_id,
                &["alpha".to_string()],
                0,
            )
            .await
            .expect("bounded payload scan should remain valid")
            .is_none(),
            "payload scans above the segment budget must defer to the streaming path"
        );
    }

    #[tokio::test]
    async fn payload_authoritative_scan_preserves_many_byte_bounded_segments() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("large-payload-packed-delta-commit");
        let fixtures = (0..1_000)
            .map(|index| CommitDeltaFixture {
                schema_key: "working_diff_row".to_string(),
                file_id: None,
                entity_pk: EntityPk::single(format!("entity-{index:04}")),
                change_id: ChangeId::for_test_label(&format!(
                    "large-payload-packed-delta-change-{index}"
                )),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(index.into()),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy((index + 1).into()),
            })
            .collect::<Vec<_>>();
        let snapshots = (0..fixtures.len())
            .map(|index| format!(r#"{{"id":"entity-{index:04}","value":"baseline"}}"#))
            .collect::<Vec<_>>();
        let deltas = fixtures
            .iter()
            .zip(&snapshots)
            .map(|(fixture, snapshot)| {
                commit_delta_ref(
                    commit_id,
                    fixture,
                    crate::json_store::JsonSlotRef::Inline(snapshot),
                    crate::json_store::JsonSlotRef::None,
                    None,
                )
            })
            .collect::<Vec<_>>();
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("large payload deltas should stage");
        assert!(
            writes.stats().staged_puts > 4,
            "the fixture must cross several byte-bounded segments"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("large payload deltas should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let members = load_commit_delta_members_with_payloads(&read, commit_id)
            .await
            .expect("large payload delta scan should load");
        assert_eq!(members.len(), fixtures.len());
        assert!(members.windows(2).all(|pair| pair[0].key < pair[1].key));
        let last_change = members
            .iter()
            .find(|member| member.change.change_id == fixtures[999].change_id)
            .map(|member| &member.change)
            .expect("last segment payload should be present");
        assert_eq!(
            last_change.snapshot,
            crate::json_store::JsonSlot::Inline(snapshots[999].clone().into_boxed_str())
        );
    }

    #[tokio::test]
    async fn oversized_candidate_sidecars_split_before_rejecting_valid_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("oversized-candidate-sidecar");
        let fixtures = (0..4)
            .map(|index| CommitDeltaFixture {
                schema_key: "large-sidecar".to_string(),
                file_id: None,
                entity_pk: EntityPk::single(format!("entity-{index}")),
                change_id: ChangeId::for_test_label(&format!(
                    "oversized-candidate-sidecar-change-{index}"
                )),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(index),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(index + 1),
            })
            .collect::<Vec<_>>();
        let snapshots = (0..fixtures.len())
            .map(|index| {
                format!(
                    r#"{{"index":{index},"value":"{}"}}"#,
                    "x".repeat(300 * 1024)
                )
            })
            .collect::<Vec<_>>();
        let deltas = fixtures
            .iter()
            .zip(&snapshots)
            .map(|(fixture, snapshot)| {
                commit_delta_ref(
                    commit_id,
                    fixture,
                    crate::json_store::JsonSlotRef::Inline(snapshot),
                    crate::json_store::JsonSlotRef::None,
                    None,
                )
            })
            .collect::<Vec<_>>();

        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas)
            .expect("individually valid rows should split below the sidecar limit");
        assert_eq!(
            writes.stats().staged_puts,
            4,
            "the oversized four-row candidate should become two segments plus its commit-state authority and immutable seal"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("split sidecars should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("split sidecar read should open");
        let members = load_commit_delta_members_with_payloads(&read, commit_id)
            .await
            .expect("every split payload should remain readable");
        assert_eq!(members.len(), fixtures.len());
        for (member, snapshot) in members.iter().zip(&snapshots) {
            assert_eq!(
                member.change.snapshot,
                crate::json_store::JsonSlot::Inline(snapshot.clone().into_boxed_str())
            );
        }
    }

    #[tokio::test]
    async fn index_only_point_replay_does_not_decode_payload_sidecars() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("index-only-skips-sidecar");
        let fixture = packed_commit_delta_fixtures()
            .into_iter()
            .next()
            .expect("fixture should exist");
        let entry = EncodedLeafEntry {
            key: encode_key_ref(TrackedStateKeyRef {
                schema_key: &fixture.schema_key,
                file_id: fixture.file_id.as_deref(),
                entity_pk: &fixture.entity_pk,
            })
            .into(),
            value: encode_value_ref(TrackedStateIndexValueRef {
                change_id: fixture.change_id,
                commit_id,
                deleted: fixture.deleted,
                created_at: fixture.created_at,
                updated_at: fixture.updated_at,
            })
            .into(),
        };
        let snapshot = format!(r#"{{"value":"{}"}}"#, "z".repeat(8 * 1024));
        let mut segment = encode_commit_delta_segment_with_payloads(
            std::slice::from_ref(&entry),
            &[CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::Inline(&snapshot),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: true,
            }],
        );
        segment.pop();
        let mut writes = storage.new_write_set();
        let delta_manifest = CommitDeltaManifest {
            selected_source_commit_id: None,
            member_count: 1,
            selection_fingerprint: [0; 32],
            direct_segment_row_counts: Vec::new(),
            single_partition: Some(super::CommitDeltaReplacementScope {
                schema_key: fixture.schema_key.clone(),
                file_id: fixture.file_id.clone(),
            }),
            lifecycle_summary: None,
            replacement_generation: None,
            replacement_parts: None,
            inline_segment: segment,
            segments: Vec::new(),
        };
        stage_fixture_manifest(
            &mut writes,
            commit_id,
            &super::commit_state_inventory_from_delta_manifest(&delta_manifest),
        )
        .expect("commit-state authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("corrupt sidecar fixture should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("index-only read should open");
        assert_eq!(
            load_commit_delta_values_encoded(
                &read,
                commit_id,
                &[Bytes::copy_from_slice(&entry.key)],
            )
            .await
            .expect("index-only replay should ignore the payload sidecar"),
            vec![Some(fixture.value(commit_id))]
        );
        let error = load_commit_delta_members_with_payloads(&read, commit_id)
            .await
            .expect_err("payload-aware replay must still validate the corrupt sidecar");
        assert!(
            error
                .to_string()
                .contains("compressed commit_delta sidecar failed to decode"),
            "unexpected payload error: {error}"
        );
    }

    #[test]
    fn indexed_payload_point_decoder_skips_unrequested_records() {
        let commit_id = CommitId::for_test_label("indexed-payload-point");
        let fixtures = (0..3)
            .map(|index| CommitDeltaFixture {
                schema_key: "indexed".to_string(),
                file_id: None,
                entity_pk: EntityPk::single(format!("entity-{index}")),
                change_id: ChangeId::for_test_label(&format!("indexed-payload-change-{index}")),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(index.into()),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy((index + 1).into()),
            })
            .collect::<Vec<_>>();
        let snapshots = [
            r#"{"value":"first"}"#,
            r#"{"value":"sparse"}"#,
            r#"{"value":"last"}"#,
        ];
        let entries = fixtures
            .iter()
            .map(|fixture| EncodedLeafEntry {
                key: encode_key_ref(TrackedStateKeyRef {
                    schema_key: &fixture.schema_key,
                    file_id: fixture.file_id.as_deref(),
                    entity_pk: &fixture.entity_pk,
                })
                .into(),
                value: encode_value_ref(TrackedStateIndexValueRef {
                    change_id: fixture.change_id,
                    commit_id,
                    deleted: fixture.deleted,
                    created_at: fixture.created_at,
                    updated_at: fixture.updated_at,
                })
                .into(),
            })
            .collect::<Vec<_>>();
        let payloads = [
            CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::Inline(snapshots[0]),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: Some("first"),
                base_coordinate: None,
                authored: true,
            },
            CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::Inline(snapshots[1]),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: true,
            },
            CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::Inline(snapshots[2]),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: Some("last"),
                base_coordinate: None,
                authored: true,
            },
        ];
        let mut encoded = encode_commit_delta_segment_with_raw_sidecar(&entries, &payloads);

        let corrupt_range = {
            let (_, index) =
                decode_commit_delta_with_payloads(&encoded, None).expect("segment should decode");
            assert_eq!(index.len(), 3);
            assert_eq!(
                index
                    .decode(0)
                    .expect("first payload should decode")
                    .authored_payload()
                    .snapshot,
                crate::json_store::JsonSlot::Inline(snapshots[0].into())
            );
            assert_eq!(
                index
                    .decode(1)
                    .expect("sparse payload should decode")
                    .authored_payload()
                    .snapshot,
                crate::json_store::JsonSlot::Inline(snapshots[1].into()),
                "every commit member must carry an authoritative payload"
            );
            let range = index
                .payload_range(2)
                .expect("last payload should have an indexed range");
            let start = range.as_ptr() as usize - encoded.as_ptr() as usize;
            start..start + range.len()
        };
        encoded[corrupt_range].fill(u8::MAX);

        let (_, index) = decode_commit_delta_with_payloads(&encoded, None)
            .expect("valid directory should not eagerly decode payload records");
        assert_eq!(
            index
                .decode(0)
                .expect("an uncorrupted requested payload should still decode")
                .authored_payload()
                .origin_key
                .as_deref(),
            Some("first"),
            "point decoding must not touch the corrupt unrequested payload"
        );
        let error = index
            .decode(2)
            .expect_err("a corrupt requested payload must fail");
        assert!(
            error
                .to_string()
                .contains("inline commit_delta payload is not UTF-8")
                || error.to_string().contains("invalid payload tag")
                || error.to_string().contains(
                    "failed to decode tracked_state indexed authored commit_delta payload"
                ),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn indexed_payload_kinds_round_trip_columnar_base_coordinates() {
        let commit_id = CommitId::for_test_label("coordinate-codec-owner");
        let base_commit_id = CommitId::for_test_label("coordinate-codec-base");
        let fixtures = packed_commit_delta_fixtures()
            .into_iter()
            .filter(|fixture| fixture.schema_key == "alpha")
            .take(3)
            .collect::<Vec<_>>();
        let entries = fixtures
            .iter()
            .map(|fixture| EncodedLeafEntry {
                key: encode_key_ref(TrackedStateKeyRef {
                    schema_key: &fixture.schema_key,
                    file_id: fixture.file_id.as_deref(),
                    entity_pk: &fixture.entity_pk,
                })
                .into(),
                value: encode_value_ref(TrackedStateIndexValueRef {
                    change_id: fixture.change_id,
                    commit_id,
                    deleted: fixture.deleted,
                    created_at: fixture.created_at,
                    updated_at: fixture.updated_at,
                })
                .into(),
            })
            .collect::<Vec<_>>();
        let coordinates = [
            TrackedStateBaseCoordinate {
                base_commit_id,
                group_index: 1,
                row_index: 2,
            },
            TrackedStateBaseCoordinate {
                base_commit_id,
                group_index: 3,
                row_index: 4,
            },
            TrackedStateBaseCoordinate {
                base_commit_id,
                group_index: 5,
                row_index: 6,
            },
        ];
        let payloads = [
            CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::Inline(r#"{"authored":true}"#),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: Some(coordinates[0]),
                authored: true,
            },
            CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::None,
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: Some(coordinates[1]),
                authored: false,
            },
            CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::Inline(r#"{"authored":true}"#),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: Some(coordinates[2]),
                authored: true,
            },
        ];
        let encoded = encode_commit_delta_segment_with_raw_sidecar(&entries, &payloads);
        let (_, decoded) =
            decode_commit_delta_with_payloads(&encoded, None).expect("coordinates should decode");

        assert_eq!(
            decoded
                .decode(0)
                .expect("authored coordinate should decode")
                .authored_payload()
                .base_coordinate,
            Some(coordinates[0])
        );
        assert!(matches!(
            decoded.decode(1).expect("selected coordinate should decode"),
            super::CommitDeltaPayload::SelectedRef(Some(coordinate))
                if coordinate == coordinates[1]
        ));
        assert_eq!(
            decoded
                .decode(2)
                .expect("authored coordinate should decode")
                .authored_payload()
                .base_coordinate,
            Some(coordinates[2])
        );
    }

    #[test]
    fn compressed_payload_sidecar_roundtrips_and_rejects_corruption() {
        let commit_id = CommitId::for_test_label("compressed-payload-sidecar");
        let fixtures = packed_commit_delta_fixtures()
            .into_iter()
            .take(2)
            .collect::<Vec<_>>();
        let entries = fixtures
            .iter()
            .map(|fixture| EncodedLeafEntry {
                key: encode_key_ref(TrackedStateKeyRef {
                    schema_key: &fixture.schema_key,
                    file_id: fixture.file_id.as_deref(),
                    entity_pk: &fixture.entity_pk,
                })
                .into(),
                value: encode_value_ref(TrackedStateIndexValueRef {
                    change_id: fixture.change_id,
                    commit_id,
                    deleted: fixture.deleted,
                    created_at: fixture.created_at,
                    updated_at: fixture.updated_at,
                })
                .into(),
            })
            .collect::<Vec<_>>();
        let first_snapshot = format!(r#"{{"value":"{}"}}"#, "a".repeat(8 * 1024));
        let second_snapshot = format!(r#"{{"value":"{}"}}"#, "b".repeat(8 * 1024));
        let payloads = [
            CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::Inline(&first_snapshot),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: true,
            },
            CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::Inline(&second_snapshot),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: true,
            },
        ];
        let encoded = encode_commit_delta_segment_with_payloads(&entries, &payloads);
        let leaf_len = usize::try_from(u32::from_be_bytes(
            encoded[COMMIT_DELTA_FORMAT_MAGIC.len()..COMMIT_DELTA_FORMAT_MAGIC.len() + 4]
                .try_into()
                .expect("fixed leaf length"),
        ))
        .expect("u32 fits usize");
        let sidecar_header = COMMIT_DELTA_FORMAT_MAGIC.len() + 4 + leaf_len;
        assert_eq!(
            encoded[sidecar_header],
            super::COMMIT_DELTA_SIDECAR_AUTHORED_INLINE_ZSTD,
            "repetitive authored-inline payload columns should use zstd"
        );

        let (_, decoded) =
            decode_commit_delta_with_payloads(&encoded, None).expect("sidecar should decode");
        assert_eq!(
            decoded.decode(0).unwrap().authored_payload().snapshot,
            crate::json_store::JsonSlot::Inline(first_snapshot.into_boxed_str())
        );
        assert_eq!(
            decoded.decode(1).unwrap().authored_payload().snapshot,
            crate::json_store::JsonSlot::Inline(second_snapshot.into_boxed_str())
        );

        let truncated = &encoded[..encoded.len() - 1];
        let error = decode_commit_delta_with_payloads(truncated, None)
            .expect_err("truncated zstd sidecar must fail");
        assert!(
            error
                .to_string()
                .contains("compressed commit_delta sidecar failed to decode"),
            "unexpected error: {error}"
        );

        let mut oversized = encoded;
        oversized[sidecar_header + 1..sidecar_header + 5]
            .copy_from_slice(&((64_u32 * 1024 * 1024) + 1).to_be_bytes());
        let error = decode_commit_delta_with_payloads(&oversized, None)
            .expect_err("oversized decoded sidecar must fail before allocation");
        assert!(
            error.to_string().contains("invalid uncompressed length"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn tiny_payload_sidecar_keeps_raw_fallback() {
        let fixture = packed_commit_delta_fixtures()
            .into_iter()
            .next()
            .expect("fixture should exist");
        let entry = EncodedLeafEntry {
            key: encode_key_ref(TrackedStateKeyRef {
                schema_key: &fixture.schema_key,
                file_id: fixture.file_id.as_deref(),
                entity_pk: &fixture.entity_pk,
            })
            .into(),
            value: encode_value_ref(TrackedStateIndexValueRef {
                change_id: fixture.change_id,
                commit_id: CommitId::for_test_label("raw-payload-sidecar"),
                deleted: fixture.deleted,
                created_at: fixture.created_at,
                updated_at: fixture.updated_at,
            })
            .into(),
        };
        let encoded = encode_commit_delta_segment_with_payloads(
            &[entry],
            &[CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::None,
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: true,
            }],
        );
        let leaf_len = usize::try_from(u32::from_be_bytes(
            encoded[COMMIT_DELTA_FORMAT_MAGIC.len()..COMMIT_DELTA_FORMAT_MAGIC.len() + 4]
                .try_into()
                .expect("fixed leaf length"),
        ))
        .expect("u32 fits usize");
        assert_eq!(
            encoded[COMMIT_DELTA_FORMAT_MAGIC.len() + 4 + leaf_len],
            super::COMMIT_DELTA_SIDECAR_RAW,
            "compression must not expand tiny sidecars"
        );
        decode_commit_delta_with_payloads(&encoded, None).expect("raw fallback should decode");
    }

    #[test]
    fn packed_commit_members_reject_empty_authoritative_payload_ranges() {
        let commit_id = CommitId::for_test_label("missing-authoritative-payload");
        let fixtures = packed_commit_delta_fixtures()
            .into_iter()
            .take(2)
            .collect::<Vec<_>>();
        let entries = fixtures
            .iter()
            .map(|fixture| EncodedLeafEntry {
                key: encode_key_ref(TrackedStateKeyRef {
                    schema_key: &fixture.schema_key,
                    file_id: fixture.file_id.as_deref(),
                    entity_pk: &fixture.entity_pk,
                })
                .into(),
                value: encode_value_ref(TrackedStateIndexValueRef {
                    change_id: fixture.change_id,
                    commit_id,
                    deleted: fixture.deleted,
                    created_at: fixture.created_at,
                    updated_at: fixture.updated_at,
                })
                .into(),
            })
            .collect::<Vec<_>>();
        let payloads = [
            CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::None,
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: true,
            },
            CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::Inline(r#"{"second":true}"#),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: true,
            },
        ];
        let mut encoded = encode_commit_delta_segment_with_raw_sidecar(&entries, &payloads);
        let leaf_len = usize::try_from(u32::from_be_bytes(
            encoded[COMMIT_DELTA_FORMAT_MAGIC.len()..COMMIT_DELTA_FORMAT_MAGIC.len() + 4]
                .try_into()
                .expect("fixed leaf length"),
        ))
        .expect("u32 fits usize");
        let offsets_start = COMMIT_DELTA_FORMAT_MAGIC.len() + 4 + leaf_len + 1 + 4 + 4;
        encoded[offsets_start + 4..offsets_start + 8].copy_from_slice(&0_u32.to_be_bytes());

        let (_, payloads) = decode_commit_delta_with_payloads(&encoded, None)
            .expect("an empty member range can retain a structurally valid directory");
        let error = payloads
            .decode(0)
            .expect_err("every physical member must carry a payload");
        assert!(
            error
                .to_string()
                .contains("missing its authoritative payload")
        );
    }

    #[tokio::test]
    async fn indexed_payload_points_preserve_large_snapshots_null_rows_and_tombstones() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("indexed-large-and-tombstone");
        let large_snapshot = format!(r#"{{"payload":"{}"}}"#, "x".repeat(64 * 1024));
        let fixtures = [
            CommitDeltaFixture {
                schema_key: "indexed".to_string(),
                file_id: None,
                entity_pk: EntityPk::single("large"),
                change_id: ChangeId::for_test_label("indexed-large-change"),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
            },
            CommitDeltaFixture {
                schema_key: "indexed".to_string(),
                file_id: None,
                entity_pk: EntityPk::single("sparse"),
                change_id: ChangeId::for_test_label("indexed-sparse-change"),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(3),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(4),
            },
            CommitDeltaFixture {
                schema_key: "indexed".to_string(),
                file_id: None,
                entity_pk: EntityPk::single("tombstone"),
                change_id: ChangeId::for_test_label("indexed-tombstone-change"),
                deleted: true,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(5),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(6),
            },
        ];
        let deltas = [
            commit_delta_ref(
                commit_id,
                &fixtures[0],
                crate::json_store::JsonSlotRef::Inline(&large_snapshot),
                crate::json_store::JsonSlotRef::None,
                Some("large"),
            ),
            commit_delta_ref(
                commit_id,
                &fixtures[1],
                crate::json_store::JsonSlotRef::None,
                crate::json_store::JsonSlotRef::None,
                None,
            ),
            commit_delta_ref(
                commit_id,
                &fixtures[2],
                crate::json_store::JsonSlotRef::None,
                crate::json_store::JsonSlotRef::None,
                Some("tombstone"),
            ),
        ];
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("indexed payload deltas should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("indexed payload deltas should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let keys = fixtures
            .iter()
            .map(CommitDeltaFixture::key)
            .collect::<Vec<_>>();
        let records = load_commit_delta_change_records(&read, commit_id, &keys)
            .await
            .expect("indexed change-record points should load");
        assert_eq!(
            records[0].as_ref().map(|record| record.snapshot.clone()),
            Some(crate::json_store::JsonSlot::Inline(
                large_snapshot.into_boxed_str()
            ))
        );
        assert!(
            records[1]
                .as_ref()
                .is_some_and(|record| record.snapshot == crate::json_store::JsonSlot::None)
        );
        assert!(
            records[2]
                .as_ref()
                .is_some_and(|record| record.snapshot == crate::json_store::JsonSlot::None)
        );
    }

    #[test]
    fn indexed_payload_directory_rejects_old_truncated_and_invalid_offsets() {
        let fixture = packed_commit_delta_fixtures()
            .into_iter()
            .next()
            .expect("fixture should exist");
        let entry = EncodedLeafEntry {
            key: encode_key_ref(TrackedStateKeyRef {
                schema_key: &fixture.schema_key,
                file_id: fixture.file_id.as_deref(),
                entity_pk: &fixture.entity_pk,
            })
            .into(),
            value: encode_value_ref(TrackedStateIndexValueRef {
                change_id: fixture.change_id,
                commit_id: CommitId::for_test_label("indexed-corruption"),
                deleted: fixture.deleted,
                created_at: fixture.created_at,
                updated_at: fixture.updated_at,
            })
            .into(),
        };
        let encoded = encode_commit_delta_segment_with_raw_sidecar(
            &[entry],
            &[CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::Inline(r#"{"ok":true}"#),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: true,
            }],
        );

        let mut old = encoded.clone();
        old[..COMMIT_DELTA_FORMAT_MAGIC.len()].copy_from_slice(b"LXCD09");
        let error = decode_commit_delta_with_payloads(&old, None)
            .expect_err("older commit-delta segments must be rejected");
        assert!(
            error
                .to_string()
                .contains("unsupported format; recreate the repository")
        );

        let leaf_len = usize::try_from(u32::from_be_bytes(
            encoded[COMMIT_DELTA_FORMAT_MAGIC.len()..COMMIT_DELTA_FORMAT_MAGIC.len() + 4]
                .try_into()
                .expect("fixed leaf length"),
        ))
        .expect("u32 fits usize");
        let payload_header = COMMIT_DELTA_FORMAT_MAGIC.len() + 4 + leaf_len;
        let truncated_sidecar_len = 4 + 7;
        let mut truncated = encoded[..payload_header + 1 + 4 + truncated_sidecar_len].to_vec();
        truncated[payload_header + 1..payload_header + 5]
            .copy_from_slice(&(truncated_sidecar_len as u32).to_be_bytes());
        let error = decode_commit_delta_with_payloads(&truncated, None)
            .expect_err("a truncated two-offset directory must fail");
        assert!(
            error.to_string().contains("payload directory is truncated"),
            "unexpected error: {error}"
        );

        let mut invalid_offset = encoded;
        let terminal_offset = payload_header + 1 + 4 + 4 + 4;
        invalid_offset[terminal_offset..terminal_offset + 4]
            .copy_from_slice(&u32::MAX.to_be_bytes());
        let error = decode_commit_delta_with_payloads(&invalid_offset, None)
            .expect_err("an out-of-bounds terminal offset must fail");
        assert!(
            error
                .to_string()
                .contains("payload offset is out of bounds"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn owned_delta_rows_preserve_cross_commit_order_missing_and_tombstones() {
        let storage = StorageAdapter::new(Memory::new());
        let first_commit = CommitId::for_test_label("owned-row-first-commit");
        let second_commit = CommitId::for_test_label("owned-row-second-commit");
        let fixtures = packed_commit_delta_fixtures();
        let snapshots = (0..fixtures.len())
            .map(|index| format!(r#"{{"id":"first-{index:04}"}}"#))
            .collect::<Vec<_>>();
        let first_deltas = fixtures
            .iter()
            .zip(&snapshots)
            .map(|(fixture, snapshot)| {
                commit_delta_ref(
                    first_commit,
                    fixture,
                    if fixture.deleted {
                        crate::json_store::JsonSlotRef::None
                    } else {
                        crate::json_store::JsonSlotRef::Inline(snapshot)
                    },
                    crate::json_store::JsonSlotRef::None,
                    None,
                )
            })
            .collect::<Vec<_>>();
        let second_fixture = CommitDeltaFixture {
            schema_key: "beta".to_string(),
            file_id: Some("second-file".to_string()),
            entity_pk: EntityPk::single("second-entity"),
            change_id: ChangeId::for_test_label("owned-row-second-change"),
            deleted: false,
            created_at: LixTimestamp::from_unix_millis_utc_lossy(400),
            updated_at: LixTimestamp::from_unix_millis_utc_lossy(401),
        };
        let second_snapshot = r#"{"id":"second"}"#;
        let second_delta = commit_delta_ref(
            second_commit,
            &second_fixture,
            crate::json_store::JsonSlotRef::Inline(second_snapshot),
            crate::json_store::JsonSlotRef::None,
            None,
        );

        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &first_deltas).expect("first owner should stage");
        stage_commit_deltas(&mut writes, &[second_delta]).expect("second owner should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("owner deltas should commit");

        let missing = TrackedStateKey {
            schema_key: "alpha".to_string(),
            file_id: None,
            entity_pk: EntityPk::single("missing"),
        };
        let owned_keys = vec![
            (second_commit, second_fixture.key()),
            (first_commit, fixtures[0].key()),
            (first_commit, missing),
            (first_commit, fixtures[255].key()),
            (second_commit, second_fixture.key()),
        ];
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let rows = load_owned_commit_delta_entries(&read, &owned_keys)
            .await
            .expect("owned rows should load directly");

        assert_eq!(rows.len(), owned_keys.len());
        assert_eq!(
            rows[0]
                .as_ref()
                .map(|row| row.change_record.snapshot.as_ref_slot()),
            Some(crate::json_store::JsonSlotRef::Inline(second_snapshot))
        );
        assert!(
            rows[1]
                .as_ref()
                .is_some_and(|row| row.value.deleted && row.change_record.snapshot.is_none())
        );
        assert!(rows[2].is_none());
        assert_eq!(
            rows[3]
                .as_ref()
                .map(|row| row.change_record.snapshot.as_ref_slot()),
            Some(crate::json_store::JsonSlotRef::Inline(
                snapshots[255].as_str()
            ))
        );
        assert_eq!(
            rows[4].as_ref().map(|row| &row.change_record),
            rows[0].as_ref().map(|row| &row.change_record)
        );
    }

    #[tokio::test]
    async fn shallow_point_replay_reads_one_immutable_semantic_authority() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("one-read-shallow-point-replay");
        let fixture = packed_commit_delta_fixtures()
            .into_iter()
            .next()
            .expect("fixture should contain one row");
        let delta = commit_delta_ref(
            commit_id,
            &fixture,
            crate::json_store::JsonSlotRef::None,
            crate::json_store::JsonSlotRef::None,
            None,
        );
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &[delta]).expect("ordinary delta should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("ordinary delta should commit");

        let manifest_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let seal_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let read = SealCountingRead {
            inner: storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("point read should open"),
            manifest_requests: std::sync::Arc::clone(&manifest_requests),
            seal_requests: std::sync::Arc::clone(&seal_requests),
        };
        let state = super::load_replay_commit_state_manifest(&read, commit_id)
            .await
            .expect("replay manifest should load")
            .expect("replay manifest should exist");
        let cache = super::CommitDeltaPointReadCache::default();
        super::seed_commit_delta_point_cache_from_replay_manifest(&state, &cache)
            .expect("replay manifest should seed the point cache");
        let encoded_key = Bytes::from(encode_key_ref(TrackedStateKeyRef {
            schema_key: &fixture.schema_key,
            file_id: fixture.file_id.as_deref(),
            entity_pk: &fixture.entity_pk,
        }));
        let values = super::load_commit_delta_values_encoded_with_cache(
            &read,
            commit_id,
            &[encoded_key],
            Some(&cache),
        )
        .await
        .expect("cached shallow point replay should load");
        assert_eq!(values, vec![Some(fixture.value(commit_id))]);
        assert_eq!(manifest_requests.load(Ordering::Relaxed), 0);
        assert_eq!(seal_requests.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn single_segment_commit_delta_stays_inline() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("inline-packed-delta-commit");
        let fixture = packed_commit_delta_fixtures()
            .into_iter()
            .next()
            .expect("fixture should contain one row");
        let delta = commit_delta_ref(
            commit_id,
            &fixture,
            crate::json_store::JsonSlotRef::None,
            crate::json_store::JsonSlotRef::None,
            None,
        );
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &[delta]).expect("inline delta should stage");
        assert_eq!(
            writes.stats().staged_puts,
            2,
            "a one-segment commit should remain inline in its commit-state authority plus immutable seal"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("inline delta should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            load_commit_delta_values_for_test(&read, commit_id, &[fixture.key()])
                .await
                .expect("inline point replay should load"),
            vec![Some(fixture.value(commit_id))]
        );
        let batch =
            scan_commit_delta_values(&read, commit_id, std::slice::from_ref(&fixture.schema_key))
                .await
                .expect("inline schema replay should load");
        assert_eq!(
            decoded_commit_delta_rows(&batch),
            vec![(fixture.key(), fixture.value(commit_id))]
        );

        let mut deletes = storage.new_write_set();
        let inventory = scan_commit_delta_inventory(&read)
            .await
            .expect("packed inventory should scan");
        stage_delete_commit_delta_inventory_entry(
            &mut deletes,
            commit_id,
            inventory
                .commits
                .get(&commit_id)
                .expect("inline commit should be inventoried"),
        )
        .expect("inline delta should stage for deletion");
        assert_eq!(
            deletes.stats().staged_deletes,
            2,
            "GC should delete the inline commit-state authority and immutable seal"
        );
    }

    #[tokio::test]
    async fn schema_scan_validates_unselected_packed_delta_entries() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("packed-delta-expected-commit");
        let wrong_commit_id = CommitId::for_test_label("packed-delta-wrong-commit");
        let fixtures = packed_commit_delta_fixtures();
        let alpha = &fixtures[0];
        let beta = &fixtures[1];
        let mut entries = vec![
            EncodedLeafEntry {
                key: encode_key_ref(TrackedStateKeyRef {
                    schema_key: &alpha.schema_key,
                    file_id: alpha.file_id.as_deref(),
                    entity_pk: &alpha.entity_pk,
                })
                .into(),
                value: encode_value_ref(TrackedStateIndexValueRef {
                    change_id: alpha.change_id,
                    commit_id,
                    deleted: alpha.deleted,
                    created_at: alpha.created_at,
                    updated_at: alpha.updated_at,
                })
                .into(),
            },
            EncodedLeafEntry {
                key: encode_key_ref(TrackedStateKeyRef {
                    schema_key: &beta.schema_key,
                    file_id: beta.file_id.as_deref(),
                    entity_pk: &beta.entity_pk,
                })
                .into(),
                value: encode_value_ref(TrackedStateIndexValueRef {
                    change_id: beta.change_id,
                    commit_id: wrong_commit_id,
                    deleted: beta.deleted,
                    created_at: beta.created_at,
                    updated_at: beta.updated_at,
                })
                .into(),
            },
        ];
        entries.sort_unstable_by(|left, right| left.key.cmp(&right.key));

        let mut writes = storage.new_write_set();
        let delta_manifest = CommitDeltaManifest {
            selected_source_commit_id: None,
            member_count: 2,
            selection_fingerprint: [0; 32],
            direct_segment_row_counts: Vec::new(),
            single_partition: None,
            lifecycle_summary: None,
            replacement_generation: None,
            replacement_parts: None,
            inline_segment: encode_commit_delta_segment(&entries),
            segments: Vec::new(),
        };
        stage_fixture_manifest(
            &mut writes,
            commit_id,
            &super::commit_state_inventory_from_delta_manifest(&delta_manifest),
        )
        .expect("commit-state authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("corrupt fixture should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = scan_commit_delta_values(&read, commit_id, &["alpha".to_string()])
            .await
            .expect_err("schema scans must validate entries outside the requested schema");
        assert!(
            error.to_string().contains("contains an entry for commit"),
            "unexpected error: {error}"
        );
        let error = load_commit_delta_change_ids(&read, commit_id)
            .await
            .expect_err("commit membership must validate every physical owner");
        assert!(
            error.to_string().contains("contains an entry for commit"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn packed_commit_delta_gc_deletes_manifest_and_segments() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("packed-delta-gc");
        let fixtures = packed_commit_delta_fixtures();
        let deltas = fixtures
            .iter()
            .map(|fixture| {
                commit_delta_ref(
                    commit_id,
                    fixture,
                    crate::json_store::JsonSlotRef::None,
                    crate::json_store::JsonSlotRef::None,
                    None,
                )
            })
            .collect::<Vec<_>>();
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("packed deltas should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("packed deltas should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut deletes = storage.new_write_set();
        let inventory = scan_commit_delta_inventory(&read)
            .await
            .expect("packed inventory should scan");
        stage_delete_commit_delta_inventory_entry(
            &mut deletes,
            commit_id,
            inventory
                .commits
                .get(&commit_id)
                .expect("packed commit should be inventoried"),
        )
        .expect("packed deltas should stage for deletion");
        let expected_deletes = u64::try_from(
            inventory
                .commits
                .get(&commit_id)
                .expect("packed commit should be inventoried")
                .segment_count
                + 2,
        )
        .expect("test segment count fits u64");
        assert_eq!(deletes.stats().staged_deletes, expected_deletes);
        storage
            .commit_write_set(deletes, StorageWriteOptions::default())
            .await
            .expect("packed delta deletion should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("post-GC read should open");
        assert!(
            load_commit_delta_values_for_test(&read, commit_id, &[fixtures[0].key()])
                .await
                .expect("post-GC point replay should load")
                .into_iter()
                .all(|value| value.is_none())
        );
        assert!(
            scan_commit_delta_values(&read, commit_id, &[])
                .await
                .expect("post-GC scan replay should load")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn packed_commit_delta_boundary_keeps_file_identity_and_sparse_schema_replay() {
        let storage = StorageAdapter::new(Memory::new());
        let inline_commit_id = CommitId::for_test_label("packed-delta-inline-boundary");
        let indexed_commit_id = CommitId::for_test_label("packed-delta-indexed-boundary");
        let fixtures = (0..129)
            .map(|index| CommitDeltaFixture {
                schema_key: match index {
                    127 => "sparse".to_string(),
                    128 => "zeta".to_string(),
                    _ => "alpha".to_string(),
                },
                file_id: (index == 127).then(|| "sparse-file".to_string()),
                entity_pk: EntityPk::single(format!("boundary-{index:04}")),
                change_id: ChangeId::for_test_label(&format!("boundary-change-{index}")),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(index.into()),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy((index + 1).into()),
            })
            .collect::<Vec<_>>();

        let mut inline_writes = storage.new_write_set();
        let inline_deltas = commit_delta_refs(inline_commit_id, &fixtures[..128]);
        stage_commit_deltas(&mut inline_writes, &inline_deltas)
            .expect("128 generic deltas should fit the history read boundary");
        assert_eq!(inline_writes.stats().staged_puts, 2);
        storage
            .commit_write_set(inline_writes, StorageWriteOptions::default())
            .await
            .expect("inline boundary deltas should commit");

        let mut indexed_writes = storage.new_write_set();
        let indexed_deltas = commit_delta_refs(indexed_commit_id, &fixtures);
        stage_commit_deltas(&mut indexed_writes, &indexed_deltas)
            .expect("129 generic deltas should use indexed segments");
        assert_eq!(indexed_writes.stats().staged_puts, 4);
        storage
            .commit_write_set(indexed_writes, StorageWriteOptions::default())
            .await
            .expect("indexed boundary deltas should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let sparse = &fixtures[127];
        assert_eq!(
            load_commit_delta_values_for_test(&read, indexed_commit_id, &[sparse.key()])
                .await
                .expect("file-scoped point replay should load"),
            vec![Some(sparse.value(indexed_commit_id))]
        );
        let batch = scan_commit_delta_values(&read, indexed_commit_id, &["sparse".to_string()])
            .await
            .expect("sparse schema replay should load");
        assert_eq!(
            decoded_commit_delta_rows(&batch),
            vec![(sparse.key(), sparse.value(indexed_commit_id))]
        );
    }

    #[tokio::test]
    async fn large_commit_delta_scan_dictionary_encodes_repeated_metadata_once() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("large-shared-decoded-delta-batch");
        let fixtures = (0..10_000)
            .map(|index| CommitDeltaFixture {
                schema_key: "shared-schema".to_string(),
                file_id: Some("01920000-0000-7000-8000-000000000442".to_string()),
                entity_pk: EntityPk::single(format!("entity-{index:05}")),
                change_id: ChangeId::for_test_label(&format!("large-shared-decoded-delta-{index}")),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(index),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(index + 1),
            })
            .collect::<Vec<_>>();
        let deltas = commit_delta_refs(commit_id, &fixtures);
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("large commit delta should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("large commit delta should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("large commit delta read should open");
        let batch = scan_commit_delta_values(&read, commit_id, &[])
            .await
            .expect("large commit delta should decode");

        assert_eq!(batch.len(), fixtures.len());
        assert_eq!(batch.schema_dictionary_len(), 1);
        assert_eq!(batch.file_dictionary_len(), 1);
        assert_eq!(
            batch.arena_count(),
            fixtures
                .len()
                .div_ceil(GENERIC_COMMIT_DELTA_SEGMENT_MAX_ROWS),
            "the batch retains one decoded arena per packed segment, never one owner per row"
        );
        assert!(
            batch.arena_count() * GENERIC_COMMIT_DELTA_SEGMENT_MAX_ROWS >= batch.len(),
            "segment arena ownership must stay bounded independently of row metadata"
        );
        let first = batch.iter().next().expect("large batch has a first row");
        let first_key = first.key_ref();
        let schema_pointer = first_key.schema_key.as_ptr();
        let file_pointer = first_key.file_id.expect("shared file id").as_ptr();
        assert!(batch.iter().all(|row| {
            let key = row.key_ref();
            key.schema_key == "shared-schema"
                && key.file_id == Some("01920000-0000-7000-8000-000000000442")
                && key.schema_key.as_ptr() == schema_pointer
                && key
                    .file_id
                    .is_some_and(|file_id| file_id.as_ptr() == file_pointer)
        }));
    }

    #[test]
    fn native_storage_space_ids_are_unique_across_owner_layouts() {
        let spaces = [
            REPOSITORY_PROTOCOL_SPACE,
            BRANCH_HEAD_CONTROL_SPACE,
            HOT_ROW_SPACE,
            HOT_FILE_SPACE,
            HOT_DIFF_SPACE,
            TRACKED_WORKING_DIFF_MARKER_SPACE,
            JSON_SPACE,
            UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE,
            TRACKED_STATE_TREE_CHUNK_SPACE,
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            TRACKED_STATE_CHANGE_LOCATOR_SPACE,
            CURRENT_STATE_PART_DIRECTORY_SPACE,
            BINARY_CAS_MANIFEST_SPACE,
            BINARY_CAS_MANIFEST_CHUNK_SPACE,
            BINARY_CAS_CHUNK_PRESENCE_SPACE,
            BINARY_CAS_CHUNK_SPACE,
            COMMIT_SPACE,
            CHANGE_SPACE,
            COMMIT_CHANGE_ID_SPACE,
            CHECKPOINT_RECOVERY_REF_SPACE,
            CHECKPOINT_GC_STATE_SPACE,
        ];
        let mut seen = BTreeMap::new();
        for space in spaces {
            assert_eq!(
                seen.insert(space.id, space.name),
                None,
                "storage space id {:?} is reused by {} and {}",
                space.id,
                seen.get(&space.id).copied().unwrap_or(space.name),
                space.name
            );
        }
    }

    fn commit_state_manifest_fixture() -> CommitStateManifest {
        let commit_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x018f_ffff_1234_7000_8000_0000_ffff_ffff,
        ));
        let schema_key = "manifest-schema";
        let entity_pk = EntityPk::single("manifest-entity");
        let timestamp = LixTimestamp::from_unix_millis_utc_lossy(1234);
        let entry = EncodedLeafEntry {
            key: encode_key_ref(TrackedStateKeyRef {
                schema_key,
                file_id: None,
                entity_pk: &entity_pk,
            })
            .into(),
            value: encode_value_ref(TrackedStateIndexValueRef {
                change_id: super::change_id_from_packed_address(commit_id, 1),
                commit_id,
                deleted: false,
                created_at: timestamp,
                updated_at: timestamp,
            })
            .into(),
        };
        CommitStateManifest {
            commit_id,
            generation: 7,
            parent_commit_ids: vec![CommitId::for_test_label("manifest-parent")],
            commit_change_id: ChangeId::for_test_label("manifest-commit-change"),
            author_account_ids: vec!["account-a".to_string()],
            created_at: timestamp,
            replay_debt: CommitStateReplayDebt {
                depth: 2,
                rows: 1,
                bytes: 64,
            },
            mutations: CommitStateMutationInventory {
                selected_source_commit_id: None,
                member_count: 1,
                selection_fingerprint: [3; 32],
                direct_part_row_counts: vec![1],
                replacement_part_digests: Vec::new(),
                single_partition: Some(super::CommitDeltaReplacementScope {
                    schema_key: schema_key.to_owned(),
                    file_id: None,
                }),
                lifecycle_summary: None,
                replacement_generation: None,
                replacement_parts: None,
                inline_part: encode_commit_delta_segment(&[entry]),
                parts: Vec::new(),
            },
            current_state_catalog: None,
            current_state_coverage_anchor: None,
            snapshot_root: None,
        }
    }

    #[test]
    fn commit_state_manifest_codec_roundtrips_all_authority_planes() {
        let expected = commit_state_manifest_fixture();
        let encoded = encode_commit_state_manifest(&expected).expect("manifest should encode");
        assert!(encoded.starts_with(COMMIT_STATE_MANIFEST_FORMAT_MAGIC));

        let decoded = decode_commit_state_manifest(&encoded).expect("manifest should round trip");
        assert_eq!(decoded, expected);
    }

    #[tokio::test]
    async fn commit_state_manifest_space_roundtrips_by_exact_commit_id() {
        let storage = StorageAdapter::new(Memory::new());
        let expected = commit_state_manifest_fixture();
        let mut writes = storage.new_write_set();
        stage_commit_state_manifest(&mut writes, &expected).expect("manifest should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("manifest should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            load_commit_state_manifest(&read, expected.commit_id)
                .await
                .expect("manifest should load"),
            Some(expected)
        );
    }

    #[tokio::test]
    async fn semantic_authority_is_the_point_source_and_guards_manifest_scans() {
        let storage = StorageAdapter::new(Memory::new());
        let manifest = commit_state_manifest_fixture();
        let mut writes = storage.new_write_set();
        stage_commit_state_manifest(&mut writes, &manifest).expect("authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("authority should commit");

        let mut forged = manifest.clone();
        forged.generation += 1;
        let mut writes = storage.new_write_set();
        super::stage_unchecked_commit_state_manifest_for_test(&mut writes, &forged)
            .expect("mutable manifest corruption should stage without replacing authority");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("mutable manifest corruption should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("corrupt read should open");
        let point_state = super::load_point_replay_commit_state(&read, manifest.commit_id)
            .await
            .expect("point replay should load immutable semantic authority")
            .expect("semantic authority should exist");
        assert_eq!(&*point_state, &manifest);
        let change_error = scan_change_records_from_commit_deltas(&read)
            .await
            .expect_err("packed change scans must verify semantic authority");
        assert!(change_error.message.contains("semantic authority"));
        let inventory_error = scan_commit_delta_inventory(&read)
            .await
            .expect_err("GC inventory must verify semantic authority");
        assert!(inventory_error.message.contains("semantic authority"));

        let mut corrupted_authority =
            super::commit_state_semantic_seal(&manifest).expect("semantic authority should encode");
        *corrupted_authority
            .last_mut()
            .expect("semantic authority should not be empty") ^= 0xff;
        let mut writes = storage.new_write_set();
        writes.put(
            StorageSpace::mutable(
                super::TRACKED_STATE_COMMIT_STATE_SEAL_SPACE.id,
                super::TRACKED_STATE_COMMIT_STATE_SEAL_NAMESPACE,
            ),
            key(super::commit_state_manifest_key(manifest.commit_id)),
            value(corrupted_authority),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("authority corruption should commit through the test-only mutable alias");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("corrupt authority read should open");
        let error = super::load_point_replay_commit_state(&read, manifest.commit_id)
            .await
            .expect_err("point replay must reject corrupt immutable authority bytes");
        assert!(error.message.contains("authority digest"));
    }

    #[tokio::test]
    async fn gc_inventory_rejects_missing_and_orphan_semantic_authority() {
        let missing_storage = StorageAdapter::new(Memory::new());
        let manifest = commit_state_manifest_fixture();
        let mut writes = missing_storage.new_write_set();
        stage_commit_state_manifest(&mut writes, &manifest).expect("authority should stage");
        missing_storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("authority should commit");
        let mut deletes = missing_storage.new_write_set();
        deletes.delete(
            super::TRACKED_STATE_COMMIT_STATE_SEAL_SPACE,
            key(super::commit_state_manifest_key(manifest.commit_id)),
        );
        missing_storage
            .commit_write_set(deletes, StorageWriteOptions::default())
            .await
            .expect("seal deletion should commit");
        let read = missing_storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("missing-seal read should open");
        let error = scan_commit_delta_inventory(&read)
            .await
            .expect_err("GC inventory must reject a missing seal");
        assert!(
            error
                .message
                .contains("orphan manifest or semantic authority")
        );

        let orphan_storage = StorageAdapter::new(Memory::new());
        let mut writes = orphan_storage.new_write_set();
        writes.put(
            super::TRACKED_STATE_COMMIT_STATE_SEAL_SPACE,
            key(super::commit_state_manifest_key(manifest.commit_id)),
            value(
                super::commit_state_semantic_seal(&manifest)
                    .unwrap()
                    .to_vec(),
            ),
        );
        orphan_storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("orphan seal should commit");
        let read = orphan_storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("orphan-seal read should open");
        let error = scan_commit_delta_inventory(&read)
            .await
            .expect_err("GC inventory must reject an orphan seal");
        assert!(
            error
                .message
                .contains("orphan manifest or semantic authority")
        );
    }

    #[tokio::test]
    async fn authoritative_root_is_visible_only_through_commit_state() {
        let storage = StorageAdapter::new(Memory::new());
        let mut manifest = commit_state_manifest_fixture();
        manifest.replay_debt = CommitStateReplayDebt::default();
        let authoritative = TrackedStateCommitRoot {
            commit_id: manifest.commit_id,
            root_id: TrackedStateRootId::new([1; 32]),
            parent_roots: vec![crate::tracked_state::types::TrackedStateCommitRootParent {
                commit_id: manifest.parent_commit_ids[0],
                root_id: TrackedStateRootId::new([3; 32]),
            }],
            changed_key_count: 1,
            row_count_estimate: 1,
            tree_height: 1,
            primary_chunk_count: 1,
            primary_chunk_bytes: 64,
        };
        manifest.snapshot_root = Some(authoritative.clone());
        let mut writes = storage.new_write_set();
        stage_commit_state_manifest(&mut writes, &manifest).expect("authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("root fixtures should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        assert_eq!(
            super::load_authoritative_commit_root(&read, &manifest.commit_id.to_string())
                .await
                .expect("authority should load"),
            Some(authoritative)
        );
        drop(read);

        manifest.snapshot_root = None;
        manifest.replay_debt = CommitStateReplayDebt {
            depth: 1,
            rows: 1,
            bytes: 64,
        };
        let mut writes = storage.new_write_set();
        super::stage_resealed_commit_state_manifest_for_test(&mut writes, &manifest)
            .expect("rootless authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("rootless authority should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("rootless read should open");
        assert!(
            super::load_authoritative_commit_root(&read, &manifest.commit_id.to_string())
                .await
                .expect("rootless authority should load")
                .is_none(),
            "a stale derived root must not override rootless authority"
        );
    }

    #[tokio::test]
    async fn rootless_rebuild_updates_only_the_mutable_snapshot_accelerator() {
        let storage = StorageAdapter::new(Memory::new());
        let mut original = commit_state_manifest_fixture();
        original.snapshot_root = None;
        original.replay_debt = CommitStateReplayDebt {
            depth: 1,
            rows: 1,
            bytes: 64,
        };
        let mut writes = storage.new_write_set();
        stage_commit_state_manifest(&mut writes, &original)
            .expect("rootless authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("rootless authority should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("published authority read should open");
        let published = super::load_published_commit_state_manifest(&read, original.commit_id)
            .await
            .expect("published authority should load")
            .expect("published authority should exist");
        let rebuilt = TrackedStateCommitRoot {
            commit_id: original.commit_id,
            root_id: TrackedStateRootId::new([9; 32]),
            parent_roots: vec![crate::tracked_state::types::TrackedStateCommitRootParent {
                commit_id: original.parent_commit_ids[0],
                root_id: TrackedStateRootId::new([8; 32]),
            }],
            changed_key_count: 1,
            row_count_estimate: 1,
            tree_height: 1,
            primary_chunk_count: 1,
            primary_chunk_bytes: 64,
        };
        let mut writes = storage.new_write_set();
        super::stage_commit_state_snapshot_root_update(
            &mut writes,
            &published,
            Some(rebuilt.clone()),
        )
        .expect("snapshot accelerator update should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("snapshot rebuild must not rewrite immutable authority");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("rebuilt authority read should open");
        let loaded = load_commit_state_manifest(&read, original.commit_id)
            .await
            .expect("rebuilt manifest should load")
            .expect("rebuilt manifest should exist");
        assert_eq!(loaded.snapshot_root, Some(rebuilt));
        let point = super::load_point_replay_commit_state(&read, original.commit_id)
            .await
            .expect("immutable point authority should load")
            .expect("immutable point authority should exist");
        assert_eq!(&*point, &original);
    }

    #[test]
    fn commit_state_direct_change_route_rejects_holes_and_other_commits() {
        let manifest = commit_state_manifest_fixture();
        let first = super::change_id_from_packed_address(manifest.commit_id, 1);
        let hole = super::change_id_from_packed_address(manifest.commit_id, 2);
        let other = super::change_id_from_packed_address(
            CommitId::with_change_address_space(uuid::Uuid::from_u128(
                0x018f_ffff_1234_7000_8000_0001_ffff_ffff,
            )),
            1,
        );

        let locator = direct_change_locator_in_commit_state(&manifest, first)
            .expect("first direct address should route");
        assert_eq!(locator.segment_index, 0);
        assert_eq!(locator.ordinal, 0);
        assert!(direct_change_locator_in_commit_state(&manifest, hole).is_none());
        assert!(direct_change_locator_in_commit_state(&manifest, other).is_none());
    }

    #[test]
    fn commit_state_manifest_allows_accelerators_and_rejects_replay_drift() {
        let mut mixed = commit_state_manifest_fixture();
        mixed.mutations.parts.push(super::CommitStateMutationPart {
            first_key: vec![1],
            last_key: vec![2],
            replacement_part: None,
        });
        assert!(
            encode_commit_state_manifest(&mixed)
                .expect_err("inline and external parts must be exclusive")
                .message
                .contains("mixes inline and external")
        );

        let mut rooted_with_debt = commit_state_manifest_fixture();
        rooted_with_debt.snapshot_root = Some(TrackedStateCommitRoot {
            commit_id: rooted_with_debt.commit_id,
            root_id: TrackedStateRootId::new([4; 32]),
            parent_roots: vec![crate::tracked_state::types::TrackedStateCommitRootParent {
                commit_id: rooted_with_debt.parent_commit_ids[0],
                root_id: TrackedStateRootId::new([5; 32]),
            }],
            changed_key_count: 1,
            row_count_estimate: 1,
            tree_height: 1,
            primary_chunk_count: 1,
            primary_chunk_bytes: 64,
        });
        let encoded = encode_commit_state_manifest(&rooted_with_debt)
            .expect("an immutable snapshot accelerator may coexist with replay debt");
        assert_eq!(
            decode_commit_state_manifest(&encoded).expect("accelerated manifest should decode"),
            rooted_with_debt
        );

        let mut invalid_debt = rooted_with_debt.clone();
        invalid_debt.replay_debt.depth = 0;
        assert!(
            encode_commit_state_manifest(&invalid_debt)
                .expect_err("replay rows at zero depth must be rejected")
                .message
                .contains("replay work at zero depth")
        );

        let mut forged_empty_authority = commit_state_manifest_fixture();
        forged_empty_authority.mutations = CommitStateMutationInventory::default();
        forged_empty_authority.mutations.replacement_parts =
            Some(super::StoredReplacementPartsAuthority {
                directory_digest: [7; 32],
                uniform_updated_at: forged_empty_authority.created_at,
            });
        assert!(
            encode_commit_state_manifest(&forged_empty_authority)
                .expect_err("replacement-part authority without a generation must be rejected")
                .message
                .contains("replacement-part authority has an invalid manifest shape")
        );
    }

    #[test]
    fn commit_state_manifest_rejects_old_formats() {
        let error = decode_commit_state_manifest(b"LXCS0old")
            .expect_err("old commit-state formats must fail closed");
        assert!(error.message.contains("recreate the repository"));
    }

    #[test]
    fn production_tracked_state_sources_do_not_call_storage_batch_writer() {
        let tracked_state_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tracked_state");
        let forbidden = ["write", "kv", "batch"].join("_");

        for path in rust_sources(&tracked_state_dir) {
            let source =
                fs::read_to_string(&path).expect("tracked_state source should be readable");
            for (line_number, line) in production_lines(&source) {
                assert!(
                    !line.contains(&forbidden),
                    "production tracked_state source must stage into StorageWriteSet instead of calling {forbidden}: {}:{}",
                    path.display(),
                    line_number
                );
            }
        }
    }

    fn rust_sources(dir: &Path) -> Vec<PathBuf> {
        let mut sources = Vec::new();
        for entry in fs::read_dir(dir).expect("tracked_state source dir should be readable") {
            let path = entry
                .expect("tracked_state source entry should be readable")
                .path();
            if path.is_dir() {
                sources.extend(rust_sources(&path));
            } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
                sources.push(path);
            }
        }
        sources
    }

    fn production_lines(source: &str) -> Vec<(usize, &str)> {
        let mut lines = Vec::new();
        let mut skipping_cfg_test_item = false;
        let mut pending_cfg_test = false;
        let mut item_started = false;
        let mut brace_depth = 0i32;

        for (index, line) in source.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed == "#[cfg(test)]" {
                pending_cfg_test = true;
                continue;
            }

            if pending_cfg_test || skipping_cfg_test_item {
                if pending_cfg_test && !item_started && trimmed.ends_with(';') {
                    pending_cfg_test = false;
                    continue;
                }
                let opens = line.matches('{').count() as i32;
                let closes = line.matches('}').count() as i32;
                if opens > 0 {
                    item_started = true;
                    skipping_cfg_test_item = true;
                }
                if item_started {
                    brace_depth += opens - closes;
                    if brace_depth <= 0 {
                        pending_cfg_test = false;
                        skipping_cfg_test_item = false;
                        item_started = false;
                        brace_depth = 0;
                    }
                }
                continue;
            }

            lines.push((index + 1, line));
        }

        lines
    }
}
