#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cmp_owned
)]

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::{Bound, Deref, Range};
use std::sync::{Arc, Mutex, OnceLock};

use crate::changelog::ChangeRecordProjection;
use crate::changelog::{
    COMMIT_SPACE, ChangeLoadRequest, ChangelogContext, ChangelogReader, CommitId,
    CommitLoadRequest, CommitRecord, commit_key,
};
use crate::common::SharedStr;
use crate::row_pk::RowPk;
use crate::storage_adapter::{
    BufferRange, EncodedMutationBatch, EncodedPut, PointReadPlan, StorageAdapterRead,
    StorageBeginScanOptions, StorageCoreProjection, StorageError, StorageGetManyRequest,
    StorageGetManyResult, StorageGetOptions, StorageKey, StorageKeyRange, StorageProjectedValue,
    StorageScanCursor, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet, ValueSemantics,
    exact_get_many,
};
use crate::tracked_state::codec::{
    DecodedLeafNodeRef, DecodedNodeRef, EncodedLeafEntry, EncodedLeafEntryRef, PendingChunkBatch,
    TrackedStateKeyBatchBuilder, TrackedStateMutationBatchBuilder, decode_key, decode_key_shared,
    decode_node_ref, decode_value, encode_key_ref, encode_key_ref_into, encode_leaf_node_refs,
    encode_schema_file_prefix, encode_schema_key_prefix, encode_single_string_key_ref_into,
    encode_value_ref,
};
use crate::tracked_state::types::{
    ColumnarPageSource, CommitStateManifest, CommitStateMutationInventory, CommitStateMutationPart,
    CurrentStatePartDescriptor, CurrentStatePartSource, CurrentStateScopedRangeRoot,
    StoredCommitDeltaReplacementGeneration, StoredReplacementPart, StoredReplacementPartsAuthority,
    TRACKED_STATE_HASH_BYTES, TrackedStateBaseCoordinate, TrackedStateCommitDeltaRef,
    TrackedStateCommitRoot, TrackedStateIndexValue, TrackedStateIndexValueRef, TrackedStateKey,
    TrackedStateKeyRef, TrackedStateRootId, TrackedStateSingleStringReplacementRef,
    TrackedStateTreeScanRequest,
};
pub(crate) use crate::tracked_state::types::{
    CommitDeltaLifecycleSummary, CommitDeltaReplacementScope,
};
use crate::{LixError, storage_codec};
use bytes::Bytes;

pub(crate) const TRACKED_STATE_TREE_CHUNK_NAMESPACE: &str = "tracked_state.tree_chunk";
pub(crate) const TRACKED_STATE_COMMIT_DELTA_SEGMENT_NAMESPACE: &str =
    "tracked_state.commit_delta_segment.v6";
pub(crate) const TRACKED_STATE_CHANGE_LOCATOR_NAMESPACE: &str = "tracked_state.change_locator.v2";
pub(crate) const TRACKED_STATE_COMMIT_STATE_MANIFEST_NAMESPACE: &str =
    "tracked_state.commit_state_manifest.v7";
pub(crate) const TRACKED_STATE_COMMIT_MUTATION_INVENTORY_NAMESPACE: &str =
    "tracked_state.commit_mutation_catalog.v1";
pub(crate) const TRACKED_STATE_COMMIT_HISTORY_DEFERRED_NAMESPACE: &str =
    "tracked_state.commit_history_deferred.v1";
const MIN_CURRENT_STATE_SCOPED_RANGE_POINT_READS: u16 = 4;
pub(crate) const TRACKED_STATE_TREE_CHUNK_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0001),
    TRACKED_STATE_TREE_CHUNK_NAMESPACE,
    ValueSemantics::Mutable,
);
pub(crate) const TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_001a),
    TRACKED_STATE_COMMIT_DELTA_SEGMENT_NAMESPACE,
    ValueSemantics::Immutable,
);
/// Keep every high-volume packed-history plane below the live-row spaces
/// (`0x0004_001b..=0x0004_001d`). Backends order the space prefix first, so a
/// locator above those spaces makes each mixed manifest/locator SST overlap
/// unrelated live-state point reads.
pub(crate) const TRACKED_STATE_CHANGE_LOCATOR_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0018),
    TRACKED_STATE_CHANGE_LOCATOR_NAMESPACE,
    ValueSemantics::Mutable,
);
/// Hard-cut tracked commit authority.
///
/// Current repositories publish one compact authority header per commit,
/// including commits with no tracked mutations. The header authenticates a
/// separately keyed mutation catalog and its optional hierarchical directory.
/// The former topology, flat delta-directory, and root authority spaces are
/// not part of the current protocol.
pub(crate) const TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_002b),
    TRACKED_STATE_COMMIT_STATE_MANIFEST_NAMESPACE,
    ValueSemantics::Immutable,
);
pub(crate) const TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE: StorageSpace =
    StorageSpace::declare(
        StorageSpaceId(0x0004_002c),
        TRACKED_STATE_COMMIT_MUTATION_INVENTORY_NAMESPACE,
        ValueSemantics::Immutable,
    );
pub(crate) const TRACKED_STATE_COMMIT_HISTORY_DEFERRED_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_002e),
    TRACKED_STATE_COMMIT_HISTORY_DEFERRED_NAMESPACE,
    ValueSemantics::Mutable,
);

// The canonical ordered mutation-part width and durable direct-ChangeId
// address stride. Physical ordered parts and their packed coordinates must
// use the same width so a part boundary never needs a second geometry.
const COMMIT_DELTA_SEGMENT_MAX_ROWS: usize = 512;
// Scan pages are bounded by row count, not bytes. Keep authority hydration
// bounded as well when a page contains large authenticated directories.
const COMMIT_STATE_SCAN_AUTHORITY_BATCH_ROWS: usize = 64;
const GENERIC_COMMIT_DELTA_SEGMENT_MAX_ROWS: usize = 128;
const GENERIC_COMMIT_DELTA_SEGMENT_TARGET_BYTES: usize = 28 * 1024;
const ORDERED_COMMIT_DELTA_SEGMENT_TARGET_BYTES: usize = 64 * 1024;
// Version 15 makes every ordinary commit member self-contained and complete
// replacements authoritative through their immutable part manifest. The
// payload-less certified-reference encoding is intentionally rejected. The
// version also binds ordered mutation parts to the canonical 512-row geometry;
// LXCD14 is intentionally rejected rather than read through a compatibility
// decoder. Version 16 permits direct-address leaves to reconstruct the exact
// per-row ChangeId from one shared commit id and the first packed ordinal.
// LXCD15 is deliberately rejected rather than read through a compatibility
// decoder.
const COMMIT_DELTA_FORMAT_MAGIC: &[u8] = b"LXCD16";
// Version 4 makes lossless columnar mutation parts a first-class, exclusive
// commit payload. LXCS3 repositories are intentionally rejected: there is no
// compatibility decoder beneath the new authority.
// Version 5 additionally replaces the nested current-state catalog/directory
// with one authenticated scoped-range serving root. LXCS4 repositories are
// intentionally rejected rather than interpreted with mixed root semantics.
// Version 6 binds the scope-run v3 node protocol and shared runtime scope
// identities. LXCS5 roots cannot be reinterpreted under its content hashes.
// Version 7 authenticates the cumulative touched-schema negative certificate.
// Pre-cut manifests are deliberately incompatible with this physical-only format.
// Version 8 binds current-state serving roots to an explicit physical base
// commit independently from semantic graph ancestry.
// Version 10 splits the authority header from a separately keyed mutation
// catalog and authenticates a content-addressed hierarchical part directory.
// Version 11 adds the detached complete-state fence to snapshot-root metadata.
// V10 remains readable because deployed repositories already contain these
// immutable headers; the sync wire protocol itself has no compatibility lane.
const COMMIT_STATE_MANIFEST_FORMAT_MAGIC: &[u8] = b"LXCS11";
const COMMIT_STATE_MANIFEST_V10_FORMAT_MAGIC: &[u8] = b"LXCS10";
const COMMIT_STATE_MUTATION_INVENTORY_FORMAT_MAGIC: &[u8] = b"LXMI1";
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

#[derive(Clone)]
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
/// Smaller physical segments retain only one cumulative row end per segment;
/// their `(segment, ordinal)` address is derived on demand. Both shapes avoid
/// retaining per-row UUIDs or addresses while the prepared batch and backend
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
    Segmented(Vec<u32>),
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
            OrderedChangeAddresses::Segmented(row_ends) => {
                let row_index = u32::try_from(row_index)
                    .expect("ordered commit-delta row index fits direct address space");
                let segment_index = row_ends.partition_point(|&end| end <= row_index);
                let segment_start = segment_index
                    .checked_sub(1)
                    .map_or(0, |previous| row_ends[previous]);
                let ordinal = row_index
                    .checked_sub(segment_start)
                    .expect("ordered row follows its segment start");
                u32::try_from(segment_index)
                    .expect("ordered commit-delta segment index fits u32")
                    .checked_mul(
                        u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS)
                            .expect("segment row limit fits u32"),
                    )
                    .and_then(|base| base.checked_add(ordinal))
                    .and_then(|address| address.checked_add(1))
                    .expect("ordered commit-delta address fits direct address space")
            }
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

/// One authenticated mutation snapshot from a retained rootless commit.
/// Root consumers use this only when no dense tree root exists; it preserves
/// the canonical packed-delta and JSON-store serving authorities.
pub(crate) struct RetainedCommitSnapshot {
    pub(crate) key: TrackedStateKey,
    pub(crate) deleted: bool,
    pub(crate) snapshot: Option<String>,
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CommitDeltaInventory {
    pub(crate) commits: BTreeMap<CommitId, CommitDeltaInventoryEntry>,
}

struct CommitDeltaPlane {
    manifests: BTreeMap<CommitId, CommitDeltaManifest>,
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
    account_id: String,
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
    #[musli(with = storage_codec::option)]
    columnar_parts: Option<crate::tracked_state::types::ColumnarMutationPartSet>,
    /// A complete leaf payload for a commit that fits in one segment. Keeping
    /// it in the directory preserves the one-record shape of tiny commits;
    /// larger commits use the indexed segment list below.
    #[musli(bytes)]
    inline_segment: Vec<u8>,
    segments: Vec<CommitDeltaSegmentBounds>,
}

/// Small immutable authority header for one tracked commit.
///
/// Large mutation-part bounds live exactly once in the separately keyed
/// mutation inventory. This header owns its digest and selected-source fact,
/// so the inventory cannot be substituted and does not duplicate topology.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredCommitStateManifest {
    commit_id: CommitId,
    change_account_id: String,
    replay_debt: crate::tracked_state::CommitStateReplayDebt,
    #[musli(with = storage_codec::option)]
    selected_source_commit_id: Option<[u8; 16]>,
    mutation_inventory_digest: [u8; 32],
    mutation_transition_digest: [u8; 32],
    mutation_member_count: u32,
    mutation_part_count: u32,
    #[musli(with = storage_codec::option)]
    mutation_directory_root: Option<super::mutation_directory::MutationDirectoryRoot>,
    touched_scope_filter: crate::tracked_state::types::CommitStateTouchedScopeFilter,
    #[musli(with = storage_codec::option)]
    current_state_scoped_ranges: Option<Box<CurrentStateScopedRangeRoot>>,
    #[musli(with = storage_codec::option)]
    snapshot_root: Option<Box<TrackedStateCommitRoot>>,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredCommitStateManifestV10 {
    commit_id: CommitId,
    change_account_id: String,
    replay_debt: crate::tracked_state::CommitStateReplayDebt,
    #[musli(with = storage_codec::option)]
    selected_source_commit_id: Option<[u8; 16]>,
    mutation_inventory_digest: [u8; 32],
    mutation_transition_digest: [u8; 32],
    mutation_member_count: u32,
    mutation_part_count: u32,
    #[musli(with = storage_codec::option)]
    mutation_directory_root: Option<super::mutation_directory::MutationDirectoryRoot>,
    touched_scope_filter: crate::tracked_state::types::CommitStateTouchedScopeFilter,
    #[musli(with = storage_codec::option)]
    current_state_scoped_ranges: Option<Box<CurrentStateScopedRangeRoot>>,
    #[musli(with = storage_codec::option)]
    snapshot_root: Option<Box<TrackedStateCommitRootV10>>,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct TrackedStateCommitRootV10 {
    commit_id: CommitId,
    root_id: TrackedStateRootId,
    parent_roots: Vec<crate::tracked_state::types::TrackedStateCommitRootParent>,
    changed_key_count: u64,
    row_count_estimate: u64,
    tree_height: u32,
}

impl From<StoredCommitStateManifestV10> for StoredCommitStateManifest {
    fn from(stored: StoredCommitStateManifestV10) -> Self {
        Self {
            commit_id: stored.commit_id,
            change_account_id: stored.change_account_id,
            replay_debt: stored.replay_debt,
            selected_source_commit_id: stored.selected_source_commit_id,
            mutation_inventory_digest: stored.mutation_inventory_digest,
            mutation_transition_digest: stored.mutation_transition_digest,
            mutation_member_count: stored.mutation_member_count,
            mutation_part_count: stored.mutation_part_count,
            mutation_directory_root: stored.mutation_directory_root,
            touched_scope_filter: stored.touched_scope_filter,
            current_state_scoped_ranges: stored.current_state_scoped_ranges,
            snapshot_root: stored.snapshot_root.map(|root| {
                Box::new(TrackedStateCommitRoot {
                    commit_id: root.commit_id,
                    root_id: root.root_id,
                    parent_roots: root.parent_roots,
                    changed_key_count: root.changed_key_count,
                    row_count_estimate: root.row_count_estimate,
                    tree_height: root.tree_height,
                    complete_state_fence: false,
                })
            }),
        }
    }
}

/// Small separately keyed mutation catalog. Large ordered part metadata lives
/// only in the authenticated directory leaves referenced by `directory_root`.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredCommitMutationInventory {
    member_count: u32,
    selection_fingerprint: [u8; 32],
    #[musli(with = storage_codec::option)]
    single_partition: Option<CommitDeltaReplacementScope>,
    #[musli(with = storage_codec::option)]
    lifecycle_summary: Option<CommitDeltaLifecycleSummary>,
    #[musli(with = storage_codec::option)]
    replacement_generation: Option<StoredCommitDeltaReplacementGeneration>,
    #[musli(with = storage_codec::option)]
    replacement_parts: Option<StoredReplacementPartsAuthority>,
    #[musli(with = storage_codec::option)]
    columnar_parts: Option<crate::tracked_state::types::ColumnarMutationPartSet>,
    #[musli(bytes)]
    inline_part: Vec<u8>,
    inline_direct: bool,
    #[musli(with = storage_codec::option)]
    directory_root: Option<super::mutation_directory::MutationDirectoryRoot>,
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
        columnar_parts: manifest.columnar_parts.clone(),
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
    let mut delta = commit_delta_manifest_from_inventory(&manifest.mutations);
    delta.account_id = manifest.change_account_id.clone();
    delta
}

/// Returns the exact collection scopes touched by a certified mutation inventory.
/// `None` means bounds or implicit cascades may affect another scope, in which
/// case an inherited serving catalog must be discarded fail-closed.
/// At an empty base, descriptor cascades cannot reach inherited rows, so their
/// own authored scope remains exact and can seed the cumulative certificate.
pub(crate) fn commit_state_inventory_exact_touched_scopes(
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
    empty_base: bool,
) -> Result<Option<Vec<CommitDeltaReplacementScope>>, LixError> {
    if inventory.selected_source_commit_id.is_some() {
        return Ok(None);
    }
    commit_state_inventory_exact_local_touched_scopes(commit_id, inventory, empty_base)
}

/// Returns only scopes authored by this inventory, excluding the complete
/// inherited state supplied by an optional selected source.
pub(crate) fn commit_state_inventory_exact_local_touched_scopes(
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
    empty_base: bool,
) -> Result<Option<Vec<CommitDeltaReplacementScope>>, LixError> {
    if inventory.member_count == 0 {
        return Ok(Some(Vec::new()));
    }
    if let Some(parts) = inventory.columnar_parts.as_ref() {
        let scope = CommitDeltaReplacementScope {
            schema_key: parts.schema_key.clone(),
            file_id: None,
        };
        if inventory.single_partition.as_ref() != Some(&scope) {
            return Ok(None);
        }
        return Ok(Some(vec![scope]));
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
        if first.schema_key == "lix_file_descriptor" && !empty_base {
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
            if key.schema_key == "lix_file_descriptor" && !empty_base {
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

/// Returns the exact collection scopes this commit's delta has **members** in.
///
/// This is deliberately a different question from
/// [`commit_state_inventory_exact_touched_scopes`], which fails closed on file
/// descriptors because an *implicit cascade* changes the current state of rows
/// that are not members of this delta. History only ever reports members, so a
/// row that this commit did not physically carry cannot produce a history entry
/// and must not force the scope set to be discarded.
///
/// `None` means the member scopes are not enumerable from the inventory alone
/// and no absence may be proven:
///
/// * a selected source supplies members from another commit's delta, and
/// * a part whose first and last key straddle two scopes may contain members in
///   scopes neither bound names.
pub(crate) fn commit_delta_member_scopes(
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<Option<Vec<crate::changelog::CommitScopeKey>>, LixError> {
    if inventory.selected_source_commit_id.is_some() {
        return Ok(None);
    }
    if inventory.member_count == 0 {
        return Ok(Some(Vec::new()));
    }
    if let Some(parts) = inventory.columnar_parts.as_ref() {
        let scope = CommitDeltaReplacementScope {
            schema_key: parts.schema_key.clone(),
            file_id: None,
        };
        if inventory.single_partition.as_ref() != Some(&scope) {
            return Ok(None);
        }
        return Ok(Some(vec![member_scope(scope)]));
    }
    if !inventory.replacement_part_digests.is_empty() {
        return Ok(inventory
            .single_partition
            .clone()
            .map(|scope| vec![member_scope(scope)]));
    }

    let mut scopes = BTreeSet::new();
    for part in &inventory.parts {
        let first = decode_key(&part.first_key)?;
        let last = decode_key(&part.last_key)?;
        if first.schema_key != last.schema_key || first.file_id != last.file_id {
            return Ok(None);
        }
        scopes.insert(CommitDeltaReplacementScope {
            schema_key: first.schema_key,
            file_id: first.file_id,
        });
    }
    if !inventory.inline_part.is_empty() {
        let leaf = decode_commit_delta_segment(&inventory.inline_part, None, commit_id)?;
        visit_commit_delta_leaf(&leaf, commit_id, |_, encoded_key, _| {
            let key = decode_key(encoded_key)?;
            scopes.insert(CommitDeltaReplacementScope {
                schema_key: key.schema_key,
                file_id: key.file_id,
            });
            Ok(())
        })?;
    }
    Ok(Some(scopes.into_iter().map(member_scope).collect()))
}

fn member_scope(scope: CommitDeltaReplacementScope) -> crate::changelog::CommitScopeKey {
    crate::changelog::CommitScopeKey {
        schema_key: scope.schema_key,
        file_id: scope.file_id,
    }
}

async fn expanded_commit_delta_manifest_from_commit_state(
    store: &(impl StorageAdapterRead + ?Sized),
    manifest: &CommitStateManifest,
) -> Result<CommitDeltaManifest, LixError> {
    let mut expanded = commit_delta_manifest_from_commit_state(manifest);
    if !manifest.mutations.replacement_part_digests.is_empty() {
        expanded.segments = recover_replacement_segment_bounds(store, manifest).await?;
    }
    Ok(expanded)
}

/// Resolves one exact authenticated serving partition from an immutable
/// published commit manifest. The sealed content-addressed catalog root binds
/// every exact-scope entry; absence means the scope is not completely covered.
/// Resolves a point batch from the authenticated unified scope/range tree.
/// `None` means at least one scope is uncovered and canonical first-parent
/// replay remains responsible for the whole batch.
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
    let Some(root) = state.current_state_scoped_ranges.as_ref() else {
        return Ok(None);
    };
    if state.replay_debt.depth < MIN_CURRENT_STATE_SCOPED_RANGE_POINT_READS {
        // Canonical replay is cheaper while its bounded manifest interval is
        // shorter than an authenticated tree plus immutable payload read.
        return Ok(None);
    }
    let Some(_touched_scopes) =
        commit_state_inventory_exact_touched_scopes(state.commit_id, &state.mutations, false)?
    else {
        return Ok(None);
    };
    if prefer_recent_replay && current_inventory_may_contain_any_key(state, encoded_keys)? {
        return Ok(None);
    }
    if validate_publication {
        if load_commit_state_manifest(store, state.commit_id)
            .await?
            .as_ref()
            != Some(state)
        {
            return Err(replacement_payload_error(
                "current-state scoped ranges were not loaded from immutable commit authority",
            ));
        }
    }
    load_complete_current_state_values_from_scoped_root(store, root, encoded_keys).await
}

async fn load_current_state_values_from_descriptors(
    store: &(impl StorageAdapterRead + ?Sized),
    encoded_keys: &[Bytes],
    descriptors: Vec<Option<CurrentStatePartDescriptor>>,
) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
    if descriptors.len() != encoded_keys.len() {
        return Err(replacement_payload_error(
            "current-state descriptor route cardinality mismatch",
        ));
    }
    let mut routed = BTreeMap::<
        (CurrentStatePartSource, [u8; 32], u16),
        (CurrentStatePartDescriptor, Vec<usize>),
    >::new();
    for (output_index, descriptor) in descriptors.into_iter().enumerate() {
        let Some(descriptor) = descriptor else {
            continue;
        };
        routed
            .entry((
                descriptor.source.clone(),
                descriptor.content_digest,
                descriptor.source_row_offset,
            ))
            .or_insert_with(|| (descriptor, Vec::new()))
            .1
            .push(output_index);
    }
    let mut values = vec![None; encoded_keys.len()];

    let replacement = routed
        .iter()
        .filter_map(
            |(_, (descriptor, output_indices))| match &descriptor.source {
                CurrentStatePartSource::Replacement(source) => {
                    Some((descriptor, source, output_indices))
                }
                _ => None,
            },
        )
        .collect::<Vec<_>>();
    let storage_keys = replacement
        .iter()
        .map(|(descriptor, source, _)| {
            let owner = CommitId::new(uuid::Uuid::from_bytes(source.owner_commit_id));
            let mut key = commit_delta_segment_key(owner, source.part_index as usize)?;
            key.extend_from_slice(&descriptor.content_digest);
            Ok(StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let loaded = PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &storage_keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    for ((descriptor, source, output_indices), value) in replacement.into_iter().zip(loaded.value) {
        let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
            replacement_payload_error("current-state directory references a missing part")
        })?;
        let owner = CommitId::new(uuid::Uuid::from_bytes(source.owner_commit_id));
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
            let packed = source
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
                created_at: source.uniform_created_at,
                updated_at: source.uniform_updated_at,
            });
        }
    }
    let native = routed
        .iter()
        .filter(|(_, (descriptor, _))| {
            matches!(
                descriptor.source,
                CurrentStatePartSource::NativeDataPart { .. }
            )
        })
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
    let columnar = routed
        .iter()
        .filter_map(
            |(_, (descriptor, output_indices))| match &descriptor.source {
                CurrentStatePartSource::ColumnarPage(source) => {
                    Some((descriptor, source, output_indices))
                }
                _ => None,
            },
        )
        .collect::<Vec<_>>();
    let mut columnar_manifests = HashMap::new();
    for (_, source, _) in &columnar {
        let id = crate::columnar_row_group::RowGroupSetId::new(source.source_id);
        if let std::collections::hash_map::Entry::Vacant(entry) =
            columnar_manifests.entry(source.source_id)
        {
            let manifest = crate::columnar_row_group::load_row_group_manifest(store, id)
                .await?
                .ok_or_else(|| {
                    replacement_payload_error("current-state columnar manifest is missing")
                })?;
            entry.insert(manifest);
        }
    }
    for (&source_id, manifest) in &columnar_manifests {
        let identity_column_index = crate::row_columnar::row_identity_column_index(manifest)
            .ok_or_else(|| {
                replacement_payload_error("current-state columnar identity contract drifted")
            })?;
        let mut page_routes = BTreeMap::new();
        for (descriptor, source, output_indices) in &columnar {
            if source.source_id == source_id {
                page_routes
                    .entry((
                        source.part_index as usize,
                        usize::from(source.source_page_index),
                    ))
                    .or_insert_with(Vec::new)
                    .push((*descriptor, *source, output_indices.as_slice()));
            }
        }
        let coordinates = page_routes.keys().copied().collect::<Vec<_>>();
        crate::columnar_row_group::visit_row_group_pages(
            store,
            crate::columnar_row_group::RowGroupSetId::new(source_id),
            manifest,
            &coordinates,
            &[identity_column_index],
            |coordinate, batch| {
                for (descriptor, source, output_indices) in &page_routes[&coordinate] {
                    apply_columnar_identity_page(
                        manifest,
                        descriptor,
                        source,
                        output_indices,
                        &batch,
                        encoded_keys,
                        &mut values,
                    )?;
                }
                Ok(())
            },
        )
        .await?;
    }
    Ok(values)
}

fn apply_columnar_identity_page(
    manifest: &crate::columnar_row_group::RowGroupManifest,
    descriptor: &CurrentStatePartDescriptor,
    source: &ColumnarPageSource,
    output_indices: &[usize],
    batch: &datafusion::arrow::record_batch::RecordBatch,
    encoded_keys: &[Bytes],
    values: &mut [Option<TrackedStateIndexValue>],
) -> Result<(), LixError> {
    use datafusion::arrow::array::{Array, StringArray};

    let first_key = decode_key(&descriptor.first_key)?;
    if manifest.content_digest()? != descriptor.content_digest
        || manifest.namespace != first_key.schema_key
        || crate::row_columnar::row_group_set_id(
            CommitId::new(uuid::Uuid::from_bytes(source.owner_commit_id)),
            &manifest.namespace,
        )
        .as_bytes()
            != source.source_id
    {
        return Err(replacement_payload_error(
            "current-state columnar descriptor disagrees with its manifest",
        ));
    }
    let group_index = usize::try_from(source.part_index)
        .map_err(|_| replacement_payload_error("columnar group index exceeds usize"))?;
    let page_index = usize::from(source.source_page_index);
    let identities = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| replacement_payload_error("columnar identity page is not UTF-8"))?;
    let slice_start = usize::from(descriptor.source_row_offset);
    let slice_end = slice_start + usize::from(descriptor.row_count);
    let identities = (slice_end <= identities.len())
        .then(|| identities.slice(slice_start, usize::from(descriptor.row_count)))
        .ok_or_else(|| replacement_payload_error("columnar page slice is out of bounds"))?;
    let identities = identities
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("slicing preserves the string array type");
    let encode_identity = |identity: &str| -> Result<Vec<u8>, LixError> {
        let row_pk = RowPk::from_json_array_text(identity)
            .map_err(|error| replacement_payload_error(&error.to_string()))?;
        Ok(encode_key_ref(TrackedStateKeyRef {
            schema_key: &manifest.namespace,
            file_id: None,
            row_pk: &row_pk,
        }))
    };
    if identities.is_empty()
        || encode_identity(identities.value(0))? != descriptor.first_key
        || encode_identity(identities.value(identities.len() - 1))? != descriptor.last_key
    {
        return Err(replacement_payload_error(
            "columnar page slice disagrees with current-state key fences",
        ));
    }
    let identity_rows = (output_indices.len() > 4).then(|| columnar_identity_row_map(identities));
    let group_base = manifest.groups[..group_index]
        .iter()
        .try_fold(0usize, |sum, group| {
            sum.checked_add(group.row_count as usize)
        })
        .ok_or_else(|| replacement_payload_error("columnar group base overflows"))?;
    let slice_ordinal_base = group_base
        .checked_add(
            page_index
                .checked_mul(crate::columnar_row_group::ROW_GROUP_PAGE_ROWS)
                .ok_or_else(|| replacement_payload_error("columnar page base overflows"))?,
        )
        .and_then(|base| base.checked_add(slice_start))
        .ok_or_else(|| replacement_payload_error("columnar slice base overflows"))?;
    for &output_index in output_indices {
        let key = decode_key(&encoded_keys[output_index])?;
        if key.schema_key != manifest.namespace || key.file_id.is_some() {
            continue;
        }
        let identity = key.row_pk.as_json_array_text()?;
        let row_index = match &identity_rows {
            Some(rows) => rows.get(identity.as_str()).copied(),
            None => {
                (0..identities.len()).find(|&row_index| identities.value(row_index) == identity)
            }
        };
        let Some(row_index) = row_index else {
            continue;
        };
        let ordinal = slice_ordinal_base
            .checked_add(row_index)
            .ok_or_else(|| replacement_payload_error("columnar row ordinal overflows"))?;
        let packed = u32::try_from(ordinal)
            .map_err(|_| replacement_payload_error("columnar row ordinal exceeds u32"))?
            .checked_add(1)
            .ok_or_else(|| replacement_payload_error("columnar change address overflows"))?;
        let owner = CommitId::new(uuid::Uuid::from_bytes(source.owner_commit_id));
        values[output_index] = Some(TrackedStateIndexValue {
            change_id: change_id_from_packed_address(owner, packed),
            commit_id: owner,
            deleted: false,
            created_at: source.uniform_created_at,
            updated_at: source.uniform_updated_at,
        });
    }
    Ok(())
}

fn columnar_identity_row_map(
    identities: &datafusion::arrow::array::StringArray,
) -> HashMap<&str, usize> {
    (0..datafusion::arrow::array::Array::len(identities))
        .map(|row_index| (identities.value(row_index), row_index))
        .collect()
}

pub(crate) async fn load_complete_current_state_values_from_scoped_root(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &CurrentStateScopedRangeRoot,
    encoded_keys: &[Bytes],
) -> Result<Option<Vec<Option<TrackedStateIndexValue>>>, LixError> {
    if encoded_keys.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let scopes = encoded_keys
        .iter()
        .map(|encoded_key| {
            let key = crate::tracked_state::codec::decode_key_borrowed(encoded_key)?;
            super::current_state_envelope::current_state_scope_prefix_from_parts(
                key.schema_key.as_ref(),
                key.file_id.as_deref(),
            )
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let requests = scopes
        .iter()
        .zip(encoded_keys)
        .map(|(scope, key)| (scope, key.as_ref()))
        .collect::<Vec<_>>();
    let routes =
        super::scoped_range::route_scoped_range_covered_points(store, &root.tree, &requests)
            .await?;
    if routes.iter().any(|route| !route.scope_covered) {
        return Ok(None);
    }
    let descriptors = routes
        .into_iter()
        .map(|route| {
            route
                .covered_part
                .as_ref()
                .map(super::current_state_envelope::current_state_descriptor_from_scoped_range_part)
                .transpose()
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    Ok(Some(
        load_current_state_values_from_descriptors(store, encoded_keys, descriptors).await?,
    ))
}

/// Uses a manifest returned by [`load_commit_state_manifest`] or
/// [`load_commit_state_manifests`]. The opaque handle proves that the manifest
/// came from the immutable physical authority in the caller's coherent read.
#[cfg(feature = "storage-benches")]
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

/// Tries the scoped-range serving root from a one-read replay manifest. The inner cost
/// gates run before publication validation, so shallow OLTP replay retains its
/// original one-manifest read while any serving hit is authenticated first.
pub(crate) async fn load_complete_current_state_values_from_replay_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    encoded_keys: &[Bytes],
) -> Result<Option<Vec<Option<TrackedStateIndexValue>>>, LixError> {
    // `load_point_replay_commit_state` decoded this exact manifest from the
    // immutable physical manifest, including its certified scoped root.
    // Re-reading the immutable manifest would add point I/O without
    // strengthening that authority.
    load_complete_current_state_values_encoded_inner(store, state, encoded_keys, false, true).await
}

fn current_inventory_may_contain_any_key(
    state: &CommitStateManifest,
    encoded_keys: &[Bytes],
) -> Result<bool, LixError> {
    let manifest = commit_delta_manifest_from_inventory(&state.mutations);
    if let Some(parts) = manifest.columnar_parts.as_ref() {
        return Ok(encoded_keys.iter().any(|key| {
            parts.first_key.as_slice() <= key.as_ref() && key.as_ref() <= parts.last_key.as_slice()
        }));
    }
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

#[cfg(test)]
pub(crate) fn validate_current_state_scoped_range_serving_base_manifest(
    state: &CommitStateManifest,
    serving_base: Option<&CommitStateManifest>,
) -> Result<(), LixError> {
    let Some(root) = state.current_state_scoped_ranges.as_ref() else {
        return Ok(());
    };
    let expected_base_commit_id = serving_base.map(|base| base.commit_id);
    let expected_base_root = serving_base
        .and_then(|base| base.current_state_scoped_ranges.as_ref())
        .map(|base| base.tree.root_id);
    if root.serving_base_commit_id != expected_base_commit_id
        || root.serving_base_root_id != expected_base_root
    {
        return Err(replacement_payload_error(
            "current-state scoped-range transition disagrees with its serving base",
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
        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
        selected_source_commit_id: inventory.selected_source_commit_id,
        member_count: inventory.member_count,
        selection_fingerprint: inventory.selection_fingerprint,
        direct_segment_row_counts: inventory.direct_part_row_counts.clone(),
        single_partition: inventory.single_partition.clone(),
        lifecycle_summary: inventory.lifecycle_summary.clone(),
        replacement_generation: inventory.replacement_generation.clone(),
        replacement_parts: inventory.replacement_parts.clone(),
        columnar_parts: inventory.columnar_parts.clone(),
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
    initialized: bool,
    bounded_root: Option<super::mutation_directory::MutationDirectoryRoot>,
    bounded_part: Option<(usize, u16, CommitDeltaSegmentBounds)>,
    segment_index: Option<usize>,
    segment: Option<Arc<DecodedCommitDeltaSegment>>,
    next_entry_index: usize,
}

#[derive(Default)]
struct CommitDeltaPointReadCacheInner {
    authorities: VecDeque<(CommitId, Arc<AuthenticatedReplayCommitStateManifest>)>,
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
            initialized: false,
            bounded_root: None,
            bounded_part: None,
            segment_index: None,
            segment: None,
            next_entry_index: 0,
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
        if !self.initialized {
            let state = match cache.authority(self.commit_id)? {
                Some(state) => state,
                None => {
                    let Some(state) = load_point_replay_commit_state(store, self.commit_id).await?
                    else {
                        return Ok(None);
                    };
                    cache.remember_authority(Arc::clone(&state))?;
                    state
                }
            };
            if state.mutations.selected_source_commit_id().is_some() {
                return Ok(None);
            }
            if let Some(root) = state.mutation_directory_root.as_ref().filter(|root| {
                root.layout == super::mutation_directory::LAYOUT_BOUNDED_DIRECT
                    || root.layout == super::mutation_directory::LAYOUT_BOUNDED_INDIRECT
            }) {
                self.bounded_root = Some(root.clone());
            } else if state.mutation_directory_root.is_some()
                || state.mutations.inline_part.is_empty()
            {
                // Point/range selectors are meaningful only for bounded
                // directory layouts. Do not reconstruct an alternate catalog
                // for ordinal or columnar layouts; the caller must use its
                // canonical fail-closed path.
                return Ok(None);
            } else {
                let decoded = match cache.segment(self.commit_id, 0, None)? {
                    Some(decoded) => decoded,
                    None => decode_owned_commit_delta_segment(&state.mutations.inline_part, None)?,
                };
                if decoded.leaf.len() != state.mutations.member_count as usize {
                    return Err(replacement_payload_error(
                        "inline membership payload row count disagrees with authenticated authority",
                    ));
                }
                self.segment = Some(decoded);
                self.segment_index = Some(0);
            }
            // Mark the cursor initialized only after an authenticated
            // membership authority has selected a serving mode. Unsupported,
            // rootless, and missing authorities return `None` above; leaving
            // the cursor uninitialized prevents a later call from entering
            // the scan loop with no segment.
            self.initialized = true;
        }

        if let Some(root) = self.bounded_root.clone() {
            if self
                .bounded_part
                .as_ref()
                .is_some_and(|(_, _, bounds)| encoded_key < bounds.first_key.as_slice())
            {
                return Ok(Some(false));
            }
            if self
                .bounded_part
                .as_ref()
                .is_none_or(|(_, _, bounds)| bounds.last_key.as_slice() < encoded_key)
            {
                let points = [Bytes::copy_from_slice(encoded_key)];
                let mut runs = super::mutation_directory::load_mutation_part_read_plan(
                    store,
                    &root,
                    super::mutation_directory::MutationDirectoryReadSelection::SortedUniquePoints(
                        &points,
                    ),
                )
                .await?
                .into_runs()
                .into_iter();
                let Some(run) = runs.next() else {
                    return Ok(Some(false));
                };
                if runs.next().is_some() {
                    return Err(replacement_payload_error(
                        "one membership point selected multiple immutable parts",
                    ));
                }
                let super::mutation_directory::MutationDirectoryEntry::Bounded {
                    part,
                    direct_row_count,
                } = run.entry
                else {
                    return Err(replacement_payload_error(
                        "bounded membership cursor selected a non-bounded part",
                    ));
                };
                self.bounded_part = Some((
                    usize::try_from(run.entry_index)
                        .map_err(|_| replacement_payload_error("part index exceeds usize"))?,
                    direct_row_count,
                    CommitDeltaSegmentBounds {
                        first_key: part.first_key,
                        last_key: part.last_key,
                        replacement_part: part.replacement_part,
                    },
                ));
            }
            let (segment_index, direct_row_count, bounds) = self
                .bounded_part
                .as_ref()
                .expect("bounded cursor retained the selected part");
            let segment_index = *segment_index;
            if self.segment_index != Some(segment_index) || self.segment.is_none() {
                self.segment = cache.segment(self.commit_id, segment_index, Some(bounds))?;
                if self.segment.is_none() {
                    let storage_key =
                        commit_delta_segment_key_for_bounds(self.commit_id, segment_index, bounds)?;
                    let loaded = PointReadPlan::from_unique_keys(
                        TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
                        vec![StorageKey(Bytes::from(storage_key))],
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
                            replacement_payload_error(
                                "bounded membership cursor references a missing immutable part",
                            )
                        })?;
                    self.segment = Some(decode_owned_commit_delta_segment(&bytes, Some(bounds))?);
                }
                validate_bounded_direct_row_count(
                    root.layout,
                    *direct_row_count,
                    self.segment
                        .as_ref()
                        .expect("bounded membership segment was loaded")
                        .leaf
                        .len(),
                )?;
                self.segment_index = Some(segment_index);
                self.next_entry_index = 0;
            }
        }
        let segment = self
            .segment
            .as_ref()
            .expect("membership cursor loaded its current immutable part");
        let mut linear_probes = 0_usize;
        while self.next_entry_index < segment.leaf.len() {
            let entry = segment.leaf.entry(self.next_entry_index).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state packed commit_delta leaf has a missing entry",
                )
            })?;
            match entry.key.cmp(encoded_key) {
                std::cmp::Ordering::Less => {
                    self.next_entry_index += 1;
                    linear_probes += 1;
                    if linear_probes == 8 {
                        self.next_entry_index = commit_delta_entry_lower_bound_from(
                            &segment.leaf,
                            encoded_key,
                            self.next_entry_index,
                        )?;
                    }
                }
                std::cmp::Ordering::Greater => return Ok(Some(false)),
                std::cmp::Ordering::Equal => {
                    self.next_entry_index += 1;
                    let value = decode_value(entry.value)?;
                    if value.commit_id != self.commit_id {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "tracked_state packed commit_delta for commit '{}' contains an entry for commit '{}'",
                                self.commit_id, value.commit_id
                            ),
                        ));
                    }
                    return Ok(Some(!value.deleted));
                }
            }
        }
        Ok(Some(false))
    }
}

impl CommitDeltaPointReadCache {
    fn authority(
        &self,
        commit_id: CommitId,
    ) -> Result<Option<Arc<AuthenticatedReplayCommitStateManifest>>, LixError> {
        let mut cache = self.inner.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "transaction commit-delta point cache lock is poisoned",
            )
        })?;
        let Some(position) = cache
            .authorities
            .iter()
            .position(|(cached_commit_id, _)| *cached_commit_id == commit_id)
        else {
            return Ok(None);
        };
        let entry = cache
            .authorities
            .remove(position)
            .expect("located authenticated commit authority cache entry");
        let authority = Arc::clone(&entry.1);
        cache.authorities.push_back(entry);
        Ok(Some(authority))
    }

    fn remember_authority(
        &self,
        authority: Arc<AuthenticatedReplayCommitStateManifest>,
    ) -> Result<(), LixError> {
        let mut cache = self.inner.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "transaction commit-delta point cache lock is poisoned",
            )
        })?;
        if let Some(position) = cache
            .authorities
            .iter()
            .position(|(commit_id, _)| *commit_id == authority.commit_id)
        {
            if cache.authorities[position].1.as_ref() != authority.as_ref() {
                return Err(replacement_payload_error(
                    "transaction point cache received mismatched authenticated authority",
                ));
            }
            cache.authorities.remove(position);
        }
        cache
            .authorities
            .push_back((authority.commit_id, authority));
        while cache.authorities.len() > DECODED_COMMIT_DELTA_CACHE_MAX_ENTRIES {
            cache.authorities.pop_front();
        }
        Ok(())
    }

    fn remember_authenticated_state(
        &self,
        state: &AuthenticatedReplayCommitStateManifest,
    ) -> Result<(), LixError> {
        if let Some(cached) = self.authority(state.commit_id)? {
            if cached.as_ref() != state {
                return Err(replacement_payload_error(
                    "transaction point cache received mismatched authenticated authority",
                ));
            }
            return Ok(());
        }
        self.remember_authority(Arc::new(state.clone()))
    }

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
/// ordinals plus the typed row key; repeated schema and file metadata is
/// stored once for the whole scan.
#[derive(Debug, Default)]
pub(crate) struct DecodedCommitDeltaBatch {
    arenas: Vec<DecodedLeafNodeRef>,
    columnar_keys: Bytes,
    schema_keys: Vec<SharedStr>,
    file_ids: Vec<SharedStr>,
    rows: Vec<DecodedCommitDeltaRow>,
    values: Vec<TrackedStateIndexValue>,
}

#[derive(Debug)]
struct DecodedCommitDeltaRow {
    arena_ordinal: u32,
    entry_ordinal: u16,
    columnar_key: Option<BufferRange>,
    schema_key_ordinal: u32,
    /// `u32::MAX` is the null file-id sentinel.
    file_id_ordinal: u32,
    row_pk: RowPk,
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
            row_pk: &row.row_pk,
        }
    }

    pub(crate) fn value(self) -> &'a TrackedStateIndexValue {
        &self.batch.values[self.ordinal]
    }

    /// Returns a zero-copy view retaining the selected segment arena.
    #[cfg(test)]
    pub(crate) fn encoded_key(&self) -> Bytes {
        let row = &self.batch.rows[self.ordinal];
        if let Some(range) = row.columnar_key {
            return self
                .batch
                .columnar_keys
                .slice(range.offset()..range.offset() + range.len());
        }
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
        if let Some(range) = row.columnar_key {
            return &self.batch.columnar_keys[range.offset()..range.offset() + range.len()];
        }
        self.batch.arenas[row.arena_ordinal as usize]
            .key(row.entry_ordinal as usize)
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
    columnar_keys: Vec<u8>,
    schema_keys: CommitDeltaStringInterner,
    file_ids: CommitDeltaStringInterner,
    rows: Vec<DecodedCommitDeltaRow>,
    values: Vec<TrackedStateIndexValue>,
}

impl DecodedCommitDeltaBatchBuilder {
    fn with_capacity(row_capacity: usize, arena_capacity: usize) -> Self {
        Self {
            arenas: Vec::with_capacity(arena_capacity),
            columnar_keys: Vec::new(),
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
                columnar_key: None,
                schema_key_ordinal,
                file_id_ordinal,
                row_pk: key.row_pk,
            });
            self.values.push(value);
            Ok(())
        })?;
        if self.rows.len() != first_row {
            self.arenas.push(leaf);
        }
        Ok(())
    }

    fn push_columnar_row(
        &mut self,
        schema_key: &str,
        row_pk: RowPk,
        value: TrackedStateIndexValue,
    ) -> Result<(), LixError> {
        let schema_key_ordinal = self.schema_keys.intern(SharedStr::from(schema_key))?;
        let start = self.columnar_keys.len();
        encode_key_ref_into(
            &mut self.columnar_keys,
            TrackedStateKeyRef {
                schema_key,
                file_id: None,
                row_pk: &row_pk,
            },
        );
        let columnar_key = BufferRange::new(start, self.columnar_keys.len() - start);
        self.rows.push(DecodedCommitDeltaRow {
            arena_ordinal: u32::MAX,
            entry_ordinal: 0,
            columnar_key: Some(columnar_key),
            schema_key_ordinal,
            file_id_ordinal: u32::MAX,
            row_pk,
        });
        self.values.push(value);
        Ok(())
    }

    fn finish(self) -> DecodedCommitDeltaBatch {
        DecodedCommitDeltaBatch {
            arenas: self.arenas,
            columnar_keys: Bytes::from(self.columnar_keys),
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
    Ok(load_snapshot_commit_root(store, commit_id)
        .await?
        .map(|metadata| metadata.root_id))
}

/// Resolves canonical snapshot metadata from immutable physical authority.
///
/// Tree chunks are rebuildable by content hash, but the manifest-owned root
/// pointer is the authority that permits readers to serve them.
pub(crate) async fn load_snapshot_commit_root(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: &str,
) -> Result<Option<TrackedStateCommitRoot>, LixError> {
    let commit_id = CommitId::parse_lix(commit_id, "tracked-state snapshot root lookup")?;
    // Rootless commits are the common bounded-replay layout. Probe physical
    // authority first so they do not pay a second semantic lookup merely to
    // prove the absence of a root pointer. A present pointer still requires
    // semantic liveness before it may authorize any chunk reads.
    let Some(snapshot_root) = load_manifest_snapshot_commit_root(store, commit_id).await? else {
        return Ok(None);
    };
    let commit_ids = [commit_id];
    let mut changelog = ChangelogContext::new().reader(store);
    let semantic_commit_exists = changelog
        .load_commits(CommitLoadRequest {
            commit_ids: &commit_ids,
        })
        .await?
        .into_iter()
        .next()
        .and_then(|(_, record)| record)
        .is_some();
    if !semantic_commit_exists {
        return Ok(None);
    }
    Ok(Some(snapshot_root))
}

/// Loads the physical pointer without granting semantic commit liveness.
///
/// Only code that has already proved semantic liveness and integrity tests may
/// use this helper; all commit-addressed read APIs must use
/// [`load_snapshot_commit_root`].
pub(super) async fn load_manifest_snapshot_commit_root(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<TrackedStateCommitRoot>, LixError> {
    let Some(bytes) = get_one(
        store,
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        commit_state_manifest_key(commit_id),
    )
    .await?
    else {
        return Ok(None);
    };
    let stored = decode_stored_commit_state_manifest(&bytes)?;
    if stored.commit_id != commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_state_manifest key for commit '{commit_id}' contains manifest for commit '{}'",
                stored.commit_id
            ),
        ));
    }
    Ok(stored.snapshot_root.map(|root| *root))
}

fn commit_delta_manifest_key(commit_id: CommitId) -> Vec<u8> {
    commit_id.as_uuid().as_bytes().to_vec()
}

fn commit_state_manifest_key(commit_id: CommitId) -> Vec<u8> {
    commit_id.as_uuid().as_bytes().to_vec()
}

fn commit_mutation_inventory_key(commit_id: CommitId) -> Vec<u8> {
    commit_id.as_uuid().as_bytes().to_vec()
}

/// Commit authority loaded from the immutable manifest plane. Only this
/// opaque handle may bypass publication-time catalog re-derivation.
#[derive(Clone, Debug)]
pub(crate) struct PublishedCommitStateManifest {
    manifest: CommitStateManifest,
}

/// Header-only immutable authority for topology and lifecycle work.
/// Scoped-range transition metadata is authenticated directly by the compact
/// header, so consumers do not need the mutation catalog or directory.
#[derive(Clone, Debug)]
pub(crate) struct PublishedCommitStateTopology {
    header: StoredCommitStateManifest,
}

impl PublishedCommitStateTopology {
    pub(crate) fn commit_id(&self) -> CommitId {
        self.header.commit_id
    }

    pub(crate) fn replay_debt(&self) -> crate::tracked_state::CommitStateReplayDebt {
        self.header.replay_debt
    }

    pub(crate) fn mutation_member_count(&self) -> u32 {
        self.header.mutation_member_count
    }

    pub(crate) fn current_state_scoped_ranges(&self) -> Option<&CurrentStateScopedRangeRoot> {
        self.header.current_state_scoped_ranges.as_deref()
    }

    fn topology_ref(&self) -> super::scoped_current_state::CommitStateTopologyRef<'_> {
        super::scoped_current_state::CommitStateTopologyRef {
            commit_id: self.header.commit_id,
            touched_scope_filter: &self.header.touched_scope_filter,
            current_state_scoped_ranges: self.header.current_state_scoped_ranges.as_deref(),
        }
    }
}

/// One-read immutable authority used by point replay. The wrapper prevents a
/// freely constructed manifest from claiming authoritative catalog coverage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthenticatedReplayCommitStateManifest {
    manifest: CommitStateManifest,
    mutation_directory_root: Option<super::mutation_directory::MutationDirectoryRoot>,
}

/// Same-write-set authority produced only after immutable physical publication.
/// This lets a later commit in one atomic transaction consume its parent catalog
/// without treating a freely constructible manifest as provenance.
pub(crate) struct StagedCommitStateManifest {
    manifest: CommitStateManifest,
    write_set_id: u64,
}

/// Authenticated topology input accepted by cumulative physical publication.
/// The variants prevent freely constructed manifests from minting a complete
/// touched-scope certificate.
#[derive(Clone, Copy)]
pub(crate) enum CertifiedCommitStateTopologyParent<'a> {
    #[cfg(any(test, feature = "storage-benches"))]
    Published(&'a PublishedCommitStateManifest),
    PublishedTopology(&'a PublishedCommitStateTopology),
    Staged(&'a StagedCommitStateManifest),
}

impl<'a> CertifiedCommitStateTopologyParent<'a> {
    fn topology(
        self,
        writes: &StorageWriteSet,
    ) -> Result<super::scoped_current_state::CommitStateTopologyRef<'a>, LixError> {
        match self {
            #[cfg(any(test, feature = "storage-benches"))]
            Self::Published(parent) => Ok((&parent.manifest).into()),
            Self::PublishedTopology(parent) => Ok(parent.topology_ref()),
            Self::Staged(parent) => {
                if parent.write_set_id != writes.identity() {
                    return Err(replacement_payload_error(
                        "staged topology parent belongs to a different storage write set",
                    ));
                }
                Ok((&parent.manifest).into())
            }
        }
    }
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

pub(crate) async fn stage_current_state_scoped_ranges_from_topology(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    parents: &[CertifiedCommitStateTopologyParent<'_>],
    selected_source: Option<CertifiedCommitStateTopologyParent<'_>>,
    commit_id: CommitId,
    account_id: &str,
    inventory: &CommitStateMutationInventory,
) -> Result<super::scoped_current_state::CertifiedCommitStatePhysicalPublication, LixError> {
    let parents = parents
        .iter()
        .copied()
        .map(|parent| parent.topology(writes))
        .collect::<Result<Vec<_>, _>>()?;
    let selected_source = selected_source
        .map(|source| source.topology(writes))
        .transpose()?;
    let serving_base = selected_source.or_else(|| parents.first().copied());
    super::scoped_current_state::stage_current_state_scoped_ranges_from_topology_refs(
        store,
        writes,
        &parents,
        selected_source,
        serving_base,
        commit_id,
        account_id,
        inventory,
    )
    .await
}

pub(crate) async fn stage_current_state_scoped_ranges_from_published_parent(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    parent: Option<&PublishedCommitStateManifest>,
    commit_id: CommitId,
    account_id: &str,
    inventory: &CommitStateMutationInventory,
) -> Result<super::scoped_current_state::CertifiedCommitStatePhysicalPublication, LixError> {
    super::scoped_current_state::stage_current_state_scoped_ranges_from_topology_refs(
        store,
        writes,
        parent.map(|parent| (&parent.manifest).into()).as_slice(),
        None,
        parent.map(|parent| (&parent.manifest).into()),
        commit_id,
        account_id,
        inventory,
    )
    .await
}

pub(crate) async fn stage_current_state_scoped_ranges_from_published_topology_parent(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    parent: Option<&PublishedCommitStateTopology>,
    commit_id: CommitId,
    account_id: &str,
    inventory: &CommitStateMutationInventory,
) -> Result<super::scoped_current_state::CertifiedCommitStatePhysicalPublication, LixError> {
    super::scoped_current_state::stage_current_state_scoped_ranges_from_topology_refs(
        store,
        writes,
        parent
            .map(PublishedCommitStateTopology::topology_ref)
            .as_slice(),
        None,
        parent.map(PublishedCommitStateTopology::topology_ref),
        commit_id,
        account_id,
        inventory,
    )
    .await
}

pub(crate) async fn stage_current_state_scoped_ranges_from_staged_parent(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    parent: &StagedCommitStateManifest,
    commit_id: CommitId,
    account_id: &str,
    inventory: &CommitStateMutationInventory,
) -> Result<super::scoped_current_state::CertifiedCommitStatePhysicalPublication, LixError> {
    if parent.write_set_id != writes.identity() {
        return Err(replacement_payload_error(
            "staged scoped-range parent belongs to a different storage write set",
        ));
    }
    super::scoped_current_state::stage_current_state_scoped_ranges_from_topology_refs(
        store,
        writes,
        &[(&parent.manifest).into()],
        None,
        Some((&parent.manifest).into()),
        commit_id,
        account_id,
        inventory,
    )
    .await
}

/// Rewrites one exactly scoped current-state partition from its certified
/// parent post-image and the current commit's certified mutation parts. Untouched
/// parent descriptors are reused byte-for-byte; only intersecting bounded
/// parts and insertion gaps become native current-state data parts.
/// Rewrites one covered scope in the unified current-state serving tree.
///
/// The mutation inventory remains historical authority. This path only
/// derives a bounded post-image serving projection: untouched tree children
/// and part envelopes are retained, while descriptors intersecting authored
/// identities are decoded and replaced with native immutable data parts.
pub(crate) async fn stage_sparse_current_state_scoped_range(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    parent: &CurrentStateScopedRangeRoot,
    scope: &CommitDeltaReplacementScope,
    members: Vec<CommitDeltaMember>,
) -> Result<Option<crate::tracked_state::scoped_range::ScopedRangeRoot>, LixError> {
    use crate::tracked_state::current_state_data_part::CurrentStateDataRow;
    use crate::tracked_state::current_state_envelope::{
        current_state_descriptor_from_scoped_range_part, current_state_scope_prefix,
        scoped_range_part_from_current_state_descriptor,
    };
    use crate::tracked_state::scoped_range::{
        ScopedRangeCoverageMarker, plan_scoped_range_part_splice,
        snapshot_staged_scoped_range_nodes, stage_scoped_range_part_splice,
    };

    if members.iter().any(|member| {
        !member.value.deleted && member.change.snapshot == crate::json_store::JsonSlot::None
    }) {
        // The selected/reference source owns this payload. Until it is
        // available through the same read-your-writes authority, omit this
        // rebuildable serving root and let canonical replay answer the commit.
        return Ok(None);
    }

    let mut mutations = BTreeMap::<Vec<u8>, Option<CurrentStateDataRow>>::new();
    for member in members {
        if member.key.schema_key != scope.schema_key || member.key.file_id != scope.file_id {
            return Err(replacement_payload_error(
                "sparse scoped-range rewrite received a member from another scope",
            ));
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
                "sparse scoped-range rewrite contains a duplicate identity",
            ));
        }
    }
    if mutations.is_empty() {
        return Ok(Some(parent.tree.clone()));
    }

    let mut mutation_rows = mutations.into_iter().map(Some).collect::<Vec<_>>();
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
    let scoped_prefix = current_state_scope_prefix(scope)?;
    let staged_nodes = snapshot_staged_scoped_range_nodes(writes)?;
    let splice = plan_scoped_range_part_splice(
        store,
        writes.identity(),
        staged_nodes,
        &parent.tree,
        &scoped_prefix,
        &encoded_keys,
    )
    .await?;

    let old_marker = splice.coverage().clone();
    let mut old_part_count = 0u64;
    let mut old_row_count = 0u64;
    let mut new_part_count = 0u64;
    let mut new_row_count = 0u64;
    let mut replacements = Vec::with_capacity(splice.leaf_count());
    for leaf_index in 0..splice.leaf_count() {
        let leaf_key_indices = splice.leaf_key_indices(leaf_index);
        let leaf_parts = splice.leaf_parts(leaf_index).collect::<Vec<_>>();
        old_part_count = old_part_count
            .checked_add(leaf_parts.len() as u64)
            .ok_or_else(|| replacement_payload_error("scoped-range part count overflows"))?;

        let leaf_mutations = leaf_key_indices
            .iter()
            .map(|&key_index| {
                mutation_rows[key_index]
                    .take()
                    .expect("each sparse mutation is assigned to one leaf")
            })
            .collect::<Vec<_>>();
        let mut output = Vec::with_capacity(leaf_parts.len() + leaf_key_indices.len());
        let compaction_ranges = sparse_current_state_fragment_compaction_ranges(&leaf_parts)?;
        let mut compaction_index = 0usize;
        let mut part_index = 0usize;
        let mut pending = leaf_mutations.into_iter().peekable();
        while part_index < leaf_parts.len() {
            if compaction_ranges
                .get(compaction_index)
                .is_some_and(|&(start, _)| start == part_index)
            {
                let (start, end) = compaction_ranges[compaction_index];
                let first = leaf_parts[start];
                let last = leaf_parts[end - 1];
                let mut gap = Vec::new();
                while pending
                    .peek()
                    .is_some_and(|(key, _)| key.as_slice() < first.first_key.as_slice())
                {
                    let (_, row) = pending.next().expect("peeked sparse mutation");
                    if let Some(row) = row {
                        gap.push(row);
                    }
                }
                stage_scoped_native_current_state_rows(writes, &gap, true, &mut output)?;
                let mut rows = BTreeMap::new();
                for &part in &leaf_parts[start..end] {
                    old_row_count = old_row_count.checked_add(part.row_count).ok_or_else(|| {
                        replacement_payload_error("scoped-range row count overflows")
                    })?;
                    let descriptor = current_state_descriptor_from_scoped_range_part(part)?;
                    for row in load_scoped_current_state_descriptor_rows(store, writes, &descriptor)
                        .await?
                    {
                        if rows.insert(row.encoded_key.clone(), row).is_some() {
                            return Err(replacement_payload_error(
                                "fragmented current-state run contains duplicate identities",
                            ));
                        }
                    }
                }
                while pending
                    .peek()
                    .is_some_and(|(key, _)| key.as_slice() <= last.last_key.as_slice())
                {
                    let (key, row) = pending.next().expect("peeked compacted mutation");
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
                stage_scoped_native_current_state_rows(
                    writes,
                    &rows.into_values().collect::<Vec<_>>(),
                    false,
                    &mut output,
                )?;
                part_index = end;
                compaction_index += 1;
                continue;
            }

            let part = leaf_parts[part_index];
            old_row_count = old_row_count
                .checked_add(part.row_count)
                .ok_or_else(|| replacement_payload_error("scoped-range row count overflows"))?;
            let descriptor = current_state_descriptor_from_scoped_range_part(part)?;
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
            stage_scoped_native_current_state_rows(writes, &gap, true, &mut output)?;

            let touches_descriptor = pending
                .peek()
                .is_some_and(|(key, _)| key.as_slice() <= descriptor.last_key.as_slice());
            if !touches_descriptor {
                output.push(descriptor);
                part_index += 1;
                continue;
            }
            let rows =
                load_scoped_current_state_descriptor_rows(store, writes, &descriptor).await?;
            let mut descriptor_mutations = Vec::new();
            while pending
                .peek()
                .is_some_and(|(key, _)| key.as_slice() <= descriptor.last_key.as_slice())
            {
                descriptor_mutations.push(pending.next().expect("peeked sparse mutation"));
            }
            stage_fragmented_scoped_current_state_descriptor(
                writes,
                &descriptor,
                &rows,
                descriptor_mutations,
                &mut output,
            )?;
            part_index += 1;
        }
        let tail = pending
            .filter_map(|(_, row)| row)
            .collect::<Vec<CurrentStateDataRow>>();
        stage_scoped_native_current_state_rows(writes, &tail, true, &mut output)?;

        new_part_count = new_part_count
            .checked_add(output.len() as u64)
            .ok_or_else(|| replacement_payload_error("scoped-range part count overflows"))?;
        new_row_count = output.iter().try_fold(new_row_count, |sum, descriptor| {
            sum.checked_add(u64::from(descriptor.row_count))
                .ok_or_else(|| replacement_payload_error("scoped-range row count overflows"))
        })?;
        replacements.push(
            output
                .iter()
                .map(|descriptor| {
                    scoped_range_part_from_current_state_descriptor(scope, descriptor)
                })
                .collect::<Result<Vec<_>, LixError>>()?,
        );
    }
    if mutation_rows.iter().any(Option::is_some) {
        return Err(replacement_payload_error(
            "sparse scoped-range splice did not assign every mutation",
        ));
    }

    let part_count = u64::from(old_marker.part_count)
        .checked_sub(old_part_count)
        .and_then(|count| count.checked_add(new_part_count))
        .and_then(|count| u32::try_from(count).ok())
        .ok_or_else(|| replacement_payload_error("scoped-range part closure overflows"))?;
    let row_count = old_marker
        .row_count
        .checked_sub(old_row_count)
        .and_then(|count| count.checked_add(new_row_count))
        .ok_or_else(|| replacement_payload_error("scoped-range row closure overflows"))?;
    let marker = ScopedRangeCoverageMarker {
        scope: scoped_prefix,
        row_count,
        part_count,
    };
    Ok(Some(
        stage_scoped_range_part_splice(writes, splice, marker, replacements)?.root,
    ))
}

const SPARSE_CURRENT_STATE_COMPACTION_MIN_PARTS: usize = 32;
const SPARSE_CURRENT_STATE_MAX_PROJECTED_SOURCE_PARTS: usize = 16;

fn sparse_current_state_fragment_compaction_ranges(
    parts: &[&crate::tracked_state::scoped_range::ScopedRangePart],
) -> Result<Vec<(usize, usize)>, LixError> {
    let mut ranges = Vec::new();
    let mut start = 0usize;
    while start < parts.len() {
        if !crate::tracked_state::current_state_envelope::current_state_descriptor_from_scoped_range_part(parts[start])?.fragmented {
            start += 1;
            continue;
        }
        let mut end = start + 1;
        while end < parts.len()
            && crate::tracked_state::current_state_envelope::current_state_descriptor_from_scoped_range_part(parts[end])?.fragmented
        {
            end += 1;
        }
        if end - start >= SPARSE_CURRENT_STATE_COMPACTION_MIN_PARTS {
            ranges.push((start, end));
        }
        start = end;
    }
    Ok(ranges)
}

/// Applies sparse mutations without copying an immutable source part's
/// untouched rows. Descriptor slices retain the original payload and only
/// authored post-images become new native parts.
fn stage_fragmented_scoped_current_state_descriptor(
    writes: &mut StorageWriteSet,
    descriptor: &CurrentStatePartDescriptor,
    rows: &[crate::tracked_state::current_state_data_part::CurrentStateDataRow],
    mutations: Vec<(
        Vec<u8>,
        Option<crate::tracked_state::current_state_data_part::CurrentStateDataRow>,
    )>,
    output: &mut Vec<CurrentStatePartDescriptor>,
) -> Result<(), LixError> {
    let mutations = mutations
        .into_iter()
        .filter(|(key, mutation)| {
            mutation.is_some()
                || rows
                    .binary_search_by(|row| row.encoded_key.as_slice().cmp(key.as_slice()))
                    .is_ok()
        })
        .collect::<Vec<_>>();
    if mutations.is_empty() {
        output.push(descriptor.clone());
        return Ok(());
    }
    if sparse_current_state_projected_source_parts(rows, &mutations)
        > SPARSE_CURRENT_STATE_MAX_PROJECTED_SOURCE_PARTS
    {
        let mut post_image = rows
            .iter()
            .cloned()
            .map(|row| (row.encoded_key.clone(), row))
            .collect::<BTreeMap<_, _>>();
        for (key, row) in mutations {
            match row {
                Some(mut row) => {
                    if let Some(previous) = post_image.get(&key) {
                        row.value.created_at = previous.value.created_at;
                    }
                    post_image.insert(key, row);
                }
                None => {
                    post_image.remove(&key);
                }
            }
        }
        return stage_scoped_native_current_state_rows(
            writes,
            &post_image.into_values().collect::<Vec<_>>(),
            false,
            output,
        );
    }
    let mut retained_start = 0usize;
    let mut native_run = Vec::new();
    for (key, mut mutation) in mutations {
        let insertion = rows.binary_search_by(|row| row.encoded_key.as_slice().cmp(key.as_slice()));
        if insertion.is_err() && mutation.is_none() {
            // Deleting an already absent identity is a physical no-op.
            continue;
        }
        let split = insertion.unwrap_or_else(|index| index);
        if retained_start < split {
            stage_scoped_native_current_state_rows(writes, &native_run, true, output)?;
            native_run.clear();
        }
        stage_retained_current_state_slice(descriptor, rows, retained_start, split, output)?;
        if let Ok(index) = insertion {
            if let Some(row) = mutation.as_mut() {
                row.value.created_at = rows[index].value.created_at;
            }
            retained_start = index + 1;
        } else {
            retained_start = split;
        }
        if let Some(row) = mutation {
            native_run.push(row);
        }
    }
    stage_scoped_native_current_state_rows(writes, &native_run, true, output)?;
    stage_retained_current_state_slice(descriptor, rows, retained_start, rows.len(), output)
}

fn sparse_current_state_projected_source_parts(
    rows: &[crate::tracked_state::current_state_data_part::CurrentStateDataRow],
    mutations: &[(
        Vec<u8>,
        Option<crate::tracked_state::current_state_data_part::CurrentStateDataRow>,
    )],
) -> usize {
    let mut retained_start = 0usize;
    let mut native_open = false;
    let mut part_count = 0usize;
    for (key, mutation) in mutations {
        let insertion = rows.binary_search_by(|row| row.encoded_key.as_slice().cmp(key.as_slice()));
        if insertion.is_err() && mutation.is_none() {
            continue;
        }
        let split = insertion.unwrap_or_else(|index| index);
        if retained_start < split {
            part_count += usize::from(native_open) + 1;
            native_open = false;
        }
        retained_start = insertion.map_or(split, |index| index + 1);
        native_open |= mutation.is_some();
    }
    part_count += usize::from(native_open);
    part_count + usize::from(retained_start < rows.len())
}

fn stage_retained_current_state_slice(
    descriptor: &CurrentStatePartDescriptor,
    rows: &[crate::tracked_state::current_state_data_part::CurrentStateDataRow],
    start: usize,
    end: usize,
    output: &mut Vec<CurrentStatePartDescriptor>,
) -> Result<(), LixError> {
    if start >= end {
        return Ok(());
    }
    let mut retained = descriptor.clone();
    retained.first_key = rows[start].encoded_key.clone();
    retained.last_key = rows[end - 1].encoded_key.clone();
    retained.source_row_offset = retained
        .source_row_offset
        .checked_add(
            u16::try_from(start)
                .map_err(|_| replacement_payload_error("current-state slice offset overflows"))?,
        )
        .ok_or_else(|| replacement_payload_error("current-state slice offset overflows"))?;
    retained.row_count = u16::try_from(end - start)
        .map_err(|_| replacement_payload_error("current-state slice row count overflows"))?;
    retained.fragmented = true;
    output.push(retained);
    Ok(())
}

fn stage_scoped_native_current_state_rows(
    writes: &mut StorageWriteSet,
    rows: &[crate::tracked_state::current_state_data_part::CurrentStateDataRow],
    fragmented: bool,
    output: &mut Vec<CurrentStatePartDescriptor>,
) -> Result<(), LixError> {
    use crate::tracked_state::current_state_data_part::{
        CURRENT_STATE_DATA_PART_REFS_SPACE, CURRENT_STATE_DATA_PART_SPACE,
        encode_bounded_current_state_data_parts,
    };

    if rows.is_empty() {
        return Ok(());
    }
    for part in encode_bounded_current_state_data_parts(rows)? {
        stage_scoped_current_state_bytes(
            writes,
            CURRENT_STATE_DATA_PART_SPACE,
            part.digest,
            &part.bytes,
        )?;
        stage_scoped_current_state_bytes(
            writes,
            CURRENT_STATE_DATA_PART_REFS_SPACE,
            part.digest,
            &part.refs_bytes,
        )?;
        output.push(CurrentStatePartDescriptor {
            first_key: part.first_key,
            last_key: part.last_key,
            content_digest: part.digest,
            source: CurrentStatePartSource::NativeDataPart {
                payload_refs_digest: part.refs_digest,
            },
            source_row_offset: 0,
            row_count: part.row_count,
            fragmented,
        });
    }
    Ok(())
}

fn stage_scoped_current_state_bytes(
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

async fn load_scoped_current_state_descriptor_rows(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    descriptor: &CurrentStatePartDescriptor,
) -> Result<Vec<crate::tracked_state::current_state_data_part::CurrentStateDataRow>, LixError> {
    use crate::tracked_state::current_state_data_part::{
        CURRENT_STATE_DATA_PART_SPACE, CurrentStateDataRow, decode_current_state_data_part,
    };

    let rows = match &descriptor.source {
        CurrentStatePartSource::Replacement(source) => {
            let owner = CommitId::new(uuid::Uuid::from_bytes(source.owner_commit_id));
            let mut physical_key = commit_delta_segment_key(owner, source.part_index as usize)?;
            physical_key.extend_from_slice(&descriptor.content_digest);
            let bytes = if let Some(bytes) =
                writes.staged_value(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &physical_key)
            {
                bytes
            } else {
                get_one(
                    store,
                    TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
                    physical_key,
                )
                .await?
                .ok_or_else(|| replacement_payload_error("current-state source part is missing"))?
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
                    let packed = source
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
                            created_at: source.uniform_created_at,
                            updated_at: source.uniform_updated_at,
                        },
                        snapshot: owned_scoped_json_slot(decoded.snapshot(ordinal)?.ok_or_else(
                            || replacement_payload_error("replacement source omitted snapshot"),
                        )?),
                        metadata: owned_scoped_json_slot(decoded.metadata(ordinal)?.ok_or_else(
                            || replacement_payload_error("replacement source omitted metadata"),
                        )?),
                    })
                })
                .collect::<Result<Vec<_>, LixError>>()?
        }
        CurrentStatePartSource::NativeDataPart { .. } => {
            let physical_key = descriptor.content_digest.to_vec();
            let bytes = if let Some(bytes) =
                writes.staged_value(CURRENT_STATE_DATA_PART_SPACE, &physical_key)
            {
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
        CurrentStatePartSource::ColumnarPage(source) => {
            let id = crate::columnar_row_group::RowGroupSetId::new(source.source_id);
            let staged_manifest =
                crate::columnar_row_group::load_staged_row_group_manifest(writes, id)?;
            let manifest = match staged_manifest {
                Some(manifest) => manifest,
                None => crate::columnar_row_group::load_row_group_manifest(store, id)
                    .await?
                    .ok_or_else(|| {
                        replacement_payload_error("columnar current-state manifest is missing")
                    })?,
            };
            let schema_key = decode_key(&descriptor.first_key)?.schema_key;
            if manifest.content_digest()? != descriptor.content_digest
                || manifest.namespace != schema_key
                || crate::row_columnar::row_group_set_id(
                    CommitId::new(uuid::Uuid::from_bytes(source.owner_commit_id)),
                    &manifest.namespace,
                )
                .as_bytes()
                    != source.source_id
            {
                return Err(replacement_payload_error(
                    "columnar current-state source disagrees with its descriptor",
                ));
            }
            let projection = (0..manifest.fields.len()).collect::<Vec<_>>();
            let group_index = usize::try_from(source.part_index)
                .map_err(|_| replacement_payload_error("columnar group index exceeds usize"))?;
            let page_index = usize::from(source.source_page_index);
            let batch = if crate::columnar_row_group::load_staged_row_group_manifest(writes, id)?
                .is_some()
            {
                crate::columnar_row_group::load_staged_row_group_page(
                    writes,
                    id,
                    &manifest,
                    group_index,
                    page_index,
                    &projection,
                )?
            } else {
                crate::columnar_row_group::load_row_group_page(
                    store,
                    id,
                    &manifest,
                    group_index,
                    page_index,
                    &projection,
                )
                .await?
            };
            let group_base = manifest.groups[..group_index]
                .iter()
                .try_fold(0usize, |sum, group| {
                    sum.checked_add(group.row_count as usize)
                })
                .ok_or_else(|| replacement_payload_error("columnar group base overflows"))?;
            let page_base = group_base
                .checked_add(
                    page_index
                        .checked_mul(crate::columnar_row_group::ROW_GROUP_PAGE_ROWS)
                        .ok_or_else(|| replacement_payload_error("columnar page base overflows"))?,
                )
                .ok_or_else(|| replacement_payload_error("columnar page base overflows"))?;
            let synthetic_parts = crate::tracked_state::types::ColumnarMutationPartSet {
                owner_commit_id: source.owner_commit_id,
                row_group_set_id: source.source_id,
                manifest_digest: descriptor.content_digest,
                schema_key: manifest.namespace.clone(),
                row_count: manifest.groups.iter().map(|group| group.row_count).sum(),
                group_row_counts: manifest
                    .groups
                    .iter()
                    .map(|group| group.row_count)
                    .collect(),
                first_key: descriptor.first_key.clone(),
                last_key: descriptor.last_key.clone(),
                page_first_keys: vec![descriptor.first_key.clone()],
                page_last_keys: vec![descriptor.last_key.clone()],
                uniform_created_at: source.uniform_created_at,
                uniform_updated_at: source.uniform_updated_at,
                origin_key: None,
            };
            let owner = CommitId::new(uuid::Uuid::from_bytes(source.owner_commit_id));
            let start = usize::from(descriptor.source_row_offset);
            let end = start + usize::from(descriptor.row_count);
            if end > batch.num_rows() {
                return Err(replacement_payload_error(
                    "columnar current-state page slice is out of bounds",
                ));
            }
            (start..end)
                .map(|row_index| {
                    let ordinal = page_base.checked_add(row_index).ok_or_else(|| {
                        replacement_payload_error("columnar row ordinal overflows")
                    })?;
                    let packed = u32::try_from(ordinal)
                        .map_err(|_| replacement_payload_error("columnar row ordinal exceeds u32"))?
                        .checked_add(1)
                        .ok_or_else(|| {
                            replacement_payload_error("columnar change address overflows")
                        })?;
                    let change_id = change_id_from_packed_address(owner, packed);
                    let record = decode_columnar_change_record(
                        &manifest,
                        &batch,
                        row_index,
                        &synthetic_parts,
                        change_id,
                        "",
                    )?;
                    Ok(CurrentStateDataRow {
                        encoded_key: encode_key_ref(TrackedStateKeyRef {
                            schema_key: &record.schema_key,
                            file_id: record.file_id.as_deref(),
                            row_pk: &record.row_pk,
                        }),
                        value: TrackedStateIndexValue {
                            change_id,
                            commit_id: owner,
                            deleted: false,
                            created_at: source.uniform_created_at,
                            updated_at: source.uniform_updated_at,
                        },
                        snapshot: record.snapshot,
                        metadata: record.metadata,
                    })
                })
                .collect::<Result<Vec<_>, LixError>>()?
        }
    };
    if rows.first().map(|row| row.encoded_key.as_slice()) != Some(descriptor.first_key.as_slice())
        || rows.last().map(|row| row.encoded_key.as_slice()) != Some(descriptor.last_key.as_slice())
    {
        return Err(replacement_payload_error(
            "current-state source slice disagrees with descriptor bounds",
        ));
    }
    Ok(rows)
}

fn owned_scoped_json_slot(slot: crate::json_store::JsonSlotRef<'_>) -> crate::json_store::JsonSlot {
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

pub(crate) fn staged_commit_delta_segment_bytes(
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

pub(crate) async fn staged_commit_delta_members(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    account_id: &str,
    inventory: &CommitStateMutationInventory,
    staged_segments: Vec<Option<Bytes>>,
) -> Result<Vec<CommitDeltaMember>, LixError> {
    #[cfg(test)]
    {
        let counts =
            SPARSE_CURRENT_STATE_MATERIALIZATION_COUNTS.get_or_init(|| Mutex::new(BTreeMap::new()));
        *counts
            .lock()
            .expect("sparse materialization counter lock")
            .entry(commit_id)
            .or_default() += 1;
    }
    let manifest = commit_delta_manifest_from_inventory(inventory);
    let mut members = Vec::with_capacity(inventory.member_count as usize);
    if let Some(inline) = manifest.inline_segment() {
        collect_strict_commit_delta_members(inline, None, commit_id, 0, account_id, &mut members)?;
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
                account_id,
                &mut members,
            )?;
        }
    }
    validate_commit_delta_member_order_and_ids(commit_id, &members)?;
    Ok(members)
}

#[cfg(test)]
static SPARSE_CURRENT_STATE_MATERIALIZATION_COUNTS: OnceLock<Mutex<BTreeMap<CommitId, u64>>> =
    OnceLock::new();

#[cfg(test)]
pub(crate) fn sparse_current_state_materialization_count_for_test(commit_id: CommitId) -> u64 {
    SPARSE_CURRENT_STATE_MATERIALIZATION_COUNTS
        .get()
        .and_then(|counts| {
            counts
                .lock()
                .expect("sparse materialization counter lock")
                .get(&commit_id)
                .copied()
        })
        .unwrap_or(0)
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

fn commit_delta_segment_key_for_part(
    commit_id: CommitId,
    segment_index: usize,
    part: &CommitStateMutationPart,
) -> Result<Vec<u8>, LixError> {
    let mut encoded = commit_delta_segment_key(commit_id, segment_index)?;
    if let Some(part) = part.replacement_part.as_ref() {
        encoded.extend_from_slice(&part.content_digest);
    }
    Ok(encoded)
}

fn validate_bounded_direct_row_count(
    layout: u8,
    direct_row_count: u16,
    decoded_row_count: usize,
) -> Result<(), LixError> {
    if layout == super::mutation_directory::LAYOUT_BOUNDED_DIRECT
        && decoded_row_count != usize::from(direct_row_count)
    {
        return Err(replacement_payload_error(
            "bounded immutable part row count disagrees with directory authority",
        ));
    }
    Ok(())
}

/// Loads the immutable physical authority for one tracked commit.
pub(crate) async fn load_commit_state_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<CommitStateManifest>, LixError> {
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_crud_sealed_manifest_load();
    let header_keys = [StorageKey(Bytes::from(commit_state_manifest_key(
        commit_id,
    )))];
    let inventory_keys = [StorageKey(Bytes::from(commit_mutation_inventory_key(
        commit_id,
    )))];
    let requests = [
        StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            keys: &header_keys,
            opts: StorageGetOptions::default(),
        },
        StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
            keys: &inventory_keys,
            opts: StorageGetOptions::default(),
        },
    ];
    let mut values = exact_get_many(store, &requests).await?.values.into_iter();
    let header = values.next().flatten().and_then(full_value_bytes);
    let inventory = values.next().flatten().and_then(full_value_bytes);
    let (header, inventory) = match (header, inventory) {
        (None, None) => return Ok(None),
        (Some(header), Some(inventory)) => (header, inventory),
        _ => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit '{commit_id}' has incomplete split physical authority"
                ),
            ));
        }
    };
    let manifest = decode_commit_state_manifest(store, &header, &inventory).await?;
    if manifest.commit_id != commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_state_manifest key for commit '{commit_id}' contains manifest for commit '{}'",
                manifest.commit_id
            ),
        ));
    }
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

/// Loads only the authenticated immutable authority header needed by commit
/// topology, branch lifecycle, and scoped-root inheritance.
pub(crate) async fn load_published_commit_state_topology(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<PublishedCommitStateTopology>, LixError> {
    let keys = [StorageKey(Bytes::from(commit_state_manifest_key(
        commit_id,
    )))];
    let request = [StorageGetManyRequest {
        space: TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        keys: &keys,
        opts: StorageGetOptions::default(),
    }];
    let Some(bytes) = exact_get_many(store, &request)
        .await?
        .values
        .into_iter()
        .next()
        .flatten()
        .and_then(full_value_bytes)
    else {
        return Ok(None);
    };
    let header = decode_stored_commit_state_manifest(&bytes)?;
    if header.commit_id != commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state topology-header key for commit '{commit_id}' contains '{}'",
                header.commit_id
            ),
        ));
    }
    Ok(Some(PublishedCommitStateTopology { header }))
}

pub(crate) async fn load_point_replay_commit_state(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<Arc<AuthenticatedReplayCommitStateManifest>>, LixError> {
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_crud_replay_manifest_load();
    let header_keys = [StorageKey(Bytes::from(commit_state_manifest_key(
        commit_id,
    )))];
    let inventory_keys = [StorageKey(Bytes::from(commit_mutation_inventory_key(
        commit_id,
    )))];
    let requests = [
        StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            keys: &header_keys,
            opts: StorageGetOptions::default(),
        },
        StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
            keys: &inventory_keys,
            opts: StorageGetOptions::default(),
        },
    ];
    let mut values = exact_get_many(store, &requests).await?.values.into_iter();
    decode_point_replay_commit_state_values(
        commit_id,
        values.next().flatten(),
        values.next().flatten(),
    )
}

/// Co-loads the semantic commit record and its physical replay authority.
///
/// State reconstruction needs both independent authorities for every replayed
/// commit. Keeping them in one adapter batch preserves that separation while
/// avoiding a second backend round trip per first-parent step.
pub(crate) async fn load_commit_record_and_point_replay_state(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<
    (
        Option<CommitRecord>,
        Option<Arc<AuthenticatedReplayCommitStateManifest>>,
    ),
    LixError,
> {
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_crud_replay_manifest_load();
    let commit_keys = [StorageKey(Bytes::from(commit_key(commit_id)))];
    let header_keys = [StorageKey(Bytes::from(commit_state_manifest_key(
        commit_id,
    )))];
    let inventory_keys = [StorageKey(Bytes::from(commit_mutation_inventory_key(
        commit_id,
    )))];
    let requests = [
        StorageGetManyRequest {
            space: COMMIT_SPACE,
            keys: &commit_keys,
            opts: StorageGetOptions::default(),
        },
        StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            keys: &header_keys,
            opts: StorageGetOptions::default(),
        },
        StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
            keys: &inventory_keys,
            opts: StorageGetOptions::default(),
        },
    ];
    let mut values = exact_get_many(store, &requests).await?.values.into_iter();
    let record = values
        .next()
        .flatten()
        .and_then(full_value_bytes)
        .map(|bytes| replay_authority_cache::decode_commit_record(commit_id, &bytes))
        .transpose()?;
    if let Some(record) = record.as_ref()
        && record.commit_id != commit_id
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "changelog commit key for commit '{commit_id}' contains record for '{}'",
                record.commit_id
            ),
        ));
    }
    if record.is_none() {
        return Ok((None, None));
    }
    let state = decode_point_replay_commit_state_values(
        commit_id,
        values.next().flatten(),
        values.next().flatten(),
    )?;
    debug_assert!(values.next().is_none());
    Ok((record, state))
}

fn decode_point_replay_commit_state_values(
    commit_id: CommitId,
    header: Option<StorageProjectedValue>,
    inventory: Option<StorageProjectedValue>,
) -> Result<Option<Arc<AuthenticatedReplayCommitStateManifest>>, LixError> {
    let header = header.and_then(full_value_bytes);
    let inventory = inventory.and_then(full_value_bytes);
    let (header, inventory) = match (header, inventory) {
        (None, None) => return Ok(None),
        (Some(header), Some(inventory)) => (header, inventory),
        _ => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit '{commit_id}' has incomplete split physical authority"
                ),
            ));
        }
    };
    if let Some(hit) = replay_authority_cache::get(commit_id, &header, &inventory) {
        return Ok(Some(hit));
    }
    let authenticated = Arc::new(authenticate_point_replay_commit_state_values(
        commit_id, &header, &inventory,
    )?);
    replay_authority_cache::insert(commit_id, &header, &inventory, &authenticated);
    Ok(Some(authenticated))
}

/// The uncached decode-and-authenticate path for one commit's split physical
/// authority.
///
/// Split out of [`decode_point_replay_commit_state_values`] so that
/// `replay_authority_cache` sits in front of a named function rather than
/// inside one, which is what lets
/// `point_replay_authority_cache_cannot_launder_a_rejected_manifest` compare
/// the cached and uncached paths directly instead of toggling a global.
fn authenticate_point_replay_commit_state_values(
    commit_id: CommitId,
    header: &Bytes,
    inventory: &Bytes,
) -> Result<AuthenticatedReplayCommitStateManifest, LixError> {
    let (stored, stored_inventory) = decode_stored_commit_state_authority(header, inventory)?;
    let mutation_directory_root = stored_inventory.directory_root.clone();
    let state = assemble_shallow_commit_state_manifest(stored, stored_inventory)?;
    if state.commit_id != commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state manifest key for commit '{commit_id}' contains authority for commit '{}'",
                state.commit_id
            ),
        ));
    }
    Ok(AuthenticatedReplayCommitStateManifest {
        manifest: state,
        mutation_directory_root,
    })
}

/// Bulk-loads commit authorities in request order.
/// Every commit that still owns physical tracked state.
///
/// The manifest header is the physical state's own authority: it exists exactly
/// while the commit owns a delta segment, and [`stage_retire_commit_physical_state`]
/// deletes it. Enumerating it is therefore an inventory of the thing being
/// collected — it costs Theta(unretired commits) and shrinks as collection
/// succeeds — and never a rediscovery of liveness, which stays with the refs.
///
/// GC needs this because a commit can stop being reachable from any ref (a
/// deleted branch's commits, for one) while still owning physical state. A
/// walk that starts at refs cannot name those commits at all.
pub(crate) async fn scan_commit_state_manifest_commit_ids(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<Vec<CommitId>, LixError> {
    // Key-only: the caller wants the commit ids, and a manifest body is the one
    // thing this plane stores that is expensive to read.
    let mut cursor = store
        .begin_scan(
            TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            StorageKeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    let mut commit_ids = Vec::new();
    loop {
        let (page, page_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in &page {
            let bytes: [u8; 16] = entry.key.0.as_ref().try_into().map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "commit state manifest key is not a commit id",
                )
            })?;
            commit_ids.push(CommitId::new(uuid::Uuid::from_bytes(bytes)));
        }
        if !page_has_more {
            break;
        }
    }
    Ok(commit_ids)
}

pub(crate) async fn load_commit_state_manifests(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_ids: &[CommitId],
) -> Result<Vec<Option<CommitStateManifest>>, LixError> {
    let header_keys = commit_ids
        .iter()
        .map(|commit_id| StorageKey(Bytes::from(commit_state_manifest_key(*commit_id))))
        .collect::<Vec<_>>();
    let inventory_keys = commit_ids
        .iter()
        .map(|commit_id| StorageKey(Bytes::from(commit_mutation_inventory_key(*commit_id))))
        .collect::<Vec<_>>();
    let requests = [
        StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            keys: &header_keys,
            opts: StorageGetOptions::default(),
        },
        StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
            keys: &inventory_keys,
            opts: StorageGetOptions::default(),
        },
    ];
    let mut values = exact_get_many(store, &requests).await?.values;
    let inventories = values.split_off(commit_ids.len());
    let headers = values;
    let mut authorities = Vec::with_capacity(commit_ids.len());
    for (commit_id, (header_value, inventory_value)) in commit_ids
        .iter()
        .copied()
        .zip(headers.into_iter().zip(inventories))
    {
        let header = header_value.and_then(full_value_bytes);
        let inventory = inventory_value.and_then(full_value_bytes);
        let (header, inventory) = match (header, inventory) {
            (None, None) => {
                authorities.push(None);
                continue;
            }
            (Some(header), Some(inventory)) => (header, inventory),
            _ => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state commit '{commit_id}' has incomplete split physical authority"
                    ),
                ));
            }
        };
        let (stored, stored_inventory) = decode_stored_commit_state_authority(&header, &inventory)?;
        if stored.commit_id != commit_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit-state batch key for commit '{commit_id}' contains manifest for commit '{}'",
                    stored.commit_id
                ),
            ));
        }
        authorities.push(Some((stored, stored_inventory)));
    }
    let roots = authorities
        .iter()
        .filter_map(|authority| {
            authority
                .as_ref()
                .and_then(|(_, inventory)| inventory.directory_root.clone())
        })
        .collect::<Vec<_>>();
    let mut directories = super::mutation_directory::load_all_mutation_part_read_plans(
        store,
        &roots,
        super::mutation_directory::MutationDirectoryFullTraversalContext::BulkCommitStateManifests,
    )
    .await?
    .into_iter();
    let mut output = Vec::with_capacity(authorities.len());
    for authority in authorities {
        let Some((stored, inventory)) = authority else {
            output.push(None);
            continue;
        };
        let entries = if inventory.directory_root.is_some() {
            directories
                .next()
                .expect("each authenticated root returns one directory")
                .into_runs()
                .into_iter()
                .map(|run| run.entry)
                .collect()
        } else {
            Vec::new()
        };
        output.push(Some(assemble_commit_state_manifest(
            stored, inventory, entries, true,
        )?));
    }
    debug_assert!(directories.next().is_none());
    Ok(output)
}

/// Loads only the small immutable authority headers in request order.
///
/// Topology membership and commit identity do not require mutation-directory
/// bytes. Each returned header has already passed format, replay, root, scoped
/// metadata, and directory-digest-shape validation; replay/history consumers
/// must use [`load_commit_state_manifests`] to authenticate and decode the
/// separately keyed directory itself.
pub(crate) async fn load_commit_state_authority_ids(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_ids: &[CommitId],
) -> Result<Vec<Option<CommitId>>, LixError> {
    let keys = commit_ids
        .iter()
        .map(|commit_id| commit_state_authority_key(*commit_id))
        .collect::<Vec<_>>();
    let request = [StorageGetManyRequest {
        space: TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        keys: &keys,
        opts: StorageGetOptions::default(),
    }];
    commit_ids
        .iter()
        .copied()
        .zip(exact_get_many(store, &request).await?.values)
        .map(|(commit_id, value)| decode_commit_state_authority_id(commit_id, value))
        .collect()
}

pub(crate) fn commit_state_authority_key(commit_id: CommitId) -> StorageKey {
    StorageKey(Bytes::from(commit_state_manifest_key(commit_id)))
}

pub(crate) fn decode_commit_state_authority_id(
    commit_id: CommitId,
    value: Option<StorageProjectedValue>,
) -> Result<Option<CommitId>, LixError> {
    let Some(bytes) = value.and_then(full_value_bytes) else {
        return Ok(None);
    };
    let stored = decode_stored_commit_state_manifest(&bytes)?;
    if stored.commit_id != commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state authority-header key for commit '{commit_id}' contains '{}'",
                stored.commit_id
            ),
        ));
    }
    Ok(Some(stored.commit_id))
}

/// Loads authenticated mutation-directory roots without reading catalogs or
/// directory nodes. GC uses this projection to mark shared content-addressed
/// nodes from retained commit authorities.
pub(crate) async fn load_commit_mutation_directory_roots(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_ids: &[CommitId],
) -> Result<Vec<Option<super::mutation_directory::MutationDirectoryRoot>>, LixError> {
    let keys = commit_ids
        .iter()
        .map(|commit_id| StorageKey(Bytes::from(commit_state_manifest_key(*commit_id))))
        .collect::<Vec<_>>();
    let request = [StorageGetManyRequest {
        space: TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        keys: &keys,
        opts: StorageGetOptions::default(),
    }];
    commit_ids
        .iter()
        .copied()
        .zip(exact_get_many(store, &request).await?.values)
        .map(|(commit_id, value)| {
            let Some(bytes) = value.and_then(full_value_bytes) else {
                return Ok(None);
            };
            let stored = decode_stored_commit_state_manifest(&bytes)?;
            if stored.commit_id != commit_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state mutation-root key for commit '{commit_id}' contains '{}'",
                        stored.commit_id
                    ),
                ));
            }
            Ok(stored.mutation_directory_root)
        })
        .collect()
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

/// Stages one complete immutable physical commit authority record.
fn stage_commit_state_manifest_bytes(
    writes: &mut StorageWriteSet,
    manifest: &CommitStateManifest,
) -> Result<(), LixError> {
    let encoded = encode_commit_state_manifest(manifest)?;
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_crud_commit_state_manifest_bytes(
        encoded.header.len()
            + encoded.mutation_inventory.len()
            + encoded.mutation_directory.as_ref().map_or(0, |directory| {
                directory
                    .node_bytes()
                    .values()
                    .map(Bytes::len)
                    .sum::<usize>()
            }),
    );
    if let Some(directory) = encoded.mutation_directory.as_ref() {
        directory.stage(writes)?;
    }
    writes.put(
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        key(commit_state_manifest_key(manifest.commit_id)),
        value(encoded.header),
    );
    writes.put(
        TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
        key(commit_mutation_inventory_key(manifest.commit_id)),
        value(encoded.mutation_inventory),
    );
    Ok(())
}

/// Stages authority that carries no serving scoped-range root. Root-bearing
/// authorities require the opaque canonical transition proof returned by
/// `stage_current_state_scoped_ranges`.
pub(crate) fn stage_commit_state_manifest(
    writes: &mut StorageWriteSet,
    manifest: &CommitStateManifest,
) -> Result<(), LixError> {
    if manifest.current_state_scoped_ranges.is_some() {
        return Err(replacement_payload_error(
            "current-state scoped ranges require canonical publication certification",
        ));
    }
    if manifest.touched_scope_filter.complete {
        return Err(replacement_payload_error(
            "complete touched-scope filters require canonical publication certification",
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

/// Publishes physical serving authority only when the exact opaque proof from
/// its canonical topology and range transition accompanies the manifest.
pub(crate) fn stage_certified_commit_state_manifest(
    writes: &mut StorageWriteSet,
    manifest: &CommitStateManifest,
    publication: &super::scoped_current_state::CertifiedCommitStatePhysicalPublication,
) -> Result<(), LixError> {
    if writes.identity() != publication.write_set_id() {
        return Err(replacement_payload_error(
            "physical publication proof belongs to a different storage write set",
        ));
    }
    if manifest.commit_id != publication.commit_id() {
        return Err(replacement_payload_error(
            "commit manifest identity disagrees with its physical publication proof",
        ));
    }
    if manifest.mutations.selected_source_commit_id() != publication.selected_source_commit_id() {
        return Err(replacement_payload_error(
            "commit manifest selected source disagrees with its physical publication proof",
        ));
    }
    if manifest.current_state_scoped_ranges != publication.root() {
        return Err(replacement_payload_error(
            "commit manifest disagrees with its canonical scoped-range publication proof",
        ));
    }
    if &manifest.touched_scope_filter != publication.touched_scope_filter() {
        return Err(replacement_payload_error(
            "commit manifest disagrees with its certified touched-scope filter",
        ));
    }
    stage_commit_state_manifest_bytes(writes, manifest)
}

pub(crate) fn stage_certified_commit_state_manifest_with_handle(
    writes: &mut StorageWriteSet,
    manifest: &CommitStateManifest,
    publication: &super::scoped_current_state::CertifiedCommitStatePhysicalPublication,
) -> Result<StagedCommitStateManifest, LixError> {
    stage_certified_commit_state_manifest(writes, manifest, publication)?;
    Ok(StagedCommitStateManifest {
        manifest: manifest.clone(),
        write_set_id: writes.identity(),
    })
}

/// Replaces immutable manifest bytes through the `cfg(test)` mutable view.
///
/// See `StorageSpace::mutable_view_for_corruption_test`: the resulting
/// physical state is faithful on the in-memory backend these tests run on,
/// and not on RocksDB or SlateDB.
#[cfg(test)]
pub(crate) fn stage_resealed_commit_state_manifest_for_test(
    writes: &mut StorageWriteSet,
    manifest: &CommitStateManifest,
) -> Result<(), LixError> {
    let encoded = encode_commit_state_manifest(manifest)?;
    if let Some(directory) = encoded.mutation_directory.as_ref() {
        directory.stage(writes)?;
    }
    writes.put(
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE.mutable_view_for_corruption_test(),
        key(commit_state_manifest_key(manifest.commit_id)),
        value(encoded.header),
    );
    writes.put(
        TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE.mutable_view_for_corruption_test(),
        key(commit_mutation_inventory_key(manifest.commit_id)),
        value(encoded.mutation_inventory),
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
    stage_commit_deltas_inner(writes, deltas, None, None, false)
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
    stage_commit_deltas_inner(writes, deltas, Some(addressable), None, false)
}

/// Stages commit deltas received from a sync authority.
///
/// Imported commits retain their wire change ids. A UUID can look directly
/// addressable while having been assigned under a different physical packing
/// order, so a mismatch falls back to an explicit locator instead of rewriting
/// the authoritative identity.
pub(crate) fn stage_imported_addressable_commit_deltas(
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
    stage_commit_deltas_inner(writes, deltas, Some(addressable), None, true)
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
        false,
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
        row_pk: first.delta.row_pk,
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
                row_pk: delta.delta.row_pk,
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
        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
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
        columnar_parts: None,
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
        let (bounds, encoded) = first_segment
            .take()
            .expect("one ordered segment remains available for its manifest");
        // Inlining a lone segment into the manifest saves a small commit one
        // storage row and one point read, and costs nothing while no reader
        // touches the payload — a small commit leaves its rows on the hot
        // plane, which is where current-value reads find them.
        //
        // A segment that fills to `COMMIT_DELTA_SEGMENT_MAX_ROWS` is the one
        // case where that stops being true. Filling a whole segment is also
        // what takes the commit over the packing threshold, so its rows leave
        // the hot plane and every current-value read has to fetch the manifest
        // and decode the entire inlined segment to reach one row.
        //
        // Measured (rocksdb, 10 240 rows, 2 000 point reads, ryzen-9950x-II):
        // a 512-row commit read at 887 us against 140 us at 511 rows and
        // 314 us at 513 — 6.3x — because 512 is the only width that both packs
        // and produces exactly one segment. Decode dominates that cost
        // (`decode_commit_delta_with_payloads` 41%, zstd 23%, manifest fetch
        // under 2.5%), so the fix is to stop inlining a full segment rather
        // than to make the fetch cheaper.
        if usize::from(segment_row_counts[0]) < COMMIT_DELTA_SEGMENT_MAX_ROWS {
            manifest.inline_segment = encoded;
        } else {
            manifest.segments.push(bounds);
            writes.reserve_space(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, 1, 0);
            writes.put(
                TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
                key(commit_delta_segment_key(commit_id, 0)?),
                value(encoded),
            );
        }
    }
    let dense_addresses = segment_row_counts
        .iter()
        .take(segment_row_counts.len().saturating_sub(1))
        .all(|&count| usize::from(count) == COMMIT_DELTA_SEGMENT_MAX_ROWS);
    let change_addresses = if dense_addresses {
        OrderedChangeAddresses::Dense
    } else {
        let mut cumulative_rows = 0u32;
        let row_ends = segment_row_counts
            .iter()
            .map(|&segment_rows| {
                cumulative_rows = cumulative_rows
                    .checked_add(u32::from(segment_rows))
                    .expect("ordered commit-delta row count fits u32");
                cumulative_rows
            })
            .collect::<Vec<_>>();
        debug_assert_eq!(usize::try_from(cumulative_rows).ok(), Some(row_count));
        OrderedChangeAddresses::Segmented(row_ends)
    };
    Ok(Some(OrderedAddressableCommitDeltaStage {
        commit_id,
        change_addresses,
        row_count,
        mutation_inventory: commit_state_inventory_from_delta_manifest(&manifest),
    }))
}

/// Publishes a lossless identity-ordered row generation as the commit's
/// authored mutation payload without encoding a second LXCD JSON sidecar.
/// The row-group set is staged by the current-state publisher in the same
/// atomic write set; this function seals only its historical authority.
pub(crate) fn stage_ordered_columnar_mutations(
    commit_id: CommitId,
    parts: crate::tracked_state::types::ColumnarMutationPartSet,
    ordered_identity_digest: [u8; 32],
) -> Result<OrderedAddressableCommitDeltaStage, LixError> {
    let row_count = usize::try_from(parts.row_count).expect("u32 row count fits usize");
    if row_count == 0
        || parts.group_row_counts.is_empty()
        || parts
            .group_row_counts
            .iter()
            .map(|&count| u64::from(count))
            .sum::<u64>()
            != u64::from(parts.row_count)
        || (row_count == 1 && parts.first_key != parts.last_key)
        || (row_count > 1 && parts.first_key >= parts.last_key)
        || parts.page_first_keys.len()
            != row_count.div_ceil(crate::columnar_row_group::ROW_GROUP_PAGE_ROWS)
        || parts.page_first_keys.len() != parts.page_last_keys.len()
    {
        return Err(replacement_payload_error(
            "columnar mutation inventory has invalid row topology",
        ));
    }
    let direct_part_row_counts = (0..row_count)
        .step_by(COMMIT_DELTA_SEGMENT_MAX_ROWS)
        .map(|offset| {
            u16::try_from((row_count - offset).min(COMMIT_DELTA_SEGMENT_MAX_ROWS))
                .expect("direct mutation segment row count fits u16")
        })
        .collect::<Vec<_>>();
    let lifecycle_summary = CommitDeltaLifecycleSummary {
        scope: CommitDeltaReplacementScope {
            schema_key: parts.schema_key.clone(),
            file_id: None,
        },
        ordered_identity_digest,
        uniform_created_at: parts.uniform_created_at,
    };
    Ok(OrderedAddressableCommitDeltaStage {
        commit_id,
        change_addresses: OrderedChangeAddresses::Dense,
        row_count,
        mutation_inventory: CommitStateMutationInventory {
            selected_source_commit_id: None,
            member_count: parts.row_count,
            selection_fingerprint: [0; 32],
            direct_part_row_counts,
            replacement_part_digests: Vec::new(),
            single_partition: Some(lifecycle_summary.scope.clone()),
            lifecycle_summary: Some(lifecycle_summary),
            replacement_generation: None,
            replacement_parts: None,
            columnar_parts: Some(parts),
            inline_part: Vec::new(),
            parts: Vec::new(),
        },
    })
}

/// Publishes a complete replacement as compact immutable identity parts.
/// Parts own identity routing and JSON authority: small values are inline and
/// large values remain content-addressed references into the JSON store.
pub(crate) struct ReplacementPartInput<'a> {
    key_start: usize,
    key_end: usize,
    commit_id: CommitId,
    created_at: crate::common::LixTimestamp,
    updated_at: crate::common::LixTimestamp,
    snapshot: crate::json_store::JsonSlotRef<'a>,
    metadata: crate::json_store::JsonSlotRef<'a>,
}

pub(crate) trait ReplacementPartInputRef<'a>: Copy {
    fn into_replacement_part_input(
        self,
        key_arena: &mut Vec<u8>,
    ) -> Result<ReplacementPartInput<'a>, LixError>;
}

impl<'a> ReplacementPartInputRef<'a> for TrackedStateCommitDeltaRef<'a> {
    fn into_replacement_part_input(
        self,
        key_arena: &mut Vec<u8>,
    ) -> Result<ReplacementPartInput<'a>, LixError> {
        if self.delta.deleted || !self.authored || self.origin_key.is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state replacement member violates immutable replacement invariants",
            ));
        }
        let key_range = encode_key_ref_into(
            key_arena,
            TrackedStateKeyRef {
                schema_key: self.delta.schema_key,
                file_id: self.delta.file_id,
                row_pk: self.delta.row_pk,
            },
        );
        Ok(ReplacementPartInput {
            key_start: key_range.start,
            key_end: key_range.end,
            commit_id: self.delta.commit_id,
            created_at: self.delta.created_at,
            updated_at: self.delta.updated_at,
            snapshot: self.snapshot,
            metadata: self.metadata,
        })
    }
}

impl<'a> ReplacementPartInputRef<'a> for TrackedStateSingleStringReplacementRef<'a> {
    fn into_replacement_part_input(
        self,
        key_arena: &mut Vec<u8>,
    ) -> Result<ReplacementPartInput<'a>, LixError> {
        let key_range = encode_single_string_key_ref_into(
            key_arena,
            self.schema_key,
            self.file_id,
            self.row_pk,
        );
        Ok(ReplacementPartInput {
            key_start: key_range.start,
            key_end: key_range.end,
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
    stage_ordered_addressable_replacement_parts_inner(writes, deltas, generation, None)
}

pub(crate) fn stage_prefixed_ordered_addressable_replacement_parts<'a, I, R>(
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
    uniform_updated_at: crate::common::LixTimestamp,
    prefix_row_count: usize,
    prefix_parts: Vec<crate::tracked_state::replacement_part::EncodedReplacementPart>,
    deltas: I,
    generation: &CommitDeltaReplacementGeneration,
) -> Result<OrderedAddressableCommitDeltaStage, LixError>
where
    I: ExactSizeIterator<Item = Result<R, LixError>>,
    R: ReplacementPartInputRef<'a>,
{
    stage_ordered_addressable_replacement_parts_inner(
        writes,
        deltas,
        generation,
        Some((
            commit_id,
            uniform_updated_at,
            prefix_row_count,
            prefix_parts,
        )),
    )
}

fn stage_ordered_addressable_replacement_parts_inner<'a, I, R>(
    writes: &mut StorageWriteSet,
    deltas: I,
    generation: &CommitDeltaReplacementGeneration,
    prefix: Option<(
        CommitId,
        crate::common::LixTimestamp,
        usize,
        Vec<crate::tracked_state::replacement_part::EncodedReplacementPart>,
    )>,
) -> Result<OrderedAddressableCommitDeltaStage, LixError>
where
    I: ExactSizeIterator<Item = Result<R, LixError>>,
    R: ReplacementPartInputRef<'a>,
{
    struct BorrowedRow<'a> {
        key_start: usize,
        key_end: usize,
        snapshot: crate::json_store::JsonSlotRef<'a>,
        metadata: crate::json_store::JsonSlotRef<'a>,
    }

    let suffix_row_count = deltas.len();
    let prefix_row_count = prefix.as_ref().map_or(0, |prefix| prefix.2);
    let row_count = prefix_row_count
        .checked_add(suffix_row_count)
        .ok_or_else(|| LixError::unknown("tracked_state replacement row count overflowed"))?;
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
    let mut commit_id = prefix.as_ref().map(|prefix| prefix.0);
    let mut uniform_updated_at = prefix.as_ref().map(|prefix| prefix.1);
    let mut previous_key = prefix
        .as_ref()
        .and_then(|prefix| prefix.3.last())
        .map_or_else(Vec::new, |part| part.last_key().to_vec());
    let mut pending = Vec::with_capacity(COMMIT_DELTA_SEGMENT_MAX_ROWS);
    let mut key_arena = Vec::new();
    let mut parts = prefix.map_or_else(Vec::new, |prefix| prefix.3);
    parts.reserve(row_count.div_ceil(COMMIT_DELTA_SEGMENT_MAX_ROWS));
    let mut compressor = None;
    for delta in deltas {
        let delta = delta?.into_replacement_part_input(&mut key_arena)?;
        #[cfg(feature = "storage-benches")]
        {
            let json_bytes = |slot: crate::json_store::JsonSlotRef<'_>| match slot {
                crate::json_store::JsonSlotRef::None => 0,
                crate::json_store::JsonSlotRef::Ref(_) => 32,
                crate::json_store::JsonSlotRef::Inline(value) => value.len(),
            };
            crate::storage_bench::record_crud_ownership(
                crate::storage_bench::CRUD_OWNERSHIP_REPLACEMENT_INPUT,
                1,
                delta.key_end.saturating_sub(delta.key_start),
                json_bytes(delta.snapshot) + json_bytes(delta.metadata),
                1,
                0,
                0,
            );
        }
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
        let key = &key_arena[delta.key_start..delta.key_end];
        if !previous_key.is_empty() && previous_key.as_slice() >= key {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state replacement members are not in canonical identity order",
            ));
        }
        previous_key.clear();
        previous_key.extend_from_slice(key);
        pending.push(BorrowedRow {
            key_start: delta.key_start,
            key_end: delta.key_end,
            snapshot: delta.snapshot,
            metadata: delta.metadata,
        });
        if pending.len() == COMMIT_DELTA_SEGMENT_MAX_ROWS {
            encode_replacement_part_prefix(
                &mut pending,
                &mut key_arena,
                &mut parts,
                &mut compressor,
            )?;
        }
    }
    let commit_id = commit_id.expect("non-empty replacement has an owner");
    addressable_change_id(commit_id, 0, 0)?;
    let uniform_updated_at = uniform_updated_at.expect("non-empty replacement has a timestamp");

    while !pending.is_empty() {
        encode_replacement_part_prefix(&mut pending, &mut key_arena, &mut parts, &mut compressor)?;
    }

    fn encode_replacement_part_prefix(
        pending: &mut Vec<BorrowedRow<'_>>,
        key_arena: &mut Vec<u8>,
        parts: &mut Vec<crate::tracked_state::replacement_part::EncodedReplacementPart>,
        compressor: &mut Option<crate::compression::ZstdLevel1Compressor>,
    ) -> Result<(), LixError> {
        let mut candidate_len = pending.len().min(COMMIT_DELTA_SEGMENT_MAX_ROWS);
        let encoded = loop {
            let refs = pending[..candidate_len]
                .iter()
                .map(
                    |row| crate::tracked_state::replacement_part::ReplacementPartRowRef {
                        encoded_key: &key_arena[row.key_start..row.key_end],
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
        #[cfg(feature = "storage-benches")]
        if let Some(part) = parts.last() {
            crate::storage_bench::record_crud_ownership(
                crate::storage_bench::CRUD_OWNERSHIP_REPLACEMENT_PART,
                usize::from(part.row_count()),
                part.first_key().len() + part.last_key().len(),
                part.bytes().len(),
                1,
                0,
                0,
            );
        }
        let removed_key_end = pending[candidate_len - 1].key_end;
        pending.drain(..candidate_len);
        if pending.is_empty() {
            key_arena.clear();
        } else {
            key_arena.drain(..removed_key_end);
            for row in pending {
                row.key_start -= removed_key_end;
                row.key_end -= removed_key_end;
            }
        }
        Ok(())
    }

    stage_preencoded_ordered_addressable_replacement_parts(
        writes,
        commit_id,
        uniform_updated_at,
        row_count,
        parts,
        generation,
    )
}

pub(crate) fn stage_preencoded_ordered_addressable_replacement_parts(
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
    uniform_updated_at: crate::common::LixTimestamp,
    row_count: usize,
    parts: Vec<crate::tracked_state::replacement_part::EncodedReplacementPart>,
    generation: &CommitDeltaReplacementGeneration,
) -> Result<OrderedAddressableCommitDeltaStage, LixError> {
    if row_count == 0 || parts.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state preencoded replacement generation cannot be empty",
        ));
    }
    if parts
        .iter()
        .map(|part| usize::from(part.row_count()))
        .sum::<usize>()
        != row_count
        || parts
            .windows(2)
            .any(|pair| pair[0].last_key() >= pair[1].first_key())
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state preencoded replacement parts are not a complete ordered row set",
        ));
    }
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_crud_ownership(
        crate::storage_bench::CRUD_OWNERSHIP_AUTHORITY,
        row_count,
        parts
            .iter()
            .map(|part| part.first_key().len() + part.last_key().len())
            .sum(),
        parts.iter().map(|part| part.bytes().len()).sum(),
        parts.len(),
        0,
        0,
    );
    addressable_change_id(commit_id, 0, 0)?;
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
        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
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
        columnar_parts: None,
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
            StorageValue {
                bytes: part.bytes().clone(),
            },
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
        let mut cumulative_rows = 0u32;
        let row_ends = manifest
            .direct_segment_row_counts
            .iter()
            .map(|&rows| {
                cumulative_rows = cumulative_rows
                    .checked_add(u32::from(rows))
                    .expect("replacement row count fits u32");
                cumulative_rows
            })
            .collect::<Vec<_>>();
        debug_assert_eq!(usize::try_from(cumulative_rows).ok(), Some(row_count));
        OrderedChangeAddresses::Segmented(row_ends)
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
        let Ok(identity) = delta.delta.row_pk.as_single_string() else {
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
        .then_with(|| left.row_pk.cmp(right.row_pk))
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
                row_pk: delta.delta.row_pk,
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
    preserve_mismatched_change_ids: bool,
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
                row_pk: delta.delta.row_pk,
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
                if preserve_mismatched_change_ids && value.change_id != change_id {
                    // Imported complete commits retain their authoritative
                    // change ids. A UUID can look directly addressable while
                    // having been assigned under a different physical packing
                    // order; keep it verbatim and publish an explicit locator.
                    addressable[source_index] = false;
                    assigned_change_ids[source_indices[source_index]] = value.change_id;
                    continue;
                }
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
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            selected_source_commit_id: selected_source_commit_id
                .map(|commit_id| *commit_id.as_uuid().as_bytes()),
            member_count,
            selection_fingerprint,
            direct_segment_row_counts,
            single_partition: single_partition_for_entries(&entries)?,
            lifecycle_summary: None,
            replacement_generation: None,
            replacement_parts: None,
            columnar_parts: None,
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
        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
        selected_source_commit_id: selected_source_commit_id
            .map(|commit_id| *commit_id.as_uuid().as_bytes()),
        member_count,
        selection_fingerprint,
        direct_segment_row_counts,
        single_partition: single_partition_for_entries(&entries)?,
        lifecycle_summary: None,
        replacement_generation: None,
        replacement_parts: None,
        columnar_parts: None,
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

pub(crate) fn change_id_from_packed_address(
    commit_id: CommitId,
    packed: u32,
) -> crate::changelog::ChangeId {
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

#[cfg(test)]
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
    if let Some(locator) = direct_change_locator(change_id) {
        match load_direct_change_authority(store, locator.commit_id).await? {
            DirectChangeAuthority::Candidate(authority) => {
                let route = route_direct_change_records_for_state(store, &authority, &[locator])
                    .await?
                    .pop()
                    .expect("one direct locator returns one route");
                if let DirectChangeRecordRoute::Owned(record) = route {
                    return Ok(Some(record));
                }
            }
            DirectChangeAuthority::NotOwned(reason) => {
                let _ = reason;
            }
        }
        #[cfg(any(test, feature = "storage-benches"))]
        super::mutation_directory::record_direct_route_explicit_fallback(1);
    }
    if let Some(locator) = load_change_locator_by_id(store, change_id).await? {
        return Ok(load_explicit_change_records_at_locators(store, &[locator])
            .await?
            .pop());
    }
    Ok(None)
}

pub(crate) async fn load_canonical_change_locator(
    store: &(impl StorageAdapterRead + ?Sized),
    change_id: crate::changelog::ChangeId,
) -> Result<Option<CommitDeltaChangeLocator>, LixError> {
    if let Some(locator) = direct_change_locator(change_id) {
        match load_direct_change_authority(store, locator.commit_id).await? {
            DirectChangeAuthority::Candidate(authority) => {
                let route = route_direct_change_records_for_state(store, &authority, &[locator])
                    .await?
                    .pop()
                    .expect("one direct locator returns one route");
                if matches!(route, DirectChangeRecordRoute::Owned(_)) {
                    return Ok(Some(locator));
                }
            }
            DirectChangeAuthority::NotOwned(reason) => {
                let _ = reason;
            }
        }
        #[cfg(any(test, feature = "storage-benches"))]
        super::mutation_directory::record_direct_route_explicit_fallback(1);
    }
    if let Some(locator) = load_change_locator_by_id(store, change_id).await? {
        return Ok(Some(locator));
    }
    Ok(None)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectNotOwnedReason {
    MissingCommitAuthority,
    UnsupportedLayout,
    AbsentInlineAuthority,
    PartIndexOutOfRange,
    LocalRowOutOfRange,
}

#[derive(Clone)]
enum DirectChangeAuthority {
    Candidate(Arc<AuthenticatedReplayCommitStateManifest>),
    NotOwned(DirectNotOwnedReason),
}

#[derive(Clone)]
enum DirectChangeRecordRoute {
    Owned(crate::changelog::ChangeRecord),
    NotOwned(DirectNotOwnedReason),
}

impl From<super::mutation_directory::MutationDirectoryNotOwnedReason> for DirectNotOwnedReason {
    fn from(reason: super::mutation_directory::MutationDirectoryNotOwnedReason) -> Self {
        match reason {
            super::mutation_directory::MutationDirectoryNotOwnedReason::UnsupportedLayout => {
                Self::UnsupportedLayout
            }
            super::mutation_directory::MutationDirectoryNotOwnedReason::PartIndexOutOfRange => {
                Self::PartIndexOutOfRange
            }
            super::mutation_directory::MutationDirectoryNotOwnedReason::LocalRowOutOfRange => {
                Self::LocalRowOutOfRange
            }
        }
    }
}

async fn load_direct_change_authority(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<DirectChangeAuthority, LixError> {
    let Some(state) = load_point_replay_commit_state(store, commit_id).await? else {
        #[cfg(any(test, feature = "storage-benches"))]
        super::mutation_directory::record_direct_route_missing_commit(1);
        return Ok(DirectChangeAuthority::NotOwned(
            DirectNotOwnedReason::MissingCommitAuthority,
        ));
    };
    let candidate = match state
        .mutation_directory_root
        .as_ref()
        .map(|root| root.layout)
    {
        Some(
            super::mutation_directory::LAYOUT_BOUNDED_DIRECT
            | super::mutation_directory::LAYOUT_COMPACT_REPLACEMENT
            | super::mutation_directory::LAYOUT_DIRECT_ROWS_ONLY,
        ) => true,
        Some(super::mutation_directory::LAYOUT_BOUNDED_INDIRECT) => false,
        Some(_) => {
            return Err(replacement_payload_error(
                "direct change authority has an unsupported directory layout",
            ));
        }
        None => !state.mutations.direct_part_row_counts.is_empty(),
    };
    if candidate && state.mutations.selected_source_commit_id().is_some() {
        return Err(replacement_payload_error(
            "selected-source alias cannot claim direct change coordinates",
        ));
    }
    if candidate {
        Ok(DirectChangeAuthority::Candidate(state))
    } else if state.mutation_directory_root.is_some() {
        #[cfg(any(test, feature = "storage-benches"))]
        super::mutation_directory::record_direct_route_not_owned(
            super::mutation_directory::MutationDirectoryNotOwnedReason::UnsupportedLayout,
            1,
        );
        Ok(DirectChangeAuthority::NotOwned(
            DirectNotOwnedReason::UnsupportedLayout,
        ))
    } else {
        #[cfg(any(test, feature = "storage-benches"))]
        super::mutation_directory::record_direct_route_absent_inline(1);
        Ok(DirectChangeAuthority::NotOwned(
            DirectNotOwnedReason::AbsentInlineAuthority,
        ))
    }
}

async fn route_direct_change_records_for_state(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    locators: &[CommitDeltaChangeLocator],
) -> Result<Vec<DirectChangeRecordRoute>, LixError> {
    if locators.is_empty() {
        return Ok(Vec::new());
    }
    #[cfg(any(test, feature = "storage-benches"))]
    let mut accounting_guard = super::mutation_directory::DirectRouteAccountingGuard::new();
    #[cfg(any(test, feature = "storage-benches"))]
    {
        super::mutation_directory::record_direct_route_start(locators.len());
    }
    if state.mutations.selected_source_commit_id().is_some() {
        return Err(replacement_payload_error(
            "selected-source alias cannot own direct change coordinates",
        ));
    }
    let mut request_indices = (0..locators.len()).collect::<Vec<_>>();
    request_indices.sort_by_key(|&index| {
        let locator = locators[index];
        (locator.segment_index, locator.ordinal)
    });
    let mut unique_locator_indices = Vec::<usize>::with_capacity(request_indices.len());
    let mut output_routes = Vec::with_capacity(request_indices.len());
    for request_index in request_indices {
        let locator = locators[request_index];
        if locator.commit_id != state.commit_id
            || direct_change_locator(locator.change_id) != Some(locator)
        {
            return Err(replacement_payload_error(
                "direct change coordinate disagrees with its encoded ChangeId owner",
            ));
        }
        let unique_index = if unique_locator_indices
            .last()
            .is_some_and(|&previous| locators[previous] == locator)
        {
            unique_locator_indices.len() - 1
        } else {
            unique_locator_indices.push(request_index);
            unique_locator_indices.len() - 1
        };
        output_routes.push((request_index, unique_index));
    }
    let coordinates = unique_locator_indices
        .iter()
        .map(|&index| {
            let locator = locators[index];
            super::mutation_directory::MutationDirectoryDirectCoordinate {
                part_index: locator.segment_index,
                local_row: locator.ordinal,
            }
        })
        .collect::<Vec<_>>();
    #[cfg(any(test, feature = "storage-benches"))]
    super::mutation_directory::record_direct_route_unique_rows(coordinates.len());

    let unique = if let Some(root) = state.mutation_directory_root.as_ref() {
        let (runs, not_owned) = super::mutation_directory::load_mutation_part_read_plan(
            store,
            root,
            super::mutation_directory::MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(
                &coordinates,
            ),
        )
        .await?
        .into_direct_routes();
        let mut unique = (0..coordinates.len()).map(|_| None).collect::<Vec<_>>();
        for route in not_owned {
            let reason = DirectNotOwnedReason::from(route.reason);
            #[cfg(any(test, feature = "storage-benches"))]
            super::mutation_directory::record_direct_route_not_owned(
                match reason {
                    DirectNotOwnedReason::UnsupportedLayout => {
                        super::mutation_directory::MutationDirectoryNotOwnedReason::UnsupportedLayout
                    }
                    DirectNotOwnedReason::PartIndexOutOfRange => {
                        super::mutation_directory::MutationDirectoryNotOwnedReason::PartIndexOutOfRange
                    }
                    DirectNotOwnedReason::LocalRowOutOfRange => {
                        super::mutation_directory::MutationDirectoryNotOwnedReason::LocalRowOutOfRange
                    }
                    DirectNotOwnedReason::MissingCommitAuthority
                    | DirectNotOwnedReason::AbsentInlineAuthority => {
                        continue;
                    }
                },
                route.selector_span.len(),
            );
            for selector_index in route.selector_span {
                unique[selector_index] = Some(DirectChangeRecordRoute::NotOwned(reason));
            }
        }
        for run in &runs {
            for coordinate in &coordinates[run.selector_span.clone()] {
                if coordinate.part_index != run.entry_index {
                    return Err(replacement_payload_error(
                        "direct-coordinate plan selected the wrong physical part",
                    ));
                }
            }
        }
        if let Some(parts) = state.mutations.columnar_parts.as_ref() {
            for run in &runs {
                let super::mutation_directory::MutationDirectoryEntry::DirectAddress {
                    direct_row_count,
                } = run.entry
                else {
                    return Err(replacement_payload_error(
                        "columnar direct-coordinate plan selected a physical part",
                    ));
                };
                if coordinates[run.selector_span.clone()]
                    .iter()
                    .any(|coordinate| coordinate.local_row >= direct_row_count)
                {
                    return Err(replacement_payload_error(
                        "columnar direct coordinate exceeds its part row count",
                    ));
                }
            }
            let owned_indices = runs
                .iter()
                .flat_map(|run| run.selector_span.clone())
                .collect::<Vec<_>>();
            #[cfg(any(test, feature = "storage-benches"))]
            super::mutation_directory::record_direct_route_claimed_rows(owned_indices.len());
            let records = load_columnar_direct_change_records(
                store,
                state,
                parts,
                &owned_indices
                    .iter()
                    .map(|&index| locators[unique_locator_indices[index]])
                    .collect::<Vec<_>>(),
            )
            .await?;
            for (selector_index, record) in owned_indices.into_iter().zip(records) {
                unique[selector_index] = Some(DirectChangeRecordRoute::Owned(record));
            }
        } else {
            #[cfg(any(test, feature = "storage-benches"))]
            super::mutation_directory::record_direct_route_claimed_rows(
                runs.iter().map(|run| run.selector_span.len()).sum(),
            );
            for (selector_index, record) in load_physical_direct_change_records(
                store,
                state,
                &coordinates,
                &unique_locator_indices
                    .iter()
                    .map(|&index| locators[index])
                    .collect::<Vec<_>>(),
                runs,
            )
            .await?
            {
                unique[selector_index] = Some(DirectChangeRecordRoute::Owned(record));
            }
        }
        unique
            .into_iter()
            .map(|route| {
                route.ok_or_else(|| {
                    replacement_payload_error(
                        "direct-coordinate plan lost a claimed or unclaimed selector",
                    )
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?
    } else {
        if state.mutations.inline_part.is_empty()
            || state.mutations.direct_part_row_counts.len() != 1
        {
            return Err(replacement_payload_error(
                "direct change candidate has invalid authenticated inline authority",
            ));
        }
        let direct_row_count = state.mutations.direct_part_row_counts[0];
        let owned_indices = coordinates
            .iter()
            .enumerate()
            .filter_map(|(index, coordinate)| {
                (coordinate.part_index == 0 && coordinate.local_row < direct_row_count)
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        let mut unique = coordinates
            .iter()
            .map(|coordinate| {
                if coordinate.part_index != 0 {
                    Some(DirectChangeRecordRoute::NotOwned(
                        DirectNotOwnedReason::PartIndexOutOfRange,
                    ))
                } else if coordinate.local_row >= direct_row_count {
                    Some(DirectChangeRecordRoute::NotOwned(
                        DirectNotOwnedReason::LocalRowOutOfRange,
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if owned_indices.is_empty() {
            unique
                .into_iter()
                .map(|route| {
                    route.ok_or_else(|| {
                        replacement_payload_error("inline direct route lost an unclaimed selector")
                    })
                })
                .collect::<Result<Vec<_>, LixError>>()?
        } else {
            #[cfg(any(test, feature = "storage-benches"))]
            super::mutation_directory::record_direct_route_claimed_rows(owned_indices.len());
            let (leaf, payloads) =
                decode_commit_delta_with_payloads(&state.mutations.inline_part, None)?;
            if leaf.len() != usize::from(direct_row_count)
                || leaf.len() != state.mutations.member_count as usize
            {
                return Err(replacement_payload_error(
                    "inline direct payload row count disagrees with authenticated authority",
                ));
            }
            for selector_index in owned_indices {
                let locator_index = unique_locator_indices[selector_index];
                let record = decode_change_at_locator_from_decoded(
                    &leaf,
                    &payloads,
                    locators[locator_index],
                    &state.change_account_id,
                )
                .map(|entry| entry.change_record)?;
                unique[selector_index] = Some(DirectChangeRecordRoute::Owned(record));
            }
            unique
                .into_iter()
                .map(|route| {
                    route.ok_or_else(|| {
                        replacement_payload_error(
                            "inline direct route lost a claimed or unclaimed selector",
                        )
                    })
                })
                .collect::<Result<Vec<_>, LixError>>()?
        }
    };

    let mut output = (0..locators.len()).map(|_| None).collect::<Vec<_>>();
    for (request_index, unique_index) in output_routes {
        output[request_index] = Some(unique[unique_index].clone());
    }
    #[cfg(any(test, feature = "storage-benches"))]
    super::mutation_directory::record_direct_route_scattered_rows(locators.len());
    #[cfg(any(test, feature = "storage-benches"))]
    accounting_guard.finish();
    output
        .into_iter()
        .map(|route| {
            route.ok_or_else(|| {
                replacement_payload_error("direct change record scatter lost a requested row")
            })
        })
        .collect()
}

async fn load_columnar_direct_change_records(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    parts: &crate::tracked_state::types::ColumnarMutationPartSet,
    locators: &[CommitDeltaChangeLocator],
) -> Result<Vec<crate::changelog::ChangeRecord>, LixError> {
    let id = crate::columnar_row_group::RowGroupSetId::new(parts.row_group_set_id);
    let manifest = crate::columnar_row_group::load_row_group_manifest(store, id)
        .await?
        .ok_or_else(|| replacement_payload_error("columnar mutation manifest is missing"))?;
    validate_columnar_mutation_manifest(&manifest, parts)?;
    let projection = (0..manifest.fields.len()).collect::<Vec<_>>();
    let mut page_groups = BTreeMap::<(usize, usize), Vec<(usize, usize)>>::new();
    for (output_index, &locator) in locators.iter().enumerate() {
        let logical_ordinal = usize::try_from(locator.segment_index)
            .expect("u32 segment fits usize")
            .checked_mul(COMMIT_DELTA_SEGMENT_MAX_ROWS)
            .and_then(|base| base.checked_add(usize::from(locator.ordinal)))
            .ok_or_else(|| replacement_payload_error("columnar mutation address overflows"))?;
        if logical_ordinal >= parts.row_count as usize {
            return Err(replacement_payload_error(
                "columnar mutation locator is outside its inventory",
            ));
        }
        let group_index = logical_ordinal / crate::columnar_row_group::ROW_GROUP_MAX_ROWS;
        let row_in_group = logical_ordinal % crate::columnar_row_group::ROW_GROUP_MAX_ROWS;
        let page_index = row_in_group / crate::columnar_row_group::ROW_GROUP_PAGE_ROWS;
        let row_in_page = row_in_group % crate::columnar_row_group::ROW_GROUP_PAGE_ROWS;
        page_groups
            .entry((group_index, page_index))
            .or_default()
            .push((output_index, row_in_page));
    }
    let page_coordinates = page_groups.keys().copied().collect::<Vec<_>>();
    let mut output = (0..locators.len()).map(|_| None).collect::<Vec<_>>();
    crate::columnar_row_group::visit_row_group_pages(
        store,
        id,
        &manifest,
        &page_coordinates,
        &projection,
        |coordinate, batch| {
            for &(output_index, row_in_page) in &page_groups[&coordinate] {
                let locator = locators[output_index];
                output[output_index] = Some(decode_columnar_change_record(
                    &manifest,
                    &batch,
                    row_in_page,
                    parts,
                    locator.change_id,
                    &state.change_account_id,
                )?);
            }
            Ok(())
        },
    )
    .await?;
    output
        .into_iter()
        .map(|record| {
            record.ok_or_else(|| {
                replacement_payload_error("columnar direct coordinate lost its payload row")
            })
        })
        .collect()
}

async fn load_physical_direct_change_records(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    coordinates: &[super::mutation_directory::MutationDirectoryDirectCoordinate],
    locators: &[CommitDeltaChangeLocator],
    runs: Vec<super::mutation_directory::MutationDirectoryPartRun>,
) -> Result<Vec<(usize, crate::changelog::ChangeRecord)>, LixError> {
    #[cfg(any(test, feature = "storage-benches"))]
    super::mutation_directory::record_direct_external_parts_loaded(runs.len());
    let storage_keys = runs
        .iter()
        .map(|run| match &run.entry {
            super::mutation_directory::MutationDirectoryEntry::Bounded { part, .. } => {
                commit_delta_segment_key_for_part(
                    state.commit_id,
                    usize::try_from(run.entry_index)
                        .map_err(|_| replacement_payload_error("part index exceeds usize"))?,
                    part,
                )
            }
            super::mutation_directory::MutationDirectoryEntry::CompactReplacement {
                content_digest,
                ..
            } => {
                let mut key = commit_delta_segment_key(
                    state.commit_id,
                    usize::try_from(run.entry_index)
                        .map_err(|_| replacement_payload_error("part index exceeds usize"))?,
                )?;
                key.extend_from_slice(content_digest);
                Ok(key)
            }
            super::mutation_directory::MutationDirectoryEntry::DirectAddress { .. } => Err(
                replacement_payload_error("non-columnar direct authority has no physical part"),
            ),
        })
        .map(|key| key.map(|key| StorageKey(Bytes::from(key))))
        .collect::<Result<Vec<_>, LixError>>()?;
    let values =
        PointReadPlan::from_unique_keys(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, storage_keys)
            .materialize(store, StorageGetOptions::default())
            .await?;
    let mut output = Vec::with_capacity(coordinates.len());
    for (run, value) in runs.into_iter().zip(values.value) {
        let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
            replacement_payload_error("direct coordinate references a missing immutable part")
        })?;
        let super::mutation_directory::MutationDirectoryPartRun {
            entry_index,
            entry,
            selector_span,
        } = run;
        let (bounds, direct_row_count) = match entry {
            super::mutation_directory::MutationDirectoryEntry::Bounded {
                part,
                direct_row_count,
            } => (
                CommitDeltaSegmentBounds {
                    first_key: part.first_key,
                    last_key: part.last_key,
                    replacement_part: part.replacement_part,
                },
                direct_row_count,
            ),
            super::mutation_directory::MutationDirectoryEntry::CompactReplacement {
                content_digest,
                direct_row_count,
            } => {
                hydrate_compact_replacement_direct_run(
                    state,
                    entry_index,
                    selector_span,
                    content_digest,
                    direct_row_count,
                    &bytes,
                    coordinates,
                    locators,
                    &mut output,
                )?;
                continue;
            }
            super::mutation_directory::MutationDirectoryEntry::DirectAddress { .. } => {
                return Err(replacement_payload_error(
                    "non-columnar direct authority selected an address-only entry",
                ));
            }
        };
        let (leaf, payloads) = decode_commit_delta_with_payloads(&bytes, Some(&bounds))?;
        if leaf.len() != usize::from(direct_row_count) {
            return Err(replacement_payload_error(
                "direct immutable part row count disagrees with directory authority",
            ));
        }
        #[cfg(any(test, feature = "storage-benches"))]
        super::mutation_directory::record_direct_part_decoded(
            leaf.len(),
            bytes.len(),
            leaf.resident_bytes() + payloads.resident_bytes(),
        );
        for output_index in selector_span {
            let coordinate = coordinates[output_index];
            let locator = locators[output_index];
            if coordinate.part_index != entry_index || coordinate.local_row != locator.ordinal {
                return Err(replacement_payload_error(
                    "direct-coordinate run disagrees with its physical locator",
                ));
            }
            output.push((
                output_index,
                decode_change_at_locator_from_decoded(
                    &leaf,
                    &payloads,
                    locator,
                    &state.change_account_id,
                )?
                .change_record,
            ));
        }
    }
    Ok(output)
}

#[inline(never)]
fn hydrate_compact_replacement_direct_run(
    state: &AuthenticatedReplayCommitStateManifest,
    entry_index: u32,
    selector_span: Range<usize>,
    content_digest: [u8; 32],
    direct_row_count: u16,
    bytes: &[u8],
    coordinates: &[super::mutation_directory::MutationDirectoryDirectCoordinate],
    locators: &[CommitDeltaChangeLocator],
    output: &mut Vec<(usize, crate::changelog::ChangeRecord)>,
) -> Result<(), LixError> {
    let generation = state
        .mutations
        .replacement_generation
        .as_ref()
        .ok_or_else(|| replacement_payload_error("compact part omitted its generation"))?;
    let lifecycle = state
        .mutations
        .lifecycle_summary
        .as_ref()
        .ok_or_else(|| replacement_payload_error("compact part omitted lifecycle metadata"))?;
    let authority = state
        .mutations
        .replacement_parts
        .as_ref()
        .ok_or_else(|| replacement_payload_error("compact part omitted payload authority"))?;
    if generation.owner_commit_id != *state.commit_id.as_uuid().as_bytes()
        || generation.integrity_digest
            != replacement_generation_integrity_digest(generation, lifecycle, authority)
    {
        return Err(replacement_payload_error(
            "compact part generation authority is invalid",
        ));
    }
    let decoded =
        crate::tracked_state::replacement_part::decode_replacement_part(&content_digest, bytes)?;
    if decoded.len() != usize::from(direct_row_count) {
        return Err(replacement_payload_error(
            "compact immutable part row count disagrees with directory authority",
        ));
    }
    #[cfg(any(test, feature = "storage-benches"))]
    super::mutation_directory::record_direct_part_decoded(
        decoded.len(),
        bytes.len(),
        decoded.len(),
    );
    for output_index in selector_span {
        let coordinate = coordinates[output_index];
        let locator = locators[output_index];
        if coordinate.part_index != entry_index || coordinate.local_row != locator.ordinal {
            return Err(replacement_payload_error(
                "direct-coordinate run disagrees with its physical locator",
            ));
        }
        let packed = entry_index
            .checked_mul(
                u32::try_from(COMMIT_DELTA_SEGMENT_MAX_ROWS)
                    .expect("replacement row bound fits u32"),
            )
            .and_then(|base| base.checked_add(u32::from(locator.ordinal)))
            .and_then(|address| address.checked_add(1))
            .ok_or_else(|| replacement_payload_error("replacement address overflows"))?;
        if locator.change_id != change_id_from_packed_address(state.commit_id, packed) {
            return Err(replacement_payload_error(
                "compact locator change id disagrees with its physical address",
            ));
        }
        let encoded_key = decoded
            .key(usize::from(locator.ordinal))?
            .ok_or_else(|| replacement_payload_error("replacement part omitted a key"))?;
        let key = decode_key(encoded_key)?;
        let snapshot = owned_scoped_json_slot(
            decoded
                .snapshot(usize::from(locator.ordinal))?
                .ok_or_else(|| replacement_payload_error("replacement part omitted a snapshot"))?,
        );
        let metadata = owned_scoped_json_slot(
            decoded
                .metadata(usize::from(locator.ordinal))?
                .ok_or_else(|| replacement_payload_error("replacement part omitted metadata"))?,
        );
        output.push((
            output_index,
            crate::changelog::ChangeRecord {
                account_id: state.change_account_id.clone(),
                format_version: 2,
                change_id: locator.change_id,
                schema_key: key.schema_key,
                row_pk: key.row_pk,
                file_id: key.file_id,
                snapshot,
                metadata,
                created_at: lifecycle.uniform_created_at,
                origin_key: None,
            },
        ));
    }
    Ok(())
}

async fn load_change_records_by_ids(
    store: &(impl StorageAdapterRead + ?Sized),
    change_ids: &[crate::changelog::ChangeId],
) -> Result<Vec<crate::changelog::ChangeRecord>, LixError> {
    if change_ids.is_empty() {
        return Ok(Vec::new());
    }
    // Self-contained sync checkpoints may carry selected payloads while the
    // authored commit body remains lazy. Prefer their canonical standalone
    // ChangeRecords before attempting commit-delta routing.
    let stored = ChangelogContext::new()
        .reader(store)
        .load_changes(ChangeLoadRequest { change_ids })
        .await?;
    let mut output = stored
        .into_iter()
        .map(|(_, record)| record)
        .collect::<Vec<_>>();
    let mut authority_cache = BTreeMap::<CommitId, DirectChangeAuthority>::new();
    let mut direct_by_commit = BTreeMap::<
        CommitId,
        (
            Arc<AuthenticatedReplayCommitStateManifest>,
            Vec<(usize, CommitDeltaChangeLocator)>,
        ),
    >::new();
    let mut explicit = Vec::<(usize, crate::changelog::ChangeId)>::new();
    for (output_index, &change_id) in change_ids.iter().enumerate() {
        if output[output_index].is_some() {
            continue;
        }
        let Some(locator) = direct_change_locator(change_id) else {
            explicit.push((output_index, change_id));
            continue;
        };
        let authority = match authority_cache.get(&locator.commit_id) {
            Some(authority) => authority.clone(),
            None => {
                let authority = load_direct_change_authority(store, locator.commit_id).await?;
                authority_cache.insert(locator.commit_id, authority.clone());
                authority
            }
        };
        let authority = match authority {
            DirectChangeAuthority::Candidate(authority) => authority,
            DirectChangeAuthority::NotOwned(reason) => {
                let _ = reason;
                explicit.push((output_index, change_id));
                continue;
            }
        };
        direct_by_commit
            .entry(locator.commit_id)
            .or_insert_with(|| (authority, Vec::new()))
            .1
            .push((output_index, locator));
    }

    for (_, (authority, requests)) in direct_by_commit {
        let locators = requests
            .iter()
            .map(|(_, locator)| *locator)
            .collect::<Vec<_>>();
        let routes = Box::pin(route_direct_change_records_for_state(
            store, &authority, &locators,
        ))
        .await?;
        for ((output_index, locator), route) in requests.into_iter().zip(routes) {
            match route {
                DirectChangeRecordRoute::Owned(record) => output[output_index] = Some(record),
                DirectChangeRecordRoute::NotOwned(reason) => {
                    let _ = reason;
                    #[cfg(any(test, feature = "storage-benches"))]
                    super::mutation_directory::record_direct_route_explicit_fallback(1);
                    explicit.push((output_index, locator.change_id));
                }
            }
        }
    }

    if !explicit.is_empty() {
        let locator_keys = explicit
            .iter()
            .map(|(_, change_id)| {
                StorageKey(Bytes::copy_from_slice(change_id.as_uuid().as_bytes()))
            })
            .collect::<Vec<_>>();
        let locator_values = PointReadPlan::new(TRACKED_STATE_CHANGE_LOCATOR_SPACE, &locator_keys)
            .materialize(store, StorageGetOptions::default())
            .await?;
        let locators = explicit
            .iter()
            .zip(locator_values.value)
            .map(|((_, change_id), value)| {
                let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
                    replacement_payload_error(&format!(
                        "selected change '{change_id}' has no authoritative locator"
                    ))
                })?;
                decode_change_locator(*change_id, &bytes)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let records = Box::pin(load_explicit_change_records_at_locators(store, &locators)).await?;
        for ((output_index, _), record) in explicit.into_iter().zip(records) {
            output[output_index] = Some(record);
        }
    }
    output
        .into_iter()
        .map(|record| {
            record.ok_or_else(|| {
                replacement_payload_error("selected change resolution lost a requested row")
            })
        })
        .collect()
}

async fn load_explicit_change_records_at_locators(
    store: &(impl StorageAdapterRead + ?Sized),
    locators: &[CommitDeltaChangeLocator],
) -> Result<Vec<crate::changelog::ChangeRecord>, LixError> {
    load_explicit_change_records_at_locators_selected(store, locators).await
}

async fn load_explicit_change_records_at_locators_selected(
    store: &(impl StorageAdapterRead + ?Sized),
    locators: &[CommitDeltaChangeLocator],
) -> Result<Vec<crate::changelog::ChangeRecord>, LixError> {
    let commit_ids = locators
        .iter()
        .map(|locator| locator.commit_id)
        .collect::<BTreeSet<_>>();
    let mut states = BTreeMap::<CommitId, Arc<AuthenticatedReplayCommitStateManifest>>::new();
    for commit_id in commit_ids {
        let state = load_point_replay_commit_state(store, commit_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state selected change references missing commit '{commit_id}'"
                    ),
                )
            })?;
        if let Some(source_commit_id) = state.mutations.selected_source_commit_id() {
            let source = load_point_replay_commit_state(store, source_commit_id)
                .await?
                .ok_or_else(|| {
                    replacement_payload_error(&format!(
                        "selected-source commit '{}' references missing authority '{}'",
                        state.commit_id, source_commit_id
                    ))
                })?;
            if source.mutations.selected_source_commit_id().is_some() {
                return Err(replacement_payload_error(
                    "selected-source mutation authority cannot alias another source",
                ));
            }
        }
        states.insert(commit_id, state);
    }

    let mut loaded = (0..locators.len()).map(|_| None).collect::<Vec<_>>();
    let mut by_commit = BTreeMap::<CommitId, Vec<usize>>::new();
    for (index, locator) in locators.iter().enumerate() {
        by_commit.entry(locator.commit_id).or_default().push(index);
    }
    for (commit_id, indices) in by_commit {
        let state = states
            .get(&commit_id)
            .expect("every selected locator has an authenticated state");
        let requested = indices
            .iter()
            .map(|&index| locators[index])
            .collect::<Vec<_>>();
        let records = load_selected_change_records_from_state(store, state, &requested).await?;
        for (index, record) in indices.into_iter().zip(records) {
            loaded[index] = Some(record);
        }
    }
    loaded
        .into_iter()
        .map(|entry| {
            entry.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state authoritative change unexpectedly disappeared",
                )
            })
        })
        .collect()
}

async fn load_selected_change_records_from_state(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    locators: &[CommitDeltaChangeLocator],
) -> Result<Vec<crate::changelog::ChangeRecord>, LixError> {
    if locators.is_empty() {
        return Ok(Vec::new());
    }
    if locators
        .iter()
        .any(|locator| locator.commit_id != state.commit_id)
    {
        return Err(replacement_payload_error(
            "selected change locator disagrees with its authenticated owner",
        ));
    }
    #[cfg(any(test, feature = "storage-benches"))]
    let mut accounting_guard = super::mutation_directory::DirectRouteAccountingGuard::new();
    #[cfg(any(test, feature = "storage-benches"))]
    super::mutation_directory::record_direct_route_start(locators.len());

    let mut request_indices = (0..locators.len()).collect::<Vec<_>>();
    request_indices.sort_by_key(|&index| {
        let locator = locators[index];
        (locator.segment_index, locator.ordinal, locator.change_id)
    });
    let mut unique_indices = Vec::with_capacity(request_indices.len());
    let mut scatter = Vec::with_capacity(request_indices.len());
    for request_index in request_indices {
        let unique_index = if unique_indices
            .last()
            .is_some_and(|&previous| locators[previous] == locators[request_index])
        {
            unique_indices.len() - 1
        } else {
            unique_indices.push(request_index);
            unique_indices.len() - 1
        };
        scatter.push((request_index, unique_index));
    }
    let unique_locators = unique_indices
        .iter()
        .map(|&index| locators[index])
        .collect::<Vec<_>>();
    #[cfg(any(test, feature = "storage-benches"))]
    super::mutation_directory::record_direct_route_unique_rows(unique_locators.len());

    let unique_records = if let Some(root) = state.mutation_directory_root.as_ref() {
        let coordinates = unique_locators
            .iter()
            .map(
                |locator| super::mutation_directory::MutationDirectoryDirectCoordinate {
                    part_index: locator.segment_index,
                    local_row: locator.ordinal,
                },
            )
            .collect::<Vec<_>>();
        let (runs, not_owned) = super::mutation_directory::load_mutation_part_read_plan(
            store,
            root,
            super::mutation_directory::MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(
                &coordinates,
            ),
        )
        .await?
        .into_direct_routes();
        if let Some(route) = not_owned.into_iter().next() {
            return Err(replacement_payload_error(&format!(
                "selected change locator is not owned by its authenticated directory ({:?})",
                route.reason
            )));
        }
        #[cfg(any(test, feature = "storage-benches"))]
        super::mutation_directory::record_direct_route_claimed_rows(unique_locators.len());
        if let Some(parts) = state.mutations.columnar_parts.as_ref() {
            load_columnar_direct_change_records(store, state, parts, &unique_locators).await?
        } else {
            let routed = load_physical_direct_change_records(
                store,
                state,
                &coordinates,
                &unique_locators,
                runs,
            )
            .await?;
            let mut records = (0..unique_locators.len()).map(|_| None).collect::<Vec<_>>();
            for (index, record) in routed {
                records[index] = Some(record);
            }
            records
                .into_iter()
                .map(|record| {
                    record.ok_or_else(|| {
                        replacement_payload_error(
                            "selected change directory route lost a requested row",
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        }
    } else if let Some(parts) = state.mutations.columnar_parts.as_ref() {
        load_columnar_direct_change_records(store, state, parts, &unique_locators).await?
    } else {
        if state.mutations.inline_part.is_empty() {
            return Err(replacement_payload_error(
                "selected change owner has no authenticated physical authority",
            ));
        }
        let (leaf, payloads) =
            decode_commit_delta_with_payloads(&state.mutations.inline_part, None)?;
        if leaf.len() != state.mutations.member_count as usize {
            return Err(replacement_payload_error(
                "inline selected change authority row count disagrees with its header",
            ));
        }
        unique_locators
            .iter()
            .map(|&locator| {
                if locator.segment_index != 0 {
                    return Err(replacement_payload_error(
                        "inline selected change locator names a nonzero segment",
                    ));
                }
                Ok(decode_change_at_locator_from_decoded(
                    &leaf,
                    &payloads,
                    locator,
                    &state.change_account_id,
                )?
                .change_record)
            })
            .collect::<Result<Vec<_>, LixError>>()?
    };

    let mut output = (0..locators.len()).map(|_| None).collect::<Vec<_>>();
    for (request_index, unique_index) in scatter {
        output[request_index] = Some(unique_records[unique_index].clone());
    }
    #[cfg(any(test, feature = "storage-benches"))]
    {
        super::mutation_directory::record_direct_route_scattered_rows(locators.len());
        accounting_guard.finish();
    }
    output
        .into_iter()
        .map(|record| {
            record.ok_or_else(|| {
                replacement_payload_error("selected change scatter lost a requested row")
            })
        })
        .collect()
}

fn decode_change_at_locator_from_decoded<S>(
    leaf: &DecodedLeafNodeRef,
    payloads: &CommitDeltaPayloadIndex<S>,
    locator: CommitDeltaChangeLocator,
    account_id: &str,
) -> Result<LoadedCommitDeltaEntry, LixError>
where
    S: AsRef<[u8]>,
{
    let change_id = locator.change_id;
    let ordinal = usize::from(locator.ordinal);
    let entry = leaf.entry(ordinal).ok_or_else(|| {
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
            account_id: account_id.to_string(),
            format_version: 2,
            change_id,
            schema_key: key.schema_key,
            row_pk: key.row_pk,
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

pub(crate) fn validate_columnar_mutation_manifest(
    manifest: &crate::columnar_row_group::RowGroupManifest,
    parts: &crate::tracked_state::types::ColumnarMutationPartSet,
) -> Result<(), LixError> {
    let owner = CommitId::new(uuid::Uuid::from_bytes(parts.owner_commit_id));
    if crate::row_columnar::row_group_set_id(owner, &parts.schema_key).as_bytes()
        != parts.row_group_set_id
        || manifest.content_digest()? != parts.manifest_digest
        || manifest.namespace != parts.schema_key
        || crate::row_columnar::row_identity_column_index(manifest).is_none()
        || manifest
            .groups
            .iter()
            .map(|group| group.row_count)
            .collect::<Vec<_>>()
            != parts.group_row_counts
    {
        return Err(replacement_payload_error(
            "columnar mutation manifest disagrees with commit authority",
        ));
    }
    Ok(())
}

fn decode_columnar_change_record(
    manifest: &crate::columnar_row_group::RowGroupManifest,
    batch: &datafusion::arrow::record_batch::RecordBatch,
    row_index: usize,
    parts: &crate::tracked_state::types::ColumnarMutationPartSet,
    change_id: crate::changelog::ChangeId,
    account_id: &str,
) -> Result<crate::changelog::ChangeRecord, LixError> {
    use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};

    let identity_column = batch
        .column(batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| replacement_payload_error("columnar mutation identity is not UTF-8"))?;
    if identity_column.is_null(row_index) {
        return Err(replacement_payload_error(
            "columnar mutation identity is null",
        ));
    }
    let row_pk = RowPk::from_json_array_text(identity_column.value(row_index))
        .map_err(|error| replacement_payload_error(&error.to_string()))?;
    let mut snapshot = serde_json::Map::new();
    for (column_index, field) in manifest
        .fields
        .iter()
        .take(manifest.fields.len() - 1)
        .enumerate()
    {
        let column = batch.column(column_index);
        let value = if column.is_null(row_index) {
            serde_json::Value::Null
        } else {
            match field.data_type {
                crate::columnar_row_group::RowGroupDataType::String => {
                    let value = column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| replacement_payload_error("columnar string type drift"))?
                        .value(row_index);
                    if field.metadata.get("lix.value_type").map(String::as_str) == Some("json") {
                        serde_json::from_str(value).map_err(|error| {
                            replacement_payload_error(&format!(
                                "columnar JSON value is invalid: {error}"
                            ))
                        })?
                    } else {
                        serde_json::Value::String(value.to_owned())
                    }
                }
                crate::columnar_row_group::RowGroupDataType::Int64 => serde_json::Value::Number(
                    column
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| replacement_payload_error("columnar integer type drift"))?
                        .value(row_index)
                        .into(),
                ),
                crate::columnar_row_group::RowGroupDataType::Float64 => {
                    let value = column
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| replacement_payload_error("columnar number type drift"))?
                        .value(row_index);
                    serde_json::Number::from_f64(value)
                        .map(serde_json::Value::Number)
                        .ok_or_else(|| replacement_payload_error("columnar number is non-finite"))?
                }
                crate::columnar_row_group::RowGroupDataType::Boolean => serde_json::Value::Bool(
                    column
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| replacement_payload_error("columnar boolean type drift"))?
                        .value(row_index),
                ),
            }
        };
        snapshot.insert(field.name.clone(), value);
    }
    let snapshot = serde_json::to_string(&snapshot)
        .map_err(|error| replacement_payload_error(&error.to_string()))?;
    Ok(crate::changelog::ChangeRecord {
        account_id: account_id.to_string(),
        format_version: 2,
        change_id,
        schema_key: parts.schema_key.clone(),
        row_pk,
        file_id: None,
        snapshot: crate::json_store::JsonSlot::from_json(&snapshot),
        metadata: crate::json_store::JsonSlot::None,
        created_at: parts.uniform_updated_at,
        origin_key: parts.origin_key.clone(),
    })
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
    let Some(state) = load_point_replay_commit_state(store, commit_id).await? else {
        return Ok(vec![None; encoded_keys.len()]);
    };
    load_commit_delta_values_encoded_from_replay_manifest(
        store,
        &state,
        encoded_keys,
        &CommitDeltaPointReadCache::default(),
    )
    .await
}

async fn load_expanded_local_commit_delta_values_encoded(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    encoded_keys: &[Bytes],
    point_cache: &CommitDeltaPointReadCache,
) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
    if state.mutation_directory_root.as_ref().is_some_and(|root| {
        root.layout == super::mutation_directory::LAYOUT_BOUNDED_DIRECT
            || root.layout == super::mutation_directory::LAYOUT_BOUNDED_INDIRECT
    }) {
        return Err(replacement_payload_error(
            "bounded mutation authority cannot use an expanded point-read manifest",
        ));
    }
    let manifest = match point_cache.manifest(state.commit_id)? {
        Some(manifest) => manifest,
        None => {
            let manifest =
                Arc::new(expanded_commit_delta_manifest_from_commit_state(store, &state).await?);
            point_cache.remember_manifest(state.commit_id, Arc::clone(&manifest))?;
            manifest
        }
    };
    load_local_commit_delta_values_encoded(store, state.commit_id, encoded_keys, &manifest).await
}

async fn load_authenticated_local_commit_delta_values_encoded(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    encoded_keys: &[Bytes],
    point_cache: &CommitDeltaPointReadCache,
) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
    if state.mutation_directory_root.as_ref().is_some_and(|root| {
        root.layout == super::mutation_directory::LAYOUT_BOUNDED_DIRECT
            || root.layout == super::mutation_directory::LAYOUT_BOUNDED_INDIRECT
    }) {
        return load_bounded_directory_values_encoded(store, state, encoded_keys).await;
    }
    if state
        .mutation_directory_root
        .as_ref()
        .is_some_and(|root| root.layout == super::mutation_directory::LAYOUT_COMPACT_REPLACEMENT)
    {
        if let Some(root) = state.current_state_scoped_ranges.as_ref()
            && let Some(values) =
                load_complete_current_state_values_from_scoped_root(store, root, encoded_keys)
                    .await?
        {
            return Ok(values);
        }
        let full_state = load_commit_state_manifest(store, state.commit_id)
            .await?
            .ok_or_else(|| {
                replacement_payload_error(
                    "compact point replay lost its mutation-directory authority",
                )
            })?;
        return load_compact_replacement_values_encoded(
            store,
            state.commit_id,
            encoded_keys,
            &full_state,
        )
        .await;
    }
    load_expanded_local_commit_delta_values_encoded(store, state, encoded_keys, point_cache).await
}

/// Replays a strict sorted-unique key batch from one authenticated immutable
/// commit authority. Selected-source authority is loaded and authenticated in
/// the same storage snapshot; cached expanded manifests are used only for
/// layouts that have no bounded hierarchy.
pub(crate) async fn load_commit_delta_values_encoded_from_replay_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    encoded_keys: &[Bytes],
    point_cache: &CommitDeltaPointReadCache,
) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
    if !encoded_keys.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(replacement_payload_error(
            "point read requires strict sorted-unique encoded keys",
        ));
    }
    point_cache.remember_authenticated_state(state)?;
    let Some(source_commit_id) = state.mutations.selected_source_commit_id() else {
        return load_authenticated_local_commit_delta_values_encoded(
            store,
            state,
            encoded_keys,
            point_cache,
        )
        .await;
    };
    let source = match point_cache.authority(source_commit_id)? {
        Some(source) => source,
        None => {
            let source = load_point_replay_commit_state(store, source_commit_id)
                .await?
                .ok_or_else(|| {
                    replacement_payload_error(&format!(
                        "selected-source commit '{}' references missing authority '{source_commit_id}'",
                        state.commit_id
                    ))
                })?;
            point_cache.remember_authority(Arc::clone(&source))?;
            source
        }
    };
    if source.mutations.selected_source_commit_id().is_some() {
        return Err(replacement_payload_error(
            "selected-source mutation authority cannot alias another source",
        ));
    }
    let mut values = load_authenticated_local_commit_delta_values_encoded(
        store,
        &source,
        encoded_keys,
        point_cache,
    )
    .await?;
    for value in values.iter_mut().flatten() {
        value.commit_id = state.commit_id;
    }
    let local = load_authenticated_local_commit_delta_values_encoded(
        store,
        state,
        encoded_keys,
        point_cache,
    )
    .await?;
    for (value, local) in values.iter_mut().zip(local) {
        if local.is_some() {
            *value = local;
        }
    }
    Ok(values)
}

async fn load_bounded_directory_values_encoded(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    encoded_keys: &[Bytes],
) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
    let root = state.mutation_directory_root.as_ref().ok_or_else(|| {
        replacement_payload_error("bounded point replay omitted its mutation-directory root")
    })?;
    let runs = super::mutation_directory::load_mutation_part_read_plan(
        store,
        root,
        super::mutation_directory::MutationDirectoryReadSelection::SortedUniquePoints(encoded_keys),
    )
    .await?
    .into_runs();
    let storage_keys = runs
        .iter()
        .map(|run| {
            let super::mutation_directory::MutationDirectoryEntry::Bounded { part, .. } =
                &run.entry
            else {
                return Err(replacement_payload_error(
                    "bounded point replay selected a non-bounded part",
                ));
            };
            commit_delta_segment_key_for_part(
                state.commit_id,
                usize::try_from(run.entry_index)
                    .map_err(|_| replacement_payload_error("part index exceeds usize"))?,
                part,
            )
            .map(|key| StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let loaded =
        PointReadPlan::from_unique_keys(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, storage_keys)
            .materialize(store, StorageGetOptions::default())
            .await?;
    let mut output = vec![None; encoded_keys.len()];
    for (run, value) in runs.into_iter().zip(loaded.value) {
        let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
            replacement_payload_error("bounded mutation directory references a missing part")
        })?;
        let super::mutation_directory::MutationDirectoryEntry::Bounded {
            part,
            direct_row_count,
        } = run.entry
        else {
            return Err(replacement_payload_error(
                "bounded point replay selected a non-bounded part",
            ));
        };
        let bounds = CommitDeltaSegmentBounds {
            first_key: part.first_key,
            last_key: part.last_key,
            replacement_part: part.replacement_part,
        };
        let (leaf, payloads) = decode_commit_delta_with_payloads(&bytes, Some(&bounds))?;
        validate_bounded_direct_row_count(root.layout, direct_row_count, leaf.len())?;
        for output_index in run.selector_span {
            output[output_index] = find_loaded_commit_delta_entry(
                &leaf,
                &payloads,
                &encoded_keys[output_index],
                state.commit_id,
                &state.change_account_id,
            )?
            .map(|entry| entry.value);
        }
    }
    Ok(output)
}

async fn load_compact_replacement_values_encoded(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    encoded_keys: &[Bytes],
    state: &CommitStateManifest,
) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
    if let Some(root) = state.current_state_scoped_ranges.as_ref()
        && let Some(values) =
            load_complete_current_state_values_from_scoped_root(store, root, encoded_keys).await?
    {
        return Ok(values);
    }
    let expanded = expanded_commit_delta_manifest_from_commit_state(store, state).await?;
    load_local_commit_delta_values_encoded(store, commit_id, encoded_keys, &expanded).await
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
    if let Some(parts) = manifest.columnar_parts.as_ref() {
        return Box::pin(load_columnar_mutation_values_encoded(
            store,
            commit_id,
            encoded_keys,
            parts,
        ))
        .await;
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

async fn load_columnar_mutation_values_encoded(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    encoded_keys: &[Bytes],
    parts: &crate::tracked_state::types::ColumnarMutationPartSet,
) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
    use datafusion::arrow::array::{Array, StringArray};

    let id = crate::columnar_row_group::RowGroupSetId::new(parts.row_group_set_id);
    let manifest = crate::columnar_row_group::load_row_group_manifest(store, id)
        .await?
        .ok_or_else(|| replacement_payload_error("columnar mutation manifest is missing"))?;
    validate_columnar_mutation_manifest(&manifest, parts)?;
    let identity_column_index = manifest.fields.len() - 1;
    let identities = encoded_keys
        .iter()
        .map(|encoded| {
            let key = decode_key(encoded)?;
            if key.schema_key != parts.schema_key || key.file_id.is_some() {
                return Ok(None);
            }
            key.row_pk.as_json_array_text().map(Some)
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let mut grouped = BTreeMap::<(usize, usize), Vec<(usize, &str)>>::new();
    for (output_index, identity) in identities.iter().enumerate() {
        let Some(identity) = identity.as_deref() else {
            continue;
        };
        if let Some((group_index, page_index, _global_page)) =
            columnar_mutation_page_for_key(parts, &encoded_keys[output_index])
        {
            grouped
                .entry((group_index, page_index))
                .or_default()
                .push((output_index, identity));
        }
    }
    let mut output = vec![None; encoded_keys.len()];
    for ((group_index, page_index), requests) in grouped {
        let batch = crate::columnar_row_group::load_row_group_page(
            store,
            id,
            &manifest,
            group_index,
            page_index,
            &[identity_column_index],
        )
        .await?;
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| replacement_payload_error("columnar mutation identity type drift"))?;
        if column.null_count() != 0 {
            return Err(replacement_payload_error(
                "columnar mutation identity contains nulls",
            ));
        }
        let row_by_identity = (requests.len() > 8).then(|| {
            (0..column.len())
                .map(|row_index| (column.value(row_index), row_index))
                .collect::<HashMap<_, _>>()
        });
        for (output_index, identity) in requests {
            let row_index = match &row_by_identity {
                Some(rows) => rows.get(identity).copied(),
                None => (0..column.len()).find(|&row_index| column.value(row_index) == identity),
            };
            let Some(row_index) = row_index else {
                continue;
            };
            let global_ordinal = group_index
                .saturating_mul(crate::columnar_row_group::ROW_GROUP_MAX_ROWS)
                .saturating_add(
                    page_index.saturating_mul(crate::columnar_row_group::ROW_GROUP_PAGE_ROWS),
                )
                .saturating_add(row_index);
            let packed = u32::try_from(global_ordinal)
                .map_err(|_| replacement_payload_error("columnar mutation address exceeds u32"))?
                + 1;
            output[output_index] = Some(TrackedStateIndexValue {
                change_id: change_id_from_packed_address(commit_id, packed),
                commit_id,
                deleted: false,
                created_at: parts.uniform_created_at,
                updated_at: parts.uniform_updated_at,
            });
        }
    }
    Ok(output)
}

fn columnar_mutation_page_for_key(
    parts: &crate::tracked_state::types::ColumnarMutationPartSet,
    encoded_key: &[u8],
) -> Option<(usize, usize, usize)> {
    let global_page = parts
        .page_first_keys
        .partition_point(|first| first.as_slice() <= encoded_key)
        .checked_sub(1)?;
    if encoded_key > parts.page_last_keys[global_page].as_slice() {
        return None;
    }
    let pages_per_group = crate::columnar_row_group::ROW_GROUP_MAX_ROWS
        / crate::columnar_row_group::ROW_GROUP_PAGE_ROWS;
    Some((
        global_page / pages_per_group,
        global_page % pages_per_group,
        global_page,
    ))
}

async fn load_columnar_owned_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    keys: &[TrackedStateKeyRef<'_>],
    parts: &crate::tracked_state::types::ColumnarMutationPartSet,
    account_id: &str,
) -> Result<Vec<Option<LoadedCommitDeltaEntry>>, LixError> {
    use datafusion::arrow::array::{Array, StringArray};

    let id = crate::columnar_row_group::RowGroupSetId::new(parts.row_group_set_id);
    let manifest = crate::columnar_row_group::load_row_group_manifest(store, id)
        .await?
        .ok_or_else(|| replacement_payload_error("columnar mutation manifest is missing"))?;
    validate_columnar_mutation_manifest(&manifest, parts)?;
    let identity_column_index = manifest.fields.len() - 1;
    let mut grouped = BTreeMap::<(usize, usize), Vec<(usize, String)>>::new();
    for (output_index, key) in keys.iter().enumerate() {
        if key.schema_key != parts.schema_key || key.file_id.is_some() {
            continue;
        }
        let identity = key.row_pk.as_json_array_text()?;
        let encoded_key = encode_key_ref(*key);
        if let Some((group_index, page_index, _global_page)) =
            columnar_mutation_page_for_key(parts, &encoded_key)
        {
            grouped
                .entry((group_index, page_index))
                .or_default()
                .push((output_index, identity));
        }
    }
    let projection = (0..manifest.fields.len()).collect::<Vec<_>>();
    let mut output = (0..keys.len()).map(|_| None).collect::<Vec<_>>();
    for ((group_index, page_index), requests) in grouped {
        let batch = crate::columnar_row_group::load_row_group_page(
            store,
            id,
            &manifest,
            group_index,
            page_index,
            &projection,
        )
        .await?;
        let identities = batch
            .column(identity_column_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| replacement_payload_error("columnar mutation identity type drift"))?;
        if identities.null_count() != 0 {
            return Err(replacement_payload_error(
                "columnar mutation identity contains nulls",
            ));
        }
        let row_by_identity = (requests.len() > 8).then(|| {
            (0..identities.len())
                .map(|row_index| (identities.value(row_index), row_index))
                .collect::<HashMap<_, _>>()
        });
        for (output_index, identity) in requests {
            let row_index = match &row_by_identity {
                Some(rows) => rows.get(identity.as_str()).copied(),
                None => {
                    (0..identities.len()).find(|&row_index| identities.value(row_index) == identity)
                }
            };
            let Some(row_index) = row_index else {
                continue;
            };
            let row_index_in_group = page_index
                .saturating_mul(crate::columnar_row_group::ROW_GROUP_PAGE_ROWS)
                .saturating_add(row_index);
            let global_ordinal = group_index
                .saturating_mul(crate::columnar_row_group::ROW_GROUP_MAX_ROWS)
                .saturating_add(row_index_in_group);
            let packed = u32::try_from(global_ordinal)
                .map_err(|_| replacement_payload_error("columnar mutation address exceeds u32"))?
                .checked_add(1)
                .ok_or_else(|| replacement_payload_error("columnar mutation address overflows"))?;
            let change_id = change_id_from_packed_address(commit_id, packed);
            // The columnar route establishes identity by JSON text match, not
            // by the byte-equality assert the packed route uses. Counted here
            // so a test can prove which of the two served a row.
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_commit_delta_columnar_row();
            output[output_index] = Some(LoadedCommitDeltaEntry {
                value: TrackedStateIndexValue {
                    change_id,
                    commit_id,
                    deleted: false,
                    created_at: parts.uniform_created_at,
                    updated_at: parts.uniform_updated_at,
                },
                change_record: decode_columnar_change_record(
                    &manifest, &batch, row_index, parts, change_id, account_id,
                )?,
                base_coordinate: Some(TrackedStateBaseCoordinate {
                    base_commit_id: commit_id,
                    group_index: u32::try_from(group_index)
                        .expect("columnar mutation group fits u32"),
                    row_index: u32::try_from(row_index_in_group)
                        .expect("columnar mutation row fits u32"),
                }),
                selected_ref: false,
            });
        }
    }
    Ok(output)
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
    #[cfg(feature = "storage-benches")]
    for key in keys {
        crate::storage_bench::record_commit_delta_request_key_clone(
            key.schema_key.len() + key.file_id.as_ref().map_or(0, String::len),
        );
    }
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

/// Marks one known commit's tracked history as intentionally deferred.
///
/// The marker is staged atomically with the commit header. Its presence keeps
/// a header-only replica from being confused with an authored empty commit.
pub(crate) fn stage_commit_history_deferred(writes: &mut StorageWriteSet, commit_id: CommitId) {
    writes.put(
        TRACKED_STATE_COMMIT_HISTORY_DEFERRED_SPACE,
        StorageKey(Bytes::from(commit_key(commit_id))),
        StorageValue {
            bytes: Bytes::from_static(b"deferred"),
        },
    );
}

/// Marks one commit's tracked history as locally available.
///
/// This deletion is staged in the same write set as the hydrated history, so
/// readers can observe either deferred authority or complete payloads, never
/// a transient header-only empty commit.
pub(crate) fn stage_commit_history_available(writes: &mut StorageWriteSet, commit_id: CommitId) {
    writes.delete(
        TRACKED_STATE_COMMIT_HISTORY_DEFERRED_SPACE,
        StorageKey(Bytes::from(commit_key(commit_id))),
    );
}

pub(crate) async fn commit_history_is_deferred(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<bool, LixError> {
    let result = PointReadPlan::new(
        TRACKED_STATE_COMMIT_HISTORY_DEFERRED_SPACE,
        &[StorageKey(Bytes::from(commit_key(commit_id)))],
    )
    .materialize(
        store,
        StorageGetOptions {
            projection: StorageCoreProjection::KeyOnly,
        },
    )
    .await?;
    Ok(result.value.into_iter().next().flatten().is_some())
}

fn sync_history_required(commit_id: CommitId) -> LixError {
    LixError::new(
        "LIX_SYNC_HISTORY_REQUIRED",
        format!("commit '{commit_id}' requires deferred history payloads"),
    )
    .with_details(serde_json::json!({
        "commitIds": [commit_id.to_string()],
    }))
}

/// Classifies a missing commit-state manifest without confusing intentionally
/// deferred history with local corruption.
pub(crate) async fn missing_commit_state_manifest_error(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> LixError {
    match commit_history_is_deferred(store, commit_id).await {
        Ok(true) => sync_history_required(commit_id),
        Ok(false) => LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("tracked_state commit_state_manifest is missing for commit '{commit_id}'"),
        ),
        Err(error) => error,
    }
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
        load_commit_delta_members_with_payloads_for_schemas(store, commit_id, &[], &[], usize::MAX)
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
    file_ids: &[String],
    max_segment_count: usize,
) -> Result<Option<Vec<CommitDeltaMember>>, LixError> {
    let Some(state) = load_point_replay_commit_state(store, commit_id).await? else {
        if commit_history_is_deferred(store, commit_id).await? {
            return Err(sync_history_required(commit_id));
        }
        return Ok(Some(Vec::new()));
    };
    let Some((local, local_segment_count)) =
        load_authenticated_local_commit_delta_members_for_schemas(
            store,
            &state,
            schema_keys,
            file_ids,
            max_segment_count,
            true,
        )
        .await?
    else {
        return Ok(None);
    };
    let Some(source_commit_id) = state.mutations.selected_source_commit_id() else {
        return Ok(Some(local));
    };
    let source = load_point_replay_commit_state(store, source_commit_id)
        .await?
        .ok_or_else(|| {
            replacement_payload_error(&format!(
                "selected-source commit delta '{commit_id}' references missing source '{source_commit_id}'"
            ))
        })?;
    if source.mutations.selected_source_commit_id().is_some() {
        return Err(replacement_payload_error(
            "selected-source commit delta chains are unsupported",
        ));
    }
    let Some((mut members, _)) = load_authenticated_local_commit_delta_members_for_schemas(
        store,
        &source,
        schema_keys,
        file_ids,
        max_segment_count.saturating_sub(local_segment_count),
        true,
    )
    .await?
    else {
        return Ok(None);
    };
    for member in &mut members {
        member.value.commit_id = commit_id;
        member.authored = false;
        member.selected_tombstone = member.value.deleted;
    }
    Ok(Some(merge_selected_source_members(members, local)))
}

/// Resolves the immutable owners of finite selected members in one commit.
///
/// Whole-source aliases are already named by the authenticated manifest and
/// are deliberately excluded here. Ordinary GC uses this bounded local
/// inventory walk to retain canonical locator owners without scanning any
/// repository-global change or commit space.
pub(crate) async fn load_local_selected_change_owner_commit_ids(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<BTreeSet<CommitId>, LixError> {
    let state = load_point_replay_commit_state(store, commit_id)
        .await?
        .ok_or_else(|| {
            replacement_payload_error(&format!(
                "selected-owner dependency commit '{commit_id}' has no physical authority"
            ))
        })?;
    let Some((members, _)) = load_authenticated_local_commit_delta_members_for_schemas(
        store,
        &state,
        &[],
        &[],
        usize::MAX,
        false,
    )
    .await?
    else {
        unreachable!("unbounded selected-owner inventory cannot exceed its segment limit")
    };
    let selected = members
        .into_iter()
        .filter(|member| !member.authored && !member.selected_tombstone)
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return Ok(BTreeSet::new());
    }

    // Explicit locators are the canonical route for random change IDs. Probe
    // them in one batch before trying the packed direct-address convention;
    // otherwise each random UUID looks like a distinct speculative commit and
    // turns one finite selection into one manifest probe per row.
    let locator_keys = selected
        .iter()
        .map(|member| {
            StorageKey(Bytes::copy_from_slice(
                member.value.change_id.as_uuid().as_bytes(),
            ))
        })
        .collect::<Vec<_>>();
    let locator_values = PointReadPlan::new(TRACKED_STATE_CHANGE_LOCATOR_SPACE, &locator_keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    let explicit_locators = selected
        .iter()
        .zip(locator_values.value)
        .map(|(member, value)| {
            value
                .and_then(full_value_bytes)
                .map(|bytes| decode_change_locator(member.value.change_id, &bytes))
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let direct_locators = selected
        .iter()
        .map(|member| direct_change_locator(member.value.change_id))
        .collect::<Vec<_>>();

    // Direct addressing remains canonical when its physical commit really
    // owns the coordinate. Probe all syntactic candidates in one request so
    // random explicit IDs cannot reintroduce one backend request per row.
    let direct_candidate_ids = direct_locators
        .iter()
        .flatten()
        .map(|locator| locator.commit_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let direct_candidate_manifests =
        load_commit_state_manifests(store, &direct_candidate_ids).await?;
    let present_direct_candidates = direct_candidate_ids
        .into_iter()
        .zip(direct_candidate_manifests)
        .filter_map(|(commit_id, manifest)| manifest.map(|_| commit_id))
        .collect::<BTreeSet<_>>();

    let mut direct_by_commit = BTreeMap::<
        CommitId,
        (
            Arc<AuthenticatedReplayCommitStateManifest>,
            Vec<(usize, CommitDeltaChangeLocator)>,
        ),
    >::new();
    let mut non_owning_direct_candidates = BTreeSet::new();
    let mut explicit_indices = BTreeSet::new();
    for (index, locator) in direct_locators.iter().copied().enumerate() {
        let Some(locator) =
            locator.filter(|locator| present_direct_candidates.contains(&locator.commit_id))
        else {
            explicit_indices.insert(index);
            continue;
        };
        if let Some((_, requests)) = direct_by_commit.get_mut(&locator.commit_id) {
            requests.push((index, locator));
            continue;
        }
        if non_owning_direct_candidates.contains(&locator.commit_id) {
            explicit_indices.insert(index);
            continue;
        }
        match load_direct_change_authority(store, locator.commit_id).await? {
            DirectChangeAuthority::Candidate(authority) => {
                direct_by_commit
                    .entry(locator.commit_id)
                    .or_insert_with(|| (authority, Vec::new()))
                    .1
                    .push((index, locator));
            }
            DirectChangeAuthority::NotOwned(_) => {
                non_owning_direct_candidates.insert(locator.commit_id);
                explicit_indices.insert(index);
            }
        }
    }

    let mut owners = BTreeSet::new();
    for (commit_id, (authority, requests)) in direct_by_commit {
        let locators = requests
            .iter()
            .map(|(_, locator)| *locator)
            .collect::<Vec<_>>();
        let routes = route_direct_change_records_for_state(store, &authority, &locators).await?;
        for ((index, _), route) in requests.into_iter().zip(routes) {
            match route {
                DirectChangeRecordRoute::Owned(record) => {
                    validate_selected_owner_record(&selected[index], &record)?;
                    owners.insert(commit_id);
                }
                DirectChangeRecordRoute::NotOwned(_) => {
                    explicit_indices.insert(index);
                }
            }
        }
    }

    let explicit = explicit_indices
        .into_iter()
        .map(|index| {
            explicit_locators[index]
                .map(|locator| (index, locator))
                .ok_or_else(|| {
                    replacement_payload_error(&format!(
                        "selected change '{}' has no authoritative locator",
                        selected[index].value.change_id
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let locators = explicit
        .iter()
        .map(|(_, locator)| *locator)
        .collect::<Vec<_>>();
    let records = load_explicit_change_records_at_locators(store, &locators).await?;
    for ((index, locator), record) in explicit.into_iter().zip(records) {
        validate_selected_owner_record(&selected[index], &record)?;
        owners.insert(locator.commit_id);
    }
    Ok(owners)
}

fn validate_selected_owner_record(
    member: &CommitDeltaMember,
    record: &crate::changelog::ChangeRecord,
) -> Result<(), LixError> {
    if record.change_id != member.value.change_id
        || record.schema_key != member.key.schema_key
        || record.file_id != member.key.file_id
        || record.row_pk != member.key.row_pk
    {
        return Err(replacement_payload_error(&format!(
            "selected change '{}' references canonical authority for a different identity",
            member.value.change_id
        )));
    }
    Ok(())
}

/// Materializes snapshots for selected schemas from one retained physical
/// commit authority without depending on its rebuildable changelog projection.
/// Rooted commits scan their authenticated snapshot tree; rootless commits use
/// their authenticated bounded mutation delta. Work is O(selected tree rows or
/// selected mutations + JSON payload bytes), with memory bounded by that one
/// retained commit selection.
pub(crate) async fn load_retained_commit_snapshots_for_schemas(
    store: &impl StorageAdapterRead,
    commit_id: CommitId,
    schema_keys: &[String],
) -> Result<Vec<RetainedCommitSnapshot>, LixError> {
    let manifest = load_commit_state_manifest(store, commit_id)
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_STORAGE_ERROR,
                format!("retained tracked-state commit '{commit_id}' has no physical manifest"),
            )
        })?;
    if let Some(snapshot_root) = manifest.snapshot_root.as_ref() {
        let request = TrackedStateTreeScanRequest {
            schema_keys: schema_keys.to_vec(),
            row_pks: Vec::new(),
            file_ids: Vec::new(),
            row_pk_lower: None,
            row_pk_upper: None,
            include_tombstones: true,
            limit: None,
        };
        let entries = crate::tracked_state::tree::TrackedStateTree::new()
            .scan(store, &snapshot_root.root_id, &request)
            .await?;
        let rows = crate::tracked_state::materialize_batch_from_index_entries(
            store,
            entries,
            &ChangeRecordProjection {
                snapshot_content: true,
                metadata: false,
            },
        )
        .await?
        .into_rows();
        return Ok(rows
            .into_iter()
            .map(|row| RetainedCommitSnapshot {
                key: TrackedStateKey {
                    row_pk: row.row_pk,
                    schema_key: row.schema_key,
                    file_id: row.file_id,
                },
                deleted: row.deleted,
                snapshot: row.snapshot_content.map(Into::into),
            })
            .collect());
    }
    let members = load_commit_delta_members_with_payloads_for_schemas(
        store,
        commit_id,
        schema_keys,
        &[],
        usize::MAX,
    )
    .await?
    .expect("unbounded retained snapshot scan cannot exceed its segment limit");
    let json_refs = members
        .iter()
        .filter_map(|member| match &member.change.snapshot {
            crate::json_store::JsonSlot::Ref(json_ref) => Some(*json_ref),
            crate::json_store::JsonSlot::None | crate::json_store::JsonSlot::Inline(_) => None,
        })
        .collect::<Vec<_>>();
    let loaded = crate::json_store::JsonStoreContext::new()
        .reader(store)
        .load_bytes_many(crate::json_store::JsonLoadRequestRef {
            refs: &json_refs,
            scope: crate::json_store::JsonReadScopeRef::OutOfBand,
        })
        .await?
        .into_values();
    let mut loaded = loaded.into_iter();
    members
        .into_iter()
        .map(|member| {
            let snapshot = match member.change.snapshot {
                crate::json_store::JsonSlot::None => None,
                crate::json_store::JsonSlot::Inline(snapshot) => Some(snapshot.into()),
                crate::json_store::JsonSlot::Ref(json_ref) => {
                    let bytes = loaded.next().flatten().ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            format!(
                                "retained commit '{commit_id}' references missing JSON '{}'",
                                json_ref.to_hex()
                            ),
                        )
                    })?;
                    Some(String::from_utf8(bytes.to_vec()).map_err(|_| {
                        LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            format!(
                                "retained commit '{commit_id}' references non-UTF-8 snapshot JSON"
                            ),
                        )
                    })?)
                }
            };
            Ok(RetainedCommitSnapshot {
                key: member.key,
                deleted: member.value.deleted,
                snapshot,
            })
        })
        .collect()
}

async fn load_authenticated_local_commit_delta_members_for_schemas(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    schema_keys: &[String],
    file_ids: &[String],
    max_segment_count: usize,
    hydrate_selected_payloads: bool,
) -> Result<Option<(Vec<CommitDeltaMember>, usize)>, LixError> {
    let Some(root) = state.mutation_directory_root.as_ref() else {
        let manifest = commit_delta_manifest_from_commit_state(state);
        let segment_count = usize::from(manifest.inline_segment().is_some());
        if segment_count > max_segment_count {
            return Ok(None);
        }
        return Ok(Some((
            load_commit_delta_members_from_manifest(
                store,
                state.commit_id,
                &manifest,
                schema_keys,
                hydrate_selected_payloads,
            )
            .await?,
            segment_count,
        )));
    };
    if root.layout == super::mutation_directory::LAYOUT_BOUNDED_DIRECT
        || root.layout == super::mutation_directory::LAYOUT_BOUNDED_INDIRECT
    {
        return load_bounded_commit_delta_members_for_schemas(
            store,
            state,
            schema_keys,
            file_ids,
            max_segment_count,
            hydrate_selected_payloads,
        )
        .await;
    }
    if root.layout == super::mutation_directory::LAYOUT_DIRECT_ROWS_ONLY {
        let manifest = commit_delta_manifest_from_commit_state(state);
        return Ok(Some((
            load_commit_delta_members_from_manifest(
                store,
                state.commit_id,
                &manifest,
                schema_keys,
                hydrate_selected_payloads,
            )
            .await?,
            0,
        )));
    }
    if root.layout != super::mutation_directory::LAYOUT_COMPACT_REPLACEMENT {
        return Err(replacement_payload_error(
            "payload scan encountered an unsupported mutation-directory layout",
        ));
    }
    let runs = super::mutation_directory::load_mutation_part_read_plan(
        store,
        root,
        super::mutation_directory::MutationDirectoryReadSelection::All(
            super::mutation_directory::MutationDirectoryFullTraversalContext::CompactMemberScan,
        ),
    )
    .await?
    .into_runs();
    if runs.len() > max_segment_count {
        return Ok(None);
    }
    let segment_count = runs.len();
    let mut expanded_state = state.manifest.clone();
    for run in runs {
        let super::mutation_directory::MutationDirectoryEntry::CompactReplacement {
            content_digest,
            direct_row_count,
        } = run.entry
        else {
            return Err(replacement_payload_error(
                "compact replacement directory contains a non-compact entry",
            ));
        };
        expanded_state
            .mutations
            .replacement_part_digests
            .push(content_digest);
        expanded_state
            .mutations
            .direct_part_row_counts
            .push(direct_row_count);
    }
    let manifest = expanded_commit_delta_manifest_from_commit_state(store, &expanded_state).await?;
    validate_commit_delta_manifest(&manifest)?;
    Ok(Some((
        load_commit_delta_members_from_manifest(
            store,
            state.commit_id,
            &manifest,
            schema_keys,
            hydrate_selected_payloads,
        )
        .await?,
        segment_count,
    )))
}

/// Builds the commit-delta key ranges a member scan should read.
///
/// The key codec is `schema_key | file_id | row_pk`, so a caller that knows
/// which files it wants can bound the scan on two components instead of one.
///
/// # Why narrowing cannot change the answer
///
/// The only caller that passes a non-empty `file_ids` is the commit-graph
/// history read, and `change_matches_history_request` treats a non-empty
/// `request.file_ids` as requiring a **non-null** `file_id` that is in the set —
/// so every member this range excludes is one that caller discards anyway.
/// Members carrying a null `file_id` are excluded by construction, which is
/// exactly what that post-filter does to them. The retains at the end of the
/// scan still run: a range selects segments, and a segment is decoded whole.
///
/// # Why it matters
///
/// Without the file bound, a point-routed history read decodes every row a
/// bulk commit wrote. Measured on this tree with the per-entry decode census:
/// 10 170 decoded entries for a 20-row answer when one commit touched 5 000
/// files, growing as `2 * bulk_rows`.
///
/// The directory router requires sorted, non-overlapping selectors and never
/// sorts defensively. Distinct `(schema_key, file_id)` prefixes are disjoint
/// because `write_file_id` terminates the file component, and the ranges are
/// sorted by encoded start key here rather than relying on the encoder to
/// preserve `BTreeSet` ordering.
fn bounded_commit_delta_key_ranges(
    requested_schemas: &BTreeSet<&str>,
    requested_files: &BTreeSet<&str>,
) -> Vec<super::mutation_directory::MutationDirectoryKeyRange> {
    fn range_from_prefix(prefix: Vec<u8>) -> super::mutation_directory::MutationDirectoryKeyRange {
        super::mutation_directory::MutationDirectoryKeyRange {
            end: prefix_successor(&prefix).map(Bytes::from),
            start: Bytes::from(prefix),
        }
    }

    let mut ranges = if requested_files.is_empty() {
        requested_schemas
            .iter()
            .map(|schema_key| range_from_prefix(encode_schema_key_prefix(schema_key)))
            .collect::<Vec<_>>()
    } else {
        requested_schemas
            .iter()
            .flat_map(|schema_key| {
                requested_files.iter().map(move |file_id| {
                    range_from_prefix(encode_schema_file_prefix(schema_key, Some(file_id)))
                })
            })
            .collect::<Vec<_>>()
    };
    ranges.sort_by(|left, right| left.start.cmp(&right.start));
    ranges
}

async fn load_bounded_commit_delta_members_for_schemas(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    schema_keys: &[String],
    file_ids: &[String],
    max_segment_count: usize,
    hydrate_selected_payloads: bool,
) -> Result<Option<(Vec<CommitDeltaMember>, usize)>, LixError> {
    let root = state.mutation_directory_root.as_ref().ok_or_else(|| {
        replacement_payload_error("bounded payload scan omitted its mutation-directory root")
    })?;
    let requested_schemas = schema_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let requested_files = file_ids.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let ranges = bounded_commit_delta_key_ranges(&requested_schemas, &requested_files);
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_commit_delta_bounded_scan(
        !requested_files.is_empty(),
        ranges.len(),
    );
    let runs = super::mutation_directory::load_mutation_part_read_plan(
        store,
        root,
        if ranges.is_empty() {
            super::mutation_directory::MutationDirectoryReadSelection::All(
                super::mutation_directory::MutationDirectoryFullTraversalContext::EmptySchemaMemberScan,
            )
        } else {
            super::mutation_directory::MutationDirectoryReadSelection::SortedRanges(&ranges)
        },
    )
    .await?
    .into_runs();
    if runs.len() > max_segment_count {
        return Ok(None);
    }
    let segment_count = runs.len();
    let storage_keys = runs
        .iter()
        .map(|run| {
            let super::mutation_directory::MutationDirectoryEntry::Bounded { part, .. } =
                &run.entry
            else {
                return Err(replacement_payload_error(
                    "bounded payload scan selected a non-bounded part",
                ));
            };
            commit_delta_segment_key_for_part(
                state.commit_id,
                usize::try_from(run.entry_index)
                    .map_err(|_| replacement_payload_error("part index exceeds usize"))?,
                part,
            )
            .map(|key| StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let segments =
        PointReadPlan::from_unique_keys(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, storage_keys)
            .materialize(store, StorageGetOptions::default())
            .await?;
    let mut members = Vec::new();
    for (run, value) in runs.into_iter().zip(segments.value) {
        let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
            replacement_payload_error("bounded payload scan references a missing immutable part")
        })?;
        let super::mutation_directory::MutationDirectoryEntry::Bounded {
            part,
            direct_row_count,
        } = run.entry
        else {
            return Err(replacement_payload_error(
                "bounded payload scan selected a non-bounded part",
            ));
        };
        let bounds = CommitDeltaSegmentBounds {
            first_key: part.first_key,
            last_key: part.last_key,
            replacement_part: part.replacement_part,
        };
        let before = members.len();
        collect_strict_commit_delta_members(
            &bytes,
            Some(&bounds),
            state.commit_id,
            run.entry_index,
            &state.change_account_id,
            &mut members,
        )?;
        validate_bounded_direct_row_count(root.layout, direct_row_count, members.len() - before)?;
    }
    // Both retains survive the range narrowing and neither is redundant with
    // it. A directory range selects *segments*, and a selected segment is
    // decoded whole, so the first and last segment of any range routinely carry
    // entries outside it. Dropping either would be a semantic change wearing a
    // range-narrowing's clothes.
    if !requested_schemas.is_empty() {
        members.retain(|member| requested_schemas.contains(member.key.schema_key.as_str()));
    }
    if !requested_files.is_empty() {
        members.retain(|member| {
            member
                .key
                .file_id
                .as_deref()
                .is_some_and(|file_id| requested_files.contains(file_id))
        });
    }
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_commit_delta_segment_members_kept(members.len());
    if hydrate_selected_payloads {
        hydrate_selected_members(store, &mut members).await?;
    }
    validate_commit_delta_member_order_and_ids(state.commit_id, &members)?;
    Ok(Some((members, segment_count)))
}

fn merge_selected_source_members(
    source: Vec<CommitDeltaMember>,
    local: Vec<CommitDeltaMember>,
) -> Vec<CommitDeltaMember> {
    let mut source = source.into_iter().peekable();
    let mut local = local.into_iter().peekable();
    let mut merged = Vec::with_capacity(source.len().saturating_add(local.len()));
    loop {
        match (source.peek(), local.peek()) {
            (Some(source_member), Some(local_member)) => {
                match source_member.key.cmp(&local_member.key) {
                    std::cmp::Ordering::Less => {
                        merged.push(source.next().expect("peeked source member"));
                    }
                    std::cmp::Ordering::Greater => {
                        merged.push(local.next().expect("peeked local member"));
                    }
                    std::cmp::Ordering::Equal => {
                        source.next();
                        merged.push(local.next().expect("peeked local member"));
                    }
                }
            }
            (Some(_), None) => {
                merged.extend(source);
                break;
            }
            (None, Some(_)) => {
                merged.extend(local);
                break;
            }
            (None, None) => break,
        }
    }
    merged
}

async fn load_commit_delta_members_from_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    manifest: &CommitDeltaManifest,
    schema_keys: &[String],
    hydrate_selected_payloads: bool,
) -> Result<Vec<CommitDeltaMember>, LixError> {
    if let Some(parts) = manifest.columnar_parts.as_ref() {
        if !schema_keys.is_empty() && !schema_keys.iter().any(|schema| schema == &parts.schema_key)
        {
            return Ok(Vec::new());
        }
        return load_columnar_mutation_members(store, commit_id, parts, &manifest.account_id).await;
    }
    let requested_schemas = schema_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut members = Vec::new();
    if let Some(inline_segment) = manifest.inline_segment() {
        collect_strict_commit_delta_members(
            inline_segment,
            None,
            commit_id,
            0,
            &manifest.account_id,
            &mut members,
        )?;
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
                &manifest.account_id,
                &mut members,
            )?;
        }
    }
    if !requested_schemas.is_empty() {
        members.retain(|member| requested_schemas.contains(member.key.schema_key.as_str()));
    }
    if hydrate_selected_payloads {
        hydrate_selected_members(store, &mut members).await?;
    }
    validate_commit_delta_member_order_and_ids(commit_id, &members)?;
    Ok(members)
}

async fn load_columnar_mutation_members(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    parts: &crate::tracked_state::types::ColumnarMutationPartSet,
    account_id: &str,
) -> Result<Vec<CommitDeltaMember>, LixError> {
    let id = crate::columnar_row_group::RowGroupSetId::new(parts.row_group_set_id);
    let manifest = crate::columnar_row_group::load_row_group_manifest(store, id)
        .await?
        .ok_or_else(|| replacement_payload_error("columnar mutation manifest is missing"))?;
    validate_columnar_mutation_manifest(&manifest, parts)?;
    let projection = (0..manifest.fields.len()).collect::<Vec<_>>();
    let mut members = Vec::with_capacity(parts.row_count as usize);
    for group_index in 0..manifest.groups.len() {
        let batch = crate::columnar_row_group::load_row_group_batch(
            store,
            id,
            &manifest,
            group_index,
            &projection,
        )
        .await?;
        for row_index in 0..batch.num_rows() {
            let global_ordinal = members.len();
            let packed = u32::try_from(global_ordinal)
                .map_err(|_| replacement_payload_error("columnar mutation address exceeds u32"))?
                .checked_add(1)
                .ok_or_else(|| replacement_payload_error("columnar mutation address overflows"))?;
            let change_id = change_id_from_packed_address(commit_id, packed);
            let change = decode_columnar_change_record(
                &manifest, &batch, row_index, parts, change_id, account_id,
            )?;
            let key = TrackedStateKey {
                schema_key: parts.schema_key.clone(),
                file_id: None,
                row_pk: change.row_pk.clone(),
            };
            members.push(CommitDeltaMember {
                key,
                value: TrackedStateIndexValue {
                    change_id,
                    commit_id,
                    deleted: false,
                    created_at: parts.uniform_created_at,
                    updated_at: parts.uniform_updated_at,
                },
                change,
                segment_index: u32::try_from(global_ordinal / COMMIT_DELTA_SEGMENT_MAX_ROWS)
                    .expect("columnar mutation segment fits u32"),
                ordinal: u32::try_from(global_ordinal % COMMIT_DELTA_SEGMENT_MAX_ROWS)
                    .expect("columnar mutation ordinal fits u32"),
                authored: true,
                base_coordinate: Some(TrackedStateBaseCoordinate {
                    base_commit_id: commit_id,
                    group_index: u32::try_from(group_index)
                        .expect("columnar mutation group fits u32"),
                    row_index: u32::try_from(row_index).expect("columnar mutation row fits u32"),
                }),
                selected_tombstone: false,
            });
        }
    }
    if members.len() != parts.row_count as usize {
        return Err(replacement_payload_error(
            "columnar mutation rows disagree with commit authority",
        ));
    }
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
            || change_record.row_pk != member.key.row_pk
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
    let batch = {
        #[cfg(feature = "storage-benches")]
        let _phase = crate::storage_bench::PlanLoadPhaseScope::enter(
            crate::storage_bench::PlanLoadPhase::DeltaSegments,
        );
        scan_commit_delta_values(store, commit_id, &[]).await?
    };
    #[cfg(feature = "storage-benches")]
    let _phase = crate::storage_bench::PlanLoadPhaseScope::enter(
        crate::storage_bench::PlanLoadPhase::Collect,
    );
    let mut members = Vec::with_capacity(batch.len());
    for row in batch.iter() {
        let key = row.key_ref();
        let key = TrackedStateKey {
            schema_key: key.schema_key.to_owned(),
            file_id: key.file_id.map(str::to_owned),
            row_pk: key.row_pk.clone(),
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
/// Arbitrary caller order is canonicalized once per physical owner. Bounded
/// authorities retain point routing for negative lookups; selected-source
/// misses are resolved from the authenticated source without flattening
/// either directory.
pub(crate) async fn load_owned_commit_delta_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    requests: &[(CommitId, TrackedStateKey)],
) -> Result<Vec<Option<LoadedCommitDeltaEntry>>, LixError> {
    let point_cache = CommitDeltaPointReadCache::default();
    let mut output =
        load_local_owned_commit_delta_entries(store, requests, Some(&point_cache)).await?;
    let mut source_requests = Vec::new();
    let mut source_outputs = Vec::new();
    let mut source_owner_commits = Vec::new();
    for (request_index, (commit_id, key)) in requests.iter().enumerate() {
        if output[request_index].is_some() {
            continue;
        }
        let Some(state) = point_cache.authority(*commit_id)? else {
            continue;
        };
        let Some(source_commit_id) = state.mutations.selected_source_commit_id() else {
            continue;
        };
        source_outputs.push(request_index);
        source_owner_commits.push(*commit_id);
        source_requests.push((source_commit_id, key.clone()));
    }
    let selected =
        load_local_owned_commit_delta_entries(store, &source_requests, Some(&point_cache)).await?;
    for source_commit_id in source_requests
        .iter()
        .map(|(source_commit_id, _)| *source_commit_id)
        .collect::<BTreeSet<_>>()
    {
        let source = point_cache.authority(source_commit_id)?.ok_or_else(|| {
            replacement_payload_error(
                "selected-source mutation authority references a missing source",
            )
        })?;
        if source.mutations.selected_source_commit_id().is_some() {
            return Err(replacement_payload_error(
                "selected-source mutation authority cannot alias another source",
            ));
        }
    }
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
/// resolve every key from the physical owner. A local negative is final unless
/// the same authenticated authority names a selected source; only that case
/// enters the source-overlay loader.
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
        (pair[0].schema_key, pair[0].file_id, pair[0].row_pk)
            < (pair[1].schema_key, pair[1].file_id, pair[1].row_pk)
    });
    let local_cache = CommitDeltaPointReadCache::default();
    let point_cache = point_cache.unwrap_or(&local_cache);
    if strictly_ordered {
        let output = load_local_owned_commit_delta_entries_one_ordered(
            store,
            commit_id,
            keys,
            Some(point_cache),
        )
        .await?;
        if output.iter().all(Option::is_some)
            || point_cache
                .authority(commit_id)?
                .is_none_or(|state| state.mutations.selected_source_commit_id().is_none())
        {
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
                    row_pk: key.row_pk.clone(),
                },
            )
        })
        .collect::<Vec<_>>();
    load_owned_commit_delta_entries(store, &requests).await
}

async fn load_local_owned_commit_delta_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    requests: &[(CommitId, TrackedStateKey)],
    point_cache: Option<&CommitDeltaPointReadCache>,
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
                row_pk: &key.row_pk,
            })
            .collect::<Vec<_>>();
        return Box::pin(load_local_owned_commit_delta_entries_one_ordered(
            store,
            requests[0].0,
            &keys,
            point_cache,
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

    let mut output = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
    for (commit_id, mut request_indices) in request_indices_by_commit {
        request_indices.sort_unstable_by(|&left, &right| requests[left].1.cmp(&requests[right].1));
        let mut unique_request_indices = Vec::<usize>::with_capacity(request_indices.len());
        let mut output_routes = Vec::with_capacity(request_indices.len());
        for request_index in request_indices {
            let unique_index = if unique_request_indices
                .last()
                .is_some_and(|&previous_index| {
                    requests[previous_index].1 == requests[request_index].1
                }) {
                unique_request_indices.len() - 1
            } else {
                unique_request_indices.push(request_index);
                unique_request_indices.len() - 1
            };
            output_routes.push((request_index, unique_index));
        }
        let keys = unique_request_indices
            .iter()
            .map(|&request_index| {
                let key = &requests[request_index].1;
                TrackedStateKeyRef {
                    schema_key: &key.schema_key,
                    file_id: key.file_id.as_deref(),
                    row_pk: &key.row_pk,
                }
            })
            .collect::<Vec<_>>();
        let unique = Box::pin(load_local_owned_commit_delta_entries_one_ordered(
            store,
            commit_id,
            &keys,
            point_cache,
        ))
        .await?;
        for (request_index, unique_index) in output_routes {
            output[request_index] = unique[unique_index].clone();
        }
    }
    Ok(output)
}

async fn load_inventory_part_entries_one_ordered(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    keys: &[TrackedStateKeyRef<'_>],
    state: &AuthenticatedReplayCommitStateManifest,
) -> Result<Vec<Option<LoadedCommitDeltaEntry>>, LixError> {
    let root = state.mutation_directory_root.as_ref().ok_or_else(|| {
        replacement_payload_error("ordered mutation inventory omitted its directory root")
    })?;
    let mut encoded_keys = TrackedStateKeyBatchBuilder::with_row_capacity(keys.len());
    for &key in keys {
        encoded_keys.push(key);
    }
    let encoded_keys = encoded_keys.finish();
    let runs = super::mutation_directory::load_mutation_part_read_plan(
        store,
        root,
        super::mutation_directory::MutationDirectoryReadSelection::SortedUniquePoints(
            &encoded_keys,
        ),
    )
    .await?
    .into_runs();
    let storage_keys = runs
        .iter()
        .map(|run| {
            let super::mutation_directory::MutationDirectoryEntry::Bounded { part, .. } =
                &run.entry
            else {
                return Err(replacement_payload_error(
                    "ordered mutation inventory selected a non-bounded part",
                ));
            };
            commit_delta_segment_key_for_part(
                commit_id,
                usize::try_from(run.entry_index)
                    .map_err(|_| replacement_payload_error("part index exceeds usize"))?,
                part,
            )
            .map(|key| StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let loaded =
        PointReadPlan::from_unique_keys(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, storage_keys)
            .materialize(store, StorageGetOptions::default())
            .await?;
    let mut output = (0..keys.len()).map(|_| None).collect::<Vec<_>>();
    for (run, value) in runs.into_iter().zip(loaded.value) {
        let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
            replacement_payload_error("mutation inventory references a missing immutable part")
        })?;
        let super::mutation_directory::MutationDirectoryEntry::Bounded {
            part,
            direct_row_count,
        } = run.entry
        else {
            return Err(replacement_payload_error(
                "ordered mutation inventory selected a non-bounded part",
            ));
        };
        let bounds = CommitDeltaSegmentBounds {
            first_key: part.first_key,
            last_key: part.last_key,
            replacement_part: part.replacement_part,
        };
        let (leaf, payloads) = decode_commit_delta_with_payloads(&bytes, Some(&bounds))?;
        validate_bounded_direct_row_count(root.layout, direct_row_count, leaf.len())?;
        for output_index in run.selector_span {
            output[output_index] = find_loaded_commit_delta_entry(
                &leaf,
                &payloads,
                &encoded_keys[output_index],
                commit_id,
                &state.change_account_id,
            )?;
        }
    }
    hydrate_selected_loaded_entries(store, &mut output).await?;
    Ok(output)
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
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_commit_delta_ordered_load(keys.len());
    let cached_state = point_cache
        .map(|cache| cache.authority(commit_id))
        .transpose()?
        .flatten();
    let state = match cached_state {
        Some(state) => state,
        None => {
            let Some(state) = load_point_replay_commit_state(store, commit_id).await? else {
                return Ok((0..keys.len()).map(|_| None).collect());
            };
            if let Some(point_cache) = point_cache {
                point_cache.remember_authority(Arc::clone(&state))?;
            }
            state
        }
    };
    if state.mutations.inline_part.is_empty()
        && state.mutation_directory_root.as_ref().is_some_and(|root| {
            root.layout == super::mutation_directory::LAYOUT_BOUNDED_DIRECT
                || root.layout == super::mutation_directory::LAYOUT_BOUNDED_INDIRECT
        })
    {
        return load_inventory_part_entries_one_ordered(store, commit_id, keys, &state).await;
    }
    let manifest = match point_cache
        .map(|cache| cache.manifest(commit_id))
        .transpose()?
        .flatten()
    {
        Some(manifest) => PointReadCommitDeltaManifest::Cached(manifest),
        None => {
            let full_state = if state.mutation_directory_root.is_some() {
                load_commit_state_manifest(store, commit_id)
                    .await?
                    .ok_or_else(|| {
                        replacement_payload_error(
                            "point replay lost its full mutation-directory authority",
                        )
                    })?
            } else {
                state.manifest.clone()
            };
            let manifest =
                expanded_commit_delta_manifest_from_commit_state(store, &full_state).await?;
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
    if let Some(parts) = manifest.columnar_parts.as_ref() {
        return Box::pin(load_columnar_owned_entries(
            store,
            commit_id,
            keys,
            parts,
            &manifest.account_id,
        ))
        .await;
    }
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
                                &manifest.account_id,
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
                    &manifest.account_id,
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
                        &manifest.account_id,
                    )?;
                }
                hydrate_selected_loaded_entries(store, &mut output).await?;
                return Ok(output);
            }
            let (leaf, payloads) = decode_commit_delta_with_payloads(inline_segment, None)?;
            for (request_index, &key) in keys.iter().enumerate() {
                let encoded_key = encode_key_ref(key);
                output[request_index] = find_loaded_commit_delta_entry(
                    &leaf,
                    &payloads,
                    &encoded_key,
                    commit_id,
                    &manifest.account_id,
                )?;
            }
            hydrate_selected_loaded_entries(store, &mut output).await?;
            return Ok(output);
        }
        let (leaf, payloads) = decode_commit_delta_with_payloads(inline_segment, None)?;
        for (request_index, &key) in keys.iter().enumerate() {
            let encoded_key = encode_key_ref(key);
            output[request_index] = find_loaded_commit_delta_entry(
                &leaf,
                &payloads,
                &encoded_key,
                commit_id,
                &manifest.account_id,
            )?;
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
                                &manifest.account_id,
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
                    &manifest.account_id,
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
                        &manifest.account_id,
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
                    &manifest.account_id,
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
                && leaf.key(leaf_index).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state packed commit_delta leaf has a missing key",
                    )
                })? < encoded_key
            {
                leaf_index += 1;
            }
            let Some(leaf_key) = leaf.key(leaf_index) else {
                continue;
            };
            if leaf_key == encoded_key {
                output[request_index] = Some(load_commit_delta_entry_at_index(
                    &leaf,
                    &payloads,
                    leaf_index,
                    commit_id,
                    &manifest.account_id,
                )?);
                leaf_index += 1;
            }
        }
    }
    hydrate_selected_loaded_entries(store, &mut output).await?;
    Ok(output)
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
            || entry.change_record.row_pk != change_record.row_pk
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
    let state = {
        #[cfg(feature = "storage-benches")]
        let _phase = crate::storage_bench::PlanLoadPhaseScope::enter(
            crate::storage_bench::PlanLoadPhase::ReplayState,
        );
        load_point_replay_commit_state(store, commit_id).await?
    };
    let Some(state) = state else {
        return Ok(DecodedCommitDeltaBatch::default());
    };
    let source = match state.mutations.selected_source_commit_id() {
        Some(source_commit_id) => Some(
            load_point_replay_commit_state(store, source_commit_id)
                .await?
                .ok_or_else(|| {
                    replacement_payload_error(&format!(
                        "selected-source commit '{}' references missing authority '{source_commit_id}'",
                        state.commit_id
                    ))
                })?,
        ),
        None => None,
    };
    scan_commit_delta_values_from_authenticated_states(
        store,
        &state,
        source.as_deref(),
        schema_keys,
    )
    .await
}

/// Scans from immutable authority already authenticated in this reader
/// snapshot. Directory nodes and parts remain content- and bound-checked by
/// the local scan; this only avoids reloading the same header and inventory.
pub(crate) async fn scan_commit_delta_values_from_authenticated_states(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    source: Option<&AuthenticatedReplayCommitStateManifest>,
    schema_keys: &[String],
) -> Result<DecodedCommitDeltaBatch, LixError> {
    let selected_source_commit_id = state.mutations.selected_source_commit_id();
    match (selected_source_commit_id, source) {
        (None, None) => {
            return Box::pin(scan_authenticated_local_commit_delta_values(
                store,
                state,
                schema_keys,
            ))
            .await;
        }
        (None, Some(_)) => {
            return Err(replacement_payload_error(
                "authenticated scan received a source for a local-only authority",
            ));
        }
        (Some(source_commit_id), None) => {
            return Err(replacement_payload_error(&format!(
                "selected-source commit '{}' references missing authority '{source_commit_id}'",
                state.commit_id
            )));
        }
        (Some(source_commit_id), Some(source)) if source.commit_id != source_commit_id => {
            return Err(replacement_payload_error(&format!(
                "selected-source commit '{}' expected authority '{source_commit_id}' but received '{}'",
                state.commit_id, source.commit_id
            )));
        }
        (Some(_), Some(source)) if source.mutations.selected_source_commit_id().is_some() => {
            return Err(replacement_payload_error(
                "selected-source mutation authority cannot alias another source",
            ));
        }
        (Some(_), Some(_)) => {}
    }
    let source = Box::pin(scan_authenticated_local_commit_delta_values(
        store,
        source.expect("validated selected source"),
        schema_keys,
    ))
    .await?;
    let local = Box::pin(scan_authenticated_local_commit_delta_values(
        store,
        state,
        schema_keys,
    ))
    .await?;
    merge_selected_source_batches(source, local, state.commit_id)
}

async fn scan_authenticated_local_commit_delta_values(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    schema_keys: &[String],
) -> Result<DecodedCommitDeltaBatch, LixError> {
    let Some(root) = state.mutation_directory_root.as_ref() else {
        let manifest = commit_delta_manifest_from_commit_state(state);
        return Box::pin(scan_local_commit_delta_values(
            store,
            state.commit_id,
            schema_keys,
            &manifest,
        ))
        .await;
    };
    if root.layout == super::mutation_directory::LAYOUT_BOUNDED_DIRECT
        || root.layout == super::mutation_directory::LAYOUT_BOUNDED_INDIRECT
    {
        return Box::pin(scan_bounded_commit_delta_values(store, state, schema_keys)).await;
    }
    if root.layout == super::mutation_directory::LAYOUT_DIRECT_ROWS_ONLY {
        let manifest = commit_delta_manifest_from_commit_state(state);
        return Box::pin(scan_local_commit_delta_values(
            store,
            state.commit_id,
            schema_keys,
            &manifest,
        ))
        .await;
    }
    if root.layout != super::mutation_directory::LAYOUT_COMPACT_REPLACEMENT {
        return Err(replacement_payload_error(
            "mutation scan encountered an unsupported directory layout",
        ));
    }

    // Compact replacement parts have ordinal identities rather than key
    // bounds, so a schema scan must authenticate their complete digest list.
    // This is a layout-specific traversal, not a general manifest expansion.
    let entries = super::mutation_directory::load_mutation_part_read_plan(
        store,
        root,
        super::mutation_directory::MutationDirectoryReadSelection::All(
            super::mutation_directory::MutationDirectoryFullTraversalContext::CompactValueScan,
        ),
    )
    .await?
    .into_runs();
    let mut expanded_state = state.manifest.clone();
    for run in entries {
        let super::mutation_directory::MutationDirectoryEntry::CompactReplacement {
            content_digest,
            direct_row_count,
        } = run.entry
        else {
            return Err(replacement_payload_error(
                "compact replacement directory contains a bounded entry",
            ));
        };
        expanded_state
            .mutations
            .replacement_part_digests
            .push(content_digest);
        expanded_state
            .mutations
            .direct_part_row_counts
            .push(direct_row_count);
    }
    let manifest = expanded_commit_delta_manifest_from_commit_state(store, &expanded_state).await?;
    validate_commit_delta_manifest(&manifest)?;
    Box::pin(scan_local_commit_delta_values(
        store,
        state.commit_id,
        schema_keys,
        &manifest,
    ))
    .await
}

async fn scan_bounded_commit_delta_values(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &AuthenticatedReplayCommitStateManifest,
    schema_keys: &[String],
) -> Result<DecodedCommitDeltaBatch, LixError> {
    let root = state.mutation_directory_root.as_ref().ok_or_else(|| {
        replacement_payload_error("bounded mutation scan omitted its directory root")
    })?;
    // This API is the canonicalization boundary above the strict directory
    // router. The router never sorts or deduplicates selectors defensively.
    let requested_schemas = schema_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let ranges = requested_schemas
        .iter()
        .map(|schema_key| {
            let start = encode_schema_key_prefix(schema_key);
            let end = prefix_successor(&start);
            super::mutation_directory::MutationDirectoryKeyRange {
                start: Bytes::from(start),
                end: end.map(Bytes::from),
            }
        })
        .collect::<Vec<_>>();
    let runs = super::mutation_directory::load_mutation_part_read_plan(
        store,
        root,
        if ranges.is_empty() {
            super::mutation_directory::MutationDirectoryReadSelection::All(
                super::mutation_directory::MutationDirectoryFullTraversalContext::EmptySchemaValueScan,
            )
        } else {
            super::mutation_directory::MutationDirectoryReadSelection::SortedRanges(&ranges)
        },
    )
    .await?
    .into_runs();
    if runs.is_empty() {
        return Ok(DecodedCommitDeltaBatch::default());
    }
    let storage_keys = runs
        .iter()
        .map(|run| {
            let super::mutation_directory::MutationDirectoryEntry::Bounded { part, .. } =
                &run.entry
            else {
                return Err(replacement_payload_error(
                    "bounded mutation scan selected a non-bounded part",
                ));
            };
            commit_delta_segment_key_for_part(
                state.commit_id,
                usize::try_from(run.entry_index)
                    .map_err(|_| replacement_payload_error("part index exceeds usize"))?,
                part,
            )
            .map(|key| StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let segments =
        PointReadPlan::from_unique_keys(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, storage_keys)
            .materialize(store, StorageGetOptions::default())
            .await?;
    let mut batch = DecodedCommitDeltaBatchBuilder::with_capacity(
        runs.len().saturating_mul(COMMIT_DELTA_SEGMENT_MAX_ROWS),
        runs.len(),
    );
    for (run, value) in runs.into_iter().zip(segments.value) {
        let bytes = value.and_then(full_value_bytes).ok_or_else(|| {
            replacement_payload_error("bounded mutation scan references a missing immutable part")
        })?;
        let super::mutation_directory::MutationDirectoryEntry::Bounded {
            part,
            direct_row_count,
        } = run.entry
        else {
            return Err(replacement_payload_error(
                "bounded mutation scan selected a non-bounded part",
            ));
        };
        let bounds = CommitDeltaSegmentBounds {
            first_key: part.first_key,
            last_key: part.last_key,
            replacement_part: part.replacement_part,
        };
        let leaf = decode_commit_delta_leaf(&bytes, Some(&bounds))?;
        validate_bounded_direct_row_count(root.layout, direct_row_count, leaf.len())?;
        batch.push_leaf(leaf, state.commit_id, &requested_schemas)?;
    }
    Ok(batch.finish())
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
    if let Some(parts) = manifest.columnar_parts.as_ref() {
        if !requested_schemas.is_empty() && !requested_schemas.contains(parts.schema_key.as_str()) {
            return Ok(DecodedCommitDeltaBatch::default());
        }
        return scan_columnar_mutation_values(store, commit_id, parts).await;
    }
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

async fn scan_columnar_mutation_values(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    parts: &crate::tracked_state::types::ColumnarMutationPartSet,
) -> Result<DecodedCommitDeltaBatch, LixError> {
    use datafusion::arrow::array::{Array, StringArray};

    let id = crate::columnar_row_group::RowGroupSetId::new(parts.row_group_set_id);
    let manifest = crate::columnar_row_group::load_row_group_manifest(store, id)
        .await?
        .ok_or_else(|| replacement_payload_error("columnar mutation manifest is missing"))?;
    validate_columnar_mutation_manifest(&manifest, parts)?;
    let identity_column_index = manifest.fields.len() - 1;
    let mut builder = DecodedCommitDeltaBatchBuilder::with_capacity(
        parts.row_count as usize,
        (parts.row_count as usize).div_ceil(COMMIT_DELTA_SEGMENT_MAX_ROWS),
    );
    let mut global_ordinal = 0usize;
    for group_index in 0..manifest.groups.len() {
        let batch = crate::columnar_row_group::load_row_group_batch(
            store,
            id,
            &manifest,
            group_index,
            &[identity_column_index],
        )
        .await?;
        let identities = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| replacement_payload_error("columnar mutation identity type drift"))?;
        for row_index in 0..identities.len() {
            let row_pk = RowPk::from_json_array_text(identities.value(row_index))
                .map_err(|error| replacement_payload_error(&error.to_string()))?;
            let packed = u32::try_from(global_ordinal)
                .map_err(|_| replacement_payload_error("columnar mutation address exceeds u32"))?
                .checked_add(1)
                .ok_or_else(|| replacement_payload_error("columnar mutation address overflows"))?;
            builder.push_columnar_row(
                &parts.schema_key,
                row_pk,
                TrackedStateIndexValue {
                    change_id: change_id_from_packed_address(commit_id, packed),
                    commit_id,
                    deleted: false,
                    created_at: parts.uniform_created_at,
                    updated_at: parts.uniform_updated_at,
                },
            )?;
            global_ordinal += 1;
        }
    }
    if global_ordinal != parts.row_count as usize {
        return Err(replacement_payload_error(
            "columnar mutation rows disagree with commit authority",
        ));
    }
    Ok(builder.finish())
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
    let columnar_key_offset = source.columnar_keys.len();
    let mut entries = BTreeMap::<Vec<u8>, (DecodedCommitDeltaRow, TrackedStateIndexValue)>::new();
    let source_rows = std::mem::take(&mut source.rows);
    let source_values = std::mem::take(&mut source.values);
    for (row, mut value) in source_rows.into_iter().zip(source_values) {
        let key = decoded_commit_delta_row_key(&source, &row)?.to_vec();
        value.commit_id = commit_id;
        entries.insert(key, (row, value));
    }
    let local_rows = std::mem::take(&mut local.rows);
    let local_values = std::mem::take(&mut local.values);
    for (mut row, value) in local_rows.into_iter().zip(local_values) {
        let key = decoded_commit_delta_row_key(&local, &row)?.to_vec();
        if let Some(range) = row.columnar_key {
            let shifted_offset =
                range
                    .offset()
                    .checked_add(columnar_key_offset)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "commit delta columnar key offset overflow",
                        )
                    })?;
            row.columnar_key = Some(BufferRange::new(shifted_offset, range.len()));
        } else {
            row.arena_ordinal = row.arena_ordinal.checked_add(arena_offset).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "commit delta arena ordinal overflow",
                )
            })?;
        }
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
    let mut columnar_keys = Vec::with_capacity(
        source
            .columnar_keys
            .len()
            .saturating_add(local.columnar_keys.len()),
    );
    columnar_keys.extend_from_slice(&source.columnar_keys);
    columnar_keys.extend_from_slice(&local.columnar_keys);
    source.columnar_keys = Bytes::from(columnar_keys);
    source.arenas.append(&mut local.arenas);
    source.schema_keys.append(&mut local.schema_keys);
    source.file_ids.append(&mut local.file_ids);
    for (_, (row, value)) in entries {
        source.rows.push(row);
        source.values.push(value);
    }
    Ok(source)
}

fn decoded_commit_delta_row_key<'a>(
    batch: &'a DecodedCommitDeltaBatch,
    row: &DecodedCommitDeltaRow,
) -> Result<&'a [u8], LixError> {
    if let Some(range) = row.columnar_key {
        return batch
            .columnar_keys
            .get(range.offset()..range.offset().saturating_add(range.len()))
            .ok_or_else(|| replacement_payload_error("columnar mutation key range is invalid"));
    }
    batch
        .arenas
        .get(row.arena_ordinal as usize)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "selected-source commit delta references a missing arena",
            )
        })?
        .key(row.entry_ordinal as usize)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "selected-source commit delta references a missing key",
            )
        })
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
    Ok(
        scan_commit_delta_values(store, commit_id, &[schema_key.to_owned()])
            .await?
            .len()
            != 0,
    )
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
    visit: impl FnMut(crate::changelog::ChangeRecord) -> Result<(), LixError>,
) -> Result<usize, LixError> {
    let mut visit = visit;
    let range = StorageKeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    };
    let mut emitted = 0usize;
    let mut cursor = store
        .begin_scan(
            TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let (page, page_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        for entry_batch in page.chunks(COMMIT_STATE_SCAN_AUTHORITY_BATCH_ROWS) {
            let commit_ids = entry_batch
                .iter()
                .map(|entry| commit_id_from_delta_key(&entry.key))
                .collect::<Result<Vec<_>, _>>()?;
            let states = load_commit_state_manifests(store, &commit_ids).await?;
            for (entry, (commit_id, state)) in
                entry_batch.iter().zip(commit_ids.into_iter().zip(states))
            {
                if entry.key.0.len() != 16 {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state commit_delta manifest key is not a 16-byte commit id",
                    ));
                }
                let StorageProjectedValue::FullValue(_) = &entry.value else {
                    unreachable!("full commit-delta scan returned a key-only row");
                };
                let state = state.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state commit-state scan lost its split authority",
                    )
                })?;
                if state.commit_id != commit_id {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state commit-state scan key and manifest commit disagree",
                    ));
                }
                let manifest =
                    expanded_commit_delta_manifest_from_commit_state(store, &state).await?;
                if let Some(parts) = manifest.columnar_parts.as_ref() {
                    emitted = emitted.saturating_add(
                        visit_columnar_mutation_change_records(
                            store,
                            commit_id,
                            parts,
                            &manifest.account_id,
                            &mut visit,
                        )
                        .await?,
                    );
                    continue;
                }
                let members =
                    load_commit_delta_members_from_manifest(store, commit_id, &manifest, &[], true)
                        .await?;
                for member in members {
                    if member.authored {
                        visit(member.change)?;
                        emitted += 1;
                    } else if is_payload_free_selected_tombstone(&member) {
                        // Cascade tombstones preserve identity history but do
                        // not introduce another public changelog fact.
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
                        let canonical =
                            load_change_records_by_ids(store, &[member.change.change_id])
                                .await?
                                .pop()
                                .ok_or_else(|| {
                                    invalid_change_locator(
                                        member.change.change_id,
                                        "does not resolve to a canonical record",
                                    )
                                })?;
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
        }
        if !page_has_more {
            break;
        }
    }
    validate_no_orphan_commit_delta_segments(store).await?;
    Ok(emitted)
}

async fn visit_columnar_mutation_change_records(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    parts: &crate::tracked_state::types::ColumnarMutationPartSet,
    account_id: &str,
    visit: &mut impl FnMut(crate::changelog::ChangeRecord) -> Result<(), LixError>,
) -> Result<usize, LixError> {
    let id = crate::columnar_row_group::RowGroupSetId::new(parts.row_group_set_id);
    let manifest = crate::columnar_row_group::load_row_group_manifest(store, id)
        .await?
        .ok_or_else(|| replacement_payload_error("columnar mutation manifest is missing"))?;
    validate_columnar_mutation_manifest(&manifest, parts)?;
    let projection = (0..manifest.fields.len()).collect::<Vec<_>>();
    let mut global_ordinal = 0usize;
    for (group_index, group) in manifest.groups.iter().enumerate() {
        let page_count =
            (group.row_count as usize).div_ceil(crate::columnar_row_group::ROW_GROUP_PAGE_ROWS);
        for page_index in 0..page_count {
            let batch = crate::columnar_row_group::load_row_group_page(
                store,
                id,
                &manifest,
                group_index,
                page_index,
                &projection,
            )
            .await?;
            for row_index in 0..batch.num_rows() {
                let packed = u32::try_from(global_ordinal)
                    .map_err(|_| {
                        replacement_payload_error("columnar mutation address exceeds u32")
                    })?
                    .checked_add(1)
                    .ok_or_else(|| {
                        replacement_payload_error("columnar mutation address overflows")
                    })?;
                let change_id = change_id_from_packed_address(commit_id, packed);
                visit(decode_columnar_change_record(
                    &manifest, &batch, row_index, parts, change_id, account_id,
                )?)?;
                global_ordinal += 1;
            }
        }
    }
    if global_ordinal != parts.row_count as usize {
        return Err(replacement_payload_error(
            "columnar mutation rows disagree with commit authority",
        ));
    }
    Ok(global_ordinal)
}

async fn validate_no_orphan_commit_delta_segments(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<(), LixError> {
    let range = StorageKeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    };
    let mut cursor = store
        .begin_scan(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let (page, page_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        if page.is_empty() {
            break;
        }
        let mut commit_ids = Vec::new();
        for entry in &page {
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
        for entry in &page {
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
        if !page_has_more {
            break;
        }
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
        mut segments,
        mut segment_keys,
    } = scan_commit_delta_plane(store).await?;
    let mut inventory = CommitDeltaInventory::default();
    for (&commit_id, manifest) in &manifests {
        let physical_segments = segments.remove(&commit_id).unwrap_or_default();
        let physical_segment_keys = segment_keys.remove(&commit_id).unwrap_or_default();
        let mut members = Vec::new();
        let segment_count = if let Some(parts) = manifest.columnar_parts.as_ref() {
            if !physical_segments.is_empty() {
                return Err(replacement_payload_error(
                    "columnar mutation inventory has legacy external segments",
                ));
            }
            members = load_columnar_mutation_members(store, commit_id, parts, &manifest.account_id)
                .await?;
            parts.group_row_counts.len()
        } else if let Some(inline_segment) = manifest.inline_segment() {
            if !physical_segments.is_empty() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state inline commit_delta for commit '{commit_id}' has external segments"
                    ),
                ));
            }
            collect_strict_commit_delta_members(
                inline_segment,
                None,
                commit_id,
                0,
                &manifest.account_id,
                &mut members,
            )?;
            1
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
                    &manifest.account_id,
                    &mut members,
                )?;
            }
            manifest.segments.len()
        };
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
    Ok(inventory)
}

async fn scan_commit_delta_plane(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<CommitDeltaPlane, LixError> {
    let commit_state_rows =
        scan_full_space(store, TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE).await?;
    let mutation_inventory_rows =
        scan_full_space(store, TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE).await?;
    let segment_rows = scan_full_space(store, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE).await?;

    let header_keys = commit_state_rows
        .iter()
        .map(|(key, _)| key)
        .collect::<BTreeSet<_>>();
    let inventory_keys = mutation_inventory_rows
        .iter()
        .map(|(key, _)| key)
        .collect::<BTreeSet<_>>();
    if header_keys != inventory_keys {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state split commit authority has orphaned headers or mutation inventories",
        ));
    }

    let mut inventory_rows = mutation_inventory_rows
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let mut authorities = Vec::with_capacity(commit_state_rows.len());
    for (key, header) in commit_state_rows {
        if key.0.len() != 16 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_state_manifest key is not a 16-byte commit id",
            ));
        }
        let commit_id = commit_id_from_delta_key(&key)?;
        let inventory = inventory_rows.remove(&key).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit-state inventory lost its split authority",
            )
        })?;
        let (stored, stored_inventory) = decode_stored_commit_state_authority(&header, &inventory)?;
        if stored.commit_id != commit_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit-state inventory found duplicate or mismatched manifest for commit '{commit_id}'"
                ),
            ));
        }
        authorities.push((commit_id, stored, stored_inventory));
    }
    debug_assert!(inventory_rows.is_empty());
    let roots = authorities
        .iter()
        .filter_map(|(_, _, inventory)| inventory.directory_root.clone())
        .collect::<Vec<_>>();
    let mut directories = super::mutation_directory::load_all_mutation_part_read_plans(
        store,
        &roots,
        super::mutation_directory::MutationDirectoryFullTraversalContext::RepositoryInventory,
    )
    .await?
    .into_iter();
    let mut manifests = BTreeMap::<CommitId, CommitDeltaManifest>::new();
    for (commit_id, stored, inventory) in authorities {
        let entries = if inventory.directory_root.is_some() {
            directories
                .next()
                .expect("each scanned mutation root returns one directory")
                .into_runs()
                .into_iter()
                .map(|run| run.entry)
                .collect()
        } else {
            Vec::new()
        };
        let manifest = assemble_commit_state_manifest(stored, inventory, entries, true)?;
        if manifests.contains_key(&commit_id) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit-state inventory found duplicate manifest for commit '{commit_id}'"
                ),
            ));
        }
        manifests.insert(
            commit_id,
            expanded_commit_delta_manifest_from_commit_state(store, &manifest).await?,
        );
    }
    debug_assert!(directories.next().is_none());

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

#[cfg(test)]
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
        TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
        key(commit_mutation_inventory_key(commit_id)),
    );
    for segment_key in &entry.physical_segment_keys {
        writes.delete(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            key(segment_key.clone()),
        );
    }
    Ok(())
}

/// Deletes one authenticated physical commit authority by identity.  Ordinary
/// GC uses this point-shaped helper after a reachability delta has proved the
/// root unreachable; it never scans the inventory space to discover rows.
pub(crate) async fn stage_delete_commit_state_manifest_for_gc(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
    manifest: &CommitStateManifest,
) -> Result<(), LixError> {
    writes.delete(
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        key(commit_state_manifest_key(commit_id)),
    );
    writes.delete(
        TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
        key(commit_mutation_inventory_key(commit_id)),
    );
    for (segment_index, part) in manifest.mutations.parts.iter().enumerate() {
        writes.delete(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            key(commit_delta_segment_key_for_part(
                commit_id,
                segment_index,
                part,
            )?),
        );
    }
    for (segment_index, digest) in manifest
        .mutations
        .replacement_part_digests
        .iter()
        .enumerate()
    {
        let mut segment_key = commit_delta_segment_key(commit_id, segment_index)?;
        segment_key.extend_from_slice(digest);
        writes.delete(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, key(segment_key));
    }
    if let Some(parts) = manifest.mutations.columnar_parts.as_ref() {
        let owner = CommitId::new(uuid::Uuid::from_bytes(parts.owner_commit_id));
        if owner != commit_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("retired columnar mutation authority '{commit_id}' names owner '{owner}'"),
            ));
        }
        let row_group_id = crate::row_columnar::row_group_set_id(commit_id, &parts.schema_key);
        if row_group_id.as_bytes() != parts.row_group_set_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "retired columnar mutation authority '{commit_id}' names an unexpected row-group set"
                ),
            ));
        }
        let row_group_manifest = crate::columnar_row_group::load_row_group_manifest(store, row_group_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "retired columnar mutation authority '{commit_id}' is missing its row-group manifest"
                    ),
                )
            })?;
        if row_group_manifest.content_digest()? != parts.manifest_digest {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "retired columnar mutation authority '{commit_id}' has a row-group digest mismatch"
                ),
            ));
        }
        crate::columnar_row_group::stage_delete_row_group_set(store, writes, row_group_id).await?;
    }
    Ok(())
}

/// The content-addressed nodes and parts that are still reachable from the
/// repository's live root closure.
///
/// Every tracked-state plane below is content addressed and therefore shared
/// between commits, so a retired commit may only drop a node the live closure
/// does not also name.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RetainedPhysicalState<'a> {
    pub(crate) mutation_nodes: &'a BTreeSet<[u8; 32]>,
    pub(crate) scoped_nodes: &'a BTreeSet<[u8; 32]>,
    pub(crate) native_parts: &'a BTreeSet<[u8; 32]>,
}

/// Reclaims the change-locator rows owned by one retired commit.
///
/// A locator row is neither content addressed nor deduped: it belongs to
/// exactly one change, and an authored change belongs to exactly one physical
/// commit. Its lifetime is therefore exactly the lifetime of the segments it
/// addresses, which is why reclaiming it needs no fence of its own. Binary CAS
/// needs `stage_cas_publication_fence` because a publisher may *reuse* a
/// deduped payload row it never wrote, so a sweep planned from an older
/// snapshot could delete a row a newer publication now depends on. No publisher
/// can reuse another change's locator, so that hazard does not exist here, and
/// the sweep's existing branch-head-control preconditions already void the
/// whole write set if any publication lands after the reachability plan.
///
/// The delete is verified rather than assumed. Only a row that still names
/// `commit_id` is removed, so a locator naming a different owner — a selected
/// member this commit merely referenced, or a locator GC relocated onto a
/// retained commit — survives even though it appears in this commit's
/// inventory.
async fn stage_reclaim_retired_change_locators(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
) -> Result<(), LixError> {
    let change_ids = scan_commit_delta_members(store, commit_id)
        .await?
        .into_iter()
        .map(|(_, value)| value.change_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if change_ids.is_empty() {
        return Ok(());
    }
    let locator_keys = change_ids
        .iter()
        .map(|change_id| StorageKey(Bytes::copy_from_slice(change_id.as_uuid().as_bytes())))
        .collect::<Vec<_>>();
    let stored = PointReadPlan::new(TRACKED_STATE_CHANGE_LOCATOR_SPACE, &locator_keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    let mut reclaimed = Vec::new();
    for (change_id, value) in change_ids.iter().copied().zip(stored.value) {
        let Some(bytes) = value.and_then(full_value_bytes) else {
            continue;
        };
        if decode_change_locator(change_id, &bytes)?.commit_id == commit_id {
            reclaimed.push(change_id.as_uuid().as_bytes().to_vec());
        }
    }
    if !reclaimed.is_empty() {
        writes.delete_batch(TRACKED_STATE_CHANGE_LOCATOR_SPACE, reclaimed);
    }
    Ok(())
}

/// Collects the out-of-band JSON payload refs one commit's own packed delta
/// physically owns.
///
/// Deliberately *local*. A selected member carries no payload of its own — it
/// is a reference to a row the selected-source commit owns, and the
/// authenticated retention closure always retains that source as a physical
/// authority. So enumerating the hydrated form here would name payloads whose
/// owner GC visits separately, and would make a retired commit look like the
/// owner of bytes it only borrowed.
///
/// This is the same bounded per-commit inventory walk
/// [`load_local_selected_change_owner_commit_ids`] uses: one authenticated
/// manifest plus that commit's own segments. It never scans a repository-global
/// change, commit, or payload space.
pub(crate) async fn collect_local_commit_delta_json_refs(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    refs: &mut BTreeSet<[u8; 32]>,
) -> Result<(), LixError> {
    // Boxed, not inlined. This runs inside the repository sweep's already very
    // large future, once per retired and once per surviving commit; leaving the
    // segment-decode chain inline grew that future past the test harness's
    // 2 MiB worker stack and aborted `cas_gc_history_retention` with a stack
    // overflow rather than a failure.
    let Some(state) = Box::pin(load_point_replay_commit_state(store, commit_id)).await? else {
        return Ok(());
    };
    let Some((members, _)) = Box::pin(load_authenticated_local_commit_delta_members_for_schemas(
        store,
        &state,
        &[],
        &[],
        usize::MAX,
        false,
    ))
    .await?
    else {
        unreachable!("unbounded commit-delta payload inventory cannot exceed its segment limit")
    };
    for member in &members {
        for slot in [&member.change.snapshot, &member.change.metadata] {
            if let crate::json_store::JsonSlot::Ref(json_ref) = slot {
                refs.insert(*json_ref.as_hash_array());
            }
        }
    }
    Ok(())
}

/// Collects the out-of-band JSON payload refs named by native current-state
/// data parts, addressed by their payload-ref summary digests.
///
/// A missing summary is a hard error rather than an empty set: treating
/// corruption as "this part names no payloads" is exactly the mistake that
/// would turn a read failure into a delete.
pub(crate) async fn collect_current_state_part_json_refs(
    store: &(impl StorageAdapterRead + ?Sized),
    refs_digests: &BTreeSet<[u8; 32]>,
    refs: &mut BTreeSet<[u8; 32]>,
) -> Result<(), LixError> {
    if refs_digests.is_empty() {
        return Ok(());
    }
    let keys = refs_digests
        .iter()
        .map(|digest| StorageKey(Bytes::copy_from_slice(digest)))
        .collect::<Vec<_>>();
    let loaded = PointReadPlan::new(
        crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE,
        &keys,
    )
    .materialize(store, StorageGetOptions::default())
    .await?;
    for (digest, value) in refs_digests.iter().zip(loaded.value) {
        let Some(bytes) = value.and_then(full_value_bytes) else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "current-state data part references a missing payload-ref summary",
            ));
        };
        refs.extend(crate::tracked_state::decode_current_state_data_part_refs(
            digest, &bytes,
        )?);
    }
    Ok(())
}

/// Retires every physical tracked-state row owned by one unreachable commit.
///
/// GC proves unreachability from refs and hands the proof here; the layout of
/// mutation directories, scoped-range trees, and native current-state parts —
/// and therefore which keys a retirement touches — stays inside the module
/// that writes them.
///
/// `released_part_refs_digests` receives the payload-ref summary digest of
/// every native current-state part this retirement actually deletes. Those
/// summaries are the only remaining owner record for the JSON payloads a
/// carried-forward row named, so the caller must be able to turn them into
/// reclamation candidates before the rows that name them are gone.
pub(crate) async fn stage_retire_commit_physical_state(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
    retained: RetainedPhysicalState<'_>,
    released_part_refs_digests: &mut BTreeSet<[u8; 32]>,
) -> Result<(), LixError> {
    let manifest = load_commit_state_manifest(store, commit_id)
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("retired GC root '{commit_id}' has no authenticated manifest"),
            )
        })?;
    stage_reclaim_retired_change_locators(store, writes, commit_id).await?;
    let retired_root = load_commit_mutation_directory_roots(store, &[commit_id])
        .await?
        .into_iter()
        .next()
        .flatten();
    if let Some(root) = retired_root {
        let nodes = crate::tracked_state::collect_mutation_directory_node_ids(store, &root).await?;
        for node_id in nodes.difference(retained.mutation_nodes) {
            writes.delete(
                crate::tracked_state::MUTATION_DIRECTORY_NODE_SPACE,
                key(node_id.to_vec()),
            );
        }
    }
    if let Some(root) = manifest.current_state_scoped_ranges.as_ref() {
        let reachable = crate::tracked_state::validate_scoped_range_trees(
            store,
            std::slice::from_ref(&root.tree),
        )
        .await?;
        for node_id in reachable.node_ids.difference(retained.scoped_nodes) {
            writes.delete(
                crate::tracked_state::SCOPED_RANGE_NODE_SPACE,
                key(node_id.to_vec()),
            );
        }
        for part in reachable.parts {
            let descriptor =
                crate::tracked_state::current_state_descriptor_from_scoped_range_part(&part)?;
            if let CurrentStatePartSource::NativeDataPart {
                payload_refs_digest,
            } = descriptor.source
                && !retained.native_parts.contains(&descriptor.content_digest)
            {
                writes.delete(
                    crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
                    key(descriptor.content_digest.to_vec()),
                );
                writes.delete(
                    crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE,
                    key(payload_refs_digest.to_vec()),
                );
                released_part_refs_digests.insert(payload_refs_digest);
            }
        }
    }
    stage_delete_commit_state_manifest_for_gc(store, writes, commit_id, &manifest).await
}

/// Loads the owning commit ids of every native current-state data part named by
/// `digests`, proving each payload is still physically present.
///
/// A scoped-range descriptor is authority for the digest, but not proof that
/// the immutable payload still exists; treating a missing payload as an empty
/// live set would silently turn corruption into deletion, so this fails closed.
pub(crate) async fn load_native_current_state_part_owners(
    store: &(impl StorageAdapterRead + ?Sized),
    digests: &BTreeSet<[u8; 32]>,
) -> Result<BTreeSet<CommitId>, LixError> {
    if digests.is_empty() {
        return Ok(BTreeSet::new());
    }
    let keys = digests
        .iter()
        .map(|digest| StorageKey(Bytes::copy_from_slice(digest)))
        .collect::<Vec<_>>();
    let loaded = PointReadPlan::new(crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    let mut commit_ids = BTreeSet::new();
    for (digest, value) in digests.iter().zip(loaded.value) {
        let Some(StorageProjectedValue::FullValue(bytes)) = value else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "live current-state directory references a missing native data part",
            ));
        };
        commit_ids.extend(
            crate::tracked_state::decode_current_state_data_part_commit_ids(digest, &bytes)?,
        );
    }
    Ok(commit_ids)
}

/// Sweeps every content-addressed tracked-state plane down to a repository-wide
/// live closure.
///
/// This is the whole-repository counterpart to
/// [`stage_retire_commit_physical_state`]: it discovers rows by scanning rather
/// than from a retirement proof, so it is reserved for the offline oracle that
/// cross-checks incremental GC.
#[cfg(test)]
pub(crate) async fn stage_sweep_unreachable_content_nodes(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    retained: RetainedPhysicalState<'_>,
) -> Result<(), LixError> {
    for (space, live) in [
        (
            crate::tracked_state::SCOPED_RANGE_NODE_SPACE,
            retained.scoped_nodes,
        ),
        (
            crate::tracked_state::MUTATION_DIRECTORY_NODE_SPACE,
            retained.mutation_nodes,
        ),
        (
            crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE,
            retained.native_parts,
        ),
        (
            crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
            retained.native_parts,
        ),
    ] {
        let range = StorageKeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        };
        let mut cursor = store
            .begin_scan(
                space,
                range,
                StorageBeginScanOptions {
                    projection: StorageCoreProjection::KeyOnly,
                    ..StorageBeginScanOptions::default()
                },
            )
            .await?;
        loop {
            let (page, page_has_more) = cursor
                .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
                .await?
                .into_parts();
            for entry in page {
                let node_id = <[u8; 32]>::try_from(entry.key.0.as_ref()).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "content-addressed space '{}' contains a malformed key",
                            space.name
                        ),
                    )
                })?;
                if !live.contains(&node_id) {
                    writes.delete(space, entry.key);
                }
            }
            if !page_has_more {
                break;
            }
        }
    }
    Ok(())
}

async fn scan_full_space(
    store: &(impl StorageAdapterRead + ?Sized),
    space: StorageSpace,
) -> Result<Vec<(StorageKey, Bytes)>, LixError> {
    let range = StorageKeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    };
    let mut rows = Vec::new();
    let mut cursor = store
        .begin_scan(
            space,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let (page, page_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in &page {
            let StorageProjectedValue::FullValue(bytes) = &entry.value else {
                unreachable!("full commit-delta scan returned a key-only row");
            };
            rows.push((entry.key.clone(), bytes.clone()));
        }
        if !page_has_more {
            break;
        }
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
    account_id: &str,
    members: &mut Vec<CommitDeltaMember>,
) -> Result<(), LixError> {
    let (leaf, payloads) = decode_commit_delta_with_payloads(bytes, expected_bounds)?;
    visit_commit_delta_leaf(&leaf, expected_commit_id, |_, _, _| Ok(()))?;
    for entry_index in 0..leaf.len() {
        // The per-entry decode loop. A selected segment is decoded whole, so
        // this is the only layer at which "how much did this scan read" and
        // "how much did it return" are distinguishable.
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_commit_delta_segment_entry_decoded();
        let entry = leaf.entry(entry_index).ok_or_else(|| {
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
            account_id: account_id.to_string(),
            format_version: 2,
            change_id: value.change_id,
            schema_key: key.schema_key.clone(),
            row_pk: key.row_pk.clone(),
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
    Ok(load_point_replay_commit_state(store, commit_id)
        .await?
        .map(|manifest| commit_delta_replay_metadata_from_inventory(&manifest.mutations)))
}

/// Returns replay metadata from an already authenticated physical manifest.
pub(crate) fn seed_commit_delta_point_cache_from_replay_manifest(
    state: &AuthenticatedReplayCommitStateManifest,
    point_cache: &CommitDeltaPointReadCache,
) -> Result<CommitDeltaReplayMetadata, LixError> {
    point_cache.remember_authenticated_state(state)?;
    if let Some(manifest) = point_cache.manifest(state.commit_id)? {
        return Ok(commit_delta_replay_metadata(&manifest));
    }
    let metadata = commit_delta_replay_metadata_from_inventory(&state.mutations);
    if state.mutation_directory_root.is_none() {
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
    let Some(full_state) = load_commit_state_manifest(store, commit_id).await? else {
        return Ok(None);
    };
    let manifest = expanded_commit_delta_manifest_from_commit_state(store, &full_state).await?;
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
        let expected_segment_count = if manifest.columnar_parts.is_some() {
            manifest.direct_segment_row_counts.len()
        } else if manifest.inline_segment.is_empty() {
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
    if let Some(parts) = manifest.columnar_parts.as_ref() {
        let actual_single_partition =
            single_partition_from_bounds(&parts.first_key, &parts.last_key)?;
        if manifest.single_partition != actual_single_partition {
            return Err(replacement_payload_error(
                "columnar mutation partition certificate does not match its bounds",
            ));
        }
        return Ok(());
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
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_commit_delta_encode();
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_commit_delta_leaf_layout(
        entries.len(),
        crate::tracked_state::codec::leaf_uses_direct_address_layout(&leaf),
    );
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
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_commit_delta_leaf_decode(leaf.len(), bytes.len());
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
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_commit_delta_sidecar_zstd(
                encoded_sidecar.len(),
                decoded.len(),
            );
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
        let entry = leaf.entry(entry_index).ok_or_else(|| {
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
    let entry = leaf.entry(index).ok_or_else(|| {
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
    account_id: &str,
) -> Result<Option<LoadedCommitDeltaEntry>, LixError>
where
    S: AsRef<[u8]>,
{
    // One encoded key reaches the search per point request, so counting it
    // here counts the caller's per-row `encode_key_ref` without instrumenting
    // eleven separate call sites.
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_commit_delta_point_key_encode(target_key.len());
    let Some(index) = find_commit_delta_entry_index(leaf, target_key)? else {
        return Ok(None);
    };
    Ok(Some(load_commit_delta_entry_at_index(
        leaf,
        payloads,
        index,
        expected_commit_id,
        account_id,
    )?))
}

fn load_commit_delta_entry_at_index<S>(
    leaf: &DecodedLeafNodeRef,
    payloads: &CommitDeltaPayloadIndex<S>,
    index: usize,
    expected_commit_id: CommitId,
    account_id: &str,
) -> Result<LoadedCommitDeltaEntry, LixError>
where
    S: AsRef<[u8]>,
{
    let entry = leaf.entry(index).ok_or_else(|| {
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
    // Inside the payload fetch, not above it: this is the row-granular site the
    // profile attributed the cost to, and it is the one that re-proves an
    // identity `find_commit_delta_entry_index` already asserted byte-equal.
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_commit_delta_row_loaded(account_id.len());
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
        account_id: account_id.to_string(),
        format_version: 2,
        change_id: value.change_id,
        schema_key: key.schema_key,
        row_pk: key.row_pk,
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
    let lower = commit_delta_entry_lower_bound_from(leaf, target_key, 0)?;
    let Some(entry) = leaf.entry(lower) else {
        return Ok(None);
    };
    if entry.key != target_key {
        return Ok(None);
    }
    Ok(Some(lower))
}

fn commit_delta_entry_lower_bound_from(
    leaf: &DecodedLeafNodeRef,
    target_key: &[u8],
    mut lower: usize,
) -> Result<usize, LixError> {
    let mut upper = leaf.len();
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        let key = leaf.key(middle).ok_or_else(|| {
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
    Ok(lower)
}

pub(crate) async fn read_chunk(
    store: &(impl StorageAdapterRead + ?Sized),
    hash: &[u8; TRACKED_STATE_HASH_BYTES],
) -> Result<Option<Bytes>, LixError> {
    #[cfg(feature = "root-replay-trace")]
    {
        let start = std::time::Instant::now();
        let bytes = get_one(store, TRACKED_STATE_TREE_CHUNK_SPACE, hash.to_vec()).await;
        let read_bytes = bytes
            .as_ref()
            .ok()
            .and_then(|value| value.as_ref())
            .map_or(0, |value| value.len() as u64);
        crate::storage_bench::record_replay_chunk_read(
            start.elapsed().as_nanos() as u64,
            read_bytes,
        );
        return bytes;
    }
    #[cfg(not(feature = "root-replay-trace"))]
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
    /// Chunk digests this writer has proven are already durable at its read
    /// snapshot. The space is content-addressed, so a present key is the same
    /// bytes by construction and re-writing it is a no-op the backend cannot
    /// elide for a mutable space.
    known_durable: HashSet<[u8; TRACKED_STATE_HASH_BYTES]>,
    /// Explicit commit-root rebuild is the repair path for a damaged chunk, so
    /// it must rewrite every node it derives. A present key proves the digest
    /// is addressed, not that the stored bytes still hash to it.
    rewrite_durable: bool,
}

impl TrackedStateChunkOverlay {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Overlay for the explicit rebuild path, which repairs corrupt chunks by
    /// rewriting them and therefore never skips a durable digest.
    pub(crate) fn repairing() -> Self {
        Self {
            rewrite_durable: true,
            ..Self::default()
        }
    }

    /// Records which of `hashes` are already durable, using one presence-only
    /// batched point read. Digests already proven durable are not probed again.
    async fn probe_durable_digests(
        &mut self,
        store: &(impl StorageAdapterRead + ?Sized),
        hashes: impl IntoIterator<Item = [u8; TRACKED_STATE_HASH_BYTES]>,
    ) -> Result<(), LixError> {
        if self.rewrite_durable {
            return Ok(());
        }
        let candidates = hashes
            .into_iter()
            .filter(|hash| !self.known_durable.contains(hash))
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(());
        }
        let keys = candidates
            .iter()
            .map(|hash| StorageKey(Bytes::copy_from_slice(hash)))
            .collect::<Vec<_>>();
        let requests = [StorageGetManyRequest {
            space: TRACKED_STATE_TREE_CHUNK_SPACE,
            keys: &keys,
            opts: StorageGetOptions {
                projection: StorageCoreProjection::KeyOnly,
            },
        }];
        let result = exact_get_many(store, &requests).await?;
        for (hash, value) in candidates.into_iter().zip(result.values) {
            if value.is_some() {
                self.known_durable.insert(hash);
            }
        }
        Ok(())
    }

    pub(crate) fn staged_chunk(&self, hash: &[u8; TRACKED_STATE_HASH_BYTES]) -> Option<&[u8]> {
        self.chunks.get(hash).map(AsRef::as_ref)
    }

    pub(crate) fn chunk_hashes(&self) -> impl Iterator<Item = [u8; TRACKED_STATE_HASH_BYTES]> + '_ {
        self.chunks.keys().copied()
    }

    /// Promotes overlay chunks a previous rootless interval staged only
    /// transiently into the durable write set.
    ///
    /// This stages through the same content-addressed entry point as
    /// [`Self::stage_chunks`]. Both producers write the one chunk space in one
    /// rebuild write set, and a rooted plan routinely re-derives a node an
    /// earlier rootless plan already produced — content-addressed, so the same
    /// digest. Staging that digest through a plain put made the second producer
    /// a duplicate mutation and failed the whole rebuild; the shared
    /// content-addressed path coalesces the identical entry while still
    /// rejecting a same-digest/different-bytes conflict.
    pub(crate) async fn stage_selected_chunks(
        &mut self,
        store: &(impl StorageAdapterRead + ?Sized),
        writes: &mut StorageWriteSet,
        hashes: impl IntoIterator<Item = [u8; TRACKED_STATE_HASH_BYTES]>,
    ) -> Result<(), LixError> {
        let hashes = hashes.into_iter().collect::<Vec<_>>();
        if hashes.is_empty() {
            return Ok(());
        }
        // Transient chunks live only in the overlay, so the durable probe must
        // run against the digests themselves rather than the staged-chunk map.
        self.probe_durable_digests(store, hashes.iter().copied())
            .await?;
        let mut entries = Vec::with_capacity(hashes.len());
        for hash in hashes {
            if self.known_durable.contains(&hash) {
                continue;
            }
            let bytes = self.chunks.get(&hash).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked-state transient chunk promotion lost its overlay bytes",
                )
            })?;
            entries.push((
                StorageKey(Bytes::copy_from_slice(&hash)),
                StorageValue {
                    bytes: bytes.clone(),
                },
            ));
        }
        writes.put_content_addressed_batch(TRACKED_STATE_TREE_CHUNK_SPACE, entries);
        Ok(())
    }

    fn staged_chunk_bytes(&self, hash: &[u8; TRACKED_STATE_HASH_BYTES]) -> Option<Bytes> {
        self.chunks.get(hash).cloned()
    }

    /// Stages the chunks this rewrite produced, skipping any digest already
    /// durable at the writer's read snapshot.
    ///
    /// The tracked-state tree is content-addressed, so an existing key is
    /// necessarily the same bytes. A mutable storage space has no
    /// already-present skip of its own (RocksDB's lives behind
    /// `ValueSemantics::Immutable`; SlateDB's mutable path writes straight into
    /// its overlay), so without this filter a root rewrite that re-derives an
    /// ancestor's nodes pays the full write-set, WAL, memtable and compaction
    /// cost for bytes that are already on disk.
    pub(crate) async fn stage_chunks(
        &mut self,
        store: &(impl StorageAdapterRead + ?Sized),
        writes: &mut StorageWriteSet,
        chunks: &PendingChunkBatch,
    ) -> Result<(), LixError> {
        if chunks.is_empty() {
            return Ok(());
        }
        self.probe_durable_digests(store, chunks.chunks().iter().map(|chunk| chunk.hash))
            .await?;
        let mut key_arena =
            Vec::with_capacity(chunks.len().saturating_mul(TRACKED_STATE_HASH_BYTES));
        let mut puts = Vec::with_capacity(chunks.len());
        for chunk in chunks.chunks() {
            // Overlay residency is independent of staging: an in-flight rewrite
            // still reads a durable-skipped node through the overlay.
            self.chunks.insert(chunk.hash, chunks.chunk_data(*chunk));
            if self.known_durable.contains(&chunk.hash) {
                continue;
            }
            let key_start = key_arena.len();
            key_arena.extend_from_slice(&chunk.hash);
            puts.push(EncodedPut {
                key: BufferRange::new(key_start, TRACKED_STATE_HASH_BYTES),
                value: BufferRange::new(chunk.data_start, chunk.data_len),
            });
        }
        if puts.is_empty() {
            return Ok(());
        }
        let batch = EncodedMutationBatch::try_new(
            Bytes::from(key_arena),
            chunks.data().clone(),
            puts,
            Vec::new(),
        )
        .expect("tracked-state chunk batch descriptors must match their arenas");
        writes.stage_content_addressed_encoded_batch(TRACKED_STATE_TREE_CHUNK_SPACE, batch);
        Ok(())
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
    mutation_inventories: HashMap<Vec<u8>, Bytes>,
    mutation_directory_nodes: HashMap<Vec<u8>, Bytes>,
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
            mutation_inventories: HashMap::new(),
            mutation_directory_nodes: HashMap::new(),
        }
    }

    pub(crate) fn with_commit_state_roots(
        store: &'a S,
        chunks: &'a TrackedStateChunkOverlay,
        commit_states: impl IntoIterator<Item = (CommitStateManifest, TrackedStateCommitRoot)>,
    ) -> Result<Self, LixError> {
        let encoded = commit_states
            .into_iter()
            .map(|(mut manifest, root)| {
                if manifest.commit_id != root.commit_id {
                    return Err(replacement_payload_error(
                        "staged snapshot root belongs to a different commit manifest",
                    ));
                }
                // This overlay exists only to audit the staged canonical root;
                // it is never published as immutable commit authority.
                manifest.replay_debt = Default::default();
                manifest.snapshot_root = Some(Box::new(root));
                let key = commit_state_manifest_key(manifest.commit_id);
                let encoded = encode_commit_state_manifest(&manifest)?;
                Ok((key, encoded))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let commit_states = encoded
            .iter()
            .map(|(key, encoded)| (key.clone(), Bytes::copy_from_slice(&encoded.header)))
            .collect();
        let mutation_inventories = encoded
            .iter()
            .map(|(key, encoded)| {
                (
                    key.clone(),
                    Bytes::copy_from_slice(&encoded.mutation_inventory),
                )
            })
            .collect();
        let mutation_directory_nodes = encoded
            .iter()
            .flat_map(|(_, encoded)| encoded.mutation_directory.iter())
            .flat_map(|directory| directory.node_bytes())
            .map(|(node_id, bytes)| (node_id.to_vec(), bytes.clone()))
            .collect();
        Ok(Self {
            store,
            chunks,
            commit_states,
            mutation_inventories,
            mutation_directory_nodes,
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
        if space == TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE.id {
            return self.mutation_inventories.get(key.0.as_ref()).cloned();
        }
        if space == super::mutation_directory::MUTATION_DIRECTORY_NODE_SPACE.id {
            return self.mutation_directory_nodes.get(key.0.as_ref()).cloned();
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

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: StorageKeyRange,
        opts: StorageBeginScanOptions,
    ) -> Result<StorageScanCursor<'_>, StorageError> {
        if space == TRACKED_STATE_TREE_CHUNK_SPACE {
            return Err(StorageError::Io(
                "tracked-state staged audit supports point reads only for overlay spaces"
                    .to_string(),
            ));
        }
        self.store.begin_scan(space, range, opts).await
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

#[derive(Debug)]
struct EncodedCommitStateManifest {
    header: Vec<u8>,
    mutation_inventory: Vec<u8>,
    mutation_directory: Option<super::mutation_directory::BuiltMutationDirectory>,
}

fn encode_commit_state_manifest(
    manifest: &CommitStateManifest,
) -> Result<EncodedCommitStateManifest, LixError> {
    validate_commit_state_manifest(manifest)?;
    let selected_source_commit_id = manifest.mutations.selected_source_commit_id;
    let (stored_inventory, mutation_directory) =
        stored_commit_mutation_inventory(&manifest.mutations)?;
    let inventory_payload =
        storage_codec::encode("tracked_state commit mutation inventory", &stored_inventory)?;
    let mut mutation_inventory = Vec::with_capacity(
        COMMIT_STATE_MUTATION_INVENTORY_FORMAT_MAGIC.len() + inventory_payload.len(),
    );
    mutation_inventory.extend_from_slice(COMMIT_STATE_MUTATION_INVENTORY_FORMAT_MAGIC);
    mutation_inventory.extend_from_slice(&inventory_payload);
    let header = StoredCommitStateManifest {
        commit_id: manifest.commit_id,
        change_account_id: manifest.change_account_id.clone(),
        replay_debt: manifest.replay_debt,
        selected_source_commit_id,
        mutation_inventory_digest: *blake3::hash(&mutation_inventory).as_bytes(),
        mutation_transition_digest:
            super::scoped_current_state::current_state_mutation_authority_digest(
                &manifest.mutations,
            )?,
        mutation_member_count: manifest.mutations.member_count,
        mutation_part_count: u32::try_from(manifest.mutations.part_count()).map_err(|_| {
            replacement_payload_error("mutation part count exceeds the authority-header bound")
        })?,
        mutation_directory_root: stored_inventory.directory_root.clone(),
        touched_scope_filter: manifest.touched_scope_filter.clone(),
        current_state_scoped_ranges: manifest.current_state_scoped_ranges.clone(),
        snapshot_root: manifest.snapshot_root.clone(),
    };
    let header_payload = storage_codec::encode("tracked_state commit_state_manifest", &header)?;
    let mut encoded_header =
        Vec::with_capacity(COMMIT_STATE_MANIFEST_FORMAT_MAGIC.len() + header_payload.len());
    encoded_header.extend_from_slice(COMMIT_STATE_MANIFEST_FORMAT_MAGIC);
    encoded_header.extend_from_slice(&header_payload);
    Ok(EncodedCommitStateManifest {
        header: encoded_header,
        mutation_inventory,
        mutation_directory,
    })
}

fn stored_commit_mutation_inventory(
    inventory: &CommitStateMutationInventory,
) -> Result<
    (
        StoredCommitMutationInventory,
        Option<super::mutation_directory::BuiltMutationDirectory>,
    ),
    LixError,
> {
    let mutation_directory = if !inventory.parts.is_empty() {
        let direct_rows = if inventory.direct_part_row_counts.is_empty() {
            None
        } else {
            Some(inventory.direct_part_row_counts.as_slice())
        };
        Some(super::mutation_directory::build_bounded_mutation_directory(
            &inventory.parts,
            direct_rows,
        )?)
    } else if !inventory.replacement_part_digests.is_empty() {
        Some(
            super::mutation_directory::build_compact_replacement_mutation_directory(
                &inventory.replacement_part_digests,
                &inventory.direct_part_row_counts,
            )?,
        )
    } else if inventory.columnar_parts.is_some() && !inventory.direct_part_row_counts.is_empty() {
        Some(
            super::mutation_directory::build_direct_rows_mutation_directory(
                &inventory.direct_part_row_counts,
            )?,
        )
    } else {
        None
    };
    let stored = StoredCommitMutationInventory {
        member_count: inventory.member_count,
        selection_fingerprint: inventory.selection_fingerprint,
        single_partition: inventory.single_partition.clone(),
        lifecycle_summary: inventory.lifecycle_summary.clone(),
        replacement_generation: inventory.replacement_generation.clone(),
        replacement_parts: inventory.replacement_parts.clone(),
        columnar_parts: inventory.columnar_parts.clone(),
        inline_part: inventory.inline_part.clone(),
        inline_direct: !inventory.inline_part.is_empty()
            && !inventory.direct_part_row_counts.is_empty(),
        directory_root: mutation_directory
            .as_ref()
            .map(|directory| directory.root.clone()),
    };
    Ok((stored, mutation_directory))
}

async fn decode_commit_state_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    header: &[u8],
    mutation_inventory: &[u8],
) -> Result<CommitStateManifest, LixError> {
    decode_commit_state_manifest_with_scoped_range_attestation(
        store,
        header,
        mutation_inventory,
        true,
    )
    .await
}

async fn decode_commit_state_manifest_with_scoped_range_attestation(
    store: &(impl StorageAdapterRead + ?Sized),
    header: &[u8],
    mutation_inventory: &[u8],
    validate_scoped_range_attestation: bool,
) -> Result<CommitStateManifest, LixError> {
    let (stored, stored_inventory) =
        decode_stored_commit_state_authority(header, mutation_inventory)?;
    let entries = match stored_inventory.directory_root.as_ref() {
        Some(root) => super::mutation_directory::load_mutation_part_read_plan(
            store,
            root,
            super::mutation_directory::MutationDirectoryReadSelection::All(
                super::mutation_directory::MutationDirectoryFullTraversalContext::FullManifestExpansion,
            ),
        )
        .await?
        .into_runs()
        .into_iter()
        .map(|run| run.entry)
        .collect(),
        None => Vec::new(),
    };
    assemble_commit_state_manifest(
        stored,
        stored_inventory,
        entries,
        validate_scoped_range_attestation,
    )
}

fn decode_stored_commit_state_authority(
    header: &[u8],
    mutation_inventory: &[u8],
) -> Result<(StoredCommitStateManifest, StoredCommitMutationInventory), LixError> {
    let stored = decode_stored_commit_state_manifest(header)?;
    if stored.mutation_inventory_digest != *blake3::hash(mutation_inventory).as_bytes() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit mutation inventory disagrees with its authority digest",
        ));
    }
    let Some(inventory_payload) =
        mutation_inventory.strip_prefix(COMMIT_STATE_MUTATION_INVENTORY_FORMAT_MAGIC)
    else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit mutation inventory has an unsupported format; recreate the repository",
        ));
    };
    let stored_inventory: StoredCommitMutationInventory =
        storage_codec::decode("tracked_state commit mutation inventory", inventory_payload)?;
    if stored.mutation_member_count != stored_inventory.member_count
        || stored.mutation_directory_root != stored_inventory.directory_root
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit mutation inventory disagrees with its authority header",
        ));
    }
    Ok((stored, stored_inventory))
}

fn assemble_commit_state_manifest(
    stored: StoredCommitStateManifest,
    stored_inventory: StoredCommitMutationInventory,
    entries: Vec<super::mutation_directory::MutationDirectoryEntry>,
    validate_scoped_range_attestation: bool,
) -> Result<CommitStateManifest, LixError> {
    use super::mutation_directory::{
        LAYOUT_BOUNDED_DIRECT, LAYOUT_BOUNDED_INDIRECT, LAYOUT_COMPACT_REPLACEMENT,
        LAYOUT_DIRECT_ROWS_ONLY, MutationDirectoryEntry,
    };
    let mut parts = Vec::new();
    let mut direct_part_row_counts = Vec::new();
    let mut replacement_part_digests = Vec::new();
    match stored_inventory
        .directory_root
        .as_ref()
        .map(|root| root.layout)
    {
        Some(LAYOUT_BOUNDED_INDIRECT | LAYOUT_BOUNDED_DIRECT) => {
            let direct = stored_inventory
                .directory_root
                .as_ref()
                .is_some_and(|root| root.layout == LAYOUT_BOUNDED_DIRECT);
            for entry in entries {
                let MutationDirectoryEntry::Bounded {
                    part,
                    direct_row_count,
                } = entry
                else {
                    return Err(replacement_payload_error(
                        "bounded mutation directory contains a compact entry",
                    ));
                };
                parts.push(part);
                if direct {
                    direct_part_row_counts.push(direct_row_count);
                }
            }
        }
        Some(LAYOUT_COMPACT_REPLACEMENT) => {
            for entry in entries {
                let MutationDirectoryEntry::CompactReplacement {
                    content_digest,
                    direct_row_count,
                } = entry
                else {
                    return Err(replacement_payload_error(
                        "compact mutation directory contains a bounded entry",
                    ));
                };
                replacement_part_digests.push(content_digest);
                direct_part_row_counts.push(direct_row_count);
            }
        }
        Some(LAYOUT_DIRECT_ROWS_ONLY) => {
            for entry in entries {
                let MutationDirectoryEntry::DirectAddress { direct_row_count } = entry else {
                    return Err(replacement_payload_error(
                        "direct-address directory contains a physical part entry",
                    ));
                };
                direct_part_row_counts.push(direct_row_count);
            }
        }
        None if entries.is_empty() => {}
        _ => {
            return Err(replacement_payload_error(
                "mutation directory has an unsupported authority layout",
            ));
        }
    }
    if stored_inventory.inline_direct {
        let inline_rows = u16::try_from(stored_inventory.member_count).map_err(|_| {
            replacement_payload_error("inline mutation row count exceeds direct-address bound")
        })?;
        if inline_rows == 0 || !direct_part_row_counts.is_empty() {
            return Err(replacement_payload_error(
                "inline direct-address authority has an invalid directory",
            ));
        }
        direct_part_row_counts.push(inline_rows);
    }
    let mutations = CommitStateMutationInventory {
        selected_source_commit_id: stored.selected_source_commit_id,
        member_count: stored_inventory.member_count,
        selection_fingerprint: stored_inventory.selection_fingerprint,
        direct_part_row_counts,
        replacement_part_digests,
        single_partition: stored_inventory.single_partition,
        lifecycle_summary: stored_inventory.lifecycle_summary,
        replacement_generation: stored_inventory.replacement_generation,
        replacement_parts: stored_inventory.replacement_parts,
        columnar_parts: stored_inventory.columnar_parts,
        inline_part: stored_inventory.inline_part,
        parts,
    };
    if super::scoped_current_state::current_state_mutation_authority_digest(&mutations)?
        != stored.mutation_transition_digest
    {
        return Err(replacement_payload_error(
            "mutation catalog disagrees with its transition authority digest",
        ));
    }
    let manifest = CommitStateManifest {
        commit_id: stored.commit_id,
        change_account_id: stored.change_account_id,
        replay_debt: stored.replay_debt,
        mutations,
        touched_scope_filter: stored.touched_scope_filter,
        current_state_scoped_ranges: stored.current_state_scoped_ranges,
        snapshot_root: stored.snapshot_root,
    };
    if u32::try_from(manifest.mutations.part_count()).ok() != Some(stored.mutation_part_count) {
        return Err(replacement_payload_error(
            "mutation part closure disagrees with its authority header",
        ));
    }
    validate_commit_state_manifest_inner(&manifest, validate_scoped_range_attestation)?;
    Ok(manifest)
}

fn assemble_shallow_commit_state_manifest(
    stored: StoredCommitStateManifest,
    stored_inventory: StoredCommitMutationInventory,
) -> Result<CommitStateManifest, LixError> {
    use super::mutation_directory::{
        LAYOUT_BOUNDED_DIRECT, LAYOUT_BOUNDED_INDIRECT, LAYOUT_COMPACT_REPLACEMENT,
        LAYOUT_DIRECT_ROWS_ONLY,
    };

    let root = stored_inventory.directory_root.as_ref();
    let has_inline = !stored_inventory.inline_part.is_empty();
    let has_columnar = stored_inventory.columnar_parts.is_some();
    let invalid_shape = has_inline && (has_columnar || root.is_some())
        || has_columnar && root.is_none_or(|root| root.layout != LAYOUT_DIRECT_ROWS_ONLY)
        || root.is_some_and(|root| {
            matches!(
                root.layout,
                LAYOUT_BOUNDED_DIRECT | LAYOUT_BOUNDED_INDIRECT | LAYOUT_COMPACT_REPLACEMENT
            ) && (has_inline || has_columnar)
        })
        || (stored_inventory.member_count > 0 && !has_inline && !has_columnar && root.is_none())
        || stored_inventory.inline_direct && !has_inline
        || has_inline && stored_inventory.member_count > COMMIT_DELTA_SEGMENT_MAX_ROWS as u32
        || root.is_some_and(|root| {
            (root.layout == LAYOUT_BOUNDED_DIRECT
                || root.layout == LAYOUT_COMPACT_REPLACEMENT
                || root.layout == LAYOUT_DIRECT_ROWS_ONLY)
                && root.direct_row_count != u64::from(stored_inventory.member_count)
        });
    if invalid_shape {
        return Err(replacement_payload_error(
            "shallow mutation catalog has an invalid directory shape",
        ));
    }
    let mutation_part_count = if let Some(columnar) = stored_inventory.columnar_parts.as_ref() {
        u32::try_from(columnar.group_row_counts.len())
            .map_err(|_| replacement_payload_error("columnar part count overflows"))?
    } else if has_inline {
        1
    } else {
        root.map_or(0, |root| root.entry_count)
    };
    if mutation_part_count != stored.mutation_part_count {
        return Err(replacement_payload_error(
            "shallow mutation part closure disagrees with its authority header",
        ));
    }
    let direct_part_row_counts = if stored_inventory.inline_direct {
        vec![u16::try_from(stored_inventory.member_count).map_err(|_| {
            replacement_payload_error("inline mutation row count exceeds direct-address bound")
        })?]
    } else {
        Vec::new()
    };
    let mutations = CommitStateMutationInventory {
        selected_source_commit_id: stored.selected_source_commit_id,
        member_count: stored_inventory.member_count,
        selection_fingerprint: stored_inventory.selection_fingerprint,
        direct_part_row_counts,
        replacement_part_digests: Vec::new(),
        single_partition: stored_inventory.single_partition,
        lifecycle_summary: stored_inventory.lifecycle_summary,
        replacement_generation: stored_inventory.replacement_generation,
        replacement_parts: stored_inventory.replacement_parts,
        columnar_parts: stored_inventory.columnar_parts,
        inline_part: stored_inventory.inline_part,
        parts: Vec::new(),
    };
    Ok(CommitStateManifest {
        commit_id: stored.commit_id,
        change_account_id: stored.change_account_id,
        replay_debt: stored.replay_debt,
        mutations,
        touched_scope_filter: stored.touched_scope_filter,
        current_state_scoped_ranges: stored.current_state_scoped_ranges,
        snapshot_root: stored.snapshot_root,
    })
}

#[cfg(test)]
fn decode_encoded_commit_state_manifest(
    encoded: &EncodedCommitStateManifest,
) -> Result<CommitStateManifest, LixError> {
    let (stored, stored_inventory) =
        decode_stored_commit_state_authority(&encoded.header, &encoded.mutation_inventory)?;
    let entries = match encoded.mutation_directory.as_ref() {
        Some(directory) => super::mutation_directory::decode_built_mutation_directory(directory)?,
        None => Vec::new(),
    };
    assemble_commit_state_manifest(stored, stored_inventory, entries, true)
}

fn decode_stored_commit_state_manifest(
    bytes: &[u8],
) -> Result<StoredCommitStateManifest, LixError> {
    let stored = if let Some(payload) = bytes.strip_prefix(COMMIT_STATE_MANIFEST_FORMAT_MAGIC) {
        storage_codec::decode("tracked_state commit_state_manifest", payload)?
    } else if let Some(payload) = bytes.strip_prefix(COMMIT_STATE_MANIFEST_V10_FORMAT_MAGIC) {
        let stored: StoredCommitStateManifestV10 =
            storage_codec::decode("tracked_state v10 commit_state_manifest", payload)?;
        stored.into()
    } else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest has an unsupported format; recreate the repository",
        ));
    };
    validate_commit_state_manifest_header(&stored)?;
    Ok(stored)
}

fn validate_commit_state_manifest_header(
    stored: &StoredCommitStateManifest,
) -> Result<(), LixError> {
    if stored.mutation_inventory_digest == [0; 32]
        || stored.mutation_transition_digest == [0; 32]
        || stored.selected_source_commit_id == Some(*stored.commit_id.as_uuid().as_bytes())
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest has invalid mutation authority",
        ));
    }
    if let Some(root) = stored.mutation_directory_root.as_ref() {
        super::mutation_directory::validate_mutation_directory_root(root)?;
        if root.layout != super::mutation_directory::LAYOUT_DIRECT_ROWS_ONLY
            && root.entry_count != stored.mutation_part_count
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_state_manifest mutation root count disagrees with its header",
            ));
        }
    }
    if stored.replay_debt.depth == 0
        && (stored.replay_debt.rows != 0 || stored.replay_debt.bytes != 0)
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest has replay work at zero depth",
        ));
    }
    if stored.replay_debt.depth > crate::tracked_state::COMMIT_STATE_MAX_REPLAY_DEPTH
        || stored.replay_debt.bytes > crate::tracked_state::COMMIT_STATE_MAX_REPLAY_BYTES
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest replay debt exceeds the protocol bound",
        ));
    }
    if stored.replay_debt.depth == 0 && stored.snapshot_root.is_none() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state rooted commit_state_manifest is missing its snapshot root",
        ));
    }
    if let Some(root) = stored.snapshot_root.as_ref()
        && (root.commit_id != stored.commit_id || stored.replay_debt.depth != 0)
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest has an invalid snapshot root",
        ));
    }
    if stored
        .snapshot_root
        .as_ref()
        .is_some_and(|root| root.complete_state_fence && !root.parent_roots.is_empty())
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state complete-state fence retains physical parent roots",
        ));
    }
    super::scoped_current_state::validate_touched_scope_filter(&stored.touched_scope_filter)?;
    if stored
        .current_state_scoped_ranges
        .as_ref()
        .is_some_and(|root| {
            root.tree.root_id == [0; 32]
                || root.tree.root_digest == [0; 32]
                || root.tree.tree_height == 0
                || root.tree.marker_count == 0
                || root.transition_digest == [0; 32]
                || root.serving_base_commit_id.is_some() != root.serving_base_root_id.is_some()
                || (stored.selected_source_commit_id.is_some()
                    && root.serving_base_commit_id
                        != stored
                            .selected_source_commit_id
                            .map(|bytes| CommitId::new(uuid::Uuid::from_bytes(bytes))))
        })
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest has an invalid current-state scoped-range root",
        ));
    }
    if let Some(root) = stored.current_state_scoped_ranges.as_ref()
        && root.transition_digest
            != super::scoped_current_state::scoped_range_transition_digest_from_authority(
                stored.commit_id,
                root.serving_base_commit_id,
                root.serving_base_root_id,
                stored.mutation_transition_digest,
                &root.tree,
            )
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest scoped-range transition disagrees with its header authority",
        ));
    }
    Ok(())
}

fn validate_commit_state_manifest(manifest: &CommitStateManifest) -> Result<(), LixError> {
    validate_commit_state_manifest_inner(manifest, true)
}

fn validate_commit_state_manifest_inner(
    manifest: &CommitStateManifest,
    validate_scoped_range_attestation: bool,
) -> Result<(), LixError> {
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
    if manifest.replay_debt.depth == 0 && manifest.snapshot_root.is_none() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state rooted commit_state_manifest is missing its snapshot root",
        ));
    }
    if let Some(root) = manifest.snapshot_root.as_ref() {
        if root.commit_id != manifest.commit_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_state_manifest snapshot root belongs to a different commit",
            ));
        }
        if manifest.replay_debt.depth != 0 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state rootless commit_state_manifest cannot publish a snapshot root",
            ));
        }
        if root.complete_state_fence && !root.parent_roots.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state complete-state fence retains physical parent roots",
            ));
        }
    }

    validate_commit_state_mutation_inventory(manifest.commit_id, &manifest.mutations)?;
    super::scoped_current_state::validate_touched_scope_filter(&manifest.touched_scope_filter)?;
    validate_current_state_scoped_ranges(manifest, validate_scoped_range_attestation)
}

fn validate_current_state_scoped_ranges(
    manifest: &CommitStateManifest,
    validate_attestation: bool,
) -> Result<(), LixError> {
    if manifest
        .current_state_scoped_ranges
        .as_ref()
        .is_some_and(|root| {
            root.tree.root_id == [0; 32]
                || root.tree.root_digest == [0; 32]
                || root.tree.tree_height == 0
                || root.tree.marker_count == 0
                || root.transition_digest == [0; 32]
                || root.serving_base_commit_id.is_some() != root.serving_base_root_id.is_some()
        })
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_state_manifest has an invalid current-state scoped-range root",
        ));
    }
    if let Some(root) = manifest.current_state_scoped_ranges.as_ref() {
        let selected_source = manifest.mutations.selected_source_commit_id();
        if selected_source.is_some() && root.serving_base_commit_id != selected_source {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state current-state serving base disagrees with commit authority",
            ));
        }
        if validate_attestation {
            super::scoped_current_state::validate_scoped_range_attestation(
                manifest.commit_id,
                &manifest.mutations,
                root,
            )?;
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
    if let Some(columnar) = inventory.columnar_parts.as_ref() {
        if inventory.selected_source_commit_id.is_some()
            || !inventory.inline_part.is_empty()
            || !inventory.parts.is_empty()
            || !inventory.replacement_part_digests.is_empty()
            || inventory.replacement_generation.is_some()
            || inventory.replacement_parts.is_some()
            || inventory.single_partition.as_ref().is_none_or(|scope| {
                scope.schema_key != columnar.schema_key || scope.file_id.is_some()
            })
            || columnar.owner_commit_id != *commit_id.as_uuid().as_bytes()
            || columnar.row_group_set_id
                != crate::row_columnar::row_group_set_id(commit_id, &columnar.schema_key).as_bytes()
            || columnar.manifest_digest == [0; 32]
            || columnar.schema_key.is_empty()
            || columnar.row_count != inventory.member_count
            || columnar.group_row_counts.is_empty()
            || columnar.group_row_counts.iter().any(|&rows| {
                rows == 0 || rows as usize > crate::columnar_row_group::ROW_GROUP_MAX_ROWS
            })
            || columnar
                .group_row_counts
                .iter()
                .map(|&rows| u64::from(rows))
                .sum::<u64>()
                != u64::from(columnar.row_count)
            || columnar.first_key.is_empty()
            || columnar.last_key.is_empty()
            || columnar.first_key > columnar.last_key
            || columnar.page_first_keys.len()
                != (columnar.row_count as usize)
                    .div_ceil(crate::columnar_row_group::ROW_GROUP_PAGE_ROWS)
            || columnar.page_first_keys.len() != columnar.page_last_keys.len()
            || columnar
                .page_first_keys
                .iter()
                .zip(&columnar.page_last_keys)
                .any(|(first, last)| first.is_empty() || first > last)
            || columnar
                .page_last_keys
                .iter()
                .zip(columnar.page_first_keys.iter().skip(1))
                .any(|(last, first)| last >= first)
            || columnar.page_first_keys.first() != Some(&columnar.first_key)
            || columnar.page_last_keys.last() != Some(&columnar.last_key)
            || inventory.lifecycle_summary.as_ref().is_none_or(|summary| {
                summary.scope.schema_key != columnar.schema_key
                    || summary.scope.file_id.is_some()
                    || summary.uniform_created_at != columnar.uniform_created_at
            })
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_state_manifest has an invalid columnar mutation inventory",
            ));
        }
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
        if (inventory.columnar_parts.is_none() && direct_rows.len() != inventory.part_count())
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
        && inventory.columnar_parts.is_none()
        && inventory.replacement_part_digests.is_empty()
        && inventory.inline_part.is_empty()
        && inventory.parts.is_empty();
    if !is_empty {
        if inventory.replacement_part_digests.is_empty() && inventory.columnar_parts.is_none() {
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
    use std::fs;
    use std::future::Future;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;

    use crate::LixError;
    use crate::changelog::{COMMIT_SPACE, ChangeId, CommitId, CommitRecord};
    use crate::common::LixTimestamp;
    use crate::row_pk::RowPk;
    use crate::storage_adapter::{
        Memory, StorageAdapter, StorageReadOptions, StorageSpace, StorageWriteOptions,
        StorageWriteSet,
    };
    use crate::storage_codec;
    use crate::tracked_state::codec::{
        EncodedLeafEntry, PendingChunk, PendingChunkBatch, TrackedStateKeyBatchBuilder,
        encode_key_ref, encode_value_ref, hash_bytes,
    };
    use crate::tracked_state::types::CurrentStatePartSource;
    use crate::tracked_state::types::{
        CommitStateManifest, CommitStateMutationInventory,
        CommitStateMutationPart as FixtureMutationPart, CommitStateReplayDebt,
        CurrentStatePartDescriptor, TrackedStateBaseCoordinate, TrackedStateCommitDeltaRef,
        TrackedStateCommitRoot, TrackedStateDeltaRef, TrackedStateIndexValue,
        TrackedStateIndexValueRef, TrackedStateKey, TrackedStateKeyRef, TrackedStateRootId,
    };

    use super::{
        AuthenticatedReplayCommitStateManifest, COMMIT_DELTA_FORMAT_MAGIC,
        COMMIT_STATE_MANIFEST_FORMAT_MAGIC, CommitDeltaChangeLocator, CommitDeltaManifest,
        CommitDeltaPayloadRef, DecodedCommitDeltaBatch, DecodedCommitDeltaCache,
        DecodedCommitDeltaSegment, GENERIC_COMMIT_DELTA_SEGMENT_MAX_ROWS,
        TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, TrackedStateChunkOverlay,
        columnar_identity_row_map, decode_commit_delta_with_payloads,
        decode_encoded_commit_state_manifest, decode_stored_commit_state_authority,
        encode_commit_delta_segment, encode_commit_delta_segment_with_payloads,
        encode_commit_delta_segment_with_raw_sidecar, encode_commit_state_manifest, key,
        load_change_record_by_id, load_commit_delta_change_ids, load_commit_delta_change_records,
        load_commit_delta_members_with_payloads, load_commit_delta_values_encoded,
        load_commit_state_manifest, load_owned_commit_delta_entries,
        scan_change_records_from_commit_deltas, scan_commit_delta_inventory,
        scan_commit_delta_members, scan_commit_delta_values, stage_change_locators,
        stage_commit_state_manifest, stage_delete_commit_delta_inventory_entry,
        stage_fragmented_scoped_current_state_descriptor, value,
    };

    #[tokio::test]
    async fn deferred_commit_history_is_not_reported_as_an_empty_commit() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("deferred-history");
        let mut writes = storage.new_write_set();
        super::stage_commit_history_deferred(&mut writes, commit_id);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("deferred history marker should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("deferred history read should open");
        let error = super::load_commit_delta_members_with_payloads_for_schemas(
            &read,
            commit_id,
            &[],
            &[],
            usize::MAX,
        )
        .await
        .expect_err("marked header-only history must require hydration");
        assert_eq!(error.code, "LIX_SYNC_HISTORY_REQUIRED");
        assert_eq!(
            error
                .details
                .expect("history error should identify commits")["commitIds"],
            serde_json::json!([commit_id.to_string()]),
        );
        drop(read);

        let mut writes = storage.new_write_set();
        super::stage_commit_history_available(&mut writes, commit_id);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("available history marker transition should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("available history read should open");
        let members = super::load_commit_delta_members_with_payloads_for_schemas(
            &read,
            commit_id,
            &[],
            &[],
            usize::MAX,
        )
        .await
        .expect("available empty history should load")
        .expect("unbounded history read should not hit a segment limit");
        assert!(members.is_empty());
    }

    #[tokio::test]
    async fn missing_manifest_classifies_deferred_history_but_keeps_corruption_internal() {
        let storage = StorageAdapter::new(Memory::new());
        let deferred = CommitId::for_test_label("classified-deferred-history");
        let corrupt = CommitId::for_test_label("classified-corrupt-history");
        let mut writes = storage.new_write_set();
        super::stage_commit_history_deferred(&mut writes, deferred);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("deferred marker commits");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("classification read opens");

        let demand = super::missing_commit_state_manifest_error(&read, deferred).await;
        assert_eq!(demand.code, "LIX_SYNC_HISTORY_REQUIRED");
        assert_eq!(
            demand.details.expect("demand includes commit ids")["commitIds"],
            serde_json::json!([deferred.to_string()]),
        );
        let corruption = super::missing_commit_state_manifest_error(&read, corrupt).await;
        assert_eq!(corruption.code, LixError::CODE_INTERNAL_ERROR);
    }

    #[tokio::test]
    async fn unmarked_commit_without_history_remains_genuinely_empty() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("genuine-empty-history");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("empty history read should open");
        let members = super::load_commit_delta_members_with_payloads_for_schemas(
            &read,
            commit_id,
            &[],
            &[],
            usize::MAX,
        )
        .await
        .expect("unmarked empty history should load")
        .expect("unbounded history read should not hit a segment limit");
        assert!(members.is_empty());
    }

    #[test]
    fn columnar_identity_lookup_does_not_assume_json_text_order() {
        use datafusion::arrow::array::StringArray;

        // Encoded RowPk order is not JSON serialization order when a
        // component requires escaping. Equality routing must therefore not
        // binary-search the JSON text representation.
        let identities = StringArray::from(vec![r#"["\n"]"#, r#"["!"]"#]);
        let rows = columnar_identity_row_map(&identities);
        assert_eq!(rows.get(r#"["\n"]"#), Some(&0));
        assert_eq!(rows.get(r#"["!"]"#), Some(&1));
    }

    #[test]
    fn fragmented_native_source_preserves_lifecycle_and_batches_adjacent_updates() {
        use crate::json_store::JsonSlot;
        use crate::tracked_state::current_state_data_part::{
            CURRENT_STATE_DATA_PART_SPACE, CurrentStateDataRow, decode_current_state_data_part,
        };

        let storage = StorageAdapter::new(Memory::new());
        let created_at = LixTimestamp::from_unix_millis_utc_lossy(10);
        let updated_at = LixTimestamp::from_unix_millis_utc_lossy(20);
        let replacement_updated_at = LixTimestamp::from_unix_millis_utc_lossy(30);
        let owner = CommitId::for_test_label("fragmented-native-owner");
        let rows = [b"a".to_vec(), b"b".to_vec()]
            .into_iter()
            .enumerate()
            .map(|(index, encoded_key)| CurrentStateDataRow {
                encoded_key,
                value: TrackedStateIndexValue {
                    change_id: ChangeId::for_test_label(&format!("fragmented-source-{index}")),
                    commit_id: owner,
                    deleted: false,
                    created_at,
                    updated_at,
                },
                snapshot: JsonSlot::Inline(format!("{{\"version\":{index}}}").into()),
                metadata: JsonSlot::None,
            })
            .collect::<Vec<_>>();
        let descriptor = CurrentStatePartDescriptor {
            first_key: rows[0].encoded_key.clone(),
            last_key: rows[1].encoded_key.clone(),
            content_digest: [7; 32],
            source: CurrentStatePartSource::NativeDataPart {
                payload_refs_digest: [8; 32],
            },
            source_row_offset: 4,
            row_count: 2,
            fragmented: false,
        };
        let mutations = rows
            .iter()
            .enumerate()
            .map(|(index, source)| {
                let mut row = source.clone();
                row.value.change_id =
                    ChangeId::for_test_label(&format!("fragmented-update-{index}"));
                row.value.commit_id = CommitId::for_test_label("fragmented-native-child");
                row.value.created_at = replacement_updated_at;
                row.value.updated_at = replacement_updated_at;
                (row.encoded_key.clone(), Some(row))
            })
            .collect::<Vec<_>>();
        let mut writes = storage.new_write_set();
        let mut output = Vec::new();
        stage_fragmented_scoped_current_state_descriptor(
            &mut writes,
            &descriptor,
            &rows,
            mutations,
            &mut output,
        )
        .expect("adjacent native updates should fragment");
        assert_eq!(
            output.len(),
            1,
            "adjacent updates must remain one native run"
        );
        assert!(matches!(
            output[0].source,
            CurrentStatePartSource::NativeDataPart { .. }
        ));
        assert_eq!(output[0].row_count, 2);
        let bytes = writes
            .staged_value(CURRENT_STATE_DATA_PART_SPACE, &output[0].content_digest)
            .expect("updated native run should be staged");
        let decoded = decode_current_state_data_part(&output[0].content_digest, &bytes)
            .expect("updated native run should decode");
        assert!(decoded.iter().all(|row| row.value.created_at == created_at));

        let mut writes = storage.new_write_set();
        let mut output = Vec::new();
        let mut first_update = decoded[0].clone();
        first_update.value.created_at = replacement_updated_at;
        stage_fragmented_scoped_current_state_descriptor(
            &mut writes,
            &descriptor,
            &rows,
            vec![(first_update.encoded_key.clone(), Some(first_update))],
            &mut output,
        )
        .expect("one native update should retain a source slice");
        assert_eq!(output.len(), 2);
        assert!(matches!(
            output[1].source,
            CurrentStatePartSource::NativeDataPart { .. }
        ));
        assert_eq!(output[1].source_row_offset, 5);
        assert_eq!(output[1].row_count, 1);

        let mut writes = storage.new_write_set();
        let mut output = Vec::new();
        stage_fragmented_scoped_current_state_descriptor(
            &mut writes,
            &descriptor,
            &rows,
            vec![(b"absent".to_vec(), None)],
            &mut output,
        )
        .expect("absent delete should be a physical no-op");
        assert_eq!(output, vec![descriptor.clone()]);
        assert!(!output[0].fragmented);

        let alternating_rows = (0..64_u16)
            .map(|index| CurrentStateDataRow {
                encoded_key: index.to_be_bytes().to_vec(),
                value: TrackedStateIndexValue {
                    change_id: ChangeId::for_test_label(&format!("alternating-source-{index}")),
                    commit_id: owner,
                    deleted: false,
                    created_at,
                    updated_at,
                },
                snapshot: JsonSlot::Inline("{}".into()),
                metadata: JsonSlot::None,
            })
            .collect::<Vec<_>>();
        let alternating_descriptor = CurrentStatePartDescriptor {
            first_key: alternating_rows[0].encoded_key.clone(),
            last_key: alternating_rows.last().unwrap().encoded_key.clone(),
            content_digest: [9; 32],
            source: CurrentStatePartSource::NativeDataPart {
                payload_refs_digest: [10; 32],
            },
            source_row_offset: 0,
            row_count: alternating_rows.len() as u16,
            fragmented: false,
        };
        let alternating_mutations = alternating_rows
            .iter()
            .step_by(2)
            .map(|source| {
                let mut row = source.clone();
                row.value.updated_at = replacement_updated_at;
                (row.encoded_key.clone(), Some(row))
            })
            .collect::<Vec<_>>();
        let mut writes = storage.new_write_set();
        let mut output = Vec::new();
        stage_fragmented_scoped_current_state_descriptor(
            &mut writes,
            &alternating_descriptor,
            &alternating_rows,
            alternating_mutations,
            &mut output,
        )
        .expect("alternating updates should use the bounded rewrite fallback");
        assert_eq!(
            output.len(),
            1,
            "alternating updates must not publish one descriptor per authored run"
        );
        assert_eq!(output[0].row_count, 64);
    }

    #[test]
    fn sparse_leaf_compaction_budget_bounds_low_density_fragment_growth() {
        let scope = crate::tracked_state::types::CommitDeltaReplacementScope {
            schema_key: "fragmented".to_string(),
            file_id: None,
        };
        let part = |index: u32, fragmented: bool| {
            let key = index.to_be_bytes().to_vec();
            crate::tracked_state::current_state_envelope::scoped_range_part_from_current_state_descriptor(
                &scope,
                &CurrentStatePartDescriptor {
                    first_key: key.clone(),
                    last_key: key,
                    content_digest: *blake3::hash(&index.to_be_bytes()).as_bytes(),
                    source: CurrentStatePartSource::NativeDataPart {
                        payload_refs_digest: [8; 32],
                    },
                    source_row_offset: 0,
                    row_count: 1,
                    fragmented,
                },
            )
            .unwrap()
        };
        let mut parts = (0..20_u32)
            .map(|index| part(index, false))
            .chain((20..51_u32).map(|index| part(index, true)))
            .chain((52..116_u32).map(|index| part(index, false)))
            .collect::<Vec<_>>();
        assert!(
            super::sparse_current_state_fragment_compaction_ranges(
                &parts.iter().collect::<Vec<_>>()
            )
            .unwrap()
            .is_empty()
        );
        parts.insert(51, part(51, true));
        assert_eq!(
            super::sparse_current_state_fragment_compaction_ranges(
                &parts.iter().collect::<Vec<_>>()
            )
            .unwrap(),
            vec![(20, 52)],
            "only the contiguous fragment run should compact; 84 healthy neighbors remain untouched"
        );
        let naturally_small = (0..64_u32)
            .map(|index| part(index, false))
            .collect::<Vec<_>>();
        assert!(
            super::sparse_current_state_fragment_compaction_ranges(
                &naturally_small.iter().collect::<Vec<_>>()
            )
            .unwrap()
            .is_empty(),
            "canonical byte-limited parts must never be mistaken for structural fragments"
        );
    }

    struct ManifestCountingRead<R> {
        inner: R,
        get_many_calls: std::sync::Arc<AtomicUsize>,
        manifest_requests: std::sync::Arc<AtomicUsize>,
        inventory_requests: std::sync::Arc<AtomicUsize>,
        directory_requests: std::sync::Arc<AtomicUsize>,
    }

    impl<R> crate::storage_adapter::StorageAdapterRead for ManifestCountingRead<R>
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
            self.get_many_calls.fetch_add(1, Ordering::Relaxed);
            for request in requests {
                if request.space == super::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE {
                    self.manifest_requests.fetch_add(1, Ordering::Relaxed);
                } else if request.space == super::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE {
                    self.inventory_requests.fetch_add(1, Ordering::Relaxed);
                } else if request.space
                    == super::super::mutation_directory::MUTATION_DIRECTORY_NODE_SPACE
                {
                    self.directory_requests.fetch_add(1, Ordering::Relaxed);
                }
            }
            self.inner.get_many(requests)
        }

        fn begin_scan(
            &self,
            space: StorageSpace,
            range: crate::storage::KeyRange,
            opts: crate::storage::BeginScanOptions,
        ) -> impl Future<
            Output = Result<crate::storage::ScanCursor<'_>, crate::storage::StorageError>,
        > + Send {
            self.inner.begin_scan(space, range, opts)
        }
    }

    fn fixture_commit_state_manifest(
        commit_id: CommitId,
        mutations: CommitStateMutationInventory,
    ) -> CommitStateManifest {
        CommitStateManifest {
            commit_id,
            change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            replay_debt: CommitStateReplayDebt {
                depth: 1,
                rows: u64::from(mutations.member_count),
                bytes: u64::from(mutations.member_count),
            },
            mutations,
            touched_scope_filter: Default::default(),
            current_state_scoped_ranges: None,
            snapshot_root: None,
        }
    }

    fn stage_fixture_manifest(
        writes: &mut StorageWriteSet,
        commit_id: CommitId,
        mutations: &CommitStateMutationInventory,
    ) -> Result<(), LixError> {
        let record = CommitRecord {
            touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
            format_version: 3,
            commit_id,
            generation: 0,
            parent_commit_ids: Vec::new(),
            first_parent_jump_commit_id: commit_id,
            first_parent_jump_span: 0,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: LixTimestamp::from_unix_millis_utc_lossy(0),
        };
        writes.put(
            COMMIT_SPACE,
            key(commit_id.as_uuid().as_bytes().to_vec()),
            value(crate::changelog::encode_commit_record(&record)?),
        );
        stage_commit_state_manifest(
            writes,
            &fixture_commit_state_manifest(commit_id, mutations.clone()),
        )
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
                    row_pk: &fixture.row_pk,
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
        row_pk: RowPk,
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
                row_pk: self.row_pk.clone(),
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
                row_pk: &key.row_pk,
            });
        }
        let encoded_keys = encoded_keys.finish();
        let mut order = (0..encoded_keys.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|left, right| encoded_keys[*left].cmp(&encoded_keys[*right]));
        let mut canonical = Vec::with_capacity(order.len());
        let mut canonical_ordinal_by_request = vec![0usize; order.len()];
        for request_index in order {
            if canonical
                .last()
                .is_none_or(|previous: &Bytes| previous != &encoded_keys[request_index])
            {
                canonical.push(encoded_keys[request_index].clone());
            }
            canonical_ordinal_by_request[request_index] = canonical.len() - 1;
        }
        let values = load_commit_delta_values_encoded(store, commit_id, &canonical).await?;
        Ok(canonical_ordinal_by_request
            .into_iter()
            .map(|ordinal| values[ordinal].clone())
            .collect())
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
                row_pk: RowPk::single(format!("row-{index:04}")),
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
                row_pk: &fixture.row_pk,
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
            assert_eq!(loaded.row_pk, expected.row_pk);
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
        let direct = super::direct_change_locator(change_id)
            .expect("assigned direct change should retain its physical coordinate");
        assert_eq!(direct.commit_id, authority.commit_id);
        assert!(
            authority.mutations.direct_part_row_counts[direct.segment_index as usize]
                > direct.ordinal
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
        assert_eq!(loaded.row_pk, fixtures[source_index].row_pk);
        let batch = super::load_change_records_by_ids(&read, &[change_id])
            .await
            .expect("direct address batch should read");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0], loaded);
    }

    #[tokio::test]
    async fn direct_change_batch_preserves_multi_owner_duplicates_and_order() {
        let storage = StorageAdapter::new(Memory::new());
        let first_commit = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_1111_0000_0000,
        ));
        let second_commit = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_2222_0000_0000,
        ));
        let fixtures = packed_commit_delta_fixtures();
        let first_deltas = commit_delta_refs(first_commit, &fixtures);
        let second_deltas = commit_delta_refs(second_commit, &fixtures);
        let mut writes = storage.new_write_set();
        let first = stage_addressable_commit_deltas(
            &mut writes,
            &first_deltas,
            &vec![true; first_deltas.len()],
        )
        .expect("first addressable owner should stage");
        let second = stage_addressable_commit_deltas(
            &mut writes,
            &second_deltas,
            &vec![true; second_deltas.len()],
        )
        .expect("second addressable owner should stage");
        assert!(first.locators.is_empty() && second.locators.is_empty());
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("both direct owners should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("multi-owner direct read should open");
        let requested = vec![
            second.assigned_change_ids[1],
            first.assigned_change_ids[0],
            second.assigned_change_ids[1],
            first.assigned_change_ids[fixtures.len() - 1],
            first.assigned_change_ids[0],
        ];
        let records = super::load_change_records_by_ids(&read, &requested)
            .await
            .expect("unordered duplicate direct coordinates should resolve");
        assert_eq!(
            records
                .iter()
                .map(|record| record.change_id)
                .collect::<Vec<_>>(),
            requested
        );
        assert_eq!(records[0], records[2]);
        assert_eq!(records[1], records[4]);
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
            assert_eq!(loaded.row_pk, fixtures[source_index].row_pk);
            assert_eq!(loaded.snapshot.is_none(), fixtures[source_index].deleted);
            let generic_loaded =
                load_change_record_by_id(&generic_read, generic.assigned_change_ids[source_index])
                    .await
                    .expect("direct generic address should read")
                    .expect("direct generic address should resolve");
            assert_eq!(generic_loaded.schema_key, loaded.schema_key);
            assert_eq!(generic_loaded.row_pk, loaded.row_pk);
            assert_eq!(generic_loaded.file_id, loaded.file_id);
            assert_eq!(generic_loaded.snapshot, loaded.snapshot);
            assert_eq!(generic_loaded.metadata, loaded.metadata);
            assert_eq!(generic_loaded.created_at, loaded.created_at);
            assert_eq!(generic_loaded.origin_key, loaded.origin_key);
        }
    }

    #[test]
    fn ordered_change_id_geometry_matches_canonical_512_row_parts() {
        let commit_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_5678_0000_0000,
        ));
        assert_eq!(super::COMMIT_DELTA_SEGMENT_MAX_ROWS, 512);

        let first = super::addressable_change_id(commit_id, 0, 0)
            .expect("first ordered coordinate should encode");
        let last = super::addressable_change_id(commit_id, 0, 511)
            .expect("last coordinate in the first ordered part should encode");
        let next = super::addressable_change_id(commit_id, 1, 0)
            .expect("first coordinate in the second ordered part should encode");

        assert_eq!(
            super::direct_change_locator(first).unwrap().segment_index,
            0
        );
        assert_eq!(super::direct_change_locator(first).unwrap().ordinal, 0);
        assert_eq!(super::direct_change_locator(last).unwrap().segment_index, 0);
        assert_eq!(super::direct_change_locator(last).unwrap().ordinal, 511);
        assert_eq!(super::direct_change_locator(next).unwrap().segment_index, 1);
        assert_eq!(super::direct_change_locator(next).unwrap().ordinal, 0);
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
                row_pk: RowPk::single(format!("row-{index:04}")),
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

        let manifest_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let inventory_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let directory_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let routed_read = ManifestCountingRead {
            inner: storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("routed point read should open"),
            get_many_calls: std::sync::Arc::new(AtomicUsize::new(0)),
            manifest_requests: std::sync::Arc::clone(&manifest_requests),
            inventory_requests: std::sync::Arc::clone(&inventory_requests),
            directory_requests: std::sync::Arc::clone(&directory_requests),
        };
        let routed_indices = (0..33usize)
            .map(|index| index * (ROW_COUNT - 1) / 32)
            .collect::<Vec<_>>();
        let routed_keys = routed_indices
            .iter()
            .map(|&index| TrackedStateKeyRef {
                schema_key: &fixtures[index].schema_key,
                file_id: fixtures[index].file_id.as_deref(),
                row_pk: &fixtures[index].row_pk,
            })
            .collect::<Vec<_>>();
        let routed = super::load_owned_commit_delta_entries_one_ordered_ref(
            &routed_read,
            commit_id,
            &routed_keys,
            None,
        )
        .await
        .expect("hierarchical point routes should load");
        for (&index, entry) in routed_indices.iter().zip(routed) {
            assert_eq!(
                entry.expect("routed mutation should exist").value.change_id,
                assigned[index]
            );
        }
        assert_eq!(manifest_requests.load(Ordering::Relaxed), 1);
        assert_eq!(inventory_requests.load(Ordering::Relaxed), 1);
        assert_eq!(directory_requests.load(Ordering::Relaxed), 1);

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
                assert_eq!(loaded.row_pk, fixtures[row_index].row_pk);
            }
            row_start += usize::from(segment_rows);
        }
        assert_eq!(row_start, ROW_COUNT);
    }

    #[tokio::test]
    async fn address_shaped_not_owned_change_dispatches_to_explicit_locator_authority() {
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
        assert_eq!(loaded.row_pk, explicit.row_pk);
        assert_ne!(loaded.row_pk, address_target.row_pk);
        let batch = super::load_change_records_by_ids(&read, &[explicit_change_id])
            .await
            .expect("explicit collision batch should use its exclusive locator authority");
        assert_eq!(batch, vec![loaded]);
        let canonical = super::load_canonical_change_locator(&read, explicit_change_id)
            .await
            .expect("canonical locator read should succeed")
            .expect("explicit collision should retain a canonical locator");
        assert_eq!(canonical.commit_id, explicit_commit_id);
    }

    #[tokio::test]
    async fn out_of_range_not_owned_change_dispatches_to_explicit_locator_authority() {
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
        assert_eq!(loaded.row_pk, explicit.row_pk);
        assert_eq!(
            super::load_change_records_by_ids(&read, &[explicit_change_id])
                .await
                .expect("out-of-range batch should use its explicit locator authority"),
            vec![loaded],
        );
        let canonical = super::load_canonical_change_locator(&read, explicit_change_id)
            .await
            .expect("canonical locator read should succeed")
            .expect("out-of-range explicit id should retain a canonical locator");
        assert_eq!(canonical.commit_id, explicit_commit_id);
    }

    #[tokio::test]
    async fn direct_claimed_short_part_holes_and_out_of_range_slots_dispatch_explicitly() {
        let storage = StorageAdapter::new(Memory::new());
        let direct_commit = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_5678_0000_0000,
        ));
        let fixtures = (0..513)
            .map(|index| CommitDeltaFixture {
                schema_key: "direct-hole".to_string(),
                file_id: None,
                row_pk: RowPk::single(format!("row-{index:04}")),
                change_id: ChangeId::for_test_label(&format!("direct-hole-{index}")),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(index.into()),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy((index + 1).into()),
            })
            .collect::<Vec<_>>();
        let deltas = fixtures
            .iter()
            .map(|fixture| {
                commit_delta_ref(
                    direct_commit,
                    fixture,
                    crate::json_store::JsonSlotRef::Inline("{}"),
                    crate::json_store::JsonSlotRef::None,
                    None,
                )
            })
            .collect::<Vec<_>>();
        let mut writes = storage.new_write_set();
        let direct_stage = stage_ordered_addressable_commit_deltas(
            &mut writes,
            deltas.iter().copied().map(Ok::<_, LixError>),
            true,
        )
        .expect("direct hole fixture should stage")
        .expect("ordered direct fixture should use the streaming route");
        assert_eq!(
            direct_stage.mutation_inventory().direct_part_row_counts,
            vec![512, 1]
        );
        let hole_change_id = super::addressable_change_id(direct_commit, 1, 1)
            .expect("short-part hole should retain its direct-shaped id");
        let out_of_range_change_id = super::addressable_change_id(direct_commit, 2, 0)
            .expect("out-of-range part should retain its direct-shaped id");
        let explicit_commit = CommitId::for_test_label("direct-hole-explicit");
        let mut explicit_hole = fixtures[0].clone();
        explicit_hole.change_id = hole_change_id;
        let mut explicit_out_of_range = fixtures[1].clone();
        explicit_out_of_range.change_id = out_of_range_change_id;
        let explicit_fixtures = [explicit_hole, explicit_out_of_range];
        let explicit_deltas = explicit_fixtures
            .iter()
            .map(|fixture| {
                commit_delta_ref(
                    explicit_commit,
                    fixture,
                    crate::json_store::JsonSlotRef::Inline("{}"),
                    crate::json_store::JsonSlotRef::None,
                    None,
                )
            })
            .collect::<Vec<_>>();
        let explicit_stage =
            stage_addressable_commit_deltas(&mut writes, &explicit_deltas, &[false, false])
                .expect("explicit collision rows should stage");
        assert_eq!(explicit_stage.locators.len(), 2);
        stage_change_locators(&mut writes, &explicit_stage.locators);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("direct and explicit collision authorities should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("direct collision read should open");
        super::super::mutation_directory::reset_mutation_directory_read_accounting();
        let direct_record = load_change_record_by_id(&read, direct_stage.change_id_at(0).unwrap())
            .await
            .expect("claimed direct row should read")
            .expect("claimed direct row should exist");
        assert_eq!(direct_record.row_pk, fixtures[0].row_pk);
        for (change_id, fixture) in [
            (hole_change_id, &explicit_fixtures[0]),
            (out_of_range_change_id, &explicit_fixtures[1]),
        ] {
            let loaded = load_change_record_by_id(&read, change_id)
                .await
                .expect("unowned direct-shaped collision should dispatch")
                .expect("explicit collision locator should resolve");
            assert_eq!(loaded.row_pk, fixture.row_pk);
            assert_eq!(
                super::load_canonical_change_locator(&read, change_id)
                    .await
                    .expect("explicit collision locator should remain canonical")
                    .expect("explicit collision should have a locator")
                    .commit_id,
                explicit_commit
            );
        }
        let requested = vec![
            hole_change_id,
            direct_stage.change_id_at(0).unwrap(),
            out_of_range_change_id,
            hole_change_id,
        ];
        let (batch_result, invocation_accounting) =
            super::super::mutation_directory::test_read_accounting::scope(
                super::load_change_records_by_ids(&read, &requested),
            )
            .await;
        let batch =
            batch_result.expect("mixed owned and unowned direct-shaped batch should dispatch");
        assert_eq!(
            batch
                .iter()
                .map(|record| record.change_id)
                .collect::<Vec<_>>(),
            requested
        );
        assert!(invocation_accounting.direct_route_calls > 0);
        assert_eq!(invocation_accounting.selector_all_roots, 0);
        assert!(invocation_accounting.selector_direct_calls > 0);
        assert!(invocation_accounting.not_owned_part_index > 0);
        assert!(invocation_accounting.not_owned_local_row > 0);
        assert!(invocation_accounting.explicit_fallback_rows > 0);

        // A claimed direct slot remains authoritative even when its immutable
        // payload is missing: the explicit locator collision must not become a
        // fallback authority for a corruption case.
        drop(read);
        let direct_part_key = super::commit_delta_segment_key_for_part(
            direct_commit,
            0,
            &direct_stage.mutation_inventory().parts[0],
        )
        .expect("direct part key should encode");
        let mut corrupt = storage.new_write_set();
        corrupt.delete_batch(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, [direct_part_key]);
        storage
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .expect("claimed direct payload deletion should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("corrupt collision read should open");
        super::super::mutation_directory::reset_mutation_directory_read_accounting();
        let error = load_change_record_by_id(&read, direct_stage.change_id_at(0).unwrap())
            .await
            .expect_err("claimed payload corruption must not fall back");
        assert!(error.to_string().contains("missing immutable part"));
        assert!(
            super::super::mutation_directory::snapshot_mutation_directory_read_accounting()
                .corruption_outcomes
                > 0
        );
    }

    #[tokio::test]
    async fn compact_replacement_direct_change_id_hydrates_through_authenticated_part() {
        use crate::tracked_state::types::{
            CommitDeltaLifecycleSummary, CommitDeltaReplacementScope,
            TrackedStateSingleStringReplacementRef,
        };

        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_5679_0000_0000,
        ));
        let created_at = LixTimestamp::from_unix_millis_utc_lossy(10);
        let scope = CommitDeltaReplacementScope {
            schema_key: "compact-direct".to_string(),
            file_id: None,
        };
        let generation = super::CommitDeltaReplacementGeneration {
            scope: scope.clone(),
            fallback_commit_id: None,
            lifecycle_summary: CommitDeltaLifecycleSummary {
                scope,
                ordered_identity_digest: [41; 32],
                uniform_created_at: created_at,
            },
        };
        let mut writes = storage.new_write_set();
        let staged = super::stage_ordered_addressable_replacement_parts(
            &mut writes,
            ["compact-000", "compact-001"].into_iter().map(|row_pk| {
                Ok(TrackedStateSingleStringReplacementRef {
                    schema_key: "compact-direct",
                    file_id: None,
                    row_pk,
                    commit_id,
                    created_at,
                    updated_at: created_at,
                    snapshot: crate::json_store::JsonSlotRef::Inline("{\"v\":1}"),
                    metadata: crate::json_store::JsonSlotRef::None,
                })
            }),
            &generation,
        )
        .expect("compact replacement should stage");
        let mut inventory = staged.mutation_inventory().clone();
        // Compact replacement authority stores bounds in the authenticated
        // digest directory, not the generic bounded-part vector.
        inventory.parts.clear();
        stage_fixture_manifest(&mut writes, commit_id, &inventory)
            .expect("compact replacement authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("compact replacement should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("compact replacement read should open");
        super::super::mutation_directory::reset_mutation_directory_read_accounting();
        let change_id = staged
            .change_id_at(1)
            .expect("replacement row should be addressed");
        let (loaded_result, invocation_accounting) =
            super::super::mutation_directory::test_read_accounting::scope(
                load_change_record_by_id(&read, change_id),
            )
            .await;
        let loaded = loaded_result
            .expect("compact direct hydration should succeed")
            .expect("compact direct row should exist");
        assert_eq!(loaded.change_id, change_id);
        assert_eq!(loaded.row_pk, RowPk::single("compact-001"));
        assert_eq!(invocation_accounting.selector_all_roots, 0);
        assert!(invocation_accounting.direct_route_calls > 0);
        assert!(invocation_accounting.selector_direct_calls > 0);
        assert_eq!(invocation_accounting.external_parts_loaded, 1);
        assert_eq!(invocation_accounting.parts_decoded, 1);
        assert_eq!(invocation_accounting.decoded_rows, 2);
        assert!(invocation_accounting.raw_bytes > 0);
        assert!(invocation_accounting.resident_bytes > 0);
    }

    #[test]
    fn change_locator_codec_compacts_sequential_ids_and_round_trips_explicit_ids() {
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
                row_pk: &orphan_fixture.row_pk,
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

        let alias_state = super::load_point_replay_commit_state(&read, alias_commit)
            .await
            .expect("alias replay authority should load")
            .expect("alias replay authority should exist");
        let source_state = super::load_point_replay_commit_state(&read, source_commit)
            .await
            .expect("source replay authority should load")
            .expect("source replay authority should exist");
        let scanned = super::scan_commit_delta_values_from_authenticated_states(
            &read,
            &alias_state,
            Some(&source_state),
            &[],
        )
        .await
        .expect("snapshot-coherent authenticated states should scan");
        assert_eq!(scanned.len(), fixtures.len());
        let error = super::scan_commit_delta_values_from_authenticated_states(
            &read,
            &alias_state,
            Some(&alias_state),
            &[],
        )
        .await
        .expect_err("a cached source for the wrong commit must fail closed");
        assert!(error.to_string().contains("expected authority"));
        let error = super::scan_commit_delta_values_from_authenticated_states(
            &read,
            &alias_state,
            None,
            &[],
        )
        .await
        .expect_err("a missing cached selected source must fail closed");
        assert!(error.to_string().contains("references missing authority"));
        let error = super::scan_commit_delta_values_from_authenticated_states(
            &read,
            &source_state,
            Some(&alias_state),
            &[],
        )
        .await
        .expect_err("a local-only authority must reject an unrelated cached source");
        assert!(error.to_string().contains("local-only authority"));

        let missing_key = TrackedStateKey {
            schema_key: fixtures[0].schema_key.clone(),
            file_id: fixtures[0].file_id.clone(),
            row_pk: RowPk::single("missing-cascade-member"),
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

    #[tokio::test]
    async fn persisted_selected_source_chain_is_rejected_before_hydration() {
        let storage = StorageAdapter::new(Memory::new());
        let source_commit = CommitId::for_test_label("persisted-selected-source-root");
        let first_alias = CommitId::for_test_label("persisted-selected-source-alias");
        let chained_alias = CommitId::for_test_label("persisted-selected-source-chain");
        let mut fixtures = packed_commit_delta_fixtures()
            .into_iter()
            .take(3)
            .collect::<Vec<_>>();
        fixtures[0].change_id = ChangeId::for_test_label("persisted-source-row");
        fixtures[1].change_id = ChangeId::for_test_label("persisted-alias-row");
        fixtures[2].change_id = ChangeId::for_test_label("persisted-chain-row");

        let mut writes = storage.new_write_set();
        let source_delta = commit_delta_ref(
            source_commit,
            &fixtures[0],
            crate::json_store::JsonSlotRef::Inline("{\"source\":true}"),
            crate::json_store::JsonSlotRef::None,
            None,
        );
        stage_commit_deltas(&mut writes, &[source_delta]).expect("source should stage");

        let first_delta = commit_delta_ref(
            first_alias,
            &fixtures[1],
            crate::json_store::JsonSlotRef::Inline("{\"alias\":true}"),
            crate::json_store::JsonSlotRef::None,
            None,
        );
        let first_stage = stage_addressable_commit_deltas_with_selected_source(
            &mut writes,
            &[first_delta],
            &[false],
            source_commit,
        )
        .expect("first selected-source alias should stage");
        stage_change_locators(&mut writes, &first_stage.locators);

        let chained_delta = commit_delta_ref(
            chained_alias,
            &fixtures[2],
            crate::json_store::JsonSlotRef::Inline("{\"chain\":true}"),
            crate::json_store::JsonSlotRef::None,
            None,
        );
        let chained_stage = stage_addressable_commit_deltas_with_selected_source(
            &mut writes,
            &[chained_delta],
            &[false],
            first_alias,
        )
        .expect("chained selected-source alias should stage for corruption fixture");
        stage_change_locators(&mut writes, &chained_stage.locators);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("selected-source chain fixture should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("selected-source chain read should open");
        let error = scan_commit_delta_inventory(&read)
            .await
            .expect_err("persisted selected-source chains must fail closed");
        assert!(
            error
                .to_string()
                .contains("selected-source commit delta chains are unsupported")
        );
    }

    #[test]
    fn selected_source_merge_preserves_columnar_key_arenas() {
        let source_commit = CommitId::for_test_label("columnar-selected-source");
        let alias_commit = CommitId::for_test_label("columnar-selected-alias");
        let timestamp = LixTimestamp::from_unix_millis_utc_lossy(7);
        let mut source = super::DecodedCommitDeltaBatchBuilder::with_capacity(2, 0);
        for identity in ["a", "c"] {
            source
                .push_columnar_row(
                    "columnar-selected-schema",
                    RowPk::single(identity),
                    TrackedStateIndexValue {
                        change_id: ChangeId::for_test_label(&format!("source-{identity}")),
                        commit_id: source_commit,
                        deleted: false,
                        created_at: timestamp,
                        updated_at: timestamp,
                    },
                )
                .expect("source columnar row should append");
        }
        let mut local = super::DecodedCommitDeltaBatchBuilder::with_capacity(2, 0);
        for identity in ["b", "c"] {
            local
                .push_columnar_row(
                    "columnar-selected-schema",
                    RowPk::single(identity),
                    TrackedStateIndexValue {
                        change_id: ChangeId::for_test_label(&format!("local-{identity}")),
                        commit_id: alias_commit,
                        deleted: false,
                        created_at: timestamp,
                        updated_at: timestamp,
                    },
                )
                .expect("local columnar row should append");
        }

        let merged =
            super::merge_selected_source_batches(source.finish(), local.finish(), alias_commit)
                .expect("selected-source merge should preserve both columnar key arenas");
        let rows = decoded_commit_delta_rows(&merged);
        assert_eq!(
            rows.iter()
                .map(|(key, _)| key.row_pk.as_single_string().unwrap())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        assert!(
            rows.iter()
                .all(|(_, value)| value.commit_id == alias_commit)
        );
        assert_eq!(
            rows[2].1.change_id,
            ChangeId::for_test_label("local-c"),
            "local mutations must override selected-source identities"
        );
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
                        row_pk: key.row_pk.clone(),
                    },
                    row.value().clone(),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn large_chunk_batch_stages_two_shared_arenas() {
        let store = StorageAdapter::new(Memory::new())
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open empty snapshot");
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
        overlay
            .stage_chunks(&store, &mut writes, &chunks)
            .await
            .expect("stage chunks against an empty durable store");

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

    /// Regression: a rebuild write set carries both chunk producers.
    ///
    /// `rebuild_commit_root_at` stages a rooted plan's chunks with
    /// `stage_chunks` and then promotes the transient chunks an earlier
    /// rootless plan produced with `stage_selected_chunks`. The tree is
    /// content-addressed, so a re-derived node is the same digest in both
    /// producers; promotion used to stage it as a plain put, which the
    /// write-set validator rejected as a duplicate mutation and which made the
    /// whole rebuild fail with `LIX_STORAGE_ERROR`.
    #[tokio::test]
    async fn promoting_a_chunk_the_write_set_already_staged_is_not_a_duplicate_mutation() {
        let store = StorageAdapter::new(Memory::new())
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open empty snapshot");
        let data = vec![7u8; 96];
        let chunks = PendingChunkBatch::from_parts(
            Bytes::from(data.clone()),
            vec![PendingChunk {
                hash: hash_bytes(&data),
                data_start: 0,
                data_len: data.len(),
            }],
        );
        let hash = chunks.chunks()[0].hash;

        let mut writes = StorageWriteSet::new();
        let mut overlay = TrackedStateChunkOverlay::repairing();
        overlay
            .stage_chunks(&store, &mut writes, &chunks)
            .await
            .expect("rooted plan stages its derived chunks");
        overlay
            .stage_selected_chunks(&store, &mut writes, [hash])
            .await
            .expect("promotion of an already staged digest stages");

        writes
            .validate()
            .expect("a re-derived content-addressed chunk is not a duplicate mutation");
        assert_eq!(
            writes.arena_stats().put_descriptors,
            1,
            "the identical content-addressed entry must be coalesced, not restated"
        );
    }

    /// A file-bounded member scan must return the schema-bounded answer,
    /// restricted.
    ///
    /// The oracle is the **unbounded** read: a scan with no `file_ids` builds a
    /// `schema_key`-only range and cannot execute the narrowing at all, so it is
    /// the answer a caller got before this bound existed. Filtering it in memory
    /// on the same component the range bounds is what the narrowed read must
    /// reproduce, exactly — including for a file id that does not exist, and
    /// including the exclusion of members carrying a null `file_id`, which is
    /// what `change_matches_history_request` does to them anyway.
    ///
    /// The fixture deliberately interleaves two schemas, three files and a
    /// null-file member across enough rows to span several segments, since a
    /// selected segment is decoded whole and the retains — not the ranges — are
    /// what make partial-segment overlap invisible.
    #[tokio::test]
    async fn file_bounded_commit_delta_members_match_the_unbounded_answer() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("file-bounded-delta-commit");
        let files = ["file-a", "file-b", "file-c"];
        let fixtures = (0..300)
            .map(|index: usize| CommitDeltaFixture {
                schema_key: if index % 2 == 0 {
                    "alpha".to_string()
                } else {
                    "beta".to_string()
                },
                // Every fourth member carries no file id at all.
                file_id: if index % 4 == 3 {
                    None
                } else {
                    Some(files[index % files.len()].to_string())
                },
                row_pk: RowPk::single(format!("row-{index:04}")),
                change_id: ChangeId::for_test_label(&format!("file-bounded-change-{index}")),
                deleted: index % 7 == 0,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(index as i64),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(index as i64 + 1),
            })
            .collect::<Vec<_>>();
        let deltas = commit_delta_refs(commit_id, &fixtures);
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("file-bounded deltas should stage");
        drop(deltas);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("file-bounded delta commit should publish");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("file-bounded delta read should open");

        let schemas = ["alpha".to_string(), "beta".to_string()];
        let unbounded = super::load_commit_delta_members_with_payloads_for_schemas(
            &read,
            commit_id,
            &schemas,
            &[],
            usize::MAX,
        )
        .await
        .expect("unbounded schema scan should load")
        .expect("unbounded schema scan should be accepted");
        assert!(
            unbounded.len() > 200,
            "the oracle must be a wide read, not a degenerate one: {}",
            unbounded.len()
        );

        for requested in [
            vec!["file-a".to_string()],
            vec!["file-b".to_string(), "file-c".to_string()],
            vec!["file-a".to_string(), "file-missing".to_string()],
            vec!["file-missing".to_string()],
        ] {
            let expected = unbounded
                .iter()
                .filter(|member| {
                    member
                        .key
                        .file_id
                        .as_deref()
                        .is_some_and(|file_id| requested.iter().any(|want| want == file_id))
                })
                .map(|member| (member.key.clone(), member.change.change_id))
                .collect::<Vec<_>>();
            let actual = super::load_commit_delta_members_with_payloads_for_schemas(
                &read,
                commit_id,
                &schemas,
                &requested,
                usize::MAX,
            )
            .await
            .expect("file-bounded scan should load")
            .expect("file-bounded scan should be accepted")
            .into_iter()
            .map(|member| (member.key, member.change.change_id))
            .collect::<Vec<_>>();
            assert_eq!(
                actual, expected,
                "the file-bounded read must equal the unbounded read restricted to {requested:?}"
            );
        }

        // Engagement, in the same process as the assertions above. Lower bounds
        // only: these counters are process-global, so a concurrent test in this
        // binary can push them up but never down.
        #[cfg(feature = "storage-benches")]
        {
            let _ = crate::storage_bench::take_commit_delta_member_scan_census();
            let _ = super::load_commit_delta_members_with_payloads_for_schemas(
                &read,
                commit_id,
                &schemas,
                &["file-a".to_string()],
                usize::MAX,
            )
            .await
            .expect("file-bounded scan should load");
            let (decoded, _kept, schema_only, file_bounded, ranges) =
                crate::storage_bench::take_commit_delta_member_scan_census();
            assert!(
                file_bounded >= 1 && ranges >= 2,
                "a scan given file ids must take the file-bounded route:                  decoded={decoded} schema_only={schema_only} file_bounded={file_bounded}                  ranges={ranges}"
            );

            let _ = crate::storage_bench::take_commit_delta_member_scan_census();
            let _ = super::load_commit_delta_members_with_payloads_for_schemas(
                &read,
                commit_id,
                &schemas,
                &[],
                usize::MAX,
            )
            .await
            .expect("unbounded scan should load");
            let (_decoded, _kept, schema_only, _file_bounded, _ranges) =
                crate::storage_bench::take_commit_delta_member_scan_census();
            assert!(
                schema_only >= 1,
                "a scan given no file ids must keep the schema-only route"
            );
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
            7,
            "generic history keeps three read-friendly segments plus an atomic header, catalog, directory root, and semantic owner"
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
            row_pk: RowPk::single("not-present"),
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
            &[],
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
                &[],
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
                row_pk: RowPk::single(format!("row-{index:04}")),
                change_id: ChangeId::for_test_label(&format!(
                    "large-payload-packed-delta-change-{index}"
                )),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(index.into()),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy((index + 1).into()),
            })
            .collect::<Vec<_>>();
        let snapshots = (0..fixtures.len())
            .map(|index| format!(r#"{{"id":"row-{index:04}","value":"baseline"}}"#))
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
                row_pk: RowPk::single(format!("row-{index}")),
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
            6,
            "the oversized four-row candidate should become two segments plus an atomic header, catalog, directory root, and semantic owner"
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
                row_pk: &fixture.row_pk,
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
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
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
            columnar_parts: None,
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
                row_pk: RowPk::single(format!("row-{index}")),
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
                    row_pk: &fixture.row_pk,
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
                    row_pk: &fixture.row_pk,
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
                    row_pk: &fixture.row_pk,
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
                row_pk: &fixture.row_pk,
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
                    row_pk: &fixture.row_pk,
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
                row_pk: RowPk::single("large"),
                change_id: ChangeId::for_test_label("indexed-large-change"),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
            },
            CommitDeltaFixture {
                schema_key: "indexed".to_string(),
                file_id: None,
                row_pk: RowPk::single("sparse"),
                change_id: ChangeId::for_test_label("indexed-sparse-change"),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(3),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(4),
            },
            CommitDeltaFixture {
                schema_key: "indexed".to_string(),
                file_id: None,
                row_pk: RowPk::single("tombstone"),
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
                row_pk: &fixture.row_pk,
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
            row_pk: RowPk::single("second-row"),
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
            row_pk: RowPk::single("missing"),
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
    async fn point_replay_coloads_commit_and_state_authorities_in_one_batch() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("co-loaded-point-replay");
        let mutations = CommitStateMutationInventory::default();
        let mut writes = storage.new_write_set();
        stage_fixture_manifest(&mut writes, commit_id, &mutations)
            .expect("commit and physical authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("co-load fixture should commit");

        let get_many_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let manifest_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let inventory_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let read = ManifestCountingRead {
            inner: storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("co-load read should open"),
            get_many_calls: std::sync::Arc::clone(&get_many_calls),
            manifest_requests: std::sync::Arc::clone(&manifest_requests),
            inventory_requests: std::sync::Arc::clone(&inventory_requests),
            directory_requests: std::sync::Arc::new(AtomicUsize::new(0)),
        };
        let (record, state) = super::load_commit_record_and_point_replay_state(&read, commit_id)
            .await
            .expect("co-loaded authorities should decode");

        assert_eq!(
            record.expect("semantic commit should exist").commit_id,
            commit_id
        );
        assert_eq!(
            state.expect("physical state should exist").commit_id,
            commit_id
        );
        assert_eq!(get_many_calls.load(Ordering::Relaxed), 1);
        assert_eq!(manifest_requests.load(Ordering::Relaxed), 1);
        assert_eq!(inventory_requests.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn shallow_point_replay_reads_one_immutable_physical_authority() {
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
        let inventory_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let directory_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let read = ManifestCountingRead {
            inner: storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("point read should open"),
            get_many_calls: std::sync::Arc::new(AtomicUsize::new(0)),
            manifest_requests: std::sync::Arc::clone(&manifest_requests),
            inventory_requests: std::sync::Arc::clone(&inventory_requests),
            directory_requests: std::sync::Arc::clone(&directory_requests),
        };
        let state = super::load_point_replay_commit_state(&read, commit_id)
            .await
            .expect("replay manifest should load")
            .expect("replay manifest should exist");
        let cache = super::CommitDeltaPointReadCache::default();
        super::seed_commit_delta_point_cache_from_replay_manifest(&state, &cache)
            .expect("replay manifest should seed the point cache");
        let encoded_key = Bytes::from(encode_key_ref(TrackedStateKeyRef {
            schema_key: &fixture.schema_key,
            file_id: fixture.file_id.as_deref(),
            row_pk: &fixture.row_pk,
        }));
        let values = super::load_commit_delta_values_encoded_from_replay_manifest(
            &read,
            &state,
            std::slice::from_ref(&encoded_key),
            &cache,
        )
        .await
        .expect("cached shallow point replay should load");
        assert_eq!(values, vec![Some(fixture.value(commit_id))]);
        assert_eq!(manifest_requests.load(Ordering::Relaxed), 1);
        assert_eq!(inventory_requests.load(Ordering::Relaxed), 1);
        assert_eq!(directory_requests.load(Ordering::Relaxed), 0);

        let mut mismatched_state = (*state).clone();
        mismatched_state
            .manifest
            .change_account_id
            .push_str("-mismatch");
        let error = super::load_commit_delta_values_encoded_from_replay_manifest(
            &read,
            &mismatched_state,
            std::slice::from_ref(&encoded_key),
            &cache,
        )
        .await
        .expect_err("cached authenticated authority mismatch must fail closed");
        assert!(
            error
                .to_string()
                .contains("mismatched authenticated authority")
        );
    }

    #[tokio::test]
    async fn warm_point_cache_cannot_bypass_bounded_directory_authority() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("warm-bounded-directory-point-replay");
        let fixtures = packed_commit_delta_fixtures();
        let deltas = commit_delta_refs(commit_id, &fixtures);
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("bounded delta should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("bounded delta should commit");

        let manifest_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let inventory_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let directory_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let read = ManifestCountingRead {
            inner: storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("bounded point read should open"),
            get_many_calls: std::sync::Arc::new(AtomicUsize::new(0)),
            manifest_requests: std::sync::Arc::clone(&manifest_requests),
            inventory_requests: std::sync::Arc::clone(&inventory_requests),
            directory_requests: std::sync::Arc::clone(&directory_requests),
        };
        let state = super::load_point_replay_commit_state(&read, commit_id)
            .await
            .expect("bounded replay authority should load")
            .expect("bounded replay authority should exist");
        assert!(state.mutation_directory_root.as_ref().is_some_and(|root| {
            root.layout == super::super::mutation_directory::LAYOUT_BOUNDED_DIRECT
                || root.layout == super::super::mutation_directory::LAYOUT_BOUNDED_INDIRECT
        }));
        let cache = super::CommitDeltaPointReadCache::default();
        super::seed_commit_delta_point_cache_from_replay_manifest(&state, &cache)
            .expect("bounded replay authority should seed the point cache");
        let encoded_key = Bytes::from(encode_key_ref(TrackedStateKeyRef {
            schema_key: &fixtures[0].schema_key,
            file_id: fixtures[0].file_id.as_deref(),
            row_pk: &fixtures[0].row_pk,
        }));
        for _ in 0..2 {
            let values = super::load_commit_delta_values_encoded_from_replay_manifest(
                &read,
                &state,
                std::slice::from_ref(&encoded_key),
                &cache,
            )
            .await
            .expect("warm bounded point replay should load through the hierarchy");
            assert_eq!(values, vec![Some(fixtures[0].value(commit_id))]);
        }
        assert_eq!(manifest_requests.load(Ordering::Relaxed), 1);
        assert_eq!(inventory_requests.load(Ordering::Relaxed), 1);
        assert!(
            directory_requests.load(Ordering::Relaxed) >= 2,
            "each warm lookup must authenticate the bounded directory"
        );
    }

    #[tokio::test]
    async fn live_membership_cursor_streams_authenticated_bounded_parts() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("bounded-live-membership-cursor");
        let fixtures = packed_commit_delta_fixtures();
        let deltas = commit_delta_refs(commit_id, &fixtures);
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("bounded delta should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("bounded delta should commit");

        let manifest_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let inventory_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let directory_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let read = ManifestCountingRead {
            inner: storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("bounded membership read should open"),
            get_many_calls: std::sync::Arc::new(AtomicUsize::new(0)),
            manifest_requests: std::sync::Arc::clone(&manifest_requests),
            inventory_requests: std::sync::Arc::clone(&inventory_requests),
            directory_requests: std::sync::Arc::clone(&directory_requests),
        };
        let cache = super::CommitDeltaPointReadCache::default();
        let mut cursor = cache.live_membership_cursor(commit_id);
        let encoded = encode_key_ref(TrackedStateKeyRef {
            schema_key: &fixtures[0].schema_key,
            file_id: fixtures[0].file_id.as_deref(),
            row_pk: &fixtures[0].row_pk,
        });
        assert_eq!(
            cursor
                .live_member(&read, &cache, &encoded)
                .await
                .expect("bounded membership should resolve"),
            Some(!fixtures[0].deleted)
        );
        let missing = encode_key_ref(TrackedStateKeyRef {
            schema_key: "zzzz-missing-schema",
            file_id: None,
            row_pk: &RowPk::single("zzzz-missing-identity"),
        });
        assert_eq!(
            cursor
                .live_member(&read, &cache, &missing)
                .await
                .expect("bounded negative membership should resolve"),
            Some(false)
        );
        assert_eq!(manifest_requests.load(Ordering::Relaxed), 1);
        assert_eq!(inventory_requests.load(Ordering::Relaxed), 1);
        assert!(directory_requests.load(Ordering::Relaxed) >= 2);
    }

    #[tokio::test]
    async fn live_membership_cursor_retries_missing_authority_without_poisoning_itself() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("missing-authority read should open");
        let cache = super::CommitDeltaPointReadCache::default();
        let commit_id = CommitId::for_test_label("missing-membership-authority");
        let mut cursor = cache.live_membership_cursor(commit_id);
        let encoded = encode_key_ref(TrackedStateKeyRef {
            schema_key: "missing-membership",
            file_id: None,
            row_pk: &RowPk::single("row"),
        });
        for _ in 0..2 {
            assert_eq!(
                cursor
                    .live_member(&read, &cache, &encoded)
                    .await
                    .expect("missing authority must fail closed without panic"),
                None
            );
        }
    }

    #[tokio::test]
    async fn live_membership_cursor_rejects_corrupt_directory_authority() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("corrupt-live-membership-cursor");
        let fixtures = packed_commit_delta_fixtures();
        let deltas = commit_delta_refs(commit_id, &fixtures);
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("bounded delta should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("bounded delta should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("bounded membership read should open");
        let loaded = super::load_point_replay_commit_state(&read, commit_id)
            .await
            .expect("bounded authority should load")
            .expect("bounded authority should exist");
        let mut state = (*loaded).clone();
        state
            .mutation_directory_root
            .as_mut()
            .expect("bounded authority should have a directory root")
            .root_digest[0] ^= 1;
        let cache = super::CommitDeltaPointReadCache::default();
        cache
            .remember_authority(std::sync::Arc::new(state))
            .expect("test corruption should seed the empty cache");
        let mut cursor = cache.live_membership_cursor(commit_id);
        let encoded = encode_key_ref(TrackedStateKeyRef {
            schema_key: &fixtures[0].schema_key,
            file_id: fixtures[0].file_id.as_deref(),
            row_pk: &fixtures[0].row_pk,
        });
        let error = cursor
            .live_member(&read, &cache, &encoded)
            .await
            .expect_err("corrupt bounded authority must fail closed");
        assert!(
            error.to_string().contains("root is invalid"),
            "unexpected corruption error: {error}"
        );
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
            3,
            "a one-segment commit should remain inline in its atomic header/catalog authority plus semantic owner"
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
            "physical inventory deletion should remove its header and catalog authority"
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
                    row_pk: &alpha.row_pk,
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
                    row_pk: &beta.row_pk,
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
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            selected_source_commit_id: None,
            member_count: 2,
            selection_fingerprint: [0; 32],
            direct_segment_row_counts: Vec::new(),
            single_partition: None,
            lifecycle_summary: None,
            replacement_generation: None,
            replacement_parts: None,
            columnar_parts: None,
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
                row_pk: RowPk::single(format!("boundary-{index:04}")),
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
        assert_eq!(inline_writes.stats().staged_puts, 3);
        storage
            .commit_write_set(inline_writes, StorageWriteOptions::default())
            .await
            .expect("inline boundary deltas should commit");

        let mut indexed_writes = storage.new_write_set();
        let indexed_deltas = commit_delta_refs(indexed_commit_id, &fixtures);
        stage_commit_deltas(&mut indexed_writes, &indexed_deltas)
            .expect("129 generic deltas should use indexed segments");
        assert_eq!(indexed_writes.stats().staged_puts, 6);
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
                row_pk: RowPk::single(format!("row-{index:05}")),
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

    fn commit_state_manifest_fixture() -> CommitStateManifest {
        let commit_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x018f_ffff_1234_7000_8000_0000_ffff_ffff,
        ));
        let schema_key = "manifest-schema";
        let row_pk = RowPk::single("manifest-row");
        let timestamp = LixTimestamp::from_unix_millis_utc_lossy(1234);
        let entry = EncodedLeafEntry {
            key: encode_key_ref(TrackedStateKeyRef {
                schema_key,
                file_id: None,
                row_pk: &row_pk,
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
            change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
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
                columnar_parts: None,
                inline_part: encode_commit_delta_segment(&[entry]),
                parts: Vec::new(),
            },
            touched_scope_filter: Default::default(),
            current_state_scoped_ranges: None,
            snapshot_root: None,
        }
    }

    fn external_commit_state_manifest_fixture(
        label: &str,
        part_count: usize,
    ) -> CommitStateManifest {
        let commit_id = CommitId::for_test_label(label);
        let schema_key = "external-directory".to_string();
        let parts = (0..part_count)
            .map(|index| {
                let first = RowPk::single(format!("row-{:06}", index * 2));
                let last = RowPk::single(format!("row-{:06}", index * 2 + 1));
                FixtureMutationPart {
                    first_key: encode_key_ref(TrackedStateKeyRef {
                        schema_key: &schema_key,
                        file_id: None,
                        row_pk: &first,
                    }),
                    last_key: encode_key_ref(TrackedStateKeyRef {
                        schema_key: &schema_key,
                        file_id: None,
                        row_pk: &last,
                    }),
                    replacement_part: None,
                }
            })
            .collect::<Vec<_>>();
        CommitStateManifest {
            commit_id,
            change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            replay_debt: CommitStateReplayDebt {
                depth: 1,
                rows: part_count as u64,
                bytes: part_count as u64,
            },
            mutations: CommitStateMutationInventory {
                selected_source_commit_id: None,
                member_count: part_count as u32,
                selection_fingerprint: [4; 32],
                direct_part_row_counts: vec![1; part_count],
                replacement_part_digests: Vec::new(),
                single_partition: Some(super::CommitDeltaReplacementScope {
                    schema_key,
                    file_id: None,
                }),
                lifecycle_summary: None,
                replacement_generation: None,
                replacement_parts: None,
                columnar_parts: None,
                inline_part: Vec::new(),
                parts,
            },
            touched_scope_filter: Default::default(),
            current_state_scoped_ranges: None,
            snapshot_root: None,
        }
    }

    /// The point-replay authority cache must never return a manifest the
    /// uncached path would have rejected.
    ///
    /// `AuthenticatedReplayCommitStateManifest` exists so that a freely
    /// constructed manifest cannot claim authoritative catalog coverage, and
    /// `replay_authority_cache` short-circuits exactly the function where the
    /// inventory-digest check, the header validation and the commit-id
    /// cross-check live. That makes "a hit only ever replays a previously
    /// authenticated value" a property to pin, not to argue.
    ///
    /// The test drives the two REAL code paths against each other -
    /// `decode_point_replay_commit_state_values` (cached) versus
    /// `authenticate_point_replay_commit_state_values` (uncached) - so there is
    /// no global switch to flip and nothing for a concurrently running test to
    /// observe.
    #[test]
    fn point_replay_authority_cache_cannot_launder_a_rejected_manifest() {
        let manifest_a = external_commit_state_manifest_fixture("replay-authority-a", 1);
        let manifest_b = external_commit_state_manifest_fixture("replay-authority-b", 2);
        assert_ne!(
            manifest_a.commit_id, manifest_b.commit_id,
            "the fixtures must be distinct commits for the swap cases to mean anything"
        );
        let a = encode_commit_state_manifest(&manifest_a).expect("fixture A encodes");
        let b = encode_commit_state_manifest(&manifest_b).expect("fixture B encodes");

        let mut tampered_header = a.header.clone();
        *tampered_header.last_mut().expect("header is non-empty") ^= 1;
        let mut tampered_inventory = a.mutation_inventory.clone();
        *tampered_inventory
            .last_mut()
            .expect("inventory is non-empty") ^= 1;

        // (label, must_be_accepted, commit id, header bytes, inventory bytes)
        let cases: Vec<(&str, bool, CommitId, Vec<u8>, Vec<u8>)> = vec![
            (
                "legitimate_a",
                true,
                manifest_a.commit_id,
                a.header.clone(),
                a.mutation_inventory.clone(),
            ),
            (
                "legitimate_b",
                true,
                manifest_b.commit_id,
                b.header.clone(),
                b.mutation_inventory.clone(),
            ),
            // B's whole authority served under A's address: the exact confusion
            // the `Authenticated` wrapper exists to stop.
            (
                "identity_swap",
                false,
                manifest_a.commit_id,
                b.header.clone(),
                b.mutation_inventory.clone(),
            ),
            // A's header paired with B's inventory: fails the digest binding.
            (
                "mismatched_pair",
                false,
                manifest_a.commit_id,
                a.header.clone(),
                b.mutation_inventory.clone(),
            ),
            // Right address, right pairing, one flipped byte in each payload.
            (
                "tampered_header",
                false,
                manifest_a.commit_id,
                tampered_header,
                a.mutation_inventory.clone(),
            ),
            (
                "tampered_inventory",
                false,
                manifest_a.commit_id,
                a.header.clone(),
                tampered_inventory,
            ),
        ];

        let projected = |bytes: &[u8]| {
            Some(super::StorageProjectedValue::FullValue(
                Bytes::copy_from_slice(bytes),
            ))
        };

        // 1. Reference outcomes from the uncached authenticate path.
        //    Non-vacuity: every adversarial case must actually be REJECTED
        //    here, or the test proves nothing about laundering.
        let mut reference: Vec<Result<AuthenticatedReplayCommitStateManifest, LixError>> =
            Vec::new();
        for (label, accepted, commit_id, header, inventory) in &cases {
            let outcome = super::authenticate_point_replay_commit_state_values(
                *commit_id,
                &Bytes::copy_from_slice(header),
                &Bytes::copy_from_slice(inventory),
            );
            assert_eq!(
                outcome.is_ok(),
                *accepted,
                "{label}: uncached path did not behave as the case requires; \
                 the laundering check below would be vacuous"
            );
            reference.push(outcome);
        }

        // 2. Warm the cache through the public path with the legitimate
        //    triples only. The adversarial triples are never inserted.
        for (label, accepted, commit_id, header, inventory) in &cases {
            if *accepted {
                super::decode_point_replay_commit_state_values(
                    *commit_id,
                    projected(header),
                    projected(inventory),
                )
                .unwrap_or_else(|error| panic!("{label} must cache cleanly: {error:?}"))
                .expect("a legitimate triple yields an authority");
            }
        }

        // 3. Positive control: the cache must actually be engaged. Without
        //    this, "the adversarial cases are still rejected" is consistent
        //    with the cache never being consulted at all.
        let (hits_before, _, _, _) = super::replay_authority_cache::counters();
        for (label, accepted, commit_id, header, inventory) in &cases {
            if *accepted {
                super::decode_point_replay_commit_state_values(
                    *commit_id,
                    projected(header),
                    projected(inventory),
                )
                .unwrap_or_else(|error| panic!("{label} must still be accepted: {error:?}"))
                .expect("a legitimate triple yields an authority");
            }
        }
        let (hits_after, _, _, _) = super::replay_authority_cache::counters();
        assert!(
            hits_after >= hits_before + 2,
            "the cache was not consulted, so the laundering check is vacuous: \
             {hits_before} -> {hits_after}"
        );

        // 4. The property: with the cache warm, every case - legitimate and
        //    adversarial alike - reaches the same accept/reject decision, and
        //    the same value, as the uncached path.
        for ((label, _, commit_id, header, inventory), expected) in cases.iter().zip(&reference) {
            let cached = super::decode_point_replay_commit_state_values(
                *commit_id,
                projected(header),
                projected(inventory),
            );
            match (expected, &cached) {
                (Ok(expected), Ok(Some(cached))) => assert_eq!(
                    expected, &**cached,
                    "{label}: the cache returned a different authority"
                ),
                (Err(expected), Err(cached)) => assert_eq!(
                    expected.message, cached.message,
                    "{label}: the cache rejected for a different reason"
                ),
                _ => panic!(
                    "{label}: the cache changed the accept/reject decision \
                     (uncached ok={}, cached ok={})",
                    expected.is_ok(),
                    cached.is_ok()
                ),
            }
        }
    }

    #[test]
    fn commit_state_manifest_codec_roundtrips_all_authority_planes() {
        let expected = commit_state_manifest_fixture();
        let encoded = encode_commit_state_manifest(&expected).expect("manifest should encode");
        assert!(
            encoded
                .header
                .starts_with(COMMIT_STATE_MANIFEST_FORMAT_MAGIC)
        );
        assert!(
            encoded
                .mutation_inventory
                .starts_with(super::COMMIT_STATE_MUTATION_INVENTORY_FORMAT_MAGIC)
        );

        let decoded =
            decode_encoded_commit_state_manifest(&encoded).expect("manifest should round trip");
        assert_eq!(decoded, expected);
    }

    #[test]
    fn commit_state_manifest_v10_snapshot_root_decodes_without_a_complete_state_fence() {
        let mut manifest = commit_state_manifest_fixture();
        manifest.replay_debt = CommitStateReplayDebt::default();
        manifest.snapshot_root = Some(Box::new(TrackedStateCommitRoot {
            commit_id: manifest.commit_id,
            root_id: TrackedStateRootId::new([1; 32]),
            parent_roots: Vec::new(),
            changed_key_count: 1,
            row_count_estimate: 1,
            tree_height: 1,
            complete_state_fence: false,
        }));
        let encoded = encode_commit_state_manifest(&manifest).expect("v11 fixture should encode");
        let stored: super::StoredCommitStateManifest = storage_codec::decode(
            "tracked_state v11 commit_state_manifest fixture",
            encoded
                .header
                .strip_prefix(COMMIT_STATE_MANIFEST_FORMAT_MAGIC)
                .expect("fixture has v11 magic"),
        )
        .expect("v11 fixture header should decode");
        let legacy_root = stored.snapshot_root.as_ref().map(|root| {
            Box::new(super::TrackedStateCommitRootV10 {
                commit_id: root.commit_id,
                root_id: root.root_id.clone(),
                parent_roots: root.parent_roots.clone(),
                changed_key_count: root.changed_key_count,
                row_count_estimate: root.row_count_estimate,
                tree_height: root.tree_height,
            })
        });
        let legacy = super::StoredCommitStateManifestV10 {
            commit_id: stored.commit_id,
            change_account_id: stored.change_account_id.clone(),
            replay_debt: stored.replay_debt,
            selected_source_commit_id: stored.selected_source_commit_id,
            mutation_inventory_digest: stored.mutation_inventory_digest,
            mutation_transition_digest: stored.mutation_transition_digest,
            mutation_member_count: stored.mutation_member_count,
            mutation_part_count: stored.mutation_part_count,
            mutation_directory_root: stored.mutation_directory_root.clone(),
            touched_scope_filter: stored.touched_scope_filter.clone(),
            current_state_scoped_ranges: stored.current_state_scoped_ranges.clone(),
            snapshot_root: legacy_root,
        };
        let payload =
            storage_codec::encode("tracked_state v10 commit_state_manifest fixture", &legacy)
                .expect("v10 fixture should encode");
        let mut bytes = super::COMMIT_STATE_MANIFEST_V10_FORMAT_MAGIC.to_vec();
        bytes.extend_from_slice(&payload);

        let decoded = super::decode_stored_commit_state_manifest(&bytes)
            .expect("deployed v10 manifest should remain readable");
        assert_eq!(decoded, stored);
        assert!(!decoded.snapshot_root.unwrap().complete_state_fence);
    }

    #[test]
    fn commit_state_manifest_codec_authenticates_scoped_range_root() {
        let mut manifest = commit_state_manifest_fixture();
        let serving_base_commit_id = None;
        let tree = super::super::scoped_range::ScopedRangeRoot {
            root_id: [7; 32],
            root_digest: [8; 32],
            marker_count: 1,
            part_count: 2,
            row_count: 17,
            tree_height: 1,
        };
        let serving_base_root_id = None;
        let transition_digest = super::super::scoped_current_state::scoped_range_transition_digest(
            manifest.commit_id,
            serving_base_commit_id,
            serving_base_root_id,
            &manifest.mutations,
            &tree,
        )
        .expect("transition should hash");
        manifest.current_state_scoped_ranges =
            Some(Box::new(super::super::types::CurrentStateScopedRangeRoot {
                tree,
                serving_base_commit_id,
                serving_base_root_id,
                transition_digest,
            }));

        let encoded = encode_commit_state_manifest(&manifest).expect("manifest should encode");
        assert_eq!(
            decode_encoded_commit_state_manifest(&encoded).unwrap(),
            manifest
        );

        let mut stored: super::StoredCommitStateManifest = storage_codec::decode(
            "tracked_state commit_state_manifest",
            encoded
                .header
                .strip_prefix(COMMIT_STATE_MANIFEST_FORMAT_MAGIC)
                .expect("encoded header has current magic"),
        )
        .expect("stored header should decode");
        stored
            .current_state_scoped_ranges
            .as_mut()
            .expect("fixture has a scoped root")
            .transition_digest[0] ^= 1;
        let payload = storage_codec::encode("tracked_state commit_state_manifest", &stored)
            .expect("tampered header fixture should encode");
        let mut tampered_header = COMMIT_STATE_MANIFEST_FORMAT_MAGIC.to_vec();
        tampered_header.extend_from_slice(&payload);
        let error = super::decode_stored_commit_state_manifest(&tampered_header)
            .expect_err("header-only scoped authority must fail closed");
        assert!(error.message.contains("transition"));

        manifest
            .current_state_scoped_ranges
            .as_mut()
            .unwrap()
            .transition_digest[0] ^= 1;
        let error = encode_commit_state_manifest(&manifest)
            .expect_err("forged scoped-range transition must fail closed");
        assert!(error.message.contains("transition"));
    }

    #[tokio::test]
    async fn published_topology_loads_only_the_authenticated_header() {
        let manifest = external_commit_state_manifest_fixture("header-only-topology", 700);
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        stage_commit_state_manifest(&mut writes, &manifest).expect("fixture should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("fixture should commit");

        let manifest_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let inventory_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let directory_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let read = ManifestCountingRead {
            inner: storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("topology read should open"),
            get_many_calls: std::sync::Arc::new(AtomicUsize::new(0)),
            manifest_requests: std::sync::Arc::clone(&manifest_requests),
            inventory_requests: std::sync::Arc::clone(&inventory_requests),
            directory_requests: std::sync::Arc::clone(&directory_requests),
        };

        let topology = super::load_published_commit_state_topology(&read, manifest.commit_id)
            .await
            .expect("topology should load")
            .expect("topology should exist");
        assert_eq!(topology.commit_id(), manifest.commit_id);
        assert_eq!(topology.replay_debt(), manifest.replay_debt);
        assert_eq!(
            topology.mutation_member_count(),
            manifest.mutations.member_count
        );
        assert_eq!(manifest_requests.load(Ordering::Relaxed), 1);
        assert_eq!(inventory_requests.load(Ordering::Relaxed), 0);
        assert_eq!(directory_requests.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn commit_state_manifest_codec_rejects_pre_cut_formats() {
        let manifest = commit_state_manifest_fixture();
        let payload = storage_codec::encode("tracked_state commit_state_manifest", &manifest)
            .expect("fixture should encode");
        for magic in [
            b"LXCS3".as_slice(),
            b"LXCS5".as_slice(),
            b"LXCS6".as_slice(),
            b"LXCS7".as_slice(),
            b"LXCS8".as_slice(),
            b"LXCS9".as_slice(),
        ] {
            let mut legacy = magic.to_vec();
            legacy.extend_from_slice(&payload);
            let error = decode_stored_commit_state_authority(&legacy, &[])
                .expect_err("the hard cut must reject pre-v10 manifest formats");
            assert!(error.message.contains("unsupported format"));
        }
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
    async fn split_mutation_authority_publishes_atomically_and_fails_closed() {
        const PART_COUNT: usize = 260;
        let manifest = external_commit_state_manifest_fixture(
            "split-mutation-authority-corruption",
            PART_COUNT,
        );
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        stage_commit_state_manifest(&mut writes, &manifest).expect("split authority should stage");
        assert_eq!(
            writes
                .staged_values_in_space(super::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE)
                .len(),
            1
        );
        assert_eq!(
            writes
                .staged_values_in_space(super::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE)
                .len(),
            1
        );
        assert!(
            writes
                .staged_values_in_space(
                    super::super::mutation_directory::MUTATION_DIRECTORY_NODE_SPACE,
                )
                .len()
                > 1,
            "a multi-level directory must publish all authenticated nodes in the same write set"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("split authority should commit atomically");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("split authority should open");
        assert_eq!(
            load_commit_state_manifest(&read, manifest.commit_id)
                .await
                .expect("complete hierarchy should load"),
            Some(manifest.clone())
        );
        drop(read);

        let mut corrupt = storage.new_write_set();
        corrupt.delete(
            super::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE.mutable_view_for_corruption_test(),
            key(super::commit_mutation_inventory_key(manifest.commit_id)),
        );
        storage
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .expect("test corruption should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("corrupt split authority should open");
        let error = load_commit_state_manifest(&read, manifest.commit_id)
            .await
            .expect_err("a missing catalog must fail closed");
        assert!(
            error
                .to_string()
                .contains("incomplete split physical authority")
        );
        let error = scan_commit_delta_inventory(&read)
            .await
            .expect_err("an orphan header must fail the global inventory");
        assert!(error.to_string().contains("orphaned headers"));
    }

    #[tokio::test]
    async fn mutation_catalog_and_directory_tampering_fail_digest_validation() {
        let manifest =
            external_commit_state_manifest_fixture("mutation-directory-digest-corruption", 260);
        let encoded = encode_commit_state_manifest(&manifest).expect("fixture should encode");
        let root_id = encoded
            .mutation_directory
            .as_ref()
            .expect("external fixture has a directory")
            .root
            .root_id;
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        stage_commit_state_manifest(&mut writes, &manifest).expect("fixture should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("fixture should commit");

        let mut tampered_catalog = encoded.mutation_inventory.clone();
        let last = tampered_catalog
            .last_mut()
            .expect("encoded catalog is non-empty");
        *last ^= 1;
        let mut corrupt = storage.new_write_set();
        corrupt.put(
            super::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE.mutable_view_for_corruption_test(),
            key(super::commit_mutation_inventory_key(manifest.commit_id)),
            value(tampered_catalog),
        );
        storage
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .expect("catalog corruption should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("catalog corruption should open");
        let error = load_commit_state_manifest(&read, manifest.commit_id)
            .await
            .expect_err("tampered catalog must fail closed");
        assert!(error.to_string().contains("authority digest"));
        drop(read);

        let mut repair = storage.new_write_set();
        repair.put(
            super::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE.mutable_view_for_corruption_test(),
            key(super::commit_mutation_inventory_key(manifest.commit_id)),
            value(encoded.mutation_inventory),
        );
        repair.put(
            super::super::mutation_directory::MUTATION_DIRECTORY_NODE_SPACE
                .mutable_view_for_corruption_test(),
            key(root_id.to_vec()),
            value(b"forged-directory-node".to_vec()),
        );
        storage
            .commit_write_set(repair, StorageWriteOptions::default())
            .await
            .expect("node corruption should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("node corruption should open");
        let error = load_commit_state_manifest(&read, manifest.commit_id)
            .await
            .expect_err("tampered directory node must fail closed");
        assert!(error.to_string().contains("content digest mismatch"));
    }

    #[tokio::test]
    async fn manifest_root_does_not_grant_semantic_commit_liveness() {
        let storage = StorageAdapter::new(Memory::new());
        let mut manifest = commit_state_manifest_fixture();
        manifest.replay_debt = CommitStateReplayDebt::default();
        let authoritative = TrackedStateCommitRoot {
            commit_id: manifest.commit_id,
            root_id: TrackedStateRootId::new([1; 32]),
            parent_roots: vec![crate::tracked_state::types::TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("snapshot-parent"),
                root_id: TrackedStateRootId::new([3; 32]),
            }],
            changed_key_count: 1,
            row_count_estimate: 1,
            tree_height: 1,
            complete_state_fence: false,
        };
        let mut writes = storage.new_write_set();
        manifest.snapshot_root = Some(Box::new(authoritative.clone()));
        stage_commit_state_manifest(&mut writes, &manifest).expect("authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("root fixtures should commit");
        let manifest_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let inventory_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let directory_requests = std::sync::Arc::new(AtomicUsize::new(0));
        let read = ManifestCountingRead {
            inner: storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
            get_many_calls: std::sync::Arc::new(AtomicUsize::new(0)),
            manifest_requests: std::sync::Arc::clone(&manifest_requests),
            inventory_requests: std::sync::Arc::clone(&inventory_requests),
            directory_requests: std::sync::Arc::clone(&directory_requests),
        };

        assert_eq!(
            super::load_manifest_snapshot_commit_root(&read, manifest.commit_id)
                .await
                .expect("physical root should load"),
            Some(authoritative.clone())
        );
        assert_eq!(manifest_requests.load(Ordering::Relaxed), 1);
        assert_eq!(inventory_requests.load(Ordering::Relaxed), 0);
        assert_eq!(directory_requests.load(Ordering::Relaxed), 0);

        assert!(
            super::load_snapshot_commit_root(&read, &manifest.commit_id.to_string())
                .await
                .expect("authorized root lookup should succeed")
                .is_none(),
            "physical retention alone must not make a missing semantic commit readable"
        );
        assert_eq!(
            load_commit_state_manifest(&read, manifest.commit_id)
                .await
                .expect("manifest should load"),
            Some(manifest)
        );
    }

    #[tokio::test]
    async fn rootless_manifest_rejects_snapshot_authority() {
        let mut original = commit_state_manifest_fixture();
        original.replay_debt = CommitStateReplayDebt {
            depth: 1,
            rows: 1,
            bytes: 64,
        };
        let rebuilt = TrackedStateCommitRoot {
            commit_id: original.commit_id,
            root_id: TrackedStateRootId::new([9; 32]),
            parent_roots: vec![crate::tracked_state::types::TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("rebuild-parent"),
                root_id: TrackedStateRootId::new([8; 32]),
            }],
            changed_key_count: 1,
            row_count_estimate: 1,
            tree_height: 1,
            complete_state_fence: false,
        };
        original.snapshot_root = Some(Box::new(rebuilt));
        let error = encode_commit_state_manifest(&original)
            .expect_err("rootless immutable authority must not accept a snapshot root");
        assert!(error.message.contains("rootless"));
    }

    #[test]
    fn rooted_manifest_requires_immutable_snapshot_authority() {
        let mut manifest = commit_state_manifest_fixture();
        manifest.replay_debt = CommitStateReplayDebt::default();

        let error = encode_commit_state_manifest(&manifest)
            .expect_err("zero replay debt must carry canonical snapshot authority");
        assert!(error.message.contains("missing its snapshot root"));
    }

    #[test]
    fn commit_state_direct_change_encoding_preserves_holes_and_other_commits() {
        let manifest = commit_state_manifest_fixture();
        let first = super::change_id_from_packed_address(manifest.commit_id, 1);
        let hole = super::change_id_from_packed_address(manifest.commit_id, 2);
        let other = super::change_id_from_packed_address(
            CommitId::with_change_address_space(uuid::Uuid::from_u128(
                0x018f_ffff_1234_7000_8000_0001_ffff_ffff,
            )),
            1,
        );

        let locator =
            super::direct_change_locator(first).expect("first direct address should decode");
        assert_eq!(locator.segment_index, 0);
        assert_eq!(locator.ordinal, 0);
        let hole =
            super::direct_change_locator(hole).expect("physical hole remains address-shaped");
        assert!(
            manifest.mutations.direct_part_row_counts[hole.segment_index as usize] <= hole.ordinal
        );
        assert_ne!(
            super::direct_change_locator(other)
                .expect("other commit address should decode")
                .commit_id,
            manifest.commit_id
        );
    }

    #[test]
    fn commit_state_manifest_rejects_replay_drift() {
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

        let mut invalid_debt = commit_state_manifest_fixture();
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
                uniform_updated_at: LixTimestamp::from_unix_millis_utc_lossy(1234),
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
        let error = decode_stored_commit_state_authority(b"LXCS0old", &[])
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

/// Process-wide, content-keyed cache for the point-replay authority.
///
/// # Why this exists
///
/// One single-row INSERT walks the first-parent replay chain and decodes ~11
/// commit-state manifests, ~11 mutation inventories and ~11 commit records.
/// Within a single write those really are distinct records, so the
/// transaction-scoped [`CommitDeltaPointReadCache`] cannot see any reuse. The
/// reuse is *across* transactions: over 50 consecutive single-row writes the
/// manifest is decoded 1265 times from 66 distinct payloads (19.2x), the
/// inventory 1049 times from 66 (15.9x), and the commit record 1154 from 68
/// (17.0x). This cache is scoped to match where the reuse actually is.
///
/// # Why it is safe
///
/// An entry is only returned when the commit id **and** both payloads compare
/// byte-for-byte equal to the ones that produced it. Those exact bytes were
/// authenticated by
/// [`authenticate_point_replay_commit_state_values`] on insert - the
/// inventory-digest check, the header validation and the commit-id cross-check
/// all ran over them. A hit therefore cannot return anything the uncached path
/// would have rejected, and cannot cross repositories or observe a rewritten
/// value at the same address. `point_replay_authority_cache_cannot_launder_a_
/// rejected_manifest` pins that property against the adversarial cases the
/// `Authenticated` wrapper exists to stop.
///
/// The commit id is the hash bucket only; it is never the authority. This
/// mirrors [`DecodedCommitDeltaCache`], which keys on a digest and still
/// compares the encoded bytes before returning a hit.
pub(crate) mod replay_authority_cache {
    use super::{
        Arc, AuthenticatedReplayCommitStateManifest, Bytes, CommitId, CommitRecord, LixError,
        storage_codec,
    };
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, OnceLock};

    /// Entries per cache. Two caches, so the ceiling is 2 x this many decoded
    /// records plus their encoded payloads.
    const CAPACITY: usize = 4096;

    /// Unconditional so that a test can prove the cache was consulted rather
    /// than assume it. Two relaxed atomics on a path that otherwise decodes
    /// and validates a record.
    pub(crate) static AUTHORITY_HITS: AtomicU64 = AtomicU64::new(0);
    pub(crate) static AUTHORITY_MISSES: AtomicU64 = AtomicU64::new(0);
    pub(crate) static COMMIT_RECORD_HITS: AtomicU64 = AtomicU64::new(0);
    pub(crate) static COMMIT_RECORD_MISSES: AtomicU64 = AtomicU64::new(0);

    /// (authority hits, authority misses, commit-record hits, commit-record misses)
    pub(crate) fn counters() -> (u64, u64, u64, u64) {
        (
            AUTHORITY_HITS.load(Ordering::Relaxed),
            AUTHORITY_MISSES.load(Ordering::Relaxed),
            COMMIT_RECORD_HITS.load(Ordering::Relaxed),
            COMMIT_RECORD_MISSES.load(Ordering::Relaxed),
        )
    }

    /// Bucket key only. Never the authority for a hit - see the module docs.
    fn bucket(commit_id: CommitId) -> u64 {
        let bytes = commit_id.as_uuid().as_bytes();
        let mut lo = [0u8; 8];
        let mut hi = [0u8; 8];
        lo.copy_from_slice(&bytes[..8]);
        hi.copy_from_slice(&bytes[8..]);
        u64::from_le_bytes(lo) ^ u64::from_le_bytes(hi).rotate_left(17)
    }

    struct Slot<T> {
        commit_id: CommitId,
        first: Bytes,
        second: Option<Bytes>,
        value: T,
    }

    struct Lru<T> {
        buckets: HashMap<u64, Vec<Slot<T>>>,
        order: VecDeque<u64>,
    }

    impl<T> Default for Lru<T> {
        fn default() -> Self {
            Self {
                buckets: HashMap::new(),
                order: VecDeque::new(),
            }
        }
    }

    impl<T: Clone> Lru<T> {
        fn get(&self, commit_id: CommitId, first: &Bytes, second: Option<&Bytes>) -> Option<T> {
            self.buckets
                .get(&bucket(commit_id))?
                .iter()
                .find_map(|slot| {
                    (slot.commit_id == commit_id
                        && slot.first.as_ref() == first.as_ref()
                        && slot.second.as_deref() == second.map(Bytes::as_ref))
                    .then(|| slot.value.clone())
                })
        }

        fn insert(
            &mut self,
            commit_id: CommitId,
            first: &Bytes,
            second: Option<&Bytes>,
            value: &T,
        ) {
            let key = bucket(commit_id);
            self.buckets.entry(key).or_default().push(Slot {
                commit_id,
                first: first.clone(),
                second: second.cloned(),
                value: value.clone(),
            });
            self.order.push_back(key);
            while self.order.len() > CAPACITY {
                let Some(evicted) = self.order.pop_front() else {
                    break;
                };
                if let Some(slots) = self.buckets.get_mut(&evicted) {
                    if !slots.is_empty() {
                        slots.remove(0);
                    }
                    if slots.is_empty() {
                        self.buckets.remove(&evicted);
                    }
                }
            }
        }
    }

    fn authorities() -> &'static Mutex<Lru<Arc<AuthenticatedReplayCommitStateManifest>>> {
        static CACHE: OnceLock<Mutex<Lru<Arc<AuthenticatedReplayCommitStateManifest>>>> =
            OnceLock::new();
        CACHE.get_or_init(|| Mutex::new(Lru::default()))
    }

    fn commit_records() -> &'static Mutex<Lru<CommitRecord>> {
        static CACHE: OnceLock<Mutex<Lru<CommitRecord>>> = OnceLock::new();
        CACHE.get_or_init(|| Mutex::new(Lru::default()))
    }

    pub(super) fn get(
        commit_id: CommitId,
        header: &Bytes,
        inventory: &Bytes,
    ) -> Option<Arc<AuthenticatedReplayCommitStateManifest>> {
        let hit = authorities()
            .lock()
            .ok()
            .and_then(|guard| guard.get(commit_id, header, Some(inventory)));
        if hit.is_some() {
            AUTHORITY_HITS.fetch_add(1, Ordering::Relaxed);
        } else {
            AUTHORITY_MISSES.fetch_add(1, Ordering::Relaxed);
        }
        hit
    }

    pub(super) fn insert(
        commit_id: CommitId,
        header: &Bytes,
        inventory: &Bytes,
        value: &Arc<AuthenticatedReplayCommitStateManifest>,
    ) {
        if let Ok(mut guard) = authorities().lock() {
            guard.insert(commit_id, header, Some(inventory), value);
        }
    }

    /// Commit records are decoded on the same replay step as the authority and
    /// are re-decoded at the same rate, so they share this cache's scope.
    pub(super) fn decode_commit_record(
        commit_id: CommitId,
        bytes: &Bytes,
    ) -> Result<CommitRecord, LixError> {
        if let Some(hit) = commit_records()
            .lock()
            .ok()
            .and_then(|guard| guard.get(commit_id, bytes, None))
        {
            COMMIT_RECORD_HITS.fetch_add(1, Ordering::Relaxed);
            return Ok(hit);
        }
        COMMIT_RECORD_MISSES.fetch_add(1, Ordering::Relaxed);
        let value = storage_codec::decode::<CommitRecord>("commit record", bytes)?;
        // The caller cross-checks `record.commit_id` against `commit_id`
        // immediately after this returns, so a record cached under a mismatched
        // address is rejected on the miss path before it can ever be inserted.
        if value.commit_id == commit_id
            && let Ok(mut guard) = commit_records().lock()
        {
            guard.insert(commit_id, bytes, None, &value);
        }
        Ok(value)
    }
}
