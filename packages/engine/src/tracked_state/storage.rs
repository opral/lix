#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cmp_owned
)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;

use crate::changelog::CommitId;
use crate::common::SharedStr;
use crate::storage_adapter::{
    BufferRange, EncodedMutationBatch, EncodedPut, PointReadPlan, ScanPlan, StorageAdapterRead,
    StorageCoreProjection, StorageError, StorageGetManyRequest, StorageGetManyResult,
    StorageGetOptions, StorageKey, StorageKeyRange, StorageProjectedValue, StorageScanChunk,
    StorageScanOptions, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::tracked_state::codec::{
    DecodedLeafNodeRef, DecodedNodeRef, EncodedLeafEntry, PendingChunkBatch,
    TrackedStateMutationBatchBuilder, decode_key, decode_key_shared, decode_node_ref, decode_value,
    encode_key_ref, encode_leaf_node, encode_schema_key_prefix,
};
use crate::tracked_state::types::{
    TRACKED_STATE_HASH_BYTES, TrackedStateCommitDeltaRef, TrackedStateCommitRoot,
    TrackedStateIndexValue, TrackedStateIndexValueRef, TrackedStateKey, TrackedStateKeyRef,
    TrackedStateRootId,
};
use crate::{LixError, storage_codec};
use bytes::Bytes;

pub(crate) const TRACKED_STATE_TREE_CHUNK_NAMESPACE: &str = "tracked_state.tree_chunk";
pub(crate) const TRACKED_STATE_COMMIT_ROOT_NAMESPACE: &str = "tracked_state.commit_root";
pub(crate) const TRACKED_STATE_COMMIT_DELTA_MANIFEST_NAMESPACE: &str =
    "tracked_state.commit_delta_manifest.v2";
pub(crate) const TRACKED_STATE_COMMIT_DELTA_SEGMENT_NAMESPACE: &str =
    "tracked_state.commit_delta_segment.v2";
pub(crate) const TRACKED_STATE_CHANGE_LOCATOR_NAMESPACE: &str = "tracked_state.change_locator.v1";
pub(crate) const TRACKED_STATE_TREE_CHUNK_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_0001),
    TRACKED_STATE_TREE_CHUNK_NAMESPACE,
);
pub(crate) const TRACKED_STATE_COMMIT_ROOT_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_0004),
    TRACKED_STATE_COMMIT_ROOT_NAMESPACE,
);
/// One commit-addressed directory for bounded packed tracked deltas.
///
/// Immutable roots are sparse checkpoints. The manifest maps an identity to
/// one small front-coded segment, avoiding both one RocksDB key per mutation
/// on writes and full-commit hydration for historical point replay.
pub(crate) const TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_0019),
    TRACKED_STATE_COMMIT_DELTA_MANIFEST_NAMESPACE,
);
pub(crate) const TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_001a),
    TRACKED_STATE_COMMIT_DELTA_SEGMENT_NAMESPACE,
);
pub(crate) const TRACKED_STATE_CHANGE_LOCATOR_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_001b),
    TRACKED_STATE_CHANGE_LOCATOR_NAMESPACE,
);

const COMMIT_DELTA_SEGMENT_MAX_ROWS: usize = 128;
const COMMIT_DELTA_SEGMENT_TARGET_BYTES: usize = 28 * 1024;
const COMMIT_DELTA_FORMAT_MAGIC: &[u8] = b"LXCD5";
const COMMIT_DELTA_PAYLOAD_OFFSET_BYTES: usize = size_of::<u32>();

#[derive(Clone, Copy, musli::Encode)]
#[musli(packed)]
struct CommitDeltaPayloadRef<'a> {
    #[musli(with = crate::json_store::json_slot_storage_ref)]
    snapshot: crate::json_store::JsonSlotRef<'a>,
    #[musli(with = crate::json_store::json_slot_storage_ref)]
    metadata: crate::json_store::JsonSlotRef<'a>,
    #[musli(with = storage_codec::option)]
    origin_key: Option<&'a str>,
}

#[derive(Debug, musli::Decode)]
#[musli(packed)]
struct CommitDeltaPayload {
    #[musli(with = crate::json_store::json_slot_storage)]
    snapshot: crate::json_store::JsonSlot,
    #[musli(with = crate::json_store::json_slot_storage)]
    metadata: crate::json_store::JsonSlot,
    #[musli(with = storage_codec::option)]
    origin_key: Option<String>,
}

/// Borrowed fixed-width directory over independently encoded payload records.
///
/// A pair of equal offsets means that the corresponding identity has no
/// authoritative payload. Non-empty ranges contain exactly one musli-encoded
/// [`CommitDeltaPayload`], so a point lookup decodes only the requested row
/// instead of reconstructing every payload in the segment.
#[derive(Debug, Clone, Copy)]
struct CommitDeltaPayloadIndexRef<'a> {
    offsets: &'a [u8],
    payload_bytes: &'a [u8],
    entry_count: usize,
}

impl<'a> CommitDeltaPayloadIndexRef<'a> {
    #[cfg(test)]
    fn len(self) -> usize {
        self.entry_count
    }

    fn decode(self, entry_index: usize) -> Result<CommitDeltaPayload, LixError> {
        let range = self.payload_range(entry_index)?;
        if range.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta member is missing its authoritative payload",
            ));
        }
        let payload: CommitDeltaPayload =
            storage_codec::decode("tracked_state indexed commit_delta payload", range)?;
        Ok(payload)
    }

    fn payload_range(self, entry_index: usize) -> Result<&'a [u8], LixError> {
        if entry_index >= self.entry_count {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta payload index is out of bounds",
            ));
        }
        let start = self.offset(entry_index)?;
        let end = self.offset(entry_index + 1)?;
        self.payload_bytes.get(start..end).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta payload range is out of bounds",
            )
        })
    }

    fn offset(self, offset_index: usize) -> Result<usize, LixError> {
        let byte_start = offset_index
            .checked_mul(COMMIT_DELTA_PAYLOAD_OFFSET_BYTES)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_delta payload directory overflows",
                )
            })?;
        let bytes = self
            .offsets
            .get(byte_start..byte_start + COMMIT_DELTA_PAYLOAD_OFFSET_BYTES)
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

pub(crate) struct LoadedCommitDeltaEntry {
    #[cfg(test)]
    pub(crate) value: TrackedStateIndexValue,
    pub(crate) change_record: crate::changelog::ChangeRecord,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CommitDeltaChangeLocator {
    pub(crate) change_id: crate::changelog::ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) segment_index: u32,
    pub(crate) ordinal: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitDeltaMember {
    pub(crate) key: TrackedStateKey,
    pub(crate) value: TrackedStateIndexValue,
    pub(crate) change: crate::changelog::ChangeRecord,
    pub(crate) segment_index: u32,
    pub(crate) ordinal: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitDeltaInventoryEntry {
    pub(crate) members: Vec<CommitDeltaMember>,
    pub(crate) segment_count: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CommitDeltaInventory {
    pub(crate) commits: BTreeMap<CommitId, CommitDeltaInventoryEntry>,
}

struct CommitDeltaPlane {
    manifests: BTreeMap<CommitId, CommitDeltaManifest>,
    segments: BTreeMap<CommitId, BTreeMap<usize, Bytes>>,
}

// Version the root metadata independently of storage backends. Version 3 is a
// hard cut for derived commit rows, prefix-friendly keys, and compact tree
// nodes. Reject older roots before their differently ordered state can be
// inherited or traversed.
const TRACKED_STATE_COMMIT_ROOT_MAGIC: &[u8] = b"LXTR3";

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct CommitDeltaManifest {
    /// A complete leaf payload for a commit that fits in one segment. Keeping
    /// it in the directory preserves the one-record shape of tiny commits;
    /// larger commits use the indexed segment list below.
    #[musli(bytes)]
    inline_segment: Vec<u8>,
    segments: Vec<CommitDeltaSegmentBounds>,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct CommitDeltaSegmentBounds {
    #[musli(bytes)]
    first_key: Vec<u8>,
    #[musli(bytes)]
    last_key: Vec<u8>,
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

impl DecodedCommitDeltaRowRef<'_> {
    pub(crate) fn key_ref(&self) -> TrackedStateKeyRef<'_> {
        let row = &self.batch.rows[self.ordinal];
        TrackedStateKeyRef {
            schema_key: self.batch.schema_keys[row.schema_key_ordinal as usize].as_str(),
            file_id: (row.file_id_ordinal != u32::MAX)
                .then(|| self.batch.file_ids[row.file_id_ordinal as usize].as_str()),
            entity_pk: &row.entity_pk,
        }
    }

    pub(crate) fn value(&self) -> &TrackedStateIndexValue {
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
    Ok(load_commit_root(store, commit_id)
        .await?
        .map(|metadata| metadata.root_id))
}

/// Commit-root keys are the raw 16 UUID bytes of the commit id; binary
/// UUIDv7 order matches the former hyphenated-text key order.
fn commit_root_key(commit_id: CommitId) -> Vec<u8> {
    commit_id.as_uuid().as_bytes().to_vec()
}

fn commit_delta_manifest_key(commit_id: CommitId) -> Vec<u8> {
    commit_id.as_uuid().as_bytes().to_vec()
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

pub(crate) async fn load_commit_root(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: &str,
) -> Result<Option<TrackedStateCommitRoot>, LixError> {
    // parse_lix canonicalizes test labels to the same synthetic UUID the
    // staging path produces, so label-keyed test fixtures keep matching.
    let typed_commit_id = CommitId::parse_lix(commit_id, "tracked-state commit root lookup")?;
    let Some(bytes) = get_one(
        store,
        TRACKED_STATE_COMMIT_ROOT_SPACE,
        commit_root_key(typed_commit_id),
    )
    .await?
    else {
        return Ok(None);
    };
    let metadata = decode_commit_root(&bytes)?;
    if metadata.commit_id != typed_commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_root key for commit '{commit_id}' contains root metadata for commit '{}'",
                metadata.commit_id
            ),
        ));
    }
    Ok(Some(metadata))
}

pub(crate) fn stage_commit_root(
    writes: &mut StorageWriteSet,
    metadata: &TrackedStateCommitRoot,
) -> Result<(), LixError> {
    writes.put(
        TRACKED_STATE_COMMIT_ROOT_SPACE,
        key(commit_root_key(metadata.commit_id)),
        value(encode_commit_root(metadata)?),
    );
    Ok(())
}

pub(crate) fn stage_delete_commit_root(writes: &mut StorageWriteSet, commit_id: CommitId) {
    writes.delete(
        TRACKED_STATE_COMMIT_ROOT_SPACE,
        key(commit_root_key(commit_id)),
    );
}

/// Stages all tracked mutations for one immutable commit as bounded, sorted
/// front-coded segments plus one tiny directory. A full commit no longer
/// writes one backend key for every affected identity.
pub(crate) fn stage_commit_deltas(
    writes: &mut StorageWriteSet,
    deltas: &[TrackedStateCommitDeltaRef<'_>],
) -> Result<Vec<CommitDeltaChangeLocator>, LixError> {
    let Some(&commit_id) = deltas.first().map(|delta| &delta.delta.commit_id) else {
        return Ok(Vec::new());
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
        .map(|(mutation, payload)| {
            (
                EncodedLeafEntry {
                    key: mutation.encoded_key,
                    value: mutation.encoded_value,
                },
                payload,
            )
        })
        .collect::<Vec<_>>();
    pending.sort_unstable_by(|left, right| left.0.key.cmp(&right.0.key));
    let (entries, payloads): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
    if entries.windows(2).any(|pair| pair[0].key == pair[1].key) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_delta for commit '{commit_id}' contains duplicate identities"
            ),
        ));
    }

    let mut encoded_segments = Vec::new();
    let mut segment_start = 0usize;
    while segment_start < entries.len() {
        let mut segment_end = (segment_start + COMMIT_DELTA_SEGMENT_MAX_ROWS).min(entries.len());
        let mut encoded = encode_commit_delta_segment_with_payloads(
            &entries[segment_start..segment_end],
            &payloads[segment_start..segment_end],
        );
        while encoded.len() > COMMIT_DELTA_SEGMENT_TARGET_BYTES && segment_end - segment_start > 1 {
            segment_end = segment_start + (segment_end - segment_start).div_ceil(2);
            encoded = encode_commit_delta_segment_with_payloads(
                &entries[segment_start..segment_end],
                &payloads[segment_start..segment_end],
            );
        }
        encoded_segments.push((segment_start..segment_end, encoded));
        segment_start = segment_end;
    }
    let segment_count = encoded_segments.len();
    writes.reserve_space(TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE, 1, 0);
    if segment_count == 1 {
        let (_, inline_segment) = encoded_segments
            .pop()
            .expect("non-empty commit delta has one encoded segment");
        writes.put(
            TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE,
            key(commit_delta_manifest_key(commit_id)),
            value(encode_commit_delta_manifest(&CommitDeltaManifest {
                inline_segment,
                segments: Vec::new(),
            })?),
        );
        return commit_delta_change_locators(commit_id, 0, &entries);
    }
    writes.reserve_space(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, segment_count, 0);
    let mut manifest = CommitDeltaManifest {
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
        });
        writes.put(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            key(commit_delta_segment_key(commit_id, segment_index)?),
            value(encoded),
        );
        locators.extend(commit_delta_change_locators(
            commit_id,
            segment_index,
            segment_entries,
        )?);
    }
    writes.put(
        TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE,
        key(commit_delta_manifest_key(commit_id)),
        value(encode_commit_delta_manifest(&manifest)?),
    );
    Ok(locators)
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
            let ordinal = u8::try_from(ordinal).expect("commit-delta segment row count fits u8");
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
    for change_id in change_ids {
        writes.delete(
            TRACKED_STATE_CHANGE_LOCATOR_SPACE,
            key(change_id.as_uuid().as_bytes().to_vec()),
        );
    }
}

pub(crate) async fn load_change_record_by_id(
    store: &(impl StorageAdapterRead + ?Sized),
    change_id: crate::changelog::ChangeId,
) -> Result<Option<crate::changelog::ChangeRecord>, LixError> {
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
    let locator = decode_change_locator(change_id, &locator)?;
    let Some(manifest) = load_commit_delta_manifest(store, locator.commit_id).await? else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state change locator for '{change_id}' references missing commit '{}'",
                locator.commit_id
            ),
        ));
    };
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
        let segment_key = StorageKey(Bytes::from(commit_delta_segment_key(
            locator.commit_id,
            segment_index,
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
    let (leaf, payloads) = decode_commit_delta_with_payloads(&segment, bounds)?;
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
    let payload = payloads.decode(ordinal)?;
    Ok(Some(crate::changelog::ChangeRecord {
        format_version: 2,
        change_id,
        schema_key: key.schema_key,
        entity_pk: key.entity_pk,
        file_id: key.file_id,
        snapshot: payload.snapshot,
        metadata: payload.metadata,
        created_at: value.updated_at,
        origin_key: payload.origin_key,
    }))
}

fn decode_change_locator(
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
    let ordinal = u8::try_from(packed_ordinal % COMMIT_DELTA_SEGMENT_MAX_ROWS as u64)
        .expect("segment remainder fits u8");
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
pub(crate) async fn load_commit_delta_values_encoded(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    encoded_keys: &[Bytes],
) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
    if encoded_keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut values = vec![None; encoded_keys.len()];
    let Some(manifest) = load_commit_delta_manifest(store, commit_id).await? else {
        return Ok(values);
    };
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
        if let Some(segment_index) = commit_delta_segment_for_key(&manifest, encoded_key) {
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
            commit_delta_segment_key(commit_id, segment_index)
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
/// record, and duplicate change ids are corruption even when their identities
/// differ.
pub(crate) async fn load_commit_delta_members_with_payloads(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Vec<CommitDeltaMember>, LixError> {
    let Some(manifest) = load_commit_delta_manifest(store, commit_id).await? else {
        return Ok(Vec::new());
    };
    let mut members = Vec::new();
    if let Some(inline_segment) = manifest.inline_segment() {
        collect_strict_commit_delta_members(inline_segment, None, commit_id, 0, &mut members)?;
    } else {
        let segment_keys = (0..manifest.segments.len())
            .map(|segment_index| {
                commit_delta_segment_key(commit_id, segment_index)
                    .map(|key| StorageKey(Bytes::from(key)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let segments = PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &segment_keys)
            .materialize(store, StorageGetOptions::default())
            .await?;
        for (segment_index, value) in segments.value.into_iter().enumerate() {
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
    validate_commit_delta_member_order_and_ids(commit_id, &members)?;
    Ok(members)
}

pub(crate) async fn scan_commit_delta_members(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Vec<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
    let batch = scan_commit_delta_values(store, commit_id, &[]).await?;
    let mut members = Vec::with_capacity(batch.len());
    let mut change_ids = BTreeSet::new();
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
        if !change_ids.insert(value.change_id) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit_delta for commit '{commit_id}' contains duplicate change id '{}'",
                    value.change_id
                ),
            ));
        }
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
    if requests.is_empty() {
        return Ok(Vec::new());
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
    let manifest_keys = commit_ids
        .iter()
        .map(|commit_id| StorageKey(Bytes::from(commit_delta_manifest_key(*commit_id))))
        .collect::<Vec<_>>();
    let manifest_values =
        PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE, &manifest_keys)
            .materialize(store, StorageGetOptions::default())
            .await?;

    let mut output = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
    let mut segmented_manifests = BTreeMap::<CommitId, CommitDeltaManifest>::new();
    let mut lookups_by_segment = BTreeMap::<(CommitId, usize), Vec<(usize, Vec<u8>)>>::new();

    for (commit_id, manifest_value) in commit_ids.into_iter().zip(manifest_values.value) {
        let Some(bytes) = manifest_value.and_then(full_value_bytes) else {
            continue;
        };
        let manifest = decode_commit_delta_manifest(&bytes)?;
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
        return Ok(output);
    }

    let segment_routes = lookups_by_segment.keys().copied().collect::<Vec<_>>();
    let segment_keys = segment_routes
        .iter()
        .map(|&(commit_id, segment_index)| {
            commit_delta_segment_key(commit_id, segment_index)
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
    Ok(output)
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
    let Some(manifest) = load_commit_delta_manifest(store, commit_id).await? else {
        return Ok(DecodedCommitDeltaBatch::default());
    };
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
    let segment_indices = commit_delta_segments_for_schemas(&manifest, &requested_schemas);
    if segment_indices.is_empty() {
        return Ok(DecodedCommitDeltaBatch::default());
    }
    let storage_keys = segment_indices
        .iter()
        .map(|&segment_index| {
            commit_delta_segment_key(commit_id, segment_index)
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

/// Scans every authoritative tracked change packed into immutable commit
/// deltas, deduplicating checkpoint and merge selections by change id.
///
/// `lix_change` is an unscoped durable-fact surface, so this is its packed
/// tracked counterpart to the point-addressed CHANGE_SPACE scan.
pub(crate) async fn scan_change_records_from_commit_deltas(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<Vec<crate::changelog::ChangeRecord>, LixError> {
    let CommitDeltaPlane {
        manifests,
        mut segments,
    } = scan_commit_delta_plane(store).await?;
    let mut records =
        BTreeMap::<crate::changelog::ChangeId, (CommitId, crate::changelog::ChangeRecord)>::new();
    for (commit_id, manifest) in manifests {
        let physical_segments = segments.remove(&commit_id).unwrap_or_default();
        if let Some(inline_segment) = manifest.inline_segment() {
            if !physical_segments.is_empty() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state inline commit_delta for commit '{commit_id}' has external segments"
                    ),
                ));
            }
            collect_validated_commit_delta_change_records(
                inline_segment,
                None,
                commit_id,
                &mut records,
            )?;
        } else {
            validate_physical_commit_delta_segments(commit_id, &manifest, &physical_segments)?;
            for (segment_index, bounds) in manifest.segments.iter().enumerate() {
                collect_validated_commit_delta_change_records(
                    &physical_segments[&segment_index],
                    Some(bounds),
                    commit_id,
                    &mut records,
                )?;
            }
        }
    }
    debug_assert!(segments.is_empty());
    Ok(records.into_values().map(|(_, change)| change).collect())
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
    } = scan_commit_delta_plane(store).await?;
    let mut inventory = CommitDeltaInventory::default();
    let mut authoritative_changes =
        BTreeMap::<crate::changelog::ChangeId, crate::changelog::ChangeRecord>::new();
    for (commit_id, manifest) in manifests {
        let physical_segments = segments.remove(&commit_id).unwrap_or_default();
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
            validate_physical_commit_delta_segments(commit_id, &manifest, &physical_segments)?;
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
        validate_commit_delta_member_order_and_ids(commit_id, &members)?;
        for member in &members {
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
        inventory.commits.insert(
            commit_id,
            CommitDeltaInventoryEntry {
                members,
                segment_count,
            },
        );
    }
    debug_assert!(segments.is_empty());
    Ok(inventory)
}

async fn scan_commit_delta_plane(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<CommitDeltaPlane, LixError> {
    let manifest_rows = scan_full_space(store, TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE).await?;
    let segment_rows = scan_full_space(store, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE).await?;

    let mut manifests = BTreeMap::<CommitId, CommitDeltaManifest>::new();
    for (key, bytes) in manifest_rows {
        if key.0.len() != 16 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta manifest key is not a 16-byte commit id",
            ));
        }
        let commit_id = commit_id_from_delta_key(&key)?;
        let manifest = decode_commit_delta_manifest(&bytes)?;
        if manifests.insert(commit_id, manifest).is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit_delta inventory found duplicate manifest for commit '{commit_id}'"
                ),
            ));
        }
    }

    let mut segments = BTreeMap::<CommitId, BTreeMap<usize, Bytes>>::new();
    for (key, bytes) in segment_rows {
        if key.0.len() != 20 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta segment key is not commit-id plus u32 suffix",
            ));
        }
        let commit_id = commit_id_from_delta_key(&key)?;
        let segment_index = usize::try_from(u32::from_be_bytes(
            key.0[16..20]
                .try_into()
                .expect("commit-delta segment suffix length checked"),
        ))
        .expect("u32 fits usize");
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
    })
}

fn validate_physical_commit_delta_segments(
    commit_id: CommitId,
    manifest: &CommitDeltaManifest,
    physical_segments: &BTreeMap<usize, Bytes>,
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
    Ok(())
}

pub(crate) fn stage_delete_commit_delta_inventory_entry(
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
    entry: &CommitDeltaInventoryEntry,
) -> Result<(), LixError> {
    writes.delete(
        TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE,
        key(commit_delta_manifest_key(commit_id)),
    );
    for segment_index in 0..entry.segment_count {
        writes.delete(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            key(commit_delta_segment_key(commit_id, segment_index)?),
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
        let change = crate::changelog::ChangeRecord {
            format_version: 2,
            change_id: value.change_id,
            schema_key: key.schema_key.clone(),
            entity_pk: key.entity_pk.clone(),
            file_id: key.file_id.clone(),
            snapshot: payload.snapshot,
            metadata: payload.metadata,
            created_at: value.updated_at,
            origin_key: payload.origin_key,
        };
        members.push(CommitDeltaMember {
            key,
            value,
            change,
            segment_index,
            ordinal: u32::try_from(entry_index).expect("segment ordinal fits u32"),
        });
    }
    Ok(())
}

fn collect_validated_commit_delta_change_records(
    bytes: &[u8],
    expected_bounds: Option<&CommitDeltaSegmentBounds>,
    expected_commit_id: CommitId,
    records: &mut BTreeMap<crate::changelog::ChangeId, (CommitId, crate::changelog::ChangeRecord)>,
) -> Result<(), LixError> {
    let (leaf, payloads) = decode_commit_delta_with_payloads(bytes, expected_bounds)?;
    visit_commit_delta_leaf(
        &leaf,
        expected_commit_id,
        |entry_index, encoded_key, value| {
            let payload = payloads.decode(entry_index)?;
            let key = decode_key(encoded_key)?;
            let change = crate::changelog::ChangeRecord {
                format_version: 2,
                change_id: value.change_id,
                schema_key: key.schema_key,
                entity_pk: key.entity_pk,
                file_id: key.file_id,
                snapshot: payload.snapshot,
                metadata: payload.metadata,
                created_at: value.updated_at,
                origin_key: payload.origin_key,
            };
            match records.entry(change.change_id) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert((expected_commit_id, change));
                }
                std::collections::btree_map::Entry::Occupied(entry)
                    if entry.get().0 == expected_commit_id =>
                {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "tracked_state commit_delta for commit '{expected_commit_id}' contains duplicate change id '{}'",
                            change.change_id
                        ),
                    ));
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get().1 != change => {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "tracked_state change '{}' has conflicting authoritative packed payloads",
                            change.change_id
                        ),
                    ));
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
            Ok(())
        },
    )
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
    let mut change_ids = BTreeSet::new();
    if let Some(change_id) = members
        .iter()
        .map(|member| member.value.change_id)
        .find(|change_id| !change_ids.insert(*change_id))
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_delta for commit '{commit_id}' contains duplicate change id '{change_id}'"
            ),
        ));
    }
    Ok(())
}

fn encode_commit_delta_manifest(manifest: &CommitDeltaManifest) -> Result<Vec<u8>, LixError> {
    let payload = storage_codec::encode("tracked_state packed commit_delta manifest", manifest)?;
    let mut encoded = Vec::with_capacity(COMMIT_DELTA_FORMAT_MAGIC.len() + payload.len());
    encoded.extend_from_slice(COMMIT_DELTA_FORMAT_MAGIC);
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn decode_commit_delta_manifest(bytes: &[u8]) -> Result<CommitDeltaManifest, LixError> {
    let Some(payload) = bytes.strip_prefix(COMMIT_DELTA_FORMAT_MAGIC) else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta manifest has an unsupported format; recreate the repository",
        ));
    };
    let manifest = storage_codec::decode("tracked_state packed commit_delta manifest", payload)?;
    validate_commit_delta_manifest(&manifest)?;
    Ok(manifest)
}

async fn load_commit_delta_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<CommitDeltaManifest>, LixError> {
    let Some(bytes) = get_one(
        store,
        TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE,
        commit_delta_manifest_key(commit_id),
    )
    .await?
    else {
        return Ok(None);
    };
    decode_commit_delta_manifest(&bytes).map(Some)
}

fn validate_commit_delta_manifest(manifest: &CommitDeltaManifest) -> Result<(), LixError> {
    if !manifest.inline_segment.is_empty() {
        if !manifest.segments.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta manifest mixes inline and indexed segments",
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
    Ok(())
}

impl CommitDeltaManifest {
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
        };
        entries.len()
    ];
    encode_commit_delta_segment_with_payloads(entries, &payloads)
}

fn encode_commit_delta_segment_with_payloads(
    entries: &[EncodedLeafEntry],
    payloads: &[CommitDeltaPayloadRef<'_>],
) -> Vec<u8> {
    debug_assert_eq!(entries.len(), payloads.len());
    let leaf = encode_leaf_node(entries);
    let mut payload_offsets = Vec::with_capacity(payloads.len() + 1);
    let mut payload_bytes = Vec::new();
    for payload in payloads {
        payload_offsets.push(
            u32::try_from(payload_bytes.len()).expect("commit-delta payload sidecar fits u32"),
        );
        payload_bytes.extend_from_slice(
            &storage_codec::encode("tracked_state indexed commit_delta payload", payload)
                .expect("commit-delta payload refs are infallible to encode"),
        );
    }
    payload_offsets
        .push(u32::try_from(payload_bytes.len()).expect("commit-delta payload sidecar fits u32"));
    let leaf_len = u32::try_from(leaf.len()).expect("commit-delta leaf fits u32");
    let entry_count = u32::try_from(entries.len()).expect("commit-delta entry count fits u32");
    let directory_bytes = payload_offsets.len() * COMMIT_DELTA_PAYLOAD_OFFSET_BYTES;
    let mut encoded = Vec::with_capacity(
        COMMIT_DELTA_FORMAT_MAGIC.len()
            + 4
            + leaf.len()
            + 4
            + directory_bytes
            + payload_bytes.len(),
    );
    encoded.extend_from_slice(COMMIT_DELTA_FORMAT_MAGIC);
    encoded.extend_from_slice(&leaf_len.to_be_bytes());
    encoded.extend_from_slice(&leaf);
    encoded.extend_from_slice(&entry_count.to_be_bytes());
    for offset in payload_offsets {
        encoded.extend_from_slice(&offset.to_be_bytes());
    }
    encoded.extend_from_slice(&payload_bytes);
    encoded
}

fn decode_commit_delta_leaf(
    bytes: &[u8],
    expected_bounds: Option<&CommitDeltaSegmentBounds>,
) -> Result<DecodedLeafNodeRef, LixError> {
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
    let (_, payload_bytes) = split_commit_delta_segment(bytes)?;
    let leaf = decode_commit_delta_leaf(bytes, expected_bounds)?;
    let (entry_count, payload_bytes) = payload_bytes.split_at_checked(4).ok_or_else(|| {
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
    let (offsets, payload_bytes) =
        payload_bytes
            .split_at_checked(directory_len)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_delta payload directory is truncated",
                )
            })?;
    let index = CommitDeltaPayloadIndexRef {
        offsets,
        payload_bytes,
        entry_count,
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
        if offset > payload_bytes.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta payload offset is out of bounds",
            ));
        }
        previous = offset;
    }
    if previous != payload_bytes.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta payload directory does not cover its sidecar",
        ));
    }
    Ok((leaf, index))
}

fn decode_commit_delta_segment(
    bytes: &[u8],
    expected_bounds: Option<&CommitDeltaSegmentBounds>,
    expected_commit_id: CommitId,
) -> Result<DecodedLeafNodeRef, LixError> {
    let (leaf, _) = decode_commit_delta_with_payloads(bytes, expected_bounds)?;
    visit_commit_delta_leaf(&leaf, expected_commit_id, |_, _, _| Ok(()))?;
    Ok(leaf)
}

/// Visits each packed delta exactly once while validating the full immutable
/// segment contract. Scan callers decode the key and retain matching values in
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

fn find_loaded_commit_delta_entry(
    leaf: &DecodedLeafNodeRef,
    payloads: &CommitDeltaPayloadIndexRef<'_>,
    target_key: &[u8],
    expected_commit_id: CommitId,
) -> Result<Option<LoadedCommitDeltaEntry>, LixError> {
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
            "tracked_state commit_delta payload has the wrong physical commit id",
        ));
    }
    let payload = payloads.decode(index)?;
    let key = decode_key(entry.key)?;
    let change_record = crate::changelog::ChangeRecord {
        format_version: 2,
        change_id: value.change_id,
        schema_key: key.schema_key,
        entity_pk: key.entity_pk,
        file_id: key.file_id,
        snapshot: payload.snapshot,
        metadata: payload.metadata,
        created_at: value.updated_at,
        origin_key: payload.origin_key,
    };
    Ok(Some(LoadedCommitDeltaEntry {
        #[cfg(test)]
        value,
        change_record,
    }))
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
/// commit-root and tree-chunk reads see bytes staged by the root writer first.
#[derive(Debug)]
pub(crate) struct TrackedStateStagedRead<'a, S: ?Sized> {
    store: &'a S,
    commit_roots: HashMap<[u8; 16], Bytes>,
    chunks: &'a TrackedStateChunkOverlay,
}

impl<'a, S> TrackedStateStagedRead<'a, S>
where
    S: StorageAdapterRead + ?Sized,
{
    pub(crate) fn new<'root>(
        store: &'a S,
        commit_roots: impl IntoIterator<Item = &'root TrackedStateCommitRoot>,
        chunks: &'a TrackedStateChunkOverlay,
    ) -> Result<Self, LixError> {
        let mut encoded_roots = HashMap::new();
        for metadata in commit_roots {
            let key = *metadata.commit_id.as_uuid().as_bytes();
            let value = Bytes::from(encode_commit_root(metadata)?);
            if let Some(existing) = encoded_roots.insert(key, value.clone())
                && existing != value
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked-state staged audit contains conflicting commit roots",
                ));
            }
        }
        Ok(Self {
            store,
            commit_roots: encoded_roots,
            chunks,
        })
    }

    fn staged_bytes(&self, space: StorageSpaceId, key: &StorageKey) -> Option<Bytes> {
        if space == TRACKED_STATE_COMMIT_ROOT_SPACE.id {
            let key = <&[u8; 16]>::try_from(key.0.as_ref()).ok()?;
            return self.commit_roots.get(key).cloned();
        }
        if space == TRACKED_STATE_TREE_CHUNK_SPACE.id {
            let key = <&[u8; TRACKED_STATE_HASH_BYTES]>::try_from(key.0.as_ref()).ok()?;
            return self.chunks.staged_chunk_bytes(key);
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
        let mut result = self.store.get_many(requests).await?;
        let requested = requests
            .iter()
            .map(|request| request.keys.len())
            .sum::<usize>();
        if result.values.len() != requested {
            return Err(StorageError::Corruption(format!(
                "tracked-state staged audit requested {} point reads but storage returned {} slots",
                requested,
                result.values.len()
            )));
        }
        let mut slots = result.values.iter_mut();
        for request in requests {
            for (key, slot) in request.keys.iter().zip(slots.by_ref()) {
                let Some(bytes) = self.staged_bytes(request.space, key) else {
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
        space: StorageSpaceId,
        range: StorageKeyRange,
        opts: StorageScanOptions,
    ) -> Result<StorageScanChunk, StorageError> {
        if space == TRACKED_STATE_COMMIT_ROOT_SPACE.id || space == TRACKED_STATE_TREE_CHUNK_SPACE.id
        {
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

fn encode_commit_root(metadata: &TrackedStateCommitRoot) -> Result<Vec<u8>, LixError> {
    let payload = storage_codec::encode("tracked_state commit_root", metadata)?;
    let mut encoded = Vec::with_capacity(TRACKED_STATE_COMMIT_ROOT_MAGIC.len() + payload.len());
    encoded.extend_from_slice(TRACKED_STATE_COMMIT_ROOT_MAGIC);
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn decode_commit_root(bytes: &[u8]) -> Result<TrackedStateCommitRoot, LixError> {
    let Some(payload) = bytes.strip_prefix(TRACKED_STATE_COMMIT_ROOT_MAGIC) else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_root has an unsupported format; recreate the repository",
        ));
    };
    storage_codec::decode("tracked_state commit_root", payload)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};

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
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};
    use crate::tracked_state::codec::{
        EncodedLeafEntry, PendingChunk, PendingChunkBatch, TrackedStateKeyBatchBuilder,
        encode_key_ref, encode_value_ref, hash_bytes,
    };
    use crate::tracked_state::types::{
        TrackedStateCommitDeltaRef, TrackedStateCommitRoot, TrackedStateCommitRootParent,
        TrackedStateDeltaRef, TrackedStateIndexValue, TrackedStateIndexValueRef, TrackedStateKey,
        TrackedStateKeyRef, TrackedStateRootId,
    };

    use super::{
        COMMIT_DELTA_FORMAT_MAGIC, COMMIT_DELTA_SEGMENT_MAX_ROWS, CommitDeltaChangeLocator,
        CommitDeltaManifest, CommitDeltaPayloadRef, DecodedCommitDeltaBatch,
        TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
        TRACKED_STATE_COMMIT_ROOT_MAGIC, TRACKED_STATE_COMMIT_ROOT_SPACE,
        TRACKED_STATE_TREE_CHUNK_SPACE, TrackedStateChunkOverlay, commit_delta_manifest_key,
        decode_commit_delta_manifest, decode_commit_delta_with_payloads, decode_commit_root,
        encode_commit_delta_manifest, encode_commit_delta_segment,
        encode_commit_delta_segment_with_payloads, encode_commit_root, key,
        load_change_record_by_id, load_commit_delta_change_ids, load_commit_delta_change_records,
        load_commit_delta_members_with_payloads, load_commit_delta_values_encoded,
        load_owned_commit_delta_entries, scan_change_records_from_commit_deltas,
        scan_commit_delta_inventory, scan_commit_delta_members, scan_commit_delta_values,
        stage_change_locators, stage_commit_deltas, stage_delete_commit_delta_inventory_entry,
        value,
    };

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
    }

    #[tokio::test]
    async fn commit_local_and_global_authority_reject_duplicate_change_ids() {
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
        let error = load_commit_delta_change_ids(&read, commit_id)
            .await
            .expect_err("commit-local authority must reject duplicate change ids");
        assert!(error.to_string().contains("contains duplicate change id"));
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
    async fn global_inventory_allows_shared_payload_and_rejects_conflicting_payload() {
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
        let second = commit_delta_ref(
            second_commit,
            &fixture,
            crate::json_store::JsonSlotRef::Inline(shared_snapshot),
            crate::json_store::JsonSlotRef::None,
            None,
        );
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &[first]).expect("first owner should stage");
        stage_commit_deltas(&mut writes, &[second]).expect("second owner should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("shared authority should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("shared authority read should open");
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
        let conflicting = commit_delta_ref(
            conflicting_commit,
            &fixture,
            crate::json_store::JsonSlotRef::Inline(r#"{"shared":false}"#),
            crate::json_store::JsonSlotRef::None,
            None,
        );
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
        let error = scan_commit_delta_inventory(&read)
            .await
            .expect_err("conflicting payloads for one change id must fail");
        assert!(
            error
                .to_string()
                .contains("conflicting authoritative packed payloads")
        );
        let error = scan_change_records_from_commit_deltas(&read)
            .await
            .expect_err("streaming scan must reject conflicting packed payloads");
        assert!(
            error
                .to_string()
                .contains("conflicting authoritative packed payloads")
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
        let mut writes = crate::storage_adapter::StorageWriteSet::new();
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
            4,
            "300 payload-bearing rows should use one manifest and three byte-bounded segments"
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
            },
            CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::Inline(snapshots[1]),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
            },
            CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::Inline(snapshots[2]),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: Some("last"),
            },
        ];
        let mut encoded = encode_commit_delta_segment_with_payloads(&entries, &payloads);

        let corrupt_range = {
            let (_, index) =
                decode_commit_delta_with_payloads(&encoded, None).expect("segment should decode");
            assert_eq!(index.len(), 3);
            assert_eq!(
                index
                    .decode(0)
                    .expect("first payload should decode")
                    .snapshot,
                crate::json_store::JsonSlot::Inline(snapshots[0].into())
            );
            assert_eq!(
                index
                    .decode(1)
                    .expect("sparse payload should decode")
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
                .contains("failed to decode tracked_state indexed commit_delta payload"),
            "unexpected error: {error}"
        );
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
            },
            CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::Inline(r#"{"second":true}"#),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
            },
        ];
        let mut encoded = encode_commit_delta_segment_with_payloads(&entries, &payloads);
        let leaf_len = usize::try_from(u32::from_be_bytes(
            encoded[COMMIT_DELTA_FORMAT_MAGIC.len()..COMMIT_DELTA_FORMAT_MAGIC.len() + 4]
                .try_into()
                .expect("fixed leaf length"),
        ))
        .expect("u32 fits usize");
        let offsets_start = COMMIT_DELTA_FORMAT_MAGIC.len() + 4 + leaf_len + 4;
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
        let encoded = encode_commit_delta_segment_with_payloads(
            &[entry],
            &[CommitDeltaPayloadRef {
                snapshot: crate::json_store::JsonSlotRef::Inline(r#"{"ok":true}"#),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
            }],
        );

        let mut old = encoded.clone();
        old[..COMMIT_DELTA_FORMAT_MAGIC.len()].copy_from_slice(b"LXCD4");
        let error = decode_commit_delta_with_payloads(&old, None)
            .expect_err("LXCD4 segments must be rejected");
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
        let truncated = &encoded[..payload_header + 4 + 7];
        let error = decode_commit_delta_with_payloads(truncated, None)
            .expect_err("a truncated two-offset directory must fail");
        assert!(
            error.to_string().contains("payload directory is truncated"),
            "unexpected error: {error}"
        );

        let mut invalid_offset = encoded;
        let terminal_offset = payload_header + 4 + 4;
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
            1,
            "a one-segment commit should remain one physical record"
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
            1,
            "GC should delete the inline manifest only"
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
        writes.put(
            TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE,
            key(commit_delta_manifest_key(commit_id)),
            value(
                encode_commit_delta_manifest(&CommitDeltaManifest {
                    inline_segment: encode_commit_delta_segment(&entries),
                    segments: Vec::new(),
                })
                .expect("inline manifest should encode"),
            ),
        );
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
                + 1,
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
        let fixtures = (0..257)
            .map(|index| CommitDeltaFixture {
                schema_key: match index {
                    255 => "sparse".to_string(),
                    256 => "zeta".to_string(),
                    _ => "alpha".to_string(),
                },
                file_id: (index == 255).then(|| "sparse-file".to_string()),
                entity_pk: EntityPk::single(format!("boundary-{index:04}")),
                change_id: ChangeId::for_test_label(&format!("boundary-change-{index}")),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(index.into()),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy((index + 1).into()),
            })
            .collect::<Vec<_>>();

        let mut inline_writes = storage.new_write_set();
        let inline_deltas = commit_delta_refs(inline_commit_id, &fixtures[..256]);
        stage_commit_deltas(&mut inline_writes, &inline_deltas)
            .expect("256 deltas should split at the byte boundary");
        assert_eq!(inline_writes.stats().staged_puts, 3);
        storage
            .commit_write_set(inline_writes, StorageWriteOptions::default())
            .await
            .expect("inline boundary deltas should commit");

        let mut indexed_writes = storage.new_write_set();
        let indexed_deltas = commit_delta_refs(indexed_commit_id, &fixtures);
        stage_commit_deltas(&mut indexed_writes, &indexed_deltas)
            .expect("257 deltas should use indexed segments");
        assert_eq!(indexed_writes.stats().staged_puts, 4);
        storage
            .commit_write_set(indexed_writes, StorageWriteOptions::default())
            .await
            .expect("indexed boundary deltas should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let sparse = &fixtures[255];
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
            fixtures.len().div_ceil(COMMIT_DELTA_SEGMENT_MAX_ROWS),
            "the batch retains one decoded arena per packed segment, never one owner per row"
        );
        assert!(
            batch.arena_count() * COMMIT_DELTA_SEGMENT_MAX_ROWS >= batch.len(),
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
    fn packed_commit_delta_manifest_rejects_unknown_format() {
        let error = decode_commit_delta_manifest(b"LXCD1not-a-v2-manifest")
            .expect_err("old packed manifests must fail loudly");
        assert!(
            error
                .to_string()
                .contains("unsupported format; recreate the repository")
        );
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
            TRACKED_STATE_COMMIT_ROOT_SPACE,
            TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE,
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
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

    #[test]
    fn commit_root_codec_roundtrips_with_parent_metadata() {
        let metadata = TrackedStateCommitRoot {
            commit_id: CommitId::for_test_label("child"),
            root_id: TrackedStateRootId::new([2; 32]),
            parent_roots: vec![TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("parent"),
                root_id: TrackedStateRootId::new([1; 32]),
            }],
            changed_key_count: 7,
            row_count_estimate: 42,
            tree_height: 3,
            primary_chunk_count: 5,
            primary_chunk_bytes: 4096,
        };

        let encoded = encode_commit_root(&metadata).expect("commit root should encode");
        assert!(encoded.starts_with(TRACKED_STATE_COMMIT_ROOT_MAGIC));
        let decoded = decode_commit_root(&encoded).expect("commit root should decode");

        assert_eq!(decoded, metadata);
    }

    #[test]
    fn commit_root_codec_rejects_malformed_storage_bytes() {
        let error = decode_commit_root(b"LXTR1not-musli")
            .expect_err("old commit-root versions must fail loudly");

        assert!(
            error
                .to_string()
                .contains("unsupported format; recreate the repository")
        );
    }

    #[test]
    fn commit_root_codec_rejects_pre_v3_roots() {
        let metadata = TrackedStateCommitRoot {
            commit_id: CommitId::for_test_label("legacy"),
            root_id: TrackedStateRootId::new([7; 32]),
            parent_roots: Vec::new(),
            changed_key_count: 1,
            row_count_estimate: 1,
            tree_height: 1,
            primary_chunk_count: 1,
            primary_chunk_bytes: 128,
        };
        let unversioned = crate::storage_codec::encode("tracked_state commit_root", &metadata)
            .expect("pre-v3 commit root should encode");
        let mut v2 = b"LXTR2".to_vec();
        v2.extend_from_slice(&unversioned);

        for old_bytes in [&unversioned, &v2] {
            let error = decode_commit_root(old_bytes)
                .expect_err("pre-v3 roots must not enter the v3 tree layout");

            assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
            assert!(
                error
                    .message
                    .contains("unsupported format; recreate the repository")
            );
        }
    }

    #[test]
    fn commit_root_codec_rejects_trailing_bytes() {
        let metadata = TrackedStateCommitRoot {
            commit_id: CommitId::for_test_label("commit"),
            root_id: TrackedStateRootId::new([9; 32]),
            parent_roots: Vec::new(),
            changed_key_count: 1,
            row_count_estimate: 2,
            tree_height: 1,
            primary_chunk_count: 1,
            primary_chunk_bytes: 128,
        };
        let mut encoded = encode_commit_root(&metadata).expect("commit root should encode");
        encoded.push(0);

        let error = decode_commit_root(&encoded)
            .expect_err("trailing bytes should fail commit root decode");

        assert!(
            error
                .to_string()
                .contains("failed to decode tracked_state commit_root")
        );
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
