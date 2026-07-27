#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cmp_owned
)]

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::changelog::CommitId;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageCoreProjection, StorageError, StorageGetManyResult,
    StorageGetOptions, StorageKey, StorageKeyRange, StorageProjectedValue, StorageScanChunk,
    StorageScanOptions, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::tracked_state::codec::{
    DecodedLeafNodeRef, DecodedNodeRef, EncodedLeafEntry, PendingChunkWrite, decode_key,
    decode_node_ref, decode_value, encode_key_ref, encode_leaf_node, encode_schema_key_prefix,
    encode_value_ref,
};
use crate::tracked_state::types::{
    TRACKED_STATE_HASH_BYTES, TrackedStateCommitRoot, TrackedStateDeltaRef, TrackedStateIndexValue,
    TrackedStateIndexValueRef, TrackedStateKey, TrackedStateKeyRef, TrackedStateRootId,
};
use crate::{LixError, storage_codec};
use bytes::Bytes;

pub(crate) const TRACKED_STATE_TREE_CHUNK_NAMESPACE: &str = "tracked_state.tree_chunk";
pub(crate) const TRACKED_STATE_COMMIT_ROOT_NAMESPACE: &str = "tracked_state.commit_root";
pub(crate) const TRACKED_STATE_COMMIT_DELTA_MANIFEST_NAMESPACE: &str =
    "tracked_state.commit_delta_manifest.v2";
pub(crate) const TRACKED_STATE_COMMIT_DELTA_SEGMENT_NAMESPACE: &str =
    "tracked_state.commit_delta_segment.v2";
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

const COMMIT_DELTA_SEGMENT_ROWS: usize = 256;
const COMMIT_DELTA_FORMAT_MAGIC: &[u8] = b"LXCD2";

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

async fn get_one(
    store: &(impl StorageAdapterRead + ?Sized),
    space: StorageSpace,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, LixError> {
    let result = PointReadPlan::new(space, &[StorageKey(Bytes::from(key))])
        .materialize(store, StorageGetOptions::default())
        .await?;
    Ok(result
        .value
        .into_iter()
        .next()
        .flatten()
        .and_then(full_value))
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

/// Removes every packed identity-delta segment for a collected commit.
///
/// Unlike immutable tree chunks, these records are commit-addressed and
/// therefore cannot be reclaimed by content-addressed storage maintenance.
/// The manifest makes collection bounded by the number of packed segments
/// instead of requiring a full prefix scan and one delete per changed row.
pub(crate) async fn stage_delete_commit_deltas(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
) -> Result<(), LixError> {
    let Some(manifest) = load_commit_delta_manifest(store, commit_id).await? else {
        return Ok(());
    };
    writes.reserve_space(TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE, 0, 1);
    writes.reserve_space(
        TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
        0,
        manifest.segments.len(),
    );
    writes.delete(
        TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE,
        key(commit_delta_manifest_key(commit_id)),
    );
    for segment_index in 0..manifest.segments.len() {
        writes.delete(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            key(commit_delta_segment_key(commit_id, segment_index)?),
        );
    }
    Ok(())
}

/// Stages all tracked mutations for one immutable commit as bounded, sorted
/// front-coded segments plus one tiny directory. A full commit no longer
/// writes one backend key for every affected identity.
pub(crate) fn stage_commit_deltas(
    writes: &mut StorageWriteSet,
    deltas: &[TrackedStateDeltaRef<'_>],
) -> Result<(), LixError> {
    let Some(&commit_id) = deltas.first().map(|delta| &delta.commit_id) else {
        return Ok(());
    };
    let mut entries = Vec::with_capacity(deltas.len());
    for delta in deltas {
        if delta.commit_id != commit_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state cannot pack deltas from different commits together",
            ));
        }
        entries.push(EncodedLeafEntry {
            key: encode_key_ref(TrackedStateKeyRef {
                schema_key: delta.schema_key,
                file_id: delta.file_id,
                entity_pk: delta.entity_pk,
            }),
            value: encode_value_ref(TrackedStateIndexValueRef {
                change_id: delta.change_id,
                commit_id: delta.commit_id,
                deleted: delta.deleted,
                created_at: delta.created_at,
                updated_at: delta.updated_at,
            }),
        });
    }
    entries.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    if entries.windows(2).any(|pair| pair[0].key == pair[1].key) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_delta for commit '{commit_id}' contains duplicate identities"
            ),
        ));
    }

    let segment_count = entries.len().div_ceil(COMMIT_DELTA_SEGMENT_ROWS);
    writes.reserve_space(TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE, 1, 0);
    if segment_count == 1 {
        writes.put(
            TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE,
            key(commit_delta_manifest_key(commit_id)),
            value(encode_commit_delta_manifest(&CommitDeltaManifest {
                inline_segment: encode_commit_delta_segment(&entries),
                segments: Vec::new(),
            })?),
        );
        return Ok(());
    }
    writes.reserve_space(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, segment_count, 0);
    let mut manifest = CommitDeltaManifest {
        inline_segment: Vec::new(),
        segments: Vec::with_capacity(segment_count),
    };
    for (segment_index, entries) in entries.chunks(COMMIT_DELTA_SEGMENT_ROWS).enumerate() {
        let first_key = entries
            .first()
            .expect("non-empty packed commit-delta segment")
            .key
            .clone();
        let last_key = entries
            .last()
            .expect("non-empty packed commit-delta segment")
            .key
            .clone();
        manifest.segments.push(CommitDeltaSegmentBounds {
            first_key,
            last_key,
        });
        writes.put(
            TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
            key(commit_delta_segment_key(commit_id, segment_index)?),
            value(encode_commit_delta_segment(entries)),
        );
    }
    writes.put(
        TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE,
        key(commit_delta_manifest_key(commit_id)),
        value(encode_commit_delta_manifest(&manifest)?),
    );
    Ok(())
}

pub(crate) async fn load_commit_delta_values(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    keys: &[TrackedStateKey],
) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut values = vec![None; keys.len()];
    let Some(manifest) = load_commit_delta_manifest(store, commit_id).await? else {
        return Ok(values);
    };
    if let Some(inline_segment) = manifest.inline_segment() {
        let leaf = decode_commit_delta_segment(inline_segment, None, commit_id)?;
        for (output_index, key) in keys.iter().enumerate() {
            let encoded_key = encode_key_ref(TrackedStateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            });
            values[output_index] = find_commit_delta_value(&leaf, &encoded_key, commit_id)?;
        }
        return Ok(values);
    }
    let mut lookups_by_segment = BTreeMap::<usize, Vec<(usize, Vec<u8>)>>::new();
    for (output_index, key) in keys.iter().enumerate() {
        let encoded_key = encode_key_ref(TrackedStateKeyRef {
            schema_key: &key.schema_key,
            file_id: key.file_id.as_deref(),
            entity_pk: &key.entity_pk,
        });
        if let Some(segment_index) = commit_delta_segment_for_key(&manifest, &encoded_key) {
            lookups_by_segment
                .entry(segment_index)
                .or_default()
                .push((output_index, encoded_key));
        }
    }
    if lookups_by_segment.is_empty() {
        return Ok(values);
    }
    let segment_indices = lookups_by_segment.keys().copied().collect::<Vec<_>>();
    let storage_keys = segment_indices
        .iter()
        .map(|&segment_index| {
            commit_delta_segment_key(commit_id, segment_index)
                .map(|key| StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let result = PointReadPlan::new(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, &storage_keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    for ((segment_index, lookups), value) in lookups_by_segment.into_iter().zip(result.value) {
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
        for (output_index, encoded_key) in lookups {
            values[output_index] = find_commit_delta_value(&leaf, &encoded_key, commit_id)?;
        }
    }
    Ok(values)
}

/// Scans only the mutations in one commit that belong to one of the requested
/// schemas. This is the partial-key counterpart to
/// [`load_commit_delta_values`]: it avoids hydrating unrelated changelog
/// changes when a history provider knows the schema but not every identity.
pub(crate) async fn scan_commit_delta_values(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    schema_keys: &[String],
) -> Result<Vec<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
    let Some(manifest) = load_commit_delta_manifest(store, commit_id).await? else {
        return Ok(Vec::new());
    };
    let requested_schemas = schema_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if let Some(inline_segment) = manifest.inline_segment() {
        let leaf = decode_commit_delta_leaf(inline_segment, None)?;
        let mut entries = Vec::with_capacity(leaf.len());
        collect_commit_delta_leaf_entries(&leaf, commit_id, &requested_schemas, &mut entries)?;
        return Ok(entries);
    }
    let segment_indices = commit_delta_segments_for_schemas(&manifest, &requested_schemas);
    if segment_indices.is_empty() {
        return Ok(Vec::new());
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
    let mut entries = Vec::new();
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
        collect_commit_delta_leaf_entries(&leaf, commit_id, &requested_schemas, &mut entries)?;
    }
    Ok(entries)
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

fn encode_commit_delta_segment(entries: &[EncodedLeafEntry]) -> Vec<u8> {
    let leaf = encode_leaf_node(entries);
    let mut encoded = Vec::with_capacity(COMMIT_DELTA_FORMAT_MAGIC.len() + leaf.len());
    encoded.extend_from_slice(COMMIT_DELTA_FORMAT_MAGIC);
    encoded.extend_from_slice(&leaf);
    encoded
}

fn decode_commit_delta_leaf(
    bytes: &[u8],
    expected_bounds: Option<&CommitDeltaSegmentBounds>,
) -> Result<DecodedLeafNodeRef, LixError> {
    let Some(leaf_bytes) = bytes.strip_prefix(COMMIT_DELTA_FORMAT_MAGIC) else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta segment has an unsupported format; recreate the repository",
        ));
    };
    let leaf = match decode_node_ref(leaf_bytes)? {
        DecodedNodeRef::Leaf(leaf) => leaf,
        DecodedNodeRef::Internal(_) => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_delta segment contains an internal tree node",
            ));
        }
    };
    if leaf.len() == 0 || leaf.len() > COMMIT_DELTA_SEGMENT_ROWS {
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

fn decode_commit_delta_segment(
    bytes: &[u8],
    expected_bounds: Option<&CommitDeltaSegmentBounds>,
    expected_commit_id: CommitId,
) -> Result<DecodedLeafNodeRef, LixError> {
    let leaf = decode_commit_delta_leaf(bytes, expected_bounds)?;
    visit_commit_delta_leaf(&leaf, expected_commit_id, |_, _| Ok(()))?;
    Ok(leaf)
}

/// Visits each packed delta exactly once while validating the full immutable
/// segment contract. Scan callers decode the key and retain matching values in
/// the same pass; point callers use the no-op visitor before their binary
/// search, preserving eager corruption detection.
fn visit_commit_delta_leaf(
    leaf: &DecodedLeafNodeRef,
    expected_commit_id: CommitId,
    mut visit: impl FnMut(&[u8], TrackedStateIndexValue) -> Result<(), LixError>,
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
        visit(entry.key, value)?;
        previous_key = Some(entry.key);
    }
    Ok(())
}

fn collect_commit_delta_leaf_entries(
    leaf: &DecodedLeafNodeRef,
    commit_id: CommitId,
    requested_schemas: &BTreeSet<&str>,
    entries: &mut Vec<(TrackedStateKey, TrackedStateIndexValue)>,
) -> Result<(), LixError> {
    visit_commit_delta_leaf(leaf, commit_id, |encoded_key, value| {
        let key = decode_key(encoded_key)?;
        if requested_schemas.is_empty() || requested_schemas.contains(key.schema_key.as_str()) {
            entries.push((key, value));
        }
        Ok(())
    })
}

fn find_commit_delta_value(
    leaf: &DecodedLeafNodeRef,
    target_key: &[u8],
    expected_commit_id: CommitId,
) -> Result<Option<TrackedStateIndexValue>, LixError> {
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

pub(crate) async fn read_chunk(
    store: &(impl StorageAdapterRead + ?Sized),
    hash: &[u8; TRACKED_STATE_HASH_BYTES],
) -> Result<Option<Vec<u8>>, LixError> {
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

pub(crate) fn stage_chunks(writes: &mut StorageWriteSet, chunks: &[PendingChunkWrite]) {
    for chunk in chunks {
        writes.put_content_addressed(
            TRACKED_STATE_TREE_CHUNK_SPACE,
            key(chunk.hash.to_vec()),
            value(chunk.data.clone()),
        );
    }
}

#[derive(Debug, Default)]
pub(crate) struct TrackedStateChunkOverlay {
    chunks: HashMap<[u8; TRACKED_STATE_HASH_BYTES], Vec<u8>>,
}

impl TrackedStateChunkOverlay {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn staged_chunk(&self, hash: &[u8; TRACKED_STATE_HASH_BYTES]) -> Option<&[u8]> {
        self.chunks.get(hash).map(Vec::as_slice)
    }

    pub(crate) fn stage_chunks(
        &mut self,
        writes: &mut StorageWriteSet,
        chunks: &[PendingChunkWrite],
    ) {
        for chunk in chunks {
            self.chunks.insert(chunk.hash, chunk.data.clone());
        }
        stage_chunks(writes, chunks);
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

    fn staged_bytes(&self, space: StorageSpaceId, key: &StorageKey) -> Option<&[u8]> {
        if space == TRACKED_STATE_COMMIT_ROOT_SPACE.id {
            let key = <&[u8; 16]>::try_from(key.0.as_ref()).ok()?;
            return self.commit_roots.get(key).map(AsRef::as_ref);
        }
        if space == TRACKED_STATE_TREE_CHUNK_SPACE.id {
            let key = <&[u8; TRACKED_STATE_HASH_BYTES]>::try_from(key.0.as_ref()).ok()?;
            return self.chunks.staged_chunk(key);
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
        space: StorageSpaceId,
        keys: &[StorageKey],
        opts: StorageGetOptions,
    ) -> Result<StorageGetManyResult, StorageError> {
        let mut result = self.store.get_many(space, keys, opts).await?;
        if result.values.len() != keys.len() {
            return Err(StorageError::Corruption(format!(
                "tracked-state staged audit requested {} point reads but storage returned {} slots",
                keys.len(),
                result.values.len()
            )));
        }
        for (key, slot) in keys.iter().zip(&mut result.values) {
            let Some(bytes) = self.staged_bytes(space, key) else {
                continue;
            };
            *slot = Some(match opts.projection {
                StorageCoreProjection::KeyOnly => StorageProjectedValue::KeyOnly,
                StorageCoreProjection::FullValue => {
                    StorageProjectedValue::FullValue(Bytes::copy_from_slice(bytes))
                }
            });
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

fn full_value(value: StorageProjectedValue) -> Option<Vec<u8>> {
    full_value_bytes(value).map(|bytes| bytes.to_vec())
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

    use crate::LixError;
    use crate::binary_cas::kv::{
        BINARY_CAS_CHUNK_PRESENCE_SPACE, BINARY_CAS_CHUNK_SPACE, BINARY_CAS_MANIFEST_CHUNK_SPACE,
        BINARY_CAS_MANIFEST_SPACE,
    };
    use crate::branch::BRANCH_HEAD_CONTROL_SPACE;
    use crate::changelog::{
        CHANGE_SPACE, COMMIT_CHANGE_ID_SPACE, COMMIT_CHANGE_REF_CHUNK_SPACE, COMMIT_SPACE,
        ChangeId, CommitId,
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
    use crate::tracked_state::codec::{EncodedLeafEntry, encode_key_ref, encode_value_ref};
    use crate::tracked_state::types::{
        TrackedStateCommitRoot, TrackedStateCommitRootParent, TrackedStateDeltaRef,
        TrackedStateIndexValue, TrackedStateIndexValueRef, TrackedStateKey, TrackedStateKeyRef,
        TrackedStateRootId,
    };

    use super::{
        CommitDeltaManifest, TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE,
        TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, TRACKED_STATE_COMMIT_ROOT_MAGIC,
        TRACKED_STATE_COMMIT_ROOT_SPACE, TRACKED_STATE_TREE_CHUNK_SPACE, commit_delta_manifest_key,
        decode_commit_delta_manifest, decode_commit_root, encode_commit_delta_manifest,
        encode_commit_delta_segment, encode_commit_root, key, load_commit_delta_values,
        scan_commit_delta_values, stage_commit_deltas, stage_delete_commit_deltas, value,
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
    ) -> Vec<TrackedStateDeltaRef<'_>> {
        fixtures
            .iter()
            .map(|fixture| TrackedStateDeltaRef {
                schema_key: &fixture.schema_key,
                file_id: fixture.file_id.as_deref(),
                entity_pk: &fixture.entity_pk,
                change_id: fixture.change_id,
                commit_id,
                deleted: fixture.deleted,
                created_at: fixture.created_at,
                updated_at: fixture.updated_at,
            })
            .collect()
    }

    #[tokio::test]
    async fn packed_commit_deltas_preserve_point_and_schema_replay() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("packed-delta-commit");
        let fixtures = packed_commit_delta_fixtures();
        let deltas = fixtures
            .iter()
            .map(|fixture| TrackedStateDeltaRef {
                schema_key: &fixture.schema_key,
                file_id: fixture.file_id.as_deref(),
                entity_pk: &fixture.entity_pk,
                change_id: fixture.change_id,
                commit_id,
                deleted: fixture.deleted,
                created_at: fixture.created_at,
                updated_at: fixture.updated_at,
            })
            .collect::<Vec<_>>();
        let mut writes = storage.new_write_set();
        stage_commit_deltas(&mut writes, &deltas).expect("packed deltas should stage");
        assert_eq!(
            writes.stats().staged_puts,
            3,
            "300 rows should use one manifest and two bounded segments"
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
        let point_values = load_commit_delta_values(&read, commit_id, &point_keys)
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
        assert!(alpha.iter().all(|(key, _)| key.schema_key == "alpha"));
        assert!(alpha.windows(2).all(|pair| pair[0].0 < pair[1].0));

        let all = scan_commit_delta_values(&read, commit_id, &[])
            .await
            .expect("unconstrained replay should scan packed deltas");
        assert_eq!(all.len(), fixtures.len());
        assert!(all.windows(2).all(|pair| pair[0].0 < pair[1].0));
    }

    #[tokio::test]
    async fn single_segment_commit_delta_stays_inline() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("inline-packed-delta-commit");
        let fixture = packed_commit_delta_fixtures()
            .into_iter()
            .next()
            .expect("fixture should contain one row");
        let delta = TrackedStateDeltaRef {
            schema_key: &fixture.schema_key,
            file_id: fixture.file_id.as_deref(),
            entity_pk: &fixture.entity_pk,
            change_id: fixture.change_id,
            commit_id,
            deleted: fixture.deleted,
            created_at: fixture.created_at,
            updated_at: fixture.updated_at,
        };
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
            load_commit_delta_values(&read, commit_id, &[fixture.key()])
                .await
                .expect("inline point replay should load"),
            vec![Some(fixture.value(commit_id))]
        );
        assert_eq!(
            scan_commit_delta_values(&read, commit_id, std::slice::from_ref(&fixture.schema_key))
                .await
                .expect("inline schema replay should load"),
            vec![(fixture.key(), fixture.value(commit_id))]
        );

        let mut deletes = storage.new_write_set();
        stage_delete_commit_deltas(&read, &mut deletes, commit_id)
            .await
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
                }),
                value: encode_value_ref(TrackedStateIndexValueRef {
                    change_id: alpha.change_id,
                    commit_id,
                    deleted: alpha.deleted,
                    created_at: alpha.created_at,
                    updated_at: alpha.updated_at,
                }),
            },
            EncodedLeafEntry {
                key: encode_key_ref(TrackedStateKeyRef {
                    schema_key: &beta.schema_key,
                    file_id: beta.file_id.as_deref(),
                    entity_pk: &beta.entity_pk,
                }),
                value: encode_value_ref(TrackedStateIndexValueRef {
                    change_id: beta.change_id,
                    commit_id: wrong_commit_id,
                    deleted: beta.deleted,
                    created_at: beta.created_at,
                    updated_at: beta.updated_at,
                }),
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
    }

    #[tokio::test]
    async fn packed_commit_delta_gc_deletes_manifest_and_segments() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("packed-delta-gc");
        let fixtures = packed_commit_delta_fixtures();
        let deltas = fixtures
            .iter()
            .map(|fixture| TrackedStateDeltaRef {
                schema_key: &fixture.schema_key,
                file_id: fixture.file_id.as_deref(),
                entity_pk: &fixture.entity_pk,
                change_id: fixture.change_id,
                commit_id,
                deleted: fixture.deleted,
                created_at: fixture.created_at,
                updated_at: fixture.updated_at,
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
        stage_delete_commit_deltas(&read, &mut deletes, commit_id)
            .await
            .expect("packed deltas should stage for deletion");
        assert_eq!(
            deletes.stats().staged_deletes,
            3,
            "GC should delete one manifest and two packed segments"
        );
        storage
            .commit_write_set(deletes, StorageWriteOptions::default())
            .await
            .expect("packed delta deletion should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("post-GC read should open");
        assert!(
            load_commit_delta_values(&read, commit_id, &[fixtures[0].key()])
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
            .expect("256 deltas should stay inline");
        assert_eq!(inline_writes.stats().staged_puts, 1);
        storage
            .commit_write_set(inline_writes, StorageWriteOptions::default())
            .await
            .expect("inline boundary deltas should commit");

        let mut indexed_writes = storage.new_write_set();
        let indexed_deltas = commit_delta_refs(indexed_commit_id, &fixtures);
        stage_commit_deltas(&mut indexed_writes, &indexed_deltas)
            .expect("257 deltas should use indexed segments");
        assert_eq!(indexed_writes.stats().staged_puts, 3);
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
            load_commit_delta_values(&read, indexed_commit_id, &[sparse.key()])
                .await
                .expect("file-scoped point replay should load"),
            vec![Some(sparse.value(indexed_commit_id))]
        );
        assert_eq!(
            scan_commit_delta_values(&read, indexed_commit_id, &["sparse".to_string()])
                .await
                .expect("sparse schema replay should load"),
            vec![(sparse.key(), sparse.value(indexed_commit_id))]
        );
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
            COMMIT_CHANGE_REF_CHUNK_SPACE,
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
