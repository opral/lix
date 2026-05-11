use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::commit_store::ChangeLocator;
use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::tracked_state::types::{
    TrackedStateDeltaEntry, TrackedStateDeltaRef, TrackedStateIndexValue,
    TrackedStateIndexValueRef, TrackedStateKey, TrackedStateKeyRef, TRACKED_STATE_HASH_BYTES,
};
use crate::LixError;

const NODE_VERSION: u8 = 2;
const VALUE_VERSION: u8 = 6;
const VALUE_DELETED_FLAG: u8 = 0b1000_0000;
const VALUE_VERSION_MASK: u8 = 0b0111_1111;
const DELTA_PACK_VERSION: u8 = 1;
const NODE_KIND_LEAF: u8 = 1;
const NODE_KIND_INTERNAL: u8 = 2;
const WEIBULL_K: i32 = 4;
const ENTITY_IDENTITY_END: u8 = 0;
const ENTITY_IDENTITY_STRING: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncodedLeafEntry {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EncodedLeafEntryRef<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) value: &'a [u8],
}

impl EncodedLeafEntry {
    pub(crate) fn as_ref(&self) -> EncodedLeafEntryRef<'_> {
        EncodedLeafEntryRef {
            key: &self.key,
            value: &self.value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingChunkWrite {
    pub(crate) hash: [u8; TRACKED_STATE_HASH_BYTES],
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChildSummary {
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
    pub(crate) child_hash: [u8; TRACKED_STATE_HASH_BYTES],
    pub(crate) subtree_count: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ChildSummaryRef<'a> {
    pub(crate) first_key: &'a [u8],
    pub(crate) last_key: &'a [u8],
    pub(crate) child_hash: [u8; TRACKED_STATE_HASH_BYTES],
    pub(crate) subtree_count: u64,
}

impl ChildSummary {
    pub(crate) fn as_ref(&self) -> ChildSummaryRef<'_> {
        ChildSummaryRef {
            first_key: &self.first_key,
            last_key: &self.last_key,
            child_hash: self.child_hash,
            subtree_count: self.subtree_count,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum DecodedNode {
    Leaf(DecodedLeafNode),
    Internal(DecodedInternalNode),
}

#[derive(Debug, Clone)]
pub(crate) enum DecodedNodeRef<'a> {
    Leaf(DecodedLeafNodeRef<'a>),
    Internal(DecodedInternalNode),
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedLeafNode {
    entries: Vec<EncodedLeafEntry>,
}

impl DecodedLeafNode {
    pub(crate) fn entries(&self) -> &[EncodedLeafEntry] {
        &self.entries
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedLeafNodeRef<'a> {
    bytes: &'a [u8],
    payload_start: usize,
    offsets: Vec<usize>,
}

impl<'a> DecodedLeafNodeRef<'a> {
    pub(crate) fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub(crate) fn entry(&self, index: usize) -> Result<Option<EncodedLeafEntryRef<'a>>, LixError> {
        if index >= self.len() {
            return Ok(None);
        }
        let start = self.payload_start + self.offsets[index];
        let end = self.payload_start + self.offsets[index + 1];
        let record = self.bytes.get(start..end).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state leaf offset points outside node payload",
            )
        })?;
        let mut cursor = 0usize;
        let key = read_sized_slice(record, &mut cursor, "leaf key")?;
        let value = read_sized_slice(record, &mut cursor, "leaf value")?;
        if cursor != record.len() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state leaf entry decode found trailing bytes",
            ));
        }
        Ok(Some(EncodedLeafEntryRef { key, value }))
    }

    pub(crate) fn key(&self, index: usize) -> Result<Option<&'a [u8]>, LixError> {
        let Some(entry) = self.entry(index)? else {
            return Ok(None);
        };
        Ok(Some(entry.key))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedInternalNode {
    children: Vec<ChildSummary>,
}

impl DecodedInternalNode {
    pub(crate) fn children(&self) -> &[ChildSummary] {
        &self.children
    }
}

pub(crate) fn hash_bytes(bytes: &[u8]) -> [u8; TRACKED_STATE_HASH_BYTES] {
    *blake3::hash(bytes).as_bytes()
}

pub(crate) fn encode_key(key: &TrackedStateKey) -> Vec<u8> {
    encode_key_ref(TrackedStateKeyRef {
        schema_key: &key.schema_key,
        file_id: key.file_id.as_deref(),
        entity_id: &key.entity_id,
    })
}

pub(crate) fn encode_key_ref(key: TrackedStateKeyRef<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    append_key_ref(&mut out, key);
    out
}

fn append_key_ref(out: &mut Vec<u8>, key: TrackedStateKeyRef<'_>) {
    push_sized_bytes(out, key.schema_key.as_bytes());
    match key.file_id {
        Some(file_id) => {
            out.push(1);
            push_sized_bytes(out, file_id.as_bytes());
        }
        None => out.push(0),
    }
    push_entity_identity(out, key.entity_id);
}

pub(crate) fn encode_schema_key_prefix(schema_key: &str) -> Vec<u8> {
    let mut out = Vec::new();
    push_sized_bytes(&mut out, schema_key.as_bytes());
    out
}

pub(crate) fn encode_schema_file_prefix(schema_key: &str, file_id: Option<&str>) -> Vec<u8> {
    let mut out = encode_schema_key_prefix(schema_key);
    match file_id {
        Some(file_id) => {
            out.push(1);
            push_sized_bytes(&mut out, file_id.as_bytes());
        }
        None => out.push(0),
    }
    out
}

pub(crate) fn decode_key(bytes: &[u8]) -> Result<TrackedStateKey, LixError> {
    let mut cursor = 0usize;
    let schema_key = read_sized_string(bytes, &mut cursor, "schema_key")?;
    let file_id = match read_u8(bytes, &mut cursor, "file_id presence")? {
        0 => None,
        1 => Some(read_sized_string(bytes, &mut cursor, "file_id")?),
        other => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("tracked-state tree key has invalid file_id presence byte {other}"),
            ))
        }
    };
    let entity_id = read_entity_identity(bytes, &mut cursor)?;
    if cursor != bytes.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state tree key decode found trailing bytes",
        ));
    }
    Ok(TrackedStateKey {
        schema_key,
        file_id,
        entity_id,
    })
}

#[cfg(test)]
pub(crate) fn encode_value(value: &TrackedStateIndexValue) -> Vec<u8> {
    encode_value_ref(TrackedStateIndexValueRef {
        change_locator: value.change_locator.as_ref(),
        deleted: value.deleted,
        snapshot_ref: value.snapshot_ref.as_ref(),
        metadata_ref: value.metadata_ref.as_ref(),
        created_at: &value.created_at,
        updated_at: &value.updated_at,
    })
}

pub(crate) fn encode_value_ref(value: TrackedStateIndexValueRef<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    append_value_ref(&mut out, value);
    out
}

fn append_value_ref(out: &mut Vec<u8>, value: TrackedStateIndexValueRef<'_>) {
    out.push(VALUE_VERSION | if value.deleted { VALUE_DELETED_FLAG } else { 0 });
    push_sized_bytes(out, value.change_locator.source_commit_id.as_bytes());
    out.extend_from_slice(&value.change_locator.source_pack_id.to_be_bytes());
    out.extend_from_slice(&value.change_locator.source_ordinal.to_be_bytes());
    push_sized_bytes(out, value.change_locator.change_id.as_bytes());
    push_sized_bytes(out, value.created_at.as_bytes());
    push_sized_bytes(out, value.updated_at.as_bytes());
    push_optional_json_ref(out, value.snapshot_ref);
    push_optional_json_ref(out, value.metadata_ref);
}

#[cfg(test)]
pub(crate) fn encoded_value_len(value: &TrackedStateIndexValue) -> usize {
    1 + sized_bytes_len(value.change_locator.source_commit_id.as_bytes())
        + 4
        + 4
        + sized_bytes_len(value.change_locator.change_id.as_bytes())
        + sized_bytes_len(value.created_at.as_bytes())
        + sized_bytes_len(value.updated_at.as_bytes())
        + optional_json_ref_len(value.snapshot_ref.as_ref())
        + optional_json_ref_len(value.metadata_ref.as_ref())
}

pub(crate) fn decode_value(bytes: &[u8]) -> Result<TrackedStateIndexValue, LixError> {
    let mut cursor = 0usize;
    let value_header = read_u8(bytes, &mut cursor, "value header")?;
    let deleted = decode_value_header(value_header)?;
    decode_value_after_header(bytes, cursor, deleted)
}

pub(crate) fn decode_visible_value(
    bytes: &[u8],
    include_tombstones: bool,
) -> Result<Option<TrackedStateIndexValue>, LixError> {
    let mut cursor = 0usize;
    let value_header = read_u8(bytes, &mut cursor, "value header")?;
    let deleted = decode_value_header(value_header)?;
    if deleted && !include_tombstones {
        return Ok(None);
    }
    decode_value_after_header(bytes, cursor, deleted).map(Some)
}

pub(crate) fn decode_value_deleted(bytes: &[u8]) -> Result<bool, LixError> {
    let value_header = *bytes.first().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state tree value is truncated before header",
        )
    })?;
    decode_value_header(value_header)
}

fn decode_value_header(value_header: u8) -> Result<bool, LixError> {
    let version = value_header & VALUE_VERSION_MASK;
    let deleted = value_header & VALUE_DELETED_FLAG != 0;
    if version != VALUE_VERSION {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("unsupported tracked-state tree value version {version}"),
        ));
    }
    Ok(deleted)
}

fn decode_value_after_header(
    bytes: &[u8],
    mut cursor: usize,
    deleted: bool,
) -> Result<TrackedStateIndexValue, LixError> {
    let source_commit_id = read_sized_string(bytes, &mut cursor, "source_commit_id")?;
    let source_pack_id =
        u32::try_from(read_u32(bytes, &mut cursor, "source_pack_id")?).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked-state source_pack_id exceeds u32",
            )
        })?;
    let source_ordinal =
        u32::try_from(read_u32(bytes, &mut cursor, "source_ordinal")?).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked-state source_ordinal exceeds u32",
            )
        })?;
    let change_id = read_sized_string(bytes, &mut cursor, "change_id")?;
    let created_at = read_sized_string(bytes, &mut cursor, "created_at")?;
    let updated_at = read_sized_string(bytes, &mut cursor, "updated_at")?;
    let snapshot_ref = read_optional_json_ref(bytes, &mut cursor, "snapshot_ref")?;
    let metadata_ref = read_optional_json_ref(bytes, &mut cursor, "metadata_ref")?;
    if cursor != bytes.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state tree value decode found trailing bytes",
        ));
    }
    Ok(TrackedStateIndexValue {
        change_locator: ChangeLocator {
            source_commit_id,
            source_pack_id,
            source_ordinal,
            change_id,
        },
        deleted,
        snapshot_ref,
        metadata_ref,
        created_at,
        updated_at,
    })
}

pub(crate) fn encode_delta_pack_refs(
    deltas: &[TrackedStateDeltaRef<'_>],
) -> Result<Vec<u8>, LixError> {
    let mut out = Vec::new();
    out.extend_from_slice(b"LXTD");
    out.push(DELTA_PACK_VERSION);
    push_u32(&mut out, deltas.len());
    for delta in deltas {
        push_sized_section(&mut out, |out| {
            append_key_ref(
                out,
                TrackedStateKeyRef {
                    schema_key: delta.change.schema_key,
                    file_id: delta.change.file_id,
                    entity_id: delta.change.entity_id,
                },
            );
        });
        push_sized_section(&mut out, |out| {
            append_value_ref(
                out,
                TrackedStateIndexValueRef {
                    change_locator: delta.locator,
                    deleted: delta.change.snapshot_ref.is_none(),
                    snapshot_ref: delta.change.snapshot_ref,
                    metadata_ref: delta.change.metadata_ref,
                    created_at: delta.created_at,
                    updated_at: delta.updated_at,
                },
            );
        });
    }
    Ok(out)
}

fn push_sized_section(out: &mut Vec<u8>, write: impl FnOnce(&mut Vec<u8>)) {
    let len_offset = out.len();
    push_u32(out, 0);
    let content_start = out.len();
    write(out);
    let len = out.len() - content_start;
    out[len_offset..len_offset + 4].copy_from_slice(&(len as u32).to_be_bytes());
}

pub(crate) fn decode_delta_pack(bytes: &[u8]) -> Result<Vec<TrackedStateDeltaEntry>, LixError> {
    let mut cursor = 0usize;
    let magic = bytes.get(0..4).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state delta pack is truncated before magic",
        )
    })?;
    if magic != b"LXTD" {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state delta pack has invalid magic",
        ));
    }
    cursor += 4;
    let version = read_u8(bytes, &mut cursor, "delta pack version")?;
    if version != DELTA_PACK_VERSION {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("unsupported tracked-state delta pack version {version}"),
        ));
    }
    let count = read_u32(bytes, &mut cursor, "delta pack entry count")?;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let key = decode_key(&read_sized_bytes(bytes, &mut cursor, "delta key")?)?;
        let value = decode_value(&read_sized_bytes(bytes, &mut cursor, "delta value")?)?;
        entries.push(TrackedStateDeltaEntry { key, value });
    }
    if cursor != bytes.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state delta pack decode found trailing bytes",
        ));
    }
    Ok(entries)
}

#[cfg(test)]
fn sized_bytes_len(bytes: &[u8]) -> usize {
    4 + bytes.len()
}

pub(crate) fn encode_leaf_node(entries: &[EncodedLeafEntry]) -> Vec<u8> {
    let entries = entries
        .iter()
        .map(EncodedLeafEntry::as_ref)
        .collect::<Vec<_>>();
    encode_leaf_node_refs(&entries)
}

pub(crate) fn encode_leaf_node_refs(entries: &[EncodedLeafEntryRef<'_>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(NODE_KIND_LEAF);
    out.push(NODE_VERSION);
    push_u32(&mut out, entries.len());

    let mut offsets = Vec::with_capacity(entries.len().saturating_add(1));
    let mut payload = Vec::new();
    offsets.push(0usize);
    for entry in entries {
        push_sized_bytes(&mut payload, entry.key);
        push_sized_bytes(&mut payload, entry.value);
        offsets.push(payload.len());
    }
    for offset in offsets {
        push_u32(&mut out, offset);
    }
    out.extend_from_slice(&payload);
    out
}

pub(crate) fn encode_internal_node(children: &[ChildSummary]) -> Vec<u8> {
    let children = children
        .iter()
        .map(ChildSummary::as_ref)
        .collect::<Vec<_>>();
    encode_internal_node_refs(&children)
}

pub(crate) fn encode_internal_node_refs(children: &[ChildSummaryRef<'_>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(NODE_KIND_INTERNAL);
    out.push(NODE_VERSION);
    push_u32(&mut out, children.len());
    for child in children {
        push_sized_bytes(&mut out, child.first_key);
        push_sized_bytes(&mut out, child.last_key);
        out.extend_from_slice(&child.child_hash);
        out.extend_from_slice(&child.subtree_count.to_be_bytes());
    }
    out
}

pub(crate) fn decode_node(bytes: &[u8]) -> Result<DecodedNode, LixError> {
    match decode_node_ref(bytes)? {
        DecodedNodeRef::Leaf(leaf) => {
            let mut entries = Vec::with_capacity(leaf.len());
            for index in 0..leaf.len() {
                let entry = leaf.entry(index)?.ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "tracked-state leaf entry disappeared during owned decode",
                    )
                })?;
                entries.push(EncodedLeafEntry {
                    key: entry.key.to_vec(),
                    value: entry.value.to_vec(),
                });
            }
            Ok(DecodedNode::Leaf(DecodedLeafNode { entries }))
        }
        DecodedNodeRef::Internal(internal) => Ok(DecodedNode::Internal(internal)),
    }
}

pub(crate) fn decode_node_ref(bytes: &[u8]) -> Result<DecodedNodeRef<'_>, LixError> {
    let mut cursor = 0usize;
    let kind = read_u8(bytes, &mut cursor, "node kind")?;
    let version = read_u8(bytes, &mut cursor, "node version")?;
    if version != NODE_VERSION {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("unsupported tracked-state tree node version {version}"),
        ));
    }
    let count = read_u32(bytes, &mut cursor, "entry count")?;
    let node = match kind {
        NODE_KIND_LEAF => {
            let leaf = decode_leaf_node_ref_after_count(bytes, &mut cursor, count)?;
            DecodedNodeRef::Leaf(leaf)
        }
        NODE_KIND_INTERNAL => {
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                let first_key = read_sized_bytes(bytes, &mut cursor, "internal first_key")?;
                let last_key = read_sized_bytes(bytes, &mut cursor, "internal last_key")?;
                let child_hash = read_fixed_hash(bytes, &mut cursor, "internal child_hash")?;
                let subtree_count = read_u64(bytes, &mut cursor, "internal subtree_count")?;
                children.push(ChildSummary {
                    first_key,
                    last_key,
                    child_hash,
                    subtree_count,
                });
            }
            DecodedNodeRef::Internal(DecodedInternalNode { children })
        }
        other => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("unknown tracked-state tree node kind {other}"),
            ))
        }
    };
    if cursor != bytes.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state tree node decode found trailing bytes",
        ));
    }
    Ok(node)
}

fn decode_leaf_node_ref_after_count<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    count: usize,
) -> Result<DecodedLeafNodeRef<'a>, LixError> {
    let mut offsets = Vec::with_capacity(count.saturating_add(1));
    for _ in 0..=count {
        offsets.push(read_u32(bytes, cursor, "leaf entry offset")?);
    }
    if offsets.first().copied() != Some(0) {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state leaf offset table must start at zero",
        ));
    }
    for window in offsets.windows(2) {
        if window[0] > window[1] {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state leaf offsets must be monotonic",
            ));
        }
    }
    let payload_len = bytes.len().checked_sub(*cursor).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state leaf payload start is past node end",
        )
    })?;
    if offsets.last().copied().unwrap_or_default() != payload_len {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state leaf offset table does not cover full payload",
        ));
    }
    let payload_start = *cursor;
    *cursor = bytes.len();
    Ok(DecodedLeafNodeRef {
        bytes,
        payload_start,
        offsets,
    })
}

pub(crate) fn child_summary_from_node(
    node_bytes: Vec<u8>,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    subtree_count: u64,
) -> (PendingChunkWrite, ChildSummary) {
    let hash = hash_bytes(&node_bytes);
    (
        PendingChunkWrite {
            hash,
            data: node_bytes,
        },
        ChildSummary {
            first_key,
            last_key,
            child_hash: hash,
            subtree_count,
        },
    )
}

pub(crate) fn boundary_trigger(
    encoded_key: &[u8],
    level: usize,
    chunk_size: usize,
    item_size: usize,
    target_chunk_bytes: usize,
) -> bool {
    if item_size == 0 || target_chunk_bytes == 0 {
        return false;
    }

    let start =
        weibull_cdf(chunk_size.saturating_sub(item_size) as f64 / target_chunk_bytes as f64);
    let end = weibull_cdf(chunk_size as f64 / target_chunk_bytes as f64);
    let remaining = 1.0 - start;
    if remaining <= 0.0 {
        return true;
    }

    let split_probability = ((end - start) / remaining).clamp(0.0, 1.0);
    let hash = xxh3_64_with_seed(encoded_key, level_salt(level));
    (hash as f64) < split_probability * (u64::MAX as f64)
}

fn weibull_cdf(normalized_size: f64) -> f64 {
    if normalized_size <= 0.0 {
        return 0.0;
    }
    -f64::exp_m1(-normalized_size.powi(WEIBULL_K))
}

fn level_salt(level: usize) -> u64 {
    let mut value = (level as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn push_entity_identity(out: &mut Vec<u8>, identity: &EntityIdentity) {
    assert!(
        !identity.parts.is_empty(),
        "tracked-state key entity identity must contain at least one part"
    );
    for part in &identity.parts {
        out.push(ENTITY_IDENTITY_STRING);
        push_sized_bytes(out, part.as_bytes());
    }
    out.push(ENTITY_IDENTITY_END);
}

fn read_entity_identity(bytes: &[u8], cursor: &mut usize) -> Result<EntityIdentity, LixError> {
    let mut parts = Vec::new();
    loop {
        let tag = read_u8(bytes, cursor, "entity identity part tag")?;
        match tag {
            ENTITY_IDENTITY_END => break,
            ENTITY_IDENTITY_STRING => {
                parts.push(read_sized_string(
                    bytes,
                    cursor,
                    "entity identity string part",
                )?);
            }
            other => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("tracked-state tree key has invalid entity identity part tag {other}"),
                ))
            }
        }
    }
    if parts.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state tree key entity identity must contain at least one part",
        ));
    }
    Ok(EntityIdentity { parts })
}

fn push_sized_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    push_u32(out, bytes.len());
    out.extend_from_slice(bytes);
}

fn push_optional_json_ref(out: &mut Vec<u8>, json_ref: Option<&JsonRef>) {
    match json_ref {
        Some(json_ref) => {
            out.push(1);
            out.extend_from_slice(json_ref.as_hash_bytes());
        }
        None => out.push(0),
    }
}

#[cfg(test)]
fn optional_json_ref_len(json_ref: Option<&JsonRef>) -> usize {
    1 + json_ref.map_or(0, |_| TRACKED_STATE_HASH_BYTES)
}

fn push_u32(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&(value as u32).to_be_bytes());
}

fn read_sized_string(
    bytes: &[u8],
    cursor: &mut usize,
    field_name: &str,
) -> Result<String, LixError> {
    String::from_utf8(read_sized_bytes(bytes, cursor, field_name)?).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked-state tree field '{field_name}' is invalid UTF-8: {error}"),
        )
    })
}

fn read_sized_bytes(
    bytes: &[u8],
    cursor: &mut usize,
    field_name: &str,
) -> Result<Vec<u8>, LixError> {
    read_sized_slice(bytes, cursor, field_name).map(<[u8]>::to_vec)
}

fn read_sized_slice<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    field_name: &str,
) -> Result<&'a [u8], LixError> {
    let len = read_u32(bytes, cursor, field_name)?;
    let end = cursor.checked_add(len).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked-state tree field '{field_name}' length overflow"),
        )
    })?;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked-state tree field '{field_name}' is truncated"),
        )
    })?;
    *cursor = end;
    Ok(slice)
}

fn read_fixed_hash(
    bytes: &[u8],
    cursor: &mut usize,
    field_name: &str,
) -> Result<[u8; TRACKED_STATE_HASH_BYTES], LixError> {
    let end = *cursor + TRACKED_STATE_HASH_BYTES;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked-state tree field '{field_name}' is truncated"),
        )
    })?;
    let mut out = [0_u8; TRACKED_STATE_HASH_BYTES];
    out.copy_from_slice(slice);
    *cursor = end;
    Ok(out)
}

fn read_optional_json_ref(
    bytes: &[u8],
    cursor: &mut usize,
    field_name: &str,
) -> Result<Option<JsonRef>, LixError> {
    match read_u8(bytes, cursor, field_name)? {
        0 => Ok(None),
        1 => Ok(Some(JsonRef::from_hash_bytes(read_fixed_hash(
            bytes, cursor, field_name,
        )?))),
        other => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked-state tree field '{field_name}' has invalid JSON ref tag {other}"),
        )),
    }
}

fn read_u8(bytes: &[u8], cursor: &mut usize, field_name: &str) -> Result<u8, LixError> {
    let value = *bytes.get(*cursor).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked-state tree field '{field_name}' is truncated"),
        )
    })?;
    *cursor += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], cursor: &mut usize, field_name: &str) -> Result<usize, LixError> {
    let end = *cursor + 4;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked-state tree field '{field_name}' is truncated"),
        )
    })?;
    let mut out = [0_u8; 4];
    out.copy_from_slice(slice);
    *cursor = end;
    Ok(u32::from_be_bytes(out) as usize)
}

fn read_u64(bytes: &[u8], cursor: &mut usize, field_name: &str) -> Result<u64, LixError> {
    let end = *cursor + 8;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked-state tree field '{field_name}' is truncated"),
        )
    })?;
    let mut out = [0_u8; 8];
    out.copy_from_slice(slice);
    *cursor = end;
    Ok(u64::from_be_bytes(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_codec_distinguishes_null_and_value_file_id() {
        let null_key = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_id: EntityIdentity::single("entity"),
        });
        let file_key = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: Some("file".to_string()),
            entity_id: EntityIdentity::single("entity"),
        });

        assert_ne!(null_key, file_key);
        assert_eq!(
            decode_key(&null_key).expect("null key"),
            TrackedStateKey {
                schema_key: "schema".to_string(),
                file_id: None,
                entity_id: EntityIdentity::single("entity"),
            }
        );
        assert_eq!(
            decode_key(&file_key).expect("file key"),
            TrackedStateKey {
                schema_key: "schema".to_string(),
                file_id: Some("file".to_string()),
                entity_id: EntityIdentity::single("entity"),
            }
        );
    }

    #[test]
    fn key_codec_encodes_composite_identity_as_string_tuple_parts() {
        let key = TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_id: EntityIdentity {
                parts: vec![
                    "namespace".to_string(),
                    "true".to_string(),
                    "42".to_string(),
                ],
            },
        };

        let encoded = encode_key(&key);

        assert_eq!(decode_key(&encoded).expect("key should decode"), key);
    }

    #[test]
    fn key_codec_rejects_non_string_identity_part_tags() {
        let mut encoded = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_id: EntityIdentity {
                parts: vec!["true".to_string()],
            },
        });
        let schema_key_len = "schema".len();
        let file_scope_offset = 4 + schema_key_len;
        let entity_tag_offset = file_scope_offset + 1;
        encoded[entity_tag_offset] = 2;

        let error = decode_key(&encoded).expect_err("non-string identity tag should reject");
        assert!(error
            .to_string()
            .contains("invalid entity identity part tag 2"));
    }

    #[test]
    fn key_codec_preserves_tuple_prefix_ordering() {
        let prefix = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_id: EntityIdentity {
                parts: vec!["a".to_string()],
            },
        });
        let extended = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_id: EntityIdentity {
                parts: vec!["a".to_string(), "b".to_string()],
            },
        });

        assert!(prefix < extended);
    }

    #[test]
    fn value_codec_roundtrips_locator_value() {
        let value = TrackedStateIndexValue {
            change_locator: ChangeLocator {
                source_commit_id: "commit".to_string(),
                source_pack_id: 7,
                source_ordinal: 11,
                change_id: "change".to_string(),
            },
            deleted: false,
            snapshot_ref: Some(JsonRef::from_hash_bytes([1; 32])),
            metadata_ref: Some(JsonRef::from_hash_bytes([2; 32])),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-02T00:00:00Z".to_string(),
        };

        let encoded = encode_value(&value);
        assert_eq!(decode_value(&encoded).expect("value"), value);
    }

    #[test]
    fn value_codec_roundtrips_second_locator_value() {
        let value = TrackedStateIndexValue {
            change_locator: ChangeLocator {
                source_commit_id: "other-commit".to_string(),
                source_pack_id: 0,
                source_ordinal: 1,
                change_id: "other-change".to_string(),
            },
            deleted: true,
            snapshot_ref: None,
            metadata_ref: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-02T00:00:00Z".to_string(),
        };

        let encoded = encode_value(&value);
        assert_eq!(decode_value(&encoded).expect("value"), value);
    }

    #[test]
    fn value_deleted_header_decode_does_not_require_full_value() {
        let live = TrackedStateIndexValue {
            change_locator: ChangeLocator {
                source_commit_id: "commit".to_string(),
                source_pack_id: 0,
                source_ordinal: 1,
                change_id: "change".to_string(),
            },
            deleted: false,
            snapshot_ref: None,
            metadata_ref: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-02T00:00:00Z".to_string(),
        };
        let mut tombstone = live.clone();
        tombstone.deleted = true;

        assert!(!decode_value_deleted(&encode_value(&live)).expect("live header"));
        assert!(decode_value_deleted(&encode_value(&tombstone)).expect("tombstone header"));
        assert!(decode_value_deleted(&[]).is_err());

        let mut unsupported = encode_value(&live);
        unsupported[0] = VALUE_VERSION + 1;
        assert!(decode_value_deleted(&unsupported)
            .expect_err("unsupported version should reject")
            .to_string()
            .contains("unsupported tracked-state tree value version"));
    }

    #[test]
    fn delta_pack_ref_encoder_roundtrips_entries() {
        let entity_id = EntityIdentity {
            parts: vec!["entity-a".to_string()],
        };
        let snapshot_ref = JsonRef::from_hash_bytes([1; 32]);
        let metadata_ref = JsonRef::from_hash_bytes([2; 32]);
        let live_change = crate::commit_store::ChangeRef {
            id: "change-live",
            entity_id: &entity_id,
            schema_key: "schema",
            file_id: Some("file-a"),
            snapshot_ref: Some(&snapshot_ref),
            metadata_ref: Some(&metadata_ref),
            created_at: "2026-01-01T00:00:00Z",
        };
        let tombstone_change = crate::commit_store::ChangeRef {
            id: "change-deleted",
            entity_id: &entity_id,
            schema_key: "schema",
            file_id: None,
            snapshot_ref: None,
            metadata_ref: None,
            created_at: "2026-01-01T00:00:00Z",
        };
        let live_locator = crate::commit_store::ChangeLocatorRef {
            source_commit_id: "commit-a",
            source_pack_id: 3,
            source_ordinal: 5,
            change_id: "change-live",
        };
        let tombstone_locator = crate::commit_store::ChangeLocatorRef {
            source_commit_id: "commit-a",
            source_pack_id: 3,
            source_ordinal: 6,
            change_id: "change-deleted",
        };
        let encoded = encode_delta_pack_refs(&[
            TrackedStateDeltaRef {
                change: live_change,
                locator: live_locator,
                created_at: "2026-01-01T00:00:00Z",
                updated_at: "2026-01-02T00:00:00Z",
            },
            TrackedStateDeltaRef {
                change: tombstone_change,
                locator: tombstone_locator,
                created_at: "2026-01-03T00:00:00Z",
                updated_at: "2026-01-04T00:00:00Z",
            },
        ])
        .expect("delta pack should encode");

        let decoded = decode_delta_pack(&encoded).expect("delta pack should decode");

        assert_eq!(
            decoded,
            vec![
                TrackedStateDeltaEntry {
                    key: TrackedStateKey {
                        schema_key: "schema".to_string(),
                        file_id: Some("file-a".to_string()),
                        entity_id: entity_id.clone(),
                    },
                    value: TrackedStateIndexValue {
                        change_locator: ChangeLocator {
                            source_commit_id: "commit-a".to_string(),
                            source_pack_id: 3,
                            source_ordinal: 5,
                            change_id: "change-live".to_string(),
                        },
                        deleted: false,
                        snapshot_ref: Some(snapshot_ref),
                        metadata_ref: Some(metadata_ref),
                        created_at: "2026-01-01T00:00:00Z".to_string(),
                        updated_at: "2026-01-02T00:00:00Z".to_string(),
                    },
                },
                TrackedStateDeltaEntry {
                    key: TrackedStateKey {
                        schema_key: "schema".to_string(),
                        file_id: None,
                        entity_id,
                    },
                    value: TrackedStateIndexValue {
                        change_locator: ChangeLocator {
                            source_commit_id: "commit-a".to_string(),
                            source_pack_id: 3,
                            source_ordinal: 6,
                            change_id: "change-deleted".to_string(),
                        },
                        deleted: true,
                        snapshot_ref: None,
                        metadata_ref: None,
                        created_at: "2026-01-03T00:00:00Z".to_string(),
                        updated_at: "2026-01-04T00:00:00Z".to_string(),
                    },
                },
            ]
        );
    }

    #[test]
    fn encoded_value_len_matches_encoded_value_bytes() {
        let values = [
            TrackedStateIndexValue {
                change_locator: ChangeLocator {
                    source_commit_id: "commit".to_string(),
                    source_pack_id: 0,
                    source_ordinal: 0,
                    change_id: "change".to_string(),
                },
                deleted: false,
                snapshot_ref: None,
                metadata_ref: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-02T00:00:00Z".to_string(),
            },
            TrackedStateIndexValue {
                change_locator: ChangeLocator {
                    source_commit_id: "commit".to_string(),
                    source_pack_id: 1,
                    source_ordinal: 2,
                    change_id: "change-2".to_string(),
                },
                deleted: true,
                snapshot_ref: Some(JsonRef::from_hash_bytes([3; 32])),
                metadata_ref: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-02T00:00:00Z".to_string(),
            },
            TrackedStateIndexValue {
                change_locator: ChangeLocator {
                    source_commit_id: "other".to_string(),
                    source_pack_id: 4,
                    source_ordinal: 8,
                    change_id: "change-3".to_string(),
                },
                deleted: false,
                snapshot_ref: None,
                metadata_ref: Some(JsonRef::from_hash_bytes([4; 32])),
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-02T00:00:00Z".to_string(),
            },
        ];

        for value in values {
            assert_eq!(encoded_value_len(&value), encode_value(&value).len());
        }
    }

    #[test]
    fn leaf_node_codec_uses_indexable_offset_table() {
        let entries = vec![
            EncodedLeafEntry {
                key: b"alpha".to_vec(),
                value: b"one".to_vec(),
            },
            EncodedLeafEntry {
                key: b"bravo".to_vec(),
                value: b"two-two".to_vec(),
            },
        ];

        let encoded = encode_leaf_node(&entries);
        assert_eq!(encoded[0], NODE_KIND_LEAF);
        assert_eq!(encoded[1], NODE_VERSION);
        assert_eq!(&encoded[2..6], 2u32.to_be_bytes().as_slice());
        assert_eq!(&encoded[6..10], 0u32.to_be_bytes().as_slice());

        let DecodedNodeRef::Leaf(leaf) = decode_node_ref(&encoded).expect("leaf ref") else {
            panic!("expected leaf node");
        };
        assert_eq!(leaf.len(), 2);
        assert_eq!(leaf.key(1).expect("second key"), Some(b"bravo".as_slice()));
        let second = leaf
            .entry(1)
            .expect("second entry")
            .expect("second entry exists");
        assert_eq!(second.key, b"bravo");
        assert_eq!(second.value, b"two-two");

        let DecodedNode::Leaf(owned) = decode_node(&encoded).expect("owned leaf") else {
            panic!("expected owned leaf node");
        };
        assert_eq!(owned.entries(), entries.as_slice());
    }

    #[test]
    fn leaf_node_codec_roundtrips_empty_leaf() {
        let encoded = encode_leaf_node(&[]);
        assert_eq!(encoded.len(), 10);

        let DecodedNodeRef::Leaf(leaf) = decode_node_ref(&encoded).expect("leaf ref") else {
            panic!("expected leaf node");
        };
        assert_eq!(leaf.len(), 0);
        assert!(leaf.entry(0).expect("missing entry").is_none());
    }

    #[test]
    fn leaf_node_codec_rejects_malformed_offsets() {
        let entries = vec![
            EncodedLeafEntry {
                key: b"alpha".to_vec(),
                value: b"one".to_vec(),
            },
            EncodedLeafEntry {
                key: b"bravo".to_vec(),
                value: b"two".to_vec(),
            },
        ];
        let encoded = encode_leaf_node(&entries);

        let mut non_zero_first = encoded.clone();
        non_zero_first[6..10].copy_from_slice(&1u32.to_be_bytes());
        assert!(decode_node_ref(&non_zero_first)
            .expect_err("non-zero first offset should reject")
            .to_string()
            .contains("offset table must start at zero"));

        let mut non_monotonic = encoded.clone();
        non_monotonic[10..14].copy_from_slice(&100u32.to_be_bytes());
        assert!(decode_node_ref(&non_monotonic)
            .expect_err("non-monotonic offsets should reject")
            .to_string()
            .contains("offsets must be monotonic"));

        let mut short_coverage = encoded;
        let payload_len = short_coverage.len() - 18;
        short_coverage[14..18].copy_from_slice(&((payload_len - 1) as u32).to_be_bytes());
        assert!(decode_node_ref(&short_coverage)
            .expect_err("short offset coverage should reject")
            .to_string()
            .contains("offset table does not cover full payload"));
    }

    #[test]
    fn content_hash_is_blake3() {
        assert_eq!(hash_bytes(b"abc"), *blake3::hash(b"abc").as_bytes());
    }

    #[test]
    fn boundary_decisions_are_xxh3_based_and_deterministic() {
        let left = boundary_trigger(b"key", 0, 4096, 128, 4096);
        let right = boundary_trigger(b"key", 0, 4096, 128, 4096);
        assert_eq!(left, right);
    }
}
