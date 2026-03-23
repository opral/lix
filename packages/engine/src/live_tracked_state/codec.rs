use crate::live_tracked_state::types::{
    LiveTrackedCodecProfile, LiveTrackedEntityKey, LiveTrackedEntityValue, LiveTrackedFieldValue,
    LiveTrackedPayloadColumn, LiveTrackedRow, LiveTrackedStateOptions, LiveTrackedValueRef,
    LIVE_TRACKED_HASH_BYTES,
};
use crate::{
    CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId, LixError,
};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_64_with_seed;

const NODE_VERSION: u8 = 2;
const NODE_KIND_LEAF: u8 = 1;
const NODE_KIND_INTERNAL: u8 = 2;

const VALUE_KIND_NULL: u8 = 0;
const VALUE_KIND_BOOLEAN_FALSE: u8 = 1;
const VALUE_KIND_BOOLEAN_TRUE: u8 = 2;
const VALUE_KIND_INTEGER: u8 = 3;
const VALUE_KIND_REAL: u8 = 4;
const VALUE_KIND_TEXT: u8 = 5;
const VALUE_KIND_JSON: u8 = 6;
const VALUE_KIND_BLOB: u8 = 7;
const VALUE_KIND_LARGE_TEXT: u8 = 8;
const VALUE_KIND_LARGE_JSON: u8 = 9;
const VALUE_KIND_LARGE_BLOB: u8 = 10;
const WEIBULL_K: i32 = 4;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PendingValueWrite {
    pub hash: [u8; LIVE_TRACKED_HASH_BYTES],
    pub data: Vec<u8>,
    pub size_bytes: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PendingChunkWrite {
    pub hash: [u8; LIVE_TRACKED_HASH_BYTES],
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EncodedLeafEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ChildSummary {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub child_hash: [u8; LIVE_TRACKED_HASH_BYTES],
    pub subtree_count: u64,
}

#[derive(Debug, Clone)]
pub(crate) enum DecodedNode {
    Leaf(DecodedLeafNode),
    Internal(DecodedInternalNode),
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedLeafNode {
    raw: Arc<Vec<u8>>,
    key_offsets: Vec<u16>,
    value_offsets: Vec<u16>,
    key_bytes_start: usize,
    key_bytes_len: usize,
    value_bytes_start: usize,
    value_bytes_len: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedInternalNode {
    raw: Arc<Vec<u8>>,
    first_key_offsets: Vec<u16>,
    last_key_offsets: Vec<u16>,
    child_hashes: Vec<[u8; LIVE_TRACKED_HASH_BYTES]>,
    #[allow(dead_code)]
    subtree_counts: Vec<u64>,
    first_key_bytes_start: usize,
    first_key_bytes_len: usize,
    last_key_bytes_start: usize,
    last_key_bytes_len: usize,
}

impl DecodedNode {
    pub(crate) fn decode(bytes: Vec<u8>) -> Result<Self, LixError> {
        decode_node(Arc::new(bytes))
    }
}

impl DecodedLeafNode {
    pub(crate) fn entry_count(&self) -> usize {
        self.key_offsets.len()
    }

    pub(crate) fn key_at(&self, index: usize) -> &[u8] {
        region_slice(
            &self.raw,
            self.key_bytes_start,
            self.key_bytes_len,
            &self.key_offsets,
            index,
        )
    }

    pub(crate) fn value_at(&self, index: usize) -> &[u8] {
        region_slice(
            &self.raw,
            self.value_bytes_start,
            self.value_bytes_len,
            &self.value_offsets,
            index,
        )
    }
}

impl DecodedInternalNode {
    pub(crate) fn child_count(&self) -> usize {
        self.child_hashes.len()
    }

    pub(crate) fn first_key_at(&self, index: usize) -> &[u8] {
        region_slice(
            &self.raw,
            self.first_key_bytes_start,
            self.first_key_bytes_len,
            &self.first_key_offsets,
            index,
        )
    }

    pub(crate) fn last_key_at(&self, index: usize) -> &[u8] {
        region_slice(
            &self.raw,
            self.last_key_bytes_start,
            self.last_key_bytes_len,
            &self.last_key_offsets,
            index,
        )
    }

    pub(crate) fn child_hash_at(&self, index: usize) -> &[u8; LIVE_TRACKED_HASH_BYTES] {
        &self.child_hashes[index]
    }

    #[allow(dead_code)]
    pub(crate) fn subtree_count_at(&self, index: usize) -> u64 {
        self.subtree_counts[index]
    }
}

pub(crate) fn hash_bytes(bytes: &[u8]) -> [u8; LIVE_TRACKED_HASH_BYTES] {
    *blake3::hash(bytes).as_bytes()
}

pub(crate) fn encode_entity_key(key: &LiveTrackedEntityKey) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        key.schema_key.as_str().len()
            + key.file_id.as_str().len()
            + key.entity_id.as_str().len()
            + 12,
    );
    push_sized_bytes(&mut out, key.schema_key.as_str().as_bytes());
    push_sized_bytes(&mut out, key.file_id.as_str().as_bytes());
    push_sized_bytes(&mut out, key.entity_id.as_str().as_bytes());
    out
}

pub(crate) fn decode_entity_key(bytes: &[u8]) -> Result<LiveTrackedEntityKey, LixError> {
    let mut cursor = 0;
    let schema = read_sized_utf8(bytes, &mut cursor, "schema_key")?;
    let file = read_sized_utf8(bytes, &mut cursor, "file_id")?;
    let entity = read_sized_utf8(bytes, &mut cursor, "entity_id")?;
    if cursor != bytes.len() {
        return Err(LixError::unknown(
            "live tracked key decode found trailing bytes",
        ));
    }
    Ok(LiveTrackedEntityKey::new(
        CanonicalSchemaKey::try_from(schema.as_str())?,
        FileId::try_from(file.as_str())?,
        EntityId::try_from(entity.as_str())?,
    ))
}

#[allow(dead_code)]
pub(crate) fn compare_encoded_keys(left: &[u8], right: &[u8]) -> Ordering {
    let mut left_cursor = 0;
    let mut right_cursor = 0;
    for _ in 0..3 {
        let left_bytes = read_sized_bytes_lossy(left, &mut left_cursor);
        let right_bytes = read_sized_bytes_lossy(right, &mut right_cursor);
        let ordering = left_bytes.cmp(right_bytes);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

pub(crate) fn compare_encoded_key_to_key(encoded: &[u8], key: &LiveTrackedEntityKey) -> Ordering {
    let mut cursor = 0;
    for target in [
        key.schema_key.as_str().as_bytes(),
        key.file_id.as_str().as_bytes(),
        key.entity_id.as_str().as_bytes(),
    ] {
        let current = read_sized_bytes_lossy(encoded, &mut cursor);
        let ordering = current.cmp(target);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

pub(crate) fn encode_entity_value_canonical(
    value: &LiveTrackedEntityValue,
    options: &LiveTrackedStateOptions,
    value_writes: &mut BTreeMap<[u8; LIVE_TRACKED_HASH_BYTES], PendingValueWrite>,
) -> Result<Vec<u8>, LixError> {
    let mut out = Vec::with_capacity(value.logical_len() + 64);
    push_entity_value_header(&mut out, value);
    push_varint(&mut out, value.columns.len());
    if value.columns.len() <= 1 {
        for column in &value.columns {
            push_sized_bytes(&mut out, column.name.as_bytes());
            encode_field_value_canonical(&mut out, &column.value, options, value_writes);
        }
        return Ok(out);
    }

    let mut needs_sort = false;
    for pair in value.columns.windows(2) {
        match pair[0].name.cmp(&pair[1].name) {
            Ordering::Less => {}
            Ordering::Equal => {
                return Err(LixError::unknown(
                    "live tracked payload column names must be unique",
                ))
            }
            Ordering::Greater => {
                needs_sort = true;
                break;
            }
        }
    }

    if !needs_sort {
        for column in &value.columns {
            push_sized_bytes(&mut out, column.name.as_bytes());
            encode_field_value_canonical(&mut out, &column.value, options, value_writes);
        }
        return Ok(out);
    }

    let mut order = (0..value.columns.len()).collect::<Vec<_>>();
    order
        .sort_unstable_by(|left, right| value.columns[*left].name.cmp(&value.columns[*right].name));
    for pair in order.windows(2) {
        if value.columns[pair[0]].name == value.columns[pair[1]].name {
            return Err(LixError::unknown(
                "live tracked payload column names must be unique",
            ));
        }
    }
    for index in order {
        let column = &value.columns[index];
        push_sized_bytes(&mut out, column.name.as_bytes());
        encode_field_value_canonical(&mut out, &column.value, options, value_writes);
    }
    Ok(out)
}

pub(crate) fn decode_entity_value(bytes: &[u8]) -> Result<LiveTrackedEntityValue, LixError> {
    let mut cursor = 0;
    let change_id = read_sized_utf8(bytes, &mut cursor, "change_id")?;
    let tombstone = *bytes
        .get(cursor)
        .ok_or_else(|| LixError::unknown("live tracked value missing tombstone byte"))?
        != 0;
    cursor += 1;
    let schema_version = CanonicalSchemaVersion::try_from(
        read_sized_utf8(bytes, &mut cursor, "schema_version")?.as_str(),
    )?;
    let plugin_key =
        CanonicalPluginKey::try_from(read_sized_utf8(bytes, &mut cursor, "plugin_key")?.as_str())?;
    let metadata = match bytes.get(cursor) {
        Some(0) => {
            cursor += 1;
            None
        }
        Some(1) => {
            cursor += 1;
            Some(read_sized_utf8(bytes, &mut cursor, "metadata")?)
        }
        _ => {
            return Err(LixError::unknown(
                "live tracked value missing metadata presence byte",
            ))
        }
    };
    let column_count = read_varint(bytes, &mut cursor)?;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let name = read_sized_utf8(bytes, &mut cursor, "column name")?;
        let value = decode_field_value(bytes, &mut cursor)?;
        columns.push(LiveTrackedPayloadColumn { name, value });
    }
    if cursor != bytes.len() {
        return Err(LixError::unknown(
            "live tracked value decode found trailing bytes",
        ));
    }
    Ok(LiveTrackedEntityValue {
        change_id,
        tombstone,
        schema_version,
        plugin_key,
        metadata,
        columns,
    })
}

pub(crate) fn encode_leaf_node(entries: &[EncodedLeafEntry]) -> Vec<u8> {
    let key_bytes_len = entries.iter().map(|entry| entry.key.len()).sum::<usize>();
    let value_bytes_len = entries.iter().map(|entry| entry.value.len()).sum::<usize>();
    let mut out = Vec::with_capacity(estimate_leaf_size(entries));
    out.push(NODE_KIND_LEAF);
    out.push(NODE_VERSION);
    push_varint(&mut out, entries.len());
    push_varint(&mut out, key_bytes_len);
    push_varint(&mut out, value_bytes_len);

    let key_offsets_start = out.len();
    out.resize(
        key_offsets_start + (entries.len() * std::mem::size_of::<u16>()),
        0,
    );
    let value_offsets_start = out.len();
    out.resize(
        value_offsets_start + (entries.len() * std::mem::size_of::<u16>()),
        0,
    );

    let key_bytes_start = out.len();
    for (index, entry) in entries.iter().enumerate() {
        let offset = node_offset(out.len() - key_bytes_start, "live tracked leaf key bytes");
        write_u16_at(
            &mut out,
            key_offsets_start + (index * std::mem::size_of::<u16>()),
            offset,
        );
        out.extend_from_slice(&entry.key);
    }

    let value_bytes_start = out.len();
    for (index, entry) in entries.iter().enumerate() {
        let offset = node_offset(
            out.len() - value_bytes_start,
            "live tracked leaf value bytes",
        );
        write_u16_at(
            &mut out,
            value_offsets_start + (index * std::mem::size_of::<u16>()),
            offset,
        );
        out.extend_from_slice(&entry.value);
    }
    out
}

pub(crate) fn encode_internal_node(children: &[ChildSummary]) -> Vec<u8> {
    let first_key_bytes_len = children
        .iter()
        .map(|child| child.first_key.len())
        .sum::<usize>();
    let last_key_bytes_len = children
        .iter()
        .map(|child| child.last_key.len())
        .sum::<usize>();
    let mut out = Vec::with_capacity(estimate_internal_size(children));
    out.push(NODE_KIND_INTERNAL);
    out.push(NODE_VERSION);
    push_varint(&mut out, children.len());
    push_varint(&mut out, first_key_bytes_len);
    push_varint(&mut out, last_key_bytes_len);

    let first_offsets_start = out.len();
    out.resize(
        first_offsets_start + (children.len() * std::mem::size_of::<u16>()),
        0,
    );
    let last_offsets_start = out.len();
    out.resize(
        last_offsets_start + (children.len() * std::mem::size_of::<u16>()),
        0,
    );

    for child in children {
        out.extend_from_slice(&child.child_hash);
    }
    for child in children {
        out.extend_from_slice(&child.subtree_count.to_le_bytes());
    }

    let first_key_bytes_start = out.len();
    for (index, child) in children.iter().enumerate() {
        let offset = node_offset(
            out.len() - first_key_bytes_start,
            "live tracked internal first-key bytes",
        );
        write_u16_at(
            &mut out,
            first_offsets_start + (index * std::mem::size_of::<u16>()),
            offset,
        );
        out.extend_from_slice(&child.first_key);
    }

    let last_key_bytes_start = out.len();
    for (index, child) in children.iter().enumerate() {
        let offset = node_offset(
            out.len() - last_key_bytes_start,
            "live tracked internal last-key bytes",
        );
        write_u16_at(
            &mut out,
            last_offsets_start + (index * std::mem::size_of::<u16>()),
            offset,
        );
        out.extend_from_slice(&child.last_key);
    }
    out
}

pub(crate) fn estimate_leaf_size(entries: &[EncodedLeafEntry]) -> usize {
    let key_bytes = entries.iter().map(|entry| entry.key.len()).sum::<usize>();
    let value_bytes = entries.iter().map(|entry| entry.value.len()).sum::<usize>();
    32 + (entries.len() * 4) + key_bytes + value_bytes
}

pub(crate) fn estimate_internal_size(children: &[ChildSummary]) -> usize {
    let first_key_bytes = children
        .iter()
        .map(|child| child.first_key.len())
        .sum::<usize>();
    let last_key_bytes = children
        .iter()
        .map(|child| child.last_key.len())
        .sum::<usize>();
    32 + (children.len() * 4)
        + (children.len() * LIVE_TRACKED_HASH_BYTES)
        + (children.len() * std::mem::size_of::<u64>())
        + first_key_bytes
        + last_key_bytes
}

pub(crate) fn leaf_codec_profile(
    rows: &[LiveTrackedRow],
    options: &LiveTrackedStateOptions,
) -> Result<LiveTrackedCodecProfile, LixError> {
    let mut value_writes = BTreeMap::new();
    let mut entries = Vec::with_capacity(rows.len());
    let mut key_bytes = 0;
    let mut value_bytes = 0;
    for row in rows {
        let encoded_key = encode_entity_key(&row.key);
        let encoded_value = encode_entity_value_canonical(&row.value, options, &mut value_writes)?;
        key_bytes += encoded_key.len();
        value_bytes += encoded_value.len();
        entries.push(EncodedLeafEntry {
            key: encoded_key,
            value: encoded_value,
        });
    }
    let encoded_leaf = encode_leaf_node(&entries);
    let _ = DecodedNode::decode(encoded_leaf.clone())?;
    Ok(LiveTrackedCodecProfile {
        row_count: rows.len(),
        encoded_leaf_bytes: encoded_leaf.len(),
        key_bytes,
        value_bytes,
        large_value_count: value_writes.len(),
        large_value_bytes: value_writes.values().map(|value| value.size_bytes).sum(),
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

fn push_entity_value_header(out: &mut Vec<u8>, value: &LiveTrackedEntityValue) {
    push_sized_bytes(out, value.change_id.as_bytes());
    out.push(u8::from(value.tombstone));
    push_sized_bytes(out, value.schema_version.as_str().as_bytes());
    push_sized_bytes(out, value.plugin_key.as_str().as_bytes());
    match &value.metadata {
        Some(metadata) => {
            out.push(1);
            push_sized_bytes(out, metadata.as_bytes());
        }
        None => out.push(0),
    }
}

fn encode_field_value_canonical(
    out: &mut Vec<u8>,
    value: &LiveTrackedFieldValue,
    options: &LiveTrackedStateOptions,
    value_writes: &mut BTreeMap<[u8; LIVE_TRACKED_HASH_BYTES], PendingValueWrite>,
) {
    match value {
        LiveTrackedFieldValue::Text(text) if text.len() > options.large_value_threshold_bytes => {
            out.push(VALUE_KIND_LARGE_TEXT);
            let reference = persist_large_value(text.as_bytes().to_vec(), value_writes);
            encode_large_ref(out, &reference);
        }
        LiveTrackedFieldValue::Json(json) if json.len() > options.large_value_threshold_bytes => {
            out.push(VALUE_KIND_LARGE_JSON);
            let reference = persist_large_value(json.as_bytes().to_vec(), value_writes);
            encode_large_ref(out, &reference);
        }
        LiveTrackedFieldValue::Blob(blob) if blob.len() > options.large_value_threshold_bytes => {
            out.push(VALUE_KIND_LARGE_BLOB);
            let reference = persist_large_value(blob.clone(), value_writes);
            encode_large_ref(out, &reference);
        }
        other => encode_field_value(out, other),
    }
}

fn persist_large_value(
    data: Vec<u8>,
    value_writes: &mut BTreeMap<[u8; LIVE_TRACKED_HASH_BYTES], PendingValueWrite>,
) -> LiveTrackedValueRef {
    let hash = hash_bytes(&data);
    let size_bytes = data.len();
    value_writes
        .entry(hash)
        .or_insert_with(|| PendingValueWrite {
            hash,
            data,
            size_bytes,
        });
    LiveTrackedValueRef::new(hash, size_bytes)
}

fn encode_field_value(out: &mut Vec<u8>, value: &LiveTrackedFieldValue) {
    match value {
        LiveTrackedFieldValue::Null => out.push(VALUE_KIND_NULL),
        LiveTrackedFieldValue::Boolean(false) => out.push(VALUE_KIND_BOOLEAN_FALSE),
        LiveTrackedFieldValue::Boolean(true) => out.push(VALUE_KIND_BOOLEAN_TRUE),
        LiveTrackedFieldValue::Integer(value) => {
            out.push(VALUE_KIND_INTEGER);
            out.extend_from_slice(&value.to_le_bytes());
        }
        LiveTrackedFieldValue::Real(value) => {
            out.push(VALUE_KIND_REAL);
            out.extend_from_slice(&value.to_le_bytes());
        }
        LiveTrackedFieldValue::Text(value) => {
            out.push(VALUE_KIND_TEXT);
            push_sized_bytes(out, value.as_bytes());
        }
        LiveTrackedFieldValue::Json(value) => {
            out.push(VALUE_KIND_JSON);
            push_sized_bytes(out, value.as_bytes());
        }
        LiveTrackedFieldValue::Blob(value) => {
            out.push(VALUE_KIND_BLOB);
            push_sized_bytes(out, value);
        }
        LiveTrackedFieldValue::LargeText(reference) => {
            out.push(VALUE_KIND_LARGE_TEXT);
            encode_large_ref(out, reference);
        }
        LiveTrackedFieldValue::LargeJson(reference) => {
            out.push(VALUE_KIND_LARGE_JSON);
            encode_large_ref(out, reference);
        }
        LiveTrackedFieldValue::LargeBlob(reference) => {
            out.push(VALUE_KIND_LARGE_BLOB);
            encode_large_ref(out, reference);
        }
    }
}

fn decode_field_value(bytes: &[u8], cursor: &mut usize) -> Result<LiveTrackedFieldValue, LixError> {
    let tag = *bytes
        .get(*cursor)
        .ok_or_else(|| LixError::unknown("live tracked field value is truncated"))?;
    *cursor += 1;
    match tag {
        VALUE_KIND_NULL => Ok(LiveTrackedFieldValue::Null),
        VALUE_KIND_BOOLEAN_FALSE => Ok(LiveTrackedFieldValue::Boolean(false)),
        VALUE_KIND_BOOLEAN_TRUE => Ok(LiveTrackedFieldValue::Boolean(true)),
        VALUE_KIND_INTEGER => {
            let number = read_exact_array::<8>(bytes, cursor)?;
            Ok(LiveTrackedFieldValue::Integer(i64::from_le_bytes(number)))
        }
        VALUE_KIND_REAL => {
            let number = read_exact_array::<8>(bytes, cursor)?;
            Ok(LiveTrackedFieldValue::Real(f64::from_le_bytes(number)))
        }
        VALUE_KIND_TEXT => Ok(LiveTrackedFieldValue::Text(read_sized_utf8(
            bytes, cursor, "text",
        )?)),
        VALUE_KIND_JSON => Ok(LiveTrackedFieldValue::Json(read_sized_utf8(
            bytes, cursor, "json",
        )?)),
        VALUE_KIND_BLOB => Ok(LiveTrackedFieldValue::Blob(read_sized_bytes(
            bytes, cursor,
        )?)),
        VALUE_KIND_LARGE_TEXT => Ok(LiveTrackedFieldValue::LargeText(decode_large_ref(
            bytes, cursor,
        )?)),
        VALUE_KIND_LARGE_JSON => Ok(LiveTrackedFieldValue::LargeJson(decode_large_ref(
            bytes, cursor,
        )?)),
        VALUE_KIND_LARGE_BLOB => Ok(LiveTrackedFieldValue::LargeBlob(decode_large_ref(
            bytes, cursor,
        )?)),
        other => Err(LixError::unknown(format!(
            "unknown live tracked field value tag {other}"
        ))),
    }
}

fn encode_large_ref(out: &mut Vec<u8>, reference: &LiveTrackedValueRef) {
    out.extend_from_slice(reference.hash());
    push_varint(out, reference.size_bytes());
}

fn decode_large_ref(bytes: &[u8], cursor: &mut usize) -> Result<LiveTrackedValueRef, LixError> {
    let hash = read_exact_array::<LIVE_TRACKED_HASH_BYTES>(bytes, cursor)?;
    let size_bytes = read_varint(bytes, cursor)?;
    Ok(LiveTrackedValueRef::new(hash, size_bytes))
}

fn decode_node(raw: Arc<Vec<u8>>) -> Result<DecodedNode, LixError> {
    let bytes = raw.as_slice();
    if bytes.len() < 2 {
        return Err(LixError::unknown("live tracked node is truncated"));
    }
    let kind = bytes[0];
    let version = bytes[1];
    if version != NODE_VERSION {
        return Err(LixError::unknown(format!(
            "unsupported live tracked node version {version}"
        )));
    }
    let mut cursor = 2;
    let count = read_varint(bytes, &mut cursor)?;
    match kind {
        NODE_KIND_LEAF => {
            let key_bytes_len = read_varint(bytes, &mut cursor)?;
            let value_bytes_len = read_varint(bytes, &mut cursor)?;
            let key_offsets = read_u16_offsets(bytes, &mut cursor, count)?;
            let value_offsets = read_u16_offsets(bytes, &mut cursor, count)?;
            let key_bytes_start = cursor;
            let key_bytes_end = key_bytes_start + key_bytes_len;
            if key_bytes_end > bytes.len() {
                return Err(LixError::unknown(
                    "live tracked leaf key region is truncated",
                ));
            }
            let value_bytes_start = key_bytes_end;
            let value_bytes_end = value_bytes_start + value_bytes_len;
            if value_bytes_end > bytes.len() {
                return Err(LixError::unknown(
                    "live tracked leaf value region is truncated",
                ));
            }
            if value_bytes_end != bytes.len() {
                return Err(LixError::unknown(
                    "live tracked leaf decode found trailing bytes",
                ));
            }
            Ok(DecodedNode::Leaf(DecodedLeafNode {
                raw,
                key_offsets,
                value_offsets,
                key_bytes_start,
                key_bytes_len,
                value_bytes_start,
                value_bytes_len,
            }))
        }
        NODE_KIND_INTERNAL => {
            let first_key_bytes_len = read_varint(bytes, &mut cursor)?;
            let last_key_bytes_len = read_varint(bytes, &mut cursor)?;
            let first_key_offsets = read_u16_offsets(bytes, &mut cursor, count)?;
            let last_key_offsets = read_u16_offsets(bytes, &mut cursor, count)?;
            let mut child_hashes = Vec::with_capacity(count);
            for _ in 0..count {
                child_hashes.push(read_exact_array::<LIVE_TRACKED_HASH_BYTES>(
                    bytes,
                    &mut cursor,
                )?);
            }
            let mut subtree_counts = Vec::with_capacity(count);
            for _ in 0..count {
                subtree_counts.push(u64::from_le_bytes(read_exact_array::<8>(
                    bytes,
                    &mut cursor,
                )?));
            }
            let first_key_bytes_start = cursor;
            let first_key_bytes_end = first_key_bytes_start + first_key_bytes_len;
            if first_key_bytes_end > bytes.len() {
                return Err(LixError::unknown(
                    "live tracked internal first-key region is truncated",
                ));
            }
            let last_key_bytes_start = first_key_bytes_end;
            let last_key_bytes_end = last_key_bytes_start + last_key_bytes_len;
            if last_key_bytes_end > bytes.len() {
                return Err(LixError::unknown(
                    "live tracked internal last-key region is truncated",
                ));
            }
            if last_key_bytes_end != bytes.len() {
                return Err(LixError::unknown(
                    "live tracked internal decode found trailing bytes",
                ));
            }
            Ok(DecodedNode::Internal(DecodedInternalNode {
                raw,
                first_key_offsets,
                last_key_offsets,
                child_hashes,
                subtree_counts,
                first_key_bytes_start,
                first_key_bytes_len,
                last_key_bytes_start,
                last_key_bytes_len,
            }))
        }
        other => Err(LixError::unknown(format!(
            "unknown live tracked node kind {other}"
        ))),
    }
}

fn region_slice<'a>(
    raw: &'a [u8],
    region_start: usize,
    region_len: usize,
    offsets: &[u16],
    index: usize,
) -> &'a [u8] {
    let start = offsets[index] as usize;
    let end = offsets.get(index + 1).copied().unwrap_or(region_len as u16) as usize;
    &raw[region_start + start..region_start + end]
}

fn read_u16_offsets(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<Vec<u16>, LixError> {
    let mut offsets = Vec::with_capacity(count);
    for _ in 0..count {
        offsets.push(u16::from_le_bytes(read_exact_array::<2>(bytes, cursor)?));
    }
    Ok(offsets)
}

fn write_u16_at(bytes: &mut [u8], position: usize, value: u16) {
    bytes[position..position + std::mem::size_of::<u16>()].copy_from_slice(&value.to_le_bytes());
}

fn node_offset(value: usize, label: &str) -> u16 {
    u16::try_from(value).unwrap_or_else(|_| panic!("{label} exceeded u16::MAX"))
}

fn push_sized_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    push_varint(out, bytes.len());
    out.extend_from_slice(bytes);
}

fn read_sized_utf8(bytes: &[u8], cursor: &mut usize, label: &str) -> Result<String, LixError> {
    String::from_utf8(read_sized_bytes(bytes, cursor)?).map_err(|error| {
        LixError::unknown(format!("live tracked {label} is not valid UTF-8: {error}"))
    })
}

fn read_sized_bytes(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>, LixError> {
    let len = read_varint(bytes, cursor)?;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| LixError::unknown("live tracked sized byte decode overflowed"))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| LixError::unknown("live tracked sized bytes are truncated"))?;
    *cursor = end;
    Ok(slice.to_vec())
}

fn read_sized_bytes_lossy<'a>(bytes: &'a [u8], cursor: &mut usize) -> &'a [u8] {
    let Some((len, len_bytes)) = read_varint_lossy(bytes.get(*cursor..).unwrap_or_default()) else {
        return &[];
    };
    *cursor += len_bytes;
    let start = *cursor;
    let end = start.saturating_add(len);
    *cursor = end.min(bytes.len());
    bytes.get(start..end).unwrap_or_default()
}

fn push_varint(out: &mut Vec<u8>, mut value: usize) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<usize, LixError> {
    let (value, read_bytes) = read_varint_lossy(bytes.get(*cursor..).unwrap_or_default())
        .ok_or_else(|| LixError::unknown("live tracked varint is truncated or invalid"))?;
    *cursor += read_bytes;
    Ok(value)
}

fn read_varint_lossy(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut out = 0_usize;
    let mut shift = 0_u32;
    for (index, byte) in bytes.iter().copied().enumerate() {
        out |= usize::from(byte & 0x7f) << shift;
        if (byte & 0x80) == 0 {
            return Some((out, index + 1));
        }
        shift += 7;
        if shift > (usize::BITS - 1) {
            return None;
        }
    }
    None
}

fn read_exact_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Result<[u8; N], LixError> {
    let end = cursor
        .checked_add(N)
        .ok_or_else(|| LixError::unknown("live tracked fixed-width decode overflowed"))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| LixError::unknown("live tracked fixed-width decode is truncated"))?;
    *cursor = end;
    let mut out = [0_u8; N];
    out.copy_from_slice(slice);
    Ok(out)
}
