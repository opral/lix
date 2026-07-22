use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::LixError;
use crate::changelog::{ChangeId, CommitId};
use crate::common::LixTimestamp;
use crate::tracked_state::types::{
    TRACKED_STATE_HASH_BYTES, TrackedStateIndexValue, TrackedStateIndexValueRef, TrackedStateKey,
    TrackedStateKeyRef,
};

const WEIBULL_K: i32 = 4;

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
    Leaf(DecodedLeafNodeRef),
    Internal(DecodedInternalNode),
}

#[derive(Debug, Clone)]
pub(crate) enum DecodedNodeRef {
    Leaf(DecodedLeafNodeRef),
    Internal(DecodedInternalNode),
}

/// Decoded view of a leaf node.
///
/// Keys are stored front-coded on disk and reconstructed into one arena at
/// decode time. Compact value parts are reconstructed byte-for-byte into the
/// same arena, keeping entry access to two borrowed slices without per-entry
/// allocations.
#[derive(Debug, Clone)]
pub(crate) struct DecodedLeafNodeRef {
    arena: Vec<u8>,
    entries: Vec<LeafEntrySpan>,
}

#[derive(Debug, Clone, Copy)]
struct LeafEntrySpan {
    key_start: usize,
    key_end: usize,
    value_start: usize,
    value_end: usize,
}

impl DecodedLeafNodeRef {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn first_key(&self) -> Option<&[u8]> {
        self.entries
            .first()
            .map(|span| &self.arena[span.key_start..span.key_end])
    }

    pub(crate) fn last_key(&self) -> Option<&[u8]> {
        self.entries
            .last()
            .map(|span| &self.arena[span.key_start..span.key_end])
    }

    #[expect(clippy::unnecessary_wraps)]
    pub(crate) fn entry(&self, index: usize) -> Result<Option<EncodedLeafEntryRef<'_>>, LixError> {
        Ok(self.entries.get(index).map(|span| EncodedLeafEntryRef {
            key: &self.arena[span.key_start..span.key_end],
            value: &self.arena[span.value_start..span.value_end],
        }))
    }

    #[expect(clippy::unnecessary_wraps)]
    pub(crate) fn key(&self, index: usize) -> Result<Option<&[u8]>, LixError> {
        Ok(self
            .entries
            .get(index)
            .map(|span| &self.arena[span.key_start..span.key_end]))
    }

    /// Materializes per-entry buffers only for mutation paths that need to
    /// retain leaf entries after this decoded node is consumed. Read and diff
    /// paths keep using the arena-backed view above.
    pub(crate) fn into_entries(self) -> Vec<EncodedLeafEntry> {
        self.entries
            .into_iter()
            .map(|span| EncodedLeafEntry {
                key: self.arena[span.key_start..span.key_end].to_vec(),
                value: self.arena[span.value_start..span.value_end].to_vec(),
            })
            .collect()
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

    pub(crate) fn into_children(self) -> Vec<ChildSummary> {
        self.children
    }
}

const NODE_KIND_LEAF_V3: u8 = 3;
const NODE_KIND_INTERNAL_V3: u8 = 4;

pub(crate) fn hash_bytes(bytes: &[u8]) -> [u8; TRACKED_STATE_HASH_BYTES] {
    *blake3::hash(bytes).as_bytes()
}

pub(crate) fn encode_key(key: &TrackedStateKey) -> Vec<u8> {
    encode_key_parts(
        &key.schema_key,
        key.file_id.as_deref(),
        &key.entity_pk.parts,
    )
}

pub(crate) fn encode_key_ref(key: TrackedStateKeyRef<'_>) -> Vec<u8> {
    encode_key_parts(key.schema_key, key.file_id, &key.entity_pk.parts)
}

pub(crate) fn encode_schema_key_prefix(schema_key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(schema_key.len() + 2);
    write_key_string(&mut out, schema_key, KEY_PART_FINAL);
    out
}

pub(crate) fn encode_schema_file_prefix(schema_key: &str, file_id: Option<&str>) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(schema_key.len() + file_id.map_or(1, |file_id| file_id.len() + 3) + 2);
    write_key_string(&mut out, schema_key, KEY_PART_FINAL);
    write_file_id(&mut out, file_id);
    out
}

pub(crate) fn decode_key(bytes: &[u8]) -> Result<TrackedStateKey, LixError> {
    let mut offset = 0usize;
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error("schema key has an invalid terminator"));
    }
    let file_id = read_file_id(bytes, &mut offset)?;
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("has trailing bytes"));
    }
    Ok(TrackedStateKey {
        schema_key,
        file_id,
        entity_pk,
    })
}

/// Decodes a key after the caller has already proven the schema/file prefix.
///
/// This is for scan paths that have matched an encoded prefix range and only
/// need to materialize the entity suffix plus the selected columns.
pub(crate) fn decode_key_with_trusted_prefix(
    bytes: &[u8],
    schema_key: &str,
    file_id: Option<&str>,
    prefix_len: usize,
) -> Result<TrackedStateKey, LixError> {
    if prefix_len > bytes.len() {
        return Err(key_codec_error(
            "trusted prefix is longer than the encoded key",
        ));
    }
    let mut offset = prefix_len;
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("has trailing bytes"));
    }
    Ok(TrackedStateKey {
        schema_key: schema_key.to_string(),
        file_id: file_id.map(str::to_string),
        entity_pk,
    })
}

const KEY_ESCAPE: u8 = 0xff;
const KEY_PART_FINAL: u8 = 0x00;
const KEY_PART_MORE: u8 = 0x01;
const FILE_ID_NONE: u8 = 0x00;
const FILE_ID_SOME: u8 = 0x01;

/// Order-preserving tracked-state key encoding.
///
/// NUL bytes are escaped as `00 ff`. Strings end in `00 00`, except a
/// non-final entity-pk part which ends in `00 01`. File ids have a one-byte
/// None/Some tag. The distinct entity terminators make complete keys
/// prefix-free while preserving the derived tuple ordering byte-for-byte.
fn encode_key_parts(schema_key: &str, file_id: Option<&str>, entity_pk: &[String]) -> Vec<u8> {
    let strings_len = schema_key.len()
        + file_id.map_or(0, str::len)
        + entity_pk.iter().map(String::len).sum::<usize>();
    let mut out = Vec::with_capacity(strings_len + 5 + entity_pk.len() * 2);
    write_key_string(&mut out, schema_key, KEY_PART_FINAL);
    write_file_id(&mut out, file_id);
    for (index, part) in entity_pk.iter().enumerate() {
        let terminator = if index + 1 == entity_pk.len() {
            KEY_PART_FINAL
        } else {
            KEY_PART_MORE
        };
        write_key_string(&mut out, part, terminator);
    }
    out
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
        if byte == 0 {
            out.extend_from_slice(&[0, KEY_ESCAPE]);
        } else {
            out.push(byte);
        }
    }
    out.extend_from_slice(&[0, terminator]);
}

fn read_file_id(bytes: &[u8], offset: &mut usize) -> Result<Option<String>, LixError> {
    let tag = *bytes
        .get(*offset)
        .ok_or_else(|| key_codec_error("file id tag is truncated"))?;
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
        other => Err(key_codec_error(format!("file id has unknown tag {other}"))),
    }
}

fn read_entity_pk(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<crate::entity_pk::EntityPk, LixError> {
    if *offset >= bytes.len() {
        return Err(key_codec_error("entity primary key is empty or truncated"));
    }
    let (first, terminator) = read_key_string(bytes, offset, "entity primary-key part")?;
    if terminator == KEY_PART_FINAL {
        return Ok(crate::entity_pk::EntityPk::single(first));
    }

    let mut parts = Vec::with_capacity(2);
    parts.push(first);
    loop {
        if *offset >= bytes.len() {
            return Err(key_codec_error("entity primary key is empty or truncated"));
        }
        let (part, terminator) = read_key_string(bytes, offset, "entity primary-key part")?;
        parts.push(part);
        match terminator {
            KEY_PART_FINAL => break,
            KEY_PART_MORE => {}
            _ => unreachable!("read_key_string validates terminators"),
        }
    }
    crate::entity_pk::EntityPk::from_parts(parts).map_err(|error| {
        key_codec_error(format!(
            "entity primary key decoded from storage is invalid: {error}"
        ))
    })
}

fn read_key_string(
    bytes: &[u8],
    offset: &mut usize,
    field: &str,
) -> Result<(String, u8), LixError> {
    let start = *offset;
    let mut segment_start = start;
    let mut decoded: Option<Vec<u8>> = None;
    loop {
        let tail = bytes
            .get(segment_start..)
            .ok_or_else(|| key_codec_error(format!("{field} is truncated")))?;
        let relative_zero = memchr::memchr(0, tail)
            .ok_or_else(|| key_codec_error(format!("{field} is truncated")))?;
        let zero = segment_start + relative_zero;
        let escape = *bytes
            .get(zero + 1)
            .ok_or_else(|| key_codec_error(format!("{field} escape is truncated")))?;
        *offset = zero + 2;
        match escape {
            KEY_ESCAPE => {
                let out = decoded.get_or_insert_with(|| {
                    Vec::with_capacity(zero.saturating_sub(start).saturating_add(16))
                });
                out.extend_from_slice(&bytes[segment_start..zero]);
                out.push(0);
                segment_start = *offset;
            }
            KEY_PART_FINAL | KEY_PART_MORE => {
                let value = decoded.map_or_else(
                    || bytes[start..zero].to_vec(),
                    |mut out| {
                        out.extend_from_slice(&bytes[segment_start..zero]);
                        out
                    },
                );
                let value = String::from_utf8(value)
                    .map_err(|_| key_codec_error(format!("{field} is not UTF-8")))?;
                return Ok((value, escape));
            }
            other => {
                return Err(key_codec_error(format!(
                    "{field} has unknown escape {other}"
                )));
            }
        }
    }
}

fn key_codec_error(message: impl Into<String>) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("tracked-state key {}", message.into()),
    )
}

#[cfg(test)]
pub(crate) fn encode_value(value: &TrackedStateIndexValue) -> Vec<u8> {
    encode_value_ref(TrackedStateIndexValueRef {
        change_id: value.change_id,
        commit_id: value.commit_id,
        deleted: value.deleted,
        created_at: value.created_at,
        updated_at: value.updated_at,
    })
}

pub(crate) fn encode_value_ref(value: TrackedStateIndexValueRef) -> Vec<u8> {
    let mut out = Vec::with_capacity(VALUE_MAX_BYTES);
    out.extend_from_slice(value.change_id.as_uuid().as_bytes());
    out.extend_from_slice(value.commit_id.as_uuid().as_bytes());
    write_value_tail(
        &mut out,
        value.deleted,
        value.created_at.packed(),
        value.updated_at.packed(),
    );
    debug_assert!((VALUE_MIN_BYTES..=VALUE_MAX_BYTES).contains(&out.len()));
    out
}

#[cfg(test)]
pub(crate) fn encoded_value_len(value: &TrackedStateIndexValue) -> usize {
    encode_value(value).len()
}

pub(crate) fn decode_value(bytes: &[u8]) -> Result<TrackedStateIndexValue, LixError> {
    decode_value_view(bytes).map(tracked_value_from_storage)
}

pub(crate) fn decode_visible_value(
    bytes: &[u8],
    include_tombstones: bool,
) -> Result<Option<TrackedStateIndexValue>, LixError> {
    let view = decode_value_view(bytes)?;
    if view.deleted && !include_tombstones {
        return Ok(None);
    }
    Ok(Some(tracked_value_from_storage(view)))
}

fn decode_value_view(bytes: &[u8]) -> Result<TrackedStateIndexValueRef, LixError> {
    if !(VALUE_MIN_BYTES..=VALUE_MAX_BYTES).contains(&bytes.len()) {
        return Err(value_codec_error(format!(
            "has {} bytes; expected {VALUE_MIN_BYTES}..={VALUE_MAX_BYTES}",
            bytes.len(),
        )));
    }
    let change_id = ChangeId::new(uuid::Uuid::from_bytes(
        bytes[..VALUE_CHANGE_ID_END]
            .try_into()
            .expect("fixed change-id slice"),
    ));
    let commit_id = CommitId::new(uuid::Uuid::from_bytes(
        bytes[VALUE_COMMIT_ID_START..VALUE_COMMIT_ID_END]
            .try_into()
            .expect("fixed commit-id slice"),
    ));
    let mut offset = VALUE_STATE_TAIL_START;
    let (deleted, created_at_packed, updated_at_packed) =
        read_value_tail_fields(bytes, &mut offset, "tracked-state value")?;
    if offset != bytes.len() {
        return Err(value_codec_error("has trailing bytes"));
    }
    let created_at = decode_value_timestamp(created_at_packed, "created_at")?;
    let updated_at = decode_value_timestamp(updated_at_packed, "updated_at")?;
    Ok(TrackedStateIndexValueRef {
        change_id,
        commit_id,
        deleted,
        created_at,
        updated_at,
    })
}

fn decode_value_timestamp(packed: u64, field: &str) -> Result<LixTimestamp, LixError> {
    LixTimestamp::from_packed(packed)
        .map_err(|error| value_codec_error(format!("has invalid {field}: {error}")))
}

fn read_value_tail_fields(
    bytes: &[u8],
    offset: &mut usize,
    context: &str,
) -> Result<(bool, u64, u64), LixError> {
    let tag = *bytes.get(*offset).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{context} state tail is truncated"),
        )
    })?;
    *offset += 1;
    let deleted = tag & VALUE_TAIL_DELETED != 0;
    let code = tag & VALUE_TAIL_CODE_MASK;
    let (created_width, updated_width, equal) = match code {
        0..=VALUE_TIMESTAMP_MAX_WIDTH => (usize::from(code), 0, true),
        VALUE_TAIL_DISTINCT_MIN..=VALUE_TAIL_DISTINCT_MAX => {
            let widths = code - VALUE_TAIL_DISTINCT_MIN;
            (
                usize::from(widths / VALUE_TIMESTAMP_WIDTH_COUNT),
                usize::from(widths % VALUE_TIMESTAMP_WIDTH_COUNT),
                false,
            )
        }
        _ => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("{context} has reserved state-tail tag {code}"),
            ));
        }
    };
    let created_at = read_minimal_le_timestamp(bytes, offset, created_width, context)?;
    let updated_at = if equal {
        created_at
    } else {
        read_minimal_le_timestamp(bytes, offset, updated_width, context)?
    };
    if !equal && created_at == updated_at {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{context} uses the distinct timestamp form for equal values"),
        ));
    }
    Ok((deleted, created_at, updated_at))
}

fn write_value_tail(out: &mut Vec<u8>, deleted: bool, created_at: u64, updated_at: u64) {
    let created_width = minimal_timestamp_width(created_at);
    let updated_width = minimal_timestamp_width(updated_at);
    let created_width_code =
        u8::try_from(created_width).expect("timestamp byte width always fits in u8");
    let updated_width_code =
        u8::try_from(updated_width).expect("timestamp byte width always fits in u8");
    let code = if created_at == updated_at {
        created_width_code
    } else {
        VALUE_TAIL_DISTINCT_MIN
            + created_width_code * VALUE_TIMESTAMP_WIDTH_COUNT
            + updated_width_code
    };
    out.push(code | (u8::from(deleted) * VALUE_TAIL_DELETED));
    out.extend_from_slice(&created_at.to_le_bytes()[..created_width]);
    if created_at != updated_at {
        out.extend_from_slice(&updated_at.to_le_bytes()[..updated_width]);
    }
}

fn minimal_timestamp_width(value: u64) -> usize {
    if value == 0 {
        0
    } else {
        usize::try_from((u64::BITS - value.leading_zeros()).div_ceil(8))
            .expect("timestamp byte width always fits in usize")
    }
}

fn read_minimal_le_timestamp(
    bytes: &[u8],
    offset: &mut usize,
    width: usize,
    context: &str,
) -> Result<u64, LixError> {
    let end = offset.checked_add(width).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{context} timestamp width overflows usize"),
        )
    })?;
    let encoded = bytes.get(*offset..end).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{context} timestamp is truncated"),
        )
    })?;
    if width > 0 && encoded[width - 1] == 0 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{context} timestamp is not minimally encoded"),
        ));
    }
    let mut storage = [0u8; 8];
    storage[..width].copy_from_slice(encoded);
    *offset = end;
    Ok(u64::from_le_bytes(storage))
}

fn read_value_tail<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    context: &str,
) -> Result<&'a [u8], LixError> {
    let start = *offset;
    read_value_tail_fields(bytes, offset, context)?;
    Ok(&bytes[start..*offset])
}

fn value_codec_error(message: impl Into<String>) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("tracked-state value {}", message.into()),
    )
}

fn tracked_value_from_storage(value: TrackedStateIndexValueRef) -> TrackedStateIndexValue {
    let TrackedStateIndexValueRef {
        change_id,
        commit_id,
        deleted,
        created_at,
        updated_at,
    } = value;
    TrackedStateIndexValue {
        change_id,
        commit_id,
        deleted,
        created_at,
        updated_at,
    }
}

pub(crate) fn encode_leaf_node(entries: &[EncodedLeafEntry]) -> Vec<u8> {
    let entries = entries
        .iter()
        .map(EncodedLeafEntry::as_ref)
        .collect::<Vec<_>>();
    encode_leaf_node_refs(&entries)
}

/// Fixed id offsets in the typed tracked-state value. The state tail starts
/// with a tag that stores deletion, timestamp widths, and whether both
/// timestamps are equal, followed by their minimal little-endian bytes.
const VALUE_CHANGE_ID_END: usize = 16;
const VALUE_COMMIT_ID_START: usize = 16;
const VALUE_COMMIT_ID_END: usize = 32;
const VALUE_STATE_TAIL_START: usize = 32;
const VALUE_MIN_BYTES: usize = VALUE_STATE_TAIL_START + 1;
const VALUE_MAX_BYTES: usize = VALUE_STATE_TAIL_START + 1 + 8 + 8;
const VALUE_TAIL_DELETED: u8 = 0x80;
const VALUE_TAIL_CODE_MASK: u8 = 0x7f;
const VALUE_TIMESTAMP_MAX_WIDTH: u8 = 8;
const VALUE_TIMESTAMP_WIDTH_COUNT: u8 = VALUE_TIMESTAMP_MAX_WIDTH + 1;
const VALUE_TAIL_DISTINCT_MIN: u8 = VALUE_TIMESTAMP_WIDTH_COUNT;
const VALUE_TAIL_DISTINCT_MAX: u8 =
    VALUE_TAIL_DISTINCT_MIN + VALUE_TIMESTAMP_WIDTH_COUNT * VALUE_TIMESTAMP_WIDTH_COUNT - 1;

/// Leaf node wire format (v3):
///
/// ```text
/// [NODE_KIND_LEAF_V3]
/// varint entry_count
/// varint commit_dict_len ++ commit_dict_len x 16 commit-id bytes
/// varint tail_dict_len ++ tail_dict_len x self-delimiting state tails
/// per entry:
///   varint shared_key_len   bytes shared with the previous entry's key
///   varint key_suffix_len ++ key suffix bytes
///   16 change-id bytes
///   varint commit_ref       0 + 16 literal bytes, or dictionary slot n-1
///   varint tail_ref         0 + literal state tail, or dictionary slot n-1
/// ```
///
/// Only values repeated within the leaf enter a dictionary, so a dictionary
/// can never expand unique commit ids or timestamp/deletion tails. Both
/// dictionaries use first-occurrence order, keeping the encoding a
/// deterministic function of the entries. State tails need no length prefix:
/// their tag declares the timestamp equality and byte widths.
///
/// Entries within a node are sorted by key, so consecutive keys share the
/// encoded schema-key/file-id prefix and most of the entity-pk; front-coding
/// removes that redundancy. Decode reconstructs the original value bytes
/// exactly. Sortedness is a caller invariant (the tree builder always
/// produces sorted entries); it is asserted in debug builds but not
/// revalidated on every release-mode read.
pub(crate) fn encode_leaf_node_refs(entries: &[EncodedLeafEntryRef<'_>]) -> Vec<u8> {
    debug_assert!(
        entries.windows(2).all(|pair| pair[0].key < pair[1].key),
        "leaf entries must be strictly sorted by key"
    );
    for entry in entries {
        assert!(
            (VALUE_MIN_BYTES..=VALUE_MAX_BYTES).contains(&entry.value.len()),
            "tracked-state leaf values must use the v3 value layout"
        );
        #[cfg(debug_assertions)]
        {
            let mut tail_end = VALUE_STATE_TAIL_START;
            read_value_tail(entry.value, &mut tail_end, "tracked-state leaf value")
                .expect("tracked-state leaf value must contain a valid v3 state tail");
            assert_eq!(
                tail_end,
                entry.value.len(),
                "tracked-state leaf value must end after its v3 state tail"
            );
        }
    }
    let commit_dictionary = repeated_dictionary::<16>(entries, VALUE_COMMIT_ID_START);
    let tail_dictionary = repeated_tail_dictionary(entries);

    let mut out = Vec::with_capacity(64 + entries.len() * 24);
    out.push(NODE_KIND_LEAF_V3);
    write_varint(&mut out, entries.len() as u64);
    write_varint(&mut out, commit_dictionary.len() as u64);
    for commit_id in &commit_dictionary {
        out.extend_from_slice(commit_id);
    }
    write_varint(&mut out, tail_dictionary.len() as u64);
    for tail in &tail_dictionary {
        out.extend_from_slice(tail);
    }
    let mut previous_key: &[u8] = &[];
    for entry in entries {
        let shared = shared_prefix_len(previous_key, entry.key);
        write_varint(&mut out, shared as u64);
        write_varint(&mut out, (entry.key.len() - shared) as u64);
        out.extend_from_slice(&entry.key[shared..]);
        out.extend_from_slice(&entry.value[..VALUE_CHANGE_ID_END]);
        let commit_ref = dictionary_ref(
            &commit_dictionary,
            &entry.value[VALUE_COMMIT_ID_START..VALUE_COMMIT_ID_END],
        );
        write_varint(&mut out, commit_ref);
        if commit_ref == 0 {
            out.extend_from_slice(&entry.value[VALUE_COMMIT_ID_START..VALUE_COMMIT_ID_END]);
        }
        let tail = &entry.value[VALUE_STATE_TAIL_START..];
        let tail_ref = slice_dictionary_ref(&tail_dictionary, tail);
        write_varint(&mut out, tail_ref);
        if tail_ref == 0 {
            out.extend_from_slice(tail);
        }
        previous_key = entry.key;
    }
    #[cfg(debug_assertions)]
    verify_leaf_round_trip(&out, entries);
    out
}

#[cfg(debug_assertions)]
fn verify_leaf_round_trip(encoded: &[u8], entries: &[EncodedLeafEntryRef<'_>]) {
    let decoded = match decode_node_ref(encoded) {
        Ok(DecodedNodeRef::Leaf(leaf)) => leaf,
        other => panic!("leaf round trip decoded unexpectedly: {other:?}"),
    };
    assert_eq!(decoded.len(), entries.len(), "leaf round trip entry count");
    for (index, entry) in entries.iter().enumerate() {
        let round_tripped = decoded
            .entry(index)
            .expect("leaf round trip entry should read")
            .expect("leaf round trip entry should exist");
        assert_eq!(round_tripped.key, entry.key, "leaf round trip key {index}");
        assert_eq!(
            round_tripped.value, entry.value,
            "leaf round trip value {index}"
        );
    }
}

fn repeated_dictionary<const N: usize>(
    entries: &[EncodedLeafEntryRef<'_>],
    start: usize,
) -> Vec<[u8; N]> {
    let mut counts = Vec::<([u8; N], usize)>::new();
    for entry in entries {
        let value = <[u8; N]>::try_from(&entry.value[start..start + N])
            .expect("fixed tracked-state value slice should match dictionary width");
        if let Some((_, count)) = counts.iter_mut().find(|(known, _)| known == &value) {
            *count += 1;
        } else {
            counts.push((value, 1));
        }
    }
    counts
        .into_iter()
        .filter_map(|(value, count)| (count > 1).then_some(value))
        .collect()
}

fn dictionary_ref<const N: usize>(dictionary: &[[u8; N]], value: &[u8]) -> u64 {
    dictionary
        .iter()
        .position(|known| known.as_slice() == value)
        .map_or(0, |index| index as u64 + 1)
}

fn repeated_tail_dictionary<'a>(entries: &[EncodedLeafEntryRef<'a>]) -> Vec<&'a [u8]> {
    let mut counts = Vec::<(&'a [u8], usize)>::new();
    for entry in entries {
        let tail = &entry.value[VALUE_STATE_TAIL_START..];
        if let Some((_, count)) = counts.iter_mut().find(|(known, _)| *known == tail) {
            *count += 1;
        } else {
            counts.push((tail, 1));
        }
    }
    counts
        .into_iter()
        .filter_map(|(tail, count)| (count > 1).then_some(tail))
        .collect()
}

fn slice_dictionary_ref(dictionary: &[&[u8]], value: &[u8]) -> u64 {
    dictionary
        .iter()
        .position(|known| *known == value)
        .map_or(0, |index| index as u64 + 1)
}

fn decode_leaf_v3(body: &[u8]) -> Result<DecodedLeafNodeRef, LixError> {
    fn usize_from(value: u64, what: &str) -> Result<usize, LixError> {
        usize::try_from(value).map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("tracked-state leaf node {what} does not fit in usize"),
            )
        })
    }
    fn slice<'b>(body: &'b [u8], offset: &mut usize, len: usize) -> Result<&'b [u8], LixError> {
        let end = offset.checked_add(len).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state leaf node length overflow",
            )
        })?;
        let bytes = body.get(*offset..end).ok_or_else(|| {
            LixError::new("LIX_ERROR_UNKNOWN", "tracked-state leaf node is truncated")
        })?;
        *offset = end;
        Ok(bytes)
    }

    let mut offset = 0usize;
    let entry_count = usize_from(
        read_varint(body, &mut offset, "tracked-state leaf node")?,
        "entry count",
    )?;
    let commit_dict_len = usize_from(
        read_varint(body, &mut offset, "tracked-state leaf node")?,
        "commit dictionary length",
    )?;
    let commit_dict_bytes = commit_dict_len.checked_mul(16).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state leaf node commit dictionary length overflow",
        )
    })?;
    let commit_dictionary = slice(body, &mut offset, commit_dict_bytes)?;
    let tail_dict_len = usize_from(
        read_varint(body, &mut offset, "tracked-state leaf node")?,
        "tail dictionary length",
    )?;
    let mut tail_dictionary = Vec::with_capacity(tail_dict_len.min(body.len()));
    for _ in 0..tail_dict_len {
        tail_dictionary.push(read_value_tail(
            body,
            &mut offset,
            "tracked-state leaf node",
        )?);
    }
    // Reconstructed keys can exceed their front-coded bytes, and dictionary
    // refs can omit up to 33 bytes from each value. Cap the count contribution
    // by the body size so corrupt metadata cannot force an unbounded reserve.
    let omitted_value_bytes = entry_count
        .min(body.len())
        .saturating_mul(VALUE_MAX_BYTES - VALUE_CHANGE_ID_END);
    let mut arena = Vec::with_capacity(body.len().saturating_add(omitted_value_bytes));
    let mut entries = Vec::with_capacity(entry_count.min(body.len()));
    let mut previous_key_start = 0usize;
    let mut previous_key_end = 0usize;
    for _ in 0..entry_count {
        let shared = usize_from(
            read_varint(body, &mut offset, "tracked-state leaf node")?,
            "shared key length",
        )?;
        let suffix_len = usize_from(
            read_varint(body, &mut offset, "tracked-state leaf node")?,
            "key suffix length",
        )?;
        let previous_key_len = previous_key_end - previous_key_start;
        if shared > previous_key_len {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state leaf node shares more key bytes than the previous key holds",
            ));
        }
        let key_start = arena.len();
        arena.extend_from_within(previous_key_start..previous_key_start + shared);
        let suffix = slice(body, &mut offset, suffix_len)?;
        arena.extend_from_slice(suffix);
        let key_end = arena.len();

        let change_id = slice(body, &mut offset, VALUE_CHANGE_ID_END)?;
        let commit_ref = usize_from(
            read_varint(body, &mut offset, "tracked-state leaf node")?,
            "commit dictionary ref",
        )?;
        let commit_id = if commit_ref == 0 {
            slice(
                body,
                &mut offset,
                VALUE_COMMIT_ID_END - VALUE_COMMIT_ID_START,
            )?
        } else {
            if commit_ref > commit_dict_len {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "tracked-state leaf node commit dictionary ref is out of bounds",
                ));
            }
            &commit_dictionary[(commit_ref - 1) * 16..commit_ref * 16]
        };
        let tail_ref = usize_from(
            read_varint(body, &mut offset, "tracked-state leaf node")?,
            "tail dictionary ref",
        )?;
        let tail = if tail_ref == 0 {
            read_value_tail(body, &mut offset, "tracked-state leaf node")?
        } else {
            if tail_ref > tail_dict_len {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "tracked-state leaf node tail dictionary ref is out of bounds",
                ));
            }
            tail_dictionary[tail_ref - 1]
        };
        let value_start = arena.len();
        arena.extend_from_slice(change_id);
        arena.extend_from_slice(commit_id);
        arena.extend_from_slice(tail);
        let value_end = arena.len();
        entries.push(LeafEntrySpan {
            key_start,
            key_end,
            value_start,
            value_end,
        });
        previous_key_start = key_start;
        previous_key_end = key_end;
    }
    if offset != body.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state leaf node has trailing bytes",
        ));
    }
    Ok(DecodedLeafNodeRef { arena, entries })
}

fn shared_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right.iter())
        .take_while(|(a, b)| a == b)
        .count()
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn read_varint(bytes: &[u8], offset: &mut usize, context: &str) -> Result<u64, LixError> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*offset).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("{context} varint is truncated"),
            )
        })?;
        *offset += 1;
        if shift >= 64 || (shift == 63 && byte > 1) {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("{context} varint overflows u64"),
            ));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

pub(crate) fn encode_internal_node(children: &[ChildSummary]) -> Vec<u8> {
    let children = children
        .iter()
        .map(ChildSummary::as_ref)
        .collect::<Vec<_>>();
    encode_internal_node_refs(&children)
}

pub(crate) fn encode_internal_node_refs(children: &[ChildSummaryRef<'_>]) -> Vec<u8> {
    assert!(
        !children.is_empty(),
        "tracked-state internal nodes must contain at least one child"
    );
    debug_assert!(
        children
            .iter()
            .all(|child| { child.first_key <= child.last_key && child.subtree_count > 0 })
    );
    debug_assert!(
        children
            .windows(2)
            .all(|pair| { pair[0].last_key < pair[1].first_key })
    );

    let mut out = Vec::with_capacity(2 + children.len() * 40);
    out.push(NODE_KIND_INTERNAL_V3);
    write_varint(&mut out, children.len() as u64);
    let mut previous_last: &[u8] = &[];
    for child in children {
        write_front_coded(&mut out, previous_last, child.first_key);
        write_front_coded(&mut out, child.first_key, child.last_key);
        out.extend_from_slice(&child.child_hash);
        write_varint(&mut out, child.subtree_count);
        previous_last = child.last_key;
    }
    out
}

fn write_front_coded(out: &mut Vec<u8>, base: &[u8], value: &[u8]) {
    let shared = shared_prefix_len(base, value);
    write_varint(out, shared as u64);
    write_varint(out, (value.len() - shared) as u64);
    out.extend_from_slice(&value[shared..]);
}

fn decode_internal_v3(body: &[u8]) -> Result<DecodedInternalNode, LixError> {
    fn usize_from(value: u64, what: &str) -> Result<usize, LixError> {
        usize::try_from(value).map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("tracked-state internal node {what} does not fit in usize"),
            )
        })
    }
    fn slice<'a>(body: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8], LixError> {
        let end = offset.checked_add(len).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state internal node length overflow",
            )
        })?;
        let bytes = body.get(*offset..end).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state internal node is truncated",
            )
        })?;
        *offset = end;
        Ok(bytes)
    }
    fn front_coded(body: &[u8], offset: &mut usize, base: &[u8]) -> Result<Vec<u8>, LixError> {
        let shared = usize_from(
            read_varint(body, offset, "tracked-state internal node")?,
            "shared boundary length",
        )?;
        if shared > base.len() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state internal node shares more boundary bytes than its base holds",
            ));
        }
        let suffix_len = usize_from(
            read_varint(body, offset, "tracked-state internal node")?,
            "boundary suffix length",
        )?;
        let suffix = slice(body, offset, suffix_len)?;
        let mut value = Vec::with_capacity(shared.saturating_add(suffix_len));
        value.extend_from_slice(&base[..shared]);
        value.extend_from_slice(suffix);
        Ok(value)
    }

    let mut offset = 0usize;
    let child_count = usize_from(
        read_varint(body, &mut offset, "tracked-state internal node")?,
        "child count",
    )?;
    if child_count == 0 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state internal node has no children",
        ));
    }
    let mut children = Vec::with_capacity(child_count.min(body.len()));
    for _ in 0..child_count {
        let previous_last = children
            .last()
            .map_or(&[][..], |child: &ChildSummary| child.last_key.as_slice());
        let first_key = front_coded(body, &mut offset, previous_last)?;
        let last_key = front_coded(body, &mut offset, &first_key)?;
        let child_hash = <[u8; TRACKED_STATE_HASH_BYTES]>::try_from(slice(
            body,
            &mut offset,
            TRACKED_STATE_HASH_BYTES,
        )?)
        .expect("fixed-size tracked-state child hash slice should convert");
        let subtree_count = read_varint(body, &mut offset, "tracked-state internal node")?;
        if subtree_count == 0 {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state internal node child has an empty subtree",
            ));
        }
        children.push(ChildSummary {
            first_key,
            last_key,
            child_hash,
            subtree_count,
        });
    }
    if offset != body.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state internal node has trailing bytes",
        ));
    }
    Ok(DecodedInternalNode { children })
}

pub(crate) fn decode_node(bytes: &[u8]) -> Result<DecodedNode, LixError> {
    match decode_node_ref(bytes)? {
        DecodedNodeRef::Leaf(leaf) => Ok(DecodedNode::Leaf(leaf)),
        DecodedNodeRef::Internal(internal) => Ok(DecodedNode::Internal(internal)),
    }
}

pub(crate) fn decode_node_ref(bytes: &[u8]) -> Result<DecodedNodeRef, LixError> {
    let (&kind, body) = bytes
        .split_first()
        .ok_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", "tracked-state tree node is empty"))?;
    match kind {
        NODE_KIND_LEAF_V3 => Ok(DecodedNodeRef::Leaf(decode_leaf_v3(body)?)),
        NODE_KIND_INTERNAL_V3 => Ok(DecodedNodeRef::Internal(decode_internal_v3(body)?)),
        other => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked-state tree node has unknown kind byte {other}"),
        )),
    }
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

#[expect(clippy::cast_precision_loss)]
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

#[cfg(test)]
mod tests {
    use super::{
        DecodedNodeRef as NodeRefForLeafTests, decode_node_ref as decode_node_ref_for_leaf_tests,
        encode_leaf_node_refs as encode_leaf_refs_for_tests,
    };

    fn raw_value(change: u8, commit: u8, tail: u8) -> Vec<u8> {
        let mut value = Vec::with_capacity(VALUE_MAX_BYTES);
        value.extend_from_slice(&[change; 16]);
        value.extend_from_slice(&[commit; 16]);
        write_value_tail(
            &mut value,
            tail & 1 != 0,
            u64::from(tail),
            u64::from(tail.wrapping_add(1)),
        );
        value
    }

    fn leaf_entries_round_trip(entries: &[(Vec<u8>, Vec<u8>)]) {
        let refs = entries
            .iter()
            .map(|(key, value)| EncodedLeafEntryRef {
                key: key.as_slice(),
                value: value.as_slice(),
            })
            .collect::<Vec<_>>();
        let encoded = encode_leaf_refs_for_tests(&refs);
        let NodeRefForLeafTests::Leaf(decoded) =
            decode_node_ref_for_leaf_tests(&encoded).expect("leaf should decode")
        else {
            panic!("leaf encoded bytes decoded as non-leaf");
        };
        assert_eq!(decoded.len(), entries.len());
        for (index, (key, value)) in entries.iter().enumerate() {
            let entry = decoded
                .entry(index)
                .expect("entry should read")
                .expect("entry should exist");
            assert_eq!(entry.key, key.as_slice(), "key {index}");
            assert_eq!(entry.value, value.as_slice(), "value {index}");
        }
    }

    #[test]
    fn leaf_v3_round_trips_representative_shapes() {
        leaf_entries_round_trip(&[]);
        leaf_entries_round_trip(&[(b"only".to_vec(), raw_value(1, 2, 3))]);
        leaf_entries_round_trip(&[
            (b"a".to_vec(), raw_value(1, 9, 7)),
            (b"ab".to_vec(), raw_value(2, 9, 7)),
            (b"abc/0001".to_vec(), raw_value(3, 9, 8)),
            (b"abc/0002".to_vec(), raw_value(4, 6, 8)),
            (b"zzz".to_vec(), raw_value(5, 6, 5)),
        ]);
        // No shared prefixes at all (front-coding worst case).
        leaf_entries_round_trip(&[
            (vec![0x00], raw_value(1, 2, 3)),
            (vec![0x80, 0x01], raw_value(4, 5, 6)),
            (vec![0xff, 0xff, 0xff], raw_value(7, 8, 9)),
        ]);
    }

    #[test]
    fn leaf_v3_round_trips_dictionaries_with_variable_tail_widths() {
        leaf_entries_round_trip(&[
            (b"a".to_vec(), raw_value(1, 9, 0)),
            (b"b".to_vec(), raw_value(2, 9, 0)),
            (b"c".to_vec(), raw_value(3, 9, 1)),
            (b"d".to_vec(), raw_value(4, 9, 1)),
        ]);
    }

    #[test]
    fn leaf_v3_round_trips_generated_sorted_keys() {
        // Deterministic pseudo-random keys with heavy shared prefixes,
        // mimicking encoded (schema_key, file_id, entity_pk) keys.
        let mut entries = (0..512usize)
            .map(|index| {
                let mut state = (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
                state ^= state >> 31;
                let key = format!(
                    "json_pointer\u{0}/packages/{:04}/{}",
                    index % 7,
                    state % 1000
                )
                .into_bytes();
                let value = raw_value(
                    state.to_le_bytes()[0],
                    (index % 3).to_le_bytes()[0],
                    (index % 5).to_le_bytes()[0],
                );
                (key, value)
            })
            .collect::<Vec<_>>();
        entries.sort();
        entries.dedup_by(|a, b| a.0 == b.0);
        leaf_entries_round_trip(&entries);
    }

    #[test]
    fn leaf_v3_round_trips_multibyte_key_varints() {
        // Keys long enough that shared and suffix lengths need two-byte
        // varints, with >127 entries so the count does too.
        let mut entries = Vec::new();
        let prefix = "p".repeat(160);
        for index in 0..200usize {
            let key = format!("{prefix}/{index:05}").into_bytes();
            let value = raw_value(index.to_le_bytes()[0], 1, 2);
            entries.push((key, value));
        }
        entries.sort();
        leaf_entries_round_trip(&entries);
    }

    /// Pins the assumption the leaf commit-id dictionary relies on: the
    /// encoded value places change_id at bytes [0..16) and commit_id at
    /// bytes [16..32) as raw uuid bytes.
    #[test]
    fn encoded_value_layout_places_ids_at_fixed_offsets() {
        let change_id = ChangeId::for_test_label("layout-change");
        let commit_id = CommitId::for_test_label("layout-commit");
        let encoded = encode_value(&TrackedStateIndexValue {
            change_id,
            commit_id,
            deleted: false,
            created_at: timestamp("created_at", "2026-01-01T00:00:00Z"),
            updated_at: timestamp("updated_at", "2026-01-01T00:00:00Z"),
        });
        assert!((VALUE_MIN_BYTES..=VALUE_MAX_BYTES).contains(&encoded.len()));
        assert_eq!(&encoded[..16], change_id.as_uuid().as_bytes());
        assert_eq!(&encoded[16..32], commit_id.as_uuid().as_bytes());
    }

    #[test]
    fn leaf_v3_round_trips_repeated_and_literal_value_parts() {
        let mut entries = Vec::new();
        for index in 0..300usize {
            let key = format!("rows/{index:05}").into_bytes();
            let value = raw_value(
                index.to_le_bytes()[0],
                (index % 3).to_le_bytes()[0],
                (index % 5).to_le_bytes()[0],
            );
            entries.push((key, value));
        }
        entries.sort();
        leaf_entries_round_trip(&entries);

        // Both dictionaries must actually compress the repeated commit ids
        // and state tails.
        let refs = entries
            .iter()
            .map(|(key, value)| EncodedLeafEntryRef {
                key: key.as_slice(),
                value: value.as_slice(),
            })
            .collect::<Vec<_>>();
        let encoded = encode_leaf_refs_for_tests(&refs);
        let verbatim_size: usize = entries
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum();
        assert!(
            encoded.len() + 300 * 10 < verbatim_size,
            "dictionaries must remove repeated value bytes: encoded={} verbatim={}",
            encoded.len(),
            verbatim_size
        );
    }

    #[test]
    fn leaf_v3_wire_format_is_pinned() {
        let entries = [
            (b"k1".to_vec(), raw_value(0xAA, 0xCC, 0xDD)),
            (b"k2".to_vec(), raw_value(0xBB, 0xCC, 0xDD)),
            (b"k3".to_vec(), raw_value(0xEE, 0xFF, 0x11)),
        ];
        let refs = entries
            .iter()
            .map(|(key, value)| EncodedLeafEntryRef {
                key: key.as_slice(),
                value: value.as_slice(),
            })
            .collect::<Vec<_>>();
        let encoded = encode_leaf_refs_for_tests(&refs);
        let mut expected = vec![
            3, // NODE_KIND_LEAF_V3
            3, // entry count
            1, // commit dictionary length
        ];
        expected.extend_from_slice(&[0xCC; 16]); // dictionary slot 0
        expected.push(1); // tail dictionary length
        expected.extend_from_slice(&[0x93, 0xDD, 0xDE]); // dictionary slot 0
        // Entry 0: full key, inline change id, both dictionary refs.
        expected.extend_from_slice(&[0, 2, b'k', b'1']);
        expected.extend_from_slice(&[0xAA; 16]);
        expected.extend_from_slice(&[1, 1]);
        // Entry 1: one-byte key suffix and both dictionary refs.
        expected.extend_from_slice(&[1, 1, b'2']);
        expected.extend_from_slice(&[0xBB; 16]);
        expected.extend_from_slice(&[1, 1]);
        // Entry 2: literal unique commit id and tail.
        expected.extend_from_slice(&[1, 1, b'3']);
        expected.extend_from_slice(&[0xEE; 16]);
        expected.push(0);
        expected.extend_from_slice(&[0xFF; 16]);
        expected.push(0);
        expected.extend_from_slice(&[0x93, 0x11, 0x12]);
        assert_eq!(encoded, expected, "v3 wire bytes must stay stable");
    }

    #[test]
    fn leaf_v3_tail_dictionary_saves_83_bytes_for_modeled_shape() {
        let entries = (0..32usize)
            .map(|index| {
                (
                    format!("rows/{index:05}").into_bytes(),
                    raw_value(index.to_le_bytes()[0], 9, (index % 4 + 1).to_le_bytes()[0]),
                )
            })
            .collect::<Vec<_>>();
        let refs = entries
            .iter()
            .map(|(key, value)| EncodedLeafEntryRef {
                key: key.as_slice(),
                value: value.as_slice(),
            })
            .collect::<Vec<_>>();
        let encoded = encode_leaf_refs_for_tests(&refs);
        let key_section = refs
            .iter()
            .scan(&[][..], |previous, entry| {
                let shared = shared_prefix_len(previous, entry.key);
                *previous = entry.key;
                Some(2 + entry.key.len() - shared)
            })
            .sum::<usize>();
        // The v2 leaf stores a one-byte commit ref, one-byte value length,
        // and the value after its dictionary-compressed 16-byte commit id.
        let v2_bytes = 1
            + 1
            + 1
            + 16
            + key_section
            + entries
                .iter()
                .map(|(_, value)| 2 + value.len() - 16)
                .sum::<usize>();

        assert_eq!(v2_bytes - encoded.len(), 83);
    }

    #[test]
    fn leaf_v3_rejects_malformed_and_legacy_bytes() {
        let entries = [(b"key-a".to_vec(), raw_value(1, 2, 3))];
        let encoded = encode_leaf_refs_for_tests(
            &entries
                .iter()
                .map(|(key, value)| EncodedLeafEntryRef {
                    key: key.as_slice(),
                    value: value.as_slice(),
                })
                .collect::<Vec<_>>(),
        );

        let mut unknown_kind = encoded.clone();
        unknown_kind[0] = 0x7f;
        assert!(decode_node_ref_for_leaf_tests(&unknown_kind).is_err());

        let truncated = &encoded[..encoded.len() - 1];
        assert!(decode_node_ref_for_leaf_tests(truncated).is_err());

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(decode_node_ref_for_leaf_tests(&trailing).is_err());

        assert!(decode_node_ref_for_leaf_tests(&[1, 0, 0]).is_err());
        assert!(decode_node_ref_for_leaf_tests(&[]).is_err());

        // A dictionary length that cannot fit in the body.
        assert!(decode_node_ref_for_leaf_tests(&[3, 1, 2, 0]).is_err());

        // A state-tail dictionary entry with a reserved width tag.
        assert!(decode_node_ref_for_leaf_tests(&[3, 0, 0, 1, 90]).is_err());

        // Commit ref is non-zero while the dictionary is empty.
        let mut bad_commit_ref = vec![3, 1, 0, 0, 0, 1, b'k'];
        bad_commit_ref.extend_from_slice(&[0; 16]);
        bad_commit_ref.push(1);
        assert!(decode_node_ref_for_leaf_tests(&bad_commit_ref).is_err());

        // Tail ref is non-zero while the dictionary is empty.
        let mut bad_tail_ref = vec![3, 1, 0, 0, 0, 1, b'k'];
        bad_tail_ref.extend_from_slice(&[0; 16]);
        bad_tail_ref.push(0);
        bad_tail_ref.extend_from_slice(&[0; 16]);
        bad_tail_ref.push(1);
        assert!(decode_node_ref_for_leaf_tests(&bad_tail_ref).is_err());
    }

    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;

    fn timestamp(field: &str, value: &str) -> LixTimestamp {
        LixTimestamp::expect_parse(field, value)
    }

    fn test_value(commit_id: &str, change_id: &str) -> TrackedStateIndexValue {
        TrackedStateIndexValue {
            change_id: ChangeId::for_test_label(change_id),
            commit_id: CommitId::for_test_label(commit_id),
            deleted: false,
            created_at: timestamp("created_at", "2026-01-01T00:00:00Z"),
            updated_at: timestamp("updated_at", "2026-01-02T00:00:00Z"),
        }
    }

    fn set_timestamps(value: &mut TrackedStateIndexValue, created_at: &str, updated_at: &str) {
        value.created_at = timestamp("created_at", created_at);
        value.updated_at = timestamp("updated_at", updated_at);
    }

    #[test]
    fn key_codec_distinguishes_null_and_value_file_id() {
        let null_key = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_pk: EntityPk::single("entity"),
        });
        let file_key = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: Some("file".to_string()),
            entity_pk: EntityPk::single("entity"),
        });

        assert_ne!(null_key, file_key);
        assert_eq!(
            decode_key(&null_key).expect("null key"),
            TrackedStateKey {
                schema_key: "schema".to_string(),
                file_id: None,
                entity_pk: EntityPk::single("entity"),
            }
        );
        assert_eq!(
            decode_key(&file_key).expect("file key"),
            TrackedStateKey {
                schema_key: "schema".to_string(),
                file_id: Some("file".to_string()),
                entity_pk: EntityPk::single("entity"),
            }
        );
    }

    #[test]
    fn key_codec_encodes_composite_identity_as_string_tuple_parts() {
        let key = TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_pk: EntityPk {
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
    fn key_codec_decodes_entity_suffix_with_trusted_prefix() {
        let key = TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: Some("file".to_string()),
            entity_pk: EntityPk {
                parts: vec!["namespace".to_string(), "id".to_string()],
            },
        };
        let encoded = encode_key(&key);
        let prefix = encode_schema_file_prefix("schema", Some("file"));

        assert_eq!(
            decode_key_with_trusted_prefix(&encoded, "schema", Some("file"), prefix.len())
                .expect("key suffix should decode"),
            key
        );
    }

    #[test]
    fn key_codec_rejects_malformed_storage_bytes() {
        let mut encoded = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_pk: EntityPk {
                parts: vec!["true".to_string()],
            },
        });
        encoded.truncate(encoded.len() - 1);

        let error = decode_key(&encoded).expect_err("truncated key should reject");
        assert!(error.to_string().contains("tracked-state key"));
    }

    #[test]
    fn key_codec_rejects_empty_entity_pk() {
        let encoded = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_pk: EntityPk { parts: Vec::new() },
        });

        let error = decode_key(&encoded).expect_err("empty entity pk should reject");

        assert!(error.message.contains("entity primary key is empty"));
    }

    #[test]
    fn key_codec_preserves_tuple_prefix_ordering() {
        let prefix = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_pk: EntityPk {
                parts: vec!["a".to_string()],
            },
        });
        let extended = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_pk: EntityPk {
                parts: vec!["a".to_string(), "b".to_string()],
            },
        });

        assert!(prefix < extended);
        assert!(!extended.starts_with(&prefix));
    }

    #[test]
    fn key_codec_wire_format_is_pinned() {
        let key = TrackedStateKey {
            schema_key: "s\0".to_string(),
            file_id: Some(String::new()),
            entity_pk: EntityPk {
                parts: vec!["a\0".to_string(), String::new()],
            },
        };
        let encoded = encode_key(&key);

        assert_eq!(
            encoded,
            vec![
                b's', 0, 0xff, 0, 0, // schema "s\\0"
                1, 0, 0, // Some("")
                b'a', 0, 0xff, 0, 1, // non-final PK part "a\\0"
                0, 0, // final empty PK part
            ]
        );
        assert_eq!(decode_key(&encoded).expect("key should decode"), key);
        assert_eq!(
            encode_key_ref(TrackedStateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            }),
            encoded
        );
    }

    #[test]
    fn key_codec_byte_order_matches_logical_order_and_is_prefix_free() {
        let strings = ["", "\0", "a", "a\0", "a\u{1}", "z", "é"];
        let mut keys = Vec::new();
        for schema in strings {
            for file_id in [None, Some(""), Some("a"), Some("a\0")] {
                for first in strings {
                    keys.push(TrackedStateKey {
                        schema_key: schema.to_string(),
                        file_id: file_id.map(str::to_string),
                        entity_pk: EntityPk::single(first),
                    });
                    keys.push(TrackedStateKey {
                        schema_key: schema.to_string(),
                        file_id: file_id.map(str::to_string),
                        entity_pk: EntityPk {
                            parts: vec![first.to_string(), "tail".to_string()],
                        },
                    });
                }
            }
        }
        keys.sort();
        keys.dedup();
        let mut by_encoded = keys
            .iter()
            .cloned()
            .map(|key| (encode_key(&key), key))
            .collect::<Vec<_>>();
        by_encoded.sort_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(
            by_encoded.iter().map(|(_, key)| key).collect::<Vec<_>>(),
            keys.iter().collect::<Vec<_>>()
        );
        for (index, (encoded, _)) in by_encoded.iter().enumerate() {
            for (other_index, (other, _)) in by_encoded.iter().enumerate() {
                if index != other_index {
                    assert!(
                        !other.starts_with(encoded),
                        "complete encoded key {index} prefixes key {other_index}"
                    );
                }
            }
        }
    }

    #[test]
    fn key_codec_prefixes_select_exact_schema_and_file() {
        let keys = [
            TrackedStateKey {
                schema_key: "a".to_string(),
                file_id: None,
                entity_pk: EntityPk::single("one"),
            },
            TrackedStateKey {
                schema_key: "a".to_string(),
                file_id: Some(String::new()),
                entity_pk: EntityPk::single("two"),
            },
            TrackedStateKey {
                schema_key: "a\0".to_string(),
                file_id: None,
                entity_pk: EntityPk::single("three"),
            },
        ];
        let encoded = keys.iter().map(encode_key).collect::<Vec<_>>();
        let schema = encode_schema_key_prefix("a");
        let null_file = encode_schema_file_prefix("a", None);
        let empty_file = encode_schema_file_prefix("a", Some(""));

        assert_eq!(
            encoded
                .iter()
                .map(|key| key.starts_with(&schema))
                .collect::<Vec<_>>(),
            vec![true, true, false]
        );
        assert_eq!(
            encoded
                .iter()
                .map(|key| key.starts_with(&null_file))
                .collect::<Vec<_>>(),
            vec![true, false, false]
        );
        assert_eq!(
            encoded
                .iter()
                .map(|key| key.starts_with(&empty_file))
                .collect::<Vec<_>>(),
            vec![false, true, false]
        );
    }

    #[test]
    fn leaf_front_coding_round_trips_across_a_nul_escape_boundary() {
        let short = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_pk: EntityPk::single("a"),
        });
        let with_nul = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_pk: EntityPk::single("a\0"),
        });
        let shared = shared_prefix_len(&short, &with_nul);
        assert_eq!(short[shared - 1], 0);
        assert_eq!(with_nul[shared], KEY_ESCAPE);

        leaf_entries_round_trip(&[(short, raw_value(1, 2, 3)), (with_nul, raw_value(4, 2, 3))]);
    }

    #[test]
    fn key_codec_rejects_invalid_escapes_tags_utf8_and_tuple_endings() {
        assert!(decode_key(&[b's', 0]).is_err());
        assert!(decode_key(&[b's', 0, 2]).is_err());
        assert!(decode_key(&[0xff, 0, 0, 0]).is_err());
        assert!(decode_key(&[b's', 0, 0, 2]).is_err());
        assert!(decode_key(&[b's', 0, 0, 0]).is_err());

        let mut missing_final = encode_key(&TrackedStateKey {
            schema_key: "s".to_string(),
            file_id: None,
            entity_pk: EntityPk::single("pk"),
        });
        *missing_final.last_mut().expect("key has terminator") = KEY_PART_MORE;
        assert!(decode_key(&missing_final).is_err());
    }

    #[test]
    fn value_codec_roundtrips_change_ref_value() {
        let value = TrackedStateIndexValue {
            change_id: ChangeId::for_test_label("change"),
            commit_id: CommitId::for_test_label("commit"),
            deleted: false,
            created_at: timestamp("created_at", "2026-01-01T00:00:00Z"),
            updated_at: timestamp("updated_at", "2026-01-02T00:00:00Z"),
        };

        let encoded = encode_value(&value);
        assert_eq!(decode_value(&encoded).expect("value"), value);
    }

    #[test]
    fn value_codec_roundtrips_second_change_ref_value() {
        let value = TrackedStateIndexValue {
            change_id: ChangeId::for_test_label("other-change"),
            commit_id: CommitId::for_test_label("other-commit"),
            deleted: true,
            created_at: timestamp("created_at", "2026-01-01T00:00:00Z"),
            updated_at: timestamp("updated_at", "2026-01-02T00:00:00Z"),
        };

        let encoded = encode_value(&value);
        assert_eq!(decode_value(&encoded).expect("value"), value);
    }

    #[test]
    fn value_codec_uses_variable_width_timestamps() {
        let mut value = test_value("commit", "change");
        set_timestamps(&mut value, "1970-01-01T00:00:00Z", "1970-01-01T00:00:00Z");
        let epoch_equal = encode_value(&value);
        assert_eq!(epoch_equal.len(), 33);
        assert_eq!(epoch_equal[VALUE_STATE_TAIL_START], 0x00);
        assert_eq!(decode_value(&epoch_equal).expect("epoch value"), value);

        set_timestamps(
            &mut value,
            "1970-01-01T00:00:00Z",
            "1970-01-01T00:00:00.001Z",
        );
        let epoch_distinct = encode_value(&value);
        assert_eq!(epoch_distinct.len(), 35);
        assert_eq!(epoch_distinct[VALUE_STATE_TAIL_START], 0x0b);
        assert_eq!(decode_value(&epoch_distinct).expect("epoch value"), value);

        set_timestamps(&mut value, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z");
        let modern_equal = encode_value(&value);
        assert_eq!(modern_equal.len(), 40);
        assert_eq!(modern_equal[VALUE_STATE_TAIL_START], 0x07);
        assert_eq!(
            &modern_equal[VALUE_STATE_TAIL_START + 1..],
            &[0x00, 0x00, 0x80, 0xaa, 0x6d, 0xb7, 0x19]
        );
        assert_eq!(decode_value(&modern_equal).expect("modern value"), value);

        set_timestamps(&mut value, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z");
        let modern_distinct = encode_value(&value);
        assert_eq!(modern_distinct.len(), 47);
        assert_eq!(modern_distinct[VALUE_STATE_TAIL_START], 0x4f);
        assert_eq!(decode_value(&modern_distinct).expect("modern value"), value);

        value.deleted = true;
        assert_eq!(
            encode_value(&value)[VALUE_STATE_TAIL_START],
            0x4f | VALUE_TAIL_DELETED
        );

        set_timestamps(&mut value, "3000-01-01T00:00:00Z", "3000-01-02T00:00:00Z");
        let far_future = encode_value(&value);
        assert_eq!(far_future.len(), VALUE_MAX_BYTES);
        assert_eq!(decode_value(&far_future).expect("far-future value"), value);
    }

    #[test]
    fn owned_value_codec_matches_borrowed_value_codec() {
        let mut compact = test_value("commit", "change");
        set_timestamps(&mut compact, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z");

        let compact_owned = encode_value(&compact);
        let compact_borrowed = encode_value_ref(TrackedStateIndexValueRef {
            change_id: compact.change_id,
            commit_id: compact.commit_id,
            deleted: compact.deleted,
            created_at: compact.created_at,
            updated_at: compact.updated_at,
        });
        assert_eq!(compact_owned, compact_borrowed);
        assert_eq!(
            decode_value(&compact_owned).expect("compact value"),
            compact
        );

        let mut distinct = compact.clone();
        set_timestamps(
            &mut distinct,
            "2026-01-01T00:00:00Z",
            "2026-01-02T00:00:00Z",
        );

        let distinct_owned = encode_value(&distinct);
        let distinct_borrowed = encode_value_ref(TrackedStateIndexValueRef {
            change_id: distinct.change_id,
            commit_id: distinct.commit_id,
            deleted: distinct.deleted,
            created_at: distinct.created_at,
            updated_at: distinct.updated_at,
        });
        assert_eq!(distinct_owned, distinct_borrowed);
        assert_eq!(
            decode_value(&distinct_owned).expect("distinct value"),
            distinct
        );
    }

    #[test]
    fn value_codec_rejects_malformed_storage_bytes() {
        let value = encode_value(&test_value("commit", "change"));
        assert!(decode_value(&value[..value.len() - 1]).is_err());

        let mut reserved_tag = value.clone();
        reserved_tag[VALUE_STATE_TAIL_START] = 90;
        assert!(decode_value(&reserved_tag).is_err());

        let mut non_minimal = value[..VALUE_STATE_TAIL_START].to_vec();
        non_minimal.extend_from_slice(&[1, 0]);
        assert!(decode_value(&non_minimal).is_err());

        let mut distinct_but_equal = value[..VALUE_STATE_TAIL_START].to_vec();
        distinct_but_equal.extend_from_slice(&[0x13, 1, 1]);
        assert!(decode_value(&distinct_but_equal).is_err());

        let mut invalid_timestamp = value[..VALUE_STATE_TAIL_START].to_vec();
        invalid_timestamp.extend_from_slice(&[2, 0xdc, 0x05]);
        assert!(decode_value(&invalid_timestamp).is_err());

        let mut trailing = encode_value(&test_value("commit", "change"));
        trailing.push(0);
        assert!(decode_value(&trailing).is_err());
    }

    #[test]
    fn encoded_value_len_matches_encoded_value_bytes() {
        let values = [
            TrackedStateIndexValue {
                change_id: ChangeId::for_test_label("change"),
                commit_id: CommitId::for_test_label("commit"),
                deleted: false,
                created_at: timestamp("created_at", "2026-01-01T00:00:00Z"),
                updated_at: timestamp("updated_at", "2026-01-02T00:00:00Z"),
            },
            TrackedStateIndexValue {
                change_id: ChangeId::for_test_label("change-2"),
                commit_id: CommitId::for_test_label("commit"),
                deleted: true,
                created_at: timestamp("created_at", "2026-01-01T00:00:00Z"),
                updated_at: timestamp("updated_at", "2026-01-02T00:00:00Z"),
            },
            TrackedStateIndexValue {
                change_id: ChangeId::for_test_label("change-3"),
                commit_id: CommitId::for_test_label("other"),
                deleted: false,
                created_at: timestamp("created_at", "2026-01-01T00:00:00Z"),
                updated_at: timestamp("updated_at", "2026-01-02T00:00:00Z"),
            },
        ];

        for value in values {
            assert_eq!(encoded_value_len(&value), encode_value(&value).len());
        }
    }

    #[test]
    fn leaf_node_codec_roundtrips_borrowed_entries() {
        let entries = vec![
            EncodedLeafEntry {
                key: b"alpha".to_vec(),
                value: raw_value(1, 2, 3),
            },
            EncodedLeafEntry {
                key: b"bravo".to_vec(),
                value: raw_value(4, 5, 6),
            },
        ];

        let encoded = encode_leaf_node(&entries);
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
        assert_eq!(second.value, raw_value(4, 5, 6));

        let DecodedNode::Leaf(owned) = decode_node(&encoded).expect("owned leaf") else {
            panic!("expected owned leaf node");
        };
        assert_eq!(owned.into_entries(), entries);
    }

    #[test]
    fn leaf_node_codec_roundtrips_empty_leaf() {
        let encoded = encode_leaf_node(&[]);

        let DecodedNodeRef::Leaf(leaf) = decode_node_ref(&encoded).expect("leaf ref") else {
            panic!("expected leaf node");
        };
        assert_eq!(leaf.len(), 0);
        assert!(leaf.entry(0).expect("missing entry").is_none());
    }

    #[test]
    fn leaf_node_codec_rejects_malformed_storage_bytes() {
        let entries = vec![
            EncodedLeafEntry {
                key: b"alpha".to_vec(),
                value: raw_value(1, 2, 3),
            },
            EncodedLeafEntry {
                key: b"bravo".to_vec(),
                value: raw_value(4, 5, 6),
            },
        ];
        let mut encoded = encode_leaf_node(&entries);
        encoded.truncate(encoded.len() - 1);

        let error = decode_node_ref(&encoded).expect_err("truncated leaf should reject");

        assert!(
            error.to_string().contains("tracked-state leaf node"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn internal_v3_round_trips_and_pins_front_coded_boundaries() {
        let children = vec![
            ChildSummary {
                first_key: b"aa".to_vec(),
                last_key: b"az".to_vec(),
                child_hash: [1; TRACKED_STATE_HASH_BYTES],
                subtree_count: 3,
            },
            ChildSummary {
                first_key: b"ba".to_vec(),
                last_key: b"bz".to_vec(),
                child_hash: [2; TRACKED_STATE_HASH_BYTES],
                subtree_count: 4,
            },
        ];
        let encoded = encode_internal_node(&children);
        let mut expected = vec![
            4, 2, // kind, child count
            0, 2, b'a', b'a', // first "aa" relative to empty
            1, 1, b'z', // last "az" relative to "aa"
        ];
        expected.extend_from_slice(&[1; TRACKED_STATE_HASH_BYTES]);
        expected.extend_from_slice(&[
            3, // subtree count
            0, 2, b'b', b'a', // first "ba" relative to previous last "az"
            1, 1, b'z', // last "bz" relative to "ba"
        ]);
        expected.extend_from_slice(&[2; TRACKED_STATE_HASH_BYTES]);
        expected.push(4);
        assert_eq!(encoded, expected, "internal v3 wire bytes must stay stable");

        let DecodedNode::Internal(decoded) = decode_node(&encoded).expect("internal node") else {
            panic!("expected internal node");
        };
        assert_eq!(decoded.children(), children);
    }

    #[test]
    fn internal_v3_rejects_empty_truncated_and_invalid_boundaries() {
        assert!(decode_node(&[NODE_KIND_INTERNAL_V3, 0]).is_err());
        assert!(decode_node(&[NODE_KIND_INTERNAL_V3, 1]).is_err());
        assert!(decode_node(&[NODE_KIND_INTERNAL_V3, 1, 1, 0]).is_err());

        let child = ChildSummary {
            first_key: b"a".to_vec(),
            last_key: b"z".to_vec(),
            child_hash: [9; TRACKED_STATE_HASH_BYTES],
            subtree_count: 1,
        };
        let encoded = encode_internal_node(&[child]);
        let mut zero_subtree = encoded.clone();
        *zero_subtree.last_mut().expect("subtree count") = 0;
        assert!(decode_node(&zero_subtree).is_err());

        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_node(&trailing).is_err());
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
