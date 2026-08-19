//! Frozen decoder for protocol-v68 `LXCD16` commit-delta segments.
//!
//! The live tracked-state codec intentionally rejects this format. Migration
//! code uses this module to recover the logical, owned members before writing
//! them through the current codec.

use std::borrow::Cow;

use crate::changelog::{ChangeId, CommitId};
use crate::common::LixError;
use crate::common::LixTimestamp;
use crate::json_store::JsonSlot;
use crate::order_preserving_key::{
    FILE_ID_NONE, FILE_ID_SOME, KEY_PART_FINAL, KEY_PART_MORE, KeyPartError, ROW_PK_BYTES,
    ROW_PK_CODEC_V1, ROW_PK_INTEGER, ROW_PK_INTEGER_BYTES, ROW_PK_STRING, ROW_PK_UUID,
    ROW_PK_UUID_BYTES, ScannedKeyValue, i64_from_ordered_integer, is_key_part_terminator,
    scan_key_part,
};
use crate::row_pk::{RowPk, RowPkComponent};
use crate::tracked_state::{TrackedStateBaseCoordinate, TrackedStateIndexValue, TrackedStateKey};

const FORMAT_MAGIC: &[u8] = b"LXCD16";
const SEGMENT_MAX_ROWS: usize = 512;
const PAYLOAD_OFFSET_BYTES: usize = size_of::<u32>();
#[cfg(not(test))]
const MAX_SIDECAR_BYTES: usize = 64 * 1024 * 1024;
#[cfg(test)]
const MAX_SIDECAR_BYTES: usize = 1024 * 1024;

const SIDECAR_RAW: u8 = 0;
const SIDECAR_ZSTD: u8 = 1;
const SIDECAR_AUTHORED_INLINE_RAW: u8 = 3;
const SIDECAR_AUTHORED_INLINE_ZSTD: u8 = 4;

const PAYLOAD_AUTHORED: u8 = 0;
const PAYLOAD_SELECTED_REF: u8 = 1;
const PAYLOAD_SELECTED_TOMBSTONE: u8 = 2;

const NODE_KIND_LEAF_V4: u8 = 5;
const NODE_KIND_INTERNAL_V4: u8 = 6;
const NODE_KIND_DIRECT_LEAF_V1: u8 = 7;

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

/// Manifest bounds against which a physical v68 segment can be authenticated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::migration) struct CommitDeltaSegmentBounds {
    pub(in crate::migration) first_key: Vec<u8>,
    pub(in crate::migration) last_key: Vec<u8>,
}

/// One owned logical member recovered from a v68 commit-delta segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::migration) struct CommitDeltaMember {
    pub(in crate::migration) key: TrackedStateKey,
    pub(in crate::migration) value: TrackedStateIndexValue,
    pub(in crate::migration) payload: CommitDeltaPayloadDescriptor,
}

/// Current-compatible owned description of a v68 member payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::migration) enum CommitDeltaPayloadDescriptor {
    Authored {
        snapshot: JsonSlot,
        metadata: JsonSlot,
        /// V68 predates native typed-row payloads.
        typed_payload: Option<Vec<u8>>,
        origin_key: Option<String>,
        base_coordinate: Option<TrackedStateBaseCoordinate>,
    },
    SelectedRef {
        base_coordinate: Option<TrackedStateBaseCoordinate>,
    },
    SelectedTombstone {
        base_coordinate: Option<TrackedStateBaseCoordinate>,
    },
}

/// Packed authored payload shape written by protocol v68.
#[derive(Debug, musli::Decode)]
#[musli(packed)]
struct AuthoredPayload {
    #[musli(with = crate::json_store::json_slot_storage)]
    snapshot: JsonSlot,
    #[musli(with = crate::json_store::json_slot_storage)]
    metadata: JsonSlot,
    #[musli(with = crate::storage_codec::option)]
    origin_key: Option<String>,
    #[musli(with = crate::storage_codec::option)]
    base_coordinate: Option<TrackedStateBaseCoordinate>,
}

#[derive(Clone, Copy)]
enum PayloadLayout {
    Indexed,
    AuthoredInline,
}

struct PayloadIndex<'a> {
    sidecar: Cow<'a, [u8]>,
    offsets_start: usize,
    payload_start: usize,
    entry_count: usize,
    layout: PayloadLayout,
}

/// Decodes and fully validates one frozen v68 `LXCD16` segment.
pub(in crate::migration) fn decode_commit_delta_segment(
    bytes: &[u8],
    expected_bounds: Option<&CommitDeltaSegmentBounds>,
    expected_commit_id: CommitId,
) -> Result<Vec<CommitDeltaMember>, LixError> {
    let (leaf_bytes, encoded_sidecar) = split_segment(bytes)?;
    let leaf = decode_leaf(leaf_bytes)?;
    if leaf.len() == 0 || leaf.len() > SEGMENT_MAX_ROWS {
        return Err(error("segment has an invalid entry count"));
    }
    if let Some(bounds) = expected_bounds
        && (leaf.first().map(|entry| entry.key.as_slice()) != Some(bounds.first_key.as_slice())
            || leaf.last().map(|entry| entry.key.as_slice()) != Some(bounds.last_key.as_slice()))
    {
        return Err(error("segment does not match its manifest bounds"));
    }

    let payloads = decode_payload_index(encoded_sidecar, leaf.len())?;
    let mut members = Vec::with_capacity(leaf.len());
    for (index, entry) in leaf.into_iter().enumerate() {
        let value = decode_value(&entry.value)?;
        if value.commit_id != expected_commit_id {
            return Err(error("segment member has the wrong physical commit id"));
        }
        members.push(CommitDeltaMember {
            key: decode_key(&entry.key)?,
            value,
            payload: payloads.decode(index)?,
        });
    }
    Ok(members)
}

struct RawLeafEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

fn decode_leaf(bytes: &[u8]) -> Result<Vec<RawLeafEntry>, LixError> {
    let (&kind, body) = bytes
        .split_first()
        .ok_or_else(|| error("tree leaf is empty"))?;
    match kind {
        NODE_KIND_LEAF_V4 => decode_leaf_v4(body),
        NODE_KIND_DIRECT_LEAF_V1 => decode_direct_leaf_v1(body),
        NODE_KIND_INTERNAL_V4 => Err(error("segment contains an internal tree node")),
        other => Err(error(format!("tree leaf has unknown kind byte {other}"))),
    }
}

fn decode_leaf_v4(body: &[u8]) -> Result<Vec<RawLeafEntry>, LixError> {
    let mut offset = 0;
    let entry_count = read_count(body, &mut offset, "leaf entry count")?;
    let commit_dict_len = read_count(body, &mut offset, "leaf commit dictionary length")?;
    let commit_dict_bytes = commit_dict_len
        .checked_mul(16)
        .ok_or_else(|| error("leaf commit dictionary length overflows"))?;
    let commit_dictionary = take(
        body,
        &mut offset,
        commit_dict_bytes,
        "leaf commit dictionary",
    )?;
    let tail_dict_len = read_count(body, &mut offset, "leaf tail dictionary length")?;
    let mut tail_dictionary = Vec::with_capacity(tail_dict_len.min(body.len()));
    for _ in 0..tail_dict_len {
        tail_dictionary.push(read_value_tail(body, &mut offset, "v68 leaf")?);
    }

    let mut entries = Vec::with_capacity(entry_count.min(body.len()));
    let mut previous_key = Vec::new();
    for _ in 0..entry_count {
        let shared = read_count(body, &mut offset, "leaf shared key length")?;
        let suffix_len = read_count(body, &mut offset, "leaf key suffix length")?;
        if shared > previous_key.len() {
            return Err(error("leaf shares more bytes than its previous key"));
        }
        let suffix = take(body, &mut offset, suffix_len, "leaf key suffix")?;
        let mut key = Vec::with_capacity(shared.saturating_add(suffix_len));
        key.extend_from_slice(&previous_key[..shared]);
        key.extend_from_slice(suffix);
        if !entries.is_empty() && previous_key >= key {
            return Err(error("leaf keys are not strictly ordered"));
        }

        let change_id = take(body, &mut offset, 16, "leaf change id")?;
        let commit_ref = read_count(body, &mut offset, "leaf commit dictionary ref")?;
        let commit_id = if commit_ref == 0 {
            take(body, &mut offset, 16, "leaf commit id")?
        } else {
            if commit_ref > commit_dict_len {
                return Err(error("leaf commit dictionary ref is out of bounds"));
            }
            &commit_dictionary[(commit_ref - 1) * 16..commit_ref * 16]
        };
        let tail_ref = read_count(body, &mut offset, "leaf tail dictionary ref")?;
        let tail = if tail_ref == 0 {
            read_value_tail(body, &mut offset, "v68 leaf")?
        } else {
            if tail_ref > tail_dict_len {
                return Err(error("leaf tail dictionary ref is out of bounds"));
            }
            tail_dictionary[tail_ref - 1]
        };
        let mut value = Vec::with_capacity(32 + tail.len());
        value.extend_from_slice(change_id);
        value.extend_from_slice(commit_id);
        value.extend_from_slice(tail);
        previous_key = key.clone();
        entries.push(RawLeafEntry { key, value });
    }
    if offset != body.len() {
        return Err(error("leaf has trailing bytes"));
    }
    Ok(entries)
}

fn decode_direct_leaf_v1(body: &[u8]) -> Result<Vec<RawLeafEntry>, LixError> {
    let mut offset = 0;
    let entry_count = read_count(body, &mut offset, "direct leaf entry count")?;
    if entry_count == 0 {
        return Err(error("direct leaf has no entries"));
    }
    let commit_id: [u8; 16] = take(body, &mut offset, 16, "direct leaf commit id")?
        .try_into()
        .expect("fixed commit-id slice");
    let first_packed = u32::from_be_bytes(
        take(body, &mut offset, 4, "direct leaf first address")?
            .try_into()
            .expect("fixed address slice"),
    );
    let tail_dict_len = read_count(body, &mut offset, "direct leaf tail dictionary length")?;
    let mut tail_dictionary = Vec::with_capacity(tail_dict_len.min(body.len()));
    for _ in 0..tail_dict_len {
        tail_dictionary.push(read_value_tail(body, &mut offset, "v68 direct leaf")?);
    }

    let mut entries = Vec::with_capacity(entry_count.min(body.len()));
    let mut previous_key = Vec::new();
    for ordinal in 0..entry_count {
        let shared = read_count(body, &mut offset, "direct leaf shared key length")?;
        let suffix_len = read_count(body, &mut offset, "direct leaf key suffix length")?;
        if shared > previous_key.len() {
            return Err(error("direct leaf shares more bytes than its previous key"));
        }
        let suffix = take(body, &mut offset, suffix_len, "direct leaf key suffix")?;
        let mut key = Vec::with_capacity(shared.saturating_add(suffix_len));
        key.extend_from_slice(&previous_key[..shared]);
        key.extend_from_slice(suffix);
        if !entries.is_empty() && previous_key >= key {
            return Err(error("direct leaf keys are not strictly ordered"));
        }
        let tail_ref = read_count(body, &mut offset, "direct leaf tail dictionary ref")?;
        let tail = if tail_ref == 0 {
            read_value_tail(body, &mut offset, "v68 direct leaf")?
        } else {
            if tail_ref > tail_dict_len {
                return Err(error("direct leaf tail dictionary ref is out of bounds"));
            }
            tail_dictionary[tail_ref - 1]
        };
        let packed = first_packed
            .checked_add(
                u32::try_from(ordinal).map_err(|_| error("direct leaf ordinal exceeds u32"))?,
            )
            .ok_or_else(|| error("direct leaf address overflows u32"))?;
        let mut value = Vec::with_capacity(32 + tail.len());
        value.extend_from_slice(&commit_id[..12]);
        value.extend_from_slice(&packed.to_be_bytes());
        value.extend_from_slice(&commit_id);
        value.extend_from_slice(tail);
        previous_key = key.clone();
        entries.push(RawLeafEntry { key, value });
    }
    if offset != body.len() {
        return Err(error("direct leaf has trailing bytes"));
    }
    Ok(entries)
}

fn read_count(bytes: &[u8], offset: &mut usize, context: &str) -> Result<usize, LixError> {
    usize::try_from(read_varint(bytes, offset, context)?)
        .map_err(|_| error(format!("{context} does not fit usize")))
}

fn read_varint(bytes: &[u8], offset: &mut usize, context: &str) -> Result<u64, LixError> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes
            .get(*offset)
            .ok_or_else(|| error(format!("{context} varint is truncated")))?;
        *offset += 1;
        if shift >= 64 || (shift == 63 && byte > 1) {
            return Err(error(format!("{context} varint overflows u64")));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

fn take<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    len: usize,
    context: &str,
) -> Result<&'a [u8], LixError> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| error(format!("{context} length overflows")))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| error(format!("{context} is truncated")))?;
    *offset = end;
    Ok(value)
}

pub(in crate::migration) fn decode_key(bytes: &[u8]) -> Result<TrackedStateKey, LixError> {
    let mut offset = 0;
    let (schema_key, terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if terminator != KEY_PART_FINAL {
        return Err(error("key schema has an invalid terminator"));
    }
    let file_id = match bytes.get(offset).copied() {
        Some(FILE_ID_NONE) => {
            offset += 1;
            None
        }
        Some(FILE_ID_SOME) => {
            offset += 1;
            let (file_id, terminator) = read_key_string(bytes, &mut offset, "file id")?;
            if terminator != KEY_PART_FINAL {
                return Err(error("key file id has an invalid terminator"));
            }
            Some(file_id)
        }
        Some(other) => return Err(error(format!("key file id has unknown tag {other}"))),
        None => return Err(error("key file id tag is truncated")),
    };
    let row_pk = read_row_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(error("key has trailing bytes"));
    }
    Ok(TrackedStateKey {
        schema_key,
        file_id,
        row_pk,
    })
}

fn read_row_pk(bytes: &[u8], offset: &mut usize) -> Result<RowPk, LixError> {
    let version = bytes
        .get(*offset)
        .copied()
        .ok_or_else(|| error("row primary key is empty or truncated"))?;
    *offset += 1;
    if version != ROW_PK_CODEC_V1 {
        return Err(error(format!(
            "row primary key has unsupported codec version {version}"
        )));
    }
    let mut components = smallvec::SmallVec::new();
    loop {
        if *offset >= bytes.len() {
            return Err(error("row primary key is empty or truncated"));
        }
        let (component, terminator) = read_row_pk_part(bytes, offset)?;
        components.push(component);
        if terminator == KEY_PART_FINAL {
            break;
        }
        debug_assert_eq!(terminator, KEY_PART_MORE);
    }
    RowPk::from_components(components)
        .map_err(|decode_error| error(format!("row primary key is invalid: {decode_error}")))
}

fn read_row_pk_part(bytes: &[u8], offset: &mut usize) -> Result<(RowPkComponent, u8), LixError> {
    let tag = bytes
        .get(*offset)
        .copied()
        .ok_or_else(|| error("row primary-key part tag is truncated"))?;
    *offset += 1;
    match tag {
        ROW_PK_STRING => {
            let (value, terminator) = read_key_string(bytes, offset, "row primary-key part")?;
            Ok((RowPkComponent::String(value.into()), terminator))
        }
        ROW_PK_BYTES => {
            let (value, terminator) = read_key_bytes(bytes, offset, "row primary-key bytes")?;
            Ok((RowPkComponent::Bytes(value.into()), terminator))
        }
        ROW_PK_UUID => {
            let uuid_end = offset
                .checked_add(ROW_PK_UUID_BYTES)
                .ok_or_else(|| error("UUID row primary-key part overflows"))?;
            let uuid_bytes = bytes
                .get(*offset..uuid_end)
                .ok_or_else(|| error("UUID row primary-key part is truncated"))?
                .try_into()
                .expect("fixed UUID slice");
            let terminator = read_fixed_terminator(bytes, uuid_end, "UUID row primary-key")?;
            *offset = uuid_end + 1;
            Ok((RowPkComponent::Uuid(uuid_bytes), terminator))
        }
        ROW_PK_INTEGER => {
            let integer_end = offset
                .checked_add(ROW_PK_INTEGER_BYTES)
                .ok_or_else(|| error("integer row primary-key part overflows"))?;
            let ordered = u64::from_be_bytes(
                bytes
                    .get(*offset..integer_end)
                    .ok_or_else(|| error("integer row primary-key part is truncated"))?
                    .try_into()
                    .expect("fixed integer slice"),
            );
            let terminator = read_fixed_terminator(bytes, integer_end, "integer row primary-key")?;
            *offset = integer_end + 1;
            Ok((
                RowPkComponent::Integer(i64_from_ordered_integer(ordered)),
                terminator,
            ))
        }
        other => Err(error(format!(
            "row primary-key part has unknown tag {other}"
        ))),
    }
}

fn read_fixed_terminator(bytes: &[u8], at: usize, context: &str) -> Result<u8, LixError> {
    let terminator = bytes
        .get(at)
        .copied()
        .ok_or_else(|| error(format!("{context} ending is truncated")))?;
    if !is_key_part_terminator(terminator) {
        return Err(error(format!(
            "{context} has invalid terminator {terminator}"
        )));
    }
    Ok(terminator)
}

fn read_key_string(
    bytes: &[u8],
    offset: &mut usize,
    field: &str,
) -> Result<(String, u8), LixError> {
    let (value, terminator) = read_key_bytes_cow(bytes, offset, field)?;
    let value = match value {
        Cow::Borrowed(value) => std::str::from_utf8(value)
            .map(str::to_owned)
            .map_err(|_| error(format!("key {field} is not UTF-8")))?,
        Cow::Owned(value) => {
            String::from_utf8(value).map_err(|_| error(format!("key {field} is not UTF-8")))?
        }
    };
    Ok((value, terminator))
}

fn read_key_bytes(
    bytes: &[u8],
    offset: &mut usize,
    field: &str,
) -> Result<(Vec<u8>, u8), LixError> {
    read_key_bytes_cow(bytes, offset, field)
        .map(|(value, terminator)| (value.into_owned(), terminator))
}

fn read_key_bytes_cow<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    field: &str,
) -> Result<(Cow<'a, [u8]>, u8), LixError> {
    let part = scan_key_part(bytes, *offset).map_err(|scan_error| match scan_error {
        KeyPartError::Truncated => error(format!("key {field} is truncated")),
        KeyPartError::EscapeTruncated => error(format!("key {field} escape is truncated")),
        KeyPartError::UnknownEscape(other) => {
            error(format!("key {field} has unknown escape {other}"))
        }
    })?;
    *offset = part.end;
    let value = match part.value {
        ScannedKeyValue::Verbatim(range) => Cow::Borrowed(&bytes[range]),
        ScannedKeyValue::Unescaped(value) => Cow::Owned(value),
    };
    Ok((value, part.terminator))
}

fn decode_value(bytes: &[u8]) -> Result<TrackedStateIndexValue, LixError> {
    if !(VALUE_MIN_BYTES..=VALUE_MAX_BYTES).contains(&bytes.len()) {
        return Err(error(format!(
            "value has {} bytes; expected {VALUE_MIN_BYTES}..={VALUE_MAX_BYTES}",
            bytes.len()
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
    let (deleted, created_at, updated_at) = read_value_tail_fields(bytes, &mut offset, "value")?;
    if offset != bytes.len() {
        return Err(error("value has trailing bytes"));
    }
    Ok(TrackedStateIndexValue {
        change_id,
        commit_id,
        deleted,
        created_at: LixTimestamp::from_packed(created_at).map_err(|decode_error| {
            error(format!("value has invalid created_at: {decode_error}"))
        })?,
        updated_at: LixTimestamp::from_packed(updated_at).map_err(|decode_error| {
            error(format!("value has invalid updated_at: {decode_error}"))
        })?,
    })
}

fn read_value_tail_fields(
    bytes: &[u8],
    offset: &mut usize,
    context: &str,
) -> Result<(bool, u64, u64), LixError> {
    let tag = *bytes
        .get(*offset)
        .ok_or_else(|| error(format!("{context} state tail is truncated")))?;
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
            return Err(error(format!(
                "{context} has reserved state-tail tag {code}"
            )));
        }
    };
    let created_at = read_minimal_timestamp(bytes, offset, created_width, context)?;
    let updated_at = if equal {
        created_at
    } else {
        read_minimal_timestamp(bytes, offset, updated_width, context)?
    };
    if !equal && created_at == updated_at {
        return Err(error(format!(
            "{context} uses the distinct timestamp form for equal values"
        )));
    }
    Ok((deleted, created_at, updated_at))
}

fn read_minimal_timestamp(
    bytes: &[u8],
    offset: &mut usize,
    width: usize,
    context: &str,
) -> Result<u64, LixError> {
    let encoded = take(bytes, offset, width, &format!("{context} timestamp"))?;
    if width > 0 && encoded[width - 1] == 0 {
        return Err(error(format!(
            "{context} timestamp is not minimally encoded"
        )));
    }
    let mut storage = [0; 8];
    storage[..width].copy_from_slice(encoded);
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

fn split_segment(bytes: &[u8]) -> Result<(&[u8], &[u8]), LixError> {
    let body = bytes
        .strip_prefix(FORMAT_MAGIC)
        .ok_or_else(|| error("segment has an unsupported format"))?;
    let (leaf_len, body) = body
        .split_at_checked(4)
        .ok_or_else(|| error("segment has a truncated leaf length"))?;
    let leaf_len = u32::from_be_bytes(leaf_len.try_into().expect("fixed leaf length")) as usize;
    body.split_at_checked(leaf_len)
        .ok_or_else(|| error("segment has a truncated leaf"))
}

fn decode_payload_index(
    encoded_sidecar: &[u8],
    leaf_entries: usize,
) -> Result<PayloadIndex<'_>, LixError> {
    let (&encoding, encoded_sidecar) = encoded_sidecar
        .split_first()
        .ok_or_else(|| error("sidecar is missing its encoding"))?;
    let (uncompressed_len, encoded_sidecar) = encoded_sidecar
        .split_at_checked(4)
        .ok_or_else(|| error("sidecar has a truncated length"))?;
    let uncompressed_len =
        u32::from_be_bytes(uncompressed_len.try_into().expect("fixed sidecar length")) as usize;
    if uncompressed_len == 0 || uncompressed_len > MAX_SIDECAR_BYTES {
        return Err(error("sidecar has an invalid uncompressed length"));
    }
    let layout = match encoding {
        SIDECAR_RAW | SIDECAR_ZSTD => PayloadLayout::Indexed,
        SIDECAR_AUTHORED_INLINE_RAW | SIDECAR_AUTHORED_INLINE_ZSTD => PayloadLayout::AuthoredInline,
        _ => return Err(error("sidecar has an unsupported encoding")),
    };
    let sidecar = match encoding {
        SIDECAR_RAW | SIDECAR_AUTHORED_INLINE_RAW if encoded_sidecar.len() == uncompressed_len => {
            Cow::Borrowed(encoded_sidecar)
        }
        SIDECAR_RAW | SIDECAR_AUTHORED_INLINE_RAW => {
            return Err(error("raw sidecar length does not match its header"));
        }
        SIDECAR_ZSTD | SIDECAR_AUTHORED_INLINE_ZSTD => Cow::Owned(
            crate::compression::decompress_zstd(encoded_sidecar, uncompressed_len).map_err(
                |decode_error| {
                    error(format!(
                        "compressed sidecar failed to decode: {decode_error}"
                    ))
                },
            )?,
        ),
        _ => unreachable!("sidecar encoding was classified above"),
    };
    if sidecar.len() != uncompressed_len {
        return Err(error("compressed sidecar length does not match its header"));
    }

    let (entry_count, body) = sidecar
        .split_at_checked(4)
        .ok_or_else(|| error("payload index has a truncated entry count"))?;
    let entry_count =
        u32::from_be_bytes(entry_count.try_into().expect("fixed entry count")) as usize;
    if entry_count != leaf_entries {
        return Err(error("payload count does not match its identity count"));
    }
    let offset_count = entry_count
        .checked_add(1)
        .ok_or_else(|| error("payload directory overflows"))?;
    let directory_len = offset_count
        .checked_mul(PAYLOAD_OFFSET_BYTES)
        .ok_or_else(|| error("payload directory overflows"))?;
    if body.len() < directory_len {
        return Err(error("payload directory is truncated"));
    }
    let payload_start = 4 + directory_len;
    let payload_bytes_len = sidecar.len() - payload_start;
    let index = PayloadIndex {
        sidecar,
        offsets_start: 4,
        payload_start,
        entry_count,
        layout,
    };
    if index.offset(0)? != 0 {
        return Err(error("payload directory does not start at zero"));
    }
    let mut previous = 0;
    for offset_index in 1..=entry_count {
        let offset = index.offset(offset_index)?;
        if offset < previous {
            return Err(error("payload offsets are not ordered"));
        }
        if offset > payload_bytes_len {
            return Err(error("payload offset is out of bounds"));
        }
        previous = offset;
    }
    if previous != payload_bytes_len {
        return Err(error("payload directory does not cover its sidecar"));
    }
    Ok(index)
}

impl PayloadIndex<'_> {
    fn decode(&self, index: usize) -> Result<CommitDeltaPayloadDescriptor, LixError> {
        let payload = self.payload_range(index)?;
        if payload.is_empty() {
            return Err(error("member is missing its authoritative payload"));
        }
        if matches!(self.layout, PayloadLayout::AuthoredInline) {
            let json =
                std::str::from_utf8(payload).map_err(|_| error("inline payload is not UTF-8"))?;
            return Ok(CommitDeltaPayloadDescriptor::Authored {
                snapshot: JsonSlot::Inline(json.into()),
                metadata: JsonSlot::None,
                typed_payload: None,
                origin_key: None,
                base_coordinate: None,
            });
        }

        let (&tag, body) = payload
            .split_first()
            .ok_or_else(|| error("member has an empty payload record"))?;
        match tag {
            PAYLOAD_AUTHORED => {
                let payload: AuthoredPayload = crate::storage_codec::decode(
                    "v68 indexed authored commit_delta payload",
                    body,
                )?;
                Ok(CommitDeltaPayloadDescriptor::Authored {
                    snapshot: payload.snapshot,
                    metadata: payload.metadata,
                    typed_payload: None,
                    origin_key: payload.origin_key,
                    base_coordinate: payload.base_coordinate,
                })
            }
            PAYLOAD_SELECTED_REF => Ok(CommitDeltaPayloadDescriptor::SelectedRef {
                base_coordinate: decode_optional_base_coordinate(body)?,
            }),
            PAYLOAD_SELECTED_TOMBSTONE => Ok(CommitDeltaPayloadDescriptor::SelectedTombstone {
                base_coordinate: decode_optional_base_coordinate(body)?,
            }),
            _ => Err(error("member has an invalid payload tag")),
        }
    }

    fn payload_range(&self, index: usize) -> Result<&[u8], LixError> {
        if index >= self.entry_count {
            return Err(error("payload index is out of bounds"));
        }
        let start = self.offset(index)?;
        let end = self.offset(index + 1)?;
        self.sidecar
            .get(self.payload_start + start..self.payload_start + end)
            .ok_or_else(|| error("payload range is out of bounds"))
    }

    fn offset(&self, index: usize) -> Result<usize, LixError> {
        let byte_start = index
            .checked_mul(PAYLOAD_OFFSET_BYTES)
            .and_then(|offset| self.offsets_start.checked_add(offset))
            .ok_or_else(|| error("payload directory overflows"))?;
        let bytes = self
            .sidecar
            .get(byte_start..byte_start + PAYLOAD_OFFSET_BYTES)
            .ok_or_else(|| error("payload directory is truncated"))?;
        Ok(u32::from_be_bytes(bytes.try_into().expect("fixed payload offset")) as usize)
    }
}

fn decode_optional_base_coordinate(
    bytes: &[u8],
) -> Result<Option<TrackedStateBaseCoordinate>, LixError> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        crate::storage_codec::decode("v68 commit_delta base coordinate", bytes).map(Some)
    }
}

fn error(message: impl Into<String>) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("v68 commit_delta {}", message.into()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::json_store::{JsonRef, JsonSlotRef};
    use crate::row_pk::RowPk;
    use crate::tracked_state::{TrackedStateKeyRef, encode_key_ref};

    struct EncodedLeafEntry {
        key: Vec<u8>,
        value: Vec<u8>,
    }

    #[derive(musli::Encode)]
    #[musli(packed)]
    struct FrozenAuthoredPayloadRef<'a> {
        #[musli(with = crate::json_store::json_slot_storage_ref)]
        snapshot: JsonSlotRef<'a>,
        #[musli(with = crate::json_store::json_slot_storage_ref)]
        metadata: JsonSlotRef<'a>,
        #[musli(with = crate::storage_codec::option)]
        origin_key: Option<&'a str>,
        #[musli(with = crate::storage_codec::option)]
        base_coordinate: Option<TrackedStateBaseCoordinate>,
    }

    fn key(label: &str) -> TrackedStateKey {
        TrackedStateKey {
            schema_key: "plugin.item".to_string(),
            file_id: None,
            row_pk: RowPk::from_parts(vec![label.to_string()]).expect("fixture row pk"),
        }
    }

    fn entries(commit_id: CommitId, count: usize) -> Vec<EncodedLeafEntry> {
        (0..count)
            .map(|index| {
                let key = key(&format!("row-{index:04}"));
                let value = TrackedStateIndexValue {
                    change_id: ChangeId::for_test_label(&format!("change-{index}")),
                    commit_id,
                    deleted: index == 2,
                    created_at: LixTimestamp::expect_parse(
                        "fixture timestamp",
                        "2026-08-18T12:34:56.789Z",
                    ),
                    updated_at: LixTimestamp::expect_parse(
                        "fixture timestamp",
                        "2026-08-18T12:35:56.789Z",
                    ),
                };
                EncodedLeafEntry {
                    key: encode_key_ref(TrackedStateKeyRef {
                        schema_key: &key.schema_key,
                        file_id: key.file_id.as_deref(),
                        row_pk: &key.row_pk,
                    }),
                    value: encode_fixture_value(&value),
                }
            })
            .collect()
    }

    fn encode_fixture_value(value: &TrackedStateIndexValue) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(value.change_id.as_uuid().as_bytes());
        bytes.extend_from_slice(value.commit_id.as_uuid().as_bytes());
        encode_fixture_tail(
            &mut bytes,
            value.deleted,
            value.created_at.packed(),
            value.updated_at.packed(),
        );
        bytes
    }

    fn encode_fixture_tail(out: &mut Vec<u8>, deleted: bool, created: u64, updated: u64) {
        fn width(value: u64) -> usize {
            usize::try_from((u64::BITS - value.leading_zeros()).div_ceil(8))
                .expect("timestamp width fits usize")
        }
        let created_width = width(created);
        let updated_width = width(updated);
        let code = if created == updated {
            created_width as u8
        } else {
            VALUE_TAIL_DISTINCT_MIN
                + created_width as u8 * VALUE_TIMESTAMP_WIDTH_COUNT
                + updated_width as u8
        };
        out.push(code | u8::from(deleted) * VALUE_TAIL_DELETED);
        out.extend_from_slice(&created.to_le_bytes()[..created_width]);
        if created != updated {
            out.extend_from_slice(&updated.to_le_bytes()[..updated_width]);
        }
    }

    fn write_fixture_varint(out: &mut Vec<u8>, mut value: u64) {
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

    fn encode_leaf_node(entries: &[EncodedLeafEntry]) -> Vec<u8> {
        let mut leaf = vec![NODE_KIND_LEAF_V4];
        write_fixture_varint(&mut leaf, entries.len() as u64);
        write_fixture_varint(&mut leaf, 0); // no commit dictionary
        write_fixture_varint(&mut leaf, 0); // no tail dictionary
        let mut previous_key = &[][..];
        for entry in entries {
            let shared = previous_key
                .iter()
                .zip(&entry.key)
                .take_while(|(left, right)| left == right)
                .count();
            write_fixture_varint(&mut leaf, shared as u64);
            write_fixture_varint(&mut leaf, (entry.key.len() - shared) as u64);
            leaf.extend_from_slice(&entry.key[shared..]);
            leaf.extend_from_slice(&entry.value[..16]);
            write_fixture_varint(&mut leaf, 0);
            leaf.extend_from_slice(&entry.value[16..32]);
            write_fixture_varint(&mut leaf, 0);
            leaf.extend_from_slice(&entry.value[32..]);
            previous_key = &entry.key;
        }
        leaf
    }

    fn indexed_sidecar() -> Vec<u8> {
        let coordinate = TrackedStateBaseCoordinate {
            base_commit_id: CommitId::for_test_label("base"),
            group_index: 7,
            row_index: 11,
        };
        let snapshot_ref = JsonRef::for_content(b"snapshot");
        let mut authored = vec![PAYLOAD_AUTHORED];
        crate::storage_codec::append(
            "frozen authored payload",
            &mut authored,
            &FrozenAuthoredPayloadRef {
                snapshot: JsonSlotRef::Ref(&snapshot_ref),
                metadata: JsonSlotRef::Inline(r#"{"source":"v68"}"#),
                origin_key: Some("legacy-origin"),
                base_coordinate: Some(coordinate),
            },
        )
        .expect("payload encodes");
        let mut selected_ref = vec![PAYLOAD_SELECTED_REF];
        crate::storage_codec::append("frozen selected coordinate", &mut selected_ref, &coordinate)
            .expect("coordinate encodes");
        let selected_tombstone = vec![PAYLOAD_SELECTED_TOMBSTONE];
        indexed_body(&[authored, selected_ref, selected_tombstone])
    }

    fn indexed_body(payloads: &[Vec<u8>]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&(payloads.len() as u32).to_be_bytes());
        let mut offset = 0u32;
        for payload in payloads {
            body.extend_from_slice(&offset.to_be_bytes());
            offset += payload.len() as u32;
        }
        body.extend_from_slice(&offset.to_be_bytes());
        for payload in payloads {
            body.extend_from_slice(payload);
        }
        body
    }

    fn inline_sidecar(payloads: &[&str]) -> Vec<u8> {
        let payloads = payloads
            .iter()
            .map(|payload| payload.as_bytes().to_vec())
            .collect::<Vec<_>>();
        indexed_body(&payloads)
    }

    fn segment(leaf: &[u8], sidecar: &[u8], encoding: u8) -> Vec<u8> {
        let stored = match encoding {
            SIDECAR_ZSTD | SIDECAR_AUTHORED_INLINE_ZSTD => {
                crate::compression::compress_zstd_level_1(sidecar).expect("sidecar compresses")
            }
            _ => sidecar.to_vec(),
        };
        let mut bytes = Vec::new();
        bytes.extend_from_slice(FORMAT_MAGIC);
        bytes.extend_from_slice(&(leaf.len() as u32).to_be_bytes());
        bytes.extend_from_slice(leaf);
        bytes.push(encoding);
        bytes.extend_from_slice(&(sidecar.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&stored);
        bytes
    }

    fn bounds(entries: &[EncodedLeafEntry]) -> CommitDeltaSegmentBounds {
        CommitDeltaSegmentBounds {
            first_key: entries.first().expect("first entry").key.clone(),
            last_key: entries.last().expect("last entry").key.clone(),
        }
    }

    #[test]
    fn decodes_indexed_raw_and_zstd_payloads() {
        let commit_id = CommitId::for_test_label("v68-indexed");
        let entries = entries(commit_id, 3);
        let leaf = encode_leaf_node(&entries);
        let sidecar = indexed_sidecar();

        for encoding in [SIDECAR_RAW, SIDECAR_ZSTD] {
            let decoded = decode_commit_delta_segment(
                &segment(&leaf, &sidecar, encoding),
                Some(&bounds(&entries)),
                commit_id,
            )
            .expect("indexed segment decodes");
            assert_eq!(decoded.len(), 3);
            let CommitDeltaPayloadDescriptor::Authored {
                snapshot,
                metadata,
                typed_payload,
                origin_key,
                base_coordinate,
            } = &decoded[0].payload
            else {
                panic!("expected authored payload");
            };
            assert_eq!(*snapshot, JsonSlot::Ref(JsonRef::for_content(b"snapshot")));
            assert_eq!(*metadata, JsonSlot::from_json(r#"{"source":"v68"}"#));
            assert_eq!(typed_payload, &None);
            assert_eq!(origin_key.as_deref(), Some("legacy-origin"));
            assert!(base_coordinate.is_some());
            assert!(matches!(
                decoded[1].payload,
                CommitDeltaPayloadDescriptor::SelectedRef {
                    base_coordinate: Some(_)
                }
            ));
            assert!(matches!(
                decoded[2].payload,
                CommitDeltaPayloadDescriptor::SelectedTombstone {
                    base_coordinate: None
                }
            ));
        }
    }

    #[test]
    fn decodes_authored_inline_raw_and_zstd_payloads() {
        let commit_id = CommitId::for_test_label("v68-inline");
        let entries = entries(commit_id, 2);
        let leaf = encode_leaf_node(&entries);
        let sidecar = inline_sidecar(&[r#"{"id":1}"#, r#"{"id":2}"#]);

        for encoding in [SIDECAR_AUTHORED_INLINE_RAW, SIDECAR_AUTHORED_INLINE_ZSTD] {
            let decoded =
                decode_commit_delta_segment(&segment(&leaf, &sidecar, encoding), None, commit_id)
                    .expect("inline segment decodes");
            assert!(decoded.iter().all(|member| matches!(
                member.payload,
                CommitDeltaPayloadDescriptor::Authored {
                    metadata: JsonSlot::None,
                    typed_payload: None,
                    origin_key: None,
                    base_coordinate: None,
                    ..
                }
            )));
        }
    }

    #[test]
    fn rejects_wrong_bounds_and_commit_id() {
        let commit_id = CommitId::for_test_label("v68-bounds");
        let entries = entries(commit_id, 1);
        let leaf = encode_leaf_node(&entries);
        let sidecar = inline_sidecar(&[r#"{"id":1}"#]);
        let bytes = segment(&leaf, &sidecar, SIDECAR_AUTHORED_INLINE_RAW);
        let wrong_bounds = CommitDeltaSegmentBounds {
            first_key: b"wrong".to_vec(),
            last_key: b"wrong".to_vec(),
        };

        assert!(decode_commit_delta_segment(&bytes, Some(&wrong_bounds), commit_id).is_err());
        assert!(
            decode_commit_delta_segment(&bytes, None, CommitId::for_test_label("other-commit"))
                .is_err()
        );
    }

    #[test]
    fn rejects_corrupt_headers_and_payload_directories() {
        let commit_id = CommitId::for_test_label("v68-corruption");
        let entries = entries(commit_id, 2);
        let leaf = encode_leaf_node(&entries);
        let sidecar = inline_sidecar(&[r#"{"id":1}"#, r#"{"id":2}"#]);

        let mut bad_magic = segment(&leaf, &sidecar, SIDECAR_AUTHORED_INLINE_RAW);
        bad_magic[0] = b'X';
        assert!(decode_commit_delta_segment(&bad_magic, None, commit_id).is_err());

        let mut bad_raw_len = segment(&leaf, &sidecar, SIDECAR_AUTHORED_INLINE_RAW);
        let length_at = FORMAT_MAGIC.len() + 4 + leaf.len() + 1;
        bad_raw_len[length_at..length_at + 4]
            .copy_from_slice(&((sidecar.len() as u32) + 1).to_be_bytes());
        assert!(decode_commit_delta_segment(&bad_raw_len, None, commit_id).is_err());

        let mut unordered = sidecar.clone();
        unordered[8..12].copy_from_slice(&9u32.to_be_bytes());
        unordered[12..16].copy_from_slice(&1u32.to_be_bytes());
        assert!(
            decode_commit_delta_segment(
                &segment(&leaf, &unordered, SIDECAR_AUTHORED_INLINE_RAW),
                None,
                commit_id
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_empty_and_oversized_leaves() {
        let commit_id = CommitId::for_test_label("v68-row-bounds");
        let empty_leaf = encode_leaf_node(&[]);
        let empty_sidecar = indexed_body(&[]);
        assert!(
            decode_commit_delta_segment(
                &segment(&empty_leaf, &empty_sidecar, SIDECAR_RAW),
                None,
                commit_id
            )
            .is_err()
        );

        let entries = entries(commit_id, SEGMENT_MAX_ROWS + 1);
        let leaf = encode_leaf_node(&entries);
        let payloads = (0..entries.len())
            .map(|_| vec![PAYLOAD_SELECTED_REF])
            .collect::<Vec<_>>();
        let sidecar = indexed_body(&payloads);
        assert!(
            decode_commit_delta_segment(&segment(&leaf, &sidecar, SIDECAR_RAW), None, commit_id)
                .is_err()
        );
    }
}
