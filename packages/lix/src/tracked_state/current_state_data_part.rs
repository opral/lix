//! Content-addressed immutable post-image parts for sparse current-state rewrites.
//!
//! Unlike complete-replacement parts, these rows retain exact per-row provenance
//! and lifecycle timestamps because a sparse post-image may mix many authoring
//! commits. Mutation inventory remains the historical authority; this payload is
//! a rebuildable serving accelerator.

use std::borrow::Cow;
use std::collections::BTreeSet;

use bytes::Bytes;

use crate::changelog::{
    AuthenticatedNativeRow, ChangePayload, CommitId, NativeRowField, NativeScalarCell,
};
use crate::json_store::JsonSlot;
use crate::storage_adapter::{StorageSpace, StorageSpaceId, ValueSemantics};
use crate::tracked_state::codec::decode_value;
use crate::tracked_state::types::TrackedStateIndexValue;
use crate::{LixError, storage_codec};

pub(crate) const CURRENT_STATE_DATA_PART_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_002f),
    "tracked_state.current_state_data_part.v2",
    ValueSemantics::Immutable,
);
pub(crate) const CURRENT_STATE_DATA_PART_REFS_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0030),
    "tracked_state.current_state_data_part_refs.v1",
    ValueSemantics::Immutable,
);

pub(crate) const CURRENT_STATE_DATA_PART_MAX_ROWS: usize = 512;
pub(crate) const CURRENT_STATE_DATA_PART_TARGET_BYTES: usize = 64 * 1024;
const CURRENT_STATE_DATA_PART_MAX_BYTES: usize = 4 * 1024 * 1024;
const CURRENT_STATE_DATA_PART_MAX_DECODED_BYTES: usize = 16 * 1024 * 1024;
const RAW_MAGIC: &[u8; 7] = b"LXCSP02";
const ZSTD_MAGIC: &[u8; 7] = b"LXCSPZ2";
const DIGEST_CONTEXT: &str = "lix native current-state data part v2";
const REFS_DIGEST_CONTEXT: &str = "lix native current-state data part refs v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CurrentStateDataRow {
    pub(crate) encoded_key: Vec<u8>,
    pub(crate) value: TrackedStateIndexValue,
    pub(crate) snapshot: ChangePayload,
    pub(crate) metadata: JsonSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredCurrentStateDataRow {
    #[musli(bytes)]
    encoded_key: Vec<u8>,
    #[musli(bytes)]
    encoded_value: Vec<u8>,
    payload_kind: u8,
    #[musli(with = crate::json_store::json_slot_storage)]
    snapshot: JsonSlot,
    native: Vec<StoredNativeRow>,
    #[musli(with = crate::json_store::json_slot_storage)]
    metadata: JsonSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredNativeRow {
    owner_commit_id: [u8; 16],
    row_group_set_id: [u8; 16],
    manifest_digest: [u8; 32],
    state_key_digest: [u8; 32],
    layout_fingerprint: String,
    semantic_payload_digest: [u8; 32],
    fields: Vec<StoredNativeRowField>,
    cells: Vec<StoredNativeScalarCell>,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
enum StoredNativeRowField {
    PrimaryKey { name: String, component_index: u32 },
    Cell { name: String, cell_index: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
enum StoredNativeScalarCell {
    Null,
    Text(String),
    Uuid([u8; 16]),
    Int8(i64),
    Float8Bits(u64),
    Boolean(bool),
    Jsonb(Vec<u8>),
    Timestamptz(crate::common::LixTimestamp),
}

impl From<&NativeScalarCell> for StoredNativeScalarCell {
    fn from(cell: &NativeScalarCell) -> Self {
        match cell {
            NativeScalarCell::Null => Self::Null,
            NativeScalarCell::Text(value) => Self::Text(value.clone()),
            NativeScalarCell::Uuid(value) => Self::Uuid(*value),
            NativeScalarCell::Int8(value) => Self::Int8(*value),
            NativeScalarCell::Float8Bits(value) => Self::Float8Bits(*value),
            NativeScalarCell::Boolean(value) => Self::Boolean(*value),
            NativeScalarCell::Jsonb(value) => Self::Jsonb(value.clone()),
            NativeScalarCell::Timestamptz(value) => Self::Timestamptz(*value),
        }
    }
}

impl From<StoredNativeScalarCell> for NativeScalarCell {
    fn from(cell: StoredNativeScalarCell) -> Self {
        match cell {
            StoredNativeScalarCell::Null => Self::Null,
            StoredNativeScalarCell::Text(value) => Self::Text(value),
            StoredNativeScalarCell::Uuid(value) => Self::Uuid(value),
            StoredNativeScalarCell::Int8(value) => Self::Int8(value),
            StoredNativeScalarCell::Float8Bits(value) => Self::Float8Bits(value),
            StoredNativeScalarCell::Boolean(value) => Self::Boolean(value),
            StoredNativeScalarCell::Jsonb(value) => Self::Jsonb(value),
            StoredNativeScalarCell::Timestamptz(value) => Self::Timestamptz(value),
        }
    }
}

fn stored_payload(payload: &ChangePayload) -> (u8, JsonSlot, Vec<StoredNativeRow>) {
    match payload {
        ChangePayload::Tombstone => (0, JsonSlot::None, Vec::new()),
        ChangePayload::Json(slot) => (1, slot.clone(), Vec::new()),
        ChangePayload::Native(row) => (
            2,
            JsonSlot::None,
            vec![StoredNativeRow {
                owner_commit_id: *row.owner_commit_id.as_uuid().as_bytes(),
                row_group_set_id: row.row_group_set_id,
                manifest_digest: row.manifest_digest,
                state_key_digest: row.state_key_digest,
                layout_fingerprint: row.layout_fingerprint.clone(),
                semantic_payload_digest: row.semantic_payload_digest,
                fields: row
                    .fields
                    .iter()
                    .map(|field| match field {
                        NativeRowField::PrimaryKey {
                            name,
                            component_index,
                        } => StoredNativeRowField::PrimaryKey {
                            name: name.clone(),
                            component_index: *component_index,
                        },
                        NativeRowField::Cell { name, cell_index } => StoredNativeRowField::Cell {
                            name: name.clone(),
                            cell_index: *cell_index,
                        },
                    })
                    .collect(),
                cells: row.cells.iter().map(Into::into).collect(),
            }],
        ),
    }
}

fn decoded_payload(
    kind: u8,
    snapshot: JsonSlot,
    mut native: Vec<StoredNativeRow>,
) -> Result<ChangePayload, LixError> {
    match (kind, snapshot, native.as_slice()) {
        (0, JsonSlot::None, []) => Ok(ChangePayload::Tombstone),
        (1, JsonSlot::Inline(value), []) => {
            Ok(ChangePayload::Json(JsonSlot::Inline(value)))
        }
        (1, JsonSlot::Ref(value), []) => Ok(ChangePayload::Json(JsonSlot::Ref(value))),
        (2, JsonSlot::None, [_]) => {
            let row = native.pop().expect("one native payload was validated");
            Ok(ChangePayload::Native(AuthenticatedNativeRow {
            owner_commit_id: CommitId::new(uuid::Uuid::from_bytes(row.owner_commit_id)),
            row_group_set_id: row.row_group_set_id,
            manifest_digest: row.manifest_digest,
            state_key_digest: row.state_key_digest,
            layout_fingerprint: row.layout_fingerprint,
            semantic_payload_digest: row.semantic_payload_digest,
            fields: row
                .fields
                .into_iter()
                .map(|field| match field {
                    StoredNativeRowField::PrimaryKey {
                        name,
                        component_index,
                    } => NativeRowField::PrimaryKey {
                        name,
                        component_index,
                    },
                    StoredNativeRowField::Cell { name, cell_index } => {
                        NativeRowField::Cell { name, cell_index }
                    }
                })
                .collect(),
            cells: row.cells.into_iter().map(Into::into).collect(),
            }))
        }
        _ => Err(part_error("payload authority is malformed or duplicated")),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EncodedCurrentStateDataPart {
    pub(crate) digest: [u8; 32],
    pub(crate) bytes: Bytes,
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
    pub(crate) row_count: u16,
    pub(crate) refs_digest: [u8; 32],
    pub(crate) refs_bytes: Bytes,
}

pub(crate) fn encode_current_state_data_part(
    rows: &[CurrentStateDataRow],
    compressor: &mut Option<crate::compression::ZstdLevel1Compressor>,
) -> Result<EncodedCurrentStateDataPart, LixError> {
    validate_rows(rows)?;
    let stored = rows
        .iter()
        .map(|row| {
            let (payload_kind, snapshot, native) = stored_payload(&row.snapshot);
            StoredCurrentStateDataRow {
                encoded_key: row.encoded_key.clone(),
                encoded_value: super::codec::encode_value_ref(
                    super::types::TrackedStateIndexValueRef {
                        change_id: row.value.change_id,
                        commit_id: row.value.commit_id,
                        deleted: row.value.deleted,
                        created_at: row.value.created_at,
                        updated_at: row.value.updated_at,
                    },
                ),
                payload_kind,
                snapshot,
                native,
                metadata: row.metadata.clone(),
            }
        })
        .collect::<Vec<_>>();
    let payload = storage_codec::encode("native current-state data part", &stored)?;
    if payload.len() > CURRENT_STATE_DATA_PART_MAX_DECODED_BYTES {
        return Err(part_error("decoded payload exceeds its bound"));
    }
    let mut encoded = Vec::with_capacity(RAW_MAGIC.len() + payload.len());
    encoded.extend_from_slice(RAW_MAGIC);
    encoded.extend_from_slice(&payload);
    if payload.len() >= 512 {
        if compressor.is_none() {
            *compressor = Some(
                crate::compression::ZstdLevel1Compressor::new()
                    .map_err(|error| part_error(format!("compressor init failed: {error}")))?,
            );
        }
        let compressed = compressor
            .as_mut()
            .expect("compressor was initialized")
            .compress(&payload)
            .map_err(|error| part_error(format!("compression failed: {error}")))?;
        let mut physical = Vec::with_capacity(ZSTD_MAGIC.len() + 4 + compressed.len());
        physical.extend_from_slice(ZSTD_MAGIC);
        physical.extend_from_slice(
            &u32::try_from(payload.len())
                .map_err(|_| part_error("decoded length exceeds u32"))?
                .to_be_bytes(),
        );
        physical.extend_from_slice(&compressed);
        if physical.len() < encoded.len() {
            encoded = physical;
        }
    }
    if encoded.len() > CURRENT_STATE_DATA_PART_MAX_BYTES {
        return Err(part_error("physical payload exceeds its bound"));
    }
    let digest = digest(&encoded);
    let mut refs = rows
        .iter()
        .flat_map(|row| {
            let snapshot = match &row.snapshot {
                ChangePayload::Json(slot) => Some(slot),
                ChangePayload::Tombstone | ChangePayload::Native(_) => None,
            };
            [snapshot, Some(&row.metadata)]
        })
        .flatten()
        .filter_map(|slot| match slot {
            JsonSlot::Ref(reference) => Some(*reference.as_hash_array()),
            JsonSlot::None | JsonSlot::Inline(_) => None,
        })
        .collect::<Vec<_>>();
    refs.sort_unstable();
    refs.dedup();
    let refs_bytes = storage_codec::encode("native current-state data part refs", &refs)?;
    let refs_digest = refs_digest(&refs_bytes);
    Ok(EncodedCurrentStateDataPart {
        digest,
        bytes: Bytes::from(encoded),
        first_key: rows.first().expect("validated rows").encoded_key.clone(),
        last_key: rows.last().expect("validated rows").encoded_key.clone(),
        row_count: u16::try_from(rows.len()).expect("native part row count is bounded"),
        refs_digest,
        refs_bytes: Bytes::from(refs_bytes),
    })
}

pub(crate) fn decode_current_state_data_part_refs(
    expected_digest: &[u8; 32],
    encoded: &[u8],
) -> Result<Vec<[u8; 32]>, LixError> {
    if &refs_digest(encoded) != expected_digest {
        return Err(part_error("payload-ref summary digest is invalid"));
    }
    let refs: Vec<[u8; 32]> =
        storage_codec::decode("native current-state data part refs", encoded)?;
    if refs.len() > CURRENT_STATE_DATA_PART_MAX_ROWS * 2
        || refs.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(part_error("payload-ref summary is oversized or unordered"));
    }
    Ok(refs)
}

pub(crate) fn encode_bounded_current_state_data_parts(
    rows: &[CurrentStateDataRow],
) -> Result<Vec<EncodedCurrentStateDataPart>, LixError> {
    let mut compressor = None;
    let mut encoded = Vec::new();
    let mut offset = 0usize;
    while offset < rows.len() {
        let mut count = (rows.len() - offset).min(CURRENT_STATE_DATA_PART_MAX_ROWS);
        let part = loop {
            let part =
                encode_current_state_data_part(&rows[offset..offset + count], &mut compressor)?;
            if part.bytes.len() <= CURRENT_STATE_DATA_PART_TARGET_BYTES || count == 1 {
                break part;
            }
            count = count.div_ceil(2);
        };
        encoded.push(part);
        offset += count;
    }
    Ok(encoded)
}

pub(crate) fn decode_current_state_data_part(
    expected_digest: &[u8; 32],
    encoded: &[u8],
) -> Result<Vec<CurrentStateDataRow>, LixError> {
    if encoded.len() > CURRENT_STATE_DATA_PART_MAX_BYTES || &digest(encoded) != expected_digest {
        return Err(part_error("content digest or physical bound is invalid"));
    }
    let payload: Cow<'_, [u8]> = if let Some(payload) = encoded.strip_prefix(RAW_MAGIC) {
        Cow::Borrowed(payload)
    } else if let Some(body) = encoded.strip_prefix(ZSTD_MAGIC) {
        let (decoded_len, compressed) = body
            .split_at_checked(4)
            .ok_or_else(|| part_error("compressed payload is truncated"))?;
        let decoded_len = usize::try_from(u32::from_be_bytes(
            decoded_len.try_into().expect("fixed decoded length"),
        ))
        .expect("u32 fits usize");
        if decoded_len > CURRENT_STATE_DATA_PART_MAX_DECODED_BYTES {
            return Err(part_error("compressed payload exceeds its decoded bound"));
        }
        Cow::Owned(
            crate::compression::decompress_zstd(compressed, decoded_len)
                .map_err(|error| part_error(format!("decompression failed: {error}")))?,
        )
    } else {
        return Err(part_error("unsupported format; recreate the repository"));
    };
    let stored: Vec<StoredCurrentStateDataRow> =
        storage_codec::decode("native current-state data part", &payload)?;
    let rows = stored
        .into_iter()
        .map(|row| {
            Ok(CurrentStateDataRow {
                encoded_key: row.encoded_key,
                value: decode_value(&row.encoded_value)?,
                snapshot: decoded_payload(row.payload_kind, row.snapshot, row.native)?,
                metadata: row.metadata,
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    validate_rows(&rows)?;
    Ok(rows)
}

pub(crate) fn decode_current_state_data_part_commit_ids(
    expected_digest: &[u8; 32],
    encoded: &[u8],
) -> Result<BTreeSet<CommitId>, LixError> {
    Ok(decode_current_state_data_part(expected_digest, encoded)?
        .into_iter()
        .map(|row| row.value.commit_id)
        .collect())
}

fn validate_rows(rows: &[CurrentStateDataRow]) -> Result<(), LixError> {
    if rows.is_empty()
        || rows.len() > CURRENT_STATE_DATA_PART_MAX_ROWS
        || rows
            .iter()
            .any(|row| {
                row.encoded_key.is_empty()
                    || row.value.deleted
                    || row.snapshot.is_none()
                    || row.snapshot.native_row().is_some_and(|native| {
                        native.owner_commit_id != row.value.commit_id
                            || native.state_key_digest
                                != *blake3::hash(&row.encoded_key).as_bytes()
                    })
            })
        || rows
            .windows(2)
            .any(|pair| pair[0].encoded_key >= pair[1].encoded_key)
    {
        return Err(part_error(
            "rows are empty, deleted, oversized, or unordered",
        ));
    }
    Ok(())
}

fn digest(encoded: &[u8]) -> [u8; 32] {
    *blake3::Hasher::new_derive_key(DIGEST_CONTEXT)
        .update(encoded)
        .finalize()
        .as_bytes()
}

fn refs_digest(encoded: &[u8]) -> [u8; 32] {
    *blake3::Hasher::new_derive_key(REFS_DIGEST_CONTEXT)
        .update(encoded)
        .finalize()
        .as_bytes()
}

fn part_error(message: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state native current-state data part {message}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;

    fn row(index: usize) -> CurrentStateDataRow {
        CurrentStateDataRow {
            encoded_key: format!("key-{index:04}").into_bytes(),
            value: TrackedStateIndexValue {
                change_id: ChangeId::for_test_label(&format!("native-change-{index}")),
                commit_id: CommitId::for_test_label(&format!("native-commit-{index}")),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(index as i64),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(index as i64 + 1),
            },
            snapshot: ChangePayload::Json(JsonSlot::Inline(
                format!(r#"{{"index":{index}}}"#).into(),
            )),
            metadata: JsonSlot::None,
        }
    }

    #[test]
    fn native_parts_round_trip_exact_provenance_and_reject_corruption() {
        let mut rows = (0..513).map(row).collect::<Vec<_>>();
        let referenced = crate::json_store::JsonRef::for_content(b"native referenced metadata");
        rows[0].metadata = JsonSlot::Ref(referenced);
        let parts = encode_bounded_current_state_data_parts(&rows).expect("parts should encode");
        assert_eq!(parts.len(), 2, "row bound must split the post-image");
        let decoded = parts
            .iter()
            .flat_map(|part| {
                decode_current_state_data_part(&part.digest, &part.bytes)
                    .expect("part should decode")
            })
            .collect::<Vec<_>>();
        assert_eq!(decoded, rows);
        assert_eq!(
            decode_current_state_data_part_refs(&parts[0].refs_digest, &parts[0].refs_bytes)
                .expect("payload refs should decode"),
            vec![*referenced.as_hash_array()]
        );

        let mut corrupt = parts[0].bytes.to_vec();
        *corrupt.last_mut().expect("encoded part is non-empty") ^= 1;
        assert!(decode_current_state_data_part(&parts[0].digest, &corrupt).is_err());
    }

    #[test]
    fn native_parts_reject_tombstones_and_unordered_rows() {
        let mut rows = vec![row(1), row(0)];
        assert!(encode_bounded_current_state_data_parts(&rows).is_err());
        rows.sort_by(|left, right| left.encoded_key.cmp(&right.encoded_key));
        rows[0].value.deleted = true;
        assert!(encode_bounded_current_state_data_parts(&rows).is_err());
    }

    #[test]
    fn typed_payload_is_key_and_owner_bound_and_round_trips_without_json() {
        let mut row = row(0);
        let owner_commit_id = row.value.commit_id;
        row.snapshot = ChangePayload::Native(AuthenticatedNativeRow {
            owner_commit_id,
            row_group_set_id: [1; 16],
            manifest_digest: [2; 32],
            state_key_digest: *blake3::hash(&row.encoded_key).as_bytes(),
            layout_fingerprint: "schema-v1:test-layout".to_owned(),
            semantic_payload_digest: [3; 32],
            fields: vec![NativeRowField::Cell {
                name: "value".to_owned(),
                cell_index: 0,
            }],
            cells: vec![NativeScalarCell::Text("native".to_owned())],
        });

        let encoded = encode_current_state_data_part(&[row.clone()], &mut None)
            .expect("typed payload should encode");
        let decoded = decode_current_state_data_part(&encoded.digest, &encoded.bytes)
            .expect("typed payload should decode");
        assert_eq!(decoded, vec![row.clone()]);
        assert!(matches!(decoded[0].snapshot, ChangePayload::Native(_)));

        if let ChangePayload::Native(native) = &mut row.snapshot {
            native.owner_commit_id = CommitId::for_test_label("wrong-owner");
        }
        assert!(encode_current_state_data_part(&[row.clone()], &mut None).is_err());
        if let ChangePayload::Native(native) = &mut row.snapshot {
            native.owner_commit_id = owner_commit_id;
            native.state_key_digest = [9; 32];
        }
        assert!(encode_current_state_data_part(&[row], &mut None).is_err());
    }
}
