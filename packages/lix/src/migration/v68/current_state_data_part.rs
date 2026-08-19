//! Frozen decoder for v68 sparse current-state data parts.
//!
//! Rows remain format-neutral by retaining the v68 tracked-state value as
//! opaque owned bytes.  The decoder nevertheless applies every structural
//! and value-canonicality check that the v68 reader applied.

use std::borrow::Cow;

use crate::json_store::JsonSlot;
use crate::{LixError, storage_codec};

const RAW_MAGIC: &[u8; 7] = b"LXCSP01";
const ZSTD_MAGIC: &[u8; 7] = b"LXCSPZ1";
const DIGEST_CONTEXT: &str = "lix native current-state data part v1";
const MAX_ROWS: usize = 512;
const MAX_PHYSICAL_BYTES: usize = 4 * 1024 * 1024;
const MAX_DECODED_BYTES: usize = 16 * 1024 * 1024;

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

/// One fully owned, runtime-neutral row from a v68 current-state part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::migration) struct CurrentStateDataPartRow {
    pub(in crate::migration) encoded_key: Vec<u8>,
    /// Canonical v68 tracked-state value bytes.
    pub(in crate::migration) encoded_value: Vec<u8>,
    pub(in crate::migration) snapshot: JsonSlot,
    pub(in crate::migration) metadata: JsonSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredCurrentStateDataRow {
    #[musli(bytes)]
    encoded_key: Vec<u8>,
    #[musli(bytes)]
    encoded_value: Vec<u8>,
    #[musli(with = crate::json_store::json_slot_storage)]
    snapshot: JsonSlot,
    #[musli(with = crate::json_store::json_slot_storage)]
    metadata: JsonSlot,
}

/// Decodes the exact LXCSP01/LXCSPZ1 format shipped by `origin/main` at v68.
pub(in crate::migration) fn decode_current_state_data_part(
    expected_digest: &[u8; 32],
    encoded: &[u8],
) -> Result<Vec<CurrentStateDataPartRow>, LixError> {
    if encoded.len() > MAX_PHYSICAL_BYTES || &digest(encoded) != expected_digest {
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
        if decoded_len > MAX_DECODED_BYTES {
            return Err(part_error("compressed payload exceeds its decoded bound"));
        }
        Cow::Owned(
            crate::compression::decompress_zstd(compressed, decoded_len)
                .map_err(|error| part_error(format!("decompression failed: {error}")))?,
        )
    } else {
        return Err(part_error("unsupported format"));
    };

    let stored: Vec<StoredCurrentStateDataRow> =
        storage_codec::decode("v68 native current-state data part", &payload)?;
    let rows = stored
        .into_iter()
        .map(|row| CurrentStateDataPartRow {
            encoded_key: row.encoded_key,
            encoded_value: row.encoded_value,
            snapshot: row.snapshot,
            metadata: row.metadata,
        })
        .collect::<Vec<_>>();
    validate_rows(&rows)?;
    Ok(rows)
}

fn validate_rows(rows: &[CurrentStateDataPartRow]) -> Result<(), LixError> {
    if rows.is_empty() || rows.len() > MAX_ROWS {
        return Err(part_error("rows are empty or oversized"));
    }
    for row in rows {
        if row.encoded_key.is_empty() || row.snapshot.is_none() {
            return Err(part_error("row has an empty key or missing snapshot"));
        }
        if validate_encoded_value(&row.encoded_value)? {
            return Err(part_error("row is a tombstone"));
        }
    }
    if rows
        .windows(2)
        .any(|pair| pair[0].encoded_key >= pair[1].encoded_key)
    {
        return Err(part_error("rows are unordered"));
    }
    Ok(())
}

/// Returns the deleted bit after validating the complete frozen value codec.
fn validate_encoded_value(bytes: &[u8]) -> Result<bool, LixError> {
    if !(VALUE_MIN_BYTES..=VALUE_MAX_BYTES).contains(&bytes.len()) {
        return Err(part_error(format!(
            "tracked-state value has {} bytes; expected {VALUE_MIN_BYTES}..={VALUE_MAX_BYTES}",
            bytes.len()
        )));
    }
    // The first 32 bytes are UUID bit patterns and every pattern is valid.
    let _change_id = &bytes[..VALUE_CHANGE_ID_END];
    let _commit_id = &bytes[VALUE_COMMIT_ID_START..VALUE_COMMIT_ID_END];
    let mut offset = VALUE_STATE_TAIL_START;
    let tag = bytes[offset];
    offset += 1;
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
            return Err(part_error(
                "tracked-state value has a reserved state-tail tag",
            ));
        }
    };
    let created_at = read_minimal_timestamp(bytes, &mut offset, created_width)?;
    let updated_at = if equal {
        created_at
    } else {
        read_minimal_timestamp(bytes, &mut offset, updated_width)?
    };
    if !equal && created_at == updated_at {
        return Err(part_error(
            "tracked-state value uses the distinct form for equal timestamps",
        ));
    }
    if offset != bytes.len() {
        return Err(part_error("tracked-state value has trailing bytes"));
    }
    crate::common::LixTimestamp::from_packed(created_at)
        .map_err(|error| part_error(format!("invalid created_at: {error}")))?;
    crate::common::LixTimestamp::from_packed(updated_at)
        .map_err(|error| part_error(format!("invalid updated_at: {error}")))?;
    Ok(deleted)
}

fn read_minimal_timestamp(bytes: &[u8], offset: &mut usize, width: usize) -> Result<u64, LixError> {
    let end = offset
        .checked_add(width)
        .ok_or_else(|| part_error("timestamp width overflows"))?;
    let encoded = bytes
        .get(*offset..end)
        .ok_or_else(|| part_error("timestamp is truncated"))?;
    if width > 0 && encoded[width - 1] == 0 {
        return Err(part_error("timestamp is not minimally encoded"));
    }
    let mut storage = [0u8; 8];
    storage[..width].copy_from_slice(encoded);
    *offset = end;
    Ok(u64::from_le_bytes(storage))
}

fn digest(encoded: &[u8]) -> [u8; 32] {
    *blake3::Hasher::new_derive_key(DIGEST_CONTEXT)
        .update(encoded)
        .finalize()
        .as_bytes()
}

fn part_error(message: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("v68 current-state data part: {message}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;

    fn encoded_value(index: u8, deleted: bool) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(
            ChangeId::for_test_label(&format!("v68-change-{index}"))
                .as_uuid()
                .as_bytes(),
        );
        bytes.extend_from_slice(
            CommitId::for_test_label(&format!("v68-commit-{index}"))
                .as_uuid()
                .as_bytes(),
        );
        let packed = LixTimestamp::from_unix_millis_utc_lossy(i64::from(index)).packed();
        let width = if packed == 0 {
            0
        } else {
            usize::try_from((u64::BITS - packed.leading_zeros()).div_ceil(8)).expect("width")
        };
        bytes.push(
            u8::try_from(width).expect("width") | if deleted { VALUE_TAIL_DELETED } else { 0 },
        );
        bytes.extend_from_slice(&packed.to_le_bytes()[..width]);
        bytes
    }

    fn frozen_payload() -> Vec<u8> {
        storage_codec::encode(
            "frozen v68 current-state fixture",
            &vec![
                StoredCurrentStateDataRow {
                    encoded_key: b"alpha".to_vec(),
                    encoded_value: encoded_value(1, false),
                    snapshot: JsonSlot::from_json(r#"{"row":1}"#),
                    metadata: JsonSlot::None,
                },
                StoredCurrentStateDataRow {
                    encoded_key: b"beta".to_vec(),
                    encoded_value: encoded_value(2, false),
                    snapshot: JsonSlot::from_json(r#"{"row":2}"#),
                    metadata: JsonSlot::from_json(r#"{"source":"v68"}"#),
                },
            ],
        )
        .expect("encode frozen fixture")
    }

    fn raw_fixture() -> Vec<u8> {
        let mut encoded = RAW_MAGIC.to_vec();
        encoded.extend_from_slice(&frozen_payload());
        encoded
    }

    #[test]
    fn decodes_frozen_raw_lxcsp01_into_owned_neutral_rows() {
        let encoded = raw_fixture();
        let rows = decode_current_state_data_part(&digest(&encoded), &encoded)
            .expect("decode raw v68 part");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].encoded_key, b"alpha");
        assert_eq!(rows[0].snapshot, JsonSlot::from_json(r#"{"row":1}"#));
        assert_eq!(rows[1].metadata, JsonSlot::from_json(r#"{"source":"v68"}"#));
        assert_eq!(rows[0].encoded_value, encoded_value(1, false));
    }

    #[test]
    fn decodes_frozen_compressed_lxcspz1_and_rejects_invalid_rows() {
        let payload = frozen_payload();
        let compressed = crate::compression::ZstdLevel1Compressor::new()
            .expect("compressor")
            .compress(&payload)
            .expect("compress fixture");
        let mut encoded = ZSTD_MAGIC.to_vec();
        encoded.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&compressed);
        assert_eq!(
            decode_current_state_data_part(&digest(&encoded), &encoded)
                .expect("decode compressed v68 part")
                .len(),
            2
        );

        let tombstone = vec![StoredCurrentStateDataRow {
            encoded_key: b"alpha".to_vec(),
            encoded_value: encoded_value(1, true),
            snapshot: JsonSlot::from_json("{}"),
            metadata: JsonSlot::None,
        }];
        let mut invalid = RAW_MAGIC.to_vec();
        invalid.extend_from_slice(
            &storage_codec::encode("frozen tombstone fixture", &tombstone)
                .expect("encode tombstone"),
        );
        assert!(decode_current_state_data_part(&digest(&invalid), &invalid).is_err());

        let mut corrupt = raw_fixture();
        *corrupt.last_mut().expect("fixture is non-empty") ^= 1;
        assert!(decode_current_state_data_part(&digest(&raw_fixture()), &corrupt).is_err());
    }
}
