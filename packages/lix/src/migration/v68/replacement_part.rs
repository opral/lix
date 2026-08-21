//! Frozen decoder for v68 complete-replacement identity parts.
//!
//! The live replacement-part codec has moved on to a typed-row format.  This
//! reader intentionally owns its output and depends on no live tracked-state
//! row type, so a migration can safely rewrite the old rows before handing
//! them to the current runtime.

use std::borrow::Cow;

use crate::LixError;
use crate::json_store::LegacyJsonValue;

const RAW_MAGIC: &[u8; 8] = b"LXRPI003";
const ZSTD_MAGIC: &[u8; 8] = b"LXRPZ003";
const DIGEST_CONTEXT: &str = "lix tracked-state replacement identity part v1";
const MAX_ROWS: usize = 512;
const MAX_PHYSICAL_BYTES: usize = 4 * 1024 * 1024;
const MAX_DECODED_BYTES: usize = 16 * 1024 * 1024;

/// One fully owned row from a v68 replacement part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::migration) struct ReplacementPartRow {
    pub(in crate::migration) encoded_key: Vec<u8>,
    pub(in crate::migration) snapshot: LegacyJsonValue,
    pub(in crate::migration) metadata: LegacyJsonValue,
}

/// Decodes the exact LXRPI003/LXRPZ003 format shipped by `origin/main` at v68.
pub(in crate::migration) fn decode_replacement_part(
    expected_digest: &[u8; 32],
    encoded: &[u8],
) -> Result<Vec<ReplacementPartRow>, LixError> {
    if encoded.len() > MAX_PHYSICAL_BYTES {
        return Err(part_error("physical payload exceeds its bound"));
    }
    if &digest(encoded) != expected_digest {
        return Err(part_error("content digest mismatch"));
    }

    let logical: Cow<'_, [u8]> = if let Some(body) = encoded.strip_prefix(ZSTD_MAGIC) {
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
        Cow::Borrowed(encoded)
    };

    let body = logical
        .strip_prefix(RAW_MAGIC)
        .ok_or_else(|| part_error("invalid magic"))?;
    let mut cursor = 0usize;
    let row_count = usize::from(decode_u16(body, &mut cursor)?);
    if row_count == 0 || row_count > MAX_ROWS {
        return Err(part_error("invalid row count"));
    }

    let mut rows = Vec::with_capacity(row_count);
    let mut previous_key = Vec::new();
    for _ in 0..row_count {
        let shared = usize::from(decode_u16(body, &mut cursor)?);
        let suffix_len = usize::from(decode_u16(body, &mut cursor)?);
        if shared > previous_key.len() {
            return Err(part_error("key prefix exceeds the previous key"));
        }
        let suffix = take_exact(body, &mut cursor, suffix_len)?;
        let mut encoded_key = Vec::with_capacity(shared + suffix_len);
        encoded_key.extend_from_slice(&previous_key[..shared]);
        encoded_key.extend_from_slice(suffix);
        if encoded_key.is_empty() || (!previous_key.is_empty() && previous_key >= encoded_key) {
            return Err(part_error("keys are not strictly ordered"));
        }
        let snapshot = decode_json_slot(body, &mut cursor, true)?;
        let metadata = decode_json_slot(body, &mut cursor, false)?;
        previous_key.clone_from(&encoded_key);
        rows.push(ReplacementPartRow {
            encoded_key,
            snapshot,
            metadata,
        });
    }
    if cursor != body.len() {
        return Err(part_error("trailing bytes"));
    }
    Ok(rows)
}

fn decode_json_slot(
    encoded: &[u8],
    cursor: &mut usize,
    required: bool,
) -> Result<LegacyJsonValue, LixError> {
    match take_exact(encoded, cursor, 1)?[0] {
        0 if required => Err(part_error("snapshot payload is missing")),
        0 => Ok(LegacyJsonValue::None),
        1 => Ok(LegacyJsonValue::Ref(
            crate::json_store::JsonRef::from_hash_bytes(
                take_exact(encoded, cursor, 32)?
                    .try_into()
                    .expect("JSON reference width checked"),
            ),
        )),
        2 => {
            let len = usize::try_from(decode_u32(encoded, cursor)?).expect("u32 fits usize");
            let json = std::str::from_utf8(take_exact(encoded, cursor, len)?)
                .map_err(|_| part_error("inline JSON is not UTF-8"))?;
            Ok(LegacyJsonValue::Inline(json.to_owned().into()))
        }
        _ => Err(part_error("JSON slot has an invalid tag")),
    }
}

fn decode_u16(encoded: &[u8], cursor: &mut usize) -> Result<u16, LixError> {
    Ok(u16::from_be_bytes(
        take_exact(encoded, cursor, 2)?
            .try_into()
            .expect("u16 width checked"),
    ))
}

fn decode_u32(encoded: &[u8], cursor: &mut usize) -> Result<u32, LixError> {
    Ok(u32::from_be_bytes(
        take_exact(encoded, cursor, 4)?
            .try_into()
            .expect("u32 width checked"),
    ))
}

fn take_exact<'a>(encoded: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8], LixError> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| part_error("codec offset overflows"))?;
    let bytes = encoded
        .get(*cursor..end)
        .ok_or_else(|| part_error("value is truncated"))?;
    *cursor = end;
    Ok(bytes)
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
        format!("v68 replacement part: {message}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json_slot(out: &mut Vec<u8>, slot: crate::json_store::LegacyJsonValueRef<'_>) {
        match slot {
            crate::json_store::LegacyJsonValueRef::None => out.push(0),
            crate::json_store::LegacyJsonValueRef::Ref(reference) => {
                out.push(1);
                out.extend_from_slice(reference.as_hash_bytes());
            }
            crate::json_store::LegacyJsonValueRef::Inline(json) => {
                out.push(2);
                out.extend_from_slice(&(json.len() as u32).to_be_bytes());
                out.extend_from_slice(json.as_bytes());
            }
        }
    }

    fn frozen_raw_fixture(snapshot: &str) -> Vec<u8> {
        let mut bytes = RAW_MAGIC.to_vec();
        bytes.extend_from_slice(&2u16.to_be_bytes());
        bytes.extend_from_slice(&0u16.to_be_bytes());
        bytes.extend_from_slice(&5u16.to_be_bytes());
        bytes.extend_from_slice(b"alpha");
        json_slot(
            &mut bytes,
            crate::json_store::LegacyJsonValueRef::Inline(snapshot),
        );
        json_slot(&mut bytes, crate::json_store::LegacyJsonValueRef::None);
        bytes.extend_from_slice(&0u16.to_be_bytes());
        bytes.extend_from_slice(&4u16.to_be_bytes());
        bytes.extend_from_slice(b"beta");
        let reference = crate::json_store::JsonRef::for_content(b"v68 snapshot");
        json_slot(
            &mut bytes,
            crate::json_store::LegacyJsonValueRef::Ref(&reference),
        );
        json_slot(
            &mut bytes,
            crate::json_store::LegacyJsonValueRef::Inline(r#"{"source":"v68"}"#),
        );
        bytes
    }

    #[test]
    fn decodes_frozen_raw_lxrpi003_into_owned_rows() {
        let encoded = frozen_raw_fixture(r#"{"name":"alpha"}"#);
        let rows = decode_replacement_part(&digest(&encoded), &encoded).expect("decode v68 part");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].encoded_key, b"alpha");
        assert_eq!(
            rows[0].snapshot,
            LegacyJsonValue::from_json(r#"{"name":"alpha"}"#)
        );
        assert_eq!(rows[1].encoded_key, b"beta");
        assert_eq!(
            rows[1].metadata,
            LegacyJsonValue::from_json(r#"{"source":"v68"}"#)
        );
    }

    #[test]
    fn decodes_frozen_compressed_lxrpz003_and_rejects_corruption() {
        let logical = frozen_raw_fixture(&format!(r#"{{"body":"{}"}}"#, "x".repeat(2048)));
        let compressed = crate::compression::ZstdLevel1Compressor::new()
            .expect("compressor")
            .compress(&logical)
            .expect("compress fixture");
        let mut encoded = ZSTD_MAGIC.to_vec();
        encoded.extend_from_slice(&(logical.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&compressed);
        assert_eq!(
            decode_replacement_part(&digest(&encoded), &encoded)
                .expect("decode compressed v68 part")
                .len(),
            2
        );

        let mut corrupt = encoded.clone();
        *corrupt.last_mut().expect("fixture is non-empty") ^= 1;
        assert!(decode_replacement_part(&digest(&encoded), &corrupt).is_err());
    }
}
