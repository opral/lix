use crate::untracked_state::{UntrackedStateIdentity, UntrackedStateRow, UntrackedStateRowRef};
use crate::LixError;

const ROW_VALUE_MAGIC: &[u8; 4] = b"LXU1";
const FLAG_HAS_SNAPSHOT_CONTENT: u8 = 1 << 0;
const FLAG_HAS_METADATA: u8 = 1 << 1;
const FLAG_GLOBAL: u8 = 1 << 2;

pub(crate) fn encode_row_value_ref(row: UntrackedStateRowRef<'_>) -> Result<Vec<u8>, LixError> {
    let snapshot_content = row.snapshot_content.map(str::as_bytes);
    let metadata = row.metadata.map(str::as_bytes);
    let created_at = row.created_at.as_bytes();
    let updated_at = row.updated_at.as_bytes();
    let mut flags = 0;
    if snapshot_content.is_some() {
        flags |= FLAG_HAS_SNAPSHOT_CONTENT;
    }
    if metadata.is_some() {
        flags |= FLAG_HAS_METADATA;
    }
    if row.global {
        flags |= FLAG_GLOBAL;
    }

    let mut out = Vec::with_capacity(
        ROW_VALUE_MAGIC.len()
            + 1
            + encoded_component_len(snapshot_content.unwrap_or_default())
            + encoded_component_len(metadata.unwrap_or_default())
            + encoded_component_len(created_at)
            + encoded_component_len(updated_at),
    );
    out.extend_from_slice(ROW_VALUE_MAGIC);
    out.push(flags);
    if let Some(snapshot_content) = snapshot_content {
        push_component(&mut out, snapshot_content);
    }
    if let Some(metadata) = metadata {
        push_component(&mut out, metadata);
    }
    push_component(&mut out, created_at);
    push_component(&mut out, updated_at);
    Ok(out)
}

pub(crate) fn decode_row_value(
    bytes: &[u8],
    identity: UntrackedStateIdentity,
) -> Result<UntrackedStateRow, LixError> {
    if bytes.len() < ROW_VALUE_MAGIC.len() + 1
        || bytes.get(..ROW_VALUE_MAGIC.len()) != Some(ROW_VALUE_MAGIC)
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode untracked-state row value: invalid row value header",
        ));
    }

    let flags = bytes[ROW_VALUE_MAGIC.len()];
    if flags & !(FLAG_HAS_SNAPSHOT_CONTENT | FLAG_HAS_METADATA | FLAG_GLOBAL) != 0 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode untracked-state row value: invalid row value flags",
        ));
    }

    let mut cursor = ROW_VALUE_MAGIC.len() + 1;
    let snapshot_content = if flags & FLAG_HAS_SNAPSHOT_CONTENT != 0 {
        Some(read_component(bytes, &mut cursor)?.to_string())
    } else {
        None
    };
    let metadata = if flags & FLAG_HAS_METADATA != 0 {
        Some(read_component(bytes, &mut cursor)?.to_string())
    } else {
        None
    };
    let created_at = read_component(bytes, &mut cursor)?.to_string();
    let updated_at = read_component(bytes, &mut cursor)?.to_string();
    if cursor != bytes.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode untracked-state row value: trailing bytes",
        ));
    }

    Ok(UntrackedStateRow {
        entity_id: identity.entity_id,
        schema_key: identity.schema_key,
        file_id: identity.file_id,
        snapshot_content,
        metadata,
        created_at,
        updated_at,
        global: flags & FLAG_GLOBAL != 0,
        version_id: identity.version_id,
    })
}

fn encoded_component_len(value: &[u8]) -> usize {
    varint_len(value.len()) + value.len()
}

fn push_component(out: &mut Vec<u8>, value: &[u8]) {
    push_varint_len(out, value.len());
    out.extend_from_slice(value);
}

fn read_component<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a str, LixError> {
    let len = read_varint_len(bytes, cursor)?;
    let component = bytes
        .get(*cursor..cursor.saturating_add(len))
        .ok_or_else(|| {
            LixError::unknown("failed to decode untracked-state row value: short value")
        })?;
    *cursor += len;
    std::str::from_utf8(component).map_err(|error| {
        LixError::unknown(format!(
            "failed to decode untracked-state row value: invalid UTF-8: {error}"
        ))
    })
}

fn push_varint_len(out: &mut Vec<u8>, mut value: usize) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn read_varint_len(bytes: &[u8], cursor: &mut usize) -> Result<usize, LixError> {
    let start = *cursor;
    let mut value = 0u128;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*cursor).ok_or_else(|| {
            LixError::unknown("failed to decode untracked-state row value: short varint")
        })?;
        *cursor += 1;
        value |= u128::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            if value > usize::MAX as u128 {
                return Err(LixError::unknown(
                    "failed to decode untracked-state row value: length overflow",
                ));
            }
            let value = value as usize;
            let mut canonical = Vec::new();
            push_varint_len(&mut canonical, value);
            if bytes.get(start..*cursor) != Some(canonical.as_slice()) {
                return Err(LixError::unknown(
                    "failed to decode untracked-state row value: non-canonical length",
                ));
            }
            return Ok(value);
        }
        shift += 7;
        if shift >= 128 {
            return Err(LixError::unknown(
                "failed to decode untracked-state row value: length overflow",
            ));
        }
    }
}

fn varint_len(mut value: usize) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        len += 1;
        value >>= 7;
    }
    len
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_identity::EntityIdentity;

    fn row() -> UntrackedStateRow {
        UntrackedStateRow {
            entity_id: EntityIdentity::single("entity-a"),
            schema_key: "schema-a".to_string(),
            file_id: Some("file-a".to_string()),
            snapshot_content: Some("{\"value\":1}".to_string()),
            metadata: None,
            created_at: "2026-05-12T00:00:00Z".to_string(),
            updated_at: "2026-05-12T00:00:01Z".to_string(),
            global: false,
            version_id: "version-a".to_string(),
        }
    }

    #[test]
    fn row_value_omits_identity_and_decodes_from_supplied_identity() {
        let row = row();
        let encoded = encode_row_value_ref(row.as_ref()).expect("row value should encode");
        let encoded_text = String::from_utf8_lossy(&encoded);

        assert!(encoded_text.contains("{\"value\":1}"));
        assert!(!encoded_text.contains("entity-a"));
        assert!(!encoded_text.contains("schema-a"));
        assert!(!encoded_text.contains("file-a"));
        assert!(!encoded_text.contains("version-a"));

        let supplied_identity = UntrackedStateIdentity {
            entity_id: EntityIdentity::single("entity-from-key"),
            schema_key: "schema-from-key".to_string(),
            file_id: Some("file-from-key".to_string()),
            version_id: "version-from-key".to_string(),
        };
        let decoded =
            decode_row_value(&encoded, supplied_identity).expect("row value should decode");

        assert_eq!(decoded.entity_id, EntityIdentity::single("entity-from-key"));
        assert_eq!(decoded.schema_key, "schema-from-key");
        assert_eq!(decoded.file_id.as_deref(), Some("file-from-key"));
        assert_eq!(decoded.version_id, "version-from-key");
        assert_eq!(decoded.snapshot_content.as_deref(), Some("{\"value\":1}"));
        assert_eq!(decoded.created_at, row.created_at);
        assert_eq!(decoded.updated_at, row.updated_at);
        assert_eq!(decoded.global, row.global);
    }

    #[test]
    fn row_value_roundtrips_metadata_and_global() {
        let mut row = row();
        row.metadata = Some("{\"source\":\"test\"}".to_string());
        row.global = true;
        let encoded = encode_row_value_ref(row.as_ref()).expect("row value should encode");
        let decoded = decode_row_value(
            &encoded,
            UntrackedStateIdentity {
                entity_id: row.entity_id.clone(),
                schema_key: row.schema_key.clone(),
                file_id: row.file_id.clone(),
                version_id: row.version_id.clone(),
            },
        )
        .expect("row value should decode");

        assert_eq!(decoded.metadata, row.metadata);
        assert!(decoded.global);
    }

    #[test]
    fn row_value_rejects_malformed_bytes() {
        let identity = UntrackedStateIdentity {
            entity_id: EntityIdentity::single("entity-a"),
            schema_key: "schema-a".to_string(),
            file_id: None,
            version_id: "version-a".to_string(),
        };

        assert!(decode_row_value(b"BAD!", identity.clone()).is_err());
        assert!(decode_row_value(b"LXU1\x80", identity.clone()).is_err());
        assert!(decode_row_value(b"LXU1\x00\xff", identity.clone()).is_err());
        assert!(decode_row_value(b"LXU1\x00\x80\x00", identity.clone()).is_err());
        let mut overflow = b"LXU1\x00".to_vec();
        overflow.extend(std::iter::repeat_n(0xff, 19));
        overflow.push(0x01);
        assert!(decode_row_value(&overflow, identity).is_err());
    }

    #[test]
    fn row_value_rejects_trailing_bytes_and_invalid_utf8() {
        let identity = UntrackedStateIdentity {
            entity_id: EntityIdentity::single("entity-a"),
            schema_key: "schema-a".to_string(),
            file_id: None,
            version_id: "version-a".to_string(),
        };
        let row = row();
        let mut encoded = encode_row_value_ref(row.as_ref()).expect("row value should encode");
        encoded.push(0);
        assert!(decode_row_value(&encoded, identity.clone()).is_err());

        let invalid_created_at = b"LXU1\x00\x01\xff\x01x".to_vec();
        assert!(decode_row_value(&invalid_created_at, identity).is_err());
    }

    #[test]
    fn row_value_has_stable_golden_encoding() {
        let mut row = row();
        row.snapshot_content = Some("abc".to_string());
        row.metadata = Some("m".to_string());
        row.created_at = "c".to_string();
        row.updated_at = "u".to_string();
        row.global = true;

        let encoded = encode_row_value_ref(row.as_ref()).expect("row value should encode");
        assert_eq!(
            encoded,
            b"LXU1\x07\x03abc\x01m\x01c\x01u",
            "row value format should remain compact and intentional"
        );
    }
}
