use crate::untracked_state::{UntrackedStateIdentity, UntrackedStateRow, UntrackedStateRowRef};
use crate::LixError;

const HEADER_VALUE_MAGIC: &[u8; 4] = b"LXUH";
const PAYLOAD_VALUE_MAGIC: &[u8; 4] = b"LXUP";
const HEADER_FLAG_GLOBAL: u8 = 1 << 0;
const PAYLOAD_FLAG_HAS_METADATA: u8 = 1 << 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UntrackedStatePayloadValue {
    pub(crate) snapshot_content: String,
    pub(crate) metadata: Option<String>,
}

pub(crate) fn encode_header_value_ref(row: UntrackedStateRowRef<'_>) -> Vec<u8> {
    let created_at = row.created_at.as_bytes();
    let updated_at = row.updated_at.as_bytes();
    let mut flags = 0;
    if row.global {
        flags |= HEADER_FLAG_GLOBAL;
    }

    let mut out = Vec::with_capacity(
        HEADER_VALUE_MAGIC.len()
            + 1
            + encoded_component_len(created_at)
            + encoded_component_len(updated_at),
    );
    out.extend_from_slice(HEADER_VALUE_MAGIC);
    out.push(flags);
    push_component(&mut out, created_at);
    push_component(&mut out, updated_at);
    out
}

pub(crate) fn decode_header_value(
    bytes: &[u8],
    identity: UntrackedStateIdentity,
) -> Result<UntrackedStateRow, LixError> {
    if bytes.len() < HEADER_VALUE_MAGIC.len() + 1
        || bytes.get(..HEADER_VALUE_MAGIC.len()) != Some(HEADER_VALUE_MAGIC)
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode untracked-state header value: invalid header",
        ));
    }

    let flags = bytes[HEADER_VALUE_MAGIC.len()];
    if flags & !HEADER_FLAG_GLOBAL != 0 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode untracked-state header value: invalid flags",
        ));
    }

    let mut cursor = HEADER_VALUE_MAGIC.len() + 1;
    let created_at = read_component(bytes, &mut cursor)?.to_string();
    let updated_at = read_component(bytes, &mut cursor)?.to_string();
    if cursor != bytes.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode untracked-state header value: trailing bytes",
        ));
    }

    Ok(UntrackedStateRow {
        entity_id: identity.entity_id,
        schema_key: identity.schema_key,
        file_id: identity.file_id,
        snapshot_content: None,
        metadata: None,
        created_at,
        updated_at,
        global: flags & HEADER_FLAG_GLOBAL != 0,
        version_id: identity.version_id,
    })
}

pub(crate) fn encode_payload_value_ref(row: UntrackedStateRowRef<'_>) -> Option<Vec<u8>> {
    let snapshot_content = row.snapshot_content?;
    let metadata = row.metadata.map(str::as_bytes);
    let mut flags = 0;
    if metadata.is_some() {
        flags |= PAYLOAD_FLAG_HAS_METADATA;
    }

    let snapshot_content = snapshot_content.as_bytes();
    let mut out = Vec::with_capacity(
        PAYLOAD_VALUE_MAGIC.len()
            + 1
            + encoded_component_len(snapshot_content)
            + encoded_component_len(metadata.unwrap_or_default()),
    );
    out.extend_from_slice(PAYLOAD_VALUE_MAGIC);
    out.push(flags);
    push_component(&mut out, snapshot_content);
    if let Some(metadata) = metadata {
        push_component(&mut out, metadata);
    }
    Some(out)
}

pub(crate) fn decode_payload_value(bytes: &[u8]) -> Result<UntrackedStatePayloadValue, LixError> {
    if bytes.len() < PAYLOAD_VALUE_MAGIC.len() + 1
        || bytes.get(..PAYLOAD_VALUE_MAGIC.len()) != Some(PAYLOAD_VALUE_MAGIC)
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode untracked-state payload value: invalid header",
        ));
    }

    let flags = bytes[PAYLOAD_VALUE_MAGIC.len()];
    if flags & !PAYLOAD_FLAG_HAS_METADATA != 0 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode untracked-state payload value: invalid flags",
        ));
    }

    let mut cursor = PAYLOAD_VALUE_MAGIC.len() + 1;
    let snapshot_content = read_component(bytes, &mut cursor)?.to_string();
    let metadata = if flags & PAYLOAD_FLAG_HAS_METADATA != 0 {
        Some(read_component(bytes, &mut cursor)?.to_string())
    } else {
        None
    };
    if cursor != bytes.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode untracked-state payload value: trailing bytes",
        ));
    }
    Ok(UntrackedStatePayloadValue {
        snapshot_content,
        metadata,
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
    if value < 0x80 {
        out.push(value as u8);
        return;
    }
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
            if *cursor - start != varint_len(value) {
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
    if value < 0x80 {
        return 1;
    }
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
    fn header_value_omits_identity_payload_and_decodes_from_supplied_identity() {
        let row = row();
        let encoded = encode_header_value_ref(row.as_ref());
        let encoded_text = String::from_utf8_lossy(&encoded);

        assert!(!encoded_text.contains("{\"value\":1}"));
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
            decode_header_value(&encoded, supplied_identity).expect("header value should decode");

        assert_eq!(decoded.entity_id, EntityIdentity::single("entity-from-key"));
        assert_eq!(decoded.schema_key, "schema-from-key");
        assert_eq!(decoded.file_id.as_deref(), Some("file-from-key"));
        assert_eq!(decoded.version_id, "version-from-key");
        assert_eq!(decoded.snapshot_content, None);
        assert_eq!(decoded.metadata, None);
        assert_eq!(decoded.created_at, row.created_at);
        assert_eq!(decoded.updated_at, row.updated_at);
        assert_eq!(decoded.global, row.global);
    }

    #[test]
    fn header_value_roundtrips_global() {
        let mut row = row();
        row.global = true;
        let encoded = encode_header_value_ref(row.as_ref());
        let decoded = decode_header_value(
            &encoded,
            UntrackedStateIdentity {
                entity_id: row.entity_id.clone(),
                schema_key: row.schema_key.clone(),
                file_id: row.file_id.clone(),
                version_id: row.version_id.clone(),
            },
        )
        .expect("header value should decode");

        assert!(decoded.global);
    }

    #[test]
    fn payload_value_roundtrips_snapshot_and_metadata() {
        let mut row = row();
        row.metadata = Some("{\"source\":\"test\"}".to_string());
        let encoded = encode_payload_value_ref(row.as_ref()).expect("payload should encode");
        let decoded = decode_payload_value(&encoded).expect("payload should decode");

        assert_eq!(decoded.snapshot_content, "{\"value\":1}");
        assert_eq!(decoded.metadata, row.metadata);
    }

    #[test]
    fn payload_value_is_absent_for_removal_rows() {
        let mut row = row();
        row.snapshot_content = None;
        assert_eq!(encode_payload_value_ref(row.as_ref()), None);
    }

    #[test]
    fn header_and_payload_values_reject_malformed_bytes() {
        let identity = UntrackedStateIdentity {
            entity_id: EntityIdentity::single("entity-a"),
            schema_key: "schema-a".to_string(),
            file_id: None,
            version_id: "version-a".to_string(),
        };

        assert!(decode_header_value(b"BAD!", identity.clone()).is_err());
        assert!(decode_header_value(b"LXUH\x80", identity.clone()).is_err());
        assert!(decode_header_value(b"LXUH\x00\xff", identity.clone()).is_err());
        assert!(decode_header_value(b"LXUH\x00\x80\x00", identity.clone()).is_err());
        assert!(decode_payload_value(b"BAD!").is_err());
        assert!(decode_payload_value(b"LXUP\x80").is_err());
        assert!(decode_payload_value(b"LXUP\x00\x80\x00").is_err());
        let mut overflow = b"LXUH\x00".to_vec();
        overflow.extend(std::iter::repeat_n(0xff, 19));
        overflow.push(0x01);
        assert!(decode_header_value(&overflow, identity).is_err());
    }

    #[test]
    fn header_and_payload_values_reject_trailing_bytes_and_invalid_utf8() {
        let identity = UntrackedStateIdentity {
            entity_id: EntityIdentity::single("entity-a"),
            schema_key: "schema-a".to_string(),
            file_id: None,
            version_id: "version-a".to_string(),
        };
        let row = row();
        let mut encoded = encode_header_value_ref(row.as_ref());
        encoded.push(0);
        assert!(decode_header_value(&encoded, identity.clone()).is_err());

        let invalid_created_at = b"LXUH\x00\x01\xff\x01x".to_vec();
        assert!(decode_header_value(&invalid_created_at, identity).is_err());

        let invalid_payload = b"LXUP\x00\x01\xff".to_vec();
        assert!(decode_payload_value(&invalid_payload).is_err());
    }

    #[test]
    fn split_values_have_stable_golden_encoding() {
        let mut row = row();
        row.snapshot_content = Some("abc".to_string());
        row.metadata = Some("m".to_string());
        row.created_at = "c".to_string();
        row.updated_at = "u".to_string();
        row.global = true;

        let header = encode_header_value_ref(row.as_ref());
        let payload = encode_payload_value_ref(row.as_ref()).expect("payload should encode");
        assert_eq!(header, b"LXUH\x01\x01c\x01u");
        assert_eq!(payload, b"LXUP\x01\x03abc\x01m");
    }
}
