use crate::entity_identity::EntityIdentity;
use crate::untracked_state::{UntrackedStateIdentity, UntrackedStateRow, UntrackedStateRowRef};
use crate::LixError;

const UNTRACKED_STATE_FILE_IDENTIFIER: &str = "LXUS";
// Durable payload bytes:
//   b"LXUP" | version:u8 |
//   snapshot_content_tag:u8 | [snapshot_content_len:u32be | snapshot_content:utf8] |
//   metadata_tag:u8 | [metadata_len:u32be | metadata:utf8] |
//   created_at_len:u32be | created_at:utf8 |
//   updated_at_len:u32be | updated_at:utf8 |
//   global:u8
const UNTRACKED_STATE_PAYLOAD_IDENTIFIER: &[u8; 4] = b"LXUP";
const UNTRACKED_STATE_PAYLOAD_VERSION_V1: u8 = 1;

#[cfg_attr(not(feature = "storage-benches"), allow(dead_code))]
pub(crate) fn encode_row_ref(row: UntrackedStateRowRef<'_>) -> Result<Vec<u8>, LixError> {
    let entity_id = row.entity_id.as_json_array_text().map_err(|error| {
        LixError::unknown(format!(
            "failed to encode untracked-state entity identity: {error}"
        ))
    })?;

    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(256);
    let entity_id = builder.create_string(&entity_id);
    let schema_key = builder.create_string(row.schema_key);
    let file_id = row.file_id.map(|value| builder.create_string(value));
    let snapshot_content = row
        .snapshot_content
        .map(|value| builder.create_string(value));
    let metadata = row.metadata.map(|value| builder.create_string(value));
    let created_at = builder.create_string(row.created_at);
    let updated_at = builder.create_string(row.updated_at);
    let version_id = builder.create_string(row.version_id);

    let root = flatbuffer::create_untracked_state_row(
        &mut builder,
        &flatbuffer::UntrackedStateRowArgs {
            entity_id,
            schema_key,
            file_id,
            snapshot_content,
            metadata,
            created_at,
            updated_at,
            global: row.global,
            version_id,
        },
    );
    builder.finish(root, Some(UNTRACKED_STATE_FILE_IDENTIFIER));
    Ok(builder.finished_data().to_vec())
}

pub(crate) fn encode_payload_ref(row: UntrackedStateRowRef<'_>) -> Result<Vec<u8>, LixError> {
    let mut out = Vec::with_capacity(payload_capacity(row));
    out.extend_from_slice(UNTRACKED_STATE_PAYLOAD_IDENTIFIER);
    out.push(UNTRACKED_STATE_PAYLOAD_VERSION_V1);
    push_optional_string(&mut out, row.snapshot_content)?;
    push_optional_string(&mut out, row.metadata)?;
    push_string(&mut out, row.created_at)?;
    push_string(&mut out, row.updated_at)?;
    out.push(u8::from(row.global));
    Ok(out)
}

pub(crate) fn decode_payload_with_identity(
    identity: UntrackedStateIdentity,
    bytes: &[u8],
) -> Result<UntrackedStateRow, LixError> {
    if !bytes.starts_with(UNTRACKED_STATE_PAYLOAD_IDENTIFIER) {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode untracked-state payload: invalid payload identifier",
        ));
    }

    let mut cursor = UNTRACKED_STATE_PAYLOAD_IDENTIFIER.len();
    let version = read_u8(bytes, &mut cursor, "version")?;
    if version != UNTRACKED_STATE_PAYLOAD_VERSION_V1 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode untracked-state payload: unsupported version {version}"),
        ));
    }
    let snapshot_content = read_optional_string(bytes, &mut cursor, "snapshot_content")?;
    let metadata = read_optional_string(bytes, &mut cursor, "metadata")?;
    let created_at = read_string(bytes, &mut cursor, "created_at")?;
    let updated_at = read_string(bytes, &mut cursor, "updated_at")?;
    let global = read_bool(bytes, &mut cursor, "global")?;
    if cursor != bytes.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode untracked-state payload: trailing bytes",
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
        global,
        version_id: identity.version_id,
    })
}

#[allow(dead_code)]
pub(crate) fn decode_row(bytes: &[u8]) -> Result<UntrackedStateRow, LixError> {
    if bytes.len() < flatbuffers::SIZE_UOFFSET + flatbuffers::FILE_IDENTIFIER_LENGTH
        || !flatbuffers::buffer_has_identifier(bytes, UNTRACKED_STATE_FILE_IDENTIFIER, false)
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode untracked-state row: invalid FlatBuffers file identifier",
        ));
    }

    let row = flatbuffer::root_as_untracked_state_row(bytes).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode untracked-state row: {error}"),
        )
    })?;

    let entity_id = required_str(row.entity_id(), "entity_id")?;
    let entity_id = EntityIdentity::from_json_array_text(entity_id).map_err(|error| {
        LixError::unknown(format!(
            "failed to decode untracked-state entity identity: {error}"
        ))
    })?;

    Ok(UntrackedStateRow {
        entity_id,
        schema_key: required_str(row.schema_key(), "schema_key")?.to_string(),
        file_id: row.file_id().map(ToString::to_string),
        snapshot_content: row.snapshot_content().map(ToString::to_string),
        metadata: row.metadata().map(ToString::to_string),
        created_at: required_str(row.created_at(), "created_at")?.to_string(),
        updated_at: required_str(row.updated_at(), "updated_at")?.to_string(),
        global: row.global(),
        version_id: required_str(row.version_id(), "version_id")?.to_string(),
    })
}

fn payload_capacity(row: UntrackedStateRowRef<'_>) -> usize {
    UNTRACKED_STATE_PAYLOAD_IDENTIFIER.len()
        + 1
        + optional_string_capacity(row.snapshot_content)
        + optional_string_capacity(row.metadata)
        + string_capacity(row.created_at)
        + string_capacity(row.updated_at)
        + 1
}

fn optional_string_capacity(value: Option<&str>) -> usize {
    1 + value.map_or(0, string_capacity)
}

fn string_capacity(value: &str) -> usize {
    4 + value.len()
}

fn push_optional_string(out: &mut Vec<u8>, value: Option<&str>) -> Result<(), LixError> {
    match value {
        Some(value) => {
            out.push(1);
            push_string(out, value)?;
        }
        None => out.push(0),
    }
    Ok(())
}

fn push_string(out: &mut Vec<u8>, value: &str) -> Result<(), LixError> {
    let len = u32::try_from(value.len()).map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to encode untracked-state payload: string length exceeds u32",
        )
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_optional_string(
    bytes: &[u8],
    cursor: &mut usize,
    field: &str,
) -> Result<Option<String>, LixError> {
    let tag = read_u8(bytes, cursor, field)?;
    match tag {
        0 => Ok(None),
        1 => read_string(bytes, cursor, field).map(Some),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode untracked-state payload: invalid optional tag for `{field}`"),
        )),
    }
}

fn read_string(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<String, LixError> {
    let len = read_u32(bytes, cursor, field)? as usize;
    let end = cursor.checked_add(len).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode untracked-state payload: `{field}` length overflow"),
        )
    })?;
    let value = bytes.get(*cursor..end).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode untracked-state payload: truncated `{field}`"),
        )
    })?;
    *cursor = end;
    std::str::from_utf8(value)
        .map(str::to_string)
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to decode untracked-state payload: invalid utf-8 for `{field}`: {error}"),
            )
        })
}

fn read_bool(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<bool, LixError> {
    match read_u8(bytes, cursor, field)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode untracked-state payload: invalid boolean for `{field}`"),
        )),
    }
}

fn read_u32(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<u32, LixError> {
    let end = cursor.checked_add(4).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode untracked-state payload: `{field}` cursor overflow"),
        )
    })?;
    let raw = bytes.get(*cursor..end).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode untracked-state payload: truncated `{field}` length"),
        )
    })?;
    *cursor = end;
    Ok(u32::from_be_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn read_u8(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<u8, LixError> {
    let value = bytes.get(*cursor).copied().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode untracked-state payload: truncated `{field}`"),
        )
    })?;
    *cursor += 1;
    Ok(value)
}

#[allow(dead_code)]
fn required_str<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str, LixError> {
    value.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode untracked-state row: missing required field `{field}`"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row_ref<'a>(
        entity_id: &'a EntityIdentity,
        snapshot_content: Option<&'a str>,
        metadata: Option<&'a str>,
    ) -> UntrackedStateRowRef<'a> {
        UntrackedStateRowRef {
            entity_id,
            schema_key: "schema.unicode",
            file_id: Some("file-1"),
            snapshot_content,
            metadata,
            created_at: "2026-05-19T00:00:00.000Z",
            updated_at: "2026-05-19T00:00:01.000Z",
            global: false,
            version_id: "version-1",
        }
    }

    fn identity(entity_id: EntityIdentity) -> UntrackedStateIdentity {
        UntrackedStateIdentity {
            version_id: "version-1".to_string(),
            schema_key: "schema.unicode".to_string(),
            entity_id,
            file_id: Some("file-1".to_string()),
        }
    }

    #[test]
    fn payload_v1_roundtrips_with_key_identity() {
        let entity_id = EntityIdentity::tuple(vec!["id-1".to_string(), "東京".to_string()])
            .expect("entity identity should build");
        let bytes = encode_payload_ref(row_ref(
            &entity_id,
            Some("{\"hello\":\"world\"}"),
            Some("{\"meta\":true}"),
        ))
        .expect("payload should encode");

        assert_eq!(&bytes[..4], b"LXUP");
        assert_eq!(bytes[4], 1);

        let decoded = decode_payload_with_identity(identity(entity_id.clone()), &bytes)
            .expect("payload should decode");
        assert_eq!(decoded.entity_id, entity_id);
        assert_eq!(decoded.schema_key, "schema.unicode");
        assert_eq!(decoded.file_id.as_deref(), Some("file-1"));
        assert_eq!(
            decoded.snapshot_content.as_deref(),
            Some("{\"hello\":\"world\"}")
        );
        assert_eq!(decoded.metadata.as_deref(), Some("{\"meta\":true}"));
        assert_eq!(decoded.created_at, "2026-05-19T00:00:00.000Z");
        assert_eq!(decoded.updated_at, "2026-05-19T00:00:01.000Z");
        assert!(!decoded.global);
        assert_eq!(decoded.version_id, "version-1");
    }

    #[test]
    fn payload_v1_roundtrips_absent_optional_fields() {
        let entity_id = EntityIdentity::single("id-1");
        let bytes =
            encode_payload_ref(row_ref(&entity_id, None, None)).expect("payload should encode");
        let decoded = decode_payload_with_identity(identity(entity_id), &bytes)
            .expect("payload should decode");
        assert_eq!(decoded.snapshot_content, None);
        assert_eq!(decoded.metadata, None);
    }

    #[test]
    fn payload_decode_rejects_invalid_identifier() {
        let entity_id = EntityIdentity::single("id-1");
        let error = decode_payload_with_identity(identity(entity_id), b"LXUSnot-payload")
            .expect_err("old full-row values are not accepted in v1 payload storage");
        assert!(error.to_string().contains("invalid payload identifier"));
    }

    #[test]
    fn payload_decode_rejects_unknown_version() {
        let entity_id = EntityIdentity::single("id-1");
        let mut bytes = encode_payload_ref(row_ref(&entity_id, Some("{}"), None))
            .expect("payload should encode");
        bytes[4] = 2;
        let error = decode_payload_with_identity(identity(entity_id), &bytes)
            .expect_err("unknown payload version should fail");
        assert!(error.to_string().contains("unsupported version 2"));
    }

    #[test]
    fn payload_decode_rejects_trailing_bytes() {
        let entity_id = EntityIdentity::single("id-1");
        let mut bytes = encode_payload_ref(row_ref(&entity_id, Some("{}"), None))
            .expect("payload should encode");
        bytes.push(0);
        let error = decode_payload_with_identity(identity(entity_id), &bytes)
            .expect_err("trailing bytes should fail");
        assert!(error.to_string().contains("trailing bytes"));
    }

    #[test]
    fn payload_decode_rejects_truncated_string() {
        let entity_id = EntityIdentity::single("id-1");
        let mut bytes = encode_payload_ref(row_ref(&entity_id, Some("{}"), None))
            .expect("payload should encode");
        bytes.truncate(bytes.len() - 2);
        let error = decode_payload_with_identity(identity(entity_id), &bytes)
            .expect_err("truncated payload should fail");
        assert!(error.to_string().contains("truncated"));
    }
}

#[allow(dead_code)]
mod flatbuffer {
    #[derive(Copy, Clone, PartialEq)]
    pub(super) struct UntrackedStateRow<'a> {
        table: flatbuffers::Table<'a>,
    }

    impl<'a> flatbuffers::Follow<'a> for UntrackedStateRow<'a> {
        type Inner = UntrackedStateRow<'a>;

        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            Self {
                table: unsafe { flatbuffers::Table::new(buf, loc) },
            }
        }
    }

    impl<'a> UntrackedStateRow<'a> {
        const VT_ENTITY_ID: flatbuffers::VOffsetT = 4;
        const VT_SCHEMA_KEY: flatbuffers::VOffsetT = 6;
        const VT_FILE_ID: flatbuffers::VOffsetT = 8;
        const VT_SNAPSHOT_CONTENT: flatbuffers::VOffsetT = 10;
        const VT_METADATA: flatbuffers::VOffsetT = 12;
        const VT_CREATED_AT: flatbuffers::VOffsetT = 14;
        const VT_UPDATED_AT: flatbuffers::VOffsetT = 16;
        const VT_GLOBAL: flatbuffers::VOffsetT = 18;
        const VT_VERSION_ID: flatbuffers::VOffsetT = 20;

        #[inline]
        pub(super) fn entity_id(&self) -> Option<&'a str> {
            unsafe {
                self.table
                    .get::<flatbuffers::ForwardsUOffset<&str>>(Self::VT_ENTITY_ID, None)
            }
        }

        #[inline]
        pub(super) fn schema_key(&self) -> Option<&'a str> {
            unsafe {
                self.table
                    .get::<flatbuffers::ForwardsUOffset<&str>>(Self::VT_SCHEMA_KEY, None)
            }
        }

        #[inline]
        pub(super) fn file_id(&self) -> Option<&'a str> {
            unsafe {
                self.table
                    .get::<flatbuffers::ForwardsUOffset<&str>>(Self::VT_FILE_ID, None)
            }
        }

        #[inline]
        pub(super) fn snapshot_content(&self) -> Option<&'a str> {
            unsafe {
                self.table
                    .get::<flatbuffers::ForwardsUOffset<&str>>(Self::VT_SNAPSHOT_CONTENT, None)
            }
        }

        #[inline]
        pub(super) fn metadata(&self) -> Option<&'a str> {
            unsafe {
                self.table
                    .get::<flatbuffers::ForwardsUOffset<&str>>(Self::VT_METADATA, None)
            }
        }

        pub(super) fn created_at(&self) -> Option<&'a str> {
            unsafe {
                self.table
                    .get::<flatbuffers::ForwardsUOffset<&str>>(Self::VT_CREATED_AT, None)
            }
        }

        #[inline]
        pub(super) fn updated_at(&self) -> Option<&'a str> {
            unsafe {
                self.table
                    .get::<flatbuffers::ForwardsUOffset<&str>>(Self::VT_UPDATED_AT, None)
            }
        }

        #[inline]
        pub(super) fn global(&self) -> bool {
            unsafe { self.table.get::<bool>(Self::VT_GLOBAL, Some(false)) }.unwrap_or(false)
        }

        #[inline]
        pub(super) fn version_id(&self) -> Option<&'a str> {
            unsafe {
                self.table
                    .get::<flatbuffers::ForwardsUOffset<&str>>(Self::VT_VERSION_ID, None)
            }
        }
    }

    impl flatbuffers::Verifiable for UntrackedStateRow<'_> {
        #[inline]
        fn run_verifier(
            verifier: &mut flatbuffers::Verifier,
            position: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            verifier
                .visit_table(position)?
                .visit_field::<flatbuffers::ForwardsUOffset<&str>>(
                    "entity_id",
                    Self::VT_ENTITY_ID,
                    true,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<&str>>(
                    "schema_key",
                    Self::VT_SCHEMA_KEY,
                    true,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<&str>>(
                    "file_id",
                    Self::VT_FILE_ID,
                    false,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<&str>>(
                    "snapshot_content",
                    Self::VT_SNAPSHOT_CONTENT,
                    false,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<&str>>(
                    "metadata",
                    Self::VT_METADATA,
                    false,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<&str>>(
                    "created_at",
                    Self::VT_CREATED_AT,
                    true,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<&str>>(
                    "updated_at",
                    Self::VT_UPDATED_AT,
                    true,
                )?
                .visit_field::<bool>("global", Self::VT_GLOBAL, false)?
                .visit_field::<flatbuffers::ForwardsUOffset<&str>>(
                    "version_id",
                    Self::VT_VERSION_ID,
                    true,
                )?
                .finish();
            Ok(())
        }
    }

    #[cfg_attr(not(feature = "storage-benches"), allow(dead_code))]
    pub(super) struct UntrackedStateRowArgs<'a> {
        pub(super) entity_id: flatbuffers::WIPOffset<&'a str>,
        pub(super) schema_key: flatbuffers::WIPOffset<&'a str>,
        pub(super) file_id: Option<flatbuffers::WIPOffset<&'a str>>,
        pub(super) snapshot_content: Option<flatbuffers::WIPOffset<&'a str>>,
        pub(super) metadata: Option<flatbuffers::WIPOffset<&'a str>>,
        pub(super) created_at: flatbuffers::WIPOffset<&'a str>,
        pub(super) updated_at: flatbuffers::WIPOffset<&'a str>,
        pub(super) global: bool,
        pub(super) version_id: flatbuffers::WIPOffset<&'a str>,
    }

    #[cfg_attr(not(feature = "storage-benches"), allow(dead_code))]
    pub(super) fn create_untracked_state_row<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
        builder: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
        args: &'args UntrackedStateRowArgs<'args>,
    ) -> flatbuffers::WIPOffset<UntrackedStateRow<'bldr>> {
        let start = builder.start_table();
        builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
            UntrackedStateRow::VT_VERSION_ID,
            args.version_id,
        );
        builder.push_slot::<bool>(UntrackedStateRow::VT_GLOBAL, args.global, false);
        builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
            UntrackedStateRow::VT_UPDATED_AT,
            args.updated_at,
        );
        builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
            UntrackedStateRow::VT_CREATED_AT,
            args.created_at,
        );
        if let Some(metadata) = args.metadata {
            builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
                UntrackedStateRow::VT_METADATA,
                metadata,
            );
        }
        if let Some(snapshot_content) = args.snapshot_content {
            builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
                UntrackedStateRow::VT_SNAPSHOT_CONTENT,
                snapshot_content,
            );
        }
        if let Some(file_id) = args.file_id {
            builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
                UntrackedStateRow::VT_FILE_ID,
                file_id,
            );
        }
        builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
            UntrackedStateRow::VT_SCHEMA_KEY,
            args.schema_key,
        );
        builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
            UntrackedStateRow::VT_ENTITY_ID,
            args.entity_id,
        );
        let offset = builder.end_table(start);
        flatbuffers::WIPOffset::new(offset.value())
    }

    #[inline]
    pub(super) fn root_as_untracked_state_row(
        bytes: &[u8],
    ) -> Result<UntrackedStateRow<'_>, flatbuffers::InvalidFlatbuffer> {
        flatbuffers::root::<UntrackedStateRow>(bytes)
    }
}
