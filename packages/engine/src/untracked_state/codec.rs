use crate::untracked_state::{UntrackedStateIdentity, UntrackedStateRow, UntrackedStateRowRef};
use crate::LixError;

const UNTRACKED_STATE_VALUE_FILE_IDENTIFIER: &str = "LXUV";

pub(crate) fn encode_row_value_ref(row: UntrackedStateRowRef<'_>) -> Result<Vec<u8>, LixError> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(256);
    let snapshot_content = row
        .snapshot_content
        .map(|value| builder.create_string(value));
    let metadata = row.metadata.map(|value| builder.create_string(value));
    let created_at = builder.create_string(row.created_at);
    let updated_at = builder.create_string(row.updated_at);

    let root = flatbuffer::create_untracked_state_row_value(
        &mut builder,
        &flatbuffer::UntrackedStateRowValueArgs {
            snapshot_content,
            metadata,
            created_at,
            updated_at,
            global: row.global,
        },
    );
    builder.finish(root, Some(UNTRACKED_STATE_VALUE_FILE_IDENTIFIER));
    Ok(builder.finished_data().to_vec())
}

pub(crate) fn decode_row_value(
    bytes: &[u8],
    identity: UntrackedStateIdentity,
) -> Result<UntrackedStateRow, LixError> {
    if bytes.len() < flatbuffers::SIZE_UOFFSET + flatbuffers::FILE_IDENTIFIER_LENGTH
        || !flatbuffers::buffer_has_identifier(bytes, UNTRACKED_STATE_VALUE_FILE_IDENTIFIER, false)
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode untracked-state row value: invalid FlatBuffers file identifier",
        ));
    }

    let value = flatbuffer::root_as_untracked_state_row_value(bytes).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode untracked-state row value: {error}"),
        )
    })?;

    Ok(UntrackedStateRow {
        entity_id: identity.entity_id,
        schema_key: identity.schema_key,
        file_id: identity.file_id,
        snapshot_content: value.snapshot_content().map(ToString::to_string),
        metadata: value.metadata().map(ToString::to_string),
        created_at: required_str(value.created_at(), "created_at")?.to_string(),
        updated_at: required_str(value.updated_at(), "updated_at")?.to_string(),
        global: value.global(),
        version_id: identity.version_id,
    })
}

fn required_str<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str, LixError> {
    value.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode untracked-state row value: missing required field `{field}`"),
        )
    })
}

mod flatbuffer {
    #[derive(Copy, Clone, PartialEq)]
    pub(super) struct UntrackedStateRowValue<'a> {
        table: flatbuffers::Table<'a>,
    }

    impl<'a> flatbuffers::Follow<'a> for UntrackedStateRowValue<'a> {
        type Inner = UntrackedStateRowValue<'a>;

        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            Self {
                table: unsafe { flatbuffers::Table::new(buf, loc) },
            }
        }
    }

    impl<'a> UntrackedStateRowValue<'a> {
        const VT_SNAPSHOT_CONTENT: flatbuffers::VOffsetT = 4;
        const VT_METADATA: flatbuffers::VOffsetT = 6;
        const VT_CREATED_AT: flatbuffers::VOffsetT = 8;
        const VT_UPDATED_AT: flatbuffers::VOffsetT = 10;
        const VT_GLOBAL: flatbuffers::VOffsetT = 12;

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

        #[inline]
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
    }

    impl flatbuffers::Verifiable for UntrackedStateRowValue<'_> {
        #[inline]
        fn run_verifier(
            verifier: &mut flatbuffers::Verifier,
            position: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            verifier
                .visit_table(position)?
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
                .finish();
            Ok(())
        }
    }

    pub(super) struct UntrackedStateRowValueArgs<'a> {
        pub(super) snapshot_content: Option<flatbuffers::WIPOffset<&'a str>>,
        pub(super) metadata: Option<flatbuffers::WIPOffset<&'a str>>,
        pub(super) created_at: flatbuffers::WIPOffset<&'a str>,
        pub(super) updated_at: flatbuffers::WIPOffset<&'a str>,
        pub(super) global: bool,
    }

    pub(super) fn create_untracked_state_row_value<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
        builder: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
        args: &'args UntrackedStateRowValueArgs<'args>,
    ) -> flatbuffers::WIPOffset<UntrackedStateRowValue<'bldr>> {
        let start = builder.start_table();
        builder.push_slot::<bool>(UntrackedStateRowValue::VT_GLOBAL, args.global, false);
        builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
            UntrackedStateRowValue::VT_UPDATED_AT,
            args.updated_at,
        );
        builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
            UntrackedStateRowValue::VT_CREATED_AT,
            args.created_at,
        );
        if let Some(metadata) = args.metadata {
            builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
                UntrackedStateRowValue::VT_METADATA,
                metadata,
            );
        }
        if let Some(snapshot_content) = args.snapshot_content {
            builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
                UntrackedStateRowValue::VT_SNAPSHOT_CONTENT,
                snapshot_content,
            );
        }
        let offset = builder.end_table(start);
        flatbuffers::WIPOffset::new(offset.value())
    }

    #[inline]
    pub(super) fn root_as_untracked_state_row_value(
        bytes: &[u8],
    ) -> Result<UntrackedStateRowValue<'_>, flatbuffers::InvalidFlatbuffer> {
        flatbuffers::root::<UntrackedStateRowValue>(bytes)
    }
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
}
