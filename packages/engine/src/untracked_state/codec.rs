use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::untracked_state::{UntrackedStateRow, UntrackedStateRowRef};
use crate::LixError;

const UNTRACKED_STATE_FILE_IDENTIFIER: &str = "LXUS";

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
    let snapshot_ref = row
        .snapshot_ref
        .map(|value| builder.create_vector(value.as_hash_bytes()));
    let metadata_ref = row
        .metadata_ref
        .map(|value| builder.create_vector(value.as_hash_bytes()));
    let created_at = builder.create_string(row.created_at);
    let updated_at = builder.create_string(row.updated_at);
    let version_id = builder.create_string(row.version_id);

    let root = flatbuffer::create_untracked_state_row(
        &mut builder,
        &flatbuffer::UntrackedStateRowArgs {
            entity_id,
            schema_key,
            file_id,
            snapshot_ref,
            metadata_ref,
            created_at,
            updated_at,
            global: row.global,
            version_id,
        },
    );
    builder.finish(root, Some(UNTRACKED_STATE_FILE_IDENTIFIER));
    Ok(builder.finished_data().to_vec())
}

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
        snapshot_ref: optional_json_ref(row.snapshot_ref(), "snapshot_ref")?,
        metadata_ref: optional_json_ref(row.metadata_ref(), "metadata_ref")?,
        created_at: required_str(row.created_at(), "created_at")?.to_string(),
        updated_at: required_str(row.updated_at(), "updated_at")?.to_string(),
        global: row.global(),
        version_id: required_str(row.version_id(), "version_id")?.to_string(),
    })
}

fn required_str<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str, LixError> {
    value.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode untracked-state row: missing required field `{field}`"),
        )
    })
}

fn optional_json_ref(
    value: Option<flatbuffers::Vector<'_, u8>>,
    field: &str,
) -> Result<Option<JsonRef>, LixError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let bytes = value.bytes();
    let hash = <[u8; 32]>::try_from(bytes).map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "failed to decode untracked-state row: field `{field}` must be exactly 32 bytes"
            ),
        )
    })?;
    Ok(Some(JsonRef::from_hash_bytes(hash)))
}

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
        const VT_SNAPSHOT_REF: flatbuffers::VOffsetT = 10;
        const VT_METADATA_REF: flatbuffers::VOffsetT = 12;
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
        pub(super) fn snapshot_ref(&self) -> Option<flatbuffers::Vector<'a, u8>> {
            unsafe {
                self.table
                    .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(
                        Self::VT_SNAPSHOT_REF,
                        None,
                    )
            }
        }

        #[inline]
        pub(super) fn metadata_ref(&self) -> Option<flatbuffers::Vector<'a, u8>> {
            unsafe {
                self.table
                    .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(
                        Self::VT_METADATA_REF,
                        None,
                    )
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
                .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(
                    "snapshot_ref",
                    Self::VT_SNAPSHOT_REF,
                    false,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(
                    "metadata_ref",
                    Self::VT_METADATA_REF,
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

    pub(super) struct UntrackedStateRowArgs<'a> {
        pub(super) entity_id: flatbuffers::WIPOffset<&'a str>,
        pub(super) schema_key: flatbuffers::WIPOffset<&'a str>,
        pub(super) file_id: Option<flatbuffers::WIPOffset<&'a str>>,
        pub(super) snapshot_ref: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
        pub(super) metadata_ref: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
        pub(super) created_at: flatbuffers::WIPOffset<&'a str>,
        pub(super) updated_at: flatbuffers::WIPOffset<&'a str>,
        pub(super) global: bool,
        pub(super) version_id: flatbuffers::WIPOffset<&'a str>,
    }

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
        if let Some(metadata_ref) = args.metadata_ref {
            builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
                UntrackedStateRow::VT_METADATA_REF,
                metadata_ref,
            );
        }
        if let Some(snapshot_ref) = args.snapshot_ref {
            builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
                UntrackedStateRow::VT_SNAPSHOT_REF,
                snapshot_ref,
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
