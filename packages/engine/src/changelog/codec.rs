use crate::changelog::{CanonicalChange, CanonicalChangeRef};
use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::LixError;

const CHANGELOG_FILE_IDENTIFIER: &str = "LXCH";

pub(crate) fn encode_change_ref(change: CanonicalChangeRef<'_>) -> Result<Vec<u8>, LixError> {
    let entity_id = change.entity_id.as_json_array_text().map_err(|error| {
        LixError::unknown(format!(
            "failed to encode changelog entity identity: {error}"
        ))
    })?;

    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(256);
    let id = builder.create_string(change.id);
    let entity_id = builder.create_string(&entity_id);
    let schema_key = builder.create_string(change.schema_key);
    let file_id = change.file_id.map(|value| builder.create_string(value));
    let snapshot_ref = change
        .snapshot_ref
        .map(|value| builder.create_vector(value.as_hash_bytes()));
    let metadata_ref = change
        .metadata_ref
        .map(|value| builder.create_vector(value.as_hash_bytes()));
    let created_at = builder.create_string(change.created_at);

    let root = flatbuffer::create_canonical_change(
        &mut builder,
        &flatbuffer::CanonicalChangeArgs {
            id,
            entity_id,
            schema_key,
            file_id,
            snapshot_ref,
            metadata_ref,
            created_at,
        },
    );
    builder.finish(root, Some(CHANGELOG_FILE_IDENTIFIER));
    Ok(builder.finished_data().to_vec())
}

pub(crate) fn decode_change(bytes: &[u8]) -> Result<CanonicalChange, LixError> {
    if bytes.len() < flatbuffers::SIZE_UOFFSET + flatbuffers::FILE_IDENTIFIER_LENGTH
        || !flatbuffers::buffer_has_identifier(bytes, CHANGELOG_FILE_IDENTIFIER, false)
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode changelog change: invalid FlatBuffers file identifier",
        ));
    }

    let change = flatbuffer::root_as_canonical_change(bytes).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode changelog change: {error}"),
        )
    })?;

    let entity_id = required_str(change.entity_id(), "entity_id")?;
    let entity_id = EntityIdentity::from_json_array_text(entity_id).map_err(|error| {
        LixError::unknown(format!(
            "failed to decode changelog entity identity: {error}"
        ))
    })?;

    Ok(CanonicalChange {
        id: required_str(change.id(), "id")?.to_string(),
        entity_id,
        schema_key: required_str(change.schema_key(), "schema_key")?.to_string(),
        file_id: change.file_id().map(ToString::to_string),
        snapshot_ref: optional_json_ref(change.snapshot_ref(), "snapshot_ref")?,
        metadata_ref: optional_json_ref(change.metadata_ref(), "metadata_ref")?,
        created_at: required_str(change.created_at(), "created_at")?.to_string(),
    })
}

fn required_str<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str, LixError> {
    value.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode changelog change: missing required field `{field}`"),
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
            format!("failed to decode changelog change: field `{field}` must be exactly 32 bytes"),
        )
    })?;
    Ok(Some(JsonRef::from_hash_bytes(hash)))
}

mod flatbuffer {
    #[derive(Copy, Clone, PartialEq)]
    pub(super) struct CanonicalChange<'a> {
        table: flatbuffers::Table<'a>,
    }

    impl<'a> flatbuffers::Follow<'a> for CanonicalChange<'a> {
        type Inner = CanonicalChange<'a>;

        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            Self {
                table: unsafe { flatbuffers::Table::new(buf, loc) },
            }
        }
    }

    impl<'a> CanonicalChange<'a> {
        const VT_ID: flatbuffers::VOffsetT = 4;
        const VT_ENTITY_ID: flatbuffers::VOffsetT = 6;
        const VT_SCHEMA_KEY: flatbuffers::VOffsetT = 8;
        const VT_FILE_ID: flatbuffers::VOffsetT = 10;
        const VT_SNAPSHOT_REF: flatbuffers::VOffsetT = 12;
        const VT_METADATA_REF: flatbuffers::VOffsetT = 14;
        const VT_CREATED_AT: flatbuffers::VOffsetT = 16;

        #[inline]
        pub(super) fn id(&self) -> Option<&'a str> {
            unsafe {
                self.table
                    .get::<flatbuffers::ForwardsUOffset<&str>>(Self::VT_ID, None)
            }
        }

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

        #[inline]
        pub(super) fn created_at(&self) -> Option<&'a str> {
            unsafe {
                self.table
                    .get::<flatbuffers::ForwardsUOffset<&str>>(Self::VT_CREATED_AT, None)
            }
        }
    }

    impl flatbuffers::Verifiable for CanonicalChange<'_> {
        #[inline]
        fn run_verifier(
            verifier: &mut flatbuffers::Verifier,
            position: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            verifier
                .visit_table(position)?
                .visit_field::<flatbuffers::ForwardsUOffset<&str>>("id", Self::VT_ID, true)?
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
                .finish();
            Ok(())
        }
    }

    pub(super) struct CanonicalChangeArgs<'a> {
        pub(super) id: flatbuffers::WIPOffset<&'a str>,
        pub(super) entity_id: flatbuffers::WIPOffset<&'a str>,
        pub(super) schema_key: flatbuffers::WIPOffset<&'a str>,
        pub(super) file_id: Option<flatbuffers::WIPOffset<&'a str>>,
        pub(super) snapshot_ref: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
        pub(super) metadata_ref: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
        pub(super) created_at: flatbuffers::WIPOffset<&'a str>,
    }

    pub(super) fn create_canonical_change<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
        builder: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
        args: &'args CanonicalChangeArgs<'args>,
    ) -> flatbuffers::WIPOffset<CanonicalChange<'bldr>> {
        let start = builder.start_table();
        builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
            CanonicalChange::VT_CREATED_AT,
            args.created_at,
        );
        if let Some(metadata_ref) = args.metadata_ref {
            builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
                CanonicalChange::VT_METADATA_REF,
                metadata_ref,
            );
        }
        if let Some(snapshot_ref) = args.snapshot_ref {
            builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
                CanonicalChange::VT_SNAPSHOT_REF,
                snapshot_ref,
            );
        }
        if let Some(file_id) = args.file_id {
            builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
                CanonicalChange::VT_FILE_ID,
                file_id,
            );
        }
        builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
            CanonicalChange::VT_SCHEMA_KEY,
            args.schema_key,
        );
        builder.push_slot_always::<flatbuffers::WIPOffset<_>>(
            CanonicalChange::VT_ENTITY_ID,
            args.entity_id,
        );
        builder.push_slot_always::<flatbuffers::WIPOffset<_>>(CanonicalChange::VT_ID, args.id);
        let offset = builder.end_table(start);
        flatbuffers::WIPOffset::new(offset.value())
    }

    #[inline]
    pub(super) fn root_as_canonical_change(
        bytes: &[u8],
    ) -> Result<CanonicalChange<'_>, flatbuffers::InvalidFlatbuffer> {
        flatbuffers::root::<CanonicalChange>(bytes)
    }
}
