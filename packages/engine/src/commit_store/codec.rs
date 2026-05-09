use crate::commit_store::{
    Change, ChangeBorrowed, ChangeIndexEntry, ChangeIndexEntryBorrowed, ChangeLocator,
    ChangeLocatorBorrowed, Commit, StoredCommitBorrowed,
};
use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::LixError;

const CHANGE_FILE_IDENTIFIER: &str = "LXCH";
const COMMIT_MAGIC: &[u8; 5] = b"LXCM1";
const CHANGE_PACK_MAGIC: &[u8; 5] = b"LXCP1";
const MEMBERSHIP_PACK_MAGIC: &[u8; 5] = b"LXMP1";
const CHANGE_INDEX_MAGIC: &[u8; 5] = b"LXCI1";
const CHANGE_INDEX_COMMIT_HEADER: u8 = 1;
const CHANGE_INDEX_PACKED_CHANGE: u8 = 2;

pub(crate) fn encode_commit_borrowed(
    commit: StoredCommitBorrowed<'_>,
) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(COMMIT_MAGIC);
    write_str(&mut bytes, commit.id)?;
    write_str(&mut bytes, commit.change_id)?;
    write_str(&mut bytes, commit.change_set_id)?;
    write_strs(&mut bytes, commit.parent_ids.iter().map(String::as_str))?;
    write_strs(
        &mut bytes,
        commit.author_account_ids.iter().map(String::as_str),
    )?;
    write_str(&mut bytes, commit.created_at)?;
    bytes.extend_from_slice(&commit.change_pack_count.to_le_bytes());
    bytes.extend_from_slice(&commit.membership_pack_count.to_le_bytes());
    Ok(bytes)
}

pub(crate) fn decode_commit(bytes: &[u8]) -> Result<Commit, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(COMMIT_MAGIC, "commit")?;
    let id = cursor.read_string("id")?;
    let change_id = cursor.read_string("change_id")?;
    let change_set_id = cursor.read_string("change_set_id")?;
    let parent_ids = cursor.read_strings("parent_ids")?;
    let author_account_ids = cursor.read_strings("author_account_ids")?;
    let created_at = cursor.read_string("created_at")?;
    let change_pack_count = cursor.read_u32("change_pack_count")?;
    let membership_pack_count = cursor.read_u32("membership_pack_count")?;
    cursor.expect_end("commit")?;
    Ok(Commit {
        id,
        change_id,
        change_set_id,
        parent_ids,
        author_account_ids,
        created_at,
        change_pack_count,
        membership_pack_count,
    })
}

pub(crate) fn encode_change_borrowed(change: ChangeBorrowed<'_>) -> Result<Vec<u8>, LixError> {
    let entity_id = change.entity_id.as_json_array_text().map_err(|error| {
        LixError::unknown(format!(
            "failed to encode commit-store change entity identity: {error}"
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
    builder.finish(root, Some(CHANGE_FILE_IDENTIFIER));
    Ok(builder.finished_data().to_vec())
}

pub(crate) fn decode_change(bytes: &[u8]) -> Result<Change, LixError> {
    if bytes.len() < flatbuffers::SIZE_UOFFSET + flatbuffers::FILE_IDENTIFIER_LENGTH
        || !flatbuffers::buffer_has_identifier(bytes, CHANGE_FILE_IDENTIFIER, false)
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "failed to decode commit-store change: invalid FlatBuffers file identifier",
        ));
    }

    let change = flatbuffer::root_as_canonical_change(bytes).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode commit-store change: {error}"),
        )
    })?;

    let entity_id = required_str(change.entity_id(), "entity_id")?;
    let entity_id = EntityIdentity::from_json_array_text(entity_id).map_err(|error| {
        LixError::unknown(format!(
            "failed to decode commit-store change entity identity: {error}"
        ))
    })?;

    Ok(Change {
        id: required_str(change.id(), "id")?.to_string(),
        entity_id,
        schema_key: required_str(change.schema_key(), "schema_key")?.to_string(),
        file_id: change.file_id().map(ToString::to_string),
        snapshot_ref: optional_json_ref(change.snapshot_ref(), "snapshot_ref")?,
        metadata_ref: optional_json_ref(change.metadata_ref(), "metadata_ref")?,
        created_at: required_str(change.created_at(), "created_at")?.to_string(),
    })
}

pub(crate) fn encode_change_pack<'a>(
    commit_id: &str,
    pack_id: u32,
    changes: impl IntoIterator<Item = ChangeBorrowed<'a>>,
) -> Result<Vec<u8>, LixError> {
    let changes = changes.into_iter().collect::<Vec<_>>();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CHANGE_PACK_MAGIC);
    write_str(&mut bytes, commit_id)?;
    bytes.extend_from_slice(&pack_id.to_le_bytes());
    write_len(&mut bytes, changes.len(), "change pack changes")?;
    for change in changes {
        write_bytes(&mut bytes, &encode_change_borrowed(change)?)?;
    }
    Ok(bytes)
}

pub(crate) fn decode_change_pack(bytes: &[u8]) -> Result<(String, u32, Vec<Change>), LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(CHANGE_PACK_MAGIC, "change pack")?;
    let commit_id = cursor.read_string("commit_id")?;
    let pack_id = cursor.read_u32("pack_id")?;
    let change_count = cursor.read_u32("change_count")? as usize;
    let mut changes = Vec::with_capacity(change_count);
    for _ in 0..change_count {
        changes.push(decode_change(cursor.read_bytes("change")?)?);
    }
    cursor.expect_end("change pack")?;
    Ok((commit_id, pack_id, changes))
}

pub(crate) fn encode_membership_pack<'a>(
    commit_id: &str,
    pack_id: u32,
    members: impl IntoIterator<Item = ChangeLocatorBorrowed<'a>>,
) -> Result<Vec<u8>, LixError> {
    let members = members.into_iter().collect::<Vec<_>>();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MEMBERSHIP_PACK_MAGIC);
    write_str(&mut bytes, commit_id)?;
    bytes.extend_from_slice(&pack_id.to_le_bytes());
    write_len(&mut bytes, members.len(), "membership pack members")?;
    for member in members {
        encode_locator(&mut bytes, member)?;
    }
    Ok(bytes)
}

pub(crate) fn decode_membership_pack(
    bytes: &[u8],
) -> Result<(String, u32, Vec<ChangeLocator>), LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(MEMBERSHIP_PACK_MAGIC, "membership pack")?;
    let commit_id = cursor.read_string("commit_id")?;
    let pack_id = cursor.read_u32("pack_id")?;
    let member_count = cursor.read_u32("member_count")? as usize;
    let mut members = Vec::with_capacity(member_count);
    for _ in 0..member_count {
        members.push(decode_locator(&mut cursor)?);
    }
    cursor.expect_end("membership pack")?;
    Ok((commit_id, pack_id, members))
}

pub(crate) fn encode_change_index_entry(
    entry: ChangeIndexEntryBorrowed<'_>,
) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CHANGE_INDEX_MAGIC);
    match entry {
        ChangeIndexEntryBorrowed::CommitHeader {
            commit_id,
            change_id,
        } => {
            bytes.push(CHANGE_INDEX_COMMIT_HEADER);
            write_str(&mut bytes, commit_id)?;
            write_str(&mut bytes, change_id)?;
        }
        ChangeIndexEntryBorrowed::PackedChange { locator } => {
            bytes.push(CHANGE_INDEX_PACKED_CHANGE);
            encode_locator(&mut bytes, locator)?;
        }
    }
    Ok(bytes)
}

pub(crate) fn decode_change_index_entry(bytes: &[u8]) -> Result<ChangeIndexEntry, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(CHANGE_INDEX_MAGIC, "change index entry")?;
    let tag = cursor.read_u8("change_index_tag")?;
    let entry = match tag {
        CHANGE_INDEX_COMMIT_HEADER => ChangeIndexEntry::CommitHeader {
            commit_id: cursor.read_string("commit_id")?,
            change_id: cursor.read_string("change_id")?,
        },
        CHANGE_INDEX_PACKED_CHANGE => ChangeIndexEntry::PackedChange {
            locator: decode_locator(&mut cursor)?,
        },
        _ => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store change index entry: unknown tag {tag}"),
            ));
        }
    };
    cursor.expect_end("change index entry")?;
    Ok(entry)
}

fn encode_locator(bytes: &mut Vec<u8>, locator: ChangeLocatorBorrowed<'_>) -> Result<(), LixError> {
    write_str(bytes, locator.source_commit_id)?;
    bytes.extend_from_slice(&locator.source_pack_id.to_le_bytes());
    bytes.extend_from_slice(&locator.source_ordinal.to_le_bytes());
    write_str(bytes, locator.change_id)
}

fn decode_locator(cursor: &mut ByteCursor<'_>) -> Result<ChangeLocator, LixError> {
    Ok(ChangeLocator {
        source_commit_id: cursor.read_string("source_commit_id")?,
        source_pack_id: cursor.read_u32("source_pack_id")?,
        source_ordinal: cursor.read_u32("source_ordinal")?,
        change_id: cursor.read_string("change_id")?,
    })
}

fn write_str(bytes: &mut Vec<u8>, value: &str) -> Result<(), LixError> {
    let len = u32::try_from(value.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "commit-store string field exceeds u32 length",
        )
    })?;
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), LixError> {
    write_len(bytes, value.len(), "bytes field")?;
    bytes.extend_from_slice(value);
    Ok(())
}

fn write_len(bytes: &mut Vec<u8>, len: usize, field: &str) -> Result<(), LixError> {
    let len = u32::try_from(len).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("commit-store {field} exceeds u32 length"),
        )
    })?;
    bytes.extend_from_slice(&len.to_le_bytes());
    Ok(())
}

fn write_strs<'a>(
    bytes: &mut Vec<u8>,
    values: impl IntoIterator<Item = &'a str>,
) -> Result<(), LixError> {
    let values = values.into_iter().collect::<Vec<_>>();
    let len = u32::try_from(values.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "commit-store string vector field exceeds u32 length",
        )
    })?;
    bytes.extend_from_slice(&len.to_le_bytes());
    for value in values {
        write_str(bytes, value)?;
    }
    Ok(())
}

struct ByteCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_magic(&mut self, magic: &[u8], label: &str) -> Result<(), LixError> {
        if self.bytes.len() < magic.len() || &self.bytes[..magic.len()] != magic {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store {label}: invalid magic"),
            ));
        }
        self.offset = magic.len();
        Ok(())
    }

    fn read_string(&mut self, field: &str) -> Result<String, LixError> {
        let len = self.read_u32(field)? as usize;
        let end = self.offset.checked_add(len).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store field `{field}`: length overflow"),
            )
        })?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store field `{field}`: truncated string"),
            )
        })?;
        self.offset = end;
        String::from_utf8(bytes.to_vec()).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store field `{field}` as UTF-8: {error}"),
            )
        })
    }

    fn read_strings(&mut self, field: &str) -> Result<Vec<String>, LixError> {
        let count = self.read_u32(field)? as usize;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(self.read_string(field)?);
        }
        Ok(values)
    }

    fn read_bytes(&mut self, field: &str) -> Result<&'a [u8], LixError> {
        let len = self.read_u32(field)? as usize;
        let end = self.offset.checked_add(len).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store field `{field}`: length overflow"),
            )
        })?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store field `{field}`: truncated bytes"),
            )
        })?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self, field: &str) -> Result<u8, LixError> {
        let byte = self.bytes.get(self.offset).copied().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store field `{field}`: truncated u8"),
            )
        })?;
        self.offset += 1;
        Ok(byte)
    }

    fn read_u32(&mut self, field: &str) -> Result<u32, LixError> {
        let end = self.offset.checked_add(4).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store field `{field}`: offset overflow"),
            )
        })?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store field `{field}`: truncated u32"),
            )
        })?;
        self.offset = end;
        Ok(u32::from_le_bytes(
            bytes
                .try_into()
                .expect("slice length was checked before u32 decode"),
        ))
    }

    fn expect_end(&self, label: &str) -> Result<(), LixError> {
        if self.offset != self.bytes.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store {label}: trailing bytes"),
            ));
        }
        Ok(())
    }
}

fn required_str<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str, LixError> {
    value.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode commit-store change: missing required field `{field}`"),
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
                "failed to decode commit-store change: field `{field}` must be exactly 32 bytes"
            ),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_codec_roundtrips() {
        let commit = Commit {
            id: "commit-1".to_string(),
            change_id: "commit-change-1".to_string(),
            change_set_id: "change-set-1".to_string(),
            parent_ids: vec!["parent-1".to_string(), "parent-2".to_string()],
            author_account_ids: vec!["author-1".to_string()],
            created_at: "2026-01-01T00:00:00Z".to_string(),
            change_pack_count: 2,
            membership_pack_count: 1,
        };

        let encoded = encode_commit_borrowed(commit.as_borrowed()).expect("commit should encode");
        let decoded = decode_commit(&encoded).expect("commit should decode");

        assert_eq!(decoded, commit);
    }

    #[test]
    fn change_codec_roundtrips() {
        let change = Change {
            id: "change-1".to_string(),
            entity_id: EntityIdentity::single("entity-1"),
            schema_key: "test_schema".to_string(),
            file_id: Some("file-1".to_string()),
            snapshot_ref: Some(JsonRef::from_hash_bytes([1; 32])),
            metadata_ref: Some(JsonRef::from_hash_bytes([2; 32])),
            created_at: "2026-01-01T00:00:00Z".to_string(),
        };

        let encoded = encode_change_borrowed(change.as_borrowed()).expect("change should encode");
        let decoded = decode_change(&encoded).expect("change should decode");

        assert_eq!(decoded, change);
    }
}
