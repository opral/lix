use crate::commit_store::{
    Change, ChangeLocator, ChangeLocatorRef, ChangeRef, Commit, StoredCommitRef,
};
use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::LixError;

const COMMIT_MAGIC: &[u8; 5] = b"LXCM1";
const CHANGE_MAGIC: &[u8; 5] = b"LXCH2";
const CHANGE_PACK_MAGIC: &[u8; 5] = b"LXCP3";
const MEMBERSHIP_PACK_MAGIC: &[u8; 5] = b"LXMP1";
const CHANGE_ID_FULL: u8 = 0;
const CHANGE_ID_COMMIT_SUFFIX: u8 = 1;

pub(crate) fn encode_commit_ref(commit: StoredCommitRef<'_>) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(COMMIT_MAGIC);
    write_str(&mut bytes, commit.id)?;
    write_str(&mut bytes, commit.change_id)?;
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
    let parent_ids = cursor.read_strings("parent_ids")?;
    let author_account_ids = cursor.read_strings("author_account_ids")?;
    let created_at = cursor.read_string("created_at")?;
    let change_pack_count = cursor.read_u32("change_pack_count")?;
    let membership_pack_count = cursor.read_u32("membership_pack_count")?;
    cursor.expect_end("commit")?;
    Ok(Commit {
        id,
        change_id,
        parent_ids,
        author_account_ids,
        created_at,
        change_pack_count,
        membership_pack_count,
    })
}

pub(crate) fn encode_change_ref(change: ChangeRef<'_>) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    write_change_ref(&mut bytes, change)?;
    Ok(bytes)
}

fn write_change_ref(bytes: &mut Vec<u8>, change: ChangeRef<'_>) -> Result<(), LixError> {
    let entity_id = change.entity_id.as_json_array_text().map_err(|error| {
        LixError::unknown(format!(
            "failed to encode commit-store change entity identity: {error}"
        ))
    })?;

    bytes.extend_from_slice(CHANGE_MAGIC);
    write_str(bytes, change.id)?;
    write_str(bytes, &entity_id)?;
    write_str(bytes, change.schema_key)?;
    write_optional_str(bytes, change.file_id)?;
    write_optional_json_ref(bytes, change.snapshot_ref);
    write_optional_json_ref(bytes, change.metadata_ref);
    write_str(bytes, change.created_at)
}

pub(crate) fn decode_change(bytes: &[u8]) -> Result<Change, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(CHANGE_MAGIC, "change")?;
    let id = cursor.read_string("id")?;
    let entity_id = cursor.read_string("entity_id")?;
    let entity_id = EntityIdentity::from_json_array_text(&entity_id).map_err(|error| {
        LixError::unknown(format!(
            "failed to decode commit-store change entity identity: {error}"
        ))
    })?;
    let schema_key = cursor.read_string("schema_key")?;
    let file_id = cursor.read_optional_string("file_id")?;
    let snapshot_ref = cursor.read_optional_json_ref("snapshot_ref")?;
    let metadata_ref = cursor.read_optional_json_ref("metadata_ref")?;
    let created_at = cursor.read_string("created_at")?;
    cursor.expect_end("change")?;
    Ok(Change {
        id,
        entity_id,
        schema_key,
        file_id,
        snapshot_ref,
        metadata_ref,
        created_at,
    })
}

pub(crate) fn encode_change_pack(
    commit_id: &str,
    pack_id: u32,
    changes: &[ChangeRef<'_>],
) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CHANGE_PACK_MAGIC);
    write_var_str(&mut bytes, commit_id, "change pack commit_id")?;
    bytes.extend_from_slice(&pack_id.to_le_bytes());
    let (shapes, change_shape_indexes) = change_shapes(changes);
    write_var_len(&mut bytes, shapes.len(), "change pack shapes")?;
    for shape in &shapes {
        write_var_str(&mut bytes, shape.schema_key, "schema_key")?;
        write_optional_var_str(&mut bytes, shape.file_id, "file_id")?;
    }
    write_var_len(&mut bytes, changes.len(), "change pack changes")?;
    for (change, shape_index) in changes.iter().copied().zip(change_shape_indexes) {
        write_var_change_id(&mut bytes, commit_id, change.id)?;
        write_var_entity_identity(&mut bytes, change.entity_id)?;
        write_var_len(&mut bytes, shape_index, "change shape index")?;
        write_optional_json_ref(&mut bytes, change.snapshot_ref);
        write_optional_json_ref(&mut bytes, change.metadata_ref);
        write_var_str(&mut bytes, change.created_at, "created_at")?;
    }
    Ok(bytes)
}

pub(crate) fn decode_change_pack(bytes: &[u8]) -> Result<(String, u32, Vec<Change>), LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(CHANGE_PACK_MAGIC, "change pack")?;
    let commit_id = cursor.read_var_string("commit_id")?;
    let pack_id = cursor.read_u32("pack_id")?;
    let shape_count = cursor.read_var_usize("shape_count")?;
    let mut shapes = Vec::with_capacity(shape_count);
    for _ in 0..shape_count {
        shapes.push(ChangeShape {
            schema_key: cursor.read_var_string("schema_key")?,
            file_id: cursor.read_optional_var_string("file_id")?,
        });
    }
    let change_count = cursor.read_var_usize("change_count")?;
    let mut changes = Vec::with_capacity(change_count);
    for _ in 0..change_count {
        let id = cursor.read_var_change_id(&commit_id)?;
        let entity_id = cursor.read_var_entity_identity()?;
        let shape_index = cursor.read_var_usize("shape_index")?;
        let shape = shapes.get(shape_index).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store change pack: shape index {shape_index} is out of bounds"),
            )
        })?;
        let snapshot_ref = cursor.read_optional_json_ref("snapshot_ref")?;
        let metadata_ref = cursor.read_optional_json_ref("metadata_ref")?;
        let created_at = cursor.read_var_string("created_at")?;
        changes.push(Change {
            id,
            entity_id,
            schema_key: shape.schema_key.clone(),
            file_id: shape.file_id.clone(),
            snapshot_ref,
            metadata_ref,
            created_at,
        });
    }
    cursor.expect_end("change pack")?;
    Ok((commit_id, pack_id, changes))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChangeShapeRef<'a> {
    schema_key: &'a str,
    file_id: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChangeShape {
    schema_key: String,
    file_id: Option<String>,
}

fn change_shapes<'a>(changes: &'a [ChangeRef<'a>]) -> (Vec<ChangeShapeRef<'a>>, Vec<usize>) {
    let mut shapes = Vec::new();
    let mut shape_indexes = Vec::with_capacity(changes.len());
    for change in changes {
        let shape = ChangeShapeRef {
            schema_key: change.schema_key,
            file_id: change.file_id,
        };
        let shape_index = match shapes.iter().position(|candidate| *candidate == shape) {
            Some(shape_index) => shape_index,
            None => {
                let shape_index = shapes.len();
                shapes.push(shape);
                shape_index
            }
        };
        shape_indexes.push(shape_index);
    }
    (shapes, shape_indexes)
}

pub(crate) fn encode_membership_pack<'a>(
    commit_id: &str,
    pack_id: u32,
    members: impl IntoIterator<Item = ChangeLocatorRef<'a>>,
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

fn encode_locator(bytes: &mut Vec<u8>, locator: ChangeLocatorRef<'_>) -> Result<(), LixError> {
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

fn write_optional_str(bytes: &mut Vec<u8>, value: Option<&str>) -> Result<(), LixError> {
    match value {
        Some(value) => {
            bytes.push(1);
            write_str(bytes, value)?;
        }
        None => bytes.push(0),
    }
    Ok(())
}

fn write_optional_json_ref(bytes: &mut Vec<u8>, value: Option<&JsonRef>) {
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(value.as_hash_bytes());
        }
        None => bytes.push(0),
    }
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

fn write_var_len(bytes: &mut Vec<u8>, len: usize, field: &str) -> Result<(), LixError> {
    let mut value = u32::try_from(len).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("commit-store {field} exceeds u32 length"),
        )
    })?;
    while value >= 0x80 {
        bytes.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    bytes.push(value as u8);
    Ok(())
}

fn write_var_str(bytes: &mut Vec<u8>, value: &str, field: &str) -> Result<(), LixError> {
    write_var_len(bytes, value.len(), field)?;
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_optional_var_str(
    bytes: &mut Vec<u8>,
    value: Option<&str>,
    field: &str,
) -> Result<(), LixError> {
    match value {
        Some(value) => {
            bytes.push(1);
            write_var_str(bytes, value, field)?;
        }
        None => bytes.push(0),
    }
    Ok(())
}

fn write_change_id(bytes: &mut Vec<u8>, commit_id: &str, change_id: &str) -> Result<(), LixError> {
    if let Some(suffix) = change_id.strip_prefix(commit_id) {
        bytes.push(CHANGE_ID_COMMIT_SUFFIX);
        write_str(bytes, suffix)
    } else {
        bytes.push(CHANGE_ID_FULL);
        write_str(bytes, change_id)
    }
}

fn write_var_change_id(
    bytes: &mut Vec<u8>,
    commit_id: &str,
    change_id: &str,
) -> Result<(), LixError> {
    if let Some(suffix) = change_id.strip_prefix(commit_id) {
        bytes.push(CHANGE_ID_COMMIT_SUFFIX);
        write_var_str(bytes, suffix, "change_id")
    } else {
        bytes.push(CHANGE_ID_FULL);
        write_var_str(bytes, change_id, "change_id")
    }
}

fn write_entity_identity(bytes: &mut Vec<u8>, identity: &EntityIdentity) -> Result<(), LixError> {
    write_len(
        bytes,
        identity.parts.len(),
        "commit-store entity identity parts",
    )?;
    for part in &identity.parts {
        write_str(bytes, part)?;
    }
    Ok(())
}

fn write_var_entity_identity(
    bytes: &mut Vec<u8>,
    identity: &EntityIdentity,
) -> Result<(), LixError> {
    write_var_len(
        bytes,
        identity.parts.len(),
        "commit-store entity identity parts",
    )?;
    for part in &identity.parts {
        write_var_str(bytes, part, "entity identity part")?;
    }
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

    fn read_optional_string(&mut self, field: &str) -> Result<Option<String>, LixError> {
        match self.read_u8(field)? {
            0 => Ok(None),
            1 => self.read_string(field).map(Some),
            tag => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store field `{field}`: invalid option tag {tag}"),
            )),
        }
    }

    fn read_optional_json_ref(&mut self, field: &str) -> Result<Option<JsonRef>, LixError> {
        match self.read_u8(field)? {
            0 => Ok(None),
            1 => {
                let end = self.offset.checked_add(32).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("failed to decode commit-store field `{field}`: offset overflow"),
                    )
                })?;
                let bytes = self.bytes.get(self.offset..end).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("failed to decode commit-store field `{field}`: truncated ref"),
                    )
                })?;
                self.offset = end;
                let hash = <[u8; 32]>::try_from(bytes).expect("json ref length was checked");
                Ok(Some(JsonRef::from_hash_bytes(hash)))
            }
            tag => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store field `{field}`: invalid option tag {tag}"),
            )),
        }
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

    fn read_var_usize(&mut self, field: &str) -> Result<usize, LixError> {
        let mut value = 0u32;
        let mut shift = 0u32;
        for byte_index in 0..5 {
            let byte = self.read_u8(field)?;
            if shift == 28 && (byte & 0x80 != 0 || byte & 0x70 != 0) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("failed to decode commit-store field `{field}`: varint exceeds u32"),
                ));
            }
            if byte_index > 0 && byte & 0x80 == 0 && byte == 0 {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("failed to decode commit-store field `{field}`: non-canonical varint"),
                ));
            }
            value |= ((byte & 0x7f) as u32) << shift;
            if byte & 0x80 == 0 {
                return Ok(value as usize);
            }
            shift += 7;
        }
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to decode commit-store field `{field}`: varint exceeds u32"),
        ))
    }

    fn read_var_string(&mut self, field: &str) -> Result<String, LixError> {
        let len = self.read_var_usize(field)?;
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

    fn read_optional_var_string(&mut self, field: &str) -> Result<Option<String>, LixError> {
        match self.read_u8(field)? {
            0 => Ok(None),
            1 => self.read_var_string(field).map(Some),
            tag => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store field `{field}`: invalid option tag {tag}"),
            )),
        }
    }

    fn read_change_id(&mut self, commit_id: &str) -> Result<String, LixError> {
        let tag = self.read_u8("change_id tag")?;
        let value = self.read_string("change_id")?;
        match tag {
            CHANGE_ID_FULL => Ok(value),
            CHANGE_ID_COMMIT_SUFFIX => Ok(format!("{commit_id}{value}")),
            tag => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store field `change_id`: invalid tag {tag}"),
            )),
        }
    }

    fn read_var_change_id(&mut self, commit_id: &str) -> Result<String, LixError> {
        let tag = self.read_u8("change_id tag")?;
        let value = self.read_var_string("change_id")?;
        match tag {
            CHANGE_ID_FULL => Ok(value),
            CHANGE_ID_COMMIT_SUFFIX => Ok(format!("{commit_id}{value}")),
            tag => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode commit-store field `change_id`: invalid tag {tag}"),
            )),
        }
    }

    fn read_entity_identity(&mut self) -> Result<EntityIdentity, LixError> {
        let count = self.read_u32("entity identity part count")? as usize;
        let mut parts = Vec::with_capacity(count);
        for _ in 0..count {
            parts.push(self.read_string("entity identity part")?);
        }
        if parts.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to decode commit-store entity identity: empty identity",
            ));
        }
        Ok(EntityIdentity { parts })
    }

    fn read_var_entity_identity(&mut self) -> Result<EntityIdentity, LixError> {
        let count = self.read_var_usize("entity identity part count")?;
        let mut parts = Vec::with_capacity(count);
        for _ in 0..count {
            parts.push(self.read_var_string("entity identity part")?);
        }
        if parts.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to decode commit-store entity identity: empty identity",
            ));
        }
        Ok(EntityIdentity { parts })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_codec_roundtrips() {
        let commit = Commit {
            id: "commit-1".to_string(),
            change_id: "commit-change-1".to_string(),
            parent_ids: vec!["parent-1".to_string(), "parent-2".to_string()],
            author_account_ids: vec!["author-1".to_string()],
            created_at: "2026-01-01T00:00:00Z".to_string(),
            change_pack_count: 2,
            membership_pack_count: 1,
        };

        let encoded = encode_commit_ref(commit.as_ref()).expect("commit should encode");
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

        let encoded = encode_change_ref(change.as_ref()).expect("change should encode");
        let decoded = decode_change(&encoded).expect("change should decode");

        assert_eq!(decoded, change);
    }

    #[test]
    fn change_codec_roundtrips_empty_optionals() {
        let change = Change {
            id: "change-1".to_string(),
            entity_id: EntityIdentity::single("entity-1"),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot_ref: None,
            metadata_ref: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
        };

        let encoded = encode_change_ref(change.as_ref()).expect("change should encode");
        let decoded = decode_change(&encoded).expect("change should decode");

        assert_eq!(decoded, change);
    }

    #[test]
    fn change_pack_compacts_shared_shape_and_commit_id_prefix() {
        let changes = [
            Change {
                id: "commit-1:change-1".to_string(),
                entity_id: EntityIdentity::single("entity-1"),
                schema_key: "test_schema".to_string(),
                file_id: Some("file-1".to_string()),
                snapshot_ref: Some(JsonRef::from_hash_bytes([1; 32])),
                metadata_ref: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
            },
            Change {
                id: "external-change".to_string(),
                entity_id: EntityIdentity::single("entity-2"),
                schema_key: "test_schema".to_string(),
                file_id: Some("file-1".to_string()),
                snapshot_ref: None,
                metadata_ref: Some(JsonRef::from_hash_bytes([2; 32])),
                created_at: "2026-01-02T00:00:00Z".to_string(),
            },
        ];

        let encoded = encode_change_pack(
            "commit-1",
            7,
            &changes.iter().map(Change::as_ref).collect::<Vec<_>>(),
        )
        .expect("pack should encode");
        let (commit_id, pack_id, decoded) =
            decode_change_pack(&encoded).expect("pack should decode");

        assert_eq!(commit_id, "commit-1");
        assert_eq!(pack_id, 7);
        assert_eq!(decoded, changes);

        let mut cursor = ByteCursor::new(&encoded);
        cursor
            .expect_magic(CHANGE_PACK_MAGIC, "change pack")
            .unwrap();
        assert_eq!(cursor.read_var_string("commit_id").unwrap(), "commit-1");
        assert_eq!(cursor.read_u32("pack_id").unwrap(), 7);
        assert_eq!(cursor.read_var_usize("shape_count").unwrap(), 1);
        assert_eq!(cursor.read_var_string("schema_key").unwrap(), "test_schema");
        assert_eq!(
            cursor
                .read_optional_var_string("file_id")
                .unwrap()
                .as_deref(),
            Some("file-1")
        );
        assert_eq!(cursor.read_var_usize("change_count").unwrap(), 2);
        assert_eq!(
            cursor.read_u8("change_id tag").unwrap(),
            CHANGE_ID_COMMIT_SUFFIX
        );
        assert_eq!(cursor.read_var_string("change_id").unwrap(), ":change-1");
    }

    #[test]
    fn change_pack_rejects_overlong_varint() {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(CHANGE_PACK_MAGIC);
        encoded.extend_from_slice(&[0x80, 0x80, 0x80, 0x80, 0x80]);

        let error = decode_change_pack(&encoded).expect_err("overlong varint should reject");
        assert!(
            error.to_string().contains("varint exceeds u32"),
            "error should mention overlong varint: {error}"
        );
    }

    #[test]
    fn change_pack_rejects_varint_above_u32() {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(CHANGE_PACK_MAGIC);
        encoded.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0x1f]);

        let error = decode_change_pack(&encoded).expect_err("too-large varint should reject");
        assert!(
            error.to_string().contains("varint exceeds u32"),
            "error should mention oversized varint: {error}"
        );
    }

    #[test]
    fn change_pack_rejects_non_canonical_varint() {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(CHANGE_PACK_MAGIC);
        encoded.extend_from_slice(&[0x80, 0x00]);

        let error = decode_change_pack(&encoded).expect_err("non-canonical varint should reject");
        assert!(
            error.to_string().contains("non-canonical varint"),
            "error should mention non-canonical varint: {error}"
        );
    }

    #[test]
    fn change_codec_rejects_invalid_optional_tag() {
        let change = Change {
            id: "change-1".to_string(),
            entity_id: EntityIdentity::single("entity-1"),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot_ref: None,
            metadata_ref: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
        };
        let mut encoded = encode_change_ref(change.as_ref()).expect("change should encode");
        let mut cursor = ByteCursor::new(&encoded);
        cursor.expect_magic(CHANGE_MAGIC, "change").unwrap();
        cursor.read_string("id").unwrap();
        cursor.read_string("entity_id").unwrap();
        cursor.read_string("schema_key").unwrap();
        let file_tag_offset = cursor.offset;
        encoded[file_tag_offset] = 2;

        let error = decode_change(&encoded).expect_err("invalid optional tag should fail");
        assert!(
            error.to_string().contains("invalid option tag"),
            "error should mention invalid tag: {error}"
        );
    }

    #[test]
    fn change_codec_rejects_truncated_json_ref() {
        let change = Change {
            id: "change-1".to_string(),
            entity_id: EntityIdentity::single("entity-1"),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot_ref: Some(JsonRef::from_hash_bytes([1; 32])),
            metadata_ref: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
        };
        let mut encoded = encode_change_ref(change.as_ref()).expect("change should encode");
        let mut cursor = ByteCursor::new(&encoded);
        cursor.expect_magic(CHANGE_MAGIC, "change").unwrap();
        cursor.read_string("id").unwrap();
        cursor.read_string("entity_id").unwrap();
        cursor.read_string("schema_key").unwrap();
        cursor.read_optional_string("file_id").unwrap();
        cursor.read_u8("snapshot_ref").unwrap();
        encoded.truncate(cursor.offset + 16);

        let error = decode_change(&encoded).expect_err("truncated ref should fail");
        assert!(
            error.to_string().contains("truncated ref"),
            "error should mention truncation: {error}"
        );
    }

    #[test]
    fn change_codec_rejects_trailing_bytes() {
        let change = Change {
            id: "change-1".to_string(),
            entity_id: EntityIdentity::single("entity-1"),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot_ref: None,
            metadata_ref: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
        };
        let mut encoded = encode_change_ref(change.as_ref()).expect("change should encode");
        encoded.push(0);

        let error = decode_change(&encoded).expect_err("trailing bytes should fail");
        assert!(
            error.to_string().contains("trailing bytes"),
            "error should mention trailing bytes: {error}"
        );
    }
}
