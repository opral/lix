use super::types::{
    ChangeRecord, ChangeRecordView, CommitChangeRef, CommitChangeRefChunk,
    CommitChangeRefChunkView, CommitChangeRefView, CommitRecord, CommitRecordView,
    EntityIdentityRef,
};
use crate::common::LixError;
use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;

const COMMIT_MAGIC: &[u8; 5] = b"LXCM1";
const CHANGE_MAGIC: &[u8; 5] = b"LXCH1";
const COMMIT_CHANGE_REF_CHUNK_MAGIC: &[u8; 5] = b"LXCR1";

pub(crate) fn encode_commit_record(record: &CommitRecord) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(COMMIT_MAGIC);
    write_u32(&mut bytes, record.format_version);
    write_str(&mut bytes, &record.commit_id)?;
    write_str_vec(&mut bytes, &record.parent_commit_ids)?;
    write_str(&mut bytes, &record.change_id)?;
    write_str_vec(&mut bytes, &record.author_account_ids)?;
    write_str(&mut bytes, &record.created_at)?;
    Ok(bytes)
}

pub(crate) fn view_commit_record(bytes: &[u8]) -> Result<CommitRecordView<'_>, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(COMMIT_MAGIC, "commit record")?;
    let view = CommitRecordView {
        format_version: cursor.read_u32("format_version")?,
        commit_id: cursor.read_str("commit_id")?,
        parent_commit_ids: cursor.read_str_vec("parent_commit_ids")?,
        change_id: cursor.read_str("change_id")?,
        author_account_ids: cursor.read_str_vec("author_account_ids")?,
        created_at: cursor.read_str("created_at")?,
    };
    cursor.expect_end("commit record")?;
    Ok(view)
}

pub(crate) fn decode_commit_record(bytes: &[u8]) -> Result<CommitRecord, LixError> {
    let view = view_commit_record(bytes)?;
    Ok(CommitRecord {
        format_version: view.format_version,
        commit_id: view.commit_id.to_string(),
        parent_commit_ids: view
            .parent_commit_ids
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
        change_id: view.change_id.to_string(),
        author_account_ids: view
            .author_account_ids
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
        created_at: view.created_at.to_string(),
    })
}

pub(crate) fn encode_change_record(record: &ChangeRecord) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CHANGE_MAGIC);
    write_u32(&mut bytes, record.format_version);
    write_str(&mut bytes, &record.change_id)?;
    write_str(&mut bytes, &record.schema_key)?;
    write_entity_identity(&mut bytes, &record.entity_id)?;
    write_optional_str(&mut bytes, record.file_id.as_deref())?;
    write_optional_json_ref(&mut bytes, record.snapshot_ref)?;
    write_optional_json_ref(&mut bytes, record.metadata_ref)?;
    write_str(&mut bytes, &record.created_at)?;
    Ok(bytes)
}

pub(crate) fn view_change_record(bytes: &[u8]) -> Result<ChangeRecordView<'_>, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(CHANGE_MAGIC, "change record")?;
    let view = ChangeRecordView {
        format_version: cursor.read_u32("format_version")?,
        change_id: cursor.read_str("change_id")?,
        schema_key: cursor.read_str("schema_key")?,
        entity_id: cursor.read_entity_identity("entity_id")?,
        file_id: cursor.read_optional_str("file_id")?,
        snapshot_ref: cursor.read_optional_json_ref("snapshot_ref")?,
        metadata_ref: cursor.read_optional_json_ref("metadata_ref")?,
        created_at: cursor.read_str("created_at")?,
    };
    cursor.expect_end("change record")?;
    Ok(view)
}

pub(crate) fn decode_change_record(bytes: &[u8]) -> Result<ChangeRecord, LixError> {
    let view = view_change_record(bytes)?;
    Ok(ChangeRecord {
        format_version: view.format_version,
        change_id: view.change_id.to_string(),
        schema_key: view.schema_key.to_string(),
        entity_id: entity_identity_from_ref(view.entity_id)?,
        file_id: view.file_id.map(str::to_string),
        snapshot_ref: view.snapshot_ref,
        metadata_ref: view.metadata_ref,
        created_at: view.created_at.to_string(),
    })
}

pub(crate) fn encode_commit_change_ref_chunk(
    chunk: &CommitChangeRefChunk,
) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(COMMIT_CHANGE_REF_CHUNK_MAGIC);
    write_u32(&mut bytes, chunk.format_version);
    write_str(&mut bytes, &chunk.commit_id)?;
    write_len(&mut bytes, chunk.entries.len(), "commit change ref entries")?;
    for entry in &chunk.entries {
        write_str(&mut bytes, &entry.schema_key)?;
        write_optional_str(&mut bytes, entry.file_id.as_deref())?;
        write_entity_identity(&mut bytes, &entry.entity_id)?;
        write_str(&mut bytes, &entry.change_id)?;
    }
    Ok(bytes)
}

pub(crate) fn view_commit_change_ref_chunk(
    bytes: &[u8],
) -> Result<CommitChangeRefChunkView<'_>, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(COMMIT_CHANGE_REF_CHUNK_MAGIC, "commit change ref chunk")?;
    let format_version = cursor.read_u32("format_version")?;
    let commit_id = cursor.read_str("commit_id")?;
    let entry_count = cursor.read_len("commit change ref entries")?;
    let mut entries = Vec::with_capacity(entry_count);
    for index in 0..entry_count {
        let context = format!("commit change ref entries[{index}]");
        entries.push(CommitChangeRefView {
            schema_key: cursor.read_str(&format!("{context}.schema_key"))?,
            file_id: cursor.read_optional_str(&format!("{context}.file_id"))?,
            entity_id: cursor.read_entity_identity(&format!("{context}.entity_id"))?,
            change_id: cursor.read_str(&format!("{context}.change_id"))?,
        });
    }
    cursor.expect_end("commit change ref chunk")?;
    Ok(CommitChangeRefChunkView {
        format_version,
        commit_id,
        entries,
    })
}

pub(crate) fn decode_commit_change_ref_chunk(
    bytes: &[u8],
) -> Result<CommitChangeRefChunk, LixError> {
    let view = view_commit_change_ref_chunk(bytes)?;
    Ok(CommitChangeRefChunk {
        format_version: view.format_version,
        commit_id: view.commit_id.to_string(),
        entries: view
            .entries
            .iter()
            .map(|entry| {
                Ok(CommitChangeRef {
                    schema_key: entry.schema_key.to_string(),
                    file_id: entry.file_id.map(str::to_string),
                    entity_id: entity_identity_from_ref(entry.entity_id.clone())?,
                    change_id: entry.change_id.to_string(),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?,
    })
}

fn write_str_vec(out: &mut Vec<u8>, values: &[String]) -> Result<(), LixError> {
    write_len(out, values.len(), "string vec")?;
    for value in values {
        write_str(out, value)?;
    }
    Ok(())
}

fn write_entity_identity(out: &mut Vec<u8>, identity: &EntityIdentity) -> Result<(), LixError> {
    write_len(out, identity.parts.len(), "entity identity parts")?;
    for part in &identity.parts {
        write_str(out, part)?;
    }
    Ok(())
}

fn write_optional_str(out: &mut Vec<u8>, value: Option<&str>) -> Result<(), LixError> {
    match value {
        Some(value) => {
            out.push(1);
            write_str(out, value)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn write_optional_json_ref(out: &mut Vec<u8>, value: Option<JsonRef>) -> Result<(), LixError> {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(value.as_hash_bytes());
        }
        None => out.push(0),
    }
    Ok(())
}

fn write_str(out: &mut Vec<u8>, value: &str) -> Result<(), LixError> {
    write_len(out, value.len(), "string bytes")?;
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_len(out: &mut Vec<u8>, len: usize, context: &str) -> Result<(), LixError> {
    let len = u32::try_from(len).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog {context} length exceeds u32"),
        )
    })?;
    write_u32(out, len);
    Ok(())
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn entity_identity_from_ref(value: EntityIdentityRef<'_>) -> Result<EntityIdentity, LixError> {
    EntityIdentity::from_parts(value.parts.iter().map(|part| (*part).to_string()).collect())
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog entity identity is invalid: {error}"),
            )
        })
}

#[derive(Clone)]
struct ByteCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_magic(&mut self, magic: &[u8], context: &str) -> Result<(), LixError> {
        let found = self.read_bytes(magic.len(), context)?;
        if found != magic {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog {context} has invalid magic"),
            ));
        }
        Ok(())
    }

    fn read_str_vec(&mut self, context: &str) -> Result<Vec<&'a str>, LixError> {
        let len = self.read_len(context)?;
        let mut values = Vec::with_capacity(len);
        for index in 0..len {
            values.push(self.read_str(&format!("{context}[{index}]"))?);
        }
        Ok(values)
    }

    fn read_entity_identity(&mut self, context: &str) -> Result<EntityIdentityRef<'a>, LixError> {
        let len = self.read_len(context)?;
        let mut parts = Vec::with_capacity(len);
        for index in 0..len {
            parts.push(self.read_str(&format!("{context}.parts[{index}]"))?);
        }
        Ok(EntityIdentityRef { parts })
    }

    fn read_optional_str(&mut self, context: &str) -> Result<Option<&'a str>, LixError> {
        match self.read_flag(context)? {
            false => Ok(None),
            true => Ok(Some(self.read_str(context)?)),
        }
    }

    fn read_optional_json_ref(&mut self, context: &str) -> Result<Option<JsonRef>, LixError> {
        match self.read_flag(context)? {
            false => Ok(None),
            true => {
                let bytes = self.read_bytes(32, context)?;
                let mut hash = [0; 32];
                hash.copy_from_slice(bytes);
                Ok(Some(JsonRef::from_hash_bytes(hash)))
            }
        }
    }

    fn read_str(&mut self, context: &str) -> Result<&'a str, LixError> {
        let len = self.read_len(context)?;
        let bytes = self.read_bytes(len, context)?;
        std::str::from_utf8(bytes).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog {context} contains invalid UTF-8: {error}"),
            )
        })
    }

    fn read_flag(&mut self, context: &str) -> Result<bool, LixError> {
        let byte = self.read_bytes(1, context)?[0];
        match byte {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog {context} option flag is invalid"),
            )),
        }
    }

    fn read_len(&mut self, context: &str) -> Result<usize, LixError> {
        Ok(self.read_u32(context)? as usize)
    }

    fn read_u32(&mut self, context: &str) -> Result<u32, LixError> {
        let bytes = self.read_bytes(4, context)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_bytes(&mut self, len: usize, context: &str) -> Result<&'a [u8], LixError> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog {context} length overflows"),
            )
        })?;
        if end > self.bytes.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog {context} is truncated"),
            ));
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn expect_end(&self, context: &str) -> Result<(), LixError> {
        if self.offset != self.bytes.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog {context} has trailing bytes"),
            ));
        }
        Ok(())
    }
}
