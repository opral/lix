use super::types::{
    ChangeRecord, ChangeRecordView, CommitChangeRef, CommitChangeRefChunk,
    CommitChangeRefChunkView, CommitChangeRefView, CommitRecord, CommitRecordView, EntityPkRef,
};
use crate::common::LixError;
use crate::entity_pk::EntityPk;
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
    write_entity_pk(&mut bytes, &record.entity_pk)?;
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
        entity_pk: cursor.read_entity_pk("entity_pk")?,
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
        entity_pk: entity_pk_from_ref(view.entity_pk)?,
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
    let schema_keys = commit_change_ref_schema_dictionary(&chunk.entries);
    let file_ids = commit_change_ref_file_dictionary(&chunk.entries);
    write_len(
        &mut bytes,
        schema_keys.len(),
        "commit change ref schema dictionary",
    )?;
    for schema_key in &schema_keys {
        write_str(&mut bytes, schema_key)?;
    }
    write_len(
        &mut bytes,
        file_ids.len(),
        "commit change ref file dictionary",
    )?;
    for file_id in &file_ids {
        write_optional_str(&mut bytes, *file_id)?;
    }
    write_len(&mut bytes, chunk.entries.len(), "commit change ref entries")?;
    for entry in &chunk.entries {
        let schema_index = dictionary_index(
            schema_keys.iter().copied(),
            entry.schema_key.as_str(),
            "commit change ref schema dictionary",
        )?;
        let file_index = optional_dictionary_index(
            file_ids.iter().copied(),
            entry.file_id.as_deref(),
            "commit change ref file dictionary",
        )?;
        write_u16_index(&mut bytes, schema_index, "commit change ref schema index")?;
        write_u16_index(&mut bytes, file_index, "commit change ref file index")?;
        write_entity_pk_compact(&mut bytes, &entry.entity_pk)?;
        write_str(&mut bytes, &entry.change_id)?;
    }
    Ok(bytes)
}

pub(crate) fn view_commit_change_ref_chunk<'a>(
    bytes: &'a [u8],
    commit_id: &'a str,
) -> Result<CommitChangeRefChunkView<'a>, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(COMMIT_CHANGE_REF_CHUNK_MAGIC, "commit change ref chunk")?;
    let format_version = cursor.read_u32("format_version")?;
    let schema_keys = cursor.read_str_vec("commit change ref schema dictionary")?;
    let file_ids = cursor.read_optional_str_vec("commit change ref file dictionary")?;
    let entry_count = cursor.read_len("commit change ref entries")?;
    let mut entries = Vec::with_capacity(entry_count);
    for index in 0..entry_count {
        let context = format!("commit change ref entries[{index}]");
        let schema_index = cursor.read_u16(&format!("{context}.schema_index"))? as usize;
        let file_index = cursor.read_u16(&format!("{context}.file_index"))? as usize;
        entries.push(CommitChangeRefView {
            schema_key: *schema_keys.get(schema_index).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("changelog {context}.schema_index is out of bounds"),
                )
            })?,
            file_id: *file_ids.get(file_index).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("changelog {context}.file_index is out of bounds"),
                )
            })?,
            entity_pk: cursor.read_entity_pk_compact(&format!("{context}.entity_pk"))?,
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
    commit_id: &str,
) -> Result<CommitChangeRefChunk, LixError> {
    let view = view_commit_change_ref_chunk(bytes, commit_id)?;
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
                    entity_pk: entity_pk_from_ref(entry.entity_pk.clone())?,
                    change_id: entry.change_id.to_string(),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?,
    })
}

fn commit_change_ref_schema_dictionary(entries: &[CommitChangeRef]) -> Vec<&str> {
    let mut values = Vec::new();
    for entry in entries {
        if !values.contains(&entry.schema_key.as_str()) {
            values.push(entry.schema_key.as_str());
        }
    }
    values
}

fn commit_change_ref_file_dictionary(entries: &[CommitChangeRef]) -> Vec<Option<&str>> {
    let mut values = Vec::new();
    for entry in entries {
        let value = entry.file_id.as_deref();
        if !values.contains(&value) {
            values.push(value);
        }
    }
    values
}

fn dictionary_index<'a>(
    mut values: impl Iterator<Item = &'a str>,
    needle: &str,
    context: &str,
) -> Result<usize, LixError> {
    values.position(|value| value == needle).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog {context} is missing value '{needle}'"),
        )
    })
}

fn optional_dictionary_index<'a>(
    mut values: impl Iterator<Item = Option<&'a str>>,
    needle: Option<&str>,
    context: &str,
) -> Result<usize, LixError> {
    values.position(|value| value == needle).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog {context} is missing optional value"),
        )
    })
}

fn write_str_vec(out: &mut Vec<u8>, values: &[String]) -> Result<(), LixError> {
    write_len(out, values.len(), "string vec")?;
    for value in values {
        write_str(out, value)?;
    }
    Ok(())
}

fn write_entity_pk(out: &mut Vec<u8>, identity: &EntityPk) -> Result<(), LixError> {
    write_len(out, identity.parts.len(), "entity primary key parts")?;
    for part in &identity.parts {
        write_str(out, part)?;
    }
    Ok(())
}

fn write_entity_pk_compact(out: &mut Vec<u8>, identity: &EntityPk) -> Result<(), LixError> {
    if identity.parts.len() == 1 {
        out.push(1);
        write_str(out, &identity.parts[0])
    } else {
        out.push(0);
        write_entity_pk(out, identity)
    }
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

fn write_u16_index(out: &mut Vec<u8>, value: usize, context: &str) -> Result<(), LixError> {
    let value = u16::try_from(value).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog {context} exceeds u16"),
        )
    })?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn entity_pk_from_ref(value: EntityPkRef<'_>) -> Result<EntityPk, LixError> {
    EntityPk::from_parts(value.parts.iter().map(|part| (*part).to_string()).collect()).map_err(
        |error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog entity primary key is invalid: {error}"),
            )
        },
    )
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

    fn read_optional_str_vec(&mut self, context: &str) -> Result<Vec<Option<&'a str>>, LixError> {
        let len = self.read_len(context)?;
        let mut values = Vec::with_capacity(len);
        for index in 0..len {
            values.push(self.read_optional_str(&format!("{context}[{index}]"))?);
        }
        Ok(values)
    }

    fn read_entity_pk(&mut self, context: &str) -> Result<EntityPkRef<'a>, LixError> {
        let len = self.read_len(context)?;
        let mut parts = Vec::with_capacity(len);
        for index in 0..len {
            parts.push(self.read_str(&format!("{context}.parts[{index}]"))?);
        }
        Ok(EntityPkRef { parts })
    }

    fn read_entity_pk_compact(&mut self, context: &str) -> Result<EntityPkRef<'a>, LixError> {
        match self.read_bytes(1, context)?[0] {
            0 => self.read_entity_pk(context),
            1 => Ok(EntityPkRef {
                parts: vec![self.read_str(&format!("{context}.part"))?],
            }),
            _ => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog {context} compact entity primary key flag is invalid"),
            )),
        }
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

    fn read_u16(&mut self, context: &str) -> Result<u16, LixError> {
        let bytes = self.read_bytes(2, context)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
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
