use super::types::{
    ChangeRecord, ChangeRecordRef, ChangeRecordView, CommitChangeRef, CommitChangeRefChunk,
    CommitChangeRefChunkRef, CommitChangeRefChunkView, CommitChangeRefEntryRef,
    CommitChangeRefEntryView, CommitChangeRefView, CommitRecord, EntityPkRef,
    ExpandedCommitChangeRefChunkView,
};
use crate::common::LixError;
use crate::entity_pk::EntityPk;
use crate::storage_codec;

pub(crate) fn encode_commit_record(record: &CommitRecord) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("commit record", record)
}

pub(crate) fn decode_commit_record(bytes: &[u8]) -> Result<CommitRecord, LixError> {
    storage_codec::decode("commit record", bytes)
}

pub(crate) fn encode_change_record(record: &ChangeRecord) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("change record", record)
}

pub(crate) fn decode_change_record(bytes: &[u8]) -> Result<ChangeRecord, LixError> {
    storage_codec::decode("change record", bytes)
}

pub(crate) fn encode_commit_change_ref_chunk(
    chunk: &CommitChangeRefChunk,
) -> Result<Vec<u8>, LixError> {
    let schema_keys = commit_change_ref_schema_dictionary(&chunk.entries);
    let file_ids = commit_change_ref_file_dictionary(&chunk.entries);
    let mut entries = Vec::with_capacity(chunk.entries.len());
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
        entries.push(CommitChangeRefEntryRef {
            schema_index: u16_index(schema_index, "commit change ref schema index")?,
            file_index: u16_index(file_index, "commit change ref file index")?,
            entity_pk: &entry.entity_pk.parts,
            change_id: &entry.change_id,
        });
    }
    storage_codec::encode(
        "commit change ref chunk",
        &CommitChangeRefChunkRef {
            format_version: chunk.format_version,
            schema_keys,
            file_ids,
            entries,
        },
    )
}

pub(crate) fn view_commit_change_ref_chunk<'a>(
    bytes: &'a [u8],
    commit_id: &'a str,
) -> Result<ExpandedCommitChangeRefChunkView<'a>, LixError> {
    let CommitChangeRefChunkView {
        format_version,
        schema_keys,
        file_ids,
        entries: storage_entries,
    } = storage_codec::decode("commit change ref chunk", bytes)?;
    let mut entries = Vec::with_capacity(storage_entries.len());
    for (index, entry) in storage_entries.into_iter().enumerate() {
        let context = format!("commit change ref entries[{index}]");
        let CommitChangeRefEntryView {
            schema_index,
            file_index,
            entity_pk,
            change_id,
        } = entry;
        let schema_index = schema_index as usize;
        let file_index = file_index as usize;
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
            entity_pk: EntityPkRef { parts: entity_pk },
            change_id,
        });
    }
    Ok(ExpandedCommitChangeRefChunkView {
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

fn u16_index(value: usize, context: &str) -> Result<u16, LixError> {
    u16::try_from(value).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog {context} exceeds u16"),
        )
    })
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
