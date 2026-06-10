use super::types::{
    ChangeId, ChangeRecord, ChangeRecordRef, ChangeRecordView, CommitChangeRef,
    CommitChangeRefChunk, CommitChangeRefChunkRef, CommitChangeRefChunkView,
    CommitChangeRefEntryRef, CommitChangeRefEntryView, CommitChangeRefView, CommitId, CommitRecord,
    EntityPkRef, ExpandedCommitChangeRefChunkView,
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
    // change_id is the storage key; the value intentionally omits it.
    storage_codec::encode(
        "change record",
        &ChangeRecordRef {
            format_version: record.format_version,
            schema_key: &record.schema_key,
            entity_pk: &record.entity_pk.parts,
            file_id: record.file_id.as_deref(),
            snapshot_ref: record.snapshot_ref.as_ref(),
            metadata_ref: record.metadata_ref.as_ref(),
            created_at: record.created_at,
        },
    )
}

pub(crate) fn decode_change_record(
    bytes: &[u8],
    change_id: ChangeId,
) -> Result<ChangeRecord, LixError> {
    let view: ChangeRecordView<'_> = storage_codec::decode("change record", bytes)?;
    Ok(ChangeRecord {
        format_version: view.format_version,
        change_id,
        schema_key: view.schema_key.to_string(),
        entity_pk: entity_pk_from_ref(view.entity_pk)?,
        file_id: view.file_id.map(str::to_string),
        snapshot_ref: view.snapshot_ref,
        metadata_ref: view.metadata_ref,
        created_at: view.created_at,
    })
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
            change_id: entry.change_id,
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

pub(crate) fn view_commit_change_ref_chunk(
    bytes: &[u8],
    commit_id: CommitId,
) -> Result<ExpandedCommitChangeRefChunkView<'_>, LixError> {
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
            schema_key: schema_keys.get(schema_index).ok_or_else(|| {
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
    commit_id: CommitId,
) -> Result<CommitChangeRefChunk, LixError> {
    let view = view_commit_change_ref_chunk(bytes, commit_id)?;
    Ok(CommitChangeRefChunk {
        format_version: view.format_version,
        commit_id: view.commit_id,
        entries: view
            .entries
            .iter()
            .map(|entry| {
                Ok(CommitChangeRef {
                    schema_key: entry.schema_key.to_string(),
                    file_id: entry.file_id.map(str::to_string),
                    entity_pk: entity_pk_from_ref(entry.entity_pk.clone())?,
                    change_id: entry.change_id,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::ChangeId;
    use crate::common::LixTimestamp;
    use crate::json_store::JsonRef;

    fn full_record() -> ChangeRecord {
        ChangeRecord {
            format_version: 1,
            change_id: ChangeId::for_test_label("roundtrip-change"),
            schema_key: "schema-\u{00e9}\u{4e2d}".to_string(),
            entity_pk: EntityPk::from_parts(vec!["part-a".to_string(), "part-b".to_string()])
                .expect("entity pk should build"),
            file_id: Some("file-1".to_string()),
            snapshot_ref: Some(JsonRef::for_content(b"snapshot")),
            metadata_ref: Some(JsonRef::for_content(b"metadata")),
            created_at: LixTimestamp::expect_parse("created_at", "2026-06-10T00:00:00.000Z"),
        }
    }

    #[test]
    fn change_record_round_trips_fully_populated() {
        let record = full_record();
        let encoded = encode_change_record(&record).expect("record should encode");
        let decoded =
            decode_change_record(&encoded, record.change_id).expect("record should decode");
        assert_eq!(decoded, record);
    }

    #[test]
    fn change_record_round_trips_with_empty_options() {
        let record = ChangeRecord {
            file_id: None,
            snapshot_ref: None,
            metadata_ref: None,
            ..full_record()
        };
        let encoded = encode_change_record(&record).expect("record should encode");
        let decoded =
            decode_change_record(&encoded, record.change_id).expect("record should decode");
        assert_eq!(decoded, record);
    }

    #[test]
    fn change_record_takes_identity_from_the_decode_argument() {
        // The stored value omits change_id; whatever id the key supplies is
        // what the decoded record carries.
        let record = full_record();
        let encoded = encode_change_record(&record).expect("record should encode");
        let other_id = ChangeId::for_test_label("other-change");
        let decoded = decode_change_record(&encoded, other_id).expect("record should decode");
        assert_eq!(decoded.change_id, other_id);
    }
}
