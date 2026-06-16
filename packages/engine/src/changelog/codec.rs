use super::types::{
    ChangeId, ChangeRecord, ChangeRecordRef, ChangeRecordView, CommitChangeRefChunk,
    CommitChangeRefChunkWire, CommitChangeRefChunkWireRef, CommitId, CommitRecord,
};
use crate::common::LixError;
use crate::entity_pk::EntityPk;
use crate::storage_codec;

pub(crate) fn encode_commit_record(record: &CommitRecord) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("commit record", record)
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
            snapshot: record.snapshot.as_ref_slot(),
            metadata: record.metadata.as_ref_slot(),
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
        entity_pk: entity_pk_from_parts(view.entity_pk)?,
        file_id: view.file_id,
        snapshot: view.snapshot,
        metadata: view.metadata,
        created_at: view.created_at,
    })
}

pub(crate) fn encode_commit_change_ref_chunk(
    chunk: &CommitChangeRefChunk,
) -> Result<Vec<u8>, LixError> {
    storage_codec::encode(
        "commit change ref chunk",
        &CommitChangeRefChunkWireRef {
            format_version: chunk.format_version,
            entries: &chunk.entries,
        },
    )
}

pub(crate) fn decode_commit_change_ref_chunk(
    bytes: &[u8],
    commit_id: CommitId,
) -> Result<CommitChangeRefChunk, LixError> {
    let wire: CommitChangeRefChunkWire = storage_codec::decode("commit change ref chunk", bytes)?;
    if wire.format_version != super::context::COMMIT_CHANGE_REF_CHUNK_FORMAT_VERSION {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "commit change ref chunk has unsupported format version {}",
                wire.format_version
            ),
        ));
    }
    Ok(CommitChangeRefChunk {
        format_version: wire.format_version,
        commit_id,
        entries: wire.entries,
    })
}

fn entity_pk_from_parts(parts: Vec<String>) -> Result<EntityPk, LixError> {
    EntityPk::from_parts(parts).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog entity primary key is invalid: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::ChangeId;
    use crate::common::LixTimestamp;
    use crate::json_store::{JsonRef, JsonSlot};

    fn ref_chunk(entries: Vec<ChangeId>) -> CommitChangeRefChunk {
        CommitChangeRefChunk {
            format_version: 1,
            commit_id: CommitId::for_test_label("ref-chunk-commit"),
            entries,
        }
    }

    #[test]
    fn ref_chunk_round_trips_change_ids() {
        let chunk = ref_chunk(vec![
            ChangeId::for_test_label("change-a"),
            ChangeId::for_test_label("change-b"),
            ChangeId::for_test_label("change-c"),
        ]);
        let encoded = encode_commit_change_ref_chunk(&chunk).expect("chunk should encode");
        let decoded =
            decode_commit_change_ref_chunk(&encoded, chunk.commit_id).expect("chunk should decode");
        assert_eq!(decoded, chunk);
    }

    #[test]
    fn ref_chunk_round_trips_empty_entries() {
        let chunk = ref_chunk(Vec::new());
        let encoded = encode_commit_change_ref_chunk(&chunk).expect("chunk should encode");
        let decoded =
            decode_commit_change_ref_chunk(&encoded, chunk.commit_id).expect("chunk should decode");
        assert_eq!(decoded, chunk);
    }

    #[test]
    fn ref_chunk_wire_format_is_pinned() {
        // Persisted layout: varint format_version, varint entry count, then
        // 16 raw uuid bytes per entry (ChangeId encodes via encode_array).
        let id = ChangeId::parse("019eb805-60d0-71c0-ade3-b0f0efab9d9a").expect("uuid");
        let chunk = ref_chunk(vec![id]);
        let encoded = encode_commit_change_ref_chunk(&chunk).expect("chunk should encode");
        let expected: Vec<u8> = [
            &[1u8][..], // format_version
            &[1u8][..], // entry count
            &[
                0x01, 0x9e, 0xb8, 0x05, 0x60, 0xd0, 0x71, 0xc0, 0xad, 0xe3, 0xb0, 0xf0, 0xef, 0xab,
                0x9d, 0x9a,
            ][..],
        ]
        .concat();
        assert_eq!(encoded, expected);
    }

    #[test]
    fn ref_chunk_decode_rejects_unknown_format_version() {
        let chunk = CommitChangeRefChunk {
            format_version: 2,
            commit_id: CommitId::for_test_label("ref-chunk-commit"),
            entries: vec![ChangeId::for_test_label("change-a")],
        };
        let encoded = encode_commit_change_ref_chunk(&chunk).expect("chunk should encode");
        let error = decode_commit_change_ref_chunk(&encoded, chunk.commit_id)
            .expect_err("unknown format version should fail decode");
        assert!(
            error.message.contains("unsupported format version 2"),
            "{}",
            error.message
        );
    }

    #[test]
    fn ref_chunk_takes_commit_identity_from_the_decode_argument() {
        // The stored value omits commit_id; it lives in the storage key.
        let chunk = ref_chunk(vec![ChangeId::for_test_label("change-a")]);
        let encoded = encode_commit_change_ref_chunk(&chunk).expect("chunk should encode");
        let other = CommitId::for_test_label("other-commit");
        let decoded = decode_commit_change_ref_chunk(&encoded, other).expect("chunk should decode");
        assert_eq!(decoded.commit_id, other);
        assert_eq!(decoded.entries, chunk.entries);
    }

    fn full_record() -> ChangeRecord {
        ChangeRecord {
            format_version: 1,
            change_id: ChangeId::for_test_label("roundtrip-change"),
            schema_key: "schema-\u{00e9}\u{4e2d}".to_string(),
            entity_pk: EntityPk::from_parts(vec!["part-a".to_string(), "part-b".to_string()])
                .expect("entity pk should build"),
            file_id: Some("file-1".to_string()),
            snapshot: JsonSlot::Ref(JsonRef::for_content(b"snapshot")),
            metadata: JsonSlot::Ref(JsonRef::for_content(b"metadata")),
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
    fn change_record_round_trips_inline_payloads() {
        // The Inline slot variant (tag 2) carries the JSON text itself; it
        // must survive encode/decode byte-exactly, including non-ASCII.
        let record = ChangeRecord {
            snapshot: JsonSlot::from_json("{\"name\":\"libf\u{00f6}\u{4e2d}\"}"),
            metadata: JsonSlot::from_json("{\"k\":1}"),
            ..full_record()
        };
        assert!(matches!(record.snapshot, JsonSlot::Inline(_)));
        let encoded = encode_change_record(&record).expect("record should encode");
        let decoded =
            decode_change_record(&encoded, record.change_id).expect("record should decode");
        assert_eq!(decoded, record);
    }

    #[test]
    fn change_record_round_trips_with_empty_options() {
        let record = ChangeRecord {
            file_id: None,
            snapshot: JsonSlot::None,
            metadata: JsonSlot::None,
            ..full_record()
        };
        let encoded = encode_change_record(&record).expect("record should encode");
        let decoded =
            decode_change_record(&encoded, record.change_id).expect("record should decode");
        assert_eq!(decoded, record);
    }

    #[test]
    fn change_record_packs_canonical_uuid_ids_and_round_trips() {
        // Canonical lowercase UUID entity pks and file ids take the 16-byte
        // arm; the record must round-trip to the identical text form and be
        // smaller than the unpacked text encoding.
        let record = ChangeRecord {
            entity_pk: EntityPk::from_parts(vec![
                "019eb805-60d0-71c0-ade3-b0f0efab9d9a".to_string(),
            ])
            .expect("entity pk should build"),
            file_id: Some("019eb805-5e65-7270-861d-cb341bc904c8".to_string()),
            ..full_record()
        };
        let encoded = encode_change_record(&record).expect("record should encode");
        let text_only = ChangeRecord {
            entity_pk: EntityPk::from_parts(vec![
                "019EB805-60D0-71C0-ADE3-B0F0EFAB9D9A".to_string(),
            ])
            .expect("entity pk should build"),
            file_id: Some("019EB805-5E65-7270-861D-CB341BC904C8".to_string()),
            ..full_record()
        };
        let text_encoded = encode_change_record(&text_only).expect("record should encode");
        assert!(
            encoded.len() + 40 <= text_encoded.len(),
            "uuid arm should save 20 bytes per id ({} vs {})",
            encoded.len(),
            text_encoded.len()
        );
        let decoded =
            decode_change_record(&encoded, record.change_id).expect("record should decode");
        assert_eq!(decoded, record);
    }

    #[test]
    fn change_record_keeps_non_canonical_ids_as_text() {
        // Uppercase hex re-hyphenates differently, so it must stay text to
        // round-trip byte-identically; same for arbitrary plugin keys.
        let record = ChangeRecord {
            entity_pk: EntityPk::from_parts(vec![
                "019EB805-60D0-71C0-ADE3-B0F0EFAB9D9A".to_string(),
                "row 5 of sheet 2".to_string(),
            ])
            .expect("entity pk should build"),
            file_id: Some("not-a-uuid".to_string()),
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
