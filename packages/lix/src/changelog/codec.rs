use super::types::{
    ChangeId, ChangeRecord, ChangeRecordRef, ChangeRecordView, CommitRecord,
    TransactionChangeRecordRef,
};
use crate::common::LixError;
use crate::storage_codec;

#[cfg(test)]
pub(crate) fn encode_commit_record(record: &CommitRecord) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("commit record", record)
}

pub(crate) fn append_commit_record(
    bytes: &mut Vec<u8>,
    record: &CommitRecord,
) -> Result<std::ops::Range<usize>, LixError> {
    storage_codec::append("commit record", bytes, record)
}

#[cfg(test)]
pub(crate) fn encode_change_record(record: &ChangeRecord) -> Result<Vec<u8>, LixError> {
    encode_change_record_ref(&ChangeRecordRef {
        format_version: record.format_version,
        account_id: &record.account_id,
        schema_key: &record.schema_key,
        row_pk: &record.row_pk,
        file_id: record.file_id.as_deref(),
        snapshot: record.snapshot.as_ref_slot(),
        metadata: record.metadata.as_ref_slot(),
        typed_payload: record.typed_payload.as_deref(),
        created_at: record.created_at,
        origin_key: record.origin_key.as_deref(),
    })
}

pub(crate) fn append_change_record(
    bytes: &mut Vec<u8>,
    record: &ChangeRecord,
) -> Result<std::ops::Range<usize>, LixError> {
    append_change_record_ref(
        bytes,
        &ChangeRecordRef {
            format_version: record.format_version,
            account_id: &record.account_id,
            schema_key: &record.schema_key,
            row_pk: &record.row_pk,
            file_id: record.file_id.as_deref(),
            snapshot: record.snapshot.as_ref_slot(),
            metadata: record.metadata.as_ref_slot(),
            typed_payload: record.typed_payload.as_deref(),
            created_at: record.created_at,
            origin_key: record.origin_key.as_deref(),
        },
    )
}

#[cfg(test)]
pub(crate) fn encode_transaction_change_record(
    record: &TransactionChangeRecordRef<'_>,
) -> Result<Vec<u8>, LixError> {
    encode_change_record_ref(&ChangeRecordRef {
        format_version: record.format_version,
        account_id: record.account_id,
        schema_key: record.schema_key,
        row_pk: record.row_pk,
        file_id: record.file_id,
        snapshot: record.snapshot,
        metadata: record.metadata,
        typed_payload: record.typed_payload,
        created_at: record.created_at,
        origin_key: record.origin_key,
    })
}

pub(crate) fn append_transaction_change_record(
    bytes: &mut Vec<u8>,
    record: &TransactionChangeRecordRef<'_>,
) -> Result<std::ops::Range<usize>, LixError> {
    let encoded_typed = record
        .typed_snapshot
        .map(|row| {
            row.durable_payload()
            .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, format!("cannot encode typed payload: {error:?}")))
        })
        .transpose()?;
    append_change_record_ref(
        bytes,
        &ChangeRecordRef {
            format_version: record.format_version,
            account_id: record.account_id,
            schema_key: record.schema_key,
            row_pk: record.row_pk,
            file_id: record.file_id,
            snapshot: record.snapshot,
            metadata: record.metadata,
            typed_payload: encoded_typed.as_deref().or(record.typed_payload),
            created_at: record.created_at,
            origin_key: record.origin_key,
        },
    )
}

#[cfg(test)]
fn encode_change_record_ref(record: &ChangeRecordRef<'_>) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    append_change_record_ref(&mut bytes, record)?;
    Ok(bytes)
}

fn append_change_record_ref(
    bytes: &mut Vec<u8>,
    record: &ChangeRecordRef<'_>,
) -> Result<std::ops::Range<usize>, LixError> {
    if record.snapshot != crate::json_store::JsonSlotRef::None && record.typed_payload.is_some() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "change record must not carry both JSON and typed payloads",
        ));
    }
    // change_id is the storage key; the value intentionally omits it.
    storage_codec::append("change record", bytes, record)
}

pub(crate) fn decode_change_record(
    bytes: &[u8],
    change_id: ChangeId,
) -> Result<ChangeRecord, LixError> {
    let view: ChangeRecordView<'_> = storage_codec::decode("change record", bytes)?;
    let record = ChangeRecord {
        format_version: view.format_version,
        change_id,
        account_id: view.account_id.to_string(),
        schema_key: view.schema_key.to_string(),
        row_pk: view.row_pk,
        file_id: view.file_id,
        snapshot: view.snapshot,
        metadata: view.metadata,
        typed_payload: view.typed_payload,
        created_at: view.created_at,
        origin_key: view.origin_key,
    };
    if record.snapshot.is_some() && record.typed_payload.is_some() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "change record carries both JSON and typed payloads",
        ));
    }
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::ChangeId;
    use crate::changelog::CommitId;
    use crate::common::LixTimestamp;
    use crate::row_pk::RowPk;
    use crate::json_store::{JsonRef, JsonSlot};

    #[test]
    fn commit_record_round_trip_preserves_first_parent_jump() {
        let commit_id = CommitId::for_test_label("codec-segment-commit");
        let base_commit_id = CommitId::for_test_label("codec-segment-base");
        let record = CommitRecord {
            touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
            format_version: 4,
            commit_id,
            generation: 70,
            parent_commit_ids: vec![CommitId::for_test_label("codec-segment-parent")],
            first_parent_jump_commit_id: base_commit_id,
            first_parent_jump_span: 6,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: LixTimestamp::expect_parse(
                "commit codec timestamp",
                "2026-08-07T00:00:00Z",
            ),
        };

        let encoded = encode_commit_record(&record).expect("commit should encode");
        let decoded: CommitRecord =
            storage_codec::decode("commit record", &encoded).expect("commit should decode");
        assert_eq!(decoded, record);
    }

    fn full_record() -> ChangeRecord {
        ChangeRecord {
            format_version: 1,
            change_id: ChangeId::for_test_label("roundtrip-change"),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            schema_key: "schema-\u{00e9}\u{4e2d}".to_string(),
            row_pk: RowPk::from_parts(vec!["part-a".to_string(), "part-b".to_string()])
                .expect("row pk should build"),
            file_id: Some("file-1".to_string()),
            snapshot: JsonSlot::Ref(JsonRef::for_content(b"snapshot")),
            metadata: JsonSlot::Ref(JsonRef::for_content(b"metadata")),
            typed_payload: None,
            created_at: LixTimestamp::expect_parse("created_at", "2026-06-10T00:00:00.000Z"),
            origin_key: Some("codec-test-origin".to_string()),
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
    fn transaction_change_record_encoding_matches_owned_record() {
        let record = ChangeRecord {
            snapshot: JsonSlot::from_json("{\"name\":\"libf\\u00f6\\u4e2d\"}"),
            metadata: JsonSlot::from_json("{\"k\":1}"),
            ..full_record()
        };
        assert_eq!(
            encode_transaction_change_record(&TransactionChangeRecordRef::from(&record))
                .expect("borrowed record should encode"),
            encode_change_record(&record).expect("owned record should encode"),
        );
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
            origin_key: None,
            ..full_record()
        };
        let encoded = encode_change_record(&record).expect("record should encode");
        let decoded =
            decode_change_record(&encoded, record.change_id).expect("record should decode");
        assert_eq!(decoded, record);
    }

    #[test]
    fn change_record_packs_canonical_uuid_ids_and_round_trips() {
        let uuid = "019eb805-60d0-71c0-ade3-b0f0efab9d9a";
        let record = ChangeRecord {
            row_pk: RowPk::from_components(smallvec::smallvec![
                crate::row_pk::RowPkComponent::Uuid(
                    storage_codec::id_string::uuid_bytes_from_canonical(uuid)
                        .expect("canonical UUID"),
                ),
            ])
            .expect("row pk should build"),
            file_id: Some("019eb805-5e65-7270-861d-cb341bc904c8".to_string()),
            ..full_record()
        };
        let encoded = encode_change_record(&record).expect("record should encode");
        let decoded =
            decode_change_record(&encoded, record.change_id).expect("record should decode");
        assert_eq!(decoded, record);
        assert_eq!(
            decoded
                .row_pk
                .as_single_string_owned()
                .expect("one UUID"),
            uuid
        );
    }

    #[test]
    fn change_record_keeps_non_canonical_ids_as_text() {
        // Uppercase hex re-hyphenates differently, so it must stay text to
        // round-trip byte-identically; same for arbitrary plugin keys.
        let record = ChangeRecord {
            row_pk: RowPk::from_parts(vec![
                "019EB805-60D0-71C0-ADE3-B0F0EFAB9D9A".to_string(),
                "row 5 of sheet 2".to_string(),
            ])
            .expect("row pk should build"),
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
