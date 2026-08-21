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

pub(crate) fn encode_change_record(record: &ChangeRecord) -> Result<Vec<u8>, LixError> {
    encode_change_record_ref(&ChangeRecordRef {
        format_version: record.format_version,
        account_id: &record.account_id,
        schema_key: &record.schema_key,
        row_pk: &record.row_pk,
        file_id: record.file_id.as_deref(),
        metadata: record.metadata.as_ref(),
        snapshot: record.snapshot.as_deref(),
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
            metadata: record.metadata.as_ref(),
            snapshot: record.snapshot.as_deref(),
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
        metadata: record.metadata,
        snapshot: record.snapshot,
        created_at: record.created_at,
        origin_key: record.origin_key,
    })
}

pub(crate) fn append_transaction_change_record(
    bytes: &mut Vec<u8>,
    record: &TransactionChangeRecordRef<'_>,
) -> Result<std::ops::Range<usize>, LixError> {
    append_change_record_ref(
        bytes,
        &ChangeRecordRef {
            format_version: record.format_version,
            account_id: record.account_id,
            schema_key: record.schema_key,
            row_pk: record.row_pk,
            file_id: record.file_id,
            metadata: record.metadata,
            snapshot: record.snapshot,
            created_at: record.created_at,
            origin_key: record.origin_key,
        },
    )
}

fn encode_change_record_ref(record: &ChangeRecordRef<'_>) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    append_change_record_ref(&mut bytes, record)?;
    Ok(bytes)
}

fn append_change_record_ref(
    bytes: &mut Vec<u8>,
    record: &ChangeRecordRef<'_>,
) -> Result<std::ops::Range<usize>, LixError> {
    validate_change_record_payload(
        record.snapshot,
        record.metadata,
        LixError::CODE_INTERNAL_ERROR,
        "cannot encode change record",
    )?;
    // change_id is the storage key; the value intentionally omits it.
    storage_codec::append("change record", bytes, record)
}

fn validate_change_record_payload(
    snapshot: Option<&[u8]>,
    metadata: Option<&lix_schema::Jsonb>,
    error_code: &str,
    context: &str,
) -> Result<(), LixError> {
    if snapshot.is_none() && metadata.is_some() {
        return Err(LixError::new(
            error_code,
            format!("{context}: a deleted row cannot carry metadata"),
        ));
    }
    Ok(())
}

pub(crate) fn decode_change_record(
    bytes: &[u8],
    change_id: ChangeId,
) -> Result<ChangeRecord, LixError> {
    let view: ChangeRecordView<'_> = storage_codec::decode("change record", bytes)?;
    validate_change_record_payload(
        view.snapshot.as_deref(),
        view.metadata.as_ref(),
        LixError::CODE_STORAGE_ERROR,
        "invalid stored change record",
    )?;
    let record = ChangeRecord {
        format_version: view.format_version,
        change_id,
        account_id: view.account_id.to_string(),
        schema_key: view.schema_key.to_string(),
        row_pk: view.row_pk,
        file_id: view.file_id,
        metadata: view.metadata,
        snapshot: view.snapshot,
        created_at: view.created_at,
        origin_key: view.origin_key,
    };
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::ChangeId;
    use crate::changelog::CommitId;
    use crate::common::LixTimestamp;
    use crate::row_pk::RowPk;

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
            metadata: Some(lix_schema::Jsonb::from_value(serde_json::json!({
                "source": "metadata"
            }))),
            snapshot: Some(vec![1, 2, 3, 4]),
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
    fn change_record_keeps_snapshot_and_metadata_at_v69_ordinals() {
        let record = full_record();
        let encoded = encode_change_record(&record).expect("record should encode");
        let snapshot = record.snapshot.as_deref().expect("fixture snapshot");
        let metadata = record
            .metadata
            .as_ref()
            .expect("fixture metadata")
            .binary()
            .expect("fixture metadata should encode");
        let snapshot_offset = encoded
            .windows(snapshot.len())
            .position(|window| window == snapshot)
            .expect("packed v69 snapshot bytes");
        let metadata_offset = encoded
            .windows(metadata.len())
            .position(|window| window == metadata.as_ref())
            .expect("packed v69 metadata bytes");
        assert!(
            snapshot_offset < metadata_offset,
            "v69 keeps snapshot at ordinal 5 and metadata at ordinal 6"
        );
    }

    #[test]
    fn transaction_change_record_encoding_matches_owned_record() {
        let record = ChangeRecord {
            metadata: Some(lix_schema::Jsonb::from_value(serde_json::json!({"k": 1}))),
            ..full_record()
        };
        assert_eq!(
            encode_transaction_change_record(&TransactionChangeRecordRef::from(&record))
                .expect("borrowed record should encode"),
            encode_change_record(&record).expect("owned record should encode"),
        );
    }

    #[test]
    fn change_record_round_trips_native_jsonb_metadata() {
        let record = ChangeRecord {
            metadata: Some(lix_schema::Jsonb::from_value(serde_json::json!({
                "name": "libf\u{00f6}\u{4e2d}"
            }))),
            ..full_record()
        };
        let encoded = encode_change_record(&record).expect("record should encode");
        let decoded =
            decode_change_record(&encoded, record.change_id).expect("record should decode");
        assert_eq!(decoded, record);
        assert!(
            decoded
                .metadata
                .as_ref()
                .is_some_and(lix_schema::Jsonb::is_binary)
        );
    }

    #[test]
    fn change_record_metadata_encoding_is_deterministic() {
        let left = ChangeRecord {
            metadata: Some(lix_schema::Jsonb::from_value(
                serde_json::from_str(r#"{"b":2,"a":1}"#).expect("left metadata JSON"),
            )),
            ..full_record()
        };
        let right = ChangeRecord {
            metadata: Some(lix_schema::Jsonb::from_value(
                serde_json::from_str(r#"{"a":1,"b":2}"#).expect("right metadata JSON"),
            )),
            ..full_record()
        };

        assert_eq!(
            encode_change_record(&left).expect("left record should encode"),
            encode_change_record(&right).expect("right record should encode"),
        );
    }

    #[test]
    fn change_record_round_trips_with_empty_options() {
        let record = ChangeRecord {
            file_id: None,
            metadata: None,
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
            decoded.row_pk.as_single_string_owned().expect("one UUID"),
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

    #[test]
    fn change_record_rejects_corrupt_binary_jsonb_metadata() {
        let record = full_record();
        let metadata = record
            .metadata
            .as_ref()
            .expect("fixture metadata")
            .binary()
            .expect("fixture metadata should encode")
            .into_owned();
        let mut encoded = encode_change_record(&record).expect("record should encode");
        let offset = encoded
            .windows(metadata.len())
            .position(|window| window == metadata)
            .expect("encoded record should contain canonical binary metadata");
        encoded[offset] = 0xff;

        let error = decode_change_record(&encoded, record.change_id)
            .expect_err("corrupt binary JSONB metadata must fail decode");
        assert!(
            error.message.contains("invalid canonical binary JSONB"),
            "{}",
            error.message
        );
    }

    #[test]
    fn owned_change_record_rejects_delete_metadata_before_encoding_or_appending() {
        let record = ChangeRecord {
            snapshot: None,
            metadata: Some(lix_schema::Jsonb::from_value(serde_json::json!({
                "source": "invalid-delete"
            }))),
            ..full_record()
        };

        let encode_error =
            encode_change_record(&record).expect_err("delete metadata must fail owned encoding");
        assert_eq!(encode_error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(
            encode_error
                .message
                .contains("deleted row cannot carry metadata")
        );

        let mut destination = vec![0xaa, 0xbb];
        let before = destination.clone();
        let append_error = append_change_record(&mut destination, &record)
            .expect_err("delete metadata must fail owned append");
        assert_eq!(append_error.code, LixError::CODE_INTERNAL_ERROR);
        assert_eq!(
            destination, before,
            "validation must precede buffer mutation"
        );
    }

    #[test]
    fn transaction_change_record_rejects_delete_metadata_before_encoding_or_appending() {
        let record = ChangeRecord {
            snapshot: None,
            metadata: Some(lix_schema::Jsonb::from_value(serde_json::json!({
                "source": "invalid-delete"
            }))),
            ..full_record()
        };
        let borrowed = TransactionChangeRecordRef::from(&record);

        let encode_error = encode_transaction_change_record(&borrowed)
            .expect_err("delete metadata must fail transaction encoding");
        assert_eq!(encode_error.code, LixError::CODE_INTERNAL_ERROR);

        let mut destination = vec![0xcc, 0xdd];
        let before = destination.clone();
        let append_error = append_transaction_change_record(&mut destination, &borrowed)
            .expect_err("delete metadata must fail transaction append");
        assert_eq!(append_error.code, LixError::CODE_INTERNAL_ERROR);
        assert_eq!(
            destination, before,
            "validation must precede buffer mutation"
        );
    }

    #[test]
    fn change_record_decode_rejects_stored_delete_metadata() {
        let record = ChangeRecord {
            snapshot: None,
            metadata: Some(lix_schema::Jsonb::from_value(serde_json::json!({
                "source": "corrupt-storage"
            }))),
            ..full_record()
        };
        let noncanonical = ChangeRecordRef {
            format_version: record.format_version,
            account_id: &record.account_id,
            schema_key: &record.schema_key,
            row_pk: &record.row_pk,
            file_id: record.file_id.as_deref(),
            metadata: record.metadata.as_ref(),
            snapshot: record.snapshot.as_deref(),
            created_at: record.created_at,
            origin_key: record.origin_key.as_deref(),
        };
        // Bypass the changelog encoder to model structurally valid but
        // semantically noncanonical bytes already present in storage.
        let encoded = storage_codec::encode("noncanonical change fixture", &noncanonical)
            .expect("fixture should be structurally encodable");

        let error = decode_change_record(&encoded, record.change_id)
            .expect_err("stored delete metadata must fail closed");
        assert_eq!(error.code, LixError::CODE_STORAGE_ERROR);
        assert!(error.message.contains("deleted row cannot carry metadata"));
    }
}
