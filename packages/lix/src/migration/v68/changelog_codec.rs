use crate::changelog::{ChangeId, ChangeRecord};
use crate::common::{LixError, LixTimestamp};
use crate::json_store::JsonSlot;
use crate::row_pk::RowPk;

/// The packed `ChangeRecord` value layout written by repository protocol v68.
///
/// Keep this field order and each field codec byte-for-byte aligned with the
/// v68 `ChangeRecordView` on `origin/main`. Packed MUSLI records are positional;
/// the current record's inserted `typed_payload` field makes its decoder
/// intentionally incompatible with these bytes.
#[derive(musli::Decode)]
#[musli(packed)]
struct ChangeRecordView<'a> {
    format_version: u32,
    account_id: &'a str,
    schema_key: &'a str,
    row_pk: RowPk,
    #[musli(with = crate::storage_codec::option_id_string)]
    file_id: Option<String>,
    #[musli(with = crate::json_store::json_slot_storage)]
    snapshot: JsonSlot,
    #[musli(with = crate::json_store::json_slot_storage)]
    metadata: JsonSlot,
    created_at: LixTimestamp,
    #[musli(with = crate::storage_codec::option)]
    origin_key: Option<String>,
}

/// Decodes one frozen v68 changelog value and upgrades it to the current
/// in-memory shape. V68 had no native typed-row payload, so that field is
/// always absent after decoding.
pub(in crate::migration) fn decode_change_record(
    bytes: &[u8],
    change_id: ChangeId,
) -> Result<ChangeRecord, LixError> {
    let view: ChangeRecordView<'_> = crate::storage_codec::decode("v68 change record", bytes)?;
    Ok(ChangeRecord {
        format_version: view.format_version,
        change_id,
        account_id: view.account_id.to_string(),
        schema_key: view.schema_key.to_string(),
        row_pk: view.row_pk,
        file_id: view.file_id,
        snapshot: view.snapshot,
        metadata: view.metadata,
        typed_payload: None,
        created_at: view.created_at,
        origin_key: view.origin_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_store::{JsonRef, JsonSlotRef};

    #[derive(musli::Encode)]
    #[musli(packed)]
    struct FrozenV68ChangeRecordRef<'a> {
        format_version: u32,
        account_id: &'a str,
        schema_key: &'a str,
        row_pk: &'a RowPk,
        #[musli(with = crate::storage_codec::option_id_string)]
        file_id: Option<&'a str>,
        #[musli(with = crate::json_store::json_slot_storage_ref)]
        snapshot: JsonSlotRef<'a>,
        #[musli(with = crate::json_store::json_slot_storage_ref)]
        metadata: JsonSlotRef<'a>,
        created_at: LixTimestamp,
        #[musli(with = crate::storage_codec::option)]
        origin_key: Option<&'a str>,
    }

    fn frozen_v68_bytes() -> (Vec<u8>, RowPk, LixTimestamp) {
        let row_pk = RowPk::from_parts(vec!["row-a".to_string(), "row-b".to_string()])
            .expect("fixture row pk");
        let created_at =
            LixTimestamp::expect_parse("fixture timestamp", "2026-08-18T12:34:56.789Z");
        let snapshot_ref = JsonRef::for_content(b"v68 snapshot");
        let record = FrozenV68ChangeRecordRef {
            format_version: 1,
            account_id: crate::ANONYMOUS_ACCOUNT_ID,
            schema_key: "plugin.item",
            row_pk: &row_pk,
            file_id: Some("019eb805-5e65-7270-861d-cb341bc904c8"),
            snapshot: JsonSlotRef::Ref(&snapshot_ref),
            metadata: JsonSlotRef::Inline(r#"{"source":"v68"}"#),
            created_at,
            origin_key: Some("legacy-origin"),
        };
        let bytes = crate::storage_codec::encode("frozen v68 fixture", &record)
            .expect("v68 fixture should encode");
        (bytes, row_pk, created_at)
    }

    #[test]
    fn decodes_frozen_v68_change_record_bytes() {
        let (bytes, row_pk, created_at) = frozen_v68_bytes();
        let change_id = ChangeId::for_test_label("v68-change-record");

        let decoded = decode_change_record(&bytes, change_id).expect("v68 bytes should decode");

        assert_eq!(decoded.format_version, 1);
        assert_eq!(decoded.change_id, change_id);
        assert_eq!(decoded.account_id, crate::ANONYMOUS_ACCOUNT_ID);
        assert_eq!(decoded.schema_key, "plugin.item");
        assert_eq!(decoded.row_pk, row_pk);
        assert_eq!(
            decoded.file_id.as_deref(),
            Some("019eb805-5e65-7270-861d-cb341bc904c8")
        );
        assert_eq!(
            decoded.snapshot,
            JsonSlot::Ref(JsonRef::for_content(b"v68 snapshot"))
        );
        assert_eq!(decoded.metadata, JsonSlot::from_json(r#"{"source":"v68"}"#));
        assert_eq!(decoded.typed_payload, None);
        assert_eq!(decoded.created_at, created_at);
        assert_eq!(decoded.origin_key.as_deref(), Some("legacy-origin"));
    }

    #[test]
    fn current_packed_decoder_does_not_decode_v68_bytes() {
        let (bytes, _, _) = frozen_v68_bytes();
        let change_id = ChangeId::for_test_label("v68-current-decoder-proof");

        assert!(crate::changelog::decode_change_record(&bytes, change_id).is_err());
    }
}
