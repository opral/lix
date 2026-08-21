use crate::changelog::{ChangeId, ChangeRecord};
use crate::row_pk::RowPk;

pub(crate) fn test_change_record() -> ChangeRecord {
    let row_pk = RowPk::single("row-1");
    let snapshot = crate::plugin::runtime::WasmTypedRow::from_test_json_unchecked(
        &row_pk,
        &serde_json::json!({"value": 1}),
    )
    .expect("test change row should type")
    .durable_payload()
    .expect("test change row should encode")
    .to_vec();
    ChangeRecord {
        format_version: 1,
        change_id: ChangeId::for_test_label("change-1"),
        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
        schema_key: "message".to_string(),
        row_pk,
        file_id: Some("file-1".to_string()),
        metadata: None,
        snapshot: Some(snapshot),
        created_at: crate::common::LixTimestamp::expect_parse("created_at", "2026-05-12T00:00:00Z"),
        origin_key: None,
    }
}
