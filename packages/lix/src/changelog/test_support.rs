use crate::changelog::{ChangeId, ChangeRecord};
use crate::row_pk::RowPk;

pub(crate) fn test_change_record() -> ChangeRecord {
    ChangeRecord {
        format_version: 1,
        change_id: ChangeId::for_test_label("change-1"),
        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
        schema_key: "message".to_string(),
        row_pk: RowPk::single("row-1"),
        file_id: Some("file-1".to_string()),
        snapshot: crate::changelog::ChangePayload::Tombstone,
        metadata: crate::json_store::JsonSlot::None,
        created_at: crate::common::LixTimestamp::expect_parse("created_at", "2026-05-12T00:00:00Z"),
        origin_key: None,
    }
}
