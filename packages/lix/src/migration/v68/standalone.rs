//! Bounded, read-only inventory of v68 standalone changelog rows.

use std::ops::Bound;

use crate::changelog::{CHANGE_SPACE, ChangeId};
use crate::common::{LixError, LixTimestamp};
use crate::json_store::{JsonLoadRequestRef, JsonReadScopeRef, JsonSlot, JsonStoreContext};
use crate::row_pk::RowPk;
use crate::storage_adapter::{
    StorageAdapterRead, StorageBeginScanOptions, StorageCoreProjection, StorageKeyRange,
    StorageProjectedValue,
};

use super::decode_change_record;

/// Fully owned v68 standalone change, including materialized JSON payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::migration) struct V68StandaloneChange {
    pub(in crate::migration) format_version: u32,
    pub(in crate::migration) change_id: ChangeId,
    pub(in crate::migration) account_id: String,
    pub(in crate::migration) schema_key: String,
    pub(in crate::migration) row_pk: RowPk,
    pub(in crate::migration) file_id: Option<String>,
    pub(in crate::migration) snapshot_json: Option<String>,
    pub(in crate::migration) metadata_json: Option<String>,
    pub(in crate::migration) created_at: LixTimestamp,
    pub(in crate::migration) origin_key: Option<String>,
}

/// Scans every v68 `CHANGE_SPACE` row without mutating storage.
///
/// `max_bytes` bounds retained key/value bytes plus materialized out-of-line
/// JSON bytes. Inline JSON is already part of the encoded change value and is
/// therefore not charged twice. The first row that would exceed either bound
/// returns an error instead of a partial inventory.
pub(in crate::migration) async fn preflight_standalone_changelog(
    read: &(impl StorageAdapterRead + ?Sized),
    max_rows: usize,
    max_bytes: usize,
) -> Result<Vec<V68StandaloneChange>, LixError> {
    let full_range = StorageKeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    };
    let mut cursor = read
        .begin_scan(
            CHANGE_SPACE,
            full_range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    let mut changes = Vec::new();
    let mut retained_bytes = 0usize;

    while let Some(entries) = cursor.next_chunk().await? {
        for entry in entries {
            if changes.len() == max_rows {
                return Err(limit_error(format!(
                    "row limit {max_rows} would be exceeded"
                )));
            }
            let key = entry.key.0;
            let change_uuid = uuid::Uuid::from_slice(&key).map_err(|error| {
                preflight_error(format!(
                    "CHANGE_SPACE key must be a 16-byte ChangeId UUID: {error}"
                ))
            })?;
            let change_id = ChangeId::new(change_uuid);
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(preflight_error(
                    "CHANGE_SPACE scan unexpectedly omitted a record value",
                ));
            };
            charge_bytes(&mut retained_bytes, key.len(), max_bytes, "change key")?;
            charge_bytes(
                &mut retained_bytes,
                value.len(),
                max_bytes,
                "encoded change value",
            )?;

            let record = decode_change_record(&value, change_id).map_err(|error| {
                preflight_error(format!("failed to decode change '{change_id}': {error}"))
            })?;
            let snapshot_json = materialize_slot(
                read,
                record.snapshot,
                change_id,
                "snapshot",
                &mut retained_bytes,
                max_bytes,
            )
            .await?;
            let metadata_json = materialize_slot(
                read,
                record.metadata,
                change_id,
                "metadata",
                &mut retained_bytes,
                max_bytes,
            )
            .await?;
            changes.push(V68StandaloneChange {
                format_version: record.format_version,
                change_id,
                account_id: record.account_id,
                schema_key: record.schema_key,
                row_pk: record.row_pk,
                file_id: record.file_id,
                snapshot_json,
                metadata_json,
                created_at: record.created_at,
                origin_key: record.origin_key,
            });
        }
    }

    Ok(changes)
}

async fn materialize_slot(
    read: &(impl StorageAdapterRead + ?Sized),
    slot: JsonSlot,
    change_id: ChangeId,
    field: &str,
    retained_bytes: &mut usize,
    max_bytes: usize,
) -> Result<Option<String>, LixError> {
    let json_ref = match slot {
        JsonSlot::None => return Ok(None),
        JsonSlot::Inline(json) => return Ok(Some(json.into_string())),
        JsonSlot::Ref(json_ref) => json_ref,
    };
    let loaded = JsonStoreContext::new()
        .load_bytes_many(
            read,
            JsonLoadRequestRef {
                refs: std::slice::from_ref(&json_ref),
                scope: JsonReadScopeRef::OutOfBand,
            },
        )
        .await?
        .into_values()
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| {
            preflight_error(format!(
                "change '{change_id}' {field} references missing JSON_SPACE value '{}'",
                json_ref.to_hex()
            ))
        })?;
    charge_bytes(
        retained_bytes,
        loaded.len(),
        max_bytes,
        &format!("change '{change_id}' {field} JSON"),
    )?;
    String::from_utf8(loaded.to_vec())
        .map(Some)
        .map_err(|error| {
            preflight_error(format!(
                "change '{change_id}' {field} JSON '{}' is not UTF-8: {error}",
                json_ref.to_hex()
            ))
        })
}

fn charge_bytes(
    retained_bytes: &mut usize,
    additional: usize,
    max_bytes: usize,
    source: &str,
) -> Result<(), LixError> {
    let total = retained_bytes
        .checked_add(additional)
        .ok_or_else(|| limit_error(format!("byte count overflow while accounting for {source}")))?;
    if total > max_bytes {
        return Err(limit_error(format!(
            "byte limit {max_bytes} would be exceeded by {source} ({total} bytes required)"
        )));
    }
    *retained_bytes = total;
    Ok(())
}

fn limit_error(message: impl std::fmt::Display) -> LixError {
    preflight_error(format!("preflight limit exceeded: {message}"))
}

fn preflight_error(message: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("v68 standalone changelog preflight failed: {message}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_store::{JsonSlotRef, JsonWritePlacementRef, NormalizedJsonRef};
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

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

    fn frozen_change_bytes(
        row_label: &str,
        snapshot: JsonSlotRef<'_>,
        metadata: JsonSlotRef<'_>,
    ) -> Vec<u8> {
        let row_pk = RowPk::from_parts(vec![row_label.to_string()]).expect("fixture row pk");
        crate::storage_codec::encode(
            "frozen v68 standalone change fixture",
            &FrozenV68ChangeRecordRef {
                format_version: 1,
                account_id: crate::ANONYMOUS_ACCOUNT_ID,
                schema_key: "plugin.item",
                row_pk: &row_pk,
                file_id: Some("019eb805-5e65-7270-861d-cb341bc904c8"),
                snapshot,
                metadata,
                created_at: LixTimestamp::expect_parse(
                    "fixture timestamp",
                    "2026-08-18T12:34:56.789Z",
                ),
                origin_key: Some("v68-origin"),
            },
        )
        .expect("fixture should encode")
    }

    async fn committed_read(
        storage: &StorageAdapter<Memory>,
        writes: crate::storage_adapter::StorageWriteSet,
    ) -> crate::storage_adapter::StorageAdapterReadScope<crate::storage_adapter::MemoryRead> {
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("fixture writes should commit");
        storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("fixture read should open")
    }

    #[tokio::test]
    async fn scans_every_page_and_materializes_inline_and_referenced_json() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        let referenced = r#"{"stored":"outside"}"#;
        let refs = JsonStoreContext::new()
            .writer()
            .stage_batch(
                &mut writes,
                JsonWritePlacementRef::OutOfBand,
                [NormalizedJsonRef::new(referenced)],
            )
            .expect("JSON fixture should stage");
        let inline = r#"{"inline":true}"#;
        for index in 0..=crate::storage_adapter::MAX_SCAN_PAGE_ROWS {
            let change_id = ChangeId::for_test_label(&format!("v68-standalone-{index:04}"));
            let (snapshot, metadata) = if index == 0 {
                (JsonSlotRef::Ref(&refs[0]), JsonSlotRef::Inline(inline))
            } else {
                (JsonSlotRef::None, JsonSlotRef::None)
            };
            writes.put(
                CHANGE_SPACE,
                change_id.as_uuid().as_bytes().as_slice(),
                frozen_change_bytes(&format!("row-{index}"), snapshot, metadata),
            );
        }
        let read = committed_read(&storage, writes).await;

        let changes = preflight_standalone_changelog(
            &read,
            crate::storage_adapter::MAX_SCAN_PAGE_ROWS + 1,
            usize::MAX,
        )
        .await
        .expect("complete v68 preflight should succeed");

        assert_eq!(
            changes.len(),
            crate::storage_adapter::MAX_SCAN_PAGE_ROWS + 1
        );
        let materialized = changes
            .iter()
            .find(|change| change.snapshot_json.is_some())
            .expect("referenced fixture should be present");
        assert_eq!(materialized.snapshot_json.as_deref(), Some(referenced));
        assert_eq!(materialized.metadata_json.as_deref(), Some(inline));
        assert_eq!(materialized.origin_key.as_deref(), Some("v68-origin"));
    }

    #[tokio::test]
    async fn reports_row_and_byte_limits_without_returning_partial_results() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        let change_id = ChangeId::for_test_label("v68-bounded");
        writes.put(
            CHANGE_SPACE,
            change_id.as_uuid().as_bytes().as_slice(),
            frozen_change_bytes("bounded", JsonSlotRef::None, JsonSlotRef::None),
        );
        let read = committed_read(&storage, writes).await;

        let row_error = preflight_standalone_changelog(&read, 0, usize::MAX)
            .await
            .expect_err("zero row limit should reject the first row");
        assert!(row_error.to_string().contains("row limit 0"));

        let byte_error = preflight_standalone_changelog(&read, 1, 0)
            .await
            .expect_err("zero byte limit should reject the first key");
        assert!(byte_error.to_string().contains("byte limit 0"));
    }

    #[tokio::test]
    async fn reports_malformed_change_keys_and_missing_json_refs() {
        let malformed_storage = StorageAdapter::new(Memory::new());
        let mut malformed_writes = malformed_storage.new_write_set();
        malformed_writes.put(
            CHANGE_SPACE,
            &[7u8; 15][..],
            frozen_change_bytes("malformed", JsonSlotRef::None, JsonSlotRef::None),
        );
        let malformed_read = committed_read(&malformed_storage, malformed_writes).await;
        let malformed_error = preflight_standalone_changelog(&malformed_read, 1, usize::MAX)
            .await
            .expect_err("15-byte change key should fail");
        assert!(malformed_error.to_string().contains("16-byte ChangeId"));

        let missing_storage = StorageAdapter::new(Memory::new());
        let mut missing_writes = missing_storage.new_write_set();
        let change_id = ChangeId::for_test_label("v68-missing-json");
        let missing_ref = crate::json_store::JsonRef::for_content(b"missing-v68-json");
        missing_writes.put(
            CHANGE_SPACE,
            change_id.as_uuid().as_bytes().as_slice(),
            frozen_change_bytes("missing", JsonSlotRef::Ref(&missing_ref), JsonSlotRef::None),
        );
        let missing_read = committed_read(&missing_storage, missing_writes).await;
        let missing_error = preflight_standalone_changelog(&missing_read, 1, usize::MAX)
            .await
            .expect_err("missing JSON ref should fail");
        assert!(
            missing_error
                .to_string()
                .contains("missing JSON_SPACE value")
        );
        assert!(missing_error.to_string().contains(&missing_ref.to_hex()));
    }
}
