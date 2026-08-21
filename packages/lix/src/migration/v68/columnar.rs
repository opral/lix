//! Frozen reconstruction of v68 row-columnar commit members.
//!
//! Protocol v68 stored lossless plugin snapshots as Arrow values plus a JSON
//! row identity. Migration must be able to recover this authority even after
//! the redundant standalone changelog record has been collected.

use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};

use crate::LixError;
use crate::changelog::{ChangeId, CommitId};
use crate::columnar_row_group::{
    RowGroupDataType, RowGroupManifest, RowGroupSetId, load_row_group_batch,
    load_row_group_manifest,
};
use crate::json_store::LegacyJsonValue;
use crate::migration::v68::V68ChangeRecord;
use crate::row_pk::RowPk;
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{ColumnarMutationPartSet, TrackedStateBaseCoordinate};

#[derive(Debug)]
pub(in crate::migration) struct V68ColumnarChange {
    pub(in crate::migration) record: V68ChangeRecord,
    pub(in crate::migration) base_coordinate: TrackedStateBaseCoordinate,
}

pub(in crate::migration) async fn load_columnar_changes(
    read: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
    parts: &ColumnarMutationPartSet,
    account_id: &str,
) -> Result<Vec<V68ColumnarChange>, LixError> {
    let id = RowGroupSetId::new(parts.row_group_set_id);
    let manifest = load_row_group_manifest(read, id)
        .await?
        .ok_or_else(|| migration_error("columnar mutation manifest is missing"))?;
    validate_manifest(commit_id, &manifest, parts)?;
    let projection = (0..manifest.fields.len()).collect::<Vec<_>>();
    let mut changes = Vec::with_capacity(parts.row_count as usize);
    for group_index in 0..manifest.groups.len() {
        let batch = load_row_group_batch(read, id, &manifest, group_index, &projection).await?;
        for row_index in 0..batch.num_rows() {
            let change_id = addressable_change_id(commit_id, changes.len())?;
            changes.push(V68ColumnarChange {
                record: decode_change(&manifest, &batch, row_index, parts, change_id, account_id)?,
                base_coordinate: TrackedStateBaseCoordinate {
                    base_commit_id: commit_id,
                    group_index: u32::try_from(group_index)
                        .map_err(|_| migration_error("columnar group index exceeds u32"))?,
                    row_index: u32::try_from(row_index)
                        .map_err(|_| migration_error("columnar row index exceeds u32"))?,
                },
            });
        }
    }
    if changes.len() != parts.row_count as usize {
        return Err(migration_error(
            "columnar mutation rows disagree with commit authority",
        ));
    }
    Ok(changes)
}

fn validate_manifest(
    commit_id: CommitId,
    manifest: &RowGroupManifest,
    parts: &ColumnarMutationPartSet,
) -> Result<(), LixError> {
    if parts.owner_commit_id != *commit_id.as_uuid().as_bytes()
        || crate::row_columnar::row_group_set_id(commit_id, &parts.schema_key).as_bytes()
            != parts.row_group_set_id
        || manifest.content_digest()? != parts.manifest_digest
        || manifest.namespace != parts.schema_key
        || crate::row_columnar::row_identity_column_index(manifest).is_none()
        || manifest
            .groups
            .iter()
            .map(|group| group.row_count)
            .collect::<Vec<_>>()
            != parts.group_row_counts
        || manifest.row_count() != u64::from(parts.row_count)
    {
        return Err(migration_error(
            "columnar mutation manifest disagrees with commit authority",
        ));
    }
    Ok(())
}

fn decode_change(
    manifest: &RowGroupManifest,
    batch: &datafusion::arrow::record_batch::RecordBatch,
    row_index: usize,
    parts: &ColumnarMutationPartSet,
    change_id: ChangeId,
    account_id: &str,
) -> Result<V68ChangeRecord, LixError> {
    let identity = batch
        .column(batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| migration_error("columnar mutation identity is not UTF-8"))?;
    if identity.is_null(row_index) {
        return Err(migration_error("columnar mutation identity is null"));
    }
    let row_pk = RowPk::from_json_array_text(identity.value(row_index))
        .map_err(|error| migration_error(error.to_string()))?;
    let mut snapshot = serde_json::Map::new();
    for (column_index, field) in manifest
        .fields
        .iter()
        .take(manifest.fields.len() - 1)
        .enumerate()
    {
        let column = batch.column(column_index);
        let value = if column.is_null(row_index) {
            serde_json::Value::Null
        } else {
            match field.data_type {
                RowGroupDataType::String => {
                    let value = column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| migration_error("columnar string type drift"))?
                        .value(row_index);
                    if field.metadata.get("lix.value_type").map(String::as_str) == Some("json") {
                        serde_json::from_str(value).map_err(|error| {
                            migration_error(format!("columnar JSON value is invalid: {error}"))
                        })?
                    } else {
                        serde_json::Value::String(value.to_owned())
                    }
                }
                RowGroupDataType::Int64 => serde_json::Value::Number(
                    column
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| migration_error("columnar integer type drift"))?
                        .value(row_index)
                        .into(),
                ),
                RowGroupDataType::Float64 => {
                    let value = column
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| migration_error("columnar number type drift"))?
                        .value(row_index);
                    serde_json::Number::from_f64(value)
                        .map(serde_json::Value::Number)
                        .ok_or_else(|| migration_error("columnar number is non-finite"))?
                }
                RowGroupDataType::Boolean => serde_json::Value::Bool(
                    column
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| migration_error("columnar boolean type drift"))?
                        .value(row_index),
                ),
            }
        };
        snapshot.insert(field.name.clone(), value);
    }
    let snapshot =
        serde_json::to_string(&snapshot).map_err(|error| migration_error(error.to_string()))?;
    Ok(V68ChangeRecord {
        format_version: 2,
        change_id,
        account_id: account_id.to_owned(),
        schema_key: parts.schema_key.clone(),
        row_pk,
        file_id: None,
        snapshot: LegacyJsonValue::from_json(&snapshot),
        metadata: LegacyJsonValue::None,
        created_at: parts.uniform_updated_at,
        origin_key: parts.origin_key.clone(),
    })
}

fn addressable_change_id(commit_id: CommitId, ordinal: usize) -> Result<ChangeId, LixError> {
    let packed = u32::try_from(ordinal)
        .ok()
        .and_then(|ordinal| ordinal.checked_add(1))
        .ok_or_else(|| migration_error("columnar mutation address overflows"))?;
    let mut bytes = *commit_id.as_uuid().as_bytes();
    if bytes[12..] != [0; 4] {
        return Err(migration_error("commit id has no direct-address space"));
    }
    bytes[12..].copy_from_slice(&packed.to_be_bytes());
    Ok(ChangeId::new(uuid::Uuid::from_bytes(bytes)))
}

fn migration_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_ERROR_MIGRATION_FAILED", message.into())
}
