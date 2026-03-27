use crate::backend::QueryExecutor;
use crate::live_state::constraints::{ScanConstraint, ScanField, ScanOperator};
use crate::live_state::raw::{scan_rows_with_executor, RawStorage};
use crate::live_state::schema_access::{payload_column_name_for_schema, tracked_relation_name};
use crate::live_state::untracked::{UntrackedWriteOperation, UntrackedWriteRow};
use crate::version::{
    active_version_file_id, active_version_plugin_key, active_version_schema_key,
    active_version_schema_version, active_version_snapshot_content,
    active_version_storage_version_id, version_descriptor_file_id, version_descriptor_schema_key,
    version_descriptor_storage_version_id,
};
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ActiveVersionRow {
    pub entity_id: String,
    pub version_id: String,
}

pub async fn load_active_version_with_backend(
    backend: &dyn LixBackend,
) -> Result<Option<ActiveVersionRow>, LixError> {
    let mut executor = backend;
    load_active_version_with_executor(&mut executor).await
}

pub(crate) async fn load_version_id_by_descriptor_name_with_backend(
    backend: &dyn LixBackend,
    name: &str,
) -> Result<Option<String>, LixError> {
    let name_column =
        payload_column_name_for_schema(version_descriptor_schema_key(), None, "name")?;
    let result = backend
        .execute(
            &format!(
                "SELECT entity_id \
                 FROM {table_name} \
                 WHERE schema_key = $1 \
                   AND file_id = $2 \
                   AND version_id = $3 \
                   AND is_tombstone = 0 \
                   AND {name_column} = $4 \
                 LIMIT 1",
                table_name = tracked_relation_name(version_descriptor_schema_key()),
                name_column = name_column,
            ),
            &[
                Value::Text(version_descriptor_schema_key().to_string()),
                Value::Text(version_descriptor_file_id().to_string()),
                Value::Text(version_descriptor_storage_version_id().to_string()),
                Value::Text(name.to_string()),
            ],
        )
        .await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    match row.first() {
        Some(Value::Text(version_id)) if !version_id.is_empty() => Ok(Some(version_id.clone())),
        Some(Value::Text(_)) | None => Ok(None),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("version descriptor entity_id must be text, got {other:?}"),
        )),
    }
}

pub fn active_version_write_row(
    entity_id: &str,
    version_id: &str,
    timestamp: &str,
) -> UntrackedWriteRow {
    UntrackedWriteRow {
        entity_id: entity_id.to_string(),
        schema_key: active_version_schema_key().to_string(),
        schema_version: active_version_schema_version().to_string(),
        file_id: active_version_file_id().to_string(),
        version_id: active_version_storage_version_id().to_string(),
        global: true,
        plugin_key: active_version_plugin_key().to_string(),
        metadata: None,
        writer_key: None,
        snapshot_content: Some(active_version_snapshot_content(entity_id, version_id)),
        created_at: Some(timestamp.to_string()),
        updated_at: timestamp.to_string(),
        operation: UntrackedWriteOperation::Upsert,
    }
}

pub(crate) async fn load_active_version_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Option<ActiveVersionRow>, LixError> {
    let constraints = vec![
        ScanConstraint {
            field: ScanField::FileId,
            operator: ScanOperator::Eq(Value::Text(active_version_file_id().to_string())),
        },
        ScanConstraint {
            field: ScanField::PluginKey,
            operator: ScanOperator::Eq(Value::Text(active_version_plugin_key().to_string())),
        },
    ];
    let required_columns = vec!["version_id".to_string()];
    let mut rows = scan_rows_with_executor(
        executor,
        RawStorage::Untracked,
        active_version_schema_key(),
        active_version_storage_version_id(),
        &constraints,
        &required_columns,
    )
    .await?;
    rows.sort_by(|left, right| right.updated_at().cmp(left.updated_at()));
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let version_id = row.property_text("version_id").ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "active version row is missing payload version_id",
        )
    })?;
    Ok(Some(ActiveVersionRow {
        entity_id: row.entity_id().to_string(),
        version_id,
    }))
}
