use crate::live_state::constraints::{ScanConstraint, ScanField, ScanOperator};
use crate::live_state::untracked::{
    scan_rows_with_backend, UntrackedScanRequest, UntrackedWriteOperation, UntrackedWriteRow,
};
use crate::version::{
    active_version_file_id, active_version_plugin_key, active_version_schema_key,
    active_version_schema_version, active_version_snapshot_content,
    active_version_storage_version_id,
};
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct WorkspaceActiveVersionRow {
    pub(crate) entity_id: String,
    pub(crate) version_id: String,
}

pub(crate) async fn load_workspace_active_version_row_with_backend(
    backend: &dyn LixBackend,
) -> Result<Option<WorkspaceActiveVersionRow>, LixError> {
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
    let mut rows = scan_rows_with_backend(
        backend,
        &UntrackedScanRequest {
            schema_key: active_version_schema_key().to_string(),
            version_id: active_version_storage_version_id().to_string(),
            constraints,
            required_columns: vec!["version_id".to_string()],
        },
    )
    .await?;
    rows.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let version_id = row.property_text("version_id").ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "workspace active version row is missing payload version_id",
        )
    })?;
    Ok(Some(WorkspaceActiveVersionRow {
        entity_id: row.entity_id,
        version_id,
    }))
}

pub(crate) fn workspace_active_version_write_row(
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
