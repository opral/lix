use crate::backend::QueryExecutor;
use crate::live_state::constraints::{ScanConstraint, ScanField, ScanOperator};
use crate::live_state::raw::{load_exact_row_with_executor, scan_rows_with_executor, RawStorage};
use crate::live_state::untracked::{UntrackedWriteOperation, UntrackedWriteRow};
use crate::version::{
    active_version_file_id, active_version_plugin_key, active_version_schema_key,
    active_version_schema_version, active_version_snapshot_content,
    active_version_storage_version_id, version_ref_file_id, version_ref_plugin_key,
    version_ref_schema_key, version_ref_schema_version, version_ref_snapshot_content,
    version_ref_storage_version_id,
};
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ActiveVersionRow {
    pub entity_id: String,
    pub version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VersionRefRow {
    pub version_id: String,
    pub commit_id: String,
}

pub async fn load_active_version_with_backend(
    backend: &dyn LixBackend,
) -> Result<Option<ActiveVersionRow>, LixError> {
    let mut executor = backend;
    load_active_version_with_executor(&mut executor).await
}

pub async fn load_version_ref_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    let mut executor = backend;
    load_version_ref_with_executor(&mut executor, version_id).await
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

pub fn version_ref_write_row(
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> UntrackedWriteRow {
    UntrackedWriteRow {
        entity_id: version_id.to_string(),
        schema_key: version_ref_schema_key().to_string(),
        schema_version: version_ref_schema_version().to_string(),
        file_id: version_ref_file_id().to_string(),
        version_id: version_ref_storage_version_id().to_string(),
        global: true,
        plugin_key: version_ref_plugin_key().to_string(),
        metadata: None,
        writer_key: None,
        snapshot_content: Some(version_ref_snapshot_content(version_id, commit_id)),
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

pub(crate) async fn load_version_ref_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    let Some(row) = load_exact_row_with_executor(
        executor,
        RawStorage::Untracked,
        version_ref_schema_key(),
        version_ref_storage_version_id(),
        version_id,
        Some(version_ref_file_id()),
    )
    .await?
    else {
        return Ok(None);
    };
    if row.plugin_key() != version_ref_plugin_key() {
        return Ok(None);
    }
    let commit_id = row.property_text("commit_id").ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("version ref row for '{}' is missing commit_id", version_id),
        )
    })?;
    Ok(Some(VersionRefRow {
        version_id: row.entity_id().to_string(),
        commit_id,
    }))
}
