use crate::backend::QueryExecutor;
use crate::errors::classification::is_missing_relation_error;
use crate::live_state::storage::{
    load_live_row_access_with_executor, quoted_live_table_name, selected_columns,
    selected_projection_sql,
};
use crate::version::{
    active_version_file_id, active_version_plugin_key, active_version_schema_key,
    active_version_schema_version, active_version_snapshot_content,
    active_version_storage_version_id, version_ref_file_id, version_ref_plugin_key,
    version_ref_schema_key, version_ref_schema_version, version_ref_snapshot_content,
    version_ref_storage_version_id,
};
use crate::{LixBackend, LixError};

use super::contracts::{
    ActiveVersionRow, ExactUntrackedRowRequest, UntrackedWriteOperation, UntrackedWriteRow,
    VersionRefRow,
};
use super::read::{decode_untracked_row, load_exact_row_with_executor};

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
    let access = load_live_row_access_with_executor(executor, active_version_schema_key()).await?;
    let selected_columns = selected_columns(&access, &["version_id".to_string()], "untracked")?;
    let projection = selected_projection_sql(&selected_columns);
    let version_column = selected_columns.first().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "active version scan requires version_id",
        )
    })?;
    let sql = format!(
        "SELECT entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, metadata, writer_key, created_at, updated_at{projection} \
         FROM {table_name} \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}' \
           AND untracked = true \
           AND {payload_column} IS NOT NULL \
         ORDER BY updated_at DESC \
         LIMIT 1",
        projection = projection,
        table_name = quoted_live_table_name(active_version_schema_key()),
        schema_key = crate::live_state::constraints::escape_sql_string(active_version_schema_key()),
        file_id = crate::live_state::constraints::escape_sql_string(active_version_file_id()),
        version_id = crate::live_state::constraints::escape_sql_string(active_version_storage_version_id()),
        payload_column = crate::live_state::constraints::quote_ident(&version_column.column_name),
    );
    let result = match executor.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(error) if is_missing_relation_error(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let decoded = decode_untracked_row(row, &selected_columns, active_version_schema_key())?;
    let version_id = decoded.property_text("version_id").ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "active version row is missing payload version_id",
        )
    })?;
    Ok(Some(ActiveVersionRow {
        entity_id: decoded.entity_id,
        version_id,
    }))
}

pub(crate) async fn load_version_ref_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    let request = ExactUntrackedRowRequest {
        schema_key: version_ref_schema_key().to_string(),
        version_id: version_ref_storage_version_id().to_string(),
        entity_id: version_id.to_string(),
        file_id: Some(version_ref_file_id().to_string()),
    };
    let Some(row) = load_exact_row_with_executor(executor, &request).await? else {
        return Ok(None);
    };
    let commit_id = row.property_text("commit_id").ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("version ref row for '{}' is missing commit_id", version_id),
        )
    })?;
    Ok(Some(VersionRefRow {
        version_id: row.entity_id,
        commit_id,
    }))
}
