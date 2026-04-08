use crate::backend::QueryExecutor;
use crate::common::errors::classification::is_missing_relation_error;
use crate::contracts::artifacts::{batch_row_constraints, exact_row_constraints};
use crate::live_state::storage::{
    build_partitioned_scan_sql, load_live_row_access_with_executor, required_bool_cell,
    required_text_cell, selected_columns, selected_projection_sql, text_from_value, ScanSqlRequest,
};
use crate::{LixBackend, LixError, Value};

use super::contracts::{
    BatchTrackedRowRequest, ExactTrackedRowRequest, TrackedRow, TrackedScanRequest,
    TrackedTombstoneMarker,
};

pub async fn load_exact_row_with_backend(
    backend: &dyn LixBackend,
    request: &ExactTrackedRowRequest,
) -> Result<Option<TrackedRow>, LixError> {
    let mut executor = backend;
    load_exact_row_with_executor(&mut executor, request).await
}

pub async fn load_exact_rows_with_backend(
    backend: &dyn LixBackend,
    request: &BatchTrackedRowRequest,
) -> Result<Vec<TrackedRow>, LixError> {
    let mut executor = backend;
    load_exact_rows_with_executor(&mut executor, request).await
}

pub async fn scan_rows_with_backend(
    backend: &dyn LixBackend,
    request: &TrackedScanRequest,
) -> Result<Vec<TrackedRow>, LixError> {
    let mut executor = backend;
    scan_rows_with_executor(&mut executor, request).await
}

pub(crate) async fn load_exact_row_with_executor(
    executor: &mut dyn QueryExecutor,
    request: &ExactTrackedRowRequest,
) -> Result<Option<TrackedRow>, LixError> {
    let scan_request = TrackedScanRequest {
        schema_key: request.schema_key.clone(),
        version_id: request.version_id.clone(),
        constraints: exact_row_constraints(request),
        required_columns: Vec::new(),
    };
    let rows = scan_rows_with_limit_and_order(
        executor,
        &scan_request,
        Some(2),
        &["updated_at DESC", "created_at DESC", "change_id DESC"],
    )
    .await?;
    if rows.len() > 1 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!(
                "expected at most one tracked row for schema '{}' entity '{}' version '{}'",
                request.schema_key, request.entity_id, request.version_id
            ),
        ));
    }
    Ok(rows.into_iter().next())
}

pub(crate) async fn load_exact_tombstone_with_executor(
    executor: &mut dyn QueryExecutor,
    request: &ExactTrackedRowRequest,
) -> Result<Option<TrackedTombstoneMarker>, LixError> {
    let scan_request = TrackedScanRequest {
        schema_key: request.schema_key.clone(),
        version_id: request.version_id.clone(),
        constraints: exact_row_constraints(request),
        required_columns: Vec::new(),
    };
    let rows = scan_tombstones_with_limit_and_order(
        executor,
        &scan_request,
        Some(2),
        &["updated_at DESC", "created_at DESC", "change_id DESC"],
    )
    .await?;
    if rows.len() > 1 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!(
                "expected at most one tracked tombstone for schema '{}' entity '{}' version '{}'",
                request.schema_key, request.entity_id, request.version_id
            ),
        ));
    }
    Ok(rows.into_iter().next())
}

pub(crate) async fn load_exact_rows_with_executor(
    executor: &mut dyn QueryExecutor,
    request: &BatchTrackedRowRequest,
) -> Result<Vec<TrackedRow>, LixError> {
    if request.entity_ids.is_empty() {
        return Ok(Vec::new());
    }

    scan_rows_with_limit_and_order(
        executor,
        &TrackedScanRequest {
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            constraints: batch_row_constraints(request),
            required_columns: Vec::new(),
        },
        None,
        &["entity_id ASC", "file_id ASC"],
    )
    .await
}

pub(crate) async fn scan_rows_with_executor(
    executor: &mut dyn QueryExecutor,
    request: &TrackedScanRequest,
) -> Result<Vec<TrackedRow>, LixError> {
    scan_rows_with_limit_and_order(executor, request, None, &["entity_id ASC", "file_id ASC"]).await
}

pub(crate) async fn scan_tombstones_with_executor(
    executor: &mut dyn QueryExecutor,
    request: &TrackedScanRequest,
) -> Result<Vec<TrackedTombstoneMarker>, LixError> {
    scan_tombstones_with_limit_and_order(executor, request, None, &["entity_id ASC", "file_id ASC"])
        .await
}

async fn scan_rows_with_limit_and_order(
    executor: &mut dyn QueryExecutor,
    request: &TrackedScanRequest,
    limit: Option<usize>,
    order_by: &[&str],
) -> Result<Vec<TrackedRow>, LixError> {
    let access = load_live_row_access_with_executor(executor, &request.schema_key).await?;
    let selected_columns = selected_columns(&access, &request.required_columns, "tracked")?;
    let projection = selected_projection_sql(&selected_columns);
    let sql = build_partitioned_scan_sql(ScanSqlRequest {
        select_prefix: "SELECT entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, metadata, change_id, writer_key, created_at, updated_at",
        schema_key: &request.schema_key,
        version_id: &request.version_id,
        projection: &projection,
        fixed_predicates: &["untracked = false", "is_tombstone = 0"],
        constraints: &request.constraints,
        order_by,
        limit,
    })?;

    let result = match executor.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(error) if is_missing_relation_error(&error) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };

    result
        .rows
        .iter()
        .map(|row| decode_tracked_row(row, &selected_columns, &request.schema_key))
        .collect()
}

async fn scan_tombstones_with_limit_and_order(
    executor: &mut dyn QueryExecutor,
    request: &TrackedScanRequest,
    limit: Option<usize>,
    order_by: &[&str],
) -> Result<Vec<TrackedTombstoneMarker>, LixError> {
    let sql = build_partitioned_scan_sql(ScanSqlRequest {
        select_prefix: "SELECT entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, metadata, change_id, writer_key, created_at, updated_at",
        schema_key: &request.schema_key,
        version_id: &request.version_id,
        projection: "",
        fixed_predicates: &["untracked = false", "is_tombstone = 1"],
        constraints: &request.constraints,
        order_by,
        limit,
    })?;

    let result = match executor.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(error) if is_missing_relation_error(&error) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };

    result
        .rows
        .iter()
        .map(|row| decode_tracked_tombstone(row, &request.schema_key))
        .collect()
}

fn decode_tracked_row(
    row: &[Value],
    selected_columns: &[&crate::live_state::storage::LiveColumnSpec],
    schema_key: &str,
) -> Result<TrackedRow, LixError> {
    let entity_id = required_text_cell(row, 0, schema_key, "entity_id", "tracked")?;
    let schema_key_value = required_text_cell(row, 1, schema_key, "schema_key", "tracked")?;
    let schema_version = required_text_cell(row, 2, schema_key, "schema_version", "tracked")?;
    let file_id = required_text_cell(row, 3, schema_key, "file_id", "tracked")?;
    let version_id = required_text_cell(row, 4, schema_key, "version_id", "tracked")?;
    let global = required_bool_cell(row, 5, schema_key, "global", "tracked")?;
    let plugin_key = required_text_cell(row, 6, schema_key, "plugin_key", "tracked")?;
    let metadata = row.get(7).and_then(text_from_value);
    let change_id = row.get(8).and_then(text_from_value);
    let writer_key = row.get(9).and_then(text_from_value);
    let created_at = required_text_cell(row, 10, schema_key, "created_at", "tracked")?;
    let updated_at = required_text_cell(row, 11, schema_key, "updated_at", "tracked")?;

    let mut values = std::collections::BTreeMap::new();
    for (offset, column) in selected_columns.iter().enumerate() {
        let raw_value = row.get(12 + offset).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "tracked row for schema '{}' is missing property '{}'",
                    schema_key, column.property_name
                ),
            )
        })?;
        let value = normalize_live_property_value(raw_value, column, schema_key)?;
        values.insert(column.property_name.clone(), value);
    }

    Ok(TrackedRow {
        entity_id,
        schema_key: schema_key_value,
        schema_version,
        file_id,
        version_id,
        global,
        plugin_key,
        metadata,
        change_id,
        writer_key,
        created_at,
        updated_at,
        values,
    })
}

fn normalize_live_property_value(
    value: &Value,
    column: &crate::live_state::storage::LiveColumnSpec,
    schema_key: &str,
) -> Result<Value, LixError> {
    match column.kind {
        crate::live_state::storage::LiveColumnKind::Boolean => match value {
            Value::Null => Ok(Value::Null),
            Value::Boolean(value) => Ok(Value::Boolean(*value)),
            Value::Integer(0) => Ok(Value::Boolean(false)),
            Value::Integer(1) => Ok(Value::Boolean(true)),
            other => Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "tracked row for schema '{}' expected boolean property '{}', got {other:?}",
                    schema_key, column.property_name
                ),
            )),
        },
        _ => Ok(value.clone()),
    }
}

fn decode_tracked_tombstone(
    row: &[Value],
    schema_key: &str,
) -> Result<TrackedTombstoneMarker, LixError> {
    Ok(TrackedTombstoneMarker {
        entity_id: required_text_cell(row, 0, schema_key, "entity_id", "tracked tombstone")?,
        schema_key: required_text_cell(row, 1, schema_key, "schema_key", "tracked tombstone")?,
        schema_version: Some(required_text_cell(
            row,
            2,
            schema_key,
            "schema_version",
            "tracked tombstone",
        )?),
        file_id: required_text_cell(row, 3, schema_key, "file_id", "tracked tombstone")?,
        version_id: required_text_cell(row, 4, schema_key, "version_id", "tracked tombstone")?,
        global: required_bool_cell(row, 5, schema_key, "global", "tracked tombstone")?,
        plugin_key: Some(required_text_cell(
            row,
            6,
            schema_key,
            "plugin_key",
            "tracked tombstone",
        )?),
        metadata: row.get(7).and_then(text_from_value),
        change_id: row.get(8).and_then(text_from_value),
        writer_key: row.get(9).and_then(text_from_value),
        created_at: row.get(10).and_then(text_from_value),
        updated_at: row.get(11).and_then(text_from_value),
    })
}
