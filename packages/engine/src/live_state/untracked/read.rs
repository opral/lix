use crate::common::is_missing_relation_error;
#[cfg(test)]
use crate::live_state::batch_row_constraints;
use crate::live_state::exact_row_constraints;
use crate::live_state::schema_access::live_storage_relation_exists_with_executor;
use crate::live_state::storage::{
    load_live_row_access_with_executor, required_bool_cell, required_text_cell, selected_columns,
    selected_projection_sql, text_from_value, ScanSqlRequest,
};
use crate::live_state::store::{LiveStateBackendRef, LiveStateExecutorRef};
use crate::{LixError, Value};

#[cfg(test)]
use super::contracts::BatchUntrackedRowRequest;
use super::contracts::{ExactUntrackedRowRequest, UntrackedRow, UntrackedScanRequest};

pub async fn load_exact_row_with_backend(
    backend: LiveStateBackendRef<'_>,
    request: &ExactUntrackedRowRequest,
) -> Result<Option<UntrackedRow>, LixError> {
    let mut executor = backend;
    load_exact_row_with_executor(&mut executor, request).await
}

#[cfg(test)]
pub async fn load_exact_rows_with_backend(
    backend: LiveStateBackendRef<'_>,
    request: &BatchUntrackedRowRequest,
) -> Result<Vec<UntrackedRow>, LixError> {
    let mut executor = backend;
    load_exact_rows_with_executor(&mut executor, request).await
}

pub async fn scan_rows_with_backend(
    backend: LiveStateBackendRef<'_>,
    request: &UntrackedScanRequest,
) -> Result<Vec<UntrackedRow>, LixError> {
    let mut executor = backend;
    scan_rows_with_executor(&mut executor, request).await
}

pub(crate) async fn scan_rows_with_backend_limit(
    backend: LiveStateBackendRef<'_>,
    request: &UntrackedScanRequest,
    limit: usize,
) -> Result<Vec<UntrackedRow>, LixError> {
    let mut executor = backend;
    scan_rows_with_executor_limit(&mut executor, request, limit).await
}

pub(crate) async fn scan_rows_with_executor_limit(
    executor: LiveStateExecutorRef<'_>,
    request: &UntrackedScanRequest,
    limit: usize,
) -> Result<Vec<UntrackedRow>, LixError> {
    scan_rows_with_limit_and_order(
        executor,
        request,
        Some(limit),
        &["entity_id ASC", "file_id ASC"],
    )
    .await
}

pub(crate) async fn load_exact_row_with_executor(
    executor: LiveStateExecutorRef<'_>,
    request: &ExactUntrackedRowRequest,
) -> Result<Option<UntrackedRow>, LixError> {
    let scan_request = UntrackedScanRequest {
        schema_key: request.schema_key.clone(),
        version_id: request.version_id.clone(),
        constraints: exact_row_constraints(request),
        required_columns: Vec::new(),
    };
    let rows =
        scan_rows_with_limit_and_order(executor, &scan_request, Some(2), &["updated_at DESC"])
            .await?;
    if rows.len() > 1 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!(
                "expected at most one untracked row for schema '{}' entity '{}' version '{}'",
                request.schema_key, request.entity_id, request.version_id
            ),
        ));
    }
    Ok(rows.into_iter().next())
}

#[cfg(test)]
pub(crate) async fn load_exact_rows_with_executor(
    executor: LiveStateExecutorRef<'_>,
    request: &BatchUntrackedRowRequest,
) -> Result<Vec<UntrackedRow>, LixError> {
    if request.entity_ids.is_empty() {
        return Ok(Vec::new());
    }

    scan_rows_with_limit_and_order(
        executor,
        &UntrackedScanRequest {
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
    executor: LiveStateExecutorRef<'_>,
    request: &UntrackedScanRequest,
) -> Result<Vec<UntrackedRow>, LixError> {
    scan_rows_with_limit_and_order(executor, request, None, &["entity_id ASC", "file_id ASC"]).await
}

pub(crate) async fn scan_rows_with_executor_and_access(
    executor: LiveStateExecutorRef<'_>,
    request: &UntrackedScanRequest,
    access: &crate::live_state::storage::LiveRowAccess,
    limit: Option<usize>,
) -> Result<Vec<UntrackedRow>, LixError> {
    scan_rows_with_limit_order_and_access(
        executor,
        request,
        access,
        limit,
        &["entity_id ASC", "file_id ASC"],
    )
    .await
}

async fn scan_rows_with_limit_and_order(
    executor: LiveStateExecutorRef<'_>,
    request: &UntrackedScanRequest,
    limit: Option<usize>,
    order_by: &[&str],
) -> Result<Vec<UntrackedRow>, LixError> {
    if !live_storage_relation_exists_with_executor(executor, &request.schema_key).await? {
        return Ok(Vec::new());
    }

    let access = load_live_row_access_with_executor(executor, &request.schema_key).await?;
    scan_rows_with_limit_order_and_access(executor, request, &access, limit, order_by).await
}

async fn scan_rows_with_limit_order_and_access(
    executor: LiveStateExecutorRef<'_>,
    request: &UntrackedScanRequest,
    access: &crate::live_state::storage::LiveRowAccess,
    limit: Option<usize>,
    order_by: &[&str],
) -> Result<Vec<UntrackedRow>, LixError> {
    let selected_columns = selected_columns(&access, &request.required_columns, "untracked")?;
    let projection = selected_projection_sql(&selected_columns);
    let result = match crate::live_state::storage::scan_live_partition_with_executor(
        executor,
        ScanSqlRequest {
        select_prefix: "SELECT entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, metadata, change_id, created_at, updated_at",
        schema_key: &request.schema_key,
        version_id: &request.version_id,
        projection: &projection,
        fixed_predicates: &["untracked = true"],
        constraints: &request.constraints,
        order_by,
        limit,
        },
    )
    .await
    {
        Ok(result) => result,
        Err(error) if is_missing_relation_error(&error) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };

    result
        .rows
        .iter()
        .map(|row| decode_untracked_row(row, &selected_columns, &request.schema_key))
        .collect()
}

pub(crate) fn decode_untracked_row(
    row: &[Value],
    selected_columns: &[&crate::live_state::storage::LiveColumnSpec],
    schema_key: &str,
) -> Result<UntrackedRow, LixError> {
    let entity_id = required_text_cell(row, 0, schema_key, "entity_id", "untracked")?;
    let schema_key_value = required_text_cell(row, 1, schema_key, "schema_key", "untracked")?;
    let schema_version = required_text_cell(row, 2, schema_key, "schema_version", "untracked")?;
    let file_id = row.get(3).and_then(text_from_value);
    let version_id = required_text_cell(row, 4, schema_key, "version_id", "untracked")?;
    let global = required_bool_cell(row, 5, schema_key, "global", "untracked")?;
    let plugin_key = row.get(6).and_then(text_from_value);
    let metadata = row.get(7).and_then(text_from_value);
    let change_id = required_text_cell(row, 8, schema_key, "change_id", "untracked")?;
    let created_at = required_text_cell(row, 9, schema_key, "created_at", "untracked")?;
    let updated_at = required_text_cell(row, 10, schema_key, "updated_at", "untracked")?;

    let mut values = std::collections::BTreeMap::new();
    for (offset, column) in selected_columns.iter().enumerate() {
        let raw_value = row.get(11 + offset).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "untracked row for schema '{}' is missing property '{}'",
                    schema_key, column.property_name
                ),
            )
        })?;
        let value = normalize_live_property_value(raw_value, column, schema_key)?;
        values.insert(column.property_name.clone(), value);
    }

    Ok(UntrackedRow {
        entity_id,
        schema_key: schema_key_value,
        schema_version,
        file_id,
        version_id,
        global,
        plugin_key,
        metadata,
        change_id,
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
                    "untracked row for schema '{}' expected boolean property '{}', got {other:?}",
                    schema_key, column.property_name
                ),
            )),
        },
        _ => Ok(value.clone()),
    }
}
