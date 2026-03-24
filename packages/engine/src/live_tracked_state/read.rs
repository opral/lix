use crate::backend::QueryExecutor;
use crate::constraints::{ScanConstraint, ScanField, ScanOperator};
use crate::errors::classification::is_missing_relation_error;
use crate::schema::live_layout::{load_live_row_access_with_executor, tracked_live_table_name};
use crate::{LixBackend, LixError, Value};

use super::contracts::{
    BatchTrackedRowRequest, ExactTrackedRowRequest, TrackedRow, TrackedScanRequest,
};
use super::shared::{
    decode_tracked_row, escape_sql_string, exact_row_constraints, quote_ident,
    render_constraint_sql, selected_columns, selected_projection_sql,
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

pub(crate) async fn load_exact_rows_with_executor(
    executor: &mut dyn QueryExecutor,
    request: &BatchTrackedRowRequest,
) -> Result<Vec<TrackedRow>, LixError> {
    if request.entity_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut constraints = vec![ScanConstraint {
        field: ScanField::EntityId,
        operator: ScanOperator::In(
            request
                .entity_ids
                .iter()
                .cloned()
                .map(Value::Text)
                .collect(),
        ),
    }];
    if let Some(file_id) = &request.file_id {
        constraints.push(ScanConstraint {
            field: ScanField::FileId,
            operator: ScanOperator::Eq(Value::Text(file_id.clone())),
        });
    }

    scan_rows_with_limit_and_order(
        executor,
        &TrackedScanRequest {
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            constraints,
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

async fn scan_rows_with_limit_and_order(
    executor: &mut dyn QueryExecutor,
    request: &TrackedScanRequest,
    limit: Option<usize>,
    order_by: &[&str],
) -> Result<Vec<TrackedRow>, LixError> {
    let access = load_live_row_access_with_executor(executor, &request.schema_key).await?;
    let selected_columns = selected_columns(&access, &request.required_columns)?;
    let projection = selected_projection_sql(&selected_columns);

    let mut sql = format!(
        "SELECT entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, metadata, change_id, writer_key, created_at, updated_at{projection} \
         FROM {table_name} \
         WHERE schema_key = '{schema_key}' \
           AND version_id = '{version_id}' \
           AND untracked = false \
           AND is_tombstone = 0",
        projection = projection,
        table_name = quote_ident(&tracked_live_table_name(&request.schema_key)),
        schema_key = escape_sql_string(&request.schema_key),
        version_id = escape_sql_string(&request.version_id),
    );

    for constraint in &request.constraints {
        let clause = render_constraint_sql(constraint)?;
        if !clause.is_empty() {
            sql.push_str(" AND ");
            sql.push_str(&clause);
        }
    }

    if !order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(&order_by.join(", "));
    }
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }

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
