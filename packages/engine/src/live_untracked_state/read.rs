use crate::backend::QueryExecutor;
use crate::constraints::{ScanConstraint, ScanField, ScanOperator};
use crate::errors::classification::is_missing_relation_error;
use crate::schema::live_layout::{load_live_row_access_with_executor, tracked_live_table_name};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    version_ref_file_id, version_ref_schema_key, version_ref_storage_version_id,
};
use crate::{LixBackend, LixError, Value};

use super::contracts::{
    ActiveVersionRow, BatchUntrackedRowRequest, ExactUntrackedRowRequest, UntrackedRow,
    UntrackedScanRequest, VersionRefRow,
};
use super::shared::{
    decode_untracked_row, escape_sql_string, exact_row_constraints, quote_ident,
    render_constraint_sql, selected_columns, selected_projection_sql,
};

pub async fn load_exact_row_with_backend(
    backend: &dyn LixBackend,
    request: &ExactUntrackedRowRequest,
) -> Result<Option<UntrackedRow>, LixError> {
    let mut executor = backend;
    load_exact_row_with_executor(&mut executor, request).await
}

pub async fn load_exact_rows_with_backend(
    backend: &dyn LixBackend,
    request: &BatchUntrackedRowRequest,
) -> Result<Vec<UntrackedRow>, LixError> {
    let mut executor = backend;
    load_exact_rows_with_executor(&mut executor, request).await
}

pub async fn scan_rows_with_backend(
    backend: &dyn LixBackend,
    request: &UntrackedScanRequest,
) -> Result<Vec<UntrackedRow>, LixError> {
    let mut executor = backend;
    scan_rows_with_executor(&mut executor, request).await
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

pub(crate) async fn load_exact_row_with_executor(
    executor: &mut dyn QueryExecutor,
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

pub(crate) async fn load_exact_rows_with_executor(
    executor: &mut dyn QueryExecutor,
    request: &BatchUntrackedRowRequest,
) -> Result<Vec<UntrackedRow>, LixError> {
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
        &UntrackedScanRequest {
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
    request: &UntrackedScanRequest,
) -> Result<Vec<UntrackedRow>, LixError> {
    scan_rows_with_limit_and_order(executor, request, None, &["entity_id ASC", "file_id ASC"]).await
}

pub(crate) async fn load_active_version_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Option<ActiveVersionRow>, LixError> {
    let access = load_live_row_access_with_executor(executor, active_version_schema_key()).await?;
    let selected_columns = selected_columns(&access, &["version_id".to_string()])?;
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
        table_name = quote_ident(&tracked_live_table_name(active_version_schema_key())),
        schema_key = escape_sql_string(active_version_schema_key()),
        file_id = escape_sql_string(active_version_file_id()),
        version_id = escape_sql_string(active_version_storage_version_id()),
        payload_column = quote_ident(&version_column.column_name),
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

async fn scan_rows_with_limit_and_order(
    executor: &mut dyn QueryExecutor,
    request: &UntrackedScanRequest,
    limit: Option<usize>,
    order_by: &[&str],
) -> Result<Vec<UntrackedRow>, LixError> {
    let access = load_live_row_access_with_executor(executor, &request.schema_key).await?;
    let selected_columns = selected_columns(&access, &request.required_columns)?;
    let projection = selected_projection_sql(&selected_columns);

    let mut sql = format!(
        "SELECT entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, metadata, writer_key, created_at, updated_at{projection} \
         FROM {table_name} \
         WHERE schema_key = '{schema_key}' \
           AND version_id = '{version_id}' \
           AND untracked = true",
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
        .map(|row| decode_untracked_row(row, &selected_columns, &request.schema_key))
        .collect()
}
