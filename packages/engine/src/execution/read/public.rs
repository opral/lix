use crate::sql::{
    sanitize_lowered_public_sql_error_description, PreparedPublicRead,
    PreparedPublicReadPlanArtifact, PublicReadResultColumn, PublicReadResultColumns,
};
use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, Value};

use super::{
    execute_read_time_projection_read, history::execute_history_read_plan_with_backend,
    ReadExecutionHost,
};

pub(crate) async fn execute_prepared_public_read_artifact_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    host: &dyn ReadExecutionHost,
    artifact: &PreparedPublicRead,
) -> Result<QueryResult, LixError> {
    host.ensure_projection_freshness_in_transaction(
        transaction,
        artifact.freshness_contract,
        &artifact.resolved_relations,
    )
    .await?;
    let backend = crate::backend::transaction_backend_view(transaction);
    execute_prepared_public_read_artifact_without_freshness_check_with_backend(
        &backend, host, artifact,
    )
    .await
}

pub(crate) async fn execute_prepared_public_read_artifact_with_backend(
    backend: &dyn LixBackend,
    host: &dyn ReadExecutionHost,
    artifact: &PreparedPublicRead,
) -> Result<QueryResult, LixError> {
    host.ensure_projection_freshness_with_backend(
        backend,
        artifact.freshness_contract,
        &artifact.resolved_relations,
    )
    .await?;
    execute_prepared_public_read_artifact_without_freshness_check_with_backend(
        backend, host, artifact,
    )
    .await
}

pub(crate) async fn execute_prepared_public_read_artifact_without_freshness_check_with_backend(
    backend: &dyn LixBackend,
    host: &dyn ReadExecutionHost,
    artifact: &PreparedPublicRead,
) -> Result<QueryResult, LixError> {
    let result = match &artifact.execution {
        PreparedPublicReadPlanArtifact::ReadTimeProjection(artifact) => {
            execute_read_time_projection_read(backend, host, &artifact.read).await?
        }
        PreparedPublicReadPlanArtifact::PreparedBatch(execution_artifact) => {
            execute_prepared_batch_with_backend(backend, &execution_artifact.prepared_batch)
                .await
                .map_err(|error| {
                    translate_lowered_public_read_error(error, &artifact.resolved_relations)
                })?
        }
        PreparedPublicReadPlanArtifact::HistoryRead(artifact) => {
            execute_history_read_plan_with_backend(backend, &artifact.plan).await?
        }
        PreparedPublicReadPlanArtifact::Sql2(artifact) => {
            if let Some(shared_backend) = host.shared_backend() {
                crate::sql2::execute_read_with_shared_backend(shared_backend, &artifact.artifact)
                    .await?
            } else {
                crate::sql2::execute_read_with_backend(backend, &artifact.artifact).await?
            }
        }
    };
    Ok(finalize_prepared_public_read_result(result, artifact))
}

fn finalize_prepared_public_read_result(
    result: QueryResult,
    artifact: &PreparedPublicRead,
) -> QueryResult {
    let result =
        decode_public_read_result_columns(result, artifact.contract.result_columns.as_ref());
    apply_public_output_columns(result, artifact.public_output_columns.as_deref())
}

fn decode_public_read_result_columns(
    mut result: QueryResult,
    result_columns: Option<&PublicReadResultColumns>,
) -> QueryResult {
    let Some(result_columns) = result_columns else {
        return result;
    };
    let column_plan = match result_columns {
        PublicReadResultColumns::Static(columns) => columns
            .iter()
            .copied()
            .chain(std::iter::repeat(PublicReadResultColumn::Untyped))
            .take(result.columns.len())
            .collect::<Vec<_>>(),
        PublicReadResultColumns::ByColumnName(columns_by_name) => result
            .columns
            .iter()
            .map(|column| {
                columns_by_name
                    .iter()
                    .find_map(|(candidate, kind)| {
                        candidate.eq_ignore_ascii_case(column).then_some(*kind)
                    })
                    .unwrap_or(PublicReadResultColumn::Untyped)
            })
            .collect::<Vec<_>>(),
    };

    if !column_plan
        .iter()
        .any(|kind| *kind == PublicReadResultColumn::Boolean)
    {
        return result;
    }

    for row in &mut result.rows {
        for (value, kind) in row.iter_mut().zip(column_plan.iter().copied()) {
            if kind == PublicReadResultColumn::Boolean {
                if let Some(decoded) = decode_boolean_value(value) {
                    *value = decoded;
                }
            }
        }
    }

    result
}

fn decode_boolean_value(value: &Value) -> Option<Value> {
    match value {
        Value::Null => Some(Value::Null),
        Value::Boolean(value) => Some(Value::Boolean(*value)),
        Value::Integer(0) => Some(Value::Boolean(false)),
        Value::Integer(1) => Some(Value::Boolean(true)),
        Value::Text(text) => match text.trim().to_ascii_lowercase().as_str() {
            "0" | "false" => Some(Value::Boolean(false)),
            "1" | "true" => Some(Value::Boolean(true)),
            _ => None,
        },
        Value::Real(_) | Value::Json(_) | Value::Blob(_) => None,
        Value::Integer(_) => None,
    }
}

fn apply_public_output_columns(
    mut result: QueryResult,
    public_output_columns: Option<&[String]>,
) -> QueryResult {
    let Some(public_output_columns) = public_output_columns else {
        return result;
    };
    if !public_output_columns.is_empty() && public_output_columns.len() == result.columns.len() {
        result.columns = public_output_columns.to_vec();
    }
    result
}

async fn execute_prepared_batch_with_backend(
    backend: &dyn LixBackend,
    batch: &crate::backend::PreparedBatch,
) -> Result<QueryResult, LixError> {
    let mut result = QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
    };
    for statement in &batch.steps {
        result = backend.execute(&statement.sql, &statement.params).await?;
    }
    Ok(result)
}

fn translate_lowered_public_read_error(error: LixError, public_surfaces: &[String]) -> LixError {
    // Read execution only sanitizes the already-SQL-shaped message for the
    // current public surfaces; the shaping rules remain owned by `sql/*`.
    let description =
        sanitize_lowered_public_sql_error_description(&error.description, public_surfaces);
    LixError::new(&error.code, description)
}
