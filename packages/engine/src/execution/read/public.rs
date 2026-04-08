use crate::backend::TransactionBackendAdapter;
use crate::common::errors::classification::sanitize_lowered_public_sql_error_description;
use crate::contracts::artifacts::{
    PreparedPublicReadArtifact, PreparedPublicReadExecutionArtifact, PublicReadResultColumn,
    PublicReadResultColumns,
};
use crate::contracts::surface::SurfaceReadFreshness;
use crate::contracts::traits::{LiveStateQueryBackend, PendingPublicReadTransaction, PendingView};
use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, Value};
use async_trait::async_trait;

use super::{
    direct::execute_direct_public_read_with_backend, execute_read_time_projection_read,
    ReadExecutionBindings,
};

#[async_trait(?Send)]
pub(crate) trait PendingPublicReadExecutionBackend {
    async fn execute_prepared_public_read_with_pending_view(
        &self,
        bindings: &dyn ReadExecutionBindings,
        pending_view: Option<&dyn PendingView>,
        public_read: &PreparedPublicReadArtifact,
    ) -> Result<QueryResult, LixError>;
}

pub(crate) async fn execute_prepared_public_read_artifact_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    bindings: &dyn ReadExecutionBindings,
    artifact: &PreparedPublicReadArtifact,
) -> Result<QueryResult, LixError> {
    ensure_surface_read_freshness_in_transaction(transaction, artifact).await?;
    let backend = TransactionBackendAdapter::new(transaction);
    execute_prepared_public_read_artifact_without_freshness_check_with_backend(
        &backend, bindings, artifact,
    )
    .await
}

pub(crate) async fn bootstrap_public_surface_registry_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    pending_view: Option<&dyn PendingView>,
) -> Result<crate::contracts::surface::SurfaceRegistry, LixError> {
    let backend = TransactionBackendAdapter::new(transaction);
    crate::live_state::build_surface_registry(&backend, pending_view).await
}

pub(crate) async fn execute_prepared_public_read_artifact_with_backend(
    backend: &dyn LixBackend,
    bindings: &dyn ReadExecutionBindings,
    artifact: &PreparedPublicReadArtifact,
) -> Result<QueryResult, LixError> {
    ensure_surface_read_freshness(backend, artifact).await?;
    execute_prepared_public_read_artifact_without_freshness_check_with_backend(
        backend, bindings, artifact,
    )
    .await
}

pub(crate) async fn execute_prepared_public_read_artifact_without_freshness_check_with_backend(
    backend: &dyn LixBackend,
    bindings: &dyn ReadExecutionBindings,
    artifact: &PreparedPublicReadArtifact,
) -> Result<QueryResult, LixError> {
    let result = match &artifact.execution {
        PreparedPublicReadExecutionArtifact::ReadTimeProjection(read) => {
            execute_read_time_projection_read(backend, bindings, read).await?
        }
        PreparedPublicReadExecutionArtifact::LoweredSql(batch) => {
            execute_prepared_batch_with_backend(backend, batch)
                .await
                .map_err(|error| {
                    translate_lowered_public_read_error(error, &artifact.surface_bindings)
                })?
        }
        PreparedPublicReadExecutionArtifact::Direct(plan) => {
            execute_direct_public_read_with_backend(backend, plan).await?
        }
    };
    Ok(finalize_prepared_public_read_result(result, artifact))
}

#[async_trait(?Send)]
impl PendingPublicReadExecutionBackend for dyn LixBackend + '_ {
    async fn execute_prepared_public_read_with_pending_view(
        &self,
        bindings: &dyn ReadExecutionBindings,
        pending_view: Option<&dyn PendingView>,
        public_read: &PreparedPublicReadArtifact,
    ) -> Result<QueryResult, LixError> {
        match public_read.contract.execution_mode() {
            crate::contracts::artifacts::PublicReadExecutionMode::PendingView => {
                crate::live_state::execute_prepared_public_read(self, pending_view, public_read)
                    .await
            }
            crate::contracts::artifacts::PublicReadExecutionMode::Committed(_) => {
                execute_prepared_public_read_artifact_with_backend(self, bindings, public_read)
                    .await
            }
        }
    }
}

#[async_trait(?Send)]
impl PendingPublicReadTransaction for dyn LixBackendTransaction + '_ {
    async fn require_live_state_ready(&mut self) -> Result<(), LixError> {
        crate::live_state::require_ready_in_transaction(self).await
    }
}

fn finalize_prepared_public_read_result(
    result: QueryResult,
    artifact: &PreparedPublicReadArtifact,
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

async fn ensure_surface_read_freshness(
    backend: &dyn LixBackend,
    artifact: &PreparedPublicReadArtifact,
) -> Result<(), LixError> {
    if artifact.freshness_contract == SurfaceReadFreshness::AllowsStaleProjection {
        return Ok(());
    }

    let status = backend.load_live_state_projection_status().await?;
    if matches!(
        status.mode,
        crate::contracts::artifacts::LiveStateMode::Ready
            | crate::contracts::artifacts::LiveStateMode::Bootstrapping
    ) {
        return Ok(());
    }

    Err(public_read_projection_stale_error(
        &artifact.surface_bindings,
        &status,
    ))
}

async fn ensure_surface_read_freshness_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    artifact: &PreparedPublicReadArtifact,
) -> Result<(), LixError> {
    if artifact.freshness_contract == SurfaceReadFreshness::AllowsStaleProjection {
        return Ok(());
    }

    if transaction.require_live_state_ready().await.is_ok() {
        return Ok(());
    }

    let backend = TransactionBackendAdapter::new(transaction);
    let status = (&backend as &dyn crate::LixBackend)
        .load_live_state_projection_status()
        .await?;
    if status.mode == crate::contracts::artifacts::LiveStateMode::Bootstrapping {
        return Ok(());
    }
    Err(public_read_projection_stale_error(
        &artifact.surface_bindings,
        &status,
    ))
}

fn public_read_projection_stale_error(
    surface_names: &[String],
    status: &crate::contracts::artifacts::LiveStateProjectionStatus,
) -> LixError {
    let surfaces = if surface_names.is_empty() {
        "this public read".to_string()
    } else {
        format!("surface(s) {}", surface_names.join(", "))
    };
    let applied = format_optional_replay_cursor(status.applied_cursor.as_ref());
    let latest = format_optional_replay_cursor(status.latest_cursor.as_ref());
    let applied_frontier =
        format_optional_committed_frontier(status.applied_committed_frontier.as_ref());
    let current_frontier = format_committed_frontier(&status.current_committed_frontier);
    LixError::new(
        crate::common::errors::ErrorCode::LiveStateNotReady.as_str(),
        format!(
            "Public read for {surfaces} requires fresh live-state projections, but live_state is {:?}. Applied committed frontier: {applied_frontier}. Current committed frontier: {current_frontier}. Applied replay cursor: {applied}. Latest replay cursor: {latest}. Canonical history/change reads may proceed while stale, but current-state projection reads must wait for replay or rebuild.",
            status.mode
        ),
    )
}

fn format_optional_replay_cursor(cursor: Option<&crate::ReplayCursor>) -> String {
    cursor
        .map(|cursor| format!("{}@{}", cursor.change_id, cursor.created_at))
        .unwrap_or_else(|| "(none)".to_string())
}

fn format_optional_committed_frontier(
    frontier: Option<&crate::CommittedVersionFrontier>,
) -> String {
    frontier
        .map(format_committed_frontier)
        .unwrap_or_else(|| "(none)".to_string())
}

fn format_committed_frontier(frontier: &crate::CommittedVersionFrontier) -> String {
    frontier.describe()
}

async fn execute_prepared_batch_with_backend(
    backend: &dyn LixBackend,
    batch: &crate::contracts::artifacts::PreparedBatch,
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
    let description =
        sanitize_lowered_public_sql_error_description(&error.description, public_surfaces);
    LixError::new(&error.code, description)
}
