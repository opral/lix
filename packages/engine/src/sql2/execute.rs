use datafusion::arrow::datatypes::Field;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;

use crate::{LixError, LixNotice, SqlQueryResult, Value};

use super::change_provider::register_lix_change_provider;
use super::commit_derived_provider::register_commit_derived_providers;
use super::directory_history_provider::register_lix_directory_history_provider;
use super::directory_provider::{
    register_lix_directory_providers, register_lix_directory_write_providers,
};
use super::entity_provider::{register_entity_providers, register_entity_write_providers};
use super::file_history_provider::register_lix_file_history_provider;
use super::file_provider::{register_lix_file_providers, register_lix_file_write_providers};
use super::history_provider::register_history_providers;
use super::lix_state_provider::{register_lix_state_providers, register_lix_state_write_providers};
use super::result_metadata::field_is_json;
use super::udfs::register_sql2_functions;
use super::version_provider::{register_lix_version_provider, register_lix_version_write_provider};
use super::{SqlExecutionContext, SqlStatementKind, SqlWriteContext, SqlWriteExecutionContext};

#[allow(dead_code)]
pub(crate) struct SqlLogicalPlan {
    session: SessionContext,
    plan: LogicalPlan,
    kind: SqlStatementKind,
    notices: Vec<LixNotice>,
}

impl SqlLogicalPlan {
    #[allow(dead_code)]
    pub(crate) fn kind(&self) -> SqlStatementKind {
        self.kind
    }

    #[allow(dead_code)]
    pub(crate) fn is_write(&self) -> bool {
        self.kind == SqlStatementKind::Write
    }
}

/// Minimal top-level sql2 entrypoint.
///
/// The final implementation will build the DataFusion session from the
/// execution context and source rows from `live_state()`.
///
/// `catalog()` is intentionally omitted from the MVP boundary for now.
#[allow(dead_code)]
pub(crate) async fn execute_sql(
    ctx: &dyn SqlExecutionContext,
    sql: &str,
    params: &[Value],
) -> Result<SqlQueryResult, LixError> {
    let plan = create_logical_plan(ctx, sql).await?;
    execute_logical_plan(plan, params).await
}

pub(crate) async fn create_logical_plan(
    ctx: &dyn SqlExecutionContext,
    sql: &str,
) -> Result<SqlLogicalPlan, LixError> {
    super::validate_supported_statement_ast(sql)?;
    let session = build_read_session(ctx).await?;
    let plan = session
        .state()
        .create_logical_plan(sql)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    validate_supported_logical_plan(&plan)?;
    let kind = classify_logical_plan(&plan);
    let notices = history_filter_notices(&plan);

    Ok(SqlLogicalPlan {
        session,
        plan,
        kind,
        notices,
    })
}

#[allow(dead_code)]
pub(crate) async fn create_write_logical_plan(
    ctx: &mut dyn SqlWriteExecutionContext,
    sql: &str,
) -> Result<SqlLogicalPlan, LixError> {
    super::validate_supported_statement_ast(sql)?;
    let session = build_write_session(ctx).await?;
    let plan = session
        .state()
        .create_logical_plan(sql)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    validate_supported_logical_plan(&plan)?;
    let kind = classify_logical_plan(&plan);

    Ok(SqlLogicalPlan {
        session,
        plan,
        kind,
        notices: Vec::new(),
    })
}

pub(crate) async fn execute_logical_plan(
    plan: SqlLogicalPlan,
    params: &[Value],
) -> Result<SqlQueryResult, LixError> {
    let SqlLogicalPlan {
        session,
        plan,
        kind: _,
        notices,
    } = plan;
    validate_parameter_count(&plan, params.len())?;

    let mut dataframe = session
        .execute_logical_plan(plan)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    if !params.is_empty() {
        dataframe = dataframe
            .with_param_values(
                params
                    .iter()
                    .map(scalar_value_from_lix_value)
                    .collect::<Vec<_>>(),
            )
            .map_err(datafusion_error_to_lix_error)?;
    }

    let result_fields = dataframe
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let batches = dataframe
        .collect()
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let mut result = query_result_from_batches(&result_fields, &batches)?;
    result.notices = notices;
    Ok(result)
}

fn validate_parameter_count(plan: &LogicalPlan, param_count: usize) -> Result<(), LixError> {
    let parameter_names = plan
        .get_parameter_names()
        .map_err(datafusion_error_to_lix_error)?;
    let expected_count = expected_positional_parameter_count(&parameter_names)?;
    if param_count == expected_count {
        return Ok(());
    }

    Err(LixError::new(
        LixError::CODE_INVALID_PARAM,
        format!(
            "SQL expected {expected_count} parameter(s), but {param_count} parameter(s) were provided"
        ),
    )
    .with_details(json!({
        "operation": "execute",
        "expected_param_count": expected_count,
        "provided_param_count": param_count,
        "placeholders": sorted_parameter_names(&parameter_names),
    })))
}

fn expected_positional_parameter_count(
    parameter_names: &HashSet<String>,
) -> Result<usize, LixError> {
    let mut max_index = 0usize;
    for name in parameter_names {
        let Some(index) = name
            .strip_prefix('$')
            .and_then(|raw| raw.parse::<usize>().ok())
        else {
            return Err(LixError::new(
                LixError::CODE_PARSE_ERROR,
                format!("unsupported SQL parameter placeholder '{name}'"),
            )
            .with_hint("Use numbered placeholders like $1, $2, ...")
            .with_details(json!({
                "operation": "execute",
                "placeholder": name,
            })));
        };
        if index == 0 {
            return Err(LixError::new(
                LixError::CODE_PARSE_ERROR,
                "SQL parameter placeholders are 1-indexed",
            )
            .with_hint("Use numbered placeholders like $1, $2, ...")
            .with_details(json!({
                "operation": "execute",
                "placeholder": name,
            })));
        }
        max_index = max_index.max(index);
    }
    Ok(max_index)
}

fn sorted_parameter_names(parameter_names: &HashSet<String>) -> Vec<String> {
    let mut names = parameter_names.iter().cloned().collect::<Vec<_>>();
    names.sort();
    names
}

async fn build_read_session(ctx: &dyn SqlExecutionContext) -> Result<SessionContext, LixError> {
    let session = new_lix_session_context();
    let version_ref = ctx.version_ref();
    let active_version_commit_id = version_ref
        .load_head(ctx.active_version_id())
        .await?
        .map(|head| head.commit_id);
    register_sql2_functions(&session, ctx.functions(), active_version_commit_id);
    register_lix_state_providers(
        &session,
        ctx.active_version_id(),
        ctx.live_state(),
        Arc::clone(&version_ref),
    )
    .await?;
    register_lix_version_provider(&session, ctx.live_state(), Arc::clone(&version_ref)).await?;
    let changelog_query_source = ctx.changelog_query_source();
    register_lix_change_provider(&session, changelog_query_source.clone()).await?;
    let commit_graph = ctx.commit_graph();
    register_commit_derived_providers(&session, commit_graph, Arc::clone(&version_ref)).await?;
    let state_history_commit_graph = ctx.commit_graph();
    register_history_providers(
        &session,
        state_history_commit_graph,
        changelog_query_source.clone(),
    )
    .await?;
    let file_history_commit_graph = ctx.commit_graph();
    register_lix_file_history_provider(
        &session,
        file_history_commit_graph,
        changelog_query_source.clone(),
        ctx.blob_reader(),
    )
    .await?;
    let directory_history_commit_graph = ctx.commit_graph();
    register_lix_directory_history_provider(
        &session,
        directory_history_commit_graph,
        changelog_query_source.clone(),
    )
    .await?;
    let entity_commit_graph = Arc::new(tokio::sync::Mutex::new(ctx.commit_graph()));
    register_lix_directory_providers(
        &session,
        ctx.active_version_id(),
        ctx.live_state(),
        Arc::clone(&version_ref),
        ctx.functions(),
    )
    .await?;
    register_lix_file_providers(
        &session,
        ctx.active_version_id(),
        ctx.live_state(),
        Arc::clone(&version_ref),
        ctx.blob_reader(),
        ctx.functions(),
    )
    .await?;
    register_entity_providers(
        &session,
        ctx.active_version_id(),
        ctx.live_state(),
        Arc::clone(&version_ref),
        entity_commit_graph,
        changelog_query_source,
        &ctx.list_visible_schemas()?,
    )
    .await?;

    Ok(session)
}

async fn build_write_session(
    ctx: &mut dyn SqlWriteExecutionContext,
) -> Result<SessionContext, LixError> {
    let session = new_lix_session_context();
    let write_ctx = SqlWriteContext::new(ctx);
    let active_version_commit_id = write_ctx
        .load_version_head(&write_ctx.active_version_id())
        .await?;
    register_sql2_functions(&session, write_ctx.functions(), active_version_commit_id);

    register_lix_state_write_providers(&session, write_ctx.clone()).await?;
    register_lix_version_write_provider(&session, write_ctx.clone()).await?;

    register_lix_directory_write_providers(&session, write_ctx.clone()).await?;
    register_lix_file_write_providers(&session, write_ctx.clone()).await?;
    register_entity_write_providers(
        &session,
        write_ctx.clone(),
        &write_ctx.list_visible_schemas()?,
    )
    .await?;

    Ok(session)
}

fn new_lix_session_context() -> SessionContext {
    SessionContext::new_with_config(SessionConfig::new().with_information_schema(true))
}

fn classify_logical_plan(plan: &LogicalPlan) -> SqlStatementKind {
    match plan {
        LogicalPlan::Dml(_) => SqlStatementKind::Write,
        LogicalPlan::Ddl(_) | LogicalPlan::Statement(_) | LogicalPlan::Copy(_) => {
            SqlStatementKind::Other
        }
        _ => SqlStatementKind::Read,
    }
}

fn validate_supported_logical_plan(plan: &LogicalPlan) -> Result<(), LixError> {
    match plan {
        LogicalPlan::Ddl(_) => {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "DDL statements are not supported by Lix SQL",
            )
            .with_hint(
                "Use Lix entity surfaces such as lix_registered_schema, lix_version, lix_file, and lix_key_value instead of CREATE/DROP statements.",
            ));
        }
        LogicalPlan::Statement(_) => {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "SQL utility statements are not supported by Lix SQL",
            ));
        }
        LogicalPlan::Copy(_) => {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "COPY statements are not supported by Lix SQL",
            ));
        }
        LogicalPlan::RecursiveQuery(_) => {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "recursive CTEs are not supported by Lix SQL",
            )
            .with_hint(
                "Use explicit commit graph surfaces such as lix_commit, lix_commit_edge, and lix_state_history instead of WITH RECURSIVE.",
            ));
        }
        _ => {}
    }

    for input in plan.inputs() {
        validate_supported_logical_plan(input)?;
    }

    Ok(())
}

fn scalar_value_from_lix_value(value: &Value) -> ScalarValue {
    match value {
        Value::Null => ScalarValue::Null,
        Value::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        Value::Integer(value) => ScalarValue::Int64(Some(*value)),
        Value::Real(value) => ScalarValue::Float64(Some(*value)),
        Value::Text(value) => ScalarValue::Utf8(Some(value.clone())),
        Value::Json(value) => ScalarValue::Utf8(Some(value.to_string())),
        Value::Blob(value) => ScalarValue::Binary(Some(value.clone())),
    }
}

fn datafusion_error_to_lix_error(error: datafusion::error::DataFusionError) -> LixError {
    super::error::datafusion_error_to_lix_error(error)
}

fn query_result_from_batches(
    result_fields: &[Field],
    batches: &[RecordBatch],
) -> Result<SqlQueryResult, LixError> {
    let result_columns = result_fields
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    let mut rows = Vec::<Vec<Value>>::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::<Value>::with_capacity(batch.num_columns());
            for (column_index, array) in batch.columns().iter().enumerate() {
                let scalar = ScalarValue::try_from_array(array.as_ref(), row_index)
                    .map_err(datafusion_error_to_lix_error)?;
                let field = result_fields.get(column_index);
                row.push(scalar_value_to_lix_value(&scalar, field)?);
            }
            rows.push(row);
        }
    }

    Ok(SqlQueryResult {
        rows,
        columns: result_columns.to_vec(),
        notices: Vec::new(),
    })
}

fn history_filter_notices(plan: &LogicalPlan) -> Vec<LixNotice> {
    let mut observations = Vec::new();
    collect_notice_observations(plan, &Vec::new(), &mut observations);

    let mut notices = Vec::new();
    let mut emitted_codes = HashSet::<String>::new();
    for observation in observations {
        for rule in HISTORY_NOTICE_RULES {
            if observation.table_name != rule.table_name {
                continue;
            }
            if !observation.references_any(rule.payload_columns)
                || observation.references_any(rule.identity_columns)
            {
                continue;
            }

            let code = format!("LIX_HISTORY_NON_IDENTITY_FILTER:{}", rule.table_name);
            if emitted_codes.insert(code) {
                notices.push(history_non_identity_filter_notice(rule.table_name));
            }
        }
    }
    notices
}

#[derive(Debug)]
struct NoticeObservation {
    table_name: String,
    filter_columns: HashSet<String>,
}

impl NoticeObservation {
    fn references_any(&self, columns: &[&str]) -> bool {
        columns
            .iter()
            .any(|column| self.filter_columns.contains(*column))
    }
}

struct HistoryNoticeRule {
    table_name: &'static str,
    payload_columns: &'static [&'static str],
    identity_columns: &'static [&'static str],
}

const HISTORY_NOTICE_RULES: &[HistoryNoticeRule] = &[
    HistoryNoticeRule {
        table_name: "lix_file_history",
        payload_columns: &[
            "path",
            "directory_id",
            "name",
            "extension",
            "hidden",
            "data",
        ],
        identity_columns: &["id", "lixcol_entity_id"],
    },
    HistoryNoticeRule {
        table_name: "lix_directory_history",
        payload_columns: &["path", "parent_id", "name", "hidden"],
        identity_columns: &["id", "lixcol_entity_id"],
    },
];

fn collect_notice_observations(
    plan: &LogicalPlan,
    active_filter_columns: &Vec<HashSet<String>>,
    observations: &mut Vec<NoticeObservation>,
) {
    match plan {
        LogicalPlan::Filter(filter) => {
            let mut next_filters = active_filter_columns.clone();
            next_filters.push(expr_column_names(&filter.predicate));
            collect_notice_observations(&filter.input, &next_filters, observations);
        }
        LogicalPlan::TableScan(scan) => {
            let mut filter_columns = HashSet::new();
            for columns in active_filter_columns {
                filter_columns.extend(columns.iter().cloned());
            }
            for filter in &scan.filters {
                filter_columns.extend(expr_column_names(filter));
            }
            if !filter_columns.is_empty() {
                observations.push(NoticeObservation {
                    table_name: table_reference_name(&scan.table_name),
                    filter_columns,
                });
            }
        }
        other => {
            for input in other.inputs() {
                collect_notice_observations(input, active_filter_columns, observations);
            }
        }
    }
}

fn expr_column_names(expr: &Expr) -> HashSet<String> {
    expr.column_refs()
        .iter()
        .map(|column| column.name.clone())
        .collect()
}

fn table_reference_name(table: &datafusion::common::TableReference) -> String {
    match table {
        datafusion::common::TableReference::Bare { table } => table.to_string(),
        datafusion::common::TableReference::Partial { table, .. } => table.to_string(),
        datafusion::common::TableReference::Full { table, .. } => table.to_string(),
    }
}

fn history_non_identity_filter_notice(view_name: &str) -> LixNotice {
    LixNotice {
        code: "LIX_HISTORY_NON_IDENTITY_FILTER".to_string(),
        message: format!("{view_name} was filtered without an identity predicate."),
        hint: Some(
            "Filter by id or lixcol_entity_id to include tombstones and renamed history."
                .to_string(),
        ),
    }
}

fn scalar_value_to_lix_value(
    value: &ScalarValue,
    field: Option<&Field>,
) -> Result<Value, LixError> {
    match value {
        ScalarValue::Null => Ok(Value::Null),
        ScalarValue::Boolean(Some(value)) => Ok(Value::Boolean(*value)),
        ScalarValue::Boolean(None) => Ok(Value::Null),
        ScalarValue::Int8(Some(value)) => Ok(Value::Integer(i64::from(*value))),
        ScalarValue::Int8(None) => Ok(Value::Null),
        ScalarValue::Int16(Some(value)) => Ok(Value::Integer(i64::from(*value))),
        ScalarValue::Int16(None) => Ok(Value::Null),
        ScalarValue::Int32(Some(value)) => Ok(Value::Integer(i64::from(*value))),
        ScalarValue::Int32(None) => Ok(Value::Null),
        ScalarValue::Int64(Some(value)) => Ok(Value::Integer(*value)),
        ScalarValue::Int64(None) => Ok(Value::Null),
        ScalarValue::UInt8(Some(value)) => Ok(Value::Integer(i64::from(*value))),
        ScalarValue::UInt8(None) => Ok(Value::Null),
        ScalarValue::UInt16(Some(value)) => Ok(Value::Integer(i64::from(*value))),
        ScalarValue::UInt16(None) => Ok(Value::Null),
        ScalarValue::UInt32(Some(value)) => Ok(Value::Integer(i64::from(*value))),
        ScalarValue::UInt32(None) => Ok(Value::Null),
        ScalarValue::UInt64(Some(value)) => match i64::try_from(*value) {
            Ok(value) => Ok(Value::Integer(value)),
            Err(_) => Ok(Value::Text(value.to_string())),
        },
        ScalarValue::UInt64(None) => Ok(Value::Null),
        ScalarValue::Float32(Some(value)) => Ok(Value::Real(f64::from(*value))),
        ScalarValue::Float32(None) => Ok(Value::Null),
        ScalarValue::Float64(Some(value)) => Ok(Value::Real(*value)),
        ScalarValue::Float64(None) => Ok(Value::Null),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => string_scalar_to_lix_value(value, field),
        ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) | ScalarValue::LargeUtf8(None) => {
            Ok(Value::Null)
        }
        ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value)) => {
            Ok(Value::Blob(value.clone()))
        }
        ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => Ok(Value::Null),
        other => Ok(Value::Text(other.to_string())),
    }
}

fn string_scalar_to_lix_value(value: &str, field: Option<&Field>) -> Result<Value, LixError> {
    if field.is_some_and(field_is_json) {
        return serde_json::from_str::<serde_json::Value>(value)
            .map(Value::Json)
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_INVALID_JSON",
                    format!(
                        "column '{}' is marked as JSON but contains invalid JSON: {error}",
                        field
                            .map(|field| field.name().as_str())
                            .unwrap_or("<unknown>")
                    ),
                )
            });
    }
    Ok(Value::Text(value.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use serde_json::json;
    use serde_json::Value as JsonValue;

    use super::{
        create_write_logical_plan, execute_logical_plan, execute_sql, SqlExecutionContext,
        SqlWriteExecutionContext,
    };
    use crate::backend::ReadScope;
    use crate::binary_cas::BlobDataReader;
    use crate::changelog::{CanonicalChange, ChangelogReader, ChangelogScanRequest};
    use crate::commit_graph::{
        CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest, CommitGraphChangeSet,
        CommitGraphChangeSetElement, CommitGraphCommit, CommitGraphEdge, CommitGraphReader,
        ReachableCommitGraphCommit,
    };
    use crate::functions::{
        FunctionProvider, FunctionProviderHandle, SharedFunctionProvider, SystemFunctionProvider,
    };
    use crate::json_store::JsonStoreContext;
    use crate::live_state::{
        LiveStateContext, LiveStateReader, LiveStateRow, LiveStateRowRequest, LiveStateScanRequest,
    };
    use crate::sql2::{ChangelogQuerySource, SqlChangelogQuerySource};
    use crate::tracked_state::TrackedStateContext;
    use crate::transaction::types::{StageRow, StageWrite, StageWriteOutcome};
    use crate::untracked_state::UntrackedStateContext;
    use crate::version::VersionRefReader;
    use crate::{Engine, ExecuteResult, SessionContext};
    use crate::{LixError, Value};

    struct DummyBlobReader;
    struct DummyLiveStateReader;
    struct RowsLiveStateReader {
        rows: Vec<LiveStateRow>,
    }
    struct BackendBlobReader(Arc<dyn crate::Backend + Send + Sync>);
    struct DummyChangelogReader;
    struct DummyCommitGraphReader;
    struct DummyVersionRefReader;

    #[allow(dead_code)]
    fn test_functions() -> FunctionProviderHandle {
        SharedFunctionProvider::new(
            Box::new(SystemFunctionProvider) as Box<dyn FunctionProvider + Send>
        )
    }

    #[derive(Default)]
    struct CapturingStagedWrites {
        deltas: Vec<CapturedStageWrite>,
    }

    #[derive(Clone)]
    struct CapturedStageWrite {
        rows: Vec<StageRow>,
    }

    impl CapturedStageWrite {
        fn pending_write_overlay(&self) -> Result<CapturedStageOverlay, LixError> {
            Ok(CapturedStageOverlay {
                rows: self.rows.clone(),
            })
        }
    }

    struct CapturedStageOverlay {
        rows: Vec<StageRow>,
    }

    impl CapturedStageOverlay {
        fn visible_semantic_rows(
            &self,
            include_tombstones: bool,
            schema_key: &str,
        ) -> Vec<CapturedStageRow> {
            self.visible_all_semantic_rows()
                .into_iter()
                .filter(|row| row.schema_key == schema_key)
                .filter(|row| include_tombstones || !row.tombstone)
                .collect()
        }

        fn visible_all_semantic_rows(&self) -> Vec<CapturedStageRow> {
            self.rows
                .iter()
                .cloned()
                .map(CapturedStageRow::from)
                .collect()
        }
    }

    struct CapturedStageRow {
        entity_id: String,
        schema_key: String,
        schema_version: String,
        version_id: String,
        file_id: Option<String>,
        snapshot_content: Option<String>,
        metadata: Option<String>,
        global: bool,
        untracked: bool,
        tombstone: bool,
    }

    impl From<StageRow> for CapturedStageRow {
        fn from(row: StageRow) -> Self {
            Self {
                entity_id: row
                    .entity_id
                    .expect("captured staged row should carry entity_id")
                    .as_string()
                    .expect("captured staged row should project entity_id"),
                schema_key: row.schema_key,
                schema_version: row.schema_version,
                version_id: row.version_id,
                file_id: row.file_id,
                global: row.global,
                untracked: row.untracked,
                tombstone: row.snapshot_content.is_none(),
                snapshot_content: row.snapshot_content,
                metadata: row.metadata,
            }
        }
    }

    struct DummySqlExecutionContext<'a> {
        active_version_id: &'a str,
        blob_reader: Arc<dyn BlobDataReader>,
        live_state: Arc<dyn LiveStateReader>,
        schema_definitions: Vec<JsonValue>,
    }

    impl<'a> SqlExecutionContext for DummySqlExecutionContext<'a> {
        fn active_version_id(&self) -> &str {
            self.active_version_id
        }

        fn live_state(&self) -> Arc<dyn LiveStateReader> {
            Arc::clone(&self.live_state)
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
            Arc::clone(&self.blob_reader)
        }

        fn changelog_query_source(&self) -> SqlChangelogQuerySource {
            let read_scope =
                ReadScope::new(Arc::new(crate::backend::testing::UnitTestBackend::new())
                    as Arc<dyn crate::Backend + Send + Sync>);
            ChangelogQuerySource {
                changelog_reader: Arc::new(DummyChangelogReader),
                json_reader: JsonStoreContext::new().reader(read_scope.store()),
            }
        }

        fn commit_graph(&self) -> Box<dyn CommitGraphReader> {
            Box::new(DummyCommitGraphReader)
        }

        fn version_ref(&self) -> Arc<dyn VersionRefReader> {
            Arc::new(DummyVersionRefReader)
        }

        fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
            Ok(self.schema_definitions.clone())
        }
    }

    struct DummySqlWriteExecutionContext<'a> {
        active_version_id: &'a str,
        blob_reader: Arc<dyn BlobDataReader>,
        live_state: Arc<dyn LiveStateReader>,
        staged_writes: Arc<Mutex<CapturingStagedWrites>>,
        schema_definitions: Vec<JsonValue>,
    }

    #[async_trait]
    impl SqlWriteExecutionContext for DummySqlWriteExecutionContext<'_> {
        fn active_version_id(&self) -> &str {
            self.active_version_id
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
            Arc::clone(&self.blob_reader)
        }

        fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
            Ok(self.schema_definitions.clone())
        }

        async fn scan_live_state(
            &mut self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<LiveStateRow>, LixError> {
            self.live_state.scan_rows(request).await
        }

        async fn load_version_head(
            &mut self,
            version_id: &str,
        ) -> Result<Option<String>, LixError> {
            Ok(Some(format!("commit-{version_id}")))
        }

        async fn stage_write(&mut self, write: StageWrite) -> Result<StageWriteOutcome, LixError> {
            let count = match &write {
                StageWrite::Rows { rows, .. } => rows.len() as u64,
                StageWrite::RowsWithFileData { count, .. } => *count,
                StageWrite::AdoptedChanges { changes } => changes.len() as u64,
            };
            let rows = match write {
                StageWrite::Rows { rows, .. } => rows,
                StageWrite::RowsWithFileData { rows, .. } => rows,
                StageWrite::AdoptedChanges { .. } => Vec::new(),
            };
            self.staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .push(CapturedStageWrite { rows });
            Ok(StageWriteOutcome { count })
        }
    }

    async fn execute_write_sql(
        ctx: &mut dyn SqlWriteExecutionContext,
        sql: &str,
        params: &[Value],
    ) -> Result<crate::SqlQueryResult, LixError> {
        let plan = create_write_logical_plan(ctx, sql).await?;
        execute_logical_plan(plan, params).await
    }

    #[async_trait]
    impl ChangelogReader for DummyChangelogReader {
        async fn load_change(&self, _change_id: &str) -> Result<Option<CanonicalChange>, LixError> {
            Ok(None)
        }

        async fn scan_changes(
            &self,
            _request: &ChangelogScanRequest,
        ) -> Result<Vec<CanonicalChange>, LixError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl VersionRefReader for DummyVersionRefReader {
        async fn load_head(
            &self,
            _version_id: &str,
        ) -> Result<Option<crate::version::VersionHead>, LixError> {
            Ok(None)
        }

        async fn scan_heads(&self) -> Result<Vec<crate::version::VersionHead>, LixError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl CommitGraphReader for DummyCommitGraphReader {
        async fn load_commit(
            &mut self,
            _commit_id: &str,
        ) -> Result<Option<CommitGraphCommit>, LixError> {
            Ok(None)
        }

        async fn all_commits(&mut self) -> Result<Vec<CommitGraphCommit>, LixError> {
            Ok(Vec::new())
        }

        async fn reachable_commits(
            &mut self,
            _head_commit_id: &str,
        ) -> Result<Vec<ReachableCommitGraphCommit>, LixError> {
            Ok(Vec::new())
        }

        async fn best_common_ancestors(
            &mut self,
            _left_commit_id: &str,
            _right_commit_id: &str,
        ) -> Result<Vec<CommitGraphCommit>, LixError> {
            Ok(Vec::new())
        }

        async fn merge_base(
            &mut self,
            _left_commit_id: &str,
            _right_commit_id: &str,
        ) -> Result<CommitGraphCommit, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "dummy commit graph reader cannot resolve merge base",
            ))
        }

        fn commit_edges(&self, _commits: &[CommitGraphCommit]) -> Vec<CommitGraphEdge> {
            Vec::new()
        }

        fn change_sets(&self, _commits: &[CommitGraphCommit]) -> Vec<CommitGraphChangeSet> {
            Vec::new()
        }

        async fn change_set_elements(
            &mut self,
            _commits: &[CommitGraphCommit],
        ) -> Result<Vec<CommitGraphChangeSetElement>, LixError> {
            Ok(Vec::new())
        }

        async fn change_history_from_commit(
            &mut self,
            _start_commit_id: &str,
            _request: &CommitGraphChangeHistoryRequest,
        ) -> Result<Vec<CommitGraphChangeHistoryEntry>, LixError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl LiveStateReader for DummyLiveStateReader {
        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<LiveStateRow>, LixError> {
            Ok(vec![])
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<LiveStateRow>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl LiveStateReader for RowsLiveStateReader {
        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<LiveStateRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<LiveStateRow>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl BlobDataReader for DummyBlobReader {
        async fn load_blob_data_by_hash(
            &self,
            _blob_hash: &str,
        ) -> Result<Option<Vec<u8>>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl BlobDataReader for BackendBlobReader {
        async fn load_blob_data_by_hash(
            &self,
            blob_hash: &str,
        ) -> Result<Option<Vec<u8>>, LixError> {
            let binary_cas = crate::binary_cas::BinaryCasContext::new();
            let reader = binary_cas.reader(self.0.as_ref());
            reader.load_blob_data_by_hash(blob_hash).await
        }
    }

    fn live_lix_state_row(entity_id: &str, metadata: Option<&str>) -> LiveStateRow {
        LiveStateRow {
            entity_id: crate::entity_identity::EntityIdentity::from_string(entity_id)
                .expect("entity id should decode"),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some("{\"key\":\"hello\",\"value\":\"world\"}".to_string()),
            metadata: metadata.map(ToOwned::to_owned),
            schema_version: "1".to_string(),
            version_id: "version-a".to_string(),
            change_id: Some(format!("change-{entity_id}")),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn live_entity_row(entity_id: &str, version_id: &str, value: &str) -> LiveStateRow {
        LiveStateRow {
            entity_id: crate::entity_identity::EntityIdentity::from_string(entity_id)
                .expect("entity id should decode"),
            schema_key: "test_state_schema".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: Some(format!("{{\"source\":\"{entity_id}\"}}")),
            schema_version: "1".to_string(),
            version_id: version_id.to_string(),
            change_id: Some(format!("change-{entity_id}")),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn live_directory_row(
        entity_id: &str,
        version_id: &str,
        parent_id: Option<&str>,
        name: &str,
        hidden: bool,
    ) -> LiveStateRow {
        LiveStateRow {
            entity_id: crate::entity_identity::EntityIdentity::from_string(entity_id)
                .expect("entity id should decode"),
            schema_key: "lix_directory_descriptor".to_string(),
            file_id: None,
            snapshot_content: Some(
                json!({
                    "id": entity_id,
                    "parent_id": parent_id,
                    "name": name,
                    "hidden": hidden
                })
                .to_string(),
            ),
            metadata: Some(format!("{{\"source\":\"{entity_id}\"}}")),
            schema_version: "1".to_string(),
            version_id: version_id.to_string(),
            change_id: Some(format!("change-{entity_id}")),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn live_file_row(
        entity_id: &str,
        version_id: &str,
        directory_id: Option<&str>,
        name: &str,
        extension: Option<&str>,
        hidden: bool,
    ) -> LiveStateRow {
        LiveStateRow {
            entity_id: crate::entity_identity::EntityIdentity::from_string(entity_id)
                .expect("entity id should decode"),
            schema_key: "lix_file_descriptor".to_string(),
            file_id: None,
            snapshot_content: Some(
                json!({
                    "id": entity_id,
                    "directory_id": directory_id,
                    "name": name,
                    "extension": extension,
                    "hidden": hidden
                })
                .to_string(),
            ),
            metadata: Some(format!("{{\"source\":\"{entity_id}\"}}")),
            schema_version: "1".to_string(),
            version_id: version_id.to_string(),
            change_id: Some(format!("change-{entity_id}")),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    #[tokio::test]
    async fn sql_execution_context_exposes_live_state_and_blob_reader() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader: Arc::clone(&blob_reader),
            live_state: Arc::clone(&live_state) as Arc<dyn LiveStateReader>,
            schema_definitions: vec![],
        };

        let actual = ctx.live_state();
        let expected = live_state as Arc<dyn LiveStateReader>;
        assert_eq!(ctx.active_version_id(), "version-a");
        assert!(Arc::ptr_eq(&actual, &expected));
        assert!(Arc::ptr_eq(&ctx.blob_reader(), &blob_reader));
    }

    #[tokio::test]
    async fn execute_sql_uses_execution_context_boundary() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            schema_definitions: vec![],
        };

        let result = execute_sql(&ctx, "SELECT 1", &[])
            .await
            .expect("sql2 execute should support literal-only queries");
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
    }

    #[tokio::test]
    async fn execute_sql_rejects_extra_parameters() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            schema_definitions: vec![],
        };

        let error = execute_sql(
            &ctx,
            "SELECT $1 AS value",
            &[Value::Integer(1), Value::Integer(2)],
        )
        .await
        .expect_err("extra params should fail instead of being ignored");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert_eq!(
            error.message,
            "SQL expected 1 parameter(s), but 2 parameter(s) were provided"
        );
        assert_eq!(
            error.details,
            Some(json!({
                "operation": "execute",
                "expected_param_count": 1,
                "provided_param_count": 2,
                "placeholders": ["$1"],
            }))
        );
    }

    #[tokio::test]
    async fn execute_sql_exposes_datafusion_information_schema() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            schema_definitions: vec![],
        };

        let information_schema_result = execute_sql(
            &ctx,
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'lix_state'",
            &[],
        )
        .await
        .expect("information_schema.tables should be enabled");
        assert_eq!(
            information_schema_result.rows,
            vec![vec![Value::Text("lix_state".to_string())]]
        );

        let tables_result = execute_sql(
            &ctx,
            "SELECT table_name FROM information_schema.tables",
            &[],
        )
        .await
        .expect("information_schema.tables should list registered tables");
        assert!(tables_result.rows.iter().any(|row| {
            row.iter()
                .any(|value| matches!(value, Value::Text(value) if value == "lix_state"))
        }));
    }

    async fn setup_engine2_history_fixture() -> Result<(SessionContext, String), LixError> {
        let backend = crate::backend::testing::UnitTestBackend::new();
        let init_receipt = Engine::initialize(Box::new(backend.clone())).await?;
        let engine = Engine::new(Box::new(backend)).await?;
        let session = engine.open_session(init_receipt.main_version_id).await?;

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"test_state_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"},\"count\":{\"type\":\"integer\"}},\"required\":[\"value\",\"count\"],\"additionalProperties\":false}'),\
                 true,\
                 true\
                 )",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO test_state_schema \
                 (lixcol_entity_id, value, count, lixcol_metadata, lixcol_untracked) \
                 VALUES ('entity-history', 'A', 7, '{\"source\":\"history\"}', false)",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO lix_directory (id, path, hidden) \
                 VALUES ('dir-docs', '/docs/', false)",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO lix_file (id, path, data, hidden) \
                 VALUES ('file-a', '/docs/readme.md', X'68656C6C6F', false)",
                &[],
            )
            .await?;

        let active_version_id = session.active_version_id().await?;
        let head_commit_id = engine
            .load_version_head_commit_id(&active_version_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "history fixture expected the session version to have a head commit",
                )
            })?;
        Ok((session, head_commit_id))
    }

    fn rows_from_execute_result(result: ExecuteResult) -> (Vec<String>, Vec<Vec<Value>>) {
        let rows = result;
        (
            rows.columns().to_vec(),
            rows.rows()
                .iter()
                .map(|row| row.values().to_vec())
                .collect(),
        )
    }

    #[tokio::test]
    async fn execute_sql_reads_lix_state_history_from_history_context() {
        let (session, head_commit_id) = setup_engine2_history_fixture()
            .await
            .expect("history fixture should initialize");
        let result = session
            .execute(
                &format!(
                    "SELECT entity_id, snapshot_content, metadata, depth, start_commit_id \
             FROM lix_state_history \
             WHERE schema_key = 'test_state_schema' \
               AND entity_id = 'entity-history' \
               AND start_commit_id = '{head_commit_id}' \
               AND depth >= 0"
                ),
                &[],
            )
            .await
            .expect("sql2 execute should read lix_state_history through real engine2 context");
        let (columns, rows) = rows_from_execute_result(result);

        assert_eq!(
            columns,
            vec![
                "entity_id",
                "snapshot_content",
                "metadata",
                "depth",
                "start_commit_id"
            ]
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Text("entity-history".to_string()));
        assert_eq!(rows[0][1], Value::Json(json!({"count": 7, "value": "A"})));
        assert_eq!(rows[0][2], Value::Json(json!({"source": "history"})));
        assert!(matches!(rows[0][3], Value::Integer(_)));
        assert_eq!(rows[0][4], Value::Text(head_commit_id.clone()));
    }

    #[tokio::test]
    async fn execute_sql_reads_entity_history_view_from_history_context() {
        let (session, head_commit_id) = setup_engine2_history_fixture()
            .await
            .expect("history fixture should initialize");
        let result = session
            .execute(
                &format!(
                    "SELECT value, count, lixcol_entity_id, lixcol_start_commit_id, lixcol_depth \
             FROM test_state_schema_history \
             WHERE lixcol_start_commit_id = '{head_commit_id}' \
               AND lixcol_entity_id = 'entity-history'"
                ),
                &[],
            )
            .await
            .expect("sql2 execute should read entity history through real engine2 context");
        let (columns, rows) = rows_from_execute_result(result);

        assert_eq!(
            columns,
            vec![
                "value",
                "count",
                "lixcol_entity_id",
                "lixcol_start_commit_id",
                "lixcol_depth",
            ]
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Text("A".to_string()));
        assert_eq!(rows[0][1], Value::Integer(7));
        assert_eq!(rows[0][2], Value::Text("entity-history".to_string()));
        assert_eq!(rows[0][3], Value::Text(head_commit_id));
        assert!(matches!(rows[0][4], Value::Integer(_)));
    }

    #[tokio::test]
    async fn execute_sql_reads_directory_history_view_from_history_context() {
        let (session, head_commit_id) = setup_engine2_history_fixture()
            .await
            .expect("history fixture should initialize");
        let result = session
            .execute(
                &format!(
                    "SELECT id, parent_id, name, path, hidden, lixcol_start_commit_id, lixcol_depth \
             FROM lix_directory_history \
             WHERE id = 'dir-docs' AND lixcol_start_commit_id = '{head_commit_id}'"
                ),
                &[],
            )
            .await
            .expect("sql2 execute should read directory history through real engine2 context");
        assert!(
            result.notices().is_empty(),
            "identity-filtered directory history should not emit soft notices"
        );
        let (columns, rows) = rows_from_execute_result(result);

        assert_eq!(
            columns,
            vec![
                "id",
                "parent_id",
                "name",
                "path",
                "hidden",
                "lixcol_start_commit_id",
                "lixcol_depth",
            ]
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Text("dir-docs".to_string()));
        assert_eq!(rows[0][1], Value::Null);
        assert_eq!(rows[0][2], Value::Text("docs".to_string()));
        assert_eq!(rows[0][3], Value::Text("/docs/".to_string()));
        assert_eq!(rows[0][4], Value::Boolean(false));
        assert_eq!(rows[0][5], Value::Text(head_commit_id.clone()));
        assert!(matches!(rows[0][6], Value::Integer(_)));

        let name_filtered_result = session
            .execute(
                &format!(
                    "SELECT id \
             FROM lix_directory_history \
             WHERE name = 'docs' \
               AND lixcol_start_commit_id = '{head_commit_id}'"
                ),
                &[],
            )
            .await
            .expect("sql2 execute should attach notices to name-filtered directory history reads");
        assert_eq!(name_filtered_result.notices().len(), 1);
        assert_eq!(
            name_filtered_result.notices()[0].code,
            "LIX_HISTORY_NON_IDENTITY_FILTER"
        );
    }

    #[tokio::test]
    async fn execute_sql_reads_file_history_view_from_history_context() {
        let (session, head_commit_id) = setup_engine2_history_fixture()
            .await
            .expect("history fixture should initialize");
        let result = session
            .execute(
                &format!(
                    "SELECT id, path, data, hidden, lixcol_start_commit_id, lixcol_depth \
             FROM lix_file_history \
             WHERE id = 'file-a' \
               AND lixcol_start_commit_id = '{head_commit_id}' \
               AND data IS NOT NULL \
             ORDER BY lixcol_depth",
                ),
                &[],
            )
            .await
            .expect("sql2 execute should read file history through real engine2 context");
        assert!(
            result.notices().is_empty(),
            "identity-filtered file history should not emit soft notices"
        );
        let (columns, rows) = rows_from_execute_result(result);

        assert_eq!(
            columns,
            vec![
                "id",
                "path",
                "data",
                "hidden",
                "lixcol_start_commit_id",
                "lixcol_depth",
            ]
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Text("file-a".to_string()));
        assert_eq!(rows[0][1], Value::Text("/docs/readme.md".to_string()));
        assert_eq!(rows[0][2], Value::Blob(b"hello".to_vec()));
        assert_eq!(rows[0][3], Value::Boolean(false));
        assert_eq!(rows[0][4], Value::Text(head_commit_id.clone()));
        assert!(matches!(rows[0][5], Value::Integer(_)));

        let path_filtered_result = session
            .execute(
                &format!(
                    "SELECT id \
             FROM lix_file_history \
             WHERE path = '/docs/readme.md' \
               AND lixcol_start_commit_id = '{head_commit_id}'"
                ),
                &[],
            )
            .await
            .expect("sql2 execute should attach notices to path-filtered file history reads");
        assert_eq!(path_filtered_result.notices().len(), 1);
        assert_eq!(
            path_filtered_result.notices()[0].code,
            "LIX_HISTORY_NON_IDENTITY_FILTER"
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_values_stages_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state (\
             entity_id, schema_key, file_id, snapshot_content, metadata, schema_version, global, untracked\
             ) VALUES (\
             'entity-1', 'lix_key_value', NULL, '{\"key\":\"hello\",\"value\":\"world\"}', '{\"source\":\"sql\"}', '1', false, false\
             )",
            &[],
        )
        .await
        .expect("INSERT INTO lix_state VALUES should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-1");
        assert_eq!(rows[0].schema_version, "1");
        assert_eq!(rows[0].version_id, "version-a");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"world\"}")
        );
        assert_eq!(rows[0].metadata.as_deref(), Some("{\"source\":\"sql\"}"));
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_defaults_global_and_untracked_to_false() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state (\
             entity_id, schema_key, file_id, snapshot_content, metadata, schema_version\
             ) VALUES (\
             'entity-defaults', 'lix_key_value', NULL, '{\"key\":\"hello\",\"value\":\"defaults\"}', NULL, '1'\
             )",
            &[],
        )
        .await
        .expect("INSERT INTO lix_state should default bookkeeping flags");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-defaults");
        assert_eq!(rows[0].version_id, "version-a");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_select_stages_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state (\
             entity_id, schema_key, file_id, snapshot_content, metadata, schema_version, global, untracked\
             ) \
             SELECT \
             'entity-from-select' AS entity_id, \
             'lix_key_value' AS schema_key, \
             NULL AS file_id, \
             '{\"key\":\"hello\",\"value\":\"from-select\"}' AS snapshot_content, \
             '{\"source\":\"select\"}' AS metadata, \
             '1' AS schema_version, \
             false AS global, \
             false AS untracked",
            &[],
        )
        .await
        .expect("INSERT INTO lix_state SELECT should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-from-select");
        assert_eq!(rows[0].schema_version, "1");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"from-select\"}")
        );
        assert_eq!(rows[0].metadata.as_deref(), Some("{\"source\":\"select\"}"));
    }

    #[tokio::test]
    async fn execute_sql_insert_into_entity_by_version_stages_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO test_state_schema_by_version (\
             lixcol_entity_id, lixcol_version_id, value\
             ) VALUES ('entity-c', 'version-b', 'C')",
            &[],
        )
        .await
        .expect("INSERT INTO entity by-version surface should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-c");
        assert_eq!(rows[0].schema_version, "1");
        assert_eq!(rows[0].version_id, "version-b");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"C\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_entity_defaults_active_version() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO test_state_schema (lixcol_entity_id, value) \
             VALUES ('entity-c', 'C')",
            &[],
        )
        .await
        .expect("INSERT INTO active entity surface should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-c");
        assert_eq!(rows[0].version_id, "version-a");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"C\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_directory_by_version_stages_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_directory_by_version (\
             id, parent_id, name, hidden, lixcol_version_id\
             ) VALUES ('dir-docs', NULL, 'docs', false, 'version-b')",
            &[],
        )
        .await
        .expect("INSERT INTO lix_directory_by_version should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_directory_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "dir-docs");
        assert_eq!(rows[0].schema_version, "1");
        assert_eq!(rows[0].version_id, "version-b");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"hidden\":false,\"id\":\"dir-docs\",\"name\":\"docs\",\"parent_id\":null}")
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_directory_defaults_active_version() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_directory (id, parent_id, name, hidden) \
             VALUES ('dir-docs', NULL, 'docs', false)",
            &[],
        )
        .await
        .expect("INSERT INTO lix_directory should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_directory_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "dir-docs");
        assert_eq!(rows[0].version_id, "version-a");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
    }

    #[tokio::test]
    async fn execute_sql_update_directory_stages_rewritten_descriptor() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_directory_row("dir-docs", "version-a", None, "docs", false),
                live_directory_row("dir-guides", "version-a", Some("dir-docs"), "guides", false),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_directory \
             SET hidden = true, lixcol_metadata = '{\"source\":\"directory-update\"}' \
             WHERE id = 'dir-docs'",
            &[],
        )
        .await
        .expect("UPDATE lix_directory should stage rewritten descriptor");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_directory_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "dir-docs");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"hidden\":true,\"id\":\"dir-docs\",\"name\":\"docs\",\"parent_id\":null}")
        );
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"source\":\"directory-update\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_update_directory_rejects_path_assignment() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![live_directory_row(
                "dir-docs",
                "version-a",
                None,
                "docs",
                false,
            )],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "UPDATE lix_directory SET path = '/renamed/' WHERE id = 'dir-docs'",
            &[],
        )
        .await
        .expect_err("path should remain read-only");

        assert!(
            error.message.contains("read-only column 'path'"),
            "unexpected error: {error:?}"
        );
        assert!(staged_writes
            .lock()
            .expect("staged writes lock")
            .deltas
            .is_empty());
    }

    #[tokio::test]
    async fn execute_sql_delete_directory_by_version_stages_tombstone() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_directory_row("dir-docs", "version-a", None, "docs", false),
                live_directory_row("dir-guides", "version-b", Some("dir-docs"), "guides", false),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "DELETE FROM lix_directory_by_version \
             WHERE id = 'dir-guides' AND lixcol_version_id = 'version-b'",
            &[],
        )
        .await
        .expect("DELETE lix_directory_by_version should stage tombstone");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "dir-guides");
        assert_eq!(rows[0].version_id, "version-b");
        assert!(rows[0].tombstone);
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_file_by_version_stages_descriptor_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_file_by_version (\
             id, directory_id, name, extension, hidden, lixcol_version_id\
             ) VALUES ('file-readme', 'dir-docs', 'readme', 'md', false, 'version-b')",
            &[],
        )
        .await
        .expect("INSERT INTO lix_file_by_version should stage descriptor write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "file-readme");
        assert_eq!(rows[0].schema_version, "1");
        assert_eq!(rows[0].version_id, "version-b");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        let snapshot: JsonValue =
            serde_json::from_str(rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "readme");
        assert_eq!(snapshot["extension"], "md");
        assert_eq!(snapshot["hidden"], false);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_file_defaults_active_version() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_file (id, directory_id, name, extension, hidden) \
             VALUES ('file-readme', 'dir-docs', 'readme', 'md', false)",
            &[],
        )
        .await
        .expect("INSERT INTO lix_file should stage descriptor write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "file-readme");
        assert_eq!(rows[0].version_id, "version-a");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_file_with_data_stages_blob_ref() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_file_by_version (\
             id, directory_id, name, extension, hidden, data, lixcol_version_id\
             ) VALUES ('file-readme', 'dir-docs', 'readme', 'md', false, X'4142', 'version-b')",
            &[],
        )
        .await
        .expect("INSERT INTO lix_file_by_version should stage descriptor and data writes");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let descriptor_rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(descriptor_rows.len(), 1);
        assert_eq!(descriptor_rows[0].entity_id, "file-readme");
        let blob_ref_rows = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_ref_rows.len(), 1);
        assert_eq!(blob_ref_rows[0].entity_id, "file-readme");
        assert_eq!(blob_ref_rows[0].file_id.as_deref(), Some("file-readme"));
        assert_eq!(blob_ref_rows[0].version_id, "version-b");
        let snapshot: JsonValue =
            serde_json::from_str(blob_ref_rows[0].snapshot_content.as_deref().unwrap())
                .expect("blob ref snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["size_bytes"], 2);
        assert!(snapshot["blob_hash"]
            .as_str()
            .is_some_and(|value| !value.is_empty()));
    }

    #[tokio::test]
    async fn execute_sql_update_file_stages_rewritten_descriptor() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_file_row(
                    "file-readme",
                    "version-a",
                    Some("dir-docs"),
                    "readme",
                    Some("md"),
                    false,
                ),
                live_file_row(
                    "file-guide",
                    "version-a",
                    Some("dir-docs"),
                    "guide",
                    Some("md"),
                    false,
                ),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_file \
             SET name = 'readme-updated', extension = 'txt', hidden = true, lixcol_metadata = '{\"source\":\"file-update\"}' \
             WHERE id = 'file-readme'",
            &[],
        )
        .await
        .expect("UPDATE lix_file should stage rewritten descriptor");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "file-readme");
        assert_eq!(rows[0].version_id, "version-a");
        let snapshot: JsonValue =
            serde_json::from_str(rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "readme-updated");
        assert_eq!(snapshot["extension"], "txt");
        assert_eq!(snapshot["hidden"], true);
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"source\":\"file-update\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_update_file_stages_data_blob_ref() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![live_file_row(
                "file-readme",
                "version-a",
                Some("dir-docs"),
                "readme",
                Some("md"),
                false,
            )],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_file SET data = X'4142' WHERE id = 'file-readme'",
            &[],
        )
        .await
        .expect("UPDATE lix_file should stage data write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        assert!(overlay
            .visible_semantic_rows(false, "lix_file_descriptor")
            .is_empty());
        let blob_ref_rows = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_ref_rows.len(), 1);
        assert_eq!(blob_ref_rows[0].entity_id, "file-readme");
        let snapshot: JsonValue =
            serde_json::from_str(blob_ref_rows[0].snapshot_content.as_deref().unwrap())
                .expect("blob ref snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["size_bytes"], 2);
    }

    #[tokio::test]
    async fn execute_sql_update_file_stages_path_assignment() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_directory_row("dir-docs", "version-a", None, "docs", false),
                live_file_row(
                    "file-readme",
                    "version-a",
                    Some("dir-docs"),
                    "readme",
                    Some("md"),
                    false,
                ),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_file SET path = '/docs/renamed.md' WHERE id = 'file-readme'",
            &[],
        )
        .await
        .expect("path update should stage descriptor rewrite");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        let snapshot: JsonValue =
            serde_json::from_str(rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "renamed");
        assert_eq!(snapshot["extension"], "md");
    }

    #[tokio::test]
    async fn execute_sql_delete_file_by_version_stages_descriptor_tombstone() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_file_row(
                    "file-readme",
                    "version-a",
                    Some("dir-docs"),
                    "readme",
                    Some("md"),
                    false,
                ),
                live_file_row(
                    "file-guide",
                    "version-b",
                    Some("dir-docs"),
                    "guide",
                    Some("md"),
                    false,
                ),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "DELETE FROM lix_file_by_version \
             WHERE id = 'file-guide' AND lixcol_version_id = 'version-b'",
            &[],
        )
        .await
        .expect("DELETE lix_file_by_version should stage descriptor tombstone");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "file-guide");
        assert_eq!(rows[0].version_id, "version-b");
        assert!(rows[0].tombstone);
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn execute_sql_update_entity_surface_stages_rewritten_snapshot() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_entity_row("entity-a", "version-a", "A"),
                live_entity_row("entity-b", "version-a", "B"),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE test_state_schema \
             SET value = 'updated', lixcol_metadata = '{\"source\":\"entity-update\"}' \
             WHERE value = 'A'",
            &[],
        )
        .await
        .expect("UPDATE entity surface should stage rewritten row");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-a");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"updated\"}")
        );
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"source\":\"entity-update\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_delete_entity_by_version_stages_tombstone() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_entity_row("entity-a", "version-a", "A"),
                live_entity_row("entity-b", "version-b", "B"),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "DELETE FROM test_state_schema_by_version \
             WHERE lixcol_version_id = 'version-b'",
            &[],
        )
        .await
        .expect("DELETE entity by-version surface should stage tombstone");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-b");
        assert_eq!(rows[0].version_id, "version-b");
        assert!(rows[0].tombstone);
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_stages_rewritten_rows() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_lix_state_row("entity-1", Some("{\"source\":\"match\"}")),
                live_lix_state_row("entity-2", Some("{\"source\":\"skip\"}")),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_state \
             SET snapshot_content = '{\"key\":\"hello\",\"value\":\"updated\"}', \
                 metadata = schema_key \
             WHERE metadata = '{\"source\":\"match\"}'",
            &[],
        )
        .await
        .expect("UPDATE lix_state should stage rewritten rows");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-1");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"updated\"}")
        );
        assert_eq!(rows[0].metadata.as_deref(), Some("lix_key_value"));
    }

    #[tokio::test]
    async fn execute_sql_delete_lix_state_without_where_stages_all_rows() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_lix_state_row("entity-1", Some("{\"source\":\"one\"}")),
                live_lix_state_row("entity-2", Some("{\"source\":\"two\"}")),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(&mut ctx, "DELETE FROM lix_state", &[])
            .await
            .expect("DELETE FROM lix_state should follow DataFusion delete-all semantics");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.tombstone));
        assert!(rows.iter().all(|row| row.snapshot_content.is_none()));
        assert!(rows.iter().any(|row| row.entity_id == "entity-1"));
        assert!(rows.iter().any(|row| row.entity_id == "entity-2"));
    }

    struct BackendSqlExecutionContext<'a> {
        active_version_id: &'a str,
        backend: Arc<dyn crate::Backend + Send + Sync>,
        blob_reader: Arc<dyn BlobDataReader>,
        live_state: Arc<dyn LiveStateReader>,
        schema_definitions: Vec<JsonValue>,
    }

    impl SqlExecutionContext for BackendSqlExecutionContext<'_> {
        fn active_version_id(&self) -> &str {
            self.active_version_id
        }

        fn live_state(&self) -> Arc<dyn LiveStateReader> {
            Arc::clone(&self.live_state)
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
            Arc::clone(&self.blob_reader)
        }

        fn changelog_query_source(&self) -> SqlChangelogQuerySource {
            let read_scope = ReadScope::new(Arc::clone(&self.backend));
            ChangelogQuerySource {
                changelog_reader: Arc::new(
                    crate::changelog::ChangelogContext::new().reader(read_scope.store()),
                ),
                json_reader: JsonStoreContext::new().reader(read_scope.store()),
            }
        }

        fn commit_graph(&self) -> Box<dyn CommitGraphReader> {
            Box::new(DummyCommitGraphReader)
        }

        fn version_ref(&self) -> Arc<dyn VersionRefReader> {
            Arc::new(
                crate::version::VersionContext::new(Arc::new(UntrackedStateContext::new()))
                    .ref_reader(Arc::clone(&self.backend)),
            )
        }

        fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
            Ok(self.schema_definitions.clone())
        }
    }

    async fn setup_sql2_state_fixture(
    ) -> Result<(crate::backend::testing::UnitTestBackend, JsonValue), crate::LixError> {
        let backend = crate::backend::testing::UnitTestBackend::new();
        let init_receipt = Engine::initialize(Box::new(backend.clone())).await?;
        crate::test_support::seed_version_head(
            &backend,
            "version-a",
            &format!("{}-version-a-root", init_receipt.initial_commit_id),
        )
        .await;
        crate::test_support::seed_version_head(
            &backend,
            "version-b",
            &format!("{}-version-b-root", init_receipt.initial_commit_id),
        )
        .await;
        let engine = Engine::new(Box::new(backend.clone())).await?;
        let session_a = engine.open_session("version-a").await?;
        let session_b = engine.open_session("version-b").await?;
        let schema_definition = json!({
            "x-lix-key": "test_state_schema",
            "x-lix-version": "1",
            "type": "object",
            "properties": {
                "value": { "type": "string" }
            },
            "required": ["value"],
            "additionalProperties": false
        });
        session_a
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"test_state_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"}},\"required\":[\"value\"],\"additionalProperties\":false}'),\
                 true,\
                 true\
                 )",
                &[],
            )
            .await?;
        session_a
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, snapshot_content, schema_version, global, untracked\
                 ) VALUES (\
                 'entity-a', 'test_state_schema', NULL, '{\"value\":\"A\"}', '1', false, false\
                 )",
                &[],
            )
            .await?;
        session_b
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, snapshot_content, schema_version, global, untracked\
                 ) VALUES (\
                 'entity-b', 'test_state_schema', NULL, '{\"value\":\"B\"}', '1', false, false\
                 )",
                &[],
            )
            .await?;
        session_a
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, snapshot_content, schema_version, global, untracked\
                 ) VALUES (\
                 'dir-docs', 'lix_directory_descriptor', NULL, '{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\",\"hidden\":false}', '1', false, false\
                 )",
                &[],
            )
            .await?;
        session_a
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-a', '/docs/readme.md', X'4142')",
                &[],
            )
            .await?;
        Ok((backend, schema_definition))
    }

    fn test_live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            TrackedStateContext::new(),
            UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(crate::changelog::ChangelogContext::new()),
        )
    }

    fn run_async_test_with_large_stack(
        test: impl FnOnce() -> futures_util::future::LocalBoxFuture<'static, ()> + Send + 'static,
    ) {
        std::thread::Builder::new()
            .name("sql2-execute-test".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime should build")
                    .block_on(test());
            })
            .expect("test thread should spawn")
            .join()
            .expect("test thread should join");
    }

    #[test]
    fn execute_sql_reads_lix_state_by_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::Backend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    backend: Arc::clone(&backend_ref),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(
                        test_live_state_context().reader(Arc::clone(&backend_ref)),
                    ),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT entity_id, version_id, snapshot_content, commit_id \
                     FROM lix_state_by_version \
                     WHERE version_id = 'version-b' AND schema_key = 'test_state_schema'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_state_by_version");

                assert_eq!(
                    result.columns,
                    vec!["entity_id", "version_id", "snapshot_content", "commit_id"]
                );
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("entity-b".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("version-b".to_string()));
                assert_eq!(result.rows[0][2], Value::Json(json!({"value": "B"})));
                match &result.rows[0][3] {
                    Value::Text(commit_id) => assert!(!commit_id.is_empty()),
                    other => panic!("expected non-null commit_id text, got {other:?}"),
                }
            })
        });
    }

    #[test]
    fn execute_sql_supports_broad_lix_state_by_version_reads() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::Backend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    backend: Arc::clone(&backend_ref),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(
                        test_live_state_context().reader(Arc::clone(&backend_ref)),
                    ),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT entity_id FROM lix_state_by_version WHERE schema_key = 'test_state_schema'",
                    &[],
                )
                .await
                .expect("broad by-version read should succeed");

                assert!(
                    result.rows.iter().any(|row| row[0] == Value::Text("entity-a".to_string()))
                        && result.rows.iter().any(|row| row[0] == Value::Text("entity-b".to_string())),
                    "expected broad by-version read to include rows from multiple visible versions: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_state_from_active_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::Backend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    backend: Arc::clone(&backend_ref),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(
                        test_live_state_context().reader(Arc::clone(&backend_ref)),
                    ),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT entity_id, snapshot_content \
                     FROM lix_state \
                     WHERE schema_key = 'test_state_schema'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_state");

                assert_eq!(result.columns, vec!["entity_id", "snapshot_content"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("entity-a".to_string()));
                assert_eq!(result.rows[0][1], Value::Json(json!({"value": "A"})));
            })
        });
    }

    #[test]
    fn execute_sql_reads_entity_view_from_active_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::Backend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    backend: Arc::clone(&backend_ref),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(
                        test_live_state_context().reader(Arc::clone(&backend_ref)),
                    ),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT value, lixcol_entity_id \
                     FROM test_state_schema",
                    &[],
                )
                .await
                .expect("sql2 execute should read entity view");

                assert_eq!(result.columns, vec!["value", "lixcol_entity_id"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("A".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("entity-a".to_string()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_entity_by_version_view() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::Backend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    backend: Arc::clone(&backend_ref),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(
                        test_live_state_context().reader(Arc::clone(&backend_ref)),
                    ),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT value, lixcol_version_id \
                     FROM test_state_schema_by_version \
                     WHERE lixcol_version_id = 'version-b'",
                    &[],
                )
                .await
                .expect("sql2 execute should read entity by-version view");

                assert_eq!(result.columns, vec!["value", "lixcol_version_id"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("B".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("version-b".to_string()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_directory_by_version_view() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::Backend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    backend: Arc::clone(&backend_ref),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(
                        test_live_state_context().reader(Arc::clone(&backend_ref)),
                    ),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name, lixcol_version_id \
                     FROM lix_directory_by_version \
                     WHERE id = 'dir-docs' AND lixcol_version_id = 'version-a'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_directory_by_version");

                assert_eq!(result.columns, vec!["path", "name", "lixcol_version_id"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("/docs/".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("docs".to_string()));
                assert_eq!(result.rows[0][2], Value::Text("version-a".to_string()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_directory_from_active_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::Backend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    backend: Arc::clone(&backend_ref),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(
                        test_live_state_context().reader(Arc::clone(&backend_ref)),
                    ),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name \
                     FROM lix_directory \
                     WHERE id = 'dir-docs'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_directory");

                assert_eq!(result.columns, vec!["path", "name"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("/docs/".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("docs".to_string()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_file_by_version_view() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::Backend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    backend: Arc::clone(&backend_ref),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(
                        test_live_state_context().reader(Arc::clone(&backend_ref)),
                    ),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name, extension, data, lixcol_version_id \
                     FROM lix_file_by_version \
                     WHERE id = 'file-a' AND lixcol_version_id = 'version-a'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_file_by_version");

                assert_eq!(
                    result.columns,
                    vec!["path", "name", "extension", "data", "lixcol_version_id"]
                );
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0][0],
                    Value::Text("/docs/readme.md".to_string())
                );
                assert_eq!(result.rows[0][1], Value::Text("readme".to_string()));
                assert_eq!(result.rows[0][2], Value::Text("md".to_string()));
                assert_eq!(result.rows[0][3], Value::Blob(vec![0x41, 0x42]));
                assert_eq!(result.rows[0][4], Value::Text("version-a".to_string()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_file_from_active_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::Backend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    backend: Arc::clone(&backend_ref),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(
                        test_live_state_context().reader(Arc::clone(&backend_ref)),
                    ),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name, extension, data \
                     FROM lix_file \
                     WHERE id = 'file-a'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_file");

                assert_eq!(result.columns, vec!["path", "name", "extension", "data"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0][0],
                    Value::Text("/docs/readme.md".to_string())
                );
                assert_eq!(result.rows[0][1], Value::Text("readme".to_string()));
                assert_eq!(result.rows[0][2], Value::Text("md".to_string()));
                assert_eq!(result.rows[0][3], Value::Blob(vec![0x41, 0x42]));
            })
        });
    }
}
