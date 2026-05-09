use datafusion::arrow::datatypes::Field;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::metadata::{FieldMetadata, ScalarAndMetadata};
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::logical_expr::{Expr, LogicalPlan, WriteOp};
use datafusion::prelude::SessionContext;
use serde_json::{json, Value as JsonValue};
use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::schema::schema_key_from_definition;
use crate::{LixError, LixNotice, SqlQueryResult, Value};

use super::predicate_typecheck::validate_json_predicate_expr_with_dfschema;
use super::result_metadata::{field_is_json, LIX_VALUE_TYPE_JSON, LIX_VALUE_TYPE_METADATA_KEY};
use super::session::{build_read_session, build_write_session};
use super::write_normalization::{
    is_binary_type, lix_file_data_type_lix_error, logical_expr_is_binary_or_null,
};
use super::{SqlExecutionContext, SqlStatementKind, SqlWriteExecutionContext};

#[allow(dead_code)]
pub(crate) struct SqlLogicalPlan {
    session: SessionContext,
    plan: LogicalPlan,
    kind: SqlStatementKind,
    notices: Vec<LixNotice>,
    strict_binary_params: BTreeSet<usize>,
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
    validate_json_predicates_in_logical_plan(&plan)?;
    let kind = classify_logical_plan(&plan);
    let notices = history_filter_notices(&plan);

    Ok(SqlLogicalPlan {
        session,
        plan,
        kind,
        notices,
        strict_binary_params: BTreeSet::new(),
    })
}

#[allow(dead_code)]
pub(crate) async fn create_write_logical_plan(
    ctx: &mut dyn SqlWriteExecutionContext,
    sql: &str,
) -> Result<SqlLogicalPlan, LixError> {
    super::validate_supported_statement_ast(sql)?;
    reject_read_only_history_view_dml(sql, &ctx.list_visible_schemas()?)?;
    let session = build_write_session(ctx).await?;
    let plan = session
        .state()
        .create_logical_plan(sql)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    validate_supported_logical_plan(&plan)?;
    validate_json_predicates_in_logical_plan(&plan)?;
    let strict_binary_params = validate_strict_lix_file_data_writes(&plan)?;
    let kind = classify_logical_plan(&plan);

    Ok(SqlLogicalPlan {
        session,
        plan,
        kind,
        notices: Vec::new(),
        strict_binary_params,
    })
}

fn validate_json_predicates_in_logical_plan(plan: &LogicalPlan) -> Result<(), LixError> {
    match plan {
        LogicalPlan::Filter(filter) => {
            validate_json_predicate_expr_with_dfschema(filter.input.schema(), &filter.predicate)?;
        }
        LogicalPlan::TableScan(scan) => {
            for filter in &scan.filters {
                validate_json_predicate_expr_with_dfschema(scan.projected_schema.as_ref(), filter)?;
            }
        }
        _ => {}
    }

    for input in plan.inputs() {
        validate_json_predicates_in_logical_plan(input)?;
    }

    Ok(())
}

fn validate_strict_lix_file_data_writes(plan: &LogicalPlan) -> Result<BTreeSet<usize>, LixError> {
    let mut strict_binary_params = BTreeSet::new();
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(strict_binary_params);
    };
    if dml.table_name.table() != "lix_file"
        || !matches!(dml.op, WriteOp::Insert(_) | WriteOp::Update)
    {
        return Ok(strict_binary_params);
    }

    reject_non_binary_lix_file_data_write(&dml.input, &mut strict_binary_params)?;
    Ok(strict_binary_params)
}

fn reject_non_binary_lix_file_data_write(
    input: &LogicalPlan,
    strict_binary_params: &mut BTreeSet<usize>,
) -> Result<(), LixError> {
    let LogicalPlan::Projection(projection) = input else {
        return Ok(());
    };

    let Some(data_expr) = projection.expr.iter().find_map(|expr| match expr {
        Expr::Alias(alias) if alias.name == "data" => Some(alias.expr.as_ref()),
        _ => None,
    }) else {
        return Ok(());
    };

    validate_lix_file_data_expr(data_expr, strict_binary_params)?;

    let Expr::Column(column) = data_expr else {
        return Ok(());
    };
    let LogicalPlan::Values(values) = projection.input.as_ref() else {
        return Ok(());
    };
    let Ok(column_index) = values.schema.index_of_column(column) else {
        return Ok(());
    };

    for row in &values.values {
        if let Some(value_expr) = row.get(column_index) {
            validate_lix_file_data_expr(value_expr, strict_binary_params)?;
        }
    }

    Ok(())
}

fn validate_lix_file_data_expr(
    expr: &Expr,
    strict_binary_params: &mut BTreeSet<usize>,
) -> Result<(), LixError> {
    match expr {
        Expr::Cast(cast) if is_binary_type(&cast.data_type) => {
            if collect_placeholder_param(&cast.expr, strict_binary_params)? {
                return Ok(());
            }
            if !logical_expr_is_binary_or_null(&cast.expr) {
                return Err(lix_file_data_type_lix_error());
            }
        }
        Expr::Placeholder(_) => {
            collect_placeholder_param(expr, strict_binary_params)?;
        }
        Expr::Alias(alias) => validate_lix_file_data_expr(&alias.expr, strict_binary_params)?,
        _ => {}
    }
    Ok(())
}

fn collect_placeholder_param(
    expr: &Expr,
    strict_binary_params: &mut BTreeSet<usize>,
) -> Result<bool, LixError> {
    match expr {
        Expr::Placeholder(placeholder) => {
            let index = placeholder_index(&placeholder.id)?;
            strict_binary_params.insert(index);
            Ok(true)
        }
        Expr::Alias(alias) => collect_placeholder_param(&alias.expr, strict_binary_params),
        _ => Ok(false),
    }
}

fn placeholder_index(id: &str) -> Result<usize, LixError> {
    id.strip_prefix('$')
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|index| *index > 0)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_PARSE_ERROR,
                format!("unsupported SQL parameter placeholder '{id}'"),
            )
            .with_hint("Use numbered placeholders like $1, $2, ...")
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
        strict_binary_params,
    } = plan;
    validate_parameter_count(&plan, params.len())?;
    validate_strict_binary_params(&strict_binary_params, params)?;

    let mut dataframe = session
        .execute_logical_plan(plan)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    if !params.is_empty() {
        dataframe = dataframe
            .with_param_values(ParamValues::List(
                params.iter().map(scalar_value_from_lix_value).collect(),
            ))
            .map_err(datafusion_error_to_lix_error)?;
    }

    let result_fields = dataframe
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let batches = super::runtime::collect_dataframe(dataframe)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let mut result = query_result_from_batches(&result_fields, &batches)?;
    result.notices = notices;
    Ok(result)
}

fn validate_strict_binary_params(
    strict_binary_params: &BTreeSet<usize>,
    params: &[Value],
) -> Result<(), LixError> {
    for index in strict_binary_params {
        let Some(value) = params.get(index - 1) else {
            continue;
        };
        if !matches!(value, Value::Blob(_)) {
            return Err(lix_file_data_type_lix_error());
        }
    }
    Ok(())
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

fn reject_read_only_history_view_dml(
    sql: &str,
    visible_schemas: &[JsonValue],
) -> Result<(), LixError> {
    let target_names = super::dml_target_table_names(sql)?;
    for target_name in target_names {
        if is_history_view_name(&target_name, visible_schemas)? {
            return Err(read_only_history_view_error(&target_name));
        }
    }
    Ok(())
}

fn is_history_view_name(table_name: &str, visible_schemas: &[JsonValue]) -> Result<bool, LixError> {
    if matches!(
        table_name,
        "lix_state_history" | "lix_file_history" | "lix_directory_history"
    ) {
        return Ok(true);
    }

    for schema in visible_schemas {
        let schema_key = schema_key_from_definition(schema)?;
        if table_name == format!("{}_history", schema_key.schema_key) {
            return Ok(true);
        }
    }

    Ok(false)
}

fn read_only_history_view_error(view_name: &str) -> LixError {
    LixError::new(
        LixError::CODE_READ_ONLY,
        format!("DML cannot write read-only history view '{view_name}'"),
    )
    .with_hint(
        "History views are query-only; write to the live surface such as lix_state, lix_file, lix_directory, or the typed entity table.",
    )
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

fn scalar_value_from_lix_value(value: &Value) -> ScalarAndMetadata {
    match value {
        Value::Null => ScalarValue::Null.into(),
        Value::Boolean(value) => ScalarValue::Boolean(Some(*value)).into(),
        Value::Integer(value) => ScalarValue::Int64(Some(*value)).into(),
        Value::Real(value) => ScalarValue::Float64(Some(*value)).into(),
        Value::Text(value) => ScalarValue::Utf8(Some(value.clone())).into(),
        Value::Json(value) => ScalarAndMetadata::new(
            ScalarValue::Utf8(Some(value.to_string())),
            Some(json_field_metadata()),
        ),
        Value::Blob(value) => ScalarValue::Binary(Some(value.clone())).into(),
    }
}

fn json_field_metadata() -> FieldMetadata {
    FieldMetadata::new(BTreeMap::from([(
        LIX_VALUE_TYPE_METADATA_KEY.to_string(),
        LIX_VALUE_TYPE_JSON.to_string(),
    )]))
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
        payload_columns: &["path", "directory_id", "name", "hidden", "data"],
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
    use crate::binary_cas::BlobDataReader;
    use crate::commit_graph::{
        CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest, CommitGraphCommit,
        CommitGraphEdge, CommitGraphReader, ReachableCommitGraphCommit,
    };
    use crate::commit_store::CommitStoreContext;
    use crate::functions::{
        FunctionProvider, FunctionProviderHandle, SharedFunctionProvider, SystemFunctionProvider,
    };
    use crate::json_store::JsonStoreContext;
    use crate::live_state::{
        LiveStateContext, LiveStateReader, LiveStateRowRequest, LiveStateScanRequest,
        MaterializedLiveStateRow,
    };
    use crate::sql2::{CommitStoreQuerySource, SqlCommitStoreQuerySource};
    use crate::storage::{
        KvEntryPage, KvExistsBatch, KvGetRequest, KvKeyPage, KvScanRequest, KvValueBatch,
        KvValuePage, StorageContext, StorageReadScope, StorageReadTransaction, StorageReader,
        StorageWriteSet,
    };
    use crate::tracked_state::TrackedStateContext;
    use crate::transaction::prepare_version_ref_row;
    use crate::transaction::types::{
        TransactionWrite, TransactionWriteOutcome, TransactionWriteRow,
    };
    use crate::untracked_state::UntrackedStateContext;
    use crate::version::VersionRefReader;
    use crate::{Engine, ExecuteResult, SessionContext};
    use crate::{LixError, Value};

    struct DummyBlobReader;
    struct DummyLiveStateReader;
    struct RowsLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
    }
    struct BackendBlobReader(StorageContext);
    struct DummyCommitGraphReader;
    struct DummyVersionRefReader;
    struct TestReadTransaction(StorageContext);

    fn test_read_scope(
        storage: StorageContext,
    ) -> StorageReadScope<Box<dyn StorageReadTransaction + Send + Sync + 'static>> {
        StorageReadScope::new(Box::new(TestReadTransaction(storage)))
    }

    #[async_trait]
    impl StorageReader for TestReadTransaction {
        async fn get_values(&mut self, request: KvGetRequest) -> Result<KvValueBatch, LixError> {
            self.0.get_values(request).await
        }

        async fn exists_many(&mut self, request: KvGetRequest) -> Result<KvExistsBatch, LixError> {
            self.0.exists_many(request).await
        }

        async fn scan_keys(&mut self, request: KvScanRequest) -> Result<KvKeyPage, LixError> {
            self.0.scan_keys(request).await
        }

        async fn scan_values(&mut self, request: KvScanRequest) -> Result<KvValuePage, LixError> {
            self.0.scan_values(request).await
        }

        async fn scan_entries(&mut self, request: KvScanRequest) -> Result<KvEntryPage, LixError> {
            self.0.scan_entries(request).await
        }
    }

    #[async_trait]
    impl StorageReadTransaction for TestReadTransaction {
        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

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
        rows: Vec<TransactionWriteRow>,
    }

    impl CapturedStageWrite {
        fn pending_write_overlay(&self) -> Result<CapturedStageOverlay, LixError> {
            Ok(CapturedStageOverlay {
                rows: self.rows.clone(),
            })
        }
    }

    struct CapturedStageOverlay {
        rows: Vec<TransactionWriteRow>,
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
        version_id: String,
        file_id: Option<String>,
        snapshot_content: Option<String>,
        metadata: Option<String>,
        global: bool,
        untracked: bool,
        tombstone: bool,
    }

    impl From<TransactionWriteRow> for CapturedStageRow {
        fn from(row: TransactionWriteRow) -> Self {
            Self {
                entity_id: row
                    .entity_id
                    .expect("captured staged row should carry entity_id")
                    .as_json_array_text()
                    .expect("captured staged row should project entity_id"),
                schema_key: row.schema_key,
                version_id: row.version_id,
                file_id: row.file_id,
                global: row.global,
                untracked: row.untracked,
                tombstone: row.snapshot.is_none(),
                snapshot_content: row.snapshot.map(|snapshot| snapshot.to_string()),
                metadata: row.metadata.map(|metadata| metadata.to_string()),
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

        fn commit_store_query_source(&self) -> SqlCommitStoreQuerySource {
            let base_scope = test_read_scope(StorageContext::new(Arc::new(
                crate::backend::testing::UnitTestBackend::new(),
            )));
            let read_scope = StorageReadScope::new(base_scope.store());
            CommitStoreQuerySource {
                commit_store_reader: Arc::new(CommitStoreContext::new().reader(read_scope.store())),
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

        fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
            Ok(self.schema_definitions.clone())
        }

        async fn load_bytes_many(
            &mut self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            self.blob_reader.load_bytes_many(hashes).await
        }

        async fn scan_live_state(
            &mut self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            self.live_state.scan_rows(request).await
        }

        async fn load_version_head(
            &mut self,
            version_id: &str,
        ) -> Result<Option<String>, LixError> {
            Ok(Some(format!("commit-{version_id}")))
        }

        async fn stage_write(
            &mut self,
            write: TransactionWrite,
        ) -> Result<TransactionWriteOutcome, LixError> {
            let count = match &write {
                TransactionWrite::Rows { rows, .. } => rows.len() as u64,
                TransactionWrite::RowsWithFileData { count, .. } => *count,
                TransactionWrite::AdoptedChanges { changes } => changes.len() as u64,
            };
            let rows = match write {
                TransactionWrite::Rows { rows, .. } => rows,
                TransactionWrite::RowsWithFileData { rows, .. } => rows,
                TransactionWrite::AdoptedChanges { .. } => Vec::new(),
            };
            self.staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .push(CapturedStageWrite { rows });
            Ok(TransactionWriteOutcome { count })
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
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(vec![])
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl LiveStateReader for RowsLiveStateReader {
        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl BlobDataReader for DummyBlobReader {
        async fn load_bytes_many(
            &self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            Ok(crate::binary_cas::BlobBytesBatch::new(vec![
                None;
                hashes.len()
            ]))
        }
    }

    #[async_trait]
    impl BlobDataReader for BackendBlobReader {
        async fn load_bytes_many(
            &self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            let binary_cas = crate::binary_cas::BinaryCasContext::new();
            let reader = binary_cas.reader(self.0.clone());
            reader.load_bytes_many(hashes).await
        }
    }

    fn live_lix_state_row(entity_id: &str, metadata: Option<&str>) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single(entity_id),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some("{\"key\":\"hello\",\"value\":\"world\"}".to_string()),
            metadata: metadata.map(str::to_string),
            deleted: false,
            version_id: "version-a".to_string(),
            change_id: Some(format!("change-{entity_id}")),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn live_entity_row(entity_id: &str, version_id: &str, value: &str) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single(entity_id),
            schema_key: "test_state_schema".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: Some(json!({ "source": entity_id }).to_string()),
            deleted: false,
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
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single(entity_id),
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
            metadata: Some(json!({ "source": entity_id }).to_string()),
            deleted: false,
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
        hidden: bool,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single(entity_id),
            schema_key: "lix_file_descriptor".to_string(),
            file_id: None,
            snapshot_content: Some(
                json!({
                    "id": entity_id,
                    "directory_id": directory_id,
                    "name": name,
                    "hidden": hidden
                })
                .to_string(),
            ),
            metadata: Some(json!({ "source": entity_id }).to_string()),
            deleted: false,
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
    async fn execute_sql_collects_union_all_partitions() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            schema_definitions: vec![],
        };

        let result = execute_sql(&ctx, "SELECT 1 UNION ALL SELECT 2", &[])
            .await
            .expect("sql2 execute should collect UNION ALL partitions");
        assert_eq!(
            result.rows,
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
        );
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

    async fn setup_engine_history_fixture() -> Result<(SessionContext, String), LixError> {
        let backend = crate::backend::testing::UnitTestBackend::new();
        let init_receipt = Engine::initialize(Box::new(backend.clone())).await?;
        let engine = Engine::new(Box::new(backend)).await?;
        let session = engine.open_session(init_receipt.main_version_id).await?;

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"test_state_schema\",\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"},\"count\":{\"type\":\"integer\"}},\"required\":[\"value\",\"count\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO test_state_schema \
	             (lixcol_entity_id, value, count, lixcol_metadata, lixcol_untracked) \
	             VALUES (lix_json('[\"entity-history\"]'), 'A', 7, '{\"source\":\"history\"}', false)",
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

    #[tokio::test]
    async fn lix_file_path_predicates_canonicalize_bound_values_like_writes() {
        let backend = crate::backend::testing::UnitTestBackend::new();
        let init_receipt = Engine::initialize(Box::new(backend.clone()))
            .await
            .expect("engine should initialize");
        let engine = Engine::new(Box::new(backend))
            .await
            .expect("engine should open");
        let session = engine
            .open_session(init_receipt.main_version_id)
            .await
            .expect("session should open");

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('file-nfc', $1, X'41')",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("NFD path insert should canonicalize");

        let nfd_result = session
            .execute(
                "SELECT id FROM lix_file WHERE path = $1",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("NFD path predicate should canonicalize");
        assert_eq!(
            rows_from_execute_result(nfd_result).1,
            vec![vec![Value::Text("file-nfc".to_string())]]
        );

        let percent_result = session
            .execute(
                "SELECT id FROM lix_file WHERE path = '/%43afe%CC%81.txt'",
                &[],
            )
            .await
            .expect("percent-encoded path predicate should canonicalize");
        assert_eq!(
            rows_from_execute_result(percent_result).1,
            vec![vec![Value::Text("file-nfc".to_string())]]
        );

        let reversed_result = session
            .execute(
                "SELECT id FROM lix_file WHERE $1 = path",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("reversed path predicate should canonicalize");
        assert_eq!(
            rows_from_execute_result(reversed_result).1,
            vec![vec![Value::Text("file-nfc".to_string())]]
        );

        let or_result = session
            .execute(
                "SELECT id FROM lix_file WHERE path = $1 OR id = 'missing'",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("OR path predicate should canonicalize");
        assert_eq!(
            rows_from_execute_result(or_result).1,
            vec![vec![Value::Text("file-nfc".to_string())]]
        );

        let not_result = session
            .execute(
                "SELECT id FROM lix_file WHERE NOT (path = $1)",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("NOT path predicate should canonicalize");
        assert!(rows_from_execute_result(not_result).1.is_empty());

        let not_in_result = session
            .execute(
                "SELECT id FROM lix_file WHERE path NOT IN ($1)",
                &[Value::Text("/%43afe%CC%81.txt".to_string())],
            )
            .await
            .expect("NOT IN path predicate should canonicalize");
        assert!(rows_from_execute_result(not_in_result).1.is_empty());

        let update_result = session
            .execute(
                "UPDATE lix_file SET hidden = true WHERE path = $1 OR id = 'missing'",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("update predicate should canonicalize through OR");
        assert_eq!(update_result.rows_affected(), 1);

        let delete_result = session
            .execute(
                "DELETE FROM lix_file WHERE path = $1",
                &[Value::Text("/%43afe%CC%81.txt".to_string())],
            )
            .await
            .expect("delete predicate should canonicalize");
        assert_eq!(delete_result.rows_affected(), 1);
    }

    #[tokio::test]
    async fn lix_file_path_predicates_reject_non_literal_path_values() {
        let backend = crate::backend::testing::UnitTestBackend::new();
        let init_receipt = Engine::initialize(Box::new(backend.clone()))
            .await
            .expect("engine should initialize");
        let engine = Engine::new(Box::new(backend))
            .await
            .expect("engine should open");
        let session = engine
            .open_session(init_receipt.main_version_id)
            .await
            .expect("session should open");

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('file-nfc', $1, X'41')",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("NFD path insert should canonicalize");

        let error = session
            .execute("SELECT id FROM lix_file WHERE path = id", &[])
            .await
            .expect_err("computed path predicate values should be rejected");
        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("filesystem path predicates only support literal path values"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn lix_directory_path_predicates_canonicalize_bound_values_like_writes() {
        let backend = crate::backend::testing::UnitTestBackend::new();
        let init_receipt = Engine::initialize(Box::new(backend.clone()))
            .await
            .expect("engine should initialize");
        let engine = Engine::new(Box::new(backend))
            .await
            .expect("engine should open");
        let session = engine
            .open_session(init_receipt.main_version_id)
            .await
            .expect("session should open");

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('dir-nfc', $1)",
                &[Value::Text("/Cafe\u{301}/".to_string())],
            )
            .await
            .expect("NFD directory path insert should canonicalize");

        let result = session
            .execute(
                "SELECT id FROM lix_directory WHERE path IN ($1)",
                &[Value::Text("/%43afe%CC%81/".to_string())],
            )
            .await
            .expect("directory path predicate should canonicalize");
        assert_eq!(
            rows_from_execute_result(result).1,
            vec![vec![Value::Text("dir-nfc".to_string())]]
        );

        let or_result = session
            .execute(
                "SELECT id FROM lix_directory WHERE id = 'missing' OR path = $1",
                &[Value::Text("/Cafe\u{301}/".to_string())],
            )
            .await
            .expect("directory OR path predicate should canonicalize");
        assert_eq!(
            rows_from_execute_result(or_result).1,
            vec![vec![Value::Text("dir-nfc".to_string())]]
        );

        let not_in_result = session
            .execute(
                "SELECT id FROM lix_directory WHERE path NOT IN ($1)",
                &[Value::Text("/%43afe%CC%81/".to_string())],
            )
            .await
            .expect("directory NOT IN path predicate should canonicalize");
        assert!(rows_from_execute_result(not_in_result).1.is_empty());
    }

    #[tokio::test]
    async fn lix_directory_path_predicates_reject_non_literal_path_values() {
        let backend = crate::backend::testing::UnitTestBackend::new();
        let init_receipt = Engine::initialize(Box::new(backend.clone()))
            .await
            .expect("engine should initialize");
        let engine = Engine::new(Box::new(backend))
            .await
            .expect("engine should open");
        let session = engine
            .open_session(init_receipt.main_version_id)
            .await
            .expect("session should open");

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('dir-nfc', $1)",
                &[Value::Text("/Cafe\u{301}/".to_string())],
            )
            .await
            .expect("NFD directory path insert should canonicalize");

        let error = session
            .execute("SELECT id FROM lix_directory WHERE path IN (id)", &[])
            .await
            .expect_err("computed directory path predicate values should be rejected");
        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("filesystem path predicates only support literal path values"),
            "{error:?}"
        );
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
        let (session, head_commit_id) = setup_engine_history_fixture()
            .await
            .expect("history fixture should initialize");
        let result = session
            .execute(
                &format!(
                    "SELECT entity_id, snapshot_content, metadata, depth, start_commit_id \
	             FROM lix_state_history \
	             WHERE schema_key = 'test_state_schema' \
	               AND entity_id = lix_json('[\"entity-history\"]') \
	               AND start_commit_id = '{head_commit_id}' \
	               AND depth >= 0"
                ),
                &[],
            )
            .await
            .expect("sql2 execute should read lix_state_history through real engine context");
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
        assert_eq!(rows[0][0], Value::Json(json!(["entity-history"])));
        assert_eq!(rows[0][1], Value::Json(json!({"count": 7, "value": "A"})));
        assert_eq!(rows[0][2], Value::Json(json!({"source": "history"})));
        assert!(matches!(rows[0][3], Value::Integer(_)));
        assert_eq!(rows[0][4], Value::Text(head_commit_id.clone()));
    }

    #[tokio::test]
    async fn execute_sql_reads_entity_history_view_from_history_context() {
        let (session, head_commit_id) = setup_engine_history_fixture()
            .await
            .expect("history fixture should initialize");
        let result = session
            .execute(
                &format!(
                    "SELECT value, count, lixcol_entity_id, lixcol_start_commit_id, lixcol_depth \
	             FROM test_state_schema_history \
	             WHERE lixcol_start_commit_id = '{head_commit_id}' \
	               AND lixcol_entity_id = lix_json('[\"entity-history\"]')"
                ),
                &[],
            )
            .await
            .expect("sql2 execute should read entity history through real engine context");
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
        assert_eq!(rows[0][2], Value::Json(json!(["entity-history"])));
        assert_eq!(rows[0][3], Value::Text(head_commit_id));
        assert!(matches!(rows[0][4], Value::Integer(_)));
    }

    #[tokio::test]
    async fn execute_sql_reads_directory_history_view_from_history_context() {
        let (session, head_commit_id) = setup_engine_history_fixture()
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
            .expect("sql2 execute should read directory history through real engine context");
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
        let (session, head_commit_id) = setup_engine_history_fixture()
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
            .expect("sql2 execute should read file history through real engine context");
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
	         entity_id, schema_key, file_id, snapshot_content, metadata, global, untracked\
	         ) VALUES (\
	         lix_json('[\"entity-1\"]'), 'lix_key_value', NULL, '{\"key\":\"hello\",\"value\":\"world\"}', '{\"source\":\"sql\"}', false, false\
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
        assert_eq!(rows[0].entity_id, "[\"entity-1\"]");
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
	         entity_id, schema_key, file_id, snapshot_content, metadata\
	         ) VALUES (\
	         lix_json('[\"entity-defaults\"]'), 'lix_key_value', NULL, '{\"key\":\"hello\",\"value\":\"defaults\"}', NULL\
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
        assert_eq!(rows[0].entity_id, "[\"entity-defaults\"]");
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
	         entity_id, schema_key, file_id, snapshot_content, metadata, global, untracked\
	         ) \
	         SELECT \
	         lix_json('[\"entity-from-select\"]') AS entity_id, \
	         'lix_key_value' AS schema_key, \
	         NULL AS file_id, \
             '{\"key\":\"hello\",\"value\":\"from-select\"}' AS snapshot_content, \
             '{\"source\":\"select\"}' AS metadata, \
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
        assert_eq!(rows[0].entity_id, "[\"entity-from-select\"]");
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
	     ) VALUES (lix_json('[\"entity-c\"]'), 'version-b', 'C')",
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
        assert_eq!(rows[0].entity_id, "[\"entity-c\"]");
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
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO test_state_schema (lixcol_entity_id, value) \
	     VALUES (lix_json('[\"entity-c\"]'), 'C')",
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
        assert_eq!(rows[0].entity_id, "[\"entity-c\"]");
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
        assert_eq!(rows[0].entity_id, "[\"dir-docs\"]");
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
        assert_eq!(rows[0].entity_id, "[\"dir-docs\"]");
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
        assert_eq!(rows[0].entity_id, "[\"dir-docs\"]");
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
        assert_eq!(rows[0].entity_id, "[\"dir-guides\"]");
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
             id, directory_id, name, hidden, lixcol_version_id\
             ) VALUES ('file-readme', 'dir-docs', 'readme.md', false, 'version-b')",
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
        assert_eq!(rows[0].entity_id, "[\"file-readme\"]");
        assert_eq!(rows[0].version_id, "version-b");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        let snapshot: JsonValue =
            serde_json::from_str(rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "readme.md");
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
            "INSERT INTO lix_file (id, directory_id, name, hidden) \
             VALUES ('file-readme', 'dir-docs', 'readme.md', false)",
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
        assert_eq!(rows[0].entity_id, "[\"file-readme\"]");
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
             id, directory_id, name, hidden, data, lixcol_version_id\
             ) VALUES ('file-readme', 'dir-docs', 'readme.md', false, X'4142', 'version-b')",
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
        assert_eq!(descriptor_rows[0].entity_id, "[\"file-readme\"]");
        let blob_ref_rows = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_ref_rows.len(), 1);
        assert_eq!(blob_ref_rows[0].entity_id, "[\"file-readme\"]");
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
                live_directory_row("dir-docs", "version-a", None, "docs", false),
                live_file_row(
                    "file-readme",
                    "version-a",
                    Some("dir-docs"),
                    "readme.md",
                    false,
                ),
                live_file_row(
                    "file-guide",
                    "version-a",
                    Some("dir-docs"),
                    "guide.md",
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
             SET name = 'readme-updated.txt', hidden = true, lixcol_metadata = '{\"source\":\"file-update\"}' \
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
        assert_eq!(rows[0].entity_id, "[\"file-readme\"]");
        assert_eq!(rows[0].version_id, "version-a");
        let snapshot: JsonValue =
            serde_json::from_str(rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "readme-updated.txt");
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
            rows: vec![
                live_directory_row("dir-docs", "version-a", None, "docs", false),
                live_file_row(
                    "file-readme",
                    "version-a",
                    Some("dir-docs"),
                    "readme.md",
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
        assert_eq!(blob_ref_rows[0].entity_id, "[\"file-readme\"]");
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
                    "readme.md",
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
        assert_eq!(snapshot["name"], "renamed.md");
    }

    #[tokio::test]
    async fn execute_sql_delete_file_by_version_stages_descriptor_tombstone() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_directory_row("dir-docs", "version-a", None, "docs", false),
                live_directory_row("dir-docs", "version-b", None, "docs", false),
                live_file_row(
                    "file-readme",
                    "version-a",
                    Some("dir-docs"),
                    "readme.md",
                    false,
                ),
                live_file_row(
                    "file-guide",
                    "version-b",
                    Some("dir-docs"),
                    "guide.md",
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
        assert_eq!(rows[0].entity_id, "[\"file-guide\"]");
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
        assert_eq!(rows[0].entity_id, "[\"entity-a\"]");
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
        assert_eq!(rows[0].entity_id, "[\"entity-b\"]");
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
                 metadata = '{\"schema_key\":\"lix_key_value\"}' \
             WHERE metadata = lix_json('{\"source\":\"match\"}')",
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
        assert_eq!(rows[0].entity_id, "[\"entity-1\"]");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"updated\"}")
        );
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"schema_key\":\"lix_key_value\"}")
        );
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
        assert!(rows.iter().any(|row| row.entity_id == "[\"entity-1\"]"));
        assert!(rows.iter().any(|row| row.entity_id == "[\"entity-2\"]"));
    }

    struct BackendSqlExecutionContext<'a> {
        active_version_id: &'a str,
        storage: StorageContext,
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

        fn commit_store_query_source(&self) -> SqlCommitStoreQuerySource {
            let base_scope = test_read_scope(self.storage.clone());
            let read_scope = StorageReadScope::new(base_scope.store());
            CommitStoreQuerySource {
                commit_store_reader: Arc::new(CommitStoreContext::new().reader(read_scope.store())),
                json_reader: JsonStoreContext::new().reader(read_scope.store()),
            }
        }

        fn commit_graph(&self) -> Box<dyn CommitGraphReader> {
            Box::new(DummyCommitGraphReader)
        }

        fn version_ref(&self) -> Arc<dyn VersionRefReader> {
            Arc::new(
                crate::version::VersionContext::new(Arc::new(UntrackedStateContext::new()))
                    .ref_reader(self.storage.clone()),
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
        let storage = crate::storage::StorageContext::new(std::sync::Arc::new(backend.clone()));
        {
            let mut transaction = storage.begin_write_transaction().await?;
            let version_ctx = crate::version::VersionContext::new(Arc::new(
                crate::untracked_state::UntrackedStateContext::new(),
            ));
            let mut writes = StorageWriteSet::new();
            let canonical_rows = vec![
                prepare_version_ref_row(
                    "version-a",
                    &init_receipt.initial_commit_id,
                    "1970-01-01T00:00:00.000Z",
                )?,
                prepare_version_ref_row(
                    "version-b",
                    &init_receipt.initial_commit_id,
                    "1970-01-01T00:00:00.000Z",
                )?,
            ];
            JsonStoreContext::new().writer().stage_batch(
                &mut writes,
                crate::json_store::JsonWritePlacementRef::Direct,
                canonical_rows
                    .iter()
                    .map(|row| crate::json_store::NormalizedJsonRef {
                        normalized: row.snapshot.as_str(),
                    }),
            )?;
            let rows = canonical_rows
                .into_iter()
                .map(|prepared| prepared.row)
                .collect::<Vec<_>>();
            version_ctx.stage_canonical_ref_rows(&mut writes, &rows)?;
            writes.apply(&mut transaction.as_mut()).await?;
            transaction.commit().await?;
        }
        let engine = Engine::new(Box::new(backend.clone())).await?;
        let session_a = engine.open_session("version-a").await?;
        let session_b = engine.open_session("version-b").await?;
        let schema_definition = json!({
            "x-lix-key": "test_state_schema",
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
                 lix_json('{\"x-lix-key\":\"test_state_schema\",\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"}},\"required\":[\"value\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await?;
        session_b
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"test_state_schema\",\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"}},\"required\":[\"value\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await?;
        session_a
            .execute(
                "INSERT INTO lix_state (\
	         entity_id, schema_key, file_id, snapshot_content, global, untracked\
	         ) VALUES (\
	         lix_json('[\"entity-a\"]'), 'test_state_schema', NULL, '{\"value\":\"A\"}', false, false\
	         )",
                &[],
            )
            .await?;
        session_b
            .execute(
                "INSERT INTO lix_state (\
	         entity_id, schema_key, file_id, snapshot_content, global, untracked\
	         ) VALUES (\
	         lix_json('[\"entity-b\"]'), 'test_state_schema', NULL, '{\"value\":\"B\"}', false, false\
	         )",
                &[],
            )
            .await?;
        session_a
		.execute(
			"INSERT INTO lix_state (\
	         entity_id, schema_key, file_id, snapshot_content, global, untracked\
	         ) VALUES (\
	         lix_json('[\"dir-docs\"]'), 'lix_directory_descriptor', NULL, '{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\",\"hidden\":false}', false, false\
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
            crate::commit_graph::CommitGraphContext::new(),
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
                let storage = StorageContext::new(Arc::clone(&backend_ref));
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(storage.clone()));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    storage: storage.clone(),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(test_live_state_context().reader(storage.clone())),
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
                assert_eq!(result.rows[0][0], Value::Json(json!(["entity-b"])));
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
                let storage = StorageContext::new(Arc::clone(&backend_ref));
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(storage.clone()));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    storage: storage.clone(),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(test_live_state_context().reader(storage.clone())),
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
					result.rows.iter().any(|row| row[0] == Value::Json(json!(["entity-a"])))
						&& result.rows.iter().any(|row| row[0] == Value::Json(json!(["entity-b"]))),
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
                let storage = StorageContext::new(Arc::clone(&backend_ref));
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(storage.clone()));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    storage: storage.clone(),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(test_live_state_context().reader(storage.clone())),
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
                assert_eq!(result.rows[0][0], Value::Json(json!(["entity-a"])));
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
                let storage = StorageContext::new(Arc::clone(&backend_ref));
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(storage.clone()));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    storage: storage.clone(),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(test_live_state_context().reader(storage.clone())),
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
                assert_eq!(result.rows[0][1], Value::Json(json!(["entity-a"])));
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
                let storage = StorageContext::new(Arc::clone(&backend_ref));
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(storage.clone()));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    storage: storage.clone(),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(test_live_state_context().reader(storage.clone())),
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
                let storage = StorageContext::new(Arc::clone(&backend_ref));
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(storage.clone()));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    storage: storage.clone(),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(test_live_state_context().reader(storage.clone())),
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
                let storage = StorageContext::new(Arc::clone(&backend_ref));
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(storage.clone()));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    storage: storage.clone(),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(test_live_state_context().reader(storage.clone())),
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
                let storage = StorageContext::new(Arc::clone(&backend_ref));
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(storage.clone()));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    storage: storage.clone(),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(test_live_state_context().reader(storage.clone())),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name, data, lixcol_version_id \
                     FROM lix_file_by_version \
                     WHERE id = 'file-a' AND lixcol_version_id = 'version-a'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_file_by_version");

                assert_eq!(
                    result.columns,
                    vec!["path", "name", "data", "lixcol_version_id"]
                );
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0][0],
                    Value::Text("/docs/readme.md".to_string())
                );
                assert_eq!(result.rows[0][1], Value::Text("readme.md".to_string()));
                assert_eq!(result.rows[0][2], Value::Blob(vec![0x41, 0x42]));
                assert_eq!(result.rows[0][3], Value::Text("version-a".to_string()));
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
                let storage = StorageContext::new(Arc::clone(&backend_ref));
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(storage.clone()));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    storage: storage.clone(),
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(test_live_state_context().reader(storage.clone())),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name, data \
                     FROM lix_file \
                     WHERE id = 'file-a'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_file");

                assert_eq!(result.columns, vec!["path", "name", "data"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0][0],
                    Value::Text("/docs/readme.md".to_string())
                );
                assert_eq!(result.rows[0][1], Value::Text("readme.md".to_string()));
                assert_eq!(result.rows[0][2], Value::Blob(vec![0x41, 0x42]));
            })
        });
    }
}
