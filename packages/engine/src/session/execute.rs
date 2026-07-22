use std::collections::BTreeSet;
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::{Arc, OnceLock};

use crate::branch::BranchRefReader;
use crate::functions::{FunctionContext, FunctionProviderHandle};
use crate::sql_telemetry::{SqlStatementTelemetry, finish_operation, start_batch};
use crate::sql2;
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    SharedStorageAdapterRead, StorageAdapter, StorageAdapterReadScope, StorageReadOptions,
    StorageWriteOptions, StorageWriteSet,
};
use crate::telemetry::TelemetrySpanKind;
use crate::transaction::{begin_commit_boundary, commit_at_boundary};
use crate::{LixError, LixNotice, SqlQueryResult, Value};
use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, LimitClause, Select, SelectFlavor, SelectItem, SetExpr,
    Statement as SqlStatement, TableFactor, Value as SqlValue, Visit, Visitor,
};
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::context::{SessionContext, SessionSqlExecutionContext, SessionWriteAccess};
use super::transaction::SessionTransaction;

/// Result of executing one SQL statement through engine.
///
/// Column names live once at the result-set level. Individual rows only own
/// values, which keeps the public API row-oriented without copying schema
/// metadata into every row. Result storage is immutable and reference counted
/// so observation fanout does not copy large blob values per subscriber.
#[derive(Debug, Clone)]
pub struct ExecuteResult {
    backing: Arc<ExecuteResultBacking>,
    rows_affected: u64,
}

#[derive(Debug)]
struct ExecuteResultBacking {
    columns: Arc<[String]>,
    rows: Vec<Row>,
    notices: Vec<LixNotice>,
    // Observe evaluations can be shared across sessions. Carry the exact
    // rendered plugin state with the rows so each receiving session can
    // acknowledge it only when `ObserveEvents::next()` delivers the event.
    file_view_mutations: Vec<sql2::SessionFileViewMutation>,
}

impl PartialEq for ExecuteResult {
    fn eq(&self, other: &Self) -> bool {
        self.rows_affected == other.rows_affected
            && (Arc::ptr_eq(&self.backing, &other.backing)
                || (self.backing.columns == other.backing.columns
                    && self.backing.rows == other.backing.rows
                    && self.backing.notices == other.backing.notices))
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct CoherentReadBatch {
    pub active_branch_id: String,
    pub active_branch_commit_id: String,
    pub storage_mutation_revision: Option<Vec<u8>>,
    pub results: Vec<ExecuteResult>,
}

impl ExecuteResult {
    fn from_sql_query_result(result: SqlQueryResult) -> Self {
        Self::from_query_parts(result.columns, result.rows, 0, result.notices)
    }

    fn from_sql_write_result(result: sql2::SqlWriteResult) -> Self {
        let sql2::SqlWriteResult {
            rows_affected,
            returning,
        } = result;
        match returning {
            Some(result) => {
                Self::from_query_parts(result.columns, result.rows, rows_affected, result.notices)
            }
            None => Self::from_rows_affected(rows_affected),
        }
    }

    pub fn from_rows_affected(rows_affected: u64) -> Self {
        Self {
            backing: empty_execute_result_backing(),
            rows_affected,
        }
    }

    pub fn from_rows(columns: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        Self::from_query_parts(columns, rows, 0, Vec::new())
    }

    fn from_query_parts(
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
        rows_affected: u64,
        notices: Vec<LixNotice>,
    ) -> Self {
        let columns: Arc<[String]> = columns.into();
        let rows = rows
            .into_iter()
            .map(|values| Row {
                columns: Arc::clone(&columns),
                values,
            })
            .collect();
        Self {
            backing: Arc::new(ExecuteResultBacking {
                columns,
                rows,
                notices,
                file_view_mutations: Vec::new(),
            }),
            rows_affected,
        }
    }

    fn with_file_view_mutations(mut self, mutations: Vec<sql2::SessionFileViewMutation>) -> Self {
        Arc::get_mut(&mut self.backing)
            .expect("fresh execute result backing must be uniquely owned")
            .file_view_mutations = mutations;
        self
    }

    pub(crate) fn file_view_mutations(&self) -> &[sql2::SessionFileViewMutation] {
        &self.backing.file_view_mutations
    }

    /// Returns the result-set column names in row value order.
    pub fn columns(&self) -> &[String] {
        self.backing.columns.as_ref()
    }

    /// Returns the owned rows. Use `iter()` for name-based access.
    pub fn rows(&self) -> &[Row] {
        &self.backing.rows
    }

    /// Iterates rows with borrowed access to the shared column metadata.
    pub fn iter(&self) -> impl Iterator<Item = RowRef<'_>> {
        self.backing.rows.iter().map(|row| RowRef {
            columns: self.backing.columns.as_ref(),
            values: row.values.as_slice(),
        })
    }

    /// Returns the number of rows in this result set.
    pub fn len(&self) -> usize {
        self.backing.rows.len()
    }

    /// Returns true when this result set has no rows.
    pub fn is_empty(&self) -> bool {
        self.backing.rows.is_empty()
    }

    /// Returns the number of rows affected by a mutation statement.
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    /// Returns non-fatal diagnostics produced while executing the statement.
    pub fn notices(&self) -> &[LixNotice] {
        &self.backing.notices
    }

    /// Looks up the value for `column_name` on an owned row from this set.
    pub fn get<'a>(&self, row: &'a Row, column_name: &str) -> Option<&'a Value> {
        let index = self.column_index(column_name)?;
        row.get_index(index)
    }

    /// Returns the index for a column name.
    pub fn column_index(&self, column_name: &str) -> Option<usize> {
        self.backing
            .columns
            .iter()
            .position(|column| column == column_name)
    }
}

fn empty_execute_result_backing() -> Arc<ExecuteResultBacking> {
    static EMPTY: OnceLock<Arc<ExecuteResultBacking>> = OnceLock::new();
    Arc::clone(EMPTY.get_or_init(|| {
        Arc::new(ExecuteResultBacking {
            columns: Vec::new().into(),
            rows: Vec::new(),
            notices: Vec::new(),
            file_view_mutations: Vec::new(),
        })
    }))
}

/// One owned row returned by a query.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    columns: Arc<[String]>,
    values: Vec<Value>,
}

impl Row {
    /// Returns the values in result-set column order.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Returns the value at `index`.
    pub fn get_index(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Returns the raw value for `column_name`, or an error when the column is absent.
    pub fn value(&self, column_name: &str) -> Result<&Value, LixError> {
        let index = self.column_index(column_name)?;
        self.values.get(index).ok_or_else(|| {
            LixError::new(
                LixError::CODE_COLUMN_NOT_FOUND,
                format!(
                    "column '{}' points past row width {}; available columns: {}",
                    column_name,
                    self.values.len(),
                    self.available_columns()
                ),
            )
        })
    }

    /// Converts the named column to a native Rust value.
    pub fn get<T>(&self, column_name: &str) -> Result<T, LixError>
    where
        T: TryFromValue,
    {
        T::try_from_value(self.value(column_name)?)
    }

    fn column_index(&self, column_name: &str) -> Result<usize, LixError> {
        self.columns
            .iter()
            .position(|column| column == column_name)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_COLUMN_NOT_FOUND,
                    format!(
                        "column '{}' does not exist; available columns: {}",
                        column_name,
                        self.available_columns()
                    ),
                )
            })
    }

    fn available_columns(&self) -> String {
        if self.columns.is_empty() {
            "<none>".to_string()
        } else {
            self.columns.join(", ")
        }
    }
}

pub trait TryFromValue: Sized {
    fn try_from_value(value: &Value) -> Result<Self, LixError>;
}

impl TryFromValue for Value {
    fn try_from_value(value: &Value) -> Result<Self, LixError> {
        Ok(value.clone())
    }
}

impl TryFromValue for String {
    fn try_from_value(value: &Value) -> Result<Self, LixError> {
        match value {
            Value::Text(value) => Ok(value.clone()),
            other => Err(value_type_error("text", other)),
        }
    }
}

impl TryFromValue for bool {
    fn try_from_value(value: &Value) -> Result<Self, LixError> {
        match value {
            Value::Boolean(value) => Ok(*value),
            other => Err(value_type_error("boolean", other)),
        }
    }
}

impl TryFromValue for i64 {
    fn try_from_value(value: &Value) -> Result<Self, LixError> {
        match value {
            Value::Integer(value) => Ok(*value),
            other => Err(value_type_error("integer", other)),
        }
    }
}

impl TryFromValue for f64 {
    fn try_from_value(value: &Value) -> Result<Self, LixError> {
        match value {
            Value::Real(value) => Ok(*value),
            other => Err(value_type_error("real", other)),
        }
    }
}

impl TryFromValue for serde_json::Value {
    fn try_from_value(value: &Value) -> Result<Self, LixError> {
        match value {
            Value::Json(value) => Ok(value.clone()),
            other => Err(value_type_error("json", other)),
        }
    }
}

impl TryFromValue for Vec<u8> {
    fn try_from_value(value: &Value) -> Result<Self, LixError> {
        match value {
            Value::Blob(value) => Ok(value.clone()),
            other => Err(value_type_error("blob", other)),
        }
    }
}

fn value_type_error(expected: &str, actual: &Value) -> LixError {
    LixError::new(
        "LIX_ERROR_VALUE_TYPE",
        format!("expected {expected} value, got {actual:?}"),
    )
}

/// Zero-copy row view with access to the result-set column names.
///
/// This is the ergonomic path for callers that want `row.get("column")`
/// without storing column metadata on every owned row.
#[derive(Debug, Clone, Copy)]
pub struct RowRef<'a> {
    columns: &'a [String],
    values: &'a [Value],
}

impl RowRef<'_> {
    /// Returns the result-set column names in row value order.
    pub fn columns(&self) -> &[String] {
        self.columns
    }

    /// Returns the row values in result-set column order.
    pub fn values(&self) -> &[Value] {
        self.values
    }

    /// Returns the value for `column_name`.
    pub fn get(&self, column_name: &str) -> Option<&Value> {
        let index = self
            .columns
            .iter()
            .position(|column| column == column_name)?;
        self.values.get(index)
    }

    /// Returns the value at `index`.
    pub fn get_index(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecuteOptions {
    pub origin_key: Option<String>,
}

/// One SQL statement to execute as part of an atomic batch.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecuteBatchStatement {
    pub sql: String,
    pub params: Vec<Value>,
}

enum ExecuteBatchExecution {
    ReadOnly(Vec<datafusion::sql::parser::Statement>),
    Transaction(Vec<datafusion::sql::parser::Statement>),
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Executes one DataFusion SQL statement against this Lix session.
    ///
    /// The SQL dialect is DataFusion SQL, not SQLite SQL. Positional
    /// placeholders use `?` or `$1`, `$2`, and so on. SQLite-specific catalog tables
    /// and transaction statements such as `sqlite_master`, `BEGIN`, and
    /// `COMMIT` are not part of this contract; use `information_schema` for
    /// catalog inspection. Lix owns transaction boundaries for each statement.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.execute_with_options(sql, params, ExecuteOptions::default())
            .await
    }

    pub async fn execute_with_options(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        self.execute_with_kind(sql, params, options, "execute")
            .await
    }

    pub(crate) async fn execute_for_observe(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        self.execute_with_kind(sql, params, ExecuteOptions::default(), "observe")
            .await
    }

    async fn execute_with_kind(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
        execution_kind: &'static str,
    ) -> Result<ExecuteResult, LixError> {
        let telemetry =
            SqlStatementTelemetry::start(self.telemetry.as_ref(), sql, execution_kind, None);
        let operation =
            self.execute_with_options_inner(sql, params, options, execution_kind == "observe");
        let result = match telemetry.as_ref() {
            Some(telemetry) => telemetry.instrument(operation).await,
            None => operation.await,
        };
        if let Some(telemetry) = telemetry {
            telemetry.finish(&result);
        }
        result
    }

    async fn execute_with_options_inner(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
        defer_file_view_acknowledgement: bool,
    ) -> Result<ExecuteResult, LixError> {
        self.ensure_open()?;
        let statement = self.sql_planning_cache.parse_statement(sql)?;
        if sql2::bind_statement_route(&statement)? == sql2::BoundStatementRoute::Write {
            let write_access = self.begin_session_write_access().await?;
            let sql_for_error = sql.to_string();
            let sql_for_planning = sql_for_error.clone();
            let params = params.to_vec();
            return self
                .with_write_transaction_reserved(write_access, |transaction| {
                    Box::pin(async move {
                        let previous_origin_key =
                            transaction.replace_origin_key(options.origin_key);
                        let result = async {
                            let tx_plan = transaction
                                .prepare_sql_write_logical_plan(&sql_for_planning, &statement)?;
                            let result = sql2::execute_write_logical_plan_result(
                                transaction,
                                tx_plan,
                                &params,
                            )
                            .await?;
                            Ok(ExecuteResult::from_sql_write_result(result))
                        }
                        .await;
                        transaction.replace_origin_key(previous_origin_key);
                        result
                    })
                })
                .await
                .map_err(|error| normalize_sql_surface_error(error, &sql_for_error));
        }

        let acknowledge_file_views = is_acknowledgeable_file_data_read(&statement, params);
        let exact_lix_file_read = exact_lix_file_read(&statement, params);
        let has_durable_runtime_function = sql2::statement_has_durable_runtime_function(&statement);
        let runtime_write_access = if has_durable_runtime_function {
            let write_access = self.begin_session_write_access().await?;
            Some(write_access)
        } else {
            None
        };
        let _operation_guard = if runtime_write_access.is_some() {
            None
        } else {
            Some(self.begin_waitable_session_operation().await?)
        };
        // Lock by statement shape, not by a pre-lock mode read. The read
        // snapshot below is where FunctionContext observes deterministic mode;
        // checking mode before this point can race with another session
        // enabling deterministic mode.
        let _deterministic_runtime_guard = if has_durable_runtime_function {
            Some(self.lock_deterministic_runtime().await)
        } else {
            None
        };
        let read_scope = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await?;
        let read_result = with_static_session_sql_read::<StorageImpl, _, _, _>(
            read_scope,
            |read_store| async move {
                self.execute_read_statement_with_store(
                    read_store,
                    sql,
                    statement,
                    params,
                    acknowledge_file_views,
                    exact_lix_file_read,
                    has_durable_runtime_function,
                )
                .await
            },
        );
        let (read_result, file_view_mutations) = match read_result.await {
            Ok(result) => result,
            Err(error) => {
                return Err(normalize_sql_surface_error(error, sql));
            }
        };
        let runtime_storage_stats = match read_result.runtime_functions.as_ref() {
            Some(runtime_functions) => {
                self.persist_runtime_functions_if_needed(
                    runtime_functions,
                    runtime_write_access.as_ref(),
                )
                .await?
            }
            None => None,
        };
        drop(runtime_write_access);
        if let Some(stats) = runtime_storage_stats {
            self.observe_invalidation.bump_if_storage_changed(&stats);
        }
        let result = ExecuteResult::from_sql_query_result(read_result.query)
            .with_file_view_mutations(file_view_mutations);
        if !defer_file_view_acknowledgement {
            self.file_views
                .apply_mutations(result.file_view_mutations().iter().cloned());
        }
        Ok(result)
    }

    /// Executes SQL statements sequentially against one atomic snapshot.
    ///
    /// Pure-read batches share one immutable read snapshot and prepared SQL
    /// session. Batches containing writes or durable runtime functions use a
    /// write transaction, so reads can observe earlier staged writes and the
    /// transaction commits only after every statement succeeds.
    pub async fn execute_batch(
        &self,
        statements: &[ExecuteBatchStatement],
    ) -> Result<Vec<ExecuteResult>, LixError> {
        self.execute_batch_with_options(statements, ExecuteOptions::default())
            .await
    }

    pub async fn execute_batch_with_options(
        &self,
        statements: &[ExecuteBatchStatement],
        options: ExecuteOptions,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        let telemetry = start_batch(
            self.telemetry.as_ref(),
            TelemetrySpanKind::SqlBatch,
            statements.len(),
        );
        let operation = self.execute_batch_with_options_inner(statements, options);
        let result = match telemetry.as_ref() {
            Some(telemetry) => telemetry.instrument(operation).await,
            None => operation.await,
        };
        if let Some(telemetry) = telemetry {
            finish_operation(telemetry, &result);
        }
        result
    }

    async fn execute_batch_with_options_inner(
        &self,
        statements: &[ExecuteBatchStatement],
        options: ExecuteOptions,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        self.ensure_open()?;
        if statements.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "execute_batch requires at least one statement",
            )
            .with_details(serde_json::json!({
                "operation": "executeBatch",
                "argument": "statements",
                "expected": "non-empty array",
            })));
        }

        let statements = statements.to_vec();
        match classify_execute_batch(&statements, &self.sql_planning_cache)? {
            ExecuteBatchExecution::ReadOnly(parsed) => {
                self.execute_read_only_batch(&statements, parsed).await
            }
            ExecuteBatchExecution::Transaction(parsed) => {
                let telemetry_sink = self.telemetry.clone();
                self.with_write_transaction(move |transaction| {
                    Box::pin(async move {
                        let mut results = Vec::with_capacity(statements.len());
                        for (statement_index, (statement, parsed)) in
                            statements.iter().zip(parsed).enumerate()
                        {
                            let telemetry = SqlStatementTelemetry::start(
                                telemetry_sink.as_ref(),
                                &statement.sql,
                                "batch",
                                Some(statement_index),
                            );
                            let operation = async {
                                execute_transaction_statement(
                                    transaction,
                                    &statement.sql,
                                    parsed,
                                    &statement.params,
                                    options.clone(),
                                )
                                .await
                                .map_err(|error| {
                                    with_batch_statement_index(
                                        normalize_sql_surface_error(error, &statement.sql),
                                        statement_index,
                                    )
                                })
                            };
                            let result = match telemetry.as_ref() {
                                Some(telemetry) => telemetry.instrument(operation).await,
                                None => operation.await,
                            };
                            if let Some(telemetry) = telemetry {
                                telemetry.finish(&result);
                            }
                            results.push(result?);
                        }
                        Ok(results)
                    })
                })
                .await
            }
        }
    }

    async fn execute_read_only_batch(
        &self,
        statements: &[ExecuteBatchStatement],
        parsed: Vec<datafusion::sql::parser::Statement>,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        let acknowledge_file_views = parsed.iter().zip(statements).all(|(parsed, statement)| {
            is_acknowledgeable_file_data_read(parsed, &statement.params)
        });
        let _operation_guard = self.begin_waitable_session_operation().await?;
        let read_scope = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await?;
        let (results, file_view_mutations) = with_static_session_sql_read::<StorageImpl, _, _, _>(
            read_scope,
            |read_store| async move {
                let file_view_collector =
                    acknowledge_file_views.then(sql2::SessionFileViews::default);
                let active_branch_id = self.active_branch_id_from_reader(&read_store).await?;
                let ctx = SessionSqlExecutionContext {
                    active_branch_id: &active_branch_id,
                    read_store,
                    live_state: Arc::clone(&self.live_state),
                    binary_cas: Arc::clone(&self.binary_cas),
                    branch_ctx: Arc::clone(&self.branch_ctx),
                    catalog_context: Arc::clone(&self.catalog_context),
                    functions: FunctionProviderHandle::system(),
                    plugin_host: self.plugin_host.clone(),
                    file_views: file_view_collector.clone(),
                };
                let read_session = sql2::prepare_read_session(&ctx, &parsed).await?;
                let mut results = Vec::with_capacity(statements.len());
                for (statement_index, (statement, parsed)) in
                    statements.iter().zip(parsed).enumerate()
                {
                    let telemetry = SqlStatementTelemetry::start(
                        self.telemetry.as_ref(),
                        &statement.sql,
                        "batch",
                        Some(statement_index),
                    );
                    let operation = async {
                        sql2::execute_read_statement_in_session_from_parsed(
                            &read_session,
                            &statement.sql,
                            parsed,
                            &statement.params,
                        )
                        .await
                        .map(ExecuteResult::from_sql_query_result)
                        .map_err(|error| {
                            with_batch_statement_index(
                                normalize_sql_surface_error(error, &statement.sql),
                                statement_index,
                            )
                        })
                    };
                    let result = match telemetry.as_ref() {
                        Some(telemetry) => telemetry.instrument(operation).await,
                        None => operation.await,
                    };
                    if let Some(telemetry) = telemetry {
                        telemetry.finish(&result);
                    }
                    results.push(result?);
                }
                drop(read_session);
                drop(ctx);
                let file_view_mutations = file_view_collector
                    .map(|collector| collector.plugin_file_mutations())
                    .unwrap_or_default();
                Ok((results, file_view_mutations))
            },
        )
        .await?;
        self.file_views.apply_mutations(file_view_mutations);
        Ok(results)
    }

    #[doc(hidden)]
    pub async fn execute_coherent_read_batch(
        &self,
        statements: &[(&str, &[Value])],
    ) -> Result<CoherentReadBatch, LixError> {
        let telemetry = start_batch(
            self.telemetry.as_ref(),
            TelemetrySpanKind::SqlCoherentReadBatch,
            statements.len(),
        );
        let operation = self.execute_coherent_read_batch_inner(statements);
        let result = match telemetry.as_ref() {
            Some(telemetry) => telemetry.instrument(operation).await,
            None => operation.await,
        };
        if let Some(telemetry) = telemetry {
            finish_operation(telemetry, &result);
        }
        result
    }

    async fn execute_coherent_read_batch_inner(
        &self,
        statements: &[(&str, &[Value])],
    ) -> Result<CoherentReadBatch, LixError> {
        self.ensure_open()?;
        let parsed = statements
            .iter()
            .map(|(sql, _)| {
                let statement = self.sql_planning_cache.parse_statement(sql)?;
                if sql2::statement_has_durable_runtime_function(&statement) {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "execute_coherent_read_batch does not support durable runtime functions",
                    ));
                }
                match sql2::bind_statement_route(&statement)? {
                    sql2::BoundStatementRoute::Read => Ok(statement),
                    sql2::BoundStatementRoute::Write => Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "execute_coherent_read_batch only accepts read statements",
                    )),
                }
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let acknowledge_file_views = parsed
            .iter()
            .zip(statements)
            .all(|(parsed, (_, params))| is_acknowledgeable_file_data_read(parsed, params));

        let _operation_guard = self.begin_waitable_session_operation().await?;
        let read_scope = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await?;
        let (batch, file_view_mutations) = with_static_session_sql_read::<StorageImpl, _, _, _>(
            read_scope,
            |read_store| async move {
                let file_view_collector =
                    acknowledge_file_views.then(sql2::SessionFileViews::default);
                let active_branch_id = self.active_branch_id_from_reader(&read_store).await?;
                let active_branch_head = self
                    .branch_ctx
                    .ref_reader(read_store.clone())
                    .load_head(&active_branch_id)
                    .await?
                    .ok_or_else(|| {
                        LixError::branch_not_found(
                            active_branch_id.clone(),
                            "execute coherent read batch",
                            "active branch",
                        )
                    })?;
                let active_branch_commit_id = active_branch_head.commit_id.to_string();
                let storage_mutation_revision =
                    StorageAdapter::<StorageImpl>::load_mutation_revision_from_read(&read_store)
                        .await?
                        .map(|revision| revision.to_vec());
                if parsed.is_empty() {
                    return Ok((
                        CoherentReadBatch {
                            active_branch_id,
                            active_branch_commit_id,
                            storage_mutation_revision,
                            results: Vec::new(),
                        },
                        Vec::new(),
                    ));
                }
                let ctx = SessionSqlExecutionContext {
                    active_branch_id: &active_branch_id,
                    read_store,
                    live_state: Arc::clone(&self.live_state),
                    binary_cas: Arc::clone(&self.binary_cas),
                    branch_ctx: Arc::clone(&self.branch_ctx),
                    catalog_context: Arc::clone(&self.catalog_context),
                    functions: FunctionProviderHandle::system(),
                    plugin_host: self.plugin_host.clone(),
                    file_views: file_view_collector.clone(),
                };
                let read_session =
                    sql2::prepare_read_session_at_head(&ctx, active_branch_head, &parsed).await?;
                let mut results = Vec::with_capacity(statements.len());
                for (statement_index, ((sql, params), statement)) in
                    statements.iter().zip(parsed).enumerate()
                {
                    let telemetry = SqlStatementTelemetry::start(
                        self.telemetry.as_ref(),
                        sql,
                        "coherent_read_batch",
                        Some(statement_index),
                    );
                    let operation = async {
                        sql2::execute_read_statement_in_session_from_parsed(
                            &read_session,
                            sql,
                            statement,
                            params,
                        )
                        .await
                        .map(ExecuteResult::from_sql_query_result)
                        .map_err(|error| normalize_sql_surface_error(error, sql))
                    };
                    let result = match telemetry.as_ref() {
                        Some(telemetry) => telemetry.instrument(operation).await,
                        None => operation.await,
                    };
                    if let Some(telemetry) = telemetry {
                        telemetry.finish(&result);
                    }
                    results.push(result?);
                }
                drop(read_session);
                drop(ctx);
                let file_view_mutations = file_view_collector
                    .map(|collector| collector.plugin_file_mutations())
                    .unwrap_or_default();
                Ok((
                    CoherentReadBatch {
                        active_branch_id,
                        active_branch_commit_id,
                        storage_mutation_revision,
                        results,
                    },
                    file_view_mutations,
                ))
            },
        )
        .await?;
        self.file_views.apply_mutations(file_view_mutations);
        Ok(batch)
    }

    #[cfg(test)]
    pub(crate) async fn execute_with_write_executor_mode(
        &self,
        sql: &str,
        params: &[Value],
        mode: sql2::WriteExecutorMode,
    ) -> Result<ExecuteResult, LixError> {
        self.ensure_open()?;
        let statement = self.sql_planning_cache.parse_statement(sql)?;
        if sql2::bind_statement_route(&statement)? == sql2::BoundStatementRoute::Write {
            let write_access = self.begin_session_write_access().await?;
            let sql_for_error = sql.to_string();
            let sql_for_planning = sql_for_error.clone();
            let params = params.to_vec();
            return self
                .with_write_transaction_reserved(write_access, |transaction| {
                    Box::pin(async move {
                        let tx_plan = transaction
                            .prepare_sql_write_logical_plan(&sql_for_planning, &statement)?;
                        let result = sql2::execute_write_logical_plan_with_mode_result(
                            transaction,
                            tx_plan,
                            &params,
                            mode,
                        )
                        .await?;
                        Ok(ExecuteResult::from_sql_write_result(result))
                    })
                })
                .await
                .map_err(|error| normalize_sql_surface_error(error, &sql_for_error));
        }
        self.execute(sql, params).await
    }

    /// Persists execution-scoped runtime function state after a successful read.
    ///
    /// Reads do not otherwise own a write transaction, but SQL functions such as
    /// `lix_uuid_v7()` can still advance runtime state. Persisting happens only
    /// after successful execution so failed reads do not consume durable
    /// sequence state.
    async fn persist_runtime_functions_if_needed(
        &self,
        runtime_functions: &FunctionContext,
        runtime_write_access: Option<&SessionWriteAccess>,
    ) -> Result<Option<crate::storage_adapter::StorageWriteSetStats>, LixError> {
        let mut writes = StorageWriteSet::new();
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        runtime_functions
            .stage_persist_if_needed(&read, &mut writes)
            .await?;
        if writes.is_empty() {
            return Ok(None);
        }
        if runtime_write_access.is_none() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "runtime function state changed without reserved write access",
            ));
        }
        let commit_boundary = self.transaction_commit_boundary();
        let _commit_guard = begin_commit_boundary(Some(&commit_boundary));
        let prepared_commit = self
            .storage
            .prepare_write_set(writes, StorageWriteOptions::default())
            .await?;
        let stats = commit_at_boundary(Some(&commit_boundary), || async move {
            let (_commit, stats) = prepared_commit.commit().await?;
            Ok(stats)
        })
        .await?;
        Ok(Some(stats))
    }

    async fn execute_read_statement_with_store(
        &self,
        read_store: SharedStorageAdapterRead<StorageImpl::Read<'static>>,
        sql: &str,
        statement: datafusion::sql::parser::Statement,
        params: &[Value],
        acknowledge_file_views: bool,
        exact_lix_file_read: Option<(sql2::ExactLixFileReadSelector, sql2::ExactLixFileReadColumn)>,
        has_durable_runtime_function: bool,
    ) -> Result<
        (
            sql2::SessionReadSqlResult,
            Vec<sql2::SessionFileViewMutation>,
        ),
        LixError,
    > {
        let file_view_collector = acknowledge_file_views.then(sql2::SessionFileViews::default);
        let active_branch_id = self.active_branch_id_from_reader(&read_store).await?;
        if let Some((selector, column)) = exact_lix_file_read {
            let live_state: Arc<dyn crate::live_state::LiveStateReader> =
                Arc::new(self.live_state.reader(read_store.clone()));
            let filesystem_path_index: Arc<dyn crate::filesystem::FilesystemPathIndexReader> =
                Arc::new(self.live_state.reader(read_store.clone()));
            let branch_ref: Arc<dyn BranchRefReader> =
                Arc::new(self.branch_ctx.ref_reader(read_store.clone()));
            let blob_reader: Arc<dyn crate::binary_cas::BlobDataReader> =
                Arc::new(self.binary_cas.reader(read_store));
            let query = sql2::execute_exact_lix_file_read(
                &active_branch_id,
                live_state,
                filesystem_path_index,
                branch_ref,
                blob_reader,
                self.plugin_host.clone(),
                file_view_collector.clone(),
                &selector,
                column,
            )
            .await?;
            let file_view_mutations = file_view_collector
                .map(|collector| collector.plugin_file_mutations())
                .unwrap_or_default();
            return Ok((
                sql2::SessionReadSqlResult {
                    runtime_functions: None,
                    query,
                },
                file_view_mutations,
            ));
        }
        let live_state: Arc<dyn crate::live_state::LiveStateReader> =
            Arc::new(self.live_state.reader(read_store.clone()));
        let runtime_functions = if has_durable_runtime_function {
            Some(FunctionContext::prepare(live_state.as_ref()).await?)
        } else {
            None
        };
        // Read providers do not consume durable function state themselves;
        // only the registered timestamp/UUID SQL UDFs do. Keep their AST
        // classifier conservative if new readable statement shapes appear.
        let functions = runtime_functions
            .as_ref()
            .map_or_else(FunctionProviderHandle::system, FunctionContext::provider);
        let ctx = SessionSqlExecutionContext {
            active_branch_id: &active_branch_id,
            read_store,
            live_state: Arc::clone(&self.live_state),
            binary_cas: Arc::clone(&self.binary_cas),
            branch_ctx: Arc::clone(&self.branch_ctx),
            catalog_context: Arc::clone(&self.catalog_context),
            functions: functions.clone(),
            plugin_host: self.plugin_host.clone(),
            file_views: file_view_collector.clone(),
        };

        let query = sql2::execute_read_statement_from_parsed(&ctx, sql, statement, params).await?;
        drop(ctx);
        drop(live_state);
        let file_view_mutations = file_view_collector
            .map(|collector| collector.plugin_file_mutations())
            .unwrap_or_default();
        Ok((
            sql2::SessionReadSqlResult {
                runtime_functions,
                query,
            },
            file_view_mutations,
        ))
    }
}

/// Runs one session SQL read using a widened storage-read lifetime.
///
/// DataFusion requires providers and plans to be `'static`, while engine
/// storage implementations such as RocksDB naturally expose borrowed read snapshots. Keep
/// the lifetime erasure private to session SQL execution so callers cannot
/// receive the widened read as a general crate capability.
async fn with_static_session_sql_read<StorageImpl, F, Fut, T>(
    read: StorageAdapterReadScope<StorageImpl::Read<'_>>,
    f: F,
) -> Result<T, LixError>
where
    StorageImpl: Storage + 'static,
    F: FnOnce(SharedStorageAdapterRead<StorageImpl::Read<'static>>) -> Fut,
    Fut: Future<Output = Result<T, LixError>>,
{
    // SAFETY: the widened read is wrapped immediately in `SharedStorageAdapterRead`,
    // only passed into this private SQL execution closure, and explicitly
    // dropped before returning. Escaped clones are detected by `finish()`.
    let read = unsafe { assume_static_storage_read::<StorageImpl>(read) };
    let read = SharedStorageAdapterRead::new(read);
    let finish = read.clone();
    let result = f(read).await;
    let finish_result = finish.finish().map_err(LixError::from);
    match (result, finish_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (_, Err(finish_error)) => Err(finish_error),
    }
}

/// Erases the storage borrow lifetime for scoped session SQL execution.
///
/// # Safety
///
/// The returned read scope must not outlive the storage value that produced
/// `read`, and it must be dropped before the enclosing SQL execution returns.
unsafe fn assume_static_storage_read<StorageImpl>(
    read: StorageAdapterReadScope<StorageImpl::Read<'_>>,
) -> StorageAdapterReadScope<StorageImpl::Read<'static>>
where
    StorageImpl: Storage + 'static,
{
    let read = std::mem::ManuallyDrop::new(read);
    unsafe {
        std::ptr::read(
            std::ptr::from_ref(&*read)
                .cast::<StorageAdapterReadScope<StorageImpl::Read<'static>>>(),
        )
    }
}

impl<StorageImpl> SessionTransaction<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Executes one SQL statement inside this transaction.
    ///
    /// Write statements are staged until `commit()`. Read statements use the
    /// transaction overlay, so they can observe writes staged by earlier calls
    /// on this transaction handle.
    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        self.execute_with_options(sql, params, ExecuteOptions::default())
            .await
    }

    pub async fn execute_with_options(
        &mut self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        let telemetry =
            SqlStatementTelemetry::start(self.telemetry.as_ref(), sql, "transaction", None);
        let operation = async {
            let _operation_guard = self.begin_session_operation()?;
            let statement = self.sql_planning_cache.parse_statement(sql)?;
            let transaction = self.transaction_mut()?;
            execute_transaction_statement(transaction, sql, statement, params, options)
                .await
                .map_err(|error| normalize_sql_surface_error(error, sql))
        };
        let result = match telemetry.as_ref() {
            Some(telemetry) => telemetry.instrument(operation).await,
            None => operation.await,
        };
        if let Some(telemetry) = telemetry {
            telemetry.finish(&result);
        }
        result
    }

    #[cfg(test)]
    pub(crate) async fn execute_with_write_executor_mode(
        &mut self,
        sql: &str,
        params: &[Value],
        mode: sql2::WriteExecutorMode,
    ) -> Result<ExecuteResult, LixError> {
        let _operation_guard = self.begin_session_operation()?;
        let statement = self.sql_planning_cache.parse_statement(sql)?;
        let transaction = self.transaction_mut()?;
        match sql2::bind_statement_route(&statement)? {
            sql2::BoundStatementRoute::Write => {
                execute_transaction_write_with_mode(transaction, sql, statement, params, mode)
                    .await
                    .map_err(|error| normalize_sql_surface_error(error, sql))
            }
            sql2::BoundStatementRoute::Read => self.execute(sql, params).await,
        }
    }

    #[cfg(test)]
    pub(crate) async fn execute_with_write_executor_mode_and_trace(
        &mut self,
        sql: &str,
        params: &[Value],
        mode: sql2::WriteExecutorMode,
    ) -> Result<(ExecuteResult, Option<sql2::WriteExecutorPath>), LixError> {
        let _operation_guard = self.begin_session_operation()?;
        let statement = self.sql_planning_cache.parse_statement(sql)?;
        let transaction = self.transaction_mut()?;
        match sql2::bind_statement_route(&statement)? {
            sql2::BoundStatementRoute::Write => execute_transaction_write_with_mode_and_trace(
                transaction,
                sql,
                statement,
                params,
                mode,
            )
            .await
            .map_err(|error| normalize_sql_surface_error(error, sql)),
            sql2::BoundStatementRoute::Read => {
                self.execute(sql, params).await.map(|result| (result, None))
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn scan_live_state_for_test(
        &mut self,
        request: &crate::live_state::LiveStateScanRequest,
    ) -> Result<Vec<crate::live_state::MaterializedLiveStateRow>, LixError> {
        let _operation_guard = self.begin_session_operation()?;
        let transaction = self.transaction_mut()?;
        <crate::transaction::Transaction<StorageImpl> as sql2::SqlWriteExecutionContext>::scan_live_state(
            transaction,
            request,
        )
        .await
    }
}

async fn execute_transaction_write_auto<StorageImpl>(
    transaction: &mut crate::transaction::Transaction<StorageImpl>,
    sql: &str,
    statement: datafusion::sql::parser::Statement,
    params: &[Value],
    options: ExecuteOptions,
) -> Result<ExecuteResult, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let previous_origin_key = transaction.replace_origin_key(options.origin_key);
    let result = async {
        let tx_plan = transaction.prepare_sql_write_logical_plan(sql, &statement)?;
        let result = sql2::execute_write_logical_plan_result(transaction, tx_plan, params).await?;
        Ok(ExecuteResult::from_sql_write_result(result))
    }
    .await;
    transaction.replace_origin_key(previous_origin_key);
    result
}

/// Returns true only when SQL directly delivers one file's bytes to the
/// caller. Materializing `data` inside an aggregate, join, filter, or derived
/// expression is not acknowledgement: the caller did not receive those bytes
/// and must not gain the ability to delete entities that only existed there.
///
/// This intentionally recognizes a narrow, predictable MVP surface. False
/// negatives merely preserve an omitted entity; false positives can lose one.
fn is_acknowledgeable_file_data_read(statement: &DataFusionStatement, params: &[Value]) -> bool {
    let Some(point_read) = simple_point_read(statement) else {
        return false;
    };

    if !point_read.select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::UnnamedExpr(expression)
                | SelectItem::ExprWithAlias {
                    expr: expression,
                    ..
                } if direct_column_name(expression).as_deref() == Some("data")
        )
    }) {
        return false;
    }

    let selection = point_read
        .select
        .selection
        .as_ref()
        .expect("simple point read requires a predicate");
    let mut equality_columns = BTreeSet::new();
    // Anonymous placeholders are bound in textual order. Atelier's point read
    // projects the active branch as `? AS active_branch_id` before filtering
    // by `file.id = ?`, so start the WHERE binder after projection params.
    let mut anonymous_placeholder_index = point_read
        .select
        .projection
        .iter()
        .map(anonymous_placeholders_in_select_item)
        .sum();
    if !collect_literal_equalities(
        selection,
        &mut equality_columns,
        params,
        &mut anonymous_placeholder_index,
    ) {
        return false;
    }
    match point_read.table_name.as_str() {
        "lix_file" => {
            equality_columns.len() == 1
                && (equality_columns.contains("id") || equality_columns.contains("path"))
        }
        "lix_file_by_branch" => {
            equality_columns.len() == 2
                && equality_columns.contains("lixcol_branch_id")
                && (equality_columns.contains("id") || equality_columns.contains("path"))
        }
        _ => false,
    }
}

struct SimplePointRead<'a> {
    select: &'a Select,
    table_name: String,
    exact_table_shape: bool,
}

fn simple_point_read(statement: &DataFusionStatement) -> Option<SimplePointRead<'_>> {
    let DataFusionStatement::Statement(statement) = statement else {
        return None;
    };
    let SqlStatement::Query(query) = statement.as_ref() else {
        return None;
    };
    if query.with.is_some()
        || query.order_by.is_some()
        || !point_read_limit_is_safe(query.limit_clause.as_ref())
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.flavor != SelectFlavor::Standard
        || select.optimizer_hint.is_some()
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !group_by_is_empty(&select.group_by)
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return None;
    }

    let [from] = select.from.as_slice() else {
        return None;
    };
    if !from.joins.is_empty() {
        return None;
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
        ..
    } = &from.relation
    else {
        return None;
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return None;
    }
    let table_name = name.0.last().and_then(|part| part.as_ident())?;
    let unquoted_table = table_name.quote_style.is_none();
    let table_name = table_name.value.to_ascii_lowercase();

    select.selection.as_ref()?;
    Some(SimplePointRead {
        select,
        table_name,
        exact_table_shape: name.0.len() == 1
            && unquoted_table
            && alias.is_none()
            && query.limit_clause.is_none(),
    })
}

fn exact_lix_file_read(
    statement: &DataFusionStatement,
    params: &[Value],
) -> Option<(sql2::ExactLixFileReadSelector, sql2::ExactLixFileReadColumn)> {
    let point_read = simple_point_read(statement)?;
    if point_read.table_name != "lix_file" || !point_read.exact_table_shape {
        return None;
    }
    let [SelectItem::UnnamedExpr(projection)] = point_read.select.projection.as_slice() else {
        return None;
    };
    let Expr::Identifier(projection) = projection else {
        return None;
    };
    if projection.quote_style.is_some() {
        return None;
    }
    let column = match projection.value.to_ascii_lowercase().as_str() {
        "data" => sql2::ExactLixFileReadColumn::Data,
        "lixcol_change_id" => sql2::ExactLixFileReadColumn::ChangeId,
        _ => return None,
    };
    let selection = point_read.select.selection.as_ref()?;
    let (identity_column, identity_value) = exact_point_identity(selection, params)?;
    let selector = match identity_column.as_str() {
        "id" => sql2::ExactLixFileReadSelector::Id(identity_value),
        "path" => sql2::ExactLixFileReadSelector::Path(identity_value),
        _ => return None,
    };
    Some((selector, column))
}

fn exact_point_identity(expression: &Expr, params: &[Value]) -> Option<(String, String)> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expression
    else {
        return None;
    };
    match (exact_point_column(left), exact_point_column(right)) {
        (Some(column), None) => Some((column, exact_point_text_param(right, params)?)),
        (None, Some(column)) => Some((column, exact_point_text_param(left, params)?)),
        _ => None,
    }
}

fn exact_point_column(expression: &Expr) -> Option<String> {
    let Expr::Identifier(identifier) = expression else {
        return None;
    };
    if identifier.quote_style.is_some() {
        return None;
    }
    Some(identifier.value.to_ascii_lowercase())
}

fn exact_point_text_param(expression: &Expr, params: &[Value]) -> Option<String> {
    let Expr::Value(value) = expression else {
        return None;
    };
    match &value.value {
        SqlValue::Placeholder(placeholder)
            if params.len() == 1 && (placeholder == "?" || placeholder == "$1") =>
        {
            let Value::Text(value) = &params[0] else {
                return None;
            };
            Some(value.clone())
        }
        _ => None,
    }
}

/// A unique id/path predicate can return at most one row. `LIMIT 1` therefore
/// leaves that delivered row unchanged, while offsets and dynamic limits can
/// hide a materialized row and must remain non-acknowledging.
fn point_read_limit_is_safe(limit_clause: Option<&LimitClause>) -> bool {
    let Some(limit_clause) = limit_clause else {
        return true;
    };
    let LimitClause::LimitOffset {
        limit,
        offset,
        limit_by,
    } = limit_clause
    else {
        return false;
    };
    if offset.is_some() || !limit_by.is_empty() {
        return false;
    }
    let Some(Expr::Value(value)) = limit else {
        // `LIMIT ALL` does not remove the unique point row.
        return limit.is_none();
    };
    matches!(&value.value, SqlValue::Number(number, _) if number.parse::<u64>().is_ok_and(|number| number > 0))
}

fn anonymous_placeholders_in_select_item(item: &SelectItem) -> usize {
    let expression = match item {
        SelectItem::UnnamedExpr(expression)
        | SelectItem::ExprWithAlias {
            expr: expression, ..
        } => expression,
        SelectItem::QualifiedWildcard(..) | SelectItem::Wildcard(..) => return 0,
    };
    let mut visitor = AnonymousPlaceholderCounter::default();
    let _ = expression.visit(&mut visitor);
    visitor.count
}

#[derive(Default)]
struct AnonymousPlaceholderCounter {
    count: usize,
}

impl Visitor for AnonymousPlaceholderCounter {
    type Break = ();

    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
        if matches!(
            expression,
            Expr::Value(value) if matches!(&value.value, SqlValue::Placeholder(placeholder) if placeholder == "?")
        ) {
            self.count = self.count.saturating_add(1);
        }
        ControlFlow::Continue(())
    }
}

fn group_by_is_empty(group_by: &GroupByExpr) -> bool {
    matches!(group_by, GroupByExpr::Expressions(expressions, modifiers)
        if expressions.is_empty() && modifiers.is_empty())
}

fn direct_column_name(expression: &Expr) -> Option<String> {
    let identifier = match expression {
        Expr::Identifier(identifier) => identifier,
        Expr::CompoundIdentifier(identifiers) => identifiers.last()?,
        Expr::Nested(expression) => return direct_column_name(expression),
        _ => return None,
    };
    Some(identifier.value.to_ascii_lowercase())
}

fn collect_literal_equalities(
    expression: &Expr,
    columns: &mut BTreeSet<String>,
    params: &[Value],
    anonymous_placeholder_index: &mut usize,
) -> bool {
    match expression {
        Expr::Nested(expression) => {
            collect_literal_equalities(expression, columns, params, anonymous_placeholder_index)
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_literal_equalities(left, columns, params, anonymous_placeholder_index)
                && collect_literal_equalities(right, columns, params, anonymous_placeholder_index)
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let column = match (direct_column_name(left), direct_column_name(right)) {
                (Some(column), None)
                    if point_identity_value_is_text(right, params, anonymous_placeholder_index) =>
                {
                    column
                }
                (None, Some(column))
                    if point_identity_value_is_text(left, params, anonymous_placeholder_index) =>
                {
                    column
                }
                _ => return false,
            };
            columns.insert(column)
        }
        _ => false,
    }
}

fn point_identity_value_is_text(
    expression: &Expr,
    params: &[Value],
    anonymous_placeholder_index: &mut usize,
) -> bool {
    let Expr::Value(value) = expression else {
        return false;
    };
    match &value.value {
        SqlValue::Placeholder(placeholder) => {
            let index = if placeholder == "?" {
                let index = *anonymous_placeholder_index;
                *anonymous_placeholder_index += 1;
                Some(index)
            } else {
                placeholder
                    .strip_prefix('$')
                    .and_then(|index| index.parse::<usize>().ok())
                    .and_then(|index| index.checked_sub(1))
            };
            index
                .and_then(|index| params.get(index))
                .is_some_and(|value| matches!(value, Value::Text(_)))
        }
        value => value.clone().into_string().is_some(),
    }
}

fn classify_execute_batch(
    statements: &[ExecuteBatchStatement],
    planning_cache: &sql2::SqlPlanningCache<crate::catalog::CatalogFingerprint>,
) -> Result<ExecuteBatchExecution, LixError> {
    // Classify the complete batch before choosing a snapshot or transaction;
    // switching execution modes between statements would break atomicity, and
    // any possible durable mutation keeps the whole batch transactional so
    // later reads retain read-after-write visibility.
    let mut parsed = Vec::with_capacity(statements.len());
    let mut is_read_only = true;
    for (statement_index, statement) in statements.iter().enumerate() {
        let parsed_statement = planning_cache
            .parse_statement(&statement.sql)
            .map_err(|error| with_batch_statement_index(error, statement_index))?;
        let route = sql2::bind_statement_route(&parsed_statement)
            .map_err(|error| with_batch_statement_index(error, statement_index))?;
        if route == sql2::BoundStatementRoute::Write
            || sql2::statement_has_durable_runtime_function(&parsed_statement)
        {
            is_read_only = false;
        }
        parsed.push(parsed_statement);
    }
    if is_read_only {
        Ok(ExecuteBatchExecution::ReadOnly(parsed))
    } else {
        Ok(ExecuteBatchExecution::Transaction(parsed))
    }
}

async fn execute_transaction_statement<StorageImpl>(
    transaction: &mut crate::transaction::Transaction<StorageImpl>,
    sql: &str,
    statement: datafusion::sql::parser::Statement,
    params: &[Value],
    options: ExecuteOptions,
) -> Result<ExecuteResult, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    match sql2::bind_statement_route(&statement)? {
        sql2::BoundStatementRoute::Write => {
            execute_transaction_write_auto(transaction, sql, statement, params, options).await
        }
        sql2::BoundStatementRoute::Read => transaction
            .execute_read_sql_statement(sql, statement, params)
            .await
            .map(ExecuteResult::from_sql_query_result),
    }
}

fn with_batch_statement_index(mut error: LixError, statement_index: usize) -> LixError {
    let mut details = match error.details.take() {
        Some(JsonValue::Object(details)) => details,
        Some(details) => {
            let mut wrapped = JsonMap::new();
            wrapped.insert("cause".to_string(), details);
            wrapped
        }
        None => JsonMap::new(),
    };
    details.insert(
        "statementIndex".to_string(),
        JsonValue::from(statement_index),
    );
    error.details = Some(JsonValue::Object(details));
    error
}

#[cfg(test)]
async fn execute_transaction_write_with_mode<StorageImpl>(
    transaction: &mut crate::transaction::Transaction<StorageImpl>,
    sql: &str,
    statement: datafusion::sql::parser::Statement,
    params: &[Value],
    mode: sql2::WriteExecutorMode,
) -> Result<ExecuteResult, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let tx_plan = transaction.prepare_sql_write_logical_plan(sql, &statement)?;
    let result =
        sql2::execute_write_logical_plan_with_mode_result(transaction, tx_plan, params, mode)
            .await?;
    Ok(ExecuteResult::from_sql_write_result(result))
}

#[cfg(test)]
async fn execute_transaction_write_with_mode_and_trace<StorageImpl>(
    transaction: &mut crate::transaction::Transaction<StorageImpl>,
    sql: &str,
    statement: datafusion::sql::parser::Statement,
    params: &[Value],
    mode: sql2::WriteExecutorMode,
) -> Result<(ExecuteResult, Option<sql2::WriteExecutorPath>), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let tx_plan = transaction.prepare_sql_write_logical_plan(sql, &statement)?;
    let (result, path) = sql2::execute_write_logical_plan_with_mode_and_trace_result(
        transaction,
        tx_plan,
        params,
        mode,
    )
    .await?;
    Ok((ExecuteResult::from_sql_write_result(result), Some(path)))
}

fn normalize_sql_surface_error(error: LixError, sql: &str) -> LixError {
    if (error.code.starts_with("LIX_ERROR_PATH_") && sql_uses_public_filesystem_path_surface(sql))
        || (error.code == LixError::CODE_SCHEMA_DEFINITION
            && error.message.to_ascii_lowercase().contains("system schema"))
    {
        return LixError {
            code: LixError::CODE_INVALID_PARAM.to_string(),
            ..error
        };
    }
    if error.code == LixError::CODE_INVALID_JSON_PATH
        && error
            .message
            .to_ascii_lowercase()
            .contains("uses variadic path segments")
    {
        return LixError {
            code: LixError::CODE_INVALID_PARAM.to_string(),
            ..error
        };
    }
    error
}

fn sql_uses_public_filesystem_path_surface(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    (lower.contains("lix_file") || lower.contains("lix_directory")) && lower.contains("path")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Engine, Memory};

    async fn open_session() -> SessionContext<Memory> {
        let storage = Memory::default();
        Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized storage should create engine");
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open")
    }

    fn batch_statement(sql: &str) -> ExecuteBatchStatement {
        ExecuteBatchStatement {
            sql: sql.to_string(),
            params: Vec::new(),
        }
    }

    #[test]
    fn exact_lix_file_read_recognizes_only_the_narrow_point_shapes() {
        let data_by_id = sql2::parse_statement("SELECT data FROM lix_file WHERE id = $1").unwrap();
        assert_eq!(
            exact_lix_file_read(&data_by_id, &[Value::Text("file-a".to_string())]),
            Some((
                sql2::ExactLixFileReadSelector::Id("file-a".to_string()),
                sql2::ExactLixFileReadColumn::Data,
            ))
        );

        let change_by_path =
            sql2::parse_statement("SELECT lixcol_change_id FROM lix_file WHERE path = ?").unwrap();
        assert_eq!(
            exact_lix_file_read(&change_by_path, &[Value::Text("/a.txt".to_string())]),
            Some((
                sql2::ExactLixFileReadSelector::Path("/a.txt".to_string()),
                sql2::ExactLixFileReadColumn::ChangeId,
            ))
        );

        for (sql, params) in [
            (
                "SELECT id FROM lix_file WHERE id = $1",
                vec![Value::Text("file-a".to_string())],
            ),
            (
                "SELECT data AS bytes FROM lix_file WHERE id = $1",
                vec![Value::Text("file-a".to_string())],
            ),
            (
                "SELECT data FROM lix_file AS file WHERE id = $1",
                vec![Value::Text("file-a".to_string())],
            ),
            ("SELECT data FROM lix_file WHERE id = 'file-a'", vec![]),
            (
                "SELECT data FROM lix_file WHERE id = $1 LIMIT 1",
                vec![Value::Text("file-a".to_string())],
            ),
            (
                "SELECT \"DATA\" FROM lix_file WHERE id = $1",
                vec![Value::Text("file-a".to_string())],
            ),
            (
                "SELECT data FROM \"LIX_FILE\" WHERE id = $1",
                vec![Value::Text("file-a".to_string())],
            ),
            (
                "SELECT data FROM lix_file WHERE id = $1 AND true",
                vec![Value::Text("file-a".to_string())],
            ),
            ("SELECT data FROM lix_file WHERE id = $1", vec![Value::Null]),
            (
                "SELECT data FROM lix_file WHERE id = $1",
                vec![
                    Value::Text("file-a".to_string()),
                    Value::Text("extra".to_string()),
                ],
            ),
        ] {
            let statement = sql2::parse_statement(sql).unwrap();
            assert_eq!(
                exact_lix_file_read(&statement, &params),
                None,
                "unexpected fast-path match for {sql}"
            );
        }
    }

    #[test]
    fn execute_batch_classifies_only_pure_reads_for_the_fast_path() {
        let cache = sql2::SqlPlanningCache::default();
        assert!(matches!(
            classify_execute_batch(
                &[
                    batch_statement("SELECT 1"),
                    batch_statement("SELECT * FROM lix_file"),
                ],
                &cache
            )
            .unwrap(),
            ExecuteBatchExecution::ReadOnly(_)
        ));
        assert!(matches!(
            classify_execute_batch(
                &[
                    batch_statement("SELECT 1"),
                    batch_statement("DELETE FROM lix_file WHERE id = 'missing'"),
                ],
                &cache
            )
            .unwrap(),
            ExecuteBatchExecution::Transaction(_)
        ));
        assert!(matches!(
            classify_execute_batch(&[batch_statement("SELECT lix_uuid_v7()")], &cache).unwrap(),
            ExecuteBatchExecution::Transaction(_)
        ));
    }

    #[test]
    fn execute_batch_classification_preserves_the_invalid_statement_index() {
        let cache = sql2::SqlPlanningCache::default();
        let result = classify_execute_batch(
            &[
                batch_statement("SELECT 1"),
                batch_statement("this is not SQL"),
            ],
            &cache,
        );
        let Err(error) = result else {
            panic!("invalid SQL should fail classification");
        };

        assert_eq!(error.details.unwrap()["statementIndex"], 1);
    }

    #[tokio::test]
    async fn execute_batch_pure_read_fast_path_preserves_order_and_parameters() {
        let session = open_session().await;
        let results = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    sql: "SELECT $1 AS value".to_string(),
                    params: vec![Value::Integer(11)],
                },
                ExecuteBatchStatement {
                    sql: "SELECT $1 AS value".to_string(),
                    params: vec![Value::Integer(22)],
                },
            ])
            .await
            .unwrap();

        assert_eq!(results[0].rows()[0].get::<i64>("value").unwrap(), 11);
        assert_eq!(results[1].rows()[0].get::<i64>("value").unwrap(), 22);
    }

    #[test]
    fn row_get_converts_native_values_and_value_keeps_wrapper() {
        let result = ExecuteResult::from_rows(
            vec!["title".to_string(), "done".to_string()],
            vec![vec![Value::Text("Hello".to_string()), Value::Boolean(true)]],
        );
        let row = &result.rows()[0];

        assert_eq!(row.get::<String>("title").unwrap(), "Hello");
        assert!(row.get::<bool>("done").unwrap());
        assert_eq!(
            row.value("title").unwrap(),
            &Value::Text("Hello".to_string())
        );
    }

    #[test]
    fn execute_result_clone_shares_immutable_backing() {
        let result = ExecuteResult::from_rows(
            vec!["data".to_string()],
            vec![vec![Value::Blob(vec![b'x'; 1024 * 1024])]],
        );
        let cloned = result.clone();

        assert!(Arc::ptr_eq(&result.backing, &cloned.backing));
        assert_eq!(result, cloned);
    }

    #[test]
    fn row_get_errors_on_missing_column_and_wrong_type() {
        let result = ExecuteResult::from_rows(
            vec!["title".to_string()],
            vec![vec![Value::Text("Hello".to_string())]],
        );
        let row = &result.rows()[0];

        let missing = row.get::<String>("missing").unwrap_err();
        assert_eq!(missing.code, LixError::CODE_COLUMN_NOT_FOUND);
        assert!(missing.message.contains("available columns: title"));

        let wrong_type = row.get::<bool>("title").unwrap_err();
        assert_eq!(wrong_type.code, "LIX_ERROR_VALUE_TYPE");
    }

    #[tokio::test]
    async fn coherent_read_batch_rejects_write_statements() {
        let session = open_session().await;
        let statements: [(&str, &[Value]); 1] = [(
            "INSERT INTO lix_key_value (key, value) VALUES ('batch-write', 'value')",
            &[],
        )];

        let error = session
            .execute_coherent_read_batch(&statements)
            .await
            .expect_err("write statement should be rejected");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(
            error
                .message
                .contains("execute_coherent_read_batch only accepts read statements")
        );
    }

    #[tokio::test]
    async fn coherent_read_batch_returns_metadata_and_ordered_results() {
        let session = open_session().await;
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('batch-read', 'value')",
                &[],
            )
            .await
            .expect("seed row");
        let active_branch_id = session
            .active_branch_id()
            .await
            .expect("active branch id should load");
        let storage_mutation_revision = session
            .storage_mutation_revision()
            .await
            .expect("mutation revision should load");
        let active_branch_commit_id = session
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await
            .expect("active branch commit should load")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("commit id should be text");
        let statements: [(&str, &[Value]); 2] = [
            ("SELECT 'first' AS label", &[]),
            (
                "SELECT key, value FROM lix_key_value WHERE key = 'batch-read'",
                &[],
            ),
        ];

        let batch = session
            .execute_coherent_read_batch(&statements)
            .await
            .expect("coherent read batch should execute");

        assert_eq!(batch.active_branch_id, active_branch_id);
        assert_eq!(batch.active_branch_commit_id, active_branch_commit_id);
        assert_eq!(batch.storage_mutation_revision, storage_mutation_revision);
        assert_eq!(batch.results.len(), 2);
        assert_eq!(
            batch.results[0].rows()[0].get::<String>("label").unwrap(),
            "first"
        );
        let row = &batch.results[1].rows()[0];
        assert_eq!(row.get::<String>("key").unwrap(), "batch-read");
        assert_eq!(
            row.get::<serde_json::Value>("value").unwrap(),
            serde_json::json!("value")
        );
    }

    #[tokio::test]
    async fn coherent_read_batch_registers_union_of_referenced_providers() {
        let session = open_session().await;
        let statements: [(&str, &[Value]); 3] = [
            ("SELECT 1 AS one", &[]),
            ("SELECT COUNT(*) AS files FROM lix_file", &[]),
            ("SELECT COUNT(*) AS states FROM lix_state", &[]),
        ];

        let batch = session
            .execute_coherent_read_batch(&statements)
            .await
            .expect("coherent batch should register every referenced provider");

        assert_eq!(batch.results.len(), 3);
        assert_eq!(batch.results[0].rows()[0].get::<i64>("one").unwrap(), 1);
        assert_eq!(batch.results[1].rows()[0].get::<i64>("files").unwrap(), 0);
        assert!(batch.results[2].rows()[0].get::<i64>("states").unwrap() > 0);
    }

    #[tokio::test]
    async fn referenced_provider_reads_preserve_complex_and_catalog_wide_queries() {
        let session = open_session().await;
        let complex = session
            .execute(
                "WITH files AS (SELECT id FROM lix_file) \
                 SELECT COUNT(*) AS row_count \
                 FROM files AS file_a \
                 JOIN files AS file_b ON file_a.id = file_b.id \
                 LEFT JOIN (\
                     SELECT entity_pk FROM lix_state \
                     UNION ALL \
                     SELECT entity_pk FROM lix_state\
                 ) AS states ON false",
                &[],
            )
            .await
            .expect("nested CTE, self-join, and UNION should resolve providers");
        assert_eq!(complex.rows()[0].get::<i64>("row_count").unwrap(), 0);

        let catalog = session
            .execute(
                "SELECT COUNT(*) AS surfaces \
                 FROM information_schema.tables \
                 WHERE table_schema = 'public'",
                &[],
            )
            .await
            .expect("information_schema should retain catalog-wide visibility");
        assert!(catalog.rows()[0].get::<i64>("surfaces").unwrap() > 1);
    }

    #[tokio::test]
    async fn read_provider_selection_loads_storage_catalog_only_for_dynamic_visibility() {
        let session = open_session().await;
        let schema_loads = || {
            session
                .catalog_context
                .sql_read_schema_load_count_for_test()
        };

        let before = schema_loads();
        session
            .execute("SELECT 1 AS one", &[])
            .await
            .expect("table-free read should execute");
        assert_eq!(schema_loads(), before, "SELECT 1 needs no SQL catalog");

        session
            .execute("SELECT COUNT(*) AS rows FROM lix_key_value", &[])
            .await
            .expect("fixed entity surface should execute");
        assert_eq!(
            schema_loads(),
            before,
            "fixed entity metadata comes from compile-time schemas"
        );

        session
            .execute(
                "SELECT COUNT(*) AS rows FROM lix_key_value_history \
                 WHERE lixcol_start_commit_id = lix_active_branch_commit_id()",
                &[],
            )
            .await
            .expect("fixed history surface should execute");
        assert_eq!(
            schema_loads(),
            before,
            "fixed history metadata comes from compile-time schemas"
        );

        session
            .execute(
                "SELECT COUNT(*) AS rows FROM lix_key_value AS kv \
                 JOIN lix_state AS state ON false",
                &[],
            )
            .await
            .expect("join of fixed surfaces should execute");
        assert_eq!(
            schema_loads(),
            before,
            "a join remains storage-free when every table is fixed"
        );

        session
            .execute(
                "SELECT COUNT(*) AS surfaces FROM information_schema.tables",
                &[],
            )
            .await
            .expect("information schema should execute");
        assert_eq!(
            schema_loads(),
            before + 1,
            "catalog-wide visibility must load dynamic schemas"
        );

        let custom_schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "x-lix-key": "custom_catalog_probe",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": { "id": { "type": "string" } },
            "required": ["id"],
            "additionalProperties": false,
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
                &[Value::Text(custom_schema.to_string())],
            )
            .await
            .expect("custom schema should register");
        let before_custom_read = schema_loads();

        session
            .execute("SELECT COUNT(*) AS rows FROM custom_catalog_probe", &[])
            .await
            .expect("custom entity should execute");
        assert_eq!(
            schema_loads(),
            before_custom_read + 1,
            "custom entity metadata must load the visible catalog"
        );

        let before_mixed_join = schema_loads();
        session
            .execute(
                "SELECT COUNT(*) AS rows FROM lix_key_value AS kv \
                 JOIN custom_catalog_probe AS custom ON false",
                &[],
            )
            .await
            .expect("mixed fixed/custom join should execute");
        assert_eq!(
            schema_loads(),
            before_mixed_join + 1,
            "one custom table makes the whole session use the visible catalog"
        );
    }

    #[tokio::test]
    async fn transaction_referenced_provider_reads_see_staged_writes() {
        let session = open_session().await;
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('selected-provider-file', '/selected.txt')",
                &[],
            )
            .await
            .expect("file should stage");

        let result = transaction
            .execute(
                "WITH selected AS (\
                     SELECT id FROM lix_file WHERE id = 'selected-provider-file'\
                 ) \
                 SELECT id FROM selected",
                &[],
            )
            .await
            .expect("selected overlay provider should expose staged writes");
        assert_eq!(
            result.rows()[0].get::<String>("id").unwrap(),
            "selected-provider-file"
        );

        transaction
            .rollback()
            .await
            .expect("transaction should roll back");
    }
}
