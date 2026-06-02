use std::future::Future;
use std::sync::Arc;

use crate::functions::FunctionContext;
use crate::sql2;
use crate::storage::{
    SharedStorageRead, StorageBackend, StorageReadOptions, StorageReadScope, StorageWriteOptions,
    StorageWriteSet,
};
use crate::transaction::{begin_commit_boundary, commit_at_boundary};
use crate::{LixError, LixNotice, SqlQueryResult, Value};

use super::context::{SessionContext, SessionSqlExecutionContext, SessionWriteAccess};
use super::transaction::SessionTransaction;

/// Result of executing one SQL statement through engine.
///
/// Column names live once at the result-set level. Individual rows only own
/// values, which keeps the public API row-oriented without copying schema
/// metadata into every row.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecuteResult {
    columns: Vec<String>,
    rows: Vec<Row>,
    rows_affected: u64,
    notices: Vec<LixNotice>,
}

impl ExecuteResult {
    fn from_sql_query_result(result: SqlQueryResult) -> Self {
        Self {
            columns: result.columns,
            rows: Vec::new(),
            rows_affected: 0,
            notices: result.notices,
        }
        .with_rows(result.rows)
    }

    pub fn from_rows_affected(rows_affected: u64) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected,
            notices: Vec::new(),
        }
    }

    pub fn from_rows(columns: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
            rows_affected: 0,
            notices: Vec::new(),
        }
        .with_rows(rows)
    }

    fn with_rows(mut self, rows: Vec<Vec<Value>>) -> Self {
        let columns = Arc::<[String]>::from(self.columns.clone().into_boxed_slice());
        self.rows = rows
            .into_iter()
            .map(|values| Row {
                columns: Arc::clone(&columns),
                values,
            })
            .collect();
        self
    }

    /// Returns the result-set column names in row value order.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Returns the owned rows. Use `iter()` for name-based access.
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Iterates rows with borrowed access to the shared column metadata.
    pub fn iter(&self) -> impl Iterator<Item = RowRef<'_>> {
        self.rows.iter().map(|row| RowRef {
            columns: self.columns.as_slice(),
            values: row.values.as_slice(),
        })
    }

    /// Returns the number of rows in this result set.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns true when this result set has no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns the number of rows affected by a mutation statement.
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    /// Returns non-fatal diagnostics produced while executing the statement.
    pub fn notices(&self) -> &[LixNotice] {
        &self.notices
    }

    /// Looks up the value for `column_name` on an owned row from this set.
    pub fn get<'a>(&self, row: &'a Row, column_name: &str) -> Option<&'a Value> {
        let index = self.column_index(column_name)?;
        row.get_index(index)
    }

    /// Returns the index for a column name.
    pub fn column_index(&self, column_name: &str) -> Option<usize> {
        self.columns.iter().position(|column| column == column_name)
    }
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

impl<B> SessionContext<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    /// Executes one DataFusion SQL statement against this Lix session.
    ///
    /// The SQL dialect is DataFusion SQL, not SQLite SQL. Positional
    /// placeholders use `?` or `$1`, `$2`, and so on. SQLite-specific catalog tables
    /// and transaction statements such as `sqlite_master`, `BEGIN`, and
    /// `COMMIT` are not part of this contract; use `information_schema` for
    /// catalog inspection. Lix owns transaction boundaries for each statement.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.ensure_open()?;
        let statement = sql2::parse_statement(sql)?;
        if sql2::bind_statement_route(&statement)? == sql2::BoundStatementRoute::Write {
            let write_access = self.begin_session_write_access().await?;
            let sql_for_error = sql.to_string();
            let params = params.to_vec();
            return self
                .with_write_transaction_reserved(write_access, |transaction| {
                    Box::pin(async move {
                        // Re-plan against the transaction-backed write
                        // session so provider hooks read and stage through the
                        // transaction-owned SQL write context.
                        transaction.prepare_sql_visible_schemas().await?;
                        let tx_plan =
                            sql2::create_write_logical_plan_from_parsed(transaction, statement)
                                .await?;
                        let affected_rows =
                            sql2::execute_write_logical_plan(transaction, tx_plan, &params).await?;
                        Ok(ExecuteResult::from_rows_affected(affected_rows))
                    })
                })
                .await
                .map_err(|error| normalize_sql_surface_error(error, &sql_for_error));
        }

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
            Some(self.begin_session_operation()?)
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
        let read_scope = self.storage.begin_read(StorageReadOptions::default())?;
        let read_result =
            with_static_session_sql_read::<B, _, _>(read_scope, |read_store| async move {
                let live_state: Arc<dyn crate::live_state::LiveStateReader> =
                    Arc::new(self.live_state.reader(read_store.clone()));
                let runtime_functions = FunctionContext::prepare(live_state.as_ref()).await?;
                let functions = runtime_functions.provider();
                let active_branch_id = self.active_branch_id_from_reader(&read_store).await?;
                let visible_schemas = self
                    .catalog_context
                    .schema_jsons_for_sql_read_planning(live_state.as_ref(), &active_branch_id)
                    .await?;
                let ctx = SessionSqlExecutionContext {
                    active_branch_id: &active_branch_id,
                    read_store,
                    live_state: Arc::clone(&self.live_state),
                    binary_cas: Arc::clone(&self.binary_cas),
                    branch_ctx: Arc::clone(&self.branch_ctx),
                    visible_schemas,
                    functions: functions.clone(),
                    plugin_host: self.plugin_host.clone(),
                };

                let result =
                    sql2::execute_read_statement_from_parsed(&ctx, sql, statement, params).await?;
                drop(ctx);
                drop(live_state);
                Ok::<_, LixError>(sql2::SessionReadSqlResult {
                    runtime_functions,
                    query: result,
                })
            });
        let read_result = match read_result.await {
            Ok(result) => result,
            Err(error) => {
                return Err(normalize_sql_surface_error(error, sql));
            }
        };
        self.persist_runtime_functions_if_needed(
            &read_result.runtime_functions,
            runtime_write_access.as_ref(),
        )
        .await?;
        Ok(ExecuteResult::from_sql_query_result(read_result.query))
    }

    #[cfg(test)]
    pub(crate) async fn execute_with_write_executor_mode(
        &self,
        sql: &str,
        params: &[Value],
        mode: sql2::WriteExecutorMode,
    ) -> Result<ExecuteResult, LixError> {
        self.ensure_open()?;
        let statement = sql2::parse_statement(sql)?;
        if sql2::bind_statement_route(&statement)? == sql2::BoundStatementRoute::Write {
            let write_access = self.begin_session_write_access().await?;
            let sql_for_error = sql.to_string();
            let params = params.to_vec();
            return self
                .with_write_transaction_reserved(write_access, |transaction| {
                    Box::pin(async move {
                        transaction.prepare_sql_visible_schemas().await?;
                        let tx_plan =
                            sql2::create_write_logical_plan_from_parsed(transaction, statement)
                                .await?;
                        let affected_rows = sql2::execute_write_logical_plan_with_mode(
                            transaction,
                            tx_plan,
                            &params,
                            mode,
                        )
                        .await?;
                        Ok(ExecuteResult::from_rows_affected(affected_rows))
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
    ) -> Result<(), LixError> {
        let mut writes = StorageWriteSet::new();
        runtime_functions
            .stage_persist_if_needed(&mut writes)
            .await?;
        if writes.is_empty() {
            return Ok(());
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
            .prepare_write_set(writes, StorageWriteOptions::default())?;
        commit_at_boundary(Some(&commit_boundary), || {
            prepared_commit.commit()?;
            Ok(())
        })?;
        Ok(())
    }
}

/// Runs one session SQL read using a widened backend-read lifetime.
///
/// DataFusion requires providers and plans to be `'static`, while engine
/// backends such as RocksDB/redb naturally expose borrowed read snapshots. Keep
/// the lifetime erasure private to session SQL execution so callers cannot
/// receive the widened read as a general crate capability.
async fn with_static_session_sql_read<B, F, Fut>(
    read: StorageReadScope<B::Read<'_>>,
    f: F,
) -> Result<sql2::SessionReadSqlResult, LixError>
where
    B: StorageBackend + 'static,
    F: FnOnce(SharedStorageRead<B::Read<'static>>) -> Fut,
    Fut: Future<Output = Result<sql2::SessionReadSqlResult, LixError>>,
{
    // SAFETY: the widened read is wrapped immediately in `SharedStorageRead`,
    // only passed into this private SQL execution closure, and explicitly
    // closed before returning. Escaped clones are detected by `close()`.
    let read = unsafe { assume_static_backend_read::<B>(read) };
    let read = SharedStorageRead::new(read);
    let close = read.clone();
    let result = f(read).await;
    let close_result = close.close().map_err(LixError::from);
    match (result, close_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (_, Err(close_error)) => Err(close_error),
    }
}

/// Erases the backend borrow lifetime for scoped session SQL execution.
///
/// # Safety
///
/// The returned read scope must not outlive the backend value that produced
/// `read`, and it must be dropped before the enclosing SQL execution returns.
unsafe fn assume_static_backend_read<B>(
    read: StorageReadScope<B::Read<'_>>,
) -> StorageReadScope<B::Read<'static>>
where
    B: StorageBackend + 'static,
{
    let read = std::mem::ManuallyDrop::new(read);
    unsafe {
        std::ptr::read(std::ptr::from_ref(&*read).cast::<StorageReadScope<B::Read<'static>>>())
    }
}

impl<B> SessionTransaction<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
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
        let _operation_guard = self.begin_session_operation()?;
        let statement = sql2::parse_statement(sql)?;
        let transaction = self.transaction_mut()?;
        match sql2::bind_statement_route(&statement)? {
            sql2::BoundStatementRoute::Write => {
                execute_transaction_write_auto(transaction, statement, params)
                    .await
                    .map_err(|error| normalize_sql_surface_error(error, sql))
            }
            sql2::BoundStatementRoute::Read => {
                let result = transaction
                    .execute_read_sql_statement(sql, statement, params)
                    .await
                    .map_err(|error| normalize_sql_surface_error(error, sql))?;
                Ok(ExecuteResult::from_sql_query_result(result))
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn execute_with_write_executor_mode(
        &mut self,
        sql: &str,
        params: &[Value],
        mode: sql2::WriteExecutorMode,
    ) -> Result<ExecuteResult, LixError> {
        let _operation_guard = self.begin_session_operation()?;
        let statement = sql2::parse_statement(sql)?;
        let transaction = self.transaction_mut()?;
        match sql2::bind_statement_route(&statement)? {
            sql2::BoundStatementRoute::Write => {
                execute_transaction_write_with_mode(transaction, statement, params, mode)
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
        let statement = sql2::parse_statement(sql)?;
        let transaction = self.transaction_mut()?;
        match sql2::bind_statement_route(&statement)? {
            sql2::BoundStatementRoute::Write => {
                execute_transaction_write_with_mode_and_trace(transaction, statement, params, mode)
                    .await
                    .map_err(|error| normalize_sql_surface_error(error, sql))
            }
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
        <crate::transaction::Transaction<B> as sql2::SqlWriteExecutionContext>::scan_live_state(
            transaction,
            request,
        )
        .await
    }
}

async fn execute_transaction_write_auto<B>(
    transaction: &mut crate::transaction::Transaction<B>,
    statement: datafusion::sql::parser::Statement,
    params: &[Value],
) -> Result<ExecuteResult, LixError>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    transaction.prepare_sql_visible_schemas().await?;
    let tx_plan = sql2::create_write_logical_plan_from_parsed(transaction, statement).await?;
    let affected_rows = sql2::execute_write_logical_plan(transaction, tx_plan, params).await?;
    Ok(ExecuteResult::from_rows_affected(affected_rows))
}

#[cfg(test)]
async fn execute_transaction_write_with_mode<B>(
    transaction: &mut crate::transaction::Transaction<B>,
    statement: datafusion::sql::parser::Statement,
    params: &[Value],
    mode: sql2::WriteExecutorMode,
) -> Result<ExecuteResult, LixError>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    transaction.prepare_sql_visible_schemas().await?;
    let tx_plan = sql2::create_write_logical_plan_from_parsed(transaction, statement).await?;
    let affected_rows =
        sql2::execute_write_logical_plan_with_mode(transaction, tx_plan, params, mode).await?;
    Ok(ExecuteResult::from_rows_affected(affected_rows))
}

#[cfg(test)]
async fn execute_transaction_write_with_mode_and_trace<B>(
    transaction: &mut crate::transaction::Transaction<B>,
    statement: datafusion::sql::parser::Statement,
    params: &[Value],
    mode: sql2::WriteExecutorMode,
) -> Result<(ExecuteResult, Option<sql2::WriteExecutorPath>), LixError>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    transaction.prepare_sql_visible_schemas().await?;
    let tx_plan = sql2::create_write_logical_plan_from_parsed(transaction, statement).await?;
    let (affected_rows, path) =
        sql2::execute_write_logical_plan_with_mode_and_trace(transaction, tx_plan, params, mode)
            .await?;
    Ok((ExecuteResult::from_rows_affected(affected_rows), Some(path)))
}

fn normalize_sql_surface_error(error: LixError, sql: &str) -> LixError {
    if error.code.starts_with("LIX_ERROR_PATH_") && sql_uses_public_filesystem_path_surface(sql) {
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
}
