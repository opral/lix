use std::sync::Arc;

use crate::functions::FunctionContext;
use crate::sql2;
use crate::storage::{StorageReadScope, StorageWriteSet};
use crate::{LixError, LixNotice, SqlQueryResult, Value};

use super::context::{SessionContext, SessionSqlExecutionContext};
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

impl SessionContext {
    /// Executes one DataFusion SQL statement against this Lix session.
    ///
    /// The SQL dialect is DataFusion SQL, not SQLite SQL. Positional
    /// placeholders use `?` or `$1`, `$2`, and so on. SQLite-specific catalog tables
    /// and transaction statements such as `sqlite_master`, `BEGIN`, and
    /// `COMMIT` are not part of this contract; use `information_schema` for
    /// catalog inspection. Lix owns transaction boundaries for each statement.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.ensure_open()?;
        let _transaction_guard = self.reserve_session_transaction()?;
        let statement = sql2::parse_statement(sql)?;
        if sql2::bind_statement_route(&statement)? == sql2::BoundStatementRoute::Write {
            let sql_for_error = sql.to_string();
            let params = params.to_vec();
            return self
                .with_write_transaction_reserved(|transaction| {
                    Box::pin(async move {
                        // Re-plan against the transaction-backed write
                        // session so provider hooks read and stage through the
                        // transaction-owned SQL write context.
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

        let read_scope = StorageReadScope::new(self.storage.begin_read_transaction().await?);
        let read_result = async {
            let mut read_store = read_scope.store();
            let live_state: Arc<dyn crate::live_state::LiveStateReader> =
                Arc::new(self.live_state.reader(read_store.clone()));
            let runtime_functions = FunctionContext::prepare(live_state.as_ref()).await?;
            let functions = runtime_functions.provider();
            let active_version_id = self.active_version_id_from_reader(&mut read_store).await?;
            let visible_schemas = self
                .catalog_context
                .schema_jsons_for_sql_read_planning(live_state.as_ref(), &active_version_id)
                .await?;
            let ctx = SessionSqlExecutionContext {
                active_version_id: &active_version_id,
                read_store,
                live_state: Arc::clone(&self.live_state),
                binary_cas: Arc::clone(&self.binary_cas),
                commit_store: Arc::clone(&self.commit_store),
                version_ctx: Arc::clone(&self.version_ctx),
                visible_schemas,
                functions: functions.clone(),
            };

            let plan = sql2::create_logical_plan_from_parsed(&ctx, sql, statement).await?;
            let result = sql2::execute_logical_plan(plan, params).await?;
            drop(ctx);
            drop(live_state);
            Ok::<_, LixError>((runtime_functions, result))
        };
        let (runtime_functions, result) = match read_result.await {
            Ok(result) => {
                read_scope.rollback().await?;
                result
            }
            Err(error) => {
                let _ = read_scope.rollback().await;
                return Err(normalize_sql_surface_error(error, sql));
            }
        };
        self.persist_runtime_functions_if_needed(&runtime_functions)
            .await?;
        Ok(ExecuteResult::from_sql_query_result(result))
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
            let _transaction_guard = self.reserve_session_transaction()?;
            let sql_for_error = sql.to_string();
            let params = params.to_vec();
            return self
                .with_write_transaction_reserved(|transaction| {
                    Box::pin(async move {
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
    ) -> Result<(), LixError> {
        let mut writes = StorageWriteSet::new();
        runtime_functions
            .stage_persist_if_needed(&mut writes)
            .await?;
        if writes.is_empty() {
            return Ok(());
        }
        let mut transaction = self.storage.begin_write_transaction().await?;
        writes.apply(&mut transaction.as_mut()).await?;
        transaction.commit().await
    }
}

impl SessionTransaction {
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
        let statement = sql2::parse_statement(sql)?;
        let transaction = self.transaction_mut()?;
        match sql2::bind_statement_route(&statement)? {
            sql2::BoundStatementRoute::Write => {
                execute_transaction_write(transaction, statement, params)
                    .await
                    .map_err(|error| normalize_sql_surface_error(error, sql))
            }
            sql2::BoundStatementRoute::Read => {
                let plan = sql2::create_transaction_read_logical_plan_from_parsed(
                    transaction,
                    sql,
                    statement,
                )
                .await
                .map_err(|error| normalize_sql_surface_error(error, sql))?;
                let result = sql2::execute_logical_plan(plan, params)
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
        let transaction = self.transaction_mut()?;
        <crate::transaction::Transaction as sql2::SqlWriteExecutionContext>::scan_live_state(
            transaction,
            request,
        )
        .await
    }
}

async fn execute_transaction_write(
    transaction: &mut crate::transaction::Transaction,
    statement: datafusion::sql::parser::Statement,
    params: &[Value],
) -> Result<ExecuteResult, LixError> {
    execute_transaction_write_auto(transaction, statement, params).await
}

async fn execute_transaction_write_auto(
    transaction: &mut crate::transaction::Transaction,
    statement: datafusion::sql::parser::Statement,
    params: &[Value],
) -> Result<ExecuteResult, LixError> {
    let tx_plan = sql2::create_write_logical_plan_from_parsed(transaction, statement).await?;
    let affected_rows = sql2::execute_write_logical_plan(transaction, tx_plan, params).await?;
    Ok(ExecuteResult::from_rows_affected(affected_rows))
}

#[cfg(test)]
async fn execute_transaction_write_with_mode(
    transaction: &mut crate::transaction::Transaction,
    statement: datafusion::sql::parser::Statement,
    params: &[Value],
    mode: sql2::WriteExecutorMode,
) -> Result<ExecuteResult, LixError> {
    let tx_plan = sql2::create_write_logical_plan_from_parsed(transaction, statement).await?;
    let affected_rows =
        sql2::execute_write_logical_plan_with_mode(transaction, tx_plan, params, mode).await?;
    Ok(ExecuteResult::from_rows_affected(affected_rows))
}

#[cfg(test)]
async fn execute_transaction_write_with_mode_and_trace(
    transaction: &mut crate::transaction::Transaction,
    statement: datafusion::sql::parser::Statement,
    params: &[Value],
    mode: sql2::WriteExecutorMode,
) -> Result<(ExecuteResult, Option<sql2::WriteExecutorPath>), LixError> {
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
    if error.code == LixError::CODE_FOREIGN_KEY {
        let lower = error.message.to_ascii_lowercase();
        if lower.contains("schema 'lix_version_ref'") && lower.contains("target 'lix_commit.") {
            return LixError {
                code: LixError::CODE_VERSION_NOT_FOUND.to_string(),
                ..error
            };
        }
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
