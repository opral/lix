use std::sync::Arc;

use crate::backend::TransactionBeginMode;
use crate::engine2::functions::FunctionContext;
use crate::engine2::transaction::Transaction;
use crate::sql2;
use crate::{LixError, QueryResult, Value};

use super::context::{SessionContext, SessionSqlExecutionContext};

/// Result of executing one SQL statement through engine2.
///
/// Queries return row data. Mutations return the number of affected rows. This
/// keeps the consumer API row-oriented while allowing sql2/DataFusion to stay
/// batch-oriented internally.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecuteResult {
    Rows(RowSet),
    AffectedRows(u64),
}

/// Rows returned by a query.
///
/// Column names live once at the result-set level. Individual rows only own
/// values, which keeps the public API row-oriented without copying schema
/// metadata into every row.
#[derive(Debug, Clone, PartialEq)]
pub struct RowSet {
    columns: Vec<String>,
    rows: Vec<Row>,
}

impl RowSet {
    fn from_query_result(result: QueryResult) -> Self {
        Self {
            columns: result.columns,
            rows: result
                .rows
                .into_iter()
                .map(|values| Row { values })
                .collect(),
        }
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
}

/// Borrowed row view with access to the result-set column names.
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
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        let live_state: Arc<dyn crate::engine2::live_state::LiveStateReader> =
            Arc::new(self.live_state.reader(Arc::clone(&self.backend)));
        let runtime_functions = FunctionContext::prepare(live_state.as_ref()).await?;
        let functions = runtime_functions.provider();
        let active_version_id = self.active_version_id().await?;
        let visible_schemas = self
            .schema_registry
            .visible_schemas(live_state, &active_version_id)
            .await?;
        let ctx = SessionSqlExecutionContext {
            active_version_id: &active_version_id,
            backend: Arc::clone(&self.backend),
            live_state: Arc::clone(&self.live_state),
            binary_cas: Arc::clone(&self.binary_cas),
            changelog: Arc::clone(&self.changelog),
            version_ref: Arc::clone(&self.version_ref),
            visible_schemas,
            functions: functions.clone(),
        };

        let plan = sql2::create_logical_plan(&ctx, sql).await?;
        let result = if plan.is_write() {
            // Open an autocommit write transaction for this statement, execute
            // through a transaction-aware SQL context, then commit on success
            // or rollback on error.
            let transaction = Transaction::open(
                active_version_id.clone(),
                &self.backend,
                Arc::clone(&self.live_state),
                Arc::clone(&self.binary_cas),
                Arc::clone(&self.changelog),
                Arc::clone(&self.version_ref),
                Arc::clone(&self.schema_registry),
                functions,
            )
            .await?;
            // Re-plan against the transaction so DataFusion provider hooks
            // stage writes through the transaction-owned write stager.
            let tx_plan = sql2::create_logical_plan(&transaction, sql).await?;
            let result = sql2::execute_logical_plan(tx_plan, params).await;
            match result {
                Ok(result) => {
                    let affected_rows = affected_rows_from_query_result(result)?;
                    transaction.commit(&runtime_functions).await?;
                    return Ok(ExecuteResult::AffectedRows(affected_rows));
                }
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            }
        } else {
            sql2::execute_logical_plan(plan, params).await?
        };
        self.persist_runtime_functions_if_needed(&runtime_functions)
            .await?;
        Ok(ExecuteResult::Rows(RowSet::from_query_result(result)))
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
        let mut transaction = self
            .backend
            .begin_transaction(TransactionBeginMode::Write)
            .await?;
        runtime_functions
            .persist_if_needed(&mut self.live_state.writer(transaction.as_mut()))
            .await?;
        transaction.commit().await
    }
}

fn affected_rows_from_query_result(result: QueryResult) -> Result<u64, LixError> {
    let Some(first_row) = result.rows.first() else {
        return Ok(0);
    };
    let Some(first_value) = first_row.first() else {
        return Ok(0);
    };
    match first_value {
        Value::Integer(value) if *value >= 0 => Ok(*value as u64),
        Value::Text(value) => value.parse::<u64>().map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to parse affected row count from SQL result: {error}"),
            )
        }),
        other => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("expected affected row count, got {other:?}"),
        )),
    }
}
