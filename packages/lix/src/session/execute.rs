use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::ControlFlow;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use crate::branch::{BranchRefReader, BranchRefStoreReader};
use crate::common::ExecuteStatementMetadata;
use crate::functions::{FunctionContext, FunctionProviderHandle};
use crate::sql_telemetry::{SqlStatementTelemetry, finish_operation, start_batch};
use crate::sql2;
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    SharedStorageAdapterRead, StorageAdapter, StorageAdapterReadScope, StorageReadDurability,
    StorageReadOptions, StorageWriteOptions,
};
use crate::telemetry::TelemetrySpanKind;
use crate::transaction::{begin_commit_boundary, commit_at_boundary};
use crate::{Blob, LixError, LixNotice, SqlQueryResult, Value};
use datafusion::arrow::array::{ArrayRef, LargeStringBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Ident, LimitClause, OrderByKind, Query, Select,
    SelectFlavor, SelectItem, SetExpr, Statement as SqlStatement, TableAlias, TableFactor,
    Value as SqlValue, Visit, Visitor,
};
#[cfg(feature = "storage-benches")]
use futures_util::TryStreamExt;
use serde_json::{Map as JsonMap, Value as JsonValue};
use tracing::Instrument as _;

use super::ExecuteIdempotency;
use super::context::{SessionContext, SessionSqlExecutionContext};
use super::idempotency::{ExecuteIdempotencyReceipt, load_receipt};
use super::transaction::SessionTransaction;
use crate::PreparedDmlParameterBatch;

const MAX_INITIAL_LITERAL_COLUMN_BYTES: usize = 64 * 1024 * 1024;

enum LiteralParameterBuilder {
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
}

impl LiteralParameterBuilder {
    fn with_capacity(large_offsets: bool, item_capacity: usize, data_capacity: usize) -> Self {
        if large_offsets {
            Self::LargeUtf8(LargeStringBuilder::with_capacity(
                item_capacity,
                data_capacity,
            ))
        } else {
            Self::Utf8(StringBuilder::with_capacity(item_capacity, data_capacity))
        }
    }

    fn append_value(&mut self, value: &str) {
        match self {
            Self::Utf8(builder) => builder.append_value(value),
            Self::LargeUtf8(builder) => builder.append_value(value),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::LargeUtf8(builder) => Arc::new(builder.finish()),
        }
    }
}

/// Result of executing one SQL statement through engine.
///
/// Column names live once at the result-set level. Individual rows only own
/// values, which keeps the public API row-oriented without copying schema
/// metadata into every row. Result storage is immutable and reference counted
/// so observation fanout does not copy large blob values per subscriber.
#[derive(Debug, Clone)]
pub struct ExecuteResult {
    statement_index: Option<usize>,
    statement_label: Option<String>,
    /// Mutation results without RETURNING carry no row backing. Keeping the
    /// empty case inline avoids one Arc clone/drop pair for every scalar write.
    backing: Option<Arc<ExecuteResultBacking>>,
    rows_affected: u64,
}

#[derive(Debug)]
struct ExecuteResultBacking {
    columns: Arc<[String]>,
    rows: OnceLock<Vec<Row>>,
    columnar: Option<ColumnarResult>,
    notices: Vec<LixNotice>,
    // Observe evaluations can be shared across sessions. Carry the exact
    // rendered plugin state with the rows so each receiving session can
    // acknowledge it only when `ObserveEvents::next()` delivers the event.
    file_view_mutations: Vec<sql2::SessionFileViewMutation>,
}

#[derive(Debug)]
struct ColumnarResult {
    fields: Vec<Field>,
    batches: Arc<[RecordBatch]>,
}

impl PartialEq for ExecuteResult {
    fn eq(&self, other: &Self) -> bool {
        self.statement_index == other.statement_index
            && self.statement_label == other.statement_label
            && self.rows_affected == other.rows_affected
            && (matches!(
                (&self.backing, &other.backing),
                (Some(left), Some(right)) if Arc::ptr_eq(left, right)
            ) || (self.columns() == other.columns()
                && self.rows() == other.rows()
                && self.notices() == other.notices()))
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

/// One materialized file read and the logical range it represents.
///
/// The result shape is independent of how the bytes are stored. Large-file
/// backends can therefore satisfy a bounded range without changing the
/// session API or exposing CAS chunks to callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRead {
    content: Blob,
    total_size: u64,
    range: Range<u64>,
    content_identity: String,
}

impl FileRead {
    pub fn content(&self) -> &Blob {
        &self.content
    }

    pub fn into_content(self) -> Blob {
        self.content
    }

    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    pub fn range(&self) -> Range<u64> {
        self.range.clone()
    }

    pub fn content_identity(&self) -> &str {
        &self.content_identity
    }
}

impl ExecuteResult {
    pub fn statement_index(&self) -> Option<usize> {
        self.statement_index
    }

    pub fn label(&self) -> Option<&str> {
        self.statement_label.as_deref()
    }

    fn with_batch_metadata(mut self, statement_index: usize, label: Option<String>) -> Self {
        self.statement_index = Some(statement_index);
        self.statement_label = label;
        self
    }

    pub(crate) fn from_session_read_result(result: sql2::SessionReadSqlResult) -> Self {
        match result.query {
            sql2::SessionReadResult::Rows(result) => Self::from_sql_query_result(result),
            sql2::SessionReadResult::Columnar {
                fields,
                batches,
                notices,
            } => Self::from_columnar_result(fields, batches, notices),
        }
    }

    fn from_sql_query_result(result: SqlQueryResult) -> Self {
        #[cfg(feature = "storage-benches")]
        let started = crate::sql_profile::is_active().then(std::time::Instant::now);
        let result = Self::from_query_parts(result.columns, result.rows, 0, result.notices);
        #[cfg(feature = "storage-benches")]
        if let Some(started) = started {
            crate::sql_profile::record_phase(
                crate::sql_profile::Phase::PublicResultMaterialization,
                started.elapsed(),
            );
        }
        result
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
            statement_index: None,
            statement_label: None,
            backing: None,
            rows_affected,
        }
    }

    pub fn from_rows(columns: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        Self::from_query_parts(columns, rows, 0, Vec::new())
    }

    pub(crate) fn from_idempotency_parts(
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
        rows_affected: u64,
        notices: Vec<LixNotice>,
    ) -> Self {
        Self::from_query_parts(columns, rows, rows_affected, notices)
    }

    fn from_query_parts(
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
        rows_affected: u64,
        notices: Vec<LixNotice>,
    ) -> Self {
        let columns: Arc<[String]> = columns.into();
        let rows: Vec<Row> = rows
            .into_iter()
            .map(|values| Row {
                columns: Arc::clone(&columns),
                values,
            })
            .collect();
        Self {
            statement_index: None,
            statement_label: None,
            backing: Some(Arc::new(ExecuteResultBacking {
                columns,
                rows: OnceLock::from(rows),
                columnar: None,
                notices,
                file_view_mutations: Vec::new(),
            })),
            rows_affected,
        }
    }

    fn from_columnar_result(
        fields: Vec<Field>,
        batches: Arc<[RecordBatch]>,
        notices: Vec<LixNotice>,
    ) -> Self {
        let columns = fields
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
            .into();
        Self {
            statement_index: None,
            statement_label: None,
            backing: Some(Arc::new(ExecuteResultBacking {
                columns,
                rows: OnceLock::new(),
                columnar: Some(ColumnarResult { fields, batches }),
                notices,
                file_view_mutations: Vec::new(),
            })),
            rows_affected: 0,
        }
    }

    fn with_file_view_mutations(mut self, mutations: Vec<sql2::SessionFileViewMutation>) -> Self {
        let backing = self.backing.get_or_insert_with(|| {
            Arc::new(ExecuteResultBacking {
                columns: Vec::new().into(),
                rows: OnceLock::from(Vec::new()),
                columnar: None,
                notices: Vec::new(),
                file_view_mutations: Vec::new(),
            })
        });
        Arc::get_mut(backing)
            .expect("fresh execute result backing must be uniquely owned")
            .file_view_mutations = mutations;
        self
    }

    pub(crate) fn file_view_mutations(&self) -> &[sql2::SessionFileViewMutation] {
        self.backing
            .as_deref()
            .map_or(&[], |backing| backing.file_view_mutations.as_slice())
    }

    /// Returns the result-set column names in row value order.
    pub fn columns(&self) -> &[String] {
        self.backing
            .as_deref()
            .map_or(&[], |backing| backing.columns.as_ref())
    }

    /// Returns the owned rows. Use `iter()` for name-based access.
    pub fn rows(&self) -> &[Row] {
        self.backing.as_deref().map_or(&[], |backing| {
            backing
                .rows
                .get_or_init(|| backing.materialize_rows())
                .as_slice()
        })
    }

    /// Iterates rows with borrowed access to the shared column metadata.
    pub fn iter(&self) -> impl Iterator<Item = RowRef<'_>> {
        let columns = self.columns();
        self.rows().iter().map(move |row| RowRef {
            columns,
            values: row.values.as_slice(),
        })
    }

    /// Returns the number of rows in this result set.
    pub fn len(&self) -> usize {
        self.rows().len()
    }

    /// Returns true when this result set has no rows.
    pub fn is_empty(&self) -> bool {
        self.rows().is_empty()
    }

    /// Returns the number of rows affected by a mutation statement.
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    /// Returns non-fatal diagnostics produced while executing the statement.
    pub fn notices(&self) -> &[LixNotice] {
        self.backing
            .as_deref()
            .map_or(&[], |backing| backing.notices.as_slice())
    }

    /// Looks up the value for `column_name` on an owned row from this set.
    pub fn get<'a>(&self, row: &'a Row, column_name: &str) -> Option<&'a Value> {
        let index = self.column_index(column_name)?;
        row.get_index(index)
    }

    /// Returns the index for a column name.
    pub fn column_index(&self, column_name: &str) -> Option<usize> {
        self.columns()
            .iter()
            .position(|column| column == column_name)
    }
}

impl ExecuteResultBacking {
    fn materialize_rows(&self) -> Vec<Row> {
        let Some(columnar) = &self.columnar else {
            return Vec::new();
        };
        #[cfg(feature = "storage-benches")]
        let started = crate::sql_profile::is_active().then(std::time::Instant::now);
        let result = sql2::query_result_from_batches(&columnar.fields, &columnar.batches)
            .expect("columnar result was validated before public ownership transfer");
        #[cfg(feature = "storage-benches")]
        if let Some(started) = started {
            crate::sql_profile::record_phase(
                crate::sql_profile::Phase::PublicResultMaterialization,
                started.elapsed(),
            );
        }
        result
            .rows
            .into_iter()
            .map(|values| Row {
                columns: Arc::clone(&self.columns),
                values,
            })
            .collect()
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
            Value::Json(value) => Ok(value.clone().into()),
            other => Err(value_type_error("json", other)),
        }
    }
}

impl TryFromValue for Vec<u8> {
    fn try_from_value(value: &Value) -> Result<Self, LixError> {
        match value {
            Value::Blob(value) => Ok(value.to_vec()),
            other => Err(value_type_error("blob", other)),
        }
    }
}

impl TryFromValue for Blob {
    fn try_from_value(value: &Value) -> Result<Self, LixError> {
        match value {
            Value::Blob(value) => Ok(value.clone()),
            other => Err(value_type_error("blob", other)),
        }
    }
}

impl TryFromValue for bytes::Bytes {
    fn try_from_value(value: &Value) -> Result<Self, LixError> {
        Blob::try_from_value(value).map(Blob::into_bytes)
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

/// Whether abandoning an in-flight SQL execution can discard its work.
///
/// This is derived from Lix's parsed and bound statement route, rather than
/// from SQL-text matching. A [`Self::CancellableRead`] has no durable side
/// effects. [`Self::Durable`] covers writes and read-shaped statements that
/// invoke a runtime function whose state is persisted after evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionDisposition {
    /// A pure read that a transport may cancel when its caller disconnects.
    CancellableRead,
    /// An operation whose completion must outlive a cancelled transport.
    Durable,
}

/// One SQL statement to execute as part of an atomic batch.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecuteBatchStatement {
    pub sql: String,
    pub params: Vec<Value>,
    /// Opaque caller metadata echoed by the corresponding batch result.
    /// Labels need not be unique; `statement_index` is the unique identity.
    pub label: Option<String>,
}

fn annotate_batch_results(
    statements: &[ExecuteBatchStatement],
    results: Vec<ExecuteResult>,
) -> Result<Vec<ExecuteResult>, LixError> {
    if results.len() != statements.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "execute batch produced a result count different from its statement count",
        )
        .with_details(serde_json::json!({
            "statementCount": statements.len(),
            "resultCount": results.len(),
        })));
    }
    Ok(results
        .into_iter()
        .enumerate()
        .map(|(statement_index, result)| {
            result.with_batch_metadata(statement_index, statements[statement_index].label.clone())
        })
        .collect())
}

enum ExecuteBatchExecution {
    ReadOnly(Vec<datafusion::sql::parser::Statement>),
    Transaction(TransactionBatchStatements),
}

enum TransactionBatchStatements {
    Shared {
        statement: datafusion::sql::parser::Statement,
        len: usize,
    },
    AutoParameterizedUpdate {
        sql: Arc<str>,
        statement: datafusion::sql::parser::Statement,
        parameter_batch: RecordBatch,
    },
    Distinct(Vec<datafusion::sql::parser::Statement>),
}

impl TransactionBatchStatements {
    fn len(&self) -> usize {
        match self {
            Self::Shared { len, .. } => *len,
            Self::AutoParameterizedUpdate {
                parameter_batch, ..
            } => parameter_batch.num_rows(),
            Self::Distinct(statements) => statements.len(),
        }
    }

    fn contains_write(&self) -> Result<bool, LixError> {
        match self {
            Self::Shared { statement, .. } => {
                Ok(sql2::bind_statement_route(statement)? == sql2::BoundStatementRoute::Write)
            }
            Self::AutoParameterizedUpdate { .. } => Ok(true),
            Self::Distinct(statements) => {
                statements
                    .iter()
                    .try_fold(false, |contains_write, statement| {
                        Ok(contains_write
                            || sql2::bind_statement_route(statement)?
                                == sql2::BoundStatementRoute::Write)
                    })
            }
        }
    }

    fn into_vec(self) -> Vec<datafusion::sql::parser::Statement> {
        match self {
            Self::Shared { statement, len } => vec![statement; len],
            Self::AutoParameterizedUpdate {
                statement,
                parameter_batch,
                ..
            } => vec![statement; parameter_batch.num_rows()],
            Self::Distinct(statements) => statements,
        }
    }
}

enum IdempotencyReceiptResolution {
    Absent,
    Replay(ExecuteIdempotencyReceipt),
}

fn validate_native_file_upsert_batch(writes: &[(String, Blob)]) -> Result<(), LixError> {
    if writes.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "upsert_file_content_batch requires at least one file",
        )
        .with_details(serde_json::json!({
            "operation": "upsertFileContentBatch",
            "argument": "writes",
            "expected": "non-empty array",
        })));
    }

    let mut paths = BTreeSet::new();
    for (path, _) in writes {
        // Preserve the public filesystem path contract before entering a
        // write transaction. The lower-level fast helper maps this validation
        // through DataFusion for SQL callers; this native surface should keep
        // its specific path errors intact.
        crate::common::LixPath::try_from_file_path(path)?;
        if !paths.insert(path.as_str()) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("upsert_file_content_batch contains duplicate path '{path}'"),
            )
            .with_details(serde_json::json!({
                "operation": "upsertFileContentBatch",
                "argument": "writes",
                "path": path,
                "expected": "unique file paths",
            })));
        }
    }

    Ok(())
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Classifies one SQL execution for a caller that owns its transport
    /// lifecycle.
    ///
    /// The classification is intentionally based on the parsed statement and
    /// Lix's bound route. It does not use SQL-text heuristics, and callers
    /// must still execute the statement to receive the normal validation and
    /// query-planning errors.
    pub fn execution_disposition(&self, sql: &str) -> Result<ExecutionDisposition, LixError> {
        let statement = self.sql_planning_cache.parse_statement(sql)?;
        execution_disposition(&statement)
    }

    /// Classifies an atomic SQL batch for a caller that owns its transport
    /// lifecycle.
    ///
    /// A batch is cancellable only when every parsed and bound statement is a
    /// pure read. Any durable statement makes the whole batch durable so its
    /// atomic transaction can complete after a transport disconnects.
    pub fn execute_batch_disposition(
        &self,
        statements: &[ExecuteBatchStatement],
    ) -> Result<ExecutionDisposition, LixError> {
        for (statement_index, statement) in statements.iter().enumerate() {
            let parsed = self
                .sql_planning_cache
                .parse_statement(&statement.sql)
                .map_err(|error| with_batch_statement_index(error, statement_index))?;
            if execution_disposition(&parsed)
                .map_err(|error| with_batch_statement_index(error, statement_index))?
                == ExecutionDisposition::Durable
            {
                return Ok(ExecutionDisposition::Durable);
            }
        }
        Ok(ExecutionDisposition::CancellableRead)
    }

    /// Executes one DataFusion SQL statement against this Lix session.
    ///
    /// The SQL dialect is DataFusion SQL, not SQLite SQL. Positional
    /// placeholders use `?` or `$1`, `$2`, and so on. SQLite-specific catalog tables
    /// and transaction statements such as `sqlite_master`, `BEGIN`, and
    /// `COMMIT` are not part of this contract; use `information_schema` for
    /// catalog inspection. Lix owns transaction boundaries for each statement.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        Box::pin(self.execute_with_options(sql, params, ExecuteOptions::default())).await
    }

    /// Executes one statement and reports neutral columnar phase timings.
    ///
    /// This diagnostic API is available only to storage benchmarks. It uses
    /// the normal public execution path and does not alter query semantics.
    #[cfg(feature = "storage-benches")]
    pub async fn execute_profiled(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<(ExecuteResult, crate::SqlReadProfile), LixError> {
        let (result, profile) = crate::sql_profile::scope(self.execute(sql, params)).await;
        result.map(|result| (result, profile))
    }

    /// Benchmark-only comparison of the eager result path with internal
    /// collected-batch and live-batch consumers. No stream escapes the scoped
    /// storage read, and this does not change the public execution contract.
    #[cfg(feature = "storage-benches")]
    #[doc(hidden)]
    pub async fn execute_result_streaming_profiled(
        &self,
        sql: &str,
        params: &[Value],
        mode: &str,
        row_limit: Option<usize>,
    ) -> Result<crate::SqlReadProfile, LixError> {
        let (result, profile) = crate::sql_profile::scope(async {
            if mode == "full" {
                let result = self.execute(sql, params).await?;
                let rows = result.rows();
                let consumed = row_limit.map_or(rows.len(), |limit| limit.min(rows.len()));
                let checksum = rows.iter().take(consumed).try_fold(0u64, |checksum, row| {
                    profile_result_checksum(checksum, row.values())
                })?;
                crate::sql_profile::record_result_rows(consumed, rows.len(), rows.len());
                crate::sql_profile::record_result_checksum(checksum);
                return Ok(());
            }

            if !matches!(mode, "stream" | "live" | "count_only") {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("unknown result streaming profile mode '{mode}'"),
                ));
            }

            self.ensure_open()?;
            let statement = self.sql_planning_cache.parse_statement(sql)?;
            if sql2::bind_statement_route(&statement)? != sql2::BoundStatementRoute::Read
                || sql2::statement_has_durable_runtime_function(&statement)
                || exact_filesystem_read_route(&statement, params).is_some()
                || late_materialized_lix_file_content_read(&statement).is_some()
            {
                return Err(LixError::new(
                    LixError::CODE_UNSUPPORTED_SQL,
                    "result streaming profiler accepts only ordinary cancellable reads",
                ));
            }

            let _operation_guard = self.begin_waitable_session_operation().await?;
            let read_scope = self
                .storage
                .begin_read(StorageReadOptions::default())
                .await?;
            with_static_session_sql_read::<StorageImpl, _, _>(
                read_scope,
                |read_store: SharedStorageAdapterRead<StorageImpl::Read<'static>>| async move {
                    let active_branch_id = self.active_branch_id_from_reader(&read_store).await?;
                    let forktree = crate::forktree::ForkTreeReadFacade::new(read_store.clone());
                    let state_view = crate::state::ForkTreeStateView::from_facade(
                        forktree.clone(),
                        &active_branch_id,
                    )
                    .await?;
                    let ctx = SessionSqlExecutionContext {
                        active_branch_id: &active_branch_id,
                        active_account_id: self.active_account_id(),
                        read_store,
                        forktree,
                        state_view,
                        catalog_context: Arc::clone(&self.catalog_context),
                        sql_planning_cache: Arc::clone(&self.sql_planning_cache),
                        functions: FunctionProviderHandle::system(),
                        plugin_host: self.plugin_host.clone(),
                        file_views: None,
                    };
                    let read_session =
                        sql2::prepare_read_session(&ctx, std::slice::from_ref(&statement)).await?;

                    match mode {
                        "stream" => {
                            let result =
                                sql2::execute_read_statement_in_session_with_collected_batches(
                                    &read_session,
                                    sql,
                                    statement,
                                    params,
                                )
                                .await?;
                            let _notice_count = result.notices.len();
                            let mut cursor =
                                sql2::BatchRowCursor::collected(&result.fields, &result.batches);
                            consume_profile_cursor(&mut cursor, row_limit).await?;
                        }
                        "live" => {
                            let mut result =
                                sql2::execute_read_statement_in_session_with_batch_stream(
                                    &read_session,
                                    sql,
                                    statement,
                                    params,
                                )
                                .await?;
                            let _notice_count = result.notices.len();
                            let mut cursor = sql2::BatchRowCursor::live(&mut result);
                            consume_profile_cursor(&mut cursor, row_limit).await?;
                            drop(cursor);
                            drop(result);
                        }
                        "count_only" => {
                            let mut result =
                                sql2::execute_read_statement_in_session_with_batch_stream(
                                    &read_session,
                                    sql,
                                    statement,
                                    params,
                                )
                                .await?;
                            let _notice_count = result.notices.len();
                            let mut rows = 0usize;
                            let mut batches = 0usize;
                            while let Some(batch) = {
                                let started = std::time::Instant::now();
                                let batch = result
                                    .stream
                                    .try_next()
                                    .await
                                    .map_err(sql2::datafusion_error_to_lix_error);
                                crate::sql_profile::record_phase(
                                    crate::sql_profile::Phase::ArrowExecution,
                                    started.elapsed(),
                                );
                                batch?
                            } {
                                rows = rows.saturating_add(batch.num_rows());
                                batches = batches.saturating_add(1);
                            }
                            crate::sql_profile::record_result_count_only(rows, batches);
                            crate::sql_profile::record_result_rows(rows, 0, 0);
                            drop(result);
                        }
                        _ => unreachable!("profile mode validated before opening read"),
                    }
                    drop(read_session);
                    drop(ctx);
                    Ok(())
                },
            )
            .await
        })
        .await;
        result?;
        Ok(profile)
    }

    pub async fn execute_with_options(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        Box::pin(self.execute_with_options_and_metadata(
            sql,
            params,
            options,
            ExecuteStatementMetadata::default(),
        ))
        .await
    }

    #[doc(hidden)]
    pub async fn execute_with_options_and_metadata(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
        metadata: ExecuteStatementMetadata,
    ) -> Result<ExecuteResult, LixError> {
        validate_execute_statement_metadata(params.len(), &metadata, None)?;
        Box::pin(self.execute_with_kind(sql, params, options, metadata, "execute", None, false))
            .await
    }

    /// Executes a protocol SQL request with durable replay for write routes.
    ///
    /// This is intentionally separate from the general engine execution API:
    /// the protocol makes a clean hard cut requiring an idempotency key for
    /// SQL writes, while in-process callers may continue to own their own
    /// transaction/retry contract. Read routes ignore a supplied key.
    #[doc(hidden)]
    pub fn execute_with_idempotency_and_options_and_metadata(
        self: Arc<Self>,
        sql: String,
        params: Vec<Value>,
        options: ExecuteOptions,
        metadata: ExecuteStatementMetadata,
        idempotency: Option<ExecuteIdempotency>,
    ) -> impl Future<Output = Result<ExecuteResult, LixError>> + Send + 'static {
        // SAFETY: the future owns its Arc session and request payload. Every
        // storage read/write handle is Send by the Storage contract; remaining
        // compiler failures are higher-ranked shared references to Sync data.
        unsafe {
            super::AssumeSendFuture::new(async move {
                validate_execute_statement_metadata(params.len(), &metadata, None)?;
                self.execute_with_kind(
                    &sql,
                    &params,
                    options,
                    metadata,
                    "execute",
                    idempotency,
                    true,
                )
                .await
            })
        }
    }

    /// Upserts one file's bytes by its full logical path without constructing
    /// a SQL parser or DataFusion plan for normal filesystem layouts.
    ///
    /// This stays separate from the batch API so the established one-file
    /// transfer path does not pay for batch-vector allocation or duplicate
    /// validation.
    pub async fn upsert_file_content(&self, path: String, content: Blob) -> Result<u64, LixError> {
        self.ensure_open()?;
        // Preserve the public filesystem path contract before entering a
        // write transaction. The lower-level fast helper maps this validation
        // through DataFusion for SQL callers; this native surface should keep
        // its specific path errors intact.
        crate::common::LixPath::try_from_file_path(&path)?;
        let write_access = self.begin_session_write_access().await?;
        self.with_write_transaction_reserved_lending(
            write_access,
            async move |transaction| {
                sql2::execute_fast_lix_file_path_writes(
                    transaction,
                    vec![(path, content, None, None)],
                    sql2::FastLixFilePathWriteConflict::UpdateContent,
                    None,
                )
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        "upsert_file_content requires a filesystem layout that its direct path index can route unambiguously",
                    )
                    .with_details(serde_json::json!({
                        "operation": "upsertFileContent",
                        "expected": "a filesystem layout that the direct path index can route unambiguously",
                    }))
                })
            },
            |_| Ok(()),
        )
        .await
    }

    /// Upserts a non-empty batch of file bytes in one transaction.
    ///
    /// Paths are validated and required to be unique before a transaction is
    /// opened. This is deliberately a direct filesystem write: if the path
    /// index cannot route an exceptional layout unambiguously, the batch is
    /// rejected instead of copying the complete payload into a SQL fallback.
    pub async fn upsert_file_content_batch(
        &self,
        writes: Vec<(String, Blob)>,
    ) -> Result<u64, LixError> {
        self.ensure_open()?;
        validate_native_file_upsert_batch(&writes)?;
        let write_access = self.begin_session_write_access().await?;
        self.with_write_transaction_reserved_lending(write_access, async move |transaction| {
                sql2::execute_fast_lix_file_path_writes(
                    transaction,
                    writes
                        .into_iter()
                        .map(|(path, content)| (path, content, None, None))
                        .collect(),
                    sql2::FastLixFilePathWriteConflict::UpdateContent,
                    None,
                )
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        "upsert_file_content_batch requires a filesystem layout that its direct path index can route unambiguously",
                    )
                    .with_details(serde_json::json!({
                        "operation": "upsertFileContentBatch",
                        "expected": "a filesystem layout that the direct path index can route unambiguously",
                    }))
                })
        }, |_| Ok(()))
        .await
    }

    /// Reads one file's bytes by its full logical path without parsing SQL.
    ///
    /// This is the structured file-transfer read path. It uses the same
    /// active-branch file selection, plugin rendering, and session file-view
    /// acknowledgement as `SELECT content FROM lix_file WHERE path = $1`, while
    /// avoiding SQL parsing, planning, and a JSON-shaped result envelope.
    pub fn read_file_content(
        &self,
        path: String,
        requested_range: Option<Range<u64>>,
    ) -> impl Future<Output = Result<Option<FileRead>, LixError>> + Send + '_ {
        // SAFETY: the read future owns its path/range and retains only shared
        // references to this Sync session. Rustc cannot prove the Send bound
        // through the higher-ranked scoped SQL-read closure.
        unsafe { super::AssumeSendFuture::new(self.read_file_content_inner(path, requested_range)) }
    }

    async fn read_file_content_inner(
        &self,
        path: String,
        requested_range: Option<Range<u64>>,
    ) -> Result<Option<FileRead>, LixError> {
        self.ensure_open()?;
        // Keep the structured API's path contract aligned with native writes.
        // SQL remains available for callers that intentionally need broader
        // `lix_file` predicates or legacy path shapes.
        crate::common::LixPath::try_from_file_path(&path)?;

        let paths = BTreeSet::from([path]);
        let _operation_guard = self.begin_waitable_session_operation().await?;
        let read_scope = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await?;
        let (content, file_view_mutations) = with_static_session_sql_read::<StorageImpl, _, _>(
            read_scope,
            |read_store: SharedStorageAdapterRead<StorageImpl::Read<'static>>| async move {
                let active_branch_id = self.active_branch_id_from_reader(&read_store).await?;
                let forktree = crate::forktree::ForkTreeReadFacade::new(read_store.clone());
                let state_view = crate::state::ForkTreeStateView::from_facade(
                    forktree.clone(),
                    &active_branch_id,
                )
                .await?;
                let filesystem_path_index: Arc<dyn crate::filesystem::FilesystemPathIndexReader> =
                    Arc::new(crate::filesystem::ForkTreeFilesystemPathIndexReader::new(
                        forktree.clone(),
                    ));
                let branch_ref: Arc<dyn BranchRefReader> =
                    Arc::new(BranchRefStoreReader::new(read_store.clone()));
                let authenticated_blob_reader: Arc<dyn crate::forktree::AuthenticatedBlobReader> =
                    Arc::new(crate::forktree::blob_reader_on_read(
                        read_store.clone(),
                        &active_branch_id,
                    )?);
                // A raw file download delivers the same bytes as a direct
                // `lix_file.content` read, so it must acknowledge rendered
                // plugin state for subsequent collaborative writes.
                let file_view_collector = sql2::SessionFileViews::default();
                let result = sql2::execute_exact_lix_file_batch_read(
                    &active_branch_id,
                    state_view,
                    filesystem_path_index,
                    branch_ref,
                    authenticated_blob_reader,
                    self.plugin_host.clone(),
                    Some(file_view_collector.clone()),
                    &paths,
                    requested_range.clone(),
                )
                .await?;
                let content = native_file_read_from_exact_result(result, &paths, requested_range)?;
                Ok((content, file_view_collector.plugin_file_mutations()))
            },
        )
        .await?;
        self.file_views.apply_mutations(file_view_mutations);
        Ok(content)
    }

    pub(crate) async fn execute_for_observe(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        self.execute_with_kind(
            sql,
            params,
            ExecuteOptions::default(),
            ExecuteStatementMetadata::default(),
            "observe",
            None,
            false,
        )
        .await
    }

    async fn execute_with_kind(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
        metadata: ExecuteStatementMetadata,
        execution_kind: &'static str,
        idempotency: Option<ExecuteIdempotency>,
        require_idempotency_for_writes: bool,
    ) -> Result<ExecuteResult, LixError> {
        let telemetry =
            SqlStatementTelemetry::start(self.telemetry.as_ref(), sql, execution_kind, None);
        let operation = self.execute_with_options_inner(
            sql,
            params,
            options,
            metadata,
            execution_kind == "observe",
            idempotency,
            require_idempotency_for_writes,
        );
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
        metadata: ExecuteStatementMetadata,
        defer_file_view_acknowledgement: bool,
        idempotency: Option<ExecuteIdempotency>,
        require_idempotency_for_writes: bool,
    ) -> Result<ExecuteResult, LixError> {
        self.ensure_open()?;
        let statement = self.sql_planning_cache.parse_statement(sql)?;
        if sql2::bind_statement_route(&statement)? == sql2::BoundStatementRoute::Write {
            if require_idempotency_for_writes && idempotency.is_none() {
                return Err(LixError::new(
                    LixError::CODE_IDEMPOTENCY_KEY_REQUIRED,
                    "Idempotency-Key is required for SQL mutations",
                ));
            }
            if let Some(idempotency) = idempotency {
                return self
                    .execute_idempotent_write(
                        sql,
                        statement,
                        params,
                        options,
                        metadata,
                        idempotency,
                    )
                    .await;
            }
            let write_access = self.begin_session_write_access().await?;
            let sql_for_error = sql.to_string();
            let sql_for_planning = sql_for_error.clone();
            let params = params.to_vec();
            return self
                .with_write_transaction_reserved_lending(
                    write_access,
                    async move |transaction| {
                        let previous_origin_key =
                            transaction.replace_origin_key(options.origin_key);
                        let result = async {
                            let tx_plan = transaction
                                .prepare_sql_write_logical_plan(&sql_for_planning, &statement)?;
                            let result = execute_prepared_transaction_write(
                                transaction,
                                tx_plan,
                                &params,
                                &metadata,
                            )
                            .await?;
                            Ok(ExecuteResult::from_sql_write_result(result))
                        }
                        .await;
                        transaction.replace_origin_key(previous_origin_key);
                        result
                    },
                    |_| Ok(()),
                )
                .await
                .map_err(|error| normalize_sql_surface_error(error, &sql_for_error));
        }

        let exact_filesystem_read = exact_filesystem_read_route(&statement, params);
        let late_file_content_read = exact_filesystem_read
            .is_none()
            .then(|| late_materialized_lix_file_content_read(&statement))
            .flatten();
        let acknowledge_file_views = is_acknowledgeable_file_content_read(&statement, params)
            || matches!(
                &exact_filesystem_read,
                Some(ExactFilesystemRead::PathContentBatch(_))
            )
            || late_file_content_read.is_some();
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
        let read_result = with_static_session_sql_read::<StorageImpl, _, _>(
            read_scope,
            |read_store: SharedStorageAdapterRead<StorageImpl::Read<'static>>| async move {
                self.execute_read_statement_with_store(
                    read_store,
                    sql,
                    statement,
                    params,
                    acknowledge_file_views,
                    exact_filesystem_read,
                    late_file_content_read,
                    has_durable_runtime_function,
                )
                .await
            },
        );
        let (mut read_result, file_view_mutations) = match read_result.await {
            Ok(result) => result,
            Err(error) => {
                return Err(normalize_sql_surface_error(error, sql));
            }
        };
        let runtime_storage_stats = match read_result.runtime_functions.take() {
            Some(runtime_functions) => {
                self.persist_runtime_functions_if_needed(
                    runtime_functions,
                    runtime_write_access.is_some(),
                )
                .await?
            }
            None => None,
        };
        drop(runtime_write_access);
        if let Some(stats) = runtime_storage_stats {
            self.observe_invalidation.bump_if_storage_changed(&stats);
        }
        let result = ExecuteResult::from_session_read_result(read_result)
            .with_file_view_mutations(file_view_mutations);
        if !defer_file_view_acknowledgement {
            self.file_views
                .apply_mutations(result.file_view_mutations().iter().cloned());
        }
        Ok(result)
    }

    async fn execute_idempotent_write(
        &self,
        sql: &str,
        statement: DataFusionStatement,
        params: &[Value],
        options: ExecuteOptions,
        metadata: ExecuteStatementMetadata,
        idempotency: ExecuteIdempotency,
    ) -> Result<ExecuteResult, LixError> {
        if let IdempotencyReceiptResolution::Replay(receipt) =
            self.resolve_idempotency_receipt(&idempotency).await?
        {
            return receipt.into_single_result();
        }

        let write_access = self.begin_session_write_access().await?;
        let sql_for_error = sql.to_string();
        let sql_for_planning = sql_for_error.clone();
        let params = params.to_vec();
        // The original identity is retained for post-commit recovery. The
        // transaction closure owns its own copy because its future may outlive
        // this call's immediate stack frame while the write lease is held.
        let idempotency_for_commit = idempotency.clone();
        let result = self
            .with_write_transaction_reserved_lending(
                write_access,
                async move |transaction| {
                    let previous_origin_key = transaction.replace_origin_key(options.origin_key);
                    let result = async {
                        let tx_plan = transaction
                            .prepare_sql_write_logical_plan(&sql_for_planning, &statement)?;
                        let result = execute_prepared_transaction_write(
                            transaction,
                            tx_plan,
                            &params,
                            &metadata,
                        )
                        .await?;
                        let result = ExecuteResult::from_sql_write_result(result);
                        let receipt =
                            ExecuteIdempotencyReceipt::single(&idempotency_for_commit, &result)?;
                        transaction
                            .stage_execute_idempotency_receipt(&idempotency_for_commit, &receipt)?;
                        Ok(result)
                    }
                    .await;
                    transaction.replace_origin_key(previous_origin_key);
                    result
                },
                |_| Ok(()),
            )
            .await
            .map_err(|error| normalize_sql_surface_error(error, &sql_for_error));

        match result {
            Ok(result) => Ok(result),
            Err(error)
                if matches!(
                    error.code.as_str(),
                    LixError::CODE_TRANSACTION_CONFLICT
                        | LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN
                ) =>
            {
                match self.resolve_idempotency_receipt(&idempotency).await {
                    Ok(IdempotencyReceiptResolution::Replay(receipt)) => {
                        // `Transaction::commit` did not return normally, so
                        // its usual invalidation path was skipped. A remote
                        // receipt proves that this transaction did publish;
                        // wake local observers before acknowledging recovery.
                        self.observe_invalidation.bump();
                        // Post-commit plugin actor publication is also
                        // skipped on the ambiguous path. Drop private views
                        // rather than let a stale acknowledgement poison the
                        // next plugin-backed edit.
                        self.file_views.clear();
                        receipt.into_single_result()
                    }
                    Ok(IdempotencyReceiptResolution::Absent) => Err(error),
                    Err(recovery_error) => Err(recovery_error),
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn resolve_idempotency_receipt(
        &self,
        idempotency: &ExecuteIdempotency,
    ) -> Result<IdempotencyReceiptResolution, LixError> {
        let visible = self
            .load_idempotency_receipt(idempotency, StorageReadDurability::Visible)
            .await?;
        let Some(visible) = visible else {
            return Ok(IdempotencyReceiptResolution::Absent);
        };
        Self::require_matching_idempotency_receipt(&visible, idempotency)?;

        let durable = match self
            .load_idempotency_receipt(idempotency, StorageReadDurability::Durable)
            .await
        {
            Ok(receipt) => receipt,
            // A backend that cannot expose a durable tier cannot turn an
            // ordinarily visible receipt into replay proof. Preserve the
            // explicit unknown outcome instead of falsely acknowledging it.
            Err(error) if error.code == LixError::CODE_STORAGE_DURABILITY_UNAVAILABLE => {
                return Err(idempotency_outcome_unknown());
            }
            Err(error) => return Err(error),
        };
        let Some(durable) = durable else {
            return Err(idempotency_outcome_unknown());
        };
        Self::require_matching_idempotency_receipt(&durable, idempotency)?;
        Ok(IdempotencyReceiptResolution::Replay(durable))
    }

    async fn load_idempotency_receipt(
        &self,
        idempotency: &ExecuteIdempotency,
        durability: StorageReadDurability,
    ) -> Result<Option<ExecuteIdempotencyReceipt>, LixError> {
        self.ensure_open()?;
        let read = self
            .storage
            .begin_read(StorageReadOptions {
                durability,
                ..StorageReadOptions::default()
            })
            .await?;
        let receipt = load_receipt(&read, idempotency).await?;
        Ok(receipt)
    }

    fn require_matching_idempotency_receipt(
        receipt: &ExecuteIdempotencyReceipt,
        idempotency: &ExecuteIdempotency,
    ) -> Result<(), LixError> {
        if receipt.matches(idempotency) {
            return Ok(());
        }
        Err(LixError::new(
            LixError::CODE_IDEMPOTENCY_KEY_REUSED,
            "Idempotency-Key was already used for a different SQL mutation or branch",
        )
        .with_details(serde_json::json!({
            "retryable": false,
        })))
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
        Box::pin(self.execute_batch_with_options(statements, ExecuteOptions::default())).await
    }

    /// Executes one prepared DML shape over a shared parameter page.
    ///
    /// Unlike [`Self::execute_batch`], this API does not duplicate the SQL
    /// text and owned parameter strings into one statement object per row.
    /// The SQL plan is prepared once and the whole page is committed
    /// atomically. The bound write must accept either the borrowed-value
    /// certificate or the physical parameter-batch route; shapes that require
    /// sequential statement semantics are rejected instead of silently
    /// degrading to per-row execution.
    pub async fn execute_prepared_dml_batch(
        &self,
        sql: Arc<str>,
        parameter_batch: PreparedDmlParameterBatch,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        Box::pin(self.execute_prepared_dml_batch_inner(sql, parameter_batch)).await
    }

    async fn execute_prepared_dml_batch_inner(
        &self,
        sql: Arc<str>,
        parameter_batch: PreparedDmlParameterBatch,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        self.ensure_open()?;
        if parameter_batch.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "execute_prepared_dml_batch requires at least one parameter row",
            ));
        }
        let statement = self.sql_planning_cache.parse_statement(&sql)?;
        if sql2::bind_statement_route(&statement)? != sql2::BoundStatementRoute::Write {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "execute_prepared_dml_batch requires a write statement",
            ));
        }

        let sql_for_error = Arc::clone(&sql);
        let result = self
            .with_write_transaction_lending(async move |transaction| {
                let plan = transaction.prepare_sql_write_logical_plan(&sql, &statement)?;
                let results = sql2::execute_write_logical_plan_prepared_dml_batch(
                    transaction,
                    &plan,
                    &parameter_batch,
                )
                .await?;
                results
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "write shape requires sequential execute_batch semantics",
                        )
                    })
                    .map(|results| {
                        results
                            .into_iter()
                            .map(ExecuteResult::from_sql_write_result)
                            .collect()
                    })
            })
            .await;
        result.map_err(|error| normalize_sql_surface_error(error, &sql_for_error))
    }

    pub async fn execute_batch_with_options(
        &self,
        statements: &[ExecuteBatchStatement],
        options: ExecuteOptions,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        Box::pin(self.execute_batch_with_options_and_metadata(
            statements,
            options,
            vec![ExecuteStatementMetadata::default(); statements.len()],
        ))
        .await
    }

    #[doc(hidden)]
    pub async fn execute_batch_with_options_and_metadata(
        &self,
        statements: &[ExecuteBatchStatement],
        options: ExecuteOptions,
        statement_metadata: Vec<ExecuteStatementMetadata>,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        let results = Box::pin(self.execute_batch_with_options_and_metadata_inner(
            statements,
            options,
            statement_metadata,
            None,
            false,
        ))
        .await?;
        annotate_batch_results(statements, results)
    }

    async fn execute_batch_with_options_and_metadata_inner(
        &self,
        statements: &[ExecuteBatchStatement],
        options: ExecuteOptions,
        statement_metadata: Vec<ExecuteStatementMetadata>,
        idempotency: Option<ExecuteIdempotency>,
        require_idempotency_for_writes: bool,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        let telemetry = start_batch(
            self.telemetry.as_ref(),
            TelemetrySpanKind::SqlBatch,
            statements.len(),
        );
        let operation = self.execute_batch_with_options_inner(
            statements,
            options,
            statement_metadata,
            idempotency,
            require_idempotency_for_writes,
        );
        let result = match telemetry.as_ref() {
            Some(telemetry) => telemetry.instrument(operation).await,
            None => operation.await,
        };
        if let Some(telemetry) = telemetry {
            finish_operation(telemetry, &result);
        }
        result
    }

    /// Executes a protocol SQL batch with durable replay for batches that
    /// contain at least one SQL write. Pure read batches and batches that only
    /// persist runtime-function state retain their existing execution path.
    #[doc(hidden)]
    pub fn execute_batch_with_idempotency_and_options_and_metadata(
        self: Arc<Self>,
        statements: Vec<ExecuteBatchStatement>,
        options: ExecuteOptions,
        statement_metadata: Vec<ExecuteStatementMetadata>,
        idempotency: Option<ExecuteIdempotency>,
    ) -> impl Future<Output = Result<Vec<ExecuteResult>, LixError>> + Send + 'static {
        // SAFETY: as above, this future owns the Arc session, statements, and
        // metadata; transaction state crosses awaits only through mutable
        // references and every storage transaction is Send.
        unsafe {
            super::AssumeSendFuture::new(async move {
                let results = self
                    .execute_batch_with_options_and_metadata_inner(
                        &statements,
                        options,
                        statement_metadata,
                        idempotency,
                        true,
                    )
                    .await?;
                annotate_batch_results(&statements, results)
            })
        }
    }

    async fn execute_batch_with_options_inner(
        &self,
        statements: &[ExecuteBatchStatement],
        options: ExecuteOptions,
        statement_metadata: Vec<ExecuteStatementMetadata>,
        idempotency: Option<ExecuteIdempotency>,
        require_idempotency_for_writes: bool,
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
        if statement_metadata.len() != statements.len() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "execute batch statement metadata must align with statements",
            )
            .with_details(serde_json::json!({
                "operation": "executeBatch",
                "statementCount": statements.len(),
                "metadataCount": statement_metadata.len(),
            })));
        }
        for (statement_index, (statement, metadata)) in
            statements.iter().zip(&statement_metadata).enumerate()
        {
            validate_execute_statement_metadata(
                statement.params.len(),
                metadata,
                Some(statement_index),
            )?;
        }

        match classify_execute_batch(statements, &self.sql_planning_cache)? {
            ExecuteBatchExecution::ReadOnly(parsed) => {
                self.execute_read_only_batch(statements, parsed).await
            }
            ExecuteBatchExecution::Transaction(parsed) => {
                let contains_write = parsed.contains_write()?;
                if !contains_write {
                    return self
                        .execute_transaction_batch(
                            statements,
                            parsed,
                            options,
                            statement_metadata,
                            None,
                        )
                        .await;
                }
                if require_idempotency_for_writes && idempotency.is_none() {
                    return Err(LixError::new(
                        LixError::CODE_IDEMPOTENCY_KEY_REQUIRED,
                        "Idempotency-Key is required for SQL mutation batches",
                    ));
                }
                let Some(idempotency) = idempotency else {
                    return self
                        .execute_transaction_batch(
                            statements,
                            parsed,
                            options,
                            statement_metadata,
                            None,
                        )
                        .await;
                };
                if let IdempotencyReceiptResolution::Replay(receipt) =
                    self.resolve_idempotency_receipt(&idempotency).await?
                {
                    return Ok(receipt.into_results());
                }
                let result = self
                    .execute_transaction_batch(
                        statements,
                        parsed,
                        options,
                        statement_metadata,
                        Some(idempotency.clone()),
                    )
                    .await;
                match result {
                    Ok(results) => Ok(results),
                    Err(error)
                        if matches!(
                            error.code.as_str(),
                            LixError::CODE_TRANSACTION_CONFLICT
                                | LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN
                        ) =>
                    {
                        match self.resolve_idempotency_receipt(&idempotency).await {
                            Ok(IdempotencyReceiptResolution::Replay(receipt)) => {
                                // See the single-statement recovery path:
                                // positive receipt proof means a commit
                                // happened after its normal invalidation
                                // callback was bypassed by an ambiguous error.
                                self.observe_invalidation.bump();
                                self.file_views.clear();
                                Ok(receipt.into_results())
                            }
                            Ok(IdempotencyReceiptResolution::Absent) => Err(error),
                            Err(recovery_error) => Err(recovery_error),
                        }
                    }
                    Err(error) => Err(error),
                }
            }
        }
    }

    async fn execute_transaction_batch(
        &self,
        statements: &[ExecuteBatchStatement],
        parsed: TransactionBatchStatements,
        options: ExecuteOptions,
        statement_metadata: Vec<ExecuteStatementMetadata>,
        idempotency: Option<ExecuteIdempotency>,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        let telemetry_sink = self.telemetry.clone();
        let parameter_route = Arc::new(AtomicBool::new(false));
        let transaction_parameter_route = Arc::clone(&parameter_route);
        let transaction_telemetry_sink = telemetry_sink.clone();
        let result = self
            .with_write_transaction_lending(async move |transaction| {
                if let Some(results) = try_execute_transaction_parameter_batch(
                    transaction,
                    statements,
                    &parsed,
                    &options,
                    &statement_metadata,
                    &transaction_parameter_route,
                )
                .await?
                {
                    if let Some(idempotency) = &idempotency {
                        let receipt = ExecuteIdempotencyReceipt::batch(idempotency, &results)?;
                        transaction.stage_execute_idempotency_receipt(idempotency, &receipt)?;
                    }
                    return Ok(results);
                }
                let mut results = Vec::with_capacity(statements.len());
                match parsed {
                    TransactionBatchStatements::AutoParameterizedUpdate {
                        sql,
                        statement: parsed,
                        parameter_batch,
                    } => {
                        for (statement_index, (statement, metadata)) in
                            statements.iter().zip(statement_metadata).enumerate()
                        {
                            let params = sql2::parameter_row(&parameter_batch, statement_index)
                                .map_err(|error| {
                                    with_batch_statement_index(error, statement_index)
                                })?;
                            let telemetry = SqlStatementTelemetry::start(
                                transaction_telemetry_sink.as_ref(),
                                &statement.sql,
                                "batch",
                                Some(statement_index),
                            );
                            // Keep the large statement executor behind a heap boundary. The
                            // lending transaction closure already carries the whole parsed batch;
                            // embedding this future in it makes debug poll stacks exceed the
                            // standard 2 MiB worker stack for ordinary entity writes.
                            let operation = Box::pin(execute_transaction_statement(
                                transaction,
                                &sql,
                                parsed.clone(),
                                &params,
                                options.clone(),
                                metadata,
                            ));
                            let result = match telemetry.as_ref() {
                                Some(telemetry) => telemetry.instrument(operation).await,
                                None => operation.await,
                            }
                            .map_err(|error| {
                                with_batch_statement_index(
                                    normalize_sql_surface_error(error, &statement.sql),
                                    statement_index,
                                )
                            });
                            if let Some(telemetry) = telemetry {
                                telemetry.finish(&result);
                            }
                            results.push(result?);
                        }
                    }
                    parsed => {
                        for (statement_index, ((statement, parsed), metadata)) in statements
                            .iter()
                            .zip(parsed.into_vec())
                            .zip(statement_metadata)
                            .enumerate()
                        {
                            let telemetry = SqlStatementTelemetry::start(
                                transaction_telemetry_sink.as_ref(),
                                &statement.sql,
                                "batch",
                                Some(statement_index),
                            );
                            // See the auto-parameterized branch above. Both batch routes need the
                            // same bounded poll-stack boundary.
                            let operation = Box::pin(execute_transaction_statement(
                                transaction,
                                &statement.sql,
                                parsed,
                                &statement.params,
                                options.clone(),
                                metadata,
                            ));
                            let result = match telemetry.as_ref() {
                                Some(telemetry) => telemetry.instrument(operation).await,
                                None => operation.await,
                            }
                            .map_err(|error| {
                                with_batch_statement_index(
                                    normalize_sql_surface_error(error, &statement.sql),
                                    statement_index,
                                )
                            });
                            if let Some(telemetry) = telemetry {
                                telemetry.finish(&result);
                            }
                            results.push(result?);
                        }
                    }
                }
                if let Some(idempotency) = &idempotency {
                    let receipt = ExecuteIdempotencyReceipt::batch(idempotency, &results)?;
                    transaction.stage_execute_idempotency_receipt(idempotency, &receipt)?;
                }
                Ok(results)
            })
            .await;
        if parameter_route.load(Ordering::Relaxed) {
            finish_parameter_batch_statement_telemetry(
                telemetry_sink.as_ref(),
                statements,
                &result,
            );
        }
        result
    }

    async fn execute_read_only_batch(
        &self,
        statements: &[ExecuteBatchStatement],
        parsed: Vec<datafusion::sql::parser::Statement>,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        let acknowledge_file_views = parsed.iter().zip(statements).all(|(parsed, statement)| {
            is_acknowledgeable_file_content_read(parsed, &statement.params)
        });
        let _operation_guard = self.begin_waitable_session_operation().await?;
        let read_scope = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await?;
        let (results, file_view_mutations) = with_static_session_sql_read::<StorageImpl, _, _>(
            read_scope,
            |read_store: SharedStorageAdapterRead<StorageImpl::Read<'static>>| async move {
                let file_view_collector =
                    acknowledge_file_views.then(sql2::SessionFileViews::default);
                let active_branch_id = self.active_branch_id_from_reader(&read_store).await?;
                let forktree = crate::forktree::ForkTreeReadFacade::new(read_store.clone());
                let state_view = crate::state::ForkTreeStateView::from_facade(
                    forktree.clone(),
                    &active_branch_id,
                )
                .await?;
                let ctx = SessionSqlExecutionContext {
                    active_branch_id: &active_branch_id,
                    active_account_id: self.active_account_id(),
                    read_store,
                    forktree,
                    state_view,
                    catalog_context: Arc::clone(&self.catalog_context),
                    sql_planning_cache: Arc::clone(&self.sql_planning_cache),
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
            .all(|(parsed, (_, params))| is_acknowledgeable_file_content_read(parsed, params));

        let _operation_guard = self.begin_waitable_session_operation().await?;
        let read_scope = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await?;
        let (batch, file_view_mutations) = with_static_session_sql_read::<StorageImpl, _, _>(
            read_scope,
            |read_store: SharedStorageAdapterRead<StorageImpl::Read<'static>>| async move {
                let file_view_collector =
                    acknowledge_file_views.then(sql2::SessionFileViews::default);
                let active_branch_id = self.active_branch_id_from_reader(&read_store).await?;
                let active_branch_head = BranchRefStoreReader::new(read_store.clone())
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
                let forktree = crate::forktree::ForkTreeReadFacade::new(read_store.clone());
                let state_view = crate::state::ForkTreeStateView::from_facade(
                    forktree.clone(),
                    &active_branch_id,
                )
                .await?;
                let ctx = SessionSqlExecutionContext {
                    active_branch_id: &active_branch_id,
                    active_account_id: self.active_account_id(),
                    read_store,
                    forktree,
                    state_view,
                    catalog_context: Arc::clone(&self.catalog_context),
                    sql_planning_cache: Arc::clone(&self.sql_planning_cache),
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
                .with_write_transaction_reserved_lending(
                    write_access,
                    async move |transaction| {
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
                    },
                    |_| Ok(()),
                )
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
        runtime_functions: FunctionContext,
        has_runtime_write_access: bool,
    ) -> Result<Option<crate::storage_adapter::StorageWriteSetStats>, LixError> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let Some(runtime_checkpoint) = runtime_functions.deterministic_sequence_checkpoint() else {
            return Ok(None);
        };
        if !has_runtime_write_access {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "runtime function state changed without reserved write access",
            ));
        }
        let publication = crate::transaction::prepare_runtime_sequence_publication(
            self.active_account_id(),
            runtime_checkpoint,
            read,
        )
        .await?;
        let (writes, publication_preconditions) = publication.into_storage_plan()?;
        let commit_boundary = self.transaction_commit_boundary();
        let _commit_guard = begin_commit_boundary(Some(&commit_boundary));
        let mut write_options = StorageWriteOptions::default();
        write_options
            .preconditions
            .extend(publication_preconditions);
        let prepared_commit = self
            .storage
            .prepare_write_set(writes, write_options)
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
        exact_filesystem_read: Option<ExactFilesystemRead>,
        late_file_content_read: Option<LateMaterializedLixFileContentRead>,
        has_durable_runtime_function: bool,
    ) -> Result<
        (
            sql2::SessionReadSqlResult,
            Vec<sql2::SessionFileViewMutation>,
        ),
        LixError,
    > {
        let file_view_collector = acknowledge_file_views.then(sql2::SessionFileViews::default);
        let active_branch_id = self
            .active_branch_id_from_reader(&read_store)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.public_read.active_branch"
            ))
            .await?;
        let forktree = crate::forktree::ForkTreeReadFacade::new(read_store.clone());
        let state_view =
            crate::state::ForkTreeStateView::from_facade(forktree.clone(), &active_branch_id)
                .await?;
        if let Some(exact_filesystem_read) = exact_filesystem_read {
            let query = match exact_filesystem_read {
                ExactFilesystemRead::RootFileListing => {
                    let filesystem_path_index: Arc<
                        dyn crate::filesystem::FilesystemPathIndexReader,
                    > = Arc::new(crate::filesystem::ForkTreeFilesystemPathIndexReader::new(
                        forktree.clone(),
                    ));
                    let branch_ref: Arc<dyn BranchRefReader> =
                        Arc::new(BranchRefStoreReader::new(read_store));
                    sql2::execute_exact_lix_file_root_listing(
                        &active_branch_id,
                        filesystem_path_index,
                        branch_ref,
                    )
                    .await?
                }
                ExactFilesystemRead::RootDirectoryListing => {
                    let filesystem_path_index: Arc<
                        dyn crate::filesystem::FilesystemPathIndexReader,
                    > = Arc::new(crate::filesystem::ForkTreeFilesystemPathIndexReader::new(
                        forktree.clone(),
                    ));
                    let branch_ref: Arc<dyn BranchRefReader> =
                        Arc::new(BranchRefStoreReader::new(read_store));
                    sql2::execute_exact_lix_directory_root_listing(
                        &active_branch_id,
                        filesystem_path_index,
                        branch_ref,
                    )
                    .await?
                }
                exact_filesystem_read => {
                    let state_view = state_view.clone();
                    let filesystem_path_index: Arc<
                        dyn crate::filesystem::FilesystemPathIndexReader,
                    > = Arc::new(crate::filesystem::ForkTreeFilesystemPathIndexReader::new(
                        forktree.clone(),
                    ));
                    let branch_ref: Arc<dyn BranchRefReader> =
                        Arc::new(BranchRefStoreReader::new(read_store.clone()));
                    let authenticated_blob_reader: Arc<
                        dyn crate::forktree::AuthenticatedBlobReader,
                    > = Arc::new(crate::forktree::blob_reader_on_read(
                        read_store.clone(),
                        &active_branch_id,
                    )?);
                    match exact_filesystem_read {
                        ExactFilesystemRead::Point(selector, column) => {
                            sql2::execute_exact_lix_file_read(
                                &active_branch_id,
                                state_view,
                                filesystem_path_index,
                                branch_ref,
                                Arc::clone(&authenticated_blob_reader),
                                self.plugin_host.clone(),
                                file_view_collector.clone(),
                                &selector,
                                column,
                            )
                            .await?
                        }
                        ExactFilesystemRead::PathContentBatch(paths) => {
                            sql2::execute_exact_lix_file_batch_read(
                                &active_branch_id,
                                state_view,
                                filesystem_path_index,
                                branch_ref,
                                authenticated_blob_reader,
                                self.plugin_host.clone(),
                                file_view_collector.clone(),
                                &paths,
                                None,
                            )
                            .await?
                        }
                        ExactFilesystemRead::IdManifestBatch(file_ids) => {
                            sql2::execute_exact_lix_file_id_manifest_batch_read(
                                &active_branch_id,
                                state_view,
                                filesystem_path_index,
                                branch_ref,
                                authenticated_blob_reader,
                                self.plugin_host.clone(),
                                file_view_collector.clone(),
                                &file_ids,
                            )
                            .await?
                        }
                        ExactFilesystemRead::RootFileListing
                        | ExactFilesystemRead::RootDirectoryListing => unreachable!(
                            "root filesystem listings handled before file content readers"
                        ),
                    }
                }
            };
            let file_view_mutations = file_view_collector
                .map(|collector| collector.plugin_file_mutations())
                .unwrap_or_default();
            return Ok((
                sql2::SessionReadSqlResult {
                    runtime_functions: None,
                    query: sql2::SessionReadResult::Rows(query),
                },
                file_view_mutations,
            ));
        }
        let runtime_functions = if has_durable_runtime_function {
            Some(FunctionContext::prepare(&read_store).await?)
        } else {
            None
        };
        // Read providers do not consume durable function state themselves;
        // only the registered timestamp/UUID SQL UDFs do. Keep their AST
        // classifier conservative if new readable statement shapes appear.
        let functions = runtime_functions
            .as_ref()
            .map_or_else(FunctionProviderHandle::system, FunctionContext::provider);
        let (statement, late_file_content_column) = match late_file_content_read {
            Some(plan) => (*plan.statement, Some(plan.data_column_index)),
            None => (statement, None),
        };
        let ctx = SessionSqlExecutionContext {
            active_branch_id: &active_branch_id,
            active_account_id: self.active_account_id(),
            read_store: read_store.clone(),
            forktree: forktree.clone(),
            state_view: state_view.clone(),
            catalog_context: Arc::clone(&self.catalog_context),
            sql_planning_cache: Arc::clone(&self.sql_planning_cache),
            functions: functions.clone(),
            plugin_host: self.plugin_host.clone(),
            file_views: file_view_collector.clone(),
        };

        let read_session =
            sql2::prepare_read_session(&ctx, std::slice::from_ref(&statement)).await?;
        let mut query = sql2::execute_read_statement_in_session_with_result(
            &read_session,
            sql,
            statement,
            params,
        )
        .await?;
        drop(read_session);
        drop(ctx);
        if let Some(data_column_index) = late_file_content_column {
            let filesystem_path_index: Arc<dyn crate::filesystem::FilesystemPathIndexReader> =
                Arc::new(crate::filesystem::ForkTreeFilesystemPathIndexReader::new(
                    forktree.clone(),
                ));
            let branch_ref: Arc<dyn BranchRefReader> =
                Arc::new(BranchRefStoreReader::new(read_store.clone()));
            let authenticated_blob_reader: Arc<dyn crate::forktree::AuthenticatedBlobReader> =
                Arc::new(crate::forktree::blob_reader_on_read(
                    read_store.clone(),
                    &active_branch_id,
                )?);
            let mut materialized = query.query.into_sql_query_result()?;
            hydrate_lix_file_content_result(
                state_view.clone(),
                &active_branch_id,
                filesystem_path_index,
                branch_ref,
                authenticated_blob_reader,
                self.plugin_host.clone(),
                file_view_collector.clone(),
                &mut materialized,
                data_column_index,
            )
            .await?;
            query.query = sql2::SessionReadResult::Rows(materialized);
        }
        let file_view_mutations = file_view_collector
            .map(|collector| collector.plugin_file_mutations())
            .unwrap_or_default();
        Ok((
            sql2::SessionReadSqlResult {
                runtime_functions,
                query: query.query,
            },
            file_view_mutations,
        ))
    }
}

fn native_file_read_from_exact_result(
    result: SqlQueryResult,
    requested_paths: &BTreeSet<String>,
    requested_range: Option<Range<u64>>,
) -> Result<Option<FileRead>, LixError> {
    if requested_range.is_none() {
        return native_file_content_from_exact_result(result, requested_paths)?
            .map(|(data, identity)| materialize_file_read(data, identity, None))
            .transpose();
    }
    if result.columns.as_slice()
        != [
            "path",
            "content",
            "total_size",
            "range_start",
            "range_end",
            "content_identity",
        ]
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native ranged file read returned an unexpected result schema",
        ));
    }
    let mut rows = result.rows.into_iter();
    let Some(mut row) = rows.next() else {
        return Ok(None);
    };
    if rows.next().is_some() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native ranged file read returned more than one path",
        ));
    }
    let [
        Value::Text(path),
        data,
        Value::Integer(total_size),
        Value::Integer(range_start),
        Value::Integer(range_end),
        Value::Text(content_identity),
    ] = row.as_mut_slice()
    else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native ranged file read returned an invalid row",
        ));
    };
    if !requested_paths.contains(path.as_str()) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native ranged file read returned an unrequested path",
        ));
    }
    let data = match std::mem::replace(data, Value::Null) {
        Value::Blob(data) => data,
        _ => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "native ranged file read returned non-binary data",
            ));
        }
    };
    let total_size = u64::try_from(*total_size).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native file size is negative",
        )
    })?;
    let range_start = u64::try_from(*range_start).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native file range is negative",
        )
    })?;
    let range_end = u64::try_from(*range_end).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native file range is negative",
        )
    })?;
    Ok(Some(FileRead {
        content: data,
        total_size,
        range: range_start..range_end,
        content_identity: std::mem::take(content_identity),
    }))
}

fn native_file_content_from_exact_result(
    result: SqlQueryResult,
    requested_paths: &BTreeSet<String>,
) -> Result<Option<(Blob, String)>, LixError> {
    if result.columns.as_slice() != ["path", "content", "content_identity"] {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native file read returned an unexpected result schema",
        ));
    }
    let mut rows = result.rows.into_iter();
    let Some(mut row) = rows.next() else {
        return Ok(None);
    };
    if rows.next().is_some() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native file read returned more than one path",
        ));
    }
    let [Value::Text(path), data, Value::Text(content_identity)] = row.as_mut_slice() else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native file read returned an invalid row",
        ));
    };
    if !requested_paths.contains(path.as_str()) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native file read returned an unrequested path",
        ));
    }
    let content = match std::mem::replace(data, Value::Null) {
        Value::Blob(content) => content,
        // A path-only `lix_file` row is a present empty file on the public
        // surface. Preserve that contract even if a storage layout represents
        // it as SQL NULL internally.
        Value::Null => Blob::default(),
        _ => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "native file read returned a non-binary data value",
            ));
        }
    };
    Ok(Some((content, std::mem::take(content_identity))))
}

fn materialize_file_read(
    data: Blob,
    content_identity: String,
    requested_range: Option<Range<u64>>,
) -> Result<FileRead, LixError> {
    let total_size = u64::try_from(data.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native file size does not fit the public 64-bit range",
        )
    })?;
    let range = match requested_range {
        None => 0..total_size,
        Some(range) => {
            if range.start >= range.end || range.start >= total_size {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "file read range is not satisfiable",
                )
                .with_details(serde_json::json!({
                    "rangeStart": range.start,
                    "rangeEnd": range.end,
                    "totalSize": total_size,
                })));
            }
            range.start..range.end.min(total_size)
        }
    };
    let start = usize::try_from(range.start)
        .map_err(|_| LixError::new(LixError::CODE_INVALID_PARAM, "file read range is too large"))?;
    let end = usize::try_from(range.end)
        .map_err(|_| LixError::new(LixError::CODE_INVALID_PARAM, "file read range is too large"))?;
    let data = Blob::from(data.as_bytes().slice(start..end));
    Ok(FileRead {
        content: data,
        total_size,
        range,
        content_identity,
    })
}

fn validate_execute_statement_metadata(
    parameter_count: usize,
    metadata: &ExecuteStatementMetadata,
    statement_index: Option<usize>,
) -> Result<(), LixError> {
    let metadata_count = metadata.parameter_blob_splices.len();
    if metadata_count == 0 || metadata_count == parameter_count {
        return Ok(());
    }
    let mut details = serde_json::json!({
        "operation": if statement_index.is_some() { "executeBatch" } else { "execute" },
        "parameterCount": parameter_count,
        "metadataCount": metadata_count,
    });
    if let Some(statement_index) = statement_index {
        details["statementIndex"] = statement_index.into();
    }
    Err(LixError::new(
        LixError::CODE_INVALID_PARAM,
        "execute statement metadata must align with SQL parameters",
    )
    .with_details(details))
}

#[allow(clippy::too_many_arguments)]
async fn hydrate_lix_file_content_result<R>(
    state_view: crate::state::ForkTreeStateView<SharedStorageAdapterRead<R>>,
    active_branch_id: &str,
    filesystem_path_index: Arc<dyn crate::filesystem::FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    authenticated_blob_reader: Arc<dyn crate::forktree::AuthenticatedBlobReader>,
    plugin_host: crate::plugin::PluginRuntimeHost,
    session_file_views: Option<sql2::SessionFileViews>,
    query: &mut SqlQueryResult,
    data_column_index: usize,
) -> Result<(), LixError>
where
    R: crate::storage_adapter::StorageRead + 'static,
{
    let mut paths = BTreeSet::new();
    for row in &query.rows {
        let Some(Value::Text(path)) = row.get(data_column_index) else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "late lix_file content placeholder was not a path",
            ));
        };
        paths.insert(path.clone());
    }
    if paths.is_empty() {
        return Ok(());
    }

    let hydrated = sql2::execute_exact_lix_file_batch_read(
        active_branch_id,
        state_view,
        filesystem_path_index,
        branch_ref,
        authenticated_blob_reader,
        plugin_host,
        session_file_views,
        &paths,
        None,
    )
    .await?;
    let mut data_by_path = BTreeMap::new();
    for mut row in hydrated.rows {
        let [Value::Text(path), data, _content_identity] = row.as_mut_slice() else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "late lix_file content hydration returned an invalid row",
            ));
        };
        data_by_path.insert(path.clone(), std::mem::replace(data, Value::Null));
    }
    query.notices.extend(hydrated.notices);

    for row in &mut query.rows {
        let Some(placeholder) = row.get_mut(data_column_index) else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "late lix_file content result was missing its placeholder column",
            ));
        };
        let Value::Text(path) = std::mem::replace(placeholder, Value::Null) else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "late lix_file content placeholder was not a path",
            ));
        };
        *placeholder = data_by_path.remove(&path).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("late lix_file content hydration did not return '{path}'"),
            )
        })?;
    }
    Ok(())
}

#[cfg(feature = "storage-benches")]
async fn consume_profile_cursor(
    cursor: &mut sql2::BatchRowCursor<'_>,
    row_limit: Option<usize>,
) -> Result<(), LixError> {
    let limit = row_limit.unwrap_or(usize::MAX);
    let mut consumed = 0usize;
    let mut checksum = 0u64;
    while consumed < limit {
        let Some(values) = cursor.next_values().await? else {
            break;
        };
        checksum = profile_result_checksum(checksum, &values)?;
        consumed += 1;
    }
    crate::sql_profile::record_result_rows(consumed, consumed, 0);
    crate::sql_profile::record_result_checksum(checksum);
    Ok(())
}

#[cfg(feature = "storage-benches")]
fn profile_result_checksum(checksum: u64, values: &[Value]) -> Result<u64, LixError> {
    if values.len() != 3 {
        return Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            "streaming profile expected exactly three projected values",
        ));
    }
    let mut checksum = if checksum == 0 {
        0xcbf2_9ce4_8422_2325
    } else {
        checksum
    };
    checksum = profile_checksum_bytes(checksum, &[0xff]);
    for value in values {
        checksum = match value {
            Value::Null => profile_checksum_bytes(checksum, &[0]),
            Value::Boolean(value) => profile_checksum_bytes(checksum, &[1, u8::from(*value)]),
            Value::Integer(value) => {
                let checksum = profile_checksum_bytes(checksum, &[2]);
                profile_checksum_bytes(checksum, &value.to_le_bytes())
            }
            Value::Real(value) => {
                let checksum = profile_checksum_bytes(checksum, &[3]);
                profile_checksum_bytes(checksum, &value.to_bits().to_le_bytes())
            }
            Value::Timestamp(value) => {
                let checksum = profile_checksum_bytes(checksum, &[7]);
                profile_checksum_bytes(checksum, &value.to_le_bytes())
            }
            Value::Text(value) => profile_checksum_sized_bytes(checksum, 4, value.as_bytes()),
            Value::Json(value) => {
                profile_checksum_sized_bytes(checksum, 5, value.to_string().as_bytes())
            }
            Value::Blob(value) => {
                profile_checksum_sized_bytes(checksum, 6, value.as_bytes().as_ref())
            }
        };
    }
    Ok(checksum)
}

#[cfg(feature = "storage-benches")]
fn profile_checksum_sized_bytes(checksum: u64, tag: u8, bytes: &[u8]) -> u64 {
    let checksum = profile_checksum_bytes(checksum, &[tag]);
    let checksum = profile_checksum_bytes(checksum, &(bytes.len() as u64).to_le_bytes());
    profile_checksum_bytes(checksum, bytes)
}

#[cfg(feature = "storage-benches")]
fn profile_checksum_bytes(mut checksum: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        checksum ^= u64::from(*byte);
        checksum = checksum.wrapping_mul(0x0000_0100_0000_01b3);
    }
    checksum
}

/// Runs one session SQL read using a widened storage-read lifetime.
///
/// DataFusion requires providers and plans to be `'static`, while engine
/// storage implementations such as RocksDB naturally expose borrowed read snapshots. Keep
/// the lifetime erasure private to session SQL execution so callers cannot
/// receive the widened read as a general crate capability.
async fn with_static_session_sql_read<StorageImpl, F, T>(
    read: StorageAdapterReadScope<StorageImpl::Read<'_>>,
    f: F,
) -> Result<T, LixError>
where
    StorageImpl: Storage + 'static,
    F: AsyncFnOnce(SharedStorageAdapterRead<StorageImpl::Read<'static>>) -> Result<T, LixError>,
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
        Box::pin(self.execute_with_options_inner(sql, params, ExecuteOptions::default())).await
    }

    /// Executes one public prepared-DML parameter page inside this explicit
    /// transaction. The page is atomic with surrounding statements; callers
    /// use ordinary `execute` for shape changes or dependency barriers.
    pub async fn execute_prepared_dml_batch(
        &mut self,
        sql: Arc<str>,
        parameter_batch: PreparedDmlParameterBatch,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        self.ensure_session_open()?;
        if parameter_batch.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "execute_prepared_dml_batch requires at least one parameter row",
            ));
        }
        let statement = self.sql_planning_cache.parse_statement(&sql)?;
        if sql2::bind_statement_route(&statement)? != sql2::BoundStatementRoute::Write {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "execute_prepared_dml_batch requires a write statement",
            ));
        }
        self.has_started_statement = true;
        let transaction = self.transaction_mut()?;
        transaction.flush_prepared_mutations().await?;
        let plan = transaction.prepare_sql_write_logical_plan(&sql, &statement)?;
        let checkpoint = transaction.begin_sql_statement_checkpoint()?;
        let result = sql2::execute_write_logical_plan_prepared_dml_batch(
            transaction,
            &plan,
            &parameter_batch,
        )
        .await;
        let result = match result {
            Ok(Some(results)) => Ok(results),
            Ok(None) => Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "write shape is not supported by prepared DML batch",
            )),
            Err(error) => Err(normalize_sql_surface_error(error, &sql)),
        };
        let results = match result {
            Ok(results) => results,
            Err(error) => {
                transaction
                    .rollback_sql_statement_checkpoint(checkpoint)
                    .await?;
                return Err(error);
            }
        };
        Ok(results
            .into_iter()
            .map(ExecuteResult::from_sql_write_result)
            .collect())
    }

    async fn execute_with_options_inner(
        &mut self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        let telemetry =
            SqlStatementTelemetry::start(self.telemetry.as_ref(), sql, "transaction", None);
        self.has_started_statement = true;
        // The explicit write lease already keeps one transaction operation
        // active for this handle's lifetime, and `&mut self` excludes an
        // overlapping execute or commit. A second manager guard per statement
        // only repeated mutex/Notify traffic without adding a state boundary.
        let operation = async {
            let auto_parameterized = params
                .is_empty()
                .then(|| self.sql_planning_cache.auto_parameterized_update(sql))
                .flatten();
            let (planning_sql, statement, auto_params) =
                if let Some(auto_parameterized) = auto_parameterized {
                    (
                        auto_parameterized.sql,
                        auto_parameterized.statement,
                        Some(auto_parameterized.params),
                    )
                } else {
                    (
                        Arc::from(sql),
                        self.sql_planning_cache.parse_statement(sql)?,
                        None,
                    )
                };
            let params = auto_params.as_deref().unwrap_or(params);
            let transaction = self.transaction_mut()?;
            if transaction.prepared_mutation_matches(&planning_sql) {
                transaction
                    .flush_prepared_mutation_barrier(
                        &planning_sql,
                        options.origin_key.as_deref(),
                        params,
                    )
                    .await?;
                let previous_origin_key =
                    transaction.replace_origin_key(options.origin_key.clone());
                let result = transaction
                    .try_execute_prepared_mutation(&planning_sql, params)
                    .await;
                transaction.replace_origin_key(previous_origin_key);
                match result {
                    Ok(Some(result)) => {
                        return Ok(ExecuteResult::from_sql_write_result(result));
                    }
                    Ok(None) => {}
                    Err(error) => return Err(normalize_sql_surface_error(error, sql)),
                }
            }
            let is_read = matches!(
                sql2::bind_statement_route(&statement)?,
                sql2::BoundStatementRoute::Read
            );
            if is_read {
                transaction.flush_prepared_mutations_for_read().await?;
            } else {
                transaction
                    .flush_prepared_mutation_barrier(
                        &planning_sql,
                        options.origin_key.as_deref(),
                        params,
                    )
                    .await?;
            }
            // A successful explicit transaction retains its function provider
            // until commit. Rewind deterministic runtime state whenever this
            // statement fails, including errors before a direct RETURNING
            // write reaches staging.
            let function_checkpoint = transaction.functions().statement_checkpoint();
            let result = async {
                let result = if is_read {
                    execute_transaction_statement(
                        transaction,
                        sql,
                        statement,
                        params,
                        options,
                        ExecuteStatementMetadata::default(),
                    )
                    .await
                } else {
                    execute_transaction_write_auto(
                        transaction,
                        &planning_sql,
                        statement,
                        params,
                        options,
                        ExecuteStatementMetadata::default(),
                        true,
                    )
                    .await
                };
                let result = result.map_err(|error| normalize_sql_surface_error(error, sql))?;
                if !is_read {
                    transaction.release_pending_plugin_actor_leases().await;
                }
                Ok(result)
            }
            .await;
            if result.is_err() {
                if let Some(function_checkpoint) = function_checkpoint {
                    transaction
                        .functions()
                        .restore_statement_checkpoint(function_checkpoint);
                }
            }
            result
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

    pub fn execute_with_options(
        &mut self,
        sql: String,
        params: Vec<Value>,
        options: ExecuteOptions,
    ) -> impl Future<Output = Result<ExecuteResult, LixError>> + Send + '_ {
        // SAFETY: the future exclusively borrows this Send transaction and
        // owns its SQL and parameter payload. Higher-ranked string references
        // are created and consumed entirely inside that exclusive borrow.
        unsafe {
            super::AssumeSendFuture::new(async move {
                self.execute_with_options_inner(&sql, &params, options)
                    .await
            })
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
        let statement = self.sql_planning_cache.parse_statement(sql)?;
        let transaction = self.transaction_mut()?;
        transaction.flush_prepared_mutations().await?;
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
        transaction.flush_prepared_mutations().await?;
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
}

async fn try_execute_transaction_parameter_batch<StorageImpl>(
    transaction: &mut crate::transaction::Transaction<StorageImpl>,
    statements: &[ExecuteBatchStatement],
    parsed: &TransactionBatchStatements,
    options: &ExecuteOptions,
    statement_metadata: &[ExecuteStatementMetadata],
    parameter_route: &AtomicBool,
) -> Result<Option<Vec<ExecuteResult>>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let Some(first_statement) = statements.first() else {
        return Ok(None);
    };
    if statements.len() < 2
        || parsed.len() != statements.len()
        || statement_metadata.len() != statements.len()
        || statement_metadata
            .iter()
            .any(|metadata| metadata != &ExecuteStatementMetadata::default())
    {
        return Ok(None);
    }

    let (planning_sql, parsed_statement, parameter_rows) = match parsed {
        TransactionBatchStatements::AutoParameterizedUpdate { sql, statement, .. } => {
            (sql.as_ref(), statement, None)
        }
        TransactionBatchStatements::Shared { statement, .. }
            if statements
                .iter()
                .all(|candidate| candidate.sql == first_statement.sql) =>
        {
            (
                first_statement.sql.as_str(),
                statement,
                Some(
                    statements
                        .iter()
                        .map(|statement| statement.params.as_slice())
                        .collect::<Vec<_>>(),
                ),
            )
        }
        TransactionBatchStatements::Shared { .. } | TransactionBatchStatements::Distinct(_) => {
            return Ok(None);
        }
    };
    if sql2::bind_statement_route(parsed_statement)? != sql2::BoundStatementRoute::Write {
        return Ok(None);
    }

    let previous_origin_key = transaction.replace_origin_key(options.origin_key.clone());
    let execution = async {
        let plan = transaction.prepare_sql_write_logical_plan(planning_sql, parsed_statement)?;
        if let TransactionBatchStatements::AutoParameterizedUpdate {
            parameter_batch, ..
        } = parsed
        {
            return sql2::execute_write_logical_plan_parameter_batch(
                transaction,
                plan,
                parameter_batch,
            )
            .await;
        }
        let parameter_rows = parameter_rows
            .as_ref()
            .expect("shared parameter execution retains borrowed rows");
        if let Some(results) =
            sql2::execute_write_logical_plan_value_batch(transaction, &plan, parameter_rows).await?
        {
            return Ok(Some(results));
        }
        let Some(parameter_batch) = sql2::parameter_record_batch(parameter_rows)? else {
            return Ok(None);
        };
        sql2::execute_write_logical_plan_parameter_batch(transaction, plan, &parameter_batch).await
    }
    .await;
    transaction.replace_origin_key(previous_origin_key);

    let result = execution
        .map(|results| {
            results.map(|results| {
                results
                    .into_iter()
                    .map(ExecuteResult::from_sql_write_result)
                    .collect()
            })
        })
        .map_err(|error| {
            let error = normalize_sql_surface_error(error, planning_sql);
            if batch_statement_index(&error).is_some() {
                error
            } else {
                with_batch_statement_index(error, 0)
            }
        });
    if !matches!(result, Ok(None)) {
        parameter_route.store(true, Ordering::Relaxed);
    }
    result
}

fn finish_parameter_batch_statement_telemetry(
    telemetry_sink: Option<&Arc<dyn crate::telemetry::TelemetrySink>>,
    statements: &[ExecuteBatchStatement],
    result: &Result<Vec<ExecuteResult>, LixError>,
) {
    match result {
        Ok(results) => {
            for (statement_index, (statement, result)) in statements.iter().zip(results).enumerate()
            {
                let Some(telemetry) = SqlStatementTelemetry::start(
                    telemetry_sink,
                    &statement.sql,
                    "batch",
                    Some(statement_index),
                ) else {
                    continue;
                };
                telemetry.finish(&Ok(result.clone()));
            }
        }
        Err(error) => {
            let statement_index = batch_statement_index(error).unwrap_or(0);
            let Some(statement) = statements.get(statement_index) else {
                return;
            };
            let Some(telemetry) = SqlStatementTelemetry::start(
                telemetry_sink,
                &statement.sql,
                "batch",
                Some(statement_index),
            ) else {
                return;
            };
            telemetry.finish(&Err(error.clone()));
        }
    }
}

fn batch_statement_index(error: &LixError) -> Option<usize> {
    error
        .details
        .as_ref()
        .and_then(JsonValue::as_object)
        .and_then(|details| details.get("statementIndex"))
        .and_then(JsonValue::as_u64)
        .and_then(|index| usize::try_from(index).ok())
}

async fn execute_transaction_write_auto<StorageImpl>(
    transaction: &mut crate::transaction::Transaction<StorageImpl>,
    sql: &str,
    statement: datafusion::sql::parser::Statement,
    params: &[Value],
    options: ExecuteOptions,
    metadata: ExecuteStatementMetadata,
    checkpoint_post_stage_returning: bool,
) -> Result<ExecuteResult, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let previous_origin_key = transaction.replace_origin_key(options.origin_key);
    let result = async {
        match transaction.try_execute_prepared_mutation(sql, params).await {
            Ok(Some(result)) => return Ok(ExecuteResult::from_sql_write_result(result)),
            Ok(None) => {}
            Err(error) => return Err(error),
        }
        let tx_plan = transaction.prepare_sql_write_logical_plan(sql, &statement)?;
        transaction.remember_prepared_mutation(sql, &tx_plan)?;
        // The first statement is the producer of the typed program. Feed it
        // through that program immediately so a homogeneous transaction never
        // creates one legacy prepared row before entering the journal.
        match transaction.try_execute_prepared_mutation(sql, params).await {
            Ok(Some(result)) => return Ok(ExecuteResult::from_sql_write_result(result)),
            Ok(None) => {}
            Err(error) => return Err(error),
        }
        let checkpoint = (checkpoint_post_stage_returning
            && sql2::write_plan_requires_post_stage_returning_checkpoint(&tx_plan))
        .then(|| transaction.begin_sql_statement_checkpoint())
        .transpose()?;
        let result =
            execute_prepared_transaction_write(transaction, tx_plan, params, &metadata).await;
        if result.is_err() {
            if let Some(checkpoint) = checkpoint {
                transaction
                    .rollback_sql_statement_checkpoint(checkpoint)
                    .await?;
            }
        }
        let result = result?;
        Ok(ExecuteResult::from_sql_write_result(result))
    }
    .await;
    transaction.replace_origin_key(previous_origin_key);
    result
}

async fn execute_prepared_transaction_write<StorageImpl>(
    transaction: &mut crate::transaction::Transaction<StorageImpl>,
    plan: sql2::SqlLogicalPlan,
    params: &[Value],
    metadata: &ExecuteStatementMetadata,
) -> Result<sql2::SqlWriteResult, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    if let Some((command, query_sql, returning)) = sql2::diff_command_query(&plan) {
        let outcome = transaction
            .execute_diff_command_query_owned(command, query_sql, params.to_vec())
            .await?;
        return sql2::SqlWriteResult::diff_command(outcome, returning.as_ref());
    }
    sql2::execute_write_logical_plan_result_with_metadata(transaction, plan, params, metadata).await
}

/// Returns true only when SQL directly delivers one file's bytes to the
/// caller. Materializing `data` inside an aggregate, join, filter, or derived
/// expression is not acknowledgement: the caller did not receive those bytes
/// and must not gain the ability to delete entities that only existed there.
///
/// This intentionally recognizes a narrow, predictable MVP surface. False
/// negatives merely preserve an omitted entity; false positives can lose one.
fn is_acknowledgeable_file_content_read(statement: &DataFusionStatement, params: &[Value]) -> bool {
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
                } if direct_column_name(expression).as_deref() == Some("content")
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

struct SimpleSingleTableSelect<'a> {
    query: &'a Query,
    select: &'a Select,
    table_identifier: &'a Ident,
    table_name: String,
    unqualified_unquoted_table: bool,
    alias: Option<&'a TableAlias>,
}

fn simple_single_table_select(
    statement: &DataFusionStatement,
) -> Option<SimpleSingleTableSelect<'_>> {
    let DataFusionStatement::Statement(statement) = statement else {
        return None;
    };
    let SqlStatement::Query(query) = statement.as_ref() else {
        return None;
    };
    if query.with.is_some()
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
    let table_identifier = name.0.last().and_then(|part| part.as_ident())?;
    let table_name = table_identifier.value.to_ascii_lowercase();

    Some(SimpleSingleTableSelect {
        query,
        select,
        table_identifier,
        table_name,
        unqualified_unquoted_table: name.0.len() == 1 && table_identifier.quote_style.is_none(),
        alias: alias.as_ref(),
    })
}

fn simple_point_read(statement: &DataFusionStatement) -> Option<SimplePointRead<'_>> {
    let simple = simple_single_table_select(statement)?;
    if simple.query.order_by.is_some()
        || !point_read_limit_is_safe(simple.query.limit_clause.as_ref())
        || simple.query.fetch.is_some()
    {
        return None;
    }

    simple.select.selection.as_ref()?;
    Some(SimplePointRead {
        select: simple.select,
        table_name: simple.table_name,
        exact_table_shape: simple.unqualified_unquoted_table
            && simple.alias.is_none()
            && simple.query.limit_clause.is_none(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LateMaterializedLixFileContentRead {
    statement: Box<DataFusionStatement>,
    data_column_index: usize,
}

/// Defers an unchanged `lix_file.content` projection until DataFusion has applied
/// metadata predicates, ordering, and limits. This keeps SQL semantics in
/// DataFusion while preventing large file bytes from entering Arrow at all.
fn late_materialized_lix_file_content_read(
    statement: &DataFusionStatement,
) -> Option<LateMaterializedLixFileContentRead> {
    let simple = simple_single_table_select(statement)?;
    if simple.table_name != "lix_file"
        || !simple.unqualified_unquoted_table
        || simple.alias.is_some_and(|alias| !alias.columns.is_empty())
    {
        return None;
    }
    let qualifier = simple
        .alias
        .map_or(simple.table_identifier, |alias| &alias.name)
        .clone();

    let mut statement = statement.clone();
    let DataFusionStatement::Statement(sql_statement) = &mut statement else {
        return None;
    };
    let SqlStatement::Query(query) = sql_statement.as_mut() else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return None;
    };

    let mut data_column_index = None;
    let mut data_output_name = None;
    for (index, item) in select.projection.iter_mut().enumerate() {
        let expression = match item {
            SelectItem::UnnamedExpr(expression)
            | SelectItem::ExprWithAlias {
                expr: expression, ..
            } => expression,
            SelectItem::QualifiedWildcard(..) | SelectItem::Wildcard(..) => return None,
        };
        let projected_column = direct_projection_identifier(expression)?;
        if identifier_matches(projected_column, "content") {
            if data_column_index.is_some() {
                return None;
            }
            let (path_expression, output_name) =
                replaceable_lix_file_content_projection(item, &qualifier)?;
            *item = SelectItem::ExprWithAlias {
                expr: path_expression,
                alias: output_name.clone(),
            };
            data_column_index = Some(index);
            data_output_name = Some(output_name.value.to_ascii_lowercase());
        }
    }
    let data_column_index = data_column_index?;
    let data_output_name = data_output_name?;

    if select
        .selection
        .as_ref()
        .is_some_and(|selection| expression_mentions_column(selection, "content"))
    {
        return None;
    }
    if let Some(order_by) = &query.order_by {
        if order_by.interpolate.is_some() {
            return None;
        }
        let OrderByKind::Expressions(expressions) = &order_by.kind else {
            return None;
        };
        if expressions.iter().any(|order| {
            order.with_fill.is_some()
                || direct_column_name(&order.expr)
                    .is_none_or(|column| column == "content" || column == data_output_name)
        }) {
            return None;
        }
    }

    Some(LateMaterializedLixFileContentRead {
        statement: Box::new(statement),
        data_column_index,
    })
}

fn replaceable_lix_file_content_projection(
    item: &SelectItem,
    qualifier: &Ident,
) -> Option<(Expr, Ident)> {
    let (expression, output_name) = match item {
        SelectItem::UnnamedExpr(expression) => {
            let output_name = direct_projection_identifier(expression)?.clone();
            (expression, output_name)
        }
        SelectItem::ExprWithAlias { expr, alias } => (expr, alias.clone()),
        SelectItem::QualifiedWildcard(..) | SelectItem::Wildcard(..) => return None,
    };
    let path_expression = direct_file_content_path_expression(expression, qualifier)?;
    Some((path_expression, output_name))
}

fn direct_file_content_path_expression(expression: &Expr, qualifier: &Ident) -> Option<Expr> {
    match expression {
        Expr::Identifier(identifier) if identifier_matches(identifier, "content") => {
            let mut path = identifier.clone();
            path.value = "path".to_string();
            Some(Expr::Identifier(path))
        }
        Expr::CompoundIdentifier(identifiers) => {
            let [expression_qualifier, identifier] = identifiers.as_slice() else {
                return None;
            };
            if !identifiers_match(expression_qualifier, qualifier)
                || !identifier_matches(identifier, "content")
            {
                return None;
            }
            let mut identifiers = identifiers.clone();
            identifiers.last_mut()?.value = "path".to_string();
            Some(Expr::CompoundIdentifier(identifiers))
        }
        _ => None,
    }
}

fn direct_projection_identifier(expression: &Expr) -> Option<&Ident> {
    match expression {
        Expr::Identifier(identifier) => Some(identifier),
        Expr::CompoundIdentifier(identifiers) => identifiers.last(),
        _ => None,
    }
}

fn identifier_matches(identifier: &Ident, expected: &str) -> bool {
    if identifier.quote_style.is_some() {
        identifier.value == expected
    } else {
        identifier.value.eq_ignore_ascii_case(expected)
    }
}

fn identifiers_match(left: &Ident, right: &Ident) -> bool {
    if left.quote_style.is_some() || right.quote_style.is_some() {
        left.quote_style == right.quote_style && left.value == right.value
    } else {
        left.value.eq_ignore_ascii_case(&right.value)
    }
}

fn expression_mentions_column(expression: &Expr, column: &str) -> bool {
    let mut visitor = ColumnReferenceVisitor {
        column,
        found: false,
    };
    let _ = expression.visit(&mut visitor);
    visitor.found
}

struct ColumnReferenceVisitor<'a> {
    column: &'a str,
    found: bool,
}

impl Visitor for ColumnReferenceVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
        if direct_column_name(expression).as_deref() == Some(self.column) {
            self.found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExactFilesystemRead {
    RootFileListing,
    RootDirectoryListing,
    Point(sql2::ExactLixFileReadSelector, sql2::ExactLixFileReadColumn),
    PathContentBatch(BTreeSet<String>),
    IdManifestBatch(BTreeSet<String>),
}

fn exact_filesystem_read_route(
    statement: &DataFusionStatement,
    params: &[Value],
) -> Option<ExactFilesystemRead> {
    if exact_lix_file_root_listing(statement, params) {
        return Some(ExactFilesystemRead::RootFileListing);
    }
    if exact_lix_directory_root_listing(statement, params) {
        return Some(ExactFilesystemRead::RootDirectoryListing);
    }
    if let Some((selector, column)) = exact_lix_file_point_read(statement, params) {
        return Some(ExactFilesystemRead::Point(selector, column));
    }

    let point_read = simple_point_read(statement)?;
    if point_read.table_name != "lix_file" || !point_read.exact_table_shape {
        return None;
    }
    exact_path_content_batch(point_read.select, params)
        .map(ExactFilesystemRead::PathContentBatch)
        .or_else(|| {
            exact_id_manifest_batch(point_read.select, params)
                .map(ExactFilesystemRead::IdManifestBatch)
        })
}

fn exact_lix_file_root_listing(statement: &DataFusionStatement, params: &[Value]) -> bool {
    exact_root_listing(
        statement,
        params,
        "lix_file",
        &["id", "path", "name", "lixcol_metadata", "lixcol_updated_at"],
        "directory_id",
    )
}

fn exact_lix_directory_root_listing(statement: &DataFusionStatement, params: &[Value]) -> bool {
    exact_root_listing(
        statement,
        params,
        "lix_directory",
        &["id", "path", "name", "lixcol_updated_at"],
        "parent_id",
    )
}

fn exact_root_listing(
    statement: &DataFusionStatement,
    params: &[Value],
    table_name: &str,
    projection: &[&str],
    parent_column: &str,
) -> bool {
    if !params.is_empty() {
        return false;
    }
    let Some(simple) = simple_single_table_select(statement) else {
        return false;
    };
    if simple.table_name != table_name
        || !simple.unqualified_unquoted_table
        || simple.alias.is_some()
        || simple.query.limit_clause.is_some()
        || simple.query.fetch.is_some()
    {
        return false;
    }
    if simple.select.projection.len() != projection.len()
        || !simple
            .select
            .projection
            .iter()
            .zip(projection)
            .all(|(item, expected)| {
                let SelectItem::UnnamedExpr(expression) = item else {
                    return false;
                };
                exact_point_column(expression).as_deref() == Some(*expected)
            })
    {
        return false;
    }
    let Some(Expr::IsNull(parent)) = simple.select.selection.as_ref() else {
        return false;
    };
    if exact_point_column(parent).as_deref() != Some(parent_column) {
        return false;
    }
    let Some(order_by) = &simple.query.order_by else {
        return false;
    };
    if order_by.interpolate.is_some() {
        return false;
    }
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return false;
    };
    let [order] = expressions.as_slice() else {
        return false;
    };
    order.with_fill.is_none()
        && order.options.asc != Some(false)
        && order.options.nulls_first.is_none()
        && exact_point_column(&order.expr).as_deref() == Some("name")
}

fn exact_lix_file_point_read(
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
        "content" => sql2::ExactLixFileReadColumn::Content,
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

/// Recognizes the exact batch download shape used by Lixray. Keeping the
/// projection and numbered placeholders strict makes the direct result path
/// equivalent to the DataFusion query without reimplementing general SQL.
fn exact_path_content_batch(select: &Select, params: &[Value]) -> Option<BTreeSet<String>> {
    let [
        SelectItem::UnnamedExpr(path_projection),
        SelectItem::UnnamedExpr(content_projection),
    ] = select.projection.as_slice()
    else {
        return None;
    };
    if exact_point_column(path_projection).as_deref() != Some("path")
        || exact_point_column(content_projection).as_deref() != Some("content")
    {
        return None;
    }
    let Expr::InList {
        expr,
        list,
        negated: false,
    } = select.selection.as_ref()?
    else {
        return None;
    };
    if exact_point_column(expr).as_deref() != Some("path")
        || list.is_empty()
        || list.len() != params.len()
    {
        return None;
    }

    let mut paths = BTreeSet::new();
    for (index, (expression, param)) in list.iter().zip(params).enumerate() {
        let Expr::Value(value) = expression else {
            return None;
        };
        let SqlValue::Placeholder(placeholder) = &value.value else {
            return None;
        };
        if placeholder != &format!("${}", index + 1) {
            return None;
        }
        let Value::Text(path) = param else {
            return None;
        };
        paths.insert(path.clone());
    }
    Some(paths)
}

/// Recognizes the exact changed-file manifest verification shape. Keeping the
/// projection and parameter-only id list strict avoids changing general SQL
/// semantics while bypassing repeated DataFusion setup for bounded exact
/// batches.
fn exact_id_manifest_batch(select: &Select, params: &[Value]) -> Option<BTreeSet<String>> {
    let [
        SelectItem::UnnamedExpr(id_projection),
        SelectItem::UnnamedExpr(path_projection),
        SelectItem::UnnamedExpr(content_projection),
        SelectItem::UnnamedExpr(metadata_projection),
    ] = select.projection.as_slice()
    else {
        return None;
    };
    if exact_point_column(id_projection).as_deref() != Some("id")
        || exact_point_column(path_projection).as_deref() != Some("path")
        || exact_point_column(content_projection).as_deref() != Some("content")
        || exact_point_column(metadata_projection).as_deref() != Some("lixcol_metadata")
    {
        return None;
    }
    let Expr::InList {
        expr,
        list,
        negated: false,
    } = select.selection.as_ref()?
    else {
        return None;
    };
    if exact_point_column(expr).as_deref() != Some("id")
        || list.is_empty()
        || list.len() != params.len()
    {
        return None;
    }
    let mut ids = BTreeSet::new();
    for (index, (expression, param)) in list.iter().zip(params).enumerate() {
        let Expr::Value(value) = expression else {
            return None;
        };
        let SqlValue::Placeholder(placeholder) = &value.value else {
            return None;
        };
        if placeholder != &format!("${}", index + 1) {
            return None;
        }
        let Value::Text(id) = param else {
            return None;
        };
        ids.insert(id.clone());
    }
    (ids.len() == list.len()).then_some(ids)
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

fn execution_disposition(
    statement: &datafusion::sql::parser::Statement,
) -> Result<ExecutionDisposition, LixError> {
    match sql2::bind_statement_route(statement)? {
        sql2::BoundStatementRoute::Write => Ok(ExecutionDisposition::Durable),
        sql2::BoundStatementRoute::Read
            if sql2::statement_has_durable_runtime_function(statement) =>
        {
            Ok(ExecutionDisposition::Durable)
        }
        sql2::BoundStatementRoute::Read => Ok(ExecutionDisposition::CancellableRead),
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
    if let Some(first) = statements.first()
        && statements
            .iter()
            .skip(1)
            .all(|statement| statement.sql == first.sql)
    {
        let parsed = planning_cache
            .parse_statement(&first.sql)
            .map_err(|error| with_batch_statement_index(error, 0))?;
        let disposition =
            execution_disposition(&parsed).map_err(|error| with_batch_statement_index(error, 0))?;
        return if disposition == ExecutionDisposition::Durable {
            Ok(ExecuteBatchExecution::Transaction(
                TransactionBatchStatements::Shared {
                    statement: parsed,
                    len: statements.len(),
                },
            ))
        } else {
            Ok(ExecuteBatchExecution::ReadOnly(vec![
                parsed;
                statements.len()
            ]))
        };
    }

    // Distinct literal UPDATE statements have the same execution shape as a
    // homogeneous bound batch. Explicit transactions already normalize this
    // narrow SQL subset one statement at a time; recognize the complete batch
    // here so the ordered mutation kernel can fold repeated identities and use
    // the certified columnar route. Shapes that have not yet been lowered into
    // that mutation program retain their original sequential execution.
    if statements.len() >= 2
        && statements
            .iter()
            .all(|statement| statement.params.is_empty())
        && let Some(first) = planning_cache.auto_parameterized_update(&statements[0].sql)
        && statements[1..].iter().all(|statement| {
            planning_cache.update_literal_shape_matches(&statement.sql, first.sql.as_ref())
        })
    {
        let mut builders = Vec::with_capacity(first.params.len());
        // Every decoded literal is a substring of its source SQL (doubled
        // quotes only shrink it), so total SQL bytes are a conservative proof
        // that every parameter column fits Arrow's 32-bit Utf8 offsets.
        let large_offsets = statements.iter().fold(0_usize, |total, statement| {
            total.saturating_add(statement.sql.len())
        }) > i32::MAX as usize;
        let per_column_byte_cap = MAX_INITIAL_LITERAL_COLUMN_BYTES / first.params.len().max(1);
        for param in &first.params {
            let Value::Text(value) = param else {
                unreachable!("auto-parameterized string literals produce text parameters")
            };
            let mut builder = LiteralParameterBuilder::with_capacity(
                large_offsets,
                statements.len(),
                value
                    .len()
                    .saturating_mul(statements.len())
                    .min(per_column_byte_cap),
            );
            builder.append_value(value);
            builders.push(builder);
        }
        let mut decoded_params = first
            .params
            .iter()
            .map(|param| match param {
                Value::Text(value) => String::with_capacity(value.len()),
                _ => unreachable!("auto-parameterized string literals produce text parameters"),
            })
            .collect::<Vec<_>>();
        for statement in &statements[1..] {
            assert!(
                planning_cache
                    .decode_certified_update_literals_into(&statement.sql, &mut decoded_params,),
                "the non-retaining pass certified this UPDATE shape"
            );
            for (builder, value) in builders.iter_mut().zip(&decoded_params) {
                builder.append_value(value);
            }
        }
        let data_type = if large_offsets {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        };
        let fields = (0..builders.len())
            .map(|index| Field::new(format!("${}", index + 1), data_type.clone(), false))
            .collect::<Vec<_>>();
        let columns = builders
            .iter_mut()
            .map(LiteralParameterBuilder::finish)
            .collect::<Vec<_>>();
        let parameter_batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| {
                LixError::unknown(format!(
                    "failed to construct literal UPDATE parameter batch: {error}"
                ))
            })?;
        return Ok(ExecuteBatchExecution::Transaction(
            TransactionBatchStatements::AutoParameterizedUpdate {
                sql: first.sql,
                statement: first.statement,
                parameter_batch,
            },
        ));
    }

    let mut parsed = Vec::with_capacity(statements.len());
    let mut is_read_only = true;
    for (statement_index, statement) in statements.iter().enumerate() {
        let parsed_statement = planning_cache
            .parse_statement(&statement.sql)
            .map_err(|error| with_batch_statement_index(error, statement_index))?;
        let disposition = execution_disposition(&parsed_statement)
            .map_err(|error| with_batch_statement_index(error, statement_index))?;
        if disposition == ExecutionDisposition::Durable {
            is_read_only = false;
        }
        parsed.push(parsed_statement);
    }
    if is_read_only {
        Ok(ExecuteBatchExecution::ReadOnly(parsed))
    } else {
        Ok(ExecuteBatchExecution::Transaction(
            TransactionBatchStatements::Distinct(parsed),
        ))
    }
}

async fn execute_transaction_statement<StorageImpl>(
    transaction: &mut crate::transaction::Transaction<StorageImpl>,
    sql: &str,
    statement: datafusion::sql::parser::Statement,
    params: &[Value],
    options: ExecuteOptions,
    metadata: ExecuteStatementMetadata,
) -> Result<ExecuteResult, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    match sql2::bind_statement_route(&statement)? {
        sql2::BoundStatementRoute::Write => {
            execute_transaction_write_auto(
                transaction,
                sql,
                statement,
                params,
                options,
                metadata,
                false,
            )
            .await
        }
        sql2::BoundStatementRoute::Read => transaction
            .execute_read_sql_statement(sql.to_string(), statement, params.to_vec())
            .await
            .map(ExecuteResult::from_sql_query_result),
    }
}

fn idempotency_outcome_unknown() -> LixError {
    LixError::new(
        LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN,
        "the matching idempotency receipt is not yet durably visible",
    )
    .with_hint("Retry later with the same Idempotency-Key; do not issue a new mutation.")
    .with_details(serde_json::json!({
        "retryable": true,
        "retryScope": "same-idempotency-key",
        "outcome": "unknown",
    }))
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
    use crate::{Memory, engine::Engine};

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

    #[tokio::test]
    async fn active_branch_update_reads_global_overlay_through_one_forktree_view() {
        let session = open_session().await;
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('overlay-key', 'before')",
                &[],
            )
            .await
            .expect("the seed row should commit");

        let updated = session
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'overlay-key'",
                &[],
            )
            .await
            .expect("the active-branch update should resolve the global overlay");
        assert_eq!(updated.rows_affected(), 1);

        let result = session
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'overlay-key'",
                &[],
            )
            .await
            .expect("the updated row should remain readable");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0]
                .get::<serde_json::Value>("value")
                .expect("value should be JSON"),
            serde_json::json!("after")
        );
    }

    #[tokio::test]
    async fn single_file_upsert_uses_the_native_filesystem_path() {
        let session = open_session().await;

        assert_eq!(
            session
                .upsert_file_content(
                    "/native-upsert.txt".to_string(),
                    Blob::from(b"old".as_slice())
                )
                .await
                .expect("the native file upsert should create the file"),
            1
        );
        assert_eq!(
            session
                .upsert_file_content(
                    "/native-upsert.txt".to_string(),
                    Blob::from(b"new".as_slice())
                )
                .await
                .expect("the native file upsert should update the file"),
            1
        );

        let result = session
            .execute(
                "SELECT content FROM lix_file WHERE path = '/native-upsert.txt'",
                &[],
            )
            .await
            .expect("the upserted file should remain queryable");
        assert_eq!(result.len(), 1);
        assert_eq!(
            result.rows()[0]
                .get::<Blob>("content")
                .expect("content should be a blob"),
            Blob::from(b"new".as_slice())
        );
    }
}
