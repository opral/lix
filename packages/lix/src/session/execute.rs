use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::ControlFlow;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use crate::binary_cas::BlobId;
use crate::branch::BranchRefReader;
use crate::common::ExecuteStatementMetadata;
use crate::functions::{FunctionContext, FunctionProviderHandle};
use crate::sql_telemetry::{SqlStatementTelemetry, finish_operation, start_batch};
use crate::sql2;
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    SharedStorageAdapterRead, StorageAdapter, StorageAdapterRead, StorageAdapterReadScope,
    StorageReadDurability, StorageReadOptions, StorageWriteOptions, StorageWriteSet,
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
use super::transaction::{SessionTransaction, transaction_state_error};
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
    #[cfg(feature = "storage-benches")]
    profile_provider_rows_examined: u64,
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
            #[cfg(feature = "storage-benches")]
            profile_provider_rows_examined: 0,
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
            #[cfg(feature = "storage-benches")]
            profile_provider_rows_examined: 0,
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
            #[cfg(feature = "storage-benches")]
            profile_provider_rows_examined: 0,
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
            Value::Json(value) => Ok(value.to_value()),
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

// Keep the single-file legacy-topology fallback semantically identical to the
// public `lix_file` upsert. Batch writes intentionally stay direct-only.
const NATIVE_FILE_UPSERT_SQL: &str = "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
    ON CONFLICT (path) DO UPDATE SET content = excluded.content";

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
    pub(crate) fn execution_disposition(
        &self,
        sql: &str,
    ) -> Result<ExecutionDisposition, LixError> {
        let statement = self.sql_planning_cache.parse_statement(sql)?;
        execution_disposition(&statement)
    }

    /// Classifies an atomic SQL batch for a caller that owns its transport
    /// lifecycle.
    ///
    /// A batch is cancellable only when every parsed and bound statement is a
    /// pure read. Any durable statement makes the whole batch durable so its
    /// atomic transaction can complete after a transport disconnects.
    pub(crate) fn execute_batch_disposition(
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

    /// Executes one PostgreSQL-dialect SQL statement against this Lix session.
    ///
    /// Lix supports a PostgreSQL-dialect subset executed by DataFusion.
    /// Positional placeholders use `$1`, `$2`, and so on. Parsing PostgreSQL
    /// syntax does not imply support for every PostgreSQL statement or runtime
    /// feature. Use `information_schema` for catalog inspection. Lix owns
    /// transaction boundaries for each statement.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        Box::pin(self.execute_with_options(sql, params, ExecuteOptions::default())).await
    }

    /// Executes one statement and reports neutral columnar phase timings.
    ///
    /// This diagnostic API is available only to storage benchmarks. It uses
    /// the normal public execution path and does not alter query semantics.
    #[cfg(feature = "storage-benches")]
    pub(crate) async fn execute_profiled(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<(ExecuteResult, crate::SqlReadProfile), LixError> {
        let (result, mut profile) = crate::sql_profile::scope(self.execute(sql, params)).await;
        if let Ok(result) = &result {
            if result.profile_provider_rows_examined != 0 {
                profile.scan_rows = profile.scan_rows.saturating_add(result.len() as u64);
            }
            profile.provider_rows_examined = profile
                .provider_rows_examined
                .saturating_add(result.profile_provider_rows_examined);
        }
        result.map(|result| (result, profile))
    }

    /// Benchmark-only comparison of the eager result path with internal
    /// collected-batch and live-batch consumers. No stream escapes the scoped
    /// storage read, and this does not change the public execution contract.
    #[cfg(feature = "storage-benches")]
    pub(crate) async fn execute_result_streaming_profiled(
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
            with_static_session_sql_read::<StorageImpl, _, _, _>(
                read_scope,
                |read_store: SharedStorageAdapterRead<StorageImpl::Read<'static>>| async move {
                    let active_branch_id = self.active_branch_id_from_reader(&read_store).await?;
                    let ctx = SessionSqlExecutionContext {
                        active_branch_id: &active_branch_id,
                        active_account_id: self.active_account_id(),
                        read_store,
                        hot_state: Arc::clone(&self.hot_state),
                        binary_cas: Arc::clone(&self.binary_cas),
                        branch_ctx: Arc::clone(&self.branch_ctx),
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

    pub(crate) async fn execute_with_options(
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

    pub(crate) async fn execute_with_options_and_metadata(
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
    pub(crate) fn execute_with_idempotency_and_options_and_metadata(
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
    pub(crate) async fn upsert_file_content(
        &self,
        path: String,
        content: Blob,
    ) -> Result<u64, LixError> {
        self.ensure_open()?;
        // Preserve the public filesystem path contract before entering a
        // write transaction. The lower-level fast helper maps this validation
        // through DataFusion for SQL callers; this native surface should keep
        // its specific path errors intact.
        crate::common::LixPath::try_from_file_path(&path)?;
        let write_access = self.begin_session_write_access().await?;
        let sql_planning_cache = Arc::clone(&self.sql_planning_cache);
        self.with_write_transaction_reserved_lending(
            write_access,
            async move |transaction| {
                // `Blob` is reference-counted. Retaining a copy lets the rare
                // general-write fallback reuse the same payload without a
                // second allocation or a second transaction.
                let fast_path = sql2::execute_fast_lix_file_path_writes(
                    transaction,
                    vec![(path.clone(), content.clone(), None, None)],
                    sql2::FastLixFilePathWriteConflict::UpdateContent,
                    None,
                )
                .await?;
                if let Some(count) = fast_path {
                    return Ok(count);
                }

                // The fast helper declines pre-existing cross-scope path
                // collisions before staging anything. The general provider
                // handles those valid legacy layouts, so keep this request in
                // the same transaction and use its public upsert semantics.
                let statement = sql_planning_cache.parse_statement(NATIVE_FILE_UPSERT_SQL)?;
                let plan = transaction
                    .prepare_sql_write_logical_plan(NATIVE_FILE_UPSERT_SQL, &statement)?;
                sql2::execute_write_logical_plan_result_with_metadata(
                    transaction,
                    plan,
                    &[Value::Text(path), Value::Blob(content)],
                    &ExecuteStatementMetadata::default(),
                )
                .await
                .map(|result| result.rows_affected)
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
    pub(crate) async fn upsert_file_content_batch(
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
    pub(crate) fn read_file_content(
        &self,
        path: String,
        requested_range: Option<Range<u64>>,
    ) -> impl Future<Output = Result<Option<FileRead>, LixError>> + Send + '_ {
        self.read_file_content_inner(path, requested_range)
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
        let (content, file_view_mutations) = with_static_session_sql_read::<StorageImpl, _, _, _>(
            read_scope,
            |read_store: SharedStorageAdapterRead<StorageImpl::Read<'static>>| async move {
                let active_branch_id = self.active_branch_id_from_reader(&read_store).await?;
                let plugin_cache_snapshot = read_store.snapshot_cache_key();
                let hot_state: Arc<dyn crate::hot_state::HotStateReader> =
                    Arc::new(self.hot_state.reader(read_store.clone()));
                let filesystem_path_index: Arc<dyn crate::filesystem::FilesystemPathIndexReader> =
                    Arc::new(self.hot_state.reader(read_store.clone()));
                let branch_ref: Arc<dyn BranchRefReader> =
                    Arc::new(self.branch_ctx.ref_reader(read_store.clone()));
                let blob_reader: Arc<dyn crate::binary_cas::BlobDataReader> =
                    Arc::new(self.binary_cas.reader(read_store));
                // A raw file download delivers the same bytes as a direct
                // `lix_file.content` read, so it must acknowledge rendered
                // plugin state for subsequent collaborative writes.
                let file_view_collector = sql2::SessionFileViews::default();
                let result = sql2::execute_exact_lix_file_batch_read(
                    &active_branch_id,
                    hot_state,
                    filesystem_path_index,
                    branch_ref,
                    blob_reader,
                    self.plugin_host.clone(),
                    Some(file_view_collector.clone()),
                    plugin_cache_snapshot,
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
        let exact_schema_point_read = exact_filesystem_read
            .is_none()
            .then(|| exact_schema_point_read_route(&statement, params))
            .flatten();
        let exact_schema_batch_read = (exact_filesystem_read.is_none()
            && exact_schema_point_read.is_none())
            .then(|| exact_schema_batch_read_route(&statement, params))
            .flatten();
        let late_file_content_read = (exact_filesystem_read.is_none()
            && exact_schema_point_read.is_none()
            && exact_schema_batch_read.is_none())
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
        let read_result = with_static_session_sql_read::<StorageImpl, _, _, _>(
            read_scope,
            |read_store: SharedStorageAdapterRead<StorageImpl::Read<'static>>| async move {
                self.execute_read_statement_with_store(
                    read_store,
                    sql,
                    statement,
                    params,
                    acknowledge_file_views,
                    exact_filesystem_read,
                    exact_schema_point_read,
                    exact_schema_batch_read,
                    late_file_content_read,
                    has_durable_runtime_function,
                )
                .await
            },
        );
        let (mut read_result, file_view_mutations, _provider_rows_examined) =
            match read_result.await {
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
        #[cfg(feature = "storage-benches")]
        let result = {
            let mut result = result;
            result.profile_provider_rows_examined = _provider_rows_examined as u64;
            result
        };
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
    pub(crate) async fn execute_prepared_dml_batch(
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

    pub(crate) async fn execute_batch_with_options(
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

    pub(crate) async fn execute_batch_with_options_and_metadata(
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
    pub(crate) fn execute_batch_with_idempotency_and_options_and_metadata(
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
                    return receipt.into_results();
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
                                receipt.into_results()
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
                            // standard 2 MiB worker stack for ordinary row writes.
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
        let (results, file_view_mutations) = with_static_session_sql_read::<StorageImpl, _, _, _>(
            read_scope,
            |read_store: SharedStorageAdapterRead<StorageImpl::Read<'static>>| async move {
                let file_view_collector =
                    acknowledge_file_views.then(sql2::SessionFileViews::default);
                let active_branch_id = self.active_branch_id_from_reader(&read_store).await?;
                let ctx = SessionSqlExecutionContext {
                    active_branch_id: &active_branch_id,
                    active_account_id: self.active_account_id(),
                    read_store,
                    hot_state: Arc::clone(&self.hot_state),
                    binary_cas: Arc::clone(&self.binary_cas),
                    branch_ctx: Arc::clone(&self.branch_ctx),
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

    pub(crate) fn execute_coherent_read_batch_owned(
        self: Arc<Self>,
        statements: Vec<(String, Vec<Value>)>,
    ) -> impl Future<Output = Result<CoherentReadBatch, LixError>> + Send + 'static {
        // SAFETY: the future owns its Arc session and every SQL/parameter
        // payload. Storage read handles are Send by the Storage contract; the
        // compiler obstruction is the higher-ranked shared reference carried
        // by borrowing adapters such as RocksDB snapshots.
        unsafe {
            super::AssumeSendFuture::new(async move {
                let statement_refs = statements
                    .iter()
                    .map(|(sql, params)| (sql.as_str(), params.as_slice()))
                    .collect::<Vec<_>>();
                self.execute_coherent_read_batch(&statement_refs).await
            })
        }
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
        let (batch, file_view_mutations) = with_static_session_sql_read::<StorageImpl, _, _, _>(
            read_scope,
            |read_store: SharedStorageAdapterRead<StorageImpl::Read<'static>>| async move {
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
                    active_account_id: self.active_account_id(),
                    read_store,
                    hot_state: Arc::clone(&self.hot_state),
                    binary_cas: Arc::clone(&self.binary_cas),
                    branch_ctx: Arc::clone(&self.branch_ctx),
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
    /// `uuidv7()` can still advance runtime state. Persisting happens only
    /// after successful execution so failed reads do not consume durable
    /// sequence state.
    async fn persist_runtime_functions_if_needed(
        &self,
        runtime_functions: FunctionContext,
        has_runtime_write_access: bool,
    ) -> Result<Option<crate::storage_adapter::StorageWriteSetStats>, LixError> {
        let mut writes = StorageWriteSet::new();
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let function_preconditions = runtime_functions
            .stage_persist_if_needed(&read, &mut writes)
            .await?;
        if writes.is_empty() {
            return Ok(None);
        }
        if !has_runtime_write_access {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "runtime function state changed without reserved write access",
            ));
        }
        let commit_boundary = self.transaction_commit_boundary();
        let _commit_guard = begin_commit_boundary(Some(&commit_boundary));
        let mut write_options = StorageWriteOptions::default();
        write_options.preconditions.extend(function_preconditions);
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
        exact_schema_point_read: Option<ExactSchemaPointRead>,
        exact_schema_batch_read: Option<ExactSchemaBatchRead>,
        late_file_content_read: Option<LateMaterializedLixFileContentRead>,
        has_durable_runtime_function: bool,
    ) -> Result<
        (
            sql2::SessionReadSqlResult,
            Vec<sql2::SessionFileViewMutation>,
            usize,
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
        if let Some(exact_filesystem_read) = exact_filesystem_read {
            let query = match exact_filesystem_read {
                ExactFilesystemRead::RootFileListing => {
                    let filesystem_path_index: Arc<
                        dyn crate::filesystem::FilesystemPathIndexReader,
                    > = Arc::new(self.hot_state.reader(read_store.clone()));
                    let branch_ref: Arc<dyn BranchRefReader> =
                        Arc::new(self.branch_ctx.ref_reader(read_store));
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
                    > = Arc::new(self.hot_state.reader(read_store.clone()));
                    let branch_ref: Arc<dyn BranchRefReader> =
                        Arc::new(self.branch_ctx.ref_reader(read_store));
                    sql2::execute_exact_lix_directory_root_listing(
                        &active_branch_id,
                        filesystem_path_index,
                        branch_ref,
                    )
                    .await?
                }
                exact_filesystem_read => {
                    let hot_state: Arc<dyn crate::hot_state::HotStateReader> =
                        Arc::new(self.hot_state.reader(read_store.clone()));
                    let filesystem_path_index: Arc<
                        dyn crate::filesystem::FilesystemPathIndexReader,
                    > = Arc::new(self.hot_state.reader(read_store.clone()));
                    let branch_ref: Arc<dyn BranchRefReader> =
                        Arc::new(self.branch_ctx.ref_reader(read_store.clone()));
                    let blob_reader: Arc<dyn crate::binary_cas::BlobDataReader> =
                        Arc::new(self.binary_cas.reader(read_store));
                    match exact_filesystem_read {
                        ExactFilesystemRead::Point(selector, column) => {
                            sql2::execute_exact_lix_file_read(
                                &active_branch_id,
                                hot_state,
                                filesystem_path_index,
                                branch_ref,
                                blob_reader,
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
                                hot_state,
                                filesystem_path_index,
                                branch_ref,
                                blob_reader,
                                self.plugin_host.clone(),
                                file_view_collector.clone(),
                                None,
                                &paths,
                                None,
                            )
                            .await?
                        }
                        ExactFilesystemRead::IdManifestBatch(file_ids) => {
                            sql2::execute_exact_lix_file_id_manifest_batch_read(
                                &active_branch_id,
                                hot_state,
                                filesystem_path_index,
                                branch_ref,
                                blob_reader,
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
                0,
            ));
        }
        if let Some(exact) = exact_schema_point_read {
            let ctx = SessionSqlExecutionContext {
                active_branch_id: &active_branch_id,
                active_account_id: self.active_account_id(),
                read_store: read_store.clone(),
                hot_state: Arc::clone(&self.hot_state),
                binary_cas: Arc::clone(&self.binary_cas),
                branch_ctx: Arc::clone(&self.branch_ctx),
                catalog_context: Arc::clone(&self.catalog_context),
                sql_planning_cache: Arc::clone(&self.sql_planning_cache),
                functions: FunctionProviderHandle::system(),
                plugin_host: self.plugin_host.clone(),
                file_views: None,
            };
            let catalog = sql2::SqlExecutionContext::public_catalog(&ctx).await?;
            if let Some((spec, row_pk)) =
                resolve_exact_schema_point_read(catalog.as_ref(), &exact)?
            {
                let reader: Arc<dyn sql2::RowSnapshotReader> = Arc::new(
                    sql2::CurrentRowSnapshotReader::new(
                        Arc::clone(&self.hot_state),
                        read_store.clone(),
                    ),
                );
                let query = sql2::execute_exact_schema_point_read(
                    &spec,
                    &active_branch_id,
                    reader,
                    row_pk,
                    &exact.projected_columns,
                    exact.output_columns,
                )
                .await?;
                if let Some(query) = query {
                    return Ok((
                        sql2::SessionReadSqlResult {
                            runtime_functions: None,
                            query: sql2::SessionReadResult::Rows(query),
                        },
                        Vec::new(),
                        1,
                    ));
                }
            }
        }
        if let Some(exact) = exact_schema_batch_read {
            let ctx = SessionSqlExecutionContext {
                active_branch_id: &active_branch_id,
                active_account_id: self.active_account_id(),
                read_store: read_store.clone(),
                hot_state: Arc::clone(&self.hot_state),
                binary_cas: Arc::clone(&self.binary_cas),
                branch_ctx: Arc::clone(&self.branch_ctx),
                catalog_context: Arc::clone(&self.catalog_context),
                sql_planning_cache: Arc::clone(&self.sql_planning_cache),
                functions: FunctionProviderHandle::system(),
                plugin_host: self.plugin_host.clone(),
                file_views: None,
            };
            let catalog = sql2::SqlExecutionContext::public_catalog(&ctx).await?;
            if let Some((spec, identities)) =
                resolve_exact_schema_batch_read(catalog.as_ref(), &exact)?
            {
                let provider_rows_examined = identities.iter().collect::<BTreeSet<_>>().len();
                let hot_state: Arc<dyn crate::hot_state::HotStateReader> =
                    Arc::new(self.hot_state.reader(read_store.clone()));
                let query = sql2::execute_exact_schema_batch_read(
                    &spec,
                    &active_branch_id,
                    hot_state,
                    identities,
                    &exact.projected_columns,
                    exact.output_columns,
                )
                .await?;
                return Ok((
                    sql2::SessionReadSqlResult {
                        runtime_functions: None,
                        query: sql2::SessionReadResult::Rows(query),
                    },
                    Vec::new(),
                    provider_rows_examined,
                ));
            }
        }
        let hot_state: Arc<dyn crate::hot_state::HotStateReader> =
            Arc::new(self.hot_state.reader(read_store.clone()));
        let runtime_functions = if has_durable_runtime_function {
            Some(FunctionContext::prepare(&read_store, None).await?)
        } else {
            None
        };
        // Read providers do not consume durable function state themselves;
        // only the registered timestamp/UUID SQL UDFs do. Keep their AST
        // classifier conservative if new readable statement shapes appear.
        let functions = runtime_functions
            .as_ref()
            .map_or_else(FunctionProviderHandle::system, FunctionContext::provider);
        let (statement, late_file_content_column, rewritten_sql) = match late_file_content_read {
            Some(plan) => {
                let statement = *plan.statement;
                let rewritten_sql = statement.to_string();
                (statement, Some(plan.data_column_index), Some(rewritten_sql))
            }
            None => (statement, None, None),
        };
        let ctx = SessionSqlExecutionContext {
            active_branch_id: &active_branch_id,
            active_account_id: self.active_account_id(),
            read_store: read_store.clone(),
            hot_state: Arc::clone(&self.hot_state),
            binary_cas: Arc::clone(&self.binary_cas),
            branch_ctx: Arc::clone(&self.branch_ctx),
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
            rewritten_sql.as_deref().unwrap_or(sql),
            statement,
            params,
        )
        .await?;
        drop(read_session);
        drop(ctx);
        if let Some(data_column_index) = late_file_content_column {
            let filesystem_path_index: Arc<dyn crate::filesystem::FilesystemPathIndexReader> =
                Arc::new(self.hot_state.reader(read_store.clone()));
            let branch_ref: Arc<dyn BranchRefReader> =
                Arc::new(self.branch_ctx.ref_reader(read_store.clone()));
            let blob_reader: Arc<dyn crate::binary_cas::BlobDataReader> =
                Arc::new(self.binary_cas.reader(read_store));
            let mut materialized = query.query.into_sql_query_result()?;
            hydrate_lix_file_content_result(
                &active_branch_id,
                Arc::clone(&hot_state),
                filesystem_path_index,
                branch_ref,
                blob_reader,
                self.plugin_host.clone(),
                file_view_collector.clone(),
                &mut materialized,
                data_column_index,
            )
            .await?;
            query.query = sql2::SessionReadResult::Rows(materialized);
        }
        drop(hot_state);
        let file_view_mutations = file_view_collector
            .map(|collector| collector.plugin_file_mutations())
            .unwrap_or_default();
        Ok((
            sql2::SessionReadSqlResult {
                runtime_functions,
                query: query.query,
            },
            file_view_mutations,
            0,
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
            .map(|data| materialize_file_read(data, None))
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
) -> Result<Option<Blob>, LixError> {
    if result.columns.as_slice() != ["path", "content"] {
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
    let [Value::Text(path), data] = row.as_mut_slice() else {
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
    Ok(Some(content))
}

fn materialize_file_read(
    data: Blob,
    requested_range: Option<Range<u64>>,
) -> Result<FileRead, LixError> {
    let total_size = u64::try_from(data.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native file size does not fit the public 64-bit range",
        )
    })?;
    let content_identity = BlobId::from_content(data.as_ref()).to_hex();
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
async fn hydrate_lix_file_content_result(
    active_branch_id: &str,
    hot_state: Arc<dyn crate::hot_state::HotStateReader>,
    filesystem_path_index: Arc<dyn crate::filesystem::FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    blob_reader: Arc<dyn crate::binary_cas::BlobDataReader>,
    plugin_host: crate::plugin::runtime::PluginRuntimeHost,
    session_file_views: Option<sql2::SessionFileViews>,
    query: &mut SqlQueryResult,
    data_column_index: usize,
) -> Result<(), LixError> {
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
        hot_state,
        filesystem_path_index,
        branch_ref,
        blob_reader,
        plugin_host,
        session_file_views,
        None,
        &paths,
        None,
    )
    .await?;
    let mut data_by_path = BTreeMap::new();
    for mut row in hydrated.rows {
        let [Value::Text(path), data] = row.as_mut_slice() else {
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
            Value::Text(value) => profile_checksum_sized_bytes(checksum, 4, value.as_bytes()),
            Value::Json(value) => {
                profile_checksum_sized_bytes(checksum, 5, value.to_string().as_bytes())
            }
            Value::Blob(value) => {
                profile_checksum_sized_bytes(checksum, 6, value.as_bytes().as_ref())
            }
            Value::Timestamp(value) => {
                let checksum = profile_checksum_bytes(checksum, &[7]);
                profile_checksum_bytes(checksum, &value.to_le_bytes())
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
        Box::pin(self.execute_with_options_inner(sql, params, ExecuteOptions::default())).await
    }

    /// Executes one public prepared-DML parameter page inside this explicit
    /// transaction. The page is atomic with surrounding statements; callers
    /// use ordinary `execute` for shape changes or dependency barriers.
    pub(crate) async fn execute_prepared_dml_batch(
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
        let may_reuse_literal_shape = self.has_started_statement;
        self.has_started_statement = true;
        // The explicit write lease already keeps one transaction operation
        // active for this handle's lifetime, and `&mut self` excludes an
        // overlapping execute or commit. A second manager guard per statement
        // only repeated mutex/Notify traffic without adding a state boundary.
        let operation = async {
            if may_reuse_literal_shape
                && params.is_empty()
                && let Some((normalized_shape, parameter_count)) = self
                    .transaction
                    .as_ref()
                    .and_then(|transaction| transaction.prepared_literal_mutation_shape())
            {
                if let Some(decoded_values) = self
                    .sql_planning_cache
                    .decode_update_literals_for_cached_shape(
                        sql,
                        normalized_shape,
                        parameter_count,
                        &mut self.prepared_literal_escape_scratch,
                        &mut self.prepared_literal_shape,
                    )
                {
                    #[cfg(feature = "storage-benches")]
                    {
                        let key_bytes = decoded_values.first().map_or(0, |value| value.len());
                        let value_bytes = decoded_values.get(1).map_or(0, |value| value.len());
                        let owned_bytes = decoded_values
                            .iter()
                            .filter_map(|value| match value {
                                std::borrow::Cow::Borrowed(_) => None,
                                std::borrow::Cow::Owned(value) => Some(value.len()),
                            })
                            .sum::<usize>();
                        crate::storage_bench::record_crud_ownership(
                            crate::storage_bench::CRUD_OWNERSHIP_SQL_BOUND,
                            1,
                            key_bytes,
                            value_bytes,
                            decoded_values.len(),
                            decoded_values
                                .iter()
                                .filter(|value| matches!(value, std::borrow::Cow::Owned(_)))
                                .count(),
                            0,
                        );
                        crate::storage_bench::record_crud_ownership_transfer(
                            crate::storage_bench::CRUD_OWNERSHIP_SQL_BOUND,
                            owned_bytes,
                            0,
                            owned_bytes,
                            0,
                        );
                    }
                    let transaction = self
                        .transaction
                        .as_mut()
                        .ok_or_else(|| transaction_state_error("Lix transaction is closed"))?;
                    let result = transaction
                        .try_execute_cached_literal_prepared_mutation(
                            options.origin_key.as_deref(),
                            &decoded_values,
                        )
                        .await;
                    for (index, value) in decoded_values.into_iter().enumerate() {
                        if let std::borrow::Cow::Owned(value) = value {
                            self.prepared_literal_escape_scratch[index] = value;
                        }
                    }
                    match result {
                        Ok(Some(result)) => {
                            return Ok(ExecuteResult::from_sql_write_result(result));
                        }
                        Ok(None) => {}
                        Err(error) => return Err(normalize_sql_surface_error(error, sql)),
                    }
                }
            }
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

    pub(crate) fn execute_with_options(
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

    #[cfg(test)]
    pub(crate) async fn scan_hot_state_for_test(
        &mut self,
        request: &crate::hot_state::HotStateScanRequest,
    ) -> Result<crate::hot_state::MaterializedHotStateBatch, LixError> {
        let _operation_guard = self.begin_session_operation()?;
        let transaction = self.transaction_mut()?;
        transaction.flush_prepared_mutations_for_read().await?;
        <crate::transaction::Transaction<StorageImpl> as sql2::SqlWriteExecutionContext>::scan_hot_state_batch(
            transaction,
            request,
        )
        .await
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
/// and must not gain the ability to delete rows that only existed there.
///
/// This intentionally recognizes a narrow, predictable MVP surface. False
/// negatives merely preserve an omitted row; false positives can lose one.
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
    if !collect_literal_equalities(selection, &mut equality_columns, params) {
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

#[derive(Debug, Clone, PartialEq)]
struct ExactSchemaPointRead {
    table_name: String,
    projected_columns: Vec<String>,
    output_columns: Vec<String>,
    equalities: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
struct ExactSchemaBatchRead {
    table_name: String,
    projected_columns: Vec<String>,
    output_columns: Vec<String>,
    identities: Vec<BTreeMap<String, Value>>,
    order_by_columns: Vec<String>,
}

fn exact_schema_point_read_route(
    statement: &DataFusionStatement,
    params: &[Value],
) -> Option<ExactSchemaPointRead> {
    let simple = simple_single_table_select(statement)?;
    if !simple.unqualified_unquoted_table
        || simple.alias.is_some()
        || simple.query.order_by.is_some()
        || simple.query.fetch.is_some()
        || !point_read_limit_is_safe(simple.query.limit_clause.as_ref())
    {
        return None;
    }
    let mut projected_columns = Vec::with_capacity(simple.select.projection.len());
    let mut output_columns = Vec::with_capacity(simple.select.projection.len());
    for item in &simple.select.projection {
        let (expression, alias) = match item {
            SelectItem::UnnamedExpr(expression) => (expression, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
            _ => return None,
        };
        let column = exact_point_column(expression)?;
        projected_columns.push(column.clone());
        output_columns.push(alias.map_or(column, |alias| alias.value.clone()));
    }
    if projected_columns.is_empty() {
        return None;
    }
    let mut equalities = BTreeMap::new();
    collect_exact_schema_equalities(
        simple.select.selection.as_ref()?,
        params,
        &mut equalities,
    )?;
    Some(ExactSchemaPointRead {
        table_name: simple.table_name,
        projected_columns,
        output_columns,
        equalities,
    })
}

fn exact_schema_batch_read_route(
    statement: &DataFusionStatement,
    params: &[Value],
) -> Option<ExactSchemaBatchRead> {
    let simple = simple_single_table_select(statement)?;
    if !simple.unqualified_unquoted_table
        || simple.alias.is_some()
        || simple.query.fetch.is_some()
        || simple.query.limit_clause.is_some()
    {
        return None;
    }
    let mut projected_columns = Vec::with_capacity(simple.select.projection.len());
    let mut output_columns = Vec::with_capacity(simple.select.projection.len());
    for item in &simple.select.projection {
        let (expression, alias) = match item {
            SelectItem::UnnamedExpr(expression) => (expression, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
            _ => return None,
        };
        let column = exact_point_column(expression)?;
        projected_columns.push(column.clone());
        output_columns.push(alias.map_or(column, |alias| alias.value.clone()));
    }
    if projected_columns.is_empty() {
        return None;
    }
    let order_by_columns = match &simple.query.order_by {
        None => Vec::new(),
        Some(order_by) if order_by.interpolate.is_none() => {
            let OrderByKind::Expressions(expressions) = &order_by.kind else {
                return None;
            };
            expressions
                .iter()
                .map(|order| {
                    (order.with_fill.is_none()
                        && order.options.asc != Some(false)
                        && order.options.nulls_first.is_none())
                    .then(|| exact_point_column(&order.expr))
                    .flatten()
                })
                .collect::<Option<Vec<_>>>()?
        }
        Some(_) => return None,
    };
    let identities = collect_exact_schema_identity_rows(
        simple.select.selection.as_ref()?,
        params,
    )?;
    (identities.len() > 1).then_some(ExactSchemaBatchRead {
        table_name: simple.table_name,
        projected_columns,
        output_columns,
        identities,
        order_by_columns,
    })
}

fn collect_exact_schema_identity_rows(
    expression: &Expr,
    params: &[Value],
) -> Option<Vec<BTreeMap<String, Value>>> {
    match expression {
        Expr::Nested(expression) => collect_exact_schema_identity_rows(expression, params),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            let mut rows = collect_exact_schema_identity_rows(left, params)?;
            rows.extend(collect_exact_schema_identity_rows(right, params)?);
            Some(rows)
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let left = collect_exact_schema_identity_rows(left, params)?;
            let right = collect_exact_schema_identity_rows(right, params)?;
            (left.len().checked_mul(right.len())? <= 4096).then_some(())?;
            let mut rows = Vec::with_capacity(left.len() * right.len());
            for left in left {
                for right in &right {
                    let mut combined = left.clone();
                    let mut compatible = true;
                    for (column, value) in right {
                        if combined
                            .insert(column.clone(), value.clone())
                            .is_some_and(|existing| existing != *value)
                        {
                            compatible = false;
                            break;
                        }
                    }
                    if compatible {
                        rows.push(combined);
                    }
                }
            }
            Some(rows)
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let (column, value) = match (exact_point_column(left), exact_point_column(right)) {
                (Some(column), None) => (column, exact_schema_literal(right, params)?),
                (None, Some(column)) => (column, exact_schema_literal(left, params)?),
                _ => return None,
            };
            Some(vec![BTreeMap::from([(column, value)])])
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if !list.is_empty() => {
            let column = exact_point_column(expr)?;
            list.iter()
                .map(|value| {
                    Some(BTreeMap::from([(
                        column.clone(),
                        exact_schema_literal(value, params)?,
                    )]))
                })
                .collect()
        }
        Expr::IsNull(expression) => {
            let column = exact_point_column(expression)?;
            Some(vec![BTreeMap::from([(column, Value::Null)])])
        }
        _ => None,
    }
}

fn collect_exact_schema_equalities(
    expression: &Expr,
    params: &[Value],
    equalities: &mut BTreeMap<String, Value>,
) -> Option<()> {
    match expression {
        Expr::Nested(expression) => collect_exact_schema_equalities(expression, params, equalities),
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            collect_exact_schema_equalities(left, params, equalities)?;
            collect_exact_schema_equalities(right, params, equalities)
        }
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            let (column, value) = match (exact_point_column(left), exact_point_column(right)) {
                (Some(column), None) => (column, exact_schema_literal(right, params)?),
                (None, Some(column)) => (column, exact_schema_literal(left, params)?),
                _ => return None,
            };
            equalities.insert(column, value).is_none().then_some(())
        }
        _ => None,
    }
}

fn exact_schema_literal(expression: &Expr, params: &[Value]) -> Option<Value> {
    let Expr::Value(value) = expression else {
        return None;
    };
    match &value.value {
        SqlValue::Placeholder(placeholder) => placeholder
            .strip_prefix('$')
            .and_then(|index| index.parse::<usize>().ok())
            .and_then(|index| index.checked_sub(1))
            .and_then(|index| params.get(index))
            .cloned(),
        SqlValue::SingleQuotedString(value) => Some(Value::Text(value.clone())),
        SqlValue::Number(value, _) => value.parse::<i64>().ok().map(Value::Integer),
        _ => None,
    }
}

fn resolve_exact_schema_point_read(
    catalog: &sql2::PublicCatalog,
    exact: &ExactSchemaPointRead,
) -> Result<Option<(sql2::SchemaSurfaceSpec, crate::row_pk::RowPk)>, LixError> {
    let Some(surface) = catalog.surface(&exact.table_name) else {
        return Ok(None);
    };
    let sql2::PublicSurfaceKind::SchemaBase { schema_key } = &surface.kind else {
        return Ok(None);
    };
    let spec = catalog.schema_spec(schema_key).cloned().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "exact schema point route is missing schema metadata",
        )
    })?;
    if exact
        .projected_columns
        .iter()
        .any(|column| spec.visible_column(column).is_none())
    {
        return Ok(None);
    }
    let primary_key_columns = spec
        .primary_key_paths
        .iter()
        .map(|path| match path.as_slice() {
            [column] => Some(column.as_str()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "exact schema point route requires top-level primary-key columns",
            )
        })?;
    if exact.equalities.len() != primary_key_columns.len()
        || exact
            .equalities
            .keys()
            .any(|column| !primary_key_columns.contains(&column.as_str()))
    {
        return Ok(None);
    }
    let parts = primary_key_columns
        .iter()
        .zip(&spec.primary_key_component_types)
        .map(|(column, component_type)| {
            let value = exact.equalities.get(*column)?;
            match (component_type, value) {
                (
                    crate::row_pk::RowPkComponentType::Uuid
                    | crate::row_pk::RowPkComponentType::String
                    | crate::row_pk::RowPkComponentType::Bytes,
                    Value::Text(value),
                ) => Some(value.clone()),
                (crate::row_pk::RowPkComponentType::Integer, Value::Integer(value)) => {
                    Some(value.to_string())
                }
                _ => None,
            }
        })
        .collect::<Option<Vec<_>>>();
    let Some(parts) = parts else {
        return Ok(None);
    };
    let Ok(row_pk) = crate::row_pk::RowPk::from_external_parts(
        parts,
        &spec.primary_key_component_types,
    ) else {
        return Ok(None);
    };
    Ok(Some((spec, row_pk)))
}

fn resolve_exact_schema_batch_read(
    catalog: &sql2::PublicCatalog,
    exact: &ExactSchemaBatchRead,
) -> Result<
    Option<(
        sql2::SchemaSurfaceSpec,
        Vec<(crate::row_pk::RowPk, Option<String>)>,
    )>,
    LixError,
> {
    let Some(surface) = catalog.surface(&exact.table_name) else {
        return Ok(None);
    };
    let sql2::PublicSurfaceKind::SchemaBase { schema_key } = &surface.kind else {
        return Ok(None);
    };
    let spec = catalog.schema_spec(schema_key).cloned().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "exact schema batch route is missing schema metadata",
        )
    })?;
    if exact
        .projected_columns
        .iter()
        .any(|column| spec.visible_column(column).is_none())
    {
        return Ok(None);
    }
    let primary_key_columns = spec
        .primary_key_paths
        .iter()
        .map(|path| match path.as_slice() {
            [column] => Some(column.as_str()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "exact schema batch route requires top-level primary-key columns",
            )
        })?;
    if !exact.order_by_columns.is_empty()
        && exact.order_by_columns
            != primary_key_columns
                .iter()
                .map(|column| (*column).to_owned())
                .collect::<Vec<_>>()
    {
        return Ok(None);
    }
    let mut seen = BTreeSet::new();
    let mut identities = Vec::with_capacity(exact.identities.len());
    for identity in &exact.identities {
        if identity.len() != primary_key_columns.len() + 1
            || identity
                .keys()
                .any(|column| {
                    column != "lixcol_file_id"
                        && !primary_key_columns.contains(&column.as_str())
                })
        {
            return Ok(None);
        }
        let file_id = match identity.get("lixcol_file_id") {
            Some(Value::Null) => None,
            Some(Value::Text(file_id)) => Some(file_id.clone()),
            _ => return Ok(None),
        };
        let parts = primary_key_columns
            .iter()
            .zip(&spec.primary_key_component_types)
            .map(|(column, component_type)| {
                let value = identity.get(*column)?;
                match (component_type, value) {
                    (
                        crate::row_pk::RowPkComponentType::Uuid
                        | crate::row_pk::RowPkComponentType::String
                        | crate::row_pk::RowPkComponentType::Bytes,
                        Value::Text(value),
                    ) => Some(value.clone()),
                    (crate::row_pk::RowPkComponentType::Integer, Value::Integer(value)) => {
                        Some(value.to_string())
                    }
                    _ => None,
                }
            })
            .collect::<Option<Vec<_>>>();
        let Some(parts) = parts else {
            return Ok(None);
        };
        let Ok(row_pk) = crate::row_pk::RowPk::from_external_parts(
            parts,
            &spec.primary_key_component_types,
        ) else {
            return Ok(None);
        };
        if seen.insert((row_pk.clone(), file_id.clone())) {
            identities.push((row_pk, file_id));
        }
    }
    if !exact.order_by_columns.is_empty() {
        identities.sort_unstable();
    }
    Ok(Some((spec, identities)))
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
        SqlValue::Placeholder(placeholder) if params.len() == 1 && placeholder == "$1" => {
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
) -> bool {
    match expression {
        Expr::Nested(expression) => collect_literal_equalities(expression, columns, params),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_literal_equalities(left, columns, params)
                && collect_literal_equalities(right, columns, params)
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let column = match (direct_column_name(left), direct_column_name(right)) {
                (Some(column), None) if point_identity_value_is_text(right, params) => column,
                (None, Some(column)) if point_identity_value_is_text(left, params) => column,
                _ => return false,
            };
            columns.insert(column)
        }
        _ => false,
    }
}

fn point_identity_value_is_text(expression: &Expr, params: &[Value]) -> bool {
    let Expr::Value(value) = expression else {
        return false;
    };
    match &value.value {
        SqlValue::Placeholder(placeholder) => {
            let index = placeholder
                .strip_prefix('$')
                .and_then(|index| index.parse::<usize>().ok())
                .and_then(|index| index.checked_sub(1));
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
    use crate::changelog::{ChangelogContext, ChangelogReader, CommitLoadRequest};
    use crate::row_pk::RowPk;
    use crate::telemetry::{
        CallbackTelemetrySink, CompletedTelemetrySpan, TelemetrySpanKind, TelemetryValue,
    };
    use crate::transaction_types::{RawWriteBatch, TransactionJson, TransactionWriteRow};
    use crate::{
        Memory,
        engine::{Engine, EngineOptions},
    };

    async fn open_session() -> SessionContext<Memory> {
        let storage = Memory::default();
        Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized storage should create engine");
        engine.open_session().await.expect("session should open")
    }

    #[tokio::test]
    async fn exact_registered_schema_point_preserves_typed_public_projection() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "native_point_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "payload", "type": "jsonb", "nullable": true },
                { "name": "optional", "type": "text", "nullable": true }
            ],
            "primary_key": ["id"]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO native_point_probe (id, payload, optional) VALUES ($1, CAST($2 AS JSONB), NULL)",
                &[
                    Value::Text("row-a".into()),
                    Value::Text(r#"{"nested":[true,null],"count":7}"#.into()),
                ],
            )
            .await
            .unwrap();

        let native = session
            .execute(
                "SELECT payload AS body, optional FROM native_point_probe WHERE id = $1 LIMIT 1",
                &[Value::Text("row-a".into())],
            )
            .await
            .unwrap();
        let planned = session
            .execute(
                "SELECT payload AS body, optional FROM native_point_probe WHERE id = CAST($1 AS TEXT) LIMIT 1",
                &[Value::Text("row-a".into())],
            )
            .await
            .unwrap();
        assert_eq!(native.columns(), &["body", "optional"]);
        assert_eq!(native, planned);

        let missing = session
            .execute(
                "SELECT payload FROM native_point_probe WHERE id = $1 LIMIT 1",
                &[Value::Text("missing".into())],
            )
            .await
            .unwrap();
        assert!(missing.is_empty());
    }

    async fn assert_columnar_lifecycle_current(
        session: &SessionContext<Memory>,
        row_count: usize,
        first: &str,
        sample: &str,
    ) {
        let rows = session
            .execute(
                "SELECT id, value FROM columnar_lifecycle_probe ORDER BY id",
                &[],
            )
            .await
            .expect("typed lifecycle scan should succeed");
        assert_eq!(rows.len(), row_count);
        assert_eq!(rows.rows()[0].get::<String>("id").unwrap(), "00000");
        assert_eq!(rows.rows()[0].get::<String>("value").unwrap(), first);
        assert_eq!(rows.rows()[1_023].get::<String>("id").unwrap(), "01023");
        assert_eq!(rows.rows()[1_023].get::<String>("value").unwrap(), sample);
    }

    async fn assert_columnar_layout_selected(
        session: &SessionContext<Memory>,
        schema_key: &str,
        expected_overlay_rows: usize,
    ) {
        let branch_id = session
            .active_branch_id()
            .await
            .expect("active branch should resolve");
        let read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("columnar route read should open");
        let overlay_rows = session
            .hot_state
            .reader(&read)
            .row_columnar_overlay_len_for_test(&branch_id, schema_key)
            .await
            .expect("columnar route should plan")
            .expect("fixture must retain the authenticated columnar path");
        assert_eq!(overlay_rows, expected_overlay_rows);
    }

    async fn assert_current_head_uses_packed_delta_without_columnar_sidecar(
        session: &SessionContext<Memory>,
        schema_key: &str,
        expected_rows: u64,
    ) {
        let branch_id = session
            .active_branch_id()
            .await
            .expect("active branch should resolve");
        let head_read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("branch-head read should open");
        let head = session
            .branch_ctx
            .ref_reader(head_read)
            .load_head(&branch_id)
            .await
            .expect("branch head should load")
            .expect("active branch should have a head");
        let state_read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replacement read should open");
        let replay =
            crate::tracked_state::load_commit_delta_replay_metadata(&state_read, head.commit_id)
                .await
                .expect("replacement metadata should load")
                .expect("current head must publish replacement metadata");
        assert_eq!(u64::from(replay.member_count), expected_rows);
        let id = crate::hot_state::row_group_set_id(head.commit_id, schema_key);
        let manifest = crate::columnar_row_group::load_row_group_manifest(&state_read, id)
            .await
            .expect("columnar manifest lookup should succeed");
        assert!(
            manifest.is_none(),
            "UPDATE replacement parts supersede the synchronous typed sidecar"
        );
    }

    async fn open_session_with_telemetry(
        spans: Arc<std::sync::Mutex<Vec<CompletedTelemetrySpan>>>,
    ) -> SessionContext<Memory> {
        let storage = Memory::default();
        Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let sink = CallbackTelemetrySink::new(move |span| {
            spans.lock().expect("telemetry span lock").push(span);
        });
        let engine =
            Engine::new_with_options(storage, EngineOptions::new().with_telemetry(Arc::new(sink)))
                .await
                .expect("initialized storage should create engine");
        engine.open_session().await.expect("session should open")
    }

    fn batch_statement(sql: &str) -> ExecuteBatchStatement {
        ExecuteBatchStatement {
            label: None,
            sql: sql.to_string(),
            params: Vec::new(),
        }
    }

    #[test]
    fn exact_filesystem_read_recognizes_only_the_narrow_shapes() {
        let root_file_listing = sql2::parse_statement(
            "SELECT id, path, name, lixcol_metadata, lixcol_updated_at \
             FROM lix_file WHERE directory_id IS NULL ORDER BY name",
        )
        .unwrap();
        assert_eq!(
            exact_filesystem_read_route(&root_file_listing, &[]),
            Some(ExactFilesystemRead::RootFileListing)
        );
        let root_directory_listing = sql2::parse_statement(
            "SELECT id, path, name, lixcol_updated_at \
             FROM lix_directory WHERE parent_id IS NULL ORDER BY name",
        )
        .unwrap();
        assert_eq!(
            exact_filesystem_read_route(&root_directory_listing, &[]),
            Some(ExactFilesystemRead::RootDirectoryListing)
        );

        let data_by_id =
            sql2::parse_statement("SELECT content FROM lix_file WHERE id = $1").unwrap();
        assert_eq!(
            exact_filesystem_read_route(
                &data_by_id,
                &[Value::Text(
                    "01920000-0000-7000-8000-0000000000a2".to_string()
                )]
            ),
            Some(ExactFilesystemRead::Point(
                sql2::ExactLixFileReadSelector::Id(
                    "01920000-0000-7000-8000-0000000000a2".to_string()
                ),
                sql2::ExactLixFileReadColumn::Content,
            ))
        );

        let change_by_path =
            sql2::parse_statement("SELECT lixcol_change_id FROM lix_file WHERE path = $1").unwrap();
        assert_eq!(
            exact_filesystem_read_route(&change_by_path, &[Value::Text("/a.txt".to_string())]),
            Some(ExactFilesystemRead::Point(
                sql2::ExactLixFileReadSelector::Path("/a.txt".to_string()),
                sql2::ExactLixFileReadColumn::ChangeId,
            ))
        );

        let data_by_paths =
            sql2::parse_statement("SELECT path, content FROM lix_file WHERE path IN ($1, $2, $3)")
                .unwrap();
        assert_eq!(
            exact_filesystem_read_route(
                &data_by_paths,
                &[
                    Value::Text("/b.txt".to_string()),
                    Value::Text("/a.txt".to_string()),
                    Value::Text("/b.txt".to_string()),
                ],
            ),
            Some(ExactFilesystemRead::PathContentBatch(BTreeSet::from([
                "/a.txt".to_string(),
                "/b.txt".to_string(),
            ])))
        );

        let manifests_by_id = sql2::parse_statement(
            "SELECT id, path, content, lixcol_metadata FROM lix_file WHERE id IN ($1, $2)",
        )
        .unwrap();
        assert_eq!(
            exact_filesystem_read_route(
                &manifests_by_id,
                &[
                    Value::Text("01920000-0000-7000-8000-0000000000a2".to_string()),
                    Value::Text("01920000-0000-7000-8000-0000000000a1".to_string()),
                ],
            ),
            Some(ExactFilesystemRead::IdManifestBatch(BTreeSet::from([
                "01920000-0000-7000-8000-0000000000a1".to_string(),
                "01920000-0000-7000-8000-0000000000a2".to_string(),
            ])))
        );

        for (sql, params) in [
            (
                "SELECT id, path, name, lixcol_updated_at \
                 FROM lix_directory AS directory \
                 WHERE parent_id IS NULL ORDER BY name",
                vec![],
            ),
            (
                "SELECT id, path, name, lixcol_updated_at \
                 FROM lix_directory WHERE parent_id IS NULL ORDER BY path",
                vec![],
            ),
            (
                "SELECT id, path, name, lixcol_updated_at \
                 FROM lix_directory WHERE parent_id IS NULL ORDER BY name DESC",
                vec![],
            ),
            (
                "SELECT id, path, name, lixcol_updated_at \
                 FROM lix_directory WHERE parent_id IS NULL ORDER BY name LIMIT 1",
                vec![],
            ),
            (
                "SELECT id, path, name, lixcol_metadata, lixcol_updated_at \
                 FROM lix_file AS file WHERE directory_id IS NULL ORDER BY name",
                vec![],
            ),
            (
                "SELECT id, path, name, lixcol_metadata, lixcol_updated_at \
                 FROM lix_file WHERE directory_id IS NULL ORDER BY path",
                vec![],
            ),
            (
                "SELECT id, path, name, lixcol_metadata, lixcol_updated_at \
                 FROM lix_file WHERE directory_id IS NULL ORDER BY name DESC",
                vec![],
            ),
            (
                "SELECT id, path, name, lixcol_metadata, lixcol_updated_at \
                 FROM lix_file WHERE directory_id IS NULL ORDER BY name LIMIT 1",
                vec![],
            ),
            (
                "SELECT id, path, name, lixcol_metadata, lixcol_updated_at \
                 FROM lix_file WHERE directory_id IS NULL ORDER BY name",
                vec![Value::Text("unused".to_string())],
            ),
            (
                "SELECT content, path FROM lix_file WHERE path IN ($1, $2)",
                vec![
                    Value::Text("/a.txt".to_string()),
                    Value::Text("/b.txt".to_string()),
                ],
            ),
            (
                "SELECT path, content FROM lix_file WHERE path IN ($2, $1)",
                vec![
                    Value::Text("/a.txt".to_string()),
                    Value::Text("/b.txt".to_string()),
                ],
            ),
            (
                "SELECT path, content FROM lix_file WHERE path IN ($1, $2) ORDER BY path",
                vec![
                    Value::Text("/a.txt".to_string()),
                    Value::Text("/b.txt".to_string()),
                ],
            ),
            (
                "SELECT path, content FROM lix_file WHERE path IN ($1, $2) LIMIT 1",
                vec![
                    Value::Text("/a.txt".to_string()),
                    Value::Text("/b.txt".to_string()),
                ],
            ),
            (
                "SELECT path, content FROM lix_file WHERE path IN ($1, $2)",
                vec![Value::Text("/a.txt".to_string()), Value::Null],
            ),
        ] {
            let statement = sql2::parse_statement(sql).unwrap();
            assert_eq!(
                exact_filesystem_read_route(&statement, &params),
                None,
                "unexpected batch fast-path match for {sql}"
            );
        }

        for (sql, params) in [
            (
                "SELECT id FROM lix_file WHERE id = $1",
                vec![Value::Text(
                    "01920000-0000-7000-8000-0000000000a2".to_string(),
                )],
            ),
            (
                "SELECT content AS bytes FROM lix_file WHERE id = $1",
                vec![Value::Text(
                    "01920000-0000-7000-8000-0000000000a2".to_string(),
                )],
            ),
            (
                "SELECT content FROM lix_file AS file WHERE id = $1",
                vec![Value::Text(
                    "01920000-0000-7000-8000-0000000000a2".to_string(),
                )],
            ),
            (
                "SELECT content FROM lix_file WHERE id = '01920000-0000-7000-8000-0000000000a2'",
                vec![],
            ),
            (
                "SELECT content FROM lix_file WHERE id = $1 LIMIT 1",
                vec![Value::Text(
                    "01920000-0000-7000-8000-0000000000a2".to_string(),
                )],
            ),
            (
                "SELECT \"DATA\" FROM lix_file WHERE id = $1",
                vec![Value::Text(
                    "01920000-0000-7000-8000-0000000000a2".to_string(),
                )],
            ),
            (
                "SELECT content FROM \"LIX_FILE\" WHERE id = $1",
                vec![Value::Text(
                    "01920000-0000-7000-8000-0000000000a2".to_string(),
                )],
            ),
            (
                "SELECT content FROM lix_file WHERE id = $1 AND true",
                vec![Value::Text(
                    "01920000-0000-7000-8000-0000000000a2".to_string(),
                )],
            ),
            (
                "SELECT content FROM lix_file WHERE id = $1",
                vec![Value::Null],
            ),
            (
                "SELECT content FROM lix_file WHERE id = $1",
                vec![
                    Value::Text("01920000-0000-7000-8000-0000000000a2".to_string()),
                    Value::Text("extra".to_string()),
                ],
            ),
        ] {
            let statement = sql2::parse_statement(sql).unwrap();
            assert_eq!(
                exact_filesystem_read_route(&statement, &params),
                None,
                "unexpected fast-path match for {sql}"
            );
        }
    }

    #[test]
    fn exact_schema_batch_route_preserves_composite_identity_expansion() {
        let statement = sql2::parse_statement(
            "SELECT tenant, revision, payload FROM batch_route_row \
             WHERE tenant = $1 AND revision IN ($2, $3, $2) \
               AND lixcol_file_id IS NULL \
             ORDER BY tenant, revision",
        )
        .expect("batch route SQL should parse");
        let route = exact_schema_batch_read_route(
            &statement,
            &[
                Value::Text("docs".to_owned()),
                Value::Integer(7),
                Value::Integer(9),
            ],
        )
        .expect("complete composite identities should route");
        assert_eq!(route.identities.len(), 3, "request slots stay aligned");
        assert_eq!(route.order_by_columns, ["tenant", "revision"]);
        assert_eq!(route.identities[0]["tenant"], Value::Text("docs".to_owned()));
        assert_eq!(route.identities[0]["revision"], Value::Integer(7));
        assert_eq!(route.identities[0]["lixcol_file_id"], Value::Null);
        assert_eq!(route.identities[1]["revision"], Value::Integer(9));
        assert_eq!(route.identities[2]["revision"], Value::Integer(7));
    }

    #[tokio::test]
    async fn exact_schema_batch_matches_relational_duplicate_missing_null_and_jsonb_semantics() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "batch_route_row",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "note", "type": "text", "nullable": true },
                { "name": "payload", "type": "jsonb", "nullable": false }
            ],
            "primary_key": ["id"]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) \
                 VALUES ('batch_route_row', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("register batch-route schema");
        session
            .execute(
                "INSERT INTO batch_route_row (id, note, payload) VALUES \
                 ('a', NULL, CAST('{\"rank\":1}' AS JSONB)), \
                 ('b', 'present', CAST('{\"rank\":2}' AS JSONB))",
                &[],
            )
            .await
            .expect("seed batch-route rows");

        let params = [
            Value::Text("b".to_owned()),
            Value::Text("missing".to_owned()),
            Value::Text("a".to_owned()),
            Value::Text("b".to_owned()),
        ];
        let exact = session
            .execute(
                "SELECT id, note, payload FROM batch_route_row \
                 WHERE id IN ($1, $2, $3, $4) \
                   AND lixcol_file_id IS NULL ORDER BY id",
                &params,
            )
            .await
            .expect("native batch route should execute");
        let relational = session
            .execute(
                "SELECT row.id, row.note, row.payload FROM batch_route_row AS row \
                 WHERE row.id IN ($1, $2, $3, $4) \
                   AND row.lixcol_file_id IS NULL ORDER BY row.id",
                &params,
            )
            .await
            .expect("relational control should execute");
        assert_eq!(exact, relational);
        assert_eq!(exact.rows().len(), 2, "duplicates and misses emit no extra rows");
        assert_eq!(exact.rows()[0].value("note").expect("note column"), &Value::Null);
        assert_eq!(
            exact.rows()[1].value("payload").expect("payload column"),
            &Value::Json(serde_json::json!({"rank": 2}).into())
        );

        #[cfg(feature = "storage-benches")]
        {
            let (profiled, profile) = session
                .execute_profiled(
                    "SELECT id, note, payload FROM batch_route_row \
                     WHERE id IN ($1, $2, $3, $4) \
                       AND lixcol_file_id IS NULL ORDER BY id",
                    &params,
                )
                .await
                .expect("profiled native batch route should execute");
            assert_eq!(profiled.rows().len(), 2);
            assert_eq!(profile.scan_rows, 2, "only returned public rows are scanned");
            assert_eq!(
                profile.provider_rows_examined, 3,
                "present, missing, and duplicate slots examine three unique exact identities"
            );
        }
    }

    #[tokio::test]
    async fn exact_root_filesystem_listings_match_the_relational_path() {
        let session = open_session().await;
        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES \
                 ('01920000-0000-7000-8000-0000000000d1', '/nested'), \
                 ('01920000-0000-7000-8000-0000000000d2', '/alpha-dir')",
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO lix_file (id, path, content, lixcol_metadata) VALUES \
                 ('01920000-0000-7000-8000-0000000000f1', '/b.txt', $1, CAST('{\"rank\":2}' AS JSONB)), \
                 ('01920000-0000-7000-8000-0000000000f2', '/nested/a.txt', $2, NULL), \
                 ('01920000-0000-7000-8000-0000000000f3', '/a.txt', $3, NULL)",
                &[
                    Value::Blob(b"bravo".to_vec().into()),
                    Value::Blob(b"nested".to_vec().into()),
                    Value::Blob(b"alpha".to_vec().into()),
                ],
            )
            .await
            .unwrap();

        let exact = session
            .execute(
                "SELECT id, path, name, lixcol_metadata, lixcol_updated_at \
                 FROM lix_file WHERE directory_id IS NULL ORDER BY name",
                &[],
            )
            .await
            .unwrap();
        let relational = session
            .execute(
                "SELECT file.id AS id, file.path AS path, file.name AS name, \
                        file.lixcol_metadata AS lixcol_metadata, \
                        file.lixcol_updated_at AS lixcol_updated_at \
                 FROM lix_file AS file \
                 WHERE file.directory_id IS NULL ORDER BY file.name",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(exact, relational);
        assert_eq!(exact.rows().len(), 2);
        assert_eq!(
            exact.rows()[0].get::<String>("id").unwrap(),
            "01920000-0000-7000-8000-0000000000f3"
        );
        assert_eq!(
            exact.rows()[1].get::<String>("id").unwrap(),
            "01920000-0000-7000-8000-0000000000f1"
        );
        assert_eq!(
            exact.rows()[1].value("lixcol_metadata").unwrap(),
            &Value::Json(serde_json::json!({"rank": 2}).into())
        );

        let exact_directories = session
            .execute(
                "SELECT id, path, name, lixcol_updated_at \
                 FROM lix_directory WHERE parent_id IS NULL ORDER BY name",
                &[],
            )
            .await
            .unwrap();
        let relational_directories = session
            .execute(
                "SELECT directory.id AS id, directory.path AS path, \
                        directory.name AS name, \
                        directory.lixcol_updated_at AS lixcol_updated_at \
                 FROM lix_directory AS directory \
                 WHERE directory.parent_id IS NULL ORDER BY directory.name",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(exact_directories, relational_directories);
        assert_eq!(exact_directories.rows().len(), 2);
        assert_eq!(
            exact_directories.rows()[0].get::<String>("id").unwrap(),
            "01920000-0000-7000-8000-0000000000d2"
        );
        assert_eq!(
            exact_directories.rows()[1].get::<String>("id").unwrap(),
            "01920000-0000-7000-8000-0000000000d1"
        );
    }

    #[test]
    fn late_file_content_read_rewrites_only_unchanged_blob_projections() {
        let statement = sql2::parse_statement(
            "SELECT path, content FROM lix_file WHERE path LIKE $1 ORDER BY path LIMIT 2",
        )
        .unwrap();
        let plan = late_materialized_lix_file_content_read(&statement).unwrap();
        assert_eq!(plan.data_column_index, 1);
        assert_eq!(
            plan.statement.to_string(),
            "SELECT path, path AS content FROM lix_file WHERE path LIKE $1 ORDER BY path LIMIT 2"
        );

        let aliased = sql2::parse_statement(
            "SELECT file.content AS bytes, file.path AS label FROM lix_file AS file WHERE file.path LIKE $1 ORDER BY file.path",
        )
        .unwrap();
        let plan = late_materialized_lix_file_content_read(&aliased).unwrap();
        assert_eq!(plan.data_column_index, 0);
        assert_eq!(
            plan.statement.to_string(),
            "SELECT file.path AS bytes, file.path AS label FROM lix_file AS file WHERE file.path LIKE $1 ORDER BY file.path"
        );

        for sql in [
            "SELECT path, length(content) FROM lix_file",
            "SELECT content, upper(path) FROM lix_file",
            "SELECT content FROM lix_file WHERE content = $1",
            "SELECT content FROM lix_file ORDER BY content",
            "SELECT content AS bytes FROM lix_file ORDER BY bytes",
            "SELECT content FROM lix_file ORDER BY 1",
            "SELECT DISTINCT content FROM lix_file",
            "SELECT content, content FROM lix_file",
            "SELECT * FROM lix_file",
            "SELECT file.content FROM lix_file AS file JOIN lix_file AS other ON file.id = other.id",
        ] {
            let statement = sql2::parse_statement(sql).unwrap();
            assert_eq!(
                late_materialized_lix_file_content_read(&statement),
                None,
                "unexpected late materialization for {sql}"
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
            classify_execute_batch(&[batch_statement("SELECT uuidv7()")], &cache).unwrap(),
            ExecuteBatchExecution::Transaction(_)
        ));
    }

    #[test]
    fn execute_batch_reuses_one_parsed_statement_for_homogeneous_writes() {
        let cache = sql2::SqlPlanningCache::default();
        let statements = [
            batch_statement("UPDATE lix_file SET path = '/a' WHERE id = 'a'"),
            batch_statement("UPDATE lix_file SET path = '/a' WHERE id = 'a'"),
        ];
        let ExecuteBatchExecution::Transaction(TransactionBatchStatements::Shared { len, .. }) =
            classify_execute_batch(&statements, &cache).unwrap()
        else {
            panic!("homogeneous durable statements should share one parsed statement");
        };
        assert_eq!(len, statements.len());
    }

    #[test]
    fn execute_batch_auto_parameterizes_distinct_literal_update_shapes() {
        let cache = sql2::SqlPlanningCache::default();
        let statements = [
            batch_statement("UPDATE notes SET value = 'first' WHERE id = 'a'"),
            batch_statement("UPDATE notes SET value = 'second' WHERE id = 'b'"),
        ];
        let ExecuteBatchExecution::Transaction(
            TransactionBatchStatements::AutoParameterizedUpdate {
                sql,
                parameter_batch,
                ..
            },
        ) = classify_execute_batch(&statements, &cache).unwrap()
        else {
            panic!("literal UPDATE statements should share one parameterized shape");
        };
        assert_eq!(sql.as_ref(), "UPDATE notes SET value = $1 WHERE id = $2");
        let parameter_rows = (0..parameter_batch.num_rows())
            .map(|row_index| sql2::parameter_row(&parameter_batch, row_index).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            parameter_rows,
            [
                vec![
                    Value::Text("first".to_string()),
                    Value::Text("a".to_string())
                ],
                vec![
                    Value::Text("second".to_string()),
                    Value::Text("b".to_string())
                ],
            ]
        );
    }

    #[test]
    fn execute_batch_declines_a_late_literal_shape_mismatch() {
        let cache = sql2::SqlPlanningCache::default();
        let statements = [
            batch_statement("UPDATE notes SET value = 'first' WHERE id = 'a'"),
            batch_statement("UPDATE notes SET value = 'second' WHERE id = 'b'"),
            batch_statement("UPDATE notes SET other = 'third' WHERE id = 'c'"),
        ];
        let ExecuteBatchExecution::Transaction(TransactionBatchStatements::Distinct(parsed)) =
            classify_execute_batch(&statements, &cache).unwrap()
        else {
            panic!("a heterogeneous batch must retain sequential classification");
        };
        assert_eq!(parsed.len(), statements.len());
    }

    #[tokio::test]
    async fn execution_disposition_uses_the_parsed_bound_statement_route() {
        let session = open_session().await;

        assert_eq!(
            session.execution_disposition("SELECT 1").unwrap(),
            ExecutionDisposition::CancellableRead
        );
        assert_eq!(
            session
                .execution_disposition("SELECT uuidv7()")
                .unwrap(),
            ExecutionDisposition::Durable
        );
        assert_eq!(
            session
                .execution_disposition(
                    "INSERT INTO lix_file (path, content) VALUES ('/disposition.txt', 'content')",
                )
                .unwrap(),
            ExecutionDisposition::Durable
        );
        assert_eq!(
            session
                .execute_batch_disposition(&[
                    batch_statement("SELECT 1"),
                    batch_statement("SELECT * FROM lix_file"),
                ])
                .unwrap(),
            ExecutionDisposition::CancellableRead
        );
        assert_eq!(
            session
                .execute_batch_disposition(&[
                    batch_statement("SELECT 1"),
                    batch_statement("SELECT CURRENT_TIMESTAMP"),
                ])
                .unwrap(),
            ExecutionDisposition::Durable
        );
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
                    label: Some("first".to_string()),
                    sql: "SELECT $1 AS value".to_string(),
                    params: vec![Value::Integer(11)],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: "SELECT $1 AS value".to_string(),
                    params: vec![Value::Integer(22)],
                },
            ])
            .await
            .unwrap();

        assert_eq!(results[0].rows()[0].get::<i64>("value").unwrap(), 11);
        assert_eq!(results[1].rows()[0].get::<i64>("value").unwrap(), 22);
        assert_eq!(results[0].statement_index(), Some(0));
        assert_eq!(results[0].label(), Some("first"));
        assert_eq!(results[1].statement_index(), Some(1));
        assert_eq!(results[1].label(), None);
    }

    #[tokio::test]
    async fn execute_batch_metadata_preserves_returning_rows_and_duplicate_labels() {
        let session = open_session().await;
        let results = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: Some("write".to_string()),
                    sql: "INSERT INTO lix_key_value (key, value) VALUES ('batch-metadata', 'one') RETURNING key, value".to_string(),
                    params: Vec::new(),
                },
                ExecuteBatchStatement {
                    label: Some("write".to_string()),
                    sql: "UPDATE lix_key_value SET value = 'two' WHERE key = 'batch-metadata' RETURNING key, value".to_string(),
                    params: Vec::new(),
                },
            ])
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].statement_index(), Some(0));
        assert_eq!(results[1].statement_index(), Some(1));
        assert_eq!(results[0].label(), Some("write"));
        assert_eq!(results[1].label(), Some("write"));
        assert_eq!(results[0].columns(), ["key", "value"]);
        assert_eq!(results[0].rows_affected(), 1);
        assert_eq!(
            results[0].rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!("one")
        );
        assert_eq!(
            results[1].rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!("two")
        );
    }

    #[tokio::test]
    async fn public_insert_preserves_declared_integer_primary_key_type() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "integer_primary_key_insert_probe",
            "columns": [
                { "name": "id", "type": "int8", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();

        let inserted = session
            .execute(
                "INSERT INTO integer_primary_key_insert_probe (id, value) VALUES ($1, $2)",
                &[Value::Integer(42), Value::Text("answer".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(inserted.rows_affected(), 1);

        let result = session
            .execute(
                "SELECT id, value FROM integer_primary_key_insert_probe WHERE id = $1",
                &[Value::Integer(42)],
            )
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0].get::<i64>("id").unwrap(), 42);
        assert_eq!(result.rows()[0].get::<String>("value").unwrap(), "answer");
    }

    #[tokio::test]
    async fn execute_batch_lowers_distinct_bound_row_inserts_once() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "parameter_insert_batch_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();

        sql2::take_certified_row_insert_parameter_batch_executions();
        let sql = "INSERT INTO parameter_insert_batch_probe (id, value) VALUES ($1, $2)";
        let results = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("a".to_string()),
                        Value::Text("value-a".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("b".to_string()),
                        Value::Text("value-b".to_string()),
                    ],
                },
            ])
            .await
            .unwrap();

        assert_eq!(
            sql2::take_certified_row_insert_parameter_batch_executions(),
            1
        );
        assert_eq!(
            results
                .iter()
                .map(ExecuteResult::rows_affected)
                .collect::<Vec<_>>(),
            vec![1, 1]
        );
        let rows = session
            .execute(
                "SELECT id, value FROM parameter_insert_batch_probe ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.rows()[0].get::<String>("value").unwrap(), "value-a");
        assert_eq!(rows.rows()[1].get::<String>("value").unwrap(), "value-b");

        sql2::take_certified_row_insert_parameter_batch_executions();
        let error = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("c".to_string()),
                        Value::Text("value-c".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("b".to_string()),
                        Value::Text("duplicate-b".to_string()),
                    ],
                },
            ])
            .await
            .expect_err("the second INSERT conflicts with committed row b");
        assert_eq!(error.details.unwrap()["statementIndex"], 1);
        assert_eq!(
            sql2::take_certified_row_insert_parameter_batch_executions(),
            0
        );
        let rows = session
            .execute(
                "SELECT id FROM parameter_insert_batch_probe WHERE id = 'c'",
                &[],
            )
            .await
            .unwrap();
        assert!(
            rows.is_empty(),
            "the fresh prefix must roll back with the conflicting batch"
        );

        let error = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("b".to_string()),
                        Value::Text("duplicate-b".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![Value::Text("d".to_string()), Value::Text("x".to_string())],
                },
            ])
            .await
            .expect_err("the committed conflict should be reported first");
        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert_eq!(error.details.unwrap()["statementIndex"], 0);

        session
            .create_checkpoint()
            .await
            .expect("packed insert base should checkpoint through its commit reference");
        assert_eq!(
            session
                .execute(
                    "UPDATE parameter_insert_batch_probe SET value = 'updated-b' WHERE id = 'b'",
                    &[],
                )
                .await
                .unwrap()
                .rows_affected(),
            1
        );
        assert_eq!(
            session
                .execute(
                    "DELETE FROM parameter_insert_batch_probe WHERE id = 'a'",
                    &[],
                )
                .await
                .unwrap()
                .rows_affected(),
            1
        );
        session
            .create_checkpoint()
            .await
            .expect("sparse packed-base overlays should checkpoint");
        let rows = session
            .execute(
                "SELECT id, value FROM parameter_insert_batch_probe ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.rows()[0].get::<String>("id").unwrap(), "b");
        assert_eq!(rows.rows()[0].get::<String>("value").unwrap(), "updated-b");
    }

    #[tokio::test]
    async fn large_ordered_parameter_insert_reuses_commit_delta_as_current_base() {
        const ROW_COUNT: usize = 1_024;
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "ordered_packed_insert_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();

        crate::transaction::take_ordered_packed_current_base_publications();
        let sql = "INSERT INTO ordered_packed_insert_probe (id, value) VALUES ($1, $2)";
        let statements = (0..ROW_COUNT)
            .map(|row_index| ExecuteBatchStatement {
                label: None,
                sql: sql.to_string(),
                params: vec![
                    Value::Text(format!("{row_index:04}")),
                    Value::Text(format!("value-{row_index:04}")),
                ],
            })
            .collect::<Vec<_>>();
        let affected = session
            .execute_batch(&statements)
            .await
            .unwrap()
            .iter()
            .map(ExecuteResult::rows_affected)
            .sum::<u64>();
        assert_eq!(affected, ROW_COUNT as u64);
        assert_eq!(
            crate::transaction::take_ordered_packed_current_base_publications(),
            1,
            "the certified ordered batch must publish its commit delta directly as current state"
        );
        let rows = session
            .execute(
                "SELECT id, value FROM ordered_packed_insert_probe WHERE id IN ('0000', '1023') ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows.rows()[0].get::<String>("value").unwrap(), "value-0000");
        assert_eq!(rows.rows()[1].get::<String>("value").unwrap(), "value-1023");
        session
            .create_checkpoint()
            .await
            .expect("ordered packed current base should remain checkpointable");
    }

    #[tokio::test]
    async fn large_certified_insert_publishes_rootless_history_and_reads_packed_head() {
        const ROW_COUNT: usize = 32 * 1_024;
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "rootless_ordered_insert_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("rootless ordered schema should register");
        let branch_id = session
            .active_branch_id()
            .await
            .expect("active branch should resolve");
        let baseline_read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("baseline read should open");
        let baseline = session
            .branch_ctx
            .ref_reader(baseline_read)
            .load_head(&branch_id)
            .await
            .expect("baseline head should load")
            .expect("schema registration should publish a head");

        let sql = "INSERT INTO rootless_ordered_insert_probe (id, value) VALUES ($1, $2)";
        let inserts = (0..ROW_COUNT)
            .map(|index| ExecuteBatchStatement {
                label: None,
                sql: sql.to_owned(),
                params: vec![
                    Value::Text(format!("{index:05}")),
                    Value::Text(format!("value-{index:05}")),
                ],
            })
            .collect::<Vec<_>>();
        let inserted = session
            .execute_batch(&inserts)
            .await
            .expect("large certified parameter batch should insert")
            .iter()
            .map(ExecuteResult::rows_affected)
            .sum::<u64>();
        assert_eq!(inserted, ROW_COUNT as u64);

        let rows = session
            .execute(
                "SELECT id, value FROM rootless_ordered_insert_probe ORDER BY id",
                &[],
            )
            .await
            .expect("packed current head should serve the rootless commit");
        assert_eq!(rows.len(), ROW_COUNT);
        assert_eq!(rows.rows()[0].get::<String>("id").unwrap(), "00000");
        assert_eq!(
            rows.rows()[ROW_COUNT - 1].get::<String>("id").unwrap(),
            "32767"
        );

        let head_read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("head read should open");
        let head = session
            .branch_ctx
            .ref_reader(head_read)
            .load_head(&branch_id)
            .await
            .expect("branch head should load")
            .expect("large insert should publish a head");
        let history_read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("history read should open");
        let commit_state =
            crate::tracked_state::load_commit_state_manifest(&history_read, head.commit_id)
                .await
                .expect("head physical state should load")
                .expect("head physical state should exist");
        assert!(commit_state.replay_debt.depth >= 1);
        assert!(commit_state.replay_debt.rows >= ROW_COUNT as u64);
        assert!(commit_state.replay_debt.bytes > commit_state.replay_debt.rows);
        assert!(
            crate::tracked_state::load_snapshot_commit_root(
                &history_read,
                &head.commit_id.to_string(),
            )
            .await
            .expect("root lookup should succeed")
            .is_none(),
            "rootless production commit must skip the duplicate immutable tree"
        );

        let diff = session
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) \
                 WHERE schema_key = 'rootless_ordered_insert_probe' AND diff_type = 'added'",
                &[
                    Value::Text(baseline.commit_id.to_string()),
                    Value::Text(head.commit_id.to_string()),
                ],
            )
            .await
            .expect("rootless insert diff should replay from the ordered delta");
        assert_eq!(
            diff.rows()[0].get::<i64>("entries").unwrap(),
            ROW_COUNT as i64
        );
        let history = session
            .execute(
                &format!(
                    "SELECT COUNT(*) AS entries \
                     FROM rootless_ordered_insert_probe_history('{}') \
                     WHERE lixcol_is_deleted = false",
                    head.commit_id
                ),
                &[],
            )
            .await
            .expect("rootless insert history should replay from the ordered delta");
        assert_eq!(
            history.rows()[0].get::<i64>("entries").unwrap(),
            ROW_COUNT as i64
        );

        session
            .execute(
                "UPDATE rootless_ordered_insert_probe SET value = 'updated' WHERE id = '00000'",
                &[],
            )
            .await
            .expect("a sparse descendant of a rootless commit should remain writable");
        let descendant_read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("descendant read should open");
        let descendant = session
            .branch_ctx
            .ref_reader(descendant_read)
            .load_head(&branch_id)
            .await
            .expect("descendant head should load")
            .expect("sparse update should publish a head");
        let descendant_history_read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("descendant history read should open");
        let descendant_state = crate::tracked_state::load_commit_state_manifest(
            &descendant_history_read,
            descendant.commit_id,
        )
        .await
        .expect("descendant physical state should load")
        .expect("descendant physical state should exist");
        assert!(
            descendant_state.replay_debt.depth > 0,
            "a sparse descendant must extend the rootless first-parent interval"
        );
        assert_eq!(
            descendant_state.replay_debt.depth,
            commit_state.replay_debt.depth + 1
        );
        assert_eq!(
            descendant_state.replay_debt.rows,
            commit_state.replay_debt.rows + 1
        );
        assert!(descendant_state.replay_debt.bytes > commit_state.replay_debt.bytes);
        assert!(
            crate::tracked_state::load_snapshot_commit_root(
                &descendant_history_read,
                &descendant.commit_id.to_string(),
            )
            .await
            .expect("descendant root lookup should succeed")
            .is_none()
        );
        let updated = session
            .execute(
                "SELECT value FROM rootless_ordered_insert_probe WHERE id = '00000'",
                &[],
            )
            .await
            .expect("rootless descendant should remain readable");
        assert_eq!(updated.rows()[0].get::<String>("value").unwrap(), "updated");
        let update_diff = session
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) \
                 WHERE schema_key = 'rootless_ordered_insert_probe' AND diff_type = 'modified'",
                &[
                    Value::Text(head.commit_id.to_string()),
                    Value::Text(descendant.commit_id.to_string()),
                ],
            )
            .await
            .expect("rootless descendant diff should remain queryable");
        assert_eq!(update_diff.rows()[0].get::<i64>("entries").unwrap(), 1);

        let draft = session
            .create_branch(crate::CreateBranchOptions {
                id: Some("01930000-0000-7000-8000-00000000b001".to_owned()),
                name: "rootless-ordered-draft".to_owned(),
                from_commit_id: Some(descendant.commit_id.to_string()),
            })
            .await
            .expect("a branch should start from a rootless history commit");
        session
            .switch_branch(crate::SwitchBranchOptions {
                branch_id: draft.id.clone(),
            })
            .await
            .expect("rootless draft should open");
        session
            .execute(
                "UPDATE rootless_ordered_insert_probe SET value = 'draft' WHERE id = '00001'",
                &[],
            )
            .await
            .expect("rootless draft should remain writable");
        session
            .switch_branch(crate::SwitchBranchOptions {
                branch_id: branch_id.clone(),
            })
            .await
            .expect("repository should switch back to the rootless main branch");
        let main_session = &session;
        session
            .execute(
                "UPDATE rootless_ordered_insert_probe SET value = 'main' WHERE id = '32767'",
                &[],
            )
            .await
            .expect("rootless main branch should remain writable");
        let merge = session
            .merge_branch(crate::MergeBranchOptions {
                source_branch_id: draft.id,
            })
            .await
            .expect("disjoint changes descending from a rootless base should merge");
        assert_eq!(merge.outcome, crate::MergeBranchOutcome::MergeCommitted);
        let merged = main_session
            .execute(
                "SELECT id, value FROM rootless_ordered_insert_probe \
                 WHERE id IN ('00001', '32767') ORDER BY id",
                &[],
            )
            .await
            .expect("merged rootless head should remain readable");
        assert_eq!(merged.rows()[0].get::<String>("value").unwrap(), "draft");
        assert_eq!(merged.rows()[1].get::<String>("value").unwrap(), "main");

        let deleted = main_session
            .execute("DELETE FROM rootless_ordered_insert_probe", &[])
            .await
            .expect("the merged fixture should delete through the file cascade");
        assert_eq!(deleted.rows_affected(), ROW_COUNT as u64);
        let reinserts = (0..ROW_COUNT)
            .map(|index| ExecuteBatchStatement {
                label: None,
                sql: sql.to_owned(),
                params: vec![
                    Value::Text(format!("{index:05}")),
                    Value::Text("second-seed".to_owned()),
                ],
            })
            .collect::<Vec<_>>();
        let reseeded = main_session
            .execute_batch(&reinserts)
            .await
            .expect("a second large ordered insert should start a bounded interval")
            .iter()
            .map(ExecuteResult::rows_affected)
            .sum::<u64>();
        assert_eq!(reseeded, ROW_COUNT as u64);
        let reseed_read = main_session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("reseed read should open");
        let reseed_head = main_session
            .branch_ctx
            .ref_reader(&reseed_read)
            .load_head(&branch_id)
            .await
            .expect("reseed head should load")
            .expect("reseed head should exist");
        let reseed_state =
            crate::tracked_state::load_commit_state_manifest(&reseed_read, reseed_head.commit_id)
                .await
                .expect("reseed physical state should load")
                .expect("reseed physical state should exist");
        assert!(reseed_state.replay_debt.depth >= 1);

        let mut rooted_fence = None;
        for generation_offset in 1..=32 {
            main_session
                .execute(
                    "UPDATE rootless_ordered_insert_probe SET value = $1 WHERE id = '00002'",
                    &[Value::Text(format!("fence-{generation_offset}"))],
                )
                .await
                .expect("a bounded rootless descendant should commit");
            let read = main_session
                .storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("root-fence read should open");
            let head = main_session
                .branch_ctx
                .ref_reader(&read)
                .load_head(&branch_id)
                .await
                .expect("root-fence head should load")
                .expect("root-fence head should exist");
            let state = crate::tracked_state::load_commit_state_manifest(&read, head.commit_id)
                .await
                .expect("root-fence physical state should load")
                .expect("root-fence physical state should exist");
            if state.replay_debt.depth == 0 {
                assert_eq!(state.replay_debt.rows, 0);
                assert_eq!(state.replay_debt.bytes, 0);
                assert!(
                    crate::tracked_state::load_snapshot_commit_root(
                        &read,
                        &head.commit_id.to_string(),
                    )
                    .await
                    .expect("root-fence lookup should succeed")
                    .is_some(),
                    "a rooted fence must publish its immutable accelerator"
                );
                rooted_fence = Some(head.commit_id);
                break;
            }
        }
        assert!(
            rooted_fence.is_some(),
            "rootless replay intervals must close within one generation fence"
        );
        let rooted_fence = rooted_fence.unwrap();
        let rebuilt_rows = main_session
            .execute(
                "SELECT id, value FROM rootless_ordered_insert_probe \
                 WHERE id IN ('00000', '00001', '00002', '32767') ORDER BY id",
                &[],
            )
            .await
            .expect("rebuilt root fence should serve representative rows");
        assert_eq!(
            rebuilt_rows.rows()[0].get::<String>("value").unwrap(),
            "second-seed"
        );
        assert_eq!(
            rebuilt_rows.rows()[1].get::<String>("value").unwrap(),
            "second-seed"
        );
        assert!(
            rebuilt_rows.rows()[2]
                .get::<String>("value")
                .unwrap()
                .starts_with("fence-")
        );
        assert_eq!(
            rebuilt_rows.rows()[3].get::<String>("value").unwrap(),
            "second-seed"
        );
        let fence_diff = main_session
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) \
                 WHERE schema_key = 'rootless_ordered_insert_probe' AND diff_type = 'modified'",
                &[
                    Value::Text(reseed_head.commit_id.to_string()),
                    Value::Text(rooted_fence.to_string()),
                ],
            )
            .await
            .expect("diff should cross the rebuilt root fence");
        assert_eq!(fence_diff.rows()[0].get::<i64>("entries").unwrap(), 1);
        let fence_history = main_session
            .execute(
                &format!(
                    "SELECT COUNT(DISTINCT id) AS entries \
                     FROM rootless_ordered_insert_probe_history('{rooted_fence}') \
                     WHERE id IN ('00000', '00001', '00002', '32767') \
                       AND lixcol_is_deleted = false"
                ),
                &[],
            )
            .await
            .expect("history should cross the rebuilt root fence");
        assert_eq!(fence_history.rows()[0].get::<i64>("entries").unwrap(), 4);
        let merge_history = main_session
            .execute(
                &format!(
                    "SELECT COUNT(*) AS entries \
                     FROM rootless_ordered_insert_probe_history('{rooted_fence}') \
                     WHERE (id = '00001' AND value = 'draft') \
                        OR (id = '32767' AND value = 'main')"
                ),
                &[],
            )
            .await
            .expect("merge-selected revisions should survive both root fences");
        assert_eq!(merge_history.rows()[0].get::<i64>("entries").unwrap(), 2);
    }

    #[tokio::test]
    async fn successive_columnar_inserts_preserve_the_existing_schema_base() {
        const BATCH_ROWS: usize = 1_024;
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "successive_columnar_insert_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("successive insert schema should register");

        let sql = "INSERT INTO successive_columnar_insert_probe (id, value) VALUES ($1, $2)";
        for generation in 0..2 {
            let first = generation * BATCH_ROWS;
            let inserts = (first..first + BATCH_ROWS)
                .map(|row_index| ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_owned(),
                    params: vec![
                        Value::Text(format!("{row_index:04}")),
                        Value::Text(format!("generation-{generation}")),
                    ],
                })
                .collect::<Vec<_>>();
            let inserted = session
                .execute_batch(&inserts)
                .await
                .expect("each ordered insert generation should commit")
                .iter()
                .map(ExecuteResult::rows_affected)
                .sum::<u64>();
            assert_eq!(inserted, BATCH_ROWS as u64);
        }

        let rows = session
            .execute(
                "SELECT id, value FROM successive_columnar_insert_probe ORDER BY id",
                &[],
            )
            .await
            .expect("the second generation must retain the first columnar base");
        assert_eq!(rows.len(), BATCH_ROWS * 2);
        assert_eq!(rows.rows()[0].get::<String>("id").unwrap(), "0000");
        assert_eq!(
            rows.rows()[0].get::<String>("value").unwrap(),
            "generation-0"
        );
        assert_eq!(rows.rows()[BATCH_ROWS].get::<String>("id").unwrap(), "1024");
        assert_eq!(
            rows.rows()[BATCH_ROWS].get::<String>("value").unwrap(),
            "generation-1"
        );
    }

    #[tokio::test]
    async fn typed_columnar_base_preserves_current_diff_and_history_across_lifecycle_changes() {
        const ROW_COUNT: usize = 65_537;
        let storage = Memory::default();
        Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("initialized storage should create engine");
        let main = engine
            .open_session_with_account(crate::SYSTEM_ACCOUNT_ID)
            .await
            .expect("session should open");
        let main_branch_id = main
            .active_branch_id()
            .await
            .expect("repository branch should resolve");
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "columnar_lifecycle_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        main.execute(
            "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("typed lifecycle schema should register");
        let before_insert = engine
            .load_branch_head_commit_id(&main_branch_id)
            .await
            .expect("pre-insert head should load")
            .expect("pre-insert head should exist");

        crate::transaction::take_ordered_packed_current_base_publications();
        crate::transaction::take_certified_columnar_current_base_publications();
        let insert_sql = "INSERT INTO columnar_lifecycle_probe (id, value) VALUES ($1, $2)";
        let inserts = (0..ROW_COUNT)
            .map(|row_index| ExecuteBatchStatement {
                label: None,
                sql: insert_sql.to_owned(),
                params: vec![
                    Value::Text(format!("{row_index:05}")),
                    Value::Text(format!("base-{row_index:04}")),
                ],
            })
            .collect::<Vec<_>>();
        let inserted = main
            .execute_batch(&inserts)
            .await
            .expect("ordered typed batch should insert")
            .iter()
            .map(ExecuteResult::rows_affected)
            .sum::<u64>();
        assert_eq!(inserted, ROW_COUNT as u64);
        assert_eq!(
            crate::transaction::take_ordered_packed_current_base_publications(),
            1,
            "fixture must activate packed current-state publication"
        );
        assert_eq!(
            crate::transaction::take_certified_columnar_current_base_publications(),
            1,
            "lossless columnar INSERT must bypass the row-wise current-base publisher"
        );
        let inserted_head = engine
            .load_branch_head_commit_id(&main_branch_id)
            .await
            .expect("insert head should load")
            .expect("insert head should exist");
        let adapter = engine.storage();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("sidecar read scope should open");
        let row_group_id = crate::hot_state::row_group_set_id(
            crate::changelog::CommitId::parse_lix(&inserted_head, "typed lifecycle insert head")
                .expect("insert head should be canonical"),
            "columnar_lifecycle_probe",
        );
        let manifest = crate::columnar_row_group::load_row_group_manifest(&read, row_group_id)
            .await
            .expect("typed lifecycle sidecar manifest should load")
            .expect("fixture must publish a typed columnar sidecar");
        assert_eq!(manifest.row_count(), ROW_COUNT as u64);
        assert_eq!(
            manifest
                .groups
                .iter()
                .map(|group| group.row_count)
                .collect::<Vec<_>>(),
            vec![65_536, 1],
            "fixture must cross both column-page and logical-group boundaries"
        );
        let commit_state = crate::tracked_state::load_commit_state_manifest(
            &read,
            crate::changelog::CommitId::parse_lix(
                &inserted_head,
                "typed lifecycle mutation authority",
            )
            .expect("insert head should be canonical"),
        )
        .await
        .expect("typed mutation authority should load")
        .expect("typed mutation authority should exist");
        let columnar_parts = commit_state
            .mutations
            .columnar_parts
            .as_ref()
            .expect("fixture must publish column pages as sole mutation authority");
        let semantic_commit_ids = [commit_state.commit_id];
        let commit_records = ChangelogContext::new()
            .reader(&read)
            .load_commits(CommitLoadRequest {
                commit_ids: &semantic_commit_ids,
            })
            .await
            .expect("typed lifecycle semantic owner should load");
        let semantic_owner = commit_records
            .into_iter()
            .next()
            .and_then(|(_, record)| record)
            .expect("typed lifecycle semantic owner should exist");
        assert_eq!(semantic_owner.account_id, crate::SYSTEM_ACCOUNT_ID);
        assert!(commit_state.mutations.inline_part.is_empty());
        assert!(commit_state.mutations.parts.is_empty());
        assert_eq!(columnar_parts.page_first_keys.len(), 33);
        let serving_root = commit_state
            .current_state_scoped_ranges
            .as_deref()
            .expect("new collection insert must publish canonical column pages");
        let serving_scope =
            crate::tracked_state::current_state_envelope::current_state_scope_prefix(
                &crate::tracked_state::CommitDeltaReplacementScope {
                    schema_key: "columnar_lifecycle_probe".to_owned(),
                    file_id: None,
                },
            )
            .expect("test scope should encode");
        let serving = crate::tracked_state::scoped_range::scan_scoped_range_scope(
            &read,
            &serving_root.tree,
            &serving_scope,
        )
        .await
        .expect("columnar serving scope should scan");
        assert_eq!(serving.parts.len(), 33);
        assert_eq!(
            serving
                .parts
                .iter()
                .map(|part| {
                    matches!(
                        crate::tracked_state::current_state_descriptor_from_scoped_range_part(part)
                            .expect("columnar locator should decode")
                            .source,
                        crate::tracked_state::CurrentStatePartSource::ColumnarPage(_)
                    )
                })
                .collect::<Vec<_>>(),
            vec![true; 33]
        );
        let owner =
            crate::changelog::CommitId::parse_lix(&inserted_head, "typed lifecycle serving owner")
                .expect("insert head should be canonical");
        let point_ids = ["02047", "02048", "65535", "65536", "70000"];
        let point_keys = point_ids
            .iter()
            .map(|identity| {
                let row_pk = RowPk::from_json_array_text(&format!("[\"{identity}\"]"))
                    .expect("test identity should parse");
                bytes::Bytes::from(crate::tracked_state::encode_key_ref(
                    crate::tracked_state::TrackedStateKeyRef {
                        schema_key: "columnar_lifecycle_probe",
                        file_id: None,
                        row_pk: &row_pk,
                    },
                ))
            })
            .collect::<Vec<_>>();
        let point_values =
            crate::tracked_state::load_complete_current_state_values_from_scoped_root(
                &read,
                serving_root,
                &point_keys,
            )
            .await
            .expect("page-backed points should load")
            .expect("the new collection scope should be covered");
        for (index, ordinal) in [2047_u32, 2048, 65_535, 65_536].into_iter().enumerate() {
            assert_eq!(
                point_values[index]
                    .as_ref()
                    .expect("inserted page identity should resolve")
                    .change_id,
                crate::tracked_state::change_id_from_packed_address(owner, ordinal + 1,)
            );
        }
        assert!(point_values[4].is_none());
        drop(read);

        assert_columnar_lifecycle_current(&main, ROW_COUNT, "base-0000", "base-1023").await;

        let inserted_diff = main
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) \
                 WHERE schema_key = 'columnar_lifecycle_probe' AND diff_type = 'added'",
                &[
                    Value::Text(before_insert.to_string()),
                    Value::Text(inserted_head.to_string()),
                ],
            )
            .await
            .expect("packed insert diff should remain queryable");
        assert_eq!(
            inserted_diff.rows()[0].get::<i64>("entries").unwrap(),
            ROW_COUNT as i64
        );
        let inserted_history = main
            .execute(
                &format!(
                    "SELECT COUNT(*) AS entries \
                     FROM columnar_lifecycle_probe_history('{inserted_head}') \
                     WHERE lixcol_is_deleted = false"
                ),
                &[],
            )
            .await
            .expect("packed insert history should remain queryable");
        assert_eq!(
            inserted_history.rows()[0].get::<i64>("entries").unwrap(),
            ROW_COUNT as i64
        );
        let attributed_changes = main
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_change \
                 WHERE schema_key = 'columnar_lifecycle_probe' AND account_id = $1",
                &[Value::Text(crate::SYSTEM_ACCOUNT_ID.to_owned())],
            )
            .await
            .expect("columnar mutation history must retain commit account attribution");
        assert_eq!(
            attributed_changes.rows()[0].get::<i64>("entries").unwrap(),
            ROW_COUNT as i64
        );
        let boundary_history = main
            .execute(
                &format!(
                    "SELECT COUNT(DISTINCT id) AS entries \
                     FROM columnar_lifecycle_probe_history('{inserted_head}') \
                     WHERE id IN ('02047', '02048', '65535', '65536') \
                       AND lixcol_is_deleted = false"
                ),
                &[],
            )
            .await
            .expect("history must address rows on both sides of page and group boundaries");
        assert_eq!(boundary_history.rows()[0].get::<i64>("entries").unwrap(), 4);

        main.execute(
            "UPDATE columnar_lifecycle_probe SET value = 'sparse-0512' WHERE id = '00512'",
            &[],
        )
        .await
        .expect("sparse typed update should commit");
        assert_columnar_layout_selected(&main, "columnar_lifecycle_probe", 1).await;

        let limited = main
            .execute(
                "SELECT id, value FROM columnar_lifecycle_probe ORDER BY id LIMIT 3",
                &[],
            )
            .await
            .expect("DataFusion LIMIT should remain above the columnar scan");
        assert_eq!(limited.len(), 3);
        assert_eq!(limited.rows()[0].get::<String>("id").unwrap(), "00000");
        assert_eq!(limited.rows()[2].get::<String>("id").unwrap(), "00002");
        let zero = main
            .execute(
                "SELECT id FROM columnar_lifecycle_probe ORDER BY id LIMIT 0",
                &[],
            )
            .await
            .expect("zero LIMIT should retain DataFusion semantics");
        assert!(zero.is_empty());

        // The immutable groups can all be pruned for this value, but the HOT
        // winner must still be reconciled and filtered before LIMIT executes.
        let overlay_match = main
            .execute(
                "SELECT id FROM columnar_lifecycle_probe \
                 WHERE value = 'sparse-0512' LIMIT 1",
                &[],
            )
            .await
            .expect("pruned columnar scan should retain matching overlay winner");
        assert_eq!(overlay_match.len(), 1);
        assert_eq!(
            overlay_match.rows()[0].get::<String>("id").unwrap(),
            "00512"
        );
        let no_match = main
            .execute(
                "SELECT id FROM columnar_lifecycle_probe \
                 WHERE value = 'not-present' LIMIT 1",
                &[],
            )
            .await
            .expect("fully pruned columnar scan should return an exact empty result");
        assert!(no_match.is_empty());

        main.execute(
            "UPDATE columnar_lifecycle_probe SET value = 'base-0512' WHERE id = '00512'",
            &[],
        )
        .await
        .expect("sparse typed restoration should commit");

        let checkpoint = main
            .create_checkpoint()
            .await
            .expect("packed typed base should checkpoint");
        let draft = main
            .create_branch(crate::CreateBranchOptions {
                id: Some("01930000-0000-7000-8000-0000000000c1".to_owned()),
                name: "columnar-lifecycle-draft".to_owned(),
                from_commit_id: Some(checkpoint.commit_id.clone()),
            })
            .await
            .expect("checkpoint branch should create");
        let draft_session = engine
            .open_session_at(draft.id.clone())
            .await
            .expect("draft session should open");
        draft_session
            .execute(
                "UPDATE columnar_lifecycle_probe SET value = 'draft-0000' WHERE id = '00000'",
                &[],
            )
            .await
            .expect("draft update should commit");
        draft_session
            .undo()
            .await
            .expect("draft update should undo");
        assert_columnar_lifecycle_current(&draft_session, ROW_COUNT, "base-0000", "base-1023")
            .await;
        draft_session
            .redo()
            .await
            .expect("draft update should redo");

        main.execute(
            "UPDATE columnar_lifecycle_probe SET value = 'main-1023' WHERE id = '01023'",
            &[],
        )
        .await
        .expect("main update should commit");
        let merge = main
            .merge_branch(crate::MergeBranchOptions {
                source_branch_id: draft.id,
            })
            .await
            .expect("disjoint typed updates should merge");
        assert_eq!(merge.outcome, crate::MergeBranchOutcome::MergeCommitted);
        assert_columnar_lifecycle_current(&main, ROW_COUNT, "draft-0000", "main-1023").await;

        let merged_head = engine
            .load_branch_head_commit_id(&main_branch_id)
            .await
            .expect("merged head should load")
            .expect("merged head should exist");
        let merged_diff = main
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) \
                 WHERE schema_key = 'columnar_lifecycle_probe' AND diff_type = 'modified'",
                &[
                    Value::Text(checkpoint.commit_id.to_string()),
                    Value::Text(merged_head.to_string()),
                ],
            )
            .await
            .expect("merged lifecycle diff should remain queryable");
        assert_eq!(merged_diff.rows()[0].get::<i64>("entries").unwrap(), 2);
        let merged_history = main
            .execute(
                &format!(
                    "SELECT value, lixcol_depth \
                     FROM columnar_lifecycle_probe_history('{merged_head}') \
                     WHERE id = '00000' ORDER BY lixcol_depth"
                ),
                &[],
            )
            .await
            .expect("merged typed history should remain queryable");
        assert_eq!(
            merged_history.rows()[0].get::<String>("value").unwrap(),
            "draft-0000"
        );
        assert!(
            merged_history
                .rows()
                .iter()
                .any(|row| row.get::<String>("value").ok().as_deref() == Some("base-0000"))
        );

        main.execute(
            "UPDATE columnar_lifecycle_probe SET value = 'temporary' WHERE id = '00512'",
            &[],
        )
        .await
        .expect("post-merge update should commit");
        main.undo().await.expect("post-merge update should undo");
        let restored = main
            .execute(
                "SELECT value FROM columnar_lifecycle_probe WHERE id = '00512'",
                &[],
            )
            .await
            .expect("undone row should remain queryable");
        assert_eq!(
            restored.rows()[0].get::<String>("value").unwrap(),
            "base-0512"
        );
        assert_columnar_lifecycle_current(&main, ROW_COUNT, "draft-0000", "main-1023").await;

        let mut corrupt = engine.storage().new_write_set();
        corrupt.delete(
            crate::columnar_row_group::ROW_GROUP_MANIFEST_SPACE,
            crate::storage_adapter::StorageKey(bytes::Bytes::copy_from_slice(
                &row_group_id.as_bytes(),
            )),
        );
        engine
            .storage()
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .expect("test corruption should commit");
        let reopened = Engine::new(storage)
            .await
            .expect("corrupt storage should still open structurally");
        let reopened_session = reopened
            .open_session()
            .await
            .expect("corrupt storage session should open");
        assert!(
            reopened_session
                .execute(
                    &format!(
                        "SELECT value FROM columnar_lifecycle_probe_history('{inserted_head}') \
                         WHERE id = '65536'"
                    ),
                    &[],
                )
                .await
                .is_err(),
            "missing authoritative columnar manifests must fail closed"
        );
    }

    #[tokio::test]
    async fn large_ordered_parameter_update_replaces_complete_packed_current_base() {
        const ROW_COUNT: usize = 2_048;
        const PARTIAL_ROW_COUNT: usize = ROW_COUNT / 2;
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "ordered_packed_update_probe",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();

        let insert_sql =
            "INSERT INTO ordered_packed_update_probe (path, value) VALUES ($1, CAST($2 AS JSONB))";
        let insert_statements = (0..ROW_COUNT)
            .map(|row_index| ExecuteBatchStatement {
                label: None,
                sql: insert_sql.to_string(),
                params: vec![
                    Value::Text(format!("{row_index:04}")),
                    Value::Text(format!("\"value-{row_index:04}\"")),
                ],
            })
            .collect::<Vec<_>>();
        session.execute_batch(&insert_statements).await.unwrap();

        crate::transaction::take_complete_replacement_packed_current_base_retirements();
        let update_sql =
            "UPDATE ordered_packed_update_probe SET value = CAST($1 AS JSONB) WHERE path = $2";
        for version in 1..=2 {
            let update_statements = (0..ROW_COUNT)
                .map(|row_index| ExecuteBatchStatement {
                    label: None,
                    sql: update_sql.to_string(),
                    params: vec![
                        Value::Text(format!("\"updated-{version}-{row_index:04}\"")),
                        Value::Text(format!("{row_index:04}")),
                    ],
                })
                .collect::<Vec<_>>();
            let affected = session
                .execute_batch(&update_statements)
                .await
                .unwrap()
                .iter()
                .map(ExecuteResult::rows_affected)
                .sum::<u64>();
            assert_eq!(affected, ROW_COUNT as u64);
            assert_eq!(
                crate::transaction::take_complete_replacement_packed_current_base_retirements(),
                1,
                "each complete certified replacement should swap one packed base reference"
            );
            assert_current_head_uses_packed_delta_without_columnar_sidecar(
                &session,
                "ordered_packed_update_probe",
                ROW_COUNT as u64,
            )
            .await;
        }

        let rows = session
            .execute(
                "SELECT path, value FROM ordered_packed_update_probe WHERE path IN ('0000', '2047') ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.rows()[0].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!("updated-2-0000")
        );
        assert_eq!(
            rows.rows()[1].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!("updated-2-2047")
        );
        let working_diff = session
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_working_diff WHERE schema_key = 'ordered_packed_update_probe' AND diff_type = 'added'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            working_diff.rows()[0].get::<i64>("entries").unwrap(),
            ROW_COUNT as i64,
            "replacing a packed base must preserve its working-diff epoch"
        );

        let partial_update_statements = (0..PARTIAL_ROW_COUNT)
            .map(|row_index| ExecuteBatchStatement {
                label: None,
                sql: update_sql.to_string(),
                params: vec![
                    Value::Text(format!("\"partial-{row_index:04}\"")),
                    Value::Text(format!("{row_index:04}")),
                ],
            })
            .collect::<Vec<_>>();
        let partial_affected = session
            .execute_batch(&partial_update_statements)
            .await
            .unwrap()
            .iter()
            .map(ExecuteResult::rows_affected)
            .sum::<u64>();
        assert_eq!(partial_affected, PARTIAL_ROW_COUNT as u64);
        assert_eq!(
            crate::transaction::take_complete_replacement_packed_current_base_retirements(),
            0,
            "a partial replacement must remain a point-addressable HOT overlay"
        );
        let partial = session
            .execute(
                "SELECT value FROM ordered_packed_update_probe WHERE path = '0000'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            partial.rows()[0].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!("partial-0000")
        );
        session
            .create_checkpoint()
            .await
            .expect("replaced packed current base should remain checkpointable");
        let working_diff = session
            .execute("SELECT COUNT(*) AS entries FROM lix_working_diff", &[])
            .await
            .unwrap();
        assert_eq!(
            working_diff.rows()[0].get::<i64>("entries").unwrap(),
            0,
            "checkpointing a replaced packed base must clear its working diff"
        );
    }

    #[tokio::test]
    async fn ordered_batch_update_preserves_non_uniform_lifecycle_without_journal_admission() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "non_uniform_journal_admission_probe",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        for path in ["a", "b"] {
            session
                .execute(
                    "INSERT INTO non_uniform_journal_admission_probe (path, value) VALUES ($1, $2)",
                    &[
                        Value::Text(path.to_string()),
                        Value::Text("base".to_string()),
                    ],
                )
                .await
                .unwrap();
        }
        let before = session
            .execute(
                "SELECT path, lixcol_created_at FROM non_uniform_journal_admission_probe ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        let update_sql =
            "UPDATE non_uniform_journal_admission_probe SET value = $1 WHERE path = $2";
        for version in ["first", "second"] {
            session
                .execute_batch(&[
                    ExecuteBatchStatement {
                        label: None,
                        sql: update_sql.to_string(),
                        params: vec![
                            Value::Text(version.to_string()),
                            Value::Text("a".to_string()),
                        ],
                    },
                    ExecuteBatchStatement {
                        label: None,
                        sql: update_sql.to_string(),
                        params: vec![
                            Value::Text(version.to_string()),
                            Value::Text("b".to_string()),
                        ],
                    },
                ])
                .await
                .unwrap();
        }
        let after = session
            .execute(
                "SELECT path, lixcol_created_at FROM non_uniform_journal_admission_probe ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(before.rows(), after.rows());
    }

    /// A file and its rows share one durability lane. An untracked
    /// parameter batch large enough to reach the dense certified transport
    /// must still land untracked — the lane is not a function of batch size.
    #[tokio::test]
    async fn expdl_dense_scale_untracked_parameter_batch_stays_untracked() {
        // The dense certified transport engages at this row count, so this is
        // the smallest batch that can exercise the dense projection.
        const ROW_COUNT: usize = 32 * 1024;
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "expdl_dense_untracked_lane_probe",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema \
                 (value, lixcol_global, lixcol_untracked) \
                 VALUES (CAST($1 AS JSONB), false, true)",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("untracked schema registration should succeed");

        let sql = "INSERT INTO expdl_dense_untracked_lane_probe \
                   (path, value, lixcol_untracked) VALUES ($1, CAST($2 AS JSONB), TRUE)";
        let statements = (0..ROW_COUNT)
            .map(|index| ExecuteBatchStatement {
                label: None,
                sql: sql.to_string(),
                params: vec![
                    Value::Text(format!("/p-{index:05}")),
                    Value::Text(format!("\"v-{index:05}\"")),
                ],
            })
            .collect::<Vec<_>>();
        session
            .execute_batch(&statements)
            .await
            .expect("dense-scale untracked parameter batch should commit");

        let totals = session
            .execute(
                "SELECT COUNT(*) AS entries FROM expdl_dense_untracked_lane_probe",
                &[],
            )
            .await
            .expect("probe rows should read");
        assert_eq!(
            totals.rows()[0].get::<i64>("entries").unwrap(),
            ROW_COUNT as i64
        );

        let lanes = session
            .execute(
                "SELECT COUNT(*) AS entries FROM expdl_dense_untracked_lane_probe \
                 WHERE lixcol_untracked",
                &[],
            )
            .await
            .expect("probe lanes should read");
        assert_eq!(
            lanes.rows()[0].get::<i64>("entries").unwrap(),
            ROW_COUNT as i64,
            "every row of an untracked batch must stay in the untracked lane"
        );

        let commits = session
            .execute(
                "SELECT COUNT(*) AS entries FROM expdl_dense_untracked_lane_probe \
                 WHERE lixcol_commit_id IS NOT NULL",
                &[],
            )
            .await
            .expect("probe commit ids should read");
        assert_eq!(
            commits.rows()[0].get::<i64>("entries").unwrap(),
            0,
            "untracked rows carry no commit id"
        );
    }

    #[tokio::test]
    async fn certified_parameter_batch_revalidates_after_staged_schema_amendment() {
        // Keep this at the production typed-transport boundary. A separate
        // route-selection unit test pins the threshold itself.
        const ROW_COUNT: usize = 32 * 1024;
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "amended_parameter_insert_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();

        let amended_schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "amended_parameter_insert_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
                { "name": "source", "type": "text", "nullable": false, "default_value": "amended-plan" },
            ],
            "primary_key": ["id"],
        });
        crate::transaction::take_direct_journal_replacement_publications(
            "amended_parameter_insert_probe",
        );
        let mut transaction = session.begin_transaction().await.unwrap();
        transaction
            .execute(
                "UPDATE lix_registered_schema SET value = $1 \
                 WHERE lixcol_row_pk = CAST('[\"amended_parameter_insert_probe\"]' AS JSONB)",
                &[Value::Json(amended_schema.into())],
            )
            .await
            .expect("compatible schema amendment should stage");

        let sql = "INSERT INTO amended_parameter_insert_probe (id, value) VALUES ($1, $2)";
        let statements = (0..ROW_COUNT)
            .map(|index| ExecuteBatchStatement {
                label: None,
                sql: sql.to_string(),
                params: vec![
                    Value::Text(format!("row-{index:05}")),
                    Value::Text(format!("value-{index:05}")),
                ],
            })
            .collect::<Vec<_>>();
        let parsed = TransactionBatchStatements::Shared {
            statement: sql2::parse_statement(sql).unwrap(),
            len: statements.len(),
        };
        let parameter_route = AtomicBool::new(false);
        let staged = try_execute_transaction_parameter_batch(
            transaction.transaction_mut().unwrap(),
            &statements,
            &parsed,
            &ExecuteOptions::default(),
            &vec![ExecuteStatementMetadata::default(); statements.len()],
            &parameter_route,
        )
        .await
        .expect("parameter batch should be revalidated");
        assert!(
            staged.is_some(),
            "the SQL batch should still use its typed parameter route"
        );
        transaction
            .commit()
            .await
            .expect("rows valid under the amended schema should commit");

        let rows = session
            .execute(
                "SELECT COUNT(*) AS entries FROM amended_parameter_insert_probe \
                 WHERE source = 'amended-plan'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.rows()[0].get::<i64>("entries").unwrap(),
            ROW_COUNT as i64,
            "transaction normalization must apply the staged schema's default"
        );
    }

    #[tokio::test]
    async fn certified_replacement_batch_revalidates_after_staged_schema_amendment() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "amended_parameter_update_probe",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO amended_parameter_update_probe (path, value) VALUES ('a', CAST('\"old-a\"' AS JSONB)), ('b', CAST('\"old-b\"' AS JSONB))",
                &[],
            )
            .await
            .unwrap();

        let amended_schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "amended_parameter_update_probe",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
                { "name": "source", "type": "text", "nullable": true, "default_value": "amended-plan" },
            ],
            "primary_key": ["path"],
        });
        let mut transaction = session.begin_transaction().await.unwrap();
        transaction
            .execute(
                "UPDATE lix_registered_schema SET value = $1 \
                 WHERE lixcol_row_pk = CAST('[\"amended_parameter_update_probe\"]' AS JSONB)",
                &[Value::Json(amended_schema.into())],
            )
            .await
            .expect("compatible schema amendment should stage");

        let sql = "UPDATE amended_parameter_update_probe SET value = CAST($1 AS JSONB) WHERE path = $2";
        let statements = [
            ExecuteBatchStatement {
                label: None,
                sql: sql.to_string(),
                params: vec![
                    Value::Text("\"updated-a\"".to_string()),
                    Value::Text("a".to_string()),
                ],
            },
            ExecuteBatchStatement {
                label: None,
                sql: sql.to_string(),
                params: vec![
                    Value::Text("\"updated-b\"".to_string()),
                    Value::Text("b".to_string()),
                ],
            },
        ];
        let parsed = TransactionBatchStatements::Shared {
            statement: sql2::parse_statement(sql).unwrap(),
            len: statements.len(),
        };
        sql2::take_certified_replacement_parameter_batch_executions();
        let staged = try_execute_transaction_parameter_batch(
            transaction.transaction_mut().unwrap(),
            &statements,
            &parsed,
            &ExecuteOptions::default(),
            &vec![ExecuteStatementMetadata::default(); statements.len()],
            &AtomicBool::new(false),
        )
        .await
        .expect("replacement batch should be revalidated");
        assert!(
            staged.is_some(),
            "the UPDATE batch should retain its typed parameter route"
        );
        assert_eq!(
            sql2::take_certified_replacement_parameter_batch_executions(),
            1,
            "the UPDATE batch must reach the certified replacement subroute"
        );
        transaction.commit().await.unwrap();

        let rows = session
            .execute(
                "SELECT path, value, source FROM amended_parameter_update_probe ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.rows()[0].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!("updated-a")
        );
        assert_eq!(
            rows.rows()[1].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!("updated-b")
        );
        assert!(
            rows.rows()
                .iter()
                .all(|row| row.get::<String>("source").unwrap() == "amended-plan"),
            "replacement normalization must apply the staged schema's default"
        );
    }

    #[tokio::test]
    async fn certified_batch_reconciles_concurrent_insert_at_commit_snapshot() {
        let storage = Memory::default();
        Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized storage should create engine");
        let setup = engine.open_session().await.unwrap();
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "concurrent_parameter_insert_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        setup
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();

        let first = engine.open_session().await.unwrap();
        let second = engine.open_session().await.unwrap();
        let sql = "INSERT INTO concurrent_parameter_insert_probe (id, value) VALUES ($1, $2)";
        let first_statements = [
            ExecuteBatchStatement {
                label: None,
                sql: sql.to_string(),
                params: vec![
                    Value::Text("first-only".to_string()),
                    Value::Text("first".to_string()),
                ],
            },
            ExecuteBatchStatement {
                label: None,
                sql: sql.to_string(),
                params: vec![
                    Value::Text("shared".to_string()),
                    Value::Text("first".to_string()),
                ],
            },
        ];
        let parsed = TransactionBatchStatements::Shared {
            statement: sql2::parse_statement(sql).unwrap(),
            len: first_statements.len(),
        };
        let mut first_transaction = first.begin_transaction().await.unwrap();
        let parameter_route = AtomicBool::new(false);
        let staged = try_execute_transaction_parameter_batch(
            first_transaction.transaction_mut().unwrap(),
            &first_statements,
            &parsed,
            &ExecuteOptions::default(),
            &vec![ExecuteStatementMetadata::default(); first_statements.len()],
            &parameter_route,
        )
        .await
        .unwrap();
        assert!(
            staged.is_some(),
            "first batch should take the certified route"
        );

        second
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("second-only".to_string()),
                        Value::Text("second".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("shared".to_string()),
                        Value::Text("second".to_string()),
                    ],
                },
            ])
            .await
            .expect("concurrent batch should commit first");

        first_transaction
            .commit()
            .await
            .expect("same-identity concurrent inserts use row-existence LWW");
        let rows = second
            .execute(
                "SELECT value FROM concurrent_parameter_insert_probe WHERE id = 'shared'",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(
            rows.rows()[0].get::<String>("value").unwrap().as_str(),
            "first" | "second"
        ));
    }

    #[tokio::test]
    async fn consecutive_certified_batches_preserve_local_conflict_statement_index() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "consecutive_parameter_insert_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO consecutive_parameter_insert_probe (id, value) VALUES ('z', 'old')",
                &[],
            )
            .await
            .unwrap();

        let sql = "INSERT INTO consecutive_parameter_insert_probe (id, value) VALUES ($1, $2)";
        let first_statements = [
            ExecuteBatchStatement {
                label: None,
                sql: sql.to_string(),
                params: vec![
                    Value::Text("a".to_string()),
                    Value::Text("first-a".to_string()),
                ],
            },
            ExecuteBatchStatement {
                label: None,
                sql: sql.to_string(),
                params: vec![
                    Value::Text("b".to_string()),
                    Value::Text("first-b".to_string()),
                ],
            },
        ];
        let second_statements = [
            ExecuteBatchStatement {
                label: None,
                sql: sql.to_string(),
                params: vec![
                    Value::Text("c".to_string()),
                    Value::Text("second-c".to_string()),
                ],
            },
            ExecuteBatchStatement {
                label: None,
                sql: sql.to_string(),
                params: vec![
                    Value::Text("z".to_string()),
                    Value::Text("duplicate-z".to_string()),
                ],
            },
        ];
        let mut transaction = session.begin_transaction().await.unwrap();
        let mut conflict = None;
        for (batch_index, statements) in [&first_statements[..], &second_statements[..]]
            .into_iter()
            .enumerate()
        {
            let parsed = TransactionBatchStatements::Shared {
                statement: sql2::parse_statement(sql).unwrap(),
                len: statements.len(),
            };
            let parameter_route = AtomicBool::new(false);
            let staged = try_execute_transaction_parameter_batch(
                transaction.transaction_mut().unwrap(),
                statements,
                &parsed,
                &ExecuteOptions::default(),
                &vec![ExecuteStatementMetadata::default(); statements.len()],
                &parameter_route,
            )
            .await;
            if batch_index == 0 {
                assert!(
                    staged.unwrap().is_some(),
                    "the first batch should take the certified route"
                );
            } else {
                conflict =
                    Some(staged.expect_err(
                        "the second row in the second batch conflicts with committed z",
                    ));
            }
        }

        let error = conflict.expect("the second batch must report its committed conflict");
        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert_eq!(error.details.unwrap()["statementIndex"], 1);
        drop(transaction);
        let rows = session
            .execute(
                "SELECT id FROM consecutive_parameter_insert_probe ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "both staged batches must roll back");
        assert_eq!(rows.rows()[0].get::<String>("id").unwrap(), "z");
    }

    #[tokio::test]
    async fn execute_batch_declines_uncertified_row_insert_rows() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "parameter_insert_fallback_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();

        sql2::take_certified_row_insert_parameter_batch_executions();
        let sql =
            "INSERT INTO parameter_insert_fallback_probe (id, value) VALUES ($1, CAST($2 AS JSONB))";
        let error = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("a".to_string()),
                        Value::Text("\"invalid-shape\"".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("b".to_string()),
                        Value::Text("not-json".to_string()),
                    ],
                },
            ])
            .await
            .expect_err("the later invalid JSON expression should be reported");
        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert_eq!(error.details.unwrap()["statementIndex"], 1);
        assert_eq!(
            sql2::take_certified_row_insert_parameter_batch_executions(),
            0
        );
    }

    #[tokio::test]
    async fn execute_batch_declines_json_marked_utf8_for_string_columns() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "parameter_insert_json_string_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();

        sql2::take_certified_row_insert_parameter_batch_executions();
        let sql = "INSERT INTO parameter_insert_json_string_probe (id, value) VALUES ($1, $2)";
        let error = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("a".to_string()),
                        Value::Json(serde_json::json!({"not": "text"}).into()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("b".to_string()),
                        Value::Json(serde_json::json!({"also": "not text"}).into()),
                    ],
                },
            ])
            .await
            .expect_err("JSON objects must not be coerced into string column values");
        assert_eq!(error.details.unwrap()["statementIndex"], 0);
        assert_eq!(
            sql2::take_certified_row_insert_parameter_batch_executions(),
            0
        );
        let rows = session
            .execute("SELECT id FROM parameter_insert_json_string_probe", &[])
            .await
            .unwrap();
        assert!(rows.rows().is_empty());
    }

    #[tokio::test]
    async fn execute_batch_preserves_early_duplicate_before_later_schema_error() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "parameter_insert_error_order_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();

        sql2::take_certified_row_insert_parameter_batch_executions();
        let sql = "INSERT INTO parameter_insert_error_order_probe (id, value) VALUES ($1, $2)";
        let error = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("a".to_string()),
                        Value::Text("valid".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("a".to_string()),
                        Value::Text("also valid".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![Value::Text("b".to_string()), Value::Text(String::new())],
                },
            ])
            .await
            .expect_err("the earlier duplicate must precede later schema validation");
        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert_eq!(error.details.unwrap()["statementIndex"], 1);
        assert_eq!(
            sql2::take_certified_row_insert_parameter_batch_executions(),
            0
        );
        let rows = session
            .execute("SELECT id FROM parameter_insert_error_order_probe", &[])
            .await
            .unwrap();
        assert!(rows.rows().is_empty());
    }

    #[tokio::test]
    async fn execute_batch_preserves_later_missing_branch_index() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "parameter_insert_branch_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        let active_branch_id = session
            .execute("SELECT lix_active_branch_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();

        let sql = "INSERT INTO parameter_insert_branch_probe_by_branch \
                   (id, value, lixcol_branch_id) VALUES ($1, $2, $3)";
        let error = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("a".to_string()),
                        Value::Text("value-a".to_string()),
                        Value::Text(active_branch_id),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("b".to_string()),
                        Value::Text("value-b".to_string()),
                        Value::Text("00000000-0000-7000-8000-000000000404".to_string()),
                    ],
                },
            ])
            .await
            .expect_err("the second row's missing branch must retain its statement index");
        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
        assert_eq!(error.details.unwrap()["statementIndex"], 1);

        let rows = session
            .execute(
                "SELECT id FROM parameter_insert_branch_probe WHERE id = 'a'",
                &[],
            )
            .await
            .unwrap();
        assert!(
            rows.is_empty(),
            "the valid prefix must roll back with the missing-branch batch"
        );
    }

    #[tokio::test]
    async fn execute_batch_preserves_later_durability_domain_error_index() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "parameter_insert_durability_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema \
                 (value, lixcol_global, lixcol_untracked) \
                 VALUES (CAST($1 AS JSONB), false, true)",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();

        let sql = "INSERT INTO parameter_insert_durability_probe \
                   (id, value, lixcol_untracked) VALUES ($1, $2, $3)";
        let error = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("a".to_string()),
                        Value::Text("value-a".to_string()),
                        Value::Boolean(true),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("b".to_string()),
                        Value::Text("value-b".to_string()),
                        Value::Boolean(false),
                    ],
                },
            ])
            .await
            .expect_err("the second row's tracked-catalog failure must retain its statement index");
        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert_eq!(error.details.unwrap()["statementIndex"], 1);

        let rows = session
            .execute(
                "SELECT id FROM parameter_insert_durability_probe WHERE id = 'a'",
                &[],
            )
            .await
            .unwrap();
        assert!(
            rows.is_empty(),
            "the untracked prefix must roll back with the mixed-durability batch"
        );
    }

    #[tokio::test]
    async fn execute_batch_lowers_distinct_bound_row_updates_once() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "parameter_batch_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO parameter_batch_probe (id, value) VALUES \
                 ('a', 'old-a'), ('b', 'old-b')",
                &[],
            )
            .await
            .unwrap();

        sql2::take_row_update_parameter_batch_executions();
        let sql = "UPDATE parameter_batch_probe SET value = $1 WHERE id = $2";
        let results = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("new-a".to_string()),
                        Value::Text("a".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("new-b".to_string()),
                        Value::Text("b".to_string()),
                    ],
                },
            ])
            .await
            .unwrap();

        assert_eq!(sql2::take_row_update_parameter_batch_executions(), 1);
        assert_eq!(
            results
                .iter()
                .map(ExecuteResult::rows_affected)
                .collect::<Vec<_>>(),
            vec![1, 1]
        );
        let rows = session
            .execute(
                "SELECT id, value FROM parameter_batch_probe ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.rows()[0].get::<String>("value").unwrap(), "new-a");
        assert_eq!(rows.rows()[1].get::<String>("value").unwrap(), "new-b");
    }

    #[tokio::test]
    async fn execute_prepared_dml_batch_preserves_order_absence_and_atomic_errors() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "prepared_dml_contract_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO prepared_dml_contract_probe (id, value) VALUES \
                 ('a', 'old-a'), ('b', 'old-b')",
                &[],
            )
            .await
            .unwrap();

        let sql =
            Arc::<str>::from("UPDATE prepared_dml_contract_probe SET value = $1 WHERE id = $2");
        let rows = PreparedDmlParameterBatch::from_rows([
            vec![Value::Text("new-b".into()), Value::Text("b".into())],
            vec![Value::Text("new-a".into()), Value::Text("a".into())],
            vec![Value::Text("missing".into()), Value::Text("missing".into())],
        ])
        .unwrap();
        let results = session
            .execute_prepared_dml_batch(Arc::clone(&sql), rows)
            .await
            .unwrap();
        assert_eq!(
            results
                .iter()
                .map(ExecuteResult::rows_affected)
                .collect::<Vec<_>>(),
            vec![1, 1, 0]
        );

        let error = session
            .execute_prepared_dml_batch(
                Arc::<str>::from(
                    "UPDATE prepared_dml_contract_probe SET value = CAST($1 AS JSONB) WHERE id = $2",
                ),
                PreparedDmlParameterBatch::from_rows([
                    vec![Value::Text("{invalid".into()), Value::Text("a".into())],
                    vec![Value::Text("{\"ok\":true}".into()), Value::Text("b".into())],
                ])
                .unwrap(),
            )
            .await
            .expect_err("invalid RETURN expression must abort the atomic prepared batch");
        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);

        let rows = session
            .execute(
                "SELECT id, value FROM prepared_dml_contract_probe ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.rows()[0].get::<String>("value").unwrap(), "new-a");
        assert_eq!(rows.rows()[1].get::<String>("value").unwrap(), "new-b");

        let mut transaction = session.begin_transaction().await.unwrap();
        transaction
            .execute(
                "UPDATE prepared_dml_contract_probe SET value = 'before' WHERE id = 'a'",
                &[],
            )
            .await
            .unwrap();
        let error = transaction
            .execute_prepared_dml_batch(
                Arc::<str>::from(
                    "UPDATE prepared_dml_contract_probe SET value = CAST($1 AS JSONB) WHERE id = $2",
                ),
                PreparedDmlParameterBatch::from_rows([
                    vec![Value::Text("{\"ok\":true}".into()), Value::Text("b".into())],
                    vec![Value::Text("{invalid".into()), Value::Text("a".into())],
                ])
                .unwrap(),
            )
            .await
            .expect_err("failed prepared statement must roll back its own staging");
        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        transaction
            .execute(
                "UPDATE prepared_dml_contract_probe SET value = 'after' WHERE id = 'b'",
                &[],
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let rows = session
            .execute(
                "SELECT id, value FROM prepared_dml_contract_probe ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.rows()[0].get::<String>("value").unwrap(), "before");
        assert_eq!(rows.rows()[1].get::<String>("value").unwrap(), "after");
    }

    #[tokio::test]
    async fn execute_batch_lowers_distinct_literal_row_updates_once() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "literal_parameter_batch_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO literal_parameter_batch_probe (id, value) VALUES \
                 ('a', 'old-a'), ('b', 'old-b')",
                &[],
            )
            .await
            .unwrap();

        sql2::take_row_update_parameter_batch_executions();
        let results = session
            .execute_batch(&[
                batch_statement(
                    "UPDATE literal_parameter_batch_probe SET value = 'new-a' WHERE id = 'a'",
                ),
                batch_statement(
                    "UPDATE literal_parameter_batch_probe SET value = 'new-b' WHERE id = 'b'",
                ),
            ])
            .await
            .unwrap();

        assert_eq!(sql2::take_row_update_parameter_batch_executions(), 1);
        assert_eq!(
            results
                .iter()
                .map(ExecuteResult::rows_affected)
                .collect::<Vec<_>>(),
            vec![1, 1]
        );
        let rows = session
            .execute(
                "SELECT id, value FROM literal_parameter_batch_probe ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.rows()[0].get::<String>("value").unwrap(), "new-a");
        assert_eq!(rows.rows()[1].get::<String>("value").unwrap(), "new-b");
    }

    #[tokio::test]
    async fn row_insert_values_use_one_certified_canonical_batch() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "certified_insert_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("schema registration should succeed");

        sql2::take_certified_row_insert_batch_executions();
        session
            .execute(
                "INSERT INTO certified_insert_probe (value, id) VALUES \
                 ('value-a', 'a'), ('value-b', 'b')",
                &[],
            )
            .await
            .expect("certified insert batch should commit");

        assert_eq!(sql2::take_certified_row_insert_batch_executions(), 1);
        let rows = session
            .execute(
                "SELECT id, value FROM certified_insert_probe ORDER BY id",
                &[],
            )
            .await
            .expect("certified rows should be readable");
        assert_eq!(rows.rows()[0].get::<String>("value").unwrap(), "value-a");
        assert_eq!(rows.rows()[1].get::<String>("value").unwrap(), "value-b");
    }

    #[tokio::test]
    async fn conflict_insert_filters_rows_before_snapshot_validation() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "conflict_validation_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("schema registration should succeed");
        session
            .execute(
                "INSERT INTO conflict_validation_probe (id, value) VALUES ('a', 'valid')",
                &[],
            )
            .await
            .expect("seed row should commit");

        sql2::take_certified_row_insert_batch_executions();
        session
            .execute(
                "INSERT INTO conflict_validation_probe (id, value) VALUES ('a', 'x') \
                 ON CONFLICT (id) DO NOTHING",
                &[],
            )
            .await
            .expect("conflicting invalid payload should be discarded before validation");

        assert_eq!(sql2::take_certified_row_insert_batch_executions(), 0);
        let rows = session
            .execute(
                "SELECT value FROM conflict_validation_probe WHERE id = 'a'",
                &[],
            )
            .await
            .expect("seed row should remain readable");
        assert_eq!(rows.rows()[0].get::<String>("value").unwrap(), "valid");
    }

    #[tokio::test]
    async fn certified_insert_compares_explicit_uuid_keys_by_external_value() {
        const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "explicit_uuid_key_probe",
            "columns": [
                { "name": "id", "type": "uuid", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("schema registration should succeed");

        sql2::take_certified_row_insert_batch_executions();
        session
            .execute(
                "INSERT INTO explicit_uuid_key_probe (id, value, lixcol_row_pk) \
                 VALUES ($1, 'value', CAST($2 AS JSONB))",
                &[
                    Value::Text(UUID.to_string()),
                    Value::Text(format!("[\"{UUID}\"]")),
                ],
            )
            .await
            .expect("matching typed and external UUID keys should commit");

        assert_eq!(sql2::take_certified_row_insert_batch_executions(), 1);
        let rows = session
            .execute(
                "SELECT value FROM explicit_uuid_key_probe WHERE id = $1",
                &[Value::Text(UUID.to_string())],
            )
            .await
            .expect("inserted UUID row should be readable");
        assert_eq!(rows.rows()[0].get::<String>("value").unwrap(), "value");
    }

    #[tokio::test]
    async fn execute_batch_certifies_out_of_order_complete_path_value_replacements() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "certified_replacement_probe",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO certified_replacement_probe (path, value) VALUES \
                 ('/a', CAST('\"old-a\"' AS JSONB)), ('/b', CAST('\"old-b\"' AS JSONB))",
                &[],
            )
            .await
            .unwrap();

        let sql = "UPDATE certified_replacement_probe SET value = CAST($1 AS JSONB) WHERE path = $2";
        let missing_results = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("invalid-missing-1".to_string()),
                        Value::Text("/missing".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("invalid-missing-2".to_string()),
                        Value::Text("/missing".to_string()),
                    ],
                },
            ])
            .await
            .expect("missing rows must not evaluate their replacement expression");
        assert_eq!(
            missing_results
                .iter()
                .map(ExecuteResult::rows_affected)
                .collect::<Vec<_>>(),
            vec![0, 0]
        );

        let error = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("not-json".to_string()),
                        Value::Text("/b".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text(r#"{"later":"valid"}"#.to_string()),
                        Value::Text("/b".to_string()),
                    ],
                },
            ])
            .await
            .expect_err("a later replacement must not hide earlier invalid JSON");
        assert_eq!(error.details.unwrap()["statementIndex"], 0);

        let error = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("invalid-b".to_string()),
                        Value::Text("/b".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("invalid-a".to_string()),
                        Value::Text("/a".to_string()),
                    ],
                },
            ])
            .await
            .expect_err("errors must retain statement order when identities are sorted");
        assert_eq!(error.details.unwrap()["statementIndex"], 0);

        sql2::take_certified_replacement_parameter_batch_executions();
        let results = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text(r#"{"nested":[1,true,"x"]}"#.to_string()),
                        Value::Text("/b".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text(r#"{"missing":1}"#.to_string()),
                        Value::Text("/missing".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("true".to_string()),
                        Value::Text("/a".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text(r#"{"final":"b"}"#.to_string()),
                        Value::Text("/b".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text(r#"{"missing":2}"#.to_string()),
                        Value::Text("/missing".to_string()),
                    ],
                },
            ])
            .await
            .unwrap();

        assert_eq!(
            sql2::take_certified_replacement_parameter_batch_executions(),
            1
        );
        assert_eq!(
            results
                .iter()
                .map(ExecuteResult::rows_affected)
                .collect::<Vec<_>>(),
            vec![1, 0, 1, 1, 0]
        );
        let rows = session
            .execute(
                "SELECT path, value FROM certified_replacement_probe ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.rows()[0].value("value").unwrap(),
            &Value::Json(serde_json::json!(true).into())
        );
        assert_eq!(
            rows.rows()[1].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!({"final": "b"})
        );
    }

    #[tokio::test]
    async fn complete_replacement_publishes_packed_current_base_and_accepts_later_overlays() {
        const ROW_COUNT: usize = 32 * 1_024;
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "packed_replacement_probe",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();

        let insert_sql =
            "INSERT INTO packed_replacement_probe (path, value) VALUES ($1, CAST($2 AS JSONB))";
        let inserts = (0..ROW_COUNT)
            .map(|row_index| ExecuteBatchStatement {
                label: None,
                sql: insert_sql.to_string(),
                params: vec![
                    Value::Text(format!("/{row_index:05}")),
                    Value::Text(format!(r#"{{"old":{row_index}}}"#)),
                ],
            })
            .collect::<Vec<_>>();
        session.execute_batch(&inserts).await.unwrap();

        crate::transaction::take_complete_replacement_packed_current_base_publications();
        crate::transaction::take_rootless_replacement_generation_publications();
        sql2::take_certified_generation_identity_replacements();
        let update_sql = "UPDATE packed_replacement_probe SET value = CAST($1 AS JSONB) WHERE path = $2";
        let updates = (0..ROW_COUNT)
            .map(|row_index| ExecuteBatchStatement {
                label: None,
                sql: update_sql.to_string(),
                params: vec![
                    Value::Text(format!(r#"{{"updated":{row_index}}}"#)),
                    Value::Text(format!("/{row_index:05}")),
                ],
            })
            .collect::<Vec<_>>();
        let affected = session
            .execute_batch(&updates)
            .await
            .unwrap()
            .iter()
            .map(ExecuteResult::rows_affected)
            .sum::<u64>();
        assert_eq!(affected, ROW_COUNT as u64);
        assert_eq!(
            sql2::take_certified_generation_identity_replacements(),
            1,
            "the untouched packed generation must prove the complete identity set without a row scan"
        );
        assert_eq!(
            crate::transaction::take_complete_replacement_packed_current_base_publications(),
            1,
            "the certified full replacement must publish one packed current base"
        );
        assert_eq!(
            crate::transaction::take_rootless_replacement_generation_publications(),
            1,
            "the certified replacement must publish a rootless partition generation"
        );

        let update_commit_id = session
            .execute("SELECT commit_id FROM lix_branch WHERE name = 'main'", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        let read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let historical = session
            .tracked_state
            .reader(read)
            .scan_batch_at_commit(
                &update_commit_id,
                &crate::tracked_state::TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["packed_replacement_probe".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(historical.len(), ROW_COUNT);
        assert!(
            historical
                .iter()
                .all(|row| row.snapshot_content().is_some_and(|snapshot| {
                    snapshot.contains("updated") && !snapshot.contains("old")
                })),
            "historical replay must stop at the replacement generation"
        );
        let read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let historical_all = session
            .tracked_state
            .reader(read)
            .scan_batch_at_commit(
                &update_commit_id,
                &crate::tracked_state::TrackedStateScanRequest::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            historical_all
                .iter()
                .filter(|row| row.schema_key() == "packed_replacement_probe")
                .count(),
            ROW_COUNT,
            "an unfiltered replay must compose the replacement with inherited partitions"
        );
        assert!(
            historical_all
                .iter()
                .any(|row| row.schema_key() == "lix_registered_schema"),
            "the replacement generation must retain unrelated durable partitions"
        );

        let mut rebuild_writes = session.storage.new_write_set();
        {
            let read = session
                .storage
                .begin_read(StorageReadOptions::default())
                .await
                .unwrap();
            session
                .tracked_state
                .root_rebuilder(&read, &mut rebuild_writes)
                .rebuild_commit_root_at(&update_commit_id)
                .await
                .unwrap();
        }
        session
            .storage
            .commit_write_set(rebuild_writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let rebuilt = session
            .tracked_state
            .reader(read)
            .scan_batch_at_commit(
                &update_commit_id,
                &crate::tracked_state::TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["packed_replacement_probe".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let rebuilt_rows = rebuilt.into_rows();
        let historical_rows = historical.into_rows();
        assert_eq!(rebuilt_rows.len(), historical_rows.len());
        for (row_index, (rebuilt_row, historical_row)) in
            rebuilt_rows.iter().zip(&historical_rows).enumerate()
        {
            assert_eq!(
                rebuilt_row, historical_row,
                "rebuilt replacement row {row_index} must equal replayed state"
            );
        }

        crate::transaction::take_rootless_replacement_generation_publications();
        let second_updates = (0..ROW_COUNT)
            .map(|row_index| ExecuteBatchStatement {
                label: None,
                sql: update_sql.to_string(),
                params: vec![
                    Value::Text(format!(r#"{{"second":{row_index}}}"#)),
                    Value::Text(format!("/{row_index:05}")),
                ],
            })
            .collect::<Vec<_>>();
        session.execute_batch(&second_updates).await.unwrap();
        assert_eq!(
            crate::transaction::take_rootless_replacement_generation_publications(),
            1,
            "a repeated replacement must collapse to one generation over the same durable fallback"
        );
        let second_commit_id = session
            .execute("SELECT commit_id FROM lix_branch WHERE name = 'main'", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        for (before, after) in [
            (&update_commit_id, &second_commit_id),
            (&second_commit_id, &update_commit_id),
        ] {
            let diff = session
                .execute(
                    "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) \
                     WHERE schema_key = 'packed_replacement_probe' AND diff_type = 'modified'",
                    &[Value::Text(before.clone()), Value::Text(after.clone())],
                )
                .await
                .unwrap();
            assert_eq!(
                diff.rows()[0].get::<i64>("entries").unwrap(),
                ROW_COUNT as i64,
                "replacement generations must retain the real commit graph in both diff directions"
            );
        }
        let main_branch_id = session.active_branch_id().await.unwrap();
        let historical_branch = session
            .create_branch(crate::CreateBranchOptions {
                id: Some("01930000-0000-7000-8000-00000000b002".to_string()),
                name: "replacement-generation-history".to_string(),
                from_commit_id: Some(update_commit_id.clone()),
            })
            .await
            .unwrap();
        session
            .switch_branch(crate::SwitchBranchOptions {
                branch_id: historical_branch.id,
            })
            .await
            .unwrap();
        let historical_points = session
            .execute(
                "SELECT path, value FROM packed_replacement_probe \
                 WHERE path IN ('/00000', '/32767') ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            historical_points.rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!({"updated": 0})
        );
        assert_eq!(
            historical_points.rows()[1]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!({"updated": ROW_COUNT - 1})
        );
        session
            .switch_branch(crate::SwitchBranchOptions {
                branch_id: main_branch_id.clone(),
            })
            .await
            .unwrap();

        let broad_rows = session
            .execute(
                "SELECT path, value FROM packed_replacement_probe ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(broad_rows.len(), ROW_COUNT);
        assert_eq!(
            broad_rows.rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!({"second": 0})
        );
        assert_eq!(
            broad_rows.rows()[ROW_COUNT - 1]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!({"second": ROW_COUNT - 1})
        );

        let rows = session
            .execute(
                "SELECT path, value FROM packed_replacement_probe WHERE path IN ('/00000', '/32767') ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.rows()[0].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!({"second": 0})
        );
        assert_eq!(
            rows.rows()[1].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!({"second": ROW_COUNT - 1})
        );

        let merge_base_branch = session
            .create_branch(crate::CreateBranchOptions {
                id: Some("01930000-0000-7000-8000-00000000b003".to_string()),
                name: "replacement-generation-merge".to_string(),
                from_commit_id: Some(second_commit_id.clone()),
            })
            .await
            .unwrap();
        session
            .switch_branch(crate::SwitchBranchOptions {
                branch_id: merge_base_branch.id.clone(),
            })
            .await
            .unwrap();
        session
            .execute(
                "UPDATE packed_replacement_probe SET value = CAST('{\"branch\":true}' AS JSONB) WHERE path = '/00000'",
                &[],
            )
            .await
            .unwrap();
        session
            .switch_branch(crate::SwitchBranchOptions {
                branch_id: main_branch_id.clone(),
            })
            .await
            .unwrap();
        session
            .execute(
                "UPDATE packed_replacement_probe SET value = CAST('{\"main\":true}' AS JSONB) WHERE path = '/32767'",
                &[],
            )
            .await
            .unwrap();
        let merge = session
            .merge_branch(crate::MergeBranchOptions {
                source_branch_id: merge_base_branch.id,
            })
            .await
            .unwrap();
        assert_eq!(merge.outcome, crate::MergeBranchOutcome::MergeCommitted);

        let merged_commit_id = session
            .execute("SELECT commit_id FROM lix_branch WHERE name = 'main'", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        let merged_rows = session
            .execute(
                "SELECT path, value FROM packed_replacement_probe ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            merged_rows.len(),
            ROW_COUNT,
            "merging sparse descendants of a replacement generation must not lose rows"
        );
        assert_eq!(
            merged_rows.rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!({"branch": true})
        );
        assert_eq!(
            merged_rows.rows()[ROW_COUNT - 1]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!({"main": true})
        );
        let merged_diff = session
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) \
                 WHERE schema_key = 'packed_replacement_probe' AND diff_type = 'modified'",
                &[
                    Value::Text(second_commit_id.clone()),
                    Value::Text(merged_commit_id.clone()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(merged_diff.rows()[0].get::<i64>("entries").unwrap(), 2);
        let merged_history = session
            .execute(
                &format!(
                    "SELECT value FROM packed_replacement_probe_history('{merged_commit_id}') \
                     WHERE path = '/00000' ORDER BY lixcol_depth"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            merged_history.rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!({"branch": true})
        );
        assert!(merged_history.rows().iter().any(|row| {
            row.get::<serde_json::Value>("value").ok() == Some(serde_json::json!({"second": 0}))
        }));

        session
            .execute(
                "UPDATE packed_replacement_probe SET value = CAST('{\"overlay\":true}' AS JSONB) WHERE path = '/00000'",
                &[],
            )
            .await
            .unwrap();
        let mixed_hot_cold = session
            .execute(
                "SELECT path, value FROM packed_replacement_probe \
                 WHERE path IN ('/00000', '/16000') ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(mixed_hot_cold.len(), 2);
        assert_eq!(
            mixed_hot_cold.rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!({"overlay": true}),
            "a mixed exact batch must retain the head-delta hit"
        );
        assert_eq!(
            mixed_hot_cold.rows()[1]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!({"second": 16_000}),
            "a mixed exact batch must resolve its cold key from inherited current state"
        );
        session
            .execute(
                "DELETE FROM packed_replacement_probe WHERE path = '/32767'",
                &[],
            )
            .await
            .unwrap();
        let rows = session
            .execute(
                "SELECT path, value FROM packed_replacement_probe ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), ROW_COUNT - 1);
        assert_eq!(rows.rows()[0].get::<String>("path").unwrap(), "/00000");
        assert_eq!(
            rows.rows()[0].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!({"overlay": true})
        );

        session
            .execute(
                "INSERT INTO packed_replacement_probe (path, value) VALUES ('/32767', CAST('{\"reinserted\":true}' AS JSONB))",
                &[],
            )
            .await
            .unwrap();
        let reinsert_commit_id = session
            .execute("SELECT commit_id FROM lix_branch WHERE name = 'main'", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        let read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let reinserted = session
            .tracked_state
            .reader(read)
            .scan_batch_at_commit(
                &reinsert_commit_id,
                &crate::tracked_state::TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["packed_replacement_probe".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let reinserted_created_at = reinserted
            .iter()
            .find(|row| row.row_pk().as_single_string().ok() == Some("/32767"))
            .expect("reinserted row must be visible")
            .created_at();

        crate::transaction::take_rootless_replacement_generation_publications();
        let post_reinsert_updates = (0..ROW_COUNT)
            .map(|row_index| ExecuteBatchStatement {
                label: None,
                sql: update_sql.to_string(),
                params: vec![
                    Value::Text(format!(r#"{{"post_reinsert":{row_index}}}"#)),
                    Value::Text(format!("/{row_index:05}")),
                ],
            })
            .collect::<Vec<_>>();
        session.execute_batch(&post_reinsert_updates).await.unwrap();
        assert_eq!(
            crate::transaction::take_rootless_replacement_generation_publications(),
            0,
            "a delete/reinsert interval must decline replacement certification without complete lifecycle evidence"
        );
        let post_reinsert_commit_id = session
            .execute("SELECT commit_id FROM lix_branch WHERE name = 'main'", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        let read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let post_reinsert = session
            .tracked_state
            .reader(read)
            .scan_batch_at_commit(
                &post_reinsert_commit_id,
                &crate::tracked_state::TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["packed_replacement_probe".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            post_reinsert
                .iter()
                .find(|row| row.row_pk().as_single_string().ok() == Some("/32767"))
                .expect("updated reinserted row must be visible")
                .created_at(),
            reinserted_created_at,
            "full updates must preserve the newer lifecycle of a reinserted identity"
        );

        // Pick a part from the authenticated current head, rather than the
        // lexicographically first object in the space. Historical and
        // unreachable parts may remain in this immutable space and deleting
        // one of those must not make GC fail closed.
        let read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let controls = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .scan()
            .await
            .expect("branch-head controls should load");
        let mut live_descriptor = None;
        for (_, control) in controls {
            let manifest =
                crate::tracked_state::load_commit_state_manifest(&read, control.head_commit_id)
                    .await
                    .expect("active head manifest should load");
            let Some(root) = manifest.and_then(|manifest| manifest.current_state_scoped_ranges)
            else {
                continue;
            };
            let reachable = crate::tracked_state::validate_scoped_range_trees(
                &read,
                std::slice::from_ref(&root.tree),
            )
            .await
            .expect("active head current-state tree should authenticate");
            live_descriptor = reachable
                .parts
                .iter()
                .map(crate::tracked_state::current_state_descriptor_from_scoped_range_part)
                .collect::<Result<Vec<_>, _>>()
                .expect("active head part descriptors should decode")
                .into_iter()
                .find(|descriptor| {
                    matches!(
                        descriptor.source,
                        crate::tracked_state::CurrentStatePartSource::NativeDataPart { .. }
                    )
                });
            if live_descriptor.is_some() {
                break;
            }
        }
        let live_descriptor =
            live_descriptor.expect("an active head should retain a live native part");
        let native_key = crate::storage_adapter::StorageKey(bytes::Bytes::copy_from_slice(
            &live_descriptor.content_digest,
        ));
        let native_presence = crate::storage_adapter::PointReadPlan::new(
            crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
            std::slice::from_ref(&native_key),
        )
        .materialize(&read, Default::default())
        .await
        .expect("live native part presence should read");
        assert!(
            native_presence.value[0].is_some(),
            "live part must exist before delete"
        );
        drop(read);
        let mut corrupt = session.storage.new_write_set();
        corrupt.delete(
            crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
            native_key,
        );
        session
            .storage
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = SharedStorageAdapterRead::new(
            session
                .storage
                .begin_read(StorageReadOptions::default())
                .await
                .unwrap(),
        );
        let mut gc_writes = session.storage.new_write_set();
        assert!(
            crate::gc::stage_repository_gc(read, &mut gc_writes)
                .await
                .is_err(),
            "GC must fail closed before sweeping when a live native part is missing"
        );
    }

    #[tokio::test]
    async fn staged_delete_disables_generation_identity_replacement() {
        const ROW_COUNT: usize = 16;
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "staged_generation_probe",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();

        let insert_sql =
            "INSERT INTO staged_generation_probe (path, value) VALUES ($1, CAST($2 AS JSONB))";
        let inserts = (0..ROW_COUNT)
            .map(|row_index| ExecuteBatchStatement {
                label: None,
                sql: insert_sql.to_string(),
                params: vec![
                    Value::Text(format!("/{row_index:04}")),
                    Value::Text(format!(r#"{{"old":{row_index}}}"#)),
                ],
            })
            .collect::<Vec<_>>();
        session.execute_batch(&inserts).await.unwrap();

        let mut transaction = session.begin_transaction().await.unwrap();
        transaction
            .execute(
                "DELETE FROM staged_generation_probe WHERE path = '/0000'",
                &[],
            )
            .await
            .unwrap();

        let update_sql = "UPDATE staged_generation_probe SET value = CAST($1 AS JSONB) WHERE path = $2";
        let updates = (0..ROW_COUNT)
            .map(|row_index| ExecuteBatchStatement {
                label: None,
                sql: update_sql.to_string(),
                params: vec![
                    Value::Text(format!(r#"{{"updated":{row_index}}}"#)),
                    Value::Text(format!("/{row_index:04}")),
                ],
            })
            .collect::<Vec<_>>();
        let parsed = TransactionBatchStatements::Shared {
            statement: sql2::parse_statement(update_sql).unwrap(),
            len: updates.len(),
        };
        sql2::take_certified_generation_identity_replacements();
        let results = try_execute_transaction_parameter_batch(
            transaction.transaction_mut().unwrap(),
            &updates,
            &parsed,
            &ExecuteOptions::default(),
            &vec![ExecuteStatementMetadata::default(); updates.len()],
            &AtomicBool::new(false),
        )
        .await
        .unwrap()
        .expect("the overlay-aware parameter batch should execute");
        assert_eq!(
            results
                .iter()
                .map(ExecuteResult::rows_affected)
                .sum::<u64>(),
            (ROW_COUNT - 1) as u64
        );
        assert_eq!(sql2::take_certified_generation_identity_replacements(), 0);
        transaction.commit().await.unwrap();

        let deleted = session
            .execute(
                "SELECT path FROM staged_generation_probe WHERE path = '/0000'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            deleted.len(),
            0,
            "the staged delete must not be resurrected"
        );
    }

    #[tokio::test]
    async fn execute_batch_keeps_repeated_generic_row_identity_sequential() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "parameter_batch_repeat_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        sql2::take_certified_row_insert_parameter_batch_executions();
        let insert_sql = "INSERT INTO parameter_batch_repeat_probe (id, value) VALUES ($1, $2)";
        let error = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: insert_sql.to_string(),
                    params: vec![
                        Value::Text("duplicate".to_string()),
                        Value::Text("first".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: insert_sql.to_string(),
                    params: vec![
                        Value::Text("duplicate".to_string()),
                        Value::Text("second".to_string()),
                    ],
                },
            ])
            .await
            .expect_err("the second INSERT repeats the first identity");
        assert_eq!(error.details.unwrap()["statementIndex"], 1);
        assert_eq!(
            sql2::take_certified_row_insert_parameter_batch_executions(),
            0
        );
        session
            .execute(
                "INSERT INTO parameter_batch_repeat_probe (id, value) VALUES ('a', 'old')",
                &[],
            )
            .await
            .unwrap();

        sql2::take_row_update_parameter_batch_executions();
        let sql = "UPDATE parameter_batch_repeat_probe SET value = $1 WHERE id = $2";
        session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("first".to_string()),
                        Value::Text("a".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("second".to_string()),
                        Value::Text("a".to_string()),
                    ],
                },
            ])
            .await
            .unwrap();

        assert_eq!(sql2::take_row_update_parameter_batch_executions(), 0);
        let row = session
            .execute(
                "SELECT value FROM parameter_batch_repeat_probe WHERE id = 'a'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row.rows()[0].get::<String>("value").unwrap(), "second");
    }

    #[tokio::test]
    async fn execute_batch_keeps_unsupported_parameterless_updates_sequential() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "parameterless_batch_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO parameterless_batch_probe (id, value) VALUES ('a', 'old')",
                &[],
            )
            .await
            .unwrap();

        sql2::take_row_update_parameter_batch_executions();
        let results = session
            .execute_batch(&[
                batch_statement(
                    "UPDATE parameterless_batch_probe SET value = 'first' WHERE id = 'a'",
                ),
                batch_statement(
                    "UPDATE parameterless_batch_probe SET value = 'second' WHERE id = 'a'",
                ),
            ])
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(
            results
                .iter()
                .map(ExecuteResult::rows_affected)
                .collect::<Vec<_>>(),
            vec![1, 1]
        );
        assert_eq!(sql2::take_row_update_parameter_batch_executions(), 0);
        let row = session
            .execute(
                "SELECT value FROM parameterless_batch_probe WHERE id = 'a'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row.rows()[0].get::<String>("value").unwrap(), "second");
    }

    #[tokio::test]
    async fn execute_batch_keeps_inter_row_constraints_on_sequential_execution() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "parameter_batch_constraint_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
            "unique": [["value"]],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO parameter_batch_constraint_probe (id, value) VALUES \
                 ('a', 'old-a'), ('b', 'old-b')",
                &[],
            )
            .await
            .unwrap();

        sql2::take_certified_row_insert_parameter_batch_executions();
        let insert_sql = "INSERT INTO parameter_batch_constraint_probe (id, value) VALUES ($1, $2)";
        session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: insert_sql.to_string(),
                    params: vec![
                        Value::Text("c".to_string()),
                        Value::Text("old-c".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: insert_sql.to_string(),
                    params: vec![
                        Value::Text("d".to_string()),
                        Value::Text("old-d".to_string()),
                    ],
                },
            ])
            .await
            .unwrap();
        assert_eq!(
            sql2::take_certified_row_insert_parameter_batch_executions(),
            0
        );

        sql2::take_row_update_parameter_batch_executions();
        let sql = "UPDATE parameter_batch_constraint_probe SET value = $1 WHERE id = $2";
        session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("new-a".to_string()),
                        Value::Text("a".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("new-b".to_string()),
                        Value::Text("b".to_string()),
                    ],
                },
            ])
            .await
            .unwrap();

        assert_eq!(sql2::take_row_update_parameter_batch_executions(), 0);
    }

    #[tokio::test]
    async fn execute_batch_parameter_batch_preserves_failing_statement_index() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "parameter_batch_error_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO parameter_batch_error_probe (id, value) VALUES \
                 ('a', 'old-a'), ('b', 'old-b')",
                &[],
            )
            .await
            .unwrap();

        sql2::take_row_update_parameter_batch_executions();
        let sql = "UPDATE parameter_batch_error_probe SET value = CAST($1 AS JSONB) WHERE id = $2";
        let error = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("\"new-a\"".to_string()),
                        Value::Text("a".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("not-json".to_string()),
                        Value::Text("b".to_string()),
                    ],
                },
            ])
            .await
            .expect_err("the second statement has invalid JSON");

        assert_eq!(error.details.unwrap()["statementIndex"], 1);
        assert_eq!(sql2::take_row_update_parameter_batch_executions(), 0);
        let rows = session
            .execute(
                "SELECT id, value FROM parameter_batch_error_probe ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.rows()[0].get::<String>("value").unwrap(), "old-a");
        assert_eq!(rows.rows()[1].get::<String>("value").unwrap(), "old-b");
    }

    #[tokio::test]
    async fn execute_batch_parameter_batch_indexes_parameter_count_errors() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "parameter_batch_count_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();

        let sql = "UPDATE parameter_batch_count_probe SET value = $1 WHERE id = $2";
        let error = session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("new-a".to_string()),
                        Value::Text("a".to_string()),
                        Value::Text("extra".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("new-b".to_string()),
                        Value::Text("b".to_string()),
                        Value::Text("extra".to_string()),
                    ],
                },
            ])
            .await
            .expect_err("extra parameters must be rejected");

        assert_eq!(error.details.unwrap()["statementIndex"], 0);
    }

    #[tokio::test]
    async fn execute_batch_parameter_batch_emits_statement_telemetry() {
        let spans = Arc::new(std::sync::Mutex::new(Vec::new()));
        let session = open_session_with_telemetry(Arc::clone(&spans)).await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "parameter_batch_telemetry_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO parameter_batch_telemetry_probe (id, value) VALUES \
                 ('a', 'old-a'), ('b', 'old-b')",
                &[],
            )
            .await
            .unwrap();
        spans.lock().expect("telemetry span lock").clear();

        let sql = "UPDATE parameter_batch_telemetry_probe SET value = $1 WHERE id = $2";
        session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("new-a".to_string()),
                        Value::Text("a".to_string()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: sql.to_string(),
                    params: vec![
                        Value::Text("new-b".to_string()),
                        Value::Text("b".to_string()),
                    ],
                },
            ])
            .await
            .unwrap();

        let spans = spans.lock().expect("telemetry span lock");
        let query_spans = spans
            .iter()
            .filter(|span| span.start.kind == TelemetrySpanKind::SqlQuery)
            .collect::<Vec<_>>();
        assert_eq!(query_spans.len(), 2);
        for (index, span) in query_spans.into_iter().enumerate() {
            assert!(span.start.attributes.iter().any(|attribute| {
                attribute.key == "lix.sql.fingerprint"
                    && matches!(&attribute.value, TelemetryValue::String(value) if !value.is_empty())
            }));
            assert!(span.start.attributes.iter().any(|attribute| {
                attribute.key == "lix.batch.index"
                    && attribute.value == TelemetryValue::U64(index as u64)
            }));
            assert!(span.end.attributes.iter().any(|attribute| {
                attribute.key == "lix.rows_affected" && attribute.value == TelemetryValue::U64(1)
            }));
        }
    }

    #[tokio::test]
    async fn exact_batch_file_read_returns_each_matching_file_once() {
        let session = open_session().await;
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2), ($3, $4)",
                &[
                    Value::Text("/b.txt".to_string()),
                    Value::Blob(b"bravo".to_vec().into()),
                    Value::Text("/a.txt".to_string()),
                    Value::Blob(b"alpha".to_vec().into()),
                ],
            )
            .await
            .unwrap();

        let result = session
            .execute(
                "SELECT path, content FROM lix_file WHERE path IN ($1, $2, $3)",
                &[
                    Value::Text("/b.txt".to_string()),
                    Value::Text("/a.txt".to_string()),
                    Value::Text("/b.txt".to_string()),
                ],
            )
            .await
            .unwrap();

        assert_eq!(result.columns(), &["path", "content"]);
        assert_eq!(result.rows().len(), 2);
        assert_eq!(result.rows()[0].get::<String>("path").unwrap(), "/a.txt");
        assert_eq!(
            result.rows()[0].value("content").unwrap(),
            &Value::Blob(b"alpha".to_vec().into())
        );
        assert_eq!(result.rows()[1].get::<String>("path").unwrap(), "/b.txt");
        assert_eq!(
            result.rows()[1].value("content").unwrap(),
            &Value::Blob(b"bravo".to_vec().into())
        );
    }

    #[tokio::test]
    async fn exact_id_manifest_batch_preserves_bytes_and_metadata() {
        let session = open_session().await;
        let a = "01920000-0000-7000-8000-0000000000a1";
        let b = "01920000-0000-7000-8000-0000000000a2";
        session
            .execute(
                "INSERT INTO lix_file (id, path, content, lixcol_metadata) \
                 VALUES ($1, $2, $3, $4), ($5, $6, $7, $8)",
                &[
                    Value::Text(b.to_string()),
                    Value::Text("/b.txt".to_string()),
                    Value::Blob(b"bravo".to_vec().into()),
                    Value::Json(serde_json::json!({"git_mode":"100644","git_oid":"b"}).into()),
                    Value::Text(a.to_string()),
                    Value::Text("/a.txt".to_string()),
                    Value::Blob(b"alpha".to_vec().into()),
                    Value::Json(serde_json::json!({"git_mode":"100644","git_oid":"a"}).into()),
                ],
            )
            .await
            .unwrap();

        let result = session
            .execute(
                "SELECT id, path, content, lixcol_metadata FROM lix_file WHERE id IN ($1, $2)",
                &[Value::Text(b.to_string()), Value::Text(a.to_string())],
            )
            .await
            .unwrap();

        assert_eq!(
            result.columns(),
            &["id", "path", "content", "lixcol_metadata"]
        );
        assert_eq!(result.rows().len(), 2);
        assert_eq!(result.rows()[0].get::<String>("id").unwrap(), a);
        assert_eq!(result.rows()[0].get::<String>("path").unwrap(), "/a.txt");
        assert_eq!(
            result.rows()[0].value("content").unwrap(),
            &Value::Blob(b"alpha".to_vec().into())
        );
        assert_eq!(
            result.rows()[0].value("lixcol_metadata").unwrap(),
            &Value::Json(serde_json::json!({"git_mode":"100644","git_oid":"a"}).into())
        );
        assert_eq!(result.rows()[1].get::<String>("id").unwrap(), b);
        assert_eq!(result.rows()[1].get::<String>("path").unwrap(), "/b.txt");
        assert_eq!(
            result.rows()[1].value("content").unwrap(),
            &Value::Blob(b"bravo".to_vec().into())
        );
    }

    #[tokio::test]
    async fn late_file_content_read_preserves_metadata_filters_order_and_limit() {
        let session = open_session().await;
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2), ($3, $4), ($5, $6)",
                &[
                    Value::Text("/a.txt".to_string()),
                    Value::Blob(b"alpha".to_vec().into()),
                    Value::Text("/b.txt".to_string()),
                    Value::Blob(b"bravo".to_vec().into()),
                    Value::Text("/c.txt".to_string()),
                    Value::Blob(b"charlie".to_vec().into()),
                ],
            )
            .await
            .unwrap();

        let result = session
            .execute(
                "SELECT path, content FROM lix_file WHERE path LIKE $1 ORDER BY path DESC LIMIT 2",
                &[Value::Text("%.txt".to_string())],
            )
            .await
            .unwrap();

        assert_eq!(result.columns(), &["path", "content"]);
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Text("/c.txt".to_string()),
                Value::Blob(b"charlie".to_vec().into()),
            ]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[
                Value::Text("/b.txt".to_string()),
                Value::Blob(b"bravo".to_vec().into()),
            ]
        );
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
    fn columnar_result_keeps_batches_until_rows_are_requested() {
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, false),
        ];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(fields.clone())),
            vec![
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![1, 2])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec!["a", "b"])),
            ],
        )
        .expect("test columnar batch should be valid");
        let batches: Arc<[RecordBatch]> = vec![batch].into();
        let result = ExecuteResult::from_columnar_result(fields, batches, Vec::new());

        assert_eq!(result.columns(), ["id", "title"]);
        let rows = result.rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<i64>("id").unwrap(), 1);
        assert_eq!(rows[1].get::<String>("title").unwrap(), "b");
        assert_eq!(result.rows().as_ptr(), rows.as_ptr());
    }

    #[test]
    fn execute_result_clone_shares_immutable_backing() {
        let result = ExecuteResult::from_rows(
            vec!["content".to_string()],
            vec![vec![Value::Blob(vec![b'x'; 1024 * 1024].into())]],
        );
        let cloned = result.clone();

        assert!(Arc::ptr_eq(
            result.backing.as_ref().unwrap(),
            cloned.backing.as_ref().unwrap()
        ));
        assert_eq!(result, cloned);
    }

    #[test]
    fn mutation_result_equality_is_independent_of_empty_backing_representation() {
        let inline = ExecuteResult::from_rows_affected(7);
        let materialized = ExecuteResult::from_query_parts(Vec::new(), Vec::new(), 7, Vec::new());

        assert_eq!(inline, materialized);
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
    async fn tracked_insert_fast_lane_rejects_duplicate_committed_identity_without_overwrite() {
        let session = open_session().await;
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('duplicate-fast-lane', 'original')",
                &[],
            )
            .await
            .expect("the original tracked row should commit");

        let error = session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('duplicate-fast-lane', 'replacement')",
                &[],
            )
            .await
            .expect_err("a committed tracked INSERT identity must remain absent-only");
        assert_eq!(error.code, LixError::CODE_UNIQUE);

        let result = session
            .execute(
                "SELECT value FROM lix_key_value \
                 WHERE key = 'duplicate-fast-lane'",
                &[],
            )
            .await
            .expect("the original row should remain readable after rejection");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0]
                .get::<serde_json::Value>("value")
                .expect("value should remain JSON"),
            serde_json::json!("original"),
            "the rejected INSERT must not overwrite committed state"
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
        let statements: [(&str, &[Value]); 3] = [
            ("SELECT 'first' AS label", &[]),
            (
                "SELECT key, value FROM lix_key_value WHERE key = 'batch-read'",
                &[],
            ),
            (
                "SELECT lixcol_depth \
                 FROM lix_key_value_history() \
                 WHERE key = 'batch-read'",
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
        assert_eq!(batch.results.len(), 3);
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
        assert_eq!(
            batch.results[2].rows()[0]
                .get::<i64>("lixcol_depth")
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn coherent_read_batch_registers_union_of_referenced_providers() {
        let session = open_session().await;
        let statements: [(&str, &[Value]); 3] = [
            ("SELECT 1 AS one", &[]),
            ("SELECT COUNT(*) AS files FROM lix_file", &[]),
            ("SELECT COUNT(*) AS changes FROM lix_change", &[]),
        ];

        let batch = session
            .execute_coherent_read_batch(&statements)
            .await
            .expect("coherent batch should register every referenced provider");

        assert_eq!(batch.results.len(), 3);
        assert_eq!(batch.results[0].rows()[0].get::<i64>("one").unwrap(), 1);
        assert_eq!(batch.results[1].rows()[0].get::<i64>("files").unwrap(), 0);
        assert!(batch.results[2].rows()[0].get::<i64>("changes").unwrap() > 0);
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
                     SELECT row_pk FROM lix_change \
                     UNION ALL \
                     SELECT row_pk FROM lix_change\
                 ) AS changes ON false",
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
    async fn read_provider_selection_reuses_compiled_catalog_for_dynamic_visibility() {
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
            .expect("fixed schema surface should execute");
        assert_eq!(
            schema_loads(),
            before,
            "fixed row metadata comes from compile-time schemas"
        );

        session
            .execute("SELECT COUNT(*) AS rows FROM lix_key_value_history()", &[])
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
                 JOIN lix_change AS change ON false",
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
            before,
            "catalog-wide visibility must use the revision-keyed catalog instead of rescanning schemas"
        );

        let custom_schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "custom_catalog_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(custom_schema.to_string())],
            )
            .await
            .expect("custom schema should register");
        let before_custom_read = schema_loads();

        session
            .execute("SELECT COUNT(*) AS rows FROM custom_catalog_probe", &[])
            .await
            .expect("custom row should execute");
        assert_eq!(
            schema_loads(),
            before_custom_read,
            "custom row metadata must use the compiled catalog instead of rescanning schemas"
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
            before_mixed_join,
            "one custom table must keep using the compiled catalog without rescanning schemas"
        );

        let mut next_schema = custom_schema.clone();
        next_schema["key"] = serde_json::json!("custom_catalog_probe_after_mutation");
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(next_schema.to_string())],
            )
            .await
            .expect("second custom schema should register");
        let before_changed_catalog_read = schema_loads();
        session
            .execute(
                "SELECT COUNT(*) AS rows FROM custom_catalog_probe_after_mutation",
                &[],
            )
            .await
            .expect("schema revision must invalidate the cached SQL catalog");
        assert_eq!(
            schema_loads(),
            before_changed_catalog_read,
            "the next catalog generation must still avoid the uncached schema projection"
        );
    }

    #[tokio::test]
    async fn programmatic_replace_then_sql_update_preserves_durable_created_at() {
        const KEY: &str = "created-at-overlay";
        const FIRST_CREATED_AT: &str = "2020-01-01T00:00:00.000Z";

        fn row(value: &str, created_at: Option<&str>) -> TransactionWriteRow {
            TransactionWriteRow {
                row_pk: Some(RowPk::single(KEY)),
                schema_key: "lix_key_value".into(),
                file_id: None,
                snapshot: Some(TransactionJson::from_value_for_test(serde_json::json!({
                    "key": KEY,
                    "value": value,
                }))),
                metadata: None,
                origin: None,
                created_at: created_at.map(str::to_owned),
                updated_at: None,
                global: true,
                change_id: None,
                commit_id: None,
                untracked: false,
                branch_id: crate::GLOBAL_BRANCH_ID.into(),
            }
        }

        let session = open_session().await;

        let mut seed = session
            .begin_transaction()
            .await
            .expect("seed transaction should begin");
        seed.transaction_mut()
            .expect("seed transaction should be open")
            .stage_rows(RawWriteBatch::from_test_rows(vec![row(
                "seed",
                Some(FIRST_CREATED_AT),
            )]))
            .await
            .expect("programmatic seed should stage");
        seed.commit()
            .await
            .expect("programmatic seed should commit");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .transaction_mut()
            .expect("transaction should be open")
            .stage_rows(RawWriteBatch::from_test_rows(vec![row(
                "programmatic replacement",
                None,
            )]))
            .await
            .expect("programmatic replacement should stage");
        transaction
            .execute(
                "UPDATE lix_key_value SET value = 'sql replacement' \
                 WHERE key = 'created-at-overlay'",
                &[],
            )
            .await
            .expect("SQL update should read and replace the staged row");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let created_at = session
            .execute(
                "SELECT lixcol_created_at FROM lix_key_value \
                 WHERE key = 'created-at-overlay'",
                &[],
            )
            .await
            .expect("final row should be readable")
            .rows()[0]
            .get::<String>("lixcol_created_at")
            .expect("created timestamp should be text");
        assert_eq!(created_at, FIRST_CREATED_AT);
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
                "INSERT INTO lix_file (id, path) VALUES ('01920000-0000-7000-8000-000000000422', '/selected.txt')",
                &[],
            )
            .await
            .expect("file should stage");

        let result = transaction
            .execute(
                "WITH selected AS (\
                     SELECT id FROM lix_file WHERE id = '01920000-0000-7000-8000-000000000422'\
                 ) \
                 SELECT id FROM selected",
                &[],
            )
            .await
            .expect("selected overlay provider should expose staged writes");
        assert_eq!(
            result.rows()[0].get::<String>("id").unwrap(),
            "01920000-0000-7000-8000-000000000422"
        );

        transaction
            .rollback()
            .await
            .expect("transaction should roll back");
    }

    #[tokio::test]
    async fn explicit_transaction_literal_updates_preserve_escaped_string_values() {
        let session = open_session().await;
        for (key, value) in [("auto'one", "seed one"), ("auto'two", "seed two")] {
            session
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
                    &[Value::Text(key.to_string()), Value::Text(value.to_string())],
                )
                .await
                .expect("seed row should commit");
        }

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        let first = transaction
            .execute(
                "UPDATE lix_key_value SET value = 'second''s value' WHERE key = 'auto''two'",
                &[],
            )
            .await
            .expect("first literal update should stage");
        let second = transaction
            .execute(
                "UPDATE lix_key_value SET value = 'first''s value' WHERE key = 'auto''one'",
                &[],
            )
            .await
            .expect("descending literal update should cross the order barrier");
        assert_eq!(first.rows_affected(), 1);
        assert_eq!(second.rows_affected(), 1);
        transaction
            .commit()
            .await
            .expect("literal updates should commit atomically");

        let values = session
            .execute(
                "SELECT key, value FROM lix_key_value WHERE key IN ($1, $2) ORDER BY key",
                &[
                    Value::Text("auto'one".to_string()),
                    Value::Text("auto'two".to_string()),
                ],
            )
            .await
            .expect("updated rows should be readable");
        assert_eq!(values.len(), 2);
        assert_eq!(
            values.rows()[0].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!("first's value")
        );
        assert_eq!(
            values.rows()[1].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!("second's value")
        );
    }

    #[tokio::test]
    async fn explicit_transaction_parameter_updates_reset_membership_on_descending_keys() {
        let session = open_session().await;
        for key in ["parameter-a", "parameter-z"] {
            session
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
                    &[
                        Value::Text(key.to_string()),
                        Value::Text("seed".to_string()),
                    ],
                )
                .await
                .expect("seed row should commit");
        }

        let mut transaction = session.begin_transaction().await.unwrap();
        let sql = "UPDATE lix_key_value SET value = $1 WHERE key = $2";
        for (key, value) in [("parameter-z", "updated-z"), ("parameter-a", "updated-a")] {
            assert_eq!(
                transaction
                    .execute(
                        sql,
                        &[Value::Text(value.to_string()), Value::Text(key.to_string()),],
                    )
                    .await
                    .expect("descending parameter update should stage")
                    .rows_affected(),
                1
            );
        }
        transaction.commit().await.unwrap();

        let values = session
            .execute(
                "SELECT key, value FROM lix_key_value WHERE key IN ($1, $2) ORDER BY key",
                &[
                    Value::Text("parameter-a".to_string()),
                    Value::Text("parameter-z".to_string()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            values.rows()[0].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!("updated-a")
        );
        assert_eq!(
            values.rows()[1].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!("updated-z")
        );
    }

    #[tokio::test]
    async fn explicit_transaction_certified_json_pointer_updates_observe_staged_rows() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "json_pointer",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("json pointer schema should register");
        session
            .execute(
                "INSERT INTO json_pointer (path, value) VALUES ($1, CAST($2 AS JSONB))",
                &[
                    Value::Text("/certified".to_string()),
                    Value::Text("{\"step\":0}".to_string()),
                ],
            )
            .await
            .expect("json pointer seed should commit");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        assert_eq!(sql2::take_certified_single_path_value_replacements(), 0);
        let sql = "UPDATE json_pointer SET value = CAST($1 AS JSONB) WHERE path = $2";
        for step in [1, 2] {
            let result = transaction
                .execute(
                    sql,
                    &[
                        Value::Text(format!("{{\"step\":{step}}}")),
                        Value::Text("/certified".to_string()),
                    ],
                )
                .await
                .expect("certified replacement should stage");
            assert_eq!(result.rows_affected(), 1);
        }
        let missing = transaction
            .execute(
                sql,
                &[
                    Value::Text("{\"step\":3}".to_string()),
                    Value::Text("/missing".to_string()),
                ],
            )
            .await
            .expect("missing certified replacement should succeed");
        assert_eq!(missing.rows_affected(), 0);
        assert_eq!(sql2::take_certified_single_path_value_replacements(), 2);
        transaction
            .commit()
            .await
            .expect("certified replacements should commit atomically");

        let result = session
            .execute(
                "SELECT value FROM json_pointer WHERE path = $1",
                &[Value::Text("/certified".to_string())],
            )
            .await
            .expect("committed json pointer should be visible");
        assert_eq!(result.len(), 1);
        assert_eq!(
            result.rows()[0]
                .get::<serde_json::Value>("value")
                .expect("JSON value should decode"),
            serde_json::json!({"step": 2})
        );
    }

    #[tokio::test]
    async fn packed_mutation_membership_defers_to_transaction_overlay() {
        const ROW_COUNT: usize = 1_024;
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "packed_journal_overlay_probe",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("overlay probe schema should register");
        let inserts = (0..ROW_COUNT)
            .map(|row_index| ExecuteBatchStatement {
                label: None,
                sql: "INSERT INTO packed_journal_overlay_probe (path, value) VALUES ($1, CAST($2 AS JSONB))"
                    .to_string(),
                params: vec![
                    Value::Text(format!("{row_index:04}")),
                    Value::Text("{\"state\":\"base\"}".to_string()),
                ],
            })
            .collect::<Vec<_>>();
        session
            .execute_batch(&inserts)
            .await
            .expect("packed base should seed");
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        let update_sql =
            "UPDATE packed_journal_overlay_probe SET value = CAST($1 AS JSONB) WHERE path = $2";
        transaction
            .execute(
                update_sql,
                &[
                    Value::Text("{\"state\":\"updated-base\"}".to_string()),
                    Value::Text("0000".to_string()),
                ],
            )
            .await
            .expect("base update should prepare packed membership");
        transaction
            .execute(
                "INSERT INTO packed_journal_overlay_probe (path, value) VALUES ('2000', CAST('{\"state\":\"inserted\"}' AS JSONB))",
                &[],
            )
            .await
            .expect("transaction-local insert should stage");
        let inserted_update = transaction
            .execute(
                update_sql,
                &[
                    Value::Text("{\"state\":\"updated-insert\"}".to_string()),
                    Value::Text("2000".to_string()),
                ],
            )
            .await
            .expect("update must observe the transaction-local insert");
        assert_eq!(inserted_update.rows_affected(), 1);

        transaction
            .execute(
                "DELETE FROM packed_journal_overlay_probe WHERE path = '0001'",
                &[],
            )
            .await
            .expect("transaction-local delete should stage");
        let deleted_update = transaction
            .execute(
                update_sql,
                &[
                    Value::Text("{\"state\":\"must-not-resurrect\"}".to_string()),
                    Value::Text("0001".to_string()),
                ],
            )
            .await
            .expect("update after a staged delete should remain a no-op");
        assert_eq!(deleted_update.rows_affected(), 0);
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let rows = session
            .execute(
                "SELECT path, value FROM packed_journal_overlay_probe \
                 WHERE path IN ('0000', '0001', '2000') ORDER BY path",
                &[],
            )
            .await
            .expect("committed overlay result should be readable");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows.rows()[0].get::<String>("path").unwrap(), "0000");
        assert_eq!(
            rows.rows()[0].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!({"state": "updated-base"})
        );
        assert_eq!(rows.rows()[1].get::<String>("path").unwrap(), "2000");
        assert_eq!(
            rows.rows()[1].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!({"state": "updated-insert"})
        );
    }

    #[tokio::test]
    async fn stale_complete_journal_replacement_preserves_disjoint_insert() {
        const ROW_COUNT: usize = 1_024;
        let storage = Memory::default();
        Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized storage should create engine");
        let session = engine
            .open_session()
            .await
            .expect("replacement session should open");
        let concurrent_session = engine
            .open_session()
            .await
            .expect("concurrent session should open");
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "stale_journal_replacement_probe",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("stale replacement schema should register");
        let inserts = (0..ROW_COUNT)
            .map(|row_index| ExecuteBatchStatement {
                label: None,
                sql: "INSERT INTO stale_journal_replacement_probe (path, value) VALUES ($1, CAST($2 AS JSONB))"
                    .to_string(),
                params: vec![
                    Value::Text(format!("{row_index:04}")),
                    Value::Text("{\"state\":\"base\"}".to_string()),
                ],
            })
            .collect::<Vec<_>>();
        session
            .execute_batch(&inserts)
            .await
            .expect("packed base should seed");
        let original_created_at = session
            .execute(
                "SELECT lixcol_created_at FROM stale_journal_replacement_probe WHERE path = '0000'",
                &[],
            )
            .await
            .expect("seed lifecycle should be readable")
            .rows()[0]
            .get::<String>("lixcol_created_at")
            .unwrap()
            .clone();
        let mut replacement = session
            .begin_transaction()
            .await
            .expect("replacement transaction should begin");
        let update_sql =
            "UPDATE stale_journal_replacement_probe SET value = CAST($1 AS JSONB) WHERE path = $2";
        for row_index in 0..ROW_COUNT {
            let result = replacement
                .execute(
                    update_sql,
                    &[
                        Value::Text("{\"state\":\"replacement\"}".to_string()),
                        Value::Text(format!("{row_index:04}")),
                    ],
                )
                .await
                .expect("replacement row should stage");
            assert_eq!(result.rows_affected(), 1);
        }

        concurrent_session
            .execute(
                "INSERT INTO stale_journal_replacement_probe (path, value) \
                 VALUES ('2000', CAST('{\"state\":\"concurrent\"}' AS JSONB))",
                &[],
            )
            .await
            .expect("disjoint insert should commit first");
        replacement
            .commit()
            .await
            .expect("stale disjoint replacement should reconcile");

        let rows = concurrent_session
            .execute(
                "SELECT COUNT(*) AS count FROM stale_journal_replacement_probe",
                &[],
            )
            .await
            .expect("final generation should be readable");
        assert_eq!(
            rows.rows()[0].get::<i64>("count").unwrap(),
            (ROW_COUNT + 1) as i64,
            "the stale complete-set proof must not erase the disjoint insert"
        );
        let concurrent = concurrent_session
            .execute(
                "SELECT value FROM stale_journal_replacement_probe WHERE path = '2000'",
                &[],
            )
            .await
            .expect("concurrent row should remain point-readable");
        assert_eq!(
            concurrent.rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!({"state": "concurrent"})
        );
        let reconciled_created_at = session
            .execute(
                "SELECT lixcol_created_at FROM stale_journal_replacement_probe WHERE path = '0000'",
                &[],
            )
            .await
            .expect("reconciled lifecycle should be readable")
            .rows()[0]
            .get::<String>("lixcol_created_at")
            .unwrap()
            .clone();
        assert_eq!(reconciled_created_at, original_created_at);
    }

    #[tokio::test]
    async fn sequential_complete_update_seals_direct_journal() {
        const ROW_COUNT: usize = 8_193;
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "direct_journal_seal_probe",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("direct journal schema should register");
        let inserts = (0..ROW_COUNT)
            .map(|row_index| ExecuteBatchStatement {
                label: None,
                sql:
                    "INSERT INTO direct_journal_seal_probe (path, value) VALUES ($1, CAST($2 AS JSONB))"
                        .to_string(),
                params: vec![
                    Value::Text(format!("{row_index:04}")),
                    Value::Text("{\"state\":\"base\"}".to_string()),
                ],
            })
            .collect::<Vec<_>>();
        session
            .execute_batch(&inserts)
            .await
            .expect("packed base should seed");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("direct journal transaction should begin");
        let update_sql =
            "UPDATE direct_journal_seal_probe SET value = CAST($1 AS JSONB) WHERE path = $2";
        for row_index in 0..ROW_COUNT {
            let result = transaction
                .execute(
                    update_sql,
                    &[
                        Value::Text("{\"state\":\"replacement\"}".to_string()),
                        Value::Text(format!("{row_index:04}")),
                    ],
                )
                .await
                .expect("direct journal row should stage");
            assert_eq!(result.rows_affected(), 1);
        }
        assert_eq!(
            crate::transaction::take_direct_journal_replacement_publications(
                "direct_journal_seal_probe",
            ),
            0
        );
        let visible = transaction
            .execute(
                "SELECT path, value FROM direct_journal_seal_probe \
                 WHERE path IN ('0000', '4096', '8192') ORDER BY path",
                &[],
            )
            .await
            .expect("point reads must route across immutable journal chunks");
        assert_eq!(visible.len(), 3);
        for (row, expected_path) in visible.rows().iter().zip(["0000", "4096", "8192"]) {
            assert_eq!(row.get::<String>("path").unwrap(), expected_path);
            assert_eq!(
                row.get::<serde_json::Value>("value").unwrap(),
                serde_json::json!({"state": "replacement"})
            );
        }
        transaction
            .commit()
            .await
            .expect("direct journal should commit");
        assert_eq!(
            crate::transaction::take_direct_journal_replacement_publications(
                "direct_journal_seal_probe",
            ),
            1,
            "the complete scalar generation must seal without PreparedStateBatch"
        );

        // Replacement-part bytes are content-addressed without their commit
        // owner. Publishing the same post-image again must bind a fresh
        // physical commit instead of reusing the decoded leaf from above.
        let mut repeated = session
            .begin_transaction()
            .await
            .expect("repeated direct journal transaction should begin");
        for row_index in 0..ROW_COUNT {
            repeated
                .execute(
                    update_sql,
                    &[
                        Value::Text("{\"state\":\"replacement\"}".to_string()),
                        Value::Text(format!("{row_index:04}")),
                    ],
                )
                .await
                .expect("identical repeated journal row should stage");
        }
        repeated
            .commit()
            .await
            .expect("identical replacement generation should commit");
        assert_eq!(
            crate::transaction::take_direct_journal_replacement_publications(
                "direct_journal_seal_probe",
            ),
            1
        );
    }

    #[tokio::test]
    async fn complete_mutation_journal_is_visible_before_commit() {
        const ROW_COUNT: usize = 1_024;
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "journal_read_your_writes_probe",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        session
            .execute_batch(
                &(0..ROW_COUNT)
                    .map(|row_index| ExecuteBatchStatement {
                        label: None,
                        sql: "INSERT INTO journal_read_your_writes_probe (path, value) VALUES ($1, CAST($2 AS JSONB))".to_string(),
                        params: vec![
                            Value::Text(format!("{row_index:04}")),
                            Value::Text("{\"state\":\"base\"}".to_string()),
                        ],
                    })
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();

        let original_created_at = session
            .execute(
                "SELECT lixcol_created_at FROM journal_read_your_writes_probe WHERE path = '0000'",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("lixcol_created_at")
            .unwrap()
            .clone();

        let mut transaction = session.begin_transaction().await.unwrap();
        let update_sql =
            "UPDATE journal_read_your_writes_probe SET value = CAST($1 AS JSONB) WHERE path = $2";
        for row_index in 0..ROW_COUNT {
            transaction
                .execute(
                    update_sql,
                    &[
                        Value::Text("{\"state\":\"replacement\"}".to_string()),
                        Value::Text(format!("{row_index:04}")),
                    ],
                )
                .await
                .unwrap();
        }
        let visible = transaction
            .execute(
                "SELECT path, value, lixcol_created_at FROM journal_read_your_writes_probe \
                 WHERE path IN ('0000', '1023') ORDER BY path",
                &[],
            )
            .await
            .expect("a read barrier must expose the immutable mutation journal");
        assert_eq!(visible.len(), 2);
        for row in visible.rows() {
            assert_eq!(
                row.get::<serde_json::Value>("value").unwrap(),
                serde_json::json!({"state": "replacement"})
            );
            assert_eq!(
                row.get::<String>("lixcol_created_at").unwrap().as_str(),
                original_created_at.as_str()
            );
        }
        transaction.commit().await.unwrap();
        assert_eq!(
            crate::transaction::take_direct_journal_replacement_publications(
                "journal_read_your_writes_probe",
            ),
            1,
            "an intervening read must not reconstruct the immutable journal"
        );
    }

    #[tokio::test]
    async fn mixed_journal_fallback_preserves_created_at() {
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "mixed_journal_lifecycle_probe",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO mixed_journal_lifecycle_probe (path, value) \
                 VALUES ('existing', CAST('{\"state\":\"base\"}' AS JSONB))",
                &[],
            )
            .await
            .unwrap();
        let original_created_at = session
            .execute(
                "SELECT lixcol_created_at FROM mixed_journal_lifecycle_probe WHERE path = 'existing'",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("lixcol_created_at")
            .unwrap()
            .clone();

        let mut transaction = session.begin_transaction().await.unwrap();
        transaction
            .execute(
                "INSERT INTO mixed_journal_lifecycle_probe (path, value) \
                 VALUES ('inserted', CAST('{\"state\":\"inserted\"}' AS JSONB))",
                &[],
            )
            .await
            .unwrap();
        transaction
            .execute(
                "UPDATE mixed_journal_lifecycle_probe SET value = CAST($1 AS JSONB) WHERE path = $2",
                &[
                    Value::Text("{\"state\":\"updated\"}".to_string()),
                    Value::Text("existing".to_string()),
                ],
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let created_at = session
            .execute(
                "SELECT lixcol_created_at FROM mixed_journal_lifecycle_probe WHERE path = 'existing'",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("lixcol_created_at")
            .unwrap()
            .clone();
        assert_eq!(created_at, original_created_at);
    }

    #[tokio::test]
    async fn checkpoint_parent_without_collection_lifecycle_uses_safe_lane() {
        const ROW_COUNT: usize = 1_024;
        let session = open_session().await;
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "rooted_journal_parent_probe",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        session
            .execute_batch(
                &(0..ROW_COUNT)
                    .map(|row_index| ExecuteBatchStatement {
                        label: None,
                        sql: "INSERT INTO rooted_journal_parent_probe (path, value) VALUES ($1, CAST($2 AS JSONB))".to_string(),
                        params: vec![
                            Value::Text(format!("{row_index:04}")),
                            Value::Text("{\"state\":\"base\"}".to_string()),
                        ],
                    })
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        let original_created_at = session
            .execute(
                "SELECT lixcol_created_at FROM rooted_journal_parent_probe WHERE path = '0000'",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("lixcol_created_at")
            .unwrap()
            .clone();
        session.create_checkpoint().await.unwrap();

        crate::transaction::take_direct_journal_replacement_publications(
            "rooted_journal_parent_probe",
        );
        let mut transaction = session.begin_transaction().await.unwrap();
        let update_sql =
            "UPDATE rooted_journal_parent_probe SET value = CAST($1 AS JSONB) WHERE path = $2";
        for row_index in 0..ROW_COUNT {
            transaction
                .execute(
                    update_sql,
                    &[
                        Value::Text("{\"state\":\"replacement\"}".to_string()),
                        Value::Text(format!("{row_index:04}")),
                    ],
                )
                .await
                .unwrap();
        }
        let visible_created_at = transaction
            .execute(
                "SELECT lixcol_created_at FROM rooted_journal_parent_probe WHERE path = '0000'",
                &[],
            )
            .await
            .expect("fallback read must hydrate lifecycle without lowering the journal")
            .rows()[0]
            .get::<String>("lixcol_created_at")
            .unwrap()
            .clone();
        assert_eq!(visible_created_at, original_created_at);
        transaction
            .commit()
            .await
            .expect("a parent without collection lifecycle authority must use the safe lane");
        assert_eq!(
            crate::transaction::take_direct_journal_replacement_publications(
                "rooted_journal_parent_probe",
            ),
            0
        );
        let committed_created_at = session
            .execute(
                "SELECT lixcol_created_at FROM rooted_journal_parent_probe WHERE path = '0000'",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("lixcol_created_at")
            .unwrap()
            .clone();
        assert_eq!(committed_created_at, original_created_at);
    }

    #[tokio::test]
    async fn explicit_transaction_origin_key_survives_addressable_change_assignment() {
        let session = open_session().await;
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('origin-key-address', 'seed')",
                &[],
            )
            .await
            .expect("seed row should commit");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute_with_options(
                "UPDATE lix_key_value SET value = 'updated' \
                 WHERE key = 'origin-key-address'"
                    .to_owned(),
                Vec::new(),
                ExecuteOptions {
                    origin_key: Some("tx-origin".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("stamped update should stage");
        transaction
            .commit()
            .await
            .expect("stamped update should commit");

        let result = session
            .execute(
                "SELECT change.origin_key \
                 FROM lix_key_value AS value \
                 JOIN lix_change AS change ON change.id = value.lixcol_change_id \
                 WHERE value.key = 'origin-key-address'",
                &[],
            )
            .await
            .expect("current change should be readable");
        assert_eq!(
            result.rows()[0]
                .get::<String>("origin_key")
                .expect("origin key should be text"),
            "tx-origin"
        );
    }

    #[tokio::test]
    async fn explicit_file_transaction_origin_key_survives_addressable_change_assignment() {
        const FILE_ID: &str = "01920000-0000-7000-8000-000000000411";
        let session = open_session().await;
        session
            .execute_with_options(
                "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
                &[
                    Value::Text(FILE_ID.to_string()),
                    Value::Text("/origin-key.md".to_string()),
                    Value::Blob(b"one\n".to_vec().into()),
                ],
                ExecuteOptions {
                    origin_key: Some("first-origin".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("seed file should commit");
        session
            .execute(
                "UPDATE lix_file SET content = $1 WHERE id = $2",
                &[
                    Value::Blob(b"two\n".to_vec().into()),
                    Value::Text(FILE_ID.to_string()),
                ],
            )
            .await
            .expect("unstamped file update should commit");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute_with_options(
                "UPDATE lix_file SET content = $1 WHERE id = $2".to_owned(),
                vec![
                    Value::Blob(b"three\n".to_vec().into()),
                    Value::Text(FILE_ID.to_string()),
                ],
                ExecuteOptions {
                    origin_key: Some("tx-origin".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("stamped file update should stage");
        transaction
            .commit()
            .await
            .expect("stamped file update should commit");

        let result = session
            .execute(
                "SELECT change.origin_key \
                 FROM lix_file AS file \
                 JOIN lix_change AS change ON change.id = file.lixcol_change_id \
                 WHERE file.id = $1",
                &[Value::Text(FILE_ID.to_string())],
            )
            .await
            .expect("current file change should be readable");
        assert_eq!(
            result.rows()[0]
                .get::<String>("origin_key")
                .expect("origin key should be text"),
            "tx-origin"
        );
    }

    #[tokio::test]
    async fn reusable_read_plans_rebind_snapshots_concurrently_and_invalidate_on_catalog_change() {
        let storage = Memory::default();
        Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized storage should create engine");
        let session = engine
            .open_session()
            .await
            .expect("first session should open");
        let concurrent = engine
            .open_session()
            .await
            .expect("second session should open");
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "read_plan_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "revision", "type": "int8", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("register reusable-plan schema");
        session
            .execute(
                "INSERT INTO read_plan_probe (id, revision) VALUES ('a', 1), ('b', 2)",
                &[],
            )
            .await
            .expect("seed reusable-plan rows");

        let sql = "SELECT revision FROM read_plan_probe WHERE id = $1 AND revision >= 0";
        let before = session.sql_planning_cache.read_plan_count();
        let first = session
            .execute(sql, &[Value::Text("a".to_string())])
            .await
            .expect("cold reusable plan should execute");
        assert_eq!(first.rows()[0].get::<i64>("revision").unwrap(), 1);
        assert_eq!(session.sql_planning_cache.read_plan_count(), before + 1);

        let left_params = [Value::Text("a".to_string())];
        let right_params = [Value::Text("b".to_string())];
        let (left, right) = tokio::join!(
            session.execute(sql, &left_params),
            concurrent.execute(sql, &right_params),
        );
        assert_eq!(
            left.expect("first concurrent cache hit").rows()[0]
                .get::<i64>("revision")
                .unwrap(),
            1
        );
        assert_eq!(
            right.expect("second concurrent cache hit").rows()[0]
                .get::<i64>("revision")
                .unwrap(),
            2
        );
        assert_eq!(session.sql_planning_cache.read_plan_count(), before + 1);

        session
            .execute(
                "INSERT INTO read_plan_probe (id, revision) VALUES ('c', 3)",
                &[],
            )
            .await
            .expect("commit ordinary data revision");
        let revised = session
            .execute(sql, &[Value::Text("c".to_string())])
            .await
            .expect("cached plan should bind the revised snapshot");
        assert_eq!(revised.rows()[0].get::<i64>("revision").unwrap(), 3);
        assert_eq!(session.sql_planning_cache.read_plan_count(), before + 1);

        let added_schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "read_plan_catalog_revision",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                &[Value::Text(added_schema.to_string())],
            )
            .await
            .expect("change the SQL catalog");
        session
            .execute(sql, &[Value::Text("c".to_string())])
            .await
            .expect("catalog change should compile a new plan");
        assert_eq!(session.sql_planning_cache.read_plan_count(), before + 2);

        let cached = session
            .execute(sql, &[Value::Text("c".to_string())])
            .await
            .expect("cached differential query");
        session.sql_planning_cache.clear_read_plans();
        let uncached = session
            .execute(sql, &[Value::Text("c".to_string())])
            .await
            .expect("uncached DataFusion differential query");
        assert_eq!(
            cached.rows()[0].values(),
            uncached.rows()[0].values(),
            "cached templates must match a fresh DataFusion plan"
        );
    }
}


/// Compile-time proofs for the `AssumeSendFuture` safety obligation.
///
/// `AssumeSendFuture` asserts `Send` unconditionally, so its soundness rests on
/// each call site's wrapped future genuinely holding only `Send` values across
/// its suspension points. Nothing in the type system re-checks that after the
/// wrapper is applied, which makes the obligation silently rot-prone.
///
/// `Memory::Read<'a> = MemoryRead` is lifetime-independent, so instantiating the
/// wrapped futures at `Memory` collapses the higher-ranked obstruction that
/// forces the wrapper in the generic case and lets rustc perform the full
/// auto-trait walk over the entire suspension state. A future change that parks
/// an `Rc`, a `RefCell` borrow guard, or any other `!Send` value across an
/// `.await` on these paths fails to compile here instead of becoming undefined
/// behaviour behind the wrapper.
#[cfg(test)]
mod assume_send_future_proofs {
    use super::*;
    use crate::storage_adapter::Memory;

    fn is_send<T: Send>(_: &T) {}
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    // session/execute.rs -- SessionContext::read_file_content
    #[allow(dead_code)]
    fn read_file_content_inner_is_send(session: &SessionContext<Memory>) {
        is_send(&session.read_file_content_inner(String::new(), None));
    }

    // session/execute.rs -- execute_with_idempotency_and_options_and_metadata
    #[allow(dead_code)]
    fn execute_with_kind_is_send(
        session: &SessionContext<Memory>,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
        metadata: ExecuteStatementMetadata,
    ) {
        is_send(&session.execute_with_kind(sql, params, options, metadata, "execute", None, true));
    }

    // session/execute.rs -- execute_batch_with_idempotency_and_options_and_metadata
    #[allow(dead_code)]
    fn execute_batch_inner_is_send(
        session: &SessionContext<Memory>,
        statements: &[ExecuteBatchStatement],
        options: ExecuteOptions,
        metadata: Vec<ExecuteStatementMetadata>,
    ) {
        is_send(&session.execute_batch_with_options_and_metadata_inner(
            statements, options, metadata, None, true,
        ));
    }

    // session/execute.rs -- SessionTransaction::execute_with_options
    #[allow(dead_code)]
    fn transaction_execute_inner_is_send(
        transaction: &mut SessionTransaction<Memory>,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) {
        is_send(&transaction.execute_with_options_inner(sql, params, options));
    }

    /// The values the wrapped futures retain are `Send`/`Sync` for *every*
    /// legal `StorageImpl`, not just `Memory`. Storage-supplied handles are
    /// covered by the `Storage`/`StorageRead`/`StorageWrite` trait contract
    /// alone, so no adapter can introduce a non-`Send` value into these paths.
    #[allow(dead_code)]
    fn retained_values_are_send_for_every_storage<S>()
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        assert_send::<SessionContext<S>>();
        assert_sync::<SessionContext<S>>();
        assert_send::<SessionTransaction<S>>();
        assert_send::<ExecuteResult>();
        assert_sync::<ExecuteResult>();
        assert_send::<Value>();
        assert_sync::<Value>();
        assert_send::<ExecuteBatchStatement>();
        assert_sync::<ExecuteBatchStatement>();
        assert_send::<ExecuteOptions>();
        assert_send::<ExecuteStatementMetadata>();
        assert_send::<S::Read<'static>>();
        assert_sync::<S::Read<'static>>();
        assert_send::<S::Write<'static>>();
    }

    /// Adapter-independent concrete types that cross suspensions on these paths.
    #[allow(dead_code)]
    fn storage_cursor_types_are_send() {
        assert_send::<crate::storage::ScanCursor<'static>>();
        assert_send::<crate::storage::GetManyResult>();
    }
}


/// Borrowing-adapter half of the `AssumeSendFuture` proofs.
///
/// # What this covers, and the limit
///
/// `assume_send_future_proofs` instantiates the wrapped futures at `Memory`,
/// where `Read<'a> = MemoryRead` is lifetime-independent. That collapses the
/// higher-ranked obstruction and lets rustc walk the *complete* suspension
/// state. The shipping RocksDB adapter is `Read<'a> = RocksDBRead<'a>`, so that
/// proof covers the easy case.
///
/// **A whole-future proof at a lifetime-dependent `Read<'a>` is not achievable,
/// even for a fully concrete adapter.** Measured against `BorrowingStorage`
/// below: five of the six remaining wrapper sites fail with the same
/// `implementation of 'Send' is not general enough` diagnostic that forces the
/// wrapper generically. The borrowing shape *is* the obstruction; making the
/// adapter concrete does not remove it. That is a rustc limitation on
/// higher-ranked auto-trait obligations, not a property of this code.
///
/// So the borrowing case is covered in two pieces instead:
///
/// 1. `read_file_content_inner` — which carries no wrapper since its
///    `with_static_session_sql_read` bound was relaxed — is proven `Send`
///    against the borrowing adapter directly, and generically for every
///    `StorageImpl` by its own `+ Send` signature.
/// 2. Every type rustc names in the remaining obstructions is proven `Sync`
///    below, **universally quantified over the lifetime**. `&'x T: Send` holds
///    exactly when `T: Sync`, so these discharge the named obligations for all
///    lifetimes; rustc simply cannot assemble them inside a generator. Combined
///    with the `Memory` walk — which enumerates the complete suspension state,
///    and finds no `Rc`, `RefCell` guard, or raw pointer anywhere — this is the
///    tightest statement the type system supports.
#[cfg(test)]
mod assume_send_future_proofs_borrowing {
    use super::*;
    use crate::session::borrowing_proof_storage::{BorrowingRead, BorrowingStorage};

    fn is_send<T: Send>(_: &T) {}
    fn assert_send<T: Send + ?Sized>() {}
    fn assert_sync<T: Sync + ?Sized>() {}

    /// Whole-future proof, borrowing adapter. This site has no wrapper.
    #[allow(dead_code)]
    fn read_file_content_inner_is_send(session: &SessionContext<BorrowingStorage>) {
        is_send(&session.read_file_content_inner(String::new(), None));
    }

    /// The shared read wrapper is `Send + Sync` for every storage adapter and
    /// **every lifetime** — `'a` is a free parameter here, so this is a genuine
    /// `for<'a>` proof of the obligation rustc reports as "not general enough".
    #[allow(dead_code)]
    fn shared_read_is_send_for_every_storage_and_lifetime<'a, S>()
    where
        S: Storage + Clone + Send + Sync + 'a,
    {
        assert_send::<SharedStorageAdapterRead<S::Read<'a>>>();
        assert_sync::<SharedStorageAdapterRead<S::Read<'a>>>();
        assert_send::<S::Read<'a>>();
        assert_sync::<S::Read<'a>>();
    }

    /// The same, pinned at the concrete borrowing adapter.
    #[allow(dead_code)]
    fn shared_read_is_send_for_borrowing_adapter<'a>() {
        assert_send::<SharedStorageAdapterRead<BorrowingRead<'a>>>();
        assert_sync::<SharedStorageAdapterRead<BorrowingRead<'a>>>();
    }

    /// Every remaining type named by a "not general enough" obstruction, proven
    /// `Sync` — which is exactly `for<'x> &'x T: Send`.
    #[allow(dead_code)]
    fn obstruction_pointees_are_sync<S>()
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        assert_sync::<str>();
        assert_sync::<[Value]>();
        assert_sync::<[ExecuteBatchStatement]>();
        assert_sync::<SessionContext<S>>();
        assert_sync::<crate::Lix<S>>();
        assert_sync::<crate::storage_adapter::Memory>();
        assert_sync::<tokio::sync::Mutex<()>>();
    }
}
