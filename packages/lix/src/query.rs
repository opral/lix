//! The SQL surface shared by [`crate::Lix`], [`Snapshot`] and transactions.
//!
//! One obvious way per job:
//! - small reads: [`Query::all`], [`Query::one`], [`Query::optional`]: buffered
//!   and capped at 64 MiB of result memory by default;
//! - large reads: [`Query::stream`]: bounded memory, no cap, never expires;
//! - writes: `execute(sql)` and `execute_many(sql, param_sets)`;
//! - several reads at one state: [`crate::Lix::snapshot`].
//!
//! [`Query`] and [`Execute`] own their SQL, parameters and target, so they are
//! `Send + 'static` and can be moved into spawned tasks.

use std::future::{Future, IntoFuture};
use std::sync::Arc;

use crate::common::{DEFAULT_MAX_RESULT_BYTES, ReadLimits, not_unique};
use crate::session::{ExecuteOptions, PageLimits};
use crate::{ExecuteResult, LixError, QueryResult, Row, RowStream, Value};

pub(crate) type SqlFuture<T> = crate::sync::SyncTransportFuture<'static, T>;

/// One read request.
pub(crate) struct QueryRequest {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
    pub(crate) limits: ReadLimits,
    pub(crate) page_limits: PageLimits,
}

/// One write request.
pub(crate) struct ExecuteRequest {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
    pub(crate) options: ExecuteOptions,
    pub(crate) max_bytes: usize,
    pub(crate) unique_columns: bool,
}

/// One statement with many parameter sets.
pub(crate) struct ExecuteManyRequest {
    pub(crate) sql: String,
    pub(crate) param_sets: Vec<Vec<Value>>,
    pub(crate) options: ExecuteOptions,
}

/// Something SQL can run against: the latest state of a `Lix`, a pinned
/// snapshot, or a transaction.
pub(crate) trait SqlTarget: crate::sync::SyncTransportBounds + 'static {
    fn query(self: Arc<Self>, request: QueryRequest) -> SqlFuture<QueryResult>;
    fn stream(self: Arc<Self>, request: QueryRequest) -> SqlFuture<RowStream>;
    fn execute(self: Arc<Self>, request: ExecuteRequest) -> SqlFuture<ExecuteResult>;
    fn execute_many(self: Arc<Self>, request: ExecuteManyRequest) -> SqlFuture<u64>;
}

pub(crate) fn unsupported_remote_operation(operation: &str) -> LixError {
    LixError::new(
        "LIX_UNSUPPORTED_REMOTE_OPERATION",
        format!("{operation} is not supported on a remote Lix handle"),
    )
    .with_details(serde_json::json!({ "operation": operation }))
    .with_hint("Use query() for reads on a remote handle, or open the repository locally.")
}

/// A read statement waiting to run. Created by `query(sql)` on a
/// [`crate::Lix`], a [`Snapshot`] or a transaction.
///
/// A `Query` does nothing until one of [`Self::all`], [`Self::one`],
/// [`Self::optional`] or [`Self::stream`] is awaited. It never writes:
/// statements that write fail with `LIX_ERROR_READ_ONLY`; use `execute`.
/// Reads that call `uuidv7()` or `current_timestamp` are reads.
#[must_use = "a Query does nothing until all(), one(), optional() or stream() is awaited"]
pub struct Query {
    target: Arc<dyn SqlTarget>,
    sql: String,
    params: Vec<Value>,
    max_bytes: usize,
    page_limits: PageLimits,
}

impl std::fmt::Debug for Query {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Query")
            .field("sql", &self.sql)
            .field("params", &self.params)
            .field("max_bytes", &self.max_bytes)
            .finish_non_exhaustive()
    }
}

impl Query {
    pub(crate) fn new(target: Arc<dyn SqlTarget>, sql: &str) -> Self {
        Self {
            target,
            sql: sql.to_owned(),
            params: Vec::new(),
            max_bytes: DEFAULT_MAX_RESULT_BYTES,
            page_limits: PageLimits::default(),
        }
    }

    /// Binds the next positional parameter (`$1`, `$2`, ...).
    pub fn bind(mut self, value: impl Into<Value>) -> Self {
        self.params.push(value.into());
        self
    }

    /// Binds several positional parameters after those already bound.
    pub fn params<I>(mut self, values: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<Value>,
    {
        self.params.extend(values.into_iter().map(Into::into));
        self
    }

    /// Caps the bytes [`Self::all`] may buffer (row and value memory).
    /// Defaults to 64 MiB; `usize::MAX` opts out. A larger result fails with
    /// `LIX_ERROR_RESULT_TOO_LARGE` and returns nothing; it is never truncated.
    /// [`Self::stream`] applies it only to statements it has to buffer;
    /// [`Self::one`] and [`Self::optional`] read at most one row and ignore it.
    pub fn max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    #[cfg(test)]
    pub(crate) fn page_limits(mut self, page_limits: PageLimits) -> Self {
        self.page_limits = page_limits;
        self
    }

    fn request(self, limits: ReadLimits) -> (Arc<dyn SqlTarget>, QueryRequest) {
        (
            self.target,
            QueryRequest {
                sql: self.sql,
                params: self.params,
                limits,
                page_limits: self.page_limits,
            },
        )
    }

    /// Runs the read and buffers every row, under the [`Self::max_bytes`] cap.
    pub async fn all(self) -> Result<QueryResult, LixError> {
        let limits = ReadLimits::bytes(self.max_bytes);
        let (target, request) = self.request(limits);
        target.query(request).await
    }

    /// Returns the only row. Fails with `LIX_ERROR_NO_ROWS` when there is none and
    /// `LIX_ERROR_NOT_UNIQUE` when there are more. Not byte-capped.
    pub async fn one(self) -> Result<Row, LixError> {
        self.optional().await?.ok_or_else(|| {
            LixError::new(LixError::CODE_NO_ROWS, "query returned no rows")
                .with_hint("Use optional() when the row may be absent.")
        })
    }

    /// Returns the row, or `None` when there is none. Fails with
    /// `LIX_ERROR_NOT_UNIQUE` when there is more than one. Not byte-capped.
    pub async fn optional(self) -> Result<Option<Row>, LixError> {
        let (target, request) = self.request(ReadLimits::single_row());
        let mut rows = target.query(request).await?.into_rows();
        if rows.len() > 1 {
            return Err(not_unique());
        }
        Ok(rows.pop())
    }

    /// Opens a stream over the result. Resolves once the statement is
    /// planned and its first page (at most 64 rows) has arrived, so planning
    /// errors fail here. No size cap and no deadline.
    ///
    /// Most reads stream with bounded memory. A few statement shapes cannot
    /// stream incrementally and are buffered in full before the first page:
    /// reads of `lix_file.content` (and other plugin-rendered file bytes),
    /// reads inside a transaction, and reads on a partial replica. Those
    /// buffers are capped by [`Self::max_bytes`] (64 MiB by default) and fail
    /// with `LIX_ERROR_RESULT_TOO_LARGE` beyond it.
    pub async fn stream(self) -> Result<RowStream, LixError> {
        let limits = ReadLimits::bytes(self.max_bytes);
        let (target, request) = self.request(limits);
        target.stream(request).await
    }
}

/// A statement waiting to run: `execute(sql)` on a [`crate::Lix`] or a
/// transaction. Await it to run it.
///
/// `execute` runs any single statement, writes and reads alike. Its rows (a
/// read's result or `RETURNING` rows) are buffered under the same 64 MiB cap
/// as `query`; a write whose `RETURNING` rows exceed it fails without
/// committing.
#[must_use = "an Execute does nothing until it is awaited"]
pub struct Execute {
    target: Arc<dyn SqlTarget>,
    request: ExecuteRequest,
}

impl std::fmt::Debug for Execute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Execute")
            .field("sql", &self.request.sql)
            .field("params", &self.request.params)
            .finish_non_exhaustive()
    }
}

impl Execute {
    pub(crate) fn new(target: Arc<dyn SqlTarget>, sql: &str) -> Self {
        Self {
            target,
            request: ExecuteRequest {
                sql: sql.to_owned(),
                params: Vec::new(),
                options: ExecuteOptions::default(),
                max_bytes: DEFAULT_MAX_RESULT_BYTES,
                unique_columns: false,
            },
        }
    }

    /// Binds the next positional parameter (`$1`, `$2`, ...).
    pub fn bind(mut self, value: impl Into<Value>) -> Self {
        self.request.params.push(value.into());
        self
    }

    /// Binds several positional parameters after those already bound.
    pub fn params<I>(mut self, values: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<Value>,
    {
        self.request
            .params
            .extend(values.into_iter().map(Into::into));
        self
    }

    /// Caps the memory of the buffered result rows (`RETURNING` or a read's
    /// rows). Defaults to 64 MiB; `usize::MAX` opts out.
    pub fn max_bytes(mut self, max_bytes: usize) -> Self {
        self.request.max_bytes = max_bytes;
        self
    }

    /// Identifies the caller-defined origin of this write.
    pub fn with_origin_key(mut self, origin_key: impl Into<String>) -> Self {
        self.request.options.origin_key = Some(origin_key.into());
        self
    }

    /// Rejects a write whose `RETURNING` repeats a column name
    /// (`RETURNING OLD.value, NEW.value`) with LIX_ERROR_AMBIGUOUS_COLUMN
    /// before it runs, for callers that key rows by name. By default such
    /// rows are returned and read by position.
    pub fn unique_column_names(mut self) -> Self {
        self.request.unique_columns = true;
        self
    }

    /// Caps whole automatic-transaction replays. Zero fails on the first
    /// failed attempt. Without an override, Lix keeps its default conflict
    /// and snapshot-recovery budgets. Not available inside a transaction,
    /// which never replays.
    pub fn with_max_auto_commit_retries(mut self, retries: u32) -> Self {
        self.request.options.max_auto_commit_retries = Some(retries);
        self
    }
}

impl IntoFuture for Execute {
    type Output = Result<ExecuteResult, LixError>;
    type IntoFuture = SqlFuture<ExecuteResult>;

    fn into_future(self) -> Self::IntoFuture {
        self.target.execute(self.request)
    }
}

/// One write statement run once per parameter set, atomically, in one call:
/// `execute_many(sql, param_sets)`. Resolves to the total rows affected.
#[must_use = "an ExecuteMany does nothing until it is awaited"]
pub struct ExecuteMany {
    target: Arc<dyn SqlTarget>,
    request: ExecuteManyRequest,
}

impl std::fmt::Debug for ExecuteMany {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecuteMany")
            .field("sql", &self.request.sql)
            .field("param_sets", &self.request.param_sets.len())
            .finish_non_exhaustive()
    }
}

impl ExecuteMany {
    pub(crate) fn new<I, P>(target: Arc<dyn SqlTarget>, sql: &str, param_sets: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: IntoIterator,
        P::Item: Into<Value>,
    {
        Self {
            target,
            request: ExecuteManyRequest {
                sql: sql.to_owned(),
                param_sets: param_sets
                    .into_iter()
                    .map(|params| params.into_iter().map(Into::into).collect())
                    .collect(),
                options: ExecuteOptions::default(),
            },
        }
    }

    /// Identifies the caller-defined origin of these writes.
    pub fn with_origin_key(mut self, origin_key: impl Into<String>) -> Self {
        self.request.options.origin_key = Some(origin_key.into());
        self
    }

    /// Caps whole automatic-transaction replays; see
    /// [`Execute::with_max_auto_commit_retries`].
    pub fn with_max_auto_commit_retries(mut self, retries: u32) -> Self {
        self.request.options.max_auto_commit_retries = Some(retries);
        self
    }
}

impl IntoFuture for ExecuteMany {
    type Output = Result<u64, LixError>;
    type IntoFuture = SqlFuture<u64>;

    fn into_future(self) -> Self::IntoFuture {
        self.target.execute_many(self.request)
    }
}

/// The parts of a snapshot beyond running SQL.
pub(crate) trait SnapshotTarget: SqlTarget {
    fn active_branch_id(&self) -> &str;
    fn commit_id(&self) -> &str;
    fn storage_mutation_revision(&self) -> Option<&[u8]>;
    fn close(self: Arc<Self>) -> SqlFuture<()>;
}

/// One pinned repository state: every read and stream through it sees the
/// same state, whatever commits meanwhile. Opened by [`crate::Lix::snapshot`].
///
/// Clones share the state. Any number of streams may be open at once, and a
/// stream keeps reading after the handle that opened it is dropped. The state
/// is released by [`Self::close`] (for every clone) or once the last handle
/// and stream are dropped. Snapshots never expire.
#[derive(Clone)]
pub struct Snapshot {
    inner: Arc<dyn SnapshotTarget>,
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Snapshot")
            .field("active_branch_id", &self.inner.active_branch_id())
            .field("commit_id", &self.inner.commit_id())
            .finish_non_exhaustive()
    }
}

impl Snapshot {
    pub(crate) fn new(inner: Arc<dyn SnapshotTarget>) -> Self {
        Self { inner }
    }

    /// Starts a read at this snapshot's state.
    pub fn query(&self, sql: &str) -> Query {
        let target: Arc<dyn SqlTarget> = self.inner.clone();
        Query::new(target, sql)
    }

    /// The branch the snapshot reads.
    pub fn active_branch_id(&self) -> &str {
        self.inner.active_branch_id()
    }

    /// The head commit of [`Self::active_branch_id`] at the snapshot.
    pub fn commit_id(&self) -> &str {
        self.inner.commit_id()
    }

    /// Storage-level revision used by storage adapters to detect change.
    #[doc(hidden)]
    pub fn storage_mutation_revision(&self) -> Option<&[u8]> {
        self.inner.storage_mutation_revision()
    }

    /// Closes the snapshot for every clone: its streams stop and later reads
    /// fail with `LIX_ERROR_SNAPSHOT_CLOSED`. Resolves once reads that were already
    /// running have finished and the pinned state is released. Idempotent.
    pub async fn close(&self) -> Result<(), LixError> {
        self.inner.clone().close().await
    }
}

/// Boxes a session future for a [`SqlTarget`].
///
/// # Safety
///
/// As for [`crate::session::AssumeSendFuture`]: every value retained across
/// suspension must be `Send`.
pub(crate) unsafe fn boxed<T: 'static>(
    future: impl Future<Output = Result<T, LixError>> + 'static,
) -> SqlFuture<T>
where
    T: Send,
{
    // SAFETY: forwarded to the caller.
    let future = unsafe { crate::session::AssumeSendFuture::new(future) };
    Box::pin(future)
}
