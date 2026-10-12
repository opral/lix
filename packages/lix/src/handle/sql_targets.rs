//! [`crate::query::SqlTarget`] for the public handles: the latest state of a
//! [`Lix`], a pinned [`crate::Snapshot`], a [`LixTransaction`] and a
//! [`RemoteLix`].

use std::sync::Arc;

use crate::common::{ReadLimits, with_read_limits};
use crate::query::{
    ExecuteManyRequest, ExecuteRequest, QueryRequest, SnapshotTarget, SqlFuture, SqlTarget, boxed,
    unsupported_remote_operation,
};
use crate::session::{AssumeSendFuture, ExecuteOptions, ReadSnapshot};
use crate::{
    ExecuteBatchStatement, ExecuteResult, ExecutionDisposition, LixError, QueryResult, RowStream,
    Value,
};

use super::{
    Lix, LixTransaction, RemoteLix, Storage, StorageSession, TransactionShared,
    closed_transaction_error,
};

/// A stream that has to buffer its statement reports the cap in stream terms.
pub(crate) fn buffered_stream_error(error: LixError) -> LixError {
    if error.code != LixError::CODE_RESULT_TOO_LARGE {
        return error;
    }
    let mut error = error;
    error.message = format!(
        "{} (this statement cannot stream incrementally, so it is buffered before its \
         first page)",
        error.message
    );
    error.with_hint(
        "File content, transaction and partial-replica reads are buffered. Raise the buffer \
         with .max_bytes(n) before .stream() (JS: { maxBytes }), or select fewer rows.",
    )
}

fn retries_in_transaction() -> LixError {
    LixError::new(
        LixError::CODE_INVALID_PARAM,
        "with_max_auto_commit_retries does not apply inside a transaction",
    )
    .with_hint("Transactions never replay; retry the whole transaction instead.")
}

fn remote_write(request: &ExecuteRequest) -> Result<(), LixError> {
    if request.max_bytes != crate::common::DEFAULT_MAX_RESULT_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "max_bytes is not supported for remote execute()",
        )
        .with_hint(
            "The server caps RETURNING rows at its default before it commits; a client-side \
             check would only fail after the write was published.",
        ));
    }
    Ok(())
}

fn snapshot_read_only() -> LixError {
    LixError::new(LixError::CODE_READ_ONLY, "a snapshot only serves reads")
        .with_hint("Write through the Lix handle or a transaction.")
}

/// Applies a byte cap to a result the engine did not collect under it, such
/// as an authority read over HTTP.
fn enforce_limits(result: &ExecuteResult, limits: ReadLimits) -> Result<(), LixError> {
    crate::session::charge_execute_result(&mut crate::common::ReadResultBudget::new(limits), result)
}

impl<StorageImpl> Lix<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) fn sql_target(&self) -> Arc<dyn SqlTarget> {
        Arc::new(self.clone())
    }

    /// Starts a read of the latest committed state. Each call reads the state
    /// current when it runs; use [`Self::snapshot`] for several reads at one
    /// state.
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), lix::LixError> {
    /// let lix = lix::open_lix().await?;
    /// let rows = lix
    ///     .query("SELECT key, value FROM lix_key_value WHERE key = $1")
    ///     .bind("theme")
    ///     .all()
    ///     .await?;
    /// for row in &rows {
    ///     println!("{}", row.get::<String>("key")?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query(&self, sql: &str) -> crate::Query {
        crate::Query::new(self.sql_target(), sql)
    }

    /// Pins the current state of the active branch. Every read and stream
    /// through the snapshot sees that state, and streams may be open
    /// concurrently. Snapshots never expire; close or drop them to release
    /// the state.
    pub async fn snapshot(&self) -> Result<crate::Snapshot, LixError> {
        // Opening reads only the branch head and storage revision, so a
        // partial replica can restart it after fetching missing inputs.
        let snapshot = self
            .retry_sync_demands(|| {
                let session = Arc::clone(&self.session);
                let sync_demand_tx = self.sync_demand_tx.clone();
                // SAFETY: the future owns its session Arc and sender; as for
                // buffered reads, the obstruction is the higher-ranked storage
                // read borrow.
                unsafe {
                    AssumeSendFuture::new(async move {
                        session.open_read_snapshot(sync_demand_tx).await
                    })
                }
            })
            .await?;
        Ok(crate::Snapshot::new(Arc::new(LixSnapshot { snapshot })))
    }

    /// The buffered read path behind [`crate::Query::all`]: local first, with
    /// replica retries and the partial-replica authority fallback.
    async fn read_latest(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.session.parse_read_only_statement(sql, params)?;
        if !matches!(
            self.engine.sync_mode().role(),
            crate::sync::SyncRole::Replica | crate::sync::SyncRole::PartialReplica
        ) {
            return self
                .session
                .execute_with_options(sql, params, ExecuteOptions::default())
                .await;
        }
        let authority_fallback = self.engine.sync_mode().role()
            == crate::sync::SyncRole::PartialReplica
            && self.server.is_some();
        let local = self
            .retry_replica_read(ExecutionDisposition::CancellableRead, || {
                self.retry_sync_demands(|| {
                    self.session
                        .execute_with_options(sql, params, ExecuteOptions::default())
                })
            })
            .await;
        match local {
            Err(error)
                if authority_fallback
                    && error.code == LixError::CODE_PARTIAL_REPLICA_SCOPE_UNSUPPORTED =>
            {
                let limits = crate::common::current_read_limits();
                let result = self
                    .execute_authority_read(sql, params, &ExecuteOptions::default())
                    .await?;
                enforce_limits(&result, limits)?;
                Ok(result)
            }
            result => result,
        }
    }

    async fn stream_latest(&self, request: QueryRequest) -> Result<RowStream, LixError> {
        let QueryRequest {
            sql,
            params,
            page_limits,
            limits,
        } = request;
        let statement = self.session.parse_read_only_statement(&sql, &params)?;
        // A partial replica fetches missing inputs by restarting the whole
        // statement after hydration, and may answer from its authority;
        // neither can resume a stream that already handed out rows. Such
        // reads, like those that need their whole result first, are read
        // buffered (default byte cap) and paged.
        if self.session.query_stream_route(&statement, &params)
            == crate::session::QueryStreamRoute::Materialized
            || self.engine.sync_mode().role() == crate::sync::SyncRole::PartialReplica
        {
            let lix = self.clone();
            // SAFETY: the future owns a Lix clone and its request values, like
            // the future `query().all()` returns.
            let result = unsafe {
                AssumeSendFuture::new(async move {
                    with_read_limits(limits, lix.read_latest(&sql, &params))
                        .await
                        .map_err(buffered_stream_error)
                })
            };
            return self
                .session
                .open_materialized_query_stream(result, page_limits, None, || Ok(()))
                .await;
        }
        // Opening pulls the first page, so a sync demand before any row was
        // handed out restarts the whole stream safely.
        self.retry_sync_demands(|| {
            self.session.open_live_query_stream(
                sql.clone(),
                statement.clone(),
                params.clone(),
                page_limits,
            )
        })
        .await
    }

    async fn execute_latest(&self, request: ExecuteRequest) -> Result<ExecuteResult, LixError> {
        let ExecuteRequest {
            sql,
            params,
            options,
            ..
        } = request;
        if !matches!(
            self.engine.sync_mode().role(),
            crate::sync::SyncRole::Replica | crate::sync::SyncRole::PartialReplica
        ) {
            return self
                .session
                .execute_with_options(&sql, &params, options)
                .await;
        }
        self.retry_sync_demands(|| {
            self.session
                .execute_with_options(&sql, &params, options.clone())
        })
        .await
    }

    async fn execute_many_latest(&self, request: ExecuteManyRequest) -> Result<u64, LixError> {
        let ExecuteManyRequest {
            sql,
            param_sets,
            options,
        } = request;
        self.session.check_execute_many_statement(&sql)?;
        if param_sets.is_empty() {
            return Ok(0);
        }
        let statements = param_sets
            .into_iter()
            .map(|params| ExecuteBatchStatement {
                sql: sql.clone(),
                params,
                label: None,
            })
            .collect::<Vec<_>>();
        let results = if matches!(
            self.engine.sync_mode().role(),
            crate::sync::SyncRole::Replica | crate::sync::SyncRole::PartialReplica
        ) {
            self.retry_sync_demands(|| {
                self.session
                    .execute_batch_with_options(&statements, options.clone())
            })
            .await?
        } else {
            self.session
                .execute_batch_with_options(&statements, options)
                .await?
        };
        Ok(results.iter().map(ExecuteResult::rows_affected).sum())
    }
}

impl<StorageImpl> SqlTarget for Lix<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    fn query(self: Arc<Self>, request: QueryRequest) -> SqlFuture<QueryResult> {
        // SAFETY: the future owns the handle Arc and the request.
        unsafe {
            boxed(async move {
                with_read_limits(
                    request.limits,
                    self.read_latest(&request.sql, &request.params),
                )
                .await
                .map(QueryResult::from_execute_result)
            })
        }
    }

    fn stream(self: Arc<Self>, request: QueryRequest) -> SqlFuture<RowStream> {
        // SAFETY: the future owns the handle Arc and the request.
        unsafe { boxed(async move { self.stream_latest(request).await }) }
    }

    fn execute(self: Arc<Self>, request: ExecuteRequest) -> SqlFuture<ExecuteResult> {
        let limits = ReadLimits::bytes(request.max_bytes);
        // SAFETY: the future owns the handle Arc and the request.
        unsafe {
            boxed(async move { with_read_limits(limits, self.execute_latest(request)).await })
        }
    }

    fn execute_many(self: Arc<Self>, request: ExecuteManyRequest) -> SqlFuture<u64> {
        // SAFETY: the future owns the handle Arc and the request.
        unsafe { boxed(async move { self.execute_many_latest(request).await }) }
    }
}

/// A snapshot opened by [`Lix::snapshot`].
struct LixSnapshot<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    snapshot: Arc<ReadSnapshot<StorageSession<StorageImpl>>>,
}

impl<StorageImpl> SqlTarget for LixSnapshot<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    fn query(self: Arc<Self>, request: QueryRequest) -> SqlFuture<QueryResult> {
        // SAFETY: the future owns the snapshot Arc and the request.
        unsafe {
            boxed(async move {
                with_read_limits(
                    request.limits,
                    self.snapshot.query(&request.sql, &request.params),
                )
                .await
                .map(QueryResult::from_execute_result)
            })
        }
    }

    fn stream(self: Arc<Self>, request: QueryRequest) -> SqlFuture<RowStream> {
        // SAFETY: the future owns the snapshot Arc and the request.
        unsafe {
            boxed(async move {
                self.snapshot
                    .stream(
                        &request.sql,
                        &request.params,
                        request.page_limits,
                        request.limits,
                    )
                    .await
            })
        }
    }

    fn execute(self: Arc<Self>, _request: ExecuteRequest) -> SqlFuture<ExecuteResult> {
        Box::pin(std::future::ready(Err(snapshot_read_only())))
    }

    fn execute_many(self: Arc<Self>, _request: ExecuteManyRequest) -> SqlFuture<u64> {
        Box::pin(std::future::ready(Err(snapshot_read_only())))
    }
}

impl<StorageImpl> SnapshotTarget for LixSnapshot<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    fn active_branch_id(&self) -> &str {
        self.snapshot.active_branch_id()
    }

    fn commit_id(&self) -> &str {
        self.snapshot.commit_id()
    }

    fn storage_mutation_revision(&self) -> Option<&[u8]> {
        self.snapshot.storage_mutation_revision()
    }

    fn close(self: Arc<Self>) -> SqlFuture<()> {
        // SAFETY: the future owns the snapshot Arc.
        unsafe {
            boxed(async move {
                self.snapshot.close().await;
                Ok(())
            })
        }
    }
}

impl<StorageImpl> TransactionShared<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    async fn read(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        let mut inner = self.inner.lock().await;
        let transaction = inner.as_mut().ok_or_else(closed_transaction_error)?;
        transaction.parse_read_only_statement(sql, params)?;
        transaction
            .execute_with_options(sql.to_owned(), params.to_vec(), ExecuteOptions::default())
            .await
    }

    async fn stream_transaction(&self, request: QueryRequest) -> Result<RowStream, LixError> {
        let mut inner = self.inner.lock().await;
        let transaction = inner.as_mut().ok_or_else(closed_transaction_error)?;
        transaction.parse_read_only_statement(&request.sql, &request.params)?;
        let result = with_read_limits(
            request.limits,
            transaction.execute_with_options(
                request.sql,
                request.params,
                ExecuteOptions::default(),
            ),
        )
        .await
        .map_err(buffered_stream_error)?;
        transaction
            .open_materialized_stream(result, request.page_limits)
            .await
    }

    async fn write(&self, request: ExecuteRequest) -> Result<ExecuteResult, LixError> {
        if request.options.max_auto_commit_retries.is_some() {
            return Err(retries_in_transaction());
        }
        let mut inner = self.inner.lock().await;
        let transaction = inner.as_mut().ok_or_else(closed_transaction_error)?;
        transaction
            .execute_with_returning_cap(request.sql, request.params, request.options)
            .await
    }

    async fn write_many(&self, request: ExecuteManyRequest) -> Result<u64, LixError> {
        if request.options.max_auto_commit_retries.is_some() {
            return Err(retries_in_transaction());
        }
        let mut inner = self.inner.lock().await;
        let transaction = inner.as_mut().ok_or_else(closed_transaction_error)?;
        transaction
            .execute_many_atomically(&request.sql, request.param_sets, request.options)
            .await
    }
}

impl<StorageImpl> SqlTarget for TransactionShared<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    fn query(self: Arc<Self>, request: QueryRequest) -> SqlFuture<QueryResult> {
        // SAFETY: the future owns the transaction Arc and the request.
        unsafe {
            boxed(async move {
                with_read_limits(request.limits, self.read(&request.sql, &request.params))
                    .await
                    .map(QueryResult::from_execute_result)
            })
        }
    }

    fn stream(self: Arc<Self>, request: QueryRequest) -> SqlFuture<RowStream> {
        // SAFETY: the future owns the transaction Arc and the request.
        unsafe { boxed(async move { self.stream_transaction(request).await }) }
    }

    fn execute(self: Arc<Self>, request: ExecuteRequest) -> SqlFuture<ExecuteResult> {
        let limits = ReadLimits::bytes(request.max_bytes);
        // SAFETY: the future owns the transaction Arc and the request.
        unsafe { boxed(async move { with_read_limits(limits, self.write(request)).await }) }
    }

    fn execute_many(self: Arc<Self>, request: ExecuteManyRequest) -> SqlFuture<u64> {
        // SAFETY: the future owns the transaction Arc and the request.
        unsafe { boxed(async move { self.write_many(request).await }) }
    }
}

impl<StorageImpl> LixTransaction<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) fn sql_target(&self) -> Arc<dyn SqlTarget> {
        self.shared.clone()
    }
}

impl SqlTarget for RemoteLix {
    fn query(self: Arc<Self>, request: QueryRequest) -> SqlFuture<QueryResult> {
        Box::pin(async move {
            self.client
                .query(&request.sql, &request.params, Some(request.limits.max_bytes))
                .await
        })
    }

    fn stream(self: Arc<Self>, _request: QueryRequest) -> SqlFuture<RowStream> {
        Box::pin(std::future::ready(Err(unsupported_remote_operation(
            "stream()",
        ))))
    }

    fn execute(self: Arc<Self>, request: ExecuteRequest) -> SqlFuture<ExecuteResult> {
        Box::pin(async move {
            remote_write(&request)?;
            self.client
                .execute(
                    &request.sql,
                    &request.params,
                    Some(protocol_options(request.options)),
                )
                .await
        })
    }

    fn execute_many(self: Arc<Self>, request: ExecuteManyRequest) -> SqlFuture<u64> {
        Box::pin(async move {
            self.client
                .execute_many(
                    &request.sql,
                    request.param_sets,
                    Some(protocol_options(request.options)),
                )
                .await
        })
    }
}

fn protocol_options(options: ExecuteOptions) -> crate::authority_client::ProtocolExecuteOptions {
    crate::authority_client::ProtocolExecuteOptions {
        origin_key: options.origin_key,
        max_auto_commit_retries: options.max_auto_commit_retries,
        ..Default::default()
    }
}

/// SQL inside a [`super::RemoteLixTransaction`].
pub(super) struct RemoteTransactionTarget(
    pub(super) Option<Arc<crate::authority_client::ProtocolTransaction<crate::sync::AuthorityHttp>>>,
);

impl RemoteTransactionTarget {
    fn transaction(
        &self,
    ) -> Result<&crate::authority_client::ProtocolTransaction<crate::sync::AuthorityHttp>, LixError>
    {
        self.0.as_deref().ok_or_else(closed_transaction_error)
    }
}

impl SqlTarget for RemoteTransactionTarget {
    fn query(self: Arc<Self>, request: QueryRequest) -> SqlFuture<QueryResult> {
        Box::pin(async move {
            self.transaction()?
                .query(&request.sql, &request.params, Some(request.limits.max_bytes))
                .await
        })
    }

    fn stream(self: Arc<Self>, _request: QueryRequest) -> SqlFuture<RowStream> {
        Box::pin(std::future::ready(Err(unsupported_remote_operation(
            "stream()",
        ))))
    }

    fn execute(self: Arc<Self>, request: ExecuteRequest) -> SqlFuture<ExecuteResult> {
        Box::pin(async move {
            if request.options.max_auto_commit_retries.is_some() {
                return Err(retries_in_transaction());
            }
            remote_write(&request)?;
            self.transaction()?
                .execute(
                    &request.sql,
                    &request.params,
                    Some(protocol_options(request.options)),
                )
                .await
        })
    }

    fn execute_many(self: Arc<Self>, request: ExecuteManyRequest) -> SqlFuture<u64> {
        Box::pin(async move {
            if request.options.max_auto_commit_retries.is_some() {
                return Err(retries_in_transaction());
            }
            self.transaction()?
                .execute_many(
                    &request.sql,
                    request.param_sets,
                    Some(protocol_options(request.options)),
                )
                .await
        })
    }
}

impl RemoteLix {
    /// Starts a read on the server. Remote handles support buffered reads;
    /// `stream()` fails with `LIX_UNSUPPORTED_REMOTE_OPERATION`.
    pub fn query(&self, sql: &str) -> crate::Query {
        crate::Query::new(Arc::new(self.clone()), sql)
    }

    /// Snapshots are not available on remote handles.
    pub async fn snapshot(&self) -> Result<crate::Snapshot, LixError> {
        Err(unsupported_remote_operation("snapshot()"))
    }
}
