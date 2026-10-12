//! Required operations shared by the host bindings. This is an internal adapter
//! contract, not another engine API: local and connected sync use `Lix`, and
//! remote uses the canonical Rust protocol client.

use lix::storage::Storage;
use lix::{
    CreateBranchOptions, CreateBranchReceipt, ExecuteResult, Lix, LixError, MergeBranchOptions,
    MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt, SwitchBranchOptions,
    SwitchBranchReceipt, Value,
};

#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecuteOptions {
    pub(crate) origin_key: Option<String>,
    pub(crate) max_auto_commit_retries: Option<u32>,
    // Transport retry identity is only used by the remote protocol client.
    #[cfg_attr(not(target_family = "wasm"), allow(dead_code))]
    pub(crate) idempotency_key: Option<String>,
    /// Caps the buffered result rows; `None` keeps the engine default.
    #[serde(skip)]
    pub(crate) max_bytes: Option<usize>,
}

/// Converts the JS `maxBytes` option: a positive integer, or `Infinity` for
/// no cap. Omitted keeps the engine default (64 MiB).
pub(crate) fn max_bytes_option(value: Option<f64>) -> Result<Option<usize>, LixError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value == f64::INFINITY {
        return Ok(Some(usize::MAX));
    }
    if !(value.is_finite()
        && value >= 1.0
        && value.fract() == 0.0
        && value <= 9_007_199_254_740_991.0)
    {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "maxBytes must be a positive integer or Infinity",
        ));
    }
    Ok(Some(value as usize))
}

/// Options of a buffered read.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ReadOptions {
    /// Byte cap; `None` keeps the engine default.
    pub(crate) max_bytes: Option<usize>,
    /// Reads at most one row (`queryOne`): fails with `LIX_ERROR_NOT_UNIQUE` when
    /// there are more, and is not byte-capped.
    pub(crate) single: bool,
}

/// Runs a buffered read with the binding's read options.
pub(crate) async fn run_query(
    query: lix::Query,
    options: ReadOptions,
) -> Result<ExecuteResult, LixError> {
    if options.single {
        let row = query.optional().await?;
        return Ok(match row {
            Some(row) => {
                ExecuteResult::from_rows(row.columns().to_vec(), vec![row.values().to_vec()])
            }
            None => ExecuteResult::from_rows(Vec::new(), Vec::new()),
        });
    }
    Ok(with_query_limit(query, options.max_bytes)
        .all()
        .await?
        .into())
}

// No default methods: every backend must explicitly route every operation.
// Static dispatch deliberately allows non-Send browser transports/futures.
pub(crate) trait SessionOperations: Sized {
    type Transaction;
    type Observation;
    type Snapshot;

    async fn open_another_session(
        &self,
        branch_id: Option<String>,
        account_id: Option<String>,
    ) -> Result<Self, LixError>;
    async fn begin_transaction(&self) -> Result<Self::Transaction, LixError>;
    async fn observe(&self, sql: &str, params: &[Value]) -> Result<Self::Observation, LixError>;
    async fn export_snapshot(&self) -> Result<Self::Snapshot, LixError>;
    /// A buffered read of the latest committed state. Writes are rejected.
    async fn query(
        &self,
        sql: &str,
        params: &[Value],
        options: ReadOptions,
    ) -> Result<ExecuteResult, LixError>;
    /// Opens a read stream; resolves once the first page has arrived.
    async fn stream(
        &self,
        sql: &str,
        params: &[Value],
        max_bytes: Option<usize>,
    ) -> Result<lix::RowStream, LixError>;
    /// Pins the current state for several reads.
    async fn read_snapshot(&self) -> Result<lix::Snapshot, LixError>;
    async fn close(&self) -> Result<(), LixError>;
    /// Runs any single statement.
    async fn execute(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError>;
    /// Runs one statement once per parameter set, atomically.
    async fn execute_many(
        &self,
        sql: &str,
        param_sets: Vec<Vec<Value>>,
        options: ExecuteOptions,
    ) -> Result<u64, LixError>;
    async fn sync_health(&self) -> Result<lix::SyncHealth, LixError>;
    async fn prepare_offline_editing(&self) -> Result<(), LixError>;
    async fn active_branch_id(&self) -> Result<String, LixError>;
    async fn active_account_id(&self) -> Result<String, LixError>;
    async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError>;
    async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<SwitchBranchReceipt, LixError>;
    async fn merge_branch_preview(
        &self,
        options: MergeBranchPreviewOptions,
    ) -> Result<MergeBranchPreview, LixError>;
    async fn merge_branch(
        &self,
        options: MergeBranchOptions,
    ) -> Result<MergeBranchReceipt, LixError>;
}

fn with_query_limit(query: lix::Query, max_bytes: Option<usize>) -> lix::Query {
    match max_bytes {
        Some(max_bytes) => query.max_bytes(max_bytes),
        None => query,
    }
}

fn configured_execute(mut execution: lix::Execute, options: ExecuteOptions) -> lix::Execute {
    if let Some(limit) = options.max_auto_commit_retries {
        execution = execution.with_max_auto_commit_retries(limit);
    }
    if let Some(origin_key) = options.origin_key {
        execution = execution.with_origin_key(origin_key);
    }
    if let Some(max_bytes) = options.max_bytes {
        execution = execution.max_bytes(max_bytes);
    }
    execution
}

fn configured_execute_many(
    mut execution: lix::ExecuteMany,
    options: ExecuteOptions,
) -> lix::ExecuteMany {
    if let Some(limit) = options.max_auto_commit_retries {
        execution = execution.with_max_auto_commit_retries(limit);
    }
    if let Some(origin_key) = options.origin_key {
        execution = execution.with_origin_key(origin_key);
    }
    execution
}

impl<S: Storage + Clone + Send + Sync + 'static> SessionOperations for Lix<S> {
    type Transaction = lix::LixTransaction<S>;
    type Observation = lix::ObserveEvents<S>;
    type Snapshot = lix::snapshot::SnapshotExportBuilder<S>;

    async fn open_another_session(
        &self,
        branch_id: Option<String>,
        account_id: Option<String>,
    ) -> Result<Self, LixError> {
        let mut builder = Lix::open_another_session(self);
        if let Some(branch_id) = branch_id {
            builder = builder.with_branch(branch_id);
        }
        if let Some(account_id) = account_id {
            builder = builder.with_account(account_id);
        }
        builder.await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, LixError> {
        Lix::begin_transaction(self).await
    }

    async fn observe(&self, sql: &str, params: &[Value]) -> Result<Self::Observation, LixError> {
        Lix::observe(self, sql, params)
    }

    async fn export_snapshot(&self) -> Result<Self::Snapshot, LixError> {
        Ok(Lix::export_snapshot(self))
    }

    async fn query(
        &self,
        sql: &str,
        params: &[Value],
        options: ReadOptions,
    ) -> Result<ExecuteResult, LixError> {
        run_query(Lix::query(self, sql).params(params), options).await
    }

    async fn stream(
        &self,
        sql: &str,
        params: &[Value],
        max_bytes: Option<usize>,
    ) -> Result<lix::RowStream, LixError> {
        with_query_limit(Lix::query(self, sql).params(params), max_bytes)
            .stream()
            .await
    }

    async fn read_snapshot(&self) -> Result<lix::Snapshot, LixError> {
        Lix::snapshot(self).await
    }

    async fn close(&self) -> Result<(), LixError> {
        Lix::close(self).await
    }

    async fn execute(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        configured_execute(Lix::execute(self, sql).params(params), options).await
    }

    async fn execute_many(
        &self,
        sql: &str,
        param_sets: Vec<Vec<Value>>,
        options: ExecuteOptions,
    ) -> Result<u64, LixError> {
        configured_execute_many(Lix::execute_many(self, sql, param_sets), options).await
    }

    async fn sync_health(&self) -> Result<lix::SyncHealth, LixError> {
        Ok(Lix::sync_health(self))
    }
    async fn prepare_offline_editing(&self) -> Result<(), LixError> {
        Lix::prepare_offline_editing(self).await
    }
    async fn active_branch_id(&self) -> Result<String, LixError> {
        Lix::active_branch_id(self).await
    }

    async fn active_account_id(&self) -> Result<String, LixError> {
        Ok(Lix::active_account_id(self).to_owned())
    }

    async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError> {
        Lix::create_branch(self, options).await
    }

    async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<SwitchBranchReceipt, LixError> {
        Lix::switch_branch(self, options).await
    }

    async fn merge_branch_preview(
        &self,
        options: MergeBranchPreviewOptions,
    ) -> Result<MergeBranchPreview, LixError> {
        Lix::merge_branch_preview(self, options).await
    }

    async fn merge_branch(
        &self,
        options: MergeBranchOptions,
    ) -> Result<MergeBranchReceipt, LixError> {
        Lix::merge_branch(self, options).await
    }
}

/// Transaction wrappers own their optional handle. Keep each engine's existing
/// finalization semantics: local consumes on an attempt; remote retains a handle
/// when a protocol failure can still be retried.
pub(crate) trait TransactionOperations {
    /// A buffered read that sees the transaction's writes.
    async fn query(
        &mut self,
        sql: &str,
        params: &[Value],
        options: ReadOptions,
    ) -> Result<ExecuteResult, LixError>;
    /// A read stream at the transaction's state when it opens.
    async fn stream(
        &mut self,
        sql: &str,
        params: &[Value],
        max_bytes: Option<usize>,
    ) -> Result<lix::RowStream, LixError>;
    async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError>;
    async fn execute_many(
        &mut self,
        sql: &str,
        param_sets: Vec<Vec<Value>>,
        options: ExecuteOptions,
    ) -> Result<u64, LixError>;
    async fn commit(&mut self) -> Result<lix::CommitReceipt, LixError>;
    async fn rollback(&mut self) -> Result<(), LixError>;
}

fn transaction_closed_error() -> LixError {
    LixError::new("LIX_INVALID_TRANSACTION_STATE", "Lix transaction is closed")
}

impl<S: Storage + Clone + Send + Sync + 'static> TransactionOperations
    for Option<lix::LixTransaction<S>>
{
    async fn query(
        &mut self,
        sql: &str,
        params: &[Value],
        options: ReadOptions,
    ) -> Result<ExecuteResult, LixError> {
        let transaction = self.as_ref().ok_or_else(transaction_closed_error)?;
        run_query(transaction.query(sql).params(params), options).await
    }

    async fn stream(
        &mut self,
        sql: &str,
        params: &[Value],
        max_bytes: Option<usize>,
    ) -> Result<lix::RowStream, LixError> {
        let transaction = self.as_ref().ok_or_else(transaction_closed_error)?;
        with_query_limit(transaction.query(sql).params(params), max_bytes)
            .stream()
            .await
    }

    async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        let transaction = self.as_ref().ok_or_else(transaction_closed_error)?;
        configured_execute(transaction.execute(sql).params(params), options).await
    }

    async fn execute_many(
        &mut self,
        sql: &str,
        param_sets: Vec<Vec<Value>>,
        options: ExecuteOptions,
    ) -> Result<u64, LixError> {
        let transaction = self.as_ref().ok_or_else(transaction_closed_error)?;
        configured_execute_many(transaction.execute_many(sql, param_sets), options).await
    }

    async fn commit(&mut self) -> Result<lix::CommitReceipt, LixError> {
        self.take()
            .ok_or_else(transaction_closed_error)?
            .commit()
            .await
    }

    async fn rollback(&mut self) -> Result<(), LixError> {
        self.take()
            .ok_or_else(transaction_closed_error)?
            .rollback()
            .await
    }
}

#[cfg(target_family = "wasm")]
mod remote {
    use super::*;
    use lix::QueryResult;

    fn not_unique() -> LixError {
        LixError::new(
            LixError::CODE_NOT_UNIQUE,
            "query returned more than one row",
        )
        .with_hint("Use query() when several rows are expected.")
    }

    /// Applies `single` to a result buffered elsewhere (remote reads).
    fn single_row(result: QueryResult, options: ReadOptions) -> Result<ExecuteResult, LixError> {
        if options.single && result.rows().len() > 1 {
            return Err(not_unique());
        }
        Ok(result.into())
    }

    fn remote_max_bytes_unsupported() -> LixError {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            "maxBytes is not supported for remote execute()",
        )
        .with_hint(
            "The server caps RETURNING rows at its default before it commits; a client-side check \
             would only fail after the write was published.",
        )
    }

    fn unsupported_remote(operation: &str) -> LixError {
        LixError::new(
            "LIX_UNSUPPORTED_REMOTE_OPERATION",
            format!("{operation} is not supported on a remote Lix handle"),
        )
        .with_details(serde_json::json!({ "operation": operation }))
        .with_hint("Use query() for reads on a remote handle, or open the repository locally.")
    }

    use lix::server_protocol::client::{
        ClientCore, ProtocolClient, ProtocolExecuteOptions, ProtocolHttp,
    };

    impl From<ExecuteOptions> for ProtocolExecuteOptions {
        fn from(options: ExecuteOptions) -> Self {
            Self {
                origin_key: options.origin_key,
                max_auto_commit_retries: options.max_auto_commit_retries,
                idempotency_key: options.idempotency_key,
            }
        }
    }

    /// A single-row read is not byte-capped.
    fn read_cap(options: ReadOptions) -> Option<usize> {
        if options.single {
            Some(usize::MAX)
        } else {
            options.max_bytes
        }
    }

    fn remote_execute_options(options: ExecuteOptions) -> Result<ProtocolExecuteOptions, LixError> {
        if options.max_bytes.is_some() {
            return Err(remote_max_bytes_unsupported());
        }
        Ok(options.into())
    }

    impl<H: ProtocolHttp> TransactionOperations
        for Option<lix::server_protocol::client::ProtocolTransaction<H>>
    {
        async fn query(
            &mut self,
            sql: &str,
            params: &[Value],
            options: ReadOptions,
        ) -> Result<ExecuteResult, LixError> {
            let result = self
                .as_ref()
                .ok_or_else(transaction_closed_error)?
                .query(sql, params, read_cap(options))
                .await?;
            single_row(result, options)
        }

        async fn stream(
            &mut self,
            _sql: &str,
            _params: &[Value],
            _max_bytes: Option<usize>,
        ) -> Result<lix::RowStream, LixError> {
            Err(unsupported_remote("stream"))
        }

        async fn execute(
            &mut self,
            sql: &str,
            params: &[Value],
            options: ExecuteOptions,
        ) -> Result<ExecuteResult, LixError> {
            let options = remote_execute_options(options)?;
            self.as_ref()
                .ok_or_else(transaction_closed_error)?
                .execute(sql, params, Some(options))
                .await
        }

        async fn execute_many(
            &mut self,
            sql: &str,
            param_sets: Vec<Vec<Value>>,
            options: ExecuteOptions,
        ) -> Result<u64, LixError> {
            let options = remote_execute_options(options)?;
            self.as_ref()
                .ok_or_else(transaction_closed_error)?
                .execute_many(sql, param_sets, Some(options))
                .await
        }

        async fn commit(&mut self) -> Result<lix::CommitReceipt, LixError> {
            let receipt = self
                .as_ref()
                .ok_or_else(transaction_closed_error)?
                .commit()
                .await?;
            *self = None;
            Ok(receipt)
        }

        async fn rollback(&mut self) -> Result<(), LixError> {
            self.as_ref()
                .ok_or_else(transaction_closed_error)?
                .rollback()
                .await?;
            *self = None;
            Ok(())
        }
    }

    impl<H: ProtocolHttp + Clone + 'static> SessionOperations for ProtocolClient<H> {
        type Transaction = lix::server_protocol::client::ProtocolTransaction<H>;
        type Observation = lix::server_protocol::client::ProtocolObserveEvents<ClientCore<H>>;
        type Snapshot = lix::server_protocol::client::ProtocolSnapshotExport;

        async fn open_another_session(
            &self,
            branch_id: Option<String>,
            account_id: Option<String>,
        ) -> Result<Self, LixError> {
            ProtocolClient::open_another_session(self, branch_id, account_id).await
        }

        async fn begin_transaction(&self) -> Result<Self::Transaction, LixError> {
            ClientCore::begin_transaction(self).await
        }

        async fn observe(
            &self,
            sql: &str,
            params: &[Value],
        ) -> Result<Self::Observation, LixError> {
            ProtocolClient::observe(self, sql, params.to_vec()).await
        }

        async fn export_snapshot(&self) -> Result<Self::Snapshot, LixError> {
            ClientCore::export_snapshot(self).await
        }

        async fn query(
            &self,
            sql: &str,
            params: &[Value],
            options: ReadOptions,
        ) -> Result<ExecuteResult, LixError> {
            let result = ClientCore::query(self, sql, params, read_cap(options)).await?;
            single_row(result, options)
        }

        async fn stream(
            &self,
            _sql: &str,
            _params: &[Value],
            _max_bytes: Option<usize>,
        ) -> Result<lix::RowStream, LixError> {
            // Remote reads run behind the protocol's buffered execute; a
            // stream would need a server-held snapshot spanning requests.
            Err(unsupported_remote("stream"))
        }

        async fn read_snapshot(&self) -> Result<lix::Snapshot, LixError> {
            Err(unsupported_remote("snapshot"))
        }

        async fn close(&self) -> Result<(), LixError> {
            ProtocolClient::close(self).await
        }

        async fn execute(
            &self,
            sql: &str,
            params: &[Value],
            options: ExecuteOptions,
        ) -> Result<ExecuteResult, LixError> {
            let options = remote_execute_options(options)?;
            ClientCore::execute(self, sql, params, Some(options)).await
        }

        async fn execute_many(
            &self,
            sql: &str,
            param_sets: Vec<Vec<Value>>,
            options: ExecuteOptions,
        ) -> Result<u64, LixError> {
            let options = remote_execute_options(options)?;
            ClientCore::execute_many(self, sql, param_sets, Some(options)).await
        }

        async fn sync_health(&self) -> Result<lix::SyncHealth, LixError> {
            // Remote SQL sessions have no local replica synchronization worker.
            Ok(lix::SyncHealth::default())
        }
        async fn prepare_offline_editing(&self) -> Result<(), LixError> {
            Err(LixError::new(
                "LIX_SYNC_MODE_MISMATCH",
                "offline editing preparation requires a partial replica",
            ))
        }
        async fn active_branch_id(&self) -> Result<String, LixError> {
            ClientCore::active_branch_id(self).await
        }

        async fn active_account_id(&self) -> Result<String, LixError> {
            ClientCore::active_account_id(self).await
        }

        async fn create_branch(
            &self,
            options: CreateBranchOptions,
        ) -> Result<CreateBranchReceipt, LixError> {
            ClientCore::create_branch(self, options).await
        }

        async fn switch_branch(
            &self,
            options: SwitchBranchOptions,
        ) -> Result<SwitchBranchReceipt, LixError> {
            self.switch_branch_and_restart(&options.branch_id).await
        }

        async fn merge_branch_preview(
            &self,
            options: MergeBranchPreviewOptions,
        ) -> Result<MergeBranchPreview, LixError> {
            ClientCore::merge_branch_preview(self, options).await
        }

        async fn merge_branch(
            &self,
            options: MergeBranchOptions,
        ) -> Result<MergeBranchReceipt, LixError> {
            ClientCore::merge_branch(self, options).await
        }
    }
}
