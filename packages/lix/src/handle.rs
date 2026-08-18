// The hard-cut keeps a small set of crate-internal transport and profiling
// helpers that no longer have enabled in-crate callers.
#![cfg_attr(test, allow(dead_code))]

use lix::plugin::runtime::WasmRuntime;
use lix::storage::Storage;
use lix::telemetry::TelemetrySink;
use lix::{
    Blob, CreateBranchOptions, CreateBranchReceipt, CreateCheckpointReceipt, ExecuteBatchStatement,
    ExecuteIdempotency, ExecuteResult, ExecuteStatementMetadata, ExecutionDisposition, LixError,
    Memory, MergeBranchOptions, MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    ObserveEvents, PreparedDmlParameterBatch, RedoReceipt, SwitchBranchOptions,
    SwitchBranchReceipt, UndoReceipt, Value,
};
use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    sync::Arc,
};

use crate::engine::{Engine, EngineOptions};
use crate::session::{CoherentReadBatch, ExecuteOptions};
use crate::session::SessionContext;
#[cfg(test)]
use crate::transaction_types::TransactionWriteRow;

/// Configures the primary session for a Lix repository.
///
/// The default builder opens an in-memory Lix. Configure a persistent adapter
/// with [`OpenLixBuilder::with_storage`] and then await the builder.
#[expect(missing_debug_implementations)]
pub struct OpenLixBuilder<StorageImpl = Memory> {
    storage: StorageImpl,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
}

impl Default for OpenLixBuilder<Memory> {
    fn default() -> Self {
        Self {
            storage: Memory::new(),
            wasm_runtime: None,
            telemetry: None,
        }
    }
}

impl<StorageImpl> OpenLixBuilder<StorageImpl> {
    /// Replaces the default in-memory storage with `storage`.
    pub fn with_storage<NewStorageImpl>(
        self,
        storage: NewStorageImpl,
    ) -> OpenLixBuilder<NewStorageImpl> {
        OpenLixBuilder {
            storage,
            wasm_runtime: self.wasm_runtime,
            telemetry: self.telemetry,
        }
    }

    /// Supplies the Component runtime used by plugins.
    pub fn with_wasm_runtime(mut self, wasm_runtime: Arc<dyn WasmRuntime>) -> Self {
        self.wasm_runtime = Some(wasm_runtime);
        self
    }

    /// Sends engine spans to `telemetry` for this Lix instance.
    pub fn with_telemetry(mut self, telemetry: Arc<dyn TelemetrySink>) -> Self {
        self.telemetry = Some(telemetry);
        self
    }
}

/// Starts configuring the primary session for a Lix repository.
///
/// The primary session starts on the repository's tracked
/// `lix_default_branch_id`. Applications own window- or session-specific
/// branch selection and can switch explicitly after opening.
///
/// Await the returned builder to open a new in-memory Lix:
///
/// ```no_run
/// # async fn example() -> Result<(), lix::LixError> {
/// let lix = lix::open_lix().await?;
/// # Ok(())
/// # }
/// ```
pub fn open_lix() -> OpenLixBuilder<Memory> {
    OpenLixBuilder::default()
}

impl<StorageImpl> IntoFuture for OpenLixBuilder<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    type Output = Result<Lix<StorageImpl>, LixError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        // SAFETY: the builder owns Send storage/runtime/telemetry values, and
        // the returned Lix contains only Send synchronization primitives. The
        // compiler cannot prove all deeply nested SQL futures are Send.
        Box::pin(unsafe {
            crate::session::AssumeSendFuture::new(async move {
                open_lix_inner(self.storage, self.wasm_runtime, self.telemetry).await
            })
        })
    }
}

/// Configures another independent session for an open Lix repository.
///
/// The new session starts on the current branch and inherits the current
/// account unless [`OpenAnotherSessionBuilder::with_account`] overrides it.
#[expect(missing_debug_implementations)]
pub struct OpenAnotherSessionBuilder<'a, StorageImpl = Memory>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix: &'a Lix<StorageImpl>,
    account_id: Option<String>,
    branch_id: Option<String>,
}

impl<'a, StorageImpl> OpenAnotherSessionBuilder<'a, StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Attributes changes from the new session to `account_id`.
    ///
    /// This selects an existing account; it does not create one.
    pub fn with_account(mut self, account_id: impl Into<String>) -> Self {
        self.account_id = Some(account_id.into());
        self
    }

    /// Opens the additional session on `branch_id` without changing the
    /// primary session or repository default.
    pub fn with_branch(mut self, branch_id: impl Into<String>) -> Self {
        self.branch_id = Some(branch_id.into());
        self
    }
}

impl<'a, StorageImpl> IntoFuture for OpenAnotherSessionBuilder<'a, StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    type Output = Result<Lix<StorageImpl>, LixError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        // SAFETY: the future only borrows a Send + Sync Lix handle and owns the
        // optional account id. Storage handles satisfy the Storage Send
        // contract; the remaining compiler limitation is caused by nested
        // higher-ranked SQL futures.
        Box::pin(unsafe {
            crate::session::AssumeSendFuture::new(async move {
                self.lix
                    .open_another_session_inner(self.account_id, self.branch_id)
                    .await
            })
        })
    }
}

/// Configures one SQL statement execution.
#[expect(missing_debug_implementations)]
pub struct ExecuteBuilder<'a, StorageImpl = Memory>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix: &'a Lix<StorageImpl>,
    sql: String,
    params: Vec<Value>,
    options: ExecuteOptions,
}

impl<StorageImpl> ExecuteBuilder<'_, StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Identifies the caller-defined origin of this execution.
    pub fn with_origin_key(mut self, origin_key: impl Into<String>) -> Self {
        self.options.origin_key = Some(origin_key.into());
        self
    }
}

impl<'a, StorageImpl> IntoFuture for ExecuteBuilder<'a, StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    type Output = Result<ExecuteResult, LixError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        // SAFETY: the builder owns the SQL, parameters, and options. The only
        // borrowed value retained across suspension is a shared reference to
        // the Sync session; storage handles are Send by the Storage contract.
        Box::pin(unsafe {
            crate::session::AssumeSendFuture::new(async move {
                self.lix
                    .session
                    .execute_with_options(&self.sql, &self.params, self.options)
                    .await
            })
        })
    }
}

/// Configures one atomic SQL batch execution.
#[expect(missing_debug_implementations)]
pub struct ExecuteBatchBuilder<'a, StorageImpl = Memory>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix: &'a Lix<StorageImpl>,
    statements: Vec<ExecuteBatchStatement>,
    options: ExecuteOptions,
}

impl<StorageImpl> ExecuteBatchBuilder<'_, StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Identifies the caller-defined origin of this batch.
    pub fn with_origin_key(mut self, origin_key: impl Into<String>) -> Self {
        self.options.origin_key = Some(origin_key.into());
        self
    }
}

impl<'a, StorageImpl> IntoFuture for ExecuteBatchBuilder<'a, StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    type Output = Result<Vec<ExecuteResult>, LixError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        // SAFETY: as above, the builder owns every request value and borrows
        // only the Sync session across suspension.
        Box::pin(unsafe {
            crate::session::AssumeSendFuture::new(async move {
                self.lix
                    .session
                    .execute_batch_with_options(&self.statements, self.options)
                    .await
            })
        })
    }
}

/// Clonable handle for a Lix repository.
///
/// Clones share the active branch, transaction exclusion, file-view state,
/// and close lifecycle.
///
/// Public operation builders erase their internal future type, so embedding
/// applications can spawn composed Lix flows without raising rustc's
/// recursion limit.
#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct Lix<StorageImpl = Memory>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    engine: Arc<Engine<StorageImpl>>,
    session: Arc<SessionContext<StorageImpl>>,
    primary_switch_gate: Option<Arc<tokio::sync::Mutex<()>>>,
}

async fn open_lix_inner<StorageImpl>(
    storage: StorageImpl,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
) -> Result<Lix<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let engine = open_or_initialize_engine(storage, wasm_runtime, telemetry, None).await?;
    let session = engine.open_session().await?;
    Ok(Lix {
        engine: Arc::new(engine),
        session: Arc::new(session),
        primary_switch_gate: Some(Arc::new(tokio::sync::Mutex::new(()))),
    })
}

impl<StorageImpl> Lix<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Starts configuring another independent session for this repository.
    ///
    /// Await the returned builder directly, or call
    /// [`OpenAnotherSessionBuilder::with_account`] first. The new session
    /// starts on this handle's current branch and otherwise inherits its
    /// account.
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), lix::LixError> {
    /// let lix = lix::open_lix().await?;
    /// let collaborator = lix.open_another_session().await?;
    /// # collaborator.close().await?;
    /// # lix.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open_another_session(&self) -> OpenAnotherSessionBuilder<'_, StorageImpl> {
        OpenAnotherSessionBuilder {
            lix: self,
            account_id: None,
            branch_id: None,
        }
    }

    async fn open_another_session_inner(
        &self,
        account_id: Option<String>,
        branch_id: Option<String>,
    ) -> Result<Self, LixError> {
        if self.session.is_closed() {
            return Err(LixError::new(
                LixError::CODE_CLOSED,
                "cannot open another session from a closed Lix handle",
            ));
        }
        let active_branch_id = match branch_id {
            Some(branch_id) => branch_id,
            None => self.active_branch_id().await?,
        };
        let active_account_id = account_id.unwrap_or_else(|| self.active_account_id().to_owned());
        self.open_internal_session(active_branch_id, active_account_id)
            .await
    }

    pub(crate) async fn open_internal_session(
        &self,
        active_branch_id: impl Into<String>,
        active_account_id: impl Into<String>,
    ) -> Result<Self, LixError> {
        if self.session.is_closed() {
            return Err(LixError::new(
                LixError::CODE_CLOSED,
                "cannot open a session from a closed Lix handle",
            ));
        }
        let active_branch_id = active_branch_id.into();
        if self
            .engine
            .load_branch_head_commit_id(&active_branch_id)
            .await?
            .is_none()
        {
            return Err(LixError::branch_not_found(
                active_branch_id,
                "open_another_session",
                "target",
            ));
        }
        let session = self
            .engine
            .open_session_at_with_account(active_branch_id, active_account_id)
            .await?;
        Ok(Self {
            engine: self.engine.clone(),
            session: Arc::new(session),
            primary_switch_gate: None,
        })
    }

    /// Executes one PostgreSQL-dialect SQL statement against this Lix session.
    ///
    /// Lix supports a PostgreSQL-dialect subset executed by DataFusion.
    /// Positional placeholders use `$1`, `$2`, and so on. Parsing PostgreSQL
    /// syntax does not imply support for every PostgreSQL statement or runtime
    /// feature. Use `information_schema` for catalog inspection. Lix owns
    /// transaction boundaries for each statement.
    /// While a transaction is active, call `execute()` on the transaction
    /// handle instead.
    pub fn execute<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Value],
    ) -> ExecuteBuilder<'a, StorageImpl> {
        ExecuteBuilder {
            lix: self,
            sql: sql.to_string(),
            params: params.to_vec(),
            options: ExecuteOptions::default(),
        }
    }

    /// Classifies one SQL execution for a caller that owns its transport
    /// lifecycle.
    ///
    /// The result comes from Lix's parsed and bound statement route. It is
    /// safe for a transport to abandon [`ExecutionDisposition::CancellableRead`]
    /// work; [`ExecutionDisposition::Durable`] work must be allowed to finish.
    pub(crate) fn execution_disposition(
        &self,
        sql: &str,
    ) -> Result<ExecutionDisposition, LixError> {
        self.session.execution_disposition(sql)
    }

    /// Upserts one file's bytes by full logical path without parsing SQL.
    ///
    /// This structured path is intended for file transfer clients. It uses the
    /// engine's filesystem fast-write path and retains normal plugin and
    /// transaction behavior.
    pub(crate) async fn upsert_file_content(
        &self,
        path: impl Into<String>,
        content: impl Into<Blob>,
    ) -> Result<u64, LixError> {
        self.session
            .upsert_file_content(path.into(), content.into())
            .await
    }

    /// Sends one sequential resumable part through the same logical file
    /// upsert. The final part atomically publishes the ordinary file version.
    pub(crate) async fn upsert_file_content_part(
        &self,
        upload_id: impl Into<String>,
        path: impl Into<String>,
        start: u64,
        total_size: u64,
        content: impl Into<Blob>,
    ) -> Result<lix::FileUploadProgress, LixError> {
        self.session
            .upsert_file_content_part(
                upload_id.into(),
                path.into(),
                start,
                total_size,
                content.into(),
            )
            .await
    }

    /// Upserts a non-empty batch of files atomically without parsing SQL for
    /// normal filesystem layouts.
    ///
    /// Each item is a full logical file path and its bytes. Paths must be
    /// unique within the batch. This direct-only API rejects exceptional
    /// layouts that its path index cannot route unambiguously.
    pub(crate) async fn upsert_file_content_batch(
        &self,
        writes: Vec<(String, Blob)>,
    ) -> Result<u64, LixError> {
        self.session.upsert_file_content_batch(writes).await
    }

    /// Reads one file's bytes by full logical path without parsing SQL.
    ///
    /// The returned `None` means the file is absent; `Some` with an empty
    /// [`Blob`] means a present empty file.
    pub(crate) async fn read_file_content(
        &self,
        path: impl Into<String>,
        range: Option<std::ops::Range<u64>>,
    ) -> Result<Option<lix::FileRead>, LixError> {
        self.session.read_file_content(path.into(), range).await
    }

    pub(crate) async fn execute_with_options_and_metadata(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
        metadata: ExecuteStatementMetadata,
    ) -> Result<ExecuteResult, LixError> {
        self.session
            .execute_with_options_and_metadata(sql, params, options, metadata)
            .await
    }

    pub(crate) fn execute_with_idempotency_and_options_and_metadata(
        self: Arc<Self>,
        sql: String,
        params: Vec<Value>,
        options: ExecuteOptions,
        metadata: ExecuteStatementMetadata,
        idempotency: Option<ExecuteIdempotency>,
    ) -> impl Future<Output = Result<ExecuteResult, LixError>> + Send + 'static {
        Arc::clone(&self.session).execute_with_idempotency_and_options_and_metadata(
            sql,
            params,
            options,
            metadata,
            idempotency,
        )
    }

    /// Executes statements sequentially against one atomic snapshot.
    /// Pure reads share one read snapshot; batches containing writes retain
    /// transactional read-after-write and rollback semantics.
    pub fn execute_batch<'a>(
        &'a self,
        statements: &'a [ExecuteBatchStatement],
    ) -> ExecuteBatchBuilder<'a, StorageImpl> {
        ExecuteBatchBuilder {
            lix: self,
            statements: statements.to_vec(),
            options: ExecuteOptions::default(),
        }
    }

    /// Executes read statements against one coherent storage snapshot and
    /// returns the snapshot metadata required by official storage adapters.
    #[doc(hidden)]
    pub fn execute_coherent_read_batch(
        &self,
        statements: &[(&str, &[Value])],
    ) -> impl Future<Output = Result<CoherentReadBatch, LixError>> + Send + 'static {
        let statements = statements
            .iter()
            .map(|(sql, params)| ((*sql).to_owned(), (*params).to_vec()))
            .collect();
        Arc::clone(&self.session).execute_coherent_read_batch_owned(statements)
    }

    /// Executes one prepared DML statement shape for a rectangular parameter
    /// page in one atomic transaction. The SQL is planned once and each
    /// parameter row produces one result in input order. This is the public
    /// bulk-write contract used by generated/transport callers; unsupported
    /// shapes fail closed rather than degrading to per-row SQL execution.
    pub(crate) async fn execute_prepared_dml_batch(
        &self,
        sql: Arc<str>,
        parameter_batch: PreparedDmlParameterBatch,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        self.session
            .execute_prepared_dml_batch(sql, parameter_batch)
            .await
    }

    /// Classifies an atomic SQL batch for a caller that owns its transport
    /// lifecycle.
    pub(crate) fn execute_batch_disposition(
        &self,
        statements: &[ExecuteBatchStatement],
    ) -> Result<ExecutionDisposition, LixError> {
        self.session.execute_batch_disposition(statements)
    }

    pub(crate) async fn execute_batch_with_options_and_metadata(
        &self,
        statements: &[ExecuteBatchStatement],
        options: ExecuteOptions,
        statement_metadata: Vec<ExecuteStatementMetadata>,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        self.session
            .execute_batch_with_options_and_metadata(statements, options, statement_metadata)
            .await
    }

    pub(crate) fn execute_batch_with_idempotency_and_options_and_metadata(
        self: Arc<Self>,
        statements: Vec<ExecuteBatchStatement>,
        options: ExecuteOptions,
        statement_metadata: Vec<ExecuteStatementMetadata>,
        idempotency: Option<ExecuteIdempotency>,
    ) -> impl Future<Output = Result<Vec<ExecuteResult>, LixError>> + Send + 'static {
        Arc::clone(&self.session).execute_batch_with_idempotency_and_options_and_metadata(
            statements,
            options,
            statement_metadata,
            idempotency,
        )
    }

    pub fn observe(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<ObserveEvents<StorageImpl>, LixError> {
        self.session.observe(sql, params)
    }

    pub async fn begin_transaction(&self) -> Result<LixTransaction<StorageImpl>, LixError> {
        Ok(LixTransaction {
            inner: self.session.begin_transaction().await?,
        })
    }

    pub fn active_branch_id(
        &self,
    ) -> impl Future<Output = Result<String, LixError>> + Send + 'static {
        Arc::clone(&self.session).active_branch_id_owned()
    }

    pub fn active_account_id(&self) -> &str {
        self.session.active_account_id()
    }

    /// Creates an active global account if it does not exist. Existing mutable
    /// account fields are deliberately left unchanged.
    pub(crate) async fn ensure_account(
        &self,
        id: &str,
        name: &str,
        kind: &str,
    ) -> Result<(), LixError> {
        let branch_id = Box::pin(self.active_branch_id()).await?;
        let system =
            Box::pin(self.open_internal_session(branch_id, lix::SYSTEM_ACCOUNT_ID)).await?;
        system
            .execute(
                "INSERT INTO lix_account_by_branch \
                 (id, name, kind, status, lixcol_branch_id, lixcol_global, lixcol_untracked) \
                 VALUES ($1, $2, $3, 'active', $4, true, false) \
                 ON CONFLICT (id, lixcol_branch_id) \
                 DO NOTHING",
                &[
                    Value::Text(id.to_string()),
                    Value::Text(name.to_string()),
                    Value::Text(kind.to_string()),
                    Value::Text(lix::GLOBAL_BRANCH_ID.to_string()),
                ],
            )
            .await?;
        Box::pin(system.close()).await
    }

    pub async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError> {
        self.session.create_branch(options).await
    }

    pub async fn create_checkpoint(&self) -> Result<CreateCheckpointReceipt, LixError> {
        self.session.create_checkpoint().await
    }

    /// Reverses the latest undoable tracked commit on this handle's active branch.
    pub async fn undo(&self) -> Result<UndoReceipt, LixError> {
        self.session.undo().await
    }

    /// Replays the latest tracked commit abandoned by undo on this handle's active branch.
    pub async fn redo(&self) -> Result<RedoReceipt, LixError> {
        self.session.redo().await
    }

    pub fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> impl Future<Output = Result<SwitchBranchReceipt, LixError>> + Send + '_ {
        // SAFETY: the future borrows a Send + Sync Lix handle and owns its
        // switch options. The compiler cannot prove the nested switch SQL
        // future is Send for every storage read lifetime.
        unsafe {
            crate::session::AssumeSendFuture::new(async move {
                let _primary_switch_guard = match &self.primary_switch_gate {
                    Some(gate) => Some(gate.lock().await),
                    None => None,
                };
                self.session.switch_branch(options).await
            })
        }
    }

    pub async fn merge_branch(
        &self,
        options: MergeBranchOptions,
    ) -> Result<MergeBranchReceipt, LixError> {
        self.session.merge_branch(options).await
    }

    pub async fn merge_branch_preview(
        &self,
        options: MergeBranchPreviewOptions,
    ) -> Result<MergeBranchPreview, LixError> {
        self.session.merge_branch_preview(options).await
    }

    pub async fn close(&self) -> Result<(), LixError> {
        self.session.close().await
    }

}

#[expect(missing_debug_implementations)]
pub struct LixTransaction<StorageImpl = Memory>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    inner: lix::SessionTransaction<StorageImpl>,
}

/// Configures one SQL statement inside an explicit transaction.
#[expect(missing_debug_implementations)]
pub struct TransactionExecuteBuilder<'a, StorageImpl = Memory>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    transaction: &'a mut LixTransaction<StorageImpl>,
    sql: &'a str,
    params: &'a [Value],
    options: ExecuteOptions,
}

impl<StorageImpl> TransactionExecuteBuilder<'_, StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Identifies the caller-defined origin of this execution.
    pub fn with_origin_key(mut self, origin_key: impl Into<String>) -> Self {
        self.options.origin_key = Some(origin_key.into());
        self
    }
}

impl<'a, StorageImpl> IntoFuture for TransactionExecuteBuilder<'a, StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    type Output = Result<ExecuteResult, LixError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            self.transaction
                .inner
                .execute_with_options(self.sql.to_owned(), self.params.to_vec(), self.options)
                .await
        })
    }
}

impl<StorageImpl> LixTransaction<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Executes one SQL statement inside this transaction.
    ///
    /// Writes are staged until `commit()`. Reads use the transaction overlay,
    /// so they can observe writes staged by earlier calls on this handle.
    pub fn execute<'a>(
        &'a mut self,
        sql: &'a str,
        params: &'a [Value],
    ) -> TransactionExecuteBuilder<'a, StorageImpl> {
        TransactionExecuteBuilder {
            transaction: self,
            sql,
            params,
            options: ExecuteOptions::default(),
        }
    }

    /// Executes one prepared DML page atomically inside this transaction.
    /// Shape changes and dependency barriers remain explicit `execute` calls.
    pub(crate) async fn execute_prepared_dml_batch(
        &mut self,
        sql: Arc<str>,
        parameter_batch: PreparedDmlParameterBatch,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        self.inner
            .execute_prepared_dml_batch(sql, parameter_batch)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn stage_test_row(
        &mut self,
        row: TransactionWriteRow,
    ) -> Result<(), LixError> {
        self.inner.stage_test_row(row).await
    }

    pub async fn commit(self) -> Result<(), LixError> {
        self.inner.commit().await
    }

    pub async fn rollback(self) -> Result<(), LixError> {
        self.inner.rollback().await
    }
}

pub(crate) async fn open_or_initialize_engine<StorageImpl>(
    storage: StorageImpl,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
    plugin_resource_limits: Option<(u64, usize)>,
) -> Result<Engine<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    match new_engine(
        storage.clone(),
        wasm_runtime.clone(),
        telemetry.clone(),
        plugin_resource_limits,
    )
    .await
    {
        Ok(engine) => Ok(engine),
        Err(error) if error.code == "LIX_ERROR_NOT_INITIALIZED" => {
            Engine::initialize(storage.clone()).await?;
            new_engine(storage, wasm_runtime, telemetry, plugin_resource_limits).await
        }
        Err(error) => Err(error),
    }
}

async fn new_engine<StorageImpl>(
    storage: StorageImpl,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
    plugin_resource_limits: Option<(u64, usize)>,
) -> Result<Engine<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    #[cfg(feature = "default_wasm_runtime")]
    let wasm_runtime = match wasm_runtime {
        Some(wasm_runtime) => Some(wasm_runtime),
        None => Some(crate::plugin::runtime::default::runtime()?),
    };
    let mut options = EngineOptions::new();
    if let Some(wasm_runtime) = wasm_runtime {
        options = options.with_wasm_runtime(wasm_runtime);
    }
    if let Some(telemetry) = telemetry {
        options = options.with_telemetry(telemetry);
    }
    if let Some((max_memory_bytes, max_live_stores)) = plugin_resource_limits {
        options = options.with_plugin_resource_limits(max_memory_bytes, max_live_stores);
    }
    Engine::new_with_options(storage, options).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sessions_share_one_engine_but_have_independent_lifecycles() {
        let root = open_lix().await.expect("open root Lix");
        let first = root
            .open_another_session()
            .await
            .expect("open first child session");
        let second = root
            .open_another_session()
            .await
            .expect("open second child session");

        first.close().await.expect("close first child session");
        let error = first
            .execute("SELECT 1", &[])
            .await
            .expect_err("closed child session must reject work");
        assert_eq!(error.code, LixError::CODE_CLOSED);

        second
            .execute("SELECT 2", &[])
            .await
            .expect("second child remains open");
        root.execute("SELECT 3", &[])
            .await
            .expect("root remains open");
    }

    #[tokio::test]
    async fn sessions_validate_and_retain_branch_switches() {
        let root = open_lix().await.expect("open root Lix");
        let main_branch_id = root.active_branch_id().await.expect("main branch");
        let draft = root
            .create_branch(CreateBranchOptions {
                id: Some("01920000-0000-7000-8000-000000000501".to_string()),
                name: "Pinned draft".to_string(),
                from_commit_id: None,
            })
            .await
            .expect("create draft");

        let session = root
            .open_another_session()
            .await
            .expect("open main session");
        let session_clone = session.clone();
        let receipt = session
            .switch_branch(SwitchBranchOptions {
                branch_id: draft.id.clone(),
            })
            .await
            .expect("switch session");

        assert_eq!(receipt.branch_id, draft.id);
        assert_eq!(
            session.active_branch_id().await.unwrap(),
            "01920000-0000-7000-8000-000000000501"
        );
        assert_eq!(
            session_clone.active_branch_id().await.unwrap(),
            "01920000-0000-7000-8000-000000000501"
        );
        assert_eq!(root.active_branch_id().await.unwrap(), main_branch_id);

        let error = session
            .switch_branch(SwitchBranchOptions {
                branch_id: "01920000-0000-7000-8000-000000000599".to_string(),
            })
            .await
            .expect_err("missing branch must not open");
        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
    }

    #[tokio::test]
    async fn accounts_are_mutable_and_changes_have_one_required_account() {
        const AUTHOR_ID: &str = "01920000-0000-7000-8000-000000000601";
        const UNUSED_ID: &str = "01920000-0000-7000-8000-000000000602";
        let root = open_lix().await.expect("open root Lix");

        root.ensure_account(AUTHOR_ID, "Ada", "human")
            .await
            .expect("provision author");
        root.ensure_account(UNUSED_ID, "Unused", "human")
            .await
            .expect("provision unused account");

        let author = root
            .open_another_session()
            .with_account(AUTHOR_ID)
            .await
            .expect("open attributed session");
        assert_eq!(author.active_account_id(), AUTHOR_ID);
        let inherited = author
            .open_another_session()
            .await
            .expect("open session inheriting the author");
        assert_eq!(inherited.active_account_id(), AUTHOR_ID);
        let active = author
            .execute("SELECT lix_active_account_id() AS account_id", &[])
            .await
            .expect("read SQL active account");
        assert_eq!(
            active.rows()[0].values(),
            &[Value::Text(AUTHOR_ID.to_string())]
        );

        author
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('account-test', CAST('true' AS JSONB))",
                &[],
            )
            .await
            .expect("write attributed change");
        let attribution = author
            .execute(
                "SELECT account_id FROM lix_change WHERE schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("query attribution");
        assert_eq!(
            attribution
                .rows()
                .last()
                .expect("attributed key-value change")
                .values(),
            &[Value::Text(AUTHOR_ID.to_string())]
        );

        let system = root
            .open_another_session()
            .with_account(lix::SYSTEM_ACCOUNT_ID)
            .await
            .expect("open system session");
        system
            .execute(
                "UPDATE lix_account_by_branch SET name = 'Ada Lovelace' \
                 WHERE id = $1 AND lixcol_branch_id = $2",
                &[
                    Value::Text(AUTHOR_ID.to_string()),
                    Value::Text(lix::GLOBAL_BRANCH_ID.to_string()),
                ],
            )
            .await
            .expect("rename account");
        let account = system
            .execute(
                "SELECT name FROM lix_account WHERE id = $1",
                &[Value::Text(AUTHOR_ID.to_string())],
            )
            .await
            .expect("read renamed account");
        assert_eq!(
            account.rows()[0].values(),
            &[Value::Text("Ada Lovelace".to_string())]
        );

        let unused = root
            .open_another_session()
            .with_account(UNUSED_ID)
            .await
            .expect("open unused account session");

        system
            .execute(
                "DELETE FROM lix_account_by_branch WHERE id = $1 AND lixcol_branch_id = $2",
                &[
                    Value::Text(UNUSED_ID.to_string()),
                    Value::Text(lix::GLOBAL_BRANCH_ID.to_string()),
                ],
            )
            .await
            .expect("delete unused account");
        let error = unused
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('deleted-account', CAST('true' AS JSONB))",
                &[],
            )
            .await
            .expect_err("deleted account must not keep writing through an open session");
        assert_eq!(error.code, "LIX_ACCOUNT_NOT_FOUND");
        let error = system
            .execute(
                "DELETE FROM lix_account_by_branch WHERE id = $1 AND lixcol_branch_id = $2",
                &[
                    Value::Text(AUTHOR_ID.to_string()),
                    Value::Text(lix::GLOBAL_BRANCH_ID.to_string()),
                ],
            )
            .await
            .expect_err("authored changes must restrict account deletion");
        assert_eq!(error.code, "LIX_FOREIGN_KEY_VIOLATION");

        system
            .execute(
                "UPDATE lix_account_by_branch SET status = 'disabled' \
                 WHERE id = $1 AND lixcol_branch_id = $2",
                &[
                    Value::Text(AUTHOR_ID.to_string()),
                    Value::Text(lix::GLOBAL_BRANCH_ID.to_string()),
                ],
            )
            .await
            .expect("disable author");
        let error = author
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('disabled-account', CAST('true' AS JSONB))",
                &[],
            )
            .await
            .expect_err("disabled account must not keep writing through an open session");
        assert_eq!(error.code, "LIX_ACCOUNT_DISABLED");

        let error = system
            .execute(
                "UPDATE lix_account_by_branch SET status = 'disabled' \
                 WHERE id = $1 AND lixcol_branch_id = $2",
                &[
                    Value::Text(lix::ANONYMOUS_ACCOUNT_ID.to_string()),
                    Value::Text(lix::GLOBAL_BRANCH_ID.to_string()),
                ],
            )
            .await
            .expect_err("built-in accounts must remain active");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn bootstrap_accounts_are_global_rows_inherited_by_branches() {
        const AUTHOR_ID: &str = "01920000-0000-7000-8000-0000000006a1";
        let root = open_lix().await.expect("open root Lix");

        // SELECT on the default (main) session inherits the two built-ins.
        // Before this fix those rows were staged onto main as local copies
        // (`lixcol_global = false`) that shadowed the global rows.
        let accounts = root
            .execute(
                "SELECT id, name, lixcol_global FROM lix_account ORDER BY name",
                &[],
            )
            .await
            .expect("query accounts should succeed");
        assert_eq!(
            accounts.rows().len(),
            2,
            "should see exactly two bootstrap accounts"
        );
        for row in accounts.rows() {
            assert_eq!(
                &row.values()[2],
                &Value::Boolean(true),
                "bootstrap account should have lixcol_global=true"
            );
        }

        // Physical copies materialize as `lixcol_global = false`. Unfiltered
        // `*_by_branch` also *projects* inherited global rows onto every
        // visible branch with `lixcol_global = true` — that is the same
        // visibility overlay `ensure_account` uses, not a second seed write.
        let local_copies = root
            .execute(
                "SELECT id, lixcol_branch_id FROM lix_account_by_branch \
                 WHERE id IN ($1, $2) AND lixcol_global = false",
                &[
                    Value::Text(lix::SYSTEM_ACCOUNT_ID.to_string()),
                    Value::Text(lix::ANONYMOUS_ACCOUNT_ID.to_string()),
                ],
            )
            .await
            .expect("query local account copies should succeed");
        assert!(
            local_copies.rows().is_empty(),
            "built-in accounts must not have branch-local copies"
        );

        let home_rows = root
            .execute(
                "SELECT id, name, lixcol_global, lixcol_branch_id \
                 FROM lix_account_by_branch \
                 WHERE id IN ($1, $2) AND lixcol_branch_id = $3 \
                 ORDER BY name",
                &[
                    Value::Text(lix::SYSTEM_ACCOUNT_ID.to_string()),
                    Value::Text(lix::ANONYMOUS_ACCOUNT_ID.to_string()),
                    Value::Text(lix::GLOBAL_BRANCH_ID.to_string()),
                ],
            )
            .await
            .expect("query home account rows should succeed");
        assert_eq!(
            home_rows.rows().len(),
            2,
            "built-in accounts live on GLOBAL_BRANCH_ID"
        );
        for row in home_rows.rows() {
            let values = row.values();
            assert_eq!(&values[2], &Value::Boolean(true));
            assert_eq!(
                &values[3],
                &Value::Text(lix::GLOBAL_BRANCH_ID.to_string())
            );
        }

        // A later ensure_account write has the same physical/home shape.
        root.ensure_account(AUTHOR_ID, "Ada", "human")
            .await
            .expect("provision author");
        let author_copies = root
            .execute(
                "SELECT id FROM lix_account_by_branch \
                 WHERE id = $1 AND lixcol_global = false",
                &[Value::Text(AUTHOR_ID.to_string())],
            )
            .await
            .expect("query ensure_account copies should succeed");
        assert!(
            author_copies.rows().is_empty(),
            "ensure_account must not create branch-local copies either"
        );

        let draft = root
            .create_branch(CreateBranchOptions {
                id: None,
                name: "draft".to_string(),
                from_commit_id: None,
            })
            .await
            .expect("create draft branch");
        root.switch_branch(SwitchBranchOptions {
            branch_id: draft.id.clone(),
        })
        .await
        .expect("switch to draft branch");

        let draft_accounts = root
            .execute(
                "SELECT id, name, lixcol_global FROM lix_account ORDER BY name",
                &[],
            )
            .await
            .expect("query accounts on draft branch should succeed");
        assert_eq!(
            draft_accounts.rows().len(),
            3,
            "draft branch should inherit the two built-ins plus the ensure_account row"
        );
        for row in draft_accounts.rows() {
            assert_eq!(
                &row.values()[2],
                &Value::Boolean(true),
                "inherited account should still have lixcol_global=true on draft"
            );
        }

        let draft_local_copies = root
            .execute(
                "SELECT id FROM lix_account_by_branch \
                 WHERE id IN ($1, $2, $3) AND lixcol_global = false",
                &[
                    Value::Text(lix::SYSTEM_ACCOUNT_ID.to_string()),
                    Value::Text(lix::ANONYMOUS_ACCOUNT_ID.to_string()),
                    Value::Text(AUTHOR_ID.to_string()),
                ],
            )
            .await
            .expect("query draft local copies should succeed");
        assert!(
            draft_local_copies.rows().is_empty(),
            "creating a branch must not copy global accounts onto the new branch"
        );
    }
}


/// See `session::execute::assume_send_future_proofs`.
#[cfg(test)]
mod assume_send_future_proofs {
    use super::*;

    fn is_send<T: Send>(_: &T) {}

    // handle.rs -- OpenLixBuilder::into_future
    #[allow(dead_code)]
    fn open_lix_inner_is_send(
        storage: Memory,
        wasm_runtime: Option<Arc<dyn WasmRuntime>>,
        telemetry: Option<Arc<dyn TelemetrySink>>,
    ) {
        is_send(&open_lix_inner(storage, wasm_runtime, telemetry));
    }

    // handle.rs -- Lix::switch_branch (body mirrored verbatim)
    #[allow(dead_code)]
    fn switch_branch_body_is_send(lix: &Lix<Memory>, options: SwitchBranchOptions) {
        is_send(&async move {
            let _primary_switch_guard = match &lix.primary_switch_gate {
                Some(gate) => Some(gate.lock().await),
                None => None,
            };
            lix.session.switch_branch(options).await
        });
    }

    #[allow(dead_code)]
    fn lix_handle_is_send_for_every_storage<S>()
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Lix<S>>();
        assert_sync::<Lix<S>>();
    }
}
