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
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use crate::engine::{Engine, EngineOptions};
use crate::session::SessionContext;
use crate::session::{CoherentReadBatch, ExecuteOptions};
#[cfg(test)]
use crate::transaction_types::TransactionWriteRow;

/// Server behavior for an opened local Lix repository.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServerMode {
    /// Execute against local storage and continuously synchronize the active
    /// branch with the server.
    Sync,
}

/// Configures the server associated with a local Lix repository.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerOptions {
    pub mode: ServerMode,
    pub url: String,
    /// HTTP headers included on browser sync protocol requests.
    pub headers: Vec<(String, String)>,
}

impl ServerOptions {
    pub fn sync(url: impl Into<String>) -> Self {
        Self {
            mode: ServerMode::Sync,
            url: url.into(),
            headers: Vec::new(),
        }
    }

    /// Adds HTTP headers used by the sync transport, such as Authorization.
    pub fn with_headers(mut self, headers: impl IntoIterator<Item = (String, String)>) -> Self {
        self.headers = headers.into_iter().collect();
        self
    }
}

/// Configures the primary session for a Lix repository.
///
/// The default builder opens an in-memory Lix. Configure a persistent adapter
/// with [`OpenLixBuilder::with_storage`] and then await the builder.
#[expect(missing_debug_implementations)]
pub struct OpenLixBuilder<StorageImpl = Memory> {
    storage: StorageImpl,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
    server: Option<ServerOptions>,
    skip_session_bind: bool,
}

impl Default for OpenLixBuilder<Memory> {
    fn default() -> Self {
        Self {
            storage: Memory::new(),
            wasm_runtime: None,
            telemetry: None,
            server: None,
            skip_session_bind: false,
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
            server: self.server,
            skip_session_bind: self.skip_session_bind,
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

    /// Runs this repository as a local replica of `server`.
    pub fn with_server(mut self, server: ServerOptions) -> Self {
        self.server = Some(server);
        self
    }

    /// Attaches a sink (if any) without emitting `lix.opened`.
    ///
    /// Use this when the handle is only a protocol root or cached runtime
    /// that later protocol sessions inherit. Handshake session creation and
    /// explicit [`Lix::bind_session`] remain the client bind.
    pub fn as_protocol_root(mut self) -> Self {
        self.skip_session_bind = true;
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
                open_lix_inner(
                    self.storage,
                    self.wasm_runtime,
                    self.telemetry,
                    self.server,
                    self.skip_session_bind,
                )
                .await
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
                    .retry_sync_demands(|| {
                        self.lix.session.execute_with_options(
                            &self.sql,
                            &self.params,
                            self.options.clone(),
                        )
                    })
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
                    .retry_sync_demands(|| {
                        self.lix
                            .session
                            .execute_batch_with_options(&self.statements, self.options.clone())
                    })
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
    sync_lease: Option<Arc<SyncSessionLease>>,
    sync_demand_tx: Option<tokio::sync::mpsc::Sender<crate::sync::SyncDemand>>,
}

#[derive(Debug)]
struct SyncSessionLease {
    runtime: Arc<crate::sync::SyncRuntime>,
    active_sessions: Arc<AtomicUsize>,
    released: AtomicBool,
}

impl SyncSessionLease {
    fn root(runtime: Arc<crate::sync::SyncRuntime>) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            active_sessions: Arc::new(AtomicUsize::new(1)),
            released: AtomicBool::new(false),
        })
    }

    fn child(&self) -> Arc<Self> {
        self.active_sessions.fetch_add(1, Ordering::AcqRel);
        Arc::new(Self {
            runtime: self.runtime.clone(),
            active_sessions: self.active_sessions.clone(),
            released: AtomicBool::new(false),
        })
    }

    async fn release(&self) -> Result<(), LixError> {
        if self.released.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if self.active_sessions.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.runtime.drain_and_join().await?;
        }
        Ok(())
    }
}

async fn open_lix_inner<StorageImpl>(
    storage: StorageImpl,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
    server: Option<ServerOptions>,
    skip_session_bind: bool,
) -> Result<Lix<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    // A fresh repository or one left in the initialization/bootstrap crash
    // window needs one handshake and snapshot before its application session
    // can be bound to the authority's account. Reopens with durable state for
    // this exact remote remain entirely local.
    let mut prepared_sync: Option<crate::sync::PreparedSync> = if let Some(server) = server.as_ref()
    {
        if sync_requires_preparation(&storage, server.url.trim_end_matches('/')).await? {
            Some(crate::sync::prepare_sync_mode(server).await?)
        } else {
            None
        }
    } else {
        None
    };
    let initial_sync_branch_id = prepared_sync
        .as_ref()
        .map(|prepared| prepared.default_branch_id.clone());
    let reopened_sync_account_id = if prepared_sync.is_none() {
        if let Some(server) = server.as_ref() {
            let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());
            let read = adapter
                .begin_read(crate::storage_adapter::StorageReadOptions::default())
                .await?;
            crate::sync::load_sync_replica_account(&read, server.url.trim_end_matches('/')).await?
        } else {
            None
        }
    } else {
        None
    };
    let engine = open_or_initialize_engine(
        storage,
        wasm_runtime,
        telemetry,
        None,
        initial_sync_branch_id.as_deref(),
    )
    .await?;
    let session = match reopened_sync_account_id {
        Some(account_id) => engine.open_session_with_account(account_id).await?,
        None => engine.open_session().await?,
    };
    let mut lix = Lix {
        engine: Arc::new(engine),
        session: Arc::new(session),
        primary_switch_gate: Some(Arc::new(tokio::sync::Mutex::new(()))),
        sync_lease: None,
        sync_demand_tx: None,
    };
    if let Some(server) = server {
        match server.mode {
            ServerMode::Sync => {
                let runtime =
                    crate::sync::activate_sync_mode(&mut lix, &server, prepared_sync.take())
                        .await?;
                lix.sync_demand_tx = Some(runtime.demand_tx.clone());
                lix.sync_lease = Some(SyncSessionLease::root(runtime));
            }
        }
    }
    if !skip_session_bind {
        lix.bind_session();
    }
    Ok(lix)
}

async fn sync_requires_preparation<StorageImpl>(
    storage: &StorageImpl,
    remote_id: &str,
) -> Result<bool, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(crate::storage_adapter::StorageReadOptions::default())
        .await?;
    if matches!(
        crate::init::repository_protocol_status(&read).await?,
        crate::init::RepositoryProtocolStatus::Missing
    ) {
        return Ok(true);
    }
    if crate::sync::load_sync_replica_account(&read, remote_id)
        .await?
        .is_some()
    {
        return Ok(false);
    }
    if crate::sync::has_any_sync_replica_state(&read).await? {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "an initialized sync replica cannot be rebound to a different remote",
        ));
    }
    Ok(true)
}

impl<StorageImpl> Lix<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    async fn retry_sync_demands<T, Operation, OperationFuture>(
        &self,
        mut operation: Operation,
    ) -> Result<T, LixError>
    where
        Operation: FnMut() -> OperationFuture,
        OperationFuture: Future<Output = Result<T, LixError>>,
    {
        let mut retry = crate::sync::SyncDemandRetry::default();
        loop {
            match operation().await {
                Err(error) => {
                    retry
                        .hydrate_for_retry(self.sync_demand_tx.as_ref(), error)
                        .await?;
                }
                result => return result,
            }
        }
    }

    pub(crate) fn storage_adapter(&self) -> crate::storage_adapter::StorageAdapter<StorageImpl> {
        self.engine.storage()
    }

    pub(crate) fn sync_mode_state(&self) -> crate::sync::SyncModeState {
        self.engine.sync_mode()
    }

    pub(crate) fn notify_observers_for_sync(&self) {
        self.engine.notify_observers();
    }

    pub(crate) fn fail_observers_for_sync(&self, error: LixError) {
        self.engine.fail_observers(error);
    }

    pub(crate) async fn repository_default_branch_id_for_sync(
        &self,
        read: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
    ) -> Result<String, LixError> {
        self.engine.load_repository_default_branch_id(read).await
    }

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
        let mut opened = self
            .open_internal_session(active_branch_id, active_account_id)
            .await?;
        opened.sync_lease = self.sync_lease.as_ref().map(|lease| lease.child());
        Ok(opened)
    }

    pub(crate) async fn open_internal_session(
        &self,
        active_branch_id: impl Into<String>,
        active_account_id: impl Into<String>,
    ) -> Result<Self, LixError> {
        self.open_internal_session_with_sync_suppression(active_branch_id, active_account_id, false)
            .await
    }

    pub(crate) async fn open_internal_session_suppressed(
        &self,
        active_branch_id: impl Into<String>,
        active_account_id: impl Into<String>,
    ) -> Result<Self, LixError> {
        self.open_internal_session_with_sync_suppression(active_branch_id, active_account_id, true)
            .await
    }

    async fn open_internal_session_with_sync_suppression(
        &self,
        active_branch_id: impl Into<String>,
        active_account_id: impl Into<String>,
        suppress_sync_outbox: bool,
    ) -> Result<Self, LixError> {
        use tracing::Instrument as _;
        async move {
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
            let mut session = session;
            session.set_sync_outbox_suppressed(suppress_sync_outbox);
            Ok(Self {
                engine: self.engine.clone(),
                session: Arc::new(session),
                primary_switch_gate: None,
                sync_lease: None,
                sync_demand_tx: self.sync_demand_tx.clone(),
            })
        }
        .instrument(tracing::info_span!(
            target: "lix",
            "lix.session.open"
        ))
        .await
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
    ///
    /// `sql` must be a single statement. To run several statements atomically,
    /// pass an array of statements to [`Self::execute_batch`]. Do not concatenate
    /// statements into one script string.
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
        let path = path.into();
        self.retry_sync_demands(|| self.session.read_file_content(path.clone(), range.clone()))
            .await
    }

    pub(crate) async fn execute_with_options_and_metadata(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
        metadata: ExecuteStatementMetadata,
    ) -> Result<ExecuteResult, LixError> {
        self.retry_sync_demands(|| {
            self.session.execute_with_options_and_metadata(
                sql,
                params,
                options.clone(),
                metadata.clone(),
            )
        })
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
        // SAFETY: the retry future owns the Lix handle and every request
        // value. Reusing the same idempotency identity on each attempt is the
        // required contract: a pre-commit history demand has no receipt, while
        // an already committed attempt replays its durable receipt.
        unsafe {
            crate::session::AssumeSendFuture::new(async move {
                self.retry_sync_demands(|| {
                    Arc::clone(&self.session).execute_with_idempotency_and_options_and_metadata(
                        sql.clone(),
                        params.clone(),
                        options.clone(),
                        metadata.clone(),
                        idempotency.clone(),
                    )
                })
                .await
            })
        }
    }

    /// Executes statements sequentially against one atomic snapshot.
    /// Pure reads share one read snapshot; batches containing writes retain
    /// transactional read-after-write and rollback semantics.
    ///
    /// Each entry is one statement plus its own parameters. Callers assemble
    /// the array; Lix does not parse a multi-statement script on their behalf.
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
        self.retry_sync_demands(|| {
            self.session.execute_batch_with_options_and_metadata(
                statements,
                options.clone(),
                statement_metadata.clone(),
            )
        })
        .await
    }

    pub(crate) fn execute_batch_with_idempotency_and_options_and_metadata(
        self: Arc<Self>,
        statements: Vec<ExecuteBatchStatement>,
        options: ExecuteOptions,
        statement_metadata: Vec<ExecuteStatementMetadata>,
        idempotency: Option<ExecuteIdempotency>,
    ) -> impl Future<Output = Result<Vec<ExecuteResult>, LixError>> + Send + 'static {
        // Preserve the exact request identity across lazy-history retries so a
        // commit-outcome-unknown response can still be retried safely with the
        // caller's original key.
        unsafe {
            crate::session::AssumeSendFuture::new(async move {
                self.retry_sync_demands(|| {
                    Arc::clone(&self.session)
                        .execute_batch_with_idempotency_and_options_and_metadata(
                            statements.clone(),
                            options.clone(),
                            statement_metadata.clone(),
                            idempotency.clone(),
                        )
                })
                .await
            })
        }
    }

    #[cfg(test)]
    pub(crate) fn set_sync_demand_sender_for_test(
        &mut self,
        sender: tokio::sync::mpsc::Sender<crate::sync::SyncDemand>,
    ) {
        self.sync_demand_tx = Some(sender);
    }

    pub fn observe(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<ObserveEvents<StorageImpl>, LixError> {
        self.session
            .observe(sql, params)
            .map(|events| events.with_sync_demand_sender(self.sync_demand_tx.clone()))
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

    /// Repository identity stored as `lix_key_value.lix_id`.
    pub fn lix_id(&self) -> &str {
        self.engine.lix_id()
    }

    /// Per-engine telemetry sink, if the host attached one.
    pub fn telemetry(&self) -> Option<&Arc<dyn TelemetrySink>> {
        self.engine.telemetry()
    }

    /// Records that this handle's session has bound to the repository.
    ///
    /// In-process [`open_lix`] and protocol handshake session creation call
    /// this once. Protocol roots opened with
    /// [`OpenLixBuilder::as_protocol_root`] skip it so attaching a sink does
    /// not emit for the internal handle. Hosts that mint a session against an
    /// already-open runtime should call the same helper instead of opening
    /// another engine.
    pub fn bind_session(&self) {
        let Ok(branch_id) = self.session.bound_branch_id() else {
            return;
        };
        crate::telemetry::bind_session(
            self.telemetry(),
            self.lix_id(),
            &branch_id,
            Some(self.active_account_id()),
        );
    }

    /// Creates an active global account if it does not exist. Existing mutable
    /// account fields are deliberately left unchanged.
    pub(crate) async fn ensure_account(
        &self,
        id: &str,
        name: &str,
        kind: &str,
    ) -> Result<(), LixError> {
        let system = Box::pin(
            self.open_internal_session(lix::GLOBAL_BRANCH_ID.to_string(), lix::SYSTEM_ACCOUNT_ID),
        )
        .await?;
        system
            .execute(
                "INSERT INTO lix_account \
                 (id, name, kind, status, lixcol_global, lixcol_untracked) \
                 VALUES ($1, $2, $3, 'active', true, false) \
                 ON CONFLICT (id) \
                 DO NOTHING",
                &[
                    Value::Text(id.to_string()),
                    Value::Text(name.to_string()),
                    Value::Text(kind.to_string()),
                ],
            )
            .await?;
        Box::pin(system.close()).await
    }

    pub async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError> {
        self.retry_sync_demands(|| self.session.create_branch(options.clone()))
            .await
    }

    pub async fn create_checkpoint(&self) -> Result<CreateCheckpointReceipt, LixError> {
        self.session.create_checkpoint().await
    }

    /// Reverses the latest undoable tracked commit on this handle's active branch.
    pub async fn undo(&self) -> Result<UndoReceipt, LixError> {
        self.retry_sync_demands(|| self.session.undo()).await
    }

    /// Replays the latest tracked commit abandoned by undo on this handle's active branch.
    pub async fn redo(&self) -> Result<RedoReceipt, LixError> {
        self.retry_sync_demands(|| self.session.redo()).await
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
        self.retry_sync_demands(|| self.session.merge_branch(options.clone()))
            .await
    }

    pub(crate) async fn replay_execute_idempotency_result(
        &self,
        idempotency: &ExecuteIdempotency,
    ) -> Result<Option<ExecuteResult>, LixError> {
        self.session
            .replay_execute_idempotency_result(idempotency)
            .await
    }

    pub async fn merge_branch_preview(
        &self,
        options: MergeBranchPreviewOptions,
    ) -> Result<MergeBranchPreview, LixError> {
        self.retry_sync_demands(|| self.session.merge_branch_preview(options.clone()))
            .await
    }

    pub async fn close(&self) -> Result<(), LixError> {
        self.session.close().await?;
        if let Some(lease) = &self.sync_lease {
            lease.release().await?;
        }
        Ok(())
    }
    pub(crate) fn set_sync_role(&self, role: crate::sync::SyncRole) -> Result<(), LixError> {
        self.engine.sync_mode().set_role(role);
        Ok(())
    }

    pub(crate) async fn align_primary_account_for_sync(
        &mut self,
        active_account_id: &str,
    ) -> Result<(), LixError> {
        if self.active_account_id() == active_account_id {
            return Ok(());
        }
        let replacement = self
            .engine
            .open_session_with_account(active_account_id.to_owned())
            .await?;
        let previous = std::mem::replace(&mut self.session, Arc::new(replacement));
        previous.close().await
    }

    pub(crate) fn align_repository_identity_for_sync(
        &mut self,
        lix_id: String,
    ) -> Result<(), LixError> {
        let engine = Arc::get_mut(&mut self.engine).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync bootstrap cloned the engine before repository identity alignment",
            )
        })?;
        engine.set_lix_id_for_sync(lix_id);
        Ok(())
    }

    pub(crate) async fn lock_collaboration_writes(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.engine.collaboration_write_gate().lock_owned().await
    }

    pub(crate) async fn reconcile_sync_branch(
        &self,
        branch_id: &str,
        local_head_commit_id: &str,
        authority_head_commit_id: &str,
        authority_checkpoint_commit_id: &str,
    ) -> Result<(), LixError> {
        let authority_head =
            crate::changelog::CommitId::parse_lix(authority_head_commit_id, "sync authority head")?;
        // Adopt the authority checkpoint before merging. Both publications
        // are independently coherent: a crash can leave the local head
        // rebased onto the new shared checkpoint, and replay then completes
        // the merge without ever publishing a merged head with stale C.
        self.align_sync_branch_checkpoint(
            branch_id,
            local_head_commit_id,
            authority_checkpoint_commit_id,
        )
        .await?;
        // Reconciliation must not change the application's session-local
        // branch selection. A short-lived session targets the affected ref and
        // shares the engine's ordinary transaction/commit machinery.
        let session = self
            .engine
            .open_session_with_account(self.active_account_id().to_owned())
            .await?;
        let result: Result<String, LixError> = async {
            session
                .switch_branch(SwitchBranchOptions {
                    branch_id: branch_id.to_owned(),
                })
                .await?;
            let merged_head = session
                .merge_sync_commit_into_active_branch(authority_head)
                .await?;
            Ok(merged_head)
        }
        .await;
        let close_result = session.close().await;
        result?;
        close_result?;
        Ok(())
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

    /// Executes one SQL statement inside this transaction with explicit options.
    ///
    /// Protocol handlers use this instead of the builder so they stay on the
    /// public transaction API without a raw `transaction.execute(` call site.
    pub(crate) fn execute_with_options(
        &mut self,
        sql: String,
        params: Vec<Value>,
        options: ExecuteOptions,
    ) -> impl Future<Output = Result<ExecuteResult, LixError>> + Send + '_ {
        self.inner.execute_with_options(sql, params, options)
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

    pub(crate) fn active_branch_id(&self) -> Result<&str, LixError> {
        self.inner.active_branch_id()
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
    initial_main_branch_id: Option<&str>,
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
            Engine::initialize_with_main_branch_id(storage.clone(), initial_main_branch_id).await?;
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
    use lix::telemetry::{
        CallbackTelemetrySink, CompletedTelemetrySpan, TelemetrySink, TelemetrySpanEnd,
        TelemetrySpanHandle, TelemetrySpanKind, TelemetrySpanStart,
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    fn opened_spans(spans: &[CompletedTelemetrySpan]) -> Vec<&CompletedTelemetrySpan> {
        spans
            .iter()
            .filter(|span| span.start.kind == TelemetrySpanKind::LixOpened)
            .collect()
    }

    fn attribute_string<'a>(span: &'a CompletedTelemetrySpan, key: &str) -> Option<&'a str> {
        span.start.attributes.iter().find_map(|attribute| {
            if attribute.key == key {
                match &attribute.value {
                    crate::telemetry::TelemetryValue::String(value) => Some(value.as_str()),
                    _ => None,
                }
            } else {
                None
            }
        })
    }

    #[tokio::test]
    async fn open_lix_emits_one_opened_span_when_a_sink_is_attached() {
        let spans = Arc::new(Mutex::new(Vec::<CompletedTelemetrySpan>::new()));
        let captured = Arc::clone(&spans);
        let telemetry = Arc::new(CallbackTelemetrySink::new(move |span| {
            captured.lock().expect("spans").push(span);
        }));
        let lix = open_lix()
            .with_telemetry(telemetry)
            .await
            .expect("open Lix");
        let branch_id = lix.active_branch_id().await.expect("branch");
        let reused = lix.clone();
        reused
            .execute("SELECT 1", &[])
            .await
            .expect("reuse should execute");
        let _another = lix
            .open_another_session()
            .await
            .expect("another session should open");

        let spans = spans.lock().expect("spans");
        let opened = opened_spans(&spans);
        assert_eq!(opened.len(), 1);
        assert_eq!(opened[0].start.name, "lix.opened");
        assert_eq!(attribute_string(opened[0], "lix.id"), Some(lix.lix_id()));
        assert_eq!(
            attribute_string(opened[0], "lix.branch_id"),
            Some(branch_id.as_str())
        );
        assert_eq!(
            attribute_string(opened[0], "lix.account_id"),
            Some(lix.active_account_id())
        );
        assert!(
            spans
                .iter()
                .any(|span| span.start.kind == TelemetrySpanKind::SqlQuery),
            "SQL spans still work after an opened span"
        );
    }

    #[tokio::test]
    async fn open_lix_without_a_sink_emits_no_spans() {
        let lix = open_lix().await.expect("open Lix");
        lix.execute("SELECT 1", &[]).await.expect("execute");
        assert!(lix.telemetry().is_none());
    }

    #[tokio::test]
    async fn disabled_opened_kind_does_no_opened_span_work() {
        struct SqlOnlySink {
            started: Mutex<Vec<TelemetrySpanKind>>,
        }

        impl SqlOnlySink {
            fn into_sink(self: Arc<Self>) -> Arc<dyn TelemetrySink> {
                self
            }
        }

        impl TelemetrySink for SqlOnlySink {
            fn enabled(&self, kind: TelemetrySpanKind) -> bool {
                !matches!(kind, TelemetrySpanKind::LixOpened)
            }

            fn start_span(&self, start: TelemetrySpanStart) -> Box<dyn TelemetrySpanHandle> {
                assert_ne!(
                    start.kind,
                    TelemetrySpanKind::LixOpened,
                    "disabled opened spans must not be started"
                );
                self.started.lock().expect("started").push(start.kind);
                Box::new(NoopHandle)
            }
        }

        struct NoopHandle;

        impl TelemetrySpanHandle for NoopHandle {
            fn enter(&self) -> Box<dyn crate::telemetry::TelemetrySpanEnterGuard + '_> {
                Box::new(())
            }

            fn finish(self: Box<Self>, _end: TelemetrySpanEnd) {}
        }

        let sink = Arc::new(SqlOnlySink {
            started: Mutex::new(Vec::new()),
        });
        let lix = open_lix()
            .with_telemetry(Arc::clone(&sink).into_sink())
            .await
            .expect("open Lix");
        lix.execute("SELECT 1", &[]).await.expect("execute");
        let started = sink.started.lock().expect("started");
        assert!(
            started
                .iter()
                .all(|kind| *kind != TelemetrySpanKind::LixOpened)
        );
        assert!(started.contains(&TelemetrySpanKind::SqlQuery));
    }

    #[tokio::test]
    async fn protocol_root_open_attaches_sink_without_emitting_opened() {
        let spans = Arc::new(Mutex::new(Vec::<CompletedTelemetrySpan>::new()));
        let captured = Arc::clone(&spans);
        let telemetry = Arc::new(CallbackTelemetrySink::new(move |span| {
            captured.lock().expect("spans").push(span);
        }));
        let lix = open_lix()
            .with_telemetry(telemetry)
            .as_protocol_root()
            .await
            .expect("open protocol root");
        lix.execute("SELECT 1", &[]).await.expect("execute");

        assert!(lix.telemetry().is_some());
        let spans = spans.lock().expect("spans");
        assert!(opened_spans(&spans).is_empty());
        assert!(
            spans
                .iter()
                .any(|span| span.start.kind == TelemetrySpanKind::SqlQuery),
            "SQL spans still work on a protocol root"
        );
    }

    #[tokio::test]
    async fn protocol_root_then_explicit_bind_emits_one_opened_span() {
        let spans = Arc::new(Mutex::new(Vec::<CompletedTelemetrySpan>::new()));
        let captured = Arc::clone(&spans);
        let telemetry = Arc::new(CallbackTelemetrySink::new(move |span| {
            captured.lock().expect("spans").push(span);
        }));
        let lix = open_lix()
            .with_telemetry(telemetry)
            .as_protocol_root()
            .await
            .expect("open protocol root");
        assert!(opened_spans(&spans.lock().expect("spans")).is_empty());

        lix.bind_session();
        let spans = spans.lock().expect("spans");
        let opened = opened_spans(&spans);
        assert_eq!(opened.len(), 1);
        assert_eq!(opened[0].start.name, "lix.opened");
        assert_eq!(attribute_string(opened[0], "lix.id"), Some(lix.lix_id()));
    }

    #[tokio::test]
    async fn host_can_bind_an_already_open_runtime_without_opening_another_engine() {
        let spans = Arc::new(Mutex::new(Vec::<CompletedTelemetrySpan>::new()));
        let captured = Arc::clone(&spans);
        let telemetry = Arc::new(CallbackTelemetrySink::new(move |span| {
            captured.lock().expect("spans").push(span);
        }));
        let lix = open_lix()
            .with_telemetry(telemetry)
            .await
            .expect("open Lix");
        let first_id = lix.lix_id().to_owned();
        lix.bind_session();
        crate::telemetry::bind_session(
            lix.telemetry(),
            lix.lix_id(),
            &lix.active_branch_id().await.expect("branch"),
            Some(lix.active_account_id()),
        );

        let spans = spans.lock().expect("spans");
        let opened = opened_spans(&spans);
        assert_eq!(opened.len(), 3);
        assert!(opened.iter().all(|span| span.start.name == "lix.opened"));
        assert!(
            opened
                .iter()
                .all(|span| attribute_string(span, "lix.id") == Some(first_id.as_str()))
        );
    }

    #[tokio::test]
    async fn retries_distinct_sync_demands_until_the_operation_succeeds() {
        let mut lix = open_lix().await.expect("open Lix");
        let (demand_tx, mut demand_rx) = tokio::sync::mpsc::channel(4);
        lix.sync_demand_tx = Some(demand_tx);
        let responder = tokio::spawn(async move {
            for _ in 0..3 {
                demand_rx
                    .recv()
                    .await
                    .expect("demand should arrive")
                    .succeed_for_test();
            }
        });
        let attempts = AtomicUsize::new(0);
        let result = lix
            .retry_sync_demands(|| {
                let attempt = attempts.fetch_add(1, Ordering::Relaxed);
                std::future::ready(match attempt {
                    0 => Err(LixError::new(
                        "LIX_SYNC_HISTORY_REQUIRED",
                        "first history body is deferred",
                    )
                    .with_details(serde_json::json!({ "commitIds": ["first"] }))),
                    1 => Err(LixError::new(
                        "LIX_SYNC_HISTORY_REQUIRED",
                        "second history body is deferred",
                    )
                    .with_details(serde_json::json!({ "commitIds": ["second"] }))),
                    2 => Err(LixError::commit_not_found(
                        uuid::Uuid::now_v7().to_string(),
                        "walk_commit_graph",
                        "graph_node",
                    )),
                    _ => Ok("hydrated"),
                })
            })
            .await
            .expect("distinct demands should retry to success");
        assert_eq!(result, "hydrated");
        assert_eq!(attempts.load(Ordering::Relaxed), 4);
        responder.await.expect("demand responder should finish");
    }

    #[tokio::test]
    async fn initialized_store_without_remote_replica_state_prepares_synchronously() {
        let storage = Memory::new();
        Engine::initialize_with_main_branch_id(storage.clone(), None)
            .await
            .expect("simulate initialization before a crashed first bootstrap");
        assert!(
            sync_requires_preparation(&storage, "https://sync.example/repository")
                .await
                .expect("preparation decision should load"),
            "an initialized store without state for this remote must bind the account before open returns",
        );
    }

    #[tokio::test]
    async fn malformed_remote_replica_state_fails_closed() {
        let storage = Memory::new();
        Engine::initialize_with_main_branch_id(storage.clone(), None)
            .await
            .expect("initialize replica storage");
        let remote_id = "https://sync.example/repository";
        let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());
        let mut writes = adapter.new_write_set();
        writes.put(
            crate::sync::SYNC_REPLICA_STATE_SPACE,
            crate::storage_adapter::StorageKey(bytes::Bytes::copy_from_slice(remote_id.as_bytes())),
            serde_json::to_vec(&serde_json::json!({
                "activeAccountId": crate::ANONYMOUS_ACCOUNT_ID
            }))
            .expect("malformed replica state should encode"),
        );
        adapter
            .commit_write_set(
                writes,
                crate::storage_adapter::StorageWriteOptions::default(),
            )
            .await
            .expect("malformed replica state should commit");

        let error = sync_requires_preparation(&storage, remote_id)
            .await
            .expect_err("malformed exact-remote state must not trigger a fresh bootstrap");
        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(error.message.contains("decode sync replica state"));
        assert!(error.message.contains("missing field `cursor`"));
    }

    #[tokio::test]
    async fn initialized_replica_reopens_only_for_its_exact_remote() {
        let storage = Memory::new();
        Engine::initialize_with_main_branch_id(storage.clone(), None)
            .await
            .expect("initialize replica storage");
        let remote_id = "https://sync.example/repository";
        let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());
        let mut writes = adapter.new_write_set();
        writes.put(
            crate::sync::SYNC_REPLICA_STATE_SPACE,
            crate::storage_adapter::StorageKey(bytes::Bytes::copy_from_slice(remote_id.as_bytes())),
            serde_json::to_vec(&serde_json::json!({
                "activeAccountId": crate::ANONYMOUS_ACCOUNT_ID,
                "cursor": 0,
                "authoritativeBranches": {},
                "authorityKnownCommitIds": []
            }))
            .expect("replica state should encode"),
        );
        adapter
            .commit_write_set(
                writes,
                crate::storage_adapter::StorageWriteOptions::default(),
            )
            .await
            .expect("replica state should commit");

        assert!(
            !sync_requires_preparation(&storage, remote_id)
                .await
                .expect("exact remote decision should load"),
            "a durable exact-remote replica must reopen without network preparation",
        );
        let error = sync_requires_preparation(&storage, "https://sync.example/other-repository")
            .await
            .expect_err("an initialized replica cannot silently change authorities");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("different remote"));
    }

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
            .with_branch(lix::GLOBAL_BRANCH_ID)
            .with_account(lix::SYSTEM_ACCOUNT_ID)
            .await
            .expect("open system session");
        system
            .execute(
                "UPDATE lix_account SET name = 'Ada Lovelace' WHERE id = $1",
                &[Value::Text(AUTHOR_ID.to_string())],
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
                "DELETE FROM lix_account WHERE id = $1",
                &[Value::Text(UNUSED_ID.to_string())],
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
                "DELETE FROM lix_account WHERE id = $1",
                &[Value::Text(AUTHOR_ID.to_string())],
            )
            .await
            .expect_err("authored changes must restrict account deletion");
        assert_eq!(error.code, "LIX_FOREIGN_KEY_VIOLATION");

        system
            .execute(
                "UPDATE lix_account SET status = 'disabled' WHERE id = $1",
                &[Value::Text(AUTHOR_ID.to_string())],
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
                "UPDATE lix_account SET status = 'disabled' WHERE id = $1",
                &[Value::Text(lix::ANONYMOUS_ACCOUNT_ID.to_string())],
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

        let global = root
            .open_another_session()
            .with_branch(lix::GLOBAL_BRANCH_ID)
            .await
            .expect("global session should open");
        let home_rows = global
            .execute(
                "SELECT id, name, lixcol_global \
                 FROM lix_account \
                 WHERE id IN ($1, $2) \
                 ORDER BY name",
                &[
                    Value::Text(lix::SYSTEM_ACCOUNT_ID.to_string()),
                    Value::Text(lix::ANONYMOUS_ACCOUNT_ID.to_string()),
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
        }

        // A later ensure_account write has the same physical/home shape.
        root.ensure_account(AUTHOR_ID, "Ada", "human")
            .await
            .expect("provision author");
        let author_rows = global
            .execute(
                "SELECT id, lixcol_global FROM lix_account WHERE id = $1",
                &[Value::Text(AUTHOR_ID.to_string())],
            )
            .await
            .expect("query ensure_account row should succeed");
        assert_eq!(author_rows.rows().len(), 1);
        assert_eq!(author_rows.rows()[0].values()[1], Value::Boolean(true));

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
        is_send(&open_lix_inner(
            storage,
            wasm_runtime,
            telemetry,
            None,
            false,
        ));
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
