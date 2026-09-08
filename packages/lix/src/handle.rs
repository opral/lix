use lix::plugin::runtime::WasmRuntime;
use lix::storage::{Storage, StorageSession};
use lix::telemetry::TelemetrySink;
use lix::{
    Blob, CreateBranchOptions, CreateBranchReceipt, ExecuteBatchStatement, ExecuteIdempotency,
    ExecuteResult, ExecuteStatementMetadata, ExecutionDisposition, LixError, Memory,
    MergeBranchOptions, MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    ObserveEvent, RedoReceipt, SwitchBranchOptions, SwitchBranchReceipt, UndoReceipt, Value,
};
use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
    },
};

use crate::authority_client::{
    ClientCore, ProtocolClient, ProtocolExecuteOptions, ProtocolObserveEvents,
    ProtocolTransaction, open_protocol_client,
};
use crate::common::ExpiredReadRetryState;
use crate::engine::{Engine, EngineOptions};
use crate::open_types::{
    OpenMigrationReport, OpenPhase, OpenProgress, OpenProgressSink, OpenReport, emit_open_progress,
};

use crate::session::SessionContext;
use crate::session::{CoherentReadBatch, ExecuteOptions};
#[cfg(test)]
use crate::transaction_types::TransactionWriteRow;

/// Adapts a Rust closure to [`OpenProgressSink`].
#[expect(missing_debug_implementations)]
pub struct CallbackOpenProgressSink<F> {
    callback: F,
}

impl<F> CallbackOpenProgressSink<F>
where
    F: Fn(OpenProgress) + Send + Sync,
{
    pub fn new(callback: F) -> Self {
        Self { callback }
    }
}

impl<F> OpenProgressSink for CallbackOpenProgressSink<F>
where
    F: Fn(OpenProgress) + Send + Sync,
{
    fn report(&self, progress: OpenProgress) {
        (self.callback)(progress);
    }
}

struct RetainingOpenProgressSink {
    downstream: Option<Arc<dyn OpenProgressSink>>,
    migrated_from: AtomicU32,
    initialized: AtomicBool,
}

impl RetainingOpenProgressSink {
    fn new(downstream: Option<Arc<dyn OpenProgressSink>>) -> Self {
        Self {
            downstream,
            migrated_from: AtomicU32::new(0),
            initialized: AtomicBool::new(false),
        }
    }

    fn migrated_from(&self) -> Option<u32> {
        match self.migrated_from.load(Ordering::Acquire) {
            0 => None,
            version => Some(version),
        }
    }

    fn initialized(&self) -> bool {
        self.initialized.load(Ordering::Acquire)
    }

    fn retain_initialized(&self, initialized: bool) {
        if initialized {
            self.initialized.store(true, Ordering::Release);
        }
    }
}

impl OpenProgressSink for RetainingOpenProgressSink {
    fn report(&self, mut progress: OpenProgress) {
        if let Some(from_format) = progress.from_format {
            self.migrated_from.store(from_format, Ordering::Release);
        } else if matches!(progress.phase, OpenPhase::Opening | OpenPhase::Complete) {
            progress.from_format = self.migrated_from();
        }
        if let Some(downstream) = &self.downstream {
            downstream.report(progress);
        }
    }
}

/// Connection information for a hosted Lix repository.
///
/// A server alone selects remote execution. Adding explicit local storage selects synchronization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerOptions {
    pub url: String,
    /// HTTP headers included on server protocol requests.
    pub headers: Vec<(String, String)>,
}

impl ServerOptions {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: Vec::new(),
        }
    }

    /// Adds HTTP headers used by the server transport, such as Authorization.
    pub fn with_headers(mut self, headers: impl IntoIterator<Item = (String, String)>) -> Self {
        self.headers = headers.into_iter().collect();
        self
    }
}

/// Configures a session after local storage has been explicitly selected.
///
/// Start with [`open_lix`] and select storage with `with_storage`. Adding a
/// server to this builder selects synchronization.
#[expect(missing_debug_implementations)]
pub struct OpenLixBuilder<StorageImpl = Memory> {
    storage: StorageImpl,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
    server: Option<ServerOptions>,
    open_progress: Option<Arc<dyn OpenProgressSink>>,
}

impl OpenLixBuilder<Memory> {
    fn memory() -> Self {
        Self {
            storage: Memory::new(),
            wasm_runtime: None,
            telemetry: None,
            server: None,
            open_progress: None,
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
            open_progress: self.open_progress,
        }
    }

    /// Restores a verified snapshot into the selected fresh storage before
    /// opening it. This is a terminal builder step.
    pub fn from_snapshot<Source>(
        self,
        source: Source,
    ) -> OpenLixFromSnapshotBuilder<StorageImpl, Source> {
        OpenLixFromSnapshotBuilder { open: self, source }
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
    ///
    /// Sync replicas require a storage adapter that implements durable reads.
    /// The default in-memory adapter is intentionally not supported because it
    /// cannot prove that a bootstrap snapshot survived its publication fence.
    pub fn with_server(mut self, server: ServerOptions) -> Self {
        self.server = Some(server);
        self
    }

    /// Observes automatic repository inspection, migration, and opening.
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), lix::LixError> {
    /// use std::sync::Arc;
    /// let sink = lix::CallbackOpenProgressSink::new(|progress| {
    ///     eprintln!("opening: {:?}", progress.phase);
    /// });
    /// let lix = lix::open_lix()
    ///     .with_open_progress_sink(Arc::new(sink))
    ///     .await?;
    /// # lix.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_open_progress_sink(mut self, sink: Arc<dyn OpenProgressSink>) -> Self {
        self.open_progress = Some(sink);
        self
    }

    /// Opens the repository as a canonical Lix Server Protocol session factory.
    ///
    /// Serving owns the repository engine directly and creates no application
    /// session. Each successful protocol handshake retains exactly one
    /// application session.
    #[cfg(feature = "server-protocol")]
    pub fn serve(self) -> crate::server_protocol::ServeLixBuilder<StorageImpl> {
        crate::server_protocol::ServeLixBuilder::new(self)
    }
}

#[cfg(feature = "server-protocol")]
impl<StorageImpl> OpenLixBuilder<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) async fn open_protocol_engine(
        self,
    ) -> Result<Engine<StorageSession<StorageImpl>>, LixError> {
        if self.server.is_some() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "a Lix Server Protocol authority cannot also be a sync replica",
            ));
        }
        let storage = StorageSession::acquire(self.storage).await?;
        let retained_progress = Arc::new(RetainingOpenProgressSink::new(self.open_progress));
        let (engine, migrated_from) = retry_expired_read(|| {
            let storage = storage.clone();
            let open_progress: Arc<dyn OpenProgressSink> = retained_progress.clone();
            let wasm_runtime = self.wasm_runtime.clone();
            let telemetry = self.telemetry.clone();
            async move {
                let admission = ensure_current_repository(&storage, Some(&open_progress)).await?;
                let migrated_from = admission
                    .report
                    .migration
                    .map(|migration| migration.from_format);
                emit_open_progress(
                    Some(&open_progress),
                    OpenProgress {
                        phase: OpenPhase::Opening,
                        from_format: migrated_from,
                        to_format: crate::init::CURRENT_FORMAT_VERSION,
                        completed: None,
                        total: None,
                    },
                );
                let (engine, _) = open_or_initialize_engine_with_adapter(
                    admission.adapter,
                    wasm_runtime,
                    telemetry,
                    None,
                    None,
                )
                .await?;
                let engine_storage = engine.storage();
                let read = engine_storage
                    .begin_read(crate::storage_adapter::StorageReadOptions::default())
                    .await?;
                if crate::sync::has_any_sync_replica_state(&read).await? {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "a persisted sync replica cannot be served as a protocol authority",
                    ));
                }
                Ok((engine, migrated_from))
            }
        })
        .await?;
        let migrated_from = migrated_from.or_else(|| retained_progress.migrated_from());
        let open_progress: Arc<dyn OpenProgressSink> = retained_progress;
        emit_open_progress(
            Some(&open_progress),
            OpenProgress {
                phase: OpenPhase::Complete,
                from_format: migrated_from,
                to_format: crate::init::CURRENT_FORMAT_VERSION,
                completed: None,
                total: None,
            },
        );
        Ok(engine)
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
pub fn open_lix() -> UnconfiguredOpenLixBuilder {
    UnconfiguredOpenLixBuilder(OpenLixBuilder::memory())
}

/// An open request without explicitly selected storage.
/// Supplying only a server opens remote execution; supplying storage opens locally.
#[expect(missing_debug_implementations)]
pub struct UnconfiguredOpenLixBuilder(OpenLixBuilder<Memory>);

impl UnconfiguredOpenLixBuilder {
    pub fn with_storage<S>(self, storage: S) -> OpenLixBuilder<S> {
        self.0.with_storage(storage)
    }
    pub fn with_server(self, server: ServerOptions) -> RemoteOpenLixBuilder {
        RemoteOpenLixBuilder {
            open: self.0,
            server,
        }
    }
    pub fn with_wasm_runtime(mut self, runtime: Arc<dyn WasmRuntime>) -> Self {
        self.0 = self.0.with_wasm_runtime(runtime);
        self
    }
    pub fn with_telemetry(mut self, telemetry: Arc<dyn TelemetrySink>) -> Self {
        self.0 = self.0.with_telemetry(telemetry);
        self
    }
    pub fn with_open_progress_sink(mut self, sink: Arc<dyn OpenProgressSink>) -> Self {
        self.0 = self.0.with_open_progress_sink(sink);
        self
    }
    pub fn from_snapshot<S>(self, source: S) -> OpenLixFromSnapshotBuilder<Memory, S> {
        self.0.from_snapshot(source)
    }
    #[cfg(feature = "server-protocol")]
    pub fn serve(self) -> crate::server_protocol::ServeLixBuilder<Memory> {
        self.0.serve()
    }
}

impl IntoFuture for UnconfiguredOpenLixBuilder {
    type Output = Result<Lix<Memory>, LixError>;
    type IntoFuture = <OpenLixBuilder<Memory> as IntoFuture>::IntoFuture;
    fn into_future(self) -> Self::IntoFuture {
        self.0.into_future()
    }
}

/// An open request with a server but no explicitly selected local storage.
#[expect(missing_debug_implementations)]
pub struct RemoteOpenLixBuilder {
    open: OpenLixBuilder<Memory>,
    server: ServerOptions,
}
impl RemoteOpenLixBuilder {
    /// Selects a local replica with durable local writes and background sync.
    pub fn with_storage<S>(self, storage: S) -> OpenLixBuilder<S> {
        self.open.with_storage(storage).with_server(self.server)
    }
}
impl IntoFuture for RemoteOpenLixBuilder {
    type Output = Result<RemoteLix, LixError>;
    type IntoFuture = crate::sync::SyncTransportFuture<'static, RemoteLix>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
                if self.open.wasm_runtime.is_some()
                    || self.open.telemetry.is_some()
                    || self.open.open_progress.is_some()
                {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "remote execution cannot configure a local runtime, telemetry sink, or storage progress sink",
                    ));
                }
                let http = crate::sync::authority_http(&self.server.headers)?;
                let client = open_protocol_client(http, self.server.url, None).await?;
                let account_id = client.active_account_id().await?;
                Ok(RemoteLix { client, account_id })
        })
    }
}

/// A repository whose operations execute on its server.
#[derive(Debug, Clone)]
pub struct RemoteLix {
    account_id: String,
    client: ProtocolClient<crate::sync::AuthorityHttp>,
}
impl RemoteLix {
    /// Streams a complete snapshot from the server without allocating local storage.
    pub fn export_snapshot(&self) -> crate::snapshot::SnapshotExportBuilder<Memory> {
        crate::snapshot::SnapshotExportBuilder::remote(
            self.client.http().clone(),
            self.client.ensure_usable().and_then(|_| self.client.join_path("snapshot")),
            self.client.session_id(),
        )
    }

    pub fn execute<'a>(&'a self, sql: &'a str, params: &'a [Value]) -> RemoteExecuteBuilder<'a> {
        RemoteExecuteBuilder {
            lix: self,
            sql,
            params,
            options: ProtocolExecuteOptions::default(),
        }
    }
    pub async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError> {
        self.client.create_branch(options).await
    }
    pub async fn merge_branch(
        &self,
        options: MergeBranchOptions,
    ) -> Result<MergeBranchReceipt, LixError> {
        self.client.merge_branch(options).await
    }
    pub async fn merge_branch_preview(
        &self,
        options: MergeBranchPreviewOptions,
    ) -> Result<MergeBranchPreview, LixError> {
        self.client.merge_branch_preview(options).await
    }
    pub async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<SwitchBranchReceipt, LixError> {
        self.client
            .switch_branch_and_restart(&options.branch_id)
            .await
    }
    pub async fn undo(&self) -> Result<UndoReceipt, LixError> {
        self.client.undo().await
    }
    pub async fn redo(&self) -> Result<RedoReceipt, LixError> {
        self.client.redo().await
    }
    pub async fn begin_transaction(&self) -> Result<RemoteLixTransaction, LixError> {
        let client = self
            .client
            .open_another_session(None, Some(self.account_id.clone()))
            .await?;
        // Own the session before awaiting begin so failure or cancellation
        // schedules closure of any transaction the server may have started.
        let mut opened = RemoteLixTransaction {
            transaction: None,
            client: Some(client),
        };
        opened.transaction = Some(
            opened
                .client
                .as_ref()
                .ok_or_else(closed_transaction_error)?
                .begin_transaction()
                .await?,
        );
        Ok(opened)
    }

    pub fn observe(&self, sql: &str, params: &[Value]) -> Result<RemoteObserveEvents, LixError> {
        self.client.ensure_usable()?;
        Ok(RemoteObserveEvents {
            client: self.client.clone(),
            sql: sql.to_owned(),
            params: params.to_vec(),
            events: None,
            closed: false,
        })
    }
    pub fn open_another_session(&self) -> RemoteOpenAnotherSessionBuilder<'_> {
        RemoteOpenAnotherSessionBuilder {
            lix: self,
            account_id: None,
            branch_id: None,
        }
    }
    pub fn execute_batch<'a>(
        &'a self,
        statements: &'a [ExecuteBatchStatement],
    ) -> RemoteExecuteBatchBuilder<'a> {
        RemoteExecuteBatchBuilder {
            lix: self,
            statements,
            options: ProtocolExecuteOptions::default(),
        }
    }
    pub async fn active_branch_id(&self) -> Result<String, LixError> {
        self.client.active_branch_id().await
    }
    pub fn active_account_id(&self) -> &str {
        &self.account_id
    }
    pub async fn close(&self) -> Result<(), LixError> {
        self.client.close().await
    }
}

/// A transaction executing on the remote repository.
///
/// Each transaction owns a dedicated server session. Dropping it schedules
/// best-effort session closure, which rolls back unfinished work without
/// blocking the parent session. Server session expiry bounds abandoned work.
/// Call [`Self::rollback`] to await rollback and session closure explicitly.
#[derive(Debug)]
pub struct RemoteLixTransaction {
    transaction: Option<ProtocolTransaction<crate::sync::AuthorityHttp>>,
    client: Option<ProtocolClient<crate::sync::AuthorityHttp>>,
}
impl RemoteLixTransaction {
    pub fn execute<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Value],
    ) -> RemoteTransactionExecuteBuilder<'a> {
        RemoteTransactionExecuteBuilder {
            transaction: self,
            sql,
            params,
            options: ProtocolExecuteOptions::default(),
        }
    }
    pub async fn commit(mut self) -> Result<(), LixError> {
        let result = self
            .transaction
            .as_ref()
            .ok_or_else(closed_transaction_error)?
            .commit()
            .await;
        let close_result = self
            .client
            .as_ref()
            .ok_or_else(closed_transaction_error)?
            .close()
            .await;
        self.client.take();
        result?;
        close_result
    }
    pub async fn rollback(mut self) -> Result<(), LixError> {
        let result = self
            .transaction
            .as_ref()
            .ok_or_else(closed_transaction_error)?
            .rollback()
            .await;
        let close_result = self
            .client
            .as_ref()
            .ok_or_else(closed_transaction_error)?
            .close()
            .await;
        self.client.take();
        result?;
        close_result
    }
}

impl Drop for RemoteLixTransaction {
    fn drop(&mut self) {
        let Some(client) = self.client.take() else {
            return;
        };
        let http = client.http().clone();
        crate::authority_client::ProtocolHttp::spawn(
            &http,
            Box::pin(async move {
                let _ = client.close().await;
            }),
        );
    }
}

/// Configures an independent remote session.
#[derive(Debug)]
pub struct RemoteOpenAnotherSessionBuilder<'a> {
    lix: &'a RemoteLix,
    account_id: Option<String>,
    branch_id: Option<String>,
}
impl RemoteOpenAnotherSessionBuilder<'_> {
    pub fn with_account(mut self, account_id: impl Into<String>) -> Self {
        self.account_id = Some(account_id.into());
        self
    }
    pub fn with_branch(mut self, branch_id: impl Into<String>) -> Self {
        self.branch_id = Some(branch_id.into());
        self
    }
}
impl<'a> IntoFuture for RemoteOpenAnotherSessionBuilder<'a> {
    type Output = Result<RemoteLix, LixError>;
    type IntoFuture = crate::sync::SyncTransportFuture<'a, RemoteLix>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
                let client = self
                    .lix
                    .client
                    .open_another_session(self.branch_id, self.account_id)
                    .await?;
                let account_id = client.active_account_id().await?;
                Ok(RemoteLix { client, account_id })
        })
    }
}

/// Configures SQL executed inside a remote transaction.
#[derive(Debug)]
pub struct RemoteTransactionExecuteBuilder<'a> {
    transaction: &'a RemoteLixTransaction,
    sql: &'a str,
    params: &'a [Value],
    options: ProtocolExecuteOptions,
}
impl RemoteTransactionExecuteBuilder<'_> {
    pub fn with_origin_key(mut self, origin_key: impl Into<String>) -> Self {
        self.options.origin_key = Some(origin_key.into());
        self
    }
}
impl<'a> IntoFuture for RemoteTransactionExecuteBuilder<'a> {
    type Output = Result<ExecuteResult, LixError>;
    type IntoFuture = crate::sync::SyncTransportFuture<'a, ExecuteResult>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
                self.transaction
                    .transaction
                    .as_ref().ok_or_else(closed_transaction_error)?
                    .execute(self.sql, self.params, Some(self.options))
                    .await
        })
    }
}

/// Observation events streamed from the remote repository.
#[expect(missing_debug_implementations)]
pub struct RemoteObserveEvents {
    client: ProtocolClient<crate::sync::AuthorityHttp>,
    sql: String,
    params: Vec<Value>,
    events: Option<ProtocolObserveEvents<ClientCore<crate::sync::AuthorityHttp>>>,
    closed: bool,
}
impl RemoteObserveEvents {
    pub async fn next(&mut self) -> Result<Option<ObserveEvent>, LixError> {
        if self.closed {
            return Ok(None);
        }
        if self.events.is_none() {
            self.events = Some(self.client.observe(&self.sql, self.params.clone()).await?);
        }
        self.events
            .as_ref()
            .expect("observation registered")
            .next()
            .await
    }
    pub fn close(&mut self) {
        self.closed = true;
        if let Some(events) = self.events.take() {
            events.close();
        }
    }
}

/// Configures an atomic SQL batch on the server.
#[derive(Debug)]
pub struct RemoteExecuteBatchBuilder<'a> {
    lix: &'a RemoteLix,
    statements: &'a [ExecuteBatchStatement],
    options: ProtocolExecuteOptions,
}
impl RemoteExecuteBatchBuilder<'_> {
    pub fn with_origin_key(mut self, origin_key: impl Into<String>) -> Self {
        self.options.origin_key = Some(origin_key.into());
        self
    }
}
impl<'a> IntoFuture for RemoteExecuteBatchBuilder<'a> {
    type Output = Result<Vec<ExecuteResult>, LixError>;
    type IntoFuture = crate::sync::SyncTransportFuture<'a, Vec<ExecuteResult>>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
                self.lix
                    .client
                    .execute_batch(self.statements, Some(self.options))
                    .await
        })
    }
}

/// Configures execution on a remote repository.
#[derive(Debug)]
pub struct RemoteExecuteBuilder<'a> {
    lix: &'a RemoteLix,
    sql: &'a str,
    params: &'a [Value],
    options: ProtocolExecuteOptions,
}
impl RemoteExecuteBuilder<'_> {
    pub fn with_origin_key(mut self, origin_key: impl Into<String>) -> Self {
        self.options.origin_key = Some(origin_key.into());
        self
    }
}
impl<'a> IntoFuture for RemoteExecuteBuilder<'a> {
    type Output = Result<ExecuteResult, LixError>;
    type IntoFuture = crate::sync::SyncTransportFuture<'a, ExecuteResult>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
                self.lix
                    .client
                    .execute(self.sql, self.params, Some(self.options))
                    .await
        })
    }
}

/// Restores a snapshot into fresh storage and then opens the resulting Lix.
#[expect(missing_debug_implementations)]
pub struct OpenLixFromSnapshotBuilder<StorageImpl, Source> {
    open: OpenLixBuilder<StorageImpl>,
    source: Source,
}

async fn finish_open<StorageImpl>(
    open: OpenLixBuilder<StorageImpl>,
    storage: StorageSession<StorageImpl>,
) -> Result<Lix<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let retained_progress = Arc::new(RetainingOpenProgressSink::new(open.open_progress.clone()));
    // Opening is one restartable unit. In cross-context storage, a competing
    // tab may commit during any phase, including sync bootstrap after the
    // engine and primary session exist.
    let mut lix = retry_expired_read(|| {
        open_lix_inner(
            storage.clone(),
            open.wasm_runtime.clone(),
            open.telemetry.clone(),
            open.server.clone(),
            retained_progress.clone(),
        )
    })
    .await?;
    let initialized = lix.open_report.initialized || retained_progress.initialized();
    let migration = lix.open_report.migration.or_else(|| {
        retained_progress
            .migrated_from()
            .map(|from_format| OpenMigrationReport {
                from_format,
                to_format: crate::init::CURRENT_FORMAT_VERSION,
            })
    });
    if initialized != lix.open_report.initialized || migration != lix.open_report.migration {
        lix.open_report = Arc::new(OpenReport {
            format: lix.open_report.format,
            initialized,
            migration,
        });
    }
    Ok(lix)
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
                // Acquire exactly once and retain this fenced generation across
                // every retry and for the complete lifetime of the returned Lix.
                let storage = StorageSession::acquire(self.storage.clone()).await?;
                finish_open(self, storage).await
            })
        })
    }
}

impl<StorageImpl, Source> IntoFuture for OpenLixFromSnapshotBuilder<StorageImpl, Source>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Source: futures_io::AsyncRead + Unpin + Send + 'static,
{
    type Output = Result<Lix<StorageImpl>, LixError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(unsafe {
            crate::session::AssumeSendFuture::new(async move {
                if self.open.server.is_some() {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "snapshot restore cannot be combined with server mode",
                    ));
                }
                let storage = StorageSession::acquire(self.open.storage.clone()).await?;
                let storage = crate::snapshot::restore_snapshot(storage, self.source).await?;
                finish_open(self.open, storage).await
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
        if self.lix.engine.sync_mode().role() != crate::sync::SyncRole::Replica {
            return Box::pin(unsafe {
                crate::session::AssumeSendFuture::new(async move {
                    self.lix
                        .session
                        .execute_with_options(&self.sql, &self.params, self.options)
                        .await
                })
            });
        }
        Box::pin(unsafe {
            crate::session::AssumeSendFuture::new(async move {
                let route = self.lix.session.statement_authority_route(&self.sql)?;
                self.lix
                    .retry_replica_read(route, || {
                        self.lix.retry_sync_demands(|| {
                            self.lix.session.execute_with_options(
                                &self.sql,
                                &self.params,
                                self.options.clone(),
                            )
                        })
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
        if self.lix.engine.sync_mode().role() != crate::sync::SyncRole::Replica {
            return Box::pin(unsafe {
                crate::session::AssumeSendFuture::new(async move {
                    self.lix
                        .session
                        .execute_batch_with_options(&self.statements, self.options)
                        .await
                })
            });
        }
        Box::pin(unsafe {
            crate::session::AssumeSendFuture::new(async move {
                let route = self.lix.session.batch_authority_route(&self.statements)?;
                self.lix
                    .retry_replica_read(route, || {
                        self.lix.retry_sync_demands(|| {
                            self.lix
                                .session
                                .execute_batch_with_options(&self.statements, self.options.clone())
                        })
                    })
                    .await
            })
        })
    }
}

/// Clonable handle for a Lix repository.
///
/// Clones share the active branch, file-view state, and close lifecycle.
/// Explicit transactions use independent contexts on the captured branch.
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
    engine: Arc<Engine<StorageSession<StorageImpl>>>,
    session: Arc<SessionContext<StorageSession<StorageImpl>>>,
    transaction_lifecycle: Arc<PublicTransactionLifecycle>,
    primary_switch_gate: Option<Arc<tokio::sync::Mutex<()>>>,
    sync_lease: Option<Arc<SyncSessionLease>>,
    sync_demand_tx: Option<tokio::sync::mpsc::Sender<crate::sync::SyncDemand>>,
    server: Option<ServerOptions>,
    open_report: Arc<OpenReport>,
}

/// Reserves only the handle's close lifecycle, not its SQL session.
#[derive(Debug, Default)]
struct PublicTransactionLifecycle {
    admission: tokio::sync::Mutex<()>,
    active: AtomicUsize,
}

#[derive(Debug)]
struct PublicTransactionLease(Arc<PublicTransactionLifecycle>);

impl PublicTransactionLease {
    fn acquire(lifecycle: Arc<PublicTransactionLifecycle>) -> Result<Self, LixError> {
        lifecycle
            .active
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                LixError::new(
                    "LIX_INVALID_TRANSACTION_STATE",
                    "Lix handle already has an active transaction",
                )
            })?;
        Ok(Self(lifecycle))
    }
}

impl Drop for PublicTransactionLease {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

/// A live query observation bound to the local storage session.
#[expect(missing_debug_implementations)]
pub struct ObserveEvents<StorageImpl = Memory>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    events: crate::session::SessionObserveEvents<StorageSession<StorageImpl>>,
}

impl<StorageImpl> ObserveEvents<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub fn next(
        &mut self,
    ) -> impl Future<Output = Result<Option<ObserveEvent>, LixError>> + Send + '_ {
        self.events.next()
    }

    pub fn close(&mut self) {
        self.events.close();
    }
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
            self.runtime.stop_and_join().await?;
        }
        Ok(())
    }
}

async fn open_lix_inner<StorageImpl>(
    storage: StorageSession<StorageImpl>,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
    server: Option<ServerOptions>,
    retained_progress: Arc<RetainingOpenProgressSink>,
) -> Result<Lix<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let server = match server {
        Some(mut server) => {
            server.url = crate::sync::normalize_sync_locator(&server.url)?.locator;
            Some(server)
        }
        None => None,
    };
    let open_progress: Arc<dyn OpenProgressSink> = retained_progress.clone();
    let admission = ensure_current_repository(&storage, Some(&open_progress)).await?;
    let mut open_report = admission.report;
    retained_progress.retain_initialized(open_report.initialized);
    let migrated_from = open_report.migration.map(|migration| migration.from_format);
    emit_open_progress(
        Some(&open_progress),
        OpenProgress {
            phase: OpenPhase::Opening,
            from_format: migrated_from,
            to_format: crate::init::CURRENT_FORMAT_VERSION,
            completed: None,
            total: None,
        },
    );
    // A fresh repository or one left in the initialization/bootstrap crash
    // window needs one handshake and snapshot before its application session
    // can be bound to the authority's account. Reopens with durable state for
    // this repository remain entirely local even when its transport URL changes.
    let (reopened_sync_account_id, mut prepared_sync) = if let Some(server) = server.as_ref() {
        match crate::sync::inspect_sync_bootstrap_with_adapter(&admission.adapter, &server.url)
            .await?
        {
            crate::sync::SyncBootstrapAdmission::Prepare => (
                None,
                Some(crate::sync::prepare_sync_bootstrap(server).await?),
            ),
            crate::sync::SyncBootstrapAdmission::Ready { account_id } => (Some(account_id), None),
        }
    } else {
        (None, None)
    };
    let initial_sync_branch_id = prepared_sync
        .as_ref()
        .map(|prepared| prepared.default_branch_id.clone());
    let (engine, engine_initialized) = open_or_initialize_engine_with_adapter(
        admission.adapter,
        wasm_runtime,
        telemetry,
        None,
        initial_sync_branch_id.as_deref(),
    )
    .await?;
    if engine_initialized {
        open_report.initialized = true;
        retained_progress.retain_initialized(true);
    }
    let session = match reopened_sync_account_id {
        Some(account_id) => engine.open_session_with_account(account_id).await?,
        None => engine.open_session().await?,
    };
    let mut lix = Lix {
        engine: Arc::new(engine),
        session: Arc::new(session),
        transaction_lifecycle: Arc::default(),
        primary_switch_gate: Some(Arc::new(tokio::sync::Mutex::new(()))),
        sync_lease: None,
        sync_demand_tx: None,
        server: server.clone(),
        open_report: Arc::new(open_report),
    };
    if let Some(server) = server {
        let initial_transport = if let Some(prepared) = prepared_sync.take() {
            Some(crate::sync::install_sync_bootstrap(&mut lix, &server, prepared).await?)
        } else {
            None
        };
        let runtime =
            crate::sync::activate_sync_mode(&mut lix, &server, initial_transport).await?;
        lix.sync_demand_tx = Some(runtime.demand_tx.clone());
        lix.sync_lease = Some(SyncSessionLease::root(runtime));
        // Foreground execution belongs to the durable local replica.
        // The sync worker owns all server traffic, including lazy history.
    }
    lix.bind_session();
    emit_open_progress(
        Some(&open_progress),
        OpenProgress {
            phase: OpenPhase::Complete,
            from_format: migrated_from,
            to_format: crate::init::CURRENT_FORMAT_VERSION,
            completed: None,
            total: None,
        },
    );
    Ok(lix)
}

struct RepositoryAdmission<StorageImpl> {
    adapter: crate::storage_adapter::StorageAdapter<StorageImpl>,
    report: OpenReport,
}

async fn ensure_current_repository<StorageImpl>(
    storage: &StorageImpl,
    progress: Option<&Arc<dyn OpenProgressSink>>,
) -> Result<RepositoryAdmission<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let current = crate::init::CURRENT_FORMAT_VERSION;
    emit_open_progress(
        progress,
        OpenProgress {
            phase: OpenPhase::Inspecting,
            from_format: None,
            to_format: current,
            completed: None,
            total: None,
        },
    );
    let admission = crate::migration::admit_repository(storage, progress).await?;
    Ok(RepositoryAdmission {
        adapter: admission.adapter,
        report: admission.report,
    })
}

impl<StorageImpl> Lix<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Configures a deterministic, stream-first snapshot export.
    pub fn export_snapshot(&self) -> crate::snapshot::SnapshotExportBuilder<StorageImpl> {
        let export = crate::snapshot::SnapshotExportBuilder::new(self.engine.storage());
        if let Some(server) = &self.server {
            export.from_sync_server(server.clone(), self.active_account_id().to_owned())
        } else if self.engine.sync_mode().role() == crate::sync::SyncRole::Replica {
            export.reject_connected_replica()
        } else {
            export
        }
    }

    #[cfg(feature = "server-protocol")]
    pub(crate) async fn open_protocol_session(
        engine: Arc<Engine<StorageSession<StorageImpl>>>,
        active_branch_id: Option<String>,
        active_account_id: String,
    ) -> Result<Self, LixError> {
        let session = match active_branch_id {
            Some(active_branch_id) => {
                if engine
                    .load_branch_head_commit_id(&active_branch_id)
                    .await?
                    .is_none()
                {
                    return Err(LixError::branch_not_found(
                        active_branch_id,
                        "open_protocol_session",
                        "target",
                    ));
                }
                engine
                    .open_session_at_with_account(active_branch_id, active_account_id)
                    .await?
            }
            None => engine.open_session_with_account(active_account_id).await?,
        };
        Ok(Self {
            engine,
            session: Arc::new(session),
            transaction_lifecycle: Arc::default(),
            primary_switch_gate: None,
            sync_lease: None,
            sync_demand_tx: None,
            server: None,
            open_report: Arc::new(OpenReport {
                format: crate::init::CURRENT_FORMAT_VERSION,
                initialized: false,
                migration: None,
            }),
        })
    }

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

    #[cfg(feature = "storage-benches")]
    #[doc(hidden)]
    pub fn storage_adapter(
        &self,
    ) -> crate::storage_adapter::StorageAdapter<StorageSession<StorageImpl>> {
        self.engine.storage()
    }

    #[cfg(not(feature = "storage-benches"))]
    pub(crate) fn storage_adapter(
        &self,
    ) -> crate::storage_adapter::StorageAdapter<StorageSession<StorageImpl>> {
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
            None => Arc::clone(&self.session).active_branch_id_owned().await?,
        };
        let active_account_id = account_id.unwrap_or_else(|| self.active_account_id().to_owned());
        if self.engine.sync_mode().role() == crate::sync::SyncRole::Replica
            && active_account_id != self.active_account_id()
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "connected sessions cannot override the authority-authenticated account",
            ));
        }
        let mut opened = self
            .open_internal_session(active_branch_id.clone(), active_account_id)
            .await?;
        opened.sync_lease = self.sync_lease.as_ref().map(|lease| lease.child());

        Ok(opened)
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
            transaction_lifecycle: Arc::default(),
            primary_switch_gate: None,
            sync_lease: None,
            sync_demand_tx: self.sync_demand_tx.clone(),
            server: self.server.clone(),
            open_report: Arc::clone(&self.open_report),
        })
    }

    /// Returns the immutable report produced while opening this repository.
    pub fn open_report(&self) -> &OpenReport {
        &self.open_report
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
        self.retry_replica_read(crate::sql2::StatementAuthorityRoute::HotRead, || {
            self.retry_sync_demands(|| self.session.read_file_content(path.clone(), range.clone()))
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
    ) -> Pin<Box<dyn Future<Output = Result<ExecuteResult, LixError>> + Send + 'static>> {
        if self.engine.sync_mode().role() != crate::sync::SyncRole::Replica {
            return Box::pin(
                Arc::clone(&self.session).execute_with_idempotency_and_options_and_metadata(
                    sql,
                    params,
                    options,
                    metadata,
                    idempotency,
                ),
            );
        }
        // SAFETY: the retry future owns the Lix handle and every request
        // value. Reusing the same idempotency identity on each attempt is the
        // required contract: a pre-commit history demand has no receipt, while
        // an already committed attempt replays its durable receipt.
        Box::pin(unsafe {
            crate::session::AssumeSendFuture::new(async move {
                let route = self.session.statement_authority_route(&sql)?;
                self.retry_replica_read(route, || {
                    self.retry_sync_demands(|| {
                        Arc::clone(&self.session).execute_with_idempotency_and_options_and_metadata(
                            sql.clone(),
                            params.clone(),
                            options.clone(),
                            metadata.clone(),
                            idempotency.clone(),
                        )
                    })
                })
                .await
            })
        })
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
        let statements = Arc::new(
            statements
                .iter()
                .map(|(sql, params)| ((*sql).to_owned(), (*params).to_vec()))
                .collect::<Vec<_>>(),
        );
        let routed = statements
            .iter()
            .map(|(sql, params)| ExecuteBatchStatement {
                sql: sql.clone(),
                params: params.clone(),
                label: None,
            })
            .collect::<Vec<_>>();
        let route = self.session.batch_authority_route(&routed);
        let session = Arc::clone(&self.session);
        let demand_tx = self.sync_demand_tx.clone();
        // SAFETY: the future owns its local session and statement values, as
        // do the ordinary execute builders. Each retry opens a complete new
        // coherent snapshot after hydration releases the old read scope.
        unsafe {
            crate::session::AssumeSendFuture::new(async move {
                if route? == crate::sql2::StatementAuthorityRoute::AuthorityWrite {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "execute_coherent_read_batch only accepts read statements without durable runtime functions",
                    ));
                }
                let mut retry = crate::sync::SyncDemandRetry::default();
                loop {
                    let result = retry_expired_read(|| {
                        Arc::clone(&session)
                            .execute_coherent_read_batch_owned(Arc::clone(&statements))
                    })
                    .await;
                    match result {
                        Ok(result) => return Ok(result),
                        Err(error) => retry.hydrate_for_retry(demand_tx.as_ref(), error).await?,
                    }
                }
            })
        }
    }

    /// Classifies an atomic SQL batch for a caller that owns its transport
    /// lifecycle.
    pub(crate) fn execute_batch_disposition(
        &self,
        statements: &[ExecuteBatchStatement],
    ) -> Result<ExecutionDisposition, LixError> {
        self.session.execute_batch_disposition(statements)
    }

    pub(crate) fn execute_batch_with_idempotency_and_options_and_metadata(
        self: Arc<Self>,
        statements: Vec<ExecuteBatchStatement>,
        options: ExecuteOptions,
        statement_metadata: Vec<ExecuteStatementMetadata>,
        idempotency: Option<ExecuteIdempotency>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ExecuteResult>, LixError>> + Send + 'static>> {
        if self.engine.sync_mode().role() != crate::sync::SyncRole::Replica {
            return Box::pin(
                Arc::clone(&self.session).execute_batch_with_idempotency_and_options_and_metadata(
                    statements,
                    options,
                    statement_metadata,
                    idempotency,
                ),
            );
        }
        // Preserve the exact request identity across lazy-history retries so a
        // commit-outcome-unknown response can still be retried safely with the
        // caller's original key.
        Box::pin(unsafe {
            crate::session::AssumeSendFuture::new(async move {
                let route = self.session.batch_authority_route(&statements)?;
                self.retry_replica_read(route, || {
                    self.retry_sync_demands(|| {
                        Arc::clone(&self.session)
                            .execute_batch_with_idempotency_and_options_and_metadata(
                                statements.clone(),
                                options.clone(),
                                statement_metadata.clone(),
                                idempotency.clone(),
                            )
                    })
                })
                .await
            })
        })
    }

    #[cfg(test)]
    pub(crate) fn set_sync_demand_sender_for_test(
        &mut self,
        sender: tokio::sync::mpsc::Sender<crate::sync::SyncDemand>,
    ) {
        self.sync_demand_tx = Some(sender);
    }

    #[cfg(test)]
    pub(crate) fn clear_sync_demand_sender_for_test(&mut self) {
        self.sync_demand_tx = None;
    }

    pub fn observe(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<ObserveEvents<StorageImpl>, LixError> {
        self.session
            .observe(sql, params)
            .map(|events| ObserveEvents {
                events: events.with_sync_demand_sender(self.sync_demand_tx.clone()),
            })
    }

    /// Starts an atomic transaction on the current branch and account.
    ///
    /// The transaction owns an independent context. Reads, observations, and
    /// writes through this handle remain outside it, and later branch switches
    /// do not retarget it. Finish or drop the transaction before closing this
    /// handle (or one of its clones).
    ///
    /// On a sync replica, fetch any required cold history before opening the
    /// transaction. Its pinned snapshot cannot be hydrated while it is active;
    /// an uncached historical read returns a structured sync-demand error.
    pub async fn begin_transaction(&self) -> Result<LixTransaction<StorageImpl>, LixError> {
        // Reserve before awaiting admission so close also notices an opening
        // transaction. The mutex coordinates only begin/close, never SQL work.
        let lifecycle = PublicTransactionLease::acquire(Arc::clone(&self.transaction_lifecycle))?;
        let _admission = self.transaction_lifecycle.admission.lock().await;
        self.session.ensure_open()?;

        let branch_id = Arc::clone(&self.session).active_branch_id_owned().await?;
        let session = Arc::new(
            self.engine
                .open_session_at_with_account(branch_id, self.active_account_id().to_owned())
                .await?
                .with_file_views_from(&self.session),
        );
        Ok(LixTransaction {
            _lifecycle: lifecycle,
            inner: Some(session.begin_transaction().await?),
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
    /// this once. Hosts that mint a session against an already-open runtime
    /// should call the same helper instead of opening another engine.
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
        self.engine.ensure_account(id, name, kind).await
    }

    pub async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError> {
        self.retry_sync_demands(|| self.session.create_branch(options.clone()))
            .await
    }

    /// Crate-internal test/support sugar. Public callers use the canonical SQL
    /// `lix_create_checkpoint(...)` function.
    pub(crate) async fn create_checkpoint(
        &self,
    ) -> Result<crate::session::CreateCheckpointReceipt, LixError> {
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

    pub async fn merge_branch_preview(
        &self,
        options: MergeBranchPreviewOptions,
    ) -> Result<MergeBranchPreview, LixError> {
        self.retry_sync_demands(|| self.session.merge_branch_preview(options.clone()))
            .await
    }

    /// Restarts the complete local serving attempt when a certified replica
    /// publication races its storage snapshot. Session reads already retry
    /// individual coherent scopes; this outer boundary covers expiry between
    /// sync-demand hydration and the final local read. Only classified reads
    /// enter it, so retrying cannot duplicate a mutation.
    async fn retry_replica_read<T, Operation, OperationFuture>(
        &self,
        route: crate::sql2::StatementAuthorityRoute,
        mut operation: Operation,
    ) -> Result<T, LixError>
    where
        Operation: FnMut() -> OperationFuture,
        OperationFuture: Future<Output = Result<T, LixError>>,
    {
        if route != crate::sql2::StatementAuthorityRoute::AuthorityWrite
            && self.engine.sync_mode().role() == crate::sync::SyncRole::Replica
        {
            retry_expired_read(operation).await
        } else {
            operation().await
        }
    }

    pub async fn close(&self) -> Result<(), LixError> {
        // A begin awaiting network I/O must not make close wait for admission.
        if self.transaction_lifecycle.active.load(Ordering::Acquire) > 0 {
            return Err(LixError::new(
                "LIX_INVALID_TRANSACTION_STATE",
                "cannot close Lix while an explicit transaction is active",
            ));
        }
        let _admission = self.transaction_lifecycle.admission.lock().await;
        if self.transaction_lifecycle.active.load(Ordering::Acquire) > 0 {
            return Err(LixError::new(
                "LIX_INVALID_TRANSACTION_STATE",
                "cannot close Lix while an explicit transaction is active",
            ));
        }
        // Check the independent transactions before mutating any session or
        // remote lifecycle, including their shared publication worker.
        self.session.close().await?;

        if let Some(lease) = &self.sync_lease {
            lease.release().await?;
        }
        Ok(())
    }
    pub(crate) fn set_sync_role(&self, role: crate::sync::SyncRole) -> Result<(), LixError> {
        if role == crate::sync::SyncRole::Replica {
            self.engine.storage().admit_sync_replica_writer();
        }
        self.engine.sync_mode().set_role(role);
        Ok(())
    }

    pub(crate) fn set_sync_replica_remote_id(&self, remote_id: &str) -> Result<(), LixError> {
        crate::sync::validate_sync_remote_id(remote_id)?;
        self.engine
            .sync_mode()
            .set_replica_remote_id(Arc::<str>::from(remote_id));
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
}

#[expect(missing_debug_implementations)]
pub struct LixTransaction<StorageImpl = Memory>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    inner: Option<lix::SessionTransaction<StorageSession<StorageImpl>>>,
    _lifecycle: PublicTransactionLease,
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
        Box::pin(unsafe {
            crate::session::AssumeSendFuture::new(async move {
                {
                    self.transaction
                        .inner
                        .as_mut()
                        .ok_or_else(closed_transaction_error)?
                        .execute_with_options(
                            self.sql.to_owned(),
                            self.params.to_vec(),
                            self.options,
                        )
                        .await
                }
            })
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
        unsafe {
            crate::session::AssumeSendFuture::new(async move {
                {
                    self.inner
                        .as_mut()
                        .ok_or_else(closed_transaction_error)?
                        .execute_with_options(sql, params, options)
                        .await
                }
            })
        }
    }

    #[cfg(test)]
    pub(crate) async fn stage_test_row(
        &mut self,
        row: TransactionWriteRow,
    ) -> Result<(), LixError> {
        self.inner
            .as_mut()
            .ok_or_else(closed_transaction_error)?
            .stage_test_row(row)
            .await
    }

    pub async fn commit(mut self) -> Result<(), LixError> {
        self.inner
            .take()
            .ok_or_else(closed_transaction_error)?
            .commit()
            .await
    }

    pub async fn rollback(mut self) -> Result<(), LixError> {
        self.inner
            .take()
            .ok_or_else(closed_transaction_error)?
            .rollback()
            .await
    }
}

fn closed_transaction_error() -> LixError {
    LixError::new(
        LixError::CODE_INVALID_SESSION_STATE,
        "Lix transaction is closed",
    )
}

async fn open_or_initialize_engine_with_adapter<StorageImpl>(
    adapter: crate::storage_adapter::StorageAdapter<StorageImpl>,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
    plugin_resource_limits: Option<(u64, usize)>,
    initial_main_branch_id: Option<&str>,
) -> Result<(Engine<StorageImpl>, bool), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    match new_engine(
        adapter.clone(),
        wasm_runtime.clone(),
        telemetry.clone(),
        plugin_resource_limits,
    )
    .await
    {
        Ok(engine) => Ok((engine, false)),
        Err(error) if error.code == "LIX_ERROR_NOT_INITIALIZED" => {
            let initialized = match Engine::initialize_with_adapter(
                adapter.clone(),
                initial_main_branch_id,
            )
            .await
            {
                Ok(_) => true,
                Err(error) if error.code == "LIX_ERROR_ALREADY_INITIALIZED" => false,
                Err(error) => return Err(error),
            };
            new_engine(adapter, wasm_runtime, telemetry, plugin_resource_limits)
                .await
                .map(|engine| (engine, initialized))
        }
        Err(error) => Err(error),
    }
}

async fn retry_expired_read<T, Operation, OperationFuture>(
    mut operation: Operation,
) -> Result<T, LixError>
where
    Operation: FnMut() -> OperationFuture,
    OperationFuture: Future<Output = Result<T, LixError>>,
{
    let mut retry = ExpiredReadRetryState::default();
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let Some(delay) = retry.next_delay(&error) else {
                    return Err(error);
                };
                tokio::task::yield_now().await;
                if !delay.is_zero() {
                    crate::sync::sleep(delay).await;
                }
            }
        }
    }
}

async fn new_engine<StorageImpl>(
    storage: crate::storage_adapter::StorageAdapter<StorageImpl>,
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
    Engine::new_with_adapter(storage, options).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use lix::telemetry::{
        CallbackTelemetrySink, CompletedTelemetrySpan, TelemetrySink, TelemetrySpanDescriptor,
        TelemetrySpanEnd, TelemetrySpanHandle, TelemetrySpanStart,
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    #[tokio::test]
    async fn opening_transaction_rejects_close_and_cancellation_releases_reservation() {
        let lix = open_lix().await.expect("open Lix");
        let admission = lix.transaction_lifecycle.admission.lock().await;
        let mut opening = Box::pin(lix.begin_transaction());
        std::future::poll_fn(|cx| {
            assert!(opening.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;

        let mut closing = Box::pin(lix.close());
        std::future::poll_fn(|cx| {
            let std::task::Poll::Ready(Err(error)) = closing.as_mut().poll(cx) else {
                panic!("close must reject immediately while a transaction is opening");
            };
            assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");
            std::task::Poll::Ready(())
        })
        .await;
        drop(closing);
        lix.execute("SELECT 1", &[])
            .await
            .expect("opening reservation does not block parent SQL");

        drop(opening);
        assert_eq!(lix.transaction_lifecycle.active.load(Ordering::Acquire), 0);
        drop(admission);
        lix.begin_transaction()
            .await
            .expect("cancelled begin releases reservation")
            .rollback()
            .await
            .expect("replacement transaction rolls back");
        lix.close().await.expect("parent closes after cancellation");
    }

    #[tokio::test]
    async fn failed_transaction_begin_releases_lifecycle_reservation() {
        let lix = open_lix().await.expect("open Lix");
        lix.close().await.expect("close Lix");
        assert!(lix.begin_transaction().await.is_err());
        assert_eq!(lix.transaction_lifecycle.active.load(Ordering::Acquire), 0);
        assert!(lix.begin_transaction().await.is_err());
        assert_eq!(lix.transaction_lifecycle.active.load(Ordering::Acquire), 0);
    }

    fn opened_spans(spans: &[CompletedTelemetrySpan]) -> Vec<&CompletedTelemetrySpan> {
        spans
            .iter()
            .filter(|span| span.start.name == "lix.repository.opened")
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

    #[cfg(not(target_family = "wasm"))]
    #[tokio::test]
    async fn server_without_storage_opens_remote_protocol_session() {
        use std::io::{Read, Write};
        let source = open_lix().await.unwrap();
        source.execute("INSERT INTO lix_key_value (key, value) VALUES ('remote-snapshot', 'true'::jsonb)", &[]).await.unwrap();
        let mut snapshot = Vec::new();
        source.export_snapshot().write_to(&mut snapshot).await.unwrap();
        let expected_snapshot = snapshot.clone();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let thread = std::thread::spawn(move || {
            let (mut connection, _) = listener.accept().unwrap();
            connection
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0];
                connection.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET /lix/v1/00000000-0000-4000-8000-000000000001/ "));
            assert!(
                request
                    .to_lowercase()
                    .contains("authorization: bearer test")
            );
            let body = serde_json::json!({
                "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                "sessionId": "remote-session", "activeBranchId": "main", "activeAccountId": "account"
            }).to_string();
            write!(connection, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            drop(connection);
            let (mut connection, _) = listener.accept().unwrap();
            connection.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0];
                connection.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") { break; }
            }
            assert!(String::from_utf8(request).unwrap().starts_with("GET /lix/v1/00000000-0000-4000-8000-000000000001/snapshot"));
            write!(connection, "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.lix.snapshot\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", snapshot.len()).unwrap();
            connection.write_all(&snapshot).unwrap();
        });
        let lix: RemoteLix = open_lix()
            .with_server(
                ServerOptions::new(format!(
                    "http://{address}/lix/00000000-0000-4000-8000-000000000001"
                ))
                .with_headers([("Authorization".to_owned(), "Bearer test".to_owned())]),
            )
            .await
            .expect("remote open");
        assert_eq!(lix.client.session_id().as_deref(), Some("remote-session"));
        let mut exported = Vec::new();
        lix.export_snapshot().write_to(&mut exported).await.expect("remote snapshot export");
        assert_eq!(exported, expected_snapshot);
        assert_eq!(lix.active_account_id(), "account");
        thread.join().unwrap();
    }

    #[cfg(not(target_family = "wasm"))]
    fn remote_request(
        listener: &std::net::TcpListener,
        method: &str,
        path: &str,
        session: Option<&str>,
    ) -> std::net::TcpStream {
        use std::io::Read;
        let (mut connection, _) = listener.accept().unwrap();
        connection
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        loop {
            let mut byte = [0];
            connection.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let request = String::from_utf8(request).unwrap();
        assert!(request.starts_with(&format!("{method} ")), "{request}");
        assert!(request.lines().next().unwrap().contains(path), "{request}");
        if let Some(session) = session {
            assert!(
                request
                    .to_lowercase()
                    .contains(&format!("lix-session-id: {session}\r\n")),
                "{request}"
            );
        }
        let size = request
            .lines()
            .find_map(|line| {
                line.to_lowercase()
                    .strip_prefix("content-length:")
                    .and_then(|n| n.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        connection.read_exact(&mut vec![0; size]).unwrap();
        connection
    }

    #[cfg(not(target_family = "wasm"))]
    fn remote_response(mut connection: std::net::TcpStream, status: u16, body: serde_json::Value) {
        use std::io::Write;
        let body = if status == 204 {
            String::new()
        } else {
            body.to_string()
        };
        write!(connection, "HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    }

    #[cfg(not(target_family = "wasm"))]
    fn remote_handshake(listener: &std::net::TcpListener, session: &str, child: bool) {
        let path = if child {
            "/?activeBranchId=feature"
        } else {
            "/ "
        };
        remote_response(
            remote_request(listener, "GET", path, None),
            200,
            serde_json::json!({
                "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                "sessionId": session, "activeBranchId": "feature", "activeAccountId": "account"
            }),
        );
    }

    #[cfg(not(target_family = "wasm"))]
    #[tokio::test]
    async fn remote_transactions_finish_and_close_their_dedicated_sessions() {
        for commit in [false, true] {
            for fail_finish in [false, true] {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let address = listener.local_addr().unwrap();
                let thread = std::thread::spawn(move || {
                    remote_handshake(&listener, "parent", false);
                    remote_handshake(&listener, "child", true);
                    remote_response(
                        remote_request(&listener, "POST", "/transaction/begin ", Some("child")),
                        200,
                        serde_json::json!({ "transactionId": "transaction" }),
                    );
                    let path = if commit {
                        "/transaction/commit "
                    } else {
                        "/transaction/rollback "
                    };
                    remote_response(
                        remote_request(&listener, "POST", path, Some("child")),
                        if fail_finish { 500 } else { 204 },
                        serde_json::json!({"error": {"code": "TEST_FINISH_FAILED", "message": "finish failed"}}),
                    );
                    remote_response(
                        remote_request(&listener, "DELETE", "/session ", Some("child")),
                        204,
                        serde_json::Value::Null,
                    );
                    remote_response(
                        remote_request(&listener, "POST", "/execute ", Some("parent")),
                        200,
                        serde_json::json!({ "columns": [], "rows": [], "rowsAffected": 0 }),
                    );
                    remote_response(
                        remote_request(&listener, "DELETE", "/session ", Some("parent")),
                        204,
                        serde_json::Value::Null,
                    );
                });
                let lix = open_lix()
                    .with_server(ServerOptions::new(format!(
                        "http://{address}/lix/00000000-0000-4000-8000-000000000001"
                    )))
                    .await
                    .unwrap();
                let transaction = lix.begin_transaction().await.unwrap();
                let child = transaction.client.as_ref().unwrap();
                assert_eq!(
                    child.active_branch_id().await.unwrap(),
                    lix.active_branch_id().await.unwrap()
                );
                assert_eq!(
                    child.active_account_id().await.unwrap(),
                    lix.active_account_id()
                );
                assert_ne!(child.session_id(), lix.client.session_id());
                let result = if commit {
                    transaction.commit().await
                } else {
                    transaction.rollback().await
                };
                if fail_finish {
                    assert_eq!(result.unwrap_err().code, "TEST_FINISH_FAILED");
                } else {
                    result.unwrap();
                }
                lix.execute("SELECT 1", &[])
                    .await
                    .expect("parent remains usable after transaction finishes");
                lix.close().await.unwrap();
                thread.join().unwrap();
            }
        }
    }

    #[cfg(not(target_family = "wasm"))]
    #[tokio::test]
    async fn dropping_remote_transaction_does_not_block_parent_during_failed_session_cleanup() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (closing_tx, closing_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            remote_handshake(&listener, "parent", false);
            remote_handshake(&listener, "abandoned", true);
            remote_response(
                remote_request(&listener, "POST", "/transaction/begin ", Some("abandoned")),
                200,
                serde_json::json!({ "transactionId": "abandoned-transaction" }),
            );
            let pending_close = remote_request(&listener, "DELETE", "/session ", Some("abandoned"));
            closing_tx.send(()).unwrap();
            // Keep cleanup unanswered while the same parent executes, opens another
            // transaction, and closes. No rollback request should be sent on drop.
            remote_response(
                remote_request(&listener, "POST", "/execute ", Some("parent")),
                200,
                serde_json::json!({ "columns": [], "rows": [], "rowsAffected": 0 }),
            );
            remote_handshake(&listener, "replacement", true);
            remote_response(
                remote_request(
                    &listener,
                    "POST",
                    "/transaction/begin ",
                    Some("replacement"),
                ),
                200,
                serde_json::json!({ "transactionId": "replacement-transaction" }),
            );
            remote_response(
                remote_request(
                    &listener,
                    "POST",
                    "/transaction/commit ",
                    Some("replacement"),
                ),
                204,
                serde_json::Value::Null,
            );
            remote_response(
                remote_request(&listener, "DELETE", "/session ", Some("replacement")),
                204,
                serde_json::Value::Null,
            );
            remote_response(
                remote_request(&listener, "DELETE", "/session ", Some("parent")),
                204,
                serde_json::Value::Null,
            );
            resume_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            remote_response(
                pending_close,
                500,
                serde_json::json!({ "error": {"code": "TEST_CLOSE_FAILED", "message": "close failed"} }),
            );
        });
        let lix = open_lix()
            .with_server(ServerOptions::new(format!(
                "http://{address}/lix/00000000-0000-4000-8000-000000000001"
            )))
            .await
            .unwrap();
        drop(lix.begin_transaction().await.unwrap());
        closing_rx.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            lix.execute("SELECT 1", &[])
                .await
                .expect("parent SQL during cleanup");
            lix.begin_transaction()
                .await
                .expect("parent transaction during cleanup")
                .commit()
                .await
                .unwrap();
            lix.close().await.expect("parent close during cleanup");
        })
        .await
        .expect("cleanup must not block parent operations");
        resume_tx.send(()).unwrap();
        thread.join().unwrap();
    }

    #[cfg(not(target_family = "wasm"))]
    #[tokio::test]
    async fn failed_or_cancelled_remote_transaction_begin_closes_only_its_dedicated_session() {
        for cancel in [false, true] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let (begin_tx, begin_rx) = tokio::sync::oneshot::channel();
            let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
            let thread = std::thread::spawn(move || {
                remote_handshake(&listener, "parent", false);
                remote_handshake(&listener, "child", true);
                let begin = remote_request(&listener, "POST", "/transaction/begin ", Some("child"));
                begin_tx.send(()).unwrap();
                let pending_begin = if cancel {
                    Some(begin)
                } else {
                    remote_response(
                        begin,
                        500,
                        serde_json::json!({ "error": {"code": "TEST_BEGIN_FAILED", "message": "begin failed"} }),
                    );
                    None
                };
                remote_response(
                    remote_request(&listener, "DELETE", "/session ", Some("child")),
                    204,
                    serde_json::Value::Null,
                );
                drop(pending_begin);
                closed_tx.send(()).unwrap();
                remote_response(
                    remote_request(&listener, "POST", "/execute ", Some("parent")),
                    200,
                    serde_json::json!({ "columns": [], "rows": [], "rowsAffected": 0 }),
                );
                remote_response(
                    remote_request(&listener, "DELETE", "/session ", Some("parent")),
                    204,
                    serde_json::Value::Null,
                );
            });
            let lix = open_lix()
                .with_server(ServerOptions::new(format!(
                    "http://{address}/lix/00000000-0000-4000-8000-000000000001"
                )))
                .await
                .unwrap();
            let opening = tokio::spawn({
                let lix = lix.clone();
                async move { lix.begin_transaction().await }
            });
            tokio::time::timeout(std::time::Duration::from_secs(5), begin_rx)
                .await
                .unwrap()
                .unwrap();
            if cancel {
                opening.abort();
                assert!(opening.await.unwrap_err().is_cancelled());
            } else {
                assert_eq!(opening.await.unwrap().unwrap_err().code, "TEST_BEGIN_FAILED");
            }
            tokio::time::timeout(std::time::Duration::from_secs(5), closed_rx)
                .await
                .unwrap()
                .unwrap();
            lix.execute("SELECT 1", &[])
                .await
                .expect("failed child begin leaves parent usable");
            lix.close().await.unwrap();
            thread.join().unwrap();
        }
    }

    #[tokio::test]
    async fn remote_open_rejects_local_configuration_before_network_access() {
        let result = open_lix()
            .with_open_progress_sink(Arc::new(CallbackOpenProgressSink::new(|_| {})))
            .with_server(ServerOptions::new("https://example.invalid/lix/00000000-0000-4000-8000-000000000001"))
            .await;
        let error = result.expect_err("remote open must reject local progress configuration");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("local runtime"));
    }

    #[tokio::test]
    async fn server_then_storage_selects_sync_without_initializing_invalid_destination() {
        let storage = Memory::new();
        let result = open_lix()
            .with_server(ServerOptions::new("https://example.test/not-a-lix"))
            .with_storage(storage.clone())
            .await;
        assert!(result.is_err());
        let local = open_lix().with_storage(storage).await.unwrap();
        assert!(local.open_report().initialized);
    }

    #[tokio::test]
    async fn invalid_sync_locator_is_rejected_before_storage_initialization() {
        let storage = Memory::new();
        let result = open_lix()
            .with_storage(storage.clone())
            .with_server(ServerOptions::new("https://example.test/not-a-lix"))
            .await;
        let Err(error) = result else {
            panic!("invalid sync locator must fail");
        };
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);

        let lix = open_lix()
            .with_storage(storage)
            .await
            .expect("open untouched storage");
        assert!(
            lix.open_report().initialized,
            "the rejected sync open must leave initialization to the next valid open"
        );
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
        assert_eq!(opened[0].start.name, "lix.repository.opened");
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
            spans.iter().any(|span| span.start.name == "lix.sql.query"),
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
    async fn open_lix_owns_the_storage_session_without_changing_the_public_handle_type() {
        fn assert_public_type(_: &Lix<Memory>) {}

        let storage = Memory::new();
        let first = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("open first Lix");
        assert_public_type(&first);

        assert!(matches!(
            storage
                .begin_read(crate::storage::ReadOptions::default())
                .await,
            Err(crate::storage::StorageError::Fenced)
        ));

        let second = open_lix()
            .with_storage(storage)
            .await
            .expect("a second current handle joins the active generation");
        second
            .execute("SELECT 1", &[])
            .await
            .expect("joined handle remains usable");
    }

    #[tokio::test]
    async fn disabled_opened_kind_does_no_opened_span_work() {
        struct SqlOnlySink {
            started: Mutex<Vec<&'static str>>,
        }

        impl SqlOnlySink {
            fn into_sink(self: Arc<Self>) -> Arc<dyn TelemetrySink> {
                self
            }
        }

        impl TelemetrySink for SqlOnlySink {
            fn enabled(&self, descriptor: &TelemetrySpanDescriptor) -> bool {
                descriptor.name() != "lix.repository.opened"
            }

            fn start_span(&self, start: TelemetrySpanStart) -> Box<dyn TelemetrySpanHandle> {
                assert_ne!(
                    start.name, "lix.repository.opened",
                    "disabled opened spans must not be started"
                );
                self.started.lock().expect("started").push(start.name);
                Box::new(NoopHandle(crate::telemetry::new_span_context(
                    start.parent_span_context.as_ref(),
                )))
            }
        }

        struct NoopHandle(opentelemetry::trace::SpanContext);

        impl TelemetrySpanHandle for NoopHandle {
            fn span_context(&self) -> &opentelemetry::trace::SpanContext {
                &self.0
            }

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
        assert!(started.iter().all(|name| *name != "lix.repository.opened"));
        assert!(started.contains(&"lix.sql.query"));
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
        assert!(
            opened
                .iter()
                .all(|span| span.start.name == "lix.repository.opened")
        );
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
    async fn replica_history_and_mixed_coherent_reads_use_local_state() {
        let lix = open_lix().await.expect("open Lix");
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('local-history', 'value')",
            &[],
        )
        .await
        .expect("seed working state");
        let checkpoint = lix.create_checkpoint().await.expect("seed checkpoint");
        lix.set_sync_role(crate::sync::SyncRole::Replica)
            .expect("mark replica");

        let params = [Value::Text(checkpoint.commit_id)];
        let sql = "SELECT commit_id FROM lix_checkpoint WHERE commit_id = $1";
        let local = lix
            .execute(sql, &params)
            .await
            .expect("cached history is local");
        assert_eq!(local.rows().len(), 1);
        let batch = lix
            .execute_coherent_read_batch(&[
                (
                    "SELECT value FROM lix_key_value WHERE key = 'local-history'",
                    &[],
                ),
                (sql, &params),
            ])
            .await
            .expect("current state and history share a local snapshot");
        assert_eq!(batch.results[0].rows().len(), 1);
        assert_eq!(batch.results[1].rows().len(), 1);
        assert!(batch.storage_mutation_revision.is_some());
        lix.close().await.expect("close replica");
    }

    #[tokio::test]
    async fn replica_coherent_reads_reject_mutations_before_execution() {
        let lix = open_lix().await.expect("open Lix");
        lix.set_sync_role(crate::sync::SyncRole::Replica)
            .expect("mark replica");
        for sql in [
            "INSERT INTO lix_key_value (key, value) VALUES ('read-only', 'unexpected')",
            "SELECT uuidv7()",
            "SELECT current_timestamp",
        ] {
            let error = lix
                .execute_coherent_read_batch(&[("SELECT * FROM lix_checkpoint", &[]), (sql, &[])])
                .await
                .expect_err("coherent reads cannot mutate either engine");
            assert_eq!(error.code, LixError::CODE_INVALID_PARAM, "{sql}");
        }
        lix.close().await.expect("close replica");
    }

    #[tokio::test]
    async fn replica_coherent_hot_read_without_authority_does_not_panic() {
        let lix = open_lix().await.expect("open Lix");
        lix.set_sync_role(crate::sync::SyncRole::Replica)
            .expect("mark replica");
        let batch = lix
            .execute_coherent_read_batch(&[("SELECT 1 AS value", &[])])
            .await
            .expect("a local read does not require a connected authority client");
        assert_eq!(batch.results[0].rows()[0].get::<i64>("value").unwrap(), 1);
        lix.close().await.expect("close replica");
    }

    #[tokio::test]
    async fn connected_hot_read_restarts_after_publication_snapshot_expiry() {
        let lix = open_lix().await.expect("open Lix");
        lix.set_sync_role(crate::sync::SyncRole::Replica)
            .expect("mark handle as a connected replica");
        let attempts = AtomicUsize::new(0);

        let result = lix
            .retry_replica_read(crate::sql2::StatementAuthorityRoute::HotRead, || {
                let attempt = attempts.fetch_add(1, Ordering::Relaxed);
                std::future::ready(if attempt == 0 {
                    Err(LixError::new(
                        LixError::CODE_STORAGE_READ_EXPIRED,
                        "authority publication invalidated the serving snapshot",
                    ))
                } else {
                    Ok("certified")
                })
            })
            .await
            .expect("connected HOT reads should transparently restart");

        assert_eq!(result, "certified");
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn connected_non_hot_operation_is_never_retried() {
        let lix = open_lix().await.expect("open Lix");
        lix.set_sync_role(crate::sync::SyncRole::Replica)
            .expect("mark handle as a connected replica");
        let attempts = AtomicUsize::new(0);

        let error = lix
            .retry_replica_read(crate::sql2::StatementAuthorityRoute::AuthorityWrite, || {
                attempts.fetch_add(1, Ordering::Relaxed);
                std::future::ready(Err::<(), _>(LixError::new(
                    LixError::CODE_STORAGE_READ_EXPIRED,
                    "mutation execution is not restartable at this boundary",
                )))
            })
            .await
            .expect_err("non-HOT operations must preserve the original error");

        assert_eq!(error.code, LixError::CODE_STORAGE_READ_EXPIRED);
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
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
        storage: StorageSession<Memory>,
        wasm_runtime: Option<Arc<dyn WasmRuntime>>,
        telemetry: Option<Arc<dyn TelemetrySink>>,
    ) {
        is_send(&open_lix_inner(
            storage,
            wasm_runtime,
            telemetry,
            None,
            Arc::new(RetainingOpenProgressSink::new(None)),
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
