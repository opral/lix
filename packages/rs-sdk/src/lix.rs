use lix_engine::wasm::WasmRuntime;
use lix_engine::{
    Backend, CreateBranchOptions, CreateBranchReceipt, Engine, ExecuteResult, InMemoryBackend,
    LixError, MergeBranchOptions, MergeBranchPreview, MergeBranchPreviewOptions,
    MergeBranchReceipt, ObserveEvents, SessionContext, SwitchBranchOptions, SwitchBranchReceipt,
    Value,
};
use std::sync::Arc;

/// Options for opening a Lix workspace session.
#[expect(missing_debug_implementations)]
pub struct OpenLixOptions<B = InMemoryBackend> {
    pub backend: B,
    pub wasm_runtime: Option<Arc<dyn WasmRuntime>>,
}

impl Default for OpenLixOptions<InMemoryBackend> {
    fn default() -> Self {
        Self {
            backend: InMemoryBackend::new(),
            wasm_runtime: None,
        }
    }
}

impl<B> OpenLixOptions<B> {
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            wasm_runtime: None,
        }
    }

    pub fn with_wasm_runtime(mut self, wasm_runtime: Arc<dyn WasmRuntime>) -> Self {
        self.wasm_runtime = Some(wasm_runtime);
        self
    }
}

/// Workspace-session handle for a Lix repository.
#[expect(missing_debug_implementations)]
pub struct Lix<B = InMemoryBackend>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    _engine: Engine<B>,
    backend: B,
    session: SessionContext<B>,
}

/// Opens a Lix workspace session.
///
/// `OpenLixOptions::default()` opens a fresh in-memory backend. Pass a
/// concrete backend in `OpenLixOptions<B>` to open SQLite or custom backends
/// with the same runtime configuration path.
pub async fn open_lix<B>(options: OpenLixOptions<B>) -> Result<Lix<B>, LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    let backend = options.backend;
    let engine = open_or_initialize_engine(backend.clone(), options.wasm_runtime).await?;
    let session = engine.open_workspace_session().await?;
    Ok(Lix {
        _engine: engine,
        backend,
        session,
    })
}

pub async fn open_lix_with_backend<B>(backend: B) -> Result<Lix<B>, LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    open_lix(OpenLixOptions::new(backend)).await
}

impl<B> Lix<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    /// Executes one DataFusion SQL statement against this Lix session.
    ///
    /// The SQL dialect is DataFusion SQL, not SQLite SQL. Positional
    /// placeholders use `?` or `$1`, `$2`, and so on. SQLite-specific catalog tables
    /// and transaction statements such as `sqlite_master`, `BEGIN`, and
    /// `COMMIT` are not part of this contract; use `information_schema` for
    /// catalog inspection. Lix owns transaction boundaries for each statement.
    /// While a transaction is active, call `execute()` on the transaction
    /// handle instead.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.session.execute(sql, params).await
    }

    pub fn observe(&self, sql: &str, params: &[Value]) -> Result<ObserveEvents<B>, LixError> {
        self.session.observe(sql, params)
    }

    pub async fn begin_transaction(&self) -> Result<LixTransaction<B>, LixError> {
        Ok(LixTransaction {
            inner: self.session.begin_transaction().await?,
        })
    }

    pub async fn active_branch_id(&self) -> Result<String, LixError> {
        self.session.active_branch_id().await
    }

    pub async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError> {
        self.session.create_branch(options).await
    }

    pub async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<SwitchBranchReceipt, LixError> {
        let (_session, receipt) = self.session.switch_branch(options).await?;
        Ok(receipt)
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

#[cfg(all(not(target_family = "wasm"), feature = "fs_backend"))]
impl Lix<crate::FsBackend> {
    pub async fn import_filesystem_paths<I, S>(&self, paths: I) -> Result<(), LixError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.ensure_open()?;
        self.backend.import_paths(paths).await
    }

    pub async fn sync_disk_to_lix(&self) -> Result<(), LixError> {
        self.ensure_open()?;
        self.backend.sync_disk_to_lix().await
    }

    fn ensure_open(&self) -> Result<(), LixError> {
        if self.session.is_closed() {
            return Err(lix_closed_error());
        }
        Ok(())
    }
}

#[cfg(all(not(target_family = "wasm"), feature = "fs_backend"))]
fn lix_closed_error() -> LixError {
    LixError::new(LixError::CODE_CLOSED, "Lix handle is closed")
        .with_hint("Open a new Lix handle before calling this method.")
}

#[expect(missing_debug_implementations)]
pub struct LixTransaction<B = InMemoryBackend>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    inner: lix_engine::SessionTransaction<B>,
}

impl<B> LixTransaction<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    /// Executes one SQL statement inside this transaction.
    ///
    /// Writes are staged until `commit()`. Reads use the transaction overlay,
    /// so they can observe writes staged by earlier calls on this handle.
    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        self.inner.execute(sql, params).await
    }

    pub async fn commit(self) -> Result<(), LixError> {
        self.inner.commit().await
    }

    pub async fn rollback(self) -> Result<(), LixError> {
        self.inner.rollback().await
    }
}

pub(crate) async fn open_or_initialize_engine<B>(
    backend: B,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
) -> Result<Engine<B>, LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    match new_engine(backend.clone(), wasm_runtime.clone()).await {
        Ok(engine) => Ok(engine),
        Err(error) if error.code == "LIX_ERROR_NOT_INITIALIZED" => {
            Engine::initialize(backend.clone()).await?;
            new_engine(backend, wasm_runtime).await
        }
        Err(error) => Err(error),
    }
}

#[cfg(feature = "fs_backend")]
pub(crate) async fn open_or_initialize_filesystem_engine<B>(
    backend: B,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
) -> Result<Engine<B>, LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    match new_engine(backend.clone(), wasm_runtime.clone()).await {
        Ok(engine) => Ok(engine),
        Err(error) if error.code == "LIX_ERROR_NOT_INITIALIZED" => {
            Engine::initialize(backend.clone()).await?;
            new_engine(backend, wasm_runtime).await
        }
        Err(error) => Err(error),
    }
}

async fn new_engine<B>(
    backend: B,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
) -> Result<Engine<B>, LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    match wasm_runtime {
        Some(wasm_runtime) => Engine::new_with_wasm_runtime(backend, wasm_runtime).await,
        None => new_engine_with_default_runtime(backend).await,
    }
}

#[cfg(feature = "default_wasm_runtime")]
async fn new_engine_with_default_runtime<B>(backend: B) -> Result<Engine<B>, LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    Engine::new_with_wasm_runtime(backend, crate::default_wasm_runtime::runtime()?).await
}

#[cfg(not(feature = "default_wasm_runtime"))]
async fn new_engine_with_default_runtime<B>(backend: B) -> Result<Engine<B>, LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    Engine::new(backend).await
}
