use lix_engine::wasm::WasmRuntime;
use lix_engine::{
    CreateBranchOptions, CreateBranchReceipt, Engine, ExecuteOptions, ExecuteResult, LixError,
    Memory, MergeBranchOptions, MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    ObserveEvents, SessionContext, Storage, SwitchBranchOptions, SwitchBranchReceipt, Value,
};
use std::sync::Arc;

/// Options for opening a Lix workspace session.
#[expect(missing_debug_implementations)]
pub struct OpenLixOptions<StorageImpl = Memory> {
    pub storage: StorageImpl,
    pub wasm_runtime: Option<Arc<dyn WasmRuntime>>,
}

impl Default for OpenLixOptions<Memory> {
    fn default() -> Self {
        Self {
            storage: Memory::new(),
            wasm_runtime: None,
        }
    }
}

impl<StorageImpl> OpenLixOptions<StorageImpl> {
    pub fn new(storage: StorageImpl) -> Self {
        Self {
            storage,
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
pub struct Lix<StorageImpl = Memory>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    _engine: Engine<StorageImpl>,
    session: SessionContext<StorageImpl>,
}

/// Opens a Lix workspace session.
///
/// `OpenLixOptions::default()` opens a fresh in-memory storage. Pass a
/// concrete storage in `OpenLixOptions<StorageImpl>` to open SQLite or custom storage implementations
/// with the same runtime configuration path.
pub async fn open_lix<StorageImpl>(
    options: OpenLixOptions<StorageImpl>,
) -> Result<Lix<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let engine = open_or_initialize_engine(options.storage, options.wasm_runtime).await?;
    let session = engine.open_workspace_session().await?;
    Ok(Lix {
        _engine: engine,
        session,
    })
}

pub async fn open_lix_with_storage<StorageImpl>(
    storage: StorageImpl,
) -> Result<Lix<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    open_lix(OpenLixOptions::new(storage)).await
}

impl<StorageImpl> Lix<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
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

    pub async fn execute_with_options(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        self.session
            .execute_with_options(sql, params, options)
            .await
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

#[expect(missing_debug_implementations)]
pub struct LixTransaction<StorageImpl = Memory>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    inner: lix_engine::SessionTransaction<StorageImpl>,
}

impl<StorageImpl> LixTransaction<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
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

    pub async fn execute_with_options(
        &mut self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        self.inner.execute_with_options(sql, params, options).await
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
) -> Result<Engine<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    match new_engine(storage.clone(), wasm_runtime.clone()).await {
        Ok(engine) => Ok(engine),
        Err(error) if error.code == "LIX_ERROR_NOT_INITIALIZED" => {
            Engine::initialize(storage.clone()).await?;
            new_engine(storage, wasm_runtime).await
        }
        Err(error) => Err(error),
    }
}

async fn new_engine<StorageImpl>(
    storage: StorageImpl,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
) -> Result<Engine<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    match wasm_runtime {
        Some(wasm_runtime) => Engine::new_with_wasm_runtime(storage, wasm_runtime).await,
        None => new_engine_with_default_runtime(storage).await,
    }
}

#[cfg(feature = "default_wasm_runtime")]
async fn new_engine_with_default_runtime<StorageImpl>(
    storage: StorageImpl,
) -> Result<Engine<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    Engine::new_with_wasm_runtime(storage, crate::default_wasm_runtime::runtime()?).await
}

#[cfg(not(feature = "default_wasm_runtime"))]
async fn new_engine_with_default_runtime<StorageImpl>(
    storage: StorageImpl,
) -> Result<Engine<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    Engine::new(storage).await
}
