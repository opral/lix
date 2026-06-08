use lix_engine::wasm::WasmRuntime;
use lix_engine::{
    Backend, CreateBranchOptions, CreateBranchReceipt, Engine, ExecuteResult, FsDirEntry,
    FsMkdirOptions, FsRmOptions, FsWriteOptions, InMemoryBackend, InstalledPluginInfo, LixError,
    MergeBranchOptions, MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    SessionContext, SwitchBranchOptions, SwitchBranchReceipt, Value,
};
#[cfg(not(target_family = "wasm"))]
use std::path::Path;
use std::sync::Arc;

#[cfg(not(target_family = "wasm"))]
use crate::worktree::WorktreeSupervisor;

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
    session: SessionContext<B>,
    #[cfg(not(target_family = "wasm"))]
    worktree: Option<WorktreeSupervisor<B>>,
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
    let engine = open_or_initialize_engine(options.backend, options.wasm_runtime).await?;
    let session = engine.open_workspace_session().await?;
    Ok(Lix {
        _engine: engine,
        session,
        #[cfg(not(target_family = "wasm"))]
        worktree: None,
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

#[cfg(not(target_family = "wasm"))]
pub async fn open_lix_with_backend_and_worktree<B, P>(
    backend: B,
    worktree_path: P,
) -> Result<Lix<B>, LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
    P: AsRef<Path>,
{
    let engine = open_or_initialize_engine(backend, None).await?;
    let session = engine.open_workspace_session().await?;
    let worktree = WorktreeSupervisor::open(engine.clone(), worktree_path.as_ref()).await?;
    Ok(Lix {
        _engine: engine,
        session,
        worktree: Some(worktree),
    })
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
        let should_sync_worktree = sql_may_change_filesystem(sql);
        let result = self.session.execute(sql, params).await?;
        if should_sync_worktree {
            self.sync_worktree_from_lix().await?;
        }
        Ok(result)
    }

    pub async fn begin_transaction(&self) -> Result<LixTransaction<B>, LixError> {
        Ok(LixTransaction {
            inner: self.session.begin_transaction().await?,
            #[cfg(not(target_family = "wasm"))]
            worktree: self.worktree.clone(),
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
        self.sync_worktree_from_lix().await?;
        Ok(receipt)
    }

    pub async fn merge_branch(
        &self,
        options: MergeBranchOptions,
    ) -> Result<MergeBranchReceipt, LixError> {
        let receipt = self.session.merge_branch(options).await?;
        self.sync_worktree_from_lix().await?;
        Ok(receipt)
    }

    pub async fn merge_branch_preview(
        &self,
        options: MergeBranchPreviewOptions,
    ) -> Result<MergeBranchPreview, LixError> {
        self.session.merge_branch_preview(options).await
    }

    pub async fn install_plugin_archive(&self, archive_bytes: &[u8]) -> Result<(), LixError> {
        self.session.install_plugin_archive(archive_bytes).await?;
        self.sync_worktree_from_lix().await
    }

    pub async fn list_installed_plugins(&self) -> Result<Vec<InstalledPluginInfo>, LixError> {
        self.session.list_installed_plugins().await
    }

    pub async fn write_file(
        &self,
        path: &str,
        data: Vec<u8>,
        options: FsWriteOptions,
    ) -> Result<(), LixError> {
        self.session.fs().write_file(path, data, options).await?;
        self.sync_worktree_from_lix().await
    }

    pub async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, LixError> {
        self.session.fs().read_file(path).await
    }

    pub async fn mkdir(&self, path: &str, options: FsMkdirOptions) -> Result<(), LixError> {
        self.session.fs().mkdir(path, options).await?;
        self.sync_worktree_from_lix().await
    }

    pub async fn readdir(&self, path: &str) -> Result<Option<Vec<FsDirEntry>>, LixError> {
        self.session.fs().readdir(path).await
    }

    pub async fn rm(&self, path: &str, options: FsRmOptions) -> Result<(), LixError> {
        self.session.fs().rm(path, options).await?;
        self.sync_worktree_from_lix().await
    }

    pub async fn close(&self) -> Result<(), LixError> {
        #[cfg(not(target_family = "wasm"))]
        if let Some(worktree) = &self.worktree {
            worktree.close().await?;
        }
        self.session.close().await
    }

    #[cfg(not(target_family = "wasm"))]
    async fn sync_worktree_from_lix(&self) -> Result<(), LixError> {
        if let Some(worktree) = &self.worktree {
            worktree.sync_from_lix().await?;
        }
        Ok(())
    }

    #[cfg(target_family = "wasm")]
    async fn sync_worktree_from_lix(&self) -> Result<(), LixError> {
        Ok(())
    }
}

#[expect(missing_debug_implementations)]
pub struct LixTransaction<B = InMemoryBackend>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    inner: lix_engine::SessionTransaction<B>,
    #[cfg(not(target_family = "wasm"))]
    worktree: Option<WorktreeSupervisor<B>>,
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
        #[cfg(not(target_family = "wasm"))]
        let worktree = self.worktree.clone();
        self.inner.commit().await?;
        #[cfg(not(target_family = "wasm"))]
        if let Some(worktree) = worktree {
            worktree.sync_from_lix().await?;
        }
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), LixError> {
        self.inner.rollback().await
    }
}

async fn open_or_initialize_engine<B>(
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

fn sql_may_change_filesystem(sql: &str) -> bool {
    let sql = sql.trim_start();
    let Some(first_word) = sql.split_whitespace().next() else {
        return false;
    };
    matches!(
        first_word.to_ascii_lowercase().as_str(),
        "insert" | "update" | "delete"
    )
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
