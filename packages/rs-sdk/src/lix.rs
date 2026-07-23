use lix_engine::telemetry::TelemetrySink;
use lix_engine::wasm::WasmRuntime;
use lix_engine::wasm::v2::WasmTransitionCounters;
use lix_engine::{
    CreateBranchOptions, CreateBranchReceipt, Engine, EngineOptions, ExecuteBatchStatement,
    ExecuteOptions, ExecuteResult, ExecuteStatementMetadata, LixError, Memory, MergeBranchOptions,
    MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt, ObserveEvents,
    SessionContext, Storage, SwitchBranchOptions, SwitchBranchReceipt, Value,
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
    engine: Engine<StorageImpl>,
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
    open_lix_with_optional_telemetry(options, None).await
}

/// Opens a Lix workspace session with an explicit per-engine telemetry sink.
///
/// Telemetry is intentionally a separate entry point so adding the opt-in does
/// not break callers that construct [`OpenLixOptions`] with a struct literal.
pub async fn open_lix_with_telemetry<StorageImpl>(
    options: OpenLixOptions<StorageImpl>,
    telemetry: Arc<dyn TelemetrySink>,
) -> Result<Lix<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    open_lix_with_optional_telemetry(options, Some(telemetry)).await
}

async fn open_lix_with_optional_telemetry<StorageImpl>(
    options: OpenLixOptions<StorageImpl>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
) -> Result<Lix<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let engine =
        open_or_initialize_engine(options.storage, options.wasm_runtime, telemetry).await?;
    let session = engine.open_workspace_session().await?;
    Ok(Lix { engine, session })
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
    /// Opens another workspace session on this handle's existing engine.
    ///
    /// The returned handle has independent session-local state, including its
    /// acknowledged plugin file views and lifecycle. It deliberately clones
    /// the existing [`Engine`] instead of constructing another engine over the
    /// same storage, so engine-wide collaboration and runtime gates remain
    /// shared by every session.
    pub async fn open_workspace_session(&self) -> Result<Self, LixError> {
        if self.session.is_closed() {
            return Err(LixError::new(
                LixError::CODE_CLOSED,
                "cannot open a workspace session from a closed Lix handle",
            ));
        }
        let session = self.engine.open_workspace_session().await?;
        Ok(Self {
            engine: self.engine.clone(),
            session,
        })
    }

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

    #[doc(hidden)]
    pub async fn execute_with_options_and_metadata(
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

    /// Executes statements sequentially against one atomic snapshot.
    /// Pure reads share one read snapshot; batches containing writes retain
    /// transactional read-after-write and rollback semantics.
    pub async fn execute_batch(
        &self,
        statements: &[ExecuteBatchStatement],
    ) -> Result<Vec<ExecuteResult>, LixError> {
        self.session.execute_batch(statements).await
    }

    pub async fn execute_batch_with_options(
        &self,
        statements: &[ExecuteBatchStatement],
        options: ExecuteOptions,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        self.session
            .execute_batch_with_options(statements, options)
            .await
    }

    #[doc(hidden)]
    pub async fn execute_batch_with_options_and_metadata(
        &self,
        statements: &[ExecuteBatchStatement],
        options: ExecuteOptions,
        statement_metadata: Vec<ExecuteStatementMetadata>,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        self.session
            .execute_batch_with_options_and_metadata(statements, options, statement_metadata)
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

    /// Returns engine-local v2 transition counters for profiling and
    /// production invariant monitoring.
    #[doc(hidden)]
    pub fn plugin_v2_transition_counters(&self) -> WasmTransitionCounters {
        self.engine.plugin_v2_transition_counters()
    }

    /// Starts a new engine-local v2 transition measurement window.
    #[doc(hidden)]
    pub fn reset_plugin_v2_transition_counters(&self) {
        self.engine.reset_plugin_v2_transition_counters();
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
    telemetry: Option<Arc<dyn TelemetrySink>>,
) -> Result<Engine<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    match new_engine(storage.clone(), wasm_runtime.clone(), telemetry.clone()).await {
        Ok(engine) => Ok(engine),
        Err(error) if error.code == "LIX_ERROR_NOT_INITIALIZED" => {
            Engine::initialize(storage.clone()).await?;
            new_engine(storage, wasm_runtime, telemetry).await
        }
        Err(error) => Err(error),
    }
}

async fn new_engine<StorageImpl>(
    storage: StorageImpl,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
) -> Result<Engine<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let wasm_runtime = match wasm_runtime {
        Some(wasm_runtime) => Some(wasm_runtime),
        None => default_wasm_runtime()?,
    };
    let mut options = EngineOptions::new();
    if let Some(wasm_runtime) = wasm_runtime {
        options = options.with_wasm_runtime(wasm_runtime);
    }
    if let Some(telemetry) = telemetry {
        options = options.with_telemetry(telemetry);
    }
    Engine::new_with_options(storage, options).await
}

#[cfg(feature = "default_wasm_runtime")]
fn default_wasm_runtime() -> Result<Option<Arc<dyn WasmRuntime>>, LixError> {
    Ok(Some(crate::default_wasm_runtime::runtime()?))
}

#[cfg(not(feature = "default_wasm_runtime"))]
fn default_wasm_runtime() -> Result<Option<Arc<dyn WasmRuntime>>, LixError> {
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn workspace_sessions_share_one_engine_but_have_independent_lifecycles() {
        let root = open_lix(OpenLixOptions::<Memory>::default())
            .await
            .expect("open root Lix");
        let first = root
            .open_workspace_session()
            .await
            .expect("open first child session");
        let second = root
            .open_workspace_session()
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
}
