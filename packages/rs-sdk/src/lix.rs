use lix_engine::telemetry::TelemetrySink;
use lix_engine::wasm::WasmRuntime;
use lix_engine::{
    CreateBranchOptions, CreateBranchReceipt, Engine, EngineOptions, ExecuteBatchStatement,
    ExecuteOptions, ExecuteResult, LixError, Memory, MergeBranchOptions, MergeBranchPreview,
    MergeBranchPreviewOptions, MergeBranchReceipt, ObserveEvents, SessionContext, Storage,
    SwitchBranchOptions, SwitchBranchReceipt, Value,
};
use std::sync::Arc;

use crate::client_state::ClientState;

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
    /// Returns a borrowed handle to JSON state owned by this client storage.
    ///
    /// Remote SDK integrations should expose this handle from a separate
    /// client-only local Lix while continuing to route workspace operations to
    /// the remote Lix.
    pub fn client_state(&self) -> ClientState<'_, StorageImpl> {
        ClientState::new(self)
    }

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

    /// Opens an independent session pinned to `active_branch_id` on this
    /// handle's existing engine.
    ///
    /// Unlike a workspace session, a pinned session never reads or writes the
    /// shared workspace branch selector. The requested branch is validated
    /// before the child handle is returned. To switch it, use
    /// [`Self::switch_branch_session`] and retain the returned replacement.
    pub async fn open_session(
        &self,
        active_branch_id: impl Into<String>,
    ) -> Result<Self, LixError> {
        if self.session.is_closed() {
            return Err(LixError::new(
                LixError::CODE_CLOSED,
                "cannot open a pinned session from a closed Lix handle",
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
                "open_session",
                "target",
            ));
        }
        let session = self.engine.open_session(active_branch_id).await?;
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
        if self.session.is_pinned() {
            return Err(LixError::new(
                LixError::CODE_INVALID_SESSION_STATE,
                "switch_branch() cannot replace a borrowed pinned Lix handle",
            )
            .with_hint("use switch_branch_session() and retain the returned Lix handle"));
        }
        let (_lix, receipt) = self.switch_branch_session(options).await?;
        Ok(receipt)
    }

    /// Switches branches and returns the replacement handle produced by the
    /// engine session transition.
    ///
    /// Callers that own pinned sessions must retain the returned handle. The
    /// compatibility [`Self::switch_branch`] method intentionally preserves
    /// its existing receipt-only API for workspace sessions.
    pub async fn switch_branch_session(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<(Self, SwitchBranchReceipt), LixError> {
        let (session, receipt) = self.session.switch_branch(options).await?;
        Ok((
            Self {
                engine: self.engine.clone(),
                session,
            },
            receipt,
        ))
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

    #[tokio::test]
    async fn pinned_sessions_validate_and_retain_branch_switches() {
        let root = open_lix(OpenLixOptions::<Memory>::default())
            .await
            .expect("open root Lix");
        let main_branch_id = root.active_branch_id().await.expect("main branch");
        let draft = root
            .create_branch(CreateBranchOptions {
                id: Some("pinned-draft".to_string()),
                name: "Pinned draft".to_string(),
                from_commit_id: None,
            })
            .await
            .expect("create draft");

        let pinned = root
            .open_session(main_branch_id.clone())
            .await
            .expect("open pinned main session");
        let error = pinned
            .switch_branch(SwitchBranchOptions {
                branch_id: draft.id.clone(),
            })
            .await
            .expect_err("receipt-only switching must reject pinned sessions");
        assert_eq!(error.code, LixError::CODE_INVALID_SESSION_STATE);
        let (switched, receipt) = pinned
            .switch_branch_session(SwitchBranchOptions {
                branch_id: draft.id.clone(),
            })
            .await
            .expect("switch pinned session");

        assert_eq!(receipt.branch_id, draft.id);
        assert_eq!(pinned.active_branch_id().await.unwrap(), main_branch_id);
        assert_eq!(switched.active_branch_id().await.unwrap(), "pinned-draft");
        assert_eq!(root.active_branch_id().await.unwrap(), main_branch_id);

        let Err(error) = root.open_session("missing-branch").await else {
            panic!("missing branch must not open");
        };
        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
    }
}
