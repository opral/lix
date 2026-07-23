use lix_engine::telemetry::TelemetrySink;
use lix_engine::wasm::WasmRuntime;
use lix_engine::{
    CreateBranchOptions, CreateBranchReceipt, Engine, EngineOptions, ExecuteBatchStatement,
    ExecuteOptions, ExecuteResult, LixError, Memory, MergeBranchOptions, MergeBranchPreview,
    MergeBranchPreviewOptions, MergeBranchReceipt, ObserveEvents, SessionContext, Storage,
    SwitchBranchOptions, SwitchBranchReceipt, Value,
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

/// Session handle for a Lix repository.
#[expect(missing_debug_implementations)]
pub struct Lix<StorageImpl = Memory>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    engine: Engine<StorageImpl>,
    session: SessionContext<StorageImpl>,
    branch_pinned: bool,
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
    Ok(Lix {
        engine,
        session,
        branch_pinned: false,
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
    /// Opens a branch-pinned session on this handle's existing engine.
    ///
    /// Unlike a workspace session, the returned handle never consults or
    /// changes the workspace's shared active-branch selector. Its branch scope
    /// remains fixed for the lifetime of the handle.
    pub async fn open_session(
        &self,
        active_branch_id: impl Into<String>,
    ) -> Result<Self, LixError> {
        if self.session.is_closed() {
            return Err(LixError::new(
                LixError::CODE_CLOSED,
                "cannot open a branch-pinned session from a closed Lix handle",
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
            branch_pinned: true,
        })
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
            branch_pinned: false,
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
        if self.branch_pinned {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "a branch-pinned session cannot switch branches; open a new session instead",
            ));
        }
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
    async fn branch_pinned_sessions_do_not_follow_the_workspace_selector() {
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
            .open_session(draft.id.clone())
            .await
            .expect("open branch-pinned session");

        pinned
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
                &[
                    Value::Text("/pinned-only.txt".to_string()),
                    Value::Blob(b"draft".to_vec()),
                ],
            )
            .await
            .expect("write pinned branch");

        assert_eq!(
            pinned.active_branch_id().await.expect("pinned branch"),
            draft.id
        );
        assert_eq!(
            root.active_branch_id().await.expect("workspace branch"),
            main_branch_id
        );
        let root_rows = root
            .execute(
                "SELECT path FROM lix_file WHERE path = '/pinned-only.txt'",
                &[],
            )
            .await
            .expect("read workspace branch");
        assert!(root_rows.rows().is_empty());
        let pinned_rows = pinned
            .execute(
                "SELECT path FROM lix_file WHERE path = '/pinned-only.txt'",
                &[],
            )
            .await
            .expect("read pinned branch");
        assert_eq!(pinned_rows.rows().len(), 1);

        let error = pinned
            .switch_branch(SwitchBranchOptions {
                branch_id: main_branch_id,
            })
            .await
            .expect_err("pinned session must reject branch switches");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert_eq!(
            pinned.active_branch_id().await.expect("still pinned"),
            "pinned-draft"
        );
    }

    #[tokio::test]
    async fn branch_pinned_sessions_reject_unknown_branches() {
        let root = open_lix(OpenLixOptions::<Memory>::default())
            .await
            .expect("open root Lix");
        let Err(error) = root.open_session("missing-branch").await else {
            panic!("unknown branch must not open");
        };
        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
        let details = error.details.expect("structured branch error details");
        assert_eq!(details["branch_id"], "missing-branch");
        assert_eq!(details["operation"], "open_session");
        assert_eq!(details["role"], "target");
    }
}
