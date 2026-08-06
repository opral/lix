use lix_engine::telemetry::TelemetrySink;
use lix_engine::wasm::WasmRuntime;
use lix_engine::wasm::WasmTransitionCounters;
use lix_engine::{
    Blob, CreateBranchOptions, CreateBranchReceipt, CreateCheckpointReceipt, Engine, EngineOptions,
    ExecuteBatchStatement, ExecuteIdempotency, ExecuteOptions, ExecuteResult,
    ExecuteStatementMetadata, ExecutionDisposition, LixError, Memory, MergeBranchOptions,
    MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt, ObserveEvents, RedoReceipt,
    SessionContext, Storage, SwitchBranchOptions, SwitchBranchReceipt, UndoReceipt, Value,
};
use std::{future::Future, sync::Arc};

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

/// Clonable workspace-session handle for a Lix repository.
///
/// Clones are concurrent handles to the same logical session: they share the
/// active workspace branch, transaction exclusion, file-view state, and close
/// lifecycle. Use [`Lix::open_session`] when an operation needs an independent
/// pinned session and lifecycle.
#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct Lix<StorageImpl = Memory>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    engine: Arc<Engine<StorageImpl>>,
    session: Arc<SessionContext<StorageImpl>>,
}

/// Opens a Lix workspace session.
///
/// `OpenLixOptions::default()` opens a fresh in-memory storage. Pass a
/// concrete storage in `OpenLixOptions<StorageImpl>` to open a custom storage
/// implementation with the same runtime configuration path.
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
        open_or_initialize_engine(options.storage, options.wasm_runtime, telemetry, None).await?;
    let session = engine.open_workspace_session().await?;
    Ok(Lix {
        engine: Arc::new(engine),
        session: Arc::new(session),
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

/// Opens a workspace with explicit per-Store memory and live-Store limits for
/// Component plugins.
pub async fn open_lix_with_storage_and_plugin_resource_limits<StorageImpl>(
    storage: StorageImpl,
    max_memory_bytes: u64,
    max_live_stores: usize,
) -> Result<Lix<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let engine = open_or_initialize_engine(
        storage,
        None,
        None,
        Some((max_memory_bytes, max_live_stores)),
    )
    .await?;
    let session = engine.open_workspace_session().await?;
    Ok(Lix {
        engine: Arc::new(engine),
        session: Arc::new(session),
    })
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
            session: Arc::new(session),
        })
    }

    /// Opens an independent session pinned to `active_branch_id` on this
    /// handle's existing engine.
    ///
    /// Unlike a workspace session, a pinned session never reads or writes the
    /// shared workspace branch selector. The requested branch is validated
    /// before the child handle is returned. Branch switches update this
    /// handle and its clones in place.
    pub async fn open_session(
        &self,
        active_branch_id: impl Into<String>,
    ) -> Result<Self, LixError> {
        self.open_session_with_account(active_branch_id, lix_engine::ANONYMOUS_ACCOUNT_ID)
            .await
    }

    /// Opens an independent branch-pinned session whose changes are attributed
    /// to `active_account_id`.
    pub async fn open_session_with_account(
        &self,
        active_branch_id: impl Into<String>,
        active_account_id: impl Into<String>,
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
        let session = self
            .engine
            .open_session_with_account(active_branch_id, active_account_id)
            .await?;
        Ok(Self {
            engine: self.engine.clone(),
            session: Arc::new(session),
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

    /// Classifies one SQL execution for a caller that owns its transport
    /// lifecycle.
    ///
    /// The result comes from Lix's parsed and bound statement route. It is
    /// safe for a transport to abandon [`ExecutionDisposition::CancellableRead`]
    /// work; [`ExecutionDisposition::Durable`] work must be allowed to finish.
    pub fn execution_disposition(&self, sql: &str) -> Result<ExecutionDisposition, LixError> {
        self.session.execution_disposition(sql)
    }

    /// Upserts one file's bytes by full logical path without parsing SQL.
    ///
    /// This structured path is intended for file transfer clients. It uses the
    /// engine's filesystem fast-write path and retains normal plugin and
    /// transaction behavior.
    pub async fn upsert_file_content(
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
    pub async fn upsert_file_content_part(
        &self,
        upload_id: impl Into<String>,
        path: impl Into<String>,
        start: u64,
        total_size: u64,
        content: impl Into<Blob>,
    ) -> Result<lix_engine::FileUploadProgress, LixError> {
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
    pub async fn upsert_file_content_batch(
        &self,
        writes: Vec<(String, Blob)>,
    ) -> Result<u64, LixError> {
        self.session.upsert_file_content_batch(writes).await
    }

    /// Reads one file's bytes by full logical path without parsing SQL.
    ///
    /// The returned `None` means the file is absent; `Some` with an empty
    /// [`Blob`] means a present empty file.
    pub async fn read_file_content(
        &self,
        path: impl Into<String>,
        range: Option<std::ops::Range<u64>>,
    ) -> Result<Option<lix_engine::FileRead>, LixError> {
        self.session.read_file_content(path.into(), range).await
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

    #[doc(hidden)]
    pub fn execute_with_idempotency_and_options_and_metadata(
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

    /// Classifies an atomic SQL batch for a caller that owns its transport
    /// lifecycle.
    pub fn execute_batch_disposition(
        &self,
        statements: &[ExecuteBatchStatement],
    ) -> Result<ExecutionDisposition, LixError> {
        self.session.execute_batch_disposition(statements)
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

    #[doc(hidden)]
    pub fn execute_batch_with_idempotency_and_options_and_metadata(
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

    pub async fn active_branch_id(&self) -> Result<String, LixError> {
        self.session.active_branch_id().await
    }

    pub fn active_account_id(&self) -> &str {
        self.session.active_account_id()
    }

    /// Creates an active global account if it does not exist. Existing mutable
    /// account fields are deliberately left unchanged.
    pub async fn ensure_account(&self, id: &str, name: &str, kind: &str) -> Result<(), LixError> {
        let branch_id = self.active_branch_id().await?;
        let system = self
            .open_session_with_account(branch_id, lix_engine::SYSTEM_ACCOUNT_ID)
            .await?;
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
                    Value::Text(lix_engine::GLOBAL_BRANCH_ID.to_string()),
                ],
            )
            .await?;
        system.close().await
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

    pub async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<SwitchBranchReceipt, LixError> {
        self.session.switch_branch(options).await
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

    /// Returns engine-local transition counters for profiling and
    /// production invariant monitoring.
    #[doc(hidden)]
    pub fn plugin_transition_counters(&self) -> WasmTransitionCounters {
        self.engine.plugin_transition_counters()
    }

    /// Starts a new engine-local transition measurement window.
    #[doc(hidden)]
    pub fn reset_plugin_transition_counters(&self) {
        self.engine.reset_plugin_transition_counters();
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

    pub fn execute_with_options(
        &mut self,
        sql: String,
        params: Vec<Value>,
        options: ExecuteOptions,
    ) -> impl Future<Output = Result<ExecuteResult, LixError>> + Send + '_ {
        self.inner.execute_with_options(sql, params, options)
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
        None => Some(crate::default_wasm_runtime::runtime()?),
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
                id: Some("01920000-0000-7000-8000-000000000501".to_string()),
                name: "Pinned draft".to_string(),
                from_commit_id: None,
            })
            .await
            .expect("create draft");

        let pinned = root
            .open_session(main_branch_id.clone())
            .await
            .expect("open pinned main session");
        let pinned_clone = pinned.clone();
        let receipt = pinned
            .switch_branch(SwitchBranchOptions {
                branch_id: draft.id.clone(),
            })
            .await
            .expect("switch pinned session");

        assert_eq!(receipt.branch_id, draft.id);
        assert_eq!(
            pinned.active_branch_id().await.unwrap(),
            "01920000-0000-7000-8000-000000000501"
        );
        assert_eq!(
            pinned_clone.active_branch_id().await.unwrap(),
            "01920000-0000-7000-8000-000000000501"
        );
        assert_eq!(root.active_branch_id().await.unwrap(), main_branch_id);

        let Err(error) = root.open_session("missing-branch").await else {
            panic!("missing branch must not open");
        };
        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
    }

    #[tokio::test]
    async fn accounts_are_mutable_and_changes_have_one_required_account() {
        const AUTHOR_ID: &str = "01920000-0000-7000-8000-000000000601";
        const UNUSED_ID: &str = "01920000-0000-7000-8000-000000000602";
        let root = open_lix(OpenLixOptions::<Memory>::default())
            .await
            .expect("open root Lix");

        root.ensure_account(AUTHOR_ID, "Ada", "human")
            .await
            .expect("provision author");
        root.ensure_account(UNUSED_ID, "Unused", "human")
            .await
            .expect("provision unused account");

        let author = root
            .open_session_with_account(
                root.active_branch_id().await.expect("active branch"),
                AUTHOR_ID,
            )
            .await
            .expect("open attributed session");
        assert_eq!(author.active_account_id(), AUTHOR_ID);
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
                "INSERT INTO lix_key_value (key, value) VALUES ('account-test', lix_json('true'))",
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
            .open_session_with_account(
                root.active_branch_id().await.expect("active branch"),
                lix_engine::SYSTEM_ACCOUNT_ID,
            )
            .await
            .expect("open system session");
        system
            .execute(
                "UPDATE lix_account_by_branch SET name = 'Ada Lovelace' \
                 WHERE id = $1 AND lixcol_branch_id = $2",
                &[
                    Value::Text(AUTHOR_ID.to_string()),
                    Value::Text(lix_engine::GLOBAL_BRANCH_ID.to_string()),
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
            .open_session_with_account(
                root.active_branch_id().await.expect("active branch"),
                UNUSED_ID,
            )
            .await
            .expect("open unused account session");

        system
            .execute(
                "DELETE FROM lix_account_by_branch WHERE id = $1 AND lixcol_branch_id = $2",
                &[
                    Value::Text(UNUSED_ID.to_string()),
                    Value::Text(lix_engine::GLOBAL_BRANCH_ID.to_string()),
                ],
            )
            .await
            .expect("delete unused account");
        let error = unused
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('deleted-account', lix_json('true'))",
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
                    Value::Text(lix_engine::GLOBAL_BRANCH_ID.to_string()),
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
                    Value::Text(lix_engine::GLOBAL_BRANCH_ID.to_string()),
                ],
            )
            .await
            .expect("disable author");
        let error = author
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('disabled-account', lix_json('true'))",
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
                    Value::Text(lix_engine::ANONYMOUS_ACCOUNT_ID.to_string()),
                    Value::Text(lix_engine::GLOBAL_BRANCH_ID.to_string()),
                ],
            )
            .await
            .expect_err("built-in accounts must remain active");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }
}
