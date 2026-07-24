#![allow(clippy::match_wild_err_arm, clippy::option_if_let_else)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tracing::Instrument as _;

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::{BinaryCasContext, BlobDataReader};
use crate::branch::{
    BranchContext, BranchLifecycle, BranchOperation, BranchRefReader, BranchReferenceRole,
};
use crate::catalog::{CatalogContext, CatalogFingerprint};
use crate::commit_graph::{CommitGraphContext, CommitGraphReader};
use crate::entity_pk::EntityPk;
use crate::filesystem::FilesystemPathIndexReader;
use crate::functions::FunctionProviderHandle;
use crate::json_store::JsonStoreContext;
use crate::live_state::{LiveStateContext, LiveStateReader, LiveStateRowRequest};
use crate::observe_coordinator::ObserveCoordinator;
use crate::observe_invalidation::ObserveInvalidation;
use crate::plugin::{PluginComponentHost, PluginRuntimeHost};
use crate::sql2::{
    ChangelogQuerySource, HistoryQuerySource, SessionFileViews, SqlChangelogQuerySource,
    SqlExecutionContext, SqlHistoryQuerySource, SqlPlanningCache,
};
use crate::storage_adapter::Storage;
use crate::storage_adapter::{Memory, StorageReadOptions};
use crate::storage_adapter::{SharedStorageAdapterRead, StorageAdapter, StorageAdapterRead};
use crate::telemetry::TelemetrySink;
use crate::tracked_state::TrackedStateContext;
use crate::transaction::{Transaction, open_transaction};
use crate::{LixError, NullableKeyFilter};

use super::transaction::{SessionOperationGuard, SessionTransactionManager, SessionWriteLease};

pub(crate) const WORKSPACE_BRANCH_KEY: &str = "lix_workspace_branch_id";

#[derive(Clone)]
pub(crate) enum SessionMode {
    Pinned { branch_id: String },
    Workspace,
}

/// Session-context state for engine execution.
///
/// A session context pins the active branch selector and shared execution
/// services. Parent-handle `execute(...)` runs as an implicit single-statement
/// transaction. Explicit transactions hold the session execution lease until
/// commit or rollback, so all SQL during that window must run through the
/// transaction handle.
#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct SessionContext<StorageImpl: Storage = Memory> {
    pub(super) mode: SessionMode,
    pub(super) storage: StorageAdapter<StorageImpl>,
    pub(super) live_state: Arc<LiveStateContext>,
    pub(super) tracked_state: Arc<TrackedStateContext>,
    pub(super) binary_cas: Arc<BinaryCasContext>,
    pub(super) branch_ctx: Arc<BranchContext>,
    pub(super) catalog_context: Arc<CatalogContext>,
    pub(super) sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
    pub(super) deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
    pub(super) collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
    pub(super) file_views: SessionFileViews,
    pub(super) observe_coordinator: Arc<ObserveCoordinator>,
    pub(super) observe_invalidation: Arc<ObserveInvalidation>,
    pub(super) plugin_host: PluginRuntimeHost,
    pub(super) telemetry: Option<Arc<dyn TelemetrySink>>,
    transaction_manager: SessionTransactionManager,
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) async fn open_workspace(
        storage: StorageAdapter<StorageImpl>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        branch_ctx: Arc<BranchContext>,
        catalog_context: Arc<CatalogContext>,
        sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
        deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
        collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
        observe_coordinator: Arc<ObserveCoordinator>,
        observe_invalidation: Arc<ObserveInvalidation>,
        plugin_host: PluginRuntimeHost,
        telemetry: Option<Arc<dyn TelemetrySink>>,
    ) -> Result<Self, LixError> {
        let session = Self::new(
            SessionMode::Workspace,
            storage,
            live_state,
            tracked_state,
            binary_cas,
            branch_ctx,
            catalog_context,
            sql_planning_cache,
            deterministic_runtime_gate,
            collaboration_write_gate,
            observe_coordinator,
            observe_invalidation,
            plugin_host,
            telemetry,
        );
        session.active_branch_id().await?;
        Ok(session)
    }

    pub(crate) async fn open(
        active_branch_id: String,
        storage: StorageAdapter<StorageImpl>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        branch_ctx: Arc<BranchContext>,
        catalog_context: Arc<CatalogContext>,
        sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
        deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
        collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
        observe_coordinator: Arc<ObserveCoordinator>,
        observe_invalidation: Arc<ObserveInvalidation>,
        plugin_host: PluginRuntimeHost,
        telemetry: Option<Arc<dyn TelemetrySink>>,
    ) -> Result<Self, LixError> {
        Ok(Self::new(
            SessionMode::Pinned {
                branch_id: active_branch_id,
            },
            storage,
            live_state,
            tracked_state,
            binary_cas,
            branch_ctx,
            catalog_context,
            sql_planning_cache,
            deterministic_runtime_gate,
            collaboration_write_gate,
            observe_coordinator,
            observe_invalidation,
            plugin_host,
            telemetry,
        ))
    }

    pub(super) fn new(
        mode: SessionMode,
        storage: StorageAdapter<StorageImpl>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        branch_ctx: Arc<BranchContext>,
        catalog_context: Arc<CatalogContext>,
        sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
        deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
        collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
        observe_coordinator: Arc<ObserveCoordinator>,
        observe_invalidation: Arc<ObserveInvalidation>,
        plugin_host: PluginRuntimeHost,
        telemetry: Option<Arc<dyn TelemetrySink>>,
    ) -> Self {
        Self::new_with_transaction_manager(
            mode,
            storage,
            live_state,
            tracked_state,
            binary_cas,
            branch_ctx,
            catalog_context,
            sql_planning_cache,
            deterministic_runtime_gate,
            collaboration_write_gate,
            observe_coordinator,
            observe_invalidation,
            plugin_host,
            telemetry,
            SessionTransactionManager::new(),
            SessionFileViews::default(),
        )
    }

    pub(super) fn new_with_transaction_manager(
        mode: SessionMode,
        storage: StorageAdapter<StorageImpl>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        branch_ctx: Arc<BranchContext>,
        catalog_context: Arc<CatalogContext>,
        sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
        deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
        collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
        observe_coordinator: Arc<ObserveCoordinator>,
        observe_invalidation: Arc<ObserveInvalidation>,
        plugin_host: PluginRuntimeHost,
        telemetry: Option<Arc<dyn TelemetrySink>>,
        transaction_manager: SessionTransactionManager,
        file_views: SessionFileViews,
    ) -> Self {
        Self {
            mode,
            storage,
            live_state,
            tracked_state,
            binary_cas,
            branch_ctx,
            catalog_context,
            sql_planning_cache,
            deterministic_runtime_gate,
            collaboration_write_gate,
            file_views,
            observe_coordinator,
            observe_invalidation,
            plugin_host,
            telemetry,
            transaction_manager,
        }
    }

    /// Releases this logical session handle. This is a lifecycle boundary only:
    /// successful writes are committed before their operation returns.
    pub async fn close(&self) -> Result<(), LixError> {
        self.transaction_manager.close().await?;
        self.observe_invalidation.bump();
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.transaction_manager.is_closed()
    }

    #[doc(hidden)]
    pub fn is_pinned(&self) -> bool {
        matches!(self.mode, SessionMode::Pinned { .. })
    }

    #[cfg(test)]
    pub(crate) fn operation_in_progress_count_for_test(&self) -> usize {
        self.transaction_manager.operation_count_for_test()
    }

    #[cfg(test)]
    pub(crate) fn commit_in_progress_for_test(&self) -> bool {
        self.transaction_manager.commit_in_progress_for_test()
    }

    #[cfg(test)]
    pub(crate) fn active_transaction_for_test(&self) -> bool {
        self.transaction_manager.active_transaction_for_test()
    }

    pub(super) fn transaction_manager(&self) -> SessionTransactionManager {
        self.transaction_manager.clone()
    }

    pub(crate) fn ensure_open(&self) -> Result<(), LixError> {
        self.transaction_manager.ensure_open()
    }

    pub(super) async fn deterministic_mode_enabled(&self) -> Result<bool, LixError> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let live_state = self.live_state.reader(&read);
        crate::functions::deterministic_mode_enabled(&live_state).await
    }

    pub(super) async fn lock_deterministic_runtime(
        &self,
    ) -> crate::functions::DeterministicRuntimeGuard {
        Arc::clone(&self.deterministic_runtime_gate)
            .lock_owned()
            .await
    }

    pub(super) fn ensure_observe_registration_allowed(&self) -> Result<(), LixError> {
        self.transaction_manager
            .ensure_observe_registration_allowed()
    }

    pub(super) async fn begin_waitable_session_operation(
        &self,
    ) -> Result<SessionOperationGuard, LixError> {
        self.transaction_manager
            .begin_waitable_session_operation()
            .await
    }

    pub(super) async fn begin_session_write_lease(&self) -> Result<SessionWriteLease, LixError> {
        self.transaction_manager.begin_write_lease().await
    }

    pub(super) fn begin_explicit_session_write_lease(&self) -> Result<SessionWriteLease, LixError> {
        self.transaction_manager.begin_explicit_write_lease()
    }

    pub(super) async fn begin_session_write_access(&self) -> Result<SessionWriteAccess, LixError> {
        let write_lease = self.begin_session_write_lease().await?;
        self.begin_session_write_access_with_lease(write_lease, true)
            .await
    }

    pub(super) async fn begin_explicit_session_write_access(
        &self,
    ) -> Result<SessionWriteAccess, LixError> {
        let write_lease = self.begin_explicit_session_write_lease()?;
        // Explicit transactions can remain open across arbitrary application
        // awaits. Serializing them for their entire lifetime would allow one
        // client to block every engine writer indefinitely. The collaboration
        // MVP serializes bounded implicit statements and execute batches.
        self.begin_session_write_access_with_lease(write_lease, false)
            .await
    }

    async fn begin_session_write_access_with_lease(
        &self,
        write_lease: SessionWriteLease,
        serialize_collaboration_write: bool,
    ) -> Result<SessionWriteAccess, LixError> {
        let collaboration_write_guard = if serialize_collaboration_write {
            Some(
                Arc::clone(&self.collaboration_write_gate)
                    .lock_owned()
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.collaboration_gate_wait"
                    ))
                    .await,
            )
        } else {
            None
        };
        let write_access = SessionWriteAccess {
            _write_lease: write_lease,
            collaboration_write_guard,
        };
        self.ensure_open()?;
        Ok(write_access)
    }

    /// Resolves the branch this session should operate on right now.
    ///
    /// This is a read-path helper. Write flows must resolve the active branch
    /// through the transaction capability so the read is scoped to the
    /// same storage transaction as the writes it influences.
    ///
    /// Pinned sessions are pure in-memory views over one branch. Workspace
    /// sessions read the shared workspace selector from untracked global
    /// `lix_key_value` state so multiple open app sessions can observe the same
    /// active workspace branch.
    pub async fn active_branch_id(&self) -> Result<String, LixError> {
        let _operation_guard = self.begin_waitable_session_operation().await?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let result = self.active_branch_id_from_reader(&read).await;
        match result {
            Ok(branch_id) => Ok(branch_id),
            Err(error) => Err(error),
        }
    }

    #[doc(hidden)]
    pub async fn storage_mutation_revision(&self) -> Result<Option<Vec<u8>>, LixError> {
        let _operation_guard = self.begin_waitable_session_operation().await?;
        Ok(self
            .storage
            .load_mutation_revision()
            .await?
            .map(|revision| revision.to_vec()))
    }

    pub(super) async fn active_branch_id_from_reader<S>(
        &self,
        reader: &S,
    ) -> Result<String, LixError>
    where
        S: StorageAdapterRead + ?Sized,
    {
        self.ensure_open()?;
        match &self.mode {
            SessionMode::Pinned { branch_id } => Ok(branch_id.clone()),
            SessionMode::Workspace => self.load_workspace_branch_id(reader).await,
        }
    }

    async fn load_workspace_branch_id<S>(&self, reader: &S) -> Result<String, LixError>
    where
        S: StorageAdapterRead + ?Sized,
    {
        let row = self
            .live_state
            .reader(reader)
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single(WORKSPACE_BRANCH_KEY),
                file_id: NullableKeyFilter::Null,
            })
            .await?
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "workspace branch selector is missing lix_key_value:lix_workspace_branch_id",
                )
            })?;
        let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "workspace branch selector is missing snapshot_content",
            )
        })?;
        let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("workspace branch selector snapshot is invalid JSON: {error}"),
            )
        })?;
        let branch_id = snapshot
            .get("value")
            .and_then(JsonValue::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "workspace branch selector value must be a non-empty string",
                )
            })?
            .to_string();

        let branch_ref = self.branch_ctx.ref_reader(reader);
        BranchLifecycle::new(&branch_ref)
            .require_existing_ref(
                &branch_id,
                BranchOperation::LoadWorkspaceSelector,
                BranchReferenceRole::WorkspaceSelector,
            )
            .await?;

        Ok(branch_id)
    }

    pub(crate) async fn with_write_transaction<T, F>(&self, f: F) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(
            &'tx mut Transaction<StorageImpl>,
        ) -> Pin<Box<dyn Future<Output = Result<T, LixError>> + 'tx>>,
    {
        self.ensure_open()?;
        let write_access = self.begin_session_write_access().await?;
        self.with_write_transaction_reserved(write_access, f).await
    }

    pub(super) async fn with_write_transaction_reserved<T, F>(
        &self,
        write_access: SessionWriteAccess,
        f: F,
    ) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(
            &'tx mut Transaction<StorageImpl>,
        ) -> Pin<Box<dyn Future<Output = Result<T, LixError>> + 'tx>>,
    {
        let planner_validation_is_serialized = write_access.serializes_collaboration_writes();
        let _deterministic_runtime_guard = if self.deterministic_mode_enabled().await? {
            Some(self.lock_deterministic_runtime().await)
        } else {
            None
        };
        let opened = open_transaction(
            &self.mode,
            self.storage.clone(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            self.plugin_host.clone(),
            Arc::clone(&self.branch_ctx),
            Arc::clone(&self.catalog_context),
            Arc::clone(&self.sql_planning_cache),
            self.file_views.clone(),
        )
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.transaction_open"
        ))
        .await?;
        self.ensure_open()?;
        let mut transaction = opened.transaction;
        transaction.attach_commit_boundary(self.transaction_commit_boundary());
        if planner_validation_is_serialized {
            transaction.trust_serialized_filesystem_planner();
        }
        let runtime_functions = opened.runtime_functions;

        match f(&mut transaction)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.transaction_plan_and_stage"
            ))
            .await
        {
            Ok(value) => {
                self.ensure_open()?;
                let outcome = transaction.commit(&runtime_functions).await?;
                drop(write_access);
                self.observe_invalidation
                    .bump_if_storage_changed(&outcome.storage_stats);
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    pub(super) fn begin_commit(&self) -> crate::transaction::CommitBoundaryGuard {
        self.transaction_manager.begin_commit()
    }

    pub(super) fn transaction_commit_boundary(
        &self,
    ) -> crate::transaction::TransactionCommitBoundary {
        self.transaction_manager.transaction_commit_boundary()
    }
}

impl<StorageImpl> PluginComponentHost for SessionContext<StorageImpl>
where
    StorageImpl: Storage,
{
    fn plugin_component_cache(
        &self,
    ) -> &std::sync::Mutex<std::collections::BTreeMap<String, crate::plugin::CachedPluginComponent>>
    {
        self.plugin_host.plugin_component_cache()
    }

    fn wasm_runtime(&self) -> &Arc<dyn crate::wasm::WasmRuntime> {
        self.plugin_host.wasm_runtime()
    }
}

pub(super) struct SessionWriteAccess {
    _write_lease: SessionWriteLease,
    collaboration_write_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl SessionWriteAccess {
    fn serializes_collaboration_writes(&self) -> bool {
        self.collaboration_write_guard.is_some()
    }
}

pub(super) fn closed_error() -> LixError {
    LixError::new(LixError::CODE_CLOSED, "Lix handle is closed")
        .with_hint("Open a new Lix handle before calling this method.")
}

/// Read-only SQL execution context derived from a session.
///
/// Write statements re-plan against `Transaction`; this context intentionally
/// has no write stager.
pub(super) struct SessionSqlExecutionContext<'a, R: crate::storage_adapter::StorageRead> {
    pub(super) active_branch_id: &'a str,
    pub(super) read_store: SharedStorageAdapterRead<R>,
    pub(super) live_state: Arc<LiveStateContext>,
    pub(super) binary_cas: Arc<BinaryCasContext>,
    pub(super) branch_ctx: Arc<BranchContext>,
    pub(super) catalog_context: Arc<CatalogContext>,
    pub(super) functions: FunctionProviderHandle,
    pub(super) plugin_host: PluginRuntimeHost,
    pub(super) file_views: Option<SessionFileViews>,
}

#[async_trait]
impl<R> SqlExecutionContext for SessionSqlExecutionContext<'_, R>
where
    R: crate::storage_adapter::StorageRead + 'static,
{
    type ReadStore = SharedStorageAdapterRead<R>;

    fn active_branch_id(&self) -> &str {
        self.active_branch_id
    }

    #[expect(trivial_casts)]
    fn live_state(&self) -> Arc<dyn LiveStateReader> {
        Arc::new(self.live_state.reader(self.read_store.clone())) as Arc<dyn LiveStateReader>
    }

    fn filesystem_path_index(&self) -> Arc<dyn FilesystemPathIndexReader> {
        let reader: Arc<dyn FilesystemPathIndexReader> =
            Arc::new(self.live_state.reader(self.read_store.clone()));
        reader
    }

    fn history_query_source(
        &self,
        default_as_of_commit_id: String,
    ) -> SqlHistoryQuerySource<Self::ReadStore> {
        HistoryQuerySource {
            store: self.read_store.clone(),
            json_reader: JsonStoreContext::new().reader(self.read_store.clone()),
            default_as_of_commit_id,
        }
    }

    fn changelog_query_source(&self) -> SqlChangelogQuerySource<Self::ReadStore> {
        ChangelogQuerySource {
            store: self.read_store.clone(),
            json_reader: JsonStoreContext::new().reader(self.read_store.clone()),
        }
    }

    fn commit_graph(&self) -> Box<dyn CommitGraphReader> {
        Box::new(CommitGraphContext::new().reader(self.read_store.clone()))
    }

    fn branch_ref(&self) -> Arc<dyn BranchRefReader> {
        Arc::new(self.branch_ctx.ref_reader(self.read_store.clone()))
    }

    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    #[expect(trivial_casts)]
    fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
        Arc::new(self.binary_cas.reader(self.read_store.clone())) as Arc<dyn BlobDataReader>
    }

    async fn load_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        let live_state = self.live_state();
        self.catalog_context
            .schema_jsons_for_sql_read_planning(live_state.as_ref(), self.active_branch_id)
            .await
    }

    fn plugin_host(&self) -> PluginRuntimeHost {
        self.plugin_host.clone()
    }

    fn session_file_views(&self) -> Option<SessionFileViews> {
        self.file_views.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::task::{Context, Poll};
    use std::thread;
    use std::time::{Duration, Instant};

    use crate::Engine;
    use crate::storage::{
        Memory, MemoryRead, MemoryWrite, ReadOptions, StorageError, WriteOptions,
    };
    use crate::storage_adapter::Storage;
    use futures_util::task::noop_waker_ref;

    const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(2);

    fn wait_until(description: &str, mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + TEST_WAIT_TIMEOUT;
        while !condition() {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {description}"
            );
            thread::yield_now();
        }
    }

    fn assert_close_pending<F>(mut future: Pin<&mut F>)
    where
        F: Future<Output = Result<(), crate::LixError>>,
    {
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(
            matches!(future.as_mut().poll(&mut cx), Poll::Pending),
            "close should remain pending while guarded work is in progress"
        );
    }

    async fn assert_close_finishes<F>(future: Pin<&mut F>, description: &str)
    where
        F: Future<Output = Result<(), crate::LixError>>,
    {
        tokio::time::timeout(TEST_WAIT_TIMEOUT, future)
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {description}"))
            .unwrap_or_else(|error| panic!("{description} failed: {error:?}"));
    }

    fn join_thread<T>(handle: thread::JoinHandle<T>, description: &str) -> T {
        wait_until(description, || handle.is_finished());
        match handle.join() {
            Ok(result) => result,
            Err(_) => panic!("{description} panicked"),
        }
    }

    async fn open_session() -> std::sync::Arc<super::SessionContext<Memory>> {
        let storage = Memory::default();
        let _receipt = Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized storage should create engine");
        std::sync::Arc::new(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
        )
    }

    async fn open_blocking_read_session() -> (
        std::sync::Arc<super::SessionContext<BlockingBeginReadStorage>>,
        BlockingGate,
    ) {
        let storage = BlockingBeginReadStorage::new();
        let gate = storage.gate();
        let _receipt = Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized storage should create engine");
        (
            std::sync::Arc::new(
                engine
                    .open_workspace_session()
                    .await
                    .expect("workspace session should open"),
            ),
            gate,
        )
    }

    async fn open_blocking_write_session() -> (
        std::sync::Arc<super::SessionContext<BlockingBeginWriteStorage>>,
        BlockingGate,
    ) {
        let storage = BlockingBeginWriteStorage::new();
        let gate = storage.gate();
        let _receipt = Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized storage should create engine");
        (
            std::sync::Arc::new(
                engine
                    .open_workspace_session()
                    .await
                    .expect("workspace session should open"),
            ),
            gate,
        )
    }

    #[tokio::test]
    async fn close_waits_for_session_operation_guard_to_drop() {
        let session = open_session().await;
        let guard = session
            .begin_waitable_session_operation()
            .await
            .expect("session operation should begin");
        let mut close = Box::pin(session.close());
        assert_close_pending(close.as_mut());

        drop(guard);
        assert_close_finishes(close.as_mut(), "close after operation guard drops").await;
    }

    #[tokio::test]
    async fn close_waits_for_commit_guard_to_drop() {
        let session = open_session().await;
        let guard = session.begin_commit();
        let mut close = Box::pin(session.close());
        assert_close_pending(close.as_mut());

        drop(guard);
        assert_close_finishes(close.as_mut(), "close after commit guard drops").await;
    }

    #[tokio::test]
    async fn session_read_execute_holds_operation_guard() {
        let session = open_session().await;
        let result = session
            .execute("SELECT 1", &[])
            .await
            .expect("read should succeed");
        assert_eq!(result.len(), 1);
        assert_eq!(session.operation_in_progress_count_for_test(), 0);
    }

    #[tokio::test]
    async fn active_transaction_read_execute_holds_operation_guard() {
        let session = open_session().await;
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        assert!(session.active_transaction_for_test());
        let result = transaction
            .execute("SELECT 1", &[])
            .await
            .expect("transaction read should succeed");
        assert_eq!(result.len(), 1);
        assert_eq!(session.operation_in_progress_count_for_test(), 1);
        assert!(session.active_transaction_for_test());
        transaction
            .rollback()
            .await
            .expect("transaction rollback should succeed");
        assert_eq!(session.operation_in_progress_count_for_test(), 0);
        assert!(!session.active_transaction_for_test());
    }

    #[tokio::test]
    async fn close_rejects_idle_explicit_transaction_without_waiting() {
        let session = open_session().await;
        let transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");

        let error = session
            .close()
            .await
            .expect_err("close should reject an idle explicit transaction");
        assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

        transaction
            .rollback()
            .await
            .expect("rollback should remain available after rejected close");
    }

    #[tokio::test]
    async fn explicit_transaction_commit_sets_commit_guard() {
        let session = open_session().await;
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('commit-guard-test', 'value')",
                &[],
            )
            .await
            .expect("transaction write should stage");
        transaction
            .commit()
            .await
            .expect("transaction commit should succeed");
        assert!(!session.commit_in_progress_for_test());
    }

    #[tokio::test]
    async fn explicit_transaction_commit_waits_for_collaboration_write_gate() {
        let session = open_session().await;
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('serialized-commit', 'value')",
                &[],
            )
            .await
            .expect("transaction write should stage");

        let collaboration_guard = std::sync::Arc::clone(&session.collaboration_write_gate)
            .lock_owned()
            .await;
        let mut commit = Box::pin(transaction.commit());
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(
            matches!(commit.as_mut().poll(&mut cx), Poll::Pending),
            "explicit commit should wait behind a bounded collaboration write"
        );

        drop(collaboration_guard);
        tokio::time::timeout(TEST_WAIT_TIMEOUT, commit)
            .await
            .expect("commit should resume after collaboration gate release")
            .expect("explicit transaction commit should succeed");
    }

    #[tokio::test]
    async fn close_waits_for_session_read_blocked_in_storage_read() {
        let (session, gate) = open_blocking_read_session().await;

        gate.block_next();
        let reader_session = std::sync::Arc::clone(&session);
        let reader = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("test runtime should build");
            runtime.block_on(async move { reader_session.execute("SELECT 1", &[]).await })
        });
        gate.wait_until_blocked();

        let mut close = Box::pin(session.close());
        assert_close_pending(close.as_mut());

        gate.release();
        let error = join_thread(reader, "blocked reader")
            .expect_err("read should observe close after storage read resumes");
        assert_eq!(error.code, crate::LixError::CODE_CLOSED);
        assert_close_finishes(close.as_mut(), "close after blocked read exits").await;
    }

    #[tokio::test]
    async fn close_rejects_active_transaction_read_blocked_in_storage_read() {
        let (session, gate) = open_blocking_read_session().await;
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");

        gate.block_next();
        let reader = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("test runtime should build");
            runtime.block_on(async move { transaction.execute("SELECT 1", &[]).await })
        });
        gate.wait_until_blocked();

        let close_error = session
            .close()
            .await
            .expect_err("close should reject an active explicit transaction read");
        assert_eq!(close_error.code, "LIX_INVALID_TRANSACTION_STATE");

        gate.release();
        let result = join_thread(reader, "blocked transaction reader")
            .expect("in-flight transaction read should finish after rejected close");
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn close_waits_for_explicit_transaction_blocked_in_storage_commit() {
        let (session, gate) = open_blocking_write_session().await;
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('blocked-commit', 'value')",
                &[],
            )
            .await
            .expect("transaction write should stage");

        gate.block_next();
        let committer = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("test runtime should build");
            runtime.block_on(async move { transaction.commit().await })
        });
        gate.wait_until_blocked();
        assert!(
            session.commit_in_progress_for_test(),
            "blocked explicit transaction commit should set the commit guard"
        );

        let mut close = Box::pin(session.close());
        assert_close_pending(close.as_mut());

        gate.release();
        join_thread(committer, "blocked committer")
            .expect("commit already at storage boundary should finish");
        assert_close_finishes(close.as_mut(), "close after commit exits").await;
        assert!(
            !session.commit_in_progress_for_test(),
            "commit guard should clear after the blocked commit exits"
        );
    }

    #[derive(Clone)]
    struct BlockingBeginReadStorage {
        inner: Memory,
        gate: BlockingGate,
    }

    impl BlockingBeginReadStorage {
        fn new() -> Self {
            Self {
                inner: Memory::default(),
                gate: BlockingGate::new(),
            }
        }

        fn gate(&self) -> BlockingGate {
            self.gate.clone()
        }
    }

    impl Storage for BlockingBeginReadStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;

        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;
        async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            self.gate.maybe_block();
            self.inner.begin_read(opts).await
        }

        async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
            self.inner.begin_write(opts).await
        }
    }

    #[derive(Clone)]
    struct BlockingBeginWriteStorage {
        inner: Memory,
        gate: BlockingGate,
    }

    impl BlockingBeginWriteStorage {
        fn new() -> Self {
            Self {
                inner: Memory::default(),
                gate: BlockingGate::new(),
            }
        }

        fn gate(&self) -> BlockingGate {
            self.gate.clone()
        }
    }

    impl Storage for BlockingBeginWriteStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;

        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;
        async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            self.inner.begin_read(opts).await
        }

        async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
            self.gate.maybe_block();
            self.inner.begin_write(opts).await
        }
    }

    #[derive(Clone)]
    struct BlockingGate {
        state: std::sync::Arc<(Mutex<BlockingGateState>, Condvar)>,
    }

    impl BlockingGate {
        fn new() -> Self {
            Self {
                state: std::sync::Arc::new((
                    Mutex::new(BlockingGateState::default()),
                    Condvar::new(),
                )),
            }
        }

        fn block_next(&self) {
            let (lock, _) = &*self.state;
            let mut state = lock.lock().expect("blocking gate lock should not poison");
            state.block_next = true;
            state.blocked = false;
            state.released = false;
        }

        fn maybe_block(&self) {
            let (lock, condvar) = &*self.state;
            let mut state = lock.lock().expect("blocking gate lock should not poison");
            if !state.block_next {
                return;
            }
            state.block_next = false;
            state.blocked = true;
            condvar.notify_all();
            let deadline = Instant::now() + TEST_WAIT_TIMEOUT;
            while !state.released {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(
                    !remaining.is_zero(),
                    "timed out waiting for blocking gate release"
                );
                let (next_state, wait_result) = condvar
                    .wait_timeout(state, remaining)
                    .expect("blocking gate lock should not poison after wait");
                state = next_state;
                assert!(
                    !wait_result.timed_out() || state.released,
                    "timed out waiting for blocking gate release"
                );
            }
        }

        fn wait_until_blocked(&self) {
            let (lock, condvar) = &*self.state;
            let mut state = lock.lock().expect("blocking gate lock should not poison");
            let deadline = Instant::now() + TEST_WAIT_TIMEOUT;
            while !state.blocked {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(!remaining.is_zero(), "timed out waiting for blocking gate");
                let (next_state, wait_result) = condvar
                    .wait_timeout(state, remaining)
                    .expect("blocking gate lock should not poison after wait");
                state = next_state;
                assert!(
                    !wait_result.timed_out() || state.blocked,
                    "timed out waiting for blocking gate"
                );
            }
        }

        fn release(&self) {
            let (lock, condvar) = &*self.state;
            let mut state = lock.lock().expect("blocking gate lock should not poison");
            state.released = true;
            condvar.notify_all();
        }
    }

    #[derive(Default)]
    struct BlockingGateState {
        block_next: bool,
        blocked: bool,
        released: bool,
    }
}
