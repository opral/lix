#![allow(clippy::match_wild_err_arm, clippy::option_if_let_else)]

use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tracing::Instrument as _;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::{
    BranchLifecycle, BranchOperation, BranchRefReader, BranchRefStoreReader, BranchReferenceRole,
};
use crate::catalog::{CatalogContext, CatalogFingerprint, CatalogSnapshot};
use crate::commit_graph::{CommitGraphReader, CommitGraphStoreReader};
use crate::domain::Domain;
use crate::entity_pk::EntityPk;
use crate::filesystem::FilesystemPathIndexReader;
use crate::forktree::{StateCell, StateKeyRef, encode_state_key};
use crate::functions::FunctionProviderHandle;
use crate::observe_coordinator::ObserveCoordinator;
use crate::observe_invalidation::ObserveInvalidation;
use crate::plugin::PluginRuntimeHost;
use crate::sql2::{
    ChangelogQuerySource, SessionFileViews, SqlChangelogQuerySource, SqlExecutionContext,
    SqlPlanningCache,
};
use crate::state::{ForkTreeStateView, TransactionStateView};
use crate::storage_adapter::Storage;
use crate::storage_adapter::{Memory, StorageReadOptions};
use crate::storage_adapter::{SharedStorageAdapterRead, StorageAdapter, StorageAdapterRead};
use crate::telemetry::TelemetrySink;
use crate::transaction::{Transaction, open_transaction};

use super::transaction::{SessionOperationGuard, SessionTransactionManager, SessionWriteLease};
use crate::transaction::CommitCoordinator;

pub(crate) const WORKSPACE_BRANCH_KEY: &str = "lix_workspace_branch_id";

/// Loads the workspace selector from its canonical authenticated state member
/// when opening a workspace session.
pub(crate) async fn load_workspace_branch_id_from_index(
    reader: &(impl StorageAdapterRead + ?Sized),
) -> Result<String, LixError> {
    let forktree = crate::forktree::ForkTreeReadFacade::new(reader);
    let entity_pk = EntityPk::single(WORKSPACE_BRANCH_KEY);
    let key = encode_state_key(StateKeyRef {
        schema_key: "lix_key_value",
        file_id: None,
        entity_pk: &entity_pk,
    });
    let view = ForkTreeStateView::from_facade(forktree, GLOBAL_BRANCH_ID).await?;
    let row = view
        .points(&[key], true)
        .await?
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "workspace branch selector is missing lix_key_value:lix_workspace_branch_id",
            )
        })?;
    let snapshot_content = match row.value.cell {
        StateCell::Value(value) => value,
        StateCell::Null | StateCell::Tombstone => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "workspace branch selector is missing snapshot_content",
            ));
        }
    };
    let snapshot =
        serde_json::from_str::<JsonValue>(snapshot_content.as_ref()).map_err(|error| {
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

    let branch_ref = BranchRefStoreReader::new(reader);
    BranchLifecycle::new(&branch_ref)
        .require_existing_ref(
            &branch_id,
            BranchOperation::LoadDefaultBranch,
            BranchReferenceRole::DefaultBranch,
        )
        .await?;

    // The selector is a constrained engine-owned reference: initialization,
    // branch switching, and branch deletion enforce its target lifecycle at
    // mutation time. Revalidating the same branch ref on every statement made
    // workspace sessions pay a second point read for an invariant that no
    // successful commit can violate.
    Ok(branch_id)
}

#[derive(Clone)]
pub(crate) enum SessionMode {
    Pinned { branch_id: Arc<RwLock<String>> },
    Workspace { branch_id: Arc<RwLock<String>> },
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
pub struct SessionContext<StorageImpl: Storage + 'static = Memory> {
    pub(super) mode: SessionMode,
    pub(super) active_account_id: Arc<str>,
    pub(super) storage: StorageAdapter<StorageImpl>,
    pub(super) catalog_context: Arc<CatalogContext>,
    pub(super) sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
    pub(super) deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
    pub(super) collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
    pub(super) commit_coordinator: Arc<CommitCoordinator<StorageImpl>>,
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
        active_account_id: String,
        storage: StorageAdapter<StorageImpl>,
        catalog_context: Arc<CatalogContext>,
        sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
        deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
        collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
        commit_coordinator: Arc<CommitCoordinator<StorageImpl>>,
        observe_coordinator: Arc<ObserveCoordinator>,
        observe_invalidation: Arc<ObserveInvalidation>,
        plugin_host: PluginRuntimeHost,
        telemetry: Option<Arc<dyn TelemetrySink>>,
    ) -> Result<Self, LixError> {
        let read =
            SharedStorageAdapterRead::new(storage.begin_read(StorageReadOptions::default()).await?);
        let branch_id = load_workspace_branch_id_from_index(&read).await?;
        drop(read);
        Ok(Self::new(
            SessionMode::Workspace {
                branch_id: Arc::new(RwLock::new(branch_id)),
            },
            active_account_id,
            storage,
            catalog_context,
            sql_planning_cache,
            deterministic_runtime_gate,
            collaboration_write_gate,
            commit_coordinator,
            observe_coordinator,
            observe_invalidation,
            plugin_host,
            telemetry,
        ))
    }

    pub(crate) async fn open(
        active_branch_id: String,
        active_account_id: String,
        storage: StorageAdapter<StorageImpl>,
        catalog_context: Arc<CatalogContext>,
        sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
        deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
        collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
        commit_coordinator: Arc<CommitCoordinator<StorageImpl>>,
        observe_coordinator: Arc<ObserveCoordinator>,
        observe_invalidation: Arc<ObserveInvalidation>,
        plugin_host: PluginRuntimeHost,
        telemetry: Option<Arc<dyn TelemetrySink>>,
    ) -> Result<Self, LixError> {
        Ok(Self::new(
            SessionMode::Pinned {
                branch_id: Arc::new(RwLock::new(active_branch_id)),
            },
            active_account_id,
            storage,
            catalog_context,
            sql_planning_cache,
            deterministic_runtime_gate,
            collaboration_write_gate,
            commit_coordinator,
            observe_coordinator,
            observe_invalidation,
            plugin_host,
            telemetry,
        ))
    }

    pub(super) fn new(
        mode: SessionMode,
        active_account_id: String,
        storage: StorageAdapter<StorageImpl>,
        catalog_context: Arc<CatalogContext>,
        sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
        deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
        collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
        commit_coordinator: Arc<CommitCoordinator<StorageImpl>>,
        observe_coordinator: Arc<ObserveCoordinator>,
        observe_invalidation: Arc<ObserveInvalidation>,
        plugin_host: PluginRuntimeHost,
        telemetry: Option<Arc<dyn TelemetrySink>>,
    ) -> Self {
        Self::new_with_transaction_manager(
            mode,
            active_account_id,
            storage,
            catalog_context,
            sql_planning_cache,
            deterministic_runtime_gate,
            collaboration_write_gate,
            commit_coordinator,
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
        active_account_id: String,
        storage: StorageAdapter<StorageImpl>,
        catalog_context: Arc<CatalogContext>,
        sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
        deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
        collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
        commit_coordinator: Arc<CommitCoordinator<StorageImpl>>,
        observe_coordinator: Arc<ObserveCoordinator>,
        observe_invalidation: Arc<ObserveInvalidation>,
        plugin_host: PluginRuntimeHost,
        telemetry: Option<Arc<dyn TelemetrySink>>,
        transaction_manager: SessionTransactionManager,
        file_views: SessionFileViews,
    ) -> Self {
        Self {
            mode,
            active_account_id: Arc::from(active_account_id),
            storage,
            catalog_context,
            sql_planning_cache,
            deterministic_runtime_gate,
            collaboration_write_gate,
            commit_coordinator,
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

    /// Returns the immutable account that authors every change from this session.
    pub fn active_account_id(&self) -> &str {
        &self.active_account_id
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
        // awaits, so the common non-deterministic path only serializes their
        // commit. Deterministic transactions add the collaboration guard before
        // taking the runtime guard to preserve the global lock order.
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
        _reader: &S,
    ) -> Result<String, LixError>
    where
        S: StorageAdapterRead + ?Sized,
    {
        self.ensure_open()?;
        match &self.mode {
            SessionMode::Pinned { branch_id } | SessionMode::Workspace { branch_id } => branch_id
                .read()
                .map(|branch_id| branch_id.clone())
                .map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "session branch selector is poisoned",
                    )
                }),
        }
    }

    /// Runs a transaction with a lending async closure.
    ///
    /// `AsyncFnOnce` ties the returned future to both the transaction borrow
    /// and the closure's captured borrows. Large callers can therefore borrow
    /// prepared input for the duration of the transaction instead of
    /// deep-cloning it into a `'static` closure environment.
    pub(crate) async fn with_write_transaction_lending<T, F>(&self, f: F) -> Result<T, LixError>
    where
        F: for<'tx> AsyncFnOnce(&'tx mut Transaction<StorageImpl>) -> Result<T, LixError>,
    {
        self.ensure_open()?;
        let write_access = self.begin_session_write_access().await?;
        self.with_write_transaction_reserved_lending(write_access, f, |_| Ok(()))
            .await
    }

    pub(super) async fn with_write_transaction_reserved_lending<T, F, A>(
        &self,
        write_access: SessionWriteAccess,
        f: F,
        after_commit: A,
    ) -> Result<T, LixError>
    where
        F: for<'tx> AsyncFnOnce(&'tx mut Transaction<StorageImpl>) -> Result<T, LixError>,
        A: FnOnce(&T) -> Result<(), LixError>,
    {
        let planner_validation_is_serialized = write_access.serializes_collaboration_writes();
        // Automatic writes already hold the collaboration gate, so taking the
        // runtime gate unconditionally cannot reduce their concurrency. It
        // avoids opening a separate read solely to decide whether
        // `Transaction::open` should be allowed to prepare deterministic
        // functions; that coherent opening snapshot remains the source of
        // truth for the mode.
        let _deterministic_runtime_guard = self.lock_deterministic_runtime().await;
        let opened = Box::pin(open_transaction(
            &self.mode,
            self.active_account_id.to_string(),
            self.storage.clone(),
            self.plugin_host.clone(),
            Arc::clone(&self.catalog_context),
            Arc::clone(&self.sql_planning_cache),
            self.file_views.clone(),
        ))
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
                let outcome = Box::pin(transaction.commit(&runtime_functions)).await?;
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_crud_physical_writes(outcome.storage_stats);
                let after_commit_result = after_commit(&value);
                drop(write_access);
                self.observe_invalidation
                    .bump_if_storage_changed(&outcome.storage_stats);
                after_commit_result?;
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

pub(super) struct SessionWriteAccess {
    _write_lease: SessionWriteLease,
    collaboration_write_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl SessionWriteAccess {
    pub(super) fn serializes_collaboration_writes(&self) -> bool {
        self.collaboration_write_guard.is_some()
    }

    pub(super) async fn serialize_collaboration_writes(
        &mut self,
        collaboration_write_gate: &Arc<tokio::sync::Mutex<()>>,
    ) {
        if self.collaboration_write_guard.is_none() {
            self.collaboration_write_guard = Some(
                Arc::clone(collaboration_write_gate)
                    .lock_owned()
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.collaboration_gate_wait"
                    ))
                    .await,
            );
        }
    }

    pub(super) fn release_collaboration_write_serialization(&mut self) {
        self.collaboration_write_guard.take();
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
    pub(super) active_account_id: &'a str,
    pub(super) read_store: SharedStorageAdapterRead<R>,
    pub(super) forktree: crate::forktree::ForkTreeReadFacade<SharedStorageAdapterRead<R>>,
    pub(crate) state_view: crate::state::ForkTreeStateView<SharedStorageAdapterRead<R>>,
    pub(super) catalog_context: Arc<CatalogContext>,
    pub(super) sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
    pub(super) functions: FunctionProviderHandle,
    pub(super) plugin_host: PluginRuntimeHost,
    pub(super) file_views: Option<SessionFileViews>,
}

impl<R> SessionSqlExecutionContext<'_, R>
where
    R: crate::storage_adapter::StorageRead + 'static,
{
    pub(crate) fn state_view(
        &self,
    ) -> &crate::state::ForkTreeStateView<SharedStorageAdapterRead<R>> {
        &self.state_view
    }

    async fn compiled_sql_catalog(&self) -> Result<Arc<CatalogSnapshot>, LixError> {
        let transaction_state = TransactionStateView::new(self.state_view.clone(), Vec::new())?;
        self.catalog_context
            .compiled_catalog_for_transaction_state(
                &transaction_state,
                &Domain::schema_catalog(self.active_branch_id.to_string(), true),
            )
            .await
    }
}

#[async_trait]
impl<R> SqlExecutionContext for SessionSqlExecutionContext<'_, R>
where
    R: crate::storage_adapter::StorageRead + 'static,
{
    type ReadStore = SharedStorageAdapterRead<R>;

    fn state_view(&self) -> &crate::state::ForkTreeStateView<Self::ReadStore> {
        &self.state_view
    }

    fn active_branch_id(&self) -> &str {
        self.active_branch_id
    }

    fn datafusion_session(&self) -> datafusion::prelude::SessionContext {
        self.sql_planning_cache.datafusion_session()
    }

    fn datafusion_read_session(&self) -> datafusion::prelude::SessionContext {
        self.sql_planning_cache.datafusion_read_session()
    }

    async fn sql_planning_environment(
        &self,
    ) -> Result<
        Option<(
            Arc<SqlPlanningCache<CatalogFingerprint>>,
            CatalogFingerprint,
        )>,
        LixError,
    > {
        let catalog = self.compiled_sql_catalog().await?;
        Ok(Some((
            Arc::clone(&self.sql_planning_cache),
            catalog.fingerprint().clone(),
        )))
    }

    fn active_account_id(&self) -> &str {
        self.active_account_id
    }

    fn filesystem_path_index(&self) -> Arc<dyn FilesystemPathIndexReader> {
        Arc::new(crate::filesystem::ForkTreeFilesystemPathIndexReader::new(
            self.forktree.clone(),
        ))
    }

    fn changelog_query_source(&self) -> SqlChangelogQuerySource<Self::ReadStore> {
        ChangelogQuerySource {
            forktree_reader: self.forktree.clone(),
        }
    }

    fn commit_graph(&self) -> Box<dyn CommitGraphReader> {
        Box::new(CommitGraphStoreReader::new(self.read_store.clone()))
    }

    fn branch_ref(&self) -> Arc<dyn BranchRefReader> {
        Arc::new(BranchRefStoreReader::new(self.read_store.clone()))
    }

    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    fn authenticated_blob_reader(
        &self,
    ) -> Result<Arc<dyn crate::forktree::AuthenticatedBlobReader>, LixError> {
        Ok(Arc::new(crate::forktree::blob_reader_on_read(
            self.read_store.clone(),
            self.active_branch_id,
        )?))
    }

    async fn load_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        Ok(self.compiled_sql_catalog().await?.schema_jsons())
    }

    async fn public_catalog(&self) -> Result<Arc<crate::sql2::PublicCatalog>, LixError> {
        let catalog = self
            .compiled_sql_catalog()
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.public_read.compiled_catalog"
            ))
            .await?;
        self.sql_planning_cache
            .public_catalog(catalog.fingerprint(), || Ok(catalog.schema_jsons()))
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

    use crate::engine::Engine;
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
    async fn automatic_writes_take_the_deterministic_runtime_gate_without_a_mode_precheck() {
        let session = open_session().await;
        let deterministic_guard = std::sync::Arc::clone(&session.deterministic_runtime_gate)
            .lock_owned()
            .await;
        let mut write = Box::pin(session.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('automatic-runtime-gate', 'value')",
            &[],
        ));
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(
            matches!(write.as_mut().poll(&mut cx), Poll::Pending),
            "automatic write should wait for the deterministic runtime gate"
        );

        drop(deterministic_guard);
        tokio::time::timeout(TEST_WAIT_TIMEOUT, write)
            .await
            .expect("automatic write should resume after runtime gate release")
            .expect("automatic write should succeed after runtime gate release");
    }

    #[tokio::test]
    async fn automatic_write_waits_for_an_active_automatic_write() {
        let session = open_session().await;
        let first_write = session
            .begin_session_write_lease()
            .await
            .expect("first automatic write lease should begin");
        let mut second_write = Box::pin(session.begin_session_write_lease());
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(
            matches!(second_write.as_mut().poll(&mut cx), Poll::Pending),
            "second automatic write should wait for the active automatic write"
        );

        drop(first_write);
        let second_write = tokio::time::timeout(TEST_WAIT_TIMEOUT, second_write)
            .await
            .expect("second automatic write should resume after the first finishes")
            .expect("second automatic write lease should begin");
        drop(second_write);
    }

    #[tokio::test]
    async fn close_waits_for_session_read_blocked_in_storage_snapshot() {
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
    async fn explicit_transaction_reads_reuse_the_opening_storage_snapshot() {
        let (session, _gate) = open_blocking_read_session().await;
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");

        let result = transaction
            .execute("SELECT 1", &[])
            .await
            .expect("transaction read should use the retained opening snapshot");
        assert_eq!(result.len(), 1);

        let close_error = session
            .close()
            .await
            .expect_err("close should reject an active explicit transaction");
        assert_eq!(close_error.code, "LIX_INVALID_TRANSACTION_STATE");
        transaction
            .rollback()
            .await
            .expect("transaction should roll back");
        session.close().await.expect("session should close");
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
