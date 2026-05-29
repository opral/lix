#![allow(clippy::match_wild_err_arm, clippy::option_if_let_else)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::{BinaryCasContext, BlobDataReader};
use crate::branch::{
    BranchContext, BranchLifecycle, BranchOperation, BranchRefReader, BranchReferenceRole,
};
use crate::catalog::CatalogContext;
use crate::commit_graph::{CommitGraphContext, CommitGraphReader};
use crate::entity_pk::EntityPk;
use crate::functions::FunctionProviderHandle;
use crate::json_store::JsonStoreContext;
use crate::live_state::{LiveStateContext, LiveStateReader, LiveStateRowRequest};
use crate::sql2::{
    ChangelogQuerySource, HistoryQuerySource, SqlChangelogQuerySource, SqlExecutionContext,
    SqlHistoryQuerySource,
};
use crate::storage::{InMemoryStorageBackend, StorageBackend, StorageReadOptions};
use crate::storage::{SharedStorageRead, StorageContext, StorageRead};
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
pub struct SessionContext<B: StorageBackend = InMemoryStorageBackend> {
    pub(super) mode: SessionMode,
    pub(super) storage: StorageContext<B>,
    pub(super) live_state: Arc<LiveStateContext>,
    pub(super) tracked_state: Arc<TrackedStateContext>,
    pub(super) binary_cas: Arc<BinaryCasContext>,
    pub(super) branch_ctx: Arc<BranchContext>,
    pub(super) catalog_context: Arc<CatalogContext>,
    pub(super) deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
    transaction_manager: SessionTransactionManager,
}

impl<B> SessionContext<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    pub(crate) async fn open_workspace(
        storage: StorageContext<B>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        branch_ctx: Arc<BranchContext>,
        catalog_context: Arc<CatalogContext>,
        deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
    ) -> Result<Self, LixError> {
        let session = Self::new(
            SessionMode::Workspace,
            storage,
            live_state,
            tracked_state,
            binary_cas,
            branch_ctx,
            catalog_context,
            deterministic_runtime_gate,
        );
        session.active_branch_id().await?;
        Ok(session)
    }

    pub(crate) async fn open(
        active_branch_id: String,
        storage: StorageContext<B>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        branch_ctx: Arc<BranchContext>,
        catalog_context: Arc<CatalogContext>,
        deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
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
            deterministic_runtime_gate,
        ))
    }

    pub(super) fn new(
        mode: SessionMode,
        storage: StorageContext<B>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        branch_ctx: Arc<BranchContext>,
        catalog_context: Arc<CatalogContext>,
        deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
    ) -> Self {
        Self::new_with_transaction_manager(
            mode,
            storage,
            live_state,
            tracked_state,
            binary_cas,
            branch_ctx,
            catalog_context,
            deterministic_runtime_gate,
            SessionTransactionManager::new(),
        )
    }

    pub(super) fn new_with_transaction_manager(
        mode: SessionMode,
        storage: StorageContext<B>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        branch_ctx: Arc<BranchContext>,
        catalog_context: Arc<CatalogContext>,
        deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
        transaction_manager: SessionTransactionManager,
    ) -> Self {
        Self {
            mode,
            storage,
            live_state,
            tracked_state,
            binary_cas,
            branch_ctx,
            catalog_context,
            deterministic_runtime_gate,
            transaction_manager,
        }
    }

    /// Releases this logical session handle. This is a lifecycle boundary only:
    /// successful writes are committed before their operation returns.
    pub async fn close(&self) -> Result<(), LixError> {
        self.transaction_manager.close().await
    }

    pub fn is_closed(&self) -> bool {
        self.transaction_manager.is_closed()
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
        let read = SharedStorageRead::new(self.storage.begin_read(StorageReadOptions::default())?);
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

    pub(super) fn begin_session_operation(&self) -> Result<SessionOperationGuard, LixError> {
        self.transaction_manager.begin_session_operation()
    }

    pub(super) fn begin_session_write_lease(&self) -> Result<SessionWriteLease, LixError> {
        self.transaction_manager.begin_write_lease()
    }

    pub(super) fn begin_explicit_session_write_lease(&self) -> Result<SessionWriteLease, LixError> {
        self.transaction_manager.begin_explicit_write_lease()
    }

    pub(super) async fn begin_session_write_access(&self) -> Result<SessionWriteAccess, LixError> {
        let write_lease = self.begin_session_write_lease()?;
        self.begin_session_write_access_with_lease(write_lease)
            .await
    }

    pub(super) async fn begin_explicit_session_write_access(
        &self,
    ) -> Result<SessionWriteAccess, LixError> {
        let write_lease = self.begin_explicit_session_write_lease()?;
        self.begin_session_write_access_with_lease(write_lease)
            .await
    }

    async fn begin_session_write_access_with_lease(
        &self,
        write_lease: SessionWriteLease,
    ) -> Result<SessionWriteAccess, LixError> {
        let write_access = SessionWriteAccess {
            _write_lease: write_lease,
        };
        self.ensure_open()?;
        Ok(write_access)
    }

    /// Resolves the branch this session should operate on right now.
    ///
    /// This is a read-path helper. Write flows must resolve the active branch
    /// through the transaction capability so the read is scoped to the
    /// same backend transaction as the writes it influences.
    ///
    /// Pinned sessions are pure in-memory views over one branch. Workspace
    /// sessions read the shared workspace selector from untracked global
    /// `lix_key_value` state so multiple open app sessions can observe the same
    /// active workspace branch.
    pub async fn active_branch_id(&self) -> Result<String, LixError> {
        let _operation_guard = self.begin_session_operation()?;
        let read = SharedStorageRead::new(self.storage.begin_read(StorageReadOptions::default())?);
        let result = self.active_branch_id_from_reader(&read).await;
        match result {
            Ok(branch_id) => Ok(branch_id),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn active_branch_id_from_reader<S>(
        &self,
        reader: &S,
    ) -> Result<String, LixError>
    where
        S: StorageRead + Send + Sync + ?Sized,
    {
        self.ensure_open()?;
        match &self.mode {
            SessionMode::Pinned { branch_id } => Ok(branch_id.clone()),
            SessionMode::Workspace => self.load_workspace_branch_id(reader).await,
        }
    }

    async fn load_workspace_branch_id<S>(&self, reader: &S) -> Result<String, LixError>
    where
        S: StorageRead + Send + Sync + ?Sized,
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
            &'tx mut Transaction<B>,
        ) -> Pin<Box<dyn Future<Output = Result<T, LixError>> + 'tx>>,
    {
        self.ensure_open()?;
        let write_access = self.begin_session_write_access().await?;
        self.with_write_transaction_reserved(write_access, f).await
    }

    pub(super) async fn with_write_transaction_reserved<T, F>(
        &self,
        _write_access: SessionWriteAccess,
        f: F,
    ) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(
            &'tx mut Transaction<B>,
        ) -> Pin<Box<dyn Future<Output = Result<T, LixError>> + 'tx>>,
    {
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
            Arc::clone(&self.branch_ctx),
            Arc::clone(&self.catalog_context),
        )
        .await?;
        self.ensure_open()?;
        let mut transaction = opened.transaction;
        transaction.attach_commit_boundary(self.transaction_commit_boundary());
        let runtime_functions = opened.runtime_functions;

        match f(&mut transaction).await {
            Ok(value) => {
                self.ensure_open()?;
                transaction.commit(&runtime_functions).await?;
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
}

pub(super) fn closed_error() -> LixError {
    LixError::new(LixError::CODE_CLOSED, "Lix handle is closed")
        .with_hint("Open a new Lix handle before calling this method.")
}

/// Read-only SQL execution context derived from a session.
///
/// Write statements re-plan against `Transaction`; this context intentionally
/// has no write stager.
pub(super) struct SessionSqlExecutionContext<'a, R: crate::storage::StorageBackendRead> {
    pub(super) active_branch_id: &'a str,
    pub(super) read_store: SharedStorageRead<R>,
    pub(super) live_state: Arc<LiveStateContext>,
    pub(super) binary_cas: Arc<BinaryCasContext>,
    pub(super) branch_ctx: Arc<BranchContext>,
    pub(super) visible_schemas: Vec<JsonValue>,
    pub(super) functions: FunctionProviderHandle,
}

impl<R> SqlExecutionContext for SessionSqlExecutionContext<'_, R>
where
    R: crate::storage::StorageBackendRead + Send + 'static,
{
    type ReadStore = SharedStorageRead<R>;

    fn active_branch_id(&self) -> &str {
        self.active_branch_id
    }

    #[expect(trivial_casts)]
    fn live_state(&self) -> Arc<dyn LiveStateReader> {
        Arc::new(self.live_state.reader(self.read_store.clone())) as Arc<dyn LiveStateReader>
    }

    fn history_query_source(&self) -> SqlHistoryQuerySource<Self::ReadStore> {
        HistoryQuerySource {
            json_reader: JsonStoreContext::new().reader(self.read_store.clone()),
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

    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        Ok(self.visible_schemas.clone())
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
    use crate::backend::{
        Backend, BackendError, InMemoryBackend, InMemoryRead, InMemoryWrite, ReadOptions,
        WriteOptions,
    };
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

    async fn open_session() -> std::sync::Arc<super::SessionContext<InMemoryBackend>> {
        let backend = InMemoryBackend::default();
        let _receipt = Engine::initialize(backend.clone())
            .await
            .expect("backend should initialize");
        let engine = Engine::new(backend)
            .await
            .expect("initialized backend should create engine");
        std::sync::Arc::new(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
        )
    }

    async fn open_blocking_read_session() -> (
        std::sync::Arc<super::SessionContext<BlockingBeginReadBackend>>,
        BlockingGate,
    ) {
        let backend = BlockingBeginReadBackend::new();
        let gate = backend.gate();
        let _receipt = Engine::initialize(backend.clone())
            .await
            .expect("backend should initialize");
        let engine = Engine::new(backend)
            .await
            .expect("initialized backend should create engine");
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
        std::sync::Arc<super::SessionContext<BlockingBeginWriteBackend>>,
        BlockingGate,
    ) {
        let backend = BlockingBeginWriteBackend::new();
        let gate = backend.gate();
        let _receipt = Engine::initialize(backend.clone())
            .await
            .expect("backend should initialize");
        let engine = Engine::new(backend)
            .await
            .expect("initialized backend should create engine");
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
            .begin_session_operation()
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
    async fn close_waits_for_session_read_blocked_in_backend_read() {
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
            .expect_err("read should observe close after backend read resumes");
        assert_eq!(error.code, crate::LixError::CODE_CLOSED);
        assert_close_finishes(close.as_mut(), "close after blocked read exits").await;
    }

    #[tokio::test]
    async fn close_rejects_active_transaction_read_blocked_in_backend_read() {
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
    async fn close_waits_for_explicit_transaction_blocked_in_backend_commit() {
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
            .expect("commit already at durable boundary should finish");
        assert_close_finishes(close.as_mut(), "close after commit exits").await;
        assert!(
            !session.commit_in_progress_for_test(),
            "commit guard should clear after the blocked commit exits"
        );
    }

    #[derive(Clone)]
    struct BlockingBeginReadBackend {
        inner: InMemoryBackend,
        gate: BlockingGate,
    }

    impl BlockingBeginReadBackend {
        fn new() -> Self {
            Self {
                inner: InMemoryBackend::default(),
                gate: BlockingGate::new(),
            }
        }

        fn gate(&self) -> BlockingGate {
            self.gate.clone()
        }
    }

    impl Backend for BlockingBeginReadBackend {
        type Read<'a>
            = InMemoryRead
        where
            Self: 'a;

        type Write<'a>
            = InMemoryWrite
        where
            Self: 'a;
        fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
            self.gate.maybe_block();
            self.inner.begin_read(opts)
        }

        fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
            self.inner.begin_write(opts)
        }
    }

    #[derive(Clone)]
    struct BlockingBeginWriteBackend {
        inner: InMemoryBackend,
        gate: BlockingGate,
    }

    impl BlockingBeginWriteBackend {
        fn new() -> Self {
            Self {
                inner: InMemoryBackend::default(),
                gate: BlockingGate::new(),
            }
        }

        fn gate(&self) -> BlockingGate {
            self.gate.clone()
        }
    }

    impl Backend for BlockingBeginWriteBackend {
        type Read<'a>
            = InMemoryRead
        where
            Self: 'a;

        type Write<'a>
            = InMemoryWrite
        where
            Self: 'a;
        fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
            self.inner.begin_read(opts)
        }

        fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
            self.gate.maybe_block();
            self.inner.begin_write(opts)
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
