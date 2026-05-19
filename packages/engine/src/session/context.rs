use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::binary_cas::{BinaryCasContext, BlobDataReader};
use crate::catalog::CatalogContext;
use crate::commit_graph::{CommitGraphContext, CommitGraphReader};
use crate::entity_identity::EntityIdentity;
use crate::functions::FunctionProviderHandle;
use crate::json_store::JsonStoreContext;
use crate::live_state::{LiveStateContext, LiveStateReader, LiveStateRowRequest};
use crate::sql2::{
    ChangelogQuerySource, HistoryQuerySource, SqlChangelogQuerySource, SqlExecutionContext,
    SqlHistoryQuerySource,
};
use crate::storage::{InMemoryStorageBackend, StorageBackend, StorageReadOptions};
use crate::storage::{StorageContext, StorageRead, StorageReadScope};
use crate::tracked_state::TrackedStateContext;
use crate::transaction::{open_transaction, Transaction};
use crate::version::{
    VersionContext, VersionLifecycle, VersionOperation, VersionRefReader, VersionReferenceRole,
};
use crate::GLOBAL_VERSION_ID;
use crate::{LixError, NullableKeyFilter};

use super::transaction::transaction_state_error;

pub(crate) const WORKSPACE_VERSION_KEY: &str = "lix_workspace_version_id";

#[derive(Clone)]
pub(crate) enum SessionMode {
    Pinned { version_id: String },
    Workspace,
}

/// Session-context state for engine execution.
///
/// A session context pins the active version selector and shared execution
/// services. Parent-handle `execute(...)` runs as an implicit single-statement
/// transaction. Explicit transactions hold the session execution lease until
/// commit or rollback, so all SQL during that window must run through the
/// transaction handle.
#[derive(Clone)]
pub struct SessionContext<B: StorageBackend = InMemoryStorageBackend> {
    pub(super) mode: SessionMode,
    pub(super) storage: StorageContext<B>,
    pub(super) live_state: Arc<LiveStateContext>,
    pub(super) tracked_state: Arc<TrackedStateContext>,
    pub(super) binary_cas: Arc<BinaryCasContext>,
    pub(super) version_ctx: Arc<VersionContext>,
    pub(super) catalog_context: Arc<CatalogContext>,
    pub(super) write_lock: Arc<tokio::sync::Mutex<()>>,
    closed: Arc<AtomicBool>,
    active_transaction: Arc<AtomicBool>,
}

impl<B> SessionContext<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    pub(crate) async fn open_workspace(
        storage: StorageContext<B>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        version_ctx: Arc<VersionContext>,
        catalog_context: Arc<CatalogContext>,
        write_lock: Arc<tokio::sync::Mutex<()>>,
    ) -> Result<Self, LixError> {
        let session = Self::new(
            SessionMode::Workspace,
            storage,
            live_state,
            tracked_state,
            binary_cas,
            version_ctx,
            catalog_context,
            write_lock,
        );
        session.active_version_id().await?;
        Ok(session)
    }

    pub(crate) async fn open(
        active_version_id: String,
        storage: StorageContext<B>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        version_ctx: Arc<VersionContext>,
        catalog_context: Arc<CatalogContext>,
        write_lock: Arc<tokio::sync::Mutex<()>>,
    ) -> Result<Self, LixError> {
        Ok(Self::new(
            SessionMode::Pinned {
                version_id: active_version_id,
            },
            storage,
            live_state,
            tracked_state,
            binary_cas,
            version_ctx,
            catalog_context,
            write_lock,
        ))
    }

    pub(super) fn new(
        mode: SessionMode,
        storage: StorageContext<B>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        version_ctx: Arc<VersionContext>,
        catalog_context: Arc<CatalogContext>,
        write_lock: Arc<tokio::sync::Mutex<()>>,
    ) -> Self {
        Self::new_with_closed(
            mode,
            storage,
            live_state,
            tracked_state,
            binary_cas,
            version_ctx,
            catalog_context,
            write_lock,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        )
    }

    pub(super) fn new_with_closed(
        mode: SessionMode,
        storage: StorageContext<B>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        version_ctx: Arc<VersionContext>,
        catalog_context: Arc<CatalogContext>,
        write_lock: Arc<tokio::sync::Mutex<()>>,
        closed: Arc<AtomicBool>,
        active_transaction: Arc<AtomicBool>,
    ) -> Self {
        Self {
            mode,
            storage,
            live_state,
            tracked_state,
            binary_cas,
            version_ctx,
            catalog_context,
            write_lock,
            closed,
            active_transaction,
        }
    }

    /// Releases this logical session handle. This is a lifecycle boundary only:
    /// successful writes are committed before their operation returns.
    pub async fn close(&self) -> Result<(), LixError> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn closed_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.closed)
    }

    pub(crate) fn active_transaction_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.active_transaction)
    }

    pub(crate) fn ensure_open(&self) -> Result<(), LixError> {
        if self.is_closed() {
            return Err(closed_error());
        }
        Ok(())
    }

    /// Resolves the version this session should operate on right now.
    ///
    /// This is a read-path helper. Write flows must resolve the active version
    /// through the transaction capability so the read is scoped to the
    /// same backend transaction as the writes it influences.
    ///
    /// Pinned sessions are pure in-memory views over one version. Workspace
    /// sessions read the shared workspace selector from untracked global
    /// `lix_key_value` state so multiple open app sessions can observe the same
    /// active workspace version.
    pub async fn active_version_id(&self) -> Result<String, LixError> {
        let transaction = self.storage.begin_read(StorageReadOptions::default())?;
        let result = self.active_version_id_from_reader(&transaction).await;
        match result {
            Ok(version_id) => Ok(version_id),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn active_version_id_from_reader<S>(
        &self,
        reader: &S,
    ) -> Result<String, LixError>
    where
        S: StorageRead + Send + Sync + ?Sized,
    {
        self.ensure_open()?;
        match &self.mode {
            SessionMode::Pinned { version_id } => Ok(version_id.clone()),
            SessionMode::Workspace => self.load_workspace_version_id(reader).await,
        }
    }

    async fn load_workspace_version_id<S>(&self, reader: &S) -> Result<String, LixError>
    where
        S: StorageRead + Send + Sync + ?Sized,
    {
        let row = self
            .live_state
            .reader(reader)
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: EntityIdentity::single(WORKSPACE_VERSION_KEY),
                file_id: NullableKeyFilter::Null,
            })
            .await?
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "workspace version selector is missing lix_key_value:lix_workspace_version_id",
                )
            })?;
        let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "workspace version selector is missing snapshot_content",
            )
        })?;
        let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("workspace version selector snapshot is invalid JSON: {error}"),
            )
        })?;
        let version_id = snapshot
            .get("value")
            .and_then(JsonValue::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "workspace version selector value must be a non-empty string",
                )
            })?
            .to_string();

        let version_ref = self.version_ctx.ref_reader(reader);
        VersionLifecycle::new(&version_ref)
            .require_existing_ref(
                &version_id,
                VersionOperation::LoadWorkspaceSelector,
                VersionReferenceRole::WorkspaceSelector,
            )
            .await?;

        Ok(version_id)
    }

    pub(crate) async fn with_write_transaction<T, F>(&self, f: F) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(
            &'tx mut Transaction<B>,
        ) -> Pin<Box<dyn Future<Output = Result<T, LixError>> + 'tx>>,
    {
        self.ensure_open()?;
        let _transaction_guard = self.reserve_session_transaction()?;
        self.with_write_transaction_reserved(f).await
    }

    pub(crate) async fn with_write_transaction_reserved<T, F>(&self, f: F) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(
            &'tx mut Transaction<B>,
        ) -> Pin<Box<dyn Future<Output = Result<T, LixError>> + 'tx>>,
    {
        let _write_guard = Arc::clone(&self.write_lock).lock_owned().await;
        let opened = open_transaction(
            &self.mode,
            self.storage.clone(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.version_ctx),
            Arc::clone(&self.catalog_context),
        )
        .await?;
        let mut transaction = opened.transaction;
        let runtime_functions = opened.runtime_functions;

        match f(&mut transaction).await {
            Ok(value) => {
                transaction.commit(&runtime_functions).await?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn reserve_session_transaction(&self) -> Result<SessionTransactionGuard, LixError> {
        let active_transaction = self.active_transaction_flag();
        if active_transaction
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(transaction_state_error(
                "Lix handle has an active transaction; use the transaction handle for reads and writes until it is committed or rolled back",
            ));
        }
        Ok(SessionTransactionGuard { active_transaction })
    }
}

fn closed_error() -> LixError {
    LixError::new(LixError::CODE_CLOSED, "Lix handle is closed")
        .with_hint("Open a new Lix handle before calling this method.")
}

pub(super) struct SessionTransactionGuard {
    active_transaction: Arc<AtomicBool>,
}

impl Drop for SessionTransactionGuard {
    fn drop(&mut self) {
        self.active_transaction.store(false, Ordering::SeqCst);
    }
}

/// Read-only SQL execution context derived from a session.
///
/// Write statements re-plan against `Transaction`; this context intentionally
/// has no write stager.
pub(super) struct SessionSqlExecutionContext<'a, R> {
    pub(super) active_version_id: &'a str,
    pub(super) read_store: StorageReadScope<R>,
    pub(super) live_state: Arc<LiveStateContext>,
    pub(super) binary_cas: Arc<BinaryCasContext>,
    pub(super) version_ctx: Arc<VersionContext>,
    pub(super) visible_schemas: Vec<JsonValue>,
    pub(super) functions: FunctionProviderHandle,
}

impl<R> SqlExecutionContext for SessionSqlExecutionContext<'_, R>
where
    R: crate::storage::StorageBackendRead + Clone + Send + Sync + 'static,
{
    type ReadStore = StorageReadScope<R>;

    fn active_version_id(&self) -> &str {
        self.active_version_id
    }

    fn live_state(&self) -> Arc<dyn LiveStateReader> {
        Arc::new(self.live_state.reader(self.read_store.clone())) as Arc<dyn LiveStateReader>
    }

    fn history_query_source(&self) -> SqlHistoryQuerySource<Self::ReadStore> {
        HistoryQuerySource {
            json_reader: JsonStoreContext::new().reader(self.read_store.store()),
        }
    }

    fn changelog_query_source(&self) -> SqlChangelogQuerySource<Self::ReadStore> {
        ChangelogQuerySource {
            store: self.read_store.clone(),
            json_reader: JsonStoreContext::new().reader(self.read_store.store()),
        }
    }

    fn commit_graph(&self) -> Box<dyn CommitGraphReader> {
        Box::new(CommitGraphContext::new().reader(self.read_store.clone()))
    }

    fn version_ref(&self) -> Arc<dyn VersionRefReader> {
        Arc::new(self.version_ctx.ref_reader(self.read_store.clone()))
    }

    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
        Arc::new(self.binary_cas.reader(self.read_store.clone())) as Arc<dyn BlobDataReader>
    }

    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        Ok(self.visible_schemas.clone())
    }
}
