use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::binary_cas::{BinaryCasContext, BlobDataReader};
use crate::catalog::CatalogContext;
use crate::commit_graph::{CommitGraphContext, CommitGraphReader};
use crate::commit_store::CommitStoreContext;
use crate::entity_identity::EntityIdentity;
use crate::functions::FunctionProviderHandle;
use crate::json_store::JsonStoreContext;
use crate::live_state::{LiveStateContext, LiveStateReader, LiveStateRowRequest};
use crate::plugin::PluginContext;
use crate::sql2::{CommitStoreQuerySource, SqlCommitStoreQuerySource, SqlExecutionContext};
use crate::storage::{
    ScopedStorageReader, StorageContext, StorageReadScope, StorageReadTransaction, StorageReader,
};
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
pub struct SessionContext {
    pub(super) mode: SessionMode,
    pub(super) storage: StorageContext,
    pub(super) live_state: Arc<LiveStateContext>,
    pub(super) tracked_state: Arc<TrackedStateContext>,
    pub(super) binary_cas: Arc<BinaryCasContext>,
    pub(super) commit_store: Arc<CommitStoreContext>,
    pub(super) version_ctx: Arc<VersionContext>,
    pub(super) catalog_context: Arc<CatalogContext>,
    pub(super) plugin_context: Arc<PluginContext>,
    closed: Arc<AtomicBool>,
    active_transaction: Arc<AtomicBool>,
}

impl SessionContext {
    pub(crate) async fn open_workspace(
        storage: StorageContext,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        commit_store: Arc<CommitStoreContext>,
        version_ctx: Arc<VersionContext>,
        catalog_context: Arc<CatalogContext>,
        plugin_context: Arc<PluginContext>,
    ) -> Result<Self, LixError> {
        let session = Self::new(
            SessionMode::Workspace,
            storage,
            live_state,
            tracked_state,
            binary_cas,
            commit_store,
            version_ctx,
            catalog_context,
            plugin_context,
        );
        session.active_version_id().await?;
        Ok(session)
    }

    pub(crate) async fn open(
        active_version_id: String,
        storage: StorageContext,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        commit_store: Arc<CommitStoreContext>,
        version_ctx: Arc<VersionContext>,
        catalog_context: Arc<CatalogContext>,
        plugin_context: Arc<PluginContext>,
    ) -> Result<Self, LixError> {
        Ok(Self::new(
            SessionMode::Pinned {
                version_id: active_version_id,
            },
            storage,
            live_state,
            tracked_state,
            binary_cas,
            commit_store,
            version_ctx,
            catalog_context,
            plugin_context,
        ))
    }

    pub(super) fn new(
        mode: SessionMode,
        storage: StorageContext,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        commit_store: Arc<CommitStoreContext>,
        version_ctx: Arc<VersionContext>,
        catalog_context: Arc<CatalogContext>,
        plugin_context: Arc<PluginContext>,
    ) -> Self {
        Self::new_with_closed(
            mode,
            storage,
            live_state,
            tracked_state,
            binary_cas,
            commit_store,
            version_ctx,
            catalog_context,
            plugin_context,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        )
    }

    pub(super) fn new_with_closed(
        mode: SessionMode,
        storage: StorageContext,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        commit_store: Arc<CommitStoreContext>,
        version_ctx: Arc<VersionContext>,
        catalog_context: Arc<CatalogContext>,
        plugin_context: Arc<PluginContext>,
        closed: Arc<AtomicBool>,
        active_transaction: Arc<AtomicBool>,
    ) -> Self {
        Self {
            mode,
            storage,
            live_state,
            tracked_state,
            binary_cas,
            commit_store,
            version_ctx,
            catalog_context,
            plugin_context,
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
        let mut transaction = self.storage.begin_read_transaction().await?;
        let result = self
            .active_version_id_from_reader(transaction.as_mut())
            .await;
        match result {
            Ok(version_id) => {
                transaction.rollback().await?;
                Ok(version_id)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub(super) async fn active_version_id_from_reader<S>(
        &self,
        reader: &mut S,
    ) -> Result<String, LixError>
    where
        S: StorageReader + ?Sized,
    {
        self.ensure_open()?;
        match &self.mode {
            SessionMode::Pinned { version_id } => Ok(version_id.clone()),
            SessionMode::Workspace => self.load_workspace_version_id(reader).await,
        }
    }

    async fn load_workspace_version_id<S>(&self, reader: &mut S) -> Result<String, LixError>
    where
        S: StorageReader + ?Sized,
    {
        let row = self
            .live_state
            .reader(&mut *reader)
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

        let version_ref = self.version_ctx.ref_reader(&mut *reader);
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
            &'tx mut Transaction,
        ) -> Pin<Box<dyn Future<Output = Result<T, LixError>> + 'tx>>,
    {
        self.ensure_open()?;
        let _transaction_guard = self.reserve_session_transaction()?;
        self.with_write_transaction_reserved(f).await
    }

    pub(crate) async fn with_write_transaction_reserved<T, F>(&self, f: F) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(
            &'tx mut Transaction,
        ) -> Pin<Box<dyn Future<Output = Result<T, LixError>> + 'tx>>,
    {
        let opened = open_transaction(
            &self.mode,
            self.storage.clone(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.commit_store),
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
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
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
pub(super) struct SessionSqlExecutionContext<'a> {
    pub(super) active_version_id: &'a str,
    pub(super) read_store:
        ScopedStorageReader<Box<dyn StorageReadTransaction + Send + Sync + 'static>>,
    pub(super) live_state: Arc<LiveStateContext>,
    pub(super) binary_cas: Arc<BinaryCasContext>,
    pub(super) commit_store: Arc<CommitStoreContext>,
    pub(super) version_ctx: Arc<VersionContext>,
    pub(super) visible_schemas: Vec<JsonValue>,
    pub(super) functions: FunctionProviderHandle,
}

impl SqlExecutionContext for SessionSqlExecutionContext<'_> {
    fn active_version_id(&self) -> &str {
        self.active_version_id
    }

    fn live_state(&self) -> Arc<dyn LiveStateReader> {
        Arc::new(self.live_state.reader(self.read_store.clone())) as Arc<dyn LiveStateReader>
    }

    fn commit_store_query_source(&self) -> SqlCommitStoreQuerySource {
        let read_scope = StorageReadScope::new(self.read_store.clone());
        CommitStoreQuerySource {
            commit_store_reader: Arc::new(self.commit_store.reader(read_scope.store())),
            json_reader: JsonStoreContext::new().reader(read_scope.store()),
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
