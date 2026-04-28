use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::backend::TransactionBeginMode;
use crate::binary_cas::{BinaryCasContext, BlobDataReader};
use crate::engine2::changelog::{ChangelogContext, ChangelogReader};
use crate::engine2::functions::{FunctionContext, FunctionProviderHandle};
use crate::engine2::live_state::{LiveStateContext, LiveStateReader};
use crate::engine2::schema_registry::SchemaRegistry;
use crate::engine2::transaction::commit;
use crate::engine2::transaction::live_state_overlay::TransactionLiveStateContext;
use crate::engine2::transaction::staging::TransactionStagedWrites;
use crate::engine2::version_ref::VersionRefContext;
use crate::sql2::{SqlExecutionContext, SqlWriteStager};
use crate::transaction::TransactionCommitOutcome;
use crate::{LixBackend, LixBackendTransaction, LixError};

/// One execution-scoped write transaction for the engine2 SQL path.
///
/// This is intentionally not a session-wide kitchen sink. It owns the backend
/// write transaction for one `SessionContext::execute(...)` call and projects
/// staged SQL writes back into the SQL DAG through an engine2-local live-state
/// overlay.
pub(crate) struct Transaction<'a> {
    active_version_id: String,
    backend: &'a Arc<dyn LixBackend + Send + Sync>,
    live_state: Arc<LiveStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    changelog: Arc<ChangelogContext>,
    version_ref: Arc<VersionRefContext>,
    staged_writes: Arc<TransactionStagedWrites>,
    backend_transaction: Box<dyn LixBackendTransaction + 'a>,
    visible_schemas: Vec<JsonValue>,
    functions: FunctionProviderHandle,
}

impl<'a> Transaction<'a> {
    /// Opens a backend write transaction and creates an execution-scoped
    /// staging area for SQL provider hooks.
    pub(crate) async fn open(
        active_version_id: String,
        backend: &'a Arc<dyn LixBackend + Send + Sync>,
        live_state: Arc<LiveStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        changelog: Arc<ChangelogContext>,
        version_ref: Arc<VersionRefContext>,
        schema_registry: Arc<SchemaRegistry>,
        functions: FunctionProviderHandle,
    ) -> Result<Self, LixError> {
        let staged_writes = Arc::new(TransactionStagedWrites::new(functions.clone()));
        let visible_live_state =
            transaction_live_state(backend, Arc::clone(&live_state), Arc::clone(&staged_writes))?;
        let visible_schemas = schema_registry
            .visible_schemas(visible_live_state, &active_version_id)
            .await?;
        let backend_transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await?;
        Ok(Self {
            active_version_id,
            backend,
            live_state,
            binary_cas,
            changelog,
            version_ref,
            staged_writes,
            backend_transaction,
            visible_schemas,
            functions,
        })
    }

    /// Commits staged writes, runtime function state, and the backend transaction.
    ///
    /// Commit owns the execution boundary: provider-staged rows become
    /// changelog facts, `lix_commit` rows, version-ref updates, and visible
    /// live_state rows before the backend transaction is committed.
    pub(crate) async fn commit(
        mut self,
        runtime_functions: &FunctionContext,
    ) -> Result<TransactionCommitOutcome, LixError> {
        let staged_writes = self.staged_writes.drain()?;
        commit::commit_staged_writes(
            &self.binary_cas,
            &self.changelog,
            &self.live_state,
            &self.version_ref,
            self.backend_transaction.as_mut(),
            staged_writes,
        )
        .await?;
        runtime_functions
            .persist_if_needed(&mut self.live_state.writer(self.backend_transaction.as_mut()))
            .await?;
        self.backend_transaction.commit().await?;
        Ok(TransactionCommitOutcome::default())
    }

    /// Rolls back the backend transaction.
    ///
    /// This is the explicit failure path for a write execution. Dropping the
    /// buffered transaction without commit is not the API we want callers to
    /// rely on.
    #[allow(dead_code)]
    pub(crate) async fn rollback(self) -> Result<(), LixError> {
        self.backend_transaction.rollback().await
    }
}

impl SqlExecutionContext for Transaction<'_> {
    /// Returns the version that active-version SQL surfaces should resolve to.
    fn active_version_id(&self) -> &str {
        &self.active_version_id
    }

    /// Returns live state with this transaction's staged rows overlaid on top.
    fn live_state(&self) -> Arc<dyn LiveStateReader> {
        transaction_live_state(
            self.backend,
            Arc::clone(&self.live_state),
            Arc::clone(&self.staged_writes),
        )
        .expect("engine2 transaction should build staging overlay")
    }

    fn changelog(&self) -> Option<Arc<dyn ChangelogReader>> {
        Some(Arc::new(self.changelog.reader(Arc::clone(self.backend))))
    }

    /// Returns the same function provider used by the owning session.
    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    /// Provides blob reads for file/data surfaces during SQL execution.
    fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
        Arc::new(self.binary_cas.reader(Arc::clone(self.backend))) as Arc<dyn BlobDataReader>
    }

    /// Provides the transaction-scoped write stager used by DataFusion provider
    /// hooks while this statement executes.
    fn write_stager(&self) -> Option<Arc<dyn SqlWriteStager>> {
        Some(Arc::clone(&self.staged_writes) as Arc<dyn SqlWriteStager>)
    }

    /// Returns the transaction-scoped schema snapshot for SQL surface
    /// registration.
    fn list_visible_schemas(&self, version_id: &str) -> Result<Vec<JsonValue>, LixError> {
        let _ = version_id;
        Ok(self.visible_schemas.clone())
    }
}

fn transaction_live_state(
    backend: &Arc<dyn LixBackend + Send + Sync>,
    live_state: Arc<LiveStateContext>,
    staged_writes: Arc<TransactionStagedWrites>,
) -> Result<Arc<dyn LiveStateReader>, LixError> {
    let staged = staged_writes.staging_overlay()?;
    let base: Arc<dyn LiveStateReader> = Arc::new(live_state.reader(Arc::clone(backend)));
    Ok(Arc::new(TransactionLiveStateContext::new(base, staged)))
}
