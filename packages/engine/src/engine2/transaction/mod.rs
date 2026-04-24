use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::backend::TransactionBeginMode;
use crate::binary_cas::BlobDataReader;
use crate::engine2::transaction::live_state_overlay::TransactionLiveStateContext;
use crate::engine2::transaction::staging::TransactionStagedWrites;
use crate::live_state::{CommittedLiveStateContext, LiveStateContext};
use crate::sql2::{SqlExecutionContext, SqlWriteStager};
use crate::transaction::TransactionCommitOutcome;
use crate::{LixBackend, LixBackendTransaction, LixError};

mod commit;
mod live_state_overlay;
mod staging;

/// One execution-scoped write transaction for the engine2 SQL path.
///
/// This is intentionally not a session-wide kitchen sink. It owns the backend
/// write transaction for one `Session::execute(...)` call and projects staged
/// SQL writes back into the SQL DAG through an engine2-local live-state
/// overlay.
pub(crate) struct Transaction<'a> {
    active_version_id: String,
    backend: &'a Arc<dyn LixBackend + Send + Sync>,
    committed_live_state: Arc<CommittedLiveStateContext>,
    staged_writes: Arc<TransactionStagedWrites>,
    backend_transaction: Box<dyn LixBackendTransaction + 'a>,
}

impl<'a> Transaction<'a> {
    /// Opens a backend write transaction and creates an execution-scoped
    /// staging area for SQL provider hooks.
    pub(crate) async fn open(
        active_version_id: String,
        backend: &'a Arc<dyn LixBackend + Send + Sync>,
        committed_live_state: Arc<CommittedLiveStateContext>,
    ) -> Result<Self, LixError> {
        let backend_transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await?;
        Ok(Self {
            active_version_id,
            backend,
            committed_live_state,
            staged_writes: Arc::new(TransactionStagedWrites::default()),
            backend_transaction,
        })
    }

    /// Commits the execution-scoped transaction.
    ///
    /// The first engine2 write path is intentionally naive: it flushes staged
    /// state rows directly into live_state, commits the backend transaction, and
    /// does not produce canonical commit graph rows yet.
    pub(crate) async fn commit(mut self) -> Result<TransactionCommitOutcome, LixError> {
        let staged_writes = self.staged_writes.drain()?;
        commit::commit_staged_writes(self.backend_transaction.as_mut(), staged_writes).await?;
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

    /// Returns committed live state with this transaction's staged rows
    /// overlaid on top.
    fn live_state(&self) -> Arc<dyn LiveStateContext> {
        let staged = self
            .staged_writes
            .staging_overlay()
            .expect("engine2 transaction should build staging overlay");
        let committed: Arc<dyn LiveStateContext> = self.committed_live_state.clone();
        Arc::new(TransactionLiveStateContext::new(committed, staged))
    }

    /// Provides blob reads for file/data surfaces during SQL execution.
    fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
        Arc::new(TransactionBackendBlobReader(Arc::clone(self.backend)))
    }

    /// Provides the transaction-scoped write stager used by DataFusion provider
    /// hooks while this statement executes.
    fn write_stager(&self) -> Option<Arc<dyn SqlWriteStager>> {
        Some(Arc::clone(&self.staged_writes) as Arc<dyn SqlWriteStager>)
    }

    /// Lists visible schemas for SQL surface registration.
    ///
    /// This is still a bootstrap implementation until engine2 owns the real
    /// schema registry context.
    fn list_visible_schemas(&self, version_id: &str) -> Result<Vec<JsonValue>, LixError> {
        let _ = version_id;
        // TODO(engine2): replace this hardcoded bootstrap schema with an
        // engine2-owned schema registry shared with the read-only session
        // context.
        let key_value_schema = crate::schema::builtin_schema_definition("lix_key_value")
            .ok_or_else(|| LixError::unknown("missing builtin lix_key_value schema"))?;
        Ok(vec![key_value_schema.clone()])
    }
}

struct TransactionBackendBlobReader(Arc<dyn LixBackend + Send + Sync>);

#[async_trait]
impl BlobDataReader for TransactionBackendBlobReader {
    /// Loads blob bytes from the backend CAS for SQL file/data reads.
    async fn load_blob_data_by_hash(&self, blob_hash: &str) -> Result<Option<Vec<u8>>, LixError> {
        crate::binary_cas::load_blob_data_by_hash(self.0.as_ref(), blob_hash).await
    }
}
