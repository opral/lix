use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::backend::TransactionBeginMode;
use crate::binary_cas::BlobDataReader;
use crate::engine2::write_services::WriteServices;
use crate::live_state::{CommittedLiveStateContext, LiveStateContext};
use crate::sql2::{
    stage_decoded_write, SqlExecutionContext, SqlWriteIntent, SqlWriteOutcome, SqlWriteStager,
};
use crate::transaction::{BufferedWriteExecutionInput, TransactionCommitOutcome};
use crate::transaction::{BufferedWriteTransaction, TransactionLiveStateContext};
use crate::{LixBackend, LixError};

/// One execution-scoped write transaction for the engine2 SQL path.
///
/// This is intentionally not a session-wide kitchen sink. It owns the buffered
/// write transaction for one `Session::execute(...)` call and projects that
/// transaction into the SQL DAG via `TransactionLiveStateContext`.
pub(crate) struct Transaction<'a> {
    backend: &'a Arc<dyn LixBackend + Send + Sync>,
    committed_live_state: Arc<CommittedLiveStateContext>,
    // Concrete engine-owned services used when flushing the buffered write
    // pipeline. The transaction owns the staged writes, not these services.
    write_services: Arc<WriteServices>,
    // TODO(engine2): replace this collector bridge with a direct transaction
    // stager. Provider hooks currently require `Arc<dyn SqlWriteStager>`, while
    // `BufferedWriteTransaction` is transaction-scoped and mutable.
    pending_sql_writes: Arc<PendingSqlWriteStager>,
    write_transaction: BufferedWriteTransaction<'a>,
}

impl<'a> Transaction<'a> {
    /// Opens a backend write transaction and wraps it in the buffered
    /// transaction pipeline.
    ///
    /// The committed live state is kept here so the transaction can later build
    /// a `TransactionLiveStateContext` that overlays staged writes on top of
    /// committed reads.
    pub(crate) async fn new(
        backend: &'a Arc<dyn LixBackend + Send + Sync>,
        committed_live_state: Arc<CommittedLiveStateContext>,
        write_services: Arc<WriteServices>,
    ) -> Result<Self, LixError> {
        let backend_transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await?;
        Ok(Self {
            backend,
            committed_live_state,
            write_services,
            pending_sql_writes: Arc::new(PendingSqlWriteStager::default()),
            write_transaction: BufferedWriteTransaction::new(backend_transaction),
        })
    }

    /// Builds the SQL execution context for this transaction.
    ///
    /// SQL should not read directly from committed live state while executing a
    /// write statement. Instead, reads go through `TransactionLiveStateContext`,
    /// which combines committed live state with the transaction's pending write
    /// overlay.
    ///
    /// TODO(engine2): once this context exposes a provider-hook write stager,
    /// refresh this context after each staged write if a single DML execution
    /// can perform subsequent reads that must observe earlier staged rows.
    pub(crate) fn sql_execution_context(
        &self,
        active_version_id: &'a str,
    ) -> Result<TransactionSqlExecutionContext<'a>, LixError> {
        let pending_overlay = self
            .write_transaction
            .buffered_write_pending_write_overlay()?;
        let live_state = Arc::new(TransactionLiveStateContext::new(
            self.committed_live_state.clone(),
            pending_overlay,
        ));

        Ok(TransactionSqlExecutionContext {
            active_version_id,
            backend: Arc::clone(self.backend),
            live_state,
            write_stager: Some(Arc::clone(&self.pending_sql_writes) as Arc<dyn SqlWriteStager>),
        })
    }

    /// Stages a SQL write intent into the buffered transaction.
    ///
    /// SQL execution decodes DML into semantic intents, but the transaction
    /// remains the only owner of the actual Lix buffered write pipeline.
    #[allow(dead_code)]
    pub(crate) fn stage_sql_write(
        &mut self,
        write: SqlWriteIntent,
    ) -> Result<SqlWriteOutcome, LixError> {
        stage_decoded_write(&mut self.write_transaction, write)
    }

    /// Commits the execution-scoped transaction.
    ///
    /// The caller is responsible for staging SQL writes before commit. Commit
    /// flushes the buffered write pipeline, then commits the backend
    /// transaction.
    pub(crate) async fn commit(
        mut self,
        active_version_id: &str,
    ) -> Result<TransactionCommitOutcome, LixError> {
        let pending_writes = self.pending_sql_writes.drain()?;
        for write in pending_writes {
            self.stage_sql_write(write)?;
        }

        // TODO(engine2): active account ids and origin key are hardcoded until
        // engine2 owns workspace/session selector state.
        let execution_input =
            BufferedWriteExecutionInput::new(None, active_version_id.to_string(), Vec::new());
        self.write_transaction
            .commit(self.write_services.as_ref(), execution_input)
            .await
    }

    /// Rolls back the backend transaction.
    ///
    /// This is the explicit failure path for a write execution. Dropping the
    /// buffered transaction without commit is not the API we want callers to
    /// rely on.
    #[allow(dead_code)]
    pub(crate) async fn rollback(self) -> Result<(), LixError> {
        self.write_transaction.rollback().await
    }
}

/// SQL-facing adapter for an execution-scoped transaction.
///
/// This type intentionally contains only what `sql2` needs: the active version,
/// blob access, transaction-aware live state, and eventually the write stager.
pub(crate) struct TransactionSqlExecutionContext<'a> {
    active_version_id: &'a str,
    backend: Arc<dyn LixBackend + Send + Sync>,
    live_state: Arc<dyn LiveStateContext>,
    write_stager: Option<Arc<dyn SqlWriteStager>>,
}

impl SqlExecutionContext for TransactionSqlExecutionContext<'_> {
    /// Returns the version that active-version SQL surfaces should resolve to.
    fn active_version_id(&self) -> &str {
        self.active_version_id
    }

    /// Returns committed live state with the transaction pending-write overlay
    /// applied.
    fn live_state(&self) -> Arc<dyn LiveStateContext> {
        Arc::clone(&self.live_state)
    }

    /// Provides blob reads for file/data surfaces during SQL execution.
    fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
        Arc::new(TransactionBackendBlobReader(Arc::clone(&self.backend)))
    }

    /// Provides the transaction-scoped write collector used by DataFusion
    /// provider hooks while this statement executes.
    fn write_stager(&self) -> Option<Arc<dyn SqlWriteStager>> {
        self.write_stager.as_ref().map(Arc::clone)
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

#[derive(Default)]
struct PendingSqlWriteStager {
    writes: Mutex<Vec<SqlWriteIntent>>,
}

impl PendingSqlWriteStager {
    fn drain(&self) -> Result<Vec<SqlWriteIntent>, LixError> {
        let mut guard = self.writes.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire pending SQL write collector lock",
            )
        })?;
        Ok(std::mem::take(&mut *guard))
    }
}

#[async_trait]
impl SqlWriteStager for PendingSqlWriteStager {
    async fn stage_write(&self, write: SqlWriteIntent) -> Result<SqlWriteOutcome, LixError> {
        let count = match &write {
            SqlWriteIntent::InsertRows { rows } | SqlWriteIntent::DeleteRows { rows } => {
                rows.len() as u64
            }
            SqlWriteIntent::InsertRowsWithFileData { count, .. } => *count,
        };
        let mut guard = self.writes.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire pending SQL write collector lock",
            )
        })?;
        guard.push(write);
        Ok(SqlWriteOutcome { count })
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
