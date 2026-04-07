pub(crate) mod ddl;
pub(crate) mod image;
pub(crate) mod prepared;
pub(crate) mod program;
pub(crate) mod program_runner;
pub(crate) mod transaction_adapter;

use async_trait::async_trait;
use std::collections::BTreeMap;

use crate::backend::prepared::PreparedBatch;
use crate::contracts::traits::SqlPreparationMetadataReader;
use crate::sql::common::dialect::SqlDialect;
pub use crate::transaction_mode::TransactionMode;
use crate::version::{
    load_local_version_head_commit_id_with_executor, load_local_version_ref_heads_map_with_executor,
};
use crate::{LixError, QueryResult, Value};
pub use image::{ImageChunkReader, ImageChunkWriter};
pub(crate) use transaction_adapter::TransactionBackendAdapter;

#[async_trait(?Send)]
pub trait LixBackend: Send + Sync {
    fn dialect(&self) -> SqlDialect;

    /// Execute a single SQL statement on the connection.
    ///
    /// No automatic transaction wrapping. If no transaction is active,
    /// the statement auto-commits (standard SQL behavior). If a transaction
    /// IS active, the statement participates in it.
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;

    /// Begin a transaction using the requested mode.
    ///
    /// The returned handle holds exclusive access to the connection.
    /// All SQL must go through the handle until commit/rollback.
    async fn begin_transaction(
        &self,
        mode: TransactionMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError>;

    /// Begin a named savepoint within an active transaction.
    ///
    /// Returns a handle that commits via `RELEASE SAVEPOINT`
    /// and rolls back via `ROLLBACK TO SAVEPOINT`.
    /// The caller provides the name.
    async fn begin_savepoint(
        &self,
        name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError>;

    /// Exports the current Lix database snapshot as a SQLite database file payload.
    async fn export_image(&self, _writer: &mut dyn ImageChunkWriter) -> Result<(), LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "export_image is not supported by this backend".to_string(),
        })
    }

    /// Restores backend state from a SQLite database file payload stream.
    async fn restore_from_image(&self, _reader: &mut dyn ImageChunkReader) -> Result<(), LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "restore_from_image is not supported by this backend".to_string(),
        })
    }

    /// Destroys the physical storage target represented by this backend.
    ///
    /// This is a persistence lifecycle operation, not a logical SQL operation.
    ///
    /// Callers should treat the backend as the authority for what constitutes
    /// the full storage target. For example:
    ///
    /// - native SQLite may delete the main database file plus WAL/SHM sidecars
    /// - wasm/opfs SQLite may clear the persisted OPFS target
    /// - Postgres may drop or clear the configured schema/database target
    ///
    /// Callers must not attempt to infer or delete backend-owned physical
    /// artifacts themselves.
    ///
    /// Implementations may choose not to support destroy if the backend
    /// instance does not have enough information or authority to remove its
    /// target.
    async fn destroy(&self) -> Result<(), LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "destroy is not supported by this backend".to_string(),
        })
    }
}

#[async_trait(?Send)]
pub(crate) trait QueryExecutor {
    fn dialect(&self) -> SqlDialect;
    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;
}

#[async_trait(?Send)]
impl<T> QueryExecutor for &T
where
    T: LixBackend + ?Sized,
{
    fn dialect(&self) -> SqlDialect {
        (*self).dialect()
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        (*self).execute(sql, params).await
    }
}

#[async_trait(?Send)]
impl<T> SqlPreparationMetadataReader for &T
where
    T: LixBackend + ?Sized,
{
    async fn execute_preparation_query(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        (*self).execute(sql, params).await
    }

    async fn load_current_version_heads_for_preparation(
        &mut self,
    ) -> Result<Option<BTreeMap<String, String>>, LixError> {
        load_current_version_heads_for_preparation_with_executor(self).await
    }

    async fn load_active_history_root_commit_id_for_preparation(
        &mut self,
        active_version_id: &str,
    ) -> Result<Option<String>, LixError> {
        load_active_history_root_commit_id_for_preparation_with_executor(self, active_version_id)
            .await
    }
}

#[async_trait(?Send)]
impl QueryExecutor for Box<dyn LixBackendTransaction + '_> {
    fn dialect(&self) -> SqlDialect {
        self.as_ref().dialect()
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.as_mut().execute(sql, params).await
    }
}

#[async_trait(?Send)]
impl SqlPreparationMetadataReader for Box<dyn LixBackendTransaction + '_> {
    async fn execute_preparation_query(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        self.as_mut().execute(sql, params).await
    }

    async fn load_current_version_heads_for_preparation(
        &mut self,
    ) -> Result<Option<BTreeMap<String, String>>, LixError> {
        load_current_version_heads_for_preparation_with_executor(self).await
    }

    async fn load_active_history_root_commit_id_for_preparation(
        &mut self,
        active_version_id: &str,
    ) -> Result<Option<String>, LixError> {
        load_active_history_root_commit_id_for_preparation_with_executor(self, active_version_id)
            .await
    }
}

#[async_trait(?Send)]
impl<T> QueryExecutor for &mut T
where
    T: LixBackendTransaction + ?Sized,
{
    fn dialect(&self) -> SqlDialect {
        (**self).dialect()
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        (**self).execute(sql, params).await
    }
}

#[async_trait(?Send)]
impl<T> SqlPreparationMetadataReader for &mut T
where
    T: LixBackendTransaction + ?Sized,
{
    async fn execute_preparation_query(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        (**self).execute(sql, params).await
    }

    async fn load_current_version_heads_for_preparation(
        &mut self,
    ) -> Result<Option<BTreeMap<String, String>>, LixError> {
        load_current_version_heads_for_preparation_with_executor(self).await
    }

    async fn load_active_history_root_commit_id_for_preparation(
        &mut self,
        active_version_id: &str,
    ) -> Result<Option<String>, LixError> {
        load_active_history_root_commit_id_for_preparation_with_executor(self, active_version_id)
            .await
    }
}

async fn load_current_version_heads_for_preparation_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Option<BTreeMap<String, String>>, LixError> {
    match load_local_version_ref_heads_map_with_executor(executor).await {
        Ok(heads) => Ok(heads),
        Err(error)
            if error
                .description
                .contains("schema 'lix_version' is not stored") =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

async fn load_active_history_root_commit_id_for_preparation_with_executor(
    executor: &mut dyn QueryExecutor,
    active_version_id: &str,
) -> Result<Option<String>, LixError> {
    match load_local_version_head_commit_id_with_executor(executor, active_version_id).await {
        Ok(commit_id) => Ok(commit_id),
        Err(error)
            if error
                .description
                .contains("schema 'lix_version' is not stored") =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[async_trait(?Send)]
pub trait LixBackendTransaction {
    fn dialect(&self) -> SqlDialect;
    fn mode(&self) -> TransactionMode;

    /// Executes one SQL statement inside the current transaction.
    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;

    /// Executes one parameterized SQL batch inside the current transaction.
    async fn execute_batch(&mut self, batch: &PreparedBatch) -> Result<QueryResult, LixError> {
        let mut last_result = QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        };
        for statement in &batch.steps {
            last_result = self.execute(&statement.sql, &statement.params).await?;
        }
        Ok(last_result)
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError>;

    async fn rollback(self: Box<Self>) -> Result<(), LixError>;
}
