mod ddl;
mod image;
mod prepared;
mod program;
mod program_runner;
mod transaction_adapter;

use async_trait::async_trait;

use crate::common::SqlDialect;
pub use crate::contracts::TransactionMode;
use crate::{LixError, QueryResult, Value};
#[allow(unused_imports)]
pub(crate) use ddl::{add_column_if_missing, execute_ddl_batch};
pub use image::{ImageChunkReader, ImageChunkWriter};
#[allow(unused_imports)]
pub use prepared::{PreparedBatch, PreparedStatement};
#[allow(unused_imports)]
pub(crate) use program::WriteProgram;
#[allow(unused_imports)]
pub(crate) use program_runner::{
    execute_write_program_with_backend, execute_write_program_with_transaction,
};
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
impl QueryExecutor for Box<dyn LixBackendTransaction + '_> {
    fn dialect(&self) -> SqlDialect {
        self.as_ref().dialect()
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.as_mut().execute(sql, params).await
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

pub(crate) fn transaction_backend_view(
    transaction: &mut dyn LixBackendTransaction,
) -> TransactionBackendAdapter<'_> {
    TransactionBackendAdapter::new(transaction)
}
