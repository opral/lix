use async_trait::async_trait;

use crate::sql::execution::contracts::prepared_statement::{PreparedBatch, PreparedStatement};
use crate::{ImageChunkReader, ImageChunkWriter, LixError, QueryResult, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Sqlite,
    Postgres,
}

#[async_trait(?Send)]
pub trait LixBackend: Send + Sync {
    fn dialect(&self) -> SqlDialect;

    /// Execute a single SQL statement on the connection.
    ///
    /// No automatic transaction wrapping. If no transaction is active,
    /// the statement auto-commits (standard SQL behavior). If a transaction
    /// IS active, the statement participates in it.
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;

    /// Begin an exclusive transaction (`BEGIN IMMEDIATE` for SQLite).
    ///
    /// The returned handle holds exclusive access to the connection.
    /// All SQL must go through the handle until commit/rollback.
    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError>;

    /// Begin a named savepoint within an active transaction.
    ///
    /// Returns a handle that commits via `RELEASE SAVEPOINT`
    /// and rolls back via `ROLLBACK TO SAVEPOINT`.
    /// The caller provides the name.
    async fn begin_savepoint(&self, name: &str) -> Result<Box<dyn LixTransaction + '_>, LixError>;

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

/// Utility: execute a single statement wrapped in a transaction.
///
/// Useful for backends (e.g. test/pool backends) that want auto-transactional
/// execute() behavior. Production backends like SqliteBackend should NOT use
/// this — they execute directly on the connection.
pub async fn execute_auto_transactional(
    backend: &dyn LixBackend,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    let mut transaction = backend.begin_transaction().await?;
    let result = transaction.execute(sql, params).await;
    match result {
        Ok(result) => {
            transaction.commit().await?;
            Ok(result)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
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
impl QueryExecutor for Box<dyn LixTransaction + '_> {
    fn dialect(&self) -> SqlDialect {
        self.as_ref().dialect()
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.as_mut().execute(sql, params).await
    }
}

#[async_trait(?Send)]
pub trait LixTransaction {
    fn dialect(&self) -> SqlDialect;

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

pub async fn execute_statement_with_backend(
    backend: &dyn LixBackend,
    statement: PreparedStatement,
) -> Result<QueryResult, LixError> {
    let mut transaction = backend.begin_transaction().await?;
    let result = transaction.execute(&statement.sql, &statement.params).await;
    match result {
        Ok(result) => {
            transaction.commit().await?;
            Ok(result)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}
