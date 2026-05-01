use async_trait::async_trait;

use crate::backend::{
    ImageChunkReader, ImageChunkWriter, KvPair, KvScanRange, PreparedBatch, TransactionBeginMode,
};
use crate::common::SqlDialect;
use crate::{LixError, QueryResult, Value};

#[async_trait]
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
        mode: TransactionBeginMode,
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

    /// Reads one value from the backend key/value store.
    async fn kv_get(&self, _namespace: &str, _key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        Err(kv_not_supported("kv_get"))
    }

    /// Scans key/value pairs in lexicographic key order.
    async fn kv_scan(
        &self,
        _namespace: &str,
        _range: KvScanRange,
        _limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        Err(kv_not_supported("kv_scan"))
    }

    /// Exports the current Lix database snapshot as a SQLite database file payload.
    async fn export_image(&self, _writer: &mut dyn ImageChunkWriter) -> Result<(), LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "export_image is not supported by this backend".to_string(),
            hint: None,
        })
    }

    /// Restores backend state from a SQLite database file payload stream.
    async fn restore_from_image(&self, _reader: &mut dyn ImageChunkReader) -> Result<(), LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "restore_from_image is not supported by this backend".to_string(),
            hint: None,
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
            hint: None,
        })
    }
}

#[async_trait]
pub trait LixBackendTransaction: Send + Sync {
    fn dialect(&self) -> SqlDialect;
    fn mode(&self) -> TransactionBeginMode;

    /// Executes one SQL statement inside the current transaction.
    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;

    /// Reads one value from the backend key/value store inside this transaction.
    async fn kv_get(&mut self, _namespace: &str, _key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        Err(kv_not_supported("transaction kv_get"))
    }

    /// Scans key/value pairs in lexicographic key order inside this transaction.
    async fn kv_scan(
        &mut self,
        _namespace: &str,
        _range: KvScanRange,
        _limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        Err(kv_not_supported("transaction kv_scan"))
    }

    /// Writes one key/value pair inside this transaction.
    async fn kv_put(
        &mut self,
        _namespace: &str,
        _key: &[u8],
        _value: &[u8],
    ) -> Result<(), LixError> {
        Err(kv_not_supported("transaction kv_put"))
    }

    /// Deletes one key/value pair inside this transaction.
    async fn kv_delete(&mut self, _namespace: &str, _key: &[u8]) -> Result<(), LixError> {
        Err(kv_not_supported("transaction kv_delete"))
    }

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

fn kv_not_supported(operation: &str) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("{operation} is not supported by this backend"),
    )
}
