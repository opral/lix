use async_trait::async_trait;

use crate::backend::{KvPair, KvScanRange, TransactionBeginMode};
use crate::LixError;

#[async_trait]
pub trait LixBackend: Send + Sync {
    async fn begin_transaction(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + Send + Sync + 'static>, LixError>;

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

    /// Releases physical resources held by this backend handle.
    ///
    /// This is a resource lifecycle operation, not a durability boundary and
    /// not a destructive operation. Successful write transactions are durable
    /// when their commit returns; callers should not rely on `close` to save
    /// data. Implementations that do not own external resources may keep the
    /// default no-op behavior.
    async fn close(&self) -> Result<(), LixError> {
        Ok(())
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
    fn mode(&self) -> TransactionBeginMode;

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

    async fn commit(self: Box<Self>) -> Result<(), LixError>;

    async fn rollback(self: Box<Self>) -> Result<(), LixError>;
}

fn kv_not_supported(operation: &str) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("{operation} is not supported by this backend"),
    )
}
