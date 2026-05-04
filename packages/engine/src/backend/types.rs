use async_trait::async_trait;

use crate::backend::{
    BackendKvGetBatch, BackendKvGetRequest, BackendKvScanBatch, BackendKvScanRequest,
    BackendKvWriteBatch, BackendKvWriteStats,
};
use crate::LixError;

#[async_trait]
pub trait Backend: Send + Sync {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError>;

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError>;

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
            message: "destroy is not supported by this backend".to_string(),
            hint: None,
            details: None,
        })
    }
}

#[async_trait]
pub trait BackendReadTransaction: Send + Sync {
    async fn get_kv_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvGetBatch, LixError>;

    async fn scan_kv(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvScanBatch, LixError>;

    async fn rollback(self: Box<Self>) -> Result<(), LixError>;
}

#[async_trait]
pub trait BackendWriteTransaction: BackendReadTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError>;

    async fn commit(self: Box<Self>) -> Result<(), LixError>;
}
