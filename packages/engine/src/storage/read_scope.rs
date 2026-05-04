use std::sync::Arc;

use crate::storage::{
    KvGetBatch, KvGetRequest, KvScanBatch, KvScanRequest, StorageReadTransaction, StorageReader,
};
use crate::LixError;
use tokio::sync::Mutex;

/// Shared read visibility over one KV store handle.
///
/// This lets multiple subsystem readers share the same transaction/backend view
/// even when the underlying handle itself is not cloneable.
pub(crate) struct StorageReadScope<S> {
    store: Arc<Mutex<S>>,
}

impl<S> StorageReadScope<S>
where
    S: StorageReader,
{
    pub(crate) fn new(store: S) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
        }
    }

    pub(crate) fn store(&self) -> ScopedStorageReader<S> {
        ScopedStorageReader {
            store: Arc::clone(&self.store),
        }
    }
}

impl StorageReadScope<Box<dyn StorageReadTransaction + Send + Sync + 'static>> {
    pub(crate) async fn rollback(self) -> Result<(), LixError> {
        let store = Arc::try_unwrap(self.store).map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "cannot close storage read scope while scoped readers are still alive",
            )
        })?;
        store.into_inner().rollback().await
    }
}

pub(crate) struct ScopedStorageReader<S> {
    store: Arc<Mutex<S>>,
}

impl<S> Clone for ScopedStorageReader<S> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
        }
    }
}

#[async_trait::async_trait]
impl<S> StorageReader for ScopedStorageReader<S>
where
    S: StorageReader,
{
    async fn get_kv_many(&mut self, request: KvGetRequest) -> Result<KvGetBatch, LixError> {
        let mut store = self.store.lock().await;
        store.get_kv_many(request).await
    }

    async fn scan_kv(&mut self, request: KvScanRequest) -> Result<KvScanBatch, LixError> {
        let mut store = self.store.lock().await;
        store.scan_kv(request).await
    }
}
