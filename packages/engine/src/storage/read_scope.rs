use std::sync::Arc;

use crate::storage::{
    KvEntryPage, KvExistsBatch, KvGetRequest, KvKeyPage, KvRead3Page, KvRead3Request, KvScan2Page,
    KvScan2Request, KvScanPlanPage, KvScanPlanRequest, KvScanRequest, KvValueBatch, KvValuePage,
    StorageReadTransaction, StorageReader,
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
    async fn get_values(&mut self, request: KvGetRequest) -> Result<KvValueBatch, LixError> {
        let mut store = self.store.lock().await;
        store.get_values(request).await
    }

    async fn exists_many(&mut self, request: KvGetRequest) -> Result<KvExistsBatch, LixError> {
        let mut store = self.store.lock().await;
        store.exists_many(request).await
    }

    async fn scan_keys(&mut self, request: KvScanRequest) -> Result<KvKeyPage, LixError> {
        let mut store = self.store.lock().await;
        store.scan_keys(request).await
    }

    async fn scan_values(&mut self, request: KvScanRequest) -> Result<KvValuePage, LixError> {
        let mut store = self.store.lock().await;
        store.scan_values(request).await
    }

    async fn scan_entries(&mut self, request: KvScanRequest) -> Result<KvEntryPage, LixError> {
        let mut store = self.store.lock().await;
        store.scan_entries(request).await
    }

    async fn scan2(&mut self, request: KvScan2Request) -> Result<KvScan2Page, LixError> {
        let mut store = self.store.lock().await;
        store.scan2(request).await
    }

    async fn scan_plan(&mut self, request: KvScanPlanRequest) -> Result<KvScanPlanPage, LixError> {
        let mut store = self.store.lock().await;
        store.scan_plan(request).await
    }

    async fn read3(&mut self, request: KvRead3Request) -> Result<KvRead3Page, LixError> {
        let mut store = self.store.lock().await;
        store.read3(request).await
    }
}
