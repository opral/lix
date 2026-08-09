use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::storage::{
    BeginScanOptions, GetManyRequest, GetManyResult, KeyRange, ScanCursor, StorageError,
    StorageRead, StorageSpace,
};

#[cfg(feature = "storage-benches")]
#[derive(Clone, Copy, Debug, Default)]
pub struct StorageAdapterReadCounters {
    pub get_many_calls: u64,
    pub requested_keys: u64,
    pub returned_values: u64,
    pub returned_bytes: u64,
}

#[cfg(feature = "storage-benches")]
static GET_MANY_CALLS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "storage-benches")]
static REQUESTED_KEYS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "storage-benches")]
static RETURNED_VALUES: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "storage-benches")]
static RETURNED_BYTES: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "storage-benches")]
pub fn reset_storage_adapter_read_counters() {
    GET_MANY_CALLS.store(0, Ordering::Relaxed);
    REQUESTED_KEYS.store(0, Ordering::Relaxed);
    RETURNED_VALUES.store(0, Ordering::Relaxed);
    RETURNED_BYTES.store(0, Ordering::Relaxed);
}

#[cfg(feature = "storage-benches")]
pub fn storage_adapter_read_counters() -> StorageAdapterReadCounters {
    StorageAdapterReadCounters {
        get_many_calls: GET_MANY_CALLS.load(Ordering::Relaxed),
        requested_keys: REQUESTED_KEYS.load(Ordering::Relaxed),
        returned_values: RETURNED_VALUES.load(Ordering::Relaxed),
        returned_bytes: RETURNED_BYTES.load(Ordering::Relaxed),
    }
}

#[cfg(feature = "storage-benches")]
fn record_get_many(requests: &[GetManyRequest<'_>], result: &GetManyResult) {
    GET_MANY_CALLS.fetch_add(1, Ordering::Relaxed);
    REQUESTED_KEYS.fetch_add(
        requests
            .iter()
            .map(|request| request.keys.len() as u64)
            .sum(),
        Ordering::Relaxed,
    );
    let mut returned_values = 0;
    let mut returned_bytes = 0;
    for value in &result.values {
        if let Some(crate::storage::ProjectedValue::FullValue(bytes)) = value {
            returned_values += 1;
            returned_bytes += bytes.len() as u64;
        }
    }
    RETURNED_VALUES.fetch_add(returned_values, Ordering::Relaxed);
    RETURNED_BYTES.fetch_add(returned_bytes, Ordering::Relaxed);
}

/// The async read capability consumed by engine stores.
///
/// Implementations preserve one coherent storage read view while allowing
/// independent point and scan requests to overlap.
pub trait StorageAdapterRead: Send + Sync {
    fn snapshot_cache_key(&self) -> Option<u128> {
        None
    }

    fn operation_cache(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send;

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send;
}

/// Type-erased state attached to one retained read scope. Domain-specific
/// readers may place an operation-local authenticated index here without
/// making this neutral adapter layer own that index's schema or bytes.
pub(crate) struct OperationReadCache {
    read_identity: u128,
    values: Mutex<Vec<Arc<dyn Any + Send + Sync>>>,
}

impl std::fmt::Debug for OperationReadCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OperationReadCache")
            .field("read_identity", &self.read_identity)
            .finish_non_exhaustive()
    }
}

static NEXT_OPERATION_READ_ID: AtomicU64 = AtomicU64::new(1);

impl OperationReadCache {
    pub(crate) fn new() -> Self {
        Self {
            read_identity: u128::from(NEXT_OPERATION_READ_ID.fetch_add(1, Ordering::Relaxed)),
            values: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn read_identity(&self) -> u128 {
        self.read_identity
    }

    pub(crate) fn get_all<T>(&self) -> Vec<Arc<T>>
    where
        T: Any + Send + Sync,
    {
        self.values
            .lock()
            .expect("operation read cache mutex is not poisoned")
            .iter()
            .filter_map(|value| Arc::downcast::<T>(value.clone()).ok())
            .collect()
    }

    pub(crate) fn install<T>(&self, value: Arc<T>) -> bool
    where
        T: Any + Send + Sync,
    {
        self.values
            .lock()
            .expect("operation read cache mutex is not poisoned")
            .push(value);
        true
    }
}

#[derive(Debug)]
pub struct StorageAdapterReadScope<R> {
    read: R,
    operation_cache: Arc<OperationReadCache>,
}

impl<R> StorageAdapterReadScope<R> {
    pub fn new(read: R) -> Self {
        Self {
            read,
            operation_cache: Arc::new(OperationReadCache::new()),
        }
    }

    fn into_parts(self) -> (R, Arc<OperationReadCache>) {
        (self.read, self.operation_cache)
    }
}

/// Cloneable SQL/DataFusion bridge for one execution-scoped storage read.
///
/// Clones share the read handle directly. Concurrency and synchronization are
/// storage responsibilities; this layer never serializes requests.
pub(crate) struct SharedStorageAdapterRead<R>
where
    R: StorageRead,
{
    read: Arc<R>,
    operation_cache: Arc<OperationReadCache>,
}

impl<R> SharedStorageAdapterRead<R>
where
    R: StorageRead,
{
    pub(crate) fn new(read: StorageAdapterReadScope<R>) -> Self {
        let (read, operation_cache) = read.into_parts();
        Self {
            read: Arc::new(read),
            operation_cache,
        }
    }

    pub(crate) fn operation_cache(&self) -> Arc<OperationReadCache> {
        Arc::clone(&self.operation_cache)
    }

    pub(crate) fn finish(self) -> Result<(), StorageError> {
        let read = Arc::try_unwrap(self.read).map_err(|read| {
            StorageError::Io(format!(
                "shared storage read still has {} active handles",
                Arc::strong_count(&read) - 1
            ))
        })?;
        drop(read);
        Ok(())
    }
}

impl<R> Clone for SharedStorageAdapterRead<R>
where
    R: StorageRead,
{
    fn clone(&self) -> Self {
        Self {
            read: Arc::clone(&self.read),
            operation_cache: Arc::clone(&self.operation_cache),
        }
    }
}

impl<R> StorageAdapterRead for StorageAdapterReadScope<R>
where
    R: StorageRead,
{
    fn operation_cache(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        let cache: Arc<dyn Any + Send + Sync> = self.operation_cache.clone();
        Some(cache)
    }

    fn snapshot_cache_key(&self) -> Option<u128> {
        self.read.snapshot_cache_key()
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        let future = self.read.get_many(requests);
        async move {
            let result = future.await;
            #[cfg(feature = "storage-benches")]
            if let Ok(result) = &result {
                record_get_many(requests, result);
            }
            result
        }
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        self.read.begin_scan(space, range, opts)
    }
}

impl<R> StorageAdapterRead for SharedStorageAdapterRead<R>
where
    R: StorageRead,
{
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.read.snapshot_cache_key()
    }

    fn operation_cache(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        Some(self.operation_cache())
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        let future = self.read.get_many(requests);
        async move {
            let result = future.await;
            #[cfg(feature = "storage-benches")]
            if let Ok(result) = &result {
                record_get_many(requests, result);
            }
            result
        }
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        self.read.begin_scan(space, range, opts)
    }
}

impl<T> StorageAdapterRead for &T
where
    T: StorageAdapterRead + ?Sized,
{
    fn snapshot_cache_key(&self) -> Option<u128> {
        (*self).snapshot_cache_key()
    }

    fn operation_cache(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        (*self).operation_cache()
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        (*self).get_many(requests)
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        (*self).begin_scan(space, range, opts)
    }
}

impl<T> StorageAdapterRead for &mut T
where
    T: StorageAdapterRead + ?Sized,
{
    fn snapshot_cache_key(&self) -> Option<u128> {
        (**self).snapshot_cache_key()
    }

    fn operation_cache(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        (**self).operation_cache()
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        (**self).get_many(requests)
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        (**self).begin_scan(space, range, opts)
    }
}
