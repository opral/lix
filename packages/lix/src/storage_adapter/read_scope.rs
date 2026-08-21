use std::sync::Arc;

use crate::storage::{
    BeginScanOptions, GetManyRequest, GetManyResult, KeyRange, ScanCursor, StorageError,
    StorageRead, StorageSpace,
};

/// The async read capability consumed by engine stores.
///
/// Implementations preserve one coherent storage read view while allowing
/// independent point and scan requests to overlap.
pub trait StorageAdapterRead: Send + Sync {
    fn snapshot_cache_key(&self) -> Option<u128> {
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

#[derive(Debug)]
pub struct StorageAdapterReadScope<R> {
    read: R,
}

impl<R> StorageAdapterReadScope<R> {
    pub fn new(read: R) -> Self {
        Self { read }
    }

    fn into_inner(self) -> R {
        self.read
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
}

impl<R> SharedStorageAdapterRead<R>
where
    R: StorageRead,
{
    pub(crate) fn new(read: StorageAdapterReadScope<R>) -> Self {
        Self {
            read: Arc::new(read.into_inner()),
        }
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
        }
    }
}

/// Charges one adapter point-read batch to the active plan-load phase.
///
/// This is the single boundary every engine point read crosses, so counting
/// here answers "how many physical reads does one plan load issue" without
/// trusting a hand audit of the call tree.
#[cfg(feature = "root-replay-trace")]
async fn traced_get_many<F>(
    requests: &[GetManyRequest<'_>],
    future: F,
) -> Result<GetManyResult, StorageError>
where
    F: Future<Output = Result<GetManyResult, StorageError>>,
{
    let keys = requests
        .iter()
        .map(|request| request.keys.len() as u64)
        .sum::<u64>();
    let start = std::time::Instant::now();
    let result = future.await;
    let nanos = start.elapsed().as_nanos() as u64;
    let (hits, bytes) = match result.as_ref() {
        Ok(result) => result
            .values
            .iter()
            .flatten()
            .fold((0u64, 0u64), |(hits, bytes), value| match value {
                crate::storage::ProjectedValue::KeyOnly => (hits + 1, bytes),
                crate::storage::ProjectedValue::FullValue(payload) => {
                    (hits + 1, bytes + payload.len() as u64)
                }
            }),
        Err(_) => (0, 0),
    };
    crate::storage_bench::record_plan_load_io(nanos, requests.len() as u64, keys, hits, bytes);
    result
}

impl<R> StorageAdapterRead for StorageAdapterReadScope<R>
where
    R: StorageRead,
{
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.read.snapshot_cache_key()
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_checkpoint_point_read(
            requests.len(),
            requests.iter().map(|request| request.keys.len()).sum(),
        );
        #[cfg(feature = "root-replay-trace")]
        {
            traced_get_many(requests, self.read.get_many(requests))
        }
        #[cfg(not(feature = "root-replay-trace"))]
        {
            self.read.get_many(requests)
        }
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_checkpoint_scan_start();
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

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_checkpoint_point_read(
            requests.len(),
            requests.iter().map(|request| request.keys.len()).sum(),
        );
        #[cfg(feature = "root-replay-trace")]
        {
            traced_get_many(requests, self.read.get_many(requests))
        }
        #[cfg(not(feature = "root-replay-trace"))]
        {
            self.read.get_many(requests)
        }
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_checkpoint_scan_start();
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
