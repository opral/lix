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

#[derive(Clone, Debug)]
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
        self.read.get_many(requests)
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

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        self.read.get_many(requests)
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
