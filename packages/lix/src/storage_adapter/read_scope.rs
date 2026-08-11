use std::sync::Arc;

use crate::storage::{
    BeginScanOptions, GetManyRequest, GetManyResult, KeyRange, ScanCursor, StorageError,
    StorageRead, StorageSpace,
};
use crate::storage_adapter::schema_intern::{SchemaIntern, SchemaInternHandle};

/// The async read capability consumed by engine stores.
///
/// Implementations preserve one coherent storage read view while allowing
/// independent point and scan requests to overlap.
pub trait StorageAdapterRead: Send + Sync {
    fn snapshot_cache_key(&self) -> Option<u128> {
        None
    }

    /// The hot-plane schema-key interning table coherent with this read view.
    ///
    /// Wrappers forward to their inner read; the adapter-created read scope is
    /// the one implementation that owns the handle.
    fn schema_intern(&self) -> &Arc<SchemaInternHandle>;

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

/// Resolves the intern table from any adapter read.
pub(crate) fn schema_intern_of<S>(store: &S) -> &SchemaIntern
where
    S: StorageAdapterRead + ?Sized,
{
    store.schema_intern().intern()
}

#[derive(Debug)]
pub struct StorageAdapterReadScope<R> {
    read: R,
    schema_intern: Arc<SchemaInternHandle>,
}

impl<R> StorageAdapterReadScope<R> {
    /// Standalone scope with a fresh, empty intern table. Only correct over
    /// storage whose hot plane is empty or written through this same scope's
    /// intern; engine paths must construct scopes via `StorageAdapter` so the
    /// persisted table is loaded first.
    pub fn new(read: R) -> Self {
        Self {
            read,
            schema_intern: Arc::default(),
        }
    }

    pub(crate) fn new_with_intern(read: R, schema_intern: Arc<SchemaInternHandle>) -> Self {
        Self {
            read,
            schema_intern,
        }
    }

    /// Test-only scope that shares an adapter's intern table, so keys staged
    /// through the adapter decode through instrumented reads.
    #[cfg(test)]
    pub(crate) fn new_with_intern_for_test<StorageImpl>(
        adapter: &crate::storage_adapter::StorageAdapter<StorageImpl>,
        read: R,
    ) -> Self
    where
        StorageImpl: crate::storage::Storage,
    {
        Self::new_with_intern(read, Arc::clone(adapter.schema_intern_handle()))
    }

    fn into_inner(self) -> (R, Arc<SchemaInternHandle>) {
        (self.read, self.schema_intern)
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
    schema_intern: Arc<SchemaInternHandle>,
}

impl<R> SharedStorageAdapterRead<R>
where
    R: StorageRead,
{
    pub(crate) fn new(read: StorageAdapterReadScope<R>) -> Self {
        let (read, schema_intern) = read.into_inner();
        Self {
            read: Arc::new(read),
            schema_intern,
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
            schema_intern: Arc::clone(&self.schema_intern),
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

    fn schema_intern(&self) -> &Arc<SchemaInternHandle> {
        &self.schema_intern
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

    fn schema_intern(&self) -> &Arc<SchemaInternHandle> {
        &self.schema_intern
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

    fn schema_intern(&self) -> &Arc<SchemaInternHandle> {
        (*self).schema_intern()
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

    fn schema_intern(&self) -> &Arc<SchemaInternHandle> {
        (**self).schema_intern()
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
