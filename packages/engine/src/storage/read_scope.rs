use std::sync::Arc;

use crate::backend::{
    BackendError, BackendRead, GetManyResult, GetOptions, Key, KeyRange, ScanChunk, ScanOptions,
    SpaceId,
};

/// The async read capability consumed by engine stores.
///
/// Implementations preserve one coherent backend read view while allowing
/// independent point and scan requests to overlap.
pub trait StorageRead: Send + Sync {
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> impl Future<Output = Result<GetManyResult, BackendError>> + Send;

    fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> impl Future<Output = Result<ScanChunk, BackendError>> + Send;
}

#[derive(Debug)]
pub struct StorageReadScope<R> {
    read: R,
}

impl<R> StorageReadScope<R> {
    pub fn new(read: R) -> Self {
        Self { read }
    }

    fn into_inner(self) -> R {
        self.read
    }
}

/// Cloneable SQL/DataFusion bridge for one execution-scoped backend read.
///
/// Clones share the read handle directly. Concurrency and synchronization are
/// backend responsibilities; this layer never serializes requests.
pub(crate) struct SharedStorageRead<R>
where
    R: BackendRead,
{
    read: Arc<R>,
}

impl<R> SharedStorageRead<R>
where
    R: BackendRead,
{
    pub(crate) fn new(read: StorageReadScope<R>) -> Self {
        Self {
            read: Arc::new(read.into_inner()),
        }
    }

    pub(crate) fn finish(self) -> Result<(), BackendError> {
        let read = Arc::try_unwrap(self.read).map_err(|read| {
            BackendError::Io(format!(
                "shared storage read still has {} active handles",
                Arc::strong_count(&read) - 1
            ))
        })?;
        drop(read);
        Ok(())
    }
}

impl<R> Clone for SharedStorageRead<R>
where
    R: BackendRead,
{
    fn clone(&self) -> Self {
        Self {
            read: Arc::clone(&self.read),
        }
    }
}

impl<R> StorageRead for StorageReadScope<R>
where
    R: BackendRead,
{
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> impl Future<Output = Result<GetManyResult, BackendError>> + Send {
        self.read.get_many(space, keys, opts)
    }

    fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> impl Future<Output = Result<ScanChunk, BackendError>> + Send {
        self.read.scan(space, range, opts)
    }
}

impl<R> StorageRead for SharedStorageRead<R>
where
    R: BackendRead,
{
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> impl Future<Output = Result<GetManyResult, BackendError>> + Send {
        self.read.get_many(space, keys, opts)
    }

    fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> impl Future<Output = Result<ScanChunk, BackendError>> + Send {
        self.read.scan(space, range, opts)
    }
}

impl<T> StorageRead for &T
where
    T: StorageRead + ?Sized,
{
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> impl Future<Output = Result<GetManyResult, BackendError>> + Send {
        (*self).get_many(space, keys, opts)
    }

    fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> impl Future<Output = Result<ScanChunk, BackendError>> + Send {
        (*self).scan(space, range, opts)
    }
}

impl<T> StorageRead for &mut T
where
    T: StorageRead + ?Sized,
{
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> impl Future<Output = Result<GetManyResult, BackendError>> + Send {
        (**self).get_many(space, keys, opts)
    }

    fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> impl Future<Output = Result<ScanChunk, BackendError>> + Send {
        (**self).scan(space, range, opts)
    }
}
