use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, Memory,
    MemoryRead, MemoryWrite, PutBatch, ReadOptions, ScanCursor, Storage, StorageChangeWatch,
    StorageError, StorageRead, StorageSpace, StorageWrite, WriteOptions,
};

use crate::js_storage::{JsStorage, JsStorageRead, JsStorageWrite};

#[derive(Clone, Debug)]
pub enum BrowserStorage {
    Memory(Memory),
    Js(JsStorage),
}

pub enum BrowserRead {
    Memory(MemoryRead),
    Js(JsStorageRead),
}

pub enum BrowserWrite {
    Memory(MemoryWrite),
    Js(JsStorageWrite),
}

impl BrowserStorage {
    pub async fn close(&self) -> Result<(), StorageError> {
        match self {
            Self::Memory(_) => Ok(()),
            Self::Js(storage) => storage.close().await,
        }
    }
}

impl Storage for BrowserStorage {
    type Read<'a>
        = BrowserRead
    where
        Self: 'a;
    type Write<'a>
        = BrowserWrite
    where
        Self: 'a;

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        match self {
            Self::Memory(storage) => storage.begin_read(opts).await.map(BrowserRead::Memory),
            Self::Js(storage) => storage.begin_read(opts).await.map(BrowserRead::Js),
        }
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        match self {
            Self::Memory(storage) => storage.begin_write(opts).await.map(BrowserWrite::Memory),
            Self::Js(storage) => storage.begin_write(opts).await.map(BrowserWrite::Js),
        }
    }

    async fn watch_for_changes(&self) -> Result<StorageChangeWatch, StorageError> {
        match self {
            Self::Memory(storage) => storage.watch_for_changes().await,
            Self::Js(storage) => storage.watch_for_changes().await,
        }
    }
}

impl StorageRead for BrowserRead {
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        match self {
            Self::Memory(read) => read.get_many(requests).await,
            Self::Js(read) => read.get_many(requests).await,
        }
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        match self {
            Self::Memory(read) => read.begin_scan(space, range, opts).await,
            Self::Js(read) => read.begin_scan(space, range, opts).await,
        }
    }
}

impl StorageWrite for BrowserWrite {
    async fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        match self {
            Self::Memory(write) => write.put_many(space, entries).await,
            Self::Js(write) => write.put_many(space, entries).await,
        }
    }

    async fn replace_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        match self {
            Self::Memory(write) => write.replace_many(space, entries).await,
            Self::Js(write) => write.replace_many(space, entries).await,
        }
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        match self {
            Self::Memory(write) => write.delete_many(space, keys).await,
            Self::Js(write) => write.delete_many(space, keys).await,
        }
    }

    async fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        match self {
            Self::Memory(write) => write.delete_range(space, range).await,
            Self::Js(write) => write.delete_range(space, range).await,
        }
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        match self {
            Self::Memory(write) => write.commit().await,
            Self::Js(write) => write.commit().await,
        }
    }

    async fn rollback(self) -> Result<(), StorageError> {
        match self {
            Self::Memory(write) => write.rollback().await,
            Self::Js(write) => write.rollback().await,
        }
    }
}
