use std::sync::Arc;

use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, Memory,
    MemoryRead, MemoryWrite, PutBatch, ReadOptions, ScanCursor, Storage, StorageError, StorageRead,
    StorageSpace, StorageWrite, WriteOptions,
};
use lix_storage_rocksdb::{RocksDB, RocksDBRead, RocksDBWrite};
use tempfile::TempDir;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChangelogBenchStorage {
    Unit,
    RocksDBTempdir,
}

#[derive(Clone)]
pub(crate) enum ChangelogScoreStorage {
    Unit(Memory),
    RocksDB {
        storage: RocksDB,
        _temp_dir: Arc<TempDir>,
    },
}

pub(crate) enum ChangelogScoreRead<'a> {
    Unit(MemoryRead),
    RocksDB(RocksDBRead<'a>),
}

pub(crate) enum ChangelogScoreWrite {
    Unit(MemoryWrite),
    RocksDB(RocksDBWrite),
}

impl ChangelogBenchStorage {
    pub(crate) fn create(self) -> ChangelogScoreStorage {
        match self {
            Self::Unit => ChangelogScoreStorage::Unit(Memory::new()),
            Self::RocksDBTempdir => {
                let temp_dir = Arc::new(tempfile::tempdir().expect("create rocksdb temp dir"));
                let path = temp_dir.path().join("changelog-scorecard.rocksdb");
                ChangelogScoreStorage::RocksDB {
                    storage: RocksDB::open(path).expect("open rocksdb scorecard storage"),
                    _temp_dir: temp_dir,
                }
            }
        }
    }
}

impl Storage for ChangelogScoreStorage {
    type Read<'a>
        = ChangelogScoreRead<'a>
    where
        Self: 'a;

    type Write<'a>
        = ChangelogScoreWrite
    where
        Self: 'a;
    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        match self {
            Self::Unit(storage) => storage.begin_read(opts).await.map(ChangelogScoreRead::Unit),
            Self::RocksDB { storage, .. } => storage
                .begin_read(opts)
                .await
                .map(ChangelogScoreRead::RocksDB),
        }
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        match self {
            Self::Unit(storage) => storage
                .begin_write(opts)
                .await
                .map(ChangelogScoreWrite::Unit),
            Self::RocksDB { storage, .. } => storage
                .begin_write(opts)
                .await
                .map(ChangelogScoreWrite::RocksDB),
        }
    }
}

impl StorageRead for ChangelogScoreRead<'_> {
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        match self {
            Self::Unit(read) => read.get_many(requests).await,
            Self::RocksDB(read) => read.get_many(requests).await,
        }
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        match self {
            Self::Unit(read) => read.begin_scan(space, range, opts).await,
            Self::RocksDB(read) => read.begin_scan(space, range, opts).await,
        }
    }
}

impl StorageWrite for ChangelogScoreWrite {
    async fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        match self {
            Self::Unit(write) => write.put_many(space, entries).await,
            Self::RocksDB(write) => write.put_many(space, entries).await,
        }
    }

    async fn replace_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        match self {
            Self::Unit(write) => write.replace_many(space, entries).await,
            Self::RocksDB(write) => write.replace_many(space, entries).await,
        }
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        match self {
            Self::Unit(write) => write.delete_many(space, keys).await,
            Self::RocksDB(write) => write.delete_many(space, keys).await,
        }
    }

    async fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        match self {
            Self::Unit(write) => write.delete_range(space, range).await,
            Self::RocksDB(write) => write.delete_range(space, range).await,
        }
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        match self {
            Self::Unit(write) => write.commit().await,
            Self::RocksDB(write) => write.commit().await,
        }
    }

    async fn rollback(self) -> Result<(), StorageError> {
        match self {
            Self::Unit(write) => write.rollback().await,
            Self::RocksDB(write) => write.rollback().await,
        }
    }
}
