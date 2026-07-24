use std::sync::Arc;

use lix_engine::Storage;
use lix_engine::storage::{
    CommitResult, GetManyResult, GetOptions, Key, KeyRange, Memory, MemoryRead, MemoryWrite,
    PutBatch, ReadOptions, ScanChunk, ScanOptions, SpaceId, StorageError, StorageRead,
    StorageWrite, WriteOptions,
};
use lix_rocksdb_storage::{RocksDB, RocksDBRead, RocksDBWrite};
use lix_sqlite_storage::{SQLite, SQLiteRead, SQLiteWrite};
use tempfile::TempDir;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChangelogBenchStorage {
    Unit,
    SQLiteTempfile,
    RocksDBTempdir,
}

#[derive(Clone)]
pub(crate) enum ChangelogScoreStorage {
    Unit(Memory),
    SQLite {
        storage: SQLite,
        _temp_dir: Arc<TempDir>,
    },
    RocksDB {
        storage: RocksDB,
        _temp_dir: Arc<TempDir>,
    },
}

pub(crate) enum ChangelogScoreRead<'a> {
    Unit(MemoryRead),
    SQLite(SQLiteRead),
    RocksDB(RocksDBRead<'a>),
}

pub(crate) enum ChangelogScoreWrite {
    Unit(MemoryWrite),
    SQLite(SQLiteWrite),
    RocksDB(RocksDBWrite),
}

impl ChangelogBenchStorage {
    pub(crate) fn create(self) -> ChangelogScoreStorage {
        match self {
            Self::Unit => ChangelogScoreStorage::Unit(Memory::new()),
            Self::SQLiteTempfile => {
                let temp_dir = Arc::new(tempfile::tempdir().expect("create sqlite temp dir"));
                let path = temp_dir.path().join("changelog-scorecard.sqlite");
                ChangelogScoreStorage::SQLite {
                    storage: SQLite::open(path).expect("open sqlite scorecard storage"),
                    _temp_dir: temp_dir,
                }
            }
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
            Self::SQLite { storage, .. } => storage
                .begin_read(opts)
                .await
                .map(ChangelogScoreRead::SQLite),
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
            Self::SQLite { storage, .. } => storage
                .begin_write(opts)
                .await
                .map(ChangelogScoreWrite::SQLite),
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
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> Result<GetManyResult, StorageError> {
        match self {
            Self::Unit(read) => read.get_many(space, keys, opts).await,
            Self::SQLite(read) => read.get_many(space, keys, opts).await,
            Self::RocksDB(read) => read.get_many(space, keys, opts).await,
        }
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        match self {
            Self::Unit(read) => read.scan(space, range, opts).await,
            Self::SQLite(read) => read.scan(space, range, opts).await,
            Self::RocksDB(read) => read.scan(space, range, opts).await,
        }
    }
}

impl StorageWrite for ChangelogScoreWrite {
    async fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), StorageError> {
        match self {
            Self::Unit(write) => write.put_many(space, entries).await,
            Self::SQLite(write) => write.put_many(space, entries).await,
            Self::RocksDB(write) => write.put_many(space, entries).await,
        }
    }

    async fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), StorageError> {
        match self {
            Self::Unit(write) => write.delete_many(space, keys).await,
            Self::SQLite(write) => write.delete_many(space, keys).await,
            Self::RocksDB(write) => write.delete_many(space, keys).await,
        }
    }

    async fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), StorageError> {
        match self {
            Self::Unit(write) => write.delete_range(space, range).await,
            Self::SQLite(write) => write.delete_range(space, range).await,
            Self::RocksDB(write) => write.delete_range(space, range).await,
        }
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        match self {
            Self::Unit(write) => write.commit().await,
            Self::SQLite(write) => write.commit().await,
            Self::RocksDB(write) => write.commit().await,
        }
    }

    async fn rollback(self) -> Result<(), StorageError> {
        match self {
            Self::Unit(write) => write.rollback().await,
            Self::SQLite(write) => write.rollback().await,
            Self::RocksDB(write) => write.rollback().await,
        }
    }
}
