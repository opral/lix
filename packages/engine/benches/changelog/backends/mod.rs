use std::sync::Arc;

use lix_backends::{
    RedbBackend, RedbRead, RedbWrite, RocksDbBackend, RocksDbRead, RocksDbWrite, SqliteBackend,
    SqliteRead, SqliteWrite,
};
use lix_engine::Backend;
use lix_engine::backend::{
    BackendError, BackendRead, BackendWrite, CommitResult, GetManyResult, GetOptions,
    InMemoryBackend, InMemoryRead, InMemoryWrite, Key, KeyRange, PutBatch, ReadOptions, ScanChunk,
    ScanOptions, SpaceId, WriteOptions,
};
use tempfile::TempDir;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChangelogBenchBackend {
    Unit,
    SqliteTempfile,
    RocksDbTempdir,
    RedbTempfile,
}

#[derive(Clone)]
pub(crate) enum ChangelogScoreBackend {
    Unit(InMemoryBackend),
    Sqlite {
        backend: SqliteBackend,
        _temp_dir: Arc<TempDir>,
    },
    RocksDb {
        backend: RocksDbBackend,
        _temp_dir: Arc<TempDir>,
    },
    Redb {
        backend: RedbBackend,
        _temp_dir: Arc<TempDir>,
    },
}

pub(crate) enum ChangelogScoreRead<'a> {
    Unit(InMemoryRead),
    Sqlite(SqliteRead),
    RocksDb(RocksDbRead<'a>),
    Redb(RedbRead),
}

pub(crate) enum ChangelogScoreWrite {
    Unit(InMemoryWrite),
    Sqlite(SqliteWrite),
    RocksDb(RocksDbWrite),
    Redb(RedbWrite),
}

impl ChangelogBenchBackend {
    pub(crate) fn create(self) -> ChangelogScoreBackend {
        match self {
            Self::Unit => ChangelogScoreBackend::Unit(InMemoryBackend::new()),
            Self::SqliteTempfile => {
                let temp_dir = Arc::new(tempfile::tempdir().expect("create sqlite temp dir"));
                let path = temp_dir.path().join("changelog-scorecard.sqlite");
                ChangelogScoreBackend::Sqlite {
                    backend: SqliteBackend::open(path).expect("open sqlite scorecard backend"),
                    _temp_dir: temp_dir,
                }
            }
            Self::RocksDbTempdir => {
                let temp_dir = Arc::new(tempfile::tempdir().expect("create rocksdb temp dir"));
                let path = temp_dir.path().join("changelog-scorecard.rocksdb");
                ChangelogScoreBackend::RocksDb {
                    backend: RocksDbBackend::open(path).expect("open rocksdb scorecard backend"),
                    _temp_dir: temp_dir,
                }
            }
            Self::RedbTempfile => {
                let temp_dir = Arc::new(tempfile::tempdir().expect("create redb temp dir"));
                let path = temp_dir.path().join("changelog-scorecard.redb");
                ChangelogScoreBackend::Redb {
                    backend: RedbBackend::open(path).expect("open redb scorecard backend"),
                    _temp_dir: temp_dir,
                }
            }
        }
    }
}

impl Backend for ChangelogScoreBackend {
    type Read<'a>
        = ChangelogScoreRead<'a>
    where
        Self: 'a;

    type Write<'a>
        = ChangelogScoreWrite
    where
        Self: 'a;
    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        match self {
            Self::Unit(backend) => backend.begin_read(opts).await.map(ChangelogScoreRead::Unit),
            Self::Sqlite { backend, .. } => backend
                .begin_read(opts)
                .await
                .map(ChangelogScoreRead::Sqlite),
            Self::RocksDb { backend, .. } => backend
                .begin_read(opts)
                .await
                .map(ChangelogScoreRead::RocksDb),
            Self::Redb { backend, .. } => {
                backend.begin_read(opts).await.map(ChangelogScoreRead::Redb)
            }
        }
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        match self {
            Self::Unit(backend) => backend
                .begin_write(opts)
                .await
                .map(ChangelogScoreWrite::Unit),
            Self::Sqlite { backend, .. } => backend
                .begin_write(opts)
                .await
                .map(ChangelogScoreWrite::Sqlite),
            Self::RocksDb { backend, .. } => backend
                .begin_write(opts)
                .await
                .map(ChangelogScoreWrite::RocksDb),
            Self::Redb { backend, .. } => backend
                .begin_write(opts)
                .await
                .map(ChangelogScoreWrite::Redb),
        }
    }
}

impl BackendRead for ChangelogScoreRead<'_> {
    async fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> Result<GetManyResult, BackendError> {
        match self {
            Self::Unit(read) => read.get_many(space, keys, opts).await,
            Self::Sqlite(read) => read.get_many(space, keys, opts).await,
            Self::RocksDb(read) => read.get_many(space, keys, opts).await,
            Self::Redb(read) => read.get_many(space, keys, opts).await,
        }
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> Result<ScanChunk, BackendError> {
        match self {
            Self::Unit(read) => read.scan(space, range, opts).await,
            Self::Sqlite(read) => read.scan(space, range, opts).await,
            Self::RocksDb(read) => read.scan(space, range, opts).await,
            Self::Redb(read) => read.scan(space, range, opts).await,
        }
    }
}

impl BackendWrite for ChangelogScoreWrite {
    async fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        match self {
            Self::Unit(write) => write.put_many(space, entries).await,
            Self::Sqlite(write) => write.put_many(space, entries).await,
            Self::RocksDb(write) => write.put_many(space, entries).await,
            Self::Redb(write) => write.put_many(space, entries).await,
        }
    }

    async fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        match self {
            Self::Unit(write) => write.delete_many(space, keys).await,
            Self::Sqlite(write) => write.delete_many(space, keys).await,
            Self::RocksDb(write) => write.delete_many(space, keys).await,
            Self::Redb(write) => write.delete_many(space, keys).await,
        }
    }

    async fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), BackendError> {
        match self {
            Self::Unit(write) => write.delete_range(space, range).await,
            Self::Sqlite(write) => write.delete_range(space, range).await,
            Self::RocksDb(write) => write.delete_range(space, range).await,
            Self::Redb(write) => write.delete_range(space, range).await,
        }
    }

    async fn commit(self) -> Result<CommitResult, BackendError> {
        match self {
            Self::Unit(write) => write.commit().await,
            Self::Sqlite(write) => write.commit().await,
            Self::RocksDb(write) => write.commit().await,
            Self::Redb(write) => write.commit().await,
        }
    }

    async fn rollback(self) -> Result<(), BackendError> {
        match self {
            Self::Unit(write) => write.rollback().await,
            Self::Sqlite(write) => write.rollback().await,
            Self::RocksDb(write) => write.rollback().await,
            Self::Redb(write) => write.rollback().await,
        }
    }
}
