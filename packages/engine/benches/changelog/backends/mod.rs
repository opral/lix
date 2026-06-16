use std::sync::Arc;

use lix_engine::backend::{
    Backend, BackendError, BackendRead, BackendWrite, CommitResult, GetOptions, InMemoryBackend,
    InMemoryRead, InMemoryWrite, Key, KeyRange, PointVisitor, PutBatch, ReadOptions, ScanOptions,
    ScanResult, ScanVisitor, SpaceId, WriteOptions,
};
use tempfile::TempDir;

#[expect(dead_code)]
#[path = "../../../tests/backend/support/redb_backend.rs"]
mod redb_backend;
#[expect(dead_code)]
#[path = "../../../tests/backend/support/rocksdb_backend.rs"]
mod rocksdb_backend;
#[expect(dead_code)]
#[path = "../../../tests/backend/support/sqlite_backend.rs"]
mod sqlite_backend;

use redb_backend::{RedbBackend, RedbRead, RedbWrite};
use rocksdb_backend::{RocksDbBackend, RocksDbRead, RocksDbWrite};
use sqlite_backend::{SqliteBackend, SqliteRead, SqliteWrite};

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
    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        match self {
            Self::Unit(backend) => backend.begin_read(opts).map(ChangelogScoreRead::Unit),
            Self::Sqlite { backend, .. } => {
                backend.begin_read(opts).map(ChangelogScoreRead::Sqlite)
            }
            Self::RocksDb { backend, .. } => {
                backend.begin_read(opts).map(ChangelogScoreRead::RocksDb)
            }
            Self::Redb { backend, .. } => backend.begin_read(opts).map(ChangelogScoreRead::Redb),
        }
    }

    fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        match self {
            Self::Unit(backend) => backend.begin_write(opts).map(ChangelogScoreWrite::Unit),
            Self::Sqlite { backend, .. } => {
                backend.begin_write(opts).map(ChangelogScoreWrite::Sqlite)
            }
            Self::RocksDb { backend, .. } => {
                backend.begin_write(opts).map(ChangelogScoreWrite::RocksDb)
            }
            Self::Redb { backend, .. } => backend.begin_write(opts).map(ChangelogScoreWrite::Redb),
        }
    }
}

impl BackendRead for ChangelogScoreRead<'_> {
    fn visit_keys<V>(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        match self {
            Self::Unit(read) => read.visit_keys(space, keys, opts, visitor),
            Self::Sqlite(read) => read.visit_keys(space, keys, opts, visitor),
            Self::RocksDb(read) => read.visit_keys(space, keys, opts, visitor),
            Self::Redb(read) => read.visit_keys(space, keys, opts, visitor),
        }
    }

    fn scan<V>(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        match self {
            Self::Unit(read) => read.scan(space, range, opts, visitor),
            Self::Sqlite(read) => read.scan(space, range, opts, visitor),
            Self::RocksDb(read) => read.scan(space, range, opts, visitor),
            Self::Redb(read) => read.scan(space, range, opts, visitor),
        }
    }
}

impl BackendWrite for ChangelogScoreWrite {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        match self {
            Self::Unit(write) => write.put_many(space, entries),
            Self::Sqlite(write) => write.put_many(space, entries),
            Self::RocksDb(write) => write.put_many(space, entries),
            Self::Redb(write) => write.put_many(space, entries),
        }
    }

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        match self {
            Self::Unit(write) => write.delete_many(space, keys),
            Self::Sqlite(write) => write.delete_many(space, keys),
            Self::RocksDb(write) => write.delete_many(space, keys),
            Self::Redb(write) => write.delete_many(space, keys),
        }
    }

    fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), BackendError> {
        match self {
            Self::Unit(write) => write.delete_range(space, range),
            Self::Sqlite(write) => write.delete_range(space, range),
            Self::RocksDb(write) => write.delete_range(space, range),
            Self::Redb(write) => write.delete_range(space, range),
        }
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        match self {
            Self::Unit(write) => write.commit(),
            Self::Sqlite(write) => write.commit(),
            Self::RocksDb(write) => write.commit(),
            Self::Redb(write) => write.commit(),
        }
    }

    fn rollback(self) -> Result<(), BackendError> {
        match self {
            Self::Unit(write) => write.rollback(),
            Self::Sqlite(write) => write.rollback(),
            Self::RocksDb(write) => write.rollback(),
            Self::Redb(write) => write.rollback(),
        }
    }
}
