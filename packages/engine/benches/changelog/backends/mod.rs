use std::sync::Arc;

use lix_engine::backend::{
    Backend, BackendCapabilities, BackendError, BackendRangeScan, BackendRead, BackendWrite,
    BufferedRangeScan, CommitResult, GetOptions, InMemoryBackend, InMemoryRead, InMemoryWrite, Key,
    KeyRange, KeyRef, PointVisitor, ProjectedValueRef, PutBatch, ReadEntry, ReadOptions,
    ScanOptions, ScanVisitor, WriteOptions,
};
use tempfile::TempDir;

#[allow(dead_code)]
#[path = "../../../tests/backend/support/redb_backend.rs"]
mod redb_backend;
#[allow(dead_code)]
#[path = "../../../tests/backend/support/rocksdb_backend.rs"]
mod rocksdb_backend;
#[allow(dead_code)]
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
    pub(crate) const CI: [Self; 4] = [
        Self::Unit,
        Self::SqliteTempfile,
        Self::RocksDbTempdir,
        Self::RedbTempfile,
    ];

    #[allow(dead_code)]
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Unit => "mem_unit",
            Self::SqliteTempfile => "sqlite_tempfile",
            Self::RocksDbTempdir => "rocksdb_tempdir",
            Self::RedbTempfile => "redb_tempfile",
        }
    }

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

    fn capabilities(&self) -> BackendCapabilities {
        match self {
            Self::Unit(backend) => backend.capabilities(),
            Self::Sqlite { backend, .. } => backend.capabilities(),
            Self::RocksDb { backend, .. } => backend.capabilities(),
            Self::Redb { backend, .. } => backend.capabilities(),
        }
    }

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
    type RangeScan<'cursor> = BufferedRangeScan;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        match self {
            Self::Unit(read) => read.visit_keys(keys, opts, visitor),
            Self::Sqlite(read) => read.visit_keys(keys, opts, visitor),
            Self::RocksDb(read) => read.visit_keys(keys, opts, visitor),
            Self::Redb(read) => read.visit_keys(keys, opts, visitor),
        }
    }

    fn with_range_scan<T, F>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
    {
        fn buffer_scan<C>(
            cursor: &mut C,
            limit_rows: usize,
        ) -> Result<BufferedRangeScan, BackendError>
        where
            C: BackendRangeScan,
        {
            struct Collector {
                rows: Vec<ReadEntry>,
            }

            impl ScanVisitor for Collector {
                fn visit(
                    &mut self,
                    key: KeyRef<'_>,
                    value: ProjectedValueRef<'_>,
                ) -> Result<(), BackendError> {
                    self.rows.push(ReadEntry {
                        key: key.to_owned_key(),
                        value: value.to_owned(),
                    });
                    Ok(())
                }
            }

            let mut visitor = Collector { rows: Vec::new() };
            let collect_limit = limit_rows.saturating_add(1);
            cursor.visit_next(collect_limit, &mut visitor)?;
            Ok(BufferedRangeScan::new(visitor.rows))
        }

        match self {
            Self::Unit(read) => read.with_range_scan(range, opts, |scan| {
                let mut buffered = buffer_scan(scan, opts.limit_rows)?;
                f(&mut buffered)
            }),
            Self::Sqlite(read) => read.with_range_scan(range, opts, |scan| {
                let mut buffered = buffer_scan(scan, opts.limit_rows)?;
                f(&mut buffered)
            }),
            Self::RocksDb(read) => read.with_range_scan(range, opts, |scan| {
                let mut buffered = buffer_scan(scan, opts.limit_rows)?;
                f(&mut buffered)
            }),
            Self::Redb(read) => read.with_range_scan(range, opts, |scan| {
                let mut buffered = buffer_scan(scan, opts.limit_rows)?;
                f(&mut buffered)
            }),
        }
    }
}

impl BackendWrite for ChangelogScoreWrite {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        match self {
            Self::Unit(write) => write.put_many(entries),
            Self::Sqlite(write) => write.put_many(entries),
            Self::RocksDb(write) => write.put_many(entries),
            Self::Redb(write) => write.put_many(entries),
        }
    }

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError> {
        match self {
            Self::Unit(write) => write.delete_many(keys),
            Self::Sqlite(write) => write.delete_many(keys),
            Self::RocksDb(write) => write.delete_many(keys),
            Self::Redb(write) => write.delete_many(keys),
        }
    }

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
        match self {
            Self::Unit(write) => write.delete_range(range),
            Self::Sqlite(write) => write.delete_range(range),
            Self::RocksDb(write) => write.delete_range(range),
            Self::Redb(write) => write.delete_range(range),
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
