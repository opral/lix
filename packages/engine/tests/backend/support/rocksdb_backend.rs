use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use lix_engine::backend_v2::{
    Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, CommitResult,
    CoreProjection, GetOptions, Key, KeyRange, KeyRef, PointVisitor, ProjectedValueRef, PutBatch,
    ReadOptions, ScanOptions, ScanResult, ScanVisitor, StoredValue, WriteConcurrency, WriteOptions,
    WriteStats,
};
use lix_engine::{BackendV2Factory, BackendV2Fixture, BackendV2TestConfig};
use rocksdb::Snapshot;
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, DB};
use tempfile::TempDir;

#[derive(Debug)]
pub struct RocksDbBackendFactory {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct RocksDbBackendFixture {
    path: PathBuf,
}

#[derive(Clone)]
pub struct RocksDbBackend {
    path: PathBuf,
    db: Arc<DB>,
}

pub struct RocksDbRead<'a> {
    snapshot: Snapshot<'a>,
}

pub struct RocksDbWrite {
    db: Arc<DB>,
    batch: WriteBatch,
    staged_put_keys: Vec<Key>,
    stats: WriteStats,
}

impl RocksDbBackendFactory {
    pub fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create rocksdb backend temp dir"),
            next_database_id: AtomicU64::new(0),
        }
    }
}

impl BackendV2Factory for RocksDbBackendFactory {
    type Backend = RocksDbBackend;
    type Fixture = RocksDbBackendFixture;

    fn create_fixture(&self) -> Self::Fixture {
        let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        let path = self
            .temp_dir
            .path()
            .join(format!("backend-{database_id}.rocksdb"));
        RocksDbBackendFixture { path }
    }

    fn config(&self) -> BackendV2TestConfig {
        BackendV2TestConfig {
            ephemeral: false,
            supports_concurrent_writers: false,
            ..BackendV2TestConfig::default()
        }
    }
}

impl BackendV2Fixture for RocksDbBackendFixture {
    type Backend = RocksDbBackend;

    fn open(&self) -> Self::Backend {
        RocksDbBackend::open(&self.path).expect("open rocksdb backend")
    }
}

impl RocksDbBackend {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, BackendError> {
        let path = path.into();
        let db = Arc::new(open_rocksdb(&path)?);
        Ok(Self { path, db })
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[allow(dead_code)]
    pub fn flush(&self) -> Result<(), BackendError> {
        self.db.flush().map_err(rocksdb_error)
    }
}

impl Backend for RocksDbBackend {
    type Read<'a>
        = RocksDbRead<'a>
    where
        Self: 'a;

    type Write<'a>
        = RocksDbWrite
    where
        Self: 'a;

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::v0(WriteConcurrency::SingleWriter)
    }

    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(RocksDbRead {
            snapshot: self.db.snapshot(),
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(RocksDbWrite {
            db: Arc::clone(&self.db),
            batch: WriteBatch::default(),
            staged_put_keys: Vec::new(),
            stats: WriteStats::default(),
        })
    }
}

impl BackendRead for RocksDbRead<'_> {
    fn visit_many<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        for (index, (key, value)) in keys
            .iter()
            .zip(
                self.snapshot
                    .multi_get(keys.iter().map(|key| key.0.as_ref()))
                    .into_iter(),
            )
            .enumerate()
        {
            let value = value.map_err(rocksdb_error)?;
            visitor.visit(
                index,
                key,
                value
                    .as_ref()
                    .map(|value| project_value_ref(value.as_ref(), opts.projection)),
            )?;
        }
        Ok(())
    }

    fn visit_range<V>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        if opts.limit_rows == 0 {
            return Ok(ScanResult::default());
        }

        let bounds = EncodedBounds::new(range, opts.resume_after);
        let mut emitted = 0;
        for item in self
            .snapshot
            .iterator(IteratorMode::From(&bounds.lower_seek, Direction::Forward))
        {
            let (encoded_key, value) = item.map_err(rocksdb_error)?;
            let encoded_key = encoded_key.as_ref();
            if !bounds.after_lower(encoded_key) {
                continue;
            }
            if !bounds.before_upper(encoded_key) {
                break;
            }
            if emitted == opts.limit_rows {
                return Ok(ScanResult {
                    emitted,
                    has_more: true,
                });
            }

            match opts.projection {
                CoreProjection::KeyOnly => {
                    visitor.visit(KeyRef(encoded_key), ProjectedValueRef::KeyOnly)?
                }
                CoreProjection::FullValue => {
                    visitor.visit(
                        KeyRef(encoded_key),
                        ProjectedValueRef::FullValue(value.as_ref()),
                    )?;
                }
            }
            emitted += 1;
        }

        Ok(ScanResult {
            emitted,
            has_more: false,
        })
    }
}

impl BackendWrite for RocksDbWrite {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        for entry in entries.entries {
            let value = stored_value_bytes(entry.value);
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            self.staged_put_keys.push(entry.key.clone());
            self.batch.put(entry.key.0.as_ref(), value.as_ref());
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError> {
        for key in keys {
            self.batch.delete(key.0.as_ref());
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
        if let Some((lower, upper)) = rocksdb_delete_range_bounds(&range) {
            self.batch.delete_range(lower.as_slice(), upper.as_slice());
        } else {
            let bounds = EncodedBounds::new(range, None);
            for item in self
                .db
                .iterator(IteratorMode::From(&bounds.lower_seek, Direction::Forward))
            {
                let (encoded_key, _value) = item.map_err(rocksdb_error)?;
                let encoded_key = encoded_key.as_ref();
                if !bounds.after_lower(encoded_key) {
                    continue;
                }
                if !bounds.before_upper(encoded_key) {
                    break;
                }
                self.batch.delete(encoded_key);
            }

            for key in &self.staged_put_keys {
                if bounds.contains(key.0.as_ref()) {
                    self.batch.delete(key.0.as_ref());
                }
            }
        }

        self.stats.deleted_ranges += 1;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        self.db.write(self.batch).map_err(rocksdb_error)?;
        Ok(CommitResult {
            commit_id: None,
            stats: self.stats,
        })
    }

    fn rollback(self) -> Result<(), BackendError> {
        Ok(())
    }
}

struct EncodedBounds {
    lower_seek: Vec<u8>,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
}

impl EncodedBounds {
    fn new(range: KeyRange, resume_after: Option<&Key>) -> Self {
        let lower = match (range.lower, resume_after) {
            (_, Some(resume_after)) => Bound::Excluded(resume_after.0.to_vec()),
            (Bound::Included(key), None) => Bound::Included(key.0.to_vec()),
            (Bound::Excluded(key), None) => Bound::Excluded(key.0.to_vec()),
            (Bound::Unbounded, None) => Bound::Unbounded,
        };

        let upper = match range.upper {
            Bound::Included(key) => Bound::Included(key.0.to_vec()),
            Bound::Excluded(key) => Bound::Excluded(key.0.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let lower_seek = match &lower {
            Bound::Included(key) | Bound::Excluded(key) => key.clone(),
            Bound::Unbounded => Vec::new(),
        };

        Self {
            lower_seek,
            lower,
            upper,
        }
    }

    fn after_lower(&self, encoded_key: &[u8]) -> bool {
        match &self.lower {
            Bound::Included(lower) if encoded_key < lower.as_slice() => false,
            Bound::Excluded(lower) if encoded_key <= lower.as_slice() => false,
            _ => true,
        }
    }

    fn before_upper(&self, encoded_key: &[u8]) -> bool {
        match &self.upper {
            Bound::Included(upper) => encoded_key <= upper.as_slice(),
            Bound::Excluded(upper) => encoded_key < upper.as_slice(),
            Bound::Unbounded => true,
        }
    }

    #[allow(dead_code)]
    fn contains(&self, encoded_key: &[u8]) -> bool {
        if !self.after_lower(encoded_key) {
            return false;
        }
        match &self.upper {
            Bound::Included(upper) => encoded_key <= upper.as_slice(),
            Bound::Excluded(upper) => encoded_key < upper.as_slice(),
            Bound::Unbounded => true,
        }
    }
}

fn rocksdb_delete_range_bounds(range: &KeyRange) -> Option<(Vec<u8>, Vec<u8>)> {
    let lower = match &range.lower {
        Bound::Included(key) => key.0.to_vec(),
        Bound::Excluded(key) => next_lexicographic_key(key)?,
        Bound::Unbounded => Vec::new(),
    };
    let upper = match &range.upper {
        Bound::Included(key) => next_lexicographic_key(key)?,
        Bound::Excluded(key) => key.0.to_vec(),
        Bound::Unbounded => return None,
    };

    if lower >= upper {
        None
    } else {
        Some((lower, upper))
    }
}

fn next_lexicographic_key(key: &Key) -> Option<Vec<u8>> {
    let mut bytes = key.0.to_vec();
    bytes.push(0);
    Some(bytes)
}

fn open_rocksdb(path: &Path) -> Result<DB, BackendError> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_use_fsync(false);
    options.set_write_buffer_size(64 * 1024 * 1024);
    DB::open(&options, path).map_err(rocksdb_error)
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}

fn project_value_ref(value: &[u8], projection: CoreProjection) -> ProjectedValueRef<'_> {
    match projection {
        CoreProjection::KeyOnly => ProjectedValueRef::KeyOnly,
        CoreProjection::FullValue => ProjectedValueRef::FullValue(value),
    }
}

fn rocksdb_error(error: rocksdb::Error) -> BackendError {
    BackendError::Io(format!("rocksdb backend: {error}"))
}
