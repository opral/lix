use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use lix_engine::backend::{
    Backend, BackendCapabilities, BackendError, BackendRangeScan, BackendRead, BackendWrite,
    CommitResult, CoreProjection, DurableWriteLock, GetOptions, Key, KeyRange, KeyRef,
    PointVisitor, ProjectedValueRef, PutBatch, ReadOptions, ScanOptions, ScanResult, ScanVisitor,
    StoredValue, WriteConcurrency, WriteOptions, WriteStats,
};
use lix_engine::{BackendFactory, BackendFixture, BackendTestConfig};
use rocksdb::{DBIteratorWithThreadMode, Snapshot};
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
    durable_write_lock: DurableWriteLock,
}

pub struct RocksDbRead<'a> {
    snapshot: Snapshot<'a>,
}

pub struct RocksDbRangeScan<'a> {
    iter: DBIteratorWithThreadMode<'a, DB>,
    bounds: EncodedBounds,
    projection: CoreProjection,
    pending: Option<(Box<[u8]>, Box<[u8]>)>,
    done: bool,
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

impl BackendFactory for RocksDbBackendFactory {
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

    fn config(&self) -> BackendTestConfig {
        BackendTestConfig {
            ephemeral: false,
            supports_concurrent_writers: false,
            ..BackendTestConfig::default()
        }
    }
}

impl BackendFixture for RocksDbBackendFixture {
    type Backend = RocksDbBackend;

    fn open(&self) -> Self::Backend {
        RocksDbBackend::open(&self.path).expect("open rocksdb backend")
    }
}

impl RocksDbBackend {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, BackendError> {
        let path = path.into();
        let durable_write_lock = durable_write_lock_for_path(&path);
        let db = Arc::new(open_rocksdb(&path)?);
        Ok(Self {
            path,
            db,
            durable_write_lock,
        })
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

    fn durable_write_lock(&self) -> DurableWriteLock {
        self.durable_write_lock.clone()
    }
}

fn durable_write_lock_for_path(path: &Path) -> DurableWriteLock {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, DurableWriteLock>>> = OnceLock::new();
    let key = canonical_lock_key(path);
    let locks = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks
        .lock()
        .expect("rocksdb durable write lock registry should not poison");
    if let Some(lock) = locks.get(&key) {
        return lock.clone();
    }
    let lock = DurableWriteLock::new();
    locks.insert(key, lock.clone());
    lock
}

fn canonical_lock_key(path: &Path) -> PathBuf {
    if let Ok(path) = path.canonicalize() {
        return path;
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .expect("current directory should be available")
            .join(path)
    };
    let Some(parent) = absolute.parent() else {
        return absolute;
    };
    let Ok(parent) = parent.canonicalize() else {
        return absolute;
    };
    match absolute.file_name() {
        Some(file_name) => parent.join(file_name),
        None => parent,
    }
}

impl<'db> BackendRead for RocksDbRead<'db> {
    type RangeScan<'cursor> = RocksDbRangeScan<'db>;

    fn visit_keys<V>(
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

    fn with_range_scan<T, F>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
    {
        let bounds = EncodedBounds::new(range, opts.resume_after);
        let mut cursor = RocksDbRangeScan {
            iter: self
                .snapshot
                .iterator(IteratorMode::From(&bounds.lower_seek, Direction::Forward)),
            bounds,
            projection: opts.projection,
            pending: None,
            done: opts.limit_rows == 0,
        };
        f(&mut cursor)
    }
}

impl BackendRangeScan for RocksDbRangeScan<'_> {
    fn visit_next<V>(
        &mut self,
        limit_rows: usize,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        if limit_rows == 0 || self.done {
            return Ok(ScanResult {
                emitted: 0,
                has_more: !self.done,
            });
        }

        let mut emitted = 0;
        while emitted < limit_rows {
            let Some((encoded_key, value)) = self.next_row()? else {
                return Ok(ScanResult {
                    emitted,
                    has_more: false,
                });
            };

            match self.projection {
                CoreProjection::KeyOnly => {
                    visitor.visit(KeyRef(encoded_key.as_ref()), ProjectedValueRef::KeyOnly)?
                }
                CoreProjection::FullValue => visitor.visit(
                    KeyRef(encoded_key.as_ref()),
                    ProjectedValueRef::FullValue(value.as_ref()),
                )?,
            }
            emitted += 1;
        }

        let has_more = self.ensure_pending()?;
        Ok(ScanResult { emitted, has_more })
    }
}

impl RocksDbRangeScan<'_> {
    fn next_row(&mut self) -> Result<Option<(Box<[u8]>, Box<[u8]>)>, BackendError> {
        if let Some(pending) = self.pending.take() {
            return Ok(Some(pending));
        }
        self.read_next_row()
    }

    fn ensure_pending(&mut self) -> Result<bool, BackendError> {
        if self.pending.is_some() {
            return Ok(true);
        }
        self.pending = self.read_next_row()?;
        Ok(self.pending.is_some())
    }

    fn read_next_row(&mut self) -> Result<Option<(Box<[u8]>, Box<[u8]>)>, BackendError> {
        if self.done {
            return Ok(None);
        }

        for item in self.iter.by_ref() {
            let (encoded_key, value) = item.map_err(rocksdb_error)?;
            let key = encoded_key.as_ref();
            if !self.bounds.after_lower(key) {
                continue;
            }
            if !self.bounds.before_upper(key) {
                self.done = true;
                return Ok(None);
            }
            return Ok(Some((encoded_key, value)));
        }

        self.done = true;
        Ok(None)
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
        let range_lower = match range.lower {
            Bound::Included(key) => Bound::Included(key.0.to_vec()),
            Bound::Excluded(key) => Bound::Excluded(key.0.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let lower = match resume_after {
            Some(resume_after) => {
                max_lower_bound(range_lower, Bound::Excluded(resume_after.0.to_vec()))
            }
            None => range_lower,
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

fn max_lower_bound(left: Bound<Vec<u8>>, right: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (left, right) {
        (Bound::Unbounded, bound) | (bound, Bound::Unbounded) => bound,
        (Bound::Included(left), Bound::Included(right)) => {
            Bound::Included(if left >= right { left } else { right })
        }
        (Bound::Included(left), Bound::Excluded(right)) => {
            if left > right {
                Bound::Included(left)
            } else {
                Bound::Excluded(right)
            }
        }
        (Bound::Excluded(left), Bound::Included(right)) => {
            if left >= right {
                Bound::Excluded(left)
            } else {
                Bound::Included(right)
            }
        }
        (Bound::Excluded(left), Bound::Excluded(right)) => {
            Bound::Excluded(if left >= right { left } else { right })
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
