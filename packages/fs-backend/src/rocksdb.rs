use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};

use bytes::Bytes;
use lix_engine::backend::{
    Backend, BackendError, BackendRead, BackendWrite, CommitResult, CoreProjection, GetOptions,
    Key, KeyRange, KeyRef, PointVisitor, ProjectedValueRef, PutBatch, ReadOptions, ScanOptions,
    ScanResult, ScanVisitor, SpaceId, StoredValue, WriteOptions, WriteStats,
};
use rocksdb::Snapshot;
use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch};

const DEFAULT_BLOB_MIN_SIZE: u64 = 32 * 1024;
const DEFAULT_BLOB_FILE_SIZE: u64 = 256 * 1024 * 1024;
const DEFAULT_BLOB_GC_AGE_CUTOFF: f64 = 0.25;

#[derive(Clone, Debug, PartialEq)]
pub struct RocksDbFilesystemBackendOptions {
    pub path: PathBuf,
    pub blob: RocksDbBlobOptions,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RocksDbBlobOptions {
    Disabled,
    Enabled {
        min_blob_size: u64,
        blob_file_size: u64,
        enable_gc: bool,
        gc_age_cutoff: f64,
    },
}

#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub struct RocksDbFilesystemBackend {
    inner: Arc<RocksDbFilesystemInner>,
}

#[allow(missing_debug_implementations)]
struct RocksDbFilesystemInner {
    path: PathBuf,
    blob: RocksDbBlobOptions,
    db: DB,
    write_gate: WriteGate,
}

#[allow(missing_debug_implementations)]
pub struct RocksDbFilesystemRead<'a> {
    snapshot: Snapshot<'a>,
}

#[allow(missing_debug_implementations)]
pub struct RocksDbFilesystemWrite {
    inner: Arc<RocksDbFilesystemInner>,
    _writer_permit: WriterPermit,
    batch: WriteBatch,
    staged_put_keys: Vec<Key>,
    stats: WriteStats,
}

static OPEN_DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<RocksDbFilesystemInner>>>> =
    OnceLock::new();

impl RocksDbFilesystemBackendOptions {
    pub fn plain(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            blob: RocksDbBlobOptions::Disabled,
        }
    }

    pub fn default_blob(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            blob: RocksDbBlobOptions::default(),
        }
    }

    pub fn blob(path: impl Into<PathBuf>, min_blob_size: u64) -> Self {
        Self {
            path: path.into(),
            blob: RocksDbBlobOptions::Enabled {
                min_blob_size,
                blob_file_size: DEFAULT_BLOB_FILE_SIZE,
                enable_gc: true,
                gc_age_cutoff: DEFAULT_BLOB_GC_AGE_CUTOFF,
            },
        }
    }
}

impl Default for RocksDbBlobOptions {
    fn default() -> Self {
        Self::Enabled {
            min_blob_size: DEFAULT_BLOB_MIN_SIZE,
            blob_file_size: DEFAULT_BLOB_FILE_SIZE,
            enable_gc: true,
            gc_age_cutoff: DEFAULT_BLOB_GC_AGE_CUTOFF,
        }
    }
}

impl RocksDbFilesystemBackend {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, BackendError> {
        Self::open_with_options(RocksDbFilesystemBackendOptions::default_blob(path))
    }

    pub fn open_with_options(
        options: RocksDbFilesystemBackendOptions,
    ) -> Result<Self, BackendError> {
        Ok(Self {
            inner: open_shared_rocksdb(options)?,
        })
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub fn flush(&self) -> Result<(), BackendError> {
        self.inner.db.flush().map_err(rocksdb_error)
    }

    pub fn compact_all(&self) -> Result<(), BackendError> {
        self.flush()?;
        self.inner.db.compact_range::<&[u8], &[u8]>(None, None);
        self.flush()
    }
}

impl Backend for RocksDbFilesystemBackend {
    type Read<'a>
        = RocksDbFilesystemRead<'a>
    where
        Self: 'a;

    type Write<'a>
        = RocksDbFilesystemWrite
    where
        Self: 'a;

    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(RocksDbFilesystemRead {
            snapshot: self.inner.db.snapshot(),
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        let writer_permit = self.inner.write_gate.acquire()?;
        Ok(RocksDbFilesystemWrite {
            inner: Arc::clone(&self.inner),
            _writer_permit: writer_permit,
            batch: WriteBatch::default(),
            staged_put_keys: Vec::new(),
            stats: WriteStats::default(),
        })
    }
}

impl BackendRead for RocksDbFilesystemRead<'_> {
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
        let physical_keys = keys
            .iter()
            .map(|key| physical_key(space, key))
            .collect::<Vec<_>>();
        for (index, (key, value)) in keys
            .iter()
            .zip(
                self.snapshot
                    .multi_get(physical_keys.iter().map(|key| key.0.as_ref())),
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
        if opts.limit_rows == 0 {
            return Ok(ScanResult {
                emitted: 0,
                has_more: false,
            });
        }

        let resume_after = opts.resume_after.map(|key| physical_key(space, key));
        let bounds = EncodedBounds::new(physical_range(space, range), resume_after.as_ref());
        let mut emitted = 0usize;

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
                    visitor.visit(KeyRef(&encoded_key[4..]), ProjectedValueRef::KeyOnly)?;
                }
                CoreProjection::FullValue => visitor.visit(
                    KeyRef(&encoded_key[4..]),
                    ProjectedValueRef::FullValue(value.as_ref()),
                )?,
            }
            emitted += 1;
        }

        Ok(ScanResult {
            emitted,
            has_more: false,
        })
    }
}

impl BackendWrite for RocksDbFilesystemWrite {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        for entry in entries.entries {
            let key = physical_key(space, &entry.key);
            let value = stored_value_bytes(entry.value);
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            self.staged_put_keys.push(key.clone());
            self.batch.put(key.0.as_ref(), value.as_ref());
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        for key in keys {
            self.batch.delete(physical_key(space, key).0.as_ref());
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), BackendError> {
        let range = physical_range(space, range);
        if let Some((lower, upper)) = rocksdb_delete_range_bounds(&range) {
            self.batch.delete_range(lower.as_slice(), upper.as_slice());
        } else {
            let bounds = EncodedBounds::new(range, None);
            for item in self
                .inner
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
        self.inner.db.write(self.batch).map_err(rocksdb_error)?;
        Ok(CommitResult {
            commit_id: None,
            stats: self.stats,
        })
    }

    fn rollback(self) -> Result<(), BackendError> {
        Ok(())
    }
}

fn open_shared_rocksdb(
    options: RocksDbFilesystemBackendOptions,
) -> Result<Arc<RocksDbFilesystemInner>, BackendError> {
    let path = registry_key(&options.path)?;
    let registry = OPEN_DATABASES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut open_databases = registry
        .lock()
        .map_err(|error| BackendError::Io(format!("rocksdb registry lock poisoned: {error}")))?;

    if let Some(existing) = open_databases.get(&path) {
        if let Some(inner) = existing.upgrade() {
            if inner.blob != options.blob {
                return Err(BackendError::Io(format!(
                    "rocksdb filesystem backend at {} is already open with different options",
                    path.display()
                )));
            }
            return Ok(inner);
        }
    }

    let open_options = RocksDbFilesystemBackendOptions {
        path: path.clone(),
        blob: options.blob,
    };
    let db = open_rocksdb(&open_options)?;
    let inner = Arc::new(RocksDbFilesystemInner {
        path: path.clone(),
        blob: options.blob,
        db,
        write_gate: WriteGate::new(),
    });
    open_databases.insert(path, Arc::downgrade(&inner));
    Ok(inner)
}

fn registry_key(path: &Path) -> Result<PathBuf, BackendError> {
    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| BackendError::Io(format!("read current directory: {error}")))?
            .join(path)
    };

    if absolute_path.exists() {
        return std::fs::canonicalize(&absolute_path).map_err(|error| {
            BackendError::Io(format!(
                "canonicalize rocksdb filesystem backend path {}: {error}",
                absolute_path.display()
            ))
        });
    }

    let parent = absolute_path.parent().ok_or_else(|| {
        BackendError::Io(format!(
            "rocksdb filesystem backend path has no parent: {}",
            absolute_path.display()
        ))
    })?;
    let file_name = absolute_path.file_name().ok_or_else(|| {
        BackendError::Io(format!(
            "rocksdb filesystem backend path has no final component: {}",
            absolute_path.display()
        ))
    })?;
    let canonical_parent = std::fs::canonicalize(parent).map_err(|error| {
        BackendError::Io(format!(
            "canonicalize rocksdb filesystem backend parent {}: {error}",
            parent.display()
        ))
    })?;
    Ok(canonical_parent.join(file_name))
}

fn open_rocksdb(options: &RocksDbFilesystemBackendOptions) -> Result<DB, BackendError> {
    let mut db_options = Options::default();
    db_options.create_if_missing(true);
    db_options.set_use_fsync(false);
    db_options.set_write_buffer_size(64 * 1024 * 1024);
    match options.blob {
        RocksDbBlobOptions::Disabled => {}
        RocksDbBlobOptions::Enabled {
            min_blob_size,
            blob_file_size,
            enable_gc,
            gc_age_cutoff,
        } => {
            db_options.set_enable_blob_files(true);
            db_options.set_min_blob_size(min_blob_size);
            db_options.set_blob_file_size(blob_file_size);
            db_options.set_enable_blob_gc(enable_gc);
            db_options.set_blob_gc_age_cutoff(gc_age_cutoff);
        }
    }
    DB::open(&db_options, &options.path)
        .map_err(|error| rocksdb_open_error(error, options.path.as_path()))
}

fn physical_key(space: SpaceId, key: &Key) -> Key {
    let mut bytes = Vec::with_capacity(4 + key.0.len());
    bytes.extend_from_slice(&space.0.to_be_bytes());
    bytes.extend_from_slice(&key.0);
    Key(Bytes::from(bytes))
}

fn physical_range(space: SpaceId, range: KeyRange) -> KeyRange {
    let map = |bound: Bound<Key>, unbounded: Bound<Key>| match bound {
        Bound::Included(key) => Bound::Included(physical_key(space, &key)),
        Bound::Excluded(key) => Bound::Excluded(physical_key(space, &key)),
        Bound::Unbounded => unbounded,
    };
    KeyRange {
        lower: map(
            range.lower,
            Bound::Included(Key(Bytes::copy_from_slice(&space.0.to_be_bytes()))),
        ),
        upper: map(
            range.upper,
            space.0.checked_add(1).map_or(Bound::Unbounded, |next| {
                Bound::Excluded(Key(Bytes::copy_from_slice(&next.to_be_bytes())))
            }),
        ),
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

#[expect(clippy::unnecessary_wraps)]
fn next_lexicographic_key(key: &Key) -> Option<Vec<u8>> {
    let mut bytes = key.0.to_vec();
    bytes.push(0);
    Some(bytes)
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
    BackendError::Io(format!("rocksdb filesystem backend: {error}"))
}

fn rocksdb_open_error(error: rocksdb::Error, path: &Path) -> BackendError {
    let message = error.to_string();
    if message.to_ascii_lowercase().contains("lock") {
        BackendError::Io(format!(
            "rocksdb filesystem backend at {} is already open by another process: {message}",
            path.display()
        ))
    } else {
        BackendError::Io(format!(
            "rocksdb filesystem backend failed to open {}: {message}",
            path.display()
        ))
    }
}

#[derive(Default)]
#[allow(missing_debug_implementations)]
struct WriteGate {
    state: Arc<WriteGateState>,
}

#[derive(Default)]
#[allow(missing_debug_implementations)]
struct WriteGateState {
    active: Mutex<bool>,
    available: Condvar,
}

#[allow(missing_debug_implementations)]
struct WriterPermit {
    state: Arc<WriteGateState>,
}

impl WriteGate {
    fn new() -> Self {
        Self::default()
    }

    fn acquire(&self) -> Result<WriterPermit, BackendError> {
        let mut active =
            self.state.active.lock().map_err(|error| {
                BackendError::Io(format!("rocksdb writer gate poisoned: {error}"))
            })?;
        while *active {
            active = self.state.available.wait(active).map_err(|error| {
                BackendError::Io(format!("rocksdb writer gate poisoned: {error}"))
            })?;
        }
        *active = true;
        Ok(WriterPermit {
            state: Arc::clone(&self.state),
        })
    }
}

impl Drop for WriterPermit {
    fn drop(&mut self) {
        if let Ok(mut active) = self.state.active.lock() {
            *active = false;
            self.state.available.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use lix_engine::backend::{
        Backend, BackendWrite, Key, PutBatch, PutEntry, ReadOptions, SpaceId, StoredValue,
        WriteOptions, get_many,
    };
    use std::env;
    use std::process::Command;
    use std::sync::mpsc;
    use std::time::Duration;

    use super::{RocksDbFilesystemBackend, RocksDbFilesystemBackendOptions};

    #[test]
    fn plain_backend_roundtrips_point_read_after_reopen() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("fs.rocksdb");
        let key = Key(Bytes::from_static(b"file-chunk"));
        let value = Bytes::from_static(b"chunk bytes");

        {
            let backend = RocksDbFilesystemBackend::open(&path).expect("open backend");
            let mut write = backend
                .begin_write(WriteOptions::default())
                .expect("begin write");
            write
                .put_many(
                    SpaceId(0x0005_0003),
                    PutBatch {
                        entries: vec![PutEntry {
                            key: key.clone(),
                            value: StoredValue {
                                bytes: value.clone(),
                            },
                        }],
                    },
                )
                .expect("put chunk");
            write.commit().expect("commit write");
            backend.flush().expect("flush backend");
        }

        let backend = RocksDbFilesystemBackend::open(&path).expect("reopen backend");
        let read = backend
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let result =
            get_many(&read, SpaceId(0x0005_0003), &[key], Default::default()).expect("read chunk");
        assert_eq!(result.values.len(), 1);
        assert_eq!(
            result.values[0].as_ref().map(|value| value.as_ref()),
            Some(lix_engine::backend::ProjectedValueRef::FullValue(
                value.as_ref()
            ))
        );
    }

    #[test]
    fn same_process_open_reuses_shared_database_handle() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("fs.rocksdb");
        let key_a = Key(Bytes::from_static(b"from-a"));
        let key_b = Key(Bytes::from_static(b"from-b"));
        let space = SpaceId(0x0005_0003);

        let backend_a = RocksDbFilesystemBackend::open(&path).expect("open first backend");
        let backend_b = RocksDbFilesystemBackend::open(&path).expect("open second backend");

        put_one(&backend_a, space, key_a.clone(), Bytes::from_static(b"a"));
        assert_eq!(
            read_one(&backend_b, space, key_a.clone()),
            Some(Bytes::from_static(b"a"))
        );

        put_one(&backend_b, space, key_b.clone(), Bytes::from_static(b"b"));
        assert_eq!(
            read_one(&backend_a, space, key_b.clone()),
            Some(Bytes::from_static(b"b"))
        );
    }

    #[test]
    fn same_process_writes_are_serialized_across_reopened_handles() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("fs.rocksdb");
        let backend_a = RocksDbFilesystemBackend::open(&path).expect("open first backend");
        let backend_b = RocksDbFilesystemBackend::open(&path).expect("open second backend");
        let write_a = backend_a
            .begin_write(WriteOptions::default())
            .expect("begin first write");

        let (attempt_tx, attempt_rx) = mpsc::channel();
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            attempt_tx.send(()).expect("signal write attempt");
            let write_b = backend_b
                .begin_write(WriteOptions::default())
                .expect("begin second write");
            acquired_tx.send(()).expect("signal write acquired");
            write_b.rollback().expect("rollback second write");
        });

        attempt_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second write should be attempted");
        assert!(
            acquired_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "second write should wait while the first write is active"
        );

        write_a.rollback().expect("rollback first write");
        acquired_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second write should acquire after first write closes");
        waiter.join().expect("writer thread should finish");
    }

    #[test]
    fn same_process_open_rejects_different_blob_options_for_open_database() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("fs.rocksdb");
        let _default_blob =
            RocksDbFilesystemBackend::open(&path).expect("open default blob backend");

        let error = match RocksDbFilesystemBackend::open_with_options(
            RocksDbFilesystemBackendOptions::blob(&path, 16),
        ) {
            Ok(_) => panic!("second open with different options should fail"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("already open with different options"),
            "error should explain option mismatch: {error}"
        );
    }

    #[test]
    fn cross_process_open_reports_locked_database() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("fs.rocksdb");
        let _backend = RocksDbFilesystemBackend::open(&path).expect("open parent backend");
        let test_binary = env::current_exe().expect("current test binary path should resolve");

        let output = Command::new(test_binary)
            .arg("--exact")
            .arg("rocksdb::tests::cross_process_open_helper")
            .arg("--nocapture")
            .env("LIX_ROCKSDB_LOCK_HELPER_PATH", &path)
            .output()
            .expect("spawn rocksdb lock helper");

        assert!(
            output.status.success(),
            "helper should observe locked RocksDB database\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn cross_process_open_helper() {
        let Some(path) = env::var_os("LIX_ROCKSDB_LOCK_HELPER_PATH") else {
            return;
        };

        let error = match RocksDbFilesystemBackend::open(path) {
            Ok(_) => panic!("child process should not open RocksDB while parent holds the DB lock"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("already open by another process"),
            "lock error should be mapped clearly: {error}"
        );
    }

    #[test]
    fn blob_options_open() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let options = RocksDbFilesystemBackendOptions::blob(temp_dir.path().join("fs.rocksdb"), 16);
        let backend =
            RocksDbFilesystemBackend::open_with_options(options).expect("open blob backend");
        backend.flush().expect("flush blob backend");
    }

    fn put_one(backend: &RocksDbFilesystemBackend, space: SpaceId, key: Key, value: Bytes) {
        let mut write = backend
            .begin_write(WriteOptions::default())
            .expect("begin write");
        write
            .put_many(
                space,
                PutBatch {
                    entries: vec![PutEntry {
                        key,
                        value: StoredValue { bytes: value },
                    }],
                },
            )
            .expect("put one row");
        write.commit().expect("commit write");
    }

    fn read_one(backend: &RocksDbFilesystemBackend, space: SpaceId, key: Key) -> Option<Bytes> {
        let read = backend
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let result = get_many(&read, space, &[key], Default::default()).expect("read one row");
        result.values[0].clone().map(|value| match value {
            lix_engine::backend::ProjectedValue::FullValue(bytes) => bytes,
            lix_engine::backend::ProjectedValue::KeyOnly => Bytes::new(),
        })
    }
}
