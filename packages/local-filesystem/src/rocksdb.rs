#![allow(
    clippy::manual_async_fn,
    reason = "explicit future signatures mirror Storage traits and keep Send guarantees visible"
)]

use std::collections::HashMap;
use std::future::Future;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use bytes::Bytes;
use lix_engine::storage::{
    CommitResult, CoreProjection, GetManyResult, GetOptions, Key, KeyRange, ProjectedValue,
    PutBatch, ReadEntry, ReadOptions, ScanChunk, ScanOptions, SpaceId, Storage, StorageError,
    StorageRead, StorageWrite, StoredValue, WriteOptions, WriteStats,
};
use rocksdb::Snapshot;
use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

const DEFAULT_BLOB_MIN_SIZE: u64 = 32 * 1024;
const DEFAULT_BLOB_FILE_SIZE: u64 = 256 * 1024 * 1024;
const DEFAULT_BLOB_GC_AGE_CUTOFF: f64 = 0.25;

#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub struct RocksDBFilesystem {
    inner: Arc<RocksDBFilesystemInner>,
}

#[allow(missing_debug_implementations)]
struct RocksDBFilesystemInner {
    db: DB,
    write_gate: WriteGate,
}

#[allow(missing_debug_implementations)]
pub struct RocksDBFilesystemRead<'a> {
    snapshot: Snapshot<'a>,
}

#[allow(missing_debug_implementations)]
pub struct RocksDBFilesystemWrite {
    inner: Arc<RocksDBFilesystemInner>,
    _writer_permit: OwnedMutexGuard<()>,
    batch: WriteBatch,
    staged_put_keys: Vec<Key>,
    stats: WriteStats,
}

static OPEN_DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<RocksDBFilesystemInner>>>> =
    OnceLock::new();

impl RocksDBFilesystem {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StorageError> {
        Ok(Self {
            inner: open_shared_rocksdb(path.into())?,
        })
    }

    #[cfg(test)]
    fn flush(&self) -> Result<(), StorageError> {
        self.inner.db.flush().map_err(rocksdb_error)
    }
}

impl Storage for RocksDBFilesystem {
    type Read<'a>
        = RocksDBFilesystemRead<'a>
    where
        Self: 'a;

    type Write<'a>
        = RocksDBFilesystemWrite
    where
        Self: 'a;

    fn begin_read(
        &self,
        _opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        async move {
            Ok(RocksDBFilesystemRead {
                snapshot: self.inner.db.snapshot(),
            })
        }
    }

    fn begin_write(
        &self,
        _opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        async move {
            let writer_permit = self.inner.write_gate.acquire().await;
            Ok(RocksDBFilesystemWrite {
                inner: Arc::clone(&self.inner),
                _writer_permit: writer_permit,
                batch: WriteBatch::default(),
                staged_put_keys: Vec::new(),
                stats: WriteStats::default(),
            })
        }
    }
}

impl StorageRead for RocksDBFilesystemRead<'_> {
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        async move {
            let physical_keys = keys
                .iter()
                .map(|key| physical_key(space, key))
                .collect::<Vec<_>>();
            let mut values = Vec::with_capacity(keys.len());
            for value in self
                .snapshot
                .multi_get(physical_keys.iter().map(|key| key.0.as_ref()))
            {
                let value = value.map_err(rocksdb_error)?;
                values.push(
                    value
                        .as_ref()
                        .map(|value| project_value(value.as_ref(), opts.projection)),
                );
            }
            Ok(GetManyResult::new(values))
        }
    }

    fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> impl Future<Output = Result<ScanChunk, StorageError>> + Send {
        async move {
            if opts.page_size() == 0 {
                return Ok(ScanChunk {
                    entries: Vec::new(),
                    has_more: false,
                });
            }
            let resume_after = opts
                .resume_after
                .as_ref()
                .map(|key| physical_key(space, key));
            let bounds = EncodedBounds::new(physical_range(space, range), resume_after.as_ref());
            let mut entries = Vec::with_capacity(opts.page_size());

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
                if entries.len() == opts.page_size() {
                    return Ok(ScanChunk {
                        entries,
                        has_more: true,
                    });
                }
                entries.push(ReadEntry {
                    key: Key(Bytes::copy_from_slice(&encoded_key[4..])),
                    value: project_value(value.as_ref(), opts.projection),
                });
            }
            Ok(ScanChunk {
                entries,
                has_more: false,
            })
        }
    }
}

impl StorageWrite for RocksDBFilesystemWrite {
    fn put_many(
        &mut self,
        space: SpaceId,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            for entry in entries.entries {
                let key = physical_key(space, &entry.key);
                let value = stored_value_bytes(entry.value);
                self.stats.put_entries += 1;
                self.stats.written_bytes += value.len() as u64;
                self.staged_put_keys.push(key.clone());
                self.batch.put(key.0.as_ref(), value.as_ref());
            }
            self.stats.storage_calls += 1;
            Ok(())
        }
    }

    fn delete_many(
        &mut self,
        space: SpaceId,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            for key in keys {
                self.batch.delete(physical_key(space, key).0.as_ref());
            }
            self.stats.deleted_entries += keys.len() as u64;
            self.stats.storage_calls += 1;
            Ok(())
        }
    }

    fn delete_range(
        &mut self,
        space: SpaceId,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
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
            self.stats.storage_calls += 1;
            Ok(())
        }
    }

    fn commit(self) -> impl Future<Output = Result<CommitResult, StorageError>> + Send {
        async move {
            self.inner.db.write(self.batch).map_err(rocksdb_error)?;
            Ok(CommitResult {
                commit_id: None,
                stats: self.stats,
            })
        }
    }

    fn rollback(self) -> impl Future<Output = Result<(), StorageError>> + Send {
        async { Ok(()) }
    }
}

fn open_shared_rocksdb(path: PathBuf) -> Result<Arc<RocksDBFilesystemInner>, StorageError> {
    let path = registry_key(&path)?;
    let registry = OPEN_DATABASES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut open_databases = registry
        .lock()
        .map_err(|error| StorageError::Io(format!("rocksdb registry lock poisoned: {error}")))?;

    if let Some(existing) = open_databases.get(&path) {
        if let Some(inner) = existing.upgrade() {
            return Ok(inner);
        }
    }

    let db = open_rocksdb(&path)?;
    let inner = Arc::new(RocksDBFilesystemInner {
        db,
        write_gate: WriteGate::new(),
    });
    open_databases.insert(path, Arc::downgrade(&inner));
    Ok(inner)
}

fn registry_key(path: &Path) -> Result<PathBuf, StorageError> {
    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| StorageError::Io(format!("read current directory: {error}")))?
            .join(path)
    };

    if absolute_path.exists() {
        return std::fs::canonicalize(&absolute_path).map_err(|error| {
            StorageError::Io(format!(
                "canonicalize rocksdb filesystem storage path {}: {error}",
                absolute_path.display()
            ))
        });
    }

    let parent = absolute_path.parent().ok_or_else(|| {
        StorageError::Io(format!(
            "rocksdb filesystem storage path has no parent: {}",
            absolute_path.display()
        ))
    })?;
    let file_name = absolute_path.file_name().ok_or_else(|| {
        StorageError::Io(format!(
            "rocksdb filesystem storage path has no final component: {}",
            absolute_path.display()
        ))
    })?;
    let canonical_parent = std::fs::canonicalize(parent).map_err(|error| {
        StorageError::Io(format!(
            "canonicalize rocksdb filesystem storage parent {}: {error}",
            parent.display()
        ))
    })?;
    Ok(canonical_parent.join(file_name))
}

fn open_rocksdb(path: &Path) -> Result<DB, StorageError> {
    let mut db_options = Options::default();
    db_options.create_if_missing(true);
    db_options.set_use_fsync(false);
    db_options.set_write_buffer_size(64 * 1024 * 1024);
    db_options.set_enable_blob_files(true);
    db_options.set_min_blob_size(DEFAULT_BLOB_MIN_SIZE);
    db_options.set_blob_file_size(DEFAULT_BLOB_FILE_SIZE);
    db_options.set_enable_blob_gc(true);
    db_options.set_blob_gc_age_cutoff(DEFAULT_BLOB_GC_AGE_CUTOFF);
    DB::open(&db_options, path).map_err(|error| rocksdb_open_error(error, path))
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

fn project_value(value: &[u8], projection: CoreProjection) -> ProjectedValue {
    match projection {
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        CoreProjection::FullValue => ProjectedValue::FullValue(Bytes::copy_from_slice(value)),
    }
}

fn rocksdb_error(error: rocksdb::Error) -> StorageError {
    StorageError::Io(format!("rocksdb filesystem storage: {error}"))
}

fn rocksdb_open_error(error: rocksdb::Error, path: &Path) -> StorageError {
    let message = error.to_string();
    if message.to_ascii_lowercase().contains("lock") {
        StorageError::Io(format!(
            "rocksdb filesystem storage at {} is already open by another process: {message}",
            path.display()
        ))
    } else {
        StorageError::Io(format!(
            "rocksdb filesystem storage failed to open {}: {message}",
            path.display()
        ))
    }
}

#[derive(Default)]
#[allow(missing_debug_implementations)]
struct WriteGate {
    state: Arc<AsyncMutex<()>>,
}

impl WriteGate {
    fn new() -> Self {
        Self::default()
    }

    async fn acquire(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.state).lock_owned().await
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use lix_engine::storage::{
        GetOptions, Key, PutBatch, PutEntry, ReadOptions, SpaceId, Storage, StorageRead,
        StorageWrite, StoredValue, WriteOptions,
    };
    use lix_engine::{StorageFactory, StorageFixture, StorageTestConfig, run_storage_conformance};
    use std::env;
    use std::future::Future;
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::TempDir;

    use super::RocksDBFilesystem;

    #[derive(Debug)]
    struct RocksDBFilesystemFactory {
        temp_dir: TempDir,
        next_database_id: AtomicU64,
    }

    #[derive(Clone, Debug)]
    struct RocksDBFilesystemFixture {
        path: std::path::PathBuf,
    }

    impl RocksDBFilesystemFactory {
        fn new() -> Self {
            Self {
                temp_dir: tempfile::tempdir().expect("create rocksdb fs storage temp dir"),
                next_database_id: AtomicU64::new(0),
            }
        }
    }

    impl StorageFactory for RocksDBFilesystemFactory {
        type Storage = RocksDBFilesystem;
        type Fixture = RocksDBFilesystemFixture;

        fn create_fixture(&self) -> Self::Fixture {
            let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
            RocksDBFilesystemFixture {
                path: self
                    .temp_dir
                    .path()
                    .join(format!("local-filesystem-{database_id}.rocksdb")),
            }
        }

        fn config(&self) -> StorageTestConfig {
            StorageTestConfig {
                supports_concurrent_writers: false,
                ..StorageTestConfig::default()
            }
        }
    }

    impl StorageFixture for RocksDBFilesystemFixture {
        type Storage = RocksDBFilesystem;

        fn open(&self) -> impl Future<Output = Self::Storage> + Send {
            async move { RocksDBFilesystem::open(&self.path).expect("open rocksdb fs storage") }
        }
    }

    #[test]
    fn passes_storage_conformance() {
        let report = block_on(run_storage_conformance(&RocksDBFilesystemFactory::new()));
        report.assert_no_failures();
    }

    #[test]
    fn blobdb_storage_roundtrips_point_read_after_reopen() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("fs.rocksdb");
        let key = Key(Bytes::from_static(b"file-chunk"));
        let value = Bytes::from_static(b"chunk bytes");

        {
            let storage = RocksDBFilesystem::open(&path).expect("open storage");
            let mut write =
                block_on(storage.begin_write(WriteOptions::default())).expect("begin write");
            block_on(write.put_many(
                SpaceId(0x0005_0003),
                PutBatch {
                    entries: vec![PutEntry {
                        key: key.clone(),
                        value: StoredValue {
                            bytes: value.clone(),
                        },
                    }],
                },
            ))
            .expect("put chunk");
            block_on(write.commit()).expect("commit write");
            storage.flush().expect("flush storage");
        }

        let storage = RocksDBFilesystem::open(&path).expect("reopen storage");
        let read = block_on(storage.begin_read(ReadOptions::default())).expect("begin read");
        let result = block_on(read.get_many(SpaceId(0x0005_0003), &[key], GetOptions::default()))
            .expect("read chunk");
        assert_eq!(result.values.len(), 1);
        assert_eq!(
            result.values[0],
            Some(lix_engine::storage::ProjectedValue::FullValue(value))
        );
    }

    #[test]
    fn same_process_open_reuses_shared_database_handle() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("fs.rocksdb");
        let key_a = Key(Bytes::from_static(b"from-a"));
        let key_b = Key(Bytes::from_static(b"from-b"));
        let space = SpaceId(0x0005_0003);

        let storage_a = RocksDBFilesystem::open(&path).expect("open first storage");
        let storage_b = RocksDBFilesystem::open(&path).expect("open second storage");

        put_one(&storage_a, space, key_a.clone(), Bytes::from_static(b"a"));
        assert_eq!(
            read_one(&storage_b, space, key_a),
            Some(Bytes::from_static(b"a"))
        );

        put_one(&storage_b, space, key_b.clone(), Bytes::from_static(b"b"));
        assert_eq!(
            read_one(&storage_a, space, key_b),
            Some(Bytes::from_static(b"b"))
        );
    }

    #[test]
    fn same_process_writes_are_serialized_across_reopened_handles() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("fs.rocksdb");
        let storage_a = RocksDBFilesystem::open(&path).expect("open first storage");
        let storage_b = RocksDBFilesystem::open(&path).expect("open second storage");
        let write_a =
            block_on(storage_a.begin_write(WriteOptions::default())).expect("begin first write");

        let (attempt_tx, attempt_rx) = mpsc::channel();
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            attempt_tx.send(()).expect("signal write attempt");
            let write_b = block_on(storage_b.begin_write(WriteOptions::default()))
                .expect("begin second write");
            acquired_tx.send(()).expect("signal write acquired");
            block_on(write_b.rollback()).expect("rollback second write");
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

        block_on(write_a.rollback()).expect("rollback first write");
        acquired_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second write should acquire after first write closes");
        waiter.join().expect("writer thread should finish");
    }

    #[test]
    fn writes_large_values_to_blob_files() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("fs.rocksdb");
        let storage = RocksDBFilesystem::open(&path).expect("open blob storage");
        put_one(
            &storage,
            SpaceId(0x0005_0003),
            Key(Bytes::from_static(b"large-value")),
            Bytes::from(vec![7; 128 * 1024]),
        );
        storage.flush().expect("flush blob storage");
        drop(storage);

        assert!(
            rocksdb_blob_file_count(&path) > 0,
            "large values should be stored in RocksDB blob files"
        );
    }

    #[test]
    fn cross_process_open_reports_locked_database() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("fs.rocksdb");
        let _storage = RocksDBFilesystem::open(&path).expect("open parent storage");
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

        let Err(error) = RocksDBFilesystem::open(path) else {
            panic!("child process should not open RocksDB while parent holds the DB lock");
        };

        assert!(
            error
                .to_string()
                .contains("already open by another process"),
            "lock error should be mapped clearly: {error}"
        );
    }

    fn put_one(storage: &RocksDBFilesystem, space: SpaceId, key: Key, value: Bytes) {
        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin write");
        block_on(write.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key,
                    value: StoredValue { bytes: value },
                }],
            },
        ))
        .expect("put one row");
        block_on(write.commit()).expect("commit write");
    }

    fn read_one(storage: &RocksDBFilesystem, space: SpaceId, key: Key) -> Option<Bytes> {
        let read = block_on(storage.begin_read(ReadOptions::default())).expect("begin read");
        let result =
            block_on(read.get_many(space, &[key], GetOptions::default())).expect("read one row");
        result.values[0].clone().map(|value| match value {
            lix_engine::storage::ProjectedValue::FullValue(bytes) => bytes,
            lix_engine::storage::ProjectedValue::KeyOnly => Bytes::new(),
        })
    }

    fn rocksdb_blob_file_count(path: &std::path::Path) -> usize {
        std::fs::read_dir(path)
            .expect("read rocksdb directory")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "blob")
            })
            .count()
    }

    fn block_on<T>(future: impl Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("build test runtime")
            .block_on(future)
    }
}
