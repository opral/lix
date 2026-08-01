#![allow(
    clippy::manual_async_fn,
    reason = "explicit future signatures mirror Storage traits and keep Send guarantees visible"
)]

use std::collections::HashMap;
use std::future::Future;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use bytes::{Buf, Bytes};
use lix_engine::storage::{
    CommitResult, CoreProjection, GetManyRequest, GetManyResult, Key, KeyRange, Precondition,
    PreconditionFailure, ProjectedValue, PutBatch, ReadDurability, ReadEntry, ReadOptions,
    ScanChunk, ScanOptions, SpaceId, Storage, StorageError, StorageRead, StorageWrite, StoredValue,
    WriteOptions, WriteStats,
};
use lix_engine::{StorageFactory, StorageFixture, StorageTestConfig};
use rocksdb::Snapshot;
use rocksdb::{BlockBasedOptions, DB, Direction, IteratorMode, Options, WriteBatch};
use tempfile::TempDir;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

const DEFAULT_BLOB_MIN_SIZE: u64 = 32 * 1024;
const DEFAULT_BLOB_FILE_SIZE: u64 = 256 * 1024 * 1024;
const BLOB_GC_FORCE_THRESHOLD: f64 = 0.5;

#[derive(Debug)]
pub struct RocksDBFactory {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct RocksDBFixture {
    path: PathBuf,
}

#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub struct RocksDB {
    path: PathBuf,
    inner: Arc<RocksDBInner>,
}

#[allow(missing_debug_implementations)]
struct RocksDBInner {
    db: DB,
    write_gate: WriteGate,
}

#[allow(missing_debug_implementations)]
pub struct RocksDBRead<'a> {
    snapshot: Snapshot<'a>,
}

#[allow(missing_debug_implementations)]
pub struct RocksDBWrite {
    inner: Arc<RocksDBInner>,
    _writer_permit: OwnedMutexGuard<()>,
    batch: WriteBatch,
    stats: WriteStats,
}

static OPEN_DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<RocksDBInner>>>> = OnceLock::new();

impl Default for RocksDBFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RocksDBFactory {
    pub fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create rocksdb storage temp dir"),
            next_database_id: AtomicU64::new(0),
        }
    }
}

impl StorageFactory for RocksDBFactory {
    type Storage = RocksDB;
    type Fixture = RocksDBFixture;

    fn create_fixture(&self) -> Self::Fixture {
        let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        let path = self
            .temp_dir
            .path()
            .join(format!("storage-{database_id}.rocksdb"));
        RocksDBFixture { path }
    }

    fn config(&self) -> StorageTestConfig {
        StorageTestConfig {
            ephemeral: false,
            supports_concurrent_writers: false,
            ..StorageTestConfig::default()
        }
    }
}

impl StorageFixture for RocksDBFixture {
    type Storage = RocksDB;

    fn open(&self) -> impl Future<Output = Self::Storage> + Send {
        async move { RocksDB::open(&self.path).expect("open rocksdb storage") }
    }
}

impl RocksDB {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let path = path.into();
        Ok(Self {
            inner: open_shared_rocksdb(path.clone())?,
            path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        self.inner.db.flush().map_err(rocksdb_error)
    }

    /// Flushes the WAL without forcing the active memtable into an SST.
    ///
    /// Storage lifecycle benchmarks use this to distinguish durable,
    /// memory-resident reads from memtable-flushed reads.
    #[doc(hidden)]
    pub fn flush_wal_for_diagnostics(&self) -> Result<(), StorageError> {
        self.inner.db.flush_wal(true).map_err(rocksdb_error)
    }
}

impl Storage for RocksDB {
    type Read<'a>
        = RocksDBRead<'a>
    where
        Self: 'a;

    type Write<'a>
        = RocksDBWrite
    where
        Self: 'a;
    fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        async move {
            if opts.durability == ReadDurability::Durable {
                return Err(StorageError::Durability);
            }
            Ok(RocksDBRead {
                snapshot: self.inner.db.snapshot(),
            })
        }
    }

    fn begin_write(
        &self,
        opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        async move {
            let writer_permit = self.inner.write_gate.acquire().await;
            check_preconditions(&self.inner.db, &opts.preconditions)?;
            Ok(RocksDBWrite {
                inner: Arc::clone(&self.inner),
                _writer_permit: writer_permit,
                batch: if opts.batch_capacity_hint_bytes == 0 {
                    WriteBatch::default()
                } else {
                    WriteBatch::with_capacity_bytes(opts.batch_capacity_hint_bytes)
                },
                stats: WriteStats::default(),
            })
        }
    }
}

fn check_preconditions(db: &DB, preconditions: &[Precondition]) -> Result<(), StorageError> {
    let mut failures = Vec::new();
    for (index, precondition) in preconditions.iter().enumerate() {
        let matches = match precondition {
            Precondition::KeyAbsent { space, key } => db
                .get(physical_key(*space, key).0)
                .map_err(rocksdb_error)?
                .is_none(),
            Precondition::KeyPresent { space, key } => db
                .get(physical_key(*space, key).0)
                .map_err(rocksdb_error)?
                .is_some(),
            Precondition::KeyValueHashEquals { space, key, hash } => db
                .get(physical_key(*space, key).0)
                .map_err(rocksdb_error)?
                .is_some_and(|value| blake3::hash(&value).as_bytes() == hash),
            Precondition::KeyValueEquals {
                space,
                key,
                expected,
            } => db
                .get(physical_key(*space, key).0)
                .map_err(rocksdb_error)?
                .is_some_and(|value| value.as_slice() == expected.as_ref()),
            Precondition::RangeEmpty { space, range } => {
                let bounds = EncodedBounds::new(physical_range(*space, range.clone()), None);
                let mut empty = true;
                for item in db.iterator(IteratorMode::From(&bounds.lower_seek, Direction::Forward))
                {
                    let (key, _) = item.map_err(rocksdb_error)?;
                    if !bounds.after_lower(&key) {
                        continue;
                    }
                    if bounds.before_upper(&key) {
                        empty = false;
                    }
                    break;
                }
                empty
            }
            Precondition::BranchEquals { ref_key, expected } => db
                .get(ref_key.0.as_ref())
                .map_err(rocksdb_error)?
                .is_some_and(|value| value.as_slice() == expected.as_ref()),
        };
        if !matches {
            failures.push(PreconditionFailure { index });
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(StorageError::PreconditionFailed(failures))
    }
}

/// RocksDB keeps its single-keyspace layout; spaces are scoped by prefixing
/// the 4-byte big-endian space id internally. Reads return logical keys.
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

impl StorageRead for RocksDBRead<'_> {
    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        async move {
            if let [request] = requests
                && let [key] = request.keys
            {
                let physical_key = physical_key(request.space, key);
                let value = self
                    .snapshot
                    .get(physical_key.0.as_ref())
                    .map_err(rocksdb_error)?;
                return Ok(GetManyResult::new(vec![value.map(|value| {
                    project_owned_value(value, request.opts.projection)
                })]));
            }
            let physical_keys = requests
                .iter()
                .flat_map(|request| {
                    request
                        .keys
                        .iter()
                        .map(|key| physical_key(request.space, key))
                })
                .collect::<Vec<_>>();
            let mut values = self
                .snapshot
                .multi_get(physical_keys.iter().map(|key| key.0.as_ref()))
                .into_iter();
            let mut results = Vec::with_capacity(physical_keys.len());
            for request in requests {
                let request_values = values
                    .by_ref()
                    .take(request.keys.len())
                    .map(|value| {
                        value.map_err(rocksdb_error).map(|value| {
                            value.map(|value| project_owned_value(value, request.opts.projection))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                results.extend(request_values);
            }
            let unexpected_value = values.next();
            debug_assert!(unexpected_value.is_none());
            Ok(GetManyResult::new(results))
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
                if !bounds.after_lower(encoded_key.as_ref()) {
                    continue;
                }
                if !bounds.before_upper(encoded_key.as_ref()) {
                    break;
                }
                if entries.len() == opts.page_size() {
                    return Ok(ScanChunk {
                        entries,
                        has_more: true,
                    });
                }
                entries.push(ReadEntry {
                    key: logical_key_from_physical(encoded_key),
                    value: project_owned_value(value, opts.projection),
                });
            }
            Ok(ScanChunk {
                entries,
                has_more: false,
            })
        }
    }
}

impl StorageWrite for RocksDBWrite {
    fn put_many(
        &mut self,
        space: SpaceId,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            let max_key_bytes = entries
                .entries
                .iter()
                .map(|entry| 4_usize.saturating_add(entry.key.0.len()))
                .max()
                .unwrap_or(0);
            let mut physical_key = Vec::with_capacity(max_key_bytes);
            let space_prefix = space.0.to_be_bytes();
            for entry in entries.entries {
                physical_key.clear();
                physical_key.extend_from_slice(&space_prefix);
                physical_key.extend_from_slice(&entry.key.0);
                let value = stored_value_bytes(entry.value);
                self.stats.put_entries += 1;
                self.stats.written_bytes += value.len() as u64;
                self.batch.put(physical_key.as_slice(), value.as_ref());
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
            let physical_key_bytes = keys.iter().fold(0_usize, |bytes, key| {
                bytes.saturating_add(4).saturating_add(key.0.len())
            });
            let mut key_bytes = Vec::with_capacity(physical_key_bytes);
            let space_prefix = space.0.to_be_bytes();
            for key in keys {
                let key_start = key_bytes.len();
                key_bytes.extend_from_slice(&space_prefix);
                key_bytes.extend_from_slice(&key.0);
                let key_end = key_bytes.len();
                self.batch.delete(&key_bytes[key_start..key_end]);
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

fn open_shared_rocksdb(path: PathBuf) -> Result<Arc<RocksDBInner>, StorageError> {
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
    let inner = Arc::new(RocksDBInner {
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
                "canonicalize rocksdb storage path {}: {error}",
                absolute_path.display()
            ))
        });
    }

    let parent = absolute_path.parent().ok_or_else(|| {
        StorageError::Io(format!(
            "rocksdb storage path has no parent: {}",
            absolute_path.display()
        ))
    })?;
    let file_name = absolute_path.file_name().ok_or_else(|| {
        StorageError::Io(format!(
            "rocksdb storage path has no final component: {}",
            absolute_path.display()
        ))
    })?;
    let canonical_parent = std::fs::canonicalize(parent).map_err(|error| {
        StorageError::Io(format!(
            "canonicalize rocksdb storage parent {}: {error}",
            parent.display()
        ))
    })?;
    Ok(canonical_parent.join(file_name))
}

fn open_rocksdb(path: &Path) -> Result<DB, StorageError> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_use_fsync(false);
    options.set_write_buffer_size(64 * 1024 * 1024);
    options.set_compression_type(rocksdb::DBCompressionType::Zstd);
    options.set_compression_options(-14, 1, 0, 0);
    let mut table_options = BlockBasedOptions::default();
    // Full whole-key filters let missing point reads skip unrelated SST data.
    table_options.set_bloom_filter(8.0, false);
    table_options.set_optimize_filters_for_memory(true);
    options.set_block_based_table_factory(&table_options);
    options.set_enable_blob_files(true);
    options.set_min_blob_size(DEFAULT_BLOB_MIN_SIZE);
    options.set_blob_file_size(DEFAULT_BLOB_FILE_SIZE);
    options.set_blob_compression_type(rocksdb::DBCompressionType::Zstd);
    // Lix payloads are immutable CAS objects and repository GC owns their
    // reachability lifecycle. Do not relocate merely old live blobs during
    // ordinary compaction (over 10x physical writes in the 20 GiB run), but
    // retain physical reclamation once a blob file is at least half garbage.
    options.set_enable_blob_gc(true);
    options.set_blob_gc_age_cutoff(0.0);
    options.set_blob_gc_force_threshold(BLOB_GC_FORCE_THRESHOLD);
    DB::open(&options, path).map_err(|error| rocksdb_open_error(error, path))
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}

/// Reclaims the iterator-owned physical key and removes its four-byte space
/// prefix without copying its logical-key bytes.
fn logical_key_from_physical(encoded_key: Box<[u8]>) -> Key {
    let mut key = Bytes::from(encoded_key);
    key.advance(4);
    Key(key)
}

fn project_owned_value<T>(value: T, projection: CoreProjection) -> ProjectedValue
where
    Bytes: From<T>,
{
    match projection {
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        // `Snapshot::get` and `Snapshot::multi_get` yield Rust-owned
        // `Vec<u8>` values, while the standard iterator yields Rust-owned
        // `Box<[u8]>` values. Retaining those allocations in `Bytes` avoids a
        // second full value copy.
        CoreProjection::FullValue => ProjectedValue::FullValue(Bytes::from(value)),
    }
}

#[cfg(test)]
mod tests {
    use super::{CoreProjection, ProjectedValue, logical_key_from_physical, project_owned_value};

    #[test]
    fn logical_key_reuses_the_physical_key_allocation() {
        let physical_key: Box<[u8]> = Box::from(&b"\0\0\0\x01logical-key"[..]);
        let logical_ptr = physical_key.as_ptr().wrapping_add(4);

        let key = logical_key_from_physical(physical_key);

        assert_eq!(key.0.as_ptr(), logical_ptr);
        assert_eq!(&key.0[..], b"logical-key");
    }

    #[test]
    fn full_value_reuses_vec_allocation() {
        let value = b"small value that used to take the copy path".to_vec();
        let ptr = value.as_ptr();

        let ProjectedValue::FullValue(value) =
            project_owned_value(value, CoreProjection::FullValue)
        else {
            panic!("full-value projection should return bytes");
        };

        assert_eq!(value.as_ptr(), ptr);
        assert_eq!(&value[..], b"small value that used to take the copy path");
    }

    #[test]
    fn full_value_reuses_boxed_slice_allocation() {
        let value: Box<[u8]> = Box::from(&b"iterator value"[..]);
        let ptr = value.as_ptr();

        let ProjectedValue::FullValue(value) =
            project_owned_value(value, CoreProjection::FullValue)
        else {
            panic!("full-value projection should return bytes");
        };

        assert_eq!(value.as_ptr(), ptr);
        assert_eq!(&value[..], b"iterator value");
    }
}

fn rocksdb_error(error: rocksdb::Error) -> StorageError {
    StorageError::Io(format!("rocksdb storage: {error}"))
}

fn rocksdb_open_error(error: rocksdb::Error, path: &Path) -> StorageError {
    let message = error.to_string();
    if message.to_ascii_lowercase().contains("lock") {
        StorageError::Io(format!(
            "rocksdb storage at {} is already open by another process: {message}",
            path.display()
        ))
    } else {
        StorageError::Io(format!(
            "rocksdb storage failed to open {}: {message}",
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
