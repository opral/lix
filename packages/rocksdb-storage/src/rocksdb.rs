#![allow(
    clippy::manual_async_fn,
    reason = "explicit future signatures mirror Storage traits and keep Send guarantees visible"
)]

use std::future::Future;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use lix_engine::storage::{
    CommitResult, CoreProjection, GetManyResult, GetOptions, Key, KeyRange, ProjectedValue,
    PutBatch, ReadEntry, ReadOptions, ScanChunk, ScanOptions, SpaceId, Storage, StorageError,
    StorageRead, StorageWrite, StoredValue, WriteOptions, WriteStats,
};
use lix_engine::{StorageFactory, StorageFixture, StorageTestConfig};
use rocksdb::Snapshot;
use rocksdb::{BlockBasedOptions, DB, Direction, IteratorMode, Options, WriteBatch};
use tempfile::TempDir;

const OWNED_VALUE_MIN_BYTES: usize = 64 * 1024;

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
    db: Arc<DB>,
}

#[allow(missing_debug_implementations)]
pub struct RocksDBRead<'a> {
    snapshot: Snapshot<'a>,
}

#[allow(missing_debug_implementations)]
pub struct RocksDBWrite {
    db: Arc<DB>,
    batch: WriteBatch,
    staged_put_keys: Vec<Key>,
    stats: WriteStats,
}

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
        let db = Arc::new(open_rocksdb(&path)?);
        Ok(Self { path, db })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        self.db.flush().map_err(rocksdb_error)
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
        _opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        async move {
            Ok(RocksDBRead {
                snapshot: self.db.snapshot(),
            })
        }
    }

    fn begin_write(
        &self,
        _opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        async move {
            Ok(RocksDBWrite {
                db: Arc::clone(&self.db),
                batch: WriteBatch::default(),
                staged_put_keys: Vec::new(),
                stats: WriteStats::default(),
            })
        }
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
                values.push(value.map(|value| project_owned_value(value, opts.projection)));
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
            self.db.write(self.batch).map_err(rocksdb_error)?;
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

fn open_rocksdb(path: &Path) -> Result<DB, StorageError> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_use_fsync(false);
    options.set_write_buffer_size(64 * 1024 * 1024);
    let mut table_options = BlockBasedOptions::default();
    // Full whole-key filters let missing point reads skip unrelated SST data.
    table_options.set_bloom_filter(8.0, false);
    table_options.set_optimize_filters_for_memory(true);
    options.set_block_based_table_factory(&table_options);
    DB::open(&options, path).map_err(rocksdb_error)
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}

fn project_owned_value<T>(value: T, projection: CoreProjection) -> ProjectedValue
where
    T: AsRef<[u8]>,
    Bytes: From<T>,
{
    match projection {
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        CoreProjection::FullValue if value.as_ref().len() >= OWNED_VALUE_MIN_BYTES => {
            ProjectedValue::FullValue(Bytes::from(value))
        }
        CoreProjection::FullValue => {
            ProjectedValue::FullValue(Bytes::copy_from_slice(value.as_ref()))
        }
    }
}

fn rocksdb_error(error: rocksdb::Error) -> StorageError {
    StorageError::Io(format!("rocksdb storage: {error}"))
}
