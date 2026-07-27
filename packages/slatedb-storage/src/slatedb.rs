#![allow(
    clippy::manual_async_fn,
    reason = "explicit future signatures mirror Storage traits and keep Send guarantees visible"
)]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, BoxStream, StreamExt, TryStreamExt};
use lix_engine::storage::{
    CommitResult, CoreProjection, GetManyRequest, GetManyResult, Key, KeyRange, Precondition,
    PreconditionFailure, ProjectedValue, PutBatch, ReadDurability, ReadEntry, ReadOptions,
    ScanChunk, ScanOptions, SpaceId, Storage, StorageError, StorageRead, StorageWrite, StoredValue,
    WriteOptions, WriteStats,
};
use lix_engine::{StorageFactory, StorageFixture, StorageTestConfig};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    Attributes, CopyOptions, Extensions, GetOptions as ObjectStoreGetOptions, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult,
};
use slatedb::config::{
    CompressionCodec, DurabilityLevel, ObjectStoreCacheOptions, ReadOptions as SlateDBReadOptions,
    ScanOptions as SlateDBScanOptions, Settings, WriteOptions as SlateDBWriteOptions,
};
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::db_cache::{DbCache, SplitCache};
use slatedb::{CloseReason, Db, DbIterator, DbSnapshot, DbStatus, WriteBatch};
use tempfile::TempDir;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard, oneshot};

#[cfg(not(unix))]
use std::io::{Read, Seek, SeekFrom};

const DB_PATH: &str = "db";
const LZ4_FORMAT_PATH: &str = "lix-lz4-v1";
const SPACE_PREFIX_LEN: usize = 4;
const MAX_SLATEDB_KEY_LEN: usize = u16::MAX as usize;
const RUNTIME_WORKER_THREADS: usize = 2;
const POINT_READ_CONCURRENCY: usize = 64;
const SNAPSHOT_POINT_CACHE_BYTES: usize = 16 * 1024 * 1024;
const SNAPSHOT_POINT_CACHE_ENTRIES: usize = 4096;
const SNAPSHOT_POINT_CACHE_MAX_VALUE_BYTES: usize = 64 * 1024;
const DEFAULT_BLOCK_CACHE_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_METADATA_CACHE_BYTES: u64 = 4 * 1024 * 1024;
const SCAN_BATCH_ROWS: usize = 1024;
const SCAN_READ_AHEAD_BYTES: usize = 2 * 1024 * 1024;
const SCAN_MAX_FETCH_TASKS: usize = 16;
const SCAN_CACHE_BLOCKS: bool = true;
const OBJECT_STORE_CACHE_PART_SIZE_BYTES: usize = 2 * 1024 * 1024;
const COMPACTOR_COMMIT_INTERVAL: Duration = Duration::from_secs(5);
const LOCAL_SST_FILE_CACHE_ENTRIES: usize = 256;
const LOCAL_SST_CONTENT_CACHE_BYTES: usize = 32 * 1024 * 1024;
const LOCAL_SST_CONTENT_MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug)]
pub struct SlateDBFactory {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct SlateDBFixture {
    path: PathBuf,
}

#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub struct SlateDB {
    path: PathBuf,
    worker: SlateDBWorker,
    write_gate: WriteGate,
    write_pipeline: WritePipeline,
    point_cache: SnapshotPointCache,
}

#[derive(Clone, Debug, Default)]
pub struct SlateDBObjectStoreOptions {
    pub cache: Option<SlateDBCacheOptions>,
}

#[derive(Clone, Debug)]
pub struct SlateDBCacheOptions {
    pub root_folder: PathBuf,
    pub max_disk_cache_bytes: usize,
    pub block_cache_bytes: u64,
    pub metadata_cache_bytes: u64,
}

/// Reads local table ranges on the caller executor.
///
/// `LocalFileSystem::get_opts` schedules file open and metadata work on the
/// blocking pool, and `GetResult::bytes` schedules the actual range read as a
/// second task. `SlateDB::open` already permits local reads on the caller
/// because it owns a dedicated current-thread runtime; doing the short local
/// syscalls there removes both dispatches while preserving the upstream path,
/// range, metadata, ETag, and precondition contracts.
#[derive(Debug)]
struct DirectLocalReads {
    inner: LocalFileSystem,
    files: Mutex<DirectLocalFileCache>,
}

#[derive(Debug, Default)]
struct DirectLocalFileCache {
    entries: HashMap<ObjectPath, DirectLocalFile>,
    eviction_order: VecDeque<ObjectPath>,
    content_bytes: usize,
}

#[derive(Clone, Debug)]
struct DirectLocalFile {
    file: Arc<std::fs::File>,
    size: u64,
    modified: SystemTime,
    e_tag: String,
    contents: Option<Bytes>,
}

impl fmt::Display for DirectLocalReads {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "direct-read {}", self.inner)
    }
}

#[async_trait]
impl ObjectStore for DirectLocalReads {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: ObjectStoreGetOptions,
    ) -> object_store::Result<GetResult> {
        let local_file = self.local_file(location)?;
        let size = local_file.size;
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: local_file.modified.into(),
            size,
            e_tag: Some(local_file.e_tag.clone()),
            version: None,
        };
        options.check_preconditions(&meta)?;
        let range = match options.range {
            Some(range) => range
                .as_range(size)
                .map_err(|source| object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(source),
                })?,
            None => 0..size,
        };
        let bytes = if options.head || range.is_empty() {
            Bytes::new()
        } else if let Some(contents) = &local_file.contents {
            let start =
                usize::try_from(range.start).map_err(|source| object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(source),
                })?;
            let end =
                usize::try_from(range.end).map_err(|source| object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(source),
                })?;
            contents.slice(start..end)
        } else {
            let length = usize::try_from(range.end - range.start).map_err(|source| {
                object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(source),
                }
            })?;
            let mut bytes = vec![0; length];
            direct_local_read_exact_at(&local_file.file, &mut bytes, range.start)
                .map_err(|source| direct_local_io_error(location, source))?;
            Bytes::from(bytes)
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
            meta,
            range,
            attributes: Attributes::default(),
            extensions: Extensions::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

impl DirectLocalReads {
    fn local_file(&self, location: &ObjectPath) -> object_store::Result<DirectLocalFile> {
        let cacheable = Path::new(location.as_ref())
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("sst"));
        if cacheable
            && let Some(file) = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .get(location)
                .cloned()
        {
            return Ok(file);
        }

        let filesystem_path = self.inner.path_to_filesystem(location)?;
        let file = std::fs::File::open(&filesystem_path)
            .map_err(|source| direct_local_io_error(location, source))?;
        let metadata = file
            .metadata()
            .map_err(|source| direct_local_io_error(location, source))?;
        if metadata.is_dir() {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "object is a directory".into(),
            });
        }
        let modified = metadata
            .modified()
            .map_err(|source| direct_local_io_error(location, source))?;
        let contents = if cacheable && metadata.len() <= LOCAL_SST_CONTENT_MAX_FILE_BYTES {
            let length =
                usize::try_from(metadata.len()).map_err(|source| object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(source),
                })?;
            let mut bytes = vec![0; length];
            direct_local_read_exact_at(&file, &mut bytes, 0)
                .map_err(|source| direct_local_io_error(location, source))?;
            Some(Bytes::from(bytes))
        } else {
            None
        };
        let file = DirectLocalFile {
            file: Arc::new(file),
            size: metadata.len(),
            modified,
            e_tag: direct_local_etag(&metadata, modified),
            contents,
        };
        if cacheable {
            let mut cache = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(existing) = cache.entries.get(location) {
                return Ok(existing.clone());
            }
            let content_bytes = file.contents.as_ref().map_or(0, Bytes::len);
            while cache.entries.len() >= LOCAL_SST_FILE_CACHE_ENTRIES
                || cache.content_bytes.saturating_add(content_bytes) > LOCAL_SST_CONTENT_CACHE_BYTES
            {
                let Some(evicted) = cache.eviction_order.pop_front() else {
                    break;
                };
                if let Some(evicted) = cache.entries.remove(&evicted) {
                    cache.content_bytes = cache
                        .content_bytes
                        .saturating_sub(evicted.contents.as_ref().map_or(0, Bytes::len));
                }
            }
            cache.eviction_order.push_back(location.clone());
            cache.content_bytes = cache.content_bytes.saturating_add(content_bytes);
            cache.entries.insert(location.clone(), file.clone());
        }
        Ok(file)
    }
}

fn direct_local_io_error(location: &ObjectPath, source: std::io::Error) -> object_store::Error {
    if source.kind() == std::io::ErrorKind::NotFound {
        object_store::Error::NotFound {
            path: location.to_string(),
            source: Box::new(source),
        }
    } else {
        object_store::Error::Generic {
            store: "LocalFileSystem",
            source: Box::new(source),
        }
    }
}

#[cfg(unix)]
fn direct_local_read_exact_at(
    file: &std::fs::File,
    bytes: &mut [u8],
    offset: u64,
) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(bytes, offset)
}

#[cfg(not(unix))]
fn direct_local_read_exact_at(
    file: &std::fs::File,
    bytes: &mut [u8],
    offset: u64,
) -> std::io::Result<()> {
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(bytes)
}

fn direct_local_etag(metadata: &std::fs::Metadata, modified: SystemTime) -> String {
    #[cfg(unix)]
    let inode = {
        use std::os::unix::fs::MetadataExt;
        metadata.ino()
    };
    #[cfg(not(unix))]
    let inode = 0;
    let modified_micros = modified
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    format!("{inode:x}-{modified_micros:x}-{:x}", metadata.len())
}

#[allow(missing_debug_implementations)]
pub struct SlateDBRead {
    worker: SlateDBWorker,
    write_pipeline: WritePipeline,
    snapshot: Arc<DbSnapshot>,
    publication_view: Option<PublicationView>,
    durability: ReadDurability,
    point_cache: SnapshotPointCache,
}

#[allow(missing_debug_implementations)]
pub struct SlateDBWrite {
    worker: SlateDBWorker,
    write_pipeline: WritePipeline,
    _writer_permit: OwnedMutexGuard<()>,
    await_durable: bool,
    base: Option<Arc<DbSnapshot>>,
    overlay: BTreeMap<Key, Option<Bytes>>,
    stats: WriteStats,
}

/// Bounded values from immutable visible snapshots.
///
/// SlateDB's snapshot sequence is the last committed sequence it exposes, so
/// the pair `(sequence, key)` identifies one point-read view even after newer
/// writes become visible. Keeping values under that key lets independently
/// opened reads reuse hot points without weakening snapshot isolation.
#[derive(Clone)]
struct SnapshotPointCache {
    state: Arc<Mutex<SnapshotPointCacheState>>,
}

#[derive(Default)]
struct SnapshotPointCacheState {
    entries: HashMap<u64, HashMap<Key, SnapshotPointCacheValue>>,
    eviction_order: VecDeque<SnapshotPointCacheKey>,
    used_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SnapshotPointCacheKey {
    sequence: u64,
    key: Key,
}

#[derive(Clone)]
struct SnapshotPointCacheValue {
    value: Option<Bytes>,
    weight: usize,
}

#[derive(Clone, Default)]
struct WritePipeline {
    state: Arc<Mutex<WritePipelineState>>,
}

#[derive(Default)]
struct WritePipelineState {
    tail: Option<Arc<WriteCompletion>>,
    queued: VecDeque<QueuedWrite>,
    draining: bool,
    visible: VecDeque<Arc<PublishedWrite>>,
    point_publications: HashMap<Key, VecDeque<PointPublication>>,
    active_views: BTreeMap<(u64, u64), usize>,
    next_publication_id: u64,
    terminal_error: Option<StorageError>,
}

struct QueuedWrite {
    overlay: Arc<BTreeMap<Key, Option<Bytes>>>,
    published: Arc<PublishedWrite>,
    completion: Arc<WriteCompletion>,
    await_durable: bool,
}

struct PublishedWrite {
    publication_id: u64,
    overlay: Arc<BTreeMap<Key, Option<Bytes>>>,
    persisted_sequence: AtomicU64,
}

struct PointPublication {
    publication_id: u64,
    write: Arc<PublishedWrite>,
    value: Option<Bytes>,
}

struct PublicationView {
    pipeline: WritePipeline,
    snapshot_sequence: u64,
    publication_id: u64,
}

struct WriteCompletion {
    done: AtomicBool,
    result: Mutex<Option<Result<u64, StorageError>>>,
    notify: Notify,
}

const PENDING_WRITE_SEQUENCE: u64 = u64::MAX;

impl WriteCompletion {
    fn new() -> Self {
        Self {
            done: AtomicBool::new(false),
            result: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    fn complete(&self, result: Result<u64, StorageError>) {
        *self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn wait(&self) -> Result<u64, StorageError> {
        loop {
            let notified = self.notify.notified();
            if self.done.load(Ordering::Acquire) {
                return self
                    .result
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
                    .expect("completed SlateDB write has a result");
            }
            notified.await;
        }
    }
}

impl WritePipeline {
    fn new() -> Self {
        Self::default()
    }

    async fn wait_for_visible(&self) -> Result<(), StorageError> {
        let tail = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tail
            .clone();
        if let Some(tail) = tail {
            tail.wait().await?;
        }
        self.terminal_error()
    }

    fn terminal_error(&self) -> Result<(), StorageError> {
        let error = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .terminal_error
            .clone();
        error.map_or(Ok(()), Err)
    }

    fn capture(&self, snapshot_sequence: u64) -> PublicationView {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.visible.front().is_some_and(|write| {
            let persisted = write.persisted_sequence.load(Ordering::Acquire);
            persisted != PENDING_WRITE_SEQUENCE && persisted <= snapshot_sequence
        }) {
            state.visible.pop_front();
        }
        let publication_id = state.next_publication_id;
        *state
            .active_views
            .entry((snapshot_sequence, publication_id))
            .or_default() += 1;
        cleanup_point_publications(&mut state);
        PublicationView {
            pipeline: self.clone(),
            snapshot_sequence,
            publication_id,
        }
    }

    fn visible_writes(
        &self,
        snapshot_sequence: u64,
        publication_id: u64,
    ) -> Vec<Arc<PublishedWrite>> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .visible
            .iter()
            .filter(|write| {
                let persisted = write.persisted_sequence.load(Ordering::Acquire);
                write.publication_id <= publication_id
                    && (persisted == PENDING_WRITE_SEQUENCE || persisted > snapshot_sequence)
            })
            .cloned()
            .collect()
    }

    fn point_value(
        &self,
        snapshot_sequence: u64,
        publication_id: u64,
        key: &Key,
    ) -> Option<Option<Bytes>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .point_publications
            .get(key)?
            .iter()
            .rev()
            .find(|publication| {
                let persisted = publication.write.persisted_sequence.load(Ordering::Acquire);
                publication.publication_id <= publication_id
                    && (persisted == PENDING_WRITE_SEQUENCE || persisted > snapshot_sequence)
            })
            .map(|publication| publication.value.clone())
    }
}

impl Drop for PublicationView {
    fn drop(&mut self) {
        let mut state = self
            .pipeline
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = (self.snapshot_sequence, self.publication_id);
        let remove = state.active_views.get_mut(&key).is_some_and(|count| {
            *count -= 1;
            *count == 0
        });
        if remove {
            state.active_views.remove(&key);
        }
        cleanup_point_publications(&mut state);
    }
}

fn cleanup_point_publications(state: &mut WritePipelineState) {
    let active_views = state.active_views.keys().copied().collect::<Vec<_>>();
    state.point_publications.retain(|_, publications| {
        publications.retain(|publication| {
            let persisted = publication.write.persisted_sequence.load(Ordering::Acquire);
            persisted == PENDING_WRITE_SEQUENCE
                || active_views.iter().any(|(snapshot, captured)| {
                    publication.publication_id <= *captured && persisted > *snapshot
                })
        });
        !publications.is_empty()
    });
}

impl SnapshotPointCache {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(SnapshotPointCacheState::default())),
        }
    }

    /// `Some(None)` is a cached missing point; outer `None` is a cache miss.
    fn get(&self, sequence: u64, key: &Key) -> Option<Option<Bytes>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(&sequence)
            .and_then(|entries| entries.get(key))
            .map(|entry| entry.value.clone())
    }

    /// `Some(None)` is a cached missing point; outer `None` is a cache miss.
    ///
    /// A multi-key read does not mutate recency on hits, so inspect its whole
    /// snapshot-key set under one lock instead of acquiring the cache mutex
    /// once for every requested key.
    fn get_many(&self, sequence: u64, keys: &[Key], values: &mut [Option<Option<Bytes>>]) {
        debug_assert_eq!(keys.len(), values.len());
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entries = state.entries.get(&sequence);
        for (key, value) in keys.iter().zip(values) {
            *value = entries
                .and_then(|entries| entries.get(key))
                .map(|entry| entry.value.clone());
        }
    }

    fn insert(&self, sequence: u64, key: Key, value: Option<Bytes>) {
        // SlateDB values can retain an entire backing block. Copy cacheable
        // values so the cache's byte bound reflects the memory it owns.
        let value = value.map(|value| Bytes::copy_from_slice(&value));
        let value_bytes = value.as_ref().map_or(0, Bytes::len);
        if value_bytes > SNAPSHOT_POINT_CACHE_MAX_VALUE_BYTES {
            return;
        }
        let weight = key.0.len().saturating_add(value_bytes);
        if weight > SNAPSHOT_POINT_CACHE_BYTES {
            return;
        }

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .entries
            .get(&sequence)
            .is_some_and(|entries| entries.contains_key(&key))
        {
            return;
        }
        while state.used_bytes.saturating_add(weight) > SNAPSHOT_POINT_CACHE_BYTES
            || state.eviction_order.len() >= SNAPSHOT_POINT_CACHE_ENTRIES
        {
            let Some(evicted_key) = state.eviction_order.pop_front() else {
                break;
            };
            let (evicted_weight, remove_sequence) = state
                .entries
                .get_mut(&evicted_key.sequence)
                .map_or((None, false), |entries| {
                    let evicted_weight = entries.remove(&evicted_key.key).map(|entry| entry.weight);
                    (evicted_weight, entries.is_empty())
                });
            if let Some(weight) = evicted_weight {
                state.used_bytes = state.used_bytes.saturating_sub(weight);
            }
            if remove_sequence {
                state.entries.remove(&evicted_key.sequence);
            }
        }
        state.used_bytes = state.used_bytes.saturating_add(weight);
        state.eviction_order.push_back(SnapshotPointCacheKey {
            sequence,
            key: key.clone(),
        });
        state
            .entries
            .entry(sequence)
            .or_default()
            .insert(key, SnapshotPointCacheValue { value, weight });
    }
}

impl Default for SlateDBFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SlateDBFactory {
    pub fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create slatedb storage temp dir"),
            next_database_id: AtomicU64::new(0),
        }
    }
}

impl StorageFactory for SlateDBFactory {
    type Storage = SlateDB;
    type Fixture = SlateDBFixture;

    fn create_fixture(&self) -> Self::Fixture {
        let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        let path = self
            .temp_dir
            .path()
            .join(format!("storage-{database_id}.slatedb"));
        SlateDBFixture { path }
    }

    fn config(&self) -> StorageTestConfig {
        StorageTestConfig {
            ephemeral: false,
            supports_concurrent_writers: false,
            ..StorageTestConfig::default()
        }
    }
}

impl StorageFixture for SlateDBFixture {
    type Storage = SlateDB;

    fn open(&self) -> impl Future<Output = Self::Storage> + Send {
        async move { SlateDB::open(&self.path).expect("open slatedb storage") }
    }
}

impl SlateDB {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let path = path.into();
        std::fs::create_dir_all(&path).map_err(|error| {
            StorageError::Io(format!(
                "create slatedb storage directory {}: {error}",
                path.display()
            ))
        })?;
        let object_store: Arc<dyn ObjectStore> = Arc::new(DirectLocalReads {
            inner: LocalFileSystem::new_with_prefix(&path).map_err(object_store_error)?,
            files: Mutex::new(DirectLocalFileCache::default()),
        });
        Self::open_object_store_with_read_dispatch(
            DB_PATH,
            object_store,
            SlateDBObjectStoreOptions::default(),
            true,
        )
        .map(|mut storage| {
            storage.path = path;
            storage
        })
    }

    pub fn open_object_store_with_options(
        db_path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        options: SlateDBObjectStoreOptions,
    ) -> Result<Self, StorageError> {
        Self::open_object_store_with_read_dispatch(db_path, object_store, options, false)
    }

    /// Opens SlateDB with a private current-thread read-dispatch choice.
    ///
    /// `SlateDB::open` is the only caller that enables it: LocalFileSystem
    /// moves filesystem work to Tokio's blocking pool before it can block a
    /// current-thread runtime. Generic ObjectStore implementations do not
    /// promise that property.
    fn open_object_store_with_read_dispatch(
        db_path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        options: SlateDBObjectStoreOptions,
        read_on_caller_current_thread: bool,
    ) -> Result<Self, StorageError> {
        validate_object_store_options(&options)?;
        let db_path = db_path.into();
        Ok(Self {
            worker: SlateDBWorker::start(
                db_path.clone(),
                object_store,
                options,
                read_on_caller_current_thread,
            )?,
            path: PathBuf::from(db_path),
            write_gate: WriteGate::new(),
            write_pipeline: WritePipeline::new(),
            point_cache: SnapshotPointCache::new(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn flush(&self) -> Result<(), StorageError> {
        self.write_pipeline.wait_for_visible().await?;
        self.worker
            .call(|db| async move { db.flush().await.map_err(slatedb_error) })
            .await
    }
}

impl Storage for SlateDB {
    type Read<'a>
        = SlateDBRead
    where
        Self: 'a;

    type Write<'a>
        = SlateDBWrite
    where
        Self: 'a;

    fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        async move {
            self.write_pipeline.terminal_error()?;
            let snapshot = self
                .worker
                .call_read(|db| async move { db.snapshot().await.map_err(slatedb_error) })
                .await?;
            let publication_view = if opts.durability == ReadDurability::Visible {
                Some(self.write_pipeline.capture(snapshot.seq()))
            } else {
                None
            };
            self.write_pipeline.terminal_error()?;
            Ok(SlateDBRead {
                worker: self.worker.clone(),
                write_pipeline: self.write_pipeline.clone(),
                snapshot,
                publication_view,
                durability: opts.durability,
                point_cache: self.point_cache.clone(),
            })
        }
    }

    fn begin_write(
        &self,
        opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        async move {
            let writer_permit = self.write_gate.acquire().await;
            check_preconditions(
                &self.worker,
                &self.write_pipeline,
                &self.point_cache,
                &opts.preconditions,
            )
            .await?;
            Ok(SlateDBWrite {
                worker: self.worker.clone(),
                write_pipeline: self.write_pipeline.clone(),
                _writer_permit: writer_permit,
                // The engine sets this only for the atomic mutation plus
                // idempotency-receipt commit. Its replay contract requires a
                // durable receipt before the request can be acknowledged.
                await_durable: opts.idempotency_key.is_some(),
                base: None,
                overlay: BTreeMap::new(),
                stats: WriteStats::default(),
            })
        }
    }
}

async fn check_preconditions(
    worker: &SlateDBWorker,
    write_pipeline: &WritePipeline,
    point_cache: &SnapshotPointCache,
    preconditions: &[Precondition],
) -> Result<(), StorageError> {
    if preconditions.is_empty() {
        return Ok(());
    }
    let preconditions = preconditions.to_vec();
    let write_pipeline = write_pipeline.clone();
    let read_pipeline = write_pipeline.clone();
    let point_cache = point_cache.clone();
    let matches = worker
        .call_read(move |db| async move {
            let snapshot = db.snapshot().await.map_err(slatedb_error)?;
            let snapshot_sequence = snapshot.seq();
            let publication_view = read_pipeline.capture(snapshot_sequence);
            let publication_id = publication_view.publication_id;
            let mut matches = Vec::with_capacity(preconditions.len());
            let mut index = 0;
            while index < preconditions.len() {
                let start = index;
                let mut point_keys = Vec::new();
                while index < preconditions.len() {
                    let Some(key) = point_precondition_physical_key(&preconditions[index])? else {
                        break;
                    };
                    point_keys.push(key);
                    index += 1;
                }

                if !point_keys.is_empty() {
                    // A tracked mutation normally supplies a branch-head and a
                    // revision predicate (and idempotent mutations add a
                    // receipt predicate). Evaluate each contiguous point run
                    // against this snapshot in one read operation rather than
                    // serializing a worker entry for every predicate.
                    let mut values = get_cached_snapshot_values(
                        Arc::clone(&snapshot),
                        point_keys.clone(),
                        &point_cache,
                    )
                    .await?;
                    for (index, key) in point_keys.iter().enumerate() {
                        if let Some(value) =
                            read_pipeline.point_value(snapshot_sequence, publication_id, key)
                        {
                            values[index] = value;
                        }
                    }
                    matches.extend(values.iter().enumerate().map(|(offset, value)| {
                        point_precondition_matches(&preconditions[start + offset], value.as_ref())
                    }));
                    continue;
                }

                let matches_precondition = match &preconditions[index] {
                    Precondition::RangeEmpty { space, range } => {
                        let range = physical_range(*space, range.clone())?;
                        let bounds = EncodedBounds::new(range.clone(), None);
                        let mut keys = collect_snapshot_keys(Arc::clone(&snapshot), bounds).await?;
                        let visible_writes =
                            read_pipeline.visible_writes(snapshot_sequence, publication_id);
                        for write in &visible_writes {
                            for (key, value) in &*write.overlay {
                                if range_contains_key(&range, key) {
                                    if value.is_some() {
                                        if !keys.contains(key) {
                                            keys.push(key.clone());
                                        }
                                    } else {
                                        keys.retain(|candidate| candidate != key);
                                    }
                                }
                            }
                        }
                        keys.is_empty()
                    }
                    Precondition::BranchEquals { .. } => false,
                    Precondition::KeyAbsent { .. }
                    | Precondition::KeyPresent { .. }
                    | Precondition::KeyValueHashEquals { .. }
                    | Precondition::KeyValueEquals { .. } => {
                        unreachable!("point preconditions are collected above")
                    }
                };
                matches.push(matches_precondition);
                index += 1;
            }
            Ok(matches)
        })
        .await?;
    write_pipeline.terminal_error()?;
    let failures = matches
        .into_iter()
        .enumerate()
        .filter_map(|(index, matches)| (!matches).then_some(PreconditionFailure { index }))
        .collect::<Vec<_>>();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(StorageError::PreconditionFailed(failures))
    }
}

async fn get_cached_snapshot_values(
    snapshot: Arc<DbSnapshot>,
    keys: Vec<Key>,
    point_cache: &SnapshotPointCache,
) -> Result<Vec<Option<Bytes>>, StorageError> {
    let sequence = snapshot.seq();
    let mut values = vec![None; keys.len()];
    point_cache.get_many(sequence, &keys, &mut values);
    let missing = keys
        .iter()
        .enumerate()
        .filter_map(|(index, key)| values[index].is_none().then_some((index, key.clone())))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        let missing_keys = missing
            .iter()
            .map(|(_, key)| key.clone())
            .collect::<Vec<_>>();
        let fetched = get_snapshot_values(snapshot, missing_keys, ReadDurability::Visible).await?;
        for ((index, key), value) in missing.into_iter().zip(fetched) {
            point_cache.insert(sequence, key, value.clone());
            values[index] = Some(value);
        }
    }
    Ok(values
        .into_iter()
        .map(|value| value.expect("all SlateDB point-cache misses are filled"))
        .collect())
}

fn point_precondition_physical_key(
    precondition: &Precondition,
) -> Result<Option<Key>, StorageError> {
    match precondition {
        Precondition::KeyAbsent { space, key }
        | Precondition::KeyPresent { space, key }
        | Precondition::KeyValueHashEquals { space, key, .. }
        | Precondition::KeyValueEquals { space, key, .. } => physical_key(*space, key).map(Some),
        Precondition::RangeEmpty { .. } | Precondition::BranchEquals { .. } => Ok(None),
    }
}

fn point_precondition_matches(precondition: &Precondition, value: Option<&Bytes>) -> bool {
    match precondition {
        Precondition::KeyAbsent { .. } => value.is_none(),
        Precondition::KeyPresent { .. } => value.is_some(),
        Precondition::KeyValueHashEquals { hash, .. } => {
            value.is_some_and(|value| blake3::hash(value.as_ref()).as_bytes() == hash)
        }
        Precondition::KeyValueEquals { expected, .. } => {
            value.is_some_and(|value| value == expected)
        }
        Precondition::RangeEmpty { .. } | Precondition::BranchEquals { .. } => {
            unreachable!("only point preconditions have batched snapshot values")
        }
    }
}

impl StorageRead for SlateDBRead {
    fn snapshot_cache_key(&self) -> Option<u128> {
        let publication_id = self
            .publication_view
            .as_ref()
            .map_or(0, |view| view.publication_id);
        Some((u128::from(self.snapshot.seq()) << 64) | u128::from(publication_id))
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        async move {
            self.write_pipeline.terminal_error()?;
            if let [request] = requests
                && let [key] = request.keys
            {
                let key = physical_key(request.space, key)?;
                let snapshot = Arc::clone(&self.snapshot);
                let durability = self.durability;
                let mut value = if durability == ReadDurability::Visible {
                    let sequence = snapshot.seq();
                    let cache = self.point_cache.clone();
                    if let Some(value) = cache.get(sequence, &key) {
                        self.worker.check_open_fast()?;
                        value
                    } else {
                        let fetched_key = key.clone();
                        let value = self
                            .worker
                            .call_read(move |_db| {
                                get_snapshot_value(snapshot, fetched_key, durability)
                            })
                            .await?;
                        cache.insert(sequence, key.clone(), value.clone());
                        value
                    }
                } else {
                    let read_key = key.clone();
                    self.worker
                        .call_read(move |_db| get_snapshot_value(snapshot, read_key, durability))
                        .await?
                };
                if let Some(view) = &self.publication_view
                    && let Some(published) = self.write_pipeline.point_value(
                        view.snapshot_sequence,
                        view.publication_id,
                        &key,
                    )
                {
                    value = published;
                }
                return Ok(GetManyResult::new(vec![
                    value.map(|value| project_value(value, request.opts.projection)),
                ]));
            }

            let mut physical_keys = Vec::with_capacity(
                requests
                    .iter()
                    .map(|request| request.keys.len())
                    .sum::<usize>(),
            );
            for request in requests {
                for key in request.keys {
                    physical_keys.push(physical_key(request.space, key)?);
                }
            }
            if physical_keys.is_empty() {
                return Ok(GetManyResult::new(Vec::new()));
            }

            let snapshot = Arc::clone(&self.snapshot);
            let durability = self.durability;
            let mut values = if durability == ReadDurability::Visible {
                let sequence = snapshot.seq();
                let cache = self.point_cache.clone();
                let mut values = vec![None; physical_keys.len()];
                let mut missing = Vec::new();
                cache.get_many(sequence, &physical_keys, &mut values);
                for (index, key) in physical_keys.iter().enumerate() {
                    if values[index].is_none() {
                        missing.push((index, key.clone()));
                    }
                }
                if missing.is_empty() {
                    self.worker.check_open_fast()?;
                } else {
                    let missing_keys = missing
                        .iter()
                        .map(|(_, key)| key.clone())
                        .collect::<Vec<_>>();
                    let fetched = self
                        .worker
                        .call_read(move |_db| {
                            get_snapshot_values(snapshot, missing_keys, durability)
                        })
                        .await?;
                    for ((index, key), value) in missing.into_iter().zip(fetched) {
                        cache.insert(sequence, key, value.clone());
                        values[index] = Some(value);
                    }
                }
                values
                    .into_iter()
                    .map(|value| value.expect("all SlateDB batch point-cache misses are filled"))
                    .collect::<Vec<_>>()
            } else {
                let read_keys = physical_keys.clone();
                self.worker
                    .call_read(move |_db| get_snapshot_values(snapshot, read_keys, durability))
                    .await?
            };
            for (key, value) in physical_keys.iter().zip(&mut values) {
                if let Some(view) = &self.publication_view
                    && let Some(published) = self.write_pipeline.point_value(
                        view.snapshot_sequence,
                        view.publication_id,
                        key,
                    )
                {
                    *value = published;
                }
            }

            let mut values = values.into_iter();
            let mut results = Vec::with_capacity(physical_keys.len());
            for request in requests {
                results.extend(
                    values.by_ref().take(request.keys.len()).map(|value| {
                        value.map(|value| project_value(value, request.opts.projection))
                    }),
                );
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
            self.write_pipeline.terminal_error()?;
            if opts.page_size() == 0 {
                return Ok(ScanChunk {
                    entries: Vec::new(),
                    has_more: false,
                });
            }

            let range = physical_range(space, range)?;
            let resume_after = opts
                .resume_after
                .as_ref()
                .map(|key| physical_key(space, key))
                .transpose()?;
            let bounds = EncodedBounds::new(range, resume_after.as_ref());
            if bounds.is_empty() {
                return Ok(ScanChunk {
                    entries: Vec::new(),
                    has_more: false,
                });
            }

            let snapshot = Arc::clone(&self.snapshot);
            let durability = self.durability;
            let visible_writes = self
                .publication_view
                .as_ref()
                .map_or_else(Vec::new, |view| {
                    self.write_pipeline
                        .visible_writes(view.snapshot_sequence, view.publication_id)
                });
            if !visible_writes.is_empty() {
                let page_size = opts.page_size();
                let projection = opts.projection;
                return self
                    .worker
                    .call_read(move |_db| {
                        scan_snapshot_with_writes(
                            snapshot,
                            bounds,
                            durability,
                            visible_writes,
                            page_size,
                            projection,
                        )
                    })
                    .await;
            }
            let mut iter = Some(
                self.worker
                    .call_read(move |_db| open_snapshot_scan(snapshot, bounds, durability))
                    .await?,
            );
            let mut all_entries = Vec::with_capacity(opts.page_size());

            loop {
                let remaining = opts.page_size() - all_entries.len();
                let batch_limit = remaining.min(SCAN_BATCH_ROWS);
                let lookahead = batch_limit == remaining;
                let current_iter = iter
                    .take()
                    .expect("slatedb scan iterator is present until scan returns");
                let projection = opts.projection;
                let batch = self
                    .worker
                    .call_read(move |_db| {
                        scan_snapshot_batch(current_iter, batch_limit, projection, lookahead)
                    })
                    .await?;
                let ScanBatch {
                    iter: next_iter,
                    entries,
                    state,
                } = batch;

                all_entries.extend(
                    entries
                        .into_iter()
                        .map(|(key, value)| ReadEntry { key, value }),
                );

                match state {
                    ScanBatchState::Exhausted => {
                        return Ok(ScanChunk {
                            entries: all_entries,
                            has_more: false,
                        });
                    }
                    ScanBatchState::HasMore => {
                        return Ok(ScanChunk {
                            entries: all_entries,
                            has_more: true,
                        });
                    }
                    ScanBatchState::MoreUnknown => iter = Some(next_iter),
                }
            }
        }
    }
}

impl StorageWrite for SlateDBWrite {
    fn put_many(
        &mut self,
        space: SpaceId,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            for entry in entries.entries {
                let key = physical_key(space, &entry.key)?;
                let value = stored_value_bytes(entry.value);
                self.stats.put_entries += 1;
                self.stats.written_bytes += value.len() as u64;
                self.overlay.insert(key, Some(value));
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
                self.overlay.insert(physical_key(space, key)?, None);
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
            let range = physical_range(space, range)?;
            let bounds = EncodedBounds::new(range.clone(), None);
            if bounds.is_empty() {
                self.stats.deleted_ranges += 1;
                self.stats.storage_calls += 1;
                return Ok(());
            }

            // Snapshot discovery is read-only until both awaits complete and
            // the overlay is updated below, so a cancelled caller can safely
            // release this work instead of holding worker shutdown open.
            if self.base.is_none() {
                self.write_pipeline.wait_for_visible().await?;
                self.base = Some(
                    self.worker
                        .call_read(|db| async move { db.snapshot().await.map_err(slatedb_error) })
                        .await?,
                );
            }
            let base = Arc::clone(
                self.base
                    .as_ref()
                    .expect("SlateDB write base snapshot is initialized"),
            );
            let base_keys = self
                .worker
                .call_read(move |_db| collect_snapshot_keys(base, bounds))
                .await?;

            let overlay_keys = self
                .overlay
                .keys()
                .filter(|key| range_contains_key(&range, key))
                .cloned()
                .collect::<Vec<_>>();
            let staged_puts_in_range = overlay_keys
                .iter()
                .filter(|key| self.overlay.get(*key).is_some_and(Option::is_some))
                .count();

            for key in overlay_keys.into_iter().chain(base_keys.iter().cloned()) {
                self.overlay.insert(key, None);
            }

            self.stats.deleted_entries += (base_keys.len() + staged_puts_in_range) as u64;
            self.stats.deleted_ranges += 1;
            self.stats.storage_calls += 1;
            Ok(())
        }
    }

    fn commit(self) -> impl Future<Output = Result<CommitResult, StorageError>> + Send {
        async move {
            let Self {
                worker,
                write_pipeline,
                _writer_permit: writer_permit,
                await_durable,
                overlay,
                stats,
                ..
            } = self;
            if overlay.is_empty() {
                return Ok(CommitResult {
                    commit_id: None,
                    stats,
                });
            }

            worker.check_open()?;
            write_pipeline.terminal_error()?;
            let overlay = Arc::new(overlay);
            let completion = Arc::new(WriteCompletion::new());
            let start_drainer = {
                let mut state = write_pipeline
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.next_publication_id = state
                    .next_publication_id
                    .checked_add(1)
                    .expect("SlateDB publication id overflow");
                let publication_id = state.next_publication_id;
                let published = Arc::new(PublishedWrite {
                    publication_id,
                    overlay: Arc::clone(&overlay),
                    persisted_sequence: AtomicU64::new(PENDING_WRITE_SEQUENCE),
                });
                state.tail = Some(Arc::clone(&completion));
                state.visible.push_back(Arc::clone(&published));
                for (key, value) in &*overlay {
                    state
                        .point_publications
                        .entry(key.clone())
                        .or_default()
                        .push_back(PointPublication {
                            publication_id,
                            write: Arc::clone(&published),
                            value: value.clone(),
                        });
                }
                state.queued.push_back(QueuedWrite {
                    overlay,
                    published,
                    completion: Arc::clone(&completion),
                    await_durable,
                });
                let start_drainer = !state.draining;
                state.draining = true;
                start_drainer
            };

            drop(writer_permit);
            if start_drainer {
                let task_pipeline = write_pipeline.clone();
                worker.spawn(move |db| drain_write_queue(db, task_pipeline));
            }

            // The writer gate protects precondition evaluation plus publication
            // into the ordered adapter pipeline. Once published, later writers
            // observe this overlay without waiting for SlateDB's task rendezvous.
            if await_durable {
                completion.wait().await?;
            }
            Ok(CommitResult {
                commit_id: None,
                stats,
            })
        }
    }

    fn rollback(self) -> impl Future<Output = Result<(), StorageError>> + Send {
        async { Ok(()) }
    }
}

async fn drain_write_queue(db: Arc<Db>, pipeline: WritePipeline) {
    loop {
        let writes = {
            let mut state = pipeline
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.queued.is_empty() {
                state.draining = false;
                return;
            }
            state.queued.drain(..).collect::<Vec<_>>()
        };

        let prior_error = pipeline.terminal_error();
        let result = if let Err(error) = prior_error {
            Err(error)
        } else {
            let mut batch = WriteBatch::new();
            let await_durable = writes.iter().any(|write| write.await_durable);
            for write in &writes {
                for (key, value) in &*write.overlay {
                    match value {
                        Some(value) => batch.put_bytes(key.0.clone(), value.clone()),
                        None => batch.delete(key.0.clone()),
                    }
                }
            }
            db.write_with_options(
                batch,
                &SlateDBWriteOptions {
                    await_durable,
                    ..SlateDBWriteOptions::default()
                },
            )
            .await
            .map(|handle| handle.seqnum())
            .map_err(slatedb_error)
            .map_err(commit_outcome_unknown)
        };

        if let Err(error) = &result {
            pipeline
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .terminal_error = Some(error.clone());
        }
        for write in writes {
            if let Ok(sequence) = &result {
                write
                    .published
                    .persisted_sequence
                    .store(*sequence, Ordering::Release);
                write.completion.complete(Ok(*sequence));
            } else {
                write.completion.complete(result.clone());
            }
        }
    }
}

#[derive(Clone)]
#[allow(missing_debug_implementations)]
struct SlateDBWorker {
    inner: Arc<SlateDBWorkerInner>,
}

#[allow(missing_debug_implementations)]
struct SlateDBWorkerInner {
    runtime: Handle,
    db: Arc<Db>,
    status: tokio::sync::watch::Receiver<DbStatus>,
    read_on_caller_current_thread: bool,
    in_flight: InFlightTracker,
    shutdown: mpsc::Sender<()>,
    manager: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone, Debug, Default)]
struct InFlightTracker {
    state: Arc<(Mutex<usize>, Condvar)>,
}

struct InFlightGuard {
    state: Arc<(Mutex<usize>, Condvar)>,
}

impl InFlightTracker {
    fn enter(&self) -> InFlightGuard {
        let mut active = self
            .state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active += 1;
        drop(active);
        InFlightGuard {
            state: Arc::clone(&self.state),
        }
    }

    fn wait_until_idle(&self) {
        let (active, idle) = &*self.state;
        let mut active = active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while *active != 0 {
            active = idle
                .wait(active)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let (active, idle) = &*self.state;
        let mut active = active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active = active
            .checked_sub(1)
            .expect("SlateDB in-flight operation count should be balanced");
        if *active == 0 {
            idle.notify_all();
        }
    }
}

impl SlateDBWorker {
    fn start(
        db_path: String,
        object_store: Arc<dyn ObjectStore>,
        options: SlateDBObjectStoreOptions,
        read_on_caller_current_thread: bool,
    ) -> Result<Self, StorageError> {
        let in_flight = InFlightTracker::default();
        let manager_in_flight = in_flight.clone();
        let (shutdown, shutdown_rx) = mpsc::channel();
        let (opened_tx, opened_rx) = mpsc::channel::<Result<(Handle, Arc<Db>), StorageError>>();
        let thread = std::thread::Builder::new()
            .name("lix-slatedb-manager".to_string())
            .spawn(move || {
                run_slatedb_manager(
                    db_path,
                    object_store,
                    options,
                    shutdown_rx,
                    opened_tx,
                    manager_in_flight,
                );
            })
            .map_err(|error| StorageError::Io(format!("spawn slatedb worker: {error}")))?;

        match opened_rx
            .recv()
            .map_err(|error| StorageError::Io(format!("slatedb worker did not open: {error}")))?
        {
            Ok((runtime, db)) => {
                let status = db.subscribe();
                Ok(Self {
                    inner: Arc::new(SlateDBWorkerInner {
                        runtime,
                        db,
                        status,
                        read_on_caller_current_thread,
                        in_flight,
                        shutdown,
                        manager: Mutex::new(Some(thread)),
                    }),
                })
            }
            Err(error) => {
                let _ = thread.join();
                Err(error)
            }
        }
    }

    fn spawn<F, Fut>(&self, operation: F)
    where
        F: FnOnce(Arc<Db>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let in_flight = self.inner.in_flight.enter();
        let db = Arc::clone(&self.inner.db);
        self.inner.runtime.spawn(async move {
            let _in_flight = in_flight;
            operation(db).await;
        });
    }

    fn check_open(&self) -> Result<(), StorageError> {
        match self.inner.db.status().close_reason {
            None => Ok(()),
            Some(CloseReason::Fenced) => Err(StorageError::Fenced),
            Some(reason) => Err(StorageError::Closed(format!("slatedb closed: {reason:?}"))),
        }
    }

    fn check_open_fast(&self) -> Result<(), StorageError> {
        let status = self.inner.status.borrow();
        match status.close_reason.as_ref() {
            None => Ok(()),
            Some(&CloseReason::Fenced) => Err(StorageError::Fenced),
            Some(reason) => Err(StorageError::Closed(format!("slatedb closed: {reason:?}"))),
        }
    }

    /// Runs an operation that must retain completion semantics after its caller
    /// is dropped.
    ///
    /// Mutating operations and flushes use this path so a cancelled caller
    /// cannot turn an already-started publication or durability operation into
    /// an ambiguous outcome. Read-only work uses [`Self::call_read`] instead.
    async fn call<R, F, Fut>(&self, operation: F) -> Result<R, StorageError>
    where
        R: Send + 'static,
        F: FnOnce(Arc<Db>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, StorageError>> + Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        // Manager shutdown waits for this guard. The guard is deliberately
        // independent of `SlateDBWorkerInner`: keeping the inner Arc in a task
        // running on its own runtime would make its synchronous manager join
        // self-deadlock when the task released the final Arc.
        let in_flight = self.inner.in_flight.enter();
        let db = Arc::clone(&self.inner.db);
        self.inner.runtime.spawn(async move {
            let _in_flight = in_flight;
            let result = operation(db).await;
            let _ = reply_tx.send(result);
        });
        reply_rx
            .await
            .map_err(|error| StorageError::Io(format!("receive slatedb worker reply: {error}")))?
    }

    /// Runs a read operation which can be safely abandoned with its caller.
    ///
    /// Writes and flushes deliberately continue through [`Self::call`]: after
    /// a caller drops its future, letting a mutating operation finish preserves
    /// a single, well-defined publication and durability outcome. Reads have
    /// no such side effects, so run them on the caller's multithreaded
    /// executor. That keeps SlateDB's own async work local to the request and
    /// avoids a manager-task spawn plus oneshot round trip for every snapshot,
    /// point read, and scan page. A current-thread runtime uses the same path
    /// only for [`SlateDB::open`]'s LocalFileSystem, which moves filesystem
    /// work to Tokio's blocking pool before it can block the executor. Generic
    /// ObjectStores may synchronously work before their first yield, so they
    /// keep using the manager there.
    /// Canceling either path drops the read future and the in-flight guard that
    /// manager shutdown waits on.
    async fn call_read<R, F, Fut>(&self, operation: F) -> Result<R, StorageError>
    where
        R: Send + 'static,
        F: FnOnce(Arc<Db>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, StorageError>> + Send + 'static,
    {
        let caller_can_run_read = matches!(
            Handle::try_current(),
            Ok(handle)
                if self.inner.read_on_caller_current_thread
                    || handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
        );
        if !caller_can_run_read {
            return self.call_read_on_manager(operation).await;
        }
        // Manager shutdown waits for this guard. The guard is deliberately
        // independent of `SlateDBWorkerInner`: the operation may retain only
        // the database Arc while the last storage handle is being dropped.
        // Keeping the guard in this caller future prevents the synchronous
        // manager close from racing that operation.
        let _in_flight = self.inner.in_flight.enter();
        operation(Arc::clone(&self.inner.db)).await
    }

    async fn call_read_on_manager<R, F, Fut>(&self, operation: F) -> Result<R, StorageError>
    where
        R: Send + 'static,
        F: FnOnce(Arc<Db>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, StorageError>> + Send + 'static,
    {
        let (mut reply_tx, reply_rx) = oneshot::channel();
        // Manager shutdown waits for this guard. The guard is deliberately
        // independent of `SlateDBWorkerInner`: keeping the inner Arc in a task
        // running on its own runtime would make its synchronous manager join
        // self-deadlock when the task released the final Arc.
        let in_flight = self.inner.in_flight.enter();
        let db = Arc::clone(&self.inner.db);
        self.inner.runtime.spawn(async move {
            let _in_flight = in_flight;
            let result = tokio::select! {
                biased;
                () = reply_tx.closed() => None,
                result = operation(db) => Some(result),
            };
            if let Some(result) = result {
                let _ = reply_tx.send(result);
            }
        });
        reply_rx
            .await
            .map_err(|error| StorageError::Io(format!("receive slatedb worker reply: {error}")))?
    }
}

impl Drop for SlateDBWorkerInner {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
        let Ok(mut manager) = self.manager.lock() else {
            return;
        };
        if let Some(manager) = manager.take() {
            let _ = manager.join();
        }
    }
}

fn run_slatedb_manager(
    db_path: String,
    object_store: Arc<dyn ObjectStore>,
    options: SlateDBObjectStoreOptions,
    shutdown: mpsc::Receiver<()>,
    opened: mpsc::Sender<Result<(Handle, Arc<Db>), StorageError>>,
    in_flight: InFlightTracker,
) {
    let runtime = match Builder::new_multi_thread()
        .worker_threads(RUNTIME_WORKER_THREADS)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = opened.send(Err(StorageError::Io(format!(
                "create slatedb runtime: {error}"
            ))));
            return;
        }
    };

    let db = match open_slatedb(&runtime, db_path, object_store, options) {
        Ok(db) => db,
        Err(error) => {
            let _ = opened.send(Err(error));
            return;
        }
    };

    let db = Arc::new(db);
    if opened
        .send(Ok((runtime.handle().clone(), Arc::clone(&db))))
        .is_err()
    {
        let _ = runtime.block_on(db.close());
        return;
    }
    let _ = shutdown.recv();
    in_flight.wait_until_idle();
    let _ = runtime.block_on(db.close());
}

fn open_slatedb(
    runtime: &Runtime,
    db_path: String,
    object_store: Arc<dyn ObjectStore>,
    options: SlateDBObjectStoreOptions,
) -> Result<Db, StorageError> {
    runtime.block_on(async move {
        let physical_db_path = join_db_path(&db_path, LZ4_FORMAT_PATH);
        let mut builder = Db::builder(physical_db_path, object_store);
        let mut settings = slatedb_settings();
        if let Some(cache) = options.cache {
            settings.object_store_cache_options = ObjectStoreCacheOptions {
                root_folder: Some(cache.root_folder),
                max_cache_size_bytes: Some(cache.max_disk_cache_bytes),
                part_size_bytes: OBJECT_STORE_CACHE_PART_SIZE_BYTES,
                cache_puts: true,
                preload_disk_cache_on_startup: None,
                scan_interval: None,
                ..ObjectStoreCacheOptions::default()
            };
            builder = builder.with_settings(settings).with_db_cache(db_cache(
                cache.block_cache_bytes,
                cache.metadata_cache_bytes,
            ));
        } else {
            // Keep the default bounded instead of accepting SlateDB's much
            // larger cache defaults. This captures hot SST blocks and
            // metadata for normal default reads without enabling the optional
            // disk object cache.
            builder = builder.with_settings(settings).with_db_cache(db_cache(
                DEFAULT_BLOCK_CACHE_BYTES,
                DEFAULT_METADATA_CACHE_BYTES,
            ));
        }
        builder.build().await.map_err(slatedb_error)
    })
}

fn join_db_path(db_path: &str, child: &str) -> String {
    let db_path = db_path.trim_end_matches('/');
    if db_path.is_empty() {
        child.to_string()
    } else {
        format!("{db_path}/{child}")
    }
}

fn slatedb_settings() -> Settings {
    let mut settings = Settings {
        compression_codec: Some(CompressionCodec::Lz4),
        ..Settings::default()
    };
    settings
        .compactor_options
        .as_mut()
        .expect("default SlateDB settings enable compaction")
        .commit_compacted_interval = COMPACTOR_COMMIT_INTERVAL;
    settings
}

fn validate_object_store_options(options: &SlateDBObjectStoreOptions) -> Result<(), StorageError> {
    let Some(cache) = &options.cache else {
        return Ok(());
    };
    if cache.root_folder.as_os_str().is_empty() {
        return Err(StorageError::Io(
            "slatedb cache root folder must not be empty".to_string(),
        ));
    }
    if cache.max_disk_cache_bytes == 0 {
        return Err(StorageError::Io(
            "slatedb disk cache size must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn moka_cache(capacity: u64) -> Option<Arc<dyn DbCache>> {
    if capacity == 0 {
        return None;
    }
    Some(Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
        max_capacity: capacity,
        time_to_live: None,
        time_to_idle: None,
    })))
}

fn db_cache(block_cache_bytes: u64, metadata_cache_bytes: u64) -> Arc<dyn DbCache> {
    Arc::new(
        SplitCache::new()
            .with_block_cache(moka_cache(block_cache_bytes))
            .with_meta_cache(moka_cache(metadata_cache_bytes))
            .build(),
    )
}

fn physical_key(space: SpaceId, key: &Key) -> Result<Key, StorageError> {
    let len = SPACE_PREFIX_LEN + key.0.len();
    if len > MAX_SLATEDB_KEY_LEN {
        return Err(StorageError::InvalidKey);
    }
    let mut bytes = Vec::with_capacity(len);
    bytes.extend_from_slice(&space.0.to_be_bytes());
    bytes.extend_from_slice(&key.0);
    Ok(Key(Bytes::from(bytes)))
}

fn physical_range(space: SpaceId, range: KeyRange) -> Result<KeyRange, StorageError> {
    let map = |bound: Bound<Key>, unbounded: Bound<Key>| -> Result<Bound<Key>, StorageError> {
        Ok(match bound {
            Bound::Included(key) => Bound::Included(physical_key(space, &key)?),
            Bound::Excluded(key) => Bound::Excluded(physical_key(space, &key)?),
            Bound::Unbounded => unbounded,
        })
    };
    Ok(KeyRange {
        lower: map(
            range.lower,
            Bound::Included(Key(Bytes::copy_from_slice(&space.0.to_be_bytes()))),
        )?,
        upper: map(
            range.upper,
            space.0.checked_add(1).map_or(Bound::Unbounded, |next| {
                Bound::Excluded(Key(Bytes::copy_from_slice(&next.to_be_bytes())))
            }),
        )?,
    })
}

#[derive(Clone, Debug)]
struct EncodedBounds {
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
        Self { lower, upper }
    }

    fn is_empty(&self) -> bool {
        bounds_are_empty(&self.lower, &self.upper)
    }

    fn range(&self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        (self.lower.clone(), self.upper.clone())
    }

    fn contains(&self, key: &Key) -> bool {
        let key = key.0.as_ref();
        let above_lower = match &self.lower {
            Bound::Included(lower) => key >= lower.as_slice(),
            Bound::Excluded(lower) => key > lower.as_slice(),
            Bound::Unbounded => true,
        };
        let below_upper = match &self.upper {
            Bound::Included(upper) => key <= upper.as_slice(),
            Bound::Excluded(upper) => key < upper.as_slice(),
            Bound::Unbounded => true,
        };
        above_lower && below_upper
    }
}

async fn get_snapshot_values(
    snapshot: Arc<DbSnapshot>,
    keys: Vec<Key>,
    durability: ReadDurability,
) -> Result<Vec<Option<Bytes>>, StorageError> {
    let read_options = slatedb_read_options(durability);
    stream::iter(keys)
        .map(|key| {
            let snapshot = Arc::clone(&snapshot);
            let read_options = read_options.clone();
            async move {
                snapshot
                    .get_with_options(key.0, &read_options)
                    .await
                    .map_err(slatedb_error)
            }
        })
        .buffered(POINT_READ_CONCURRENCY)
        .try_collect()
        .await
}

async fn get_snapshot_value(
    snapshot: Arc<DbSnapshot>,
    key: Key,
    durability: ReadDurability,
) -> Result<Option<Bytes>, StorageError> {
    let read_options = slatedb_read_options(durability);
    snapshot
        .get_with_options(key.0, &read_options)
        .await
        .map_err(slatedb_error)
}

struct ScanBatch {
    iter: DbIterator,
    entries: Vec<(Key, ProjectedValue)>,
    state: ScanBatchState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanBatchState {
    Exhausted,
    MoreUnknown,
    HasMore,
}

async fn open_snapshot_scan(
    snapshot: Arc<DbSnapshot>,
    bounds: EncodedBounds,
    durability: ReadDurability,
) -> Result<DbIterator, StorageError> {
    let scan_options = slatedb_scan_options(durability);
    snapshot
        .scan_with_options(bounds.range(), &scan_options)
        .await
        .map_err(slatedb_error)
}

async fn scan_snapshot_batch(
    mut iter: DbIterator,
    limit_rows: usize,
    projection: CoreProjection,
    lookahead: bool,
) -> Result<ScanBatch, StorageError> {
    let mut entries = Vec::with_capacity(limit_rows);
    while entries.len() < limit_rows {
        let Some(row) = iter.next().await.map_err(slatedb_error)? else {
            return Ok(ScanBatch {
                iter,
                entries,
                state: ScanBatchState::Exhausted,
            });
        };
        if row.key.len() < SPACE_PREFIX_LEN {
            return Err(StorageError::Corruption(format!(
                "slatedb key was shorter than space prefix: {:?}",
                row.key
            )));
        }
        let key = Key(Bytes::copy_from_slice(&row.key[SPACE_PREFIX_LEN..]));
        let value = match projection {
            CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
            CoreProjection::FullValue => ProjectedValue::FullValue(row.value),
        };
        entries.push((key, value));
    }

    let state = if lookahead {
        if iter.next().await.map_err(slatedb_error)?.is_some() {
            ScanBatchState::HasMore
        } else {
            ScanBatchState::Exhausted
        }
    } else {
        ScanBatchState::MoreUnknown
    };
    Ok(ScanBatch {
        iter,
        entries,
        state,
    })
}

async fn scan_snapshot_with_writes(
    snapshot: Arc<DbSnapshot>,
    bounds: EncodedBounds,
    durability: ReadDurability,
    visible_writes: Vec<Arc<PublishedWrite>>,
    page_size: usize,
    projection: CoreProjection,
) -> Result<ScanChunk, StorageError> {
    let scan_options = slatedb_scan_options(durability);
    let mut base_iter = snapshot
        .scan_with_options(bounds.range(), &scan_options)
        .await
        .map_err(slatedb_error)?;
    let mut overlay = BTreeMap::new();
    for write in visible_writes {
        for (key, value) in &*write.overlay {
            if bounds.contains(key) {
                overlay.insert(key.clone(), value.clone());
            }
        }
    }
    let mut overlay = overlay.into_iter().peekable();
    let mut base_row = base_iter.next().await.map_err(slatedb_error)?;
    let mut rows = Vec::with_capacity(page_size.saturating_add(1));
    while rows.len() <= page_size {
        let next = match (base_row.as_ref(), overlay.peek()) {
            (Some(base_entry), Some((overlay_key, _))) => {
                match base_entry.key.cmp(&overlay_key.0) {
                    std::cmp::Ordering::Less => {
                        let row = (Key(base_entry.key.clone()), Some(base_entry.value.clone()));
                        base_row = base_iter.next().await.map_err(slatedb_error)?;
                        Some(row)
                    }
                    std::cmp::Ordering::Equal => {
                        let (key, value) = overlay
                            .next()
                            .expect("peeked SlateDB publication overlay entry");
                        base_row = base_iter.next().await.map_err(slatedb_error)?;
                        Some((key, value))
                    }
                    std::cmp::Ordering::Greater => overlay.next(),
                }
            }
            (Some(base_entry), None) => {
                let row = (Key(base_entry.key.clone()), Some(base_entry.value.clone()));
                base_row = base_iter.next().await.map_err(slatedb_error)?;
                Some(row)
            }
            (None, Some(_)) => overlay.next(),
            (None, None) => None,
        };
        let Some((key, value)) = next else {
            break;
        };
        if let Some(value) = value {
            rows.push((key, value));
        }
    }

    let has_more = rows.len() > page_size;
    let entries = rows
        .into_iter()
        .take(page_size)
        .map(|(key, value)| {
            if key.0.len() < SPACE_PREFIX_LEN {
                return Err(StorageError::Corruption(format!(
                    "slatedb key was shorter than space prefix: {:?}",
                    key.0
                )));
            }
            Ok(ReadEntry {
                key: Key(key.0.slice(SPACE_PREFIX_LEN..)),
                value: project_value(value, projection),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ScanChunk { entries, has_more })
}

async fn collect_snapshot_keys(
    snapshot: Arc<DbSnapshot>,
    bounds: EncodedBounds,
) -> Result<Vec<Key>, StorageError> {
    let scan_options = slatedb_scan_options(ReadDurability::Visible);
    let mut iter = snapshot
        .scan_with_options(bounds.range(), &scan_options)
        .await
        .map_err(slatedb_error)?;
    let mut keys = Vec::new();
    while let Some(row) = iter.next().await.map_err(slatedb_error)? {
        keys.push(Key(row.key));
    }
    Ok(keys)
}

fn slatedb_read_options(durability: ReadDurability) -> SlateDBReadOptions {
    SlateDBReadOptions::new().with_durability_filter(slatedb_durability_filter(durability))
}

fn slatedb_scan_options(durability: ReadDurability) -> SlateDBScanOptions {
    // SlateDB's default scan options fetch one block at a time. Keep iteration
    // ordered, but let SlateDB prefetch remote SST blocks behind the iterator.
    SlateDBScanOptions::default()
        .with_durability_filter(slatedb_durability_filter(durability))
        .with_read_ahead_bytes(SCAN_READ_AHEAD_BYTES)
        .with_max_fetch_tasks(SCAN_MAX_FETCH_TASKS)
        .with_cache_blocks(SCAN_CACHE_BLOCKS)
}

fn slatedb_durability_filter(durability: ReadDurability) -> DurabilityLevel {
    match durability {
        ReadDurability::Visible => DurabilityLevel::Memory,
        ReadDurability::Durable => DurabilityLevel::Remote,
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

fn bounds_are_empty(lower: &Bound<Vec<u8>>, upper: &Bound<Vec<u8>>) -> bool {
    match (lower, upper) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(lower), Bound::Included(upper)) => lower > upper,
        (Bound::Included(lower) | Bound::Excluded(lower), Bound::Excluded(upper))
        | (Bound::Excluded(lower), Bound::Included(upper)) => lower >= upper,
    }
}

fn range_contains_key(range: &KeyRange, key: &Key) -> bool {
    let lower_matches = match &range.lower {
        Bound::Included(lower) => key >= lower,
        Bound::Excluded(lower) => key > lower,
        Bound::Unbounded => true,
    };
    let upper_matches = match &range.upper {
        Bound::Included(upper) => key <= upper,
        Bound::Excluded(upper) => key < upper,
        Bound::Unbounded => true,
    };
    lower_matches && upper_matches
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}

fn project_value(value: Bytes, projection: CoreProjection) -> ProjectedValue {
    match projection {
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        CoreProjection::FullValue => ProjectedValue::FullValue(value),
    }
}

fn slatedb_error(error: slatedb::Error) -> StorageError {
    match error.kind() {
        slatedb::ErrorKind::Closed(CloseReason::Fenced) => StorageError::Fenced,
        // SlateDB's public contract requires a new instance after *any*
        // Closed reason. Keep fencing distinct for callers, while making
        // background-task failures and future close reasons terminal too.
        slatedb::ErrorKind::Closed(_) => StorageError::Closed(format!("slatedb storage: {error}")),
        _ => StorageError::Io(format!("slatedb storage: {error}")),
    }
}

/// Errors from an accepted SlateDB write cannot prove the batch was not
/// applied: SlateDB can fail after its atomic WAL/memtable publication and
/// before returning the durability watcher. Preserve the known terminal
/// states, but make every other attempted commit outcome explicit so callers
/// do not blindly replay it.
fn commit_outcome_unknown(error: StorageError) -> StorageError {
    match error {
        StorageError::Fenced | StorageError::Closed(_) => error,
        error => StorageError::CommitOutcomeUnknown(error.to_string()),
    }
}

fn object_store_error(error: object_store::Error) -> StorageError {
    StorageError::Io(format!("slatedb object store: {error}"))
}

#[derive(Clone, Default)]
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
    use super::*;
    use async_trait::async_trait;
    use futures_util::stream::BoxStream;
    use lix_engine::storage::{
        GetOptions, ProjectedValue, PutEntry, ReadOptions, Storage, StorageRead, StorageWrite,
        StoredValue, WriteOptions,
    };
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{
        CopyOptions, Error as ObjectStoreError, GetOptions as ObjectStoreGetOptions, GetResult,
        ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, PutOptions, PutPayload,
        PutResult, RenameOptions, Result as ObjectStoreResult,
    };
    use slatedb::config::{FlushOptions, FlushType};
    use std::ops::Range;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    tokio::task_local! {
        static CALLER_READ_MARKER: ();
    }

    #[test]
    fn uses_lz4_compression_by_default() {
        assert_eq!(
            slatedb_settings().compression_codec,
            Some(CompressionCodec::Lz4)
        );
    }

    #[test]
    fn disk_cache_parts_match_scan_read_ahead() {
        assert_eq!(OBJECT_STORE_CACHE_PART_SIZE_BYTES, SCAN_READ_AHEAD_BYTES);
    }

    #[test]
    fn batches_completed_compactions_on_the_compactor_poll_interval() {
        let settings = slatedb_settings();
        let compactor = settings
            .compactor_options
            .as_ref()
            .expect("Lix enables SlateDB compaction");
        let default_settings = Settings::default();
        let default_compactor = default_settings
            .compactor_options
            .as_ref()
            .expect("default SlateDB settings enable compaction");
        assert_eq!(
            compactor.commit_compacted_interval,
            COMPACTOR_COMMIT_INTERVAL
        );
        assert_eq!(compactor.commit_compacted_interval, compactor.poll_interval);
        assert_eq!(compactor.poll_interval, default_compactor.poll_interval);
    }

    #[test]
    fn opens_fresh_local_versioned_storage() {
        let directory = tempfile::tempdir().expect("create fresh local storage directory");
        let storage = SlateDB::open(directory.path()).expect("open fresh local LZ4 storage");
        assert_eq!(storage.path(), directory.path());
    }

    #[test]
    fn cached_open_does_not_preload_ssts() {
        let cache_dir = tempfile::tempdir().expect("create disk-cache directory");
        assert_open_does_not_preload_ssts(
            "test-on-demand-disk-cache",
            SlateDBObjectStoreOptions {
                cache: Some(SlateDBCacheOptions {
                    root_folder: cache_dir.path().join("object-cache"),
                    max_disk_cache_bytes: 16 * 1024 * 1024,
                    block_cache_bytes: 0,
                    metadata_cache_bytes: 0,
                }),
            },
        );
    }

    #[test]
    fn default_memory_cache_does_not_preload_ssts() {
        assert_open_does_not_preload_ssts(
            "test-on-demand-memory-cache",
            SlateDBObjectStoreOptions::default(),
        );
    }

    fn assert_open_does_not_preload_ssts(db_path: &str, options: SlateDBObjectStoreOptions) {
        let inner = Arc::new(InMemory::new());
        let db_path = db_path.to_string();
        seed_compacted_sst(inner.clone(), &db_path);

        let store = Arc::new(BlockingStore::new(inner));
        let blocked_reads = store.block_compacted_reads();
        let (opened_tx, opened_rx) = mpsc::channel();
        let opener = std::thread::spawn(move || {
            opened_tx
                .send(SlateDB::open_object_store_with_options(
                    db_path, store, options,
                ))
                .expect("send cached open result");
        });

        let opened = opened_rx.recv_timeout(Duration::from_secs(5));
        drop(blocked_reads);
        opener.join().expect("join cached opener");
        let storage = opened
            .expect("cached open must not wait for SST reads")
            .expect("open cached SlateDB");
        drop(storage);
    }

    #[test]
    fn default_memory_cache_serves_a_warm_sst_read_without_object_store_access() {
        let inner = Arc::new(InMemory::new());
        let db_path = "test-default-memory-cache-hit";
        seed_compacted_sst(inner.clone(), db_path);

        let store = Arc::new(BlockingStore::new(inner));
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open default-memory-cache storage");

        let first = block_on(storage.worker.call_read(|db| async move {
            let snapshot = db.snapshot().await.map_err(slatedb_error)?;
            snapshot.get(b"key").await.map_err(slatedb_error)
        }))
        .expect("warm raw SlateDB SST read");
        assert_eq!(first, Some(Bytes::from_static(b"value")));

        let blocked_reads = store.block_sst_reads();
        let reader_storage = storage;
        let (result_tx, result_rx) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            let result = block_on(reader_storage.worker.call_read(|db| async move {
                let snapshot = db.snapshot().await.map_err(slatedb_error)?;
                snapshot.get(b"key").await.map_err(slatedb_error)
            }));
            result_tx.send(result).expect("send warm raw read result");
        });

        let second = match result_rx.recv_timeout(Duration::from_secs(2)) {
            Ok(result) => result.expect("warm raw read should succeed"),
            Err(error) => {
                drop(blocked_reads);
                reader.join().expect("join blocked raw reader");
                panic!("warm raw read touched the object store: {error}");
            }
        };
        drop(blocked_reads);
        reader.join().expect("join warm raw reader");
        assert_eq!(second, Some(Bytes::from_static(b"value")));
    }

    fn seed_compacted_sst(inner: Arc<InMemory>, db_path: &str) {
        let physical_db_path = join_db_path(db_path, LZ4_FORMAT_PATH);
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build raw SlateDB test runtime")
            .block_on(async {
                let db = Db::builder(physical_db_path, inner.clone())
                    .with_settings(slatedb_settings())
                    .with_db_cache_disabled()
                    .build()
                    .await
                    .expect("open raw SlateDB");
                let mut batch = WriteBatch::new();
                batch.put(b"key", b"value");
                db.write_with_options(
                    batch,
                    &SlateDBWriteOptions {
                        await_durable: false,
                        ..SlateDBWriteOptions::default()
                    },
                )
                .await
                .expect("write raw SlateDB row");
                db.flush().await.expect("flush raw SlateDB WAL");
                db.flush_with_options(FlushOptions {
                    flush_type: FlushType::MemTable,
                })
                .await
                .expect("flush raw SlateDB memtable");
                db.close().await.expect("close raw SlateDB");
            });
    }

    #[test]
    fn fresh_storage_uses_versioned_lz4_format() {
        let store = Arc::new(InMemory::new());
        let db_path = "test-lz4-physical-format";
        let space = SpaceId(7);
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open fresh LZ4 storage");

        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin LZ4 write");
        block_on(write.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: Key(Bytes::from_static(b"lz4-key")),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"lz4-value"),
                    },
                }],
            },
        ))
        .expect("stage LZ4 row");
        block_on(write.commit()).expect("commit LZ4 row");
        block_on(storage.flush()).expect("flush LZ4 row");
        block_on(storage.worker.call(|db| async move {
            db.flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .map_err(slatedb_error)?;
            assert!(
                db.manifest()
                    .l0()
                    .iter()
                    .any(|view| { view.sst.info.compression_codec == Some(CompressionCodec::Lz4) }),
                "new physical SST must record the LZ4 codec"
            );
            Ok(())
        }))
        .expect("flush and inspect LZ4 SST");
        drop(storage);

        let physical_prefix = format!("{db_path}/{LZ4_FORMAT_PATH}/");
        let object_paths = block_on(async {
            let mut objects = store.list(None);
            let mut paths = Vec::new();
            while let Some(object) = objects.next().await {
                paths.push(
                    object
                        .expect("list fresh LZ4 storage object")
                        .location
                        .to_string(),
                );
            }
            paths
        });
        assert!(!object_paths.is_empty(), "fresh storage must write objects");
        assert!(
            object_paths
                .iter()
                .all(|path| path.starts_with(&physical_prefix)),
            "all objects must use the versioned LZ4 namespace: {object_paths:?}"
        );
    }

    #[test]
    fn open_object_store_round_trips_with_memory_store() {
        let storage = SlateDB::open_object_store_with_options(
            "test-db",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open memory object-store slatedb storage");

        let space = SpaceId(7);
        let key = Key(Bytes::from_static(b"hello"));
        let value = Bytes::from_static(b"world");

        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin write");
        block_on(write.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: value.clone(),
                    },
                }],
            },
        ))
        .expect("put row");
        block_on(write.commit()).expect("commit row");

        let read = block_on(storage.begin_read(ReadOptions::default())).expect("begin read");
        let result = block_on(read.get_many(&[GetManyRequest {
            space,
            keys: std::slice::from_ref(&key),
            opts: GetOptions::default(),
        }]))
        .expect("read row");

        assert_eq!(result.values, vec![Some(ProjectedValue::FullValue(value))]);
        assert_eq!(
            block_on(read.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions {
                    projection: CoreProjection::KeyOnly,
                },
            }]))
            .expect("read singleton key only")
            .values,
            vec![Some(ProjectedValue::KeyOnly)]
        );
        assert_eq!(
            block_on(read.get_many(&[GetManyRequest {
                space,
                keys: &[Key(Bytes::from_static(b"missing"))],
                opts: GetOptions::default(),
            }]))
            .expect("read singleton missing key")
            .values,
            vec![None]
        );
    }

    #[test]
    fn batched_point_preconditions_preserve_duplicate_and_mixed_failure_indexes() {
        let storage = SlateDB::open_object_store_with_options(
            "test-batched-point-preconditions",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open memory object-store slatedb storage");
        let space = SpaceId(7);
        let present = Key(Bytes::from_static(b"present"));
        let missing = Key(Bytes::from_static(b"missing"));
        let value = Bytes::from_static(b"value");
        let value_hash = *blake3::hash(&value).as_bytes();

        let mut seed =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin seed write");
        block_on(seed.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: present.clone(),
                    value: StoredValue {
                        bytes: value.clone(),
                    },
                }],
            },
        ))
        .expect("stage seed value");
        block_on(seed.commit()).expect("commit seed value");

        let passing = block_on(storage.begin_write(WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space,
                    key: present.clone(),
                    expected: value.clone(),
                },
                Precondition::KeyPresent {
                    space,
                    key: present.clone(),
                },
                Precondition::KeyAbsent {
                    space,
                    key: missing.clone(),
                },
            ],
            ..WriteOptions::default()
        }))
        .expect("all batched point preconditions pass");
        drop(passing);

        let error = block_on(storage.begin_write(WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space,
                    key: present.clone(),
                    expected: value,
                },
                Precondition::KeyAbsent {
                    space,
                    key: present.clone(),
                },
                Precondition::RangeEmpty {
                    space,
                    range: KeyRange {
                        lower: Bound::Included(present.clone()),
                        upper: Bound::Included(present.clone()),
                    },
                },
                Precondition::KeyPresent {
                    space,
                    key: missing,
                },
                Precondition::KeyAbsent {
                    space,
                    key: present.clone(),
                },
                Precondition::BranchEquals {
                    ref_key: Key(Bytes::from_static(b"branch-ref")),
                    expected: Bytes::from_static(b"ignored"),
                },
                Precondition::KeyValueHashEquals {
                    space,
                    key: present,
                    hash: value_hash,
                },
            ],
            ..WriteOptions::default()
        }))
        .err()
        .expect("mixed failed preconditions report every original index");

        assert_eq!(
            error,
            StorageError::PreconditionFailed(vec![
                PreconditionFailure { index: 1 },
                PreconditionFailure { index: 2 },
                PreconditionFailure { index: 3 },
                PreconditionFailure { index: 4 },
                PreconditionFailure { index: 5 },
            ])
        );
    }

    #[test]
    fn pending_publication_is_visible_to_points_scans_and_preconditions() {
        let storage = SlateDB::open_object_store_with_options(
            "test-pending-publication-overlay",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open pending-publication storage");
        let blocker = Arc::new(WriteCompletion::new());
        storage
            .write_pipeline
            .state
            .lock()
            .expect("lock write pipeline")
            .tail = Some(Arc::clone(&blocker));

        let space = SpaceId(7);
        let key = Key(Bytes::from_static(b"pending"));
        let value = Bytes::from_static(b"value");
        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin pending write");
        block_on(write.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: value.clone(),
                    },
                }],
            },
        ))
        .expect("stage pending write");
        block_on(write.commit()).expect("publish pending write");

        let read =
            block_on(storage.begin_read(ReadOptions::default())).expect("begin overlay read");
        assert_eq!(
            block_on(read.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions::default(),
            }]))
            .expect("read pending point")
            .values,
            vec![Some(ProjectedValue::FullValue(value.clone()))]
        );
        assert_eq!(
            block_on(read.scan(
                space,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                ScanOptions::default(),
            ))
            .expect("scan pending point")
            .entries,
            vec![ReadEntry {
                key: key.clone(),
                value: ProjectedValue::FullValue(value.clone()),
            }]
        );

        let checked = block_on(storage.begin_write(WriteOptions {
            preconditions: vec![Precondition::KeyValueEquals {
                space,
                key,
                expected: value,
            }],
            ..WriteOptions::default()
        }))
        .expect("pending point satisfies the next writer precondition");
        drop(checked);

        blocker.complete(Ok(0));
        block_on(storage.flush()).expect("flush released pending write");
    }

    #[test]
    fn visible_point_cache_isolated_by_snapshot_sequence() {
        let storage = SlateDB::open_object_store_with_options(
            "test-snapshot-point-cache",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open snapshot cache storage");
        let space = SpaceId(7);
        let key = Key(Bytes::from_static(b"versioned-key"));

        let mut initial =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin initial write");
        block_on(initial.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"first"),
                    },
                }],
            },
        ))
        .expect("stage initial value");
        block_on(initial.commit()).expect("commit initial value");

        let before_update =
            block_on(storage.begin_read(ReadOptions::default())).expect("begin old snapshot");
        let before_update_cache_key = before_update
            .snapshot_cache_key()
            .expect("SlateDB read should expose a snapshot cache key");
        assert_eq!(
            block_on(before_update.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions::default(),
            }]))
            .expect("read old snapshot")
            .values,
            vec![Some(ProjectedValue::FullValue(Bytes::from_static(
                b"first"
            )))]
        );

        let mut update =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin update write");
        block_on(update.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"second"),
                    },
                }],
            },
        ))
        .expect("stage updated value");
        block_on(update.commit()).expect("commit updated value");

        let after_update =
            block_on(storage.begin_read(ReadOptions::default())).expect("begin new snapshot");
        assert_ne!(
            before_update_cache_key,
            after_update
                .snapshot_cache_key()
                .expect("updated SlateDB read should expose a snapshot cache key")
        );
        assert_eq!(
            block_on(after_update.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions::default(),
            }]))
            .expect("read new snapshot")
            .values,
            vec![Some(ProjectedValue::FullValue(Bytes::from_static(
                b"second"
            )))]
        );
        assert_eq!(
            block_on(before_update.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions::default(),
            }]))
            .expect("reread old snapshot")
            .values,
            vec![Some(ProjectedValue::FullValue(Bytes::from_static(
                b"first"
            )))],
            "an old snapshot must not observe the value cached for a newer sequence"
        );
    }

    #[test]
    fn snapshot_point_cache_batch_preserves_hits_misses_and_duplicates() {
        let cache = SnapshotPointCache::new();
        let present = Key(Bytes::from_static(b"present"));
        let missing = Key(Bytes::from_static(b"cached-missing"));
        let unseen = Key(Bytes::from_static(b"unseen"));
        let value = Bytes::from_static(b"value");
        cache.insert(7, present.clone(), Some(value.clone()));
        cache.insert(7, missing.clone(), None);

        let keys = [present.clone(), missing.clone(), unseen, present.clone()];
        let mut values = vec![None; keys.len()];
        cache.get_many(7, &keys, &mut values);
        assert_eq!(
            values,
            vec![
                Some(Some(value.clone())),
                Some(None),
                None,
                Some(Some(value))
            ]
        );
        let keys = [present, missing];
        let mut values = vec![None; keys.len()];
        cache.get_many(8, &keys, &mut values);
        assert_eq!(values, vec![None, None]);
    }

    #[test]
    fn snapshot_point_cache_limits_entries_with_one_snapshot_bucket() {
        let cache = SnapshotPointCache::new();
        let first = Key(Bytes::from_static(b"cache-entry-0000"));
        for index in 0..=SNAPSHOT_POINT_CACHE_ENTRIES {
            cache.insert(
                7,
                Key(Bytes::from(format!("cache-entry-{index:04}"))),
                Some(Bytes::from_static(b"value")),
            );
        }

        let keys = [first];
        let mut values = [None];
        cache.get_many(7, &keys, &mut values);
        assert_eq!(values, [None]);
        let keys = [Key(Bytes::from(format!(
            "cache-entry-{SNAPSHOT_POINT_CACHE_ENTRIES:04}"
        )))];
        let mut values = [None];
        cache.get_many(7, &keys, &mut values);
        assert_eq!(values, [Some(Some(Bytes::from_static(b"value")))]);
    }

    #[test]
    fn fenced_writer_reports_a_terminal_error_after_slatedb_closes_it() {
        let object_store = Arc::new(InMemory::new());
        let db_path = "test-fenced-writer";
        let first = SlateDB::open_object_store_with_options(
            db_path,
            object_store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open first SlateDB writer");
        let space = SpaceId(10);

        let mut seed =
            block_on(first.begin_write(WriteOptions::default())).expect("begin seed write");
        block_on(seed.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: Key(Bytes::from_static(b"before-fence")),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"value"),
                    },
                }],
            },
        ))
        .expect("stage seed write");
        block_on(seed.commit()).expect("commit seed write");
        block_on(first.flush()).expect("durably flush seed write");

        let _second = SlateDB::open_object_store_with_options(
            db_path,
            object_store,
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open newer SlateDB writer");

        // A newer writer fences this one asynchronously through SlateDB's
        // manifest poll. Wait for that terminal state before asserting that a
        // subsequent commit is rejected.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match block_on(first.begin_read(ReadOptions::default())) {
                Err(StorageError::Fenced) => break,
                Ok(read) => drop(read),
                Err(error) => panic!("old writer returned the wrong error after fencing: {error}"),
            }
            assert!(
                Instant::now() < deadline,
                "SlateDB did not close the fenced writer within the test deadline"
            );
            std::thread::sleep(Duration::from_millis(10));
        }

        let mut fenced =
            block_on(first.begin_write(WriteOptions::default())).expect("begin fenced write");
        block_on(fenced.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: Key(Bytes::from_static(b"after-fence")),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"value"),
                    },
                }],
            },
        ))
        .expect("stage fenced write");
        let error = block_on(fenced.commit()).expect_err("fenced writer must reject commits");

        assert_eq!(error, StorageError::Fenced);
    }

    #[test]
    fn closed_slatedb_panic_is_a_distinct_terminal_storage_error() {
        let error =
            slatedb::Error::closed("background worker panicked".to_string(), CloseReason::Panic);

        assert!(matches!(slatedb_error(error), StorageError::Closed(_)));
    }

    #[test]
    fn commit_is_visible_while_background_wal_flush_is_blocked() {
        let store = Arc::new(BlockingStore::new(Arc::new(InMemory::new())));
        let storage = SlateDB::open_object_store_with_options(
            "test-commit-visibility",
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open commit visibility storage");
        let space = SpaceId(8);
        let key = Key(Bytes::from_static(b"visible-before-durable"));
        let queued_key = Key(Bytes::from_static(b"visible-while-draining"));

        let blocked_write = store.block_next_write();
        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin visibility write");
        block_on(write.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"value"),
                    },
                }],
            },
        ))
        .expect("stage visibility write");
        block_on(write.commit()).expect("publish visibility value");

        // The request has returned, but SlateDB's first background WAL upload
        // is still in flight.
        blocked_write.wait_for_entries(1, "SlateDB WAL write");

        let mut queued =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin queued write");
        block_on(queued.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: queued_key.clone(),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"queued"),
                    },
                }],
            },
        ))
        .expect("stage queued write");
        block_on(queued.commit()).expect("publish queued value");

        let read = block_on(storage.begin_read(ReadOptions::default()))
            .expect("begin visible in-memory read");
        let values = block_on(read.get_many(&[GetManyRequest {
            space,
            keys: &[key, queued_key],
            opts: GetOptions::default(),
        }]))
        .expect("read visible in-memory value")
        .values;
        assert_eq!(
            values,
            vec![
                Some(ProjectedValue::FullValue(Bytes::from_static(b"value"))),
                Some(ProjectedValue::FullValue(Bytes::from_static(b"queued"))),
            ]
        );

        drop(blocked_write);
        block_on(storage.flush()).expect("flush visible value");
    }

    #[test]
    fn durable_reads_exclude_writes_awaiting_wal_upload() {
        let store = Arc::new(BlockingStore::new(Arc::new(InMemory::new())));
        let storage = SlateDB::open_object_store_with_options(
            "test-durable-read-filter",
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open durable read storage");
        let space = SpaceId(8);
        let key = Key(Bytes::from_static(b"visible-before-remote-durable"));
        let value = Bytes::from_static(b"value");

        let blocked_write = store.block_next_write();
        let committer_storage = storage.clone();
        let committer_key = key.clone();
        let committer_value = value.clone();
        let (commit_tx, commit_rx) = mpsc::channel();
        let committer = std::thread::spawn(move || {
            let mut write = block_on(committer_storage.begin_write(WriteOptions::default()))
                .expect("begin durable read write");
            block_on(write.put_many(
                space,
                PutBatch {
                    entries: vec![PutEntry {
                        key: committer_key,
                        value: StoredValue {
                            bytes: committer_value,
                        },
                    }],
                },
            ))
            .expect("stage durable read row");
            commit_tx
                .send(block_on(write.commit()))
                .expect("send durable read commit result");
        });

        blocked_write.wait_for_entries(1, "SlateDB WAL write");
        commit_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("commit should complete after local publication")
            .expect("commit visible durable read row");
        committer.join().expect("join durable read committer");
        let visible =
            block_on(storage.begin_read(ReadOptions::default())).expect("begin visible read");
        assert_eq!(
            block_on(visible.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions::default(),
            }]))
            .expect("read visible value")
            .values,
            vec![Some(ProjectedValue::FullValue(value.clone()))],
            "the ordinary read tier may include published in-memory state"
        );
        let durable = block_on(storage.begin_read(ReadOptions {
            durability: ReadDurability::Durable,
            ..ReadOptions::default()
        }))
        .expect("begin remote-durable read");
        assert_eq!(
            block_on(durable.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions::default(),
            }]))
            .expect("read remote-durable value")
            .values,
            vec![None],
            "a remote-durable read must not claim a blocked WAL upload persisted"
        );

        drop(blocked_write);
        block_on(storage.flush()).expect("flush published durable read row");

        let durable = block_on(storage.begin_read(ReadOptions {
            durability: ReadDurability::Durable,
            ..ReadOptions::default()
        }))
        .expect("begin completed remote-durable read");
        assert_eq!(
            block_on(durable.get_many(&[GetManyRequest {
                space,
                keys: &[key],
                opts: GetOptions::default(),
            }]))
            .expect("read completed remote-durable value")
            .values,
            vec![Some(ProjectedValue::FullValue(value))]
        );
    }

    #[test]
    fn explicit_flush_reports_background_durability_failure() {
        let store = Arc::new(BlockingStore::new(Arc::new(InMemory::new())));
        let storage = SlateDB::open_object_store_with_options(
            "test-failed-commit",
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open failed commit storage");
        let space = SpaceId(9);
        let key = Key(Bytes::from_static(b"rejected"));

        let blocked_write = store.block_next_write();
        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin buffered write");
        block_on(write.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key,
                    value: StoredValue {
                        bytes: Bytes::from_static(b"not-durable"),
                    },
                }],
            },
        ))
        .expect("stage buffered write");
        block_on(write.commit()).expect("publish buffered write");

        blocked_write.wait_for_entries(1, "failing background WAL write");
        store.fail_writes();
        drop(blocked_write);
        let flush_error = block_on(storage.flush()).expect_err("WAL flush must fail");
        assert!(
            matches!(flush_error, StorageError::Io(message) if message.contains("injected write failure")),
            "flush should preserve the SlateDB write error"
        );
    }

    #[test]
    fn dropping_last_handle_waits_for_background_flush() {
        let store = Arc::new(BlockingStore::new(Arc::new(InMemory::new())));
        let db_path = "test-close-background-durability";
        let space = SpaceId(8);
        let key = Key(Bytes::from_static(b"background-commit"));
        let value = Bytes::from_static(b"durable");
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open close-test storage");
        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin close-test write");
        block_on(write.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: value.clone(),
                    },
                }],
            },
        ))
        .expect("stage close-test value");

        let blocked_write = store.block_next_write();
        block_on(write.commit()).expect("publish close-test value");
        blocked_write.wait_for_entries(1, "background commit WAL write");

        let (closed_tx, closed_rx) = mpsc::channel();
        let closer = std::thread::spawn(move || {
            drop(storage);
            let _ = closed_tx.send(());
        });
        assert!(
            matches!(
                closed_rx.recv_timeout(Duration::from_millis(50)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "close must wait for the background WAL flush"
        );
        drop(blocked_write);
        closed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("close should finish after WAL durability");
        closer.join().expect("join close-test closer");

        let reopened = SlateDB::open_object_store_with_options(
            db_path,
            store,
            SlateDBObjectStoreOptions::default(),
        )
        .expect("reopen close-test storage");
        let read =
            block_on(reopened.begin_read(ReadOptions::default())).expect("begin close-test read");
        let result = block_on(read.get_many(&[GetManyRequest {
            space,
            keys: &[key],
            opts: GetOptions::default(),
        }]))
        .expect("read close-test value");
        assert_eq!(result.values, vec![Some(ProjectedValue::FullValue(value))]);
    }

    #[test]
    fn cloned_snapshot_reads_overlap() {
        let inner = Arc::new(InMemory::new());
        let db_path = "test-concurrent-reads";
        let space = SpaceId(9);
        let left_key = Key(Bytes::from_static(b"left"));
        let right_key = Key(Bytes::from_static(b"right"));
        let value = Bytes::from(vec![b'x'; 128 * 1024]);

        {
            let storage = SlateDB::open_object_store_with_options(
                db_path,
                inner.clone(),
                SlateDBObjectStoreOptions::default(),
            )
            .expect("open concurrent-read seed storage");
            let mut write = block_on(storage.begin_write(WriteOptions::default()))
                .expect("begin concurrent-read seed write");
            block_on(write.put_many(
                space,
                PutBatch {
                    entries: vec![
                        PutEntry {
                            key: left_key.clone(),
                            value: StoredValue {
                                bytes: value.clone(),
                            },
                        },
                        PutEntry {
                            key: right_key.clone(),
                            value: StoredValue {
                                bytes: value.clone(),
                            },
                        },
                    ],
                },
            ))
            .expect("stage concurrent-read seed values");
            block_on(write.commit()).expect("commit concurrent-read seed values");
        }

        let store = Arc::new(BlockingStore::new(inner));
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("reopen concurrent-read storage");
        let read = Arc::new(
            block_on(storage.begin_read(ReadOptions::default()))
                .expect("begin shared snapshot read"),
        );
        let left_read = Arc::clone(&read);
        let right_read = Arc::clone(&read);
        let blocked_reads = store.block_sst_reads();

        let left = std::thread::spawn(move || {
            block_on(left_read.get_many(&[GetManyRequest {
                space,
                keys: &[left_key],
                opts: GetOptions::default(),
            }]))
        });
        blocked_reads.wait_for_entries(1, "first SST read");
        let right = std::thread::spawn(move || {
            block_on(right_read.get_many(&[GetManyRequest {
                space,
                keys: &[right_key],
                opts: GetOptions::default(),
            }]))
        });
        blocked_reads.wait_for_entries(2, "second concurrent SST read");
        drop(blocked_reads);

        assert_eq!(
            left.join()
                .expect("join left read")
                .expect("read left value")
                .values,
            vec![Some(ProjectedValue::FullValue(value.clone()))]
        );
        assert_eq!(
            right
                .join()
                .expect("join right read")
                .expect("read right value")
                .values,
            vec![Some(ProjectedValue::FullValue(value))]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_object_store_read_yields_to_executor() {
        let inner = Arc::new(InMemory::new());
        let db_path = "test-async-read-yields";
        let space = SpaceId(10);
        let key = Key(Bytes::from_static(b"remote-key"));
        let value = Bytes::from(vec![b'x'; 128 * 1024]);

        {
            let storage = SlateDB::open_object_store_with_options(
                db_path,
                inner.clone(),
                SlateDBObjectStoreOptions::default(),
            )
            .expect("open async-read seed storage");
            let mut write = storage
                .begin_write(WriteOptions::default())
                .await
                .expect("begin async-read seed write");
            write
                .put_many(
                    space,
                    PutBatch {
                        entries: vec![PutEntry {
                            key: key.clone(),
                            value: StoredValue {
                                bytes: value.clone(),
                            },
                        }],
                    },
                )
                .await
                .expect("stage async-read seed value");
            write.commit().await.expect("commit async-read seed value");
        }

        let store = Arc::new(BlockingStore::new(inner));
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("reopen async-read storage");
        let read = storage
            .begin_read(ReadOptions::default())
            .await
            .expect("begin async object-store read");
        let blocked_read = store.block_sst_reads();

        let (release_tx, release_rx) = mpsc::channel();
        let releaser = std::thread::spawn(move || {
            blocked_read.wait_for_entries(1, "pending async SST read");
            let _ = release_rx.recv_timeout(Duration::from_secs(2));
            drop(blocked_read);
        });

        let (task_tx, task_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = task_tx.send(());
        });

        let keys = [key];
        let requests = [GetManyRequest {
            space,
            keys: &keys,
            opts: GetOptions::default(),
        }];
        let point_read = read.get_many(&requests);
        tokio::pin!(point_read);
        tokio::select! {
            biased;
            result = &mut point_read => {
                panic!("blocked object-store read completed before independent task: {result:?}");
            }
            result = task_rx => {
                result.expect("independent Tokio task should run while read is pending");
            }
        }

        release_tx.send(()).expect("release pending SST read");
        let result = point_read.await.expect("finish async object-store read");
        assert_eq!(result.values, vec![Some(ProjectedValue::FullValue(value))]);
        releaser.join().expect("join SST read releaser");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_pending_read_cancels_it_before_storage_close() {
        let storage = SlateDB::open_object_store_with_options(
            "test-cancel-pending-read",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open cancellable read storage");
        assert_dropping_pending_read_cancels_before_storage_close(storage).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_pending_local_filesystem_read_cancels_before_storage_close() {
        let directory = tempfile::tempdir().expect("create local cancellable read storage");
        let storage = SlateDB::open(directory.path()).expect("open local cancellable read storage");
        assert_dropping_pending_read_cancels_before_storage_close(storage).await;
    }

    async fn assert_dropping_pending_read_cancels_before_storage_close(storage: SlateDB) {
        let release = Arc::new(Notify::new());
        let release_for_read = Arc::clone(&release);
        let (started_tx, started_rx) = oneshot::channel();
        let worker = storage.worker.clone();
        let pending_read = tokio::spawn(async move {
            worker
                .call_read(move |_db| {
                    let release = Arc::clone(&release_for_read);
                    async move {
                        let _ = started_tx.send(());
                        release.notified().await;
                        Ok::<(), StorageError>(())
                    }
                })
                .await
        });

        started_rx
            .await
            .expect("pending read operation should start before cancellation");
        pending_read.abort();
        let error = pending_read
            .await
            .expect_err("dropping the caller should cancel its read future");
        assert!(
            error.is_cancelled(),
            "pending read task should be cancelled"
        );

        let (closed_tx, closed_rx) = mpsc::channel();
        let closer = std::thread::spawn(move || {
            drop(storage);
            let _ = closed_tx.send(());
        });
        if let Err(error) = closed_rx.recv_timeout(Duration::from_secs(2)) {
            // Keep the regression test self-cleaning if read cancellation ever
            // regresses: the old detached operation can finish before joining.
            release.notify_one();
            closed_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("close should finish after releasing pending read");
            closer.join().expect("join fallback closer");
            panic!("storage close should wait only for the cancelled read to drain: {error:?}");
        }
        closer.join().expect("join storage closer");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_operation_stays_on_the_callers_executor() {
        let storage = SlateDB::open_object_store_with_options(
            "test-caller-runtime-read",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open caller-runtime read storage");

        CALLER_READ_MARKER
            .scope((), async {
                storage
                    .worker
                    .call_read(|_db| async move {
                        assert!(
                            CALLER_READ_MARKER.try_with(|()| ()).is_ok(),
                            "read work must retain the caller task context"
                        );
                        Ok::<(), StorageError>(())
                    })
                    .await
                    .expect("run read on caller executor");
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_filesystem_read_operation_stays_on_the_callers_executor() {
        let directory = tempfile::tempdir().expect("create local caller-runtime storage");
        let storage = SlateDB::open(directory.path()).expect("open local caller-runtime storage");

        CALLER_READ_MARKER
            .scope((), async {
                storage
                    .worker
                    .call_read(|db| async move {
                        assert!(
                            CALLER_READ_MARKER.try_with(|()| ()).is_ok(),
                            "local filesystem read work must retain the caller task context"
                        );
                        db.snapshot().await.map_err(slatedb_error)?;
                        Ok::<(), StorageError>(())
                    })
                    .await
                    .expect("run local read on caller executor");
            })
            .await;
    }

    fn block_on<T>(future: impl Future<Output = T>) -> T {
        Builder::new_current_thread()
            .build()
            .expect("build test runtime")
            .block_on(future)
    }

    #[derive(Clone, Debug)]
    struct BlockingStore {
        inner: Arc<InMemory>,
        next_write: Arc<AtomicBool>,
        fail_writes: Arc<AtomicBool>,
        writes: Arc<OperationBlock>,
        block_reads: Arc<AtomicBool>,
        block_compacted_reads: Arc<AtomicBool>,
        reads: Arc<OperationBlock>,
    }

    impl BlockingStore {
        fn new(inner: Arc<InMemory>) -> Self {
            Self {
                inner,
                next_write: Arc::new(AtomicBool::new(false)),
                fail_writes: Arc::new(AtomicBool::new(false)),
                writes: Arc::new(OperationBlock::default()),
                block_reads: Arc::new(AtomicBool::new(false)),
                block_compacted_reads: Arc::new(AtomicBool::new(false)),
                reads: Arc::new(OperationBlock::default()),
            }
        }

        fn block_next_write(&self) -> OperationBlockGuard {
            OperationBlockGuard::arm(Arc::clone(&self.next_write), Arc::clone(&self.writes))
        }

        fn fail_writes(&self) {
            self.fail_writes.store(true, Ordering::Release);
        }

        fn block_sst_reads(&self) -> OperationBlockGuard {
            OperationBlockGuard::arm(Arc::clone(&self.block_reads), Arc::clone(&self.reads))
        }

        fn block_compacted_reads(&self) -> OperationBlockGuard {
            OperationBlockGuard::arm(
                Arc::clone(&self.block_compacted_reads),
                Arc::clone(&self.reads),
            )
        }

        fn maybe_block_write(&self) {
            if self.next_write.swap(false, Ordering::AcqRel) {
                self.writes.enter();
            }
        }

        fn maybe_fail_write(&self) -> ObjectStoreResult<()> {
            if self.fail_writes.load(Ordering::Acquire) {
                Err(ObjectStoreError::NotSupported {
                    source: Box::new(std::io::Error::other("injected write failure")),
                })
            } else {
                Ok(())
            }
        }

        fn maybe_block_read(&self, location: &ObjectPath) {
            let is_sst = location
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("sst"));
            let block_all_ssts = self.block_reads.load(Ordering::Acquire);
            let block_compacted = self.block_compacted_reads.load(Ordering::Acquire)
                && location.as_ref().contains("/compacted/");
            if is_sst && (block_all_ssts || block_compacted) {
                self.reads.enter();
            }
        }
    }

    impl fmt::Display for BlockingStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("BlockingStore")
        }
    }

    #[derive(Debug, Default)]
    struct OperationBlock {
        state: Mutex<OperationBlockState>,
        available: Condvar,
    }

    #[derive(Debug, Default)]
    struct OperationBlockState {
        entries: usize,
        released: bool,
    }

    impl OperationBlock {
        fn reset(&self) {
            let mut state = self.state.lock().expect("lock operation block");
            state.entries = 0;
            state.released = false;
        }

        fn enter(&self) {
            let mut state = self.state.lock().expect("lock operation block");
            state.entries += 1;
            self.available.notify_all();
            while !state.released {
                state = self
                    .available
                    .wait(state)
                    .expect("wait for operation release");
            }
        }

        fn release(&self) {
            let mut state = self.state.lock().expect("lock operation block");
            state.released = true;
            self.available.notify_all();
        }
    }

    #[derive(Debug)]
    struct OperationBlockGuard {
        enabled: Arc<AtomicBool>,
        block: Arc<OperationBlock>,
    }

    impl OperationBlockGuard {
        fn arm(enabled: Arc<AtomicBool>, block: Arc<OperationBlock>) -> Self {
            block.reset();
            enabled.store(true, Ordering::Release);
            Self { enabled, block }
        }

        fn wait_for_entries(&self, expected: usize, description: &str) {
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut state = self.block.state.lock().expect("lock operation block");
            while state.entries < expected {
                let now = Instant::now();
                assert!(now < deadline, "timed out waiting for {description}");
                let (next_state, _) = self
                    .block
                    .available
                    .wait_timeout(state, deadline - now)
                    .expect("wait for blocked operation");
                state = next_state;
            }
        }
    }

    impl Drop for OperationBlockGuard {
        fn drop(&mut self) {
            self.enabled.store(false, Ordering::Release);
            self.block.release();
        }
    }

    #[async_trait]
    impl ObjectStore for BlockingStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.maybe_block_write();
            self.maybe_fail_write()?;
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            options: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: ObjectStoreGetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.maybe_block_read(location);
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &ObjectPath,
            ranges: &[Range<u64>],
        ) -> ObjectStoreResult<Vec<Bytes>> {
            self.maybe_block_read(location);
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&ObjectPath>,
            offset: &ObjectPath,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: RenameOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }
}
