#![allow(
    clippy::manual_async_fn,
    reason = "explicit future signatures mirror Storage traits and keep Send guarantees visible"
)]

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::JoinHandle;

use bytes::Bytes;
use futures_util::stream::{self, StreamExt, TryStreamExt};
use lix_engine::storage::{
    CommitResult, CoreProjection, GetManyResult, GetOptions, Key, KeyRange, ProjectedValue,
    PutBatch, ReadEntry, ReadOptions, ScanChunk, ScanOptions, SpaceId, Storage, StorageError,
    StorageRead, StorageWrite, StoredValue, WriteOptions, WriteStats,
};
use lix_engine::{StorageFactory, StorageFixture, StorageTestConfig};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use slatedb::config::{
    ObjectStoreCacheOptions, PreloadLevel, ScanOptions as SlateDBScanOptions, Settings,
    WriteOptions as SlateDBWriteOptions,
};
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::db_cache::{DbCache, SplitCache};
use slatedb::{Db, DbIterator, DbSnapshot, WriteBatch};
use tempfile::TempDir;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, oneshot};

const DB_PATH: &str = "db";
const SPACE_PREFIX_LEN: usize = 4;
const MAX_SLATEDB_KEY_LEN: usize = u16::MAX as usize;
const RUNTIME_WORKER_THREADS: usize = 2;
const POINT_READ_CONCURRENCY: usize = 64;
const SCAN_BATCH_ROWS: usize = 1024;
const SCAN_READ_AHEAD_BYTES: usize = 2 * 1024 * 1024;
const SCAN_MAX_FETCH_TASKS: usize = 16;
const SCAN_CACHE_BLOCKS: bool = true;
const OBJECT_STORE_CACHE_PART_SIZE_BYTES: usize = 4 * 1024 * 1024;

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
}

#[derive(Clone, Debug)]
pub struct SlateDBOptions {
    pub path: PathBuf,
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

#[allow(missing_debug_implementations)]
pub struct SlateDBRead {
    worker: SlateDBWorker,
    snapshot: Arc<DbSnapshot>,
}

#[allow(missing_debug_implementations)]
pub struct SlateDBWrite {
    worker: SlateDBWorker,
    _writer_permit: OwnedMutexGuard<()>,
    base: Arc<DbSnapshot>,
    overlay: BTreeMap<Key, Option<Bytes>>,
    stats: WriteStats,
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
    pub fn new(options: SlateDBOptions) -> Result<Self, StorageError> {
        Self::open(options.path)
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let path = path.into();
        std::fs::create_dir_all(&path).map_err(|error| {
            StorageError::Io(format!(
                "create slatedb storage directory {}: {error}",
                path.display()
            ))
        })?;
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&path).map_err(object_store_error)?);
        Self::open_object_store(DB_PATH, object_store).map(|mut storage| {
            storage.path = path;
            storage
        })
    }

    pub fn open_object_store(
        db_path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, StorageError> {
        Self::open_object_store_with_options(
            db_path,
            object_store,
            SlateDBObjectStoreOptions::default(),
        )
    }

    pub fn open_object_store_with_options(
        db_path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        options: SlateDBObjectStoreOptions,
    ) -> Result<Self, StorageError> {
        validate_object_store_options(&options)?;
        let db_path = db_path.into();
        Ok(Self {
            worker: SlateDBWorker::start(db_path.clone(), object_store, options)?,
            path: PathBuf::from(db_path),
            write_gate: WriteGate::new(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn flush(&self) -> Result<(), StorageError> {
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
        _opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        async move {
            let snapshot = self
                .worker
                .call(|db| async move { db.snapshot().await.map_err(slatedb_error) })
                .await?;
            Ok(SlateDBRead {
                worker: self.worker.clone(),
                snapshot,
            })
        }
    }

    fn begin_write(
        &self,
        _opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        async move {
            let writer_permit = self.write_gate.acquire().await;
            let base = self
                .worker
                .call(|db| async move { db.snapshot().await.map_err(slatedb_error) })
                .await?;
            Ok(SlateDBWrite {
                worker: self.worker.clone(),
                _writer_permit: writer_permit,
                base,
                overlay: BTreeMap::new(),
                stats: WriteStats::default(),
            })
        }
    }
}

impl StorageRead for SlateDBRead {
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        async move {
            if keys.is_empty() {
                return Ok(GetManyResult::new(Vec::new()));
            }

            let physical_keys = keys
                .iter()
                .map(|key| physical_key(space, key))
                .collect::<Result<Vec<_>, _>>()?;
            let snapshot = Arc::clone(&self.snapshot);
            let values = self
                .worker
                .call(move |_db| get_snapshot_values(snapshot, physical_keys))
                .await?;
            Ok(GetManyResult::new(
                values
                    .into_iter()
                    .map(|value| value.map(|value| project_value(value, opts.projection)))
                    .collect(),
            ))
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
            let mut iter = Some(
                self.worker
                    .call(move |_db| open_snapshot_scan(snapshot, bounds))
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
                    .call(move |_db| {
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

            let base = Arc::clone(&self.base);
            let base_keys = self
                .worker
                .call(move |_db| collect_snapshot_keys(base, bounds))
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
                _writer_permit: writer_permit,
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

            worker
                .call(move |db| async move {
                    let _writer_permit = writer_permit;
                    let mut batch = WriteBatch::new();
                    for (key, value) in overlay {
                        match value {
                            Some(value) => batch.put_bytes(key.0, value),
                            None => batch.delete(key.0),
                        }
                    }
                    db.write_with_options(
                        batch,
                        &SlateDBWriteOptions {
                            await_durable: false,
                            ..SlateDBWriteOptions::default()
                        },
                    )
                    .await
                    .map_err(slatedb_error)?;
                    // SlateDB owns WAL durability. Returning here makes the
                    // commit visible immediately while its background flusher
                    // batches this write with nearby commits.
                    Ok(CommitResult {
                        commit_id: None,
                        stats,
                    })
                })
                .await
        }
    }

    fn rollback(self) -> impl Future<Output = Result<(), StorageError>> + Send {
        async { Ok(()) }
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
            Ok((runtime, db)) => Ok(Self {
                inner: Arc::new(SlateDBWorkerInner {
                    runtime,
                    db,
                    in_flight,
                    shutdown,
                    manager: Mutex::new(Some(thread)),
                }),
            }),
            Err(error) => {
                let _ = thread.join();
                Err(error)
            }
        }
    }

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
        let mut builder = Db::builder(db_path, object_store);
        if let Some(cache) = options.cache {
            let settings = Settings {
                object_store_cache_options: ObjectStoreCacheOptions {
                    root_folder: Some(cache.root_folder),
                    max_cache_size_bytes: Some(cache.max_disk_cache_bytes),
                    part_size_bytes: OBJECT_STORE_CACHE_PART_SIZE_BYTES,
                    cache_puts: true,
                    preload_disk_cache_on_startup: Some(PreloadLevel::AllSst),
                    scan_interval: None,
                    ..ObjectStoreCacheOptions::default()
                },
                ..Settings::default()
            };
            let db_cache = SplitCache::new()
                .with_block_cache(moka_cache(cache.block_cache_bytes))
                .with_meta_cache(moka_cache(cache.metadata_cache_bytes))
                .build();
            builder = builder
                .with_settings(settings)
                .with_db_cache(Arc::new(db_cache));
        } else {
            // The SlateDB dependency is compiled with Moka support so cached
            // callers can choose bounded capacities. Keep the legacy constructor
            // cacheless instead of accepting SlateDB's much larger defaults.
            builder = builder.with_db_cache_disabled();
        }
        builder.build().await.map_err(slatedb_error)
    })
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
}

async fn get_snapshot_values(
    snapshot: Arc<DbSnapshot>,
    keys: Vec<Key>,
) -> Result<Vec<Option<Bytes>>, StorageError> {
    stream::iter(keys)
        .map(|key| {
            let snapshot = Arc::clone(&snapshot);
            async move { snapshot.get(key.0).await.map_err(slatedb_error) }
        })
        .buffered(POINT_READ_CONCURRENCY)
        .try_collect()
        .await
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
) -> Result<DbIterator, StorageError> {
    let scan_options = slatedb_scan_options();
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

async fn collect_snapshot_keys(
    snapshot: Arc<DbSnapshot>,
    bounds: EncodedBounds,
) -> Result<Vec<Key>, StorageError> {
    let scan_options = slatedb_scan_options();
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

fn slatedb_scan_options() -> SlateDBScanOptions {
    // SlateDB's default scan options fetch one block at a time. Keep iteration
    // ordered, but let SlateDB prefetch remote SST blocks behind the iterator.
    SlateDBScanOptions::default()
        .with_read_ahead_bytes(SCAN_READ_AHEAD_BYTES)
        .with_max_fetch_tasks(SCAN_MAX_FETCH_TASKS)
        .with_cache_blocks(SCAN_CACHE_BLOCKS)
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
    StorageError::Io(format!("slatedb storage: {error}"))
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
    use std::ops::Range;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    #[test]
    fn open_object_store_round_trips_with_memory_store() {
        let storage = SlateDB::open_object_store("test-db", Arc::new(InMemory::new()))
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
        let result =
            block_on(read.get_many(space, &[key], GetOptions::default())).expect("read row");

        assert_eq!(result.values, vec![Some(ProjectedValue::FullValue(value))]);
    }

    #[test]
    fn commit_is_visible_while_background_wal_flush_is_blocked() {
        let store = Arc::new(BlockingStore::new(Arc::new(InMemory::new())));
        let storage = SlateDB::open_object_store("test-commit-visibility", store.clone())
            .expect("open commit visibility storage");
        let space = SpaceId(8);
        let key = Key(Bytes::from_static(b"visible-before-durable"));

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

        let read = block_on(storage.begin_read(ReadOptions::default()))
            .expect("begin visible in-memory read");
        let values = block_on(read.get_many(space, &[key], GetOptions::default()))
            .expect("read visible in-memory value")
            .values;
        assert_eq!(
            values,
            vec![Some(ProjectedValue::FullValue(Bytes::from_static(
                b"value"
            )))]
        );

        drop(blocked_write);
        block_on(storage.flush()).expect("flush visible value");
    }

    #[test]
    fn explicit_flush_reports_background_durability_failure() {
        let store = Arc::new(BlockingStore::new(Arc::new(InMemory::new())));
        let storage = SlateDB::open_object_store("test-failed-commit", store.clone())
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
        let storage =
            SlateDB::open_object_store(db_path, store.clone()).expect("open close-test storage");
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

        let reopened =
            SlateDB::open_object_store(db_path, store).expect("reopen close-test storage");
        let read =
            block_on(reopened.begin_read(ReadOptions::default())).expect("begin close-test read");
        let result = block_on(read.get_many(space, &[key], GetOptions::default()))
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
            let storage = SlateDB::open_object_store(db_path, inner.clone())
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
        let storage = SlateDB::open_object_store(db_path, store.clone())
            .expect("reopen concurrent-read storage");
        let read = Arc::new(
            block_on(storage.begin_read(ReadOptions::default()))
                .expect("begin shared snapshot read"),
        );
        let left_read = Arc::clone(&read);
        let right_read = Arc::clone(&read);
        let blocked_reads = store.block_sst_reads();

        let left = std::thread::spawn(move || {
            block_on(left_read.get_many(space, &[left_key], GetOptions::default()))
        });
        blocked_reads.wait_for_entries(1, "first SST read");
        let right = std::thread::spawn(move || {
            block_on(right_read.get_many(space, &[right_key], GetOptions::default()))
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
            let storage = SlateDB::open_object_store(db_path, inner.clone())
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
        let storage =
            SlateDB::open_object_store(db_path, store.clone()).expect("reopen async-read storage");
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
        let point_read = read.get_many(space, &keys, GetOptions::default());
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
            if self.block_reads.load(Ordering::Acquire)
                && location
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("sst"))
            {
                self.reads.enter();
            }
        }
    }

    impl std::fmt::Display for BlockingStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
