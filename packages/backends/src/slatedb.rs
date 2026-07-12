use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::JoinHandle;

use bytes::Bytes;
use futures_util::stream::{self, StreamExt, TryStreamExt};
use lix_engine::backend::{
    Backend, BackendError, BackendRead, BackendWrite, CommitResult, CoreProjection, GetOptions,
    Key, KeyRange, PointVisitor, ProjectedValue, ProjectedValueRef, PutBatch, ReadOptions,
    ScanOptions, ScanResult, ScanVisitor, SpaceId, StoredValue, WriteOptions, WriteStats,
};
use lix_engine::{BackendFactory, BackendFixture, BackendTestConfig};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use slatedb::config::{
    ObjectStoreCacheOptions, PreloadLevel, ScanOptions as SlateDbScanOptions, Settings,
    WriteOptions as SlateDbWriteOptions,
};
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::db_cache::{DbCache, SplitCache};
use slatedb::{Db, DbIterator, DbSnapshot, WriteBatch};
use tempfile::TempDir;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::Mutex as AsyncMutex;

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
pub struct SlateDbBackendFactory {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct SlateDbBackendFixture {
    path: PathBuf,
}

#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub struct SlateDbBackend {
    path: PathBuf,
    worker: SlateDbWorker,
    write_gate: WriteGate,
}

#[derive(Clone, Debug)]
pub struct SlateDbBackendOptions {
    pub path: PathBuf,
}

#[derive(Clone, Debug, Default)]
pub struct SlateDbObjectStoreOptions {
    pub cache: Option<SlateDbCacheOptions>,
}

#[derive(Clone, Debug)]
pub struct SlateDbCacheOptions {
    pub root_folder: PathBuf,
    pub max_disk_cache_bytes: usize,
    pub block_cache_bytes: u64,
    pub metadata_cache_bytes: u64,
}

#[allow(missing_debug_implementations)]
pub struct SlateDbRead {
    worker: SlateDbWorker,
    snapshot: Arc<DbSnapshot>,
}

#[allow(missing_debug_implementations)]
pub struct SlateDbWrite {
    worker: SlateDbWorker,
    _writer_permit: WriterPermit,
    base: Arc<DbSnapshot>,
    overlay: BTreeMap<Key, Option<Bytes>>,
    stats: WriteStats,
}

impl Default for SlateDbBackendFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SlateDbBackendFactory {
    pub fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create slatedb backend temp dir"),
            next_database_id: AtomicU64::new(0),
        }
    }
}

impl BackendFactory for SlateDbBackendFactory {
    type Backend = SlateDbBackend;
    type Fixture = SlateDbBackendFixture;

    fn create_fixture(&self) -> Self::Fixture {
        let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        let path = self
            .temp_dir
            .path()
            .join(format!("backend-{database_id}.slatedb"));
        SlateDbBackendFixture { path }
    }

    fn config(&self) -> BackendTestConfig {
        BackendTestConfig {
            ephemeral: false,
            supports_concurrent_writers: false,
            ..BackendTestConfig::default()
        }
    }
}

impl BackendFixture for SlateDbBackendFixture {
    type Backend = SlateDbBackend;

    fn open(&self) -> Self::Backend {
        SlateDbBackend::open(&self.path).expect("open slatedb backend")
    }
}

impl SlateDbBackend {
    pub fn new(options: SlateDbBackendOptions) -> Result<Self, BackendError> {
        Self::open(options.path)
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self, BackendError> {
        let path = path.into();
        std::fs::create_dir_all(&path).map_err(|error| {
            BackendError::Io(format!(
                "create slatedb backend directory {}: {error}",
                path.display()
            ))
        })?;
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&path).map_err(object_store_error)?);
        Self::open_object_store(DB_PATH, object_store).map(|mut backend| {
            backend.path = path;
            backend
        })
    }

    pub fn open_object_store(
        db_path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, BackendError> {
        Self::open_object_store_with_options(
            db_path,
            object_store,
            SlateDbObjectStoreOptions::default(),
        )
    }

    pub fn open_object_store_with_options(
        db_path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        options: SlateDbObjectStoreOptions,
    ) -> Result<Self, BackendError> {
        validate_object_store_options(&options)?;
        let db_path = db_path.into();
        Ok(Self {
            worker: SlateDbWorker::start(db_path.clone(), object_store, options)?,
            path: PathBuf::from(db_path),
            write_gate: WriteGate::new(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn flush(&self) -> Result<(), BackendError> {
        let durability_failed = Arc::clone(&self.worker.inner.durability_failed);
        self.worker.call_with_visibility_lock(move |db| async move {
            db.flush().await.map_err(|error| {
                durability_failed.store(true, Ordering::Release);
                slatedb_error(error)
            })
        })
    }
}

impl Backend for SlateDbBackend {
    type Read<'a>
        = SlateDbRead
    where
        Self: 'a;

    type Write<'a>
        = SlateDbWrite
    where
        Self: 'a;

    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        let snapshot = self.worker.call_with_visibility_lock(|db| async move {
            db.snapshot().await.map_err(slatedb_error)
        })?;
        Ok(SlateDbRead {
            worker: self.worker.clone(),
            snapshot,
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        let writer_permit = self.write_gate.acquire()?;
        self.worker.ensure_usable()?;
        let base = self
            .worker
            .call(|db| async move { db.snapshot().await.map_err(slatedb_error) })?;
        Ok(SlateDbWrite {
            worker: self.worker.clone(),
            _writer_permit: writer_permit,
            base,
            overlay: BTreeMap::new(),
            stats: WriteStats::default(),
        })
    }
}

impl BackendRead for SlateDbRead {
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
        if keys.is_empty() {
            return Ok(());
        }

        let physical_keys = keys
            .iter()
            .map(|key| physical_key(space, key))
            .collect::<Result<Vec<_>, _>>()?;
        let snapshot = Arc::clone(&self.snapshot);
        let values = self
            .worker
            .call(move |_db| get_snapshot_values(snapshot, physical_keys))?;

        for (index, (key, value)) in keys.iter().zip(values.iter()).enumerate() {
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
            return Ok(ScanResult::default());
        }

        let range = physical_range(space, range)?;
        let resume_after = opts
            .resume_after
            .map(|key| physical_key(space, key))
            .transpose()?;
        let bounds = EncodedBounds::new(range, resume_after.as_ref());
        if bounds.is_empty() {
            return Ok(ScanResult::default());
        }

        let snapshot = Arc::clone(&self.snapshot);
        let mut iter = Some(
            self.worker
                .call(move |_db| open_snapshot_scan(snapshot, bounds))?,
        );
        let mut emitted = 0usize;

        loop {
            let remaining = opts.limit_rows - emitted;
            let batch_limit = remaining.min(SCAN_BATCH_ROWS);
            let lookahead = batch_limit == remaining;
            let current_iter = iter
                .take()
                .expect("slatedb scan iterator is present until scan returns");
            let projection = opts.projection;
            let batch = self.worker.call(move |_db| {
                scan_snapshot_batch(current_iter, batch_limit, projection, lookahead)
            })?;
            let ScanBatch {
                iter: next_iter,
                entries,
                state,
            } = batch;

            for (key, value) in &entries {
                visitor.visit(key.as_ref(), value.as_ref())?;
            }
            emitted += entries.len();

            match state {
                ScanBatchState::Exhausted => {
                    return Ok(ScanResult {
                        emitted,
                        has_more: false,
                    });
                }
                ScanBatchState::HasMore => {
                    return Ok(ScanResult {
                        emitted,
                        has_more: true,
                    });
                }
                ScanBatchState::MoreUnknown => {
                    iter = Some(next_iter);
                }
            }
        }
    }
}

impl BackendWrite for SlateDbWrite {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        for entry in entries.entries {
            let key = physical_key(space, &entry.key)?;
            let value = stored_value_bytes(entry.value);
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            self.overlay.insert(key, Some(value));
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        for key in keys {
            self.overlay.insert(physical_key(space, key)?, None);
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), BackendError> {
        let range = physical_range(space, range)?;
        let bounds = EncodedBounds::new(range.clone(), None);
        if bounds.is_empty() {
            self.stats.deleted_ranges += 1;
            self.stats.backend_calls += 1;
            return Ok(());
        }

        let base = Arc::clone(&self.base);
        let base_keys = self
            .worker
            .call(move |_db| collect_snapshot_keys(base, bounds))?;

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
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        let stats = self.stats;
        if self.overlay.is_empty() {
            return Ok(CommitResult {
                commit_id: None,
                stats,
            });
        }

        let durability_failed = Arc::clone(&self.worker.inner.durability_failed);
        self.worker.call_with_visibility_lock(move |db| async move {
            let mut batch = WriteBatch::new();
            for (key, value) in self.overlay {
                match value {
                    Some(value) => batch.put_bytes(key.0, value),
                    None => batch.delete(key.0),
                }
            }
            db.write_with_options(
                batch,
                &SlateDbWriteOptions {
                    await_durable: false,
                    ..SlateDbWriteOptions::default()
                },
            )
            .await
            .map_err(|error| {
                durability_failed.store(true, Ordering::Release);
                slatedb_error(error)
            })?;
            // SlateDB's default durable write waits for its periodic WAL
            // flush (100 ms by default). The backend already serializes
            // writers, so separate commits cannot benefit from that group
            // commit window. Request and await the WAL flush immediately to
            // preserve the existing durability contract without the timer
            // latency floor.
            db.flush().await.map_err(|error| {
                durability_failed.store(true, Ordering::Release);
                slatedb_error(error)
            })?;
            Ok(CommitResult {
                commit_id: None,
                stats,
            })
        })
    }

    fn rollback(self) -> Result<(), BackendError> {
        Ok(())
    }
}

#[derive(Clone)]
#[allow(missing_debug_implementations)]
struct SlateDbWorker {
    inner: Arc<SlateDbWorkerInner>,
}

#[allow(missing_debug_implementations)]
struct SlateDbWorkerInner {
    runtime: Handle,
    db: Arc<Db>,
    // A commit becomes visible before its explicit WAL flush completes. New
    // snapshots share this lock with that write-plus-flush window; operations
    // on already-pinned snapshots remain fully concurrent.
    visibility_lock: Arc<AsyncMutex<()>>,
    // SlateDB applies writes to its visible memtable before WAL durability.
    // Publish a terminal failure before releasing the visibility lock so a
    // failed commit can never be captured by a later backend snapshot.
    durability_failed: Arc<AtomicBool>,
    #[cfg(test)]
    next_visibility_wait: Mutex<Option<mpsc::Sender<()>>>,
    shutdown: mpsc::Sender<()>,
    manager: Mutex<Option<JoinHandle<()>>>,
}

impl SlateDbWorker {
    fn start(
        db_path: String,
        object_store: Arc<dyn ObjectStore>,
        options: SlateDbObjectStoreOptions,
    ) -> Result<Self, BackendError> {
        let (shutdown, shutdown_rx) = mpsc::channel();
        let (opened_tx, opened_rx) = mpsc::channel::<Result<(Handle, Arc<Db>), BackendError>>();
        let thread = std::thread::Builder::new()
            .name("lix-slatedb-manager".to_string())
            .spawn(move || {
                run_slatedb_manager(db_path, object_store, options, shutdown_rx, opened_tx);
            })
            .map_err(|error| BackendError::Io(format!("spawn slatedb worker: {error}")))?;

        match opened_rx
            .recv()
            .map_err(|error| BackendError::Io(format!("slatedb worker did not open: {error}")))?
        {
            Ok((runtime, db)) => Ok(Self {
                inner: Arc::new(SlateDbWorkerInner {
                    runtime,
                    db,
                    visibility_lock: Arc::new(AsyncMutex::new(())),
                    durability_failed: Arc::new(AtomicBool::new(false)),
                    #[cfg(test)]
                    next_visibility_wait: Mutex::new(None),
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

    fn call<R, F, Fut>(&self, operation: F) -> Result<R, BackendError>
    where
        R: Send + 'static,
        F: FnOnce(Arc<Db>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, BackendError>> + Send + 'static,
    {
        self.call_inner(None, operation)
    }

    fn ensure_usable(&self) -> Result<(), BackendError> {
        if self.inner.durability_failed.load(Ordering::Acquire) {
            Err(BackendError::Durability)
        } else {
            Ok(())
        }
    }

    fn call_with_visibility_lock<R, F, Fut>(&self, operation: F) -> Result<R, BackendError>
    where
        R: Send + 'static,
        F: FnOnce(Arc<Db>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, BackendError>> + Send + 'static,
    {
        self.call_inner(Some(Arc::clone(&self.inner.visibility_lock)), operation)
    }

    fn call_inner<R, F, Fut>(
        &self,
        visibility_lock: Option<Arc<AsyncMutex<()>>>,
        operation: F,
    ) -> Result<R, BackendError>
    where
        R: Send + 'static,
        F: FnOnce(Arc<Db>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, BackendError>> + Send + 'static,
    {
        let (reply_tx, reply_rx) = mpsc::channel();
        let db = Arc::clone(&self.inner.db);
        let durability_failed = visibility_lock
            .as_ref()
            .map(|_| Arc::clone(&self.inner.durability_failed));
        #[cfg(test)]
        let visibility_wait = if visibility_lock.is_some() {
            self.inner
                .next_visibility_wait
                .lock()
                .expect("lock visibility wait probe")
                .take()
        } else {
            None
        };
        self.inner.runtime.spawn(async move {
            let _visibility_guard = match visibility_lock {
                Some(visibility_lock) => {
                    #[cfg(test)]
                    if let Some(visibility_wait) = visibility_wait {
                        let _ = visibility_wait.send(());
                    }
                    Some(visibility_lock.lock_owned().await)
                }
                None => None,
            };
            let result = if durability_failed.is_some_and(|failed| failed.load(Ordering::Acquire)) {
                Err(BackendError::Durability)
            } else {
                operation(db).await
            };
            let _ = reply_tx.send(result);
        });
        reply_rx
            .recv()
            .map_err(|error| BackendError::Io(format!("receive slatedb worker reply: {error}")))?
    }

    #[cfg(test)]
    fn observe_next_visibility_wait(&self) -> mpsc::Receiver<()> {
        let (wait_tx, wait_rx) = mpsc::channel();
        *self
            .inner
            .next_visibility_wait
            .lock()
            .expect("lock visibility wait probe") = Some(wait_tx);
        wait_rx
    }
}

impl Drop for SlateDbWorkerInner {
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
    options: SlateDbObjectStoreOptions,
    shutdown: mpsc::Receiver<()>,
    opened: mpsc::Sender<Result<(Handle, Arc<Db>), BackendError>>,
) {
    let runtime = match Builder::new_multi_thread()
        .worker_threads(RUNTIME_WORKER_THREADS)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = opened.send(Err(BackendError::Io(format!(
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
    let _ = runtime.block_on(db.close());
}

fn open_slatedb(
    runtime: &Runtime,
    db_path: String,
    object_store: Arc<dyn ObjectStore>,
    options: SlateDbObjectStoreOptions,
) -> Result<Db, BackendError> {
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

fn validate_object_store_options(options: &SlateDbObjectStoreOptions) -> Result<(), BackendError> {
    let Some(cache) = &options.cache else {
        return Ok(());
    };
    if cache.root_folder.as_os_str().is_empty() {
        return Err(BackendError::Io(
            "slatedb cache root folder must not be empty".to_string(),
        ));
    }
    if cache.max_disk_cache_bytes == 0 {
        return Err(BackendError::Io(
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

fn physical_key(space: SpaceId, key: &Key) -> Result<Key, BackendError> {
    let len = SPACE_PREFIX_LEN + key.0.len();
    if len > MAX_SLATEDB_KEY_LEN {
        return Err(BackendError::InvalidKey);
    }
    let mut bytes = Vec::with_capacity(len);
    bytes.extend_from_slice(&space.0.to_be_bytes());
    bytes.extend_from_slice(&key.0);
    Ok(Key(Bytes::from(bytes)))
}

fn physical_range(space: SpaceId, range: KeyRange) -> Result<KeyRange, BackendError> {
    let map = |bound: Bound<Key>, unbounded: Bound<Key>| -> Result<Bound<Key>, BackendError> {
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
) -> Result<Vec<Option<Bytes>>, BackendError> {
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
) -> Result<DbIterator, BackendError> {
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
) -> Result<ScanBatch, BackendError> {
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
            return Err(BackendError::Corruption(format!(
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
) -> Result<Vec<Key>, BackendError> {
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

fn slatedb_scan_options() -> SlateDbScanOptions {
    // SlateDB's default scan options fetch one block at a time. Keep iteration
    // ordered, but let SlateDB prefetch remote SST blocks behind the iterator.
    SlateDbScanOptions::default()
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

fn project_value_ref(value: &[u8], projection: CoreProjection) -> ProjectedValueRef<'_> {
    match projection {
        CoreProjection::KeyOnly => ProjectedValueRef::KeyOnly,
        CoreProjection::FullValue => ProjectedValueRef::FullValue(value),
    }
}

fn slatedb_error(error: slatedb::Error) -> BackendError {
    BackendError::Io(format!("slatedb backend: {error}"))
}

fn object_store_error(error: object_store::Error) -> BackendError {
    BackendError::Io(format!("slatedb object store: {error}"))
}

#[derive(Clone, Default)]
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
                BackendError::Io(format!("slatedb writer gate poisoned: {error}"))
            })?;
        while *active {
            active = self.state.available.wait(active).map_err(|error| {
                BackendError::Io(format!("slatedb writer gate poisoned: {error}"))
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
    use super::*;
    use async_trait::async_trait;
    use futures_util::stream::BoxStream;
    use lix_engine::backend::{
        Backend, BackendWrite, GetOptions, ProjectedValue, PutEntry, ReadOptions, StoredValue,
        WriteOptions, get_many,
    };
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{
        CopyOptions, Error as ObjectStoreError, GetOptions as ObjectStoreGetOptions, GetResult,
        ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, PutOptions, PutPayload,
        PutResult, RenameOptions, Result as ObjectStoreResult,
    };
    use std::ops::Range;
    use std::time::{Duration, Instant};

    #[test]
    fn open_object_store_round_trips_with_memory_store() {
        let backend = SlateDbBackend::open_object_store("test-db", Arc::new(InMemory::new()))
            .expect("open memory object-store slatedb backend");

        let space = SpaceId(7);
        let key = Key(Bytes::from_static(b"hello"));
        let value = Bytes::from_static(b"world");

        let mut write = backend
            .begin_write(WriteOptions::default())
            .expect("begin write");
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
            .expect("put row");
        write.commit().expect("commit row");

        let read = backend
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let result = get_many(&read, space, &[key], GetOptions::default()).expect("read row");

        assert_eq!(result.values, vec![Some(ProjectedValue::FullValue(value))]);
    }

    #[test]
    fn visibility_operations_wait_for_commit_wal_durability() {
        let store = Arc::new(BlockingStore::new(Arc::new(InMemory::new())));
        let backend = SlateDbBackend::open_object_store("test-commit-visibility", store.clone())
            .expect("open commit visibility backend");
        let space = SpaceId(8);
        let key = Key(Bytes::from_static(b"visible-after-durable"));

        let blocked_write = store.block_next_write();
        let commit_backend = backend.clone();
        let commit_key = key.clone();
        let commit = std::thread::spawn(move || {
            let mut write = commit_backend
                .begin_write(WriteOptions::default())
                .expect("begin visibility write");
            write
                .put_many(
                    space,
                    PutBatch {
                        entries: vec![PutEntry {
                            key: commit_key,
                            value: StoredValue {
                                bytes: Bytes::from_static(b"value"),
                            },
                        }],
                    },
                )
                .expect("stage visibility write");
            write.commit()
        });
        blocked_write.wait_for_entries(1, "SlateDB WAL write");

        let visibility_wait = backend.worker.observe_next_visibility_wait();
        let read_backend = backend.clone();
        let read_key = key;
        let (read_result_tx, read_result_rx) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            let result = (|| {
                let read = read_backend.begin_read(ReadOptions::default())?;
                get_many(&read, space, &[read_key], GetOptions::default())
                    .map(|result| result.values)
            })();
            let _ = read_result_tx.send(result);
        });

        visibility_wait
            .recv_timeout(Duration::from_secs(2))
            .expect("reader should reach the visibility lock");
        assert!(
            matches!(
                read_result_rx.recv_timeout(Duration::from_millis(50)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "begin_read must remain queued until the WAL write is durable"
        );

        let flush_wait = backend.worker.observe_next_visibility_wait();
        let flush_backend = backend;
        let (flush_result_tx, flush_result_rx) = mpsc::channel();
        let flusher = std::thread::spawn(move || {
            let _ = flush_result_tx.send(flush_backend.flush());
        });
        flush_wait
            .recv_timeout(Duration::from_secs(2))
            .expect("flush should reach the visibility lock");
        assert!(
            matches!(
                flush_result_rx.recv_timeout(Duration::from_millis(50)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "flush must remain queued until the commit WAL write finishes"
        );

        drop(blocked_write);
        commit
            .join()
            .expect("join visibility commit")
            .expect("commit visibility value");
        let values = read_result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("visibility read should finish after WAL durability")
            .expect("read visibility value");
        assert_eq!(
            values,
            vec![Some(ProjectedValue::FullValue(Bytes::from_static(
                b"value"
            )))]
        );
        reader.join().expect("join visibility reader");
        flush_result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("flush should finish after commit WAL durability")
            .expect("flush committed database");
        flusher.join().expect("join queued flusher");
    }

    #[test]
    fn failed_commit_rejects_queued_readers_and_future_writers() {
        let store = Arc::new(BlockingStore::new(Arc::new(InMemory::new())));
        let backend = SlateDbBackend::open_object_store("test-failed-commit", store.clone())
            .expect("open failed commit backend");
        let space = SpaceId(9);
        let key = Key(Bytes::from_static(b"rejected"));

        let blocked_write = store.block_next_write();
        store.fail_writes();
        let commit_backend = backend.clone();
        let commit = std::thread::spawn(move || {
            let mut write = commit_backend
                .begin_write(WriteOptions::default())
                .expect("begin failing write");
            write
                .put_many(
                    space,
                    PutBatch {
                        entries: vec![PutEntry {
                            key,
                            value: StoredValue {
                                bytes: Bytes::from_static(b"not-durable"),
                            },
                        }],
                    },
                )
                .expect("stage failing write");
            write.commit()
        });
        blocked_write.wait_for_entries(1, "failing SlateDB WAL write");

        let visibility_wait = backend.worker.observe_next_visibility_wait();
        let read_backend = backend.clone();
        let reader =
            std::thread::spawn(move || read_backend.begin_read(ReadOptions::default()).map(|_| ()));
        visibility_wait
            .recv_timeout(Duration::from_secs(2))
            .expect("reader should queue on the visibility lock");

        drop(blocked_write);
        let commit_error = commit
            .join()
            .expect("join failing commit")
            .expect_err("WAL failure must fail commit");
        assert!(
            matches!(commit_error, BackendError::Io(message) if message.contains("injected write failure")),
            "commit should preserve the SlateDB write error"
        );
        assert_eq!(
            reader
                .join()
                .expect("join queued reader")
                .expect_err("queued reader must reject a failed commit"),
            BackendError::Durability
        );
        assert_eq!(
            backend
                .begin_write(WriteOptions::default())
                .map(|_| ())
                .expect_err("future writers must reject a failed commit"),
            BackendError::Durability
        );
    }

    #[test]
    fn independent_backend_reads_overlap() {
        let inner = Arc::new(InMemory::new());
        let db_path = "test-concurrent-reads";
        let space = SpaceId(9);
        let left_key = Key(Bytes::from_static(b"left"));
        let right_key = Key(Bytes::from_static(b"right"));
        let value = Bytes::from(vec![b'x'; 128 * 1024]);

        {
            let backend = SlateDbBackend::open_object_store(db_path, inner.clone())
                .expect("open concurrent-read seed backend");
            let mut write = backend
                .begin_write(WriteOptions::default())
                .expect("begin concurrent-read seed write");
            write
                .put_many(
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
                )
                .expect("stage concurrent-read seed values");
            write.commit().expect("commit concurrent-read seed values");
        }

        let store = Arc::new(BlockingStore::new(inner));
        let backend = SlateDbBackend::open_object_store(db_path, store.clone())
            .expect("reopen concurrent-read backend");
        let left_read = backend
            .begin_read(ReadOptions::default())
            .expect("begin left read");
        let right_read = backend
            .begin_read(ReadOptions::default())
            .expect("begin right read");
        let blocked_reads = store.block_sst_reads();

        let left = std::thread::spawn(move || {
            get_many(&left_read, space, &[left_key], GetOptions::default())
        });
        blocked_reads.wait_for_entries(1, "first SST read");
        let right = std::thread::spawn(move || {
            get_many(&right_read, space, &[right_key], GetOptions::default())
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
