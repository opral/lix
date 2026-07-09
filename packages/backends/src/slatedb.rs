use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
};
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::db_cache::{DbCache, SplitCache};
use slatedb::{Db, DbIterator, DbSnapshot, WriteBatch};
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

const DB_PATH: &str = "db";
const SPACE_PREFIX_LEN: usize = 4;
const MAX_SLATEDB_KEY_LEN: usize = u16::MAX as usize;
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
        self.worker
            .call(|runtime, db| runtime.block_on(db.flush()).map_err(slatedb_error))
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
        let snapshot = self
            .worker
            .call(|runtime, db| runtime.block_on(db.snapshot()).map_err(slatedb_error))?;
        Ok(SlateDbRead {
            worker: self.worker.clone(),
            snapshot,
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        let writer_permit = self.write_gate.acquire()?;
        let base = self
            .worker
            .call(|runtime, db| runtime.block_on(db.snapshot()).map_err(slatedb_error))?;
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
            .call(move |runtime, _db| get_snapshot_values(runtime, snapshot, physical_keys))?;

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
                .call(move |runtime, _db| open_snapshot_scan(runtime, &snapshot, bounds))?,
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
            let batch = self.worker.call(move |runtime, _db| {
                scan_snapshot_batch(runtime, current_iter, batch_limit, projection, lookahead)
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
            .call(move |runtime, _db| collect_snapshot_keys(runtime, &base, bounds))?;

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

        self.worker.call(move |runtime, db| {
            let mut batch = WriteBatch::new();
            for (key, value) in self.overlay {
                match value {
                    Some(value) => batch.put_bytes(key.0, value),
                    None => batch.delete(key.0),
                }
            }
            runtime.block_on(db.write(batch)).map_err(slatedb_error)?;
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
    commands: mpsc::Sender<SlateDbCommand>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

enum SlateDbCommand {
    Run(Box<dyn FnOnce(&Runtime, &Db) + Send + 'static>),
    Shutdown,
}

impl SlateDbWorker {
    fn start(
        db_path: String,
        object_store: Arc<dyn ObjectStore>,
        options: SlateDbObjectStoreOptions,
    ) -> Result<Self, BackendError> {
        let (commands, receiver) = mpsc::channel();
        let (opened_tx, opened_rx) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("lix-slatedb".to_string())
            .spawn(move || {
                run_slatedb_worker(db_path, object_store, options, receiver, opened_tx);
            })
            .map_err(|error| BackendError::Io(format!("spawn slatedb worker: {error}")))?;

        match opened_rx
            .recv()
            .map_err(|error| BackendError::Io(format!("slatedb worker did not open: {error}")))?
        {
            Ok(()) => Ok(Self {
                inner: Arc::new(SlateDbWorkerInner {
                    commands,
                    thread: Mutex::new(Some(thread)),
                }),
            }),
            Err(error) => {
                let _ = thread.join();
                Err(error)
            }
        }
    }

    fn call<R>(
        &self,
        operation: impl FnOnce(&Runtime, &Db) -> Result<R, BackendError> + Send + 'static,
    ) -> Result<R, BackendError>
    where
        R: Send + 'static,
    {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.inner
            .commands
            .send(SlateDbCommand::Run(Box::new(move |runtime, db| {
                let _ = reply_tx.send(operation(runtime, db));
            })))
            .map_err(|error| BackendError::Io(format!("send slatedb worker command: {error}")))?;
        reply_rx
            .recv()
            .map_err(|error| BackendError::Io(format!("receive slatedb worker reply: {error}")))?
    }
}

impl Drop for SlateDbWorkerInner {
    fn drop(&mut self) {
        let _ = self.commands.send(SlateDbCommand::Shutdown);
        let Ok(mut thread) = self.thread.lock() else {
            return;
        };
        if let Some(thread) = thread.take() {
            let _ = thread.join();
        }
    }
}

fn run_slatedb_worker(
    db_path: String,
    object_store: Arc<dyn ObjectStore>,
    options: SlateDbObjectStoreOptions,
    receiver: mpsc::Receiver<SlateDbCommand>,
    opened: mpsc::Sender<Result<(), BackendError>>,
) {
    let runtime = match Builder::new_current_thread().enable_all().build() {
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

    let _ = opened.send(Ok(()));

    while let Ok(command) = receiver.recv() {
        match command {
            SlateDbCommand::Run(operation) => operation(&runtime, &db),
            SlateDbCommand::Shutdown => break,
        }
    }

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

fn get_snapshot_values(
    runtime: &Runtime,
    snapshot: Arc<DbSnapshot>,
    keys: Vec<Key>,
) -> Result<Vec<Option<Bytes>>, BackendError> {
    runtime.block_on(async move {
        stream::iter(keys)
            .map(|key| {
                let snapshot = Arc::clone(&snapshot);
                async move { snapshot.get(key.0).await.map_err(slatedb_error) }
            })
            .buffered(POINT_READ_CONCURRENCY)
            .try_collect()
            .await
    })
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

fn open_snapshot_scan(
    runtime: &Runtime,
    snapshot: &DbSnapshot,
    bounds: EncodedBounds,
) -> Result<DbIterator, BackendError> {
    runtime.block_on(async {
        let scan_options = slatedb_scan_options();
        snapshot
            .scan_with_options(bounds.range(), &scan_options)
            .await
            .map_err(slatedb_error)
    })
}

fn scan_snapshot_batch(
    runtime: &Runtime,
    mut iter: DbIterator,
    limit_rows: usize,
    projection: CoreProjection,
    lookahead: bool,
) -> Result<ScanBatch, BackendError> {
    runtime.block_on(async {
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
    })
}

fn collect_snapshot_keys(
    runtime: &Runtime,
    snapshot: &DbSnapshot,
    bounds: EncodedBounds,
) -> Result<Vec<Key>, BackendError> {
    runtime.block_on(async {
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
    })
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
    use lix_engine::backend::{
        Backend, BackendWrite, GetOptions, ProjectedValue, PutEntry, ReadOptions, StoredValue,
        WriteOptions, get_many,
    };
    use object_store::memory::InMemory;

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
}
