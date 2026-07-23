#![allow(
    clippy::manual_async_fn,
    reason = "explicit future signatures mirror Storage traits and keep Send guarantees visible"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::future::Future;
use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use lix_engine::wasm::WasmRuntime;
use lix_engine::{
    CommitResult, Engine, Key, KeyRange, LixError, PutBatch, ReadOptions, SessionContext, SpaceId,
    Storage, StorageError, StorageWrite, Value, WriteOptions,
};
use notify_debouncer_full::notify::{Config, RecommendedWatcher, RecursiveMode};
use notify_debouncer_full::{DebounceEventResult, Debouncer, RecommendedCache, new_debouncer_opt};
use tokio::sync::oneshot;

#[cfg(feature = "local_filesystem")]
use lix_local_filesystem::RocksDBFilesystem;

type FilesystemDebouncer = Debouncer<RecommendedWatcher, RecommendedCache>;
const LIX_DIRECTORY_GITIGNORE: &[u8] = b"*\n";
const FILESYSTEM_POLL_INTERVAL: Duration = Duration::from_secs(15);
const FILE_UPSERT_BATCH_MAX_ROWS: usize = 500;
const FILE_UPSERT_BATCH_MAX_BYTES: usize = 8 * 1024 * 1024;
const FILESYSTEM_PARALLEL_SNAPSHOT_MAX_WORKERS: usize = 8;
// Avoid paying thread startup cost for tiny directory roots.
const FILESYSTEM_PARALLEL_SNAPSHOT_MIN_DIRS: usize = 4;

#[derive(Clone)]
pub(crate) struct FilesystemSync<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    inner: StorageImpl,
    supervisor: FilesystemSupervisor<StorageImpl>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FilesystemLayout {
    root: PathBuf,
    lix_dir: PathBuf,
    lix_dir_is_default: bool,
}

impl FilesystemLayout {
    fn lix_path_to_local_path(&self, path: &str) -> Result<PathBuf, LixError> {
        if path == "/.lix" {
            Ok(self.lix_dir.clone())
        } else if let Some(rest) = path.strip_prefix("/.lix/") {
            lix_path_to_local_path(&self.lix_dir, &format!("/{rest}"))
        } else {
            lix_path_to_local_path(&self.root, path)
        }
    }

    fn local_path_to_lix_path(&self, path: &Path, is_directory: bool) -> Result<String, LixError> {
        if path.starts_with(&self.lix_dir) {
            let path = local_path_to_lix_path(&self.lix_dir, path, is_directory)?;
            if path == "/" {
                return Ok(if is_directory {
                    "/.lix/".to_string()
                } else {
                    "/.lix".to_string()
                });
            }
            return Ok(format!("/.lix{path}"));
        }
        local_path_to_lix_path(&self.root, path, is_directory)
    }

    fn local_base_for_path(&self, path: &Path) -> Option<&Path> {
        if path.starts_with(&self.lix_dir) {
            Some(&self.lix_dir)
        } else if path.starts_with(&self.root) {
            Some(&self.root)
        } else {
            None
        }
    }

    fn lix_dir_is_inside_root(&self) -> bool {
        self.lix_dir.starts_with(&self.root)
    }
}

#[cfg(feature = "local_filesystem")]
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LocalFilesystemOpenOptions {
    pub root: PathBuf,
    pub lix_dir: Option<PathBuf>,
    pub sync_all_files: bool,
}

#[cfg(feature = "local_filesystem")]
impl LocalFilesystemOpenOptions {
    pub fn new<P>(root: P, sync_all_files: bool) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            root: root.into(),
            lix_dir: None,
            sync_all_files,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct FilesystemPathFilter {
    include_files: Option<BTreeSet<String>>,
}

pub(crate) struct FilesystemWrite<'a, StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    inner: StorageImpl::Write<'a>,
    supervisor: FilesystemSupervisor<StorageImpl>,
}

#[cfg(feature = "local_filesystem")]
#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct LocalFilesystem {
    inner: FilesystemSync<RocksDBFilesystem>,
}

#[cfg(feature = "local_filesystem")]
pub type LocalFilesystemRead<'a> = lix_local_filesystem::RocksDBFilesystemRead<'a>;

#[cfg(feature = "local_filesystem")]
#[expect(missing_debug_implementations)]
pub struct LocalFilesystemWrite<'a> {
    inner: FilesystemWrite<'a, RocksDBFilesystem>,
}

#[derive(Clone)]
struct FilesystemSupervisor<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    inner: Arc<FilesystemSupervisorInner>,
    _marker: PhantomData<fn() -> StorageImpl>,
}

struct FilesystemSupervisorInner {
    event_tx: mpsc::Sender<FilesystemEvent>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

struct FilesystemWatcher {
    debouncer: FilesystemDebouncer,
    watched_paths: Vec<FilesystemWatchPath>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FilesystemWatchPath {
    path: PathBuf,
    recursive: bool,
}

struct FilesystemState<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    session: SessionContext<StorageImpl>,
    layout: FilesystemLayout,
    path_filter: Mutex<FilesystemPathFilter>,
    sync_lock: tokio::sync::Mutex<()>,
    last_materialized: Mutex<Option<MaterializedSnapshot>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Snapshot {
    directories: BTreeSet<String>,
    files: BTreeMap<String, Vec<u8>>,
    unmanaged_paths: BTreeSet<String>,
}

impl Snapshot {
    fn filtered(&self, path_filter: &FilesystemPathFilter) -> Self {
        if path_filter.is_unfiltered() {
            return self.clone();
        }
        Self {
            directories: self
                .directories
                .iter()
                .filter(|path| path_filter.includes_directory(path))
                .cloned()
                .collect(),
            files: self
                .files
                .iter()
                .filter(|(path, _)| path_filter.includes_file(path))
                .map(|(path, data)| (path.clone(), data.clone()))
                .collect(),
            unmanaged_paths: self
                .unmanaged_paths
                .iter()
                .filter(|path| path_filter.includes_path(path))
                .cloned()
                .collect(),
        }
    }
}

impl FilesystemPathFilter {
    fn from_sync_all_files(sync_all_files: bool) -> Self {
        let include_files = if sync_all_files {
            None
        } else {
            Some(BTreeSet::new())
        };
        Self { include_files }
    }

    fn is_unfiltered(&self) -> bool {
        self.include_files.is_none()
    }

    fn add_file(&mut self, path: &str) -> bool {
        let Some(include_files) = self.include_files.as_mut() else {
            return false;
        };
        include_files.insert(path.to_string())
    }

    fn remove_file(&mut self, path: &str) -> bool {
        let Some(include_files) = self.include_files.as_mut() else {
            return false;
        };
        include_files.remove(path)
    }

    fn explicitly_includes_file(&self, path: &str) -> bool {
        self.include_files
            .as_ref()
            .is_some_and(|include_files| include_files.contains(path))
    }

    fn includes_file(&self, path: &str) -> bool {
        is_lix_storage_path(path)
            || self
                .include_files
                .as_ref()
                .is_none_or(|include_files| include_files.contains(path))
    }

    fn includes_directory(&self, path: &str) -> bool {
        if is_lix_storage_path(path) {
            return true;
        }
        if path == "/" {
            return true;
        }
        let Some(include_files) = self.include_files.as_ref() else {
            return true;
        };
        include_files.iter().any(|file_path| {
            let directory = parent_lix_directory_path(file_path);
            lix_directory_contains_directory(path, &directory)
        })
    }

    fn includes_path(&self, path: &str) -> bool {
        if path.ends_with('/') {
            self.includes_directory(path)
        } else {
            self.includes_file(path)
        }
    }

    fn local_watch_paths(&self, layout: &FilesystemLayout) -> Result<Vec<PathBuf>, LixError> {
        let mut paths = BTreeSet::new();
        let Some(include_files) = self.include_files.as_ref() else {
            return Ok(paths.into_iter().collect());
        };
        for path in include_files {
            let local_path = layout.lix_path_to_local_path(path)?;
            if local_path.exists() {
                paths.insert(local_path.clone());
            }
            if let Some(parent) = local_path.parent() {
                paths.insert(parent.to_path_buf());
            }
        }
        Ok(paths.into_iter().collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MaterializedSnapshot {
    disk: Snapshot,
    lix_revision: LixRevision,
    lix_file_paths: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LixRevision {
    active_branch_id: String,
    active_branch_commit_id: String,
    storage_mutation_revision: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LixSnapshotRead {
    snapshot: Snapshot,
    revision: LixRevision,
}

enum FilesystemEvent {
    DiskChanged,
    SyncDiskToLix {
        reply_tx: oneshot::Sender<Result<(), LixError>>,
    },
    SyncFromLix {
        reply_tx: oneshot::Sender<Result<(), LixError>>,
    },
    ImportPaths {
        paths: Vec<String>,
        reply_tx: oneshot::Sender<Result<(), LixError>>,
    },
    Shutdown,
}

#[cfg(feature = "local_filesystem")]
impl LocalFilesystem {
    pub async fn open<P>(dir: P) -> Result<Self, LixError>
    where
        P: AsRef<Path>,
    {
        Box::pin(Self::open_with_options(LocalFilesystemOpenOptions {
            root: dir.as_ref().to_path_buf(),
            lix_dir: None,
            sync_all_files: true,
        }))
        .await
    }

    pub async fn open_with_options(options: LocalFilesystemOpenOptions) -> Result<Self, LixError> {
        Box::pin(Self::open_with_options_and_runtime(options, None)).await
    }

    /// Opens a filesystem storage whose disk-sync supervisor uses the same
    /// component runtime as the Lix session that owns it.
    pub async fn open_with_options_and_wasm_runtime(
        options: LocalFilesystemOpenOptions,
        wasm_runtime: Arc<dyn WasmRuntime>,
    ) -> Result<Self, LixError> {
        Box::pin(Self::open_with_options_and_runtime(
            options,
            Some(wasm_runtime),
        ))
        .await
    }

    async fn open_with_options_and_runtime(
        options: LocalFilesystemOpenOptions,
        wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    ) -> Result<Self, LixError> {
        let layout = prepare_filesystem_layout(&options.root, options.lix_dir.as_deref())?;
        let storage = open_filesystem_rocksdb(&layout)?;
        let engine =
            crate::lix::open_or_initialize_engine(storage.clone(), wasm_runtime, None).await?;
        let inner = Box::pin(FilesystemSync::open_with_engine(
            storage,
            engine,
            layout,
            options.sync_all_files,
        ))
        .await?;
        Ok(Self { inner })
    }

    pub async fn import_paths<I, S>(&self, paths: I) -> Result<(), LixError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.inner.import_paths(paths).await
    }

    pub async fn sync_disk_to_lix(&self) -> Result<(), LixError> {
        self.inner.sync_disk_to_lix().await
    }
}

#[cfg(feature = "local_filesystem")]
impl Storage for LocalFilesystem {
    type Read<'a>
        = LocalFilesystemRead<'a>
    where
        Self: 'a;

    type Write<'a>
        = LocalFilesystemWrite<'a>
    where
        Self: 'a;

    fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        self.inner.begin_read(opts)
    }

    fn begin_write(
        &self,
        opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        async move {
            Ok(LocalFilesystemWrite {
                inner: self.inner.begin_write(opts).await?,
            })
        }
    }
}

#[cfg(feature = "local_filesystem")]
impl StorageWrite for LocalFilesystemWrite<'_> {
    fn put_many(
        &mut self,
        space: SpaceId,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.put_many(space, entries)
    }

    fn delete_many(
        &mut self,
        space: SpaceId,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.delete_many(space, keys)
    }

    fn delete_range(
        &mut self,
        space: SpaceId,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.delete_range(space, range)
    }

    fn commit(self) -> impl Future<Output = Result<CommitResult, StorageError>> + Send {
        self.inner.commit()
    }

    fn rollback(self) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.rollback()
    }
}

impl<StorageImpl> FilesystemSync<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    async fn open_with_engine(
        storage: StorageImpl,
        engine: Engine<StorageImpl>,
        layout: FilesystemLayout,
        sync_all_files: bool,
    ) -> Result<Self, LixError> {
        Ok(Self {
            inner: storage,
            supervisor: FilesystemSupervisor::open(engine, layout, sync_all_files).await?,
        })
    }

    async fn import_paths<I, S>(&self, paths: I) -> Result<(), LixError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.supervisor
            .import_paths(
                paths
                    .into_iter()
                    .map(|path| path.as_ref().to_string())
                    .collect(),
            )
            .await
    }

    async fn sync_disk_to_lix(&self) -> Result<(), LixError> {
        self.supervisor.sync_disk_to_lix().await
    }
}

impl<StorageImpl> Storage for FilesystemSync<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    type Read<'a>
        = StorageImpl::Read<'a>
    where
        Self: 'a;

    type Write<'a>
        = FilesystemWrite<'a, StorageImpl>
    where
        Self: 'a;

    fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        self.inner.begin_read(opts)
    }

    fn begin_write(
        &self,
        opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        async move {
            Ok(FilesystemWrite {
                inner: self.inner.begin_write(opts).await?,
                supervisor: self.supervisor.clone(),
            })
        }
    }
}

impl<StorageImpl> StorageWrite for FilesystemWrite<'_, StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    fn put_many(
        &mut self,
        space: SpaceId,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.put_many(space, entries)
    }

    fn delete_many(
        &mut self,
        space: SpaceId,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.delete_many(space, keys)
    }

    fn delete_range(
        &mut self,
        space: SpaceId,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.delete_range(space, range)
    }

    fn commit(self) -> impl Future<Output = Result<CommitResult, StorageError>> + Send {
        async move {
            let result = self.inner.commit().await?;
            self.supervisor.sync_from_lix().await?;
            Ok(result)
        }
    }

    fn rollback(self) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.rollback()
    }
}

impl<StorageImpl> FilesystemSupervisor<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    async fn open(
        engine: Engine<StorageImpl>,
        layout: FilesystemLayout,
        sync_all_files: bool,
    ) -> Result<Self, LixError> {
        validate_filesystem_root_directory(&layout.root)?;
        validate_filesystem_lix_directory(&layout.lix_dir)?;
        let path_filter = FilesystemPathFilter::from_sync_all_files(sync_all_files);
        let session = engine.open_workspace_session().await?;
        let state = Arc::new(FilesystemState {
            session,
            layout,
            path_filter: Mutex::new(path_filter),
            sync_lock: tokio::sync::Mutex::new(()),
            last_materialized: Mutex::new(None),
        });

        state.sync_disk_to_lix(false).await?;
        state.sync_from_lix().await?;

        let (event_tx, event_rx) = mpsc::channel();
        let callback_tx = event_tx.clone();
        let watcher_config = Config::default().with_follow_symlinks(false);
        let debouncer = new_debouncer_opt::<_, RecommendedWatcher, RecommendedCache>(
            Duration::from_millis(500),
            None,
            move |_result: DebounceEventResult| {
                let _ = callback_tx.send(FilesystemEvent::DiskChanged);
            },
            RecommendedCache::new(),
            watcher_config,
        )
        .ok()
        .and_then(|debouncer| {
            let path_filter = state.path_filter();
            let mut watcher = FilesystemWatcher {
                debouncer,
                watched_paths: Vec::new(),
            };
            if watcher.refresh(&state.layout, &path_filter).is_ok() {
                Some(watcher)
            } else {
                watcher.stop();
                None
            }
        });
        let poll_filesystem = cfg!(target_os = "macos") || debouncer.is_none();
        let worker_state = Arc::clone(&state);
        let worker = thread::Builder::new()
            .name("lix-sdk-filesystem-sync".to_string())
            .spawn(move || filesystem_worker(worker_state, event_rx, poll_filesystem, debouncer))
            .map_err(|error| {
                LixError::new(
                    "LIX_FILESYSTEM_THREAD_ERROR",
                    format!("failed to start filesystem sync worker: {error}"),
                )
            })?;

        Ok(Self {
            inner: Arc::new(FilesystemSupervisorInner {
                event_tx,
                worker: Mutex::new(Some(worker)),
            }),
            _marker: PhantomData,
        })
    }

    async fn sync_disk_to_lix(&self) -> Result<(), LixError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(FilesystemEvent::SyncDiskToLix { reply_tx })
            .map_err(|error| {
                filesystem_error(format!(
                    "filesystem sync failed: filesystem worker stopped: {error}"
                ))
            })?;
        match reply_rx.await {
            Ok(result) => result,
            Err(error) => Err(filesystem_error(format!(
                "filesystem sync failed: filesystem worker stopped: {error}"
            ))),
        }
    }

    async fn sync_from_lix(&self) -> Result<(), StorageError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(FilesystemEvent::SyncFromLix { reply_tx })
            .map_err(|error| {
                StorageError::Io(format!(
                    "filesystem sync failed: filesystem worker stopped: {error}"
                ))
            })?;
        match reply_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(filesystem_sync_storage_error(error)),
            Err(error) => Err(StorageError::Io(format!(
                "filesystem sync failed: filesystem worker stopped: {error}"
            ))),
        }
    }

    async fn import_paths(&self, paths: Vec<String>) -> Result<(), LixError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(FilesystemEvent::ImportPaths { paths, reply_tx })
            .map_err(|error| {
                filesystem_error(format!(
                    "filesystem import failed: filesystem worker stopped: {error}"
                ))
            })?;
        match reply_rx.await {
            Ok(result) => result,
            Err(error) => Err(filesystem_error(format!(
                "filesystem import failed: filesystem worker stopped: {error}"
            ))),
        }
    }
}

impl Drop for FilesystemSupervisorInner {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl FilesystemSupervisorInner {
    fn shutdown(&self) {
        let _ = self.event_tx.send(FilesystemEvent::Shutdown);
        if let Ok(mut worker) = self.worker.lock() {
            if let Some(worker) = worker.take() {
                let _ = worker.join();
            }
        }
    }
}

impl<StorageImpl> FilesystemState<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    fn path_filter(&self) -> FilesystemPathFilter {
        self.path_filter
            .lock()
            .expect("filesystem path filter lock should not poison")
            .clone()
    }

    fn replace_path_filter(&self, path_filter: FilesystemPathFilter) -> bool {
        let mut current = self
            .path_filter
            .lock()
            .expect("filesystem path filter lock should not poison");
        if *current == path_filter {
            return false;
        }
        *current = path_filter;
        true
    }

    fn path_filters_for_lix_materialization(
        &self,
        current_filter: &FilesystemPathFilter,
        target: &Snapshot,
    ) -> (FilesystemPathFilter, FilesystemPathFilter) {
        if current_filter.is_unfiltered() {
            return (current_filter.clone(), current_filter.clone());
        }

        let Some(previous_file_paths) = self.last_materialized_lix_file_paths() else {
            return (current_filter.clone(), current_filter.clone());
        };
        let current_file_paths = syncable_lix_file_paths(target);
        let mut materialization_filter = current_filter.clone();
        let mut final_filter = current_filter.clone();

        for path in current_file_paths.difference(&previous_file_paths) {
            materialization_filter.add_file(path);
            final_filter.add_file(path);
        }

        for path in previous_file_paths.difference(&current_file_paths) {
            if current_filter.explicitly_includes_file(path) {
                materialization_filter.add_file(path);
                final_filter.remove_file(path);
            }
        }

        (materialization_filter, final_filter)
    }

    async fn sync_from_lix(&self) -> Result<(), LixError> {
        let _guard = self.sync_lock.lock().await;
        let path_filter = self.path_filter();
        let lix_revision = self.collect_lix_revision().await?;
        if self.is_last_materialized_lix_revision(&lix_revision) {
            let local = collect_local_snapshot(&self.layout, &path_filter)?;
            if self.is_last_materialized_disk(&local) {
                return Ok(());
            }
        }
        let lix = self.collect_lix_snapshot_read().await?;
        let (materialization_filter, final_filter) =
            self.path_filters_for_lix_materialization(&path_filter, &lix.snapshot);
        let disk =
            self.materialize_snapshot_with_filter(&lix.snapshot, None, &materialization_filter)?;
        let remembered_disk = if materialization_filter == final_filter {
            disk
        } else {
            self.remembered_snapshot_for_filter(&lix.snapshot, &final_filter)?
        };
        self.replace_path_filter(final_filter);
        self.remember_materialized(remembered_disk, lix.revision, lix_file_paths(&lix.snapshot));
        Ok(())
    }

    async fn sync_disk_to_lix(&self, skip_if_last_materialized: bool) -> Result<(), LixError> {
        let _guard = self.sync_lock.lock().await;
        let path_filter = self.path_filter();
        let local = collect_local_snapshot(&self.layout, &path_filter)?;
        if skip_if_last_materialized && self.is_last_materialized_disk(&local) {
            let lix_revision = self.collect_lix_revision().await?;
            if self.is_last_materialized(&local, &lix_revision) {
                return Ok(());
            }
        }
        let previous = self.last_materialized_disk();
        let lix = self
            .apply_local_snapshot_to_lix_with_filter(&local, previous.as_ref(), &path_filter)
            .await?;
        let materialized =
            self.materialize_snapshot_with_filter(&lix.snapshot, Some(&local), &path_filter)?;
        self.remember_materialized(materialized, lix.revision, lix_file_paths(&lix.snapshot));
        Ok(())
    }

    async fn import_paths(&self, paths: Vec<String>) -> Result<(), LixError> {
        if paths.is_empty() {
            return Ok(());
        }
        let normalized_paths = paths
            .into_iter()
            .map(|path| normalize_filter_file_path(&path))
            .collect::<Result<Vec<_>, LixError>>()?;
        {
            let mut path_filter = self
                .path_filter
                .lock()
                .expect("filesystem path filter lock should not poison");
            for path in normalized_paths {
                path_filter.add_file(&path);
            }
        }
        self.sync_disk_to_lix(true).await
    }

    async fn close(&self) -> Result<(), LixError> {
        self.session.close().await
    }

    async fn collect_lix_snapshot_read(&self) -> Result<LixSnapshotRead, LixError> {
        let mut snapshot = Snapshot::default();
        snapshot.directories.insert("/".to_string());
        let statements: [(&str, &[Value]); 2] = [
            ("SELECT path FROM lix_directory ORDER BY path", &[]),
            ("SELECT path, data FROM lix_file ORDER BY path", &[]),
        ];
        let batch = self
            .session
            .execute_coherent_read_batch(&statements)
            .await?;
        let [directories, files] = batch.results.try_into().map_err(|results: Vec<_>| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "coherent filesystem snapshot read returned {} result sets",
                    results.len()
                ),
            )
        })?;
        for row in directories.rows() {
            snapshot.directories.insert(row.get::<String>("path")?);
        }
        for row in files.rows() {
            let path = row.get::<String>("path")?;
            let data = row.get::<Vec<u8>>("data")?;
            snapshot.files.insert(path, data);
        }

        Ok(LixSnapshotRead {
            snapshot,
            revision: LixRevision {
                active_branch_id: batch.active_branch_id,
                active_branch_commit_id: batch.active_branch_commit_id,
                storage_mutation_revision: batch.storage_mutation_revision,
            },
        })
    }

    async fn collect_lix_revision(&self) -> Result<LixRevision, LixError> {
        let batch = self.session.execute_coherent_read_batch(&[]).await?;
        Ok(LixRevision {
            active_branch_id: batch.active_branch_id,
            active_branch_commit_id: batch.active_branch_commit_id,
            storage_mutation_revision: batch.storage_mutation_revision,
        })
    }

    async fn apply_local_snapshot_to_lix_with_filter(
        &self,
        local: &Snapshot,
        previous: Option<&Snapshot>,
        path_filter: &FilesystemPathFilter,
    ) -> Result<LixSnapshotRead, LixError> {
        let lix = self.collect_lix_snapshot_read().await?;
        let mut needs_fresh_lix_read = false;

        for path in lix.snapshot.files.keys() {
            if !local.files.contains_key(path)
                && path_filter.includes_file(path)
                && !is_plugin_storage_path(path)
                && !is_materialization_ignored_path(path)
            {
                if previous
                    .as_ref()
                    .is_some_and(|snapshot| !snapshot.files.contains_key(path))
                {
                    continue;
                }
                if lix_path_blocked_by_unmanaged(&self.layout, path)?
                    || snapshot_unmanaged_blocks_lix_path(previous, path)
                {
                    continue;
                }
                needs_fresh_lix_read = true;
                self.session
                    .execute(
                        "DELETE FROM lix_file WHERE path = $1",
                        &[Value::Text(path.clone())],
                    )
                    .await?;
            }
        }

        if path_filter.is_unfiltered() {
            let mut directories_to_remove = Vec::new();
            for path in lix.snapshot.directories.difference(&local.directories) {
                if path.as_str() == "/"
                    || is_plugin_storage_path(path)
                    || is_materialization_ignored_path(path)
                {
                    continue;
                }
                if previous
                    .as_ref()
                    .is_some_and(|snapshot| !snapshot.directories.contains(path))
                {
                    continue;
                }
                if lix_path_blocked_by_unmanaged(&self.layout, path)?
                    || snapshot_unmanaged_blocks_lix_path(previous, path)
                {
                    continue;
                }
                directories_to_remove.push(path.clone());
            }
            sort_directories_deepest_first(&mut directories_to_remove);
            for path in directories_to_remove {
                needs_fresh_lix_read = true;
                self.session
                    .execute(
                        "DELETE FROM lix_directory WHERE path = $1",
                        &[Value::Text(path)],
                    )
                    .await?;
            }
        }

        let mut directories_to_create = local
            .directories
            .difference(&lix.snapshot.directories)
            .filter(|path| path.as_str() != "/")
            .filter(|path| {
                previous
                    .as_ref()
                    .is_none_or(|snapshot| !snapshot.directories.contains(*path))
            })
            .cloned()
            .collect::<Vec<_>>();
        sort_directories_shallowest_first(&mut directories_to_create);
        for path in directories_to_create {
            needs_fresh_lix_read = true;
            self.session
                .execute(
                    "INSERT INTO lix_directory (path) VALUES ($1) ON CONFLICT (path) DO NOTHING",
                    &[Value::Text(path)],
                )
                .await?;
        }

        let mut files_to_upsert = Vec::new();
        for (path, data) in local
            .files
            .iter()
            .filter(|(path, _)| !is_materialization_ignored_path(path))
        {
            if previous
                .as_ref()
                .is_some_and(|snapshot| snapshot.files.get(path) == Some(data))
            {
                continue;
            }
            if lix.snapshot.files.get(path) != Some(data) {
                files_to_upsert.push((path.as_str(), data.as_slice()));
            }
        }
        if !files_to_upsert.is_empty() {
            needs_fresh_lix_read = true;
            self.upsert_local_files_to_lix(&files_to_upsert).await?;
        }

        if needs_fresh_lix_read || self.collect_lix_revision().await? != lix.revision {
            return self.collect_lix_snapshot_read().await;
        }
        Ok(lix)
    }

    async fn upsert_local_files_to_lix(&self, files: &[(&str, &[u8])]) -> Result<(), LixError> {
        let mut start = 0;
        while start < files.len() {
            let end = lix_file_upsert_chunk_end(
                files,
                start,
                FILE_UPSERT_BATCH_MAX_ROWS,
                FILE_UPSERT_BATCH_MAX_BYTES,
            );
            let chunk = &files[start..end];
            let sql = lix_file_upsert_sql(chunk.len());
            let mut params = Vec::with_capacity(chunk.len() * 2);
            for (path, data) in chunk {
                params.push(Value::Text((*path).to_string()));
                params.push(Value::Blob((*data).to_vec()));
            }
            self.session.execute(&sql, &params).await?;
            start = end;
        }
        Ok(())
    }

    fn materialize_snapshot_with_filter(
        &self,
        target: &Snapshot,
        base: Option<&Snapshot>,
        path_filter: &FilesystemPathFilter,
    ) -> Result<Snapshot, LixError> {
        ensure_filesystem_root_directory(&self.layout.root)?;
        ensure_filesystem_lix_directory(&self.layout.lix_dir)?;
        let local = collect_local_snapshot(&self.layout, path_filter)?;
        let previous = self.last_materialized_disk();

        for path in local.files.keys().filter(|path| {
            path_filter.includes_file(path)
                && !target.files.contains_key(*path)
                && !is_materialization_ignored_path(path)
                && previous
                    .as_ref()
                    .is_none_or(|snapshot| snapshot.files.contains_key(*path))
        }) {
            if base.is_some_and(|snapshot| {
                !snapshot.files.contains_key(path)
                    || snapshot.files.get(path) != local.files.get(path)
            }) {
                continue;
            }
            remove_materialized_file(&self.layout, path)?;
        }

        if path_filter.is_unfiltered() {
            let mut directories_to_remove = local
                .directories
                .difference(&target.directories)
                .filter(|path| path.as_str() != "/" && !is_materialization_ignored_path(path))
                .filter(|path| {
                    previous
                        .as_ref()
                        .is_none_or(|snapshot| snapshot.directories.contains(*path))
                })
                .filter(|path| {
                    base.is_none_or(|snapshot| {
                        snapshot.directories.contains(*path)
                            && local.directories.contains(*path)
                                == snapshot.directories.contains(*path)
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            sort_directories_deepest_first(&mut directories_to_remove);
            for path in directories_to_remove {
                remove_materialized_directory(&self.layout, &path)?;
            }
        }

        let mut directories_to_create = target
            .directories
            .iter()
            .filter(|path| {
                path.as_str() != "/"
                    && path_filter.includes_directory(path)
                    && !is_materialization_ignored_path(path)
            })
            .filter(|path| base.is_none_or(|snapshot| !snapshot.directories.contains(*path)))
            .filter(|path| {
                base.is_none_or(|snapshot| {
                    local.directories.contains(*path) == snapshot.directories.contains(*path)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        sort_directories_shallowest_first(&mut directories_to_create);
        for path in directories_to_create {
            create_materialized_directory(&self.layout, &path)?;
        }

        for (path, data) in target.files.iter().filter(|(path, _)| {
            path_filter.includes_file(path) && !is_materialization_ignored_path(path)
        }) {
            if base.is_some_and(|snapshot| snapshot.files.get(path) == Some(data)) {
                continue;
            }
            if base.is_some_and(|snapshot| snapshot.files.get(path) != local.files.get(path)) {
                continue;
            }
            if local.files.get(path) != Some(data) {
                write_materialized_file(&self.layout, path, data)?;
            }
        }

        self.remembered_snapshot_for_filter(target, path_filter)
    }

    fn remembered_snapshot_for_filter(
        &self,
        target: &Snapshot,
        path_filter: &FilesystemPathFilter,
    ) -> Result<Snapshot, LixError> {
        let materialized = collect_local_snapshot(&self.layout, path_filter)?;
        let mut remembered = target.filtered(path_filter);
        remembered.unmanaged_paths = materialized.unmanaged_paths;
        Ok(remembered)
    }

    fn remember_materialized(
        &self,
        disk: Snapshot,
        lix_revision: LixRevision,
        lix_file_paths: BTreeSet<String>,
    ) {
        *self
            .last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison") =
            Some(MaterializedSnapshot {
                disk,
                lix_revision,
                lix_file_paths,
            });
    }

    fn last_materialized_disk(&self) -> Option<Snapshot> {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .as_ref()
            .map(|snapshot| snapshot.disk.clone())
    }

    fn last_materialized_lix_file_paths(&self) -> Option<BTreeSet<String>> {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .as_ref()
            .map(|snapshot| snapshot.lix_file_paths.clone())
    }

    fn is_last_materialized_disk(&self, snapshot: &Snapshot) -> bool {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .as_ref()
            .is_some_and(|materialized| &materialized.disk == snapshot)
    }

    fn is_last_materialized_lix_revision(&self, lix_revision: &LixRevision) -> bool {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .as_ref()
            .is_some_and(|materialized| &materialized.lix_revision == lix_revision)
    }

    fn is_last_materialized(&self, disk: &Snapshot, lix_revision: &LixRevision) -> bool {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .as_ref()
            .is_some_and(|materialized| {
                &materialized.disk == disk && &materialized.lix_revision == lix_revision
            })
    }
}

fn filesystem_worker<StorageImpl>(
    state: Arc<FilesystemState<StorageImpl>>,
    event_rx: mpsc::Receiver<FilesystemEvent>,
    mut poll_filesystem: bool,
    mut debouncer: Option<FilesystemWatcher>,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread().build() else {
        return;
    };
    loop {
        let e = if poll_filesystem {
            event_rx.recv_timeout(FILESYSTEM_POLL_INTERVAL)
        } else {
            event_rx
                .recv()
                .map_err(|mpsc::RecvError| mpsc::RecvTimeoutError::Disconnected)
        };
        match e {
            Ok(FilesystemEvent::DiskChanged) | Err(mpsc::RecvTimeoutError::Timeout) => {
                if drain_filesystem_events(
                    &runtime,
                    &state,
                    &event_rx,
                    true,
                    &mut debouncer,
                    &mut poll_filesystem,
                ) {
                    return;
                }
            }
            Ok(FilesystemEvent::SyncDiskToLix { reply_tx }) => {
                let _ = sync_disk_to_lix_for_replies(
                    &runtime,
                    &state,
                    vec![reply_tx],
                    &mut debouncer,
                    &mut poll_filesystem,
                );
                if drain_filesystem_events(
                    &runtime,
                    &state,
                    &event_rx,
                    false,
                    &mut debouncer,
                    &mut poll_filesystem,
                ) {
                    return;
                }
            }
            Ok(FilesystemEvent::SyncFromLix { reply_tx }) => {
                let _ = sync_from_lix_for_replies(
                    &runtime,
                    &state,
                    vec![reply_tx],
                    &mut debouncer,
                    &mut poll_filesystem,
                );
                if drain_filesystem_events(
                    &runtime,
                    &state,
                    &event_rx,
                    false,
                    &mut debouncer,
                    &mut poll_filesystem,
                ) {
                    return;
                }
            }
            Ok(FilesystemEvent::ImportPaths { paths, reply_tx }) => {
                let _ = import_paths_for_replies(
                    &runtime,
                    &state,
                    vec![(paths, reply_tx)],
                    &mut debouncer,
                    &mut poll_filesystem,
                );
                if drain_filesystem_events(
                    &runtime,
                    &state,
                    &event_rx,
                    false,
                    &mut debouncer,
                    &mut poll_filesystem,
                ) {
                    return;
                }
            }
            Ok(FilesystemEvent::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = runtime.block_on(state.close());
                return;
            }
        }
    }
}

fn drain_filesystem_events<StorageImpl>(
    runtime: &tokio::runtime::Runtime,
    state: &Arc<FilesystemState<StorageImpl>>,
    event_rx: &mpsc::Receiver<FilesystemEvent>,
    mut sync_disk: bool,
    debouncer: &mut Option<FilesystemWatcher>,
    poll_filesystem: &mut bool,
) -> bool
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut sync_disk_replies = Vec::new();
    let mut sync_replies = Vec::new();
    let mut import_replies = Vec::new();
    loop {
        match event_rx.try_recv() {
            Ok(FilesystemEvent::DiskChanged) => sync_disk = true,
            Ok(FilesystemEvent::SyncDiskToLix { reply_tx }) => sync_disk_replies.push(reply_tx),
            Ok(FilesystemEvent::SyncFromLix { reply_tx }) => sync_replies.push(reply_tx),
            Ok(FilesystemEvent::ImportPaths { paths, reply_tx }) => {
                import_replies.push((paths, reply_tx));
            }
            Ok(FilesystemEvent::Shutdown) | Err(mpsc::TryRecvError::Disconnected) => {
                let _ = runtime.block_on(state.close());
                return true;
            }
            Err(mpsc::TryRecvError::Empty) => break,
        }
    }
    if sync_disk || !sync_disk_replies.is_empty() {
        let _ = sync_disk_to_lix_for_replies(
            runtime,
            state,
            sync_disk_replies,
            debouncer,
            poll_filesystem,
        );
    }
    if !sync_replies.is_empty() {
        let _ = sync_from_lix_for_replies(runtime, state, sync_replies, debouncer, poll_filesystem);
    }
    if !import_replies.is_empty() {
        let _ =
            import_paths_for_replies(runtime, state, import_replies, debouncer, poll_filesystem);
    }
    false
}

fn sync_disk_to_lix_for_replies<StorageImpl>(
    runtime: &tokio::runtime::Runtime,
    state: &Arc<FilesystemState<StorageImpl>>,
    replies: Vec<oneshot::Sender<Result<(), LixError>>>,
    debouncer: &mut Option<FilesystemWatcher>,
    poll_filesystem: &mut bool,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let result = runtime.block_on(state.sync_disk_to_lix(true));
    if result.is_ok() {
        refresh_filesystem_watcher(state, debouncer, poll_filesystem);
    }
    for reply in replies {
        let _ = reply.send(result.clone());
    }
    result
}

fn import_paths_for_replies<StorageImpl>(
    runtime: &tokio::runtime::Runtime,
    state: &Arc<FilesystemState<StorageImpl>>,
    replies: Vec<(Vec<String>, oneshot::Sender<Result<(), LixError>>)>,
    debouncer: &mut Option<FilesystemWatcher>,
    poll_filesystem: &mut bool,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut first_error = None;
    for (paths, reply) in replies {
        let result = runtime.block_on(state.import_paths(paths));
        if result.is_ok() {
            refresh_filesystem_watcher(state, debouncer, poll_filesystem);
        } else if first_error.is_none() {
            first_error = result.clone().err();
        }
        let _ = reply.send(result);
    }
    first_error.map_or(Ok(()), Err)
}

fn sync_from_lix_for_replies<StorageImpl>(
    runtime: &tokio::runtime::Runtime,
    state: &Arc<FilesystemState<StorageImpl>>,
    replies: Vec<oneshot::Sender<Result<(), LixError>>>,
    debouncer: &mut Option<FilesystemWatcher>,
    poll_filesystem: &mut bool,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let result = runtime.block_on(state.sync_from_lix());
    if result.is_ok() {
        refresh_filesystem_watcher(state, debouncer, poll_filesystem);
    }
    for reply in replies {
        let _ = reply.send(result.clone());
    }
    result
}

fn refresh_filesystem_watcher<StorageImpl>(
    state: &Arc<FilesystemState<StorageImpl>>,
    debouncer: &mut Option<FilesystemWatcher>,
    poll_filesystem: &mut bool,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let Some(watcher) = debouncer.as_mut() else {
        *poll_filesystem = true;
        return;
    };
    let path_filter = state.path_filter();
    if watcher.refresh(&state.layout, &path_filter).is_err() {
        if let Some(watcher) = debouncer.take() {
            watcher.stop();
        }
        *poll_filesystem = true;
    }
}

fn collect_local_snapshot(
    layout: &FilesystemLayout,
    path_filter: &FilesystemPathFilter,
) -> Result<Snapshot, LixError> {
    validate_filesystem_root_directory(&layout.root)?;
    validate_filesystem_lix_directory(&layout.lix_dir)?;

    let mut snapshot = Snapshot::default();
    snapshot.directories.insert("/".to_string());
    if path_filter.is_unfiltered() {
        let child_dirs = collect_local_directory_shallow(layout, &layout.root, &mut snapshot)?;
        let child_snapshot = collect_local_child_directories(layout, child_dirs)?;
        merge_snapshot(&mut snapshot, child_snapshot);
    } else {
        collect_filtered_local_snapshot(layout, path_filter, &mut snapshot)?;
    }
    collect_lix_directory_snapshot(layout, &mut snapshot)?;
    Ok(snapshot)
}

fn collect_local_child_directories(
    layout: &FilesystemLayout,
    child_dirs: Vec<PathBuf>,
) -> Result<Snapshot, LixError> {
    if child_dirs.is_empty() {
        return Ok(Snapshot::default());
    }

    let worker_count = thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(FILESYSTEM_PARALLEL_SNAPSHOT_MAX_WORKERS)
        .min(child_dirs.len());
    if worker_count <= 1 || child_dirs.len() < FILESYSTEM_PARALLEL_SNAPSHOT_MIN_DIRS {
        let mut snapshot = Snapshot::default();
        for child_dir in child_dirs {
            collect_local_directory(layout, &child_dir, &mut snapshot)?;
        }
        return Ok(snapshot);
    }

    let chunk_size = child_dirs.len().div_ceil(worker_count);
    let mut handles = Vec::with_capacity(worker_count);
    let mut first_error = None;
    for (worker_index, chunk) in child_dirs.chunks(chunk_size).enumerate() {
        let layout = layout.clone();
        let child_dirs = chunk.to_vec();
        let worker = thread::Builder::new()
            .name(format!("lix-sdk-filesystem-snapshot-{worker_index}"))
            .spawn(move || {
                let mut snapshot = Snapshot::default();
                for child_dir in child_dirs {
                    collect_local_directory(&layout, &child_dir, &mut snapshot)?;
                }
                Ok::<_, LixError>(snapshot)
            });
        match worker {
            Ok(handle) => handles.push(handle),
            Err(error) => {
                first_error = Some(LixError::new(
                    "LIX_FILESYSTEM_THREAD_ERROR",
                    format!("failed to start filesystem snapshot worker: {error}"),
                ));
                break;
            }
        }
    }

    let mut snapshot = Snapshot::default();
    for handle in handles {
        match handle.join() {
            Ok(Ok(child_snapshot)) => {
                if first_error.is_none() {
                    merge_snapshot(&mut snapshot, child_snapshot);
                }
            }
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            Err(_) => {
                if first_error.is_none() {
                    first_error = Some(LixError::unknown("filesystem snapshot worker panicked"));
                }
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(snapshot)
}

fn collect_local_directory(
    layout: &FilesystemLayout,
    directory: &Path,
    snapshot: &mut Snapshot,
) -> Result<(), LixError> {
    let child_dirs = collect_local_directory_shallow(layout, directory, snapshot)?;
    for child_dir in child_dirs {
        collect_local_directory(layout, &child_dir, snapshot)?;
    }
    Ok(())
}

fn collect_local_directory_shallow(
    layout: &FilesystemLayout,
    directory: &Path,
    snapshot: &mut Snapshot,
) -> Result<Vec<PathBuf>, LixError> {
    let mut child_dirs = Vec::new();
    let entries = std::fs::read_dir(directory)
        .map_err(|error| io_error("read filesystem directory", directory, error))?;
    for entry in entries {
        let entry =
            entry.map_err(|error| io_error("read filesystem directory entry", directory, error))?;
        let path = entry.path();
        if is_filesystem_sync_ignored_local_path(layout, &path) {
            continue;
        }
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io_error("read filesystem entry type", &path, error)),
        };
        if is_unmanaged_file_type(&file_type) {
            remember_unmanaged_local_path(layout, directory, &path, snapshot);
            continue;
        }
        if file_type.is_dir() {
            let Ok(lix_path) = layout.local_path_to_lix_path(&path, true) else {
                remember_unmanaged_local_path(layout, directory, &path, snapshot);
                continue;
            };
            snapshot.directories.insert(lix_path);
            child_dirs.push(path);
        } else if file_type.is_file() {
            let Ok(lix_path) = layout.local_path_to_lix_path(&path, false) else {
                remember_unmanaged_local_path(layout, directory, &path, snapshot);
                continue;
            };
            let data = std::fs::read(&path)
                .map_err(|error| io_error("read filesystem file", &path, error))?;
            snapshot.files.insert(lix_path, data);
        }
    }
    Ok(child_dirs)
}

fn merge_snapshot(target: &mut Snapshot, source: Snapshot) {
    target.directories.extend(source.directories);
    target.files.extend(source.files);
    target.unmanaged_paths.extend(source.unmanaged_paths);
}

fn lix_file_paths(snapshot: &Snapshot) -> BTreeSet<String> {
    snapshot.files.keys().cloned().collect()
}

fn syncable_lix_file_paths(snapshot: &Snapshot) -> BTreeSet<String> {
    snapshot
        .files
        .keys()
        .filter(|path| is_dynamic_filter_file_path(path))
        .cloned()
        .collect()
}

fn is_dynamic_filter_file_path(path: &str) -> bool {
    !is_lix_storage_path(path) && !is_filesystem_sync_ignored_lix_path(path)
}

fn collect_filtered_local_snapshot(
    layout: &FilesystemLayout,
    path_filter: &FilesystemPathFilter,
    snapshot: &mut Snapshot,
) -> Result<(), LixError> {
    let Some(include_files) = path_filter.include_files.as_ref() else {
        return Ok(());
    };
    for lix_path in include_files {
        if is_lix_storage_path(lix_path) || is_filesystem_sync_ignored_lix_path(lix_path) {
            continue;
        }
        let local_path = layout.lix_path_to_local_path(lix_path)?;
        if path_contains_unmanaged_entry(layout, &local_path)? {
            snapshot.unmanaged_paths.insert(lix_path.clone());
            continue;
        }
        let metadata = match std::fs::symlink_metadata(&local_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(io_error(
                    "read filesystem file metadata",
                    &local_path,
                    error,
                ));
            }
        };
        if is_unmanaged_file_type(&metadata.file_type()) {
            snapshot.unmanaged_paths.insert(lix_path.clone());
            continue;
        }
        if !metadata.is_file() {
            continue;
        }
        insert_parent_lix_directories(lix_path, snapshot);
        let data = std::fs::read(&local_path)
            .map_err(|error| io_error("read filesystem file", &local_path, error))?;
        snapshot.files.insert(lix_path.clone(), data);
    }
    Ok(())
}

fn collect_lix_directory_snapshot(
    layout: &FilesystemLayout,
    snapshot: &mut Snapshot,
) -> Result<(), LixError> {
    snapshot.directories.insert("/.lix/".to_string());
    let child_dirs = collect_local_directory_shallow(layout, &layout.lix_dir, snapshot)?;
    let child_snapshot = collect_local_child_directories(layout, child_dirs)?;
    merge_snapshot(snapshot, child_snapshot);
    Ok(())
}

fn remember_unmanaged_local_path(
    layout: &FilesystemLayout,
    directory: &Path,
    path: &Path,
    snapshot: &mut Snapshot,
) {
    if let Ok(lix_path) = layout.local_path_to_lix_path(path, false) {
        snapshot.unmanaged_paths.insert(lix_path);
    } else if layout.local_base_for_path(directory) != Some(directory) {
        if let Ok(parent_path) = layout.local_path_to_lix_path(directory, true) {
            snapshot.unmanaged_paths.insert(parent_path);
        }
    }
}

impl FilesystemWatcher {
    fn refresh(
        &mut self,
        layout: &FilesystemLayout,
        path_filter: &FilesystemPathFilter,
    ) -> Result<(), notify_debouncer_full::notify::Error> {
        let next_paths = filesystem_watch_paths(layout, path_filter)?;
        if self.watched_paths == next_paths {
            return Ok(());
        }
        for watched_path in self.watched_paths.iter().rev() {
            let _ = self.debouncer.unwatch(&watched_path.path);
        }
        for watched_path in &next_paths {
            self.debouncer
                .watch(&watched_path.path, watched_path.recursive_mode())?;
        }
        self.watched_paths = next_paths;
        Ok(())
    }

    fn stop(self) {
        self.debouncer.stop();
    }
}

impl FilesystemWatchPath {
    fn recursive_mode(&self) -> RecursiveMode {
        if self.recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        }
    }
}

fn filesystem_watch_paths(
    layout: &FilesystemLayout,
    path_filter: &FilesystemPathFilter,
) -> Result<Vec<FilesystemWatchPath>, notify_debouncer_full::notify::Error> {
    let mut paths = BTreeMap::<PathBuf, bool>::new();
    if path_filter.is_unfiltered() {
        paths.insert(layout.root.clone(), true);
        if !layout.lix_dir_is_inside_root() {
            paths.insert(layout.lix_dir.clone(), true);
        }
        return Ok(paths
            .into_iter()
            .map(|(path, recursive)| FilesystemWatchPath { path, recursive })
            .collect());
    }
    for path in path_filter
        .local_watch_paths(layout)
        .map_err(|error| notify_debouncer_full::notify::Error::generic(&error.format()))?
    {
        paths.entry(path).or_insert(false);
    }
    paths.insert(layout.lix_dir.clone(), true);
    Ok(paths
        .into_iter()
        .map(|(path, recursive)| FilesystemWatchPath { path, recursive })
        .collect())
}

fn ensure_filesystem_root_directory(root: &Path) -> Result<(), LixError> {
    std::fs::create_dir_all(root)
        .map_err(|error| io_error("create filesystem root", root, error))?;
    validate_filesystem_root_directory(root)
}

fn validate_filesystem_root_directory(root: &Path) -> Result<(), LixError> {
    let metadata = std::fs::symlink_metadata(root)
        .map_err(|error| io_error("read filesystem root metadata", root, error))?;
    if metadata.file_type().is_symlink() {
        let root = root.display();
        return Err(filesystem_error(format!(
            "filesystem root {root} must not be a symlink"
        )));
    }
    if !metadata.is_dir() {
        let root = root.display();
        return Err(filesystem_error(format!(
            "filesystem root {root} must be a directory"
        )));
    }
    Ok(())
}

fn ensure_filesystem_lix_directory(lix_dir: &Path) -> Result<(), LixError> {
    validate_lix_directory_name(lix_dir)?;
    match std::fs::create_dir(lix_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(io_error("create filesystem .lix directory", lix_dir, error));
        }
    }

    validate_filesystem_lix_directory(lix_dir)?;
    ensure_gitignore(lix_dir, LIX_DIRECTORY_GITIGNORE)?;
    Ok(())
}

fn validate_filesystem_lix_directory(lix_dir: &Path) -> Result<(), LixError> {
    validate_lix_directory_name(lix_dir)?;
    let metadata = std::fs::symlink_metadata(lix_dir)
        .map_err(|error| io_error("read filesystem .lix directory", lix_dir, error))?;
    if metadata.file_type().is_symlink() {
        let path = lix_dir.display();
        return Err(filesystem_error(format!(
            "filesystem .lix path {path} must not be a symlink"
        )));
    }
    if !metadata.is_dir() {
        let path = lix_dir.display();
        return Err(filesystem_error(format!(
            "filesystem .lix path {path} must be a directory"
        )));
    }
    Ok(())
}

fn validate_lix_directory_name(lix_dir: &Path) -> Result<(), LixError> {
    if lix_dir.file_name().and_then(|name| name.to_str()) != Some(".lix") {
        let path = lix_dir.display();
        return Err(filesystem_error(format!(
            "filesystem .lix path {path} must be named .lix"
        )));
    }
    Ok(())
}

fn remove_materialized_file(layout: &FilesystemLayout, path: &str) -> Result<(), LixError> {
    if is_filesystem_sync_ignored_lix_path(path) {
        return Ok(());
    }
    let Some(local_path) = materialization_local_path(layout, path) else {
        return Ok(());
    };
    if path_contains_unmanaged_entry(layout, &local_path)? {
        return Ok(());
    }
    let metadata = match std::fs::symlink_metadata(&local_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(io_error(
                "read filesystem file metadata",
                &local_path,
                error,
            ));
        }
    };
    if !metadata.is_file() {
        return Ok(());
    }
    std::fs::remove_file(&local_path)
        .map_err(|error| io_error("remove filesystem file", &local_path, error))
}

fn remove_materialized_directory(layout: &FilesystemLayout, path: &str) -> Result<(), LixError> {
    if is_filesystem_sync_ignored_lix_path(path) {
        return Ok(());
    }
    let Some(local_path) = materialization_local_path(layout, path) else {
        return Ok(());
    };
    if path_contains_unmanaged_entry(layout, &local_path)? {
        return Ok(());
    }
    let metadata = match std::fs::symlink_metadata(&local_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(io_error(
                "read filesystem directory metadata",
                &local_path,
                error,
            ));
        }
    };
    if !metadata.is_dir() {
        return Ok(());
    }
    match std::fs::remove_dir(&local_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => Ok(()),
        Err(error) => Err(io_error("remove filesystem directory", &local_path, error)),
    }
}

fn create_materialized_directory(layout: &FilesystemLayout, path: &str) -> Result<(), LixError> {
    if is_filesystem_sync_ignored_lix_path(path) {
        return Ok(());
    }
    let Some(local_path) = materialization_local_path(layout, path) else {
        return Ok(());
    };
    if path_contains_unmanaged_entry(layout, &local_path)? {
        return Ok(());
    }
    std::fs::create_dir_all(&local_path)
        .map_err(|error| io_error("create filesystem directory", &local_path, error))
}

fn write_materialized_file(
    layout: &FilesystemLayout,
    path: &str,
    data: &[u8],
) -> Result<(), LixError> {
    if is_filesystem_sync_ignored_lix_path(path) {
        return Ok(());
    }
    let Some(local_path) = materialization_local_path(layout, path) else {
        return Ok(());
    };
    if path_contains_unmanaged_entry(layout, &local_path)? {
        return Ok(());
    }
    if let Some(parent) = local_path.parent() {
        if path_contains_unmanaged_entry(layout, parent)? {
            return Ok(());
        }
        std::fs::create_dir_all(parent)
            .map_err(|error| io_error("create filesystem file parent", parent, error))?;
        if path_contains_unmanaged_entry(layout, parent)? {
            return Ok(());
        }
    }
    if path_contains_unmanaged_entry(layout, &local_path)? {
        return Ok(());
    }
    std::fs::write(&local_path, data)
        .map_err(|error| io_error("write filesystem file", &local_path, error))
}

fn lix_file_upsert_sql(row_count: usize) -> String {
    debug_assert!(row_count > 0);
    let mut sql = String::from("INSERT INTO lix_file (path, data) VALUES ");
    for row in 0..row_count {
        if row > 0 {
            sql.push_str(", ");
        }
        let _ = write!(sql, "(${}, ${})", row * 2 + 1, row * 2 + 2);
    }
    sql.push_str(" ON CONFLICT (path) DO UPDATE SET data = excluded.data");
    sql
}

fn lix_file_upsert_chunk_end(
    files: &[(&str, &[u8])],
    start: usize,
    max_rows: usize,
    max_bytes: usize,
) -> usize {
    debug_assert!(start < files.len());
    let max_rows = max_rows.max(1);
    let mut end = start;
    let mut bytes = 0usize;
    while end < files.len() && end - start < max_rows {
        let (path, data) = files[end];
        let file_bytes = path.len().saturating_add(data.len());
        if end > start && bytes.saturating_add(file_bytes) > max_bytes {
            break;
        }
        bytes = bytes.saturating_add(file_bytes);
        end += 1;
    }
    end
}

fn lix_path_blocked_by_unmanaged(layout: &FilesystemLayout, path: &str) -> Result<bool, LixError> {
    let Some(local_path) = materialization_local_path(layout, path) else {
        return Ok(true);
    };
    path_contains_unmanaged_entry(layout, &local_path)
}

fn snapshot_unmanaged_blocks_lix_path(snapshot: Option<&Snapshot>, path: &str) -> bool {
    snapshot.is_some_and(|snapshot| {
        snapshot
            .unmanaged_paths
            .iter()
            .any(|unmanaged_path| unmanaged_path_blocks_lix_path(unmanaged_path, path))
    })
}

fn unmanaged_path_blocks_lix_path(unmanaged_path: &str, path: &str) -> bool {
    let unmanaged_path = unmanaged_path.strip_suffix('/').unwrap_or(unmanaged_path);
    let path = path.strip_suffix('/').unwrap_or(path);
    path == unmanaged_path
        || path
            .strip_prefix(unmanaged_path)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn materialization_local_path(layout: &FilesystemLayout, path: &str) -> Option<PathBuf> {
    layout.lix_path_to_local_path(path).ok()
}

fn path_contains_unmanaged_entry(
    layout: &FilesystemLayout,
    local_path: &Path,
) -> Result<bool, LixError> {
    let Some(base) = layout.local_base_for_path(local_path) else {
        return Ok(true);
    };
    let Ok(relative) = local_path.strip_prefix(base) else {
        return Ok(true);
    };
    let mut current = base.to_path_buf();
    for component in relative.components() {
        let Component::Normal(segment) = component else {
            return Ok(true);
        };
        current.push(segment);
        let metadata = match std::fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => {
                return Err(io_error("read filesystem path metadata", &current, error));
            }
        };
        if is_unmanaged_file_type(&metadata.file_type()) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn is_unmanaged_file_type(file_type: &std::fs::FileType) -> bool {
    file_type.is_symlink() || (!file_type.is_file() && !file_type.is_dir())
}

fn local_path_to_lix_path(
    root: &Path,
    path: &Path,
    is_directory: bool,
) -> Result<String, LixError> {
    let relative = path.strip_prefix(root).map_err(|error| {
        let path = path.display();
        let root = root.display();
        filesystem_error(format!(
            "filesystem path {path} is not inside root {root}: {error}"
        ))
    })?;
    let mut segments = Vec::new();
    for component in relative.components() {
        let Component::Normal(segment) = component else {
            let path = path.display();
            return Err(filesystem_error(format!(
                "filesystem path {path} contains an unsupported path component"
            )));
        };
        let segment = segment.to_str().ok_or_else(|| {
            let path = path.display();
            filesystem_error(format!("filesystem path {path} is not valid UTF-8"))
        })?;
        segments.push(segment.to_string());
    }
    if segments.is_empty() {
        return Ok("/".to_string());
    }
    let mut lix_path = format!("/{}", segments.join("/"));
    if is_directory {
        lix_path.push('/');
    }
    Ok(lix_path)
}

fn normalize_filter_file_path(path: &str) -> Result<String, LixError> {
    if path.is_empty() {
        return Err(filesystem_error("filesystem filter path must not be empty"));
    }
    let normalized = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    if normalized.ends_with('/') {
        return Err(filesystem_error(format!(
            "filesystem filter path {path:?} must refer to a file"
        )));
    }
    validate_lix_path(&normalized)?;
    Ok(normalized)
}

fn validate_lix_path(path: &str) -> Result<(), LixError> {
    let _ = lix_path_to_local_path(Path::new("/"), path)?;
    Ok(())
}

fn parent_lix_directory_path(path: &str) -> String {
    let Some(index) = path.rfind('/') else {
        return "/".to_string();
    };
    if index == 0 {
        "/".to_string()
    } else {
        format!("{}/", &path[..index])
    }
}

fn lix_directory_contains_directory(parent: &str, child: &str) -> bool {
    let parent = parent.trim_end_matches('/');
    let child = child.trim_end_matches('/');
    if parent.is_empty() {
        return true;
    }
    child == parent
        || child
            .strip_prefix(parent)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn insert_parent_lix_directories(path: &str, snapshot: &mut Snapshot) {
    let mut directory = parent_lix_directory_path(path);
    let mut directories = Vec::new();
    while directory != "/" {
        directories.push(directory.clone());
        let parent = parent_lix_directory_path(directory.trim_end_matches('/'));
        if parent == directory {
            break;
        }
        directory = parent;
    }
    directories.reverse();
    snapshot.directories.extend(directories);
}

fn lix_path_to_local_path(root: &Path, path: &str) -> Result<PathBuf, LixError> {
    if path == "/" {
        return Ok(root.to_path_buf());
    }
    let body = path
        .strip_prefix('/')
        .ok_or_else(|| filesystem_error(format!("Lix path {path:?} is not absolute")))?;
    let body = body.strip_suffix('/').unwrap_or(body);
    if body.is_empty() {
        return Ok(root.to_path_buf());
    }
    let mut local = root.to_path_buf();
    for segment in body.split('/') {
        push_lix_path_segment(&mut local, segment, path)?;
    }
    Ok(local)
}

fn push_lix_path_segment(local: &mut PathBuf, segment: &str, path: &str) -> Result<(), LixError> {
    if segment.is_empty() || segment == "." || segment == ".." {
        return Err(filesystem_error(format!(
            "Lix path {path:?} contains unsupported segment {segment:?}"
        )));
    }

    let mut components = Path::new(segment).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(component)), None) => {
            local.push(component);
        }
        _ => {
            return Err(filesystem_error(format!(
                "Lix path {path:?} contains segment {segment:?} that cannot be mapped to a single host path component"
            )));
        }
    }

    Ok(())
}

fn is_plugin_storage_path(path: &str) -> bool {
    path == "/.lix/plugins" || path.starts_with("/.lix/plugins/")
}

fn is_filesystem_metadata_path(path: &str) -> bool {
    path == "/.lix/.gitignore"
        || path == "/.lix/.gitignore/"
        || is_filesystem_internal_path(path)
        || is_legacy_filesystem_metadata_path(path)
}

fn is_filesystem_internal_path(path: &str) -> bool {
    path == "/.lix/.internal" || path.starts_with("/.lix/.internal/")
}

fn is_legacy_filesystem_metadata_path(path: &str) -> bool {
    let path = path.strip_suffix('/').unwrap_or(path);
    path == "/.lix_system"
        || path.starts_with("/.lix_system/")
        || path
            .strip_prefix("/.lix/")
            .is_some_and(is_legacy_filesystem_sqlite_metadata_name)
}

fn is_legacy_filesystem_sqlite_metadata_name(name: &str) -> bool {
    LEGACY_FILESYSTEM_SQLITE_METADATA_NAMES.contains(&name)
}

fn is_filesystem_sync_ignored_local_path(layout: &FilesystemLayout, path: &Path) -> bool {
    if path.starts_with(&layout.lix_dir) {
        return layout
            .local_path_to_lix_path(path, path.is_dir())
            .is_ok_and(|path| is_filesystem_sync_ignored_lix_path(&path));
    }

    let Ok(relative) = path.strip_prefix(&layout.root) else {
        return true;
    };
    let mut depth = 0usize;
    for component in relative.components() {
        let Component::Normal(segment) = component else {
            return true;
        };
        depth += 1;
        let segment = segment.to_str();
        if segment == Some(".git") {
            return true;
        }
        if depth == 1 && segment == Some(".lix") {
            return true;
        }
        if depth == 1 && segment == Some(".lix_system") {
            return true;
        }
    }
    false
}

fn is_materialization_ignored_path(path: &str) -> bool {
    is_filesystem_metadata_path(path)
}

fn is_filesystem_sync_ignored_lix_path(path: &str) -> bool {
    lix_path_contains_segment(path, ".git") || is_materialization_ignored_path(path)
}

fn is_lix_storage_path(path: &str) -> bool {
    let path = path.strip_suffix('/').unwrap_or(path);
    path == "/.lix" || path.starts_with("/.lix/")
}

fn lix_path_contains_segment(path: &str, segment: &str) -> bool {
    path.trim_matches('/')
        .split('/')
        .any(|candidate| candidate == segment)
}

fn sort_directories_deepest_first(paths: &mut [String]) {
    paths.sort_by(|left, right| {
        path_depth(right)
            .cmp(&path_depth(left))
            .then_with(|| right.len().cmp(&left.len()))
            .then_with(|| right.cmp(left))
    });
}

fn sort_directories_shallowest_first(paths: &mut [String]) {
    paths.sort_by(|left, right| {
        path_depth(left)
            .cmp(&path_depth(right))
            .then_with(|| left.len().cmp(&right.len()))
            .then_with(|| left.cmp(right))
    });
}

fn path_depth(path: &str) -> usize {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .count()
}

fn io_error(operation: &str, path: &Path, error: std::io::Error) -> LixError {
    let path = path.display();
    LixError::new(
        "LIX_FILESYSTEM_IO_ERROR",
        format!("{operation} {path}: {error}"),
    )
}

fn filesystem_sync_storage_error(error: LixError) -> StorageError {
    StorageError::Io(format!("filesystem sync failed: {}", error.format()))
}

fn filesystem_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_FILESYSTEM_ERROR", message)
}

#[cfg(feature = "local_filesystem")]
fn prepare_filesystem_layout(
    root: &Path,
    lix_dir: Option<&Path>,
) -> Result<FilesystemLayout, LixError> {
    ensure_filesystem_root_directory(root)?;
    let root = std::fs::canonicalize(root)
        .map_err(|error| io_error("canonicalize filesystem root", root, error))?;
    let default_lix_dir = root.join(".lix");
    let requested_lix_dir = lix_dir.map_or_else(|| default_lix_dir.clone(), absolute_path);
    validate_lix_directory_name(&requested_lix_dir)?;
    let requested_lix_dir = normalize_path_lexically(&requested_lix_dir);
    let default_lix_dir = normalize_path_lexically(&default_lix_dir);

    if requested_lix_dir != default_lix_dir && requested_lix_dir.starts_with(&root) {
        return Err(filesystem_error(format!(
            "external filesystem .lix path {} must be outside root {}",
            requested_lix_dir.display(),
            root.display()
        )));
    }

    ensure_filesystem_lix_directory(&requested_lix_dir)?;
    let canonical_lix_dir = std::fs::canonicalize(&requested_lix_dir).map_err(|error| {
        io_error(
            "canonicalize filesystem .lix directory",
            &requested_lix_dir,
            error,
        )
    })?;
    if canonical_lix_dir != default_lix_dir && canonical_lix_dir.starts_with(&root) {
        return Err(filesystem_error(format!(
            "external filesystem .lix path {} must be outside root {}",
            canonical_lix_dir.display(),
            root.display()
        )));
    }

    Ok(FilesystemLayout {
        root,
        lix_dir_is_default: canonical_lix_dir == default_lix_dir,
        lix_dir: canonical_lix_dir,
    })
}

#[cfg(feature = "local_filesystem")]
fn absolute_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

#[cfg(feature = "local_filesystem")]
fn normalize_path_lexically(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::Normal(segment) => normalized.push(segment),
        }
    }
    normalized
}

#[cfg(feature = "local_filesystem")]
fn open_filesystem_rocksdb(layout: &FilesystemLayout) -> Result<RocksDBFilesystem, LixError> {
    let metadata_dir = ensure_filesystem_rocksdb_metadata_directory(layout)?;
    RocksDBFilesystem::open(metadata_dir).map_err(rocksdb_error)
}

#[cfg(feature = "local_filesystem")]
fn ensure_filesystem_rocksdb_metadata_directory(
    layout: &FilesystemLayout,
) -> Result<PathBuf, LixError> {
    ensure_filesystem_lix_directory(&layout.lix_dir)?;
    if layout.lix_dir_is_default {
        remove_legacy_filesystem_root_metadata(&layout.root, &layout.lix_dir)?;
    } else {
        remove_legacy_filesystem_lix_metadata(&layout.lix_dir)?;
    }
    let internal_dir = layout.lix_dir.join(".internal");
    reset_legacy_filesystem_internal_directory(&internal_dir)?;
    ensure_metadata_directory(&internal_dir, "filesystem metadata directory")?;
    let metadata_dir = internal_dir.join("rocksdb");
    ensure_metadata_directory(&metadata_dir, "filesystem RocksDB metadata directory")?;
    Ok(metadata_dir)
}

#[cfg(feature = "local_filesystem")]
fn remove_legacy_filesystem_root_metadata(root: &Path, lix_dir: &Path) -> Result<(), LixError> {
    remove_legacy_filesystem_lix_metadata(lix_dir)?;
    remove_legacy_metadata_path(&root.join(".lix_system"))
}

#[cfg(feature = "local_filesystem")]
fn remove_legacy_filesystem_lix_metadata(lix_dir: &Path) -> Result<(), LixError> {
    for name in LEGACY_FILESYSTEM_SQLITE_METADATA_NAMES {
        remove_legacy_metadata_file(&lix_dir.join(name))?;
    }
    Ok(())
}

#[cfg(feature = "local_filesystem")]
fn reset_legacy_filesystem_internal_directory(internal_dir: &Path) -> Result<(), LixError> {
    if internal_dir.join("rocksdb").exists() {
        return Ok(());
    }
    if !legacy_filesystem_sqlite_metadata_exists(internal_dir) {
        return Ok(());
    }

    let metadata = std::fs::symlink_metadata(internal_dir).map_err(|error| {
        io_error(
            "read legacy filesystem metadata directory",
            internal_dir,
            error,
        )
    })?;
    if metadata.file_type().is_symlink() {
        let display = internal_dir.display();
        return Err(filesystem_error(format!(
            "legacy filesystem metadata directory {display} must not be a symlink"
        )));
    }
    if !metadata.is_dir() {
        let display = internal_dir.display();
        return Err(filesystem_error(format!(
            "legacy filesystem metadata path {display} must be a directory"
        )));
    }

    std::fs::remove_dir_all(internal_dir).map_err(|error| {
        io_error(
            "remove legacy filesystem metadata directory",
            internal_dir,
            error,
        )
    })
}

#[cfg(feature = "local_filesystem")]
fn legacy_filesystem_sqlite_metadata_exists(internal_dir: &Path) -> bool {
    LEGACY_FILESYSTEM_SQLITE_METADATA_NAMES
        .iter()
        .any(|name| internal_dir.join(name).exists())
}

#[cfg(feature = "local_filesystem")]
fn remove_legacy_metadata_file(path: &Path) -> Result<(), LixError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(io_error(
                "read legacy filesystem metadata file",
                path,
                error,
            ));
        }
    };
    if metadata.file_type().is_symlink() {
        let display = path.display();
        return Err(filesystem_error(format!(
            "legacy filesystem metadata file {display} must not be a symlink"
        )));
    }
    if !metadata.is_file() {
        let display = path.display();
        return Err(filesystem_error(format!(
            "legacy filesystem metadata path {display} must be a file"
        )));
    }
    std::fs::remove_file(path)
        .map_err(|error| io_error("remove legacy filesystem metadata file", path, error))
}

#[cfg(feature = "local_filesystem")]
fn remove_legacy_metadata_path(path: &Path) -> Result<(), LixError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(io_error(
                "read legacy filesystem metadata path",
                path,
                error,
            ));
        }
    };
    if metadata.file_type().is_symlink() {
        let display = path.display();
        return Err(filesystem_error(format!(
            "legacy filesystem metadata path {display} must not be a symlink"
        )));
    }
    if metadata.is_dir() {
        return std::fs::remove_dir_all(path)
            .map_err(|error| io_error("remove legacy filesystem metadata directory", path, error));
    }
    if metadata.is_file() {
        return std::fs::remove_file(path)
            .map_err(|error| io_error("remove legacy filesystem metadata file", path, error));
    }
    let display = path.display();
    Err(filesystem_error(format!(
        "legacy filesystem metadata path {display} must be a file or directory"
    )))
}

#[cfg(feature = "local_filesystem")]
fn ensure_metadata_directory(path: &Path, label: &str) -> Result<(), LixError> {
    match std::fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(io_error(&format!("create {label}"), path, error)),
    }

    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| io_error(&format!("read {label}"), path, error))?;
    if metadata.file_type().is_symlink() {
        let display = path.display();
        return Err(filesystem_error(format!(
            "{label} {display} must not be a symlink"
        )));
    }
    if !metadata.is_dir() {
        let display = path.display();
        return Err(filesystem_error(format!(
            "{label} {display} must be a directory"
        )));
    }
    Ok(())
}

fn ensure_gitignore(directory: &Path, content: &[u8]) -> Result<(), LixError> {
    let gitignore = directory.join(".gitignore");
    match std::fs::read(&gitignore) {
        Ok(existing) if existing == content => return Ok(()),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(io_error("read filesystem .gitignore", &gitignore, error));
        }
    }
    std::fs::write(&gitignore, content)
        .map_err(|error| io_error("write filesystem .gitignore", &gitignore, error))
}

const LEGACY_FILESYSTEM_SQLITE_METADATA_NAMES: &[&str] = &[
    "db.sqlite",
    "db.sqlite-wal",
    "db.sqlite-shm",
    "db.sqlite-journal",
];

#[cfg(feature = "local_filesystem")]
fn rocksdb_error(error: StorageError) -> LixError {
    LixError::new(
        LixError::CODE_STORAGE_ERROR,
        format!("failed to open filesystem RocksDB storage: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "local_filesystem")]
    use lix_engine::Value;

    #[cfg(feature = "local_filesystem")]
    async fn lix_read_file<StorageImpl>(
        session: &SessionContext<StorageImpl>,
        path: &str,
    ) -> Result<Option<Vec<u8>>, LixError>
    where
        StorageImpl: Storage + Clone + Send + Sync + 'static,
    {
        let result = session
            .execute(
                "SELECT data FROM lix_file WHERE path = $1",
                &[Value::Text(path.to_string())],
            )
            .await?;
        result
            .rows()
            .first()
            .map(|row| row.get::<Vec<u8>>("data"))
            .transpose()
    }

    #[cfg(feature = "local_filesystem")]
    async fn lix_write_file<StorageImpl>(
        session: &SessionContext<StorageImpl>,
        path: &str,
        data: Vec<u8>,
    ) -> Result<(), LixError>
    where
        StorageImpl: Storage + Clone + Send + Sync + 'static,
    {
        session
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
             ON CONFLICT (path) DO UPDATE SET data = excluded.data",
                &[Value::Text(path.to_string()), Value::Blob(data)],
            )
            .await?;
        Ok(())
    }

    #[cfg(feature = "local_filesystem")]
    async fn open_test_filesystem_state(
        layout: FilesystemLayout,
        path_filter: FilesystemPathFilter,
    ) -> FilesystemState<RocksDBFilesystem> {
        let storage = open_filesystem_rocksdb(&layout).unwrap();
        let engine = crate::lix::open_or_initialize_engine(storage.clone(), None, None)
            .await
            .unwrap();
        FilesystemState {
            session: engine.open_workspace_session().await.unwrap(),
            layout,
            path_filter: Mutex::new(path_filter),
            sync_lock: tokio::sync::Mutex::new(()),
            last_materialized: Mutex::new(None),
        }
    }

    #[test]
    fn local_paths_render_opaque_segments() {
        let root = Path::new("root");

        assert_eq!(
            local_path_to_lix_path(root, &root.join("bad%name.txt"), false).unwrap(),
            "/bad%name.txt"
        );
        assert_eq!(
            local_path_to_lix_path(root, &root.join("#hash?.txt"), false).unwrap(),
            "/#hash?.txt"
        );
        assert_eq!(
            local_path_to_lix_path(root, &root.join("dir%23"), true).unwrap(),
            "/dir%23/"
        );
    }

    #[cfg(unix)]
    #[test]
    fn local_paths_preserve_backslash_segments_on_unix() {
        let root = Path::new("root");

        assert_eq!(
            local_path_to_lix_path(root, &root.join(r"a\b.txt"), false).unwrap(),
            r"/a\b.txt"
        );
        assert_eq!(
            lix_path_to_local_path(root, r"/a\b.txt").unwrap(),
            root.join(r"a\b.txt")
        );
    }

    #[test]
    fn lix_paths_map_opaque_segments_to_local_paths() {
        let root = Path::new("root");

        assert_eq!(
            lix_path_to_local_path(root, "/bad%name.txt").unwrap(),
            root.join("bad%name.txt")
        );
        assert_eq!(
            lix_path_to_local_path(root, "/#hash?.txt").unwrap(),
            root.join("#hash?.txt")
        );
    }

    #[test]
    fn lix_paths_reject_structurally_unsafe_segments() {
        let root = Path::new("root");

        for path in ["relative", "/a//b", "/./b", "/../b"] {
            let error = lix_path_to_local_path(root, path).expect_err("path should fail");
            assert_eq!(error.code, "LIX_FILESYSTEM_ERROR");
        }
    }

    #[test]
    fn collect_local_snapshot_hydrates_top_level_directories() {
        let tempdir = tempfile::tempdir().unwrap();
        let root = tempdir.path();
        let lix_dir = root.join(".lix");
        ensure_filesystem_lix_directory(&lix_dir).unwrap();

        std::fs::write(root.join("root.txt"), b"root").unwrap();
        for index in 0..FILESYSTEM_PARALLEL_SNAPSHOT_MIN_DIRS {
            let dir = root.join(format!("dir-{index}"));
            let nested = dir.join("nested");
            std::fs::create_dir_all(&nested).unwrap();
            std::fs::write(dir.join("file.txt"), format!("file-{index}")).unwrap();
            std::fs::write(nested.join("deep.txt"), format!("deep-{index}")).unwrap();
        }

        let layout = FilesystemLayout {
            root: std::fs::canonicalize(root).unwrap(),
            lix_dir: std::fs::canonicalize(lix_dir).unwrap(),
            lix_dir_is_default: true,
        };
        let snapshot = collect_local_snapshot(&layout, &FilesystemPathFilter::default()).unwrap();

        assert!(snapshot.directories.contains("/"));
        assert_eq!(snapshot.files.get("/root.txt").unwrap(), b"root");
        for index in 0..FILESYSTEM_PARALLEL_SNAPSHOT_MIN_DIRS {
            assert!(snapshot.directories.contains(&format!("/dir-{index}/")));
            assert!(
                snapshot
                    .directories
                    .contains(&format!("/dir-{index}/nested/"))
            );
            assert_eq!(
                snapshot
                    .files
                    .get(&format!("/dir-{index}/file.txt"))
                    .unwrap(),
                format!("file-{index}").as_bytes()
            );
            assert_eq!(
                snapshot
                    .files
                    .get(&format!("/dir-{index}/nested/deep.txt"))
                    .unwrap(),
                format!("deep-{index}").as_bytes()
            );
        }
        assert_eq!(
            snapshot.files.len(),
            1 + (FILESYSTEM_PARALLEL_SNAPSHOT_MIN_DIRS * 2)
        );
        assert!(snapshot.unmanaged_paths.is_empty());
    }

    #[test]
    fn collect_local_snapshot_with_empty_filter_keeps_lix_storage_only() {
        let tempdir = tempfile::tempdir().unwrap();
        let root = tempdir.path();
        std::fs::write(root.join("root.md"), b"root").unwrap();
        std::fs::create_dir_all(root.join("docs")).unwrap();
        std::fs::write(root.join("docs").join("note.markdown"), b"note").unwrap();

        let layout = prepare_filesystem_layout(root, None).unwrap();
        std::fs::create_dir_all(layout.lix_dir.join("app_data")).unwrap();
        std::fs::write(
            layout.lix_dir.join("app_data").join("test.bin"),
            b"internal",
        )
        .unwrap();
        let path_filter = FilesystemPathFilter::from_sync_all_files(false);

        let snapshot = collect_local_snapshot(&layout, &path_filter).unwrap();

        assert!(!path_filter.is_unfiltered());
        assert!(snapshot.directories.contains("/"));
        assert!(!snapshot.directories.contains("/docs/"));
        assert!(!snapshot.files.contains_key("/root.md"));
        assert!(!snapshot.files.contains_key("/docs/note.markdown"));
        assert!(snapshot.directories.contains("/.lix/"));
        assert!(snapshot.directories.contains("/.lix/app_data/"));
        assert_eq!(
            snapshot.files.get("/.lix/app_data/test.bin").unwrap(),
            b"internal"
        );
    }

    #[test]
    fn lix_file_upsert_sql_batches_path_data_rows() {
        assert_eq!(
            lix_file_upsert_sql(3),
            "INSERT INTO lix_file (path, data) VALUES ($1, $2), ($3, $4), ($5, $6) ON CONFLICT (path) DO UPDATE SET data = excluded.data"
        );
    }

    #[test]
    fn lix_file_upsert_chunk_end_respects_row_and_byte_budgets() {
        let a = [0u8; 3];
        let b = [0u8; 4];
        let c = [0u8; 4];
        let files = [
            ("/a", a.as_slice()),
            ("/b", b.as_slice()),
            ("/c", c.as_slice()),
        ];

        assert_eq!(lix_file_upsert_chunk_end(&files, 0, 2, usize::MAX), 2);
        assert_eq!(lix_file_upsert_chunk_end(&files, 0, 10, 11), 2);
        assert_eq!(lix_file_upsert_chunk_end(&files, 1, 10, 6), 2);
    }

    #[test]
    fn lix_file_upsert_chunk_end_allows_single_file_over_byte_budget() {
        let large = [0u8; 16];
        let small = [0u8; 1];
        let files = [
            ("/large.bin", large.as_slice()),
            ("/small.bin", small.as_slice()),
        ];

        assert_eq!(lix_file_upsert_chunk_end(&files, 0, 10, 8), 1);
    }

    #[cfg(feature = "local_filesystem")]
    #[tokio::test]
    async fn disk_sync_remembers_canonical_snapshot_for_idle_skip() {
        let tempdir = tempfile::tempdir().unwrap();
        let layout = prepare_filesystem_layout(tempdir.path(), None).unwrap();
        let state = open_test_filesystem_state(layout, FilesystemPathFilter::default()).await;

        Box::pin(state.sync_disk_to_lix(false)).await.unwrap();

        let path_filter = state.path_filter();
        let local = collect_local_snapshot(&state.layout, &path_filter).unwrap();
        let lix_revision = state.collect_lix_revision().await.unwrap();
        assert!(
            state.is_last_materialized(&local, &lix_revision),
            "an unchanged filesystem should be recognized as already materialized"
        );

        state.close().await.unwrap();
    }

    #[cfg(feature = "local_filesystem")]
    #[tokio::test]
    async fn disk_sync_does_not_reimport_unchanged_materialized_file_deleted_in_lix() {
        let tempdir = tempfile::tempdir().unwrap();
        let layout = prepare_filesystem_layout(tempdir.path(), None).unwrap();
        let state = open_test_filesystem_state(layout, FilesystemPathFilter::default()).await;

        Box::pin(state.sync_disk_to_lix(false)).await.unwrap();
        lix_write_file(&state.session, "/sql.txt", b"updated".to_vec())
            .await
            .unwrap();
        state.sync_from_lix().await.unwrap();
        assert_eq!(
            std::fs::read(tempdir.path().join("sql.txt")).unwrap(),
            b"updated"
        );

        state
            .session
            .execute(
                "DELETE FROM lix_file WHERE path = $1",
                &[Value::Text("/sql.txt".to_string())],
            )
            .await
            .unwrap();
        Box::pin(state.sync_disk_to_lix(true)).await.unwrap();

        assert!(!tempdir.path().join("sql.txt").exists());
        let rows = state
            .session
            .execute(
                "SELECT path FROM lix_file WHERE path = $1",
                &[Value::Text("/sql.txt".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 0);

        state.close().await.unwrap();
    }

    #[cfg(feature = "local_filesystem")]
    #[tokio::test]
    async fn disk_sync_does_not_skip_lix_side_file_data_change() {
        let tempdir = tempfile::tempdir().unwrap();
        let layout = prepare_filesystem_layout(tempdir.path(), None).unwrap();
        let state = open_test_filesystem_state(layout, FilesystemPathFilter::default()).await;

        Box::pin(state.sync_disk_to_lix(false)).await.unwrap();
        lix_write_file(&state.session, "/sql.txt", b"first".to_vec())
            .await
            .unwrap();
        state.sync_from_lix().await.unwrap();
        assert_eq!(
            std::fs::read(tempdir.path().join("sql.txt")).unwrap(),
            b"first"
        );

        lix_write_file(&state.session, "/sql.txt", b"second".to_vec())
            .await
            .unwrap();
        Box::pin(state.sync_disk_to_lix(true)).await.unwrap();

        assert_eq!(
            std::fs::read(tempdir.path().join("sql.txt")).unwrap(),
            b"second"
        );

        state.close().await.unwrap();
    }

    #[cfg(feature = "local_filesystem")]
    #[tokio::test]
    async fn disk_sync_materialization_preserves_file_changed_after_import() {
        let tempdir = tempfile::tempdir().unwrap();
        let layout = prepare_filesystem_layout(tempdir.path(), None).unwrap();
        let state = open_test_filesystem_state(layout, FilesystemPathFilter::default()).await;

        Box::pin(state.sync_disk_to_lix(false)).await.unwrap();
        let disk_path = tempdir.path().join("disk.txt");
        std::fs::write(&disk_path, b"disk").unwrap();
        let path_filter = state.path_filter();
        let local = collect_local_snapshot(&state.layout, &path_filter).unwrap();
        let previous = state.last_materialized_disk();
        state
            .apply_local_snapshot_to_lix_with_filter(&local, previous.as_ref(), &path_filter)
            .await
            .unwrap();

        assert_eq!(
            lix_read_file(&state.session, "/disk.txt")
                .await
                .unwrap()
                .as_deref(),
            Some(b"disk".as_slice())
        );
        std::fs::write(&disk_path, b"changed").unwrap();

        let target = state.collect_lix_snapshot_read().await.unwrap();
        let materialized = state
            .materialize_snapshot_with_filter(&target.snapshot, Some(&local), &path_filter)
            .unwrap();
        state.remember_materialized(
            materialized,
            target.revision,
            lix_file_paths(&target.snapshot),
        );
        assert_eq!(std::fs::read(&disk_path).unwrap(), b"changed");

        Box::pin(state.sync_disk_to_lix(true)).await.unwrap();
        assert_eq!(
            lix_read_file(&state.session, "/disk.txt")
                .await
                .unwrap()
                .as_deref(),
            Some(b"changed".as_slice())
        );

        state.close().await.unwrap();
    }

    #[cfg(feature = "local_filesystem")]
    #[tokio::test]
    async fn local_filesystem_sync_disk_to_lix_respects_include_paths() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("tracked.md"), b"initial").unwrap();
        std::fs::write(tempdir.path().join("ignored.md"), b"ignored").unwrap();

        let storage = LocalFilesystem::open_with_options(LocalFilesystemOpenOptions {
            root: tempdir.path().to_path_buf(),
            lix_dir: None,
            sync_all_files: false,
        })
        .await
        .unwrap();
        let lix = crate::lix::open_lix_with_storage(storage.clone())
            .await
            .unwrap();
        storage.import_paths(["tracked.md"]).await.unwrap();

        std::fs::write(tempdir.path().join("tracked.md"), b"changed").unwrap();
        std::fs::write(tempdir.path().join("ignored.md"), b"changed").unwrap();

        storage.sync_disk_to_lix().await.unwrap();

        let tracked = lix
            .execute(
                "SELECT data FROM lix_file WHERE path = $1",
                &[Value::Text("/tracked.md".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(
            tracked
                .rows()
                .first()
                .unwrap()
                .get::<Vec<u8>>("data")
                .unwrap(),
            b"changed"
        );

        let ignored = lix
            .execute(
                "SELECT data FROM lix_file WHERE path = $1",
                &[Value::Text("/ignored.md".to_string())],
            )
            .await
            .unwrap();
        assert!(ignored.rows().is_empty());

        lix.close().await.unwrap();
    }

    #[cfg(feature = "local_filesystem")]
    #[tokio::test]
    async fn local_filesystem_sync_disk_to_lix_outlives_lix_session() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("tracked.md"), b"initial").unwrap();

        let storage = LocalFilesystem::open_with_options(LocalFilesystemOpenOptions {
            root: tempdir.path().to_path_buf(),
            lix_dir: None,
            sync_all_files: true,
        })
        .await
        .unwrap();
        let lix = crate::lix::open_lix_with_storage(storage.clone())
            .await
            .unwrap();

        lix.close().await.unwrap();
        std::fs::write(tempdir.path().join("tracked.md"), b"changed").unwrap();

        storage.sync_disk_to_lix().await.unwrap();

        let lix = crate::lix::open_lix_with_storage(storage).await.unwrap();
        let tracked = lix
            .execute(
                "SELECT data FROM lix_file WHERE path = $1",
                &[Value::Text("/tracked.md".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(
            tracked
                .rows()
                .first()
                .unwrap()
                .get::<Vec<u8>>("data")
                .unwrap(),
            b"changed"
        );
        lix.close().await.unwrap();
    }

    #[cfg(feature = "local_filesystem")]
    #[tokio::test]
    async fn external_lix_dir_stores_rocksdb_and_lix_files_outside_root() {
        let root = tempfile::tempdir().unwrap();
        let external = tempfile::tempdir().unwrap();
        let lix_dir = external.path().join(".lix");

        let storage = LocalFilesystem::open_with_options(LocalFilesystemOpenOptions {
            root: root.path().to_path_buf(),
            lix_dir: Some(lix_dir.clone()),
            sync_all_files: true,
        })
        .await
        .unwrap();
        let lix = crate::lix::open_lix_with_storage(storage).await.unwrap();

        lix.execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text("/.lix/app_data/test.bin".to_string()),
                Value::Blob(b"plugin".to_vec()),
            ],
        )
        .await
        .unwrap();

        assert!(lix_dir.join(".internal").join("rocksdb").is_dir());
        assert_eq!(
            std::fs::read(lix_dir.join("app_data").join("test.bin")).unwrap(),
            b"plugin"
        );
        assert!(!root.path().join(".lix").exists());

        lix.close().await.unwrap();
    }
}
