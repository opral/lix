use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;

use lix_engine::wasm::WasmRuntime;
use lix_engine::{
    Backend, BackendError, BackendRead, BackendWrite, CommitResult, Engine, GetOptions,
    InMemoryBackend, Key, KeyRange, LixError, PointVisitor, PutBatch, ReadOptions, ScanOptions,
    ScanResult, ScanVisitor, SessionContext, SessionTransaction, SpaceId, Value, WriteOptions,
};
use notify_debouncer_full::notify::{Config, RecommendedWatcher, RecursiveMode};
use notify_debouncer_full::{DebounceEventResult, Debouncer, RecommendedCache, new_debouncer_opt};

#[cfg(feature = "sqlite")]
use crate::sqlite_backend::SqliteBackend;

type FilesystemDebouncer = Debouncer<RecommendedWatcher, RecommendedCache>;
const LIX_DIRECTORY_GITIGNORE: &[u8] = b"*\n";
const FILESYSTEM_POLL_INTERVAL: Duration = Duration::from_secs(15);
// Each file upsert uses two SQL parameters. 400 rows stays under SQLite's
// historical 999-parameter floor while reducing per-chunk live-state scans.
const FILESYSTEM_FILE_UPSERT_CHUNK_SIZE: usize = 400;
const FILESYSTEM_FILE_UPSERT_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;
const FILESYSTEM_UNRESOLVED_METADATA_KEY: &str = "lix_fs_unresolved";

#[derive(Clone)]
pub(crate) struct FilesystemSync<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    inner: B,
    supervisor: FilesystemSupervisor<B>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilesystemMetadataMode {
    Persistent,
    Ephemeral,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct FsBackendFilter {
    pub include_paths: Vec<String>,
}

impl FsBackendFilter {
    pub fn include_paths(include_paths: Vec<String>) -> Self {
        Self { include_paths }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct FilesystemPathFilter {
    include_files: BTreeSet<String>,
}

pub(crate) struct FilesystemWrite<'a, B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    inner: B::Write<'a>,
    supervisor: FilesystemSupervisor<B>,
}

#[cfg(feature = "sqlite")]
#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct FsBackend {
    inner: FsBackendInner,
}

#[cfg(feature = "sqlite")]
#[derive(Clone)]
enum FsBackendInner {
    Persistent(FilesystemSync<SqliteBackend>),
    Memory(FilesystemSync<InMemoryBackend>),
}

#[cfg(feature = "sqlite")]
#[expect(missing_debug_implementations)]
pub enum FsRead<'a> {
    Persistent(crate::sqlite_backend::SqliteRead),
    Memory(<InMemoryBackend as Backend>::Read<'a>),
}

#[cfg(feature = "sqlite")]
#[expect(missing_debug_implementations)]
pub struct FsWrite<'a> {
    inner: FsWriteInner<'a>,
}

#[cfg(feature = "sqlite")]
enum FsWriteInner<'a> {
    Persistent(FilesystemWrite<'a, SqliteBackend>),
    Memory(FilesystemWrite<'a, InMemoryBackend>),
}

#[derive(Clone)]
struct FilesystemSupervisor<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    inner: Arc<FilesystemSupervisorInner>,
    _marker: PhantomData<fn() -> B>,
}

struct FilesystemSupervisorInner {
    event_tx: mpsc::Sender<FilesystemEvent>,
    debouncer: Mutex<Option<FilesystemDebouncer>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

struct FilesystemState<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    session: SessionContext<B>,
    root: PathBuf,
    metadata_mode: FilesystemMetadataMode,
    path_filter: FilesystemPathFilter,
    sync_lock: tokio::sync::Mutex<()>,
    last_materialized: Mutex<Option<MaterializedState>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Snapshot {
    directories: BTreeSet<String>,
    files: BTreeMap<String, Vec<u8>>,
    unmanaged_paths: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct InventorySnapshot {
    directories: BTreeSet<String>,
    files: BTreeSet<String>,
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
    fn from_filter(filter: FsBackendFilter) -> Result<Self, LixError> {
        let mut include_files = BTreeSet::new();
        for path in filter.include_paths {
            include_files.insert(normalize_filter_file_path(&path)?);
        }
        Ok(Self { include_files })
    }

    fn is_unfiltered(&self) -> bool {
        self.include_files.is_empty()
    }

    fn includes_file(&self, path: &str) -> bool {
        self.is_unfiltered() || self.include_files.contains(path)
    }

    fn includes_directory(&self, path: &str) -> bool {
        if self.is_unfiltered() || path == "/" {
            return true;
        }
        self.include_files.iter().any(|file_path| {
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

    fn local_watch_paths(&self, root: &Path) -> Result<Vec<PathBuf>, LixError> {
        let mut paths = BTreeSet::new();
        for path in &self.include_files {
            let local_path = lix_path_to_local_path(root, path)?;
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MaterializedState {
    Bytes(MaterializedSnapshot),
    Inventory {
        disk: InventorySnapshot,
        lix_revision: LixRevision,
    },
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct LixInventoryRead {
    directories: BTreeSet<String>,
    directory_ids: BTreeMap<String, String>,
    files: BTreeSet<String>,
    unresolved_files: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiskSyncOutcome {
    NeedsMaterialization,
    InventoryMaterialized,
}

enum FilesystemEvent {
    DiskChanged,
    SyncFromLix {
        reply_tx: mpsc::SyncSender<Result<(), LixError>>,
    },
    Shutdown,
}

#[cfg(feature = "sqlite")]
impl FsBackend {
    pub async fn open<P>(dir: P) -> Result<Self, LixError>
    where
        P: AsRef<Path>,
    {
        Self::open_with_filter(dir, FsBackendFilter::default()).await
    }

    pub async fn open_with_filter<P>(dir: P, filter: FsBackendFilter) -> Result<Self, LixError>
    where
        P: AsRef<Path>,
    {
        FilesystemPathFilter::from_filter(filter.clone())?;
        let backend = open_filesystem_sqlite_backend(dir.as_ref())?;
        let inner = FilesystemSync::open_with_metadata_mode(
            backend,
            dir.as_ref(),
            FilesystemMetadataMode::Persistent,
            filter,
        )
        .await?;
        Ok(Self {
            inner: FsBackendInner::Persistent(inner),
        })
    }

    pub async fn open_memory<P>(dir: P) -> Result<Self, LixError>
    where
        P: AsRef<Path>,
    {
        Self::open_memory_with_filter(dir, FsBackendFilter::default()).await
    }

    pub async fn open_memory_with_filter<P>(
        dir: P,
        filter: FsBackendFilter,
    ) -> Result<Self, LixError>
    where
        P: AsRef<Path>,
    {
        FilesystemPathFilter::from_filter(filter.clone())?;
        let backend = InMemoryBackend::new();
        let inner = FilesystemSync::open_with_metadata_mode(
            backend,
            dir.as_ref(),
            FilesystemMetadataMode::Ephemeral,
            filter,
        )
        .await?;
        Ok(Self {
            inner: FsBackendInner::Memory(inner),
        })
    }

    pub async fn open_with_wasm_runtime<P>(
        dir: P,
        wasm_runtime: Arc<dyn WasmRuntime>,
    ) -> Result<Self, LixError>
    where
        P: AsRef<Path>,
    {
        let backend = open_filesystem_sqlite_backend(dir.as_ref())?;
        let inner =
            FilesystemSync::open_with_wasm_runtime(backend, dir.as_ref(), wasm_runtime).await?;
        Ok(Self {
            inner: FsBackendInner::Persistent(inner),
        })
    }
}

#[cfg(feature = "sqlite")]
impl Backend for FsBackend {
    type Read<'a>
        = FsRead<'a>
    where
        Self: 'a;

    type Write<'a>
        = FsWrite<'a>
    where
        Self: 'a;

    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        match &self.inner {
            FsBackendInner::Persistent(inner) => Ok(FsRead::Persistent(inner.begin_read(opts)?)),
            FsBackendInner::Memory(inner) => Ok(FsRead::Memory(inner.begin_read(opts)?)),
        }
    }

    fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        match &self.inner {
            FsBackendInner::Persistent(inner) => Ok(FsWrite {
                inner: FsWriteInner::Persistent(inner.begin_write(opts)?),
            }),
            FsBackendInner::Memory(inner) => Ok(FsWrite {
                inner: FsWriteInner::Memory(inner.begin_write(opts)?),
            }),
        }
    }
}

#[cfg(feature = "sqlite")]
impl BackendRead for FsRead<'_> {
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
        match self {
            Self::Persistent(read) => read.visit_keys(space, keys, opts, visitor),
            Self::Memory(read) => read.visit_keys(space, keys, opts, visitor),
        }
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
        match self {
            Self::Persistent(read) => read.scan(space, range, opts, visitor),
            Self::Memory(read) => read.scan(space, range, opts, visitor),
        }
    }
}

#[cfg(feature = "sqlite")]
impl BackendWrite for FsWrite<'_> {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        match &mut self.inner {
            FsWriteInner::Persistent(write) => write.put_many(space, entries),
            FsWriteInner::Memory(write) => write.put_many(space, entries),
        }
    }

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        match &mut self.inner {
            FsWriteInner::Persistent(write) => write.delete_many(space, keys),
            FsWriteInner::Memory(write) => write.delete_many(space, keys),
        }
    }

    fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), BackendError> {
        match &mut self.inner {
            FsWriteInner::Persistent(write) => write.delete_range(space, range),
            FsWriteInner::Memory(write) => write.delete_range(space, range),
        }
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        match self.inner {
            FsWriteInner::Persistent(write) => write.commit(),
            FsWriteInner::Memory(write) => write.commit(),
        }
    }

    fn rollback(self) -> Result<(), BackendError> {
        match self.inner {
            FsWriteInner::Persistent(write) => write.rollback(),
            FsWriteInner::Memory(write) => write.rollback(),
        }
    }
}

impl<B> FilesystemSync<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    pub async fn open_with_wasm_runtime<P>(
        backend: B,
        root: P,
        wasm_runtime: Arc<dyn WasmRuntime>,
    ) -> Result<Self, LixError>
    where
        P: AsRef<Path>,
    {
        let engine =
            crate::lix::open_or_initialize_engine(backend.clone(), Some(wasm_runtime)).await?;
        Self::open_with_engine(
            backend,
            engine,
            root.as_ref(),
            FilesystemMetadataMode::Persistent,
            FsBackendFilter::default(),
        )
        .await
    }

    async fn open_with_metadata_mode<P>(
        backend: B,
        root: P,
        metadata_mode: FilesystemMetadataMode,
        filter: FsBackendFilter,
    ) -> Result<Self, LixError>
    where
        P: AsRef<Path>,
    {
        let engine = crate::lix::open_or_initialize_engine(backend.clone(), None).await?;
        Self::open_with_engine(backend, engine, root.as_ref(), metadata_mode, filter).await
    }

    async fn open_with_engine(
        backend: B,
        engine: Engine<B>,
        root: &Path,
        metadata_mode: FilesystemMetadataMode,
        filter: FsBackendFilter,
    ) -> Result<Self, LixError> {
        Ok(Self {
            inner: backend,
            supervisor: FilesystemSupervisor::open(engine, root, metadata_mode, filter).await?,
        })
    }
}

impl<B> Backend for FilesystemSync<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    type Read<'a>
        = B::Read<'a>
    where
        Self: 'a;

    type Write<'a>
        = FilesystemWrite<'a, B>
    where
        Self: 'a;

    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        self.inner.begin_read(opts)
    }

    fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(FilesystemWrite {
            inner: self.inner.begin_write(opts)?,
            supervisor: self.supervisor.clone(),
        })
    }
}

impl<B> BackendWrite for FilesystemWrite<'_, B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        self.inner.put_many(space, entries)
    }

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        self.inner.delete_many(space, keys)
    }

    fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), BackendError> {
        self.inner.delete_range(space, range)
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        let result = self.inner.commit()?;
        self.supervisor.sync_from_lix_blocking()?;
        Ok(result)
    }

    fn rollback(self) -> Result<(), BackendError> {
        self.inner.rollback()
    }
}

impl<B> FilesystemSupervisor<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    async fn open(
        engine: Engine<B>,
        root: &Path,
        metadata_mode: FilesystemMetadataMode,
        filter: FsBackendFilter,
    ) -> Result<Self, LixError> {
        ensure_filesystem_root_directory(root)?;
        let root = std::fs::canonicalize(root)
            .map_err(|error| io_error("canonicalize filesystem root", root, error))?;
        let path_filter = FilesystemPathFilter::from_filter(filter)?;
        if metadata_mode == FilesystemMetadataMode::Persistent {
            ensure_filesystem_lix_directory(&root)?;
            migrate_legacy_filesystem_system_directory(&root)?;
        }
        let session = engine.open_workspace_session().await?;
        let state = Arc::new(FilesystemState {
            session,
            root,
            metadata_mode,
            path_filter,
            sync_lock: tokio::sync::Mutex::new(()),
            last_materialized: Mutex::new(None),
        });
        let (event_tx, event_rx) = mpsc::channel();
        let watcher_setup = state.path_filter.is_unfiltered().then(|| {
            start_filesystem_watcher_setup(
                state.root.clone(),
                state.metadata_mode,
                state.path_filter.clone(),
                event_tx.clone(),
            )
        });

        let initial_sync_result = async {
            if metadata_mode == FilesystemMetadataMode::Persistent {
                state.migrate_legacy_lix_system_paths().await?;
            }
            if state.sync_disk_to_lix(false).await? == DiskSyncOutcome::NeedsMaterialization {
                state.sync_from_lix().await?;
            }
            Ok::<(), LixError>(())
        }
        .await;

        let debouncer = if let Some(watcher_setup) = watcher_setup.flatten() {
            let debouncer = finish_filesystem_watcher_setup(Some(watcher_setup));
            initial_sync_result?;
            debouncer
        } else {
            initial_sync_result?;
            create_filesystem_debouncer(
                &state.root,
                state.metadata_mode,
                &state.path_filter,
                event_tx.clone(),
            )
        };
        let poll_filesystem = cfg!(target_os = "macos") || debouncer.is_none();
        let worker_state = Arc::clone(&state);
        let worker = std::thread::Builder::new()
            .name("lix-sdk-filesystem-sync".to_string())
            .spawn(move || filesystem_worker(worker_state, event_rx, poll_filesystem))
            .map_err(|error| {
                LixError::new(
                    "LIX_FILESYSTEM_THREAD_ERROR",
                    format!("failed to start filesystem sync worker: {error}"),
                )
            })?;

        Ok(Self {
            inner: Arc::new(FilesystemSupervisorInner {
                event_tx,
                debouncer: Mutex::new(debouncer),
                worker: Mutex::new(Some(worker)),
            }),
            _marker: PhantomData,
        })
    }

    fn sync_from_lix_blocking(&self) -> Result<(), BackendError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.inner
            .event_tx
            .send(FilesystemEvent::SyncFromLix { reply_tx })
            .map_err(|error| {
                BackendError::Io(format!(
                    "filesystem sync failed: filesystem worker stopped: {error}"
                ))
            })?;
        match reply_rx.recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(filesystem_sync_backend_error(error)),
            Err(error) => Err(BackendError::Io(format!(
                "filesystem sync failed: filesystem worker stopped: {error}"
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
        if let Ok(mut debouncer) = self.debouncer.lock() {
            let _ = debouncer.take().map(FilesystemDebouncer::stop);
        }
        let _ = self.event_tx.send(FilesystemEvent::Shutdown);
        if let Ok(mut worker) = self.worker.lock() {
            if let Some(worker) = worker.take() {
                let _ = worker.join();
            }
        }
    }
}

fn start_filesystem_watcher_setup(
    root: PathBuf,
    metadata_mode: FilesystemMetadataMode,
    path_filter: FilesystemPathFilter,
    event_tx: mpsc::Sender<FilesystemEvent>,
) -> Option<JoinHandle<Option<FilesystemDebouncer>>> {
    std::thread::Builder::new()
        .name("lix-sdk-filesystem-watch-setup".to_string())
        .spawn(move || create_filesystem_debouncer(&root, metadata_mode, &path_filter, event_tx))
        .ok()
}

fn finish_filesystem_watcher_setup(
    watcher_setup: Option<JoinHandle<Option<FilesystemDebouncer>>>,
) -> Option<FilesystemDebouncer> {
    watcher_setup.and_then(|handle| handle.join().ok().flatten())
}

impl<B> FilesystemState<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    async fn sync_from_lix(&self) -> Result<(), LixError> {
        let _guard = self.sync_lock.lock().await;
        if let Some(lix_inventory) = self.lix_inventory_for_inventory_sync().await? {
            let resolved = self
                .collect_lix_resolved_snapshot_read(&lix_inventory)
                .await?;
            self.materialize_inventory_snapshot(&resolved, &lix_inventory)?;
            let lix_revision = self.collect_lix_revision().await?;
            let materialized_inventory =
                collect_local_inventory(&self.root, self.metadata_mode, &self.path_filter)?;
            self.remember_inventory(materialized_inventory, lix_revision);
            return Ok(());
        }
        let lix_revision = self.collect_lix_revision().await?;
        if self.is_last_materialized_lix_revision(&lix_revision) {
            let local = collect_local_snapshot(&self.root, self.metadata_mode, &self.path_filter)?;
            if self.is_last_materialized_disk(&local) {
                return Ok(());
            }
        }
        let lix = self.collect_lix_snapshot_read().await?;
        let disk = self.materialize_snapshot(&lix.snapshot)?;
        self.remember_materialized(disk, lix.revision);
        Ok(())
    }

    async fn sync_disk_to_lix(
        &self,
        skip_if_last_materialized: bool,
    ) -> Result<DiskSyncOutcome, LixError> {
        let _guard = self.sync_lock.lock().await;
        if let Some(lix_inventory) = self.lix_inventory_for_inventory_sync().await? {
            let inventory =
                collect_local_inventory(&self.root, self.metadata_mode, &self.path_filter)?;
            let has_resolved_files = lix_inventory
                .files
                .difference(&lix_inventory.unresolved_files)
                .next()
                .is_some();
            let has_plugin_files = inventory
                .files
                .iter()
                .any(|path| is_plugin_storage_path(path));
            if skip_if_last_materialized
                && !has_resolved_files
                && !has_plugin_files
                && self.is_last_materialized_inventory(&inventory)
            {
                let lix_revision = self.collect_lix_revision().await?;
                if self.is_last_materialized_inventory_with_revision(&inventory, &lix_revision) {
                    return Ok(DiskSyncOutcome::InventoryMaterialized);
                }
            }
            self.apply_local_inventory_to_lix(
                &inventory,
                &lix_inventory,
                !skip_if_last_materialized,
            )
            .await?;
            self.apply_plugin_local_file_data_to_lix(&inventory).await?;
            self.apply_resolved_local_file_data_to_lix(&inventory, &lix_inventory)
                .await?;
            let lix_revision = self.collect_lix_revision().await?;
            self.remember_inventory(inventory, lix_revision);
            return Ok(DiskSyncOutcome::InventoryMaterialized);
        }
        let local = collect_local_snapshot(&self.root, self.metadata_mode, &self.path_filter)?;
        if skip_if_last_materialized && self.is_last_materialized_disk(&local) {
            let lix_revision = self.collect_lix_revision().await?;
            if self.is_last_materialized(&local, &lix_revision) {
                return Ok(DiskSyncOutcome::NeedsMaterialization);
            }
        }
        let previous = self.last_materialized_disk();
        self.apply_local_snapshot_to_lix(&local, previous.as_ref())
            .await?;
        let lix = self.collect_lix_snapshot_read().await?;
        let materialized = self.materialize_snapshot_after_disk_sync(&lix.snapshot, &local)?;
        self.remember_materialized(materialized, lix.revision);
        Ok(DiskSyncOutcome::NeedsMaterialization)
    }

    async fn close(&self) -> Result<(), LixError> {
        self.session.close().await
    }

    async fn migrate_legacy_lix_system_paths(&self) -> Result<(), LixError> {
        let files = self
            .session
            .execute(
                "SELECT path FROM lix_file \
                 WHERE path = '/.lix_system' OR path = '/.lix_system/' OR path LIKE '/.lix_system/%' \
                 ORDER BY path",
                &[],
            )
            .await?;
        let legacy_files = files
            .rows()
            .iter()
            .map(|row| row.get::<String>("path"))
            .collect::<Result<Vec<_>, LixError>>()?;
        for path in legacy_files
            .iter()
            .filter(|path| is_legacy_lix_system_path(path))
        {
            if let Some(new_path) = migrate_legacy_lix_system_path(path) {
                let rows = self
                    .session
                    .execute(
                        "SELECT data FROM lix_file WHERE path = $1",
                        &[Value::Text(path.clone())],
                    )
                    .await?;
                let Some(row) = rows.rows().first() else {
                    continue;
                };
                let data = row.get::<Vec<u8>>("data").map_err(|error| {
                    if error.code == "LIX_FILESYSTEM_DATA_UNRESOLVED" {
                        LixError::new(
                            "LIX_FILESYSTEM_LEGACY_UNRESOLVED",
                            format!("legacy filesystem path {path:?} has unresolved data"),
                        )
                        .with_hint(
                            "Hydrate or remove the legacy .lix_system file before filesystem open.",
                        )
                    } else {
                        error
                    }
                })?;
                self.session
                    .execute(
                        "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
                         ON CONFLICT (path) DO UPDATE SET data = excluded.data",
                        &[Value::Text(new_path.clone()), Value::Blob(data.clone())],
                    )
                    .await?;
                write_materialized_file(&self.root, &new_path, &data, self.metadata_mode)?;
            }
            self.session
                .execute(
                    "DELETE FROM lix_file WHERE path = $1",
                    &[Value::Text(path.clone())],
                )
                .await?;
        }

        let directories = self
            .session
            .execute("SELECT path FROM lix_directory ORDER BY path", &[])
            .await?;
        let mut directory_paths = directories
            .rows()
            .iter()
            .map(|row| row.get::<String>("path"))
            .collect::<Result<Vec<_>, _>>()?;
        directory_paths.retain(|path| is_legacy_lix_system_path(path));
        sort_directories_shallowest_first(&mut directory_paths);
        for path in &directory_paths {
            if let Some(new_path) = migrate_legacy_lix_system_path(path) {
                self.session
                    .execute(
                        "INSERT INTO lix_directory (path) VALUES ($1) ON CONFLICT (path) DO NOTHING",
                        &[Value::Text(new_path.clone())],
                    )
                    .await?;
                create_materialized_directory(&self.root, &new_path, self.metadata_mode)?;
            }
        }
        sort_directories_deepest_first(&mut directory_paths);
        for path in directory_paths {
            self.session
                .execute(
                    "DELETE FROM lix_directory WHERE path = $1",
                    &[Value::Text(path)],
                )
                .await?;
        }
        Ok(())
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

    async fn collect_lix_inventory_read(&self) -> Result<LixInventoryRead, LixError> {
        let mut directories = BTreeSet::from(["/".to_string()]);
        let mut directory_ids = BTreeMap::new();
        let mut files = BTreeSet::new();
        let mut unresolved_files = BTreeSet::new();
        let statements: [(&str, &[Value]); 2] = [
            ("SELECT path, id FROM lix_directory ORDER BY path", &[]),
            (
                "SELECT path, lixcol_metadata FROM lix_file ORDER BY path",
                &[],
            ),
        ];
        let batch = self
            .session
            .execute_coherent_read_batch(&statements)
            .await?;
        let [directory_rows, file_rows] = batch.results.try_into().map_err(|results: Vec<_>| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "coherent filesystem inventory read returned {} result sets",
                    results.len()
                ),
            )
        })?;
        for row in directory_rows.rows() {
            let path = row.get::<String>("path")?;
            if path != "/" {
                let id = row.get::<String>("id")?;
                directory_ids.insert(path.clone(), id);
            }
            directories.insert(path);
        }
        for row in file_rows.rows() {
            let path = row.get::<String>("path")?;
            if metadata_value_is_filesystem_unresolved(row.value("lixcol_metadata")?)? {
                unresolved_files.insert(path.clone());
            }
            files.insert(path);
        }
        Ok(LixInventoryRead {
            directories,
            directory_ids,
            files,
            unresolved_files,
        })
    }

    async fn collect_lix_resolved_snapshot_read(
        &self,
        inventory: &LixInventoryRead,
    ) -> Result<Snapshot, LixError> {
        let mut snapshot = Snapshot::default();
        snapshot.directories = inventory.directories.clone();
        for path in inventory.files.difference(&inventory.unresolved_files) {
            if is_materialization_ignored_path(path, self.metadata_mode) {
                continue;
            }
            let rows = self
                .session
                .execute(
                    "SELECT data FROM lix_file WHERE path = $1",
                    &[Value::Text(path.clone())],
                )
                .await?;
            let Some(row) = rows.rows().first() else {
                continue;
            };
            snapshot
                .files
                .insert(path.clone(), row.get::<Vec<u8>>("data")?);
        }
        Ok(snapshot)
    }

    async fn lix_inventory_for_inventory_sync(&self) -> Result<Option<LixInventoryRead>, LixError> {
        let inventory = self.collect_lix_inventory_read().await?;
        let should_sync = !inventory.unresolved_files.is_empty()
            || (inventory.files.is_empty()
                && inventory.directories.iter().all(|path| {
                    path == "/" || is_materialization_ignored_path(path, self.metadata_mode)
                }));
        Ok(should_sync.then_some(inventory))
    }

    async fn apply_local_inventory_to_lix(
        &self,
        local: &InventorySnapshot,
        lix: &LixInventoryRead,
        delete_all_missing: bool,
    ) -> Result<(), LixError> {
        let previous_inventory = self.last_materialized_inventory();
        let mut files_to_import = local
            .files
            .difference(&lix.files)
            .filter(|path| !is_materialization_ignored_path(path, self.metadata_mode))
            .filter(|path| {
                delete_all_missing
                    || !previous_inventory
                        .as_ref()
                        .is_some_and(|previous| previous.files.contains(*path))
            })
            .cloned()
            .collect::<Vec<_>>();
        files_to_import.sort();
        let mut directories_to_import = local
            .directories
            .difference(&lix.directories)
            .filter(|path| path.as_str() != "/")
            .filter(|path| !is_materialization_ignored_path(path, self.metadata_mode))
            .filter(|path| {
                delete_all_missing
                    || !previous_inventory
                        .as_ref()
                        .is_some_and(|previous| previous.directories.contains(*path))
            })
            .cloned()
            .collect::<Vec<_>>();
        sort_directories_shallowest_first(&mut directories_to_import);

        if !directories_to_import.is_empty() || !files_to_import.is_empty() {
            let existing_directory_ids = lix
                .directory_ids
                .iter()
                .map(|(path, id)| (path.clone(), id.clone()))
                .collect::<Vec<_>>();
            self.session
                .import_unresolved_filesystem_inventory(
                    &existing_directory_ids,
                    &directories_to_import,
                    &files_to_import,
                )
                .await?;
        }

        for path in lix.files.difference(&local.files) {
            if !delete_all_missing
                && !previous_inventory
                    .as_ref()
                    .is_some_and(|previous| previous.files.contains(path))
            {
                continue;
            }
            if self.path_filter.includes_file(path)
                && !is_materialization_ignored_path(path, self.metadata_mode)
            {
                if inventory_unmanaged_blocks_lix_path(local, path)
                    || lix_path_blocked_by_unmanaged(&self.root, path)?
                {
                    continue;
                }
                self.session
                    .execute(
                        "DELETE FROM lix_file WHERE path = $1",
                        &[Value::Text(path.clone())],
                    )
                    .await?;
            }
        }

        if self.path_filter.is_unfiltered() {
            let mut directories_to_remove = lix
                .directories
                .difference(&local.directories)
                .filter(|path| path.as_str() != "/")
                .filter(|path| !is_materialization_ignored_path(path, self.metadata_mode))
                .filter(|path| {
                    delete_all_missing
                        || previous_inventory
                            .as_ref()
                            .is_some_and(|previous| previous.directories.contains(*path))
                })
                .cloned()
                .collect::<Vec<_>>();
            sort_directories_deepest_first(&mut directories_to_remove);
            for path in directories_to_remove {
                if inventory_unmanaged_blocks_lix_path(local, &path)
                    || lix_path_blocked_by_unmanaged(&self.root, &path)?
                {
                    continue;
                }
                self.session
                    .execute(
                        "DELETE FROM lix_directory WHERE path = $1",
                        &[Value::Text(path)],
                    )
                    .await?;
            }
        }

        Ok(())
    }

    async fn apply_resolved_local_file_data_to_lix(
        &self,
        local: &InventorySnapshot,
        lix: &LixInventoryRead,
    ) -> Result<(), LixError> {
        self.apply_selected_local_file_data_to_lix(
            local.files.iter().filter(|path| {
                lix.files.contains(*path)
                    && !lix.unresolved_files.contains(*path)
                    && !is_plugin_storage_path(path)
            }),
            true,
        )
        .await
    }

    async fn apply_plugin_local_file_data_to_lix(
        &self,
        local: &InventorySnapshot,
    ) -> Result<(), LixError> {
        self.apply_selected_local_file_data_to_lix(
            local
                .files
                .iter()
                .filter(|path| is_valid_plugin_storage_archive_path(path)),
            false,
        )
        .await
    }

    async fn apply_selected_local_file_data_to_lix<'a>(
        &self,
        paths: impl IntoIterator<Item = &'a String>,
        compare_existing: bool,
    ) -> Result<(), LixError> {
        let mut file_upserts = Vec::with_capacity(FILESYSTEM_FILE_UPSERT_CHUNK_SIZE);
        let mut file_transaction = None;
        let mut file_transaction_bytes = 0usize;

        for path in paths {
            if is_materialization_ignored_path(path, self.metadata_mode) {
                continue;
            }
            let local_path = lix_path_to_local_path(&self.root, path)?;
            let data = std::fs::read(&local_path)
                .map_err(|error| io_error("read resolved filesystem file", &local_path, error))?;
            if compare_existing && self.lix_file_data_equals(path, &data).await? {
                continue;
            }
            file_upserts.push((path.clone(), data));
            if file_upserts.len() == FILESYSTEM_FILE_UPSERT_CHUNK_SIZE {
                self.execute_owned_file_upsert_chunk(
                    &mut file_transaction,
                    &mut file_transaction_bytes,
                    &file_upserts,
                )
                .await?;
                file_upserts.clear();
            }
        }

        if !file_upserts.is_empty() {
            self.execute_owned_file_upsert_chunk(
                &mut file_transaction,
                &mut file_transaction_bytes,
                &file_upserts,
            )
            .await?;
        }
        if let Some(transaction) = file_transaction {
            transaction.commit().await?;
        }

        Ok(())
    }

    async fn lix_file_data_equals(&self, path: &str, data: &[u8]) -> Result<bool, LixError> {
        let rows = self
            .session
            .execute(
                "SELECT data FROM lix_file WHERE path = $1",
                &[Value::Text(path.to_string())],
            )
            .await?;
        let Some(row) = rows.rows().first() else {
            return Ok(false);
        };
        Ok(row.get::<Vec<u8>>("data")? == data)
    }

    async fn collect_lix_revision(&self) -> Result<LixRevision, LixError> {
        let batch = self.session.execute_coherent_read_batch(&[]).await?;
        Ok(LixRevision {
            active_branch_id: batch.active_branch_id,
            active_branch_commit_id: batch.active_branch_commit_id,
            storage_mutation_revision: batch.storage_mutation_revision,
        })
    }

    async fn apply_local_snapshot_to_lix(
        &self,
        local: &Snapshot,
        previous: Option<&Snapshot>,
    ) -> Result<(), LixError> {
        let lix = self.collect_lix_snapshot_read().await?.snapshot;
        let mut transaction = self.session.begin_transaction().await?;

        let metadata_result = async {
            let mut changed = false;
            for path in lix.files.keys() {
                if !local.files.contains_key(path)
                    && self.path_filter.includes_file(path)
                    && !is_plugin_storage_path(path)
                    && !is_materialization_ignored_path(path, self.metadata_mode)
                {
                    if previous
                        .as_ref()
                        .is_some_and(|snapshot| !snapshot.files.contains_key(path))
                    {
                        continue;
                    }
                    if lix_path_blocked_by_unmanaged(&self.root, path)?
                        || snapshot_unmanaged_blocks_lix_path(previous, path)
                    {
                        continue;
                    }
                    transaction
                        .execute(
                            "DELETE FROM lix_file WHERE path = $1",
                            &[Value::Text(path.clone())],
                        )
                        .await?;
                    changed = true;
                }
            }

            if self.path_filter.is_unfiltered() {
                let mut directories_to_remove = Vec::new();
                for path in lix.directories.difference(&local.directories) {
                    if path.as_str() == "/"
                        || is_plugin_storage_path(path)
                        || is_materialization_ignored_path(path, self.metadata_mode)
                    {
                        continue;
                    }
                    if previous
                        .as_ref()
                        .is_some_and(|snapshot| !snapshot.directories.contains(path))
                    {
                        continue;
                    }
                    if lix_path_blocked_by_unmanaged(&self.root, path)?
                        || snapshot_unmanaged_blocks_lix_path(previous, path)
                    {
                        continue;
                    }
                    directories_to_remove.push(path.clone());
                }
                sort_directories_deepest_first(&mut directories_to_remove);
                for path in directories_to_remove {
                    transaction
                        .execute(
                            "DELETE FROM lix_directory WHERE path = $1",
                            &[Value::Text(path)],
                        )
                        .await?;
                    changed = true;
                }
            }

            let mut directories_to_create = local
                .directories
                .difference(&lix.directories)
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
                transaction
                    .execute(
                        "INSERT INTO lix_directory (path) VALUES ($1) ON CONFLICT (path) DO NOTHING",
                        &[Value::Text(path)],
                    )
                    .await?;
                changed = true;
            }

            Ok(changed)
        }
        .await;

        match metadata_result {
            Ok(true) => {
                transaction.commit().await?;
            }
            Ok(false) => {
                transaction.rollback().await?;
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        }

        let mut file_upserts = Vec::with_capacity(FILESYSTEM_FILE_UPSERT_CHUNK_SIZE);
        let mut file_transaction = None;
        let mut file_transaction_bytes = 0usize;
        for (path, data) in local
            .files
            .iter()
            .filter(|(path, _)| !is_materialization_ignored_path(path, self.metadata_mode))
        {
            if previous
                .as_ref()
                .is_some_and(|snapshot| snapshot.files.get(path) == Some(data))
            {
                continue;
            }
            if lix.files.get(path) != Some(data) {
                file_upserts.push((path, data));
                if file_upserts.len() == FILESYSTEM_FILE_UPSERT_CHUNK_SIZE {
                    self.execute_file_upsert_chunk(
                        &mut file_transaction,
                        &mut file_transaction_bytes,
                        &file_upserts,
                    )
                    .await?;
                    file_upserts.clear();
                }
            }
        }
        if !file_upserts.is_empty() {
            self.execute_file_upsert_chunk(
                &mut file_transaction,
                &mut file_transaction_bytes,
                &file_upserts,
            )
            .await?;
        }
        if let Some(transaction) = file_transaction {
            transaction.commit().await?;
        }

        Ok(())
    }

    async fn execute_file_upsert_chunk(
        &self,
        current_transaction: &mut Option<SessionTransaction<B>>,
        current_transaction_bytes: &mut usize,
        file_upserts: &[(&String, &Vec<u8>)],
    ) -> Result<(), LixError> {
        if file_upserts.is_empty() {
            return Ok(());
        }
        if current_transaction.is_none() {
            *current_transaction = Some(self.session.begin_transaction().await?);
        }
        let chunk_bytes = file_upsert_chunk_bytes(file_upserts);
        let result = {
            let transaction = current_transaction
                .as_mut()
                .expect("file upsert transaction should be open");
            self.execute_file_upsert_chunk_in_transaction(transaction, file_upserts)
                .await
        };

        match result {
            Ok(()) => {
                *current_transaction_bytes = current_transaction_bytes.saturating_add(chunk_bytes);
                if *current_transaction_bytes >= FILESYSTEM_FILE_UPSERT_TRANSACTION_BYTES {
                    let transaction = current_transaction
                        .take()
                        .expect("file upsert transaction should be open");
                    transaction.commit().await?;
                    *current_transaction_bytes = 0;
                }
                Ok(())
            }
            Err(error) => {
                if let Some(transaction) = current_transaction.take() {
                    let _ = transaction.rollback().await;
                }
                Err(error)
            }
        }
    }

    async fn execute_owned_file_upsert_chunk(
        &self,
        current_transaction: &mut Option<SessionTransaction<B>>,
        current_transaction_bytes: &mut usize,
        file_upserts: &[(String, Vec<u8>)],
    ) -> Result<(), LixError> {
        let file_upserts = file_upserts
            .iter()
            .map(|(path, data)| (path, data))
            .collect::<Vec<_>>();
        self.execute_file_upsert_chunk(
            current_transaction,
            current_transaction_bytes,
            &file_upserts,
        )
        .await
    }

    async fn execute_file_upsert_chunk_in_transaction(
        &self,
        transaction: &mut SessionTransaction<B>,
        file_upserts: &[(&String, &Vec<u8>)],
    ) -> Result<(), LixError> {
        if file_upserts.is_empty() {
            return Ok(());
        }
        let mut sql = String::from("INSERT INTO lix_file (path, data) VALUES ");
        let mut params = Vec::with_capacity(file_upserts.len() * 2);
        for (index, (path, data)) in file_upserts.iter().enumerate() {
            if index > 0 {
                sql.push_str(", ");
            }
            let path_param = index * 2 + 1;
            let data_param = path_param + 1;
            sql.push_str(&format!("(${path_param}, ${data_param})"));
            params.push(Value::Text((*path).clone()));
            params.push(Value::Blob((*data).clone()));
        }
        sql.push_str(" ON CONFLICT (path) DO UPDATE SET data = excluded.data");
        transaction.execute(&sql, &params).await?;
        Ok(())
    }

    fn materialize_snapshot(&self, target: &Snapshot) -> Result<Snapshot, LixError> {
        self.materialize_snapshot_with_base(target, None)
    }

    fn materialize_snapshot_after_disk_sync(
        &self,
        target: &Snapshot,
        base: &Snapshot,
    ) -> Result<Snapshot, LixError> {
        self.materialize_snapshot_with_base(target, Some(base))
    }

    fn materialize_inventory_snapshot(
        &self,
        target: &Snapshot,
        target_inventory: &LixInventoryRead,
    ) -> Result<(), LixError> {
        ensure_filesystem_root_directory(&self.root)?;
        let previous = self.last_materialized_inventory();

        if let Some(previous) = previous.as_ref() {
            for path in previous
                .files
                .difference(&target_inventory.files)
                .filter(|path| {
                    self.path_filter.includes_file(path)
                        && !is_materialization_ignored_path(path, self.metadata_mode)
                })
            {
                remove_materialized_file(&self.root, path, self.metadata_mode)?;
            }

            if self.path_filter.is_unfiltered() {
                let mut directories_to_remove = previous
                    .directories
                    .difference(&target_inventory.directories)
                    .filter(|path| {
                        path.as_str() != "/"
                            && !is_materialization_ignored_path(path, self.metadata_mode)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                sort_directories_deepest_first(&mut directories_to_remove);
                for path in directories_to_remove {
                    remove_materialized_directory(&self.root, &path, self.metadata_mode)?;
                }
            }
        }

        let mut directories_to_create = target
            .directories
            .iter()
            .filter(|path| {
                path.as_str() != "/"
                    && self.path_filter.includes_directory(path)
                    && !is_materialization_ignored_path(path, self.metadata_mode)
            })
            .cloned()
            .collect::<Vec<_>>();
        sort_directories_shallowest_first(&mut directories_to_create);
        for path in directories_to_create {
            create_materialized_directory(&self.root, &path, self.metadata_mode)?;
        }

        for (path, data) in target.files.iter().filter(|(path, _)| {
            self.path_filter.includes_file(path)
                && !is_materialization_ignored_path(path, self.metadata_mode)
        }) {
            let local_path = lix_path_to_local_path(&self.root, path)?;
            if std::fs::read(&local_path).ok().as_deref() != Some(data.as_slice()) {
                write_materialized_file(&self.root, path, data, self.metadata_mode)?;
            }
        }

        Ok(())
    }

    fn materialize_snapshot_with_base(
        &self,
        target: &Snapshot,
        base: Option<&Snapshot>,
    ) -> Result<Snapshot, LixError> {
        ensure_filesystem_root_directory(&self.root)?;
        let local = collect_local_snapshot(&self.root, self.metadata_mode, &self.path_filter)?;
        let previous = self.last_materialized_disk();

        for path in local.files.keys().filter(|path| {
            self.path_filter.includes_file(path)
                && !target.files.contains_key(*path)
                && !is_materialization_ignored_path(path, self.metadata_mode)
                && previous
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.files.contains_key(*path))
        }) {
            if base.is_some_and(|snapshot| {
                !snapshot.files.contains_key(path)
                    || snapshot.files.get(path) != local.files.get(path)
            }) {
                continue;
            }
            remove_materialized_file(&self.root, path, self.metadata_mode)?;
        }

        if self.path_filter.is_unfiltered() {
            let mut directories_to_remove = local
                .directories
                .difference(&target.directories)
                .filter(|path| {
                    path.as_str() != "/"
                        && !is_materialization_ignored_path(path, self.metadata_mode)
                })
                .filter(|path| {
                    previous
                        .as_ref()
                        .is_some_and(|snapshot| snapshot.directories.contains(*path))
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
                remove_materialized_directory(&self.root, &path, self.metadata_mode)?;
            }
        }

        let mut directories_to_create = target
            .directories
            .iter()
            .filter(|path| {
                path.as_str() != "/"
                    && self.path_filter.includes_directory(path)
                    && !is_materialization_ignored_path(path, self.metadata_mode)
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
            create_materialized_directory(&self.root, &path, self.metadata_mode)?;
        }

        for (path, data) in target.files.iter().filter(|(path, _)| {
            self.path_filter.includes_file(path)
                && !is_materialization_ignored_path(path, self.metadata_mode)
        }) {
            if base.is_some_and(|snapshot| snapshot.files.get(path) == Some(data)) {
                continue;
            }
            if base.is_some_and(|snapshot| snapshot.files.get(path) != local.files.get(path)) {
                continue;
            }
            if local.files.get(path) != Some(data) {
                write_materialized_file(&self.root, path, data, self.metadata_mode)?;
            }
        }

        let materialized =
            collect_local_snapshot(&self.root, self.metadata_mode, &self.path_filter)?;
        let mut remembered = target.filtered(&self.path_filter);
        remembered.unmanaged_paths = materialized.unmanaged_paths;
        Ok(remembered)
    }

    fn remember_materialized(&self, disk: Snapshot, lix_revision: LixRevision) {
        *self
            .last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison") =
            Some(MaterializedState::Bytes(MaterializedSnapshot {
                disk,
                lix_revision,
            }));
    }

    fn remember_inventory(&self, disk: InventorySnapshot, lix_revision: LixRevision) {
        *self
            .last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison") =
            Some(MaterializedState::Inventory { disk, lix_revision });
    }

    fn last_materialized_disk(&self) -> Option<Snapshot> {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .as_ref()
            .and_then(|snapshot| match snapshot {
                MaterializedState::Bytes(snapshot) => Some(snapshot.disk.clone()),
                MaterializedState::Inventory { .. } => None,
            })
    }

    fn last_materialized_inventory(&self) -> Option<InventorySnapshot> {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .as_ref()
            .and_then(|snapshot| match snapshot {
                MaterializedState::Inventory { disk, .. } => Some(disk.clone()),
                MaterializedState::Bytes(_) => None,
            })
    }

    fn is_last_materialized_disk(&self, snapshot: &Snapshot) -> bool {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .as_ref()
            .is_some_and(|materialized| match materialized {
                MaterializedState::Bytes(materialized) => &materialized.disk == snapshot,
                MaterializedState::Inventory { .. } => false,
            })
    }

    fn is_last_materialized_inventory(&self, snapshot: &InventorySnapshot) -> bool {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .as_ref()
            .is_some_and(|materialized| match materialized {
                MaterializedState::Inventory { disk, .. } => disk == snapshot,
                MaterializedState::Bytes(_) => false,
            })
    }

    fn is_last_materialized_inventory_with_revision(
        &self,
        disk: &InventorySnapshot,
        lix_revision: &LixRevision,
    ) -> bool {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .as_ref()
            .is_some_and(|materialized| match materialized {
                MaterializedState::Inventory {
                    disk: materialized_disk,
                    lix_revision: materialized_revision,
                } => materialized_disk == disk && materialized_revision == lix_revision,
                MaterializedState::Bytes(_) => false,
            })
    }

    fn is_last_materialized_lix_revision(&self, lix_revision: &LixRevision) -> bool {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .as_ref()
            .is_some_and(|materialized| match materialized {
                MaterializedState::Bytes(materialized) => {
                    &materialized.lix_revision == lix_revision
                }
                MaterializedState::Inventory {
                    lix_revision: materialized_revision,
                    ..
                } => materialized_revision == lix_revision,
            })
    }

    fn is_last_materialized(&self, disk: &Snapshot, lix_revision: &LixRevision) -> bool {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .as_ref()
            .is_some_and(|materialized| {
                matches!(
                    materialized,
                    MaterializedState::Bytes(materialized)
                        if &materialized.disk == disk && &materialized.lix_revision == lix_revision
                )
            })
    }
}

fn filesystem_worker<B>(
    state: Arc<FilesystemState<B>>,
    event_rx: mpsc::Receiver<FilesystemEvent>,
    poll_filesystem: bool,
) where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
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
                if drain_filesystem_events(&runtime, &state, &event_rx, true) {
                    return;
                }
            }
            Ok(FilesystemEvent::SyncFromLix { reply_tx }) => {
                sync_from_lix_for_replies(&runtime, &state, vec![reply_tx]);
                if drain_filesystem_events(&runtime, &state, &event_rx, false) {
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

fn drain_filesystem_events<B>(
    runtime: &tokio::runtime::Runtime,
    state: &Arc<FilesystemState<B>>,
    event_rx: &mpsc::Receiver<FilesystemEvent>,
    mut sync_disk: bool,
) -> bool
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    let mut sync_replies = Vec::new();
    loop {
        match event_rx.try_recv() {
            Ok(FilesystemEvent::DiskChanged) => sync_disk = true,
            Ok(FilesystemEvent::SyncFromLix { reply_tx }) => sync_replies.push(reply_tx),
            Ok(FilesystemEvent::Shutdown) | Err(mpsc::TryRecvError::Disconnected) => {
                let _ = runtime.block_on(state.close());
                return true;
            }
            Err(mpsc::TryRecvError::Empty) => break,
        }
    }
    if sync_disk {
        let _ = runtime.block_on(state.sync_disk_to_lix(true));
    }
    if !sync_replies.is_empty() {
        sync_from_lix_for_replies(runtime, state, sync_replies);
    }
    false
}

fn sync_from_lix_for_replies<B>(
    runtime: &tokio::runtime::Runtime,
    state: &Arc<FilesystemState<B>>,
    replies: Vec<mpsc::SyncSender<Result<(), LixError>>>,
) where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    let result = runtime.block_on(state.sync_from_lix());
    for reply in replies {
        let _ = reply.send(result.clone());
    }
}

fn file_upsert_chunk_bytes(file_upserts: &[(&String, &Vec<u8>)]) -> usize {
    file_upserts.iter().fold(0usize, |total, (path, data)| {
        total.saturating_add(path.len()).saturating_add(data.len())
    })
}

fn collect_local_snapshot(
    root: &Path,
    metadata_mode: FilesystemMetadataMode,
    path_filter: &FilesystemPathFilter,
) -> Result<Snapshot, LixError> {
    validate_filesystem_root_directory(root)?;

    let mut snapshot = Snapshot::default();
    snapshot.directories.insert("/".to_string());
    if path_filter.is_unfiltered() {
        collect_local_directory(root, root, metadata_mode, &mut snapshot)?;
    } else {
        collect_filtered_local_snapshot(root, metadata_mode, path_filter, &mut snapshot)?;
    }
    Ok(snapshot)
}

fn collect_local_inventory(
    root: &Path,
    metadata_mode: FilesystemMetadataMode,
    path_filter: &FilesystemPathFilter,
) -> Result<InventorySnapshot, LixError> {
    validate_filesystem_root_directory(root)?;

    let mut snapshot = InventorySnapshot::default();
    snapshot.directories.insert("/".to_string());
    if path_filter.is_unfiltered() {
        collect_local_inventory_directory(root, root, metadata_mode, &mut snapshot)?;
    } else {
        collect_filtered_local_inventory(root, metadata_mode, path_filter, &mut snapshot)?;
    }
    Ok(snapshot)
}

fn collect_local_inventory_directory(
    root: &Path,
    directory: &Path,
    metadata_mode: FilesystemMetadataMode,
    snapshot: &mut InventorySnapshot,
) -> Result<(), LixError> {
    let entries = std::fs::read_dir(directory)
        .map_err(|error| io_error("read filesystem directory", directory, error))?;
    for entry in entries {
        let entry =
            entry.map_err(|error| io_error("read filesystem directory entry", directory, error))?;
        let path = entry.path();
        if is_filesystem_sync_ignored_local_path(root, &path, metadata_mode) {
            continue;
        }
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io_error("read filesystem entry type", &path, error)),
        };
        if is_unmanaged_file_type(&file_type) {
            remember_unmanaged_inventory_path(root, directory, &path, snapshot);
            continue;
        }
        if file_type.is_dir() {
            let Ok(lix_path) = local_path_to_lix_path(root, &path, true) else {
                remember_unmanaged_inventory_path(root, directory, &path, snapshot);
                continue;
            };
            if is_invalid_plugin_storage_inventory_path(&lix_path, true) {
                remember_unmanaged_inventory_path(root, directory, &path, snapshot);
                continue;
            }
            snapshot.directories.insert(lix_path);
            collect_local_inventory_directory(root, &path, metadata_mode, snapshot)?;
        } else if file_type.is_file() {
            let Ok(lix_path) = local_path_to_lix_path(root, &path, false) else {
                remember_unmanaged_inventory_path(root, directory, &path, snapshot);
                continue;
            };
            if is_invalid_plugin_storage_inventory_path(&lix_path, false) {
                remember_unmanaged_inventory_path(root, directory, &path, snapshot);
                continue;
            }
            snapshot.files.insert(lix_path);
        }
    }
    Ok(())
}

fn collect_filtered_local_inventory(
    root: &Path,
    metadata_mode: FilesystemMetadataMode,
    path_filter: &FilesystemPathFilter,
    snapshot: &mut InventorySnapshot,
) -> Result<(), LixError> {
    for lix_path in &path_filter.include_files {
        if is_filesystem_sync_ignored_lix_path(lix_path, metadata_mode) {
            continue;
        }
        let local_path = lix_path_to_local_path(root, lix_path)?;
        if path_contains_unmanaged_entry(root, &local_path)? {
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
        if is_invalid_plugin_storage_inventory_path(lix_path, false) {
            snapshot.unmanaged_paths.insert(lix_path.clone());
            continue;
        }
        insert_parent_inventory_directories(lix_path, snapshot);
        snapshot.files.insert(lix_path.clone());
    }
    Ok(())
}

fn remember_unmanaged_inventory_path(
    root: &Path,
    directory: &Path,
    path: &Path,
    snapshot: &mut InventorySnapshot,
) {
    if let Ok(lix_path) = local_path_to_lix_path(root, path, false) {
        snapshot.unmanaged_paths.insert(lix_path);
    } else if directory != root {
        if let Ok(parent_path) = local_path_to_lix_path(root, directory, true) {
            snapshot.unmanaged_paths.insert(parent_path);
        }
    }
}

fn insert_parent_inventory_directories(path: &str, snapshot: &mut InventorySnapshot) {
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

fn collect_local_directory(
    root: &Path,
    directory: &Path,
    metadata_mode: FilesystemMetadataMode,
    snapshot: &mut Snapshot,
) -> Result<(), LixError> {
    let entries = std::fs::read_dir(directory)
        .map_err(|error| io_error("read filesystem directory", directory, error))?;
    for entry in entries {
        let entry =
            entry.map_err(|error| io_error("read filesystem directory entry", directory, error))?;
        let path = entry.path();
        if is_filesystem_sync_ignored_local_path(root, &path, metadata_mode) {
            continue;
        }
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io_error("read filesystem entry type", &path, error)),
        };
        if is_unmanaged_file_type(&file_type) {
            remember_unmanaged_local_path(root, directory, &path, snapshot);
            continue;
        }
        if file_type.is_dir() {
            let Ok(lix_path) = local_path_to_lix_path(root, &path, true) else {
                remember_unmanaged_local_path(root, directory, &path, snapshot);
                continue;
            };
            snapshot.directories.insert(lix_path);
            collect_local_directory(root, &path, metadata_mode, snapshot)?;
        } else if file_type.is_file() {
            let Ok(lix_path) = local_path_to_lix_path(root, &path, false) else {
                remember_unmanaged_local_path(root, directory, &path, snapshot);
                continue;
            };
            let data = std::fs::read(&path)
                .map_err(|error| io_error("read filesystem file", &path, error))?;
            snapshot.files.insert(lix_path, data);
        }
    }
    Ok(())
}

fn collect_filtered_local_snapshot(
    root: &Path,
    metadata_mode: FilesystemMetadataMode,
    path_filter: &FilesystemPathFilter,
    snapshot: &mut Snapshot,
) -> Result<(), LixError> {
    for lix_path in &path_filter.include_files {
        if is_filesystem_sync_ignored_lix_path(lix_path, metadata_mode) {
            continue;
        }
        let local_path = lix_path_to_local_path(root, lix_path)?;
        if path_contains_unmanaged_entry(root, &local_path)? {
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

fn remember_unmanaged_local_path(
    root: &Path,
    directory: &Path,
    path: &Path,
    snapshot: &mut Snapshot,
) {
    if let Ok(lix_path) = local_path_to_lix_path(root, path, false) {
        snapshot.unmanaged_paths.insert(lix_path);
    } else if directory != root {
        if let Ok(parent_path) = local_path_to_lix_path(root, directory, true) {
            snapshot.unmanaged_paths.insert(parent_path);
        }
    }
}

fn watch_filesystem_paths(
    debouncer: &mut FilesystemDebouncer,
    root: &Path,
    path_filter: &FilesystemPathFilter,
) -> Result<(), notify_debouncer_full::notify::Error> {
    if path_filter.is_unfiltered() {
        return debouncer.watch(root, RecursiveMode::Recursive);
    }
    for path in path_filter
        .local_watch_paths(root)
        .map_err(|error| notify_debouncer_full::notify::Error::generic(&error.format()))?
    {
        debouncer.watch(&path, RecursiveMode::NonRecursive)?;
    }
    Ok(())
}

fn create_filesystem_debouncer(
    root: &Path,
    metadata_mode: FilesystemMetadataMode,
    path_filter: &FilesystemPathFilter,
    event_tx: mpsc::Sender<FilesystemEvent>,
) -> Option<FilesystemDebouncer> {
    let watcher_config = Config::default().with_follow_symlinks(false);
    let callback_root = root.to_path_buf();
    new_debouncer_opt::<_, RecommendedWatcher, RecommendedCache>(
        Duration::from_millis(500),
        None,
        move |result: DebounceEventResult| {
            if debounce_result_should_sync(&result, &callback_root, metadata_mode) {
                let _ = event_tx.send(FilesystemEvent::DiskChanged);
            }
        },
        RecommendedCache::new(),
        watcher_config,
    )
    .ok()
    .and_then(|mut debouncer| {
        if watch_filesystem_paths(&mut debouncer, root, path_filter).is_ok() {
            Some(debouncer)
        } else {
            debouncer.stop();
            None
        }
    })
}

fn debounce_result_should_sync(
    result: &DebounceEventResult,
    root: &Path,
    metadata_mode: FilesystemMetadataMode,
) -> bool {
    let Ok(events) = result else {
        return true;
    };
    let mut saw_path = false;
    for event in events {
        for path in &event.paths {
            saw_path = true;
            if !is_filesystem_sync_ignored_local_path(root, path, metadata_mode) {
                return true;
            }
        }
    }
    !saw_path
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

fn ensure_filesystem_lix_directory(root: &Path) -> Result<PathBuf, LixError> {
    let lix_dir = root.join(".lix");
    match std::fs::create_dir(&lix_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(io_error(
                "create filesystem .lix directory",
                &lix_dir,
                error,
            ));
        }
    }

    let metadata = std::fs::symlink_metadata(&lix_dir)
        .map_err(|error| io_error("read filesystem .lix directory", &lix_dir, error))?;
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

    ensure_gitignore(&lix_dir, LIX_DIRECTORY_GITIGNORE)?;
    Ok(lix_dir)
}

fn migrate_legacy_filesystem_system_directory(root: &Path) -> Result<(), LixError> {
    let legacy_dir = root.join(".lix_system");
    let metadata = match std::fs::symlink_metadata(&legacy_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(io_error(
                "read legacy filesystem system directory",
                &legacy_dir,
                error,
            ));
        }
    };
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        merge_legacy_directory_contents(root, &legacy_dir, &root.join(".lix"))?;
        std::fs::remove_dir_all(&legacy_dir).map_err(|error| {
            io_error(
                "remove legacy filesystem system directory",
                &legacy_dir,
                error,
            )
        })
    } else {
        std::fs::remove_file(&legacy_dir)
            .map_err(|error| io_error("remove legacy filesystem system path", &legacy_dir, error))
    }
}

fn merge_legacy_directory_contents(
    root: &Path,
    source: &Path,
    target: &Path,
) -> Result<(), LixError> {
    let entries = std::fs::read_dir(source)
        .map_err(|error| io_error("read legacy filesystem system directory", source, error))?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            io_error(
                "read legacy filesystem system directory entry",
                source,
                error,
            )
        })?;
        let source_path = entry.path();
        let file_name = entry.file_name();
        if is_discarded_legacy_system_entry_name(&file_name) {
            remove_legacy_system_entry(&source_path)?;
            continue;
        }
        let target_path = target.join(&file_name);
        let Ok(target_lix_path) = local_path_to_lix_path(root, &target_path, false) else {
            remove_legacy_system_entry(&source_path)?;
            continue;
        };
        if is_filesystem_metadata_path(&target_lix_path) {
            remove_legacy_system_entry(&source_path)?;
            continue;
        }
        let file_type = entry.file_type().map_err(|error| {
            io_error(
                "read legacy filesystem system entry type",
                &source_path,
                error,
            )
        })?;
        if file_type.is_dir() {
            match std::fs::symlink_metadata(&target_path) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                    merge_legacy_directory_contents(root, &source_path, &target_path)?;
                    std::fs::remove_dir(&source_path).map_err(|error| {
                        io_error(
                            "remove migrated legacy filesystem system directory",
                            &source_path,
                            error,
                        )
                    })?;
                }
                Ok(_) => {
                    return Err(filesystem_error(format!(
                        "cannot migrate legacy filesystem system directory {} to {} because the target exists",
                        source_path.display(),
                        target_path.display()
                    )));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::rename(&source_path, &target_path).map_err(|error| {
                        io_error(
                            "move legacy filesystem system directory",
                            &source_path,
                            error,
                        )
                    })?;
                }
                Err(error) => {
                    return Err(io_error(
                        "read legacy filesystem system target",
                        &target_path,
                        error,
                    ));
                }
            }
        } else {
            if target_path.exists() {
                return Err(filesystem_error(format!(
                    "cannot migrate legacy filesystem system file {} to {} because the target exists",
                    source_path.display(),
                    target_path.display()
                )));
            }
            std::fs::rename(&source_path, &target_path).map_err(|error| {
                io_error("move legacy filesystem system file", &source_path, error)
            })?;
        }
    }
    Ok(())
}

fn is_discarded_legacy_system_entry_name(name: &std::ffi::OsStr) -> bool {
    matches!(name.to_str(), Some(".gitignore" | ".DS_Store"))
}

fn remove_legacy_system_entry(path: &Path) -> Result<(), LixError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| io_error("read legacy filesystem system entry", path, error))?;
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        std::fs::remove_dir_all(path)
            .map_err(|error| io_error("remove legacy filesystem system entry", path, error))
    } else {
        std::fs::remove_file(path)
            .map_err(|error| io_error("remove legacy filesystem system entry", path, error))
    }
}

fn remove_materialized_file(
    root: &Path,
    path: &str,
    metadata_mode: FilesystemMetadataMode,
) -> Result<(), LixError> {
    if is_filesystem_sync_ignored_lix_path(path, metadata_mode) {
        return Ok(());
    }
    let Some(local_path) = materialization_local_path(root, path) else {
        return Ok(());
    };
    if path_contains_unmanaged_entry(root, &local_path)? {
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

fn remove_materialized_directory(
    root: &Path,
    path: &str,
    metadata_mode: FilesystemMetadataMode,
) -> Result<(), LixError> {
    if is_filesystem_sync_ignored_lix_path(path, metadata_mode) {
        return Ok(());
    }
    let Some(local_path) = materialization_local_path(root, path) else {
        return Ok(());
    };
    if path_contains_unmanaged_entry(root, &local_path)? {
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

fn create_materialized_directory(
    root: &Path,
    path: &str,
    metadata_mode: FilesystemMetadataMode,
) -> Result<(), LixError> {
    if is_filesystem_sync_ignored_lix_path(path, metadata_mode) {
        return Ok(());
    }
    let Some(local_path) = materialization_local_path(root, path) else {
        return Ok(());
    };
    if path_contains_unmanaged_entry(root, &local_path)? {
        return Ok(());
    }
    std::fs::create_dir_all(&local_path)
        .map_err(|error| io_error("create filesystem directory", &local_path, error))
}

fn write_materialized_file(
    root: &Path,
    path: &str,
    data: &[u8],
    metadata_mode: FilesystemMetadataMode,
) -> Result<(), LixError> {
    if is_filesystem_sync_ignored_lix_path(path, metadata_mode) {
        return Ok(());
    }
    let Some(local_path) = materialization_local_path(root, path) else {
        return Ok(());
    };
    if path_contains_unmanaged_entry(root, &local_path)? {
        return Ok(());
    }
    if let Some(parent) = local_path.parent() {
        if path_contains_unmanaged_entry(root, parent)? {
            return Ok(());
        }
        std::fs::create_dir_all(parent)
            .map_err(|error| io_error("create filesystem file parent", parent, error))?;
        if path_contains_unmanaged_entry(root, parent)? {
            return Ok(());
        }
    }
    if path_contains_unmanaged_entry(root, &local_path)? {
        return Ok(());
    }
    std::fs::write(&local_path, data)
        .map_err(|error| io_error("write filesystem file", &local_path, error))
}

fn lix_path_blocked_by_unmanaged(root: &Path, path: &str) -> Result<bool, LixError> {
    let Some(local_path) = materialization_local_path(root, path) else {
        return Ok(true);
    };
    path_contains_unmanaged_entry(root, &local_path)
}

fn snapshot_unmanaged_blocks_lix_path(snapshot: Option<&Snapshot>, path: &str) -> bool {
    snapshot.is_some_and(|snapshot| {
        snapshot
            .unmanaged_paths
            .iter()
            .any(|unmanaged_path| unmanaged_path_blocks_lix_path(unmanaged_path, path))
    })
}

fn inventory_unmanaged_blocks_lix_path(inventory: &InventorySnapshot, path: &str) -> bool {
    inventory
        .unmanaged_paths
        .iter()
        .any(|unmanaged_path| unmanaged_path_blocks_lix_path(unmanaged_path, path))
}

fn unmanaged_path_blocks_lix_path(unmanaged_path: &str, path: &str) -> bool {
    let unmanaged_path = unmanaged_path.strip_suffix('/').unwrap_or(unmanaged_path);
    let path = path.strip_suffix('/').unwrap_or(path);
    path == unmanaged_path
        || path
            .strip_prefix(unmanaged_path)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn materialization_local_path(root: &Path, path: &str) -> Option<PathBuf> {
    lix_path_to_local_path(root, path).ok()
}

fn path_contains_unmanaged_entry(root: &Path, local_path: &Path) -> Result<bool, LixError> {
    let Ok(relative) = local_path.strip_prefix(root) else {
        return Ok(true);
    };
    let mut current = root.to_path_buf();
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

fn metadata_is_filesystem_unresolved(metadata: Option<&str>) -> bool {
    let Some(metadata) = metadata else {
        return false;
    };
    serde_json::from_str::<serde_json::Value>(metadata)
        .ok()
        .is_some_and(|value| json_metadata_is_filesystem_unresolved(&value))
}

fn metadata_value_is_filesystem_unresolved(value: &Value) -> Result<bool, LixError> {
    match value {
        Value::Null => Ok(false),
        Value::Text(value) => Ok(metadata_is_filesystem_unresolved(Some(value))),
        Value::Json(value) => Ok(json_metadata_is_filesystem_unresolved(value)),
        other => Err(LixError::new(
            "LIX_ERROR_VALUE_TYPE",
            format!("expected nullable metadata, got {other:?}"),
        )),
    }
}

fn json_metadata_is_filesystem_unresolved(value: &serde_json::Value) -> bool {
    value
        .as_object()
        .and_then(|object| object.get(FILESYSTEM_UNRESOLVED_METADATA_KEY))
        .is_some()
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

fn is_invalid_plugin_storage_inventory_path(path: &str, is_directory: bool) -> bool {
    if !is_plugin_storage_path(path) {
        return false;
    }
    if path == "/.lix/plugins" || path == "/.lix/plugins/" {
        return false;
    }
    is_directory || !is_valid_plugin_storage_archive_path(path)
}

fn is_valid_plugin_storage_archive_path(path: &str) -> bool {
    let Some(file_name) = path.strip_prefix("/.lix/plugins/") else {
        return false;
    };
    let Some(plugin_key) = file_name.strip_suffix(".lixplugin") else {
        return false;
    };
    if plugin_key.is_empty() || plugin_key.len() > 128 || plugin_key.contains('/') {
        return false;
    }
    let mut bytes = plugin_key.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && bytes.all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'))
}

fn is_filesystem_metadata_path(path: &str) -> bool {
    path == "/.lix/.gitignore"
        || path == "/.lix/.gitignore/"
        || is_filesystem_internal_path(path)
        || is_legacy_filesystem_sqlite_metadata_path(path)
}

fn is_filesystem_internal_path(path: &str) -> bool {
    path == "/.lix/.internal" || path.starts_with("/.lix/.internal/")
}

fn is_legacy_lix_system_path(path: &str) -> bool {
    let path = path.strip_suffix('/').unwrap_or(path);
    path == "/.lix_system" || path.starts_with("/.lix_system/")
}

fn migrate_legacy_lix_system_path(path: &str) -> Option<String> {
    if path == "/.lix_system" {
        return Some("/.lix".to_string());
    }
    if path == "/.lix_system/" {
        return Some("/.lix/".to_string());
    }
    let new_path = path
        .strip_prefix("/.lix_system/")
        .map(|suffix| format!("/.lix/{suffix}"))?;
    if is_filesystem_metadata_path(&new_path) {
        None
    } else {
        Some(new_path)
    }
}

fn is_legacy_filesystem_sqlite_metadata_path(path: &str) -> bool {
    matches!(
        path.strip_prefix("/.lix/"),
        Some("db.sqlite" | "db.sqlite-wal" | "db.sqlite-shm" | "db.sqlite-journal")
    )
}

fn is_filesystem_sync_ignored_local_path(
    root: &Path,
    path: &Path,
    metadata_mode: FilesystemMetadataMode,
) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return true;
    };
    let mut depth = 0usize;
    let mut first_segment_is_lix = false;
    for component in relative.components() {
        let Component::Normal(segment) = component else {
            return true;
        };
        depth += 1;
        let segment = segment.to_str();
        if segment == Some(".git") {
            return true;
        }
        if metadata_mode == FilesystemMetadataMode::Ephemeral
            && depth == 1
            && matches!(segment, Some(".lix" | ".lix_system"))
        {
            return true;
        }
        if depth == 1 && segment == Some(".lix") {
            first_segment_is_lix = true;
        }
        if depth == 2 && first_segment_is_lix && segment == Some(".gitignore") {
            return true;
        }
        if depth == 2 && first_segment_is_lix && segment == Some(".internal") {
            return true;
        }
        if depth == 2
            && first_segment_is_lix
            && matches!(
                segment,
                Some("db.sqlite" | "db.sqlite-wal" | "db.sqlite-shm" | "db.sqlite-journal")
            )
        {
            return true;
        }
    }
    false
}

fn is_materialization_ignored_path(path: &str, metadata_mode: FilesystemMetadataMode) -> bool {
    match metadata_mode {
        FilesystemMetadataMode::Persistent => is_filesystem_metadata_path(path),
        FilesystemMetadataMode::Ephemeral => {
            is_lix_storage_path(path) || is_legacy_lix_system_path(path)
        }
    }
}

fn is_filesystem_sync_ignored_lix_path(path: &str, metadata_mode: FilesystemMetadataMode) -> bool {
    lix_path_contains_segment(path, ".git") || is_materialization_ignored_path(path, metadata_mode)
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

fn filesystem_sync_backend_error(error: LixError) -> BackendError {
    BackendError::Io(format!("filesystem sync failed: {}", error.format()))
}

fn filesystem_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_FILESYSTEM_ERROR", message)
}

#[cfg(feature = "sqlite")]
fn open_filesystem_sqlite_backend(dir: &Path) -> Result<SqliteBackend, LixError> {
    ensure_filesystem_root_directory(dir)?;
    let metadata_dir = ensure_filesystem_sqlite_metadata_directory(dir)?;
    SqliteBackend::open(metadata_dir.join("db.sqlite")).map_err(sqlite_backend_error)
}

#[cfg(feature = "sqlite")]
fn ensure_filesystem_sqlite_metadata_directory(dir: &Path) -> Result<PathBuf, LixError> {
    let lix_dir = ensure_filesystem_lix_directory(dir)?;
    let metadata_dir = lix_dir.join(".internal");
    match std::fs::create_dir(&metadata_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(io_error(
                "create filesystem SQLite metadata directory",
                &metadata_dir,
                error,
            ));
        }
    }

    let metadata = std::fs::symlink_metadata(&metadata_dir).map_err(|error| {
        io_error(
            "read filesystem SQLite metadata directory",
            &metadata_dir,
            error,
        )
    })?;
    if metadata.file_type().is_symlink() {
        let path = metadata_dir.display();
        return Err(filesystem_error(format!(
            "filesystem SQLite metadata path {path} must not be a symlink"
        )));
    }
    if !metadata.is_dir() {
        let path = metadata_dir.display();
        return Err(filesystem_error(format!(
            "filesystem SQLite metadata path {path} must be a directory"
        )));
    }

    move_legacy_filesystem_sqlite_metadata(&lix_dir, &metadata_dir)?;
    Ok(metadata_dir)
}

fn move_legacy_filesystem_sqlite_metadata(
    lix_dir: &Path,
    metadata_dir: &Path,
) -> Result<(), LixError> {
    let mut files_to_move = Vec::new();
    for file_name in [
        "db.sqlite",
        "db.sqlite-wal",
        "db.sqlite-shm",
        "db.sqlite-journal",
    ] {
        let legacy_path = lix_dir.join(file_name);
        let target_path = metadata_dir.join(file_name);
        let legacy_metadata = match std::fs::symlink_metadata(&legacy_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(io_error(
                    "read legacy filesystem SQLite metadata",
                    &legacy_path,
                    error,
                ));
            }
        };
        if !legacy_metadata.is_file() {
            return Err(filesystem_error(format!(
                "legacy filesystem SQLite metadata {} must be a regular file",
                legacy_path.display()
            )));
        }
        if target_path.exists() {
            return Err(filesystem_error(format!(
                "cannot move legacy filesystem SQLite metadata {} to {} because the target already exists",
                legacy_path.display(),
                target_path.display()
            )));
        }
        files_to_move.push((legacy_path, target_path));
    }

    for (legacy_path, target_path) in files_to_move {
        std::fs::rename(&legacy_path, &target_path).map_err(|error| {
            io_error(
                "move legacy filesystem SQLite metadata",
                &legacy_path,
                error,
            )
        })?;
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

#[cfg(feature = "sqlite")]
fn sqlite_backend_error(error: BackendError) -> LixError {
    LixError::new(
        LixError::CODE_STORAGE_ERROR,
        format!("failed to open filesystem SQLite backend: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "sqlite")]
    use lix_engine::Value;

    async fn lix_read_file<B>(
        session: &SessionContext<B>,
        path: &str,
    ) -> Result<Option<Vec<u8>>, LixError>
    where
        B: Backend + Clone + Send + Sync + 'static,
        for<'backend> B::Read<'backend>: Send,
        for<'backend> B::Write<'backend>: Send,
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

    async fn lix_write_file<B>(
        session: &SessionContext<B>,
        path: &str,
        data: Vec<u8>,
    ) -> Result<(), LixError>
    where
        B: Backend + Clone + Send + Sync + 'static,
        for<'backend> B::Read<'backend>: Send,
        for<'backend> B::Write<'backend>: Send,
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

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn disk_sync_remembers_canonical_snapshot_for_idle_skip() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = open_filesystem_sqlite_backend(tempdir.path()).unwrap();
        let engine = crate::lix::open_or_initialize_engine(backend, None)
            .await
            .unwrap();
        let root = std::fs::canonicalize(tempdir.path()).unwrap();
        let state = FilesystemState {
            session: engine.open_workspace_session().await.unwrap(),
            root,
            metadata_mode: FilesystemMetadataMode::Persistent,
            path_filter: FilesystemPathFilter::default(),
            sync_lock: tokio::sync::Mutex::new(()),
            last_materialized: Mutex::new(None),
        };

        state.sync_disk_to_lix(false).await.unwrap();

        let local =
            collect_local_snapshot(&state.root, state.metadata_mode, &state.path_filter).unwrap();
        let lix_revision = state.collect_lix_revision().await.unwrap();
        assert!(
            state.is_last_materialized(&local, &lix_revision),
            "an unchanged filesystem should be recognized as already materialized"
        );

        state.close().await.unwrap();
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn disk_sync_does_not_reimport_unchanged_materialized_file_deleted_in_lix() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = open_filesystem_sqlite_backend(tempdir.path()).unwrap();
        let engine = crate::lix::open_or_initialize_engine(backend, None)
            .await
            .unwrap();
        let root = std::fs::canonicalize(tempdir.path()).unwrap();
        let state = FilesystemState {
            session: engine.open_workspace_session().await.unwrap(),
            root,
            metadata_mode: FilesystemMetadataMode::Persistent,
            path_filter: FilesystemPathFilter::default(),
            sync_lock: tokio::sync::Mutex::new(()),
            last_materialized: Mutex::new(None),
        };

        state.sync_disk_to_lix(false).await.unwrap();
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
        state.sync_disk_to_lix(true).await.unwrap();

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

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn disk_sync_does_not_skip_lix_side_file_data_change() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = open_filesystem_sqlite_backend(tempdir.path()).unwrap();
        let engine = crate::lix::open_or_initialize_engine(backend, None)
            .await
            .unwrap();
        let root = std::fs::canonicalize(tempdir.path()).unwrap();
        let state = FilesystemState {
            session: engine.open_workspace_session().await.unwrap(),
            root,
            metadata_mode: FilesystemMetadataMode::Persistent,
            path_filter: FilesystemPathFilter::default(),
            sync_lock: tokio::sync::Mutex::new(()),
            last_materialized: Mutex::new(None),
        };

        state.sync_disk_to_lix(false).await.unwrap();
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
        state.sync_disk_to_lix(true).await.unwrap();

        assert_eq!(
            std::fs::read(tempdir.path().join("sql.txt")).unwrap(),
            b"second"
        );

        state.close().await.unwrap();
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn disk_sync_materialization_preserves_file_changed_after_import() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = open_filesystem_sqlite_backend(tempdir.path()).unwrap();
        let engine = crate::lix::open_or_initialize_engine(backend, None)
            .await
            .unwrap();
        let root = std::fs::canonicalize(tempdir.path()).unwrap();
        let state = FilesystemState {
            session: engine.open_workspace_session().await.unwrap(),
            root,
            metadata_mode: FilesystemMetadataMode::Persistent,
            path_filter: FilesystemPathFilter::default(),
            sync_lock: tokio::sync::Mutex::new(()),
            last_materialized: Mutex::new(None),
        };

        state.sync_disk_to_lix(false).await.unwrap();
        let disk_path = tempdir.path().join("disk.txt");
        std::fs::write(&disk_path, b"disk").unwrap();
        let local =
            collect_local_snapshot(&state.root, state.metadata_mode, &state.path_filter).unwrap();
        let previous = state.last_materialized_disk();
        state
            .apply_local_snapshot_to_lix(&local, previous.as_ref())
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
            .materialize_snapshot_after_disk_sync(&target.snapshot, &local)
            .unwrap();
        state.remember_materialized(materialized, target.revision);
        assert_eq!(std::fs::read(&disk_path).unwrap(), b"changed");

        state.sync_disk_to_lix(true).await.unwrap();
        assert_eq!(
            lix_read_file(&state.session, "/disk.txt")
                .await
                .unwrap()
                .as_deref(),
            Some(b"changed".as_slice())
        );

        state.close().await.unwrap();
    }

    #[test]
    fn inventory_unmanaged_blocks_lix_path_descendants() {
        let inventory = InventorySnapshot {
            unmanaged_paths: BTreeSet::from(["/dir/".to_string()]),
            ..InventorySnapshot::default()
        };

        assert!(inventory_unmanaged_blocks_lix_path(
            &inventory,
            "/dir/file.txt"
        ));
        assert!(inventory_unmanaged_blocks_lix_path(&inventory, "/dir/"));
        assert!(!inventory_unmanaged_blocks_lix_path(
            &inventory,
            "/dir-adjacent/file.txt"
        ));
    }
}
