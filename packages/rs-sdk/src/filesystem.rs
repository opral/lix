use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use lix_engine::{
    Backend, BackendError, BackendRead, BackendWrite, CommitResult, Engine, GetOptions,
    InMemoryBackend, Key, KeyRange, LixError, MountedFilesystem, PointVisitor, PutBatch,
    ReadOptions, ScanOptions, ScanResult, ScanVisitor, SessionContext, SpaceId, Value,
    WriteOptions,
};
use notify_debouncer_full::notify::{Config, RecommendedWatcher, RecursiveMode};
use notify_debouncer_full::{DebounceEventResult, Debouncer, RecommendedCache, new_debouncer_opt};

#[cfg(feature = "fs_backend")]
use lix_fs_backend::RocksDbFilesystemBackend;

type FilesystemDebouncer = Debouncer<RecommendedWatcher, RecommendedCache>;
const LIX_DIRECTORY_GITIGNORE: &[u8] = b"*\n";
const FILESYSTEM_POLL_INTERVAL: Duration = Duration::from_secs(15);
const DESCRIPTOR_INSERT_BATCH_MAX_ROWS: usize = 500;
const FILE_UPSERT_BATCH_MAX_ROWS: usize = 500;
const FILE_UPSERT_BATCH_MAX_BYTES: usize = 8 * 1024 * 1024;
const FILESYSTEM_PARALLEL_SNAPSHOT_MAX_WORKERS: usize = 8;
// Avoid paying thread startup cost for tiny directory roots.
const FILESYSTEM_PARALLEL_SNAPSHOT_MIN_DIRS: usize = 4;

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

#[cfg(feature = "fs_backend")]
#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct FsBackend {
    inner: FsBackendInner,
}

#[cfg(feature = "fs_backend")]
#[derive(Clone)]
enum FsBackendInner {
    Persistent(FilesystemSync<RocksDbFilesystemBackend>),
    Memory(FilesystemSync<InMemoryBackend>),
}

#[cfg(feature = "fs_backend")]
#[expect(missing_debug_implementations)]
pub enum FsRead<'a> {
    Persistent(lix_fs_backend::RocksDbFilesystemRead<'a>),
    Memory(<InMemoryBackend as Backend>::Read<'a>),
}

#[cfg(feature = "fs_backend")]
#[expect(missing_debug_implementations)]
pub struct FsWrite<'a> {
    inner: FsWriteInner<'a>,
}

#[cfg(feature = "fs_backend")]
enum FsWriteInner<'a> {
    Persistent(FilesystemWrite<'a, RocksDbFilesystemBackend>),
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
    root: PathBuf,
    metadata_mode: FilesystemMetadataMode,
    path_filter: FilesystemPathFilter,
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
    last_materialized: Mutex<Option<MaterializedSnapshot>>,
}

#[derive(Clone)]
struct FsMountedFilesystem {
    root: PathBuf,
    metadata_mode: FilesystemMetadataMode,
    path_filter: FilesystemPathFilter,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Snapshot {
    directories: BTreeSet<String>,
    files: BTreeSet<String>,
    stored_file_data: BTreeMap<String, Vec<u8>>,
    unmanaged_paths: BTreeSet<String>,
}

#[async_trait::async_trait]
impl MountedFilesystem for FsMountedFilesystem {
    async fn read_file_data(&self, path: &str) -> Result<Option<Vec<u8>>, LixError> {
        if path.ends_with('/')
            || !self.path_filter.includes_file(path)
            || is_materialization_ignored_path(path, self.metadata_mode)
        {
            return Ok(None);
        }
        let local_path = lix_path_to_local_path(&self.root, path)?;
        if path_contains_unmanaged_entry(&self.root, &local_path)? {
            return Ok(None);
        }
        match std::fs::read(&local_path) {
            Ok(data) => Ok(Some(data)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_error(
                "read filesystem stored file data",
                &local_path,
                error,
            )),
        }
    }
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
                .filter(|path| path_filter.includes_file(path))
                .cloned()
                .collect(),
            stored_file_data: self
                .stored_file_data
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
    SyncFromLix {
        reply_tx: mpsc::SyncSender<Result<(), LixError>>,
    },
    Shutdown,
}

#[cfg(feature = "fs_backend")]
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
        let backend = open_filesystem_rocksdb_backend(dir.as_ref())?;
        let inner = FilesystemSync::open_filesystem(
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
        let inner = FilesystemSync::open_filesystem(
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
}

#[cfg(feature = "fs_backend")]
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

    fn mounted_filesystem(&self) -> Option<Arc<dyn MountedFilesystem>> {
        match &self.inner {
            FsBackendInner::Persistent(inner) => inner.mounted_filesystem(),
            FsBackendInner::Memory(inner) => inner.mounted_filesystem(),
        }
    }
}

#[cfg(feature = "fs_backend")]
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

#[cfg(feature = "fs_backend")]
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
    #[cfg(feature = "fs_backend")]
    async fn open_filesystem<P>(
        backend: B,
        root: P,
        metadata_mode: FilesystemMetadataMode,
        filter: FsBackendFilter,
    ) -> Result<Self, LixError>
    where
        P: AsRef<Path>,
    {
        let engine =
            crate::lix::open_or_initialize_filesystem_engine(backend.clone(), None).await?;
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

    fn mounted_filesystem(&self) -> Option<Arc<dyn MountedFilesystem>> {
        Some(Arc::new(FsMountedFilesystem {
            root: self.supervisor.root().to_path_buf(),
            metadata_mode: self.supervisor.metadata_mode(),
            path_filter: self.supervisor.path_filter().clone(),
        }))
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
        }
        let session = engine.open_workspace_session().await?;
        let state = Arc::new(FilesystemState {
            session,
            root: root.clone(),
            metadata_mode,
            path_filter: path_filter.clone(),
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
        .and_then(|mut debouncer| {
            if watch_filesystem_paths(&mut debouncer, &state.root, &state.path_filter).is_ok() {
                Some(debouncer)
            } else {
                debouncer.stop();
                None
            }
        });
        let poll_filesystem = cfg!(target_os = "macos") || debouncer.is_none();
        let worker_state = Arc::clone(&state);
        let worker = thread::Builder::new()
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
                root,
                metadata_mode,
                path_filter,
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

    fn root(&self) -> &Path {
        &self.inner.root
    }

    fn metadata_mode(&self) -> FilesystemMetadataMode {
        self.inner.metadata_mode
    }

    fn path_filter(&self) -> &FilesystemPathFilter {
        &self.inner.path_filter
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

impl<B> FilesystemState<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    async fn sync_from_lix(&self) -> Result<(), LixError> {
        let _guard = self.sync_lock.lock().await;
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

    async fn sync_disk_to_lix(&self, skip_if_last_materialized: bool) -> Result<(), LixError> {
        let _guard = self.sync_lock.lock().await;
        let local = collect_local_snapshot(&self.root, self.metadata_mode, &self.path_filter)?;
        if skip_if_last_materialized && self.is_last_materialized_disk(&local) {
            let lix_revision = self.collect_lix_revision().await?;
            if self.is_last_materialized(&local, &lix_revision) {
                return Ok(());
            }
        }
        let previous = self.last_materialized_disk();
        let lix = self
            .apply_local_snapshot_to_lix(&local, previous.as_ref())
            .await?;
        let materialized = self.materialize_snapshot_after_disk_sync(&lix.snapshot, &local)?;
        self.remember_materialized(materialized, lix.revision);
        Ok(())
    }

    async fn close(&self) -> Result<(), LixError> {
        self.session.close().await
    }

    async fn collect_lix_snapshot_read(&self) -> Result<LixSnapshotRead, LixError> {
        let mut snapshot = Snapshot::default();
        snapshot.directories.insert("/".to_string());
        let statements: [(&str, &[Value]); 3] = [
            ("SELECT path FROM lix_directory ORDER BY path", &[]),
            ("SELECT path FROM lix_file ORDER BY path", &[]),
            (
                "SELECT path, data FROM lix_file \
                 WHERE id IN (\
                    SELECT file_id FROM lix_state \
                    WHERE schema_key = 'lix_binary_blob_ref'\
                 ) \
                 ORDER BY path",
                &[],
            ),
        ];
        let batch = self
            .session
            .execute_coherent_read_batch(&statements)
            .await?;
        let [directories, files, stored_file_data] =
            batch.results.try_into().map_err(|results: Vec<_>| {
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
            snapshot.files.insert(row.get::<String>("path")?);
        }
        for row in stored_file_data.rows() {
            let path = row.get::<String>("path")?;
            let data = row.get::<Vec<u8>>("data")?;
            snapshot.stored_file_data.insert(path, data);
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

    async fn apply_local_snapshot_to_lix(
        &self,
        local: &Snapshot,
        previous: Option<&Snapshot>,
    ) -> Result<LixSnapshotRead, LixError> {
        let lix = self.collect_lix_snapshot_read().await?;
        let mut needs_fresh_lix_read = false;

        for path in &lix.snapshot.files {
            if !local.files.contains(path)
                && self.path_filter.includes_file(path)
                && !is_plugin_storage_path(path)
                && !is_materialization_ignored_path(path, self.metadata_mode)
            {
                if previous
                    .as_ref()
                    .is_some_and(|snapshot| !snapshot.files.contains(path))
                {
                    continue;
                }
                if lix_path_blocked_by_unmanaged(&self.root, path)?
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

        if self.path_filter.is_unfiltered() {
            let mut directories_to_remove = Vec::new();
            for path in lix.snapshot.directories.difference(&local.directories) {
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
        if !directories_to_create.is_empty() {
            needs_fresh_lix_read = true;
            self.insert_local_directories_to_lix(&directories_to_create)
                .await?;
        }

        let mut file_descriptors_to_insert = Vec::new();
        let mut stored_file_data_to_upsert = Vec::new();
        for path in local
            .files
            .iter()
            .filter(|path| !is_materialization_ignored_path(path, self.metadata_mode))
        {
            if !lix.snapshot.files.contains(path) {
                if previous
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.files.contains(path))
                {
                    continue;
                }
                file_descriptors_to_insert.push(path.as_str());
                continue;
            }

            if !lix.snapshot.stored_file_data.contains_key(path) {
                continue;
            }
            let data = read_local_file_data(&self.root, path)?;
            if previous
                .as_ref()
                .is_some_and(|snapshot| snapshot.stored_file_data.get(path) == Some(&data))
            {
                continue;
            }
            if lix.snapshot.stored_file_data.get(path) != Some(&data) {
                stored_file_data_to_upsert.push((path.as_str(), data));
            }
        }
        if !file_descriptors_to_insert.is_empty() {
            needs_fresh_lix_read = true;
            self.insert_local_file_descriptors_to_lix(&file_descriptors_to_insert)
                .await?;
        }
        if !stored_file_data_to_upsert.is_empty() {
            needs_fresh_lix_read = true;
            let stored_file_data_to_upsert = stored_file_data_to_upsert
                .iter()
                .map(|(path, data)| (*path, data.as_slice()))
                .collect::<Vec<_>>();
            self.upsert_local_stored_file_data_to_lix(&stored_file_data_to_upsert)
                .await?;
        }

        if needs_fresh_lix_read || self.collect_lix_revision().await? != lix.revision {
            return self.collect_lix_snapshot_read().await;
        }
        Ok(lix)
    }

    async fn insert_local_directories_to_lix(
        &self,
        directories: &[String],
    ) -> Result<(), LixError> {
        for chunk in directories.chunks(DESCRIPTOR_INSERT_BATCH_MAX_ROWS) {
            let sql = descriptor_insert_sql("lix_directory", chunk.len());
            let params = chunk
                .iter()
                .map(|path| Value::Text(path.clone()))
                .collect::<Vec<_>>();
            self.session.execute(&sql, &params).await?;
        }
        Ok(())
    }

    async fn insert_local_file_descriptors_to_lix(&self, files: &[&str]) -> Result<(), LixError> {
        for chunk in files.chunks(DESCRIPTOR_INSERT_BATCH_MAX_ROWS) {
            let sql = descriptor_insert_sql("lix_file", chunk.len());
            let params = chunk
                .iter()
                .map(|path| Value::Text((*path).to_string()))
                .collect::<Vec<_>>();
            self.session.execute(&sql, &params).await?;
        }
        Ok(())
    }

    async fn upsert_local_stored_file_data_to_lix(
        &self,
        files: &[(&str, &[u8])],
    ) -> Result<(), LixError> {
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

    fn materialize_snapshot_with_base(
        &self,
        target: &Snapshot,
        base: Option<&Snapshot>,
    ) -> Result<Snapshot, LixError> {
        ensure_filesystem_root_directory(&self.root)?;
        let local = collect_local_snapshot(&self.root, self.metadata_mode, &self.path_filter)?;
        let previous = self.last_materialized_disk();

        for path in local.files.iter().filter(|path| {
            self.path_filter.includes_file(path)
                && !target.files.contains(*path)
                && !is_materialization_ignored_path(path, self.metadata_mode)
                && previous
                    .as_ref()
                    .is_none_or(|snapshot| snapshot.files.contains(*path))
        }) {
            if base.is_some_and(|snapshot| {
                !snapshot.files.contains(path)
                    || snapshot.stored_file_data.get(path) != local.stored_file_data.get(path)
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

        for (path, data) in target.stored_file_data.iter().filter(|(path, _)| {
            self.path_filter.includes_file(path)
                && !is_materialization_ignored_path(path, self.metadata_mode)
        }) {
            if base.is_some_and(|snapshot| snapshot.stored_file_data.get(path) == Some(data)) {
                continue;
            }
            let local_data = if local.files.contains(path) {
                Some(read_local_file_data(&self.root, path)?)
            } else {
                None
            };
            if base.is_some() && local_data.is_some() && local_data.as_ref() != Some(data) {
                let base_matches_local = base
                    .and_then(|snapshot| snapshot.stored_file_data.get(path))
                    .is_some_and(|base_data| Some(base_data) == local_data.as_ref());
                let previous_matches_local = previous
                    .as_ref()
                    .and_then(|snapshot| snapshot.stored_file_data.get(path))
                    .is_some_and(|previous_data| Some(previous_data) == local_data.as_ref());
                if !base_matches_local && !previous_matches_local {
                    continue;
                }
            }
            if local_data.as_ref() != Some(data) {
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
            Some(MaterializedSnapshot { disk, lix_revision });
    }

    fn last_materialized_disk(&self) -> Option<Snapshot> {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .as_ref()
            .map(|snapshot| snapshot.disk.clone())
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

fn collect_local_snapshot(
    root: &Path,
    metadata_mode: FilesystemMetadataMode,
    path_filter: &FilesystemPathFilter,
) -> Result<Snapshot, LixError> {
    validate_filesystem_root_directory(root)?;

    let mut snapshot = Snapshot::default();
    snapshot.directories.insert("/".to_string());
    if path_filter.is_unfiltered() {
        let child_dirs = collect_local_directory_shallow(root, root, metadata_mode, &mut snapshot)?;
        let child_snapshot = collect_local_child_directories(root, child_dirs, metadata_mode)?;
        merge_snapshot(&mut snapshot, child_snapshot);
    } else {
        collect_filtered_local_snapshot(root, metadata_mode, path_filter, &mut snapshot)?;
    }
    Ok(snapshot)
}

fn collect_local_child_directories(
    root: &Path,
    child_dirs: Vec<PathBuf>,
    metadata_mode: FilesystemMetadataMode,
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
            collect_local_directory(root, &child_dir, metadata_mode, &mut snapshot)?;
        }
        return Ok(snapshot);
    }

    let chunk_size = child_dirs.len().div_ceil(worker_count);
    let mut handles = Vec::with_capacity(worker_count);
    let mut first_error = None;
    for (worker_index, chunk) in child_dirs.chunks(chunk_size).enumerate() {
        let root = root.to_path_buf();
        let child_dirs = chunk.to_vec();
        let worker = thread::Builder::new()
            .name(format!("lix-sdk-filesystem-snapshot-{worker_index}"))
            .spawn(move || {
                let mut snapshot = Snapshot::default();
                for child_dir in child_dirs {
                    collect_local_directory(&root, &child_dir, metadata_mode, &mut snapshot)?;
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
    root: &Path,
    directory: &Path,
    metadata_mode: FilesystemMetadataMode,
    snapshot: &mut Snapshot,
) -> Result<(), LixError> {
    let child_dirs = collect_local_directory_shallow(root, directory, metadata_mode, snapshot)?;
    for child_dir in child_dirs {
        collect_local_directory(root, &child_dir, metadata_mode, snapshot)?;
    }
    Ok(())
}

fn collect_local_directory_shallow(
    root: &Path,
    directory: &Path,
    metadata_mode: FilesystemMetadataMode,
    snapshot: &mut Snapshot,
) -> Result<Vec<PathBuf>, LixError> {
    let mut child_dirs = Vec::new();
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
            child_dirs.push(path);
        } else if file_type.is_file() {
            let Ok(lix_path) = local_path_to_lix_path(root, &path, false) else {
                remember_unmanaged_local_path(root, directory, &path, snapshot);
                continue;
            };
            snapshot.files.insert(lix_path);
        }
    }
    Ok(child_dirs)
}

fn merge_snapshot(target: &mut Snapshot, source: Snapshot) {
    target.directories.extend(source.directories);
    target.files.extend(source.files);
    target.stored_file_data.extend(source.stored_file_data);
    target.unmanaged_paths.extend(source.unmanaged_paths);
}

fn read_local_file_data(root: &Path, path: &str) -> Result<Vec<u8>, LixError> {
    let local_path = lix_path_to_local_path(root, path)?;
    std::fs::read(&local_path).map_err(|error| io_error("read filesystem file", &local_path, error))
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
        snapshot.files.insert(lix_path.clone());
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

fn descriptor_insert_sql(table_name: &str, row_count: usize) -> String {
    debug_assert!(row_count > 0);
    let mut sql = format!("INSERT INTO {table_name} (path) VALUES ");
    for row in 0..row_count {
        if row > 0 {
            sql.push_str(", ");
        }
        let _ = write!(sql, "(${})", row + 1);
    }
    sql.push_str(" ON CONFLICT (path) DO NOTHING");
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
            && segment == Some(".lix")
        {
            return true;
        }
        if metadata_mode == FilesystemMetadataMode::Persistent
            && depth == 1
            && segment == Some(".lix_system")
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
        if metadata_mode == FilesystemMetadataMode::Persistent
            && depth == 2
            && first_segment_is_lix
            && segment.is_some_and(is_legacy_filesystem_sqlite_metadata_name)
        {
            return true;
        }
    }
    false
}

fn is_materialization_ignored_path(path: &str, metadata_mode: FilesystemMetadataMode) -> bool {
    match metadata_mode {
        FilesystemMetadataMode::Persistent => is_filesystem_metadata_path(path),
        FilesystemMetadataMode::Ephemeral => is_lix_storage_path(path),
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

#[cfg(feature = "fs_backend")]
fn open_filesystem_rocksdb_backend(dir: &Path) -> Result<RocksDbFilesystemBackend, LixError> {
    ensure_filesystem_root_directory(dir)?;
    let metadata_dir = ensure_filesystem_rocksdb_metadata_directory(dir)?;
    RocksDbFilesystemBackend::open(metadata_dir).map_err(rocksdb_backend_error)
}

#[cfg(feature = "fs_backend")]
fn ensure_filesystem_rocksdb_metadata_directory(dir: &Path) -> Result<PathBuf, LixError> {
    let lix_dir = ensure_filesystem_lix_directory(dir)?;
    remove_legacy_filesystem_root_metadata(dir, &lix_dir)?;
    let internal_dir = lix_dir.join(".internal");
    reset_legacy_filesystem_internal_directory(&internal_dir)?;
    ensure_metadata_directory(&internal_dir, "filesystem metadata directory")?;
    let metadata_dir = internal_dir.join("rocksdb");
    ensure_metadata_directory(&metadata_dir, "filesystem RocksDB metadata directory")?;
    Ok(metadata_dir)
}

#[cfg(feature = "fs_backend")]
fn remove_legacy_filesystem_root_metadata(root: &Path, lix_dir: &Path) -> Result<(), LixError> {
    for name in LEGACY_FILESYSTEM_SQLITE_METADATA_NAMES {
        remove_legacy_metadata_file(&lix_dir.join(name))?;
    }
    remove_legacy_metadata_path(&root.join(".lix_system"))
}

#[cfg(feature = "fs_backend")]
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

#[cfg(feature = "fs_backend")]
fn legacy_filesystem_sqlite_metadata_exists(internal_dir: &Path) -> bool {
    LEGACY_FILESYSTEM_SQLITE_METADATA_NAMES
        .iter()
        .any(|name| internal_dir.join(name).exists())
}

#[cfg(feature = "fs_backend")]
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

#[cfg(feature = "fs_backend")]
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

#[cfg(feature = "fs_backend")]
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

#[cfg(feature = "fs_backend")]
fn rocksdb_backend_error(error: BackendError) -> LixError {
    LixError::new(
        LixError::CODE_STORAGE_ERROR,
        format!("failed to open filesystem RocksDB backend: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "fs_backend")]
    use lix_engine::Value;

    #[cfg(feature = "fs_backend")]
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

    #[cfg(feature = "fs_backend")]
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

    #[test]
    fn collect_local_snapshot_records_descriptors_without_file_data() {
        let tempdir = tempfile::tempdir().unwrap();
        let root = tempdir.path();

        std::fs::write(root.join("root.txt"), b"root").unwrap();
        for index in 0..FILESYSTEM_PARALLEL_SNAPSHOT_MIN_DIRS {
            let dir = root.join(format!("dir-{index}"));
            let nested = dir.join("nested");
            std::fs::create_dir_all(&nested).unwrap();
            std::fs::write(dir.join("file.txt"), format!("file-{index}")).unwrap();
            std::fs::write(nested.join("deep.txt"), format!("deep-{index}")).unwrap();
        }

        let snapshot = collect_local_snapshot(
            root,
            FilesystemMetadataMode::Ephemeral,
            &FilesystemPathFilter::default(),
        )
        .unwrap();

        assert!(snapshot.directories.contains("/"));
        assert!(snapshot.files.contains("/root.txt"));
        for index in 0..FILESYSTEM_PARALLEL_SNAPSHOT_MIN_DIRS {
            assert!(snapshot.directories.contains(&format!("/dir-{index}/")));
            assert!(
                snapshot
                    .directories
                    .contains(&format!("/dir-{index}/nested/"))
            );
            assert!(snapshot.files.contains(&format!("/dir-{index}/file.txt")));
            assert!(
                snapshot
                    .files
                    .contains(&format!("/dir-{index}/nested/deep.txt"))
            );
        }
        assert_eq!(
            snapshot.files.len(),
            1 + (FILESYSTEM_PARALLEL_SNAPSHOT_MIN_DIRS * 2)
        );
        assert!(snapshot.stored_file_data.is_empty());
        assert!(snapshot.unmanaged_paths.is_empty());
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

    #[cfg(feature = "fs_backend")]
    #[tokio::test]
    async fn disk_sync_remembers_canonical_snapshot_for_idle_skip() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = open_filesystem_rocksdb_backend(tempdir.path()).unwrap();
        let engine = crate::lix::open_or_initialize_filesystem_engine(backend.clone(), None)
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

    #[cfg(feature = "fs_backend")]
    #[tokio::test]
    async fn disk_sync_does_not_reimport_unchanged_materialized_file_deleted_in_lix() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = open_filesystem_rocksdb_backend(tempdir.path()).unwrap();
        let engine = crate::lix::open_or_initialize_filesystem_engine(backend.clone(), None)
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

    #[cfg(feature = "fs_backend")]
    #[tokio::test]
    async fn disk_sync_does_not_skip_lix_side_file_data_change() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = open_filesystem_rocksdb_backend(tempdir.path()).unwrap();
        let engine = crate::lix::open_or_initialize_filesystem_engine(backend.clone(), None)
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

    #[cfg(feature = "fs_backend")]
    #[tokio::test]
    async fn disk_sync_materialization_preserves_file_changed_after_import() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = open_filesystem_rocksdb_backend(tempdir.path()).unwrap();
        let engine = crate::lix::open_or_initialize_filesystem_engine(backend.clone(), None)
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
        lix_write_file(&state.session, "/disk.txt", b"disk".to_vec())
            .await
            .unwrap();
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

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn fs_backend_mounted_filesystem_reads_disk_file_data() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("note.md"), b"from disk").unwrap();

        let backend = FsBackend::open_memory(tempdir.path()).await.unwrap();
        let resolver = backend
            .mounted_filesystem()
            .expect("FsBackend should expose mounted filesystem");

        let data = resolver
            .read_file_data("/note.md")
            .await
            .expect("mounted filesystem read should succeed")
            .expect("mounted filesystem should find included file");
        assert_eq!(data, b"from disk");

        let missing = resolver
            .read_file_data("/missing.md")
            .await
            .expect("missing mounted filesystem read should not fail");
        assert_eq!(missing, None);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn fs_backend_mounted_filesystem_respects_include_filter() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("included.md"), b"included").unwrap();
        std::fs::write(tempdir.path().join("excluded.md"), b"excluded").unwrap();

        let backend = FsBackend::open_memory_with_filter(
            tempdir.path(),
            FsBackendFilter::include_paths(vec!["included.md".to_string()]),
        )
        .await
        .unwrap();
        let resolver = backend
            .mounted_filesystem()
            .expect("FsBackend should expose mounted filesystem");

        assert_eq!(
            resolver
                .read_file_data("/included.md")
                .await
                .unwrap()
                .as_deref(),
            Some(b"included".as_slice())
        );
        assert_eq!(resolver.read_file_data("/excluded.md").await.unwrap(), None);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn fs_backend_memory_opens_descriptor_only_and_lazy_reads_data() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("note.md"), b"from disk").unwrap();

        let backend = FsBackend::open_memory(tempdir.path()).await.unwrap();
        let engine = Engine::new(backend).await.unwrap();
        let session = engine.open_workspace_session().await.unwrap();

        let files = session
            .execute(
                "SELECT path FROM lix_file WHERE path = $1",
                &[Value::Text("/note.md".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(files.rows()[0].get::<String>("path").unwrap(), "/note.md");

        let blob_refs = session
            .execute(
                "SELECT COUNT(*) AS count FROM lix_state WHERE schema_key = $1",
                &[Value::Text("lix_binary_blob_ref".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(
            blob_refs.rows()[0].get::<i64>("count").unwrap(),
            0,
            "opening an ephemeral filesystem workspace should sync descriptors without importing stored file data"
        );

        let data = session
            .execute(
                "SELECT data FROM lix_file WHERE path = $1",
                &[Value::Text("/note.md".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(data.rows()[0].get::<Vec<u8>>("data").unwrap(), b"from disk");

        let blob_refs = session
            .execute(
                "SELECT COUNT(*) AS count FROM lix_state WHERE schema_key = $1",
                &[Value::Text("lix_binary_blob_ref".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(
            blob_refs.rows()[0].get::<i64>("count").unwrap(),
            0,
            "lazy reads should not import filesystem bytes into BCAS"
        );

        session
            .execute(
                "UPDATE lix_file SET data = $1 WHERE path = $2",
                &[
                    Value::Blob(b"from lix".to_vec()),
                    Value::Text("/note.md".to_string()),
                ],
            )
            .await
            .unwrap();

        assert_eq!(
            std::fs::read(tempdir.path().join("note.md")).unwrap(),
            b"from lix",
            "Lix writes should still materialize back to disk"
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn fs_backend_memory_opens_large_folder_descriptor_only_and_lazy_reads_one_file() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("root.txt"), b"root").unwrap();
        for index in 0..FILESYSTEM_PARALLEL_SNAPSHOT_MIN_DIRS {
            let nested = tempdir.path().join(format!("dir-{index}/nested"));
            std::fs::create_dir_all(&nested).unwrap();
            std::fs::write(
                tempdir.path().join(format!("dir-{index}/file.txt")),
                format!("file-{index}"),
            )
            .unwrap();
            std::fs::write(nested.join("deep.txt"), format!("deep-{index}")).unwrap();
        }

        let backend = FsBackend::open_memory(tempdir.path()).await.unwrap();
        let engine = Engine::new(backend).await.unwrap();
        let session = engine.open_workspace_session().await.unwrap();

        let file_count = session
            .execute("SELECT COUNT(*) AS count FROM lix_file", &[])
            .await
            .unwrap();
        let expected_file_count =
            i64::try_from(1 + (FILESYSTEM_PARALLEL_SNAPSHOT_MIN_DIRS * 2)).unwrap();
        assert_eq!(
            file_count.rows()[0].get::<i64>("count").unwrap(),
            expected_file_count
        );

        let blob_refs = session
            .execute(
                "SELECT COUNT(*) AS count FROM lix_state WHERE schema_key = $1",
                &[Value::Text("lix_binary_blob_ref".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(blob_refs.rows()[0].get::<i64>("count").unwrap(), 0);

        let data = session
            .execute(
                "SELECT data FROM lix_file WHERE path = $1",
                &[Value::Text("/dir-2/nested/deep.txt".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(data.rows()[0].get::<Vec<u8>>("data").unwrap(), b"deep-2");

        let blob_refs = session
            .execute(
                "SELECT COUNT(*) AS count FROM lix_state WHERE schema_key = $1",
                &[Value::Text("lix_binary_blob_ref".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(blob_refs.rows()[0].get::<i64>("count").unwrap(), 0);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn fs_backend_rejects_update_where_data_for_descriptor_only_files() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("note.md"), b"from disk").unwrap();

        let backend = FsBackend::open_memory(tempdir.path()).await.unwrap();
        let engine = Engine::new(backend).await.unwrap();
        let session = engine.open_workspace_session().await.unwrap();

        let error = session
            .execute(
                "UPDATE lix_file SET name = $1 WHERE data = $2",
                &[
                    Value::Text("renamed.md".to_string()),
                    Value::Blob(b"from disk".to_vec()),
                ],
            )
            .await
            .expect_err("UPDATE WHERE data should be rejected");
        assert!(
            error
                .message
                .contains("UPDATE lix_file WHERE data is not supported"),
            "{error:?}"
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn fs_backend_rejects_path_only_rename_for_descriptor_only_files() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("old.md"), b"from disk").unwrap();

        let backend = FsBackend::open_memory(tempdir.path()).await.unwrap();
        let engine = Engine::new(backend).await.unwrap();
        let session = engine.open_workspace_session().await.unwrap();

        let error = session
            .execute(
                "UPDATE lix_file SET path = $1 WHERE path = $2",
                &[
                    Value::Text("/new.md".to_string()),
                    Value::Text("/old.md".to_string()),
                ],
            )
            .await
            .expect_err("path-only rename should be rejected");
        assert!(
            error.message.contains(
                "UPDATE lix_file path without data is not supported for descriptor-only files"
            ),
            "{error:?}"
        );
        assert_eq!(
            std::fs::read(tempdir.path().join("old.md")).unwrap(),
            b"from disk"
        );
        assert!(!tempdir.path().join("new.md").exists());
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn fs_backend_persistent_opens_descriptor_only_and_lazy_reads_data() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("note.md"), b"from disk").unwrap();

        let backend = FsBackend::open(tempdir.path()).await.unwrap();
        let engine = Engine::new(backend).await.unwrap();
        let session = engine.open_workspace_session().await.unwrap();

        let blob_refs = session
            .execute(
                "SELECT COUNT(*) AS count FROM lix_state WHERE schema_key = $1",
                &[Value::Text("lix_binary_blob_ref".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(blob_refs.rows()[0].get::<i64>("count").unwrap(), 0);

        let data = session
            .execute(
                "SELECT data FROM lix_file WHERE path = $1",
                &[Value::Text("/note.md".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(data.rows()[0].get::<Vec<u8>>("data").unwrap(), b"from disk");
    }
}
