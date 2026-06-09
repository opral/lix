use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;

use lix_engine::wasm::WasmRuntime;
use lix_engine::{
    Backend, BackendError, BackendWrite, CommitResult, Engine, FsMkdirOptions, FsRmOptions,
    FsWriteOptions, LixError, PutBatch, ReadOptions, SessionContext, WriteOptions,
};
use notify_debouncer_full::notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_full::{DebounceEventResult, Debouncer, RecommendedCache, new_debouncer};

#[cfg(feature = "sqlite")]
use crate::sqlite_backend::SqliteBackend;

type FilesystemDebouncer = Debouncer<RecommendedWatcher, RecommendedCache>;

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
    inner: FilesystemSync<SqliteBackend>,
}

#[cfg(feature = "sqlite")]
#[expect(missing_debug_implementations)]
pub struct FsWrite<'a> {
    inner: FilesystemWrite<'a, SqliteBackend>,
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
    sync_lock: tokio::sync::Mutex<()>,
    last_materialized: Mutex<Option<Snapshot>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Snapshot {
    directories: BTreeSet<String>,
    files: BTreeMap<String, Vec<u8>>,
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
        let backend = open_filesystem_sqlite_backend(dir.as_ref())?;
        let inner = FilesystemSync::open(backend, dir.as_ref()).await?;
        Ok(Self { inner })
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
        Ok(Self { inner })
    }
}

#[cfg(feature = "sqlite")]
impl Backend for FsBackend {
    type Read<'a>
        = crate::sqlite_backend::SqliteRead
    where
        Self: 'a;

    type Write<'a>
        = FsWrite<'a>
    where
        Self: 'a;

    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        self.inner.begin_read(opts)
    }

    fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(FsWrite {
            inner: self.inner.begin_write(opts)?,
        })
    }
}

#[cfg(feature = "sqlite")]
impl BackendWrite for FsWrite<'_> {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        self.inner.put_many(entries)
    }

    fn delete_many(&mut self, keys: &[lix_engine::Key]) -> Result<(), BackendError> {
        self.inner.delete_many(keys)
    }

    fn delete_range(&mut self, range: lix_engine::KeyRange) -> Result<(), BackendError> {
        self.inner.delete_range(range)
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        self.inner.commit()
    }

    fn rollback(self) -> Result<(), BackendError> {
        self.inner.rollback()
    }
}

impl<B> FilesystemSync<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    pub async fn open<P>(backend: B, root: P) -> Result<Self, LixError>
    where
        P: AsRef<Path>,
    {
        let engine = crate::lix::open_or_initialize_engine(backend.clone(), None).await?;
        Self::open_with_engine(backend, engine, root.as_ref()).await
    }

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
        Self::open_with_engine(backend, engine, root.as_ref()).await
    }

    async fn open_with_engine(
        backend: B,
        engine: Engine<B>,
        root: &Path,
    ) -> Result<Self, LixError> {
        Ok(Self {
            inner: backend,
            supervisor: FilesystemSupervisor::open(engine, root).await?,
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
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        self.inner.put_many(entries)
    }

    fn delete_many(&mut self, keys: &[lix_engine::Key]) -> Result<(), BackendError> {
        self.inner.delete_many(keys)
    }

    fn delete_range(&mut self, range: lix_engine::KeyRange) -> Result<(), BackendError> {
        self.inner.delete_range(range)
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
    async fn open(engine: Engine<B>, root: &Path) -> Result<Self, LixError> {
        std::fs::create_dir_all(root)
            .map_err(|error| io_error("create filesystem root", root, error))?;
        let root = std::fs::canonicalize(root)
            .map_err(|error| io_error("canonicalize filesystem root", root, error))?;
        let session = engine.open_workspace_session().await?;
        let state = Arc::new(FilesystemState {
            session,
            root,
            sync_lock: tokio::sync::Mutex::new(()),
            last_materialized: Mutex::new(None),
        });

        state.sync_disk_to_lix(false).await?;
        state.sync_from_lix().await?;

        let (event_tx, event_rx) = mpsc::channel();
        let worker_state = Arc::clone(&state);
        let worker = std::thread::Builder::new()
            .name("lix-sdk-filesystem-sync".to_string())
            .spawn(move || filesystem_worker(worker_state, event_rx))
            .map_err(|error| {
                LixError::new(
                    "LIX_FILESYSTEM_THREAD_ERROR",
                    format!("failed to start filesystem sync worker: {error}"),
                )
            })?;

        let callback_tx = event_tx.clone();
        let mut debouncer = new_debouncer(
            Duration::from_millis(250),
            None,
            move |_result: DebounceEventResult| {
                let _ = callback_tx.send(FilesystemEvent::DiskChanged);
            },
        )
        .map_err(notify_error)?;
        debouncer
            .watch(state.root.as_path(), RecursiveMode::Recursive)
            .map_err(notify_error)?;

        Ok(Self {
            inner: Arc::new(FilesystemSupervisorInner {
                event_tx,
                debouncer: Mutex::new(Some(debouncer)),
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

impl<B> FilesystemState<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    async fn sync_from_lix(&self) -> Result<(), LixError> {
        let _guard = self.sync_lock.lock().await;
        let snapshot = self.collect_lix_snapshot().await?;
        self.materialize_snapshot(&snapshot)?;
        self.remember_materialized(snapshot);
        Ok(())
    }

    async fn sync_disk_to_lix(&self, skip_if_last_materialized: bool) -> Result<(), LixError> {
        let _guard = self.sync_lock.lock().await;
        let local = collect_local_snapshot(&self.root)?;
        if skip_if_last_materialized && self.is_last_materialized(&local) {
            return Ok(());
        }
        self.apply_local_snapshot_to_lix(&local).await?;
        let lix = self.collect_lix_snapshot().await?;
        self.materialize_snapshot(&lix)?;
        self.remember_materialized(lix);
        Ok(())
    }

    async fn close(&self) -> Result<(), LixError> {
        self.session.close().await
    }

    async fn collect_lix_snapshot(&self) -> Result<Snapshot, LixError> {
        let mut snapshot = Snapshot::default();
        let directories = self
            .session
            .execute("SELECT path FROM lix_directory ORDER BY path", &[])
            .await?;
        for row in directories.rows() {
            snapshot.directories.insert(row.get::<String>("path")?);
        }
        let files = self
            .session
            .execute("SELECT path FROM lix_file ORDER BY path", &[])
            .await?;
        for row in files.rows() {
            let path = row.get::<String>("path")?;
            let data = self
                .session
                .fs()
                .read_file(&path)
                .await?
                .unwrap_or_default();
            snapshot.files.insert(path, data);
        }
        Ok(snapshot)
    }

    async fn apply_local_snapshot_to_lix(&self, local: &Snapshot) -> Result<(), LixError> {
        let lix = self.collect_lix_snapshot().await?;

        for path in lix.files.keys() {
            if !local.files.contains_key(path)
                && !is_plugin_storage_path(path)
                && !is_filesystem_metadata_path(path)
            {
                self.session.fs().rm(path, FsRmOptions::default()).await?;
            }
        }

        let mut directories_to_remove = lix
            .directories
            .difference(&local.directories)
            .filter(|path| {
                path.as_str() != "/"
                    && !is_plugin_storage_path(path)
                    && !is_filesystem_metadata_path(path)
            })
            .cloned()
            .collect::<Vec<_>>();
        sort_directories_deepest_first(&mut directories_to_remove);
        for path in directories_to_remove {
            self.session
                .fs()
                .rm(
                    &path,
                    FsRmOptions {
                        recursive: true,
                        ..FsRmOptions::default()
                    },
                )
                .await?;
        }

        let mut directories_to_create = local
            .directories
            .difference(&lix.directories)
            .filter(|path| path.as_str() != "/")
            .cloned()
            .collect::<Vec<_>>();
        sort_directories_shallowest_first(&mut directories_to_create);
        for path in directories_to_create {
            self.session
                .fs()
                .mkdir(&path, FsMkdirOptions::default())
                .await?;
        }

        for (path, data) in local
            .files
            .iter()
            .filter(|(path, _)| !is_filesystem_metadata_path(path))
        {
            if is_plugin_storage_path(path) {
                self.session.install_plugin_archive(data).await?;
            } else if lix.files.get(path) != Some(data) {
                self.session
                    .fs()
                    .write_file(path, data.clone(), FsWriteOptions::default())
                    .await?;
            }
        }

        Ok(())
    }

    fn materialize_snapshot(&self, target: &Snapshot) -> Result<(), LixError> {
        std::fs::create_dir_all(&self.root)
            .map_err(|error| io_error("create filesystem root", &self.root, error))?;
        let local = collect_local_snapshot(&self.root)?;
        let previous = self.last_materialized();

        for path in local.files.keys().filter(|path| {
            !target.files.contains_key(*path)
                && !is_filesystem_metadata_path(path)
                && previous
                    .as_ref()
                    .is_none_or(|snapshot| snapshot.files.contains_key(*path))
        }) {
            let local_path = lix_path_to_local_path(&self.root, path)?;
            std::fs::remove_file(&local_path)
                .map_err(|error| io_error("remove filesystem file", &local_path, error))?;
        }

        let mut directories_to_remove = local
            .directories
            .difference(&target.directories)
            .filter(|path| path.as_str() != "/" && !is_filesystem_metadata_path(path))
            .filter(|path| {
                previous
                    .as_ref()
                    .is_none_or(|snapshot| snapshot.directories.contains(*path))
            })
            .cloned()
            .collect::<Vec<_>>();
        sort_directories_deepest_first(&mut directories_to_remove);
        for path in directories_to_remove {
            let local_path = lix_path_to_local_path(&self.root, &path)?;
            std::fs::remove_dir(&local_path)
                .map_err(|error| io_error("remove filesystem directory", &local_path, error))?;
        }

        let mut directories_to_create = target
            .directories
            .iter()
            .filter(|path| path.as_str() != "/" && !is_filesystem_metadata_path(path))
            .cloned()
            .collect::<Vec<_>>();
        sort_directories_shallowest_first(&mut directories_to_create);
        for path in directories_to_create {
            let local_path = lix_path_to_local_path(&self.root, &path)?;
            std::fs::create_dir_all(&local_path)
                .map_err(|error| io_error("create filesystem directory", &local_path, error))?;
        }

        for (path, data) in target
            .files
            .iter()
            .filter(|(path, _)| !is_filesystem_metadata_path(path))
        {
            let local_path = lix_path_to_local_path(&self.root, path)?;
            if let Some(parent) = local_path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|error| io_error("create filesystem file parent", parent, error))?;
            }
            if local.files.get(path) != Some(data) {
                std::fs::write(&local_path, data)
                    .map_err(|error| io_error("write filesystem file", &local_path, error))?;
            }
        }

        Ok(())
    }

    fn remember_materialized(&self, snapshot: Snapshot) {
        *self
            .last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison") = Some(snapshot);
    }

    fn last_materialized(&self) -> Option<Snapshot> {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .clone()
    }

    fn is_last_materialized(&self, snapshot: &Snapshot) -> bool {
        self.last_materialized
            .lock()
            .expect("filesystem materialized snapshot lock should not poison")
            .as_ref()
            == Some(snapshot)
    }
}

fn filesystem_worker<B>(state: Arc<FilesystemState<B>>, event_rx: mpsc::Receiver<FilesystemEvent>)
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread().build() else {
        return;
    };
    loop {
        match event_rx.recv_timeout(Duration::from_secs(1)) {
            Ok(FilesystemEvent::DiskChanged) => {
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
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = runtime.block_on(state.sync_disk_to_lix(true));
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

fn collect_local_snapshot(root: &Path) -> Result<Snapshot, LixError> {
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

    let mut snapshot = Snapshot::default();
    snapshot.directories.insert("/".to_string());
    collect_local_directory(root, root, &mut snapshot)?;
    Ok(snapshot)
}

fn collect_local_directory(
    root: &Path,
    directory: &Path,
    snapshot: &mut Snapshot,
) -> Result<(), LixError> {
    let entries = std::fs::read_dir(directory)
        .map_err(|error| io_error("read filesystem directory", directory, error))?;
    for entry in entries {
        let entry =
            entry.map_err(|error| io_error("read filesystem directory entry", directory, error))?;
        if directory == root && is_filesystem_metadata_file_name(&entry.file_name()) {
            continue;
        }
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("read filesystem entry type", &path, error))?;
        if file_type.is_symlink() {
            let path = path.display();
            return Err(filesystem_error(format!(
                "filesystem path {path} must not be a symlink"
            )));
        }
        if file_type.is_dir() {
            snapshot
                .directories
                .insert(local_path_to_lix_path(root, &path, true)?);
            collect_local_directory(root, &path, snapshot)?;
        } else if file_type.is_file() {
            let data = std::fs::read(&path)
                .map_err(|error| io_error("read filesystem file", &path, error))?;
            snapshot
                .files
                .insert(local_path_to_lix_path(root, &path, false)?, data);
        } else {
            let path = path.display();
            return Err(filesystem_error(format!(
                "filesystem path {path} is not a regular file or directory"
            )));
        }
    }
    Ok(())
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
        validate_lix_path_segment(segment, path)?;
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
        if segment.is_empty() {
            return Err(filesystem_error(format!(
                "Lix path {path:?} contains an empty segment"
            )));
        }
        local.push(segment);
    }
    Ok(local)
}

fn validate_lix_path_segment(segment: &str, path: &Path) -> Result<(), LixError> {
    if segment.is_empty()
        || segment == "."
        || segment == ".."
        || segment.contains('/')
        || segment.contains('\\')
        || segment.contains('\0')
        || segment.contains('%')
        || segment.contains('?')
        || segment.contains('#')
    {
        let path = path.display();
        return Err(filesystem_error(format!(
            "filesystem path {path} contains invalid Lix path segment {segment:?}"
        )));
    }
    Ok(())
}

fn is_plugin_storage_path(path: &str) -> bool {
    path == "/.lix_system/plugins" || path.starts_with("/.lix_system/plugins/")
}

fn is_filesystem_metadata_path(path: &str) -> bool {
    matches!(
        path.trim_end_matches('/'),
        "/.lix" | "/.lix-wal" | "/.lix-shm" | "/.lix-journal"
    )
}

fn is_filesystem_metadata_file_name(name: &std::ffi::OsStr) -> bool {
    matches!(
        name.to_str(),
        Some(".lix" | ".lix-wal" | ".lix-shm" | ".lix-journal")
    )
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
    path.trim_matches('/')
        .split('/')
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

fn notify_error(error: notify_debouncer_full::notify::Error) -> LixError {
    LixError::new(
        "LIX_FILESYSTEM_NOTIFY_ERROR",
        format!("filesystem watcher error: {error}"),
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
    std::fs::create_dir_all(dir).map_err(|error| io_error("create filesystem root", dir, error))?;
    SqliteBackend::open(dir.join(".lix")).map_err(sqlite_backend_error)
}

#[cfg(feature = "sqlite")]
fn sqlite_backend_error(error: BackendError) -> LixError {
    LixError::new(
        LixError::CODE_STORAGE_ERROR,
        format!("failed to open filesystem SQLite backend: {error}"),
    )
}
