use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;

use lix_engine::wasm::WasmRuntime;
use lix_engine::{
    Backend, BackendError, BackendWrite, CommitResult, Engine, LixError, PutBatch, ReadOptions,
    SessionContext, SpaceId, Value, WriteOptions,
};
use notify_debouncer_full::notify::{Config, RecommendedWatcher, RecursiveMode};
use notify_debouncer_full::{DebounceEventResult, Debouncer, RecommendedCache, new_debouncer_opt};

#[cfg(feature = "sqlite")]
use crate::sqlite_backend::SqliteBackend;

type FilesystemDebouncer = Debouncer<RecommendedWatcher, RecommendedCache>;
const HOST_METADATA_GITIGNORE: &[u8] = b"*\n";
const FILESYSTEM_POLL_INTERVAL: Duration = Duration::from_secs(15);

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
    last_materialized: Mutex<Option<MaterializedSnapshot>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Snapshot {
    directories: BTreeSet<String>,
    files: BTreeMap<String, Vec<u8>>,
    unmanaged_paths: BTreeSet<String>,
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
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        self.inner.put_many(space, entries)
    }

    fn delete_many(
        &mut self,
        space: SpaceId,
        keys: &[lix_engine::Key],
    ) -> Result<(), BackendError> {
        self.inner.delete_many(space, keys)
    }

    fn delete_range(
        &mut self,
        space: SpaceId,
        range: lix_engine::KeyRange,
    ) -> Result<(), BackendError> {
        self.inner.delete_range(space, range)
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
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        self.inner.put_many(space, entries)
    }

    fn delete_many(
        &mut self,
        space: SpaceId,
        keys: &[lix_engine::Key],
    ) -> Result<(), BackendError> {
        self.inner.delete_many(space, keys)
    }

    fn delete_range(
        &mut self,
        space: SpaceId,
        range: lix_engine::KeyRange,
    ) -> Result<(), BackendError> {
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
    async fn open(engine: Engine<B>, root: &Path) -> Result<Self, LixError> {
        ensure_filesystem_root_directory(root)?;
        let root = std::fs::canonicalize(root)
            .map_err(|error| io_error("canonicalize filesystem root", root, error))?;
        ensure_filesystem_system_directory(&root)?;
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
            if debouncer
                .watch(state.root.as_path(), RecursiveMode::Recursive)
                .is_ok()
            {
                Some(debouncer)
            } else {
                debouncer.stop();
                None
            }
        });
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

impl<B> FilesystemState<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    async fn sync_from_lix(&self) -> Result<(), LixError> {
        let _guard = self.sync_lock.lock().await;
        let lix = self.collect_lix_snapshot().await?;
        let disk = self.materialize_snapshot(&lix)?;
        let lix_revision = self.collect_lix_revision().await?;
        self.remember_materialized(disk, lix_revision);
        Ok(())
    }

    async fn sync_disk_to_lix(&self, skip_if_last_materialized: bool) -> Result<(), LixError> {
        let _guard = self.sync_lock.lock().await;
        let local = collect_local_snapshot(&self.root)?;
        if skip_if_last_materialized && self.is_last_materialized_disk(&local) {
            let lix_revision = self.collect_lix_revision().await?;
            if self.is_last_materialized(&local, &lix_revision) {
                return Ok(());
            }
        }
        let previous = self.last_materialized_disk();
        self.apply_local_snapshot_to_lix(&local, previous.as_ref())
            .await?;
        let lix = self.collect_lix_snapshot().await?;
        let materialized = self.materialize_snapshot_after_disk_sync(&lix, &local)?;
        let lix_revision = self.collect_lix_revision().await?;
        self.remember_materialized(materialized, lix_revision);
        Ok(())
    }

    async fn close(&self) -> Result<(), LixError> {
        self.session.close().await
    }

    async fn collect_lix_snapshot(&self) -> Result<Snapshot, LixError> {
        let mut snapshot = Snapshot::default();
        snapshot.directories.insert("/".to_string());
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
            let data = lix_read_file(&self.session, &path)
                .await?
                .unwrap_or_default();
            snapshot.files.insert(path, data);
        }
        Ok(snapshot)
    }

    async fn collect_lix_revision(&self) -> Result<LixRevision, LixError> {
        let active_branch_id = self.session.active_branch_id().await?;
        let rows = self
            .session
            .execute(
                "SELECT lix_active_branch_commit_id() AS active_branch_commit_id",
                &[],
            )
            .await?;
        let active_branch_commit_id = rows
            .rows()
            .first()
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "lix_active_branch_commit_id() returned no rows",
                )
            })?
            .get::<String>("active_branch_commit_id")?;

        Ok(LixRevision {
            active_branch_id,
            active_branch_commit_id,
        })
    }

    async fn apply_local_snapshot_to_lix(
        &self,
        local: &Snapshot,
        previous: Option<&Snapshot>,
    ) -> Result<(), LixError> {
        let lix = self.collect_lix_snapshot().await?;

        for path in lix.files.keys() {
            if !local.files.contains_key(path)
                && !is_plugin_storage_path(path)
                && !is_filesystem_metadata_path(path)
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
                lix_remove_file(&self.session, path).await?;
            }
        }

        let mut directories_to_remove = Vec::new();
        for path in lix.directories.difference(&local.directories) {
            if path.as_str() == "/"
                || is_plugin_storage_path(path)
                || is_filesystem_metadata_path(path)
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
            lix_remove_directory_recursive(&self.session, &path).await?;
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
            lix_make_directory(&self.session, &path).await?;
        }

        for (path, data) in local
            .files
            .iter()
            .filter(|(path, _)| !is_filesystem_metadata_path(path))
        {
            if previous
                .as_ref()
                .is_some_and(|snapshot| snapshot.files.get(path) == Some(data))
            {
                continue;
            }
            if lix.files.get(path) != Some(data) {
                lix_write_file(&self.session, path, data.clone()).await?;
            }
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
        let local = collect_local_snapshot(&self.root)?;
        let previous = self.last_materialized_disk();

        for path in local.files.keys().filter(|path| {
            !target.files.contains_key(*path)
                && !is_filesystem_metadata_path(path)
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
            remove_materialized_file(&self.root, path)?;
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
            .filter(|path| {
                base.is_none_or(|snapshot| {
                    snapshot.directories.contains(*path)
                        && local.directories.contains(*path) == snapshot.directories.contains(*path)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        sort_directories_deepest_first(&mut directories_to_remove);
        for path in directories_to_remove {
            remove_materialized_directory(&self.root, &path)?;
        }

        let mut directories_to_create = target
            .directories
            .iter()
            .filter(|path| path.as_str() != "/" && !is_filesystem_metadata_path(path))
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
            create_materialized_directory(&self.root, &path)?;
        }

        for (path, data) in target
            .files
            .iter()
            .filter(|(path, _)| !is_filesystem_metadata_path(path))
        {
            if base.is_some_and(|snapshot| snapshot.files.get(path) == Some(data)) {
                continue;
            }
            if base.is_some_and(|snapshot| snapshot.files.get(path) != local.files.get(path)) {
                continue;
            }
            if local.files.get(path) != Some(data) {
                write_materialized_file(&self.root, path, data)?;
            }
        }

        let materialized = collect_local_snapshot(&self.root)?;
        let mut remembered = target.clone();
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

async fn lix_make_directory<B>(session: &SessionContext<B>, path: &str) -> Result<(), LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    session
        .execute(
            "INSERT INTO lix_directory (path) VALUES ($1) ON CONFLICT (path) DO NOTHING",
            &[Value::Text(path.to_string())],
        )
        .await?;
    Ok(())
}

async fn lix_remove_file<B>(session: &SessionContext<B>, path: &str) -> Result<(), LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    session
        .execute(
            "DELETE FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    Ok(())
}

async fn lix_remove_directory_recursive<B>(
    session: &SessionContext<B>,
    path: &str,
) -> Result<(), LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    session
        .execute(
            "DELETE FROM lix_directory WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    Ok(())
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

fn collect_local_snapshot(root: &Path) -> Result<Snapshot, LixError> {
    validate_filesystem_root_directory(root)?;

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
        if is_filesystem_sync_ignored_local_path(root, &path) {
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
            collect_local_directory(root, &path, snapshot)?;
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

fn ensure_filesystem_system_directory(root: &Path) -> Result<PathBuf, LixError> {
    let system_dir = root.join(".lix_system");
    match std::fs::create_dir(&system_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(io_error(
                "create filesystem system directory",
                &system_dir,
                error,
            ));
        }
    }

    let metadata = std::fs::symlink_metadata(&system_dir)
        .map_err(|error| io_error("read filesystem system directory", &system_dir, error))?;
    if metadata.file_type().is_symlink() {
        let path = system_dir.display();
        return Err(filesystem_error(format!(
            "filesystem system path {path} must not be a symlink"
        )));
    }
    if !metadata.is_dir() {
        let path = system_dir.display();
        return Err(filesystem_error(format!(
            "filesystem system path {path} must be a directory"
        )));
    }

    ensure_metadata_gitignore(&system_dir)?;
    Ok(system_dir)
}

fn remove_materialized_file(root: &Path, path: &str) -> Result<(), LixError> {
    if is_filesystem_sync_ignored_lix_path(path) {
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

fn remove_materialized_directory(root: &Path, path: &str) -> Result<(), LixError> {
    if is_filesystem_sync_ignored_lix_path(path) {
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

fn create_materialized_directory(root: &Path, path: &str) -> Result<(), LixError> {
    if is_filesystem_sync_ignored_lix_path(path) {
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

fn write_materialized_file(root: &Path, path: &str, data: &[u8]) -> Result<(), LixError> {
    if is_filesystem_sync_ignored_lix_path(path) {
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
    path == "/.lix_system/plugins" || path.starts_with("/.lix_system/plugins/")
}

fn is_filesystem_metadata_path(path: &str) -> bool {
    path == "/.lix" || path.starts_with("/.lix/")
}

fn is_filesystem_metadata_file_name(name: &std::ffi::OsStr) -> bool {
    matches!(name.to_str(), Some(".lix"))
}

fn is_filesystem_sync_ignored_local_path(root: &Path, path: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return true;
    };
    let mut depth = 0usize;
    let mut first_segment_is_lix_system = false;
    for component in relative.components() {
        let Component::Normal(segment) = component else {
            return true;
        };
        depth += 1;
        let segment = segment.to_str();
        if segment == Some(".git") {
            return true;
        }
        if depth == 1 && segment == Some(".lix_system") {
            first_segment_is_lix_system = true;
        }
        if depth == 2 && first_segment_is_lix_system && segment == Some(".gitignore") {
            return true;
        }
    }
    false
}

fn is_filesystem_sync_ignored_lix_path(path: &str) -> bool {
    lix_path_contains_segment(path, ".git") || path == "/.lix_system/.gitignore"
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
    let metadata_dir = dir.join(".lix");
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

    ensure_metadata_gitignore(&metadata_dir)?;
    Ok(metadata_dir)
}

fn ensure_metadata_gitignore(directory: &Path) -> Result<(), LixError> {
    let gitignore = directory.join(".gitignore");
    match std::fs::read(&gitignore) {
        Ok(existing) if existing == HOST_METADATA_GITIGNORE => return Ok(()),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(io_error(
                "read filesystem metadata .gitignore",
                &gitignore,
                error,
            ));
        }
    }
    std::fs::write(&gitignore, HOST_METADATA_GITIGNORE)
        .map_err(|error| io_error("write filesystem metadata .gitignore", &gitignore, error))
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
            sync_lock: tokio::sync::Mutex::new(()),
            last_materialized: Mutex::new(None),
        };

        state.sync_disk_to_lix(false).await.unwrap();

        let local = collect_local_snapshot(&state.root).unwrap();
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
            sync_lock: tokio::sync::Mutex::new(()),
            last_materialized: Mutex::new(None),
        };

        state.sync_disk_to_lix(false).await.unwrap();
        let disk_path = tempdir.path().join("disk.txt");
        std::fs::write(&disk_path, b"disk").unwrap();
        let local = collect_local_snapshot(&state.root).unwrap();
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

        let target = state.collect_lix_snapshot().await.unwrap();
        let materialized = state
            .materialize_snapshot_after_disk_sync(&target, &local)
            .unwrap();
        let target_revision = state.collect_lix_revision().await.unwrap();
        state.remember_materialized(materialized, target_revision);
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
}
