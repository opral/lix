use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;

use lix_engine::{
    Backend, Engine, FsMkdirOptions, FsRmOptions, FsWriteOptions, LixError, SessionContext,
};
use notify_debouncer_full::notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_full::{DebounceEventResult, Debouncer, RecommendedCache, new_debouncer};

type WorktreeDebouncer = Debouncer<RecommendedWatcher, RecommendedCache>;

#[derive(Clone)]
pub(crate) struct WorktreeSupervisor<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    inner: Arc<WorktreeSupervisorInner>,
    state: Arc<WorktreeState<B>>,
}

struct WorktreeSupervisorInner {
    event_tx: mpsc::Sender<WorktreeEvent>,
    debouncer: Mutex<Option<WorktreeDebouncer>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

struct WorktreeState<B>
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

enum WorktreeEvent {
    DiskChanged,
    Shutdown,
}

impl<B> WorktreeSupervisor<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    pub(crate) async fn open(engine: Engine<B>, root: &Path) -> Result<Self, LixError> {
        std::fs::create_dir_all(root)
            .map_err(|error| io_error("create worktree root", root, error))?;
        let root = std::fs::canonicalize(root)
            .map_err(|error| io_error("canonicalize worktree root", root, error))?;
        let session = engine.open_workspace_session().await?;
        let state = Arc::new(WorktreeState {
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
            .name("lix-sdk-worktree-sync".to_string())
            .spawn(move || worktree_worker(worker_state, event_rx))
            .map_err(|error| {
                LixError::new(
                    "LIX_WORKTREE_THREAD_ERROR",
                    format!("failed to start worktree sync worker: {error}"),
                )
            })?;

        let callback_tx = event_tx.clone();
        let mut debouncer = new_debouncer(
            Duration::from_millis(250),
            None,
            move |_result: DebounceEventResult| {
                let _ = callback_tx.send(WorktreeEvent::DiskChanged);
            },
        )
        .map_err(notify_error)?;
        debouncer
            .watch(state.root.as_path(), RecursiveMode::Recursive)
            .map_err(notify_error)?;

        Ok(Self {
            inner: Arc::new(WorktreeSupervisorInner {
                event_tx,
                debouncer: Mutex::new(Some(debouncer)),
                worker: Mutex::new(Some(worker)),
            }),
            state,
        })
    }

    pub(crate) async fn sync_from_lix(&self) -> Result<(), LixError> {
        self.state.sync_from_lix().await
    }

    pub(crate) async fn close(&self) -> Result<(), LixError> {
        self.inner.shutdown();
        self.state.session.close().await
    }
}

impl Drop for WorktreeSupervisorInner {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl WorktreeSupervisorInner {
    fn shutdown(&self) {
        if let Ok(mut debouncer) = self.debouncer.lock() {
            let _ = debouncer.take().map(WorktreeDebouncer::stop);
        }
        let _ = self.event_tx.send(WorktreeEvent::Shutdown);
        if let Ok(mut worker) = self.worker.lock() {
            if let Some(worker) = worker.take() {
                let _ = worker.join();
            }
        }
    }
}

impl<B> WorktreeState<B>
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
            if !local.files.contains_key(path) && !is_plugin_storage_path(path) {
                self.session.fs().rm(path, FsRmOptions::default()).await?;
            }
        }

        let mut directories_to_remove = lix
            .directories
            .difference(&local.directories)
            .filter(|path| path.as_str() != "/" && !is_plugin_storage_path(path))
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

        for (path, data) in &local.files {
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
            .map_err(|error| io_error("create worktree root", &self.root, error))?;
        let local = collect_local_snapshot(&self.root)?;
        let previous = self.last_materialized();

        for path in local.files.keys().filter(|path| {
            !target.files.contains_key(*path)
                && previous
                    .as_ref()
                    .is_none_or(|snapshot| snapshot.files.contains_key(*path))
        }) {
            let local_path = lix_path_to_local_path(&self.root, path)?;
            std::fs::remove_file(&local_path)
                .map_err(|error| io_error("remove worktree file", &local_path, error))?;
        }

        let mut directories_to_remove = local
            .directories
            .difference(&target.directories)
            .filter(|path| path.as_str() != "/")
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
                .map_err(|error| io_error("remove worktree directory", &local_path, error))?;
        }

        let mut directories_to_create = target
            .directories
            .iter()
            .filter(|path| path.as_str() != "/")
            .cloned()
            .collect::<Vec<_>>();
        sort_directories_shallowest_first(&mut directories_to_create);
        for path in directories_to_create {
            let local_path = lix_path_to_local_path(&self.root, &path)?;
            std::fs::create_dir_all(&local_path)
                .map_err(|error| io_error("create worktree directory", &local_path, error))?;
        }

        for (path, data) in &target.files {
            let local_path = lix_path_to_local_path(&self.root, path)?;
            if let Some(parent) = local_path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|error| io_error("create worktree file parent", parent, error))?;
            }
            if local.files.get(path) != Some(data) {
                std::fs::write(&local_path, data)
                    .map_err(|error| io_error("write worktree file", &local_path, error))?;
            }
        }

        Ok(())
    }

    fn remember_materialized(&self, snapshot: Snapshot) {
        *self
            .last_materialized
            .lock()
            .expect("worktree materialized snapshot lock should not poison") = Some(snapshot);
    }

    fn last_materialized(&self) -> Option<Snapshot> {
        self.last_materialized
            .lock()
            .expect("worktree materialized snapshot lock should not poison")
            .clone()
    }

    fn is_last_materialized(&self, snapshot: &Snapshot) -> bool {
        self.last_materialized
            .lock()
            .expect("worktree materialized snapshot lock should not poison")
            .as_ref()
            == Some(snapshot)
    }
}

fn worktree_worker<B>(state: Arc<WorktreeState<B>>, event_rx: mpsc::Receiver<WorktreeEvent>)
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread().build() else {
        return;
    };
    while let Ok(WorktreeEvent::DiskChanged) | Err(mpsc::RecvTimeoutError::Timeout) =
        event_rx.recv_timeout(Duration::from_secs(1))
    {
        loop {
            match event_rx.try_recv() {
                Ok(WorktreeEvent::DiskChanged) => {}
                Err(mpsc::TryRecvError::Empty) => break,
                Ok(WorktreeEvent::Shutdown) | Err(mpsc::TryRecvError::Disconnected) => return,
            }
        }
        let _ = runtime.block_on(state.sync_disk_to_lix(true));
    }
}

fn collect_local_snapshot(root: &Path) -> Result<Snapshot, LixError> {
    let metadata = std::fs::symlink_metadata(root)
        .map_err(|error| io_error("read worktree root metadata", root, error))?;
    if metadata.file_type().is_symlink() {
        let root = root.display();
        return Err(worktree_error(format!(
            "worktree root {root} must not be a symlink"
        )));
    }
    if !metadata.is_dir() {
        let root = root.display();
        return Err(worktree_error(format!(
            "worktree root {root} must be a directory"
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
        .map_err(|error| io_error("read worktree directory", directory, error))?;
    for entry in entries {
        let entry =
            entry.map_err(|error| io_error("read worktree directory entry", directory, error))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("read worktree entry type", &path, error))?;
        if file_type.is_symlink() {
            let path = path.display();
            return Err(worktree_error(format!(
                "worktree path {path} must not be a symlink"
            )));
        }
        if file_type.is_dir() {
            snapshot
                .directories
                .insert(local_path_to_lix_path(root, &path, true)?);
            collect_local_directory(root, &path, snapshot)?;
        } else if file_type.is_file() {
            let data = std::fs::read(&path)
                .map_err(|error| io_error("read worktree file", &path, error))?;
            snapshot
                .files
                .insert(local_path_to_lix_path(root, &path, false)?, data);
        } else {
            let path = path.display();
            return Err(worktree_error(format!(
                "worktree path {path} is not a regular file or directory"
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
        worktree_error(format!(
            "worktree path {path} is not inside root {root}: {error}"
        ))
    })?;
    let mut segments = Vec::new();
    for component in relative.components() {
        let Component::Normal(segment) = component else {
            let path = path.display();
            return Err(worktree_error(format!(
                "worktree path {path} contains an unsupported path component"
            )));
        };
        let segment = segment.to_str().ok_or_else(|| {
            let path = path.display();
            worktree_error(format!("worktree path {path} is not valid UTF-8"))
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
        .ok_or_else(|| worktree_error(format!("Lix path {path:?} is not absolute")))?;
    let body = body.strip_suffix('/').unwrap_or(body);
    if body.is_empty() {
        return Ok(root.to_path_buf());
    }
    let mut local = root.to_path_buf();
    for segment in body.split('/') {
        if segment.is_empty() {
            return Err(worktree_error(format!(
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
        return Err(worktree_error(format!(
            "worktree path {path} contains invalid Lix path segment {segment:?}"
        )));
    }
    Ok(())
}

fn is_plugin_storage_path(path: &str) -> bool {
    path == "/.lix/plugins" || path.starts_with("/.lix/plugins/")
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
        "LIX_WORKTREE_IO_ERROR",
        format!("{operation} {path}: {error}"),
    )
}

fn notify_error(error: notify_debouncer_full::notify::Error) -> LixError {
    LixError::new(
        "LIX_WORKTREE_NOTIFY_ERROR",
        format!("worktree watcher error: {error}"),
    )
}

fn worktree_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_WORKTREE_ERROR", message)
}
