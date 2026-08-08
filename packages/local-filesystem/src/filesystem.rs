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

use lix::integration::{Engine, SessionContext};
use lix::storage::{
    CommitResult, Key, KeyRange, PutBatch, ReadOptions, Storage, StorageError, StorageSpace,
    StorageWrite, WriteOptions,
};
use lix::{LixError, LixPath, Value};
use notify_debouncer_full::notify::{Config, RecommendedWatcher, RecursiveMode};
use notify_debouncer_full::{DebounceEventResult, Debouncer, RecommendedCache, new_debouncer_opt};
use tokio::sync::oneshot;

use crate::RocksDBFilesystem;

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

    fn local_path_to_lix_path(&self, path: &Path) -> Result<String, LixError> {
        if path.starts_with(&self.lix_dir) {
            let path = local_path_to_lix_path(&self.lix_dir, path)?;
            if path == "/" {
                return Ok("/.lix".to_string());
            }
            return Ok(format!("/.lix{path}"));
        }
        local_path_to_lix_path(&self.root, path)
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
}

pub(crate) struct FilesystemWrite<'a, StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    inner: StorageImpl::Write<'a>,
    supervisor: FilesystemSupervisor<StorageImpl>,
}

#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct LocalFilesystem {
    inner: FilesystemSync<RocksDBFilesystem>,
}

pub type LocalFilesystemRead<'a> = crate::RocksDBFilesystemRead<'a>;

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
    event_tx: Mutex<Option<mpsc::Sender<FilesystemEvent>>>,
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
        reply_tx: oneshot::Sender<Result<(), LixError>>,
    },
    Shutdown,
}

impl LocalFilesystem {
    pub async fn open<P>(path: P) -> Result<Self, LixError>
    where
        P: AsRef<Path>,
    {
        let layout = prepare_filesystem_layout(path.as_ref())?;
        let storage = open_filesystem_rocksdb(&layout)?;
        let engine = lix::storage::open_engine(storage.clone(), None).await?;
        let inner = FilesystemSync::open_with_engine(storage, engine, layout).await?;
        Ok(Self { inner })
    }
}

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

impl StorageWrite for LocalFilesystemWrite<'_> {
    fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.put_many(space, entries)
    }

    fn delete_many(
        &mut self,
        space: StorageSpace,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.delete_many(space, keys)
    }

    fn delete_range(
        &mut self,
        space: StorageSpace,
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
    ) -> Result<Self, LixError> {
        Ok(Self {
            inner: storage,
            supervisor: FilesystemSupervisor::open(engine, layout).await?,
        })
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
        space: StorageSpace,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.put_many(space, entries)
    }

    fn delete_many(
        &mut self,
        space: StorageSpace,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.delete_many(space, keys)
    }

    fn delete_range(
        &mut self,
        space: StorageSpace,
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
    async fn open(engine: Engine<StorageImpl>, layout: FilesystemLayout) -> Result<Self, LixError> {
        validate_filesystem_root_directory(&layout.root)?;
        validate_filesystem_lix_directory(&layout.lix_dir)?;
        let session = engine.open_workspace_session().await?;
        let state = Arc::new(FilesystemState {
            session,
            layout,
            sync_lock: tokio::sync::Mutex::new(()),
            last_materialized: Mutex::new(None),
        });

        state.ingest_workspace(false).await?;
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
            let mut watcher = FilesystemWatcher {
                debouncer,
                watched_paths: Vec::new(),
            };
            if watcher.refresh(&state.layout).is_ok() {
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
                event_tx: Mutex::new(Some(event_tx)),
                worker: Mutex::new(Some(worker)),
            }),
            _marker: PhantomData,
        })
    }

    async fn sync_from_lix(&self) -> Result<(), StorageError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let event_tx = self
                .inner
                .event_tx
                .lock()
                .map_err(|_| StorageError::Io("filesystem sync owner lock poisoned".into()))?;
            let event_tx = event_tx
                .as_ref()
                .ok_or_else(|| StorageError::Io("filesystem sync owner is shutting down".into()))?;
            event_tx
                .send(FilesystemEvent::SyncFromLix { reply_tx })
                .map_err(|error| {
                    StorageError::Io(format!(
                        "filesystem sync failed: filesystem worker stopped: {error}"
                    ))
                })?;
        }
        match reply_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(filesystem_sync_storage_error(error)),
            Err(error) => Err(StorageError::Io(format!(
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
        if let Ok(mut event_tx) = self.event_tx.lock()
            && let Some(event_tx) = event_tx.take()
        {
            // Taking the sole synchronization sender closes admission first.
            // FIFO delivery then drains every Lix-to-disk request accepted
            // before this shutdown marker. Watcher-only disk notifications may
            // arrive later and are intentionally outside the close guarantee.
            let _ = event_tx.send(FilesystemEvent::Shutdown);
        }
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
    async fn sync_from_lix(&self) -> Result<(), LixError> {
        let _guard = self.sync_lock.lock().await;
        let lix_revision = self.collect_lix_revision().await?;
        if self.is_last_materialized_lix_revision(&lix_revision) {
            let local = collect_local_snapshot(&self.layout)?;
            if self.is_last_materialized_disk(&local) {
                return Ok(());
            }
        }
        let lix = self.collect_lix_snapshot_read().await?;
        let disk = self.materialize_snapshot(&lix.snapshot, None)?;
        self.remember_materialized(disk, lix.revision);
        Ok(())
    }

    async fn ingest_workspace(&self, skip_if_last_materialized: bool) -> Result<(), LixError> {
        let _guard = self.sync_lock.lock().await;
        let local = collect_local_snapshot(&self.layout)?;
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
        let materialized = self.materialize_snapshot(&lix.snapshot, Some(&local))?;
        self.remember_materialized(materialized, lix.revision);
        Ok(())
    }

    async fn close(&self) -> Result<(), LixError> {
        self.session.close().await
    }

    async fn collect_lix_snapshot_read(&self) -> Result<LixSnapshotRead, LixError> {
        let mut snapshot = Snapshot::default();
        snapshot.directories.insert("/".to_string());
        let statements: [(&str, &[Value]); 2] = [
            ("SELECT path FROM lix_directory ORDER BY path", &[]),
            ("SELECT path, content FROM lix_file ORDER BY path", &[]),
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
            let data = row.get::<Vec<u8>>("content")?;
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

    async fn apply_local_snapshot_to_lix(
        &self,
        local: &Snapshot,
        previous: Option<&Snapshot>,
    ) -> Result<LixSnapshotRead, LixError> {
        let lix = self.collect_lix_snapshot_read().await?;
        let mut needs_fresh_lix_read = false;

        for path in lix.snapshot.files.keys() {
            if !local.files.contains_key(path)
                && !is_lix_storage_path(path)
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

        let mut directories_to_remove = Vec::new();
        for path in lix.snapshot.directories.difference(&local.directories) {
            if path.as_str() == "/"
                || is_lix_storage_path(path)
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

        let mut directories_to_create = local
            .directories
            .difference(&lix.snapshot.directories)
            .filter(|path| path.as_str() != "/" && !is_lix_storage_path(path))
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
        for (path, data) in local.files.iter().filter(|(path, _)| {
            !is_lix_storage_path(path) && !is_materialization_ignored_path(path)
        }) {
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
                params.push(Value::Blob((*data).to_vec().into()));
            }
            self.session.execute(&sql, &params).await?;
            start = end;
        }
        Ok(())
    }

    fn materialize_snapshot(
        &self,
        target: &Snapshot,
        base: Option<&Snapshot>,
    ) -> Result<Snapshot, LixError> {
        ensure_filesystem_root_directory(&self.layout.root)?;
        ensure_filesystem_lix_directory(&self.layout.lix_dir)?;
        let local = collect_local_snapshot(&self.layout)?;
        let previous = self.last_materialized_disk();

        for path in local.files.keys().filter(|path| {
            !target.files.contains_key(*path)
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
                        && local.directories.contains(*path) == snapshot.directories.contains(*path)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        sort_directories_deepest_first(&mut directories_to_remove);
        for path in directories_to_remove {
            remove_materialized_directory(&self.layout, &path)?;
        }

        let mut directories_to_create = target
            .directories
            .iter()
            .filter(|path| path.as_str() != "/" && !is_materialization_ignored_path(path))
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

        for (path, data) in target
            .files
            .iter()
            .filter(|(path, _)| !is_materialization_ignored_path(path))
        {
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

        self.remembered_snapshot(target)
    }

    fn remembered_snapshot(&self, target: &Snapshot) -> Result<Snapshot, LixError> {
        let materialized = collect_local_snapshot(&self.layout)?;
        let mut remembered = target.clone();
        // The physical metadata root always exists, but it is not a user
        // `lix_directory` row and must never be imported as one.
        remembered.directories.insert("/.lix".to_string());
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
                if runtime.block_on(state.ingest_workspace(true)).is_ok() {
                    refresh_filesystem_watcher(&state, &mut debouncer, &mut poll_filesystem);
                }
            }
            Ok(FilesystemEvent::SyncFromLix { reply_tx }) => {
                let result = runtime.block_on(state.sync_from_lix());
                if result.is_ok() {
                    refresh_filesystem_watcher(&state, &mut debouncer, &mut poll_filesystem);
                }
                let _ = reply_tx.send(result);
            }
            Ok(FilesystemEvent::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                if let Some(watcher) = debouncer.take() {
                    watcher.stop();
                }
                let _ = runtime.block_on(state.close());
                return;
            }
        }
    }
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
    if watcher.refresh(&state.layout).is_err() {
        if let Some(watcher) = debouncer.take() {
            watcher.stop();
        }
        *poll_filesystem = true;
    }
}

fn collect_local_snapshot(layout: &FilesystemLayout) -> Result<Snapshot, LixError> {
    validate_filesystem_root_directory(&layout.root)?;
    validate_filesystem_lix_directory(&layout.lix_dir)?;

    let mut snapshot = Snapshot::default();
    snapshot.directories.insert("/".to_string());
    let child_dirs = collect_local_directory_shallow(layout, &layout.root, &mut snapshot)?;
    let child_snapshot = collect_local_child_directories(layout, child_dirs)?;
    merge_snapshot(&mut snapshot, child_snapshot);
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
            let Ok(lix_path) = layout.local_path_to_lix_path(&path) else {
                remember_unmanaged_local_path(layout, directory, &path, snapshot);
                continue;
            };
            snapshot.directories.insert(lix_path);
            child_dirs.push(path);
        } else if file_type.is_file() {
            let Ok(lix_path) = layout.local_path_to_lix_path(&path) else {
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

fn collect_lix_directory_snapshot(
    layout: &FilesystemLayout,
    snapshot: &mut Snapshot,
) -> Result<(), LixError> {
    snapshot.directories.insert("/.lix".to_string());
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
    if let Ok(lix_path) = layout.local_path_to_lix_path(path) {
        snapshot.unmanaged_paths.insert(lix_path);
    } else if layout.local_base_for_path(directory) != Some(directory) {
        if let Ok(parent_path) = layout.local_path_to_lix_path(directory) {
            snapshot.unmanaged_paths.insert(parent_path);
        }
    }
}

impl FilesystemWatcher {
    fn refresh(
        &mut self,
        layout: &FilesystemLayout,
    ) -> Result<(), notify_debouncer_full::notify::Error> {
        let next_paths = filesystem_watch_paths(layout)?;
        if self.watched_paths == next_paths {
            return Ok(());
        }
        let previous = self
            .watched_paths
            .iter()
            .map(|watched| (watched.path.as_path(), watched.recursive))
            .collect::<BTreeMap<_, _>>();
        let next = next_paths
            .iter()
            .map(|watched| (watched.path.as_path(), watched.recursive))
            .collect::<BTreeMap<_, _>>();
        for (path, recursive) in &previous {
            if next.get(path) != Some(recursive) {
                let _ = self.debouncer.unwatch(path);
            }
        }
        for watched_path in &next_paths {
            if previous.get(watched_path.path.as_path()) != Some(&watched_path.recursive) {
                self.debouncer
                    .watch(&watched_path.path, watched_path.recursive_mode())?;
            }
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
) -> Result<Vec<FilesystemWatchPath>, notify_debouncer_full::notify::Error> {
    let mut paths = BTreeMap::<PathBuf, bool>::new();
    // Keep the workspace root non-recursive so the authoritative `.lix`
    // directory is never recursively watched. Every other top-level directory
    // gets one recursive registration; root events refresh this set after
    // creates, deletes, and renames.
    paths.insert(layout.root.clone(), false);
    for entry in std::fs::read_dir(&layout.root)? {
        let entry = entry?;
        let path = entry.path();
        if path == layout.lix_dir || is_filesystem_sync_ignored_local_path(layout, &path) {
            continue;
        }
        if entry.file_type()?.is_dir() {
            paths.insert(path, true);
        }
    }
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
        return Err(unsupported_materialization_entry(path, &local_path));
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
        return Err(unsupported_materialization_entry(path, &local_path));
    }
    if let Some(parent) = local_path.parent() {
        if path_contains_unmanaged_entry(layout, parent)? {
            return Err(unsupported_materialization_entry(path, &local_path));
        }
        std::fs::create_dir_all(parent)
            .map_err(|error| io_error("create filesystem file parent", parent, error))?;
        if path_contains_unmanaged_entry(layout, parent)? {
            return Err(unsupported_materialization_entry(path, &local_path));
        }
    }
    if path_contains_unmanaged_entry(layout, &local_path)? {
        return Err(unsupported_materialization_entry(path, &local_path));
    }
    std::fs::write(&local_path, data)
        .map_err(|error| io_error("write filesystem file", &local_path, error))
}

fn lix_file_upsert_sql(row_count: usize) -> String {
    debug_assert!(row_count > 0);
    let mut sql = String::from("INSERT INTO lix_file (path, content) VALUES ");
    for row in 0..row_count {
        if row > 0 {
            sql.push_str(", ");
        }
        let _ = write!(sql, "(${}, ${})", row * 2 + 1, row * 2 + 2);
    }
    sql.push_str(" ON CONFLICT (path) DO UPDATE SET content = excluded.content");
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

fn local_path_to_lix_path(root: &Path, path: &Path) -> Result<String, LixError> {
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
    Ok(format!("/{}", segments.join("/")))
}

fn lix_path_to_local_path(root: &Path, path: &str) -> Result<PathBuf, LixError> {
    let parsed = LixPath::try_from_directory_path(path)?;
    let mut local = root.to_path_buf();
    for segment in parsed.segments() {
        push_lix_path_segment(&mut local, segment, path)?;
    }
    Ok(local)
}

fn push_lix_path_segment(local: &mut PathBuf, segment: &str, path: &str) -> Result<(), LixError> {
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

fn is_filesystem_metadata_path(path: &str) -> bool {
    path == "/.lix/.gitignore"
        || is_filesystem_internal_path(path)
        || is_legacy_filesystem_metadata_path(path)
}

fn is_filesystem_internal_path(path: &str) -> bool {
    path == "/.lix/.internal" || path.starts_with("/.lix/.internal/")
}

fn is_legacy_filesystem_metadata_path(path: &str) -> bool {
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
            .local_path_to_lix_path(path)
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

fn unsupported_materialization_entry(path: &str, local_path: &Path) -> LixError {
    LixError::new(
        "LIX_FILESYSTEM_UNSUPPORTED_ENTRY",
        format!(
            "cannot materialize regular Lix path {path} at {}: the path is blocked by a symlink or another unsupported filesystem entry",
            local_path.display()
        ),
    )
}

fn prepare_filesystem_layout(root: &Path) -> Result<FilesystemLayout, LixError> {
    ensure_filesystem_root_directory(root)?;
    let root = std::fs::canonicalize(root)
        .map_err(|error| io_error("canonicalize filesystem root", root, error))?;
    let lix_dir = root.join(".lix");
    ensure_filesystem_lix_directory(&lix_dir)?;
    let lix_dir = std::fs::canonicalize(&lix_dir)
        .map_err(|error| io_error("canonicalize filesystem .lix directory", &lix_dir, error))?;
    Ok(FilesystemLayout { root, lix_dir })
}

fn open_filesystem_rocksdb(layout: &FilesystemLayout) -> Result<RocksDBFilesystem, LixError> {
    let metadata_dir = ensure_filesystem_rocksdb_metadata_directory(layout)?;
    RocksDBFilesystem::open(metadata_dir).map_err(rocksdb_error)
}

fn ensure_filesystem_rocksdb_metadata_directory(
    layout: &FilesystemLayout,
) -> Result<PathBuf, LixError> {
    ensure_filesystem_lix_directory(&layout.lix_dir)?;
    remove_legacy_filesystem_root_metadata(&layout.root, &layout.lix_dir)?;
    let internal_dir = layout.lix_dir.join(".internal");
    reset_legacy_filesystem_internal_directory(&internal_dir)?;
    ensure_metadata_directory(&internal_dir, "filesystem metadata directory")?;
    let metadata_dir = internal_dir.join("rocksdb");
    ensure_metadata_directory(&metadata_dir, "filesystem RocksDB metadata directory")?;
    Ok(metadata_dir)
}

fn remove_legacy_filesystem_root_metadata(root: &Path, lix_dir: &Path) -> Result<(), LixError> {
    remove_legacy_filesystem_lix_metadata(lix_dir)?;
    remove_legacy_metadata_path(&root.join(".lix_system"))
}

fn remove_legacy_filesystem_lix_metadata(lix_dir: &Path) -> Result<(), LixError> {
    for name in LEGACY_FILESYSTEM_SQLITE_METADATA_NAMES {
        remove_legacy_metadata_file(&lix_dir.join(name))?;
    }
    Ok(())
}

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

fn legacy_filesystem_sqlite_metadata_exists(internal_dir: &Path) -> bool {
    LEGACY_FILESYSTEM_SQLITE_METADATA_NAMES
        .iter()
        .any(|name| internal_dir.join(name).exists())
}

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

fn rocksdb_error(error: StorageError) -> LixError {
    LixError::new(
        LixError::CODE_STORAGE_ERROR,
        format!("failed to open filesystem RocksDB storage: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use lix::Value;

    async fn lix_read_file<StorageImpl>(
        session: &SessionContext<StorageImpl>,
        path: &str,
    ) -> Result<Option<Vec<u8>>, LixError>
    where
        StorageImpl: Storage + Clone + Send + Sync + 'static,
    {
        let result = session
            .execute(
                "SELECT content FROM lix_file WHERE path = $1",
                &[Value::Text(path.to_string())],
            )
            .await?;
        result
            .rows()
            .first()
            .map(|row| row.get::<Vec<u8>>("content"))
            .transpose()
    }

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
                "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
             ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                &[Value::Text(path.to_string()), Value::Blob(data.into())],
            )
            .await?;
        Ok(())
    }

    async fn open_test_filesystem_state(
        layout: FilesystemLayout,
    ) -> FilesystemState<RocksDBFilesystem> {
        let storage = open_filesystem_rocksdb(&layout).unwrap();
        let engine = lix::storage::open_engine(storage.clone(), None)
            .await
            .unwrap();
        FilesystemState {
            session: engine.open_workspace_session().await.unwrap(),
            layout,
            sync_lock: tokio::sync::Mutex::new(()),
            last_materialized: Mutex::new(None),
        }
    }

    #[test]
    fn local_paths_render_opaque_segments() {
        let root = Path::new("root");

        assert_eq!(
            local_path_to_lix_path(root, &root.join("bad%name.txt")).unwrap(),
            "/bad%name.txt"
        );
        assert_eq!(
            local_path_to_lix_path(root, &root.join("#hash?.txt")).unwrap(),
            "/#hash?.txt"
        );
        assert_eq!(
            local_path_to_lix_path(root, &root.join("dir%23")).unwrap(),
            "/dir%23"
        );
    }

    #[test]
    fn unmanaged_paths_block_only_the_same_path_or_descendants() {
        assert!(unmanaged_path_blocks_lix_path("/docs", "/docs"));
        assert!(unmanaged_path_blocks_lix_path("/docs", "/docs/readme.md"));
        assert!(!unmanaged_path_blocks_lix_path(
            "/docs",
            "/docs-old/readme.md"
        ));
    }

    #[cfg(unix)]
    #[test]
    fn local_paths_preserve_backslash_segments_on_unix() {
        let root = Path::new("root");

        assert_eq!(
            local_path_to_lix_path(root, &root.join(r"a\b.txt")).unwrap(),
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

    #[cfg(unix)]
    #[test]
    fn materializing_a_regular_file_reports_symlink_collisions() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().unwrap();
        let root = tempdir.path().join("workspace");
        let lix_dir = root.join(".lix");
        std::fs::create_dir_all(&lix_dir).unwrap();
        std::fs::write(root.join("target.txt"), b"target").unwrap();
        symlink("target.txt", root.join("link.txt")).unwrap();
        let layout = FilesystemLayout { root, lix_dir };

        let error = write_materialized_file(&layout, "/link.txt", b"replacement")
            .expect_err("a symlink collision must be reported");

        assert_eq!(error.code, "LIX_FILESYSTEM_UNSUPPORTED_ENTRY");
        assert!(error.message.contains("/link.txt"));
        assert_eq!(
            std::fs::read(layout.root.join("target.txt")).unwrap(),
            b"target"
        );
    }

    #[test]
    fn lix_paths_reject_structurally_unsafe_segments() {
        let root = Path::new("root");

        for (path, expected_code) in [
            ("relative", "LIX_ERROR_PATH_MISSING_LEADING_SLASH"),
            ("/a//b", "LIX_ERROR_PATH_EMPTY_SEGMENT"),
            ("/./b", "LIX_ERROR_PATH_DOT_SEGMENT"),
            ("/../b", "LIX_ERROR_PATH_DOT_SEGMENT"),
            ("/nul\0name", "LIX_ERROR_PATH_NUL"),
        ] {
            let error = lix_path_to_local_path(root, path).expect_err("path should fail");
            assert_eq!(error.code, expected_code);
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
        };
        let snapshot = collect_local_snapshot(&layout).unwrap();

        assert!(snapshot.directories.contains("/"));
        assert_eq!(snapshot.files.get("/root.txt").unwrap(), b"root");
        for index in 0..FILESYSTEM_PARALLEL_SNAPSHOT_MIN_DIRS {
            assert!(snapshot.directories.contains(&format!("/dir-{index}")));
            assert!(
                snapshot
                    .directories
                    .contains(&format!("/dir-{index}/nested"))
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
    fn watcher_never_recursively_registers_lix_storage() {
        let tempdir = tempfile::tempdir().unwrap();
        let layout = prepare_filesystem_layout(tempdir.path()).unwrap();
        std::fs::create_dir_all(tempdir.path().join("docs")).unwrap();

        let watched = filesystem_watch_paths(&layout).unwrap();

        assert!(watched.contains(&FilesystemWatchPath {
            path: layout.root.clone(),
            recursive: false,
        }));
        assert!(watched.contains(&FilesystemWatchPath {
            path: layout.root.join("docs"),
            recursive: true,
        }));
        assert!(!watched.iter().any(|path| path.path == layout.lix_dir));
    }

    #[test]
    fn lix_file_upsert_sql_batches_path_content_rows() {
        assert_eq!(
            lix_file_upsert_sql(3),
            "INSERT INTO lix_file (path, content) VALUES ($1, $2), ($3, $4), ($5, $6) ON CONFLICT (path) DO UPDATE SET content = excluded.content"
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

    #[tokio::test]
    async fn disk_sync_remembers_canonical_snapshot_for_idle_skip() {
        let tempdir = tempfile::tempdir().unwrap();
        let layout = prepare_filesystem_layout(tempdir.path()).unwrap();
        let state = open_test_filesystem_state(layout).await;

        state.ingest_workspace(false).await.unwrap();

        let local = collect_local_snapshot(&state.layout).unwrap();
        let lix_revision = state.collect_lix_revision().await.unwrap();
        assert!(
            state.is_last_materialized(&local, &lix_revision),
            "an unchanged filesystem should be recognized as already materialized"
        );

        state.close().await.unwrap();
    }

    #[tokio::test]
    async fn disk_sync_does_not_reimport_unchanged_materialized_file_deleted_in_lix() {
        let tempdir = tempfile::tempdir().unwrap();
        let layout = prepare_filesystem_layout(tempdir.path()).unwrap();
        let state = open_test_filesystem_state(layout).await;

        state.ingest_workspace(false).await.unwrap();
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
        state.ingest_workspace(true).await.unwrap();

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

    #[tokio::test]
    async fn disk_sync_does_not_skip_lix_side_file_content_change() {
        let tempdir = tempfile::tempdir().unwrap();
        let layout = prepare_filesystem_layout(tempdir.path()).unwrap();
        let state = open_test_filesystem_state(layout).await;

        state.ingest_workspace(false).await.unwrap();
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
        state.ingest_workspace(true).await.unwrap();

        assert_eq!(
            std::fs::read(tempdir.path().join("sql.txt")).unwrap(),
            b"second"
        );

        state.close().await.unwrap();
    }

    #[tokio::test]
    async fn disk_sync_materialization_preserves_file_changed_after_import() {
        let tempdir = tempfile::tempdir().unwrap();
        let layout = prepare_filesystem_layout(tempdir.path()).unwrap();
        let state = open_test_filesystem_state(layout).await;

        state.ingest_workspace(false).await.unwrap();
        let disk_path = tempdir.path().join("disk.txt");
        std::fs::write(&disk_path, b"disk").unwrap();
        let local = collect_local_snapshot(&state.layout).unwrap();
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
            .materialize_snapshot(&target.snapshot, Some(&local))
            .unwrap();
        state.remember_materialized(materialized, target.revision);
        assert_eq!(std::fs::read(&disk_path).unwrap(), b"changed");

        state.ingest_workspace(true).await.unwrap();
        assert_eq!(
            lix_read_file(&state.session, "/disk.txt")
                .await
                .unwrap()
                .as_deref(),
            Some(b"changed".as_slice())
        );

        state.close().await.unwrap();
    }

    #[tokio::test]
    async fn lix_directory_changes_are_never_imported_as_workspace_files() {
        let tempdir = tempfile::tempdir().unwrap();
        let layout = prepare_filesystem_layout(tempdir.path()).unwrap();
        let state = open_test_filesystem_state(layout).await;
        std::fs::create_dir_all(tempdir.path().join(".lix").join("manual")).unwrap();
        std::fs::write(
            tempdir
                .path()
                .join(".lix")
                .join("manual")
                .join("secret.bin"),
            [0, 1, 2, 3],
        )
        .unwrap();

        state.ingest_workspace(false).await.unwrap();

        let rows = state
            .session
            .execute(
                "SELECT path FROM lix_file WHERE path = $1",
                &[Value::Text("/.lix/manual/secret.bin".to_string())],
            )
            .await
            .unwrap();
        assert!(rows.rows().is_empty());
        state.close().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_drains_accepted_lix_to_disk_work_and_rejects_new_work() {
        let tempdir = tempfile::tempdir().unwrap();
        let storage = LocalFilesystem::open(tempdir.path()).await.unwrap();
        let raw_storage = storage.inner.inner.clone();
        let lix = lix::open_lix().with_storage(raw_storage).await.unwrap();
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
            &[
                Value::Text("/accepted.bin".to_string()),
                Value::Blob(vec![0, 255, 1, 254].into()),
            ],
        )
        .await
        .unwrap();
        lix.close().await.unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let event_tx = storage.inner.supervisor.inner.event_tx.lock().unwrap();
            event_tx
                .as_ref()
                .unwrap()
                .send(FilesystemEvent::SyncFromLix { reply_tx })
                .unwrap();
        }
        storage.inner.supervisor.inner.shutdown();

        assert!(reply_rx.await.unwrap().is_ok());
        assert_eq!(
            std::fs::read(tempdir.path().join("accepted.bin")).unwrap(),
            [0, 255, 1, 254]
        );
        assert!(storage.inner.supervisor.sync_from_lix().await.is_err());
    }
}
