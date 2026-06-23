use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::thread;

use lix_engine::{
    Backend, BackendError, BackendRead, BackendWrite, CommitResult, Engine, GetOptions,
    InMemoryBackend, Key, KeyRange, LixError, MountedFilesystem, MountedFilesystemListing,
    PointVisitor, PutBatch, ReadOptions, ScanOptions, ScanResult, ScanVisitor, SpaceId,
    WriteOptions,
};

#[cfg(feature = "fs_backend")]
use lix_fs_backend::RocksDbFilesystemBackend;

const LIX_DIRECTORY_GITIGNORE: &[u8] = b"*\n";
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
    async fn list(&self) -> Result<MountedFilesystemListing, BackendError> {
        let snapshot = collect_local_snapshot(&self.root, self.metadata_mode, &self.path_filter)
            .map_err(|error| BackendError::Io(error.format()))?;
        Ok(MountedFilesystemListing {
            directories: snapshot.directories,
            files: snapshot.files,
        })
    }

    async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, BackendError> {
        if path.ends_with('/')
            || !self.path_filter.includes_file(path)
            || is_filesystem_sync_ignored_lix_path(path, self.metadata_mode)
        {
            return Ok(None);
        }
        let local_path = lix_path_to_local_path(&self.root, path)
            .map_err(|error| BackendError::Io(error.format()))?;
        if path_contains_unmanaged_entry(&self.root, &local_path)
            .map_err(|error| BackendError::Io(error.format()))?
        {
            return Ok(None);
        }
        match std::fs::read(&local_path) {
            Ok(data) => Ok(Some(data)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(BackendError::Io(
                io_error("read filesystem stored file data", &local_path, error).format(),
            )),
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
        _engine: Engine<B>,
        root: &Path,
        metadata_mode: FilesystemMetadataMode,
        filter: FsBackendFilter,
    ) -> Result<Self, LixError> {
        Ok(Self {
            inner: backend,
            supervisor: FilesystemSupervisor::open(root, metadata_mode, filter).await?,
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
        self.inner.commit()
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
        root: &Path,
        metadata_mode: FilesystemMetadataMode,
        filter: FsBackendFilter,
    ) -> Result<Self, LixError> {
        ensure_filesystem_root_directory(root)?;
        let root = std::fs::canonicalize(root)
            .map_err(|error| io_error("canonicalize filesystem root", root, error))?;
        let path_filter = FilesystemPathFilter::from_filter(filter)?;
        if metadata_mode == FilesystemMetadataMode::Persistent {
            migrate_legacy_filesystem_system_directory(&root)?;
            ensure_filesystem_lix_directory(&root)?;
        }
        Ok(Self {
            inner: Arc::new(FilesystemSupervisorInner {
                root,
                metadata_mode,
                path_filter,
            }),
            _marker: PhantomData,
        })
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
    matches!(
        name,
        "db.sqlite" | "db.sqlite-wal" | "db.sqlite-shm" | "db.sqlite-journal"
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

fn io_error(operation: &str, path: &Path, error: std::io::Error) -> LixError {
    let path = path.display();
    LixError::new(
        "LIX_FILESYSTEM_IO_ERROR",
        format!("{operation} {path}: {error}"),
    )
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
    use lix_engine::{CreateBranchOptions, Value};

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

    #[cfg(feature = "fs_backend")]
    #[tokio::test]
    async fn fs_backend_mounted_filesystem_reads_disk_file_data() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("note.md"), b"from disk").unwrap();

        let backend = FsBackend::open_memory(tempdir.path()).await.unwrap();
        let resolver = backend
            .mounted_filesystem()
            .expect("FsBackend should expose mounted filesystem");

        let data = resolver
            .read_file("/note.md")
            .await
            .expect("mounted filesystem read should succeed")
            .expect("mounted filesystem should find included file");
        assert_eq!(data, b"from disk");

        let missing = resolver
            .read_file("/missing.md")
            .await
            .expect("missing mounted filesystem read should not fail");
        assert_eq!(missing, None);
    }

    #[cfg(feature = "fs_backend")]
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
            resolver.read_file("/included.md").await.unwrap().as_deref(),
            Some(b"included".as_slice())
        );
        assert_eq!(resolver.read_file("/excluded.md").await.unwrap(), None);
    }

    #[cfg(feature = "fs_backend")]
    #[tokio::test]
    async fn fs_backend_memory_opens_overlay_only_and_reads_mounted_data() {
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
            "opening an ephemeral filesystem workspace should not import mounted file data"
        );
        let descriptors = session
            .execute(
                "SELECT COUNT(*) AS count FROM lix_state WHERE schema_key IN ($1, $2)",
                &[
                    Value::Text("lix_file_descriptor".to_string()),
                    Value::Text("lix_directory_descriptor".to_string()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            descriptors.rows()[0].get::<i64>("count").unwrap(),
            0,
            "opening an ephemeral filesystem workspace should not import mounted descriptors"
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
            "mounted reads should not hydrate filesystem bytes into BCAS"
        );
    }

    #[cfg(feature = "fs_backend")]
    #[tokio::test]
    async fn fs_backend_by_branch_expands_mounted_overlay_per_branch() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("note.md"), b"from disk").unwrap();

        let backend = FsBackend::open_memory(tempdir.path()).await.unwrap();
        let engine = Engine::new(backend).await.unwrap();
        let session = engine.open_workspace_session().await.unwrap();
        let active_branch_id = session.active_branch_id().await.unwrap();
        let feature = session
            .create_branch(CreateBranchOptions {
                id: Some("feature-branch".to_string()),
                name: "Feature".to_string(),
                from_commit_id: None,
            })
            .await
            .unwrap();

        let rows = session
            .execute(
                "SELECT lixcol_branch_id, data FROM lix_file_by_branch \
                 WHERE path = $1 AND lixcol_branch_id IN ($2, $3) \
                 ORDER BY lixcol_branch_id",
                &[
                    Value::Text("/note.md".to_string()),
                    Value::Text(active_branch_id.clone()),
                    Value::Text(feature.id.clone()),
                ],
            )
            .await
            .expect("by-branch should include mounted overlay rows for each queried branch");
        assert_eq!(rows.rows().len(), 2);
        assert_eq!(rows.rows()[0].get::<Vec<u8>>("data").unwrap(), b"from disk");
        assert_eq!(rows.rows()[1].get::<Vec<u8>>("data").unwrap(), b"from disk");

        session
            .execute(
                "INSERT INTO lix_file_by_branch (path, data, lixcol_branch_id) VALUES ($1, $2, $3)",
                &[
                    Value::Text("/note.md".to_string()),
                    Value::Blob(b"from lix branch".to_vec()),
                    Value::Text(feature.id.clone()),
                ],
            )
            .await
            .unwrap();

        let rows = session
            .execute(
                "SELECT lixcol_branch_id, data FROM lix_file_by_branch \
                 WHERE path = $1 AND lixcol_branch_id IN ($2, $3) \
                 ORDER BY lixcol_branch_id",
                &[
                    Value::Text("/note.md".to_string()),
                    Value::Text(active_branch_id.clone()),
                    Value::Text(feature.id),
                ],
            )
            .await
            .unwrap();
        assert_eq!(rows.rows().len(), 2);
        let by_branch = rows
            .rows()
            .iter()
            .map(|row| {
                (
                    row.get::<String>("lixcol_branch_id").unwrap(),
                    row.get::<Vec<u8>>("data").unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(by_branch.get("feature-branch").unwrap(), b"from lix branch");
        assert_eq!(by_branch.get(&active_branch_id).unwrap(), b"from disk");
    }

    #[cfg(feature = "fs_backend")]
    #[tokio::test]
    async fn fs_backend_memory_opens_large_folder_overlay_only_and_reads_one_file() {
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
        let descriptors = session
            .execute(
                "SELECT COUNT(*) AS count FROM lix_state WHERE schema_key IN ($1, $2)",
                &[
                    Value::Text("lix_file_descriptor".to_string()),
                    Value::Text("lix_directory_descriptor".to_string()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(descriptors.rows()[0].get::<i64>("count").unwrap(), 0);

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

    #[cfg(feature = "fs_backend")]
    #[tokio::test]
    async fn fs_backend_update_where_data_rejects_descriptor_only_files() {
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
            .expect_err("UPDATE WHERE data should not auto-hydrate");
        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(tempdir.path().join("note.md").exists());
    }

    #[cfg(feature = "fs_backend")]
    #[tokio::test]
    async fn fs_backend_update_assignment_reading_data_rejects_descriptor_only_files() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("note.md"), b"from disk").unwrap();

        let backend = FsBackend::open_memory(tempdir.path()).await.unwrap();
        let engine = Engine::new(backend).await.unwrap();
        let session = engine.open_workspace_session().await.unwrap();

        let error = session
            .execute(
                "UPDATE lix_file SET data = data WHERE path = $1",
                &[Value::Text("/note.md".to_string())],
            )
            .await
            .expect_err("UPDATE assignment reading data should not auto-hydrate");
        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert_eq!(
            std::fs::read(tempdir.path().join("note.md")).unwrap(),
            b"from disk"
        );
    }

    #[cfg(feature = "fs_backend")]
    #[tokio::test]
    async fn fs_backend_delete_where_data_rejects_descriptor_only_files() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("note.md"), b"from disk").unwrap();

        let backend = FsBackend::open_memory(tempdir.path()).await.unwrap();
        let engine = Engine::new(backend).await.unwrap();
        let session = engine.open_workspace_session().await.unwrap();

        let error = session
            .execute(
                "DELETE FROM lix_file WHERE data = $1",
                &[Value::Blob(b"from disk".to_vec())],
            )
            .await
            .expect_err("DELETE WHERE data should not auto-hydrate");
        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(tempdir.path().join("note.md").exists());
    }

    #[cfg(feature = "fs_backend")]
    #[tokio::test]
    async fn fs_backend_persistent_opens_overlay_only_and_reads_mounted_data() {
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
        let descriptors = session
            .execute(
                "SELECT COUNT(*) AS count FROM lix_state WHERE schema_key IN ($1, $2)",
                &[
                    Value::Text("lix_file_descriptor".to_string()),
                    Value::Text("lix_directory_descriptor".to_string()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(descriptors.rows()[0].get::<i64>("count").unwrap(), 0);

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
