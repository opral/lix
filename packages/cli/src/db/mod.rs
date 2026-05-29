use crate::app::AppContext;
use crate::error::CliError;
use base64::Engine as _;
use bytes::Bytes;
use lix_sdk::{
    open_lix_with_backend, Backend, BackendError, BackendRangeScan, BackendRead, BackendWrite,
    CommitResult, CoreProjection, GetOptions, Key, KeyRange, Lix, LixError, PointVisitor,
    ProjectedValueRef, PutBatch, ReadOptions, ScanOptions, ScanResult, ScanVisitor, StoredValue,
    WriteOptions, WriteStats,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub type FileLix = Lix<FileBackend>;

pub fn resolve_db_path(context: &AppContext) -> Result<PathBuf, CliError> {
    if let Some(path) = &context.lix_path {
        validate_lix_file_path(path)?;
        if !path.exists() {
            return Err(CliError::msg(format!(
                "lix file does not exist: {}",
                path.display()
            )));
        }
        return Ok(path.clone());
    }

    let cwd =
        std::env::current_dir().map_err(|source| CliError::io("failed to read cwd", source))?;
    let mut candidates = find_lix_files(&cwd)?;

    if candidates.is_empty() {
        return Err(CliError::msg(
            "no .lix files found in current directory; pass --path <path-to-file.lix>",
        ));
    }
    if candidates.len() > 1 {
        candidates.sort();
        let paths = candidates
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(CliError::msg(format!(
            "multiple .lix files found ({paths}); pass --path <path-to-file.lix>"
        )));
    }

    Ok(candidates.remove(0))
}

pub fn open_lix_at(path: &Path) -> Result<FileLix, CliError> {
    let backend = FileBackend::from_path(path)?;

    block_on(open_lix_with_backend(backend))
        .map_err(|err| CliError::msg(format!("failed to open lix at {}: {}", path.display(), err)))
}

pub fn init_lix_at(path: &Path) -> Result<bool, CliError> {
    validate_lix_file_path(path)?;

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|source| {
                CliError::io("failed to create parent directory for lix file", source)
            })?;
        }
    }

    let initialized = !path.exists();
    let _ = open_lix_at(path)?;
    Ok(initialized)
}

pub fn destroy_lix_at(path: &Path) -> Result<(), CliError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(CliError::io("failed to destroy lix file", error)),
    }
    .and_then(|_| remove_sidecar(path, "wal"))
    .and_then(|_| remove_sidecar(path, "shm"))
    .and_then(|_| remove_sidecar(path, "journal"))
}

/// Prepares a `.lix` output target for initialization.
///
/// The CLI delegates storage-backed cleanup to the backend boundary so command
/// code does not need to know how a backend represents its physical artifacts.
pub fn prepare_lix_output_path(path: &Path, force: bool) -> Result<(), CliError> {
    validate_lix_file_path(path)?;

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|source| CliError::io("failed to create output directory", source))?;
        }
    }

    if path.exists() && path.is_dir() {
        return Err(CliError::msg(format!(
            "output path points to a directory, expected a file: {}",
            path.display()
        )));
    }

    if force {
        destroy_lix_at(path)?;
        return Ok(());
    }

    if path.exists() {
        return Err(CliError::msg(format!(
            "output path already exists: {}",
            path.display()
        )));
    }

    Ok(())
}

fn find_lix_files(cwd: &Path) -> Result<Vec<PathBuf>, CliError> {
    let mut files = Vec::new();
    let entries =
        fs::read_dir(cwd).map_err(|source| CliError::io("failed to read cwd entries", source))?;
    for entry in entries {
        let entry =
            entry.map_err(|source| CliError::io("failed to read directory entry", source))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) == Some("lix") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn validate_lix_file_path(path: &Path) -> Result<(), CliError> {
    if path.extension().and_then(|ext| ext.to_str()) == Some("lix") {
        return Ok(());
    }

    Err(CliError::msg(format!(
        "expected a .lix file path: {}",
        path.display()
    )))
}

pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should initialize")
        .block_on(future)
}

fn remove_sidecar(path: &Path, suffix: &str) -> Result<(), CliError> {
    let sidecar = PathBuf::from(format!("{}-{suffix}", path.display()));
    match fs::remove_file(sidecar) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(CliError::io("failed to destroy lix sidecar file", error)),
    }
}

type KvMap = BTreeMap<Vec<u8>, Vec<u8>>;

// TODO: Replace this custom whole-file KV backend with SQLite for the CLI.
// This backend exists only as a transitional local `.lix` file adapter. It
// snapshots the entire file into memory and rewrites it on commit, so it is not
// a real concurrent storage backend. The instance-local write gate below only
// rejects overlapping writes through the same opened handle; separate handles
// or processes can still overwrite each other. SQLite should own CLI durability
// and write concurrency instead.
#[derive(Clone)]
pub struct FileBackend {
    path: Arc<PathBuf>,
    kv: Arc<Mutex<KvMap>>,
    write_active: Arc<Mutex<bool>>,
}

impl FileBackend {
    fn from_path(path: &Path) -> Result<Self, CliError> {
        let kv = read_kv_file(path)?;
        Ok(Self {
            path: Arc::new(path.to_path_buf()),
            kv: Arc::new(Mutex::new(kv)),
            write_active: Arc::new(Mutex::new(false)),
        })
    }
}

#[derive(Clone)]
pub struct FileBackendRead {
    kv: KvMap,
}

pub struct FileBackendRangeScan {
    rows: Vec<(Key, Vec<u8>)>,
    position: usize,
    projection: CoreProjection,
}

pub struct FileBackendWrite {
    path: Arc<PathBuf>,
    parent: Arc<Mutex<KvMap>>,
    write_active: Arc<Mutex<bool>>,
    kv: KvMap,
    stats: WriteStats,
    closed: bool,
}

impl Backend for FileBackend {
    type Read<'a>
        = FileBackendRead
    where
        Self: 'a;

    type Write<'a>
        = FileBackendWrite
    where
        Self: 'a;
    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(FileBackendRead {
            kv: self
                .kv
                .lock()
                .map_err(|_| backend_lock_error("cli file backend kv"))?
                .clone(),
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        {
            let mut active = self
                .write_active
                .lock()
                .map_err(|_| backend_lock_error("cli file backend write gate"))?;
            if *active {
                return Err(BackendError::Io(
                    "cli file backend write transaction already active".to_string(),
                ));
            }
            *active = true;
        }
        let kv = match self
            .kv
            .lock()
            .map_err(|_| backend_lock_error("cli file backend kv"))
            .map(|parent| parent.clone())
        {
            Ok(kv) => kv,
            Err(error) => {
                self.clear_write_active();
                return Err(error);
            }
        };
        Ok(FileBackendWrite {
            path: Arc::clone(&self.path),
            parent: Arc::clone(&self.kv),
            write_active: Arc::clone(&self.write_active),
            kv,
            stats: WriteStats::default(),
            closed: false,
        })
    }
}

impl FileBackend {
    fn clear_write_active(&self) {
        if let Ok(mut active) = self.write_active.lock() {
            *active = false;
        }
    }
}

impl BackendRead for FileBackendRead {
    type RangeScan<'cursor> = FileBackendRangeScan;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        for (index, key) in keys.iter().enumerate() {
            let value = self
                .kv
                .get(key.0.as_ref())
                .map(|value| project_value_ref(value, opts.projection));
            visitor.visit(index, key, value)?;
        }
        Ok(())
    }

    fn with_range_scan<T, F>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
    {
        let mut rows = self
            .kv
            .iter()
            .filter(|(key, _)| key_matches_range(key, &range, opts.resume_after))
            .map(|(key, value)| (Key(Bytes::copy_from_slice(key)), value.clone()))
            .collect::<Vec<_>>();
        rows.sort_by(|(left, _), (right, _)| left.cmp(right));

        let mut scan = FileBackendRangeScan {
            rows,
            position: 0,
            projection: opts.projection,
        };
        f(&mut scan)
    }
}

impl BackendRangeScan for FileBackendRangeScan {
    fn visit_next<V>(
        &mut self,
        limit_rows: usize,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        if limit_rows == 0 {
            return Ok(ScanResult {
                emitted: 0,
                has_more: self.position < self.rows.len(),
            });
        }

        let mut emitted = 0usize;
        while emitted < limit_rows {
            let Some((key, value)) = self.rows.get(self.position) else {
                break;
            };
            visitor.visit(key.as_ref(), project_value_ref(value, self.projection))?;
            self.position += 1;
            emitted += 1;
        }

        Ok(ScanResult {
            emitted,
            has_more: self.position < self.rows.len(),
        })
    }
}

impl BackendWrite for FileBackendWrite {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        for entry in entries.entries {
            self.stats.put_entries += 1;
            self.stats.written_bytes += entry.value.bytes.len() as u64;
            self.kv
                .insert(entry.key.0.to_vec(), stored_value_bytes(entry.value));
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError> {
        for key in keys {
            self.kv.remove(key.0.as_ref());
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
        let before = self.kv.len();
        self.kv
            .retain(|key, _| !key_matches_range(key, &range, None));
        self.stats.deleted_entries += (before - self.kv.len()) as u64;
        self.stats.deleted_ranges += 1;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn commit(mut self) -> Result<CommitResult, BackendError> {
        write_kv_file(&self.path, &self.kv).map_err(lix_to_backend_error)?;
        *self
            .parent
            .lock()
            .map_err(|_| backend_lock_error("cli file backend kv"))? = self.kv.clone();
        self.closed = true;
        self.clear_write_active();
        Ok(CommitResult {
            commit_id: None,
            stats: self.stats.clone(),
        })
    }

    fn rollback(mut self) -> Result<(), BackendError> {
        self.closed = true;
        self.clear_write_active();
        Ok(())
    }
}

impl FileBackendWrite {
    fn clear_write_active(&self) {
        if let Ok(mut active) = self.write_active.lock() {
            *active = false;
        }
    }
}

impl Drop for FileBackendWrite {
    fn drop(&mut self) {
        if !self.closed {
            self.clear_write_active();
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct FileSnapshot {
    entries: Vec<FileEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct FileEntry {
    namespace: String,
    key: String,
    value: String,
}

fn read_kv_file(path: &Path) -> Result<KvMap, CliError> {
    if !path.exists() {
        return Ok(KvMap::new());
    }
    let bytes = fs::read(path).map_err(|source| CliError::io("failed to read lix file", source))?;
    if bytes.is_empty() {
        return Ok(KvMap::new());
    }
    let snapshot: FileSnapshot = serde_json::from_slice(&bytes)
        .map_err(|error| CliError::msg(format!("failed to decode lix file: {error}")))?;
    let mut kv = KvMap::new();
    for entry in snapshot.entries {
        if !entry.namespace.is_empty() {
            return Err(CliError::msg(format!(
                "unsupported legacy lix namespace '{}' in {}",
                entry.namespace,
                path.display()
            )));
        }
        kv.insert(decode_bytes(&entry.key)?, decode_bytes(&entry.value)?);
    }
    Ok(kv)
}

fn write_kv_file(path: &Path, kv: &KvMap) -> Result<(), LixError> {
    let snapshot = FileSnapshot {
        entries: kv
            .iter()
            .map(|(key, value)| FileEntry {
                namespace: String::new(),
                key: encode_bytes(key),
                value: encode_bytes(value),
            })
            .collect(),
    };
    let bytes = serde_json::to_vec(&snapshot).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to encode lix file snapshot: {error}"),
        )
    })?;
    fs::write(path, bytes).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to write lix file '{}': {error}", path.display()),
        )
    })
}

fn key_matches_range(key: &[u8], range: &KeyRange, resume_after: Option<&Key>) -> bool {
    if let Some(resume_after) = resume_after {
        if key <= resume_after.0.as_ref() {
            return false;
        }
    }

    let lower_matches = match &range.lower {
        Bound::Included(lower) => key >= lower.0.as_ref(),
        Bound::Excluded(lower) => key > lower.0.as_ref(),
        Bound::Unbounded => true,
    };
    let upper_matches = match &range.upper {
        Bound::Included(upper) => key <= upper.0.as_ref(),
        Bound::Excluded(upper) => key < upper.0.as_ref(),
        Bound::Unbounded => true,
    };
    lower_matches && upper_matches
}

fn project_value_ref(value: &[u8], projection: CoreProjection) -> ProjectedValueRef<'_> {
    match projection {
        CoreProjection::KeyOnly => ProjectedValueRef::KeyOnly,
        CoreProjection::FullValue => ProjectedValueRef::FullValue(value),
    }
}

fn stored_value_bytes(value: StoredValue) -> Vec<u8> {
    value.bytes.to_vec()
}

fn encode_bytes(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn decode_bytes(value: &str) -> Result<Vec<u8>, CliError> {
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|error| CliError::msg(format!("failed to decode lix file bytes: {error}")))
}

fn backend_lock_error(name: &str) -> BackendError {
    BackendError::Io(format!("{name} mutex was poisoned"))
}

fn lix_to_backend_error(error: LixError) -> BackendError {
    BackendError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{init_lix_at, prepare_lix_output_path, resolve_db_path};
    use crate::app::AppContext;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn resolve_db_path_rejects_explicit_non_lix_path() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("temp dir should be created");
        let path = temp_dir.join("project.sqlite");
        fs::write(&path, b"not-lix").expect("seed file should be written");
        let context = AppContext {
            lix_path: Some(path.clone()),
            no_hints: false,
        };

        let error = resolve_db_path(&context).expect_err("non-.lix path should be rejected");
        assert_eq!(
            error.to_string(),
            format!("expected a .lix file path: {}", path.display())
        );

        fs::remove_file(&path).expect("seed file should be removable");
        fs::remove_dir_all(&temp_dir).expect("temp dir should be removable");
    }

    #[test]
    fn init_lix_at_rejects_non_lix_path() {
        let temp_dir = unique_temp_dir();
        let path = temp_dir.join("project.sqlite");

        let error = init_lix_at(&path).expect_err("non-.lix init path should be rejected");
        assert_eq!(
            error.to_string(),
            format!("expected a .lix file path: {}", path.display())
        );
        assert!(
            !temp_dir.exists(),
            "validator should reject before creating parent directories"
        );
    }

    #[test]
    fn prepare_output_path_rejects_non_lix_path() {
        let temp_dir = unique_temp_dir();
        let path = temp_dir.join("output.db");

        let error = prepare_lix_output_path(&path, false)
            .expect_err("non-.lix output path should be rejected");
        assert_eq!(
            error.to_string(),
            format!("expected a .lix file path: {}", path.display())
        );
        assert!(
            !temp_dir.exists(),
            "validator should reject before creating parent directories"
        );
    }

    fn unique_temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("lix-cli-db-test-{}-{nanos}", std::process::id()))
    }
}
