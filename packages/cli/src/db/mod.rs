#![allow(
    clippy::manual_async_fn,
    reason = "explicit future signatures mirror Storage traits and keep Send guarantees visible"
)]

use crate::app::AppContext;
use crate::error::CliError;
use base64::Engine as _;
use bytes::Bytes;
use lix_sdk::{
    CommitResult, CoreProjection, GetManyResult, GetOptions, Key, KeyRange, Lix, LixError,
    ProjectedValue, PutBatch, ReadEntry, ReadOptions, ScanChunk, ScanOptions, SpaceId, Storage,
    StorageError, StorageRead, StorageWrite, StoredValue, WriteOptions, WriteStats,
    open_lix_with_storage,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub type FileLix = Lix<FileStorage>;

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
    let storage = FileStorage::from_path(path)?;

    block_on(open_lix_with_storage(storage))
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
    .and_then(|()| remove_sidecar(path, "wal"))
    .and_then(|()| remove_sidecar(path, "shm"))
    .and_then(|()| remove_sidecar(path, "journal"))
}

/// Prepares a `.lix` output target for initialization.
///
/// The CLI delegates storage-backed cleanup to the storage boundary so command
/// code does not need to know how a storage represents its physical artifacts.
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

pub fn block_on<F: Future>(future: F) -> F::Output {
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

// TODO: Replace this custom whole-file KV storage with SQLite for the CLI.
// This storage exists only as a transitional local `.lix` file adapter. It
// snapshots the entire file into memory and rewrites it on commit, so it is not
// a real concurrent storage implementation. The instance-local write gate below only
// rejects overlapping writes through the same opened handle; separate handles
// or processes can still overwrite each other. SQLite should own CLI durability
// and write concurrency instead.
#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct FileStorage {
    path: Arc<PathBuf>,
    kv: Arc<Mutex<KvMap>>,
    write_active: Arc<Mutex<bool>>,
}

impl FileStorage {
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
#[expect(missing_debug_implementations)]
pub struct FileStorageRead {
    kv: KvMap,
}

#[allow(missing_debug_implementations)]
pub struct FileStorageWrite {
    path: Arc<PathBuf>,
    parent: Arc<Mutex<KvMap>>,
    write_active: Arc<Mutex<bool>>,
    kv: KvMap,
    stats: WriteStats,
    closed: bool,
}

impl Storage for FileStorage {
    type Read<'a>
        = FileStorageRead
    where
        Self: 'a;

    type Write<'a>
        = FileStorageWrite
    where
        Self: 'a;
    fn begin_read(
        &self,
        _opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        async move {
            Ok(FileStorageRead {
                kv: self
                    .kv
                    .lock()
                    .map_err(|_| storage_lock_error("cli file storage kv"))?
                    .clone(),
            })
        }
    }

    fn begin_write(
        &self,
        _opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        async move {
            {
                let mut active = self
                    .write_active
                    .lock()
                    .map_err(|_| storage_lock_error("cli file storage write gate"))?;
                if *active {
                    return Err(StorageError::Io(
                        "cli file storage write transaction already active".to_string(),
                    ));
                }
                *active = true;
            }
            let kv = match self
                .kv
                .lock()
                .map_err(|_| storage_lock_error("cli file storage kv"))
                .map(|parent| parent.clone())
            {
                Ok(kv) => kv,
                Err(error) => {
                    self.clear_write_active();
                    return Err(error);
                }
            };
            Ok(FileStorageWrite {
                path: Arc::clone(&self.path),
                parent: Arc::clone(&self.kv),
                write_active: Arc::clone(&self.write_active),
                kv,
                stats: WriteStats::default(),
                closed: false,
            })
        }
    }
}

impl FileStorage {
    fn clear_write_active(&self) {
        if let Ok(mut active) = self.write_active.lock() {
            *active = false;
        }
    }
}

/// The CLI file storage keeps one flat map; spaces are scoped by prefixing
/// the 4-byte big-endian space id internally. Reads return logical keys.
fn physical_key(space: SpaceId, key: &Key) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4 + key.0.len());
    bytes.extend_from_slice(&space.0.to_be_bytes());
    bytes.extend_from_slice(&key.0);
    bytes
}

fn physical_range(space: SpaceId, range: KeyRange) -> KeyRange {
    let map = |bound: Bound<Key>, unbounded: Bound<Key>| match bound {
        Bound::Included(key) => Bound::Included(Key(Bytes::from(physical_key(space, &key)))),
        Bound::Excluded(key) => Bound::Excluded(Key(Bytes::from(physical_key(space, &key)))),
        Bound::Unbounded => unbounded,
    };
    KeyRange {
        lower: map(
            range.lower,
            Bound::Included(Key(Bytes::copy_from_slice(&space.0.to_be_bytes()))),
        ),
        upper: map(
            range.upper,
            space.0.checked_add(1).map_or(Bound::Unbounded, |next| {
                Bound::Excluded(Key(Bytes::copy_from_slice(&next.to_be_bytes())))
            }),
        ),
    }
}

impl StorageRead for FileStorageRead {
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        async move {
            Ok(GetManyResult::new(
                keys.iter()
                    .map(|key| {
                        self.kv
                            .get(physical_key(space, key).as_slice())
                            .map(|value| project_value(value, opts.projection))
                    })
                    .collect(),
            ))
        }
    }

    fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> impl Future<Output = Result<ScanChunk, StorageError>> + Send {
        async move {
            if opts.page_size() == 0 {
                return Ok(ScanChunk {
                    entries: Vec::new(),
                    has_more: false,
                });
            }
            let range = physical_range(space, range);
            let resume_after = opts
                .resume_after
                .as_ref()
                .map(|key| Key(Bytes::from(physical_key(space, key))));
            let mut rows = self
                .kv
                .iter()
                .filter(|(key, _)| key_matches_range(key, &range, resume_after.as_ref()));
            let entries = rows
                .by_ref()
                .take(opts.page_size())
                .map(|(key, value)| ReadEntry {
                    key: Key(Bytes::copy_from_slice(&key[4..])),
                    value: project_value(value, opts.projection),
                })
                .collect();
            Ok(ScanChunk {
                entries,
                has_more: rows.next().is_some(),
            })
        }
    }
}

impl StorageWrite for FileStorageWrite {
    fn put_many(
        &mut self,
        space: SpaceId,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            for entry in entries.entries {
                self.stats.put_entries += 1;
                self.stats.written_bytes += entry.value.bytes.len() as u64;
                self.kv.insert(
                    physical_key(space, &entry.key),
                    stored_value_bytes(entry.value),
                );
            }
            self.stats.storage_calls += 1;
            Ok(())
        }
    }

    fn delete_many(
        &mut self,
        space: SpaceId,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            for key in keys {
                self.kv.remove(physical_key(space, key).as_slice());
            }
            self.stats.deleted_entries += keys.len() as u64;
            self.stats.storage_calls += 1;
            Ok(())
        }
    }

    fn delete_range(
        &mut self,
        space: SpaceId,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            let range = physical_range(space, range);
            let before = self.kv.len();
            self.kv
                .retain(|key, _| !key_matches_range(key, &range, None));
            self.stats.deleted_entries += (before - self.kv.len()) as u64;
            self.stats.deleted_ranges += 1;
            self.stats.storage_calls += 1;
            Ok(())
        }
    }

    fn commit(mut self) -> impl Future<Output = Result<CommitResult, StorageError>> + Send {
        async move {
            write_kv_file(&self.path, &self.kv).map_err(lix_to_storage_error)?;
            *self
                .parent
                .lock()
                .map_err(|_| storage_lock_error("cli file storage kv"))? = self.kv.clone();
            self.closed = true;
            self.clear_write_active();
            Ok(CommitResult {
                commit_id: None,
                stats: self.stats,
            })
        }
    }

    fn rollback(mut self) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            self.closed = true;
            self.clear_write_active();
            Ok(())
        }
    }
}

impl FileStorageWrite {
    fn clear_write_active(&self) {
        if let Ok(mut active) = self.write_active.lock() {
            *active = false;
        }
    }
}

impl Drop for FileStorageWrite {
    fn drop(&mut self) {
        if !self.closed {
            self.clear_write_active();
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileSnapshot {
    entries: Vec<FileEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileEntry {
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
        kv.insert(decode_bytes(&entry.key)?, decode_bytes(&entry.value)?);
    }
    Ok(kv)
}

fn write_kv_file(path: &Path, kv: &KvMap) -> Result<(), LixError> {
    let snapshot = FileSnapshot {
        entries: kv
            .iter()
            .map(|(key, value)| FileEntry {
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

fn project_value(value: &[u8], projection: CoreProjection) -> ProjectedValue {
    match projection {
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        CoreProjection::FullValue => ProjectedValue::FullValue(Bytes::copy_from_slice(value)),
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

fn storage_lock_error(name: &str) -> StorageError {
    StorageError::Io(format!("{name} mutex was poisoned"))
}

fn lix_to_storage_error(error: LixError) -> StorageError {
    StorageError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{init_lix_at, prepare_lix_output_path, read_kv_file, resolve_db_path};
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

    #[test]
    fn file_storage_rejects_removed_namespace_field() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("temp dir should be created");
        let path = temp_dir.join("legacy.lix");
        fs::write(
            &path,
            r#"{"entries":[{"namespace":"legacy","key":"a2V5","value":"dmFsdWU="}]}"#,
        )
        .expect("legacy snapshot should be written");

        let error = read_kv_file(&path).expect_err("removed namespace field should be rejected");
        assert!(error.to_string().contains("unknown field `namespace`"));

        fs::remove_dir_all(&temp_dir).expect("temp dir should be removable");
    }

    fn unique_temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("lix-cli-db-test-{}-{nanos}", std::process::id()))
    }
}
