use crate::app::AppContext;
use crate::error::CliError;
use async_trait::async_trait;
use base64::Engine as _;
use lix_rs_sdk::{
    open_lix, KvPair, KvScanRange, Lix, LixBackend, LixBackendTransaction, LixError,
    OpenLixOptions, TransactionBeginMode,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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

pub fn open_lix_at(path: &Path) -> Result<Lix, CliError> {
    let backend = FileBackend::from_path(path)?;

    block_on(open_lix(OpenLixOptions {
        backend: Some(Box::new(backend)),
    }))
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

type KvMap = BTreeMap<(String, Vec<u8>), Vec<u8>>;

#[derive(Clone)]
struct FileBackend {
    path: Arc<PathBuf>,
    kv: Arc<Mutex<KvMap>>,
}

impl FileBackend {
    fn from_path(path: &Path) -> Result<Self, CliError> {
        let kv = read_kv_file(path)?;
        Ok(Self {
            path: Arc::new(path.to_path_buf()),
            kv: Arc::new(Mutex::new(kv)),
        })
    }
}

#[async_trait]
impl LixBackend for FileBackend {
    async fn begin_transaction(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + Send + Sync + 'static>, LixError> {
        let snapshot = self
            .kv
            .lock()
            .map_err(|_| lock_error("cli file backend kv"))?
            .clone();
        Ok(Box::new(FileBackendTransaction {
            mode,
            path: Arc::clone(&self.path),
            parent: Arc::clone(&self.kv),
            kv: snapshot,
        }))
    }

    async fn kv_get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self
            .kv
            .lock()
            .map_err(|_| lock_error("cli file backend kv"))?
            .get(&(namespace.to_string(), key.to_vec()))
            .cloned())
    }

    async fn kv_scan(
        &self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        let guard = self
            .kv
            .lock()
            .map_err(|_| lock_error("cli file backend kv"))?;
        Ok(scan_map(&guard, namespace, &range, limit))
    }
}

struct FileBackendTransaction {
    mode: TransactionBeginMode,
    path: Arc<PathBuf>,
    parent: Arc<Mutex<KvMap>>,
    kv: KvMap,
}

#[async_trait]
impl LixBackendTransaction for FileBackendTransaction {
    fn mode(&self) -> TransactionBeginMode {
        self.mode
    }

    async fn kv_get(&mut self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self.kv.get(&(namespace.to_string(), key.to_vec())).cloned())
    }

    async fn kv_scan(
        &mut self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        Ok(scan_map(&self.kv, namespace, &range, limit))
    }

    async fn kv_put(&mut self, namespace: &str, key: &[u8], value: &[u8]) -> Result<(), LixError> {
        self.kv
            .insert((namespace.to_string(), key.to_vec()), value.to_vec());
        Ok(())
    }

    async fn kv_delete(&mut self, namespace: &str, key: &[u8]) -> Result<(), LixError> {
        self.kv.remove(&(namespace.to_string(), key.to_vec()));
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        write_kv_file(&self.path, &self.kv)?;
        *self
            .parent
            .lock()
            .map_err(|_| lock_error("cli file backend kv"))? = self.kv;
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        Ok(())
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
        kv.insert(
            (entry.namespace, decode_bytes(&entry.key)?),
            decode_bytes(&entry.value)?,
        );
    }
    Ok(kv)
}

fn write_kv_file(path: &Path, kv: &KvMap) -> Result<(), LixError> {
    let snapshot = FileSnapshot {
        entries: kv
            .iter()
            .map(|((namespace, key), value)| FileEntry {
                namespace: namespace.clone(),
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

fn scan_map(kv: &KvMap, namespace: &str, range: &KvScanRange, limit: Option<usize>) -> Vec<KvPair> {
    let mut pairs = kv
        .iter()
        .filter(|((candidate_namespace, key), _)| {
            candidate_namespace == namespace && key_matches_range(key, range)
        })
        .map(|((_, key), value)| KvPair::new(key.clone(), value.clone()))
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.key.cmp(&right.key));
    if let Some(limit) = limit {
        pairs.truncate(limit);
    }
    pairs
}

fn key_matches_range(key: &[u8], range: &KvScanRange) -> bool {
    match range {
        KvScanRange::Prefix(prefix) => key.starts_with(prefix),
        KvScanRange::Range { start, end } => start.as_slice() <= key && key < end.as_slice(),
    }
}

fn encode_bytes(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn decode_bytes(value: &str) -> Result<Vec<u8>, CliError> {
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|error| CliError::msg(format!("failed to decode lix file bytes: {error}")))
}

fn lock_error(name: &str) -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", format!("{name} mutex was poisoned"))
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
