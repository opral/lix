use crate::app::AppContext;
use crate::error::CliError;
use lix::{Lix, open_lix};
use lix_storage_rocksdb::RocksDB;
use std::fs;
use std::future::{Future, IntoFuture};
use std::path::{Path, PathBuf};

pub type LocalLix = Lix<RocksDB>;

pub fn resolve_db_path(context: &AppContext) -> Result<PathBuf, CliError> {
    if let Some(path) = &context.lix_path {
        validate_lix_path(path)?;
        if !path.is_dir() {
            return Err(CliError::msg(format!(
                "lix store does not exist: {}",
                path.display()
            )));
        }
        return Ok(path.clone());
    }

    let cwd =
        std::env::current_dir().map_err(|source| CliError::io("failed to read cwd", source))?;
    let mut candidates = find_lix_stores(&cwd)?;

    if candidates.is_empty() {
        return Err(CliError::msg(
            "no .lix stores found in current directory; pass --path <path-to-store.lix>",
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
            "multiple .lix stores found ({paths}); pass --path <path-to-store.lix>"
        )));
    }

    Ok(candidates.remove(0))
}

pub fn open_lix_at(path: &Path) -> Result<LocalLix, CliError> {
    validate_lix_path(path)?;
    let storage = RocksDB::open(path).map_err(|error| {
        CliError::msg(format!(
            "failed to open RocksDB lix store at {}: {error}",
            path.display()
        ))
    })?;

    block_on(open_lix().with_storage(storage)).map_err(|error| {
        CliError::msg(format!("failed to open lix at {}: {error}", path.display()))
    })
}

pub fn init_lix_at(path: &Path) -> Result<bool, CliError> {
    validate_lix_path(path)?;

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| {
            CliError::io("failed to create parent directory for lix store", source)
        })?;
    }

    let initialized = !path.exists();
    let _ = open_lix_at(path)?;
    Ok(initialized)
}

pub fn destroy_lix_at(path: &Path) -> Result<(), CliError> {
    if !path.exists() {
        return Ok(());
    }
    if !path.is_dir() {
        return Err(CliError::msg(format!(
            "expected a directory-backed .lix store: {}",
            path.display()
        )));
    }
    fs::remove_dir_all(path)
        .map_err(|source| CliError::io("failed to destroy lix store directory", source))
}

/// Prepares a directory-backed `.lix` RocksDB target for initialization.
pub fn prepare_lix_output_path(path: &Path, force: bool) -> Result<(), CliError> {
    validate_lix_path(path)?;

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .map_err(|source| CliError::io("failed to create output directory", source))?;
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

fn find_lix_stores(cwd: &Path) -> Result<Vec<PathBuf>, CliError> {
    let mut stores = Vec::new();
    let entries =
        fs::read_dir(cwd).map_err(|source| CliError::io("failed to read cwd entries", source))?;
    for entry in entries {
        let entry =
            entry.map_err(|source| CliError::io("failed to read directory entry", source))?;
        let path = entry.path();
        if path.is_dir() && path.extension().and_then(|ext| ext.to_str()) == Some("lix") {
            stores.push(path);
        }
    }
    stores.sort();
    Ok(stores)
}

fn validate_lix_path(path: &Path) -> Result<(), CliError> {
    if path.extension().and_then(|ext| ext.to_str()) == Some("lix") {
        return Ok(());
    }

    Err(CliError::msg(format!(
        "expected a .lix store path: {}",
        path.display()
    )))
}

pub fn block_on<F>(future: F) -> F::Output
where
    F: IntoFuture,
    F::IntoFuture: Future<Output = F::Output>,
{
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should initialize")
        .block_on(future.into_future())
}

#[cfg(test)]
mod tests {
    use super::{
        destroy_lix_at, init_lix_at, open_lix_at, prepare_lix_output_path, resolve_db_path,
    };
    use crate::app::AppContext;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn resolve_db_path_rejects_explicit_non_lix_path() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("temp dir should be created");
        let path = temp_dir.join("project.sqlite");
        fs::create_dir(&path).expect("seed directory should be created");
        let context = AppContext {
            lix_path: Some(path.clone()),
            no_hints: false,
        };

        let error = resolve_db_path(&context).expect_err("non-.lix path should be rejected");
        assert_eq!(
            error.to_string(),
            format!("expected a .lix store path: {}", path.display())
        );

        fs::remove_dir_all(&temp_dir).expect("temp dir should be removable");
    }

    #[test]
    fn init_lix_at_rejects_non_lix_path() {
        let temp_dir = unique_temp_dir();
        let path = temp_dir.join("project.sqlite");

        let error = init_lix_at(&path).expect_err("non-.lix path should be rejected");
        assert_eq!(
            error.to_string(),
            format!("expected a .lix store path: {}", path.display())
        );
        assert!(!temp_dir.exists());
    }

    #[test]
    fn prepare_output_path_rejects_non_lix_path() {
        let temp_dir = unique_temp_dir();
        let path = temp_dir.join("output.db");

        let error =
            prepare_lix_output_path(&path, false).expect_err("non-.lix path should be rejected");
        assert_eq!(
            error.to_string(),
            format!("expected a .lix store path: {}", path.display())
        );
        assert!(!temp_dir.exists());
    }

    #[test]
    fn rocksdb_directory_initializes_and_reopens() {
        let temp_dir = unique_temp_dir();
        let path = temp_dir.join("project.lix");
        assert!(init_lix_at(&path).expect("initialize RocksDB lix store"));
        assert!(path.is_dir());
        assert!(!init_lix_at(&path).expect("reopen initialized RocksDB lix store"));
        drop(open_lix_at(&path).expect("open initialized RocksDB lix store"));
        destroy_lix_at(&path).expect("destroy RocksDB lix store");
        fs::remove_dir_all(&temp_dir).expect("temp parent should be removable");
    }

    fn unique_temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("lix-cli-db-test-{}-{nanos}", std::process::id()))
    }
}
