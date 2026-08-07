use crate::app::AppContext;
use crate::error::CliError;
use lix::{Lix, open_lix};
use lix_storage_sqlite::SQLite;
use std::fs;
use std::future::{Future, IntoFuture};
use std::path::{Path, PathBuf};

pub type SqliteLix = Lix<SQLite>;

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

pub fn open_lix_at(path: &Path) -> Result<SqliteLix, CliError> {
    let storage = SQLite::open(path).map_err(|error| {
        CliError::msg(format!("failed to open lix at {}: {error}", path.display()))
    })?;

    block_on(open_lix().with_storage(storage))
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

fn remove_sidecar(path: &Path, suffix: &str) -> Result<(), CliError> {
    let sidecar = PathBuf::from(format!("{}-{suffix}", path.display()));
    match fs::remove_file(sidecar) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(CliError::io("failed to destroy lix sidecar file", error)),
    }
}

#[cfg(test)]
mod tests {
    use super::{block_on, init_lix_at, open_lix_at, prepare_lix_output_path, resolve_db_path};
    use crate::app::AppContext;
    use lix::Value;
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
    fn sqlite_lix_persists_across_fresh_opens() {
        run_on_large_stack("sqlite-lix-reopen", || {
            let temp_dir = unique_temp_dir();
            let path = temp_dir.join("workspace.lix");

            assert!(init_lix_at(&path).expect("SQLite .lix file should initialize"));
            let header = fs::read(&path).expect("initialized .lix file should be readable");
            assert!(
                header.starts_with(b"SQLite format 3\0"),
                "the .lix file must be owned directly by SQLite"
            );

            let lix = open_lix_at(&path).expect("initialized SQLite .lix file should open");
            block_on(lix.execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('persisted', 'yes')",
                &[],
            ))
            .expect("local mutation should commit");
            block_on(lix.close()).expect("first handle should close");
            drop(lix);

            let reopened = open_lix_at(&path).expect("SQLite .lix file should reopen");
            let result = block_on(reopened.execute(
                "SELECT value FROM lix_key_value WHERE key = 'persisted'",
                &[],
            ))
            .expect("persisted row should read after reopen");
            assert_eq!(
                result.rows()[0].get_index(0),
                Some(&Value::Json(serde_json::Value::String("yes".to_string())))
            );
            block_on(reopened.close()).expect("reopened handle should close");

            cleanup_lix_path(&path);
            fs::remove_dir_all(&temp_dir).expect("temp dir should be removable");
        });
    }

    #[test]
    fn legacy_json_snapshot_is_rejected_without_migration_or_fallback() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("temp dir should be created");
        let path = temp_dir.join("legacy.lix");
        let legacy = br#"{"entries":[{"key":"a2V5","value":"dmFsdWU="}]}"#;
        fs::write(&path, legacy).expect("legacy JSON snapshot should be written");

        let error = match open_lix_at(&path) {
            Ok(_) => panic!("legacy JSON must not be migrated or opened"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("not a database"),
            "unexpected legacy JSON error: {error}"
        );
        assert_eq!(
            fs::read(&path).expect("rejected legacy file should remain readable"),
            legacy,
            "a rejected legacy file must remain byte-for-byte unchanged"
        );

        cleanup_lix_path(&path);
        fs::remove_dir_all(&temp_dir).expect("temp dir should be removable");
    }

    #[test]
    fn corrupt_sqlite_lix_is_rejected_without_reinitialization() {
        run_on_large_stack("sqlite-lix-corruption", || {
            let temp_dir = unique_temp_dir();
            let path = temp_dir.join("corrupt.lix");
            init_lix_at(&path).expect("SQLite .lix file should initialize");
            remove_sidecars_for_test(&path);

            let corrupt = b"not a sqlite database";
            fs::write(&path, corrupt).expect("SQLite fixture should be corrupted");

            let error = match open_lix_at(&path) {
                Ok(_) => panic!("corrupt SQLite .lix file must fail closed"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains("not a database"),
                "unexpected corruption error: {error}"
            );
            assert_eq!(
                fs::read(&path).expect("corrupt .lix file should remain readable"),
                corrupt,
                "opening corruption must not silently reinitialize the file"
            );

            cleanup_lix_path(&path);
            fs::remove_dir_all(&temp_dir).expect("temp dir should be removable");
        });
    }

    fn run_on_large_stack(name: &str, work: impl FnOnce() + Send + 'static) {
        std::thread::Builder::new()
            .name(name.to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(work)
            .expect("test thread should spawn")
            .join()
            .expect("test thread should complete");
    }

    fn remove_sidecars_for_test(path: &std::path::Path) {
        for suffix in ["wal", "shm", "journal"] {
            let _ = fs::remove_file(format!("{}-{suffix}", path.display()));
        }
    }

    fn cleanup_lix_path(path: &std::path::Path) {
        let _ = fs::remove_file(path);
        remove_sidecars_for_test(path);
    }

    fn unique_temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("lix-cli-db-test-{}-{nanos}", std::process::id()))
    }
}
