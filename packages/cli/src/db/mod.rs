use crate::app::AppContext;
use crate::error::CliError;
use lix_rs_sdk::{Lix, LixConfig, SqliteBackend, WasmtimeRuntime};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn resolve_db_path(context: &AppContext) -> Result<PathBuf, CliError> {
    if let Some(path) = &context.lix_path {
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
    let backend = SqliteBackend::from_path(path).map_err(|err| {
        CliError::msg(format!(
            "failed to open sqlite backend at {}: {}",
            path.display(),
            err
        ))
    })?;

    let config = LixConfig::new(Box::new(backend), default_wasm_runtime()?);

    pollster::block_on(Lix::open(config)).map_err(|err| {
        CliError::msg(format!(
            "failed to open lix database at {}: {}",
            path.display(),
            err
        ))
    })
}

pub fn init_lix_at(path: &Path) -> Result<bool, CliError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|source| {
                CliError::io("failed to create parent directory for lix file", source)
            })?;
        }
    }

    let init_backend = SqliteBackend::from_path(path).map_err(|err| {
        CliError::msg(format!(
            "failed to open sqlite backend at {}: {}",
            path.display(),
            err
        ))
    })?;
    let init_config = LixConfig::new(Box::new(init_backend), default_wasm_runtime()?);
    let result = pollster::block_on(Lix::init(init_config)).map_err(|err| {
        CliError::msg(format!(
            "failed to initialize lix database at {}: {}",
            path.display(),
            err
        ))
    })?;
    Ok(result.initialized)
}

pub fn destroy_lix_at(path: &Path) -> Result<(), CliError> {
    SqliteBackend::destroy_path(path).map_err(|err| {
        CliError::msg(format!(
            "failed to destroy sqlite backend at {}: {}",
            path.display(),
            err
        ))
    })
}

/// Prepares a `.lix` output target for initialization.
///
/// The CLI delegates storage-backed cleanup to the backend boundary so command
/// code does not need to know how a backend represents its physical artifacts.
pub fn prepare_lix_output_path(path: &Path, force: bool) -> Result<(), CliError> {
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

fn default_wasm_runtime() -> Result<Arc<WasmtimeRuntime>, CliError> {
    WasmtimeRuntime::new()
        .map(Arc::new)
        .map_err(|err| CliError::msg(format!("failed to initialize wasmtime runtime: {err}")))
}
