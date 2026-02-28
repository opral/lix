use crate::app::AppContext;
use crate::error::CliError;
use lix_rs_sdk::{open_lix, Lix, OpenLixConfig, SqliteBackend};
use std::fs;
use std::path::{Path, PathBuf};

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

    let config = OpenLixConfig {
        backend: Some(Box::new(backend)),
        ..Default::default()
    };

    pollster::block_on(open_lix(config)).map_err(|err| {
        CliError::msg(format!(
            "failed to open lix database at {}: {}",
            path.display(),
            err
        ))
    })
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
