use std::io::{Cursor, Read};
use std::path::{Component, Path};

use crate::common::LixError;
use crate::contracts::{parse_plugin_manifest_json, InstalledPlugin, WasmComponentInstance};
use zip::read::ZipArchive;

const APPLY_CHANGES_EXPORTS: &[&str] = &["apply-changes", "api#apply-changes"];

pub(crate) async fn call_apply_changes(
    instance: &dyn WasmComponentInstance,
    payload: &[u8],
) -> Result<Vec<u8>, LixError> {
    let mut errors = Vec::new();
    for export in APPLY_CHANGES_EXPORTS {
        match instance.call(export, payload).await {
            Ok(output) => return Ok(output),
            Err(error) => errors.push(format!("{export}: {}", error.description)),
        }
    }

    Err(LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "plugin materialization: failed to call apply-changes export ({})",
            errors.join("; ")
        ),
    })
}

pub(crate) fn parse_installed_plugin_from_archive_bytes(
    plugin_key: &str,
    archive_path: &str,
    archive_bytes: &[u8],
) -> Result<InstalledPlugin, LixError> {
    let files = read_plugin_archive_files(archive_path, archive_bytes)?;
    let manifest_bytes = files.get("manifest.json").ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "plugin materialization: archive '{}' is missing manifest.json",
            archive_path
        ),
    })?;
    let manifest_raw = std::str::from_utf8(manifest_bytes).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "plugin materialization: archive '{}' manifest.json must be UTF-8: {error}",
            archive_path
        ),
    })?;
    let validated_manifest = parse_plugin_manifest_json(manifest_raw)?;
    if validated_manifest.manifest.key != plugin_key {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: archive '{}' key mismatch: path key '{}' vs manifest key '{}'",
                archive_path, plugin_key, validated_manifest.manifest.key
            ),
        });
    }

    let entry_path = normalize_plugin_archive_path(&validated_manifest.manifest.entry)?;
    let wasm = files.get(&entry_path).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "plugin materialization: archive '{}' is missing entry file '{}'",
            archive_path, validated_manifest.manifest.entry
        ),
    })?;
    ensure_valid_plugin_wasm(wasm)?;

    let manifest = validated_manifest.manifest;
    let content_type = manifest.file_match.content_type;

    Ok(InstalledPlugin {
        key: manifest.key,
        runtime: manifest.runtime,
        api_version: manifest.api_version,
        path_glob: manifest.file_match.path_glob,
        content_type,
        entry: manifest.entry,
        manifest_json: validated_manifest.normalized_json,
        wasm: wasm.clone(),
    })
}

fn read_plugin_archive_files(
    archive_path: &str,
    archive_bytes: &[u8],
) -> Result<std::collections::BTreeMap<String, Vec<u8>>, LixError> {
    if archive_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: archive '{}' is empty",
                archive_path
            ),
        });
    }

    let mut archive = ZipArchive::new(Cursor::new(archive_bytes)).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "plugin materialization: archive '{}' is not a valid zip file: {error}",
            archive_path
        ),
    })?;
    let mut files = std::collections::BTreeMap::<String, Vec<u8>>::new();

    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: failed to read archive '{}' entry index {}: {error}",
                archive_path, index
            ),
        })?;
        let raw_name = entry.name().to_string();
        if entry.is_dir() {
            continue;
        }
        if is_plugin_archive_symlink(entry.unix_mode()) {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "plugin materialization: archive '{}' entry '{}' must not be a symlink",
                    archive_path, raw_name
                ),
            });
        }
        let normalized_name = normalize_plugin_archive_path(&raw_name)?;
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: failed to read archive '{}' entry '{}': {error}",
                archive_path, raw_name
            ),
        })?;
        if files.insert(normalized_name.clone(), bytes).is_some() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "plugin materialization: archive '{}' contains duplicate entry '{}'",
                    archive_path, normalized_name
                ),
            });
        }
    }

    Ok(files)
}

fn is_plugin_archive_symlink(mode: Option<u32>) -> bool {
    const MODE_FILE_TYPE_MASK: u32 = 0o170000;
    const MODE_SYMLINK: u32 = 0o120000;
    mode.is_some_and(|value| (value & MODE_FILE_TYPE_MASK) == MODE_SYMLINK)
}

fn normalize_plugin_archive_path(path: &str) -> Result<String, LixError> {
    if path.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "plugin archive path must not be empty".to_string(),
        });
    }
    if path.starts_with('/') || path.starts_with('\\') {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("plugin archive path '{}' must be relative", path),
        });
    }
    if path.contains('\\') {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin archive path '{}' must use forward slash separators",
                path
            ),
        });
    }

    let mut segments = Vec::<String>::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(value) => {
                let segment = value.to_str().ok_or_else(|| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "plugin archive path '{}' contains non-UTF-8 components",
                        path
                    ),
                })?;
                if segment.is_empty() {
                    return Err(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: format!("plugin archive path '{}' is invalid", path),
                    });
                }
                segments.push(segment.to_string());
            }
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "plugin archive path '{}' must not contain traversal or absolute components",
                        path
                    ),
                });
            }
        }
    }

    if segments.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("plugin archive path '{}' is invalid", path),
        });
    }
    Ok(segments.join("/"))
}

fn ensure_valid_plugin_wasm(wasm_bytes: &[u8]) -> Result<(), LixError> {
    if wasm_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "plugin materialization: wasm bytes must not be empty".to_string(),
        });
    }
    if wasm_bytes.len() < 8 || !wasm_bytes.starts_with(&[0x00, 0x61, 0x73, 0x6d]) {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "plugin materialization: wasm bytes must start with a valid wasm header"
                .to_string(),
        });
    }
    Ok(())
}
