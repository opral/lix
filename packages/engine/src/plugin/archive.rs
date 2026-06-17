use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read};

use serde_json::Value as JsonValue;
use zip::read::ZipArchive;

use crate::LixError;
use crate::schema::{schema_key_from_definition, validate_lix_schema_definition};

use super::{InstalledPlugin, InstalledPluginMetadata, PluginManifest, parse_plugin_manifest_json};

#[derive(Debug, Clone)]
pub(crate) struct ParsedPluginArchive {
    pub manifest: PluginManifest,
    pub schemas: Vec<JsonValue>,
}

pub(crate) fn parse_plugin_archive_for_install(
    archive_bytes: &[u8],
) -> Result<ParsedPluginArchive, LixError> {
    let files = read_archive_files_for_install(archive_bytes)?;

    let manifest_bytes = files.get("manifest.json").ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: "Plugin archive must contain manifest.json".to_string(),
        hint: None,
        details: None,
    })?;
    let manifest_raw = std::str::from_utf8(manifest_bytes).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!("Plugin archive manifest.json must be UTF-8: {error}"),
        hint: None,
        details: None,
    })?;
    let validated_manifest = parse_plugin_manifest_json(manifest_raw)?;

    let entry_path = parse_archive_path_for_install(&validated_manifest.manifest.entry)?;
    let wasm_bytes = files
        .get(&entry_path)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "Plugin archive is missing manifest entry file '{}'",
                validated_manifest.manifest.entry
            ),
            hint: None,
            details: None,
        })?
        .clone();
    ensure_valid_plugin_wasm_for_install(&wasm_bytes)?;

    let mut schemas = Vec::with_capacity(validated_manifest.manifest.schemas.len());
    let mut seen_schema_keys = BTreeSet::<String>::new();
    for schema_path in &validated_manifest.manifest.schemas {
        let schema_entry_path = parse_archive_path_for_install(schema_path)?;
        let schema_bytes = files.get(&schema_entry_path).ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!("Plugin archive is missing schema file '{schema_path}'"),
            hint: None,
            details: None,
        })?;
        let schema_json: JsonValue =
            serde_json::from_slice(schema_bytes).map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                message: format!("Plugin archive schema '{schema_path}' is invalid JSON: {error}"),
                hint: None,
                details: None,
            })?;
        validate_lix_schema_definition(&schema_json)?;
        let schema_key = schema_key_from_definition(&schema_json)?;
        if !seen_schema_keys.insert(schema_key.schema_key.clone()) {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                message: format!(
                    "Plugin archive declares duplicate schema '{}'",
                    schema_key.schema_key
                ),
                hint: None,
                details: None,
            });
        }
        schemas.push(schema_json);
    }

    Ok(ParsedPluginArchive {
        manifest: validated_manifest.manifest,
        schemas,
    })
}

pub(crate) fn load_installed_plugin_from_archive_bytes(
    plugin_key: &str,
    archive_path: &str,
    archive_bytes: &[u8],
) -> Result<InstalledPlugin, LixError> {
    let files = read_plugin_archive_files(archive_path, archive_bytes)?;
    let manifest_bytes = files.get("manifest.json").ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!(
            "plugin materialization: archive '{archive_path}' is missing manifest.json"
        ),
        hint: None,
        details: None,
    })?;
    let manifest_raw = std::str::from_utf8(manifest_bytes).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!(
            "plugin materialization: archive '{archive_path}' manifest.json must be UTF-8: {error}"
        ),
        hint: None,
        details: None,
    })?;
    let validated_manifest = parse_plugin_manifest_json(manifest_raw)?;
    if validated_manifest.manifest.key != plugin_key {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "plugin materialization: archive '{}' key mismatch: file id key '{}' vs manifest key '{}'",
                archive_path, plugin_key, validated_manifest.manifest.key
            ),
            hint: None,
            details: None,
        });
    }

    let entry_path =
        parse_plugin_archive_path_for_materialization(&validated_manifest.manifest.entry)?;
    let wasm = files.get(&entry_path).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!(
            "plugin materialization: archive '{}' is missing entry file '{}'",
            archive_path, validated_manifest.manifest.entry
        ),
        hint: None,
        details: None,
    })?;
    ensure_valid_plugin_wasm_for_materialization(wasm)?;

    let manifest = validated_manifest.manifest;
    let content_type = manifest.file_match.content_type;
    let mut schema_keys = Vec::with_capacity(manifest.schemas.len());
    for schema_path in &manifest.schemas {
        let schema_entry_path = parse_plugin_archive_path_for_materialization(schema_path)?;
        let schema_bytes = files.get(&schema_entry_path).ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "plugin materialization: archive '{archive_path}' is missing schema file '{schema_path}'"
            ),
            hint: None,
            details: None,
        })?;
        let schema_json: JsonValue = serde_json::from_slice(schema_bytes).map_err(|error| {
            LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                message: format!(
                    "plugin materialization: archive '{archive_path}' schema '{schema_path}' is invalid JSON: {error}"
                ),
                hint: None,
                details: None,
            }
        })?;
        validate_lix_schema_definition(&schema_json)?;
        let schema_key = schema_key_from_definition(&schema_json)?;
        schema_keys.push(schema_key.schema_key);
    }

    Ok(InstalledPlugin {
        key: manifest.key,
        runtime: manifest.runtime,
        api_version: manifest.api_version,
        path_glob: manifest.file_match.path_glob,
        content_type,
        entry: manifest.entry,
        schema_keys,
        manifest_json: validated_manifest.normalized_json,
        wasm: wasm.clone(),
    })
}

pub(crate) fn load_installed_plugin_metadata_from_archive_bytes(
    plugin_key: &str,
    archive_path: &str,
    archive_blob_hash: &str,
    archive_bytes: &[u8],
) -> Result<InstalledPluginMetadata, LixError> {
    let files = read_plugin_archive_files(archive_path, archive_bytes)?;
    let manifest_bytes = files.get("manifest.json").ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!(
            "plugin metadata discovery: archive '{archive_path}' is missing manifest.json"
        ),
        hint: None,
        details: None,
    })?;
    let manifest_raw = std::str::from_utf8(manifest_bytes).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!(
            "plugin metadata discovery: archive '{archive_path}' manifest.json must be UTF-8: {error}"
        ),
        hint: None,
        details: None,
    })?;
    let validated_manifest = parse_plugin_manifest_json(manifest_raw)?;
    if validated_manifest.manifest.key != plugin_key {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "plugin metadata discovery: archive '{}' key mismatch: file id key '{}' vs manifest key '{}'",
                archive_path, plugin_key, validated_manifest.manifest.key
            ),
            hint: None,
            details: None,
        });
    }

    let manifest = validated_manifest.manifest;
    let content_type = manifest.file_match.content_type;
    let mut schema_keys = Vec::with_capacity(manifest.schemas.len());
    for schema_path in &manifest.schemas {
        let schema_entry_path = parse_plugin_archive_path_for_materialization(schema_path)?;
        let schema_bytes = files.get(&schema_entry_path).ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "plugin metadata discovery: archive '{archive_path}' is missing schema file '{schema_path}'"
            ),
            hint: None,
            details: None,
        })?;
        let schema_json: JsonValue = serde_json::from_slice(schema_bytes).map_err(|error| {
            LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                message: format!(
                    "plugin metadata discovery: archive '{archive_path}' schema '{schema_path}' is invalid JSON: {error}"
                ),
                hint: None,
                details: None,
            }
        })?;
        validate_lix_schema_definition(&schema_json)?;
        let schema_key = schema_key_from_definition(&schema_json)?;
        schema_keys.push(schema_key.schema_key);
    }

    Ok(InstalledPluginMetadata {
        key: manifest.key,
        archive_path: archive_path.to_string(),
        archive_blob_hash: archive_blob_hash.to_string(),
        path_glob: manifest.file_match.path_glob,
        content_type,
        schema_keys,
    })
}

fn read_archive_files_for_install(
    archive_bytes: &[u8],
) -> Result<BTreeMap<String, Vec<u8>>, LixError> {
    if archive_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: "Plugin archive bytes must not be empty".to_string(),
            hint: None,
            details: None,
        });
    }

    let mut archive = ZipArchive::new(Cursor::new(archive_bytes)).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!("Plugin archive is not a valid zip file: {error}"),
        hint: None,
        details: None,
    })?;
    let mut files = BTreeMap::<String, Vec<u8>>::new();

    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!("Failed to read plugin archive entry at index {index}: {error}"),
            hint: None,
            details: None,
        })?;
        let raw_name = entry.name().to_string();

        if entry.is_dir() {
            continue;
        }
        if is_symlink_mode(entry.unix_mode()) {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                message: format!("Plugin archive entry '{raw_name}' must not be a symlink"),
                hint: None,
                details: None,
            });
        }

        let entry_path = parse_archive_path_for_install(&raw_name)?;
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!("Failed to read plugin archive entry '{raw_name}': {error}"),
            hint: None,
            details: None,
        })?;
        if files.insert(entry_path.clone(), bytes).is_some() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                message: format!("Plugin archive contains duplicate entry '{entry_path}'"),
                hint: None,
                details: None,
            });
        }
    }

    Ok(files)
}

fn read_plugin_archive_files(
    archive_path: &str,
    archive_bytes: &[u8],
) -> Result<BTreeMap<String, Vec<u8>>, LixError> {
    if archive_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!("plugin materialization: archive '{archive_path}' is empty"),
            hint: None,
            details: None,
        });
    }

    let mut archive = ZipArchive::new(Cursor::new(archive_bytes)).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!(
            "plugin materialization: archive '{archive_path}' is not a valid zip file: {error}"
        ),
        hint: None,
        details: None,
    })?;
    let mut files = BTreeMap::<String, Vec<u8>>::new();

    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "plugin materialization: failed to read archive '{archive_path}' entry index {index}: {error}"
            ),
            hint: None,
            details: None,
        })?;

        let entry_name = entry.name().to_string();
        if entry.is_dir() {
            continue;
        }
        let entry_path = parse_plugin_archive_path_for_materialization(&entry_name)?;

        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "plugin materialization: failed to read archive '{archive_path}' entry '{entry_name}': {error}"
            ),
            hint: None,
            details: None,
        })?;
        files.insert(entry_path, bytes);
    }

    Ok(files)
}

fn parse_archive_path_for_install(path: &str) -> Result<String, LixError> {
    parse_plugin_archive_path(path, "Plugin archive")
}

fn parse_plugin_archive_path_for_materialization(path: &str) -> Result<String, LixError> {
    parse_plugin_archive_path(path, "plugin materialization: archive")
}

fn parse_plugin_archive_path(path: &str, context: &str) -> Result<String, LixError> {
    if path.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{context} path must not be empty"),
        ));
    }
    if path.starts_with('/') || path.starts_with('\\') {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{context} path '{path}' must be relative"),
        ));
    }
    if path.contains('\\') {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{context} path '{path}' must use forward slash separators"),
        ));
    }

    for segment in path.split('/') {
        if segment.is_empty() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("{context} path '{path}' is invalid"),
            ));
        }
        if matches!(segment, "." | "..") {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("{context} path '{path}' must not contain traversal or dot components"),
            ));
        }
    }

    Ok(path.to_string())
}

fn ensure_valid_plugin_wasm_for_install(wasm_bytes: &[u8]) -> Result<(), LixError> {
    if wasm_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: "Plugin wasm bytes must not be empty".to_string(),
            hint: None,
            details: None,
        });
    }
    if wasm_bytes.len() < 8 || !wasm_bytes.starts_with(&[0x00, 0x61, 0x73, 0x6d]) {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: "Plugin wasm bytes must start with a valid wasm header".to_string(),
            hint: None,
            details: None,
        });
    }
    Ok(())
}

fn ensure_valid_plugin_wasm_for_materialization(bytes: &[u8]) -> Result<(), LixError> {
    const WASM_MAGIC: &[u8; 4] = b"\0asm";
    if bytes.len() < WASM_MAGIC.len() || &bytes[..WASM_MAGIC.len()] != WASM_MAGIC {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: "plugin materialization: entry file must be a valid WebAssembly module"
                .to_string(),
            hint: None,
            details: None,
        });
    }

    Ok(())
}

fn is_symlink_mode(mode: Option<u32>) -> bool {
    const MODE_FILE_TYPE_MASK: u32 = 0o170_000;
    const MODE_SYMLINK: u32 = 0o120_000;
    mode.is_some_and(|value| (value & MODE_FILE_TYPE_MASK) == MODE_SYMLINK)
}

#[cfg(test)]
mod tests {
    use super::{parse_archive_path_for_install, parse_plugin_archive_path_for_materialization};

    #[test]
    fn install_archive_path_parsing_is_slash_based() {
        assert_eq!(
            parse_archive_path_for_install("schemas/table.json").as_deref(),
            Ok("schemas/table.json")
        );
        assert!(
            parse_archive_path_for_install("schemas\\table.json")
                .expect_err("backslash must not be accepted as a portable archive separator")
                .message
                .contains("forward slash")
        );
        assert!(
            parse_archive_path_for_install("schemas//table.json")
                .expect_err("empty slash segments must be rejected")
                .message
                .contains("invalid")
        );
        assert!(
            parse_archive_path_for_install("schemas/../table.json")
                .expect_err("archive paths must not traverse")
                .message
                .contains("traversal")
        );
    }

    #[test]
    fn materialization_archive_path_parsing_is_slash_based() {
        assert_eq!(
            parse_plugin_archive_path_for_materialization("schemas/table.json").as_deref(),
            Ok("schemas/table.json")
        );
        assert!(
            parse_plugin_archive_path_for_materialization("schemas\\table.json")
                .expect_err("backslash must not be accepted as a portable archive separator")
                .message
                .contains("forward slash")
        );
        assert!(
            parse_plugin_archive_path_for_materialization("schemas//table.json")
                .expect_err("empty slash segments must be rejected")
                .message
                .contains("invalid")
        );
        assert!(
            parse_plugin_archive_path_for_materialization("schemas/../table.json")
                .expect_err("archive paths must not traverse")
                .message
                .contains("traversal")
        );
        assert!(
            parse_plugin_archive_path_for_materialization("schemas/./table.json")
                .expect_err("archive paths must not contain dot segments")
                .message
                .contains("dot")
        );
    }
}
