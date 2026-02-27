use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read};
use std::path::{Component, Path};

use serde_json::Value as JsonValue;
use zip::read::ZipArchive;

use crate::plugin::manifest::parse_plugin_manifest_json;
use crate::plugin::types::PluginManifest;
use crate::schema::{validate_lix_schema_definition, SchemaKey};

use super::*;

const INSTALL_STORED_SCHEMA_SQL: &str =
    "INSERT INTO lix_stored_schema_by_version (value, lixcol_version_id) \
     VALUES (lix_json(?), 'global') \
     ON CONFLICT (lixcol_entity_id, lixcol_file_id, lixcol_version_id) DO NOTHING";

struct ParsedPluginArchive {
    manifest: PluginManifest,
    normalized_manifest_json: String,
    wasm_bytes: Vec<u8>,
    schemas: Vec<ParsedSchema>,
}

struct ParsedSchema {
    normalized_schema_json: String,
}

impl Engine {
    pub async fn install_plugin(&self, archive_bytes: &[u8]) -> Result<(), LixError> {
        let parsed = parse_plugin_archive(archive_bytes)?;
        ensure_valid_wasm_binary(&parsed.wasm_bytes)?;

        let mut transaction = self.backend.begin_transaction().await?;
        let mut active_version_id = self.active_version_id.read().unwrap().clone();
        let starting_active_version_id = active_version_id.clone();
        let mut pending_state_commit_stream_changes = Vec::new();
        let options = ExecuteOptions::default();

        let install_result = install_plugin_in_transaction(
            self,
            transaction.as_mut(),
            &parsed,
            &options,
            &mut active_version_id,
            &mut pending_state_commit_stream_changes,
        )
        .await;

        match install_result {
            Ok(()) => transaction.commit().await?,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        }

        if active_version_id != starting_active_version_id {
            self.set_active_version_id(active_version_id);
        }
        self.invalidate_installed_plugins_cache()?;
        self.emit_state_commit_stream_changes(pending_state_commit_stream_changes);
        Ok(())
    }
}

async fn install_plugin_in_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    parsed: &ParsedPluginArchive,
    options: &ExecuteOptions,
    active_version_id: &mut String,
    pending_state_commit_stream_changes: &mut Vec<StateCommitStreamChange>,
) -> Result<(), LixError> {
    for schema in &parsed.schemas {
        engine
            .execute_with_options_in_transaction(
                transaction,
                INSTALL_STORED_SCHEMA_SQL,
                &[Value::Text(schema.normalized_schema_json.clone())],
                options,
                active_version_id,
                None,
                false,
                pending_state_commit_stream_changes,
            )
            .await?;
    }

    let now = crate::functions::timestamp::timestamp();
    upsert_plugin_record_in_transaction(
        transaction,
        &parsed.manifest,
        &parsed.normalized_manifest_json,
        &parsed.wasm_bytes,
        &now,
    )
    .await
}

fn parse_plugin_archive(archive_bytes: &[u8]) -> Result<ParsedPluginArchive, LixError> {
    let files = read_archive_files(archive_bytes)?;

    let manifest_bytes = files.get("manifest.json").ok_or_else(|| LixError {
        message: "Plugin archive must contain manifest.json".to_string(),
    })?;
    let manifest_raw = std::str::from_utf8(manifest_bytes).map_err(|error| LixError {
        message: format!("Plugin archive manifest.json must be UTF-8: {error}"),
    })?;
    let validated_manifest = parse_plugin_manifest_json(manifest_raw)?;

    let entry_path = normalize_archive_path(&validated_manifest.manifest.entry)?;
    let wasm_bytes = files
        .get(&entry_path)
        .ok_or_else(|| LixError {
            message: format!(
                "Plugin archive is missing manifest entry file '{}'",
                validated_manifest.manifest.entry
            ),
        })?
        .clone();

    let mut schemas = Vec::with_capacity(validated_manifest.manifest.schemas.len());
    let mut seen_schema_keys = BTreeSet::<(String, String)>::new();
    for schema_path in &validated_manifest.manifest.schemas {
        let normalized_schema_path = normalize_archive_path(schema_path)?;
        let schema_bytes = files.get(&normalized_schema_path).ok_or_else(|| LixError {
            message: format!("Plugin archive is missing schema file '{schema_path}'"),
        })?;
        let schema_json: JsonValue =
            serde_json::from_slice(schema_bytes).map_err(|error| LixError {
                message: format!("Plugin archive schema '{schema_path}' is invalid JSON: {error}"),
            })?;
        validate_lix_schema_definition(&schema_json)?;
        let schema_key = extract_schema_key(&schema_json)?;
        if !seen_schema_keys.insert((
            schema_key.schema_key.clone(),
            schema_key.schema_version.clone(),
        )) {
            return Err(LixError {
                message: format!(
                    "Plugin archive declares duplicate schema '{}~{}'",
                    schema_key.schema_key, schema_key.schema_version
                ),
            });
        }
        let normalized_schema_json =
            serde_json::to_string(&schema_json).map_err(|error| LixError {
                message: format!(
                    "Failed to normalize schema JSON '{}' from plugin archive: {error}",
                    schema_path
                ),
            })?;
        schemas.push(ParsedSchema {
            normalized_schema_json,
        });
    }

    Ok(ParsedPluginArchive {
        manifest: validated_manifest.manifest,
        normalized_manifest_json: validated_manifest.normalized_json,
        wasm_bytes,
        schemas,
    })
}

fn extract_schema_key(schema: &JsonValue) -> Result<SchemaKey, LixError> {
    let object = schema.as_object().ok_or_else(|| LixError {
        message: "schema definition must be a JSON object".to_string(),
    })?;
    let schema_key = object
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            message: "schema definition must include string x-lix-key".to_string(),
        })?;
    let schema_version = object
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            message: "schema definition must include string x-lix-version".to_string(),
        })?;
    Ok(SchemaKey::new(
        schema_key.to_string(),
        schema_version.to_string(),
    ))
}

fn read_archive_files(archive_bytes: &[u8]) -> Result<BTreeMap<String, Vec<u8>>, LixError> {
    if archive_bytes.is_empty() {
        return Err(LixError {
            message: "Plugin archive bytes must not be empty".to_string(),
        });
    }

    let mut archive = ZipArchive::new(Cursor::new(archive_bytes)).map_err(|error| LixError {
        message: format!("Plugin archive is not a valid zip file: {error}"),
    })?;
    let mut files = BTreeMap::<String, Vec<u8>>::new();

    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(|error| LixError {
            message: format!("Failed to read plugin archive entry at index {index}: {error}"),
        })?;
        let raw_name = entry.name().to_string();

        if entry.is_dir() {
            continue;
        }
        if is_symlink_mode(entry.unix_mode()) {
            return Err(LixError {
                message: format!("Plugin archive entry '{raw_name}' must not be a symlink"),
            });
        }

        let normalized_path = normalize_archive_path(&raw_name)?;
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).map_err(|error| LixError {
            message: format!("Failed to read plugin archive entry '{raw_name}': {error}"),
        })?;
        if files.insert(normalized_path.clone(), bytes).is_some() {
            return Err(LixError {
                message: format!("Plugin archive contains duplicate entry '{normalized_path}'"),
            });
        }
    }

    Ok(files)
}

fn is_symlink_mode(mode: Option<u32>) -> bool {
    const MODE_FILE_TYPE_MASK: u32 = 0o170000;
    const MODE_SYMLINK: u32 = 0o120000;
    mode.is_some_and(|value| (value & MODE_FILE_TYPE_MASK) == MODE_SYMLINK)
}

fn normalize_archive_path(path: &str) -> Result<String, LixError> {
    if path.is_empty() {
        return Err(LixError {
            message: "Plugin archive path must not be empty".to_string(),
        });
    }
    if path.starts_with('/') || path.starts_with('\\') {
        return Err(LixError {
            message: format!("Plugin archive path '{path}' must be relative"),
        });
    }
    if path.contains('\\') {
        return Err(LixError {
            message: format!("Plugin archive path '{path}' must use forward slash separators"),
        });
    }

    let mut segments = Vec::<String>::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(value) => {
                let segment = value.to_str().ok_or_else(|| LixError {
                    message: format!(
                        "Plugin archive path '{path}' contains non-UTF-8 components"
                    ),
                })?;
                if segment.is_empty() {
                    return Err(LixError {
                        message: format!("Plugin archive path '{path}' is invalid"),
                    });
                }
                segments.push(segment.to_string());
            }
            Component::CurDir | Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(LixError {
                    message: format!(
                        "Plugin archive path '{path}' must not contain traversal or absolute components"
                    ),
                })
            }
        }
    }

    if segments.is_empty() {
        return Err(LixError {
            message: format!("Plugin archive path '{path}' is invalid"),
        });
    }

    Ok(segments.join("/"))
}

async fn upsert_plugin_record_in_transaction(
    transaction: &mut dyn LixTransaction,
    manifest: &PluginManifest,
    manifest_json: &str,
    wasm_bytes: &[u8],
    timestamp: &str,
) -> Result<(), LixError> {
    transaction
        .execute(
            "INSERT INTO lix_internal_plugin (\
             key, runtime, api_version, match_path_glob, entry, manifest_json, wasm, created_at, updated_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8) \
             ON CONFLICT (key) DO UPDATE SET \
             runtime = EXCLUDED.runtime, \
             api_version = EXCLUDED.api_version, \
             match_path_glob = EXCLUDED.match_path_glob, \
             entry = EXCLUDED.entry, \
             manifest_json = EXCLUDED.manifest_json, \
             wasm = EXCLUDED.wasm, \
             updated_at = EXCLUDED.updated_at",
            &[
                Value::Text(manifest.key.clone()),
                Value::Text(manifest.runtime.as_str().to_string()),
                Value::Text(manifest.api_version.clone()),
                Value::Text(manifest.file_match.path_glob.clone()),
                Value::Text(manifest.entry.clone()),
                Value::Text(manifest_json.to_string()),
                Value::Blob(wasm_bytes.to_vec()),
                Value::Text(timestamp.to_string()),
            ],
        )
        .await?;

    Ok(())
}

fn ensure_valid_wasm_binary(wasm_bytes: &[u8]) -> Result<(), LixError> {
    if wasm_bytes.is_empty() {
        return Err(LixError {
            message: "Plugin wasm bytes must not be empty".to_string(),
        });
    }
    if wasm_bytes.len() < 8 || !wasm_bytes.starts_with(&[0x00, 0x61, 0x73, 0x6d]) {
        return Err(LixError {
            message: "Plugin wasm bytes must start with a valid wasm header".to_string(),
        });
    }
    Ok(())
}
