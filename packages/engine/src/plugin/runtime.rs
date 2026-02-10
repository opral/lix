use crate::materialization::{MaterializationPlan, MaterializationWriteOp};
use crate::plugin::types::{InstalledPlugin, PluginRuntime};
use crate::{LixBackend, LixError, LoadWasmComponentRequest, Value, WasmLimits, WasmRuntime};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DETECT_CHANGES_EXPORTS: &[&str] = &["detect-changes", "api#detect-changes"];
const APPLY_CHANGES_EXPORTS: &[&str] = &["apply-changes", "api#apply-changes"];
const PLUGIN_WORLD: &str = "lix:plugin/plugin@0.1.0";

#[derive(Debug, Clone)]
pub(crate) struct FileChangeDetectionRequest {
    pub file_id: String,
    pub version_id: String,
    pub path: String,
    pub before_data: Option<Vec<u8>>,
    pub after_data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct DetectedFileChange {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub plugin_key: String,
    pub snapshot_content: Option<String>,
}

#[derive(Debug, Clone)]
struct FileDescriptorRow {
    file_id: String,
    version_id: String,
    path: String,
    extension: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct FileDescriptorSnapshot {
    name: String,
    extension: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PluginFile {
    id: String,
    path: String,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PluginEntityChange {
    entity_id: String,
    schema_key: String,
    schema_version: String,
    snapshot_content: Option<String>,
}

#[derive(Debug, Serialize)]
struct ApplyChangesRequest {
    file: PluginFile,
    changes: Vec<PluginEntityChange>,
}

#[derive(Debug, Serialize)]
struct DetectChangesRequest {
    before: Option<PluginFile>,
    after: PluginFile,
}

pub(crate) async fn detect_file_changes_with_plugins(
    backend: &dyn LixBackend,
    runtime: &dyn WasmRuntime,
    writes: &[FileChangeDetectionRequest],
) -> Result<Vec<DetectedFileChange>, LixError> {
    if writes.is_empty() {
        return Ok(Vec::new());
    }

    let installed_plugins = load_installed_plugins(backend).await?;
    if installed_plugins.is_empty() {
        return Ok(Vec::new());
    }

    let mut detected = Vec::new();
    for write in writes {
        let Some(plugin) = select_plugin_for_path(&write.path, &installed_plugins) else {
            continue;
        };

        let before = write.before_data.as_ref().map(|data| PluginFile {
            id: write.file_id.clone(),
            path: write.path.clone(),
            data: data.clone(),
        });
        let after = PluginFile {
            id: write.file_id.clone(),
            path: write.path.clone(),
            data: write.after_data.clone(),
        };

        let payload =
            serde_json::to_vec(&DetectChangesRequest { before, after }).map_err(|error| {
                LixError {
                    message: format!(
                        "plugin detect-changes: failed to encode request payload: {error}"
                    ),
                }
            })?;

        let instance = runtime
            .load_component(LoadWasmComponentRequest {
                key: plugin.key.clone(),
                bytes: plugin.wasm.clone(),
                world: PLUGIN_WORLD.to_string(),
                limits: WasmLimits::default(),
            })
            .await?;

        let output = call_detect_changes(instance.as_ref(), &payload).await?;
        let plugin_changes: Vec<PluginEntityChange> =
            serde_json::from_slice(&output).map_err(|error| LixError {
                message: format!(
                    "plugin detect-changes: failed to decode plugin output for key '{}': {error}",
                    plugin.key
                ),
            })?;

        let mut seen_keys: BTreeSet<(String, String)> = BTreeSet::new();
        for change in plugin_changes {
            let dedupe_key = (change.schema_key.clone(), change.entity_id.clone());
            if !seen_keys.insert(dedupe_key.clone()) {
                return Err(LixError {
                    message: format!(
                        "plugin detect-changes: duplicate change key for plugin '{}' file '{}' version '{}': schema_key='{}' entity_id='{}'",
                        plugin.key,
                        write.file_id,
                        write.version_id,
                        dedupe_key.0,
                        dedupe_key.1
                    ),
                });
            }

            detected.push(DetectedFileChange {
                entity_id: change.entity_id,
                schema_key: change.schema_key,
                schema_version: change.schema_version,
                file_id: write.file_id.clone(),
                version_id: write.version_id.clone(),
                plugin_key: plugin.key.clone(),
                snapshot_content: change.snapshot_content,
            });
        }
    }

    Ok(detected)
}

pub(crate) async fn materialize_file_data_with_plugins(
    backend: &dyn LixBackend,
    runtime: &dyn WasmRuntime,
    plan: &MaterializationPlan,
) -> Result<(), LixError> {
    let installed_plugins = load_installed_plugins(backend).await?;
    if installed_plugins.is_empty() {
        return Ok(());
    }

    let mut descriptors: BTreeMap<(String, String), FileDescriptorRow> = BTreeMap::new();
    let mut tombstoned_files: Vec<(String, String)> = Vec::new();
    for write in &plan.writes {
        if write.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY {
            continue;
        }
        let key = (write.version_id.clone(), write.entity_id.clone());
        if write.op == MaterializationWriteOp::Tombstone {
            tombstoned_files.push((key.1, key.0));
            continue;
        }
        let Some(snapshot_json) = write.snapshot_content.as_deref() else {
            continue;
        };
        let snapshot: FileDescriptorSnapshot =
            serde_json::from_str(snapshot_json).map_err(|error| LixError {
                message: format!(
                    "plugin materialization: invalid file descriptor snapshot JSON: {error}"
                ),
            })?;
        let path = file_path_from_snapshot(&snapshot);
        descriptors.insert(
            key.clone(),
            FileDescriptorRow {
                file_id: key.1,
                version_id: key.0,
                path,
                extension: normalize_extension(snapshot.extension),
            },
        );
    }

    for (file_id, version_id) in tombstoned_files {
        delete_file_cache_data(backend, &file_id, &version_id).await?;
    }

    for descriptor in descriptors.values() {
        let Some(plugin) = select_plugin_for_file(descriptor, &installed_plugins) else {
            continue;
        };

        let mut seen_keys: BTreeSet<(String, String)> = BTreeSet::new();
        let mut changes: Vec<PluginEntityChange> = Vec::new();
        for write in plan.writes.iter().filter(|write| {
            write.version_id == descriptor.version_id
                && write.file_id == descriptor.file_id
                && write.plugin_key == plugin.key
                && write.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY
        }) {
            let dedupe_key = (write.schema_key.clone(), write.entity_id.clone());
            if !seen_keys.insert(dedupe_key.clone()) {
                return Err(LixError {
                    message: format!(
                        "plugin materialization: duplicate change key for plugin '{}' file '{}' version '{}': schema_key='{}' entity_id='{}'",
                        plugin.key,
                        descriptor.file_id,
                        descriptor.version_id,
                        dedupe_key.0,
                        dedupe_key.1
                    ),
                });
            }

            changes.push(PluginEntityChange {
                entity_id: write.entity_id.clone(),
                schema_key: write.schema_key.clone(),
                schema_version: write.schema_version.clone(),
                snapshot_content: if write.op == MaterializationWriteOp::Tombstone {
                    None
                } else {
                    write.snapshot_content.clone()
                },
            });
        }

        if changes.is_empty() {
            continue;
        }

        let previous_data =
            load_file_cache_data(backend, &descriptor.file_id, &descriptor.version_id).await?;
        let request_payload = ApplyChangesRequest {
            file: PluginFile {
                id: descriptor.file_id.clone(),
                path: descriptor.path.clone(),
                data: previous_data,
            },
            changes,
        };
        let payload = serde_json::to_vec(&request_payload).map_err(|error| LixError {
            message: format!(
                "plugin materialization: failed to encode apply-changes payload: {error}"
            ),
        })?;

        let instance = runtime
            .load_component(LoadWasmComponentRequest {
                key: plugin.key.clone(),
                bytes: plugin.wasm.clone(),
                world: PLUGIN_WORLD.to_string(),
                limits: WasmLimits::default(),
            })
            .await?;
        let output = call_apply_changes(instance.as_ref(), &payload).await?;
        upsert_file_cache_data(
            backend,
            &descriptor.file_id,
            &descriptor.version_id,
            &output,
        )
        .await?;
    }

    Ok(())
}

fn file_path_from_snapshot(snapshot: &FileDescriptorSnapshot) -> String {
    let name = snapshot.name.trim();
    let extension = snapshot
        .extension
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if let Some(extension) = extension {
        format!("/{name}.{extension}")
    } else {
        format!("/{name}")
    }
}

fn normalize_extension(value: Option<String>) -> Option<String> {
    value
        .map(|entry| entry.trim().trim_start_matches('.').to_ascii_lowercase())
        .filter(|entry| !entry.is_empty())
}

fn select_plugin_for_file<'a>(
    descriptor: &FileDescriptorRow,
    plugins: &'a [InstalledPlugin],
) -> Option<&'a InstalledPlugin> {
    plugins.iter().find(|plugin| {
        glob_matches_extension(&plugin.detect_changes_glob, descriptor.extension.as_deref())
    })
}

fn select_plugin_for_path<'a>(
    path: &str,
    plugins: &'a [InstalledPlugin],
) -> Option<&'a InstalledPlugin> {
    let extension = file_extension_from_path(path);
    plugins
        .iter()
        .find(|plugin| glob_matches_extension(&plugin.detect_changes_glob, extension.as_deref()))
}

fn file_extension_from_path(path: &str) -> Option<String> {
    let file_name = path.rsplit('/').next().unwrap_or(path);
    let extension = file_name.rsplit_once('.').map(|(_, ext)| ext.to_string());
    normalize_extension(extension)
}

fn glob_matches_extension(glob: &str, extension: Option<&str>) -> bool {
    let normalized = glob.trim().to_ascii_lowercase();
    if normalized == "*" || normalized == "**/*" {
        return true;
    }

    if let Some(ext) = normalized.strip_prefix("*.") {
        return extension
            .map(|value| value.eq_ignore_ascii_case(ext))
            .unwrap_or(false);
    }

    false
}

async fn call_apply_changes(
    instance: &dyn crate::WasmInstance,
    payload: &[u8],
) -> Result<Vec<u8>, LixError> {
    let mut errors = Vec::new();
    for export in APPLY_CHANGES_EXPORTS {
        match instance.call(export, payload).await {
            Ok(output) => return Ok(output),
            Err(error) => errors.push(format!("{export}: {}", error.message)),
        }
    }

    Err(LixError {
        message: format!(
            "plugin materialization: failed to call apply-changes export ({})",
            errors.join("; ")
        ),
    })
}

async fn call_detect_changes(
    instance: &dyn crate::WasmInstance,
    payload: &[u8],
) -> Result<Vec<u8>, LixError> {
    let mut errors = Vec::new();
    for export in DETECT_CHANGES_EXPORTS {
        match instance.call(export, payload).await {
            Ok(output) => return Ok(output),
            Err(error) => errors.push(format!("{export}: {}", error.message)),
        }
    }

    Err(LixError {
        message: format!(
            "plugin detect-changes: failed to call detect-changes export ({})",
            errors.join("; ")
        ),
    })
}

async fn load_installed_plugins(
    backend: &dyn LixBackend,
) -> Result<Vec<InstalledPlugin>, LixError> {
    let rows = backend
        .execute(
            "SELECT key, runtime, api_version, detect_changes_glob, entry, manifest_json, wasm \
             FROM lix_internal_plugin \
             WHERE runtime = 'wasm-component-v1' \
             ORDER BY key",
            &[],
        )
        .await?;

    let mut plugins = Vec::with_capacity(rows.rows.len());
    for row in rows.rows {
        plugins.push(parse_installed_plugin_row(&row)?);
    }
    Ok(plugins)
}

fn parse_installed_plugin_row(row: &[Value]) -> Result<InstalledPlugin, LixError> {
    let key = text_required(row, 0, "key")?;
    let runtime_raw = text_required(row, 1, "runtime")?;
    let runtime = PluginRuntime::from_str(&runtime_raw).ok_or_else(|| LixError {
        message: format!("plugin materialization: unsupported runtime '{runtime_raw}'"),
    })?;
    let api_version = text_required(row, 2, "api_version")?;
    let detect_changes_glob = text_required(row, 3, "detect_changes_glob")?;
    let entry = text_required(row, 4, "entry")?;
    let manifest_json = text_required(row, 5, "manifest_json")?;
    let wasm = blob_required(row, 6, "wasm")?;

    Ok(InstalledPlugin {
        key,
        runtime,
        api_version,
        detect_changes_glob,
        entry,
        manifest_json,
        wasm,
    })
}

async fn load_file_cache_data(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
) -> Result<Vec<u8>, LixError> {
    let result = backend
        .execute(
            "SELECT data \
             FROM lix_internal_file_data_cache \
             WHERE file_id = $1 AND version_id = $2 \
             LIMIT 1",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
            ],
        )
        .await?;

    let Some(row) = result.rows.first() else {
        return Ok(Vec::new());
    };
    blob_required(row, 0, "data")
}

async fn upsert_file_cache_data(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
    data: &[u8],
) -> Result<(), LixError> {
    backend
        .execute(
            "INSERT INTO lix_internal_file_data_cache (file_id, version_id, data) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (file_id, version_id) DO UPDATE SET \
             data = EXCLUDED.data",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
                Value::Blob(data.to_vec()),
            ],
        )
        .await?;
    Ok(())
}

async fn delete_file_cache_data(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
) -> Result<(), LixError> {
    backend
        .execute(
            "DELETE FROM lix_internal_file_data_cache \
             WHERE file_id = $1 AND version_id = $2",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
            ],
        )
        .await?;
    Ok(())
}

fn text_required(row: &[Value], index: usize, column: &str) -> Result<String, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            message: format!(
                "plugin materialization: row missing column '{column}' at index {index}"
            ),
        });
    };
    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(LixError {
            message: format!(
                "plugin materialization: expected text column '{column}' at index {index}, got {other:?}"
            ),
        }),
    }
}

fn blob_required(row: &[Value], index: usize, column: &str) -> Result<Vec<u8>, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            message: format!(
                "plugin materialization: row missing column '{column}' at index {index}"
            ),
        });
    };
    match value {
        Value::Blob(bytes) => Ok(bytes.clone()),
        Value::Text(text) => Ok(text.as_bytes().to_vec()),
        other => Err(LixError {
            message: format!(
                "plugin materialization: expected blob column '{column}' at index {index}, got {other:?}"
            ),
        }),
    }
}
