use crate::cel::CelEvaluator;
use crate::materialization::{MaterializationPlan, MaterializationWrite, MaterializationWriteOp};
use crate::plugin::matching::select_best_glob_match;
use crate::plugin::types::{
    InstalledPlugin, PluginContentType, PluginManifest, PluginRuntime, StateContextColumn,
};
use crate::engine::sql2::planning::preprocess::preprocess_sql_to_plan as preprocess_sql;
use crate::{LixBackend, LixError, Value, WasmLimits, WasmRuntime};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
pub(crate) const BUILTIN_BINARY_FALLBACK_PLUGIN_KEY: &str = "lix_builtin_binary_fallback";
const BUILTIN_BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const BUILTIN_BINARY_BLOB_REF_SCHEMA_VERSION: &str = "1";
const DETECT_CHANGES_EXPORTS: &[&str] = &["detect-changes", "api#detect-changes"];
const APPLY_CHANGES_EXPORTS: &[&str] = &["apply-changes", "api#apply-changes"];
const BINARY_CHUNK_CODEC_LEGACY: &str = "legacy";
const BINARY_CHUNK_CODEC_RAW: &str = "raw";
const BINARY_CHUNK_CODEC_ZSTD: &str = "zstd";
const BINARY_CHUNK_CODEC_PREFIX_RAW: &[u8] = b"LIXRAW01";
const BINARY_CHUNK_CODEC_PREFIX_ZSTD: &[u8] = b"LIXZSTD1";

#[derive(Debug, Clone)]
pub(crate) struct FileChangeDetectionRequest {
    pub file_id: String,
    pub version_id: String,
    pub before_path: Option<String>,
    pub after_path: Option<String>,
    pub data_is_authoritative: bool,
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
}

#[derive(Debug, Clone)]
struct FileHistoryDescriptorRow {
    file_id: String,
    root_commit_id: String,
    depth: i64,
    commit_id: String,
    path: String,
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

#[derive(Clone)]
pub(crate) struct CachedPluginComponent {
    pub wasm: Vec<u8>,
    pub instance: Arc<dyn crate::WasmComponentInstance>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    state_context: Option<DetectStateContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DetectStateContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    active_state: Option<Vec<PluginActiveStateRow>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PluginActiveStateRow {
    entity_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    change_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BuiltinBinaryBlobRefSnapshot {
    pub(crate) id: String,
    pub(crate) blob_hash: String,
    pub(crate) size_bytes: u64,
}

#[allow(dead_code)]
pub(crate) async fn detect_file_changes_with_plugins(
    backend: &dyn LixBackend,
    runtime: &dyn WasmRuntime,
    writes: &[FileChangeDetectionRequest],
    installed_plugins: &[InstalledPlugin],
) -> Result<Vec<DetectedFileChange>, LixError> {
    let mut loaded_instances: BTreeMap<String, CachedPluginComponent> = BTreeMap::new();
    detect_file_changes_with_plugins_with_cache(
        backend,
        runtime,
        writes,
        installed_plugins,
        &mut loaded_instances,
    )
    .await
}

pub(crate) async fn detect_file_changes_with_plugins_with_cache(
    backend: &dyn LixBackend,
    runtime: &dyn WasmRuntime,
    writes: &[FileChangeDetectionRequest],
    installed_plugins: &[InstalledPlugin],
    loaded_instances: &mut BTreeMap<String, CachedPluginComponent>,
) -> Result<Vec<DetectedFileChange>, LixError> {
    if writes.is_empty() {
        return Ok(Vec::new());
    }

    let mut detected = Vec::new();
    let mut state_context_columns_by_plugin: BTreeMap<String, Option<Vec<StateContextColumn>>> =
        BTreeMap::new();
    for write in writes {
        let has_before_context = write.before_path.is_some() || write.before_data.is_some();
        let before_path = write
            .before_path
            .as_deref()
            .or(write.after_path.as_deref())
            .unwrap_or("");
        let before_content_type = write
            .before_data
            .as_deref()
            .map(classify_content_type_from_bytes);
        let before_plugin = if has_before_context {
            select_plugin_for_path(before_path, before_content_type, installed_plugins)
        } else {
            None
        };
        let before_uses_builtin_fallback = has_before_context && before_plugin.is_none();
        let after_content_type = classify_content_type_from_bytes(&write.after_data);
        let after_plugin = write.after_path.as_deref().and_then(|path| {
            select_plugin_for_path(path, Some(after_content_type), installed_plugins)
        });
        let is_delete_without_after_path =
            write.after_path.is_none() && write.after_data.is_empty();
        let after_uses_builtin_fallback =
            !is_delete_without_after_path && write.after_path.is_some() && after_plugin.is_none();

        if let Some(previous_plugin) = before_plugin {
            let plugin_changed = after_plugin
                .map(|plugin| plugin.key.as_str())
                .unwrap_or_default()
                != previous_plugin.key.as_str();
            if plugin_changed {
                for existing in load_existing_plugin_entities(
                    backend,
                    &write.file_id,
                    &write.version_id,
                    &previous_plugin.key,
                )
                .await?
                {
                    detected.push(DetectedFileChange {
                        entity_id: existing.entity_id,
                        schema_key: existing.schema_key,
                        schema_version: existing.schema_version,
                        file_id: write.file_id.clone(),
                        version_id: write.version_id.clone(),
                        plugin_key: previous_plugin.key.clone(),
                        snapshot_content: None,
                    });
                }
            }
        }

        if before_uses_builtin_fallback {
            let fallback_changed = after_plugin.is_some() || is_delete_without_after_path;
            if fallback_changed {
                for existing in load_existing_plugin_entities(
                    backend,
                    &write.file_id,
                    &write.version_id,
                    BUILTIN_BINARY_FALLBACK_PLUGIN_KEY,
                )
                .await?
                {
                    detected.push(DetectedFileChange {
                        entity_id: existing.entity_id,
                        schema_key: existing.schema_key,
                        schema_version: existing.schema_version,
                        file_id: write.file_id.clone(),
                        version_id: write.version_id.clone(),
                        plugin_key: BUILTIN_BINARY_FALLBACK_PLUGIN_KEY.to_string(),
                        snapshot_content: None,
                    });
                }
            }
        }

        if is_delete_without_after_path {
            // Deletes flow through with no "after" path/data.
            // In that case, we already emitted tombstones above for the previous plugin.
            continue;
        }

        if after_uses_builtin_fallback {
            // Path-only writes (no authoritative data assignment) must not rewrite builtin
            // blob refs to an "empty" hash when bytes are unknown/missing from prefetch.
            // In that case, preserve existing fallback state unless we have real before bytes.
            if !write.data_is_authoritative {
                if write.before_data.is_none() && before_uses_builtin_fallback {
                    continue;
                }
            }

            let blob_bytes = if write.data_is_authoritative {
                write.after_data.as_slice()
            } else if let Some(before_data) = write.before_data.as_ref() {
                before_data.as_slice()
            } else {
                continue;
            };

            let after_blob_hash = binary_blob_hash_hex(blob_bytes);
            let before_blob_hash = if before_uses_builtin_fallback {
                load_builtin_binary_blob_ref_for_file(backend, &write.file_id, &write.version_id)
                    .await?
                    .map(|snapshot| snapshot.blob_hash)
            } else {
                None
            };
            if before_blob_hash.as_deref() == Some(after_blob_hash.as_str()) {
                continue;
            }

            let snapshot_content = serde_json::to_string(&BuiltinBinaryBlobRefSnapshot {
                id: write.file_id.clone(),
                blob_hash: after_blob_hash,
                size_bytes: blob_bytes.len() as u64,
            })
            .map_err(|error| LixError {
                message: format!(
                    "plugin detect-changes: failed to encode builtin binary fallback snapshot: {error}"
                ),
            })?;

            detected.push(DetectedFileChange {
                entity_id: write.file_id.clone(),
                schema_key: BUILTIN_BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
                schema_version: BUILTIN_BINARY_BLOB_REF_SCHEMA_VERSION.to_string(),
                file_id: write.file_id.clone(),
                version_id: write.version_id.clone(),
                plugin_key: BUILTIN_BINARY_FALLBACK_PLUGIN_KEY.to_string(),
                snapshot_content: Some(snapshot_content),
            });
            continue;
        }

        let Some(plugin) = after_plugin else {
            return Err(no_matching_plugin_for_detect_error(
                write,
                after_content_type,
                "install a plugin whose match.path_glob and optional match.content_type cover this file",
            ));
        };

        let plugin_changed = before_plugin
            .map(|entry| entry.key.as_str())
            .unwrap_or_default()
            != plugin.key.as_str();

        let instance = load_or_init_plugin_component(runtime, loaded_instances, plugin).await?;

        let mut before_data = if plugin_changed || !has_before_context {
            None
        } else {
            write.before_data.clone()
        };
        if before_data.is_none() && !plugin_changed && has_before_context {
            let cached = load_file_cache_data(backend, &write.file_id, &write.version_id).await?;
            if !cached.is_empty() {
                before_data = Some(cached);
            }
        }
        if before_data.is_none() && !plugin_changed && has_before_context {
            before_data = reconstruct_before_file_data_from_state(
                backend,
                instance.as_ref(),
                plugin,
                &write.file_id,
                &write.version_id,
                before_path,
            )
            .await?;
        }

        let before = before_data.as_ref().map(|data| PluginFile {
            id: write.file_id.clone(),
            path: before_path.to_string(),
            data: data.clone(),
        });
        let after = PluginFile {
            id: write.file_id.clone(),
            path: write
                .after_path
                .clone()
                .unwrap_or_else(|| before_path.to_string()),
            data: write.after_data.clone(),
        };

        let selected_state_columns =
            if let Some(cached) = state_context_columns_by_plugin.get(&plugin.key).cloned() {
                cached
            } else {
                let resolved = resolve_state_context_columns(plugin)?;
                state_context_columns_by_plugin.insert(plugin.key.clone(), resolved.clone());
                resolved
            };
        let state_context = match selected_state_columns {
            Some(columns) => {
                let rows = load_active_state_context_rows(
                    backend,
                    &write.file_id,
                    &write.version_id,
                    &plugin.key,
                    &columns,
                )
                .await?;
                Some(DetectStateContext {
                    active_state: Some(rows),
                })
            }
            None => None,
        };

        let detect_payload = serde_json::to_vec(&DetectChangesRequest {
            before: before.clone(),
            after: after.clone(),
            state_context,
        })
        .map_err(|error| LixError {
            message: format!("plugin detect-changes: failed to encode request payload: {error}"),
        })?;
        let detect_output = call_detect_changes(instance.as_ref(), &detect_payload).await?;
        let mut plugin_changes: Vec<PluginEntityChange> = serde_json::from_slice(&detect_output)
            .map_err(|error| LixError {
                message: format!(
                    "plugin detect-changes: failed to decode plugin output for key '{}': {error}",
                    plugin.key
                ),
            })?;

        let mut plugin_change_keys = plugin_changes
            .iter()
            .map(|change| (change.schema_key.clone(), change.entity_id.clone()))
            .collect::<BTreeSet<_>>();

        if has_before_context {
            if plugin_detect_emits_complete_diff(plugin) {
                // This plugin computes explicit add/remove changes from before/after file bytes,
                // so no DB reconciliation is needed for missing tombstones.
            } else {
                let existing_entities = load_existing_plugin_entities(
                    backend,
                    &write.file_id,
                    &write.version_id,
                    &plugin.key,
                )
                .await?;
                append_implicit_tombstones_for_projection(
                    &mut plugin_changes,
                    &existing_entities,
                    &mut plugin_change_keys,
                );
            }
        }

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

async fn load_or_init_plugin_component(
    runtime: &dyn WasmRuntime,
    loaded_instances: &mut BTreeMap<String, CachedPluginComponent>,
    plugin: &InstalledPlugin,
) -> Result<Arc<dyn crate::WasmComponentInstance>, LixError> {
    if let Some(cached) = loaded_instances.get(&plugin.key) {
        if cached.wasm == plugin.wasm {
            return Ok(cached.instance.clone());
        }
    }

    let loaded = runtime
        .init_component(plugin.wasm.clone(), WasmLimits::default())
        .await?;
    loaded_instances.insert(
        plugin.key.clone(),
        CachedPluginComponent {
            wasm: plugin.wasm.clone(),
            instance: loaded.clone(),
        },
    );
    Ok(loaded)
}

pub(crate) async fn materialize_file_data_with_plugins(
    backend: &dyn LixBackend,
    runtime: &dyn WasmRuntime,
    plan: &MaterializationPlan,
) -> Result<(), LixError> {
    let installed_plugins = load_installed_plugins(backend).await?;

    let mut descriptor_targets: BTreeSet<(String, String)> = BTreeSet::new();
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
        let Some(_) = write.snapshot_content.as_deref() else {
            continue;
        };
        descriptor_targets.insert(key);
    }

    for (file_id, version_id) in tombstoned_files {
        delete_file_cache_data(backend, &file_id, &version_id).await?;
    }

    let descriptor_paths = load_file_paths_for_descriptors(backend, &descriptor_targets).await?;
    let mut descriptors: BTreeMap<(String, String), FileDescriptorRow> = BTreeMap::new();
    for ((version_id, file_id), path) in descriptor_paths {
        descriptors.insert(
            (version_id.clone(), file_id.clone()),
            FileDescriptorRow {
                file_id,
                version_id,
                path,
            },
        );
    }

    let mut writes_by_target: BTreeMap<(String, String, String), Vec<&MaterializationWrite>> =
        BTreeMap::new();
    for write in &plan.writes {
        if write.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY {
            continue;
        }
        writes_by_target
            .entry((
                write.version_id.clone(),
                write.file_id.clone(),
                write.plugin_key.clone(),
            ))
            .or_default()
            .push(write);
    }

    let mut loaded_instances: BTreeMap<String, std::sync::Arc<dyn crate::WasmComponentInstance>> =
        BTreeMap::new();

    for descriptor in descriptors.values() {
        let plugin = select_plugin_for_file(descriptor, &installed_plugins);
        let target_plugin_key = plugin
            .map(|entry| entry.key.clone())
            .unwrap_or_else(|| BUILTIN_BINARY_FALLBACK_PLUGIN_KEY.to_string());
        let Some(grouped_writes) = writes_by_target.get(&(
            descriptor.version_id.clone(),
            descriptor.file_id.clone(),
            target_plugin_key.clone(),
        )) else {
            continue;
        };

        let mut seen_keys: BTreeSet<(String, String)> = BTreeSet::new();
        let mut changes: Vec<PluginEntityChange> = Vec::new();
        for write in grouped_writes {
            let dedupe_key = (write.schema_key.clone(), write.entity_id.clone());
            if !seen_keys.insert(dedupe_key.clone()) {
                return Err(LixError {
                    message: format!(
                        "plugin materialization: duplicate change key for plugin '{}' file '{}' version '{}': schema_key='{}' entity_id='{}'",
                        target_plugin_key,
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

        if plugin.is_none() {
            let blob_ref = builtin_binary_blob_ref_from_changes(&changes, &descriptor.file_id)?;
            if let Some(blob_ref) = blob_ref {
                let blob_data = load_binary_blob_data_by_hash(backend, &blob_ref.blob_hash)
                    .await?
                    .ok_or_else(|| LixError {
                        message: format!(
                            "plugin materialization: missing builtin binary blob payload for hash '{}' (file_id='{}' version_id='{}')",
                            blob_ref.blob_hash, descriptor.file_id, descriptor.version_id
                        ),
                    })?;
                upsert_file_cache_data(
                    backend,
                    &descriptor.file_id,
                    &descriptor.version_id,
                    &blob_data,
                )
                .await?;
            } else {
                delete_file_cache_data(backend, &descriptor.file_id, &descriptor.version_id)
                    .await?;
            }
            continue;
        }
        let plugin = plugin.expect("plugin must be present");

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

        let instance = if let Some(existing) = loaded_instances.get(&plugin.key) {
            existing.clone()
        } else {
            let loaded = runtime
                .init_component(plugin.wasm.clone(), WasmLimits::default())
                .await?;
            loaded_instances.insert(plugin.key.clone(), loaded.clone());
            loaded
        };
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

pub(crate) async fn materialize_missing_file_data_with_plugins(
    backend: &dyn LixBackend,
    runtime: &dyn WasmRuntime,
    versions: Option<&BTreeSet<String>>,
) -> Result<(), LixError> {
    let descriptors = load_missing_file_descriptors(backend, versions).await?;
    if descriptors.is_empty() {
        return Ok(());
    }

    let installed_plugins = load_installed_plugins(backend).await?;

    let mut loaded_instances: BTreeMap<String, std::sync::Arc<dyn crate::WasmComponentInstance>> =
        BTreeMap::new();

    for descriptor in descriptors.values() {
        let plugin = select_plugin_for_file(descriptor, &installed_plugins);
        if plugin.is_none() {
            let blob_ref = load_builtin_binary_blob_ref_for_file(
                backend,
                &descriptor.file_id,
                &descriptor.version_id,
            )
            .await?;
            let Some(blob_ref) = blob_ref else {
                continue;
            };
            let blob_data = load_binary_blob_data_by_hash(backend, &blob_ref.blob_hash)
                .await?
                .ok_or_else(|| LixError {
                    message: format!(
                        "plugin materialization: missing builtin binary blob payload for hash '{}' (file_id='{}' version_id='{}')",
                        blob_ref.blob_hash, descriptor.file_id, descriptor.version_id
                    ),
                })?;
            upsert_file_cache_data(
                backend,
                &descriptor.file_id,
                &descriptor.version_id,
                &blob_data,
            )
            .await?;
            continue;
        }
        let plugin = plugin.expect("plugin must be present");

        let changes = load_plugin_state_changes_for_file(
            backend,
            &descriptor.file_id,
            &descriptor.version_id,
            &plugin.key,
        )
        .await?;
        if changes.is_empty() {
            continue;
        }

        let payload = serde_json::to_vec(&ApplyChangesRequest {
            file: PluginFile {
                id: descriptor.file_id.clone(),
                path: descriptor.path.clone(),
                data: Vec::new(),
            },
            changes,
        })
        .map_err(|error| LixError {
            message: format!(
                "plugin materialization: failed to encode on-demand apply-changes payload: {error}"
            ),
        })?;

        let instance = if let Some(existing) = loaded_instances.get(&plugin.key) {
            existing.clone()
        } else {
            let loaded = runtime
                .init_component(plugin.wasm.clone(), WasmLimits::default())
                .await?;
            loaded_instances.insert(plugin.key.clone(), loaded.clone());
            loaded
        };
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

pub(crate) async fn materialize_missing_file_history_data_with_plugins(
    backend: &dyn LixBackend,
    runtime: &dyn WasmRuntime,
) -> Result<(), LixError> {
    let descriptors = load_missing_file_history_descriptors(backend).await?;
    if descriptors.is_empty() {
        return Ok(());
    }

    let installed_plugins = load_installed_plugins(backend).await?;

    let mut loaded_instances: BTreeMap<String, std::sync::Arc<dyn crate::WasmComponentInstance>> =
        BTreeMap::new();

    for descriptor in descriptors.values() {
        let plugin = select_plugin_for_path(&descriptor.path, None, &installed_plugins);
        if plugin.is_none() {
            let changes = load_plugin_state_changes_for_file_at_history_slice(
                backend,
                &descriptor.file_id,
                BUILTIN_BINARY_FALLBACK_PLUGIN_KEY,
                &descriptor.root_commit_id,
                &descriptor.commit_id,
                descriptor.depth,
            )
            .await?;
            let Some(blob_ref) =
                builtin_binary_blob_ref_from_changes(&changes, &descriptor.file_id)?
            else {
                continue;
            };
            let blob_data = load_binary_blob_data_by_hash(backend, &blob_ref.blob_hash)
                .await?
                .ok_or_else(|| LixError {
                    message: format!(
                        "plugin materialization: missing builtin binary history blob payload for hash '{}' (file_id='{}' root_commit_id='{}' depth='{}')",
                        blob_ref.blob_hash,
                        descriptor.file_id,
                        descriptor.root_commit_id,
                        descriptor.depth
                    ),
                })?;
            upsert_file_history_cache_data(
                backend,
                &descriptor.file_id,
                &descriptor.root_commit_id,
                descriptor.depth,
                &blob_data,
            )
            .await?;
            continue;
        }
        let plugin = plugin.expect("plugin must be present");

        let changes = load_plugin_state_changes_for_file_at_history_slice(
            backend,
            &descriptor.file_id,
            &plugin.key,
            &descriptor.root_commit_id,
            &descriptor.commit_id,
            descriptor.depth,
        )
        .await?;
        if changes.is_empty() {
            continue;
        }

        let payload = serde_json::to_vec(&ApplyChangesRequest {
            file: PluginFile {
                id: descriptor.file_id.clone(),
                path: descriptor.path.clone(),
                data: Vec::new(),
            },
            changes,
        })
        .map_err(|error| LixError {
            message: format!(
                "plugin materialization: failed to encode history apply-changes payload: {error}"
            ),
        })?;

        let instance = if let Some(existing) = loaded_instances.get(&plugin.key) {
            existing.clone()
        } else {
            let loaded = runtime
                .init_component(plugin.wasm.clone(), WasmLimits::default())
                .await?;
            loaded_instances.insert(plugin.key.clone(), loaded.clone());
            loaded
        };
        let output = call_apply_changes(instance.as_ref(), &payload).await?;
        upsert_file_history_cache_data(
            backend,
            &descriptor.file_id,
            &descriptor.root_commit_id,
            descriptor.depth,
            &output,
        )
        .await?;
    }

    Ok(())
}

fn select_plugin_for_file<'a>(
    descriptor: &FileDescriptorRow,
    plugins: &'a [InstalledPlugin],
) -> Option<&'a InstalledPlugin> {
    select_plugin_for_path(&descriptor.path, None, plugins)
}

fn select_plugin_for_path<'a>(
    path: &str,
    file_content_type: Option<PluginContentType>,
    plugins: &'a [InstalledPlugin],
) -> Option<&'a InstalledPlugin> {
    select_best_glob_match(
        path,
        file_content_type,
        plugins,
        |plugin| plugin.path_glob.as_str(),
        |plugin| plugin.content_type,
    )
}

fn classify_content_type_from_bytes(data: &[u8]) -> PluginContentType {
    if data.contains(&0) {
        return PluginContentType::Binary;
    }
    if std::str::from_utf8(data).is_ok() {
        PluginContentType::Text
    } else {
        PluginContentType::Binary
    }
}

fn plugin_content_type_label(content_type: PluginContentType) -> &'static str {
    match content_type {
        PluginContentType::Text => "text",
        PluginContentType::Binary => "binary",
    }
}

pub(crate) fn binary_blob_hash_hex(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

fn no_matching_plugin_for_detect_error(
    write: &FileChangeDetectionRequest,
    content_type: PluginContentType,
    reason: &str,
) -> LixError {
    let path = write.after_path.as_deref().unwrap_or("<none>");
    LixError {
        message: format!(
            "plugin detect-changes: no plugin matched file type for file_id='{}' version_id='{}' path='{}' content_type='{}': {}",
            write.file_id,
            write.version_id,
            path,
            plugin_content_type_label(content_type),
            reason,
        ),
    }
}

fn plugin_detect_emits_complete_diff(plugin: &InstalledPlugin) -> bool {
    plugin.key == "text_plugin"
}

async fn call_apply_changes(
    instance: &dyn crate::WasmComponentInstance,
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
    instance: &dyn crate::WasmComponentInstance,
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

async fn reconstruct_before_file_data_from_state(
    backend: &dyn LixBackend,
    instance: &dyn crate::WasmComponentInstance,
    plugin: &InstalledPlugin,
    file_id: &str,
    version_id: &str,
    path: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    let changes =
        load_plugin_state_changes_for_file(backend, file_id, version_id, &plugin.key).await?;
    if changes.is_empty() {
        return Ok(None);
    }

    let payload = serde_json::to_vec(&ApplyChangesRequest {
        file: PluginFile {
            id: file_id.to_string(),
            path: path.to_string(),
            data: Vec::new(),
        },
        changes,
    })
    .map_err(|error| LixError {
        message: format!("plugin detect-changes: failed to encode apply fallback payload: {error}"),
    })?;

    match call_apply_changes(instance, &payload).await {
        Ok(data) => Ok(Some(data)),
        Err(_) => Ok(None),
    }
}

async fn load_plugin_state_changes_for_file(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
    plugin_key: &str,
) -> Result<Vec<PluginEntityChange>, LixError> {
    let params = vec![
        Value::Text(file_id.to_string()),
        Value::Text(version_id.to_string()),
        Value::Text(plugin_key.to_string()),
    ];
    let preprocessed = preprocess_sql(
        backend,
        &CelEvaluator::new(),
        "SELECT entity_id, schema_key, schema_version, snapshot_content \
         FROM lix_state_by_version \
         WHERE file_id = $1 \
           AND version_id = $2 \
           AND plugin_key = $3 \
           AND snapshot_content IS NOT NULL \
         ORDER BY entity_id",
        &params,
    )
    .await?;
    let rows = backend
        .execute(&preprocessed.sql, preprocessed.single_statement_params()?)
        .await?;

    let mut changes = Vec::with_capacity(rows.rows.len());
    for row in rows.rows {
        changes.push(PluginEntityChange {
            entity_id: text_required(&row, 0, "entity_id")?,
            schema_key: text_required(&row, 1, "schema_key")?,
            schema_version: text_required(&row, 2, "schema_version")?,
            snapshot_content: Some(text_required(&row, 3, "snapshot_content")?),
        });
    }
    Ok(changes)
}

fn parse_builtin_binary_blob_ref_snapshot(
    raw_snapshot: &str,
) -> Result<BuiltinBinaryBlobRefSnapshot, LixError> {
    serde_json::from_str(raw_snapshot).map_err(|error| LixError {
        message: format!(
            "plugin materialization: builtin binary fallback snapshot_content is invalid JSON: {error}"
        ),
    })
}

fn builtin_binary_blob_ref_from_changes(
    changes: &[PluginEntityChange],
    file_id: &str,
) -> Result<Option<BuiltinBinaryBlobRefSnapshot>, LixError> {
    for change in changes {
        if change.schema_key != BUILTIN_BINARY_BLOB_REF_SCHEMA_KEY {
            continue;
        }
        if change.schema_version != BUILTIN_BINARY_BLOB_REF_SCHEMA_VERSION {
            return Err(LixError {
                message: format!(
                    "plugin materialization: builtin binary fallback schema version mismatch for file '{}' (got '{}', expected '{}')",
                    file_id, change.schema_version, BUILTIN_BINARY_BLOB_REF_SCHEMA_VERSION
                ),
            });
        }
        let Some(raw_snapshot) = change.snapshot_content.as_deref() else {
            continue;
        };
        let parsed = parse_builtin_binary_blob_ref_snapshot(raw_snapshot)?;
        if parsed.id != file_id {
            return Err(LixError {
                message: format!(
                    "plugin materialization: builtin binary fallback snapshot id '{}' does not match file_id '{}'",
                    parsed.id, file_id
                ),
            });
        }
        return Ok(Some(parsed));
    }
    Ok(None)
}

async fn load_builtin_binary_blob_ref_for_file(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
) -> Result<Option<BuiltinBinaryBlobRefSnapshot>, LixError> {
    let changes = load_plugin_state_changes_for_file(
        backend,
        file_id,
        version_id,
        BUILTIN_BINARY_FALLBACK_PLUGIN_KEY,
    )
    .await?;
    builtin_binary_blob_ref_from_changes(&changes, file_id)
}

fn resolve_state_context_columns(
    plugin: &InstalledPlugin,
) -> Result<Option<Vec<StateContextColumn>>, LixError> {
    let manifest: PluginManifest =
        serde_json::from_str(&plugin.manifest_json).map_err(|error| LixError {
            message: format!(
                "plugin detect-changes: invalid stored manifest_json for plugin '{}': {error}",
                plugin.key
            ),
        })?;

    let Some(state_context) = manifest
        .detect_changes
        .as_ref()
        .and_then(|config| config.state_context.as_ref())
    else {
        return Ok(None);
    };

    if !state_context.includes_active_state() {
        return Ok(None);
    }

    let mut columns = state_context
        .resolved_columns_or_default()
        .unwrap_or_else(|| StateContextColumn::default_active_state_columns().to_vec());

    if !columns.contains(&StateContextColumn::EntityId) {
        columns.insert(0, StateContextColumn::EntityId);
    }

    let mut deduped = Vec::new();
    for column in columns {
        if !deduped.contains(&column) {
            deduped.push(column);
        }
    }

    Ok(Some(deduped))
}

async fn load_active_state_context_rows(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
    plugin_key: &str,
    columns: &[StateContextColumn],
) -> Result<Vec<PluginActiveStateRow>, LixError> {
    if columns.is_empty() {
        return Ok(Vec::new());
    }

    let select_columns = columns
        .iter()
        .map(|column| state_context_column_sql_name(*column))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT {select_columns} \
         FROM lix_state_by_version \
         WHERE file_id = $1 \
           AND version_id = $2 \
           AND plugin_key = $3 \
           AND snapshot_content IS NOT NULL \
         ORDER BY entity_id"
    );

    let params = vec![
        Value::Text(file_id.to_string()),
        Value::Text(version_id.to_string()),
        Value::Text(plugin_key.to_string()),
    ];

    let preprocessed = preprocess_sql(backend, &CelEvaluator::new(), &sql, &params).await?;
    let rows = backend
        .execute(&preprocessed.sql, preprocessed.single_statement_params()?)
        .await?;

    let mut result = Vec::with_capacity(rows.rows.len());
    for row in rows.rows {
        let mut payload = PluginActiveStateRow::default();
        for (index, column) in columns.iter().enumerate() {
            match column {
                StateContextColumn::EntityId => {
                    payload.entity_id = text_required(&row, index, "entity_id")?;
                }
                StateContextColumn::SchemaKey => {
                    payload.schema_key = nullable_text(&row, index, "schema_key")?;
                }
                StateContextColumn::SchemaVersion => {
                    payload.schema_version = nullable_text(&row, index, "schema_version")?;
                }
                StateContextColumn::SnapshotContent => {
                    payload.snapshot_content = nullable_text(&row, index, "snapshot_content")?;
                }
                StateContextColumn::FileId => {
                    payload.file_id = nullable_text(&row, index, "file_id")?;
                }
                StateContextColumn::PluginKey => {
                    payload.plugin_key = nullable_text(&row, index, "plugin_key")?;
                }
                StateContextColumn::VersionId => {
                    payload.version_id = nullable_text(&row, index, "version_id")?;
                }
                StateContextColumn::ChangeId => {
                    payload.change_id = nullable_text(&row, index, "change_id")?;
                }
                StateContextColumn::Metadata => {
                    payload.metadata = nullable_text(&row, index, "metadata")?;
                }
                StateContextColumn::CreatedAt => {
                    payload.created_at = nullable_text(&row, index, "created_at")?;
                }
                StateContextColumn::UpdatedAt => {
                    payload.updated_at = nullable_text(&row, index, "updated_at")?;
                }
            }
        }
        if payload.entity_id.is_empty() {
            return Err(LixError {
                message: "plugin detect-changes: state_context row is missing required entity_id"
                    .to_string(),
            });
        }
        result.push(payload);
    }

    Ok(result)
}

fn state_context_column_sql_name(column: StateContextColumn) -> &'static str {
    match column {
        StateContextColumn::EntityId => "entity_id",
        StateContextColumn::SchemaKey => "schema_key",
        StateContextColumn::SchemaVersion => "schema_version",
        StateContextColumn::SnapshotContent => "snapshot_content",
        StateContextColumn::FileId => "file_id",
        StateContextColumn::PluginKey => "plugin_key",
        StateContextColumn::VersionId => "version_id",
        StateContextColumn::ChangeId => "change_id",
        StateContextColumn::Metadata => "metadata",
        StateContextColumn::CreatedAt => "created_at",
        StateContextColumn::UpdatedAt => "updated_at",
    }
}

async fn load_missing_file_descriptors(
    backend: &dyn LixBackend,
    versions: Option<&BTreeSet<String>>,
) -> Result<BTreeMap<(String, String), FileDescriptorRow>, LixError> {
    let mut sql = String::from(
        "SELECT id, lixcol_version_id, path \
         FROM lix_file_by_version \
         WHERE path IS NOT NULL \
           AND NOT EXISTS (\
               SELECT 1 \
               FROM lix_internal_file_data_cache cache \
               WHERE cache.file_id = id \
                 AND cache.version_id = lixcol_version_id\
           )",
    );
    let mut params = Vec::new();
    if let Some(versions) = versions {
        if versions.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut placeholders = Vec::with_capacity(versions.len());
        for version in versions {
            placeholders.push(format!("${}", params.len() + 1));
            params.push(Value::Text(version.clone()));
        }
        sql.push_str(" AND lixcol_version_id IN (");
        sql.push_str(&placeholders.join(", "));
        sql.push(')');
    }
    sql.push_str(" ORDER BY lixcol_version_id, id");

    let preprocessed = preprocess_sql(backend, &CelEvaluator::new(), &sql, &params).await?;
    let rows = backend
        .execute(&preprocessed.sql, preprocessed.single_statement_params()?)
        .await?;

    let mut descriptors: BTreeMap<(String, String), FileDescriptorRow> = BTreeMap::new();
    for row in rows.rows {
        let file_id = text_required(&row, 0, "id")?;
        let version_id = text_required(&row, 1, "lixcol_version_id")?;
        let path = text_required(&row, 2, "path")?;
        descriptors.insert(
            (version_id.clone(), file_id.clone()),
            FileDescriptorRow {
                file_id,
                version_id,
                path,
            },
        );
    }
    Ok(descriptors)
}

async fn load_file_paths_for_descriptors(
    backend: &dyn LixBackend,
    targets: &BTreeSet<(String, String)>,
) -> Result<BTreeMap<(String, String), String>, LixError> {
    if targets.is_empty() {
        return Ok(BTreeMap::new());
    }

    let mut sql = String::from("WITH requested(file_id, version_id) AS (VALUES ");
    let mut params = Vec::with_capacity(targets.len() * 2);
    for (index, (version_id, file_id)) in targets.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        let file_placeholder = params.len() + 1;
        params.push(Value::Text(file_id.clone()));
        let version_placeholder = params.len() + 1;
        params.push(Value::Text(version_id.clone()));
        sql.push_str(&format!(
            "(${}, ${})",
            file_placeholder, version_placeholder
        ));
    }
    sql.push_str(
        ") \
         SELECT f.id, f.lixcol_version_id, f.path \
         FROM lix_file_by_version f \
         JOIN requested r \
           ON r.file_id = f.id \
          AND r.version_id = f.lixcol_version_id \
         WHERE f.path IS NOT NULL \
         ORDER BY f.lixcol_version_id, f.id",
    );

    let preprocessed = preprocess_sql(backend, &CelEvaluator::new(), &sql, &params).await?;
    let rows = backend
        .execute(&preprocessed.sql, preprocessed.single_statement_params()?)
        .await?;

    let mut out = BTreeMap::new();
    for row in rows.rows {
        let file_id = text_required(&row, 0, "id")?;
        let version_id = text_required(&row, 1, "lixcol_version_id")?;
        let path = text_required(&row, 2, "path")?;
        out.insert((version_id, file_id), path);
    }
    Ok(out)
}

async fn load_missing_file_history_descriptors(
    backend: &dyn LixBackend,
) -> Result<BTreeMap<(String, String, i64), FileHistoryDescriptorRow>, LixError> {
    let sql = "SELECT \
                 id AS file_id, \
                 lixcol_root_commit_id AS root_commit_id, \
                 lixcol_depth AS depth, \
                 lixcol_commit_id AS commit_id, \
                 path \
               FROM lix_file_history \
               WHERE path IS NOT NULL \
                 AND NOT EXISTS (\
                   SELECT 1 \
                   FROM lix_internal_file_history_data_cache cache \
                   WHERE cache.file_id = id \
                     AND cache.root_commit_id = lixcol_root_commit_id \
                     AND cache.depth = lixcol_depth\
                 ) \
               ORDER BY lixcol_root_commit_id, lixcol_depth, id";

    let preprocessed = preprocess_sql(backend, &CelEvaluator::new(), sql, &[]).await?;
    let rows = backend
        .execute(&preprocessed.sql, preprocessed.single_statement_params()?)
        .await?;

    let mut descriptors: BTreeMap<(String, String, i64), FileHistoryDescriptorRow> =
        BTreeMap::new();
    for row in rows.rows {
        let file_id = text_required(&row, 0, "file_id")?;
        let root_commit_id = text_required(&row, 1, "root_commit_id")?;
        let depth = i64_required(&row, 2, "depth")?;
        let commit_id = text_required(&row, 3, "commit_id")?;
        let path = text_required(&row, 4, "path")?;
        descriptors.insert(
            (root_commit_id.clone(), file_id.clone(), depth),
            FileHistoryDescriptorRow {
                file_id,
                root_commit_id,
                depth,
                commit_id,
                path,
            },
        );
    }
    Ok(descriptors)
}

async fn load_plugin_state_changes_for_file_at_history_slice(
    backend: &dyn LixBackend,
    file_id: &str,
    plugin_key: &str,
    root_commit_id: &str,
    commit_id: &str,
    depth: i64,
) -> Result<Vec<PluginEntityChange>, LixError> {
    let params = vec![
        Value::Text(file_id.to_string()),
        Value::Text(plugin_key.to_string()),
        Value::Text(root_commit_id.to_string()),
        Value::Text(commit_id.to_string()),
        Value::Integer(depth),
    ];
    let preprocessed = preprocess_sql(
        backend,
        &CelEvaluator::new(),
        "WITH target_commit_depth AS (\
            SELECT COALESCE((\
              SELECT depth \
              FROM lix_internal_commit_ancestry \
              WHERE commit_id = $3 \
                AND ancestor_id = $4 \
              LIMIT 1\
            ), $5) AS raw_depth\
         ) \
         SELECT entity_id, schema_key, schema_version, snapshot_content, depth \
         FROM lix_state_history \
         WHERE file_id = $1 \
           AND plugin_key = $2 \
           AND root_commit_id = $3 \
           AND depth >= (SELECT raw_depth FROM target_commit_depth) \
         ORDER BY entity_id ASC, depth ASC",
        &params,
    )
    .await?;
    let rows = backend
        .execute(&preprocessed.sql, preprocessed.single_statement_params()?)
        .await?;

    let mut changes = Vec::new();
    let mut previous_entity_id: Option<String> = None;
    for row in rows.rows {
        let entity_id = text_required(&row, 0, "entity_id")?;
        if previous_entity_id
            .as_ref()
            .is_some_and(|previous| previous == &entity_id)
        {
            continue;
        }
        previous_entity_id = Some(entity_id.clone());
        changes.push(PluginEntityChange {
            entity_id,
            schema_key: text_required(&row, 1, "schema_key")?,
            schema_version: text_required(&row, 2, "schema_version")?,
            snapshot_content: nullable_text(&row, 3, "snapshot_content")?,
        });
    }
    Ok(changes)
}

struct PluginEntityKey {
    entity_id: String,
    schema_key: String,
    schema_version: String,
}

fn append_implicit_tombstones_for_projection(
    plugin_changes: &mut Vec<PluginEntityChange>,
    existing_entities: &[PluginEntityKey],
    plugin_change_keys: &mut BTreeSet<(String, String)>,
) {
    // Treat non-complete detect output as a delta by default.
    // Seed from existing entities so unchanged rows are preserved, then apply explicit
    // upserts/tombstones from plugin output.
    let mut full_after_keys = existing_entities
        .iter()
        .map(|existing| (existing.schema_key.clone(), existing.entity_id.clone()))
        .collect::<BTreeSet<_>>();

    for change in plugin_changes.iter() {
        let key = (change.schema_key.clone(), change.entity_id.clone());
        if change.snapshot_content.is_some() {
            full_after_keys.insert(key);
        } else {
            full_after_keys.remove(&key);
        }
    }

    for existing in existing_entities {
        let key = (existing.schema_key.clone(), existing.entity_id.clone());
        if !full_after_keys.contains(&key) && plugin_change_keys.insert(key) {
            plugin_changes.push(PluginEntityChange {
                entity_id: existing.entity_id.clone(),
                schema_key: existing.schema_key.clone(),
                schema_version: existing.schema_version.clone(),
                snapshot_content: None,
            });
        }
    }
}

async fn load_existing_plugin_entities(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
    plugin_key: &str,
) -> Result<Vec<PluginEntityKey>, LixError> {
    let changes =
        load_plugin_state_changes_for_file(backend, file_id, version_id, plugin_key).await?;
    Ok(changes
        .into_iter()
        .map(|change| PluginEntityKey {
            entity_id: change.entity_id,
            schema_key: change.schema_key,
            schema_version: change.schema_version,
        })
        .collect())
}

pub(crate) async fn load_installed_plugins(
    backend: &dyn LixBackend,
) -> Result<Vec<InstalledPlugin>, LixError> {
    let rows = backend
        .execute(
            "SELECT key, runtime, api_version, match_path_glob, entry, manifest_json, wasm \
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
    let path_glob = text_required(row, 3, "match_path_glob")?;
    let entry = text_required(row, 4, "entry")?;
    let manifest_json = text_required(row, 5, "manifest_json")?;
    let wasm = blob_required(row, 6, "wasm")?;
    let content_type = serde_json::from_str::<PluginManifest>(&manifest_json)
        .ok()
        .and_then(|manifest| manifest.file_match.content_type);

    Ok(InstalledPlugin {
        key,
        runtime,
        api_version,
        path_glob,
        content_type,
        entry,
        manifest_json,
        wasm,
    })
}

async fn load_binary_blob_data_by_hash(
    backend: &dyn LixBackend,
    blob_hash: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    let inline_result = backend
        .execute(
            "SELECT data \
             FROM lix_internal_binary_blob_store \
             WHERE blob_hash = $1 \
             LIMIT 1",
            &[Value::Text(blob_hash.to_string())],
        )
        .await?;

    if let Some(row) = inline_result.rows.first() {
        return Ok(Some(blob_required(row, 0, "data")?));
    }

    let manifest_rows = backend
        .execute(
            "SELECT size_bytes, chunk_count \
             FROM lix_internal_binary_blob_manifest \
             WHERE blob_hash = $1 \
             LIMIT 1",
            &[Value::Text(blob_hash.to_string())],
        )
        .await?;
    let Some(manifest_row) = manifest_rows.rows.first() else {
        return Ok(None);
    };
    let manifest_size_bytes = i64_required(manifest_row, 0, "size_bytes")?;
    let manifest_chunk_count = i64_required(manifest_row, 1, "chunk_count")?;
    if manifest_size_bytes < 0 || manifest_chunk_count < 0 {
        return Err(LixError {
            message: format!(
                "plugin materialization: invalid negative manifest values for blob hash '{}'",
                blob_hash
            ),
        });
    }

    let chunk_rows = backend
        .execute(
            "SELECT mc.chunk_index, mc.chunk_hash, mc.chunk_size, cs.data, cs.codec \
             FROM lix_internal_binary_blob_manifest_chunk mc \
             LEFT JOIN lix_internal_binary_chunk_store cs ON cs.chunk_hash = mc.chunk_hash \
             WHERE mc.blob_hash = $1 \
             ORDER BY mc.chunk_index ASC",
            &[Value::Text(blob_hash.to_string())],
        )
        .await?;

    let expected_chunk_count = usize::try_from(manifest_chunk_count).map_err(|_| LixError {
        message: format!(
            "plugin materialization: chunk count out of range for blob hash '{}'",
            blob_hash
        ),
    })?;
    if chunk_rows.rows.len() != expected_chunk_count {
        return Err(LixError {
            message: format!(
                "plugin materialization: chunk manifest mismatch for blob hash '{}': expected {} chunks, got {}",
                blob_hash,
                expected_chunk_count,
                chunk_rows.rows.len()
            ),
        });
    }

    let mut reconstructed = Vec::with_capacity(usize::try_from(manifest_size_bytes).unwrap_or(0));
    for (expected_index, row) in chunk_rows.rows.iter().enumerate() {
        let chunk_index = i64_required(row, 0, "chunk_index")?;
        let chunk_hash = text_required(row, 1, "chunk_hash")?;
        let chunk_size = i64_required(row, 2, "chunk_size")?;
        if chunk_index != expected_index as i64 {
            return Err(LixError {
                message: format!(
                    "plugin materialization: unexpected chunk order for blob hash '{}': expected index {}, got {}",
                    blob_hash, expected_index, chunk_index
                ),
            });
        }
        if chunk_size < 0 {
            return Err(LixError {
                message: format!(
                    "plugin materialization: invalid negative chunk size for blob hash '{}' chunk '{}'",
                    blob_hash, chunk_hash
                ),
            });
        }
        let chunk_data = blob_required(row, 3, "data").map_err(|_| LixError {
            message: format!(
                "plugin materialization: missing chunk payload for blob hash '{}' chunk '{}'",
                blob_hash, chunk_hash
            ),
        })?;
        let codec = nullable_text(row, 4, "codec")?;
        let expected_chunk_size = usize::try_from(chunk_size).map_err(|_| LixError {
            message: format!(
                "plugin materialization: chunk size out of range for blob hash '{}' chunk '{}': {}",
                blob_hash, chunk_hash, chunk_size
            ),
        })?;
        let decoded_chunk_data = decode_binary_chunk_payload(
            &chunk_data,
            codec.as_deref(),
            expected_chunk_size,
            blob_hash,
            &chunk_hash,
        )?;
        if decoded_chunk_data.len() as i64 != chunk_size {
            return Err(LixError {
                message: format!(
                    "plugin materialization: chunk size mismatch for blob hash '{}' chunk '{}': expected {}, got {}",
                    blob_hash,
                    chunk_hash,
                    chunk_size,
                    decoded_chunk_data.len()
                ),
            });
        }
        reconstructed.extend_from_slice(&decoded_chunk_data);
    }

    if reconstructed.len() as i64 != manifest_size_bytes {
        return Err(LixError {
            message: format!(
                "plugin materialization: reconstructed size mismatch for blob hash '{}': expected {}, got {}",
                blob_hash,
                manifest_size_bytes,
                reconstructed.len()
            ),
        });
    }

    Ok(Some(reconstructed))
}

fn decode_binary_chunk_payload(
    chunk_data: &[u8],
    codec: Option<&str>,
    expected_chunk_size: usize,
    blob_hash: &str,
    chunk_hash: &str,
) -> Result<Vec<u8>, LixError> {
    match codec {
        Some(BINARY_CHUNK_CODEC_RAW) => Ok(chunk_data.to_vec()),
        Some(BINARY_CHUNK_CODEC_ZSTD) => {
            decode_binary_chunk_zstd_payload(chunk_data, expected_chunk_size, blob_hash, chunk_hash)
        }
        Some(BINARY_CHUNK_CODEC_LEGACY) | None => {
            decode_legacy_binary_chunk_payload(chunk_data, expected_chunk_size, blob_hash, chunk_hash)
        }
        Some(other) => Err(LixError {
            message: format!(
                "plugin materialization: unsupported chunk codec '{}' for blob hash '{}' chunk '{}'",
                other, blob_hash, chunk_hash
            ),
        }),
    }
}

fn decode_legacy_binary_chunk_payload(
    chunk_data: &[u8],
    expected_chunk_size: usize,
    blob_hash: &str,
    chunk_hash: &str,
) -> Result<Vec<u8>, LixError> {
    if let Some(raw_payload) = chunk_data.strip_prefix(BINARY_CHUNK_CODEC_PREFIX_RAW) {
        return Ok(raw_payload.to_vec());
    }

    if let Some(compressed_payload) = chunk_data.strip_prefix(BINARY_CHUNK_CODEC_PREFIX_ZSTD) {
        return decode_binary_chunk_zstd_payload(
            compressed_payload,
            expected_chunk_size,
            blob_hash,
            chunk_hash,
        );
    }

    // Backward compatibility for unframed rows written before Phase 2.
    Ok(chunk_data.to_vec())
}

#[cfg(not(target_arch = "wasm32"))]
fn decode_binary_chunk_zstd_payload(
    compressed_payload: &[u8],
    expected_chunk_size: usize,
    blob_hash: &str,
    chunk_hash: &str,
) -> Result<Vec<u8>, LixError> {
    zstd::bulk::decompress(compressed_payload, expected_chunk_size).map_err(|error| LixError {
        message: format!(
            "plugin materialization: chunk decompression failed for blob hash '{}' chunk '{}': {error}",
            blob_hash, chunk_hash
        ),
    })
}

#[cfg(target_arch = "wasm32")]
fn decode_binary_chunk_zstd_payload(
    compressed_payload: &[u8],
    _expected_chunk_size: usize,
    blob_hash: &str,
    chunk_hash: &str,
) -> Result<Vec<u8>, LixError> {
    use std::io::Read as _;

    let mut decoder = ruzstd::decoding::StreamingDecoder::new(compressed_payload).map_err(
        |error| LixError {
            message: format!(
                "plugin materialization: chunk decompression failed for blob hash '{}' chunk '{}': {error}",
                blob_hash, chunk_hash
            ),
        },
    )?;

    let mut output = Vec::new();
    decoder.read_to_end(&mut output).map_err(|error| LixError {
        message: format!(
            "plugin materialization: chunk decompression failed for blob hash '{}' chunk '{}': {error}",
            blob_hash, chunk_hash
        ),
    })?;
    Ok(output)
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

async fn upsert_file_history_cache_data(
    backend: &dyn LixBackend,
    file_id: &str,
    root_commit_id: &str,
    depth: i64,
    data: &[u8],
) -> Result<(), LixError> {
    backend
        .execute(
            "INSERT INTO lix_internal_file_history_data_cache (file_id, root_commit_id, depth, data) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (file_id, root_commit_id, depth) DO UPDATE SET \
             data = EXCLUDED.data",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(root_commit_id.to_string()),
                Value::Integer(depth),
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

fn nullable_text(row: &[Value], index: usize, column: &str) -> Result<Option<String>, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            message: format!(
                "plugin materialization: row missing column '{column}' at index {index}"
            ),
        });
    };
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => Ok(Some(text.clone())),
        other => Err(LixError {
            message: format!(
                "plugin materialization: expected nullable text column '{column}' at index {index}, got {other:?}"
            ),
        }),
    }
}

fn i64_required(row: &[Value], index: usize, column: &str) -> Result<i64, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            message: format!(
                "plugin materialization: row missing column '{column}' at index {index}"
            ),
        });
    };
    match value {
        Value::Integer(number) => Ok(*number),
        other => Err(LixError {
            message: format!(
                "plugin materialization: expected integer column '{column}' at index {index}, got {other:?}"
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

#[cfg(test)]
mod tests {
    use super::{
        append_implicit_tombstones_for_projection, classify_content_type_from_bytes,
        load_or_init_plugin_component, resolve_state_context_columns, select_plugin_for_path,
        CachedPluginComponent, PluginEntityChange, PluginEntityKey,
    };
    use crate::plugin::matching::glob_matches_path;
    use crate::plugin::types::{
        InstalledPlugin, PluginContentType, PluginRuntime, StateContextColumn,
    };
    use crate::{LixError, WasmComponentInstance, WasmLimits, WasmRuntime};
    use async_trait::async_trait;
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Default)]
    struct CountingRuntime {
        init_calls: Arc<AtomicUsize>,
    }

    struct NoopComponent;

    fn test_plugin(
        key: &str,
        path_glob: &str,
        content_type: Option<PluginContentType>,
    ) -> InstalledPlugin {
        InstalledPlugin {
            key: key.to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: path_glob.to_string(),
            content_type,
            entry: "plugin.wasm".to_string(),
            manifest_json: "{}".to_string(),
            wasm: vec![1],
        }
    }

    #[async_trait(?Send)]
    impl WasmRuntime for CountingRuntime {
        async fn init_component(
            &self,
            _bytes: Vec<u8>,
            _limits: WasmLimits,
        ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
            self.init_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(NoopComponent))
        }
    }

    #[async_trait(?Send)]
    impl WasmComponentInstance for NoopComponent {
        async fn call(&self, _export: &str, _input: &[u8]) -> Result<Vec<u8>, LixError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn reconciliation_preserves_unchanged_entities_for_delta_output() {
        let mut plugin_changes = vec![PluginEntityChange {
            entity_id: "a".to_string(),
            schema_key: "json_pointer".to_string(),
            schema_version: "1".to_string(),
            snapshot_content: Some("{\"path\":\"/a\"}".to_string()),
        }];
        let existing = vec![
            PluginEntityKey {
                entity_id: "a".to_string(),
                schema_key: "json_pointer".to_string(),
                schema_version: "1".to_string(),
            },
            PluginEntityKey {
                entity_id: "b".to_string(),
                schema_key: "json_pointer".to_string(),
                schema_version: "1".to_string(),
            },
        ];
        let mut keys = plugin_changes
            .iter()
            .map(|change| (change.schema_key.clone(), change.entity_id.clone()))
            .collect::<BTreeSet<_>>();

        append_implicit_tombstones_for_projection(&mut plugin_changes, &existing, &mut keys);

        let tombstone = plugin_changes
            .iter()
            .find(|change| change.entity_id == "b" && change.schema_key == "json_pointer");
        assert!(
            tombstone.is_none(),
            "delta output must not infer tombstones for unchanged entities"
        );
    }

    #[test]
    fn reconciliation_does_not_duplicate_explicit_tombstones() {
        let mut plugin_changes = vec![PluginEntityChange {
            entity_id: "b".to_string(),
            schema_key: "json_pointer".to_string(),
            schema_version: "1".to_string(),
            snapshot_content: None,
        }];
        let existing = vec![PluginEntityKey {
            entity_id: "b".to_string(),
            schema_key: "json_pointer".to_string(),
            schema_version: "1".to_string(),
        }];
        let mut keys = plugin_changes
            .iter()
            .map(|change| (change.schema_key.clone(), change.entity_id.clone()))
            .collect::<BTreeSet<_>>();

        append_implicit_tombstones_for_projection(&mut plugin_changes, &existing, &mut keys);

        let tombstones = plugin_changes
            .iter()
            .filter(|change| {
                change.entity_id == "b"
                    && change.schema_key == "json_pointer"
                    && change.snapshot_content.is_none()
            })
            .count();
        assert_eq!(tombstones, 1);
    }

    #[test]
    fn match_path_glob_matches_paths() {
        assert!(glob_matches_path("*.{md,mdx}", "/notes.md"));
        assert!(glob_matches_path("*.{md,mdx}", "/notes.MDX"));
        assert!(glob_matches_path("docs/**/*.md", "docs/nested/readme.md"));
        assert!(glob_matches_path("**/*.mdx", "/docs/nested/readme.mdx"));
        assert!(!glob_matches_path("*.{md,mdx}", "/notes.json"));
        assert!(!glob_matches_path("docs/**/*.md", "notes/readme.md"));
    }

    #[test]
    fn match_path_glob_invalid_pattern_does_not_match() {
        assert!(!glob_matches_path("*.{md,mdx", "/notes.md"));
    }

    #[test]
    fn select_plugin_prefers_specific_glob_over_catch_all() {
        let plugins = vec![
            test_plugin("text_plugin", "*", None),
            test_plugin("plugin_md_v2", "*.{md,mdx}", None),
        ];

        let markdown_plugin = select_plugin_for_path("/docs/readme.md", None, &plugins)
            .expect("markdown should match");
        assert_eq!(markdown_plugin.key, "plugin_md_v2");

        let fallback_plugin = select_plugin_for_path("/docs/data.json", None, &plugins)
            .expect("catch-all should match non-markdown");
        assert_eq!(fallback_plugin.key, "text_plugin");
    }

    #[test]
    fn select_plugin_applies_content_type_filter_when_available() {
        let plugins = vec![
            test_plugin("text_plugin", "*", Some(PluginContentType::Text)),
            test_plugin("binary_plugin", "*", Some(PluginContentType::Binary)),
        ];

        let selected = select_plugin_for_path(
            "/images/logo.png",
            Some(PluginContentType::Binary),
            &plugins,
        )
        .expect("binary plugin should match");
        assert_eq!(selected.key, "binary_plugin");
    }

    #[test]
    fn classify_content_type_detects_text_and_binary() {
        assert_eq!(
            classify_content_type_from_bytes(br#"{"hello":"world"}"#),
            PluginContentType::Text
        );
        assert_eq!(
            classify_content_type_from_bytes(&[0x89, 0x50, 0x4e, 0x47]),
            PluginContentType::Binary
        );
    }

    #[test]
    fn state_context_columns_disabled_by_default() {
        let plugin = InstalledPlugin {
            key: "k".to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: "*.md".to_string(),
            content_type: None,
            entry: "plugin.wasm".to_string(),
            manifest_json: r#"{
                "key":"k",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "match":{"path_glob":"*.md"}
            }"#
            .to_string(),
            wasm: vec![1],
        };

        let resolved = resolve_state_context_columns(&plugin).expect("resolution should succeed");
        assert_eq!(resolved, None);
    }

    #[test]
    fn state_context_columns_default_to_core_set() {
        let plugin = InstalledPlugin {
            key: "k".to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: "*.md".to_string(),
            content_type: None,
            entry: "plugin.wasm".to_string(),
            manifest_json: r#"{
                "key":"k",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "match":{"path_glob":"*.md"},
                "detect_changes": {
                    "state_context": {
                        "include_active_state": true
                    }
                }
            }"#
            .to_string(),
            wasm: vec![1],
        };

        let resolved = resolve_state_context_columns(&plugin).expect("resolution should succeed");
        assert_eq!(
            resolved,
            Some(vec![
                StateContextColumn::EntityId,
                StateContextColumn::SchemaKey,
                StateContextColumn::SchemaVersion,
                StateContextColumn::SnapshotContent
            ])
        );
    }

    #[test]
    fn state_context_columns_respect_explicit_manifest_selection() {
        let plugin = InstalledPlugin {
            key: "k".to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: "*.md".to_string(),
            content_type: None,
            entry: "plugin.wasm".to_string(),
            manifest_json: r#"{
                "key":"k",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "match":{"path_glob":"*.md"},
                "detect_changes": {
                    "state_context": {
                        "include_active_state": true,
                        "columns": ["entity_id", "snapshot_content"]
                    }
                }
            }"#
            .to_string(),
            wasm: vec![1],
        };

        let resolved = resolve_state_context_columns(&plugin).expect("resolution should succeed");
        assert_eq!(
            resolved,
            Some(vec![
                StateContextColumn::EntityId,
                StateContextColumn::SnapshotContent
            ])
        );
    }

    #[tokio::test]
    async fn component_cache_reinitializes_when_same_key_wasm_changes() {
        let runtime = CountingRuntime::default();
        let mut loaded = std::collections::BTreeMap::<String, CachedPluginComponent>::new();
        let mut plugin = InstalledPlugin {
            key: "k".to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: "*.json".to_string(),
            content_type: None,
            entry: "plugin.wasm".to_string(),
            manifest_json: "{}".to_string(),
            wasm: vec![1],
        };

        load_or_init_plugin_component(&runtime, &mut loaded, &plugin)
            .await
            .expect("first init should succeed");
        load_or_init_plugin_component(&runtime, &mut loaded, &plugin)
            .await
            .expect("second lookup should reuse cache");
        assert_eq!(runtime.init_calls.load(Ordering::SeqCst), 1);

        plugin.wasm = vec![2];
        load_or_init_plugin_component(&runtime, &mut loaded, &plugin)
            .await
            .expect("changed wasm should reinitialize instance");
        assert_eq!(runtime.init_calls.load(Ordering::SeqCst), 2);
    }
}
