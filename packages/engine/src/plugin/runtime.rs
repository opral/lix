use crate::cel::CelEvaluator;
use crate::materialization::{MaterializationPlan, MaterializationWrite, MaterializationWriteOp};
use crate::plugin::types::{InstalledPlugin, PluginRuntime};
use crate::sql::preprocess_sql;
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

        let instance = runtime
            .load_component(LoadWasmComponentRequest {
                key: plugin.key.clone(),
                bytes: plugin.wasm.clone(),
                world: PLUGIN_WORLD.to_string(),
                limits: WasmLimits::default(),
            })
            .await?;

        let mut before_data = write.before_data.clone();
        if before_data.is_none() {
            let cached = load_file_cache_data(backend, &write.file_id, &write.version_id).await?;
            if !cached.is_empty() {
                before_data = Some(cached);
            }
        }
        if before_data.is_none() {
            before_data = reconstruct_before_file_data_from_state(
                backend,
                instance.as_ref(),
                plugin,
                &write.file_id,
                &write.version_id,
                &write.path,
            )
            .await?;
        }

        let had_before_data = before_data.is_some();
        let before = before_data.as_ref().map(|data| PluginFile {
            id: write.file_id.clone(),
            path: write.path.clone(),
            data: data.clone(),
        });
        let after = PluginFile {
            id: write.file_id.clone(),
            path: write.path.clone(),
            data: write.after_data.clone(),
        };

        let detect_payload = serde_json::to_vec(&DetectChangesRequest {
            before: before.clone(),
            after: after.clone(),
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

        let full_after_changes = if had_before_data {
            let full_payload = serde_json::to_vec(&DetectChangesRequest {
                before: None,
                after: after.clone(),
            })
            .map_err(|error| LixError {
                message: format!(
                    "plugin detect-changes: failed to encode full-state payload: {error}"
                ),
            })?;
            let full_output = call_detect_changes(instance.as_ref(), &full_payload).await?;
            serde_json::from_slice::<Vec<PluginEntityChange>>(&full_output).map_err(|error| {
                LixError {
                    message: format!(
                        "plugin detect-changes: failed to decode full-state output for key '{}': {error}",
                        plugin.key
                    ),
                }
            })?
        } else {
            plugin_changes.clone()
        };
        let full_after_keys = full_after_changes
            .iter()
            .map(|change| (change.schema_key.clone(), change.entity_id.clone()))
            .collect::<BTreeSet<_>>();

        for existing in
            load_existing_plugin_entities(backend, &write.file_id, &write.version_id, &plugin.key)
                .await?
        {
            let key = (existing.schema_key.clone(), existing.entity_id.clone());
            if !full_after_keys.contains(&key) {
                plugin_changes.push(PluginEntityChange {
                    entity_id: existing.entity_id,
                    schema_key: existing.schema_key,
                    schema_version: existing.schema_version,
                    snapshot_content: None,
                });
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

pub(crate) async fn materialize_file_data_with_plugins(
    backend: &dyn LixBackend,
    runtime: &dyn WasmRuntime,
    plan: &MaterializationPlan,
) -> Result<(), LixError> {
    let installed_plugins = load_installed_plugins(backend).await?;
    if installed_plugins.is_empty() {
        return Ok(());
    }

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
                extension: file_extension_from_path(&path),
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

    let mut loaded_instances: BTreeMap<String, std::sync::Arc<dyn crate::WasmInstance>> =
        BTreeMap::new();

    for descriptor in descriptors.values() {
        let Some(plugin) = select_plugin_for_file(descriptor, &installed_plugins) else {
            continue;
        };

        let Some(grouped_writes) = writes_by_target.get(&(
            descriptor.version_id.clone(),
            descriptor.file_id.clone(),
            plugin.key.clone(),
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

        let instance = if let Some(existing) = loaded_instances.get(&plugin.key) {
            existing.clone()
        } else {
            let loaded = runtime
                .load_component(LoadWasmComponentRequest {
                    key: plugin.key.clone(),
                    bytes: plugin.wasm.clone(),
                    world: PLUGIN_WORLD.to_string(),
                    limits: WasmLimits::default(),
                })
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
    let installed_plugins = load_installed_plugins(backend).await?;
    if installed_plugins.is_empty() {
        return Ok(());
    }

    let descriptors = load_missing_file_descriptors(backend, versions).await?;
    if descriptors.is_empty() {
        return Ok(());
    }

    let mut loaded_instances: BTreeMap<String, std::sync::Arc<dyn crate::WasmInstance>> =
        BTreeMap::new();

    for descriptor in descriptors.values() {
        let Some(plugin) = select_plugin_for_file(descriptor, &installed_plugins) else {
            continue;
        };

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
                .load_component(LoadWasmComponentRequest {
                    key: plugin.key.clone(),
                    bytes: plugin.wasm.clone(),
                    world: PLUGIN_WORLD.to_string(),
                    limits: WasmLimits::default(),
                })
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
    let installed_plugins = load_installed_plugins(backend).await?;
    if installed_plugins.is_empty() {
        return Ok(());
    }

    let descriptors = load_missing_file_history_descriptors(backend).await?;
    if descriptors.is_empty() {
        return Ok(());
    }

    let mut loaded_instances: BTreeMap<String, std::sync::Arc<dyn crate::WasmInstance>> =
        BTreeMap::new();

    for descriptor in descriptors.values() {
        let Some(plugin) = select_plugin_for_path(&descriptor.path, &installed_plugins) else {
            continue;
        };

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
                .load_component(LoadWasmComponentRequest {
                    key: plugin.key.clone(),
                    bytes: plugin.wasm.clone(),
                    world: PLUGIN_WORLD.to_string(),
                    limits: WasmLimits::default(),
                })
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

fn normalize_extension(value: Option<String>) -> Option<String> {
    value
        .map(|entry| entry.trim().trim_start_matches('.').to_ascii_lowercase())
        .filter(|entry| !entry.is_empty())
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

async fn reconstruct_before_file_data_from_state(
    backend: &dyn LixBackend,
    instance: &dyn crate::WasmInstance,
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
        .execute(&preprocessed.sql, &preprocessed.params)
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
        .execute(&preprocessed.sql, &preprocessed.params)
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
                extension: file_extension_from_path(&path),
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
        .execute(&preprocessed.sql, &preprocessed.params)
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
        .execute(&preprocessed.sql, &preprocessed.params)
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
            SELECT MIN(depth) AS raw_depth \
            FROM lix_state_history \
            WHERE file_id = $1 \
              AND root_commit_id = $3 \
              AND commit_id = $4\
         ) \
         SELECT entity_id, schema_key, schema_version, snapshot_content, depth \
         FROM lix_state_history \
         WHERE file_id = $1 \
           AND plugin_key = $2 \
           AND root_commit_id = $3 \
           AND depth >= COALESCE((SELECT raw_depth FROM target_commit_depth), $5) \
         ORDER BY entity_id ASC, depth ASC",
        &params,
    )
    .await?;
    let rows = backend
        .execute(&preprocessed.sql, &preprocessed.params)
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
