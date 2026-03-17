use crate::cel::CelEvaluator;
use crate::filesystem::live_projection::{
    build_filesystem_file_history_projection_sql, build_filesystem_file_projection_sql,
    build_filesystem_state_history_source_sql, FilesystemProjectionScope,
};
use crate::plugin::manifest::parse_plugin_manifest_json;
use crate::plugin::matching::select_best_glob_match;
use crate::plugin::storage::plugin_key_from_archive_path;
use crate::plugin::types::{InstalledPlugin, PluginContentType};
use crate::schema::live_layout::tracked_live_table_name;
use crate::sql::ast::lowering::lower_statement;
use crate::sql::execution::parse::parse_sql;
use crate::sql::execution::preprocess::preprocess_sql_to_plan as preprocess_sql;
use crate::sql::public::runtime::lower_public_read_query_with_backend;
use crate::state::commit::build_exact_commit_depth_cte_sql;
use crate::state::materialization::{LiveStateRebuildPlan, LiveStateWrite, LiveStateWriteOp};
use crate::{LixBackend, LixError, Value, WasmLimits, WasmRuntime};
use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read};
use std::path::{Component, Path};
use std::sync::Arc;
use zip::read::ZipArchive;

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
pub(crate) const BUILTIN_BINARY_FALLBACK_PLUGIN_KEY: &str = "lix_builtin_binary_fallback";
const BUILTIN_BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const BUILTIN_BINARY_BLOB_REF_SCHEMA_VERSION: &str = "1";
const APPLY_CHANGES_EXPORTS: &[&str] = &["apply-changes", "api#apply-changes"];
const BINARY_CHUNK_CODEC_LEGACY: &str = "legacy";
const BINARY_CHUNK_CODEC_RAW: &str = "raw";
const BINARY_CHUNK_CODEC_ZSTD: &str = "zstd";
const BINARY_CHUNK_CODEC_PREFIX_RAW: &[u8] = b"LIXRAW01";
const BINARY_CHUNK_CODEC_PREFIX_ZSTD: &[u8] = b"LIXZSTD1";

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BuiltinBinaryBlobRefSnapshot {
    pub(crate) id: String,
    pub(crate) blob_hash: String,
    pub(crate) size_bytes: u64,
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
    plan: &LiveStateRebuildPlan,
) -> Result<(), LixError> {
    let installed_plugins = load_installed_plugins(backend).await?;

    let mut descriptor_targets: BTreeSet<(String, String)> = BTreeSet::new();
    let mut tombstoned_files: Vec<(String, String)> = Vec::new();
    for write in &plan.writes {
        if write.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY {
            continue;
        }
        let key = (write.version_id.clone(), write.entity_id.clone());
        if write.op == LiveStateWriteOp::Tombstone {
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

    let mut writes_by_target: BTreeMap<(String, String, String), Vec<&LiveStateWrite>> =
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

    let mut loaded_instances: BTreeMap<String, CachedPluginComponent> = BTreeMap::new();

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
                return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
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
                snapshot_content: if write.op == LiveStateWriteOp::Tombstone {
                    None
                } else {
                    write
                        .snapshot_content
                        .as_ref()
                        .map(|value| value.to_string())
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
                    .ok_or_else(|| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
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
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: failed to encode apply-changes payload: {error}"
            ),
        })?;

        let instance =
            load_or_init_plugin_component(runtime, &mut loaded_instances, plugin).await?;
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

    let mut loaded_instances: BTreeMap<String, CachedPluginComponent> = BTreeMap::new();

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
                .ok_or_else(|| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
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
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: failed to encode history apply-changes payload: {error}"
            ),
        })?;

        let instance =
            load_or_init_plugin_component(runtime, &mut loaded_instances, plugin).await?;
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

pub(crate) fn binary_blob_hash_hex(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

async fn call_apply_changes(
    instance: &dyn crate::WasmComponentInstance,
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

fn parse_builtin_binary_blob_ref_snapshot(
    raw_snapshot: &str,
) -> Result<BuiltinBinaryBlobRefSnapshot, LixError> {
    serde_json::from_str(raw_snapshot).map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
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
            return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
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
            return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                    "plugin materialization: builtin binary fallback snapshot id '{}' does not match file_id '{}'",
                    parsed.id, file_id
                ),
            });
        }
        return Ok(Some(parsed));
    }
    Ok(None)
}

async fn load_file_paths_for_descriptors(
    backend: &dyn LixBackend,
    targets: &BTreeSet<(String, String)>,
) -> Result<BTreeMap<(String, String), String>, LixError> {
    if targets.is_empty() {
        return Ok(BTreeMap::new());
    }

    let file_projection_sql = build_filesystem_file_projection_sql(
        FilesystemProjectionScope::ExplicitVersion,
        false,
        backend.dialect(),
    );
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
         FROM (",
    );
    sql.push_str(&file_projection_sql);
    sql.push_str(
        ") f \
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
    let commit_table = tracked_live_table_name("lix_commit");
    let history_source_sql =
        build_filesystem_state_history_source_sql(backend.dialect(), "", "", "", false);
    let history_projection_sql = build_filesystem_file_history_projection_sql(&history_source_sql);
    let sql = format!(
        "SELECT \
             history.id AS file_id, \
             history.lixcol_root_commit_id AS root_commit_id, \
             history.lixcol_depth AS depth, \
             history.lixcol_commit_id AS commit_id, \
             history.path \
           FROM ({history_projection_sql}) history \
           WHERE history.path IS NOT NULL \
             AND history.lixcol_root_commit_id IN (\
               SELECT entity_id \
               FROM {commit_table} \
               WHERE schema_key = 'lix_commit' \
                 AND version_id = 'global' \
                 AND is_tombstone = 0\
             ) \
             AND NOT EXISTS (\
               SELECT 1 \
               FROM lix_internal_file_history_data_cache cache \
               WHERE cache.file_id = history.id \
                 AND cache.root_commit_id = history.lixcol_root_commit_id \
                 AND cache.depth = history.lixcol_depth\
             ) \
           ORDER BY history.lixcol_root_commit_id, history.lixcol_depth, history.id"
    );

    let preprocessed = preprocess_sql(backend, &CelEvaluator::new(), &sql, &[]).await?;
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
    let sql = format!(
        "WITH {target_commit_depth_cte} \
         SELECT entity_id, schema_key, schema_version, snapshot_content, depth \
         FROM lix_state_history \
         WHERE file_id = $1 \
           AND plugin_key = $2 \
           AND root_commit_id = $3 \
           AND depth >= (SELECT raw_depth FROM target_commit_depth) \
         ORDER BY entity_id ASC, depth ASC",
        target_commit_depth_cte =
            build_exact_commit_depth_cte_sql(backend.dialect(), "$3", "$4", "$5")
                .trim_end_matches(", "),
    );
    let rows = execute_read_query_with_public_lowering(backend, &sql, &params).await?;

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

async fn execute_read_query_with_public_lowering(
    backend: &dyn LixBackend,
    sql: &str,
    params: &[Value],
) -> Result<crate::QueryResult, LixError> {
    let mut statements = parse_sql(sql).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "plugin runtime: failed to parse query for public lowering: {error:?}"
        ),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "plugin runtime: expected a single query statement".to_string(),
        });
    }
    let Statement::Query(query) = statements.remove(0) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "plugin runtime: expected a SELECT query for public lowering".to_string(),
        });
    };
    let lowered_query = lower_public_read_query_with_backend(backend, *query, params).await?;
    for schema_key in &lowered_query.required_schema_keys {
        crate::schema::registry::ensure_schema_live_table(backend, schema_key).await?;
    }
    let lowered_statement = lower_statement(
        Statement::Query(Box::new(lowered_query.query)),
        backend.dialect(),
    )?;
    backend
        .execute(&lowered_statement.to_string(), params)
        .await
}

pub(crate) async fn load_installed_plugins(
    backend: &dyn LixBackend,
) -> Result<Vec<InstalledPlugin>, LixError> {
    let rows = backend
        .execute(
            "SELECT binary_ref.file_id, path_cache.path, binary_ref.blob_hash \
             FROM lix_internal_binary_file_version_ref AS binary_ref \
             INNER JOIN lix_internal_file_path_cache AS path_cache \
                 ON path_cache.file_id = binary_ref.file_id \
                AND path_cache.version_id = binary_ref.version_id \
             WHERE binary_ref.version_id = 'global' \
               AND path_cache.path LIKE '/.lix/plugins/%.lixplugin' \
               AND path_cache.path NOT LIKE '/.lix/plugins/%/%' \
             ORDER BY path_cache.path",
            &[],
        )
        .await?;

    let mut plugins = Vec::with_capacity(rows.rows.len());
    for row in rows.rows {
        plugins.push(load_installed_plugin_from_blob_ref_row(backend, &row).await?);
    }
    Ok(plugins)
}

async fn load_installed_plugin_from_blob_ref_row(
    backend: &dyn LixBackend,
    row: &[Value],
) -> Result<InstalledPlugin, LixError> {
    let file_id = text_required(row, 0, "file_id")?;
    let archive_path = text_required(row, 1, "path")?;
    let blob_hash = text_required(row, 2, "blob_hash")?;
    let Some(plugin_key) = plugin_key_from_archive_path(&archive_path) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: unsupported plugin archive path '{}'",
                archive_path
            ),
        });
    };
    let archive_bytes = load_binary_blob_data_by_hash(backend, &blob_hash)
        .await?
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: missing plugin archive blob '{}' for file '{}' ({})",
                blob_hash, archive_path, file_id
            ),
        })?;
    if archive_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: archive '{}' is empty",
                archive_path
            ),
        });
    }
    parse_installed_plugin_from_archive_bytes(&plugin_key, &archive_path, &archive_bytes)
}

fn parse_installed_plugin_from_archive_bytes(
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
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
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
) -> Result<BTreeMap<String, Vec<u8>>, LixError> {
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
    let mut files = BTreeMap::<String, Vec<u8>>::new();

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
                return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
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
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
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
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "plugin materialization: chunk count out of range for blob hash '{}'",
            blob_hash
        ),
    })?;
    if chunk_rows.rows.len() != expected_chunk_count {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
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
            return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                    "plugin materialization: unexpected chunk order for blob hash '{}': expected index {}, got {}",
                    blob_hash, expected_index, chunk_index
                ),
            });
        }
        if chunk_size < 0 {
            return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                    "plugin materialization: invalid negative chunk size for blob hash '{}' chunk '{}'",
                    blob_hash, chunk_hash
                ),
            });
        }
        let chunk_data = blob_required(row, 3, "data").map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: missing chunk payload for blob hash '{}' chunk '{}'",
                blob_hash, chunk_hash
            ),
        })?;
        let codec = nullable_text(row, 4, "codec")?;
        let expected_chunk_size = usize::try_from(chunk_size).map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
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
            return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
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
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
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
        Some(other) => Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
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
    zstd::bulk::decompress(compressed_payload, expected_chunk_size).map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
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
        |error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                "plugin materialization: chunk decompression failed for blob hash '{}' chunk '{}': {error}",
                blob_hash, chunk_hash
            ),
        },
    )?;

    let mut output = Vec::new();
    decoder.read_to_end(&mut output).map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
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
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: row missing column '{column}' at index {index}"
            ),
        });
    };
    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                "plugin materialization: expected text column '{column}' at index {index}, got {other:?}"
            ),
        }),
    }
}

fn nullable_text(row: &[Value], index: usize, column: &str) -> Result<Option<String>, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: row missing column '{column}' at index {index}"
            ),
        });
    };
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => Ok(Some(text.clone())),
        other => Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                "plugin materialization: expected nullable text column '{column}' at index {index}, got {other:?}"
            ),
        }),
    }
}

fn i64_required(row: &[Value], index: usize, column: &str) -> Result<i64, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: row missing column '{column}' at index {index}"
            ),
        });
    };
    match value {
        Value::Integer(number) => Ok(*number),
        other => Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                "plugin materialization: expected integer column '{column}' at index {index}, got {other:?}"
            ),
        }),
    }
}

fn blob_required(row: &[Value], index: usize, column: &str) -> Result<Vec<u8>, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: row missing column '{column}' at index {index}"
            ),
        });
    };
    match value {
        Value::Blob(bytes) => Ok(bytes.clone()),
        other => Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                "plugin materialization: expected blob column '{column}' at index {index}, got {other:?}"
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        blob_required, load_or_init_plugin_component, select_plugin_for_path, CachedPluginComponent,
    };
    use crate::plugin::matching::glob_matches_path;
    use crate::plugin::types::{InstalledPlugin, PluginContentType, PluginRuntime};
    use crate::{LixError, Value, WasmComponentInstance, WasmLimits, WasmRuntime};
    use async_trait::async_trait;
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
    fn blob_required_rejects_text_values() {
        let err = blob_required(&[Value::Text("hello".to_string())], 0, "data")
            .expect_err("text should not be accepted as blob data");

        assert!(
            err.description
                .contains("expected blob column 'data' at index 0"),
            "unexpected error: {}",
            err.description
        );
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
