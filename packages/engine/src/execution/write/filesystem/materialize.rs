use crate::binary_cas::read::load_binary_blob_data_by_hash;
use crate::contracts::artifacts::FilesystemProjectionScope;
use crate::contracts::plugin::{select_best_glob_match, InstalledPlugin, PluginContentType};
use crate::contracts::traits::FilesystemPluginMaterializer;
use crate::execution::write::filesystem::query::load_file_row_by_id;
use crate::live_state::{LiveStateRebuildPlan, LiveStateWrite, LiveStateWriteOp};
use crate::{LixBackend, LixError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const BUILTIN_BINARY_FALLBACK_PLUGIN_KEY: &str = "lix_builtin_binary_fallback";
const BUILTIN_BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const BUILTIN_BINARY_BLOB_REF_SCHEMA_VERSION: &str = "1";

#[derive(Debug, Clone)]
struct FileDescriptorRow {
    file_id: String,
    version_id: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BuiltinBinaryBlobRefSnapshot {
    id: String,
    blob_hash: String,
    size_bytes: u64,
}

pub(crate) async fn materialize_file_data_with_plugins(
    backend: &dyn LixBackend,
    plugin_materializer: &dyn FilesystemPluginMaterializer,
    plan: &LiveStateRebuildPlan,
) -> Result<(), LixError> {
    let installed_plugins = plugin_materializer.load_installed_plugins().await?;

    let mut descriptor_targets: BTreeSet<(String, String)> = BTreeSet::new();
    let mut tombstoned_files: Vec<(String, String)> = Vec::new();
    for write in &plan.writes {
        if write.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY {
            continue;
        }
        let key = (write.version_id.clone(), write.entity_id.clone());
        if write.op == LiveStateWriteOp::Tombstone {
            tombstoned_files.push((key.1.to_string(), key.0.to_string()));
            continue;
        }
        let Some(_) = write.snapshot_content.as_deref() else {
            continue;
        };
        descriptor_targets.insert((key.0.to_string(), key.1.to_string()));
    }

    for (file_id, version_id) in tombstoned_files {
        crate::live_state::delete_file_payload_cache_data(backend, &file_id, &version_id).await?;
    }

    let descriptors = load_file_descriptors(backend, &descriptor_targets).await?;

    let mut writes_by_target: BTreeMap<(String, String, String), Vec<&LiveStateWrite>> =
        BTreeMap::new();
    for write in &plan.writes {
        if write.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY {
            continue;
        }
        writes_by_target
            .entry((
                write.version_id.to_string(),
                write.file_id.to_string(),
                write.plugin_key.to_string(),
            ))
            .or_default()
            .push(write);
    }

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
            let dedupe_key = (write.schema_key.to_string(), write.entity_id.to_string());
            if !seen_keys.insert(dedupe_key.clone()) {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
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
                entity_id: write.entity_id.to_string(),
                schema_key: write.schema_key.to_string(),
                schema_version: write.schema_version.to_string(),
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
                    .ok_or_else(|| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: format!(
                            "plugin materialization: missing builtin binary blob payload for hash '{}' (file_id='{}' version_id='{}')",
                            blob_ref.blob_hash, descriptor.file_id, descriptor.version_id
                        ),
                    })?;
                crate::live_state::upsert_file_payload_cache_data(
                    backend,
                    &descriptor.file_id,
                    &descriptor.version_id,
                    &blob_data,
                )
                .await?;
            } else {
                crate::live_state::delete_file_payload_cache_data(
                    backend,
                    &descriptor.file_id,
                    &descriptor.version_id,
                )
                .await?;
            }
            continue;
        }
        let plugin = plugin.expect("plugin must be present");

        let previous_data = crate::live_state::load_file_payload_cache_data(
            backend,
            &descriptor.file_id,
            &descriptor.version_id,
        )
        .await?;
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
        let output = plugin_materializer
            .apply_plugin_changes(plugin, &payload)
            .await?;
        crate::live_state::upsert_file_payload_cache_data(
            backend,
            &descriptor.file_id,
            &descriptor.version_id,
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

fn parse_builtin_binary_blob_ref_snapshot(
    raw_snapshot: &str,
) -> Result<BuiltinBinaryBlobRefSnapshot, LixError> {
    serde_json::from_str(raw_snapshot).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
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
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
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
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "plugin materialization: builtin binary fallback snapshot id '{}' does not match file_id '{}'",
                    parsed.id, file_id
                ),
            });
        }
        return Ok(Some(parsed));
    }
    Ok(None)
}

async fn load_file_descriptors(
    backend: &dyn LixBackend,
    targets: &BTreeSet<(String, String)>,
) -> Result<BTreeMap<(String, String), FileDescriptorRow>, LixError> {
    let mut descriptors = BTreeMap::new();
    for (version_id, file_id) in targets {
        let row = load_file_row_by_id(
            backend,
            version_id,
            file_id,
            FilesystemProjectionScope::ExplicitVersion,
        )
        .await
        .map_err(|error| LixError::new("LIX_ERROR_UNKNOWN", error.message))?;
        let Some(row) = row else {
            continue;
        };
        if row.path.is_empty() {
            continue;
        }
        descriptors.insert(
            (version_id.clone(), file_id.clone()),
            FileDescriptorRow {
                file_id: row.id,
                version_id: version_id.clone(),
                path: row.path,
            },
        );
    }
    Ok(descriptors)
}

#[cfg(test)]
mod tests {
    use super::select_plugin_for_path;
    use crate::contracts::plugin::glob_matches_path;
    use crate::contracts::plugin::{InstalledPlugin, PluginContentType, PluginRuntime};

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
}
