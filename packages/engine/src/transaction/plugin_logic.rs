use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::binary_cas::{BlobDataReader, BlobHash};
use crate::entity_identity::EntityIdentity;
use crate::live_state::{LiveStateFilter, LiveStateReader, LiveStateScanRequest};
use crate::plugin::{
    parse_plugin_manifest_json, InstalledPlugin, PluginActiveStateRow, PluginContext,
    PluginDetectChangesInput, PluginDetectStateContext, PluginEntityChange, PluginFileInput,
    StateContextColumn,
};
use crate::sql2::filesystem_visibility::VisibleFilesystem;
use crate::transaction::types::{TransactionFileData, TransactionJson, TransactionWriteRow};
use crate::{LixError, NullableKeyFilter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingPluginDetection {
    pub(crate) file_id: String,
    pub(crate) path: String,
    pub(crate) version_id: String,
    pub(crate) global: bool,
    pub(crate) untracked: bool,
    pub(crate) data: Vec<u8>,
    pub(crate) before: Option<PluginFileInput>,
    pub(crate) state_context: Option<PluginDetectStateContext>,
    pub(crate) plugin: InstalledPlugin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginFileWriteInput {
    pub(crate) file_id: String,
    pub(crate) path: String,
    pub(crate) version_id: String,
    pub(crate) global: bool,
    pub(crate) untracked: bool,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginFileDeleteInput {
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) global: bool,
    pub(crate) untracked: bool,
}

pub(crate) async fn pending_plugin_detections_for_file_data_writes(
    plugin_context: &PluginContext,
    live_state: Arc<dyn LiveStateReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    file_data_writes: &[TransactionFileData],
) -> Result<Vec<PendingPluginDetection>, LixError> {
    let mut plugins_by_version = BTreeMap::<String, Vec<InstalledPlugin>>::new();
    let mut filesystem_by_version = BTreeMap::<String, VisibleFilesystem>::new();
    let mut pending = Vec::new();

    for write in file_data_writes {
        let plugins = match plugins_by_version.get(&write.version_id) {
            Some(plugins) => plugins,
            None => {
                let plugins = plugin_context
                    .load_installed_plugins_for_version(
                        Arc::clone(&live_state),
                        Arc::clone(&blob_reader),
                        &write.version_id,
                    )
                    .await?;
                plugins_by_version.insert(write.version_id.clone(), plugins);
                plugins_by_version
                    .get(&write.version_id)
                    .expect("plugins should exist after insertion")
            }
        };

        let Some(plugin) = plugin_context.select_plugin_for_file(plugins, &write.path, None) else {
            continue;
        };
        let filesystem = match filesystem_by_version.get(&write.version_id) {
            Some(filesystem) => filesystem,
            None => {
                let filesystem =
                    VisibleFilesystem::load(Arc::clone(&live_state), &write.version_id).await?;
                filesystem_by_version.insert(write.version_id.clone(), filesystem);
                filesystem_by_version
                    .get(&write.version_id)
                    .expect("filesystem should exist after insertion")
            }
        };
        let before =
            previous_file_input(Arc::clone(&blob_reader), filesystem, &write.file_id).await?;
        let state_context = detect_state_context_if_requested(
            Arc::clone(&live_state),
            plugin,
            &write.version_id,
            &write.file_id,
            write.untracked,
        )
        .await?;
        pending.push(PendingPluginDetection {
            file_id: write.file_id.clone(),
            path: write.path.clone(),
            version_id: write.version_id.clone(),
            global: write.global,
            untracked: write.untracked,
            data: write.data.clone(),
            before,
            state_context,
            plugin: plugin.clone(),
        });
    }

    Ok(pending)
}

pub(crate) fn plugin_detect_changes_input(
    detection: &PendingPluginDetection,
) -> PluginDetectChangesInput {
    PluginDetectChangesInput {
        before: detection.before.clone(),
        after: PluginFileInput {
            id: detection.file_id.clone(),
            path: detection.path.clone(),
            data: detection.data.clone(),
        },
        state_context: detection.state_context.clone(),
    }
}

pub(crate) fn plugin_changes_to_transaction_rows(
    plugin: &InstalledPlugin,
    file: PluginFileWriteInput,
    changes: Vec<PluginEntityChange>,
) -> Result<Vec<TransactionWriteRow>, LixError> {
    changes
        .into_iter()
        .map(|change| {
            if !plugin
                .schema_keys
                .iter()
                .any(|schema_key| schema_key == &change.schema_key)
            {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "plugin '{}' emitted schema '{}' that is not declared by the plugin",
                        plugin.key, change.schema_key
                    ),
                ));
            }
            let snapshot = match change.snapshot_content {
                Some(content) => {
                    let value = serde_json::from_str(&content).map_err(|error| {
                        LixError::new(
                            LixError::CODE_UNKNOWN,
                            format!("plugin detect_changes snapshot_content must be JSON: {error}"),
                        )
                    })?;
                    Some(TransactionJson::from_value(
                        value,
                        "plugin detect_changes snapshot_content",
                    )?)
                }
                None => None,
            };
            Ok(TransactionWriteRow {
                entity_id: Some(EntityIdentity::single(change.entity_id)),
                schema_key: change.schema_key,
                file_id: Some(file.file_id.clone()),
                snapshot,
                metadata: None,
                origin: None,
                created_at: None,
                updated_at: None,
                global: file.global,
                change_id: None,
                commit_id: None,
                untracked: file.untracked,
                version_id: file.version_id.clone(),
            })
        })
        .collect()
}

pub(crate) async fn plugin_tombstone_rows_for_file_deletes(
    plugin_context: &PluginContext,
    live_state: Arc<dyn LiveStateReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    file_deletes: &[PluginFileDeleteInput],
) -> Result<Vec<TransactionWriteRow>, LixError> {
    let mut plugins_by_version = BTreeMap::<String, Vec<InstalledPlugin>>::new();
    let mut filesystem_by_version = BTreeMap::<String, VisibleFilesystem>::new();
    let mut rows = Vec::new();

    for delete in file_deletes {
        let filesystem = match filesystem_by_version.get(&delete.version_id) {
            Some(filesystem) => filesystem,
            None => {
                let filesystem =
                    VisibleFilesystem::load(Arc::clone(&live_state), &delete.version_id).await?;
                filesystem_by_version.insert(delete.version_id.clone(), filesystem);
                filesystem_by_version
                    .get(&delete.version_id)
                    .expect("filesystem should exist after insertion")
            }
        };
        if !filesystem
            .blob_refs_by_file_id
            .contains_key(&delete.file_id)
        {
            continue;
        }
        let path = visible_file_path(filesystem, &delete.file_id)?;
        let plugins = match plugins_by_version.get(&delete.version_id) {
            Some(plugins) => plugins,
            None => {
                let plugins = plugin_context
                    .load_installed_plugins_for_version(
                        Arc::clone(&live_state),
                        Arc::clone(&blob_reader),
                        &delete.version_id,
                    )
                    .await?;
                plugins_by_version.insert(delete.version_id.clone(), plugins);
                plugins_by_version
                    .get(&delete.version_id)
                    .expect("plugins should exist after insertion")
            }
        };
        let Some(plugin) = plugin_context.select_plugin_for_file(plugins, &path, None) else {
            continue;
        };
        rows.extend(
            active_plugin_rows(
                Arc::clone(&live_state),
                plugin,
                &delete.version_id,
                &delete.file_id,
                delete.untracked,
            )
            .await?
            .into_iter()
            .map(|row| TransactionWriteRow {
                entity_id: Some(row.entity_id),
                schema_key: row.schema_key,
                file_id: Some(delete.file_id.clone()),
                snapshot: None,
                metadata: None,
                origin: None,
                created_at: None,
                updated_at: None,
                global: delete.global,
                change_id: None,
                commit_id: None,
                untracked: delete.untracked,
                version_id: delete.version_id.clone(),
            }),
        );
    }

    Ok(rows)
}

async fn previous_file_input(
    blob_reader: Arc<dyn BlobDataReader>,
    filesystem: &VisibleFilesystem,
    file_id: &str,
) -> Result<Option<PluginFileInput>, LixError> {
    let Some(blob_ref) = filesystem.blob_refs_by_file_id.get(file_id) else {
        return Ok(None);
    };
    let path = visible_file_path(filesystem, file_id)?;
    let hash = BlobHash::from_hex(&blob_ref.blob_hash)?;
    let bytes = blob_reader
        .load_bytes_many(&[hash])
        .await?
        .into_vec()
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_UNKNOWN,
                format!(
                    "plugin detect_changes: missing previous file bytes for '{}' ({})",
                    path, blob_ref.blob_hash
                ),
            )
        })?;
    Ok(Some(PluginFileInput {
        id: file_id.to_string(),
        path,
        data: bytes,
    }))
}

fn visible_file_path(filesystem: &VisibleFilesystem, file_id: &str) -> Result<String, LixError> {
    for files in filesystem.files_by_directory_id.values() {
        if let Some(file) = files.get(file_id) {
            return filesystem.file_path(file);
        }
    }
    Err(LixError::new(
        LixError::CODE_UNKNOWN,
        format!("plugin detect_changes: missing visible file descriptor for '{file_id}'"),
    ))
}

async fn detect_state_context_if_requested(
    live_state: Arc<dyn LiveStateReader>,
    plugin: &InstalledPlugin,
    version_id: &str,
    file_id: &str,
    untracked: bool,
) -> Result<Option<PluginDetectStateContext>, LixError> {
    let manifest = parse_plugin_manifest_json(&plugin.manifest_json)?.manifest;
    let Some(state_context_config) = manifest
        .detect_changes
        .as_ref()
        .and_then(|config| config.state_context.as_ref())
    else {
        return Ok(None);
    };
    let Some(columns) = state_context_config.resolved_columns_or_default() else {
        return Ok(None);
    };
    let column_set = columns.into_iter().collect::<BTreeSet<_>>();

    let rows = active_plugin_rows(live_state, plugin, version_id, file_id, untracked).await?;

    Ok(Some(PluginDetectStateContext {
        active_state: Some(
            rows.into_iter()
                .map(|row| {
                    Ok(PluginActiveStateRow {
                        entity_id: row.entity_id.as_single_string_owned()?,
                        schema_key: column_set
                            .contains(&StateContextColumn::SchemaKey)
                            .then_some(row.schema_key),
                        snapshot_content: column_set
                            .contains(&StateContextColumn::SnapshotContent)
                            .then_some(row.snapshot_content)
                            .flatten(),
                        file_id: column_set
                            .contains(&StateContextColumn::FileId)
                            .then_some(row.file_id)
                            .flatten(),
                        plugin_key: column_set
                            .contains(&StateContextColumn::PluginKey)
                            .then(|| plugin.key.clone()),
                        version_id: column_set
                            .contains(&StateContextColumn::VersionId)
                            .then_some(row.version_id),
                        change_id: column_set
                            .contains(&StateContextColumn::ChangeId)
                            .then_some(row.change_id)
                            .flatten(),
                        metadata: column_set
                            .contains(&StateContextColumn::Metadata)
                            .then_some(row.metadata)
                            .flatten(),
                        created_at: column_set
                            .contains(&StateContextColumn::CreatedAt)
                            .then_some(row.created_at),
                        updated_at: column_set
                            .contains(&StateContextColumn::UpdatedAt)
                            .then_some(row.updated_at),
                    })
                })
                .collect::<Result<Vec<_>, LixError>>()?,
        ),
    }))
}

async fn active_plugin_rows(
    live_state: Arc<dyn LiveStateReader>,
    plugin: &InstalledPlugin,
    version_id: &str,
    file_id: &str,
    untracked: bool,
) -> Result<Vec<crate::live_state::MaterializedLiveStateRow>, LixError> {
    if plugin.schema_keys.is_empty() {
        return Ok(Vec::new());
    }
    live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: plugin.schema_keys.clone(),
                version_ids: vec![version_id.to_string()],
                file_ids: vec![NullableKeyFilter::Value(file_id.to_string())],
                untracked: Some(untracked),
                include_tombstones: false,
                ..LiveStateFilter::default()
            },
            ..LiveStateScanRequest::default()
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::{PluginContentType, PluginRuntime};

    #[tokio::test]
    async fn selects_matching_plugin_for_file_data_write_path() {
        let plugin = InstalledPlugin {
            key: "test_plugin_json".to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: "*.json".to_string(),
            content_type: Some(PluginContentType::Text),
            entry: "plugin.wasm".to_string(),
            schema_keys: vec!["test_json_entity".to_string()],
            manifest_json: "{}".to_string(),
            wasm: b"\0asm\x01\0\0\0".to_vec(),
        };
        let file_data = TransactionFileData {
            file_id: "file-1".to_string(),
            path: "/foo.json".to_string(),
            version_id: "version-a".to_string(),
            global: false,
            untracked: false,
            data: br#"{"hello":"world"}"#.to_vec(),
        };

        let plugins = [plugin];
        let selected = crate::plugin::select_plugin_for_file(&plugins, &file_data.path, None)
            .expect("matching plugin should be selected for lix_file bytes");

        assert_eq!(selected.key, "test_plugin_json");
    }

    #[test]
    fn rejects_plugin_changes_for_undeclared_schema() {
        let plugin = InstalledPlugin {
            key: "test_plugin_json".to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: "*.json".to_string(),
            content_type: Some(PluginContentType::Text),
            entry: "plugin.wasm".to_string(),
            schema_keys: vec!["test_json_entity".to_string()],
            manifest_json: "{}".to_string(),
            wasm: b"\0asm\x01\0\0\0".to_vec(),
        };

        let error = plugin_changes_to_transaction_rows(
            &plugin,
            PluginFileWriteInput {
                file_id: "file-1".to_string(),
                path: "/foo.json".to_string(),
                version_id: "version-a".to_string(),
                global: false,
                untracked: false,
                data: br#"{"hello":"world"}"#.to_vec(),
            },
            vec![PluginEntityChange {
                entity_id: "entity-1".to_string(),
                schema_key: "other_schema".to_string(),
                snapshot_content: Some(r#"{"id":"entity-1"}"#.to_string()),
            }],
        )
        .expect_err("plugin must not emit undeclared schemas");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.message.contains("other_schema"));
    }
}
