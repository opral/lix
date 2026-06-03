use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::LixError;
use crate::binary_cas::{BlobDataReader, BlobHash};
use crate::entity_pk::EntityPk;
use crate::filesystem::FilesystemIndex;
use crate::live_state::MaterializedLiveStateRow;

use super::component::{
    PluginComponentHost, detect_changes_with_plugin as detect_changes_with_component,
    render_with_plugin as render_with_component,
};
use super::{
    InstalledPlugin, load_installed_plugin_from_archive_bytes, plugin_key_from_archive_path,
    select_best_glob_match,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginDetectedChange {
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct PluginDetectChangesRequest {
    state: Vec<PluginEntityState>,
    file: PluginFile,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct PluginRenderRequest {
    state: Vec<PluginEntityState>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct PluginFile {
    data: Vec<u8>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct PluginEntityState {
    entity_pk: Vec<String>,
    schema_key: String,
    snapshot_content: String,
    metadata: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PluginDetectedChangeWire {
    entity_pk: Vec<String>,
    schema_key: String,
    snapshot_content: Option<String>,
    metadata: Option<String>,
}

pub(crate) async fn load_installed_plugins_from_filesystem(
    filesystem: &FilesystemIndex,
    blob_reader: &dyn BlobDataReader,
) -> Result<Vec<InstalledPlugin>, LixError> {
    let mut plugins = Vec::new();
    for (path, file) in filesystem.file_entries() {
        let Some(plugin_key) = plugin_key_from_archive_path(path) else {
            continue;
        };
        let Some(blob_hash) = file.blob_hash.as_deref() else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("installed plugin archive '{path}' is missing binary blob data"),
            ));
        };
        let hash = BlobHash::from_hex(blob_hash)?;
        let mut batch = blob_reader.load_bytes_many(&[hash]).await?.into_vec();
        let Some(archive_bytes) = batch.pop().flatten() else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("installed plugin archive '{path}' blob '{blob_hash}' is missing"),
            ));
        };
        plugins.push(load_installed_plugin_from_archive_bytes(
            &plugin_key,
            path,
            &archive_bytes,
        )?);
    }
    Ok(plugins)
}

pub(crate) fn select_plugin_for_path<'a>(
    plugins: &'a [InstalledPlugin],
    path: &str,
) -> Option<&'a InstalledPlugin> {
    // Plugin ownership is path-based. File bytes, especially empty bytes, are
    // not reliable for deciding which plugin is active for a path.
    select_best_glob_match(
        path,
        None::<()>,
        plugins,
        |plugin| plugin.path_glob.as_str(),
        |_plugin| None::<()>,
    )
}

pub(crate) async fn detect_changes_with_plugin(
    host: &impl PluginComponentHost,
    plugin: &InstalledPlugin,
    active_state: &[MaterializedLiveStateRow],
    file_data: Vec<u8>,
) -> Result<Vec<PluginDetectedChange>, LixError> {
    let request = PluginDetectChangesRequest {
        state: plugin_entity_state_from_live_rows(active_state)?,
        file: PluginFile { data: file_data },
    };
    let payload = serialize_plugin_payload(&request, "detect-changes")?;
    let output = detect_changes_with_component(host, plugin, &payload).await?;
    let changes: Vec<PluginDetectedChangeWire> =
        serde_json::from_slice(&output).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("plugin detect-changes returned invalid JSON: {error}"),
            )
        })?;
    changes
        .into_iter()
        .map(|change| {
            Ok(PluginDetectedChange {
                entity_pk: EntityPk::from_parts(change.entity_pk).map_err(|error| {
                    LixError::unknown(format!("plugin emitted invalid entity_pk: {error}"))
                })?,
                schema_key: change.schema_key,
                snapshot_content: change.snapshot_content,
                metadata: change.metadata,
            })
        })
        .collect()
}

pub(crate) async fn render_plugin_state(
    host: &impl PluginComponentHost,
    plugin: &InstalledPlugin,
    active_state: &[MaterializedLiveStateRow],
) -> Result<Vec<u8>, LixError> {
    let request = PluginRenderRequest {
        state: plugin_entity_state_from_live_rows(active_state)?,
    };
    let payload = serialize_plugin_payload(&request, "render")?;
    render_with_component(host, plugin, &payload).await
}

pub(crate) async fn render_materialized_plugin_file(
    host: &impl PluginComponentHost,
    plugin: &InstalledPlugin,
    active_state: &[MaterializedLiveStateRow],
) -> Result<Option<Vec<u8>>, LixError> {
    // A matching plugin is not enough: raw empty files also have no blob ref.
    // Durable plugin-owned state is the signal that the file was materialized.
    if active_state.is_empty() {
        return Ok(None);
    }

    Ok(Some(render_plugin_state(host, plugin, active_state).await?))
}

pub(crate) fn plugin_state_rows<'a>(
    plugin: &InstalledPlugin,
    rows: impl IntoIterator<Item = &'a MaterializedLiveStateRow>,
) -> Vec<MaterializedLiveStateRow> {
    let schema_keys = plugin.schema_keys.iter().collect::<BTreeSet<_>>();
    rows.into_iter()
        .filter(|row| schema_keys.contains(&row.schema_key) && row.snapshot_content.is_some())
        .cloned()
        .collect()
}

fn plugin_entity_state_from_live_rows(
    rows: &[MaterializedLiveStateRow],
) -> Result<Vec<PluginEntityState>, LixError> {
    rows.iter()
        .map(|row| {
            let snapshot_content = row.snapshot_content.clone().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "plugin state row '{}' '{}' is missing snapshot_content",
                        row.schema_key,
                        row.entity_pk.as_json_array_text().unwrap_or_default()
                    ),
                )
            })?;
            Ok(PluginEntityState {
                entity_pk: row.entity_pk.parts.clone(),
                schema_key: row.schema_key.clone(),
                snapshot_content,
                metadata: row.metadata.clone(),
            })
        })
        .collect()
}

fn serialize_plugin_payload<T: Serialize>(
    value: &T,
    export_name: &str,
) -> Result<Vec<u8>, LixError> {
    serde_json::to_vec(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to encode plugin {export_name} payload: {error}"),
        )
    })
}
