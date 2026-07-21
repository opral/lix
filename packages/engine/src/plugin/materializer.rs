use std::collections::BTreeSet;
use std::sync::Arc;

use crate::LixError;
use crate::entity_pk::EntityPk;
use crate::live_state::{LiveStateProjection, MaterializedLiveStateRow};
use crate::wasm::{WasmComponentInstance, WasmPluginEntityState, WasmPluginFile};

use super::InstalledPlugin;
use super::component::{
    PluginComponentHost, detect_changes_with_plugin as detect_changes_with_component,
    render_with_plugin as render_with_component,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginDetectedChange {
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
}

pub(crate) async fn detect_changes_with_plugin(
    host: &impl PluginComponentHost,
    plugin: &InstalledPlugin,
    active_state: &[MaterializedLiveStateRow],
    file: WasmPluginFile,
) -> Result<Vec<PluginDetectedChange>, LixError> {
    let changes = detect_changes_with_component(
        host,
        plugin,
        plugin_entity_state_from_live_rows(active_state)?,
        file,
    )
    .await?;
    normalize_detected_changes(changes)
}

/// Executes a component that was resolved from the warm hash cache before any
/// CAS read. Keeping the exact `Arc` avoids a key-only cache race with another
/// branch that has a different component version under the same plugin key.
pub(crate) async fn detect_changes_with_component_instance(
    instance: &Arc<dyn WasmComponentInstance>,
    active_state: &[MaterializedLiveStateRow],
    file: WasmPluginFile,
) -> Result<Vec<PluginDetectedChange>, LixError> {
    let changes = instance
        .detect_changes(plugin_entity_state_from_live_rows(active_state)?, file)
        .await?;
    normalize_detected_changes(changes)
}

fn normalize_detected_changes(
    changes: Vec<crate::wasm::WasmPluginDetectedChange>,
) -> Result<Vec<PluginDetectedChange>, LixError> {
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
    render_with_component(
        host,
        plugin,
        plugin_entity_state_from_live_rows(active_state)?,
    )
    .await
}

pub(crate) async fn render_plugin_state_with_component_instance(
    instance: &Arc<dyn WasmComponentInstance>,
    active_state: &[MaterializedLiveStateRow],
) -> Result<Vec<u8>, LixError> {
    instance
        .render(plugin_entity_state_from_live_rows(active_state)?)
        .await
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

pub(crate) fn retain_plugin_state_rows(
    plugin: &InstalledPlugin,
    rows: Vec<MaterializedLiveStateRow>,
) -> Vec<MaterializedLiveStateRow> {
    retain_plugin_state_rows_for_schema_keys(&plugin.schema_keys, rows)
}

pub(crate) fn retain_plugin_state_rows_for_schema_keys(
    schema_keys: &[String],
    mut rows: Vec<MaterializedLiveStateRow>,
) -> Vec<MaterializedLiveStateRow> {
    let schema_keys = schema_keys.iter().collect::<BTreeSet<_>>();
    rows.retain(|row| schema_keys.contains(&row.schema_key) && row.snapshot_content.is_some());
    rows
}

pub(crate) fn plugin_state_live_state_projection() -> LiveStateProjection {
    LiveStateProjection {
        columns: vec!["snapshot_content".to_string(), "metadata".to_string()],
    }
}

fn plugin_entity_state_from_live_rows(
    rows: &[MaterializedLiveStateRow],
) -> Result<Vec<WasmPluginEntityState>, LixError> {
    rows.iter()
        .map(|row| {
            let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "plugin state row '{}' '{}' is missing snapshot_content",
                        row.schema_key,
                        row.entity_pk.as_json_array_text().unwrap_or_default()
                    ),
                )
            })?;
            Ok(WasmPluginEntityState {
                entity_pk: row.entity_pk.parts.clone(),
                schema_key: row.schema_key.clone(),
                snapshot_content: snapshot_content.to_string(),
                metadata: row.metadata.clone(),
            })
        })
        .collect()
}
