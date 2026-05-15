use serde::{Deserialize, Serialize};

use crate::common::LixError;

use super::component::load_or_init_plugin_component;
use super::component::PluginComponentHost;
use super::InstalledPlugin;

const DETECT_CHANGES_EXPORTS: &[&str] = &["detect-changes", "api#detect-changes"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginDetectChangesInput {
    pub(crate) before: Option<PluginFileInput>,
    pub(crate) after: PluginFileInput,
    pub(crate) state_context: Option<PluginDetectStateContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PluginFileInput {
    pub(crate) id: String,
    pub(crate) path: String,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PluginDetectStateContext {
    #[serde(default)]
    pub(crate) active_state: Option<Vec<PluginActiveStateRow>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PluginActiveStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
    pub(crate) version_id: Option<String>,
    pub(crate) change_id: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: Option<String>,
    pub(crate) updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PluginEntityChange {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) snapshot_content: Option<String>,
}

pub(crate) async fn detect_changes_with_plugin(
    host: &impl PluginComponentHost,
    plugin: &InstalledPlugin,
    input: PluginDetectChangesInput,
) -> Result<Vec<PluginEntityChange>, LixError> {
    let instance = load_or_init_plugin_component(host, plugin).await?;
    let payload = serde_json::to_vec(&DetectChangesCallInput::from(input)).map_err(|error| {
        LixError::new(
            LixError::CODE_UNKNOWN,
            format!("plugin detect_changes input serialization failed: {error}"),
        )
    })?;

    let mut errors = Vec::new();
    for export in DETECT_CHANGES_EXPORTS {
        match instance.call(export, &payload).await {
            Ok(output) => {
                return serde_json::from_slice(&output).map_err(|error| {
                    LixError::new(
                        LixError::CODE_UNKNOWN,
                        format!("plugin detect_changes output must be JSON: {error}"),
                    )
                });
            }
            Err(error) => errors.push(format!("{export}: {}", error.message)),
        }
    }

    Err(LixError::new(
        LixError::CODE_UNKNOWN,
        format!(
            "plugin detect_changes: failed to call detect-changes export ({})",
            errors.join("; ")
        ),
    ))
}

#[derive(Debug, Clone, Serialize)]
struct DetectChangesCallInput {
    before: Option<PluginFileInput>,
    after: PluginFileInput,
    state_context: Option<PluginDetectStateContext>,
}

impl From<PluginDetectChangesInput> for DetectChangesCallInput {
    fn from(input: PluginDetectChangesInput) -> Self {
        Self {
            before: input.before,
            after: input.after,
            state_context: input.state_context,
        }
    }
}
