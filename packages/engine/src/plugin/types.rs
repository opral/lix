use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PluginRuntime {
    WasmComponentV1,
}

impl PluginRuntime {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WasmComponentV1 => "wasm-component-v1",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "wasm-component-v1" => Some(Self::WasmComponentV1),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginManifest {
    pub key: String,
    pub runtime: PluginRuntime,
    pub api_version: String,
    #[serde(rename = "match")]
    pub file_match: PluginMatch,
    #[serde(default)]
    pub detect_changes: Option<DetectChangesConfig>,
    #[serde(default)]
    pub entry: Option<String>,
}

impl PluginManifest {
    pub fn entry_or_default(&self) -> &str {
        self.entry.as_deref().unwrap_or("plugin.wasm")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginMatch {
    pub path_glob: String,
    #[serde(default)]
    pub content_type: Option<PluginContentType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginContentType {
    Text,
    Binary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPluginManifest {
    pub manifest: PluginManifest,
    pub normalized_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledPlugin {
    pub key: String,
    pub runtime: PluginRuntime,
    pub api_version: String,
    pub path_glob: String,
    pub content_type: Option<PluginContentType>,
    pub entry: String,
    pub manifest_json: String,
    pub wasm: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectChangesConfig {
    #[serde(default)]
    pub state_context: Option<DetectStateContextConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectStateContextConfig {
    #[serde(default)]
    pub include_active_state: Option<bool>,
    #[serde(default)]
    pub columns: Option<Vec<StateContextColumn>>,
}

#[allow(dead_code)]
impl DetectStateContextConfig {
    pub fn includes_active_state(&self) -> bool {
        self.include_active_state.unwrap_or(false)
    }

    pub fn resolved_columns_or_default(&self) -> Option<Vec<StateContextColumn>> {
        if !self.includes_active_state() {
            return None;
        }
        Some(
            self.columns
                .clone()
                .unwrap_or_else(|| StateContextColumn::default_active_state_columns().to_vec()),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateContextColumn {
    EntityId,
    SchemaKey,
    SchemaVersion,
    SnapshotContent,
    FileId,
    PluginKey,
    VersionId,
    ChangeId,
    Metadata,
    CreatedAt,
    UpdatedAt,
}

#[allow(dead_code)]
impl StateContextColumn {
    pub const fn default_active_state_columns() -> &'static [StateContextColumn] {
        &[
            StateContextColumn::EntityId,
            StateContextColumn::SchemaKey,
            StateContextColumn::SchemaVersion,
            StateContextColumn::SnapshotContent,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::{DetectStateContextConfig, StateContextColumn};

    #[test]
    fn resolved_columns_returns_none_when_active_state_is_not_enabled() {
        let config = DetectStateContextConfig {
            include_active_state: None,
            columns: None,
        };

        assert_eq!(config.resolved_columns_or_default(), None);
    }

    #[test]
    fn resolved_columns_uses_defaults_when_columns_are_omitted() {
        let config = DetectStateContextConfig {
            include_active_state: Some(true),
            columns: None,
        };

        assert_eq!(
            config.resolved_columns_or_default(),
            Some(StateContextColumn::default_active_state_columns().to_vec())
        );
    }

    #[test]
    fn resolved_columns_uses_explicit_column_selection() {
        let config = DetectStateContextConfig {
            include_active_state: Some(true),
            columns: Some(vec![
                StateContextColumn::EntityId,
                StateContextColumn::SchemaKey,
            ]),
        };

        assert_eq!(
            config.resolved_columns_or_default(),
            Some(vec![
                StateContextColumn::EntityId,
                StateContextColumn::SchemaKey
            ])
        );
    }
}
