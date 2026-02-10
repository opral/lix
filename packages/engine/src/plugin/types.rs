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
    pub detect_changes_glob: String,
    #[serde(default)]
    pub entry: Option<String>,
}

impl PluginManifest {
    pub fn entry_or_default(&self) -> &str {
        self.entry.as_deref().unwrap_or("plugin.wasm")
    }
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
    pub detect_changes_glob: String,
    pub entry: String,
    pub manifest_json: String,
    pub wasm: Vec<u8>,
}
