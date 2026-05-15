use super::{PluginContentType, PluginRuntime};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledPlugin {
    pub key: String,
    pub runtime: PluginRuntime,
    pub api_version: String,
    pub path_glob: String,
    pub content_type: Option<PluginContentType>,
    pub entry: String,
    pub schema_keys: Vec<String>,
    pub manifest_json: String,
    pub wasm: Vec<u8>,
}
