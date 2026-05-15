use std::collections::BTreeMap;
use std::sync::Arc;

use crate::binary_cas::BlobDataReader;
use crate::live_state::LiveStateReader;
use crate::plugin::{InstalledPlugin, PluginContext};
use crate::transaction::types::TransactionFileData;
use crate::LixError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilePluginMatch {
    pub(crate) file_id: String,
    pub(crate) path: String,
    pub(crate) version_id: String,
    pub(crate) plugin: InstalledPlugin,
}

pub(crate) async fn select_plugins_for_file_data_writes(
    plugin_context: &PluginContext,
    live_state: Arc<dyn LiveStateReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    file_data_writes: &[TransactionFileData],
) -> Result<Vec<FilePluginMatch>, LixError> {
    let mut plugins_by_version = BTreeMap::<String, Vec<InstalledPlugin>>::new();
    let mut matches = Vec::new();

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
        matches.push(FilePluginMatch {
            file_id: write.file_id.clone(),
            path: write.path.clone(),
            version_id: write.version_id.clone(),
            plugin: plugin.clone(),
        });
    }

    Ok(matches)
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
            manifest_json: "{}".to_string(),
            wasm: b"\0asm\x01\0\0\0".to_vec(),
        };
        let file_data = TransactionFileData {
            file_id: "file-1".to_string(),
            path: "/foo.json".to_string(),
            version_id: "version-a".to_string(),
            untracked: false,
            data: br#"{"hello":"world"}"#.to_vec(),
        };

        let plugins = [plugin];
        let selected = crate::plugin::select_plugin_for_file(&plugins, &file_data.path, None)
            .expect("matching plugin should be selected for lix_file bytes");

        assert_eq!(selected.key, "test_plugin_json");
    }
}
