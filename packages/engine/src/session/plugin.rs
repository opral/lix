use crate::filesystem::{filesystem_schema_keys, FilesystemIndex};
use crate::live_state::{LiveStateFilter, LiveStateScanRequest};
use crate::plugin::{
    load_installed_plugins_from_filesystem, parse_plugin_archive_for_install,
    plugin_storage_archive_path, PluginContentType,
};
use crate::storage::{SharedStorageRead, StorageBackend, StorageReadOptions};
use crate::LixError;

use super::{FsWriteOptions, SessionContext};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledPluginInfo {
    pub key: String,
    pub api_version: String,
    pub path_glob: String,
    pub content_type: Option<String>,
    pub entry: String,
    pub schema_keys: Vec<String>,
    pub manifest_json: String,
}

impl<B> SessionContext<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    pub async fn install_plugin(&self, archive_bytes: &[u8]) -> Result<(), LixError> {
        let parsed = parse_plugin_archive_for_install(archive_bytes)?;
        let archive_path = plugin_storage_archive_path(&parsed.manifest.key);
        self.fs()
            .write_file(
                &archive_path,
                archive_bytes.to_vec(),
                FsWriteOptions::default(),
            )
            .await
    }

    pub async fn list_installed_plugins(&self) -> Result<Vec<InstalledPluginInfo>, LixError> {
        let _operation_guard = self.begin_waitable_session_operation().await?;
        let read = SharedStorageRead::new(self.storage.begin_read(StorageReadOptions::default())?);
        let active_branch_id = self.active_branch_id_from_reader(&read).await?;
        let live_state = self.live_state.reader(&read);
        let filesystem_rows = live_state
            .scan_rows(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: filesystem_schema_keys(),
                    branch_ids: vec![active_branch_id],
                    ..Default::default()
                },
                ..Default::default()
            })
            .await?;
        let index = FilesystemIndex::from_live_rows(filesystem_rows)?;
        let blob_reader = self.binary_cas.reader(read);
        let plugins = load_installed_plugins_from_filesystem(&index, &blob_reader).await?;
        Ok(plugins
            .into_iter()
            .map(|plugin| InstalledPluginInfo {
                key: plugin.key,
                api_version: plugin.api_version,
                path_glob: plugin.path_glob,
                content_type: plugin.content_type.map(plugin_content_type_name),
                entry: plugin.entry,
                schema_keys: plugin.schema_keys,
                manifest_json: plugin.manifest_json,
            })
            .collect())
    }
}

fn plugin_content_type_name(content_type: PluginContentType) -> String {
    match content_type {
        PluginContentType::Text => "text".to_string(),
        PluginContentType::Binary => "binary".to_string(),
    }
}
