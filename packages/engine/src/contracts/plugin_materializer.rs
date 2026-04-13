use async_trait::async_trait;

use crate::common::LixError;

use super::InstalledPlugin;

#[async_trait(?Send)]
pub trait FilesystemPluginMaterializer {
    async fn load_installed_plugins(&self) -> Result<Vec<InstalledPlugin>, LixError>;

    async fn apply_plugin_changes(
        &self,
        plugin: &InstalledPlugin,
        payload: &[u8],
    ) -> Result<Vec<u8>, LixError>;
}
