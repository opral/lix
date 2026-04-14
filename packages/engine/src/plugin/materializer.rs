use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::binary_cas::load_blob_data_by_hash;
use crate::common::LixError;
use crate::live_state::{list_installed_plugin_archive_refs, PluginArchiveRef};
use crate::LixBackend;

use super::component::{apply_changes_with_plugin, PluginComponentHost};
use super::{
    load_installed_plugin_from_archive_bytes, plugin_key_from_archive_path, PluginContentType,
    PluginRuntime,
};

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

#[async_trait(?Send)]
pub trait FilesystemPluginMaterializer {
    async fn load_installed_plugins(&self) -> Result<Vec<InstalledPlugin>, LixError>;

    async fn apply_plugin_changes(
        &self,
        plugin: &InstalledPlugin,
        payload: &[u8],
    ) -> Result<Vec<u8>, LixError>;
}

pub(crate) trait PluginMaterializationHost: PluginComponentHost {
    fn plugin_backend(&self) -> &Arc<dyn LixBackend + Send + Sync>;

    fn installed_plugins_cache(&self) -> &RwLock<Option<Vec<InstalledPlugin>>>;
}

pub(crate) async fn load_installed_plugins_with_runtime_cache(
    host: &impl PluginMaterializationHost,
) -> Result<Vec<InstalledPlugin>, LixError> {
    if let Some(cached) = host
        .installed_plugins_cache()
        .read()
        .map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "installed plugin cache lock poisoned".to_string(),
        })?
        .clone()
    {
        return Ok(cached);
    }

    let plugins = load_installed_plugins_from_backend(host).await?;
    let mut guard = host
        .installed_plugins_cache()
        .write()
        .map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "installed plugin cache lock poisoned".to_string(),
        })?;
    *guard = Some(plugins.clone());
    Ok(plugins)
}

pub(crate) async fn load_installed_plugins_from_backend(
    host: &impl PluginMaterializationHost,
) -> Result<Vec<InstalledPlugin>, LixError> {
    let archive_refs = list_installed_plugin_archive_refs(host.plugin_backend().as_ref()).await?;
    let mut plugins = Vec::with_capacity(archive_refs.len());
    for archive_ref in archive_refs {
        plugins.push(load_installed_plugin_from_archive_ref(host, &archive_ref).await?);
    }
    Ok(plugins)
}

pub(crate) async fn load_installed_plugin_from_archive_ref(
    host: &impl PluginMaterializationHost,
    archive_ref: &PluginArchiveRef,
) -> Result<InstalledPlugin, LixError> {
    let Some(plugin_key) = plugin_key_from_archive_path(&archive_ref.path) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: unsupported plugin archive path '{}'",
                archive_ref.path
            ),
        });
    };
    let archive_bytes =
        load_blob_data_by_hash(host.plugin_backend().as_ref(), &archive_ref.blob_hash)
            .await?
            .ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "plugin materialization: missing plugin archive blob '{}' for file '{}' ({})",
                    archive_ref.blob_hash, archive_ref.path, archive_ref.file_id
                ),
            })?;
    if archive_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: archive '{}' is empty",
                archive_ref.path
            ),
        });
    }
    load_installed_plugin_from_archive_bytes(&plugin_key, &archive_ref.path, &archive_bytes)
}

pub(crate) fn invalidate_installed_plugins_cache(
    host: &impl PluginMaterializationHost,
) -> Result<(), LixError> {
    let mut guard = host
        .installed_plugins_cache()
        .write()
        .map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "installed plugin cache lock poisoned".to_string(),
        })?;
    *guard = None;
    let mut component_guard = host.plugin_component_cache().lock().map_err(|_| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "plugin component cache lock poisoned".to_string(),
    })?;
    component_guard.clear();
    Ok(())
}

#[async_trait(?Send)]
impl<T> FilesystemPluginMaterializer for T
where
    T: PluginMaterializationHost,
{
    async fn load_installed_plugins(&self) -> Result<Vec<InstalledPlugin>, LixError> {
        load_installed_plugins_with_runtime_cache(self).await
    }

    async fn apply_plugin_changes(
        &self,
        plugin: &InstalledPlugin,
        payload: &[u8],
    ) -> Result<Vec<u8>, LixError> {
        apply_changes_with_plugin(self, plugin, payload).await
    }
}
