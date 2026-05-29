use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::common::LixError;
use crate::live_state::{list_installed_plugin_archive_refs, PluginArchiveRef};
use crate::Backend;

use super::component::{render_with_plugin, PluginComponentHost};
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

    async fn render_plugin_state(
        &self,
        plugin: &InstalledPlugin,
        payload: &[u8],
    ) -> Result<Vec<u8>, LixError>;
}

pub(crate) trait PluginMaterializationHost: PluginComponentHost {
    fn plugin_backend(&self) -> &Arc<dyn Backend + Send + Sync>;

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
            message: "installed plugin cache lock poisoned".to_string(),
            hint: None,
            details: None,
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
            message: "installed plugin cache lock poisoned".to_string(),
            hint: None,
            details: None,
        })?;
    *guard = Some(plugins.clone());
    Ok(plugins)
}

pub(crate) async fn load_installed_plugins_from_backend(
    host: &impl PluginMaterializationHost,
) -> Result<Vec<InstalledPlugin>, LixError> {
    load_installed_plugins_from_backend_state(host.plugin_backend().as_ref()).await
}

pub(crate) async fn load_installed_plugins_from_backend_state(
    backend: &dyn Backend,
) -> Result<Vec<InstalledPlugin>, LixError> {
    let archive_refs = list_installed_plugin_archive_refs(backend).await?;
    let mut plugins = Vec::with_capacity(archive_refs.len());
    for archive_ref in archive_refs {
        plugins.push(
            load_installed_plugin_from_archive_ref_with_backend(backend, &archive_ref).await?,
        );
    }
    Ok(plugins)
}

pub(crate) async fn load_installed_plugin_from_archive_ref_with_backend(
    backend: &dyn Backend,
    archive_ref: &PluginArchiveRef,
) -> Result<InstalledPlugin, LixError> {
    let Some(plugin_key) = plugin_key_from_archive_path(&archive_ref.path) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "plugin materialization: unsupported plugin archive path '{}'",
                archive_ref.path
            ),
            hint: None,
            details: None,
        });
    };
    let binary_cas = crate::binary_cas::BinaryCasContext::new();
    let mut reader = binary_cas.reader(backend);
    let archive_hash = crate::binary_cas::BlobHash::from_hex(&archive_ref.blob_hash)?;
    let archive_bytes = reader
        .load_bytes_many(&[archive_hash])
        .await?
        .into_vec()
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "plugin materialization: missing plugin archive blob '{}' for file '{}' ({})",
                archive_ref.blob_hash, archive_ref.path, archive_ref.file_id
            ),
            hint: None,
            details: None,
        })?;
    if archive_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "plugin materialization: archive '{}' is empty",
                archive_ref.path
            ),
            hint: None,
            details: None,
        });
    }
    load_installed_plugin_from_archive_bytes(&plugin_key, &archive_ref.path, &archive_bytes)
}

pub(crate) async fn list_installed_plugin_manifest_keys(
    backend: &dyn Backend,
) -> Result<BTreeSet<String>, LixError> {
    Ok(load_installed_plugins_from_backend_state(backend)
        .await?
        .into_iter()
        .map(|plugin| plugin.key)
        .collect())
}

#[allow(dead_code)]
pub(crate) async fn installed_plugin_manifest_key_exists(
    backend: &dyn Backend,
    plugin_key: &str,
) -> Result<bool, LixError> {
    Ok(list_installed_plugin_manifest_keys(backend)
        .await?
        .contains(plugin_key))
}

pub(crate) fn invalidate_installed_plugins_cache(
    host: &impl PluginMaterializationHost,
) -> Result<(), LixError> {
    let mut guard = host
        .installed_plugins_cache()
        .write()
        .map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: "installed plugin cache lock poisoned".to_string(),
            hint: None,
            details: None,
        })?;
    *guard = None;
    let mut component_guard = host.plugin_component_cache().lock().map_err(|_| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: "plugin component cache lock poisoned".to_string(),
        hint: None,
            details: None,
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

    async fn render_plugin_state(
        &self,
        plugin: &InstalledPlugin,
        payload: &[u8],
    ) -> Result<Vec<u8>, LixError> {
        render_with_plugin(self, plugin, payload).await
    }
}
