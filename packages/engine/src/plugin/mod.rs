//! Plugin subsystem root.
//!
//! Phase 1 establishes `crate::plugin::*` as the owner path for plugin-domain
//! code under concrete plugin-owned modules instead of legacy ownership-neutral
//! buckets.

mod archive;
mod component;
mod install;
mod manifest;
mod materializer;
mod registry;
mod storage;

pub(crate) use archive::{
    ParsedPluginArchive, load_installed_plugin_from_archive_bytes, parse_plugin_archive_for_install,
};
pub(crate) use component::{
    CachedPluginComponent, PluginComponentHost, PluginRuntimeHost, load_or_init_plugin_component,
};
pub(crate) use install::{PluginArchiveInstallPlan, plugin_install_plan_from_archive_path};
pub(crate) use manifest::{
    PluginContentType, PluginManifest, PluginRuntime, parse_plugin_manifest_json,
};
pub(crate) use materializer::{
    PluginDetectedChange, detect_changes_with_component_instance,
    plugin_state_live_state_projection, render_plugin_state,
    render_plugin_state_with_component_instance, retain_plugin_state_rows,
    retain_plugin_state_rows_for_schema_keys,
};
pub(crate) use registry::{
    CompiledPluginCatalog, PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginCatalogCache,
    PluginFileOwner, PluginRegistry, PluginRegistryEntry, PluginRegistryEntryInput,
};
#[cfg(test)]
pub(crate) use storage::plugin_storage_archive_path;
pub(crate) use storage::{
    is_plugin_storage_path, plugin_key_from_archive_file_id, plugin_key_from_archive_path,
    plugin_storage_archive_file_id, reject_normal_plugin_storage_mutation,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstalledPlugin {
    pub key: String,
    pub runtime: PluginRuntime,
    pub api_version: String,
    pub path_glob: String,
    pub content_type: Option<PluginContentType>,
    pub entry: String,
    pub schema_keys: Vec<String>,
    pub manifest_json: String,
    /// Content-addressed identity computed while the component bytes are
    /// already in hand. Warm component-cache lookups must use this fixed-size
    /// value instead of rehashing or comparing the full WASM payload.
    pub wasm_hash: crate::binary_cas::BlobHash,
    pub wasm: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstalledPluginMetadata {
    pub key: String,
    pub archive_path: String,
    pub archive_blob_hash: String,
    pub path_glob: String,
    pub content_type: Option<PluginContentType>,
    pub schema_keys: Vec<String>,
}
