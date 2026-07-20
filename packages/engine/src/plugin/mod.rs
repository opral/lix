//! Plugin subsystem root.
//!
//! Phase 1 establishes `crate::plugin::*` as the owner path for plugin-domain
//! code under concrete plugin-owned modules instead of legacy ownership-neutral
//! buckets.

mod archive;
#[cfg(test)]
pub(crate) mod bench_stats;
pub(crate) mod component;
mod install;
mod manifest;
mod materializer;
mod storage;

pub(crate) use archive::{
    ParsedPluginArchive, load_installed_plugin_from_archive_bytes,
    load_installed_plugin_metadata_from_archive_bytes, parse_plugin_archive_for_install,
};
pub(crate) use component::{CachedPluginComponent, PluginComponentHost, PluginRuntimeHost};
pub(crate) use install::plugin_schema_rows_from_archive_path;
#[allow(unused_imports)]
pub(crate) use manifest::{
    PluginContentType, PluginManifest, PluginMatch, PluginRuntime, ValidatedPluginManifest,
    glob_matches_path, parse_plugin_manifest_json, select_best_glob_match,
};
#[allow(unused_imports)]
pub(crate) use materializer::{
    PluginDetectedChange, detect_changes_with_plugin, load_installed_plugins_from_filesystem,
    plugin_state_live_state_projection, render_materialized_plugin_file, render_plugin_state,
    retain_plugin_state_rows, select_plugin_for_path,
};
#[allow(unused_imports)]
pub(crate) use storage::{
    PLUGIN_ARCHIVE_FILE_EXTENSION, PLUGIN_STORAGE_ROOT_DIRECTORY_PATH, is_plugin_storage_path,
    plugin_key_from_archive_path, plugin_storage_archive_file_id, plugin_storage_archive_path,
    reject_normal_plugin_storage_mutation,
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
