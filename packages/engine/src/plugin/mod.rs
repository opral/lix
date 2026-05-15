//! Plugin subsystem root.
//!
//! Phase 1 establishes `crate::plugin::*` as the owner path for plugin-domain
//! code under concrete plugin-owned modules instead of legacy ownership-neutral
//! buckets.

mod archive;
pub(crate) mod component;
mod manifest;
mod storage;
mod types;

pub(crate) use archive::{
    load_installed_plugin_from_archive_bytes, parse_plugin_archive_for_install, ParsedPluginArchive,
};
#[allow(unused_imports)]
pub(crate) use manifest::{
    glob_matches_path, parse_plugin_manifest_json, select_best_glob_match, DetectChangesConfig,
    DetectStateContextConfig, PluginContentType, PluginManifest, PluginMatch, PluginRuntime,
    StateContextColumn, ValidatedPluginManifest,
};
#[allow(unused_imports)]
pub(crate) use storage::{
    plugin_key_from_archive_path, plugin_storage_archive_file_id, plugin_storage_archive_path,
    PLUGIN_ARCHIVE_FILE_EXTENSION, PLUGIN_STORAGE_ROOT_DIRECTORY_PATH,
};
pub(crate) use types::InstalledPlugin;
