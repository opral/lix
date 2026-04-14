//! Plugin subsystem root.
//!
//! Phase 1 establishes `crate::plugin::*` as the owner path for plugin-domain
//! code under concrete plugin-owned modules instead of sealed `contracts/*`
//! children.

mod archive;
pub(crate) mod component;
mod install;
mod manifest;
mod materializer;
mod storage;

pub(crate) use archive::{
    load_installed_plugin_from_archive_bytes, parse_plugin_archive_for_install, ParsedPluginArchive,
};
pub(crate) use component::{CachedPluginComponent, PluginComponentHost};
pub(crate) use install::{
    install_plugin_archive_with_writer, prepare_registered_schema_write_statement,
    PluginInstallWriteContext, PluginInstallWriteExecutor,
};
#[allow(unused_imports)]
pub(crate) use manifest::{
    glob_matches_path, parse_plugin_manifest_json, select_best_glob_match, DetectChangesConfig,
    DetectStateContextConfig, PluginContentType, PluginManifest, PluginMatch, PluginRuntime,
    StateContextColumn, ValidatedPluginManifest,
};
#[allow(unused_imports)]
pub(crate) use materializer::{
    invalidate_installed_plugins_cache, load_installed_plugins_with_runtime_cache,
    FilesystemPluginMaterializer, InstalledPlugin, PluginMaterializationHost,
};
#[allow(unused_imports)]
pub(crate) use storage::{
    plugin_key_from_archive_path, plugin_storage_archive_file_id, plugin_storage_archive_path,
    PLUGIN_ARCHIVE_FILE_EXTENSION, PLUGIN_STORAGE_ROOT_DIRECTORY_PATH,
};
