//! Plugin subsystem root.
//!
//! Phase 1 establishes `crate::plugin::*` as the owner path for plugin-domain
//! code under concrete plugin-owned modules instead of legacy ownership-neutral
//! buckets.

mod archive;
pub(crate) mod component;
mod context;
mod detect_changes;
mod manifest;
mod matching;
mod registry;
mod storage;
mod types;

pub(crate) use archive::{
    load_installed_plugin_from_archive_bytes, parse_plugin_archive_for_install, ParsedPluginArchive,
};
pub(crate) use context::PluginContext;
pub(crate) use detect_changes::{
    PluginActiveStateRow, PluginDetectChangesInput, PluginDetectStateContext, PluginEntityChange,
    PluginFileInput,
};
#[allow(unused_imports)]
pub(crate) use manifest::{
    glob_matches_path, parse_plugin_manifest_json, select_best_glob_match, DetectChangesConfig,
    DetectStateContextConfig, PluginManifest, PluginMatch, StateContextColumn,
    ValidatedPluginManifest,
};
pub use manifest::{PluginContentType, PluginRuntime};
#[allow(unused_imports)]
pub(crate) use matching::select_plugin_for_file;
#[allow(unused_imports)]
pub(crate) use storage::{
    plugin_key_from_archive_path, plugin_storage_archive_file_id, plugin_storage_archive_path,
    PLUGIN_ARCHIVE_FILE_EXTENSION, PLUGIN_STORAGE_ROOT_DIRECTORY_PATH,
};
pub use types::InstalledPlugin;
