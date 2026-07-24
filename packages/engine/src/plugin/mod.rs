//! Plugin subsystem root.
//!
//! Phase 1 establishes `crate::plugin::*` as the owner path for plugin-domain
//! code under concrete plugin-owned modules instead of legacy ownership-neutral
//! buckets.

mod actor;
mod archive;
mod component;
mod id_namespace;
mod incremental;
mod install;
mod manifest;
mod materializer;
mod registry;
mod storage;

pub(crate) use actor::{
    DEFAULT_MAX_PLUGIN_FILE_ACTORS, PluginActorCache, PluginActorColdInstall, PluginActorColdOpen,
    PluginActorKey, PluginActorLease, PluginObservation,
};
pub(crate) use archive::{
    ParsedPluginArchive, load_installed_plugin_from_archive_bytes, parse_plugin_archive_for_install,
};
pub(crate) use component::{
    CachedPluginComponent, DEFAULT_PLUGIN_V2_MEMORY_BYTES, PluginComponentHost, PluginRuntimeHost,
    load_or_init_plugin_component,
};
pub(crate) use id_namespace::{
    BoundIdNamespace, is_reservation_key, local_mutation_identity, require_existing_id_authorities,
    reservation_tombstone_row, reserve_namespace_row, validate_host_allocated_changes,
    validate_namespace_reservation,
};
pub(crate) use incremental::{
    ArcByteSource, V2SchemaAllowlist, VecEntityChangeSource, VecEntitySource,
    build_file_update_splices, drain_entity_transition_edits, drain_file_transition_changes,
    host_entity_change_with_lazy_snapshot, host_entity_with_lazy_snapshot,
    transport_splice_preserves_utf8,
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

/// Returns a MIME type only when the file path carries an unambiguous format
/// extension understood by the engine. Unknown paths remain `None`; a
/// Component descriptor must not claim CSV merely because the current
/// production plugin happens to implement CSV.
pub(crate) fn inferred_media_type_for_path(path: Option<&str>) -> Option<&'static str> {
    let filename = path?.rsplit('/').next()?;
    let (_, extension) = filename.rsplit_once('.')?;
    if extension.eq_ignore_ascii_case("csv") {
        Some("text/csv")
    } else if extension.eq_ignore_ascii_case("tsv") {
        Some("text/tab-separated-values")
    } else if extension.eq_ignore_ascii_case("json") {
        Some("application/json")
    } else if extension.eq_ignore_ascii_case("md") || extension.eq_ignore_ascii_case("markdown") {
        Some("text/markdown")
    } else if extension.eq_ignore_ascii_case("excalidraw") {
        Some("application/json")
    } else {
        None
    }
}

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

#[cfg(test)]
mod tests {
    use super::inferred_media_type_for_path;

    #[test]
    fn component_media_type_inference_is_truthful_and_conservative() {
        assert_eq!(
            inferred_media_type_for_path(Some("/data/report.CSV")),
            Some("text/csv")
        );
        assert_eq!(
            inferred_media_type_for_path(Some("/data/report.tsv")),
            Some("text/tab-separated-values")
        );
        assert_eq!(
            inferred_media_type_for_path(Some("/data/report.json")),
            Some("application/json")
        );
        assert_eq!(
            inferred_media_type_for_path(Some("/data/readme.md")),
            Some("text/markdown")
        );
        assert_eq!(
            inferred_media_type_for_path(Some("/data/drawing.excalidraw")),
            Some("application/json")
        );
        assert_eq!(inferred_media_type_for_path(Some("/data/report")), None);
        assert_eq!(inferred_media_type_for_path(Some("/data/report.bin")), None);
        assert_eq!(inferred_media_type_for_path(None), None);
    }
}
