//! Engine-side execution and lifecycle support for Lix plugins.
//!
//! The public Component contract lives here. General WebAssembly compute
//! configuration remains under [`crate::wasm`], while format-neutral plugin
//! encodings live under [`crate::plugin::wire`].

#[cfg(feature = "default_wasm_runtime")]
pub(crate) mod default;

mod api;
mod contract;

pub use api::*;
pub use contract::*;

mod actor;
mod archive;
pub(crate) mod arena;
mod component;
mod conflict;
mod create_context;
mod incremental;
mod install;
mod manifest;
mod materializer;
mod registry;
mod row_reconcile;
mod storage;

pub(crate) use actor::{
    DEFAULT_MAX_LIVE_PLUGIN_STORES, PluginActorCache, PluginActorColdInstall, PluginActorColdOpen,
    PluginActorKey, PluginActorLease, PluginActorStagedCheckpoint, PluginActorStore,
    PluginActorStorePermit, PluginObservation, PluginRowAuthorities, PluginRowAuthorityRange,
};
pub(crate) use archive::{ParsedPluginArchive, parse_plugin_archive_for_install};
pub(crate) use component::{DEFAULT_PLUGIN_MEMORY_BYTES, PluginRuntimeHost};
pub(crate) use conflict::ConflictRank;
pub(crate) use create_context::{
    BoundCreateContext, is_reservation_key, local_mutation_identity, materialize_keyless_creates,
    require_existing_id_authorities, reservation_tombstone_row, reserve_create_row,
    validate_create_changes, validate_create_reservation,
};
pub(crate) use incremental::{
    ArcByteSource, FileBytesSha256, LiveBatchRowSource, SchemaAllowlist,
    ValidatedColumnMergeTransition, ValidatedFileTransition, ValidatedSameLengthOutputSplice,
    VecColumnMergeSource, VecRowChangeSource, VecRowSource, build_file_update_splices,
    drain_column_merge_transition_results, drain_file_transition_changes,
    drain_row_transition_edits, transport_splice_preserves_prefix_exclusion,
    transport_splice_preserves_utf8,
};
pub(crate) use install::{PluginArchiveInstallPlan, plugin_install_plan_from_archive_path};
pub(crate) use manifest::{
    PluginContentMatcher, PluginManifest, PluginRuntime, parse_plugin_manifest_json,
};
pub(crate) use materializer::plugin_state_hot_state_projection;
pub(crate) use registry::{
    CompiledPluginCatalog, PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginCatalogCache,
    PluginFileOwner, PluginRegistry, PluginRegistryEntry, PluginRegistryEntryInput,
    collect_gc_wasm_blob_roots, load_plugin_registry_at_commit,
};
pub(crate) use row_reconcile::{
    ReconciledRow, ReconciledTypedRow, RowVersionRef, TypedColumnMergeResult, TypedRowVersionRef,
    primary_key_columns, reconcile_row, reconcile_typed_row, visit_typed_row_overlaps,
};
#[cfg(test)]
pub(crate) use storage::plugin_storage_archive_path;
pub(crate) use storage::{
    is_plugin_storage_path, plugin_archive_delete_origin, plugin_archive_file_id_matches,
    plugin_key_from_archive_delete_origin, plugin_key_from_archive_path,
    plugin_storage_archive_file_id, reject_normal_plugin_storage_mutation,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstalledPlugin {
    pub key: String,
    pub runtime: PluginRuntime,
    pub api_version: String,
    pub capabilities: PluginCapabilities,
    pub path_glob: Option<String>,
    pub content: Option<PluginContentMatcher>,
    pub entry: Option<String>,
    pub schema_keys: Vec<String>,
    pub manifest_json: String,
    /// Content-addressed identity computed while the component bytes are
    /// already in hand. Warm component-cache lookups must use this fixed-size
    /// value instead of rehashing or comparing the full WASM payload.
    pub wasm_hash: Option<crate::binary_cas::BlobId>,
    pub wasm: Option<Vec<u8>>,
}

/// Executable capabilities discovered from a plugin component at install.
/// They are durable generation metadata, not user configuration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginCapabilities {
    pub column_merger: bool,
    pub file_projection: bool,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstalledPluginMetadata {
    pub key: String,
    pub archive_path: String,
    pub archive_blob_hash: String,
    pub path_glob: Option<String>,
    pub content: Option<PluginContentMatcher>,
    pub schema_keys: Vec<String>,
}
