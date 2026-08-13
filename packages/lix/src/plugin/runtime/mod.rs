//! Plugin subsystem root.
//!
//! Phase 1 establishes `crate::plugin::*` as the owner path for plugin-domain
//! code under concrete plugin-owned modules instead of ownership-neutral
//! buckets.

#[cfg(feature = "default_wasm_runtime")]
pub(crate) mod default;

// Runtime providers are an advanced integration surface. Keeping them under
// `lix::plugin::runtime` makes their ownership explicit without exposing the
// engine's installation and registry implementation.
pub use crate::wasm::*;

mod actor;
mod archive;
pub(crate) mod arena;
mod component;
mod conflict;
mod create_context;
mod incremental;
mod install;
pub(crate) mod layout;
mod manifest;
mod materializer;
mod registry;
mod storage;

pub(crate) use actor::{
    DEFAULT_MAX_LIVE_PLUGIN_STORES, PluginActorCache, PluginActorColdInstall, PluginActorColdOpen,
    PluginActorKey, PluginActorLease, PluginActorStagedCheckpoint, PluginActorStore,
    PluginActorStorePermit, PluginEntityAuthorities, PluginEntityAuthorityRange, PluginObservation,
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
    ArcByteSource, FileBytesSha256, LiveBatchEntitySource, SchemaAllowlist,
    ValidatedConflictTransition, ValidatedFileTransition, ValidatedSameLengthOutputSplice,
    VecEntityChangeSource, VecEntityConflictSource, VecEntitySource, build_file_update_splices,
    canonicalize_snapshot, certify_dense_fresh_file, drain_conflict_transition_resolutions,
    drain_entity_transition_edits, drain_file_transition_changes,
    host_entity_change_with_lazy_snapshot, host_entity_with_lazy_snapshot,
    transport_splice_preserves_prefix_exclusion, transport_splice_preserves_utf8,
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
    pub path_glob: String,
    pub content: Option<PluginContentMatcher>,
    pub entry: String,
    pub schema_keys: Vec<String>,
    pub manifest_json: String,
    /// Content-addressed identity computed while the component bytes are
    /// already in hand. Warm component-cache lookups must use this fixed-size
    /// value instead of rehashing or comparing the full WASM payload.
    pub wasm_hash: crate::binary_cas::BlobId,
    pub wasm: Vec<u8>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstalledPluginMetadata {
    pub key: String,
    pub archive_path: String,
    pub archive_blob_hash: String,
    pub path_glob: String,
    pub content: Option<PluginContentMatcher>,
    pub schema_keys: Vec<String>,
}
