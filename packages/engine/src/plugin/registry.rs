//! Durable, branch-local plugin registry state.
//!
//! The registry is one tracked `lix_key_value` entity per branch. File
//! ownership uses the same reserved entity key for every file and relies on
//! `file_id` for identity. That layout gives the transaction hot paths one
//! exact registry read and one batched owner read instead of a filesystem
//! scan.

use std::num::NonZeroUsize;
use std::sync::Arc;

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use lru::LruCache;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

use crate::binary_cas::BlobHash;
use crate::entity_pk::EntityPk;
use crate::live_state::MaterializedLiveStateRow;
use crate::transaction::types::{TransactionJson, TransactionWriteRow};
use crate::{GLOBAL_BRANCH_ID, LixError};

use super::InstalledPlugin;
use super::manifest::{
    PluginContentType, PluginManifest, PluginRuntime, parse_plugin_manifest_json,
};
use super::storage::{plugin_storage_archive_file_id, plugin_storage_archive_path};

pub(crate) const PLUGIN_REGISTRY_KEY: &str = "lix_plugin_registry_v1";
pub(crate) const PLUGIN_OWNER_KEY: &str = "lix_plugin_owner_v1";
pub(crate) const MAX_PLUGIN_REGISTRY_ENTRIES: usize = 128;

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const REGISTRY_FORMAT_VERSION: u32 = 1;
const MAX_CACHED_PLUGIN_CATALOGS: usize = 16;
const DEFAULT_CACHED_PLUGIN_CATALOGS: usize = 8;

/// Install-time data used to construct one canonical registry entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginRegistryEntryInput {
    pub(crate) key: String,
    pub(crate) runtime: PluginRuntime,
    pub(crate) api_version: String,
    pub(crate) path_glob: String,
    pub(crate) content_type: Option<PluginContentType>,
    pub(crate) entry: String,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) manifest_json: String,
    pub(crate) archive_file_id: String,
    pub(crate) archive_path: String,
    pub(crate) archive_blob_hash: String,
    pub(crate) wasm_blob_hash: String,
}

/// Metadata needed by current-state plugin matching and execution.
///
/// Path-only matching is encoded explicitly as `content_type: null`. Registry
/// rows are an internal engine format, so missing fields are rejected instead
/// of carrying compatibility for unreleased representations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginRegistryEntry {
    key: String,
    runtime: PluginRuntime,
    api_version: String,
    path_glob: String,
    #[serde(deserialize_with = "deserialize_required_content_type")]
    content_type: Option<PluginContentType>,
    entry: String,
    schema_keys: Vec<String>,
    manifest_json: String,
    archive_file_id: String,
    archive_path: String,
    archive_blob_hash: String,
    wasm_blob_hash: String,
}

fn deserialize_required_content_type<'de, D>(
    deserializer: D,
) -> Result<Option<PluginContentType>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<PluginContentType>::deserialize(deserializer)
}

impl PluginRegistryEntry {
    pub(crate) fn new(input: PluginRegistryEntryInput) -> Result<Self, LixError> {
        let mut entry = Self {
            key: input.key,
            runtime: input.runtime,
            api_version: input.api_version,
            path_glob: input.path_glob,
            content_type: input.content_type,
            entry: input.entry,
            schema_keys: input.schema_keys,
            manifest_json: canonicalize_json_text(
                &input.manifest_json,
                "plugin registry manifest_json",
            )?,
            archive_file_id: input.archive_file_id,
            archive_path: input.archive_path,
            archive_blob_hash: input.archive_blob_hash,
            wasm_blob_hash: input.wasm_blob_hash,
        };
        entry.schema_keys.sort();
        // Install-time validation pays the complete JSON-Schema and glob
        // checks once. Durable reads below use the already-validated compact
        // fields and generation integrity, so warm transactions do not
        // recompile one glob per plugin before consulting the catalog cache.
        parse_plugin_manifest_json(&entry.manifest_json)?;
        validate_entry(&entry)?;
        Ok(entry)
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn runtime(&self) -> PluginRuntime {
        self.runtime
    }

    pub(crate) fn api_version(&self) -> &str {
        &self.api_version
    }

    pub(crate) fn path_glob(&self) -> &str {
        &self.path_glob
    }

    pub(crate) fn content_type(&self) -> Option<PluginContentType> {
        self.content_type
    }

    pub(crate) fn entry(&self) -> &str {
        &self.entry
    }

    pub(crate) fn schema_keys(&self) -> &[String] {
        &self.schema_keys
    }

    pub(crate) fn manifest_json(&self) -> &str {
        &self.manifest_json
    }

    pub(crate) fn archive_file_id(&self) -> &str {
        &self.archive_file_id
    }

    pub(crate) fn archive_path(&self) -> &str {
        &self.archive_path
    }

    pub(crate) fn archive_blob_hash(&self) -> &str {
        &self.archive_blob_hash
    }

    pub(crate) fn wasm_blob_hash(&self) -> &str {
        &self.wasm_blob_hash
    }

    pub(crate) fn to_installed_plugin(&self, wasm: Vec<u8>) -> Result<InstalledPlugin, LixError> {
        let wasm_hash = BlobHash::from_content(&wasm);
        let actual_hash = wasm_hash.to_hex();
        if actual_hash != self.wasm_blob_hash {
            return Err(invalid_registry(format!(
                "plugin '{}' WASM bytes hash '{}' does not match registry hash '{}'",
                self.key, actual_hash, self.wasm_blob_hash
            )));
        }
        Ok(InstalledPlugin {
            key: self.key.clone(),
            runtime: self.runtime,
            api_version: self.api_version.clone(),
            path_glob: self.path_glob.clone(),
            content_type: self.content_type,
            entry: self.entry.clone(),
            schema_keys: self.schema_keys.clone(),
            manifest_json: self.manifest_json.clone(),
            wasm_hash,
            wasm,
        })
    }
}

/// Canonical contents of `lix_key_value:lix_plugin_registry_v1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginRegistry {
    plugin_count: u32,
    generation: String,
    plugins: Vec<PluginRegistryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PluginRegistryWire {
    version: u32,
    plugin_count: u32,
    generation: String,
    plugins: Vec<PluginRegistryEntry>,
}

#[derive(Serialize)]
struct PluginRegistryGenerationPayload<'a> {
    version: u32,
    plugins: &'a [PluginRegistryEntry],
}

impl PluginRegistry {
    pub(crate) fn empty() -> Self {
        Self::new(Vec::new()).expect("the empty plugin registry is valid")
    }

    pub(crate) fn new(mut plugins: Vec<PluginRegistryEntry>) -> Result<Self, LixError> {
        if plugins.len() > MAX_PLUGIN_REGISTRY_ENTRIES {
            return Err(invalid_registry(format!(
                "plugin_count {} exceeds the v1 limit of {MAX_PLUGIN_REGISTRY_ENTRIES}",
                plugins.len()
            )));
        }
        plugins.sort_by(|left, right| left.key.cmp(&right.key));
        for entry in &plugins {
            validate_entry(entry)?;
        }
        validate_strictly_increasing_plugin_keys(&plugins)?;

        let plugin_count = u32::try_from(plugins.len()).map_err(|_| {
            invalid_registry("plugin_count cannot be represented by the v1 registry format")
        })?;
        let generation = calculate_generation(&plugins)?;
        Ok(Self {
            plugin_count,
            generation,
            plugins,
        })
    }

    pub(crate) fn plugin_count(&self) -> u32 {
        self.plugin_count
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }

    pub(crate) fn generation(&self) -> &str {
        &self.generation
    }

    pub(crate) fn plugins(&self) -> &[PluginRegistryEntry] {
        &self.plugins
    }

    pub(crate) fn plugin(&self, key: &str) -> Option<&PluginRegistryEntry> {
        self.plugins
            .binary_search_by(|entry| entry.key.as_str().cmp(key))
            .ok()
            .map(|index| &self.plugins[index])
    }

    pub(crate) fn get(&self, key: &str) -> Option<&PluginRegistryEntry> {
        self.plugin(key)
    }

    pub(crate) fn upsert(
        &mut self,
        plugin: PluginRegistryEntry,
    ) -> Result<Option<PluginRegistryEntry>, LixError> {
        let mut next = self.clone();
        let replaced = match next
            .plugins
            .binary_search_by(|entry| entry.key.cmp(&plugin.key))
        {
            Ok(index) => Some(std::mem::replace(&mut next.plugins[index], plugin)),
            Err(index) => {
                next.plugins.insert(index, plugin);
                None
            }
        };
        next.recompute_generation()?;
        *self = next;
        Ok(replaced)
    }

    pub(crate) fn remove(
        &mut self,
        plugin_key: &str,
    ) -> Result<Option<PluginRegistryEntry>, LixError> {
        let mut next = self.clone();
        let removed = next
            .plugins
            .binary_search_by(|entry| entry.key.as_str().cmp(plugin_key))
            .ok()
            .map(|index| next.plugins.remove(index));
        next.recompute_generation()?;
        *self = next;
        Ok(removed)
    }

    pub(crate) fn recompute_generation(&mut self) -> Result<(), LixError> {
        if self.plugins.len() > MAX_PLUGIN_REGISTRY_ENTRIES {
            return Err(invalid_registry(format!(
                "plugin_count {} exceeds the v1 limit of {MAX_PLUGIN_REGISTRY_ENTRIES}",
                self.plugins.len()
            )));
        }
        validate_strictly_increasing_plugin_keys(&self.plugins)?;
        for entry in &self.plugins {
            validate_entry(entry)?;
        }
        self.plugin_count = u32::try_from(self.plugins.len()).map_err(|_| {
            invalid_registry("plugin_count cannot be represented by the v1 registry format")
        })?;
        self.generation = calculate_generation(&self.plugins)?;
        Ok(())
    }

    pub(crate) fn with_upserted(&self, plugin: PluginRegistryEntry) -> Result<Self, LixError> {
        let mut plugins = self.plugins.clone();
        match plugins.binary_search_by(|entry| entry.key.cmp(&plugin.key)) {
            Ok(index) => plugins[index] = plugin,
            Err(index) => plugins.insert(index, plugin),
        }
        Self::new(plugins)
    }

    pub(crate) fn without(&self, plugin_key: &str) -> Result<Self, LixError> {
        let mut plugins = self.plugins.clone();
        if let Ok(index) = plugins.binary_search_by(|entry| entry.key.as_str().cmp(plugin_key)) {
            plugins.remove(index);
        }
        Self::new(plugins)
    }

    /// Decode the JSON held in the `value` field. A missing entity is the
    /// canonical empty registry and requires no filesystem discovery.
    pub(crate) fn from_optional_value(value: Option<&JsonValue>) -> Result<Self, LixError> {
        let Some(value) = value else {
            return Ok(Self::empty());
        };
        let wire: PluginRegistryWire = serde_json::from_value(value.clone()).map_err(|error| {
            invalid_registry(format!("registry payload has an invalid shape: {error}"))
        })?;
        Self::from_wire(wire)
    }

    /// Decode and validate the complete `lix_key_value` snapshot wrapper.
    pub(crate) fn from_optional_snapshot(snapshot: Option<&JsonValue>) -> Result<Self, LixError> {
        let Some(snapshot) = snapshot else {
            return Ok(Self::empty());
        };
        let value = decode_key_value_snapshot(snapshot, PLUGIN_REGISTRY_KEY)?;
        Self::from_optional_value(Some(value))
    }

    pub(crate) fn from_optional_live_state_row(
        row: Option<&MaterializedLiveStateRow>,
        branch_id: &str,
    ) -> Result<Self, LixError> {
        let Some(row) = row else {
            return Ok(Self::empty());
        };
        validate_live_state_identity(row, PLUGIN_REGISTRY_KEY, None, branch_id)?;
        if row.deleted || row.snapshot_content.is_none() {
            return Ok(Self::empty());
        }
        let snapshot = parse_snapshot_content(row, "plugin registry")?;
        Self::from_optional_snapshot(Some(&snapshot))
    }

    pub(crate) fn to_value(&self) -> Result<JsonValue, LixError> {
        self.validate()?;
        serde_json::to_value(PluginRegistryWire {
            version: REGISTRY_FORMAT_VERSION,
            plugin_count: self.plugin_count,
            generation: self.generation.clone(),
            plugins: self.plugins.clone(),
        })
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to serialize plugin registry: {error}"),
            )
        })
    }

    pub(crate) fn to_snapshot(&self) -> Result<JsonValue, LixError> {
        Ok(json!({
            "key": PLUGIN_REGISTRY_KEY,
            "value": self.to_value()?,
        }))
    }

    pub(crate) fn to_canonical_json(&self) -> Result<String, LixError> {
        Ok(canonical_json(&self.to_value()?))
    }

    pub(crate) fn write_row(&self, branch_id: &str) -> Result<TransactionWriteRow, LixError> {
        tracked_key_value_write_row(
            PLUGIN_REGISTRY_KEY,
            None,
            Some(self.to_snapshot()?),
            branch_id,
        )
    }

    pub(crate) fn delete_row(branch_id: &str) -> Result<TransactionWriteRow, LixError> {
        tracked_key_value_write_row(PLUGIN_REGISTRY_KEY, None, None, branch_id)
    }

    fn from_wire(wire: PluginRegistryWire) -> Result<Self, LixError> {
        if wire.version != REGISTRY_FORMAT_VERSION {
            return Err(invalid_registry(format!(
                "unsupported version {}; expected {REGISTRY_FORMAT_VERSION}",
                wire.version
            )));
        }
        if wire.plugins.len() > MAX_PLUGIN_REGISTRY_ENTRIES {
            return Err(invalid_registry(format!(
                "plugin_count {} exceeds the v1 limit of {MAX_PLUGIN_REGISTRY_ENTRIES}",
                wire.plugins.len()
            )));
        }
        let actual_count = u32::try_from(wire.plugins.len()).map_err(|_| {
            invalid_registry("plugin_count cannot be represented by the v1 registry format")
        })?;
        if wire.plugin_count != actual_count {
            return Err(invalid_registry(format!(
                "plugin_count {} does not match {} plugin entries",
                wire.plugin_count, actual_count
            )));
        }
        validate_strictly_increasing_plugin_keys(&wire.plugins)?;
        for entry in &wire.plugins {
            validate_entry(entry)?;
        }
        let expected_generation = calculate_generation(&wire.plugins)?;
        if wire.generation != expected_generation {
            return Err(invalid_registry(format!(
                "generation integrity check failed: stored '{}' but calculated '{expected_generation}'",
                wire.generation
            )));
        }
        Ok(Self {
            plugin_count: wire.plugin_count,
            generation: wire.generation,
            plugins: wire.plugins,
        })
    }

    fn validate(&self) -> Result<(), LixError> {
        let wire = PluginRegistryWire {
            version: REGISTRY_FORMAT_VERSION,
            plugin_count: self.plugin_count,
            generation: self.generation.clone(),
            plugins: self.plugins.clone(),
        };
        Self::from_wire(wire).map(|_| ())
    }
}

/// Durable per-file ownership. `file_id` is storage identity, not duplicated
/// in the snapshot payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginFileOwner {
    file_id: String,
    plugin_key: String,
    schema_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PluginFileOwnerValue {
    version: u32,
    plugin_key: String,
    schema_keys: Vec<String>,
}

impl PluginFileOwner {
    pub(crate) fn new(
        file_id: impl Into<String>,
        plugin_key: impl Into<String>,
        mut schema_keys: Vec<String>,
    ) -> Result<Self, LixError> {
        schema_keys.sort();
        let owner = Self {
            file_id: file_id.into(),
            plugin_key: plugin_key.into(),
            schema_keys,
        };
        owner.validate()?;
        Ok(owner)
    }

    pub(crate) fn file_id(&self) -> &str {
        &self.file_id
    }

    pub(crate) fn plugin_key(&self) -> &str {
        &self.plugin_key
    }

    pub(crate) fn schema_keys(&self) -> &[String] {
        &self.schema_keys
    }

    pub(crate) fn from_registry_entry(
        file_id: impl Into<String>,
        plugin: &PluginRegistryEntry,
    ) -> Result<Self, LixError> {
        Self::new(file_id, plugin.key(), plugin.schema_keys().to_vec())
    }

    pub(crate) fn to_snapshot(&self) -> Result<JsonValue, LixError> {
        self.validate()?;
        Ok(json!({
            "key": PLUGIN_OWNER_KEY,
            "value": PluginFileOwnerValue {
                version: REGISTRY_FORMAT_VERSION,
                plugin_key: self.plugin_key.clone(),
                schema_keys: self.schema_keys.clone(),
            },
        }))
    }

    pub(crate) fn from_live_state_row(
        row: &MaterializedLiveStateRow,
        branch_id: &str,
    ) -> Result<Option<Self>, LixError> {
        let file_id = row.file_id.as_deref().ok_or_else(|| {
            invalid_registry("plugin owner row is missing its file_id storage identity")
        })?;
        validate_live_state_identity(row, PLUGIN_OWNER_KEY, Some(file_id), branch_id)?;
        if row.deleted || row.snapshot_content.is_none() {
            return Ok(None);
        }
        let snapshot = parse_snapshot_content(row, "plugin owner")?;
        let value = decode_key_value_snapshot(&snapshot, PLUGIN_OWNER_KEY)?;
        let owner_value: PluginFileOwnerValue =
            serde_json::from_value(value.clone()).map_err(|error| {
                invalid_registry(format!(
                    "plugin owner payload has an invalid shape: {error}"
                ))
            })?;
        if owner_value.version != REGISTRY_FORMAT_VERSION {
            return Err(invalid_registry(format!(
                "plugin owner version {} is unsupported; expected {REGISTRY_FORMAT_VERSION}",
                owner_value.version
            )));
        }
        Ok(Some(Self::new(
            file_id,
            owner_value.plugin_key,
            owner_value.schema_keys,
        )?))
    }

    pub(crate) fn write_row(&self, branch_id: &str) -> Result<TransactionWriteRow, LixError> {
        tracked_key_value_write_row(
            PLUGIN_OWNER_KEY,
            Some(self.file_id.clone()),
            Some(self.to_snapshot()?),
            branch_id,
        )
    }

    pub(crate) fn delete_row(
        file_id: impl Into<String>,
        branch_id: &str,
    ) -> Result<TransactionWriteRow, LixError> {
        let file_id = file_id.into();
        if file_id.is_empty() {
            return Err(invalid_registry("plugin owner file_id must not be empty"));
        }
        tracked_key_value_write_row(PLUGIN_OWNER_KEY, Some(file_id), None, branch_id)
    }

    fn validate(&self) -> Result<(), LixError> {
        if self.file_id.is_empty() {
            return Err(invalid_registry("plugin owner file_id must not be empty"));
        }
        if !valid_plugin_key(&self.plugin_key) {
            return Err(invalid_registry(format!(
                "plugin owner key '{}' is invalid",
                self.plugin_key
            )));
        }
        if self.schema_keys.is_empty() {
            return Err(invalid_registry(format!(
                "plugin owner for file '{}' must retain at least one schema key",
                self.file_id
            )));
        }
        if self.schema_keys.windows(2).any(|keys| keys[0] >= keys[1])
            || self.schema_keys.iter().any(String::is_empty)
        {
            return Err(invalid_registry(format!(
                "plugin owner for file '{}' schema_keys must be non-empty, unique, and lexicographically sorted",
                self.file_id
            )));
        }
        Ok(())
    }
}

/// One compiled multi-pattern matcher for a registry generation.
#[derive(Debug)]
pub(crate) struct CompiledPluginCatalog {
    generation: String,
    plugins: Arc<[PluginRegistryEntry]>,
    globs: GlobSet,
    specificity: Vec<(u8, i32)>,
}

impl CompiledPluginCatalog {
    pub(crate) fn compile(registry: &PluginRegistry) -> Result<Self, LixError> {
        registry.validate()?;
        let mut builder = GlobSetBuilder::new();
        let mut specificity = Vec::with_capacity(registry.plugins.len());
        for plugin in &registry.plugins {
            let glob = GlobBuilder::new(&plugin.path_glob)
                .literal_separator(false)
                .build()
                .map_err(|error| {
                    invalid_registry(format!(
                        "plugin '{}' has invalid path_glob '{}': {error}",
                        plugin.key, plugin.path_glob
                    ))
                })?;
            builder.add(glob);
            specificity.push(glob_specificity_rank(&plugin.path_glob));
        }
        let globs = builder.build().map_err(|error| {
            invalid_registry(format!("failed to compile plugin matcher catalog: {error}"))
        })?;
        Ok(Self {
            generation: registry.generation.clone(),
            plugins: registry.plugins.clone().into(),
            globs,
            specificity,
        })
    }

    pub(crate) fn generation(&self) -> &str {
        &self.generation
    }

    pub(crate) fn plugin_count(&self) -> usize {
        self.plugins.len()
    }

    /// Returns whether the named plugin's already-compiled glob matches the
    /// path, independent of whether another, more-specific plugin would win
    /// fresh-file selection.
    ///
    /// That distinction lets a durable file owner keep rendering under
    /// overlapping globs without recompiling an individual matcher. Content
    /// type is intentionally not rechecked: the owner records selection made
    /// when file bytes were available.
    pub(crate) fn matches_plugin(&self, plugin_key: &str, path: &str) -> bool {
        if path.is_empty() {
            return false;
        }
        let Ok(plugin_index) = self
            .plugins
            .binary_search_by(|plugin| plugin.key.as_str().cmp(plugin_key))
        else {
            return false;
        };
        self.globs.matches(path).contains(&plugin_index)
    }

    /// Select the most specific compatible matcher. When callers already have
    /// file bytes they pass the classified content type and both predicates
    /// must match. `None` deliberately keeps the prior path-only behavior for
    /// owner validation and other flows where bytes are not available.
    /// Equal-specificity matches resolve by lexicographically smallest plugin
    /// key because registry entries are canonicalized by key and matching
    /// indices are ascending.
    pub(crate) fn select(
        &self,
        path: &str,
        file_content_type: Option<PluginContentType>,
    ) -> Option<&PluginRegistryEntry> {
        self.select_with_content_type(path, || file_content_type)
    }

    /// Selects for a known payload without scanning its bytes unless at least
    /// one path-matching plugin actually declares a content-type constraint.
    pub(crate) fn select_for_bytes(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Option<&PluginRegistryEntry> {
        let mut classified = None;
        self.select_with_content_type(path, || {
            Some(*classified.get_or_insert_with(|| PluginContentType::from_bytes(bytes)))
        })
    }

    fn select_with_content_type(
        &self,
        path: &str,
        mut file_content_type: impl FnMut() -> Option<PluginContentType>,
    ) -> Option<&PluginRegistryEntry> {
        if path.is_empty() {
            return None;
        }
        let matches = self.globs.matches(path);
        let mut selected = None;
        let mut selected_rank = None;
        for index in matches {
            let rank = self.specificity[index];
            if selected_rank.is_some_and(|current| rank <= current) {
                continue;
            }
            if let Some(required) = self.plugins[index].content_type()
                && file_content_type().is_some_and(|actual| actual != required)
            {
                continue;
            }
            selected = Some(index);
            selected_rank = Some(rank);
        }
        selected.map(|index| &self.plugins[index])
    }
}

/// Small generation-keyed LRU. It is deliberately owned by an engine
/// context rather than process-global state, and its capacity is hard-bounded.
#[derive(Debug)]
pub(crate) struct PluginCatalogCache {
    catalogs: LruCache<String, Arc<CompiledPluginCatalog>>,
}

impl Default for PluginCatalogCache {
    fn default() -> Self {
        Self::new(DEFAULT_CACHED_PLUGIN_CATALOGS)
    }
}

impl PluginCatalogCache {
    pub(crate) fn new(requested_capacity: usize) -> Self {
        let capacity = requested_capacity.clamp(1, MAX_CACHED_PLUGIN_CATALOGS);
        Self {
            catalogs: LruCache::new(
                NonZeroUsize::new(capacity).expect("clamped plugin catalog capacity is non-zero"),
            ),
        }
    }

    pub(crate) fn get_or_compile(
        &mut self,
        registry: &PluginRegistry,
    ) -> Result<Arc<CompiledPluginCatalog>, LixError> {
        if let Some(catalog) = self.catalogs.get(registry.generation()) {
            return Ok(Arc::clone(catalog));
        }
        let catalog = Arc::new(CompiledPluginCatalog::compile(registry)?);
        self.catalogs
            .put(registry.generation().to_string(), Arc::clone(&catalog));
        Ok(catalog)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.catalogs.len()
    }
}

fn validate_entry(entry: &PluginRegistryEntry) -> Result<(), LixError> {
    if !valid_plugin_key(&entry.key) {
        return Err(invalid_registry(format!(
            "plugin key '{}' is invalid",
            entry.key
        )));
    }
    if entry.archive_file_id != plugin_storage_archive_file_id(&entry.key) {
        return Err(invalid_registry(format!(
            "plugin '{}' archive_file_id '{}' is not canonical",
            entry.key, entry.archive_file_id
        )));
    }
    if entry.archive_path != plugin_storage_archive_path(&entry.key) {
        return Err(invalid_registry(format!(
            "plugin '{}' archive_path '{}' is not canonical",
            entry.key, entry.archive_path
        )));
    }
    validate_blob_hash(&entry.archive_blob_hash, "archive_blob_hash", &entry.key)?;
    validate_blob_hash(&entry.wasm_blob_hash, "wasm_blob_hash", &entry.key)?;
    if entry.schema_keys.is_empty() {
        return Err(invalid_registry(format!(
            "plugin '{}' must own at least one schema",
            entry.key
        )));
    }
    if entry.schema_keys.windows(2).any(|keys| keys[0] >= keys[1]) {
        return Err(invalid_registry(format!(
            "plugin '{}' schema_keys must be unique and lexicographically sorted",
            entry.key
        )));
    }
    if entry.schema_keys.iter().any(String::is_empty) {
        return Err(invalid_registry(format!(
            "plugin '{}' has an empty schema key",
            entry.key
        )));
    }

    let manifest: PluginManifest = serde_json::from_str(&entry.manifest_json).map_err(|error| {
        invalid_registry(format!(
            "plugin '{}' manifest_json has an invalid shape: {error}",
            entry.key
        ))
    })?;
    if manifest.key != entry.key
        || manifest.runtime != entry.runtime
        || manifest.api_version != entry.api_version
        || manifest.file_match.path_glob != entry.path_glob
        || manifest.file_match.content_type != entry.content_type
        || manifest.entry != entry.entry
    {
        return Err(invalid_registry(format!(
            "plugin '{}' registry metadata does not match manifest_json",
            entry.key
        )));
    }
    let canonical_manifest = canonicalize_json_text(
        &entry.manifest_json,
        &format!("plugin '{}' manifest_json", entry.key),
    )?;
    if canonical_manifest != entry.manifest_json {
        return Err(invalid_registry(format!(
            "plugin '{}' manifest_json is not canonical",
            entry.key
        )));
    }
    Ok(())
}

fn validate_strictly_increasing_plugin_keys(
    plugins: &[PluginRegistryEntry],
) -> Result<(), LixError> {
    if plugins
        .windows(2)
        .any(|plugins| plugins[0].key >= plugins[1].key)
    {
        return Err(invalid_registry(
            "plugin entries must have unique, lexicographically sorted keys",
        ));
    }
    Ok(())
}

fn validate_blob_hash(hash: &str, field: &str, plugin_key: &str) -> Result<(), LixError> {
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(invalid_registry(format!(
            "plugin '{plugin_key}' {field} must be a 64-character lowercase hex hash"
        )));
    }
    Ok(())
}

fn valid_plugin_key(plugin_key: &str) -> bool {
    if plugin_key.is_empty() || plugin_key.len() > 128 {
        return false;
    }
    let mut bytes = plugin_key.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && bytes.all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'))
}

fn calculate_generation(plugins: &[PluginRegistryEntry]) -> Result<String, LixError> {
    let payload = serde_json::to_value(PluginRegistryGenerationPayload {
        version: REGISTRY_FORMAT_VERSION,
        plugins,
    })
    .map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to serialize plugin registry generation payload: {error}"),
        )
    })?;
    Ok(blake3::hash(canonical_json(&payload).as_bytes())
        .to_hex()
        .to_string())
}

fn canonicalize_json_text(raw: &str, context: &str) -> Result<String, LixError> {
    let value: JsonValue = serde_json::from_str(raw)
        .map_err(|error| invalid_registry(format!("{context} must be valid JSON: {error}")))?;
    Ok(canonical_json(&value))
}

fn canonical_json(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(value) => value.to_string(),
        JsonValue::Number(value) => value.to_string(),
        JsonValue::String(value) => {
            serde_json::to_string(value).expect("serializing a JSON string cannot fail")
        }
        JsonValue::Array(values) => {
            let mut out = String::from("[");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&canonical_json(value));
            }
            out.push(']');
            out
        }
        JsonValue::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            let mut out = String::from("{");
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(
                    &serde_json::to_string(key).expect("serializing a JSON key cannot fail"),
                );
                out.push(':');
                out.push_str(&canonical_json(&values[key]));
            }
            out.push('}');
            out
        }
    }
}

fn decode_key_value_snapshot<'a>(
    snapshot: &'a JsonValue,
    expected_key: &str,
) -> Result<&'a JsonValue, LixError> {
    let object = snapshot.as_object().ok_or_else(|| {
        invalid_registry(format!(
            "reserved lix_key_value '{expected_key}' snapshot must be an object"
        ))
    })?;
    if object.len() != 2 {
        return Err(invalid_registry(format!(
            "reserved lix_key_value '{expected_key}' snapshot must contain only key and value"
        )));
    }
    if object.get("key").and_then(JsonValue::as_str) != Some(expected_key) {
        return Err(invalid_registry(format!(
            "reserved lix_key_value snapshot key must be '{expected_key}'"
        )));
    }
    object.get("value").ok_or_else(|| {
        invalid_registry(format!(
            "reserved lix_key_value '{expected_key}' snapshot is missing value"
        ))
    })
}

fn tracked_key_value_write_row(
    key: &str,
    file_id: Option<String>,
    snapshot: Option<JsonValue>,
    branch_id: &str,
) -> Result<TransactionWriteRow, LixError> {
    validate_branch_local_scope(branch_id)?;
    let snapshot = snapshot
        .map(|snapshot| TransactionJson::from_value(snapshot, "plugin registry key-value row"))
        .transpose()?;
    Ok(TransactionWriteRow {
        entity_pk: Some(EntityPk::single(key)),
        schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
        file_id,
        snapshot,
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: branch_id.to_string(),
    })
}

fn validate_live_state_identity(
    row: &MaterializedLiveStateRow,
    key: &str,
    expected_file_id: Option<&str>,
    branch_id: &str,
) -> Result<(), LixError> {
    validate_branch_local_scope(branch_id)?;
    if row.schema_key != KEY_VALUE_SCHEMA_KEY
        || row.entity_pk.as_single_string().ok() != Some(key)
        || row.file_id.as_deref() != expected_file_id
        || row.global
        || row.untracked
        || row.branch_id != branch_id
    {
        return Err(invalid_registry(format!(
            "reserved plugin row '{key}' has invalid tracked branch-local storage identity"
        )));
    }
    Ok(())
}

fn validate_branch_local_scope(branch_id: &str) -> Result<(), LixError> {
    if branch_id.is_empty() || branch_id == GLOBAL_BRANCH_ID {
        return Err(invalid_registry(
            "plugin registry rows require a non-empty, non-global branch id",
        ));
    }
    Ok(())
}

fn parse_snapshot_content(
    row: &MaterializedLiveStateRow,
    kind: &str,
) -> Result<JsonValue, LixError> {
    let raw = row.snapshot_content.as_deref().ok_or_else(|| {
        invalid_registry(format!("{kind} live-state row is missing snapshot_content"))
    })?;
    serde_json::from_str(raw)
        .map_err(|error| invalid_registry(format!("{kind} snapshot is invalid JSON: {error}")))
}

fn glob_specificity_rank(glob: &str) -> (u8, i32) {
    if matches!(glob, "*" | "**/*" | "**") {
        return (0, i32::MIN);
    }
    let mut literal_chars = 0i32;
    let mut wildcard_chars = 0i32;
    for ch in glob.chars() {
        match ch {
            '*' | '?' | '[' | ']' | '{' | '}' => wildcard_chars += 1,
            _ => literal_chars += 1,
        }
    }
    (1, literal_chars - wildcard_chars)
}

fn invalid_registry(message: impl Into<String>) -> LixError {
    LixError::new(
        LixError::CODE_INVALID_PLUGIN,
        format!("Invalid durable plugin registry: {}", message.into()),
    )
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::Arc;

    use serde_json::{Value as JsonValue, json};

    use super::*;

    fn hash(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn manifest_with_content_type(
        key: &str,
        path_glob: &str,
        content_type: Option<PluginContentType>,
    ) -> String {
        let content_type = content_type
            .map(|content_type| {
                let value = serde_json::to_string(&content_type)
                    .expect("plugin content type should serialize");
                format!(r#","content_type":{value}"#)
            })
            .unwrap_or_default();
        format!(
            r#"{{
                "schemas":["schema/default.json"],
                "entry":"plugin.wasm",
                "match":{{"path_glob":{path_glob:?}{content_type}}},
                "api_version":"0.1.0",
                "runtime":"wasm-component-v1",
                "key":{key:?}
            }}"#
        )
    }

    fn entry(key: &str, path_glob: &str, hash_byte: char) -> PluginRegistryEntry {
        entry_with_content_type(key, path_glob, None, hash_byte)
    }

    fn entry_with_content_type(
        key: &str,
        path_glob: &str,
        content_type: Option<PluginContentType>,
        hash_byte: char,
    ) -> PluginRegistryEntry {
        PluginRegistryEntry::new(PluginRegistryEntryInput {
            key: key.to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: path_glob.to_string(),
            content_type,
            entry: "plugin.wasm".to_string(),
            schema_keys: vec![format!("{key}_schema")],
            manifest_json: manifest_with_content_type(key, path_glob, content_type),
            archive_file_id: plugin_storage_archive_file_id(key),
            archive_path: plugin_storage_archive_path(key),
            archive_blob_hash: hash(hash_byte),
            wasm_blob_hash: hash(hash_byte),
        })
        .expect("test registry entry should be valid")
    }

    #[test]
    fn missing_registry_is_empty_without_discovery() {
        let registry = PluginRegistry::from_optional_value(None).expect("missing row is valid");
        assert!(registry.is_empty());
        assert_eq!(registry.plugin_count(), 0);
        assert_eq!(registry.generation().len(), 64);
    }

    #[test]
    fn canonical_encoding_and_generation_ignore_input_order() {
        let first = PluginRegistry::new(vec![
            entry("plugin_b", "*.b", 'b'),
            entry("plugin_a", "*.a", 'a'),
        ])
        .expect("registry should be valid");
        let second = PluginRegistry::new(vec![
            entry("plugin_a", "*.a", 'a'),
            entry("plugin_b", "*.b", 'b'),
        ])
        .expect("registry should be valid");

        assert_eq!(first.generation(), second.generation());
        assert_eq!(
            first.to_canonical_json().unwrap(),
            second.to_canonical_json().unwrap()
        );
        assert_eq!(first.plugins()[0].key(), "plugin_a");

        let decoded = PluginRegistry::from_optional_snapshot(Some(&first.to_snapshot().unwrap()))
            .expect("canonical snapshot should decode");
        assert_eq!(decoded, first);
    }

    #[test]
    fn upsert_and_remove_change_generation_deterministically() {
        let empty = PluginRegistry::empty();
        let installed = empty
            .with_upserted(entry("plugin_a", "*.json", 'a'))
            .expect("install should be valid");
        assert_ne!(installed.generation(), empty.generation());
        assert_eq!(installed.plugin("plugin_a").unwrap().path_glob(), "*.json");
        let removed = installed
            .without("plugin_a")
            .expect("remove should be valid");
        assert_eq!(removed, empty);
    }

    #[test]
    fn rejects_count_generation_order_and_hash_integrity_failures() {
        let registry = PluginRegistry::new(vec![entry("plugin_a", "*.a", 'a')]).unwrap();
        let mut value = registry.to_value().unwrap();

        value["plugin_count"] = json!(2);
        assert_invalid(value.clone(), "plugin_count");
        value["plugin_count"] = json!(1);

        value["generation"] = json!(hash('f'));
        assert_invalid(value.clone(), "generation integrity");
        value["generation"] = json!(registry.generation());

        value["plugins"][0]["archive_blob_hash"] = json!("ABC");
        assert_invalid(value, "lowercase hex hash");

        let two = PluginRegistry::new(vec![
            entry("plugin_a", "*.a", 'a'),
            entry("plugin_b", "*.b", 'b'),
        ])
        .unwrap();
        let mut out_of_order = two.to_value().unwrap();
        out_of_order["plugins"].as_array_mut().unwrap().swap(0, 1);
        assert_invalid(out_of_order, "sorted keys");
    }

    #[test]
    fn content_type_is_required_and_path_only_matching_is_explicit() {
        let path_only = PluginRegistry::new(vec![entry("plugin_a", "*.json", 'a')]).unwrap();
        let path_only_value = path_only.to_value().unwrap();
        assert_eq!(
            path_only_value["plugins"][0]["content_type"],
            JsonValue::Null
        );
        assert_eq!(
            PluginRegistry::from_optional_value(Some(&path_only_value)).unwrap(),
            path_only
        );

        let mut missing = path_only_value;
        missing["plugins"][0]
            .as_object_mut()
            .unwrap()
            .remove("content_type");
        assert_invalid(missing, "missing field `content_type`");

        let typed = PluginRegistry::new(vec![entry_with_content_type(
            "plugin_a",
            "*.json",
            Some(PluginContentType::Text),
            'a',
        )])
        .unwrap();
        let typed_value = typed.to_value().unwrap();
        assert_eq!(typed_value["plugins"][0]["content_type"], json!("text"));
        assert_eq!(
            PluginRegistry::from_optional_value(Some(&typed_value)).unwrap(),
            typed
        );
        assert_ne!(path_only.generation(), typed.generation());

        let mut mismatched = typed_value;
        mismatched["plugins"][0]["content_type"] = json!("binary");
        assert_invalid(mismatched, "does not match manifest_json");
    }

    #[test]
    fn owner_rows_share_one_entity_key_and_use_file_id_identity() {
        let owner = PluginFileOwner::new(
            "file-a",
            "plugin_a",
            vec!["plugin_a_note".to_string(), "plugin_a_meta".to_string()],
        )
        .unwrap();
        let row = owner.write_row("main").unwrap();
        assert_eq!(
            row.entity_pk.unwrap().as_single_string().unwrap(),
            PLUGIN_OWNER_KEY
        );
        assert_eq!(row.file_id.as_deref(), Some("file-a"));
        assert!(!row.global);
        assert!(!row.untracked);
        assert_eq!(row.branch_id, "main");
        assert_eq!(row.snapshot.unwrap().value()["key"], PLUGIN_OWNER_KEY);
        assert_eq!(owner.schema_keys(), ["plugin_a_meta", "plugin_a_note"]);

        let registry_row = PluginRegistry::empty().write_row("main").unwrap();
        assert_eq!(registry_row.file_id, None);
        assert_eq!(
            registry_row.entity_pk.unwrap().as_single_string().unwrap(),
            PLUGIN_REGISTRY_KEY
        );
    }

    #[test]
    fn installed_plugin_materialization_verifies_extracted_wasm_hash() {
        let wasm = b"compiled component".to_vec();
        let mut input = PluginRegistryEntryInput {
            key: "plugin_a".to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: "*.json".to_string(),
            content_type: Some(PluginContentType::Text),
            entry: "plugin.wasm".to_string(),
            schema_keys: vec!["plugin_a_schema".to_string()],
            manifest_json: manifest_with_content_type(
                "plugin_a",
                "*.json",
                Some(PluginContentType::Text),
            ),
            archive_file_id: plugin_storage_archive_file_id("plugin_a"),
            archive_path: plugin_storage_archive_path("plugin_a"),
            archive_blob_hash: hash('a'),
            wasm_blob_hash: BlobHash::from_content(&wasm).to_hex(),
        };
        let registry_entry = PluginRegistryEntry::new(input.clone()).unwrap();
        let installed = registry_entry
            .to_installed_plugin(wasm.clone())
            .expect("matching extracted WASM should materialize");
        assert_eq!(installed.key, "plugin_a");
        assert_eq!(installed.content_type, Some(PluginContentType::Text));
        assert_eq!(installed.wasm_hash, BlobHash::from_content(&wasm));
        assert_eq!(installed.wasm, wasm);

        input.wasm_blob_hash = hash('b');
        let registry_entry = PluginRegistryEntry::new(input).unwrap();
        let error = registry_entry
            .to_installed_plugin(b"compiled component".to_vec())
            .expect_err("mismatched extracted WASM must fail integrity validation");
        assert!(error.message.contains("WASM bytes hash"));
    }

    #[test]
    fn compiled_catalog_is_deterministic_and_lru_is_bounded() {
        let registry = PluginRegistry::new(vec![
            entry("plugin_z", "*.json", 'a'),
            entry("plugin_a", "*.json", 'b'),
            entry("plugin_specific", "src/*.json", 'c'),
            entry("plugin_all", "**/*", 'd'),
        ])
        .unwrap();
        let catalog = CompiledPluginCatalog::compile(&registry).unwrap();
        assert_eq!(
            catalog.select("src/data.json", None).unwrap().key(),
            "plugin_specific"
        );
        assert_eq!(catalog.select("data.json", None).unwrap().key(), "plugin_a");
        assert_eq!(
            catalog.select("data.txt", None).unwrap().key(),
            "plugin_all"
        );
        assert!(catalog.select("", None).is_none());
        assert!(catalog.matches_plugin("plugin_specific", "src/data.json"));
        assert!(catalog.matches_plugin("plugin_a", "src/data.json"));
        assert!(catalog.matches_plugin("plugin_z", "src/data.json"));
        assert!(catalog.matches_plugin("plugin_all", "src/data.json"));
        assert!(!catalog.matches_plugin("plugin_specific", "data.json"));
        assert!(!catalog.matches_plugin("missing", "src/data.json"));
        assert!(!catalog.matches_plugin("plugin_all", ""));

        let mut cache = PluginCatalogCache::new(2);
        let first = cache.get_or_compile(&registry).unwrap();
        let hit = cache.get_or_compile(&registry).unwrap();
        assert!(Arc::ptr_eq(&first, &hit));
        for index in 0..3 {
            let next = PluginRegistry::new(vec![entry(
                &format!("plugin_{index}"),
                &format!("*.{index}"),
                char::from_digit(index + 1, 16).unwrap(),
            )])
            .unwrap();
            cache.get_or_compile(&next).unwrap();
        }
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn compiled_catalog_applies_content_type_only_when_known() {
        assert_eq!(PluginContentType::from_bytes(b""), PluginContentType::Text);
        assert_eq!(
            PluginContentType::from_bytes(b"hello"),
            PluginContentType::Text
        );
        assert_eq!(
            PluginContentType::from_bytes(&[0xff, 0xfe]),
            PluginContentType::Binary
        );

        let text =
            entry_with_content_type("plugin_text", "*.data", Some(PluginContentType::Text), 'a');
        let binary = entry_with_content_type(
            "plugin_binary",
            "*.data",
            Some(PluginContentType::Binary),
            'b',
        );
        let text_only =
            CompiledPluginCatalog::compile(&PluginRegistry::new(vec![text.clone()]).unwrap())
                .unwrap();
        let classification_calls = Cell::new(0);
        assert!(
            text_only
                .select_with_content_type("document.other", || {
                    classification_calls.set(classification_calls.get() + 1);
                    Some(PluginContentType::Text)
                })
                .is_none()
        );
        assert_eq!(classification_calls.get(), 0);
        assert_eq!(
            text_only
                .select("document.data", Some(PluginContentType::Text))
                .map(PluginRegistryEntry::key),
            Some("plugin_text")
        );
        assert!(
            text_only
                .select("document.data", Some(PluginContentType::Binary))
                .is_none()
        );

        let catalog =
            CompiledPluginCatalog::compile(&PluginRegistry::new(vec![text, binary]).unwrap())
                .unwrap();
        assert_eq!(
            catalog
                .select("document.data", Some(PluginContentType::Text))
                .map(PluginRegistryEntry::key),
            Some("plugin_text")
        );
        assert_eq!(
            catalog
                .select("document.data", Some(PluginContentType::Binary))
                .map(PluginRegistryEntry::key),
            Some("plugin_binary")
        );
        assert_eq!(
            catalog
                .select_for_bytes("document.data", b"hello")
                .map(PluginRegistryEntry::key),
            Some("plugin_text")
        );
        assert_eq!(
            catalog
                .select_for_bytes("document.data", &[0xff, 0xfe])
                .map(PluginRegistryEntry::key),
            Some("plugin_binary")
        );
        assert_eq!(
            catalog
                .select("document.data", None)
                .map(PluginRegistryEntry::key),
            Some("plugin_binary")
        );
        assert!(catalog.matches_plugin("plugin_text", "document.data"));
        assert!(catalog.matches_plugin("plugin_binary", "document.data"));
    }

    #[test]
    fn complete_snapshot_wrapper_is_strict() {
        let registry = PluginRegistry::empty();
        let mut wrong_key = registry.to_snapshot().unwrap();
        wrong_key["key"] = json!("not_the_registry");
        let error = PluginRegistry::from_optional_snapshot(Some(&wrong_key)).unwrap_err();
        assert!(error.message.contains("snapshot key"));

        let extra = json!({
            "key": PLUGIN_REGISTRY_KEY,
            "value": registry.to_value().unwrap(),
            "extra": true,
        });
        let error = PluginRegistry::from_optional_snapshot(Some(&extra)).unwrap_err();
        assert!(error.message.contains("only key and value"));
    }

    fn assert_invalid(value: JsonValue, expected: &str) {
        let error = PluginRegistry::from_optional_value(Some(&value))
            .expect_err("registry value should be rejected");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(
            error.message.contains(expected),
            "expected {expected:?} in {}",
            error.message
        );
    }
}
