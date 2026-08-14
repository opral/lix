//! Durable, branch-local plugin registry state.
//!
//! The registry is one tracked `lix_key_value` row per branch. File
//! ownership uses the same reserved row key for every file and relies on
//! `file_id` for identity. That layout gives the transaction hot paths one
//! exact registry read and one batched owner read instead of a filesystem
//! scan.

use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use lru::LruCache;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

use crate::binary_cas::BlobId;
use crate::branch::BranchHeadControl;
use crate::changelog::{ChangeRecordProjection, CommitId};
use crate::hot_state::MaterializedHotStateRow;
use crate::row_pk::RowPk;
use crate::tracked_state::{
    MaterializedTrackedStateRowRef, TrackedStateFilter, TrackedStateReadColumns,
    TrackedStateScanRequest, TrackedStateStoreReader,
};
use crate::transaction_types::{TransactionJson, TransactionWriteRow};
use crate::{GLOBAL_BRANCH_ID, LixError, NullableKeyFilter};

use super::InstalledPlugin;
use super::manifest::{
    PluginContentMatcher, PluginManifest, PluginRuntime, parse_plugin_manifest_json,
    validate_runtime_api_version,
};
use super::storage::{plugin_storage_archive_file_id, plugin_storage_archive_path};

pub(crate) const PLUGIN_REGISTRY_KEY: &str = "lix_plugin_registry_v2";
pub(crate) const PLUGIN_OWNER_KEY: &str = "lix_plugin_owner_v2";
pub(crate) const MAX_PLUGIN_REGISTRY_ENTRIES: usize = 128;

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const PLUGIN_REGISTRY_FORMAT_VERSION: u32 = 5;
const PLUGIN_FILE_OWNER_FORMAT_VERSION: u32 = 2;
const MAX_CACHED_PLUGIN_CATALOGS: usize = 16;
const DEFAULT_CACHED_PLUGIN_CATALOGS: usize = 8;

/// Install-time data used to construct one canonical registry entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginRegistryEntryInput {
    pub(crate) key: String,
    pub(crate) runtime: PluginRuntime,
    pub(crate) api_version: String,
    pub(crate) path_glob: String,
    pub(crate) content: Option<PluginContentMatcher>,
    pub(crate) entry: String,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) create_schema_keys: Vec<String>,
    pub(crate) manifest_json: String,
    pub(crate) archive_file_id: String,
    pub(crate) archive_path: String,
    pub(crate) archive_blob_hash: String,
    pub(crate) wasm_blob_hash: String,
}

/// Metadata needed by current-state plugin matching and execution.
///
/// Path-only matching is encoded explicitly as `content: null`. Registry
/// rows are an internal engine format, so missing fields are rejected instead
/// of carrying compatibility for unreleased representations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginRegistryEntry {
    key: String,
    runtime: PluginRuntime,
    api_version: String,
    path_glob: String,
    #[serde(deserialize_with = "deserialize_required_content")]
    content: Option<PluginContentMatcher>,
    entry: String,
    schema_keys: Vec<String>,
    create_schema_keys: Vec<String>,
    manifest_json: String,
    archive_file_id: String,
    archive_path: String,
    archive_blob_hash: String,
    wasm_blob_hash: String,
}

fn deserialize_required_content<'de, D>(
    deserializer: D,
) -> Result<Option<PluginContentMatcher>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<PluginContentMatcher>::deserialize(deserializer)
}

impl PluginRegistryEntry {
    pub(crate) fn new(input: PluginRegistryEntryInput) -> Result<Self, LixError> {
        let manifest_json =
            canonicalize_json_text(&input.manifest_json, "plugin registry manifest_json")?;
        parse_plugin_manifest_json(&manifest_json)?;
        let mut entry = Self {
            key: input.key,
            runtime: input.runtime,
            api_version: input.api_version,
            path_glob: input.path_glob,
            content: input.content,
            entry: input.entry,
            schema_keys: input.schema_keys,
            create_schema_keys: input.create_schema_keys,
            manifest_json,
            archive_file_id: input.archive_file_id,
            archive_path: input.archive_path,
            archive_blob_hash: input.archive_blob_hash,
            wasm_blob_hash: input.wasm_blob_hash,
        };
        entry.schema_keys.sort();
        entry.create_schema_keys.sort();
        // Install-time validation pays the complete JSON-Schema and glob
        // checks once. Durable reads below use the already-validated compact
        // fields and generation integrity, so warm transactions do not
        // recompile one glob per plugin before consulting the catalog cache.
        validate_entry(&entry)?;
        Ok(entry)
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn content(&self) -> Option<PluginContentMatcher> {
        self.content
    }

    pub(crate) fn api_version(&self) -> &str {
        &self.api_version
    }

    pub(crate) fn schema_keys(&self) -> &[String] {
        &self.schema_keys
    }

    pub(crate) fn create_schema_keys(&self) -> &[String] {
        &self.create_schema_keys
    }

    pub(crate) fn archive_blob_hash(&self) -> &str {
        &self.archive_blob_hash
    }

    pub(crate) fn wasm_blob_hash(&self) -> &str {
        &self.wasm_blob_hash
    }

    /// Verifies the durable contract that every existing owner relies on
    /// before a content-addressed component component generation is replaced.
    ///
    /// Schema definitions themselves live in `lix_registered_schema` and are
    /// compared by the lifecycle reconciler. This check covers the registry
    /// half of that contract, including the exact schema-key set.
    pub(crate) fn validate_owned_upgrade_contract(
        &self,
        replacement: &Self,
    ) -> Result<(), LixError> {
        let incompatible = self.key != replacement.key
            || self.api_version != replacement.api_version
            || self.path_glob != replacement.path_glob
            || self.content != replacement.content
            || self.schema_keys != replacement.schema_keys
            || self.create_schema_keys != replacement.create_schema_keys;
        if incompatible {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                format!(
                    "owned plugin '{}' may only upgrade between wasm-component generations with the same API version, matcher, content type, schema keys, and create-default contract",
                    self.key
                ),
            )
            .with_hint(
                "Move or delete every owned file before changing the plugin contract, then install the replacement archive.",
            ));
        }
        Ok(())
    }

    pub(crate) fn to_installed_plugin(&self, wasm: Vec<u8>) -> Result<InstalledPlugin, LixError> {
        let wasm_hash = BlobId::from_content(&wasm);
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
            content: self.content,
            entry: self.entry.clone(),
            schema_keys: self.schema_keys.clone(),
            manifest_json: self.manifest_json.clone(),
            wasm_hash,
            wasm,
        })
    }
}

/// Canonical contents of `lix_key_value:lix_plugin_registry_v2`.
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
                "plugin_count {} exceeds the registry capacity of {MAX_PLUGIN_REGISTRY_ENTRIES}",
                plugins.len()
            )));
        }
        plugins.sort_by(|left, right| left.key.cmp(&right.key));
        for entry in &plugins {
            validate_entry(entry)?;
        }
        validate_strictly_increasing_plugin_keys(&plugins)?;

        let plugin_count = u32::try_from(plugins.len()).map_err(|_| {
            invalid_registry("plugin_count cannot be represented by the registry format")
        })?;
        let generation = calculate_generation(&plugins)?;
        Ok(Self {
            plugin_count,
            generation,
            plugins,
        })
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
                "plugin_count {} exceeds the registry capacity of {MAX_PLUGIN_REGISTRY_ENTRIES}",
                self.plugins.len()
            )));
        }
        validate_strictly_increasing_plugin_keys(&self.plugins)?;
        for entry in &self.plugins {
            validate_entry(entry)?;
        }
        self.plugin_count = u32::try_from(self.plugins.len()).map_err(|_| {
            invalid_registry("plugin_count cannot be represented by the registry format")
        })?;
        self.generation = calculate_generation(&self.plugins)?;
        Ok(())
    }

    /// Decode the JSON held in the `value` field. A missing row is the
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

    pub(crate) fn from_optional_hot_state_row(
        row: Option<&MaterializedHotStateRow>,
        branch_id: &str,
    ) -> Result<Self, LixError> {
        let Some(row) = row else {
            return Ok(Self::empty());
        };
        // The branch registry is branch-global with no file, so it is always
        // tracked regardless of any file's lane.
        validate_hot_state_identity(row, PLUGIN_REGISTRY_KEY, None, branch_id, false)?;
        if row.deleted || row.snapshot_content.is_none() {
            return Ok(Self::empty());
        }
        let snapshot = parse_snapshot_content(row, "plugin registry")?;
        Self::from_optional_snapshot(Some(&snapshot))
    }

    pub(crate) fn to_value(&self) -> Result<JsonValue, LixError> {
        self.validate()?;
        serde_json::to_value(PluginRegistryWire {
            version: PLUGIN_REGISTRY_FORMAT_VERSION,
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

    pub(crate) fn write_row(&self, branch_id: &str) -> Result<TransactionWriteRow, LixError> {
        plugin_key_value_write_row(
            PLUGIN_REGISTRY_KEY,
            None,
            Some(self.to_snapshot()?),
            branch_id,
            false,
        )
    }

    fn from_wire(wire: PluginRegistryWire) -> Result<Self, LixError> {
        if wire.version != PLUGIN_REGISTRY_FORMAT_VERSION {
            return Err(invalid_registry(format!(
                "unsupported version {}; expected {PLUGIN_REGISTRY_FORMAT_VERSION}",
                wire.version
            )));
        }
        if wire.plugins.len() > MAX_PLUGIN_REGISTRY_ENTRIES {
            return Err(invalid_registry(format!(
                "plugin_count {} exceeds the registry capacity of {MAX_PLUGIN_REGISTRY_ENTRIES}",
                wire.plugins.len()
            )));
        }
        let actual_count = u32::try_from(wire.plugins.len()).map_err(|_| {
            invalid_registry("plugin_count cannot be represented by the registry format")
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
            version: PLUGIN_REGISTRY_FORMAT_VERSION,
            plugin_count: self.plugin_count,
            generation: self.generation.clone(),
            plugins: self.plugins.clone(),
        };
        Self::from_wire(wire).map(|_| ())
    }
}

/// Loads one retained registry through the plugin-owned durable decoder.
pub(crate) async fn load_plugin_registry_at_commit<S>(
    reader: &mut TrackedStateStoreReader<S>,
    commit_id: &str,
) -> Result<PluginRegistry, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    let registry_key = crate::tracked_state::TrackedStateKey {
        schema_key: KEY_VALUE_SCHEMA_KEY.to_owned(),
        row_pk: RowPk::single(PLUGIN_REGISTRY_KEY),
        file_id: None,
    };
    let rows = reader
        .load_projected_batch_at_commit(
            commit_id,
            std::slice::from_ref(&registry_key),
            &ChangeRecordProjection::full(),
        )
        .await?
        .into_rows();
    let row = rows.into_iter().next().flatten();
    let snapshot = match row {
        None => None,
        Some(row) if row.deleted || row.snapshot_content.is_none() => None,
        Some(row) => Some(
            serde_json::from_str(row.snapshot_content.as_deref().expect("checked")).map_err(
                |error| {
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!("historical plugin registry snapshot is invalid JSON: {error}"),
                    )
                },
            )?,
        ),
    };
    PluginRegistry::from_optional_snapshot(snapshot.as_ref())
}

/// Re-derives every WASM payload root owned by current and retained plugin
/// registry generations. Registry snapshots remain the sole serving authority;
/// this returns only their authenticated content hashes for binary-CAS marking.
pub(crate) async fn collect_gc_wasm_blob_roots<S>(
    store: &S,
    controls: &[(String, BranchHeadControl)],
    retained_commits: &BTreeSet<CommitId>,
) -> Result<BTreeSet<BlobId>, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    let request = TrackedStateScanRequest {
        filter: TrackedStateFilter {
            schema_keys: vec![KEY_VALUE_SCHEMA_KEY.to_owned()],
            row_pks: vec![RowPk::single(PLUGIN_REGISTRY_KEY)],
            file_ids: vec![NullableKeyFilter::Null],
            ..TrackedStateFilter::default()
        },
        read_columns: TrackedStateReadColumns {
            columns: vec!["snapshot_content".to_owned()],
        },
        limit: None,
    };
    let current = crate::hot_state::TrackedHeadContext::new()
        .reader(store)
        .scan_live_batches_for_controls(controls, &request, None)
        .await?;
    let mut roots = BTreeSet::new();
    for (branch_id, rows) in current {
        for row in rows.into_rows() {
            let registry = PluginRegistry::from_optional_hot_state_row(Some(&row), &branch_id)?;
            extend_registry_wasm_roots(&registry, &mut roots)?;
        }
    }

    let retained_schema_keys = [KEY_VALUE_SCHEMA_KEY.to_owned()];
    for commit_id in retained_commits {
        for row in crate::tracked_state::load_retained_commit_snapshots_for_schemas(
            store,
            *commit_id,
            &retained_schema_keys,
        )
        .await?
        {
            if row.deleted
                || row.key.file_id.is_some()
                || row.key.row_pk != RowPk::single(PLUGIN_REGISTRY_KEY)
            {
                continue;
            }
            let snapshot: JsonValue = serde_json::from_str(
                row.snapshot.as_deref().ok_or_else(|| {
                    invalid_registry(format!(
                        "historical plugin registry mutation in commit '{commit_id}' has no snapshot"
                    ))
                })?,
            )
            .map_err(|error| {
                invalid_registry(format!(
                    "historical plugin registry mutation in commit '{commit_id}' is invalid JSON: {error}"
                ))
            })?;
            let registry = PluginRegistry::from_optional_snapshot(Some(&snapshot))?;
            extend_registry_wasm_roots(&registry, &mut roots)?;
        }
    }
    Ok(roots)
}

fn extend_registry_wasm_roots(
    registry: &PluginRegistry,
    roots: &mut BTreeSet<BlobId>,
) -> Result<(), LixError> {
    for plugin in registry.plugins() {
        roots.insert(BlobId::from_hex(plugin.wasm_blob_hash())?);
    }
    Ok(())
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
                version: PLUGIN_FILE_OWNER_FORMAT_VERSION,
                plugin_key: self.plugin_key.clone(),
                schema_keys: self.schema_keys.clone(),
            },
        }))
    }

    pub(crate) fn from_hot_state_row(
        row: &MaterializedHotStateRow,
        branch_id: &str,
        untracked: bool,
    ) -> Result<Option<Self>, LixError> {
        let file_id = row.file_id.as_deref().ok_or_else(|| {
            invalid_registry("plugin owner row is missing its file_id storage identity")
        })?;
        validate_hot_state_identity(row, PLUGIN_OWNER_KEY, Some(file_id), branch_id, untracked)?;
        if row.deleted || row.snapshot_content.is_none() {
            return Ok(None);
        }
        let snapshot = parse_snapshot_content(row, "plugin owner")?;
        Self::from_snapshot(file_id, &snapshot).map(Some)
    }

    #[cfg(test)]
    pub(crate) fn from_tracked_state_row(
        row: &crate::tracked_state::MaterializedTrackedStateRow,
    ) -> Result<Option<Self>, LixError> {
        let file_id = row.file_id.as_deref().ok_or_else(|| {
            invalid_registry("plugin owner row is missing its file_id storage identity")
        })?;
        if row.schema_key != KEY_VALUE_SCHEMA_KEY || row.row_pk != RowPk::single(PLUGIN_OWNER_KEY) {
            return Err(invalid_registry(
                "tracked plugin owner row has an invalid storage identity",
            ));
        }
        if row.deleted || row.snapshot_content.is_none() {
            return Ok(None);
        }
        let snapshot = serde_json::from_str(
            row.snapshot_content.as_deref().expect("checked above"),
        )
        .map_err(|error| {
            invalid_registry(format!(
                "tracked plugin owner snapshot is invalid JSON: {error}"
            ))
        })?;
        Self::from_snapshot(file_id, &snapshot).map(Some)
    }

    pub(crate) fn from_tracked_state_row_ref(
        row: MaterializedTrackedStateRowRef<'_>,
    ) -> Result<Option<Self>, LixError> {
        let file_id = row.file_id().ok_or_else(|| {
            invalid_registry("plugin owner row is missing its file_id storage identity")
        })?;
        if row.schema_key() != KEY_VALUE_SCHEMA_KEY
            || row.row_pk().as_single_string().ok() != Some(PLUGIN_OWNER_KEY)
        {
            return Err(invalid_registry(
                "tracked plugin owner row has an invalid storage identity",
            ));
        }
        if row.deleted() || row.snapshot_content().is_none() {
            return Ok(None);
        }
        let snapshot = serde_json::from_str(
            row.snapshot_content().expect("checked above").as_str(),
        )
        .map_err(|error| {
            invalid_registry(format!(
                "tracked plugin owner snapshot is invalid JSON: {error}"
            ))
        })?;
        Self::from_snapshot(file_id, &snapshot).map(Some)
    }

    pub(crate) fn from_snapshot(
        file_id: impl Into<String>,
        snapshot: &JsonValue,
    ) -> Result<Self, LixError> {
        let file_id = file_id.into();
        let value = decode_key_value_snapshot(snapshot, PLUGIN_OWNER_KEY)?;
        let owner_value: PluginFileOwnerValue =
            serde_json::from_value(value.clone()).map_err(|error| {
                invalid_registry(format!(
                    "plugin owner payload has an invalid shape: {error}"
                ))
            })?;
        if owner_value.version != PLUGIN_FILE_OWNER_FORMAT_VERSION {
            return Err(invalid_registry(format!(
                "plugin owner version {} is unsupported; expected {PLUGIN_FILE_OWNER_FORMAT_VERSION}",
                owner_value.version
            )));
        }
        Self::new(file_id, owner_value.plugin_key, owner_value.schema_keys)
    }

    pub(crate) fn write_row(
        &self,
        branch_id: &str,
        untracked: bool,
    ) -> Result<TransactionWriteRow, LixError> {
        plugin_key_value_write_row(
            PLUGIN_OWNER_KEY,
            Some(self.file_id.clone()),
            Some(self.to_snapshot()?),
            branch_id,
            untracked,
        )
    }

    pub(crate) fn delete_row(
        file_id: impl Into<String>,
        branch_id: &str,
        untracked: bool,
    ) -> Result<TransactionWriteRow, LixError> {
        let file_id = file_id.into();
        if file_id.is_empty() {
            return Err(invalid_registry("plugin owner file_id must not be empty"));
        }
        plugin_key_value_write_row(PLUGIN_OWNER_KEY, Some(file_id), None, branch_id, untracked)
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
            plugins: registry.plugins.clone().into(),
            globs,
            specificity,
        })
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

    /// Selects for a known payload without scanning its bytes unless at least
    /// one path-matching plugin actually declares a content-type constraint.
    pub(crate) fn select_for_bytes(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Option<&PluginRegistryEntry> {
        self.select_for_bytes_with_classification_work(path, bytes)
            .0
    }

    /// Selects a plugin and reports bytes examined by the lazy full-payload
    /// content classifier. Path-only catalogs therefore report zero even for
    /// large payloads.
    pub(crate) fn select_for_bytes_with_classification_work(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> (Option<&PluginRegistryEntry>, u64) {
        let mut utf8 = None;
        let mut prefix_matches = HashMap::new();
        let mut classified_bytes = 0u64;
        let selected = self.select_with_content(path, |required| {
            let matches = match required {
                PluginContentMatcher::Text | PluginContentMatcher::Binary => {
                    let is_utf8 = *utf8.get_or_insert_with(|| {
                        classified_bytes = classified_bytes.saturating_add(bytes.len() as u64);
                        std::str::from_utf8(bytes).is_ok()
                    });
                    match required {
                        PluginContentMatcher::Text => is_utf8,
                        PluginContentMatcher::Binary => !is_utf8,
                        PluginContentMatcher::PrefixExcludes { .. } => {
                            unreachable!("prefix predicates use their bounded classifier branch")
                        }
                    }
                }
                matcher @ PluginContentMatcher::PrefixExcludes {
                    bytes: scan_bytes, ..
                } => *prefix_matches.entry(matcher).or_insert_with(|| {
                    classified_bytes =
                        classified_bytes.saturating_add(bytes.len().min(scan_bytes) as u64);
                    matcher.matches_bytes(bytes)
                }),
            };
            Some(matches)
        });
        (selected, classified_bytes)
    }

    fn select_with_content(
        &self,
        path: &str,
        mut content_matches: impl FnMut(PluginContentMatcher) -> Option<bool>,
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
            if let Some(required) = self.plugins[index].content()
                && !content_matches(required).unwrap_or(false)
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
    if entry
        .create_schema_keys
        .windows(2)
        .any(|keys| keys[0] >= keys[1])
        || entry
            .create_schema_keys
            .iter()
            .any(|key| entry.schema_keys.binary_search(key).is_err())
    {
        return Err(invalid_registry(format!(
            "plugin '{}' create_schema_keys must be unique, sorted, and owned by the plugin",
            entry.key
        )));
    }
    let manifest: PluginManifest = serde_json::from_str(&entry.manifest_json).map_err(|error| {
        invalid_registry(format!(
            "plugin '{}' manifest_json has an invalid shape: {error}",
            entry.key
        ))
    })?;
    validate_runtime_api_version(entry.runtime, &entry.api_version).map_err(|error| {
        invalid_registry(format!(
            "plugin '{}' manifest_json has an unsupported API version: {}",
            entry.key, error.message
        ))
    })?;
    if manifest.key != entry.key
        || manifest.file_match.path_glob != entry.path_glob
        || manifest.file_match.content != entry.content
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
        version: PLUGIN_REGISTRY_FORMAT_VERSION,
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

/// Builds a reserved `lix_key_value` row for plugin bookkeeping.
///
/// File-scoped plugin rows must be written in the same durability lane as the
/// file they describe, so the lane is a parameter rather than a constant. Rows
/// that are not file-scoped (the branch's plugin registry) are always tracked.
fn plugin_key_value_write_row(
    key: &str,
    file_id: Option<String>,
    snapshot: Option<JsonValue>,
    branch_id: &str,
    untracked: bool,
) -> Result<TransactionWriteRow, LixError> {
    validate_branch_local_scope(branch_id)?;
    let snapshot = snapshot
        .map(|snapshot| TransactionJson::from_value(snapshot, "plugin registry key-value row"))
        .transpose()?;
    Ok(TransactionWriteRow {
        row_pk: Some(RowPk::single(key)),
        schema_key: KEY_VALUE_SCHEMA_KEY.into(),
        file_id: file_id.map(Into::into),
        snapshot,
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked,
        branch_id: branch_id.into(),
    })
}

fn validate_hot_state_identity(
    row: &MaterializedHotStateRow,
    key: &str,
    expected_file_id: Option<&str>,
    branch_id: &str,
    expected_untracked: bool,
) -> Result<(), LixError> {
    validate_branch_local_scope(branch_id)?;
    if row.schema_key != KEY_VALUE_SCHEMA_KEY
        || row.row_pk.as_single_string().ok() != Some(key)
        || row.file_id.as_deref() != expected_file_id
        || row.global
        // A file-scoped reserved row lives in its own file's lane. The branch
        // registry stays tracked (it is branch-global with no file), but an
        // owner row for an untracked file is untracked, and reading it back
        // must accept exactly the lane it was written in.
        || row.untracked != expected_untracked
        || row.branch_id.as_ref() != branch_id
    {
        return Err(invalid_registry(format!(
            "reserved plugin row '{key}' has invalid branch-local storage identity"
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
    row: &MaterializedHotStateRow,
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

    fn manifest_with_content(
        key: &str,
        path_glob: &str,
        content: Option<PluginContentMatcher>,
    ) -> String {
        let content = content
            .map(|content| {
                let value =
                    serde_json::to_string(&content).expect("plugin content type should serialize");
                format!(r#","content":{value}"#)
            })
            .unwrap_or_default();
        format!(
            r#"{{
                "schemas":["schema/default.json"],
                "entry":"plugin.wasm",
                "match":{{"path_glob":{path_glob:?}{content}}},
                "key":{key:?}
            }}"#
        )
    }

    fn entry(key: &str, path_glob: &str, hash_byte: char) -> PluginRegistryEntry {
        entry_with_content(key, path_glob, None, hash_byte)
    }

    fn entry_with_content(
        key: &str,
        path_glob: &str,
        content: Option<PluginContentMatcher>,
        hash_byte: char,
    ) -> PluginRegistryEntry {
        PluginRegistryEntry::new(PluginRegistryEntryInput {
            key: key.to_string(),
            runtime: PluginRuntime::WasmComponent,
            api_version: "1.0.0".to_string(),
            path_glob: path_glob.to_string(),
            content,
            entry: "plugin.wasm".to_string(),
            schema_keys: vec![format!("{key}_schema")],
            create_schema_keys: Vec::new(),
            manifest_json: manifest_with_content(key, path_glob, content),
            archive_file_id: plugin_storage_archive_file_id(key),
            archive_path: plugin_storage_archive_path(key),
            archive_blob_hash: hash(hash_byte),
            wasm_blob_hash: hash(hash_byte),
        })
        .expect("test registry entry should be valid")
    }

    fn component_entry(hash_byte: char) -> PluginRegistryEntry {
        let key = "plugin_csv";
        let path_glob = "*.csv";
        PluginRegistryEntry::new(PluginRegistryEntryInput {
            key: key.to_string(),
            runtime: PluginRuntime::WasmComponent,
            api_version: "1.0.0".to_string(),
            path_glob: path_glob.to_string(),
            content: Some(PluginContentMatcher::Text),
            entry: "plugin.wasm".to_string(),
            schema_keys: vec!["csv_row".to_string()],
            create_schema_keys: vec!["csv_row".to_string()],
            manifest_json: format!(
                r#"{{"entry":"plugin.wasm","key":"{key}","match":{{"content":"text","path_glob":"{path_glob}"}},"schemas":["schema/csv_row.json"]}}"#
            ),
            archive_file_id: plugin_storage_archive_file_id(key),
            archive_path: plugin_storage_archive_path(key),
            archive_blob_hash: hash(hash_byte),
            wasm_blob_hash: hash(hash_byte),
        })
        .expect("test component registry entry should be valid")
    }

    #[test]
    fn durable_registry_rejects_non_v1_component_api() {
        let mut prototype = component_entry('a');
        prototype.api_version = "4.0.0".to_owned();
        let plugins = vec![prototype];
        let wire = PluginRegistryWire {
            version: PLUGIN_REGISTRY_FORMAT_VERSION,
            plugin_count: 1,
            generation: calculate_generation(&plugins).unwrap(),
            plugins,
        };

        let error =
            PluginRegistry::from_wire(wire).expect_err("durable non-v1 components must hard fail");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("lix:plugin"));
        assert!(error.message.contains("1.0.0"));
    }

    #[test]
    fn owned_component_upgrade_contract_allows_only_generation_packaging_changes() {
        let previous = component_entry('a');
        let replacement = component_entry('b');
        previous
            .validate_owned_upgrade_contract(&replacement)
            .expect("content-addressed component replacement should preserve the contract");

        let mut incompatible = Vec::new();
        let mut value = replacement.clone();
        value.api_version = "2.2.0".to_string();
        incompatible.push(value);
        let mut value = replacement.clone();
        value.path_glob = "*.tsv".to_string();
        incompatible.push(value);
        let mut value = replacement.clone();
        value.content = Some(PluginContentMatcher::Binary);
        incompatible.push(value);
        let mut value = replacement.clone();
        value.schema_keys = vec!["csv_table".to_string()];
        incompatible.push(value);
        let mut value = replacement;
        value.create_schema_keys.clear();
        incompatible.push(value);

        for replacement in incompatible {
            let error = previous
                .validate_owned_upgrade_contract(&replacement)
                .expect_err("owned plugin contract mutation must fail closed");
            assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        }
    }

    #[test]
    fn missing_registry_is_empty_without_discovery() {
        let registry = PluginRegistry::from_optional_value(None).expect("missing row is valid");
        assert!(registry.is_empty());
        assert!(registry.plugins().is_empty());
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
            canonical_json(&first.to_value().unwrap()),
            canonical_json(&second.to_value().unwrap())
        );
        assert_eq!(first.plugins()[0].key(), "plugin_a");

        let decoded = PluginRegistry::from_optional_snapshot(Some(&first.to_snapshot().unwrap()))
            .expect("canonical snapshot should decode");
        assert_eq!(decoded, first);
    }

    #[test]
    fn upsert_and_remove_change_generation_deterministically() {
        let empty = PluginRegistry::empty();
        let mut installed = empty.clone();
        installed
            .upsert(entry("plugin_a", "*.json", 'a'))
            .expect("install should be valid");
        assert_ne!(installed.generation(), empty.generation());
        assert_eq!(installed.plugin("plugin_a").unwrap().path_glob, "*.json");
        installed
            .remove("plugin_a")
            .expect("remove should be valid");
        assert_eq!(installed, empty);
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
    fn content_is_required_and_path_only_matching_is_explicit() {
        let path_only = PluginRegistry::new(vec![entry("plugin_a", "*.json", 'a')]).unwrap();
        let path_only_value = path_only.to_value().unwrap();
        assert_eq!(path_only_value["plugins"][0]["content"], JsonValue::Null);
        assert_eq!(
            PluginRegistry::from_optional_value(Some(&path_only_value)).unwrap(),
            path_only
        );

        let mut missing = path_only_value;
        missing["plugins"][0]
            .as_object_mut()
            .unwrap()
            .remove("content");
        assert_invalid(missing, "missing field `content`");

        let typed = PluginRegistry::new(vec![entry_with_content(
            "plugin_a",
            "*.json",
            Some(PluginContentMatcher::Text),
            'a',
        )])
        .unwrap();
        let typed_value = typed.to_value().unwrap();
        assert_eq!(typed_value["plugins"][0]["content"], json!("text"));
        assert_eq!(
            PluginRegistry::from_optional_value(Some(&typed_value)).unwrap(),
            typed
        );
        assert_ne!(path_only.generation(), typed.generation());

        let mut mismatched = typed_value;
        mismatched["plugins"][0]["content"] = json!("binary");
        assert_invalid(mismatched, "does not match manifest_json");
    }

    #[test]
    fn owner_rows_share_one_row_key_and_use_file_id_identity() {
        let owner = PluginFileOwner::new(
            "01920000-0000-7000-8000-0000000000a2",
            "plugin_a",
            vec!["plugin_a_note".to_string(), "plugin_a_meta".to_string()],
        )
        .unwrap();
        let row = owner.write_row("main", false).unwrap();
        assert_eq!(
            row.row_pk.unwrap().as_single_string().unwrap(),
            PLUGIN_OWNER_KEY
        );
        assert_eq!(
            row.file_id.as_deref(),
            Some("01920000-0000-7000-8000-0000000000a2")
        );
        assert!(!row.global);
        assert!(!row.untracked);
        assert_eq!(row.branch_id, "main");
        assert_eq!(row.snapshot.unwrap().value()["key"], PLUGIN_OWNER_KEY);
        assert_eq!(owner.schema_keys(), ["plugin_a_meta", "plugin_a_note"]);

        let registry_row = PluginRegistry::empty().write_row("main").unwrap();
        assert_eq!(registry_row.file_id, None);
        assert_eq!(
            registry_row.row_pk.unwrap().as_single_string().unwrap(),
            PLUGIN_REGISTRY_KEY
        );
    }

    #[test]
    fn installed_plugin_verifies_extracted_wasm_hash() {
        let wasm = b"compiled component".to_vec();
        let mut input = PluginRegistryEntryInput {
            key: "plugin_a".to_string(),
            runtime: PluginRuntime::WasmComponent,
            api_version: "1.0.0".to_string(),
            path_glob: "*.json".to_string(),
            content: Some(PluginContentMatcher::Text),
            entry: "plugin.wasm".to_string(),
            schema_keys: vec!["plugin_a_schema".to_string()],
            create_schema_keys: Vec::new(),
            manifest_json: manifest_with_content(
                "plugin_a",
                "*.json",
                Some(PluginContentMatcher::Text),
            ),
            archive_file_id: plugin_storage_archive_file_id("plugin_a"),
            archive_path: plugin_storage_archive_path("plugin_a"),
            archive_blob_hash: hash('a'),
            wasm_blob_hash: BlobId::from_content(&wasm).to_hex(),
        };
        let registry_entry = PluginRegistryEntry::new(input.clone()).unwrap();
        let installed = registry_entry
            .to_installed_plugin(wasm.clone())
            .expect("matching extracted WASM should materialize");
        assert_eq!(installed.key, "plugin_a");
        assert_eq!(installed.content, Some(PluginContentMatcher::Text));
        assert_eq!(installed.wasm_hash, BlobId::from_content(&wasm));
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
            catalog
                .select_for_bytes("src/data.json", b"")
                .unwrap()
                .key(),
            "plugin_specific"
        );
        assert_eq!(
            catalog.select_for_bytes("data.json", b"").unwrap().key(),
            "plugin_a"
        );
        assert_eq!(
            catalog.select_for_bytes("data.txt", b"").unwrap().key(),
            "plugin_all"
        );
        assert!(catalog.select_for_bytes("", b"").is_none());
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
    fn compiled_catalog_applies_content_only_when_known() {
        assert!(PluginContentMatcher::Text.matches_bytes(b""));
        assert!(PluginContentMatcher::Text.matches_bytes(b"hello"));
        assert!(!PluginContentMatcher::Text.matches_bytes(&[0xff, 0xfe]));
        assert!(PluginContentMatcher::Binary.matches_bytes(&[0xff, 0xfe]));
        let prefix_text = PluginContentMatcher::PrefixExcludes {
            byte: 0,
            bytes: 8_000,
        };
        assert!(prefix_text.matches_bytes(&[0xff, 0xfe]));
        assert!(!prefix_text.matches_bytes(b"text\0data"));
        let mut nul_outside_text_window = vec![b'x'; 8_000];
        nul_outside_text_window.push(0);
        assert!(prefix_text.matches_bytes(&nul_outside_text_window));

        let text = entry_with_content(
            "plugin_text",
            "*.data",
            Some(PluginContentMatcher::Text),
            'a',
        );
        let binary = entry_with_content(
            "plugin_binary",
            "*.data",
            Some(PluginContentMatcher::Binary),
            'b',
        );
        let text_only =
            CompiledPluginCatalog::compile(&PluginRegistry::new(vec![text.clone()]).unwrap())
                .unwrap();
        let classification_calls = Cell::new(0);
        assert!(
            text_only
                .select_with_content("document.other", |required| {
                    classification_calls.set(classification_calls.get() + 1);
                    Some(required == PluginContentMatcher::Text)
                })
                .is_none()
        );
        assert_eq!(classification_calls.get(), 0);
        assert_eq!(
            text_only
                .select_with_content("document.data", |required| {
                    Some(required == PluginContentMatcher::Text)
                })
                .map(PluginRegistryEntry::key),
            Some("plugin_text")
        );
        assert!(
            text_only
                .select_with_content("document.data", |required| {
                    Some(required == PluginContentMatcher::Binary)
                })
                .is_none()
        );

        let catalog =
            CompiledPluginCatalog::compile(&PluginRegistry::new(vec![text, binary]).unwrap())
                .unwrap();
        assert_eq!(
            catalog
                .select_with_content("document.data", |required| {
                    Some(required == PluginContentMatcher::Text)
                })
                .map(PluginRegistryEntry::key),
            Some("plugin_text")
        );
        assert_eq!(
            catalog
                .select_with_content("document.data", |required| {
                    Some(required == PluginContentMatcher::Binary)
                })
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
        let (selected, classified_bytes) =
            catalog.select_for_bytes_with_classification_work("document.data", b"hello");
        assert_eq!(selected.map(PluginRegistryEntry::key), Some("plugin_text"));
        assert_eq!(classified_bytes, 5);
        let (selected, classified_bytes) =
            catalog.select_for_bytes_with_classification_work("document.other", b"hello");
        assert!(selected.is_none());
        assert_eq!(classified_bytes, 0);
        assert!(catalog.matches_plugin("plugin_text", "document.data"));
        assert!(catalog.matches_plugin("plugin_binary", "document.data"));
    }

    #[test]
    fn prefix_exclusion_matcher_is_bounded() {
        let prefix_text = PluginContentMatcher::PrefixExcludes {
            byte: 0,
            bytes: 8_000,
        };
        let prefix_filtered =
            entry_with_content("plugin_prefix_filtered", "*", Some(prefix_text), 'a');
        let utf8_specific = entry_with_content(
            "plugin_utf8_specific",
            "*.data",
            Some(PluginContentMatcher::Text),
            'b',
        );
        let catalog = CompiledPluginCatalog::compile(
            &PluginRegistry::new(vec![prefix_filtered, utf8_specific]).unwrap(),
        )
        .unwrap();

        let large_non_utf8_text = std::iter::repeat_n(0xff, 1_048_576).collect::<Vec<_>>();
        let (selected, classified_bytes) =
            catalog.select_for_bytes_with_classification_work("asset.bin", &large_non_utf8_text);
        assert_eq!(
            selected.map(PluginRegistryEntry::key),
            Some("plugin_prefix_filtered")
        );
        assert_eq!(
            classified_bytes, 8_000,
            "prefix classification must inspect only its configured window"
        );
        assert!(
            classified_bytes * 100 < large_non_utf8_text.len() as u64,
            "the text matcher should inspect under 1% of this 1 MiB payload"
        );

        let (selected, _) =
            catalog.select_for_bytes_with_classification_work("document.data", b"valid UTF-8 text");
        assert_eq!(
            selected.map(PluginRegistryEntry::key),
            Some("plugin_utf8_specific"),
            "a more-specific UTF-8 parser must win when both predicates match"
        );

        let (selected, _) =
            catalog.select_for_bytes_with_classification_work("asset.bin", b"raw\0bytes");
        assert!(
            selected.is_none(),
            "NUL-bearing data must remain a raw binary file"
        );
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
