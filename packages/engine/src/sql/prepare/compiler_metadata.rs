use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::catalog::SurfaceRegistry;
use crate::schema::{
    builtin_schema_definition, lix_state_surface_schema_definition,
    schema_from_registered_snapshot, SchemaKey,
};
use crate::sql::{
    SqlPreparationMetadataReader, SqlPreparationPendingOverlay, SqlPreparationPendingStorage,
};
use crate::{LixBackend, LixError};
use serde_json::Value as JsonValue;

const LIX_STATE_SURFACE_SCHEMA_KEY: &str = "lix_state";
#[derive(Debug, Clone, Default)]
pub(crate) struct SqlCompilerMetadata {
    pub(crate) known_live_schema_definitions: BTreeMap<String, JsonValue>,
    pub(crate) current_version_heads: Option<BTreeMap<String, String>>,
}

pub(crate) async fn load_sql_compiler_metadata(
    backend: &dyn LixBackend,
    registry: &SurfaceRegistry,
) -> Result<SqlCompilerMetadata, LixError> {
    let mut reader = backend;
    load_sql_compiler_metadata_with_reader(&mut reader, registry).await
}

pub(crate) async fn load_sql_compiler_metadata_with_reader(
    reader: &mut dyn SqlPreparationMetadataReader,
    registry: &SurfaceRegistry,
) -> Result<SqlCompilerMetadata, LixError> {
    load_sql_compiler_metadata_with_reader_and_pending_overlay(reader, registry, None).await
}

pub(crate) async fn load_sql_compiler_metadata_with_reader_and_pending_overlay(
    reader: &mut dyn SqlPreparationMetadataReader,
    registry: &SurfaceRegistry,
    pending_overlay: Option<&dyn SqlPreparationPendingOverlay>,
) -> Result<SqlCompilerMetadata, LixError> {
    let pending_schemas = collect_pending_latest_schema_entries(pending_overlay)?;
    let current_version_heads = reader.load_current_version_heads_for_preparation().await?;
    let mut known_live_schema_definitions = BTreeMap::new();
    for schema_key in registry.registered_state_surface_schema_keys() {
        known_live_schema_definitions.insert(
            schema_key.clone(),
            load_latest_schema_for_preparation_with_pending(
                reader,
                registry,
                &schema_key,
                current_version_heads.as_ref(),
                pending_schemas.get(&schema_key),
            )
            .await?,
        );
    }

    Ok(SqlCompilerMetadata {
        known_live_schema_definitions,
        current_version_heads,
    })
}

#[derive(Debug, Clone)]
struct PendingLatestSchemaEntry {
    key: SchemaKey,
    schema: JsonValue,
}

async fn load_latest_schema_for_preparation_with_pending(
    reader: &mut dyn SqlPreparationMetadataReader,
    registry: &SurfaceRegistry,
    schema_key: &str,
    current_version_heads: Option<&BTreeMap<String, String>>,
    pending_entry: Option<&PendingLatestSchemaEntry>,
) -> Result<JsonValue, LixError> {
    if schema_key == LIX_STATE_SURFACE_SCHEMA_KEY {
        return Ok(lix_state_surface_schema_definition().clone());
    }

    if let Some(schema) = builtin_schema_definition(schema_key) {
        return Ok(schema.clone());
    }

    if let Some(schema) = registry.dynamic_schema_definition(schema_key) {
        return Ok(schema.clone());
    }

    let stored_entry = reader
        .load_latest_registered_schema_entry_for_preparation(schema_key, current_version_heads)
        .await?;
    match (pending_entry, stored_entry) {
        (Some(pending), Some((stored_key, stored_schema))) => {
            if compare_schema_keys(&pending.key, &stored_key) != Ordering::Less {
                Ok(pending.schema.clone())
            } else {
                Ok(stored_schema)
            }
        }
        (Some(pending), None) => Ok(pending.schema.clone()),
        (None, Some((_, stored_schema))) => Ok(stored_schema),
        (None, None) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("schema '{schema_key}' is not stored"),
        )),
    }
}

fn collect_pending_latest_schema_entries(
    pending_overlay: Option<&dyn SqlPreparationPendingOverlay>,
) -> Result<BTreeMap<String, PendingLatestSchemaEntry>, LixError> {
    let mut entries = BTreeMap::new();
    let Some(pending_overlay) = pending_overlay else {
        return Ok(entries);
    };

    for (_, snapshot_content) in pending_overlay.visible_registered_schema_entries() {
        let Some(snapshot_content) = snapshot_content.as_deref() else {
            continue;
        };
        remember_pending_schema_entry(&mut entries, snapshot_content)?;
    }

    for storage in [
        SqlPreparationPendingStorage::Tracked,
        SqlPreparationPendingStorage::Untracked,
    ] {
        for row in pending_overlay.visible_registered_schema_rows(storage) {
            if row.tombstone {
                continue;
            }
            let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                continue;
            };
            remember_pending_schema_entry(&mut entries, snapshot_content)?;
        }
    }

    Ok(entries)
}

fn remember_pending_schema_entry(
    entries: &mut BTreeMap<String, PendingLatestSchemaEntry>,
    snapshot_content: &str,
) -> Result<(), LixError> {
    let snapshot: JsonValue = serde_json::from_str(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("registered schema snapshot_content invalid JSON: {error}"),
        )
    })?;
    let (key, schema) = schema_from_registered_snapshot(&snapshot)?;
    let schema_key = key.schema_key.clone();
    let replace = entries
        .get(&schema_key)
        .is_none_or(|current| compare_schema_keys(&key, &current.key) != Ordering::Less);
    if replace {
        entries.insert(schema_key, PendingLatestSchemaEntry { key, schema });
    }
    Ok(())
}

fn compare_schema_keys(left: &SchemaKey, right: &SchemaKey) -> Ordering {
    match (left.version_number(), right.version_number()) {
        (Some(left_version), Some(right_version)) => left_version.cmp(&right_version),
        _ => left.schema_version.cmp(&right.schema_version),
    }
}
