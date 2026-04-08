use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::contracts::surface::SurfaceRegistry;
use crate::contracts::traits::{PendingSemanticStorage, PendingView, SqlPreparationMetadataReader};
use crate::schema::builtin::builtin_schema_definition;
use crate::schema::{schema_from_registered_snapshot, SchemaKey};
use crate::sql::common::text::escape_sql_string;
use crate::{LixBackend, LixError, Value};
use serde_json::{json, Value as JsonValue};

const REGISTERED_SCHEMA_TABLE: &str = "lix_internal_registered_schema_bootstrap";
const GLOBAL_VERSION: &str = "global";
const LIVE_STATE_INTERNAL_SCHEMA_KEY: &str = "lix_state";
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
    load_sql_compiler_metadata_with_reader_and_pending_view(reader, registry, None).await
}

pub(crate) async fn load_sql_compiler_metadata_with_reader_and_pending_view(
    reader: &mut dyn SqlPreparationMetadataReader,
    registry: &SurfaceRegistry,
    pending_view: Option<&dyn PendingView>,
) -> Result<SqlCompilerMetadata, LixError> {
    let pending_schemas = collect_pending_latest_schema_entries(pending_view)?;
    let mut known_live_schema_definitions = BTreeMap::new();
    for schema_key in registry.registered_state_surface_schema_keys() {
        known_live_schema_definitions.insert(
            schema_key.clone(),
            load_latest_schema_for_preparation_with_pending(
                reader,
                &schema_key,
                pending_schemas.get(&schema_key),
            )
            .await?,
        );
    }

    Ok(SqlCompilerMetadata {
        known_live_schema_definitions,
        current_version_heads: reader.load_current_version_heads_for_preparation().await?,
    })
}

#[derive(Debug, Clone)]
struct PendingLatestSchemaEntry {
    key: SchemaKey,
    schema: JsonValue,
}

async fn load_latest_schema_for_preparation_with_pending(
    reader: &mut dyn SqlPreparationMetadataReader,
    schema_key: &str,
    pending_entry: Option<&PendingLatestSchemaEntry>,
) -> Result<JsonValue, LixError> {
    if schema_key == LIVE_STATE_INTERNAL_SCHEMA_KEY {
        return Ok(lix_state_internal_schema());
    }

    if let Some(schema) = builtin_schema_definition(schema_key) {
        return Ok(schema.clone());
    }

    let stored_entry = load_latest_schema_entry_for_preparation(reader, schema_key).await?;
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

async fn load_latest_schema_entry_for_preparation(
    reader: &mut dyn SqlPreparationMetadataReader,
    schema_key: &str,
) -> Result<Option<(SchemaKey, JsonValue)>, LixError> {
    let prefix = format!("{schema_key}~");
    let prefix_escaped = escape_sql_string(&prefix);
    let prefix_len = prefix.len();
    let sql = format!(
        "SELECT schema_version, snapshot_content \
         FROM {table} \
         WHERE substr(entity_id, 1, {prefix_len}) = '{prefix_escaped}' \
           AND version_id = '{global_version}' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL \
         ORDER BY CAST(schema_version AS INTEGER) DESC \
         LIMIT 1",
        table = REGISTERED_SCHEMA_TABLE,
        prefix_len = prefix_len,
        prefix_escaped = prefix_escaped,
        global_version = GLOBAL_VERSION,
    );
    let result = reader.execute_preparation_query(&sql, &[]).await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };

    let schema_version = required_text_cell(row, 0, "schema_version")?;
    let snapshot_content = required_text_cell(row, 1, "snapshot_content")?;
    Ok(Some((
        SchemaKey::new(schema_key.to_string(), schema_version),
        schema_from_registered_snapshot_content(&snapshot_content)?,
    )))
}

fn schema_from_registered_snapshot_content(raw: &str) -> Result<JsonValue, LixError> {
    let parsed: JsonValue = serde_json::from_str(raw).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("registered schema snapshot_content invalid JSON: {error}"),
        )
    })?;

    parsed.get("value").cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "registered schema snapshot_content missing value",
        )
    })
}

fn collect_pending_latest_schema_entries(
    pending_view: Option<&dyn PendingView>,
) -> Result<BTreeMap<String, PendingLatestSchemaEntry>, LixError> {
    let mut entries = BTreeMap::new();
    let Some(pending_view) = pending_view else {
        return Ok(entries);
    };

    for (_, snapshot_content) in pending_view.visible_registered_schema_entries() {
        let Some(snapshot_content) = snapshot_content.as_deref() else {
            continue;
        };
        remember_pending_schema_entry(&mut entries, snapshot_content)?;
    }

    for storage in [
        PendingSemanticStorage::Tracked,
        PendingSemanticStorage::Untracked,
    ] {
        for row in pending_view.visible_semantic_rows(storage, "lix_registered_schema") {
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

fn required_text_cell(row: &[Value], index: usize, name: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(text)) => Ok(text.clone()),
        Some(_) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("expected text value for {name}"),
        )),
        None => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("missing value for {name}"),
        )),
    }
}

fn lix_state_internal_schema() -> JsonValue {
    json!({
        "x-lix-key": "lix_state",
        "x-lix-version": "1",
        "x-lix-primary-key": [
            "/entity_id",
            "/schema_key",
            "/file_id"
        ],
        "type": "object",
        "properties": {
            "entity_id": { "type": "string" },
            "schema_key": { "type": "string" },
            "file_id": { "type": "string" }
        },
        "required": [
            "entity_id",
            "schema_key",
            "file_id"
        ],
        "additionalProperties": true
    })
}
