use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use jsonschema::JSONSchema;
use serde_json::Value as JsonValue;

use crate::sql::{MutationOperation, MutationRow, UpdateValidationPlan};
use crate::validate_lix_schema_definition;
use crate::{LixBackend, LixError, Value};

const STORED_SCHEMA_TABLE: &str = "lix_internal_state_materialized_v1_lix_stored_schema";
const STORED_SCHEMA_KEY: &str = "lix_stored_schema";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SchemaCacheKey {
    schema_key: String,
    schema_version: String,
}

#[derive(Debug, Default)]
pub struct SchemaCache {
    inner: RwLock<HashMap<SchemaCacheKey, Arc<JSONSchema>>>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

pub async fn validate_inserts(
    backend: &dyn LixBackend,
    cache: &SchemaCache,
    mutations: &[MutationRow],
) -> Result<(), LixError> {
    for row in mutations {
        if row.operation != MutationOperation::Insert {
            continue;
        }

        if row.schema_key == STORED_SCHEMA_KEY {
            validate_stored_schema_insert(backend, row).await?;
            continue;
        }

        let Some(snapshot) = row.snapshot_content.as_ref() else {
            continue;
        };

        validate_snapshot_content(
            backend,
            cache,
            &row.schema_key,
            &row.schema_version,
            snapshot,
        )
        .await?;
    }

    Ok(())
}

pub async fn validate_updates(
    backend: &dyn LixBackend,
    cache: &SchemaCache,
    plans: &[UpdateValidationPlan],
) -> Result<(), LixError> {
    let mut definition_cache: HashMap<SchemaCacheKey, JsonValue> = HashMap::new();

    for plan in plans {
        let mut sql = format!(
            "SELECT entity_id, file_id, version_id, plugin_key, schema_key, schema_version FROM {}",
            plan.table
        );
        if let Some(where_clause) = &plan.where_clause {
            sql.push_str(" WHERE ");
            sql.push_str(where_clause);
        }

        let result = backend.execute(&sql, &[]).await?;
        if result.rows.is_empty() {
            continue;
        }

        let snapshot = plan.snapshot_content.as_ref();

        for row in result.rows {
            let schema_key = value_to_string(&row[4], "schema_key")?;
            let schema_version = value_to_string(&row[5], "schema_version")?;

            if schema_key == STORED_SCHEMA_KEY {
                if let Some(snapshot) = snapshot {
                    validate_stored_schema_snapshot(backend, snapshot).await?;
                }
                continue;
            }

            let key = SchemaCacheKey {
                schema_key: schema_key.clone(),
                schema_version: schema_version.clone(),
            };
            let schema = if let Some(schema) = definition_cache.get(&key) {
                schema.clone()
            } else {
                let schema = load_schema_definition(backend, &schema_key, &schema_version).await?;
                definition_cache.insert(key.clone(), schema.clone());
                schema
            };

            if schema.get("x-lix-immutable").and_then(|v| v.as_bool()) == Some(true) {
                return Err(LixError {
                    message: format!(
                        "Schema '{}' is immutable and cannot be updated.",
                        schema_key
                    ),
                });
            }

            if let Some(snapshot) = snapshot {
                validate_snapshot_content(backend, cache, &schema_key, &schema_version, snapshot)
                    .await?;
            }
        }
    }

    Ok(())
}

async fn validate_snapshot_content(
    backend: &dyn LixBackend,
    cache: &SchemaCache,
    schema_key: &str,
    schema_version: &str,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let compiled = load_compiled_schema(backend, cache, schema_key, schema_version).await?;
    let details = match compiled.validate(snapshot) {
        Ok(()) => None,
        Err(errors) => {
            let mut parts = Vec::new();
            for error in errors {
                let path = error.instance_path.to_string();
                let message = error.to_string();
                if path.is_empty() {
                    parts.push(message);
                } else {
                    parts.push(format!("{path} {message}"));
                }
            }
            Some(parts.join("; "))
        }
    };

    if let Some(details) = details {
        return Err(LixError {
            message: format!(
                "snapshot_content does not match schema '{}' ({}): {details}",
                schema_key, schema_version
            ),
        });
    }

    Ok(())
}

fn extract_stored_schema_value(snapshot: &JsonValue) -> Result<&JsonValue, LixError> {
    snapshot.get("value").ok_or_else(|| LixError {
        message: "stored schema snapshot_content missing value".to_string(),
    })
}

async fn validate_stored_schema_snapshot(
    backend: &dyn LixBackend,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let schema_value = extract_stored_schema_value(snapshot)?;
    validate_lix_schema_definition(schema_value)?;
    validate_foreign_key_reference_targets(backend, schema_value).await?;
    Ok(())
}

async fn validate_stored_schema_insert(
    backend: &dyn LixBackend,
    row: &MutationRow,
) -> Result<(), LixError> {
    let snapshot = row.snapshot_content.as_ref().ok_or_else(|| LixError {
        message: "stored schema insert requires snapshot_content".to_string(),
    })?;
    validate_stored_schema_snapshot(backend, snapshot).await?;

    Ok(())
}

async fn validate_foreign_key_reference_targets(
    backend: &dyn LixBackend,
    schema: &JsonValue,
) -> Result<(), LixError> {
    let Some(foreign_keys) = schema.get("x-lix-foreign-keys").and_then(|v| v.as_array()) else {
        return Ok(());
    };

    for (index, foreign_key) in foreign_keys.iter().enumerate() {
        let references = foreign_key
            .get("references")
            .and_then(|v| v.as_object())
            .ok_or_else(|| LixError {
                message: format!(
                    "foreign key at index {index} missing references object in schema definition"
                ),
            })?;
        let referenced_key = references
            .get("schemaKey")
            .and_then(|v| v.as_str())
            .ok_or_else(|| LixError {
                message: format!(
                    "foreign key at index {index} references.schemaKey must be a string"
                ),
            })?;
        let referenced_properties = references
            .get("properties")
            .and_then(|v| v.as_array())
            .ok_or_else(|| LixError {
                message: format!(
                    "foreign key at index {index} references.properties must be an array"
                ),
            })?;

        let referenced_properties: Vec<String> = referenced_properties
            .iter()
            .filter_map(|value| value.as_str())
            .map(|value| value.to_string())
            .collect();

        let referenced_schema = load_latest_schema_definition(backend, referenced_key).await?;
        let allowed_keys = collect_unique_key_groups(&referenced_schema);
        if !allowed_keys
            .iter()
            .any(|group| group == &referenced_properties)
        {
            return Err(LixError {
                message: format!(
                    "foreign key at index {index} references properties that are not a primary key or unique key on schema '{}'",
                    referenced_key
                ),
            });
        }
    }

    Ok(())
}

fn collect_unique_key_groups(schema: &JsonValue) -> Vec<Vec<String>> {
    let mut keys = Vec::new();
    if let Some(primary) = schema
        .get("x-lix-primary-key")
        .and_then(|value| value.as_array())
    {
        let group: Vec<String> = primary
            .iter()
            .filter_map(|value| value.as_str())
            .map(|value| value.to_string())
            .collect();
        if !group.is_empty() {
            keys.push(group);
        }
    }
    if let Some(unique_groups) = schema
        .get("x-lix-unique")
        .and_then(|value| value.as_array())
    {
        for group in unique_groups {
            let Some(group_values) = group.as_array() else {
                continue;
            };
            let group_values: Vec<String> = group_values
                .iter()
                .filter_map(|value| value.as_str())
                .map(|value| value.to_string())
                .collect();
            if !group_values.is_empty() {
                keys.push(group_values);
            }
        }
    }
    keys
}

async fn load_compiled_schema(
    backend: &dyn LixBackend,
    cache: &SchemaCache,
    schema_key: &str,
    schema_version: &str,
) -> Result<Arc<JSONSchema>, LixError> {
    let key = SchemaCacheKey {
        schema_key: schema_key.to_string(),
        schema_version: schema_version.to_string(),
    };

    if let Some(existing) = cache.inner.read().unwrap().get(&key) {
        return Ok(existing.clone());
    }

    let schema = load_schema_definition(backend, schema_key, schema_version).await?;
    let compiled = JSONSchema::compile(&schema).map_err(|err| LixError {
        message: format!(
            "failed to compile schema '{}' ({}): {err}",
            schema_key, schema_version
        ),
    })?;
    let compiled = Arc::new(compiled);

    cache.inner.write().unwrap().insert(key, compiled.clone());

    Ok(compiled)
}

async fn load_schema_definition(
    backend: &dyn LixBackend,
    schema_key: &str,
    schema_version: &str,
) -> Result<JsonValue, LixError> {
    let entity_id = format!("{}~{}", schema_key, schema_version);
    let entity_id = escape_sql_string(&entity_id);

    let sql = format!(
        "SELECT snapshot_content FROM {table} \
         WHERE entity_id = '{entity_id}' \
           AND version_id = 'global' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL \
         LIMIT 1",
        table = STORED_SCHEMA_TABLE,
        entity_id = entity_id,
    );

    let result = backend.execute(&sql, &[]).await?;
    let row = result.rows.get(0).ok_or_else(|| LixError {
        message: format!("schema '{}' ({}) is not stored", schema_key, schema_version),
    })?;

    let raw = match row.get(0) {
        Some(Value::Text(text)) => text,
        _ => {
            return Err(LixError {
                message: "stored schema row missing snapshot_content".to_string(),
            })
        }
    };

    let parsed: JsonValue = serde_json::from_str(raw).map_err(|err| LixError {
        message: format!("stored schema snapshot_content invalid JSON: {err}"),
    })?;

    let schema = parsed.get("value").cloned().ok_or_else(|| LixError {
        message: "stored schema snapshot_content missing value".to_string(),
    })?;

    Ok(schema)
}

async fn load_latest_schema_definition(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<JsonValue, LixError> {
    let prefix = format!("{schema_key}~");
    let prefix_escaped = escape_sql_string(&prefix);
    let prefix_len = prefix.len();
    let sql = format!(
        "SELECT snapshot_content \
         FROM {table} \
         WHERE substr(entity_id, 1, {prefix_len}) = '{prefix_escaped}' \
           AND version_id = 'global' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL \
         ORDER BY CAST(schema_version AS INTEGER) DESC \
         LIMIT 1",
        table = STORED_SCHEMA_TABLE,
        prefix_len = prefix_len,
        prefix_escaped = prefix_escaped,
    );

    let result = backend.execute(&sql, &[]).await?;
    let row = result.rows.get(0).ok_or_else(|| LixError {
        message: format!("schema '{}' is not stored", schema_key),
    })?;

    let raw = match row.get(0) {
        Some(Value::Text(text)) => text,
        _ => {
            return Err(LixError {
                message: "stored schema row missing snapshot_content".to_string(),
            })
        }
    };

    let parsed: JsonValue = serde_json::from_str(raw).map_err(|err| LixError {
        message: format!("stored schema snapshot_content invalid JSON: {err}"),
    })?;
    let schema = parsed.get("value").cloned().ok_or_else(|| LixError {
        message: "stored schema snapshot_content missing value".to_string(),
    })?;

    Ok(schema)
}

fn escape_sql_string(input: &str) -> String {
    input.replace('\'', "''")
}

fn value_to_string(value: &Value, name: &str) -> Result<String, LixError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            message: format!("expected text value for {name}"),
        }),
    }
}
