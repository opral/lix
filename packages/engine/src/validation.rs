use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use jsonschema::JSONSchema;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::schema::{
    schema_from_stored_snapshot, validate_lix_schema_definition, OverlaySchemaProvider, SchemaKey,
    SchemaProvider, SqlStoredSchemaProvider,
};
use crate::sql::{bind_sql_with_state, MutationRow, PlaceholderState, UpdateValidationPlan};
use crate::{LixBackend, LixError, Value};

const STORED_SCHEMA_KEY: &str = "lix_stored_schema";

#[derive(Debug, Default)]
pub struct SchemaCache {
    inner: RwLock<HashMap<SchemaKey, Arc<JSONSchema>>>,
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
    let mut schema_provider = OverlaySchemaProvider::from_backend(backend);

    for row in mutations {
        if row.schema_key == STORED_SCHEMA_KEY {
            validate_stored_schema_insert(&mut schema_provider, row).await?;
            if let Some(snapshot) = row.snapshot_content.as_ref() {
                let (key, schema) = schema_from_stored_snapshot(snapshot)?;
                schema_provider.remember_pending_schema(key, schema);
            }
            continue;
        }

        let Some(snapshot) = row.snapshot_content.as_ref() else {
            continue;
        };

        let key = SchemaKey::new(row.schema_key.clone(), row.schema_version.clone());
        validate_snapshot_content(&mut schema_provider, cache, &key, snapshot).await?;
        validate_entity_id_matches_primary_key(
            &mut schema_provider,
            &key,
            &row.entity_id,
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
    params: &[Value],
    placeholder_state: PlaceholderState,
) -> Result<(), LixError> {
    let mut schema_provider = SqlStoredSchemaProvider::new(backend);

    for plan in plans {
        let mut sql = format!(
            "SELECT entity_id, file_id, version_id, plugin_key, schema_key, schema_version, snapshot_content FROM {}",
            plan.table
        );
        if let Some(where_clause) = &plan.where_clause {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clause.to_string());
        }

        let bound = bind_sql_with_state(&sql, params, backend.dialect(), placeholder_state)?;
        let result = backend.execute(&bound.sql, &bound.params).await?;
        if result.rows.is_empty() {
            continue;
        }

        for row in result.rows {
            let entity_id = value_to_string(&row[0], "entity_id")?;
            let schema_key = value_to_string(&row[4], "schema_key")?;
            let schema_version = value_to_string(&row[5], "schema_version")?;
            let snapshot = resolve_update_snapshot(plan, row.get(6), &schema_key)?;

            if schema_key == STORED_SCHEMA_KEY {
                if let Some(snapshot) = snapshot.as_ref() {
                    validate_stored_schema_snapshot(&mut schema_provider, snapshot).await?;
                }
                continue;
            }

            let key = SchemaKey::new(schema_key.clone(), schema_version.clone());
            let schema = schema_provider.load_schema(&key).await?;

            if schema.get("x-lix-immutable").and_then(|v| v.as_bool()) == Some(true) {
                return Err(LixError {
                    message: format!(
                        "Schema '{}' is immutable and cannot be updated.",
                        schema_key
                    ),
                });
            }

            if let Some(snapshot) = snapshot.as_ref() {
                validate_snapshot_content(&mut schema_provider, cache, &key, snapshot).await?;
                validate_entity_id_matches_primary_key(
                    &mut schema_provider,
                    &key,
                    &entity_id,
                    snapshot,
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn validate_snapshot_content<P: SchemaProvider + ?Sized>(
    provider: &mut P,
    cache: &SchemaCache,
    key: &SchemaKey,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let compiled = load_compiled_schema(provider, cache, key).await?;
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
                key.schema_key, key.schema_version
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

async fn validate_stored_schema_snapshot<P: SchemaProvider + ?Sized>(
    provider: &mut P,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let schema_value = extract_stored_schema_value(snapshot)?;
    validate_lix_schema_definition(schema_value)?;
    validate_foreign_key_reference_targets(provider, schema_value).await?;
    Ok(())
}

async fn validate_stored_schema_insert<P: SchemaProvider + ?Sized>(
    provider: &mut P,
    row: &MutationRow,
) -> Result<(), LixError> {
    let snapshot = row.snapshot_content.as_ref().ok_or_else(|| LixError {
        message: "stored schema insert requires snapshot_content".to_string(),
    })?;
    validate_stored_schema_snapshot(provider, snapshot).await?;

    Ok(())
}

async fn validate_foreign_key_reference_targets<P: SchemaProvider + ?Sized>(
    provider: &mut P,
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

        let referenced_schema = provider.load_latest_schema(referenced_key).await?;
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

async fn load_compiled_schema<P: SchemaProvider + ?Sized>(
    provider: &mut P,
    cache: &SchemaCache,
    key: &SchemaKey,
) -> Result<Arc<JSONSchema>, LixError> {
    if let Some(existing) = cache.inner.read().unwrap().get(key) {
        return Ok(existing.clone());
    }

    let schema = provider.load_schema(key).await?;
    let compiled = JSONSchema::compile(&schema).map_err(|err| LixError {
        message: format!(
            "failed to compile schema '{}' ({}): {err}",
            key.schema_key, key.schema_version
        ),
    })?;
    let compiled = Arc::new(compiled);

    cache
        .inner
        .write()
        .unwrap()
        .insert(key.clone(), compiled.clone());

    Ok(compiled)
}

fn value_to_string(value: &Value, name: &str) -> Result<String, LixError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            message: format!("expected text value for {name}"),
        }),
    }
}

fn resolve_update_snapshot(
    plan: &UpdateValidationPlan,
    row_snapshot_value: Option<&Value>,
    schema_key: &str,
) -> Result<Option<JsonValue>, LixError> {
    if let Some(snapshot) = plan.snapshot_content.as_ref() {
        return Ok(Some(snapshot.clone()));
    }
    let Some(patch) = plan.snapshot_patch.as_ref() else {
        return Ok(None);
    };
    let mut base = parse_row_snapshot_content(row_snapshot_value, schema_key)?;
    apply_snapshot_patch(&mut base, patch, schema_key)?;
    Ok(Some(base))
}

fn parse_row_snapshot_content(
    value: Option<&Value>,
    schema_key: &str,
) -> Result<JsonValue, LixError> {
    match value {
        None | Some(Value::Null) => Ok(JsonValue::Object(JsonMap::new())),
        Some(Value::Text(text)) => serde_json::from_str::<JsonValue>(text).map_err(|err| LixError {
            message: format!(
                "snapshot_content for schema '{}' is not valid JSON during update validation: {err}",
                schema_key
            ),
        }),
        Some(other) => Err(LixError {
            message: format!(
                "snapshot_content for schema '{}' must be text or null during update validation, got {other:?}",
                schema_key
            ),
        }),
    }
}

fn apply_snapshot_patch(
    snapshot: &mut JsonValue,
    patch: &BTreeMap<String, JsonValue>,
    schema_key: &str,
) -> Result<(), LixError> {
    let object = snapshot.as_object_mut().ok_or_else(|| LixError {
        message: format!(
            "snapshot_content for schema '{}' must be a JSON object for property update validation",
            schema_key
        ),
    })?;
    for (property, value) in patch {
        object.insert(property.clone(), value.clone());
    }
    Ok(())
}

async fn validate_entity_id_matches_primary_key<P: SchemaProvider + ?Sized>(
    provider: &mut P,
    key: &SchemaKey,
    entity_id: &str,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let schema = provider.load_schema(key).await?;
    let Some(primary_key) = schema
        .get("x-lix-primary-key")
        .and_then(JsonValue::as_array)
    else {
        return Ok(());
    };
    if primary_key.is_empty() {
        return Ok(());
    }

    let mut parts = Vec::with_capacity(primary_key.len());
    for pointer_value in primary_key {
        let pointer = pointer_value.as_str().ok_or_else(|| LixError {
            message: format!(
                "schema '{}' ({}) has non-string x-lix-primary-key entry",
                key.schema_key, key.schema_version
            ),
        })?;
        let pointer_path = parse_json_pointer(pointer)?;
        if pointer_path.is_empty() {
            return Err(LixError {
                message: format!(
                    "schema '{}' ({}) has invalid empty x-lix-primary-key pointer",
                    key.schema_key, key.schema_version
                ),
            });
        }

        let value = json_pointer_get(snapshot, &pointer_path).ok_or_else(|| LixError {
            message: format!(
                "entity_id '{}' is inconsistent for schema '{}' ({}): missing primary-key field at pointer '{}'",
                entity_id, key.schema_key, key.schema_version, pointer
            ),
        })?;
        parts.push(entity_id_component_from_json_value(value, pointer)?);
    }

    let expected = if parts.len() == 1 {
        parts.pop().unwrap()
    } else {
        parts.join("~")
    };

    if expected != entity_id {
        return Err(LixError {
            message: format!(
                "entity_id '{}' is inconsistent for schema '{}' ({}): expected '{}'",
                entity_id, key.schema_key, key.schema_version, expected
            ),
        });
    }

    Ok(())
}

fn entity_id_component_from_json_value(
    value: &JsonValue,
    pointer: &str,
) -> Result<String, LixError> {
    match value {
        JsonValue::Null => Err(LixError {
            message: format!(
                "cannot derive entity_id from null primary-key value at pointer '{}'",
                pointer
            ),
        }),
        JsonValue::String(text) => Ok(text.clone()),
        JsonValue::Bool(flag) => Ok(flag.to_string()),
        JsonValue::Number(number) => Ok(number.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => Ok(value.to_string()),
    }
}

fn parse_json_pointer(pointer: &str) -> Result<Vec<String>, LixError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(LixError {
            message: format!("invalid JSON pointer '{pointer}'"),
        });
    }
    pointer[1..]
        .split('/')
        .map(decode_json_pointer_segment)
        .collect()
}

fn decode_json_pointer_segment(segment: &str) -> Result<String, LixError> {
    let mut out = String::new();
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') => out.push('~'),
                Some('1') => out.push('/'),
                _ => {
                    return Err(LixError {
                        message: format!("invalid JSON pointer segment '{segment}'"),
                    })
                }
            }
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

fn json_pointer_get<'a>(value: &'a JsonValue, pointer: &[String]) -> Option<&'a JsonValue> {
    let mut current = value;
    for segment in pointer {
        match current {
            JsonValue::Object(object) => {
                current = object.get(segment)?;
            }
            JsonValue::Array(array) => {
                let index = segment.parse::<usize>().ok()?;
                current = array.get(index)?;
            }
            _ => return None,
        }
    }
    Some(current)
}
