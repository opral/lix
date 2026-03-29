use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::identity::{derive_entity_id_from_json_paths, EntityIdDerivationError};
use crate::schema::annotations::defaults::apply_schema_defaults;
use crate::sql::logical_plan::public_ir::{
    CanonicalStateAssignments, MutationPayload, PlannedStateRow,
};
use crate::sql::semantic_ir::semantics::effective_state_resolver::ExactEffectiveStateRow;
use crate::Value;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StateAssignmentsError {
    pub(crate) message: String,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EntityAssignmentsSemantics<'a> {
    pub(crate) property_columns: &'a [String],
    pub(crate) primary_key_paths: &'a [Vec<String>],
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EntityInsertSemantics<'a> {
    pub(crate) schema: &'a JsonValue,
    pub(crate) schema_key: &'a str,
    pub(crate) schema_version: &'a str,
    pub(crate) property_columns: &'a [String],
    pub(crate) primary_key_paths: &'a [Vec<String>],
    pub(crate) state_defaults: &'a BTreeMap<String, Value>,
}

pub(crate) fn assignments_from_payload(
    payload: &MutationPayload,
    context: &str,
) -> Result<CanonicalStateAssignments, StateAssignmentsError> {
    let MutationPayload::UpdatePatch(columns) = payload else {
        return Err(StateAssignmentsError {
            message: format!("{context} requires a patch payload"),
        });
    };
    Ok(CanonicalStateAssignments {
        columns: columns.clone(),
    })
}

pub(crate) fn apply_state_assignments(
    current_values: &BTreeMap<String, Value>,
    assignments: &CanonicalStateAssignments,
) -> BTreeMap<String, Value> {
    let mut values = current_values.clone();
    for (key, value) in &assignments.columns {
        values.insert(key.clone(), value.clone());
    }
    values
}

pub(crate) fn build_state_insert_row(
    entity_id: String,
    schema_key: String,
    version_id: Option<String>,
    values: BTreeMap<String, Value>,
    writer_key: Option<String>,
) -> PlannedStateRow {
    PlannedStateRow {
        entity_id,
        schema_key,
        version_id,
        values,
        writer_key,
        tombstone: false,
    }
}

pub(crate) fn build_entity_insert_rows_with_functions<P>(
    payloads: Vec<BTreeMap<String, Value>>,
    version_ids: Vec<Option<String>>,
    semantics: EntityInsertSemantics<'_>,
    functions: SharedFunctionProvider<P>,
) -> Result<Vec<PlannedStateRow>, StateAssignmentsError>
where
    P: LixFunctionProvider + Send + 'static,
{
    if payloads.len() != version_ids.len() {
        return Err(StateAssignmentsError {
            message: "public entity insert resolver requires one version target per payload row"
                .to_string(),
        });
    }

    let mut rows = Vec::with_capacity(payloads.len());
    for (payload, version_id) in payloads.into_iter().zip(version_ids.into_iter()) {
        let snapshot =
            snapshot_from_entity_payload_with_functions(&payload, semantics, functions.clone())?;
        let entity_id = if let Some(entity_id) = payload.get("entity_id").and_then(text_from_value)
        {
            entity_id.to_string()
        } else {
            derive_entity_id_from_snapshot(&snapshot, semantics.primary_key_paths)?
        };
        let mut values = BTreeMap::new();
        values.insert("entity_id".to_string(), Value::Text(entity_id.clone()));
        values.insert(
            "schema_key".to_string(),
            Value::Text(semantics.schema_key.to_string()),
        );
        values.insert(
            "file_id".to_string(),
            Value::Text(resolved_entity_state_text(&payload, semantics, "file_id")?),
        );
        values.insert(
            "plugin_key".to_string(),
            Value::Text(resolved_entity_state_text(
                &payload,
                semantics,
                "plugin_key",
            )?),
        );
        values.insert(
            "schema_version".to_string(),
            Value::Text(resolved_entity_state_text(
                &payload,
                semantics,
                "schema_version",
            )?),
        );
        values.insert(
            "snapshot_content".to_string(),
            Value::Text(
                serde_json::to_string(&JsonValue::Object(snapshot)).map_err(|error| {
                    StateAssignmentsError {
                        message: format!(
                            "public entity insert resolver could not serialize snapshot: {error}"
                        ),
                    }
                })?,
            ),
        );
        if let Some(version_id) = version_id.clone() {
            values.insert("version_id".to_string(), Value::Text(version_id));
        }
        if let Some(metadata) = resolved_entity_state_value(&payload, semantics, "metadata") {
            if metadata != Value::Null {
                values.insert("metadata".to_string(), metadata);
            }
        }
        for key in ["global", "untracked"] {
            if let Some(value) = resolved_entity_state_value(&payload, semantics, key) {
                if value != Value::Null {
                    values.insert(key.to_string(), value);
                }
            }
        }
        rows.push(build_state_insert_row(
            entity_id,
            semantics.schema_key.to_string(),
            version_id,
            values,
            None,
        ));
    }
    Ok(rows)
}

pub(crate) fn ensure_identity_columns_preserved(
    entity_id: &str,
    schema_key: &str,
    file_id: &str,
    version_id: &str,
    values: &BTreeMap<String, Value>,
) -> Result<(), StateAssignmentsError> {
    for (column, expected) in [
        ("entity_id", entity_id),
        ("schema_key", schema_key),
        ("file_id", file_id),
        ("version_id", version_id),
    ] {
        let Some(actual) = values.get(column).and_then(text_from_value) else {
            return Err(StateAssignmentsError {
                message: format!("public update resolver requires '{column}' in authoritative row"),
            });
        };
        if actual != expected {
            return Err(StateAssignmentsError {
                message: format!("public update resolver does not support changing '{column}'"),
            });
        }
    }

    Ok(())
}

pub(crate) fn apply_entity_state_assignments(
    current_row: &ExactEffectiveStateRow,
    assignments: &CanonicalStateAssignments,
    semantics: EntityAssignmentsSemantics<'_>,
) -> Result<BTreeMap<String, Value>, StateAssignmentsError> {
    let mut snapshot = parse_snapshot_object(&current_row.values)?;
    let mut values = current_row.values.clone();
    for (key, value) in &assignments.columns {
        if semantics
            .primary_key_paths
            .iter()
            .any(|path| path.len() == 1 && path[0] == *key)
        {
            return Err(StateAssignmentsError {
                message:
                    "public entity live slice does not yet support primary-key property updates"
                        .to_string(),
            });
        }
        if semantics
            .property_columns
            .iter()
            .any(|column| column == key)
        {
            snapshot.insert(key.clone(), engine_value_to_json_value(value)?);
            continue;
        }
        if apply_entity_state_column_assignment(&mut values, key, value)? {
            continue;
        }
        return Err(StateAssignmentsError {
            message: format!(
                "public entity live slice does not yet support updating state column '{}'",
                key
            ),
        });
    }

    let expected_entity_id = derive_entity_id_from_snapshot(&snapshot, semantics.primary_key_paths)
        .map_err(|_| StateAssignmentsError {
            message:
                "public entity update resolver requires a stable primary-key-derived entity_id"
                    .to_string(),
        })?;
    if expected_entity_id != current_row.entity_id {
        return Err(StateAssignmentsError {
            message:
                "public entity live slice does not yet support updates that change entity identity"
                    .to_string(),
        });
    }

    values.insert(
        "snapshot_content".to_string(),
        Value::Text(
            serde_json::to_string(&JsonValue::Object(snapshot)).map_err(|error| {
                StateAssignmentsError {
                    message: format!(
                        "public entity update resolver could not serialize snapshot: {error}"
                    ),
                }
            })?,
        ),
    );
    ensure_identity_columns_preserved(
        &current_row.entity_id,
        &current_row.schema_key,
        &current_row.file_id,
        &current_row.version_id,
        &values,
    )?;
    Ok(values)
}

pub(crate) fn derive_entity_id_from_snapshot(
    snapshot: &JsonMap<String, JsonValue>,
    primary_key_paths: &[Vec<String>],
) -> Result<String, StateAssignmentsError> {
    if primary_key_paths.is_empty() {
        return Err(StateAssignmentsError {
            message: "public entity resolver requires x-lix-primary-key for entity writes"
                .to_string(),
        });
    }

    let snapshot = JsonValue::Object(snapshot.clone());
    derive_entity_id_from_json_paths(&snapshot, primary_key_paths)
        .map(|entity_id| entity_id.into_inner())
        .map_err(|error| StateAssignmentsError {
            message: match error {
                EntityIdDerivationError::EmptyPrimaryKeyPath { .. } => {
                    "public entity resolver does not support empty primary-key pointers".to_string()
                }
                EntityIdDerivationError::MissingPrimaryKeyValue { .. } => {
                    "public entity resolver could not derive entity_id from the primary-key fields"
                        .to_string()
                }
                EntityIdDerivationError::NullPrimaryKeyValue { .. } => {
                    "public entity resolver cannot derive entity_id from null primary-key values"
                        .to_string()
                }
                EntityIdDerivationError::EmptyPrimaryKeyValue { .. } => {
                    "public entity resolver cannot derive entity_id from empty primary-key values"
                        .to_string()
                }
            },
        })
}

pub(crate) fn engine_value_to_json_value(
    value: &Value,
) -> Result<JsonValue, StateAssignmentsError> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Text(value) => Ok(JsonValue::String(value.clone())),
        Value::Json(value) => Ok(value.clone()),
        Value::Boolean(value) => Ok(JsonValue::Bool(*value)),
        Value::Integer(value) => Ok(JsonValue::Number((*value).into())),
        Value::Real(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .ok_or_else(|| StateAssignmentsError {
                message: "public entity resolver cannot represent NaN/inf JSON numbers".to_string(),
            }),
        Value::Blob(_) => Err(StateAssignmentsError {
            message: "public entity resolver does not support blob entity properties".to_string(),
        }),
    }
}

fn parse_snapshot_object(
    values: &BTreeMap<String, Value>,
) -> Result<JsonMap<String, JsonValue>, StateAssignmentsError> {
    let Some(snapshot_text) = values.get("snapshot_content").and_then(text_from_value) else {
        return Err(StateAssignmentsError {
            message: "public entity resolver requires snapshot_content in authoritative pre-state"
                .to_string(),
        });
    };
    let JsonValue::Object(object) =
        serde_json::from_str::<JsonValue>(snapshot_text).map_err(|error| {
            StateAssignmentsError {
                message: format!(
                    "public entity resolver could not parse snapshot_content JSON: {error}"
                ),
            }
        })?
    else {
        return Err(StateAssignmentsError {
            message: "public entity resolver requires object snapshot_content".to_string(),
        });
    };
    Ok(object)
}

fn snapshot_from_entity_payload_with_functions<P>(
    payload: &BTreeMap<String, Value>,
    semantics: EntityInsertSemantics<'_>,
    functions: SharedFunctionProvider<P>,
) -> Result<JsonMap<String, JsonValue>, StateAssignmentsError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let mut snapshot = JsonMap::new();
    for key in semantics.property_columns {
        if let Some(value) = payload.get(key) {
            snapshot.insert(key.clone(), engine_value_to_json_value(value)?);
        }
    }
    apply_schema_defaults(
        &mut snapshot,
        semantics.schema,
        crate::cel::shared_runtime(),
        functions,
        semantics.schema_key,
        semantics.schema_version,
    )
    .map_err(|error| StateAssignmentsError {
        message: error.description,
    })?;
    Ok(snapshot)
}

fn apply_entity_state_column_assignment(
    values: &mut BTreeMap<String, Value>,
    key: &str,
    value: &Value,
) -> Result<bool, StateAssignmentsError> {
    match key {
        "entity_id" | "schema_key" | "file_id" | "version_id" | "plugin_key" | "schema_version" => {
            let Some(text) = text_from_value(value) else {
                return Err(StateAssignmentsError {
                    message: format!("public entity resolver expected text {key}, got {value:?}"),
                });
            };
            values.insert(key.to_string(), Value::Text(text.to_string()));
            Ok(true)
        }
        "metadata" => match value {
            Value::Null => {
                values.remove(key);
                Ok(true)
            }
            Value::Text(text) => {
                values.insert(key.to_string(), Value::Text(text.clone()));
                Ok(true)
            }
            other => Err(StateAssignmentsError {
                message: format!("public entity resolver expected text/null {key}, got {other:?}"),
            }),
        },
        _ => Ok(false),
    }
}

fn resolved_entity_state_text(
    payload: &BTreeMap<String, Value>,
    semantics: EntityInsertSemantics<'_>,
    key: &str,
) -> Result<String, StateAssignmentsError> {
    resolved_entity_state_value(payload, semantics, key)
        .and_then(|value| text_from_value(&value).map(ToString::to_string))
        .ok_or_else(|| StateAssignmentsError {
            message: format!(
                "public entity resolver requires a concrete '{}' value or schema override",
                key
            ),
        })
}

fn resolved_entity_state_value(
    payload: &BTreeMap<String, Value>,
    semantics: EntityInsertSemantics<'_>,
    key: &str,
) -> Option<Value> {
    payload
        .get(key)
        .cloned()
        .or_else(|| semantics.state_defaults.get(key).cloned())
}

fn text_from_value(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) => Some(value.as_str()),
        _ => None,
    }
}
