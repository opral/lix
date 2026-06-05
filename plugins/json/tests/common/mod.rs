#![allow(dead_code)]

use plugin_json_v2::exports::lix::plugin::api::{EntityState, Guest};
use plugin_json_v2::{DetectedChange, File, JsonPlugin, PluginError, Scalar};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
struct SnapshotContentJson {
    path: String,
    value: Value,
}

pub type SnapshotContent = BTreeMap<String, Scalar>;

pub fn file_from_json(json: &str) -> File {
    File {
        data: json.as_bytes().to_vec(),
    }
}

pub fn parse_snapshot_value_from_change(change: &DetectedChange) -> Value {
    let parsed: SnapshotContentJson = serde_json::from_value(snapshot_content_value(
        change
            .snapshot_content
            .as_ref()
            .expect("change should have snapshot_content"),
    ))
    .expect("snapshot content should parse");
    assert_eq!(change.entity_pk, [parsed.path]);
    parsed.value
}

pub fn snapshot_content(path: &str, value: Value) -> SnapshotContent {
    snapshot_content_from_value(serde_json::json!({
        "path": path,
        "value": value,
    }))
}

pub fn snapshot_content_from_value(value: Value) -> SnapshotContent {
    let Value::Object(object) = value else {
        panic!("snapshot_content must be a JSON object");
    };

    object
        .into_iter()
        .map(|(key, value)| (key, scalar_from_value(value)))
        .collect()
}

fn snapshot_content_value(snapshot_content: &SnapshotContent) -> Value {
    let object = snapshot_content
        .iter()
        .map(|(key, value)| (key.clone(), value_from_scalar(value)))
        .collect::<Map<_, _>>();
    Value::Object(object)
}

fn scalar_from_value(value: Value) -> Scalar {
    match value {
        Value::Null => Scalar::Nil,
        Value::Bool(value) => Scalar::Boolean(value),
        Value::String(value) => Scalar::Text(value),
        Value::Number(_) | Value::Array(_) | Value::Object(_) => {
            Scalar::Json(serde_json::to_string(&value).expect("snapshot scalar should encode"))
        }
    }
}

fn value_from_scalar(value: &Scalar) -> Value {
    match value {
        Scalar::Nil => Value::Null,
        Scalar::Boolean(value) => Value::Bool(*value),
        Scalar::Number(value) => {
            Value::Number(serde_json::Number::from_f64(*value).expect("finite JSON number"))
        }
        Scalar::Text(value) => Value::String(value.clone()),
        Scalar::Json(value) => serde_json::from_str(value).expect("JSON scalar should parse"),
    }
}

pub fn active_state_from_changes(changes: Vec<DetectedChange>) -> Vec<EntityState> {
    apply_changes_to_active_state(Vec::new(), changes)
}

pub fn apply_changes_to_active_state(
    active_state: Vec<EntityState>,
    changes: Vec<DetectedChange>,
) -> Vec<EntityState> {
    let mut rows = active_state
        .into_iter()
        .map(|row| ((row.schema_key.clone(), row.entity_pk.clone()), row))
        .collect::<BTreeMap<_, _>>();

    for change in changes {
        let key = (change.schema_key.clone(), change.entity_pk.clone());
        match change.snapshot_content {
            Some(snapshot_content) => {
                rows.insert(
                    key,
                    EntityState {
                        entity_pk: change.entity_pk,
                        schema_key: change.schema_key,
                        snapshot_content,
                        metadata: change.metadata,
                    },
                );
            }
            None => {
                rows.remove(&key);
            }
        }
    }

    rows.into_values().collect()
}

pub fn entity_state_rows_from_changes(changes: Vec<DetectedChange>) -> Vec<EntityState> {
    changes
        .into_iter()
        .filter_map(|change| {
            change.snapshot_content.map(|snapshot_content| EntityState {
                entity_pk: change.entity_pk,
                schema_key: change.schema_key,
                snapshot_content,
                metadata: change.metadata,
            })
        })
        .collect()
}

pub fn detect_changes_from_files(
    before: Option<File>,
    after: File,
) -> Result<Vec<DetectedChange>, PluginError> {
    let state = match before {
        Some(before) => active_state_from_changes(JsonPlugin::detect_changes(Vec::new(), before)?),
        None => Vec::new(),
    };
    JsonPlugin::detect_changes(state, after)
}

pub fn render_projection(changes: Vec<DetectedChange>) -> Result<Vec<u8>, PluginError> {
    JsonPlugin::render(entity_state_rows_from_changes(changes))
}
