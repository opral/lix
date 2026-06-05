#![allow(dead_code)]

use plugin_md_v2::exports::lix::plugin::api::{EntityState, Guest};
use plugin_md_v2::{
    BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY, DetectedChange, File, MarkdownPlugin, PluginError,
    ROOT_ENTITY_PK, Scalar,
};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub type StateKey = (String, Vec<String>);
pub type StateRows = BTreeMap<StateKey, DetectedChange>;

pub fn file_from_markdown(markdown: &str) -> File {
    File {
        data: markdown.as_bytes().to_vec(),
    }
}

pub fn empty_file() -> File {
    File { data: Vec::new() }
}

pub fn decode_utf8(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).expect("materialized markdown should be valid UTF-8")
}

pub fn is_document_change(change: &DetectedChange) -> bool {
    change.schema_key == DOCUMENT_SCHEMA_KEY
}

pub fn is_block_change(change: &DetectedChange) -> bool {
    change.schema_key == BLOCK_SCHEMA_KEY
}

pub fn parse_document_order(change: &DetectedChange) -> Vec<String> {
    assert!(is_document_change(change));
    let parsed = snapshot_value(change);
    assert_eq!(
        parsed.get("id").and_then(Value::as_str),
        Some(ROOT_ENTITY_PK)
    );
    parsed
        .get("order")
        .and_then(Value::as_array)
        .expect("document snapshot should contain order array")
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .expect("order entries should be strings")
                .to_string()
        })
        .collect()
}

pub fn parse_block_markdown(change: &DetectedChange) -> String {
    assert!(is_block_change(change));
    let parsed = snapshot_value(change);
    parsed
        .get("markdown")
        .and_then(Value::as_str)
        .expect("block snapshot should contain markdown")
        .to_string()
}

pub fn snapshot_value(change: &DetectedChange) -> Value {
    let snapshot_content = change
        .snapshot_content
        .as_ref()
        .expect("snapshot should be present");
    let object = snapshot_content
        .iter()
        .map(|(key, value)| (key.clone(), value_from_scalar(value)))
        .collect::<Map<_, _>>();
    Value::Object(object)
}

pub fn assert_invalid_input(error: PluginError) {
    match error {
        PluginError::InvalidInput(_) => {}
        PluginError::Internal(message) => {
            panic!("expected invalid-input error, got internal error: {message}")
        }
    }
}

pub fn merge_delta(state: &mut StateRows, delta: Vec<DetectedChange>) {
    for change in delta {
        let key = (change.schema_key.clone(), change.entity_pk.clone());
        if change.snapshot_content.is_some() {
            state.insert(key, change);
        } else {
            state.remove(&key);
        }
    }
}

pub fn collect_state_rows(state: &StateRows) -> Vec<DetectedChange> {
    state.values().cloned().collect()
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
        Some(before) => {
            active_state_from_changes(MarkdownPlugin::detect_changes(Vec::new(), before)?)
        }
        None => Vec::new(),
    };
    MarkdownPlugin::detect_changes(state, after)
}

pub fn render_projection(changes: Vec<DetectedChange>) -> Result<Vec<u8>, PluginError> {
    MarkdownPlugin::render(entity_state_rows_from_changes(changes))
}

pub fn document_change(order: Vec<String>) -> DetectedChange {
    DetectedChange {
        entity_pk: vec![ROOT_ENTITY_PK.to_string()],
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content(serde_json::json!({
            "id": ROOT_ENTITY_PK,
            "order": order,
        }))),
        metadata: None,
    }
}

pub fn block_change(id: &str, node_type: &str, markdown: &str) -> DetectedChange {
    DetectedChange {
        entity_pk: vec![id.to_string()],
        schema_key: BLOCK_SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content(serde_json::json!({
            "id": id,
            "type": node_type,
            "node": {},
            "markdown": markdown,
        }))),
        metadata: None,
    }
}

pub fn snapshot_content(value: Value) -> BTreeMap<String, Scalar> {
    let Value::Object(object) = value else {
        panic!("snapshot_content must be a JSON object");
    };

    object
        .into_iter()
        .map(|(key, value)| (key, scalar_from_value(value)))
        .collect()
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
