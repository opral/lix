#![allow(dead_code)]

use plugin_md_v2::{
    BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY, DetectedChange, File, PluginError, ROOT_ENTITY_PK,
};
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
    let raw = change
        .snapshot_content
        .as_ref()
        .expect("document snapshot should be present");
    let parsed: serde_json::Value =
        serde_json::from_str(raw).expect("document snapshot should be valid JSON");
    assert_eq!(
        parsed.get("id").and_then(serde_json::Value::as_str),
        Some(ROOT_ENTITY_PK)
    );
    parsed
        .get("order")
        .and_then(serde_json::Value::as_array)
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
    let raw = change
        .snapshot_content
        .as_ref()
        .expect("block snapshot should be present");
    let parsed: serde_json::Value =
        serde_json::from_str(raw).expect("block snapshot should be valid JSON");
    parsed
        .get("markdown")
        .and_then(serde_json::Value::as_str)
        .expect("block snapshot should contain markdown")
        .to_string()
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

pub fn document_change(order: Vec<String>) -> DetectedChange {
    DetectedChange {
        entity_pk: vec![ROOT_ENTITY_PK.to_string()],
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        snapshot_content: Some(
            serde_json::json!({
                "id": ROOT_ENTITY_PK,
                "order": order,
            })
            .to_string(),
        ),
        metadata: None,
    }
}

pub fn block_change(id: &str, node_type: &str, markdown: &str) -> DetectedChange {
    DetectedChange {
        entity_pk: vec![id.to_string()],
        schema_key: BLOCK_SCHEMA_KEY.to_string(),
        snapshot_content: Some(
            serde_json::json!({
                "id": id,
                "type": node_type,
                "node": {},
                "markdown": markdown,
            })
            .to_string(),
        ),
        metadata: None,
    }
}
