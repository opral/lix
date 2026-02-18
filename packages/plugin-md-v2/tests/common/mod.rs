#![allow(dead_code)]

use plugin_md_v2::{
    PluginApiError, PluginEntityChange, PluginFile, BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY,
    ROOT_ENTITY_ID, SCHEMA_VERSION,
};
use std::collections::BTreeMap;

pub type StateKey = (String, String);
pub type StateRows = BTreeMap<StateKey, PluginEntityChange>;

pub fn file_from_markdown(id: &str, path: &str, markdown: &str) -> PluginFile {
    PluginFile {
        id: id.to_string(),
        path: path.to_string(),
        data: markdown.as_bytes().to_vec(),
    }
}

pub fn empty_file(id: &str, path: &str) -> PluginFile {
    PluginFile {
        id: id.to_string(),
        path: path.to_string(),
        data: Vec::new(),
    }
}

pub fn decode_utf8(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).expect("materialized markdown should be valid UTF-8")
}

pub fn is_document_change(change: &PluginEntityChange) -> bool {
    change.schema_key == DOCUMENT_SCHEMA_KEY
}

pub fn is_block_change(change: &PluginEntityChange) -> bool {
    change.schema_key == BLOCK_SCHEMA_KEY
}

pub fn parse_document_order(change: &PluginEntityChange) -> Vec<String> {
    assert!(is_document_change(change));
    let raw = change
        .snapshot_content
        .as_ref()
        .expect("document snapshot should be present");
    let parsed: serde_json::Value =
        serde_json::from_str(raw).expect("document snapshot should be valid JSON");
    assert_eq!(
        parsed.get("id").and_then(serde_json::Value::as_str),
        Some(ROOT_ENTITY_ID)
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

pub fn parse_block_markdown(change: &PluginEntityChange) -> String {
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

pub fn assert_invalid_input(error: PluginApiError) {
    match error {
        PluginApiError::InvalidInput(_) => {}
        PluginApiError::Internal(message) => {
            panic!("expected invalid-input error, got internal error: {message}")
        }
    }
}

pub fn apply_delta(state: &mut StateRows, delta: Vec<PluginEntityChange>) {
    for change in delta {
        let key = (change.schema_key.clone(), change.entity_id.clone());
        if change.snapshot_content.is_some() {
            state.insert(key, change);
        } else {
            state.remove(&key);
        }
    }
}

pub fn collect_state_rows(state: &StateRows) -> Vec<PluginEntityChange> {
    state.values().cloned().collect()
}

pub fn document_change(order: Vec<String>) -> PluginEntityChange {
    PluginEntityChange {
        entity_id: ROOT_ENTITY_ID.to_string(),
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(
            serde_json::json!({
                "id": ROOT_ENTITY_ID,
                "order": order,
            })
            .to_string(),
        ),
    }
}

pub fn block_change(id: &str, node_type: &str, markdown: &str) -> PluginEntityChange {
    PluginEntityChange {
        entity_id: id.to_string(),
        schema_key: BLOCK_SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(
            serde_json::json!({
                "id": id,
                "type": node_type,
                "node": {},
                "markdown": markdown,
            })
            .to_string(),
        ),
    }
}
