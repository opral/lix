#![allow(dead_code)]

use plugin_json_v2::{PluginEntityChange, PluginFile};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct SnapshotContent {
    path: String,
    value: Value,
}

pub fn file_from_json(id: &str, path: &str, json: &str) -> PluginFile {
    PluginFile {
        id: id.to_string(),
        path: path.to_string(),
        data: json.as_bytes().to_vec(),
    }
}

pub fn parse_snapshot_value_from_change(change: &PluginEntityChange) -> Value {
    let Some(snapshot_content) = change.snapshot_content.as_ref() else {
        panic!("change should have snapshot_content");
    };

    let parsed: SnapshotContent =
        serde_json::from_str(snapshot_content).expect("snapshot content should parse");
    assert_eq!(parsed.path, change.entity_id);
    parsed.value
}

pub fn snapshot_content(path: &str, value: Value) -> String {
    serde_json::json!({
        "path": path,
        "value": value,
    })
    .to_string()
}
