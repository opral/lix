#![allow(dead_code)]

use plugin_json_v2::{DetectedChange, File};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct SnapshotContent {
    path: String,
    value: Value,
}

pub fn file_from_json(json: &str) -> File {
    File {
        data: json.as_bytes().to_vec(),
    }
}

pub fn parse_snapshot_value_from_change(change: &DetectedChange) -> Value {
    let Some(snapshot_content) = change.snapshot_content.as_ref() else {
        panic!("change should have snapshot_content");
    };

    let parsed: SnapshotContent =
        serde_json::from_str(snapshot_content).expect("snapshot content should parse");
    assert_eq!(change.entity_pk, [parsed.path]);
    parsed.value
}

pub fn snapshot_content(path: &str, value: Value) -> String {
    serde_json::json!({
        "path": path,
        "value": value,
    })
    .to_string()
}
