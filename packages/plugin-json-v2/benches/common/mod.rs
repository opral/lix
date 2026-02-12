#![allow(dead_code)]

use plugin_json_v2::{detect_changes, PluginEntityChange, PluginFile, SCHEMA_KEY};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

fn make_document(scale: usize, mutate: bool) -> Value {
    let mut root = Map::new();

    for i in 0..scale {
        if mutate && i % 11 == 0 {
            continue;
        }

        let mut entry = Map::new();
        let value = if mutate && i % 3 == 0 {
            (i as i64) * 2
        } else {
            i as i64
        };
        entry.insert("value".to_string(), Value::Number(value.into()));
        entry.insert("enabled".to_string(), Value::Bool(i % 2 == 0));

        let mut tags = Vec::new();
        tags.push(Value::String(format!("tag-{i}")));
        tags.push(Value::Number((i as i64 + 1).into()));
        if mutate && i % 5 == 0 {
            tags.push(Value::String("new".to_string()));
        }
        entry.insert("tags".to_string(), Value::Array(tags));

        root.insert(format!("item-{i}"), Value::Object(entry));
    }

    if mutate {
        let extra = scale / 10 + 1;
        for i in 0..extra {
            let mut entry = Map::new();
            entry.insert(
                "value".to_string(),
                Value::Number((10_000 + i as i64).into()),
            );
            entry.insert("enabled".to_string(), Value::Bool(true));
            entry.insert(
                "tags".to_string(),
                Value::Array(vec![Value::String("added".to_string())]),
            );
            root.insert(format!("added-{i}"), Value::Object(entry));
        }
    }

    root.insert(
        "meta".to_string(),
        serde_json::json!({
            "version": if mutate { 2 } else { 1 },
            "name": if mutate { "after" } else { "before" },
        }),
    );

    Value::Object(root)
}

pub fn dataset_small() -> (Vec<u8>, Vec<u8>) {
    let before = make_document(20, false);
    let after = make_document(20, true);
    (
        serde_json::to_vec(&before).expect("before JSON should serialize"),
        serde_json::to_vec(&after).expect("after JSON should serialize"),
    )
}

pub fn dataset_medium() -> (Vec<u8>, Vec<u8>) {
    let before = make_document(200, false);
    let after = make_document(200, true);
    (
        serde_json::to_vec(&before).expect("before JSON should serialize"),
        serde_json::to_vec(&after).expect("after JSON should serialize"),
    )
}

pub fn dataset_large() -> (Vec<u8>, Vec<u8>) {
    let before = make_document(1000, false);
    let after = make_document(1000, true);
    (
        serde_json::to_vec(&before).expect("before JSON should serialize"),
        serde_json::to_vec(&after).expect("after JSON should serialize"),
    )
}

pub fn file_from_bytes(id: &str, path: &str, data: &[u8]) -> PluginFile {
    PluginFile {
        id: id.to_string(),
        path: path.to_string(),
        data: data.to_vec(),
    }
}

pub fn merge_latest_state_rows(
    changesets: Vec<Vec<PluginEntityChange>>,
) -> Vec<PluginEntityChange> {
    let mut latest = BTreeMap::new();
    for changes in changesets {
        for change in changes {
            if change.schema_key != SCHEMA_KEY {
                continue;
            }
            latest.insert(
                (change.schema_key.clone(), change.entity_id.clone()),
                change,
            );
        }
    }
    latest.into_values().collect()
}

pub fn projection_for_transition(before: &[u8], after: &[u8]) -> Vec<PluginEntityChange> {
    let before_file = file_from_bytes("f1", "/x.json", before);
    let after_file = file_from_bytes("f1", "/x.json", after);
    let baseline =
        detect_changes(None, before_file.clone()).expect("baseline detect_changes should work");
    let delta =
        detect_changes(Some(before_file), after_file).expect("delta detect_changes should work");
    merge_latest_state_rows(vec![baseline, delta])
}
