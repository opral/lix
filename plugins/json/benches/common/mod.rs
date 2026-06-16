#![expect(dead_code)]
use plugin_json_v2::exports::lix::plugin::api::{EntityState, Guest};
use plugin_json_v2::{DetectedChange, File, JsonPlugin, SCHEMA_KEY};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[expect(clippy::cast_possible_wrap)]
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

pub fn file_from_bytes(data: &[u8]) -> File {
    File {
        filename: None,
        data: data.to_vec(),
    }
}

pub fn merge_latest_state_rows(changesets: Vec<Vec<DetectedChange>>) -> Vec<DetectedChange> {
    let mut latest = BTreeMap::new();
    for changes in changesets {
        for change in changes {
            if change.schema_key != SCHEMA_KEY {
                continue;
            }
            latest.insert(
                (change.schema_key.clone(), change.entity_pk.clone()),
                change,
            );
        }
    }
    latest.into_values().collect()
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

pub fn active_state_for_transition(before: &[u8], after: &[u8]) -> Vec<EntityState> {
    let before_file = file_from_bytes(before);
    let after_file = file_from_bytes(after);
    let baseline = JsonPlugin::detect_changes(Vec::new(), before_file)
        .expect("baseline detect_changes should work");
    let before_state = active_state_from_changes(baseline);
    let delta = JsonPlugin::detect_changes(before_state.clone(), after_file)
        .expect("delta detect_changes should work");
    apply_changes_to_active_state(before_state, delta)
}
