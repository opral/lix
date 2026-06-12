#![allow(dead_code)]

use plugin_json_v2::exports::lix::plugin::api::{EntityState, Guest};
use plugin_json_v2::{DetectedChange, File, JsonPlugin, PluginError};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
struct SnapshotContent {
    path: String,
    value: Value,
}

pub fn file_from_json(json: &str) -> File {
    File {
        filename: None,
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
