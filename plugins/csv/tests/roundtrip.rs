use plugin_csv::exports::lix::plugin::api::{EntityState, Guest};
use plugin_csv::{
    CsvPlugin, DetectedChange, File, PluginError, ROOT_ENTITY_PK, ROW_SCHEMA_KEY, TABLE_SCHEMA_KEY,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use uuid::Uuid;

fn file_from_bytes(data: &[u8]) -> File {
    File {
        filename: None,
        data: data.to_vec(),
    }
}

fn csv_rows(prefix: &str, row_count: usize) -> Vec<u8> {
    let mut rows = String::new();
    for offset in 0..row_count {
        writeln!(&mut rows, "{prefix}{offset}").unwrap();
    }
    rows.into_bytes()
}

fn active_state_from_file(file: File) -> Vec<EntityState> {
    active_state_from_changes(
        CsvPlugin::detect_changes(Vec::new(), file).expect("detect_changes should succeed"),
    )
}

fn apply_changes_to_active_state(
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

fn active_state_from_changes(changes: Vec<DetectedChange>) -> Vec<EntityState> {
    apply_changes_to_active_state(Vec::new(), changes)
}

fn render_active_state(active_state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
    CsvPlugin::render(active_state)
}

fn render_changes(changes: Vec<DetectedChange>) -> Result<Vec<u8>, PluginError> {
    render_active_state(active_state_from_changes(changes))
}

fn snapshot_value(change: &DetectedChange) -> Value {
    let raw = change
        .snapshot_content
        .as_ref()
        .expect("snapshot_content should exist");
    serde_json::from_str(raw).expect("snapshot_content should parse")
}

fn active_state_snapshot_value(row: &EntityState) -> Value {
    serde_json::from_str(&row.snapshot_content).expect("snapshot_content should parse")
}

fn snapshot_order_key_from_value(value: &Value) -> String {
    let raw = value
        .get("order_key")
        .and_then(Value::as_str)
        .expect("row order_key should exist")
        .to_string();
    assert_order_key_is_valid(&raw);
    raw
}

fn snapshot_order_key(change: &DetectedChange) -> String {
    snapshot_order_key_from_value(&snapshot_value(change))
}

fn assert_generated_row_id_is_uuid_v7(change: &DetectedChange) {
    let [entity_pk] = change.entity_pk.as_slice() else {
        panic!("row entity_pk should have one component");
    };
    let value = snapshot_value(change);
    assert_eq!(
        value.get("id").and_then(Value::as_str),
        Some(entity_pk.as_str())
    );
    let uuid = Uuid::parse_str(entity_pk).expect("row entity_pk should parse as UUID");
    assert_eq!(uuid.get_version_num(), 7);
}

fn row_order_keys_by_first_cell(active_state: &[EntityState]) -> BTreeMap<String, String> {
    active_state
        .iter()
        .filter(|row| row.schema_key == ROW_SCHEMA_KEY)
        .map(|row| {
            let value = active_state_snapshot_value(row);
            let first_cell = value
                .get("cells")
                .and_then(Value::as_array)
                .and_then(|cells| cells.first())
                .and_then(Value::as_str)
                .expect("row first cell should exist")
                .to_string();
            (first_cell, snapshot_order_key_from_value(&value))
        })
        .collect()
}

fn row_ids_by_first_cell(active_state: &[EntityState]) -> BTreeMap<String, String> {
    active_state
        .iter()
        .filter(|row| row.schema_key == ROW_SCHEMA_KEY)
        .map(|row| {
            let value = active_state_snapshot_value(row);
            let first_cell = value
                .get("cells")
                .and_then(Value::as_array)
                .and_then(|cells| cells.first())
                .and_then(Value::as_str)
                .expect("row first cell should exist")
                .to_string();
            let [entity_pk] = row.entity_pk.as_slice() else {
                panic!("row entity_pk should have one component");
            };
            (first_cell, entity_pk.clone())
        })
        .collect()
}

fn csv_active_state_with_row_order_keys(rows: &[(&str, &str, &str)]) -> Vec<EntityState> {
    let mut state = vec![EntityState {
        entity_pk: vec![ROOT_ENTITY_PK.to_string()],
        schema_key: TABLE_SCHEMA_KEY.to_string(),
        snapshot_content: serde_json::json!({
            "id": ROOT_ENTITY_PK,
            "dialect": {
                "delimiter": ",",
                "quote": "\"",
                "terminator": "\n",
            },
        })
        .to_string(),
        metadata: None,
    }];

    state.extend(rows.iter().map(|(id, order_key, first_cell)| {
        EntityState {
            entity_pk: vec![(*id).to_string()],
            schema_key: ROW_SCHEMA_KEY.to_string(),
            snapshot_content: serde_json::json!({
                "id": id,
                "order_key": order_key,
                "cells": [first_cell],
            })
            .to_string(),
            metadata: None,
        }
    }));

    state
}

fn assert_order_key_is_valid(raw: &str) {
    assert!(!raw.is_empty(), "order_key should not be empty");
    assert!(
        raw.bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')),
        "order_key should contain only lowercase hexadecimal digits: {raw}"
    );
    assert_eq!(raw.len() % 2, 0, "order_key should have even length: {raw}");
    assert!(
        !raw.ends_with("00"),
        "order_key should not end with the minimum byte: {raw}"
    );
}

#[test]
fn detects_initial_projection_and_renders_csv() {
    let expected = b"name,age\nAda,37\n";
    let after = file_from_bytes(expected);

    let changes =
        CsvPlugin::detect_changes(Vec::new(), after).expect("detect_changes should succeed");

    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == ROW_SCHEMA_KEY)
            .count(),
        2
    );
    let table = changes
        .iter()
        .find(|change| change.schema_key == TABLE_SCHEMA_KEY)
        .expect("table snapshot should exist");
    assert_eq!(table.entity_pk, [ROOT_ENTITY_PK]);
    assert!(snapshot_value(table).get("row_ids").is_none());
    assert_eq!(
        snapshot_value(table)
            .get("dialect")
            .and_then(|value| value.get("delimiter"))
            .and_then(Value::as_str),
        Some(",")
    );
    assert_eq!(
        snapshot_value(table)
            .get("dialect")
            .and_then(|value| value.get("terminator"))
            .and_then(Value::as_str),
        Some("\n")
    );
    for row in changes
        .iter()
        .filter(|change| change.schema_key == ROW_SCHEMA_KEY)
    {
        assert_generated_row_id_is_uuid_v7(row);
        assert_order_key_is_valid(
            snapshot_value(row)
                .get("order_key")
                .and_then(Value::as_str)
                .expect("row order_key should exist"),
        );
    }

    let output = render_changes(changes).expect("render should succeed");
    assert_eq!(output, expected);
}

#[test]
fn detects_initial_csv_larger_than_fixed_width_order_key_limit() {
    let expected = csv_rows("row", 200);
    let after = file_from_bytes(&expected);

    let changes =
        CsvPlugin::detect_changes(Vec::new(), after).expect("detect_changes should succeed");

    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == ROW_SCHEMA_KEY)
            .count(),
        200
    );

    let output = render_changes(changes).expect("render should succeed");
    assert_eq!(output, expected);
}

#[test]
fn applies_delta_to_existing_csv() {
    let before_bytes = b"name,age\nAda,36\n";
    let after_bytes = b"name,age\nAda,37\nGrace,85\n";
    let before = file_from_bytes(before_bytes);
    let after = file_from_bytes(after_bytes);
    let before_state = active_state_from_file(before);

    let changes = CsvPlugin::detect_changes(before_state.clone(), after)
        .expect("detect_changes should succeed");
    let output = render_active_state(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed");

    assert_eq!(output, after_bytes);
}

#[test]
fn appends_csv_rows_larger_than_fixed_width_order_key_limit() {
    let before_bytes = csv_rows("row", 1);
    let after_bytes = csv_rows("row", 201);
    let before = file_from_bytes(&before_bytes);
    let after = file_from_bytes(&after_bytes);
    let before_state = active_state_from_file(before);

    let changes = CsvPlugin::detect_changes(before_state.clone(), after)
        .expect("detect_changes should succeed");

    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == ROW_SCHEMA_KEY)
            .count(),
        200
    );

    let output = render_active_state(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed");
    assert_eq!(output, after_bytes);
}

#[test]
fn applies_delta_to_existing_tsv() {
    let before_bytes = b"name\tage\nAda\t36\n";
    let after_bytes = b"name\tage\nAda\t37\nGrace\t85\n";
    let before = file_from_bytes(before_bytes);
    let after = file_from_bytes(after_bytes);
    let before_state = active_state_from_file(before);

    let changes = CsvPlugin::detect_changes(before_state.clone(), after)
        .expect("detect_changes should succeed");

    let output = render_active_state(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed");

    assert_eq!(output, after_bytes);
}

#[test]
fn detects_sorted_rows_as_order_key_changes() {
    let before_bytes = b"a\nb\nc\n";
    let after_bytes = b"c\nb\na\n";
    let before_state = active_state_from_file(file_from_bytes(before_bytes));
    let before_ids = row_ids_by_first_cell(&before_state);

    let changes = CsvPlugin::detect_changes(before_state.clone(), file_from_bytes(after_bytes))
        .expect("detect_changes should succeed");

    assert!(
        changes
            .iter()
            .filter(|change| change.schema_key == ROW_SCHEMA_KEY)
            .all(|change| change.snapshot_content.is_some()),
        "sorting rows should not delete existing row entities"
    );

    let active_state = apply_changes_to_active_state(before_state, changes);
    let after_ids = row_ids_by_first_cell(&active_state);
    let output = render_active_state(active_state).expect("render should succeed");

    assert_eq!(after_ids, before_ids);
    assert_eq!(output, after_bytes);
}

#[test]
fn inserted_rows_get_order_key_between_neighbors() {
    let before_bytes = b"a\nc\n";
    let after_bytes = b"a\nb\nc\n";
    let before = file_from_bytes(before_bytes);
    let after = file_from_bytes(after_bytes);
    let before_state = active_state_from_file(before);
    let order_keys_by_cell = row_order_keys_by_first_cell(&before_state);

    let changes = CsvPlugin::detect_changes(before_state.clone(), after)
        .expect("detect_changes should succeed");
    let inserted_order_key = {
        let row_changes = changes
            .iter()
            .filter(|change| change.schema_key == ROW_SCHEMA_KEY)
            .collect::<Vec<_>>();

        assert_eq!(row_changes.len(), 1);
        snapshot_order_key(row_changes[0])
    };
    let lower = order_keys_by_cell
        .get("a")
        .expect("before state should contain row a");
    let upper = order_keys_by_cell
        .get("c")
        .expect("before state should contain row c");
    assert!(inserted_order_key.as_str() > lower.as_str());
    assert!(inserted_order_key.as_str() < upper.as_str());

    let output = render_active_state(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed");

    assert_eq!(output, after_bytes);
}

#[test]
fn inserts_many_rows_inside_narrow_order_key_gap() {
    let before_state =
        csv_active_state_with_row_order_keys(&[("row:a", "80", "a"), ("row:z", "8001", "z")]);
    let mut after_csv = String::from("a\n");
    for offset in 0..256 {
        writeln!(&mut after_csv, "mid{offset}").unwrap();
    }
    after_csv.push_str("z\n");

    let changes =
        CsvPlugin::detect_changes(before_state.clone(), file_from_bytes(after_csv.as_bytes()))
            .expect("detect_changes should succeed");

    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == ROW_SCHEMA_KEY)
            .count(),
        256
    );

    let output = render_active_state(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed");
    assert_eq!(output, after_csv.as_bytes());
}

#[test]
fn repairs_duplicate_row_order_keys_when_inserting_between_them() {
    let before_state =
        csv_active_state_with_row_order_keys(&[("row:a", "80", "a"), ("row:c", "80", "c")]);
    let after_bytes = b"a\nb\nc\n";

    let changes = CsvPlugin::detect_changes(before_state.clone(), file_from_bytes(after_bytes))
        .expect("detect_changes should succeed");
    let active_state = apply_changes_to_active_state(before_state, changes);
    let order_keys_by_cell = row_order_keys_by_first_cell(&active_state);
    let unique_order_keys = order_keys_by_cell
        .values()
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(order_keys_by_cell.len(), 3);
    assert_eq!(unique_order_keys.len(), 3);

    let output = render_active_state(active_state).expect("render should succeed");
    assert_eq!(output, after_bytes);
}

#[test]
fn render_rebuilds_from_unordered_active_state() {
    let expected = b"name,age\nAda,37\nGrace,85\n";
    let after = file_from_bytes(expected);
    let mut active_state = active_state_from_file(after);
    active_state.reverse();

    let output = CsvPlugin::render(active_state).expect("render should succeed");

    assert_eq!(output, expected);
}

#[test]
fn render_uses_quote_byte_from_table_dialect() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["row:0".to_string()],
            schema_key: ROW_SCHEMA_KEY.to_string(),
            snapshot_content: Some(
                r#"{"id":"row:0","order_key":"80","cells":["a;b","plain"]}"#.to_string(),
            ),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec![ROOT_ENTITY_PK.to_string()],
            schema_key: TABLE_SCHEMA_KEY.to_string(),
            snapshot_content: Some(
                r#"{"id":"root","dialect":{"delimiter":";","quote":"'","terminator":"\n"}}"#
                    .to_string(),
            ),
            metadata: None,
        },
    ];

    let output = render_changes(changes).expect("render should succeed");

    assert_eq!(output, b"'a;b';plain\n");
}

#[test]
fn detects_and_renders_crlf_terminator() {
    let expected = b"name,age\r\nAda,37\r\n";
    let after = file_from_bytes(expected);

    let changes =
        CsvPlugin::detect_changes(Vec::new(), after).expect("detect_changes should succeed");
    let table = changes
        .iter()
        .find(|change| change.schema_key == TABLE_SCHEMA_KEY)
        .expect("table snapshot should exist");

    assert_eq!(
        snapshot_value(table)
            .get("dialect")
            .and_then(|value| value.get("terminator"))
            .and_then(Value::as_str),
        Some("\r\n")
    );

    let output = render_changes(changes).expect("render should succeed");

    assert_eq!(output, expected);
}

#[test]
fn rejects_row_snapshot_with_invalid_order_key() {
    let changes = vec![DetectedChange {
        entity_pk: vec!["row:0".to_string()],
        schema_key: ROW_SCHEMA_KEY.to_string(),
        snapshot_content: Some(r#"{"id":"row:0","order_key":"ba00","cells":["a"]}"#.to_string()),
        metadata: None,
    }];

    let error = render_changes(changes).expect_err("render should reject invalid projection");

    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("invalid csv row order_key"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}
