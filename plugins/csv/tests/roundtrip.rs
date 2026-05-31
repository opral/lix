use plugin_csv::exports::lix::plugin::api::Guest as Plugin;
use plugin_csv::{
    CsvPlugin, PluginActiveStateRow, PluginApiError, PluginDetectStateContext, PluginEntityChange,
    PluginFile, ROOT_ENTITY_PK, ROW_SCHEMA_KEY, TABLE_SCHEMA_KEY,
};
use serde_json::Value;
use std::collections::BTreeMap;

fn file_from_bytes(id: &str, path: &str, data: &[u8]) -> PluginFile {
    PluginFile {
        id: id.to_string(),
        path: path.to_string(),
        data: data.to_vec(),
    }
}

fn active_state_row(change: PluginEntityChange) -> PluginActiveStateRow {
    PluginActiveStateRow {
        entity_pk: change.entity_pk,
        schema_key: change.schema_key,
        snapshot_content: change.snapshot_content,
        file_id: None,
        plugin_key: None,
        branch_id: None,
        change_id: None,
        metadata: None,
        created_at: None,
        updated_at: None,
    }
}

fn state_context(active_state: Vec<PluginActiveStateRow>) -> PluginDetectStateContext {
    PluginDetectStateContext { active_state }
}

fn empty_state_context() -> PluginDetectStateContext {
    state_context(Vec::new())
}

fn plugin_detect_changes(
    state: PluginDetectStateContext,
    file: PluginFile,
) -> Result<Vec<PluginEntityChange>, PluginApiError> {
    <CsvPlugin as Plugin>::detect_changes(state, file)
}

fn plugin_render(state: PluginDetectStateContext) -> Result<Vec<u8>, PluginApiError> {
    <CsvPlugin as Plugin>::render(state)
}

fn active_state_from_file(file: PluginFile) -> Vec<PluginActiveStateRow> {
    plugin_detect_changes(empty_state_context(), file)
        .expect("detect_changes should succeed")
        .into_iter()
        .map(active_state_row)
        .collect()
}

fn apply_changes_to_active_state(
    active_state: Vec<PluginActiveStateRow>,
    changes: Vec<PluginEntityChange>,
) -> Vec<PluginActiveStateRow> {
    let mut rows = active_state
        .into_iter()
        .map(|row| ((row.schema_key.clone(), row.entity_pk.clone()), row))
        .collect::<BTreeMap<_, _>>();

    for change in changes {
        let key = (change.schema_key.clone(), change.entity_pk.clone());
        if change.snapshot_content.is_some() {
            rows.insert(key, active_state_row(change));
        } else {
            rows.remove(&key);
        }
    }

    rows.into_values().collect()
}

fn render_active_state(active_state: Vec<PluginActiveStateRow>) -> Result<Vec<u8>, PluginApiError> {
    plugin_render(state_context(active_state))
}

fn render_changes(changes: Vec<PluginEntityChange>) -> Result<Vec<u8>, PluginApiError> {
    render_active_state(changes.into_iter().map(active_state_row).collect())
}

fn snapshot_value(change: &PluginEntityChange) -> Value {
    let raw = change
        .snapshot_content
        .as_ref()
        .expect("snapshot_content should exist");
    serde_json::from_str(raw).expect("snapshot_content should parse")
}

fn active_state_snapshot_value(row: &PluginActiveStateRow) -> Value {
    let raw = row
        .snapshot_content
        .as_ref()
        .expect("snapshot_content should exist");
    serde_json::from_str(raw).expect("snapshot_content should parse")
}

fn snapshot_order_key_from_value(value: &Value) -> u128 {
    let raw = value
        .get("order_key")
        .and_then(Value::as_str)
        .expect("row order_key should exist")
        .to_string();
    u128::from_str_radix(&raw, 16).expect("row order_key should parse")
}

fn snapshot_order_key(change: &PluginEntityChange) -> u128 {
    snapshot_order_key_from_value(&snapshot_value(change))
}

fn row_order_keys_by_first_cell(active_state: &[PluginActiveStateRow]) -> BTreeMap<String, u128> {
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

#[test]
fn detects_initial_projection_and_renders_csv() {
    let expected = b"name,age\nAda,37\n";
    let after = file_from_bytes("f1", "/people.csv", expected);

    let changes =
        plugin_detect_changes(empty_state_context(), after).expect("detect_changes should succeed");

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
    assert_eq!(table.entity_pk, ROOT_ENTITY_PK);
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
        assert_eq!(
            snapshot_value(row)
                .get("order_key")
                .and_then(Value::as_str)
                .expect("row order_key should exist")
                .len(),
            32
        );
    }

    let output = render_changes(changes).expect("render should succeed");
    assert_eq!(output, expected);
}

#[test]
fn applies_delta_to_existing_csv() {
    let before_bytes = b"name,age\nAda,36\n";
    let after_bytes = b"name,age\nAda,37\nGrace,85\n";
    let before = file_from_bytes("f1", "/people.csv", before_bytes);
    let after = file_from_bytes("f1", "/people.csv", after_bytes);
    let before_state = active_state_from_file(before);

    let changes = plugin_detect_changes(state_context(before_state.clone()), after)
        .expect("detect_changes should succeed");
    let output = render_active_state(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed");

    assert_eq!(output, after_bytes);
}

#[test]
fn applies_delta_to_existing_tsv() {
    let before_bytes = b"name\tage\nAda\t36\n";
    let after_bytes = b"name\tage\nAda\t37\nGrace\t85\n";
    let before = file_from_bytes("f1", "/people.tsv", before_bytes);
    let after = file_from_bytes("f1", "/people.tsv", after_bytes);
    let before_state = active_state_from_file(before);

    let changes = plugin_detect_changes(state_context(before_state.clone()), after)
        .expect("detect_changes should succeed");

    let output = render_active_state(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed");

    assert_eq!(output, after_bytes);
}

#[test]
fn inserted_rows_get_fractional_index_between_neighbors() {
    let before_bytes = b"a\nc\n";
    let after_bytes = b"a\nb\nc\n";
    let before = file_from_bytes("f1", "/letters.csv", before_bytes);
    let after = file_from_bytes("f1", "/letters.csv", after_bytes);
    let before_state = active_state_from_file(before);
    let order_keys_by_cell = row_order_keys_by_first_cell(&before_state);

    let changes = plugin_detect_changes(state_context(before_state.clone()), after)
        .expect("detect_changes should succeed");
    let inserted_order_key = {
        let row_changes = changes
            .iter()
            .filter(|change| change.schema_key == ROW_SCHEMA_KEY)
            .collect::<Vec<_>>();

        assert_eq!(row_changes.len(), 1);
        snapshot_order_key(row_changes[0])
    };
    let lower = *order_keys_by_cell
        .get("a")
        .expect("before state should contain row a");
    let upper = *order_keys_by_cell
        .get("c")
        .expect("before state should contain row c");
    assert!(inserted_order_key > lower);
    assert!(inserted_order_key < upper);

    let output = render_active_state(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed");

    assert_eq!(output, after_bytes);
}

#[test]
fn render_rebuilds_from_unordered_active_state() {
    let expected = b"name,age\nAda,37\nGrace,85\n";
    let after = file_from_bytes("f1", "/people.csv", expected);
    let mut active_state = plugin_detect_changes(empty_state_context(), after)
        .expect("detect_changes should succeed")
        .into_iter()
        .map(active_state_row)
        .collect::<Vec<_>>();
    active_state.reverse();

    let output = plugin_render(state_context(active_state)).expect("render should succeed");

    assert_eq!(output, expected);
}

#[test]
fn render_uses_quote_byte_from_table_dialect() {
    let changes = vec![
        PluginEntityChange {
            entity_pk: "row:0".to_string(),
            schema_key: ROW_SCHEMA_KEY.to_string(),
            snapshot_content: Some(
                r#"{"id":"row:0","order_key":"80000000000000000000000000000000","cells":["a;b","plain"]}"#
                    .to_string(),
            ),
        },
        PluginEntityChange {
            entity_pk: ROOT_ENTITY_PK.to_string(),
            schema_key: TABLE_SCHEMA_KEY.to_string(),
            snapshot_content: Some(
                r#"{"id":"root","dialect":{"delimiter":";","quote":"'","terminator":"\n"}}"#
                    .to_string(),
            ),
        },
    ];

    let output = render_changes(changes).expect("render should succeed");

    assert_eq!(output, b"'a;b';plain\n");
}

#[test]
fn detects_and_renders_crlf_terminator() {
    let expected = b"name,age\r\nAda,37\r\n";
    let after = file_from_bytes("f1", "/people.csv", expected);

    let changes =
        plugin_detect_changes(empty_state_context(), after).expect("detect_changes should succeed");
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
    let changes = vec![PluginEntityChange {
        entity_pk: "row:0".to_string(),
        schema_key: ROW_SCHEMA_KEY.to_string(),
        snapshot_content: Some(r#"{"id":"row:0","order_key":"bad","cells":["a"]}"#.to_string()),
    }];

    let error = render_changes(changes).expect_err("render should reject invalid projection");

    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("invalid csv row order_key"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}
