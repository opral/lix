use plugin_csv::{
    PluginActiveStateRow, PluginApiError, PluginDetectStateContext, PluginEntityChange, PluginFile,
    ROOT_ENTITY_PK, ROW_SCHEMA_KEY, TABLE_SCHEMA_KEY, apply_changes, detect_changes, render,
};
use serde_json::Value;

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
        schema_key: Some(change.schema_key),
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

fn snapshot_value(change: &PluginEntityChange) -> Value {
    let raw = change
        .snapshot_content
        .as_ref()
        .expect("snapshot_content should exist");
    serde_json::from_str(raw).expect("snapshot_content should parse")
}

fn snapshot_order_key(change: &PluginEntityChange) -> u128 {
    let raw = snapshot_value(change)
        .get("order_key")
        .and_then(Value::as_str)
        .expect("row order_key should exist")
        .to_string();
    u128::from_str_radix(&raw, 16).expect("row order_key should parse")
}

#[test]
fn detects_initial_projection_and_renders_csv() {
    let expected = b"name,age\nAda,37\n";
    let after = file_from_bytes("f1", "/people.csv", expected);

    let changes = detect_changes(None, after).expect("detect_changes should succeed");

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

    let output = apply_changes(file_from_bytes("f1", "/people.csv", b""), changes)
        .expect("apply_changes should render");
    assert_eq!(output, expected);
}

#[test]
fn applies_delta_to_existing_csv() {
    let before_bytes = b"name,age\nAda,36\n";
    let after_bytes = b"name,age\nAda,37\nGrace,85\n";
    let before = file_from_bytes("f1", "/people.csv", before_bytes);
    let after = file_from_bytes("f1", "/people.csv", after_bytes);

    let changes =
        detect_changes(Some(before.clone()), after).expect("detect_changes should succeed");
    let output = apply_changes(before, changes).expect("apply_changes should render");

    assert_eq!(output, after_bytes);
}

#[test]
fn applies_delta_to_existing_tsv() {
    let before_bytes = b"name\tage\nAda\t36\n";
    let after_bytes = b"name\tage\nAda\t37\nGrace\t85\n";
    let before = file_from_bytes("f1", "/people.tsv", before_bytes);
    let after = file_from_bytes("f1", "/people.tsv", after_bytes);

    let changes =
        detect_changes(Some(before.clone()), after).expect("detect_changes should succeed");

    let output = apply_changes(before, changes).expect("apply_changes should render");

    assert_eq!(output, after_bytes);
}

#[test]
fn inserted_rows_get_fractional_index_between_neighbors() {
    let before_bytes = b"a\nc\n";
    let after_bytes = b"a\nb\nc\n";
    let before = file_from_bytes("f1", "/letters.csv", before_bytes);
    let after = file_from_bytes("f1", "/letters.csv", after_bytes);

    let changes =
        detect_changes(Some(before.clone()), after).expect("detect_changes should succeed");
    let row_changes = changes
        .iter()
        .filter(|change| change.schema_key == ROW_SCHEMA_KEY)
        .collect::<Vec<_>>();

    assert_eq!(row_changes.len(), 1);
    let inserted_order_key = snapshot_order_key(row_changes[0]);
    let lower = u128::MAX / 3;
    let upper = lower * 2;
    assert!(inserted_order_key > lower);
    assert!(inserted_order_key < upper);

    let output = apply_changes(before, changes).expect("apply_changes should render");

    assert_eq!(output, after_bytes);
}

#[test]
fn render_rebuilds_from_unordered_active_state() {
    let expected = b"name,age\nAda,37\nGrace,85\n";
    let after = file_from_bytes("f1", "/people.csv", expected);
    let mut active_state = detect_changes(None, after)
        .expect("detect_changes should succeed")
        .into_iter()
        .map(active_state_row)
        .collect::<Vec<_>>();
    active_state.reverse();

    let output = render(PluginDetectStateContext { active_state }).expect("render should succeed");

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
                r#"{"id":"root","dialect":{"delimiter":";","quote":"'"}}"#.to_string(),
            ),
        },
    ];

    let output = apply_changes(file_from_bytes("f1", "/people.csv", b""), changes)
        .expect("apply_changes should render");

    assert_eq!(output, b"'a;b';plain\n");
}

#[test]
fn rejects_row_snapshot_with_invalid_order_key() {
    let changes = vec![PluginEntityChange {
        entity_pk: "row:0".to_string(),
        schema_key: ROW_SCHEMA_KEY.to_string(),
        snapshot_content: Some(r#"{"id":"row:0","order_key":"bad","cells":["a"]}"#.to_string()),
    }];

    let error = apply_changes(file_from_bytes("f1", "/people.csv", b""), changes)
        .expect_err("apply_changes should reject invalid projection");

    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("invalid csv row order_key"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}
