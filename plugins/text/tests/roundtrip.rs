use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use text_plugin::exports::lix::plugin::api::{EntityState, Guest};
use text_plugin::{
    DOCUMENT_SCHEMA_KEY, DetectedChange, File, LINE_SCHEMA_KEY, PluginError, ROOT_ENTITY_PK,
    TextPlugin,
};
use uuid::Uuid;

fn file_from_bytes(data: &[u8]) -> File {
    File {
        data: data.to_vec(),
    }
}

fn text_lines(prefix: &str, line_count: usize) -> Vec<u8> {
    let mut lines = String::new();
    for offset in 0..line_count {
        writeln!(&mut lines, "{prefix}{offset}").unwrap();
    }
    lines.into_bytes()
}

fn active_state_from_file(file: File) -> Vec<EntityState> {
    active_state_from_changes(
        TextPlugin::detect_changes(Vec::new(), file).expect("detect_changes should succeed"),
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
    TextPlugin::render(active_state)
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
        .expect("line order_key should exist")
        .to_string();
    assert_order_key_is_valid(&raw);
    raw
}

fn snapshot_order_key(change: &DetectedChange) -> String {
    snapshot_order_key_from_value(&snapshot_value(change))
}

fn snapshot_line(change: &DetectedChange) -> String {
    snapshot_value(change)
        .get("line")
        .and_then(Value::as_str)
        .expect("line content should exist")
        .to_string()
}

fn assert_generated_line_id_is_uuid_v7(change: &DetectedChange) {
    let [entity_pk] = change.entity_pk.as_slice() else {
        panic!("line entity_pk should have one component");
    };
    let value = snapshot_value(change);
    assert_eq!(
        value.get("id").and_then(Value::as_str),
        Some(entity_pk.as_str())
    );
    let uuid = Uuid::parse_str(entity_pk).expect("line entity_pk should parse as UUID");
    assert_eq!(uuid.get_version_num(), 7);
}

fn line_order_keys_by_content(active_state: &[EntityState]) -> BTreeMap<String, String> {
    active_state
        .iter()
        .filter(|row| row.schema_key == LINE_SCHEMA_KEY)
        .map(|row| {
            let value = active_state_snapshot_value(row);
            let line = value
                .get("line")
                .and_then(Value::as_str)
                .expect("line content should exist")
                .to_string();
            (line, snapshot_order_key_from_value(&value))
        })
        .collect()
}

fn line_ids_by_content(active_state: &[EntityState]) -> BTreeMap<String, String> {
    active_state
        .iter()
        .filter(|row| row.schema_key == LINE_SCHEMA_KEY)
        .map(|row| {
            let value = active_state_snapshot_value(row);
            let line = value
                .get("line")
                .and_then(Value::as_str)
                .expect("line content should exist")
                .to_string();
            let [entity_pk] = row.entity_pk.as_slice() else {
                panic!("line entity_pk should have one component");
            };
            (line, entity_pk.clone())
        })
        .collect()
}

fn text_active_state_with_line_order_keys(lines: &[(&str, &str, &str)]) -> Vec<EntityState> {
    let mut state = vec![EntityState {
        entity_pk: vec![ROOT_ENTITY_PK.to_string()],
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        snapshot_content: serde_json::json!({
            "id": ROOT_ENTITY_PK,
            "line_endings": "\n",
        })
        .to_string(),
        metadata: None,
    }];

    state.extend(lines.iter().map(|(id, order_key, line)| {
        EntityState {
            entity_pk: vec![(*id).to_string()],
            schema_key: LINE_SCHEMA_KEY.to_string(),
            snapshot_content: serde_json::json!({
                "id": id,
                "order_key": order_key,
                "line": line,
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
fn detects_initial_projection_and_renders_text() {
    let expected = b"name\nAda\n";
    let after = file_from_bytes(expected);

    let changes =
        TextPlugin::detect_changes(Vec::new(), after).expect("detect_changes should succeed");

    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
            .count(),
        3
    );
    let document = changes
        .iter()
        .find(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
        .expect("document snapshot should exist");
    assert_eq!(document.entity_pk, [ROOT_ENTITY_PK]);
    assert_eq!(
        snapshot_value(document)
            .get("line_endings")
            .and_then(Value::as_str),
        Some("\n")
    );
    let lines = changes
        .iter()
        .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
        .map(snapshot_line)
        .collect::<Vec<_>>();
    assert_eq!(lines, ["name", "Ada", ""]);
    for line in changes
        .iter()
        .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
    {
        assert_generated_line_id_is_uuid_v7(line);
        assert_order_key_is_valid(
            snapshot_value(line)
                .get("order_key")
                .and_then(Value::as_str)
                .expect("line order_key should exist"),
        );
    }

    let output = render_changes(changes).expect("render should succeed");
    assert_eq!(output, expected);
}

#[test]
fn detects_empty_initial_text_file_as_document() {
    let expected = b"";
    let after = file_from_bytes(expected);

    let changes =
        TextPlugin::detect_changes(Vec::new(), after).expect("detect_changes should succeed");

    assert_eq!(changes.len(), 1);
    let document = changes
        .iter()
        .find(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
        .expect("document snapshot should exist");
    assert_eq!(document.entity_pk, [ROOT_ENTITY_PK]);
    assert_eq!(
        snapshot_value(document)
            .get("line_endings")
            .and_then(Value::as_str),
        Some("\n")
    );
    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
            .count(),
        0
    );

    let output = render_changes(changes).expect("render should succeed");
    assert_eq!(output, expected);
}

#[test]
fn detects_initial_text_larger_than_fixed_width_order_key_limit() {
    let expected = text_lines("line", 200);
    let after = file_from_bytes(&expected);

    let changes =
        TextPlugin::detect_changes(Vec::new(), after).expect("detect_changes should succeed");

    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
            .count(),
        201
    );

    let output = render_changes(changes).expect("render should succeed");
    assert_eq!(output, expected);
}

#[test]
fn applies_delta_to_existing_text() {
    let before_bytes = b"name\nAda\n";
    let after_bytes = b"name\nAda Lovelace\nGrace\n";
    let before = file_from_bytes(before_bytes);
    let after = file_from_bytes(after_bytes);
    let before_state = active_state_from_file(before);

    let changes = TextPlugin::detect_changes(before_state.clone(), after)
        .expect("detect_changes should succeed");
    let output = render_active_state(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed");

    assert_eq!(output, after_bytes);
}

#[test]
fn appends_text_lines_larger_than_fixed_width_order_key_limit() {
    let before_bytes = text_lines("line", 1);
    let after_bytes = text_lines("line", 201);
    let before = file_from_bytes(&before_bytes);
    let after = file_from_bytes(&after_bytes);
    let before_state = active_state_from_file(before);

    let changes = TextPlugin::detect_changes(before_state.clone(), after)
        .expect("detect_changes should succeed");

    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
            .count(),
        200
    );

    let output = render_active_state(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed");
    assert_eq!(output, after_bytes);
}

#[test]
fn preserves_absence_of_final_line_ending() {
    let expected = b"alpha\nbeta";
    let after = file_from_bytes(expected);

    let changes =
        TextPlugin::detect_changes(Vec::new(), after).expect("detect_changes should succeed");
    let lines = changes
        .iter()
        .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
        .map(snapshot_line)
        .collect::<Vec<_>>();

    assert_eq!(lines, ["alpha", "beta"]);

    let output = render_changes(changes).expect("render should succeed");
    assert_eq!(output, expected);
}

#[test]
fn detects_and_renders_crlf_line_endings() {
    let expected = b"name\r\nAda\r\n";
    let after = file_from_bytes(expected);

    let changes =
        TextPlugin::detect_changes(Vec::new(), after).expect("detect_changes should succeed");
    let document = changes
        .iter()
        .find(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
        .expect("document snapshot should exist");

    assert_eq!(
        snapshot_value(document)
            .get("line_endings")
            .and_then(Value::as_str),
        Some("\r\n")
    );

    let output = render_changes(changes).expect("render should succeed");
    assert_eq!(output, expected);
}

#[test]
fn decodes_utf16le_bom_and_renders_utf8() {
    let after = file_from_bytes(&[
        0xff, 0xfe, // UTF-16LE BOM
        b'n', 0x00, b'a', 0x00, b'm', 0x00, b'e', 0x00, b'\n', 0x00,
    ]);

    let changes =
        TextPlugin::detect_changes(Vec::new(), after).expect("detect_changes should succeed");
    let lines = changes
        .iter()
        .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
        .map(snapshot_line)
        .collect::<Vec<_>>();

    assert_eq!(lines, ["name", ""]);

    let output = render_changes(changes).expect("render should succeed");
    assert_eq!(output, b"name\n");
}

#[test]
fn decodes_legacy_single_byte_text_and_renders_utf8() {
    let after = file_from_bytes(b"caf\xe9\n");

    let changes =
        TextPlugin::detect_changes(Vec::new(), after).expect("detect_changes should succeed");
    let line = changes
        .iter()
        .find(|change| change.schema_key == LINE_SCHEMA_KEY)
        .expect("line snapshot should exist");

    assert_eq!(
        snapshot_value(line).get("line").and_then(Value::as_str),
        Some("café")
    );

    let output = render_changes(changes).expect("render should succeed");
    assert_eq!(output, "café\n".as_bytes());
}

#[test]
fn silently_decodes_malformed_text() {
    let after = file_from_bytes(b"a\xff\n");

    let changes =
        TextPlugin::detect_changes(Vec::new(), after).expect("detect_changes should succeed");
    let line = changes
        .iter()
        .find(|change| change.schema_key == LINE_SCHEMA_KEY)
        .expect("line snapshot should exist");

    assert_eq!(
        snapshot_value(line).get("line").and_then(Value::as_str),
        Some("aÿ")
    );

    let output = render_changes(changes).expect("render should succeed");
    assert_eq!(output, "aÿ\n".as_bytes());
}

#[test]
fn detects_sorted_lines_as_order_key_changes() {
    let before_bytes = b"a\nb\nc\n";
    let after_bytes = b"c\nb\na\n";
    let before_state = active_state_from_file(file_from_bytes(before_bytes));
    let before_ids = line_ids_by_content(&before_state);

    let changes = TextPlugin::detect_changes(before_state.clone(), file_from_bytes(after_bytes))
        .expect("detect_changes should succeed");

    assert!(
        changes
            .iter()
            .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
            .all(|change| change.snapshot_content.is_some()),
        "sorting lines should not delete existing line entities"
    );

    let active_state = apply_changes_to_active_state(before_state, changes);
    let after_ids = line_ids_by_content(&active_state);
    let output = render_active_state(active_state).expect("render should succeed");

    assert_eq!(after_ids, before_ids);
    assert_eq!(output, after_bytes);
}

#[test]
fn inserted_lines_get_order_key_between_neighbors() {
    let before_bytes = b"a\nc\n";
    let after_bytes = b"a\nb\nc\n";
    let before = file_from_bytes(before_bytes);
    let after = file_from_bytes(after_bytes);
    let before_state = active_state_from_file(before);
    let order_keys_by_line = line_order_keys_by_content(&before_state);

    let changes = TextPlugin::detect_changes(before_state.clone(), after)
        .expect("detect_changes should succeed");
    let inserted_order_key = {
        let line_changes = changes
            .iter()
            .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
            .collect::<Vec<_>>();

        assert_eq!(line_changes.len(), 1);
        snapshot_order_key(line_changes[0])
    };
    let lower = order_keys_by_line
        .get("a")
        .expect("before state should contain line a");
    let upper = order_keys_by_line
        .get("c")
        .expect("before state should contain line c");
    assert!(inserted_order_key.as_str() > lower.as_str());
    assert!(inserted_order_key.as_str() < upper.as_str());

    let output = render_active_state(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed");

    assert_eq!(output, after_bytes);
}

#[test]
fn inserts_many_lines_inside_narrow_order_key_gap() {
    let before_state =
        text_active_state_with_line_order_keys(&[("line:a", "80", "a"), ("line:z", "8001", "z")]);
    let mut after_text = String::from("a\n");
    for offset in 0..256 {
        writeln!(&mut after_text, "mid{offset}").unwrap();
    }
    after_text.push('z');

    let changes =
        TextPlugin::detect_changes(before_state.clone(), file_from_bytes(after_text.as_bytes()))
            .expect("detect_changes should succeed");

    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
            .count(),
        256
    );

    let output = render_active_state(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed");
    assert_eq!(output, after_text.as_bytes());
}

#[test]
fn repairs_duplicate_line_order_keys_when_inserting_between_them() {
    let before_state =
        text_active_state_with_line_order_keys(&[("line:a", "80", "a"), ("line:c", "80", "c")]);
    let after_bytes = b"a\nb\nc";

    let changes = TextPlugin::detect_changes(before_state.clone(), file_from_bytes(after_bytes))
        .expect("detect_changes should succeed");
    let active_state = apply_changes_to_active_state(before_state, changes);
    let order_keys_by_line = line_order_keys_by_content(&active_state);
    let unique_order_keys = order_keys_by_line
        .values()
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(order_keys_by_line.len(), 3);
    assert_eq!(unique_order_keys.len(), 3);

    let output = render_active_state(active_state).expect("render should succeed");
    assert_eq!(output, after_bytes);
}

#[test]
fn render_rebuilds_from_unordered_active_state() {
    let expected = b"name\nAda\nGrace\n";
    let after = file_from_bytes(expected);
    let mut active_state = active_state_from_file(after);
    active_state.reverse();

    let output = TextPlugin::render(active_state).expect("render should succeed");

    assert_eq!(output, expected);
}

#[test]
fn render_uses_line_endings_from_document_snapshot() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["line:0".to_string()],
            schema_key: LINE_SCHEMA_KEY.to_string(),
            snapshot_content: Some(r#"{"id":"line:0","order_key":"80","line":"a"}"#.to_string()),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["line:1".to_string()],
            schema_key: LINE_SCHEMA_KEY.to_string(),
            snapshot_content: Some(r#"{"id":"line:1","order_key":"c0","line":"b"}"#.to_string()),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["line:2".to_string()],
            schema_key: LINE_SCHEMA_KEY.to_string(),
            snapshot_content: Some(r#"{"id":"line:2","order_key":"e0","line":""}"#.to_string()),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec![ROOT_ENTITY_PK.to_string()],
            schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
            snapshot_content: Some(r#"{"id":"root","line_endings":"\r\n"}"#.to_string()),
            metadata: None,
        },
    ];

    let output = render_changes(changes).expect("render should succeed");

    assert_eq!(output, b"a\r\nb\r\n");
}

#[test]
fn rejects_line_snapshot_with_invalid_order_key() {
    let changes = vec![DetectedChange {
        entity_pk: vec!["line:0".to_string()],
        schema_key: LINE_SCHEMA_KEY.to_string(),
        snapshot_content: Some(r#"{"id":"line:0","order_key":"ba00","line":"a"}"#.to_string()),
        metadata: None,
    }];

    let error = render_changes(changes).expect_err("render should reject invalid projection");

    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("invalid text line order_key"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}
