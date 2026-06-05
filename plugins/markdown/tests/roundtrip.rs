use plugin_md_v2::exports::lix::plugin::api::{EntityState, Guest};
use plugin_md_v2::{
    BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY, DetectedChange, File, MarkdownPlugin, PluginError,
    ROOT_ENTITY_PK,
};
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

fn file_from_bytes(data: &[u8]) -> File {
    File {
        data: data.to_vec(),
    }
}

fn active_state_from_file(file: File) -> Vec<EntityState> {
    active_state_from_changes(
        MarkdownPlugin::detect_changes(Vec::new(), file).expect("detect_changes should succeed"),
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
    MarkdownPlugin::render(active_state)
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
        .expect("block order_key should exist")
        .to_string();
    assert_order_key_is_valid(&raw);
    raw
}

fn snapshot_order_key(change: &DetectedChange) -> String {
    snapshot_order_key_from_value(&snapshot_value(change))
}

fn snapshot_block(change: &DetectedChange) -> String {
    snapshot_value(change)
        .get("block")
        .and_then(Value::as_str)
        .expect("block content should exist")
        .to_string()
}

fn assert_generated_block_id_is_uuid_v7(change: &DetectedChange) {
    let [entity_pk] = change.entity_pk.as_slice() else {
        panic!("block entity_pk should have one component");
    };
    let value = snapshot_value(change);
    assert_eq!(
        value.get("id").and_then(Value::as_str),
        Some(entity_pk.as_str())
    );
    let uuid = Uuid::parse_str(entity_pk).expect("block entity_pk should parse as UUID");
    assert_eq!(uuid.get_version_num(), 7);
}

fn block_order_keys_by_content(active_state: &[EntityState]) -> BTreeMap<String, String> {
    active_state
        .iter()
        .filter(|row| row.schema_key == BLOCK_SCHEMA_KEY)
        .map(|row| {
            let value = active_state_snapshot_value(row);
            let block = value
                .get("block")
                .and_then(Value::as_str)
                .expect("block content should exist")
                .to_string();
            (block, snapshot_order_key_from_value(&value))
        })
        .collect()
}

fn markdown_active_state_with_block_order_keys(blocks: &[(&str, &str, &str)]) -> Vec<EntityState> {
    let mut state = vec![EntityState {
        entity_pk: vec![ROOT_ENTITY_PK.to_string()],
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        snapshot_content: serde_json::json!({
            "id": ROOT_ENTITY_PK,
        })
        .to_string(),
        metadata: None,
    }];

    state.extend(blocks.iter().map(|(id, order_key, block)| {
        EntityState {
            entity_pk: vec![(*id).to_string()],
            schema_key: BLOCK_SCHEMA_KEY.to_string(),
            snapshot_content: serde_json::json!({
                "id": id,
                "order_key": order_key,
                "block": block,
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
fn detects_initial_projection_and_renders_normalized_markdown() {
    let after = file_from_bytes(b"# Title\r\n\r\nHello **Ada**\r\n\r\n- one\r\n- two\r\n");

    let changes =
        MarkdownPlugin::detect_changes(Vec::new(), after).expect("detect_changes should succeed");

    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY)
            .count(),
        3
    );
    let document = changes
        .iter()
        .find(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
        .expect("document snapshot should exist");
    assert_eq!(document.entity_pk, [ROOT_ENTITY_PK]);
    assert_eq!(snapshot_value(document), serde_json::json!({"id": "root"}));

    let blocks = changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY)
        .map(snapshot_block)
        .collect::<Vec<_>>();
    assert_eq!(blocks, ["# Title", "Hello **Ada**", "- one\n- two"]);
    for block in changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY)
    {
        assert_generated_block_id_is_uuid_v7(block);
        assert_order_key_is_valid(
            snapshot_value(block)
                .get("order_key")
                .and_then(Value::as_str)
                .expect("block order_key should exist"),
        );
    }

    let output = render_changes(changes).expect("render should succeed");
    assert_eq!(output, b"# Title\n\nHello **Ada**\n\n- one\n- two\n");
}

#[test]
fn detects_empty_initial_markdown_file_as_document() {
    let changes = MarkdownPlugin::detect_changes(Vec::new(), file_from_bytes(b""))
        .expect("detect_changes should succeed");

    assert_eq!(changes.len(), 1);
    let document = changes
        .iter()
        .find(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
        .expect("document snapshot should exist");
    assert_eq!(document.entity_pk, [ROOT_ENTITY_PK]);
    assert_eq!(snapshot_value(document), serde_json::json!({"id": "root"}));
    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY)
            .count(),
        0
    );

    let output = render_changes(changes).expect("render should succeed");
    assert_eq!(output, b"\n");
}

#[test]
fn applies_delta_to_existing_markdown() {
    let before_bytes = b"# Title\n\nHello\n";
    let after_bytes = b"# Title\n\nHello **Ada**\n\n- one\n";
    let before_state = active_state_from_file(file_from_bytes(before_bytes));

    let changes =
        MarkdownPlugin::detect_changes(before_state.clone(), file_from_bytes(after_bytes))
            .expect("detect_changes should succeed");
    let output = render_active_state(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed");

    assert_eq!(output, b"# Title\n\nHello **Ada**\n\n- one\n");
}

#[test]
fn normalizes_adjacent_blocks_to_blank_line_separators() {
    let output = render_changes(vec![
        DetectedChange {
            entity_pk: vec![ROOT_ENTITY_PK.to_string()],
            schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
            snapshot_content: Some(r#"{"id":"root"}"#.to_string()),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["block:0".to_string()],
            schema_key: BLOCK_SCHEMA_KEY.to_string(),
            snapshot_content: Some(
                r##"{"id":"block:0","order_key":"80","block":"# Title"}"##.to_string(),
            ),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["block:1".to_string()],
            schema_key: BLOCK_SCHEMA_KEY.to_string(),
            snapshot_content: Some(
                r#"{"id":"block:1","order_key":"c0","block":"paragraph"}"#.to_string(),
            ),
            metadata: None,
        },
    ])
    .expect("render should succeed");

    assert_eq!(output, b"# Title\n\nparagraph\n");
}

#[test]
fn inserted_blocks_get_order_key_between_neighbors() {
    let before_state = active_state_from_file(file_from_bytes(b"# A\n\n# C"));
    let order_keys_by_block = block_order_keys_by_content(&before_state);

    let changes =
        MarkdownPlugin::detect_changes(before_state.clone(), file_from_bytes(b"# A\n\n# B\n\n# C"))
            .expect("detect_changes should succeed");
    let inserted_order_key = {
        let block_changes = changes
            .iter()
            .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY)
            .collect::<Vec<_>>();

        assert_eq!(block_changes.len(), 1);
        snapshot_order_key(block_changes[0])
    };
    let lower = order_keys_by_block
        .get("# A")
        .expect("before state should contain block A");
    let upper = order_keys_by_block
        .get("# C")
        .expect("before state should contain block C");
    assert!(inserted_order_key.as_str() > lower.as_str());
    assert!(inserted_order_key.as_str() < upper.as_str());

    let output = render_active_state(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed");

    assert_eq!(output, b"# A\n\n# B\n\n# C\n");
}

#[test]
fn repairs_duplicate_block_order_keys_when_inserting_between_them() {
    let before_state = markdown_active_state_with_block_order_keys(&[
        ("block:a", "80", "# A"),
        ("block:c", "80", "# C"),
    ]);

    let changes =
        MarkdownPlugin::detect_changes(before_state.clone(), file_from_bytes(b"# A\n\n# B\n\n# C"))
            .expect("detect_changes should succeed");
    let active_state = apply_changes_to_active_state(before_state, changes);
    let order_keys_by_block = block_order_keys_by_content(&active_state);
    let unique_order_keys = order_keys_by_block
        .values()
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(order_keys_by_block.len(), 3);
    assert_eq!(unique_order_keys.len(), 3);

    let output = render_active_state(active_state).expect("render should succeed");
    assert_eq!(output, b"# A\n\n# B\n\n# C\n");
}

#[test]
fn render_rebuilds_from_unordered_active_state() {
    let mut active_state = active_state_from_file(file_from_bytes(b"# A\n\n# B\n\n# C"));
    active_state.reverse();

    let output = MarkdownPlugin::render(active_state).expect("render should succeed");

    assert_eq!(output, b"# A\n\n# B\n\n# C\n");
}

#[test]
fn rejects_block_snapshot_with_invalid_order_key() {
    let changes = vec![DetectedChange {
        entity_pk: vec!["block:0".to_string()],
        schema_key: BLOCK_SCHEMA_KEY.to_string(),
        snapshot_content: Some(
            r##"{"id":"block:0","order_key":"ba00","block":"# A"}"##.to_string(),
        ),
        metadata: None,
    }];

    let error = render_changes(changes).expect_err("render should reject invalid projection");

    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("invalid markdown block order_key"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn roundtrip_file_detect_state_render_markdown() {
    let markdown = "# Title\n\nParagraph one.\n\nParagraph two.\n";
    let state = active_state_from_file(file_from_bytes(markdown.as_bytes()));

    let output = render_active_state(state).expect("render should succeed");

    assert_eq!(output, b"# Title\n\nParagraph one.\n\nParagraph two.\n");
}

#[test]
fn roundtrip_edit_move_delete_across_block_rows() {
    let before = b"Alpha.\n\nBravo.\n\nCharlie.\n";
    let after = b"Charlie.\n\nAlpha updated.\n";
    let before_state = active_state_from_file(file_from_bytes(before));

    let delta = MarkdownPlugin::detect_changes(before_state.clone(), file_from_bytes(after))
        .expect("detect_changes should succeed");
    let output = render_active_state(apply_changes_to_active_state(before_state, delta))
        .expect("render should succeed");

    assert_eq!(output, b"Charlie.\n\nAlpha updated.\n");
}

#[test]
fn roundtrip_multi_step_evolution() {
    let a = b"# Title\n\nOne.\n";
    let b = b"# Title v2\n\nOne.\n\nTwo.\n";
    let c = b"Two.\n\n# Title v3\n";
    let mut state = active_state_from_file(file_from_bytes(a));

    let delta_b = MarkdownPlugin::detect_changes(state.clone(), file_from_bytes(b))
        .expect("detect_changes should succeed");
    state = apply_changes_to_active_state(state, delta_b);

    let delta_c = MarkdownPlugin::detect_changes(state.clone(), file_from_bytes(c))
        .expect("detect_changes should succeed");
    state = apply_changes_to_active_state(state, delta_c);

    let output = render_active_state(state).expect("render should succeed");
    assert_eq!(output, b"Two.\n\n# Title v3\n");
}

#[test]
fn roundtrip_delete_all_blocks_to_empty_document() {
    let before_state = active_state_from_file(file_from_bytes(b"A\n\nB\n"));

    let delta = MarkdownPlugin::detect_changes(before_state.clone(), file_from_bytes(b""))
        .expect("detect_changes should succeed");
    let output = render_active_state(apply_changes_to_active_state(before_state, delta))
        .expect("render should succeed");

    assert_eq!(output, b"\n");
}

#[test]
fn roundtrip_list_internal_edit_keeps_top_level_block_model() {
    let before_state = active_state_from_file(file_from_bytes(b"- one\n- two\n"));

    let delta = MarkdownPlugin::detect_changes(
        before_state.clone(),
        file_from_bytes(b"- one\n- two changed\n"),
    )
    .expect("detect_changes should succeed");

    assert_eq!(
        delta
            .iter()
            .filter(
                |change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_none()
            )
            .count(),
        0
    );
    assert_eq!(
        delta
            .iter()
            .filter(
                |change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some()
            )
            .count(),
        1
    );

    let output = render_active_state(apply_changes_to_active_state(before_state, delta))
        .expect("render should succeed");
    assert_eq!(output, b"- one\n- two changed\n");
}

#[test]
fn roundtrip_table_row_add_remove_reorder() {
    let initial = b"| a | b |\n| - | - |\n| 1 | 2 |\n";
    let add = b"| a | b |\n| - | - |\n| 1 | 2 |\n| 3 | 4 |\n";
    let reorder = b"| a | b |\n| - | - |\n| 3 | 4 |\n| 1 | 2 |\n";
    let remove = b"| a | b |\n| - | - |\n| 3 | 4 |\n";
    let mut state = active_state_from_file(file_from_bytes(initial));

    for next in [add.as_slice(), reorder.as_slice(), remove.as_slice()] {
        let delta = MarkdownPlugin::detect_changes(state.clone(), file_from_bytes(next))
            .expect("detect_changes should succeed");
        assert_eq!(
            delta
                .iter()
                .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY
                    && change.snapshot_content.is_some())
                .count(),
            1
        );
        state = apply_changes_to_active_state(state, delta);
    }

    let output = render_active_state(state).expect("render should succeed");
    assert_eq!(output, b"| a | b |\n| - | - |\n| 3 | 4 |\n");
}

#[test]
fn roundtrip_large_tiny_edits_500_with_state_context() {
    let paragraphs = (1..=500)
        .map(|index| format!("P{index}"))
        .collect::<Vec<_>>();
    let before = paragraphs.join("\n\n") + "\n";
    let mut state = active_state_from_file(file_from_bytes(before.as_bytes()));

    let mut after = paragraphs;
    for index in [10usize, 111, 222, 333, 444] {
        after[index] = format!("{} x", after[index]);
    }
    let after = after.join("\n\n") + "\n";

    let delta = MarkdownPlugin::detect_changes(state.clone(), file_from_bytes(after.as_bytes()))
        .expect("detect_changes should succeed");
    assert_eq!(
        delta
            .iter()
            .filter(
                |change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some()
            )
            .count(),
        5
    );
    state = apply_changes_to_active_state(state, delta);

    let output = render_active_state(state).expect("render should succeed");
    assert_eq!(output, after.as_bytes());
}

#[test]
fn roundtrip_large_duplicate_edit_with_state_context() {
    let before_blocks = (0..500).map(|_| "Same").collect::<Vec<_>>();
    let before = before_blocks.join("\n\n") + "\n";
    let mut state = active_state_from_file(file_from_bytes(before.as_bytes()));

    let mut after_blocks = before_blocks;
    after_blocks[349] = "Same updated";
    let after = after_blocks.join("\n\n") + "\n";

    let delta = MarkdownPlugin::detect_changes(state.clone(), file_from_bytes(after.as_bytes()))
        .expect("detect_changes should succeed");
    assert_eq!(
        delta
            .iter()
            .filter(
                |change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some()
            )
            .count(),
        1
    );
    state = apply_changes_to_active_state(state, delta);

    let output = render_active_state(state).expect("render should succeed");
    assert_eq!(output, after.as_bytes());
}

#[test]
fn guest_interface_uses_active_state_for_low_noise_edit_and_render() {
    let before_state = active_state_from_file(file_from_bytes(b"Hello\n\nWorld\n"));

    let delta = MarkdownPlugin::detect_changes(
        before_state.clone(),
        file_from_bytes(b"Hello updated\n\nWorld\n"),
    )
    .expect("detect_changes should succeed");
    assert_eq!(
        delta
            .iter()
            .filter(
                |change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some()
            )
            .count(),
        1
    );

    let output = render_active_state(apply_changes_to_active_state(before_state, delta))
        .expect("render should succeed");
    assert_eq!(output, b"Hello updated\n\nWorld\n");
}
