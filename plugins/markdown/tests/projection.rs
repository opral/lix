use plugin_md_v2::exports::lix::plugin::api::{EntityState, Guest};
use plugin_md_v2::{
    BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY, MarkdownPlugin, PluginError, ROOT_ENTITY_PK,
};

fn entity_state(
    entity_pk: &[&str],
    schema_key: &str,
    snapshot_content: serde_json::Value,
) -> EntityState {
    EntityState {
        entity_pk: entity_pk.iter().map(|part| (*part).to_string()).collect(),
        schema_key: schema_key.to_string(),
        snapshot_content: snapshot_content.to_string(),
        metadata: None,
    }
}

fn document_row() -> EntityState {
    entity_state(
        &[ROOT_ENTITY_PK],
        DOCUMENT_SCHEMA_KEY,
        serde_json::json!({"id": ROOT_ENTITY_PK}),
    )
}

fn block_row(id: &str, order_key: &str, block: &str) -> EntityState {
    entity_state(
        &[id],
        BLOCK_SCHEMA_KEY,
        serde_json::json!({
            "id": id,
            "order_key": order_key,
            "block": block,
        }),
    )
}

fn render(state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
    MarkdownPlugin::render(state)
}

fn assert_invalid_input(error: PluginError) {
    match error {
        PluginError::InvalidInput(_) => {}
        PluginError::Internal(message) => panic!("expected InvalidInput, got Internal({message})"),
    }
}

#[test]
fn renders_single_trailing_newline_when_no_markdown_rows_are_present() {
    let data = render(Vec::new()).expect("render should succeed");

    assert_eq!(data, b"\n");
}

#[test]
fn materializes_deterministically_from_order_keys() {
    let data = render(vec![
        block_row("b2", "c0", "Second paragraph."),
        document_row(),
        block_row("b1", "80", "# Title"),
    ])
    .expect("render should succeed");

    assert_eq!(data, b"# Title\n\nSecond paragraph.\n");
}

#[test]
fn rejects_duplicate_document_rows() {
    let error = render(vec![document_row(), document_row()]).expect_err("render should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_duplicate_block_rows() {
    let error = render(vec![block_row("b1", "80", "a"), block_row("b1", "c0", "b")])
        .expect_err("render should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_unknown_document_entity_pk() {
    let error = render(vec![entity_state(
        &["other"],
        DOCUMENT_SCHEMA_KEY,
        serde_json::json!({"id": ROOT_ENTITY_PK}),
    )])
    .expect_err("render should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_invalid_block_snapshot_json() {
    let error = MarkdownPlugin::render(vec![EntityState {
        entity_pk: vec!["b1".to_string()],
        schema_key: BLOCK_SCHEMA_KEY.to_string(),
        snapshot_content: "{".to_string(),
        metadata: None,
    }])
    .expect_err("render should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_invalid_document_snapshot_json() {
    let error = MarkdownPlugin::render(vec![EntityState {
        entity_pk: vec![ROOT_ENTITY_PK.to_string()],
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        snapshot_content: "{".to_string(),
        metadata: None,
    }])
    .expect_err("render should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_block_snapshot_id_mismatch_with_entity_pk() {
    let error = render(vec![entity_state(
        &["b1"],
        BLOCK_SCHEMA_KEY,
        serde_json::json!({
            "id": "b2",
            "order_key": "80",
            "block": "hello",
        }),
    )])
    .expect_err("render should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_document_snapshot_id_mismatch_with_root() {
    let error = render(vec![entity_state(
        &[ROOT_ENTITY_PK],
        DOCUMENT_SCHEMA_KEY,
        serde_json::json!({"id": "other"}),
    )])
    .expect_err("render should fail");

    assert_invalid_input(error);
}

#[test]
fn ignores_unknown_schema_rows() {
    let data = render(vec![entity_state(
        &["unknown1"],
        "other_schema",
        serde_json::json!({"x": 1}),
    )])
    .expect("render should succeed");

    assert_eq!(data, b"\n");
}
