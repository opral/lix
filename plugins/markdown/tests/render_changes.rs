mod common;

use common::{assert_invalid_input, block_change, decode_utf8, document_change};
use plugin_md_v2::{BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY, DetectedChange};

#[test]
fn materializes_markdown_from_document_order_and_blocks() {
    let changes = vec![
        block_change("b2", "paragraph", "Second paragraph."),
        document_change(vec!["b1".to_string(), "b2".to_string()]),
        block_change("b1", "heading", "# Title"),
    ];

    let data = common::render_projection(changes).expect("render_changes should succeed");

    assert_eq!(decode_utf8(data), "# Title\n\nSecond paragraph.\n");
}

#[test]
fn renders_empty_when_no_markdown_rows_are_present() {
    let data = common::render_projection(Vec::new()).expect("render_changes should succeed");

    assert!(data.is_empty());
}

#[test]
fn rejects_duplicate_document_rows() {
    let changes = vec![
        document_change(vec!["b1".to_string()]),
        document_change(vec!["b2".to_string()]),
    ];

    let error = common::render_projection(changes).expect_err("render_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_duplicate_block_rows() {
    let changes = vec![
        block_change("b1", "paragraph", "a"),
        block_change("b1", "paragraph", "b"),
    ];

    let error = common::render_projection(changes).expect_err("render_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_unknown_document_entity_pk() {
    let changes = vec![DetectedChange {
        entity_pk: vec!["other".to_string()],
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        snapshot_content: Some(
            serde_json::json!({
                "id": "other",
                "order": ["b1"],
            })
            .to_string(),
        ),
        metadata: None,
    }];

    let error = common::render_projection(changes).expect_err("render_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_invalid_block_snapshot_json() {
    let changes = vec![DetectedChange {
        entity_pk: vec!["b1".to_string()],
        schema_key: BLOCK_SCHEMA_KEY.to_string(),
        snapshot_content: Some("{".to_string()),
        metadata: None,
    }];

    let error = common::render_projection(changes).expect_err("render_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_invalid_document_snapshot_json() {
    let changes = vec![DetectedChange {
        entity_pk: vec![plugin_md_v2::ROOT_ENTITY_PK.to_string()],
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        snapshot_content: Some("{".to_string()),
        metadata: None,
    }];

    let error = common::render_projection(changes).expect_err("render_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_block_snapshot_id_mismatch_with_entity_pk() {
    let changes = vec![DetectedChange {
        entity_pk: vec!["b1".to_string()],
        schema_key: BLOCK_SCHEMA_KEY.to_string(),
        snapshot_content: Some(
            serde_json::json!({
                "id": "b2",
                "type": "paragraph",
                "node": {},
                "markdown": "hello",
            })
            .to_string(),
        ),
        metadata: None,
    }];

    let error = common::render_projection(changes).expect_err("render_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_document_snapshot_id_mismatch_with_root() {
    let changes = vec![DetectedChange {
        entity_pk: vec![plugin_md_v2::ROOT_ENTITY_PK.to_string()],
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        snapshot_content: Some(
            serde_json::json!({
                "id": "other",
                "order": ["b1"],
            })
            .to_string(),
        ),
        metadata: None,
    }];

    let error = common::render_projection(changes).expect_err("render_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn ignores_unknown_schema_rows() {
    let changes = vec![DetectedChange {
        entity_pk: vec!["unknown1".to_string()],
        schema_key: "other_schema".to_string(),
        snapshot_content: Some("{\"x\":1}".to_string()),
        metadata: None,
    }];

    let data = common::render_projection(changes).expect("render_changes should succeed");

    assert!(data.is_empty());
}

#[test]
fn skips_missing_block_ids_referenced_in_document_order() {
    let changes = vec![
        document_change(vec!["b1".to_string(), "b2".to_string()]),
        block_change("b1", "paragraph", "Only this exists."),
    ];

    let data = common::render_projection(changes).expect("render_changes should succeed");

    assert_eq!(decode_utf8(data), "Only this exists.\n");
}

#[test]
fn appends_orphan_blocks_not_in_document_order() {
    let changes = vec![
        document_change(vec!["b1".to_string()]),
        block_change("b2", "paragraph", "Second"),
        block_change("b1", "paragraph", "First"),
    ];

    let data = common::render_projection(changes).expect("render_changes should succeed");

    assert_eq!(decode_utf8(data), "First\n\nSecond\n");
}

#[test]
fn materializes_deterministically_without_document_row() {
    let changes = vec![
        block_change("b2", "paragraph", "Second"),
        block_change("b1", "paragraph", "First"),
    ];

    let data = common::render_projection(changes).expect("render_changes should succeed");

    // BTreeMap key ordering makes this deterministic.
    assert_eq!(decode_utf8(data), "First\n\nSecond\n");
}

#[test]
fn normalizes_block_markdown_whitespace_and_trailing_newline() {
    let changes = vec![
        document_change(vec!["b1".to_string(), "b2".to_string()]),
        block_change("b1", "heading", "\n# Title\n"),
        block_change("b2", "paragraph", "\n\nParagraph\n\n"),
    ];

    let data = common::render_projection(changes).expect("render_changes should succeed");

    assert_eq!(decode_utf8(data), "# Title\n\nParagraph\n");
}
