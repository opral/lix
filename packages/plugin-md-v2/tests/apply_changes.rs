mod common;

use common::{
    assert_invalid_input, block_change, decode_utf8, document_change, empty_file,
    file_from_markdown,
};
use plugin_md_v2::{apply_changes, BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY, SCHEMA_VERSION};

#[test]
fn materializes_markdown_from_document_order_and_blocks() {
    let file = empty_file("f1", "/notes.md");
    let changes = vec![
        block_change("b2", "paragraph", "Second paragraph."),
        document_change(vec!["b1".to_string(), "b2".to_string()]),
        block_change("b1", "heading", "# Title"),
    ];

    let data = apply_changes(file, changes).expect("apply_changes should succeed");

    assert_eq!(decode_utf8(data), "# Title\n\nSecond paragraph.\n");
}

#[test]
fn document_tombstone_results_in_empty_file() {
    let file = file_from_markdown("f1", "/notes.md", "before");
    let changes = vec![plugin_md_v2::PluginEntityChange {
        entity_id: plugin_md_v2::ROOT_ENTITY_ID.to_string(),
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: None,
    }];

    let data = apply_changes(file, changes).expect("apply_changes should succeed");

    assert!(data.is_empty());
}

#[test]
fn passes_through_when_no_markdown_rows_are_present() {
    let file = file_from_markdown("f1", "/notes.md", "keep me");

    let data = apply_changes(file, Vec::new()).expect("apply_changes should succeed");

    assert_eq!(decode_utf8(data), "keep me");
}

#[test]
fn rejects_duplicate_document_rows() {
    let file = empty_file("f1", "/notes.md");
    let changes = vec![
        document_change(vec!["b1".to_string()]),
        document_change(vec!["b2".to_string()]),
    ];

    let error = apply_changes(file, changes).expect_err("apply_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_duplicate_block_rows() {
    let file = empty_file("f1", "/notes.md");
    let changes = vec![
        block_change("b1", "paragraph", "a"),
        block_change("b1", "paragraph", "b"),
    ];

    let error = apply_changes(file, changes).expect_err("apply_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_unknown_document_entity_id() {
    let file = empty_file("f1", "/notes.md");
    let changes = vec![plugin_md_v2::PluginEntityChange {
        entity_id: "other".to_string(),
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(
            serde_json::json!({
                "id": "other",
                "order": ["b1"],
            })
            .to_string(),
        ),
    }];

    let error = apply_changes(file, changes).expect_err("apply_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_schema_version_mismatch() {
    let file = empty_file("f1", "/notes.md");
    let changes = vec![plugin_md_v2::PluginEntityChange {
        entity_id: "b1".to_string(),
        schema_key: BLOCK_SCHEMA_KEY.to_string(),
        schema_version: "2".to_string(),
        snapshot_content: Some(
            serde_json::json!({
                "id": "b1",
                "type": "paragraph",
                "node": {},
                "markdown": "x",
            })
            .to_string(),
        ),
    }];

    let error = apply_changes(file, changes).expect_err("apply_changes should fail");

    assert_invalid_input(error);
}
