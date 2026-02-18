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

#[test]
fn rejects_invalid_block_snapshot_json() {
    let file = empty_file("f1", "/notes.md");
    let changes = vec![plugin_md_v2::PluginEntityChange {
        entity_id: "b1".to_string(),
        schema_key: BLOCK_SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some("{".to_string()),
    }];

    let error = apply_changes(file, changes).expect_err("apply_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_invalid_document_snapshot_json() {
    let file = empty_file("f1", "/notes.md");
    let changes = vec![plugin_md_v2::PluginEntityChange {
        entity_id: plugin_md_v2::ROOT_ENTITY_ID.to_string(),
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some("{".to_string()),
    }];

    let error = apply_changes(file, changes).expect_err("apply_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_block_snapshot_id_mismatch_with_entity_id() {
    let file = empty_file("f1", "/notes.md");
    let changes = vec![plugin_md_v2::PluginEntityChange {
        entity_id: "b1".to_string(),
        schema_key: BLOCK_SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(
            serde_json::json!({
                "id": "b2",
                "type": "paragraph",
                "node": {},
                "markdown": "hello",
            })
            .to_string(),
        ),
    }];

    let error = apply_changes(file, changes).expect_err("apply_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_document_snapshot_id_mismatch_with_root() {
    let file = empty_file("f1", "/notes.md");
    let changes = vec![plugin_md_v2::PluginEntityChange {
        entity_id: plugin_md_v2::ROOT_ENTITY_ID.to_string(),
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
fn ignores_unknown_schema_rows() {
    let file = file_from_markdown("f1", "/notes.md", "keep me");
    let changes = vec![
        plugin_md_v2::PluginEntityChange {
            entity_id: "unknown1".to_string(),
            schema_key: "other_schema".to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some("{\"x\":1}".to_string()),
        },
        plugin_md_v2::PluginEntityChange {
            entity_id: "unknown2".to_string(),
            schema_key: "other_schema".to_string(),
            schema_version: "999".to_string(),
            snapshot_content: None,
        },
    ];

    let data = apply_changes(file, changes).expect("apply_changes should succeed");

    assert_eq!(decode_utf8(data), "keep me");
}

#[test]
fn skips_missing_block_ids_referenced_in_document_order() {
    let file = empty_file("f1", "/notes.md");
    let changes = vec![
        document_change(vec!["b1".to_string(), "b2".to_string()]),
        block_change("b1", "paragraph", "Only this exists."),
    ];

    let data = apply_changes(file, changes).expect("apply_changes should succeed");

    assert_eq!(decode_utf8(data), "Only this exists.\n");
}

#[test]
fn appends_orphan_blocks_not_in_document_order() {
    let file = empty_file("f1", "/notes.md");
    let changes = vec![
        document_change(vec!["b1".to_string()]),
        block_change("b2", "paragraph", "Second"),
        block_change("b1", "paragraph", "First"),
    ];

    let data = apply_changes(file, changes).expect("apply_changes should succeed");

    assert_eq!(decode_utf8(data), "First\n\nSecond\n");
}

#[test]
fn materializes_deterministically_without_document_row() {
    let file = empty_file("f1", "/notes.md");
    let changes = vec![
        block_change("b2", "paragraph", "Second"),
        block_change("b1", "paragraph", "First"),
    ];

    let data = apply_changes(file, changes).expect("apply_changes should succeed");

    // BTreeMap key ordering makes this deterministic.
    assert_eq!(decode_utf8(data), "First\n\nSecond\n");
}

#[test]
fn normalizes_block_markdown_whitespace_and_trailing_newline() {
    let file = empty_file("f1", "/notes.md");
    let changes = vec![
        document_change(vec!["b1".to_string(), "b2".to_string()]),
        block_change("b1", "heading", "\n# Title\n"),
        block_change("b2", "paragraph", "\n\nParagraph\n\n"),
    ];

    let data = apply_changes(file, changes).expect("apply_changes should succeed");

    assert_eq!(decode_utf8(data), "# Title\n\nParagraph\n");
}

#[test]
fn tombstoned_block_is_not_rendered_even_if_order_mentions_it() {
    let file = empty_file("f1", "/notes.md");
    let changes = vec![
        document_change(vec!["b1".to_string(), "b2".to_string()]),
        block_change("b1", "paragraph", "Alive"),
        plugin_md_v2::PluginEntityChange {
            entity_id: "b2".to_string(),
            schema_key: BLOCK_SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: None,
        },
    ];

    let data = apply_changes(file, changes).expect("apply_changes should succeed");

    assert_eq!(decode_utf8(data), "Alive\n");
}
