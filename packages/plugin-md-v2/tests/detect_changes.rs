mod common;

use common::{
    assert_invalid_input, file_from_markdown, is_block_change, is_document_change,
    parse_document_order,
};
use plugin_md_v2::{detect_changes, BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY, SCHEMA_VERSION};

#[test]
fn returns_empty_when_documents_are_equal() {
    let before = file_from_markdown("f1", "/notes.md", "# Title\n\nSame paragraph.\n");
    let after = file_from_markdown("f1", "/notes.md", "# Title\n\nSame paragraph.\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert!(changes.is_empty());
}

#[test]
fn emits_document_and_block_rows_for_new_file() {
    let after = file_from_markdown("f1", "/notes.md", "# Title\n\nParagraph.\n");

    let changes = detect_changes(None, after).expect("detect_changes should succeed");

    let document_rows = changes
        .iter()
        .filter(|change| is_document_change(change))
        .collect::<Vec<_>>();
    let block_rows = changes
        .iter()
        .filter(|change| is_block_change(change))
        .collect::<Vec<_>>();

    assert_eq!(document_rows.len(), 1);
    assert_eq!(block_rows.len(), 2);

    for row in block_rows {
        assert_eq!(row.schema_key, BLOCK_SCHEMA_KEY);
        assert_eq!(row.schema_version, SCHEMA_VERSION);
        assert!(row.snapshot_content.is_some());
    }

    let order = parse_document_order(document_rows[0]);
    assert_eq!(order.len(), 2);
}

#[test]
fn move_only_emits_document_row() {
    let before = file_from_markdown("f1", "/notes.md", "First paragraph.\n\nSecond paragraph.\n");
    let after = file_from_markdown("f1", "/notes.md", "Second paragraph.\n\nFirst paragraph.\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].schema_key, DOCUMENT_SCHEMA_KEY);
}

#[test]
fn edit_emits_delete_insert_and_document_update() {
    let before = file_from_markdown("f1", "/notes.md", "# Title\n\nOriginal paragraph.\n");
    let after = file_from_markdown("f1", "/notes.md", "# Title\n\nUpdated paragraph.\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    let tombstones = changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_none())
        .count();
    let upserts = changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some())
        .count();
    let document_rows = changes
        .iter()
        .filter(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
        .count();

    assert_eq!(tombstones, 1);
    assert_eq!(upserts, 1);
    assert_eq!(document_rows, 1);
}

#[test]
fn delete_emits_block_tombstone_and_document_update() {
    let before = file_from_markdown("f1", "/notes.md", "# Title\n\nKeep this.\n\nDelete this.\n");
    let after = file_from_markdown("f1", "/notes.md", "# Title\n\nKeep this.\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    let tombstones = changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_none())
        .count();
    let document_rows = changes
        .iter()
        .filter(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
        .count();

    assert_eq!(tombstones, 1);
    assert_eq!(document_rows, 1);
}

#[test]
fn supports_gfm_mdx_math_and_frontmatter_input() {
    let before = file_from_markdown("f1", "/doc.mdx", "# Start\n");
    let after = file_from_markdown(
        "f1",
        "/doc.mdx",
        "---\ntitle: Demo\n---\n\n# Start\n\n| a | b |\n| - | - |\n| 1 | 2 |\n\n- [x] done\n\nInline math $a+b$\n\n$$\nx^2 + y^2\n$$\n\n<Component prop={1}>Hello</Component>\n",
    );

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert!(changes
        .iter()
        .any(|change| change.schema_key == DOCUMENT_SCHEMA_KEY));
    assert!(changes
        .iter()
        .any(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some()));
}

#[test]
fn rejects_non_utf8_input() {
    let after = plugin_md_v2::PluginFile {
        id: "f1".to_string(),
        path: "/notes.md".to_string(),
        data: vec![0xFF, 0xFE, 0xFD],
    };

    let error = detect_changes(None, after).expect_err("detect_changes should fail");

    assert_invalid_input(error);
}
