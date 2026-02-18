mod common;

use common::{assert_invalid_input, file_from_markdown, parse_snapshot_markdown};
use plugin_md_v2::{detect_changes, ROOT_ENTITY_ID, SCHEMA_KEY, SCHEMA_VERSION};

#[test]
fn returns_empty_when_ast_is_equal() {
    let before = file_from_markdown("f1", "/notes.md", "# Title\n\nSame paragraph.\n");
    let after = file_from_markdown("f1", "/notes.md", "# Title\n\nSame paragraph.\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert!(changes.is_empty());
}

#[test]
fn emits_root_upsert_when_ast_changes() {
    let before = file_from_markdown("f1", "/notes.md", "# Title\n\nOne.\n");
    let after = file_from_markdown("f1", "/notes.md", "# Title\n\nTwo.\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].entity_id, ROOT_ENTITY_ID);
    assert_eq!(changes[0].schema_key, SCHEMA_KEY);
    assert_eq!(changes[0].schema_version, SCHEMA_VERSION);
    assert_eq!(parse_snapshot_markdown(&changes[0]), "# Title\n\nTwo.\n");
}

#[test]
fn emits_root_upsert_for_new_file() {
    let after = file_from_markdown("f1", "/notes.md", "# Fresh\n");

    let changes = detect_changes(None, after).expect("detect_changes should succeed");

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].entity_id, ROOT_ENTITY_ID);
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

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].entity_id, ROOT_ENTITY_ID);
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
