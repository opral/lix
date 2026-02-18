mod common;

use common::{assert_invalid_input, file_from_markdown, root_change};
use plugin_md_v2::{apply_changes, PluginEntityChange, ROOT_ENTITY_ID, SCHEMA_KEY, SCHEMA_VERSION};

#[test]
fn applies_root_snapshot_to_file_data() {
    let file = file_from_markdown("f1", "/notes.md", "before");
    let changes = vec![root_change("# Updated\n\nParagraph.\n")];

    let data = apply_changes(file, changes).expect("apply_changes should succeed");

    assert_eq!(
        String::from_utf8(data).unwrap(),
        "# Updated\n\nParagraph.\n"
    );
}

#[test]
fn returns_empty_bytes_for_root_tombstone() {
    let file = file_from_markdown("f1", "/notes.md", "before");
    let changes = vec![PluginEntityChange {
        entity_id: ROOT_ENTITY_ID.to_string(),
        schema_key: SCHEMA_KEY.to_string(),
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

    assert_eq!(String::from_utf8(data).unwrap(), "keep me");
}

#[test]
fn rejects_duplicate_root_rows() {
    let file = file_from_markdown("f1", "/notes.md", "before");
    let changes = vec![root_change("first"), root_change("second")];

    let error = apply_changes(file, changes).expect_err("apply_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn rejects_unknown_entity_id_for_markdown_schema() {
    let file = file_from_markdown("f1", "/notes.md", "before");
    let changes = vec![PluginEntityChange {
        entity_id: "other".to_string(),
        schema_key: SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some("{\"markdown\":\"x\"}".to_string()),
    }];

    let error = apply_changes(file, changes).expect_err("apply_changes should fail");

    assert_invalid_input(error);
}
