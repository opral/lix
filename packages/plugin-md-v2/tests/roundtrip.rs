mod common;

use common::{
    apply_delta, collect_state_rows, decode_utf8, empty_file, file_from_markdown,
    is_document_change, StateRows,
};
use plugin_md_v2::{apply_changes, detect_changes, BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY};

#[test]
fn roundtrip_file_detect_state_apply_markdown() {
    let markdown = "# Title\n\nParagraph one.\n\nParagraph two.\n";
    let file = file_from_markdown("f1", "/notes.md", markdown);

    let delta = detect_changes(None, file).expect("detect_changes should succeed");

    let mut state = StateRows::new();
    apply_delta(&mut state, delta);

    let materialized = apply_changes(empty_file("f1", "/notes.md"), collect_state_rows(&state))
        .expect("apply_changes should succeed");

    assert_eq!(decode_utf8(materialized), markdown);
}

#[test]
fn roundtrip_edit_move_delete_across_block_rows() {
    let before_markdown = "Alpha.\n\nBravo.\n\nCharlie.\n";
    let after_markdown = "Charlie.\n\nAlpha updated.\n";

    let before_file = file_from_markdown("f1", "/notes.md", before_markdown);

    let mut state = StateRows::new();
    let bootstrap =
        detect_changes(None, before_file.clone()).expect("bootstrap detect should succeed");
    apply_delta(&mut state, bootstrap);

    let delta = detect_changes(
        Some(before_file),
        file_from_markdown("f1", "/notes.md", after_markdown),
    )
    .expect("delta detect should succeed");

    assert!(delta
        .iter()
        .any(|change| change.schema_key == DOCUMENT_SCHEMA_KEY));
    assert!(delta.iter().any(|change| {
        change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_none()
    }));
    assert!(delta.iter().any(|change| {
        change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some()
    }));

    apply_delta(&mut state, delta);

    let materialized = apply_changes(empty_file("f1", "/notes.md"), collect_state_rows(&state))
        .expect("apply_changes should succeed");

    assert_eq!(decode_utf8(materialized), after_markdown);
}

#[test]
fn roundtrip_move_only_updates_document_order() {
    let before_markdown = "First block.\n\nSecond block.\n";
    let after_markdown = "Second block.\n\nFirst block.\n";

    let delta = detect_changes(
        Some(file_from_markdown("f1", "/notes.md", before_markdown)),
        file_from_markdown("f1", "/notes.md", after_markdown),
    )
    .expect("detect_changes should succeed");

    assert_eq!(delta.len(), 1);
    assert!(delta.iter().all(is_document_change));
}
