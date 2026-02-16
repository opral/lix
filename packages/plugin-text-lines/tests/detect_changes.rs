mod common;

use common::{file_from_bytes, parse_document_snapshot};
use plugin_text_lines::{
    detect_changes, DOCUMENT_ENTITY_ID, DOCUMENT_SCHEMA_KEY, LINE_SCHEMA_KEY, SCHEMA_VERSION,
};

#[test]
fn creation_returns_full_projection() {
    let after = file_from_bytes("f1", "/doc.txt", b"a\nb\n");

    let changes = detect_changes(None, after).expect("detect_changes should succeed");

    let line_changes = changes
        .iter()
        .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
        .collect::<Vec<_>>();
    assert_eq!(line_changes.len(), 2);
    assert!(line_changes
        .iter()
        .all(|change| change.schema_version == SCHEMA_VERSION));
    assert!(line_changes
        .iter()
        .all(|change| change.snapshot_content.is_some()));

    let document_change = changes
        .iter()
        .find(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
        .expect("document snapshot should exist");
    assert_eq!(document_change.entity_id, DOCUMENT_ENTITY_ID);
    let doc = parse_document_snapshot(document_change);
    assert_eq!(doc.line_ids.len(), 2);
}

#[test]
fn insertion_in_middle_emits_inserted_line_and_document_change() {
    let before = file_from_bytes("f1", "/doc.txt", b"a\nb\n");
    let after = file_from_bytes("f1", "/doc.txt", b"a\nx\nb\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    let line_inserts = changes
        .iter()
        .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
        .filter(|change| change.snapshot_content.is_some())
        .collect::<Vec<_>>();
    let line_tombstones = changes
        .iter()
        .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
        .filter(|change| change.snapshot_content.is_none())
        .collect::<Vec<_>>();

    assert_eq!(line_inserts.len(), 1);
    assert_eq!(line_tombstones.len(), 0);
    assert!(changes
        .iter()
        .any(|change| change.schema_key == DOCUMENT_SCHEMA_KEY));
}

#[test]
fn deletion_emits_line_tombstone_and_document_change() {
    let before = file_from_bytes("f1", "/doc.txt", b"a\nb\n");
    let after = file_from_bytes("f1", "/doc.txt", b"a\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    let line_tombstones = changes
        .iter()
        .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
        .filter(|change| change.snapshot_content.is_none())
        .collect::<Vec<_>>();

    assert_eq!(line_tombstones.len(), 1);
    assert!(changes
        .iter()
        .any(|change| change.schema_key == DOCUMENT_SCHEMA_KEY));
}

#[test]
fn unchanged_file_returns_no_changes() {
    let before = file_from_bytes("f1", "/doc.txt", b"unchanged\n");
    let after = file_from_bytes("f1", "/doc.txt", b"unchanged\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert!(changes.is_empty());
}

#[test]
fn line_reorder_emits_delete_and_insert() {
    let before = file_from_bytes("f1", "/doc.txt", b"a\nb\n");
    let after = file_from_bytes("f1", "/doc.txt", b"b\na\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    let line_inserts = changes
        .iter()
        .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
        .filter(|change| change.snapshot_content.is_some())
        .collect::<Vec<_>>();
    let line_tombstones = changes
        .iter()
        .filter(|change| change.schema_key == LINE_SCHEMA_KEY)
        .filter(|change| change.snapshot_content.is_none())
        .collect::<Vec<_>>();

    assert_eq!(line_inserts.len(), 1);
    assert_eq!(line_tombstones.len(), 1);
    assert_ne!(line_inserts[0].entity_id, line_tombstones[0].entity_id);
    assert!(changes
        .iter()
        .any(|change| change.schema_key == DOCUMENT_SCHEMA_KEY));
}
