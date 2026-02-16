mod common;

use common::file_from_bytes;
use plugin_text_lines::{apply_changes, detect_changes};

#[test]
fn detect_then_apply_roundtrips_exact_bytes() {
    let payload = b"first line\nsecond line\r\nthird line\n";
    let file = file_from_bytes("f1", "/doc.txt", payload);

    let changes = detect_changes(None, file).expect("detect_changes should succeed");
    let reconstructed = apply_changes(file_from_bytes("f1", "/doc.txt", b""), changes)
        .expect("apply_changes should succeed");

    assert_eq!(reconstructed, payload);
}

#[test]
fn update_roundtrip_preserves_exact_target_bytes() {
    let before = file_from_bytes("f1", "/doc.txt", b"a\nb\nc\n");
    let after_payload = b"a\nx\nc\n";
    let after = file_from_bytes("f1", "/doc.txt", after_payload);

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");
    let reconstructed = apply_changes(file_from_bytes("f1", "/doc.txt", b""), changes)
        .expect("apply_changes should succeed");

    assert_eq!(reconstructed, after_payload);
}
