mod common;

use common::{file_from_bytes, parse_document_snapshot};
use plugin_text_lines::{
    apply_changes, detect_changes, PluginApiError, PluginEntityChange, DOCUMENT_SCHEMA_KEY,
    LINE_SCHEMA_KEY,
};

#[test]
fn applies_full_projection_and_reconstructs_bytes() {
    let expected = b"line 1\nline 2\r\nline 3";
    let after = file_from_bytes("f1", "/doc.txt", expected);

    let changes = detect_changes(None, after).expect("detect_changes should succeed");
    let output = apply_changes(file_from_bytes("f1", "/doc.txt", b""), changes)
        .expect("apply_changes should succeed");

    assert_eq!(output, expected);
}

#[test]
fn supports_binary_bytes() {
    let expected = vec![0xff, b'\n', 0x00, b'\r', b'\n', 0x7f];
    let after = file_from_bytes("f1", "/bin.dat", &expected);

    let changes = detect_changes(None, after).expect("detect_changes should succeed");
    let output = apply_changes(file_from_bytes("f1", "/bin.dat", b""), changes)
        .expect("apply_changes should succeed");

    assert_eq!(output, expected);
}

#[test]
fn rejects_missing_document_snapshot() {
    let changes = vec![PluginEntityChange {
        entity_id: "line:abc:0".to_string(),
        schema_key: LINE_SCHEMA_KEY.to_string(),
        schema_version: "1".to_string(),
        snapshot_content: Some(r#"{"content_base64":"YQ==","ending":"\n"}"#.to_string()),
    }];

    let error = apply_changes(file_from_bytes("f1", "/doc.txt", b""), changes)
        .expect_err("apply_changes should fail");

    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("missing text_document snapshot"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn document_order_drives_output_order() {
    let after = file_from_bytes("f1", "/doc.txt", b"a\nb\n");
    let mut changes = detect_changes(None, after).expect("detect_changes should succeed");

    let document_index = changes
        .iter()
        .position(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
        .expect("document row should exist");
    let mut doc = parse_document_snapshot(&changes[document_index]);
    doc.line_ids.reverse();
    changes[document_index].snapshot_content = Some(
        serde_json::json!({
            "line_ids": doc.line_ids,
        })
        .to_string(),
    );

    let output = apply_changes(file_from_bytes("f1", "/doc.txt", b""), changes)
        .expect("apply_changes should succeed");

    assert_eq!(output, b"b\na\n");
}
