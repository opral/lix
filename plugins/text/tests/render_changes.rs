mod common;

use common::{file_from_bytes, parse_document_snapshot};
use text_plugin::{
    DOCUMENT_SCHEMA_KEY, DetectedChange, LINE_SCHEMA_KEY, PluginError, detect_changes,
    render_changes,
};

#[test]
fn applies_full_projection_and_reconstructs_bytes() {
    let expected = b"line 1\nline 2\r\nline 3";
    let after = file_from_bytes(expected);

    let changes = detect_changes(None, after).expect("detect_changes should succeed");
    let output =
        render_changes(file_from_bytes(b""), changes).expect("render_changes should succeed");

    assert_eq!(output, expected);
}

#[test]
fn supports_binary_bytes() {
    let expected = vec![0xff, b'\n', 0x00, b'\r', b'\n', 0x7f];
    let after = file_from_bytes(&expected);

    let changes = detect_changes(None, after).expect("detect_changes should succeed");
    let output =
        render_changes(file_from_bytes(b""), changes).expect("render_changes should succeed");

    assert_eq!(output, expected);
}

#[test]
fn rejects_missing_document_snapshot() {
    let changes = vec![DetectedChange {
        entity_pk: vec!["line:abc:0".to_string()],
        schema_key: LINE_SCHEMA_KEY.to_string(),
        snapshot_content: Some(r#"{"content_base64":"YQ==","ending":"\n"}"#.to_string()),
        metadata: None,
    }];

    let error =
        render_changes(file_from_bytes(b""), changes).expect_err("render_changes should fail");

    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("missing text_document snapshot"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn document_order_drives_output_order() {
    let after = file_from_bytes(b"a\nb\n");
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

    let output =
        render_changes(file_from_bytes(b""), changes).expect("render_changes should succeed");

    assert_eq!(output, b"b\na\n");
}
