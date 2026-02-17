mod common;

use common::file_from_bytes;
use plugin_text_lines::{apply_changes, detect_changes, PluginEntityChange};
use std::collections::BTreeMap;

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
    let before_payload = b"a\nb\nc\n";
    let before = file_from_bytes("f1", "/doc.txt", before_payload);
    let after_payload = b"a\nx\nc\n";
    let after = file_from_bytes("f1", "/doc.txt", after_payload);

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");
    let reconstructed = apply_changes(file_from_bytes("f1", "/doc.txt", before_payload), changes)
        .expect("apply_changes should succeed");

    assert_eq!(reconstructed, after_payload);
}

#[test]
fn projected_change_log_reconstructs_from_empty_base() {
    let before_payload = b"a\nb\nc\n";
    let before_for_initial = file_from_bytes("f1", "/doc.txt", before_payload);
    let before_for_delta = file_from_bytes("f1", "/doc.txt", before_payload);
    let after_payload = b"a\nx\nc\n";
    let after = file_from_bytes("f1", "/doc.txt", after_payload);

    let initial_changes =
        detect_changes(None, before_for_initial).expect("initial detect_changes should succeed");
    let delta_changes =
        detect_changes(Some(before_for_delta), after).expect("delta detect_changes should succeed");

    let projected_changes = collapse_to_latest_projection([initial_changes, delta_changes]);
    let reconstructed = apply_changes(file_from_bytes("f1", "/doc.txt", b""), projected_changes)
        .expect("apply_changes should succeed for projected changes");

    assert_eq!(reconstructed, after_payload);
}

fn collapse_to_latest_projection(
    batches: [Vec<PluginEntityChange>; 2],
) -> Vec<PluginEntityChange> {
    let mut latest = BTreeMap::<(String, String), PluginEntityChange>::new();
    for batch in batches {
        for change in batch {
            latest.insert(
                (change.schema_key.clone(), change.entity_id.clone()),
                change,
            );
        }
    }
    latest.into_values().collect()
}
