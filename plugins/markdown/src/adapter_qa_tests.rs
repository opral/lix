use super::*;
use sdk::testing::{Harness, Snapshot};

fn accept_rows(
    rows: &mut Vec<sdk::TypedRowRecord>,
    changes: &[sdk::TypedRowChange],
    creates: sdk::CreateContext,
) {
    for change in changes {
        let primary_key = change.local_ref.map_or_else(
            || change.primary_key.clone(),
            |local| vec![sdk::TypedValue::Uuid(creates.id(local))],
        );
        rows.retain(|row| row.schema_key != change.schema_key || row.primary_key != primary_key);
        if let Some(mut row) = change.row.clone() {
            if change.local_ref.is_some() {
                row.insert("id".to_owned(), primary_key[0].clone());
            }
            rows.push(sdk::TypedRowRecord {
                schema_key: change.schema_key.clone(),
                schema_fingerprint: change.schema_fingerprint,
                primary_key,
                row,
            });
        }
    }
}

#[test]
fn native_adapter_exercises_all_hooks_and_cold_reopen_without_stale_overlays() {
    let harness = Harness::<MarkdownPlugin>::default();
    let creates = sdk::CreateContext::from_namespace_bytes([9; 12]);
    let mut file = Snapshot {
        file_id: "native-markdown".to_owned(),
        path: "doc.md".to_owned(),
        bytes: b"Alpha\n\nBravo\n".to_vec(),
        ..Snapshot::default()
    };
    let parsed = harness.parse(&file, creates).unwrap();
    let mut rows = Vec::new();
    accept_rows(&mut rows, &parsed.row_changes, creates);
    file = parsed.into_snapshot();
    let rendered = harness
        .serialize(&file.file_id, &file.path, &rows, None)
        .unwrap();
    assert_eq!(rendered.snapshot().bytes, file.bytes);
    file = rendered.into_snapshot();

    let creates = sdk::CreateContext::from_namespace_bytes([10; 12]);
    let changed = harness
        .parse_changes(
            &file,
            &file.path,
            &[sdk::FileEdit {
                offset: 0,
                delete_len: 5,
                insert: b"Omega".to_vec(),
            }],
            None,
            creates,
        )
        .unwrap();
    accept_rows(&mut rows, &changed.row_changes, creates);
    file = changed.into_snapshot();
    assert_eq!(file.bytes, b"Omega\n\nBravo\n");
    assert!(
        file.state.contains_key(&block_overlay_key(0)),
        "fixture must use sparse adapter path"
    );
    let orphan_overlay = b"markdown/block-overlay/orphan".to_vec();
    file.state
        .insert(orphan_overlay.clone(), b"obsolete".to_vec());

    let target = rows.iter().find(|row| {
        row.row.get("payload_json").is_some_and(|value| {
            matches!(value, sdk::TypedValue::Jsonb(payload) if serde_json::to_string(payload).unwrap().contains("Omega"))
        })
    }).unwrap();
    let mut row = target.row.clone();
    let Some(sdk::TypedValue::Jsonb(payload)) = row.get("payload_json") else {
        unreachable!()
    };
    let payload: Value = serde_json::from_str(
        &serde_json::to_string(payload)
            .unwrap()
            .replace("Omega", "Delta"),
    )
    .unwrap();
    row.insert(
        "payload_json".to_owned(),
        sdk::TypedValue::Jsonb(payload.into()),
    );
    let row_edit = sdk::TypedRowChange {
        schema_key: target.schema_key.clone(),
        schema_fingerprint: target.schema_fingerprint,
        primary_key: target.primary_key.clone(),
        row: Some(row),
        local_ref: None,
        effect: sdk::ChangeEffect::Content,
    };
    let changed = harness
        .serialize_changes(&file, std::slice::from_ref(&row_edit))
        .unwrap();
    assert_eq!(changed.snapshot().bytes, b"Delta\n\nBravo\n");
    assert!(!changed.snapshot().state.contains_key(&orphan_overlay));
    accept_rows(&mut rows, &[row_edit], creates);
    file = changed.into_snapshot();

    let unchanged = harness.serialize_changes(&file, &[]).unwrap();
    assert_eq!(unchanged.snapshot().bytes, b"Delta\n\nBravo\n");
    file = unchanged.into_snapshot();
    file.state.clear();
    let creates = sdk::CreateContext::from_namespace_bytes([11; 12]);
    let identities_before = rows
        .iter()
        .map(|row| {
            let [sdk::TypedValue::Uuid(id)] = row.primary_key.as_slice() else {
                unreachable!()
            };
            *id
        })
        .collect::<std::collections::BTreeSet<_>>();
    let reopened = harness
        .parse_changes(
            &file,
            "renamed.md",
            &[sdk::FileEdit {
                offset: 7,
                delete_len: 5,
                insert: b"Charlie".to_vec(),
            }],
            Some(&rows),
            creates,
        )
        .unwrap();
    assert_eq!(reopened.snapshot().bytes, b"Delta\n\nCharlie\n");
    assert_eq!(reopened.snapshot().path, "renamed.md");
    assert!(!reopened.snapshot().state.is_empty());
    accept_rows(&mut rows, &reopened.row_changes, creates);
    let identities_after = rows
        .iter()
        .map(|row| {
            let [sdk::TypedValue::Uuid(id)] = row.primary_key.as_slice() else {
                unreachable!()
            };
            *id
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(identities_before, identities_after);
    file = reopened.into_snapshot();
    let roundtrip = harness
        .serialize(&file.file_id, &file.path, &rows, Some(&file))
        .unwrap();
    assert_eq!(roundtrip.snapshot().bytes, file.bytes);
}
