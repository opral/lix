use super::*;
use sdk::testing::{Harness, Snapshot};

#[test]
#[ignore = "manual matched baseline adapter profile"]
fn profile_matched_sparse_sql() {
    use std::time::Instant;
    let mut harness = Harness::<MarkdownPlugin>::default();
    harness.max_batch_bytes = 16 * 1024 * 1024;
    let creates = sdk::CreateContext::from_namespace_bytes([27; 12]);
    for count in [1_000, 10_000, 100_000] {
        let input = Snapshot {
            file_id: "matched-profile".into(),
            path: "doc.md".into(),
            bytes: format!("{}\n", vec!["Ordinary paragraph text."; count].join("\n\n"))
                .into_bytes(),
            ..Snapshot::default()
        };
        let started = Instant::now();
        let parsed = harness.parse(&input, creates).unwrap();
        let import_us = started.elapsed().as_micros();
        let mut rows = Vec::new();
        accept_rows(&mut rows, &parsed.row_changes, creates);
        let mut file = parsed.into_snapshot();
        let target = rows.iter().filter(|row| row.row.get("payload_json").is_some_and(|value| {
            matches!(value, sdk::TypedValue::Jsonb(payload) if serde_json::to_string(payload).unwrap().contains("Ordinary paragraph"))
        })).nth(count / 2).unwrap().clone();
        let mut times = Vec::new();
        for iteration in 0..9 {
            let replacement = if iteration % 2 == 0 {
                "A longer ordinary paragraph text."
            } else {
                "Short text."
            };
            let mut row = target.row.clone();
            let Some(sdk::TypedValue::Jsonb(payload)) = row.get("payload_json") else {
                unreachable!()
            };
            let payload: Value = serde_json::from_str(
                &serde_json::to_string(payload)
                    .unwrap()
                    .replace("Ordinary paragraph text.", replacement),
            )
            .unwrap();
            row.insert("payload_json", sdk::TypedValue::Jsonb(payload.into()));
            let change = sdk::TypedRowChange {
                schema_key: target.schema_key.clone(),
                schema_fingerprint: target.schema_fingerprint,
                primary_key: target.primary_key.clone(),
                row: Some(row),
                local_ref: None,
                effect: sdk::ChangeEffect::Content,
            };
            let started = Instant::now();
            let result = harness.serialize_changes(&file, &[change]).unwrap();
            times.push(started.elapsed().as_micros());
            file = result.into_snapshot();
            assert!(String::from_utf8_lossy(&file.bytes).contains(replacement));
        }
        times.sort_unstable();
        eprintln!(
            "MATCHED markdown paragraphs={count} import_us={import_us} edit_p50_us={} edit_p95_us={} samples={times:?}",
            times[4], times[8]
        );
    }
}

#[test]
fn sparse_sql_paragraph_edits_read_only_the_changed_block() {
    profile_sparse_sql(1_000);
}

#[test]
#[ignore = "manual adapter cost and latency profile"]
fn profile_sparse_sql_paragraph_edits() {
    for count in [1_000, 10_000, 100_000] {
        profile_sparse_sql(count);
    }
}

fn profile_sparse_sql(count: usize) {
    let mut harness = Harness::<MarkdownPlugin>::default();
    harness.max_batch_bytes = 16 * 1024 * 1024;
    let creates = sdk::CreateContext::from_namespace_bytes([19; 12]);
    let input = Snapshot {
        file_id: "sparse-profile".into(),
        path: "doc.md".into(),
        bytes: format!("{}\n", vec!["Ordinary paragraph text."; count].join("\n\n")).into_bytes(),
        ..Snapshot::default()
    };
    let parsed = harness.parse(&input, creates).unwrap();
    let mut rows = Vec::new();
    accept_rows(&mut rows, &parsed.row_changes, creates);
    let mut file = parsed.into_snapshot();
    let target = rows.iter().filter(|row| row.row.get("payload_json").is_some_and(|value| {
        matches!(value, sdk::TypedValue::Jsonb(payload) if serde_json::to_string(payload).unwrap().contains("Ordinary paragraph"))
    })).nth(count / 2).unwrap().clone();
    for replacement in [
        "A longer ordinary paragraph text.",
        "Short text.",
        "A [literal] bracket.",
    ] {
        let mut row = target.row.clone();
        let Some(sdk::TypedValue::Jsonb(payload)) = row.get("payload_json") else {
            unreachable!()
        };
        let payload: Value = serde_json::from_str(
            &serde_json::to_string(payload)
                .unwrap()
                .replace("Ordinary paragraph text.", replacement),
        )
        .unwrap();
        row.insert("payload_json", sdk::TypedValue::Jsonb(payload.into()));
        let change = sdk::TypedRowChange {
            schema_key: target.schema_key.clone(),
            schema_fingerprint: target.schema_fingerprint,
            primary_key: target.primary_key.clone(),
            row: Some(row),
            local_ref: None,
            effect: sdk::ChangeEffect::Content,
        };
        let mut fallback = file.clone();
        fallback
            .state
            .retain(|key, _| !key.starts_with(BLOCK_IDS_STATE));
        let started = std::time::Instant::now();
        let baseline = harness
            .serialize_changes(&fallback, std::slice::from_ref(&change))
            .unwrap();
        let baseline_time = started.elapsed();
        let started = std::time::Instant::now();
        let sparse = harness
            .serialize_changes(&file, std::slice::from_ref(&change))
            .unwrap();
        let sparse_time = started.elapsed();
        assert_eq!(sparse.snapshot().bytes, baseline.snapshot().bytes);
        if replacement.contains('[') {
            assert!(sparse.metrics.file_bytes_read >= input.bytes.len() as u64 - 100);
        } else {
            assert!(sparse.metrics.file_bytes_read < 100);
            assert!(sparse.metrics.state_bytes_written < 4096);
        }
        assert!(baseline.metrics.file_bytes_read >= input.bytes.len() as u64 - 100);
        eprintln!(
            "markdown paragraphs={count} replacement={replacement:?} fallback_us={} sparse_us={} fallback_metrics={:?} sparse_metrics={:?}",
            baseline_time.as_micros(),
            sparse_time.as_micros(),
            baseline.metrics,
            sparse.metrics
        );
        accept_rows(&mut rows, &[change], creates);
        file = sparse.into_snapshot();
        let restored = harness
            .serialize(&file.file_id, &file.path, &rows, Some(&file))
            .unwrap();
        assert_eq!(restored.snapshot().bytes, file.bytes);
    }
}

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
    assert!(changed.snapshot().state.contains_key(&orphan_overlay));
    accept_rows(&mut rows, &[row_edit], creates);
    file = changed.into_snapshot();

    let unchanged = harness.serialize_changes(&file, &[]).unwrap();
    assert_eq!(unchanged.snapshot().bytes, b"Delta\n\nBravo\n");
    assert!(!unchanged.snapshot().state.contains_key(&orphan_overlay));
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
