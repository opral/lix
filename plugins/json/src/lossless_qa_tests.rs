use super::core::{Document, FileEdit, IdNamespace, RowRecord};

fn assert_roundtrip(source: &str) {
    let (document, _) =
        Document::open_fresh_file(source.as_bytes().to_vec(), None, IdNamespace([7; 16]))
            .unwrap_or_else(|error| panic!("parse {source:?}: {error}"));
    let mut rows = document.row_records().expect("export rows");
    // SQL does not promise row order. Cold serialization must work in arbitrary order.
    rows.reverse();
    let (cold, _) =
        Document::open_rows(rows).unwrap_or_else(|error| panic!("import {source:?}: {error}"));
    assert_eq!(cold.bytes(), source.as_bytes(), "cold roundtrip {source:?}");
    let (unchanged, edits) = cold.rows_changed(&[]).expect("empty changes");
    assert!(edits.is_empty());
    assert_eq!(unchanged.bytes(), source.as_bytes());
}

#[test]
fn lossless_qa_adversarial_lexemes_and_layout() {
    for scalar in [
        "null",
        "true",
        "false",
        "-0",
        "0.000",
        "1E+03",
        "-123456789012345678901234567890",
        "1e-9999",
        r#""\u0061\/\b\f\n\r\t\\\"""#,
        r#""\ud83d\ude00""#,
        "\"é中😀\"",
    ] {
        for whitespace in ["", " ", "\r\n\t "] {
            assert_roundtrip(&format!("{whitespace}{scalar}{whitespace}"));
            assert_roundtrip(&format!(
                "{whitespace}{{\"\\u0061\" \t: {scalar} ,\r\n\"z\": [ {scalar},{{ }} ] }}{whitespace}"
            ));
        }
    }
}

#[test]
fn lossless_qa_generated_nested_documents_and_replacements() {
    let mut seed = 0x93ae_8a56_u64;
    fn next(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        *seed
    }
    fn generate(seed: &mut u64, depth: usize) -> String {
        let scalar = ["null", "true", "-0", "17.000e+2", r#""\u0061""#, "\"😀\""];
        if depth == 0 || next(seed) % 3 == 0 {
            return scalar[(next(seed) % scalar.len() as u64) as usize].into();
        }
        let count = (next(seed) % 5) as usize;
        let object = next(seed) % 2 == 0;
        let mut children = Vec::new();
        for i in 0..count {
            let value = generate(seed, depth - 1);
            children.push(if object {
                format!("\"k{}~/\" \t: {value}", i % 2)
            } else {
                value
            });
        }
        let joined = children.join(" \r\n,\t");
        if object {
            format!("{{ \n{joined}\t }}")
        } else {
            format!("[ \n{joined}\t ]")
        }
    }
    for _ in 0..256 {
        let before = generate(&mut seed, 4);
        let after = generate(&mut seed, 4);
        assert_roundtrip(&before);
        let (document, _) =
            Document::open_file(before.as_bytes().to_vec(), None, IdNamespace([8; 16])).unwrap();
        let mut accepted_rows = document.row_records().unwrap();
        let (successor, changes) = document
            .file_changed(
                &[FileEdit {
                    offset: 0,
                    delete_len: before.len() as u64,
                    insert: after.as_bytes(),
                }],
                IdNamespace([9; 16]),
            )
            .unwrap();
        assert_eq!(successor.bytes(), after.as_bytes());
        for change in changes {
            accepted_rows
                .retain(|row| row.schema_key != change.schema_key || row.row_pk != change.row_pk);
            if let Some(row) = change.row {
                accepted_rows.push(RowRecord {
                    schema_key: change.schema_key,
                    row_pk: change.row_pk,
                    row,
                });
            }
        }
        accepted_rows.reverse();
        let (replayed, _) = Document::open_rows(accepted_rows).unwrap_or_else(|error| {
            panic!("replayed delta error {error}: {before:?} -> {after:?}")
        });
        assert_eq!(
            replayed.bytes(),
            after.as_bytes(),
            "replayed changes {before:?} -> {after:?}"
        );
        let (cold, _) = Document::open_rows(successor.row_records().unwrap()).unwrap();
        assert_eq!(
            cold.bytes(),
            after.as_bytes(),
            "changed cold roundtrip {after:?}"
        );
    }
}

#[test]
fn lossless_qa_unrepresentable_numbers_fail_row_export() {
    for source in ["1e9999", "-1e9999"] {
        let (document, _) =
            Document::open_fresh_file(source.as_bytes().to_vec(), None, IdNamespace([7; 16]))
                .unwrap();
        assert!(
            document.row_records().is_err(),
            "unsupported number must not silently change"
        );
    }
}

#[test]
fn lossless_qa_nesting_limit_does_not_overflow_default_stack() {
    const CHILD_FLAG: &str = "LIX_JSON_NESTING_QA_CHILD";
    if std::env::var_os(CHILD_FLAG).is_some() {
        for depth in [128, 256, 512, 1024, 1025] {
            eprintln!("checking nesting depth {depth}");
            let source = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
            let parsed =
                Document::open_fresh_file(source.as_bytes().to_vec(), None, IdNamespace([7; 16]));
            if depth <= 1024 {
                let (document, _) = parsed.expect("supported nesting");
                let (cold, _) = Document::open_rows(document.row_records().unwrap()).unwrap();
                assert_eq!(cold.bytes(), source.as_bytes());
                let changed_source = source.replace('0', "1");
                let (changed, _) = document
                    .file_changed(
                        &[FileEdit {
                            offset: 0,
                            delete_len: source.len() as u64,
                            insert: changed_source.as_bytes(),
                        }],
                        IdNamespace([8; 16]),
                    )
                    .unwrap();
                assert_eq!(changed.bytes(), changed_source.as_bytes());
                let mut deepest = changed.row_records().unwrap().pop().unwrap();
                deepest
                    .row
                    .insert("order_key", super::sdk::TypedValue::Text("90".into()));
                let (reordered, _) = changed
                    .rows_changed(&[super::core::RowChange {
                        schema_key: deepest.schema_key,
                        row_pk: deepest.row_pk,
                        row: Some(deepest.row),
                        effect: super::core::ChangeEffect::Content,
                    }])
                    .unwrap();
                assert_eq!(reordered.bytes(), changed_source.as_bytes());
            } else {
                assert!(parsed.is_err());
            }
        }
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "lossless_qa_tests::lossless_qa_nesting_limit_does_not_overflow_default_stack",
            "--nocapture",
        ])
        .env(CHILD_FLAG, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "nested JSON child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn lossless_qa_native_warm_and_cold_numeric_lexical_edits() {
    use super::{JsonPlugin, sdk};
    use sdk::testing::{Harness, Snapshot};
    fn accept(
        rows: &mut Vec<sdk::TypedRowRecord>,
        transition: &sdk::testing::Transition,
        creates: sdk::CreateContext,
    ) {
        for change in &transition.row_changes {
            let primary_key = change.local_ref.map_or_else(
                || change.primary_key.clone(),
                |local| vec![sdk::TypedValue::Uuid(creates.id(local))],
            );
            rows.retain(|row| {
                row.schema_key != change.schema_key || row.primary_key != primary_key
            });
            if let Some(mut row) = change.row.clone() {
                if change.local_ref.is_some() {
                    row.insert("id", primary_key[0].clone());
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
    let harness = Harness::<JsonPlugin>::default();
    for warm in [false, true] {
        let creates = sdk::CreateContext::from_namespace_bytes([0x77; 12]);
        let initial = Snapshot {
            file_id: "numeric-lexemes".into(),
            path: "qa.json".into(),
            bytes: b"{\"n\":1.00,\"keep\":\"\\u0061\"}\n".to_vec(),
            ..Snapshot::default()
        };
        let parsed = harness.parse(&initial, creates).unwrap();
        let mut rows = Vec::new();
        accept(&mut rows, &parsed, creates);
        let mut file = parsed.into_snapshot();
        let mut old_number = "1.00";
        for number in [
            "1e-9999",
            "18446744073709551616",
            "0.123456789012345678901234567890",
            "-0",
            "1.000e+0",
        ] {
            if !warm {
                file.state.clear();
            }
            let transition = harness
                .parse_changes(
                    &file,
                    &file.path,
                    &[sdk::FileEdit {
                        offset: 5,
                        delete_len: old_number.len() as u64,
                        insert: number.as_bytes().to_vec(),
                    }],
                    Some(&rows),
                    creates,
                )
                .unwrap();
            accept(&mut rows, &transition, creates);
            file = transition.into_snapshot();
            let serialized = harness
                .serialize(&file.file_id, &file.path, &rows, None)
                .unwrap();
            assert_eq!(
                serialized.snapshot().bytes,
                file.bytes,
                "warm={warm}, number={number}"
            );
            old_number = number;
        }
    }
}

#[test]
fn lossless_qa_duplicate_occurrence_gap_survives_cached_reopen() {
    use super::core::{ChangeEffect, RowChange};
    use super::sdk;
    let (document, _) = Document::open_file(
        br#"{"a":1,"a":2,"a":3}"#.to_vec(),
        None,
        IdNamespace([7; 16]),
    )
    .unwrap();
    let rows = document.row_records().unwrap();
    let first = rows
        .iter()
        .find(|row| row.row.get("occurrence") == Some(&sdk::TypedValue::Int8(0)))
        .unwrap();
    let (deleted, _) = document
        .rows_changed(&[RowChange {
            schema_key: first.schema_key.clone(),
            row_pk: first.row_pk.clone(),
            row: None,
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    assert_eq!(deleted.bytes(), br#"{"a":2,"a":3}"#);
    let checkpoint = deleted.identity_checkpoint();
    let reopened =
        Document::open_file_with_checkpoint(deleted.bytes(), IdNamespace([7; 16]), &checkpoint)
            .unwrap();
    assert_eq!(
        reopened.row_records().unwrap(),
        deleted.row_records().unwrap()
    );
    let second = reopened
        .row_records()
        .unwrap()
        .into_iter()
        .find(|row| row.row.get("occurrence") == Some(&sdk::TypedValue::Int8(1)))
        .unwrap();
    let mut updated = second.row;
    updated.insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(4).into()),
    );
    let (changed, _) = reopened
        .rows_changed(&[RowChange {
            schema_key: second.schema_key,
            row_pk: second.row_pk,
            row: Some(updated),
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    assert_eq!(changed.bytes(), br#"{"a":4,"a":3}"#);
}

#[test]
fn lossless_qa_unicode_and_host_scalar_boundaries() {
    use super::{JsonPlugin, sdk};
    use sdk::testing::{Harness, Snapshot};
    for source in [r#""\ud800""#, r#""\udfff""#, r#""\u0000""#, "1e9999"] {
        let result = Harness::<JsonPlugin>::default().parse(
            &Snapshot {
                file_id: "boundary".into(),
                path: "qa.json".into(),
                bytes: source.as_bytes().to_vec(),
                ..Snapshot::default()
            },
            sdk::CreateContext::from_namespace_bytes([7; 12]),
        );
        assert!(
            result.is_err(),
            "host-unrepresentable scalar must reject: {source}"
        );
    }
    // Object keys use SDK text, not JSONB strings, so escaped NUL is representable.
    assert_roundtrip(r#"{"\u0000":1}"#);
}

#[test]
fn lossless_qa_identical_shifted_subtree_keeps_custom_ids_and_occurrence_gaps() {
    use super::sdk;
    let (document, _) = Document::open_file(
        br#"[{"box":{"k":1,"k":2}},{"box":{"k":2}},{"same":[1,1]}]"#.to_vec(),
        None,
        IdNamespace([0x31; 16]),
    )
    .unwrap();
    let mut rows = document.row_records().unwrap();
    let box_row = rows
        .iter_mut()
        .find(|row| row.row.get("key") == Some(&sdk::TypedValue::Text("box".into())))
        .unwrap();
    let old_container = box_row.row.get("container_id").unwrap().clone();
    let custom = sdk::TypedValue::Text("custom-container".into());
    box_row.row.insert("container_id", custom.clone());
    rows.retain(|row| {
        !(row.row.get("parent_id") == Some(&old_container)
            && row.row.get("occurrence") == Some(&sdk::TypedValue::Int8(0)))
    });
    for row in &mut rows {
        if row.row.get("parent_id") == Some(&old_container) {
            row.row.insert("parent_id", custom.clone());
            row.row_pk[0] = custom.clone();
        }
    }
    let (document, _) = Document::open_rows(rows.clone()).unwrap();
    let before = document.bytes();
    let mut expected = before.clone();
    expected.splice(1..1, b"0,".iter().copied());
    for complete_replacement in [false, true] {
        let mut rows = rows.clone();
        let edit = if complete_replacement {
            FileEdit {
                offset: 0,
                delete_len: before.len() as u64,
                insert: &expected,
            }
        } else {
            FileEdit {
                offset: 1,
                delete_len: 0,
                insert: b"0,",
            }
        };
        let (shifted, changes) = document
            .file_changed(&[edit], IdNamespace([0x32; 16]))
            .unwrap();
        assert_eq!(shifted.bytes(), expected);
        for change in changes {
            rows.retain(|row| row.schema_key != change.schema_key || row.row_pk != change.row_pk);
            if let Some(row) = change.row {
                rows.push(RowRecord {
                    schema_key: change.schema_key,
                    row_pk: change.row_pk,
                    row,
                });
            }
        }
        let survivor = rows
            .iter()
            .find(|row| row.row.get("parent_id") == Some(&custom))
            .unwrap();
        assert_eq!(
            survivor.row.get("occurrence"),
            Some(&sdk::TypedValue::Int8(1))
        );
        let (cold, _) = Document::open_rows(rows).unwrap();
        assert_eq!(cold.bytes(), expected);
    }
}
