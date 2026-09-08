use super::*;
use sdk::testing::{Harness, Snapshot};

fn accept(
    rows: &mut Vec<sdk::TypedRowRecord>,
    transition: &sdk::testing::Transition,
    creates: sdk::CreateContext,
) {
    if transition.replaces_all_rows {
        rows.clear();
    }
    for change in &transition.row_changes {
        let primary_key = change.local_ref.map_or_else(
            || change.primary_key.clone(),
            |local| vec![sdk::TypedValue::Uuid(creates.id(local))],
        );
        rows.retain(|r| r.schema_key != change.schema_key || r.primary_key != primary_key);
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

fn parse(bytes: &[u8]) -> (Snapshot, Vec<sdk::TypedRowRecord>) {
    let file = Snapshot {
        file_id: "json-native".into(),
        path: "data.json".into(),
        bytes: bytes.to_vec(),
        ..Snapshot::default()
    };
    let creates = sdk::CreateContext::from_namespace_bytes([0x61; 12]);
    let parsed = Harness::<JsonPlugin>::default()
        .parse(&file, creates)
        .unwrap();
    let mut rows = Vec::new();
    accept(&mut rows, &parsed, creates);
    (parsed.into_snapshot(), rows)
}

fn render(file: &Snapshot, rows: &[sdk::TypedRowRecord], warm: bool) -> sdk::testing::Transition {
    Harness::<JsonPlugin>::default()
        .serialize(&file.file_id, &file.path, rows, warm.then_some(file))
        .unwrap()
}

#[test]
fn native_exact_roundtrip_json_corpus() {
    for bytes in [
        b"null".as_slice(),
        b"1.0",
        b"100.0",
        b"[1.0,100.0,-0.0]",
        b" 1.2300e+04 \r\n",
        b"-0",
        b"{\r\n  \"a\" : 1.00 , \"b\": [ true, null, \"\\u0061\" ] \r\n}\n",
        b"{\"empty\": { \n }, \"array\": [ \t ]}",
        "{\"雪\":\"😀\",\"escaped\":\"\\uD83D\\uDE00\"}".as_bytes(),
        b"[1,2,3]",
        b"{\"a\":1,\"b\":2}",
    ] {
        let (file, rows) = parse(bytes);
        for warm in [false, true] {
            assert_eq!(
                render(&file, &rows, warm).snapshot().bytes,
                bytes,
                "warm={warm} source={bytes:?}"
            );
        }
        let no_op = Harness::<JsonPlugin>::default()
            .serialize_changes(&file, &[])
            .unwrap();
        assert_eq!(no_op.snapshot().bytes, bytes);
    }
}

#[test]
fn native_repeated_file_changes_match_cold_rows_and_keep_array_ids() {
    let harness = Harness::<JsonPlugin>::default();
    let (mut file, mut rows) = parse(b"{\"items\":[{\"value\":1},{\"value\":2}],\"name\":\"old\"}");
    for revision in 0..16 {
        let value = if revision % 2 == 0 { "1000" } else { "2" };
        let after =
            format!("{{\"items\":[{{\"value\":1}},{{\"value\":{value}}}],\"name\":\"old\"}}")
                .into_bytes();
        let creates = sdk::CreateContext::from_namespace_bytes([0x70 + revision; 12]);
        let prior_ids = rows
            .iter()
            .filter(|r| r.schema_key.as_ref() == ARRAY_ITEM_SCHEMA_KEY)
            .map(|r| r.primary_key.clone())
            .collect::<Vec<_>>();
        let start = file
            .bytes
            .windows(b"\"value\":".len())
            .enumerate()
            .rfind(|(_, w)| *w == b"\"value\":")
            .unwrap()
            .0
            + b"\"value\":".len();
        let end = start + file.bytes[start..].iter().position(|b| *b == b'}').unwrap();
        let edit = sdk::FileEdit {
            offset: start as u64,
            delete_len: (end - start) as u64,
            insert: value.as_bytes().to_vec(),
        };
        let changed = harness
            .parse_changes(
                &file,
                &file.path,
                std::slice::from_ref(&edit),
                None,
                creates,
            )
            .unwrap();
        let mut cold = file.clone();
        cold.state.clear();
        let changed_cold = harness
            .parse_changes(&cold, &cold.path, &[edit], Some(&rows), creates)
            .unwrap();
        let mut cold_rows = rows.clone();
        accept(&mut cold_rows, &changed_cold, creates);
        accept(&mut rows, &changed, creates);
        file = changed.into_snapshot();
        assert_eq!(file.bytes, after);
        assert_eq!(render(&file, &rows, false).snapshot().bytes, after);
        assert_eq!(render(&file, &cold_rows, false).snapshot().bytes, after);
        let new_ids = rows
            .iter()
            .filter(|r| r.schema_key.as_ref() == ARRAY_ITEM_SCHEMA_KEY)
            .map(|r| r.primary_key.clone())
            .collect::<Vec<_>>();
        assert_eq!(prior_ids, new_ids);
    }
}

#[test]
fn native_invalid_json_rolls_back() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, _) = parse(b"{\"value\":1}");
    for bytes in [
        b"".as_slice(),
        b"{",
        b"{\"value\":01}",
        b"{\"value\":NaN}",
        b"{\"value\":\"\\uD800\"}",
        b"{\"value\":1} junk",
    ] {
        let saved = file.clone();
        let result = harness.parse_changes(
            &file,
            &file.path,
            &[sdk::FileEdit {
                offset: 0,
                delete_len: file.bytes.len() as u64,
                insert: bytes.to_vec(),
            }],
            None,
            sdk::CreateContext::from_namespace_bytes([0x62; 12]),
        );
        assert!(result.is_err(), "accepted invalid JSON {bytes:?}");
        assert_eq!(file, saved);
    }
}

#[test]
fn native_scalar_row_edits_preserve_spelling_and_refresh_offsets() {
    let harness = Harness::<JsonPlugin>::default();
    let (mut file, mut rows) =
        parse(b"{ \"number\": 1.2300e+04, \"string\": \"\\u0041\", \"edit\": 1 }");
    for replacement in [100000, 2, 300] {
        let record = rows
            .iter()
            .find(|r| r.row.get("key") == Some(&sdk::TypedValue::Text("edit".into())))
            .unwrap()
            .clone();
        let mut row = record.row.clone();
        row.insert(
            "scalar_json",
            sdk::TypedValue::Jsonb(serde_json::json!(replacement).into()),
        );
        let change = sdk::TypedRowChange {
            schema_key: record.schema_key,
            schema_fingerprint: record.schema_fingerprint,
            primary_key: record.primary_key,
            row: Some(row),
            local_ref: None,
            effect: sdk::ChangeEffect::Content,
        };
        let transition = harness
            .serialize_changes(&file, std::slice::from_ref(&change))
            .unwrap();
        let fake = &change;
        rows.iter_mut()
            .find(|r| r.schema_key == fake.schema_key && r.primary_key == fake.primary_key)
            .unwrap()
            .row = fake.row.clone().unwrap();
        file = transition.into_snapshot();
        assert!(String::from_utf8_lossy(&file.bytes).contains("1.2300e+04"));
        assert!(String::from_utf8_lossy(&file.bytes).contains("\\u0041"));
        assert_eq!(render(&file, &rows, false).snapshot().bytes, file.bytes);
    }
    let index = rows
        .iter()
        .position(|r| r.row.get("key") == Some(&sdk::TypedValue::Text("number".into())))
        .unwrap();
    let mut row = rows[index].row.clone();
    row.insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(7).into()),
    );
    let change = sdk::TypedRowChange {
        schema_key: rows[index].schema_key.clone(),
        schema_fingerprint: rows[index].schema_fingerprint,
        primary_key: rows[index].primary_key.clone(),
        row: Some(row.clone()),
        local_ref: None,
        effect: sdk::ChangeEffect::Content,
    };
    let transition = harness.serialize_changes(&file, &[change]).unwrap();
    rows[index].row = row;
    file = transition.into_snapshot();
    assert!(String::from_utf8_lossy(&file.bytes).contains("\"number\": 7,"));
    assert_eq!(render(&file, &rows, false).snapshot().bytes, file.bytes);
}

#[test]
fn native_object_insert_delete_and_rename_are_supported() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, rows) = parse(b"{\"a\":1}");
    let record = rows
        .iter()
        .find(|r| r.schema_key.as_ref() == OBJECT_MEMBER_SCHEMA_KEY)
        .unwrap();
    let mut insertion = record.row.clone();
    insertion.insert("key", sdk::TypedValue::Text("new".into()));
    let changes = [
        sdk::TypedRowChange {
            schema_key: record.schema_key.clone(),
            schema_fingerprint: record.schema_fingerprint,
            primary_key: record.primary_key.clone(),
            row: None,
            local_ref: None,
            effect: sdk::ChangeEffect::Content,
        },
        sdk::TypedRowChange {
            schema_key: record.schema_key.clone(),
            schema_fingerprint: record.schema_fingerprint,
            primary_key: vec![
                sdk::TypedValue::Text("root".into()),
                sdk::TypedValue::Text("new".into()),
            ],
            row: Some(insertion),
            local_ref: None,
            effect: sdk::ChangeEffect::Content,
        },
    ];
    let deleted = harness.serialize_changes(&file, &changes[..1]).unwrap();
    assert_eq!(deleted.snapshot().bytes, b"{}");
    let inserted = harness.serialize_changes(&file, &changes[1..]).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&inserted.snapshot().bytes).unwrap(),
        serde_json::json!({"a":1,"new":1})
    );
    let renamed = harness.serialize_changes(&file, &changes).unwrap();
    assert_eq!(renamed.snapshot().bytes, br#"{"new":1}"#);
}

#[test]
fn native_array_insert_then_structural_reparse_preserves_durable_rows() {
    let harness = Harness::<JsonPlugin>::default();
    let (mut file, mut rows) = parse(b"[1,2]");
    for (revision, next) in [b"[0,1,2]".as_slice(), b"[0,1,2,3]", b"[0,1,2,3,4]"]
        .into_iter()
        .enumerate()
    {
        let creates = sdk::CreateContext::from_namespace_bytes([0x80 + revision as u8; 12]);
        let changed = harness
            .parse_changes(
                &file,
                &file.path,
                &[sdk::FileEdit {
                    offset: 0,
                    delete_len: file.bytes.len() as u64,
                    insert: next.to_vec(),
                }],
                None,
                creates,
            )
            .unwrap();
        accept(&mut rows, &changed, creates);
        file = changed.into_snapshot();
        assert_eq!(render(&file, &rows, false).snapshot().bytes, next);
    }
}

#[test]
fn native_numeric_and_string_limits_return_errors_without_panicking() {
    for source in [b"1e9999".as_slice(), b"\"\\u0000\"", b"{\"\\u0000\":1}"] {
        let file = Snapshot {
            file_id: "limits".into(),
            path: "limits.json".into(),
            bytes: source.to_vec(),
            ..Snapshot::default()
        };
        let result = std::panic::catch_unwind(|| {
            Harness::<JsonPlugin>::default()
                .parse(&file, sdk::CreateContext::from_namespace_bytes([0x90; 12]))
        });
        assert!(result.is_ok(), "panicked on {source:?}");
    }
}

#[test]
fn native_array_insertion_identity_survives_later_scalar_and_reopen() {
    let harness = Harness::<JsonPlugin>::default();
    let (mut file, mut rows) = parse(b"[1,2]");
    let creates = sdk::CreateContext::from_namespace_bytes([0xa1; 12]);
    let first = harness
        .parse_changes(
            &file,
            &file.path,
            &[sdk::FileEdit {
                offset: 1,
                delete_len: 0,
                insert: b"0,".to_vec(),
            }],
            None,
            creates,
        )
        .unwrap();
    accept(&mut rows, &first, creates);
    file = first.into_snapshot();
    let index = rows
        .iter()
        .position(|r| {
            r.row.get("scalar_json") == Some(&sdk::TypedValue::Jsonb(serde_json::json!(0).into()))
        })
        .unwrap();
    let inserted_id = rows[index].primary_key.clone();
    let creates = sdk::CreateContext::from_namespace_bytes([0xa2; 12]);
    let second = harness
        .parse_changes(
            &file,
            &file.path,
            &[sdk::FileEdit {
                offset: 6,
                delete_len: 0,
                insert: b",3".to_vec(),
            }],
            None,
            creates,
        )
        .unwrap();
    assert!(!second.replaces_all_rows);
    accept(&mut rows, &second, creates);
    file = second.into_snapshot();
    let index = rows
        .iter()
        .position(|r| {
            r.row.get("scalar_json") == Some(&sdk::TypedValue::Jsonb(serde_json::json!(0).into()))
        })
        .unwrap();
    assert_eq!(rows[index].primary_key, inserted_id);
    assert_eq!(render(&file, &rows, false).snapshot().bytes, b"[0,1,2,3]");
    let record = rows[index].clone();
    let mut row = record.row;
    row.insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(100).into()),
    );
    let write = harness
        .serialize_changes(
            &file,
            &[sdk::TypedRowChange {
                schema_key: record.schema_key,
                schema_fingerprint: record.schema_fingerprint,
                primary_key: record.primary_key,
                row: Some(row),
                local_ref: None,
                effect: sdk::ChangeEffect::Content,
            }],
        )
        .unwrap();
    assert_eq!(write.snapshot().bytes, b"[100,1,2,3]");
}

#[test]
fn native_scalar_spelling_change_is_format_only() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, _) = parse(b"{\"n\":1.00}");
    let changed = harness
        .parse_changes(
            &file,
            &file.path,
            &[sdk::FileEdit {
                offset: 5,
                delete_len: 4,
                insert: b"1e0".to_vec(),
            }],
            None,
            sdk::CreateContext::from_namespace_bytes([0xb0; 12]),
        )
        .unwrap();
    assert_eq!(changed.row_changes.len(), 1);
    assert_eq!(changed.row_changes[0].effect, sdk::ChangeEffect::FormatOnly);
}

#[test]
fn native_full_serialize_clears_prior_scalar_shift_state() {
    let harness = Harness::<JsonPlugin>::default();
    let (mut file, mut rows) = parse(b"{\"a\":1,\"b\":2}");
    let creates = sdk::CreateContext::from_namespace_bytes([0xc1; 12]);
    let changed = harness
        .parse_changes(
            &file,
            &file.path,
            &[sdk::FileEdit {
                offset: 5,
                delete_len: 1,
                insert: b"10000".to_vec(),
            }],
            None,
            creates,
        )
        .unwrap();
    accept(&mut rows, &changed, creates);
    file = changed.into_snapshot();
    assert!(file.state.contains_key(SCALAR_SHIFTS_STATE));
    file = render(&file, &rows, true).into_snapshot();
    let record = rows
        .iter()
        .find(|r| r.row.get("key") == Some(&sdk::TypedValue::Text("b".into())))
        .unwrap();
    let mut row = record.row.clone();
    row.insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(4).into()),
    );
    let changed = harness
        .serialize_changes(
            &file,
            &[sdk::TypedRowChange {
                schema_key: record.schema_key.clone(),
                schema_fingerprint: record.schema_fingerprint,
                primary_key: record.primary_key.clone(),
                row: Some(row),
                local_ref: None,
                effect: sdk::ChangeEffect::Content,
            }],
        )
        .unwrap();
    assert_eq!(changed.snapshot().bytes, b"{\"a\":10000,\"b\":4}");
}

#[test]
fn native_nested_array_identities_survive_structural_edits_and_cold_restore() {
    let harness = Harness::<JsonPlugin>::default();
    let (mut file, mut rows) = parse(b"{\"items\":[{\"inner\":[1,2]}]}");
    for (revision, next) in [
        b"{\"items\":[{\"inner\":[0,1,2]}]}".as_slice(),
        b"{\"items\":[{\"inner\":[0,1,2]},{\"inner\":[3]}]}",
        b"{\"items\":[{\"inner\":[3]},{\"inner\":[0,1,2]}]}",
    ]
    .into_iter()
    .enumerate()
    {
        let creates = sdk::CreateContext::from_namespace_bytes([0xd0 + revision as u8; 12]);
        let changed = harness
            .parse_changes(
                &file,
                &file.path,
                &[sdk::FileEdit {
                    offset: 0,
                    delete_len: file.bytes.len() as u64,
                    insert: next.to_vec(),
                }],
                None,
                creates,
            )
            .unwrap();
        accept(&mut rows, &changed, creates);
        file = changed.into_snapshot();
        assert_eq!(render(&file, &rows, false).snapshot().bytes, next);
        file = render(&file, &rows, false).into_snapshot();
        let record = rows
            .iter()
            .find(|r| {
                r.row.get("scalar_json")
                    == Some(&sdk::TypedValue::Jsonb(serde_json::json!(0).into()))
            })
            .unwrap()
            .clone();
        let mut row = record.row;
        row.insert(
            "scalar_json",
            sdk::TypedValue::Jsonb(serde_json::json!(0).into()),
        );
        let changed = harness
            .serialize_changes(
                &file,
                &[sdk::TypedRowChange {
                    schema_key: record.schema_key,
                    schema_fingerprint: record.schema_fingerprint,
                    primary_key: record.primary_key,
                    row: Some(row),
                    local_ref: None,
                    effect: sdk::ChangeEffect::Content,
                }],
            )
            .unwrap();
        assert_eq!(changed.snapshot().bytes, next);
    }
}

#[test]
fn native_corrupt_cache_rejects_without_panicking_or_mutation() {
    let harness = Harness::<JsonPlugin>::default();
    let (mut file, rows) = parse(b"{\"a\":1}");
    file.state
        .insert(SCALAR_INDEX_STATE.to_vec(), SCALAR_INDEX_MAGIC.to_vec());
    let record = rows
        .iter()
        .find(|r| r.schema_key.as_ref() == OBJECT_MEMBER_SCHEMA_KEY)
        .unwrap();
    let change = sdk::TypedRowChange {
        schema_key: record.schema_key.clone(),
        schema_fingerprint: record.schema_fingerprint,
        primary_key: record.primary_key.clone(),
        row: Some(record.row.clone()),
        local_ref: None,
        effect: sdk::ChangeEffect::Content,
    };
    let result = std::panic::catch_unwind(|| harness.serialize_changes(&file, &[change]));
    assert!(result.is_ok(), "invalid state panicked");
    assert!(result.unwrap().is_err());
}

#[test]
fn native_paged_identities_restore_and_shrink_without_stale_pages() {
    let mut harness = Harness::<JsonPlugin>::default();
    harness.max_batch_bytes = 2 * 1024 * 1024;
    let bytes = format!("[{}]", vec!["0"; 20_000].join(",")).into_bytes();
    let file = Snapshot {
        file_id: "large".into(),
        path: "large.json".into(),
        bytes,
        ..Snapshot::default()
    };
    let creates = sdk::CreateContext::from_namespace_bytes([0xe0; 12]);
    let file = harness.parse(&file, creates).unwrap().into_snapshot();
    assert!(file.state.contains_key(&identity_page_key(1)));
    let changed = harness
        .parse_changes(
            &file,
            &file.path,
            &[sdk::FileEdit {
                offset: (file.bytes.len() - 1) as u64,
                delete_len: 0,
                insert: b",1".to_vec(),
            }],
            None,
            sdk::CreateContext::from_namespace_bytes([0xe1; 12]),
        )
        .unwrap();
    assert!(changed.snapshot().bytes.ends_with(b",0,1]"));
    let file = changed.into_snapshot();
    let changed = harness
        .parse_changes(
            &file,
            &file.path,
            &[sdk::FileEdit {
                offset: 0,
                delete_len: file.bytes.len() as u64,
                insert: b"[]".to_vec(),
            }],
            None,
            sdk::CreateContext::from_namespace_bytes([0xe2; 12]),
        )
        .unwrap();
    assert_eq!(changed.snapshot().bytes, b"[]");
    assert!(!changed.snapshot().state.contains_key(&identity_page_key(1)));
}

#[test]
fn native_duplicate_member_keys_fail_before_rows_are_accepted() {
    for source in [
        br#"{"a":1,"a":2}"#.as_slice(),
        br#"{"a":1,"\u0061":2}"#,
        br#"[{"a":1,"a":2}]"#,
    ] {
        let file = Snapshot {
            file_id: "duplicates".into(),
            path: "data.json".into(),
            bytes: source.to_vec(),
            ..Snapshot::default()
        };
        assert!(
            Harness::<JsonPlugin>::default()
                .parse(&file, sdk::CreateContext::from_namespace_bytes([0xf0; 12]))
                .is_err(),
            "accepted duplicate fields {source:?}"
        );
    }
}

fn upsert_record(record: &sdk::TypedRowRecord) -> sdk::TypedRowChange {
    sdk::TypedRowChange {
        schema_key: record.schema_key.clone(),
        schema_fingerprint: record.schema_fingerprint,
        primary_key: record.primary_key.clone(),
        row: Some(record.row.clone()),
        local_ref: None,
        effect: sdk::ChangeEffect::Content,
    }
}

fn delete_record(record: &sdk::TypedRowRecord) -> sdk::TypedRowChange {
    let mut change = upsert_record(record);
    change.row = None;
    change
}

#[test]
fn native_structural_batches_validate_the_final_tree_and_roll_back() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, rows) = parse(br#"{"box":{"x":1},"keep":2}"#);
    let member = |key: &str| {
        rows.iter()
            .find(|r| r.row.get("key") == Some(&sdk::TypedValue::Text(key.into())))
            .unwrap()
    };
    let root = rows
        .iter()
        .find(|r| r.schema_key.as_ref() == ROOT_SCHEMA_KEY)
        .unwrap();
    let saved = file.clone();
    let mut updated = upsert_record(member("keep"));
    updated.row.as_mut().unwrap().insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(9).into()),
    );
    for batch in [
        vec![delete_record(root)],
        vec![updated, delete_record(member("box"))],
    ] {
        assert!(harness.serialize_changes(&file, &batch).is_err());
        assert_eq!(file, saved);
    }
    // Parent-first deletion is valid when the same batch removes descendants.
    let deleted = harness
        .serialize_changes(
            &file,
            &[delete_record(member("box")), delete_record(member("x"))],
        )
        .unwrap();
    assert_eq!(deleted.snapshot().bytes, br#"{"keep":2}"#);
    // Or move the child to a surviving container in the same batch.
    let mut moved = member("x").clone();
    moved.primary_key[0] = sdk::TypedValue::Text("root".into());
    moved.row.insert("parent_id", moved.primary_key[0].clone());
    let moved = harness
        .serialize_changes(
            &file,
            &[
                delete_record(member("box")),
                delete_record(member("x")),
                upsert_record(&moved),
            ],
        )
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&moved.snapshot().bytes).unwrap(),
        serde_json::json!({"x":1,"keep":2})
    );
}

#[test]
fn native_array_reorder_move_and_cycles_preserve_identity_or_reject() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, rows) = parse(br#"[[1],[2]]"#);
    let arrays: Vec<_> = rows
        .iter()
        .filter(|r| {
            r.schema_key.as_ref() == ARRAY_ITEM_SCHEMA_KEY
                && r.row.get("kind") == Some(&sdk::TypedValue::Text("array".into()))
        })
        .collect();
    let mut a = arrays[0].clone();
    let mut b = arrays[1].clone();
    let order = a.row.get("order_key").unwrap().clone();
    a.row
        .insert("order_key", b.row.get("order_key").unwrap().clone());
    b.row.insert("order_key", order);
    let reordered = harness
        .serialize_changes(&file, &[upsert_record(&a), upsert_record(&b)])
        .unwrap();
    assert_eq!(reordered.snapshot().bytes, b"[[2],[1]]");
    let mut changed_rows = rows.clone();
    for row in &mut changed_rows {
        if row.primary_key == a.primary_key {
            *row = a.clone();
        }
        if row.primary_key == b.primary_key {
            *row = b.clone();
        }
    }
    let file = reordered.into_snapshot();
    assert_eq!(
        render(&file, &changed_rows, false).snapshot().bytes,
        file.bytes
    );
    let id_text = |r: &sdk::TypedRowRecord| match &r.primary_key[0] {
        sdk::TypedValue::Uuid(id) => sdk::TypedValue::Text(id.to_string()),
        _ => panic!("uuid"),
    };
    a.row.insert("parent_id", id_text(&b));
    b.row.insert("parent_id", id_text(&a));
    assert!(
        harness
            .serialize_changes(&file, &[upsert_record(&a), upsert_record(&b)])
            .is_err()
    );
    a.row
        .insert("parent_id", sdk::TypedValue::Text("missing".into()));
    assert!(
        harness
            .serialize_changes(&file, &[upsert_record(&a)])
            .is_err()
    );
}

#[test]
fn native_scalar_container_conversion_and_empty_container_insertion() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, _) = parse(b" 0\n");
    let (_, target_rows) = parse(br#"{"child":1}"#);
    let changes: Vec<_> = target_rows.iter().map(upsert_record).collect();
    let converted = harness.serialize_changes(&file, &changes).unwrap();
    assert_eq!(converted.snapshot().bytes, br#"{"child":1}"#);
    let (_, scalar_rows) = parse(b"true");
    let mut back: Vec<_> = target_rows
        .iter()
        .filter(|r| r.schema_key.as_ref() != ROOT_SCHEMA_KEY)
        .map(delete_record)
        .collect();
    back.push(upsert_record(&scalar_rows[0]));
    let converted = harness
        .serialize_changes(converted.snapshot(), &back)
        .unwrap();
    assert_eq!(converted.snapshot().bytes, b"true");
    let (empty, _) = parse(b"{  }");
    let member = target_rows
        .iter()
        .find(|r| r.schema_key.as_ref() == OBJECT_MEMBER_SCHEMA_KEY)
        .unwrap();
    let inserted = harness
        .serialize_changes(&empty, &[upsert_record(member)])
        .unwrap();
    assert_eq!(inserted.snapshot().bytes, br#"{"child":1}"#);
    let mut edited = member.clone();
    edited.row.insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(7).into()),
    );
    let scalar = harness
        .serialize_changes(inserted.snapshot(), &[upsert_record(&edited)])
        .unwrap();
    assert!(
        scalar.file_replacement.is_none(),
        "scalar fast path must survive a structural rebuild"
    );
    assert_eq!(scalar.snapshot().bytes, br#"{"child":7}"#);
}

#[test]
fn native_structural_edit_preserves_unmodified_spelling_and_streams_large_files() {
    let mut harness = Harness::<JsonPlugin>::default();
    harness.max_batch_bytes = 2 * 1024 * 1024;
    let source = format!(
        r#"{{"keep":1.2300e+04,"escaped":"\u0041","large":"{}","remove":0}}"#,
        "x".repeat(3 * 1024 * 1024)
    );
    // Cold admission permits oversized values; serialize_changes itself emits
    // a streamed replacement using the ordinary page budget.
    harness.max_batch_bytes = 8 * 1024 * 1024;
    let raw = Snapshot {
        file_id: "large".into(),
        path: "large.json".into(),
        bytes: source.into_bytes(),
        ..Snapshot::default()
    };
    let creates = sdk::CreateContext::from_namespace_bytes([5; 12]);
    let parsed = harness.parse(&raw, creates).unwrap();
    let mut rows = Vec::new();
    accept(&mut rows, &parsed, creates);
    let file = parsed.into_snapshot();
    let removed = rows
        .iter()
        .find(|r| r.row.get("key") == Some(&sdk::TypedValue::Text("remove".into())))
        .unwrap();
    harness.max_batch_bytes = 2 * 1024 * 1024;
    let transition = harness
        .serialize_changes(&file, &[delete_record(removed)])
        .unwrap();
    let output = std::str::from_utf8(&transition.snapshot().bytes).unwrap();
    assert!(output.contains("1.2300e+04"));
    assert!(output.contains(r#""escaped":"\u0041""#));
    assert!(transition.file_replacement.is_some());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(output).unwrap()["large"]
            .as_str()
            .unwrap()
            .len(),
        3 * 1024 * 1024
    );
}
