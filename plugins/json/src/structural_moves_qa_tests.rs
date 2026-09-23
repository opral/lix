use super::*;
use sdk::testing::{Harness, Snapshot};

fn parse(source: &[u8]) -> (Snapshot, Vec<sdk::TypedRowRecord>) {
    let creates = sdk::CreateContext::from_namespace_bytes([0x29; 12]);
    let transition = Harness::<JsonPlugin>::default()
        .parse(
            &Snapshot {
                file_id: "moves-qa".into(),
                path: "moves.json".into(),
                bytes: source.to_vec(),
                ..Snapshot::default()
            },
            creates,
        )
        .unwrap();
    let rows = transition
        .row_changes
        .iter()
        .filter_map(|change| {
            let mut row = change.row.clone()?;
            let primary_key = change.local_ref.map_or_else(
                || change.primary_key.clone(),
                |local| vec![sdk::TypedValue::Uuid(creates.id(local))],
            );
            if change.local_ref.is_some() {
                row.insert("id", primary_key[0].clone());
            }
            Some(sdk::TypedRowRecord {
                schema_key: change.schema_key.clone(),
                schema_fingerprint: change.schema_fingerprint,
                primary_key,
                row,
            })
        })
        .collect();
    (transition.into_snapshot(), rows)
}

fn upsert(row: &sdk::TypedRowRecord) -> sdk::TypedRowChange {
    sdk::TypedRowChange {
        schema_key: row.schema_key.clone(),
        schema_fingerprint: row.schema_fingerprint,
        primary_key: row.primary_key.clone(),
        row: Some(row.row.clone()),
        local_ref: None,
        effect: sdk::ChangeEffect::Content,
    }
}
fn delete(row: &sdk::TypedRowRecord) -> sdk::TypedRowChange {
    sdk::TypedRowChange {
        row: None,
        ..upsert(row)
    }
}
fn text(value: &str) -> sdk::TypedValue {
    sdk::TypedValue::Text(value.into())
}
fn array_id(row: &sdk::TypedRowRecord) -> sdk::TypedValue {
    match &row.primary_key[0] {
        sdk::TypedValue::Uuid(id) => text(&id.to_string()),
        _ => panic!("array item expected"),
    }
}
fn apply(rows: &mut Vec<sdk::TypedRowRecord>, changes: &[sdk::TypedRowChange]) {
    for change in changes {
        rows.retain(|row| {
            row.schema_key != change.schema_key || row.primary_key != change.primary_key
        });
        if let Some(row) = &change.row {
            rows.push(sdk::TypedRowRecord {
                schema_key: change.schema_key.clone(),
                schema_fingerprint: change.schema_fingerprint,
                primary_key: change.primary_key.clone(),
                row: row.clone(),
            });
        }
    }
}
fn cold_bytes(file: &Snapshot, rows: &[sdk::TypedRowRecord]) -> Vec<u8> {
    Harness::<JsonPlugin>::default()
        .serialize(&file.file_id, &file.path, rows, None)
        .unwrap()
        .snapshot()
        .bytes
        .clone()
}

#[test]
fn structural_moves_nested_array_then_scalar_and_cold_roundtrip() {
    let harness = Harness::<JsonPlugin>::default();
    let (mut file, mut rows) = parse(br#"[[[1.00e+02]],[],3]"#);
    let mut top: Vec<_> = rows
        .iter()
        .filter(|r| {
            r.schema_key.as_ref() == ARRAY_ITEM_SCHEMA_KEY
                && r.row.get("parent_id") == Some(&text("root"))
        })
        .cloned()
        .collect();
    top.sort_by_key(|r| match r.row.get("order_key").unwrap() {
        sdk::TypedValue::Text(t) => t.clone(),
        _ => panic!(),
    });
    let mut inner = rows
        .iter()
        .find(|r| r.row.get("parent_id") == Some(&array_id(&top[0])))
        .unwrap()
        .clone();
    let mut leaf = rows
        .iter()
        .find(|r| r.row.get("parent_id") == Some(&array_id(&inner)))
        .unwrap()
        .clone();
    for round in 0..12 {
        let target = if round % 2 == 0 { &top[1] } else { &top[0] };
        inner.row.insert("parent_id", array_id(target));
        let changes = [upsert(&inner)];
        file = harness
            .serialize_changes(&file, &changes)
            .unwrap()
            .into_snapshot();
        apply(&mut rows, &changes);
        assert_eq!(cold_bytes(&file, &rows), file.bytes);
        let actual: serde_json::Value = serde_json::from_slice(&file.bytes).unwrap();
        assert_eq!(actual.as_array().map(Vec::len), Some(3));
        assert_eq!(actual[2].as_f64(), Some(3.0));
        let (occupied, empty) = if round % 2 == 0 {
            (&actual[1], &actual[0])
        } else {
            (&actual[0], &actual[1])
        };
        assert_eq!(empty.as_array().map(Vec::len), Some(0));
        assert_eq!(
            occupied[0][0].as_f64(),
            Some(if round == 0 {
                100.0
            } else {
                (round - 1) as f64
            })
        );
        if round == 0 {
            assert!(String::from_utf8_lossy(&file.bytes).contains("1.00e+02"));
        }
        leaf.row.insert(
            "scalar_json",
            sdk::TypedValue::Jsonb(serde_json::json!(round).into()),
        );
        let changes = [upsert(&leaf)];
        let updated = harness.serialize_changes(&file, &changes).unwrap();
        assert!(updated.file_replacement.is_none());
        file = updated.into_snapshot();
        apply(&mut rows, &changes);
        assert_eq!(cold_bytes(&file, &rows), file.bytes);
    }
}

#[test]
fn structural_moves_parent_deleted_while_nested_subtree_survives() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, mut rows) = parse(br#"[[{"deep":[1,2]}],[]]"#);
    let top: Vec<_> = rows
        .iter()
        .filter(|r| {
            r.schema_key.as_ref() == ARRAY_ITEM_SCHEMA_KEY
                && r.row.get("parent_id") == Some(&text("root"))
        })
        .cloned()
        .collect();
    let parent = top
        .iter()
        .find(|r| {
            rows.iter()
                .any(|c| c.row.get("parent_id") == Some(&array_id(r)))
        })
        .unwrap();
    let destination = top
        .iter()
        .find(|r| r.primary_key != parent.primary_key)
        .unwrap();
    let mut moved = rows
        .iter()
        .find(|r| r.row.get("parent_id") == Some(&array_id(parent)))
        .unwrap()
        .clone();
    moved.row.insert("parent_id", array_id(destination));
    let changes = [delete(parent), upsert(&moved)];
    let updated = harness.serialize_changes(&file, &changes).unwrap();
    apply(&mut rows, &changes);
    assert_eq!(updated.snapshot().bytes, br#"[[{"deep":[1,2]}]]"#);
    assert_eq!(
        cold_bytes(updated.snapshot(), &rows),
        updated.snapshot().bytes
    );
}

#[test]
fn structural_moves_invalid_graphs_reject_with_scalar_update_atomically() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, rows) = parse(br#"[[1],{},2]"#);
    let scalar = rows
        .iter()
        .find(|r| {
            r.row.get("scalar_json") == Some(&sdk::TypedValue::Jsonb(serde_json::json!(2).into()))
        })
        .unwrap();
    let array = rows
        .iter()
        .find(|r| {
            r.schema_key.as_ref() == ARRAY_ITEM_SCHEMA_KEY
                && r.row.get("kind") == Some(&text("array"))
        })
        .unwrap();
    let object = rows
        .iter()
        .find(|r| r.row.get("kind") == Some(&text("object")))
        .unwrap();
    let mut child = rows
        .iter()
        .find(|r| r.row.get("parent_id") == Some(&array_id(array)))
        .unwrap()
        .clone();
    let mut scalar_update = scalar.clone();
    scalar_update.row.insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(999).into()),
    );
    let saved = file.clone();
    for parent in [
        array_id(scalar),
        array_id(object),
        text("missing"),
        array_id(&child),
    ] {
        child.row.insert("parent_id", parent);
        assert!(
            harness
                .serialize_changes(&file, &[upsert(&scalar_update), upsert(&child)])
                .is_err()
        );
        assert_eq!(file, saved);
    }
    assert_eq!(
        harness
            .serialize_changes(&file, &[])
            .unwrap()
            .snapshot()
            .bytes,
        saved.bytes
    );
}

#[test]
fn structural_moves_repeated_identity_batch_uses_final_state() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, mut rows) = parse(br#"[[1],[]]"#);
    let mut child = rows
        .iter()
        .find(|r| r.row.get("kind") == Some(&text("number")))
        .unwrap()
        .clone();
    let final_parent = rows
        .iter()
        .find(|r| {
            r.schema_key.as_ref() == ARRAY_ITEM_SCHEMA_KEY
                && r.row.get("kind") == Some(&text("array"))
                && Some(&array_id(r)) != child.row.get("parent_id")
        })
        .unwrap();
    child.row.insert("parent_id", text("temporarily-missing"));
    let intermediate = upsert(&child);
    child.row.insert("parent_id", array_id(final_parent));
    child.row.insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(8).into()),
    );
    let changes = [intermediate, delete(&child), upsert(&child)];
    let transition = harness.serialize_changes(&file, &changes).unwrap();
    apply(&mut rows, &changes);
    assert_eq!(transition.snapshot().bytes, b"[[],[8]]");
    assert_eq!(
        cold_bytes(transition.snapshot(), &rows),
        transition.snapshot().bytes
    );
}

#[test]
fn structural_moves_delete_reinsert_container_changes_kind_in_final_batch() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, mut rows) = parse(b"[[1],[]]");
    let leaf = rows
        .iter()
        .find(|r| r.row.get("kind") == Some(&text("number")))
        .unwrap()
        .clone();
    let container = rows
        .iter()
        .find(|r| {
            r.schema_key.as_ref() == ARRAY_ITEM_SCHEMA_KEY
                && Some(&array_id(r)) == leaf.row.get("parent_id")
        })
        .unwrap()
        .clone();
    let mut converted = container.clone();
    converted.row.insert("kind", text("object"));
    let (_, source) = parse(br#"{"x":2}"#);
    let mut member = source
        .iter()
        .find(|r| r.schema_key.as_ref() == OBJECT_MEMBER_SCHEMA_KEY)
        .unwrap()
        .clone();
    member.primary_key[0] = array_id(&container);
    member
        .row
        .insert("parent_id", member.primary_key[0].clone());
    let changes = [
        delete(&container),
        upsert(&member),
        upsert(&converted),
        delete(&leaf),
    ];
    let updated = harness.serialize_changes(&file, &changes).unwrap();
    apply(&mut rows, &changes);
    assert_eq!(updated.snapshot().bytes, br#"[{"x":2},[]]"#);
    assert_eq!(
        cold_bytes(updated.snapshot(), &rows),
        updated.snapshot().bytes
    );
    member.row.insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(12345).into()),
    );
    let updated = harness
        .serialize_changes(updated.snapshot(), &[upsert(&member)])
        .unwrap();
    assert_eq!(updated.snapshot().bytes, br#"[{"x":12345},[]]"#);
    assert!(updated.file_replacement.is_none());
}

#[test]
fn structural_moves_equal_order_keys_are_deterministic_across_batch_orders() {
    let harness = Harness::<JsonPlugin>::default();
    for source in [br#"{"z":1,"b":2,"a":3}"#.as_slice(), b"[1,2,3]"] {
        let (file, mut rows) = parse(source);
        let mut changes = Vec::new();
        for row in &mut rows {
            if row.schema_key.as_ref() != ROOT_SCHEMA_KEY {
                row.row.insert("order_key", text("80"));
                changes.push(upsert(row));
            }
        }
        let expected = cold_bytes(&file, &rows);
        for _ in 0..6 {
            let updated = harness.serialize_changes(&file, &changes).unwrap();
            assert_eq!(updated.snapshot().bytes, expected);
            rows.rotate_left(1);
            assert_eq!(cold_bytes(&file, &rows), expected);
            changes.rotate_left(1);
            changes.reverse();
        }
        if source[0] == b'{' {
            assert_eq!(expected, br#"{"a":3,"b":2,"z":1}"#);
        }
    }
}

#[test]
fn structural_moves_nested_object_rekey_updates_container_chain_and_keeps_array_id() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, rows) = parse(br#"{"old":{"nested":{"list":[1.00]}}}"#);
    let (_, target_rows) = parse(br#"{"new":{"nested":{"list":[1.00]}}}"#);
    let mut changes: Vec<_> = rows
        .iter()
        .filter(|r| r.schema_key.as_ref() == OBJECT_MEMBER_SCHEMA_KEY)
        .map(delete)
        .collect();
    let outer = target_rows
        .iter()
        .find(|r| r.row.get("key") == Some(&text("new")))
        .unwrap();
    let old_outer = rows
        .iter()
        .find(|r| r.row.get("key") == Some(&text("old")))
        .unwrap();
    // Moving only the outer object would leave descendants at its old container ID.
    assert!(
        harness
            .serialize_changes(&file, &[delete(old_outer), upsert(outer)])
            .is_err()
    );
    changes.extend(
        target_rows
            .iter()
            .filter(|r| r.schema_key.as_ref() != ROOT_SCHEMA_KEY)
            .map(upsert),
    );
    let old_leaf = rows
        .iter()
        .find(|r| r.schema_key.as_ref() == ARRAY_ITEM_SCHEMA_KEY)
        .unwrap();
    let mut leaf = target_rows
        .iter()
        .find(|r| r.schema_key.as_ref() == ARRAY_ITEM_SCHEMA_KEY)
        .unwrap()
        .clone();
    assert_eq!(old_leaf.primary_key, leaf.primary_key);
    let updated = harness.serialize_changes(&file, &changes).unwrap();
    assert_eq!(
        updated.snapshot().bytes,
        br#"{"new":{"nested":{"list":[1.00]}}}"#
    );
    assert_eq!(
        cold_bytes(updated.snapshot(), &target_rows),
        updated.snapshot().bytes
    );
    leaf.row.insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(999).into()),
    );
    let updated = harness
        .serialize_changes(updated.snapshot(), &[upsert(&leaf)])
        .unwrap();
    assert!(updated.file_replacement.is_none());
    assert_eq!(
        updated.snapshot().bytes,
        br#"{"new":{"nested":{"list":[999]}}}"#
    );
}

#[test]
fn structural_moves_custom_container_ids_survive_rename_and_warm_cold_file_edits() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, mut rows) = parse(br#"{"old":{"value":1}}"#);
    let parent = rows
        .iter()
        .find(|r| r.row.get("key") == Some(&text("old")))
        .unwrap()
        .clone();
    let mut renamed = parent.clone();
    renamed.primary_key[1] = text("new");
    renamed.row.insert("key", text("new"));
    renamed.row.insert("container_id", text("my-container"));
    let mut leaf = rows
        .iter()
        .find(|r| r.row.get("key") == Some(&text("value")))
        .unwrap()
        .clone();
    let old_leaf = leaf.clone();
    leaf.primary_key[0] = text("my-container");
    leaf.row.insert("parent_id", text("my-container"));
    let changes = [
        delete(&parent),
        delete(&old_leaf),
        upsert(&renamed),
        upsert(&leaf),
    ];
    let file = harness
        .serialize_changes(&file, &changes)
        .unwrap()
        .into_snapshot();
    apply(&mut rows, &changes);
    assert_eq!(file.bytes, br#"{"new":{"value":1}}"#);
    assert_eq!(cold_bytes(&file, &rows), file.bytes);
    // Change structure so reconciliation, rather than only the scalar fast path, runs.
    let replacement = br#"{"new":{"value":2,"added":true}}"#;
    for cold in [false, true] {
        let mut before = file.clone();
        if cold {
            before.state.clear();
        }
        let changed = harness
            .parse_changes(
                &before,
                &before.path,
                &[sdk::FileEdit {
                    offset: 0,
                    delete_len: before.bytes.len() as u64,
                    insert: replacement.to_vec(),
                }],
                cold.then_some(rows.as_slice()),
                sdk::CreateContext::from_namespace_bytes([0x31; 12]),
            )
            .unwrap();
        let mut updated = rows.clone();
        apply(&mut updated, &changed.row_changes);
        let parent = updated
            .iter()
            .find(|r| r.row.get("key") == Some(&text("new")))
            .unwrap();
        assert_eq!(parent.row.get("container_id"), Some(&text("my-container")));
        for key in ["value", "added"] {
            let child = updated
                .iter()
                .find(|r| r.row.get("key") == Some(&text(key)))
                .unwrap();
            assert_eq!(child.row.get("parent_id"), Some(&text("my-container")));
        }
        assert_eq!(cold_bytes(changed.snapshot(), &updated), replacement);
    }
}

#[test]
fn structural_moves_duplicate_or_empty_container_ids_are_rejected() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, rows) = parse(br#"{"left":{},"right":{}}"#);
    let mut left = rows
        .iter()
        .find(|r| r.row.get("key") == Some(&text("left")))
        .unwrap()
        .clone();
    let right = rows
        .iter()
        .find(|r| r.row.get("key") == Some(&text("right")))
        .unwrap();
    for id in [
        text(""),
        text("root"),
        right.row.get("container_id").unwrap().clone(),
    ] {
        left.row.insert("container_id", id);
        assert!(harness.serialize_changes(&file, &[upsert(&left)]).is_err());
    }
}

#[test]
fn structural_moves_custom_ids_inside_moved_array_subtree_remain_stable() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, mut rows) = parse(br#"[[{"box":{"value":1}}],[]]"#);
    let box_row = rows
        .iter()
        .find(|r| r.row.get("key") == Some(&text("box")))
        .unwrap()
        .clone();
    let leaf = rows
        .iter()
        .find(|r| r.row.get("key") == Some(&text("value")))
        .unwrap()
        .clone();
    let mut custom_box = box_row.clone();
    custom_box.row.insert("container_id", text("custom-box"));
    let mut custom_leaf = leaf.clone();
    custom_leaf.primary_key[0] = text("custom-box");
    custom_leaf.row.insert("parent_id", text("custom-box"));
    let changes = [upsert(&custom_box), delete(&leaf), upsert(&custom_leaf)];
    let file = harness
        .serialize_changes(&file, &changes)
        .unwrap()
        .into_snapshot();
    apply(&mut rows, &changes);
    let mut moved = rows
        .iter()
        .find(|r| {
            r.schema_key.as_ref() == ARRAY_ITEM_SCHEMA_KEY
                && r.row.get("kind") == Some(&text("object"))
        })
        .unwrap()
        .clone();
    let destination = rows
        .iter()
        .find(|r| {
            r.schema_key.as_ref() == ARRAY_ITEM_SCHEMA_KEY
                && r.row.get("parent_id") == Some(&text("root"))
                && Some(&array_id(r)) != moved.row.get("parent_id")
        })
        .unwrap();
    moved.row.insert("parent_id", array_id(destination));
    let changes = [upsert(&moved)];
    let file = harness
        .serialize_changes(&file, &changes)
        .unwrap()
        .into_snapshot();
    apply(&mut rows, &changes);
    assert_eq!(file.bytes, br#"[[],[{"box":{"value":1}}]]"#);
    let replacement = br#"[[],[{"box":{"value":2,"extra":3}}]]"#;
    let changed = harness
        .parse_changes(
            &file,
            &file.path,
            &[sdk::FileEdit {
                offset: 0,
                delete_len: file.bytes.len() as u64,
                insert: replacement.to_vec(),
            }],
            None,
            sdk::CreateContext::from_namespace_bytes([0x41; 12]),
        )
        .unwrap();
    apply(&mut rows, &changed.row_changes);
    let custom = rows
        .iter()
        .find(|r| r.row.get("key") == Some(&text("box")))
        .unwrap();
    assert_eq!(custom.row.get("container_id"), Some(&text("custom-box")));
    assert_eq!(cold_bytes(changed.snapshot(), &rows), replacement);
}

#[test]
fn structural_moves_source_insert_cannot_alias_a_custom_container_identity() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, mut rows) = parse(br#"{"left":{}}"#);
    let replacement = br#"{"left":{},"right":{"value":1}}"#;
    let (_, future) = parse(replacement);
    let future_id = future
        .iter()
        .find(|r| r.row.get("key") == Some(&text("right")))
        .unwrap()
        .row
        .get("container_id")
        .unwrap()
        .clone();
    let mut left = rows
        .iter()
        .find(|r| r.row.get("key") == Some(&text("left")))
        .unwrap()
        .clone();
    left.row.insert("container_id", future_id);
    let changes = [upsert(&left)];
    let file = harness
        .serialize_changes(&file, &changes)
        .unwrap()
        .into_snapshot();
    apply(&mut rows, &changes);
    let result = harness.parse_changes(
        &file,
        &file.path,
        &[sdk::FileEdit {
            offset: 0,
            delete_len: file.bytes.len() as u64,
            insert: replacement.to_vec(),
        }],
        None,
        sdk::CreateContext::from_namespace_bytes([0x51; 12]),
    );
    // Reject atomically or allocate a distinct identity; never accept an ambiguous graph.
    if let Ok(changed) = result {
        apply(&mut rows, &changed.row_changes);
        assert_eq!(cold_bytes(changed.snapshot(), &rows), replacement);
    }
}

#[test]
fn structural_moves_sql_null_representation_is_unambiguous_for_null_kind() {
    let harness = Harness::<JsonPlugin>::default();
    for source in [b"null".as_slice(), br#"{"value":null}"#, b"[null]"] {
        let (file, rows) = parse(source);
        let original = rows
            .iter()
            .find(|r| r.row.get("kind") == Some(&text("null")))
            .unwrap();
        for scalar in [None, Some(sdk::TypedValue::Null)] {
            let mut changed = original.clone();
            if let Some(value) = scalar {
                changed.row.insert("scalar_json", value);
            } else {
                let mut row = sdk::TypedRow::new();
                for (key, value) in changed.row.iter() {
                    if key != "scalar_json" {
                        row.insert(key, value.clone());
                    }
                }
                changed.row = row;
            }
            let changes = [upsert(&changed)];
            let updated = harness.serialize_changes(&file, &changes).unwrap();
            assert_eq!(updated.snapshot().bytes, source);
            let mut cold_rows = rows.clone();
            apply(&mut cold_rows, &changes);
            assert_eq!(cold_bytes(updated.snapshot(), &cold_rows), source);
            changed.row.insert("kind", text("number"));
            assert!(
                harness
                    .serialize_changes(&file, &[upsert(&changed)])
                    .is_err()
            );
        }
    }
}
