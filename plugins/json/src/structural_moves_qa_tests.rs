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
        let expected = if round == 0 {
            serde_json::json!([[], [[100.0]], 3])
        } else if round % 2 == 0 {
            serde_json::json!([[], [[round - 1]], 3])
        } else {
            serde_json::json!([[[round - 1]], [], 3])
        };
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&file.bytes).unwrap(),
            expected
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
