use super::*;
use sdk::testing::{Harness, Snapshot};

fn parse(source: &[u8]) -> (Snapshot, Vec<sdk::TypedRowRecord>) {
    let creates = sdk::CreateContext::from_namespace_bytes([0x52; 12]);
    let parsed = Harness::<JsonPlugin>::default()
        .parse(
            &Snapshot {
                file_id: "format-qa".into(),
                path: "qa.json".into(),
                bytes: source.into(),
                ..Snapshot::default()
            },
            creates,
        )
        .unwrap();
    let rows = parsed
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
    (parsed.into_snapshot(), rows)
}

fn upsert(record: &sdk::TypedRowRecord) -> sdk::TypedRowChange {
    sdk::TypedRowChange {
        schema_key: record.schema_key.clone(),
        schema_fingerprint: record.schema_fingerprint,
        primary_key: record.primary_key.clone(),
        row: Some(record.row.clone()),
        local_ref: None,
        effect: sdk::ChangeEffect::Content,
    }
}
fn member<'a>(rows: &'a [sdk::TypedRowRecord], key: &str) -> &'a sdk::TypedRowRecord {
    rows.iter()
        .find(|r| r.row.get("key") == Some(&sdk::TypedValue::Text(key.into())))
        .unwrap()
}
fn apply(rows: &mut Vec<sdk::TypedRowRecord>, changes: &[sdk::TypedRowChange]) {
    for change in changes {
        rows.retain(|r| r.schema_key != change.schema_key || r.primary_key != change.primary_key);
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
fn cold(file: &Snapshot, rows: &[sdk::TypedRowRecord]) -> Snapshot {
    Harness::<JsonPlugin>::default()
        .serialize(&file.file_id, &file.path, rows, None)
        .unwrap()
        .into_snapshot()
}

#[test]
fn structural_format_qa_same_row_last_upsert_wins_even_when_original() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, rows) = parse(b"{\"a\":1.00,\"b\":2}");
    let original = upsert(member(&rows, "a"));
    let mut changed = original.clone();
    changed.row.as_mut().unwrap().insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(7).into()),
    );
    let result = harness
        .serialize_changes(&file, &[changed, original])
        .unwrap();
    assert_eq!(result.snapshot().bytes, file.bytes);
}

#[test]
fn structural_format_qa_rename_formatted_key_preserves_other_bytes() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, mut rows) =
        parse(b" \r\n{\n  \"\\u0061\" \t: 1.2300e+04 ,\n  \"keep\": \"\\u0041\"\n}\t");
    let original = member(&rows, "a");
    let mut deletion = upsert(original);
    deletion.row = None;
    let mut renamed = upsert(original);
    renamed.primary_key[1] = sdk::TypedValue::Text("new".into());
    renamed
        .row
        .as_mut()
        .unwrap()
        .insert("key", sdk::TypedValue::Text("new".into()));
    let changes = [deletion, renamed];
    let transition = harness.serialize_changes(&file, &changes).unwrap();
    let output = transition.snapshot();
    let value: serde_json::Value = serde_json::from_slice(&output.bytes).unwrap();
    assert_eq!(value, serde_json::json!({"new":12300.0,"keep":"A"}));
    let text = std::str::from_utf8(&output.bytes).unwrap();
    assert!(text.contains("1.2300e+04"));
    assert!(text.contains("\n  \"keep\": \"\\u0041\"\n"));
    assert!(text.starts_with(" \r\n{"));
    assert!(text.ends_with("}\t"));
    apply(&mut rows, &changes);
    assert_eq!(cold(output, &rows).bytes, output.bytes);
}

#[test]
fn structural_format_qa_deletions_preserve_remaining_layout_and_followup_edits() {
    let harness = Harness::<JsonPlugin>::default();
    for removed in ["first", "middle", "last"] {
        let (file, mut rows) = parse(
            b" \n{\r\n \"first\" : 1.00 \t, \"middle\":\"\\u0041\"\n,\t\"last\" : -0 \r\n}\t",
        );
        let mut deletion = upsert(member(&rows, removed));
        deletion.row = None;
        let changes = [deletion];
        let transition = harness.serialize_changes(&file, &changes).unwrap();
        apply(&mut rows, &changes);
        let mut file = transition.into_snapshot();
        assert_eq!(cold(&file, &rows).bytes, file.bytes);
        let kept = if removed == "first" { "last" } else { "first" };
        let mut scalar = upsert(member(&rows, kept));
        scalar.row.as_mut().unwrap().insert(
            "scalar_json",
            sdk::TypedValue::Jsonb(serde_json::json!(200).into()),
        );
        let transition = harness
            .serialize_changes(&file, std::slice::from_ref(&scalar))
            .unwrap();
        assert!(transition.file_replacement.is_none());
        apply(&mut rows, &[scalar]);
        file = transition.into_snapshot();
        assert_eq!(cold(&file, &rows).bytes, file.bytes);
        let value: serde_json::Value = serde_json::from_slice(&file.bytes).unwrap();
        assert!(value.get(removed).is_none());
        assert_eq!(value[kept], 200);
        assert!(file.bytes.starts_with(b" \n{"));
        assert!(file.bytes.ends_with(b"}\t"));
    }
}

#[test]
fn structural_format_qa_empty_container_to_scalar_ignores_obsolete_layout() {
    let harness = Harness::<JsonPlugin>::default();
    for source in [b" \n{ \t }\r\n".as_slice(), b" \n[ \t ]\r\n"] {
        let (file, mut rows) = parse(source);
        let mut scalar = upsert(&rows[0]);
        let row = scalar.row.as_mut().unwrap();
        row.insert("kind", sdk::TypedValue::Text("boolean".into()));
        row.insert(
            "scalar_json",
            sdk::TypedValue::Jsonb(serde_json::json!(true).into()),
        );
        let result = harness
            .serialize_changes(&file, std::slice::from_ref(&scalar))
            .unwrap();
        assert_eq!(result.snapshot().bytes, b" \ntrue\r\n");
        apply(&mut rows, &[scalar]);
        assert_eq!(
            cold(result.snapshot(), &rows).bytes,
            result.snapshot().bytes
        );
    }
}

#[test]
fn structural_format_qa_reordering_array_retains_each_value_and_layout() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, mut rows) = parse(b"\t[\n 1.00 \t,\r\n \"\\u0041\" , -0\n]\r\n");
    let mut items: Vec<_> = rows
        .iter()
        .filter(|r| r.schema_key.as_ref() == ARRAY_ITEM_SCHEMA_KEY)
        .cloned()
        .collect();
    let orders: Vec<_> = items
        .iter()
        .map(|r| r.row.get("order_key").unwrap().clone())
        .collect();
    for (item, order) in items.iter_mut().zip(orders.into_iter().rev()) {
        item.row.insert("order_key", order);
    }
    let changes: Vec<_> = items.iter().map(upsert).collect();
    let result = harness.serialize_changes(&file, &changes).unwrap();
    assert_eq!(
        result.snapshot().bytes,
        b"\t[ -0\n,\r\n \"\\u0041\" ,\n 1.00 \t]\r\n"
    );
    apply(&mut rows, &changes);
    let mut file = cold(result.snapshot(), &rows);
    let changed_index = rows
        .iter()
        .position(|r| {
            r.row.get("scalar_json") == Some(&sdk::TypedValue::Jsonb(serde_json::json!(1.0).into()))
        })
        .unwrap();
    rows[changed_index].row.insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(123456).into()),
    );
    file = harness
        .serialize_changes(&file, &[upsert(&rows[changed_index])])
        .unwrap()
        .into_snapshot();
    assert_eq!(file.bytes, b"\t[ -0\n,\r\n \"\\u0041\" ,\n 123456 \t]\r\n");
    assert_eq!(cold(&file, &rows).bytes, file.bytes);
}

#[test]
fn structural_format_qa_escaped_rename_updates_only_key_token() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, mut rows) = parse(b"{\n  \"a\"\t : \"\\u0041\" \n}");
    let original = member(&rows, "a");
    let mut deletion = upsert(original);
    deletion.row = None;
    let mut renamed = upsert(original);
    renamed.primary_key[1] = sdk::TypedValue::Text("new\"\\\n雪".into());
    renamed
        .row
        .as_mut()
        .unwrap()
        .insert("key", renamed.primary_key[1].clone());
    let changes = [deletion, renamed];
    let result = harness.serialize_changes(&file, &changes).unwrap();
    assert_eq!(
        std::str::from_utf8(&result.snapshot().bytes).unwrap(),
        "{\n  \"new\\\"\\\\\\n雪\"\t : \"\\u0041\" \n}"
    );
    apply(&mut rows, &changes);
    assert_eq!(
        cold(result.snapshot(), &rows).bytes,
        result.snapshot().bytes
    );
}

#[test]
fn structural_format_qa_invalid_layout_cannot_inject_json_on_rebuild() {
    let harness = Harness::<JsonPlugin>::default();
    let (file, rows) = parse(b"{\"a\":1,\"b\":2}");
    let mut deletion = upsert(member(&rows, "b"));
    deletion.row = None;
    for (field, text) in [
        ("prefix_json", "\"a\":1,\"injected\":"),
        ("prefix_json", "\"a\" 1"),
        ("prefix_json", "\"\\uD800\":"),
        ("prefix_json", "\"a\":/* comment */"),
        ("suffix_json", ",\"injected\":2"),
        ("empty_json", "injected"),
    ] {
        let mut changed = upsert(member(&rows, "a"));
        changed
            .row
            .as_mut()
            .unwrap()
            .insert(field, sdk::TypedValue::Text(text.into()));
        let saved = file.clone();
        let result = harness.serialize_changes(&file, &[deletion.clone(), changed]);
        assert!(result.is_err(), "accepted {field}={text:?}");
        assert_eq!(file, saved);
    }
}
