use super::*;
use sdk::testing::{Harness, Snapshot};
use serde_json::{Value, json};

const SOURCE: &str = "{\r\n  \"type\": \"excalidraw\", \"version\":2,\r\n  \"future\": {\"keep\":[null,{\"text\":\"λ😀\"}]},\r\n  \"elements\": [\r\n    { \"id\": \"a\", \"type\": \"rectangle\", \"x\": 1, \"custom\": {\"n\":4e0,\"unicode\":\"\\u03bb\"} },\r\n    { \"id\": \"b\", \"type\": \"text\", \"text\": \"untouched\", \"isDeleted\": false }\r\n  ],\r\n  \"appState\": {\"theme\":\"dark\"},\r\n  \"files\": { \"image\" : { \"mimeType\":\"image/png\", \"dataURL\":\"data:image/png;base64,AAAA\", \"extra\": [1,{\"a\":true}] } }\r\n}\r\n";

fn ctx(n: u8) -> sdk::CreateContext {
    sdk::CreateContext::from_namespace_bytes([n; 12])
}
fn initial() -> (
    Harness<ExcalidrawPlugin>,
    Snapshot,
    Vec<sdk::TypedRowRecord>,
) {
    let harness = Harness::<ExcalidrawPlugin>::default();
    let file = Snapshot {
        file_id: "qa".into(),
        path: "scene.excalidraw".into(),
        bytes: SOURCE.as_bytes().to_vec(),
        ..Snapshot::default()
    };
    let parsed = harness.parse(&file, ctx(1)).unwrap();
    let mut rows = Vec::new();
    accept(&mut rows, &parsed.row_changes);
    (harness, parsed.into_snapshot(), rows)
}
fn accept(rows: &mut Vec<sdk::TypedRowRecord>, changes: &[sdk::TypedRowChange]) {
    for c in changes {
        assert!(c.local_ref.is_none());
        rows.retain(|r| r.schema_key != c.schema_key || r.primary_key != c.primary_key);
        if let Some(row) = &c.row {
            rows.push(sdk::TypedRowRecord {
                schema_key: c.schema_key.clone(),
                schema_fingerprint: c.schema_fingerprint,
                primary_key: c.primary_key.clone(),
                row: row.clone(),
            });
        }
    }
}
fn change(r: &sdk::TypedRowRecord) -> sdk::TypedRowChange {
    sdk::TypedRowChange {
        schema_key: r.schema_key.clone(),
        schema_fingerprint: r.schema_fingerprint,
        primary_key: r.primary_key.clone(),
        row: Some(r.row.clone()),
        local_ref: None,
        effect: sdk::ChangeEffect::Content,
    }
}
fn element(rows: &[sdk::TypedRowRecord], id: &str) -> sdk::TypedRowRecord {
    rows.iter()
        .find(|r| {
            r.schema_key.as_ref() == core::ELEMENT_SCHEMA_KEY
                && r.primary_key == [sdk::TypedValue::Text(id.into())]
        })
        .unwrap()
        .clone()
}
fn payload(row: &sdk::TypedRowRecord, field: &str) -> Value {
    let sdk::TypedValue::Jsonb(value) = row.row.get(field).unwrap() else {
        panic!("expected JSONB")
    };
    serde_json::to_value(value).unwrap()
}
fn set_payload(row: &mut sdk::TypedRowRecord, field: &str, value: Value) {
    row.row.insert(field, sdk::TypedValue::Jsonb(value.into()));
}
fn json(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).unwrap()
}
fn replace(file: &Snapshot, needle: &str, insert: &str) -> sdk::FileEdit {
    let text = std::str::from_utf8(&file.bytes).unwrap();
    sdk::FileEdit {
        offset: text.find(needle).unwrap() as u64,
        delete_len: needle.len() as u64,
        insert: insert.as_bytes().to_vec(),
    }
}
fn canonical_rows(rows: &[sdk::TypedRowRecord]) -> Vec<(String, String, sdk::TypedRow)> {
    let mut out = rows
        .iter()
        .map(|r| {
            (
                r.schema_key.to_string(),
                format!("{:?}", r.primary_key),
                r.row.clone(),
            )
        })
        .collect::<Vec<_>>();
    out.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    out
}

#[test]
fn qa_noop_rows_and_full_serialize_preserve_exact_source() {
    let (h, file, rows) = initial();
    assert_eq!(
        h.serialize(&file.file_id, &file.path, &rows, Some(&file))
            .unwrap()
            .snapshot()
            .bytes,
        file.bytes
    );
    assert_eq!(
        h.serialize(&file.file_id, &file.path, &rows, None)
            .unwrap()
            .snapshot()
            .bytes,
        file.bytes,
        "cold render needs exact payload spelling"
    );
    for changes in [
        vec![],
        vec![change(&element(&rows, "a"))],
        rows.iter().map(change).collect(),
    ] {
        let out = h.serialize_changes(&file, &changes).unwrap();
        assert_eq!(
            out.snapshot().bytes,
            file.bytes,
            "unchanged rows must not normalize source"
        );
        assert!(out.file_edits.is_empty());
    }
}

#[test]
fn qa_element_edit_preserves_unrelated_raw_objects_and_nested_data() {
    let (h, file, rows) = initial();
    let mut a = element(&rows, "a");
    let mut p = payload(&a, "element_json");
    p["x"] = json!(42);
    set_payload(&mut a, "element_json", p);
    let out = h.serialize_changes(&file, &[change(&a)]).unwrap();
    let mut expected = json(&file.bytes);
    expected["elements"][0]["x"] = json!(42);
    assert_eq!(json(&out.snapshot().bytes), expected);
    let untouched =
        "{ \"id\": \"b\", \"type\": \"text\", \"text\": \"untouched\", \"isDeleted\": false }";
    assert!(
        std::str::from_utf8(&out.snapshot().bytes)
            .unwrap()
            .contains(untouched)
    );
    assert!(std::str::from_utf8(&out.snapshot().bytes).unwrap().contains("\"image\" : { \"mimeType\":\"image/png\", \"dataURL\":\"data:image/png;base64,AAAA\", \"extra\": [1,{\"a\":true}] }"));
}

#[test]
fn qa_sparse_edits_full_serialize_reopen_and_cold_edits_agree() {
    let (h, mut file, mut rows) = initial();
    let edit = replace(&file, "\"x\": 1", "\"x\": 123456");
    let out = h
        .parse_changes(&file, &file.path, &[edit], None, ctx(2))
        .unwrap();
    assert!(
        out.snapshot().state.contains_key(ELEMENT_SHIFTS_KEY),
        "fixture takes sparse path"
    );
    accept(&mut rows, &out.row_changes);
    file = out.into_snapshot();
    file = h
        .serialize(&file.file_id, &file.path, &rows, Some(&file))
        .unwrap()
        .into_snapshot();
    let edit = replace(&file, "untouched", "edited");
    let warm = h
        .parse_changes(&file, &file.path, std::slice::from_ref(&edit), None, ctx(3))
        .unwrap();
    let mut cold = file.clone();
    cold.state.clear();
    let cold = h
        .parse_changes(&cold, &cold.path, &[edit], Some(&rows), ctx(3))
        .unwrap();
    let mut warm_rows = rows.clone();
    let mut cold_rows = rows.clone();
    accept(&mut warm_rows, &warm.row_changes);
    accept(&mut cold_rows, &cold.row_changes);
    assert_eq!(warm.snapshot().bytes, cold.snapshot().bytes);
    assert_eq!(canonical_rows(&warm_rows), canonical_rows(&cold_rows));
    let fresh = h.parse(warm.snapshot(), ctx(4)).unwrap();
    let mut fresh_rows = Vec::new();
    accept(&mut fresh_rows, &fresh.row_changes);
    assert_eq!(canonical_rows(&warm_rows), canonical_rows(&fresh_rows));
}

#[test]
fn qa_reorder_delete_create_preserves_native_ids_and_unknown_content() {
    let (h, file, mut rows) = initial();
    let mut expected = json(&file.bytes);
    expected["elements"].as_array_mut().unwrap().swap(0, 1);
    let text = serde_json::to_vec(&expected).unwrap();
    let out = h
        .parse_changes(
            &file,
            "renamed.excalidraw",
            &[sdk::FileEdit {
                offset: 0,
                delete_len: file.bytes.len() as u64,
                insert: text,
            }],
            None,
            ctx(2),
        )
        .unwrap();
    accept(&mut rows, &out.row_changes);
    let mut file = out.into_snapshot();
    let mut a = element(&rows, "a");
    let mut p = payload(&a, "element_json");
    p["id"] = json!("c");
    p["custom"]["new"] = json!([true,null,{"deep":"preserved"}]);
    a.primary_key = vec![sdk::TypedValue::Text("c".into())];
    a.row.insert("id", sdk::TypedValue::Text("c".into()));
    set_payload(&mut a, "element_json", p.clone());
    let mut delete = change(&element(&rows, "a"));
    delete.row = None;
    let changes = vec![delete, change(&a)];
    let out = h.serialize_changes(&file, &changes).unwrap();
    accept(&mut rows, &changes);
    file = out.into_snapshot();
    expected["elements"][1] = p;
    assert_eq!(json(&file.bytes), expected);
    let fresh = h.parse(&file, ctx(3)).unwrap();
    let mut fresh_rows = Vec::new();
    accept(&mut fresh_rows, &fresh.row_changes);
    let rendered = h.serialize(&file.file_id, &file.path, &rows, None).unwrap();
    assert_eq!(json(&rendered.snapshot().bytes), json(&file.bytes));
    assert_eq!(
        element(&fresh_rows, "c").primary_key,
        [sdk::TypedValue::Text("c".into())]
    );
}

#[test]
fn qa_invalid_json_and_row_changes_roll_back() {
    let (h, file, rows) = initial();
    let before = file.clone();
    for insert in [
        b"{".to_vec(),
        b"{\"elements\":[{\"id\":\"x\",\"type\":\"rectangle\"},{\"id\":\"x\",\"type\":\"text\"}]}"
            .to_vec(),
    ] {
        assert!(
            h.parse_changes(
                &file,
                &file.path,
                &[sdk::FileEdit {
                    offset: 0,
                    delete_len: file.bytes.len() as u64,
                    insert
                }],
                None,
                ctx(2)
            )
            .is_err()
        );
        assert_eq!(file, before);
    }
    let mut a = element(&rows, "a");
    let mut p = payload(&a, "element_json");
    p["id"] = json!("wrong");
    set_payload(&mut a, "element_json", p);
    assert!(h.serialize_changes(&file, &[change(&a)]).is_err());
    assert_eq!(file, before);
}

#[test]
fn qa_repeated_edits_are_deterministic_and_preserve_data() {
    let (h, file, rows) = initial();
    let run = || {
        let (mut file, mut rows) = (file.clone(), rows.clone());
        for n in 0..24 {
            let value = json!({"id":"a","type":"rectangle","x":n,"custom":{"nested":[null,{"text":"λ😀"}],"n":4.0}});
            let mut a = element(&rows, "a");
            set_payload(&mut a, "element_json", value);
            let changes = [change(&a)];
            let out = h.serialize_changes(&file, &changes).unwrap();
            accept(&mut rows, &changes);
            file = out.into_snapshot();
            if n % 4 == 0 {
                file.state.clear();
                let noop = h
                    .parse_changes(&file, &file.path, &[], Some(&rows), ctx(n as u8 + 2))
                    .unwrap();
                accept(&mut rows, &noop.row_changes);
                file = noop.into_snapshot();
            }
            assert_eq!(json(&file.bytes)["elements"][0]["x"], json!(n));
        }
        (file, canonical_rows(&rows))
    };
    assert_eq!(run(), run());
}

#[test]
fn qa_full_render_discards_sparse_length_shifts() {
    let h = Harness::<ExcalidrawPlugin>::default();
    let file = Snapshot {
        file_id: "small".into(),
        path: "a.excalidraw".into(),
        bytes: br#"{"elements":[{"id":"a","type":"text","text":"x"}]}"#.to_vec(),
        ..Snapshot::default()
    };
    let parsed = h.parse(&file, ctx(1)).unwrap();
    let mut rows = Vec::new();
    accept(&mut rows, &parsed.row_changes);
    let file = parsed.into_snapshot();
    let out = h
        .parse_changes(
            &file,
            &file.path,
            &[replace(&file, "\"x\"", "\"abcdefghijklmnopqrstuvwxyz\"")],
            None,
            ctx(2),
        )
        .unwrap();
    accept(&mut rows, &out.row_changes);
    let file = out.into_snapshot();
    let file = h
        .serialize(&file.file_id, &file.path, &rows, Some(&file))
        .unwrap()
        .into_snapshot();
    let out = h
        .parse_changes(
            &file,
            &file.path,
            &[replace(&file, "abcdef", "ABCDEF")],
            None,
            ctx(3),
        )
        .unwrap();
    assert_eq!(
        json(&out.snapshot().bytes)["elements"][0]["text"],
        json!("ABCDEFghijklmnopqrstuvwxyz")
    );
}

#[test]
fn qa_whitespace_before_collection_commas_survives_cold_render_and_edits() {
    let h = Harness::<ExcalidrawPlugin>::default();
    let file=Snapshot{file_id:"spacing".into(),path:"s.excalidraw".into(),bytes:br#"{"elements":[{"id":"a","type":"text","text":"x"}  , {"id":"b","type":"rectangle"} ],"files":{"f":{"dataURL":"A"}   , "g":{"dataURL":"B"}}}"#.to_vec(),..Snapshot::default()};
    let parsed = h.parse(&file, ctx(1)).unwrap();
    let mut rows = Vec::new();
    accept(&mut rows, &parsed.row_changes);
    let file = parsed.into_snapshot();
    assert_eq!(
        h.serialize(&file.file_id, &file.path, &rows, None)
            .unwrap()
            .snapshot()
            .bytes,
        file.bytes
    );
    let mut a = element(&rows, "a");
    let mut p = payload(&a, "element_json");
    p["text"] = json!("changed");
    set_payload(&mut a, "element_json", p);
    let out = h.serialize_changes(&file, &[change(&a)]).unwrap();
    assert_eq!(
        std::str::from_utf8(&out.snapshot().bytes).unwrap(),
        std::str::from_utf8(&file.bytes)
            .unwrap()
            .replace("\"x\"", "\"changed\"")
    );
}

#[test]
fn qa_row_reorder_and_embedded_file_edit_keep_every_other_value() {
    let (h, file, mut rows) = initial();
    let (mut a, mut b) = (element(&rows, "a"), element(&rows, "b"));
    let ak = a.row.get("order_key").unwrap().clone();
    let bk = b.row.get("order_key").unwrap().clone();
    a.row.insert("order_key", bk);
    b.row.insert("order_key", ak);
    let updates = [change(&a), change(&b)];
    let out = h.serialize_changes(&file, &updates).unwrap();
    accept(&mut rows, &updates);
    let file = out.into_snapshot();
    let mut expected = json(SOURCE.as_bytes());
    expected["elements"].as_array_mut().unwrap().swap(0, 1);
    assert_eq!(json(&file.bytes), expected);
    let mut image = rows
        .iter()
        .find(|r| r.schema_key.as_ref() == core::FILE_SCHEMA_KEY)
        .unwrap()
        .clone();
    let mut p = payload(&image, "file_json");
    p["dataURL"] = json!("data:image/png;base64,BBBB");
    set_payload(&mut image, "file_json", p);
    let out = h.serialize_changes(&file, &[change(&image)]).unwrap();
    assert_eq!(
        std::str::from_utf8(&out.snapshot().bytes).unwrap(),
        std::str::from_utf8(&file.bytes)
            .unwrap()
            .replace("base64,AAAA", "base64,BBBB")
    );
}

#[test]
fn qa_stale_or_absent_spelling_hints_never_override_payload_edits() {
    let (h, file, rows) = initial();
    for hint in [
        sdk::TypedValue::Null,
        sdk::TypedValue::Text("not json".into()),
        sdk::TypedValue::Text(r#"{"id":"wrong","type":"text","x":9000}"#.into()),
    ] {
        let mut a = element(&rows, "a");
        a.row.insert("source_json", hint);
        let mut p = payload(&a, "element_json");
        p["x"] = json!(777);
        set_payload(&mut a, "element_json", p);
        let out = h.serialize_changes(&file, &[change(&a)]).unwrap();
        assert_eq!(json(&out.snapshot().bytes)["elements"][0]["x"], json!(777));
        assert_eq!(json(&out.snapshot().bytes)["elements"][0]["id"], json!("a"));
        assert_eq!(
            json(&out.snapshot().bytes)["elements"][0]["custom"]["unicode"],
            json!("λ")
        );
    }
}

#[test]
fn qa_spelling_only_and_identical_file_edits_have_correct_effects() {
    let (h, file, _) = initial();
    for path in [&file.path, "renamed.excalidraw"] {
        let out = h
            .parse_changes(
                &file,
                path,
                &[replace(&file, "\"x\": 1", "\"x\": 1.0")],
                None,
                ctx(2),
            )
            .unwrap();
        assert_eq!(out.row_changes.len(), 1);
        assert_eq!(out.row_changes[0].effect, sdk::ChangeEffect::FormatOnly);
    }
    let out = h
        .parse_changes(
            &file,
            &file.path,
            &[replace(&file, "untouched", "untouched")],
            None,
            ctx(2),
        )
        .unwrap();
    assert!(out.row_changes.is_empty());
    assert_eq!(out.snapshot().bytes, file.bytes);
}

#[test]
fn qa_large_embedded_file_avoids_redundant_canonical_hint_and_roundtrips() {
    let value = json!({"elements":[],"files":{"image":{"dataURL":format!("data:image/png;base64,{}","A".repeat(600*1024)),"mimeType":"image/png"}}});
    for pretty in [false, true] {
        let mut h = Harness::<ExcalidrawPlugin>::default();
        if pretty {
            h.max_batch_bytes = 2 * 1024 * 1024;
        }
        let bytes = if pretty {
            serde_json::to_vec_pretty(&value).unwrap()
        } else {
            serde_json::to_vec(&value).unwrap()
        };
        let file = Snapshot {
            file_id: "large".into(),
            path: "large.excalidraw".into(),
            bytes,
            ..Snapshot::default()
        };
        let parsed = h.parse(&file, ctx(1)).unwrap();
        let mut rows = Vec::new();
        accept(&mut rows, &parsed.row_changes);
        let image = rows
            .iter()
            .find(|r| r.schema_key.as_ref() == core::FILE_SCHEMA_KEY)
            .unwrap();
        assert_eq!(
            matches!(image.row.get("source_json"), Some(sdk::TypedValue::Null)),
            !pretty
        );
        let rendered = h.serialize(&file.file_id, &file.path, &rows, None).unwrap();
        assert_eq!(rendered.snapshot().bytes, file.bytes);
    }
}

#[test]
fn qa_soft_delete_then_remove_all_children_retains_scene_metadata() {
    let (h, file, mut rows) = initial();
    let mut b = element(&rows, "b");
    let mut p = payload(&b, "element_json");
    p["isDeleted"] = json!(true);
    set_payload(&mut b, "element_json", p);
    b.row.insert("is_deleted", sdk::TypedValue::Boolean(true));
    let changes = [change(&b)];
    let out = h.serialize_changes(&file, &changes).unwrap();
    accept(&mut rows, &changes);
    let file = out.into_snapshot();
    assert_eq!(json(&file.bytes)["elements"][1]["isDeleted"], json!(true));
    let deletes = rows
        .iter()
        .filter(|r| r.schema_key.as_ref() != core::SCENE_SCHEMA_KEY)
        .map(|r| {
            let mut c = change(r);
            c.row = None;
            c
        })
        .collect::<Vec<_>>();
    let out = h.serialize_changes(&file, &deletes).unwrap();
    let mut expected = json(&file.bytes);
    expected["elements"] = json!([]);
    expected["files"] = json!({});
    assert_eq!(json(&out.snapshot().bytes), expected);
}

#[test]
fn qa_unrepresentable_jsonb_is_rejected_without_panicking_or_mutation() {
    let h = Harness::<ExcalidrawPlugin>::default();
    let file = Snapshot {
        file_id: "invalid-jsonb".into(),
        path: "invalid.excalidraw".into(),
        bytes: br#"{"elements":[{"id":"a","type":"text","text":"\u0000"}]}"#.to_vec(),
        ..Snapshot::default()
    };
    let before = file.clone();
    assert!(h.parse(&file, ctx(1)).is_err());
    assert_eq!(file, before);
}
