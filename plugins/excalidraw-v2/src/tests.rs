use super::*;
use serde_json::Value;
use std::collections::HashMap;

fn namespace() -> IdNamespace {
    IdNamespace::from_halves(0x0011_2233_4455_6677, 0x8899_aabb_ccdd_eeff)
}

fn fixture() -> Vec<u8> {
    br##"{
  "type": "excalidraw",
  "version": 2,
  "source": "https://excalidraw.com",
  "elements": [
    {"id":"a","type":"rectangle","x":1.25,"y":2,"width":100,"height":80,"isDeleted":false},
    {"id":"b","type":"ellipse","x":20,"y":30,"width":50,"height":40,"isDeleted":false}
  ],
  "appState": {"gridSize":20,"viewBackgroundColor":"#ffffff"},
  "files": {
    "file-1": {"id":"file-1","mimeType":"image/png","dataURL":"data:image/png;base64,AA==","created":123}
  }
}
"##
    .to_vec()
}

fn open(bytes: &[u8]) -> Document {
    Document::open_file(bytes.to_vec(), Some("drawing.excalidraw"), namespace())
        .expect("open Excalidraw fixture")
        .0
}

fn records(document: &Document) -> Vec<EntityRecord> {
    document
        .initial_changes()
        .map(|change| {
            let change = change.expect("initial change");
            EntityRecord {
                schema_key: change.schema_key,
                entity_pk: change.entity_pk,
                snapshot: change.snapshot.expect("initial upsert"),
            }
        })
        .collect()
}

fn record<'a>(records: &'a [EntityRecord], schema: &str, id: &str) -> &'a EntityRecord {
    records
        .iter()
        .find(|record| record.schema_key == schema && record.entity_pk == [id])
        .unwrap_or_else(|| panic!("missing ({schema}, {id})"))
}

fn snapshot(record: &EntityRecord) -> Value {
    serde_json::from_slice(&record.snapshot).expect("valid snapshot")
}

fn snapshot_string(record: &EntityRecord, field: &str) -> String {
    snapshot(record)
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("missing string field {field}"))
        .to_owned()
}

fn record_map(document: &Document) -> HashMap<(String, String), EntityRecord> {
    records(document)
        .into_iter()
        .map(|record| {
            (
                (
                    record.schema_key.clone(),
                    record.entity_pk.first().expect("one-component key").clone(),
                ),
                record,
            )
        })
        .collect()
}

fn offset_of(bytes: &[u8], needle: &[u8]) -> usize {
    bytes
        .windows(needle.len())
        .position(|window| window == needle)
        .unwrap_or_else(|| {
            panic!(
                "missing byte sequence {:?}",
                String::from_utf8_lossy(needle)
            )
        })
}

fn apply_edits(before: &[u8], edits: &[ByteEdit]) -> Vec<u8> {
    let mut after = Vec::new();
    let mut cursor = 0usize;
    for edit in edits {
        let start = usize::try_from(edit.offset).expect("offset fits usize");
        let end = start + usize::try_from(edit.delete_len).expect("length fits usize");
        assert!(start >= cursor);
        assert!(end <= before.len());
        after.extend_from_slice(&before[cursor..start]);
        after.extend_from_slice(&edit.insert);
        cursor = end;
    }
    after.extend_from_slice(&before[cursor..]);
    after
}

fn has_number(value: &Value) -> bool {
    match value {
        Value::Number(_) => true,
        Value::Array(values) => values.iter().any(has_number),
        Value::Object(values) => values.values().any(has_number),
        Value::Null | Value::Bool(_) | Value::String(_) => false,
    }
}

#[test]
fn manifest_and_schemas_expose_the_three_semantic_units() {
    let manifest: Value = serde_json::from_str(MANIFEST_JSON).unwrap();
    assert_eq!(manifest["key"], "plugin_excalidraw_v2");
    assert_eq!(manifest["runtime"], "wasm-component-v2");
    assert_eq!(manifest["api_version"], "2.0.0");
    assert_eq!(manifest["match"]["path_glob"], "*.excalidraw");
    assert_eq!(manifest["schemas"].as_array().unwrap().len(), 3);

    let keys = SCHEMAS
        .iter()
        .map(|(_, schema)| {
            serde_json::from_str::<Value>(schema)
                .unwrap()
                .get("x-lix-key")
                .and_then(Value::as_str)
                .unwrap()
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        [SCENE_SCHEMA_KEY, ELEMENT_SCHEMA_KEY, FILE_SCHEMA_KEY]
    );
}

#[test]
fn exact_pretty_json_roundtrip_and_number_free_entities() {
    let bytes = fixture();
    let document = open(&bytes);
    assert_eq!(document.bytes(), bytes);
    let records = records(&document);
    assert_eq!(records.len(), 4);
    assert_eq!(
        records
            .iter()
            .filter(|record| record.schema_key == ELEMENT_SCHEMA_KEY)
            .count(),
        2
    );
    for record in &records {
        assert!(!has_number(&snapshot(record)), "{record:#?}");
    }

    let (reopened, edit) = Document::open_entities(records).expect("open acknowledged entities");
    assert_eq!(edit.offset, 0);
    assert_eq!(edit.delete_len, 0);
    assert_eq!(edit.insert.as_slice(), bytes);
    assert_eq!(reopened.bytes(), bytes);
}

#[test]
fn localized_file_edit_emits_exactly_one_existing_element_upsert() {
    let before = fixture();
    let document = open(&before);
    let accepted = record_map(&document);
    let offset = offset_of(&before, b"1.25");
    let splice = [InputSplice {
        offset: u64::try_from(offset).unwrap(),
        delete_len: 4,
        insert: b"123.5",
    }];
    let (after, changes) = document
        .file_changed(&splice, IdNamespace::from_halves(9, 10))
        .expect("localized geometry edit");

    let mut expected = before;
    expected.splice(offset..offset + 4, b"123.5".iter().copied());
    assert_eq!(after.bytes(), expected);
    assert_eq!(changes.len(), 1, "{changes:#?}");
    assert_eq!(changes[0].schema_key, ELEMENT_SCHEMA_KEY);
    assert_eq!(changes[0].entity_pk, ["a"]);
    let changed: Value =
        serde_json::from_slice(changes[0].snapshot.as_ref().expect("upsert")).unwrap();
    assert!(
        changed
            .get("element_json")
            .and_then(Value::as_str)
            .unwrap()
            .contains("\"x\":123.5")
    );

    let after_records = record_map(&after);
    for id in ["a", "b"] {
        assert_eq!(
            snapshot_string(
                accepted
                    .get(&(ELEMENT_SCHEMA_KEY.to_owned(), id.to_owned()))
                    .unwrap(),
                "order_key"
            ),
            snapshot_string(
                after_records
                    .get(&(ELEMENT_SCHEMA_KEY.to_owned(), id.to_owned()))
                    .unwrap(),
                "order_key"
            )
        );
    }
}

#[test]
fn one_element_entity_update_renders_one_localized_splice() {
    let before = fixture();
    let document = open(&before);
    let mut element = record(&records(&document), ELEMENT_SCHEMA_KEY, "b").clone();
    let mut value = snapshot(&element);
    let raw = value
        .get("element_json")
        .and_then(Value::as_str)
        .unwrap()
        .replace("\"isDeleted\":false", "\"isDeleted\":true");
    value["element_json"] = Value::String(raw);
    value["is_deleted"] = Value::Bool(true);
    element.snapshot = serde_json::to_vec(&value).unwrap();

    let (after, edits) = document
        .entities_changed(&[EntityChange {
            schema_key: element.schema_key,
            entity_pk: element.entity_pk,
            snapshot: Some(element.snapshot),
            effect: ChangeEffect::Content,
        }])
        .expect("element entity update");

    assert_eq!(edits.len(), 1);
    assert!(
        edits[0].offset > 0,
        "localized edit must not replace the file"
    );
    assert!(edits[0].delete_len < u64::try_from(before.len()).unwrap());
    assert_eq!(apply_edits(&before, &edits), after.bytes());
    let parsed: Value = serde_json::from_slice(&after.bytes()).unwrap();
    assert_eq!(parsed["elements"][1]["isDeleted"], Value::Bool(true));
}

#[test]
fn one_file_entity_update_renders_one_localized_splice() {
    let before = fixture();
    let document = open(&before);
    let mut file = record(&records(&document), FILE_SCHEMA_KEY, "file-1").clone();
    let mut value = snapshot(&file);
    let raw = value
        .get("file_json")
        .and_then(Value::as_str)
        .unwrap()
        .replace("AA==", "AABBCC==");
    value["file_json"] = Value::String(raw);
    file.snapshot = serde_json::to_vec(&value).unwrap();

    let (after, edits) = document
        .entities_changed(&[EntityChange {
            schema_key: file.schema_key,
            entity_pk: file.entity_pk,
            snapshot: Some(file.snapshot),
            effect: ChangeEffect::Content,
        }])
        .expect("file entity update");
    assert_eq!(edits.len(), 1);
    assert!(edits[0].offset > 0);
    assert_eq!(apply_edits(&before, &edits), after.bytes());
    assert!(
        String::from_utf8(after.bytes())
            .unwrap()
            .contains("AABBCC==")
    );
}

#[test]
fn insertion_keeps_existing_native_ids_and_order_keys() {
    let before = fixture();
    let document = open(&before);
    let accepted = record_map(&document);
    let second = offset_of(&before, br#"{"id":"b""#);
    let insert = br#"{"id":"new","type":"line","x":3,"y":4,"isDeleted":false},
    "#;
    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(second).unwrap(),
                delete_len: 0,
                insert,
            }],
            IdNamespace::from_halves(11, 12),
        )
        .expect("insert element");
    let after_records = record_map(&after);
    assert!(after_records.contains_key(&(ELEMENT_SCHEMA_KEY.to_owned(), "new".to_owned())));
    for id in ["a", "b"] {
        assert_eq!(
            snapshot_string(
                accepted
                    .get(&(ELEMENT_SCHEMA_KEY.to_owned(), id.to_owned()))
                    .unwrap(),
                "order_key"
            ),
            snapshot_string(
                after_records
                    .get(&(ELEMENT_SCHEMA_KEY.to_owned(), id.to_owned()))
                    .unwrap(),
                "order_key"
            )
        );
    }
    assert!(
        changes.iter().any(|change| {
            change.schema_key == ELEMENT_SCHEMA_KEY && change.entity_pk == ["new"]
        }),
        "{changes:#?}"
    );
    let parsed: Value = serde_json::from_slice(&after.bytes()).unwrap();
    assert_eq!(parsed["elements"].as_array().unwrap().len(), 3);
}

#[test]
fn element_tombstone_preserves_other_entities_and_renders_valid_json() {
    let before = fixture();
    let document = open(&before);
    let (after, edits) = document
        .entities_changed(&[EntityChange {
            schema_key: ELEMENT_SCHEMA_KEY.to_owned(),
            entity_pk: vec!["a".to_owned()],
            snapshot: None,
            effect: ChangeEffect::Content,
        }])
        .expect("delete one element");
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].offset, 0, "structural edits use the full fallback");
    assert_eq!(apply_edits(&before, &edits), after.bytes());
    let parsed: Value = serde_json::from_slice(&after.bytes()).unwrap();
    let elements = parsed["elements"].as_array().unwrap();
    assert_eq!(elements.len(), 1);
    assert_eq!(elements[0]["id"], "b");
    assert_eq!(
        records(&after)
            .iter()
            .filter(|record| record.schema_key == FILE_SCHEMA_KEY)
            .count(),
        1
    );
}

#[test]
fn files_are_optional_but_file_entities_require_the_marker() {
    let bytes = br#"{"type":"excalidraw","version":2,"elements":[]}"#;
    let document = open(bytes);
    assert_eq!(document.bytes(), bytes);
    let mut imported = records(&document);
    imported.push(EntityRecord {
        schema_key: FILE_SCHEMA_KEY.to_owned(),
        entity_pk: vec!["x".to_owned()],
        snapshot: br#"{"file_json":"{}","id":"x","order_key":"80","prefix_json":"\"x\":"}"#
            .to_vec(),
    });
    let error = Document::open_entities(imported).unwrap_err();
    assert!(error.contains("files marker"), "{error}");
}

#[test]
fn malformed_or_inconsistent_entities_are_rejected() {
    let document = open(&fixture());
    let mut element = record(&records(&document), ELEMENT_SCHEMA_KEY, "a").clone();
    let mut value = snapshot(&element);
    value["element_type"] = Value::String("ellipse".to_owned());
    element.snapshot = serde_json::to_vec(&value).unwrap();
    let error = Document::open_entities(
        records(&document)
            .into_iter()
            .map(|record| {
                if record.schema_key == ELEMENT_SCHEMA_KEY && record.entity_pk == ["a"] {
                    element.clone()
                } else {
                    record
                }
            })
            .collect(),
    )
    .unwrap_err();
    assert!(error.contains("element_type"), "{error}");

    let duplicate = br#"{"type":"excalidraw","elements":[{"id":"x","type":"line"},{"id":"x","type":"line"}],"files":{}}"#;
    let error = Document::open_file(duplicate.to_vec(), None, namespace()).unwrap_err();
    assert!(error.contains("duplicate Excalidraw element id"), "{error}");
}

#[test]
fn fork_is_immutable_and_invalid_splices_do_not_mutate_the_document() {
    let before = fixture();
    let document = open(&before);
    let fork = document.fork();
    let error = fork
        .file_changed(
            &[
                InputSplice {
                    offset: 10,
                    delete_len: 3,
                    insert: b"x",
                },
                InputSplice {
                    offset: 9,
                    delete_len: 0,
                    insert: b"y",
                },
            ],
            namespace(),
        )
        .unwrap_err();
    assert!(error.contains("sorted and non-overlapping"), "{error}");
    assert_eq!(document.bytes(), before);
    assert_eq!(fork.bytes(), before);
}
