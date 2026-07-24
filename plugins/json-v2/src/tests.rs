use crate::core::{
    ARRAY_ITEM_SCHEMA_KEY, ChangeEffect, Document, EntityChange, EntityRecord, IdNamespace,
    InputSplice, OBJECT_MEMBER_SCHEMA_KEY, ROOT_SCHEMA_KEY,
};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::io::Write as _;

const ROOT_ID: &str = "root";
const LARGE_PROPERTY_COUNT: usize = 220_000;
const LARGE_TARGET_BYTES: usize = 10_000_000;

type EntityKey = (String, Vec<String>);

fn namespace() -> IdNamespace {
    namespace_from(1, 2)
}

fn namespace_from(high: u64, low: u64) -> IdNamespace {
    IdNamespace::from_halves(high, low)
}

fn open_with(bytes: &[u8], ids: IdNamespace) -> Document {
    Document::open_file(bytes.to_vec(), Some("document.json"), ids)
        .expect("open canonical JSON")
        .0
}

fn open(bytes: &[u8]) -> Document {
    open_with(bytes, namespace())
}

fn initial_records(document: &Document) -> Vec<EntityRecord> {
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

fn entity_key(record: &EntityRecord) -> EntityKey {
    (record.schema_key.clone(), record.entity_pk.clone())
}

fn change_key(change: &EntityChange) -> EntityKey {
    (change.schema_key.clone(), change.entity_pk.clone())
}

fn snapshot(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("valid entity snapshot")
}

fn field<'a>(value: &'a Value, name: &str) -> &'a str {
    value
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("snapshot field {name:?} must be a string"))
}

fn record_snapshot(record: &EntityRecord) -> Value {
    snapshot(&record.snapshot)
}

fn object_member<'a>(records: &'a [EntityRecord], parent_id: &str, key: &str) -> &'a EntityRecord {
    records
        .iter()
        .find(|record| {
            if record.schema_key != OBJECT_MEMBER_SCHEMA_KEY {
                return false;
            }
            let value = record_snapshot(record);
            field(&value, "parent_id") == parent_id && field(&value, "key") == key
        })
        .unwrap_or_else(|| panic!("missing object member ({parent_id:?}, {key:?})"))
}

fn child_container_id(records: &[EntityRecord], parent_id: &str, key: &str) -> String {
    let value = record_snapshot(object_member(records, parent_id, key));
    field(&value, "container_id").to_owned()
}

fn array_item_ids(records: &[EntityRecord], parent_id: &str) -> Vec<String> {
    let mut items = records
        .iter()
        .filter_map(|record| {
            if record.schema_key != ARRAY_ITEM_SCHEMA_KEY {
                return None;
            }
            let value = record_snapshot(record);
            (field(&value, "parent_id") == parent_id).then(|| {
                (
                    field(&value, "order_key").to_owned(),
                    field(&value, "id").to_owned(),
                )
            })
        })
        .collect::<Vec<_>>();
    items.sort_unstable();
    items.into_iter().map(|(_, id)| id).collect()
}

fn named_array_items(document: &Document, root_key: &str) -> Vec<(String, String)> {
    let records = initial_records(document);
    let parent_id = child_container_id(&records, ROOT_ID, root_key);
    array_item_ids(&records, &parent_id)
        .into_iter()
        .map(|id| {
            let member = record_snapshot(object_member(&records, &id, "name"));
            let name = serde_json::from_str::<String>(field(&member, "scalar_json"))
                .expect("name scalar is a JSON string");
            (id, name)
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

fn apply_edits(before: &[u8], edits: &[crate::core::ByteEdit]) -> Vec<u8> {
    let mut rendered = Vec::new();
    let mut cursor = 0usize;
    for edit in edits {
        let offset = usize::try_from(edit.offset).expect("edit offset fits usize");
        let delete_len = usize::try_from(edit.delete_len).expect("edit length fits usize");
        assert!(offset >= cursor, "output edits use sorted base coordinates");
        assert!(offset + delete_len <= before.len());
        rendered.extend_from_slice(&before[cursor..offset]);
        rendered.extend_from_slice(&edit.insert);
        cursor = offset + delete_len;
    }
    rendered.extend_from_slice(&before[cursor..]);
    rendered
}

#[test]
fn ax_01_existing_duplicate_array_items_keep_opaque_ids() {
    let bytes = br#"{"a":0,"rows":[{"name":"x"},{"name":"x"}]}"#;
    let document = open(bytes);
    let records = initial_records(&document);
    let rows_id = child_container_id(&records, ROOT_ID, "rows");
    let accepted_ids = array_item_ids(&records, &rows_id);
    assert_eq!(accepted_ids.len(), 2);
    assert_ne!(accepted_ids[0], accepted_ids[1]);

    let (reopened, edit) =
        Document::open_entities(records).expect("cold-open acknowledged identities");
    assert_eq!(edit.insert.as_slice(), bytes);
    let reopened_records = initial_records(&reopened);
    let reopened_rows_id = child_container_id(&reopened_records, ROOT_ID, "rows");
    assert_eq!(
        array_item_ids(&reopened_records, &reopened_rows_id),
        accepted_ids
    );
}

#[test]
fn cold_open_preserves_pretty_layout_escaped_keys_and_outer_whitespace() {
    let bytes = b" \n{\n  \"\\u0061\" : [\n    1  ,\n    { \"b\" : true }\n  ],\n  \"empty\" : { \n }\n}\t\r\n";
    let document = open(bytes);
    let records = initial_records(&document);

    let root = record_snapshot(
        records
            .iter()
            .find(|record| record.schema_key == ROOT_SCHEMA_KEY)
            .expect("root entity"),
    );
    assert_eq!(field(&root, "prefix_json"), " \n");
    assert_eq!(field(&root, "suffix_json"), "\t\r\n");
    let escaped = record_snapshot(object_member(&records, ROOT_ID, "a"));
    assert_eq!(field(&escaped, "prefix_json"), "\n  \"\\u0061\" : ");

    let empty = object_member(&records, ROOT_ID, "empty");
    assert_eq!(field(&record_snapshot(empty), "empty_json"), " \n ");

    let (reopened, edit) =
        Document::open_entities(records.clone()).expect("cold-open lossless JSON layout");
    assert_eq!(edit.offset, 0);
    assert_eq!(edit.delete_len, 0);
    assert_eq!(edit.insert.as_slice(), bytes);
    assert_eq!(reopened.bytes(), bytes);
    assert_eq!(initial_records(&reopened), records);
}

#[test]
fn compact_json_omits_layout_fields_and_import_validates_raw_keys() {
    let mut records = initial_records(&open(br#"{"a":[1,{"b":2}]}"#));
    assert!(records.iter().all(|record| {
        let value = record_snapshot(record);
        value.get("prefix_json").is_none()
            && value.get("suffix_json").is_none()
            && value.get("empty_json").is_none()
    }));

    let member = records
        .iter_mut()
        .find(|record| record.entity_pk == [ROOT_ID, "a"])
        .expect("root object member a");
    let mut value = record_snapshot(member);
    value["prefix_json"] = json!("\"different-key\":");
    member.snapshot = serde_json::to_vec(&value).expect("serialize invalid layout");

    let error = Document::open_entities(records)
        .expect_err("layout must not be able to change the entity's decoded object key");
    assert!(
        error.contains("prefix_json key does not match"),
        "unexpected error: {error}"
    );
}

#[test]
fn formatting_only_file_edits_are_sparse_durable_and_cold_exact() {
    let before = br#"{"a":1,"b":2}"#;
    let document = open(before);

    let (spaced, changes) = document
        .file_changed(
            &[InputSplice {
                offset: 1,
                delete_len: 0,
                insert: b" ",
            }],
            namespace_from(70, 71),
        )
        .expect("insert object layout whitespace");
    assert_eq!(spaced.bytes(), br#"{ "a":1,"b":2}"#);
    assert_eq!(changes.len(), 1, "{changes:#?}");
    assert_eq!(changes[0].schema_key, OBJECT_MEMBER_SCHEMA_KEY);
    assert_eq!(changes[0].entity_pk, [ROOT_ID, "a"]);
    assert_eq!(changes[0].effect, ChangeEffect::FormatOnly);

    let key_offset = offset_of(&spaced.bytes(), br#""a""#);
    let (escaped, changes) = spaced
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(key_offset).expect("offset fits u64"),
                delete_len: 3,
                insert: br#""\u0061""#,
            }],
            namespace_from(72, 73),
        )
        .expect("change only object key spelling");
    assert_eq!(escaped.bytes(), br#"{ "\u0061":1,"b":2}"#);
    assert_eq!(changes.len(), 1, "{changes:#?}");
    assert_eq!(changes[0].entity_pk, [ROOT_ID, "a"]);
    assert_eq!(changes[0].effect, ChangeEffect::FormatOnly);

    let escaped_bytes = escaped.bytes();
    let (outer, changes) = escaped
        .file_changed(
            &[
                InputSplice {
                    offset: 0,
                    delete_len: 0,
                    insert: b"\n",
                },
                InputSplice {
                    offset: u64::try_from(escaped_bytes.len()).expect("length fits u64"),
                    delete_len: 0,
                    insert: b"\n",
                },
            ],
            namespace_from(74, 75),
        )
        .expect("change only outer whitespace");
    assert_eq!(outer.bytes(), b"\n{ \"\\u0061\":1,\"b\":2}\n");
    assert_eq!(changes.len(), 1, "{changes:#?}");
    assert_eq!(changes[0].schema_key, ROOT_SCHEMA_KEY);
    assert_eq!(changes[0].effect, ChangeEffect::FormatOnly);

    let records = initial_records(&outer);
    let (reopened, edit) =
        Document::open_entities(records.clone()).expect("cold-open formatting-only successor");
    assert_eq!(edit.insert.as_slice(), outer.bytes());
    assert_eq!(reopened.bytes(), outer.bytes());
    assert_eq!(initial_records(&reopened), records);
}

#[test]
fn ax_02_leaf_edit_emits_one_existing_object_member_upsert() {
    let before = br#"{"a":0,"rows":[{"name":"x"},{"name":"x"}]}"#;
    let document = open(before);
    let accepted = initial_records(&document);
    let accepted_key = entity_key(object_member(&accepted, ROOT_ID, "a"));
    let offset = offset_of(before, b"0");
    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(offset).expect("offset fits u64"),
                delete_len: 1,
                insert: b"1",
            }],
            namespace_from(3, 4),
        )
        .expect("localized scalar edit");

    assert_eq!(
        after.bytes(),
        br#"{"a":1,"rows":[{"name":"x"},{"name":"x"}]}"#
    );
    assert_eq!(changes.len(), 1);
    assert_eq!(change_key(&changes[0]), accepted_key);
    let value = snapshot(changes[0].snapshot.as_ref().expect("upsert"));
    assert_eq!(field(&value, "scalar_json"), "1");
}

#[test]
fn ax_03_front_array_insert_allocates_one_id_and_preserves_suffix_ids() {
    let before = br#"{"rows":[{"name":"x"},{"name":"x"}]}"#;
    let document = open(before);
    let accepted = named_array_items(&document, "rows");
    let accepted_ids = accepted
        .iter()
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    let offset = offset_of(before, br#""rows":["#) + br#""rows":["#.len();
    let splice = [InputSplice {
        offset: u64::try_from(offset).expect("offset fits u64"),
        delete_len: 0,
        insert: br#"{"name":"z"},"#,
    }];
    let retry_namespace = namespace_from(5, 6);

    let (first, first_changes) = document
        .file_changed(&splice, retry_namespace)
        .expect("front insertion");
    let (retried, retry_changes) = document
        .file_changed(&splice, retry_namespace)
        .expect("retry front insertion");
    let first_items = named_array_items(&first, "rows");
    let retried_items = named_array_items(&retried, "rows");

    assert_eq!(
        first_items
            .iter()
            .map(|(_, name)| name.as_str())
            .collect::<Vec<_>>(),
        ["z", "x", "x"]
    );
    assert_eq!(first_items[1].0, accepted_ids[0]);
    assert_eq!(first_items[2].0, accepted_ids[1]);
    assert_eq!(retried_items, first_items);

    let accepted_set = accepted_ids.into_iter().collect::<HashSet<_>>();
    let new_array_ids = |changes: &[EntityChange]| {
        changes
            .iter()
            .filter(|change| {
                change.schema_key == ARRAY_ITEM_SCHEMA_KEY
                    && change.snapshot.is_some()
                    && !accepted_set.contains(&change.entity_pk[0])
            })
            .map(|change| change.entity_pk[0].clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        new_array_ids(&first_changes),
        vec![first_items[0].0.clone()]
    );
    assert_eq!(
        new_array_ids(&retry_changes),
        vec![first_items[0].0.clone()]
    );
}

#[test]
fn identical_front_array_insert_preserves_acknowledged_ids_as_suffix() {
    let before = br#"{"rows":[{"name":"x"},{"name":"x"}]}"#;
    let expected = br#"{"rows":[{"name":"x"},{"name":"x"},{"name":"x"}]}"#;
    let document = open(before);
    let records = initial_records(&document);
    let rows_id = child_container_id(&records, ROOT_ID, "rows");
    let accepted_ids = array_item_ids(&records, &rows_id);
    let offset = offset_of(before, br#""rows":["#) + br#""rows":["#.len();

    let (after, _) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(offset).expect("offset fits u64"),
                delete_len: 0,
                insert: br#"{"name":"x"},"#,
            }],
            namespace_from(22, 23),
        )
        .expect("insert an identical item at the front");
    let after_records = initial_records(&after);
    let after_rows_id = child_container_id(&after_records, ROOT_ID, "rows");
    let after_ids = array_item_ids(&after_records, &after_rows_id);

    assert_eq!(after.bytes(), expected);
    assert_eq!(after_ids.len(), 3);
    assert!(!accepted_ids.contains(&after_ids[0]));
    assert_eq!(&after_ids[1..], accepted_ids.as_slice());
}

#[test]
fn deleting_first_identical_array_item_preserves_second_identity() {
    let before = br#"{"rows":[{"name":"x"},{"name":"x"}]}"#;
    let expected = br#"{"rows":[{"name":"x"}]}"#;
    let document = open(before);
    let records = initial_records(&document);
    let rows_id = child_container_id(&records, ROOT_ID, "rows");
    let accepted_ids = array_item_ids(&records, &rows_id);
    let offset = offset_of(before, br#""rows":["#) + br#""rows":["#.len();

    let (after, _) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(offset).expect("offset fits u64"),
                delete_len: u64::try_from(br#"{"name":"x"},"#.len()).expect("length fits u64"),
                insert: b"",
            }],
            namespace_from(24, 25),
        )
        .expect("delete the first duplicate");
    let after_records = initial_records(&after);
    let after_rows_id = child_container_id(&after_records, ROOT_ID, "rows");

    assert_eq!(after.bytes(), expected);
    assert_eq!(
        array_item_ids(&after_records, &after_rows_id),
        [accepted_ids[1].clone()]
    );
}

#[test]
fn multi_splice_array_insert_and_edit_preserve_corresponding_ids() {
    let before = br#"{"rows":["a","b"]}"#;
    let expected = br#"{"rows":["x","a","c"]}"#;
    let document = open(before);
    let records = initial_records(&document);
    let rows_id = child_container_id(&records, ROOT_ID, "rows");
    let accepted_ids = array_item_ids(&records, &rows_id);
    let insert_offset = offset_of(before, br#""rows":["#) + br#""rows":["#.len();
    let edit_offset = offset_of(before, br#""b""#) + 1;

    let (after, _) = document
        .file_changed(
            &[
                InputSplice {
                    offset: u64::try_from(insert_offset).expect("offset fits u64"),
                    delete_len: 0,
                    insert: br#""x","#,
                },
                InputSplice {
                    offset: u64::try_from(edit_offset).expect("offset fits u64"),
                    delete_len: 1,
                    insert: b"c",
                },
            ],
            namespace_from(34, 35),
        )
        .expect("insert one array item and edit a later item");
    let after_records = initial_records(&after);
    let after_rows_id = child_container_id(&after_records, ROOT_ID, "rows");
    let after_ids = array_item_ids(&after_records, &after_rows_id);

    assert_eq!(after.bytes(), expected);
    assert_eq!(after_ids.len(), 3);
    assert!(!accepted_ids.contains(&after_ids[0]));
    assert_eq!(after_ids[1], accepted_ids[0], "a keeps its old ID");
    assert_eq!(
        after_ids[2], accepted_ids[1],
        "b becoming c keeps its old ID"
    );
}

#[test]
fn array_scalar_becoming_object_preserves_id_and_cold_opens() {
    let before = br#"{"rows":[1]}"#;
    let expected = br#"{"rows":[{"x":1}]}"#;
    let document = open(before);
    let records = initial_records(&document);
    let rows_id = child_container_id(&records, ROOT_ID, "rows");
    let accepted_ids = array_item_ids(&records, &rows_id);
    let offset = offset_of(before, b"1");

    let (after, _) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(offset).expect("offset fits u64"),
                delete_len: 1,
                insert: br#"{"x":1}"#,
            }],
            namespace_from(30, 31),
        )
        .expect("change an array scalar into an object");
    let after_records = initial_records(&after);
    let after_rows_id = child_container_id(&after_records, ROOT_ID, "rows");

    assert_eq!(after.bytes(), expected);
    assert_eq!(array_item_ids(&after_records, &after_rows_id), accepted_ids);
    assert_eq!(
        field(
            &record_snapshot(object_member(&after_records, &accepted_ids[0], "x")),
            "scalar_json"
        ),
        "1"
    );

    let (reopened, edit) =
        Document::open_entities(after_records.clone()).expect("cold-open changed graph");
    assert_eq!(edit.insert.as_slice(), expected);
    assert_eq!(reopened.bytes(), expected);
    assert_eq!(initial_records(&reopened), after_records);
}

#[test]
fn array_empty_object_gaining_nested_object_preserves_id_and_cold_opens() {
    let before = br#"{"rows":[{}]}"#;
    let expected = br#"{"rows":[{"new":{"x":1}}]}"#;
    let document = open(before);
    let records = initial_records(&document);
    let rows_id = child_container_id(&records, ROOT_ID, "rows");
    let accepted_ids = array_item_ids(&records, &rows_id);
    let offset = offset_of(before, b"{}");

    let (after, _) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(offset).expect("offset fits u64"),
                delete_len: 2,
                insert: br#"{"new":{"x":1}}"#,
            }],
            namespace_from(32, 33),
        )
        .expect("add a nested object below an array item");
    let after_records = initial_records(&after);
    let after_rows_id = child_container_id(&after_records, ROOT_ID, "rows");
    let new_id = child_container_id(&after_records, &accepted_ids[0], "new");

    assert_eq!(after.bytes(), expected);
    assert_eq!(array_item_ids(&after_records, &after_rows_id), accepted_ids);
    assert_eq!(
        field(
            &record_snapshot(object_member(&after_records, &new_id, "x")),
            "scalar_json"
        ),
        "1"
    );

    let (reopened, edit) =
        Document::open_entities(after_records.clone()).expect("cold-open nested graph");
    assert_eq!(edit.insert.as_slice(), expected);
    assert_eq!(reopened.bytes(), expected);
    assert_eq!(initial_records(&reopened), after_records);
}

#[test]
fn ax_04_array_move_changes_order_not_identity() {
    let before = br#"{"rows":[{"name":"a"},{"name":"b"},{"name":"c"}]}"#;
    let expected = br#"{"rows":[{"name":"b"},{"name":"c"},{"name":"a"}]}"#;
    let document = open(before);
    let accepted = named_array_items(&document, "rows");
    let accepted_by_name = accepted
        .iter()
        .map(|(id, name)| (name.clone(), id.clone()))
        .collect::<HashMap<_, _>>();
    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: 0,
                delete_len: u64::try_from(before.len()).expect("length fits u64"),
                insert: expected,
            }],
            namespace_from(7, 8),
        )
        .expect("move array item");

    let moved = named_array_items(&after, "rows");
    assert_eq!(
        moved
            .iter()
            .map(|(_, name)| name.as_str())
            .collect::<Vec<_>>(),
        ["b", "c", "a"]
    );
    for (id, name) in &moved {
        assert_eq!(accepted_by_name[name], *id);
    }
    assert!(changes.iter().any(|change| {
        change.schema_key == ARRAY_ITEM_SCHEMA_KEY
            && change.snapshot.is_some()
            && accepted_by_name
                .values()
                .any(|id| id == &change.entity_pk[0])
    }));
    assert!(
        changes
            .iter()
            .filter(|change| change.schema_key == ARRAY_ITEM_SCHEMA_KEY)
            .all(|change| accepted_by_name
                .values()
                .any(|id| id == &change.entity_pk[0]))
    );
}

#[test]
fn ax_05_container_delete_tombstones_acknowledged_subtree() {
    let before = br#"{"keep":1,"gone":{"nested":[{"x":1},{"x":2}]}}"#;
    let expected = br#"{"keep":1}"#;
    let document = open(before);
    let accepted = initial_records(&document);
    let retained = [
        (ROOT_SCHEMA_KEY.to_owned(), vec![ROOT_ID.to_owned()]),
        entity_key(object_member(&accepted, ROOT_ID, "keep")),
    ]
    .into_iter()
    .collect::<HashSet<_>>();
    let removed = accepted
        .iter()
        .map(entity_key)
        .filter(|key| !retained.contains(key))
        .collect::<HashSet<_>>();
    assert!(removed.len() >= 6, "fixture must contain a real subtree");

    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: 0,
                delete_len: u64::try_from(before.len()).expect("length fits u64"),
                insert: expected,
            }],
            namespace_from(9, 10),
        )
        .expect("delete nested container");
    let tombstones = changes
        .iter()
        .filter(|change| change.snapshot.is_none())
        .map(change_key)
        .collect::<HashSet<_>>();

    assert_eq!(after.bytes(), expected);
    assert_eq!(tombstones, removed);
}

#[test]
fn ax_06_decoded_slash_and_tilde_keys_are_distinct_and_render() {
    let bytes = br#"{"a/b":1,"a~b":2,"~1":3,"/":4}"#;
    let document = open(bytes);
    let records = initial_records(&document);
    let keys = records
        .iter()
        .filter(|record| record.schema_key == OBJECT_MEMBER_SCHEMA_KEY)
        .filter_map(|record| {
            let value = record_snapshot(record);
            (field(&value, "parent_id") == ROOT_ID).then(|| field(&value, "key").to_owned())
        })
        .collect::<HashSet<_>>();
    assert_eq!(
        keys,
        ["a/b", "a~b", "~1", "/"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    );

    let (reopened, edit) = Document::open_entities(records).expect("cold render escaped keys");
    assert_eq!(edit.insert.as_slice(), bytes);
    assert_eq!(reopened.bytes(), bytes);
}

#[test]
fn ax_07_committed_leaf_change_renders_exact_semantic_json() {
    let before = br#"{"a":{"b":0},"other":true}"#;
    let expected = br#"{"a":{"b":7},"other":true}"#;
    let document = open(before);
    let records = initial_records(&document);
    let a_id = child_container_id(&records, ROOT_ID, "a");
    let b = object_member(&records, &a_id, "b");
    let mut b_snapshot = record_snapshot(b);
    b_snapshot["scalar_json"] = json!("7");
    let (after, edits) = document
        .entities_changed(&[EntityChange {
            schema_key: b.schema_key.clone(),
            entity_pk: b.entity_pk.clone(),
            snapshot: Some(serde_json::to_vec(&b_snapshot).expect("serialize leaf snapshot")),
            effect: ChangeEffect::Content,
        }])
        .expect("apply committed leaf update");

    assert_eq!(
        serde_json::from_slice::<Value>(&after.bytes()).expect("valid rendered JSON"),
        json!({"a": {"b": 7}, "other": true})
    );
    assert_eq!(after.bytes(), expected);
    assert_eq!(apply_edits(before, &edits), expected);
}

#[test]
fn semantic_scalar_kind_change_stays_sparse() {
    let before = br#"{"value":"one","other":true}"#;
    let expected = br#"{"value":7,"other":true}"#;
    let document = open(before);
    let records = initial_records(&document);
    let value = object_member(&records, ROOT_ID, "value");
    let mut value_snapshot = record_snapshot(value);
    value_snapshot["kind"] = json!("number");
    value_snapshot["scalar_json"] = json!("7");

    let (after, edits) = document
        .entities_changed(&[EntityChange {
            schema_key: value.schema_key.clone(),
            entity_pk: value.entity_pk.clone(),
            snapshot: Some(serde_json::to_vec(&value_snapshot).expect("serialize scalar")),
            effect: ChangeEffect::Content,
        }])
        .expect("scalar kind changes remain direct semantic value changes");

    assert_eq!(after.bytes(), expected);
    assert_eq!(apply_edits(before, &edits), expected);
    assert_eq!(edits.len(), 1);
}

#[test]
fn ax_08_failed_or_discarded_transition_keeps_accepted_base() {
    let before = br#"{"a":0}"#;
    let document = open(before);
    let accepted_records = initial_records(&document);
    let offset = offset_of(before, b"0");

    assert!(
        document
            .file_changed(
                &[InputSplice {
                    offset: u64::try_from(offset).expect("offset fits u64"),
                    delete_len: 1,
                    insert: b"]",
                }],
                namespace_from(11, 12),
            )
            .is_err()
    );
    assert_eq!(document.bytes(), before);
    assert_eq!(initial_records(&document), accepted_records);

    let (discarded, _) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(offset).expect("offset fits u64"),
                delete_len: 1,
                insert: b"1",
            }],
            namespace_from(13, 14),
        )
        .expect("construct discardable successor");
    assert_eq!(discarded.bytes(), br#"{"a":1}"#);
    assert_eq!(document.bytes(), before);
    assert_eq!(initial_records(&document), accepted_records);
}

#[test]
fn length_changing_scalar_then_later_array_edit_keeps_spans_and_ids_correct() {
    let before = br#"{"lead":"x","rows":[{"name":"a"},{"name":"b"}]}"#;
    let after_scalar_bytes = br#"{"lead":"expanded","rows":[{"name":"a"},{"name":"b"}]}"#;
    let expected = br#"{"lead":"expanded","rows":[{"name":"z"},{"name":"a"},{"name":"b"}]}"#;
    let document = open(before);
    let accepted_items = named_array_items(&document, "rows");
    let accepted_ids = accepted_items
        .iter()
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    let scalar_offset = offset_of(before, br#""x""#);

    let (after_scalar, scalar_changes) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(scalar_offset).expect("offset fits u64"),
                delete_len: 3,
                insert: br#""expanded""#,
            }],
            namespace_from(26, 27),
        )
        .expect("length-changing scalar edit");
    assert_eq!(after_scalar.bytes(), after_scalar_bytes);
    assert_eq!(scalar_changes.len(), 1);

    let array_offset = offset_of(after_scalar_bytes, br#""rows":["#) + br#""rows":["#.len();
    let (after_array, _) = after_scalar
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(array_offset).expect("offset fits u64"),
                delete_len: 0,
                insert: br#"{"name":"z"},"#,
            }],
            namespace_from(28, 29),
        )
        .expect("structural edit after the shifted scalar");
    let after_items = named_array_items(&after_array, "rows");

    assert_eq!(after_array.bytes(), expected);
    assert_eq!(
        after_items
            .iter()
            .map(|(_, name)| name.as_str())
            .collect::<Vec<_>>(),
        ["z", "a", "b"]
    );
    assert_eq!(after_items[1].0, accepted_ids[0]);
    assert_eq!(after_items[2].0, accepted_ids[1]);
}

#[test]
fn semantic_array_order_change_is_rejected_and_leaves_bytes_unchanged() {
    let before = br#"{"rows":[1,2]}"#;
    let document = open(before);
    let records = initial_records(&document);
    let rows_id = child_container_id(&records, ROOT_ID, "rows");
    let accepted_ids = array_item_ids(&records, &rows_id);
    let first = records
        .iter()
        .find(|record| {
            record.schema_key == ARRAY_ITEM_SCHEMA_KEY
                && record.entity_pk.len() == 1
                && record.entity_pk[0] == accepted_ids[0]
        })
        .expect("first scalar array entity");
    let mut moved_snapshot = record_snapshot(first);
    moved_snapshot["order_key"] = json!("ffffffffffffffff");

    let error = document
        .entities_changed(&[EntityChange {
            schema_key: first.schema_key.clone(),
            entity_pk: first.entity_pk.clone(),
            snapshot: Some(
                serde_json::to_vec(&moved_snapshot).expect("serialize moved item snapshot"),
            ),
            effect: ChangeEffect::Content,
        }])
        .expect_err("direct semantic moves must use an authoritative byte write");

    assert!(error.contains("one existing scalar value only"));
    assert_eq!(document.bytes(), before);
    assert_eq!(
        array_item_ids(&initial_records(&document), &rows_id),
        accepted_ids
    );
}

#[test]
fn semantic_structural_changes_and_stale_scalar_replays_are_rejected() {
    let before = br#"{"gone":1,"keep":2,"container":{"child":3}}"#;
    let document = open(before);
    let records = initial_records(&document);
    let gone = object_member(&records, ROOT_ID, "gone");
    let container = object_member(&records, ROOT_ID, "container");

    let delete_error = document
        .entities_changed(&[EntityChange {
            schema_key: gone.schema_key.clone(),
            entity_pk: gone.entity_pk.clone(),
            snapshot: None,
            effect: ChangeEffect::Content,
        }])
        .expect_err("direct semantic deletes must use an authoritative byte write");
    assert!(delete_error.contains("one existing scalar value only"));

    let container_error = document
        .entities_changed(&[EntityChange {
            schema_key: container.schema_key.clone(),
            entity_pk: container.entity_pk.clone(),
            snapshot: Some(container.snapshot.clone()),
            effect: ChangeEffect::Content,
        }])
        .expect_err("direct container changes must use an authoritative byte write");
    assert!(container_error.contains("one existing scalar value only"));

    let after_bytes = br#"{"keep":2,"container":{"child":3}}"#;
    let (after, _) = document
        .file_changed(
            &[InputSplice {
                offset: 0,
                delete_len: u64::try_from(before.len()).expect("fixture length fits u64"),
                insert: after_bytes,
            }],
            namespace_from(41, 42),
        )
        .expect("a file write owns structural deletion");
    let mut stale_gone = record_snapshot(gone);
    stale_gone["scalar_json"] = json!("7");
    let stale_error = after
        .entities_changed(&[EntityChange {
            schema_key: gone.schema_key.clone(),
            entity_pk: gone.entity_pk.clone(),
            snapshot: Some(serde_json::to_vec(&stale_gone).expect("serialize stale scalar")),
            effect: ChangeEffect::Content,
        }])
        .expect_err("a stale scalar must not resurrect a byte-deleted node");
    assert!(stale_error.contains("one existing scalar value only"));
    assert_eq!(after.bytes(), after_bytes);
}

#[test]
fn vertical_tab_and_form_feed_are_not_json_whitespace() {
    for bytes in [b"{\x0b\"a\":1}".as_slice(), b"{\"a\":\x0c1}".as_slice()] {
        assert!(
            Document::open_file(bytes.to_vec(), Some("invalid.json"), namespace()).is_err(),
            "invalid JSON whitespace byte was accepted: {bytes:?}"
        );
    }
}

#[test]
#[ignore = "large-file acceptance gate"]
fn flat_ten_megabyte_leaf_edit_is_one_sparse_change() {
    let (before, edit_offset) = flat_object_fixture(LARGE_TARGET_BYTES);
    let document = open(&before);
    let replacement = alternate_ascii_hex(before[edit_offset]);
    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(edit_offset).expect("offset fits u64"),
                delete_len: 1,
                insert: &[replacement],
            }],
            namespace_from(15, 16),
        )
        .expect("flat 10 MB sparse leaf edit");

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].schema_key, OBJECT_MEMBER_SCHEMA_KEY);
    assert_eq!(
        changes[0].entity_pk,
        [ROOT_ID.to_owned(), "property_110000".to_owned()]
    );
    assert_eq!(after.bytes().len(), LARGE_TARGET_BYTES);
    assert_eq!(after.bytes()[edit_offset], replacement);
}

#[test]
#[ignore = "large-file acceptance gate"]
fn nested_ten_megabyte_leaf_edit_is_one_sparse_change() {
    const WRAPPER_PREFIX: &[u8] = br#"{"outer":"#;
    const WRAPPER_SUFFIX: &[u8] = b"}";
    let inner_target = LARGE_TARGET_BYTES - WRAPPER_PREFIX.len() - WRAPPER_SUFFIX.len();
    let (inner, inner_edit_offset) = flat_object_fixture(inner_target);
    let mut before = Vec::with_capacity(LARGE_TARGET_BYTES);
    before.extend_from_slice(WRAPPER_PREFIX);
    before.extend_from_slice(&inner);
    before.extend_from_slice(WRAPPER_SUFFIX);
    assert_eq!(before.len(), LARGE_TARGET_BYTES);
    let edit_offset = WRAPPER_PREFIX.len() + inner_edit_offset;
    let replacement = alternate_ascii_hex(before[edit_offset]);

    let document = open(&before);
    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(edit_offset).expect("offset fits u64"),
                delete_len: 1,
                insert: &[replacement],
            }],
            namespace_from(17, 18),
        )
        .expect("nested 10 MB sparse leaf edit");

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].schema_key, OBJECT_MEMBER_SCHEMA_KEY);
    assert_eq!(changes[0].entity_pk.len(), 2);
    assert_ne!(changes[0].entity_pk[0], ROOT_ID);
    assert_eq!(changes[0].entity_pk[1], "property_110000");
    assert_eq!(after.bytes().len(), LARGE_TARGET_BYTES);
    assert_eq!(after.bytes()[edit_offset], replacement);
}

#[test]
fn retry_namespace_reuses_id_and_distinct_namespace_does_not() {
    let before = br#"{"rows":[]}"#;
    let document = open(before);
    let offset = offset_of(before, b"[]") + 1;
    let splice = [InputSplice {
        offset: u64::try_from(offset).expect("offset fits u64"),
        delete_len: 0,
        insert: b"1",
    }];
    let retry_namespace = namespace_from(19, 20);
    let distinct_namespace = namespace_from(19, 21);

    let (first, first_changes) = document
        .file_changed(&splice, retry_namespace)
        .expect("first insertion");
    let (retry, retry_changes) = document
        .file_changed(&splice, retry_namespace)
        .expect("retried insertion");
    let (distinct, distinct_changes) = document
        .file_changed(&splice, distinct_namespace)
        .expect("distinct insertion");
    let inserted_id = |changes: &[EntityChange]| {
        let change = changes
            .iter()
            .find(|change| change.schema_key == ARRAY_ITEM_SCHEMA_KEY)
            .expect("array item upsert");
        assert!(change.snapshot.is_some());
        assert_eq!(change.entity_pk.len(), 1);
        change.entity_pk[0].clone()
    };
    let first_id = inserted_id(&first_changes);
    let retry_id = inserted_id(&retry_changes);
    let distinct_id = inserted_id(&distinct_changes);

    assert_eq!(first.bytes(), br#"{"rows":[1]}"#);
    assert_eq!(retry.bytes(), first.bytes());
    assert_eq!(distinct.bytes(), first.bytes());
    assert_eq!(retry_id, first_id);
    assert_ne!(distinct_id, first_id);
    assert_eq!(first_id.len(), 32);
    assert!(
        first_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    );
}

fn flat_object_fixture(target_bytes: usize) -> (Vec<u8>, usize) {
    const BASE_MEMBER_BYTES: usize = 44;
    let base_bytes = 2 + LARGE_PROPERTY_COUNT * BASE_MEMBER_BYTES + LARGE_PROPERTY_COUNT - 1;
    let padding = target_bytes
        .checked_sub(base_bytes)
        .expect("target accommodates fixed fixture members");
    assert!(padding <= LARGE_PROPERTY_COUNT);

    let mut bytes = Vec::with_capacity(target_bytes);
    let mut state = 0x6a73_6f6e_2d31_306du64;
    let middle = LARGE_PROPERTY_COUNT / 2;
    let mut edit_offset = None;
    bytes.push(b'{');
    for index in 0..LARGE_PROPERTY_COUNT {
        if index > 0 {
            bytes.push(b',');
        }
        state = splitmix64(state);
        let first = state;
        state = splitmix64(state);
        let second = u32::try_from(state & u64::from(u32::MAX)).expect("masked");
        write!(
            &mut bytes,
            "\"property_{index:06}\":\"{first:016x}{second:08x}"
        )
        .expect("write property");
        if index == middle {
            edit_offset = Some(bytes.len() - 24);
        }
        if index < padding {
            bytes.push(b'f');
        }
        bytes.push(b'"');
    }
    bytes.push(b'}');
    assert_eq!(bytes.len(), target_bytes);
    (bytes, edit_offset.expect("middle edit offset"))
}

fn alternate_ascii_hex(byte: u8) -> u8 {
    if byte == b'0' { b'1' } else { b'0' }
}

fn splitmix64(mut state: u64) -> u64 {
    state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
