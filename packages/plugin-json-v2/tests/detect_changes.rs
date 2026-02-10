mod common;

use common::{file_from_json, parse_snapshot_value_from_change};
use plugin_json_v2::{detect_changes, SCHEMA_KEY, SCHEMA_VERSION};
use serde_json::Value;

#[test]
fn returns_empty_when_documents_are_equal() {
    let before = file_from_json("f1", "/x.json", r#"{"Name":"Anna","Age":20}"#);
    let after = file_from_json("f1", "/x.json", r#"{"Name":"Anna","Age":20}"#);

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert!(changes.is_empty());
}

#[test]
fn detects_root_insert() {
    let before = file_from_json("f1", "/x.json", r#"{"Name":"Anna","Age":20}"#);
    let after = file_from_json(
        "f1",
        "/x.json",
        r#"{"Name":"Anna","Age":20,"City":"New York"}"#,
    );

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].entity_id, "/City");
    assert_eq!(changes[0].schema_key, SCHEMA_KEY);
    assert_eq!(changes[0].schema_version, SCHEMA_VERSION);
    assert_eq!(
        parse_snapshot_value_from_change(&changes[0]),
        Value::String("New York".to_string())
    );
}

#[test]
fn detects_nested_array_updates_and_deletions() {
    let before = file_from_json("f1", "/x.json", r#"{"list":["a","b","c"]}"#);
    let after = file_from_json("f1", "/x.json", r#"{"list":["a","x"]}"#);

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].entity_id, "/list/1");
    assert_eq!(
        parse_snapshot_value_from_change(&changes[0]),
        Value::String("x".to_string())
    );
    assert_eq!(changes[1].entity_id, "/list/2");
    assert_eq!(changes[1].snapshot_content, None);
}

#[test]
fn detects_container_replacement() {
    let before = file_from_json("f1", "/x.json", r#"{"a":{"x":1}}"#);
    let after = file_from_json("f1", "/x.json", r#"{"a":2}"#);

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].entity_id, "/a/x");
    assert_eq!(changes[0].snapshot_content, None);
    assert_eq!(changes[1].entity_id, "/a");
    assert_eq!(
        parse_snapshot_value_from_change(&changes[1]),
        Value::Number(2.into())
    );
}

#[test]
fn handles_file_creation_without_synthetic_root_deletion() {
    let after = file_from_json("f1", "/x.json", r#"{"Name":"Anna"}"#);

    let changes = detect_changes(None, after).expect("detect_changes should succeed");

    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].entity_id, "");
    assert_eq!(
        parse_snapshot_value_from_change(&changes[0]),
        Value::Object(serde_json::Map::new())
    );
    assert_eq!(changes[1].entity_id, "/Name");
    assert_eq!(
        parse_snapshot_value_from_change(&changes[1]),
        Value::String("Anna".to_string())
    );
}

#[test]
fn detects_multi_delete_array_in_descending_order() {
    let before = file_from_json("f1", "/x.json", r#"{"list":["a","b","c","d"]}"#);
    let after = file_from_json("f1", "/x.json", r#"{"list":["a"]}"#);

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(changes.len(), 3);
    assert_eq!(changes[0].entity_id, "/list/3");
    assert_eq!(changes[0].snapshot_content, None);
    assert_eq!(changes[1].entity_id, "/list/2");
    assert_eq!(changes[1].snapshot_content, None);
    assert_eq!(changes[2].entity_id, "/list/1");
    assert_eq!(changes[2].snapshot_content, None);
}

#[test]
fn deleting_non_empty_container_emits_subtree_tombstones() {
    let before = file_from_json("f1", "/x.json", r#"{"a":{"b":1}}"#);
    let after = file_from_json("f1", "/x.json", r#"{}"#);

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].entity_id, "/a");
    assert_eq!(changes[0].snapshot_content, None);
    assert_eq!(changes[1].entity_id, "/a/b");
    assert_eq!(changes[1].snapshot_content, None);
}

#[test]
fn replacing_non_empty_container_with_scalar_tombstones_subtree() {
    let before = file_from_json("f1", "/x.json", r#"{"a":{"b":1}}"#);
    let after = file_from_json("f1", "/x.json", r#"2"#);

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(changes.len(), 3);
    assert_eq!(changes[0].entity_id, "/a");
    assert_eq!(changes[0].snapshot_content, None);
    assert_eq!(changes[1].entity_id, "/a/b");
    assert_eq!(changes[1].snapshot_content, None);
    assert_eq!(changes[2].entity_id, "");
    assert_eq!(
        parse_snapshot_value_from_change(&changes[2]),
        Value::Number(2.into())
    );
}

#[test]
fn deleting_whole_object_property_emits_subtree_tombstones() {
    let before = file_from_json(
        "f1",
        "/x.json",
        r#"{"keep":1,"obj":{"k":1,"nested":{"z":2}}}"#,
    );
    let after = file_from_json("f1", "/x.json", r#"{"keep":1}"#);

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");
    let mut entity_ids = changes
        .iter()
        .map(|change| change.entity_id.as_str())
        .collect::<Vec<_>>();
    entity_ids.sort_unstable();

    assert_eq!(
        entity_ids,
        vec!["/obj", "/obj/k", "/obj/nested", "/obj/nested/z"]
    );
    assert!(changes
        .iter()
        .all(|change| change.snapshot_content.is_none()));
}

#[test]
fn deleting_whole_array_property_emits_subtree_tombstones() {
    let before = file_from_json("f1", "/x.json", r#"{"keep":1,"arr":[{"x":1},2,3]}"#);
    let after = file_from_json("f1", "/x.json", r#"{"keep":1}"#);

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");
    let mut entity_ids = changes
        .iter()
        .map(|change| change.entity_id.as_str())
        .collect::<Vec<_>>();
    entity_ids.sort_unstable();

    assert_eq!(
        entity_ids,
        vec!["/arr", "/arr/0", "/arr/0/x", "/arr/1", "/arr/2"]
    );
    assert!(changes
        .iter()
        .all(|change| change.snapshot_content.is_none()));
}

#[test]
fn deleting_nested_subtree_emits_all_descendant_tombstones() {
    let before = file_from_json("f1", "/x.json", r#"{"a":{"b":{"c":1,"d":2},"e":3},"x":0}"#);
    let after = file_from_json("f1", "/x.json", r#"{"a":{"e":3},"x":0}"#);

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");
    let mut entity_ids = changes
        .iter()
        .map(|change| change.entity_id.as_str())
        .collect::<Vec<_>>();
    entity_ids.sort_unstable();

    assert_eq!(entity_ids, vec!["/a/b", "/a/b/c", "/a/b/d"]);
    assert!(changes
        .iter()
        .all(|change| change.snapshot_content.is_none()));
}
