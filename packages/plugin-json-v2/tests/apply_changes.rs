mod common;

use common::{file_from_json, snapshot_content};
use plugin_json_v2::{
    apply_changes, PluginApiError, PluginEntityChange, SCHEMA_KEY, SCHEMA_VERSION,
};
use serde_json::Value;

fn with_root_object(mut changes: Vec<PluginEntityChange>) -> Vec<PluginEntityChange> {
    if changes.iter().any(|change| change.entity_id.is_empty()) {
        return changes;
    }

    let mut with_root = vec![PluginEntityChange {
        entity_id: "".to_string(),
        schema_key: SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(snapshot_content("", Value::Object(serde_json::Map::new()))),
    }];
    with_root.append(&mut changes);
    with_root
}

#[test]
fn applies_insert_update_delete() {
    let file = file_from_json("f1", "/x.json", r#"{"stale":"cache"}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/Name".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content(
                "/Name",
                Value::String("Samuel".to_string()),
            )),
        },
        PluginEntityChange {
            entity_id: "/Age".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/Age", Value::Number(20.into()))),
        },
        PluginEntityChange {
            entity_id: "/City".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: None,
        },
    ];

    let output =
        apply_changes(file, with_root_object(changes)).expect("apply_changes should succeed");

    let parsed: Value = serde_json::from_slice(&output).expect("output should be valid JSON");
    assert_eq!(parsed, serde_json::json!({"Name":"Samuel","Age":20}));
}

#[test]
fn applies_array_changes_with_indexes() {
    let file = file_from_json("f1", "/x.json", r#"{"stale":"cache"}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/list".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/list", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/list/0".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/list/0", Value::String("a".to_string()))),
        },
        PluginEntityChange {
            entity_id: "/list/1".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/list/1", Value::String("x".to_string()))),
        },
        PluginEntityChange {
            entity_id: "/list/2".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/list/2", Value::String("c".to_string()))),
        },
        PluginEntityChange {
            entity_id: "/list/3".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/list/3", Value::String("d".to_string()))),
        },
    ];

    let output =
        apply_changes(file, with_root_object(changes)).expect("apply_changes should succeed");

    let parsed: Value = serde_json::from_slice(&output).expect("output should be valid JSON");
    assert_eq!(parsed, serde_json::json!({"list":["a","x","c","d"]}));
}

#[test]
fn rejects_snapshot_missing_path() {
    let file = file_from_json("f1", "/x.json", r#"{"foo":1}"#);
    let changes = vec![PluginEntityChange {
        entity_id: "/foo".to_string(),
        schema_key: SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(r#"{"value":2}"#.to_string()),
    }];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("must contain 'path'"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn infers_array_parent_for_numeric_pointer_segment() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/team".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/team", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/team/0".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content(
                "/team/0",
                Value::Object(serde_json::Map::new()),
            )),
        },
        PluginEntityChange {
            entity_id: "/team/0/name".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content(
                "/team/0/name",
                Value::String("Ada".to_string()),
            )),
        },
    ];

    let output =
        apply_changes(file, with_root_object(changes)).expect("apply_changes should succeed");
    let parsed: Value = serde_json::from_slice(&output).expect("output should parse");
    assert_eq!(parsed, serde_json::json!({"team":[{"name":"Ada"}]}));
}

#[test]
fn removing_root_sets_null() {
    let file = file_from_json("f1", "/x.json", r#"{"foo":1}"#);
    let changes = vec![PluginEntityChange {
        entity_id: "".to_string(),
        schema_key: SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: None,
    }];

    let output =
        apply_changes(file, with_root_object(changes)).expect("apply_changes should succeed");
    let parsed: Value = serde_json::from_slice(&output).expect("output should parse");
    assert_eq!(parsed, Value::Null);
}

#[test]
fn rejects_duplicate_entity_ids_in_projection_set() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/foo".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/foo", Value::Number(1.into()))),
        },
        PluginEntityChange {
            entity_id: "/foo".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/foo", Value::Number(2.into()))),
        },
    ];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("duplicate entity_id"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_mismatched_snapshot_path() {
    let file = file_from_json("f1", "/x.json", r#"{"foo":1}"#);
    let changes = vec![PluginEntityChange {
        entity_id: "/foo".to_string(),
        schema_key: SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(r#"{"path":"/bar","value":2}"#.to_string()),
    }];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("snapshot path '/bar'"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_schema_version_mismatch() {
    let file = file_from_json("f1", "/x.json", r#"{"foo":1}"#);
    let changes = vec![PluginEntityChange {
        entity_id: "/foo".to_string(),
        schema_key: SCHEMA_KEY.to_string(),
        schema_version: "999".to_string(),
        snapshot_content: Some(snapshot_content("/foo", Value::Number(2.into()))),
    }];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("unsupported schema_version"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_invalid_json_pointer_escape() {
    let file = file_from_json("f1", "/x.json", r#"{"foo":1}"#);
    let changes = vec![PluginEntityChange {
        entity_id: "/foo/~2bar".to_string(),
        schema_key: SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(snapshot_content("/foo/~2bar", Value::Number(2.into()))),
    }];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("invalid JSON pointer escape"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_invalid_dash_placement() {
    let file = file_from_json("f1", "/x.json", r#"{"list":[{"x":"a"}]}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/list".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/list", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/list/-/x".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content(
                "/list/-/x",
                Value::String("b".to_string()),
            )),
        },
    ];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("non-canonical '-' array token"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn allows_proto_like_keys_when_projection_rows_are_consistent() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/__proto__".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content(
                "/__proto__",
                Value::Object(serde_json::Map::new()),
            )),
        },
        PluginEntityChange {
            entity_id: "/__proto__/x".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content(
                "/__proto__/x",
                Value::String("pwn".to_string()),
            )),
        },
    ];

    let output =
        apply_changes(file, with_root_object(changes)).expect("apply_changes should succeed");
    let parsed: Value = serde_json::from_slice(&output).expect("output should parse");
    assert_eq!(parsed, serde_json::json!({"__proto__":{"x":"pwn"}}));
}

#[test]
fn rejects_descendant_upsert_under_tombstoned_ancestor() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/a".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: None,
        },
        PluginEntityChange {
            entity_id: "/a/b".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/a/b", Value::Number(1.into()))),
        },
    ];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("conflicts with tombstoned ancestor"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_root_tombstone_with_non_root_rows() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: None,
        },
        PluginEntityChange {
            entity_id: "/a".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/a", Value::Number(1.into()))),
        },
    ];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("root tombstone cannot coexist"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_snapshot_path_non_string() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![PluginEntityChange {
        entity_id: "/safe".to_string(),
        schema_key: SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(r#"{"path":123,"value":1}"#.to_string()),
    }];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("must be a string"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_snapshot_with_additional_properties_or_missing_value() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);

    let with_extra = vec![PluginEntityChange {
        entity_id: "/safe".to_string(),
        schema_key: SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(r#"{"path":"/safe","value":1,"extra":true}"#.to_string()),
    }];
    let error = apply_changes(file.clone(), with_root_object(with_extra))
        .expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("unsupported properties"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }

    let missing_value = vec![PluginEntityChange {
        entity_id: "/safe".to_string(),
        schema_key: SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(r#"{"path":"/safe"}"#.to_string()),
    }];
    let error = apply_changes(file, with_root_object(missing_value))
        .expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("must contain 'value'"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_numeric_child_without_parent_container_row() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![PluginEntityChange {
        entity_id: "/foo/0".to_string(),
        schema_key: SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(snapshot_content("/foo/0", Value::String("x".to_string()))),
    }];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("missing ancestor container row"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_huge_array_index_growth() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/arr".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/arr/100001".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content(
                "/arr/100001",
                Value::String("x".to_string()),
            )),
        },
    ];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("exceeds max supported index"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_leading_zero_array_indices_under_array_ancestor() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/arr".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/arr/01".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr/01", Value::String("A".to_string()))),
        },
    ];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("non-canonical array index token"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn accepts_canonical_zero_array_index() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/arr".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/arr/0".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr/0", Value::String("A".to_string()))),
        },
    ];

    let output =
        apply_changes(file, with_root_object(changes)).expect("apply_changes should succeed");
    let parsed: Value = serde_json::from_slice(&output).expect("output should parse");
    assert_eq!(parsed, serde_json::json!({"arr":["A"]}));
}

#[test]
fn rejects_sparse_array_projection_rows() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/arr".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/arr/5".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr/5", Value::String("x".to_string()))),
        },
    ];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("sparse array projection"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_aliasing_array_indices_via_non_canonical_form() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/arr".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/arr/1".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr/1", Value::String("A".to_string()))),
        },
        PluginEntityChange {
            entity_id: "/arr/01".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr/01", Value::String("B".to_string()))),
        },
    ];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("non-canonical array index token"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_tombstone_with_leading_zero_token_under_live_array_context() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/arr".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/arr/0".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr/0", Value::String("A".to_string()))),
        },
        PluginEntityChange {
            entity_id: "/arr/01".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: None,
        },
    ];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("non-canonical array index token"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_tombstone_with_dash_token_under_live_array_context() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/arr".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/arr/0".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr/0", Value::String("A".to_string()))),
        },
        PluginEntityChange {
            entity_id: "/arr/-".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: None,
        },
    ];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("non-canonical '-' array token"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn allows_tombstone_with_leading_zero_token_with_only_live_array_container() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/arr".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/arr/00".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: None,
        },
    ];

    let output =
        apply_changes(file, with_root_object(changes)).expect("apply_changes should succeed");
    let parsed: Value = serde_json::from_slice(&output).expect("output should parse");
    assert_eq!(parsed, serde_json::json!({"arr":[]}));
}

#[test]
fn allows_tombstone_with_dash_token_with_only_live_array_container() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/arr".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/arr/-".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: None,
        },
    ];

    let output =
        apply_changes(file, with_root_object(changes)).expect("apply_changes should succeed");
    let parsed: Value = serde_json::from_slice(&output).expect("output should parse");
    assert_eq!(parsed, serde_json::json!({"arr":[]}));
}

#[test]
fn rejects_live_array_row_with_non_canonical_tombstone_alias() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/arr".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/arr/0".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr/0", Value::Null)),
        },
        PluginEntityChange {
            entity_id: "/arr/1".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr/1", Value::String("B".to_string()))),
        },
        PluginEntityChange {
            entity_id: "/arr/01".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: None,
        },
    ];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("non-canonical array index token"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn allows_tombstone_non_numeric_token_under_live_array_context() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/arr".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/arr/0".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr/0", Value::Null)),
        },
        PluginEntityChange {
            entity_id: "/arr/foo".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: None,
        },
    ];

    let output =
        apply_changes(file, with_root_object(changes)).expect("apply_changes should succeed");
    let parsed: Value = serde_json::from_slice(&output).expect("output should parse");
    assert_eq!(parsed, serde_json::json!({"arr":[null]}));
}

#[test]
fn rejects_root_scalar_with_non_root_descendants() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("", Value::Number(7.into()))),
        },
        PluginEntityChange {
            entity_id: "/a".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/a", Value::Number(1.into()))),
        },
    ];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("is not a container"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_scalar_ancestor_with_descendant_row() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/a".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/a", Value::Number(1.into()))),
        },
        PluginEntityChange {
            entity_id: "/a/b".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/a/b", Value::Number(2.into()))),
        },
    ];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("is not a container"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_final_dash_token_in_projection_rows() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![
        PluginEntityChange {
            entity_id: "/arr".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
        },
        PluginEntityChange {
            entity_id: "/arr/-".to_string(),
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content("/arr/-", Value::String("x".to_string()))),
        },
    ];

    let error =
        apply_changes(file, with_root_object(changes)).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("non-canonical '-' array token"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_non_root_rows_when_root_row_is_missing() {
    let file = file_from_json("f1", "/x.json", r#"{}"#);
    let changes = vec![PluginEntityChange {
        entity_id: "/0".to_string(),
        schema_key: SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(snapshot_content("/0", Value::String("x".to_string()))),
    }];

    let error = apply_changes(file, changes).expect_err("apply_changes should fail");
    match error {
        PluginApiError::InvalidInput(message) => {
            assert!(message.contains("non-root projection rows require a root row"));
        }
        PluginApiError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}
