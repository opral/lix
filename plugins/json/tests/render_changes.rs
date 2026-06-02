mod common;

use common::snapshot_content;
use plugin_json_v2::{DetectedChange, PluginError, SCHEMA_KEY};
use serde_json::Value;

fn with_root_object(mut changes: Vec<DetectedChange>) -> Vec<DetectedChange> {
    if changes.iter().any(|change| change.entity_pk == [""]) {
        return changes;
    }

    let mut with_root = vec![DetectedChange {
        entity_pk: vec!["".to_string()],
        schema_key: SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content("", Value::Object(serde_json::Map::new()))),
        metadata: None,
    }];
    with_root.append(&mut changes);
    with_root
}

#[test]
fn applies_insert_update() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["/Name".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content(
                "/Name",
                Value::String("Samuel".to_string()),
            )),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/Age".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/Age", Value::Number(20.into()))),
            metadata: None,
        },
    ];

    let output = common::render_projection(with_root_object(changes))
        .expect("render_changes should succeed");

    let parsed: Value = serde_json::from_slice(&output).expect("output should be valid JSON");
    assert_eq!(parsed, serde_json::json!({"Name":"Samuel","Age":20}));
}

#[test]
fn applies_array_changes_with_indexes() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["/list".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/list", Value::Array(Vec::new()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/list/0".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/list/0", Value::String("a".to_string()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/list/1".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/list/1", Value::String("x".to_string()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/list/2".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/list/2", Value::String("c".to_string()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/list/3".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/list/3", Value::String("d".to_string()))),
            metadata: None,
        },
    ];

    let output = common::render_projection(with_root_object(changes))
        .expect("render_changes should succeed");

    let parsed: Value = serde_json::from_slice(&output).expect("output should be valid JSON");
    assert_eq!(parsed, serde_json::json!({"list":["a","x","c","d"]}));
}

#[test]
fn rejects_snapshot_missing_path() {
    let changes = vec![DetectedChange {
        entity_pk: vec!["/foo".to_string()],
        schema_key: SCHEMA_KEY.to_string(),
        snapshot_content: Some(r#"{"value":2}"#.to_string()),
        metadata: None,
    }];

    let error = common::render_projection(with_root_object(changes))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("must contain 'path'"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn infers_array_parent_for_numeric_pointer_segment() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["/team".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/team", Value::Array(Vec::new()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/team/0".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content(
                "/team/0",
                Value::Object(serde_json::Map::new()),
            )),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/team/0/name".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content(
                "/team/0/name",
                Value::String("Ada".to_string()),
            )),
            metadata: None,
        },
    ];

    let output = common::render_projection(with_root_object(changes))
        .expect("render_changes should succeed");
    let parsed: Value = serde_json::from_slice(&output).expect("output should parse");
    assert_eq!(parsed, serde_json::json!({"team":[{"name":"Ada"}]}));
}

#[test]
fn rejects_duplicate_entity_pks_in_projection_set() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["/foo".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/foo", Value::Number(1.into()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/foo".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/foo", Value::Number(2.into()))),
            metadata: None,
        },
    ];

    let error = common::render_projection(with_root_object(changes))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("duplicate entity_pk"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_mismatched_snapshot_path() {
    let changes = vec![DetectedChange {
        entity_pk: vec!["/foo".to_string()],
        schema_key: SCHEMA_KEY.to_string(),
        snapshot_content: Some(r#"{"path":"/bar","value":2}"#.to_string()),
        metadata: None,
    }];

    let error = common::render_projection(with_root_object(changes))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("snapshot path '/bar'"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_invalid_json_pointer_escape() {
    let changes = vec![DetectedChange {
        entity_pk: vec!["/foo/~2bar".to_string()],
        schema_key: SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content("/foo/~2bar", Value::Number(2.into()))),
        metadata: None,
    }];

    let error = common::render_projection(with_root_object(changes))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("invalid JSON pointer escape"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_invalid_dash_placement() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["/list".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/list", Value::Array(Vec::new()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/list/-/x".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content(
                "/list/-/x",
                Value::String("b".to_string()),
            )),
            metadata: None,
        },
    ];

    let error = common::render_projection(with_root_object(changes))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("non-canonical '-' array token"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn allows_proto_like_keys_when_projection_rows_are_consistent() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["/__proto__".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content(
                "/__proto__",
                Value::Object(serde_json::Map::new()),
            )),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/__proto__/x".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content(
                "/__proto__/x",
                Value::String("pwn".to_string()),
            )),
            metadata: None,
        },
    ];

    let output = common::render_projection(with_root_object(changes))
        .expect("render_changes should succeed");
    let parsed: Value = serde_json::from_slice(&output).expect("output should parse");
    assert_eq!(parsed, serde_json::json!({"__proto__":{"x":"pwn"}}));
}

#[test]
fn rejects_snapshot_path_non_string() {
    let changes = vec![DetectedChange {
        entity_pk: vec!["/safe".to_string()],
        schema_key: SCHEMA_KEY.to_string(),
        snapshot_content: Some(r#"{"path":123,"value":1}"#.to_string()),
        metadata: None,
    }];

    let error = common::render_projection(with_root_object(changes))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("must be a string"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_snapshot_with_additional_properties_or_missing_value() {
    let with_extra = vec![DetectedChange {
        entity_pk: vec!["/safe".to_string()],
        schema_key: SCHEMA_KEY.to_string(),
        snapshot_content: Some(r#"{"path":"/safe","value":1,"extra":true}"#.to_string()),
        metadata: None,
    }];
    let error = common::render_projection(with_root_object(with_extra))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("unsupported properties"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }

    let missing_value = vec![DetectedChange {
        entity_pk: vec!["/safe".to_string()],
        schema_key: SCHEMA_KEY.to_string(),
        snapshot_content: Some(r#"{"path":"/safe"}"#.to_string()),
        metadata: None,
    }];
    let error = common::render_projection(with_root_object(missing_value))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("must contain 'value'"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_numeric_child_without_parent_container_row() {
    let changes = vec![DetectedChange {
        entity_pk: vec!["/foo/0".to_string()],
        schema_key: SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content("/foo/0", Value::String("x".to_string()))),
        metadata: None,
    }];

    let error = common::render_projection(with_root_object(changes))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("missing ancestor container row"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_huge_array_index_growth() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["/arr".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/arr/100001".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content(
                "/arr/100001",
                Value::String("x".to_string()),
            )),
            metadata: None,
        },
    ];

    let error = common::render_projection(with_root_object(changes))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("exceeds max supported index"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_leading_zero_array_indices_under_array_ancestor() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["/arr".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/arr/01".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/arr/01", Value::String("A".to_string()))),
            metadata: None,
        },
    ];

    let error = common::render_projection(with_root_object(changes))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("non-canonical array index token"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn accepts_canonical_zero_array_index() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["/arr".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/arr/0".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/arr/0", Value::String("A".to_string()))),
            metadata: None,
        },
    ];

    let output = common::render_projection(with_root_object(changes))
        .expect("render_changes should succeed");
    let parsed: Value = serde_json::from_slice(&output).expect("output should parse");
    assert_eq!(parsed, serde_json::json!({"arr":["A"]}));
}

#[test]
fn rejects_sparse_array_projection_rows() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["/arr".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/arr/5".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/arr/5", Value::String("x".to_string()))),
            metadata: None,
        },
    ];

    let error = common::render_projection(with_root_object(changes))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("sparse array projection"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_aliasing_array_indices_via_non_canonical_form() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["/arr".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/arr/1".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/arr/1", Value::String("A".to_string()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/arr/01".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/arr/01", Value::String("B".to_string()))),
            metadata: None,
        },
    ];

    let error = common::render_projection(with_root_object(changes))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("non-canonical array index token"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_root_scalar_with_non_root_descendants() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("", Value::Number(7.into()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/a".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/a", Value::Number(1.into()))),
            metadata: None,
        },
    ];

    let error = common::render_projection(with_root_object(changes))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("is not a container"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_scalar_ancestor_with_descendant_row() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["/a".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/a", Value::Number(1.into()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/a/b".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/a/b", Value::Number(2.into()))),
            metadata: None,
        },
    ];

    let error = common::render_projection(with_root_object(changes))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("is not a container"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_final_dash_token_in_projection_rows() {
    let changes = vec![
        DetectedChange {
            entity_pk: vec!["/arr".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/arr", Value::Array(Vec::new()))),
            metadata: None,
        },
        DetectedChange {
            entity_pk: vec!["/arr/-".to_string()],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content("/arr/-", Value::String("x".to_string()))),
            metadata: None,
        },
    ];

    let error = common::render_projection(with_root_object(changes))
        .expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("non-canonical '-' array token"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}

#[test]
fn rejects_non_root_rows_when_root_row_is_missing() {
    let changes = vec![DetectedChange {
        entity_pk: vec!["/0".to_string()],
        schema_key: SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content("/0", Value::String("x".to_string()))),
        metadata: None,
    }];

    let error = common::render_projection(changes).expect_err("render_changes should fail");
    match error {
        PluginError::InvalidInput(message) => {
            assert!(message.contains("non-root projection rows require a root row"));
        }
        PluginError::Internal(message) => {
            panic!("expected InvalidInput, got Internal({message})");
        }
    }
}
