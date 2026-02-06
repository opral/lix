use serde_json::Value as JsonValue;
use std::sync::OnceLock;

use crate::schema::lix_schema_definition;

const LIX_STORED_SCHEMA_KEY: &str = "lix_stored_schema";
const LIX_KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const LIX_CHANGE_SCHEMA_KEY: &str = "lix_change";
const LIX_CHANGE_SET_SCHEMA_KEY: &str = "lix_change_set";
const LIX_COMMIT_SCHEMA_KEY: &str = "lix_commit";
const LIX_VERSION_TIP_SCHEMA_KEY: &str = "lix_version_tip";
const LIX_CHANGE_SET_ELEMENT_SCHEMA_KEY: &str = "lix_change_set_element";
const LIX_COMMIT_EDGE_SCHEMA_KEY: &str = "lix_commit_edge";

const LIX_STORED_SCHEMA_JSON: &str = include_str!("lix_stored_schema.json");
const LIX_KEY_VALUE_SCHEMA_JSON: &str = include_str!("lix_key_value.json");
const LIX_CHANGE_SCHEMA_JSON: &str = include_str!("lix_change.json");
const LIX_CHANGE_SET_SCHEMA_JSON: &str = include_str!("lix_change_set.json");
const LIX_COMMIT_SCHEMA_JSON: &str = include_str!("lix_commit.json");
const LIX_VERSION_TIP_SCHEMA_JSON: &str = include_str!("lix_version_tip.json");
const LIX_CHANGE_SET_ELEMENT_SCHEMA_JSON: &str = include_str!("lix_change_set_element.json");
const LIX_COMMIT_EDGE_SCHEMA_JSON: &str = include_str!("lix_commit_edge.json");

static LIX_STORED_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_KEY_VALUE_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_CHANGE_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_CHANGE_SET_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_COMMIT_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_VERSION_TIP_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_CHANGE_SET_ELEMENT_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_COMMIT_EDGE_SCHEMA: OnceLock<JsonValue> = OnceLock::new();

const BUILTIN_SCHEMA_KEYS: &[&str] = &[
    LIX_STORED_SCHEMA_KEY,
    LIX_KEY_VALUE_SCHEMA_KEY,
    LIX_CHANGE_SCHEMA_KEY,
    LIX_CHANGE_SET_SCHEMA_KEY,
    LIX_COMMIT_SCHEMA_KEY,
    LIX_VERSION_TIP_SCHEMA_KEY,
    LIX_CHANGE_SET_ELEMENT_SCHEMA_KEY,
    LIX_COMMIT_EDGE_SCHEMA_KEY,
];

pub(crate) fn builtin_schema_keys() -> &'static [&'static str] {
    BUILTIN_SCHEMA_KEYS
}

pub(crate) fn builtin_schema_definition(schema_key: &str) -> Option<&'static JsonValue> {
    match schema_key {
        LIX_STORED_SCHEMA_KEY => {
            Some(LIX_STORED_SCHEMA.get_or_init(|| parse_stored_schema_with_inlined_definition()))
        }
        LIX_KEY_VALUE_SCHEMA_KEY => {
            Some(LIX_KEY_VALUE_SCHEMA.get_or_init(|| {
                parse_builtin_schema("lix_key_value.json", LIX_KEY_VALUE_SCHEMA_JSON)
            }))
        }
        LIX_CHANGE_SCHEMA_KEY => Some(
            LIX_CHANGE_SCHEMA
                .get_or_init(|| parse_builtin_schema("lix_change.json", LIX_CHANGE_SCHEMA_JSON)),
        ),
        LIX_CHANGE_SET_SCHEMA_KEY => Some(LIX_CHANGE_SET_SCHEMA.get_or_init(|| {
            parse_builtin_schema("lix_change_set.json", LIX_CHANGE_SET_SCHEMA_JSON)
        })),
        LIX_COMMIT_SCHEMA_KEY => Some(
            LIX_COMMIT_SCHEMA
                .get_or_init(|| parse_builtin_schema("lix_commit.json", LIX_COMMIT_SCHEMA_JSON)),
        ),
        LIX_VERSION_TIP_SCHEMA_KEY => Some(LIX_VERSION_TIP_SCHEMA.get_or_init(|| {
            parse_builtin_schema("lix_version_tip.json", LIX_VERSION_TIP_SCHEMA_JSON)
        })),
        LIX_CHANGE_SET_ELEMENT_SCHEMA_KEY => {
            Some(LIX_CHANGE_SET_ELEMENT_SCHEMA.get_or_init(|| {
                parse_builtin_schema(
                    "lix_change_set_element.json",
                    LIX_CHANGE_SET_ELEMENT_SCHEMA_JSON,
                )
            }))
        }
        LIX_COMMIT_EDGE_SCHEMA_KEY => Some(LIX_COMMIT_EDGE_SCHEMA.get_or_init(|| {
            parse_builtin_schema("lix_commit_edge.json", LIX_COMMIT_EDGE_SCHEMA_JSON)
        })),
        _ => None,
    }
}

#[allow(dead_code)]
pub(crate) fn builtin_schema_json(schema_key: &str) -> Option<&'static str> {
    match schema_key {
        LIX_STORED_SCHEMA_KEY => Some(LIX_STORED_SCHEMA_JSON),
        LIX_KEY_VALUE_SCHEMA_KEY => Some(LIX_KEY_VALUE_SCHEMA_JSON),
        LIX_CHANGE_SCHEMA_KEY => Some(LIX_CHANGE_SCHEMA_JSON),
        LIX_CHANGE_SET_SCHEMA_KEY => Some(LIX_CHANGE_SET_SCHEMA_JSON),
        LIX_COMMIT_SCHEMA_KEY => Some(LIX_COMMIT_SCHEMA_JSON),
        LIX_VERSION_TIP_SCHEMA_KEY => Some(LIX_VERSION_TIP_SCHEMA_JSON),
        LIX_CHANGE_SET_ELEMENT_SCHEMA_KEY => Some(LIX_CHANGE_SET_ELEMENT_SCHEMA_JSON),
        LIX_COMMIT_EDGE_SCHEMA_KEY => Some(LIX_COMMIT_EDGE_SCHEMA_JSON),
        _ => None,
    }
}

fn parse_builtin_schema(file_name: &str, raw_json: &str) -> JsonValue {
    serde_json::from_str(raw_json).unwrap_or_else(|error| {
        panic!("builtin schema file '{file_name}' must contain valid JSON: {error}")
    })
}

fn parse_stored_schema_with_inlined_definition() -> JsonValue {
    let mut schema = parse_builtin_schema("lix_stored_schema.json", LIX_STORED_SCHEMA_JSON);
    let value_schema = schema
        .pointer_mut("/properties/value")
        .expect("lix_stored_schema.json must define /properties/value");
    let value_schema_object = value_schema
        .as_object_mut()
        .expect("lix_stored_schema.json /properties/value must be an object");

    value_schema_object.insert(
        "allOf".to_string(),
        JsonValue::Array(vec![lix_schema_definition().clone()]),
    );

    schema
}

#[cfg(test)]
mod tests {
    use super::{builtin_schema_definition, BUILTIN_SCHEMA_KEYS};

    #[test]
    fn builtin_schemas_use_lix_plugin_key_override() {
        for schema_key in BUILTIN_SCHEMA_KEYS {
            let schema = builtin_schema_definition(schema_key).expect("schema should exist");
            let plugin_key = schema
                .get("x-lix-override-lixcols")
                .and_then(|value| value.as_object())
                .and_then(|map| map.get("lixcol_plugin_key"))
                .and_then(|value| value.as_str());
            assert_eq!(
                plugin_key,
                Some("\"lix\""),
                "schema '{}' must override lixcol_plugin_key to 'lix'",
                schema_key
            );
        }
    }

    #[test]
    fn stored_schema_value_inlines_lix_schema_definition() {
        let schema = builtin_schema_definition("lix_stored_schema").expect("schema should exist");
        let all_of = schema
            .pointer("/properties/value/allOf")
            .and_then(|value| value.as_array())
            .expect("stored schema value must define allOf array");
        assert_eq!(all_of.len(), 1);
        assert_eq!(all_of[0], *crate::schema::lix_schema_definition());
    }
}
