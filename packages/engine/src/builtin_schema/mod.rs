use serde_json::Value as JsonValue;
use std::sync::OnceLock;

use crate::schema::lix_schema_definition;

pub(crate) mod types;

const LIX_STORED_SCHEMA_KEY: &str = "lix_stored_schema";
const LIX_KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const LIX_ACCOUNT_SCHEMA_KEY: &str = "lix_account";
const LIX_ACTIVE_ACCOUNT_SCHEMA_KEY: &str = "lix_active_account";
const LIX_CHANGE_SCHEMA_KEY: &str = "lix_change";
const LIX_CHANGE_AUTHOR_SCHEMA_KEY: &str = "lix_change_author";
const LIX_CHANGE_SET_SCHEMA_KEY: &str = "lix_change_set";
const LIX_COMMIT_SCHEMA_KEY: &str = "lix_commit";
const LIX_VERSION_DESCRIPTOR_SCHEMA_KEY: &str = "lix_version_descriptor";
const LIX_VERSION_POINTER_SCHEMA_KEY: &str = "lix_version_pointer";
const LIX_ACTIVE_VERSION_SCHEMA_KEY: &str = "lix_active_version";
const LIX_CHANGE_SET_ELEMENT_SCHEMA_KEY: &str = "lix_change_set_element";
const LIX_COMMIT_EDGE_SCHEMA_KEY: &str = "lix_commit_edge";
const LIX_FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const LIX_DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

const LIX_STORED_SCHEMA_JSON: &str = include_str!("lix_stored_schema.json");
const LIX_KEY_VALUE_SCHEMA_JSON: &str = include_str!("lix_key_value.json");
const LIX_ACCOUNT_SCHEMA_JSON: &str = include_str!("lix_account.json");
const LIX_ACTIVE_ACCOUNT_SCHEMA_JSON: &str = include_str!("lix_active_account.json");
const LIX_CHANGE_SCHEMA_JSON: &str = include_str!("lix_change.json");
const LIX_CHANGE_AUTHOR_SCHEMA_JSON: &str = include_str!("lix_change_author.json");
const LIX_CHANGE_SET_SCHEMA_JSON: &str = include_str!("lix_change_set.json");
const LIX_COMMIT_SCHEMA_JSON: &str = include_str!("lix_commit.json");
const LIX_VERSION_DESCRIPTOR_SCHEMA_JSON: &str = include_str!("lix_version_descriptor.json");
const LIX_VERSION_POINTER_SCHEMA_JSON: &str = include_str!("lix_version_pointer.json");
const LIX_ACTIVE_VERSION_SCHEMA_JSON: &str = include_str!("lix_active_version.json");
const LIX_CHANGE_SET_ELEMENT_SCHEMA_JSON: &str = include_str!("lix_change_set_element.json");
const LIX_COMMIT_EDGE_SCHEMA_JSON: &str = include_str!("lix_commit_edge.json");
const LIX_FILE_DESCRIPTOR_SCHEMA_JSON: &str = include_str!("lix_file_descriptor.json");
const LIX_DIRECTORY_DESCRIPTOR_SCHEMA_JSON: &str = include_str!("lix_directory_descriptor.json");

static LIX_STORED_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_KEY_VALUE_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_ACCOUNT_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_ACTIVE_ACCOUNT_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_CHANGE_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_CHANGE_AUTHOR_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_CHANGE_SET_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_COMMIT_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_VERSION_DESCRIPTOR_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_VERSION_POINTER_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_ACTIVE_VERSION_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_CHANGE_SET_ELEMENT_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_COMMIT_EDGE_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_FILE_DESCRIPTOR_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_DIRECTORY_DESCRIPTOR_SCHEMA: OnceLock<JsonValue> = OnceLock::new();

const BUILTIN_SCHEMA_KEYS: &[&str] = &[
    LIX_STORED_SCHEMA_KEY,
    LIX_KEY_VALUE_SCHEMA_KEY,
    LIX_ACCOUNT_SCHEMA_KEY,
    LIX_ACTIVE_ACCOUNT_SCHEMA_KEY,
    LIX_CHANGE_SCHEMA_KEY,
    LIX_CHANGE_AUTHOR_SCHEMA_KEY,
    LIX_CHANGE_SET_SCHEMA_KEY,
    LIX_COMMIT_SCHEMA_KEY,
    LIX_VERSION_DESCRIPTOR_SCHEMA_KEY,
    LIX_VERSION_POINTER_SCHEMA_KEY,
    LIX_ACTIVE_VERSION_SCHEMA_KEY,
    LIX_CHANGE_SET_ELEMENT_SCHEMA_KEY,
    LIX_COMMIT_EDGE_SCHEMA_KEY,
    LIX_FILE_DESCRIPTOR_SCHEMA_KEY,
    LIX_DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
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
        LIX_ACCOUNT_SCHEMA_KEY => Some(
            LIX_ACCOUNT_SCHEMA
                .get_or_init(|| parse_builtin_schema("lix_account.json", LIX_ACCOUNT_SCHEMA_JSON)),
        ),
        LIX_ACTIVE_ACCOUNT_SCHEMA_KEY => Some(LIX_ACTIVE_ACCOUNT_SCHEMA.get_or_init(|| {
            parse_builtin_schema("lix_active_account.json", LIX_ACTIVE_ACCOUNT_SCHEMA_JSON)
        })),
        LIX_CHANGE_SCHEMA_KEY => Some(
            LIX_CHANGE_SCHEMA
                .get_or_init(|| parse_builtin_schema("lix_change.json", LIX_CHANGE_SCHEMA_JSON)),
        ),
        LIX_CHANGE_AUTHOR_SCHEMA_KEY => Some(LIX_CHANGE_AUTHOR_SCHEMA.get_or_init(|| {
            parse_builtin_schema("lix_change_author.json", LIX_CHANGE_AUTHOR_SCHEMA_JSON)
        })),
        LIX_CHANGE_SET_SCHEMA_KEY => Some(LIX_CHANGE_SET_SCHEMA.get_or_init(|| {
            parse_builtin_schema("lix_change_set.json", LIX_CHANGE_SET_SCHEMA_JSON)
        })),
        LIX_COMMIT_SCHEMA_KEY => Some(
            LIX_COMMIT_SCHEMA
                .get_or_init(|| parse_builtin_schema("lix_commit.json", LIX_COMMIT_SCHEMA_JSON)),
        ),
        LIX_VERSION_DESCRIPTOR_SCHEMA_KEY => {
            Some(LIX_VERSION_DESCRIPTOR_SCHEMA.get_or_init(|| {
                parse_builtin_schema(
                    "lix_version_descriptor.json",
                    LIX_VERSION_DESCRIPTOR_SCHEMA_JSON,
                )
            }))
        }
        LIX_VERSION_POINTER_SCHEMA_KEY => Some(LIX_VERSION_POINTER_SCHEMA.get_or_init(|| {
            parse_builtin_schema("lix_version_pointer.json", LIX_VERSION_POINTER_SCHEMA_JSON)
        })),
        LIX_ACTIVE_VERSION_SCHEMA_KEY => Some(LIX_ACTIVE_VERSION_SCHEMA.get_or_init(|| {
            parse_builtin_schema("lix_active_version.json", LIX_ACTIVE_VERSION_SCHEMA_JSON)
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
        LIX_FILE_DESCRIPTOR_SCHEMA_KEY => Some(LIX_FILE_DESCRIPTOR_SCHEMA.get_or_init(|| {
            parse_builtin_schema("lix_file_descriptor.json", LIX_FILE_DESCRIPTOR_SCHEMA_JSON)
        })),
        LIX_DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
            Some(LIX_DIRECTORY_DESCRIPTOR_SCHEMA.get_or_init(|| {
                parse_builtin_schema(
                    "lix_directory_descriptor.json",
                    LIX_DIRECTORY_DESCRIPTOR_SCHEMA_JSON,
                )
            }))
        }
        _ => None,
    }
}

#[allow(dead_code)]
pub(crate) fn builtin_schema_json(schema_key: &str) -> Option<&'static str> {
    match schema_key {
        LIX_STORED_SCHEMA_KEY => Some(LIX_STORED_SCHEMA_JSON),
        LIX_KEY_VALUE_SCHEMA_KEY => Some(LIX_KEY_VALUE_SCHEMA_JSON),
        LIX_ACCOUNT_SCHEMA_KEY => Some(LIX_ACCOUNT_SCHEMA_JSON),
        LIX_ACTIVE_ACCOUNT_SCHEMA_KEY => Some(LIX_ACTIVE_ACCOUNT_SCHEMA_JSON),
        LIX_CHANGE_SCHEMA_KEY => Some(LIX_CHANGE_SCHEMA_JSON),
        LIX_CHANGE_AUTHOR_SCHEMA_KEY => Some(LIX_CHANGE_AUTHOR_SCHEMA_JSON),
        LIX_CHANGE_SET_SCHEMA_KEY => Some(LIX_CHANGE_SET_SCHEMA_JSON),
        LIX_COMMIT_SCHEMA_KEY => Some(LIX_COMMIT_SCHEMA_JSON),
        LIX_VERSION_DESCRIPTOR_SCHEMA_KEY => Some(LIX_VERSION_DESCRIPTOR_SCHEMA_JSON),
        LIX_VERSION_POINTER_SCHEMA_KEY => Some(LIX_VERSION_POINTER_SCHEMA_JSON),
        LIX_ACTIVE_VERSION_SCHEMA_KEY => Some(LIX_ACTIVE_VERSION_SCHEMA_JSON),
        LIX_CHANGE_SET_ELEMENT_SCHEMA_KEY => Some(LIX_CHANGE_SET_ELEMENT_SCHEMA_JSON),
        LIX_COMMIT_EDGE_SCHEMA_KEY => Some(LIX_COMMIT_EDGE_SCHEMA_JSON),
        LIX_FILE_DESCRIPTOR_SCHEMA_KEY => Some(LIX_FILE_DESCRIPTOR_SCHEMA_JSON),
        LIX_DIRECTORY_DESCRIPTOR_SCHEMA_KEY => Some(LIX_DIRECTORY_DESCRIPTOR_SCHEMA_JSON),
        _ => None,
    }
}

pub(crate) fn decode_lixcol_literal(raw: &str) -> String {
    serde_json::from_str::<String>(raw).unwrap_or_else(|_| raw.trim_matches('"').to_string())
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
