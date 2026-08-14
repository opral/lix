use serde_json::Value as JsonValue;
use std::sync::OnceLock;

const LIX_REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const LIX_KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const LIX_ACCOUNT_SCHEMA_KEY: &str = "lix_account";
const LIX_CHANGE_SCHEMA_KEY: &str = "lix_change";
const LIX_COMMIT_SCHEMA_KEY: &str = "lix_commit";
const LIX_BRANCH_DESCRIPTOR_SCHEMA_KEY: &str = "lix_branch_descriptor";
const LIX_BRANCH_REF_SCHEMA_KEY: &str = "lix_branch_ref";
const LIX_COMMIT_EDGE_SCHEMA_KEY: &str = "lix_commit_edge";
const LIX_FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const LIX_DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const LIX_BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
pub(crate) const LIX_CHECKPOINT_SCHEMA_KEY: &str = "lix_checkpoint";
const LIX_CHECKPOINT_MARKER_SCHEMA_KEY: &str = "lix_checkpoint_marker";
const LIX_UNDO_REDO_MARKER_SCHEMA_KEY: &str = "lix_undo_redo_marker";
const LIX_COLLECTION_GENERATION_SCHEMA_KEY: &str = "lix_collection_generation";

const LIX_REGISTERED_SCHEMA_JSON: &str = include_str!("lix_registered_schema.json");
const LIX_KEY_VALUE_SCHEMA_JSON: &str = include_str!("lix_key_value.json");
const LIX_ACCOUNT_SCHEMA_JSON: &str = include_str!("lix_account.json");
const LIX_CHANGE_SCHEMA_JSON: &str = include_str!("lix_change.json");
const LIX_COMMIT_SCHEMA_JSON: &str = include_str!("lix_commit.json");
const LIX_BRANCH_DESCRIPTOR_SCHEMA_JSON: &str = include_str!("lix_branch_descriptor.json");
const LIX_BRANCH_REF_SCHEMA_JSON: &str = include_str!("lix_branch_ref.json");
const LIX_COMMIT_EDGE_SCHEMA_JSON: &str = include_str!("lix_commit_edge.json");
const LIX_FILE_DESCRIPTOR_SCHEMA_JSON: &str = include_str!("lix_file_descriptor.json");
const LIX_DIRECTORY_DESCRIPTOR_SCHEMA_JSON: &str = include_str!("lix_directory_descriptor.json");
const LIX_BINARY_BLOB_REF_SCHEMA_JSON: &str = include_str!("lix_binary_blob_ref.json");
const LIX_CHECKPOINT_SCHEMA_JSON: &str = include_str!("lix_checkpoint.json");
const LIX_CHECKPOINT_MARKER_SCHEMA_JSON: &str = include_str!("lix_checkpoint_marker.json");
const LIX_UNDO_REDO_MARKER_SCHEMA_JSON: &str = include_str!("lix_undo_redo_marker.json");
const LIX_COLLECTION_GENERATION_SCHEMA_JSON: &str = include_str!("lix_collection_generation.json");

static LIX_REGISTERED_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_KEY_VALUE_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_ACCOUNT_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_CHANGE_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_COMMIT_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_BRANCH_DESCRIPTOR_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_BRANCH_REF_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_COMMIT_EDGE_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_FILE_DESCRIPTOR_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_DIRECTORY_DESCRIPTOR_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_BINARY_BLOB_REF_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_CHECKPOINT_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_CHECKPOINT_MARKER_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_UNDO_REDO_MARKER_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static LIX_COLLECTION_GENERATION_SCHEMA: OnceLock<JsonValue> = OnceLock::new();

const BUILTIN_SCHEMA_KEYS: &[&str] = &[
    LIX_REGISTERED_SCHEMA_KEY,
    LIX_KEY_VALUE_SCHEMA_KEY,
    LIX_ACCOUNT_SCHEMA_KEY,
    LIX_CHANGE_SCHEMA_KEY,
    LIX_COMMIT_SCHEMA_KEY,
    LIX_BRANCH_DESCRIPTOR_SCHEMA_KEY,
    LIX_BRANCH_REF_SCHEMA_KEY,
    LIX_COMMIT_EDGE_SCHEMA_KEY,
    LIX_FILE_DESCRIPTOR_SCHEMA_KEY,
    LIX_DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
    LIX_BINARY_BLOB_REF_SCHEMA_KEY,
    LIX_CHECKPOINT_SCHEMA_KEY,
    LIX_CHECKPOINT_MARKER_SCHEMA_KEY,
    LIX_UNDO_REDO_MARKER_SCHEMA_KEY,
    LIX_COLLECTION_GENERATION_SCHEMA_KEY,
];

pub(super) fn is_seed_schema_key(schema_key: &str) -> bool {
    BUILTIN_SCHEMA_KEYS.contains(&schema_key)
}

pub(super) fn seed_schema_definitions() -> Vec<&'static JsonValue> {
    BUILTIN_SCHEMA_KEYS
        .iter()
        .map(|schema_key| {
            seed_schema_definition(schema_key)
                .unwrap_or_else(|| panic!("missing seed schema definition for '{schema_key}'"))
        })
        .collect()
}

pub(super) fn seed_schema_definition(schema_key: &str) -> Option<&'static JsonValue> {
    match schema_key {
        LIX_REGISTERED_SCHEMA_KEY => {
            Some(LIX_REGISTERED_SCHEMA.get_or_init(|| {
                parse_builtin_schema("lix_registered_schema.json", LIX_REGISTERED_SCHEMA_JSON)
            }))
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
        LIX_CHANGE_SCHEMA_KEY => Some(
            LIX_CHANGE_SCHEMA
                .get_or_init(|| parse_builtin_schema("lix_change.json", LIX_CHANGE_SCHEMA_JSON)),
        ),
        LIX_COMMIT_SCHEMA_KEY => Some(
            LIX_COMMIT_SCHEMA
                .get_or_init(|| parse_builtin_schema("lix_commit.json", LIX_COMMIT_SCHEMA_JSON)),
        ),
        LIX_BRANCH_DESCRIPTOR_SCHEMA_KEY => Some(LIX_BRANCH_DESCRIPTOR_SCHEMA.get_or_init(|| {
            parse_builtin_schema(
                "lix_branch_descriptor.json",
                LIX_BRANCH_DESCRIPTOR_SCHEMA_JSON,
            )
        })),
        LIX_BRANCH_REF_SCHEMA_KEY => Some(LIX_BRANCH_REF_SCHEMA.get_or_init(|| {
            parse_builtin_schema("lix_branch_ref.json", LIX_BRANCH_REF_SCHEMA_JSON)
        })),
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
        LIX_BINARY_BLOB_REF_SCHEMA_KEY => Some(LIX_BINARY_BLOB_REF_SCHEMA.get_or_init(|| {
            parse_builtin_schema("lix_binary_blob_ref.json", LIX_BINARY_BLOB_REF_SCHEMA_JSON)
        })),
        LIX_CHECKPOINT_SCHEMA_KEY => Some(LIX_CHECKPOINT_SCHEMA.get_or_init(|| {
            parse_builtin_schema("lix_checkpoint.json", LIX_CHECKPOINT_SCHEMA_JSON)
        })),
        LIX_CHECKPOINT_MARKER_SCHEMA_KEY => {
            Some(LIX_CHECKPOINT_MARKER_SCHEMA.get_or_init(|| {
                parse_builtin_schema(
                    "lix_checkpoint_marker.json",
                    LIX_CHECKPOINT_MARKER_SCHEMA_JSON,
                )
            }))
        }
        LIX_UNDO_REDO_MARKER_SCHEMA_KEY => Some(LIX_UNDO_REDO_MARKER_SCHEMA.get_or_init(|| {
            parse_builtin_schema(
                "lix_undo_redo_marker.json",
                LIX_UNDO_REDO_MARKER_SCHEMA_JSON,
            )
        })),
        LIX_COLLECTION_GENERATION_SCHEMA_KEY => {
            Some(LIX_COLLECTION_GENERATION_SCHEMA.get_or_init(|| {
                parse_builtin_schema(
                    "lix_collection_generation.json",
                    LIX_COLLECTION_GENERATION_SCHEMA_JSON,
                )
            }))
        }
        _ => None,
    }
}

fn parse_builtin_schema(file_name: &str, raw_json: &str) -> JsonValue {
    serde_json::from_str(raw_json).unwrap_or_else(|error| {
        panic!("builtin schema file '{file_name}' must contain valid JSON: {error}")
    })
}

#[cfg(test)]
mod tests {
    use super::{BUILTIN_SCHEMA_KEYS, seed_schema_definition};

    #[test]
    fn builtin_schemas_load_without_extra_override_metadata() {
        for schema_key in BUILTIN_SCHEMA_KEYS {
            seed_schema_definition(schema_key).expect("schema should exist");
        }
    }

}
