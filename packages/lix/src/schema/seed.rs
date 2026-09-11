use serde_json::Value as JsonValue;

pub(crate) fn is_seed_schema_key(schema_key: &str) -> bool {
    super::builtin::is_seed_schema_key(schema_key)
}

pub(crate) fn seed_schema_definition(schema_key: &str) -> Option<&'static JsonValue> {
    super::builtin::seed_schema_definition(schema_key)
}

pub(crate) fn seed_schema_definitions() -> Vec<&'static JsonValue> {
    super::builtin::seed_schema_definitions()
}

/// What a seed schema, or one of its columns, means, for the seed documents
/// that shipped without `description` annotations.
///
/// A seed document is fingerprinted as written, and durable rows carry that
/// fingerprint, so adding text to the document would orphan every existing
/// row of the schema. The words live here instead and the catalog reads them
/// as if the document carried them. Pass `None` for the table itself.
pub(crate) fn seed_schema_description(schema_key: &str, column: Option<&str>) -> Option<&'static str> {
    Some(match (schema_key, column) {
        ("lix_file_descriptor", None) => {
            "The identity of a file: its name and the directory holding it. The composed lix_file view joins this descriptor with the file's path and content."
        }
        ("lix_file_descriptor", Some("id")) => {
            "Stable file identifier (UUIDv7); it survives renames and moves."
        }
        ("lix_file_descriptor", Some("directory_id")) => {
            "Directory holding the file (references lix_directory_descriptor.id); null at the repository root."
        }
        ("lix_file_descriptor", Some("name")) => {
            "File name: the last path segment, extension included."
        }
        ("lix_directory_descriptor", None) => {
            "The identity of a directory: its name and parent. The composed lix_directory view derives each path from the chain of parents."
        }
        ("lix_directory_descriptor", Some("id")) => "Stable directory identifier (UUIDv7).",
        ("lix_directory_descriptor", Some("name")) => "Directory name: the last path segment.",
        ("lix_directory_descriptor", Some("parent_id")) => {
            "Parent directory (references lix_directory_descriptor.id); null for a top-level directory."
        }
        ("lix_key_value", None) => "A repository setting or application value stored under a key.",
        ("lix_key_value", Some("key")) => "Unique key naming the value.",
        _ => return None,
    })
}
