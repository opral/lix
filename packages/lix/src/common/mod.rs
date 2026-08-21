pub(crate) mod error;
pub(crate) mod exact_batch;
mod execution_metadata;
pub(crate) mod identity;
pub(crate) mod json_pointer;
pub(crate) mod lix_path;
pub(crate) mod metadata;
pub(crate) mod string_dictionary;
pub(crate) mod timestamp;
pub(crate) mod types;
pub(crate) mod wire;

pub use error::LixError;
pub(crate) use exact_batch::{ExactBatch, ExactValue};
pub use execution_metadata::{ExecuteStatementMetadata, MutationIdentity, RequestBlobSpliceProvenance};
#[cfg(feature = "server-protocol")]
pub(crate) use execution_metadata::VerifiedRequestBlob;
pub use identity::{BranchId, CanonicalPluginKey, CanonicalSchemaKey, RowPk, FileId};
pub(crate) use identity::{json_pointer_get, validate_non_empty_identity_value};
pub(crate) use json_pointer::format_json_pointer;
#[cfg(test)]
pub(crate) use json_pointer::parse_json_pointer;
pub use lix_path::{LixPath, validate_lix_path_segment};
pub(crate) use lix_path::{compose_directory_path, compose_file_path};
pub(crate) use metadata::{
    parse_row_metadata_value, serialize_row_metadata, validate_row_metadata,
};
pub(crate) use string_dictionary::{
    FastHashBuilder, StringDictionary, StringDictionaryBuilder, fast_hash_builder,
};
pub(crate) use timestamp::LixTimestamp;
pub use types::{Blob, Json, LixNotice, NullableKeyFilter, SharedStr, SqlQueryResult, Value};
pub use wire::{WireQueryResult, WireValue};

/// Renders a JSON value through the public SQL string-column coercion.
///
/// Registered schemas are validated separately. The SQL surface has
/// historically kept malformed stored values readable by coercing any
/// non-null JSON scalar or container to text, so native indexes and joins
/// must use this exact representation too.
pub(crate) fn json_value_to_string(value: &serde_json::Value) -> Result<Option<String>, LixError> {
    Ok(match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Bool(value) => Some(value.to_string()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Some(serde_json::to_string(value).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("failed to render JSON string value: {error}"),
                )
            })?)
        }
    })
}
