use serde_json::Value as JsonValue;

use crate::LixError;

pub(crate) type RowMetadata = JsonValue;

pub(crate) fn parse_row_metadata(
    value: &str,
    context: impl AsRef<str>,
) -> Result<RowMetadata, LixError> {
    let metadata = serde_json::from_str::<JsonValue>(value).map_err(|error| {
        LixError::new(
            "LIX_ERROR_INVALID_JSON",
            format!("{} metadata is invalid JSON: {error}", context.as_ref()),
        )
    })?;
    validate_row_metadata(metadata, context)
}

pub(crate) fn validate_row_metadata(
    metadata: RowMetadata,
    context: impl AsRef<str>,
) -> Result<RowMetadata, LixError> {
    if metadata.is_object() {
        return Ok(metadata);
    }
    Err(LixError::new(
        LixError::CODE_SCHEMA_VALIDATION,
        format!("{} metadata must be a JSON object", context.as_ref()),
    ))
}

pub(crate) fn serialize_row_metadata(metadata: &RowMetadata) -> String {
    serde_json::to_string(metadata).expect("serde_json::Value metadata serializes")
}
