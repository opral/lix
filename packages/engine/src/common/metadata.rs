use crate::LixError;

pub(crate) fn parse_row_metadata(
    value: &str,
    context: impl AsRef<str>,
) -> Result<String, LixError> {
    let metadata = parse_row_metadata_value(value, context)?;
    Ok(serde_json::to_string(&metadata).expect("serde_json::Value metadata serializes"))
}

pub(crate) fn parse_row_metadata_value(
    value: &str,
    context: impl AsRef<str>,
) -> Result<serde_json::Value, LixError> {
    let metadata = serde_json::from_str::<serde_json::Value>(value).map_err(|error| {
        LixError::new(
            "LIX_ERROR_INVALID_JSON",
            format!("{} metadata is invalid JSON: {error}", context.as_ref()),
        )
    })?;
    validate_row_metadata(&metadata, context)?;
    Ok(metadata)
}

pub(crate) fn validate_row_metadata(
    metadata: &serde_json::Value,
    context: impl AsRef<str>,
) -> Result<(), LixError> {
    if metadata.is_object() {
        return Ok(());
    }
    Err(LixError::new(
        LixError::CODE_SCHEMA_VALIDATION,
        format!("{} metadata must be a JSON object", context.as_ref()),
    ))
}

pub(crate) fn serialize_row_metadata(metadata: &str) -> String {
    metadata.to_owned()
}
