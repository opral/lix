use crate::LixError;

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

/// Returns the SQL comparison key for canonical JSONB metadata text.
///
/// Callers must pass the compact, recursively key-sorted rendering produced
/// by the JSONB renderer or `Json` canonicalizer. The key preserves that
/// rendering while normalizing equivalent exact-number spellings, so SQL
/// string comparisons and hashes agree without changing stored JSONB bytes.
pub(crate) fn metadata_sql_equality_key(
    canonical_metadata: &str,
) -> Result<String, lix_schema::JsonbError> {
    lix_schema::jsonb_equality_key(canonical_metadata)
}
