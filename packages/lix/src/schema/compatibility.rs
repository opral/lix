use serde_json::Value as JsonValue;

use crate::LixError;

pub(crate) fn validate_schema_amendment(
    previous: &JsonValue,
    next: &JsonValue,
) -> Result<(), LixError> {
    let previous = super::definition::parse_lix_schema(previous)?;
    let next = super::definition::parse_lix_schema(next)?;
    lix_schema::validate_amendment(&previous, &next).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("incompatible schema amendment: {error}"),
        )
    })
}
