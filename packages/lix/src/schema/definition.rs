use std::sync::OnceLock;

use serde_json::Value as JsonValue;

use crate::LixError;

static LIX_SCHEMA_DEFINITION: OnceLock<JsonValue> = OnceLock::new();

pub fn lix_schema_definition() -> &'static JsonValue {
    LIX_SCHEMA_DEFINITION.get_or_init(|| {
        serde_json::from_str(lix_schema::SCHEMA_V1_JSON)
            .expect("embedded Lix Schema v1 meta-schema must be valid JSON")
    })
}

pub const fn lix_schema_definition_json() -> &'static str {
    lix_schema::SCHEMA_V1_JSON
}

pub fn validate_lix_schema_definition(schema: &JsonValue) -> Result<(), LixError> {
    parse_lix_schema(schema).map(|_| ())
}

pub fn validate_lix_schema(schema: &JsonValue, data: &JsonValue) -> Result<(), LixError> {
    compile_lix_schema(schema)?
        .validate(data)
        .map_err(|error| schema_error(LixError::CODE_SCHEMA_VALIDATION, "Data validation failed", error))
}

pub(crate) fn compile_lix_schema(
    schema: &JsonValue,
) -> Result<lix_schema::CompiledSchema, LixError> {
    let schema = parse_lix_schema(schema)?;
    lix_schema::CompiledSchema::compile(&schema).map_err(|error| {
        schema_error(
            LixError::CODE_SCHEMA_DEFINITION,
            "Invalid Lix schema definition",
            error,
        )
    })
}

pub(crate) fn parse_lix_schema(schema: &JsonValue) -> Result<lix_schema::Schema, LixError> {
    let parsed = lix_schema::from_value(schema.clone()).map_err(|error| {
        schema_error(
            LixError::CODE_SCHEMA_DEFINITION,
            "Invalid Lix schema definition",
            error,
        )
    })?;
    if let Some(column) = parsed.columns.iter().find(|column| {
        column.name.starts_with("lixcol_") || column.name.contains("_lixcol_")
    }) {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "Invalid Lix schema definition: column '{}' uses the reserved lixcol_ segment",
                column.name
            ),
        ));
    }
    Ok(parsed)
}

pub(crate) fn format_lix_schema_validation_errors(error: lix_schema::Error) -> String {
    error.to_string()
}

fn schema_error(code: &str, prefix: &str, error: lix_schema::Error) -> LixError {
    LixError::new(code, format!("{prefix}: {error}"))
}
