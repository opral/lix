use jsonschema::JSONSchema;
use serde_json::Value as JsonValue;
use std::sync::OnceLock;

use crate::LixError;

static LIX_SCHEMA_DEFINITION: OnceLock<JsonValue> = OnceLock::new();
static LIX_SCHEMA_VALIDATOR: OnceLock<Result<JSONSchema, LixError>> = OnceLock::new();

pub fn lix_schema_definition() -> &'static JsonValue {
    LIX_SCHEMA_DEFINITION.get_or_init(|| {
        // NOTE: x-lix-version is intentionally constrained to a monotonic integer (as a string).
        // This keeps translation rules open while avoiding a future breaking change when versioning
        // semantics become concrete.
        let raw = include_str!("schema_definition.json");
        serde_json::from_str(raw).expect("schema_definition.json must be valid JSON")
    })
}

pub fn lix_schema_definition_json() -> &'static str {
    include_str!("schema_definition.json")
}

pub fn validate_lix_schema_definition(schema: &JsonValue) -> Result<(), LixError> {
    let validator = lix_schema_validator()?;
    if let Err(errors) = validator.validate(schema) {
        let details = format_validation_errors(errors);
        return Err(LixError {
            message: format!("Invalid Lix schema definition: {details}"),
        });
    }

    assert_primary_key_pointers(schema)?;
    assert_unique_pointers(schema)?;

    Ok(())
}

pub fn validate_lix_schema(schema: &JsonValue, data: &JsonValue) -> Result<(), LixError> {
    validate_lix_schema_definition(schema)?;

    let validator = compile_schema(schema)?;
    if let Err(errors) = validator.validate(data) {
        let details = format_validation_errors(errors);
        return Err(LixError {
            message: format!("Data validation failed: {details}"),
        });
    }

    Ok(())
}

fn lix_schema_validator() -> Result<&'static JSONSchema, LixError> {
    let result = LIX_SCHEMA_VALIDATOR.get_or_init(|| compile_schema(lix_schema_definition()));
    match result {
        Ok(schema) => Ok(schema),
        Err(err) => Err(LixError {
            message: err.message.clone(),
        }),
    }
}

fn compile_schema(schema: &JsonValue) -> Result<JSONSchema, LixError> {
    let mut options = JSONSchema::options();
    options.with_meta_schemas();
    options.with_format("json-pointer", is_json_pointer);
    options.with_format("cel", |_value| true);

    options.compile(schema).map_err(|err| LixError {
        message: format!("Failed to compile Lix schema definition: {err}"),
    })
}

fn is_json_pointer(value: &str) -> bool {
    parse_json_pointer(value).is_ok()
}

fn parse_json_pointer(pointer: &str) -> Result<Vec<String>, LixError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(LixError {
            message: "Invalid JSON pointer".to_string(),
        });
    }

    let mut segments = Vec::new();
    for raw in pointer[1..].split('/') {
        segments.push(unescape_pointer_segment(raw)?);
    }
    Ok(segments)
}

fn unescape_pointer_segment(segment: &str) -> Result<String, LixError> {
    let mut out = String::new();
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') => out.push('~'),
                Some('1') => out.push('/'),
                _ => {
                    return Err(LixError {
                        message: "Invalid JSON pointer".to_string(),
                    })
                }
            }
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

fn assert_primary_key_pointers(schema: &JsonValue) -> Result<(), LixError> {
    let Some(primary_key) = schema
        .get("x-lix-primary-key")
        .and_then(|value| value.as_array())
    else {
        return Ok(());
    };

    for pointer in primary_key {
        let Some(pointer) = pointer.as_str() else {
            continue;
        };
        let segments = parse_json_pointer(pointer)?;
        if segments.is_empty() || !schema_has_property(schema, &segments) {
            return Err(LixError {
                message: format!(
                    "Invalid Lix schema definition: x-lix-primary-key references missing property \"{}\".",
                    pointer
                ),
            });
        }
    }

    Ok(())
}

fn assert_unique_pointers(schema: &JsonValue) -> Result<(), LixError> {
    let Some(unique_groups) = schema
        .get("x-lix-unique")
        .and_then(|value| value.as_array())
    else {
        return Ok(());
    };

    for group in unique_groups {
        let Some(group) = group.as_array() else {
            continue;
        };
        for pointer in group {
            let Some(pointer) = pointer.as_str() else {
                continue;
            };
            let segments = parse_json_pointer(pointer)?;
            if segments.is_empty() || !schema_has_property(schema, &segments) {
                return Err(LixError {
                    message: format!(
                        "Invalid Lix schema definition: x-lix-unique references missing property \"{}\".",
                        pointer
                    ),
                });
            }
        }
    }

    Ok(())
}

fn schema_has_property(schema: &JsonValue, segments: &[String]) -> bool {
    let mut node = schema;
    for segment in segments {
        let properties = match node.get("properties") {
            Some(properties) => properties,
            None => return false,
        };
        let properties = match properties.as_object() {
            Some(properties) => properties,
            None => return false,
        };
        let next = match properties.get(segment) {
            Some(next) => next,
            None => return false,
        };
        node = next;
    }
    true
}

fn format_validation_errors<'a>(
    errors: impl Iterator<Item = jsonschema::ValidationError<'a>>,
) -> String {
    let mut parts = Vec::new();
    for error in errors {
        let path = error.instance_path.to_string();
        let message = error.to_string();
        if path.is_empty() {
            parts.push(message);
        } else {
            parts.push(format!("{path} {message}"));
        }
    }
    if parts.is_empty() {
        "Unknown validation error".to_string()
    } else {
        parts.join("; ")
    }
}
