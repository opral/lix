use cel::Program;
use jsonschema::{Draft, JSONSchema};
use serde_json::Value as JsonValue;
use std::collections::BTreeSet;
use std::sync::OnceLock;

use crate::common::parse_json_pointer;
use crate::LixError;

static LIX_SCHEMA_DEFINITION: OnceLock<JsonValue> = OnceLock::new();
static LIX_SCHEMA_VALIDATOR: OnceLock<Result<JSONSchema, LixError>> = OnceLock::new();

pub fn lix_schema_definition() -> &'static JsonValue {
    LIX_SCHEMA_DEFINITION.get_or_init(|| {
        let raw = include_str!("definition.json");
        serde_json::from_str(raw).expect("definition.json must be valid JSON")
    })
}

pub fn lix_schema_definition_json() -> &'static str {
    include_str!("definition.json")
}

pub fn validate_lix_schema_definition(schema: &JsonValue) -> Result<(), LixError> {
    if let Some(err) = detect_missing_pointer_slash(schema) {
        return Err(err);
    }
    if let Some(err) = detect_state_foreign_key_tuple_shape(schema) {
        return Err(err);
    }

    let validator = lix_schema_validator()?;
    if let Err(errors) = validator.validate(schema) {
        let details = format_lix_schema_validation_errors(errors);
        return Err(LixError {
            code: LixError::CODE_SCHEMA_DEFINITION.to_string(),
            message: format!("Invalid Lix schema definition: {details}"),
            hint: None,
            details: None,
        });
    }

    assert_primary_key_pointers(schema)?;
    assert_unique_pointers(schema)?;
    assert_state_foreign_key_pointers(schema)?;
    assert_known_x_lix_top_level_fields(schema)?;
    assert_entity_properties_do_not_use_reserved_lix_prefix(schema)?;
    assert_entity_properties_have_projectable_types(schema)?;

    Ok(())
}

fn assert_entity_properties_do_not_use_reserved_lix_prefix(
    schema: &JsonValue,
) -> Result<(), LixError> {
    let Some(schema_key) = schema.get("x-lix-key").and_then(JsonValue::as_str) else {
        return Ok(());
    };
    let Some(properties) = schema.get("properties").and_then(JsonValue::as_object) else {
        return Ok(());
    };

    for property_name in properties.keys() {
        if property_name.starts_with("lix") {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "Invalid Lix schema definition: schema '{schema_key}' property '/{property_name}' uses reserved prefix 'lix'."
                ),
            )
            .with_hint("Property names starting with 'lix' are reserved for Lix system fields."));
        }
    }

    Ok(())
}

fn assert_entity_properties_have_projectable_types(schema: &JsonValue) -> Result<(), LixError> {
    let Some(schema_key) = schema.get("x-lix-key").and_then(JsonValue::as_str) else {
        return Ok(());
    };
    let Some(properties) = schema.get("properties").and_then(JsonValue::as_object) else {
        return Ok(());
    };

    for (property_name, property_schema) in properties {
        if !schema_property_has_sql_projection_type(property_schema) {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "Invalid Lix schema definition: schema '{schema_key}' property '/{property_name}' must declare a SQL-projectable JSON Schema type"
                ),
            )
            .with_hint("Use an explicit type such as string, number, integer, boolean, object, array, or a supported union of those types."));
        }
    }

    Ok(())
}

fn schema_property_has_sql_projection_type(schema: &JsonValue) -> bool {
    let mut kinds = BTreeSet::new();
    collect_schema_type_kinds(schema, &mut kinds);
    kinds.remove("null");
    kinds.iter().any(|kind| {
        matches!(
            *kind,
            "boolean" | "integer" | "number" | "string" | "object" | "array"
        )
    })
}

fn collect_schema_type_kinds<'a>(schema: &'a JsonValue, out: &mut BTreeSet<&'a str>) {
    match schema.get("type") {
        Some(JsonValue::String(kind)) => {
            out.insert(kind.as_str());
        }
        Some(JsonValue::Array(kinds)) => {
            for kind in kinds.iter().filter_map(JsonValue::as_str) {
                out.insert(kind);
            }
        }
        _ => {}
    }

    for keyword in ["anyOf", "oneOf", "allOf"] {
        if let Some(JsonValue::Array(branches)) = schema.get(keyword) {
            for branch in branches {
                collect_schema_type_kinds(branch, out);
            }
        }
    }
}

/// Detect the common no-leading-slash mistake in JSON-Pointer-valued fields
/// (`x-lix-primary-key`, `x-lix-unique`, `x-lix-foreign-keys[].properties`,
/// `x-lix-foreign-keys[].references.properties`,
/// `x-lix-state-foreign-keys[]`) and return a targeted
/// error + hint suggesting the fix.
///
/// Surfacing this before the meta-schema validator runs replaces the
/// generic `format "json-pointer"` failure with a message that tells the
/// user exactly what to change (e.g. `"id"` → `"/id"`).
fn detect_missing_pointer_slash(schema: &JsonValue) -> Option<LixError> {
    let mut offenders: Vec<(String, String)> = Vec::new();

    fn collect(items: Option<&Vec<JsonValue>>, label: &str, out: &mut Vec<(String, String)>) {
        let Some(items) = items else {
            return;
        };
        for item in items {
            if let Some(s) = item.as_str() {
                if !s.is_empty() && !s.starts_with('/') {
                    out.push((label.to_string(), s.to_string()));
                }
            }
        }
    }

    collect(
        schema
            .get("x-lix-primary-key")
            .and_then(JsonValue::as_array),
        "x-lix-primary-key",
        &mut offenders,
    );

    if let Some(groups) = schema.get("x-lix-unique").and_then(JsonValue::as_array) {
        for group in groups {
            collect(group.as_array(), "x-lix-unique", &mut offenders);
        }
    }

    if let Some(fks) = schema
        .get("x-lix-foreign-keys")
        .and_then(JsonValue::as_array)
    {
        for fk in fks {
            collect(
                fk.get("properties").and_then(JsonValue::as_array),
                "x-lix-foreign-keys[].properties",
                &mut offenders,
            );
            collect(
                fk.get("references")
                    .and_then(|r| r.get("properties"))
                    .and_then(JsonValue::as_array),
                "x-lix-foreign-keys[].references.properties",
                &mut offenders,
            );
        }
    }

    if let Some(fks) = schema
        .get("x-lix-state-foreign-keys")
        .and_then(JsonValue::as_array)
    {
        for fk in fks {
            collect(fk.as_array(), "x-lix-state-foreign-keys", &mut offenders);
        }
    }

    if offenders.is_empty() {
        return None;
    }

    let examples = offenders
        .iter()
        .take(3)
        .map(|(field, value)| format!("{field}: \"{value}\" → \"/{value}\""))
        .collect::<Vec<_>>()
        .join("; ");
    let message = format!(
        "Invalid Lix schema definition: JSON Pointer values must begin with '/'. Offending entries: {examples}"
    );
    let hint = format!(
        "Did you mean [\"/{}\"]? JSON Pointer values must prefix property names with '/' (RFC 6901).",
        offenders[0].1
    );
    Some(
        LixError {
            code: LixError::CODE_SCHEMA_DEFINITION.to_string(),
            message,
            hint: None,
            details: None,
        }
        .with_hint(hint),
    )
}

fn detect_state_foreign_key_tuple_shape(schema: &JsonValue) -> Option<LixError> {
    let foreign_keys = schema
        .get("x-lix-state-foreign-keys")
        .and_then(JsonValue::as_array)?;
    for (index, foreign_key) in foreign_keys.iter().enumerate() {
        let Some(local_pointers) = foreign_key.as_array() else {
            continue;
        };
        if local_pointers.len() != 3 {
            return Some(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "Invalid Lix schema definition: x-lix-state-foreign-keys[{index}] must contain exactly three JSON Pointers ordered as [entity_id, schema_key, file_id]; [0] entity_id, [1] schema_key, [2] file_id."
                ),
            ));
        }
    }
    None
}

pub fn validate_lix_schema(schema: &JsonValue, data: &JsonValue) -> Result<(), LixError> {
    validate_lix_schema_definition(schema)?;

    let validator = compile_lix_schema(schema)?;
    if let Err(errors) = validator.validate(data) {
        let details = format_lix_schema_validation_errors(errors);
        return Err(LixError {
            code: LixError::CODE_SCHEMA_VALIDATION.to_string(),
            message: format!("Data validation failed: {details}"),
            hint: None,
            details: None,
        });
    }

    Ok(())
}

fn lix_schema_validator() -> Result<&'static JSONSchema, LixError> {
    let result = LIX_SCHEMA_VALIDATOR.get_or_init(|| compile_lix_schema(lix_schema_definition()));
    match result {
        Ok(schema) => Ok(schema),
        Err(err) => Err(LixError {
            code: LixError::CODE_SCHEMA_DEFINITION.to_string(),
            message: err.message.clone(),
            hint: None,
            details: None,
        }),
    }
}

pub(crate) fn compile_lix_schema(schema: &JsonValue) -> Result<JSONSchema, LixError> {
    let mut options = JSONSchema::options();
    options.with_meta_schemas();
    if schema_uses_draft_2020_12_without_fragment(schema) {
        options.with_draft(Draft::Draft202012);
    }
    options.should_validate_formats(true);
    options.with_format("json-pointer", is_json_pointer);
    options.with_format("cel", is_cel_expression);

    options.compile(schema).map_err(|err| LixError {
        code: LixError::CODE_SCHEMA_DEFINITION.to_string(),
        message: format!("Failed to compile Lix schema definition: {err}"),
        hint: None,
        details: None,
    })
}

fn schema_uses_draft_2020_12_without_fragment(schema: &JsonValue) -> bool {
    schema
        .get("$schema")
        .and_then(JsonValue::as_str)
        .is_some_and(|url| url == "https://json-schema.org/draft/2020-12/schema")
}

fn is_json_pointer(value: &str) -> bool {
    parse_json_pointer(value).is_ok()
}

fn is_cel_expression(value: &str) -> bool {
    Program::compile(value).is_ok()
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
        let Some(property_schema) = (!segments.is_empty())
            .then(|| schema_property(schema, &segments))
            .flatten()
        else {
            return Err(LixError { code: LixError::CODE_SCHEMA_DEFINITION.to_string(), message: format!(
                    "Invalid Lix schema definition: x-lix-primary-key references missing property \"{}\".",
                    pointer
                ),
                hint: None,
            details: None,
            });
        };
        if !schema_property_is_string_only(property_schema) {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "Invalid Lix schema definition: x-lix-primary-key property \"{pointer}\" must have type \"string\"."
                ),
            ));
        }
        if !schema_pointer_is_required(schema, &segments) {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "Invalid Lix schema definition: x-lix-primary-key property \"{pointer}\" must be required."
                ),
            ));
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
                return Err(LixError { code: LixError::CODE_SCHEMA_DEFINITION.to_string(), message: format!(
                        "Invalid Lix schema definition: x-lix-unique references missing property \"{}\".",
                        pointer
                    ),
                    hint: None,
            details: None,
                });
            }
        }
    }

    Ok(())
}

fn assert_state_foreign_key_pointers(schema: &JsonValue) -> Result<(), LixError> {
    let Some(foreign_keys) = schema
        .get("x-lix-state-foreign-keys")
        .and_then(|value| value.as_array())
    else {
        return Ok(());
    };

    for (index, foreign_key) in foreign_keys.iter().enumerate() {
        let Some(local_pointers) = foreign_key.as_array() else {
            continue;
        };
        if local_pointers.len() != 3 {
            continue;
        }

        let roles = [
            ("entity_id", "a non-empty JSON array of strings"),
            ("schema_key", "a string"),
            ("file_id", "a string or null"),
        ];
        for (slot, (role, expected)) in roles.iter().enumerate() {
            let Some(pointer) = local_pointers[slot].as_str() else {
                continue;
            };
            let segments = parse_json_pointer(pointer)?;
            let Some(property_schema) = (!segments.is_empty())
                .then(|| schema_property(schema, &segments))
                .flatten()
            else {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "Invalid Lix schema definition: x-lix-state-foreign-keys[{index}][{slot}] ({role}) references missing property \"{pointer}\"."
                    ),
                ));
            };
            if !schema_pointer_is_required(schema, &segments) {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "Invalid Lix schema definition: x-lix-state-foreign-keys[{index}][{slot}] ({role}) property \"{pointer}\" must be required. Tuple order is [entity_id, schema_key, file_id]."
                    ),
                ));
            }

            let valid = match *role {
                "entity_id" => schema_property_is_string_array(property_schema),
                "schema_key" => schema_property_is_string_only(property_schema),
                "file_id" => schema_property_is_string_or_null(property_schema),
                _ => unreachable!("state foreign key roles are exhaustive"),
            };
            if !valid {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "Invalid Lix schema definition: x-lix-state-foreign-keys[{index}][{slot}] ({role}) property \"{pointer}\" must be {expected}. Tuple order is [entity_id, schema_key, file_id]."
                    ),
                ));
            }
        }
    }

    Ok(())
}

fn assert_known_x_lix_top_level_fields(schema: &JsonValue) -> Result<(), LixError> {
    let Some(object) = schema.as_object() else {
        return Ok(());
    };

    for key in object.keys() {
        if !key.starts_with("x-lix-") {
            continue;
        }

        let known = matches!(
            key.as_str(),
            "x-lix-key"
                | "x-lix-primary-key"
                | "x-lix-unique"
                | "x-lix-foreign-keys"
                | "x-lix-state-foreign-keys"
        );

        if !known {
            return Err(LixError {
                code: LixError::CODE_SCHEMA_DEFINITION.to_string(),
                message: format!(
                    "Invalid Lix schema definition: unknown x-lix field '{}'.",
                    key
                ),
                hint: None,
                details: None,
            });
        }
    }

    Ok(())
}

fn schema_has_property(schema: &JsonValue, segments: &[String]) -> bool {
    schema_property(schema, segments).is_some()
}

fn schema_pointer_is_required(schema: &JsonValue, segments: &[String]) -> bool {
    if segments.is_empty() {
        return false;
    }

    let mut node = schema;
    for segment in segments {
        let required = node
            .get("required")
            .and_then(JsonValue::as_array)
            .map(|required| {
                required
                    .iter()
                    .any(|required_property| required_property.as_str() == Some(segment))
            })
            .unwrap_or(false);
        if !required {
            return false;
        }

        let Some(next) = node
            .get("properties")
            .and_then(JsonValue::as_object)
            .and_then(|properties| properties.get(segment))
        else {
            return false;
        };
        node = next;
    }

    true
}

fn schema_property<'a>(schema: &'a JsonValue, segments: &[String]) -> Option<&'a JsonValue> {
    let mut node = schema;
    for segment in segments {
        let properties = node.get("properties")?.as_object()?;
        let next = properties.get(segment)?;
        node = next;
    }
    Some(node)
}

fn schema_property_is_string_only(schema: &JsonValue) -> bool {
    let mut kinds = BTreeSet::new();
    collect_schema_type_kinds(schema, &mut kinds);
    kinds.len() == 1 && kinds.contains("string")
}

fn schema_property_is_string_or_null(schema: &JsonValue) -> bool {
    let mut kinds = BTreeSet::new();
    collect_schema_type_kinds(schema, &mut kinds);
    kinds.remove("null");
    kinds.len() == 1 && kinds.contains("string")
}

fn schema_property_is_string_array(schema: &JsonValue) -> bool {
    let mut kinds = BTreeSet::new();
    collect_schema_type_kinds(schema, &mut kinds);
    if kinds.len() != 1 || !kinds.contains("array") {
        return false;
    }
    let Some(items) = schema.get("items") else {
        return false;
    };
    if !schema_property_is_string_only(items) {
        return false;
    }
    schema
        .get("minItems")
        .and_then(JsonValue::as_u64)
        .is_some_and(|min_items| min_items >= 1)
}

pub(crate) fn format_lix_schema_validation_errors<'a>(
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

#[cfg(test)]
mod pointer_slash_detection_tests {
    use super::*;
    use serde_json::json;

    fn minimal_schema_with(extras: serde_json::Value) -> JsonValue {
        let mut obj = json!({
            "type": "object",
            "x-lix-key": "book",
            "properties": {
                "id": { "type": "string" },
                "author_id": { "type": "string" },
                "tenant_id": { "type": "string" },
                "handle": { "type": "string" },
            },
            "required": ["id"],
            "additionalProperties": false,
        });
        let extras_obj = extras.as_object().expect("extras must be object").clone();
        for (k, v) in extras_obj {
            obj.as_object_mut().unwrap().insert(k, v);
        }
        obj
    }

    fn err_for(schema: &JsonValue) -> LixError {
        validate_lix_schema_definition(schema).expect_err("should reject")
    }

    #[test]
    fn primary_key_without_slash_emits_targeted_hint() {
        let schema = minimal_schema_with(json!({ "x-lix-primary-key": ["id"] }));
        let err = err_for(&schema);
        assert_eq!(
            err.code,
            LixError::CODE_SCHEMA_DEFINITION,
            "schema-definition errors should carry the categorized code"
        );
        assert!(
            err.message.contains("must begin with '/'"),
            "unexpected message: {}",
            err.message
        );
        assert!(
            err.message.contains("x-lix-primary-key: \"id\" → \"/id\""),
            "message should show the fix: {}",
            err.message
        );
        let hint = err.hint.as_deref().expect("should carry a hint");
        assert!(
            hint.contains("/id"),
            "hint should show fixed pointer: {hint}"
        );
        assert!(
            hint.contains("RFC 6901"),
            "hint should cite the RFC: {hint}"
        );
    }

    #[test]
    fn unique_without_slash_emits_targeted_hint() {
        let schema = minimal_schema_with(json!({
            "x-lix-primary-key": ["/id"],
            "x-lix-unique": [["handle"]],
        }));
        let err = err_for(&schema);
        assert!(
            err.message
                .contains("x-lix-unique: \"handle\" → \"/handle\""),
            "should flag x-lix-unique entry: {}",
            err.message
        );
        assert!(err.hint.is_some());
    }

    #[test]
    fn foreign_key_local_without_slash_emits_targeted_hint() {
        let schema = minimal_schema_with(json!({
            "x-lix-primary-key": ["/id"],
            "x-lix-foreign-keys": [{
                "properties": ["author_id"],
                "references": {
                    "schemaKey": "author",
                    "properties": ["/id"],
                }
            }]
        }));
        let err = err_for(&schema);
        assert!(
            err.message
                .contains("x-lix-foreign-keys[].properties: \"author_id\" → \"/author_id\""),
            "should flag FK local entry: {}",
            err.message
        );
    }

    #[test]
    fn foreign_key_remote_without_slash_emits_targeted_hint() {
        let schema = minimal_schema_with(json!({
            "x-lix-primary-key": ["/id"],
            "x-lix-foreign-keys": [{
                "properties": ["/author_id"],
                "references": {
                    "schemaKey": "author",
                    "properties": ["id"],
                }
            }]
        }));
        let err = err_for(&schema);
        assert!(
            err.message
                .contains("x-lix-foreign-keys[].references.properties: \"id\" → \"/id\""),
            "should flag FK remote entry: {}",
            err.message
        );
    }

    #[test]
    fn valid_pointers_pass_pre_check() {
        let schema = minimal_schema_with(json!({
            "x-lix-primary-key": ["/id"],
            "x-lix-unique": [["/handle"], ["/tenant_id", "/handle"]],
            "x-lix-foreign-keys": [{
                "properties": ["/author_id"],
                "references": {
                    "schemaKey": "author",
                    "properties": ["/id"],
                }
            }]
        }));
        assert!(detect_missing_pointer_slash(&schema).is_none());
    }

    #[test]
    fn draft_2020_12_json_pointer_format_still_asserts() {
        let schema = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "pointer": {
                    "type": "string",
                    "format": "json-pointer"
                }
            }
        });

        let validator = compile_lix_schema(&schema).expect("2020-12 schema should compile");

        assert!(validator.is_valid(&json!({ "pointer": "/id" })));
        assert!(!validator.is_valid(&json!({ "pointer": "id" })));
    }
}
