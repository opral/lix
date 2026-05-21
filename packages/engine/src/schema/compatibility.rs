use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;

use crate::common::top_level_property_name;
use crate::entity_pk::canonical_json_text;
use crate::LixError;

const DOC_ONLY_SCHEMA_FIELDS: &[&str] = &["$comment", "deprecated", "description", "title"];
const CONSTRAINT_FIELDS: &[&str] = &[
    "x-lix-primary-key",
    "x-lix-unique",
    "x-lix-foreign-keys",
    "x-lix-state-foreign-keys",
];

/// Validates that `next` is a compatible amendment of `previous`.
///
/// The 0.6 schema model treats `x-lix-key` as the durable relation identity.
/// Same-key amendments may widen accepted data by adding optional top-level
/// properties, but they must not alter identity, constraints, requiredness, or
/// existing field semantics. Nested object schemas are deliberately frozen for
/// 0.6; recursive schema evolution is a later, explicit feature.
///
/// Primary-key column order is semantic because it defines composite
/// `entity_pk` tuple order, so primary keys are never normalized. Relational
/// constraints are frozen even when a particular addition could be
/// retroactively safe, such as a new FK on a new optional property. That is a
/// deliberate MVP rule we may relax later.
pub(crate) fn validate_schema_amendment(
    previous: &JsonValue,
    next: &JsonValue,
) -> Result<(), LixError> {
    let previous_key = schema_key(previous, "previous")?;
    let next_key = schema_key(next, "next")?;
    if previous_key != next_key {
        return schema_amendment_error(format!(
            "schema amendment must keep x-lix-key stable; previous '{previous_key}', next '{next_key}'"
        ));
    }

    require_additional_properties_false(previous, "previous", previous_key)?;
    require_additional_properties_false(next, "next", next_key)?;

    validate_constraints_unchanged(previous, next, previous_key)?;

    let changed_top_level_semantic_keys = changed_top_level_semantic_keys(previous, next);
    if !changed_top_level_semantic_keys.is_empty() {
        return schema_amendment_error(format!(
            "schema '{previous_key}' cannot change top-level schema semantics: {}",
            changed_top_level_semantic_keys.join(", ")
        ));
    }

    let previous_required = string_set_field(previous, "required", "previous", previous_key)?;
    let next_required = string_set_field(next, "required", "next", next_key)?;
    if previous_required != next_required {
        return schema_amendment_error(format!(
            "schema '{previous_key}' cannot amend required properties"
        ));
    }

    let previous_properties = properties_field(previous, "previous", previous_key)?;
    let next_properties = properties_field(next, "next", next_key)?;

    for (property_name, previous_property_schema) in &previous_properties {
        let Some(next_property_schema) = next_properties.get(property_name) else {
            return schema_amendment_error(format!(
                "schema '{previous_key}' cannot remove property '/{property_name}'"
            ));
        };
        if strip_doc_only_fields(previous_property_schema)
            != strip_doc_only_fields(next_property_schema)
        {
            return schema_amendment_error(format!(
                "schema '{previous_key}' cannot change existing property '/{property_name}' except for doc-only fields"
            ));
        }
    }

    let constrained_property_names = constrained_top_level_property_names(next)?;
    for property_name in next_properties.keys() {
        if previous_properties.contains_key(property_name) {
            continue;
        }
        if next_required.contains(property_name) {
            return schema_amendment_error(format!(
                "schema '{previous_key}' cannot add required property '/{property_name}'"
            ));
        }
        if constrained_property_names.contains(property_name) {
            return schema_amendment_error(format!(
                "schema '{previous_key}' cannot add property '/{property_name}' as part of primary, unique, or foreign-key constraints"
            ));
        }
    }

    Ok(())
}

fn schema_key<'a>(schema: &'a JsonValue, side: &str) -> Result<&'a str, LixError> {
    schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("{side} schema must include string x-lix-key"),
            )
        })
}

fn require_additional_properties_false(
    schema: &JsonValue,
    side: &str,
    schema_key: &str,
) -> Result<(), LixError> {
    if schema.get("additionalProperties") == Some(&JsonValue::Bool(false)) {
        return Ok(());
    }
    schema_amendment_error(format!(
        "{side} schema '{schema_key}' must set additionalProperties to false"
    ))
}

fn validate_constraints_unchanged(
    previous: &JsonValue,
    next: &JsonValue,
    schema_key: &str,
) -> Result<(), LixError> {
    // Primary-key column order is semantic because it defines composite
    // entity_pk tuple order, so it is compared directly and never normalized.
    if previous.get("x-lix-primary-key") != next.get("x-lix-primary-key") {
        return schema_amendment_error(format!(
            "schema '{schema_key}' cannot amend constraint field 'x-lix-primary-key'"
        ));
    }

    for field in [
        "x-lix-unique",
        "x-lix-foreign-keys",
        "x-lix-state-foreign-keys",
    ] {
        if normalized_constraint_list(previous.get(field), field)?
            != normalized_constraint_list(next.get(field), field)?
        {
            return schema_amendment_error(format!(
                "schema '{schema_key}' cannot amend constraint field '{field}'"
            ));
        }
    }

    Ok(())
}

fn normalized_constraint_list(
    value: Option<&JsonValue>,
    field: &str,
) -> Result<Vec<JsonValue>, LixError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Some(values) = value.as_array() else {
        return schema_amendment_error(format!(
            "schema constraint field '{field}' must be an array"
        ));
    };

    let mut values = values.clone();
    values.sort_by(|left, right| {
        let left = canonical_json_text(left)
            .expect("canonical json from in-memory serde_json::Value cannot fail");
        let right = canonical_json_text(right)
            .expect("canonical json from in-memory serde_json::Value cannot fail");
        left.cmp(&right)
    });
    Ok(values)
}

fn properties_field(
    schema: &JsonValue,
    side: &str,
    schema_key: &str,
) -> Result<BTreeMap<String, JsonValue>, LixError> {
    match schema.get("properties") {
        Some(JsonValue::Object(object)) => Ok(object
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()),
        Some(_) => schema_amendment_error(format!(
            "{side} schema '{schema_key}' field 'properties' must be an object"
        )),
        None => Ok(BTreeMap::new()),
    }
}

fn string_set_field(
    schema: &JsonValue,
    field: &str,
    side: &str,
    schema_key: &str,
) -> Result<BTreeSet<String>, LixError> {
    let Some(value) = schema.get(field) else {
        return Ok(BTreeSet::new());
    };
    let Some(values) = value.as_array() else {
        return schema_amendment_error(format!(
            "{side} schema '{schema_key}' field '{field}' must be an array of strings"
        ));
    };
    values
        .iter()
        .map(|value| {
            value.as_str().map(str::to_string).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "{side} schema '{schema_key}' field '{field}' must be an array of strings"
                    ),
                )
            })
        })
        .collect()
}

fn strip_doc_only_fields(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(object) => JsonValue::Object(
            object
                .iter()
                .filter(|(key, _)| !DOC_ONLY_SCHEMA_FIELDS.contains(&key.as_str()))
                .map(|(key, value)| (key.clone(), strip_doc_only_fields(value)))
                .collect(),
        ),
        JsonValue::Array(values) => {
            JsonValue::Array(values.iter().map(strip_doc_only_fields).collect())
        }
        _ => value.clone(),
    }
}

fn top_level_semantic_fields(schema: &JsonValue) -> BTreeMap<String, JsonValue> {
    let JsonValue::Object(object) = strip_doc_only_fields(schema) else {
        return BTreeMap::new();
    };
    object
        .into_iter()
        .filter(|(key, _)| {
            key != "properties" && key != "required" && !CONSTRAINT_FIELDS.contains(&key.as_str())
        })
        .collect()
}

fn changed_top_level_semantic_keys(previous: &JsonValue, next: &JsonValue) -> Vec<String> {
    let previous = top_level_semantic_fields(previous);
    let next = top_level_semantic_fields(next);
    previous
        .keys()
        .chain(next.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| previous.get(*key) != next.get(*key))
        .cloned()
        .collect()
}

fn constrained_top_level_property_names(schema: &JsonValue) -> Result<BTreeSet<String>, LixError> {
    let mut names = BTreeSet::new();

    collect_top_level_pointer_names(schema.get("x-lix-primary-key"), &mut names)?;
    if let Some(unique_groups) = schema.get("x-lix-unique").and_then(JsonValue::as_array) {
        for group in unique_groups {
            collect_top_level_pointer_names(Some(group), &mut names)?;
        }
    }
    if let Some(foreign_keys) = schema
        .get("x-lix-foreign-keys")
        .and_then(JsonValue::as_array)
    {
        for foreign_key in foreign_keys {
            collect_top_level_pointer_names(foreign_key.get("properties"), &mut names)?;
        }
    }
    if let Some(foreign_keys) = schema
        .get("x-lix-state-foreign-keys")
        .and_then(JsonValue::as_array)
    {
        for foreign_key in foreign_keys {
            collect_top_level_pointer_names(Some(foreign_key), &mut names)?;
        }
    }

    Ok(names)
}

fn collect_top_level_pointer_names(
    value: Option<&JsonValue>,
    names: &mut BTreeSet<String>,
) -> Result<(), LixError> {
    let Some(value) = value else {
        return Ok(());
    };
    let Some(pointers) = value.as_array() else {
        return schema_amendment_error(
            "schema constraint fields must contain arrays of JSON Pointers".to_string(),
        );
    };
    for pointer in pointers {
        let Some(pointer) = pointer.as_str() else {
            return schema_amendment_error(
                "schema constraint fields must contain JSON Pointer strings".to_string(),
            );
        };
        if let Some(name) = top_level_property_name(pointer)? {
            names.insert(name);
        }
    }
    Ok(())
}

fn schema_amendment_error<T>(message: String) -> Result<T, LixError> {
    Err(LixError::new(LixError::CODE_SCHEMA_DEFINITION, message))
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value as JsonValue};

    use super::validate_schema_amendment;

    fn base_schema() -> JsonValue {
        json!({
            "x-lix-key": "library_book",
            "type": "object",
            "x-lix-primary-key": ["/id"],
            "x-lix-unique": [["/isbn"]],
            "x-lix-foreign-keys": [
                {
                    "properties": ["/author_id"],
                    "references": {
                        "schemaKey": "library_author",
                        "properties": ["/id"]
                    }
                }
            ],
            "x-lix-state-foreign-keys": [
                ["/target_entity_pk", "/target_schema_key", "/target_file_id"]
            ],
            "properties": {
                "id": { "type": "string", "description": "Stable id" },
                "isbn": { "type": "string" },
                "title": { "type": "string", "title": "Title" },
                "author_id": { "type": "string" },
                "target_entity_pk": {
                    "type": "array",
                    "items": { "type": "string" }
                },
                "target_schema_key": { "type": "string" },
                "target_file_id": { "type": ["string", "null"] }
            },
            "required": [
                "id",
                "isbn",
                "title",
                "author_id",
                "target_entity_pk",
                "target_schema_key",
                "target_file_id"
            ],
            "additionalProperties": false
        })
    }

    #[test]
    fn allows_doc_only_changes_on_existing_properties() {
        let previous = base_schema();
        let mut next = base_schema();
        next["description"] = json!("A library book relation");
        next["title"] = json!("Library Book");
        next["$comment"] = json!("Top-level schema docs");
        next["deprecated"] = json!(false);
        next["properties"]["title"]["description"] = json!("Human readable title");
        next["properties"]["title"]["title"] = json!("Book title");
        next["properties"]["title"]["$comment"] = json!("Shown in schema docs");
        next["properties"]["title"]["deprecated"] = json!(true);

        validate_schema_amendment(&previous, &next).expect("doc-only changes are compatible");
    }

    #[test]
    fn allows_adding_optional_property() {
        let previous = base_schema();
        let mut next = base_schema();
        next["properties"]["subtitle"] = json!({
            "type": "string",
            "description": "Optional subtitle"
        });

        validate_schema_amendment(&previous, &next)
            .expect("optional property addition is compatible");
    }

    #[test]
    fn allows_empty_properties_to_grow_with_optional_properties() {
        let previous = json!({
            "x-lix-key": "library_empty",
            "type": "object",
            "properties": {},
            "additionalProperties": false
        });
        let next = json!({
            "x-lix-key": "library_empty",
            "type": "object",
            "properties": {
                "title": { "type": "string" }
            },
            "additionalProperties": false
        });

        validate_schema_amendment(&previous, &next)
            .expect("optional property addition from an empty schema is compatible");
    }

    #[test]
    fn accepts_cosmetic_constraint_list_reordering() {
        let mut previous = base_schema();
        previous["x-lix-unique"] = json!([["/isbn"], ["/title"]]);
        previous["x-lix-foreign-keys"] = json!([
            {
                "properties": ["/author_id"],
                "references": {
                    "schemaKey": "library_author",
                    "properties": ["/id"]
                }
            },
            {
                "properties": ["/isbn"],
                "references": {
                    "schemaKey": "library_isbn",
                    "properties": ["/id"]
                }
            }
        ]);
        previous["x-lix-state-foreign-keys"] = json!([
            ["/target_entity_pk", "/target_schema_key", "/target_file_id"],
            ["/other_entity_pk", "/other_schema_key", "/other_file_id"]
        ]);
        let mut next = previous.clone();
        next["x-lix-unique"] = json!([["/title"], ["/isbn"]]);
        next["x-lix-foreign-keys"] = json!([
            {
                "properties": ["/isbn"],
                "references": {
                    "schemaKey": "library_isbn",
                    "properties": ["/id"]
                }
            },
            {
                "properties": ["/author_id"],
                "references": {
                    "schemaKey": "library_author",
                    "properties": ["/id"]
                }
            }
        ]);
        next["x-lix-state-foreign-keys"] = json!([
            ["/other_entity_pk", "/other_schema_key", "/other_file_id"],
            ["/target_entity_pk", "/target_schema_key", "/target_file_id"]
        ]);

        validate_schema_amendment(&previous, &next)
            .expect("cosmetic constraint list ordering should not matter");
    }

    #[test]
    fn rejects_required_set_shrink() {
        let previous = base_schema();
        let mut next = base_schema();
        next["required"] = json!([
            "id",
            "isbn",
            "author_id",
            "target_entity_pk",
            "target_schema_key",
            "target_file_id"
        ]);

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("required properties must be frozen");

        assert!(
            error.message.contains("required properties"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_schema_key_change() {
        let previous = base_schema();
        let mut next = base_schema();
        next["x-lix-key"] = json!("library_periodical");

        let error =
            validate_schema_amendment(&previous, &next).expect_err("schema key must be stable");

        assert!(
            error.message.contains("x-lix-key"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_additional_properties_change() {
        let previous = base_schema();
        let mut next = base_schema();
        next["additionalProperties"] = json!(true);

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("additionalProperties must remain false");

        assert!(
            error.message.contains("additionalProperties"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_primary_key_change() {
        let previous = base_schema();
        let mut next = base_schema();
        next["x-lix-primary-key"] = json!(["/isbn"]);

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("primary-key changes are incompatible");

        assert!(
            error.message.contains("x-lix-primary-key"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_primary_key_reordering() {
        let mut previous = base_schema();
        previous["x-lix-primary-key"] = json!(["/id", "/isbn"]);
        let mut next = previous.clone();
        next["x-lix-primary-key"] = json!(["/isbn", "/id"]);

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("primary-key column order is semantic");

        assert!(
            error.message.contains("x-lix-primary-key"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_unique_constraint_change() {
        let previous = base_schema();
        let mut next = base_schema();
        next["x-lix-unique"] = json!([["/title"]]);

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("unique changes are incompatible");

        assert!(
            error.message.contains("x-lix-unique"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_foreign_key_change() {
        let previous = base_schema();
        let mut next = base_schema();
        next["x-lix-foreign-keys"][0]["references"]["schemaKey"] = json!("library_person");

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("foreign-key changes are incompatible");

        assert!(
            error.message.contains("x-lix-foreign-keys"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_inner_foreign_key_pointer_reordering() {
        let mut previous = base_schema();
        previous["x-lix-foreign-keys"] = json!([
            {
                "properties": ["/author_id", "/isbn"],
                "references": {
                    "schemaKey": "library_author",
                    "properties": ["/id", "/isbn"]
                }
            }
        ]);
        let mut next = previous.clone();
        next["x-lix-foreign-keys"] = json!([
            {
                "properties": ["/isbn", "/author_id"],
                "references": {
                    "schemaKey": "library_author",
                    "properties": ["/isbn", "/id"]
                }
            }
        ]);

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("FK tuple order is semantic and must remain frozen");

        assert!(
            error.message.contains("x-lix-foreign-keys"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_state_foreign_key_change() {
        let previous = base_schema();
        let mut next = base_schema();
        next["x-lix-state-foreign-keys"] = json!([]);

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("state foreign-key changes are incompatible");

        assert!(
            error.message.contains("x-lix-state-foreign-keys"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_existing_property_type_change() {
        let previous = base_schema();
        let mut next = base_schema();
        next["properties"]["title"]["type"] = json!("number");

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("existing property semantics must not change");

        assert!(
            error.message.contains("/title"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_nested_object_property_addition() {
        let mut previous = base_schema();
        previous["properties"]["metadata"] = json!({
            "type": "object",
            "properties": {
                "source": { "type": "string" }
            },
            "additionalProperties": false
        });
        let mut next = previous.clone();
        next["properties"]["metadata"]["properties"]["page"] = json!({ "type": "number" });

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("nested schema amendments are frozen for MVP");

        assert!(
            error.message.contains("/metadata"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_top_level_type_change() {
        let previous = base_schema();
        let mut next = base_schema();
        next["type"] = json!("array");

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("top-level schema semantics must not change");

        assert!(
            error.message.contains("top-level schema semantics"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_top_level_examples_change_and_names_field() {
        let previous = base_schema();
        let mut next = base_schema();
        next["examples"] = json!([{ "title": "Example" }]);

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("examples are not an amendment annotation in the MVP");

        assert!(
            error.message.contains("examples"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_existing_property_default_change() {
        let mut previous = base_schema();
        let mut next = base_schema();
        previous["properties"]["title"]["default"] = json!("Untitled");
        next["properties"]["title"]["default"] = json!("Draft");

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("existing defaults must not change");

        assert!(
            error.message.contains("/title"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_removed_property() {
        let previous = base_schema();
        let mut next = base_schema();
        next["properties"].as_object_mut().unwrap().remove("title");

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("properties must not be removed");

        assert!(
            error.message.contains("remove property '/title'"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_added_required_property() {
        let previous = base_schema();
        let mut next = base_schema();
        next["properties"]["subtitle"] = json!({ "type": "string" });
        next["required"]
            .as_array_mut()
            .unwrap()
            .push(json!("subtitle"));

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("new properties must be optional");

        assert!(
            error.message.contains("required"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_added_property_that_is_part_of_existing_constraints() {
        let mut previous = base_schema();
        previous["x-lix-unique"] = json!([["/subtitle"]]);
        let mut next = previous.clone();
        next["properties"]["subtitle"] = json!({ "type": "string" });

        let error = validate_schema_amendment(&previous, &next)
            .expect_err("new properties must not be constraint participants");

        assert!(
            error
                .message
                .contains("primary, unique, or foreign-key constraints"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn rejects_required_growth_for_existing_property() {
        let mut previous = base_schema();
        previous["required"]
            .as_array_mut()
            .unwrap()
            .retain(|value| value != "title");
        let next = base_schema();

        let error =
            validate_schema_amendment(&previous, &next).expect_err("required set must not grow");

        assert!(
            error.message.contains("cannot amend required properties"),
            "unexpected error: {error:?}"
        );
    }
}
