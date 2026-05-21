use crate::{validate_lix_schema, validate_lix_schema_definition};
use serde_json::json;

#[test]
fn validate_lix_schema_definition_passes_for_valid_schema() {
    let valid_schema = json!({
        "x-lix-key": "test_entity",
        "type": "object",
        "properties": {
            "id": { "type": "string" }
        },
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&valid_schema).is_ok());
}

#[test]
fn validate_lix_schema_definition_rejects_unprojectable_entity_properties() {
    let schema = json!({
        "x-lix-key": "test_entity",
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "kind": {}
        },
        "required": ["id", "kind"],
        "additionalProperties": false
    });

    let err = validate_lix_schema_definition(&schema).unwrap_err();
    assert!(
        err.to_string().contains("property '/kind'"),
        "error should identify the unprojectable property: {err:?}"
    );
    assert!(
        err.to_string().contains("SQL-projectable JSON Schema type"),
        "error should explain the projection requirement: {err:?}"
    );
}

#[test]
fn validate_lix_schema_definition_rejects_reserved_lix_property_prefixes() {
    for property_name in ["lixcol_entity_pk", "lix_internal", "lixfoo"] {
        let schema = json!({
            "x-lix-key": "test_entity",
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                property_name: { "type": "string" }
            },
            "required": ["id", property_name],
            "additionalProperties": false
        });

        let err = validate_lix_schema_definition(&schema)
            .expect_err("reserved property names should be rejected");
        assert!(
            err.to_string().contains(&format!(
                "property '/{property_name}' uses reserved prefix 'lix'"
            )),
            "error should identify the reserved property name: {err:?}"
        );
    }
}

#[test]
fn validate_lix_schema_definition_throws_for_invalid_schema() {
    let invalid_schema = json!({
        "type": "object",
        "properties": {
            "id": { "type": "string" }
        },
        "additionalProperties": false
    });

    let err = validate_lix_schema_definition(&invalid_schema).unwrap_err();
    assert!(err.to_string().contains("Invalid Lix schema definition"));
}

#[test]
fn validate_lix_schema_validates_both_schema_and_data_successfully() {
    let schema = json!({
        "x-lix-key": "user",
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "name": { "type": "string" }
        },
        "required": ["id", "name"],
        "additionalProperties": false
    });

    let valid_data = json!({
        "id": "123",
        "name": "John Doe"
    });

    assert!(validate_lix_schema(&schema, &valid_data).is_ok());
}

#[test]
fn validate_lix_schema_throws_when_schema_is_invalid() {
    let invalid_schema = json!({
        "type": "object",
        "properties": {
            "id": { "type": "string" }
        },
        "additionalProperties": false
    });

    let data = json!({ "id": "123" });

    let err = validate_lix_schema(&invalid_schema, &data).unwrap_err();
    assert!(err.to_string().contains("Invalid Lix schema definition"));
}

#[test]
fn validate_lix_schema_throws_when_data_does_not_match_schema() {
    let schema = json!({
        "x-lix-key": "user",
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "name": { "type": "string" }
        },
        "required": ["id", "name"],
        "additionalProperties": false
    });

    let invalid_data = json!({ "id": "123" });

    let err = validate_lix_schema(&schema, &invalid_data).unwrap_err();
    assert!(err.to_string().contains("Data validation failed"));
}

#[test]
fn validate_lix_schema_definition_rejects_when_additional_properties_missing() {
    let schema = json!({
        "x-lix-key": "user",
        "type": "object",
        "properties": {
            "id": { "type": "string" }
        },
        "required": ["id"]
    });

    let err = validate_lix_schema_definition(&schema).unwrap_err();
    assert!(err.to_string().contains("Invalid Lix schema definition"));
}

#[test]
fn additional_properties_must_be_false() {
    let schema_with_additional_props = json!({
        "x-lix-key": "user",
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "name": { "type": "string" }
        },
        "required": ["id", "name"],
        "additionalProperties": true
    });

    assert!(validate_lix_schema_definition(&schema_with_additional_props).is_err());

    let valid_schema = json!({
        "x-lix-key": "user",
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "name": { "type": "string" }
        },
        "required": ["id", "name"],
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&valid_schema).is_ok());

    let data = json!({
        "id": "123",
        "name": "John Doe",
        "extraField": "not allowed"
    });

    let err = validate_lix_schema(&valid_schema, &data).unwrap_err();
    assert!(err.to_string().contains("Data validation failed"));
}

#[test]
fn validate_lix_schema_definition_rejects_missing_primary_key_properties() {
    let schema = json!({
        "x-lix-key": "missing_pk",
        "type": "object",
        "properties": {
            "value": { "type": "string" }
        },
        "required": ["value"],
        "x-lix-primary-key": ["/entity_pk"],
        "additionalProperties": false
    });

    let err = validate_lix_schema_definition(&schema).unwrap_err();
    assert!(err
        .to_string()
        .contains("x-lix-primary-key references missing property"));
}

#[test]
fn validate_lix_schema_definition_rejects_non_string_primary_key_properties() {
    let schema = json!({
        "x-lix-key": "numeric_pk",
        "type": "object",
        "properties": {
            "id": { "type": "number" },
            "value": { "type": "string" }
        },
        "required": ["id", "value"],
        "x-lix-primary-key": ["/id"],
        "additionalProperties": false
    });

    let err = validate_lix_schema_definition(&schema).unwrap_err();
    assert!(err
        .to_string()
        .contains("x-lix-primary-key property \"/id\" must have type \"string\""));
}

#[test]
fn validate_lix_schema_definition_rejects_optional_primary_key_properties() {
    let schema = json!({
        "x-lix-key": "optional_pk",
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "value": { "type": "string" }
        },
        "required": ["value"],
        "x-lix-primary-key": ["/id"],
        "additionalProperties": false
    });

    let err = validate_lix_schema_definition(&schema)
        .expect_err("primary-key property should be required");
    assert!(err
        .to_string()
        .contains("x-lix-primary-key property \"/id\" must be required"));
}

#[test]
fn validate_lix_schema_definition_rejects_missing_unique_constraint_properties() {
    let schema = json!({
        "x-lix-key": "missing_unique",
        "type": "object",
        "properties": {
            "value": { "type": "string" }
        },
        "x-lix-unique": [["/entity_pk", "/value"]],
        "additionalProperties": false
    });

    let err = validate_lix_schema_definition(&schema).unwrap_err();
    assert!(err
        .to_string()
        .contains("x-lix-unique references missing property"));
}

#[test]
fn x_key_is_required() {
    let schema = json!({
        "type": "object",
        "x-lix-key": null,
        "properties": {
            "name": { "type": "string" }
        },
        "required": ["name"],
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_err());
}

#[test]
fn x_lix_key_must_be_snake_case() {
    let base_schema = json!({
        "type": "object",
        "properties": {
            "name": { "type": "string" }
        },
        "required": ["name"],
        "additionalProperties": false
    });

    let invalid_keys = [
        "Invalid-Key!",
        "also.invalid",
        "123starts_with_number",
        "contains space",
        "camelCaseKey",
        "UPPER_CASE",
        "mixed-Case_Value",
    ];
    for key in invalid_keys {
        let mut schema = base_schema.clone();
        schema["x-lix-key"] = json!(key);
        assert!(validate_lix_schema_definition(&schema).is_err());
    }

    let valid_keys = ["abc", "abc123", "abc_123", "a", "snake_case_key"];
    for key in valid_keys {
        let mut schema = base_schema.clone();
        schema["x-lix-key"] = json!(key);
        assert!(validate_lix_schema_definition(&schema).is_ok());
    }
}

#[test]
fn x_lix_unique_is_optional() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "mock",
        "properties": {
            "name": { "type": "string" }
        },
        "required": ["name"],
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_ok());
}

#[test]
fn x_lix_unique_must_be_array_of_arrays_when_present() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "mock",
        "x-lix-unique": [["/id"], ["/name", "/age"]],
        "properties": {
            "id": { "type": "string" },
            "name": { "type": "string" },
            "age": { "type": "number" }
        },
        "required": ["id", "name", "age"],
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_ok());
}

#[test]
fn x_lix_unique_fails_with_invalid_structure() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "mock",
        "x-lix-unique": ["/id", "/name"],
        "properties": {
            "id": { "type": "string" },
            "name": { "type": "string" }
        },
        "required": ["id", "name"],
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_err());
}

#[test]
fn x_lix_primary_key_must_include_at_least_one_unique_pointer() {
    let base_schema = json!({
        "type": "object",
        "x-lix-key": "mock",
        "properties": {
            "id": { "type": "string" }
        },
        "required": ["id"],
        "additionalProperties": false
    });

    let mut empty_pk = base_schema.clone();
    empty_pk["x-lix-primary-key"] = json!([]);
    assert!(validate_lix_schema_definition(&empty_pk).is_err());

    let mut duplicate_pk = base_schema.clone();
    duplicate_pk["x-lix-primary-key"] = json!(["/id", "/id"]);
    assert!(validate_lix_schema_definition(&duplicate_pk).is_err());

    let mut valid_pk = base_schema.clone();
    valid_pk["x-lix-primary-key"] = json!(["/id"]);
    assert!(validate_lix_schema_definition(&valid_pk).is_ok());
}

#[test]
fn x_lix_unique_groups_must_include_unique_pointers() {
    let base_schema = json!({
        "type": "object",
        "x-lix-key": "mock",
        "properties": {
            "id": { "type": "string" },
            "email": { "type": "string" }
        },
        "required": ["id", "email"],
        "additionalProperties": false
    });

    let mut empty_group = base_schema.clone();
    empty_group["x-lix-unique"] = json!([[]]);
    assert!(validate_lix_schema_definition(&empty_group).is_err());

    let mut duplicate_pointers = base_schema.clone();
    duplicate_pointers["x-lix-unique"] = json!([["/email", "/email"]]);
    assert!(validate_lix_schema_definition(&duplicate_pointers).is_err());

    let mut valid_unique = base_schema.clone();
    valid_unique["x-lix-unique"] = json!([["/email"]]);
    assert!(validate_lix_schema_definition(&valid_unique).is_ok());
}

#[test]
fn x_lix_entity_views_is_rejected() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "mock",
        "x-lix-entity-views": ["lix_state", "lix_state_by_version"],
        "properties": {
            "name": { "type": "string" }
        },
        "required": ["name"],
        "additionalProperties": false
    });

    let err =
        validate_lix_schema_definition(&schema).expect_err("x-lix-entity-views should be rejected");
    assert!(err.to_string().contains("x-lix-entity-views"));
}

#[test]
fn x_lix_primary_key_is_optional() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "mock",
        "properties": {
            "name": { "type": "string" }
        },
        "required": ["name"],
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_ok());
}

#[test]
fn x_lix_primary_key_must_be_array_of_strings_when_present() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "mock",
        "x-lix-primary-key": ["/id", "/version"],
        "properties": {
            "id": { "type": "string" },
            "version": { "type": "string" },
            "name": { "type": "string" }
        },
        "required": ["id", "version", "name"],
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_ok());
}

#[test]
fn x_lix_foreign_keys_is_optional() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "blog_post",
        "properties": {
            "id": { "type": "string" },
            "author_id": { "type": "string" }
        },
        "required": ["id", "author_id"],
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_ok());
}

#[test]
fn x_lix_foreign_keys_with_valid_structure() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "blog_post",
        "x-lix-foreign-keys": [
            {
                "properties": ["/author_id"],
                "references": {
                    "schemaKey": "user_profile",
                    "properties": ["/id"]
                }
            },
            {
                "properties": ["/category_id"],
                "references": {
                    "schemaKey": "post_category",
                    "properties": ["/id"]
                }
            }
        ],
        "properties": {
            "id": { "type": "string" },
            "author_id": { "type": "string" },
            "category_id": { "type": "string" }
        },
        "required": ["id", "author_id", "category_id"],
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_ok());
}

#[test]
fn x_lix_foreign_keys_reject_duplicate_pointers() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "invalid_fk_duplicates",
        "x-lix-foreign-keys": [
            {
                "properties": ["/local", "/local"],
                "references": {
                    "schemaKey": "remote_schema",
                    "properties": ["/id", "/version"]
                }
            }
        ],
        "properties": {
            "local": { "type": "string" }
        },
        "required": ["local"],
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_err());
}

#[test]
fn x_lix_foreign_keys_fails_without_required_fields() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "blog_post",
        "x-lix-foreign-keys": [
            {
                "properties": ["/author_id"]
            }
        ],
        "properties": {
            "id": { "type": "string" },
            "author_id": { "type": "string" }
        },
        "required": ["id", "author_id"],
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_err());
}

#[test]
fn x_lix_foreign_keys_use_schema_key_identity_only() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "comment",
        "x-lix-foreign-keys": [
            {
                "properties": ["/post_id"],
                "references": {
                    "schemaKey": "blog_post",
                    "properties": ["/id"]
                }
            }
        ],
        "properties": {
            "id": { "type": "string" },
            "post_id": { "type": "string" }
        },
        "required": ["id", "post_id"],
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_ok());
}

#[test]
fn x_lix_foreign_keys_rejects_mode_field() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "child_entity",
        "x-lix-primary-key": ["/id"],
        "x-lix-foreign-keys": [
            {
                "properties": ["/parent_id"],
                "references": { "schemaKey": "parent_entity", "properties": ["/id"] },
                "mode": "materialized"
            }
        ],
        "properties": {
            "id": { "type": "string" },
            "parent_id": { "type": "string" }
        },
        "required": ["id", "parent_id"],
        "additionalProperties": false
    });

    let err = validate_lix_schema_definition(&schema).expect_err("mode should be rejected");
    assert!(err.to_string().contains("mode"));
}

#[test]
fn x_lix_foreign_keys_rejects_scope_field() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "child_entity",
        "x-lix-primary-key": ["/id"],
        "x-lix-foreign-keys": [
            {
                "properties": ["/parent_id"],
                "references": { "schemaKey": "parent_entity", "properties": ["/id"] },
                "scope": ["file_id"]
            }
        ],
        "properties": {
            "id": { "type": "string" },
            "parent_id": { "type": "string" }
        },
        "required": ["id", "parent_id"],
        "additionalProperties": false
    });

    let err = validate_lix_schema_definition(&schema).expect_err("scope should be rejected");
    assert!(err.to_string().contains("scope"));
}

#[test]
fn x_lix_state_foreign_keys_with_ordered_state_address_tuple() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "label_assignment",
        "x-lix-state-foreign-keys": [
            ["/target_entity_pk", "/target_schema_key", "/target_file_id"]
        ],
        "x-lix-foreign-keys": [
            {
                "properties": ["/label_id"],
                "references": {
                    "schemaKey": "lix_label",
                    "properties": ["/id"]
                }
            }
        ],
        "properties": {
            "target_entity_pk": {
                "type": "array",
                "items": { "type": "string" },
                "minItems": 1
            },
            "target_schema_key": { "type": "string" },
            "target_file_id": { "type": ["string", "null"] },
            "label_id": { "type": "string" }
        },
        "required": ["target_entity_pk", "target_schema_key", "target_file_id", "label_id"],
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_ok());
}

#[test]
fn x_lix_state_foreign_keys_rejects_wrong_tuple_order_by_type() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "bad_label_assignment",
        "x-lix-state-foreign-keys": [
            ["/target_schema_key", "/target_entity_pk", "/target_file_id"]
        ],
        "properties": {
            "target_entity_pk": {
                "type": "array",
                "items": { "type": "string" },
                "minItems": 1
            },
            "target_schema_key": { "type": "string" },
            "target_file_id": { "type": ["string", "null"] }
        },
        "required": ["target_entity_pk", "target_schema_key", "target_file_id"],
        "additionalProperties": false
    });

    let err =
        validate_lix_schema_definition(&schema).expect_err("wrong tuple order should be rejected");
    assert!(
        err.message.contains("[entity_pk, schema_key, file_id]"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn x_lix_state_foreign_keys_requires_address_tuple_properties() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "optional_label_assignment",
        "x-lix-state-foreign-keys": [
            ["/target_entity_pk", "/target_schema_key", "/target_file_id"]
        ],
        "properties": {
            "target_entity_pk": {
                "type": "array",
                "items": { "type": "string" },
                "minItems": 1
            },
            "target_schema_key": { "type": "string" },
            "target_file_id": { "type": ["string", "null"] }
        },
        "required": ["target_entity_pk", "target_schema_key"],
        "additionalProperties": false
    });

    let err = validate_lix_schema_definition(&schema)
        .expect_err("state foreign key tuple fields should be required");
    assert!(
        err.message.contains("file_id") && err.message.contains("must be required"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn x_lix_foreign_keys_treat_schema_keys_literally() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "custom_label_assignment",
        "x-lix-foreign-keys": [
            {
                "properties": ["/label_id"],
                "references": {
                    "schemaKey": "label",
                    "properties": ["/id"]
                }
            }
        ],
        "properties": {
            "label_id": { "type": "string" }
        },
        "required": ["label_id"],
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_ok());
}

#[test]
fn x_lix_default_accepts_valid_cel_expression() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "mock",
        "properties": {
            "id": { "type": "string", "x-lix-default": "lix_uuid_v7()" }
        },
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_ok());
}

#[test]
fn x_lix_default_rejects_invalid_cel_expression() {
    let schema = json!({
        "type": "object",
        "x-lix-key": "mock",
        "properties": {
            "id": { "type": "string", "x-lix-default": "lix_uuid_v7(" }
        },
        "additionalProperties": false
    });

    assert!(validate_lix_schema_definition(&schema).is_err());
}
