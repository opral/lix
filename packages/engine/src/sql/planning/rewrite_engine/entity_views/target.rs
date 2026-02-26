use std::collections::{BTreeSet, HashMap};

use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use sqlparser::ast::{ObjectName, ObjectNamePart};

use crate::builtin_schema::builtin_schema_definition;
use crate::cel::CelEvaluator;
use crate::schema::{SchemaProvider, SqlStoredSchemaProvider};
use crate::{LixBackend, LixError};

const RESERVED_VIEW_NAMES: &[&str] = &[
    "lix_state",
    "lix_state_by_version",
    "lix_state_history",
    "lix_version",
    "lix_active_version",
    "lix_active_account",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntityViewVariant {
    Base,
    ByVersion,
    History,
}

#[derive(Debug, Clone)]
pub(crate) struct EntityViewOverridePredicate {
    pub column: String,
    pub value: JsonValue,
}

#[derive(Debug, Clone)]
pub(crate) struct PrimaryKeyField {
    pub pointer: String,
    pub path: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct EntityViewTarget {
    pub view_name: String,
    pub schema_key: String,
    pub variant: EntityViewVariant,
    pub schema: JsonValue,
    pub properties: Vec<String>,
    pub primary_key_fields: Vec<PrimaryKeyField>,
    pub primary_key_properties: Vec<String>,
    pub schema_version: String,
    pub file_id_override: Option<String>,
    pub plugin_key_override: Option<String>,
    pub version_id_override: Option<String>,
    pub override_predicates: Vec<EntityViewOverridePredicate>,
}

pub(crate) fn resolve_target_from_object_name(
    name: &ObjectName,
) -> Result<Option<EntityViewTarget>, LixError> {
    let Some(view_name) = object_name_terminal(name) else {
        return Ok(None);
    };
    resolve_target_from_view_name(&view_name)
}

pub(crate) async fn resolve_target_from_object_name_with_backend(
    backend: &dyn LixBackend,
    name: &ObjectName,
) -> Result<Option<EntityViewTarget>, LixError> {
    let Some(view_name) = object_name_terminal(name) else {
        return Ok(None);
    };
    resolve_target_from_view_name_with_backend(backend, &view_name).await
}

pub(crate) fn resolve_target_from_view_name(
    view_name: &str,
) -> Result<Option<EntityViewTarget>, LixError> {
    let Some((schema_key, variant)) = parse_view_name(view_name) else {
        return Ok(None);
    };

    let Some(schema) = builtin_schema_definition(&schema_key) else {
        return Ok(None);
    };

    build_target_from_schema(view_name, &schema_key, variant, schema)
}

pub(crate) async fn resolve_target_from_view_name_with_backend(
    backend: &dyn LixBackend,
    view_name: &str,
) -> Result<Option<EntityViewTarget>, LixError> {
    let Some((schema_key, variant)) = parse_view_name(view_name) else {
        return Ok(None);
    };

    if let Some(schema) = builtin_schema_definition(&schema_key) {
        return build_target_from_schema(view_name, &schema_key, variant, schema);
    }

    let mut provider = SqlStoredSchemaProvider::new(backend);
    let schema = match provider.load_latest_schema(&schema_key).await {
        Ok(schema) => schema,
        Err(err) if err.message.contains("is not stored") => return Ok(None),
        Err(err) => return Err(err),
    };

    build_target_from_schema(view_name, &schema_key, variant, &schema)
}

pub(crate) async fn resolve_targets_with_backend(
    backend: &dyn LixBackend,
    view_names: &[String],
) -> Result<HashMap<String, EntityViewTarget>, LixError> {
    let mut out = HashMap::new();
    let unique = view_names
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    for name in unique {
        if let Some(target) = resolve_target_from_view_name_with_backend(backend, &name).await? {
            out.insert(name, target);
        }
    }
    Ok(out)
}

fn build_target_from_schema(
    view_name: &str,
    schema_key: &str,
    variant: EntityViewVariant,
    schema: &JsonValue,
) -> Result<Option<EntityViewTarget>, LixError> {
    if !variant_enabled(schema, variant) {
        return Ok(None);
    }

    if let Some(stored_key) = schema.get("x-lix-key").and_then(JsonValue::as_str) {
        if !stored_key.eq_ignore_ascii_case(schema_key) {
            return Ok(None);
        }
    }

    let mut properties = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| properties.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    properties.sort();

    let primary_key_fields = extract_primary_key_fields(schema)?;
    let primary_key_properties =
        extract_top_level_primary_key_properties(&primary_key_fields, &properties);
    let schema_version = schema
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .unwrap_or("1")
        .to_string();
    let evaluator = CelEvaluator::new();

    let file_id_override =
        extract_lixcol_string_override(schema, schema_key, "lixcol_file_id", &evaluator)?;
    let plugin_key_override =
        extract_lixcol_string_override(schema, schema_key, "lixcol_plugin_key", &evaluator)?;
    let version_id_override =
        extract_lixcol_string_override(schema, schema_key, "lixcol_version_id", &evaluator)?;
    let override_predicates = collect_override_predicates(schema, schema_key, variant, &evaluator)?;

    Ok(Some(EntityViewTarget {
        view_name: view_name.to_string(),
        schema_key: schema_key.to_string(),
        variant,
        schema: schema.clone(),
        properties,
        primary_key_fields,
        primary_key_properties,
        schema_version,
        file_id_override,
        plugin_key_override,
        version_id_override,
        override_predicates,
    }))
}

fn parse_view_name(view_name: &str) -> Option<(String, EntityViewVariant)> {
    if view_name.is_empty() {
        return None;
    }
    let lower = view_name.to_ascii_lowercase();
    if RESERVED_VIEW_NAMES
        .iter()
        .any(|name| lower.eq_ignore_ascii_case(name))
    {
        return None;
    }
    if lower.starts_with("lix_internal_") {
        return None;
    }
    if !lower.starts_with("lix_") {
        return None;
    }

    if let Some(schema_key) = view_name.strip_suffix("_by_version") {
        if schema_key.is_empty() {
            return None;
        }
        return Some((schema_key.to_string(), EntityViewVariant::ByVersion));
    }
    if let Some(schema_key) = view_name.strip_suffix("_history") {
        if schema_key.is_empty() {
            return None;
        }
        return Some((schema_key.to_string(), EntityViewVariant::History));
    }
    Some((view_name.to_string(), EntityViewVariant::Base))
}

fn variant_enabled(schema: &JsonValue, variant: EntityViewVariant) -> bool {
    let Some(selected) = schema
        .get("x-lix-entity-views")
        .and_then(JsonValue::as_array)
    else {
        return true;
    };
    let key = match variant {
        EntityViewVariant::Base => "lix_state",
        EntityViewVariant::ByVersion => "lix_state_by_version",
        EntityViewVariant::History => "lix_state_history",
    };
    selected.iter().any(|entry| {
        entry
            .as_str()
            .map(|value| value.eq_ignore_ascii_case(key))
            .unwrap_or(false)
    })
}

fn extract_primary_key_fields(schema: &JsonValue) -> Result<Vec<PrimaryKeyField>, LixError> {
    let Some(pk) = schema
        .get("x-lix-primary-key")
        .and_then(JsonValue::as_array)
    else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for pointer in pk {
        let Some(pointer) = pointer.as_str() else {
            continue;
        };
        let path = parse_json_pointer_path(pointer)?;
        if path.is_empty() {
            continue;
        }
        out.push(PrimaryKeyField {
            pointer: pointer.to_string(),
            path,
        });
    }
    Ok(out)
}

fn extract_top_level_primary_key_properties(
    fields: &[PrimaryKeyField],
    properties: &[String],
) -> Vec<String> {
    let mut out = Vec::new();
    for field in fields {
        if field.path.len() != 1 {
            continue;
        }
        let property = &field.path[0];
        if properties.iter().any(|prop| prop == property) && !out.iter().any(|p| p == property) {
            out.push(property.clone());
        }
    }
    out
}

fn parse_json_pointer_path(pointer: &str) -> Result<Vec<String>, LixError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(LixError {
            message: format!("invalid x-lix-primary-key pointer '{pointer}'"),
        });
    }
    let mut path = Vec::new();
    for segment in pointer[1..].split('/') {
        let decoded = decode_json_pointer_segment(segment)?;
        if decoded.is_empty() {
            continue;
        }
        path.push(decoded);
    }
    Ok(path)
}

fn decode_json_pointer_segment(segment: &str) -> Result<String, LixError> {
    let mut out = String::new();
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') => out.push('~'),
                Some('1') => out.push('/'),
                _ => {
                    return Err(LixError {
                        message: format!("invalid JSON pointer segment '{segment}'"),
                    })
                }
            }
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

fn raw_lixcol_override_expression<'a>(schema: &'a JsonValue, key: &str) -> Option<&'a str> {
    schema
        .get("x-lix-override-lixcols")
        .and_then(JsonValue::as_object)
        .and_then(|overrides| overrides.get(key))
        .and_then(JsonValue::as_str)
}

fn evaluate_lixcol_override(
    schema: &JsonValue,
    schema_key: &str,
    key: &str,
    evaluator: &CelEvaluator,
) -> Result<Option<JsonValue>, LixError> {
    let Some(raw_expression) = raw_lixcol_override_expression(schema, key) else {
        return Ok(None);
    };
    let expression = raw_expression.trim();
    if expression.is_empty() {
        return Ok(None);
    }
    evaluator
        .evaluate(expression, &JsonMap::new())
        .map(Some)
        .map_err(|err| LixError {
            message: format!(
                "invalid x-lix-override-lixcols expression for '{}.{}': {}",
                schema_key, key, err.message
            ),
        })
}

fn extract_lixcol_string_override(
    schema: &JsonValue,
    schema_key: &str,
    key: &str,
    evaluator: &CelEvaluator,
) -> Result<Option<String>, LixError> {
    let Some(value) = evaluate_lixcol_override(schema, schema_key, key, evaluator)? else {
        return Ok(None);
    };
    match value {
        JsonValue::String(text) => Ok(Some(text)),
        _ => Err(LixError {
            message: format!(
                "x-lix-override-lixcols '{}.{}' must evaluate to a string",
                schema_key, key
            ),
        }),
    }
}

fn extract_lixcol_scalar_override(
    schema: &JsonValue,
    schema_key: &str,
    key: &str,
    evaluator: &CelEvaluator,
) -> Result<Option<JsonValue>, LixError> {
    let Some(value) = evaluate_lixcol_override(schema, schema_key, key, evaluator)? else {
        return Ok(None);
    };
    match value {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            Ok(Some(value))
        }
        JsonValue::Array(_) | JsonValue::Object(_) => Err(LixError {
            message: format!(
                "x-lix-override-lixcols '{}.{}' must evaluate to a scalar or null",
                schema_key, key
            ),
        }),
    }
}

fn collect_override_predicates(
    schema: &JsonValue,
    schema_key: &str,
    variant: EntityViewVariant,
    evaluator: &CelEvaluator,
) -> Result<Vec<EntityViewOverridePredicate>, LixError> {
    let keys = [
        "lixcol_entity_id",
        "lixcol_file_id",
        "lixcol_plugin_key",
        "lixcol_inherited_from_version_id",
        "lixcol_metadata",
        "lixcol_untracked",
    ];
    let mut out = Vec::new();
    for key in keys {
        if !override_allowed_for_variant(key, variant) {
            continue;
        }
        let Some(column) = override_column_name(key) else {
            continue;
        };
        let Some(value) = extract_lixcol_scalar_override(schema, schema_key, key, evaluator)?
        else {
            continue;
        };
        out.push(EntityViewOverridePredicate {
            column: column.to_string(),
            value,
        });
    }
    Ok(out)
}

fn override_column_name(key: &str) -> Option<&'static str> {
    Some(match key {
        "lixcol_entity_id" => "entity_id",
        "lixcol_file_id" => "file_id",
        "lixcol_plugin_key" => "plugin_key",
        "lixcol_inherited_from_version_id" => "inherited_from_version_id",
        "lixcol_metadata" => "metadata",
        "lixcol_untracked" => "untracked",
        _ => return None,
    })
}

fn override_allowed_for_variant(key: &str, variant: EntityViewVariant) -> bool {
    match key {
        "lixcol_inherited_from_version_id" | "lixcol_untracked" => {
            variant == EntityViewVariant::Base || variant == EntityViewVariant::ByVersion
        }
        _ => true,
    }
}

fn object_name_terminal(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.clone())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{build_target_from_schema, resolve_target_from_view_name, EntityViewVariant};

    #[test]
    fn resolves_builtin_by_version_view() {
        let target = resolve_target_from_view_name("lix_key_value_by_version")
            .expect("resolve should succeed")
            .expect("target should resolve");
        assert_eq!(target.schema_key, "lix_key_value");
    }

    #[test]
    fn resolves_nested_primary_key_fields_for_lix_stored_schema() {
        let target = resolve_target_from_view_name("lix_stored_schema_by_version")
            .expect("resolve should succeed")
            .expect("target should resolve");
        assert_eq!(target.schema_key, "lix_stored_schema");
        assert_eq!(target.primary_key_fields.len(), 2);
        assert_eq!(target.primary_key_fields[0].pointer, "/value/x-lix-key");
        assert_eq!(target.primary_key_fields[1].pointer, "/value/x-lix-version");
        assert!(target.primary_key_properties.is_empty());
    }

    #[test]
    fn evaluates_cel_override_values_for_target_resolution() {
        let schema = json!({
            "x-lix-key": "lix_custom_entity",
            "x-lix-version": "1",
                "x-lix-override-lixcols": {
                    "lixcol_file_id": "'file-custom'",
                    "lixcol_plugin_key": "'plugin-custom'",
                    "lixcol_version_id": "'version-custom'",
                    "lixcol_untracked": "true"
                },
            "type": "object",
            "properties": {
                "id": { "type": "string" }
            },
            "required": ["id"],
            "additionalProperties": false
        });

        let target = build_target_from_schema(
            "lix_custom_entity",
            "lix_custom_entity",
            EntityViewVariant::Base,
            &schema,
        )
        .expect("target resolution should succeed")
        .expect("target should resolve");

        assert_eq!(target.file_id_override.as_deref(), Some("file-custom"));
        assert_eq!(target.plugin_key_override.as_deref(), Some("plugin-custom"));
        assert_eq!(
            target.version_id_override.as_deref(),
            Some("version-custom")
        );
        assert_eq!(target.override_predicates.len(), 3);
        assert!(target
            .override_predicates
            .iter()
            .any(|predicate| predicate.column == "untracked" && predicate.value == json!(true)));
    }

    #[test]
    fn rejects_invalid_cel_override_expression() {
        let schema = json!({
            "x-lix-key": "lix_custom_entity",
            "x-lix-version": "1",
            "x-lix-override-lixcols": {
                "lixcol_file_id": "lix_uuid_v7("
            },
            "type": "object",
            "properties": {
                "id": { "type": "string" }
            },
            "required": ["id"],
            "additionalProperties": false
        });

        let err = build_target_from_schema(
            "lix_custom_entity",
            "lix_custom_entity",
            EntityViewVariant::Base,
            &schema,
        )
        .expect_err("invalid CEL expression should fail target resolution");
        assert!(err
            .to_string()
            .contains("invalid x-lix-override-lixcols expression"));
    }

    #[test]
    fn rejects_non_string_file_id_override() {
        let schema = json!({
            "x-lix-key": "lix_custom_entity",
            "x-lix-version": "1",
            "x-lix-override-lixcols": {
                "lixcol_file_id": "1"
            },
            "type": "object",
            "properties": {
                "id": { "type": "string" }
            },
            "required": ["id"],
            "additionalProperties": false
        });

        let err = build_target_from_schema(
            "lix_custom_entity",
            "lix_custom_entity",
            EntityViewVariant::Base,
            &schema,
        )
        .expect_err("non-string file_id override should fail target resolution");
        assert!(err.to_string().contains("must evaluate to a string"));
    }
}
