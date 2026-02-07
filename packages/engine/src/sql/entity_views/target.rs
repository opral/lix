use std::collections::{BTreeSet, HashMap};

use serde_json::Value as JsonValue;
use sqlparser::ast::{ObjectName, ObjectNamePart};

use crate::builtin_schema::{builtin_schema_definition, decode_lixcol_literal};
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
pub(crate) struct EntityViewTarget {
    pub view_name: String,
    pub schema_key: String,
    pub variant: EntityViewVariant,
    pub schema: JsonValue,
    pub properties: Vec<String>,
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

    let primary_key_properties = extract_primary_key_properties(schema, &properties)?;
    let schema_version = schema
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .unwrap_or("1")
        .to_string();

    let file_id_override = extract_lixcol_override(schema, "lixcol_file_id");
    let plugin_key_override = extract_lixcol_override(schema, "lixcol_plugin_key");
    let version_id_override = extract_lixcol_override(schema, "lixcol_version_id");
    let override_predicates = collect_override_predicates(schema, variant);

    Ok(Some(EntityViewTarget {
        view_name: view_name.to_string(),
        schema_key: schema_key.to_string(),
        variant,
        schema: schema.clone(),
        properties,
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
        EntityViewVariant::Base => "state",
        EntityViewVariant::ByVersion => "state_by_version",
        EntityViewVariant::History => "state_history",
    };
    selected.iter().any(|entry| {
        entry
            .as_str()
            .map(|value| value.eq_ignore_ascii_case(key))
            .unwrap_or(false)
    })
}

fn extract_primary_key_properties(
    schema: &JsonValue,
    properties: &[String],
) -> Result<Vec<String>, LixError> {
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
        let Some(property) = top_level_pointer_property(pointer)? else {
            continue;
        };
        if properties.iter().any(|prop| prop == &property) {
            out.push(property);
        }
    }
    Ok(out)
}

fn top_level_pointer_property(pointer: &str) -> Result<Option<String>, LixError> {
    if pointer.is_empty() {
        return Ok(None);
    }
    if !pointer.starts_with('/') {
        return Err(LixError {
            message: format!("invalid x-lix-primary-key pointer '{pointer}'"),
        });
    }
    if pointer[1..].contains('/') {
        return Ok(None);
    }
    let segment = decode_json_pointer_segment(&pointer[1..])?;
    if segment.is_empty() {
        return Ok(None);
    }
    Ok(Some(segment))
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

fn extract_lixcol_override(schema: &JsonValue, key: &str) -> Option<String> {
    schema
        .get("x-lix-override-lixcols")
        .and_then(JsonValue::as_object)
        .and_then(|overrides| overrides.get(key))
        .and_then(JsonValue::as_str)
        .map(decode_lixcol_literal)
}

fn extract_lixcol_literal_override(schema: &JsonValue, key: &str) -> Option<JsonValue> {
    let raw = schema
        .get("x-lix-override-lixcols")
        .and_then(JsonValue::as_object)
        .and_then(|overrides| overrides.get(key))
        .and_then(JsonValue::as_str)?
        .trim()
        .to_string();
    if raw.is_empty() {
        return None;
    }
    let value = serde_json::from_str::<JsonValue>(&raw).ok()?;
    match value {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            Some(value)
        }
        JsonValue::Array(_) | JsonValue::Object(_) => None,
    }
}

fn collect_override_predicates(
    schema: &JsonValue,
    variant: EntityViewVariant,
) -> Vec<EntityViewOverridePredicate> {
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
        let Some(value) = extract_lixcol_literal_override(schema, key) else {
            continue;
        };
        out.push(EntityViewOverridePredicate {
            column: column.to_string(),
            value,
        });
    }
    out
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
    use super::resolve_target_from_view_name;

    #[test]
    fn resolves_builtin_by_version_view() {
        let target = resolve_target_from_view_name("lix_key_value_by_version")
            .expect("resolve should succeed")
            .expect("target should resolve");
        assert_eq!(target.schema_key, "lix_key_value");
    }
}
