mod binding;
mod registry;

use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::OnceLock;

use crate::schema::{
    builtin_schema_definition, builtin_schema_keys, collect_dynamic_entity_surface_overrides,
    decode_lixcol_literal, DynamicEntitySurfaceOverride, LixcolOverrideValue,
    SchemaAnnotationEvaluator,
};
use crate::LixError;

#[allow(unused_imports)]
pub(crate) use registry::*;

#[allow(unused_imports)]
pub(crate) use binding::{
    bind_filesystem_relation, bind_named_relation, bind_registry_relation, bind_schema_relation,
    bind_surface_relation, bind_version_relation, FilesystemProjectionScope,
    FilesystemRelationBinding, FilesystemRelationKind, RelationBindContext, RelationBinding,
    SchemaRelationBinding, StoredVersionHeadSourceBinding, VersionDescriptorSourceBinding,
    VersionHeadSourceBinding, VersionRelationBinding,
};

pub(crate) fn build_builtin_surface_registry() -> SurfaceRegistry {
    let mut registry = SurfaceRegistry::new();
    registry.insert_descriptors(builtin_surface_descriptors());
    register_builtin_entity_surfaces(&mut registry);
    registry
}

pub(crate) fn dynamic_entity_surface_spec_from_schema(
    schema: &JsonValue,
    evaluator: &dyn SchemaAnnotationEvaluator,
) -> Result<DynamicEntitySurfaceSpec, LixError> {
    let schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "schema is missing string x-lix-key".to_string(),
        })?;

    let mut visible_columns = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            let mut columns = properties
                .keys()
                .filter(|key| !key.starts_with("lixcol_"))
                .cloned()
                .collect::<Vec<_>>();
            columns.sort();
            columns
        })
        .unwrap_or_default();
    visible_columns.dedup();

    let column_types = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            properties
                .iter()
                .filter(|(key, _)| !key.starts_with("lixcol_"))
                .filter_map(|(key, property_schema)| {
                    surface_column_type_from_schema(property_schema).map(|kind| (key.clone(), kind))
                })
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    let predicate_overrides = collect_dynamic_override_predicates(schema, schema_key, evaluator)?;

    Ok(DynamicEntitySurfaceSpec {
        schema_key: schema_key.to_string(),
        visible_columns,
        column_types,
        predicate_overrides,
    })
}

pub(crate) fn register_dynamic_entity_surface_spec(
    registry: &mut SurfaceRegistry,
    spec: DynamicEntitySurfaceSpec,
) -> CatalogEpoch {
    let changed =
        registry.insert_descriptors(entity_surface_descriptors(&spec, CatalogSource::Dynamic));
    if changed {
        registry.advance_catalog_epoch();
    }
    registry.catalog_epoch()
}

pub(crate) fn remove_dynamic_entity_surfaces_for_schema_key(
    registry: &mut SurfaceRegistry,
    schema_key: &str,
) -> bool {
    let removed = registry.remove_descriptors_matching(|descriptor| {
        descriptor.catalog_source == CatalogSource::Dynamic
            && descriptor.implicit_overrides.fixed_schema_key.as_deref() == Some(schema_key)
    });
    if removed {
        registry.advance_catalog_epoch();
    }
    removed
}

pub(crate) fn builtin_public_surface_names() -> Vec<String> {
    builtin_surface_registry().public_surface_names()
}

pub(crate) fn builtin_public_surface_columns(relation_name: &str) -> Option<Vec<String>> {
    builtin_surface_registry().public_surface_columns(relation_name)
}

fn builtin_surface_registry() -> &'static SurfaceRegistry {
    static BUILTIN_SURFACE_REGISTRY: OnceLock<SurfaceRegistry> = OnceLock::new();
    BUILTIN_SURFACE_REGISTRY.get_or_init(build_builtin_surface_registry)
}

fn register_builtin_entity_surfaces(registry: &mut SurfaceRegistry) {
    for schema_key in builtin_schema_keys() {
        if !builtin_schema_exposed_as_entity_surface(schema_key) {
            continue;
        }
        let Some(schema) = builtin_schema_definition(schema_key) else {
            continue;
        };
        let Ok(spec) = builtin_entity_surface_spec_from_schema(schema) else {
            continue;
        };
        registry.insert_descriptors(entity_surface_descriptors(&spec, CatalogSource::Builtin));
    }
}

fn builtin_schema_exposed_as_entity_surface(schema_key: &str) -> bool {
    !matches!(schema_key, "lix_active_version" | "lix_active_account")
}

fn builtin_entity_surface_spec_from_schema(
    schema: &JsonValue,
) -> Result<DynamicEntitySurfaceSpec, LixError> {
    let schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "schema is missing string x-lix-key".to_string(),
        })?;

    let mut visible_columns = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            let mut columns = properties
                .keys()
                .filter(|key| !key.starts_with("lixcol_"))
                .cloned()
                .collect::<Vec<_>>();
            columns.sort();
            columns
        })
        .unwrap_or_default();
    visible_columns.dedup();

    let column_types = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            properties
                .iter()
                .filter(|(key, _)| !key.starts_with("lixcol_"))
                .filter_map(|(key, property_schema)| {
                    surface_column_type_from_schema(property_schema).map(|kind| (key.clone(), kind))
                })
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    Ok(DynamicEntitySurfaceSpec {
        schema_key: schema_key.to_string(),
        visible_columns,
        column_types,
        predicate_overrides: collect_builtin_override_predicates(schema, schema_key)?,
    })
}

fn collect_builtin_override_predicates(
    schema: &JsonValue,
    schema_key: &str,
) -> Result<Vec<SurfaceOverridePredicate>, LixError> {
    let Some(overrides) = schema
        .get("x-lix-override-lixcols")
        .and_then(JsonValue::as_object)
    else {
        return Ok(Vec::new());
    };

    if overrides.contains_key("lixcol_version_id") {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "schema '{}' uses removed x-lix-override-lixcols.lixcol_version_id support; use lixcol_global for global write scope",
                schema_key
            ),
        });
    }

    let mut predicates = Vec::new();
    for key in [
        "lixcol_entity_id",
        "lixcol_file_id",
        "lixcol_plugin_key",
        "lixcol_global",
        "lixcol_metadata",
        "lixcol_untracked",
    ] {
        let Some(raw_value) = overrides.get(key).and_then(JsonValue::as_str) else {
            continue;
        };
        let Some(column) = (match key {
            "lixcol_entity_id" => Some("entity_id"),
            "lixcol_file_id" => Some("file_id"),
            "lixcol_plugin_key" => Some("plugin_key"),
            "lixcol_global" => Some("global"),
            "lixcol_metadata" => Some("metadata"),
            "lixcol_untracked" => Some("untracked"),
            _ => None,
        }) else {
            continue;
        };
        predicates.push(SurfaceOverridePredicate {
            column: column.to_string(),
            value: parse_builtin_override_value(schema_key, key, raw_value)?,
        });
    }

    Ok(predicates)
}

fn parse_builtin_override_value(
    schema_key: &str,
    key: &str,
    raw_value: &str,
) -> Result<SurfaceOverrideValue, LixError> {
    let value = serde_json::from_str::<JsonValue>(raw_value).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "builtin schema '{}.{}' must use a scalar JSON literal override: {}",
            schema_key, key, error
        ),
    })?;

    match value {
        JsonValue::Null => Ok(SurfaceOverrideValue::Null),
        JsonValue::Bool(value) => Ok(SurfaceOverrideValue::Boolean(value)),
        JsonValue::Number(value) => Ok(SurfaceOverrideValue::Number(value.to_string())),
        JsonValue::String(_) => Ok(SurfaceOverrideValue::String(decode_lixcol_literal(
            raw_value,
        ))),
        JsonValue::Array(_) | JsonValue::Object(_) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "builtin schema '{}.{}' override must evaluate to a scalar or null",
                schema_key, key
            ),
        }),
    }
}

fn collect_dynamic_override_predicates(
    schema: &JsonValue,
    schema_key: &str,
    evaluator: &dyn SchemaAnnotationEvaluator,
) -> Result<Vec<SurfaceOverridePredicate>, LixError> {
    collect_dynamic_entity_surface_overrides(schema, schema_key, evaluator).map(|overrides| {
        overrides
            .into_iter()
            .map(dynamic_surface_override_to_predicate)
            .collect()
    })
}

fn dynamic_surface_override_to_predicate(
    override_entry: DynamicEntitySurfaceOverride,
) -> SurfaceOverridePredicate {
    let value = match override_entry.value {
        LixcolOverrideValue::Null => SurfaceOverrideValue::Null,
        LixcolOverrideValue::Boolean(value) => SurfaceOverrideValue::Boolean(value),
        LixcolOverrideValue::Number(value) => SurfaceOverrideValue::Number(value),
        LixcolOverrideValue::String(value) => SurfaceOverrideValue::String(value),
    };
    SurfaceOverridePredicate {
        column: override_entry.column,
        value,
    }
}

fn surface_column_type_from_schema(schema: &JsonValue) -> Option<SurfaceColumnType> {
    let types = match schema.get("type") {
        Some(JsonValue::String(kind)) => vec![kind.as_str()],
        Some(JsonValue::Array(kinds)) => kinds
            .iter()
            .filter_map(JsonValue::as_str)
            .collect::<Vec<_>>(),
        _ => return None,
    };

    if types.iter().any(|kind| *kind == "boolean") {
        return Some(SurfaceColumnType::Boolean);
    }
    if types.iter().any(|kind| *kind == "integer") {
        return Some(SurfaceColumnType::Integer);
    }
    if types.iter().any(|kind| *kind == "number") {
        return Some(SurfaceColumnType::Number);
    }
    if types.iter().any(|kind| *kind == "string") {
        return Some(SurfaceColumnType::String);
    }
    if types.iter().any(|kind| matches!(*kind, "object" | "array")) {
        return Some(SurfaceColumnType::Json);
    }
    None
}
