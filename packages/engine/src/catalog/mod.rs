mod api;
mod binding;
mod change_surface;
mod declaration;
mod dependency;
mod directory;
mod directory_surface;
mod file;
mod file_surface;
mod filesystem_query;
mod history_read;
mod public_surface_registry;
mod read_surface;
mod read_time_projection;
mod registry;
mod scan;
mod state;
mod transaction_write;
mod version;
mod version_surface;
mod working_changes_surface;
mod write_surface;

use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use crate::schema::{builtin_schema_definition, builtin_schema_keys};
use crate::LixError;

#[allow(unused_imports)]
pub(crate) use api::{
    builtin_catalog_compiler_facade, catalog_compiler_facade_for_registry, CatalogCompilerApi,
    CatalogCompilerFacade,
};

#[allow(unused_imports)]
pub(crate) use registry::*;
#[allow(unused_imports)]
pub(crate) use state::{
    state_by_version_relation_name, state_relation_column_is_nullable_for_variant,
    state_relation_columns_for_variant, state_surface_effective_foreign_key_target_schema_key,
    state_surface_validation_schema,
};

#[allow(unused_imports)]
pub(crate) use binding::{
    bind_filesystem_relation, bind_named_relation, bind_registry_relation, bind_schema_relation,
    bind_surface_relation, bind_version_relation, FilesystemProjectionScope,
    FilesystemRelationBinding, FilesystemRelationKind, RelationBindContext, RelationBinding,
    SchemaRelationBinding, StoredVersionHeadSourceBinding, VersionDescriptorSourceBinding,
    VersionHeadSourceBinding, VersionRelationBinding,
};
#[allow(unused_imports)]
pub(crate) use change_surface::{
    open_change_surface_snapshot, open_change_surface_snapshot_with_shared_backend,
    ChangeSurfaceColumn, ChangeSurfaceFilter, ChangeSurfaceRow, ChangeSurfaceScanRequest,
    ChangeSurfaceSnapshot,
};
#[allow(unused_imports)]
pub(crate) use declaration::{
    builtin_catalog_projection_registry, CatalogDerivedRow, CatalogProjectionContext,
    CatalogProjectionDefinition, CatalogProjectionInput, CatalogProjectionInputRows,
    CatalogProjectionInputSpec, CatalogProjectionInputVersionScope, CatalogProjectionLifecycle,
    CatalogProjectionRegistration, CatalogProjectionRegistry, CatalogProjectionSourceRow,
    CatalogProjectionStorageKind, CatalogProjectionSurfaceSpec, RegisteredCatalogProjection,
};
#[allow(unused_imports)]
pub(crate) use dependency::{
    dependency_metadata_for_surface_binding, dependency_metadata_for_surface_name,
    CatalogSurfaceDependencyMetadata,
};
#[allow(unused_imports)]
pub(crate) use directory::LixDirectoryProjection;
#[allow(unused_imports)]
pub(crate) use directory::{
    builtin_lix_directory_by_version_catalog_registration,
    builtin_lix_directory_catalog_registration, LixDirectoryByVersionProjection,
};
#[allow(unused_imports)]
pub(crate) use directory_surface::{
    open_directory_by_version_surface_snapshot,
    open_directory_by_version_surface_snapshot_with_shared_backend,
    open_directory_surface_snapshot, DirectorySurfaceColumn, DirectorySurfaceFilter,
    DirectorySurfaceRow, DirectorySurfaceScanRequest, DirectorySurfaceSnapshot,
};
#[allow(unused_imports)]
pub(crate) use file::LixFileProjection;
#[allow(unused_imports)]
pub(crate) use file::{
    builtin_lix_file_by_version_catalog_registration, builtin_lix_file_catalog_registration,
    LixFileByVersionProjection,
};
#[allow(unused_imports)]
pub(crate) use file_surface::{
    open_file_by_version_surface_snapshot,
    open_file_by_version_surface_snapshot_with_shared_backend, open_file_surface_snapshot,
    FileSurfaceColumn, FileSurfaceFilter, FileSurfaceRow, FileSurfaceScanRequest,
    FileSurfaceSnapshot,
};
#[allow(unused_imports)]
pub(crate) use filesystem_query::*;
#[allow(unused_imports)]
pub(crate) use history_read::{history_read_semantics, CatalogHistoryReadSemantics};
#[allow(unused_imports)]
pub(crate) use public_surface_registry::{
    apply_registered_schema_snapshot_to_surface_registry, load_public_surface_registry_with_backend,
};
#[allow(unused_imports)]
pub(crate) use read_surface::{
    explicit_version_counterpart_surface_name, read_preparation_semantics,
    CatalogReadPreparationSemantics,
};
#[allow(unused_imports)]
pub(crate) use read_time_projection::CatalogReadTimeProjectionRequest;
#[allow(unused_imports)]
pub(crate) use scan::{
    admin_scan_kind, filesystem_scan_semantics, is_working_changes_surface, CatalogAdminScanKind,
    CatalogFilesystemScanSemantics, CatalogScanVersionScope,
};
#[allow(unused_imports)]
pub(crate) use transaction_write::{
    transaction_insert_semantics, CatalogTransactionInsertSemantics,
};
#[allow(unused_imports)]
pub(crate) use version::{builtin_lix_version_catalog_registration, LixVersionProjection};
#[allow(unused_imports)]
pub(crate) use version_surface::{
    load_version_surface_row_with_backend, open_version_surface_snapshot,
    open_version_surface_snapshot_with_shared_backend, VersionSurfaceColumn, VersionSurfaceRow,
    VersionSurfaceScanRequest, VersionSurfaceSnapshot,
};
#[allow(unused_imports)]
pub(crate) use working_changes_surface::{
    open_working_changes_surface_snapshot,
    open_working_changes_surface_snapshot_with_shared_backend, WorkingChangesSurfaceColumn,
    WorkingChangesSurfaceFilter, WorkingChangesSurfaceRow, WorkingChangesSurfaceScanRequest,
    WorkingChangesSurfaceSnapshot,
};
#[allow(unused_imports)]
pub(crate) use write_surface::{
    write_surface_semantics, CatalogAdminWriteBehavior, CatalogWriteSurfaceSemantics,
    CatalogWriteTargetKind, CatalogWriteVersionSemantics,
};

pub(crate) fn build_builtin_surface_registry() -> SurfaceRegistry {
    let mut registry = SurfaceRegistry::new();
    registry.insert_descriptors(builtin_surface_descriptors());
    register_builtin_entity_surfaces(&mut registry);
    registry
}

pub(crate) fn dynamic_entity_surface_spec_from_schema(
    schema: &JsonValue,
) -> Result<DynamicEntitySurfaceSpec, LixError> {
    let schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "schema is missing string x-lix-key".to_string(),
            hint: None,
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
        schema: schema.clone(),
        visible_columns,
        column_types,
    })
}

pub(crate) fn register_dynamic_entity_surface_spec(
    registry: &mut SurfaceRegistry,
    spec: DynamicEntitySurfaceSpec,
) -> CatalogEpoch {
    registry.upsert_dynamic_schema(spec.schema_key.clone(), spec.schema.clone());
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
    registry.remove_dynamic_schema(schema_key);
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
            hint: None,
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
        schema: schema.clone(),
        visible_columns,
        column_types,
    })
}

fn surface_column_type_from_schema(schema: &JsonValue) -> Option<SurfaceColumnType> {
    let mut kinds = BTreeSet::new();
    collect_surface_type_kinds(schema, &mut kinds);
    kinds.remove("null");

    if kinds.is_empty() {
        return None;
    }

    if kinds.len() == 1 {
        return match kinds.into_iter().next() {
            Some("boolean") => Some(SurfaceColumnType::Boolean),
            Some("integer") => Some(SurfaceColumnType::Integer),
            Some("number") => Some(SurfaceColumnType::Number),
            Some("string") => Some(SurfaceColumnType::String),
            Some("object" | "array") => Some(SurfaceColumnType::Json),
            _ => None,
        };
    }

    // Design note:
    // Mixed JSON kinds inside a JSON Schema still describe a JSON value domain.
    // For example, `anyOf(string, object)` is heterogeneous JSON, not an
    // engine-native polymorphic payload. `Variant` is reserved for explicit
    // owner-chosen engine types and must not be inferred solely from JSON Schema
    // composition.
    Some(SurfaceColumnType::Json)
}

fn collect_surface_type_kinds<'a>(schema: &'a JsonValue, out: &mut BTreeSet<&'a str>) {
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
                collect_surface_type_kinds(branch, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_builtin_surface_registry, builtin_schema_exposed_as_entity_surface,
        dynamic_entity_surface_spec_from_schema, surface_column_type_from_schema,
    };
    use crate::catalog::SurfaceColumnType;
    use crate::schema::builtin_schema_keys;
    use serde_json::json;

    #[test]
    fn single_type_schema_properties_remain_scalar_surface_types() {
        let spec = dynamic_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "phase8_scalar_schema",
            "type": "object",
            "properties": {
                "title": { "type": "string" },
                "count": { "type": "integer" },
                "score": { "type": "number" },
                "published": { "type": "boolean" }
            }
        }))
        .expect("schema should compile");

        assert_eq!(spec.column_types.get("title"), Some(&SurfaceColumnType::String));
        assert_eq!(spec.column_types.get("count"), Some(&SurfaceColumnType::Integer));
        assert_eq!(spec.column_types.get("score"), Some(&SurfaceColumnType::Number));
        assert_eq!(
            spec.column_types.get("published"),
            Some(&SurfaceColumnType::Boolean)
        );
    }

    #[test]
    fn multi_type_schema_properties_remain_json_surface_types() {
        let kind = surface_column_type_from_schema(&json!({
            "anyOf": [
                { "type": "string" },
                { "type": "number" },
                { "type": "object" },
                { "type": "array" },
                { "type": "boolean" },
                { "type": "null" }
            ]
        }));

        assert_eq!(kind, Some(SurfaceColumnType::Json));
    }

    #[test]
    fn object_type_schema_properties_map_to_json_surface_types() {
        let spec = dynamic_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "phase8_object_schema",
            "type": "object",
            "properties": {
                "payload": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" }
                    }
                }
            }
        }))
        .expect("schema should compile");

        assert_eq!(spec.column_types.get("payload"), Some(&SurfaceColumnType::Json));
    }

    #[test]
    fn anyof_string_object_schema_properties_map_to_json_surface_types() {
        let spec = dynamic_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "phase8_anyof_schema",
            "type": "object",
            "properties": {
                "payload": {
                    "anyOf": [
                        { "type": "string" },
                        { "type": "object" }
                    ]
                }
            }
        }))
        .expect("schema should compile");

        assert_eq!(spec.column_types.get("payload"), Some(&SurfaceColumnType::Json));
    }

    #[test]
    fn builtin_lix_key_value_value_column_is_json() {
        let registry = build_builtin_surface_registry();
        let resolved = registry
            .bind_relation_name("lix_key_value")
            .expect("builtin lix_key_value should be registered");

        assert_eq!(
            resolved.column_types.get("value"),
            Some(&SurfaceColumnType::Json)
        );
        assert_eq!(
            resolved.column_types.get("key"),
            Some(&SurfaceColumnType::String)
        );
    }

    #[test]
    fn builtin_schema_derived_entity_surfaces_do_not_infer_variant_columns() {
        let registry = build_builtin_surface_registry();

        for schema_key in builtin_schema_keys() {
            if !builtin_schema_exposed_as_entity_surface(schema_key) {
                continue;
            }

            let resolved = registry
                .bind_relation_name(schema_key)
                .unwrap_or_else(|| panic!("builtin entity surface '{schema_key}' should bind"));

            let variant_columns = resolved
                .column_types
                .iter()
                .filter_map(|(column_name, column_type)| {
                    (*column_type == SurfaceColumnType::Variant).then_some(column_name.as_str())
                })
                .collect::<Vec<_>>();

            assert!(
                variant_columns.is_empty(),
                "builtin schema-derived surface '{}' should not infer Variant columns: {:?}",
                schema_key,
                variant_columns
            );
        }
    }

    #[test]
    fn builtin_registry_currently_has_no_catalog_owned_variant_columns() {
        let registry = build_builtin_surface_registry();

        let relation_names = [
            "lix_state",
            "lix_state_by_version",
            "lix_state_history",
            "lix_change",
            "lix_working_changes",
            "lix_file",
            "lix_file_by_version",
            "lix_file_history",
            "lix_file_history_by_version",
            "lix_directory",
            "lix_directory_by_version",
            "lix_directory_history",
            "lix_version",
        ];

        for relation_name in relation_names {
            let resolved = registry.bind_relation_name(&relation_name).unwrap_or_else(|| {
                panic!("builtin registry relation '{relation_name}' should bind")
            });

            let variant_columns = resolved
                .column_types
                .iter()
                .filter_map(|(column_name, column_type)| {
                    (*column_type == SurfaceColumnType::Variant).then_some(column_name.as_str())
                })
                .collect::<Vec<_>>();

            assert!(
                variant_columns.is_empty(),
                "catalog-owned relation '{}' currently exposes Variant columns: {:?}",
                relation_name,
                variant_columns
            );
        }
    }
}
