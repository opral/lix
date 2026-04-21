mod api;
mod binding;
mod declaration;
mod dependency;
mod directory;
mod file;
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
mod write_surface;

use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
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
    state_by_version_relation_name, state_surface_effective_foreign_key_target_schema_key,
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
pub(crate) use file::LixFileProjection;
#[allow(unused_imports)]
pub(crate) use file::{
    builtin_lix_file_by_version_catalog_registration, builtin_lix_file_catalog_registration,
    LixFileByVersionProjection,
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
        visible_columns,
        column_types,
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
        visible_columns,
        column_types,
    })
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
