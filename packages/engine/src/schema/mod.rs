pub(crate) mod annotations;
pub(crate) mod builtin;
mod definition;
mod init;
mod key;
mod overlay;
mod provider;
pub(crate) mod public_surfaces;
pub(crate) mod relation_policy;
#[cfg(test)]
mod tests;

pub use definition::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};
pub(crate) use init::{init, seed_bootstrap};
pub use key::{schema_from_registered_snapshot, schema_key_from_definition, SchemaKey};
pub use overlay::OverlaySchemaProvider;
pub use provider::{SchemaProvider, SqlRegisteredSchemaProvider};
pub(crate) use public_surfaces::{
    apply_registered_schema_snapshot_to_surface_registry, build_builtin_surface_registry,
    builtin_public_surface_columns, builtin_public_surface_names,
    load_public_surface_registry_with_backend,
};
pub(crate) use relation_policy::{
    object_name_is_internal_storage_relation, object_name_is_protected_builtin_ddl_target,
};
