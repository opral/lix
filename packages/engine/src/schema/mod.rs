mod annotations;
mod builtin;
mod definition;
mod key;
#[cfg(test)]
mod tests;

pub(crate) use annotations::defaults::apply_schema_defaults_with_shared_runtime;
pub(crate) use annotations::overrides::{
    collect_dynamic_entity_surface_overrides, collect_state_column_overrides_with_shared_runtime,
    DynamicEntitySurfaceOverride, LixcolOverrideValue, SchemaAnnotationEvaluator,
};
pub(crate) use builtin::{
    builtin_schema_definition, builtin_schema_json, builtin_schema_keys, decode_lixcol_literal,
    lix_state_surface_schema_definition, LixActiveVersion, LixCommit, LixVersionDescriptor,
    LixVersionRef,
};
pub use definition::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};
pub use key::{schema_from_registered_snapshot, schema_key_from_definition, SchemaKey};
