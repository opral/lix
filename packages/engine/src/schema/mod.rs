mod annotations;
mod builtin;
mod definition;
mod key;
#[cfg(test)]
mod tests;

pub(crate) use annotations::defaults::apply_schema_defaults_with_shared_runtime;
pub(crate) use builtin::{
    builtin_schema_definition, builtin_schema_keys, lix_state_surface_schema_definition,
};
pub(crate) use definition::{compile_lix_schema, format_lix_schema_validation_errors};
pub use definition::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};
pub use key::{schema_from_registered_snapshot, schema_key_from_definition, SchemaKey};
