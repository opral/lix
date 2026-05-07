mod builtin;
mod definition;
mod key;
pub(crate) mod seed;
#[cfg(test)]
mod tests;

pub(crate) use definition::{compile_lix_schema, format_lix_schema_validation_errors};
pub use definition::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};
pub(crate) use key::{registered_schema_entity_id, reject_unsupported_registered_schema_version};
pub use key::{schema_from_registered_snapshot, schema_key_from_definition, SchemaKey};
#[cfg(test)]
pub(crate) use seed::seed_schema_definition;
pub(crate) use seed::{is_seed_schema_key, seed_schema_definitions};
