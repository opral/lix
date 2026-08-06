mod builtin;
pub(crate) mod compatibility;
mod definition;
mod key;
pub(crate) mod seed;
#[cfg(test)]
mod tests;

pub(crate) use compatibility::validate_schema_amendment;
pub(crate) use definition::{compile_lix_schema, format_lix_schema_validation_errors};
pub use definition::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};
pub(crate) use key::registered_schema_entity_pk;
pub use key::{SchemaKey, schema_from_registered_snapshot, schema_key_from_definition};
pub(crate) use seed::{is_seed_schema_key, seed_schema_definition, seed_schema_definitions};
