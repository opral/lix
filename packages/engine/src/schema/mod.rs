pub(crate) mod annotations;
pub(crate) mod builtin;
mod definition;
mod init;
mod key;
mod overlay;
mod provider;
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
