pub(crate) mod builtin;
pub(crate) mod defaults;
mod definition;
mod key;
pub(crate) mod live_layout;
mod overlay;
mod provider;
pub(crate) mod registry;

pub use definition::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};
pub use key::{schema_from_registered_snapshot, schema_key_from_definition, SchemaKey};
pub use overlay::OverlaySchemaProvider;
pub use provider::{SchemaProvider, SqlRegisteredSchemaProvider};
