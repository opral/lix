//! PostgreSQL-derived relational schemas for Lix.
//!
//! Schema v1 is intentionally a strict subset of PostgreSQL table semantics.
//! Parsing rejects unknown fields. Compilation performs all definition checks
//! once so row validation can use a small typed plan.

mod amendment;
mod ddl;
mod error;
mod model;
mod row;
mod validate;

pub use amendment::validate_amendment;
pub use ddl::to_postgres_ddl;
pub use error::{Error, ErrorKind};
pub use model::{Column, DataType, ForeignKey, ForeignKeyReference, Schema};
pub use row::{CompiledSchema, SqlRow, SqlValue};

/// Canonical identifier for the first Lix relational schema language.
pub const SCHEMA_V1_URI: &str = "https://lix.dev/schema-v1.json";

/// JSON meta-schema used by editors, agents, and non-Rust tooling.
pub const SCHEMA_V1_JSON: &str = include_str!("../schema/schema-v1.json");

/// Parse and semantically validate a Schema v1 document.
pub fn from_json(input: &str) -> Result<Schema, Error> {
    let schema: Schema = serde_json::from_str(input)
        .map_err(|error| Error::new(ErrorKind::Parse, "$", error.to_string()))?;
    schema.validate()?;
    Ok(schema)
}

/// Parse and semantically validate an already-decoded Schema v1 document.
pub fn from_value(value: serde_json::Value) -> Result<Schema, Error> {
    let schema: Schema = serde_json::from_value(value)
        .map_err(|error| Error::new(ErrorKind::Parse, "$", error.to_string()))?;
    schema.validate()?;
    Ok(schema)
}

impl Schema {
    /// Serialize using the canonical field and declaration order.
    pub fn to_canonical_json(&self) -> Result<String, Error> {
        self.validate()?;
        serde_json::to_string(self)
            .map_err(|error| Error::new(ErrorKind::Serialization, "$", error.to_string()))
    }

    /// Stable semantic fingerprint of the canonical Schema v1 document.
    pub fn fingerprint(&self) -> Result<blake3::Hash, Error> {
        Ok(blake3::hash(self.to_canonical_json()?.as_bytes()))
    }
}
