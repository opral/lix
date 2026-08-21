//! PostgreSQL-derived relational schemas for Lix.
//!
//! Schema v1 is intentionally a strict subset of PostgreSQL table semantics.
//! Parsing rejects unknown fields. Compilation performs all definition checks
//! once so row validation can use a small typed plan.

mod amendment;
mod ddl;
mod error;
mod jsonb;
mod model;
mod row;
mod validate;
pub mod value_layout;

pub use amendment::validate_amendment;
pub use ddl::to_postgres_ddl;
pub use error::{Error, ErrorKind};
pub use jsonb::{
    Jsonb, JsonbError, binary_to_json_string, validate_binary, validate_canonical_json_text,
    validated_binary_to_json_string,
};
pub use model::{Column, DataType, ForeignKey, ForeignKeyReference, Schema};
pub use row::{CompiledSchema, Row, Value};

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

    /// Stable semantic fingerprint whose JSON object keys are sorted
    /// recursively while declaration arrays retain their authored order.
    ///
    /// This is the fingerprint used by the plugin wire format. Sorting object
    /// keys makes equivalent schema documents agree even when their textual
    /// object-field order differs.
    pub fn wire_fingerprint(&self) -> Result<blake3::Hash, Error> {
        let value = serde_json::to_value(self)
            .map_err(|error| Error::new(ErrorKind::Serialization, "$", error.to_string()))?;
        let canonical = canonical_json_value(value);
        let bytes = serde_json::to_vec(&canonical)
            .map_err(|error| Error::new(ErrorKind::Serialization, "$", error.to_string()))?;
        Ok(blake3::hash(&bytes))
    }
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut ordered = serde_json::Map::new();
            let object = object
                .into_iter()
                .collect::<std::collections::BTreeMap<_, _>>();
            for (key, value) in object {
                ordered.insert(key, canonical_json_value(value));
            }
            serde_json::Value::Object(ordered)
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        value => value,
    }
}
