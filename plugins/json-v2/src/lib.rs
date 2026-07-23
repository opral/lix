//! Incremental top-level-object JSON guest for the Lix Wasm Component plugin API v2.

mod bindings;
mod core;
mod packet;

pub use core::{
    ByteEdit, ChangeEffect, Document, EntityChange, EntityRecord, IdNamespace, InitialChanges,
    InputSplice, JsonPropertySnapshot, PROPERTY_SCHEMA_KEY,
};

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");
pub const SCHEMA_PATH: &str = "schema/json_property.json";
pub const SCHEMA_JSON: &str = include_str!("../schema/json_property.json");

#[cfg(test)]
mod tests;
