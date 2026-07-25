//! Incremental CSV guest for the Lix Wasm Component plugin API v2.

mod bindings;
mod core;
mod packet;

pub use core::{
    ByteEdit, ChangeEffect, Dialect, Document, EntityChange, EntityRecord, IdNamespace,
    InitialChanges, InputSplice, ROOT_ENTITY_PK, ROW_SCHEMA_KEY, RowSnapshot, TABLE_SCHEMA_KEY,
    Terminator, describe_memory, parse_row_snapshot, render_row,
};

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");

#[cfg(test)]
mod tests;
