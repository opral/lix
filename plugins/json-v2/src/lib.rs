//! Recursive stable-identity JSON guest for the Lix Wasm Component plugin API v2.

mod bindings;
mod core;
mod packet;

pub use core::{
    ARRAY_ITEM_SCHEMA_KEY, ByteEdit, ChangeEffect, Document, EntityChange, EntityRecord,
    IdNamespace, InitialChanges, InputSplice, OBJECT_MEMBER_SCHEMA_KEY, ROOT_SCHEMA_KEY,
};

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");
pub const SCHEMAS: [(&str, &str); 3] = [
    (
        "schema/json_root.json",
        include_str!("../schema/json_root.json"),
    ),
    (
        "schema/json_object_member.json",
        include_str!("../schema/json_object_member.json"),
    ),
    (
        "schema/json_array_item.json",
        include_str!("../schema/json_array_item.json"),
    ),
];

#[cfg(test)]
mod tests;
