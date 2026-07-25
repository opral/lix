//! Excalidraw guest for the Lix Wasm Component plugin API v2.

mod bindings;
mod core;
mod packet;

pub use core::{
    ByteEdit, ChangeEffect, Document, ELEMENT_SCHEMA_KEY, EntityChange, EntityImportBuilder,
    EntityRecord, FILE_SCHEMA_KEY, IdNamespace, InitialChanges, InputSplice, SCENE_SCHEMA_KEY,
};

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");
pub const SCHEMAS: [(&str, &str); 3] = [
    (
        "schema/excalidraw_scene.json",
        include_str!("../schema/excalidraw_scene.json"),
    ),
    (
        "schema/excalidraw_element.json",
        include_str!("../schema/excalidraw_element.json"),
    ),
    (
        "schema/excalidraw_file.json",
        include_str!("../schema/excalidraw_file.json"),
    ),
];

#[cfg(test)]
mod tests;
