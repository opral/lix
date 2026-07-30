//! ABI-neutral Excalidraw semantic core.

#[path = "../../excalidraw-v2/src/core.rs"]
mod core;

pub use core::{
    ByteEdit, ChangeEffect, Document, ELEMENT_SCHEMA_KEY, EntityChange, EntityImportBuilder,
    EntityRecord, FILE_SCHEMA_KEY, IdNamespace, InitialChanges, InputSplice, SCENE_SCHEMA_KEY,
    V3ObjectIndexRecord, v3_reparse_object_snapshot,
};
