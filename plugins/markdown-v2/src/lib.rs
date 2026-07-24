//! GitHub Flavored Markdown guest for the Lix Wasm Component plugin API v2.

mod bindings;
mod core;
mod markdown_file;
mod model;
mod packet;
pub mod schemas;

pub use core::{
    ByteEdit, ChangeEffect, DetectedChange, Document, EntityChange, EntityRecord, EntityState,
    File, IdNamespace, InputSplice, MarkdownPlugin, NODE_SCHEMA_KEY, PluginError,
};

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");
pub const SCHEMAS: [(&str, &str); 1] = [(
    "schema/markdown_node_v2.json",
    include_str!("../schema/markdown_node_v2.json"),
)];

#[cfg(test)]
mod tests;
