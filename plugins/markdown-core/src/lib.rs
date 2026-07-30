//! ABI-neutral incremental Markdown semantic core.

#[path = "../../markdown-v2/src/core.rs"]
mod core;
#[path = "../../markdown-v2/src/markdown_file.rs"]
mod markdown_file;
#[path = "../../markdown-v2/src/model.rs"]
mod model;
#[path = "../../markdown-v2/src/schemas.rs"]
pub mod schemas;

pub use core::{
    ByteEdit, ChangeEffect, DetectedChange, Document, EntityChange, EntityRecord, EntityState,
    File, IdNamespace, InputSplice, MarkdownPlugin, NODE_SCHEMA_KEY, PluginError,
    V3TopLevelIndexRecord, v3_reidentify_snapshot, v3_single_top_level_snapshot,
};
