use crate::exports::lix::plugin::api::{EntityChange, File, Guest, PluginError};

wit_bindgen::generate!({
    path: "../engine/wit",
    world: "plugin",
});

mod apply_changes;
mod common;
mod detect_changes;
pub mod schemas;

pub const ROOT_ENTITY_ID: &str = "root";
pub const DOCUMENT_SCHEMA_KEY: &str = schemas::DOCUMENT_SCHEMA_KEY;
pub const BLOCK_SCHEMA_KEY: &str = schemas::BLOCK_SCHEMA_KEY;
pub const SCHEMA_VERSION: &str = schemas::ENTITY_SCHEMA_VERSION;

pub use crate::exports::lix::plugin::api::{
    EntityChange as PluginEntityChange, File as PluginFile, PluginError as PluginApiError,
};

struct MarkdownPlugin;

impl Guest for MarkdownPlugin {
    fn detect_changes(before: Option<File>, after: File) -> Result<Vec<EntityChange>, PluginError> {
        detect_changes::detect_changes(before, after)
    }

    fn apply_changes(file: File, changes: Vec<EntityChange>) -> Result<Vec<u8>, PluginError> {
        apply_changes::apply_changes(file, changes)
    }
}

pub fn detect_changes(before: Option<File>, after: File) -> Result<Vec<EntityChange>, PluginError> {
    <MarkdownPlugin as Guest>::detect_changes(before, after)
}

pub fn apply_changes(file: File, changes: Vec<EntityChange>) -> Result<Vec<u8>, PluginError> {
    <MarkdownPlugin as Guest>::apply_changes(file, changes)
}

export!(MarkdownPlugin);
