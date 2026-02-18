use crate::exports::lix::plugin::api::{EntityChange, File, Guest, PluginError};

wit_bindgen::generate!({
    path: "../engine/wit",
    world: "plugin",
});

mod apply_changes;
mod common;
mod detect_changes;

pub const SCHEMA_KEY: &str = "markdown_source";
pub const SCHEMA_VERSION: &str = "1";
pub const ROOT_ENTITY_ID: &str = "root";

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
