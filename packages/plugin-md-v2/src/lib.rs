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
    ActiveStateRow as PluginActiveStateRow, DetectStateContext as PluginDetectStateContext,
    EntityChange as PluginEntityChange, File as PluginFile, PluginError as PluginApiError,
};

struct MarkdownPlugin;

impl Guest for MarkdownPlugin {
    fn detect_changes(
        before: Option<File>,
        after: File,
        state_context: Option<crate::exports::lix::plugin::api::DetectStateContext>,
    ) -> Result<Vec<EntityChange>, PluginError> {
        detect_changes::detect_changes(before, after, state_context)
    }

    fn apply_changes(file: File, changes: Vec<EntityChange>) -> Result<Vec<u8>, PluginError> {
        apply_changes::apply_changes(file, changes)
    }
}

pub fn detect_changes(before: Option<File>, after: File) -> Result<Vec<EntityChange>, PluginError> {
    <MarkdownPlugin as Guest>::detect_changes(before, after, None)
}

pub fn detect_changes_with_state_context(
    before: Option<File>,
    after: File,
    state_context: Option<PluginDetectStateContext>,
) -> Result<Vec<EntityChange>, PluginError> {
    <MarkdownPlugin as Guest>::detect_changes(before, after, state_context)
}

pub fn apply_changes(file: File, changes: Vec<EntityChange>) -> Result<Vec<u8>, PluginError> {
    <MarkdownPlugin as Guest>::apply_changes(file, changes)
}

export!(MarkdownPlugin);
