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
    let state_context = project_state_context_from_before(before)?;
    <MarkdownPlugin as Guest>::detect_changes(None, after, Some(state_context))
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

fn empty_state_context() -> PluginDetectStateContext {
    PluginDetectStateContext {
        active_state: Some(Vec::new()),
    }
}

fn project_state_context_from_before(
    before: Option<File>,
) -> Result<PluginDetectStateContext, PluginError> {
    let Some(before_file) = before else {
        return Ok(empty_state_context());
    };

    // Compatibility helper for tests/callers using detect_changes(before, after):
    // bootstrap a projected active-state from `before`.
    let bootstrap =
        <MarkdownPlugin as Guest>::detect_changes(None, before_file, Some(empty_state_context()))?;

    Ok(PluginDetectStateContext {
        active_state: Some(
            bootstrap
                .into_iter()
                .map(|row| PluginActiveStateRow {
                    entity_id: row.entity_id,
                    schema_key: Some(row.schema_key),
                    schema_version: Some(row.schema_version),
                    snapshot_content: row.snapshot_content,
                    file_id: None,
                    plugin_key: None,
                    version_id: None,
                    change_id: None,
                    metadata: None,
                    created_at: None,
                    updated_at: None,
                })
                .collect(),
        ),
    })
}

export!(MarkdownPlugin);
