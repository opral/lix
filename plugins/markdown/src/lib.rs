#[expect(clippy::same_length_and_capacity)]
mod bindings {
    wit_bindgen::generate!({
        path: "../../packages/engine/wit",
        world: "plugin",
    });
}
pub use bindings::*;

use crate::exports::lix::plugin::api::{
    ActiveStateRow, DetectStateContext, EntityChange, File, Guest as Plugin, PluginError,
};

mod common;
mod detect_changes;
mod render_changes;
pub mod schemas;

pub const ROOT_ENTITY_PK: &str = "root";
pub const DOCUMENT_SCHEMA_KEY: &str = schemas::DOCUMENT_SCHEMA_KEY;
pub const BLOCK_SCHEMA_KEY: &str = schemas::BLOCK_SCHEMA_KEY;

pub use crate::exports::lix::plugin::api::{
    ActiveStateRow as PluginActiveStateRow, DetectStateContext as PluginDetectStateContext,
    EntityChange as PluginEntityChange, File as PluginFile, PluginError as PluginApiError,
};

struct MarkdownPlugin;

impl Plugin for MarkdownPlugin {
    fn detect_changes(
        state: DetectStateContext,
        file: File,
    ) -> Result<Vec<EntityChange>, PluginError> {
        detect_changes::detect_changes(None, file, Some(state))
    }

    fn render(state: DetectStateContext) -> Result<Vec<u8>, PluginError> {
        render_state_context(state)
    }
}

pub fn detect_changes(before: Option<File>, after: File) -> Result<Vec<EntityChange>, PluginError> {
    let state_context = project_state_context_from_before(before)?;
    <MarkdownPlugin as Plugin>::detect_changes(state_context, after)
}

pub fn detect_changes_with_state_context(
    before: Option<File>,
    after: File,
    state_context: Option<PluginDetectStateContext>,
) -> Result<Vec<EntityChange>, PluginError> {
    let state_context = match state_context {
        Some(state_context) => state_context,
        None => project_state_context_from_before(before)?,
    };
    <MarkdownPlugin as Plugin>::detect_changes(state_context, after)
}

pub fn render(state_context: PluginDetectStateContext) -> Result<Vec<u8>, PluginError> {
    <MarkdownPlugin as Plugin>::render(state_context)
}

pub fn render_changes(file: File, changes: Vec<EntityChange>) -> Result<Vec<u8>, PluginError> {
    render_changes::render_changes(file, changes)
}

fn empty_state_context() -> PluginDetectStateContext {
    PluginDetectStateContext {
        active_state: Vec::new(),
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
    let bootstrap = <MarkdownPlugin as Plugin>::detect_changes(empty_state_context(), before_file)?;

    Ok(PluginDetectStateContext {
        active_state: bootstrap
            .into_iter()
            .map(|row| PluginActiveStateRow {
                entity_pk: row.entity_pk,
                schema_key: row.schema_key,
                snapshot_content: row.snapshot_content,
                file_id: None,
                plugin_key: None,
                branch_id: None,
                change_id: None,
                metadata: None,
                created_at: None,
                updated_at: None,
            })
            .collect(),
    })
}

fn render_state_context(state: DetectStateContext) -> Result<Vec<u8>, PluginError> {
    render_changes::render_changes(
        empty_file(),
        entity_changes_from_active_state(state.active_state),
    )
}

fn entity_changes_from_active_state(rows: Vec<ActiveStateRow>) -> Vec<EntityChange> {
    rows.into_iter()
        .map(|row| EntityChange {
            entity_pk: row.entity_pk,
            schema_key: row.schema_key,
            snapshot_content: row.snapshot_content,
        })
        .collect()
}

fn empty_file() -> File {
    File {
        id: String::new(),
        path: String::new(),
        data: Vec::new(),
    }
}

#[cfg(target_family = "wasm")]
export!(MarkdownPlugin);
