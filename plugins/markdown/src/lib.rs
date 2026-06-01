#[expect(clippy::same_length_and_capacity)]
mod bindings {
    wit_bindgen::generate!({
        path: "../../packages/engine/wit",
        world: "plugin",
    });
}
pub use bindings::*;

pub use crate::exports::lix::plugin::api::{DetectedChange, File, PluginError};
use crate::exports::lix::plugin::api::{EntityState, Guest as Plugin};

mod common;
mod detect_changes;
mod render_changes;
pub mod schemas;

pub const ROOT_ENTITY_PK: &str = "root";
pub const DOCUMENT_SCHEMA_KEY: &str = schemas::DOCUMENT_SCHEMA_KEY;
pub const BLOCK_SCHEMA_KEY: &str = schemas::BLOCK_SCHEMA_KEY;

struct MarkdownPlugin;

impl Plugin for MarkdownPlugin {
    fn detect_changes(
        state: Vec<EntityState>,
        file: File,
    ) -> Result<Vec<DetectedChange>, PluginError> {
        let state = detected_changes_from_state(state)?;
        detect_changes::detect_changes(None, file, Some(state))
    }

    fn render(state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
        let state = detected_changes_from_state(state)?;
        render_changes::render_changes(empty_file(), state)
    }
}

pub fn detect_changes(
    before: Option<File>,
    after: File,
) -> Result<Vec<DetectedChange>, PluginError> {
    let state_context = project_state_context_from_before(before)?;
    detect_changes::detect_changes(None, after, Some(state_context))
}

pub fn detect_changes_with_state_context(
    before: Option<File>,
    after: File,
    state_context: Option<Vec<DetectedChange>>,
) -> Result<Vec<DetectedChange>, PluginError> {
    let state_context = match state_context {
        Some(state_context) => state_context,
        None => project_state_context_from_before(before)?,
    };
    detect_changes::detect_changes(None, after, Some(state_context))
}

pub fn render(state_context: Vec<DetectedChange>) -> Result<Vec<u8>, PluginError> {
    render_changes::render_changes(empty_file(), state_context)
}

pub fn render_changes(file: File, changes: Vec<DetectedChange>) -> Result<Vec<u8>, PluginError> {
    render_changes::render_changes(file, changes)
}

fn project_state_context_from_before(
    before: Option<File>,
) -> Result<Vec<DetectedChange>, PluginError> {
    let Some(before_file) = before else {
        return Ok(Vec::new());
    };

    // Compatibility helper for tests/callers using detect_changes(before, after):
    // bootstrap a projected active-state from `before`.
    detect_changes::detect_changes(None, before_file, Some(Vec::new()))
}

fn detected_changes_from_state(
    state: Vec<EntityState>,
) -> Result<Vec<DetectedChange>, PluginError> {
    state
        .into_iter()
        .map(|row| {
            validate_single_entity_pk(&row.entity_pk)?;
            Ok(DetectedChange {
                entity_pk: row.entity_pk,
                schema_key: row.schema_key,
                snapshot_content: Some(row.snapshot_content),
                metadata: row.metadata,
            })
        })
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn single_entity_pk(mut entity_pk: Vec<String>) -> Result<String, PluginError> {
    validate_single_entity_pk(&entity_pk)?;
    Ok(entity_pk.remove(0))
}

fn validate_single_entity_pk(entity_pk: &[String]) -> Result<(), PluginError> {
    if entity_pk.len() != 1 {
        return Err(PluginError::InvalidInput(format!(
            "expected single-component entity_pk, got {} components",
            entity_pk.len()
        )));
    }
    Ok(())
}

fn empty_file() -> File {
    File { data: Vec::new() }
}

#[cfg(target_family = "wasm")]
export!(MarkdownPlugin);
