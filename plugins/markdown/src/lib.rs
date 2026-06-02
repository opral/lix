// TODO: workaround wit_bindgen
#![expect(clippy::same_length_and_capacity)]

wit_bindgen::generate!({
    path: "../../packages/engine/wit",
    world: "plugin",
});

pub use crate::exports::lix::plugin::api::{DetectedChange, File, PluginError};
use crate::exports::lix::plugin::api::{EntityState, Guest as Plugin};

mod common;
mod detect_changes;
mod render_changes;
pub mod schemas;

pub const ROOT_ENTITY_PK: &str = "root";
pub const DOCUMENT_SCHEMA_KEY: &str = schemas::DOCUMENT_SCHEMA_KEY;
pub const BLOCK_SCHEMA_KEY: &str = schemas::BLOCK_SCHEMA_KEY;
pub const MANIFEST_JSON: &str = include_str!("../manifest.json");

#[derive(Clone, Copy, Debug)]
pub struct MarkdownPlugin;
#[cfg(target_family = "wasm")]
export!(MarkdownPlugin);

impl Plugin for MarkdownPlugin {
    fn detect_changes(
        state: Vec<EntityState>,
        file: File,
    ) -> Result<Vec<DetectedChange>, PluginError> {
        detect_changes::detect_changes(None, file, Some(state))
    }

    fn render(state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
        render_changes::render_state(empty_file(), state)
    }
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
