#[expect(clippy::same_length_and_capacity)]
mod bindings {
    wit_bindgen::generate!({
        path: "../../packages/engine/wit",
        world: "plugin",
    });
}
pub use bindings::*;

use crate::exports::lix::plugin::api::{
    DetectStateContext, EntityChange, File, Guest as Plugin, PluginError,
};

pub mod schemas;

pub const ROOT_ENTITY_PK: &str = "root";
pub const DOCUMENT_SCHEMA_KEY: &str = schemas::DOCUMENT_SCHEMA_KEY;
pub const ROW_SCHEMA_KEY: &str = schemas::ROW_SCHEMA_KEY;

const MANIFEST_JSON: &str = include_str!("../manifest.json");

pub use crate::exports::lix::plugin::api::{
    ActiveStateRow as PluginActiveStateRow, DetectStateContext as PluginDetectStateContext,
    EntityChange as PluginEntityChange, File as PluginFile, PluginError as PluginApiError,
};

struct CsvPlugin;

impl Plugin for CsvPlugin {
    fn detect_changes(
        _state: DetectStateContext,
        _file: File,
    ) -> Result<Vec<EntityChange>, PluginError> {
        todo!()
    }

    fn render(_state: DetectStateContext) -> Result<Vec<u8>, PluginError> {
        todo!()
    }
}

pub fn detect_changes(
    _before: Option<File>,
    after: File,
) -> Result<Vec<EntityChange>, PluginError> {
    <CsvPlugin as Plugin>::detect_changes(empty_state_context(), after)
}

pub fn detect_changes_with_state_context(
    _before: Option<File>,
    after: File,
    state_context: Option<PluginDetectStateContext>,
) -> Result<Vec<EntityChange>, PluginError> {
    <CsvPlugin as Plugin>::detect_changes(state_context.unwrap_or_else(empty_state_context), after)
}

pub fn render(state_context: PluginDetectStateContext) -> Result<Vec<u8>, PluginError> {
    <CsvPlugin as Plugin>::render(state_context)
}

pub fn apply_changes(_file: File, _changes: Vec<EntityChange>) -> Result<Vec<u8>, PluginError> {
    <CsvPlugin as Plugin>::render(empty_state_context())
}

pub fn manifest_json() -> &'static str {
    MANIFEST_JSON
}

fn empty_state_context() -> PluginDetectStateContext {
    PluginDetectStateContext {
        active_state: Vec::new(),
    }
}

#[cfg(target_family = "wasm")]
export!(CsvPlugin);
