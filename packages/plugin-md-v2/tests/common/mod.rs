#![allow(dead_code)]

use plugin_md_v2::{
    PluginApiError, PluginEntityChange, PluginFile, ROOT_ENTITY_ID, SCHEMA_KEY, SCHEMA_VERSION,
};
use serde_json::Value;

pub fn file_from_markdown(id: &str, path: &str, markdown: &str) -> PluginFile {
    PluginFile {
        id: id.to_string(),
        path: path.to_string(),
        data: markdown.as_bytes().to_vec(),
    }
}

pub fn parse_snapshot_markdown(change: &PluginEntityChange) -> String {
    let raw = change
        .snapshot_content
        .as_ref()
        .expect("snapshot_content should be present");
    let parsed: Value = serde_json::from_str(raw).expect("snapshot_content should be valid JSON");
    parsed
        .get("markdown")
        .and_then(Value::as_str)
        .expect("snapshot_content.markdown should be a string")
        .to_string()
}

pub fn root_change(markdown: &str) -> PluginEntityChange {
    PluginEntityChange {
        entity_id: ROOT_ENTITY_ID.to_string(),
        schema_key: SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(
            serde_json::json!({
                "markdown": markdown,
            })
            .to_string(),
        ),
    }
}

pub fn assert_invalid_input(error: PluginApiError) {
    match error {
        PluginApiError::InvalidInput(_) => {}
        PluginApiError::Internal(message) => {
            panic!("expected invalid-input error, got internal error: {message}")
        }
    }
}
