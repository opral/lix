use crate::exports::lix::plugin::api::{EntityChange, File, Guest, PluginError};

wit_bindgen::generate!({
    path: "../../../../wit",
    world: "plugin",
});

struct TestPlugin;

impl Guest for TestPlugin {
    fn detect_changes(
        before: Option<File>,
        after: File,
        state_context: Option<crate::exports::lix::plugin::api::DetectStateContext>,
    ) -> Result<Vec<EntityChange>, PluginError> {
        let after_path = after.path;
        let after_value = String::from_utf8(after.data)
            .map_err(|error| PluginError::InvalidInput(error.to_string()))?;
        let before_path = before.as_ref().map(|file| file.path.clone());
        let before_value = before
            .map(|file| String::from_utf8(file.data))
            .transpose()
            .map_err(|error| PluginError::InvalidInput(error.to_string()))?;
        let active_state = state_context
            .as_ref()
            .and_then(|context| context.active_state.as_ref())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let active_state_schema = active_state
            .first()
            .and_then(|row| row.schema_key.as_ref())
            .cloned();
        let active_state_entity_id = active_state.first().map(|row| row.entity_id.clone());
        let active_state_file_id = active_state
            .first()
            .and_then(|row| row.file_id.as_ref())
            .cloned();
        let active_state_plugin_key = active_state
            .first()
            .and_then(|row| row.plugin_key.as_ref())
            .cloned();
        let active_state_version_id = active_state
            .first()
            .and_then(|row| row.version_id.as_ref())
            .cloned();
        let active_state_snapshot_content = active_state
            .first()
            .and_then(|row| row.snapshot_content.as_ref())
            .cloned();
        let value = serde_json::json!({
            "active_state_entity_id": active_state_entity_id,
            "active_state_file_id": active_state_file_id,
            "active_state_len": active_state.len(),
            "active_state_plugin_key": active_state_plugin_key,
            "active_state_schema": active_state_schema,
            "active_state_snapshot_content": active_state_snapshot_content,
            "active_state_version_id": active_state_version_id,
            "after": after_value,
            "after_path": after_path,
            "before": before_value,
            "before_path": before_path,
        })
        .to_string();
        let snapshot_content = serde_json::json!({
            "id": "entity-1",
            "value": value,
        })
        .to_string();

        Ok(vec![EntityChange {
            entity_id: "entity-1".to_string(),
            schema_key: "test_json_entity".to_string(),
            snapshot_content: Some(snapshot_content),
        }])
    }

    fn apply_changes(file: File, _changes: Vec<EntityChange>) -> Result<Vec<u8>, PluginError> {
        Ok(file.data)
    }
}

export!(TestPlugin);
