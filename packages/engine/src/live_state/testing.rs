use crate::live_state::LiveRow;
use crate::{LixError, Value};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub(crate) use crate::live_state::lifecycle::LIVE_STATE_SCHEMA_EPOCH;

pub(crate) fn local_version_head_live_row(
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> LiveRow {
    LiveRow {
        entity_id: version_id.to_string(),
        file_id: crate::version_state::version_ref_file_id().to_string(),
        schema_key: crate::version_state::version_ref_schema_key().to_string(),
        schema_version: crate::version_state::version_ref_schema_version().to_string(),
        version_id: crate::version_state::version_ref_storage_version_id().to_string(),
        plugin_key: crate::version_state::version_ref_plugin_key().to_string(),
        metadata: None,
        change_id: None,
        writer_key: None,
        global: true,
        untracked: true,
        created_at: Some(timestamp.to_string()),
        updated_at: Some(timestamp.to_string()),
        snapshot_content: Some(crate::version_state::version_ref_snapshot_content(
            version_id, commit_id,
        )),
    }
}

pub(crate) fn live_relation_name(schema_key: &str) -> String {
    crate::common::naming::tracked_relation_name(schema_key)
}

pub(crate) fn live_schema_column_names(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
) -> Result<Vec<String>, LixError> {
    crate::live_state::schema_access::schema_column_names(schema_key, schema_definition)
}

pub(crate) fn normalized_values_for_schema(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    snapshot_content: Option<&str>,
) -> Result<BTreeMap<String, Value>, LixError> {
    crate::live_state::schema_access::normalized_values_for_schema(
        schema_key,
        schema_definition,
        snapshot_content,
    )
}
