use crate::live_state::LiveRow;

pub(crate) use crate::live_state::lifecycle::LIVE_STATE_SCHEMA_EPOCH;

fn local_version_head_change_id(version_id: &str, commit_id: &str, timestamp: &str) -> String {
    format!("change-version-ref::{version_id}::{commit_id}::{timestamp}")
}

pub(crate) fn local_version_head_live_row(
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> LiveRow {
    LiveRow {
        entity_id: version_id.to_string(),
        file_id: None,
        schema_key: crate::version::version_ref_schema_key().to_string(),
        schema_version: crate::version::version_ref_schema_version().to_string(),
        version_id: crate::version::version_ref_storage_version_id().to_string(),
        plugin_key: None,
        metadata: None,
        change_id: Some(local_version_head_change_id(
            version_id, commit_id, timestamp,
        )),
        global: true,
        untracked: true,
        created_at: Some(timestamp.to_string()),
        updated_at: Some(timestamp.to_string()),
        snapshot_content: Some(crate::version::version_ref_snapshot_content(
            version_id, commit_id,
        )),
    }
}
