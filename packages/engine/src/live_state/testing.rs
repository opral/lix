use crate::live_state::LiveRow;

pub(crate) use crate::live_state::lifecycle::LIVE_STATE_SCHEMA_EPOCH;

pub(crate) fn local_version_head_live_row(
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> LiveRow {
    LiveRow {
        entity_id: version_id.to_string(),
        file_id: crate::contracts::version_artifacts::version_ref_file_id().to_string(),
        schema_key: crate::contracts::version_artifacts::version_ref_schema_key().to_string(),
        schema_version: crate::contracts::version_artifacts::version_ref_schema_version()
            .to_string(),
        version_id: crate::contracts::version_artifacts::version_ref_storage_version_id()
            .to_string(),
        plugin_key: crate::contracts::version_artifacts::version_ref_plugin_key().to_string(),
        metadata: None,
        change_id: None,
        writer_key: None,
        global: true,
        untracked: true,
        created_at: Some(timestamp.to_string()),
        updated_at: Some(timestamp.to_string()),
        snapshot_content: Some(
            crate::contracts::version_artifacts::version_ref_snapshot_content(
                version_id, commit_id,
            ),
        ),
    }
}
