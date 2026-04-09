use crate::live_state::LiveRow;

pub(crate) fn local_version_head_live_row(
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> LiveRow {
    let row = super::projection::local_version_head_write_row(version_id, commit_id, timestamp);
    LiveRow {
        entity_id: row.entity_id,
        file_id: row.file_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        version_id: row.version_id,
        plugin_key: row.plugin_key,
        metadata: row.metadata,
        change_id: None,
        writer_key: row.writer_key,
        global: row.global,
        untracked: true,
        created_at: row.created_at,
        updated_at: Some(row.updated_at),
        snapshot_content: row.snapshot_content,
    }
}
