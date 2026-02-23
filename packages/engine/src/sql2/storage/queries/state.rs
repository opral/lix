use super::super::tables;

pub(crate) fn insert_detected_file_domain_changes_sql(row_values: &str, untracked: bool) -> String {
    if untracked {
        return format!(
            "INSERT INTO {} (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, metadata, writer_key, untracked\
             ) VALUES {row_values}",
            tables::state::INTERNAL_STATE_VTABLE,
        );
    }

    format!(
        "INSERT INTO {} (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, metadata, writer_key\
         ) VALUES {row_values}",
        tables::state::STATE_BY_VERSION,
    )
}

pub(crate) fn select_builtin_binary_blob_ref_snapshot_sql() -> String {
    format!(
        "SELECT snapshot_content \
         FROM {} \
         WHERE file_id = $1 \
           AND version_id = $2 \
           AND plugin_key = $3 \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL \
         ORDER BY updated_at DESC \
         LIMIT 1",
        tables::state::INTERNAL_STATE_MATERIALIZED_LIX_BINARY_BLOB_REF,
    )
}
