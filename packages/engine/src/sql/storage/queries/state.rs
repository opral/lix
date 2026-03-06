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
