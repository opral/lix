pub(crate) const LIVE_FILE_PREFETCH_BLOB_HASH_COLUMN: &str = "__lix_blob_hash";

pub(crate) fn build_live_file_projection_sql() -> String {
    build_live_file_projection_sql_with_select(
        "f.id, \
         f.directory_id, \
         f.name, \
         f.extension, \
         CASE \
             WHEN f.directory_id IS NULL THEN \
                 CASE \
                     WHEN f.extension IS NULL OR f.extension = '' THEN '/' || f.name \
                     ELSE '/' || f.name || '.' || f.extension \
                 END \
             WHEN dp.path IS NULL THEN NULL \
             ELSE \
                 CASE \
                     WHEN f.extension IS NULL OR f.extension = '' THEN dp.path || f.name \
                     ELSE dp.path || f.name || '.' || f.extension \
                 END \
         END AS path, \
         bbs.data AS data, \
         f.metadata, \
         f.hidden, \
         f.lixcol_entity_id, \
         f.lixcol_schema_key, \
         f.lixcol_file_id, \
         f.lixcol_version_id, \
         f.lixcol_plugin_key, \
         f.lixcol_schema_version, \
         f.lixcol_global, \
         f.lixcol_change_id, \
         f.lixcol_created_at, \
         f.lixcol_updated_at, \
         f.lixcol_commit_id, \
         f.lixcol_writer_key, \
         f.lixcol_untracked, \
         f.lixcol_metadata",
    )
}

pub(crate) fn build_live_file_prefetch_projection_sql() -> String {
    build_live_file_projection_sql_with_select(&format!(
        "f.id, \
         f.directory_id, \
         f.name, \
         f.extension, \
         CASE \
             WHEN f.directory_id IS NULL THEN \
                 CASE \
                     WHEN f.extension IS NULL OR f.extension = '' THEN '/' || f.name \
                     ELSE '/' || f.name || '.' || f.extension \
                 END \
             WHEN dp.path IS NULL THEN NULL \
             ELSE \
                 CASE \
                     WHEN f.extension IS NULL OR f.extension = '' THEN dp.path || f.name \
                     ELSE dp.path || f.name || '.' || f.extension \
                 END \
         END AS path, \
         bbs.data AS data, \
         bfr.blob_hash AS {blob_hash_column}, \
         f.metadata, \
         f.hidden, \
         f.lixcol_entity_id, \
         f.lixcol_schema_key, \
         f.lixcol_file_id, \
         f.lixcol_version_id, \
         f.lixcol_plugin_key, \
         f.lixcol_schema_version, \
         f.lixcol_global, \
         f.lixcol_change_id, \
         f.lixcol_created_at, \
         f.lixcol_updated_at, \
         f.lixcol_commit_id, \
         f.lixcol_writer_key, \
         f.lixcol_untracked, \
         f.lixcol_metadata",
        blob_hash_column = LIVE_FILE_PREFETCH_BLOB_HASH_COLUMN
    ))
}

fn build_live_file_projection_sql_with_select(select_list: &str) -> String {
    format!(
        "WITH RECURSIVE directory_descriptor_rows AS (\
             SELECT \
                lix_json_extract(snapshot_content, 'id') AS id, \
                lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
                lix_json_extract(snapshot_content, 'name') AS name, \
                version_id AS lixcol_version_id \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_directory_descriptor' \
               AND snapshot_content IS NOT NULL\
         ), \
         directory_paths AS (\
             SELECT \
                d.id, \
                d.lixcol_version_id, \
                '/' || d.name || '/' AS path \
             FROM directory_descriptor_rows d \
             WHERE d.parent_id IS NULL \
             UNION ALL \
             SELECT \
                child.id, \
                child.lixcol_version_id, \
                parent.path || child.name || '/' AS path \
             FROM directory_descriptor_rows child \
             JOIN directory_paths parent \
               ON parent.id = child.parent_id \
              AND parent.lixcol_version_id = child.lixcol_version_id\
         ), \
         file_descriptor_rows AS (\
             SELECT \
                lix_json_extract(snapshot_content, 'id') AS id, \
                lix_json_extract(snapshot_content, 'directory_id') AS directory_id, \
                lix_json_extract(snapshot_content, 'name') AS name, \
                lix_json_extract(snapshot_content, 'extension') AS extension, \
                lix_json_extract(snapshot_content, 'metadata') AS metadata, \
                lix_json_extract(snapshot_content, 'hidden') AS hidden, \
                entity_id AS lixcol_entity_id, \
                schema_key AS lixcol_schema_key, \
                file_id AS lixcol_file_id, \
                version_id AS lixcol_version_id, \
                plugin_key AS lixcol_plugin_key, \
                schema_version AS lixcol_schema_version, \
                global AS lixcol_global, \
                change_id AS lixcol_change_id, \
                created_at AS lixcol_created_at, \
                updated_at AS lixcol_updated_at, \
                commit_id AS lixcol_commit_id, \
                writer_key AS lixcol_writer_key, \
                untracked AS lixcol_untracked, \
                metadata AS lixcol_metadata \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_file_descriptor' \
               AND snapshot_content IS NOT NULL\
         ), \
         binary_blob_ref_rows AS (\
             SELECT \
                lix_json_extract(snapshot_content, 'id') AS id, \
                version_id AS lixcol_version_id, \
                lix_json_extract(snapshot_content, 'blob_hash') AS blob_hash, \
                lix_json_extract(snapshot_content, 'size_bytes') AS size_bytes \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_binary_blob_ref' \
               AND snapshot_content IS NOT NULL\
         ) \
         SELECT {select_list} \
         FROM file_descriptor_rows f \
         LEFT JOIN directory_paths dp \
           ON dp.id = f.directory_id \
          AND dp.lixcol_version_id = f.lixcol_version_id \
         LEFT JOIN binary_blob_ref_rows bfr \
           ON bfr.id = f.id \
          AND bfr.lixcol_version_id = f.lixcol_version_id \
         LEFT JOIN lix_internal_binary_blob_store bbs \
           ON bbs.blob_hash = bfr.blob_hash"
    )
}
