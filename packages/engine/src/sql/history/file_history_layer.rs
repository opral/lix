pub(crate) fn file_history_projection_sql() -> String {
    "WITH RECURSIVE directory_history_base AS (\
                     SELECT \
                        lix_json_text(snapshot_content, 'id') AS id, \
                        lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                        lix_json_text(snapshot_content, 'name') AS name, \
                        root_commit_id AS lixcol_root_commit_id, \
                        depth AS lixcol_depth \
                     FROM lix_state_history \
                     WHERE schema_key = 'lix_directory_descriptor' \
                       AND snapshot_content IS NOT NULL\
                 ), \
                 directory_history_path_walk AS (\
                     SELECT \
                        base.id AS target_id, \
                        base.lixcol_root_commit_id AS root_commit_id, \
                        base.lixcol_depth AS target_depth, \
                        base.parent_id AS next_parent_id, \
                        '/' || base.name || '/' AS path, \
                        0 AS step \
                     FROM directory_history_base base \
                     UNION ALL \
                     SELECT \
                        walk.target_id, \
                        walk.root_commit_id, \
                        walk.target_depth, \
                        parent.parent_id AS next_parent_id, \
                        '/' || parent.name || walk.path AS path, \
                        walk.step + 1 AS step \
                     FROM directory_history_path_walk walk \
                     JOIN directory_history_base parent \
                       ON parent.id = walk.next_parent_id \
                      AND parent.lixcol_root_commit_id = walk.root_commit_id \
                      AND parent.lixcol_depth = (\
                          SELECT MIN(candidate.lixcol_depth) \
                         FROM directory_history_base candidate \
                          WHERE candidate.id = walk.next_parent_id \
                            AND candidate.lixcol_root_commit_id = walk.root_commit_id \
                            AND candidate.lixcol_depth >= walk.target_depth\
                      ) \
                     WHERE walk.next_parent_id IS NOT NULL \
                       AND walk.step < 1024 \
                 ), \
                 directory_history_paths AS (\
                     SELECT \
                        walk.target_id, \
                        walk.root_commit_id, \
                        walk.target_depth, \
                        walk.path \
                     FROM directory_history_path_walk walk \
                     JOIN (\
                        SELECT \
                            target_id, \
                            root_commit_id, \
                            target_depth, \
                            MAX(step) AS max_step \
                        FROM directory_history_path_walk \
                        WHERE next_parent_id IS NULL \
                        GROUP BY target_id, root_commit_id, target_depth\
                     ) terminal \
                       ON terminal.target_id = walk.target_id \
                      AND terminal.root_commit_id = walk.root_commit_id \
                      AND terminal.target_depth = walk.target_depth \
                      AND terminal.max_step = walk.step \
                 ), \
                 file_history_descriptor_rows AS (\
                    SELECT \
                        lix_json_text(snapshot_content, 'id') AS id, \
                        lix_json_text(snapshot_content, 'directory_id') AS directory_id, \
                        lix_json_text(snapshot_content, 'name') AS name, \
                        lix_json_text(snapshot_content, 'extension') AS extension, \
                        lix_json_text(snapshot_content, 'metadata') AS metadata, \
                        lix_json_text(snapshot_content, 'hidden') AS hidden, \
                        entity_id AS lixcol_entity_id, \
                        schema_key AS lixcol_schema_key, \
                        file_id AS lixcol_file_id, \
                        version_id AS lixcol_version_id, \
                        plugin_key AS lixcol_plugin_key, \
                        schema_version AS lixcol_schema_version, \
                        change_id AS lixcol_change_id, \
                        metadata AS lixcol_metadata, \
                        commit_id AS lixcol_commit_id, \
                        root_commit_id AS lixcol_root_commit_id, \
                        depth AS lixcol_depth \
                    FROM lix_state_history \
                    WHERE schema_key = 'lix_file_descriptor' \
                      AND snapshot_content IS NOT NULL\
                 ), \
                 descriptor_depth_zero_roots AS (\
                    SELECT \
                        id, \
                        lixcol_root_commit_id \
                    FROM file_history_descriptor_rows \
                    WHERE lixcol_depth = 0 \
                    GROUP BY id, lixcol_root_commit_id\
                 ), \
                 content_only_roots AS (\
                    SELECT \
                        ranked.id, \
                        ranked.lixcol_root_commit_id, \
                        ranked.lixcol_commit_id, \
                        ranked.lixcol_change_id \
                    FROM (\
                        SELECT \
                            sh.file_id AS id, \
                            sh.root_commit_id AS lixcol_root_commit_id, \
                            sh.commit_id AS lixcol_commit_id, \
                            sh.change_id AS lixcol_change_id, \
                            ROW_NUMBER() OVER (\
                                PARTITION BY sh.file_id, sh.root_commit_id \
                                ORDER BY ic.created_at DESC, sh.change_id DESC\
                            ) AS row_num \
                        FROM lix_state_history sh \
                        JOIN lix_internal_change ic \
                          ON ic.id = sh.change_id \
                        LEFT JOIN descriptor_depth_zero_roots d0 \
                          ON d0.id = sh.file_id \
                         AND d0.lixcol_root_commit_id = sh.root_commit_id \
                        WHERE sh.depth = 0 \
                          AND sh.file_id IS NOT NULL \
                          AND sh.file_id != 'lix' \
                          AND sh.schema_key != 'lix_file_descriptor' \
                          AND sh.snapshot_content IS NOT NULL \
                          AND d0.id IS NULL\
                    ) ranked \
                    WHERE ranked.row_num = 1\
                 ), \
                 content_history_rows AS (\
                    SELECT \
                        d.id, \
                        d.directory_id, \
                        d.name, \
                        d.extension, \
                        d.metadata, \
                        d.hidden, \
                        c.id AS lixcol_entity_id, \
                        d.lixcol_schema_key, \
                        c.id AS lixcol_file_id, \
                        d.lixcol_version_id, \
                        d.lixcol_plugin_key, \
                        d.lixcol_schema_version, \
                        c.lixcol_change_id, \
                        d.lixcol_metadata, \
                        c.lixcol_commit_id, \
                        c.lixcol_root_commit_id, \
                        0 AS lixcol_depth \
                    FROM content_only_roots c \
                    JOIN file_history_descriptor_rows d \
                      ON d.id = c.id \
                     AND d.lixcol_root_commit_id = c.lixcol_root_commit_id \
                     AND d.lixcol_depth = (\
                          SELECT MIN(candidate.lixcol_depth) \
                          FROM file_history_descriptor_rows candidate \
                          WHERE candidate.id = c.id \
                            AND candidate.lixcol_root_commit_id = c.lixcol_root_commit_id\
                     )\
                 ), \
                 file_history_rows AS (\
                    SELECT * FROM file_history_descriptor_rows \
                    UNION ALL \
                    SELECT * FROM content_history_rows\
                 ), \
                 file_history_ranked_rows AS (\
                    SELECT \
                        fhr.id, \
                        fhr.directory_id, \
                        fhr.name, \
                        fhr.extension, \
                        fhr.metadata, \
                        fhr.hidden, \
                        fhr.lixcol_entity_id, \
                        fhr.lixcol_schema_key, \
                        fhr.lixcol_file_id, \
                        fhr.lixcol_version_id, \
                        fhr.lixcol_plugin_key, \
                        fhr.lixcol_schema_version, \
                        fhr.lixcol_change_id, \
                        fhr.lixcol_metadata, \
                        fhr.lixcol_commit_id, \
                        fhr.lixcol_root_commit_id, \
                        fhr.lixcol_depth AS lixcol_raw_depth, \
                        ROW_NUMBER() OVER (\
                            PARTITION BY fhr.id, fhr.lixcol_root_commit_id \
                            ORDER BY \
                                fhr.lixcol_depth ASC, \
                                fhr.lixcol_commit_id DESC, \
                                fhr.lixcol_change_id DESC\
                        ) - 1 AS lixcol_depth \
                    FROM file_history_rows fhr\
                 ) \
                 SELECT \
                    f.id, \
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
                    COALESCE(fd.data, lix_empty_blob()) AS data, \
                    f.metadata, \
                    f.hidden, \
                    f.lixcol_entity_id, \
                    f.lixcol_schema_key, \
                    f.lixcol_file_id, \
                    f.lixcol_version_id, \
                    f.lixcol_plugin_key, \
                    f.lixcol_schema_version, \
                    f.lixcol_change_id, \
                    f.lixcol_metadata, \
                    f.lixcol_commit_id, \
                    f.lixcol_root_commit_id, \
                    f.lixcol_depth \
                 FROM file_history_ranked_rows f \
                 LEFT JOIN directory_history_paths dp \
                   ON dp.target_id = f.directory_id \
                  AND dp.root_commit_id = f.lixcol_root_commit_id \
                  AND dp.target_depth = (\
                      SELECT MIN(candidate.lixcol_depth) \
                      FROM directory_history_base candidate \
                      WHERE candidate.id = f.directory_id \
                        AND candidate.lixcol_root_commit_id = f.lixcol_root_commit_id \
                        AND candidate.lixcol_depth >= f.lixcol_raw_depth\
                  ) \
                 LEFT JOIN lix_internal_file_history_data_cache fd \
                   ON fd.file_id = f.id \
                  AND fd.root_commit_id = f.lixcol_root_commit_id \
                  AND fd.depth = f.lixcol_depth"
                .to_string()
}

pub(crate) fn missing_file_history_cache_descriptor_selection_sql() -> &'static str {
    "SELECT \
       id AS file_id, \
       lixcol_root_commit_id AS root_commit_id, \
       lixcol_depth AS depth, \
       lixcol_commit_id AS commit_id, \
       path \
     FROM lix_file_history \
     WHERE path IS NOT NULL \
       AND NOT EXISTS (\
         SELECT 1 \
         FROM lix_internal_file_history_data_cache cache \
         WHERE cache.file_id = id \
           AND cache.root_commit_id = lixcol_root_commit_id \
           AND cache.depth = lixcol_depth\
       ) \
     ORDER BY lixcol_root_commit_id, lixcol_depth, id"
}

pub(crate) fn plugin_history_state_changes_for_slice_sql() -> &'static str {
    // Depth contract: resolve the effective lower-bound depth from
    // commit ancestry for (root_commit_id, commit_id), falling back to
    // the requested descriptor depth ($5) when ancestry linkage is missing.
    "WITH target_commit_depth AS (\
       SELECT COALESCE((\
         SELECT depth \
         FROM lix_internal_commit_ancestry \
         WHERE commit_id = $3 \
           AND ancestor_id = $4 \
         LIMIT 1\
       ), $5) AS raw_depth\
     ) \
     SELECT entity_id, schema_key, schema_version, snapshot_content, depth \
     FROM lix_state_history \
     WHERE file_id = $1 \
       AND plugin_key = $2 \
       AND root_commit_id = $3 \
       AND depth >= (SELECT raw_depth FROM target_commit_depth) \
     ORDER BY entity_id ASC, depth ASC"
}

#[cfg(test)]
mod tests {
    use super::{
        missing_file_history_cache_descriptor_selection_sql,
        plugin_history_state_changes_for_slice_sql,
    };

    #[test]
    fn missing_history_cache_descriptor_sql_uses_shared_file_history_projection() {
        let sql = missing_file_history_cache_descriptor_selection_sql();
        assert!(sql.contains("FROM lix_file_history"));
        assert!(sql.contains("lix_internal_file_history_data_cache"));
        assert!(sql.contains("lixcol_root_commit_id AS root_commit_id"));
        assert!(sql.contains("lixcol_depth AS depth"));
        assert!(sql.contains("lixcol_commit_id AS commit_id"));
        assert!(sql.contains("ORDER BY lixcol_root_commit_id, lixcol_depth, id"));
    }

    #[test]
    fn file_history_projection_sql_keeps_root_depth_cache_join_contract() {
        let sql = super::file_history_projection_sql();
        assert!(sql.contains("LEFT JOIN lix_internal_file_history_data_cache fd"));
        assert!(sql.contains("fd.root_commit_id = f.lixcol_root_commit_id"));
        assert!(sql.contains("fd.depth = f.lixcol_depth"));
    }

    #[test]
    fn plugin_history_slice_sql_enforces_shared_depth_contract() {
        let sql = plugin_history_state_changes_for_slice_sql();
        assert!(sql.contains("FROM lix_internal_commit_ancestry"));
        assert!(sql.contains("COALESCE(("));
        assert!(sql.contains("root_commit_id = $3"));
        assert!(sql.contains("depth >= (SELECT raw_depth FROM target_commit_depth)"));
    }
}
