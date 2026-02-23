pub(crate) fn directory_history_projection_sql() -> String {
    "WITH RECURSIVE directory_history_base AS (\
                     SELECT \
                        lix_json_text(snapshot_content, 'id') AS id, \
                        lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                        lix_json_text(snapshot_content, 'name') AS name, \
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
                 ) \
                 SELECT \
                    d.id, \
                    d.parent_id, \
                    d.name, \
                    dp.path AS path, \
                    d.hidden, \
                    d.lixcol_entity_id, \
                    d.lixcol_schema_key, \
                    d.lixcol_file_id, \
                    d.lixcol_version_id, \
                    d.lixcol_plugin_key, \
                    d.lixcol_schema_version, \
                    d.lixcol_change_id, \
                    d.lixcol_metadata, \
                    d.lixcol_commit_id, \
                    d.lixcol_root_commit_id, \
                    d.lixcol_depth \
                 FROM directory_history_base d \
                 LEFT JOIN directory_history_paths dp \
                   ON dp.target_id = d.id \
                  AND dp.root_commit_id = d.lixcol_root_commit_id \
                  AND dp.target_depth = d.lixcol_depth"
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::directory_history_projection_sql;

    #[test]
    fn directory_history_projection_sql_uses_state_history_root_depth_contract() {
        let sql = directory_history_projection_sql();
        assert!(sql.contains("FROM lix_state_history"));
        assert!(sql.contains("root_commit_id AS lixcol_root_commit_id"));
        assert!(sql.contains("depth AS lixcol_depth"));
        assert!(sql.contains("directory_history_path_walk"));
    }
}
