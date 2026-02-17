use sqlparser::ast::Query;

use crate::sql::{escape_sql_string, parse_single_query};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
};
use crate::LixError;

pub(crate) const FILE_VIEW: &str = "lix_file";
pub(crate) const FILE_BY_VERSION_VIEW: &str = "lix_file_by_version";
pub(crate) const FILE_HISTORY_VIEW: &str = "lix_file_history";
pub(crate) const DIRECTORY_VIEW: &str = "lix_directory";
pub(crate) const DIRECTORY_BY_VERSION_VIEW: &str = "lix_directory_by_version";
pub(crate) const DIRECTORY_HISTORY_VIEW: &str = "lix_directory_history";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilesystemReadOp {
    File,
    FileByVersion,
    FileHistory,
    Directory,
    DirectoryByVersion,
    DirectoryHistory,
}

pub(crate) fn infer_filesystem_read_op(view_name: &str) -> Option<FilesystemReadOp> {
    match view_name.to_ascii_lowercase().as_str() {
        FILE_VIEW => Some(FilesystemReadOp::File),
        FILE_BY_VERSION_VIEW => Some(FilesystemReadOp::FileByVersion),
        FILE_HISTORY_VIEW => Some(FilesystemReadOp::FileHistory),
        DIRECTORY_VIEW => Some(FilesystemReadOp::Directory),
        DIRECTORY_BY_VERSION_VIEW => Some(FilesystemReadOp::DirectoryByVersion),
        DIRECTORY_HISTORY_VIEW => Some(FilesystemReadOp::DirectoryHistory),
        _ => None,
    }
}

pub(crate) fn build_filesystem_projection_query(
    view_name: &str,
) -> Result<Option<Query>, LixError> {
    let Some(op) = infer_filesystem_read_op(view_name) else {
        return Ok(None);
    };
    Ok(Some(build_projection_query(op)?))
}

fn build_projection_query(op: FilesystemReadOp) -> Result<Query, LixError> {
    let sql = match op {
        FilesystemReadOp::File => format!(
            "WITH RECURSIVE directory_descriptor_rows AS (\
                 SELECT \
                    lix_json_text(snapshot_content, 'id') AS id, \
                    lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_text(snapshot_content, 'name') AS name, \
                    version_id AS lixcol_version_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_directory_descriptor' \
                   AND snapshot_content IS NOT NULL \
                   AND {active_version_scope_descriptor}\
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
                    inherited_from_version_id AS lixcol_inherited_from_version_id, \
                    change_id AS lixcol_change_id, \
                    created_at AS lixcol_created_at, \
                    updated_at AS lixcol_updated_at, \
                    writer_key AS lixcol_writer_key, \
                    untracked AS lixcol_untracked, \
                    metadata AS lixcol_metadata \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_file_descriptor' \
                   AND snapshot_content IS NOT NULL \
                   AND {active_version_scope_descriptor}\
             ) \
             SELECT \
                f.id, \
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
                COALESCE(fd.data, lix_empty_blob()) AS data, \
                f.metadata, \
                f.hidden, \
                f.lixcol_entity_id, \
                f.lixcol_schema_key, \
                f.lixcol_file_id, \
                f.lixcol_plugin_key, \
                f.lixcol_schema_version, \
                f.lixcol_inherited_from_version_id, \
                f.lixcol_change_id, \
                f.lixcol_created_at, \
                f.lixcol_updated_at, \
                v.commit_id AS lixcol_commit_id, \
                f.lixcol_writer_key, \
                f.lixcol_untracked, \
                f.lixcol_metadata \
             FROM file_descriptor_rows f \
             LEFT JOIN directory_paths dp \
               ON dp.id = f.directory_id \
              AND dp.lixcol_version_id = f.lixcol_version_id \
             LEFT JOIN lix_version v \
               ON v.id = f.lixcol_version_id \
             LEFT JOIN lix_internal_file_data_cache fd \
               ON fd.file_id = f.id \
              AND fd.version_id = f.lixcol_version_id \
             WHERE {active_version_scope}",
            active_version_scope = active_version_scope_predicate("f.lixcol_version_id"),
            active_version_scope_descriptor = active_version_scope_predicate("version_id")
        ),
        FilesystemReadOp::FileByVersion => "WITH RECURSIVE directory_descriptor_rows AS (\
                 SELECT \
                    lix_json_text(snapshot_content, 'id') AS id, \
                    lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_text(snapshot_content, 'name') AS name, \
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
                    inherited_from_version_id AS lixcol_inherited_from_version_id, \
                    change_id AS lixcol_change_id, \
                    created_at AS lixcol_created_at, \
                    updated_at AS lixcol_updated_at, \
                    writer_key AS lixcol_writer_key, \
                    untracked AS lixcol_untracked, \
                    metadata AS lixcol_metadata \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_file_descriptor' \
                   AND snapshot_content IS NOT NULL\
             ) \
             SELECT \
                f.id, \
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
                COALESCE(fd.data, lix_empty_blob()) AS data, \
                f.metadata, \
                f.hidden, \
                f.lixcol_entity_id, \
                f.lixcol_schema_key, \
                f.lixcol_file_id, \
                f.lixcol_version_id, \
                f.lixcol_plugin_key, \
                f.lixcol_schema_version, \
                f.lixcol_inherited_from_version_id, \
                f.lixcol_change_id, \
                f.lixcol_created_at, \
                f.lixcol_updated_at, \
                v.commit_id AS lixcol_commit_id, \
                f.lixcol_writer_key, \
                f.lixcol_untracked, \
                f.lixcol_metadata \
             FROM file_descriptor_rows f \
             LEFT JOIN directory_paths dp \
               ON dp.id = f.directory_id \
              AND dp.lixcol_version_id = f.lixcol_version_id \
             LEFT JOIN lix_version v \
               ON v.id = f.lixcol_version_id \
             LEFT JOIN lix_internal_file_data_cache fd \
               ON fd.file_id = f.id \
              AND fd.version_id = f.lixcol_version_id"
            .to_string(),
        FilesystemReadOp::FileHistory => "WITH RECURSIVE directory_history_base AS (\
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
            .to_string(),
        FilesystemReadOp::Directory => format!(
            "WITH RECURSIVE directory_descriptor_rows AS (\
                 SELECT \
                    lix_json_text(snapshot_content, 'id') AS id, \
                    lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_text(snapshot_content, 'name') AS name, \
                    lix_json_text(snapshot_content, 'hidden') AS hidden, \
                    entity_id AS lixcol_entity_id, \
                    schema_key AS lixcol_schema_key, \
                    schema_version AS lixcol_schema_version, \
                    version_id AS lixcol_version_id, \
                    inherited_from_version_id AS lixcol_inherited_from_version_id, \
                    change_id AS lixcol_change_id, \
                    metadata AS lixcol_metadata, \
                    created_at AS lixcol_created_at, \
                    updated_at AS lixcol_updated_at, \
                    untracked AS lixcol_untracked \
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
             ) \
             SELECT \
                d.id, \
                d.parent_id, \
                d.name, \
                dp.path AS path, \
                d.hidden, \
                d.lixcol_entity_id, \
                d.lixcol_schema_key, \
                d.lixcol_schema_version, \
                d.lixcol_inherited_from_version_id, \
                d.lixcol_change_id, \
                d.lixcol_created_at, \
                d.lixcol_updated_at, \
                v.commit_id AS lixcol_commit_id, \
                d.lixcol_untracked, \
                d.lixcol_metadata \
             FROM directory_descriptor_rows d \
             LEFT JOIN directory_paths dp \
               ON dp.id = d.id \
              AND dp.lixcol_version_id = d.lixcol_version_id \
             LEFT JOIN lix_version v \
               ON v.id = d.lixcol_version_id \
             WHERE {active_version_scope}",
            active_version_scope = active_version_scope_predicate("d.lixcol_version_id")
        ),
        FilesystemReadOp::DirectoryByVersion => "WITH RECURSIVE directory_descriptor_rows AS (\
                 SELECT \
                    lix_json_text(snapshot_content, 'id') AS id, \
                    lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_text(snapshot_content, 'name') AS name, \
                    lix_json_text(snapshot_content, 'hidden') AS hidden, \
                    entity_id AS lixcol_entity_id, \
                    schema_key AS lixcol_schema_key, \
                    schema_version AS lixcol_schema_version, \
                    version_id AS lixcol_version_id, \
                    inherited_from_version_id AS lixcol_inherited_from_version_id, \
                    change_id AS lixcol_change_id, \
                    metadata AS lixcol_metadata, \
                    created_at AS lixcol_created_at, \
                    updated_at AS lixcol_updated_at, \
                    untracked AS lixcol_untracked \
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
             ) \
             SELECT \
                d.id, \
                d.parent_id, \
                d.name, \
                dp.path AS path, \
                d.hidden, \
                d.lixcol_entity_id, \
                d.lixcol_schema_key, \
                d.lixcol_schema_version, \
                d.lixcol_version_id, \
                d.lixcol_inherited_from_version_id, \
                d.lixcol_change_id, \
                d.lixcol_created_at, \
                d.lixcol_updated_at, \
                v.commit_id AS lixcol_commit_id, \
                d.lixcol_untracked, \
                d.lixcol_metadata \
             FROM directory_descriptor_rows d \
             LEFT JOIN directory_paths dp \
               ON dp.id = d.id \
              AND dp.lixcol_version_id = d.lixcol_version_id \
             LEFT JOIN lix_version v \
               ON v.id = d.lixcol_version_id"
            .to_string(),
        FilesystemReadOp::DirectoryHistory => "WITH RECURSIVE directory_history_base AS (\
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
            .to_string(),
    };
    parse_single_query(&sql)
}

fn active_version_scope_predicate(version_column: &str) -> String {
    format!(
        "{version_column} IN (\
         SELECT lix_json_text(snapshot_content, 'version_id') \
         FROM lix_internal_state_untracked \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{storage_version_id}' \
           AND snapshot_content IS NOT NULL\
         )",
        version_column = version_column,
        schema_key = escape_sql_string(active_version_schema_key()),
        file_id = escape_sql_string(active_version_file_id()),
        storage_version_id = escape_sql_string(active_version_storage_version_id()),
    )
}
