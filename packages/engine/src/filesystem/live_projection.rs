use crate::engine::query_storage::sql_text::escape_sql_string;
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    version_descriptor_schema_key, GLOBAL_VERSION_ID,
};
use crate::{LixBackend, LixError, Value};

pub(crate) const LIVE_FILE_PREFETCH_BLOB_HASH_COLUMN: &str = "__lix_blob_hash";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilesystemProjectionScope {
    ActiveVersion,
    ExplicitVersion,
}

pub(crate) fn build_live_file_prefetch_projection_sql() -> String {
    build_filesystem_file_projection_sql(FilesystemProjectionScope::ExplicitVersion, true)
}

pub(crate) async fn resolve_file_id_by_path_in_version(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &str,
) -> Result<Option<String>, LixError> {
    let sql = format!(
        "SELECT id \
         FROM ({live_projection_sql}) AS live_files \
         WHERE lixcol_version_id = '{version_id}' \
           AND path = '{path}' \
         LIMIT 1",
        live_projection_sql = build_live_file_prefetch_projection_sql(),
        version_id = escape_sql_string(version_id),
        path = escape_sql_string(path),
    );
    let result = backend.execute(&sql, &[]).await?;
    Ok(result.rows.first().and_then(|row| match row.first() {
        Some(Value::Text(id)) => Some(id.clone()),
        _ => None,
    }))
}

pub(crate) fn build_filesystem_file_projection_sql(
    scope: FilesystemProjectionScope,
    include_blob_hash: bool,
) -> String {
    let commit_id_projection = match scope {
        FilesystemProjectionScope::ActiveVersion => active_version_commit_id_sql(),
        FilesystemProjectionScope::ExplicitVersion => "f.lixcol_commit_id".to_string(),
    };
    let blob_hash_projection = if include_blob_hash {
        format!(", bfr.blob_hash AS {LIVE_FILE_PREFETCH_BLOB_HASH_COLUMN}")
    } else {
        String::new()
    };

    format!(
        "WITH RECURSIVE \
           {target_versions_cte}, \
           commit_by_version AS ( \
             SELECT \
               entity_id AS commit_id, \
               lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id \
             FROM lix_internal_state_materialized_v1_lix_commit \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           change_set_element_by_version AS ( \
             SELECT \
               lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id, \
               lix_json_extract(snapshot_content, 'change_id') AS change_id \
             FROM lix_internal_state_materialized_v1_lix_change_set_element \
             WHERE schema_key = 'lix_change_set_element' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           change_commit_by_change_id AS ( \
             SELECT \
               cse.change_id AS change_id, \
               MAX(cbv.commit_id) AS commit_id \
             FROM change_set_element_by_version cse \
             JOIN commit_by_version cbv \
               ON cbv.change_set_id = cse.change_set_id \
             WHERE cse.change_id IS NOT NULL \
             GROUP BY cse.change_id \
           ), \
           directory_descriptor_candidates AS ( \
             {directory_candidates_sql} \
           ), \
           directory_descriptor_ranked AS ( \
             SELECT \
               c.entity_id AS entity_id, \
               c.schema_key AS schema_key, \
               c.file_id AS file_id, \
               c.version_id AS version_id, \
               c.plugin_key AS plugin_key, \
               c.snapshot_content AS snapshot_content, \
               c.schema_version AS schema_version, \
               c.created_at AS created_at, \
               c.updated_at AS updated_at, \
               c.global AS global, \
               c.change_id AS change_id, \
               c.commit_id AS commit_id, \
               c.untracked AS untracked, \
               c.writer_key AS writer_key, \
               c.metadata AS metadata, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY c.version_id, c.entity_id, c.schema_key, c.file_id \
                 ORDER BY \
                   c.precedence ASC, \
                   c.updated_at DESC, \
                   c.created_at DESC, \
                   COALESCE(c.change_id, '') DESC \
               ) AS rn \
             FROM directory_descriptor_candidates c \
           ), \
           directory_descriptor_rows AS ( \
             SELECT \
               lix_json_extract(snapshot_content, 'id') AS id, \
               lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
               lix_json_extract(snapshot_content, 'name') AS name, \
               version_id AS lixcol_version_id \
             FROM directory_descriptor_ranked \
             WHERE rn = 1 \
               AND snapshot_content IS NOT NULL \
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
           file_descriptor_candidates AS ( \
             {file_candidates_sql} \
           ), \
           file_descriptor_ranked AS ( \
             SELECT \
               c.entity_id AS entity_id, \
               c.schema_key AS schema_key, \
               c.file_id AS file_id, \
               c.version_id AS version_id, \
               c.plugin_key AS plugin_key, \
               c.snapshot_content AS snapshot_content, \
               c.schema_version AS schema_version, \
               c.created_at AS created_at, \
               c.updated_at AS updated_at, \
               c.global AS global, \
               c.change_id AS change_id, \
               c.commit_id AS commit_id, \
               c.untracked AS untracked, \
               c.writer_key AS writer_key, \
               c.metadata AS metadata, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY c.version_id, c.entity_id, c.schema_key, c.file_id \
                 ORDER BY \
                   c.precedence ASC, \
                   c.updated_at DESC, \
                   c.created_at DESC, \
                   COALESCE(c.change_id, '') DESC \
               ) AS rn \
             FROM file_descriptor_candidates c \
           ), \
           file_descriptor_rows AS ( \
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
             FROM file_descriptor_ranked \
             WHERE rn = 1 \
               AND snapshot_content IS NOT NULL \
           ), \
           binary_blob_ref_candidates AS ( \
             {blob_candidates_sql} \
           ), \
           binary_blob_ref_ranked AS ( \
             SELECT \
               c.entity_id AS entity_id, \
               c.version_id AS version_id, \
               c.snapshot_content AS snapshot_content, \
               c.updated_at AS updated_at, \
               c.created_at AS created_at, \
               c.change_id AS change_id, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY c.version_id, c.entity_id \
                 ORDER BY \
                   c.precedence ASC, \
                   c.updated_at DESC, \
                   c.created_at DESC, \
                   COALESCE(c.change_id, '') DESC \
               ) AS rn \
             FROM binary_blob_ref_candidates c \
           ), \
           binary_blob_ref_rows AS ( \
             SELECT \
               lix_json_extract(snapshot_content, 'id') AS id, \
               version_id AS lixcol_version_id, \
               lix_json_extract(snapshot_content, 'blob_hash') AS blob_hash, \
               lix_json_extract(snapshot_content, 'size_bytes') AS size_bytes \
             FROM binary_blob_ref_ranked \
             WHERE rn = 1 \
               AND snapshot_content IS NOT NULL \
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
           bbs.data AS data{blob_hash_projection}, \
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
           {commit_id_projection} AS lixcol_commit_id, \
           f.lixcol_writer_key, \
           f.lixcol_untracked, \
           f.lixcol_metadata \
         FROM file_descriptor_rows f \
         LEFT JOIN directory_paths dp \
           ON dp.id = f.directory_id \
          AND dp.lixcol_version_id = f.lixcol_version_id \
         LEFT JOIN binary_blob_ref_rows bfr \
           ON bfr.id = f.id \
          AND bfr.lixcol_version_id = f.lixcol_version_id \
         LEFT JOIN lix_internal_binary_blob_store bbs \
           ON bbs.blob_hash = bfr.blob_hash",
        target_versions_cte = target_versions_cte_sql(
            scope,
            &[
                FILE_DESCRIPTOR_SCHEMA_KEY,
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                BINARY_BLOB_REF_SCHEMA_KEY
            ]
        ),
        directory_candidates_sql = effective_state_candidates_sql(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        file_candidates_sql = effective_state_candidates_sql(FILE_DESCRIPTOR_SCHEMA_KEY),
        blob_candidates_sql = effective_state_candidates_sql(BINARY_BLOB_REF_SCHEMA_KEY),
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        blob_hash_projection = blob_hash_projection,
        commit_id_projection = commit_id_projection,
    )
}

pub(crate) fn build_filesystem_directory_projection_sql(
    scope: FilesystemProjectionScope,
) -> String {
    let commit_id_projection = match scope {
        FilesystemProjectionScope::ActiveVersion => active_version_commit_id_sql(),
        FilesystemProjectionScope::ExplicitVersion => "d.lixcol_commit_id".to_string(),
    };
    format!(
        "WITH RECURSIVE \
           {target_versions_cte}, \
           commit_by_version AS ( \
             SELECT \
               entity_id AS commit_id, \
               lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id \
             FROM lix_internal_state_materialized_v1_lix_commit \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           change_set_element_by_version AS ( \
             SELECT \
               lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id, \
               lix_json_extract(snapshot_content, 'change_id') AS change_id \
             FROM lix_internal_state_materialized_v1_lix_change_set_element \
             WHERE schema_key = 'lix_change_set_element' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           change_commit_by_change_id AS ( \
             SELECT \
               cse.change_id AS change_id, \
               MAX(cbv.commit_id) AS commit_id \
             FROM change_set_element_by_version cse \
             JOIN commit_by_version cbv \
               ON cbv.change_set_id = cse.change_set_id \
             WHERE cse.change_id IS NOT NULL \
             GROUP BY cse.change_id \
           ), \
           directory_descriptor_candidates AS ( \
             {directory_candidates_sql} \
           ), \
           directory_descriptor_ranked AS ( \
             SELECT \
               c.entity_id AS entity_id, \
               c.schema_key AS schema_key, \
               c.file_id AS file_id, \
               c.version_id AS version_id, \
               c.plugin_key AS plugin_key, \
               c.snapshot_content AS snapshot_content, \
               c.schema_version AS schema_version, \
               c.created_at AS created_at, \
               c.updated_at AS updated_at, \
               c.global AS global, \
               c.change_id AS change_id, \
               c.commit_id AS commit_id, \
               c.untracked AS untracked, \
               c.writer_key AS writer_key, \
               c.metadata AS metadata, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY c.version_id, c.entity_id, c.schema_key, c.file_id \
                 ORDER BY \
                   c.precedence ASC, \
                   c.updated_at DESC, \
                   c.created_at DESC, \
                   COALESCE(c.change_id, '') DESC \
               ) AS rn \
             FROM directory_descriptor_candidates c \
           ), \
           directory_descriptor_rows AS ( \
             SELECT \
               lix_json_extract(snapshot_content, 'id') AS id, \
               lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
               lix_json_extract(snapshot_content, 'name') AS name, \
               lix_json_extract(snapshot_content, 'hidden') AS hidden, \
               entity_id AS lixcol_entity_id, \
               schema_key AS lixcol_schema_key, \
               schema_version AS lixcol_schema_version, \
               version_id AS lixcol_version_id, \
               global AS lixcol_global, \
               change_id AS lixcol_change_id, \
               created_at AS lixcol_created_at, \
               updated_at AS lixcol_updated_at, \
               commit_id AS lixcol_commit_id, \
               untracked AS lixcol_untracked, \
               metadata AS lixcol_metadata \
             FROM directory_descriptor_ranked \
             WHERE rn = 1 \
               AND snapshot_content IS NOT NULL \
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
            d.lixcol_global, \
            d.lixcol_change_id, \
            d.lixcol_created_at, \
            d.lixcol_updated_at, \
            {commit_id_projection} AS lixcol_commit_id, \
            d.lixcol_untracked, \
            d.lixcol_metadata \
         FROM directory_descriptor_rows d \
         LEFT JOIN directory_paths dp \
           ON dp.id = d.id \
          AND dp.lixcol_version_id = d.lixcol_version_id",
        target_versions_cte = target_versions_cte_sql(scope, &[DIRECTORY_DESCRIPTOR_SCHEMA_KEY]),
        directory_candidates_sql = effective_state_candidates_sql(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        commit_id_projection = commit_id_projection,
    )
}

pub(crate) fn build_filesystem_file_history_projection_sql(
    state_history_source_sql: &str,
) -> String {
    format!(
        "WITH RECURSIVE \
           state_history_source AS ( \
             {state_history_source_sql} \
           ), \
           directory_history_base AS (\
                SELECT \
                   lix_json_extract(snapshot_content, 'id') AS id, \
                   lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
                   lix_json_extract(snapshot_content, 'name') AS name, \
                   change_id AS lixcol_change_id, \
                   commit_id AS lixcol_commit_id, \
                   commit_created_at AS lixcol_commit_created_at, \
                   root_commit_id AS lixcol_root_commit_id, \
                   depth AS lixcol_depth \
                FROM state_history_source \
                WHERE schema_key = 'lix_directory_descriptor' \
                  AND snapshot_content IS NOT NULL \
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
                   change_id AS lixcol_change_id, \
                   commit_id AS lixcol_origin_commit_id, \
                   commit_created_at AS lixcol_commit_created_at, \
                   metadata AS lixcol_metadata, \
                   root_commit_id AS lixcol_root_commit_id, \
                   depth AS lixcol_depth \
               FROM state_history_source \
               WHERE schema_key = 'lix_file_descriptor' \
                 AND snapshot_content IS NOT NULL \
            ), \
            binary_blob_ref_history_rows AS (\
               SELECT \
                   sh.file_id AS id, \
                   sh.change_id AS lixcol_change_id, \
                   sh.commit_id AS lixcol_commit_id, \
                   sh.commit_created_at AS lixcol_commit_created_at, \
                   sh.root_commit_id AS lixcol_root_commit_id, \
                   sh.depth AS lixcol_depth, \
                   lix_json_extract(sh.snapshot_content, 'blob_hash') AS blob_hash \
               FROM state_history_source sh \
               WHERE sh.schema_key = 'lix_binary_blob_ref' \
                 AND sh.snapshot_content IS NOT NULL \
            ), \
            binary_blob_ref_history_depth_bounds AS (\
               SELECT \
                   b.id, \
                   b.lixcol_root_commit_id, \
                   MAX(b.lixcol_depth) AS max_lixcol_depth \
               FROM binary_blob_ref_history_rows b \
               GROUP BY b.id, b.lixcol_root_commit_id\
            ), \
            file_history_directory_checkpoint_candidates AS (\
               SELECT DISTINCT \
                   d.id, \
                   dir.lixcol_root_commit_id, \
                   dir.lixcol_depth, \
                   dir.lixcol_change_id, \
                   dir.lixcol_commit_id, \
                   dir.lixcol_commit_created_at \
               FROM file_history_descriptor_rows d \
               JOIN directory_history_base dir \
                 ON dir.id = d.directory_id \
                AND dir.lixcol_root_commit_id = d.lixcol_root_commit_id \
               LEFT JOIN binary_blob_ref_history_depth_bounds bounds \
                 ON bounds.id = d.id \
                AND bounds.lixcol_root_commit_id = d.lixcol_root_commit_id \
               WHERE bounds.max_lixcol_depth IS NULL \
                  OR dir.lixcol_depth <= bounds.max_lixcol_depth \
            ), \
            file_history_checkpoint_candidates AS (\
               SELECT \
                   d.id, \
                   d.lixcol_root_commit_id, \
                   d.lixcol_depth, \
                   d.lixcol_change_id, \
                   d.lixcol_origin_commit_id AS lixcol_commit_id, \
                   d.lixcol_commit_created_at \
               FROM file_history_descriptor_rows d \
               LEFT JOIN binary_blob_ref_history_depth_bounds bounds \
                 ON bounds.id = d.id \
                AND bounds.lixcol_root_commit_id = d.lixcol_root_commit_id \
               WHERE bounds.max_lixcol_depth IS NULL \
                  OR d.lixcol_depth <= bounds.max_lixcol_depth \
               UNION ALL \
               SELECT \
                   b.id, \
                   b.lixcol_root_commit_id, \
                   b.lixcol_depth, \
                   b.lixcol_change_id, \
                   b.lixcol_commit_id, \
                   b.lixcol_commit_created_at \
               FROM binary_blob_ref_history_rows b \
               UNION ALL \
               SELECT \
                   dir.id, \
                   dir.lixcol_root_commit_id, \
                   dir.lixcol_depth, \
                   dir.lixcol_change_id, \
                   dir.lixcol_commit_id, \
                   dir.lixcol_commit_created_at \
               FROM file_history_directory_checkpoint_candidates dir\
            ), \
            file_history_checkpoint_rows AS (\
               SELECT \
                   ranked.id, \
                   ranked.lixcol_root_commit_id, \
                   ranked.lixcol_depth AS lixcol_raw_depth, \
                   ranked.lixcol_change_id, \
                   ranked.lixcol_commit_id, \
                   ranked.lixcol_commit_created_at \
               FROM (\
                   SELECT \
                       candidate.id, \
                       candidate.lixcol_root_commit_id, \
                       candidate.lixcol_depth, \
                       candidate.lixcol_change_id, \
                       candidate.lixcol_commit_id, \
                       candidate.lixcol_commit_created_at, \
                       ROW_NUMBER() OVER (\
                           PARTITION BY candidate.id, candidate.lixcol_root_commit_id, candidate.lixcol_depth \
                           ORDER BY candidate.lixcol_commit_created_at DESC, candidate.lixcol_commit_id DESC, candidate.lixcol_change_id DESC\
                       ) AS row_num \
                   FROM file_history_checkpoint_candidates candidate\
               ) ranked \
               WHERE ranked.row_num = 1\
            ), \
            file_history_ranked_checkpoints AS (\
               SELECT \
                   checkpoint.id, \
                   checkpoint.lixcol_root_commit_id, \
                   checkpoint.lixcol_raw_depth, \
                   checkpoint.lixcol_change_id, \
                   checkpoint.lixcol_commit_id, \
                   checkpoint.lixcol_commit_created_at, \
                   ROW_NUMBER() OVER (\
                       PARTITION BY checkpoint.id, checkpoint.lixcol_root_commit_id \
                       ORDER BY checkpoint.lixcol_raw_depth ASC, checkpoint.lixcol_commit_id DESC, checkpoint.lixcol_change_id DESC\
                   ) - 1 AS lixcol_depth \
               FROM file_history_checkpoint_rows checkpoint\
            ) \
            SELECT \
               checkpoint.id, \
               CASE \
                   WHEN descriptor.directory_id IS NULL THEN \
                       CASE \
                           WHEN descriptor.extension IS NULL OR descriptor.extension = '' THEN '/' || descriptor.name \
                           ELSE '/' || descriptor.name || '.' || descriptor.extension \
                       END \
                   WHEN dp.path IS NULL THEN NULL \
                   ELSE \
                       CASE \
                           WHEN descriptor.extension IS NULL OR descriptor.extension = '' THEN dp.path || descriptor.name \
                           ELSE dp.path || descriptor.name || '.' || descriptor.extension \
                       END \
               END AS path, \
               COALESCE(fd.data, bbs.data) AS data, \
               descriptor.metadata, \
               descriptor.hidden, \
               checkpoint.id AS lixcol_entity_id, \
               descriptor.lixcol_schema_key, \
               CASE \
                   WHEN descriptor.lixcol_change_id = checkpoint.lixcol_change_id THEN descriptor.lixcol_file_id \
                   ELSE checkpoint.id \
               END AS lixcol_file_id, \
               descriptor.lixcol_version_id, \
               descriptor.lixcol_plugin_key, \
               descriptor.lixcol_schema_version, \
               checkpoint.lixcol_change_id, \
               descriptor.lixcol_metadata, \
               checkpoint.lixcol_commit_id, \
               checkpoint.lixcol_commit_created_at, \
               checkpoint.lixcol_root_commit_id, \
               checkpoint.lixcol_depth \
            FROM file_history_ranked_checkpoints checkpoint \
            JOIN file_history_descriptor_rows descriptor \
              ON descriptor.id = checkpoint.id \
             AND descriptor.lixcol_root_commit_id = checkpoint.lixcol_root_commit_id \
             AND descriptor.lixcol_depth = (\
                 SELECT MIN(candidate.lixcol_depth) \
                 FROM file_history_descriptor_rows candidate \
                 WHERE candidate.id = checkpoint.id \
                   AND candidate.lixcol_root_commit_id = checkpoint.lixcol_root_commit_id \
                   AND candidate.lixcol_depth >= checkpoint.lixcol_raw_depth\
             ) \
            LEFT JOIN directory_history_paths dp \
              ON dp.target_id = descriptor.directory_id \
             AND dp.root_commit_id = checkpoint.lixcol_root_commit_id \
             AND dp.target_depth = (\
                 SELECT MIN(candidate.lixcol_depth) \
                 FROM directory_history_base candidate \
                 WHERE candidate.id = descriptor.directory_id \
                   AND candidate.lixcol_root_commit_id = checkpoint.lixcol_root_commit_id \
                   AND candidate.lixcol_depth >= descriptor.lixcol_depth\
             ) \
            LEFT JOIN lix_internal_file_history_data_cache fd \
              ON fd.file_id = checkpoint.id \
             AND fd.root_commit_id = checkpoint.lixcol_root_commit_id \
             AND fd.depth = checkpoint.lixcol_depth \
            LEFT JOIN binary_blob_ref_history_rows bhr \
              ON bhr.id = checkpoint.id \
             AND bhr.lixcol_root_commit_id = checkpoint.lixcol_root_commit_id \
             AND bhr.lixcol_depth = (\
                 SELECT MIN(candidate.lixcol_depth) \
                 FROM binary_blob_ref_history_rows candidate \
                 WHERE candidate.id = checkpoint.id \
                   AND candidate.lixcol_root_commit_id = checkpoint.lixcol_root_commit_id \
                   AND candidate.lixcol_depth >= checkpoint.lixcol_raw_depth\
             ) \
            LEFT JOIN lix_internal_binary_blob_store bbs \
              ON bbs.blob_hash = bhr.blob_hash",
        state_history_source_sql = state_history_source_sql,
    )
}

pub(crate) fn build_filesystem_directory_history_projection_sql(
    state_history_source_sql: &str,
) -> String {
    format!(
        "WITH RECURSIVE \
           state_history_source AS ( \
             {state_history_source_sql} \
           ), \
           directory_history_base AS (\
                SELECT \
                   lix_json_extract(snapshot_content, 'id') AS id, \
                   lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
                   lix_json_extract(snapshot_content, 'name') AS name, \
                   lix_json_extract(snapshot_content, 'hidden') AS hidden, \
                   entity_id AS lixcol_entity_id, \
                   schema_key AS lixcol_schema_key, \
                   file_id AS lixcol_file_id, \
                   version_id AS lixcol_version_id, \
                   plugin_key AS lixcol_plugin_key, \
                   schema_version AS lixcol_schema_version, \
                   change_id AS lixcol_change_id, \
                   metadata AS lixcol_metadata, \
                   commit_id AS lixcol_commit_id, \
                   commit_created_at AS lixcol_commit_created_at, \
                   root_commit_id AS lixcol_root_commit_id, \
                   depth AS lixcol_depth \
                FROM state_history_source \
                WHERE schema_key = 'lix_directory_descriptor' \
                  AND snapshot_content IS NOT NULL \
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
               d.lixcol_commit_created_at, \
               d.lixcol_root_commit_id, \
               d.lixcol_depth \
            FROM directory_history_base d \
            LEFT JOIN directory_history_paths dp \
              ON dp.target_id = d.id \
             AND dp.root_commit_id = d.lixcol_root_commit_id \
             AND dp.target_depth = d.lixcol_depth",
        state_history_source_sql = state_history_source_sql,
    )
}

fn target_versions_cte_sql(scope: FilesystemProjectionScope, schema_keys: &[&str]) -> String {
    match scope {
        FilesystemProjectionScope::ActiveVersion => format!(
            "target_versions AS ( \
               SELECT DISTINCT \
                 lix_json_extract(snapshot_content, 'version_id') AS version_id \
               FROM lix_internal_state_untracked \
               WHERE schema_key = '{schema_key}' \
                 AND file_id = '{file_id}' \
                 AND version_id = '{storage_version_id}' \
                 AND snapshot_content IS NOT NULL \
             )",
            schema_key = escape_sql_string(active_version_schema_key()),
            file_id = escape_sql_string(active_version_file_id()),
            storage_version_id = escape_sql_string(active_version_storage_version_id()),
        ),
        FilesystemProjectionScope::ExplicitVersion => {
            let union_rows = schema_keys
                .iter()
                .flat_map(|schema_key| {
                    let quoted =
                        quote_ident(&format!("lix_internal_state_materialized_v1_{schema_key}"));
                    [
                        format!(
                            "SELECT DISTINCT version_id \
                             FROM {quoted} \
                             WHERE version_id <> '{global_version}'",
                            quoted = quoted,
                            global_version = escape_sql_string(GLOBAL_VERSION_ID),
                        ),
                        format!(
                            "SELECT DISTINCT version_id \
                             FROM lix_internal_state_untracked \
                             WHERE schema_key = '{schema_key}' \
                               AND version_id <> '{global_version}'",
                            schema_key = escape_sql_string(schema_key),
                            global_version = escape_sql_string(GLOBAL_VERSION_ID),
                        ),
                    ]
                })
                .collect::<Vec<_>>();
            let union_sql = if union_rows.is_empty() {
                String::new()
            } else {
                format!(" UNION {}", union_rows.join(" UNION "))
            };
            format!(
                "all_target_versions AS ( \
                   SELECT '{global_version}' AS version_id \
                   UNION \
                   SELECT DISTINCT entity_id AS version_id \
                   FROM lix_internal_state_materialized_v1_lix_version_descriptor \
                   WHERE schema_key = '{version_descriptor_schema_key}' \
                     AND version_id = '{global_version}' \
                     AND is_tombstone = 0 \
                     AND snapshot_content IS NOT NULL{union_sql} \
                 ), \
                 target_versions AS ( \
                   SELECT version_id \
                   FROM all_target_versions \
                 )",
                version_descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
                global_version = escape_sql_string(GLOBAL_VERSION_ID),
                union_sql = union_sql,
            )
        }
    }
}

fn effective_state_candidates_sql(schema_key: &str) -> String {
    let table_name = quote_ident(&format!("lix_internal_state_materialized_v1_{schema_key}"));
    let schema_filter = format!("u.schema_key = '{}'", escape_sql_string(schema_key));
    format!(
        "SELECT \
           t.entity_id AS entity_id, \
           t.schema_key AS schema_key, \
           t.file_id AS file_id, \
           tv.version_id AS version_id, \
           t.plugin_key AS plugin_key, \
           t.snapshot_content AS snapshot_content, \
           t.schema_version AS schema_version, \
           t.created_at AS created_at, \
           t.updated_at AS updated_at, \
           CASE WHEN tv.version_id = '{global_version}' THEN true ELSE false END AS global, \
           t.change_id AS change_id, \
           cc.commit_id AS commit_id, \
           false AS untracked, \
           t.writer_key AS writer_key, \
           t.metadata AS metadata, \
           2 AS precedence \
         FROM {table_name} t \
         JOIN target_versions tv \
           ON tv.version_id = t.version_id \
         LEFT JOIN change_commit_by_change_id cc \
           ON cc.change_id = t.change_id \
         WHERE 1 = 1 \
         UNION ALL \
         SELECT \
           t.entity_id AS entity_id, \
           t.schema_key AS schema_key, \
           t.file_id AS file_id, \
           tv.version_id AS version_id, \
           t.plugin_key AS plugin_key, \
           t.snapshot_content AS snapshot_content, \
           t.schema_version AS schema_version, \
           t.created_at AS created_at, \
           t.updated_at AS updated_at, \
           true AS global, \
           t.change_id AS change_id, \
           cc.commit_id AS commit_id, \
           false AS untracked, \
           t.writer_key AS writer_key, \
           t.metadata AS metadata, \
           4 AS precedence \
         FROM {table_name} t \
         JOIN target_versions tv \
           ON tv.version_id <> '{global_version}' \
          AND t.version_id = '{global_version}' \
         LEFT JOIN change_commit_by_change_id cc \
           ON cc.change_id = t.change_id \
         WHERE t.version_id = '{global_version}' \
         UNION ALL \
         SELECT \
           u.entity_id AS entity_id, \
           u.schema_key AS schema_key, \
           u.file_id AS file_id, \
           tv.version_id AS version_id, \
           u.plugin_key AS plugin_key, \
           u.snapshot_content AS snapshot_content, \
           u.schema_version AS schema_version, \
           u.created_at AS created_at, \
           u.updated_at AS updated_at, \
           CASE WHEN tv.version_id = '{global_version}' THEN true ELSE false END AS global, \
           NULL AS change_id, \
           'untracked' AS commit_id, \
           true AS untracked, \
           u.writer_key AS writer_key, \
           u.metadata AS metadata, \
           1 AS precedence \
         FROM lix_internal_state_untracked u \
         JOIN target_versions tv \
           ON tv.version_id = u.version_id \
         WHERE {schema_filter} \
         UNION ALL \
         SELECT \
           u.entity_id AS entity_id, \
           u.schema_key AS schema_key, \
           u.file_id AS file_id, \
           tv.version_id AS version_id, \
           u.plugin_key AS plugin_key, \
           u.snapshot_content AS snapshot_content, \
           u.schema_version AS schema_version, \
           u.created_at AS created_at, \
           u.updated_at AS updated_at, \
           true AS global, \
           NULL AS change_id, \
           'untracked' AS commit_id, \
           true AS untracked, \
           u.writer_key AS writer_key, \
           u.metadata AS metadata, \
           3 AS precedence \
         FROM lix_internal_state_untracked u \
         JOIN target_versions tv \
           ON tv.version_id <> '{global_version}' \
          AND u.version_id = '{global_version}' \
         WHERE {schema_filter} \
           AND u.version_id = '{global_version}'",
        table_name = table_name,
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        schema_filter = schema_filter,
    )
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn active_version_commit_id_sql() -> String {
    format!(
        "(\
         SELECT lix_json_extract(vp.snapshot_content, 'commit_id') \
         FROM lix_internal_state_materialized_v1_lix_version_pointer vp \
         WHERE vp.schema_key = 'lix_version_pointer' \
           AND vp.version_id = '{global_version}' \
           AND vp.snapshot_content IS NOT NULL \
           AND vp.entity_id = (\
               SELECT lix_json_extract(snapshot_content, 'version_id') \
               FROM lix_internal_state_untracked \
               WHERE schema_key = '{schema_key}' \
                 AND file_id = '{file_id}' \
                 AND version_id = '{storage_version_id}' \
                 AND snapshot_content IS NOT NULL \
               LIMIT 1\
           ) \
         LIMIT 1\
        )",
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        schema_key = escape_sql_string(active_version_schema_key()),
        file_id = escape_sql_string(active_version_file_id()),
        storage_version_id = escape_sql_string(active_version_storage_version_id()),
    )
}

pub(crate) fn build_filesystem_state_history_source_sql(
    requested_roots_where: &str,
    requested_versions_where: &str,
    default_root_scope: &str,
    force_active_scope: bool,
) -> String {
    let active_version_rows_sql = if force_active_scope {
        format!(
            "active_version_rows AS ( \
               SELECT DISTINCT \
                 lix_json_extract(snapshot_content, 'version_id') AS version_id \
               FROM lix_internal_state_untracked \
               WHERE schema_key = '{active_schema_key}' \
                 AND file_id = '{active_file_id}' \
                 AND version_id = '{active_storage_version_id}' \
                 AND snapshot_content IS NOT NULL \
             ), ",
            active_schema_key = escape_sql_string(active_version_schema_key()),
            active_file_id = escape_sql_string(active_version_file_id()),
            active_storage_version_id = escape_sql_string(active_version_storage_version_id()),
        )
    } else {
        String::new()
    };
    let default_root_commits_sql = if force_active_scope {
        format!(
            "default_root_commits AS ( \
           SELECT DISTINCT \
             lix_json_extract(vp.snapshot_content, 'commit_id') AS root_commit_id, \
             vp.entity_id AS root_version_id \
           FROM lix_internal_state_materialized_v1_lix_version_pointer vp \
           JOIN active_version_rows av \
             ON av.version_id = vp.entity_id \
           WHERE vp.schema_key = 'lix_version_pointer' \
             AND vp.version_id = '{global_version}' \
             AND vp.is_tombstone = 0 \
             AND vp.snapshot_content IS NOT NULL \
           UNION \
           SELECT DISTINCT \
             lix_json_extract(vd.snapshot_content, 'commit_id') AS root_commit_id, \
             vd.entity_id AS root_version_id \
           FROM lix_internal_state_materialized_v1_lix_version_descriptor vd \
           JOIN active_version_rows av \
             ON av.version_id = vd.entity_id \
           WHERE vd.schema_key = '{version_descriptor_schema_key}' \
             AND vd.version_id = '{global_version}' \
             AND vd.is_tombstone = 0 \
             AND vd.snapshot_content IS NOT NULL \
         ), ",
            version_descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
            global_version = escape_sql_string(GLOBAL_VERSION_ID),
        )
    } else {
        format!(
            "default_root_commits AS ( \
           SELECT DISTINCT \
             lix_json_extract(vp.snapshot_content, 'commit_id') AS root_commit_id, \
             vp.entity_id AS root_version_id \
           FROM lix_internal_state_materialized_v1_lix_version_pointer vp \
           WHERE vp.schema_key = 'lix_version_pointer' \
             AND vp.version_id = '{global_version}' \
             AND vp.is_tombstone = 0 \
             AND vp.snapshot_content IS NOT NULL \
           UNION \
           SELECT DISTINCT \
             lix_json_extract(vd.snapshot_content, 'commit_id') AS root_commit_id, \
             vd.entity_id AS root_version_id \
           FROM lix_internal_state_materialized_v1_lix_version_descriptor vd \
           WHERE vd.schema_key = '{version_descriptor_schema_key}' \
             AND vd.version_id = '{global_version}' \
             AND vd.is_tombstone = 0 \
             AND vd.snapshot_content IS NOT NULL \
         ), ",
            version_descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
            global_version = escape_sql_string(GLOBAL_VERSION_ID),
        )
    };
    format!(
        "WITH \
           {active_version_rows_sql}\
           {default_root_commits_sql}\
           requested_commits AS ( \
             SELECT DISTINCT \
               c.entity_id AS commit_id, \
               COALESCE(d.root_version_id, c.version_id) AS root_version_id \
             FROM lix_internal_state_materialized_v1_lix_commit c \
             LEFT JOIN default_root_commits d \
               ON d.root_commit_id = c.entity_id \
             WHERE c.schema_key = 'lix_commit' \
               AND c.version_id = '{global_version}' \
               AND c.is_tombstone = 0 \
               AND c.snapshot_content IS NOT NULL{requested_roots_where}{requested_versions_where} \
               {default_root_scope} \
           ), \
           reachable_commits_from_requested AS ( \
             SELECT \
               ancestry.ancestor_id AS commit_id, \
               requested.commit_id AS root_commit_id, \
               requested.root_version_id AS root_version_id, \
               ancestry.depth AS commit_depth \
             FROM requested_commits requested \
             JOIN lix_internal_commit_ancestry ancestry \
               ON ancestry.commit_id = requested.commit_id \
             WHERE ancestry.depth <= 512 \
           ), \
           commit_changesets AS ( \
             SELECT \
               c.entity_id AS commit_id, \
               lix_json_extract(c.snapshot_content, 'change_set_id') AS change_set_id, \
               c.created_at AS commit_created_at, \
               rc.root_commit_id AS root_commit_id, \
               rc.root_version_id AS root_version_id, \
               rc.commit_depth AS commit_depth \
             FROM lix_internal_state_materialized_v1_lix_commit c \
             JOIN reachable_commits_from_requested rc \
               ON rc.commit_id = c.entity_id \
             WHERE c.schema_key = 'lix_commit' \
               AND c.version_id = '{global_version}' \
               AND c.is_tombstone = 0 \
               AND c.snapshot_content IS NOT NULL \
           ), \
           cse_in_reachable_commits AS ( \
             SELECT \
               lix_json_extract(cse.snapshot_content, 'entity_id') AS target_entity_id, \
               lix_json_extract(cse.snapshot_content, 'file_id') AS target_file_id, \
               lix_json_extract(cse.snapshot_content, 'schema_key') AS target_schema_key, \
               lix_json_extract(cse.snapshot_content, 'change_id') AS target_change_id, \
               cc.commit_id AS origin_commit_id, \
               cc.commit_created_at AS commit_created_at, \
               cc.root_commit_id AS root_commit_id, \
               cc.root_version_id AS root_version_id, \
               cc.commit_depth AS commit_depth \
             FROM lix_internal_state_materialized_v1_lix_change_set_element cse \
             JOIN commit_changesets cc \
               ON lix_json_extract(cse.snapshot_content, 'change_set_id') = cc.change_set_id \
             WHERE cse.schema_key = 'lix_change_set_element' \
               AND cse.version_id = '{global_version}' \
               AND cse.is_tombstone = 0 \
               AND cse.snapshot_content IS NOT NULL \
           ), \
           ranked AS ( \
             SELECT \
               ch.entity_id AS entity_id, \
               ch.schema_key AS schema_key, \
               ch.file_id AS file_id, \
               ch.plugin_key AS plugin_key, \
               CASE \
                 WHEN ch.snapshot_id = 'no-content' THEN NULL \
                 ELSE s.content \
               END AS snapshot_content, \
               ch.metadata AS metadata, \
               ch.schema_version AS schema_version, \
               r.target_change_id AS change_id, \
               r.origin_commit_id AS commit_id, \
               r.commit_created_at AS commit_created_at, \
               r.root_commit_id AS root_commit_id, \
               r.root_version_id AS version_id, \
               r.commit_depth AS depth, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY \
                   r.target_entity_id, \
                   r.target_file_id, \
                   r.target_schema_key, \
                   r.root_commit_id, \
                   r.commit_depth \
                 ORDER BY ch.created_at DESC, ch.id DESC \
               ) AS rn \
             FROM cse_in_reachable_commits r \
             JOIN lix_internal_change ch \
               ON ch.id = r.target_change_id \
             LEFT JOIN lix_internal_snapshot s \
               ON s.id = ch.snapshot_id \
           ) \
         SELECT \
           ranked.entity_id AS entity_id, \
           ranked.schema_key AS schema_key, \
           ranked.file_id AS file_id, \
           ranked.plugin_key AS plugin_key, \
           ranked.snapshot_content AS snapshot_content, \
           ranked.metadata AS metadata, \
           ranked.schema_version AS schema_version, \
           ranked.change_id AS change_id, \
           ranked.commit_id AS commit_id, \
           ranked.commit_created_at AS commit_created_at, \
           ranked.root_commit_id AS root_commit_id, \
           ranked.depth AS depth, \
           ranked.version_id AS version_id \
         FROM ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL",
        active_version_rows_sql = active_version_rows_sql,
        default_root_commits_sql = default_root_commits_sql,
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        requested_roots_where = requested_roots_where,
        requested_versions_where = requested_versions_where,
        default_root_scope = default_root_scope,
    )
}
