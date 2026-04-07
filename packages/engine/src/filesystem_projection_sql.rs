use crate::schema::annotations::writer_key::WORKSPACE_WRITER_KEY_TABLE;
use crate::binary_cas::schema::INTERNAL_BINARY_BLOB_STORE;
use crate::contracts::artifacts::FilesystemProjectionScope;
use crate::schema::access::{payload_column_name_for_schema, tracked_relation_name};
use crate::common::text::escape_sql_string;
use crate::version_artifacts::{version_descriptor_schema_key, GLOBAL_VERSION_ID};
use crate::{LixError, SqlDialect};

pub(crate) const LIVE_FILE_PREFETCH_BLOB_HASH_COLUMN: &str = "__lix_blob_hash";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

pub(crate) fn build_filesystem_file_projection_sql(
    scope: FilesystemProjectionScope,
    active_version_id: Option<&str>,
    include_blob_hash: bool,
    dialect: SqlDialect,
) -> Result<String, LixError> {
    let commit_change_set_id_column =
        quote_ident(&live_payload_column_name("lix_commit", "change_set_id"));
    let cse_change_set_id_column = quote_ident(&live_payload_column_name(
        "lix_change_set_element",
        "change_set_id",
    ));
    let cse_change_id_column = quote_ident(&live_payload_column_name(
        "lix_change_set_element",
        "change_id",
    ));
    let commit_id_projection = match scope {
        FilesystemProjectionScope::ActiveVersion => {
            active_version_commit_id_sql(required_active_version_id(scope, active_version_id)?)?
        }
        FilesystemProjectionScope::ExplicitVersion => "f.lixcol_commit_id".to_string(),
    };
    let blob_hash_projection = if include_blob_hash {
        format!(", bfr.blob_hash AS {LIVE_FILE_PREFETCH_BLOB_HASH_COLUMN}")
    } else {
        String::new()
    };
    let live_commit_table = tracked_relation_name("lix_commit");
    let live_cse_table = tracked_relation_name("lix_change_set_element");

    Ok(format!(
        "WITH RECURSIVE \
           {target_versions_cte}, \
           commit_by_version AS ( \
             SELECT \
               entity_id AS commit_id, \
               {commit_change_set_id_column} AS change_set_id \
             FROM {live_commit_table} \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
           ), \
           change_set_element_by_version AS ( \
             SELECT \
               {cse_change_set_id_column} AS change_set_id, \
               {cse_change_id_column} AS change_id \
             FROM {live_cse_table} \
             WHERE schema_key = 'lix_change_set_element' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
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
               c.payload_id AS payload_id, \
               c.payload_parent_id AS payload_parent_id, \
               c.payload_name AS payload_name, \
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
               payload_id AS id, \
               payload_parent_id AS parent_id, \
               payload_name AS name, \
               version_id AS lixcol_version_id \
             FROM directory_descriptor_ranked \
             WHERE rn = 1 \
               AND payload_id IS NOT NULL \
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
               c.payload_id AS payload_id, \
               c.payload_directory_id AS payload_directory_id, \
               c.payload_name AS payload_name, \
               c.payload_extension AS payload_extension, \
               c.payload_metadata AS payload_metadata, \
               c.payload_hidden AS payload_hidden, \
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
               payload_id AS id, \
               payload_directory_id AS directory_id, \
               payload_name AS name, \
               payload_extension AS extension, \
               payload_metadata AS metadata, \
               payload_hidden AS hidden, \
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
               AND payload_id IS NOT NULL \
           ), \
           binary_blob_ref_candidates AS ( \
             {blob_candidates_sql} \
           ), \
           binary_blob_ref_ranked AS ( \
             SELECT \
               c.entity_id AS entity_id, \
               c.version_id AS version_id, \
               c.payload_id AS payload_id, \
               c.payload_blob_hash AS payload_blob_hash, \
               c.payload_size_bytes AS payload_size_bytes, \
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
               payload_id AS id, \
               version_id AS lixcol_version_id, \
               payload_blob_hash AS blob_hash, \
               payload_size_bytes AS size_bytes \
             FROM binary_blob_ref_ranked \
             WHERE rn = 1 \
               AND payload_id IS NOT NULL \
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
        LEFT JOIN {binary_blob_store} bbs \
           ON bbs.blob_hash = bfr.blob_hash",
        target_versions_cte = target_versions_cte_sql(
            scope,
            active_version_id,
            &[
                FILE_DESCRIPTOR_SCHEMA_KEY,
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                BINARY_BLOB_REF_SCHEMA_KEY
            ]
        )?,
        directory_candidates_sql = effective_directory_descriptor_candidates_sql(),
        file_candidates_sql = effective_file_descriptor_candidates_sql(dialect),
        blob_candidates_sql = effective_binary_blob_ref_candidates_sql(),
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        blob_hash_projection = blob_hash_projection,
        binary_blob_store = INTERNAL_BINARY_BLOB_STORE,
        commit_id_projection = commit_id_projection,
        commit_change_set_id_column = commit_change_set_id_column,
        cse_change_set_id_column = cse_change_set_id_column,
        cse_change_id_column = cse_change_id_column,
    ))
}

pub(crate) fn build_filesystem_directory_projection_sql(
    scope: FilesystemProjectionScope,
    active_version_id: Option<&str>,
    _dialect: SqlDialect,
) -> Result<String, LixError> {
    let commit_change_set_id_column =
        quote_ident(&live_payload_column_name("lix_commit", "change_set_id"));
    let cse_change_set_id_column = quote_ident(&live_payload_column_name(
        "lix_change_set_element",
        "change_set_id",
    ));
    let cse_change_id_column = quote_ident(&live_payload_column_name(
        "lix_change_set_element",
        "change_id",
    ));
    let commit_id_projection = match scope {
        FilesystemProjectionScope::ActiveVersion => {
            active_version_commit_id_sql(required_active_version_id(scope, active_version_id)?)?
        }
        FilesystemProjectionScope::ExplicitVersion => "d.lixcol_commit_id".to_string(),
    };
    let live_commit_table = tracked_relation_name("lix_commit");
    let live_cse_table = tracked_relation_name("lix_change_set_element");
    Ok(format!(
        "WITH RECURSIVE \
           {target_versions_cte}, \
           commit_by_version AS ( \
             SELECT \
               entity_id AS commit_id, \
               {commit_change_set_id_column} AS change_set_id \
             FROM {live_commit_table} \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
           ), \
           change_set_element_by_version AS ( \
             SELECT \
               {cse_change_set_id_column} AS change_set_id, \
               {cse_change_id_column} AS change_id \
             FROM {live_cse_table} \
             WHERE schema_key = 'lix_change_set_element' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
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
               c.payload_id AS payload_id, \
               c.payload_parent_id AS payload_parent_id, \
               c.payload_name AS payload_name, \
               c.payload_hidden AS payload_hidden, \
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
               payload_id AS id, \
               payload_parent_id AS parent_id, \
               payload_name AS name, \
               payload_hidden AS hidden, \
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
               AND payload_id IS NOT NULL \
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
        target_versions_cte =
            target_versions_cte_sql(scope, active_version_id, &[DIRECTORY_DESCRIPTOR_SCHEMA_KEY])?,
        directory_candidates_sql = effective_directory_descriptor_candidates_sql(),
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        commit_id_projection = commit_id_projection,
        commit_change_set_id_column = commit_change_set_id_column,
        cse_change_set_id_column = cse_change_set_id_column,
        cse_change_id_column = cse_change_id_column,
    ))
}

fn target_versions_cte_sql(
    scope: FilesystemProjectionScope,
    active_version_id: Option<&str>,
    schema_keys: &[&str],
) -> Result<String, LixError> {
    match scope {
        FilesystemProjectionScope::ActiveVersion => Ok(format!(
            "target_versions AS ( \
               SELECT '{active_version_id}' AS version_id \
             )",
            active_version_id =
                escape_sql_string(required_active_version_id(scope, active_version_id)?),
        )),
        FilesystemProjectionScope::ExplicitVersion => {
            let union_rows = schema_keys
                .iter()
                .flat_map(|schema_key| {
                    let quoted = quote_ident(&tracked_relation_name(schema_key));
                    [
                        format!(
                            "SELECT DISTINCT version_id \
                             FROM {quoted} \
                             WHERE version_id <> '{global_version}' \
                               AND untracked = false",
                            quoted = quoted,
                            global_version = escape_sql_string(GLOBAL_VERSION_ID),
                        ),
                        format!(
                            "SELECT DISTINCT version_id \
                             FROM {untracked_table} \
                    WHERE version_id <> '{global_version}' \
                      AND untracked = true",
                            untracked_table = quote_ident(&tracked_relation_name(schema_key)),
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
            let live_version_descriptor_table = tracked_relation_name("lix_version_descriptor");
            Ok(format!(
                "all_target_versions AS ( \
                   SELECT '{global_version}' AS version_id \
                   UNION \
                   SELECT DISTINCT entity_id AS version_id \
                   FROM {live_version_descriptor_table} \
                   WHERE schema_key = '{version_descriptor_schema_key}' \
                     AND version_id = '{global_version}' \
                     AND is_tombstone = 0{union_sql} \
                 ), \
                 target_versions AS ( \
                   SELECT version_id \
                   FROM all_target_versions \
                 )",
                live_version_descriptor_table = live_version_descriptor_table,
                version_descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
                global_version = escape_sql_string(GLOBAL_VERSION_ID),
                union_sql = union_sql,
            ))
        }
    }
}

fn effective_state_candidates_sql(
    schema_key: &str,
    payload_columns: &[(&str, String, String)],
) -> String {
    let table_name = quote_ident(&tracked_relation_name(schema_key));
    let untracked_table = quote_ident(&tracked_relation_name(schema_key));
    let workspace_writer_key_table = quote_ident(WORKSPACE_WRITER_KEY_TABLE);
    let tracked_payload_projection = payload_columns
        .iter()
        .map(|(alias, tracked_expr, _)| format!("{tracked_expr} AS {alias}"))
        .collect::<Vec<_>>()
        .join(", ");
    let untracked_payload_projection = payload_columns
        .iter()
        .map(|(alias, _, untracked_expr)| format!("{untracked_expr} AS {alias}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT \
           t.entity_id AS entity_id, \
           t.schema_key AS schema_key, \
           t.file_id AS file_id, \
           tv.version_id AS version_id, \
           t.plugin_key AS plugin_key, \
           {tracked_payload_projection}, \
           t.schema_version AS schema_version, \
           t.created_at AS created_at, \
           t.updated_at AS updated_at, \
           CASE WHEN tv.version_id = '{global_version}' THEN true ELSE false END AS global, \
           t.change_id AS change_id, \
           cc.commit_id AS commit_id, \
           false AS untracked, \
           wk.writer_key AS writer_key, \
           t.metadata AS metadata, \
           2 AS precedence \
         FROM {table_name} t \
         JOIN target_versions tv \
           ON tv.version_id = t.version_id \
         LEFT JOIN change_commit_by_change_id cc \
           ON cc.change_id = t.change_id \
         LEFT JOIN {workspace_writer_key_table} wk \
           ON wk.version_id = t.version_id \
          AND wk.schema_key = t.schema_key \
          AND wk.entity_id = t.entity_id \
          AND wk.file_id = t.file_id \
         WHERE t.untracked = false \
         UNION ALL \
         SELECT \
           t.entity_id AS entity_id, \
           t.schema_key AS schema_key, \
           t.file_id AS file_id, \
           tv.version_id AS version_id, \
           t.plugin_key AS plugin_key, \
           {tracked_payload_projection}, \
           t.schema_version AS schema_version, \
           t.created_at AS created_at, \
           t.updated_at AS updated_at, \
           true AS global, \
           t.change_id AS change_id, \
           cc.commit_id AS commit_id, \
           false AS untracked, \
           gwk.writer_key AS writer_key, \
           t.metadata AS metadata, \
           4 AS precedence \
         FROM {table_name} t \
         JOIN target_versions tv \
           ON tv.version_id <> '{global_version}' \
          AND t.version_id = '{global_version}' \
         LEFT JOIN change_commit_by_change_id cc \
           ON cc.change_id = t.change_id \
         LEFT JOIN {workspace_writer_key_table} gwk \
           ON gwk.version_id = t.version_id \
          AND gwk.schema_key = t.schema_key \
          AND gwk.entity_id = t.entity_id \
          AND gwk.file_id = t.file_id \
         WHERE t.version_id = '{global_version}' \
           AND t.untracked = false \
         UNION ALL \
         SELECT \
           u.entity_id AS entity_id, \
           u.schema_key AS schema_key, \
           u.file_id AS file_id, \
           tv.version_id AS version_id, \
           u.plugin_key AS plugin_key, \
           {untracked_payload_projection}, \
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
         FROM {untracked_table} u \
         JOIN target_versions tv \
           ON tv.version_id = u.version_id \
         WHERE u.untracked = true \
         UNION ALL \
         SELECT \
           u.entity_id AS entity_id, \
           u.schema_key AS schema_key, \
           u.file_id AS file_id, \
           tv.version_id AS version_id, \
           u.plugin_key AS plugin_key, \
           {untracked_payload_projection}, \
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
         FROM {untracked_table} u \
         JOIN target_versions tv \
           ON tv.version_id <> '{global_version}' \
          AND u.version_id = '{global_version}' \
         WHERE u.version_id = '{global_version}' \
           AND u.untracked = true",
        table_name = table_name,
        untracked_table = untracked_table,
        tracked_payload_projection = tracked_payload_projection,
        untracked_payload_projection = untracked_payload_projection,
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        workspace_writer_key_table = workspace_writer_key_table,
    )
}

fn effective_directory_descriptor_candidates_sql() -> String {
    let id_column = live_payload_column_name(DIRECTORY_DESCRIPTOR_SCHEMA_KEY, "id");
    let parent_id_column = live_payload_column_name(DIRECTORY_DESCRIPTOR_SCHEMA_KEY, "parent_id");
    let name_column = live_payload_column_name(DIRECTORY_DESCRIPTOR_SCHEMA_KEY, "name");
    let hidden_column = live_payload_column_name(DIRECTORY_DESCRIPTOR_SCHEMA_KEY, "hidden");
    effective_state_candidates_sql(
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
        &[
            (
                "payload_id",
                format!("t.{}", quote_ident(&id_column)),
                format!("u.{}", quote_ident(&id_column)),
            ),
            (
                "payload_parent_id",
                format!("t.{}", quote_ident(&parent_id_column)),
                format!("u.{}", quote_ident(&parent_id_column)),
            ),
            (
                "payload_name",
                format!("t.{}", quote_ident(&name_column)),
                format!("u.{}", quote_ident(&name_column)),
            ),
            (
                "payload_hidden",
                format!("COALESCE(t.{}, false)", quote_ident(&hidden_column)),
                format!("COALESCE(u.{}, false)", quote_ident(&hidden_column)),
            ),
        ],
    )
}

fn effective_file_descriptor_candidates_sql(dialect: SqlDialect) -> String {
    let id_column = live_payload_column_name(FILE_DESCRIPTOR_SCHEMA_KEY, "id");
    let directory_id_column = live_payload_column_name(FILE_DESCRIPTOR_SCHEMA_KEY, "directory_id");
    let name_column = live_payload_column_name(FILE_DESCRIPTOR_SCHEMA_KEY, "name");
    let extension_column = live_payload_column_name(FILE_DESCRIPTOR_SCHEMA_KEY, "extension");
    let metadata_column = live_payload_column_name(FILE_DESCRIPTOR_SCHEMA_KEY, "metadata");
    let hidden_column = live_payload_column_name(FILE_DESCRIPTOR_SCHEMA_KEY, "hidden");
    effective_state_candidates_sql(
        FILE_DESCRIPTOR_SCHEMA_KEY,
        &[
            (
                "payload_id",
                format!("t.{}", quote_ident(&id_column)),
                format!("u.{}", quote_ident(&id_column)),
            ),
            (
                "payload_directory_id",
                format!("t.{}", quote_ident(&directory_id_column)),
                format!("u.{}", quote_ident(&directory_id_column)),
            ),
            (
                "payload_name",
                format!("t.{}", quote_ident(&name_column)),
                format!("u.{}", quote_ident(&name_column)),
            ),
            (
                "payload_extension",
                format!("t.{}", quote_ident(&extension_column)),
                format!("u.{}", quote_ident(&extension_column)),
            ),
            (
                "payload_metadata",
                normalized_json_text_projection(
                    dialect,
                    &qualified_column_ref("t", &metadata_column),
                ),
                normalized_json_text_projection(
                    dialect,
                    &qualified_column_ref("u", &metadata_column),
                ),
            ),
            (
                "payload_hidden",
                format!("COALESCE(t.{}, false)", quote_ident(&hidden_column)),
                format!("COALESCE(u.{}, false)", quote_ident(&hidden_column)),
            ),
        ],
    )
}

fn effective_binary_blob_ref_candidates_sql() -> String {
    let id_column = live_payload_column_name(BINARY_BLOB_REF_SCHEMA_KEY, "id");
    let blob_hash_column = live_payload_column_name(BINARY_BLOB_REF_SCHEMA_KEY, "blob_hash");
    let size_bytes_column = live_payload_column_name(BINARY_BLOB_REF_SCHEMA_KEY, "size_bytes");
    effective_state_candidates_sql(
        BINARY_BLOB_REF_SCHEMA_KEY,
        &[
            (
                "payload_id",
                format!("t.{}", quote_ident(&id_column)),
                format!("u.{}", quote_ident(&id_column)),
            ),
            (
                "payload_blob_hash",
                format!("t.{}", quote_ident(&blob_hash_column)),
                format!("u.{}", quote_ident(&blob_hash_column)),
            ),
            (
                "payload_size_bytes",
                format!("t.{}", quote_ident(&size_bytes_column)),
                format!("u.{}", quote_ident(&size_bytes_column)),
            ),
        ],
    )
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn active_version_commit_id_sql(active_version_id: &str) -> Result<String, LixError> {
    // Filesystem current-version projection consults the replica-local version
    // head row for the active version.
    let version_ref_commit_id_column =
        quote_ident(&live_payload_column_name("lix_version_ref", "commit_id"));
    let live_version_ref_table = tracked_relation_name("lix_version_ref");
    Ok(format!(
        "(\
         SELECT {version_ref_commit_id_column} \
         FROM {live_version_ref_table} vp \
         WHERE vp.schema_key = 'lix_version_ref' \
           AND vp.version_id = '{global_version}' \
           AND vp.untracked = true \
           AND {version_ref_commit_id_column} IS NOT NULL \
           AND vp.entity_id = '{active_version_id}' \
         LIMIT 1\
        )",
        version_ref_commit_id_column = version_ref_commit_id_column,
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        active_version_id = escape_sql_string(active_version_id),
    ))
}

fn required_active_version_id(
    scope: FilesystemProjectionScope,
    active_version_id: Option<&str>,
) -> Result<&str, LixError> {
    match (scope, active_version_id) {
        (FilesystemProjectionScope::ActiveVersion, Some(active_version_id)) => {
            Ok(active_version_id)
        }
        (FilesystemProjectionScope::ActiveVersion, None) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "filesystem active-version projection requires a session-requested version id",
        )),
        (FilesystemProjectionScope::ExplicitVersion, _) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "explicit filesystem projections must not request an active version id",
        )),
    }
}

fn live_payload_column_name(schema_key: &str, property_name: &str) -> String {
    payload_column_name_for_schema(schema_key, None, property_name).unwrap_or_else(|error| {
        panic!(
            "builtin live schema '{schema_key}' must include '{property_name}': {}",
            error.description
        )
    })
}

fn qualified_column_ref(table_alias: &str, column_name: &str) -> String {
    format!("{}.{}", quote_ident(table_alias), quote_ident(column_name))
}

fn normalized_json_text_projection(dialect: SqlDialect, column_ref: &str) -> String {
    match dialect {
        SqlDialect::Sqlite => format!(
            "CASE \
               WHEN {column_ref} IS NULL THEN NULL \
               ELSE json_extract({column_ref}, '$') || '' \
             END",
        ),
        SqlDialect::Postgres => format!(
            "CASE \
               WHEN {column_ref} IS NULL THEN NULL \
               ELSE (CAST({column_ref} AS JSONB) #>> '{{}}') \
             END",
        ),
    }
}
