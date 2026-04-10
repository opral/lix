use std::collections::BTreeMap;

use crate::binary_cas::INTERNAL_BINARY_BLOB_STORE;
use crate::catalog::{
    FilesystemProjectionScope, FilesystemRelationBinding, FilesystemRelationKind,
    StoredVersionHeadSourceBinding, VersionHeadSourceBinding, VersionRelationBinding,
};
use crate::common::text::escape_sql_string;
use crate::live_state::tracked_relation_name;
use crate::live_state::{payload_column_name_for_schema, WRITER_KEY_TABLE};
use crate::sql::physical_plan::source_sql::build_lazy_change_commit_by_change_id_ctes_sql;
use crate::{LixError, SqlDialect};

pub(crate) fn build_version_relation_sql(
    dialect: SqlDialect,
    binding: &VersionRelationBinding,
) -> String {
    let current_refs_cte_sql = build_current_version_refs_unique_cte_sql(&binding.head_source);
    let name_expr = json_text_extract_sql(dialect, "d.snapshot_content", "name");
    let hidden_expr = json_boolean_extract_sql(dialect, "d.snapshot_content", "hidden");
    let (parent_join_sql, parent_value_expr) = json_array_text_join_sql(
        dialect,
        "commit_headers.commit_snapshot_content",
        "parent_commit_ids",
        "parent_rows",
        "parent_commit_id",
    );
    let (change_join_sql, change_value_expr, change_position_expr) =
        json_array_text_join_with_position_sql(
            dialect,
            "commit_headers.commit_snapshot_content",
            "change_ids",
            "change_rows",
            "change_id",
            "change_position",
        );
    format!(
        "WITH RECURSIVE \
         {current_refs_cte_sql}\
         global_head AS ( \
             SELECT commit_id \
             FROM current_refs \
             WHERE version_id = '{global_version}' \
         ), \
         descriptor_seed_commits AS ( \
             SELECT commit_id \
             FROM global_head \
             UNION \
             SELECT commit_id \
             FROM canonical_commit_headers \
             WHERE NOT EXISTS (SELECT 1 FROM global_head) \
         ), \
         reachable_global_commit_walk AS ( \
             SELECT commit_id, 0 AS depth \
             FROM descriptor_seed_commits \
             UNION ALL \
             SELECT \
               {parent_value_expr} AS commit_id, \
               walk.depth + 1 AS depth \
             FROM reachable_global_commit_walk walk \
             JOIN canonical_commit_headers commit_headers \
               ON commit_headers.commit_id = walk.commit_id \
             {parent_join_sql} \
             WHERE {parent_value_expr} IS NOT NULL \
         ), \
         reachable_global_commits AS ( \
             SELECT commit_id, MIN(depth) AS depth \
             FROM reachable_global_commit_walk \
             GROUP BY commit_id \
         ), \
         descriptor_members AS ( \
             SELECT \
               descriptor_change.entity_id AS entity_id, \
               descriptor_change.id AS change_id, \
               descriptor_snapshot.content AS snapshot_content, \
               reachable.depth AS depth, \
               {change_position_expr} AS change_position \
             FROM reachable_global_commits reachable \
             JOIN canonical_commit_headers commit_headers \
               ON commit_headers.commit_id = reachable.commit_id \
             {change_join_sql} \
             JOIN lix_internal_change descriptor_change \
               ON descriptor_change.id = {change_value_expr} \
             LEFT JOIN lix_internal_snapshot descriptor_snapshot \
               ON descriptor_snapshot.id = descriptor_change.snapshot_id \
             WHERE descriptor_change.schema_key = '{descriptor_schema_key}' \
               AND descriptor_change.schema_version = '{descriptor_schema_version}' \
               AND descriptor_change.file_id = '{descriptor_file_id}' \
               AND descriptor_change.plugin_key = '{descriptor_plugin_key}' \
         ), \
         ranked_descriptors AS ( \
             SELECT \
               entity_id, \
               snapshot_content, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY entity_id \
                 ORDER BY depth ASC, change_position DESC \
               ) AS rn \
             FROM descriptor_members \
         ), \
         descriptor_state AS ( \
             SELECT entity_id, snapshot_content \
             FROM ranked_descriptors \
             WHERE rn = 1 \
               AND snapshot_content IS NOT NULL \
         ) \
         SELECT \
             d.entity_id AS id, \
             COALESCE({name_expr}, '') AS name, \
             COALESCE({hidden_expr}, false) AS hidden, \
             COALESCE(r.commit_id, '') AS commit_id \
         FROM descriptor_state d \
         LEFT JOIN current_refs r \
           ON r.version_id = d.entity_id \
        ORDER BY d.entity_id ASC",
        current_refs_cte_sql = current_refs_cte_sql,
        global_version = escape_sql_string(&binding.global_version_id),
        parent_join_sql = parent_join_sql,
        parent_value_expr = parent_value_expr,
        change_join_sql = change_join_sql,
        change_value_expr = change_value_expr,
        change_position_expr = change_position_expr,
        name_expr = name_expr,
        hidden_expr = hidden_expr,
        descriptor_schema_key = escape_sql_string(&binding.descriptor_source.schema_key),
        descriptor_schema_version = escape_sql_string(&binding.descriptor_source.schema_version),
        descriptor_file_id = escape_sql_string(&binding.descriptor_source.file_id),
        descriptor_plugin_key = escape_sql_string(&binding.descriptor_source.plugin_key),
    )
}

pub(crate) fn build_filesystem_relation_sql(
    binding: &FilesystemRelationBinding,
    dialect: SqlDialect,
) -> Result<String, LixError> {
    match binding.kind {
        FilesystemRelationKind::File => build_filesystem_file_projection_sql(binding, dialect),
        FilesystemRelationKind::Directory => {
            build_filesystem_directory_projection_sql(binding, dialect)
        }
    }
}

fn build_local_version_ref_heads_source_sql(source: &StoredVersionHeadSourceBinding) -> String {
    format!(
        "SELECT \
            entity_id AS version_id, \
            commit_id AS commit_id \
         FROM {table} \
         WHERE schema_key = '{ref_schema_key}' \
           AND schema_version = '{ref_schema_version}' \
           AND file_id = '{ref_file_id}' \
           AND plugin_key = '{ref_plugin_key}' \
           AND version_id = '{storage_version_id}' \
           AND untracked = true \
           AND is_tombstone = 0 \
           AND commit_id IS NOT NULL \
           AND commit_id <> ''",
        table = tracked_relation_name(&source.schema_key),
        ref_schema_key = escape_sql_string(&source.schema_key),
        ref_schema_version = escape_sql_string(&source.schema_version),
        ref_file_id = escape_sql_string(&source.file_id),
        ref_plugin_key = escape_sql_string(&source.plugin_key),
        storage_version_id = escape_sql_string(&source.storage_version_id),
    )
}

fn build_current_version_refs_unique_cte_sql(head_source: &VersionHeadSourceBinding) -> String {
    match head_source {
        VersionHeadSourceBinding::InlineCurrentHeads(current_version_heads) => {
            build_inline_current_version_refs_cte_sql(current_version_heads)
        }
        VersionHeadSourceBinding::StoredRefs(source) => format!(
            "canonical_commit_headers AS ( \
                 SELECT \
                   commit_change.entity_id AS commit_id, \
                   commit_snapshot.content AS commit_snapshot_content \
                 FROM lix_internal_change commit_change \
                 LEFT JOIN lix_internal_snapshot commit_snapshot \
                   ON commit_snapshot.id = commit_change.snapshot_id \
                 WHERE commit_change.schema_key = 'lix_commit' \
                   AND commit_change.file_id = 'lix' \
                   AND commit_change.plugin_key = 'lix' \
                   AND commit_snapshot.content IS NOT NULL \
             ), \
             current_refs AS ( \
                 {current_refs_source_sql} \
             ), ",
            current_refs_source_sql = build_local_version_ref_heads_source_sql(source),
        ),
    }
}

fn build_inline_current_version_refs_cte_sql(
    current_version_heads: &BTreeMap<String, String>,
) -> String {
    let current_refs_sql = if current_version_heads.is_empty() {
        "SELECT NULL AS version_id, NULL AS commit_id WHERE 1 = 0".to_string()
    } else {
        let values = current_version_heads
            .iter()
            .map(|(version_id, commit_id)| {
                format!(
                    "('{}', '{}')",
                    escape_sql_string(version_id),
                    escape_sql_string(commit_id)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("VALUES {values}")
    };
    format!(
        "canonical_commit_headers AS ( \
             SELECT \
               commit_change.entity_id AS commit_id, \
               commit_snapshot.content AS commit_snapshot_content \
             FROM lix_internal_change commit_change \
             LEFT JOIN lix_internal_snapshot commit_snapshot \
               ON commit_snapshot.id = commit_change.snapshot_id \
             WHERE commit_change.schema_key = 'lix_commit' \
               AND commit_change.file_id = 'lix' \
               AND commit_change.plugin_key = 'lix' \
               AND commit_snapshot.content IS NOT NULL \
         ), \
         current_refs(version_id, commit_id) AS ( \
             {current_refs_sql} \
         ), ",
        current_refs_sql = current_refs_sql,
    )
}

fn build_filesystem_file_projection_sql(
    binding: &FilesystemRelationBinding,
    dialect: SqlDialect,
) -> Result<String, LixError> {
    let commit_id_projection = match binding.scope {
        FilesystemProjectionScope::ActiveVersion => {
            active_version_commit_id_sql(binding, required_active_version_id(binding)?)?
        }
        FilesystemProjectionScope::ExplicitVersion => "f.lixcol_commit_id".to_string(),
    };
    Ok(format!(
        "WITH RECURSIVE \
           {target_versions_cte}, \
           {lazy_change_commit_ctes}, \
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
            binding,
            &[
                binding.file_descriptor_schema_key.as_str(),
                binding.directory_descriptor_schema_key.as_str(),
                binding.binary_blob_ref_schema_key.as_str()
            ]
        )?,
        lazy_change_commit_ctes = build_lazy_change_commit_by_change_id_ctes_sql(dialect),
        directory_candidates_sql = effective_directory_descriptor_candidates_sql(
            &binding.directory_descriptor_schema_key,
            &binding.global_version_id,
        ),
        file_candidates_sql = effective_file_descriptor_candidates_sql(
            &binding.file_descriptor_schema_key,
            &binding.global_version_id,
            dialect,
        ),
        blob_candidates_sql = effective_binary_blob_ref_candidates_sql(
            &binding.binary_blob_ref_schema_key,
            &binding.global_version_id,
        ),
        binary_blob_store = INTERNAL_BINARY_BLOB_STORE,
        commit_id_projection = commit_id_projection,
    ))
}

fn build_filesystem_directory_projection_sql(
    binding: &FilesystemRelationBinding,
    dialect: SqlDialect,
) -> Result<String, LixError> {
    let commit_id_projection = match binding.scope {
        FilesystemProjectionScope::ActiveVersion => {
            active_version_commit_id_sql(binding, required_active_version_id(binding)?)?
        }
        FilesystemProjectionScope::ExplicitVersion => "d.lixcol_commit_id".to_string(),
    };
    Ok(format!(
        "WITH RECURSIVE \
           {target_versions_cte}, \
           {lazy_change_commit_ctes}, \
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
            target_versions_cte_sql(binding, &[binding.directory_descriptor_schema_key.as_str()])?,
        lazy_change_commit_ctes = build_lazy_change_commit_by_change_id_ctes_sql(dialect),
        directory_candidates_sql = effective_directory_descriptor_candidates_sql(
            &binding.directory_descriptor_schema_key,
            &binding.global_version_id,
        ),
        commit_id_projection = commit_id_projection,
    ))
}

fn target_versions_cte_sql(
    binding: &FilesystemRelationBinding,
    schema_keys: &[&str],
) -> Result<String, LixError> {
    match binding.scope {
        FilesystemProjectionScope::ActiveVersion => Ok(format!(
            "target_versions AS ( \
               SELECT '{active_version_id}' AS version_id \
             )",
            active_version_id = escape_sql_string(required_active_version_id(binding)?),
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
                            global_version = escape_sql_string(&binding.global_version_id),
                        ),
                        format!(
                            "SELECT DISTINCT version_id \
                             FROM {untracked_table} \
                    WHERE version_id <> '{global_version}' \
                      AND untracked = true",
                            untracked_table = quote_ident(&tracked_relation_name(schema_key)),
                            global_version = escape_sql_string(&binding.global_version_id),
                        ),
                    ]
                })
                .collect::<Vec<_>>();
            let union_sql = if union_rows.is_empty() {
                String::new()
            } else {
                format!(" UNION {}", union_rows.join(" UNION "))
            };
            let live_version_descriptor_table =
                tracked_relation_name(&binding.version_descriptor_schema_key);
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
                version_descriptor_schema_key =
                    escape_sql_string(&binding.version_descriptor_schema_key),
                global_version = escape_sql_string(&binding.global_version_id),
                union_sql = union_sql,
            ))
        }
    }
}

fn required_active_version_id(binding: &FilesystemRelationBinding) -> Result<&str, LixError> {
    match (binding.scope, binding.active_version_id.as_ref()) {
        (FilesystemProjectionScope::ActiveVersion, Some(active_version_id)) => {
            Ok(active_version_id.as_str())
        }
        (FilesystemProjectionScope::ActiveVersion, None) => Err(LixError::new(
            "LIX_ERROR_INVALID_ARGUMENT",
            "filesystem relation requires active_version_id for active-version scope",
        )),
        (FilesystemProjectionScope::ExplicitVersion, _) => Err(LixError::new(
            "LIX_ERROR_INVALID_ARGUMENT",
            "filesystem relation active_version_id is only valid for active-version scope",
        )),
    }
}

fn effective_state_candidates_sql(
    schema_key: &str,
    global_version_id: &str,
    payload_columns: &[(&str, String, String)],
) -> String {
    let table_name = quote_ident(&tracked_relation_name(schema_key));
    let untracked_table = quote_ident(&tracked_relation_name(schema_key));
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
         {tracked_writer_key_join_sql} \
         LEFT JOIN change_commit_by_change_id cc \
           ON cc.change_id = t.change_id \
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
           wk_global.writer_key AS writer_key, \
           t.metadata AS metadata, \
           4 AS precedence \
         FROM {table_name} t \
         JOIN target_versions tv \
           ON tv.version_id <> '{global_version}' \
          AND t.version_id = '{global_version}' \
         {tracked_global_writer_key_join_sql} \
         LEFT JOIN change_commit_by_change_id cc \
           ON cc.change_id = t.change_id \
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
           uwk.writer_key AS writer_key, \
           u.metadata AS metadata, \
           1 AS precedence \
         FROM {untracked_table} u \
         JOIN target_versions tv \
           ON tv.version_id = u.version_id \
         {untracked_writer_key_join_sql} \
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
           uwk_global.writer_key AS writer_key, \
           u.metadata AS metadata, \
           3 AS precedence \
         FROM {untracked_table} u \
         JOIN target_versions tv \
           ON tv.version_id <> '{global_version}' \
          AND u.version_id = '{global_version}' \
         {untracked_global_writer_key_join_sql} \
         WHERE u.version_id = '{global_version}' \
           AND u.untracked = true",
        table_name = table_name,
        untracked_table = untracked_table,
        tracked_payload_projection = tracked_payload_projection,
        untracked_payload_projection = untracked_payload_projection,
        tracked_writer_key_join_sql = tracked_writer_key_join_sql("t", "wk"),
        tracked_global_writer_key_join_sql = tracked_writer_key_join_sql("t", "wk_global"),
        untracked_writer_key_join_sql = tracked_writer_key_join_sql("u", "uwk"),
        untracked_global_writer_key_join_sql = tracked_writer_key_join_sql("u", "uwk_global"),
        global_version = escape_sql_string(global_version_id),
    )
}

fn effective_directory_descriptor_candidates_sql(
    schema_key: &str,
    global_version_id: &str,
) -> String {
    let id_column = live_payload_column_name(schema_key, "id");
    let parent_id_column = live_payload_column_name(schema_key, "parent_id");
    let name_column = live_payload_column_name(schema_key, "name");
    let hidden_column = live_payload_column_name(schema_key, "hidden");
    effective_state_candidates_sql(
        schema_key,
        global_version_id,
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

fn effective_file_descriptor_candidates_sql(
    schema_key: &str,
    global_version_id: &str,
    dialect: SqlDialect,
) -> String {
    let id_column = live_payload_column_name(schema_key, "id");
    let directory_id_column = live_payload_column_name(schema_key, "directory_id");
    let name_column = live_payload_column_name(schema_key, "name");
    let extension_column = live_payload_column_name(schema_key, "extension");
    let metadata_column = live_payload_column_name(schema_key, "metadata");
    let hidden_column = live_payload_column_name(schema_key, "hidden");
    effective_state_candidates_sql(
        schema_key,
        global_version_id,
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

fn effective_binary_blob_ref_candidates_sql(schema_key: &str, global_version_id: &str) -> String {
    let id_column = live_payload_column_name(schema_key, "id");
    let blob_hash_column = live_payload_column_name(schema_key, "blob_hash");
    let size_bytes_column = live_payload_column_name(schema_key, "size_bytes");
    effective_state_candidates_sql(
        schema_key,
        global_version_id,
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

fn json_array_text_join_sql(
    dialect: SqlDialect,
    json_column: &str,
    field: &str,
    alias: &str,
    value_column: &str,
) -> (String, String) {
    match dialect {
        SqlDialect::Sqlite => (
            format!("JOIN json_each({json_column}, '$.{field}') AS {alias}"),
            format!("{alias}.value"),
        ),
        SqlDialect::Postgres => (
            format!(
                "JOIN LATERAL jsonb_array_elements_text(CAST({json_column} AS JSONB) -> '{field}') AS {alias}({value_column}) ON TRUE"
            ),
            format!("{alias}.{value_column}"),
        ),
    }
}

fn json_array_text_join_with_position_sql(
    dialect: SqlDialect,
    json_column: &str,
    field: &str,
    alias: &str,
    value_column: &str,
    position_column: &str,
) -> (String, String, String) {
    match dialect {
        SqlDialect::Sqlite => (
            format!("JOIN json_each({json_column}, '$.{field}') AS {alias}"),
            format!("{alias}.value"),
            format!("CAST({alias}.key AS INTEGER)"),
        ),
        SqlDialect::Postgres => (
            format!(
                "JOIN LATERAL jsonb_array_elements_text(CAST({json_column} AS JSONB) -> '{field}') WITH ORDINALITY AS {alias}({value_column}, {position_column}) ON TRUE"
            ),
            format!("{alias}.{value_column}"),
            format!("{alias}.{position_column}"),
        ),
    }
}

fn json_text_extract_sql(dialect: SqlDialect, json_column: &str, field: &str) -> String {
    match dialect {
        SqlDialect::Sqlite => format!("json_extract({json_column}, '$.{field}')"),
        SqlDialect::Postgres => format!("CAST({json_column} AS JSONB) ->> '{field}'"),
    }
}

fn json_boolean_extract_sql(dialect: SqlDialect, json_column: &str, field: &str) -> String {
    match dialect {
        SqlDialect::Sqlite => format!("json_extract({json_column}, '$.{field}')"),
        SqlDialect::Postgres => {
            format!("CAST((CAST({json_column} AS JSONB) ->> '{field}') AS BOOLEAN)")
        }
    }
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn tracked_writer_key_join_sql(row_alias: &str, writer_alias: &str) -> String {
    format!(
        "LEFT JOIN {writer_key_table} {writer_alias} \
           ON {writer_alias}.version_id = {row_alias}.version_id \
          AND {writer_alias}.schema_key = {row_alias}.schema_key \
          AND {writer_alias}.entity_id = {row_alias}.entity_id \
          AND {writer_alias}.file_id = {row_alias}.file_id",
        writer_key_table = WRITER_KEY_TABLE,
        row_alias = quote_ident(row_alias),
        writer_alias = quote_ident(writer_alias),
    )
}

fn active_version_commit_id_sql(
    binding: &FilesystemRelationBinding,
    active_version_id: &str,
) -> Result<String, LixError> {
    let version_ref_commit_id_column = quote_ident(&live_payload_column_name(
        &binding.version_ref_schema_key,
        "commit_id",
    ));
    let live_version_ref_table = tracked_relation_name(&binding.version_ref_schema_key);
    Ok(format!(
        "(\
         SELECT {version_ref_commit_id_column} \
         FROM {live_version_ref_table} vp \
         WHERE vp.schema_key = '{version_ref_schema_key}' \
           AND vp.version_id = '{global_version}' \
           AND vp.untracked = true \
           AND {version_ref_commit_id_column} IS NOT NULL \
           AND vp.entity_id = '{active_version_id}' \
         LIMIT 1\
        )",
        version_ref_commit_id_column = version_ref_commit_id_column,
        version_ref_schema_key = escape_sql_string(&binding.version_ref_schema_key),
        global_version = escape_sql_string(&binding.global_version_id),
        active_version_id = escape_sql_string(active_version_id),
    ))
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
