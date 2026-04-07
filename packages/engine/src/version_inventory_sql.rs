use std::collections::BTreeMap;

use crate::text::escape_sql_string;
use crate::version::build_local_version_ref_heads_source_sql;
use crate::version_artifacts::{
    version_descriptor_file_id, version_descriptor_plugin_key, version_descriptor_schema_key,
    version_descriptor_schema_version, GLOBAL_VERSION_ID,
};
use crate::SqlDialect;

pub(crate) fn build_admin_version_source_sql(dialect: SqlDialect) -> String {
    build_admin_version_source_sql_with_current_heads(dialect, None)
}

pub(crate) fn build_admin_version_source_sql_with_current_heads(
    dialect: SqlDialect,
    current_version_heads: Option<&BTreeMap<String, String>>,
) -> String {
    let current_refs_cte_sql =
        build_current_version_refs_unique_cte_sql(dialect, current_version_heads);
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
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        parent_join_sql = parent_join_sql,
        parent_value_expr = parent_value_expr,
        change_join_sql = change_join_sql,
        change_value_expr = change_value_expr,
        change_position_expr = change_position_expr,
        name_expr = name_expr,
        hidden_expr = hidden_expr,
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_schema_version = escape_sql_string(version_descriptor_schema_version()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_plugin_key = escape_sql_string(version_descriptor_plugin_key()),
    )
}

fn build_current_version_refs_unique_cte_sql(
    _dialect: SqlDialect,
    current_version_heads: Option<&BTreeMap<String, String>>,
) -> String {
    if let Some(current_version_heads) = current_version_heads {
        return build_inline_current_version_refs_cte_sql(current_version_heads);
    }

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
         current_refs AS ( \
             {current_refs_source_sql} \
         ), ",
        current_refs_source_sql = build_local_version_ref_heads_source_sql(),
    )
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
