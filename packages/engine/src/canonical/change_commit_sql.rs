use crate::SqlDialect;

pub(crate) fn build_lazy_change_commit_by_change_id_ctes_sql(dialect: SqlDialect) -> String {
    let change_set_id_expr = match dialect {
        SqlDialect::Sqlite => {
            "json_extract(commit_rows.commit_snapshot_content, '$.change_set_id')".to_string()
        }
        SqlDialect::Postgres => {
            "(CAST(commit_rows.commit_snapshot_content AS JSONB) ->> 'change_set_id')".to_string()
        }
    };
    let (change_id_join_sql, change_id_value_expr) = json_array_text_join_sql(
        dialect,
        "commit_rows.commit_snapshot_content",
        "change_ids",
        "member_change_rows",
        "change_id",
    );

    format!(
        "commit_rows AS ( \
             SELECT \
               commit_change.entity_id AS commit_id, \
               commit_snapshot.content AS commit_snapshot_content \
             FROM lix_internal_change commit_change \
             LEFT JOIN lix_internal_snapshot commit_snapshot \
               ON commit_snapshot.id = commit_change.snapshot_id \
             WHERE commit_change.schema_key = 'lix_commit' \
               AND commit_snapshot.content IS NOT NULL \
         ), \
         commit_members AS ( \
             SELECT \
               commit_rows.commit_id AS commit_id, \
               {change_set_id_expr} AS change_set_id, \
               {change_id_value_expr} AS change_id \
             FROM commit_rows \
             {change_id_join_sql} \
             WHERE {change_set_id_expr} IS NOT NULL \
               AND {change_id_value_expr} IS NOT NULL \
         ), \
         change_commit_by_change_id AS ( \
             SELECT \
               commit_members.change_id AS change_id, \
               MAX(commit_members.commit_id) AS commit_id \
             FROM commit_members \
             JOIN lix_internal_change changes \
               ON changes.id = commit_members.change_id \
             WHERE commit_members.change_id IS NOT NULL \
             GROUP BY commit_members.change_id \
         )",
        change_set_id_expr = change_set_id_expr,
        change_id_join_sql = change_id_join_sql,
        change_id_value_expr = change_id_value_expr,
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
