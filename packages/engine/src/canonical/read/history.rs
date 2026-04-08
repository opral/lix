use crate::common::text::escape_sql_string;
use crate::schema::builtin::GLOBAL_VERSION_ID;
use crate::{LixError, SqlDialect};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CanonicalHistoryContentMode {
    MetadataOnly,
    IncludeSnapshotContent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalRootCommit {
    pub(crate) commit_id: String,
    pub(crate) version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CanonicalHistoryRootSelection {
    AllRoots,
    RequestedRootCommitIds(Vec<String>),
    ResolvedRootCommits(Vec<CanonicalRootCommit>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalHistoryRootFacts {
    pub(crate) traversal: CanonicalHistoryRootSelection,
    pub(crate) root_version_refs: Vec<CanonicalRootCommit>,
}

pub(crate) fn build_state_history_source_sql(
    dialect: SqlDialect,
    root_facts: &CanonicalHistoryRootFacts,
    content_mode: CanonicalHistoryContentMode,
    max_depth: Option<i64>,
) -> Result<String, LixError> {
    let max_depth_filter_sql = max_depth
        .map(|max_depth| format!("AND walk.commit_depth < {max_depth} "))
        .unwrap_or_default();
    let (parent_join_sql, parent_value_expr) = json_array_text_join_sql(
        dialect,
        "COALESCE(commit_headers.commit_snapshot_content, '{}')",
        "parent_commit_ids",
        "parent_rows",
        "parent_commit_id",
    );
    let (change_join_sql, change_value_expr, change_position_expr) =
        json_array_text_join_with_position_sql(
            dialect,
            "COALESCE(commit_headers.commit_snapshot_content, '{}')",
            "change_ids",
            "change_rows",
            "change_id",
            "change_position",
        );
    let snapshot_projection = match content_mode {
        CanonicalHistoryContentMode::MetadataOnly => "NULL AS snapshot_content".to_string(),
        CanonicalHistoryContentMode::IncludeSnapshotContent => {
            "s.content AS snapshot_content".to_string()
        }
    };
    let snapshot_join = match content_mode {
        CanonicalHistoryContentMode::MetadataOnly => String::new(),
        CanonicalHistoryContentMode::IncludeSnapshotContent => "LEFT JOIN lix_internal_snapshot s \
             ON s.id = h.snapshot_id "
            .to_string(),
    };
    let root_version_refs_cte_sql = build_root_commits_cte_sql(
        "root_version_refs",
        &root_facts.root_version_refs,
        "commit_id",
        "root_version_id",
    );
    let requested_commits_cte_sql = build_requested_commits_cte_sql(root_facts)?;

    Ok(format!(
        "WITH RECURSIVE \
           canonical_commit_headers AS ( \
             SELECT \
               commit_change.entity_id AS commit_id, \
               commit_snapshot.content AS commit_snapshot_content, \
               commit_change.created_at AS commit_created_at \
             FROM lix_internal_change commit_change \
             LEFT JOIN lix_internal_snapshot commit_snapshot \
               ON commit_snapshot.id = commit_change.snapshot_id \
             WHERE commit_change.schema_key = 'lix_commit' \
           ), \
           {root_version_refs_cte_sql}\
           {requested_commits_cte_sql}\
           reachable_commit_walk AS ( \
             SELECT \
               requested.commit_id AS commit_id, \
               requested.commit_id AS root_commit_id, \
               requested.root_version_id AS root_version_id, \
               0 AS commit_depth \
             FROM requested_commits requested \
             UNION ALL \
             SELECT \
               {parent_value_expr} AS commit_id, \
               walk.root_commit_id AS root_commit_id, \
               walk.root_version_id AS root_version_id, \
               walk.commit_depth + 1 AS commit_depth \
             FROM reachable_commit_walk walk \
             JOIN canonical_commit_headers commit_headers \
               ON commit_headers.commit_id = walk.commit_id \
             {parent_join_sql} \
             WHERE {parent_value_expr} IS NOT NULL \
               {max_depth_filter_sql}\
           ), \
           reachable_commits AS ( \
             SELECT \
               commit_id, \
               root_commit_id, \
               root_version_id, \
               MIN(commit_depth) AS commit_depth \
             FROM reachable_commit_walk \
             GROUP BY commit_id, root_commit_id, root_version_id \
           ), \
           commit_members AS ( \
             SELECT \
               changes.entity_id AS entity_id, \
               changes.schema_key AS schema_key, \
               changes.file_id AS file_id, \
               changes.plugin_key AS plugin_key, \
               changes.schema_version AS schema_version, \
               changes.metadata AS metadata, \
               changes.snapshot_id AS snapshot_id, \
               changes.id AS change_id, \
               reachable.commit_id AS commit_id, \
               reachable.root_commit_id AS root_commit_id, \
               reachable.root_version_id AS root_version_id, \
               reachable.commit_depth AS commit_depth, \
               commit_headers.commit_created_at AS commit_created_at, \
               {change_position_expr} AS change_position \
             FROM reachable_commits reachable \
             JOIN canonical_commit_headers commit_headers \
               ON commit_headers.commit_id = reachable.commit_id \
             {change_join_sql} \
             JOIN lix_internal_change changes \
               ON changes.id = {change_value_expr} \
           ), \
           ranked AS ( \
             SELECT \
               members.entity_id, \
               members.schema_key, \
               members.file_id, \
               members.plugin_key, \
               members.schema_version, \
               members.metadata, \
               members.snapshot_id, \
               members.change_id, \
               members.commit_id, \
               members.root_commit_id, \
               members.root_version_id, \
               members.commit_depth, \
               members.commit_created_at, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY members.root_commit_id, members.entity_id, members.schema_key, members.file_id, members.commit_depth \
                 ORDER BY members.change_position DESC \
               ) AS rn \
             FROM commit_members members \
           ), \
           source_rows AS ( \
             SELECT \
               ranked.entity_id, \
               ranked.schema_key, \
               ranked.file_id, \
               ranked.plugin_key, \
               ranked.schema_version, \
               ranked.metadata, \
               ranked.snapshot_id, \
               ranked.change_id, \
               ranked.commit_id, \
               ranked.root_commit_id, \
               ranked.root_version_id, \
               ranked.commit_depth, \
               ranked.commit_created_at \
             FROM ranked \
             WHERE ranked.rn = 1 \
           ), \
           breakpoint_rows AS ( \
             SELECT \
               source.entity_id, \
               source.schema_key, \
               source.file_id, \
               source.plugin_key, \
               source.schema_version, \
               source.metadata, \
               source.snapshot_id, \
               source.change_id, \
               source.commit_id, \
               source.root_commit_id, \
               source.root_version_id, \
               source.commit_depth, \
               source.commit_created_at, \
               LAG(source.plugin_key) OVER ( \
                 PARTITION BY source.root_commit_id, source.entity_id, source.schema_key, source.file_id \
                 ORDER BY source.commit_depth \
               ) AS prev_plugin_key, \
               LAG(source.schema_version) OVER ( \
                 PARTITION BY source.root_commit_id, source.entity_id, source.schema_key, source.file_id \
                 ORDER BY source.commit_depth \
               ) AS prev_schema_version, \
               LAG(source.metadata) OVER ( \
                 PARTITION BY source.root_commit_id, source.entity_id, source.schema_key, source.file_id \
                 ORDER BY source.commit_depth \
               ) AS prev_metadata, \
               LAG(source.snapshot_id) OVER ( \
                 PARTITION BY source.root_commit_id, source.entity_id, source.schema_key, source.file_id \
                 ORDER BY source.commit_depth \
               ) AS prev_snapshot_id, \
               LAG(source.change_id) OVER ( \
                 PARTITION BY source.root_commit_id, source.entity_id, source.schema_key, source.file_id \
                 ORDER BY source.commit_depth \
               ) AS prev_change_id \
             FROM source_rows source \
           ), \
           history_rows AS ( \
             SELECT \
               bp.entity_id, \
               bp.schema_key, \
               bp.file_id, \
               bp.plugin_key, \
               bp.schema_version, \
               bp.metadata, \
               bp.snapshot_id, \
               bp.change_id, \
               bp.commit_id AS commit_id, \
               bp.commit_created_at AS commit_created_at, \
               bp.root_commit_id AS root_commit_id, \
               bp.root_version_id AS version_id, \
               bp.commit_depth AS depth \
             FROM breakpoint_rows bp \
             WHERE bp.prev_plugin_key IS NULL \
               OR bp.plugin_key != bp.prev_plugin_key \
               OR bp.schema_version != bp.prev_schema_version \
               OR COALESCE(bp.metadata, '__LIX_NULL__') != COALESCE(bp.prev_metadata, '__LIX_NULL__') \
               OR bp.snapshot_id != bp.prev_snapshot_id \
               OR bp.change_id != bp.prev_change_id \
           ) \
         SELECT \
           h.entity_id AS entity_id, \
           h.schema_key AS schema_key, \
           h.file_id AS file_id, \
           h.plugin_key AS plugin_key, \
           {snapshot_projection}, \
           h.metadata AS metadata, \
           h.schema_version AS schema_version, \
           h.change_id AS change_id, \
           h.commit_id AS commit_id, \
           h.commit_created_at AS commit_created_at, \
           h.root_commit_id AS root_commit_id, \
           h.depth AS depth, \
           h.version_id AS version_id \
         FROM history_rows h \
         {snapshot_join}\
         WHERE h.snapshot_id != 'no-content'",
        root_version_refs_cte_sql = root_version_refs_cte_sql,
        requested_commits_cte_sql = requested_commits_cte_sql,
        parent_join_sql = parent_join_sql,
        parent_value_expr = parent_value_expr,
        change_join_sql = change_join_sql,
        change_value_expr = change_value_expr,
        change_position_expr = change_position_expr,
        max_depth_filter_sql = max_depth_filter_sql,
        snapshot_projection = snapshot_projection,
        snapshot_join = snapshot_join,
    ))
}

fn build_requested_commits_cte_sql(
    root_facts: &CanonicalHistoryRootFacts,
) -> Result<String, LixError> {
    match &root_facts.traversal {
        CanonicalHistoryRootSelection::AllRoots => Ok(format!(
            "requested_commits AS ( \
               SELECT DISTINCT \
                 headers.commit_id AS commit_id, \
                 COALESCE(refs.root_version_id, '{global_version}') AS root_version_id \
               FROM canonical_commit_headers headers \
               LEFT JOIN root_version_refs refs \
                 ON refs.commit_id = headers.commit_id \
             ), ",
            global_version = escape_sql_string(GLOBAL_VERSION_ID),
        )),
        CanonicalHistoryRootSelection::RequestedRootCommitIds(root_commit_ids) => Ok(format!(
            "{requested_root_ids_cte_sql}\
                 requested_commits AS ( \
                   SELECT DISTINCT \
                     headers.commit_id AS commit_id, \
                     COALESCE(refs.root_version_id, '{global_version}') AS root_version_id \
                   FROM requested_root_ids requested \
                   JOIN canonical_commit_headers headers \
                     ON headers.commit_id = requested.commit_id \
                   LEFT JOIN root_version_refs refs \
                     ON refs.commit_id = headers.commit_id \
                 ), ",
            requested_root_ids_cte_sql =
                build_text_list_cte_sql("requested_root_ids", root_commit_ids, "commit_id"),
            global_version = escape_sql_string(GLOBAL_VERSION_ID),
        )),
        CanonicalHistoryRootSelection::ResolvedRootCommits(root_commits) => Ok(format!(
            "{resolved_root_commits_cte_sql}\
             requested_commits AS ( \
               SELECT DISTINCT \
                 resolved.commit_id AS commit_id, \
                 resolved.root_version_id AS root_version_id \
               FROM resolved_root_commits resolved \
               JOIN canonical_commit_headers headers \
                 ON headers.commit_id = resolved.commit_id \
             ), ",
            resolved_root_commits_cte_sql = build_root_commits_cte_sql(
                "resolved_root_commits",
                root_commits,
                "commit_id",
                "root_version_id"
            ),
        )),
    }
}

fn build_root_commits_cte_sql(
    cte_name: &str,
    rows: &[CanonicalRootCommit],
    commit_id_column: &str,
    version_id_column: &str,
) -> String {
    let select_sql = if rows.is_empty() {
        format!(
            "SELECT CAST(NULL AS TEXT) AS {commit_id_column}, CAST(NULL AS TEXT) AS {version_id_column} \
             WHERE 1 = 0"
        )
    } else {
        build_text_pair_select_sql(rows, commit_id_column, version_id_column)
    };
    format!("{cte_name} AS ({select_sql}), ")
}

fn build_text_list_cte_sql(cte_name: &str, values: &[String], column_name: &str) -> String {
    let select_sql = if values.is_empty() {
        format!("SELECT CAST(NULL AS TEXT) AS {column_name} WHERE 1 = 0")
    } else {
        build_text_list_select_sql(values, column_name)
    };
    format!("{cte_name} AS ({select_sql}), ")
}

fn build_text_pair_select_sql(
    rows: &[CanonicalRootCommit],
    left_column: &str,
    right_column: &str,
) -> String {
    let mut sql = String::new();
    for (index, row) in rows.iter().enumerate() {
        if index == 0 {
            sql.push_str("SELECT ");
            sql.push_str(&format!(
                "'{}' AS {left_column}, '{}' AS {right_column}",
                escape_sql_string(&row.commit_id),
                escape_sql_string(&row.version_id),
            ));
        } else {
            sql.push_str(" UNION ALL SELECT ");
            sql.push_str(&format!(
                "'{}', '{}'",
                escape_sql_string(&row.commit_id),
                escape_sql_string(&row.version_id),
            ));
        }
    }
    sql
}

fn build_text_list_select_sql(values: &[String], column_name: &str) -> String {
    let mut sql = String::new();
    for (index, value) in values.iter().enumerate() {
        if index == 0 {
            sql.push_str("SELECT ");
            sql.push_str(&format!("'{}' AS {column_name}", escape_sql_string(value)));
        } else {
            sql.push_str(" UNION ALL SELECT ");
            sql.push_str(&format!("'{}'", escape_sql_string(value)));
        }
    }
    sql
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_history_uses_internal_change_sources_not_live_tables() {
        let sql = build_state_history_source_sql(
            SqlDialect::Sqlite,
            &CanonicalHistoryRootFacts {
                traversal: CanonicalHistoryRootSelection::AllRoots,
                root_version_refs: vec![CanonicalRootCommit {
                    commit_id: "commit-main".to_string(),
                    version_id: "main".to_string(),
                }],
            },
            CanonicalHistoryContentMode::IncludeSnapshotContent,
            Some(32),
        )
        .expect("canonical history SQL should build");

        assert!(sql.contains("FROM lix_internal_change commit_change"));
        assert!(sql.contains("JOIN lix_internal_change changes"));
        assert!(sql.contains("json_each("));
        assert!(!sql.contains("lix_internal_live_v1_lix_commit"));
        assert!(!sql.contains("lix_internal_live_v1_lix_change_set_element"));
    }

    #[test]
    fn resolved_root_commits_are_rendered_as_typed_requested_commit_rows() {
        let sql = build_state_history_source_sql(
            SqlDialect::Sqlite,
            &CanonicalHistoryRootFacts {
                traversal: CanonicalHistoryRootSelection::ResolvedRootCommits(vec![
                    CanonicalRootCommit {
                        commit_id: "commit-main".to_string(),
                        version_id: "main".to_string(),
                    },
                    CanonicalRootCommit {
                        commit_id: "commit-feature".to_string(),
                        version_id: "feature".to_string(),
                    },
                ]),
                root_version_refs: Vec::new(),
            },
            CanonicalHistoryContentMode::MetadataOnly,
            None,
        )
        .expect("canonical history SQL should build");

        assert!(sql.contains("resolved_root_commits AS (SELECT 'commit-main' AS commit_id, 'main' AS root_version_id"));
        assert!(sql.contains("UNION ALL SELECT 'commit-feature', 'feature'"));
        assert!(!sql.contains("root_version_refs AS (SELECT 'commit-main'"));
    }

    #[test]
    fn canonical_history_has_no_hidden_default_depth_cap() {
        let sql = build_state_history_source_sql(
            SqlDialect::Sqlite,
            &CanonicalHistoryRootFacts {
                traversal: CanonicalHistoryRootSelection::AllRoots,
                root_version_refs: vec![CanonicalRootCommit {
                    commit_id: "commit-main".to_string(),
                    version_id: "main".to_string(),
                }],
            },
            CanonicalHistoryContentMode::MetadataOnly,
            None,
        )
        .expect("canonical history SQL should build");

        assert!(!sql.contains("walk.commit_depth < 512"));
        assert!(!sql.contains("walk.commit_depth < {max_depth}"));
    }
}
