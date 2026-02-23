use sqlparser::ast::{
    Ident, ObjectName, ObjectNamePart, Query, Select, SetExpr, Statement, TableAlias, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::sql::{
    directory_history_projection_sql, escape_sql_string, file_history_projection_sql,
};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
};
use crate::{LixError, Value};

const FILE_VIEW: &str = "lix_file";
const FILE_BY_VERSION_VIEW: &str = "lix_file_by_version";
const FILE_HISTORY_VIEW: &str = "lix_file_history";
const DIRECTORY_VIEW: &str = "lix_directory";
const DIRECTORY_BY_VERSION_VIEW: &str = "lix_directory_by_version";
const DIRECTORY_HISTORY_VIEW: &str = "lix_directory_history";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_with_params(query, &[])
}

pub fn rewrite_query_with_params(
    query: Query,
    _params: &[Value],
) -> Result<Option<Query>, LixError> {
    let mut changed = false;
    let mut new_query = query.clone();
    new_query.body = Box::new(rewrite_set_expr(*query.body, &mut changed)?);

    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

fn rewrite_set_expr(expr: SetExpr, changed: &mut bool) -> Result<SetExpr, LixError> {
    Ok(match expr {
        SetExpr::Select(select) => {
            let mut select = *select;
            rewrite_select(&mut select, changed)?;
            SetExpr::Select(Box::new(select))
        }
        SetExpr::Query(query) => {
            let mut query = *query;
            query.body = Box::new(rewrite_set_expr(*query.body, changed)?);
            SetExpr::Query(Box::new(query))
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => SetExpr::SetOperation {
            op,
            set_quantifier,
            left: Box::new(rewrite_set_expr(*left, changed)?),
            right: Box::new(rewrite_set_expr(*right, changed)?),
        },
        other => other,
    })
}

fn rewrite_select(select: &mut Select, changed: &mut bool) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_table_with_joins(table, changed)?;
    }
    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    changed: &mut bool,
) -> Result<(), LixError> {
    rewrite_table_factor(&mut table.relation, changed)?;
    for join in &mut table.joins {
        rewrite_table_factor(&mut join.relation, changed)?;
    }
    Ok(())
}

fn rewrite_table_factor(relation: &mut TableFactor, changed: &mut bool) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let Some(view_name) = object_name_terminal(name) else {
                return Ok(());
            };
            let Some(source_query) = build_filesystem_projection_query(&view_name)? else {
                return Ok(());
            };
            let derived_alias = alias.clone().or_else(|| Some(default_alias(&view_name)));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(source_query),
                alias: derived_alias,
            };
            *changed = true;
        }
        TableFactor::Derived { subquery, .. } => {
            if let Some(rewritten) = rewrite_query((**subquery).clone())? {
                *subquery = Box::new(rewritten);
                *changed = true;
            }
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_with_joins(table_with_joins, changed)?;
        }
        _ => {}
    }
    Ok(())
}

fn build_filesystem_projection_query(view_name: &str) -> Result<Option<Query>, LixError> {
    let sql = match view_name.to_ascii_lowercase().as_str() {
        FILE_VIEW => format!(
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
        FILE_BY_VERSION_VIEW => "WITH RECURSIVE directory_descriptor_rows AS (\
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
        FILE_HISTORY_VIEW => file_history_projection_sql(),
        DIRECTORY_VIEW => format!(
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
        DIRECTORY_BY_VERSION_VIEW => "WITH RECURSIVE directory_descriptor_rows AS (\
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
        DIRECTORY_HISTORY_VIEW => directory_history_projection_sql(),
        _ => return Ok(None),
    };

    Ok(Some(parse_single_query(&sql)?))
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

fn parse_single_query(sql: &str) -> Result<Query, LixError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        message: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "expected a single SELECT statement".to_string(),
        });
    }
    let statement = statements.remove(0);
    match statement {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            message: "expected SELECT statement".to_string(),
        }),
    }
}

fn object_name_terminal(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.clone())
}

fn default_alias(view_name: &str) -> TableAlias {
    TableAlias {
        explicit: false,
        name: Ident::new(view_name),
        columns: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{rewrite_query, rewrite_query_with_params};
    use crate::sql::parse_sql_statements;
    use crate::Value;

    fn rewrite_single_query_sql(sql: &str) -> String {
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        rewrite_query(query)
            .expect("rewrite")
            .expect("query should be rewritten")
            .to_string()
    }

    #[test]
    fn rewrites_file_view_reads_to_descriptor_projection() {
        let rewritten =
            rewrite_single_query_sql("SELECT id, data FROM lix_file WHERE path = '/src/index.ts'");

        assert!(rewritten.contains("COALESCE(fd.data, lix_empty_blob()) AS data"));
        assert!(rewritten.contains("FROM lix_state_by_version"));
        assert!(rewritten.contains("schema_key = 'lix_file_descriptor'"));
        assert!(rewritten.contains("FROM lix_internal_state_untracked"));
    }

    #[test]
    fn rewrites_simple_file_path_data_query_to_projection() {
        let rewritten = rewrite_single_query_sql("SELECT path, data FROM lix_file ORDER BY path");

        assert!(rewritten.contains("COALESCE(fd.data, lix_empty_blob()) AS data"));
        assert!(rewritten.contains("FROM lix_state_by_version"));
        assert!(rewritten.contains("schema_key = 'lix_file_descriptor'"));
        assert!(rewritten.contains("LEFT JOIN lix_version v"));
    }

    #[test]
    fn rewrites_simple_file_path_data_point_query_with_param() {
        let sql = "SELECT path, data FROM lix_file WHERE path = ?";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query_with_params(
            query,
            &[Value::Text("/bench/read/07/file-00007.txt".to_string())],
        )
        .expect("rewrite")
        .expect("query should be rewritten")
        .to_string();

        assert!(rewritten.contains("WHERE path = ?"));
        assert!(rewritten.contains("directory_paths"));
        assert!(!rewritten.contains("dir_level_0"));
        assert!(!rewritten.contains("dir_level_1"));
    }

    #[test]
    fn leaves_non_filesystem_reads_untouched() {
        let sql = "SELECT * FROM lix_state";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query(query).expect("rewrite");
        assert!(rewritten.is_none());
    }

    #[test]
    fn rewrites_directory_history_to_shared_history_projection() {
        let rewritten = rewrite_single_query_sql(
            "SELECT id, path, lixcol_root_commit_id, lixcol_depth FROM lix_directory_history",
        );

        assert!(rewritten.contains("directory_history_base AS ("));
        assert!(rewritten.contains("FROM lix_state_history"));
        assert!(rewritten.contains("root_commit_id AS lixcol_root_commit_id"));
        assert!(rewritten.contains("depth AS lixcol_depth"));
        assert!(rewritten.contains("LEFT JOIN directory_history_paths dp"));
    }

    #[test]
    fn rewrites_file_history_with_descriptor_rows_from_state_history() {
        let rewritten = rewrite_single_query_sql(
            "SELECT id, path, data, lixcol_root_commit_id, lixcol_depth FROM lix_file_history",
        );

        assert!(rewritten.contains("file_history_descriptor_rows AS ("));
        assert!(rewritten.contains("FROM lix_state_history"));
        assert!(rewritten.contains("schema_key = 'lix_file_descriptor'"));
        assert!(rewritten.contains("root_commit_id AS lixcol_root_commit_id"));
        assert!(rewritten.contains("depth AS lixcol_depth"));
        assert!(rewritten.contains("LEFT JOIN lix_internal_file_history_data_cache fd"));
    }

    #[test]
    fn rewrites_file_history_with_content_only_root_fallback_rows() {
        let rewritten = rewrite_single_query_sql(
            "SELECT id, lixcol_root_commit_id, lixcol_depth FROM lix_file_history",
        );

        assert!(rewritten.contains("descriptor_depth_zero_roots AS ("));
        assert!(rewritten.contains("content_only_roots AS ("));
        assert!(rewritten.contains("content_history_rows AS ("));
        assert!(rewritten.contains("SELECT * FROM file_history_descriptor_rows"));
        assert!(rewritten.contains("UNION ALL"));
        assert!(rewritten.contains("SELECT * FROM content_history_rows"));
        assert!(rewritten.contains("file_history_ranked_rows AS ("));
    }

    #[test]
    fn file_history_rewrite_does_not_scope_to_active_version_only() {
        let rewritten = rewrite_single_query_sql(
            "SELECT id, lixcol_root_commit_id, lixcol_depth FROM lix_file_history WHERE lixcol_root_commit_id = 'commit-root-a'",
        );

        assert!(rewritten.contains("root_commit_id AS lixcol_root_commit_id"));
        assert!(rewritten.contains("FROM lix_state_history"));
        assert!(!rewritten.contains("FROM lix_internal_state_untracked"));
    }

    #[test]
    fn file_history_rewrite_keeps_next_shape_anchors_for_root_depth_path_joining() {
        let rewritten = rewrite_single_query_sql(
            "SELECT id, path, lixcol_root_commit_id, lixcol_depth FROM lix_file_history",
        );

        assert!(rewritten.contains("directory_history_path_walk"));
        assert!(rewritten.contains("candidate.lixcol_depth >= walk.target_depth"));
        assert!(rewritten.contains("fhr.lixcol_depth AS lixcol_raw_depth"));
        assert!(rewritten.contains("ROW_NUMBER() OVER ("));
        assert!(rewritten.contains("PARTITION BY fhr.id, fhr.lixcol_root_commit_id"));
        assert!(rewritten.contains("fd.root_commit_id = f.lixcol_root_commit_id"));
        assert!(rewritten.contains("fd.depth = f.lixcol_depth"));
    }
}
