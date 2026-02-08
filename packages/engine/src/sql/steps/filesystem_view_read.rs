use sqlparser::ast::{
    Ident, ObjectName, ObjectNamePart, Query, Select, SetExpr, Statement, TableAlias, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::sql::escape_sql_string;
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
};
use crate::LixError;

const FILE_VIEW: &str = "lix_file";
const FILE_BY_VERSION_VIEW: &str = "lix_file_by_version";
const FILE_HISTORY_VIEW: &str = "lix_file_history";
const DIRECTORY_VIEW: &str = "lix_directory";
const DIRECTORY_BY_VERSION_VIEW: &str = "lix_directory_by_version";
const DIRECTORY_HISTORY_VIEW: &str = "lix_directory_history";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
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
            "SELECT \
                f.id, \
                f.path, \
                lix_empty_blob() AS data, \
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
                f.lixcol_untracked, \
                f.lixcol_metadata \
            FROM lix_file_descriptor_by_version f \
            LEFT JOIN lix_version v \
              ON v.id = f.lixcol_version_id \
            WHERE {active_version_scope}",
            active_version_scope = active_version_scope_predicate("f.lixcol_version_id")
        ),
        FILE_BY_VERSION_VIEW => "SELECT \
                f.id, \
                f.path, \
                lix_empty_blob() AS data, \
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
                f.lixcol_untracked, \
                f.lixcol_metadata \
            FROM lix_file_descriptor_by_version f \
            LEFT JOIN lix_version v \
              ON v.id = f.lixcol_version_id"
            .to_string(),
        FILE_HISTORY_VIEW => "SELECT \
                id, \
                path, \
                lix_empty_blob() AS data, \
                metadata, \
                hidden, \
                lixcol_entity_id, \
                lixcol_schema_key, \
                lixcol_file_id, \
                lixcol_version_id, \
                lixcol_plugin_key, \
                lixcol_schema_version, \
                lixcol_change_id, \
                lixcol_metadata, \
                lixcol_commit_id, \
                lixcol_root_commit_id, \
                lixcol_depth \
            FROM lix_file_descriptor_history"
            .to_string(),
        DIRECTORY_VIEW => format!(
            "SELECT \
                d.id, \
                d.parent_id, \
                d.name, \
                d.path, \
                d.hidden, \
                d.lixcol_entity_id, \
                d.lixcol_schema_key, \
                d.lixcol_file_id, \
                d.lixcol_plugin_key, \
                d.lixcol_schema_version, \
                d.lixcol_inherited_from_version_id, \
                d.lixcol_change_id, \
                d.lixcol_created_at, \
                d.lixcol_updated_at, \
                v.commit_id AS lixcol_commit_id, \
                d.lixcol_untracked, \
                d.lixcol_metadata \
            FROM lix_directory_descriptor_by_version d \
            LEFT JOIN lix_version v \
              ON v.id = d.lixcol_version_id \
            WHERE {active_version_scope}",
            active_version_scope = active_version_scope_predicate("d.lixcol_version_id")
        ),
        DIRECTORY_BY_VERSION_VIEW => "SELECT \
                d.id, \
                d.parent_id, \
                d.name, \
                d.path, \
                d.hidden, \
                d.lixcol_entity_id, \
                d.lixcol_schema_key, \
                d.lixcol_file_id, \
                d.lixcol_version_id, \
                d.lixcol_plugin_key, \
                d.lixcol_schema_version, \
                d.lixcol_inherited_from_version_id, \
                d.lixcol_change_id, \
                d.lixcol_created_at, \
                d.lixcol_updated_at, \
                v.commit_id AS lixcol_commit_id, \
                d.lixcol_untracked, \
                d.lixcol_metadata \
            FROM lix_directory_descriptor_by_version d \
            LEFT JOIN lix_version v \
              ON v.id = d.lixcol_version_id"
            .to_string(),
        DIRECTORY_HISTORY_VIEW => "SELECT \
                id, \
                parent_id, \
                name, \
                path, \
                hidden, \
                lixcol_entity_id, \
                lixcol_schema_key, \
                lixcol_file_id, \
                lixcol_version_id, \
                lixcol_plugin_key, \
                lixcol_schema_version, \
                lixcol_change_id, \
                lixcol_metadata, \
                lixcol_commit_id, \
                lixcol_root_commit_id, \
                lixcol_depth \
            FROM lix_directory_descriptor_history"
            .to_string(),
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
    use super::rewrite_query;
    use crate::sql::pipeline::parse_sql_statements;

    #[test]
    fn rewrites_file_view_reads_to_descriptor_projection() {
        let sql = "SELECT id, data FROM lix_file WHERE path = '/src/index.ts'";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query(query)
            .expect("rewrite")
            .expect("query should be rewritten")
            .to_string();

        assert!(rewritten.contains("lix_empty_blob() AS data"));
        assert!(rewritten.contains("FROM lix_file_descriptor_by_version"));
        assert!(rewritten.contains("FROM lix_internal_state_untracked"));
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
}
