use sqlparser::ast::{
    Expr, Ident, ObjectName, ObjectNamePart, Query, Select, SelectItem, SetExpr, TableAlias,
    TableFactor, TableWithJoins,
};

use crate::filesystem::path::parse_file_path;
use crate::filesystem::planner::read::{
    build_filesystem_projection_query, build_lix_file_path_data_fast_query,
    build_lix_file_path_data_point_query, FILE_VIEW,
};
use crate::sql::{resolve_expr_cell_with_state, PlaceholderState};
use crate::{LixError, Value};

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_with_params(query, &[])
}

pub fn rewrite_query_with_params(
    query: Query,
    params: &[Value],
) -> Result<Option<Query>, LixError> {
    if let Some(rewritten) = try_rewrite_lix_file_path_data_fast_path(&query, params)? {
        return Ok(Some(rewritten));
    }

    let mut changed = false;
    let mut new_query = query.clone();
    new_query.body = Box::new(rewrite_set_expr(*query.body, &mut changed)?);

    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

enum FilePathDataFastPathKind {
    Scan,
    PointPath(String),
}

fn try_rewrite_lix_file_path_data_fast_path(
    query: &Query,
    params: &[Value],
) -> Result<Option<Query>, LixError> {
    if query.with.is_some() || query.limit_clause.is_some() || query.fetch.is_some() {
        return Ok(None);
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    let select = select.as_ref();
    let Some(kind) = classify_simple_lix_file_path_data_select(select, params)? else {
        return Ok(None);
    };
    if !order_by_uses_only_path_or_data(query) {
        return Ok(None);
    }

    let order_by_sql = query.order_by.as_ref().map(ToString::to_string);
    let rewritten = match kind {
        FilePathDataFastPathKind::Scan => build_lix_file_path_data_fast_query(order_by_sql)?,
        FilePathDataFastPathKind::PointPath(path) => {
            build_lix_file_path_data_point_query(&path, order_by_sql)?
        }
    };
    Ok(Some(rewritten))
}

fn classify_simple_lix_file_path_data_select(
    select: &Select,
    params: &[Value],
) -> Result<Option<FilePathDataFastPathKind>, LixError> {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Ok(None);
    }
    let TableFactor::Table { name, .. } = &select.from[0].relation else {
        return Ok(None);
    };
    let Some(table_name) = object_name_terminal(name) else {
        return Ok(None);
    };
    if !table_name.eq_ignore_ascii_case(FILE_VIEW) {
        return Ok(None);
    }

    if select.prewhere.is_some() {
        return Ok(None);
    }
    if !group_by_expr_is_empty(&select.group_by)
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || select.qualify.is_some()
        || select.connect_by.is_some()
    {
        return Ok(None);
    }

    let projection_matches = select.projection.len() == 2
        && select_item_is_column(&select.projection[0], "path")
        && select_item_is_column(&select.projection[1], "data");
    if !projection_matches {
        return Ok(None);
    }

    let Some(selection) = select.selection.as_ref() else {
        return Ok(Some(FilePathDataFastPathKind::Scan));
    };

    let Some(path) = extract_path_equality_value(selection, params)? else {
        return Ok(None);
    };
    let Ok(parsed) = parse_file_path(&path) else {
        return Ok(None);
    };
    Ok(Some(FilePathDataFastPathKind::PointPath(
        parsed.normalized_path,
    )))
}

fn extract_path_equality_value(
    selection: &Expr,
    params: &[Value],
) -> Result<Option<String>, LixError> {
    let Expr::BinaryOp { left, op, right } = selection else {
        return Ok(None);
    };
    if *op != sqlparser::ast::BinaryOperator::Eq {
        return Ok(None);
    }

    let value_expr = if expr_is_column(left, "path") {
        right.as_ref()
    } else if expr_is_column(right, "path") {
        left.as_ref()
    } else {
        return Ok(None);
    };

    let mut placeholder_state = PlaceholderState::new();
    let resolved = resolve_expr_cell_with_state(value_expr, params, &mut placeholder_state)?;
    let Some(Value::Text(path)) = resolved.value else {
        return Ok(None);
    };
    Ok(Some(path))
}

fn group_by_expr_is_empty(group_by: &sqlparser::ast::GroupByExpr) -> bool {
    match group_by {
        sqlparser::ast::GroupByExpr::All(_) => false,
        sqlparser::ast::GroupByExpr::Expressions(expressions, _) => expressions.is_empty(),
    }
}

fn select_item_is_column(item: &SelectItem, column_name: &str) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) => expr_is_column(expr, column_name),
        SelectItem::ExprWithAlias { expr, alias } => {
            expr_is_column(expr, column_name) && alias.value.eq_ignore_ascii_case(column_name)
        }
        _ => false,
    }
}

fn expr_is_column(expr: &Expr, column_name: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(column_name),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|part| part.value.eq_ignore_ascii_case(column_name))
            .unwrap_or(false),
        _ => false,
    }
}

fn order_by_uses_only_path_or_data(query: &Query) -> bool {
    let Some(order_by) = query.order_by.as_ref() else {
        return true;
    };
    let sqlparser::ast::OrderByKind::Expressions(expressions) = &order_by.kind else {
        return false;
    };
    expressions.iter().all(|order_expr| {
        order_expr.with_fill.is_none()
            && (expr_is_column(&order_expr.expr, "path")
                || expr_is_column(&order_expr.expr, "data"))
    })
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

        assert!(rewritten.contains("COALESCE(fd.data, lix_empty_blob()) AS data"));
        assert!(rewritten.contains("FROM lix_state_by_version"));
        assert!(rewritten.contains("schema_key = 'lix_file_descriptor'"));
        assert!(rewritten.contains("FROM lix_internal_state_untracked"));
    }

    #[test]
    fn rewrites_simple_file_path_data_query_to_projection_fast_path() {
        let sql = "SELECT path, data FROM lix_file ORDER BY path";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query(query)
            .expect("rewrite")
            .expect("query should be rewritten")
            .to_string();

        assert!(rewritten.contains("COALESCE(fd.data, lix_empty_blob()) AS data"));
        assert!(rewritten.contains("FROM lix_state_by_version"));
        assert!(rewritten.contains("schema_key = 'lix_file_descriptor'"));
        assert!(
            !rewritten.contains("LEFT JOIN lix_version v"),
            "fast path should skip commit lookup join"
        );
    }

    #[test]
    fn rewrites_simple_file_path_data_point_query_with_param_fast_path() {
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

        assert!(rewritten.contains("'/bench/read/07/file-00007.txt' AS path"));
        assert!(rewritten.contains("dir_level_0"));
        assert!(rewritten.contains("dir_level_1"));
        assert!(rewritten.contains("lix_json_text(snapshot_content, 'name') = 'file-00007'"));
        assert!(
            !rewritten.contains("directory_paths"),
            "point fast path should avoid recursive directory path projection"
        );
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
