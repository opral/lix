use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, Query, Select,
    SelectItem, SetExpr, TableFactor, TableWithJoins,
};

use crate::engine::sql::planning::param_context::{
    normalize_query_placeholders, PlaceholderOrdinalState,
};
use crate::engine::sql::planning::rewrite_engine::steps::column_usage::{
    projected_lix_state_wrapper_columns, select_shape_is_complex,
};
use crate::engine::sql::planning::rewrite_engine::steps::state_columns::LIX_STATE_VISIBLE_COLUMNS;
use crate::engine::sql::planning::rewrite_engine::steps::state_pushdown::{
    retarget_pushdown_predicates, take_pushdown_predicates, StatePushdown,
};
use crate::engine::sql::planning::rewrite_engine::{
    default_alias, escape_sql_string, object_name_matches, parse_single_query,
    rewrite_query_with_select_rewriter,
};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
};
use crate::LixError;

const LIX_STATE_VIEW_NAME: &str = "lix_state";
const ACTIVE_VERSION_TABLE: &str = "lix_internal_state_untracked";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    let mut query = query;
    normalize_query_placeholders(&mut query, &mut PlaceholderOrdinalState::new())?;
    rewrite_query_with_select_rewriter(query, &mut rewrite_select)
}

fn rewrite_select(select: &mut Select, changed: &mut bool) -> Result<(), LixError> {
    let allow_unqualified = select.from.len() == 1 && select.from[0].joins.is_empty();
    let complex_shape = select_shape_is_complex(select);
    let projection = select.projection.clone();
    let mut selection = select.selection.clone();
    let prewhere = select.prewhere.clone();
    let having = select.having.clone();
    let qualify = select.qualify.clone();
    for table in &mut select.from {
        rewrite_table_with_joins(
            table,
            &projection,
            &mut selection,
            prewhere.as_ref(),
            having.as_ref(),
            qualify.as_ref(),
            allow_unqualified,
            complex_shape,
            changed,
        )?;
    }
    select.selection = selection;
    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    projection: &[SelectItem],
    selection: &mut Option<Expr>,
    prewhere: Option<&Expr>,
    having: Option<&Expr>,
    qualify: Option<&Expr>,
    allow_unqualified: bool,
    complex_shape: bool,
    changed: &mut bool,
) -> Result<(), LixError> {
    rewrite_table_factor(
        &mut table.relation,
        projection,
        selection,
        prewhere,
        having,
        qualify,
        allow_unqualified,
        complex_shape,
        changed,
    )?;
    for join in &mut table.joins {
        rewrite_table_factor(
            &mut join.relation,
            projection,
            selection,
            prewhere,
            having,
            qualify,
            false,
            true,
            changed,
        )?;
    }
    Ok(())
}

fn rewrite_table_factor(
    relation: &mut TableFactor,
    projection: &[SelectItem],
    selection: &mut Option<Expr>,
    prewhere: Option<&Expr>,
    having: Option<&Expr>,
    qualify: Option<&Expr>,
    allow_unqualified: bool,
    complex_shape: bool,
    changed: &mut bool,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if object_name_matches(name, LIX_STATE_VIEW_NAME) =>
        {
            let relation_name = alias
                .as_ref()
                .map(|value| value.name.value.clone())
                .unwrap_or_else(|| LIX_STATE_VIEW_NAME.to_string());
            validate_no_version_column_references(
                projection,
                selection.as_ref(),
                prewhere,
                having,
                qualify,
                &relation_name,
                allow_unqualified,
            )?;
            let pushdown_predicates = if allow_unqualified {
                let pushdown =
                    take_pushdown_predicates(selection, &relation_name, allow_unqualified);
                wrapper_pushdown_predicates(&pushdown)
            } else {
                Vec::new()
            };
            let projection_columns = if complex_shape {
                LIX_STATE_VISIBLE_COLUMNS.to_vec()
            } else {
                projected_lix_state_wrapper_columns(
                    projection,
                    selection.as_ref(),
                    prewhere,
                    having,
                    qualify,
                    &relation_name,
                    allow_unqualified,
                )
            };
            let derived_query = build_lix_state_wrapper_query(&projection_columns, &pushdown_predicates)?;
            let derived_alias = alias.clone().or_else(|| Some(default_lix_state_alias()));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            *changed = true;
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_with_joins(
                table_with_joins,
                projection,
                selection,
                prewhere,
                having,
                qualify,
                allow_unqualified,
                complex_shape,
                changed,
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_no_version_column_references(
    projection: &[SelectItem],
    selection: Option<&Expr>,
    prewhere: Option<&Expr>,
    having: Option<&Expr>,
    qualify: Option<&Expr>,
    relation_name: &str,
    allow_unqualified: bool,
) -> Result<(), LixError> {
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                if expr_references_lix_state_version_column(expr, relation_name, allow_unqualified)
                {
                    return Err(version_column_read_error());
                }
            }
            SelectItem::QualifiedWildcard(
                sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
                _,
            ) => {
                if expr_references_lix_state_version_column(expr, relation_name, allow_unqualified)
                {
                    return Err(version_column_read_error());
                }
            }
            _ => {}
        }
    }

    for expr in [selection, prewhere, having, qualify].into_iter().flatten() {
        if expr_references_lix_state_version_column(expr, relation_name, allow_unqualified) {
            return Err(version_column_read_error());
        }
    }

    Ok(())
}

fn expr_references_lix_state_version_column(
    expr: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
) -> bool {
    match expr {
        Expr::Identifier(ident) => {
            allow_unqualified
                && (ident.value.eq_ignore_ascii_case("version_id")
                    || ident.value.eq_ignore_ascii_case("lixcol_version_id"))
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = &parts[parts.len() - 2].value;
            let column = &parts[parts.len() - 1].value;
            qualifier.eq_ignore_ascii_case(relation_name)
                && (column.eq_ignore_ascii_case("version_id")
                    || column.eq_ignore_ascii_case("lixcol_version_id"))
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_references_lix_state_version_column(left, relation_name, allow_unqualified)
                || expr_references_lix_state_version_column(right, relation_name, allow_unqualified)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Nested(expr) => {
            expr_references_lix_state_version_column(expr, relation_name, allow_unqualified)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_lix_state_version_column(expr, relation_name, allow_unqualified)
                || expr_references_lix_state_version_column(low, relation_name, allow_unqualified)
                || expr_references_lix_state_version_column(high, relation_name, allow_unqualified)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            expr_references_lix_state_version_column(expr, relation_name, allow_unqualified)
                || expr_references_lix_state_version_column(
                    pattern,
                    relation_name,
                    allow_unqualified,
                )
        }
        Expr::InList { expr, list, .. } => {
            expr_references_lix_state_version_column(expr, relation_name, allow_unqualified)
                || list.iter().any(|item| {
                    expr_references_lix_state_version_column(item, relation_name, allow_unqualified)
                })
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_ref().is_some_and(|value| {
                expr_references_lix_state_version_column(value, relation_name, allow_unqualified)
            }) || conditions.iter().any(|condition| {
                expr_references_lix_state_version_column(
                    &condition.condition,
                    relation_name,
                    allow_unqualified,
                ) || expr_references_lix_state_version_column(
                    &condition.result,
                    relation_name,
                    allow_unqualified,
                )
            }) || else_result.as_ref().is_some_and(|value| {
                expr_references_lix_state_version_column(value, relation_name, allow_unqualified)
            })
        }
        Expr::Tuple(items) => items.iter().any(|item| {
            expr_references_lix_state_version_column(item, relation_name, allow_unqualified)
        }),
        Expr::Function(function) => match &function.args {
            FunctionArguments::List(list) => list.args.iter().any(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                    expr_references_lix_state_version_column(
                        inner,
                        relation_name,
                        allow_unqualified,
                    )
                }
                FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => match arg {
                    FunctionArgExpr::Expr(inner) => expr_references_lix_state_version_column(
                        inner,
                        relation_name,
                        allow_unqualified,
                    ),
                    _ => false,
                },
                _ => false,
            }),
            _ => false,
        },
        Expr::InSubquery { expr, .. } => {
            expr_references_lix_state_version_column(expr, relation_name, allow_unqualified)
        }
        _ => false,
    }
}

fn version_column_read_error() -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description:
            "lix_state does not expose version_id; use lix_state_by_version for explicit version reads"
                .to_string(),
    }
}

fn build_lix_state_wrapper_query(
    projected_columns: &[&str],
    extra_predicates: &[Expr],
) -> Result<Query, LixError> {
    let mut query = parse_single_query("SELECT 1 FROM lix_state_by_version AS s")?;
    let select = select_mut(&mut query)?;
    select.projection = projected_columns
        .iter()
        .map(|column| SelectItem::ExprWithAlias {
            expr: qualified_column_expr("s", column),
            alias: Ident::new(*column),
        })
        .collect();

    let mut predicates = vec![active_version_predicate_expr()?];
    predicates.extend(extra_predicates.iter().cloned());
    select.selection = join_with_and(predicates);

    Ok(query)
}

fn wrapper_pushdown_predicates(pushdown: &StatePushdown) -> Vec<Expr> {
    let mut out = Vec::new();
    out.extend(pushdown.source_predicates.iter().cloned());
    out.extend(retarget_pushdown_predicates(
        &pushdown.ranked_predicates,
        "ranked",
        "s",
    ));
    out
}

fn active_version_predicate_expr() -> Result<Expr, LixError> {
    let active_version_subquery = parse_single_query(&format!(
        "SELECT lix_json_extract(snapshot_content, 'version_id') \
         FROM {active_table} \
         WHERE schema_key = '{active_schema_key}' \
           AND file_id = '{active_file_id}' \
           AND version_id = '{active_storage_version_id}' \
           AND snapshot_content IS NOT NULL \
         ORDER BY updated_at DESC \
         LIMIT 1",
        active_table = ACTIVE_VERSION_TABLE,
        active_schema_key = escape_sql_string(active_version_schema_key()),
        active_file_id = escape_sql_string(active_version_file_id()),
        active_storage_version_id = escape_sql_string(active_version_storage_version_id()),
    ))?;

    Ok(Expr::BinaryOp {
        left: Box::new(qualified_column_expr("s", "version_id")),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Subquery(Box::new(active_version_subquery))),
    })
}

fn join_with_and(mut predicates: Vec<Expr>) -> Option<Expr> {
    if predicates.is_empty() {
        return None;
    }
    let mut current = predicates.remove(0);
    for predicate in predicates {
        current = Expr::BinaryOp {
            left: Box::new(current),
            op: BinaryOperator::And,
            right: Box::new(predicate),
        };
    }
    Some(current)
}

fn qualified_column_expr(qualifier: &str, column: &str) -> Expr {
    Expr::CompoundIdentifier(vec![Ident::new(qualifier), Ident::new(column)])
}

fn select_mut(query: &mut Query) -> Result<&mut Select, LixError> {
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected SELECT body when rewriting lix_state".to_string(),
        });
    };
    Ok(select.as_mut())
}

fn default_lix_state_alias() -> sqlparser::ast::TableAlias {
    default_alias(LIX_STATE_VIEW_NAME)
}

#[cfg(test)]
mod tests {
    use super::rewrite_query;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn rewrites_lix_state_to_by_version_wrapper_query() {
        let query = parse_query("SELECT entity_id, schema_key FROM lix_state");

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("FROM lix_state_by_version AS s"));
        assert!(sql.contains(
            "WHERE s.version_id = (SELECT lix_json_extract(snapshot_content, 'version_id')"
        ));
        assert!(!sql.contains("FROM lix_state "));
    }

    #[test]
    fn wrapper_projection_excludes_version_id_columns() {
        let query = parse_query("SELECT * FROM lix_state");

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("s.entity_id AS entity_id"));
        assert!(sql.contains("s.metadata AS metadata"));
        assert!(!sql.contains("s.version_id AS version_id"));
        assert!(!sql.contains("s.lixcol_version_id"));
    }

    #[test]
    fn wrapper_projection_omits_commit_when_not_referenced() {
        let query = parse_query("SELECT entity_id FROM lix_state");

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("s.commit_id AS commit_id"));
    }

    #[test]
    fn wrapper_projection_keeps_commit_when_referenced() {
        let query = parse_query("SELECT commit_id FROM lix_state");

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("s.commit_id AS commit_id"));
    }

    #[test]
    fn rejects_version_id_projection_for_lix_state() {
        let query = parse_query("SELECT version_id FROM lix_state");
        let error = rewrite_query(query).expect_err("version_id projection should be rejected");
        assert!(error.description.contains("does not expose version_id"));
    }

    #[test]
    fn rejects_lixcol_version_id_projection_for_lix_state() {
        let query = parse_query("SELECT lixcol_version_id FROM lix_state");
        let error =
            rewrite_query(query).expect_err("lixcol_version_id projection should be rejected");
        assert!(error.description.contains("does not expose version_id"));
    }

    #[test]
    fn rejects_version_id_predicate_for_lix_state() {
        let query = parse_query("SELECT entity_id FROM lix_state WHERE version_id = 'v-1'");
        let error = rewrite_query(query).expect_err("version predicate should be rejected");
        assert!(error.description.contains("does not expose version_id"));
    }

    #[test]
    fn pushes_schema_and_file_predicates_into_wrapper_query() {
        let query = parse_query(
            "SELECT entity_id \
             FROM lix_state \
             WHERE schema_key = 'lix_entity_label' \
               AND file_id = 'lix'",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("s.schema_key = 'lix_entity_label'"));
        assert!(sql.contains("s.file_id = 'lix'"));
        assert!(!sql.contains("WHERE schema_key = 'lix_entity_label'"));
    }

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("valid SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            _ => panic!("expected query"),
        }
    }
}
