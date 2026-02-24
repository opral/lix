use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, SetExpr, TableFactor,
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct ColumnReferenceOptions {
    pub include_from_derived_subqueries: bool,
}

impl Default for ColumnReferenceOptions {
    fn default() -> Self {
        Self {
            include_from_derived_subqueries: false,
        }
    }
}

pub(crate) fn expr_references_column_name(
    expr: &Expr,
    column: &str,
    options: ColumnReferenceOptions,
) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(column),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case(column))
            .unwrap_or(false),
        Expr::BinaryOp { left, right, .. } => {
            expr_references_column_name(left, column, options)
                || expr_references_column_name(right, column, options)
        }
        Expr::UnaryOp { expr, .. } => expr_references_column_name(expr, column, options),
        Expr::Nested(inner) => expr_references_column_name(inner, column, options),
        Expr::InList { expr, list, .. } => {
            expr_references_column_name(expr, column, options)
                || list
                    .iter()
                    .any(|item| expr_references_column_name(item, column, options))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_column_name(expr, column, options)
                || expr_references_column_name(low, column, options)
                || expr_references_column_name(high, column, options)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            expr_references_column_name(expr, column, options)
                || expr_references_column_name(pattern, column, options)
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            expr_references_column_name(inner, column, options)
        }
        Expr::Cast { expr, .. } => expr_references_column_name(expr, column, options),
        Expr::Function(function) => match &function.args {
            FunctionArguments::List(list) => list.args.iter().any(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                    expr_references_column_name(expr, column, options)
                }
                FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => match arg {
                    FunctionArgExpr::Expr(expr) => {
                        expr_references_column_name(expr, column, options)
                    }
                    _ => false,
                },
                _ => false,
            }),
            _ => false,
        },
        Expr::InSubquery { expr, subquery, .. } => {
            expr_references_column_name(expr, column, options)
                || query_references_column_name(subquery, column, options)
        }
        Expr::Subquery(subquery) | Expr::Exists { subquery, .. } => {
            query_references_column_name(subquery, column, options)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .is_some_and(|operand| expr_references_column_name(operand, column, options))
                || conditions.iter().any(|condition| {
                    expr_references_column_name(&condition.condition, column, options)
                        || expr_references_column_name(&condition.result, column, options)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|value| expr_references_column_name(value, column, options))
        }
        Expr::Tuple(items) => items
            .iter()
            .any(|item| expr_references_column_name(item, column, options)),
        _ => false,
    }
}

fn query_references_column_name(
    query: &Query,
    column: &str,
    options: ColumnReferenceOptions,
) -> bool {
    if query_set_expr_references_column_name(query.body.as_ref(), column, options) {
        return true;
    }

    query.with.as_ref().is_some_and(|with| {
        with.cte_tables
            .iter()
            .any(|cte| query_references_column_name(&cte.query, column, options))
    })
}

fn query_set_expr_references_column_name(
    expr: &SetExpr,
    column: &str,
    options: ColumnReferenceOptions,
) -> bool {
    match expr {
        SetExpr::Select(select) => {
            select.projection.iter().any(|item| match item {
                sqlparser::ast::SelectItem::UnnamedExpr(expr)
                | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                    expr_references_column_name(expr, column, options)
                }
                sqlparser::ast::SelectItem::QualifiedWildcard(
                    sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
                    _,
                ) => expr_references_column_name(expr, column, options),
                _ => false,
            }) || select
                .selection
                .as_ref()
                .is_some_and(|expr| expr_references_column_name(expr, column, options))
                || select
                    .prewhere
                    .as_ref()
                    .is_some_and(|expr| expr_references_column_name(expr, column, options))
                || select
                    .having
                    .as_ref()
                    .is_some_and(|expr| expr_references_column_name(expr, column, options))
                || select
                    .qualify
                    .as_ref()
                    .is_some_and(|expr| expr_references_column_name(expr, column, options))
                || (options.include_from_derived_subqueries
                    && select.from.iter().any(|table| {
                        table_factor_references_column_name(&table.relation, column, options)
                            || table.joins.iter().any(|join| {
                                table_factor_references_column_name(&join.relation, column, options)
                            })
                    }))
        }
        SetExpr::Query(query) => query_references_column_name(query, column, options),
        SetExpr::SetOperation { left, right, .. } => {
            query_set_expr_references_column_name(left.as_ref(), column, options)
                || query_set_expr_references_column_name(right.as_ref(), column, options)
        }
        SetExpr::Values(values) => values
            .rows
            .iter()
            .flatten()
            .any(|expr| expr_references_column_name(expr, column, options)),
        _ => false,
    }
}

fn table_factor_references_column_name(
    relation: &TableFactor,
    column: &str,
    options: ColumnReferenceOptions,
) -> bool {
    match relation {
        TableFactor::Derived { subquery, .. } => {
            query_references_column_name(subquery, column, options)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            table_factor_references_column_name(&table_with_joins.relation, column, options)
                || table_with_joins.joins.iter().any(|join| {
                    table_factor_references_column_name(&join.relation, column, options)
                })
        }
        _ => false,
    }
}
