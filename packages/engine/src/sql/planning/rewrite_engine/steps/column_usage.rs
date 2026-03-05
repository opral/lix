use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, SelectItem,
};

use crate::engine::sql::planning::rewrite_engine::steps::state_columns::lix_state_visible_columns_without_commit;

pub(crate) fn projected_lix_state_wrapper_columns(
    projection: &[SelectItem],
    selection: Option<&Expr>,
    prewhere: Option<&Expr>,
    having: Option<&Expr>,
    qualify: Option<&Expr>,
    relation_name: &str,
    allow_unqualified: bool,
) -> Vec<&'static str> {
    let include_commit = query_shape_requires_commit_column(
        projection,
        selection,
        prewhere,
        having,
        qualify,
        relation_name,
        allow_unqualified,
    );

    let mut columns = lix_state_visible_columns_without_commit();
    if include_commit {
        columns.push("commit_id");
    }
    columns
}

fn query_shape_requires_commit_column(
    projection: &[SelectItem],
    selection: Option<&Expr>,
    prewhere: Option<&Expr>,
    having: Option<&Expr>,
    qualify: Option<&Expr>,
    relation_name: &str,
    allow_unqualified: bool,
) -> bool {
    if projection.iter().any(|item| match item {
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => true,
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_references_relation_commit_column(expr, relation_name, allow_unqualified)
        }
    }) {
        return true;
    }

    for expr in [selection, prewhere, having, qualify].into_iter().flatten() {
        if expr_references_relation_commit_column(expr, relation_name, allow_unqualified) {
            return true;
        }
    }

    false
}

fn expr_references_relation_commit_column(
    expr: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
) -> bool {
    match expr {
        Expr::Identifier(ident) => {
            allow_unqualified
                && (ident.value.eq_ignore_ascii_case("commit_id")
                    || ident.value.eq_ignore_ascii_case("lixcol_commit_id"))
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = &parts[parts.len() - 2].value;
            let column = &parts[parts.len() - 1].value;
            qualifier.eq_ignore_ascii_case(relation_name)
                && (column.eq_ignore_ascii_case("commit_id")
                    || column.eq_ignore_ascii_case("lixcol_commit_id"))
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_references_relation_commit_column(left, relation_name, allow_unqualified)
                || expr_references_relation_commit_column(right, relation_name, allow_unqualified)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Nested(expr) => {
            expr_references_relation_commit_column(expr, relation_name, allow_unqualified)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_relation_commit_column(expr, relation_name, allow_unqualified)
                || expr_references_relation_commit_column(low, relation_name, allow_unqualified)
                || expr_references_relation_commit_column(high, relation_name, allow_unqualified)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            expr_references_relation_commit_column(expr, relation_name, allow_unqualified)
                || expr_references_relation_commit_column(pattern, relation_name, allow_unqualified)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_relation_commit_column(expr, relation_name, allow_unqualified)
                || list.iter().any(|item| {
                    expr_references_relation_commit_column(item, relation_name, allow_unqualified)
                })
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_ref().is_some_and(|value| {
                expr_references_relation_commit_column(value, relation_name, allow_unqualified)
            }) || conditions.iter().any(|condition| {
                expr_references_relation_commit_column(
                    &condition.condition,
                    relation_name,
                    allow_unqualified,
                ) || expr_references_relation_commit_column(
                    &condition.result,
                    relation_name,
                    allow_unqualified,
                )
            }) || else_result.as_ref().is_some_and(|value| {
                expr_references_relation_commit_column(value, relation_name, allow_unqualified)
            })
        }
        Expr::Tuple(items) => items.iter().any(|item| {
            expr_references_relation_commit_column(item, relation_name, allow_unqualified)
        }),
        Expr::Function(function) => match &function.args {
            FunctionArguments::List(list) => list.args.iter().any(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                    expr_references_relation_commit_column(inner, relation_name, allow_unqualified)
                }
                FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => match arg {
                    FunctionArgExpr::Expr(inner) => expr_references_relation_commit_column(
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
            expr_references_relation_commit_column(expr, relation_name, allow_unqualified)
        }
        _ => false,
    }
}

pub(crate) fn select_shape_is_complex(select: &sqlparser::ast::Select) -> bool {
    if select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || !select.named_window.is_empty()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        return true;
    }

    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) => !exprs.is_empty() || !modifiers.is_empty(),
        GroupByExpr::All(_) => true,
    }
}
