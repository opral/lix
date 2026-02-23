use super::super::ast::nodes::Statement;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, ObjectNamePart, Query,
    Select, SetExpr, TableFactor, TableWithJoins, Update, UpdateTableFromKind,
};

#[derive(Debug, Clone, Copy)]
struct ColumnReferenceOptions {
    include_from_derived_subqueries: bool,
}

impl Default for ColumnReferenceOptions {
    fn default() -> Self {
        Self {
            include_from_derived_subqueries: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileReadMaterializationScope {
    ActiveVersionOnly,
    AllVersions,
}

pub(crate) fn file_read_materialization_scope_for_statements(
    statements: &[Statement],
) -> Option<FileReadMaterializationScope> {
    let mut scope = None;
    for statement in statements {
        let Some(statement_scope) = file_read_materialization_scope_for_statement(statement) else {
            continue;
        };
        match statement_scope {
            FileReadMaterializationScope::AllVersions => {
                return Some(FileReadMaterializationScope::AllVersions);
            }
            FileReadMaterializationScope::ActiveVersionOnly => {
                scope.get_or_insert(FileReadMaterializationScope::ActiveVersionOnly);
            }
        }
    }
    scope
}

pub(crate) fn file_history_read_materialization_required_for_statements(
    statements: &[Statement],
) -> bool {
    statements
        .iter()
        .any(file_history_read_materialization_required_for_statement)
}

fn file_history_read_materialization_required_for_statement(statement: &Statement) -> bool {
    statement_reads_table_name(statement, "lix_file_history")
}

fn file_read_materialization_scope_for_statement(
    statement: &Statement,
) -> Option<FileReadMaterializationScope> {
    let mentions_by_version = statement_reads_table_name(statement, "lix_file_by_version");
    if mentions_by_version {
        return Some(FileReadMaterializationScope::AllVersions);
    }
    if statement_reads_table_name(statement, "lix_file") {
        return Some(FileReadMaterializationScope::ActiveVersionOnly);
    }
    None
}

fn statement_reads_table_name(statement: &Statement, table_name: &str) -> bool {
    match statement {
        Statement::Query(query) => query_mentions_table_name(query, table_name),
        Statement::Insert(insert) => insert
            .source
            .as_deref()
            .is_some_and(|query| query_mentions_table_name(query, table_name)),
        Statement::Update(update) => {
            let target_matches = table_with_joins_mentions_table_name(&update.table, table_name);
            (target_matches && update_references_data_column(update))
                || update.from.as_ref().is_some_and(|from| match from {
                    UpdateTableFromKind::BeforeSet(from) | UpdateTableFromKind::AfterSet(from) => {
                        from.iter()
                            .any(|table| table_with_joins_mentions_table_name(table, table_name))
                    }
                })
                || update
                    .selection
                    .as_ref()
                    .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
                || update
                    .assignments
                    .iter()
                    .any(|assignment| expr_mentions_table_name(&assignment.value, table_name))
        }
        Statement::Delete(delete) => {
            delete.using.as_ref().is_some_and(|tables| {
                tables
                    .iter()
                    .any(|table| table_with_joins_mentions_table_name(table, table_name))
            }) || delete
                .selection
                .as_ref()
                .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
        }
        _ => false,
    }
}

fn update_references_data_column(update: &Update) -> bool {
    update
        .selection
        .as_ref()
        .is_some_and(expr_references_data_column)
        || update
            .assignments
            .iter()
            .any(|assignment| expr_references_data_column(&assignment.value))
}

fn expr_references_data_column(expr: &Expr) -> bool {
    expr_references_column_name(
        expr,
        "data",
        ColumnReferenceOptions {
            include_from_derived_subqueries: true,
        },
    )
}

fn query_mentions_table_name(query: &Query, table_name: &str) -> bool {
    if query_set_expr_mentions_table_name(query.body.as_ref(), table_name) {
        return true;
    }

    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            if query_mentions_table_name(&cte.query, table_name) {
                return true;
            }
        }
    }

    if query
        .order_by
        .as_ref()
        .is_some_and(|order_by| order_by_mentions_table_name(order_by, table_name))
    {
        return true;
    }

    if query
        .limit_clause
        .as_ref()
        .is_some_and(|limit_clause| limit_clause_mentions_table_name(limit_clause, table_name))
    {
        return true;
    }

    if query
        .fetch
        .as_ref()
        .and_then(|fetch| fetch.quantity.as_ref())
        .is_some_and(|quantity| expr_mentions_table_name(quantity, table_name))
    {
        return true;
    }

    false
}

fn query_set_expr_mentions_table_name(expr: &SetExpr, table_name: &str) -> bool {
    match expr {
        SetExpr::Select(select) => select_mentions_table_name(select, table_name),
        SetExpr::Query(query) => query_mentions_table_name(query, table_name),
        SetExpr::SetOperation { left, right, .. } => {
            query_set_expr_mentions_table_name(left.as_ref(), table_name)
                || query_set_expr_mentions_table_name(right.as_ref(), table_name)
        }
        SetExpr::Values(values) => values
            .rows
            .iter()
            .flatten()
            .any(|expr| expr_mentions_table_name(expr, table_name)),
        SetExpr::Insert(statement)
        | SetExpr::Update(statement)
        | SetExpr::Delete(statement)
        | SetExpr::Merge(statement) => statement_reads_table_name(statement, table_name),
        SetExpr::Table(table) => table
            .table_name
            .as_ref()
            .is_some_and(|name| name.eq_ignore_ascii_case(table_name)),
    }
}

fn select_mentions_table_name(select: &Select, table_name: &str) -> bool {
    if select
        .from
        .iter()
        .any(|table| table_with_joins_mentions_table_name(table, table_name))
    {
        return true;
    }

    if select
        .projection
        .iter()
        .any(|item| select_item_mentions_table_name(item, table_name))
    {
        return true;
    }

    if select
        .prewhere
        .as_ref()
        .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select
        .selection
        .as_ref()
        .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if group_by_expr_mentions_table_name(&select.group_by, table_name) {
        return true;
    }

    if select
        .cluster_by
        .iter()
        .any(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select
        .distribute_by
        .iter()
        .any(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select
        .sort_by
        .iter()
        .any(|order_by_expr| order_by_expr_mentions_table_name(order_by_expr, table_name))
    {
        return true;
    }

    if select
        .having
        .as_ref()
        .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select
        .qualify
        .as_ref()
        .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select.connect_by.as_ref().is_some_and(|connect_by| {
        expr_mentions_table_name(&connect_by.condition, table_name)
            || connect_by
                .relationships
                .iter()
                .any(|expr| expr_mentions_table_name(expr, table_name))
    }) {
        return true;
    }

    false
}

fn table_with_joins_mentions_table_name(table: &TableWithJoins, table_name: &str) -> bool {
    if table_factor_mentions_table_name(&table.relation, table_name) {
        return true;
    }

    table.joins.iter().any(|join| {
        table_factor_mentions_table_name(&join.relation, table_name)
            || join_operator_mentions_table_name(&join.join_operator, table_name)
    })
}

fn table_factor_mentions_table_name(table: &TableFactor, table_name: &str) -> bool {
    match table {
        TableFactor::Table { name, .. } => object_name_matches_table_name(name, table_name),
        TableFactor::Derived { subquery, .. } => query_mentions_table_name(subquery, table_name),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => table_with_joins_mentions_table_name(table_with_joins, table_name),
        _ => false,
    }
}

fn select_item_mentions_table_name(item: &sqlparser::ast::SelectItem, table_name: &str) -> bool {
    match item {
        sqlparser::ast::SelectItem::UnnamedExpr(expr)
        | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
            expr_mentions_table_name(expr, table_name)
        }
        sqlparser::ast::SelectItem::QualifiedWildcard(
            sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
            _,
        ) => expr_mentions_table_name(expr, table_name),
        _ => false,
    }
}

fn group_by_expr_mentions_table_name(
    group_by: &sqlparser::ast::GroupByExpr,
    table_name: &str,
) -> bool {
    match group_by {
        sqlparser::ast::GroupByExpr::All(_) => false,
        sqlparser::ast::GroupByExpr::Expressions(expressions, _) => expressions
            .iter()
            .any(|expr| expr_mentions_table_name(expr, table_name)),
    }
}

fn order_by_mentions_table_name(order_by: &sqlparser::ast::OrderBy, table_name: &str) -> bool {
    match &order_by.kind {
        sqlparser::ast::OrderByKind::All(_) => false,
        sqlparser::ast::OrderByKind::Expressions(expressions) => expressions
            .iter()
            .any(|expr| order_by_expr_mentions_table_name(expr, table_name)),
    }
}

fn order_by_expr_mentions_table_name(
    order_by_expr: &sqlparser::ast::OrderByExpr,
    table_name: &str,
) -> bool {
    if expr_mentions_table_name(&order_by_expr.expr, table_name) {
        return true;
    }

    order_by_expr.with_fill.as_ref().is_some_and(|with_fill| {
        with_fill
            .from
            .as_ref()
            .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
            || with_fill
                .to
                .as_ref()
                .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
            || with_fill
                .step
                .as_ref()
                .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    })
}

fn limit_clause_mentions_table_name(
    limit_clause: &sqlparser::ast::LimitClause,
    table_name: &str,
) -> bool {
    match limit_clause {
        sqlparser::ast::LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            limit
                .as_ref()
                .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
                || offset
                    .as_ref()
                    .is_some_and(|offset| expr_mentions_table_name(&offset.value, table_name))
                || limit_by
                    .iter()
                    .any(|expr| expr_mentions_table_name(expr, table_name))
        }
        sqlparser::ast::LimitClause::OffsetCommaLimit { offset, limit } => {
            expr_mentions_table_name(offset, table_name)
                || expr_mentions_table_name(limit, table_name)
        }
    }
}

fn join_operator_mentions_table_name(
    join_operator: &sqlparser::ast::JoinOperator,
    table_name: &str,
) -> bool {
    let (match_condition, constraint) = match join_operator {
        sqlparser::ast::JoinOperator::AsOf {
            match_condition,
            constraint,
        } => (Some(match_condition), Some(constraint)),
        sqlparser::ast::JoinOperator::Join(constraint)
        | sqlparser::ast::JoinOperator::Inner(constraint)
        | sqlparser::ast::JoinOperator::Left(constraint)
        | sqlparser::ast::JoinOperator::LeftOuter(constraint)
        | sqlparser::ast::JoinOperator::Right(constraint)
        | sqlparser::ast::JoinOperator::RightOuter(constraint)
        | sqlparser::ast::JoinOperator::FullOuter(constraint)
        | sqlparser::ast::JoinOperator::CrossJoin(constraint)
        | sqlparser::ast::JoinOperator::Semi(constraint)
        | sqlparser::ast::JoinOperator::LeftSemi(constraint)
        | sqlparser::ast::JoinOperator::RightSemi(constraint)
        | sqlparser::ast::JoinOperator::Anti(constraint)
        | sqlparser::ast::JoinOperator::LeftAnti(constraint)
        | sqlparser::ast::JoinOperator::RightAnti(constraint)
        | sqlparser::ast::JoinOperator::StraightJoin(constraint) => (None, Some(constraint)),
        sqlparser::ast::JoinOperator::CrossApply | sqlparser::ast::JoinOperator::OuterApply => {
            (None, None)
        }
    };

    match_condition.is_some_and(|expr| expr_mentions_table_name(expr, table_name))
        || constraint
            .is_some_and(|constraint| join_constraint_mentions_table_name(constraint, table_name))
}

fn join_constraint_mentions_table_name(
    constraint: &sqlparser::ast::JoinConstraint,
    table_name: &str,
) -> bool {
    match constraint {
        sqlparser::ast::JoinConstraint::On(expr) => expr_mentions_table_name(expr, table_name),
        _ => false,
    }
}

fn expr_mentions_table_name(expr: &Expr, table_name: &str) -> bool {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            expr_mentions_table_name(left, table_name)
                || expr_mentions_table_name(right, table_name)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => expr_mentions_table_name(expr, table_name),
        Expr::InList { expr, list, .. } => {
            expr_mentions_table_name(expr, table_name)
                || list
                    .iter()
                    .any(|item| expr_mentions_table_name(item, table_name))
        }
        Expr::InSubquery { expr, subquery, .. } => {
            expr_mentions_table_name(expr, table_name)
                || query_mentions_table_name(subquery, table_name)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_mentions_table_name(expr, table_name)
                || expr_mentions_table_name(low, table_name)
                || expr_mentions_table_name(high, table_name)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            expr_mentions_table_name(expr, table_name)
                || expr_mentions_table_name(pattern, table_name)
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            expr_mentions_table_name(expr, table_name)
                || expr_mentions_table_name(array_expr, table_name)
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            expr_mentions_table_name(left, table_name)
                || expr_mentions_table_name(right, table_name)
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => {
            query_mentions_table_name(subquery, table_name)
        }
        Expr::Function(function) => match &function.args {
            FunctionArguments::List(list) => list.args.iter().any(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                    expr_mentions_table_name(expr, table_name)
                }
                FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => match arg {
                    FunctionArgExpr::Expr(expr) => expr_mentions_table_name(expr, table_name),
                    _ => false,
                },
                _ => false,
            }),
            _ => false,
        },
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .is_some_and(|operand| expr_mentions_table_name(operand, table_name))
                || conditions.iter().any(|condition| {
                    expr_mentions_table_name(&condition.condition, table_name)
                        || expr_mentions_table_name(&condition.result, table_name)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|value| expr_mentions_table_name(value, table_name))
        }
        Expr::Tuple(items) => items
            .iter()
            .any(|item| expr_mentions_table_name(item, table_name)),
        _ => false,
    }
}

fn object_name_matches_table_name(name: &ObjectName, table_name: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(table_name))
        .unwrap_or(false)
}

fn expr_references_column_name(expr: &Expr, column: &str, options: ColumnReferenceOptions) -> bool {
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
