use crate::sql::logical_plan::direct_reads::{
    DirectPublicReadPlan, DirectoryHistoryDirectReadPlan, FileHistoryDirectReadPlan,
    StateHistoryDirectReadPlan,
};
use crate::sql::logical_plan::plan::{
    InternalLogicalPlan, LogicalPlan, PublicReadLogicalPlan, PublicWriteLogicalPlan,
};
use crate::sql::logical_plan::public_ir::{
    BroadPublicReadQuery, BroadPublicReadRelation, BroadPublicReadSetExpr,
    BroadPublicReadStatement, BroadPublicReadTableFactor, BroadPublicReadTableWithJoins,
};
use crate::sql::logical_plan::result_contract::ResultContract;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, OrderBy, OrderByExpr,
    Query, Select, SelectItem, SetExpr,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogicalPlanVerificationError {
    pub(crate) message: String,
}

impl LogicalPlanVerificationError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub(crate) fn verify_logical_plan(plan: &LogicalPlan) -> Result<(), LogicalPlanVerificationError> {
    match plan {
        LogicalPlan::PublicRead(plan) => verify_public_read_logical_plan(plan),
        LogicalPlan::PublicWrite(plan) => verify_public_write_logical_plan(plan),
        LogicalPlan::Internal(plan) => verify_internal_logical_plan(plan),
    }
}

pub(crate) fn verify_public_read_logical_plan(
    plan: &PublicReadLogicalPlan,
) -> Result<(), LogicalPlanVerificationError> {
    match plan {
        PublicReadLogicalPlan::Structured { read, .. } => {
            if read.surface_binding.descriptor.public_name.is_empty() {
                return Err(LogicalPlanVerificationError::new(
                    "structured public read must target a named surface",
                ));
            }
        }
        PublicReadLogicalPlan::DirectHistory {
            read, direct_plan, ..
        } => {
            if read.surface_binding.descriptor.public_name.is_empty() {
                return Err(LogicalPlanVerificationError::new(
                    "direct history read must target a named surface",
                ));
            }
            verify_direct_public_read_plan(direct_plan)?;
        }
        PublicReadLogicalPlan::Broad {
            broad_statement,
            surface_bindings,
            ..
        } => {
            if surface_bindings.is_empty() {
                return Err(LogicalPlanVerificationError::new(
                    "broad public read logical plan must record at least one bound surface",
                ));
            }
            if !broad_public_read_statement_has_typed_surface_relation(broad_statement) {
                return Err(LogicalPlanVerificationError::new(
                    "broad public read logical plan must retain typed public surface relation structure",
                ));
            }
        }
    }

    Ok(())
}

fn broad_public_read_statement_has_typed_surface_relation(
    statement: &BroadPublicReadStatement,
) -> bool {
    match statement {
        BroadPublicReadStatement::Query(query) => {
            broad_public_read_query_has_typed_surface_relation(query)
        }
        BroadPublicReadStatement::Explain { statement, .. } => {
            broad_public_read_statement_has_typed_surface_relation(statement)
        }
    }
}

fn broad_public_read_query_has_typed_surface_relation(query: &BroadPublicReadQuery) -> bool {
    query.with.as_ref().is_some_and(|with| {
        with.cte_tables
            .iter()
            .any(broad_public_read_query_has_typed_surface_relation)
    }) || broad_public_read_set_expr_has_typed_surface_relation(&query.body)
        || query_has_nested_query_expressions(&query.original)
}

fn broad_public_read_set_expr_has_typed_surface_relation(expr: &BroadPublicReadSetExpr) -> bool {
    match expr {
        BroadPublicReadSetExpr::Select(select) => select
            .from
            .iter()
            .any(broad_public_read_table_with_joins_has_typed_surface_relation),
        BroadPublicReadSetExpr::Query(query) => {
            broad_public_read_query_has_typed_surface_relation(query)
        }
        BroadPublicReadSetExpr::SetOperation { left, right, .. } => {
            broad_public_read_set_expr_has_typed_surface_relation(left)
                || broad_public_read_set_expr_has_typed_surface_relation(right)
        }
        BroadPublicReadSetExpr::Table { relation, .. } => {
            matches!(
                relation,
                BroadPublicReadRelation::Public(_) | BroadPublicReadRelation::LoweredPublic(_)
            )
        }
        BroadPublicReadSetExpr::Other(_) => false,
    }
}

fn broad_public_read_table_with_joins_has_typed_surface_relation(
    table: &BroadPublicReadTableWithJoins,
) -> bool {
    broad_public_read_table_factor_has_typed_surface_relation(&table.relation)
        || table
            .joins
            .iter()
            .any(|join| broad_public_read_table_factor_has_typed_surface_relation(&join.relation))
}

fn broad_public_read_table_factor_has_typed_surface_relation(
    factor: &BroadPublicReadTableFactor,
) -> bool {
    match factor {
        BroadPublicReadTableFactor::Table { relation, .. } => {
            matches!(
                relation,
                BroadPublicReadRelation::Public(_) | BroadPublicReadRelation::LoweredPublic(_)
            )
        }
        BroadPublicReadTableFactor::Derived { subquery, .. } => {
            broad_public_read_query_has_typed_surface_relation(subquery)
        }
        BroadPublicReadTableFactor::NestedJoin {
            table_with_joins, ..
        } => broad_public_read_table_with_joins_has_typed_surface_relation(table_with_joins),
        BroadPublicReadTableFactor::Other(_) => false,
    }
}

fn query_has_nested_query_expressions(query: &Query) -> bool {
    set_expr_has_nested_query_expressions(query.body.as_ref())
        || query
            .order_by
            .as_ref()
            .is_some_and(order_by_has_nested_query_expressions)
        || query
            .limit_clause
            .as_ref()
            .is_some_and(|limit_clause| match limit_clause {
                sqlparser::ast::LimitClause::LimitOffset {
                    limit,
                    offset,
                    limit_by,
                } => {
                    limit
                        .as_ref()
                        .is_some_and(expr_has_nested_query_expressions)
                        || offset
                            .as_ref()
                            .is_some_and(|offset| expr_has_nested_query_expressions(&offset.value))
                        || limit_by.iter().any(expr_has_nested_query_expressions)
                }
                sqlparser::ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                    expr_has_nested_query_expressions(offset)
                        || expr_has_nested_query_expressions(limit)
                }
            })
        || query.fetch.as_ref().is_some_and(|fetch| {
            fetch
                .quantity
                .as_ref()
                .is_some_and(expr_has_nested_query_expressions)
        })
}

fn set_expr_has_nested_query_expressions(expr: &SetExpr) -> bool {
    match expr {
        SetExpr::Select(select) => select_has_nested_query_expressions(select),
        SetExpr::Query(query) => query_has_nested_query_expressions(query),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_has_nested_query_expressions(left)
                || set_expr_has_nested_query_expressions(right)
        }
        SetExpr::Values(values) => values
            .rows
            .iter()
            .flatten()
            .any(expr_has_nested_query_expressions),
        _ => false,
    }
}

fn select_has_nested_query_expressions(select: &Select) -> bool {
    select
        .projection
        .iter()
        .any(select_item_has_nested_query_expressions)
        || select
            .prewhere
            .as_ref()
            .is_some_and(expr_has_nested_query_expressions)
        || select
            .selection
            .as_ref()
            .is_some_and(expr_has_nested_query_expressions)
        || match &select.group_by {
            GroupByExpr::All(_) => false,
            GroupByExpr::Expressions(expressions, _) => {
                expressions.iter().any(expr_has_nested_query_expressions)
            }
        }
        || select
            .cluster_by
            .iter()
            .any(expr_has_nested_query_expressions)
        || select
            .distribute_by
            .iter()
            .any(expr_has_nested_query_expressions)
        || select
            .sort_by
            .iter()
            .any(order_by_expr_has_nested_query_expressions)
        || select
            .having
            .as_ref()
            .is_some_and(expr_has_nested_query_expressions)
        || select
            .qualify
            .as_ref()
            .is_some_and(expr_has_nested_query_expressions)
        || select.connect_by.as_ref().is_some_and(|connect_by| {
            expr_has_nested_query_expressions(&connect_by.condition)
                || connect_by
                    .relationships
                    .iter()
                    .any(expr_has_nested_query_expressions)
        })
}

fn select_item_has_nested_query_expressions(item: &SelectItem) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_has_nested_query_expressions(expr)
        }
        SelectItem::QualifiedWildcard(
            sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
            _,
        ) => expr_has_nested_query_expressions(expr),
        _ => false,
    }
}

fn order_by_has_nested_query_expressions(order_by: &OrderBy) -> bool {
    match &order_by.kind {
        sqlparser::ast::OrderByKind::Expressions(expressions) => expressions
            .iter()
            .any(order_by_expr_has_nested_query_expressions),
        sqlparser::ast::OrderByKind::All(_) => false,
    }
}

fn order_by_expr_has_nested_query_expressions(order_by_expr: &OrderByExpr) -> bool {
    expr_has_nested_query_expressions(&order_by_expr.expr)
        || order_by_expr.with_fill.as_ref().is_some_and(|with_fill| {
            with_fill
                .from
                .as_ref()
                .is_some_and(expr_has_nested_query_expressions)
                || with_fill
                    .to
                    .as_ref()
                    .is_some_and(expr_has_nested_query_expressions)
                || with_fill
                    .step
                    .as_ref()
                    .is_some_and(expr_has_nested_query_expressions)
        })
}

fn expr_has_nested_query_expressions(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(_) | Expr::Exists { .. } => true,
        Expr::InSubquery { expr, subquery, .. } => {
            expr_has_nested_query_expressions(expr) || query_has_nested_query_expressions(subquery)
        }
        Expr::BinaryOp { left, right, .. }
        | Expr::AnyOp { left, right, .. }
        | Expr::AllOp { left, right, .. } => {
            expr_has_nested_query_expressions(left) || expr_has_nested_query_expressions(right)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => expr_has_nested_query_expressions(expr),
        Expr::InList { expr, list, .. } => {
            expr_has_nested_query_expressions(expr)
                || list.iter().any(expr_has_nested_query_expressions)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_has_nested_query_expressions(expr)
                || expr_has_nested_query_expressions(low)
                || expr_has_nested_query_expressions(high)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            expr_has_nested_query_expressions(expr) || expr_has_nested_query_expressions(pattern)
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            expr_has_nested_query_expressions(expr) || expr_has_nested_query_expressions(array_expr)
        }
        Expr::Function(function) => match &function.args {
            FunctionArguments::List(list) => list.args.iter().any(function_arg_has_nested_query),
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
                .is_some_and(|expr| expr_has_nested_query_expressions(expr.as_ref()))
                || conditions.iter().any(|condition| {
                    expr_has_nested_query_expressions(&condition.condition)
                        || expr_has_nested_query_expressions(&condition.result)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|expr| expr_has_nested_query_expressions(expr.as_ref()))
        }
        Expr::Tuple(items) => items.iter().any(expr_has_nested_query_expressions),
        _ => false,
    }
}

fn function_arg_has_nested_query(arg: &FunctionArg) -> bool {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
            expr_has_nested_query_expressions(expr)
        }
        FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
            matches!(arg, FunctionArgExpr::Expr(expr) if expr_has_nested_query_expressions(expr))
        }
        _ => false,
    }
}

pub(crate) fn verify_public_write_logical_plan(
    plan: &PublicWriteLogicalPlan,
) -> Result<(), LogicalPlanVerificationError> {
    if plan
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        .is_empty()
    {
        return Err(LogicalPlanVerificationError::new(
            "planned write must target a named surface",
        ));
    }

    Ok(())
}

pub(crate) fn verify_internal_logical_plan(
    plan: &InternalLogicalPlan,
) -> Result<(), LogicalPlanVerificationError> {
    if plan.normalized_statements.prepared_statements.is_empty()
        && !matches!(plan.result_contract, ResultContract::DmlNoReturning)
        && plan.normalized_statements.mutations.is_empty()
        && plan.normalized_statements.update_validations.is_empty()
    {
        return Err(LogicalPlanVerificationError::new(
            "internal logical plan must contain statements or explicit internal effects",
        ));
    }

    Ok(())
}

fn verify_direct_public_read_plan(
    plan: &DirectPublicReadPlan,
) -> Result<(), LogicalPlanVerificationError> {
    match plan {
        DirectPublicReadPlan::StateHistory(plan) => verify_state_history_direct_plan(plan),
        DirectPublicReadPlan::EntityHistory(plan) => {
            if plan.surface_binding.descriptor.public_name.is_empty() {
                Err(LogicalPlanVerificationError::new(
                    "entity history direct read must target a named surface",
                ))
            } else {
                Ok(())
            }
        }
        DirectPublicReadPlan::FileHistory(plan) => verify_file_history_direct_plan(plan),
        DirectPublicReadPlan::DirectoryHistory(plan) => verify_directory_history_direct_plan(plan),
    }
}

fn verify_state_history_direct_plan(
    plan: &StateHistoryDirectReadPlan,
) -> Result<(), LogicalPlanVerificationError> {
    if plan.having.is_some() && plan.group_by_fields.is_empty() && plan.projections.is_empty() {
        return Err(LogicalPlanVerificationError::new(
            "state history aggregate predicates require grouped or projected inputs",
        ));
    }

    Ok(())
}

fn verify_file_history_direct_plan(
    plan: &FileHistoryDirectReadPlan,
) -> Result<(), LogicalPlanVerificationError> {
    match (&plan.aggregate, &plan.aggregate_output_name) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        (Some(_), None) => Err(LogicalPlanVerificationError::new(
            "file history aggregate requires an output name",
        )),
        (None, Some(_)) => Err(LogicalPlanVerificationError::new(
            "file history aggregate output name requires an aggregate",
        )),
    }
}

fn verify_directory_history_direct_plan(
    plan: &DirectoryHistoryDirectReadPlan,
) -> Result<(), LogicalPlanVerificationError> {
    match (&plan.aggregate, &plan.aggregate_output_name) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        (Some(_), None) => Err(LogicalPlanVerificationError::new(
            "directory history aggregate requires an output name",
        )),
        (None, Some(_)) => Err(LogicalPlanVerificationError::new(
            "directory history aggregate output name requires an aggregate",
        )),
    }
}
