use crate::sql::logical_plan::direct_reads::{
    DirectPublicReadPlan, DirectoryHistoryDirectReadPlan, FileHistoryDirectReadPlan,
    StateHistoryDirectReadPlan,
};
use crate::sql::logical_plan::plan::{
    InternalLogicalPlan, LogicalPlan, PublicReadLogicalPlan, PublicWriteLogicalPlan,
};
use crate::sql::logical_plan::public_ir::{
    BroadPublicReadGroupByKind, BroadPublicReadJoinConstraint, BroadPublicReadJoinKind,
    BroadPublicReadProjectionItemKind, BroadPublicReadQuery, BroadPublicReadRelation,
    BroadPublicReadSetExpr, BroadPublicReadStatement, BroadPublicReadTableFactor,
    BroadPublicReadTableWithJoins, BroadSqlExpr, BroadSqlExprKind, BroadSqlFunction,
    BroadSqlFunctionArg, BroadSqlFunctionArgExpr, BroadSqlFunctionArguments,
};
use crate::sql::logical_plan::result_contract::ResultContract;

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
        PublicReadLogicalPlan::Structured { plan } => {
            let read = plan.structured_read();
            if read.surface_binding.descriptor.public_name.is_empty() {
                return Err(LogicalPlanVerificationError::new(
                    "structured public read must target a named surface",
                ));
            }
        }
        PublicReadLogicalPlan::DirectHistory { plan, direct_plan } => {
            let read = plan.structured_read();
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
            .any(|cte| broad_public_read_query_has_typed_surface_relation(&cte.query))
    }) || broad_public_read_set_expr_has_typed_surface_relation(&query.body)
        || query
            .order_by
            .as_ref()
            .is_some_and(broad_public_read_order_by_has_typed_surface_relation)
        || query
            .limit_clause
            .as_ref()
            .is_some_and(broad_public_read_limit_clause_has_typed_surface_relation)
}

fn broad_public_read_set_expr_has_typed_surface_relation(expr: &BroadPublicReadSetExpr) -> bool {
    match expr {
        BroadPublicReadSetExpr::Select(select) => {
            broad_public_read_select_has_typed_surface_relation(select)
        }
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
    }
}

fn broad_public_read_select_has_typed_surface_relation(
    select: &crate::sql::logical_plan::public_ir::BroadPublicReadSelect,
) -> bool {
    broad_public_read_distinct_has_typed_surface_relation(select.distinct.as_ref())
        || select
            .from
            .iter()
            .any(broad_public_read_table_with_joins_has_typed_surface_relation)
        || select
            .projection
            .iter()
            .any(broad_public_read_projection_item_has_typed_surface_relation)
        || select
            .selection
            .as_ref()
            .is_some_and(broad_sql_expr_has_typed_surface_relation)
        || broad_public_read_group_by_has_typed_surface_relation(&select.group_by)
        || select
            .having
            .as_ref()
            .is_some_and(broad_sql_expr_has_typed_surface_relation)
}

fn broad_public_read_distinct_has_typed_surface_relation(
    distinct: Option<&crate::sql::logical_plan::public_ir::BroadPublicReadDistinct>,
) -> bool {
    match distinct {
        Some(crate::sql::logical_plan::public_ir::BroadPublicReadDistinct::On(expressions)) => {
            expressions
                .iter()
                .any(broad_sql_expr_has_typed_surface_relation)
        }
        Some(crate::sql::logical_plan::public_ir::BroadPublicReadDistinct::Distinct) | None => {
            false
        }
    }
}

fn broad_public_read_projection_item_has_typed_surface_relation(
    item: &crate::sql::logical_plan::public_ir::BroadPublicReadProjectionItem,
) -> bool {
    match &item.kind {
        BroadPublicReadProjectionItemKind::Expr { expr, .. } => {
            broad_sql_expr_has_typed_surface_relation(expr)
        }
        BroadPublicReadProjectionItemKind::Wildcard
        | BroadPublicReadProjectionItemKind::QualifiedWildcard { .. } => false,
    }
}

fn broad_public_read_group_by_has_typed_surface_relation(
    group_by: &crate::sql::logical_plan::public_ir::BroadPublicReadGroupBy,
) -> bool {
    match &group_by.kind {
        BroadPublicReadGroupByKind::All => false,
        BroadPublicReadGroupByKind::Expressions(expressions) => expressions
            .iter()
            .any(broad_sql_expr_has_typed_surface_relation),
    }
}

fn broad_public_read_order_by_has_typed_surface_relation(
    order_by: &crate::sql::logical_plan::public_ir::BroadPublicReadOrderBy,
) -> bool {
    match &order_by.kind {
        crate::sql::logical_plan::public_ir::BroadPublicReadOrderByKind::All => false,
        crate::sql::logical_plan::public_ir::BroadPublicReadOrderByKind::Expressions(
            expressions,
        ) => expressions
            .iter()
            .any(|expr| broad_sql_expr_has_typed_surface_relation(&expr.expr)),
    }
}

fn broad_public_read_limit_clause_has_typed_surface_relation(
    limit_clause: &crate::sql::logical_plan::public_ir::BroadPublicReadLimitClause,
) -> bool {
    match &limit_clause.kind {
        crate::sql::logical_plan::public_ir::BroadPublicReadLimitClauseKind::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            limit
                .as_ref()
                .is_some_and(broad_sql_expr_has_typed_surface_relation)
                || offset
                    .as_ref()
                    .is_some_and(|offset| broad_sql_expr_has_typed_surface_relation(&offset.value))
                || limit_by
                    .iter()
                    .any(broad_sql_expr_has_typed_surface_relation)
        }
        crate::sql::logical_plan::public_ir::BroadPublicReadLimitClauseKind::OffsetCommaLimit {
            offset,
            limit,
        } => {
            broad_sql_expr_has_typed_surface_relation(offset)
                || broad_sql_expr_has_typed_surface_relation(limit)
        }
    }
}

fn broad_public_read_table_with_joins_has_typed_surface_relation(
    table: &BroadPublicReadTableWithJoins,
) -> bool {
    broad_public_read_table_factor_has_typed_surface_relation(&table.relation)
        || table
            .joins
            .iter()
            .any(broad_public_read_join_has_typed_surface_relation)
}

fn broad_public_read_join_has_typed_surface_relation(
    join: &crate::sql::logical_plan::public_ir::BroadPublicReadJoin,
) -> bool {
    broad_public_read_table_factor_has_typed_surface_relation(&join.relation)
        || broad_public_read_join_kind_has_typed_surface_relation(&join.kind)
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
    }
}

fn broad_sql_expr_has_typed_surface_relation(expr: &BroadSqlExpr) -> bool {
    match &expr.kind {
        BroadSqlExprKind::Identifier(_)
        | BroadSqlExprKind::CompoundIdentifier(_)
        | BroadSqlExprKind::Value(_)
        | BroadSqlExprKind::TypedString { .. }
        | BroadSqlExprKind::Unsupported { .. } => false,
        BroadSqlExprKind::BinaryOp { left, right, .. }
        | BroadSqlExprKind::IsDistinctFrom { left, right }
        | BroadSqlExprKind::IsNotDistinctFrom { left, right } => {
            broad_sql_expr_has_typed_surface_relation(left)
                || broad_sql_expr_has_typed_surface_relation(right)
        }
        BroadSqlExprKind::AnyOp { left, right, .. }
        | BroadSqlExprKind::AllOp { left, right, .. } => {
            broad_sql_expr_has_typed_surface_relation(left)
                || broad_sql_expr_has_typed_surface_relation(right)
        }
        BroadSqlExprKind::UnaryOp { expr, .. }
        | BroadSqlExprKind::Nested(expr)
        | BroadSqlExprKind::IsNull(expr)
        | BroadSqlExprKind::IsNotNull(expr)
        | BroadSqlExprKind::IsTrue(expr)
        | BroadSqlExprKind::IsNotTrue(expr)
        | BroadSqlExprKind::IsFalse(expr)
        | BroadSqlExprKind::IsNotFalse(expr)
        | BroadSqlExprKind::IsUnknown(expr)
        | BroadSqlExprKind::IsNotUnknown(expr)
        | BroadSqlExprKind::Cast { expr, .. } => broad_sql_expr_has_typed_surface_relation(expr),
        BroadSqlExprKind::InList { expr, list, .. } => {
            broad_sql_expr_has_typed_surface_relation(expr)
                || list.iter().any(broad_sql_expr_has_typed_surface_relation)
        }
        BroadSqlExprKind::InSubquery { expr, subquery, .. } => {
            broad_sql_expr_has_typed_surface_relation(expr)
                || broad_public_read_query_has_typed_surface_relation(subquery)
        }
        BroadSqlExprKind::InUnnest {
            expr, array_expr, ..
        } => {
            broad_sql_expr_has_typed_surface_relation(expr)
                || broad_sql_expr_has_typed_surface_relation(array_expr)
        }
        BroadSqlExprKind::Between {
            expr, low, high, ..
        } => {
            broad_sql_expr_has_typed_surface_relation(expr)
                || broad_sql_expr_has_typed_surface_relation(low)
                || broad_sql_expr_has_typed_surface_relation(high)
        }
        BroadSqlExprKind::Like { expr, pattern, .. }
        | BroadSqlExprKind::ILike { expr, pattern, .. } => {
            broad_sql_expr_has_typed_surface_relation(expr)
                || broad_sql_expr_has_typed_surface_relation(pattern)
        }
        BroadSqlExprKind::Function(function) => {
            broad_sql_function_has_typed_surface_relation(function)
        }
        BroadSqlExprKind::Case {
            operand,
            conditions,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| broad_sql_expr_has_typed_surface_relation(expr))
                || conditions.iter().any(|when| {
                    broad_sql_expr_has_typed_surface_relation(&when.condition)
                        || broad_sql_expr_has_typed_surface_relation(&when.result)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|expr| broad_sql_expr_has_typed_surface_relation(expr))
        }
        BroadSqlExprKind::Exists { subquery, .. } | BroadSqlExprKind::ScalarSubquery(subquery) => {
            broad_public_read_query_has_typed_surface_relation(subquery)
        }
        BroadSqlExprKind::Tuple(items) => {
            items.iter().any(broad_sql_expr_has_typed_surface_relation)
        }
    }
}

fn broad_public_read_join_kind_has_typed_surface_relation(kind: &BroadPublicReadJoinKind) -> bool {
    match kind {
        BroadPublicReadJoinKind::Join(constraint)
        | BroadPublicReadJoinKind::Inner(constraint)
        | BroadPublicReadJoinKind::Left(constraint)
        | BroadPublicReadJoinKind::LeftOuter(constraint)
        | BroadPublicReadJoinKind::Right(constraint)
        | BroadPublicReadJoinKind::RightOuter(constraint)
        | BroadPublicReadJoinKind::FullOuter(constraint)
        | BroadPublicReadJoinKind::CrossJoin(constraint)
        | BroadPublicReadJoinKind::Semi(constraint)
        | BroadPublicReadJoinKind::LeftSemi(constraint)
        | BroadPublicReadJoinKind::RightSemi(constraint)
        | BroadPublicReadJoinKind::Anti(constraint)
        | BroadPublicReadJoinKind::LeftAnti(constraint)
        | BroadPublicReadJoinKind::RightAnti(constraint)
        | BroadPublicReadJoinKind::StraightJoin(constraint) => {
            broad_public_read_join_constraint_has_typed_surface_relation(constraint)
        }
        BroadPublicReadJoinKind::CrossApply | BroadPublicReadJoinKind::OuterApply => false,
        BroadPublicReadJoinKind::AsOf {
            match_condition,
            constraint,
        } => {
            broad_sql_expr_has_typed_surface_relation(match_condition)
                || broad_public_read_join_constraint_has_typed_surface_relation(constraint)
        }
    }
}

fn broad_public_read_join_constraint_has_typed_surface_relation(
    constraint: &BroadPublicReadJoinConstraint,
) -> bool {
    match constraint {
        BroadPublicReadJoinConstraint::On(expr) => broad_sql_expr_has_typed_surface_relation(expr),
        BroadPublicReadJoinConstraint::None
        | BroadPublicReadJoinConstraint::Natural
        | BroadPublicReadJoinConstraint::Using(_) => false,
    }
}

fn broad_sql_function_has_typed_surface_relation(function: &BroadSqlFunction) -> bool {
    broad_sql_function_arguments_have_typed_surface_relation(&function.parameters)
        || broad_sql_function_arguments_have_typed_surface_relation(&function.args)
        || function
            .filter
            .as_ref()
            .is_some_and(|expr| broad_sql_expr_has_typed_surface_relation(expr))
        || function
            .within_group
            .iter()
            .any(|expr| broad_sql_expr_has_typed_surface_relation(&expr.expr))
}

fn broad_sql_function_arguments_have_typed_surface_relation(
    arguments: &BroadSqlFunctionArguments,
) -> bool {
    match arguments {
        BroadSqlFunctionArguments::None => false,
        BroadSqlFunctionArguments::Subquery(query) => {
            broad_public_read_query_has_typed_surface_relation(query)
        }
        BroadSqlFunctionArguments::List(list) => list
            .args
            .iter()
            .any(broad_sql_function_arg_has_typed_surface_relation),
    }
}

fn broad_sql_function_arg_has_typed_surface_relation(arg: &BroadSqlFunctionArg) -> bool {
    match arg {
        BroadSqlFunctionArg::Named { arg, .. } => {
            broad_sql_function_arg_expr_has_typed_surface_relation(arg)
        }
        BroadSqlFunctionArg::ExprNamed { name, arg, .. } => {
            broad_sql_expr_has_typed_surface_relation(name)
                || broad_sql_function_arg_expr_has_typed_surface_relation(arg)
        }
        BroadSqlFunctionArg::Unnamed(arg) => {
            broad_sql_function_arg_expr_has_typed_surface_relation(arg)
        }
    }
}

fn broad_sql_function_arg_expr_has_typed_surface_relation(arg: &BroadSqlFunctionArgExpr) -> bool {
    match arg {
        BroadSqlFunctionArgExpr::Expr(expr) => broad_sql_expr_has_typed_surface_relation(expr),
        BroadSqlFunctionArgExpr::QualifiedWildcard(_) | BroadSqlFunctionArgExpr::Wildcard => false,
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
