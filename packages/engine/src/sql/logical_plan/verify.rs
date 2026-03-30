use crate::sql::logical_plan::direct_reads::{
    DirectPublicReadPlan, DirectoryHistoryDirectReadPlan, FileHistoryDirectReadPlan,
    StateHistoryDirectReadPlan,
};
use crate::sql::logical_plan::plan::{
    InternalLogicalPlan, LogicalPlan, PublicReadLogicalPlan, PublicWriteLogicalPlan,
};
use crate::sql::logical_plan::public_ir::{
    BroadNestedQueryExpr, BroadPublicReadGroupByKind, BroadPublicReadProjectionItemKind,
    BroadPublicReadQuery, BroadPublicReadRelation, BroadPublicReadSetExpr,
    BroadPublicReadStatement, BroadPublicReadTableFactor, BroadPublicReadTableWithJoins,
    BroadSqlExpr,
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
        BroadPublicReadSetExpr::Other { .. } => false,
    }
}

fn broad_public_read_select_has_typed_surface_relation(
    select: &crate::sql::logical_plan::public_ir::BroadPublicReadSelect,
) -> bool {
    select
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

fn broad_public_read_projection_item_has_typed_surface_relation(
    item: &crate::sql::logical_plan::public_ir::BroadPublicReadProjectionItem,
) -> bool {
    match &item.kind {
        BroadPublicReadProjectionItemKind::Expr { nested_queries, .. } => nested_queries
            .iter()
            .any(broad_nested_query_expr_has_typed_surface_relation),
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
                    .is_some_and(broad_sql_expr_has_typed_surface_relation)
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
        || join
            .constraint_expressions
            .iter()
            .any(broad_sql_expr_has_typed_surface_relation)
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
        BroadPublicReadTableFactor::Other { .. } => false,
    }
}

fn broad_sql_expr_has_typed_surface_relation(expr: &BroadSqlExpr) -> bool {
    expr.nested_queries
        .iter()
        .any(broad_nested_query_expr_has_typed_surface_relation)
}

fn broad_nested_query_expr_has_typed_surface_relation(expr: &BroadNestedQueryExpr) -> bool {
    match expr {
        BroadNestedQueryExpr::ScalarSubquery(query) => {
            broad_public_read_query_has_typed_surface_relation(query)
        }
        BroadNestedQueryExpr::Exists { subquery, .. } => {
            broad_public_read_query_has_typed_surface_relation(subquery)
        }
        BroadNestedQueryExpr::InSubquery { expr, subquery, .. } => {
            broad_sql_expr_has_typed_surface_relation(expr)
                || broad_public_read_query_has_typed_surface_relation(subquery)
        }
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
