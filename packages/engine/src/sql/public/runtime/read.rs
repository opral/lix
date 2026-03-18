use super::bind::bind_public_query;
use super::*;
use crate::schema::live_layout::LiveTableLayout;
use crate::schema::registry::load_live_table_layout_with_backend;
use crate::sql::common::placeholders::{resolve_placeholder_index, PlaceholderState};
use crate::sql::public::catalog::SurfaceBinding;
use crate::sql::public::planner::backend::lowerer::{
    lower_read_for_execution_with_layouts,
    rewrite_supported_public_read_surfaces_in_statement_with_registry_and_dialect,
    LoweredReadProgram, LoweredResultColumn, LoweredResultColumns,
};
use crate::sql::public::planner::canonicalize::canonicalize_read;
use crate::state::history::{
    load_state_history_rows, StateHistoryContentMode, StateHistoryLineageScope,
    StateHistoryRequest, StateHistoryRootScope, StateHistoryRow, StateHistoryVersionScope,
};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    JoinConstraint, JoinOperator, LimitClause, OrderBy, OrderByExpr, OrderByKind, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoweredPublicReadQuery {
    pub(crate) query: Query,
    pub(crate) required_schema_keys: BTreeSet<String>,
    pub(crate) result_columns: Option<LoweredResultColumns>,
}

pub(super) async fn execute_public_read_query_strict(
    backend: &dyn LixBackend,
    mut query: Query,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    let mut nested_required_schema_keys = BTreeSet::new();
    lower_nested_public_read_subqueries_in_query(
        backend,
        &mut query,
        params,
        &mut nested_required_schema_keys,
    )
    .await?;
    let lowered = lower_public_read_query_with_details(backend, query, params).await?;
    let mut required_schema_keys = lowered.required_schema_keys;
    required_schema_keys.extend(nested_required_schema_keys);
    for schema_key in &required_schema_keys {
        crate::schema::registry::ensure_schema_live_table(backend, schema_key).await?;
    }
    let bound = bind_public_query(lowered.query, params, backend.dialect())?;
    let result = backend.execute(&bound.sql, &bound.params).await?;
    let Some(result_columns) = lowered.result_columns.as_ref() else {
        return Ok(result);
    };
    Ok(decode_public_read_result(
        result,
        &LoweredReadProgram {
            statements: Vec::new(),
            pushdown_decision: PushdownDecision::default(),
            result_columns: result_columns.clone(),
        },
    ))
}

pub(crate) fn decode_public_read_result(
    result: QueryResult,
    lowered_read: &LoweredReadProgram,
) -> QueryResult {
    decode_public_read_result_columns(result, &lowered_read.result_columns)
}

pub(crate) fn decode_public_read_result_columns(
    mut result: QueryResult,
    result_columns: &LoweredResultColumns,
) -> QueryResult {
    let column_plan = match result_columns {
        LoweredResultColumns::Static(columns) => columns
            .iter()
            .copied()
            .chain(std::iter::repeat(LoweredResultColumn::Untyped))
            .take(result.columns.len())
            .collect::<Vec<_>>(),
        LoweredResultColumns::ByColumnName(columns_by_name) => result
            .columns
            .iter()
            .map(|column| {
                columns_by_name
                    .iter()
                    .find_map(|(candidate, kind)| {
                        candidate.eq_ignore_ascii_case(column).then_some(*kind)
                    })
                    .unwrap_or(LoweredResultColumn::Untyped)
            })
            .collect::<Vec<_>>(),
    };

    if !column_plan
        .iter()
        .any(|kind| *kind == LoweredResultColumn::Boolean)
    {
        return result;
    }

    for row in &mut result.rows {
        for (value, kind) in row.iter_mut().zip(column_plan.iter().copied()) {
            if kind == LoweredResultColumn::Boolean {
                if let Some(decoded) = decode_boolean_value(value) {
                    *value = decoded;
                }
            }
        }
    }

    result
}

pub(crate) async fn execute_prepared_public_read(
    backend: &dyn LixBackend,
    prepared: &PreparedPublicRead,
) -> Result<QueryResult, LixError> {
    match &prepared.execution {
        PreparedPublicReadExecution::LoweredSql(lowered) => {
            execute_lowered_public_read(backend, lowered, prepared.dependency_spec.as_ref()).await
        }
        PreparedPublicReadExecution::Direct(plan) => {
            execute_direct_public_read(backend, plan).await
        }
    }
}

async fn execute_lowered_public_read(
    backend: &dyn LixBackend,
    lowered: &LoweredReadProgram,
    dependency_spec: Option<&DependencySpec>,
) -> Result<QueryResult, LixError> {
    for schema_key in required_schema_keys_from_dependency_spec(dependency_spec) {
        crate::schema::registry::ensure_schema_live_table(backend, &schema_key).await?;
    }

    let mut result = QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
    };
    for statement in lowered.statements.iter().cloned() {
        let statement = lower_statement(statement, backend.dialect())?;
        result = backend.execute(&statement.to_string(), &[]).await?;
    }
    Ok(decode_public_read_result_columns(
        result,
        &lowered.result_columns,
    ))
}

async fn execute_direct_public_read(
    backend: &dyn LixBackend,
    plan: &DirectPublicReadPlan,
) -> Result<QueryResult, LixError> {
    match plan {
        DirectPublicReadPlan::StateHistory(plan) => {
            execute_direct_state_history_read(backend, plan).await
        }
    }
}

fn decode_boolean_value(value: &Value) -> Option<Value> {
    match value {
        Value::Null => Some(Value::Null),
        Value::Boolean(value) => Some(Value::Boolean(*value)),
        Value::Integer(0) => Some(Value::Boolean(false)),
        Value::Integer(1) => Some(Value::Boolean(true)),
        Value::Text(text) => match text.trim().to_ascii_lowercase().as_str() {
            "0" | "false" => Some(Value::Boolean(false)),
            "1" | "true" => Some(Value::Boolean(true)),
            _ => None,
        },
        Value::Real(_) | Value::Json(_) | Value::Blob(_) => None,
        Value::Integer(_) => None,
    }
}

async fn lower_nested_public_read_subqueries_in_query(
    backend: &dyn LixBackend,
    query: &mut Query,
    params: &[Value],
    required_schema_keys: &mut BTreeSet<String>,
) -> Result<(), LixError> {
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            Box::pin(lower_nested_public_read_subqueries_in_query(
                backend,
                &mut cte.query,
                params,
                required_schema_keys,
            ))
            .await?;
        }
    }
    Box::pin(lower_nested_public_read_subqueries_in_set_expr(
        backend,
        query.body.as_mut(),
        params,
        required_schema_keys,
    ))
    .await?;
    if let Some(order_by) = &mut query.order_by {
        lower_nested_public_read_subqueries_in_order_by(
            backend,
            order_by,
            params,
            required_schema_keys,
        )
        .await?;
    }
    if let Some(limit_clause) = &mut query.limit_clause {
        Box::pin(lower_nested_public_read_subqueries_in_limit_clause(
            backend,
            limit_clause,
            params,
            required_schema_keys,
        ))
        .await?;
    }
    if let Some(quantity) = query
        .fetch
        .as_mut()
        .and_then(|fetch| fetch.quantity.as_mut())
    {
        Box::pin(lower_nested_public_read_subqueries_in_expr(
            backend,
            quantity,
            params,
            required_schema_keys,
        ))
        .await?;
    }
    Ok(())
}

async fn lower_nested_public_read_subqueries_in_set_expr(
    backend: &dyn LixBackend,
    expr: &mut SetExpr,
    params: &[Value],
    required_schema_keys: &mut BTreeSet<String>,
) -> Result<(), LixError> {
    match expr {
        SetExpr::Select(select) => {
            Box::pin(lower_nested_public_read_subqueries_in_select(
                backend,
                select,
                params,
                required_schema_keys,
            ))
            .await
        }
        SetExpr::Query(query) => {
            Box::pin(lower_nested_public_read_subqueries_in_query(
                backend,
                query,
                params,
                required_schema_keys,
            ))
            .await
        }
        SetExpr::SetOperation { left, right, .. } => {
            Box::pin(lower_nested_public_read_subqueries_in_set_expr(
                backend,
                left.as_mut(),
                params,
                required_schema_keys,
            ))
            .await?;
            Box::pin(lower_nested_public_read_subqueries_in_set_expr(
                backend,
                right.as_mut(),
                params,
                required_schema_keys,
            ))
            .await
        }
        SetExpr::Values(values) => {
            for row in &mut values.rows {
                for expr in row {
                    Box::pin(lower_nested_public_read_subqueries_in_expr(
                        backend,
                        expr,
                        params,
                        required_schema_keys,
                    ))
                    .await?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

async fn lower_nested_public_read_subqueries_in_select(
    backend: &dyn LixBackend,
    select: &mut Select,
    params: &[Value],
    required_schema_keys: &mut BTreeSet<String>,
) -> Result<(), LixError> {
    for table in &mut select.from {
        Box::pin(lower_nested_public_read_subqueries_in_table_with_joins(
            backend,
            table,
            params,
            required_schema_keys,
        ))
        .await?;
    }
    if let Some(prewhere) = &mut select.prewhere {
        Box::pin(lower_nested_public_read_subqueries_in_expr(
            backend,
            prewhere,
            params,
            required_schema_keys,
        ))
        .await?;
    }
    if let Some(selection) = &mut select.selection {
        Box::pin(lower_nested_public_read_subqueries_in_expr(
            backend,
            selection,
            params,
            required_schema_keys,
        ))
        .await?;
    }
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                Box::pin(lower_nested_public_read_subqueries_in_expr(
                    backend,
                    expr,
                    params,
                    required_schema_keys,
                ))
                .await?;
            }
            SelectItem::QualifiedWildcard(
                sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
                _,
            ) => {
                Box::pin(lower_nested_public_read_subqueries_in_expr(
                    backend,
                    expr,
                    params,
                    required_schema_keys,
                ))
                .await?;
            }
            _ => {}
        }
    }
    match &mut select.group_by {
        GroupByExpr::All(_) => {}
        GroupByExpr::Expressions(expressions, _) => {
            for expr in expressions {
                Box::pin(lower_nested_public_read_subqueries_in_expr(
                    backend,
                    expr,
                    params,
                    required_schema_keys,
                ))
                .await?;
            }
        }
    }
    for expr in &mut select.cluster_by {
        Box::pin(lower_nested_public_read_subqueries_in_expr(
            backend,
            expr,
            params,
            required_schema_keys,
        ))
        .await?;
    }
    for expr in &mut select.distribute_by {
        Box::pin(lower_nested_public_read_subqueries_in_expr(
            backend,
            expr,
            params,
            required_schema_keys,
        ))
        .await?;
    }
    for expr in &mut select.sort_by {
        lower_nested_public_read_subqueries_in_order_by_expr(
            backend,
            expr,
            params,
            required_schema_keys,
        )
        .await?;
    }
    if let Some(having) = &mut select.having {
        Box::pin(lower_nested_public_read_subqueries_in_expr(
            backend,
            having,
            params,
            required_schema_keys,
        ))
        .await?;
    }
    if let Some(qualify) = &mut select.qualify {
        Box::pin(lower_nested_public_read_subqueries_in_expr(
            backend,
            qualify,
            params,
            required_schema_keys,
        ))
        .await?;
    }
    if let Some(connect_by) = &mut select.connect_by {
        Box::pin(lower_nested_public_read_subqueries_in_expr(
            backend,
            &mut connect_by.condition,
            params,
            required_schema_keys,
        ))
        .await?;
        for expr in &mut connect_by.relationships {
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                expr,
                params,
                required_schema_keys,
            ))
            .await?;
        }
    }
    Ok(())
}

async fn lower_nested_public_read_subqueries_in_table_with_joins(
    backend: &dyn LixBackend,
    table: &mut TableWithJoins,
    params: &[Value],
    required_schema_keys: &mut BTreeSet<String>,
) -> Result<(), LixError> {
    Box::pin(lower_nested_public_read_subqueries_in_table_factor(
        backend,
        &mut table.relation,
        params,
        required_schema_keys,
    ))
    .await?;
    for join in &mut table.joins {
        Box::pin(lower_nested_public_read_subqueries_in_table_factor(
            backend,
            &mut join.relation,
            params,
            required_schema_keys,
        ))
        .await?;
        lower_nested_public_read_subqueries_in_join_operator(
            backend,
            &mut join.join_operator,
            params,
            required_schema_keys,
        )
        .await?;
    }
    Ok(())
}

async fn lower_nested_public_read_subqueries_in_table_factor(
    backend: &dyn LixBackend,
    relation: &mut TableFactor,
    params: &[Value],
    required_schema_keys: &mut BTreeSet<String>,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Derived { subquery, .. } => {
            Box::pin(lower_nested_public_read_subqueries_in_query(
                backend,
                subquery,
                params,
                required_schema_keys,
            ))
            .await
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            Box::pin(lower_nested_public_read_subqueries_in_table_with_joins(
                backend,
                table_with_joins,
                params,
                required_schema_keys,
            ))
            .await
        }
        _ => Ok(()),
    }
}

async fn lower_nested_public_read_subqueries_in_order_by(
    backend: &dyn LixBackend,
    order_by: &mut OrderBy,
    params: &[Value],
    required_schema_keys: &mut BTreeSet<String>,
) -> Result<(), LixError> {
    let OrderByKind::Expressions(expressions) = &mut order_by.kind else {
        return Ok(());
    };
    for item in expressions {
        lower_nested_public_read_subqueries_in_order_by_expr(
            backend,
            item,
            params,
            required_schema_keys,
        )
        .await?;
    }
    Ok(())
}

async fn lower_nested_public_read_subqueries_in_order_by_expr(
    backend: &dyn LixBackend,
    order_by_expr: &mut OrderByExpr,
    params: &[Value],
    required_schema_keys: &mut BTreeSet<String>,
) -> Result<(), LixError> {
    Box::pin(lower_nested_public_read_subqueries_in_expr(
        backend,
        &mut order_by_expr.expr,
        params,
        required_schema_keys,
    ))
    .await?;
    if let Some(with_fill) = &mut order_by_expr.with_fill {
        if let Some(from) = &mut with_fill.from {
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                from,
                params,
                required_schema_keys,
            ))
            .await?;
        }
        if let Some(to) = &mut with_fill.to {
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                to,
                params,
                required_schema_keys,
            ))
            .await?;
        }
        if let Some(step) = &mut with_fill.step {
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                step,
                params,
                required_schema_keys,
            ))
            .await?;
        }
    }
    Ok(())
}

async fn lower_nested_public_read_subqueries_in_limit_clause(
    backend: &dyn LixBackend,
    limit_clause: &mut LimitClause,
    params: &[Value],
    required_schema_keys: &mut BTreeSet<String>,
) -> Result<(), LixError> {
    match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if let Some(limit) = limit {
                Box::pin(lower_nested_public_read_subqueries_in_expr(
                    backend,
                    limit,
                    params,
                    required_schema_keys,
                ))
                .await?;
            }
            if let Some(offset) = offset {
                Box::pin(lower_nested_public_read_subqueries_in_expr(
                    backend,
                    &mut offset.value,
                    params,
                    required_schema_keys,
                ))
                .await?;
            }
            for expr in limit_by {
                Box::pin(lower_nested_public_read_subqueries_in_expr(
                    backend,
                    expr,
                    params,
                    required_schema_keys,
                ))
                .await?;
            }
            Ok(())
        }
        LimitClause::OffsetCommaLimit { offset, limit } => {
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                offset,
                params,
                required_schema_keys,
            ))
            .await?;
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                limit,
                params,
                required_schema_keys,
            ))
            .await
        }
    }
}

async fn lower_nested_public_read_subqueries_in_join_operator(
    backend: &dyn LixBackend,
    join_operator: &mut JoinOperator,
    params: &[Value],
    required_schema_keys: &mut BTreeSet<String>,
) -> Result<(), LixError> {
    let (match_condition, constraint) = match join_operator {
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => (Some(match_condition), Some(constraint)),
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint)
        | JoinOperator::StraightJoin(constraint) => (None, Some(constraint)),
        JoinOperator::CrossApply | JoinOperator::OuterApply => (None, None),
    };
    if let Some(expr) = match_condition {
        Box::pin(lower_nested_public_read_subqueries_in_expr(
            backend,
            expr,
            params,
            required_schema_keys,
        ))
        .await?;
    }
    if let Some(JoinConstraint::On(expr)) = constraint {
        Box::pin(lower_nested_public_read_subqueries_in_expr(
            backend,
            expr,
            params,
            required_schema_keys,
        ))
        .await?;
    }
    Ok(())
}

async fn lower_nested_public_read_subqueries_in_expr(
    backend: &dyn LixBackend,
    expr: &mut Expr,
    params: &[Value],
    required_schema_keys: &mut BTreeSet<String>,
) -> Result<(), LixError> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                left.as_mut(),
                params,
                required_schema_keys,
            ))
            .await?;
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                right.as_mut(),
                params,
                required_schema_keys,
            ))
            .await
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => {
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                expr.as_mut(),
                params,
                required_schema_keys,
            ))
            .await
        }
        Expr::InList { expr, list, .. } => {
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                expr.as_mut(),
                params,
                required_schema_keys,
            ))
            .await?;
            for item in list {
                Box::pin(lower_nested_public_read_subqueries_in_expr(
                    backend,
                    item,
                    params,
                    required_schema_keys,
                ))
                .await?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                expr.as_mut(),
                params,
                required_schema_keys,
            ))
            .await?;
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                low.as_mut(),
                params,
                required_schema_keys,
            ))
            .await?;
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                high.as_mut(),
                params,
                required_schema_keys,
            ))
            .await
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                expr.as_mut(),
                params,
                required_schema_keys,
            ))
            .await?;
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                pattern.as_mut(),
                params,
                required_schema_keys,
            ))
            .await
        }
        Expr::Subquery(query) => {
            Box::pin(lower_nested_public_read_subqueries_in_query(
                backend,
                query,
                params,
                required_schema_keys,
            ))
            .await?;
            let lowered =
                lower_public_read_query_with_details(backend, (**query).clone(), params).await?;
            required_schema_keys.extend(lowered.required_schema_keys);
            *query = Box::new(lowered.query);
            Ok(())
        }
        Expr::Exists { subquery, .. } => {
            Box::pin(lower_nested_public_read_subqueries_in_query(
                backend,
                subquery,
                params,
                required_schema_keys,
            ))
            .await?;
            let lowered =
                lower_public_read_query_with_details(backend, (**subquery).clone(), params).await?;
            required_schema_keys.extend(lowered.required_schema_keys);
            *subquery = Box::new(lowered.query);
            Ok(())
        }
        Expr::InSubquery { expr, subquery, .. } => {
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                expr.as_mut(),
                params,
                required_schema_keys,
            ))
            .await?;
            Box::pin(lower_nested_public_read_subqueries_in_query(
                backend,
                subquery,
                params,
                required_schema_keys,
            ))
            .await?;
            let lowered =
                lower_public_read_query_with_details(backend, (**subquery).clone(), params).await?;
            required_schema_keys.extend(lowered.required_schema_keys);
            *subquery = Box::new(lowered.query);
            Ok(())
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                expr.as_mut(),
                params,
                required_schema_keys,
            ))
            .await?;
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                array_expr.as_mut(),
                params,
                required_schema_keys,
            ))
            .await
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                left.as_mut(),
                params,
                required_schema_keys,
            ))
            .await?;
            Box::pin(lower_nested_public_read_subqueries_in_expr(
                backend,
                right.as_mut(),
                params,
                required_schema_keys,
            ))
            .await
        }
        Expr::Function(function) => match &mut function.args {
            FunctionArguments::List(list) => {
                for arg in &mut list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                            Box::pin(lower_nested_public_read_subqueries_in_expr(
                                backend,
                                expr,
                                params,
                                required_schema_keys,
                            ))
                            .await?;
                        }
                        FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                            if let FunctionArgExpr::Expr(expr) = arg {
                                Box::pin(lower_nested_public_read_subqueries_in_expr(
                                    backend,
                                    expr,
                                    params,
                                    required_schema_keys,
                                ))
                                .await?;
                            }
                        }
                        _ => {}
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        },
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                Box::pin(lower_nested_public_read_subqueries_in_expr(
                    backend,
                    operand,
                    params,
                    required_schema_keys,
                ))
                .await?;
            }
            for condition in conditions {
                Box::pin(lower_nested_public_read_subqueries_in_expr(
                    backend,
                    &mut condition.condition,
                    params,
                    required_schema_keys,
                ))
                .await?;
                Box::pin(lower_nested_public_read_subqueries_in_expr(
                    backend,
                    &mut condition.result,
                    params,
                    required_schema_keys,
                ))
                .await?;
            }
            if let Some(else_result) = else_result {
                Box::pin(lower_nested_public_read_subqueries_in_expr(
                    backend,
                    else_result,
                    params,
                    required_schema_keys,
                ))
                .await?;
            }
            Ok(())
        }
        Expr::Tuple(items) => {
            for item in items {
                Box::pin(lower_nested_public_read_subqueries_in_expr(
                    backend,
                    item,
                    params,
                    required_schema_keys,
                ))
                .await?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(super) async fn lower_public_read_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<LoweredPublicReadQuery, LixError> {
    lower_public_read_query_with_details(backend, query, params).await
}

async fn lower_public_read_query_with_details(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<LoweredPublicReadQuery, LixError> {
    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
    if !statement_references_public_surface(&registry, &Statement::Query(Box::new(query.clone()))) {
        return Ok(LoweredPublicReadQuery {
            query,
            required_schema_keys: BTreeSet::new(),
            result_columns: None,
        });
    }
    let active_version_id = load_active_version_id_for_public_read(backend).await?;
    let parsed = vec![Statement::Query(Box::new(query.clone()))];
    let prepared = try_prepare_public_read_with_registry_and_internal_access(
        backend,
        &registry,
        &parsed,
        params,
        &active_version_id,
        None,
        true,
    )
    .await?;
    let maybe_lowered_from_prepared = prepared
        .as_ref()
        .and_then(|prepared| prepared.lowered_read().cloned());
    let (lowered, required_schema_keys) = if let Some(lowered) = maybe_lowered_from_prepared {
        let required_schema_keys = prepared
            .as_ref()
            .map(|prepared| {
                required_schema_keys_from_dependency_spec(prepared.dependency_spec.as_ref())
            })
            .unwrap_or_default();
        (lowered, required_schema_keys)
    } else {
        let rewritten = rewrite_public_read_query_to_lowered_sql_with_registry(
            query.clone(),
            backend.dialect(),
            &registry,
        )?;
        if rewritten != query {
            return Ok(LoweredPublicReadQuery {
                query: rewritten,
                required_schema_keys: BTreeSet::new(),
                result_columns: None,
            });
        }
        let bound_statement = BoundStatement::from_statement(
            Statement::Query(Box::new(query)),
            params.to_vec(),
            ExecutionContext {
                dialect: Some(backend.dialect()),
                writer_key: None,
                requested_version_id: Some(active_version_id),
            },
        );
        let structured_read = canonicalize_read(bound_statement, &registry)
            .map(|canonicalized| canonicalized.into_structured_read())
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "public read subquery canonicalization failed: {}",
                        error.message
                    ),
                )
            })?;
        let dependency_spec = augment_dependency_spec_for_public_read(
            &registry,
            &structured_read,
            derive_dependency_spec_from_structured_public_read(&structured_read),
        );
        let effective_state = build_effective_state(&structured_read, dependency_spec.as_ref());
        let known_live_layouts = load_known_live_layouts_for_public_read(
            backend,
            dependency_spec.as_ref(),
            effective_state.as_ref().map(|(request, _)| request),
        )
        .await?;
        let lowered = lower_read_for_execution_with_layouts(
            backend.dialect(),
            &structured_read,
            effective_state.as_ref().map(|(request, _)| request),
            effective_state.as_ref().map(|(_, plan)| plan),
            &known_live_layouts,
        )?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public read lowering could not prepare read subquery",
            )
        })?;
        (
            lowered,
            required_schema_keys_from_dependency_spec(dependency_spec.as_ref()),
        )
    };
    let statement = lowered.statements.into_iter().next().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public read subquery lowered to no statements",
        )
    })?;
    let statement = lower_statement(statement, backend.dialect())?;
    match statement {
        Statement::Query(query) => Ok(LoweredPublicReadQuery {
            query: *query,
            required_schema_keys,
            result_columns: Some(lowered.result_columns),
        }),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "expected lowered subquery to remain a SELECT query",
        )),
    }
}

fn required_schema_keys_from_dependency_spec(
    dependency_spec: Option<&DependencySpec>,
) -> BTreeSet<String> {
    dependency_spec
        .map(|spec| {
            spec.schema_keys
                .iter()
                .filter(|schema_key| schema_key.as_str() != "lix_active_version")
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

async fn load_known_live_layouts_for_dependency_spec(
    backend: &dyn LixBackend,
    dependency_spec: Option<&DependencySpec>,
) -> Result<BTreeMap<String, LiveTableLayout>, LixError> {
    let mut layouts = BTreeMap::new();
    for schema_key in required_schema_keys_from_dependency_spec(dependency_spec) {
        if let Some(layout) = crate::schema::live_layout::builtin_live_table_layout(&schema_key)? {
            layouts.insert(schema_key, layout);
            continue;
        }
        let layout = load_live_table_layout_with_backend(backend, &schema_key).await?;
        layouts.insert(schema_key, layout);
    }
    Ok(layouts)
}

async fn load_known_live_layouts_for_public_read(
    backend: &dyn LixBackend,
    dependency_spec: Option<&DependencySpec>,
    effective_state_request: Option<&EffectiveStateRequest>,
) -> Result<BTreeMap<String, LiveTableLayout>, LixError> {
    let mut layouts = load_known_live_layouts_for_dependency_spec(backend, dependency_spec).await?;
    if let Some(request) = effective_state_request {
        for schema_key in &request.schema_set {
            if layouts.contains_key(schema_key) {
                continue;
            }
            if let Some(layout) = crate::schema::live_layout::builtin_live_table_layout(schema_key)?
            {
                layouts.insert(schema_key.clone(), layout);
                continue;
            }
            let layout = load_live_table_layout_with_backend(backend, schema_key).await?;
            layouts.insert(schema_key.clone(), layout);
        }
    }
    Ok(layouts)
}

fn build_direct_state_history_plan(
    structured_read: &StructuredPublicRead,
) -> Result<Option<StateHistoryDirectReadPlan>, LixError> {
    if structured_read.surface_binding.descriptor.public_name != "lix_state_history" {
        return Ok(None);
    }
    if structured_read.query.uses_wildcard_projection()
        && structured_read.query.projection.len() != 1
    {
        return Ok(None);
    }

    let mut request = StateHistoryRequest {
        lineage_scope: StateHistoryLineageScope::ActiveVersion,
        content_mode: StateHistoryContentMode::MetadataOnly,
        ..StateHistoryRequest::default()
    };
    let predicates = build_state_history_predicates_and_request(structured_read, &mut request)?;
    if state_history_query_needs_snapshot_content(structured_read, &predicates)? {
        request.content_mode = StateHistoryContentMode::IncludeSnapshotContent;
    }

    let (projections, wildcard_projection, wildcard_columns, projection_aliases) =
        build_state_history_projection_plan(structured_read)?;
    let sort_keys = build_state_history_sort_keys(structured_read, &projection_aliases)?;
    let (limit, offset) = direct_limit_values(
        structured_read.query.limit_clause.as_ref(),
        &structured_read.bound_statement.bound_parameters,
    )?;
    let result_columns = direct_state_history_result_columns(
        &structured_read.surface_binding,
        &projections,
        wildcard_projection,
    );

    Ok(Some(StateHistoryDirectReadPlan {
        request,
        predicates,
        projections,
        wildcard_projection,
        wildcard_columns,
        sort_keys,
        limit,
        offset,
        result_columns,
    }))
}

fn build_state_history_predicates_and_request(
    structured_read: &StructuredPublicRead,
    request: &mut StateHistoryRequest,
) -> Result<Vec<StateHistoryPredicate>, LixError> {
    let mut predicates = Vec::new();
    let mut root_commit_ids = BTreeSet::new();
    let mut version_ids = BTreeSet::new();
    let mut entity_ids = BTreeSet::new();
    let mut file_ids = BTreeSet::new();
    let mut schema_keys = BTreeSet::new();
    let mut plugin_keys = BTreeSet::new();
    let mut min_depth = request.min_depth;
    let mut max_depth = request.max_depth;
    let mut placeholder_state = PlaceholderState::new();

    for predicate_expr in &structured_read.query.selection_predicates {
        let predicate = parse_state_history_predicate(
            predicate_expr,
            &structured_read.surface_binding,
            &structured_read.bound_statement.bound_parameters,
            &mut placeholder_state,
        )?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct state-history execution does not support this predicate shape",
            )
        })?;
        apply_state_history_pushdown(
            &predicate,
            &mut root_commit_ids,
            &mut version_ids,
            &mut entity_ids,
            &mut file_ids,
            &mut schema_keys,
            &mut plugin_keys,
            &mut min_depth,
            &mut max_depth,
        );
        predicates.push(predicate);
    }

    if !root_commit_ids.is_empty() {
        request.root_scope =
            StateHistoryRootScope::RequestedRoots(root_commit_ids.into_iter().collect());
    }
    if !version_ids.is_empty() {
        request.version_scope =
            StateHistoryVersionScope::RequestedVersions(version_ids.into_iter().collect());
    }
    request.entity_ids = entity_ids.into_iter().collect();
    request.file_ids = file_ids.into_iter().collect();
    request.schema_keys = schema_keys.into_iter().collect();
    request.plugin_keys = plugin_keys.into_iter().collect();
    request.min_depth = min_depth;
    request.max_depth = max_depth;

    Ok(predicates)
}

fn apply_state_history_pushdown(
    predicate: &StateHistoryPredicate,
    root_commit_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
    entity_ids: &mut BTreeSet<String>,
    file_ids: &mut BTreeSet<String>,
    schema_keys: &mut BTreeSet<String>,
    plugin_keys: &mut BTreeSet<String>,
    min_depth: &mut Option<i64>,
    max_depth: &mut Option<i64>,
) {
    match predicate {
        StateHistoryPredicate::Eq(field, value) => {
            push_text_value_for_field(
                field,
                value,
                root_commit_ids,
                version_ids,
                entity_ids,
                file_ids,
                schema_keys,
                plugin_keys,
            );
            if *field == DirectStateHistoryField::Depth {
                if let Some(depth) = value_as_i64(value) {
                    update_min_depth(min_depth, depth);
                    update_max_depth(max_depth, depth);
                }
            }
        }
        StateHistoryPredicate::In(field, values) => {
            for value in values {
                push_text_value_for_field(
                    field,
                    value,
                    root_commit_ids,
                    version_ids,
                    entity_ids,
                    file_ids,
                    schema_keys,
                    plugin_keys,
                );
            }
        }
        StateHistoryPredicate::Gt(field, value) if *field == DirectStateHistoryField::Depth => {
            if let Some(depth) = value_as_i64(value) {
                update_min_depth(min_depth, depth.saturating_add(1));
            }
        }
        StateHistoryPredicate::GtEq(field, value) if *field == DirectStateHistoryField::Depth => {
            if let Some(depth) = value_as_i64(value) {
                update_min_depth(min_depth, depth);
            }
        }
        StateHistoryPredicate::Lt(field, value) if *field == DirectStateHistoryField::Depth => {
            if let Some(depth) = value_as_i64(value) {
                update_max_depth(max_depth, depth.saturating_sub(1));
            }
        }
        StateHistoryPredicate::LtEq(field, value) if *field == DirectStateHistoryField::Depth => {
            if let Some(depth) = value_as_i64(value) {
                update_max_depth(max_depth, depth);
            }
        }
        StateHistoryPredicate::NotEq(_, _)
        | StateHistoryPredicate::Gt(_, _)
        | StateHistoryPredicate::GtEq(_, _)
        | StateHistoryPredicate::Lt(_, _)
        | StateHistoryPredicate::LtEq(_, _)
        | StateHistoryPredicate::IsNull(_)
        | StateHistoryPredicate::IsNotNull(_) => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn push_text_value_for_field(
    field: &DirectStateHistoryField,
    value: &Value,
    root_commit_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
    entity_ids: &mut BTreeSet<String>,
    file_ids: &mut BTreeSet<String>,
    schema_keys: &mut BTreeSet<String>,
    plugin_keys: &mut BTreeSet<String>,
) {
    let Some(text) = value_as_text(value) else {
        return;
    };
    match field {
        DirectStateHistoryField::RootCommitId => {
            root_commit_ids.insert(text.to_string());
        }
        DirectStateHistoryField::VersionId => {
            version_ids.insert(text.to_string());
        }
        DirectStateHistoryField::EntityId => {
            entity_ids.insert(text.to_string());
        }
        DirectStateHistoryField::FileId => {
            file_ids.insert(text.to_string());
        }
        DirectStateHistoryField::SchemaKey => {
            schema_keys.insert(text.to_string());
        }
        DirectStateHistoryField::PluginKey => {
            plugin_keys.insert(text.to_string());
        }
        DirectStateHistoryField::SnapshotContent
        | DirectStateHistoryField::Metadata
        | DirectStateHistoryField::SchemaVersion
        | DirectStateHistoryField::ChangeId
        | DirectStateHistoryField::CommitId
        | DirectStateHistoryField::CommitCreatedAt
        | DirectStateHistoryField::Depth => {}
    }
}

fn update_min_depth(min_depth: &mut Option<i64>, candidate: i64) {
    match min_depth {
        Some(current) => *current = (*current).max(candidate),
        None => *min_depth = Some(candidate),
    }
}

fn update_max_depth(max_depth: &mut Option<i64>, candidate: i64) {
    match max_depth {
        Some(current) => *current = (*current).min(candidate),
        None => *max_depth = Some(candidate),
    }
}

fn state_history_query_needs_snapshot_content(
    structured_read: &StructuredPublicRead,
    predicates: &[StateHistoryPredicate],
) -> Result<bool, LixError> {
    if structured_read.query.uses_wildcard_projection() {
        return Ok(true);
    }

    for projection in &structured_read.query.projection {
        let field = direct_state_history_field_from_select_item(
            &structured_read.surface_binding,
            projection,
        )?;
        if field == DirectStateHistoryField::SnapshotContent {
            return Ok(true);
        }
    }
    for predicate in predicates {
        if state_history_predicate_field(predicate) == DirectStateHistoryField::SnapshotContent {
            return Ok(true);
        }
    }
    if let Some(order_by) = &structured_read.query.order_by {
        let OrderByKind::Expressions(expressions) = &order_by.kind else {
            return Ok(true);
        };
        for sort in expressions {
            if sort.with_fill.is_some() {
                return Ok(true);
            }
            let Some(field) =
                direct_state_history_field_from_expr(&structured_read.surface_binding, &sort.expr)?
            else {
                continue;
            };
            if field == DirectStateHistoryField::SnapshotContent {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn build_state_history_projection_plan(
    structured_read: &StructuredPublicRead,
) -> Result<
    (
        Vec<StateHistoryProjection>,
        bool,
        Vec<String>,
        BTreeMap<String, DirectStateHistoryField>,
    ),
    LixError,
> {
    if structured_read.query.uses_wildcard_projection() {
        return Ok((
            Vec::new(),
            true,
            structured_read.surface_binding.exposed_columns.clone(),
            BTreeMap::new(),
        ));
    }

    let mut projections = Vec::new();
    let mut aliases = BTreeMap::new();
    for item in &structured_read.query.projection {
        let field =
            direct_state_history_field_from_select_item(&structured_read.surface_binding, item)?;
        let output_name = direct_state_history_output_name(item);
        aliases.insert(output_name.to_ascii_lowercase(), field.clone());
        projections.push(StateHistoryProjection { output_name, field });
    }
    Ok((projections, false, Vec::new(), aliases))
}

fn build_state_history_sort_keys(
    structured_read: &StructuredPublicRead,
    projection_aliases: &BTreeMap<String, DirectStateHistoryField>,
) -> Result<Vec<StateHistorySortKey>, LixError> {
    let Some(order_by) = &structured_read.query.order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct state-history execution does not support ORDER BY ALL",
        ));
    };

    let mut sort_keys = Vec::new();
    for expr in expressions {
        if expr.with_fill.is_some() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct state-history execution does not support ORDER BY ... WITH FILL",
            ));
        }

        let output_name = direct_expr_output_name(&expr.expr);
        let field =
            direct_state_history_field_from_expr(&structured_read.surface_binding, &expr.expr)?
                .or_else(|| {
                    projection_aliases
                        .get(&output_name.to_ascii_lowercase())
                        .cloned()
                });
        let Some(field) = field else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct state-history execution does not support this ORDER BY expression",
            ));
        };
        sort_keys.push(StateHistorySortKey {
            output_name,
            field: Some(field),
            descending: matches!(expr.options.asc, Some(false)),
        });
    }
    Ok(sort_keys)
}

fn direct_state_history_result_columns(
    surface_binding: &SurfaceBinding,
    projections: &[StateHistoryProjection],
    wildcard_projection: bool,
) -> LoweredResultColumns {
    if wildcard_projection {
        return LoweredResultColumns::ByColumnName(
            surface_binding
                .column_types
                .iter()
                .map(
                    |(name, column_type): (
                        &String,
                        &crate::sql::public::catalog::SurfaceColumnType,
                    )| {
                        (
                            name.clone(),
                            direct_lowered_result_column_from_surface_type(*column_type),
                        )
                    },
                )
                .collect(),
        );
    }

    LoweredResultColumns::Static(
        projections
            .iter()
            .map(|projection| {
                direct_surface_column_type(
                    surface_binding,
                    direct_state_history_field_name(&projection.field),
                )
                .map(direct_lowered_result_column_from_surface_type)
                .unwrap_or(LoweredResultColumn::Untyped)
            })
            .collect(),
    )
}

fn direct_lowered_result_column_from_surface_type(
    column_type: crate::sql::public::catalog::SurfaceColumnType,
) -> LoweredResultColumn {
    match column_type {
        crate::sql::public::catalog::SurfaceColumnType::Boolean => LoweredResultColumn::Boolean,
        crate::sql::public::catalog::SurfaceColumnType::String
        | crate::sql::public::catalog::SurfaceColumnType::Integer
        | crate::sql::public::catalog::SurfaceColumnType::Number
        | crate::sql::public::catalog::SurfaceColumnType::Json => LoweredResultColumn::Untyped,
    }
}

fn direct_surface_column_type(
    surface_binding: &SurfaceBinding,
    column: &str,
) -> Option<crate::sql::public::catalog::SurfaceColumnType> {
    surface_binding.column_types.iter().find_map(
        |(candidate, kind): (&String, &crate::sql::public::catalog::SurfaceColumnType)| {
            candidate.eq_ignore_ascii_case(column).then_some(*kind)
        },
    )
}

fn direct_state_history_field_from_select_item(
    surface_binding: &SurfaceBinding,
    item: &SelectItem,
) -> Result<DirectStateHistoryField, LixError> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "wildcard projection should be handled before direct state-history field extraction",
            ))
        }
    };
    direct_state_history_field_from_expr(surface_binding, expr)?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct state-history execution does not support this projection expression",
        )
    })
}

fn direct_state_history_output_name(item: &SelectItem) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => direct_expr_output_name(expr),
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => "*".to_string(),
    }
}

fn direct_expr_output_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|part| part.value.clone())
            .unwrap_or_default(),
        _ => expr.to_string(),
    }
}

fn direct_state_history_field_from_expr(
    surface_binding: &SurfaceBinding,
    expr: &Expr,
) -> Result<Option<DirectStateHistoryField>, LixError> {
    match expr {
        Expr::Identifier(ident) => {
            direct_state_history_field_from_column_name(surface_binding, &ident.value).map(Some)
        }
        Expr::CompoundIdentifier(parts) => {
            let Some(ident) = parts.last() else {
                return Ok(None);
            };
            direct_state_history_field_from_column_name(surface_binding, &ident.value).map(Some)
        }
        Expr::Nested(inner) => direct_state_history_field_from_expr(surface_binding, inner),
        _ => Ok(None),
    }
}

fn direct_state_history_field_from_column_name(
    surface_binding: &SurfaceBinding,
    column: &str,
) -> Result<DirectStateHistoryField, LixError> {
    match column.to_ascii_lowercase().as_str() {
        "entity_id" | "lixcol_entity_id" => Ok(DirectStateHistoryField::EntityId),
        "schema_key" | "lixcol_schema_key" => Ok(DirectStateHistoryField::SchemaKey),
        "file_id" | "lixcol_file_id" => Ok(DirectStateHistoryField::FileId),
        "plugin_key" | "lixcol_plugin_key" => Ok(DirectStateHistoryField::PluginKey),
        "snapshot_content" => Ok(DirectStateHistoryField::SnapshotContent),
        "metadata" | "lixcol_metadata" => Ok(DirectStateHistoryField::Metadata),
        "schema_version" | "lixcol_schema_version" => Ok(DirectStateHistoryField::SchemaVersion),
        "change_id" | "lixcol_change_id" => Ok(DirectStateHistoryField::ChangeId),
        "commit_id" | "lixcol_commit_id" => Ok(DirectStateHistoryField::CommitId),
        "commit_created_at" => Ok(DirectStateHistoryField::CommitCreatedAt),
        "root_commit_id" | "lixcol_root_commit_id" => Ok(DirectStateHistoryField::RootCommitId),
        "depth" | "lixcol_depth" => Ok(DirectStateHistoryField::Depth),
        "version_id" | "lixcol_version_id" => Ok(DirectStateHistoryField::VersionId),
        _ => Err(crate::errors::sql_unknown_column_error(
            column,
            Some(&surface_binding.descriptor.public_name),
            &surface_binding
                .exposed_columns
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            None,
        )),
    }
}

fn parse_state_history_predicate(
    expr: &Expr,
    surface_binding: &SurfaceBinding,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<StateHistoryPredicate>, LixError> {
    match expr {
        Expr::Nested(inner) => {
            parse_state_history_predicate(inner, surface_binding, params, placeholder_state)
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(field) = direct_state_history_field_from_expr(surface_binding, left)? {
                if let Some(value) = direct_value_from_expr(right, params, placeholder_state)? {
                    return Ok(state_history_predicate_from_operator(field, op, value));
                }
                if let Expr::InList { .. } = right.as_ref() {
                    return Ok(None);
                }
            }
            if let Some(field) = direct_state_history_field_from_expr(surface_binding, right)? {
                if let Some(value) = direct_value_from_expr(left, params, placeholder_state)? {
                    return Ok(state_history_predicate_from_reversed_operator(
                        field, op, value,
                    ));
                }
            }
            Ok(None)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if *negated {
                return Ok(None);
            }
            let Some(field) = direct_state_history_field_from_expr(surface_binding, expr)? else {
                return Ok(None);
            };
            let mut values = Vec::new();
            for item in list {
                let Some(value) = direct_value_from_expr(item, params, placeholder_state)? else {
                    return Ok(None);
                };
                values.push(value);
            }
            Ok(Some(StateHistoryPredicate::In(field, values)))
        }
        Expr::IsNull(expr) => direct_state_history_field_from_expr(surface_binding, expr)?
            .map(StateHistoryPredicate::IsNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "direct state-history execution does not support this predicate shape",
                )
            }),
        Expr::IsNotNull(expr) => direct_state_history_field_from_expr(surface_binding, expr)?
            .map(StateHistoryPredicate::IsNotNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "direct state-history execution does not support this predicate shape",
                )
            }),
        _ => Ok(None),
    }
}

fn state_history_predicate_from_operator(
    field: DirectStateHistoryField,
    op: &BinaryOperator,
    value: Value,
) -> Option<StateHistoryPredicate> {
    match op {
        BinaryOperator::Eq => Some(StateHistoryPredicate::Eq(field, value)),
        BinaryOperator::NotEq => Some(StateHistoryPredicate::NotEq(field, value)),
        BinaryOperator::Gt => Some(StateHistoryPredicate::Gt(field, value)),
        BinaryOperator::GtEq => Some(StateHistoryPredicate::GtEq(field, value)),
        BinaryOperator::Lt => Some(StateHistoryPredicate::Lt(field, value)),
        BinaryOperator::LtEq => Some(StateHistoryPredicate::LtEq(field, value)),
        _ => None,
    }
}

fn state_history_predicate_from_reversed_operator(
    field: DirectStateHistoryField,
    op: &BinaryOperator,
    value: Value,
) -> Option<StateHistoryPredicate> {
    match op {
        BinaryOperator::Eq => Some(StateHistoryPredicate::Eq(field, value)),
        BinaryOperator::NotEq => Some(StateHistoryPredicate::NotEq(field, value)),
        BinaryOperator::Gt => Some(StateHistoryPredicate::Lt(field, value)),
        BinaryOperator::GtEq => Some(StateHistoryPredicate::LtEq(field, value)),
        BinaryOperator::Lt => Some(StateHistoryPredicate::Gt(field, value)),
        BinaryOperator::LtEq => Some(StateHistoryPredicate::GtEq(field, value)),
        _ => None,
    }
}

fn direct_value_from_expr(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<Value>, LixError> {
    match expr {
        Expr::Nested(inner) => direct_value_from_expr(inner, params, placeholder_state),
        Expr::UnaryOp { op, expr } => {
            let Some(value) = direct_value_from_expr(expr, params, placeholder_state)? else {
                return Ok(None);
            };
            match (op, value) {
                (sqlparser::ast::UnaryOperator::Minus, Value::Integer(value)) => {
                    Ok(Some(Value::Integer(-value)))
                }
                (sqlparser::ast::UnaryOperator::Minus, Value::Real(value)) => {
                    Ok(Some(Value::Real(-value)))
                }
                (sqlparser::ast::UnaryOperator::Plus, value) => Ok(Some(value)),
                _ => Ok(None),
            }
        }
        Expr::Value(value) => match &value.value {
            SqlValue::Placeholder(token) => {
                let index = resolve_placeholder_index(token, params.len(), placeholder_state)?;
                Ok(Some(params[index].clone()))
            }
            value => Ok(Some(sql_value_to_engine_value(value)?)),
        },
        _ => Ok(None),
    }
}

fn sql_value_to_engine_value(value: &SqlValue) -> Result<Value, LixError> {
    match value {
        SqlValue::Number(raw, _) => raw
            .parse::<i64>()
            .map(Value::Integer)
            .or_else(|_| raw.parse::<f64>().map(Value::Real))
            .map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("could not parse numeric literal '{raw}'"),
                )
            }),
        SqlValue::SingleQuotedString(text)
        | SqlValue::DoubleQuotedString(text)
        | SqlValue::TripleSingleQuotedString(text)
        | SqlValue::TripleDoubleQuotedString(text)
        | SqlValue::EscapedStringLiteral(text)
        | SqlValue::UnicodeStringLiteral(text)
        | SqlValue::NationalStringLiteral(text)
        | SqlValue::SingleQuotedRawStringLiteral(text)
        | SqlValue::DoubleQuotedRawStringLiteral(text)
        | SqlValue::TripleSingleQuotedRawStringLiteral(text)
        | SqlValue::TripleDoubleQuotedRawStringLiteral(text)
        | SqlValue::SingleQuotedByteStringLiteral(text)
        | SqlValue::DoubleQuotedByteStringLiteral(text)
        | SqlValue::TripleSingleQuotedByteStringLiteral(text)
        | SqlValue::TripleDoubleQuotedByteStringLiteral(text) => Ok(Value::Text(text.clone())),
        SqlValue::Boolean(value) => Ok(Value::Boolean(*value)),
        SqlValue::Null => Ok(Value::Null),
        SqlValue::DollarQuotedString(text) => Ok(Value::Text(text.value.clone())),
        SqlValue::HexStringLiteral(text) => {
            Ok(Value::Blob(decode_hex_literal(text).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("could not parse hex literal '{text}': {error}"),
                )
            })?))
        }
        SqlValue::Placeholder(_) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "unexpected placeholder literal during direct state-history preparation",
        )),
    }
}

fn direct_limit_values(
    limit_clause: Option<&LimitClause>,
    params: &[Value],
) -> Result<(Option<u64>, u64), LixError> {
    let Some(limit_clause) = limit_clause else {
        return Ok((None, 0));
    };

    let mut placeholder_state = PlaceholderState::new();
    match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if !limit_by.is_empty() {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "direct state-history execution does not support LIMIT BY",
                ));
            }
            let limit = limit
                .as_ref()
                .map(|expr| direct_u64_from_expr(expr, params, &mut placeholder_state))
                .transpose()?;
            let offset = offset
                .as_ref()
                .map(|offset| direct_u64_from_expr(&offset.value, params, &mut placeholder_state))
                .transpose()?
                .unwrap_or(0);
            Ok((limit, offset))
        }
        LimitClause::OffsetCommaLimit { offset, limit } => Ok((
            Some(direct_u64_from_expr(limit, params, &mut placeholder_state)?),
            direct_u64_from_expr(offset, params, &mut placeholder_state)?,
        )),
    }
}

fn decode_hex_literal(text: &str) -> Result<Vec<u8>, &'static str> {
    if text.len() % 2 != 0 {
        return Err("hex literal must have even length");
    }

    let mut bytes = Vec::with_capacity(text.len() / 2);
    let mut chars = text.as_bytes().chunks_exact(2);
    for pair in &mut chars {
        let high = decode_hex_nibble(pair[0])?;
        let low = decode_hex_nibble(pair[1])?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn decode_hex_nibble(byte: u8) -> Result<u8, &'static str> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("hex literal contains non-hex characters"),
    }
}

fn direct_u64_from_expr(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<u64, LixError> {
    let Some(value) = direct_value_from_expr(expr, params, placeholder_state)? else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct state-history execution requires literal LIMIT/OFFSET values",
        ));
    };
    match value {
        Value::Integer(value) if value >= 0 => Ok(value as u64),
        Value::Text(text) => text.parse::<u64>().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("could not parse LIMIT/OFFSET value '{text}'"),
            )
        }),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct state-history execution requires integer LIMIT/OFFSET values",
        )),
    }
}

async fn execute_direct_state_history_read(
    backend: &dyn LixBackend,
    plan: &StateHistoryDirectReadPlan,
) -> Result<QueryResult, LixError> {
    let mut rows = load_state_history_rows(backend, &plan.request).await?;
    rows.retain(|row| state_history_row_matches_predicates(row, &plan.predicates));
    rows.sort_by(|left, right| compare_state_history_rows(left, right, &plan.sort_keys));

    let offset = usize::try_from(plan.offset).unwrap_or(usize::MAX);
    let limit = plan.limit.and_then(|value| usize::try_from(value).ok());
    let rows = rows.into_iter().skip(offset);
    let rows = if let Some(limit) = limit {
        rows.take(limit).collect::<Vec<_>>()
    } else {
        rows.collect::<Vec<_>>()
    };

    let columns = if plan.wildcard_projection {
        plan.wildcard_columns.clone()
    } else {
        plan.projections
            .iter()
            .map(|projection| projection.output_name.clone())
            .collect()
    };
    let rows = rows
        .into_iter()
        .map(|row| project_state_history_row(&row, plan))
        .collect();

    Ok(decode_public_read_result_columns(
        QueryResult { rows, columns },
        &plan.result_columns,
    ))
}

fn state_history_row_matches_predicates(
    row: &StateHistoryRow,
    predicates: &[StateHistoryPredicate],
) -> bool {
    predicates
        .iter()
        .all(|predicate| state_history_row_matches_predicate(row, predicate))
}

fn state_history_row_matches_predicate(
    row: &StateHistoryRow,
    predicate: &StateHistoryPredicate,
) -> bool {
    match predicate {
        StateHistoryPredicate::Eq(field, value) => state_history_field_value(row, field) == *value,
        StateHistoryPredicate::NotEq(field, value) => {
            state_history_field_value(row, field) != *value
        }
        StateHistoryPredicate::Gt(field, value) => {
            compare_public_values(&state_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt())
        }
        StateHistoryPredicate::GtEq(field, value) => {
            compare_public_values(&state_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq())
        }
        StateHistoryPredicate::Lt(field, value) => {
            compare_public_values(&state_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt())
        }
        StateHistoryPredicate::LtEq(field, value) => {
            compare_public_values(&state_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq())
        }
        StateHistoryPredicate::In(field, values) => values
            .iter()
            .any(|value| state_history_field_value(row, field) == *value),
        StateHistoryPredicate::IsNull(field) => {
            matches!(state_history_field_value(row, field), Value::Null)
        }
        StateHistoryPredicate::IsNotNull(field) => {
            !matches!(state_history_field_value(row, field), Value::Null)
        }
    }
}

fn compare_state_history_rows(
    left: &StateHistoryRow,
    right: &StateHistoryRow,
    sort_keys: &[StateHistorySortKey],
) -> std::cmp::Ordering {
    if sort_keys.is_empty() {
        return std::cmp::Ordering::Equal;
    }

    for key in sort_keys {
        let Some(field) = &key.field else {
            continue;
        };
        let ordering = compare_public_values(
            &state_history_field_value(left, field),
            &state_history_field_value(right, field),
        )
        .unwrap_or(std::cmp::Ordering::Equal);
        if ordering != std::cmp::Ordering::Equal {
            return if key.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    std::cmp::Ordering::Equal
}

fn project_state_history_row(
    row: &StateHistoryRow,
    plan: &StateHistoryDirectReadPlan,
) -> Vec<Value> {
    if plan.wildcard_projection {
        return plan
            .wildcard_columns
            .iter()
            .map(|column| {
                direct_state_history_field_from_column_name_for_projection(column)
                    .map(|field| state_history_field_value(row, &field))
                    .unwrap_or(Value::Null)
            })
            .collect();
    }

    plan.projections
        .iter()
        .map(|projection| state_history_field_value(row, &projection.field))
        .collect()
}

fn direct_state_history_field_from_column_name_for_projection(
    column: &str,
) -> Option<DirectStateHistoryField> {
    match column.to_ascii_lowercase().as_str() {
        "entity_id" => Some(DirectStateHistoryField::EntityId),
        "schema_key" => Some(DirectStateHistoryField::SchemaKey),
        "file_id" => Some(DirectStateHistoryField::FileId),
        "plugin_key" => Some(DirectStateHistoryField::PluginKey),
        "snapshot_content" => Some(DirectStateHistoryField::SnapshotContent),
        "metadata" => Some(DirectStateHistoryField::Metadata),
        "schema_version" => Some(DirectStateHistoryField::SchemaVersion),
        "change_id" => Some(DirectStateHistoryField::ChangeId),
        "commit_id" => Some(DirectStateHistoryField::CommitId),
        "commit_created_at" => Some(DirectStateHistoryField::CommitCreatedAt),
        "root_commit_id" => Some(DirectStateHistoryField::RootCommitId),
        "depth" => Some(DirectStateHistoryField::Depth),
        "version_id" => Some(DirectStateHistoryField::VersionId),
        _ => None,
    }
}

fn state_history_field_value(row: &StateHistoryRow, field: &DirectStateHistoryField) -> Value {
    match field {
        DirectStateHistoryField::EntityId => Value::Text(row.entity_id.clone()),
        DirectStateHistoryField::SchemaKey => Value::Text(row.schema_key.clone()),
        DirectStateHistoryField::FileId => Value::Text(row.file_id.clone()),
        DirectStateHistoryField::PluginKey => Value::Text(row.plugin_key.clone()),
        DirectStateHistoryField::SnapshotContent => row
            .snapshot_content
            .as_ref()
            .map(|value: &String| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        DirectStateHistoryField::Metadata => row
            .metadata
            .as_ref()
            .map(|value: &String| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        DirectStateHistoryField::SchemaVersion => Value::Text(row.schema_version.clone()),
        DirectStateHistoryField::ChangeId => Value::Text(row.change_id.clone()),
        DirectStateHistoryField::CommitId => Value::Text(row.commit_id.clone()),
        DirectStateHistoryField::CommitCreatedAt => Value::Text(row.commit_created_at.clone()),
        DirectStateHistoryField::RootCommitId => Value::Text(row.root_commit_id.clone()),
        DirectStateHistoryField::Depth => Value::Integer(row.depth),
        DirectStateHistoryField::VersionId => Value::Text(row.version_id.clone()),
    }
}

fn state_history_predicate_field(predicate: &StateHistoryPredicate) -> DirectStateHistoryField {
    match predicate {
        StateHistoryPredicate::Eq(field, _)
        | StateHistoryPredicate::NotEq(field, _)
        | StateHistoryPredicate::Gt(field, _)
        | StateHistoryPredicate::GtEq(field, _)
        | StateHistoryPredicate::Lt(field, _)
        | StateHistoryPredicate::LtEq(field, _)
        | StateHistoryPredicate::In(field, _)
        | StateHistoryPredicate::IsNull(field)
        | StateHistoryPredicate::IsNotNull(field) => field.clone(),
    }
}

fn direct_state_history_field_name(field: &DirectStateHistoryField) -> &'static str {
    match field {
        DirectStateHistoryField::EntityId => "entity_id",
        DirectStateHistoryField::SchemaKey => "schema_key",
        DirectStateHistoryField::FileId => "file_id",
        DirectStateHistoryField::PluginKey => "plugin_key",
        DirectStateHistoryField::SnapshotContent => "snapshot_content",
        DirectStateHistoryField::Metadata => "metadata",
        DirectStateHistoryField::SchemaVersion => "schema_version",
        DirectStateHistoryField::ChangeId => "change_id",
        DirectStateHistoryField::CommitId => "commit_id",
        DirectStateHistoryField::CommitCreatedAt => "commit_created_at",
        DirectStateHistoryField::RootCommitId => "root_commit_id",
        DirectStateHistoryField::Depth => "depth",
        DirectStateHistoryField::VersionId => "version_id",
    }
}

fn compare_public_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Integer(left), Value::Integer(right)) => Some(left.cmp(right)),
        (Value::Real(left), Value::Real(right)) => left.partial_cmp(right),
        (Value::Integer(left), Value::Real(right)) => (*left as f64).partial_cmp(right),
        (Value::Real(left), Value::Integer(right)) => left.partial_cmp(&(*right as f64)),
        (Value::Text(left), Value::Text(right)) => Some(left.cmp(right)),
        (Value::Boolean(left), Value::Boolean(right)) => Some(left.cmp(right)),
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        _ => None,
    }
}

fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(text) => Some(text.as_str()),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(value) => Some(*value),
        _ => None,
    }
}

enum SpecializedPublicReadPreparation {
    Prepared(PreparedPublicRead),
    Declined { reason: String },
}

async fn try_prepare_public_read_via_specialized_optimization(
    backend: &dyn LixBackend,
    bound_statement: BoundStatement,
    active_version_id: &str,
    explain_envelope: Option<&ExplainEnvelope>,
    registry: &SurfaceRegistry,
) -> Result<SpecializedPublicReadPreparation, LixError> {
    let canonicalized = match canonicalize_read(bound_statement.clone(), registry) {
        Ok(canonicalized) => canonicalized,
        Err(error) => {
            return Ok(SpecializedPublicReadPreparation::Declined {
                reason: error.message,
            })
        }
    };
    let structured_read =
        maybe_bind_active_history_root(backend, canonicalized.structured_read(), active_version_id)
            .await
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "public read preparation could not bind active history root",
                )
            })?;
    ensure_public_read_history_timeline_roots(
        backend,
        &requested_history_root_commit_ids_from_selection(structured_read.query.selection.as_ref()),
    )
    .await
    .map_err(|error| LixError::new(error.code, error.description))?;
    let dependency_spec = augment_dependency_spec_for_public_read(
        registry,
        &structured_read,
        derive_dependency_spec_from_structured_public_read(&structured_read),
    );
    if canonicalized.surface_binding.descriptor.surface_family == SurfaceFamily::State {
        if let Some(error) = unknown_public_state_schema_error(registry, dependency_spec.as_ref()) {
            return Err(error);
        }
    }
    let effective_state = build_effective_state(&structured_read, dependency_spec.as_ref());
    let known_live_layouts = load_known_live_layouts_for_public_read(
        backend,
        dependency_spec.as_ref(),
        effective_state.as_ref().map(|(request, _)| request),
    )
    .await?;
    let surface_binding = structured_read.surface_binding.clone();
    let effective_state_request = effective_state.as_ref().map(|(request, _)| request.clone());
    let effective_state_plan = effective_state.as_ref().map(|(_, plan)| plan.clone());
    let direct_execution =
        explain_envelope.is_none() && surface_binding.descriptor.public_name == "lix_state_history";

    let (execution, pushdown_decision, lowered_sql, dependency_spec) = if direct_execution {
        match build_direct_state_history_plan(&structured_read) {
            Ok(Some(plan)) => {
                let pushdown_decision = direct_state_history_pushdown_decision(&plan);
                (
                    PreparedPublicReadExecution::Direct(DirectPublicReadPlan::StateHistory(plan)),
                    pushdown_decision,
                    Vec::new(),
                    dependency_spec,
                )
            }
            Ok(None) => {
                return Ok(SpecializedPublicReadPreparation::Declined {
                    reason: format!(
                        "specialized read optimization declined '{}'",
                        structured_read.surface_binding.descriptor.public_name
                    ),
                })
            }
            Err(error) if specialized_public_read_error_is_semantic(&error) => return Err(error),
            Err(error) => {
                return Ok(SpecializedPublicReadPreparation::Declined {
                    reason: error.description,
                })
            }
        }
    } else {
        let lowered_read = match lower_read_for_execution_with_layouts(
            backend.dialect(),
            &structured_read,
            effective_state.as_ref().map(|(request, _)| request),
            effective_state.as_ref().map(|(_, plan)| plan),
            &known_live_layouts,
        ) {
            Ok(Some(program)) => wrap_lowered_read_for_explain(program, explain_envelope),
            Ok(None) => {
                return Ok(SpecializedPublicReadPreparation::Declined {
                    reason: format!(
                        "specialized read optimization declined '{}'",
                        structured_read.surface_binding.descriptor.public_name
                    ),
                })
            }
            Err(error) if specialized_public_read_error_is_semantic(&error) => return Err(error),
            Err(error) => {
                return Ok(SpecializedPublicReadPreparation::Declined {
                    reason: error.description,
                })
            }
        };
        let lowered_sql = lowered_read
            .statements
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let pushdown_decision = Some(lowered_read.pushdown_decision.clone());
        (
            PreparedPublicReadExecution::LoweredSql(lowered_read),
            pushdown_decision,
            lowered_sql,
            dependency_spec,
        )
    };

    Ok(SpecializedPublicReadPreparation::Prepared(
        PreparedPublicRead {
            optimization: Some(PublicReadOptimization {
                structured_read,
                effective_state_request: effective_state_request.clone(),
                effective_state_plan: effective_state_plan.clone(),
            }),
            debug_trace: PublicExecutionDebugTrace {
                bound_statements: vec![bound_statement],
                surface_bindings: vec![surface_binding.descriptor.public_name.clone()],
                bound_public_leaves: vec![bound_public_leaf(&surface_binding)],
                dependency_spec: dependency_spec.clone(),
                effective_state_request,
                effective_state_plan,
                pushdown_decision,
                write_command: None,
                scope_proof: None,
                schema_proof: None,
                target_set_proof: None,
                resolved_write_plan: None,
                domain_change_batches: Vec::new(),
                commit_preconditions: Vec::new(),
                invariant_trace: None,
                write_phase_trace: Vec::new(),
                lowered_sql,
            },
            dependency_spec,
            execution,
        },
    ))
}

fn direct_state_history_pushdown_decision(
    plan: &StateHistoryDirectReadPlan,
) -> Option<PushdownDecision> {
    let mut accepted_predicates = Vec::new();
    if let StateHistoryRootScope::RequestedRoots(root_commit_ids) = &plan.request.root_scope {
        for root_commit_id in root_commit_ids {
            accepted_predicates.push(format!("root_commit_id = '{root_commit_id}'"));
        }
    }

    Some(PushdownDecision {
        accepted_predicates,
        rejected_predicates: Vec::new(),
        residual_predicates: Vec::new(),
    })
}

fn specialized_public_read_error_is_semantic(error: &LixError) -> bool {
    error.code == "LIX_ERROR_SQL_UNKNOWN_COLUMN"
        || error
            .description
            .contains("lix_state does not expose version_id")
}

pub(super) async fn try_prepare_public_read(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<Option<PreparedPublicRead>, LixError> {
    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
    try_prepare_public_read_with_registry_and_internal_access(
        backend,
        &registry,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
        false,
    )
    .await
}

pub(super) async fn try_prepare_public_read_with_registry_and_internal_access(
    backend: &dyn LixBackend,
    registry: &SurfaceRegistry,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
) -> Result<Option<PreparedPublicRead>, LixError> {
    try_prepare_public_read_with_internal_access(
        backend,
        registry,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
        allow_internal_tables,
    )
    .await
}

async fn try_prepare_public_read_with_internal_access(
    backend: &dyn LixBackend,
    registry: &SurfaceRegistry,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
) -> Result<Option<PreparedPublicRead>, LixError> {
    if parsed_statements.len() != 1 {
        return Ok(None);
    }
    if let Some(error) = public_read_preflight_error(&registry, &parsed_statements[0]) {
        return Err(error);
    }
    let Some((statement, explain_envelope)) = explain_query_statement(&parsed_statements[0]) else {
        return Ok(None);
    };
    let read_summary = summarize_bound_public_read_statement(&registry, &statement);
    if !allow_internal_tables && !read_summary.internal_relations.is_empty() {
        return Err(mixed_public_internal_query_error(
            &read_summary.internal_relations,
        ));
    }
    let bound_statement = BoundStatement::from_statement(
        statement,
        params.to_vec(),
        ExecutionContext {
            dialect: Some(backend.dialect()),
            writer_key: writer_key.map(ToString::to_string),
            requested_version_id: Some(active_version_id.to_string()),
        },
    );
    let mut attempted_broad_lowering = false;
    if read_summary.bound_surface_bindings.len() > 1 {
        attempted_broad_lowering = true;
        if let Some(prepared) = prepare_public_read_via_surface_lowering(
            backend,
            bound_statement.clone(),
            explain_envelope.as_ref(),
            &registry,
            allow_internal_tables,
        )
        .await?
        {
            return Ok(Some(prepared));
        }
    }
    let specialized = try_prepare_public_read_via_specialized_optimization(
        backend,
        bound_statement.clone(),
        active_version_id,
        explain_envelope.as_ref(),
        &registry,
    )
    .await?;
    match specialized {
        SpecializedPublicReadPreparation::Prepared(prepared) => return Ok(Some(prepared)),
        SpecializedPublicReadPreparation::Declined { reason } => {
            if !attempted_broad_lowering {
                if let Some(prepared) = prepare_public_read_via_surface_lowering(
                    backend,
                    bound_statement,
                    explain_envelope.as_ref(),
                    &registry,
                    allow_internal_tables,
                )
                .await?
                {
                    return Ok(Some(prepared));
                }
            }
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("public read preparation failed: {reason}"),
            ));
        }
    }
}

async fn prepare_public_read_via_surface_lowering(
    backend: &dyn LixBackend,
    bound_statement: BoundStatement,
    explain_envelope: Option<&ExplainEnvelope>,
    registry: &SurfaceRegistry,
    allow_internal_tables: bool,
) -> Result<Option<PreparedPublicRead>, LixError> {
    let read_summary = summarize_bound_public_read_statement(registry, &bound_statement.statement);
    ensure_public_read_history_timeline_roots(
        backend,
        &read_summary.requested_history_root_commit_ids,
    )
    .await
    .map_err(|error| LixError::new(error.code, error.description))?;
    if read_summary.bound_surface_bindings.is_empty() {
        return Ok(None);
    }
    if !allow_internal_tables && !read_summary.internal_relations.is_empty() {
        return Err(mixed_public_internal_query_error(
            &read_summary.internal_relations,
        ));
    }

    let mut rewritten_statement = bound_statement.statement.clone();
    rewrite_supported_public_read_surfaces_in_statement_with_registry_and_dialect(
        &mut rewritten_statement,
        registry,
        backend.dialect(),
    )?;
    if statement_references_public_surface(registry, &rewritten_statement) {
        return Ok(None);
    }
    if rewritten_statement == bound_statement.statement {
        return Ok(None);
    }

    let lowered_read = wrap_lowered_read_for_explain(
        LoweredReadProgram {
            statements: vec![rewritten_statement.clone()],
            pushdown_decision: PushdownDecision::default(),
            result_columns: LoweredResultColumns::Static(Vec::new()),
        },
        explain_envelope,
    );
    let dependency_spec = augment_dependency_spec_for_broad_public_read(
        registry,
        derive_dependency_spec_from_bound_public_surface_bindings(
            &read_summary.bound_surface_bindings,
        ),
    );
    if let Some(error) = unknown_public_state_schema_error(registry, dependency_spec.as_ref()) {
        return Err(error);
    }
    let bound_public_leaves = read_summary
        .bound_surface_bindings
        .iter()
        .map(bound_public_leaf)
        .collect::<Vec<_>>();

    Ok(Some(PreparedPublicRead {
        optimization: None,
        debug_trace: PublicExecutionDebugTrace {
            bound_statements: vec![bound_statement.clone()],
            surface_bindings: read_summary
                .bound_surface_bindings
                .iter()
                .map(|binding| binding.descriptor.public_name.clone())
                .collect(),
            bound_public_leaves,
            dependency_spec: dependency_spec.clone(),
            effective_state_request: None,
            effective_state_plan: None,
            pushdown_decision: Some(PushdownDecision::default()),
            write_command: None,
            scope_proof: None,
            schema_proof: None,
            target_set_proof: None,
            resolved_write_plan: None,
            domain_change_batches: Vec::new(),
            commit_preconditions: Vec::new(),
            invariant_trace: None,
            write_phase_trace: Vec::new(),
            lowered_sql: lowered_read
                .statements
                .iter()
                .map(ToString::to_string)
                .collect(),
        },
        dependency_spec,
        execution: PreparedPublicReadExecution::LoweredSql(lowered_read),
    }))
}

pub(super) async fn prepare_public_read(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Option<PreparedPublicRead> {
    try_prepare_public_read(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
    )
    .await
    .ok()
    .flatten()
}

pub(super) async fn prepare_public_read_strict(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<Option<PreparedPublicRead>, LixError> {
    try_prepare_public_read(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
    )
    .await
}
