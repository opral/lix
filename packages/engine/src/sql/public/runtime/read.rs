use super::*;
use crate::sql::ast::utils::bind_sql;
use crate::sql::public::planner::canonicalize::canonicalize_read;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, JoinConstraint,
    JoinOperator, LimitClause, OrderBy, OrderByExpr, OrderByKind, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, TableWithJoins,
};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoweredPublicReadQuery {
    pub(crate) query: Query,
    pub(crate) required_schema_keys: BTreeSet<String>,
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
        crate::schema::registry::register_schema(backend, schema_key).await?;
    }
    let bound = bind_sql(
        &Statement::Query(Box::new(lowered.query)).to_string(),
        params,
        backend.dialect(),
    )?;
    backend.execute(&bound.sql, &bound.params).await
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
    if let Some(quantity) = query.fetch.as_mut().and_then(|fetch| fetch.quantity.as_mut()) {
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
        });
    }
    let active_version_id = load_active_version_id_for_public_read(backend).await?;
    let parsed = vec![Statement::Query(Box::new(query.clone()))];
    let prepared = try_prepare_public_read_with_internal_access(
        backend,
        &parsed,
        params,
        &active_version_id,
        None,
        true,
    )
    .await?;
    let (lowered, required_schema_keys) = if let Some(prepared) = prepared {
        let required_schema_keys =
            required_schema_keys_from_dependency_spec(prepared.dependency_spec.as_ref());
        (prepared.lowered_read, required_schema_keys)
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
        let lowered = lower_read_for_execution(
            &structured_read,
            effective_state.as_ref().map(|(request, _)| request),
            effective_state.as_ref().map(|(_, plan)| plan),
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
        if let Some(error) = unknown_public_state_schema_error(registry, dependency_spec.as_ref())
        {
            return Err(error);
        }
    }
    let effective_state = build_effective_state(&structured_read, dependency_spec.as_ref());
    let lowered_read = match lower_read_for_execution(
        &structured_read,
        effective_state.as_ref().map(|(request, _)| request),
        effective_state.as_ref().map(|(_, plan)| plan),
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
    let surface_binding = structured_read.surface_binding.clone();
    let effective_state_request = effective_state.as_ref().map(|(request, _)| request.clone());
    let effective_state_plan = effective_state.as_ref().map(|(_, plan)| plan.clone());

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
                pushdown_decision: Some(lowered_read.pushdown_decision.clone()),
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
            lowered_read,
        },
    ))
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
    try_prepare_public_read_with_internal_access(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
        false,
    )
    .await
}

pub(super) async fn try_prepare_public_read_with_internal_access(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
) -> Result<Option<PreparedPublicRead>, LixError> {
    if parsed_statements.len() != 1 {
        return Ok(None);
    }
    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
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
    rewrite_supported_public_read_surfaces_in_statement_with_registry(
        &mut rewritten_statement,
        registry,
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
        lowered_read,
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
