use super::*;

pub(crate) fn rewrite_supported_public_read_surfaces_in_statement(
    statement: &mut Statement,
) -> Result<(), LixError> {
    rewrite_supported_public_read_surfaces_in_statement_with_registry(
        statement,
        &SurfaceRegistry::with_builtin_surfaces(),
    )
}

pub(crate) fn rewrite_supported_public_read_surfaces_in_statement_with_registry(
    statement: &mut Statement,
    registry: &SurfaceRegistry,
) -> Result<(), LixError> {
    match statement {
        Statement::Query(query) => rewrite_supported_public_read_surfaces_in_query(query, registry),
        Statement::Explain { statement, .. } => {
            rewrite_supported_public_read_surfaces_in_statement_with_registry(statement, registry)
        }
        _ => Ok(()),
    }
}

fn rewrite_supported_public_read_surfaces_in_query(
    query: &mut Query,
    registry: &SurfaceRegistry,
) -> Result<(), LixError> {
    rewrite_supported_public_read_surfaces_in_query_scoped(query, registry, &BTreeSet::new(), true)
}

fn rewrite_supported_public_read_surfaces_in_query_scoped(
    query: &mut Query,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
    top_level: bool,
) -> Result<(), LixError> {
    let mut scoped_ctes = visible_ctes.clone();
    if let Some(with) = &mut query.with {
        let mut cte_scope = visible_ctes.clone();
        for cte in &mut with.cte_tables {
            rewrite_supported_public_read_surfaces_in_query_scoped(
                &mut cte.query,
                registry,
                &cte_scope,
                false,
            )?;
            cte_scope.insert(cte.alias.name.value.to_ascii_lowercase());
        }
        scoped_ctes = cte_scope;
    }

    rewrite_supported_public_read_surfaces_in_set_expr(
        query.body.as_mut(),
        registry,
        &scoped_ctes,
        top_level,
    )?;

    if let Some(order_by) = &mut query.order_by {
        rewrite_supported_public_read_surfaces_in_order_by(order_by, registry, &scoped_ctes)?;
    }
    if let Some(limit_clause) = &mut query.limit_clause {
        rewrite_supported_public_read_surfaces_in_limit_clause(
            limit_clause,
            registry,
            &scoped_ctes,
        )?;
    }
    if let Some(quantity) = query
        .fetch
        .as_mut()
        .and_then(|fetch| fetch.quantity.as_mut())
    {
        rewrite_supported_public_read_surfaces_in_expr(quantity, registry, &scoped_ctes)?;
    }
    Ok(())
}

fn rewrite_supported_public_read_surfaces_in_set_expr(
    expr: &mut SetExpr,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
    top_level: bool,
) -> Result<(), LixError> {
    match expr {
        SetExpr::Select(select) => rewrite_supported_public_read_surfaces_in_select(
            select,
            registry,
            visible_ctes,
            top_level,
        ),
        SetExpr::Query(query) => rewrite_supported_public_read_surfaces_in_query_scoped(
            query,
            registry,
            visible_ctes,
            false,
        ),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_supported_public_read_surfaces_in_set_expr(
                left.as_mut(),
                registry,
                visible_ctes,
                false,
            )?;
            rewrite_supported_public_read_surfaces_in_set_expr(
                right.as_mut(),
                registry,
                visible_ctes,
                false,
            )
        }
        _ => Ok(()),
    }
}

fn rewrite_supported_public_read_surfaces_in_select(
    select: &mut Select,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
    top_level: bool,
) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_supported_public_read_surfaces_in_table_with_joins(
            table,
            registry,
            visible_ctes,
            top_level,
        )?;
    }
    if let Some(prewhere) = &mut select.prewhere {
        rewrite_supported_public_read_surfaces_in_expr(prewhere, registry, visible_ctes)?;
    }
    if let Some(selection) = &mut select.selection {
        rewrite_supported_public_read_surfaces_in_expr(selection, registry, visible_ctes)?;
    }
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)?;
            }
            SelectItem::QualifiedWildcard(
                sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
                _,
            ) => rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)?,
            _ => {}
        }
    }
    rewrite_supported_public_read_surfaces_in_group_by(
        &mut select.group_by,
        registry,
        visible_ctes,
    )?;
    for expr in &mut select.cluster_by {
        rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)?;
    }
    for expr in &mut select.distribute_by {
        rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)?;
    }
    for expr in &mut select.sort_by {
        rewrite_supported_public_read_surfaces_in_order_by_expr(expr, registry, visible_ctes)?;
    }
    if let Some(having) = &mut select.having {
        rewrite_supported_public_read_surfaces_in_expr(having, registry, visible_ctes)?;
    }
    if let Some(qualify) = &mut select.qualify {
        rewrite_supported_public_read_surfaces_in_expr(qualify, registry, visible_ctes)?;
    }
    if let Some(connect_by) = &mut select.connect_by {
        rewrite_supported_public_read_surfaces_in_expr(
            &mut connect_by.condition,
            registry,
            visible_ctes,
        )?;
        for expr in &mut connect_by.relationships {
            rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)?;
        }
    }
    Ok(())
}

fn rewrite_supported_public_read_surfaces_in_table_with_joins(
    table: &mut TableWithJoins,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
    top_level: bool,
) -> Result<(), LixError> {
    rewrite_supported_public_read_surfaces_in_table_factor(
        &mut table.relation,
        registry,
        visible_ctes,
        top_level,
    )?;
    for join in &mut table.joins {
        rewrite_supported_public_read_surfaces_in_table_factor(
            &mut join.relation,
            registry,
            visible_ctes,
            top_level,
        )?;
        rewrite_supported_public_read_surfaces_in_join_operator(
            &mut join.join_operator,
            registry,
            visible_ctes,
        )?;
    }
    Ok(())
}

fn rewrite_supported_public_read_surfaces_in_table_factor(
    relation: &mut TableFactor,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
    top_level: bool,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let Some(surface_name) = table_name_terminal(name) else {
                return Ok(());
            };
            if visible_ctes.contains(&surface_name.to_ascii_lowercase()) {
                return Ok(());
            }
            let Some(derived_query) =
                build_supported_public_read_surface_query(surface_name, registry, top_level)?
            else {
                return Ok(());
            };
            let derived_alias = alias.clone().or_else(|| {
                Some(TableAlias {
                    explicit: false,
                    name: Ident::new(surface_name),
                    columns: Vec::new(),
                })
            });
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            Ok(())
        }
        TableFactor::Derived { subquery, .. } => {
            rewrite_supported_public_read_surfaces_in_query_scoped(
                subquery,
                registry,
                visible_ctes,
                false,
            )
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => rewrite_supported_public_read_surfaces_in_table_with_joins(
            table_with_joins,
            registry,
            visible_ctes,
            false,
        ),
        _ => Ok(()),
    }
}

fn rewrite_supported_public_read_surfaces_in_expr(
    expr: &mut Expr,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<(), LixError> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            rewrite_supported_public_read_surfaces_in_expr(left, registry, visible_ctes)?;
            rewrite_supported_public_read_surfaces_in_expr(right, registry, visible_ctes)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => {
            rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)
        }
        Expr::InList { expr, list, .. } => {
            rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)?;
            for item in list {
                rewrite_supported_public_read_surfaces_in_expr(item, registry, visible_ctes)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)?;
            rewrite_supported_public_read_surfaces_in_expr(low, registry, visible_ctes)?;
            rewrite_supported_public_read_surfaces_in_expr(high, registry, visible_ctes)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)?;
            rewrite_supported_public_read_surfaces_in_expr(pattern, registry, visible_ctes)
        }
        Expr::Subquery(query) => rewrite_supported_public_read_surfaces_in_query_scoped(
            query,
            registry,
            visible_ctes,
            false,
        ),
        Expr::Exists { subquery, .. } => rewrite_supported_public_read_surfaces_in_query_scoped(
            subquery,
            registry,
            visible_ctes,
            false,
        ),
        Expr::InSubquery { expr, subquery, .. } => {
            rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)?;
            rewrite_supported_public_read_surfaces_in_query_scoped(
                subquery,
                registry,
                visible_ctes,
                false,
            )
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)?;
            rewrite_supported_public_read_surfaces_in_expr(array_expr, registry, visible_ctes)
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            rewrite_supported_public_read_surfaces_in_expr(left, registry, visible_ctes)?;
            rewrite_supported_public_read_surfaces_in_expr(right, registry, visible_ctes)
        }
        Expr::Function(function) => rewrite_supported_public_read_surfaces_in_function_args(
            &mut function.args,
            registry,
            visible_ctes,
        ),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                rewrite_supported_public_read_surfaces_in_expr(operand, registry, visible_ctes)?;
            }
            for condition in conditions {
                rewrite_supported_public_read_surfaces_in_expr(
                    &mut condition.condition,
                    registry,
                    visible_ctes,
                )?;
                rewrite_supported_public_read_surfaces_in_expr(
                    &mut condition.result,
                    registry,
                    visible_ctes,
                )?;
            }
            if let Some(else_result) = else_result {
                rewrite_supported_public_read_surfaces_in_expr(
                    else_result,
                    registry,
                    visible_ctes,
                )?;
            }
            Ok(())
        }
        Expr::Tuple(items) => {
            for item in items {
                rewrite_supported_public_read_surfaces_in_expr(item, registry, visible_ctes)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn rewrite_supported_public_read_surfaces_in_group_by(
    group_by: &mut GroupByExpr,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<(), LixError> {
    match group_by {
        GroupByExpr::All(_) => Ok(()),
        GroupByExpr::Expressions(expressions, _) => {
            for expr in expressions {
                rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)?;
            }
            Ok(())
        }
    }
}

fn rewrite_supported_public_read_surfaces_in_order_by(
    order_by: &mut OrderBy,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<(), LixError> {
    match &mut order_by.kind {
        sqlparser::ast::OrderByKind::All(_) => Ok(()),
        sqlparser::ast::OrderByKind::Expressions(expressions) => {
            for expr in expressions {
                rewrite_supported_public_read_surfaces_in_order_by_expr(
                    expr,
                    registry,
                    visible_ctes,
                )?;
            }
            Ok(())
        }
    }
}

fn rewrite_supported_public_read_surfaces_in_order_by_expr(
    order_by_expr: &mut OrderByExpr,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<(), LixError> {
    rewrite_supported_public_read_surfaces_in_expr(
        &mut order_by_expr.expr,
        registry,
        visible_ctes,
    )?;
    if let Some(with_fill) = &mut order_by_expr.with_fill {
        if let Some(from) = &mut with_fill.from {
            rewrite_supported_public_read_surfaces_in_expr(from, registry, visible_ctes)?;
        }
        if let Some(to) = &mut with_fill.to {
            rewrite_supported_public_read_surfaces_in_expr(to, registry, visible_ctes)?;
        }
        if let Some(step) = &mut with_fill.step {
            rewrite_supported_public_read_surfaces_in_expr(step, registry, visible_ctes)?;
        }
    }
    Ok(())
}

fn rewrite_supported_public_read_surfaces_in_limit_clause(
    limit_clause: &mut LimitClause,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<(), LixError> {
    match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if let Some(limit) = limit {
                rewrite_supported_public_read_surfaces_in_expr(limit, registry, visible_ctes)?;
            }
            if let Some(offset) = offset {
                rewrite_supported_public_read_surfaces_in_expr(
                    &mut offset.value,
                    registry,
                    visible_ctes,
                )?;
            }
            for expr in limit_by {
                rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)?;
            }
            Ok(())
        }
        LimitClause::OffsetCommaLimit { offset, limit } => {
            rewrite_supported_public_read_surfaces_in_expr(offset, registry, visible_ctes)?;
            rewrite_supported_public_read_surfaces_in_expr(limit, registry, visible_ctes)
        }
    }
}

fn rewrite_supported_public_read_surfaces_in_join_operator(
    join_operator: &mut JoinOperator,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
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
        rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)?;
    }
    if let Some(constraint) = constraint {
        rewrite_supported_public_read_surfaces_in_join_constraint(
            constraint,
            registry,
            visible_ctes,
        )?;
    }
    Ok(())
}

fn rewrite_supported_public_read_surfaces_in_join_constraint(
    constraint: &mut JoinConstraint,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<(), LixError> {
    match constraint {
        JoinConstraint::On(expr) => {
            rewrite_supported_public_read_surfaces_in_expr(expr, registry, visible_ctes)
        }
        _ => Ok(()),
    }
}

fn rewrite_supported_public_read_surfaces_in_function_args(
    args: &mut FunctionArguments,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<(), LixError> {
    match args {
        FunctionArguments::List(list) => {
            for arg in &mut list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        rewrite_supported_public_read_surfaces_in_expr(
                            expr,
                            registry,
                            visible_ctes,
                        )?;
                    }
                    FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                        if let FunctionArgExpr::Expr(expr) = arg {
                            rewrite_supported_public_read_surfaces_in_expr(
                                expr,
                                registry,
                                visible_ctes,
                            )?;
                        }
                    }
                    _ => {}
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn build_supported_public_read_surface_query(
    surface_name: &str,
    registry: &SurfaceRegistry,
    _top_level: bool,
) -> Result<Option<Query>, LixError> {
    let Some(surface_binding) = registry.bind_relation_name(surface_name) else {
        return Ok(None);
    };

    match surface_binding.descriptor.surface_family {
        SurfaceFamily::State => build_public_state_surface_query(&surface_binding, registry),
        SurfaceFamily::Entity => build_builtin_entity_surface_query(&surface_binding).map(Some),
        SurfaceFamily::Filesystem => {
            build_nested_filesystem_surface_query(&surface_binding.descriptor.public_name)
        }
        SurfaceFamily::Admin => build_public_admin_surface_query(&surface_binding),
        SurfaceFamily::Change => build_public_change_surface_query(&surface_binding),
    }
}

fn build_public_state_surface_query(
    surface_binding: &SurfaceBinding,
    registry: &SurfaceRegistry,
) -> Result<Option<Query>, LixError> {
    let Some(state_scan) = CanonicalStateScan::from_surface_binding(surface_binding.clone()) else {
        return Ok(None);
    };
    let schema_set = registry
        .registered_state_backed_schema_keys()
        .into_iter()
        .collect();
    let request = EffectiveStateRequest {
        schema_set,
        version_scope: state_scan.version_scope,
        include_global_overlay: true,
        include_untracked_overlay: true,
        include_tombstones: state_scan.include_tombstones,
        predicate_classes: Vec::new(),
        required_columns: surface_binding.exposed_columns.clone(),
    };
    build_state_source_query(surface_binding, &request, &[])
}

fn build_public_admin_surface_query(
    surface_binding: &SurfaceBinding,
) -> Result<Option<Query>, LixError> {
    let Some(admin_scan) = CanonicalAdminScan::from_surface_binding(surface_binding.clone()) else {
        return Ok(None);
    };
    build_admin_source_query(admin_scan.kind).map(Some)
}

fn build_public_change_surface_query(
    surface_binding: &SurfaceBinding,
) -> Result<Option<Query>, LixError> {
    if CanonicalWorkingChangesScan::from_surface_binding(surface_binding.clone()).is_some() {
        return build_working_changes_source_query().map(Some);
    }
    if CanonicalChangeScan::from_surface_binding(surface_binding.clone()).is_some() {
        return build_change_source_query().map(Some);
    }
    Ok(None)
}

fn build_builtin_entity_surface_query(surface_binding: &SurfaceBinding) -> Result<Query, LixError> {
    let Some(schema_key) = surface_binding.implicit_overrides.fixed_schema_key.clone() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "sql2 public-surface rewrite requires fixed schema binding for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    };
    let Some(state_scan) = CanonicalStateScan::from_surface_binding(surface_binding.clone()) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "sql2 public-surface rewrite could not build canonical state scan for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    };
    let request = EffectiveStateRequest {
        schema_set: BTreeSet::from([schema_key]),
        version_scope: state_scan.version_scope,
        include_global_overlay: true,
        include_untracked_overlay: true,
        include_tombstones: state_scan.include_tombstones,
        predicate_classes: Vec::new(),
        required_columns: surface_binding.exposed_columns.clone(),
    };
    build_entity_source_query(surface_binding, &request, &[])?.ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "sql2 public-surface rewrite could not lower entity surface '{}'",
            surface_binding.descriptor.public_name
        ),
    })
}
