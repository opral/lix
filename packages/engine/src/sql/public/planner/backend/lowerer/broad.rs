use super::*;
use crate::sql::public::planner::ir::{
    BroadPublicReadJoin, BroadPublicReadQuery, BroadPublicReadRelation, BroadPublicReadSelect,
    BroadPublicReadSetExpr, BroadPublicReadStatement, BroadPublicReadTableFactor,
    BroadPublicReadTableWithJoins, BroadPublicReadWith,
};
use sqlparser::ast::With;

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
    let Some(bound_statement) = bind_broad_public_read_statement(statement, registry)? else {
        return Ok(());
    };
    *statement = lower_broad_public_read_statement(&bound_statement, registry)?;
    Ok(())
}

pub(crate) fn summarize_bound_public_read_statement_with_registry(
    statement: &Statement,
    registry: &SurfaceRegistry,
) -> Result<Option<BroadPublicRelationSummary>, LixError> {
    let Some(bound_statement) = bind_broad_public_read_statement(statement, registry)? else {
        return Ok(None);
    };
    let mut summary = BroadPublicRelationSummary::default();
    collect_broad_public_read_statement_summary(&bound_statement, registry, &mut summary)?;
    Ok(Some(summary))
}

fn bind_broad_public_read_statement(
    statement: &Statement,
    registry: &SurfaceRegistry,
) -> Result<Option<BroadPublicReadStatement>, LixError> {
    match statement {
        Statement::Query(query) => Ok(Some(BroadPublicReadStatement::Query(
            bind_broad_public_read_query_scoped(query, registry, &BTreeSet::new())?,
        ))),
        Statement::Explain {
            statement: inner, ..
        } => {
            let Some(bound_inner) = bind_broad_public_read_statement(inner, registry)? else {
                return Ok(None);
            };
            Ok(Some(BroadPublicReadStatement::Explain {
                original: statement.clone(),
                statement: Box::new(bound_inner),
            }))
        }
        _ => Ok(None),
    }
}

fn bind_broad_public_read_query_scoped(
    query: &Query,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadQuery, LixError> {
    let mut scoped_ctes = visible_ctes.clone();
    let with = if let Some(with) = &query.with {
        let mut cte_scope = visible_ctes.clone();
        let mut cte_tables = Vec::with_capacity(with.cte_tables.len());
        for cte in &with.cte_tables {
            cte_tables.push(bind_broad_public_read_query_scoped(
                &cte.query, registry, &cte_scope,
            )?);
            cte_scope.insert(cte.alias.name.value.to_ascii_lowercase());
        }
        scoped_ctes = cte_scope;
        Some(BroadPublicReadWith {
            original: with.clone(),
            cte_tables,
        })
    } else {
        None
    };

    Ok(BroadPublicReadQuery {
        original: query.clone(),
        with,
        body: bind_broad_public_read_set_expr(query.body.as_ref(), registry, &scoped_ctes)?,
    })
}

fn bind_broad_public_read_set_expr(
    expr: &SetExpr,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadSetExpr, LixError> {
    match expr {
        SetExpr::Select(select) => Ok(BroadPublicReadSetExpr::Select(BroadPublicReadSelect {
            original: select.as_ref().clone(),
            from: select
                .from
                .iter()
                .map(|table| bind_broad_public_read_table_with_joins(table, registry, visible_ctes))
                .collect::<Result<_, _>>()?,
        })),
        SetExpr::Query(query) => Ok(BroadPublicReadSetExpr::Query(Box::new(
            bind_broad_public_read_query_scoped(query, registry, visible_ctes)?,
        ))),
        SetExpr::SetOperation { left, right, .. } => Ok(BroadPublicReadSetExpr::SetOperation {
            original: expr.clone(),
            left: Box::new(bind_broad_public_read_set_expr(
                left,
                registry,
                visible_ctes,
            )?),
            right: Box::new(bind_broad_public_read_set_expr(
                right,
                registry,
                visible_ctes,
            )?),
        }),
        SetExpr::Table(table) => {
            let Some(table_name) = table.table_name.as_deref() else {
                return Ok(BroadPublicReadSetExpr::Other(expr.clone()));
            };
            Ok(BroadPublicReadSetExpr::Table {
                original: expr.clone(),
                relation: classify_broad_public_read_relation(table_name, registry, visible_ctes),
            })
        }
        _ => Ok(BroadPublicReadSetExpr::Other(expr.clone())),
    }
}

fn bind_broad_public_read_table_with_joins(
    table: &TableWithJoins,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadTableWithJoins, LixError> {
    Ok(BroadPublicReadTableWithJoins {
        original: table.clone(),
        relation: bind_broad_public_read_table_factor(&table.relation, registry, visible_ctes)?,
        joins: table
            .joins
            .iter()
            .map(|join| bind_broad_public_read_join(join, registry, visible_ctes))
            .collect::<Result<_, _>>()?,
    })
}

fn bind_broad_public_read_join(
    join: &sqlparser::ast::Join,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadJoin, LixError> {
    Ok(BroadPublicReadJoin {
        original: join.clone(),
        relation: bind_broad_public_read_table_factor(&join.relation, registry, visible_ctes)?,
    })
}

fn bind_broad_public_read_table_factor(
    relation: &TableFactor,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadTableFactor, LixError> {
    match relation {
        TableFactor::Table { name, .. } => {
            let Some(relation_name) = table_name_terminal(name) else {
                return Ok(BroadPublicReadTableFactor::Other(relation.clone()));
            };
            Ok(BroadPublicReadTableFactor::Table {
                original: relation.clone(),
                relation: classify_broad_public_read_relation(
                    relation_name,
                    registry,
                    visible_ctes,
                ),
            })
        }
        TableFactor::Derived { subquery, .. } => Ok(BroadPublicReadTableFactor::Derived {
            original: relation.clone(),
            subquery: Box::new(bind_broad_public_read_query_scoped(
                subquery,
                registry,
                visible_ctes,
            )?),
        }),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => Ok(BroadPublicReadTableFactor::NestedJoin {
            original: relation.clone(),
            table_with_joins: Box::new(bind_broad_public_read_table_with_joins(
                table_with_joins,
                registry,
                visible_ctes,
            )?),
        }),
        _ => Ok(BroadPublicReadTableFactor::Other(relation.clone())),
    }
}

fn classify_broad_public_read_relation(
    relation_name: &str,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> BroadPublicReadRelation {
    let normalized = relation_name.to_ascii_lowercase();
    if visible_ctes.contains(&normalized) {
        return BroadPublicReadRelation::Cte(normalized);
    }
    if let Some(binding) = registry.bind_relation_name(relation_name) {
        return BroadPublicReadRelation::Public(binding);
    }
    if normalized.starts_with("lix_internal_") {
        return BroadPublicReadRelation::Internal(normalized);
    }
    BroadPublicReadRelation::External(normalized)
}

fn lower_broad_public_read_statement(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
) -> Result<Statement, LixError> {
    match statement {
        BroadPublicReadStatement::Query(query) => Ok(Statement::Query(Box::new(
            lower_broad_public_read_query(query, registry)?,
        ))),
        BroadPublicReadStatement::Explain {
            original,
            statement: bound_statement,
        } => {
            let mut lowered = original.clone();
            if let Statement::Explain {
                statement: lowered_statement,
                ..
            } = &mut lowered
            {
                **lowered_statement =
                    lower_broad_public_read_statement(bound_statement.as_ref(), registry)?;
            }
            Ok(lowered)
        }
    }
}

fn lower_broad_public_read_query(
    query: &BroadPublicReadQuery,
    registry: &SurfaceRegistry,
) -> Result<Query, LixError> {
    let mut lowered = query.original.clone();
    lowered.with = query
        .with
        .as_ref()
        .map(|with| lower_broad_public_read_with(with, registry))
        .transpose()?;
    lowered.body = Box::new(lower_broad_public_read_set_expr(&query.body, registry)?);
    lower_nested_public_surfaces_in_query_expressions(&mut lowered, registry)?;
    Ok(lowered)
}

fn lower_broad_public_read_with(
    with: &BroadPublicReadWith,
    registry: &SurfaceRegistry,
) -> Result<With, LixError> {
    let mut lowered = with.original.clone();
    for (cte, bound_query) in lowered.cte_tables.iter_mut().zip(&with.cte_tables) {
        cte.query = Box::new(lower_broad_public_read_query(bound_query, registry)?);
    }
    Ok(lowered)
}

fn lower_broad_public_read_set_expr(
    expr: &BroadPublicReadSetExpr,
    registry: &SurfaceRegistry,
) -> Result<SetExpr, LixError> {
    match expr {
        BroadPublicReadSetExpr::Select(select) => Ok(SetExpr::Select(Box::new(
            lower_broad_public_read_select(select, registry)?,
        ))),
        BroadPublicReadSetExpr::Query(query) => Ok(SetExpr::Query(Box::new(
            lower_broad_public_read_query(query, registry)?,
        ))),
        BroadPublicReadSetExpr::SetOperation {
            original,
            left,
            right,
        } => {
            let mut lowered = original.clone();
            if let SetExpr::SetOperation {
                left: lowered_left,
                right: lowered_right,
                ..
            } = &mut lowered
            {
                *lowered_left =
                    Box::new(lower_broad_public_read_set_expr(left.as_ref(), registry)?);
                *lowered_right =
                    Box::new(lower_broad_public_read_set_expr(right.as_ref(), registry)?);
            }
            Ok(lowered)
        }
        BroadPublicReadSetExpr::Table { original, relation } => {
            lower_broad_public_read_table_relation(relation, original, registry)
        }
        BroadPublicReadSetExpr::Other(expr) => {
            let mut lowered = expr.clone();
            lower_nested_public_surfaces_in_set_expr_expressions(&mut lowered, registry)?;
            Ok(lowered)
        }
    }
}

fn lower_broad_public_read_select(
    select: &BroadPublicReadSelect,
    registry: &SurfaceRegistry,
) -> Result<Select, LixError> {
    let mut lowered = select.original.clone();
    lowered.from = select
        .from
        .iter()
        .map(|table| lower_broad_public_read_table_with_joins(table, registry))
        .collect::<Result<_, _>>()?;
    lower_nested_public_surfaces_in_select_expressions(&mut lowered, registry)?;
    Ok(lowered)
}

fn lower_broad_public_read_table_with_joins(
    table: &BroadPublicReadTableWithJoins,
    registry: &SurfaceRegistry,
) -> Result<TableWithJoins, LixError> {
    let mut lowered = table.original.clone();
    lowered.relation = lower_broad_public_read_table_factor(&table.relation, registry)?;
    lowered.joins = table
        .joins
        .iter()
        .map(|join| lower_broad_public_read_join(join, registry))
        .collect::<Result<_, _>>()?;
    Ok(lowered)
}

fn lower_broad_public_read_join(
    join: &BroadPublicReadJoin,
    registry: &SurfaceRegistry,
) -> Result<sqlparser::ast::Join, LixError> {
    let mut lowered = join.original.clone();
    lowered.relation = lower_broad_public_read_table_factor(&join.relation, registry)?;
    lower_nested_public_surfaces_in_join_operator(&mut lowered.join_operator, registry)?;
    Ok(lowered)
}

fn lower_broad_public_read_table_factor(
    relation: &BroadPublicReadTableFactor,
    registry: &SurfaceRegistry,
) -> Result<TableFactor, LixError> {
    match relation {
        BroadPublicReadTableFactor::Table { original, relation } => {
            lower_broad_public_read_relation(relation, original, registry)
        }
        BroadPublicReadTableFactor::Derived { original, subquery } => {
            let mut lowered = original.clone();
            if let TableFactor::Derived {
                subquery: lowered_subquery,
                ..
            } = &mut lowered
            {
                *lowered_subquery =
                    Box::new(lower_broad_public_read_query(subquery.as_ref(), registry)?);
            }
            Ok(lowered)
        }
        BroadPublicReadTableFactor::NestedJoin {
            original,
            table_with_joins,
        } => {
            let mut lowered = original.clone();
            if let TableFactor::NestedJoin {
                table_with_joins: lowered_table_with_joins,
                ..
            } = &mut lowered
            {
                *lowered_table_with_joins = Box::new(lower_broad_public_read_table_with_joins(
                    table_with_joins.as_ref(),
                    registry,
                )?);
            }
            Ok(lowered)
        }
        BroadPublicReadTableFactor::Other(relation) => Ok(relation.clone()),
    }
}

fn lower_broad_public_read_relation(
    relation: &BroadPublicReadRelation,
    original: &TableFactor,
    registry: &SurfaceRegistry,
) -> Result<TableFactor, LixError> {
    match relation {
        BroadPublicReadRelation::Public(binding) => {
            let Some(derived_query) = build_supported_public_read_surface_query(
                &binding.descriptor.public_name,
                registry,
                false,
            )?
            else {
                return Ok(original.clone());
            };
            let TableFactor::Table { alias, .. } = original else {
                return Ok(original.clone());
            };
            let derived_alias = alias.clone().or_else(|| {
                Some(TableAlias {
                    explicit: false,
                    name: Ident::new(&binding.descriptor.public_name),
                    columns: Vec::new(),
                })
            });
            Ok(TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            })
        }
        BroadPublicReadRelation::Internal(_)
        | BroadPublicReadRelation::External(_)
        | BroadPublicReadRelation::Cte(_) => Ok(original.clone()),
    }
}

fn lower_broad_public_read_table_relation(
    relation: &BroadPublicReadRelation,
    original: &SetExpr,
    registry: &SurfaceRegistry,
) -> Result<SetExpr, LixError> {
    match relation {
        BroadPublicReadRelation::Public(binding) => {
            let Some(derived_query) = build_supported_public_read_surface_query(
                &binding.descriptor.public_name,
                registry,
                true,
            )?
            else {
                return Ok(original.clone());
            };
            Ok(SetExpr::Query(Box::new(derived_query)))
        }
        BroadPublicReadRelation::Internal(_)
        | BroadPublicReadRelation::External(_)
        | BroadPublicReadRelation::Cte(_) => Ok(original.clone()),
    }
}

fn lower_nested_public_surfaces_in_query_expressions(
    query: &mut Query,
    registry: &SurfaceRegistry,
) -> Result<(), LixError> {
    if let Some(order_by) = &mut query.order_by {
        lower_nested_public_surfaces_in_order_by(order_by, registry)?;
    }
    if let Some(limit_clause) = &mut query.limit_clause {
        lower_nested_public_surfaces_in_limit_clause(limit_clause, registry)?;
    }
    if let Some(quantity) = query
        .fetch
        .as_mut()
        .and_then(|fetch| fetch.quantity.as_mut())
    {
        lower_nested_public_surfaces_in_expr(quantity, registry)?;
    }
    Ok(())
}

fn lower_nested_public_surfaces_in_set_expr_expressions(
    expr: &mut SetExpr,
    registry: &SurfaceRegistry,
) -> Result<(), LixError> {
    match expr {
        SetExpr::Select(select) => {
            lower_nested_public_surfaces_in_select_expressions(select, registry)
        }
        SetExpr::Query(query) => {
            *query = Box::new(lower_query_via_broad_binding(query.as_ref(), registry)?);
            Ok(())
        }
        SetExpr::SetOperation { left, right, .. } => {
            lower_nested_public_surfaces_in_set_expr_expressions(left, registry)?;
            lower_nested_public_surfaces_in_set_expr_expressions(right, registry)
        }
        SetExpr::Values(values) => {
            for row in &mut values.rows {
                for expr in row {
                    lower_nested_public_surfaces_in_expr(expr, registry)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn lower_nested_public_surfaces_in_select_expressions(
    select: &mut Select,
    registry: &SurfaceRegistry,
) -> Result<(), LixError> {
    if let Some(prewhere) = &mut select.prewhere {
        lower_nested_public_surfaces_in_expr(prewhere, registry)?;
    }
    if let Some(selection) = &mut select.selection {
        lower_nested_public_surfaces_in_expr(selection, registry)?;
    }
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                lower_nested_public_surfaces_in_expr(expr, registry)?;
            }
            SelectItem::QualifiedWildcard(
                sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
                _,
            ) => lower_nested_public_surfaces_in_expr(expr, registry)?,
            _ => {}
        }
    }
    match &mut select.group_by {
        GroupByExpr::All(_) => {}
        GroupByExpr::Expressions(expressions, _) => {
            for expr in expressions {
                lower_nested_public_surfaces_in_expr(expr, registry)?;
            }
        }
    }
    for expr in &mut select.cluster_by {
        lower_nested_public_surfaces_in_expr(expr, registry)?;
    }
    for expr in &mut select.distribute_by {
        lower_nested_public_surfaces_in_expr(expr, registry)?;
    }
    for expr in &mut select.sort_by {
        lower_nested_public_surfaces_in_order_by_expr(expr, registry)?;
    }
    if let Some(having) = &mut select.having {
        lower_nested_public_surfaces_in_expr(having, registry)?;
    }
    if let Some(qualify) = &mut select.qualify {
        lower_nested_public_surfaces_in_expr(qualify, registry)?;
    }
    if let Some(connect_by) = &mut select.connect_by {
        lower_nested_public_surfaces_in_expr(&mut connect_by.condition, registry)?;
        for expr in &mut connect_by.relationships {
            lower_nested_public_surfaces_in_expr(expr, registry)?;
        }
    }
    Ok(())
}

fn lower_nested_public_surfaces_in_order_by(
    order_by: &mut OrderBy,
    registry: &SurfaceRegistry,
) -> Result<(), LixError> {
    match &mut order_by.kind {
        sqlparser::ast::OrderByKind::All(_) => Ok(()),
        sqlparser::ast::OrderByKind::Expressions(expressions) => {
            for expr in expressions {
                lower_nested_public_surfaces_in_order_by_expr(expr, registry)?;
            }
            Ok(())
        }
    }
}

fn lower_nested_public_surfaces_in_order_by_expr(
    order_by_expr: &mut OrderByExpr,
    registry: &SurfaceRegistry,
) -> Result<(), LixError> {
    lower_nested_public_surfaces_in_expr(&mut order_by_expr.expr, registry)?;
    if let Some(with_fill) = &mut order_by_expr.with_fill {
        if let Some(from) = &mut with_fill.from {
            lower_nested_public_surfaces_in_expr(from, registry)?;
        }
        if let Some(to) = &mut with_fill.to {
            lower_nested_public_surfaces_in_expr(to, registry)?;
        }
        if let Some(step) = &mut with_fill.step {
            lower_nested_public_surfaces_in_expr(step, registry)?;
        }
    }
    Ok(())
}

fn lower_nested_public_surfaces_in_limit_clause(
    limit_clause: &mut LimitClause,
    registry: &SurfaceRegistry,
) -> Result<(), LixError> {
    match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if let Some(limit) = limit {
                lower_nested_public_surfaces_in_expr(limit, registry)?;
            }
            if let Some(offset) = offset {
                lower_nested_public_surfaces_in_expr(&mut offset.value, registry)?;
            }
            for expr in limit_by {
                lower_nested_public_surfaces_in_expr(expr, registry)?;
            }
            Ok(())
        }
        LimitClause::OffsetCommaLimit { offset, limit } => {
            lower_nested_public_surfaces_in_expr(offset, registry)?;
            lower_nested_public_surfaces_in_expr(limit, registry)
        }
    }
}

fn lower_nested_public_surfaces_in_join_operator(
    join_operator: &mut JoinOperator,
    registry: &SurfaceRegistry,
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
        lower_nested_public_surfaces_in_expr(expr, registry)?;
    }
    if let Some(constraint) = constraint {
        lower_nested_public_surfaces_in_join_constraint(constraint, registry)?;
    }
    Ok(())
}

fn lower_nested_public_surfaces_in_join_constraint(
    constraint: &mut JoinConstraint,
    registry: &SurfaceRegistry,
) -> Result<(), LixError> {
    match constraint {
        JoinConstraint::On(expr) => lower_nested_public_surfaces_in_expr(expr, registry),
        _ => Ok(()),
    }
}

fn lower_nested_public_surfaces_in_expr(
    expr: &mut Expr,
    registry: &SurfaceRegistry,
) -> Result<(), LixError> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            lower_nested_public_surfaces_in_expr(left, registry)?;
            lower_nested_public_surfaces_in_expr(right, registry)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => lower_nested_public_surfaces_in_expr(expr, registry),
        Expr::InList { expr, list, .. } => {
            lower_nested_public_surfaces_in_expr(expr, registry)?;
            for item in list {
                lower_nested_public_surfaces_in_expr(item, registry)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            lower_nested_public_surfaces_in_expr(expr, registry)?;
            lower_nested_public_surfaces_in_expr(low, registry)?;
            lower_nested_public_surfaces_in_expr(high, registry)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            lower_nested_public_surfaces_in_expr(expr, registry)?;
            lower_nested_public_surfaces_in_expr(pattern, registry)
        }
        Expr::Subquery(query) => {
            *query = Box::new(lower_query_via_broad_binding(query.as_ref(), registry)?);
            Ok(())
        }
        Expr::Exists { subquery, .. } => {
            *subquery = Box::new(lower_query_via_broad_binding(subquery.as_ref(), registry)?);
            Ok(())
        }
        Expr::InSubquery { expr, subquery, .. } => {
            lower_nested_public_surfaces_in_expr(expr, registry)?;
            *subquery = Box::new(lower_query_via_broad_binding(subquery.as_ref(), registry)?);
            Ok(())
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            lower_nested_public_surfaces_in_expr(expr, registry)?;
            lower_nested_public_surfaces_in_expr(array_expr, registry)
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            lower_nested_public_surfaces_in_expr(left, registry)?;
            lower_nested_public_surfaces_in_expr(right, registry)
        }
        Expr::Function(function) => {
            lower_nested_public_surfaces_in_function_args(&mut function.args, registry)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                lower_nested_public_surfaces_in_expr(operand, registry)?;
            }
            for condition in conditions {
                lower_nested_public_surfaces_in_expr(&mut condition.condition, registry)?;
                lower_nested_public_surfaces_in_expr(&mut condition.result, registry)?;
            }
            if let Some(else_result) = else_result {
                lower_nested_public_surfaces_in_expr(else_result, registry)?;
            }
            Ok(())
        }
        Expr::Tuple(items) => {
            for item in items {
                lower_nested_public_surfaces_in_expr(item, registry)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn lower_nested_public_surfaces_in_function_args(
    args: &mut FunctionArguments,
    registry: &SurfaceRegistry,
) -> Result<(), LixError> {
    match args {
        FunctionArguments::List(list) => {
            for arg in &mut list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        lower_nested_public_surfaces_in_expr(expr, registry)?;
                    }
                    FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                        if let FunctionArgExpr::Expr(expr) = arg {
                            lower_nested_public_surfaces_in_expr(expr, registry)?;
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

fn lower_query_via_broad_binding(
    query: &Query,
    registry: &SurfaceRegistry,
) -> Result<Query, LixError> {
    let bound = bind_broad_public_read_query_scoped(query, registry, &BTreeSet::new())?;
    lower_broad_public_read_query(&bound, registry)
}

fn collect_broad_public_read_statement_summary(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    out: &mut BroadPublicRelationSummary,
) -> Result<(), LixError> {
    match statement {
        BroadPublicReadStatement::Query(query) => {
            collect_broad_public_read_query_summary(query, registry, out)
        }
        BroadPublicReadStatement::Explain { statement, .. } => {
            collect_broad_public_read_statement_summary(statement, registry, out)
        }
    }
}

fn collect_broad_public_read_query_summary(
    query: &BroadPublicReadQuery,
    registry: &SurfaceRegistry,
    out: &mut BroadPublicRelationSummary,
) -> Result<(), LixError> {
    if let Some(with) = &query.with {
        for cte_query in &with.cte_tables {
            collect_broad_public_read_query_summary(cte_query, registry, out)?;
        }
    }
    collect_broad_public_read_set_expr_summary(&query.body, registry, out)
}

fn collect_broad_public_read_set_expr_summary(
    expr: &BroadPublicReadSetExpr,
    registry: &SurfaceRegistry,
    out: &mut BroadPublicRelationSummary,
) -> Result<(), LixError> {
    match expr {
        BroadPublicReadSetExpr::Select(select) => {
            collect_broad_public_read_select_summary(select, registry, out)
        }
        BroadPublicReadSetExpr::Query(query) => {
            collect_broad_public_read_query_summary(query, registry, out)
        }
        BroadPublicReadSetExpr::SetOperation { left, right, .. } => {
            collect_broad_public_read_set_expr_summary(left, registry, out)?;
            collect_broad_public_read_set_expr_summary(right, registry, out)
        }
        BroadPublicReadSetExpr::Table { relation, .. } => {
            collect_broad_public_read_relation_summary(relation, out);
            Ok(())
        }
        BroadPublicReadSetExpr::Other(expr) => {
            collect_nested_public_query_summaries_in_set_expr(expr, registry, out)
        }
    }
}

fn collect_broad_public_read_select_summary(
    select: &BroadPublicReadSelect,
    registry: &SurfaceRegistry,
    out: &mut BroadPublicRelationSummary,
) -> Result<(), LixError> {
    for table in &select.from {
        collect_broad_public_read_table_with_joins_summary(table, registry, out)?;
    }
    if let Some(prewhere) = &select.original.prewhere {
        collect_nested_public_query_summaries_in_expr(prewhere, registry, out)?;
    }
    if let Some(selection) = &select.original.selection {
        collect_nested_public_query_summaries_in_expr(selection, registry, out)?;
    }
    for item in &select.original.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
            }
            SelectItem::QualifiedWildcard(
                sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
                _,
            ) => collect_nested_public_query_summaries_in_expr(expr, registry, out)?,
            _ => {}
        }
    }
    match &select.original.group_by {
        GroupByExpr::All(_) => {}
        GroupByExpr::Expressions(expressions, _) => {
            for expr in expressions {
                collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
            }
        }
    }
    for expr in &select.original.cluster_by {
        collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
    }
    for expr in &select.original.distribute_by {
        collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
    }
    for expr in &select.original.sort_by {
        collect_nested_public_query_summaries_in_order_by_expr(expr, registry, out)?;
    }
    if let Some(having) = &select.original.having {
        collect_nested_public_query_summaries_in_expr(having, registry, out)?;
    }
    if let Some(qualify) = &select.original.qualify {
        collect_nested_public_query_summaries_in_expr(qualify, registry, out)?;
    }
    if let Some(connect_by) = &select.original.connect_by {
        collect_nested_public_query_summaries_in_expr(&connect_by.condition, registry, out)?;
        for expr in &connect_by.relationships {
            collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
        }
    }
    Ok(())
}

fn collect_broad_public_read_table_with_joins_summary(
    table: &BroadPublicReadTableWithJoins,
    registry: &SurfaceRegistry,
    out: &mut BroadPublicRelationSummary,
) -> Result<(), LixError> {
    collect_broad_public_read_table_factor_summary(&table.relation, registry, out)?;
    for join in &table.joins {
        collect_broad_public_read_table_factor_summary(&join.relation, registry, out)?;
        collect_nested_public_query_summaries_in_join_operator(
            &join.original.join_operator,
            registry,
            out,
        )?;
    }
    Ok(())
}

fn collect_broad_public_read_table_factor_summary(
    relation: &BroadPublicReadTableFactor,
    registry: &SurfaceRegistry,
    out: &mut BroadPublicRelationSummary,
) -> Result<(), LixError> {
    match relation {
        BroadPublicReadTableFactor::Table { relation, .. } => {
            collect_broad_public_read_relation_summary(relation, out);
            Ok(())
        }
        BroadPublicReadTableFactor::Derived { subquery, .. } => {
            collect_broad_public_read_query_summary(subquery, registry, out)
        }
        BroadPublicReadTableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_broad_public_read_table_with_joins_summary(table_with_joins, registry, out),
        BroadPublicReadTableFactor::Other(_) => Ok(()),
    }
}

fn collect_broad_public_read_relation_summary(
    relation: &BroadPublicReadRelation,
    out: &mut BroadPublicRelationSummary,
) {
    match relation {
        BroadPublicReadRelation::Public(binding) => {
            out.public_relations
                .insert(binding.descriptor.public_name.clone());
        }
        BroadPublicReadRelation::Internal(name) => {
            out.internal_relations.insert(name.clone());
        }
        BroadPublicReadRelation::External(name) => {
            out.external_relations.insert(name.clone());
        }
        BroadPublicReadRelation::Cte(_) => {}
    }
}

fn collect_nested_public_query_summaries_in_set_expr(
    expr: &SetExpr,
    registry: &SurfaceRegistry,
    out: &mut BroadPublicRelationSummary,
) -> Result<(), LixError> {
    match expr {
        SetExpr::Select(select) => {
            if let Some(prewhere) = &select.prewhere {
                collect_nested_public_query_summaries_in_expr(prewhere, registry, out)?;
            }
            if let Some(selection) = &select.selection {
                collect_nested_public_query_summaries_in_expr(selection, registry, out)?;
            }
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
                    }
                    SelectItem::QualifiedWildcard(
                        sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
                        _,
                    ) => collect_nested_public_query_summaries_in_expr(expr, registry, out)?,
                    _ => {}
                }
            }
            Ok(())
        }
        SetExpr::Query(query) => {
            let bound = bind_broad_public_read_query_scoped(query, registry, &BTreeSet::new())?;
            collect_broad_public_read_query_summary(&bound, registry, out)
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_nested_public_query_summaries_in_set_expr(left, registry, out)?;
            collect_nested_public_query_summaries_in_set_expr(right, registry, out)
        }
        SetExpr::Values(values) => {
            for row in &values.rows {
                for expr in row {
                    collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn collect_nested_public_query_summaries_in_order_by_expr(
    order_by_expr: &OrderByExpr,
    registry: &SurfaceRegistry,
    out: &mut BroadPublicRelationSummary,
) -> Result<(), LixError> {
    collect_nested_public_query_summaries_in_expr(&order_by_expr.expr, registry, out)?;
    if let Some(with_fill) = &order_by_expr.with_fill {
        if let Some(from) = &with_fill.from {
            collect_nested_public_query_summaries_in_expr(from, registry, out)?;
        }
        if let Some(to) = &with_fill.to {
            collect_nested_public_query_summaries_in_expr(to, registry, out)?;
        }
        if let Some(step) = &with_fill.step {
            collect_nested_public_query_summaries_in_expr(step, registry, out)?;
        }
    }
    Ok(())
}

fn collect_nested_public_query_summaries_in_join_operator(
    join_operator: &JoinOperator,
    registry: &SurfaceRegistry,
    out: &mut BroadPublicRelationSummary,
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
        collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
    }
    if let Some(JoinConstraint::On(expr)) = constraint {
        collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
    }
    Ok(())
}

fn collect_nested_public_query_summaries_in_expr(
    expr: &Expr,
    registry: &SurfaceRegistry,
    out: &mut BroadPublicRelationSummary,
) -> Result<(), LixError> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            collect_nested_public_query_summaries_in_expr(left, registry, out)?;
            collect_nested_public_query_summaries_in_expr(right, registry, out)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => {
            collect_nested_public_query_summaries_in_expr(expr, registry, out)
        }
        Expr::InList { expr, list, .. } => {
            collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
            for item in list {
                collect_nested_public_query_summaries_in_expr(item, registry, out)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
            collect_nested_public_query_summaries_in_expr(low, registry, out)?;
            collect_nested_public_query_summaries_in_expr(high, registry, out)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
            collect_nested_public_query_summaries_in_expr(pattern, registry, out)
        }
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        } => {
            let bound = bind_broad_public_read_query_scoped(query, registry, &BTreeSet::new())?;
            collect_broad_public_read_query_summary(&bound, registry, out)
        }
        Expr::InSubquery { expr, subquery, .. } => {
            collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
            let bound = bind_broad_public_read_query_scoped(subquery, registry, &BTreeSet::new())?;
            collect_broad_public_read_query_summary(&bound, registry, out)
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
            collect_nested_public_query_summaries_in_expr(array_expr, registry, out)
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            collect_nested_public_query_summaries_in_expr(left, registry, out)?;
            collect_nested_public_query_summaries_in_expr(right, registry, out)
        }
        Expr::Function(function) => match &function.args {
            FunctionArguments::List(list) => {
                for arg in &list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                            collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
                        }
                        FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                            if let FunctionArgExpr::Expr(expr) = arg {
                                collect_nested_public_query_summaries_in_expr(expr, registry, out)?;
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
                collect_nested_public_query_summaries_in_expr(operand, registry, out)?;
            }
            for condition in conditions {
                collect_nested_public_query_summaries_in_expr(&condition.condition, registry, out)?;
                collect_nested_public_query_summaries_in_expr(&condition.result, registry, out)?;
            }
            if let Some(else_result) = else_result {
                collect_nested_public_query_summaries_in_expr(else_result, registry, out)?;
            }
            Ok(())
        }
        Expr::Tuple(items) => {
            for item in items {
                collect_nested_public_query_summaries_in_expr(item, registry, out)?;
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
                "public-surface lowering requires fixed schema binding for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    };
    let Some(state_scan) = CanonicalStateScan::from_surface_binding(surface_binding.clone()) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "public-surface lowering could not build canonical state scan for '{}'",
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
            "public-surface lowering could not lower entity surface '{}'",
            surface_binding.descriptor.public_name
        ),
    })
}
