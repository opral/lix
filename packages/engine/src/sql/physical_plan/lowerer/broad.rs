use super::*;
use crate::schema::builtin::builtin_schema_definition;
use crate::sql::logical_plan::public_ir::{
    BroadNestedQueryExpr, BroadPublicReadAlias, BroadPublicReadCte, BroadPublicReadGroupBy,
    BroadPublicReadGroupByKind, BroadPublicReadJoin, BroadPublicReadLimitClause,
    BroadPublicReadLimitClauseKind, BroadPublicReadOrderBy, BroadPublicReadOrderByExpr,
    BroadPublicReadOrderByKind, BroadPublicReadProjectionItem, BroadPublicReadProjectionItemKind,
    BroadPublicReadQuery, BroadPublicReadRelation, BroadPublicReadSelect, BroadPublicReadSetExpr,
    BroadPublicReadSetOperationKind, BroadPublicReadSetQuantifier, BroadPublicReadStatement,
    BroadPublicReadTableFactor, BroadPublicReadTableWithJoins, BroadPublicReadWith, BroadSqlExpr,
    BroadSqlProvenance,
};
use serde_json::Value as JsonValue;
use sqlparser::ast::{JoinConstraint, JoinOperator, SetOperator, SetQuantifier, With};
#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    static FORBID_BROAD_BINDING_FOR_TEST: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
pub(crate) struct ForbidBroadBindingForTestGuard {
    previous: bool,
}

#[cfg(test)]
impl Drop for ForbidBroadBindingForTestGuard {
    fn drop(&mut self) {
        FORBID_BROAD_BINDING_FOR_TEST.set(self.previous);
    }
}

#[cfg(test)]
pub(crate) fn forbid_broad_binding_for_test() -> ForbidBroadBindingForTestGuard {
    let previous = FORBID_BROAD_BINDING_FOR_TEST.replace(true);
    ForbidBroadBindingForTestGuard { previous }
}

#[cfg(test)]
fn assert_broad_binding_allowed_for_test() {
    if FORBID_BROAD_BINDING_FOR_TEST.get() {
        panic!("broad binding must not run inside this test scope");
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RenderedBroadPublicReadStatement {
    pub(crate) shell_statement: Statement,
    pub(crate) relation_render_nodes: Vec<TerminalRelationRenderNode>,
}

pub(crate) fn lower_broad_public_read_for_execution(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    params_len: usize,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<LoweredReadProgram>, LixError> {
    ensure_broad_public_read_statement_is_fully_typed(statement)?;

    if broad_public_read_statement_contains_public_relations(statement) {
        return Ok(None);
    }

    let rendered = lower_broad_public_read_statement(
        statement,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
    )?;
    if rendered.relation_render_nodes.is_empty() {
        return Ok(None);
    }

    Ok(Some(LoweredReadProgram {
        statements: vec![compile_lowered_read_statement(
            dialect,
            params_len,
            rendered.shell_statement,
            rendered.relation_render_nodes,
        )?],
        pushdown_decision: PushdownDecision::default(),
        result_columns: LoweredResultColumns::Static(Vec::new()),
    }))
}

pub(crate) fn broad_public_relation_supports_terminal_render(
    binding: &SurfaceBinding,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<bool, LixError> {
    build_supported_public_read_surface_sql(
        &binding.descriptor.public_name,
        registry,
        false,
        dialect,
        active_version_id,
        known_live_layouts,
    )
    .map(|sql| sql.is_some())
}

pub(crate) fn bind_broad_public_read_statement_with_registry(
    statement: &Statement,
    registry: &SurfaceRegistry,
) -> Result<Option<BroadPublicReadStatement>, LixError> {
    #[cfg(test)]
    assert_broad_binding_allowed_for_test();

    match statement {
        Statement::Query(query) => Ok(Some(BroadPublicReadStatement::Query(
            bind_broad_public_read_query_scoped(query, registry, &BTreeSet::new())?,
        ))),
        Statement::Explain {
            statement: inner, ..
        } => {
            let Some(bound_inner) =
                bind_broad_public_read_statement_with_registry(inner, registry)?
            else {
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
    #[cfg(test)]
    assert_broad_binding_allowed_for_test();

    let mut scoped_ctes = visible_ctes.clone();
    let with = if let Some(with) = &query.with {
        let mut cte_scope = visible_ctes.clone();
        let mut cte_tables = Vec::with_capacity(with.cte_tables.len());
        for cte in &with.cte_tables {
            cte_tables.push(BroadPublicReadCte {
                name: cte.alias.name.value.clone(),
                query: bind_broad_public_read_query_scoped(&cte.query, registry, &cte_scope)?,
            });
            cte_scope.insert(cte.alias.name.value.to_ascii_lowercase());
        }
        scoped_ctes = cte_scope;
        Some(BroadPublicReadWith {
            provenance: BroadSqlProvenance::from_raw(with.clone()),
            cte_tables,
        })
    } else {
        None
    };

    Ok(BroadPublicReadQuery {
        provenance: BroadSqlProvenance::from_raw(query.clone()),
        with,
        body: bind_broad_public_read_set_expr(query.body.as_ref(), registry, &scoped_ctes)?,
        order_by: query
            .order_by
            .as_ref()
            .map(|order_by| bind_broad_public_read_order_by(order_by, registry, &scoped_ctes))
            .transpose()?,
        limit_clause: query
            .limit_clause
            .as_ref()
            .map(|limit_clause| {
                bind_broad_public_read_limit_clause(limit_clause, registry, &scoped_ctes)
            })
            .transpose()?,
    })
}

fn bind_broad_public_read_set_expr(
    expr: &SetExpr,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadSetExpr, LixError> {
    match expr {
        SetExpr::Select(select) => Ok(BroadPublicReadSetExpr::Select(BroadPublicReadSelect {
            provenance: BroadSqlProvenance::from_raw(select.as_ref().clone()),
            projection: select
                .projection
                .iter()
                .map(|item| bind_broad_public_read_projection_item(item, registry, visible_ctes))
                .collect::<Result<_, _>>()?,
            from: select
                .from
                .iter()
                .map(|table| bind_broad_public_read_table_with_joins(table, registry, visible_ctes))
                .collect::<Result<_, _>>()?,
            selection: select
                .selection
                .as_ref()
                .map(|expr| bind_broad_public_read_expr(expr, registry, visible_ctes))
                .transpose()?,
            group_by: bind_broad_public_read_group_by(&select.group_by, registry, visible_ctes)?,
            having: select
                .having
                .as_ref()
                .map(|expr| bind_broad_public_read_expr(expr, registry, visible_ctes))
                .transpose()?,
        })),
        SetExpr::Query(query) => Ok(BroadPublicReadSetExpr::Query(Box::new(
            bind_broad_public_read_query_scoped(query, registry, visible_ctes)?,
        ))),
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => Ok(BroadPublicReadSetExpr::SetOperation {
            provenance: BroadSqlProvenance::from_raw(expr.clone()),
            operator: bind_broad_public_read_set_operation_kind(*op),
            quantifier: bind_broad_public_read_set_quantifier(*set_quantifier),
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
                return Ok(BroadPublicReadSetExpr::Other {
                    provenance: BroadSqlProvenance::from_raw(expr.clone()),
                });
            };
            Ok(BroadPublicReadSetExpr::Table {
                provenance: BroadSqlProvenance::from_raw(expr.clone()),
                relation: classify_broad_public_read_relation(table_name, registry, visible_ctes),
            })
        }
        _ => Ok(BroadPublicReadSetExpr::Other {
            provenance: BroadSqlProvenance::from_raw(expr.clone()),
        }),
    }
}

fn bind_broad_public_read_table_with_joins(
    table: &TableWithJoins,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadTableWithJoins, LixError> {
    Ok(BroadPublicReadTableWithJoins {
        provenance: BroadSqlProvenance::from_raw(table.clone()),
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
        provenance: BroadSqlProvenance::from_raw(join.clone()),
        operator: broad_public_read_join_operator_label(&join.join_operator).to_string(),
        relation: bind_broad_public_read_table_factor(&join.relation, registry, visible_ctes)?,
        constraint_expressions: bind_broad_public_read_join_constraint_expressions(
            &join.join_operator,
            registry,
            visible_ctes,
        )?,
    })
}

fn bind_broad_public_read_table_factor(
    relation: &TableFactor,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadTableFactor, LixError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let Some(relation_name) = table_name_terminal(name) else {
                return Ok(BroadPublicReadTableFactor::Other {
                    provenance: BroadSqlProvenance::from_raw(relation.clone()),
                });
            };
            Ok(BroadPublicReadTableFactor::Table {
                provenance: BroadSqlProvenance::from_raw(relation.clone()),
                alias: alias.as_ref().map(broad_public_read_alias),
                relation: classify_broad_public_read_relation(
                    relation_name,
                    registry,
                    visible_ctes,
                ),
            })
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => Ok(BroadPublicReadTableFactor::Derived {
            provenance: BroadSqlProvenance::from_raw(relation.clone()),
            alias: alias.as_ref().map(broad_public_read_alias),
            subquery: Box::new(bind_broad_public_read_query_scoped(
                subquery,
                registry,
                visible_ctes,
            )?),
        }),
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
            ..
        } => Ok(BroadPublicReadTableFactor::NestedJoin {
            provenance: BroadSqlProvenance::from_raw(relation.clone()),
            alias: alias.as_ref().map(broad_public_read_alias),
            table_with_joins: Box::new(bind_broad_public_read_table_with_joins(
                table_with_joins,
                registry,
                visible_ctes,
            )?),
        }),
        _ => Ok(BroadPublicReadTableFactor::Other {
            provenance: BroadSqlProvenance::from_raw(relation.clone()),
        }),
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

fn bind_broad_public_read_projection_item(
    item: &SelectItem,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadProjectionItem, LixError> {
    let kind = match item {
        SelectItem::Wildcard(_) => BroadPublicReadProjectionItemKind::Wildcard,
        SelectItem::QualifiedWildcard(qualifier, _) => {
            BroadPublicReadProjectionItemKind::QualifiedWildcard {
                qualifier: vec![qualifier.to_string()],
            }
        }
        SelectItem::UnnamedExpr(expr) => {
            let bound_expr = bind_broad_public_read_expr(expr, registry, visible_ctes)?;
            BroadPublicReadProjectionItemKind::Expr {
                alias: None,
                sql: bound_expr.sql.clone(),
                nested_queries: bound_expr.nested_queries,
            }
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            let bound_expr = bind_broad_public_read_expr(expr, registry, visible_ctes)?;
            BroadPublicReadProjectionItemKind::Expr {
                alias: Some(alias.value.clone()),
                sql: bound_expr.sql.clone(),
                nested_queries: bound_expr.nested_queries,
            }
        }
    };

    Ok(BroadPublicReadProjectionItem {
        provenance: BroadSqlProvenance::from_raw(item.clone()),
        kind,
    })
}

fn bind_broad_public_read_group_by(
    group_by: &GroupByExpr,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadGroupBy, LixError> {
    let kind = match group_by {
        GroupByExpr::All(_) => BroadPublicReadGroupByKind::All,
        GroupByExpr::Expressions(expressions, _) => BroadPublicReadGroupByKind::Expressions(
            expressions
                .iter()
                .map(|expr| bind_broad_public_read_expr(expr, registry, visible_ctes))
                .collect::<Result<_, _>>()?,
        ),
    };
    Ok(BroadPublicReadGroupBy {
        provenance: BroadSqlProvenance::from_raw(group_by.clone()),
        kind,
    })
}

fn bind_broad_public_read_order_by(
    order_by: &OrderBy,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadOrderBy, LixError> {
    let kind = match &order_by.kind {
        sqlparser::ast::OrderByKind::All(_) => BroadPublicReadOrderByKind::All,
        sqlparser::ast::OrderByKind::Expressions(expressions) => {
            BroadPublicReadOrderByKind::Expressions(
                expressions
                    .iter()
                    .map(|expr| {
                        Ok(BroadPublicReadOrderByExpr {
                            provenance: BroadSqlProvenance::from_raw(expr.clone()),
                            expr: bind_broad_public_read_expr(&expr.expr, registry, visible_ctes)?,
                            asc: expr.options.asc,
                            nulls_first: expr.options.nulls_first,
                        })
                    })
                    .collect::<Result<_, LixError>>()?,
            )
        }
    };

    Ok(BroadPublicReadOrderBy {
        provenance: BroadSqlProvenance::from_raw(order_by.clone()),
        kind,
    })
}

fn bind_broad_public_read_limit_clause(
    limit_clause: &LimitClause,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadLimitClause, LixError> {
    let kind = match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => BroadPublicReadLimitClauseKind::LimitOffset {
            limit: limit
                .as_ref()
                .map(|expr| bind_broad_public_read_expr(expr, registry, visible_ctes))
                .transpose()?,
            offset: offset
                .as_ref()
                .map(|offset| bind_broad_public_read_expr(&offset.value, registry, visible_ctes))
                .transpose()?,
            limit_by: limit_by
                .iter()
                .map(|expr| bind_broad_public_read_expr(expr, registry, visible_ctes))
                .collect::<Result<_, _>>()?,
        },
        LimitClause::OffsetCommaLimit { offset, limit } => {
            BroadPublicReadLimitClauseKind::OffsetCommaLimit {
                offset: bind_broad_public_read_expr(offset, registry, visible_ctes)?,
                limit: bind_broad_public_read_expr(limit, registry, visible_ctes)?,
            }
        }
    };

    Ok(BroadPublicReadLimitClause {
        provenance: BroadSqlProvenance::from_raw(limit_clause.clone()),
        kind,
    })
}

fn bind_broad_public_read_expr(
    expr: &Expr,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadSqlExpr, LixError> {
    Ok(BroadSqlExpr {
        provenance: BroadSqlProvenance::from_raw(expr.clone()),
        sql: expr.to_string(),
        nested_queries: bind_broad_public_read_nested_queries(expr, registry, visible_ctes)?,
    })
}

fn bind_broad_public_read_nested_queries(
    expr: &Expr,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<Vec<BroadNestedQueryExpr>, LixError> {
    let mut queries = Vec::new();
    collect_broad_public_read_nested_queries(expr, registry, visible_ctes, &mut queries)?;
    Ok(queries)
}

fn collect_broad_public_read_nested_queries(
    expr: &Expr,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
    out: &mut Vec<BroadNestedQueryExpr>,
) -> Result<(), LixError> {
    match expr {
        Expr::Subquery(query) => {
            out.push(BroadNestedQueryExpr::ScalarSubquery(Box::new(
                bind_broad_public_read_query_scoped(query, registry, visible_ctes)?,
            )));
        }
        Expr::Exists { negated, subquery } => {
            out.push(BroadNestedQueryExpr::Exists {
                negated: *negated,
                subquery: Box::new(bind_broad_public_read_query_scoped(
                    subquery,
                    registry,
                    visible_ctes,
                )?),
            });
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let bound_expr = bind_broad_public_read_expr(expr, registry, visible_ctes)?;
            out.push(BroadNestedQueryExpr::InSubquery {
                negated: *negated,
                expr_sql: bound_expr.sql.clone(),
                expr: Box::new(bound_expr),
                subquery: Box::new(bind_broad_public_read_query_scoped(
                    subquery,
                    registry,
                    visible_ctes,
                )?),
            });
        }
        Expr::BinaryOp { left, right, .. }
        | Expr::AnyOp { left, right, .. }
        | Expr::AllOp { left, right, .. } => {
            collect_broad_public_read_nested_queries(left, registry, visible_ctes, out)?;
            collect_broad_public_read_nested_queries(right, registry, visible_ctes, out)?;
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => {
            collect_broad_public_read_nested_queries(expr, registry, visible_ctes, out)?;
        }
        Expr::InList { expr, list, .. } => {
            collect_broad_public_read_nested_queries(expr, registry, visible_ctes, out)?;
            for candidate in list {
                collect_broad_public_read_nested_queries(candidate, registry, visible_ctes, out)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_broad_public_read_nested_queries(expr, registry, visible_ctes, out)?;
            collect_broad_public_read_nested_queries(low, registry, visible_ctes, out)?;
            collect_broad_public_read_nested_queries(high, registry, visible_ctes, out)?;
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            collect_broad_public_read_nested_queries(expr, registry, visible_ctes, out)?;
            collect_broad_public_read_nested_queries(pattern, registry, visible_ctes, out)?;
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            collect_broad_public_read_nested_queries(expr, registry, visible_ctes, out)?;
            collect_broad_public_read_nested_queries(array_expr, registry, visible_ctes, out)?;
        }
        Expr::Function(function) => match &function.args {
            FunctionArguments::List(list) => {
                for arg in &list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                            collect_broad_public_read_nested_queries(
                                expr,
                                registry,
                                visible_ctes,
                                out,
                            )?;
                        }
                        FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                            if let FunctionArgExpr::Expr(expr) = arg {
                                collect_broad_public_read_nested_queries(
                                    expr,
                                    registry,
                                    visible_ctes,
                                    out,
                                )?;
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        },
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_broad_public_read_nested_queries(
                    operand.as_ref(),
                    registry,
                    visible_ctes,
                    out,
                )?;
            }
            for condition in conditions {
                collect_broad_public_read_nested_queries(
                    &condition.condition,
                    registry,
                    visible_ctes,
                    out,
                )?;
                collect_broad_public_read_nested_queries(
                    &condition.result,
                    registry,
                    visible_ctes,
                    out,
                )?;
            }
            if let Some(else_result) = else_result {
                collect_broad_public_read_nested_queries(
                    else_result.as_ref(),
                    registry,
                    visible_ctes,
                    out,
                )?;
            }
        }
        Expr::Tuple(items) => {
            for item in items {
                collect_broad_public_read_nested_queries(item, registry, visible_ctes, out)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn bind_broad_public_read_join_constraint_expressions(
    join_operator: &JoinOperator,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<Vec<BroadSqlExpr>, LixError> {
    let mut expressions = Vec::new();
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
        expressions.push(bind_broad_public_read_expr(expr, registry, visible_ctes)?);
    }
    if let Some(JoinConstraint::On(expr)) = constraint {
        expressions.push(bind_broad_public_read_expr(expr, registry, visible_ctes)?);
    }
    Ok(expressions)
}

fn bind_broad_public_read_set_operation_kind(op: SetOperator) -> BroadPublicReadSetOperationKind {
    match op {
        SetOperator::Union => BroadPublicReadSetOperationKind::Union,
        SetOperator::Except => BroadPublicReadSetOperationKind::Except,
        SetOperator::Intersect => BroadPublicReadSetOperationKind::Intersect,
        SetOperator::Minus => BroadPublicReadSetOperationKind::Minus,
    }
}

fn bind_broad_public_read_set_quantifier(
    quantifier: SetQuantifier,
) -> Option<BroadPublicReadSetQuantifier> {
    match quantifier {
        SetQuantifier::All => Some(BroadPublicReadSetQuantifier::All),
        SetQuantifier::Distinct => Some(BroadPublicReadSetQuantifier::Distinct),
        SetQuantifier::ByName => Some(BroadPublicReadSetQuantifier::ByName),
        SetQuantifier::AllByName => Some(BroadPublicReadSetQuantifier::AllByName),
        SetQuantifier::DistinctByName => Some(BroadPublicReadSetQuantifier::DistinctByName),
        SetQuantifier::None => None,
    }
}

fn broad_public_read_alias(alias: &TableAlias) -> BroadPublicReadAlias {
    BroadPublicReadAlias {
        name: alias.name.value.clone(),
        columns: alias
            .columns
            .iter()
            .map(|column| column.name.value.clone())
            .collect(),
    }
}

fn broad_public_read_join_operator_label(operator: &JoinOperator) -> &'static str {
    match operator {
        JoinOperator::Join(_) => "join",
        JoinOperator::Inner(_) => "inner",
        JoinOperator::Left(_) => "left",
        JoinOperator::LeftOuter(_) => "left_outer",
        JoinOperator::Right(_) => "right",
        JoinOperator::RightOuter(_) => "right_outer",
        JoinOperator::FullOuter(_) => "full_outer",
        JoinOperator::CrossJoin(_) => "cross_join",
        JoinOperator::Semi(_) => "semi",
        JoinOperator::LeftSemi(_) => "left_semi",
        JoinOperator::RightSemi(_) => "right_semi",
        JoinOperator::Anti(_) => "anti",
        JoinOperator::LeftAnti(_) => "left_anti",
        JoinOperator::RightAnti(_) => "right_anti",
        JoinOperator::CrossApply => "cross_apply",
        JoinOperator::OuterApply => "outer_apply",
        JoinOperator::AsOf { .. } => "as_of",
        JoinOperator::StraightJoin(_) => "straight_join",
    }
}

fn lower_broad_public_read_statement(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<RenderedBroadPublicReadStatement, LixError> {
    let mut substitutions = RenderRelationSubstitutionCollector::default();
    let shell_statement = lower_broad_public_read_statement_into_shell(
        statement,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        &mut substitutions,
    )?;
    Ok(RenderedBroadPublicReadStatement {
        shell_statement,
        relation_render_nodes: substitutions.into_substitutions(),
    })
}

fn broad_public_read_statement_contains_public_relations(
    statement: &BroadPublicReadStatement,
) -> bool {
    broad_public_read_statement_contains_relation_kind(statement, |relation| {
        matches!(relation, BroadPublicReadRelation::Public(_))
    })
}

fn ensure_broad_public_read_statement_is_fully_typed(
    statement: &BroadPublicReadStatement,
) -> Result<(), LixError> {
    match statement {
        BroadPublicReadStatement::Query(query) => {
            ensure_broad_public_read_query_is_fully_typed(query, "query")
        }
        BroadPublicReadStatement::Explain { statement, .. } => {
            ensure_broad_public_read_statement_is_fully_typed(statement)
        }
    }
}

fn ensure_broad_public_read_query_is_fully_typed(
    query: &BroadPublicReadQuery,
    path: &str,
) -> Result<(), LixError> {
    if let Some(with) = &query.with {
        for (index, cte) in with.cte_tables.iter().enumerate() {
            ensure_broad_public_read_query_is_fully_typed(
                &cte.query,
                &format!("{path}.with.cte[{index}]"),
            )?;
        }
    }
    ensure_broad_public_read_set_expr_is_fully_typed(&query.body, &format!("{path}.body"))?;
    if let Some(order_by) = &query.order_by {
        ensure_broad_public_read_order_by_is_fully_typed(order_by, &format!("{path}.order_by"))?;
    }
    if let Some(limit_clause) = &query.limit_clause {
        ensure_broad_public_read_limit_clause_is_fully_typed(
            limit_clause,
            &format!("{path}.limit_clause"),
        )?;
    }
    Ok(())
}

fn ensure_broad_public_read_set_expr_is_fully_typed(
    expr: &BroadPublicReadSetExpr,
    path: &str,
) -> Result<(), LixError> {
    match expr {
        BroadPublicReadSetExpr::Select(select) => {
            ensure_broad_public_read_select_is_fully_typed(select, path)
        }
        BroadPublicReadSetExpr::Query(query) => {
            ensure_broad_public_read_query_is_fully_typed(query, path)
        }
        BroadPublicReadSetExpr::SetOperation { left, right, .. } => {
            ensure_broad_public_read_set_expr_is_fully_typed(left, &format!("{path}.left"))?;
            ensure_broad_public_read_set_expr_is_fully_typed(right, &format!("{path}.right"))
        }
        BroadPublicReadSetExpr::Table { .. } => Ok(()),
        BroadPublicReadSetExpr::Other { .. } => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "broad public-read physical lowering requires fully typed routed IR; legacy set-expression fallback remains at {path}"
            ),
        )),
    }
}

fn ensure_broad_public_read_select_is_fully_typed(
    select: &BroadPublicReadSelect,
    path: &str,
) -> Result<(), LixError> {
    for (index, projection) in select.projection.iter().enumerate() {
        ensure_broad_public_read_projection_item_is_fully_typed(
            projection,
            &format!("{path}.projection[{index}]"),
        )?;
    }
    for (index, table) in select.from.iter().enumerate() {
        ensure_broad_public_read_table_with_joins_is_fully_typed(
            table,
            &format!("{path}.from[{index}]"),
        )?;
    }
    if let Some(selection) = &select.selection {
        ensure_broad_sql_expr_is_fully_typed(selection, &format!("{path}.selection"))?;
    }
    ensure_broad_public_read_group_by_is_fully_typed(
        &select.group_by,
        &format!("{path}.group_by"),
    )?;
    if let Some(having) = &select.having {
        ensure_broad_sql_expr_is_fully_typed(having, &format!("{path}.having"))?;
    }
    Ok(())
}

fn ensure_broad_public_read_table_with_joins_is_fully_typed(
    table: &BroadPublicReadTableWithJoins,
    path: &str,
) -> Result<(), LixError> {
    ensure_broad_public_read_table_factor_is_fully_typed(
        &table.relation,
        &format!("{path}.relation"),
    )?;
    for (index, join) in table.joins.iter().enumerate() {
        ensure_broad_public_read_table_factor_is_fully_typed(
            &join.relation,
            &format!("{path}.joins[{index}].relation"),
        )?;
        for (expr_index, expr) in join.constraint_expressions.iter().enumerate() {
            ensure_broad_sql_expr_is_fully_typed(
                expr,
                &format!("{path}.joins[{index}].constraint_expressions[{expr_index}]"),
            )?;
        }
    }
    Ok(())
}

fn ensure_broad_public_read_table_factor_is_fully_typed(
    factor: &BroadPublicReadTableFactor,
    path: &str,
) -> Result<(), LixError> {
    match factor {
        BroadPublicReadTableFactor::Table { .. } => Ok(()),
        BroadPublicReadTableFactor::Derived { subquery, .. } => {
            ensure_broad_public_read_query_is_fully_typed(subquery, &format!("{path}.subquery"))
        }
        BroadPublicReadTableFactor::NestedJoin {
            table_with_joins, ..
        } => ensure_broad_public_read_table_with_joins_is_fully_typed(
            table_with_joins,
            &format!("{path}.nested_join"),
        ),
        BroadPublicReadTableFactor::Other { .. } => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "broad public-read physical lowering requires fully typed routed IR; legacy table-factor fallback remains at {path}"
            ),
        )),
    }
}

fn ensure_broad_public_read_projection_item_is_fully_typed(
    item: &BroadPublicReadProjectionItem,
    path: &str,
) -> Result<(), LixError> {
    match &item.kind {
        BroadPublicReadProjectionItemKind::Expr { nested_queries, .. } => {
            for (index, nested_query) in nested_queries.iter().enumerate() {
                ensure_broad_nested_query_expr_is_fully_typed(
                    nested_query,
                    &format!("{path}.nested_queries[{index}]"),
                )?;
            }
            Ok(())
        }
        BroadPublicReadProjectionItemKind::Wildcard
        | BroadPublicReadProjectionItemKind::QualifiedWildcard { .. } => Ok(()),
    }
}

fn ensure_broad_public_read_group_by_is_fully_typed(
    group_by: &BroadPublicReadGroupBy,
    path: &str,
) -> Result<(), LixError> {
    match &group_by.kind {
        BroadPublicReadGroupByKind::All => Ok(()),
        BroadPublicReadGroupByKind::Expressions(expressions) => {
            for (index, expr) in expressions.iter().enumerate() {
                ensure_broad_sql_expr_is_fully_typed(
                    expr,
                    &format!("{path}.expressions[{index}]"),
                )?;
            }
            Ok(())
        }
    }
}

fn ensure_broad_public_read_order_by_is_fully_typed(
    order_by: &BroadPublicReadOrderBy,
    path: &str,
) -> Result<(), LixError> {
    match &order_by.kind {
        BroadPublicReadOrderByKind::All => Ok(()),
        BroadPublicReadOrderByKind::Expressions(expressions) => {
            for (index, expr) in expressions.iter().enumerate() {
                ensure_broad_sql_expr_is_fully_typed(
                    &expr.expr,
                    &format!("{path}.expressions[{index}].expr"),
                )?;
            }
            Ok(())
        }
    }
}

fn ensure_broad_public_read_limit_clause_is_fully_typed(
    limit_clause: &BroadPublicReadLimitClause,
    path: &str,
) -> Result<(), LixError> {
    match &limit_clause.kind {
        BroadPublicReadLimitClauseKind::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if let Some(limit) = limit {
                ensure_broad_sql_expr_is_fully_typed(limit, &format!("{path}.limit"))?;
            }
            if let Some(offset) = offset {
                ensure_broad_sql_expr_is_fully_typed(offset, &format!("{path}.offset"))?;
            }
            for (index, expr) in limit_by.iter().enumerate() {
                ensure_broad_sql_expr_is_fully_typed(expr, &format!("{path}.limit_by[{index}]"))?;
            }
            Ok(())
        }
        BroadPublicReadLimitClauseKind::OffsetCommaLimit { offset, limit } => {
            ensure_broad_sql_expr_is_fully_typed(offset, &format!("{path}.offset"))?;
            ensure_broad_sql_expr_is_fully_typed(limit, &format!("{path}.limit"))
        }
    }
}

fn ensure_broad_sql_expr_is_fully_typed(expr: &BroadSqlExpr, path: &str) -> Result<(), LixError> {
    for (index, nested_query) in expr.nested_queries.iter().enumerate() {
        ensure_broad_nested_query_expr_is_fully_typed(
            nested_query,
            &format!("{path}.nested_queries[{index}]"),
        )?;
    }
    Ok(())
}

fn ensure_broad_nested_query_expr_is_fully_typed(
    expr: &BroadNestedQueryExpr,
    path: &str,
) -> Result<(), LixError> {
    match expr {
        BroadNestedQueryExpr::ScalarSubquery(query) => {
            ensure_broad_public_read_query_is_fully_typed(query, path)
        }
        BroadNestedQueryExpr::Exists { subquery, .. } => {
            ensure_broad_public_read_query_is_fully_typed(subquery, &format!("{path}.subquery"))
        }
        BroadNestedQueryExpr::InSubquery { expr, subquery, .. } => {
            ensure_broad_sql_expr_is_fully_typed(expr, &format!("{path}.expr"))?;
            ensure_broad_public_read_query_is_fully_typed(subquery, &format!("{path}.subquery"))
        }
    }
}

fn broad_public_read_statement_contains_relation_kind(
    statement: &BroadPublicReadStatement,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match statement {
        BroadPublicReadStatement::Query(query) => {
            broad_public_read_query_contains_relation_kind(query, predicate)
        }
        BroadPublicReadStatement::Explain { statement, .. } => {
            broad_public_read_statement_contains_relation_kind(statement, predicate)
        }
    }
}

fn broad_public_read_query_contains_relation_kind(
    query: &BroadPublicReadQuery,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    query.with.as_ref().is_some_and(|with| {
        with.cte_tables
            .iter()
            .any(|cte| broad_public_read_query_contains_relation_kind(&cte.query, predicate))
    }) || broad_public_read_set_expr_contains_relation_kind(&query.body, predicate)
        || query.order_by.as_ref().is_some_and(|order_by| {
            broad_public_read_order_by_contains_relation_kind(order_by, predicate)
        })
        || query.limit_clause.as_ref().is_some_and(|limit_clause| {
            broad_public_read_limit_clause_contains_relation_kind(limit_clause, predicate)
        })
}

fn broad_public_read_set_expr_contains_relation_kind(
    expr: &BroadPublicReadSetExpr,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match expr {
        BroadPublicReadSetExpr::Select(select) => {
            broad_public_read_select_contains_relation_kind(select, predicate)
        }
        BroadPublicReadSetExpr::Query(query) => {
            broad_public_read_query_contains_relation_kind(query, predicate)
        }
        BroadPublicReadSetExpr::SetOperation { left, right, .. } => {
            broad_public_read_set_expr_contains_relation_kind(left, predicate)
                || broad_public_read_set_expr_contains_relation_kind(right, predicate)
        }
        BroadPublicReadSetExpr::Table { relation, .. } => predicate(relation),
        BroadPublicReadSetExpr::Other { .. } => false,
    }
}

fn broad_public_read_select_contains_relation_kind(
    select: &BroadPublicReadSelect,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    select.projection.iter().any(|projection| {
        broad_public_read_projection_item_contains_relation_kind(projection, predicate)
    }) || select
        .from
        .iter()
        .any(|table| broad_public_read_table_with_joins_contains_relation_kind(table, predicate))
        || select
            .selection
            .as_ref()
            .is_some_and(|selection| broad_sql_expr_contains_relation_kind(selection, predicate))
        || broad_public_read_group_by_contains_relation_kind(&select.group_by, predicate)
        || select
            .having
            .as_ref()
            .is_some_and(|having| broad_sql_expr_contains_relation_kind(having, predicate))
}

fn broad_public_read_table_with_joins_contains_relation_kind(
    table: &BroadPublicReadTableWithJoins,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    broad_public_read_table_factor_contains_relation_kind(&table.relation, predicate)
        || table.joins.iter().any(|join| {
            broad_public_read_table_factor_contains_relation_kind(&join.relation, predicate)
                || broad_public_read_join_contains_relation_kind(join, predicate)
        })
}

fn broad_public_read_table_factor_contains_relation_kind(
    factor: &BroadPublicReadTableFactor,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match factor {
        BroadPublicReadTableFactor::Table { relation, .. } => predicate(relation),
        BroadPublicReadTableFactor::Derived { subquery, .. } => {
            broad_public_read_query_contains_relation_kind(subquery, predicate)
        }
        BroadPublicReadTableFactor::NestedJoin {
            table_with_joins, ..
        } => broad_public_read_table_with_joins_contains_relation_kind(table_with_joins, predicate),
        BroadPublicReadTableFactor::Other { .. } => false,
    }
}

fn broad_public_read_projection_item_contains_relation_kind(
    item: &BroadPublicReadProjectionItem,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match &item.kind {
        BroadPublicReadProjectionItemKind::Expr { nested_queries, .. } => nested_queries
            .iter()
            .any(|expr| broad_nested_query_expr_contains_relation_kind(expr, predicate)),
        BroadPublicReadProjectionItemKind::Wildcard
        | BroadPublicReadProjectionItemKind::QualifiedWildcard { .. } => false,
    }
}

fn broad_public_read_group_by_contains_relation_kind(
    group_by: &BroadPublicReadGroupBy,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match &group_by.kind {
        BroadPublicReadGroupByKind::All => false,
        BroadPublicReadGroupByKind::Expressions(expressions) => expressions
            .iter()
            .any(|expr| broad_sql_expr_contains_relation_kind(expr, predicate)),
    }
}

fn broad_public_read_order_by_contains_relation_kind(
    order_by: &BroadPublicReadOrderBy,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match &order_by.kind {
        BroadPublicReadOrderByKind::All => false,
        BroadPublicReadOrderByKind::Expressions(expressions) => expressions
            .iter()
            .any(|expr| broad_sql_expr_contains_relation_kind(&expr.expr, predicate)),
    }
}

fn broad_public_read_limit_clause_contains_relation_kind(
    limit_clause: &BroadPublicReadLimitClause,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match &limit_clause.kind {
        BroadPublicReadLimitClauseKind::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            limit
                .as_ref()
                .is_some_and(|expr| broad_sql_expr_contains_relation_kind(expr, predicate))
                || offset
                    .as_ref()
                    .is_some_and(|expr| broad_sql_expr_contains_relation_kind(expr, predicate))
                || limit_by
                    .iter()
                    .any(|expr| broad_sql_expr_contains_relation_kind(expr, predicate))
        }
        BroadPublicReadLimitClauseKind::OffsetCommaLimit { offset, limit } => {
            broad_sql_expr_contains_relation_kind(offset, predicate)
                || broad_sql_expr_contains_relation_kind(limit, predicate)
        }
    }
}

fn broad_public_read_join_contains_relation_kind(
    join: &BroadPublicReadJoin,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    join.constraint_expressions
        .iter()
        .any(|expr| broad_sql_expr_contains_relation_kind(expr, predicate))
}

fn broad_sql_expr_contains_relation_kind(
    expr: &BroadSqlExpr,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    expr.nested_queries
        .iter()
        .any(|expr| broad_nested_query_expr_contains_relation_kind(expr, predicate))
}

fn broad_nested_query_expr_contains_relation_kind(
    expr: &BroadNestedQueryExpr,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match expr {
        BroadNestedQueryExpr::ScalarSubquery(query) => {
            broad_public_read_query_contains_relation_kind(query, predicate)
        }
        BroadNestedQueryExpr::Exists { subquery, .. } => {
            broad_public_read_query_contains_relation_kind(subquery, predicate)
        }
        BroadNestedQueryExpr::InSubquery { expr, subquery, .. } => {
            broad_sql_expr_contains_relation_kind(expr, predicate)
                || broad_public_read_query_contains_relation_kind(subquery, predicate)
        }
    }
}

fn lower_broad_public_read_statement_into_shell(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<Statement, LixError> {
    match statement {
        BroadPublicReadStatement::Query(query) => {
            Ok(Statement::Query(Box::new(lower_broad_public_read_query(
                query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?)))
        }
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
                **lowered_statement = lower_broad_public_read_statement_into_shell(
                    bound_statement.as_ref(),
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            Ok(lowered)
        }
    }
}

fn lower_broad_public_read_query(
    query: &BroadPublicReadQuery,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<Query, LixError> {
    let mut lowered = query.provenance.cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "broad public-read query lowering requires query provenance",
        )
    })?;
    lowered.with = query
        .with
        .as_ref()
        .map(|with| {
            lower_broad_public_read_with(
                with,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        })
        .transpose()?;
    lowered.body = Box::new(lower_broad_public_read_set_expr(
        &query.body,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        substitutions,
    )?);
    lowered.order_by = query
        .order_by
        .as_ref()
        .map(|order_by| {
            lower_broad_public_read_order_by_clause(
                order_by,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        })
        .transpose()?;
    lowered.limit_clause = query
        .limit_clause
        .as_ref()
        .map(|limit_clause| {
            lower_broad_public_read_limit_clause_exprs(
                limit_clause,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        })
        .transpose()?;
    Ok(lowered)
}

fn lower_broad_public_read_with(
    with: &BroadPublicReadWith,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<With, LixError> {
    let mut lowered = with.provenance.cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "broad public-read WITH lowering requires WITH provenance",
        )
    })?;
    for (cte, bound_query) in lowered.cte_tables.iter_mut().zip(&with.cte_tables) {
        cte.query = Box::new(lower_broad_public_read_query(
            &bound_query.query,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?);
    }
    Ok(lowered)
}

fn lower_broad_public_read_set_expr(
    expr: &BroadPublicReadSetExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<SetExpr, LixError> {
    match expr {
        BroadPublicReadSetExpr::Select(select) => {
            Ok(SetExpr::Select(Box::new(lower_broad_public_read_select(
                select,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?)))
        }
        BroadPublicReadSetExpr::Query(query) => {
            Ok(SetExpr::Query(Box::new(lower_broad_public_read_query(
                query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?)))
        }
        BroadPublicReadSetExpr::SetOperation {
            provenance,
            left,
            right,
            ..
        } => {
            let mut lowered = provenance.cloned().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "broad public-read set operation lowering requires provenance",
                )
            })?;
            if let SetExpr::SetOperation {
                left: lowered_left,
                right: lowered_right,
                ..
            } = &mut lowered
            {
                *lowered_left = Box::new(lower_broad_public_read_set_expr(
                    left.as_ref(),
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?);
                *lowered_right = Box::new(lower_broad_public_read_set_expr(
                    right.as_ref(),
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?);
            }
            Ok(lowered)
        }
        BroadPublicReadSetExpr::Table {
            provenance,
            relation,
        } => {
            let original = provenance.cloned().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "broad public-read table set-expr lowering requires provenance",
                )
            })?;
            lower_broad_public_read_table_relation(
                relation,
                &original,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        BroadPublicReadSetExpr::Other { provenance } => {
            let _ = provenance;
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "broad public-read physical lowering does not support legacy set-expression fallbacks",
            ))
        }
    }
}

fn lower_broad_public_read_select(
    select: &BroadPublicReadSelect,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<Select, LixError> {
    let mut lowered = select.provenance.cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "broad public-read select lowering requires select provenance",
        )
    })?;
    lowered.from = select
        .from
        .iter()
        .map(|table| {
            lower_broad_public_read_table_with_joins(
                table,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        })
        .collect::<Result<_, _>>()?;
    for (projection, typed_projection) in lowered.projection.iter_mut().zip(&select.projection) {
        lower_broad_public_read_projection_item_nested_queries(
            projection,
            typed_projection,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    if let Some(selection) = &select.selection {
        lowered.selection = Some(lower_broad_sql_expr(
            selection,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?);
    }
    lowered.group_by = lower_broad_public_read_group_by_clause(
        &select.group_by,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        substitutions,
    )?;
    if let Some(having) = &select.having {
        lowered.having = Some(lower_broad_sql_expr(
            having,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?);
    }
    Ok(lowered)
}

fn lower_broad_public_read_table_with_joins(
    table: &BroadPublicReadTableWithJoins,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<TableWithJoins, LixError> {
    let mut lowered = table.provenance.cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "broad public-read table-with-joins lowering requires provenance",
        )
    })?;
    lowered.relation = lower_broad_public_read_table_factor(
        &table.relation,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        substitutions,
    )?;
    lowered.joins = table
        .joins
        .iter()
        .map(|join| {
            lower_broad_public_read_join(
                join,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        })
        .collect::<Result<_, _>>()?;
    Ok(lowered)
}

fn lower_broad_public_read_join(
    join: &BroadPublicReadJoin,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<sqlparser::ast::Join, LixError> {
    let mut lowered = join.provenance.cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "broad public-read join lowering requires provenance",
        )
    })?;
    lowered.relation = lower_broad_public_read_table_factor(
        &join.relation,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        substitutions,
    )?;
    lower_broad_public_read_join_operator_exprs(
        &mut lowered.join_operator,
        &join.constraint_expressions,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        substitutions,
    )?;
    Ok(lowered)
}

fn lower_broad_public_read_projection_item_nested_queries(
    item: &mut SelectItem,
    typed_item: &BroadPublicReadProjectionItem,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    let nested_queries = match &typed_item.kind {
        BroadPublicReadProjectionItemKind::Expr { nested_queries, .. } => nested_queries.as_slice(),
        BroadPublicReadProjectionItemKind::Wildcard
        | BroadPublicReadProjectionItemKind::QualifiedWildcard { .. } => return Ok(()),
    };

    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            apply_lowered_nested_queries_to_expr(
                expr,
                &mut nested_queries.iter(),
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        _ => Ok(()),
    }
}

fn lower_broad_public_read_group_by_clause(
    group_by: &BroadPublicReadGroupBy,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<GroupByExpr, LixError> {
    let mut lowered = group_by.provenance.cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "broad public-read GROUP BY lowering requires provenance",
        )
    })?;
    if let (
        GroupByExpr::Expressions(lowered_expressions, _),
        BroadPublicReadGroupByKind::Expressions(expressions),
    ) = (&mut lowered, &group_by.kind)
    {
        for (lowered_expr, typed_expr) in lowered_expressions.iter_mut().zip(expressions) {
            *lowered_expr = lower_broad_sql_expr(
                typed_expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
        }
    }
    Ok(lowered)
}

fn lower_broad_public_read_order_by_clause(
    order_by: &BroadPublicReadOrderBy,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<OrderBy, LixError> {
    let mut lowered = order_by.provenance.cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "broad public-read ORDER BY lowering requires provenance",
        )
    })?;
    if let (
        sqlparser::ast::OrderByKind::Expressions(lowered_expressions),
        BroadPublicReadOrderByKind::Expressions(expressions),
    ) = (&mut lowered.kind, &order_by.kind)
    {
        for (lowered_expr, typed_expr) in lowered_expressions.iter_mut().zip(expressions) {
            lowered_expr.expr = lower_broad_sql_expr(
                &typed_expr.expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
        }
    }
    Ok(lowered)
}

fn lower_broad_public_read_limit_clause_exprs(
    limit_clause: &BroadPublicReadLimitClause,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<LimitClause, LixError> {
    let mut lowered = limit_clause.provenance.cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "broad public-read LIMIT lowering requires provenance",
        )
    })?;
    match (&mut lowered, &limit_clause.kind) {
        (
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            },
            BroadPublicReadLimitClauseKind::LimitOffset {
                limit: typed_limit,
                offset: typed_offset,
                limit_by: typed_limit_by,
            },
        ) => {
            if let (Some(lowered_limit), Some(typed_limit)) = (limit.as_mut(), typed_limit.as_ref())
            {
                *lowered_limit = lower_broad_sql_expr(
                    typed_limit,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            if let (Some(lowered_offset), Some(typed_offset)) =
                (offset.as_mut(), typed_offset.as_ref())
            {
                lowered_offset.value = lower_broad_sql_expr(
                    typed_offset,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            for (lowered_expr, typed_expr) in limit_by.iter_mut().zip(typed_limit_by) {
                *lowered_expr = lower_broad_sql_expr(
                    typed_expr,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
        }
        (
            LimitClause::OffsetCommaLimit { offset, limit },
            BroadPublicReadLimitClauseKind::OffsetCommaLimit {
                offset: typed_offset,
                limit: typed_limit,
            },
        ) => {
            *offset = lower_broad_sql_expr(
                typed_offset,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            *limit = lower_broad_sql_expr(
                typed_limit,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
        }
        _ => {}
    }
    Ok(lowered)
}

fn lower_broad_sql_expr(
    expr: &BroadSqlExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<Expr, LixError> {
    let mut lowered = expr.provenance.cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "broad public-read expression lowering requires provenance",
        )
    })?;
    let mut nested_queries = expr.nested_queries.iter();
    apply_lowered_nested_queries_to_expr(
        &mut lowered,
        &mut nested_queries,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        substitutions,
    )?;
    if nested_queries.next().is_some() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "broad public-read expression lowering left nested queries unapplied",
        ));
    }
    Ok(lowered)
}

fn apply_lowered_nested_queries_to_expr<'a>(
    expr: &mut Expr,
    nested_queries: &mut std::slice::Iter<'a, BroadNestedQueryExpr>,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    match expr {
        Expr::BinaryOp { left, right, .. }
        | Expr::AnyOp { left, right, .. }
        | Expr::AllOp { left, right, .. } => {
            apply_lowered_nested_queries_to_expr(
                left,
                nested_queries,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            apply_lowered_nested_queries_to_expr(
                right,
                nested_queries,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => apply_lowered_nested_queries_to_expr(
            expr,
            nested_queries,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        ),
        Expr::InList { expr, list, .. } => {
            apply_lowered_nested_queries_to_expr(
                expr,
                nested_queries,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            for item in list {
                apply_lowered_nested_queries_to_expr(
                    item,
                    nested_queries,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            apply_lowered_nested_queries_to_expr(
                expr,
                nested_queries,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            apply_lowered_nested_queries_to_expr(
                low,
                nested_queries,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            apply_lowered_nested_queries_to_expr(
                high,
                nested_queries,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            apply_lowered_nested_queries_to_expr(
                expr,
                nested_queries,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            apply_lowered_nested_queries_to_expr(
                pattern,
                nested_queries,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        Expr::Subquery(query) => {
            let Some(BroadNestedQueryExpr::ScalarSubquery(typed_query)) = nested_queries.next()
            else {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "broad public-read expression lowering expected a scalar subquery node",
                ));
            };
            *query = Box::new(lower_broad_public_read_query(
                typed_query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?);
            Ok(())
        }
        Expr::Exists { subquery, .. } => {
            let Some(BroadNestedQueryExpr::Exists {
                subquery: typed_query,
                ..
            }) = nested_queries.next()
            else {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "broad public-read expression lowering expected an EXISTS subquery node",
                ));
            };
            *subquery = Box::new(lower_broad_public_read_query(
                typed_query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?);
            Ok(())
        }
        Expr::InSubquery { expr, subquery, .. } => {
            let Some(BroadNestedQueryExpr::InSubquery {
                expr: typed_expr,
                subquery: typed_query,
                ..
            }) = nested_queries.next()
            else {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "broad public-read expression lowering expected an IN-subquery node",
                ));
            };
            *expr = Box::new(lower_broad_sql_expr(
                typed_expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?);
            *subquery = Box::new(lower_broad_public_read_query(
                typed_query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?);
            Ok(())
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            apply_lowered_nested_queries_to_expr(
                expr,
                nested_queries,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            apply_lowered_nested_queries_to_expr(
                array_expr,
                nested_queries,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        Expr::Function(function) => apply_lowered_nested_queries_to_function_args(
            &mut function.args,
            nested_queries,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        ),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                apply_lowered_nested_queries_to_expr(
                    operand,
                    nested_queries,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            for condition in conditions {
                apply_lowered_nested_queries_to_expr(
                    &mut condition.condition,
                    nested_queries,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
                apply_lowered_nested_queries_to_expr(
                    &mut condition.result,
                    nested_queries,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            if let Some(else_result) = else_result {
                apply_lowered_nested_queries_to_expr(
                    else_result,
                    nested_queries,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            Ok(())
        }
        Expr::Tuple(items) => {
            for item in items {
                apply_lowered_nested_queries_to_expr(
                    item,
                    nested_queries,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn apply_lowered_nested_queries_to_function_args<'a>(
    args: &mut FunctionArguments,
    nested_queries: &mut std::slice::Iter<'a, BroadNestedQueryExpr>,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    match args {
        FunctionArguments::List(list) => {
            for arg in &mut list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        apply_lowered_nested_queries_to_expr(
                            expr,
                            nested_queries,
                            registry,
                            dialect,
                            active_version_id,
                            known_live_layouts,
                            substitutions,
                        )?;
                    }
                    FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                        if let FunctionArgExpr::Expr(expr) = arg {
                            apply_lowered_nested_queries_to_expr(
                                expr,
                                nested_queries,
                                registry,
                                dialect,
                                active_version_id,
                                known_live_layouts,
                                substitutions,
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

fn lower_broad_public_read_join_operator_exprs(
    join_operator: &mut JoinOperator,
    typed_exprs: &[BroadSqlExpr],
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    let mut typed_exprs = typed_exprs.iter();
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
    if let Some(match_condition) = match_condition {
        let typed_match_condition = typed_exprs.next().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "broad public-read join lowering expected a typed match condition",
            )
        })?;
        *match_condition = lower_broad_sql_expr(
            typed_match_condition,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    if let Some(JoinConstraint::On(on_expr)) = constraint {
        let typed_on_expr = typed_exprs.next().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "broad public-read join lowering expected a typed ON condition",
            )
        })?;
        *on_expr = lower_broad_sql_expr(
            typed_on_expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    if typed_exprs.next().is_some() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "broad public-read join lowering left typed join expressions unapplied",
        ));
    }
    Ok(())
}

fn lower_broad_public_read_table_factor(
    relation: &BroadPublicReadTableFactor,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<TableFactor, LixError> {
    match relation {
        BroadPublicReadTableFactor::Table {
            provenance,
            relation,
            ..
        } => {
            let original = provenance.cloned().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "broad public-read table factor lowering requires provenance",
                )
            })?;
            lower_broad_public_read_relation(
                relation,
                &original,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        BroadPublicReadTableFactor::Derived {
            provenance,
            subquery,
            ..
        } => {
            let mut lowered = provenance.cloned().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "broad public-read derived table lowering requires provenance",
                )
            })?;
            if let TableFactor::Derived {
                subquery: lowered_subquery,
                ..
            } = &mut lowered
            {
                *lowered_subquery = Box::new(lower_broad_public_read_query(
                    subquery.as_ref(),
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?);
            }
            Ok(lowered)
        }
        BroadPublicReadTableFactor::NestedJoin {
            provenance,
            table_with_joins,
            ..
        } => {
            let mut lowered = provenance.cloned().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "broad public-read nested join lowering requires provenance",
                )
            })?;
            if let TableFactor::NestedJoin {
                table_with_joins: lowered_table_with_joins,
                ..
            } = &mut lowered
            {
                *lowered_table_with_joins = Box::new(lower_broad_public_read_table_with_joins(
                    table_with_joins.as_ref(),
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?);
            }
            Ok(lowered)
        }
        BroadPublicReadTableFactor::Other { .. } => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "broad public-read physical lowering does not support legacy table-factor fallbacks",
        )),
    }
}

fn lower_broad_public_read_relation(
    relation: &BroadPublicReadRelation,
    original: &TableFactor,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<TableFactor, LixError> {
    match relation {
        BroadPublicReadRelation::LoweredPublic(binding) => {
            let Some(source_sql) = build_supported_public_read_surface_sql(
                &binding.descriptor.public_name,
                registry,
                false,
                dialect,
                active_version_id,
                known_live_layouts,
            )?
            else {
                return Ok(original.clone());
            };
            let TableFactor::Table { alias, .. } = original else {
                return Ok(original.clone());
            };
            Ok(substitutions.replacement_table_factor(
                &binding.descriptor.public_name,
                alias.clone().or_else(|| {
                    Some(TableAlias {
                        explicit: true,
                        name: Ident::new(&binding.descriptor.public_name),
                        columns: Vec::new(),
                    })
                }),
                source_sql,
            ))
        }
        BroadPublicReadRelation::Public(_)
        | BroadPublicReadRelation::Internal(_)
        | BroadPublicReadRelation::External(_)
        | BroadPublicReadRelation::Cte(_) => Ok(original.clone()),
    }
}

fn lower_broad_public_read_table_relation(
    relation: &BroadPublicReadRelation,
    original: &SetExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<SetExpr, LixError> {
    match relation {
        BroadPublicReadRelation::LoweredPublic(binding) => {
            let Some(source_sql) = build_supported_public_read_surface_sql(
                &binding.descriptor.public_name,
                registry,
                true,
                dialect,
                active_version_id,
                known_live_layouts,
            )?
            else {
                return Ok(original.clone());
            };
            Ok(SetExpr::Query(Box::new(Query {
                with: None,
                body: Box::new(SetExpr::Select(Box::new(Select {
                    select_token: AttachedToken::empty(),
                    distinct: None,
                    top: None,
                    top_before_distinct: false,
                    projection: vec![SelectItem::Wildcard(Default::default())],
                    exclude: None,
                    into: None,
                    from: vec![TableWithJoins {
                        relation: substitutions.replacement_table_factor(
                            &binding.descriptor.public_name,
                            Some(TableAlias {
                                explicit: true,
                                name: Ident::new(&binding.descriptor.public_name),
                                columns: Vec::new(),
                            }),
                            source_sql,
                        ),
                        joins: Vec::new(),
                    }],
                    lateral_views: Vec::new(),
                    prewhere: None,
                    selection: None,
                    group_by: GroupByExpr::Expressions(Vec::new(), Vec::new()),
                    cluster_by: Vec::new(),
                    distribute_by: Vec::new(),
                    sort_by: Vec::new(),
                    having: None,
                    named_window: Vec::new(),
                    qualify: None,
                    window_before_qualify: false,
                    value_table_mode: None,
                    connect_by: None,
                    flavor: sqlparser::ast::SelectFlavor::Standard,
                }))),
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: Vec::new(),
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: Vec::new(),
            })))
        }
        BroadPublicReadRelation::Public(_)
        | BroadPublicReadRelation::Internal(_)
        | BroadPublicReadRelation::External(_)
        | BroadPublicReadRelation::Cte(_) => Ok(original.clone()),
    }
}

fn build_supported_public_read_surface_sql(
    surface_name: &str,
    registry: &SurfaceRegistry,
    _top_level: bool,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<String>, LixError> {
    let Some(surface_binding) = registry.bind_relation_name(surface_name) else {
        return Ok(None);
    };

    match surface_binding.descriptor.surface_family {
        SurfaceFamily::State => build_public_state_surface_sql(
            &surface_binding,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        ),
        SurfaceFamily::Entity => build_entity_surface_sql_for_broad_lowering(
            dialect,
            &surface_binding,
            active_version_id,
            known_live_layouts,
        ),
        SurfaceFamily::Filesystem => build_nested_filesystem_surface_sql(
            dialect,
            active_version_id,
            &surface_binding.descriptor.public_name,
        ),
        SurfaceFamily::Admin => build_public_admin_surface_sql(dialect, &surface_binding),
        SurfaceFamily::Change => {
            build_public_change_surface_sql(dialect, &surface_binding, active_version_id)
        }
    }
}

fn build_public_state_surface_sql(
    surface_binding: &SurfaceBinding,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<String>, LixError> {
    let Some(state_scan) = CanonicalStateScan::from_surface_binding(surface_binding.clone()) else {
        return Ok(None);
    };
    let schema_set: BTreeSet<String> = registry
        .registered_state_surface_schema_keys()
        .into_iter()
        .collect();
    let request = EffectiveStateRequest {
        schema_set,
        version_scope: state_scan.version_scope,
        include_global_overlay: true,
        include_untracked_overlay: true,
        include_tombstones: state_scan.include_tombstones,
        predicate_classes: Vec::new(),
        required_columns: surface_binding
            .descriptor
            .visible_columns
            .iter()
            .chain(surface_binding.descriptor.hidden_columns.iter())
            .cloned()
            .collect(),
    };
    if state_scan.version_scope == VersionScope::ActiveVersion && active_version_id.is_none() {
        return Ok(None);
    }
    build_state_source_sql(
        dialect,
        active_version_id,
        surface_binding,
        &request,
        &[],
        known_live_layouts,
    )
}

fn build_public_admin_surface_sql(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
) -> Result<Option<String>, LixError> {
    let Some(admin_scan) = CanonicalAdminScan::from_surface_binding(surface_binding.clone()) else {
        return Ok(None);
    };
    build_admin_source_sql(admin_scan.kind, dialect).map(Some)
}

fn build_public_change_surface_sql(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
    active_version_id: Option<&str>,
) -> Result<Option<String>, LixError> {
    if CanonicalWorkingChangesScan::from_surface_binding(surface_binding.clone()).is_some() {
        let Some(active_version_id) = active_version_id else {
            return Ok(None);
        };
        return Ok(Some(build_working_changes_source_sql(
            dialect,
            active_version_id,
        )));
    }
    if CanonicalChangeScan::from_surface_binding(surface_binding.clone()).is_some() {
        return Ok(Some(build_change_source_sql()));
    }
    Ok(None)
}

fn build_entity_surface_sql_for_broad_lowering(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<String>, LixError> {
    let Some(schema_key) = surface_binding.implicit_overrides.fixed_schema_key.clone() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "public-surface lowering requires fixed schema binding for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    };
    if builtin_schema_definition(&schema_key).is_none()
        && !known_live_layouts.contains_key(&schema_key)
    {
        return Ok(None);
    }
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
        required_columns: surface_binding
            .descriptor
            .visible_columns
            .iter()
            .chain(surface_binding.descriptor.hidden_columns.iter())
            .cloned()
            .collect(),
    };
    if state_scan.version_scope == VersionScope::ActiveVersion && active_version_id.is_none() {
        return Ok(None);
    }
    Ok(Some(
        build_entity_source_sql(
            dialect,
            active_version_id,
            surface_binding,
            &request,
            &[],
            known_live_layouts,
        )?
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "public-surface lowering could not lower entity surface '{}'",
                surface_binding.descriptor.public_name
            ),
        })?,
    ))
}
