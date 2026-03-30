use crate::sql::catalog::SurfaceRegistry;
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
use crate::LixError;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, JoinConstraint,
    JoinOperator, LimitClause, ObjectName, OrderBy, Query, SelectItem, SetExpr, SetOperator,
    SetQuantifier, Statement, TableAlias, TableFactor, TableWithJoins,
};
#[cfg(test)]
use std::cell::Cell;
use std::collections::BTreeSet;

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

fn table_name_terminal(name: &ObjectName) -> Option<&str> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.as_str())
}

#[cfg(test)]
mod tests {
    use super::bind_broad_public_read_statement_with_registry;
    use crate::sql::catalog::SurfaceRegistry;
    use crate::sql::logical_plan::public_ir::{
        BroadNestedQueryExpr, BroadPublicReadGroupByKind, BroadPublicReadLimitClauseKind,
        BroadPublicReadOrderByKind, BroadPublicReadProjectionItemKind, BroadPublicReadSetExpr,
        BroadPublicReadStatement,
    };
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse_one(sql: &str) -> Statement {
        Parser::parse_sql(&GenericDialect {}, sql)
            .expect("SQL should parse")
            .into_iter()
            .next()
            .expect("statement should exist")
    }

    #[test]
    fn binds_broad_public_read_queries_into_typed_ir_shapes() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let bound = bind_broad_public_read_statement_with_registry(
            &parse_one(
                "WITH latest AS ( \
                   SELECT entity_id \
                   FROM lix_state_by_version \
                   WHERE lixcol_version_id = 'main' \
                 ) \
                 SELECT \
                   s.schema_key, \
                   (SELECT COUNT(*) FROM lix_file f WHERE f.id = 'file-stable-child') AS file_count \
                 FROM lix_state s \
                 WHERE EXISTS (SELECT 1 FROM latest) \
                   AND s.entity_id IN (SELECT entity_id FROM latest) \
                 GROUP BY s.schema_key \
                 HAVING COUNT(*) > 0 \
                 ORDER BY s.schema_key \
                 LIMIT 5",
            ),
            &registry,
        )
        .expect("broad binding should succeed")
        .expect("query should bind as a broad public read");

        let BroadPublicReadStatement::Query(query) = bound else {
            panic!("expected broad query statement");
        };

        assert!(query.provenance.as_ref().is_some());
        assert!(query.order_by.is_some());
        assert!(query.limit_clause.is_some());

        let with = query.with.as_ref().expect("expected typed WITH clause");
        assert!(with.provenance.as_ref().is_some());
        assert_eq!(with.cte_tables.len(), 1);
        assert_eq!(with.cte_tables[0].name, "latest");
        assert!(matches!(
            with.cte_tables[0].query.body,
            BroadPublicReadSetExpr::Select(_)
        ));

        let BroadPublicReadSetExpr::Select(select) = &query.body else {
            panic!("expected top-level broad select");
        };

        assert_eq!(select.projection.len(), 2);
        match &select.projection[0].kind {
            BroadPublicReadProjectionItemKind::Expr {
                alias,
                nested_queries,
                ..
            } => {
                assert_eq!(alias, &None);
                assert!(nested_queries.is_empty());
            }
            other => panic!("unexpected first projection item: {other:?}"),
        }
        match &select.projection[1].kind {
            BroadPublicReadProjectionItemKind::Expr {
                alias,
                nested_queries,
                ..
            } => {
                assert_eq!(alias.as_deref(), Some("file_count"));
                assert!(matches!(
                    nested_queries.as_slice(),
                    [BroadNestedQueryExpr::ScalarSubquery(_)]
                ));
            }
            other => panic!("unexpected second projection item: {other:?}"),
        }

        let selection = select
            .selection
            .as_ref()
            .expect("expected typed selection expression");
        assert_eq!(selection.nested_queries.len(), 2);
        assert!(selection
            .nested_queries
            .iter()
            .any(|expr| matches!(expr, BroadNestedQueryExpr::Exists { .. })));
        assert!(selection
            .nested_queries
            .iter()
            .any(|expr| matches!(expr, BroadNestedQueryExpr::InSubquery { .. })));

        assert!(matches!(
            &select.group_by.kind,
            BroadPublicReadGroupByKind::Expressions(expressions) if expressions.len() == 1
        ));
        assert!(select.having.is_some());

        let order_by = query.order_by.as_ref().expect("expected typed ORDER BY");
        assert!(matches!(
            &order_by.kind,
            BroadPublicReadOrderByKind::Expressions(expressions) if expressions.len() == 1
        ));

        let limit_clause = query
            .limit_clause
            .as_ref()
            .expect("expected typed LIMIT clause");
        assert!(matches!(
            &limit_clause.kind,
            BroadPublicReadLimitClauseKind::LimitOffset {
                limit: Some(_),
                offset: None,
                ..
            }
        ));
    }

    #[test]
    fn physical_plan_modules_do_not_export_broad_binding_entrypoints() {
        let lowerer_src = include_str!("../physical_plan/lowerer.rs");
        let lowerer_broad_src = include_str!("../physical_plan/lowerer/broad.rs");

        assert!(
            !lowerer_src.contains("pub(crate) fn bind_broad_public_read_statement_with_registry"),
            "sql/physical_plan/lowerer.rs must not export a broad binding entrypoint"
        );
        assert!(
            !lowerer_src
                .contains("pub(crate) use broad::bind_broad_public_read_statement_with_registry"),
            "sql/physical_plan/lowerer.rs must not re-export a broad binding entrypoint"
        );
        assert!(
            !lowerer_broad_src
                .contains("pub(crate) fn bind_broad_public_read_statement_with_registry"),
            "sql/physical_plan/lowerer/broad.rs must not define a public broad binding entrypoint"
        );
    }
}
