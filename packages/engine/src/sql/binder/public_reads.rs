use crate::sql::catalog::SurfaceRegistry;
use crate::sql::logical_plan::public_ir::{
    BroadPublicReadAlias, BroadPublicReadCte, BroadPublicReadDistinct, BroadPublicReadGroupBy,
    BroadPublicReadGroupByKind, BroadPublicReadJoin, BroadPublicReadJoinConstraint,
    BroadPublicReadJoinKind, BroadPublicReadLimitClause, BroadPublicReadLimitClauseKind,
    BroadPublicReadOffset, BroadPublicReadOrderBy, BroadPublicReadOrderByExpr,
    BroadPublicReadOrderByKind, BroadPublicReadProjectionItem, BroadPublicReadProjectionItemKind,
    BroadPublicReadQuery, BroadPublicReadRelation, BroadPublicReadSelect, BroadPublicReadSetExpr,
    BroadPublicReadSetOperationKind, BroadPublicReadSetQuantifier, BroadPublicReadStatement,
    BroadPublicReadTableFactor, BroadPublicReadTableWithJoins, BroadPublicReadWith,
    BroadSqlCaseWhen, BroadSqlExpr, BroadSqlExprKind, BroadSqlFunction, BroadSqlFunctionArg,
    BroadSqlFunctionArgExpr, BroadSqlFunctionArgumentList, BroadSqlFunctionArguments,
    BroadSqlProvenance,
};
use crate::LixError;
use sqlparser::ast::{
    Distinct, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, JoinConstraint,
    JoinOperator, LimitClause, ObjectName, OrderBy, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, SetOperator, SetQuantifier, Statement, TableAlias,
    TableFactor, TableWithJoins, TypedString,
};
use std::cell::Cell;
use std::collections::BTreeSet;
use std::time::Duration;

thread_local! {
    static BROAD_BINDING_DELAY_US_FOR_TEST: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
thread_local! {
    static FORBID_BROAD_BINDING_FOR_TEST: Cell<bool> = const { Cell::new(false) };
}

#[doc(hidden)]
pub struct BroadBindingDelayForTestGuard {
    previous_delay_us: u64,
}

impl Drop for BroadBindingDelayForTestGuard {
    fn drop(&mut self) {
        BROAD_BINDING_DELAY_US_FOR_TEST.set(self.previous_delay_us);
    }
}

#[doc(hidden)]
pub fn delay_broad_binding_for_test(delay: Duration) -> BroadBindingDelayForTestGuard {
    let previous_delay_us =
        BROAD_BINDING_DELAY_US_FOR_TEST.replace(delay.as_micros().min(u128::from(u64::MAX)) as u64);
    BroadBindingDelayForTestGuard { previous_delay_us }
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

fn apply_broad_binding_delay_for_test() {
    let delay_us = BROAD_BINDING_DELAY_US_FOR_TEST.get();
    if delay_us > 0 {
        std::thread::sleep(Duration::from_micros(delay_us));
    }
}

pub(crate) fn bind_broad_public_read_statement_with_registry(
    statement: &Statement,
    registry: &SurfaceRegistry,
) -> Result<Option<BroadPublicReadStatement>, LixError> {
    match statement {
        Statement::Query(query) => {
            #[cfg(test)]
            assert_broad_binding_allowed_for_test();
            apply_broad_binding_delay_for_test();

            Ok(Some(BroadPublicReadStatement::Query(
                bind_broad_public_read_query_scoped(query, registry, &BTreeSet::new())?,
            )))
        }
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
                alias: broad_public_read_alias(&cte.alias),
                materialized: cte.materialized.clone(),
                from: cte.from.as_ref().map(|ident| ident.value.clone()),
                query: bind_broad_public_read_query_scoped(&cte.query, registry, &cte_scope)?,
            });
            cte_scope.insert(cte.alias.name.value.to_ascii_lowercase());
        }
        scoped_ctes = cte_scope;
        Some(BroadPublicReadWith {
            provenance: BroadSqlProvenance::from_raw(with.clone()),
            recursive: with.recursive,
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
        SetExpr::Select(select) => Ok(BroadPublicReadSetExpr::Select(
            bind_broad_public_read_select(select, registry, visible_ctes)?,
        )),
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

fn bind_broad_public_read_select(
    select: &Select,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadSelect, LixError> {
    Ok(BroadPublicReadSelect {
        provenance: BroadSqlProvenance::from_raw(select.clone()),
        distinct: select
            .distinct
            .as_ref()
            .map(|distinct| bind_broad_public_read_distinct(distinct, registry, visible_ctes))
            .transpose()?,
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
    })
}

fn bind_broad_public_read_distinct(
    distinct: &Distinct,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadDistinct, LixError> {
    match distinct {
        Distinct::Distinct => Ok(BroadPublicReadDistinct::Distinct),
        Distinct::On(expressions) => Ok(BroadPublicReadDistinct::On(
            expressions
                .iter()
                .map(|expr| bind_broad_public_read_expr(expr, registry, visible_ctes))
                .collect::<Result<_, _>>()?,
        )),
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
        global: join.global,
        kind: bind_broad_public_read_join_kind(&join.join_operator, registry, visible_ctes)?,
        relation: bind_broad_public_read_table_factor(&join.relation, registry, visible_ctes)?,
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
            lateral: matches!(relation, TableFactor::Derived { lateral: true, .. }),
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
        SelectItem::QualifiedWildcard(
            SelectItemQualifiedWildcardKind::ObjectName(qualifier),
            _,
        ) => BroadPublicReadProjectionItemKind::QualifiedWildcard {
            qualifier: qualifier.clone(),
        },
        SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::Expr(expr), _) => {
            BroadPublicReadProjectionItemKind::Expr {
                alias: None,
                expr: bind_broad_public_read_expr(expr, registry, visible_ctes)?,
            }
        }
        SelectItem::UnnamedExpr(expr) => BroadPublicReadProjectionItemKind::Expr {
            alias: None,
            expr: bind_broad_public_read_expr(expr, registry, visible_ctes)?,
        },
        SelectItem::ExprWithAlias { expr, alias } => BroadPublicReadProjectionItemKind::Expr {
            alias: Some(alias.value.clone()),
            expr: bind_broad_public_read_expr(expr, registry, visible_ctes)?,
        },
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
                .map(|offset| {
                    bind_broad_public_read_expr(&offset.value, registry, visible_ctes).map(
                        |value| BroadPublicReadOffset {
                            value,
                            rows: offset.rows,
                        },
                    )
                })
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
    let kind = match expr {
        Expr::Identifier(ident) => BroadSqlExprKind::Identifier(ident.clone()),
        Expr::CompoundIdentifier(parts) => BroadSqlExprKind::CompoundIdentifier(parts.clone()),
        Expr::Value(value) => BroadSqlExprKind::Value(value.clone()),
        Expr::TypedString(TypedString {
            data_type,
            value,
            uses_odbc_syntax,
        }) => BroadSqlExprKind::TypedString {
            data_type: data_type.clone(),
            value: value.clone(),
            uses_odbc_syntax: *uses_odbc_syntax,
        },
        Expr::BinaryOp { left, op, right } => BroadSqlExprKind::BinaryOp {
            left: Box::new(bind_broad_public_read_expr(left, registry, visible_ctes)?),
            op: op.clone(),
            right: Box::new(bind_broad_public_read_expr(right, registry, visible_ctes)?),
        },
        Expr::AnyOp {
            left,
            compare_op,
            right,
            is_some,
        } => BroadSqlExprKind::AnyOp {
            left: Box::new(bind_broad_public_read_expr(left, registry, visible_ctes)?),
            compare_op: compare_op.clone(),
            right: Box::new(bind_broad_public_read_expr(right, registry, visible_ctes)?),
            is_some: *is_some,
        },
        Expr::AllOp {
            left,
            compare_op,
            right,
        } => BroadSqlExprKind::AllOp {
            left: Box::new(bind_broad_public_read_expr(left, registry, visible_ctes)?),
            compare_op: compare_op.clone(),
            right: Box::new(bind_broad_public_read_expr(right, registry, visible_ctes)?),
        },
        Expr::UnaryOp { op, expr } => BroadSqlExprKind::UnaryOp {
            op: *op,
            expr: Box::new(bind_broad_public_read_expr(expr, registry, visible_ctes)?),
        },
        Expr::Nested(expr) => BroadSqlExprKind::Nested(Box::new(bind_broad_public_read_expr(
            expr,
            registry,
            visible_ctes,
        )?)),
        Expr::IsNull(expr) => BroadSqlExprKind::IsNull(Box::new(bind_broad_public_read_expr(
            expr,
            registry,
            visible_ctes,
        )?)),
        Expr::IsNotNull(expr) => BroadSqlExprKind::IsNotNull(Box::new(
            bind_broad_public_read_expr(expr, registry, visible_ctes)?,
        )),
        Expr::IsTrue(expr) => BroadSqlExprKind::IsTrue(Box::new(bind_broad_public_read_expr(
            expr,
            registry,
            visible_ctes,
        )?)),
        Expr::IsNotTrue(expr) => BroadSqlExprKind::IsNotTrue(Box::new(
            bind_broad_public_read_expr(expr, registry, visible_ctes)?,
        )),
        Expr::IsFalse(expr) => BroadSqlExprKind::IsFalse(Box::new(bind_broad_public_read_expr(
            expr,
            registry,
            visible_ctes,
        )?)),
        Expr::IsNotFalse(expr) => BroadSqlExprKind::IsNotFalse(Box::new(
            bind_broad_public_read_expr(expr, registry, visible_ctes)?,
        )),
        Expr::IsUnknown(expr) => BroadSqlExprKind::IsUnknown(Box::new(
            bind_broad_public_read_expr(expr, registry, visible_ctes)?,
        )),
        Expr::IsNotUnknown(expr) => BroadSqlExprKind::IsNotUnknown(Box::new(
            bind_broad_public_read_expr(expr, registry, visible_ctes)?,
        )),
        Expr::IsDistinctFrom(left, right) => BroadSqlExprKind::IsDistinctFrom {
            left: Box::new(bind_broad_public_read_expr(left, registry, visible_ctes)?),
            right: Box::new(bind_broad_public_read_expr(right, registry, visible_ctes)?),
        },
        Expr::IsNotDistinctFrom(left, right) => BroadSqlExprKind::IsNotDistinctFrom {
            left: Box::new(bind_broad_public_read_expr(left, registry, visible_ctes)?),
            right: Box::new(bind_broad_public_read_expr(right, registry, visible_ctes)?),
        },
        Expr::Cast {
            kind,
            expr,
            data_type,
            format,
        } => BroadSqlExprKind::Cast {
            kind: kind.clone(),
            expr: Box::new(bind_broad_public_read_expr(expr, registry, visible_ctes)?),
            data_type: data_type.clone(),
            format: format.clone(),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => BroadSqlExprKind::InList {
            expr: Box::new(bind_broad_public_read_expr(expr, registry, visible_ctes)?),
            list: list
                .iter()
                .map(|item| bind_broad_public_read_expr(item, registry, visible_ctes))
                .collect::<Result<_, _>>()?,
            negated: *negated,
        },
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => BroadSqlExprKind::InSubquery {
            expr: Box::new(bind_broad_public_read_expr(expr, registry, visible_ctes)?),
            subquery: Box::new(bind_broad_public_read_query_scoped(
                subquery,
                registry,
                visible_ctes,
            )?),
            negated: *negated,
        },
        Expr::InUnnest {
            expr,
            array_expr,
            negated,
        } => BroadSqlExprKind::InUnnest {
            expr: Box::new(bind_broad_public_read_expr(expr, registry, visible_ctes)?),
            array_expr: Box::new(bind_broad_public_read_expr(
                array_expr,
                registry,
                visible_ctes,
            )?),
            negated: *negated,
        },
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => BroadSqlExprKind::Between {
            expr: Box::new(bind_broad_public_read_expr(expr, registry, visible_ctes)?),
            negated: *negated,
            low: Box::new(bind_broad_public_read_expr(low, registry, visible_ctes)?),
            high: Box::new(bind_broad_public_read_expr(high, registry, visible_ctes)?),
        },
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => BroadSqlExprKind::Like {
            negated: *negated,
            any: *any,
            expr: Box::new(bind_broad_public_read_expr(expr, registry, visible_ctes)?),
            pattern: Box::new(bind_broad_public_read_expr(
                pattern,
                registry,
                visible_ctes,
            )?),
            escape_char: escape_char.clone(),
        },
        Expr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => BroadSqlExprKind::ILike {
            negated: *negated,
            any: *any,
            expr: Box::new(bind_broad_public_read_expr(expr, registry, visible_ctes)?),
            pattern: Box::new(bind_broad_public_read_expr(
                pattern,
                registry,
                visible_ctes,
            )?),
            escape_char: escape_char.clone(),
        },
        Expr::Function(function) if function.over.is_none() => {
            BroadSqlExprKind::Function(BroadSqlFunction {
                name: function.name.clone(),
                uses_odbc_syntax: function.uses_odbc_syntax,
                parameters: bind_broad_public_read_function_arguments(
                    &function.parameters,
                    registry,
                    visible_ctes,
                )?,
                args: bind_broad_public_read_function_arguments(
                    &function.args,
                    registry,
                    visible_ctes,
                )?,
                filter: function
                    .filter
                    .as_ref()
                    .map(|expr| bind_broad_public_read_expr(expr, registry, visible_ctes))
                    .transpose()?
                    .map(Box::new),
                null_treatment: function.null_treatment,
                within_group: function
                    .within_group
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
            })
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => BroadSqlExprKind::Case {
            operand: operand
                .as_ref()
                .map(|expr| bind_broad_public_read_expr(expr, registry, visible_ctes))
                .transpose()?
                .map(Box::new),
            conditions: conditions
                .iter()
                .map(|when| {
                    Ok(BroadSqlCaseWhen {
                        condition: bind_broad_public_read_expr(
                            &when.condition,
                            registry,
                            visible_ctes,
                        )?,
                        result: bind_broad_public_read_expr(&when.result, registry, visible_ctes)?,
                    })
                })
                .collect::<Result<_, LixError>>()?,
            else_result: else_result
                .as_ref()
                .map(|expr| bind_broad_public_read_expr(expr, registry, visible_ctes))
                .transpose()?
                .map(Box::new),
        },
        Expr::Exists { negated, subquery } => BroadSqlExprKind::Exists {
            negated: *negated,
            subquery: Box::new(bind_broad_public_read_query_scoped(
                subquery,
                registry,
                visible_ctes,
            )?),
        },
        Expr::Subquery(query) => BroadSqlExprKind::ScalarSubquery(Box::new(
            bind_broad_public_read_query_scoped(query, registry, visible_ctes)?,
        )),
        Expr::Tuple(items) => BroadSqlExprKind::Tuple(
            items
                .iter()
                .map(|item| bind_broad_public_read_expr(item, registry, visible_ctes))
                .collect::<Result<_, _>>()?,
        ),
        _ => BroadSqlExprKind::Unsupported {
            diagnostics_sql: expr.to_string(),
        },
    };
    Ok(BroadSqlExpr { kind })
}

fn bind_broad_public_read_function_arguments(
    arguments: &FunctionArguments,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadSqlFunctionArguments, LixError> {
    match arguments {
        FunctionArguments::None => Ok(BroadSqlFunctionArguments::None),
        FunctionArguments::Subquery(query) => Ok(BroadSqlFunctionArguments::Subquery(Box::new(
            bind_broad_public_read_query_scoped(query, registry, visible_ctes)?,
        ))),
        FunctionArguments::List(list) => Ok(BroadSqlFunctionArguments::List(
            BroadSqlFunctionArgumentList {
                duplicate_treatment: list.duplicate_treatment,
                args: list
                    .args
                    .iter()
                    .map(|arg| bind_broad_public_read_function_arg(arg, registry, visible_ctes))
                    .collect::<Result<_, _>>()?,
            },
        )),
    }
}

fn bind_broad_public_read_function_arg(
    arg: &FunctionArg,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadSqlFunctionArg, LixError> {
    match arg {
        FunctionArg::Named {
            name,
            arg,
            operator,
        } => Ok(BroadSqlFunctionArg::Named {
            name: name.clone(),
            arg: bind_broad_public_read_function_arg_expr(arg, registry, visible_ctes)?,
            operator: operator.clone(),
        }),
        FunctionArg::ExprNamed {
            name,
            arg,
            operator,
        } => Ok(BroadSqlFunctionArg::ExprNamed {
            name: bind_broad_public_read_expr(name, registry, visible_ctes)?,
            arg: bind_broad_public_read_function_arg_expr(arg, registry, visible_ctes)?,
            operator: operator.clone(),
        }),
        FunctionArg::Unnamed(arg) => Ok(BroadSqlFunctionArg::Unnamed(
            bind_broad_public_read_function_arg_expr(arg, registry, visible_ctes)?,
        )),
    }
}

fn bind_broad_public_read_function_arg_expr(
    arg: &FunctionArgExpr,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadSqlFunctionArgExpr, LixError> {
    match arg {
        FunctionArgExpr::Expr(expr) => Ok(BroadSqlFunctionArgExpr::Expr(
            bind_broad_public_read_expr(expr, registry, visible_ctes)?,
        )),
        FunctionArgExpr::QualifiedWildcard(object_name) => Ok(
            BroadSqlFunctionArgExpr::QualifiedWildcard(object_name.clone()),
        ),
        FunctionArgExpr::Wildcard => Ok(BroadSqlFunctionArgExpr::Wildcard),
    }
}

fn bind_broad_public_read_join_kind(
    join_operator: &JoinOperator,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadJoinKind, LixError> {
    Ok(match join_operator {
        JoinOperator::Join(constraint) => BroadPublicReadJoinKind::Join(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::Inner(constraint) => BroadPublicReadJoinKind::Inner(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::Left(constraint) => BroadPublicReadJoinKind::Left(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::LeftOuter(constraint) => BroadPublicReadJoinKind::LeftOuter(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::Right(constraint) => BroadPublicReadJoinKind::Right(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::RightOuter(constraint) => BroadPublicReadJoinKind::RightOuter(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::FullOuter(constraint) => BroadPublicReadJoinKind::FullOuter(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::CrossJoin(constraint) => BroadPublicReadJoinKind::CrossJoin(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::Semi(constraint) => BroadPublicReadJoinKind::Semi(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::LeftSemi(constraint) => BroadPublicReadJoinKind::LeftSemi(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::RightSemi(constraint) => BroadPublicReadJoinKind::RightSemi(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::Anti(constraint) => BroadPublicReadJoinKind::Anti(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::LeftAnti(constraint) => BroadPublicReadJoinKind::LeftAnti(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::RightAnti(constraint) => BroadPublicReadJoinKind::RightAnti(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::StraightJoin(constraint) => BroadPublicReadJoinKind::StraightJoin(
            bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        ),
        JoinOperator::CrossApply => BroadPublicReadJoinKind::CrossApply,
        JoinOperator::OuterApply => BroadPublicReadJoinKind::OuterApply,
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => BroadPublicReadJoinKind::AsOf {
            match_condition: bind_broad_public_read_expr(match_condition, registry, visible_ctes)?,
            constraint: bind_broad_public_read_join_constraint(constraint, registry, visible_ctes)?,
        },
    })
}

fn bind_broad_public_read_join_constraint(
    constraint: &JoinConstraint,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadJoinConstraint, LixError> {
    match constraint {
        JoinConstraint::None => Ok(BroadPublicReadJoinConstraint::None),
        JoinConstraint::Natural => Ok(BroadPublicReadJoinConstraint::Natural),
        JoinConstraint::Using(attrs) => Ok(BroadPublicReadJoinConstraint::Using(
            attrs.iter().map(ToString::to_string).collect(),
        )),
        JoinConstraint::On(expr) => Ok(BroadPublicReadJoinConstraint::On(
            bind_broad_public_read_expr(expr, registry, visible_ctes)?,
        )),
    }
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
        explicit: alias.explicit,
        name: alias.name.value.clone(),
        columns: alias
            .columns
            .iter()
            .map(|column| column.name.value.clone())
            .collect(),
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
        BroadPublicReadDistinct, BroadPublicReadGroupByKind, BroadPublicReadLimitClauseKind,
        BroadPublicReadOrderByKind, BroadPublicReadProjectionItemKind, BroadPublicReadSetExpr,
        BroadPublicReadStatement, BroadSqlExpr, BroadSqlExprKind,
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

    fn expr_contains_variant(
        expr: &BroadSqlExpr,
        predicate: &impl Fn(&BroadSqlExprKind) -> bool,
    ) -> bool {
        if predicate(&expr.kind) {
            return true;
        }
        match &expr.kind {
            BroadSqlExprKind::Identifier(_)
            | BroadSqlExprKind::CompoundIdentifier(_)
            | BroadSqlExprKind::Value(_)
            | BroadSqlExprKind::TypedString { .. }
            | BroadSqlExprKind::Unsupported { .. } => false,
            BroadSqlExprKind::BinaryOp { left, right, .. }
            | BroadSqlExprKind::IsDistinctFrom { left, right }
            | BroadSqlExprKind::IsNotDistinctFrom { left, right } => {
                expr_contains_variant(left, predicate) || expr_contains_variant(right, predicate)
            }
            BroadSqlExprKind::AnyOp { left, right, .. }
            | BroadSqlExprKind::AllOp { left, right, .. } => {
                expr_contains_variant(left, predicate) || expr_contains_variant(right, predicate)
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
            | BroadSqlExprKind::Cast { expr, .. } => expr_contains_variant(expr, predicate),
            BroadSqlExprKind::InList { expr, list, .. } => {
                expr_contains_variant(expr, predicate)
                    || list
                        .iter()
                        .any(|item| expr_contains_variant(item, predicate))
            }
            BroadSqlExprKind::InSubquery { expr, subquery, .. } => {
                expr_contains_variant(expr, predicate)
                    || query_contains_variant(subquery, predicate)
            }
            BroadSqlExprKind::InUnnest {
                expr, array_expr, ..
            } => {
                expr_contains_variant(expr, predicate)
                    || expr_contains_variant(array_expr, predicate)
            }
            BroadSqlExprKind::Between {
                expr, low, high, ..
            } => {
                expr_contains_variant(expr, predicate)
                    || expr_contains_variant(low, predicate)
                    || expr_contains_variant(high, predicate)
            }
            BroadSqlExprKind::Like { expr, pattern, .. }
            | BroadSqlExprKind::ILike { expr, pattern, .. } => {
                expr_contains_variant(expr, predicate) || expr_contains_variant(pattern, predicate)
            }
            BroadSqlExprKind::Function(function) => {
                function
                    .filter
                    .as_ref()
                    .is_some_and(|expr| expr_contains_variant(expr, predicate))
                    || function
                        .within_group
                        .iter()
                        .any(|expr| expr_contains_variant(&expr.expr, predicate))
            }
            BroadSqlExprKind::Case {
                operand,
                conditions,
                else_result,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|expr| expr_contains_variant(expr, predicate))
                    || conditions.iter().any(|when| {
                        expr_contains_variant(&when.condition, predicate)
                            || expr_contains_variant(&when.result, predicate)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|expr| expr_contains_variant(expr, predicate))
            }
            BroadSqlExprKind::Exists { subquery, .. }
            | BroadSqlExprKind::ScalarSubquery(subquery) => {
                query_contains_variant(subquery, predicate)
            }
            BroadSqlExprKind::Tuple(items) => items
                .iter()
                .any(|expr| expr_contains_variant(expr, predicate)),
        }
    }

    fn query_contains_variant(
        query: &crate::sql::logical_plan::public_ir::BroadPublicReadQuery,
        predicate: &impl Fn(&BroadSqlExprKind) -> bool,
    ) -> bool {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                if query_contains_variant(&cte.query, predicate) {
                    return true;
                }
            }
        }
        set_expr_contains_variant(&query.body, predicate)
            || query.order_by.as_ref().is_some_and(|order_by| {
                matches!(
                    &order_by.kind,
                    BroadPublicReadOrderByKind::Expressions(expressions)
                        if expressions
                            .iter()
                            .any(|expr| expr_contains_variant(&expr.expr, predicate))
                )
            })
            || query
                .limit_clause
                .as_ref()
                .is_some_and(|limit_clause| match &limit_clause.kind {
                    BroadPublicReadLimitClauseKind::LimitOffset {
                        limit,
                        offset,
                        limit_by,
                    } => {
                        limit
                            .as_ref()
                            .is_some_and(|expr| expr_contains_variant(expr, predicate))
                            || offset.as_ref().is_some_and(|offset| {
                                expr_contains_variant(&offset.value, predicate)
                            })
                            || limit_by
                                .iter()
                                .any(|expr| expr_contains_variant(expr, predicate))
                    }
                    BroadPublicReadLimitClauseKind::OffsetCommaLimit { offset, limit } => {
                        expr_contains_variant(offset, predicate)
                            || expr_contains_variant(limit, predicate)
                    }
                })
    }

    fn set_expr_contains_variant(
        expr: &crate::sql::logical_plan::public_ir::BroadPublicReadSetExpr,
        predicate: &impl Fn(&BroadSqlExprKind) -> bool,
    ) -> bool {
        match expr {
            BroadPublicReadSetExpr::Select(select) => {
                broad_public_read_distinct_contains_variant(select.distinct.as_ref(), predicate)
                    || select.projection.iter().any(|projection| {
                        matches!(
                            &projection.kind,
                            BroadPublicReadProjectionItemKind::Expr { expr, .. }
                                if expr_contains_variant(expr, predicate)
                        )
                    })
                    || select
                        .selection
                        .as_ref()
                        .is_some_and(|expr| expr_contains_variant(expr, predicate))
                    || matches!(
                        &select.group_by.kind,
                        BroadPublicReadGroupByKind::Expressions(expressions)
                            if expressions
                                .iter()
                                .any(|expr| expr_contains_variant(expr, predicate))
                    )
                    || select
                        .having
                        .as_ref()
                        .is_some_and(|expr| expr_contains_variant(expr, predicate))
            }
            BroadPublicReadSetExpr::Query(query) => query_contains_variant(query, predicate),
            BroadPublicReadSetExpr::SetOperation { left, right, .. } => {
                set_expr_contains_variant(left, predicate)
                    || set_expr_contains_variant(right, predicate)
            }
            BroadPublicReadSetExpr::Table { .. } | BroadPublicReadSetExpr::Other { .. } => false,
        }
    }

    fn broad_public_read_distinct_contains_variant(
        distinct: Option<&BroadPublicReadDistinct>,
        predicate: &impl Fn(&BroadSqlExprKind) -> bool,
    ) -> bool {
        match distinct {
            Some(BroadPublicReadDistinct::On(expressions)) => expressions
                .iter()
                .any(|expr| expr_contains_variant(expr, predicate)),
            Some(BroadPublicReadDistinct::Distinct) | None => false,
        }
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
        assert_eq!(with.cte_tables[0].alias.name, "latest");
        assert!(matches!(
            with.cte_tables[0].query.body,
            BroadPublicReadSetExpr::Select(_)
        ));

        let BroadPublicReadSetExpr::Select(select) = &query.body else {
            panic!("expected top-level broad select");
        };

        assert!(select.distinct.is_none());
        assert_eq!(select.projection.len(), 2);
        match &select.projection[0].kind {
            BroadPublicReadProjectionItemKind::Expr { alias, expr } => {
                assert_eq!(alias, &None);
                assert!(!expr_contains_variant(expr, &|kind| {
                    matches!(
                        kind,
                        BroadSqlExprKind::ScalarSubquery(_)
                            | BroadSqlExprKind::Exists { .. }
                            | BroadSqlExprKind::InSubquery { .. }
                    )
                }));
            }
            other => panic!("unexpected first projection item: {other:?}"),
        }
        match &select.projection[1].kind {
            BroadPublicReadProjectionItemKind::Expr { alias, expr } => {
                assert_eq!(alias.as_deref(), Some("file_count"));
                assert!(matches!(expr.kind, BroadSqlExprKind::ScalarSubquery(_)));
            }
            other => panic!("unexpected second projection item: {other:?}"),
        }

        let selection = select
            .selection
            .as_ref()
            .expect("expected typed selection expression");
        assert!(expr_contains_variant(selection, &|kind| {
            matches!(kind, BroadSqlExprKind::Exists { .. })
        }));
        assert!(expr_contains_variant(selection, &|kind| {
            matches!(kind, BroadSqlExprKind::InSubquery { .. })
        }));

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
    fn binds_broad_public_read_select_distinct_into_typed_ir() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let bound = bind_broad_public_read_statement_with_registry(
            &parse_one(
                "SELECT DISTINCT schema_key \
                 FROM lix_state_by_version \
                 WHERE entity_id = 'version-a'",
            ),
            &registry,
        )
        .expect("broad binding should succeed")
        .expect("query should bind as a broad public read");

        let BroadPublicReadStatement::Query(query) = bound else {
            panic!("expected broad query statement");
        };
        let BroadPublicReadSetExpr::Select(select) = &query.body else {
            panic!("expected typed select");
        };

        assert!(matches!(
            &select.distinct,
            Some(BroadPublicReadDistinct::Distinct)
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
