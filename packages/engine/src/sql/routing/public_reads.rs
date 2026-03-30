use crate::sql::catalog::{SurfaceBinding, SurfaceFamily, SurfaceRegistry, SurfaceVariant};
use crate::sql::logical_plan::public_ir::{
    BroadNestedQueryExpr, BroadPublicReadCte, BroadPublicReadGroupBy, BroadPublicReadGroupByKind,
    BroadPublicReadJoin, BroadPublicReadLimitClause, BroadPublicReadLimitClauseKind,
    BroadPublicReadOrderBy, BroadPublicReadOrderByExpr, BroadPublicReadOrderByKind,
    BroadPublicReadProjectionItem, BroadPublicReadProjectionItemKind, BroadPublicReadQuery,
    BroadPublicReadRelation, BroadPublicReadSelect, BroadPublicReadSetExpr,
    BroadPublicReadStatement, BroadPublicReadTableFactor, BroadPublicReadTableWithJoins,
    BroadPublicReadWith, BroadSqlExpr,
};
#[cfg(test)]
use crate::sql::physical_plan::lowerer::bind_broad_public_read_statement_with_registry;
use crate::sql::physical_plan::lowerer::broad_public_relation_supports_terminal_render;
use crate::sql::routing::registry::{
    run_fallible_pass, run_infallible_pass, RoutingPassMetadata, RoutingPassOutcome,
    RoutingPassRegistry, RoutingPassSettings, RoutingPassTrace,
};
use crate::{LixError, SqlDialect};
use serde_json::Value as JsonValue;
#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
thread_local! {
    static FORBID_BROAD_ROUTING_FOR_TEST: Cell<bool> = const { Cell::new(false) };
    static BROAD_ROUTING_DELAY_US_FOR_TEST: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) struct ForbidBroadRoutingForTestGuard {
    previous: bool,
}

#[cfg(test)]
impl Drop for ForbidBroadRoutingForTestGuard {
    fn drop(&mut self) {
        FORBID_BROAD_ROUTING_FOR_TEST.set(self.previous);
    }
}

#[cfg(test)]
pub(crate) fn forbid_broad_routing_for_test() -> ForbidBroadRoutingForTestGuard {
    let previous = FORBID_BROAD_ROUTING_FOR_TEST.replace(true);
    ForbidBroadRoutingForTestGuard { previous }
}

#[cfg(test)]
pub(crate) struct BroadRoutingDelayForTestGuard {
    previous_delay_us: u64,
}

#[cfg(test)]
impl Drop for BroadRoutingDelayForTestGuard {
    fn drop(&mut self) {
        BROAD_ROUTING_DELAY_US_FOR_TEST.set(self.previous_delay_us);
    }
}

#[cfg(test)]
pub(crate) fn delay_broad_routing_for_test(delay: Duration) -> BroadRoutingDelayForTestGuard {
    let previous_delay_us =
        BROAD_ROUTING_DELAY_US_FOR_TEST.replace(delay.as_micros().min(u128::from(u64::MAX)) as u64);
    BroadRoutingDelayForTestGuard { previous_delay_us }
}

#[cfg(test)]
fn assert_broad_routing_allowed_for_test() {
    if FORBID_BROAD_ROUTING_FOR_TEST.get() {
        panic!("broad routing must not run inside this test scope");
    }
}

#[cfg(test)]
fn apply_broad_routing_delay_for_test() {
    let delay_us = BROAD_ROUTING_DELAY_US_FOR_TEST.get();
    if delay_us > 0 {
        std::thread::sleep(Duration::from_micros(delay_us));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublicReadExecutionRouteDecision {
    pub(crate) direct_execution: bool,
    pub(crate) pass_traces: Vec<RoutingPassTrace>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RoutedBroadPublicRead {
    pub(crate) broad_statement: BroadPublicReadStatement,
    pub(crate) pass_traces: Vec<RoutingPassTrace>,
}

const EXECUTION_STRATEGY_ROUTE_PASS: RoutingPassMetadata = RoutingPassMetadata {
    name: "public-read.route-execution-strategy",
    order: 10,
    description: "route structured public reads into direct-history or lowered-sql execution",
};

const LOWERABLE_BROAD_RELATION_ROUTE_PASS: RoutingPassMetadata = RoutingPassMetadata {
    name: "public-read.route-lowerable-relations",
    order: 20,
    description: "route typed broad public relations into lowerable broad relations",
};

const PUBLIC_READ_REGISTRY: RoutingPassRegistry = RoutingPassRegistry {
    name: "public-read",
    passes: &[
        EXECUTION_STRATEGY_ROUTE_PASS,
        LOWERABLE_BROAD_RELATION_ROUTE_PASS,
    ],
};

pub(crate) fn public_read_routing_pass_registry() -> &'static RoutingPassRegistry {
    &PUBLIC_READ_REGISTRY
}

pub(crate) fn route_public_read_execution_strategy(
    binding: &SurfaceBinding,
) -> PublicReadExecutionRouteDecision {
    route_public_read_execution_strategy_with_settings(binding, &RoutingPassSettings::default())
}

fn route_public_read_execution_strategy_with_settings(
    binding: &SurfaceBinding,
    settings: &RoutingPassSettings,
) -> PublicReadExecutionRouteDecision {
    let metadata = public_read_routing_pass_registry().passes[0];
    let direct_execution = is_direct_only_history_surface(binding);
    let trace = run_infallible_pass(metadata, settings, || {
        let mut diagnostics = vec![format!(
            "surface '{}' family={} variant={}",
            binding.descriptor.public_name,
            surface_family_name(binding.descriptor.surface_family),
            surface_variant_name(binding.descriptor.surface_variant)
        )];
        if direct_execution {
            diagnostics.push("direct history execution strategy selected".to_string());
        } else {
            diagnostics.push("surface is not eligible for direct history execution".to_string());
        }
        RoutingPassOutcome {
            changed: direct_execution,
            diagnostics,
        }
    });
    PublicReadExecutionRouteDecision {
        direct_execution,
        pass_traces: vec![trace],
    }
}

fn surface_family_name(family: SurfaceFamily) -> &'static str {
    match family {
        SurfaceFamily::State => "state",
        SurfaceFamily::Entity => "entity",
        SurfaceFamily::Filesystem => "filesystem",
        SurfaceFamily::Admin => "admin",
        SurfaceFamily::Change => "change",
    }
}

fn surface_variant_name(variant: SurfaceVariant) -> &'static str {
    match variant {
        SurfaceVariant::Default => "default",
        SurfaceVariant::ByVersion => "by_version",
        SurfaceVariant::History => "history",
        SurfaceVariant::WorkingChanges => "working_changes",
    }
}

#[cfg(test)]
pub(crate) fn route_broad_public_read_statement(
    statement: &sqlparser::ast::Statement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
) -> Result<RoutedBroadPublicRead, LixError> {
    let broad_statement = bind_broad_public_read_statement_with_registry(statement, registry)?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "broad public-read routing requires a typed broad public-read statement",
            )
        })?;
    route_broad_public_read_statement_with_known_live_layouts(
        &broad_statement,
        registry,
        dialect,
        active_version_id,
        &BTreeMap::new(),
    )
}

pub(crate) fn route_broad_public_read_statement_with_known_live_layouts(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<RoutedBroadPublicRead, LixError> {
    route_broad_public_read_statement_with_settings(
        statement,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        &RoutingPassSettings::default(),
    )
}

fn route_broad_public_read_statement_with_settings(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    settings: &RoutingPassSettings,
) -> Result<RoutedBroadPublicRead, LixError> {
    #[cfg(test)]
    {
        assert_broad_routing_allowed_for_test();
        apply_broad_routing_delay_for_test();
    }

    let metadata = public_read_routing_pass_registry().passes[1];
    let before_summary = summarize_broad_public_read_statement(statement);
    let mut optimized_broad_statement = statement.clone();
    let trace = run_fallible_pass(metadata, settings, || {
        optimized_broad_statement = route_broad_public_read_statement_relations(
            statement,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?;
        let changed = optimized_broad_statement != *statement;
        let lowered_summary = summarize_lowered_public_relations(&optimized_broad_statement);
        Ok::<RoutingPassOutcome, LixError>(RoutingPassOutcome {
            changed,
            diagnostics: broad_routing_diagnostics(&before_summary, &lowered_summary, changed),
        })
    })?;

    Ok(RoutedBroadPublicRead {
        broad_statement: optimized_broad_statement,
        pass_traces: vec![trace],
    })
}

fn broad_routing_diagnostics(
    summary: &BTreeSet<String>,
    lowered_summary: &BTreeSet<String>,
    changed: bool,
) -> Vec<String> {
    let mut diagnostics = Vec::new();
    if summary.is_empty() {
        diagnostics.push("no typed public relations matched broad routing".to_string());
    } else {
        diagnostics.push(format!(
            "public relations: {}",
            summary.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    if !lowered_summary.is_empty() {
        diagnostics.push(format!(
            "lowered public relations: {}",
            lowered_summary
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if changed {
        diagnostics
            .push("routed typed broad public relations into lowerable broad relations".to_string());
    } else {
        diagnostics.push("typed broad statement was already routed or not lowerable".to_string());
    }
    diagnostics
}

fn route_broad_public_read_statement_relations(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadStatement, LixError> {
    match statement {
        BroadPublicReadStatement::Query(query) => Ok(BroadPublicReadStatement::Query(
            route_broad_public_read_query(
                query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadStatement::Explain {
            original,
            statement,
        } => Ok(BroadPublicReadStatement::Explain {
            original: original.clone(),
            statement: Box::new(route_broad_public_read_statement_relations(
                statement,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        }),
    }
}

fn route_broad_public_read_query(
    query: &BroadPublicReadQuery,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadQuery, LixError> {
    Ok(BroadPublicReadQuery {
        provenance: query.provenance.clone(),
        with: query
            .with
            .as_ref()
            .map(|with| {
                route_broad_public_read_with(
                    with,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .transpose()?,
        body: route_broad_public_read_set_expr(
            &query.body,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
        order_by: query
            .order_by
            .as_ref()
            .map(|order_by| {
                route_broad_public_read_order_by(
                    order_by,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .transpose()?,
        limit_clause: query
            .limit_clause
            .as_ref()
            .map(|limit_clause| {
                route_broad_public_read_limit_clause(
                    limit_clause,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .transpose()?,
    })
}

fn route_broad_public_read_with(
    with: &BroadPublicReadWith,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadWith, LixError> {
    Ok(BroadPublicReadWith {
        provenance: with.provenance.clone(),
        cte_tables: with
            .cte_tables
            .iter()
            .map(|cte| -> Result<BroadPublicReadCte, LixError> {
                Ok(BroadPublicReadCte {
                    name: cte.name.clone(),
                    query: route_broad_public_read_query(
                        &cte.query,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )?,
                })
            })
            .collect::<Result<_, _>>()?,
    })
}

fn route_broad_public_read_set_expr(
    expr: &BroadPublicReadSetExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadSetExpr, LixError> {
    match expr {
        BroadPublicReadSetExpr::Select(select) => Ok(BroadPublicReadSetExpr::Select(
            route_broad_public_read_select(
                select,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadSetExpr::Query(query) => Ok(BroadPublicReadSetExpr::Query(Box::new(
            route_broad_public_read_query(
                query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        ))),
        BroadPublicReadSetExpr::SetOperation {
            provenance,
            operator,
            quantifier,
            left,
            right,
        } => Ok(BroadPublicReadSetExpr::SetOperation {
            provenance: provenance.clone(),
            operator: *operator,
            quantifier: *quantifier,
            left: Box::new(route_broad_public_read_set_expr(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            right: Box::new(route_broad_public_read_set_expr(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        }),
        BroadPublicReadSetExpr::Table {
            provenance,
            relation,
        } => Ok(BroadPublicReadSetExpr::Table {
            provenance: provenance.clone(),
            relation: route_broad_public_read_relation(
                relation,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        }),
        BroadPublicReadSetExpr::Other { provenance } => Ok(BroadPublicReadSetExpr::Other {
            provenance: provenance.clone(),
        }),
    }
}

fn route_broad_public_read_select(
    select: &BroadPublicReadSelect,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadSelect, LixError> {
    Ok(BroadPublicReadSelect {
        provenance: select.provenance.clone(),
        projection: select
            .projection
            .iter()
            .map(|projection| {
                route_broad_public_read_projection_item(
                    projection,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .collect::<Result<_, _>>()?,
        from: select
            .from
            .iter()
            .map(|table| {
                route_broad_public_read_table_with_joins(
                    table,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .collect::<Result<_, _>>()?,
        selection: select
            .selection
            .as_ref()
            .map(|selection| {
                route_broad_sql_expr(
                    selection,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .transpose()?,
        group_by: route_broad_public_read_group_by(
            &select.group_by,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
        having: select
            .having
            .as_ref()
            .map(|having| {
                route_broad_sql_expr(
                    having,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .transpose()?,
    })
}

fn route_broad_public_read_table_with_joins(
    table: &BroadPublicReadTableWithJoins,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadTableWithJoins, LixError> {
    Ok(BroadPublicReadTableWithJoins {
        provenance: table.provenance.clone(),
        relation: route_broad_public_read_table_factor(
            &table.relation,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
        joins: table
            .joins
            .iter()
            .map(|join| {
                route_broad_public_read_join(
                    join,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .collect::<Result<_, _>>()?,
    })
}

fn route_broad_public_read_join(
    join: &BroadPublicReadJoin,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadJoin, LixError> {
    Ok(BroadPublicReadJoin {
        provenance: join.provenance.clone(),
        operator: join.operator.clone(),
        relation: route_broad_public_read_table_factor(
            &join.relation,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
        constraint_expressions: join
            .constraint_expressions
            .iter()
            .map(|expr| {
                route_broad_sql_expr(
                    expr,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .collect::<Result<_, _>>()?,
    })
}

fn route_broad_public_read_projection_item(
    item: &BroadPublicReadProjectionItem,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadProjectionItem, LixError> {
    let kind = match &item.kind {
        BroadPublicReadProjectionItemKind::Wildcard => BroadPublicReadProjectionItemKind::Wildcard,
        BroadPublicReadProjectionItemKind::QualifiedWildcard { qualifier } => {
            BroadPublicReadProjectionItemKind::QualifiedWildcard {
                qualifier: qualifier.clone(),
            }
        }
        BroadPublicReadProjectionItemKind::Expr {
            alias,
            sql,
            nested_queries,
        } => BroadPublicReadProjectionItemKind::Expr {
            alias: alias.clone(),
            sql: sql.clone(),
            nested_queries: nested_queries
                .iter()
                .map(|nested_query| {
                    route_broad_nested_query_expr(
                        nested_query,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                })
                .collect::<Result<_, _>>()?,
        },
    };

    Ok(BroadPublicReadProjectionItem {
        provenance: item.provenance.clone(),
        kind,
    })
}

fn route_broad_public_read_group_by(
    group_by: &BroadPublicReadGroupBy,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadGroupBy, LixError> {
    let kind = match &group_by.kind {
        BroadPublicReadGroupByKind::All => BroadPublicReadGroupByKind::All,
        BroadPublicReadGroupByKind::Expressions(expressions) => {
            BroadPublicReadGroupByKind::Expressions(
                expressions
                    .iter()
                    .map(|expr| {
                        route_broad_sql_expr(
                            expr,
                            registry,
                            dialect,
                            active_version_id,
                            known_live_layouts,
                        )
                    })
                    .collect::<Result<_, _>>()?,
            )
        }
    };

    Ok(BroadPublicReadGroupBy {
        provenance: group_by.provenance.clone(),
        kind,
    })
}

fn route_broad_public_read_order_by(
    order_by: &BroadPublicReadOrderBy,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadOrderBy, LixError> {
    let kind = match &order_by.kind {
        BroadPublicReadOrderByKind::All => BroadPublicReadOrderByKind::All,
        BroadPublicReadOrderByKind::Expressions(expressions) => {
            BroadPublicReadOrderByKind::Expressions(
                expressions
                    .iter()
                    .map(|expr| {
                        Ok(BroadPublicReadOrderByExpr {
                            provenance: expr.provenance.clone(),
                            expr: route_broad_sql_expr(
                                &expr.expr,
                                registry,
                                dialect,
                                active_version_id,
                                known_live_layouts,
                            )?,
                            asc: expr.asc,
                            nulls_first: expr.nulls_first,
                        })
                    })
                    .collect::<Result<_, LixError>>()?,
            )
        }
    };

    Ok(BroadPublicReadOrderBy {
        provenance: order_by.provenance.clone(),
        kind,
    })
}

fn route_broad_public_read_limit_clause(
    limit_clause: &BroadPublicReadLimitClause,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadLimitClause, LixError> {
    let kind = match &limit_clause.kind {
        BroadPublicReadLimitClauseKind::LimitOffset {
            limit,
            offset,
            limit_by,
        } => BroadPublicReadLimitClauseKind::LimitOffset {
            limit: limit
                .as_ref()
                .map(|expr| {
                    route_broad_sql_expr(
                        expr,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                })
                .transpose()?,
            offset: offset
                .as_ref()
                .map(|expr| {
                    route_broad_sql_expr(
                        expr,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                })
                .transpose()?,
            limit_by: limit_by
                .iter()
                .map(|expr| {
                    route_broad_sql_expr(
                        expr,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                })
                .collect::<Result<_, _>>()?,
        },
        BroadPublicReadLimitClauseKind::OffsetCommaLimit { offset, limit } => {
            BroadPublicReadLimitClauseKind::OffsetCommaLimit {
                offset: route_broad_sql_expr(
                    offset,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )?,
                limit: route_broad_sql_expr(
                    limit,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )?,
            }
        }
    };

    Ok(BroadPublicReadLimitClause {
        provenance: limit_clause.provenance.clone(),
        kind,
    })
}

fn route_broad_sql_expr(
    expr: &BroadSqlExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadSqlExpr, LixError> {
    Ok(BroadSqlExpr {
        provenance: expr.provenance.clone(),
        sql: expr.sql.clone(),
        nested_queries: expr
            .nested_queries
            .iter()
            .map(|nested_query| {
                route_broad_nested_query_expr(
                    nested_query,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .collect::<Result<_, _>>()?,
    })
}

fn route_broad_nested_query_expr(
    expr: &BroadNestedQueryExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadNestedQueryExpr, LixError> {
    match expr {
        BroadNestedQueryExpr::ScalarSubquery(query) => Ok(BroadNestedQueryExpr::ScalarSubquery(
            Box::new(route_broad_public_read_query(
                query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        )),
        BroadNestedQueryExpr::Exists { negated, subquery } => Ok(BroadNestedQueryExpr::Exists {
            negated: *negated,
            subquery: Box::new(route_broad_public_read_query(
                subquery,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        }),
        BroadNestedQueryExpr::InSubquery {
            negated,
            expr,
            expr_sql,
            subquery,
        } => Ok(BroadNestedQueryExpr::InSubquery {
            negated: *negated,
            expr_sql: expr_sql.clone(),
            expr: Box::new(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            subquery: Box::new(route_broad_public_read_query(
                subquery,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        }),
    }
}

fn route_broad_public_read_table_factor(
    factor: &BroadPublicReadTableFactor,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadTableFactor, LixError> {
    match factor {
        BroadPublicReadTableFactor::Table {
            provenance,
            alias,
            relation,
        } => Ok(BroadPublicReadTableFactor::Table {
            provenance: provenance.clone(),
            alias: alias.clone(),
            relation: route_broad_public_read_relation(
                relation,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        }),
        BroadPublicReadTableFactor::Derived {
            provenance,
            alias,
            subquery,
        } => Ok(BroadPublicReadTableFactor::Derived {
            provenance: provenance.clone(),
            alias: alias.clone(),
            subquery: Box::new(route_broad_public_read_query(
                subquery,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        }),
        BroadPublicReadTableFactor::NestedJoin {
            provenance,
            alias,
            table_with_joins,
        } => Ok(BroadPublicReadTableFactor::NestedJoin {
            provenance: provenance.clone(),
            alias: alias.clone(),
            table_with_joins: Box::new(route_broad_public_read_table_with_joins(
                table_with_joins,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        }),
        BroadPublicReadTableFactor::Other { provenance } => Ok(BroadPublicReadTableFactor::Other {
            provenance: provenance.clone(),
        }),
    }
}

fn route_broad_public_read_relation(
    relation: &BroadPublicReadRelation,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadRelation, LixError> {
    match relation {
        BroadPublicReadRelation::Public(binding)
            if broad_public_relation_supports_terminal_render(
                binding,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )? =>
        {
            Ok(BroadPublicReadRelation::LoweredPublic(binding.clone()))
        }
        _ => Ok(relation.clone()),
    }
}

fn summarize_broad_public_read_statement(statement: &BroadPublicReadStatement) -> BTreeSet<String> {
    let mut relations = BTreeSet::new();
    collect_public_relations(statement, &mut relations);
    relations
}

fn summarize_lowered_public_relations(statement: &BroadPublicReadStatement) -> BTreeSet<String> {
    let mut relations = BTreeSet::new();
    collect_lowered_public_relations(statement, &mut relations);
    relations
}

fn collect_public_relations(statement: &BroadPublicReadStatement, out: &mut BTreeSet<String>) {
    match statement {
        BroadPublicReadStatement::Query(query) => collect_public_relations_in_query(query, out),
        BroadPublicReadStatement::Explain { statement, .. } => {
            collect_public_relations(statement, out)
        }
    }
}

fn collect_public_relations_in_query(query: &BroadPublicReadQuery, out: &mut BTreeSet<String>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_public_relations_in_query(&cte.query, out);
        }
    }
    collect_public_relations_in_set_expr(&query.body, out);
    if let Some(order_by) = &query.order_by {
        collect_public_relations_in_order_by(order_by, out);
    }
    if let Some(limit_clause) = &query.limit_clause {
        collect_public_relations_in_limit_clause(limit_clause, out);
    }
}

fn collect_public_relations_in_set_expr(expr: &BroadPublicReadSetExpr, out: &mut BTreeSet<String>) {
    match expr {
        BroadPublicReadSetExpr::Select(select) => collect_public_relations_in_select(select, out),
        BroadPublicReadSetExpr::Query(query) => collect_public_relations_in_query(query, out),
        BroadPublicReadSetExpr::SetOperation { left, right, .. } => {
            collect_public_relations_in_set_expr(left, out);
            collect_public_relations_in_set_expr(right, out);
        }
        BroadPublicReadSetExpr::Table { relation, .. } => {
            collect_public_relation_name(relation, out);
        }
        BroadPublicReadSetExpr::Other { .. } => {}
    }
}

fn collect_public_relations_in_select(select: &BroadPublicReadSelect, out: &mut BTreeSet<String>) {
    for projection in &select.projection {
        collect_public_relations_in_projection_item(projection, out);
    }
    for table in &select.from {
        collect_public_relations_in_table_with_joins(table, out);
    }
    if let Some(selection) = &select.selection {
        collect_public_relations_in_sql_expr(selection, out);
    }
    collect_public_relations_in_group_by(&select.group_by, out);
    if let Some(having) = &select.having {
        collect_public_relations_in_sql_expr(having, out);
    }
}

fn collect_public_relations_in_table_with_joins(
    table: &BroadPublicReadTableWithJoins,
    out: &mut BTreeSet<String>,
) {
    collect_public_relations_in_table_factor(&table.relation, out);
    for join in &table.joins {
        collect_public_relations_in_table_factor(&join.relation, out);
        for expr in &join.constraint_expressions {
            collect_public_relations_in_sql_expr(expr, out);
        }
    }
}

fn collect_public_relations_in_table_factor(
    factor: &BroadPublicReadTableFactor,
    out: &mut BTreeSet<String>,
) {
    match factor {
        BroadPublicReadTableFactor::Table { relation, .. } => {
            collect_public_relation_name(relation, out);
        }
        BroadPublicReadTableFactor::Derived { subquery, .. } => {
            collect_public_relations_in_query(subquery, out);
        }
        BroadPublicReadTableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_public_relations_in_table_with_joins(table_with_joins, out),
        BroadPublicReadTableFactor::Other { .. } => {}
    }
}

fn collect_public_relations_in_projection_item(
    item: &BroadPublicReadProjectionItem,
    out: &mut BTreeSet<String>,
) {
    match &item.kind {
        BroadPublicReadProjectionItemKind::Expr { nested_queries, .. } => {
            for query in nested_queries {
                collect_public_relations_in_nested_query_expr(query, out);
            }
        }
        BroadPublicReadProjectionItemKind::Wildcard
        | BroadPublicReadProjectionItemKind::QualifiedWildcard { .. } => {}
    }
}

fn collect_public_relations_in_group_by(
    group_by: &BroadPublicReadGroupBy,
    out: &mut BTreeSet<String>,
) {
    match &group_by.kind {
        BroadPublicReadGroupByKind::All => {}
        BroadPublicReadGroupByKind::Expressions(expressions) => {
            for expr in expressions {
                collect_public_relations_in_sql_expr(expr, out);
            }
        }
    }
}

fn collect_public_relations_in_order_by(
    order_by: &BroadPublicReadOrderBy,
    out: &mut BTreeSet<String>,
) {
    match &order_by.kind {
        BroadPublicReadOrderByKind::All => {}
        BroadPublicReadOrderByKind::Expressions(expressions) => {
            for expr in expressions {
                collect_public_relations_in_sql_expr(&expr.expr, out);
            }
        }
    }
}

fn collect_public_relations_in_limit_clause(
    limit_clause: &BroadPublicReadLimitClause,
    out: &mut BTreeSet<String>,
) {
    match &limit_clause.kind {
        BroadPublicReadLimitClauseKind::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if let Some(limit) = limit {
                collect_public_relations_in_sql_expr(limit, out);
            }
            if let Some(offset) = offset {
                collect_public_relations_in_sql_expr(offset, out);
            }
            for expr in limit_by {
                collect_public_relations_in_sql_expr(expr, out);
            }
        }
        BroadPublicReadLimitClauseKind::OffsetCommaLimit { offset, limit } => {
            collect_public_relations_in_sql_expr(offset, out);
            collect_public_relations_in_sql_expr(limit, out);
        }
    }
}

fn collect_public_relations_in_sql_expr(expr: &BroadSqlExpr, out: &mut BTreeSet<String>) {
    for nested_query in &expr.nested_queries {
        collect_public_relations_in_nested_query_expr(nested_query, out);
    }
}

fn collect_public_relations_in_nested_query_expr(
    expr: &BroadNestedQueryExpr,
    out: &mut BTreeSet<String>,
) {
    match expr {
        BroadNestedQueryExpr::ScalarSubquery(query) => {
            collect_public_relations_in_query(query, out);
        }
        BroadNestedQueryExpr::Exists { subquery, .. } => {
            collect_public_relations_in_query(subquery, out);
        }
        BroadNestedQueryExpr::InSubquery { expr, subquery, .. } => {
            collect_public_relations_in_sql_expr(expr, out);
            collect_public_relations_in_query(subquery, out);
        }
    }
}

fn collect_public_relation_name(relation: &BroadPublicReadRelation, out: &mut BTreeSet<String>) {
    match relation {
        BroadPublicReadRelation::Public(binding)
        | BroadPublicReadRelation::LoweredPublic(binding) => {
            out.insert(binding.descriptor.public_name.clone());
        }
        BroadPublicReadRelation::Internal(_)
        | BroadPublicReadRelation::External(_)
        | BroadPublicReadRelation::Cte(_) => {}
    }
}

fn collect_lowered_public_relations(
    statement: &BroadPublicReadStatement,
    out: &mut BTreeSet<String>,
) {
    match statement {
        BroadPublicReadStatement::Query(query) => {
            collect_lowered_public_relations_in_query(query, out)
        }
        BroadPublicReadStatement::Explain { statement, .. } => {
            collect_lowered_public_relations(statement, out)
        }
    }
}

fn collect_lowered_public_relations_in_query(
    query: &BroadPublicReadQuery,
    out: &mut BTreeSet<String>,
) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_lowered_public_relations_in_query(&cte.query, out);
        }
    }
    collect_lowered_public_relations_in_set_expr(&query.body, out);
    if let Some(order_by) = &query.order_by {
        collect_lowered_public_relations_in_order_by(order_by, out);
    }
    if let Some(limit_clause) = &query.limit_clause {
        collect_lowered_public_relations_in_limit_clause(limit_clause, out);
    }
}

fn collect_lowered_public_relations_in_set_expr(
    expr: &BroadPublicReadSetExpr,
    out: &mut BTreeSet<String>,
) {
    match expr {
        BroadPublicReadSetExpr::Select(select) => {
            collect_lowered_public_relations_in_select(select, out)
        }
        BroadPublicReadSetExpr::Query(query) => {
            collect_lowered_public_relations_in_query(query, out)
        }
        BroadPublicReadSetExpr::SetOperation { left, right, .. } => {
            collect_lowered_public_relations_in_set_expr(left, out);
            collect_lowered_public_relations_in_set_expr(right, out);
        }
        BroadPublicReadSetExpr::Table { relation, .. } => {
            if let BroadPublicReadRelation::LoweredPublic(binding) = relation {
                out.insert(binding.descriptor.public_name.clone());
            }
        }
        BroadPublicReadSetExpr::Other { .. } => {}
    }
}

fn collect_lowered_public_relations_in_select(
    select: &BroadPublicReadSelect,
    out: &mut BTreeSet<String>,
) {
    for projection in &select.projection {
        collect_lowered_public_relations_in_projection_item(projection, out);
    }
    for table in &select.from {
        collect_lowered_public_relations_in_table_with_joins(table, out);
    }
    if let Some(selection) = &select.selection {
        collect_lowered_public_relations_in_sql_expr(selection, out);
    }
    collect_lowered_public_relations_in_group_by(&select.group_by, out);
    if let Some(having) = &select.having {
        collect_lowered_public_relations_in_sql_expr(having, out);
    }
}

fn collect_lowered_public_relations_in_table_with_joins(
    table: &BroadPublicReadTableWithJoins,
    out: &mut BTreeSet<String>,
) {
    collect_lowered_public_relations_in_table_factor(&table.relation, out);
    for join in &table.joins {
        collect_lowered_public_relations_in_table_factor(&join.relation, out);
        for expr in &join.constraint_expressions {
            collect_lowered_public_relations_in_sql_expr(expr, out);
        }
    }
}

fn collect_lowered_public_relations_in_table_factor(
    factor: &BroadPublicReadTableFactor,
    out: &mut BTreeSet<String>,
) {
    match factor {
        BroadPublicReadTableFactor::Table { relation, .. } => {
            if let BroadPublicReadRelation::LoweredPublic(binding) = relation {
                out.insert(binding.descriptor.public_name.clone());
            }
        }
        BroadPublicReadTableFactor::Derived { subquery, .. } => {
            collect_lowered_public_relations_in_query(subquery, out);
        }
        BroadPublicReadTableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_lowered_public_relations_in_table_with_joins(table_with_joins, out),
        BroadPublicReadTableFactor::Other { .. } => {}
    }
}

fn collect_lowered_public_relations_in_projection_item(
    item: &BroadPublicReadProjectionItem,
    out: &mut BTreeSet<String>,
) {
    match &item.kind {
        BroadPublicReadProjectionItemKind::Expr { nested_queries, .. } => {
            for query in nested_queries {
                collect_lowered_public_relations_in_nested_query_expr(query, out);
            }
        }
        BroadPublicReadProjectionItemKind::Wildcard
        | BroadPublicReadProjectionItemKind::QualifiedWildcard { .. } => {}
    }
}

fn collect_lowered_public_relations_in_group_by(
    group_by: &BroadPublicReadGroupBy,
    out: &mut BTreeSet<String>,
) {
    match &group_by.kind {
        BroadPublicReadGroupByKind::All => {}
        BroadPublicReadGroupByKind::Expressions(expressions) => {
            for expr in expressions {
                collect_lowered_public_relations_in_sql_expr(expr, out);
            }
        }
    }
}

fn collect_lowered_public_relations_in_order_by(
    order_by: &BroadPublicReadOrderBy,
    out: &mut BTreeSet<String>,
) {
    match &order_by.kind {
        BroadPublicReadOrderByKind::All => {}
        BroadPublicReadOrderByKind::Expressions(expressions) => {
            for expr in expressions {
                collect_lowered_public_relations_in_sql_expr(&expr.expr, out);
            }
        }
    }
}

fn collect_lowered_public_relations_in_limit_clause(
    limit_clause: &BroadPublicReadLimitClause,
    out: &mut BTreeSet<String>,
) {
    match &limit_clause.kind {
        BroadPublicReadLimitClauseKind::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if let Some(limit) = limit {
                collect_lowered_public_relations_in_sql_expr(limit, out);
            }
            if let Some(offset) = offset {
                collect_lowered_public_relations_in_sql_expr(offset, out);
            }
            for expr in limit_by {
                collect_lowered_public_relations_in_sql_expr(expr, out);
            }
        }
        BroadPublicReadLimitClauseKind::OffsetCommaLimit { offset, limit } => {
            collect_lowered_public_relations_in_sql_expr(offset, out);
            collect_lowered_public_relations_in_sql_expr(limit, out);
        }
    }
}

fn collect_lowered_public_relations_in_sql_expr(expr: &BroadSqlExpr, out: &mut BTreeSet<String>) {
    for nested_query in &expr.nested_queries {
        collect_lowered_public_relations_in_nested_query_expr(nested_query, out);
    }
}

fn collect_lowered_public_relations_in_nested_query_expr(
    expr: &BroadNestedQueryExpr,
    out: &mut BTreeSet<String>,
) {
    match expr {
        BroadNestedQueryExpr::ScalarSubquery(query) => {
            collect_lowered_public_relations_in_query(query, out);
        }
        BroadNestedQueryExpr::Exists { subquery, .. } => {
            collect_lowered_public_relations_in_query(subquery, out);
        }
        BroadNestedQueryExpr::InSubquery { expr, subquery, .. } => {
            collect_lowered_public_relations_in_sql_expr(expr, out);
            collect_lowered_public_relations_in_query(subquery, out);
        }
    }
}

fn is_direct_only_history_surface(binding: &SurfaceBinding) -> bool {
    binding.descriptor.surface_variant == SurfaceVariant::History
        && matches!(
            binding.descriptor.surface_family,
            SurfaceFamily::State | SurfaceFamily::Entity | SurfaceFamily::Filesystem
        )
}

#[cfg(test)]
mod tests {
    use super::{
        public_read_routing_pass_registry, route_broad_public_read_statement,
        route_public_read_execution_strategy,
    };
    use crate::sql::catalog::SurfaceRegistry;
    use crate::sql::logical_plan::public_ir::{
        BroadNestedQueryExpr, BroadPublicReadProjectionItemKind, BroadPublicReadRelation,
        BroadPublicReadSetExpr, BroadPublicReadStatement, BroadPublicReadTableFactor,
    };
    use crate::sql::parser::parse_sql_statements;

    #[test]
    fn registry_exposes_stable_pass_order() {
        let registry = public_read_routing_pass_registry();
        assert_eq!(registry.name, "public-read");
        assert_eq!(
            registry
                .passes
                .iter()
                .map(|pass| pass.name)
                .collect::<Vec<_>>(),
            vec![
                "public-read.route-execution-strategy",
                "public-read.route-lowerable-relations"
            ]
        );
    }

    #[test]
    fn broad_routing_records_route_trace() {
        let statement = parse_sql_statements("SELECT key FROM lix_key_value")
            .expect("sql should parse")
            .into_iter()
            .next()
            .expect("statement should exist");
        let optimized = route_broad_public_read_statement(
            &statement,
            &SurfaceRegistry::with_builtin_surfaces(),
            crate::SqlDialect::Sqlite,
            Some("main"),
        )
        .expect("broad rewrite should succeed");

        assert_eq!(optimized.pass_traces.len(), 1);
        assert_eq!(
            optimized.pass_traces[0].name,
            "public-read.route-lowerable-relations"
        );
        assert!(optimized.pass_traces[0].enabled);
        assert!(optimized.pass_traces[0]
            .diagnostics
            .iter()
            .any(|line| line.contains("public relations")));
        let BroadPublicReadStatement::Query(query) = &optimized.broad_statement else {
            panic!("routing should keep a broad query statement");
        };
        let BroadPublicReadSetExpr::Select(select) = &query.body else {
            panic!("routing should keep the broad select body");
        };
        let BroadPublicReadTableFactor::Table { relation, .. } = &select.from[0].relation else {
            panic!("expected routing to keep the root as a table factor");
        };
        let BroadPublicReadRelation::LoweredPublic(binding) = relation else {
            panic!("routing should lower the public surface relation");
        };
        assert_eq!(binding.descriptor.public_name, "lix_key_value");
    }

    #[test]
    fn broad_rewrite_keeps_active_version_surfaces_unlowered_without_version_input() {
        let statement = parse_sql_statements("SELECT key FROM lix_key_value")
            .expect("sql should parse")
            .into_iter()
            .next()
            .expect("statement should exist");
        let optimized = route_broad_public_read_statement(
            &statement,
            &SurfaceRegistry::with_builtin_surfaces(),
            crate::SqlDialect::Sqlite,
            None,
        )
        .expect("broad rewrite should still produce a typed result");

        let BroadPublicReadStatement::Query(query) = &optimized.broad_statement else {
            panic!("routing should keep a broad query statement");
        };
        let BroadPublicReadSetExpr::Select(select) = &query.body else {
            panic!("routing should keep the broad select body");
        };
        let BroadPublicReadTableFactor::Table { relation, .. } = &select.from[0].relation else {
            panic!("expected routing to keep the root as a table factor");
        };
        let BroadPublicReadRelation::Public(binding) = relation else {
            panic!("routing should not lower active-version public surfaces without a version");
        };
        assert_eq!(binding.descriptor.public_name, "lix_key_value");
        assert!(!optimized.pass_traces[0].changed);
    }

    #[test]
    fn broad_rewrite_optimizes_nested_scalar_subqueries() {
        let statement = parse_sql_statements(
            "SELECT (SELECT lixcol_change_id FROM lix_directory WHERE id = 'dir-stable-parent')",
        )
        .expect("sql should parse")
        .into_iter()
        .next()
        .expect("statement should exist");
        let optimized = route_broad_public_read_statement(
            &statement,
            &SurfaceRegistry::with_builtin_surfaces(),
            crate::SqlDialect::Sqlite,
            Some("main"),
        )
        .expect("broad rewrite should succeed");

        assert!(optimized.pass_traces[0].changed);
        assert!(optimized.pass_traces[0]
            .diagnostics
            .iter()
            .any(|line| line.contains("lix_directory")));

        let BroadPublicReadStatement::Query(query) = &optimized.broad_statement else {
            panic!("routing should keep a broad query statement");
        };
        let BroadPublicReadSetExpr::Select(select) = &query.body else {
            panic!("routing should keep the broad select body");
        };
        let BroadPublicReadProjectionItemKind::Expr { nested_queries, .. } =
            &select.projection[0].kind
        else {
            panic!("expected a typed expression projection");
        };
        let [BroadNestedQueryExpr::ScalarSubquery(subquery)] = nested_queries.as_slice() else {
            panic!("expected a typed scalar subquery");
        };
        let BroadPublicReadSetExpr::Select(subquery_select) = &subquery.body else {
            panic!("expected nested query to remain a select");
        };
        let BroadPublicReadTableFactor::Table { relation, .. } = &subquery_select.from[0].relation
        else {
            panic!("expected routing to keep the nested root as a table factor");
        };
        let BroadPublicReadRelation::LoweredPublic(binding) = relation else {
            panic!("routing should lower the nested public surface relation");
        };
        assert_eq!(binding.descriptor.public_name, "lix_directory");
    }

    #[test]
    fn direct_history_strategy_records_trace() {
        let binding = SurfaceRegistry::with_builtin_surfaces()
            .bind_relation_name("lix_state_history")
            .expect("builtin history surface should bind");
        let decision = route_public_read_execution_strategy(&binding);

        assert!(decision.direct_execution);
        assert_eq!(decision.pass_traces.len(), 1);
        assert_eq!(
            decision.pass_traces[0].name,
            "public-read.route-execution-strategy"
        );
        assert!(decision.pass_traces[0].changed);
    }
}
