use crate::catalog::{SurfaceBinding, SurfaceFamily, SurfaceRegistry, SurfaceVariant};
use crate::contracts::artifacts::ReadTimeProjectionRead;
#[cfg(test)]
use crate::sql::binder::bind_broad_public_read_statement_with_registry;
use crate::sql::logical_plan::public_ir::{
    BroadPublicReadCte, BroadPublicReadDistinct, BroadPublicReadGroupBy,
    BroadPublicReadGroupByKind, BroadPublicReadJoin, BroadPublicReadJoinConstraint,
    BroadPublicReadJoinKind, BroadPublicReadLimitClause, BroadPublicReadLimitClauseKind,
    BroadPublicReadOffset, BroadPublicReadOrderBy, BroadPublicReadOrderByExpr,
    BroadPublicReadOrderByKind, BroadPublicReadProjectionItem, BroadPublicReadProjectionItemKind,
    BroadPublicReadQuery, BroadPublicReadRelation, BroadPublicReadSelect, BroadPublicReadSetExpr,
    BroadPublicReadStatement, BroadPublicReadTableFactor, BroadPublicReadTableWithJoins,
    BroadPublicReadWith, BroadSqlCaseWhen, BroadSqlExpr, BroadSqlExprKind, BroadSqlFunction,
    BroadSqlFunctionArg, BroadSqlFunctionArgExpr, BroadSqlFunctionArgumentList,
    BroadSqlFunctionArguments,
};
use crate::sql::logical_plan::SurfaceReadPlan;
use crate::sql::physical_plan::lowerer::broad_public_relation_supports_terminal_render;
use crate::sql::physical_plan::try_compile_read_time_projection_read;
use crate::sql::prepare::public_surface::routing::registry::{
    run_fallible_pass, run_infallible_pass, RoutingPassMetadata, RoutingPassOutcome,
    RoutingPassRegistry, RoutingPassSettings, RoutingPassTrace,
};
use crate::{LixError, SqlDialect};
use serde_json::Value as JsonValue;
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

thread_local! {
    static BROAD_ROUTING_DELAY_US_FOR_TEST: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
thread_local! {
    static FORBID_BROAD_ROUTING_FOR_TEST: Cell<bool> = const { Cell::new(false) };
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

#[doc(hidden)]
pub struct BroadRoutingDelayForTestGuard {
    previous_delay_us: u64,
}

impl Drop for BroadRoutingDelayForTestGuard {
    fn drop(&mut self) {
        BROAD_ROUTING_DELAY_US_FOR_TEST.set(self.previous_delay_us);
    }
}

#[doc(hidden)]
pub fn delay_broad_routing_for_test(delay: Duration) -> BroadRoutingDelayForTestGuard {
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

fn apply_broad_routing_delay_for_test() {
    let delay_us = BROAD_ROUTING_DELAY_US_FOR_TEST.get();
    if delay_us > 0 {
        std::thread::sleep(Duration::from_micros(delay_us));
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PublicReadExecutionStrategy {
    DirectHistory,
    DerivedRowset(ReadTimeProjectionRead),
    GeneralProgram,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PublicReadExecutionStrategySelection {
    pub(crate) strategy: PublicReadExecutionStrategy,
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
    surface_read_plan: &SurfaceReadPlan,
) -> PublicReadExecutionStrategySelection {
    route_public_read_execution_strategy_with_settings(
        surface_read_plan,
        &RoutingPassSettings::default(),
    )
}

fn route_public_read_execution_strategy_with_settings(
    surface_read_plan: &SurfaceReadPlan,
    settings: &RoutingPassSettings,
) -> PublicReadExecutionStrategySelection {
    let metadata = public_read_routing_pass_registry().passes[0];
    let binding = &surface_read_plan.structured_read().surface_binding;
    let strategy = if is_direct_only_history_surface(binding) {
        PublicReadExecutionStrategy::DirectHistory
    } else if let Some(rowset_read) = try_compile_read_time_projection_read(surface_read_plan) {
        PublicReadExecutionStrategy::DerivedRowset(rowset_read)
    } else {
        PublicReadExecutionStrategy::GeneralProgram
    };
    let trace = run_infallible_pass(metadata, settings, || {
        let mut diagnostics = vec![format!(
            "surface '{}' family={} variant={}",
            binding.descriptor.public_name,
            surface_family_name(binding.descriptor.surface_family),
            surface_variant_name(binding.descriptor.surface_variant)
        )];
        diagnostics.push(match &strategy {
            PublicReadExecutionStrategy::DirectHistory => {
                "direct history execution strategy selected".to_string()
            }
            PublicReadExecutionStrategy::DerivedRowset(_) => {
                "derived rowset execution strategy selected".to_string()
            }
            PublicReadExecutionStrategy::GeneralProgram => {
                "general lowered program execution strategy selected".to_string()
            }
        });
        RoutingPassOutcome {
            changed: !matches!(strategy, PublicReadExecutionStrategy::GeneralProgram),
            diagnostics,
        }
    });
    PublicReadExecutionStrategySelection {
        strategy,
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
    assert_broad_routing_allowed_for_test();
    apply_broad_routing_delay_for_test();

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
        recursive: with.recursive,
        cte_tables: with
            .cte_tables
            .iter()
            .map(|cte| -> Result<BroadPublicReadCte, LixError> {
                Ok(BroadPublicReadCte {
                    alias: cte.alias.clone(),
                    materialized: cte.materialized.clone(),
                    from: cte.from.clone(),
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
        distinct: select
            .distinct
            .as_ref()
            .map(|distinct| {
                route_broad_public_read_distinct(
                    distinct,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .transpose()?,
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

fn route_broad_public_read_distinct(
    distinct: &BroadPublicReadDistinct,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadDistinct, LixError> {
    match distinct {
        BroadPublicReadDistinct::Distinct => Ok(BroadPublicReadDistinct::Distinct),
        BroadPublicReadDistinct::On(expressions) => Ok(BroadPublicReadDistinct::On(
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
        )),
    }
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
        global: join.global,
        kind: route_broad_public_read_join_kind(
            &join.kind,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
        relation: route_broad_public_read_table_factor(
            &join.relation,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
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
        BroadPublicReadProjectionItemKind::Expr { alias, expr } => {
            BroadPublicReadProjectionItemKind::Expr {
                alias: alias.clone(),
                expr: route_broad_sql_expr(
                    expr,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )?,
            }
        }
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
                .map(|offset| {
                    route_broad_sql_expr(
                        &offset.value,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                    .map(|value| BroadPublicReadOffset {
                        value,
                        rows: offset.rows,
                    })
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
        kind: route_broad_sql_expr_kind(
            &expr.kind,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
    })
}

fn route_broad_sql_expr_kind(
    kind: &BroadSqlExprKind,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadSqlExprKind, LixError> {
    Ok(match kind {
        BroadSqlExprKind::Identifier(ident) => BroadSqlExprKind::Identifier(ident.clone()),
        BroadSqlExprKind::CompoundIdentifier(parts) => {
            BroadSqlExprKind::CompoundIdentifier(parts.clone())
        }
        BroadSqlExprKind::Value(value) => BroadSqlExprKind::Value(value.clone()),
        BroadSqlExprKind::TypedString {
            data_type,
            value,
            uses_odbc_syntax,
        } => BroadSqlExprKind::TypedString {
            data_type: data_type.clone(),
            value: value.clone(),
            uses_odbc_syntax: *uses_odbc_syntax,
        },
        BroadSqlExprKind::BinaryOp { left, op, right } => BroadSqlExprKind::BinaryOp {
            left: Box::new(route_broad_sql_expr(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            op: op.clone(),
            right: Box::new(route_broad_sql_expr(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        },
        BroadSqlExprKind::AnyOp {
            left,
            compare_op,
            right,
            is_some,
        } => BroadSqlExprKind::AnyOp {
            left: Box::new(route_broad_sql_expr(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            compare_op: compare_op.clone(),
            right: Box::new(route_broad_sql_expr(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            is_some: *is_some,
        },
        BroadSqlExprKind::AllOp {
            left,
            compare_op,
            right,
        } => BroadSqlExprKind::AllOp {
            left: Box::new(route_broad_sql_expr(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            compare_op: compare_op.clone(),
            right: Box::new(route_broad_sql_expr(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        },
        BroadSqlExprKind::UnaryOp { op, expr } => BroadSqlExprKind::UnaryOp {
            op: *op,
            expr: Box::new(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        },
        BroadSqlExprKind::Nested(expr) => BroadSqlExprKind::Nested(Box::new(route_broad_sql_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?)),
        BroadSqlExprKind::IsNull(expr) => BroadSqlExprKind::IsNull(Box::new(route_broad_sql_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?)),
        BroadSqlExprKind::IsNotNull(expr) => {
            BroadSqlExprKind::IsNotNull(Box::new(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?))
        }
        BroadSqlExprKind::IsTrue(expr) => BroadSqlExprKind::IsTrue(Box::new(route_broad_sql_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?)),
        BroadSqlExprKind::IsNotTrue(expr) => {
            BroadSqlExprKind::IsNotTrue(Box::new(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?))
        }
        BroadSqlExprKind::IsFalse(expr) => {
            BroadSqlExprKind::IsFalse(Box::new(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?))
        }
        BroadSqlExprKind::IsNotFalse(expr) => {
            BroadSqlExprKind::IsNotFalse(Box::new(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?))
        }
        BroadSqlExprKind::IsUnknown(expr) => {
            BroadSqlExprKind::IsUnknown(Box::new(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?))
        }
        BroadSqlExprKind::IsNotUnknown(expr) => {
            BroadSqlExprKind::IsNotUnknown(Box::new(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?))
        }
        BroadSqlExprKind::IsDistinctFrom { left, right } => BroadSqlExprKind::IsDistinctFrom {
            left: Box::new(route_broad_sql_expr(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            right: Box::new(route_broad_sql_expr(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        },
        BroadSqlExprKind::IsNotDistinctFrom { left, right } => {
            BroadSqlExprKind::IsNotDistinctFrom {
                left: Box::new(route_broad_sql_expr(
                    left,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )?),
                right: Box::new(route_broad_sql_expr(
                    right,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )?),
            }
        }
        BroadSqlExprKind::Cast {
            kind,
            expr,
            data_type,
            format,
        } => BroadSqlExprKind::Cast {
            kind: kind.clone(),
            expr: Box::new(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            data_type: data_type.clone(),
            format: format.clone(),
        },
        BroadSqlExprKind::InList {
            expr,
            list,
            negated,
        } => BroadSqlExprKind::InList {
            expr: Box::new(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            list: list
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
            negated: *negated,
        },
        BroadSqlExprKind::InSubquery {
            expr,
            subquery,
            negated,
        } => BroadSqlExprKind::InSubquery {
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
            negated: *negated,
        },
        BroadSqlExprKind::InUnnest {
            expr,
            array_expr,
            negated,
        } => BroadSqlExprKind::InUnnest {
            expr: Box::new(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            array_expr: Box::new(route_broad_sql_expr(
                array_expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            negated: *negated,
        },
        BroadSqlExprKind::Between {
            expr,
            negated,
            low,
            high,
        } => BroadSqlExprKind::Between {
            expr: Box::new(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            negated: *negated,
            low: Box::new(route_broad_sql_expr(
                low,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            high: Box::new(route_broad_sql_expr(
                high,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        },
        BroadSqlExprKind::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => BroadSqlExprKind::Like {
            negated: *negated,
            any: *any,
            expr: Box::new(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            pattern: Box::new(route_broad_sql_expr(
                pattern,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            escape_char: escape_char.clone(),
        },
        BroadSqlExprKind::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => BroadSqlExprKind::ILike {
            negated: *negated,
            any: *any,
            expr: Box::new(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            pattern: Box::new(route_broad_sql_expr(
                pattern,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            escape_char: escape_char.clone(),
        },
        BroadSqlExprKind::Function(function) => {
            BroadSqlExprKind::Function(route_broad_sql_function(
                function,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadSqlExprKind::Case {
            operand,
            conditions,
            else_result,
        } => BroadSqlExprKind::Case {
            operand: operand
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
                .transpose()?
                .map(Box::new),
            conditions: conditions
                .iter()
                .map(|when| {
                    Ok(BroadSqlCaseWhen {
                        condition: route_broad_sql_expr(
                            &when.condition,
                            registry,
                            dialect,
                            active_version_id,
                            known_live_layouts,
                        )?,
                        result: route_broad_sql_expr(
                            &when.result,
                            registry,
                            dialect,
                            active_version_id,
                            known_live_layouts,
                        )?,
                    })
                })
                .collect::<Result<_, LixError>>()?,
            else_result: else_result
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
                .transpose()?
                .map(Box::new),
        },
        BroadSqlExprKind::Exists { negated, subquery } => BroadSqlExprKind::Exists {
            negated: *negated,
            subquery: Box::new(route_broad_public_read_query(
                subquery,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        },
        BroadSqlExprKind::ScalarSubquery(query) => {
            BroadSqlExprKind::ScalarSubquery(Box::new(route_broad_public_read_query(
                query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?))
        }
        BroadSqlExprKind::Tuple(items) => BroadSqlExprKind::Tuple(
            items
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
        ),
        BroadSqlExprKind::Unsupported { diagnostics_sql } => BroadSqlExprKind::Unsupported {
            diagnostics_sql: diagnostics_sql.clone(),
        },
    })
}

fn route_broad_sql_function(
    function: &BroadSqlFunction,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadSqlFunction, LixError> {
    Ok(BroadSqlFunction {
        name: function.name.clone(),
        uses_odbc_syntax: function.uses_odbc_syntax,
        parameters: route_broad_sql_function_arguments(
            &function.parameters,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
        args: route_broad_sql_function_arguments(
            &function.args,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
        filter: function
            .filter
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
            .transpose()?
            .map(Box::new),
        null_treatment: function.null_treatment,
        within_group: function
            .within_group
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
    })
}

fn route_broad_sql_function_arguments(
    arguments: &BroadSqlFunctionArguments,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadSqlFunctionArguments, LixError> {
    match arguments {
        BroadSqlFunctionArguments::None => Ok(BroadSqlFunctionArguments::None),
        BroadSqlFunctionArguments::Subquery(query) => Ok(BroadSqlFunctionArguments::Subquery(
            Box::new(route_broad_public_read_query(
                query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        )),
        BroadSqlFunctionArguments::List(list) => Ok(BroadSqlFunctionArguments::List(
            BroadSqlFunctionArgumentList {
                duplicate_treatment: list.duplicate_treatment,
                args: list
                    .args
                    .iter()
                    .map(|arg| {
                        route_broad_sql_function_arg(
                            arg,
                            registry,
                            dialect,
                            active_version_id,
                            known_live_layouts,
                        )
                    })
                    .collect::<Result<_, _>>()?,
            },
        )),
    }
}

fn route_broad_sql_function_arg(
    arg: &BroadSqlFunctionArg,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadSqlFunctionArg, LixError> {
    match arg {
        BroadSqlFunctionArg::Named {
            name,
            arg,
            operator,
        } => Ok(BroadSqlFunctionArg::Named {
            name: name.clone(),
            arg: route_broad_sql_function_arg_expr(
                arg,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
            operator: operator.clone(),
        }),
        BroadSqlFunctionArg::ExprNamed {
            name,
            arg,
            operator,
        } => Ok(BroadSqlFunctionArg::ExprNamed {
            name: route_broad_sql_expr(
                name,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
            arg: route_broad_sql_function_arg_expr(
                arg,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
            operator: operator.clone(),
        }),
        BroadSqlFunctionArg::Unnamed(arg) => Ok(BroadSqlFunctionArg::Unnamed(
            route_broad_sql_function_arg_expr(
                arg,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
    }
}

fn route_broad_sql_function_arg_expr(
    arg: &BroadSqlFunctionArgExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadSqlFunctionArgExpr, LixError> {
    match arg {
        BroadSqlFunctionArgExpr::Expr(expr) => {
            Ok(BroadSqlFunctionArgExpr::Expr(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?))
        }
        BroadSqlFunctionArgExpr::QualifiedWildcard(object_name) => Ok(
            BroadSqlFunctionArgExpr::QualifiedWildcard(object_name.clone()),
        ),
        BroadSqlFunctionArgExpr::Wildcard => Ok(BroadSqlFunctionArgExpr::Wildcard),
    }
}

fn route_broad_public_read_join_kind(
    kind: &BroadPublicReadJoinKind,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadJoinKind, LixError> {
    match kind {
        BroadPublicReadJoinKind::Join(constraint) => Ok(BroadPublicReadJoinKind::Join(
            route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadJoinKind::Inner(constraint) => Ok(BroadPublicReadJoinKind::Inner(
            route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadJoinKind::Left(constraint) => Ok(BroadPublicReadJoinKind::Left(
            route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadJoinKind::LeftOuter(constraint) => Ok(BroadPublicReadJoinKind::LeftOuter(
            route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadJoinKind::Right(constraint) => Ok(BroadPublicReadJoinKind::Right(
            route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadJoinKind::RightOuter(constraint) => Ok(BroadPublicReadJoinKind::RightOuter(
            route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadJoinKind::FullOuter(constraint) => Ok(BroadPublicReadJoinKind::FullOuter(
            route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadJoinKind::CrossJoin(constraint) => Ok(BroadPublicReadJoinKind::CrossJoin(
            route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadJoinKind::Semi(constraint) => Ok(BroadPublicReadJoinKind::Semi(
            route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadJoinKind::LeftSemi(constraint) => Ok(BroadPublicReadJoinKind::LeftSemi(
            route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadJoinKind::RightSemi(constraint) => Ok(BroadPublicReadJoinKind::RightSemi(
            route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadJoinKind::Anti(constraint) => Ok(BroadPublicReadJoinKind::Anti(
            route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadJoinKind::LeftAnti(constraint) => Ok(BroadPublicReadJoinKind::LeftAnti(
            route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadJoinKind::RightAnti(constraint) => Ok(BroadPublicReadJoinKind::RightAnti(
            route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadJoinKind::StraightJoin(constraint) => Ok(
            BroadPublicReadJoinKind::StraightJoin(route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        ),
        BroadPublicReadJoinKind::CrossApply => Ok(BroadPublicReadJoinKind::CrossApply),
        BroadPublicReadJoinKind::OuterApply => Ok(BroadPublicReadJoinKind::OuterApply),
        BroadPublicReadJoinKind::AsOf {
            match_condition,
            constraint,
        } => Ok(BroadPublicReadJoinKind::AsOf {
            match_condition: route_broad_sql_expr(
                match_condition,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
            constraint: route_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        }),
    }
}

fn route_broad_public_read_join_constraint(
    constraint: &BroadPublicReadJoinConstraint,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadJoinConstraint, LixError> {
    match constraint {
        BroadPublicReadJoinConstraint::None => Ok(BroadPublicReadJoinConstraint::None),
        BroadPublicReadJoinConstraint::Natural => Ok(BroadPublicReadJoinConstraint::Natural),
        BroadPublicReadJoinConstraint::Using(columns) => {
            Ok(BroadPublicReadJoinConstraint::Using(columns.clone()))
        }
        BroadPublicReadJoinConstraint::On(expr) => {
            Ok(BroadPublicReadJoinConstraint::On(route_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?))
        }
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
            lateral,
            alias,
            subquery,
        } => Ok(BroadPublicReadTableFactor::Derived {
            provenance: provenance.clone(),
            lateral: *lateral,
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
    collect_relation_names(statement, out, |relation, out| match relation {
        BroadPublicReadRelation::Public(binding)
        | BroadPublicReadRelation::LoweredPublic(binding) => {
            out.insert(binding.descriptor.public_name.clone());
        }
        BroadPublicReadRelation::Internal(_)
        | BroadPublicReadRelation::External(_)
        | BroadPublicReadRelation::Cte(_) => {}
    });
}

fn collect_lowered_public_relations(
    statement: &BroadPublicReadStatement,
    out: &mut BTreeSet<String>,
) {
    collect_relation_names(statement, out, |relation, out| {
        if let BroadPublicReadRelation::LoweredPublic(binding) = relation {
            out.insert(binding.descriptor.public_name.clone());
        }
    });
}

fn collect_relation_names<F>(
    statement: &BroadPublicReadStatement,
    out: &mut BTreeSet<String>,
    mut visit_relation: F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    collect_relation_names_in_statement(statement, out, &mut visit_relation);
}

fn collect_relation_names_in_statement<F>(
    statement: &BroadPublicReadStatement,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    match statement {
        BroadPublicReadStatement::Query(query) => {
            collect_relation_names_in_query(query, out, visit_relation)
        }
        BroadPublicReadStatement::Explain { statement, .. } => {
            collect_relation_names_in_statement(statement, out, visit_relation)
        }
    }
}

fn collect_relation_names_in_query<F>(
    query: &BroadPublicReadQuery,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_relation_names_in_query(&cte.query, out, visit_relation);
        }
    }
    collect_relation_names_in_set_expr(&query.body, out, visit_relation);
    if let Some(order_by) = &query.order_by {
        collect_relation_names_in_order_by(order_by, out, visit_relation);
    }
    if let Some(limit_clause) = &query.limit_clause {
        collect_relation_names_in_limit_clause(limit_clause, out, visit_relation);
    }
}

fn collect_relation_names_in_set_expr<F>(
    expr: &BroadPublicReadSetExpr,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    match expr {
        BroadPublicReadSetExpr::Select(select) => {
            collect_relation_names_in_select(select, out, visit_relation)
        }
        BroadPublicReadSetExpr::Query(query) => {
            collect_relation_names_in_query(query, out, visit_relation)
        }
        BroadPublicReadSetExpr::SetOperation { left, right, .. } => {
            collect_relation_names_in_set_expr(left, out, visit_relation);
            collect_relation_names_in_set_expr(right, out, visit_relation);
        }
        BroadPublicReadSetExpr::Table { relation, .. } => visit_relation(relation, out),
    }
}

fn collect_relation_names_in_select<F>(
    select: &BroadPublicReadSelect,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    if let Some(distinct) = &select.distinct {
        collect_relation_names_in_distinct(distinct, out, visit_relation);
    }
    for projection in &select.projection {
        collect_relation_names_in_projection_item(projection, out, visit_relation);
    }
    for table in &select.from {
        collect_relation_names_in_table_with_joins(table, out, visit_relation);
    }
    if let Some(selection) = &select.selection {
        collect_relation_names_in_sql_expr(selection, out, visit_relation);
    }
    collect_relation_names_in_group_by(&select.group_by, out, visit_relation);
    if let Some(having) = &select.having {
        collect_relation_names_in_sql_expr(having, out, visit_relation);
    }
}

fn collect_relation_names_in_distinct<F>(
    distinct: &BroadPublicReadDistinct,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    if let BroadPublicReadDistinct::On(expressions) = distinct {
        for expr in expressions {
            collect_relation_names_in_sql_expr(expr, out, visit_relation);
        }
    }
}

fn collect_relation_names_in_table_with_joins<F>(
    table: &BroadPublicReadTableWithJoins,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    collect_relation_names_in_table_factor(&table.relation, out, visit_relation);
    for join in &table.joins {
        collect_relation_names_in_join(join, out, visit_relation);
    }
}

fn collect_relation_names_in_join<F>(
    join: &BroadPublicReadJoin,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    collect_relation_names_in_table_factor(&join.relation, out, visit_relation);
    collect_relation_names_in_join_kind(&join.kind, out, visit_relation);
}

fn collect_relation_names_in_join_kind<F>(
    kind: &BroadPublicReadJoinKind,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
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
            collect_relation_names_in_join_constraint(constraint, out, visit_relation);
        }
        BroadPublicReadJoinKind::CrossApply | BroadPublicReadJoinKind::OuterApply => {}
        BroadPublicReadJoinKind::AsOf {
            match_condition,
            constraint,
        } => {
            collect_relation_names_in_sql_expr(match_condition, out, visit_relation);
            collect_relation_names_in_join_constraint(constraint, out, visit_relation);
        }
    }
}

fn collect_relation_names_in_join_constraint<F>(
    constraint: &BroadPublicReadJoinConstraint,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    if let BroadPublicReadJoinConstraint::On(expr) = constraint {
        collect_relation_names_in_sql_expr(expr, out, visit_relation);
    }
}

fn collect_relation_names_in_table_factor<F>(
    factor: &BroadPublicReadTableFactor,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    match factor {
        BroadPublicReadTableFactor::Table { relation, .. } => visit_relation(relation, out),
        BroadPublicReadTableFactor::Derived { subquery, .. } => {
            collect_relation_names_in_query(subquery, out, visit_relation);
        }
        BroadPublicReadTableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_relation_names_in_table_with_joins(table_with_joins, out, visit_relation),
    }
}

fn collect_relation_names_in_projection_item<F>(
    item: &BroadPublicReadProjectionItem,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    if let BroadPublicReadProjectionItemKind::Expr { expr, .. } = &item.kind {
        collect_relation_names_in_sql_expr(expr, out, visit_relation);
    }
}

fn collect_relation_names_in_group_by<F>(
    group_by: &BroadPublicReadGroupBy,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    if let BroadPublicReadGroupByKind::Expressions(expressions) = &group_by.kind {
        for expr in expressions {
            collect_relation_names_in_sql_expr(expr, out, visit_relation);
        }
    }
}

fn collect_relation_names_in_order_by<F>(
    order_by: &BroadPublicReadOrderBy,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    if let BroadPublicReadOrderByKind::Expressions(expressions) = &order_by.kind {
        for expr in expressions {
            collect_relation_names_in_sql_expr(&expr.expr, out, visit_relation);
        }
    }
}

fn collect_relation_names_in_limit_clause<F>(
    limit_clause: &BroadPublicReadLimitClause,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    match &limit_clause.kind {
        BroadPublicReadLimitClauseKind::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if let Some(limit) = limit {
                collect_relation_names_in_sql_expr(limit, out, visit_relation);
            }
            if let Some(offset) = offset {
                collect_relation_names_in_sql_expr(&offset.value, out, visit_relation);
            }
            for expr in limit_by {
                collect_relation_names_in_sql_expr(expr, out, visit_relation);
            }
        }
        BroadPublicReadLimitClauseKind::OffsetCommaLimit { offset, limit } => {
            collect_relation_names_in_sql_expr(offset, out, visit_relation);
            collect_relation_names_in_sql_expr(limit, out, visit_relation);
        }
    }
}

fn collect_relation_names_in_sql_expr<F>(
    expr: &BroadSqlExpr,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    match &expr.kind {
        BroadSqlExprKind::Identifier(_)
        | BroadSqlExprKind::CompoundIdentifier(_)
        | BroadSqlExprKind::Value(_)
        | BroadSqlExprKind::TypedString { .. }
        | BroadSqlExprKind::Unsupported { .. } => {}
        BroadSqlExprKind::BinaryOp { left, right, .. }
        | BroadSqlExprKind::IsDistinctFrom { left, right }
        | BroadSqlExprKind::IsNotDistinctFrom { left, right } => {
            collect_relation_names_in_sql_expr(left, out, visit_relation);
            collect_relation_names_in_sql_expr(right, out, visit_relation);
        }
        BroadSqlExprKind::AnyOp { left, right, .. }
        | BroadSqlExprKind::AllOp { left, right, .. } => {
            collect_relation_names_in_sql_expr(left, out, visit_relation);
            collect_relation_names_in_sql_expr(right, out, visit_relation);
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
        | BroadSqlExprKind::IsNotUnknown(expr) => {
            collect_relation_names_in_sql_expr(expr, out, visit_relation);
        }
        BroadSqlExprKind::Cast { expr, .. } => {
            collect_relation_names_in_sql_expr(expr, out, visit_relation);
        }
        BroadSqlExprKind::InList { expr, list, .. } => {
            collect_relation_names_in_sql_expr(expr, out, visit_relation);
            for item in list {
                collect_relation_names_in_sql_expr(item, out, visit_relation);
            }
        }
        BroadSqlExprKind::InSubquery { expr, subquery, .. } => {
            collect_relation_names_in_sql_expr(expr, out, visit_relation);
            collect_relation_names_in_query(subquery, out, visit_relation);
        }
        BroadSqlExprKind::InUnnest {
            expr, array_expr, ..
        } => {
            collect_relation_names_in_sql_expr(expr, out, visit_relation);
            collect_relation_names_in_sql_expr(array_expr, out, visit_relation);
        }
        BroadSqlExprKind::Between {
            expr, low, high, ..
        } => {
            collect_relation_names_in_sql_expr(expr, out, visit_relation);
            collect_relation_names_in_sql_expr(low, out, visit_relation);
            collect_relation_names_in_sql_expr(high, out, visit_relation);
        }
        BroadSqlExprKind::Like { expr, pattern, .. }
        | BroadSqlExprKind::ILike { expr, pattern, .. } => {
            collect_relation_names_in_sql_expr(expr, out, visit_relation);
            collect_relation_names_in_sql_expr(pattern, out, visit_relation);
        }
        BroadSqlExprKind::Function(function) => {
            collect_relation_names_in_sql_function(function, out, visit_relation);
        }
        BroadSqlExprKind::Case {
            operand,
            conditions,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_relation_names_in_sql_expr(operand, out, visit_relation);
            }
            for when in conditions {
                collect_relation_names_in_sql_expr(&when.condition, out, visit_relation);
                collect_relation_names_in_sql_expr(&when.result, out, visit_relation);
            }
            if let Some(else_result) = else_result {
                collect_relation_names_in_sql_expr(else_result, out, visit_relation);
            }
        }
        BroadSqlExprKind::Exists { subquery, .. } | BroadSqlExprKind::ScalarSubquery(subquery) => {
            collect_relation_names_in_query(subquery, out, visit_relation);
        }
        BroadSqlExprKind::Tuple(items) => {
            for item in items {
                collect_relation_names_in_sql_expr(item, out, visit_relation);
            }
        }
    }
}

fn collect_relation_names_in_sql_function<F>(
    function: &BroadSqlFunction,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    collect_relation_names_in_sql_function_arguments(&function.parameters, out, visit_relation);
    collect_relation_names_in_sql_function_arguments(&function.args, out, visit_relation);
    if let Some(filter) = &function.filter {
        collect_relation_names_in_sql_expr(filter, out, visit_relation);
    }
    for expr in &function.within_group {
        collect_relation_names_in_sql_expr(&expr.expr, out, visit_relation);
    }
}

fn collect_relation_names_in_sql_function_arguments<F>(
    arguments: &BroadSqlFunctionArguments,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    match arguments {
        BroadSqlFunctionArguments::None => {}
        BroadSqlFunctionArguments::Subquery(query) => {
            collect_relation_names_in_query(query, out, visit_relation);
        }
        BroadSqlFunctionArguments::List(list) => {
            for arg in &list.args {
                collect_relation_names_in_sql_function_arg(arg, out, visit_relation);
            }
        }
    }
}

fn collect_relation_names_in_sql_function_arg<F>(
    arg: &BroadSqlFunctionArg,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    match arg {
        BroadSqlFunctionArg::Named { arg, .. } => {
            collect_relation_names_in_sql_function_arg_expr(arg, out, visit_relation);
        }
        BroadSqlFunctionArg::ExprNamed { name, arg, .. } => {
            collect_relation_names_in_sql_expr(name, out, visit_relation);
            collect_relation_names_in_sql_function_arg_expr(arg, out, visit_relation);
        }
        BroadSqlFunctionArg::Unnamed(arg) => {
            collect_relation_names_in_sql_function_arg_expr(arg, out, visit_relation);
        }
    }
}

fn collect_relation_names_in_sql_function_arg_expr<F>(
    arg: &BroadSqlFunctionArgExpr,
    out: &mut BTreeSet<String>,
    visit_relation: &mut F,
) where
    F: FnMut(&BroadPublicReadRelation, &mut BTreeSet<String>),
{
    if let BroadSqlFunctionArgExpr::Expr(expr) = arg {
        collect_relation_names_in_sql_expr(expr, out, visit_relation);
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
        route_public_read_execution_strategy, PublicReadExecutionStrategy,
    };
    use crate::sql::logical_plan::public_ir::{
        BroadPublicReadProjectionItemKind, BroadPublicReadRelation, BroadPublicReadSetExpr,
        BroadPublicReadStatement, BroadPublicReadTableFactor, BroadSqlExprKind,
    };
    use crate::sql::logical_plan::{
        public_ir::{
            CanonicalAdminScan, CanonicalFilesystemScan, CanonicalStateScan,
            NormalizedPublicReadQuery, ReadCommand, ReadContract, ReadPlan, StructuredPublicRead,
        },
        SurfaceReadPlan,
    };
    use crate::sql::parser::parse_sql_statements;
    use sqlparser::ast::{Expr, GroupByExpr, Ident, SelectItem};

    fn surface_read_plan_for(surface_name: &str, projection: Vec<SelectItem>) -> SurfaceReadPlan {
        let binding = crate::catalog::build_builtin_surface_registry()
            .bind_relation_name(surface_name)
            .expect("builtin surface should bind");
        let root = if let Some(scan) = CanonicalStateScan::from_surface_binding(binding.clone()) {
            ReadPlan::scan(scan)
        } else if let Some(scan) = CanonicalAdminScan::from_surface_binding(binding.clone()) {
            ReadPlan::admin_scan(scan)
        } else if let Some(scan) = CanonicalFilesystemScan::from_surface_binding(binding.clone()) {
            ReadPlan::filesystem_scan(scan)
        } else {
            panic!("test helper only supports state/admin/filesystem surfaces");
        };

        SurfaceReadPlan {
            read: StructuredPublicRead {
                bound_parameters: Vec::new(),
                requested_version_id: Some("main".to_string()),
                surface_binding: binding,
                read_command: ReadCommand {
                    root,
                    contract: ReadContract::CommittedAtStart,
                    requested_commit_mapping: None,
                },
                query: NormalizedPublicReadQuery {
                    source_alias: None,
                    projection,
                    selection: None,
                    selection_predicates: Vec::new(),
                    group_by: GroupByExpr::Expressions(vec![], vec![]),
                    having: None,
                    order_by: None,
                    limit_clause: None,
                },
            },
            dependency_spec: None,
            effective_state_request: None,
            effective_state_plan: None,
        }
    }

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
            &crate::catalog::build_builtin_surface_registry(),
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
            &crate::catalog::build_builtin_surface_registry(),
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
            &crate::catalog::build_builtin_surface_registry(),
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
        let BroadPublicReadProjectionItemKind::Expr { expr, .. } = &select.projection[0].kind
        else {
            panic!("expected a typed expression projection");
        };
        let BroadSqlExprKind::ScalarSubquery(subquery) = &expr.kind else {
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
        let plan = surface_read_plan_for(
            "lix_state_history",
            vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(
                "entity_id",
            )))],
        );
        let decision = route_public_read_execution_strategy(&plan);

        assert!(matches!(
            decision.strategy,
            PublicReadExecutionStrategy::DirectHistory
        ));
        assert_eq!(decision.pass_traces.len(), 1);
        assert_eq!(
            decision.pass_traces[0].name,
            "public-read.route-execution-strategy"
        );
        assert!(decision.pass_traces[0].changed);
    }

    #[test]
    fn derived_rowset_strategy_records_trace() {
        let plan = surface_read_plan_for(
            "lix_version",
            vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("id")))],
        );
        let decision = route_public_read_execution_strategy(&plan);

        assert!(matches!(
            decision.strategy,
            PublicReadExecutionStrategy::DerivedRowset(_)
        ));
        assert_eq!(
            decision.pass_traces[0].name,
            "public-read.route-execution-strategy"
        );
        assert!(decision.pass_traces[0].changed);
    }

    #[test]
    fn general_program_strategy_records_trace() {
        let plan = surface_read_plan_for(
            "lix_version",
            vec![SelectItem::Wildcard(Default::default())],
        );
        let decision = route_public_read_execution_strategy(&plan);

        assert!(matches!(
            decision.strategy,
            PublicReadExecutionStrategy::GeneralProgram
        ));
        assert_eq!(
            decision.pass_traces[0].name,
            "public-read.route-execution-strategy"
        );
        assert!(!decision.pass_traces[0].changed);
    }

    #[test]
    fn non_declared_surface_does_not_route_to_derived_rowset() {
        let plan = surface_read_plan_for(
            "lix_state",
            vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(
                "entity_id",
            )))],
        );
        let decision = route_public_read_execution_strategy(&plan);

        assert!(matches!(
            decision.strategy,
            PublicReadExecutionStrategy::GeneralProgram
        ));
    }
}
