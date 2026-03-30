use crate::sql::catalog::{SurfaceBinding, SurfaceFamily, SurfaceRegistry, SurfaceVariant};
use crate::sql::logical_plan::public_ir::{
    BroadPublicReadJoin, BroadPublicReadQuery, BroadPublicReadRelation, BroadPublicReadSelect,
    BroadPublicReadSetExpr, BroadPublicReadStatement, BroadPublicReadTableFactor,
    BroadPublicReadTableWithJoins, BroadPublicReadWith,
};
use crate::sql::optimizer::registry::{
    run_fallible_pass, run_infallible_pass, OptimizerPassMetadata, OptimizerPassOutcome,
    OptimizerPassRegistry, OptimizerPassSettings, OptimizerPassTrace,
};
#[cfg(test)]
use crate::sql::physical_plan::lowerer::bind_broad_public_read_statement_with_registry;
use crate::sql::physical_plan::lowerer::broad_public_relation_supports_terminal_render;
use crate::{LixError, SqlDialect};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectExecutionStrategyDecision {
    pub(crate) direct_execution: bool,
    pub(crate) pass_traces: Vec<OptimizerPassTrace>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OptimizedBroadPublicRead {
    pub(crate) broad_statement: BroadPublicReadStatement,
    pub(crate) pass_traces: Vec<OptimizerPassTrace>,
}

const DIRECT_HISTORY_STRATEGY_PASS: OptimizerPassMetadata = OptimizerPassMetadata {
    name: "public-read.choose-direct-history-strategy",
    order: 10,
    description: "choose direct history execution for eligible history surfaces",
};

const BROAD_SURFACE_REWRITE_PASS: OptimizerPassMetadata = OptimizerPassMetadata {
    name: "public-read.rewrite-supported-surfaces",
    order: 20,
    description: "rewrite typed broad public surface relations into lowered relations",
};

const PUBLIC_READ_REGISTRY: OptimizerPassRegistry = OptimizerPassRegistry {
    name: "public-read",
    passes: &[DIRECT_HISTORY_STRATEGY_PASS, BROAD_SURFACE_REWRITE_PASS],
};

pub(crate) fn public_read_pass_registry() -> &'static OptimizerPassRegistry {
    &PUBLIC_READ_REGISTRY
}

pub(crate) fn choose_specialized_public_read_strategy(
    binding: &SurfaceBinding,
) -> DirectExecutionStrategyDecision {
    choose_specialized_public_read_strategy_with_settings(
        binding,
        &OptimizerPassSettings::default(),
    )
}

fn choose_specialized_public_read_strategy_with_settings(
    binding: &SurfaceBinding,
    settings: &OptimizerPassSettings,
) -> DirectExecutionStrategyDecision {
    let metadata = public_read_pass_registry().passes[0];
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
        OptimizerPassOutcome {
            changed: direct_execution,
            diagnostics,
        }
    });
    DirectExecutionStrategyDecision {
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
pub(crate) fn optimize_broad_public_read_statement(
    statement: &sqlparser::ast::Statement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
) -> Result<OptimizedBroadPublicRead, LixError> {
    let broad_statement = bind_broad_public_read_statement_with_registry(statement, registry)?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "broad optimizer requires a typed broad public-read statement",
            )
        })?;
    optimize_broad_public_read_statement_with_known_live_layouts(
        &broad_statement,
        registry,
        dialect,
        active_version_id,
        &BTreeMap::new(),
    )
}

pub(crate) fn optimize_broad_public_read_statement_with_known_live_layouts(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<OptimizedBroadPublicRead, LixError> {
    optimize_broad_public_read_statement_with_settings(
        statement,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        &OptimizerPassSettings::default(),
    )
}

fn optimize_broad_public_read_statement_with_settings(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    settings: &OptimizerPassSettings,
) -> Result<OptimizedBroadPublicRead, LixError> {
    let metadata = public_read_pass_registry().passes[1];
    let before_summary = summarize_broad_public_read_statement(statement);
    let mut optimized_broad_statement = statement.clone();
    let trace = run_fallible_pass(metadata, settings, || {
        optimized_broad_statement = optimize_broad_public_read_statement_relations(
            statement,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?;
        let changed = optimized_broad_statement != *statement;
        let lowered_summary = summarize_lowered_public_relations(&optimized_broad_statement);
        Ok::<OptimizerPassOutcome, LixError>(OptimizerPassOutcome {
            changed,
            diagnostics: broad_rewrite_diagnostics(&before_summary, &lowered_summary, changed),
        })
    })?;

    Ok(OptimizedBroadPublicRead {
        broad_statement: optimized_broad_statement,
        pass_traces: vec![trace],
    })
}

fn broad_rewrite_diagnostics(
    summary: &BTreeSet<String>,
    lowered_summary: &BTreeSet<String>,
    changed: bool,
) -> Vec<String> {
    let mut diagnostics = Vec::new();
    if summary.is_empty() {
        diagnostics.push("no typed public relations matched broad optimizer".to_string());
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
            .push("rewrote typed broad public relations into lowered broad relations".to_string());
    } else {
        diagnostics.push("typed broad statement was already lowered or not renderable".to_string());
    }
    diagnostics
}

fn optimize_broad_public_read_statement_relations(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadStatement, LixError> {
    match statement {
        BroadPublicReadStatement::Query(query) => Ok(BroadPublicReadStatement::Query(
            optimize_broad_public_read_query(
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
            statement: Box::new(optimize_broad_public_read_statement_relations(
                statement,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        }),
    }
}

fn optimize_broad_public_read_query(
    query: &BroadPublicReadQuery,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadQuery, LixError> {
    Ok(BroadPublicReadQuery {
        original: query.original.clone(),
        with: query
            .with
            .as_ref()
            .map(|with| {
                optimize_broad_public_read_with(
                    with,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .transpose()?,
        body: optimize_broad_public_read_set_expr(
            &query.body,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
    })
}

fn optimize_broad_public_read_with(
    with: &BroadPublicReadWith,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadWith, LixError> {
    Ok(BroadPublicReadWith {
        original: with.original.clone(),
        cte_tables: with
            .cte_tables
            .iter()
            .map(|query| {
                optimize_broad_public_read_query(
                    query,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .collect::<Result<_, _>>()?,
    })
}

fn optimize_broad_public_read_set_expr(
    expr: &BroadPublicReadSetExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadSetExpr, LixError> {
    match expr {
        BroadPublicReadSetExpr::Select(select) => Ok(BroadPublicReadSetExpr::Select(
            optimize_broad_public_read_select(
                select,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        )),
        BroadPublicReadSetExpr::Query(query) => Ok(BroadPublicReadSetExpr::Query(Box::new(
            optimize_broad_public_read_query(
                query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        ))),
        BroadPublicReadSetExpr::SetOperation {
            original,
            left,
            right,
        } => Ok(BroadPublicReadSetExpr::SetOperation {
            original: original.clone(),
            left: Box::new(optimize_broad_public_read_set_expr(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            right: Box::new(optimize_broad_public_read_set_expr(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        }),
        BroadPublicReadSetExpr::Table { original, relation } => Ok(BroadPublicReadSetExpr::Table {
            original: original.clone(),
            relation: optimize_broad_public_read_relation(
                relation,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        }),
        BroadPublicReadSetExpr::Other(other) => Ok(BroadPublicReadSetExpr::Other(other.clone())),
    }
}

fn optimize_broad_public_read_select(
    select: &BroadPublicReadSelect,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadSelect, LixError> {
    Ok(BroadPublicReadSelect {
        original: select.original.clone(),
        from: select
            .from
            .iter()
            .map(|table| {
                optimize_broad_public_read_table_with_joins(
                    table,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .collect::<Result<_, _>>()?,
    })
}

fn optimize_broad_public_read_table_with_joins(
    table: &BroadPublicReadTableWithJoins,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadTableWithJoins, LixError> {
    Ok(BroadPublicReadTableWithJoins {
        original: table.original.clone(),
        relation: optimize_broad_public_read_table_factor(
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
                optimize_broad_public_read_join(
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

fn optimize_broad_public_read_join(
    join: &BroadPublicReadJoin,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadJoin, LixError> {
    Ok(BroadPublicReadJoin {
        original: join.original.clone(),
        relation: optimize_broad_public_read_table_factor(
            &join.relation,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
    })
}

fn optimize_broad_public_read_table_factor(
    factor: &BroadPublicReadTableFactor,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<BroadPublicReadTableFactor, LixError> {
    match factor {
        BroadPublicReadTableFactor::Table { original, relation } => {
            Ok(BroadPublicReadTableFactor::Table {
                original: original.clone(),
                relation: optimize_broad_public_read_relation(
                    relation,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )?,
            })
        }
        BroadPublicReadTableFactor::Derived { original, subquery } => {
            Ok(BroadPublicReadTableFactor::Derived {
                original: original.clone(),
                subquery: Box::new(optimize_broad_public_read_query(
                    subquery,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )?),
            })
        }
        BroadPublicReadTableFactor::NestedJoin {
            original,
            table_with_joins,
        } => Ok(BroadPublicReadTableFactor::NestedJoin {
            original: original.clone(),
            table_with_joins: Box::new(optimize_broad_public_read_table_with_joins(
                table_with_joins,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        }),
        BroadPublicReadTableFactor::Other(other) => {
            Ok(BroadPublicReadTableFactor::Other(other.clone()))
        }
    }
}

fn optimize_broad_public_read_relation(
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
            collect_public_relations_in_query(cte, out);
        }
    }
    collect_public_relations_in_set_expr(&query.body, out);
}

fn collect_public_relations_in_set_expr(expr: &BroadPublicReadSetExpr, out: &mut BTreeSet<String>) {
    match expr {
        BroadPublicReadSetExpr::Select(select) => {
            for table in &select.from {
                collect_public_relations_in_table_with_joins(table, out);
            }
        }
        BroadPublicReadSetExpr::Query(query) => collect_public_relations_in_query(query, out),
        BroadPublicReadSetExpr::SetOperation { left, right, .. } => {
            collect_public_relations_in_set_expr(left, out);
            collect_public_relations_in_set_expr(right, out);
        }
        BroadPublicReadSetExpr::Table { relation, .. } => {
            collect_public_relation_name(relation, out);
        }
        BroadPublicReadSetExpr::Other(_) => {}
    }
}

fn collect_public_relations_in_table_with_joins(
    table: &BroadPublicReadTableWithJoins,
    out: &mut BTreeSet<String>,
) {
    collect_public_relations_in_table_factor(&table.relation, out);
    for join in &table.joins {
        collect_public_relations_in_table_factor(&join.relation, out);
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
        BroadPublicReadTableFactor::Other(_) => {}
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
            collect_lowered_public_relations_in_query(cte, out);
        }
    }
    collect_lowered_public_relations_in_set_expr(&query.body, out);
}

fn collect_lowered_public_relations_in_set_expr(
    expr: &BroadPublicReadSetExpr,
    out: &mut BTreeSet<String>,
) {
    match expr {
        BroadPublicReadSetExpr::Select(select) => {
            for table in &select.from {
                collect_lowered_public_relations_in_table_with_joins(table, out);
            }
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
        BroadPublicReadSetExpr::Other(_) => {}
    }
}

fn collect_lowered_public_relations_in_table_with_joins(
    table: &BroadPublicReadTableWithJoins,
    out: &mut BTreeSet<String>,
) {
    collect_lowered_public_relations_in_table_factor(&table.relation, out);
    for join in &table.joins {
        collect_lowered_public_relations_in_table_factor(&join.relation, out);
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
        BroadPublicReadTableFactor::Other(_) => {}
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
        choose_specialized_public_read_strategy, optimize_broad_public_read_statement,
        public_read_pass_registry,
    };
    use crate::sql::catalog::SurfaceRegistry;
    use crate::sql::logical_plan::public_ir::{
        BroadPublicReadRelation, BroadPublicReadSetExpr, BroadPublicReadStatement,
        BroadPublicReadTableFactor,
    };
    use crate::sql::parser::parse_sql_statements;

    #[test]
    fn registry_exposes_stable_pass_order() {
        let registry = public_read_pass_registry();
        assert_eq!(registry.name, "public-read");
        assert_eq!(
            registry
                .passes
                .iter()
                .map(|pass| pass.name)
                .collect::<Vec<_>>(),
            vec![
                "public-read.choose-direct-history-strategy",
                "public-read.rewrite-supported-surfaces"
            ]
        );
    }

    #[test]
    fn broad_rewrite_records_optimizer_trace() {
        let statement = parse_sql_statements("SELECT key FROM lix_key_value")
            .expect("sql should parse")
            .into_iter()
            .next()
            .expect("statement should exist");
        let optimized = optimize_broad_public_read_statement(
            &statement,
            &SurfaceRegistry::with_builtin_surfaces(),
            crate::SqlDialect::Sqlite,
            Some("main"),
        )
        .expect("broad rewrite should succeed");

        assert_eq!(optimized.pass_traces.len(), 1);
        assert_eq!(
            optimized.pass_traces[0].name,
            "public-read.rewrite-supported-surfaces"
        );
        assert!(optimized.pass_traces[0].enabled);
        assert!(optimized.pass_traces[0]
            .diagnostics
            .iter()
            .any(|line| line.contains("public relations")));
        let BroadPublicReadStatement::Query(query) = &optimized.broad_statement else {
            panic!("optimizer should keep a broad query statement");
        };
        let BroadPublicReadSetExpr::Select(select) = &query.body else {
            panic!("optimizer should keep the broad select body");
        };
        let BroadPublicReadTableFactor::Table { relation, .. } = &select.from[0].relation else {
            panic!("expected optimizer to keep the root as a table factor");
        };
        let BroadPublicReadRelation::LoweredPublic(binding) = relation else {
            panic!("optimizer should lower the public surface relation");
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
        let optimized = optimize_broad_public_read_statement(
            &statement,
            &SurfaceRegistry::with_builtin_surfaces(),
            crate::SqlDialect::Sqlite,
            None,
        )
        .expect("broad rewrite should still produce a typed result");

        let BroadPublicReadStatement::Query(query) = &optimized.broad_statement else {
            panic!("optimizer should keep a broad query statement");
        };
        let BroadPublicReadSetExpr::Select(select) = &query.body else {
            panic!("optimizer should keep the broad select body");
        };
        let BroadPublicReadTableFactor::Table { relation, .. } = &select.from[0].relation else {
            panic!("expected optimizer to keep the root as a table factor");
        };
        let BroadPublicReadRelation::Public(binding) = relation else {
            panic!("optimizer should not lower active-version public surfaces without a version");
        };
        assert_eq!(binding.descriptor.public_name, "lix_key_value");
        assert!(!optimized.pass_traces[0].changed);
    }

    #[test]
    fn direct_history_strategy_records_trace() {
        let binding = SurfaceRegistry::with_builtin_surfaces()
            .bind_relation_name("lix_state_history")
            .expect("builtin history surface should bind");
        let decision = choose_specialized_public_read_strategy(&binding);

        assert!(decision.direct_execution);
        assert_eq!(decision.pass_traces.len(), 1);
        assert_eq!(
            decision.pass_traces[0].name,
            "public-read.choose-direct-history-strategy"
        );
        assert!(decision.pass_traces[0].changed);
    }
}
