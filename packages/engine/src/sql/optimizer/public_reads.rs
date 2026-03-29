use crate::sql::catalog::{SurfaceBinding, SurfaceFamily, SurfaceRegistry, SurfaceVariant};
use crate::sql::optimizer::registry::{
    run_fallible_pass, run_infallible_pass, OptimizerPassMetadata, OptimizerPassOutcome,
    OptimizerPassRegistry, OptimizerPassSettings, OptimizerPassTrace,
};
use crate::sql::physical_plan::lowerer::{
    rewrite_supported_public_read_surfaces_in_statement_with_registry_and_active_version_id_and_layouts,
    summarize_bound_public_read_statement_with_registry, BroadPublicRelationSummary,
};
use crate::sql::physical_plan::TerminalRelationRenderNode;
use crate::{LixError, SqlDialect};
use serde_json::Value as JsonValue;
use sqlparser::ast::Statement;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectExecutionStrategyDecision {
    pub(crate) direct_execution: bool,
    pub(crate) pass_traces: Vec<OptimizerPassTrace>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OptimizedPublicReadStatement {
    pub(crate) shell_statement: Statement,
    pub(crate) relation_render_nodes: Vec<TerminalRelationRenderNode>,
    pub(crate) pass_traces: Vec<OptimizerPassTrace>,
}

const DIRECT_HISTORY_STRATEGY_PASS: OptimizerPassMetadata = OptimizerPassMetadata {
    name: "public-read.choose-direct-history-strategy",
    order: 10,
    description:
        "choose direct history execution only for eligible history surfaces outside EXPLAIN",
};

const BROAD_SURFACE_REWRITE_PASS: OptimizerPassMetadata = OptimizerPassMetadata {
    name: "public-read.rewrite-supported-surfaces",
    order: 20,
    description: "rewrite broad public surface references into derived lowered relations",
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
    explain_requested: bool,
) -> DirectExecutionStrategyDecision {
    choose_specialized_public_read_strategy_with_settings(
        binding,
        explain_requested,
        &OptimizerPassSettings::default(),
    )
}

fn choose_specialized_public_read_strategy_with_settings(
    binding: &SurfaceBinding,
    explain_requested: bool,
    settings: &OptimizerPassSettings,
) -> DirectExecutionStrategyDecision {
    let metadata = public_read_pass_registry().passes[0];
    let direct_execution = is_direct_only_history_surface(binding) && !explain_requested;
    let trace = run_infallible_pass(metadata, settings, || {
        let mut diagnostics = vec![format!(
            "surface '{}' family={:?} variant={:?}",
            binding.descriptor.public_name,
            binding.descriptor.surface_family,
            binding.descriptor.surface_variant
        )];
        if explain_requested {
            diagnostics.push("EXPLAIN requested; forcing lowered SQL strategy".to_string());
        } else if direct_execution {
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

#[cfg(test)]
pub(crate) fn optimize_broad_public_read_statement(
    statement: &Statement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
) -> Result<OptimizedPublicReadStatement, LixError> {
    optimize_broad_public_read_statement_with_known_live_layouts(
        statement,
        registry,
        dialect,
        active_version_id,
        &BTreeMap::new(),
    )
}

pub(crate) fn optimize_broad_public_read_statement_with_known_live_layouts(
    statement: &Statement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<OptimizedPublicReadStatement, LixError> {
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
    statement: &Statement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    settings: &OptimizerPassSettings,
) -> Result<OptimizedPublicReadStatement, LixError> {
    let metadata = public_read_pass_registry().passes[1];
    let before_summary = summarize_bound_public_read_statement_with_registry(statement, registry)?;
    let mut rewritten_statement = statement.clone();
    let mut relation_render_nodes = Vec::new();
    let trace = run_fallible_pass(metadata, settings, || {
        let rewritten = rewrite_supported_public_read_surfaces_in_statement_with_registry_and_active_version_id_and_layouts(
            &rewritten_statement,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?;
        rewritten_statement = rewritten.shell_statement;
        relation_render_nodes = rewritten.relation_render_nodes;
        let changed = rewritten_statement != *statement || !relation_render_nodes.is_empty();
        Ok::<OptimizerPassOutcome, LixError>(OptimizerPassOutcome {
            changed,
            diagnostics: broad_rewrite_diagnostics(before_summary.as_ref(), changed),
        })
    })?;

    Ok(OptimizedPublicReadStatement {
        shell_statement: rewritten_statement,
        relation_render_nodes,
        pass_traces: vec![trace],
    })
}

fn broad_rewrite_diagnostics(
    summary: Option<&BroadPublicRelationSummary>,
    changed: bool,
) -> Vec<String> {
    let mut diagnostics = Vec::new();
    match summary {
        Some(summary) if !summary.public_relations.is_empty() => {
            diagnostics.push(format!(
                "public relations: {}",
                summary
                    .public_relations
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        _ => diagnostics.push("no public relations matched broad surface rewrite".to_string()),
    }
    if changed {
        diagnostics.push("rewrote public relations into lowered derived relations".to_string());
    } else {
        diagnostics.push("statement already lowered or not rewriteable".to_string());
    }
    diagnostics
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
            None,
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
    }

    #[test]
    fn direct_history_strategy_records_trace() {
        let binding = SurfaceRegistry::with_builtin_surfaces()
            .bind_relation_name("lix_state_history")
            .expect("builtin history surface should bind");
        let decision = choose_specialized_public_read_strategy(&binding, false);

        assert!(decision.direct_execution);
        assert_eq!(decision.pass_traces.len(), 1);
        assert_eq!(
            decision.pass_traces[0].name,
            "public-read.choose-direct-history-strategy"
        );
        assert!(decision.pass_traces[0].changed);
    }
}
