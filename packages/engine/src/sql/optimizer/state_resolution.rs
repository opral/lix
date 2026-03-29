use crate::sql::analysis::state_resolution::canonical::{
    statement_targets_table_name, CanonicalStateResolution,
};
use crate::sql::optimizer::registry::{
    run_infallible_pass, OptimizerPassMetadata, OptimizerPassOutcome, OptimizerPassRegistry,
    OptimizerPassSettings, OptimizerPassTrace,
};
use sqlparser::ast::Statement;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct OptimizedStateResolution {
    pub(crate) read_only_query: bool,
    pub(crate) should_refresh_file_cache: bool,
    pub(crate) should_invalidate_installed_plugins_cache: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StateResolutionOptimization {
    pub(crate) optimized: OptimizedStateResolution,
    pub(crate) pass_traces: Vec<OptimizerPassTrace>,
}

const STATE_RESOLUTION_REQUIREMENTS_PASS: OptimizerPassMetadata = OptimizerPassMetadata {
    name: "state-resolution.refresh-file-cache",
    order: 10,
    description: "derive file-cache refresh requirements from canonical state targets",
};

const STATE_RESOLUTION_REGISTRY: OptimizerPassRegistry = OptimizerPassRegistry {
    name: "state-resolution",
    passes: &[STATE_RESOLUTION_REQUIREMENTS_PASS],
};

pub(crate) fn state_resolution_pass_registry() -> &'static OptimizerPassRegistry {
    &STATE_RESOLUTION_REGISTRY
}

pub(crate) fn optimize_state_resolution(
    statements: &[Statement],
    canonical: CanonicalStateResolution,
) -> StateResolutionOptimization {
    optimize_state_resolution_with_settings(
        statements,
        canonical,
        &OptimizerPassSettings::default(),
    )
}

fn optimize_state_resolution_with_settings(
    statements: &[Statement],
    canonical: CanonicalStateResolution,
    settings: &OptimizerPassSettings,
) -> StateResolutionOptimization {
    let metadata = state_resolution_pass_registry().passes[0];
    let should_refresh_file_cache = should_refresh_file_cache_for_statements(statements);
    let trace = run_infallible_pass(metadata, settings, || {
        let mut diagnostics = Vec::new();
        if canonical.read_only_query {
            diagnostics.push("read-only query; file-cache refresh not required".to_string());
        } else if should_refresh_file_cache {
            diagnostics.push(
                "writes target lix_state/lix_state_by_version; file cache refresh required"
                    .to_string(),
            );
        } else {
            diagnostics.push("no file-cache-refresh tables targeted".to_string());
        }
        if canonical.should_invalidate_installed_plugins_cache {
            diagnostics.push("installed plugins cache invalidation requested".to_string());
        }

        OptimizerPassOutcome {
            changed: !canonical.read_only_query && should_refresh_file_cache,
            diagnostics,
        }
    });

    StateResolutionOptimization {
        optimized: OptimizedStateResolution {
            read_only_query: canonical.read_only_query,
            should_refresh_file_cache: !canonical.read_only_query && should_refresh_file_cache,
            should_invalidate_installed_plugins_cache: canonical
                .should_invalidate_installed_plugins_cache,
        },
        pass_traces: vec![trace],
    }
}

fn should_refresh_file_cache_for_statements(statements: &[Statement]) -> bool {
    statements
        .iter()
        .any(statement_targets_file_cache_refresh_table)
}

fn statement_targets_file_cache_refresh_table(statement: &Statement) -> bool {
    statement_targets_table_name(statement, "lix_state")
        || statement_targets_table_name(statement, "lix_state_by_version")
}

#[cfg(test)]
mod tests {
    use super::{optimize_state_resolution, state_resolution_pass_registry};
    use crate::sql::analysis::state_resolution::canonical::canonicalize_state_resolution;
    use crate::sql::parser::parse_sql_statements;

    #[test]
    fn registry_exposes_stable_pass_order() {
        let registry = state_resolution_pass_registry();
        assert_eq!(registry.name, "state-resolution");
        assert_eq!(
            registry
                .passes
                .iter()
                .map(|pass| pass.name)
                .collect::<Vec<_>>(),
            vec!["state-resolution.refresh-file-cache"]
        );
    }

    #[test]
    fn captures_trace_for_file_cache_refresh() {
        let statements = parse_sql_statements(
            "UPDATE lix_state SET snapshot_content = '{\"id\":\"1\"}' WHERE entity_id = '1'",
        )
        .expect("sql should parse");
        let optimization =
            optimize_state_resolution(&statements, canonicalize_state_resolution(&statements));

        assert!(optimization.optimized.should_refresh_file_cache);
        assert_eq!(optimization.pass_traces.len(), 1);
        assert_eq!(
            optimization.pass_traces[0].name,
            "state-resolution.refresh-file-cache"
        );
        assert!(optimization.pass_traces[0].changed);
        assert!(optimization.pass_traces[0]
            .diagnostics
            .iter()
            .any(|line| line.contains("file cache refresh required")));
    }
}
