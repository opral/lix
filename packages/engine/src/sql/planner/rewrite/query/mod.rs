use sqlparser::ast::Query;

use crate::sql::entity_views::read as entity_view_read;
use crate::sql::read_pipeline::context::AnalysisContext;
use crate::sql::read_pipeline::registry::RewritePhase;
use crate::sql::read_pipeline::rules::query::analyze::relation_discovery;
use crate::sql::read_pipeline::rules::query::optimize::projection_cleanup;
use crate::sql::read_pipeline::validator::{validate_final_read_query, validate_phase_invariants};
use crate::sql::read_views::{
    lix_active_account_view_read, lix_active_version_view_read, lix_state_by_version_view_read,
    lix_state_history_view_read, lix_state_view_read, lix_version_view_read, vtable_read,
};
use crate::sql::steps::filesystem_step;
use crate::{LixBackend, LixError, Value};

const MAX_PASSES_PER_PHASE: usize = 32;
const PHASE_ORDER: [RewritePhase; 4] = [
    RewritePhase::Analyze,
    RewritePhase::Canonicalize,
    RewritePhase::Optimize,
    RewritePhase::Lower,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlannerQueryRule {
    AnalyzeRelationDiscovery,
    CanonicalLogicalViews,
    ProjectionCleanup,
    VtableRead,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PlannerRuleOutcome {
    NoChange,
    Changed(Query),
}

const ANALYZE_RULES: &[PlannerQueryRule] = &[PlannerQueryRule::AnalyzeRelationDiscovery];
const CANONICALIZE_RULES: &[PlannerQueryRule] = &[PlannerQueryRule::CanonicalLogicalViews];
const OPTIMIZE_RULES: &[PlannerQueryRule] = &[PlannerQueryRule::ProjectionCleanup];
const LOWER_RULES: &[PlannerQueryRule] = &[PlannerQueryRule::VtableRead];

fn rules_for_phase(phase: RewritePhase) -> &'static [PlannerQueryRule] {
    match phase {
        RewritePhase::Analyze => ANALYZE_RULES,
        RewritePhase::Canonicalize => CANONICALIZE_RULES,
        RewritePhase::Optimize => OPTIMIZE_RULES,
        RewritePhase::Lower => LOWER_RULES,
    }
}

impl PlannerQueryRule {
    fn matches_context(self, context: &AnalysisContext) -> bool {
        match self {
            Self::AnalyzeRelationDiscovery => true,
            Self::CanonicalLogicalViews => context.references_any_logical_read_view(),
            Self::ProjectionCleanup => context.has_nested_query_shapes(),
            Self::VtableRead => context.references_relation("lix_internal_state_vtable"),
        }
    }

    async fn apply_with_backend_and_params(
        self,
        backend: &dyn LixBackend,
        query: Query,
        params: &[Value],
        context: &AnalysisContext,
    ) -> Result<PlannerRuleOutcome, LixError> {
        if !self.matches_context(context) {
            return Ok(PlannerRuleOutcome::NoChange);
        }

        match self {
            Self::AnalyzeRelationDiscovery => {
                relation_discovery::validate_relation_discovery_consistency(&query)?;
                Ok(PlannerRuleOutcome::NoChange)
            }
            Self::CanonicalLogicalViews => Ok(outcome_from_option(
                rewrite_canonical_logical_views_with_backend(backend, query, params).await?,
            )),
            Self::ProjectionCleanup => Ok(outcome_from_option(projection_cleanup::rewrite_query(
                query,
            )?)),
            Self::VtableRead => Ok(outcome_from_option(
                vtable_read::rewrite_query_with_backend(backend, query).await?,
            )),
        }
    }
}

fn outcome_from_option(rewritten: Option<Query>) -> PlannerRuleOutcome {
    if let Some(query) = rewritten {
        PlannerRuleOutcome::Changed(query)
    } else {
        PlannerRuleOutcome::NoChange
    }
}

async fn rewrite_canonical_logical_views_with_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Option<Query>, LixError> {
    let mut current = query;
    let mut changed = false;

    let filesystem_rewritten = if params.is_empty() {
        filesystem_step::rewrite_query(current.clone())?
    } else {
        filesystem_step::rewrite_query_with_params(current.clone(), params)?
    };
    changed |= apply_step(&mut current, filesystem_rewritten);

    let entity_rewritten =
        entity_view_read::rewrite_query_with_backend(backend, current.clone()).await?;
    changed |= apply_step(&mut current, entity_rewritten);

    apply_standard_logical_view_rewrites(&mut current, &mut changed)?;

    Ok(if changed { Some(current) } else { None })
}

fn apply_step(current: &mut Query, rewritten: Option<Query>) -> bool {
    if let Some(next) = rewritten {
        *current = next;
        true
    } else {
        false
    }
}

fn apply_standard_logical_view_rewrites(
    current: &mut Query,
    changed: &mut bool,
) -> Result<(), LixError> {
    let rewritten = lix_version_view_read::rewrite_query(current.clone())?;
    *changed |= apply_step(current, rewritten);
    let rewritten = lix_active_account_view_read::rewrite_query(current.clone())?;
    *changed |= apply_step(current, rewritten);
    let rewritten = lix_active_version_view_read::rewrite_query(current.clone())?;
    *changed |= apply_step(current, rewritten);
    let rewritten = lix_state_view_read::rewrite_query(current.clone())?;
    *changed |= apply_step(current, rewritten);
    let rewritten = lix_state_by_version_view_read::rewrite_query(current.clone())?;
    *changed |= apply_step(current, rewritten);
    let rewritten = lix_state_history_view_read::rewrite_query(current.clone())?;
    *changed |= apply_step(current, rewritten);
    Ok(())
}

pub(crate) async fn rewrite_query_with_backend_and_params(
    backend: &dyn LixBackend,
    mut query: Query,
    params: &[Value],
) -> Result<Query, LixError> {
    let mut context = AnalysisContext::from_query(&query);

    for phase in PHASE_ORDER {
        run_phase_with_backend(phase, backend, &mut query, params, &mut context).await?;
    }

    validate_final_read_query(&query)?;
    Ok(query)
}

async fn run_phase_with_backend(
    phase: RewritePhase,
    backend: &dyn LixBackend,
    query: &mut Query,
    params: &[Value],
    context: &mut AnalysisContext,
) -> Result<(), LixError> {
    for _ in 0..MAX_PASSES_PER_PHASE {
        let changed =
            apply_rules_for_phase_with_backend(phase, backend, query, params, context).await?;

        context.refresh_from_query(query);
        validate_phase_invariants(phase, query, context)?;
        if !changed {
            return Ok(());
        }
    }

    Err(LixError {
        message: format!(
            "planner read rewrite phase '{phase:?}' exceeded maximum pass count ({MAX_PASSES_PER_PHASE})"
        ),
    })
}

async fn apply_rules_for_phase_with_backend(
    phase: RewritePhase,
    backend: &dyn LixBackend,
    query: &mut Query,
    params: &[Value],
    context: &mut AnalysisContext,
) -> Result<bool, LixError> {
    let mut changed = false;

    for rule in rules_for_phase(phase) {
        match rule
            .apply_with_backend_and_params(backend, query.clone(), params, context)
            .await?
        {
            PlannerRuleOutcome::NoChange => {}
            PlannerRuleOutcome::Changed(rewritten) => {
                *query = rewritten;
                changed = true;
                context.refresh_from_query(query);
            }
        }
    }

    Ok(changed)
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::rewrite_query_with_backend_and_params;
    use crate::{LixBackend, LixError, LixTransaction, QueryResult, SqlDialect, Value};

    #[derive(Default)]
    struct InertBackend;

    #[async_trait::async_trait(?Send)]
    impl LixBackend for InertBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            Ok(QueryResult { rows: Vec::new() })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Err(LixError {
                message: "test backend does not support transactions".to_string(),
            })
        }
    }

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            other => panic!("expected query, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn planner_query_rewrite_rewrites_nested_logical_views() {
        let backend = InertBackend;
        let query = parse_query(
            "SELECT version_id \
             FROM lix_state_by_version \
             WHERE version_id IN (SELECT version_id FROM lix_active_version)",
        );

        let rewritten = rewrite_query_with_backend_and_params(&backend, query, &[])
            .await
            .expect("planner query rewrite");
        let sql = rewritten.to_string();

        assert!(!sql.contains("FROM lix_active_version"));
        assert!(sql.contains("lix_internal_state_vtable"));
    }
}
