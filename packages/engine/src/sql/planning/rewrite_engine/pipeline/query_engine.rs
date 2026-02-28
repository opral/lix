use sqlparser::ast::Query;
use std::collections::BTreeMap;

use crate::{LixBackend, LixError, Value};

use super::context::AnalysisContext;
use super::registry::{rules_for_phase, QueryRuleOutcome, RewritePhase};
use super::validator::{validate_final_read_query, validate_phase_invariants};

const MAX_PASSES_PER_PHASE: usize = 32;
const PHASE_ORDER: [RewritePhase; 4] = [
    RewritePhase::Analyze,
    RewritePhase::Canonicalize,
    RewritePhase::Optimize,
    RewritePhase::Lower,
];

#[derive(Debug, Default, Clone)]
pub(crate) struct ReadRewriteSession {
    materialized_schema_keys_cache: Option<Vec<String>>,
    version_chain_cache: BTreeMap<String, Vec<String>>,
}

impl ReadRewriteSession {
    pub(crate) fn cached_version_chain(&self, version_id: &str) -> Option<&[String]> {
        self.version_chain_cache.get(version_id).map(Vec::as_slice)
    }

    pub(crate) fn cache_version_chain(&mut self, version_id: &str, chain: &[String]) {
        self.version_chain_cache
            .insert(version_id.to_string(), chain.to_vec());
    }

    fn seed_context(&self, context: &mut AnalysisContext) {
        if let Some(keys) = &self.materialized_schema_keys_cache {
            context.set_materialized_schema_keys_cache(keys.clone());
        }
    }

    fn absorb_context(&mut self, context: &AnalysisContext) {
        if let Some(keys) = context.materialized_schema_keys_cache() {
            self.materialized_schema_keys_cache = Some(keys.to_vec());
        }
    }
}

pub(crate) fn rewrite_read_query(query: Query) -> Result<Query, LixError> {
    run_query_engine_sync(query, &[])
}

pub(crate) async fn rewrite_read_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
) -> Result<Query, LixError> {
    run_query_engine_with_backend_and_params(backend, query, &[], None).await
}

pub(crate) async fn rewrite_read_query_with_backend_and_params(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Query, LixError> {
    run_query_engine_with_backend_and_params(backend, query, params, None).await
}

pub(crate) async fn rewrite_read_query_with_backend_and_params_in_session(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
    session: &mut ReadRewriteSession,
) -> Result<Query, LixError> {
    run_query_engine_with_backend_and_params(backend, query, params, Some(session)).await
}

fn run_query_engine_sync(mut query: Query, params: &[Value]) -> Result<Query, LixError> {
    let mut context = AnalysisContext::from_query(&query);

    for phase in PHASE_ORDER {
        run_phase_sync(phase, &mut query, params, &mut context)?;
    }

    validate_final_read_query(&query)?;
    Ok(query)
}

fn run_phase_sync(
    phase: RewritePhase,
    query: &mut Query,
    params: &[Value],
    context: &mut AnalysisContext,
) -> Result<(), LixError> {
    for _ in 0..MAX_PASSES_PER_PHASE {
        let changed = apply_rules_for_phase_sync(phase, query, params, context)?;

        context.refresh_from_query(query);
        validate_phase_invariants(phase, query, context)?;
        if !changed {
            return Ok(());
        }
    }

    Err(LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: format!(
            "read rewrite phase '{phase:?}' exceeded maximum pass count ({MAX_PASSES_PER_PHASE})"
        ),
    })
}

fn apply_rules_for_phase_sync(
    phase: RewritePhase,
    query: &mut Query,
    params: &[Value],
    context: &mut AnalysisContext,
) -> Result<bool, LixError> {
    let mut changed = false;

    for rule in rules_for_phase(phase) {
        match rule.apply_sync(query.clone(), params, context)? {
            QueryRuleOutcome::NotApplicable | QueryRuleOutcome::NoChange => {}
            QueryRuleOutcome::Changed(rewritten) => {
                *query = rewritten;
                changed = true;
                context.refresh_from_query(query);
            }
        }
    }

    Ok(changed)
}

async fn run_query_engine_with_backend_and_params(
    backend: &dyn LixBackend,
    mut query: Query,
    params: &[Value],
    mut session: Option<&mut ReadRewriteSession>,
) -> Result<Query, LixError> {
    let mut context = AnalysisContext::from_query(&query);
    if let Some(session) = session.as_mut() {
        session.seed_context(&mut context);
    }

    for phase in PHASE_ORDER {
        run_phase_with_backend(phase, backend, &mut query, params, &mut context).await?;
    }

    if let Some(session) = session.as_mut() {
        session.absorb_context(&context);
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

    Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!(
            "read rewrite backend phase '{phase:?}' exceeded maximum pass count ({MAX_PASSES_PER_PHASE})"
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
            QueryRuleOutcome::NotApplicable | QueryRuleOutcome::NoChange => {}
            QueryRuleOutcome::Changed(rewritten) => {
                *query = rewritten;
                changed = true;
                context.refresh_from_query(query);
            }
        }
    }

    Ok(changed)
}
