use sqlparser::ast::Query;

use crate::{LixBackend, LixError, Value};

use super::context::AnalysisContext;
use super::registry::{rules_for_phase, RewritePhase};
use super::validator::{validate_final_read_query, validate_phase_invariants};

const MAX_PASSES_PER_PHASE: usize = 32;

pub(crate) fn rewrite_read_query(query: Query) -> Result<Query, LixError> {
    run_query_engine_sync(query, &[])
}

pub(crate) async fn rewrite_read_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
) -> Result<Query, LixError> {
    run_query_engine_with_backend_and_params(backend, query, &[]).await
}

pub(crate) async fn rewrite_read_query_with_backend_and_params(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Query, LixError> {
    run_query_engine_with_backend_and_params(backend, query, params).await
}

fn run_query_engine_sync(mut query: Query, params: &[Value]) -> Result<Query, LixError> {
    let mut context = AnalysisContext::from_query(&query);

    for phase in [
        RewritePhase::Analyze,
        RewritePhase::Canonicalize,
        RewritePhase::Optimize,
        RewritePhase::Lower,
    ] {
        let mut converged = false;
        for _ in 0..MAX_PASSES_PER_PHASE {
            let mut changed = false;
            for rule in rules_for_phase(phase) {
                if !rule.matches_context(&context) {
                    continue;
                }
                if let Some(rewritten) = rule.apply_sync(query.clone(), params)? {
                    query = rewritten;
                    changed = true;
                    context = AnalysisContext::from_query(&query);
                }
            }

            context = AnalysisContext::from_query(&query);
            validate_phase_invariants(phase, &query, &context)?;
            if !changed {
                converged = true;
                break;
            }
        }

        if !converged {
            return Err(LixError {
                message: format!(
                    "read rewrite phase '{phase:?}' exceeded maximum pass count ({MAX_PASSES_PER_PHASE})"
                ),
            });
        }
    }

    validate_final_read_query(&query)?;
    Ok(query)
}

async fn run_query_engine_with_backend_and_params(
    backend: &dyn LixBackend,
    mut query: Query,
    params: &[Value],
) -> Result<Query, LixError> {
    let mut context = AnalysisContext::from_query(&query);

    for phase in [
        RewritePhase::Analyze,
        RewritePhase::Canonicalize,
        RewritePhase::Optimize,
        RewritePhase::Lower,
    ] {
        let mut converged = false;
        for _ in 0..MAX_PASSES_PER_PHASE {
            let mut changed = false;
            for rule in rules_for_phase(phase) {
                if !rule.matches_context(&context) {
                    continue;
                }
                if let Some(rewritten) = rule
                    .apply_with_backend_and_params(backend, query.clone(), params)
                    .await?
                {
                    query = rewritten;
                    changed = true;
                    context = AnalysisContext::from_query(&query);
                }
            }

            context = AnalysisContext::from_query(&query);
            validate_phase_invariants(phase, &query, &context)?;
            if !changed {
                converged = true;
                break;
            }
        }

        if !converged {
            return Err(LixError {
                message: format!(
                    "read rewrite backend phase '{phase:?}' exceeded maximum pass count ({MAX_PASSES_PER_PHASE})"
                ),
            });
        }
    }

    validate_final_read_query(&query)?;
    Ok(query)
}
