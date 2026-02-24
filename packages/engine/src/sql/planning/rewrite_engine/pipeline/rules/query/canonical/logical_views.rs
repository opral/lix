use sqlparser::ast::Query;

use crate::engine::sql::planning::rewrite_engine::pipeline::registry::QueryRuleOutcome;
use crate::{LixBackend, LixError, Value};

use super::{
    entity_views, filesystem_views, lix_active_account, lix_active_version, lix_state,
    lix_state_by_version, lix_state_history, lix_version,
};

pub(crate) fn rewrite_query(query: Query, params: &[Value]) -> Result<QueryRuleOutcome, LixError> {
    let mut current = query;
    let mut changed = false;

    let rewritten = filesystem_views::rewrite_query(current.clone(), params)?;
    changed |= apply_step(&mut current, rewritten);
    let rewritten = entity_views::rewrite_query(current.clone())?;
    changed |= apply_step(&mut current, rewritten);
    let rewritten = lix_version::rewrite_query(current.clone())?;
    changed |= apply_step(&mut current, rewritten);
    let rewritten = lix_active_account::rewrite_query(current.clone())?;
    changed |= apply_step(&mut current, rewritten);
    let rewritten = lix_active_version::rewrite_query(current.clone())?;
    changed |= apply_step(&mut current, rewritten);
    let rewritten = lix_state::rewrite_query(current.clone())?;
    changed |= apply_step(&mut current, rewritten);
    let rewritten = lix_state_by_version::rewrite_query(current.clone())?;
    changed |= apply_step(&mut current, rewritten);
    let rewritten = lix_state_history::rewrite_query(current.clone())?;
    changed |= apply_step(&mut current, rewritten);

    if changed {
        Ok(QueryRuleOutcome::Changed(current))
    } else {
        Ok(QueryRuleOutcome::NoChange)
    }
}

pub(crate) async fn rewrite_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<QueryRuleOutcome, LixError> {
    let mut current = query;
    let mut changed = false;

    let rewritten = filesystem_views::rewrite_query(current.clone(), params)?;
    changed |= apply_step(&mut current, rewritten);
    let rewritten = entity_views::rewrite_query_with_backend(backend, current.clone()).await?;
    changed |= apply_step(&mut current, rewritten);
    let rewritten = lix_version::rewrite_query(current.clone())?;
    changed |= apply_step(&mut current, rewritten);
    let rewritten = lix_active_account::rewrite_query(current.clone())?;
    changed |= apply_step(&mut current, rewritten);
    let rewritten = lix_active_version::rewrite_query(current.clone())?;
    changed |= apply_step(&mut current, rewritten);
    let rewritten = lix_state::rewrite_query(current.clone())?;
    changed |= apply_step(&mut current, rewritten);
    let rewritten = lix_state_by_version::rewrite_query(current.clone())?;
    changed |= apply_step(&mut current, rewritten);
    let rewritten =
        lix_state_history::rewrite_query_with_backend(backend, current.clone(), params).await?;
    changed |= apply_step(&mut current, rewritten);

    if changed {
        Ok(QueryRuleOutcome::Changed(current))
    } else {
        Ok(QueryRuleOutcome::NoChange)
    }
}

fn apply_step(current: &mut Query, rewritten: Option<Query>) -> bool {
    if let Some(next) = rewritten {
        *current = next;
        true
    } else {
        false
    }
}
