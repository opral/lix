//! Rebuildable materialization of query-serving live-state rows.
//!
//! `materialize` projects canonical committed meaning into derived live-state
//! rows for query serving.
//!
//! Losing or rebuilding this state may affect performance or readiness, but it
//! must not change committed semantics.

mod apply;
pub(crate) mod filesystem;
mod loader;
mod plan;
mod types;

pub use types::{
    LatestVisibleWinnerDebugRow, LiveStateApplyReport, LiveStateRebuildDebugMode,
    LiveStateRebuildDebugTrace, LiveStateRebuildPlan, LiveStateRebuildReport,
    LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning, LiveStateWrite,
    LiveStateWriteOp, ScopeWinnerDebugRow, StageStat, TraversedCommitDebugRow,
    TraversedEdgeDebugRow, VersionHeadDebugRow,
};

use crate::{LixBackend, LixBackendTransaction, LixError};

pub async fn rebuild_plan(
    backend: &dyn LixBackend,
    req: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildPlan, LixError> {
    plan::live_state_rebuild_plan_internal(backend, req).await
}

pub(crate) async fn rebuild_plan_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    req: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildPlan, LixError> {
    let mut executor = crate::backend::transaction_backend_view(transaction);
    plan::live_state_rebuild_plan_with_executor(&mut executor, req).await
}

pub async fn apply_rebuild_plan(
    backend: &dyn LixBackend,
    plan: &LiveStateRebuildPlan,
) -> Result<LiveStateApplyReport, LixError> {
    apply::apply_live_state_rebuild_plan_internal(backend, plan).await
}

pub(crate) async fn apply_rebuild_scope_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    plan: &LiveStateRebuildPlan,
) -> Result<(usize, std::collections::BTreeSet<String>), LixError> {
    apply::apply_live_state_scope_in_transaction(transaction, plan).await
}

pub async fn rebuild(
    backend: &dyn LixBackend,
    req: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildReport, LixError> {
    let plan = rebuild_plan(backend, req).await?;
    let apply = apply_rebuild_plan(backend, &plan).await?;
    Ok(LiveStateRebuildReport { plan, apply })
}
