mod apply;
mod loader;
mod plan;
mod types;

pub use types::{
    LatestVisibleWinnerDebugRow, LiveStateApplyReport, LiveStateRebuildDebugMode,
    LiveStateRebuildDebugTrace, LiveStateRebuildPlan, LiveStateRebuildReport,
    LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning, LiveStateWrite,
    LiveStateWriteOp, ScopeWinnerDebugRow, StageStat, TraversedCommitDebugRow,
    TraversedEdgeDebugRow, VersionAncestryDebugRow, VersionHeadDebugRow,
};

use crate::{LixBackend, LixError};

pub async fn live_state_rebuild_plan(
    backend: &dyn LixBackend,
    req: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildPlan, LixError> {
    plan::live_state_rebuild_plan_internal(backend, req).await
}

pub async fn apply_live_state_rebuild_plan(
    backend: &dyn LixBackend,
    plan: &LiveStateRebuildPlan,
) -> Result<LiveStateApplyReport, LixError> {
    apply::apply_live_state_rebuild_plan_internal(backend, plan).await
}

pub async fn rebuild_live_state(
    backend: &dyn LixBackend,
    req: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildReport, LixError> {
    let plan = live_state_rebuild_plan(backend, req).await?;
    let apply = apply_live_state_rebuild_plan(backend, &plan).await?;
    Ok(LiveStateRebuildReport { plan, apply })
}
