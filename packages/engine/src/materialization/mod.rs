mod apply;
mod loader;
mod plan;
mod types;

pub use types::{
    InheritanceWinnerDebugRow, LatestVisibleWinnerDebugRow, MaterializationApplyReport,
    MaterializationDebugMode, MaterializationDebugTrace, MaterializationPlan,
    MaterializationReport, MaterializationRequest, MaterializationScope, MaterializationWarning,
    MaterializationWrite, MaterializationWriteOp, StageStat, TraversedCommitDebugRow,
    TraversedEdgeDebugRow, VersionAncestryDebugRow, VersionPointerDebugRow,
};

use crate::{LixBackend, LixError};

pub async fn materialization_plan(
    backend: &dyn LixBackend,
    req: &MaterializationRequest,
) -> Result<MaterializationPlan, LixError> {
    plan::materialization_plan_internal(backend, req).await
}

pub async fn apply_materialization_plan(
    backend: &dyn LixBackend,
    plan: &MaterializationPlan,
) -> Result<MaterializationApplyReport, LixError> {
    apply::apply_materialization_plan_internal(backend, plan).await
}

pub async fn materialize(
    backend: &dyn LixBackend,
    req: &MaterializationRequest,
) -> Result<MaterializationReport, LixError> {
    let plan = materialization_plan(backend, req).await?;
    let apply = apply_materialization_plan(backend, &plan).await?;
    Ok(MaterializationReport { plan, apply })
}
