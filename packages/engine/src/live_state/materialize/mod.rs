//! Rebuildable materialization of query-serving live-state rows.
//!
//! `materialize` projects canonical committed meaning into derived live-state
//! rows for query serving. Workspace annotation data such as `writer_key` may
//! be supplied as optional overlay hints for current read surfaces, but it is
//! not required for semantic replay correctness.
//!
//! Losing or rebuilding this state may affect performance or readiness, but it
//! must not change committed semantics.

mod apply;
mod loader;
mod plan;
mod types;

use std::collections::BTreeMap;

pub use types::{
    LatestVisibleWinnerDebugRow, LiveStateApplyReport, LiveStateRebuildDebugMode,
    LiveStateRebuildDebugTrace, LiveStateRebuildPlan, LiveStateRebuildReport,
    LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning, LiveStateWrite,
    LiveStateWriteOp, ScopeWinnerDebugRow, StageStat, TraversedCommitDebugRow,
    TraversedEdgeDebugRow, VersionHeadDebugRow,
};

use crate::backend::TransactionBackendAdapter;
use crate::live_state::shared::identity::RowIdentity;
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
    let mut executor = TransactionBackendAdapter::new(transaction);
    plan::live_state_rebuild_plan_with_executor(&mut executor, req).await
}

pub async fn apply_rebuild_plan(
    backend: &dyn LixBackend,
    plan: &LiveStateRebuildPlan,
) -> Result<LiveStateApplyReport, LixError> {
    apply::apply_live_state_rebuild_plan_internal(backend, plan).await
}

pub(crate) async fn apply_rebuild_scope_with_writer_key_hints_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    plan: &LiveStateRebuildPlan,
    writer_key_hints: &BTreeMap<RowIdentity, Option<String>>,
) -> Result<(usize, std::collections::BTreeSet<String>), LixError> {
    apply::apply_live_state_scope_with_writer_key_hints_in_transaction(
        transaction,
        plan,
        writer_key_hints,
    )
    .await
}

pub async fn rebuild(
    backend: &dyn LixBackend,
    req: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildReport, LixError> {
    let plan = rebuild_plan(backend, req).await?;
    let apply = apply_rebuild_plan(backend, &plan).await?;
    Ok(LiveStateRebuildReport { plan, apply })
}
