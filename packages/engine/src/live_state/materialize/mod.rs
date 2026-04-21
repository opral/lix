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
mod rebuild_files;
mod types;

pub use types::{
    LatestVisibleWinnerDebugRow, LiveStateApplyReport, LiveStateRebuildDebugMode,
    LiveStateRebuildDebugTrace, LiveStateRebuildPlan, LiveStateRebuildReport,
    LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning, LiveStateWrite,
    LiveStateWriteOp, StageStat, TraversedCommitDebugRow, TraversedEdgeDebugRow,
    VersionHeadDebugRow, VisibilityWinnerDebugRow,
};

use crate::live_state::store::{
    LiveStateBackendRef, LiveStateExecutorRef, LiveStateTransactionRef,
};
use crate::plugin::FilesystemPluginMaterializer;
use crate::LixError;

pub(crate) async fn rebuild_plan_with_backend(
    backend: LiveStateBackendRef<'_>,
    req: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildPlan, LixError> {
    plan::live_state_rebuild_plan_internal(backend, req).await
}

pub(crate) async fn rebuild_plan_with_executor(
    executor: LiveStateExecutorRef<'_>,
    req: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildPlan, LixError> {
    plan::live_state_rebuild_plan_with_executor(executor, req).await
}

pub(crate) async fn rebuild_plan_with_transaction(
    transaction: LiveStateTransactionRef<'_>,
    req: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildPlan, LixError> {
    let mut executor = crate::live_state::store_sql::executor_from_transaction(transaction);
    rebuild_plan_with_executor(&mut executor, req).await
}

pub(crate) async fn apply_rebuild_plan_in_transaction(
    transaction: LiveStateTransactionRef<'_>,
    plan: &LiveStateRebuildPlan,
) -> Result<LiveStateApplyReport, LixError> {
    apply::apply_live_state_rebuild_plan_internal(transaction, plan).await
}

pub(crate) async fn apply_rebuild_scope_in_transaction(
    transaction: LiveStateTransactionRef<'_>,
    plan: &LiveStateRebuildPlan,
) -> Result<(usize, std::collections::BTreeSet<String>), LixError> {
    apply::apply_live_state_scope_in_transaction(transaction, plan).await
}

pub(crate) async fn rebuild_file_payloads_with_plugins(
    backend: LiveStateBackendRef<'_>,
    plugin_materializer: &dyn FilesystemPluginMaterializer,
    plan: &LiveStateRebuildPlan,
) -> Result<(), LixError> {
    rebuild_files::rebuild_file_payloads_with_plugins(backend, plugin_materializer, plan).await
}
