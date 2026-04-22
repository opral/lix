//! Projection-local replay status loading.
//!
//! This module owns the local status surface that describes how a derived
//! projection has been caught up on this replica. It is operational state,
//! separate from canonical semantics.

use crate::live_state::lifecycle;
use crate::live_state::lifecycle::LiveStateSnapshot;
use crate::live_state::store::{LiveStateBackendRef, LiveStateExecutorRef};
use crate::live_state::store_sql::{
    load_latest_replay_cursor_with_executor, load_nullable_live_state_status_with_executor,
    SqlLiveStateStore,
};
use crate::live_state::LiveStateProjectionStatus;
use crate::LixError;

pub(crate) async fn load_live_state_projection_status_with_backend(
    backend: LiveStateBackendRef<'_>,
) -> Result<LiveStateProjectionStatus, LixError> {
    lifecycle::load_projection_status(&SqlLiveStateStore::from_backend(backend)).await
}

pub(crate) async fn load_live_state_projection_status_with_executor(
    executor: LiveStateExecutorRef<'_>,
) -> Result<LiveStateProjectionStatus, LixError> {
    let status = load_nullable_live_state_status_with_executor(executor).await?;
    let latest_replay_cursor = load_latest_replay_cursor_with_executor(executor).await?;
    let current_committed_frontier =
        crate::live_state::load_current_committed_version_frontier_with_executor(executor).await?;
    Ok(lifecycle::projection_status_from_snapshot(
        LiveStateSnapshot {
            status,
            latest_replay_cursor,
            current_committed_frontier,
        },
    ))
}
