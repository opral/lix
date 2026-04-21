//! Projection-local replay status loading.
//!
//! This module owns the local status surface that describes how a derived
//! projection has been caught up on this replica. It is operational state,
//! separate from canonical semantics.

use crate::live_state::lifecycle;
use crate::live_state::store::{LiveStateBackendRef, LiveStateExecutorRef};
use crate::live_state::store_sql::SqlLiveStateStore;
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
    lifecycle::load_projection_status(&SqlLiveStateStore::from_executor(executor)).await
}
