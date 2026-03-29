//! Projection-local replay status loading.
//!
//! This module owns the local status surface that describes how a derived
//! projection has been caught up on this replica. It is operational state,
//! separate from canonical semantics.

use crate::live_state::lifecycle;
use crate::live_state::LiveStateProjectionStatus;
use crate::{LixBackend, LixError};

pub(crate) async fn load_live_state_projection_status_with_backend(
    backend: &dyn LixBackend,
) -> Result<LiveStateProjectionStatus, LixError> {
    lifecycle::load_projection_status_with_backend(backend).await
}
