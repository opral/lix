//! Projection-local replay boundary helpers.
//!
//! Replay cursors and replay recovery live here because they are local scanning
//! mechanics for derived projections. They are rebuildable and non-semantic.

use crate::live_state::lifecycle;
use crate::live_state::LiveStateMode;
use crate::{LixBackend, LixBackendTransaction, LixError, ReplayCursor};

pub(crate) async fn load_latest_live_state_replay_cursor_with_backend(
    backend: &dyn LixBackend,
) -> Result<Option<ReplayCursor>, LixError> {
    lifecycle::load_latest_replay_cursor(backend).await
}

pub(crate) async fn advance_live_state_projection_replay_boundary_to_cursor_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    cursor: &ReplayCursor,
) -> Result<(), LixError> {
    lifecycle::advance_commit_replay_boundary_to_cursor_in_transaction(transaction, cursor).await
}

pub(crate) async fn mark_live_state_projection_needs_rebuild_at_replay_cursor_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    cursor: &ReplayCursor,
) -> Result<(), LixError> {
    lifecycle::mark_needs_rebuild_at_replay_cursor_in_transaction(transaction, cursor).await
}

pub(crate) async fn mark_live_state_projection_ready_with_backend(
    backend: &dyn LixBackend,
    cursor: &ReplayCursor,
) -> Result<(), LixError> {
    lifecycle::mark_live_state_ready_with_backend(backend, cursor).await
}

pub(crate) async fn mark_live_state_projection_ready_at_replay_cursor_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    cursor: &ReplayCursor,
) -> Result<(), LixError> {
    lifecycle::mark_live_state_ready_at_replay_cursor_in_transaction(transaction, cursor).await
}

pub(crate) async fn mark_live_state_projection_ready_without_replay_cursor_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<(), LixError> {
    lifecycle::mark_live_state_ready_without_replay_cursor_in_transaction(transaction).await
}

pub(crate) async fn mark_live_state_projection_replay_state_lost_with_backend(
    backend: &dyn LixBackend,
) -> Result<(), LixError> {
    lifecycle::mark_live_state_mode_with_backend(backend, LiveStateMode::NeedsRebuild).await
}
