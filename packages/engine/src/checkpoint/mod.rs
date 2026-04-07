//! Checkpoint subsystem boundary.
//!
//! `checkpoint` is the user-facing commit-checkpoint subsystem.
//!
//! It owns two distinct layers:
//! - canonical checkpoint label facts attached to commits
//! - rebuildable history/filtering helpers derived from those label facts
//!
//! This subsystem is distinct from projection replay. Replay cursors and
//! applied watermarks belong to `live_state/projection/*`, not to
//! `checkpoint/*`.
//!
//! Checkpoint-managed rows currently split into two buckets:
//!
//! - The system-managed checkpoint label row plus commit-label links are
//!   canonical commit-graph facts.
//! - `lix_internal_last_checkpoint` is rebuildable checkpoint-history cache
//!   state.
//!
//! The pointer table is a convenience cache over canonical version heads plus
//! checkpoint labels; correctness must not depend on it being the only source
//! of truth.
//!
mod create_checkpoint;
mod history;
mod init;

pub(crate) use create_checkpoint::create_checkpoint_in_session;
pub use create_checkpoint::CreateCheckpointResult;
pub(crate) use init::{init, seed_bootstrap};
