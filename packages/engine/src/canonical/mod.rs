//! Canonical committed-history subsystem boundary.
//!
//! `canonical` owns durable canonical facts, canonical graph derivation, and
//! committed read resolution over those facts.
//!
//! Use this subsystem when the question is:
//! - what canonical facts were committed?
//! - what commit graph do those facts imply?
//! - what state/history does an explicit commit/root input mean?
//!
//! The intended ownership model is:
//! - canonical changes are the only semantic source of truth
//! - commit graph facts are a canonical projection derived from those changes
//! - `refs` owns replica-local committed-head/root selection
//! - `commit` owns write orchestration that composes canonical facts with other
//!   owners atomically
//! - committed meaning/state is resolved from commit-graph facts plus explicit
//!   selected roots supplied by higher owners
//!
//! `canonical` owns:
//! - canonical change facts and commit headers stored in the journal
//! - commit DAG interpretation and canonical history indexes
//! - commit-addressed and root-addressed state lookup
//!
//! Derived mirrors, replay cursors, and storage-local append order may help
//! execution, but they must not redefine committed semantics.
//!
//! Plan 20 Phase 1 introduces the canonical-only package layout:
//! - `canonical::journal`
//! - `canonical::graph`
//! - `canonical::read`
//!
//! `checkpoint` depends on canonical as a derived acceleration layer.
//! `live_state` may mirror canonical facts and derived commit-family surfaces
//! such as `lix_commit`, `lix_change_set`, `lix_change_set_element`, and
//! `lix_commit_edge` as read-only query surfaces for SQL/public reads, but it
//! does not own the meaning of those facts.
//!
pub(crate) mod checkpoint_labels;
pub(crate) mod graph;
mod init;
pub(crate) mod json;
pub(crate) mod journal;
pub(crate) mod read;

pub(crate) const ENTITY_STATE_TIMELINE_BREAKPOINT_TABLE: &str =
    "lix_internal_entity_state_timeline_breakpoint";
pub(crate) const TIMELINE_STATUS_TABLE: &str = "lix_internal_timeline_status";

pub(crate) use init::{init, seed_bootstrap};
