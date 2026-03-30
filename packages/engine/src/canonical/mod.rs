//! Canonical committed-history subsystem boundary.
//!
//! `canonical` owns the semantic meaning of committed history. Use this module
//! when the question is "what happened?" or "what state does commit/root X
//! mean?".
//!
//! The intended model is:
//! - canonical changes are the only semantic source of truth
//! - commit graph facts are a canonical projection derived from those changes
//! - replica-local version-head state selects committed heads/roots in that
//!   graph
//! - committed meaning/state is resolved from commit-graph facts plus those
//!   local selectors
//!
//! `canonical` owns:
//! - canonical change facts for canonical-owned entities such as `lix_commit`,
//!   `lix_commit_edge`, `lix_change_set`, and `lix_change_set_element`
//! - commit DAG interpretation and canonical history indexes
//! - head/root resolution
//! - commit-addressed and root-addressed state lookup
//!
//! Derived mirrors, replay cursors, and storage-local append order may help
//! execution, but they must not redefine committed semantics.
//!
//! `checkpoint` depends on canonical as a derived acceleration layer.
//! `live_state` may mirror canonical facts as read-only query surfaces for
//! SQL/public reads, but it does not own the meaning of those facts.
//!
pub(crate) mod append;
mod change_log;
#[allow(dead_code)]
mod create_commit;
mod create_commit_preflight;
mod generate_commit;
pub(crate) mod graph;
mod graph_index;
mod graph_sql;
pub(crate) mod history;
mod init;
pub(crate) mod lineage;
pub(crate) mod pending_session;
pub(crate) mod readers;
pub(crate) mod receipt;
pub(crate) mod refs;
pub(crate) mod roots;
pub(crate) mod state_source;
mod types;
pub(crate) mod version_state;
pub(crate) use init::{init, seed_bootstrap};
pub(crate) use receipt::CanonicalCommitReceipt;
pub(crate) use types::ProposedDomainChange;
