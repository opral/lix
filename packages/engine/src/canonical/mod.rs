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
//! - tracked lineage is expressed by commit membership around canonical facts
//! - non-commit visibility is expected to be expressed by separate untracked
//!   visibility membership around canonical facts
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
//! In the stronger canonical model, classifications such as "tracked" versus
//! "untracked" are not intrinsic facts about the row identity itself. They are
//! lineage or visibility relations layered around the canonical fact row and may be
//! surfaced publicly as derived metadata for ergonomics.
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
mod api;
mod change_commit_sql;
mod checkpoint_labels;
mod graph;
mod init;
mod journal;
pub(crate) mod json;
mod read;
mod receipt;
pub(crate) mod store;
pub(crate) mod store_sql;

pub(crate) use change_commit_sql::build_lazy_change_commit_by_change_id_ctes_sql;
pub use json::CanonicalJson;
pub use receipt::{CanonicalCommitReceipt, UpdatedVersionRef};

#[allow(unused_imports)]
pub(crate) use api::{
    append_changes, append_untracked_change_visibility_rows, canonical_untracked_visibility_kind,
    canonical_untracked_visibility_row_id_for_change,
    canonical_untracked_visibility_write_from_change_visibility,
    compact_stale_untracked_changes_in_transaction,
    compact_untracked_changes_for_touched_rows_in_transaction, load_change, load_commit,
    load_commit_member_change, load_exact_row_at_commit, load_history, load_visible_state,
    replace_snapshot_content_in_transaction, resolve_merge_base, CanonicalAppendSummary,
    CanonicalChange, CanonicalChangeWrite, CanonicalCommit, CanonicalContentMode,
    CanonicalHistoryContentMode, CanonicalHistoryRequest, CanonicalHistoryRootSelection,
    CanonicalHistoryRow, CanonicalRootCommit, CanonicalStateIdentity, CanonicalStateRow,
    CanonicalTombstoneMode, CanonicalUntrackedVisibilityKind, CanonicalUntrackedVisibilityWrite,
    CanonicalVisibility, CanonicalVisibleStateFilter, CanonicalVisibleStateRequest,
    CanonicalVisibleStateRow,
};
#[allow(unused_imports)]
pub(crate) use checkpoint_labels::{
    checkpoint_commit_label_entity_id, checkpoint_commit_label_snapshot, checkpoint_label_snapshot,
    resolve_last_checkpoint_commit_id_for_tip_with_executor, CheckpointVersionHeadFact,
    CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY, CHECKPOINT_LABEL_ID, CHECKPOINT_LABEL_NAME,
    CHECKPOINT_LABEL_SCHEMA_KEY,
};
pub(crate) use graph::{build_commit_generation_seed_sql, COMMIT_GRAPH_NODE_TABLE};
pub(crate) use init::init;
#[allow(unused_imports)]
pub(crate) use read::{
    load_exact_committed_change_from_commit_with_executor, ExactCommittedStateRowRequest,
};
pub(crate) const ENTITY_STATE_TIMELINE_BREAKPOINT_TABLE: &str =
    "lix_internal_entity_state_timeline_breakpoint";
pub(crate) const TIMELINE_STATUS_TABLE: &str = "lix_internal_timeline_status";

pub(crate) fn internal_exact_relation_names() -> &'static [&'static str] {
    &[
        journal::write::CHANGE_TABLE,
        journal::write::UNTRACKED_CHANGE_VISIBILITY_TABLE,
        graph::index::COMMIT_GRAPH_NODE_TABLE,
        journal::write::SNAPSHOT_TABLE,
        ENTITY_STATE_TIMELINE_BREAKPOINT_TABLE,
        TIMELINE_STATUS_TABLE,
    ]
}
