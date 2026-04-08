//! Canonical committed-read owner package.
//!
//! This package owns commit-addressed state and history reads derived from
//! canonical journal facts.

pub(crate) mod history;
pub(crate) mod state;

pub(crate) use history::{
    build_state_history_source_sql, CanonicalHistoryContentMode, CanonicalHistoryRootFacts,
    CanonicalHistoryRootSelection, CanonicalRootCommit,
};
pub(crate) use state::{
    load_canonical_change_row_by_id, load_commit_lineage_entry_by_id,
    load_exact_committed_state_row_from_commit_with_executor, CommitLineageEntry,
    CommitQueryExecutor, CommittedCanonicalChangeRow, ExactCommittedStateRow,
    ExactCommittedStateRowRequest,
};
