//! Canonical committed-read owner package.
//!
//! This package owns committed state/history/version-descriptor reads derived
//! from canonical journal facts plus explicit local head selection.

pub(crate) mod history;
mod state_history_runtime;
pub(crate) mod state;
pub(crate) mod version_descriptors;

pub(crate) use history::{
    build_state_history_source_sql, CanonicalHistoryContentMode, CanonicalHistoryRootFacts,
    CanonicalHistoryRootSelection, CanonicalRootCommit,
};
pub(crate) use state_history_runtime::load_state_history_rows;
pub(crate) use state::{
    load_canonical_change_row_by_id, load_commit_lineage_entry_by_id,
    load_exact_committed_state_row_at_version_head,
    load_exact_committed_state_row_at_version_head_with_executor,
    load_exact_committed_state_row_from_commit_with_executor, load_version_info_for_versions,
    CommitLineageEntry, CommitQueryExecutor, CommittedCanonicalChangeRow, ExactCommittedStateRow,
    ExactCommittedStateRowRequest, VersionInfo, VersionSnapshot,
};
pub(crate) use version_descriptors::{
    build_admin_version_source_sql, build_admin_version_source_sql_with_current_heads,
    find_version_id_by_name_with_backend, find_version_id_by_name_with_executor,
    load_all_version_descriptors_with_executor, load_version_descriptor_with_backend,
    version_exists_with_backend, version_exists_with_executor,
};
