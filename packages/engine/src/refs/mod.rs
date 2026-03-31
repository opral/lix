//! Replica-local committed-head selection boundary.
//!
//! `refs` owns version-head selection and root resolution over canonical graph
//! facts.

pub(crate) mod roots;
pub(crate) mod version_heads;

pub(crate) use roots::{
    load_all_version_head_commit_ids, resolve_history_root_facts_with_backend, HistoryRootFacts,
    HistoryRootTraversal, RootCommitResolutionRequest, RootCommitScope, RootLineageScope,
    RootVersionScope,
};
pub(crate) use version_heads::{
    load_committed_version_head_commit_id, load_committed_version_ref_with_backend,
    load_committed_version_ref_with_executor, load_current_committed_version_frontier_with_backend,
    load_current_committed_version_frontier_with_executor, VersionRefRow,
};
