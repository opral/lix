mod context;
mod types;
mod walker;

/// Test-only engagement counters for the reachable-node cache.
#[cfg(test)]
pub(crate) use context::reachable_census;
pub(crate) use context::{CommitGraphContext, CommitGraphStoreReader, canonical_commit_change};
pub(crate) use types::{
    CommitGraphChange, CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest,
    CommitGraphEdge, CommitGraphHistory, CommitGraphNode, CommitGraphReader,
    ReachableCommitGraphNode, commit_edges,
};
