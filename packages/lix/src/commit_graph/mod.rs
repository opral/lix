mod context;
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) mod scope_digest_census;
mod types;
mod walker;

pub(crate) use context::{CommitGraphContext, CommitGraphStoreReader, canonical_commit_change};
#[cfg(test)]
pub(crate) use scope_digest_census::scope_digest_census;
#[cfg(test)]
pub(crate) use scope_digest_census::{
    reset_thread_scope_digest_census, thread_scope_digest_census,
};
pub(crate) use types::{
    CommitGraphChange, CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest,
    CommitGraphHistory, CommitGraphNode, CommitGraphReader,
    ReachableCommitGraphNode, commit_edges,
};
