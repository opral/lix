//! Commit-graph helpers for canonical history.
//!
//! The commit graph is a canonical projection over canonical change facts.
//! Materialized graph indexes may be rebuilt from those facts and are not an
//! independent source of truth.

pub(crate) mod index;
pub(crate) mod lineage;
pub(crate) mod seed;

pub(crate) use index::{
    build_commit_graph_node_prepared_batch, resolve_commit_graph_node_write_rows_with_executor,
    COMMIT_GRAPH_NODE_TABLE,
};
pub(crate) use lineage::{
    build_version_commit_depth_map, build_version_head_map, collect_commit_edges,
    min_depth_by_commit, VersionCommitDepthMap, VersionHeadMap,
};
pub(crate) use seed::build_commit_generation_seed_sql;
