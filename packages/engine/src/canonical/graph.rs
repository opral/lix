//! Commit-graph helpers for canonical history.
//!
//! The commit graph is a canonical projection over canonical change facts.
//! Materialized graph indexes may be rebuilt from those facts and are not an
//! independent source of truth.

use super::graph_index::build_commit_generation_seed_sql as graph_build_commit_generation_seed_sql;

pub(crate) use super::graph_index::COMMIT_GRAPH_NODE_TABLE;

pub(crate) fn build_commit_generation_seed_sql() -> String {
    graph_build_commit_generation_seed_sql()
}
