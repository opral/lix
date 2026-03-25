use super::graph_index::{
    build_commit_generation_seed_sql as graph_build_commit_generation_seed_sql,
    build_reachable_commits_for_root_cte_sql as graph_build_reachable_commits_for_root_cte_sql,
    build_reachable_commits_from_requested_cte_sql as graph_build_reachable_commits_from_requested_cte_sql,
};

pub(crate) use super::graph_index::COMMIT_GRAPH_NODE_TABLE;

pub(crate) fn build_commit_generation_seed_sql() -> String {
    graph_build_commit_generation_seed_sql()
}

pub(crate) fn build_reachable_commits_for_root_cte_sql(
    dialect: crate::SqlDialect,
    root_commit_id: &str,
    start_depth: i64,
    end_depth: i64,
) -> String {
    graph_build_reachable_commits_for_root_cte_sql(
        dialect,
        root_commit_id,
        start_depth,
        end_depth,
    )
}

pub(crate) fn build_reachable_commits_from_requested_cte_sql(
    dialect: crate::SqlDialect,
    requested_commits_cte_name: &str,
    max_depth: i64,
) -> String {
    graph_build_reachable_commits_from_requested_cte_sql(
        dialect,
        requested_commits_cte_name,
        max_depth,
    )
}
