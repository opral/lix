use crate::live_state::schema_access::{payload_column_name_for_schema, tracked_relation_name};
use crate::version::GLOBAL_VERSION_ID;
use crate::SqlDialect;

const COMMIT_GRAPH_NODE_TABLE: &str = "lix_internal_commit_graph_node";

pub(crate) fn build_exact_commit_depth_cte_sql(
    _dialect: SqlDialect,
    root_placeholder: &str,
    target_placeholder: &str,
    fallback_depth_placeholder: &str,
) -> String {
    let commit_edge_table = tracked_relation_name("lix_commit_edge");
    let edge_parent_expr = quote_ident(&payload_column_name("lix_commit_edge", "parent_id"));
    let edge_child_expr = quote_ident(&payload_column_name("lix_commit_edge", "child_id"));
    format!(
        "target_commit_depth AS ( \
           WITH RECURSIVE reachable(commit_id, depth) AS ( \
             SELECT {root_placeholder} AS commit_id, 0 AS depth \
             UNION ALL \
             SELECT \
               {edge_parent_expr} AS commit_id, \
               reachable.depth + 1 AS depth \
             FROM reachable \
             JOIN {commit_edge_table} edge \
               ON {edge_child_expr} = reachable.commit_id \
             WHERE edge.schema_key = 'lix_commit_edge' \
               AND edge.version_id = '{global_version}' \
               AND edge.is_tombstone = 0 \
               AND {edge_parent_expr} IS NOT NULL \
               AND reachable.depth < {fallback_depth_placeholder} \
           ) \
           SELECT COALESCE(( \
             SELECT MIN(depth) \
             FROM reachable \
             WHERE commit_id = {target_placeholder} \
           ), {fallback_depth_placeholder}) AS raw_depth \
         ), ",
        root_placeholder = root_placeholder,
        target_placeholder = target_placeholder,
        fallback_depth_placeholder = fallback_depth_placeholder,
        global_version = GLOBAL_VERSION_ID,
        edge_parent_expr = edge_parent_expr,
        edge_child_expr = edge_child_expr,
    )
}

pub(crate) fn build_commit_generation_seed_sql() -> String {
    let commit_table = tracked_relation_name("lix_commit");
    let commit_edge_table = tracked_relation_name("lix_commit_edge");
    let commit_edge_parent_id_column =
        quote_ident(&payload_column_name("lix_commit_edge", "parent_id"));
    let commit_edge_child_id_column =
        quote_ident(&payload_column_name("lix_commit_edge", "child_id"));
    format!(
        "WITH RECURSIVE \
           commits AS ( \
             SELECT entity_id AS commit_id \
             FROM {commit_table} \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
           ), \
           edges AS ( \
             SELECT \
               {commit_edge_parent_id_column} AS parent_id, \
               {commit_edge_child_id_column} AS child_id \
             FROM {commit_edge_table} \
             WHERE schema_key = 'lix_commit_edge' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND {commit_edge_parent_id_column} IS NOT NULL \
               AND {commit_edge_child_id_column} IS NOT NULL \
           ), \
           roots AS ( \
             SELECT c.commit_id \
             FROM commits c \
             LEFT JOIN edges e ON e.child_id = c.commit_id \
             WHERE e.child_id IS NULL \
           ), \
           walk(commit_id, generation) AS ( \
             SELECT r.commit_id, 0 AS generation \
             FROM roots r \
             UNION ALL \
             SELECT e.child_id, walk.generation + 1 AS generation \
             FROM walk \
             JOIN edges e ON e.parent_id = walk.commit_id \
           ) \
         INSERT INTO {table} (commit_id, generation) \
         SELECT commit_id, MAX(generation) AS generation \
         FROM walk \
         GROUP BY commit_id \
         ON CONFLICT (commit_id) DO UPDATE \
         SET generation = CASE \
           WHEN excluded.generation > {table}.generation THEN excluded.generation \
           ELSE {table}.generation \
         END",
        table = COMMIT_GRAPH_NODE_TABLE,
        global_version = GLOBAL_VERSION_ID,
        commit_edge_parent_id_column = commit_edge_parent_id_column,
        commit_edge_child_id_column = commit_edge_child_id_column,
    )
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn payload_column_name(schema_key: &str, property_name: &str) -> String {
    payload_column_name_for_schema(schema_key, None, property_name).unwrap_or_else(|error| {
        panic!(
            "builtin live schema '{schema_key}' must include '{property_name}': {}",
            error.description
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_commit_depth_sql_targets_commit_edge_live_rows() {
        let sql = build_exact_commit_depth_cte_sql(SqlDialect::Sqlite, "?1", "?2", "?3");

        assert!(sql.contains("lix_commit_edge"));
        assert!(sql.contains("target_commit_depth AS"));
        assert!(sql.contains("reachable.depth < ?3"));
    }

    #[test]
    fn commit_generation_seed_sql_targets_commit_graph_node_table() {
        let sql = build_commit_generation_seed_sql();
        let commit_table = tracked_relation_name("lix_commit");
        let commit_edge_table = tracked_relation_name("lix_commit_edge");

        assert!(sql.contains("INSERT INTO lix_internal_commit_graph_node"));
        assert!(sql.contains(&format!("FROM {commit_table}")));
        assert!(sql.contains(&format!("FROM {commit_edge_table}")));
    }
}
