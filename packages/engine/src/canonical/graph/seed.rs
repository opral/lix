use crate::canonical::graph::COMMIT_GRAPH_NODE_TABLE;
use crate::SqlDialect;

pub(crate) fn build_commit_generation_seed_sql(dialect: SqlDialect) -> String {
    let (parent_join_sql, parent_value_expr) = json_array_text_join_sql(
        dialect,
        "commit_headers.commit_snapshot_content",
        "parent_commit_ids",
        "parent_rows",
        "parent_id",
    );
    format!(
        "WITH RECURSIVE \
           canonical_commit_headers AS ( \
             SELECT \
               commit_change.entity_id AS commit_id, \
               commit_snapshot.content AS commit_snapshot_content \
             FROM lix_internal_change commit_change \
             LEFT JOIN lix_internal_snapshot commit_snapshot \
               ON commit_snapshot.id = commit_change.snapshot_id \
             WHERE commit_change.schema_key = 'lix_commit' \
               AND commit_snapshot.content IS NOT NULL \
           ), \
           edges AS ( \
             SELECT \
               {parent_value_expr} AS parent_id, \
               commit_headers.commit_id AS child_id \
             FROM canonical_commit_headers commit_headers \
             {parent_join_sql} \
             WHERE {parent_value_expr} IS NOT NULL \
           ), \
           roots AS ( \
             SELECT c.commit_id \
             FROM canonical_commit_headers c \
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
        parent_join_sql = parent_join_sql,
        parent_value_expr = parent_value_expr,
    )
}

fn json_array_text_join_sql(
    dialect: SqlDialect,
    json_column: &str,
    field: &str,
    alias: &str,
    value_column: &str,
) -> (String, String) {
    match dialect {
        SqlDialect::Sqlite => (
            format!("JOIN json_each({json_column}, '$.{field}') AS {alias}"),
            format!("{alias}.value"),
        ),
        SqlDialect::Postgres => (
            format!(
                "JOIN LATERAL jsonb_array_elements_text(CAST({json_column} AS JSONB) -> '{field}') AS {alias}({value_column}) ON TRUE"
            ),
            format!("{alias}.{value_column}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::init as canonical_init;
    use crate::test_support::{
        init_test_backend_core, seed_canonical_change_row, CanonicalChangeSeed, TestSqliteBackend,
    };
    use crate::{LixBackend, Value};

    #[test]
    fn commit_generation_seed_sql_targets_canonical_commit_headers() {
        let sql = build_commit_generation_seed_sql(SqlDialect::Sqlite);

        assert!(sql.contains("INSERT INTO lix_internal_commit_graph_node"));
        assert!(sql.contains("FROM lix_internal_change commit_change"));
        assert!(sql.contains("LEFT JOIN lix_internal_snapshot commit_snapshot"));
        assert!(!sql.contains("lix_internal_live_v1_lix_commit"));
        assert!(!sql.contains("lix_internal_live_v1_lix_commit_edge"));
    }

    #[tokio::test]
    async fn commit_generation_seed_sql_rebuilds_without_live_commit_mirrors() {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");
        canonical_init(&backend)
            .await
            .expect("canonical init should succeed");
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "commit-change-1",
                entity_id: "commit-1",
                schema_key: "lix_commit",
                schema_version: "1",
                file_id: "lix",
                plugin_key: "lix",
                snapshot_id: "snapshot-1",
                snapshot_content: Some(
                    r#"{"id":"commit-1","change_set_id":"cs-1","parent_commit_ids":[],"change_ids":[]}"#,
                ),
                metadata: None,
                created_at: "2026-03-30T10:00:00Z",
            },
        )
        .await
        .expect("canonical commit row should seed");
        backend
            .execute("DROP TABLE IF EXISTS lix_internal_live_v1_lix_commit", &[])
            .await
            .expect("dropping live commit mirror should succeed");
        backend
            .execute(
                "DROP TABLE IF EXISTS lix_internal_live_v1_lix_commit_edge",
                &[],
            )
            .await
            .expect("dropping live commit-edge mirror should succeed");

        backend
            .execute(
                &build_commit_generation_seed_sql(crate::SqlDialect::Sqlite),
                &[],
            )
            .await
            .expect("canonical graph seed should succeed without live mirrors");

        let result = backend
            .execute(
                "SELECT commit_id, generation FROM lix_internal_commit_graph_node",
                &[],
            )
            .await
            .expect("graph node query should succeed");
        assert_eq!(
            result.rows,
            vec![vec![Value::Text("commit-1".to_string()), Value::Integer(0)]]
        );
    }
}
