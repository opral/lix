#![allow(dead_code)]

use sqlparser::ast::Query;

use crate::engine::sql2::planning::rewrite_engine::pipeline::query_engine;
use crate::{LixBackend, LixError, Value};

pub(crate) use query_engine::ReadRewriteSession;

pub(crate) fn rewrite_read_query(query: Query) -> Result<Query, LixError> {
    query_engine::rewrite_read_query(query)
}

pub(crate) async fn rewrite_read_query_with_backend_and_params(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Query, LixError> {
    query_engine::rewrite_read_query_with_backend_and_params(backend, query, params).await
}

pub(crate) async fn rewrite_read_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
) -> Result<Query, LixError> {
    query_engine::rewrite_read_query_with_backend(backend, query).await
}

pub(crate) async fn rewrite_read_query_with_backend_and_params_in_session(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
    session: &mut ReadRewriteSession,
) -> Result<Query, LixError> {
    query_engine::rewrite_read_query_with_backend_and_params_in_session(
        backend, query, params, session,
    )
    .await
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::rewrite_read_query;
    use crate::engine::sql2::planning::rewrite_engine::pipeline::validator::validate_no_unresolved_logical_read_views;

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            sqlparser::ast::Statement::Query(query) => *query,
            other => panic!("expected query, got {other:?}"),
        }
    }

    #[test]
    fn validator_rejects_unresolved_logical_views() {
        let query = parse_query("SELECT version_id FROM lix_active_version");
        let err =
            validate_no_unresolved_logical_read_views(&query).expect_err("validator should fail");
        assert!(err.message.contains("lix_active_version"));
    }

    #[test]
    fn rewrite_engine_rewrites_nested_lix_active_version() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM lix_state_by_version \
             WHERE schema_key = 'bench_schema' \
               AND version_id IN (SELECT version_id FROM lix_active_version)",
        );
        let rewritten = rewrite_read_query(query).expect("rewrite should succeed");
        let sql = rewritten.to_string();
        assert!(!sql.contains("FROM lix_active_version"));
        assert!(sql.contains("lix_internal_state_vtable"));
    }

    #[test]
    fn rewrite_engine_rewrites_nested_lix_active_account() {
        let query = parse_query(
            "SELECT 1 \
             WHERE EXISTS (SELECT 1 FROM lix_active_account WHERE account_id = 'a')",
        );
        let rewritten = rewrite_read_query(query).expect("rewrite should succeed");
        let sql = rewritten.to_string();
        assert!(!sql.contains("FROM lix_active_account"));
        assert!(sql.contains("lix_internal_state_vtable"));
    }

    #[test]
    fn rewrite_engine_rewrites_logical_views_in_cte_derived_and_scalar_subqueries() {
        let query = parse_query(
            "WITH current_version AS ( \
                SELECT version_id FROM lix_active_version \
            ) \
            SELECT COUNT(*) FROM ( \
                SELECT schema_key FROM lix_state_by_version \
                WHERE version_id = (SELECT version_id FROM current_version LIMIT 1) \
            ) AS scoped \
            WHERE EXISTS ( \
                SELECT 1 FROM lix_active_account WHERE account_id = 'a' \
            )",
        );

        let rewritten = rewrite_read_query(query).expect("rewrite should succeed");
        validate_no_unresolved_logical_read_views(&rewritten)
            .expect("rewritten query should not contain logical view names");
    }
}
