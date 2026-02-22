use sqlparser::ast::Statement;

use crate::sql::{
    bind_sql_with_state, lower_statement, parse_single_query_with_dialect, PlaceholderState,
};
use crate::{LixBackend, LixError, QueryResult, Value};

use super::rewrite_read_query_with_backend_and_params;

pub(crate) async fn execute_rewritten_read_sql_with_state(
    backend: &dyn LixBackend,
    sql: &str,
    params: &[Value],
    placeholder_state: PlaceholderState,
    query_context: &str,
) -> Result<QueryResult, LixError> {
    let query =
        parse_single_query_with_dialect(sql, backend.dialect()).map_err(|error| LixError {
            message: format!("failed to parse {query_context}: {}", error.message),
        })?;
    let rewritten = rewrite_read_query_with_backend_and_params(backend, query, params).await?;
    let lowered = lower_statement(Statement::Query(Box::new(rewritten)), backend.dialect())?;
    let Statement::Query(lowered_query) = lowered else {
        return Err(LixError {
            message: format!("{query_context} rewrite expected query statement"),
        });
    };
    let bound = bind_sql_with_state(
        &lowered_query.to_string(),
        params,
        backend.dialect(),
        placeholder_state,
    )?;
    backend.execute(&bound.sql, &bound.params).await
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::{LixTransaction, SqlDialect};

    use super::*;

    #[derive(Default)]
    struct RecordingBackend {
        calls: Mutex<Vec<(String, Vec<Value>)>>,
        rows: Vec<Vec<Value>>,
    }

    #[async_trait::async_trait(?Send)]
    impl LixBackend for RecordingBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
            self.calls
                .lock()
                .expect("record execute calls")
                .push((sql.to_string(), params.to_vec()));
            Ok(QueryResult {
                rows: self.rows.clone(),
            })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Err(LixError {
                message: "test backend does not support transactions".to_string(),
            })
        }
    }

    #[tokio::test]
    async fn rewrites_and_executes_logical_view_query() {
        let backend = RecordingBackend {
            rows: vec![vec![Value::Text("v1".to_string())]],
            ..Default::default()
        };

        let result = execute_rewritten_read_sql_with_state(
            &backend,
            "SELECT version_id FROM lix_active_version WHERE version_id = $1",
            &[Value::Text("v1".to_string())],
            PlaceholderState::new(),
            "active version row loader query",
        )
        .await
        .expect("execute rewritten query");

        assert_eq!(result.rows, vec![vec![Value::Text("v1".to_string())]]);
        let calls = backend.calls.lock().expect("read execute calls");
        assert!(!calls.is_empty());
        let expected_params = vec![Value::Text("v1".to_string())];
        let (executed_sql, executed_params) = calls
            .iter()
            .find(|(_, params)| params == &expected_params)
            .expect("expected a rewritten read query call with bound filter params");
        let executed_sql_lower = executed_sql.to_ascii_lowercase();
        assert!(!executed_sql_lower.contains("from lix_active_version"));
        assert_eq!(executed_params, &expected_params);
    }

    #[tokio::test]
    async fn reports_query_context_for_non_select_sql() {
        let backend = RecordingBackend::default();

        let error = execute_rewritten_read_sql_with_state(
            &backend,
            "DELETE FROM lix_active_version",
            &[],
            PlaceholderState::new(),
            "active version row loader query",
        )
        .await
        .expect_err("non-select loader query should fail");

        assert!(error
            .message
            .contains("failed to parse active version row loader query"));
        assert!(error.message.contains("expected SELECT statement"));
    }
}
