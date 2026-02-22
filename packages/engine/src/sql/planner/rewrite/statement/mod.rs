use sqlparser::ast::Statement;

use crate::functions::LixFunctionProvider;
use crate::sql::pipeline::registry::statement_rules;
use crate::sql::pipeline::rules::statement::apply_backend_rule;
use crate::sql::pipeline::validator::validate_statement_output;
use crate::sql::DetectedFileDomainChange;
use crate::{LixBackend, LixError, Value};

use crate::sql::planner::ir::logical::LogicalStatementPlan;

pub(crate) async fn rewrite_statement_to_logical_plan_with_backend<P>(
    backend: &dyn LixBackend,
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    provider: &mut P,
    detected_file_domain_changes: &[DetectedFileDomainChange],
) -> Result<LogicalStatementPlan, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    for rule in statement_rules() {
        // Keep this large async rewrite future on the heap to prevent stack overflows.
        let rewrite_output = Box::pin(apply_backend_rule(
            *rule,
            backend,
            statement.clone(),
            params,
            writer_key,
            provider,
            detected_file_domain_changes,
        ))
        .await?;
        if let Some(rewrite_output) = rewrite_output {
            validate_statement_output(&rewrite_output)?;
            return Ok(LogicalStatementPlan::from_rewrite_output(rewrite_output));
        }
    }

    Err(LixError {
        message: "statement backend rewrite engine could not match statement rule".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::rewrite_statement_to_logical_plan_with_backend;
    use crate::functions::SystemFunctionProvider;
    use crate::sql::parse_sql_statements_with_dialect;
    use crate::{LixBackend, LixError, LixTransaction, QueryResult, SqlDialect, Value};
    use sqlparser::ast::Statement;

    struct UnexpectedBackendCall;

    #[async_trait::async_trait(?Send)]
    impl LixBackend for UnexpectedBackendCall {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, _: &str, _: &[Value]) -> Result<QueryResult, LixError> {
            Err(LixError {
                message: "planner rewrite attempted backend execution".to_string(),
            })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Err(LixError {
                message: "planner rewrite should not open transactions".to_string(),
            })
        }
    }

    fn parse_single_statement(sql: &str) -> Statement {
        let mut statements =
            parse_sql_statements_with_dialect(sql, SqlDialect::Sqlite).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        statements.remove(0)
    }

    #[tokio::test]
    async fn rewrites_query_statements_via_rule_dispatch() {
        let backend = UnexpectedBackendCall;
        let mut provider = SystemFunctionProvider;
        let statement = parse_single_statement("SELECT 1");

        let plan = rewrite_statement_to_logical_plan_with_backend(
            &backend,
            statement,
            &[],
            None,
            &mut provider,
            &[],
        )
        .await
        .expect("rewrite query statement");

        assert_eq!(plan.statements.len(), 1);
        assert!(matches!(plan.statements[0].statement, Statement::Query(_)));
    }

    #[tokio::test]
    async fn rewrites_explain_statements_via_rule_dispatch() {
        let backend = UnexpectedBackendCall;
        let mut provider = SystemFunctionProvider;
        let statement = parse_single_statement("EXPLAIN SELECT 1");

        let plan = rewrite_statement_to_logical_plan_with_backend(
            &backend,
            statement,
            &[],
            None,
            &mut provider,
            &[],
        )
        .await
        .expect("rewrite explain statement");

        assert_eq!(plan.statements.len(), 1);
        assert!(matches!(
            plan.statements[0].statement,
            Statement::Explain { .. }
        ));
    }

    #[tokio::test]
    async fn rewrites_write_statements_via_canonical_rule_dispatch() {
        let backend = UnexpectedBackendCall;
        let mut provider = SystemFunctionProvider;
        let statement = parse_single_statement("UPDATE test_table SET value = 1");

        let plan = rewrite_statement_to_logical_plan_with_backend(
            &backend,
            statement,
            &[],
            None,
            &mut provider,
            &[],
        )
        .await
        .expect("rewrite update statement");

        assert_eq!(plan.statements.len(), 1);
        assert!(matches!(plan.statements[0].statement, Statement::Update(_)));
    }

    #[tokio::test]
    async fn rewrites_passthrough_statements_via_rule_dispatch() {
        let backend = UnexpectedBackendCall;
        let mut provider = SystemFunctionProvider;
        let statement = parse_single_statement("CREATE TABLE test_table (id TEXT)");

        let plan = rewrite_statement_to_logical_plan_with_backend(
            &backend,
            statement,
            &[],
            None,
            &mut provider,
            &[],
        )
        .await
        .expect("rewrite passthrough statement");

        assert_eq!(plan.statements.len(), 1);
        assert!(matches!(
            plan.statements[0].statement,
            Statement::CreateTable(_)
        ));
    }
}
