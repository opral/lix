use sqlparser::ast::Statement;

use crate::functions::LixFunctionProvider;
use crate::sql::pipeline::query_engine::rewrite_read_query_with_backend_and_params;
use crate::sql::pipeline::validator::{validate_final_read_query, validate_statement_output_parts};
use crate::sql::planner::rewrite::write;
use crate::sql::DetectedFileDomainChange;
use crate::{LixBackend, LixError, Value};

use crate::sql::planner::ir::logical::{LogicalStatementOperation, LogicalStatementPlan};

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
    match statement {
        Statement::Query(query) => {
            let rewritten =
                rewrite_read_query_with_backend_and_params(backend, *query, params).await?;
            validate_final_read_query(&rewritten)?;
            Ok(LogicalStatementPlan::new(
                LogicalStatementOperation::QueryRead,
                vec![Statement::Query(Box::new(rewritten))],
            ))
        }
        Statement::Explain {
            describe_alias,
            analyze,
            verbose,
            query_plan,
            estimate,
            statement,
            format,
            options,
        } => {
            let rewritten_statement = match *statement {
                Statement::Query(query) => {
                    let rewritten =
                        rewrite_read_query_with_backend_and_params(backend, *query, params).await?;
                    validate_final_read_query(&rewritten)?;
                    Statement::Query(Box::new(rewritten))
                }
                other => other,
            };

            Ok(LogicalStatementPlan::new(
                LogicalStatementOperation::ExplainRead,
                vec![Statement::Explain {
                    describe_alias,
                    analyze,
                    verbose,
                    query_plan,
                    estimate,
                    statement: Box::new(rewritten_statement),
                    format,
                    options,
                }],
            ))
        }
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
            let Some(rewrite_output) = write::rewrite_backend_statement(
                backend,
                statement,
                params,
                writer_key,
                provider,
                detected_file_domain_changes,
            )
            .await?
            else {
                return Err(LixError {
                    message: "planner canonical write rewrite produced no output".to_string(),
                });
            };

            validate_statement_output_parts(
                &rewrite_output.statements,
                &rewrite_output.registrations,
                rewrite_output.postprocess.as_ref(),
                &rewrite_output.mutations,
                &rewrite_output.update_validations,
            )?;
            Ok(LogicalStatementPlan::new(
                LogicalStatementOperation::CanonicalWrite,
                rewrite_output.statements,
            )
            .with_rewrite_metadata(
                rewrite_output.params,
                rewrite_output.registrations,
                rewrite_output.postprocess,
                rewrite_output.mutations,
                rewrite_output.update_validations,
            ))
        }
        other => Ok(LogicalStatementPlan::new(
            LogicalStatementOperation::Passthrough,
            vec![other],
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::rewrite_statement_to_logical_plan_with_backend;
    use crate::functions::SystemFunctionProvider;
    use crate::sql::parse_sql_statements_with_dialect;
    use crate::sql::planner::ir::logical::LogicalStatementOperation;
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

        assert_eq!(plan.operation, LogicalStatementOperation::QueryRead);
        assert_eq!(plan.planned_statements.len(), 1);
        assert!(matches!(plan.planned_statements[0], Statement::Query(_)));
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

        assert_eq!(plan.operation, LogicalStatementOperation::ExplainRead);
        assert_eq!(plan.planned_statements.len(), 1);
        assert!(matches!(
            plan.planned_statements[0],
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

        assert_eq!(plan.operation, LogicalStatementOperation::CanonicalWrite);
        assert_eq!(plan.planned_statements.len(), 1);
        assert!(matches!(plan.planned_statements[0], Statement::Update(_)));
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

        assert_eq!(plan.operation, LogicalStatementOperation::Passthrough);
        assert_eq!(plan.planned_statements.len(), 1);
        assert!(matches!(
            plan.planned_statements[0],
            Statement::CreateTable(_)
        ));
    }
}
