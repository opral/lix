use std::collections::BTreeSet;

use sqlparser::ast::{Query, Statement};

use crate::functions::LixFunctionProvider;
use crate::sql::planner::rewrite::write;
use crate::sql::planner::validate::validate_statement_output_parts;
use crate::sql::DetectedFileDomainChange;
use crate::{LixBackend, LixError, Value};

use crate::sql::planner::ir::logical::{
    LogicalReadOperator, LogicalReadSemantics, LogicalStatementOperation, LogicalStatementPlan,
    LogicalStatementSemantics, LogicalStatementStep,
};
use crate::sql::planner::rewrite::query::{
    collect_relation_names_via_walker, rewrite_query_with_backend_and_params,
};

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
    let logical_plan = match statement {
        Statement::Query(query) => {
            let semantics = LogicalStatementSemantics::QueryRead(read_semantics_for_query(&query));
            let rewritten = rewrite_query_with_backend_and_params(backend, *query, params).await?;
            LogicalStatementPlan::new(
                LogicalStatementOperation::QueryRead,
                semantics,
                vec![LogicalStatementStep::QueryRead],
                vec![rewritten.to_string()],
            )
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
            match *statement {
                Statement::Query(query) => {
                    let semantics =
                        LogicalStatementSemantics::ExplainRead(read_semantics_for_query(&query));
                    let rewritten =
                        rewrite_query_with_backend_and_params(backend, *query, params).await?;
                    let explain_statement = Statement::Explain {
                        describe_alias,
                        analyze,
                        verbose,
                        query_plan,
                        estimate,
                        statement: Box::new(Statement::Query(Box::new(rewritten))),
                        format,
                        options,
                    };
                    LogicalStatementPlan::new(
                        LogicalStatementOperation::ExplainRead,
                        semantics,
                        vec![LogicalStatementStep::ExplainRead],
                        vec![explain_statement.to_string()],
                    )
                }
                other => {
                    let explain_statement = Statement::Explain {
                        describe_alias,
                        analyze,
                        verbose,
                        query_plan,
                        estimate,
                        statement: Box::new(other),
                        format,
                        options,
                    };
                    LogicalStatementPlan::new(
                        LogicalStatementOperation::Passthrough,
                        LogicalStatementSemantics::Passthrough,
                        vec![LogicalStatementStep::Passthrough],
                        vec![explain_statement.to_string()],
                    )
                }
            }
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
            let emission_sql = rewrite_output
                .statements
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let planned_statements = rewrite_output
                .statements
                .iter()
                .map(|statement| {
                    ensure_canonical_write_statement(statement)
                        .map(|_| LogicalStatementStep::CanonicalWrite)
                })
                .collect::<Result<Vec<_>, _>>()?;
            LogicalStatementPlan::new(
                LogicalStatementOperation::CanonicalWrite,
                LogicalStatementSemantics::CanonicalWrite,
                planned_statements,
                emission_sql,
            )
            .with_rewrite_metadata(
                rewrite_output.params,
                rewrite_output.registrations,
                rewrite_output.postprocess,
                rewrite_output.mutations,
                rewrite_output.update_validations,
            )
        }
        other => LogicalStatementPlan::new(
            LogicalStatementOperation::Passthrough,
            LogicalStatementSemantics::Passthrough,
            vec![LogicalStatementStep::Passthrough],
            vec![other.to_string()],
        ),
    };
    logical_plan.validate_plan_shape()?;
    Ok(logical_plan)
}

fn ensure_canonical_write_statement(statement: &Statement) -> Result<(), LixError> {
    match statement {
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => Ok(()),
        _ => Err(LixError {
            message: "canonical write rewrite emitted non-canonical statement".to_string(),
        }),
    }
}

fn read_semantics_for_query(query: &Query) -> LogicalReadSemantics {
    let mut operators = BTreeSet::new();
    for relation in collect_relation_names_via_walker(query) {
        match relation.as_str() {
            "lix_state" => {
                operators.insert(LogicalReadOperator::State);
            }
            "lix_state_by_version" => {
                operators.insert(LogicalReadOperator::StateByVersion);
            }
            "lix_state_history" => {
                operators.insert(LogicalReadOperator::StateHistory);
            }
            _ => {}
        }
    }
    LogicalReadSemantics::from_operators(operators)
}

#[cfg(test)]
mod tests {
    use super::rewrite_statement_to_logical_plan_with_backend;
    use crate::functions::SystemFunctionProvider;
    use crate::sql::parse_sql_statements_with_dialect;
    use crate::sql::planner::ir::logical::{
        LogicalReadOperator, LogicalStatementOperation, LogicalStatementSemantics,
        LogicalStatementStep,
    };
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
        assert!(matches!(
            plan.planned_statements[0],
            LogicalStatementStep::QueryRead
        ));
        let LogicalStatementSemantics::QueryRead(read) = plan.semantics else {
            panic!("expected query read semantics");
        };
        assert!(read.operators.is_empty());
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
            LogicalStatementStep::ExplainRead
        ));
        let LogicalStatementSemantics::ExplainRead(read) = plan.semantics else {
            panic!("expected explain read semantics");
        };
        assert!(read.operators.is_empty());
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
        assert!(matches!(
            plan.planned_statements[0],
            LogicalStatementStep::CanonicalWrite
        ));
        assert_eq!(plan.semantics, LogicalStatementSemantics::CanonicalWrite);
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
            LogicalStatementStep::Passthrough
        ));
        assert_eq!(plan.semantics, LogicalStatementSemantics::Passthrough);
    }

    #[tokio::test]
    async fn tracks_state_read_semantic_operators() {
        struct QueryRewriteBackend;

        #[async_trait::async_trait(?Send)]
        impl LixBackend for QueryRewriteBackend {
            fn dialect(&self) -> SqlDialect {
                SqlDialect::Sqlite
            }

            async fn execute(&self, _: &str, _: &[Value]) -> Result<QueryResult, LixError> {
                Ok(QueryResult { rows: Vec::new() })
            }

            async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
                Err(LixError {
                    message: "planner rewrite should not open transactions".to_string(),
                })
            }
        }

        let backend = QueryRewriteBackend;
        let mut provider = SystemFunctionProvider;
        let statement = parse_single_statement(
            "SELECT COUNT(*) FROM lix_state_by_version \
             UNION ALL SELECT COUNT(*) FROM lix_state_history",
        );

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

        let LogicalStatementSemantics::QueryRead(read) = plan.semantics else {
            panic!("expected query read semantics");
        };
        assert!(read
            .operators
            .contains(&LogicalReadOperator::StateByVersion));
        assert!(read.operators.contains(&LogicalReadOperator::StateHistory));
    }
}
