use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, Query, SetExpr, Statement, Value as SqlValue, Visit, Visitor,
};

use crate::functions::LixFunctionProvider;
use crate::sql::planner::catalog::PlannerCatalogSnapshot;
use crate::sql::planner::types::ReadMaintenanceRequirements;
use crate::sql::planner::rewrite::write;
use crate::sql::planner::validate::validate_statement_output_parts;
use crate::sql::FileReadMaterializationScope;
use crate::sql::DetectedFileDomainChange;
use crate::{LixBackend, LixError, Value};

use crate::sql::planner::ir::logical::{
    LogicalReadOperator, LogicalReadSemantics, LogicalStatementOperation, LogicalStatementPlan,
    LogicalStatementSemantics, LogicalStatementStep,
};
use crate::sql::planner::rewrite::query::{
    collect_relation_names_via_walker, rewrite_query_with_backend_and_params_and_catalog,
};

pub(crate) async fn rewrite_statement_to_logical_plan_with_backend<P>(
    backend: &dyn LixBackend,
    catalog_snapshot: &PlannerCatalogSnapshot,
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
            let source_semantics = read_semantics_for_query(&query);
            let source_requirements = read_maintenance_requirements_for_query(&query);
            let semantics = LogicalStatementSemantics::QueryRead(source_semantics.clone());
            let rewritten = rewrite_query_with_backend_and_params_and_catalog(
                backend,
                *query,
                params,
                catalog_snapshot,
            )
            .await?;
            let rewritten_semantics = read_semantics_for_query(&rewritten);
            let requirements = merge_read_maintenance_requirements(
                source_requirements,
                read_maintenance_requirements_for_semantics(&rewritten_semantics),
            );
            LogicalStatementPlan::new(
                LogicalStatementOperation::QueryRead,
                semantics,
                vec![LogicalStatementStep::QueryRead],
                vec![Statement::Query(Box::new(rewritten.clone()))],
            )
            .with_maintenance_requirements(requirements)
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
                    let source_semantics = read_semantics_for_query(&query);
                    let source_requirements = read_maintenance_requirements_for_query(&query);
                    let semantics =
                        LogicalStatementSemantics::ExplainRead(source_semantics.clone());
                    let rewritten = rewrite_query_with_backend_and_params_and_catalog(
                        backend,
                        *query,
                        params,
                        catalog_snapshot,
                    )
                    .await?;
                    let rewritten_semantics = read_semantics_for_query(&rewritten);
                    let requirements = merge_read_maintenance_requirements(
                        source_requirements,
                        read_maintenance_requirements_for_semantics(&rewritten_semantics),
                    );
                    let explain_statement = Statement::Explain {
                        describe_alias,
                        analyze,
                        verbose,
                        query_plan,
                        estimate,
                        statement: Box::new(Statement::Query(Box::new(rewritten.clone()))),
                        format,
                        options,
                    };
                    LogicalStatementPlan::new(
                        LogicalStatementOperation::ExplainRead,
                        semantics,
                        vec![LogicalStatementStep::ExplainRead],
                        vec![explain_statement],
                    )
                    .with_maintenance_requirements(requirements)
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
                        vec![explain_statement],
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
            let emission_statements = rewrite_output.statements.clone();
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
                emission_statements,
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
            vec![other],
        ),
    };
    logical_plan.validate_plan_shape()?;
    Ok(logical_plan)
}

fn ensure_canonical_write_statement(statement: &Statement) -> Result<(), LixError> {
    match statement {
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => Ok(()),
        Statement::Query(query) if is_canonical_noop_query(query) => Ok(()),
        _ => Err(LixError {
            message: format!(
                "canonical write rewrite emitted non-canonical statement: {statement}"
            ),
        }),
    }
}

fn is_canonical_noop_query(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    if !select.from.is_empty() {
        return false;
    }
    let Some(selection) = select.selection.as_ref() else {
        return false;
    };
    is_constant_false_equality(selection)
}

fn is_constant_false_equality(expr: &Expr) -> bool {
    let Expr::BinaryOp { left, op, right } = expr else {
        return false;
    };
    if !matches!(op, sqlparser::ast::BinaryOperator::Eq) {
        return false;
    }
    let left_number = number_literal_value(left.as_ref());
    let right_number = number_literal_value(right.as_ref());
    matches!(
        (left_number.as_deref(), right_number.as_deref()),
        (Some("1"), Some("0")) | (Some("0"), Some("1"))
    )
}

fn number_literal_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(value_with_span) => match &value_with_span.value {
            SqlValue::Number(number, _) => Some(number.clone()),
            _ => None,
        },
        _ => None,
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
            "lix_file" => {
                operators.insert(LogicalReadOperator::File);
            }
            "lix_file_by_version" => {
                operators.insert(LogicalReadOperator::FileByVersion);
            }
            "lix_file_history" => {
                operators.insert(LogicalReadOperator::FileHistory);
            }
            _ if relation.starts_with("lix_")
                && relation.ends_with("_history")
                && relation != "lix_directory_history"
                && relation != "lix_file_history"
                && relation != "lix_state_history" =>
            {
                // Entity history views (e.g. lix_key_value_history) lower through
                // lix_state_history and require timeline maintenance before execution.
                operators.insert(LogicalReadOperator::StateHistory);
            }
            _ => {}
        }
    }
    LogicalReadSemantics::from_operators(operators)
}

fn read_maintenance_requirements_for_query(query: &Query) -> ReadMaintenanceRequirements {
    let semantics = read_semantics_for_query(query);
    let mut requirements = read_maintenance_requirements_for_semantics(&semantics);
    if requirements.requires_history_timeline_materialization {
        requirements.history_roots = collect_history_root_commit_literals(query);
    }
    requirements
}

fn read_maintenance_requirements_for_semantics(
    semantics: &LogicalReadSemantics,
) -> ReadMaintenanceRequirements {
    let mut requirements = ReadMaintenanceRequirements::default();

    if semantics.operators.contains(&LogicalReadOperator::FileByVersion)
        || semantics.operators.contains(&LogicalReadOperator::FileHistory)
    {
        requirements.file_materialization_scope = Some(FileReadMaterializationScope::AllVersions);
    } else if semantics.operators.contains(&LogicalReadOperator::File) {
        requirements.file_materialization_scope =
            Some(FileReadMaterializationScope::ActiveVersionOnly);
    }

    if semantics.operators.contains(&LogicalReadOperator::FileHistory) {
        requirements.requires_file_history_materialization = true;
        requirements.requires_history_timeline_materialization = true;
    }
    if semantics.operators.contains(&LogicalReadOperator::StateHistory) {
        requirements.requires_history_timeline_materialization = true;
    }

    requirements
}

fn collect_history_root_commit_literals(query: &Query) -> BTreeSet<String> {
    let mut collector = HistoryRootCommitLiteralCollector::default();
    let _ = query.visit(&mut collector);
    collector.roots
}

#[derive(Default)]
struct HistoryRootCommitLiteralCollector {
    roots: BTreeSet<String>,
}

impl Visitor for HistoryRootCommitLiteralCollector {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
                if is_history_root_commit_column(left) {
                    if let Some(root) = extract_string_literal(right) {
                        self.roots.insert(root);
                    }
                } else if is_history_root_commit_column(right) {
                    if let Some(root) = extract_string_literal(left) {
                        self.roots.insert(root);
                    }
                }
            }
            Expr::InList {
                expr,
                list,
                negated: false,
            } if is_history_root_commit_column(expr) => {
                for candidate in list {
                    if let Some(root) = extract_string_literal(candidate) {
                        self.roots.insert(root);
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn is_history_root_commit_column(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(ident) => is_history_root_commit_column_name(&ident.value),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|ident| is_history_root_commit_column_name(&ident.value))
            .unwrap_or(false),
        Expr::Nested(inner) => is_history_root_commit_column(inner),
        _ => false,
    }
}

fn is_history_root_commit_column_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("root_commit_id")
        || name.eq_ignore_ascii_case("lixcol_root_commit_id")
}

fn extract_string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(value) => match &value.value {
            SqlValue::SingleQuotedString(value) => Some(value.clone()),
            SqlValue::DoubleQuotedString(value) => Some(value.clone()),
            _ => None,
        },
        Expr::Nested(inner) => extract_string_literal(inner),
        _ => None,
    }
}

fn merge_read_maintenance_requirements(
    left: ReadMaintenanceRequirements,
    right: ReadMaintenanceRequirements,
) -> ReadMaintenanceRequirements {
    let file_materialization_scope = match (
        left.file_materialization_scope,
        right.file_materialization_scope,
    ) {
        (Some(FileReadMaterializationScope::AllVersions), _)
        | (_, Some(FileReadMaterializationScope::AllVersions)) => {
            Some(FileReadMaterializationScope::AllVersions)
        }
        (Some(FileReadMaterializationScope::ActiveVersionOnly), _)
        | (_, Some(FileReadMaterializationScope::ActiveVersionOnly)) => {
            Some(FileReadMaterializationScope::ActiveVersionOnly)
        }
        _ => None,
    };

    let mut history_roots = left.history_roots;
    history_roots.extend(right.history_roots);

    ReadMaintenanceRequirements {
        history_roots,
        file_materialization_scope,
        requires_file_history_materialization: left.requires_file_history_materialization
            || right.requires_file_history_materialization,
        requires_history_timeline_materialization: left.requires_history_timeline_materialization
            || right.requires_history_timeline_materialization,
    }
}

#[cfg(test)]
mod tests {
    use super::rewrite_statement_to_logical_plan_with_backend;
    use crate::functions::SystemFunctionProvider;
    use crate::sql::planner::catalog::PlannerCatalogSnapshot;
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
            &PlannerCatalogSnapshot::default(),
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
            &PlannerCatalogSnapshot::default(),
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
            &PlannerCatalogSnapshot::default(),
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
            &PlannerCatalogSnapshot::default(),
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
            &PlannerCatalogSnapshot::default(),
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
