use std::collections::BTreeSet;

use sqlparser::ast::{AnalyzeFormatKind, DescribeAlias, Query, Statement, UtilityOption};

use crate::sql::types::{MutationRow, PostprocessPlan, SchemaRegistration, UpdateValidationPlan};
use crate::LixError;
use crate::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogicalStatementOperation {
    QueryRead,
    ExplainRead,
    CanonicalWrite,
    Passthrough,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum LogicalReadOperator {
    State,
    StateByVersion,
    StateHistory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogicalReadSemantics {
    pub(crate) operators: BTreeSet<LogicalReadOperator>,
}

impl LogicalReadSemantics {
    pub(crate) fn empty() -> Self {
        Self {
            operators: BTreeSet::new(),
        }
    }

    pub(crate) fn from_operators(operators: BTreeSet<LogicalReadOperator>) -> Self {
        Self { operators }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LogicalStatementSemantics {
    QueryRead(LogicalReadSemantics),
    ExplainRead(LogicalReadSemantics),
    CanonicalWrite,
    Passthrough,
}

#[derive(Debug, Clone)]
pub(crate) enum LogicalStatementStep {
    Query(Query),
    ExplainRead(LogicalExplainRead),
    Statement(Statement),
}

#[derive(Debug, Clone)]
pub(crate) struct LogicalExplainRead {
    pub(crate) describe_alias: DescribeAlias,
    pub(crate) analyze: bool,
    pub(crate) verbose: bool,
    pub(crate) query_plan: bool,
    pub(crate) estimate: bool,
    pub(crate) format: Option<AnalyzeFormatKind>,
    pub(crate) options: Option<Vec<UtilityOption>>,
    pub(crate) query: Query,
}

impl LogicalExplainRead {
    pub(crate) fn into_statement(self) -> Statement {
        Statement::Explain {
            describe_alias: self.describe_alias,
            analyze: self.analyze,
            verbose: self.verbose,
            query_plan: self.query_plan,
            estimate: self.estimate,
            statement: Box::new(Statement::Query(Box::new(self.query))),
            format: self.format,
            options: self.options,
        }
    }
}

impl LogicalStatementStep {
    pub(crate) fn as_statement(&self) -> Statement {
        match self {
            Self::Query(query) => Statement::Query(Box::new(query.clone())),
            Self::ExplainRead(explain_read) => explain_read.clone().into_statement(),
            Self::Statement(statement) => statement.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LogicalStatementPlan {
    pub(crate) operation: LogicalStatementOperation,
    pub(crate) semantics: LogicalStatementSemantics,
    pub(crate) planned_statements: Vec<LogicalStatementStep>,
    pub(crate) appended_params: Vec<Value>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

impl LogicalStatementPlan {
    pub(crate) fn new(
        operation: LogicalStatementOperation,
        semantics: LogicalStatementSemantics,
        planned_statements: Vec<LogicalStatementStep>,
    ) -> Self {
        Self {
            operation,
            semantics,
            planned_statements,
            appended_params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        }
    }

    pub(crate) fn with_rewrite_metadata(
        mut self,
        appended_params: Vec<Value>,
        registrations: Vec<SchemaRegistration>,
        postprocess: Option<PostprocessPlan>,
        mutations: Vec<MutationRow>,
        update_validations: Vec<UpdateValidationPlan>,
    ) -> Self {
        self.appended_params = appended_params;
        self.registrations = registrations;
        self.postprocess = postprocess;
        self.mutations = mutations;
        self.update_validations = update_validations;
        self
    }

    pub(crate) fn validate_plan_shape(&self) -> Result<(), LixError> {
        if self.planned_statements.is_empty() {
            return Err(LixError {
                message: "logical plan has no planned statements".to_string(),
            });
        }

        match (self.operation, &self.semantics) {
            (LogicalStatementOperation::QueryRead, LogicalStatementSemantics::QueryRead(_)) => {
                let has_non_query = self
                    .planned_statements
                    .iter()
                    .any(|step| !matches!(step, LogicalStatementStep::Query(_)));
                if has_non_query {
                    return Err(LixError {
                        message: "query read plans may only contain query steps".to_string(),
                    });
                }
            }
            (LogicalStatementOperation::ExplainRead, LogicalStatementSemantics::ExplainRead(_)) => {
                let has_non_explain = self
                    .planned_statements
                    .iter()
                    .any(|step| !matches!(step, LogicalStatementStep::ExplainRead(_)));
                if has_non_explain {
                    return Err(LixError {
                        message: "explain plans may only contain explain read steps".to_string(),
                    });
                }
            }
            (LogicalStatementOperation::CanonicalWrite, LogicalStatementSemantics::CanonicalWrite) => {
                let has_query = self
                    .planned_statements
                    .iter()
                    .any(|step| matches!(step, LogicalStatementStep::Query(_)));
                let has_explain = self
                    .planned_statements
                    .iter()
                    .any(|step| matches!(step, LogicalStatementStep::ExplainRead(_)));
                if has_query || has_explain {
                    return Err(LixError {
                        message: "canonical write plans may not contain read steps".to_string(),
                    });
                }
            }
            (LogicalStatementOperation::Passthrough, LogicalStatementSemantics::Passthrough) => {
                let has_read = self
                    .planned_statements
                    .iter()
                    .any(|step| matches!(step, LogicalStatementStep::Query(_)));
                if has_read {
                    return Err(LixError {
                        message: "passthrough plans may not contain query plan steps".to_string(),
                    });
                }
            }
            _ => {
                return Err(LixError {
                    message: "logical statement operation and semantics are inconsistent".to_string(),
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parse_sql_statements_with_dialect;
    use crate::{LixError, SqlDialect};

    fn query_from_sql(sql: &str) -> Query {
        let mut statements =
            parse_sql_statements_with_dialect(sql, SqlDialect::Sqlite).expect("parse statements");
        assert_eq!(statements.len(), 1);
        let Statement::Query(query) = statements.remove(0) else {
            panic!("expected single query statement");
        };
        *query
    }

    fn statement_from_sql(sql: &str) -> Statement {
        let mut statements =
            parse_sql_statements_with_dialect(sql, SqlDialect::Sqlite).expect("parse statements");
        assert_eq!(statements.len(), 1);
        statements.remove(0)
    }

    #[test]
    fn validates_query_read_plan_shape() {
        let plan = LogicalStatementPlan::new(
            LogicalStatementOperation::QueryRead,
            LogicalStatementSemantics::QueryRead(LogicalReadSemantics::empty()),
            vec![LogicalStatementStep::Query(query_from_sql("SELECT 1"))],
        );

        assert!(plan.validate_plan_shape().is_ok());
    }

    #[test]
    fn rejects_inconsistent_operation_and_semantics() {
        let plan = LogicalStatementPlan::new(
            LogicalStatementOperation::QueryRead,
            LogicalStatementSemantics::Passthrough,
            vec![LogicalStatementStep::Statement(statement_from_sql(
                "CREATE TABLE t (id INTEGER)",
            ))],
        );

        assert!(matches!(plan.validate_plan_shape(), Err(LixError { message }) if message.contains("inconsistent")));
    }

    #[test]
    fn rejects_query_plan_with_non_query_steps() {
        let plan = LogicalStatementPlan::new(
            LogicalStatementOperation::QueryRead,
            LogicalStatementSemantics::QueryRead(LogicalReadSemantics::empty()),
            vec![LogicalStatementStep::Statement(
                statement_from_sql("INSERT INTO t (id) VALUES (1)"),
            )],
        );

        assert!(matches!(plan.validate_plan_shape(), Err(LixError { message }) if message.contains("only contain query steps")));
    }
}
