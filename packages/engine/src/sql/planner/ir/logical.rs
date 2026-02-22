use std::collections::BTreeSet;

use crate::sql::planner::types::ReadMaintenanceRequirements;
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
    File,
    FileByVersion,
    FileHistory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogicalReadSemantics {
    pub(crate) operators: BTreeSet<LogicalReadOperator>,
}

impl LogicalReadSemantics {
    #[cfg(test)]
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
    QueryRead,
    ExplainRead,
    CanonicalWrite,
    Passthrough,
}

#[derive(Debug, Clone)]
pub(crate) struct LogicalStatementPlan {
    pub(crate) operation: LogicalStatementOperation,
    pub(crate) semantics: LogicalStatementSemantics,
    pub(crate) planned_statements: Vec<LogicalStatementStep>,
    pub(crate) emission_sql: Vec<String>,
    pub(crate) appended_params: Vec<Value>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) maintenance_requirements: ReadMaintenanceRequirements,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

impl LogicalStatementPlan {
    pub(crate) fn new(
        operation: LogicalStatementOperation,
        semantics: LogicalStatementSemantics,
        planned_statements: Vec<LogicalStatementStep>,
        emission_sql: Vec<String>,
    ) -> Self {
        Self {
            operation,
            semantics,
            planned_statements,
            emission_sql,
            appended_params: Vec::new(),
            registrations: Vec::new(),
            maintenance_requirements: ReadMaintenanceRequirements::default(),
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

    pub(crate) fn with_maintenance_requirements(
        mut self,
        maintenance_requirements: ReadMaintenanceRequirements,
    ) -> Self {
        self.maintenance_requirements = maintenance_requirements;
        self
    }

    pub(crate) fn validate_plan_shape(&self) -> Result<(), LixError> {
        if self.planned_statements.is_empty() {
            return Err(LixError {
                message: "logical plan has no planned statements".to_string(),
            });
        }
        if self.planned_statements.len() != self.emission_sql.len() {
            return Err(LixError {
                message: format!(
                    "logical plan step count ({}) must match emission SQL count ({})",
                    self.planned_statements.len(),
                    self.emission_sql.len()
                ),
            });
        }

        match (self.operation, &self.semantics) {
            (LogicalStatementOperation::QueryRead, LogicalStatementSemantics::QueryRead(_)) => {
                let has_non_query = self
                    .planned_statements
                    .iter()
                    .any(|step| !matches!(step, LogicalStatementStep::QueryRead));
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
                    .any(|step| !matches!(step, LogicalStatementStep::ExplainRead));
                if has_non_explain {
                    return Err(LixError {
                        message: "explain plans may only contain explain read steps".to_string(),
                    });
                }
            }
            (LogicalStatementOperation::CanonicalWrite, LogicalStatementSemantics::CanonicalWrite) => {
                let has_non_canonical = self
                    .planned_statements
                    .iter()
                    .any(|step| !matches!(step, LogicalStatementStep::CanonicalWrite));
                if has_non_canonical {
                    return Err(LixError {
                        message: "canonical write plans may only contain canonical write steps"
                            .to_string(),
                    });
                }
                if !self.maintenance_requirements.is_empty() {
                    return Err(LixError {
                        message: "canonical write plans cannot carry read maintenance requirements"
                            .to_string(),
                    });
                }
            }
            (LogicalStatementOperation::Passthrough, LogicalStatementSemantics::Passthrough) => {
                let has_non_passthrough = self
                    .planned_statements
                    .iter()
                    .any(|step| !matches!(step, LogicalStatementStep::Passthrough));
                if has_non_passthrough {
                    return Err(LixError {
                        message: "passthrough plans may only contain passthrough steps".to_string(),
                    });
                }
                if !self.maintenance_requirements.is_empty() {
                    return Err(LixError {
                        message: "passthrough plans cannot carry read maintenance requirements"
                            .to_string(),
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
    use crate::LixError;

    #[test]
    fn validates_query_read_plan_shape() {
        let plan = LogicalStatementPlan::new(
            LogicalStatementOperation::QueryRead,
            LogicalStatementSemantics::QueryRead(LogicalReadSemantics::empty()),
            vec![LogicalStatementStep::QueryRead],
            vec!["SELECT 1".to_string()],
        );

        assert!(plan.validate_plan_shape().is_ok());
    }

    #[test]
    fn rejects_inconsistent_operation_and_semantics() {
        let plan = LogicalStatementPlan::new(
            LogicalStatementOperation::QueryRead,
            LogicalStatementSemantics::Passthrough,
            vec![LogicalStatementStep::Passthrough],
            vec!["CREATE TABLE t (id INTEGER)".to_string()],
        );

        assert!(matches!(plan.validate_plan_shape(), Err(LixError { message }) if message.contains("inconsistent")));
    }

    #[test]
    fn rejects_query_plan_with_non_query_steps() {
        let plan = LogicalStatementPlan::new(
            LogicalStatementOperation::QueryRead,
            LogicalStatementSemantics::QueryRead(LogicalReadSemantics::empty()),
            vec![LogicalStatementStep::CanonicalWrite],
            vec!["INSERT INTO t (id) VALUES (1)".to_string()],
        );

        assert!(matches!(plan.validate_plan_shape(), Err(LixError { message }) if message.contains("only contain query steps")));
    }
}
