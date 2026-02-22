use std::collections::BTreeSet;

use sqlparser::ast::Statement;

use crate::sql::types::{MutationRow, PostprocessPlan, SchemaRegistration, UpdateValidationPlan};
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
pub(crate) struct LogicalStatementPlan {
    pub(crate) operation: LogicalStatementOperation,
    pub(crate) semantics: LogicalStatementSemantics,
    pub(crate) planned_statements: Vec<Statement>,
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
        planned_statements: Vec<Statement>,
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
}
