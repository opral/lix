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

#[derive(Debug, Clone)]
pub(crate) struct LogicalStatementPlan {
    pub(crate) operation: LogicalStatementOperation,
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
        planned_statements: Vec<Statement>,
    ) -> Self {
        Self {
            operation,
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
