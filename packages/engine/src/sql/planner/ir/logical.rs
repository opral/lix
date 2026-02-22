use sqlparser::ast::Statement;

use crate::sql::types::{
    MutationRow, PostprocessPlan, RewriteOutput, SchemaRegistration, UpdateValidationPlan,
};
use crate::Value;

#[derive(Debug, Clone)]
pub(crate) struct LogicalStatement {
    pub(crate) statement: Statement,
}

#[derive(Debug, Clone)]
pub(crate) struct LogicalStatementPlan {
    pub(crate) statements: Vec<LogicalStatement>,
    pub(crate) appended_params: Vec<Value>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

impl LogicalStatementPlan {
    pub(crate) fn from_rewrite_output(output: RewriteOutput) -> Self {
        Self {
            statements: output
                .statements
                .into_iter()
                .map(|statement| LogicalStatement { statement })
                .collect(),
            appended_params: output.params,
            registrations: output.registrations,
            postprocess: output.postprocess,
            mutations: output.mutations,
            update_validations: output.update_validations,
        }
    }
}
