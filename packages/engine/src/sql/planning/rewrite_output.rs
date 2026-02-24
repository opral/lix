use super::super::contracts::planned_statement::{
    MutationRow, SchemaRegistration, UpdateValidationPlan,
};
use super::super::contracts::postprocess_actions::PostprocessPlan;
use crate::Value;
use sqlparser::ast::Statement;

#[derive(Debug, Clone)]
pub(crate) struct StatementRewriteOutput {
    pub(crate) statements: Vec<Statement>,
    pub(crate) params: Vec<Value>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}
