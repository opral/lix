use sqlparser::ast::Statement;

use crate::sql::types::{MutationRow, PostprocessPlan, SchemaRegistration, UpdateValidationPlan};
use crate::Value;

#[derive(Debug, Clone)]
pub(crate) struct WriteRewriteOutput {
    pub(crate) statements: Vec<Statement>,
    pub(crate) params: Vec<Value>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}
