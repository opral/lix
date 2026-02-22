use sqlparser::ast::Statement;

use crate::sql::types::{
    MutationRow, PostprocessPlan, PreparedStatement, SchemaRegistration, UpdateValidationPlan,
};

#[derive(Debug, Clone)]
pub(crate) struct StatementBlock {
    pub(crate) statements: Vec<Statement>,
    pub(crate) explicit_transaction_script: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledStatementPlan {
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}
