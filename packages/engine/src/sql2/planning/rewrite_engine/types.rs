use crate::engine::sql2::ast::nodes::Statement;
use crate::engine::sql2::contracts::planned_statement::PlannedStatementSet;
use crate::Value;

pub(crate) type SchemaRegistration =
    crate::engine::sql2::contracts::planned_statement::SchemaRegistration;
pub(crate) type MutationOperation =
    crate::engine::sql2::contracts::planned_statement::MutationOperation;
pub(crate) type MutationRow = crate::engine::sql2::contracts::planned_statement::MutationRow;
pub(crate) type UpdateValidationPlan =
    crate::engine::sql2::contracts::planned_statement::UpdateValidationPlan;
pub(crate) type PostprocessPlan =
    crate::engine::sql2::contracts::postprocess_actions::PostprocessPlan;
pub(crate) type VtableDeletePlan =
    crate::engine::sql2::contracts::postprocess_actions::VtableDeletePlan;
pub(crate) type VtableUpdatePlan =
    crate::engine::sql2::contracts::postprocess_actions::VtableUpdatePlan;
pub(crate) type PreparedStatement =
    crate::engine::sql2::contracts::prepared_statement::PreparedStatement;

#[derive(Debug, Clone)]
pub(crate) struct RewriteOutput {
    pub(crate) statements: Vec<Statement>,
    pub(crate) params: Vec<Value>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreprocessOutput {
    pub(crate) sql: String,
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

impl From<PreprocessOutput> for PlannedStatementSet {
    fn from(output: PreprocessOutput) -> Self {
        Self {
            sql: output.sql,
            prepared_statements: output.prepared_statements,
            registrations: output.registrations,
            postprocess: output.postprocess,
            mutations: output.mutations,
            update_validations: output.update_validations,
        }
    }
}
