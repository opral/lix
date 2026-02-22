use std::collections::BTreeSet;

use sqlparser::ast::Statement;

use crate::sql::FileReadMaterializationScope;
use crate::sql::types::{
    MutationRow, PostprocessPlan, PreparedStatement, SchemaRegistration, UpdateValidationPlan,
};

#[derive(Debug, Clone)]
pub(crate) struct StatementBlock {
    pub(crate) statements: Vec<Statement>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReadMaintenanceRequirements {
    pub(crate) history_roots: BTreeSet<String>,
    pub(crate) file_materialization_scope: Option<FileReadMaterializationScope>,
    pub(crate) requires_file_history_materialization: bool,
    pub(crate) requires_history_timeline_materialization: bool,
}

impl ReadMaintenanceRequirements {
    pub(crate) fn is_empty(&self) -> bool {
        self.history_roots.is_empty()
            && self.file_materialization_scope.is_none()
            && !self.requires_file_history_materialization
            && !self.requires_history_timeline_materialization
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledStatementPlan {
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) maintenance_requirements: ReadMaintenanceRequirements,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}
