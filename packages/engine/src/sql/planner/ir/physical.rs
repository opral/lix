use crate::sql::types::PreparedStatement;

use super::logical::LogicalStatementOperation;

#[derive(Debug, Clone)]
pub(crate) struct PhysicalStatementPlan {
    pub(crate) operation: LogicalStatementOperation,
    pub(crate) prepared_statements: Vec<PreparedStatement>,
}
