use crate::sql::types::PreparedStatement;
use crate::Value;

use super::logical::LogicalStatementOperation;

#[derive(Debug, Clone)]
pub(crate) struct PhysicalStatementPlan {
    pub(crate) operation: LogicalStatementOperation,
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) compatibility_params: Vec<Value>,
}
