use crate::sql::types::PreparedStatement;
use crate::Value;

#[derive(Debug, Clone)]
pub(crate) struct PhysicalStatementPlan {
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) compatibility_params: Vec<Value>,
}
