use crate::Value;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedStatement {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
}
