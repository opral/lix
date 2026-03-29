use crate::{SqlDialect, Value};
use sqlparser::ast::Statement;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatementKind {
    Query,
    Insert,
    Update,
    Delete,
    Explain,
    Other,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ExecutionContext {
    pub(crate) dialect: Option<SqlDialect>,
    pub(crate) writer_key: Option<String>,
    pub(crate) requested_version_id: Option<String>,
    pub(crate) active_account_ids: Vec<String>,
}

impl ExecutionContext {
    #[cfg(test)]
    pub(crate) fn with_dialect(dialect: SqlDialect) -> Self {
        Self {
            dialect: Some(dialect),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BoundStatementMetadata {
    pub(crate) statement_kind: StatementKind,
    pub(crate) execution_context: ExecutionContext,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundStatement {
    pub(crate) statement: Statement,
    pub(crate) statement_kind: StatementKind,
    pub(crate) bound_parameters: Vec<Value>,
    pub(crate) normalized_scalar_literals: Vec<Value>,
    pub(crate) execution_context: ExecutionContext,
}
