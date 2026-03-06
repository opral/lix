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
}

impl ExecutionContext {
    pub(crate) fn with_dialect(dialect: SqlDialect) -> Self {
        Self {
            dialect: Some(dialect),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundStatement {
    pub(crate) statement: Statement,
    pub(crate) statement_kind: StatementKind,
    pub(crate) bound_parameters: Vec<Value>,
    pub(crate) normalized_scalar_literals: Vec<Value>,
    pub(crate) execution_context: ExecutionContext,
}

impl BoundStatement {
    pub(crate) fn from_statement(
        statement: Statement,
        bound_parameters: Vec<Value>,
        execution_context: ExecutionContext,
    ) -> Self {
        let statement_kind = crate::sql2::core::parser::statement_kind(&statement);
        Self::new(
            statement,
            statement_kind,
            bound_parameters,
            Vec::new(),
            execution_context,
        )
    }

    pub(crate) fn new(
        statement: Statement,
        statement_kind: StatementKind,
        bound_parameters: Vec<Value>,
        normalized_scalar_literals: Vec<Value>,
        execution_context: ExecutionContext,
    ) -> Self {
        Self {
            statement,
            statement_kind,
            bound_parameters,
            normalized_scalar_literals,
            execution_context,
        }
    }
}
