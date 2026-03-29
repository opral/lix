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

impl BoundStatement {
    pub(crate) fn from_statement(
        statement: Statement,
        bound_parameters: Vec<Value>,
        execution_context: ExecutionContext,
    ) -> Self {
        let metadata = bind_statement_metadata(&statement, execution_context);
        Self::new(
            statement,
            metadata.statement_kind,
            bound_parameters,
            Vec::new(),
            metadata.execution_context,
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

pub(crate) fn classify_statement(statement: &Statement) -> StatementKind {
    match statement {
        Statement::Query(_) => StatementKind::Query,
        Statement::Insert(_) => StatementKind::Insert,
        Statement::Update(_) => StatementKind::Update,
        Statement::Delete(_) => StatementKind::Delete,
        Statement::Explain { .. } | Statement::ExplainTable { .. } => StatementKind::Explain,
        _ => StatementKind::Other,
    }
}

pub(crate) fn bind_statement_metadata(
    statement: &Statement,
    execution_context: ExecutionContext,
) -> BoundStatementMetadata {
    BoundStatementMetadata {
        statement_kind: classify_statement(statement),
        execution_context,
    }
}
