use crate::sql::semantic_ir::{BoundStatementMetadata, StatementContext, StatementKind};
use sqlparser::ast::Statement;

pub(crate) fn classify_statement(statement: &Statement) -> StatementKind {
    match statement {
        Statement::Query(_) => StatementKind::Query,
        Statement::Insert(_) => StatementKind::Insert,
        Statement::Update(_) => StatementKind::Update,
        Statement::Delete(_) => StatementKind::Delete,
        Statement::Explain { .. } | Statement::ExplainTable { .. } => StatementKind::Explain,
        _ => StatementKind::Utility,
    }
}

pub(crate) fn bind_statement_metadata(
    statement: &Statement,
    statement_context: StatementContext,
) -> BoundStatementMetadata {
    BoundStatementMetadata {
        statement_kind: classify_statement(statement),
        statement_context,
    }
}
