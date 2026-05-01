use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::LixError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlStatementKind {
    Read,
    Write,
    Other,
}

pub(crate) fn classify_statement(sql: &str) -> Result<SqlStatementKind, LixError> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 SQL parse error: {error}"),
        )
    })?;
    let [statement] = statements.as_slice() else {
        return Ok(SqlStatementKind::Other);
    };
    Ok(classify_ast_statement(statement))
}

fn classify_ast_statement(statement: &Statement) -> SqlStatementKind {
    match statement {
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
            SqlStatementKind::Write
        }
        Statement::Query(_) => SqlStatementKind::Read,
        Statement::Explain { statement, .. } => classify_ast_statement(statement.as_ref()),
        _ => SqlStatementKind::Other,
    }
}
