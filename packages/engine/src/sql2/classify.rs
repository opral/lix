use datafusion::sql::sqlparser::ast::{Query, SetExpr, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::LixError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlStatementKind {
    Read,
    Write,
    Other,
}

pub(crate) fn classify_statement(sql: &str) -> Result<SqlStatementKind, LixError> {
    let statements = parse_sql_statements(sql)?;
    let [statement] = statements.as_slice() else {
        return Ok(SqlStatementKind::Other);
    };
    Ok(classify_ast_statement(statement))
}

pub(crate) fn validate_supported_statement_ast(sql: &str) -> Result<(), LixError> {
    let statements = parse_sql_statements(sql)?;
    let [statement] = statements.as_slice() else {
        return Err(unsupported_sql_error(
            "Lix SQL only supports one statement per execute() call",
        ));
    };
    validate_supported_ast_statement(statement)
}

fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, LixError> {
    Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| {
        LixError::new(
            LixError::CODE_PARSE_ERROR,
            format!("sql2 SQL parse error: {error}"),
        )
    })
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

fn validate_supported_ast_statement(statement: &Statement) -> Result<(), LixError> {
    match statement {
        Statement::Query(query) => validate_supported_query(query),
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => Ok(()),
        Statement::Explain { statement, .. } => validate_supported_ast_statement(statement),
        _ => Err(unsupported_sql_error(format!(
            "SQL statement is not supported by Lix SQL: {statement}"
        ))),
    }
}

fn validate_supported_query(query: &Query) -> Result<(), LixError> {
    if query.with.as_ref().is_some_and(|with| with.recursive) {
        return Err(
            unsupported_sql_error("recursive CTEs are not supported by Lix SQL").with_hint(
                "Use explicit commit graph surfaces such as lix_commit, lix_commit_edge, and lix_state_history instead of WITH RECURSIVE.",
            ),
        );
    }

    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            validate_supported_query(&cte.query)?;
        }
    }
    validate_supported_set_expr(&query.body)
}

fn validate_supported_set_expr(expr: &SetExpr) -> Result<(), LixError> {
    match expr {
        SetExpr::Query(query) => validate_supported_query(query),
        SetExpr::SetOperation { left, right, .. } => {
            validate_supported_set_expr(left)?;
            validate_supported_set_expr(right)
        }
        _ => Ok(()),
    }
}

fn unsupported_sql_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_UNSUPPORTED_SQL, message)
}
