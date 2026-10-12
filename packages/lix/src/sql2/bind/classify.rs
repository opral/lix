use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{Query, SetExpr, Statement as SqlStatement};

use crate::LixError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlStatementKind {
    Read,
    Write,
    Other,
}

pub(crate) fn validate_supported_datafusion_statement_ast(
    statement: &DataFusionStatement,
) -> Result<(), LixError> {
    match statement {
        DataFusionStatement::Statement(statement) => validate_supported_ast_statement(statement),
        DataFusionStatement::Explain(explain) => {
            if classify_datafusion_statement(explain.statement.as_ref()) == SqlStatementKind::Write
            {
                return Err(unsupported_sql_error(
                    "EXPLAIN of write statements is not supported by Lix SQL",
                ));
            }
            validate_supported_datafusion_statement_ast(explain.statement.as_ref())
        }
        _ => Err(unsupported_sql_error(format!(
            "SQL statement is not supported by Lix SQL: {statement}"
        ))),
    }
}

pub(crate) fn classify_datafusion_statement(statement: &DataFusionStatement) -> SqlStatementKind {
    match statement {
        DataFusionStatement::Statement(statement) => classify_ast_statement(statement),
        DataFusionStatement::Explain(explain) => {
            classify_datafusion_statement(explain.statement.as_ref())
        }
        _ => SqlStatementKind::Other,
    }
}

fn classify_ast_statement(statement: &SqlStatement) -> SqlStatementKind {
    match statement {
        SqlStatement::Insert(_) | SqlStatement::Update(_) | SqlStatement::Delete(_) => {
            SqlStatementKind::Write
        }
        // A data-modifying CTE (`WITH d AS (DELETE ... RETURNING *) SELECT`)
        // is a write in a query's clothing; Lix runs neither path for it.
        SqlStatement::Query(query) if query_modifies_data(query) => SqlStatementKind::Other,
        SqlStatement::Query(_) | SqlStatement::Explain { .. } => SqlStatementKind::Read,
        _ => SqlStatementKind::Other,
    }
}

/// Whether a query body or any of its CTEs is an `INSERT`, `UPDATE`,
/// `DELETE` or `MERGE`.
fn query_modifies_data(query: &Query) -> bool {
    query
        .with
        .as_ref()
        .is_some_and(|with| with.cte_tables.iter().any(|cte| query_modifies_data(&cte.query)))
        || set_expr_modifies_data(&query.body)
}

fn set_expr_modifies_data(expr: &SetExpr) -> bool {
    match expr {
        SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Merge(_) => true,
        SetExpr::Query(query) => query_modifies_data(query),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_modifies_data(left) || set_expr_modifies_data(right)
        }
        _ => false,
    }
}

fn validate_supported_ast_statement(statement: &SqlStatement) -> Result<(), LixError> {
    match statement {
        SqlStatement::Query(query) => validate_supported_query(query),
        SqlStatement::Insert(_) | SqlStatement::Update(_) | SqlStatement::Delete(_) => Ok(()),
        SqlStatement::Explain { statement, .. } => {
            if classify_ast_statement(statement.as_ref()) == SqlStatementKind::Write {
                return Err(unsupported_sql_error(
                    "EXPLAIN of write statements is not supported by Lix SQL",
                ));
            }
            validate_supported_ast_statement(statement)
        }
        _ => Err(unsupported_sql_error(format!(
            "SQL statement is not supported by Lix SQL: {statement}"
        ))),
    }
}

fn validate_supported_query(query: &Query) -> Result<(), LixError> {
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
