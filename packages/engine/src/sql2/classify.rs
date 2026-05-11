use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    FromTable, ObjectName, Query, SetExpr, Statement as SqlStatement, TableFactor, TableObject,
    TableWithJoins,
};
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

pub(crate) fn validate_supported_datafusion_statement_ast(
    statement: &DataFusionStatement,
) -> Result<(), LixError> {
    match statement {
        DataFusionStatement::Statement(statement) => validate_supported_ast_statement(statement),
        DataFusionStatement::Explain(explain) => {
            validate_supported_datafusion_statement_ast(explain.statement.as_ref())
        }
        _ => Err(unsupported_sql_error(format!(
            "SQL statement is not supported by Lix SQL: {statement}"
        ))),
    }
}

pub(crate) fn datafusion_statement_dml_target_table_names(
    statement: &DataFusionStatement,
) -> Vec<String> {
    let mut targets = Vec::new();
    collect_datafusion_statement_dml_target_table_names(statement, &mut targets);
    targets
}

fn parse_sql_statements(sql: &str) -> Result<Vec<SqlStatement>, LixError> {
    Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| {
        LixError::new(
            LixError::CODE_PARSE_ERROR,
            format!("sql2 SQL parse error: {error}"),
        )
    })
}

fn collect_datafusion_statement_dml_target_table_names(
    statement: &DataFusionStatement,
    targets: &mut Vec<String>,
) {
    match statement {
        DataFusionStatement::Statement(statement) => {
            collect_dml_target_table_names(statement, targets);
        }
        DataFusionStatement::Explain(explain) => {
            collect_datafusion_statement_dml_target_table_names(
                explain.statement.as_ref(),
                targets,
            );
        }
        _ => {}
    }
}

fn collect_dml_target_table_names(statement: &SqlStatement, targets: &mut Vec<String>) {
    match statement {
        SqlStatement::Insert(insert) => {
            if let TableObject::TableName(name) = &insert.table {
                if let Some(table_name) = object_name_table_part(name) {
                    targets.push(table_name);
                }
            }
        }
        SqlStatement::Update(update) => {
            collect_table_with_joins_target(&update.table, targets);
        }
        SqlStatement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
            };
            for table in tables {
                collect_table_with_joins_target(table, targets);
            }
        }
        SqlStatement::Explain { statement, .. } => {
            collect_dml_target_table_names(statement.as_ref(), targets);
        }
        _ => {}
    }
}

fn collect_table_with_joins_target(table: &TableWithJoins, targets: &mut Vec<String>) {
    if let TableFactor::Table { name, .. } = &table.relation {
        if let Some(table_name) = object_name_table_part(name) {
            targets.push(table_name);
        }
    }
}

fn object_name_table_part(name: &ObjectName) -> Option<String> {
    name.0.last().and_then(|part| part.as_ident()).map(|ident| {
        if ident.quote_style.is_some() {
            ident.value.clone()
        } else {
            ident.value.to_ascii_lowercase()
        }
    })
}

fn classify_ast_statement(statement: &SqlStatement) -> SqlStatementKind {
    match statement {
        SqlStatement::Insert(_) | SqlStatement::Update(_) | SqlStatement::Delete(_) => {
            SqlStatementKind::Write
        }
        SqlStatement::Query(_) => SqlStatementKind::Read,
        SqlStatement::Explain { statement, .. } => classify_ast_statement(statement.as_ref()),
        _ => SqlStatementKind::Other,
    }
}

fn validate_supported_ast_statement(statement: &SqlStatement) -> Result<(), LixError> {
    match statement {
        SqlStatement::Query(query) => validate_supported_query(query),
        SqlStatement::Insert(_) | SqlStatement::Update(_) | SqlStatement::Delete(_) => Ok(()),
        SqlStatement::Explain { statement, .. } => validate_supported_ast_statement(statement),
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
