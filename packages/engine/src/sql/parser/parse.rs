use crate::sql::prepare::contracts::planner_error::PlannerError;
use crate::LixError;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub(crate) struct ParsedSql {
    pub(crate) statements: Vec<Statement>,
    pub(crate) parse_duration: Duration,
}

pub(crate) fn parse_sql_script_with_timing(sql: &str) -> Result<ParsedSql, ParserError> {
    let started = Instant::now();
    let statements = Parser::parse_sql(&GenericDialect {}, sql)?;
    Ok(ParsedSql {
        statements,
        parse_duration: started.elapsed(),
    })
}

#[cfg(test)]
pub(crate) fn parse_sql_script(sql: &str) -> Result<Vec<Statement>, ParserError> {
    parse_sql_script_with_timing(sql).map(|parsed| parsed.statements)
}

pub(crate) fn parse_sql_statements_with_timing(sql: &str) -> Result<ParsedSql, LixError> {
    parse_sql_script_with_timing(sql).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
    })
}

pub(crate) fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, LixError> {
    parse_sql_statements_with_timing(sql).map(|parsed| parsed.statements)
}

pub(crate) fn parse_sql_with_timing(sql: &str) -> Result<ParsedSql, PlannerError> {
    parse_sql_statements_with_timing(sql).map_err(PlannerError::parse)
}

pub(crate) fn parse_sql(sql: &str) -> Result<Vec<Statement>, PlannerError> {
    parse_sql_with_timing(sql).map(|parsed| parsed.statements)
}
