use crate::sql::executor::contracts::planner_error::PlannerError;
use crate::LixError;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};

pub(crate) fn parse_sql_script(sql: &str) -> Result<Vec<Statement>, ParserError> {
    Parser::parse_sql(&GenericDialect {}, sql)
}

pub(crate) fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, LixError> {
    parse_sql_script(sql).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
    })
}

pub(crate) fn parse_sql(sql: &str) -> Result<Vec<Statement>, PlannerError> {
    parse_sql_statements(sql).map_err(PlannerError::parse)
}
