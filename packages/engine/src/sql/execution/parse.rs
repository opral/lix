use crate::sql::common::ast::parse_sql_statements;
use crate::sql::execution::contracts::planner_error::PlannerError;
use sqlparser::ast::Statement;

pub(crate) fn parse_sql(sql: &str) -> Result<Vec<Statement>, PlannerError> {
    parse_sql_statements(sql).map_err(PlannerError::parse)
}
