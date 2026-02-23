use sqlparser::ast::Statement;

use super::super::contracts::planner_error::PlannerError;

pub(crate) fn parse_sql(sql: &str) -> Result<Vec<Statement>, PlannerError> {
    crate::sql::parse_sql_statements(sql).map_err(PlannerError::parse)
}
