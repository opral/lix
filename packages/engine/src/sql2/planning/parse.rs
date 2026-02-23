use super::super::ast::nodes::Statement;
use super::super::ast::utils::parse_sql_statements;
use super::super::contracts::planner_error::PlannerError;

pub(crate) fn parse_sql(sql: &str) -> Result<Vec<Statement>, PlannerError> {
    parse_sql_statements(sql).map_err(PlannerError::parse)
}
