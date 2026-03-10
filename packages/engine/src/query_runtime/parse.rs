use crate::query_runtime::contracts::planner_error::PlannerError;
use crate::sql_shared::ast::parse_sql_statements;
use sqlparser::ast::Statement;

pub(crate) fn parse_sql(sql: &str) -> Result<Vec<Statement>, PlannerError> {
    parse_sql_statements(sql).map_err(PlannerError::parse)
}
