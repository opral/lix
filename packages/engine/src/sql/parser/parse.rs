use crate::sql::prepare::contracts::planner_error::PlannerError;
#[cfg(test)]
pub(crate) use crate::sql::support::parse_sql_script;
pub(crate) use crate::sql::support::ParsedSql;
pub(crate) use crate::sql::support::{parse_sql_statements, parse_sql_statements_with_timing};

pub(crate) fn parse_sql_with_timing(sql: &str) -> Result<ParsedSql, PlannerError> {
    parse_sql_statements_with_timing(sql).map_err(PlannerError::parse)
}
