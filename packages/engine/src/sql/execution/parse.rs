use crate::sql::common::ast::parse_sql_statements;
use crate::sql::compat::internal_state_vtable::normalize_legacy_internal_state_vtable_statements;
use crate::sql::execution::contracts::planner_error::PlannerError;
use sqlparser::ast::Statement;

pub(crate) fn parse_sql(sql: &str) -> Result<Vec<Statement>, PlannerError> {
    let mut statements = parse_sql_statements(sql).map_err(PlannerError::parse)?;
    normalize_legacy_internal_state_vtable_statements(&mut statements);
    Ok(statements)
}
