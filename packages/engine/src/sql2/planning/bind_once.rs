use crate::{SqlDialect, Value};

use super::super::ast::nodes::Statement;
use super::super::ast::utils::{bind_sql_with_state, PlaceholderState};
use super::super::contracts::planner_error::PlannerError;

pub(crate) fn bind_script_placeholders_once(
    statements: &[Statement],
    params: &[Value],
    dialect: SqlDialect,
) -> Result<Vec<(String, Vec<Value>)>, PlannerError> {
    if params.is_empty() {
        return Ok(statements
            .iter()
            .map(|statement| (statement.to_string(), Vec::new()))
            .collect());
    }

    let mut placeholder_state = PlaceholderState::new();
    let mut bound_statements = Vec::with_capacity(statements.len());
    for statement in statements {
        let bound = bind_sql_with_state(&statement.to_string(), params, dialect, placeholder_state)
            .map_err(PlannerError::bind_once)?;
        placeholder_state = bound.state;
        bound_statements.push((bound.sql, bound.params));
    }

    Ok(bound_statements)
}
