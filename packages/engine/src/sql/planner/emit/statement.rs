use crate::functions::LixFunctionProvider;
use crate::sql::steps::inline_lix_functions::inline_lix_functions_with_provider;
use crate::sql::types::PreparedStatement;
use crate::sql::{
    bind_statement_with_state_and_appended_params, lower_statement,
    parse_sql_statements_with_dialect, PlaceholderState,
};
use crate::{LixError, SqlDialect, Value};

use crate::sql::planner::ir::logical::LogicalStatementPlan;

pub(crate) fn emit_physical_statement_plan_with_state<P: LixFunctionProvider>(
    logical_plan: &LogicalStatementPlan,
    base_params: &[Value],
    dialect: SqlDialect,
    provider: &mut P,
    mut placeholder_state: PlaceholderState,
) -> Result<(Vec<PreparedStatement>, PlaceholderState), LixError> {
    let mut prepared_statements = Vec::with_capacity(logical_plan.emission_sql.len());

    for emission_sql in &logical_plan.emission_sql {
        let mut parsed = parse_sql_statements_with_dialect(emission_sql, dialect)?;
        if parsed.len() != 1 {
            return Err(LixError {
                message: format!(
                    "logical emission SQL must parse to exactly one statement, got {} for: {}",
                    parsed.len(),
                    emission_sql
                ),
            });
        }
        let statement = parsed.remove(0);
        let inlined = inline_lix_functions_with_provider(statement, provider);
        let lowered = lower_statement(inlined, dialect)?;
        let bound = bind_statement_with_state_and_appended_params(
            lowered,
            base_params,
            &logical_plan.appended_params,
            dialect,
            placeholder_state,
        )?;
        placeholder_state = bound.state;
        prepared_statements.push(PreparedStatement {
            statement: bound.statement,
            sql: bound.sql,
            params: bound.params,
        });
    }

    Ok((prepared_statements, placeholder_state))
}
