use crate::functions::LixFunctionProvider;
use crate::sql::steps::inline_lix_functions::inline_lix_functions_with_provider;
use crate::sql::types::PreparedStatement;
use crate::sql::{bind_sql_with_state_and_appended_params, lower_statement, PlaceholderState};
use crate::{LixError, SqlDialect, Value};

use crate::sql::planner::ir::logical::LogicalStatement;
use crate::sql::planner::ir::physical::PhysicalStatementPlan;

pub(crate) fn emit_physical_statement_plan_with_state<P: LixFunctionProvider>(
    logical_statements: &[LogicalStatement],
    appended_params: &[Value],
    base_params: &[Value],
    dialect: SqlDialect,
    provider: &mut P,
    mut placeholder_state: PlaceholderState,
) -> Result<(PhysicalStatementPlan, PlaceholderState), LixError> {
    let mut prepared_statements = Vec::with_capacity(logical_statements.len());

    for logical_statement in logical_statements {
        let inlined =
            inline_lix_functions_with_provider(logical_statement.statement.clone(), provider);
        let lowered = lower_statement(inlined, dialect)?;
        let bound = bind_sql_with_state_and_appended_params(
            &lowered.to_string(),
            base_params,
            appended_params,
            dialect,
            placeholder_state,
        )?;
        placeholder_state = bound.state;
        prepared_statements.push(PreparedStatement {
            statement: lowered,
            sql: bound.sql,
            params: bound.params,
        });
    }

    let compatibility_params = if prepared_statements.len() == 1 {
        prepared_statements[0].params.clone()
    } else {
        Vec::new()
    };

    Ok((
        PhysicalStatementPlan {
            prepared_statements,
            compatibility_params,
        },
        placeholder_state,
    ))
}
