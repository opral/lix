use crate::functions::LixFunctionProvider;
use crate::sql::steps::inline_lix_functions::inline_lix_functions_with_provider;
use crate::sql::types::PreparedStatement;
use crate::sql::{
    bind_statement_with_state_and_appended_params, lower_statement, PlaceholderState,
};
use crate::{LixError, SqlDialect, Value};

use crate::sql::planner::ir::logical::LogicalStatementPlan;
use crate::sql::planner::ir::physical::PhysicalStatementPlan;

pub(crate) fn emit_physical_statement_plan_with_state<P: LixFunctionProvider>(
    logical_plan: &LogicalStatementPlan,
    base_params: &[Value],
    dialect: SqlDialect,
    provider: &mut P,
    mut placeholder_state: PlaceholderState,
) -> Result<(PhysicalStatementPlan, PlaceholderState), LixError> {
    let mut prepared_statements = Vec::with_capacity(logical_plan.planned_statements.len());

    for statement in &logical_plan.planned_statements {
        let inlined = inline_lix_functions_with_provider(statement.clone(), provider);
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

    Ok((
        PhysicalStatementPlan {
            operation: logical_plan.operation,
            prepared_statements,
        },
        placeholder_state,
    ))
}
