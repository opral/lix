#[path = "pipeline/rules/mod.rs"]
mod rules;
mod steps;
#[path = "pipeline/validator.rs"]
mod validator;

use crate::functions::LixFunctionProvider;
use crate::engine::sql::contracts::planned_statement::PlannedStatementSet;
use crate::sql_shared::ast::parse_sql_statements;
use crate::{LixBackend, LixError, Value};
use sqlparser::ast::Statement;

pub(crate) use crate::engine::sql::ast::walk::object_name_matches;
pub(crate) use crate::engine::sql::ast::utils::PlaceholderState;
pub(crate) use crate::engine::sql::ast::utils::{
    resolve_expr_cell_with_state, ResolvedCell, RowSourceResolver,
};
pub(crate) type SchemaRegistration =
    crate::engine::sql::contracts::planned_statement::SchemaRegistration;
pub(crate) type MutationOperation =
    crate::engine::sql::contracts::planned_statement::MutationOperation;
pub(crate) type MutationRow = crate::engine::sql::contracts::planned_statement::MutationRow;
pub(crate) type UpdateValidationPlan =
    crate::engine::sql::contracts::planned_statement::UpdateValidationPlan;
pub(crate) type PostprocessPlan =
    crate::engine::sql::contracts::postprocess_actions::PostprocessPlan;
pub(crate) type VtableDeletePlan =
    crate::engine::sql::contracts::postprocess_actions::VtableDeletePlan;
pub(crate) type VtableUpdatePlan =
    crate::engine::sql::contracts::postprocess_actions::VtableUpdatePlan;
pub(crate) type PreparedStatement =
    crate::engine::sql::contracts::prepared_statement::PreparedStatement;
pub(crate) use steps::vtable_read;

#[derive(Debug, Clone)]
pub(crate) struct RewriteOutput {
    pub(crate) statements: Vec<Statement>,
    pub(crate) effect_only: bool,
    pub(crate) params: Vec<Value>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreprocessOutput {
    pub(crate) sql: String,
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

impl From<PreprocessOutput> for PlannedStatementSet {
    fn from(output: PreprocessOutput) -> Self {
        Self {
            sql: output.sql,
            prepared_statements: output.prepared_statements,
            registrations: output.registrations,
            postprocess: output.postprocess,
            mutations: output.mutations,
            update_validations: output.update_validations,
        }
    }
}

pub(crate) fn parse_single_query(sql: &str) -> Result<sqlparser::ast::Query, LixError> {
    let mut statements = parse_sql_statements(sql)?;
    if statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected a single SELECT statement".to_string(),
        });
    }
    match statements.remove(0) {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected SELECT statement".to_string(),
        }),
    }
}

pub(crate) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn rewrite_statement<P: LixFunctionProvider>(
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    provider: &mut P,
) -> Result<RewriteOutput, LixError> {
    let output = if let Some(output) =
        rules::statement::canonical::rewrite_sync_statement(statement.clone(), params, writer_key, provider)?
    {
        output
    } else {
        rules::statement::passthrough::apply(statement)
    };
    validator::validate_statement_output(&output)?;
    Ok(output)
}

pub(crate) async fn rewrite_statement_with_backend<P>(
    backend: &dyn LixBackend,
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    provider: &mut P,
) -> Result<RewriteOutput, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    let output = if let Some(output) =
        rules::statement::canonical::rewrite_backend_statement(
            backend,
            statement.clone(),
            params,
            writer_key,
            provider,
        )
        .await?
    {
        output
    } else {
        rules::statement::passthrough::apply(statement)
    };
    validator::validate_statement_output(&output)?;
    Ok(output)
}
