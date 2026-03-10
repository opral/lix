#[path = "pipeline/rules/mod.rs"]
mod rules;
mod row_resolution;
mod steps;
mod types;
#[path = "pipeline/validator.rs"]
mod validator;

use crate::functions::LixFunctionProvider;
use crate::sql_shared::ast::parse_sql_statements;
use crate::{LixBackend, LixError, Value};
use sqlparser::ast::Statement;

pub(crate) use crate::engine::sql::ast::walk::object_name_matches;
pub(crate) use crate::engine::sql::ast::utils::PlaceholderState;
pub(crate) use row_resolution::{resolve_expr_cell_with_state, ResolvedCell, RowSourceResolver};
pub(crate) use steps::vtable_read;
pub(crate) use types::{PostprocessPlan, RewriteOutput, SchemaRegistration};

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
