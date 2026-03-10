mod ast_utils;
mod params;
mod pipeline;
mod row_resolution;
mod steps;
mod types;

use crate::functions::LixFunctionProvider;
use crate::{LixBackend, LixError, Value};
use sqlparser::ast::Statement;

pub(crate) use ast_utils::{object_name_matches, parse_single_query, quote_ident};
#[cfg(test)]
pub(crate) use params::bind_sql_with_state_and_appended_params;
pub(crate) use params::PlaceholderState;
#[cfg(test)]
pub use pipeline::parse_sql_statements;
#[cfg(test)]
pub(crate) use pipeline::preprocess_sql_rewrite_only;
pub(crate) use row_resolution::{resolve_expr_cell_with_state, ResolvedCell, RowSourceResolver};
pub(crate) use steps::vtable_read;
pub(crate) use types::{PostprocessPlan, RewriteOutput, SchemaRegistration};

pub(crate) fn rewrite_statement<P: LixFunctionProvider>(
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    provider: &mut P,
) -> Result<RewriteOutput, LixError> {
    let output = if let Some(output) =
        pipeline::rules::statement::canonical::rewrite_sync_statement(
            statement.clone(),
            params,
            writer_key,
            provider,
        )?
    {
        output
    } else {
        pipeline::rules::statement::passthrough::apply(statement)
    };
    pipeline::validator::validate_statement_output(&output)?;
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
        pipeline::rules::statement::canonical::rewrite_backend_statement(
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
        pipeline::rules::statement::passthrough::apply(statement)
    };
    pipeline::validator::validate_statement_output(&output)?;
    Ok(output)
}
