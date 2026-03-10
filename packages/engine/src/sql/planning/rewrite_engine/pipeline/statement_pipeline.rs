use sqlparser::ast::Statement;

use crate::engine::sql::planning::rewrite_engine::types::RewriteOutput;
use crate::functions::LixFunctionProvider;
use crate::{LixBackend, LixError, Value};

use super::rules::statement::{canonical, passthrough};
use super::validator::validate_statement_output;

pub(crate) struct StatementPipeline<'a> {
    params: &'a [Value],
    writer_key: Option<&'a str>,
}

impl<'a> StatementPipeline<'a> {
    pub(crate) fn new(params: &'a [Value], writer_key: Option<&'a str>) -> Self {
        Self {
            params,
            writer_key,
        }
    }

    pub(crate) fn rewrite_statement<P: LixFunctionProvider>(
        &self,
        statement: Statement,
        provider: &mut P,
    ) -> Result<RewriteOutput, LixError> {
        let output = if let Some(output) = canonical::rewrite_sync_statement(
            statement.clone(),
            self.params,
            self.writer_key,
            provider,
        )? {
            output
        } else {
            passthrough::apply(statement)
        };
        validate_statement_output(&output)?;
        Ok(output)
    }

    pub(crate) async fn rewrite_statement_with_backend<P>(
        &self,
        backend: &dyn LixBackend,
        statement: Statement,
        provider: &mut P,
    ) -> Result<RewriteOutput, LixError>
    where
        P: LixFunctionProvider + Clone + Send + 'static,
    {
        let output = if let Some(output) = canonical::rewrite_backend_statement(
            backend,
            statement.clone(),
            self.params,
            self.writer_key,
            provider,
        )
        .await?
        {
            output
        } else {
            passthrough::apply(statement)
        };
        validate_statement_output(&output)?;
        Ok(output)
    }
}
