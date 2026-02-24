use sqlparser::ast::Statement;

use crate::engine::sql::planning::rewrite_engine::types::RewriteOutput;
use crate::engine::sql::planning::rewrite_engine::DetectedFileDomainChange;
use crate::functions::LixFunctionProvider;
use crate::{LixBackend, LixError, Value};

use super::registry::statement_rules;
use super::rules::statement::{apply_backend_rule, apply_sync_rule};
use super::validator::validate_statement_output;

pub(crate) struct StatementPipeline<'a> {
    params: &'a [Value],
    writer_key: Option<&'a str>,
}

impl<'a> StatementPipeline<'a> {
    pub(crate) fn new(params: &'a [Value], writer_key: Option<&'a str>) -> Self {
        Self { params, writer_key }
    }

    pub(crate) fn rewrite_statement<P: LixFunctionProvider>(
        &self,
        statement: Statement,
        provider: &mut P,
    ) -> Result<RewriteOutput, LixError> {
        let output = StatementRuleEngine::new(self.params, self.writer_key)
            .rewrite_statement(statement, provider)?;
        validate_statement_output(&output)?;
        Ok(output)
    }

    pub(crate) async fn rewrite_statement_with_backend<P>(
        &self,
        backend: &dyn LixBackend,
        statement: Statement,
        provider: &mut P,
        detected_file_domain_changes: &[DetectedFileDomainChange],
    ) -> Result<RewriteOutput, LixError>
    where
        P: LixFunctionProvider + Clone + Send + 'static,
    {
        let output = StatementRuleEngine::new(self.params, self.writer_key)
            .rewrite_statement_with_backend(
                backend,
                statement,
                provider,
                detected_file_domain_changes,
            )
            .await?;
        validate_statement_output(&output)?;
        Ok(output)
    }
}

struct StatementRuleEngine<'a> {
    params: &'a [Value],
    writer_key: Option<&'a str>,
}

impl<'a> StatementRuleEngine<'a> {
    fn new(params: &'a [Value], writer_key: Option<&'a str>) -> Self {
        Self { params, writer_key }
    }

    fn rewrite_statement<P: LixFunctionProvider>(
        &self,
        statement: Statement,
        provider: &mut P,
    ) -> Result<RewriteOutput, LixError> {
        for rule in statement_rules() {
            if let Some(output) = apply_sync_rule(
                *rule,
                statement.clone(),
                self.params,
                self.writer_key,
                provider,
            )? {
                return Ok(output);
            }
        }
        Err(LixError {
            message: "statement rewrite engine could not match statement rule".to_string(),
        })
    }

    async fn rewrite_statement_with_backend<P>(
        &self,
        backend: &dyn LixBackend,
        statement: Statement,
        provider: &mut P,
        detected_file_domain_changes: &[DetectedFileDomainChange],
    ) -> Result<RewriteOutput, LixError>
    where
        P: LixFunctionProvider + Clone + Send + 'static,
    {
        for rule in statement_rules() {
            if let Some(output) = apply_backend_rule(
                *rule,
                backend,
                statement.clone(),
                self.params,
                self.writer_key,
                provider,
                detected_file_domain_changes,
            )
            .await?
            {
                return Ok(output);
            }
        }
        Err(LixError {
            message: "statement backend rewrite engine could not match statement rule".to_string(),
        })
    }
}
