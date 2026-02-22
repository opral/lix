use sqlparser::ast::Statement;

use crate::functions::LixFunctionProvider;
use crate::sql::types::RewriteOutput;
use crate::sql::DetectedFileDomainChange;
use crate::{LixBackend, LixError, Value};

use crate::sql::pipeline::registry::StatementRule;

pub(crate) mod canonical;
pub(crate) mod explain_read;
pub(crate) mod passthrough;
pub(crate) mod query_read;

#[cfg(test)]
struct UnexpectedBackendCall;

#[cfg(test)]
#[async_trait::async_trait(?Send)]
impl LixBackend for UnexpectedBackendCall {
    fn dialect(&self) -> crate::SqlDialect {
        crate::SqlDialect::Sqlite
    }

    async fn execute(&self, _: &str, _: &[Value]) -> Result<crate::QueryResult, LixError> {
        Err(LixError {
            message: "sync statement rewrite attempted backend execution".to_string(),
        })
    }

    async fn begin_transaction(&self) -> Result<Box<dyn crate::LixTransaction + '_>, LixError> {
        Err(LixError {
            message: "sync statement rewrite should not open transactions".to_string(),
        })
    }
}

#[cfg(test)]
pub(crate) fn apply_sync_rule<P>(
    rule: StatementRule,
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    provider: &mut P,
) -> Result<Option<RewriteOutput>, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    match rule {
        StatementRule::QueryRead => return query_read::apply_sync(statement),
        StatementRule::ExplainRead => return explain_read::apply_sync(statement),
        StatementRule::Passthrough => return Ok(Some(passthrough::apply(statement))),
        StatementRule::VtableWriteCanonical => {}
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| LixError {
            message: format!("failed to initialize sync rewrite runtime: {error}"),
        })?;
    runtime.block_on(async {
        apply_backend_rule(
            rule,
            &UnexpectedBackendCall,
            statement,
            params,
            writer_key,
            provider,
            &[],
        )
        .await
    })
}

pub(crate) async fn apply_backend_rule<P>(
    rule: StatementRule,
    backend: &dyn LixBackend,
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    provider: &mut P,
    detected_file_domain_changes: &[DetectedFileDomainChange],
) -> Result<Option<RewriteOutput>, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    match rule {
        StatementRule::QueryRead => query_read::apply_backend(backend, statement, params).await,
        StatementRule::ExplainRead => explain_read::apply_backend(backend, statement, params).await,
        StatementRule::VtableWriteCanonical => {
            canonical::rewrite_backend_statement(
                backend,
                statement,
                params,
                writer_key,
                provider,
                detected_file_domain_changes,
            )
            .await
        }
        StatementRule::Passthrough => Ok(Some(passthrough::apply(statement))),
    }
}
