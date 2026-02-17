use sqlparser::ast::Statement;

use crate::functions::LixFunctionProvider;
use crate::sql::types::RewriteOutput;
use crate::sql::DetectedFileDomainChange;
use crate::{LixBackend, LixError, Value};

use crate::sql::pipeline::registry::StatementRule;

pub(crate) mod canonical;
pub(crate) mod context;
pub(crate) mod explain_read;
pub(crate) mod helpers;
pub(crate) mod outcome;
pub(crate) mod passthrough;
pub(crate) mod query_read;

pub(crate) fn apply_sync_rule<P: LixFunctionProvider>(
    rule: StatementRule,
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    provider: &mut P,
) -> Result<Option<RewriteOutput>, LixError> {
    match rule {
        StatementRule::QueryRead => query_read::apply_sync(statement),
        StatementRule::ExplainRead => explain_read::apply_sync(statement),
        StatementRule::VtableWriteCanonical => {
            canonical::rewrite_sync_statement(statement, params, writer_key, provider)
        }
        StatementRule::Passthrough => Ok(Some(passthrough::apply(statement))),
    }
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
