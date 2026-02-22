use sqlparser::ast::Statement;

use crate::functions::LixFunctionProvider;
use crate::sql::planner::rewrite::write;
use crate::sql::types::RewriteOutput;
use crate::sql::DetectedFileDomainChange;
use crate::{LixBackend, LixError, Value};

pub(crate) async fn rewrite_backend_statement<P>(
    backend: &dyn LixBackend,
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    functions: &mut P,
    detected_file_domain_changes: &[DetectedFileDomainChange],
) -> Result<Option<RewriteOutput>, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    let output = write::rewrite_backend_statement(
        backend,
        statement,
        params,
        writer_key,
        functions,
        detected_file_domain_changes,
    )
    .await?;

    Ok(output.map(|output| RewriteOutput {
        statements: output.statements,
        params: output.params,
        registrations: output.registrations,
        postprocess: output.postprocess,
        mutations: output.mutations,
        update_validations: output.update_validations,
    }))
}
