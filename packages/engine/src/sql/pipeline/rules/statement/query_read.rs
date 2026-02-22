use sqlparser::ast::Statement;

#[cfg(test)]
use crate::sql::pipeline::query_engine::rewrite_read_query;
use crate::sql::pipeline::query_engine::rewrite_read_query_with_backend_and_params;
use crate::sql::types::RewriteOutput;
use crate::{LixBackend, LixError, Value};

#[cfg(test)]
pub(crate) fn apply_sync(statement: Statement) -> Result<Option<RewriteOutput>, LixError> {
    let Statement::Query(query) = statement else {
        return Ok(None);
    };
    let rewritten = rewrite_read_query(*query)?;
    Ok(Some(RewriteOutput {
        statements: vec![Statement::Query(Box::new(rewritten))],
        params: Vec::new(),
        registrations: Vec::new(),
        postprocess: None,
        mutations: Vec::new(),
        update_validations: Vec::new(),
    }))
}

pub(crate) async fn apply_backend(
    backend: &dyn LixBackend,
    statement: Statement,
    params: &[Value],
) -> Result<Option<RewriteOutput>, LixError> {
    let Statement::Query(query) = statement else {
        return Ok(None);
    };
    let rewritten = rewrite_read_query_with_backend_and_params(backend, *query, params).await?;
    Ok(Some(RewriteOutput {
        statements: vec![Statement::Query(Box::new(rewritten))],
        params: Vec::new(),
        registrations: Vec::new(),
        postprocess: None,
        mutations: Vec::new(),
        update_validations: Vec::new(),
    }))
}
