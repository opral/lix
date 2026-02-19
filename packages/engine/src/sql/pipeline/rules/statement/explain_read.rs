use sqlparser::ast::Statement;

use crate::sql::pipeline::query_engine::{
    rewrite_read_query, rewrite_read_query_with_backend_and_params,
};
use crate::sql::types::RewriteOutput;
use crate::{LixBackend, LixError, Value};

pub(crate) fn apply_sync(statement: Statement) -> Result<Option<RewriteOutput>, LixError> {
    let Statement::Explain {
        describe_alias,
        analyze,
        verbose,
        query_plan,
        estimate,
        statement,
        format,
        options,
    } = statement
    else {
        return Ok(None);
    };

    let rewritten_statement = match *statement {
        Statement::Query(query) => Statement::Query(Box::new(rewrite_read_query(*query)?)),
        other => other,
    };

    Ok(Some(RewriteOutput {
        statements: vec![Statement::Explain {
            describe_alias,
            analyze,
            verbose,
            query_plan,
            estimate,
            statement: Box::new(rewritten_statement),
            format,
            options,
        }],
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
    let Statement::Explain {
        describe_alias,
        analyze,
        verbose,
        query_plan,
        estimate,
        statement,
        format,
        options,
    } = statement
    else {
        return Ok(None);
    };

    let rewritten_statement = match *statement {
        Statement::Query(query) => Statement::Query(Box::new(
            rewrite_read_query_with_backend_and_params(backend, *query, params).await?,
        )),
        other => other,
    };

    Ok(Some(RewriteOutput {
        statements: vec![Statement::Explain {
            describe_alias,
            analyze,
            verbose,
            query_plan,
            estimate,
            statement: Box::new(rewritten_statement),
            format,
            options,
        }],
        params: Vec::new(),
        registrations: Vec::new(),
        postprocess: None,
        mutations: Vec::new(),
        update_validations: Vec::new(),
    }))
}
