use sqlparser::ast::Statement;

use crate::engine::sql::planning::preprocess::{
    lower_public_read_query_with_sql2_backend, rewrite_public_read_query_to_lowered_sql,
    statement_references_internal_state_vtable,
    statement_references_public_sql2_surface,
    statement_references_public_sql2_surface_with_backend,
};
use crate::engine::sql::planning::rewrite_engine::steps::vtable_read;
use crate::engine::sql::planning::rewrite_engine::types::RewriteOutput;
use crate::{LixBackend, LixError, SqlDialect, Value};

pub(crate) fn apply_sync(statement: Statement) -> Result<Option<RewriteOutput>, LixError> {
    if statement_references_internal_state_vtable(&statement) {
        let Statement::Query(query) = statement else {
            return Ok(None);
        };
        let original = *query;
        let rewritten = vtable_read::rewrite_query(original.clone(), &[])?.unwrap_or(original);
        return Ok(Some(RewriteOutput {
            statements: vec![Statement::Query(Box::new(rewritten))],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        }));
    }
    if !statement_references_public_sql2_surface(&statement) {
        return Ok(None);
    }
    let Statement::Query(query) = statement else {
        return Ok(None);
    };
    let rewritten = rewrite_public_read_query_to_lowered_sql(*query, SqlDialect::Sqlite)?;
    Ok(Some(RewriteOutput {
        statements: vec![Statement::Query(Box::new(rewritten))],
        effect_only: false,
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
    if statement_references_internal_state_vtable(&statement) {
        let Statement::Query(query) = statement else {
            return Ok(None);
        };
        let original = *query;
        let rewritten = vtable_read::rewrite_query_with_backend(backend, original.clone(), params)
            .await?
            .unwrap_or(original);
        return Ok(Some(RewriteOutput {
            statements: vec![Statement::Query(Box::new(rewritten))],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        }));
    }
    if !statement_references_public_sql2_surface_with_backend(backend, &statement).await {
        return Ok(None);
    }
    let Statement::Query(query) = statement else {
        return Ok(None);
    };
    let rewritten = lower_public_read_query_with_sql2_backend(backend, *query, params).await?;
    Ok(Some(RewriteOutput {
        statements: vec![Statement::Query(Box::new(rewritten))],
        effect_only: false,
        params: Vec::new(),
        registrations: Vec::new(),
        postprocess: None,
        mutations: Vec::new(),
        update_validations: Vec::new(),
    }))
}
