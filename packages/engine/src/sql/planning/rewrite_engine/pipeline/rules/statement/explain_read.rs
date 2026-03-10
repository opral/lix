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
            Statement::Query(query) => {
                let original = *query;
                let rewritten =
                    vtable_read::rewrite_query(original.clone(), &[])?.unwrap_or(original);
                Statement::Query(Box::new(rewritten))
            }
            other => other,
        };
        return Ok(Some(RewriteOutput {
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
            rewrite_public_read_query_to_lowered_sql(*query, SqlDialect::Sqlite)?,
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
            Statement::Query(query) => {
                let original = *query;
                let rewritten = vtable_read::rewrite_query_with_backend(
                    backend,
                    original.clone(),
                    params,
                )
                .await?
                .unwrap_or(original);
                Statement::Query(Box::new(rewritten))
            }
            other => other,
        };
        return Ok(Some(RewriteOutput {
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
            lower_public_read_query_with_sql2_backend(backend, *query, params).await?,
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
        effect_only: false,
        params: Vec::new(),
        registrations: Vec::new(),
        postprocess: None,
        mutations: Vec::new(),
        update_validations: Vec::new(),
    }))
}
