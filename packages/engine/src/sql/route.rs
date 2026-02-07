use sqlparser::ast::{Insert, Statement};

use crate::functions::LixFunctionProvider;
use crate::sql::steps::{
    lix_active_account_view_read, lix_active_account_view_write, lix_active_version_view_read,
    lix_active_version_view_write, lix_state_by_version_view_read, lix_state_by_version_view_write,
    lix_state_view_read, lix_state_view_write, lix_version_view_read, lix_version_view_write,
    stored_schema, vtable_read, vtable_write,
};
use crate::sql::types::{
    MutationRow, PostprocessPlan, RewriteOutput, SchemaRegistration, UpdateValidationPlan,
};
use crate::{LixBackend, LixError, Value};

pub fn rewrite_statement<P: LixFunctionProvider>(
    statement: Statement,
    params: &[Value],
    functions: &mut P,
) -> Result<RewriteOutput, LixError> {
    match statement {
        Statement::Insert(insert) => {
            if let Some(version_inserts) =
                lix_version_view_write::rewrite_insert(insert.clone(), params)?
            {
                return rewrite_vtable_inserts(version_inserts, params, functions);
            }
            if let Some(active_account_inserts) =
                lix_active_account_view_write::rewrite_insert(insert.clone(), params)?
            {
                return rewrite_vtable_inserts(active_account_inserts, params, functions);
            }

            let mut current = Statement::Insert(insert);
            if let Statement::Insert(inner) = &current {
                if let Some(rewritten) =
                    lix_state_by_version_view_write::rewrite_insert(inner.clone())?
                {
                    current = Statement::Insert(rewritten);
                }
            }
            let mut registrations: Vec<SchemaRegistration> = Vec::new();
            let mut statements: Vec<Statement> = Vec::new();
            let mut mutations: Vec<MutationRow> = Vec::new();
            let update_validations: Vec<UpdateValidationPlan> = Vec::new();

            if let Statement::Insert(inner) = &current {
                if let Some(rewritten) = stored_schema::rewrite_insert(inner.clone(), params)? {
                    registrations.push(rewritten.registration);
                    mutations.push(rewritten.mutation);
                    current = rewritten.statement;
                }
            }
            if let Statement::Insert(inner) = &current {
                if let Some(rewritten) =
                    vtable_write::rewrite_insert(inner.clone(), params, functions)?
                {
                    registrations.extend(rewritten.registrations);
                    statements = rewritten.statements;
                    mutations = rewritten.mutations;
                }
            }

            if statements.is_empty() {
                statements.push(current);
            }

            Ok(RewriteOutput {
                statements,
                registrations,
                postprocess: None,
                mutations,
                update_validations,
            })
        }
        Statement::Update(update) => {
            let update = if let Some(rewritten) =
                lix_state_by_version_view_write::rewrite_update(update.clone())?
            {
                rewritten
            } else {
                update
            };
            let rewritten = vtable_write::rewrite_update(update.clone(), params)?;
            match rewritten {
                Some(vtable_write::UpdateRewrite::Statement(rewrite)) => Ok(RewriteOutput {
                    statements: vec![rewrite.statement],
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: rewrite.validation.into_iter().collect(),
                }),
                Some(vtable_write::UpdateRewrite::Planned(rewrite)) => Ok(RewriteOutput {
                    statements: vec![rewrite.statement],
                    registrations: Vec::new(),
                    postprocess: Some(PostprocessPlan::VtableUpdate(rewrite.plan)),
                    mutations: Vec::new(),
                    update_validations: rewrite.validation.into_iter().collect(),
                }),
                None => Ok(RewriteOutput {
                    statements: vec![Statement::Update(update)],
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                }),
            }
        }
        Statement::Delete(delete) => {
            let delete = if let Some(rewritten) =
                lix_state_by_version_view_write::rewrite_delete(delete.clone())?
            {
                rewritten
            } else {
                delete
            };
            let rewritten = vtable_write::rewrite_delete(delete.clone())?;
            match rewritten {
                Some(vtable_write::DeleteRewrite::Statement(statement)) => Ok(RewriteOutput {
                    statements: vec![statement],
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                }),
                Some(vtable_write::DeleteRewrite::Planned(rewrite)) => Ok(RewriteOutput {
                    statements: vec![rewrite.statement],
                    registrations: Vec::new(),
                    postprocess: Some(PostprocessPlan::VtableDelete(rewrite.plan)),
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                }),
                None => Ok(RewriteOutput {
                    statements: vec![Statement::Delete(delete)],
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                }),
            }
        }
        Statement::Query(query) => {
            let query = *query;
            let query = lix_version_view_read::rewrite_query(query.clone())?.unwrap_or(query);
            let query =
                lix_active_account_view_read::rewrite_query(query.clone())?.unwrap_or(query);
            let query =
                lix_active_version_view_read::rewrite_query(query.clone())?.unwrap_or(query);
            let query =
                lix_state_by_version_view_read::rewrite_query(query.clone())?.unwrap_or(query);
            let query = lix_state_view_read::rewrite_query(query.clone())?.unwrap_or(query);
            let query = vtable_read::rewrite_query(query.clone())?.unwrap_or(query);
            Ok(RewriteOutput {
                statements: vec![Statement::Query(Box::new(query))],
                registrations: Vec::new(),
                postprocess: None,
                mutations: Vec::new(),
                update_validations: Vec::new(),
            })
        }
        other => Ok(RewriteOutput {
            statements: vec![other],
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        }),
    }
}

pub async fn rewrite_statement_with_backend<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    statement: Statement,
    params: &[Value],
    functions: &mut P,
) -> Result<RewriteOutput, LixError> {
    match statement {
        Statement::Insert(insert) => {
            if let Some(version_inserts) =
                lix_version_view_write::rewrite_insert_with_backend(backend, insert.clone(), params)
                    .await?
            {
                return rewrite_vtable_inserts_with_backend(
                    backend,
                    version_inserts,
                    params,
                    functions,
                )
                .await;
            }
            if let Some(active_account_inserts) =
                lix_active_account_view_write::rewrite_insert(insert.clone(), params)?
            {
                return rewrite_vtable_inserts_with_backend(
                    backend,
                    active_account_inserts,
                    params,
                    functions,
                )
                .await;
            }

            let mut current = Statement::Insert(insert);
            if let Statement::Insert(inner) = &current {
                if let Some(rewritten) =
                    lix_state_by_version_view_write::rewrite_insert(inner.clone())?
                {
                    current = Statement::Insert(rewritten);
                }
            }
            if let Statement::Insert(inner) = &current {
                if let Some(rewritten) =
                    lix_state_view_write::rewrite_insert_with_backend(backend, inner.clone())
                        .await?
                {
                    current = Statement::Insert(rewritten);
                }
            }
            let mut registrations: Vec<SchemaRegistration> = Vec::new();
            let mut statements: Vec<Statement> = Vec::new();
            let mut mutations: Vec<MutationRow> = Vec::new();
            let update_validations: Vec<UpdateValidationPlan> = Vec::new();

            if let Statement::Insert(inner) = &current {
                if let Some(rewritten) = stored_schema::rewrite_insert(inner.clone(), params)? {
                    registrations.push(rewritten.registration);
                    mutations.push(rewritten.mutation);
                    current = rewritten.statement;
                }
            }
            if let Statement::Insert(inner) = &current {
                if let Some(rewritten) = vtable_write::rewrite_insert_with_backend(
                    backend,
                    inner.clone(),
                    params,
                    functions,
                )
                .await?
                {
                    registrations.extend(rewritten.registrations);
                    statements = rewritten.statements;
                    mutations = rewritten.mutations;
                }
            }

            if statements.is_empty() {
                statements.push(current);
            }

            Ok(RewriteOutput {
                statements,
                registrations,
                postprocess: None,
                mutations,
                update_validations,
            })
        }
        Statement::Update(update) => {
            if let Some(active_version_inserts) =
                lix_active_version_view_write::rewrite_update_with_backend(
                    backend,
                    update.clone(),
                    params,
                )
                .await?
            {
                return rewrite_vtable_inserts_with_backend(
                    backend,
                    active_version_inserts,
                    params,
                    functions,
                )
                .await;
            }

            if let Some(rewritten) =
                lix_state_by_version_view_write::rewrite_update(update.clone())?
            {
                return rewrite_statement(Statement::Update(rewritten), params, functions);
            }

            if let Some(rewritten) =
                lix_state_view_write::rewrite_update_with_backend(backend, update.clone(), params)
                    .await?
            {
                return rewrite_statement(Statement::Update(rewritten), params, functions);
            }

            if let Some(version_inserts) =
                lix_version_view_write::rewrite_update_with_backend(backend, update.clone(), params)
                    .await?
            {
                return rewrite_vtable_inserts_with_backend(
                    backend,
                    version_inserts,
                    params,
                    functions,
                )
                .await;
            }

            rewrite_statement(Statement::Update(update), params, functions)
        }
        Statement::Delete(delete) => {
            if let Some(rewritten) =
                lix_state_by_version_view_write::rewrite_delete(delete.clone())?
            {
                return rewrite_statement(Statement::Delete(rewritten), params, functions);
            }

            if let Some(rewritten) = lix_active_account_view_write::rewrite_delete_with_backend(
                backend,
                delete.clone(),
                params,
            )
            .await?
            {
                return rewrite_statement(rewritten, params, functions);
            }

            if let Some(rewritten) =
                lix_state_view_write::rewrite_delete_with_backend(backend, delete.clone()).await?
            {
                return rewrite_statement(Statement::Delete(rewritten), params, functions);
            }

            if let Some(version_inserts) =
                lix_version_view_write::rewrite_delete_with_backend(backend, delete.clone(), params)
                    .await?
            {
                return rewrite_vtable_inserts_with_backend(
                    backend,
                    version_inserts,
                    params,
                    functions,
                )
                .await;
            }

            rewrite_statement(Statement::Delete(delete), params, functions)
        }
        Statement::Query(query) => {
            let query = *query;
            let query = lix_version_view_read::rewrite_query(query.clone())?.unwrap_or(query);
            let query =
                lix_active_account_view_read::rewrite_query(query.clone())?.unwrap_or(query);
            let query =
                lix_active_version_view_read::rewrite_query(query.clone())?.unwrap_or(query);
            let query =
                lix_state_by_version_view_read::rewrite_query(query.clone())?.unwrap_or(query);
            let query = lix_state_view_read::rewrite_query(query.clone())?.unwrap_or(query);
            let query = vtable_read::rewrite_query_with_backend(backend, query.clone())
                .await?
                .unwrap_or(query);
            Ok(RewriteOutput {
                statements: vec![Statement::Query(Box::new(query))],
                registrations: Vec::new(),
                postprocess: None,
                mutations: Vec::new(),
                update_validations: Vec::new(),
            })
        }
        other => rewrite_statement(other, params, functions),
    }
}

fn rewrite_vtable_inserts<P: LixFunctionProvider>(
    inserts: Vec<Insert>,
    params: &[Value],
    functions: &mut P,
) -> Result<RewriteOutput, LixError> {
    let mut statements = Vec::new();
    let mut registrations = Vec::new();
    let mut mutations = Vec::new();

    for insert in inserts {
        let Some(rewritten) = vtable_write::rewrite_insert(insert, params, functions)? else {
            return Err(LixError {
                message: "lix_version rewrite expected vtable insert rewrite".to_string(),
            });
        };
        statements.extend(rewritten.statements);
        registrations.extend(rewritten.registrations);
        mutations.extend(rewritten.mutations);
    }

    Ok(RewriteOutput {
        statements,
        registrations,
        postprocess: None,
        mutations,
        update_validations: Vec::new(),
    })
}

async fn rewrite_vtable_inserts_with_backend<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    inserts: Vec<Insert>,
    params: &[Value],
    functions: &mut P,
) -> Result<RewriteOutput, LixError> {
    let mut statements = Vec::new();
    let mut registrations = Vec::new();
    let mut mutations = Vec::new();

    for insert in inserts {
        let Some(rewritten) =
            vtable_write::rewrite_insert_with_backend(backend, insert, params, functions).await?
        else {
            return Err(LixError {
                message: "lix_version rewrite expected backend vtable insert rewrite".to_string(),
            });
        };
        statements.extend(rewritten.statements);
        registrations.extend(rewritten.registrations);
        mutations.extend(rewritten.mutations);
    }

    Ok(RewriteOutput {
        statements,
        registrations,
        postprocess: None,
        mutations,
        update_validations: Vec::new(),
    })
}
