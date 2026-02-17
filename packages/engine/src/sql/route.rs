use sqlparser::ast::{Insert, Query, Statement};

use crate::cel::CelEvaluator;
use crate::functions::LixFunctionProvider;
use crate::functions::SharedFunctionProvider;
use crate::sql::entity_views::{read as entity_view_read, write as entity_view_write};
use crate::sql::planner::{effective_state_read, state_history_read};
use crate::sql::steps::{
    filesystem_step, lix_active_account_view_read, lix_active_account_view_write,
    lix_active_version_view_read, lix_active_version_view_write, lix_state_by_version_view_write,
    lix_state_history_view_write, lix_state_view_write, lix_version_view_read,
    lix_version_view_write, stored_schema, vtable_read, vtable_write,
};
use crate::sql::types::{
    MutationRow, PostprocessPlan, RewriteOutput, SchemaRegistration, UpdateValidationPlan,
};
use crate::sql::{expr_references_column_name, ColumnReferenceOptions, DetectedFileDomainChange};
use crate::{LixBackend, LixError, Value};

pub fn rewrite_statement<P: LixFunctionProvider>(
    statement: Statement,
    params: &[Value],
    functions: &mut P,
) -> Result<RewriteOutput, LixError> {
    rewrite_statement_with_writer_key(statement, params, functions, None)
}

pub(crate) fn rewrite_statement_with_writer_key<P: LixFunctionProvider>(
    statement: Statement,
    params: &[Value],
    functions: &mut P,
    writer_key: Option<&str>,
) -> Result<RewriteOutput, LixError> {
    const MAX_REWRITE_PASSES: usize = 32;
    let mut current = statement;

    for _ in 0..MAX_REWRITE_PASSES {
        match current {
            Statement::Insert(insert) => {
                lix_state_history_view_write::reject_insert(&insert)?;
                if let Some(rewritten) = filesystem_step::rewrite_insert(insert.clone())? {
                    current = Statement::Insert(rewritten);
                    continue;
                }
                if let Some(version_inserts) =
                    lix_version_view_write::rewrite_insert(insert.clone(), params)?
                {
                    return rewrite_vtable_inserts(version_inserts, params, functions, writer_key);
                }
                if let Some(active_account_inserts) =
                    lix_active_account_view_write::rewrite_insert(insert.clone(), params)?
                {
                    return rewrite_vtable_inserts(
                        active_account_inserts,
                        params,
                        functions,
                        writer_key,
                    );
                }
                if let Some(rewritten) = entity_view_write::rewrite_insert(insert.clone(), params)?
                {
                    current = Statement::Insert(rewritten);
                    continue;
                }

                let mut current_insert = Statement::Insert(insert);
                if let Statement::Insert(inner) = &current_insert {
                    if let Some(rewritten) =
                        lix_state_by_version_view_write::rewrite_insert(inner.clone())?
                    {
                        current_insert = Statement::Insert(rewritten);
                    }
                }
                let mut registrations: Vec<SchemaRegistration> = Vec::new();
                let mut statements: Vec<Statement> = Vec::new();
                let mut mutations: Vec<MutationRow> = Vec::new();
                let update_validations: Vec<UpdateValidationPlan> = Vec::new();

                if let Statement::Insert(inner) = &current_insert {
                    if let Some(rewritten) = stored_schema::rewrite_insert(inner.clone(), params)? {
                        registrations.push(rewritten.registration);
                        mutations.push(rewritten.mutation);
                        current_insert = rewritten.statement;
                    }
                }
                if let Statement::Insert(inner) = &current_insert {
                    if let Some(rewritten) = vtable_write::rewrite_insert_with_writer_key(
                        inner.clone(),
                        params,
                        writer_key,
                        functions,
                    )? {
                        registrations.extend(rewritten.registrations);
                        statements = rewritten.statements;
                        mutations = rewritten.mutations;
                    }
                }

                if statements.is_empty() {
                    statements.push(current_insert);
                }

                return Ok(RewriteOutput {
                    statements,
                    registrations,
                    postprocess: None,
                    mutations,
                    update_validations,
                });
            }
            Statement::Update(update) => {
                lix_state_history_view_write::reject_update(&update)?;
                if let Some(rewritten) = filesystem_step::rewrite_update(update.clone())? {
                    current = rewritten;
                    continue;
                }
                if let Some(rewritten) = entity_view_write::rewrite_update(update.clone(), params)?
                {
                    current = Statement::Update(rewritten);
                    continue;
                }
                let update = if let Some(rewritten) =
                    lix_state_by_version_view_write::rewrite_update(update.clone())?
                {
                    rewritten
                } else {
                    update
                };
                let rewritten = vtable_write::rewrite_update(update.clone(), params)?;
                return match rewritten {
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
                };
            }
            Statement::Delete(delete) => {
                lix_state_history_view_write::reject_delete(&delete)?;
                if let Some(rewritten) = filesystem_step::rewrite_delete(delete.clone())? {
                    current = Statement::Delete(rewritten);
                    continue;
                }
                if let Some(rewritten) = entity_view_write::rewrite_delete(delete.clone())? {
                    current = Statement::Delete(rewritten);
                    continue;
                }
                let mut effective_scope_fallback = false;
                let delete = if let Some(rewritten) =
                    lix_state_by_version_view_write::rewrite_delete(delete.clone())?
                {
                    effective_scope_fallback = true;
                    rewritten
                } else {
                    delete
                };
                let rewritten = if effective_scope_fallback {
                    vtable_write::rewrite_delete_with_options(delete.clone(), true)?
                } else {
                    vtable_write::rewrite_delete(delete.clone())?
                };
                return match rewritten {
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
                };
            }
            Statement::Query(query) => {
                let query = rewrite_read_query(*query)?;
                return Ok(RewriteOutput {
                    statements: vec![Statement::Query(Box::new(query))],
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                });
            }
            Statement::Explain {
                describe_alias,
                analyze,
                verbose,
                query_plan,
                estimate,
                statement,
                format,
                options,
            } => {
                let statement = match *statement {
                    Statement::Query(query) => {
                        Statement::Query(Box::new(rewrite_read_query(*query)?))
                    }
                    other => other,
                };
                return Ok(RewriteOutput {
                    statements: vec![Statement::Explain {
                        describe_alias,
                        analyze,
                        verbose,
                        query_plan,
                        estimate,
                        statement: Box::new(statement),
                        format,
                        options,
                    }],
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                });
            }
            other => {
                return Ok(RewriteOutput {
                    statements: vec![other],
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                });
            }
        }
    }

    Err(LixError {
        message: "statement rewrite exceeded maximum pass count".to_string(),
    })
}

pub async fn rewrite_statement_with_backend<P>(
    backend: &dyn LixBackend,
    statement: Statement,
    params: &[Value],
    functions: &mut P,
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
) -> Result<RewriteOutput, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    match statement {
        Statement::Insert(insert) => {
            lix_state_history_view_write::reject_insert(&insert)?;
            let filesystem_insert_side_effects =
                filesystem_step::insert_side_effect_statements_with_backend(
                    backend, &insert, params,
                )
                .await?;
            let mut insert_detected_file_domain_changes = detected_file_domain_changes.to_vec();
            insert_detected_file_domain_changes.extend(
                filesystem_insert_side_effects
                    .tracked_directory_changes
                    .clone(),
            );
            let insert = if let Some(rewritten) = filesystem_step::rewrite_insert_with_backend(
                backend,
                insert.clone(),
                params,
                Some(&filesystem_insert_side_effects.resolved_directory_ids),
                filesystem_insert_side_effects.active_version_id.as_deref(),
            )
            .await?
            {
                rewritten
            } else {
                insert
            };
            if let Some(version_inserts) =
                lix_version_view_write::rewrite_insert_with_backend(backend, insert.clone(), params)
                    .await?
            {
                return rewrite_vtable_inserts_with_backend(
                    backend,
                    version_inserts,
                    params,
                    functions,
                    &insert_detected_file_domain_changes,
                    writer_key,
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
                    &insert_detected_file_domain_changes,
                    writer_key,
                )
                .await;
            }
            let insert = if let Some(rewritten) = entity_view_write::rewrite_insert_with_backend(
                backend,
                insert.clone(),
                params,
                &CelEvaluator::new(),
                SharedFunctionProvider::new(functions.clone()),
            )
            .await?
            {
                rewritten
            } else {
                insert
            };

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
                    &insert_detected_file_domain_changes,
                    writer_key,
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

            let mut output = RewriteOutput {
                statements,
                registrations,
                postprocess: None,
                mutations,
                update_validations,
            };

            if !filesystem_insert_side_effects.statements.is_empty() {
                output = prepend_statements_with_backend(
                    backend,
                    filesystem_insert_side_effects.statements,
                    output,
                    params,
                    functions,
                    detected_file_domain_changes,
                    writer_key,
                )
                .await?;
            }

            Ok(output)
        }
        Statement::Update(update) => {
            lix_state_history_view_write::reject_update(&update)?;
            let update = if let Some(rewritten) =
                filesystem_step::rewrite_update_with_backend(backend, update.clone(), params)
                    .await?
            {
                match rewritten {
                    Statement::Update(update) => update,
                    other => {
                        return rewrite_statement_with_writer_key(
                            other, params, functions, writer_key,
                        )
                    }
                }
            } else {
                update
            };
            let update = if let Some(rewritten) =
                entity_view_write::rewrite_update_with_backend(backend, update.clone(), params)
                    .await?
            {
                rewritten
            } else {
                update
            };
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
                    detected_file_domain_changes,
                    writer_key,
                )
                .await;
            }

            if let Some(rewritten) =
                lix_state_by_version_view_write::rewrite_update(update.clone())?
            {
                return rewrite_statement_with_writer_key(
                    Statement::Update(rewritten),
                    params,
                    functions,
                    writer_key,
                );
            }

            if let Some(rewritten) =
                lix_state_view_write::rewrite_update_with_backend(backend, update.clone(), params)
                    .await?
            {
                return rewrite_statement_with_writer_key(
                    Statement::Update(rewritten),
                    params,
                    functions,
                    writer_key,
                );
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
                    detected_file_domain_changes,
                    writer_key,
                )
                .await;
            }

            rewrite_statement_with_writer_key(
                Statement::Update(update),
                params,
                functions,
                writer_key,
            )
        }
        Statement::Delete(delete) => {
            lix_state_history_view_write::reject_delete(&delete)?;
            let mut effective_scope_fallback = false;
            let delete = if let Some(rewritten) =
                filesystem_step::rewrite_delete_with_backend(backend, delete.clone(), params)
                    .await?
            {
                rewritten
            } else {
                delete
            };
            let delete = if let Some(rewritten) =
                entity_view_write::rewrite_delete_with_backend(backend, delete.clone()).await?
            {
                rewritten
            } else {
                delete
            };
            let delete = if let Some(rewritten) =
                lix_state_by_version_view_write::rewrite_delete(delete.clone())?
            {
                effective_scope_fallback = true;
                rewritten
            } else {
                delete
            };
            let output = if let Some(rewritten) =
                lix_active_account_view_write::rewrite_delete_with_backend(
                    backend,
                    delete.clone(),
                    params,
                )
                .await?
            {
                rewrite_statement_with_writer_key(rewritten, params, functions, writer_key)?
            } else {
                let delete = if let Some(rewritten) =
                    lix_state_view_write::rewrite_delete_with_backend(backend, delete.clone())
                        .await?
                {
                    effective_scope_fallback =
                        !selection_mentions_inherited_from_version_id(delete.selection.as_ref());
                    rewritten
                } else {
                    delete
                };
                if let Some(version_inserts) = lix_version_view_write::rewrite_delete_with_backend(
                    backend,
                    delete.clone(),
                    params,
                )
                .await?
                {
                    rewrite_vtable_inserts_with_backend(
                        backend,
                        version_inserts,
                        params,
                        functions,
                        detected_file_domain_changes,
                        writer_key,
                    )
                    .await?
                } else {
                    let rewritten = vtable_write::rewrite_delete_with_options(
                        delete.clone(),
                        effective_scope_fallback,
                    )?;
                    match rewritten {
                        Some(vtable_write::DeleteRewrite::Statement(statement)) => RewriteOutput {
                            statements: vec![statement],
                            registrations: Vec::new(),
                            postprocess: None,
                            mutations: Vec::new(),
                            update_validations: Vec::new(),
                        },
                        Some(vtable_write::DeleteRewrite::Planned(rewrite)) => RewriteOutput {
                            statements: vec![rewrite.statement],
                            registrations: Vec::new(),
                            postprocess: Some(PostprocessPlan::VtableDelete(rewrite.plan)),
                            mutations: Vec::new(),
                            update_validations: Vec::new(),
                        },
                        None => rewrite_statement_with_writer_key(
                            Statement::Delete(delete),
                            params,
                            functions,
                            writer_key,
                        )?,
                    }
                }
            };
            Ok(output)
        }
        Statement::Query(query) => {
            let query = rewrite_read_query_with_backend_and_params(backend, *query, params).await?;
            Ok(RewriteOutput {
                statements: vec![Statement::Query(Box::new(query))],
                registrations: Vec::new(),
                postprocess: None,
                mutations: Vec::new(),
                update_validations: Vec::new(),
            })
        }
        Statement::Explain {
            describe_alias,
            analyze,
            verbose,
            query_plan,
            estimate,
            statement,
            format,
            options,
        } => {
            let statement = match *statement {
                Statement::Query(query) => Statement::Query(Box::new(
                    rewrite_read_query_with_backend_and_params(backend, *query, params).await?,
                )),
                other => other,
            };
            Ok(RewriteOutput {
                statements: vec![Statement::Explain {
                    describe_alias,
                    analyze,
                    verbose,
                    query_plan,
                    estimate,
                    statement: Box::new(statement),
                    format,
                    options,
                }],
                registrations: Vec::new(),
                postprocess: None,
                mutations: Vec::new(),
                update_validations: Vec::new(),
            })
        }
        other => rewrite_statement_with_writer_key(other, params, functions, writer_key),
    }
}

pub(crate) fn rewrite_read_query(query: Query) -> Result<Query, LixError> {
    let mut current = query;
    for pass in READ_NORMALIZATION_PASSES {
        current = apply_read_normalization_pass_sync(current, *pass)?;
    }
    apply_final_read_planners_sync(current)
}

pub(crate) async fn rewrite_read_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
) -> Result<Query, LixError> {
    rewrite_read_query_with_backend_and_params(backend, query, &[]).await
}

pub(crate) async fn rewrite_read_query_with_backend_and_params(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Query, LixError> {
    let mut current = query;
    for pass in READ_NORMALIZATION_PASSES {
        current =
            apply_read_normalization_pass_with_backend(backend, current, *pass, params).await?;
    }
    apply_final_read_planners_with_backend(backend, current).await
}

#[derive(Clone, Copy)]
enum ReadNormalizationPass {
    Filesystem,
    EntityView,
    LixVersion,
    LixActiveAccount,
    LixActiveVersion,
}

const READ_NORMALIZATION_PASSES: &[ReadNormalizationPass] = &[
    ReadNormalizationPass::Filesystem,
    ReadNormalizationPass::EntityView,
    ReadNormalizationPass::LixVersion,
    ReadNormalizationPass::LixActiveAccount,
    ReadNormalizationPass::LixActiveVersion,
];

fn apply_read_normalization_pass_sync(
    query: Query,
    pass: ReadNormalizationPass,
) -> Result<Query, LixError> {
    Ok(match pass {
        ReadNormalizationPass::Filesystem => {
            filesystem_step::rewrite_query(query.clone())?.unwrap_or(query)
        }
        ReadNormalizationPass::EntityView => {
            entity_view_read::rewrite_query(query.clone())?.unwrap_or(query)
        }
        ReadNormalizationPass::LixVersion => {
            lix_version_view_read::rewrite_query(query.clone())?.unwrap_or(query)
        }
        ReadNormalizationPass::LixActiveAccount => {
            lix_active_account_view_read::rewrite_query(query.clone())?.unwrap_or(query)
        }
        ReadNormalizationPass::LixActiveVersion => {
            lix_active_version_view_read::rewrite_query(query.clone())?.unwrap_or(query)
        }
    })
}

async fn apply_read_normalization_pass_with_backend(
    backend: &dyn LixBackend,
    query: Query,
    pass: ReadNormalizationPass,
    params: &[Value],
) -> Result<Query, LixError> {
    Ok(match pass {
        ReadNormalizationPass::Filesystem => {
            filesystem_step::rewrite_query_with_params(query.clone(), params)?.unwrap_or(query)
        }
        ReadNormalizationPass::EntityView => {
            entity_view_read::rewrite_query_with_backend(backend, query.clone())
                .await?
                .unwrap_or(query)
        }
        ReadNormalizationPass::LixVersion => {
            lix_version_view_read::rewrite_query(query.clone())?.unwrap_or(query)
        }
        ReadNormalizationPass::LixActiveAccount => {
            lix_active_account_view_read::rewrite_query(query.clone())?.unwrap_or(query)
        }
        ReadNormalizationPass::LixActiveVersion => {
            lix_active_version_view_read::rewrite_query(query.clone())?.unwrap_or(query)
        }
    })
}

fn apply_final_read_planners_sync(query: Query) -> Result<Query, LixError> {
    let query =
        effective_state_read::rewrite_lix_state_by_version_query(query.clone())?.unwrap_or(query);
    let query = effective_state_read::rewrite_lix_state_query(query.clone())?.unwrap_or(query);
    let query =
        state_history_read::rewrite_lix_state_history_query(query.clone())?.unwrap_or(query);
    Ok(vtable_read::rewrite_query(query.clone())?.unwrap_or(query))
}

async fn apply_final_read_planners_with_backend(
    backend: &dyn LixBackend,
    query: Query,
) -> Result<Query, LixError> {
    let query =
        effective_state_read::rewrite_lix_state_by_version_query(query.clone())?.unwrap_or(query);
    let query = effective_state_read::rewrite_lix_state_query(query.clone())?.unwrap_or(query);
    let query =
        state_history_read::rewrite_lix_state_history_query(query.clone())?.unwrap_or(query);
    Ok(
        vtable_read::rewrite_query_with_backend(backend, query.clone())
            .await?
            .unwrap_or(query),
    )
}

fn selection_mentions_inherited_from_version_id(selection: Option<&sqlparser::ast::Expr>) -> bool {
    selection
        .map(|expr| {
            expr_references_column_name(
                expr,
                "inherited_from_version_id",
                ColumnReferenceOptions {
                    include_from_derived_subqueries: true,
                },
            )
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::selection_mentions_inherited_from_version_id;
    use sqlparser::ast::{Expr, Value, ValueWithSpan};

    #[test]
    fn inherited_column_detection_ignores_string_literals() {
        let selection = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("metadata".into())),
            op: sqlparser::ast::BinaryOperator::Eq,
            right: Box::new(Expr::Value(ValueWithSpan::from(Value::SingleQuotedString(
                "inherited_from_version_id".to_string(),
            )))),
        };
        assert!(!selection_mentions_inherited_from_version_id(Some(
            &selection
        )));
    }

    #[test]
    fn inherited_column_detection_matches_real_column_reference() {
        let selection = Expr::IsNull(Box::new(Expr::CompoundIdentifier(vec![
            "ranked".into(),
            "inherited_from_version_id".into(),
        ])));
        assert!(selection_mentions_inherited_from_version_id(Some(
            &selection
        )));
    }
}

async fn prepend_statements_with_backend<P>(
    backend: &dyn LixBackend,
    side_effects: Vec<Statement>,
    mut output: RewriteOutput,
    params: &[Value],
    functions: &mut P,
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
) -> Result<RewriteOutput, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    if side_effects.is_empty() {
        return Ok(output);
    }

    let mut prefixed = RewriteOutput {
        statements: Vec::new(),
        registrations: Vec::new(),
        postprocess: None,
        mutations: Vec::new(),
        update_validations: Vec::new(),
    };

    for statement in side_effects {
        let rewritten = Box::pin(rewrite_statement_with_backend(
            backend,
            statement,
            params,
            functions,
            detected_file_domain_changes,
            writer_key,
        ))
        .await?;
        merge_rewrite_output(&mut prefixed, rewritten)?;
    }

    merge_rewrite_output(
        &mut prefixed,
        std::mem::replace(
            &mut output,
            RewriteOutput {
                statements: Vec::new(),
                registrations: Vec::new(),
                postprocess: None,
                mutations: Vec::new(),
                update_validations: Vec::new(),
            },
        ),
    )?;

    Ok(prefixed)
}

fn merge_rewrite_output(base: &mut RewriteOutput, mut next: RewriteOutput) -> Result<(), LixError> {
    if base.postprocess.is_some() && next.postprocess.is_some() {
        return Err(LixError {
            message: "only one postprocess rewrite is supported per query".to_string(),
        });
    }
    if base.postprocess.is_none() {
        base.postprocess = next.postprocess.take();
    }
    base.statements.extend(next.statements);
    base.registrations.extend(next.registrations);
    base.mutations.extend(next.mutations);
    base.update_validations.extend(next.update_validations);
    Ok(())
}

fn rewrite_vtable_inserts<P: LixFunctionProvider>(
    inserts: Vec<Insert>,
    params: &[Value],
    functions: &mut P,
    writer_key: Option<&str>,
) -> Result<RewriteOutput, LixError> {
    let mut statements = Vec::new();
    let mut registrations = Vec::new();
    let mut mutations = Vec::new();

    for insert in inserts {
        let Some(rewritten) =
            vtable_write::rewrite_insert_with_writer_key(insert, params, writer_key, functions)?
        else {
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
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
) -> Result<RewriteOutput, LixError> {
    let mut statements = Vec::new();
    let mut registrations = Vec::new();
    let mut mutations = Vec::new();

    for insert in inserts {
        let Some(rewritten) = vtable_write::rewrite_insert_with_backend(
            backend,
            insert,
            params,
            detected_file_domain_changes,
            writer_key,
            functions,
        )
        .await?
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
