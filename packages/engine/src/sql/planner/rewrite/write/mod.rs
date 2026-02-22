use std::collections::VecDeque;

use sqlparser::ast::Statement;

use crate::functions::LixFunctionProvider;
use crate::sql::steps::lix_state_history_view_write;
use crate::sql::DetectedFileDomainChange;
use crate::{LixBackend, LixError, Value};

pub(crate) mod context;
pub(crate) mod entity_view_write;
pub(crate) mod filesystem_write;
pub(crate) mod helpers;
pub(crate) mod lix_active_account_write;
pub(crate) mod lix_active_version_write;
pub(crate) mod lix_state_by_version_write;
pub(crate) mod lix_state_write;
pub(crate) mod lix_version_write;
pub(crate) mod outcome;
pub(crate) mod stored_schema_write;
pub(crate) mod types;
pub(crate) mod vtable_write;

use self::context::StatementContext;
use self::helpers::{merge_rewrite_output, rewrite_vtable_inserts_with_backend};
use self::outcome::StatementRuleOutcome;
use self::types::WriteRewriteOutput;

const MAX_REWRITE_PASSES: usize = 32;

pub(crate) async fn rewrite_backend_statement<P>(
    backend: &dyn LixBackend,
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    functions: &mut P,
    detected_file_domain_changes: &[DetectedFileDomainChange],
) -> Result<Option<WriteRewriteOutput>, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    enum Pending {
        Statement(Statement),
        Output(WriteRewriteOutput),
    }

    let mut queue = VecDeque::from([Pending::Statement(statement)]);
    let mut final_output = WriteRewriteOutput {
        statements: Vec::new(),
        params: Vec::new(),
        registrations: Vec::new(),
        postprocess: None,
        mutations: Vec::new(),
        update_validations: Vec::new(),
    };

    while let Some(pending) = queue.pop_front() {
        match pending {
            Pending::Output(output) => {
                merge_rewrite_output(&mut final_output, output)?;
            }
            Pending::Statement(statement) => {
                let mut context = StatementContext::new_backend(
                    backend,
                    params,
                    writer_key,
                    detected_file_domain_changes,
                );
                let outcome = rewrite_backend_loop(statement, &mut context, functions).await?;
                let side_effects = std::mem::take(&mut context.side_effects);

                match outcome {
                    StatementRuleOutcome::Continue(statement) => {
                        return Err(LixError {
                            message: format!(
                                "write canonical backend rewrite terminated without convergence for statement: {statement}"
                            ),
                        });
                    }
                    StatementRuleOutcome::Emit(output) => {
                        queue.push_front(Pending::Output(output));
                        for side_effect in side_effects.into_iter().rev() {
                            queue.push_front(Pending::Statement(side_effect));
                        }
                    }
                    StatementRuleOutcome::NoMatch => {}
                }
            }
        }
    }

    if final_output.statements.is_empty() {
        Ok(None)
    } else {
        Ok(Some(final_output))
    }
}

async fn rewrite_backend_loop<P>(
    statement: Statement,
    context: &mut StatementContext<'_>,
    functions: &mut P,
) -> Result<StatementRuleOutcome, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    let Some(backend) = context.backend else {
        return Err(LixError {
            message: "backend rewrite requested without backend context".to_string(),
        });
    };

    let mut current = statement;

    if !matches!(
        current,
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
    ) {
        return Ok(StatementRuleOutcome::NoMatch);
    }

    for _ in 0..MAX_REWRITE_PASSES {
        match current {
            Statement::Insert(insert) => {
                lix_state_history_view_write::reject_insert(&insert)?;

                let filesystem_insert_side_effects =
                    filesystem_write::insert_side_effects_with_backend(
                        backend,
                        &insert,
                        context.params,
                    )
                    .await?;
                context.side_effects = filesystem_insert_side_effects.statements.clone();

                let mut insert_detected_file_domain_changes =
                    context.detected_file_domain_changes.to_vec();
                insert_detected_file_domain_changes.extend(
                    filesystem_insert_side_effects
                        .tracked_directory_changes
                        .clone(),
                );

                let insert = if let Some(rewritten) = filesystem_write::rewrite_insert_with_backend(
                    backend,
                    insert.clone(),
                    context.params,
                    Some(&filesystem_insert_side_effects.resolved_directory_ids),
                    filesystem_insert_side_effects.active_version_id.as_deref(),
                )
                .await?
                {
                    rewritten
                } else {
                    insert
                };

                if let Some(version_inserts) = lix_version_write::rewrite_insert_with_backend(
                    backend,
                    insert.clone(),
                    context.params,
                )
                .await?
                {
                    let output = rewrite_vtable_inserts_with_backend(
                        backend,
                        version_inserts,
                        context.params,
                        functions,
                        &insert_detected_file_domain_changes,
                        context.writer_key,
                    )
                    .await?;
                    return Ok(StatementRuleOutcome::Emit(output));
                }

                if let Some(active_account_inserts) =
                    lix_active_account_write::rewrite_insert(insert.clone(), context.params)?
                {
                    let output = rewrite_vtable_inserts_with_backend(
                        backend,
                        active_account_inserts,
                        context.params,
                        functions,
                        &insert_detected_file_domain_changes,
                        context.writer_key,
                    )
                    .await?;
                    return Ok(StatementRuleOutcome::Emit(output));
                }

                let insert = if let Some(rewritten) =
                    entity_view_write::rewrite_insert_with_backend(
                        backend,
                        insert.clone(),
                        context.params,
                        functions,
                    )
                    .await?
                {
                    rewritten
                } else {
                    insert
                };

                let mut current_insert = insert;
                if let Some(rewritten) =
                    lix_state_by_version_write::rewrite_insert(current_insert.clone())?
                {
                    current_insert = rewritten;
                }
                if let Some(rewritten) =
                    lix_state_write::rewrite_insert_with_backend(backend, current_insert.clone())
                        .await?
                {
                    current_insert = rewritten;
                }

                if let Some(rewritten) =
                    stored_schema_write::rewrite_insert(current_insert.clone(), context.params)?
                {
                    context.registrations.push(rewritten.registration);
                    context.mutations.push(rewritten.mutation);
                    let Statement::Insert(insert_statement) = rewritten.statement else {
                        return Err(LixError {
                            message: "stored schema rewrite expected insert statement".to_string(),
                        });
                    };
                    current_insert = insert_statement;
                }

                let mut statements = Vec::new();
                if let Some(rewritten) = vtable_write::rewrite_insert_with_backend(
                    backend,
                    current_insert.clone(),
                    context.params,
                    context.generated_params.len(),
                    &insert_detected_file_domain_changes,
                    context.writer_key,
                    functions,
                )
                .await?
                {
                    context.registrations.extend(rewritten.registrations);
                    context.generated_params.extend(rewritten.params);
                    context.mutations.extend(rewritten.mutations);
                    statements = rewritten.statements;
                }

                if statements.is_empty() {
                    statements.push(Statement::Insert(current_insert));
                }

                return Ok(StatementRuleOutcome::Emit(context.take_output(statements)));
            }
            Statement::Update(update) => {
                lix_state_history_view_write::reject_update(&update)?;

                if let Some(rewritten) = filesystem_write::rewrite_update_with_backend(
                    backend,
                    update.clone(),
                    context.params,
                )
                .await?
                {
                    current = rewritten;
                    continue;
                }

                let update = if let Some(rewritten) =
                    entity_view_write::rewrite_update_with_backend(
                        backend,
                        update.clone(),
                        context.params,
                    )
                    .await?
                {
                    rewritten
                } else {
                    update
                };

                if let Some(active_version_inserts) =
                    lix_active_version_write::rewrite_update_with_backend(
                        backend,
                        update.clone(),
                        context.params,
                    )
                    .await?
                {
                    let output = rewrite_vtable_inserts_with_backend(
                        backend,
                        active_version_inserts,
                        context.params,
                        functions,
                        context.detected_file_domain_changes,
                        context.writer_key,
                    )
                    .await?;
                    return Ok(StatementRuleOutcome::Emit(output));
                }

                if let Some(rewritten) = lix_state_by_version_write::rewrite_update(update.clone())?
                {
                    current = Statement::Update(rewritten);
                    continue;
                }

                if let Some(rewritten) = lix_state_write::rewrite_update_with_backend(
                    backend,
                    update.clone(),
                    context.params,
                )
                .await?
                {
                    current = Statement::Update(rewritten);
                    continue;
                }

                if let Some(version_inserts) = lix_version_write::rewrite_update_with_backend(
                    backend,
                    update.clone(),
                    context.params,
                )
                .await?
                {
                    let output = rewrite_vtable_inserts_with_backend(
                        backend,
                        version_inserts,
                        context.params,
                        functions,
                        context.detected_file_domain_changes,
                        context.writer_key,
                    )
                    .await?;
                    return Ok(StatementRuleOutcome::Emit(output));
                }

                let output = vtable_write::rewrite_update(update, context.params)?;
                return Ok(StatementRuleOutcome::Emit(output));
            }
            Statement::Delete(delete) => {
                lix_state_history_view_write::reject_delete(&delete)?;

                let mut effective_scope_fallback = false;
                let delete = if let Some(rewritten) = filesystem_write::rewrite_delete_with_backend(
                    backend,
                    delete.clone(),
                    context.params,
                )
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
                    lix_state_by_version_write::rewrite_delete(delete.clone())?
                {
                    effective_scope_fallback = true;
                    rewritten
                } else {
                    delete
                };

                if let Some(rewritten) = lix_active_account_write::rewrite_delete_with_backend(
                    backend,
                    delete.clone(),
                    context.params,
                )
                .await?
                {
                    current = rewritten;
                    continue;
                }

                let delete = if let Some(rewritten) =
                    lix_state_write::rewrite_delete_with_backend(backend, delete.clone()).await?
                {
                    effective_scope_fallback =
                        !vtable_write::selection_mentions_inherited_from_version_id(
                            delete.selection.as_ref(),
                        );
                    rewritten
                } else {
                    delete
                };

                if let Some(version_inserts) = lix_version_write::rewrite_delete_with_backend(
                    backend,
                    delete.clone(),
                    context.params,
                )
                .await?
                {
                    let output = rewrite_vtable_inserts_with_backend(
                        backend,
                        version_inserts,
                        context.params,
                        functions,
                        context.detected_file_domain_changes,
                        context.writer_key,
                    )
                    .await?;
                    return Ok(StatementRuleOutcome::Emit(output));
                }

                let output = vtable_write::rewrite_delete(delete, effective_scope_fallback)?;
                return Ok(StatementRuleOutcome::Emit(output));
            }
            _ => {
                return Ok(StatementRuleOutcome::Emit(
                    context.take_output(vec![current]),
                ));
            }
        }
    }

    Ok(StatementRuleOutcome::Continue(current))
}
