use std::collections::VecDeque;

use sqlparser::ast::Statement;

use crate::engine::sql::planning::rewrite_engine::pipeline::query_engine::rewrite_read_query_with_backend_and_params;
use crate::engine::sql::planning::rewrite_engine::steps::lix_change_view_write;
use crate::engine::sql::planning::rewrite_engine::steps::lix_state_history_view_write;
use crate::engine::sql::planning::rewrite_engine::types::RewriteOutput;
use crate::functions::LixFunctionProvider;
use crate::{LixBackend, LixError, Value};

pub(crate) mod entity_view_write;
pub(crate) mod lix_active_account_write;
pub(crate) mod lix_active_version_write;
pub(crate) mod lix_state_by_version_write;
pub(crate) mod lix_state_write;
pub(crate) mod lix_version_write;
pub(crate) mod stored_schema_write;
pub(crate) mod vtable_write;

use super::context::StatementContext;
use super::helpers::{
    merge_rewrite_output, rewrite_vtable_inserts, rewrite_vtable_inserts_with_backend,
};
use super::outcome::StatementRuleOutcome;

const MAX_REWRITE_PASSES: usize = 32;

pub(crate) fn rewrite_sync_statement<P: LixFunctionProvider>(
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    _active_version_id_hint: Option<&str>,
    functions: &mut P,
) -> Result<Option<RewriteOutput>, LixError> {
    let mut context = StatementContext::new_sync(params, writer_key);
    let outcome = rewrite_sync_loop(statement, &mut context, functions)?;

    match outcome {
        StatementRuleOutcome::Emit(output) => Ok(Some(output)),
        StatementRuleOutcome::Continue(statement) => Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                "write canonical sync rewrite terminated without convergence for statement: {statement}"
            ),
        }),
        StatementRuleOutcome::NoMatch => Ok(None),
    }
}

pub(crate) async fn rewrite_backend_statement<P>(
    backend: &dyn LixBackend,
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    active_version_id_hint: Option<&str>,
    functions: &mut P,
) -> Result<Option<RewriteOutput>, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    enum Pending {
        Statement(Statement),
        Output(RewriteOutput),
    }

    let mut queue = VecDeque::from([Pending::Statement(statement)]);
    let mut final_output = RewriteOutput {
        statements: Vec::new(),
        effect_only: false,
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
                    active_version_id_hint,
                );
                let outcome = rewrite_backend_loop(statement, &mut context, functions).await?;
                let side_effects = std::mem::take(&mut context.side_effects);

                match outcome {
                    StatementRuleOutcome::Continue(statement) => {
                        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
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

    if final_output.statements.is_empty() && !final_output.effect_only {
        Ok(None)
    } else {
        Ok(Some(final_output))
    }
}

fn rewrite_sync_loop<P: LixFunctionProvider>(
    statement: Statement,
    context: &mut StatementContext<'_>,
    functions: &mut P,
) -> Result<StatementRuleOutcome, LixError> {
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
                lix_change_view_write::reject_insert(&insert)?;
                lix_state_history_view_write::reject_insert(&insert)?;

                if let Some(version_inserts) =
                    lix_version_write::rewrite_insert(insert.clone(), context.params)?
                {
                    let output = rewrite_vtable_inserts(
                        version_inserts,
                        context.params,
                        functions,
                        context.writer_key,
                    )?;
                    return Ok(StatementRuleOutcome::Emit(output));
                }
                if let Some(active_account_inserts) =
                    lix_active_account_write::rewrite_insert(insert.clone(), context.params)?
                {
                    let output = rewrite_vtable_inserts(
                        active_account_inserts,
                        context.params,
                        functions,
                        context.writer_key,
                    )?;
                    return Ok(StatementRuleOutcome::Emit(output));
                }
                if let Some(rewritten) =
                    entity_view_write::rewrite_insert(insert.clone(), context.params)?
                {
                    current = Statement::Insert(rewritten);
                    continue;
                }

                let mut current_insert = insert;
                let mut supplemental_statements = Vec::new();
                if let Some(rewritten) =
                    lix_state_by_version_write::rewrite_insert(current_insert.clone())?
                {
                    current_insert = rewritten;
                }
                if let Some(rewritten) =
                    stored_schema_write::rewrite_insert(current_insert.clone(), context.params)?
                {
                    context.registrations.push(rewritten.registration);
                    context.mutations.push(rewritten.mutation);
                    supplemental_statements.extend(rewritten.supplemental_statements);
                    let Statement::Insert(insert_statement) = rewritten.statement else {
                        return Err(LixError {
                            code: "LIX_ERROR_UNKNOWN".to_string(),
                            description: "stored schema rewrite expected insert statement"
                                .to_string(),
                        });
                    };
                    current_insert = insert_statement;
                }
                let mut statements = Vec::new();
                if let Some(rewritten) = vtable_write::rewrite_insert_with_writer_key(
                    current_insert.clone(),
                    context.params,
                    context.writer_key,
                    functions,
                )? {
                    context.registrations.extend(rewritten.registrations);
                    context.generated_params.extend(rewritten.params);
                    context.mutations.extend(rewritten.mutations);
                    statements = rewritten.statements;
                }
                if statements.is_empty() {
                    let target = insert_target_name(&current_insert);
                    if is_allowed_internal_write_target(&target) {
                        statements.push(Statement::Insert(current_insert));
                    } else {
                        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                                "strict rewrite violation: statement routing: unsupported INSERT target '{}'",
                                target
                            ),
                        });
                    }
                }
                statements.extend(supplemental_statements);

                return Ok(StatementRuleOutcome::Emit(context.take_output(statements)));
            }
            Statement::Update(update) => {
                lix_change_view_write::reject_update(&update)?;
                lix_state_history_view_write::reject_update(&update)?;

                if let Some(rewritten) =
                    entity_view_write::rewrite_update(update.clone(), context.params)?
                {
                    current = Statement::Update(rewritten);
                    continue;
                }

                let update = if let Some(rewritten) =
                    lix_state_by_version_write::rewrite_update(update.clone())?
                {
                    rewritten
                } else {
                    update
                };

                let output = vtable_write::rewrite_update(update, context.params)?;
                return Ok(StatementRuleOutcome::Emit(output));
            }
            Statement::Delete(delete) => {
                lix_change_view_write::reject_delete(&delete)?;
                lix_state_history_view_write::reject_delete(&delete)?;

                if let Some(rewritten) = entity_view_write::rewrite_delete(delete.clone())? {
                    current = Statement::Delete(rewritten);
                    continue;
                }

                let mut effective_scope_fallback = false;
                let delete = if let Some(rewritten) =
                    lix_state_by_version_write::rewrite_delete(delete.clone())?
                {
                    effective_scope_fallback = true;
                    rewritten
                } else {
                    delete
                };

                let output =
                    vtable_write::rewrite_delete(delete, effective_scope_fallback, context.params)?;
                return Ok(StatementRuleOutcome::Emit(output));
            }
            Statement::Query(query) => {
                let query = crate::engine::sql::planning::rewrite_engine::pipeline::query_engine::rewrite_read_query(*query)?;
                return Ok(StatementRuleOutcome::Emit(RewriteOutput {
                    statements: vec![Statement::Query(Box::new(query))],
                    effect_only: false,
                    params: Vec::new(),
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                }));
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
                        crate::engine::sql::planning::rewrite_engine::pipeline::query_engine::rewrite_read_query(*query)?,
                    )),
                    other => other,
                };
                return Ok(StatementRuleOutcome::Emit(RewriteOutput {
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
                    effect_only: false,
                    params: Vec::new(),
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                }));
            }
            other => {
                return Ok(StatementRuleOutcome::Emit(RewriteOutput {
                    statements: vec![other],
                    effect_only: false,
                    params: Vec::new(),
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                }));
            }
        }
    }

    Ok(StatementRuleOutcome::Continue(current))
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
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "backend rewrite requested without backend context".to_string(),
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
                lix_change_view_write::reject_insert(&insert)?;
                lix_state_history_view_write::reject_insert(&insert)?;

                if let Some(version_rewrite) = lix_version_write::rewrite_insert_with_backend(
                    backend,
                    insert.clone(),
                    context.params,
                )
                .await?
                {
                    let mut output = rewrite_vtable_inserts_with_backend(
                        backend,
                        version_rewrite.vtable_inserts,
                        context.params,
                        functions,
                        context.writer_key,
                    )
                    .await?;
                    output
                        .statements
                        .extend(version_rewrite.supplemental_statements);
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
                    lix_state_write::rewrite_insert_with_backend(backend, current_insert.clone())
                        .await?
                {
                    current_insert = rewritten;
                }
                if let Some(rewritten) =
                    lix_state_by_version_write::rewrite_insert(current_insert.clone())?
                {
                    current_insert = rewritten;
                }

                let mut supplemental_statements = Vec::new();
                if let Some(rewritten) =
                    stored_schema_write::rewrite_insert(current_insert.clone(), context.params)?
                {
                    context.registrations.push(rewritten.registration);
                    context.mutations.push(rewritten.mutation);
                    supplemental_statements.extend(rewritten.supplemental_statements);
                    let Statement::Insert(insert_statement) = rewritten.statement else {
                        return Err(LixError {
                            code: "LIX_ERROR_UNKNOWN".to_string(),
                            description: "stored schema rewrite expected insert statement"
                                .to_string(),
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
                    context.writer_key,
                    functions,
                )
                .await
                .map_err(|error| LixError {
                    code: error.code,
                    description: format!(
                        "backend insert vtable lowering failed: {}",
                        error.description
                    ),
                })? {
                    context.registrations.extend(rewritten.registrations);
                    context.generated_params.extend(rewritten.params);
                    context.mutations.extend(rewritten.mutations);
                    statements = rewritten.statements;
                }

                if statements.is_empty() {
                    let target = insert_target_name(&current_insert);
                    if is_allowed_internal_write_target(&target) {
                        statements.push(Statement::Insert(current_insert));
                    } else {
                        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                                "strict rewrite violation: statement routing: unsupported INSERT target '{}'",
                                target
                            ),
                        });
                    }
                }
                statements.extend(supplemental_statements);

                return Ok(StatementRuleOutcome::Emit(context.take_output(statements)));
            }
            Statement::Update(update) => {
                lix_change_view_write::reject_update(&update)?;
                lix_state_history_view_write::reject_update(&update)?;

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
                        context.writer_key,
                    )
                    .await?;
                    return Ok(StatementRuleOutcome::Emit(output));
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

                if let Some(rewritten) = lix_state_by_version_write::rewrite_update(update.clone())?
                {
                    current = Statement::Update(rewritten);
                    continue;
                }

                if let Some(version_rewrite) = lix_version_write::rewrite_update_with_backend(
                    backend,
                    update.clone(),
                    context.params,
                )
                .await?
                {
                    let mut output = rewrite_vtable_inserts_with_backend(
                        backend,
                        version_rewrite.vtable_inserts,
                        context.params,
                        functions,
                        context.writer_key,
                    )
                    .await?;
                    output
                        .statements
                        .extend(version_rewrite.supplemental_statements);
                    return Ok(StatementRuleOutcome::Emit(output));
                }

                let output = vtable_write::rewrite_update(update, context.params)?;
                return Ok(StatementRuleOutcome::Emit(output));
            }
            Statement::Delete(delete) => {
                lix_change_view_write::reject_delete(&delete)?;
                lix_state_history_view_write::reject_delete(&delete)?;

                let mut effective_scope_fallback = false;
                let delete = if let Some(rewritten) =
                    entity_view_write::rewrite_delete_with_backend(backend, delete.clone()).await?
                {
                    rewritten
                } else {
                    delete
                };

                let delete = if let Some(rewritten) =
                    lix_state_write::rewrite_delete_with_backend(backend, delete.clone()).await?
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

                if let Some(version_rewrite) = lix_version_write::rewrite_delete_with_backend(
                    backend,
                    delete.clone(),
                    context.params,
                )
                .await?
                {
                    let mut output = rewrite_vtable_inserts_with_backend(
                        backend,
                        version_rewrite.vtable_inserts,
                        context.params,
                        functions,
                        context.writer_key,
                    )
                    .await?;
                    output
                        .statements
                        .extend(version_rewrite.supplemental_statements);
                    return Ok(StatementRuleOutcome::Emit(output));
                }

                let output =
                    vtable_write::rewrite_delete(delete, effective_scope_fallback, context.params)?;
                return Ok(StatementRuleOutcome::Emit(output));
            }
            Statement::Query(query) => {
                let query =
                    rewrite_read_query_with_backend_and_params(backend, *query, context.params)
                        .await?;
                return Ok(StatementRuleOutcome::Emit(RewriteOutput {
                    statements: vec![Statement::Query(Box::new(query))],
                    effect_only: false,
                    params: Vec::new(),
                    registrations: Vec::new(),
                    postprocess: context.postprocess.take(),
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                }));
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
                        rewrite_read_query_with_backend_and_params(backend, *query, context.params)
                            .await?,
                    )),
                    other => other,
                };
                return Ok(StatementRuleOutcome::Emit(RewriteOutput {
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
                    effect_only: false,
                    params: Vec::new(),
                    registrations: Vec::new(),
                    postprocess: context.postprocess.take(),
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                }));
            }
            other => {
                return Ok(StatementRuleOutcome::Emit(RewriteOutput {
                    statements: vec![other],
                    effect_only: false,
                    params: Vec::new(),
                    registrations: Vec::new(),
                    postprocess: context.postprocess.take(),
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                }));
            }
        }
    }

    Ok(StatementRuleOutcome::Continue(current))
}

fn insert_target_name(insert: &sqlparser::ast::Insert) -> String {
    match &insert.table {
        sqlparser::ast::TableObject::TableName(name) => name.to_string(),
        _ => "<non-table-target>".to_string(),
    }
}

fn is_allowed_internal_write_target(target: &str) -> bool {
    let normalized = target.trim_matches('"').to_ascii_lowercase();
    normalized.starts_with("lix_internal_")
}

#[cfg(test)]
mod tests {}
