use std::collections::VecDeque;

use sqlparser::ast::{BinaryOperator, Expr, GroupByExpr, Query, Select, SelectItem, Statement};

use crate::engine::sql::contracts::effects::DetectedFileDomainChange as Sql2DetectedFileDomainChange;
use crate::engine::sql::planning::rewrite_engine::pipeline::query_engine::rewrite_read_query_with_backend_and_params;
use crate::engine::sql::planning::rewrite_engine::steps::lix_change_view_write;
use crate::engine::sql::planning::rewrite_engine::steps::lix_state_history_view_write;
use crate::engine::sql::planning::rewrite_engine::types::{PostprocessPlan, RewriteOutput};
use crate::engine::sql::planning::rewrite_engine::DetectedFileDomainChange;
use crate::functions::LixFunctionProvider;
use crate::{LixBackend, LixError, Value};

pub(crate) mod entity_view_write;
pub(crate) mod filesystem_write;
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
    functions: &mut P,
    detected_file_domain_changes: &[DetectedFileDomainChange],
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

    if final_output.statements.is_empty() {
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

                if let Some(rewritten) = filesystem_write::rewrite_insert(insert.clone())? {
                    current = Statement::Insert(rewritten);
                    continue;
                }
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

                return Ok(StatementRuleOutcome::Emit(context.take_output(statements)));
            }
            Statement::Update(update) => {
                lix_change_view_write::reject_update(&update)?;
                lix_state_history_view_write::reject_update(&update)?;

                if let Some(rewritten) = filesystem_write::rewrite_update(update.clone())? {
                    current = rewritten;
                    continue;
                }
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

                if let Some(rewritten) = filesystem_write::rewrite_delete(delete.clone())? {
                    current = Statement::Delete(rewritten);
                    continue;
                }
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
                        .iter()
                        .map(sql_change_to_detected_file_domain_change),
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
                        &insert_detected_file_domain_changes,
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

                if let Some(rewritten) =
                    stored_schema_write::rewrite_insert(current_insert.clone(), context.params)?
                {
                    context.registrations.push(rewritten.registration);
                    context.mutations.push(rewritten.mutation);
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

                return Ok(StatementRuleOutcome::Emit(context.take_output(statements)));
            }
            Statement::Update(update) => {
                lix_change_view_write::reject_update(&update)?;
                lix_state_history_view_write::reject_update(&update)?;

                if let Some(rewritten) = filesystem_write::rewrite_update_with_backend(
                    backend,
                    update.clone(),
                    context.params,
                )
                .await?
                {
                    if filesystem_noop_statement(&rewritten)
                        && !context.detected_file_domain_changes.is_empty()
                    {
                        context.postprocess = Some(PostprocessPlan::DomainChangesOnly);
                    }
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
                        context.detected_file_domain_changes,
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
                        context.detected_file_domain_changes,
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

fn filesystem_noop_statement(statement: &Statement) -> bool {
    let Statement::Query(query) = statement else {
        return false;
    };
    filesystem_noop_query(query)
}

fn filesystem_noop_query(query: &Query) -> bool {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return false;
    }

    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    filesystem_noop_select(select)
}

fn filesystem_noop_select(select: &Select) -> bool {
    if select.projection.len() != 1 {
        return false;
    }
    if !select.from.is_empty() {
        return false;
    }
    if !select_group_by_is_empty(select) {
        return false;
    }
    if select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        return false;
    }

    let Some(selection) = select.selection.as_ref() else {
        return false;
    };

    filesystem_noop_projection_item(select.projection.first())
        && filesystem_noop_predicate(selection)
}

fn select_group_by_is_empty(select: &Select) -> bool {
    match &select.group_by {
        GroupByExpr::Expressions(expressions, modifiers) => {
            expressions.is_empty() && modifiers.is_empty()
        }
        GroupByExpr::All(_) => false,
    }
}

fn filesystem_noop_projection_item(item: Option<&SelectItem>) -> bool {
    let Some(SelectItem::UnnamedExpr(expr)) = item else {
        return false;
    };
    expr_is_integer_literal(expr, 0)
}

fn filesystem_noop_predicate(expr: &Expr) -> bool {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = strip_nested_expr(expr)
    else {
        return false;
    };

    (expr_is_integer_literal(left.as_ref(), 1) && expr_is_integer_literal(right.as_ref(), 0))
        || (expr_is_integer_literal(left.as_ref(), 0) && expr_is_integer_literal(right.as_ref(), 1))
}

fn strip_nested_expr(mut expr: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expr {
        expr = inner.as_ref();
    }
    expr
}

fn expr_is_integer_literal(expr: &Expr, expected: i64) -> bool {
    let Expr::Value(value) = strip_nested_expr(expr) else {
        return false;
    };
    let sqlparser::ast::Value::Number(number, _) = &value.value else {
        return false;
    };
    number.parse::<i64>().ok() == Some(expected)
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

fn sql_change_to_detected_file_domain_change(
    change: &Sql2DetectedFileDomainChange,
) -> DetectedFileDomainChange {
    DetectedFileDomainChange {
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        schema_version: change.schema_version.clone(),
        file_id: change.file_id.clone(),
        version_id: change.version_id.clone(),
        plugin_key: change.plugin_key.clone(),
        snapshot_content: change.snapshot_content.clone(),
        metadata: change.metadata.clone(),
        writer_key: change.writer_key.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::filesystem_noop_statement;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse_statement(sql: &str) -> Statement {
        let mut statements =
            Parser::parse_sql(&GenericDialect {}, sql).expect("test SQL should parse");
        assert_eq!(statements.len(), 1, "test SQL should produce one statement");
        statements.remove(0)
    }

    #[test]
    fn filesystem_noop_statement_matches_ast_shape() {
        let statement = parse_statement("SELECT 0 WHERE 1 = 0");
        assert!(filesystem_noop_statement(&statement));
    }

    #[test]
    fn filesystem_noop_statement_ignores_sql_rendering_variants() {
        let statement = parse_statement("select (0) where (1)=(0)");
        assert!(filesystem_noop_statement(&statement));
    }

    #[test]
    fn filesystem_noop_statement_rejects_non_noop_predicate() {
        let statement = parse_statement("SELECT 0 WHERE 1 = 1");
        assert!(!filesystem_noop_statement(&statement));
    }
}
