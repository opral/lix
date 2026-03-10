use std::collections::VecDeque;

use sqlparser::ast::{
    Delete, FromTable, Insert, Statement, TableFactor, TableObject, TableWithJoins, Update,
};

use crate::engine::sql::planning::rewrite_engine::object_name_matches;
use crate::engine::sql::planning::rewrite_engine::{PostprocessPlan, RewriteOutput};
use crate::engine::sql::planning::rewrite_engine::{stored_schema, vtable_write};
use crate::functions::LixFunctionProvider;
use crate::errors;
use crate::{LixBackend, LixError, Value};

const MAX_REWRITE_PASSES: usize = 32;
const LIX_CHANGE_VIEW_NAME: &str = "lix_change";
const LIX_STATE_HISTORY_VIEW_NAME: &str = "lix_state_history";
const LIX_STATE_HISTORY_BY_VERSION_VIEW_NAME: &str = "lix_state_history_by_version";

struct StatementContext<'a> {
    params: &'a [Value],
    writer_key: Option<&'a str>,
    backend: Option<&'a dyn LixBackend>,
    side_effects: Vec<Statement>,
    registrations: Vec<crate::engine::sql::planning::rewrite_engine::SchemaRegistration>,
    generated_params: Vec<Value>,
    mutations: Vec<crate::engine::sql::planning::rewrite_engine::MutationRow>,
    update_validations: Vec<crate::engine::sql::planning::rewrite_engine::UpdateValidationPlan>,
    postprocess: Option<crate::engine::sql::planning::rewrite_engine::PostprocessPlan>,
}

impl<'a> StatementContext<'a> {
    fn new_sync(params: &'a [Value], writer_key: Option<&'a str>) -> Self {
        Self {
            params,
            writer_key,
            backend: None,
            side_effects: Vec::new(),
            registrations: Vec::new(),
            generated_params: Vec::new(),
            mutations: Vec::new(),
            update_validations: Vec::new(),
            postprocess: None,
        }
    }

    fn new_backend(
        backend: &'a dyn LixBackend,
        params: &'a [Value],
        writer_key: Option<&'a str>,
    ) -> Self {
        Self {
            params,
            writer_key,
            backend: Some(backend),
            side_effects: Vec::new(),
            registrations: Vec::new(),
            generated_params: Vec::new(),
            mutations: Vec::new(),
            update_validations: Vec::new(),
            postprocess: None,
        }
    }

    fn take_output(&mut self, statements: Vec<Statement>) -> RewriteOutput {
        RewriteOutput {
            statements,
            effect_only: false,
            params: std::mem::take(&mut self.generated_params),
            registrations: std::mem::take(&mut self.registrations),
            postprocess: self.postprocess.take(),
            mutations: std::mem::take(&mut self.mutations),
            update_validations: std::mem::take(&mut self.update_validations),
        }
    }
}

enum StatementRuleOutcome {
    Continue(Statement),
    Emit(RewriteOutput),
    NoMatch,
}

fn merge_rewrite_output(base: &mut RewriteOutput, mut next: RewriteOutput) -> Result<(), LixError> {
    if base.postprocess.is_some() && next.postprocess.is_some() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "only one postprocess rewrite is supported per query".to_string(),
        });
    }
    if base.postprocess.is_none() {
        base.postprocess = next.postprocess.take();
    }
    base.statements.extend(next.statements);
    base.effect_only = base.effect_only || next.effect_only;
    base.params.extend(next.params);
    base.registrations.extend(next.registrations);
    base.mutations.extend(next.mutations);
    base.update_validations.extend(next.update_validations);
    Ok(())
}

fn reject_read_only_public_write(statement: &Statement) -> Result<(), LixError> {
    match statement {
        Statement::Insert(insert) => {
            if table_object_is_read_only_public_surface(&insert.table) {
                return Err(read_only_public_write_error(read_only_insert_target_name(insert), "INSERT"));
            }
        }
        Statement::Update(update) => {
            if table_with_joins_is_read_only_public_surface(&update.table) {
                return Err(read_only_public_write_error(read_only_update_target_name(update), "UPDATE"));
            }
        }
        Statement::Delete(delete) => {
            if let Some(surface_name) = read_only_delete_target_name(delete) {
                return Err(read_only_public_write_error(surface_name, "DELETE"));
            }
        }
        _ => {}
    }
    Ok(())
}

fn read_only_public_write_error(surface_name: &str, operation: &str) -> LixError {
    errors::read_only_view_write_error(surface_name, operation)
}

fn read_only_insert_target_name(insert: &Insert) -> &'static str {
    match &insert.table {
        TableObject::TableName(name) if object_name_matches(name, LIX_CHANGE_VIEW_NAME) => {
            LIX_CHANGE_VIEW_NAME
        }
        TableObject::TableName(name)
            if object_name_matches(name, LIX_STATE_HISTORY_VIEW_NAME)
                || object_name_matches(name, LIX_STATE_HISTORY_BY_VERSION_VIEW_NAME) =>
        {
            LIX_STATE_HISTORY_VIEW_NAME
        }
        _ => LIX_CHANGE_VIEW_NAME,
    }
}

fn read_only_update_target_name(update: &Update) -> &'static str {
    if table_with_joins_matches(&update.table, LIX_CHANGE_VIEW_NAME) {
        LIX_CHANGE_VIEW_NAME
    } else {
        LIX_STATE_HISTORY_VIEW_NAME
    }
}

fn read_only_delete_target_name(delete: &Delete) -> Option<&'static str> {
    match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
            for table in tables {
                if table_with_joins_matches(table, LIX_CHANGE_VIEW_NAME) {
                    return Some(LIX_CHANGE_VIEW_NAME);
                }
                if table_with_joins_matches(table, LIX_STATE_HISTORY_VIEW_NAME)
                    || table_with_joins_matches(table, LIX_STATE_HISTORY_BY_VERSION_VIEW_NAME)
                {
                    return Some(LIX_STATE_HISTORY_VIEW_NAME);
                }
            }
            None
        }
    }
}

fn table_object_is_read_only_public_surface(table: &TableObject) -> bool {
    match table {
        TableObject::TableName(name) => {
            object_name_matches(name, LIX_CHANGE_VIEW_NAME)
                || object_name_matches(name, LIX_STATE_HISTORY_VIEW_NAME)
                || object_name_matches(name, LIX_STATE_HISTORY_BY_VERSION_VIEW_NAME)
        }
        _ => false,
    }
}

fn table_with_joins_is_read_only_public_surface(table: &TableWithJoins) -> bool {
    table_with_joins_matches(table, LIX_CHANGE_VIEW_NAME)
        || table_with_joins_matches(table, LIX_STATE_HISTORY_VIEW_NAME)
        || table_with_joins_matches(table, LIX_STATE_HISTORY_BY_VERSION_VIEW_NAME)
}

fn table_with_joins_matches(table: &TableWithJoins, surface_name: &str) -> bool {
    table.joins.is_empty()
        && matches!(
            &table.relation,
            TableFactor::Table { name, .. } if object_name_matches(name, surface_name)
        )
}

fn rewrite_vtable_update_output(
    update: Update,
    rewritten: Option<vtable_write::UpdateRewrite>,
) -> Result<RewriteOutput, LixError> {
    match rewritten {
        Some(vtable_write::UpdateRewrite::Statement(rewrite)) => Ok(RewriteOutput {
            statements: vec![rewrite.statement],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: rewrite.validation.into_iter().collect(),
        }),
        Some(vtable_write::UpdateRewrite::Planned(rewrite)) => {
            let mut statements = rewrite.pre_statements;
            statements.push(rewrite.statement);
            Ok(RewriteOutput {
                statements,
                effect_only: false,
                params: Vec::new(),
                registrations: Vec::new(),
                postprocess: Some(PostprocessPlan::VtableUpdate(rewrite.plan)),
                mutations: Vec::new(),
                update_validations: rewrite.validations,
            })
        }
        None => {
            let target = update_target_name(&update);
            if is_allowed_internal_write_target(&target) {
                Ok(RewriteOutput {
                    statements: vec![Statement::Update(update)],
                    effect_only: false,
                    params: Vec::new(),
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                })
            } else {
                Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "strict rewrite violation: statement routing: unsupported UPDATE target '{}'",
                        target
                    ),
                })
            }
        }
    }
}

fn rewrite_vtable_delete_output(
    delete: Delete,
    effective_scope_fallback: bool,
    params: &[Value],
) -> Result<RewriteOutput, LixError> {
    let rewritten = if effective_scope_fallback {
        vtable_write::rewrite_delete_with_options(delete.clone(), true, params)?
    } else {
        vtable_write::rewrite_delete(delete.clone(), params)?
    };

    match rewritten {
        Some(vtable_write::DeleteRewrite::Statement(statement)) => Ok(RewriteOutput {
            statements: vec![statement],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        }),
        Some(vtable_write::DeleteRewrite::Planned(rewrite)) => Ok(RewriteOutput {
            statements: vec![rewrite.statement],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableDelete(rewrite.plan)),
            mutations: Vec::new(),
            update_validations: Vec::new(),
        }),
        None => {
            let target = delete_target_name(&delete);
            if is_allowed_internal_write_target(&target) {
                Ok(RewriteOutput {
                    statements: vec![Statement::Delete(delete)],
                    effect_only: false,
                    params: Vec::new(),
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                })
            } else {
                Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "strict rewrite violation: statement routing: unsupported DELETE target '{}'",
                        target
                    ),
                })
            }
        }
    }
}

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
    let current = statement;

    if !matches!(
        current,
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
    ) {
        return Ok(StatementRuleOutcome::NoMatch);
    }

    for _ in 0..MAX_REWRITE_PASSES {
        match current {
            Statement::Insert(insert) => {
                reject_read_only_public_write(&Statement::Insert(insert.clone()))?;

                let mut current_insert = insert;
                let mut supplemental_statements = Vec::new();
                if let Some(rewritten) =
                    stored_schema::rewrite_insert(current_insert.clone(), context.params)?
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
                reject_read_only_public_write(&Statement::Update(update.clone()))?;

                let output = rewrite_vtable_update_output(
                    update.clone(),
                    vtable_write::rewrite_update(update, context.params)?,
                )?;
                return Ok(StatementRuleOutcome::Emit(output));
            }
            Statement::Delete(delete) => {
                reject_read_only_public_write(&Statement::Delete(delete.clone()))?;

                let output =
                    rewrite_vtable_delete_output(delete, false, context.params)?;
                return Ok(StatementRuleOutcome::Emit(output));
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

    let current = statement;

    if !matches!(
        current,
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
    ) {
        return Ok(StatementRuleOutcome::NoMatch);
    }

    for _ in 0..MAX_REWRITE_PASSES {
        match current {
            Statement::Insert(insert) => {
                reject_read_only_public_write(&Statement::Insert(insert.clone()))?;
                let mut current_insert = insert;
                let mut supplemental_statements = Vec::new();
                if let Some(rewritten) =
                    stored_schema::rewrite_insert(current_insert.clone(), context.params)?
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
                reject_read_only_public_write(&Statement::Update(update.clone()))?;

                let output = rewrite_vtable_update_output(
                    update.clone(),
                    vtable_write::rewrite_update(update, context.params)?,
                )?;
                return Ok(StatementRuleOutcome::Emit(output));
            }
            Statement::Delete(delete) => {
                reject_read_only_public_write(&Statement::Delete(delete.clone()))?;

                let output =
                    rewrite_vtable_delete_output(delete, false, context.params)?;
                return Ok(StatementRuleOutcome::Emit(output));
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

fn update_target_name(update: &Update) -> String {
    match &update.table.relation {
        sqlparser::ast::TableFactor::Table { name, .. } => name.to_string(),
        _ => "<non-table-target>".to_string(),
    }
}

fn delete_target_name(delete: &Delete) -> String {
    let tables = match &delete.from {
        sqlparser::ast::FromTable::WithFromKeyword(tables)
        | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
    };
    tables
        .first()
        .map(|table| match &table.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => name.to_string(),
            _ => "<non-table-target>".to_string(),
        })
        .unwrap_or_else(|| "<missing-target>".to_string())
}

fn is_allowed_internal_write_target(target: &str) -> bool {
    let normalized = target.trim_matches('"').to_ascii_lowercase();
    normalized.starts_with("lix_internal_")
}

#[cfg(test)]
mod tests {}
