use crate::engine::{
    normalize_sql_execution_error_with_backend, Engine, TransactionBackendAdapter,
};
use crate::sql::execution::execution_program::{
    BoundStatementTemplateInstance, ExecutionContext, StatementTemplate, StatementTemplateCacheKey,
};
use crate::sql::execution::parse::parse_sql;
use crate::sql::execution::shared_path::{
    self, prepared_execution_mutates_public_surface_registry,
};
use crate::transaction::PendingTransactionView;
use crate::{LixBackendTransaction, LixError, Value};
use sqlparser::ast::Statement;

use super::CompiledExecutionStep;

pub(super) struct SqlBufferedWriteCommand {
    pub(super) statement: Statement,
    pub(super) compiled: CompiledExecutionStep,
    pub(super) registry_mutated_during_planning: bool,
}

pub(super) async fn compile_sql_buffered_write_command(
    engine: &Engine,
    transaction: &mut dyn LixBackendTransaction,
    pending_transaction_view: Option<&PendingTransactionView>,
    bound_statement_template: &BoundStatementTemplateInstance,
    allow_internal_tables: bool,
    context: &ExecutionContext,
    skip_side_effect_collection: bool,
) -> Result<SqlBufferedWriteCommand, LixError> {
    let writer_key = context.options.writer_key.clone();
    let parsed_statements = std::slice::from_ref(bound_statement_template.statement());
    let backend = TransactionBackendAdapter::new(transaction);
    let compiled = match shared_path::compile_execution_step_from_template_instance_with_backend(
        engine,
        &backend,
        pending_transaction_view,
        bound_statement_template,
        context.active_version_id.as_str(),
        writer_key.as_deref(),
        allow_internal_tables,
        Some(&context.public_surface_registry),
        shared_path::PreparationPolicy {
            skip_side_effect_collection,
        },
    )
    .await
    {
        Ok(compiled) => compiled,
        Err(error) => {
            return Err(normalize_sql_execution_error_with_backend(
                &backend,
                error,
                parsed_statements,
            )
            .await);
        }
    };
    let registry_mutated_during_planning =
        prepared_execution_mutates_public_surface_registry(compiled.execution())?;

    Ok(SqlBufferedWriteCommand {
        statement: bound_statement_template.statement().clone(),
        compiled,
        registry_mutated_during_planning,
    })
}

pub(super) fn bind_single_statement_template(
    transaction: &mut dyn LixBackendTransaction,
    sql: &str,
    params: &[Value],
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
) -> Result<BoundStatementTemplateInstance, LixError> {
    let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
    if parsed_statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description:
                "execute_with_options_in_write_transaction expects exactly one SQL statement"
                    .to_string(),
        });
    }

    let dialect = transaction.dialect();
    let cache_key = StatementTemplateCacheKey::new(
        sql,
        dialect,
        allow_internal_tables,
        context.public_surface_registry_generation,
    );
    let template = match context.statement_template_cache.get(&cache_key) {
        Some(template) => template.clone(),
        None => {
            let template = StatementTemplate::compile_with_registry(
                parsed_statements[0].clone(),
                &context.public_surface_registry,
                dialect,
                params.len(),
            )?;
            context
                .statement_template_cache
                .insert(cache_key, template.clone());
            template
        }
    };
    template.bind(params)
}
