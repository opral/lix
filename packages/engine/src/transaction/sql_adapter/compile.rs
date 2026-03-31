use crate::contracts::traits::PendingView;
use crate::engine::Engine;
use crate::runtime::{normalize_sql_execution_error_with_backend, TransactionBackendAdapter};
use crate::sql::executor::execution_program::{
    BoundStatementTemplateInstance, ExecutionContext, StatementTemplate, StatementTemplateCacheKey,
};
use crate::sql::executor::{
    compile_execution_from_template_instance_with_backend,
    prepared_execution_mutates_public_surface_registry, PreparationPolicy,
};
use crate::sql::parser::parse_sql_with_timing;
use crate::transaction::PendingTransactionView;
use crate::{LixBackendTransaction, LixError, Value};
use sqlparser::ast::Statement;

use super::runtime::CompiledExecutionStep;

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
    let runtime_state = context
        .execution_runtime_state()
        .expect("write execution should install an execution runtime state before compilation");
    if runtime_state.settings().enabled {
        runtime_state
            .ensure_sequence_initialized_in_transaction(engine.runtime().as_ref(), transaction)
            .await?;
    }
    let backend = TransactionBackendAdapter::new(transaction);
    let compiled_execution = match compile_execution_from_template_instance_with_backend(
        engine.runtime().as_ref(),
        &backend,
        pending_transaction_view.map(|view| view as &dyn PendingView),
        bound_statement_template,
        context.active_version_id.as_str(),
        &context.active_account_ids,
        writer_key.as_deref(),
        allow_internal_tables,
        Some(&context.public_surface_registry),
        Some(runtime_state),
        PreparationPolicy {
            skip_side_effect_collection,
        },
    )
    .await
    {
        Ok(compiled_execution) => compiled_execution,
        Err(error) => {
            return Err(normalize_sql_execution_error_with_backend(
                &backend,
                error,
                parsed_statements,
            )
            .await);
        }
    };
    let compiled = CompiledExecutionStep::compile(compiled_execution, writer_key.as_deref())?;
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
    let parsed = parse_sql_with_timing(sql).map_err(LixError::from)?;
    let parsed_statements = parsed.statements;
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
        context.public_surface_registry_generation(),
    );
    let template = match context.cached_statement_template(&cache_key) {
        Some(template) => template,
        None => {
            let template = StatementTemplate::compile_with_registry(
                parsed_statements[0].clone(),
                &context.public_surface_registry,
                dialect,
                params.len(),
            )?;
            context.cache_statement_template(cache_key, template.clone());
            template
        }
    };
    let runtime_bindings = context.runtime_binding_values()?;
    template.bind(params, &runtime_bindings, Some(parsed.parse_duration))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        boot, BootArgs, Engine, ExecuteOptions, NoopWasmRuntime, QueryResult, Session, SqlDialect,
    };
    use async_trait::async_trait;
    use std::sync::Arc;

    struct NoopBackend;

    struct NoopTransaction;

    #[async_trait(?Send)]
    impl crate::LixBackend for NoopBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            _mode: crate::TransactionMode,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "transactions are not needed in this unit test backend",
            ))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            self.begin_transaction(crate::TransactionMode::Write).await
        }
    }

    #[async_trait(?Send)]
    impl crate::LixBackendTransaction for NoopTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::TransactionMode {
            crate::TransactionMode::Write
        }

        async fn execute(
            &mut self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<QueryResult, LixError> {
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    fn test_engine() -> Arc<Engine> {
        Arc::new(boot(BootArgs::new(
            Box::new(NoopBackend),
            Arc::new(NoopWasmRuntime),
        )))
    }

    fn test_session(engine: &Arc<Engine>) -> Session {
        Session::new_for_test(Arc::clone(engine), "version-test".to_string(), Vec::new())
    }

    #[test]
    fn statement_template_cache_is_shared_across_repeated_calls_in_one_session() {
        let engine = test_engine();
        let session = test_session(&engine);
        let sql = "SELECT 1";
        let cache_key = StatementTemplateCacheKey::new(sql, SqlDialect::Sqlite, false, 0);
        let mut transaction = NoopTransaction;

        let mut first_context = session.new_execution_context(ExecuteOptions::default());
        assert!(
            first_context
                .cached_statement_template(&cache_key)
                .is_none(),
            "cache should start empty for a fresh session runtime"
        );

        bind_single_statement_template(&mut transaction, sql, &[], false, &mut first_context)
            .expect("first template bind should succeed");
        assert!(
            first_context
                .cached_statement_template(&cache_key)
                .is_some(),
            "first bind should populate the session-owned statement template cache"
        );

        let second_context = session.new_execution_context(ExecuteOptions::default());
        assert!(
            second_context
                .cached_statement_template(&cache_key)
                .is_some(),
            "a new execution context in the same session should reuse the cached template"
        );
    }

    #[test]
    fn registry_generation_bumps_are_session_local_and_create_new_cache_namespaces() {
        let engine = test_engine();
        let session_a = test_session(&engine);
        let session_b = test_session(&engine);
        let sql = "SELECT 1";
        let cache_key_v0 = StatementTemplateCacheKey::new(sql, SqlDialect::Sqlite, false, 0);
        let cache_key_v1 = StatementTemplateCacheKey::new(sql, SqlDialect::Sqlite, false, 1);
        let mut transaction = NoopTransaction;

        let mut initial_context = session_a.new_execution_context(ExecuteOptions::default());
        bind_single_statement_template(&mut transaction, sql, &[], false, &mut initial_context)
            .expect("initial template bind should succeed");
        assert!(
            initial_context
                .cached_statement_template(&cache_key_v0)
                .is_some(),
            "initial cache namespace should contain the first template"
        );
        assert_eq!(session_a.snapshot().public_surface_registry_generation, 0);
        assert_eq!(session_b.snapshot().public_surface_registry_generation, 0);

        let mut bumped_context = session_a.new_execution_context(ExecuteOptions::default());
        bumped_context.bump_public_surface_registry_generation();
        assert_eq!(session_a.snapshot().public_surface_registry_generation, 1);
        assert_eq!(
            session_b.snapshot().public_surface_registry_generation,
            0,
            "another session should not inherit the bumped registry generation"
        );

        let mut session_a_after_bump = session_a.new_execution_context(ExecuteOptions::default());
        assert!(
            session_a_after_bump
                .cached_statement_template(&cache_key_v1)
                .is_none(),
            "new registry generations should start with a fresh cache namespace"
        );
        bind_single_statement_template(
            &mut transaction,
            sql,
            &[],
            false,
            &mut session_a_after_bump,
        )
        .expect("template bind after generation bump should succeed");
        assert!(
            session_a_after_bump
                .cached_statement_template(&cache_key_v1)
                .is_some(),
            "binding after the bump should populate the new cache namespace"
        );

        let session_b_context = session_b.new_execution_context(ExecuteOptions::default());
        assert!(
            session_b_context
                .cached_statement_template(&cache_key_v0)
                .is_none(),
            "another session should not see session-local template cache entries"
        );
    }
}
