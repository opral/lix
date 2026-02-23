use std::collections::BTreeSet;

use crate::deterministic_mode::DeterministicSettings;
use crate::deterministic_mode::RuntimeFunctionProvider;
use crate::functions::SharedFunctionProvider;
use crate::schema_registry::register_schema_sql_statements;
use crate::sql::{build_delete_followup_sql, build_update_followup_sql, DetectedFileDomainChange};
use crate::{Engine, LixError, QueryResult};

use super::super::contracts::execution_plan::ExecutionPlan;
use super::super::contracts::executor_error::ExecutorError;
use super::super::contracts::planned_statement::MutationOperation;
use super::super::contracts::postprocess_actions::PostprocessPlan;
use super::super::type_bridge::{
    from_sql_prepared_statements, to_sql_vtable_delete_plan, to_sql_vtable_update_plan,
};
use super::execute_prepared::{execute_prepared_with_backend, execute_prepared_with_transaction};

pub(crate) struct SqlExecutionOutcome {
    pub(crate) result: QueryResult,
    pub(crate) postprocess_file_cache_targets: BTreeSet<(String, String)>,
    pub(crate) plugin_changes_committed: bool,
}

pub(crate) async fn execute_plan_sql(
    engine: &Engine,
    plan: &ExecutionPlan,
    detected_file_domain_changes: &[DetectedFileDomainChange],
    should_refresh_file_cache: bool,
    functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    writer_key: Option<&str>,
) -> Result<SqlExecutionOutcome, ExecutorError> {
    for registration in &plan.preprocess.registrations {
        crate::schema_registry::register_schema(engine.backend.as_ref(), &registration.schema_key)
            .await
            .map_err(ExecutorError::execute)?;
    }

    let mut postprocess_file_cache_targets = BTreeSet::new();
    let mut plugin_changes_committed = false;
    let result = match &plan.preprocess.postprocess {
        None => {
            let result = execute_prepared_with_backend(
                engine.backend.as_ref(),
                &plan.preprocess.prepared_statements,
            )
            .await
            .map_err(ExecutorError::execute)?;
            let tracked_insert_mutation_present =
                plan.preprocess.mutations.iter().any(|mutation| {
                    mutation.operation == MutationOperation::Insert && !mutation.untracked
                });
            if tracked_insert_mutation_present && !detected_file_domain_changes.is_empty() {
                plugin_changes_committed = true;
            }
            result
        }
        Some(postprocess_plan) => {
            let mut transaction = engine
                .backend
                .begin_transaction()
                .await
                .map_err(ExecutorError::execute)?;
            let result = match execute_prepared_with_transaction(
                transaction.as_mut(),
                &plan.preprocess.prepared_statements,
            )
            .await
            {
                Ok(result) => result,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(ExecutorError::execute(error));
                }
            };

            match postprocess_plan {
                PostprocessPlan::VtableUpdate(update_plan) if should_refresh_file_cache => {
                    match super::super::super::collect_postprocess_file_cache_targets(
                        &result.rows,
                        &update_plan.schema_key,
                    ) {
                        Ok(targets) => {
                            postprocess_file_cache_targets.extend(targets);
                        }
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            return Err(ExecutorError::execute(error));
                        }
                    }
                }
                PostprocessPlan::VtableDelete(delete_plan) if should_refresh_file_cache => {
                    match super::super::super::collect_postprocess_file_cache_targets(
                        &result.rows,
                        &delete_plan.schema_key,
                    ) {
                        Ok(targets) => {
                            postprocess_file_cache_targets.extend(targets);
                        }
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            return Err(ExecutorError::execute(error));
                        }
                    }
                }
                _ => {}
            }

            let additional_schema_keys = detected_file_domain_changes
                .iter()
                .map(|change| change.schema_key.clone())
                .collect::<BTreeSet<_>>();
            for schema_key in additional_schema_keys {
                for statement in register_schema_sql_statements(&schema_key, transaction.dialect())
                {
                    if let Err(error) = transaction.execute(&statement, &[]).await {
                        let _ = transaction.rollback().await;
                        return Err(ExecutorError::execute(error));
                    }
                }
            }

            let mut followup_functions = functions.clone();
            let followup_statements = match postprocess_plan {
                PostprocessPlan::VtableUpdate(update_plan) => {
                    let update_plan = to_sql_vtable_update_plan(update_plan);
                    match build_update_followup_sql(
                        transaction.as_mut(),
                        &update_plan,
                        &result.rows,
                        detected_file_domain_changes,
                        writer_key,
                        &mut followup_functions,
                    )
                    .await
                    {
                        Ok(statements) => statements,
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            return Err(ExecutorError::execute(error));
                        }
                    }
                }
                PostprocessPlan::VtableDelete(delete_plan) => {
                    let delete_plan = to_sql_vtable_delete_plan(delete_plan);
                    match build_delete_followup_sql(
                        transaction.as_mut(),
                        &delete_plan,
                        &result.rows,
                        &plan.preprocess.params,
                        detected_file_domain_changes,
                        writer_key,
                        &mut followup_functions,
                    )
                    .await
                    {
                        Ok(statements) => statements,
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            return Err(ExecutorError::execute(error));
                        }
                    }
                }
            };
            let followup_statements = from_sql_prepared_statements(followup_statements);

            if let Err(error) =
                execute_prepared_with_transaction(transaction.as_mut(), &followup_statements).await
            {
                let _ = transaction.rollback().await;
                return Err(ExecutorError::execute(error));
            }
            transaction.commit().await.map_err(ExecutorError::execute)?;
            plugin_changes_committed = true;
            result
        }
    };

    Ok(SqlExecutionOutcome {
        result,
        postprocess_file_cache_targets,
        plugin_changes_committed,
    })
}

pub(crate) async fn persist_runtime_sequence(
    engine: &Engine,
    settings: DeterministicSettings,
    sequence_start: i64,
    functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<(), LixError> {
    engine
        .persist_runtime_sequence_with_backend(
            engine.backend.as_ref(),
            settings,
            sequence_start,
            functions,
        )
        .await
}
