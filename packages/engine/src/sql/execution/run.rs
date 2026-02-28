use std::collections::BTreeSet;

use crate::deterministic_mode::DeterministicSettings;
use crate::deterministic_mode::RuntimeFunctionProvider;
use crate::functions::SharedFunctionProvider;
use crate::schema_registry::register_schema_sql_statements;
use crate::state_commit_stream::{
    state_commit_stream_changes_from_postprocess_rows, StateCommitStreamChange,
    StateCommitStreamOperation,
};
use crate::{Engine, LixError, LixTransaction, QueryResult};

use super::super::contracts::effects::DetectedFileDomainChange;
use super::super::contracts::execution_plan::ExecutionPlan;
use super::super::contracts::executor_error::ExecutorError;
use super::super::contracts::planned_statement::MutationOperation;
use super::super::contracts::postprocess_actions::PostprocessPlan;
use super::super::contracts::result_contract::ResultContract;
use super::super::planning::lower_sql::lower_to_prepared_statements;
use super::execute_prepared::{execute_prepared_with_backend, execute_prepared_with_transaction};
use super::followup::{build_delete_followup_statements, build_update_followup_statements};

pub(crate) struct SqlExecutionOutcome {
    pub(crate) public_result: QueryResult,
    pub(crate) postprocess_file_cache_targets: BTreeSet<(String, String)>,
    pub(crate) plugin_changes_committed: bool,
    pub(crate) state_commit_stream_changes: Vec<StateCommitStreamChange>,
}

pub(crate) async fn execute_plan_sql(
    engine: &Engine,
    plan: &ExecutionPlan,
    detected_file_domain_changes: &[DetectedFileDomainChange],
    should_refresh_file_cache: bool,
    functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    writer_key: Option<&str>,
) -> Result<SqlExecutionOutcome, ExecutorError> {
    let prepared_statements = lower_to_prepared_statements(plan);

    for registration in &plan.preprocess.registrations {
        crate::schema_registry::register_schema(engine.backend.as_ref(), &registration.schema_key)
            .await
            .map_err(ExecutorError::execute)?;
    }

    let mut postprocess_file_cache_targets = BTreeSet::new();
    let mut plugin_changes_committed = false;
    let mut state_commit_stream_changes = Vec::new();
    let internal_result = match &plan.preprocess.postprocess {
        None => {
            let result =
                execute_prepared_with_backend(engine.backend.as_ref(), &prepared_statements)
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
            let result =
                match execute_prepared_with_transaction(transaction.as_mut(), &prepared_statements)
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

            match postprocess_plan {
                PostprocessPlan::VtableUpdate(update_plan) => {
                    match state_commit_stream_changes_from_postprocess_rows(
                        &result.rows,
                        &update_plan.schema_key,
                        StateCommitStreamOperation::Update,
                        writer_key,
                    ) {
                        Ok(changes) => state_commit_stream_changes.extend(changes),
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            return Err(ExecutorError::execute(error));
                        }
                    }
                }
                PostprocessPlan::VtableDelete(delete_plan) => {
                    match state_commit_stream_changes_from_postprocess_rows(
                        &result.rows,
                        &delete_plan.schema_key,
                        StateCommitStreamOperation::Delete,
                        writer_key,
                    ) {
                        Ok(changes) => state_commit_stream_changes.extend(changes),
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            return Err(ExecutorError::execute(error));
                        }
                    }
                }
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
            let followup_params = prepared_statements
                .first()
                .map(|statement| statement.params.as_slice())
                .unwrap_or(&[]);
            let followup_statements = match postprocess_plan {
                PostprocessPlan::VtableUpdate(update_plan) => {
                    match build_update_followup_statements(
                        transaction.as_mut(),
                        update_plan,
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
                    match build_delete_followup_statements(
                        transaction.as_mut(),
                        delete_plan,
                        &result.rows,
                        followup_params,
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
    let public_result = public_result_from_contract(plan.result_contract, &internal_result);

    Ok(SqlExecutionOutcome {
        public_result,
        postprocess_file_cache_targets,
        plugin_changes_committed,
        state_commit_stream_changes,
    })
}

pub(crate) async fn execute_plan_sql_with_transaction(
    transaction: &mut dyn LixTransaction,
    plan: &ExecutionPlan,
    detected_file_domain_changes: &[DetectedFileDomainChange],
    should_refresh_file_cache: bool,
    functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    writer_key: Option<&str>,
) -> Result<SqlExecutionOutcome, ExecutorError> {
    let prepared_statements = lower_to_prepared_statements(plan);

    for registration in &plan.preprocess.registrations {
        for statement in
            register_schema_sql_statements(&registration.schema_key, transaction.dialect())
        {
            transaction
                .execute(&statement, &[])
                .await
                .map_err(ExecutorError::execute)?;
        }
    }

    let mut postprocess_file_cache_targets = BTreeSet::new();
    let mut plugin_changes_committed = false;
    let mut state_commit_stream_changes = Vec::new();

    let internal_result = match &plan.preprocess.postprocess {
        None => {
            let result = execute_prepared_with_transaction(transaction, &prepared_statements)
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
            let result = execute_prepared_with_transaction(transaction, &prepared_statements)
                .await
                .map_err(ExecutorError::execute)?;

            match postprocess_plan {
                PostprocessPlan::VtableUpdate(update_plan) if should_refresh_file_cache => {
                    let targets = super::super::super::collect_postprocess_file_cache_targets(
                        &result.rows,
                        &update_plan.schema_key,
                    )
                    .map_err(ExecutorError::execute)?;
                    postprocess_file_cache_targets.extend(targets);
                }
                PostprocessPlan::VtableDelete(delete_plan) if should_refresh_file_cache => {
                    let targets = super::super::super::collect_postprocess_file_cache_targets(
                        &result.rows,
                        &delete_plan.schema_key,
                    )
                    .map_err(ExecutorError::execute)?;
                    postprocess_file_cache_targets.extend(targets);
                }
                _ => {}
            }

            match postprocess_plan {
                PostprocessPlan::VtableUpdate(update_plan) => {
                    let changes = state_commit_stream_changes_from_postprocess_rows(
                        &result.rows,
                        &update_plan.schema_key,
                        StateCommitStreamOperation::Update,
                        writer_key,
                    )
                    .map_err(ExecutorError::execute)?;
                    state_commit_stream_changes.extend(changes);
                }
                PostprocessPlan::VtableDelete(delete_plan) => {
                    let changes = state_commit_stream_changes_from_postprocess_rows(
                        &result.rows,
                        &delete_plan.schema_key,
                        StateCommitStreamOperation::Delete,
                        writer_key,
                    )
                    .map_err(ExecutorError::execute)?;
                    state_commit_stream_changes.extend(changes);
                }
            }

            let additional_schema_keys = detected_file_domain_changes
                .iter()
                .map(|change| change.schema_key.clone())
                .collect::<BTreeSet<_>>();
            for schema_key in additional_schema_keys {
                for statement in register_schema_sql_statements(&schema_key, transaction.dialect())
                {
                    transaction
                        .execute(&statement, &[])
                        .await
                        .map_err(ExecutorError::execute)?;
                }
            }

            let mut followup_functions = functions.clone();
            let followup_params = prepared_statements
                .first()
                .map(|statement| statement.params.as_slice())
                .unwrap_or(&[]);
            let followup_statements = match postprocess_plan {
                PostprocessPlan::VtableUpdate(update_plan) => build_update_followup_statements(
                    transaction,
                    update_plan,
                    &result.rows,
                    detected_file_domain_changes,
                    writer_key,
                    &mut followup_functions,
                )
                .await
                .map_err(ExecutorError::execute)?,
                PostprocessPlan::VtableDelete(delete_plan) => build_delete_followup_statements(
                    transaction,
                    delete_plan,
                    &result.rows,
                    followup_params,
                    detected_file_domain_changes,
                    writer_key,
                    &mut followup_functions,
                )
                .await
                .map_err(ExecutorError::execute)?,
            };
            execute_prepared_with_transaction(transaction, &followup_statements)
                .await
                .map_err(ExecutorError::execute)?;
            plugin_changes_committed = true;
            result
        }
    };
    let public_result = public_result_from_contract(plan.result_contract, &internal_result);

    Ok(SqlExecutionOutcome {
        public_result,
        postprocess_file_cache_targets,
        plugin_changes_committed,
        state_commit_stream_changes,
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

fn public_result_from_contract(
    contract: ResultContract,
    internal_result: &QueryResult,
) -> QueryResult {
    match contract {
        ResultContract::DmlNoReturning => QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        ResultContract::Select | ResultContract::DmlReturning | ResultContract::Other => {
            internal_result.clone()
        }
    }
}
