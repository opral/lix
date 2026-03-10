use std::collections::BTreeSet;

use crate::deterministic_mode::DeterministicSettings;
use crate::deterministic_mode::RuntimeFunctionProvider;
use crate::functions::SharedFunctionProvider;
use crate::schema_registry::register_schema_sql_statements;
use crate::state_commit_stream::StateCommitStreamChange;
use crate::{Engine, LixError, LixTransaction, QueryResult};

use super::super::contracts::effects::PlanEffects;
use super::super::contracts::execution_plan::ExecutionPlan;
use super::super::contracts::executor_error::ExecutorError;
use crate::internal_state::followup::execute_postprocess_with_transaction;
use super::super::contracts::result_contract::ResultContract;
use super::super::planning::lower_sql::lower_to_prepared_statements;
use super::execute_prepared::{execute_prepared_with_backend, execute_prepared_with_transaction};

pub(crate) struct SqlExecutionOutcome {
    pub(crate) public_result: QueryResult,
    pub(crate) postprocess_file_cache_targets: BTreeSet<(String, String)>,
    pub(crate) plugin_changes_committed: bool,
    pub(crate) plan_effects_override: Option<PlanEffects>,
    pub(crate) state_commit_stream_changes: Vec<StateCommitStreamChange>,
}

pub(crate) async fn execute_plan_sql(
    engine: &Engine,
    plan: &ExecutionPlan,
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
            result
        }
        Some(postprocess_plan) => {
            let mut transaction = engine
                .backend
                .begin_transaction()
                .await
                .map_err(ExecutorError::execute)?;
            let outcome = match execute_postprocess_with_transaction(
                transaction.as_mut(),
                &prepared_statements,
                postprocess_plan,
                should_refresh_file_cache,
                functions,
                writer_key,
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(ExecutorError::execute(error));
                }
            };
            transaction.commit().await.map_err(ExecutorError::execute)?;
            plugin_changes_committed = true;
            postprocess_file_cache_targets = outcome.postprocess_file_cache_targets;
            state_commit_stream_changes = outcome.state_commit_stream_changes;
            outcome.internal_result
        }
    };
    let public_result = public_result_from_contract(plan.result_contract, &internal_result);

    Ok(SqlExecutionOutcome {
        public_result,
        postprocess_file_cache_targets,
        plugin_changes_committed,
        plan_effects_override: None,
        state_commit_stream_changes,
    })
}

pub(crate) async fn execute_plan_sql_with_transaction(
    transaction: &mut dyn LixTransaction,
    plan: &ExecutionPlan,
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
            result
        }
        Some(postprocess_plan) => {
            let outcome = execute_postprocess_with_transaction(
                transaction,
                &prepared_statements,
                postprocess_plan,
                should_refresh_file_cache,
                functions,
                writer_key,
            )
            .await
                .map_err(ExecutorError::execute)?;
            postprocess_file_cache_targets = outcome.postprocess_file_cache_targets;
            state_commit_stream_changes = outcome.state_commit_stream_changes;
            plugin_changes_committed = true;
            outcome.internal_result
        }
    };
    let public_result = public_result_from_contract(plan.result_contract, &internal_result);

    Ok(SqlExecutionOutcome {
        public_result,
        postprocess_file_cache_targets,
        plugin_changes_committed,
        plan_effects_override: None,
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
