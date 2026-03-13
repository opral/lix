use std::collections::BTreeSet;

use crate::deterministic_mode::DeterministicSettings;
use crate::deterministic_mode::RuntimeFunctionProvider;
use crate::functions::SharedFunctionProvider;
use crate::schema::registry::ensure_schema_live_table_in_transaction;
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::contracts::execution_plan::ExecutionPlan;
use crate::sql::execution::contracts::executor_error::ExecutorError;
use crate::sql::execution::contracts::result_contract::ResultContract;
use crate::sql::execution::lower_sql::lower_to_prepared_statements;
use crate::state::internal::followup::{
    execute_internal_state_plan_with_backend, execute_internal_state_plan_with_transaction,
};
use crate::state::stream::StateCommitStreamChange;
use crate::{Engine, LixError, LixTransaction, QueryResult};

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

    for registration in &plan.preprocess.live_table_requirements {
        crate::schema::registry::ensure_schema_live_table(
            engine.backend_ref(),
            &registration.schema_key,
        )
        .await
        .map_err(ExecutorError::execute)?;
    }

    let outcome = execute_internal_state_plan_with_backend(
        engine.backend_ref(),
        &prepared_statements,
        plan.preprocess.internal_state.as_ref(),
        should_refresh_file_cache,
        functions,
        writer_key,
    )
    .await
    .map_err(ExecutorError::execute)?;
    let plugin_changes_committed = plan.preprocess.internal_state.is_some();
    let postprocess_file_cache_targets = outcome.postprocess_file_cache_targets;
    let state_commit_stream_changes = outcome.state_commit_stream_changes;
    let internal_result = outcome.internal_result;
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

    for registration in &plan.preprocess.live_table_requirements {
        ensure_schema_live_table_in_transaction(transaction, &registration.schema_key)
            .await
            .map_err(ExecutorError::execute)?;
    }

    let outcome = execute_internal_state_plan_with_transaction(
        transaction,
        &prepared_statements,
        plan.preprocess.internal_state.as_ref(),
        should_refresh_file_cache,
        functions,
        writer_key,
    )
    .await
    .map_err(ExecutorError::execute)?;
    let postprocess_file_cache_targets = outcome.postprocess_file_cache_targets;
    let state_commit_stream_changes = outcome.state_commit_stream_changes;
    let plugin_changes_committed = plan.preprocess.internal_state.is_some();
    let internal_result = outcome.internal_result;
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
            engine.backend_ref(),
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
