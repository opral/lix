use std::collections::BTreeSet;

use crate::canonical::pending_session::PendingPublicCommitSession;
use crate::canonical::CanonicalCommitReceipt;
use crate::deterministic_mode::RuntimeFunctionProvider;
use crate::engine::{
    normalize_sql_execution_error_with_backend, Engine, TransactionBackendAdapter,
};
use crate::functions::SharedFunctionProvider;
use crate::live_state::SchemaRegistrationSet;
use crate::sql::executor::contracts::effects::PlanEffects;
use crate::sql::executor::contracts::executor_error::ExecutorError;
use crate::sql::executor::execute_prepared::execute_prepared_with_transaction;
use crate::sql::executor::{
    schema_registrations_for_compiled_execution, CompiledExecution, CompiledInternalExecution,
    PreparedPublicRead,
};
use crate::sql::logical_plan::ResultContract;
use crate::sql::services::pending_reads::execute_prepared_public_read_with_pending_transaction_view;
use crate::state::stream::StateCommitStreamChange;
use crate::transaction::PendingTransactionView;
use crate::{LixBackendTransaction, LixError, QueryResult};
use sqlparser::ast::Statement;

use super::planned_write::{build_planned_write_delta, PlannedWriteDelta};
use super::planned_write_runner::execute_planned_write_delta;
use crate::transaction::coordinator::apply_schema_registrations_in_transaction;
pub(crate) struct CompiledExecutionStep {
    execution: CompiledExecution,
    planned_write_delta: Option<PlannedWriteDelta>,
    schema_registrations: SchemaRegistrationSet,
}

pub(crate) enum CompiledExecutionRoute<'a> {
    PublicRead(&'a PreparedPublicRead),
    PlannedWriteDelta(&'a PlannedWriteDelta),
    PublicWriteNoop,
    Internal(&'a CompiledInternalExecution),
}

pub(crate) enum CompiledExecutionStepResult {
    Immediate(QueryResult),
    Outcome(SqlExecutionOutcome),
}

impl CompiledExecutionStep {
    pub(crate) fn compile(
        execution: CompiledExecution,
        writer_key: Option<&str>,
    ) -> Result<Self, LixError> {
        let schema_registrations = schema_registrations_for_compiled_execution(&execution);
        let planned_write_delta = build_planned_write_delta(&execution, writer_key)?;
        Ok(Self {
            execution,
            planned_write_delta,
            schema_registrations,
        })
    }

    pub(crate) fn execution(&self) -> &CompiledExecution {
        &self.execution
    }

    pub(crate) fn planned_write_delta(&self) -> Option<&PlannedWriteDelta> {
        self.planned_write_delta.as_ref()
    }

    pub(crate) fn schema_registrations(&self) -> &SchemaRegistrationSet {
        &self.schema_registrations
    }

    pub(crate) fn has_materialization_plan(&self) -> bool {
        self.planned_write_delta
            .as_ref()
            .is_some_and(|delta| !delta.materialization_plan().units.is_empty())
    }

    pub(crate) fn is_bufferable_write(&self, statement: &Statement) -> bool {
        self.planned_write_delta.is_some()
            && !matches!(self.execution.result_contract, ResultContract::DmlReturning)
            && !matches!(statement, Statement::Query(_) | Statement::Explain { .. })
    }

    pub(crate) fn route(&self) -> CompiledExecutionRoute<'_> {
        if let Some(public_read) = self.execution.public_read() {
            return CompiledExecutionRoute::PublicRead(public_read);
        }
        if let Some(delta) = self.planned_write_delta.as_ref() {
            return CompiledExecutionRoute::PlannedWriteDelta(delta);
        }
        if self.execution.public_write().is_some() {
            return CompiledExecutionRoute::PublicWriteNoop;
        }
        CompiledExecutionRoute::Internal(
            self.execution
                .internal_execution()
                .expect("compiled non-public execution must include internal ops"),
        )
    }
}

pub(crate) async fn execute_compiled_execution_step_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixBackendTransaction,
    step: &CompiledExecutionStep,
    parsed_statements: &[Statement],
    pending_transaction_view: Option<&PendingTransactionView>,
    pending_public_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
    writer_key: Option<&str>,
) -> Result<CompiledExecutionStepResult, LixError> {
    match step.route() {
        CompiledExecutionRoute::PublicRead(public_read) => {
            let backend = TransactionBackendAdapter::new(transaction);
            let public_result = match execute_prepared_public_read_with_pending_transaction_view(
                &backend,
                pending_transaction_view,
                public_read,
            )
            .await
            {
                Ok(result) => result,
                Err(error) => {
                    let normalized = normalize_sql_execution_error_with_backend(
                        &backend,
                        error,
                        parsed_statements,
                    )
                    .await;
                    return Err(normalized);
                }
            };
            Ok(CompiledExecutionStepResult::Immediate(public_result))
        }
        CompiledExecutionRoute::PlannedWriteDelta(delta) => {
            let execution = execute_planned_write_delta(
                engine,
                transaction,
                delta,
                pending_public_commit_session,
            )
            .await?;
            Ok(CompiledExecutionStepResult::Outcome(execution))
        }
        CompiledExecutionRoute::PublicWriteNoop => Ok(CompiledExecutionStepResult::Outcome(
            empty_public_write_execution_outcome(),
        )),
        CompiledExecutionRoute::Internal(internal) => {
            apply_schema_registrations_in_transaction(transaction, step.schema_registrations())
                .await?;
            match execute_internal_execution_with_transaction(
                transaction,
                internal,
                step.execution().result_contract,
                step.execution().runtime_state.provider(),
                writer_key,
            )
            .await
            .map_err(LixError::from)
            {
                Ok(execution) => Ok(CompiledExecutionStepResult::Outcome(execution)),
                Err(error) => {
                    let backend = TransactionBackendAdapter::new(transaction);
                    let normalized = normalize_sql_execution_error_with_backend(
                        &backend,
                        error,
                        parsed_statements,
                    )
                    .await;
                    Err(LixError {
                        code: normalized.code,
                        description: format!(
                            "transaction internal execution failed: {}",
                            normalized.description
                        ),
                    })
                }
            }
        }
    }
}

pub(crate) struct SqlExecutionOutcome {
    pub(crate) public_result: QueryResult,
    pub(crate) internal_write_file_cache_targets: BTreeSet<(String, String)>,
    pub(crate) plugin_changes_committed: bool,
    pub(crate) canonical_commit_receipt: Option<CanonicalCommitReceipt>,
    pub(crate) plan_effects_override: Option<PlanEffects>,
    pub(crate) state_commit_stream_changes: Vec<StateCommitStreamChange>,
    pub(crate) observe_tick_emitted: bool,
}

pub(crate) fn empty_public_write_execution_outcome() -> SqlExecutionOutcome {
    SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        internal_write_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        canonical_commit_receipt: None,
        plan_effects_override: Some(PlanEffects::default()),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: false,
    }
}

pub(crate) async fn execute_internal_execution_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    internal: &CompiledInternalExecution,
    result_contract: ResultContract,
    functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    writer_key: Option<&str>,
) -> Result<SqlExecutionOutcome, ExecutorError> {
    let _ = (functions, writer_key, internal.should_refresh_file_cache);
    let internal_result =
        execute_prepared_with_transaction(transaction, &internal.prepared_statements)
            .await
            .map_err(ExecutorError::execute)?;
    let public_result = public_result_from_contract(result_contract, &internal_result);

    Ok(SqlExecutionOutcome {
        public_result,
        internal_write_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        canonical_commit_receipt: None,
        plan_effects_override: None,
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: false,
    })
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
