use std::collections::BTreeSet;

use crate::deterministic_mode::RuntimeFunctionProvider;
use crate::engine::{
    normalize_sql_execution_error_with_backend, Engine, TransactionBackendAdapter,
};
use crate::functions::SharedFunctionProvider;
use crate::live_state::SchemaRegistration;
use crate::schema::registry::coalesce_live_table_requirements;
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::contracts::executor_error::ExecutorError;
use crate::sql::execution::contracts::planned_statement::{
    MutationRow, SchemaLiveTableRequirement, UpdateValidationPlan,
};
use crate::sql::execution::contracts::prepared_statement::PreparedStatement;
use crate::sql::execution::contracts::result_contract::ResultContract;
use crate::canonical::pending_session::PendingPublicCommitSession;
use crate::sql::execution::shared_path::{self, PendingTransactionView};
use crate::sql::public::runtime::{PreparedPublicRead, PreparedPublicWrite};
use crate::state::internal::followup::execute_internal_postprocess_with_transaction;
use crate::state::internal::PostprocessPlan;
use crate::state::stream::StateCommitStreamChange;
use crate::{LixBackendTransaction, LixError, QueryResult};
use sqlparser::ast::Statement;

use crate::transaction::contracts::SchemaRegistrationSet;
use crate::transaction::coordinator::apply_schema_registrations_in_transaction;
use super::planned_write::{build_planned_write_delta, PlannedWriteDelta};
use super::planned_write_runner::execute_planned_write_delta;

pub(crate) struct CompiledExecution {
    pub(crate) intent: crate::sql::execution::intent::ExecutionIntent,
    pub(crate) settings: crate::deterministic_mode::DeterministicSettings,
    pub(crate) sequence_start: i64,
    pub(crate) functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    pub(crate) result_contract: ResultContract,
    pub(crate) effects: PlanEffects,
    pub(crate) read_only_query: bool,
    pub(crate) body: CompiledExecutionBody,
}

pub(crate) enum CompiledExecutionBody {
    PublicRead(PreparedPublicRead),
    PublicWrite(PreparedPublicWrite),
    Internal(CompiledInternalExecution),
}

impl CompiledExecution {
    pub(crate) fn public_read(&self) -> Option<&PreparedPublicRead> {
        match &self.body {
            CompiledExecutionBody::PublicRead(read) => Some(read),
            CompiledExecutionBody::PublicWrite(_) | CompiledExecutionBody::Internal(_) => None,
        }
    }

    pub(crate) fn public_write(&self) -> Option<&PreparedPublicWrite> {
        match &self.body {
            CompiledExecutionBody::PublicWrite(write) => Some(write),
            CompiledExecutionBody::PublicRead(_) | CompiledExecutionBody::Internal(_) => None,
        }
    }

    pub(crate) fn internal_execution(&self) -> Option<&CompiledInternalExecution> {
        match &self.body {
            CompiledExecutionBody::Internal(internal) => Some(internal),
            CompiledExecutionBody::PublicRead(_) | CompiledExecutionBody::PublicWrite(_) => None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct CompiledInternalExecution {
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
    pub(crate) should_refresh_file_cache: bool,
}

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
        let schema_registrations = build_compiled_execution_schema_registrations(&execution);
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
            let public_result =
                match shared_path::execute_prepared_public_read_with_pending_transaction_view(
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
            shared_path::empty_public_write_execution_outcome(),
        )),
        CompiledExecutionRoute::Internal(internal) => {
            apply_schema_registrations_in_transaction(transaction, step.schema_registrations())
                .await?;
            match execute_internal_execution_with_transaction(
                transaction,
                internal,
                step.execution().result_contract,
                &step.execution().functions,
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
    pub(crate) postprocess_file_cache_targets: BTreeSet<(String, String)>,
    pub(crate) plugin_changes_committed: bool,
    pub(crate) plan_effects_override: Option<PlanEffects>,
    pub(crate) state_commit_stream_changes: Vec<StateCommitStreamChange>,
    pub(crate) observe_tick_emitted: bool,
}

pub(crate) async fn execute_internal_execution_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    internal: &CompiledInternalExecution,
    result_contract: ResultContract,
    functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    writer_key: Option<&str>,
) -> Result<SqlExecutionOutcome, ExecutorError> {
    let outcome = execute_internal_postprocess_with_transaction(
        transaction,
        &internal.prepared_statements,
        internal.postprocess.as_ref(),
        internal.should_refresh_file_cache,
        functions,
        writer_key,
    )
    .await
    .map_err(ExecutorError::execute)?;
    let postprocess_file_cache_targets = outcome.postprocess_file_cache_targets;
    let state_commit_stream_changes = outcome.state_commit_stream_changes;
    let plugin_changes_committed = internal.postprocess.is_some();
    let internal_result = outcome.internal_result;
    let public_result = public_result_from_contract(result_contract, &internal_result);

    Ok(SqlExecutionOutcome {
        public_result,
        postprocess_file_cache_targets,
        plugin_changes_committed,
        plan_effects_override: None,
        state_commit_stream_changes,
        observe_tick_emitted: false,
    })
}

fn public_result_from_contract(contract: ResultContract, internal_result: &QueryResult) -> QueryResult {
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

fn build_compiled_execution_schema_registrations(
    execution: &CompiledExecution,
) -> SchemaRegistrationSet {
    let mut registrations = SchemaRegistrationSet::default();
    if let Some(internal) = execution.internal_execution() {
        for requirement in coalesce_live_table_requirements(&internal.live_table_requirements) {
            match requirement.layout.as_ref() {
                Some(layout) => registrations.insert(SchemaRegistration::with_legacy_layout(
                    requirement.schema_key.clone(),
                    layout,
                )),
                None => registrations.insert(requirement.schema_key.clone()),
            }
        }
    }
    registrations
}
