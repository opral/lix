use std::collections::BTreeSet;
use std::time::Instant;

use crate::contracts::PendingView;
use crate::contracts::{
    coalesce_live_table_requirements, CanonicalCommitReceipt, PendingPublicCommitSession,
    PlanEffects, PreparedBatch, PreparedInternalWriteArtifact, PreparedPublicReadArtifact,
    PreparedWriteDiagnosticContext, PreparedWriteStatementKind, PreparedWriteStep, ResultContract,
    SchemaRegistration, SchemaRegistrationSet, StateCommitStreamChange,
};
use crate::contracts::{render_analyzed_explain_result, render_plain_explain_result};
use crate::execution::write::buffered::apply_schema_registrations_in_transaction;
use crate::execution::write::transaction::normalize_sql_error_with_transaction_and_relation_names;
use crate::execution::write::{
    PendingTransactionView, PreparedWriteRuntimeState, WriteExecutionBindings,
};
use crate::{LixBackendTransaction, LixError, QueryResult};

use super::planned_write_runner::execute_planned_write_delta;
use super::registered_schema_bootstrap::mirror_registered_schema_mutations_in_transaction;
use crate::execution::write::buffered::{build_planned_write_delta, PlannedWriteDelta};

pub(crate) struct PreparedWriteExecutionStep {
    prepared: PreparedWriteStep,
    runtime_state: PreparedWriteRuntimeState,
    planned_write_delta: Option<PlannedWriteDelta>,
    schema_registrations: SchemaRegistrationSet,
}

pub(crate) enum PreparedWriteExecutionRoute<'a> {
    Explain,
    PublicRead(&'a PreparedPublicReadArtifact),
    PlannedWriteDelta(&'a PlannedWriteDelta),
    PublicWriteNoop,
    Internal(&'a PreparedInternalWriteArtifact),
}

pub(crate) enum PreparedWriteExecutionStepResult {
    Immediate(QueryResult),
    Outcome(SqlExecutionOutcome),
}

impl PreparedWriteExecutionStep {
    pub(crate) fn build(
        prepared: PreparedWriteStep,
        runtime_state: &PreparedWriteRuntimeState,
    ) -> Result<Self, LixError> {
        let schema_registrations = schema_registrations_for_prepared_write_step(&prepared);
        let planned_write_delta = if prepared.diagnostic_context.explain_mode.is_some() {
            None
        } else {
            build_planned_write_delta(&prepared, runtime_state)?
        };
        Ok(Self {
            prepared,
            runtime_state: runtime_state.clone(),
            planned_write_delta,
            schema_registrations,
        })
    }

    pub(crate) fn prepared(&self) -> &PreparedWriteStep {
        &self.prepared
    }

    pub(crate) fn diagnostic_context(&self) -> &PreparedWriteDiagnosticContext {
        &self.prepared.diagnostic_context
    }

    pub(crate) fn statement_kind(&self) -> PreparedWriteStatementKind {
        self.prepared.statement_kind
    }

    pub(crate) fn planned_write_delta(&self) -> Option<&PlannedWriteDelta> {
        self.planned_write_delta.as_ref()
    }

    pub(crate) fn runtime_state(&self) -> &PreparedWriteRuntimeState {
        &self.runtime_state
    }

    pub(crate) fn schema_registrations(&self) -> &SchemaRegistrationSet {
        &self.schema_registrations
    }

    pub(crate) fn has_materialization_plan(&self) -> bool {
        self.planned_write_delta
            .as_ref()
            .is_some_and(|delta| !delta.materialization_plan().units.is_empty())
    }

    pub(crate) fn is_bufferable_write(&self) -> bool {
        self.prepared.diagnostic_context.explain_mode.is_none()
            && self.planned_write_delta.is_some()
            && !matches!(self.prepared.result_contract, ResultContract::DmlReturning)
            && !matches!(
                self.prepared.statement_kind,
                PreparedWriteStatementKind::Query | PreparedWriteStatementKind::Explain
            )
    }

    pub(crate) fn route(&self) -> PreparedWriteExecutionRoute<'_> {
        if self
            .prepared
            .diagnostic_context
            .plain_explain_template
            .is_some()
        {
            return PreparedWriteExecutionRoute::Explain;
        }
        if let Some(public_read) = self.prepared.public_read() {
            return PreparedWriteExecutionRoute::PublicRead(public_read);
        }
        if let Some(delta) = self.planned_write_delta.as_ref() {
            return PreparedWriteExecutionRoute::PlannedWriteDelta(delta);
        }
        if self.prepared.public_write().is_some() {
            return PreparedWriteExecutionRoute::PublicWriteNoop;
        }
        PreparedWriteExecutionRoute::Internal(
            self.prepared
                .internal_write()
                .expect("prepared non-public execution must include internal ops"),
        )
    }
}

pub(crate) async fn execute_prepared_write_execution_step_with_transaction(
    bindings: &dyn WriteExecutionBindings,
    transaction: &mut dyn LixBackendTransaction,
    step: &PreparedWriteExecutionStep,
    pending_transaction_view: Option<&PendingTransactionView>,
    pending_public_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
) -> Result<PreparedWriteExecutionStepResult, LixError> {
    match step.route() {
        PreparedWriteExecutionRoute::Explain => {
            let template = step
                .diagnostic_context()
                .plain_explain_template
                .as_ref()
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "plain explain route expected a non-analyze explain template",
                    )
                })?;
            Ok(PreparedWriteExecutionStepResult::Immediate(
                render_plain_explain_result(template)?,
            ))
        }
        PreparedWriteExecutionRoute::PublicRead(public_read) => {
            let execution_started = Instant::now();
            let public_result = match bindings
                .execute_prepared_public_read_with_pending_view(
                    transaction,
                    pending_transaction_view.map(|view| view as &dyn PendingView),
                    public_read,
                )
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    let normalized = normalize_sql_error_with_transaction_and_relation_names(
                        transaction,
                        error,
                        step.diagnostic_context().relation_names(),
                    )
                    .await;
                    return Err(normalized);
                }
            };
            if let Some(template) = step.diagnostic_context().analyzed_explain_template.as_ref() {
                return Ok(PreparedWriteExecutionStepResult::Immediate(
                    render_analyzed_explain_result(
                        template,
                        &public_result,
                        execution_started.elapsed(),
                    )?,
                ));
            }
            Ok(PreparedWriteExecutionStepResult::Immediate(public_result))
        }
        PreparedWriteExecutionRoute::PlannedWriteDelta(delta) => {
            let execution = execute_planned_write_delta(
                bindings,
                transaction,
                delta,
                pending_public_commit_session,
            )
            .await?;
            Ok(PreparedWriteExecutionStepResult::Outcome(execution))
        }
        PreparedWriteExecutionRoute::PublicWriteNoop => Ok(
            PreparedWriteExecutionStepResult::Outcome(empty_public_write_execution_outcome()),
        ),
        PreparedWriteExecutionRoute::Internal(internal) => {
            apply_schema_registrations_in_transaction(transaction, step.schema_registrations())
                .await?;
            let execution_started = Instant::now();
            match execute_internal_execution_with_transaction(
                transaction,
                internal,
                step.prepared().result_contract,
                step.runtime_state().functions(),
                internal.writer_key.as_deref(),
            )
            .await
            .map_err(LixError::from)
            {
                Ok(execution) => {
                    if let Some(template) =
                        step.diagnostic_context().analyzed_explain_template.as_ref()
                    {
                        return Ok(PreparedWriteExecutionStepResult::Immediate(
                            render_analyzed_explain_result(
                                template,
                                &execution.public_result,
                                execution_started.elapsed(),
                            )?,
                        ));
                    }
                    Ok(PreparedWriteExecutionStepResult::Outcome(execution))
                }
                Err(error) => {
                    let normalized = normalize_sql_error_with_transaction_and_relation_names(
                        transaction,
                        error,
                        step.diagnostic_context().relation_names(),
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
    internal: &PreparedInternalWriteArtifact,
    result_contract: ResultContract,
    functions: &dyn crate::contracts::LixFunctionProvider,
    writer_key: Option<&str>,
) -> Result<SqlExecutionOutcome, LixError> {
    let _ = (functions, writer_key, internal.should_refresh_file_cache);
    let internal_result =
        execute_prepared_with_transaction(transaction, &internal.prepared_batch).await?;
    mirror_registered_schema_mutations_in_transaction(transaction, &internal.mutations).await?;
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

fn schema_registrations_for_prepared_write_step(step: &PreparedWriteStep) -> SchemaRegistrationSet {
    let mut registrations = SchemaRegistrationSet::default();
    let Some(internal) = step.internal_write() else {
        return registrations;
    };

    for requirement in coalesce_live_table_requirements(&internal.live_table_requirements) {
        match requirement.schema_definition.as_ref() {
            Some(schema_definition) => {
                registrations.insert(SchemaRegistration::with_schema_definition(
                    requirement.schema_key.clone(),
                    schema_definition.clone(),
                ));
            }
            None => {
                registrations.insert(requirement.schema_key.clone());
            }
        }
    }

    registrations
}

async fn execute_prepared_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    batch: &PreparedBatch,
) -> Result<QueryResult, LixError> {
    let mut last_result = QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
    };
    for statement in &batch.steps {
        last_result = transaction
            .execute(&statement.sql, &statement.params)
            .await?;
    }
    Ok(last_result)
}
