use std::collections::{BTreeMap, BTreeSet};

use crate::deterministic_mode::RuntimeFunctionProvider;
use crate::engine::{
    normalize_sql_execution_error_with_backend, Engine, ExecuteOptions, TransactionBackendAdapter,
};
use crate::functions::SharedFunctionProvider;
use crate::schema::registry::{
    coalesce_live_table_requirements, ensure_schema_live_table_with_requirement_in_transaction,
};
use crate::sql::ast::utils::{
    bind_statement_binding_template, compile_statement_binding_template_with_state,
    PlaceholderState, StatementBindingTemplate,
};
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::contracts::executor_error::ExecutorError;
use crate::sql::execution::contracts::planned_statement::{
    MutationRow, SchemaLiveTableRequirement, UpdateValidationPlan,
};
use crate::sql::execution::contracts::prepared_statement::PreparedStatement;
use crate::sql::execution::contracts::requirements::PlanRequirements;
use crate::sql::execution::contracts::result_contract::ResultContract;
use crate::sql::execution::shared_path::{
    self, PendingPublicCommitSession, PendingTransactionView,
};
use crate::sql::execution::write_txn_plan::{build_txn_delta, TxnDelta};
use crate::sql::execution::write_txn_runner::{
    run_txn_delta_with_transaction, stamp_watermark_before_commit,
};
use crate::sql::public::catalog::SurfaceRegistry;
use crate::sql::public::runtime::{
    classify_public_execution_route_with_registry, PreparedPublicRead, PublicExecutionRoute,
};
use crate::state::internal::followup::execute_internal_postprocess_with_transaction;
use crate::state::internal::script::coalesce_vtable_inserts_in_transactions;
use crate::state::internal::PostprocessPlan;
use crate::state::stream::StateCommitStreamChange;
use crate::{ExecuteResult, LixBackendTransaction, LixError, QueryResult, SqlDialect, Value};
use sqlparser::ast::Statement;

pub(crate) struct ExecutionProgram {
    source_statements: Vec<Statement>,
    steps: Vec<ExecutionProgramStep>,
}

pub(crate) struct ExecutionContext {
    pub(crate) options: ExecuteOptions,
    pub(crate) public_surface_registry: SurfaceRegistry,
    pub(crate) public_surface_registry_generation: u64,
    pub(crate) active_version_id: String,
    pub(crate) active_version_changed: bool,
    pub(crate) installed_plugins_cache_invalidation_pending: bool,
    pub(crate) public_surface_registry_dirty: bool,
    pub(crate) pending_state_commit_stream_changes: Vec<StateCommitStreamChange>,
    pub(crate) observe_tick_already_emitted: bool,
    pub(crate) pending_public_commit_session: Option<PendingPublicCommitSession>,
    pub(crate) mutation_journal: crate::sql::execution::write_txn_plan::MutationJournal,
    pub(crate) statement_template_cache: BTreeMap<StatementTemplateCacheKey, StatementTemplate>,
}

impl ExecutionContext {
    pub(crate) fn new(
        options: ExecuteOptions,
        public_surface_registry: SurfaceRegistry,
        active_version_id: String,
    ) -> Self {
        Self {
            options,
            public_surface_registry,
            public_surface_registry_generation: 0,
            active_version_id,
            active_version_changed: false,
            installed_plugins_cache_invalidation_pending: false,
            public_surface_registry_dirty: false,
            pending_state_commit_stream_changes: Vec::new(),
            observe_tick_already_emitted: false,
            pending_public_commit_session: None,
            mutation_journal: Default::default(),
            statement_template_cache: BTreeMap::new(),
        }
    }

    pub(crate) fn bump_public_surface_registry_generation(&mut self) {
        self.public_surface_registry_generation += 1;
    }
}

enum ExecutionProgramStep {
    TransactionControl,
    Statement(ExecutionProgramStatement),
}

struct ExecutionProgramStatement {
    bound_template: BoundStatementTemplateInstance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatementTemplateOwnership {
    PublicRead,
    PublicWrite,
    Internal,
}

#[derive(Clone)]
pub(crate) struct StatementTemplate {
    binding_template: StatementBindingTemplate,
    ownership_hint: Option<StatementTemplateOwnership>,
    plan_requirements: PlanRequirements,
    requires_generated_filesystem_insert_id: bool,
}

impl StatementTemplate {
    pub(crate) fn compile(
        statement: Statement,
        dialect: SqlDialect,
        params_len: usize,
        placeholder_state: PlaceholderState,
    ) -> Result<(Self, PlaceholderState), LixError> {
        Self::build(statement, None, dialect, params_len, placeholder_state)
    }

    pub(crate) fn compile_with_registry(
        statement: Statement,
        registry: &SurfaceRegistry,
        dialect: SqlDialect,
        params_len: usize,
    ) -> Result<Self, LixError> {
        let ownership_hint = Some(
            match classify_public_execution_route_with_registry(
                registry,
                std::slice::from_ref(&statement),
            ) {
                Some(PublicExecutionRoute::Read) => StatementTemplateOwnership::PublicRead,
                Some(PublicExecutionRoute::Write) => StatementTemplateOwnership::PublicWrite,
                None => StatementTemplateOwnership::Internal,
            },
        );
        let (template, _) = Self::build(
            statement,
            ownership_hint,
            dialect,
            params_len,
            PlaceholderState::new(),
        )?;
        Ok(template)
    }

    fn build(
        statement: Statement,
        ownership_hint: Option<StatementTemplateOwnership>,
        dialect: SqlDialect,
        params_len: usize,
        placeholder_state: PlaceholderState,
    ) -> Result<(Self, PlaceholderState), LixError> {
        let binding_template = compile_statement_binding_template_with_state(
            &statement,
            params_len,
            dialect,
            placeholder_state,
        )?;
        let next_placeholder_state = binding_template.state.clone();
        Ok((
            Self {
                binding_template,
                plan_requirements:
                    crate::sql::execution::derive_requirements::derive_plan_requirements(
                        std::slice::from_ref(&statement),
                    ),
                requires_generated_filesystem_insert_id:
                    crate::filesystem::statements_require_generated_filesystem_insert_ids(
                        std::slice::from_ref(&statement),
                    ),
                ownership_hint,
            },
            next_placeholder_state,
        ))
    }

    pub(crate) fn bind(
        &self,
        params: &[Value],
    ) -> Result<BoundStatementTemplateInstance, LixError> {
        let bound = bind_statement_binding_template(&self.binding_template, params)?;
        Ok(BoundStatementTemplateInstance {
            statement: bound.statement,
            params: bound.params,
            ownership_hint: self.ownership_hint,
            plan_requirements: self.plan_requirements.clone(),
            requires_generated_filesystem_insert_id: self.requires_generated_filesystem_insert_id,
        })
    }
}

#[derive(Clone)]
pub(crate) struct BoundStatementTemplateInstance {
    statement: Statement,
    params: Vec<Value>,
    ownership_hint: Option<StatementTemplateOwnership>,
    plan_requirements: PlanRequirements,
    requires_generated_filesystem_insert_id: bool,
}

impl BoundStatementTemplateInstance {
    pub(crate) fn statement(&self) -> &Statement {
        &self.statement
    }

    pub(crate) fn params(&self) -> &[Value] {
        &self.params
    }

    pub(crate) fn ownership_hint(&self) -> Option<StatementTemplateOwnership> {
        self.ownership_hint
    }

    pub(crate) fn plan_requirements(&self) -> &PlanRequirements {
        &self.plan_requirements
    }

    pub(crate) fn requires_generated_filesystem_insert_id(&self) -> bool {
        self.requires_generated_filesystem_insert_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct StatementTemplateCacheKey {
    sql: String,
    dialect: u8,
    allow_internal_tables: bool,
    public_surface_registry_generation: u64,
}

impl StatementTemplateCacheKey {
    pub(crate) fn new(
        sql: &str,
        dialect: SqlDialect,
        allow_internal_tables: bool,
        public_surface_registry_generation: u64,
    ) -> Self {
        Self {
            sql: sql.to_string(),
            dialect: match dialect {
                SqlDialect::Sqlite => 1,
                SqlDialect::Postgres => 2,
            },
            allow_internal_tables,
            public_surface_registry_generation,
        }
    }
}

impl ExecutionProgram {
    pub(crate) fn compile(
        original_statements: Vec<Statement>,
        params: &[Value],
        dialect: SqlDialect,
    ) -> Result<Self, LixError> {
        let source_statements = coalesce_vtable_inserts_in_transactions(original_statements)?;
        let mut steps = Vec::with_capacity(source_statements.len());
        let mut placeholder_state = PlaceholderState::new();
        for statement in source_statements.iter().cloned() {
            if is_transaction_control(&statement) {
                steps.push(ExecutionProgramStep::TransactionControl);
                continue;
            }

            let (template, next_placeholder_state) =
                StatementTemplate::compile(statement, dialect, params.len(), placeholder_state)?;
            let bound_template = template.bind(params)?;
            placeholder_state = next_placeholder_state;
            steps.push(ExecutionProgramStep::Statement(ExecutionProgramStatement {
                bound_template,
            }));
        }

        Ok(Self {
            source_statements,
            steps,
        })
    }

    pub(crate) fn source_statements(&self) -> &[Statement] {
        &self.source_statements
    }
}

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
    PublicWrite(crate::sql::public::runtime::PreparedPublicWrite),
    Internal(CompiledInternalExecution),
}

impl CompiledExecution {
    pub(crate) fn public_read(&self) -> Option<&PreparedPublicRead> {
        match &self.body {
            CompiledExecutionBody::PublicRead(read) => Some(read),
            CompiledExecutionBody::PublicWrite(_) | CompiledExecutionBody::Internal(_) => None,
        }
    }

    pub(crate) fn public_write(&self) -> Option<&crate::sql::public::runtime::PreparedPublicWrite> {
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
    txn_delta: Option<TxnDelta>,
}

pub(crate) enum CompiledExecutionRoute<'a> {
    PublicRead(&'a PreparedPublicRead),
    TxnDelta(&'a TxnDelta),
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
        let txn_delta = build_txn_delta(&execution, writer_key)?;
        Ok(Self {
            execution,
            txn_delta,
        })
    }

    pub(crate) fn execution(&self) -> &CompiledExecution {
        &self.execution
    }

    pub(crate) fn txn_delta(&self) -> Option<&TxnDelta> {
        self.txn_delta.as_ref()
    }

    pub(crate) fn has_materialization_plan(&self) -> bool {
        self.txn_delta
            .as_ref()
            .is_some_and(|delta| !delta.materialization_plan().units.is_empty())
    }

    pub(crate) fn is_bufferable_write(&self, statement: &Statement) -> bool {
        self.txn_delta.is_some()
            && !matches!(self.execution.result_contract, ResultContract::DmlReturning)
            && !matches!(statement, Statement::Query(_) | Statement::Explain { .. })
    }

    pub(crate) fn route(&self) -> CompiledExecutionRoute<'_> {
        if let Some(public_read) = self.execution.public_read() {
            return CompiledExecutionRoute::PublicRead(public_read);
        }
        if let Some(delta) = self.txn_delta.as_ref() {
            return CompiledExecutionRoute::TxnDelta(delta);
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

pub(crate) async fn execute_execution_program_with_backend(
    engine: &Engine,
    program: &ExecutionProgram,
    options: ExecuteOptions,
    active_version_id: String,
    allow_internal_tables: bool,
) -> Result<ExecuteResult, LixError> {
    let mut transaction = engine.begin_write_unit().await?;
    let mut context = engine.new_execution_context_with_active_version(options, active_version_id);
    let result = execute_execution_program_with_transaction(
        engine,
        transaction.as_mut(),
        program,
        allow_internal_tables,
        &mut context,
    )
    .await;

    match result {
        Ok(result) => {
            engine
                .prepare_execution_context_for_commit(transaction.as_mut(), &mut context)
                .await?;
            stamp_watermark_before_commit(transaction.as_mut()).await?;
            transaction.commit().await?;
            engine.finalize_committed_execution_context(context).await?;
            Ok(result)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

pub(crate) async fn execute_execution_program_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixBackendTransaction,
    program: &ExecutionProgram,
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
) -> Result<ExecuteResult, LixError> {
    let previous_active_version_id = context.active_version_id.clone();
    let mut results = Vec::with_capacity(program.steps.len());

    for step in &program.steps {
        match step {
            ExecutionProgramStep::TransactionControl => {}
            ExecutionProgramStep::Statement(step) => {
                let result = engine
                    .execute_bound_statement_template_instance_in_transaction(
                        transaction,
                        &step.bound_template,
                        allow_internal_tables,
                        context,
                        None,
                        false,
                    )
                    .await?;
                results.push(result);
            }
        }
    }

    if context.active_version_id != previous_active_version_id {
        context.active_version_changed = true;
    }
    if crate::sql::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements(
        program.source_statements(),
    ) {
        context.installed_plugins_cache_invalidation_pending = true;
    }

    Ok(ExecuteResult {
        statements: results,
    })
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
                        return Err(enrich_public_read_unknown_column_error(
                            public_read,
                            normalized,
                        )
                        .unwrap_or_else(|error| error));
                    }
                };
            Ok(CompiledExecutionStepResult::Immediate(public_result))
        }
        CompiledExecutionRoute::TxnDelta(delta) => {
            let execution = run_txn_delta_with_transaction(
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
    for registration in coalesce_live_table_requirements(&internal.live_table_requirements) {
        ensure_schema_live_table_with_requirement_in_transaction(transaction, &registration)
            .await
            .map_err(ExecutorError::execute)?;
    }

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

fn is_transaction_control(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
    )
}

fn enrich_public_read_unknown_column_error(
    public_read: &PreparedPublicRead,
    error: LixError,
) -> Result<LixError, LixError> {
    if error.code != "LIX_ERROR_SQL_UNKNOWN_COLUMN" {
        return Err(error);
    }
    let Some(missing_column) = parse_unknown_column_name(&error.description) else {
        return Err(error);
    };
    let Some(structured_read) = public_read.structured_read() else {
        return Err(error);
    };
    let available_columns = structured_read
        .surface_binding
        .descriptor
        .visible_columns
        .iter()
        .chain(
            structured_read
                .surface_binding
                .descriptor
                .hidden_columns
                .iter(),
        )
        .map(String::as_str)
        .collect::<Vec<_>>();
    Ok(crate::errors::sql_unknown_column_error(
        &missing_column,
        Some(&structured_read.surface_binding.descriptor.public_name),
        available_columns.as_slice(),
        None,
    ))
}

fn parse_unknown_column_name(description: &str) -> Option<String> {
    extract_name_after_prefix(description, "no such column:")
        .or_else(|| extract_name_between(description, "column `", "` does not exist"))
        .or_else(|| extract_name_between(description, "column \"", "\" does not exist"))
        .or_else(|| extract_name_between(description, "column '", "' does not exist"))
}

fn extract_name_between(description: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let lower = description.to_ascii_lowercase();
    let start_marker_lower = start_marker.to_ascii_lowercase();
    let end_marker_lower = end_marker.to_ascii_lowercase();
    let start = lower.find(&start_marker_lower)? + start_marker_lower.len();
    let end = lower[start..].find(&end_marker_lower)? + start;
    sanitize_name(&description[start..end])
}

fn extract_name_after_prefix(description: &str, prefix: &str) -> Option<String> {
    let lower = description.to_ascii_lowercase();
    let marker = prefix.to_ascii_lowercase();
    let mut start = lower.find(&marker)? + marker.len();
    while description[start..]
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_whitespace())
    {
        start += 1;
    }
    let mut end = description.len();
    for stop in [' ', '\n', '\r', '\t', ',', ')', ';'] {
        if let Some(index) = description[start..].find(stop) {
            end = end.min(start + index);
        }
    }
    sanitize_name(&description[start..end])
}

fn sanitize_name(raw: &str) -> Option<String> {
    let trimmed = raw
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('\'');
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}
