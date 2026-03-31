use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::contracts::artifacts::ExecuteOptions;
use crate::contracts::surface::SurfaceRegistry;
use crate::runtime::execution_state::{ExecutionRuntimeEffects, ExecutionRuntimeState};
use crate::sql::binder::{
    bind_statement_binding_template, compile_statement_binding_template_with_state,
    RuntimeBindingValues, StatementBindingTemplate,
};
use crate::sql::internal::script::coalesce_state_surface_inserts_in_transactions;
use crate::sql::parser::placeholders::PlaceholderState;
use crate::write_runtime::{BorrowedWriteTransaction, WriteProgramExecutor, WriteTransaction};
use crate::{ExecuteResult, LixError, SqlDialect, Value};
use sqlparser::ast::Statement;

use super::contracts::requirements::PlanRequirements;
use super::{classify_public_execution_route_with_registry, PublicExecutionRoute};

pub(crate) struct ExecutionProgram {
    source_statements: Vec<Statement>,
    steps: Vec<ExecutionProgramStep>,
}

pub(crate) type SessionExecutionRuntimeHandle = Arc<SessionExecutionRuntime>;

pub(crate) struct SessionExecutionRuntime {
    public_surface_registry_generation: AtomicU64,
    statement_template_cache: Mutex<BTreeMap<StatementTemplateCacheKey, StatementTemplate>>,
}

impl SessionExecutionRuntime {
    pub(crate) fn new() -> SessionExecutionRuntimeHandle {
        Arc::new(Self {
            public_surface_registry_generation: AtomicU64::new(0),
            statement_template_cache: Mutex::new(BTreeMap::new()),
        })
    }

    pub(crate) fn public_surface_registry_generation(&self) -> u64 {
        self.public_surface_registry_generation
            .load(Ordering::SeqCst)
    }

    pub(crate) fn bump_public_surface_registry_generation(&self) {
        self.public_surface_registry_generation
            .fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn cached_statement_template(
        &self,
        key: &StatementTemplateCacheKey,
    ) -> Option<StatementTemplate> {
        self.statement_template_cache
            .lock()
            .expect("statement template cache lock poisoned")
            .get(key)
            .cloned()
    }

    pub(crate) fn cache_statement_template(
        &self,
        key: StatementTemplateCacheKey,
        template: StatementTemplate,
    ) {
        self.statement_template_cache
            .lock()
            .expect("statement template cache lock poisoned")
            .insert(key, template);
    }
}

pub(crate) struct ExecutionContext {
    pub(crate) options: ExecuteOptions,
    pub(crate) public_surface_registry: SurfaceRegistry,
    session_runtime: SessionExecutionRuntimeHandle,
    pub(crate) active_version_id: String,
    pub(crate) active_account_ids: Vec<String>,
    execution_runtime_state: Option<ExecutionRuntimeState>,
}

impl ExecutionContext {
    pub(crate) fn new(
        options: ExecuteOptions,
        public_surface_registry: SurfaceRegistry,
        session_runtime: SessionExecutionRuntimeHandle,
        active_version_id: String,
        active_account_ids: Vec<String>,
    ) -> Self {
        Self {
            options,
            public_surface_registry,
            session_runtime,
            active_version_id,
            active_account_ids,
            execution_runtime_state: None,
        }
    }

    pub(crate) fn bump_public_surface_registry_generation(&mut self) {
        self.session_runtime
            .bump_public_surface_registry_generation();
    }

    pub(crate) fn public_surface_registry_generation(&self) -> u64 {
        self.session_runtime.public_surface_registry_generation()
    }

    pub(crate) fn cached_statement_template(
        &self,
        key: &StatementTemplateCacheKey,
    ) -> Option<StatementTemplate> {
        self.session_runtime.cached_statement_template(key)
    }

    pub(crate) fn cache_statement_template(
        &self,
        key: StatementTemplateCacheKey,
        template: StatementTemplate,
    ) {
        self.session_runtime.cache_statement_template(key, template);
    }

    pub(crate) fn session_runtime(&self) -> SessionExecutionRuntimeHandle {
        Arc::clone(&self.session_runtime)
    }

    pub(crate) fn execution_runtime_state(&self) -> Option<&ExecutionRuntimeState> {
        self.execution_runtime_state.as_ref()
    }

    pub(crate) fn set_execution_runtime_state(&mut self, runtime_state: ExecutionRuntimeState) {
        self.execution_runtime_state = Some(runtime_state);
    }

    pub(crate) fn clear_execution_runtime_state(&mut self) {
        self.execution_runtime_state = None;
    }

    pub(crate) fn runtime_binding_values(&self) -> Result<RuntimeBindingValues, LixError> {
        Ok(RuntimeBindingValues {
            active_version_id: self.active_version_id.clone(),
            active_account_ids_json: serde_json::to_string(&self.active_account_ids).map_err(
                |error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("active account ids serialization failed: {error}"),
                    )
                },
            )?,
        })
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
                    crate::sql::executor::derive_requirements::derive_plan_requirements(
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
        runtime_bindings: &RuntimeBindingValues,
        parse_duration: Option<Duration>,
    ) -> Result<BoundStatementTemplateInstance, LixError> {
        let bound =
            bind_statement_binding_template(&self.binding_template, params, runtime_bindings)?;
        Ok(BoundStatementTemplateInstance {
            statement: bound.statement,
            params: bound.params,
            parse_duration,
            ownership_hint: self.ownership_hint,
            plan_requirements: self.plan_requirements.clone(),
        })
    }
}

#[derive(Clone)]
pub(crate) struct BoundStatementTemplateInstance {
    statement: Statement,
    params: Vec<Value>,
    parse_duration: Option<Duration>,
    ownership_hint: Option<StatementTemplateOwnership>,
    plan_requirements: PlanRequirements,
}

impl BoundStatementTemplateInstance {
    pub(crate) fn statement(&self) -> &Statement {
        &self.statement
    }

    pub(crate) fn params(&self) -> &[Value] {
        &self.params
    }

    pub(crate) fn parse_duration(&self) -> Option<Duration> {
        self.parse_duration
    }

    pub(crate) fn ownership_hint(&self) -> Option<StatementTemplateOwnership> {
        self.ownership_hint
    }

    pub(crate) fn plan_requirements(&self) -> &PlanRequirements {
        &self.plan_requirements
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
        runtime_bindings: &RuntimeBindingValues,
        parse_duration: Option<Duration>,
    ) -> Result<Self, LixError> {
        let source_statements =
            coalesce_state_surface_inserts_in_transactions(original_statements)?;
        let single_statement_parse_duration = (source_statements.len() == 1)
            .then_some(parse_duration)
            .flatten();
        let mut steps = Vec::with_capacity(source_statements.len());
        let mut placeholder_state = PlaceholderState::new();
        for statement in source_statements.iter().cloned() {
            if is_transaction_control(&statement) {
                steps.push(ExecutionProgramStep::TransactionControl);
                continue;
            }

            let (template, next_placeholder_state) =
                StatementTemplate::compile(statement, dialect, params.len(), placeholder_state)?;
            let bound_template =
                template.bind(params, runtime_bindings, single_statement_parse_duration)?;
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

    pub(crate) fn is_plain_committed_read(&self) -> bool {
        self.steps.iter().all(|step| match step {
            ExecutionProgramStep::TransactionControl => true,
            ExecutionProgramStep::Statement(step) => {
                step.bound_template.plan_requirements().read_only_query
            }
        })
    }

    pub(crate) fn runtime_effects(&self) -> ExecutionRuntimeEffects {
        self.steps.iter().fold(
            ExecutionRuntimeEffects::default(),
            |effects, step| match step {
                ExecutionProgramStep::TransactionControl => effects,
                ExecutionProgramStep::Statement(step) => {
                    effects.merge(step.bound_template.plan_requirements().runtime_effects)
                }
            },
        )
    }

    pub(crate) fn steps(&self) -> impl Iterator<Item = &BoundStatementTemplateInstance> {
        self.steps.iter().filter_map(|step| match step {
            ExecutionProgramStep::TransactionControl => None,
            ExecutionProgramStep::Statement(step) => Some(&step.bound_template),
        })
    }
}

pub(crate) async fn execute_execution_program_with_write_transaction(
    executor: &dyn WriteProgramExecutor,
    write_transaction: &mut WriteTransaction<'_>,
    program: &ExecutionProgram,
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
) -> Result<ExecuteResult, LixError> {
    let mut results = Vec::with_capacity(program.steps.len());

    for step in &program.steps {
        match step {
            ExecutionProgramStep::TransactionControl => {}
            ExecutionProgramStep::Statement(step) => {
                let result = executor
                    .execute_bound_statement_template_instance_in_write_transaction(
                        write_transaction,
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

    if crate::sql::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements(
        program.source_statements(),
    ) {
        write_transaction.mark_installed_plugins_cache_invalidation_pending();
    }

    Ok(ExecuteResult {
        statements: results,
    })
}

pub(crate) async fn execute_execution_program_with_borrowed_write_transaction(
    executor: &dyn WriteProgramExecutor,
    write_transaction: &mut BorrowedWriteTransaction<'_>,
    program: &ExecutionProgram,
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
) -> Result<ExecuteResult, LixError> {
    let mut results = Vec::with_capacity(program.steps.len());

    for step in &program.steps {
        match step {
            ExecutionProgramStep::TransactionControl => {}
            ExecutionProgramStep::Statement(step) => {
                let result = executor
                    .execute_bound_statement_template_instance_in_borrowed_write_transaction(
                        write_transaction,
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

    if crate::sql::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements(
        program.source_statements(),
    ) {
        write_transaction.mark_installed_plugins_cache_invalidation_pending();
    }

    Ok(ExecuteResult {
        statements: results,
    })
}

fn is_transaction_control(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
    )
}
