use std::collections::BTreeMap;

use crate::engine::{Engine, ExecuteOptions};
use crate::sql_support::binding::{
    bind_statement_binding_template, compile_statement_binding_template_with_state,
    PlaceholderState, StatementBindingTemplate,
};
use crate::sql::execution::contracts::requirements::PlanRequirements;
use crate::sql::public::catalog::SurfaceRegistry;
use crate::sql::public::runtime::{
    classify_public_execution_route_with_registry, PublicExecutionRoute,
};
use crate::sql::internal::script::coalesce_vtable_inserts_in_transactions;
use crate::transaction::{
    execute_bound_statement_template_instance_in_borrowed_write_transaction,
    execute_bound_statement_template_instance_in_write_transaction,
    execute_program_with_new_write_transaction, BorrowedWriteTransaction, WriteTransaction,
};
use crate::{ExecuteResult, LixError, SqlDialect, Value};
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

pub(crate) async fn execute_execution_program_with_backend(
    engine: &Engine,
    program: &ExecutionProgram,
    options: ExecuteOptions,
    active_version_id: String,
    allow_internal_tables: bool,
) -> Result<ExecuteResult, LixError> {
    execute_program_with_new_write_transaction(
        engine,
        program,
        options,
        active_version_id,
        allow_internal_tables,
    )
    .await
}

pub(crate) async fn execute_execution_program_with_write_transaction(
    engine: &Engine,
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
                let result = execute_bound_statement_template_instance_in_write_transaction(
                    engine,
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
    engine: &Engine,
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
                let result =
                    execute_bound_statement_template_instance_in_borrowed_write_transaction(
                        engine,
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
