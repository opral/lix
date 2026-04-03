use std::time::Duration;

use crate::contracts::surface::SurfaceRegistry;
use crate::runtime::execution_state::ExecutionRuntimeEffects;
use crate::sql::binder::{
    bind_statement_binding_template, compile_statement_binding_template_with_state,
    RuntimeBindingValues, StatementBindingTemplate,
};
use crate::sql::internal::script::coalesce_state_surface_inserts_in_transactions;
use crate::sql::parser::placeholders::PlaceholderState;
use crate::{LixError, SqlDialect, Value};
use sqlparser::ast::Statement;

use super::contracts::requirements::PlanRequirements;
use super::{classify_public_execution_route_with_registry, PublicExecutionRoute};

pub(crate) struct ExecutionProgram {
    source_statements: Vec<Statement>,
    steps: Vec<ExecutionProgramStep>,
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
                    crate::sql::prepare::derive_requirements::derive_plan_requirements(
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

fn is_transaction_control(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
    )
}
