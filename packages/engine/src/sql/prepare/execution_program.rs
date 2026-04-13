use std::time::Duration;

use crate::contracts::ExecutionRuntimeEffects;
use crate::sql::binder::{
    bind_statement_binding_template, compile_statement_binding_template_with_state,
    RuntimeBindingValues, StatementBindingTemplate,
};
use crate::sql::parser::placeholders::PlaceholderState;
use crate::sql::prepare::script::coalesce_state_surface_inserts_in_transactions;
use crate::{LixError, SqlDialect, Value};
use sqlparser::ast::Statement;

use super::contracts::requirements::PlanRequirements;

pub(crate) struct StatementBatch {
    source_statements: Vec<Statement>,
    steps: Vec<StatementBatchStep>,
}

enum StatementBatchStep {
    TransactionControl,
    Statement(PreparedStatementStep),
}

struct PreparedStatementStep {
    bound_statement: BoundStatementInstance,
}

#[derive(Clone)]
pub(crate) struct StatementTemplate {
    binding_template: StatementBindingTemplate,
    plan_requirements: PlanRequirements,
}

impl StatementTemplate {
    pub(crate) fn compile(
        statement: Statement,
        dialect: SqlDialect,
        params_len: usize,
        placeholder_state: PlaceholderState,
    ) -> Result<(Self, PlaceholderState), LixError> {
        Self::build(statement, dialect, params_len, placeholder_state)
    }

    fn build(
        statement: Statement,
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
            },
            next_placeholder_state,
        ))
    }

    pub(crate) fn bind(
        &self,
        params: &[Value],
        runtime_bindings: &RuntimeBindingValues,
        parse_duration: Option<Duration>,
    ) -> Result<BoundStatementInstance, LixError> {
        let bound =
            bind_statement_binding_template(&self.binding_template, params, runtime_bindings)?;
        Ok(BoundStatementInstance {
            statement: bound.statement,
            params: bound.params,
            parse_duration,
            plan_requirements: self.plan_requirements.clone(),
        })
    }
}

#[derive(Clone)]
pub(crate) struct BoundStatementInstance {
    statement: Statement,
    params: Vec<Value>,
    parse_duration: Option<Duration>,
    plan_requirements: PlanRequirements,
}

impl BoundStatementInstance {
    pub(crate) fn statement(&self) -> &Statement {
        &self.statement
    }

    pub(crate) fn params(&self) -> &[Value] {
        &self.params
    }

    pub(crate) fn parse_duration(&self) -> Option<Duration> {
        self.parse_duration
    }

    pub(crate) fn plan_requirements(&self) -> &PlanRequirements {
        &self.plan_requirements
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct StatementTemplateCacheKey {
    sql: String,
    dialect: u8,
    allow_internal_relations: bool,
    public_surface_registry_generation: u64,
}

#[cfg(test)]
impl StatementTemplateCacheKey {
    pub(crate) fn new(
        sql: &str,
        dialect: SqlDialect,
        allow_internal_relations: bool,
        public_surface_registry_generation: u64,
    ) -> Self {
        Self {
            sql: sql.to_string(),
            dialect: match dialect {
                SqlDialect::Sqlite => 1,
                SqlDialect::Postgres => 2,
            },
            allow_internal_relations,
            public_surface_registry_generation,
        }
    }
}

impl StatementBatch {
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
                steps.push(StatementBatchStep::TransactionControl);
                continue;
            }

            let (template, next_placeholder_state) =
                StatementTemplate::compile(statement, dialect, params.len(), placeholder_state)?;
            let bound_statement =
                template.bind(params, runtime_bindings, single_statement_parse_duration)?;
            placeholder_state = next_placeholder_state;
            steps.push(StatementBatchStep::Statement(PreparedStatementStep {
                bound_statement,
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
            StatementBatchStep::TransactionControl => true,
            StatementBatchStep::Statement(step) => {
                step.bound_statement.plan_requirements().read_only_query
            }
        })
    }

    pub(crate) fn runtime_effects(&self) -> ExecutionRuntimeEffects {
        self.steps.iter().fold(
            ExecutionRuntimeEffects::default(),
            |effects, step| match step {
                StatementBatchStep::TransactionControl => effects,
                StatementBatchStep::Statement(step) => {
                    effects.merge(step.bound_statement.plan_requirements().runtime_effects)
                }
            },
        )
    }

    pub(crate) fn steps(&self) -> impl Iterator<Item = &BoundStatementInstance> {
        self.steps.iter().filter_map(|step| match step {
            StatementBatchStep::TransactionControl => None,
            StatementBatchStep::Statement(step) => Some(&step.bound_statement),
        })
    }
}

fn is_transaction_control(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
    )
}
