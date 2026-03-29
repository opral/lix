use crate::backend::prepared::PreparedStatement;
use crate::live_state::{
    coalesce_live_table_requirements, SchemaRegistration, SchemaRegistrationSet,
};
use crate::sql::executor::contracts::effects::PlanEffects;
use crate::sql::executor::contracts::planned_statement::{
    MutationRow, SchemaLiveTableRequirement, UpdateValidationPlan,
};
use crate::sql::executor::runtime_state::ExecutionRuntimeState;
use crate::sql::executor::public_runtime::{PreparedPublicRead, PreparedPublicWrite};
use crate::sql::logical_plan::ResultContract;

#[derive(Clone)]
pub(crate) struct CompiledInternalExecution {
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
    pub(crate) should_refresh_file_cache: bool,
}

pub(crate) struct CompiledExecution {
    pub(crate) intent: crate::sql::executor::intent::ExecutionIntent,
    pub(crate) runtime_state: ExecutionRuntimeState,
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

pub(crate) fn schema_registrations_for_compiled_execution(
    execution: &CompiledExecution,
) -> SchemaRegistrationSet {
    let mut registrations = SchemaRegistrationSet::default();
    if let Some(internal) = execution.internal_execution() {
        for requirement in coalesce_live_table_requirements(&internal.live_table_requirements) {
            match requirement.schema_definition.as_ref() {
                Some(schema_definition) => {
                    registrations.insert(SchemaRegistration::with_schema_definition(
                        requirement.schema_key.clone(),
                        schema_definition.clone(),
                    ))
                }
                None => registrations.insert(requirement.schema_key.clone()),
            }
        }
    }
    registrations
}
