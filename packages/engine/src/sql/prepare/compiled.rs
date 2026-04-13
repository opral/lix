use crate::contracts::PreparedStatement;
use crate::sql::explain::ExplainArtifacts;
use crate::sql::logical_plan::ResultContract;

use super::contracts::effects::PlanEffects;
use super::contracts::planned_statement::{
    MutationRow, SchemaLiveTableRequirement, UpdateValidationPlan,
};
use super::public_surface::{PublicReadPlan, PublicWritePlan};

#[derive(Clone)]
pub(crate) struct CompiledDirectExecution {
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
    pub(crate) should_refresh_file_cache: bool,
}

pub(crate) struct CompiledExecution {
    pub(crate) filesystem_intent: crate::sql::prepare::intent::FilesystemIntent,
    pub(crate) explain: Option<ExplainArtifacts>,
    pub(crate) result_contract: ResultContract,
    pub(crate) effects: PlanEffects,
    pub(crate) read_only_query: bool,
    pub(crate) body: CompiledExecutionBody,
}

pub(crate) enum CompiledExecutionBody {
    PublicRead(PublicReadPlan),
    PublicWrite(PublicWritePlan),
    Direct(CompiledDirectExecution),
}

impl CompiledExecution {
    pub(crate) fn public_read(&self) -> Option<&PublicReadPlan> {
        match &self.body {
            CompiledExecutionBody::PublicRead(read) => Some(read),
            CompiledExecutionBody::PublicWrite(_) | CompiledExecutionBody::Direct(_) => None,
        }
    }

    pub(crate) fn public_write(&self) -> Option<&PublicWritePlan> {
        match &self.body {
            CompiledExecutionBody::PublicWrite(write) => Some(write),
            CompiledExecutionBody::PublicRead(_) | CompiledExecutionBody::Direct(_) => None,
        }
    }

    pub(crate) fn public_write_mut(&mut self) -> Option<&mut PublicWritePlan> {
        match &mut self.body {
            CompiledExecutionBody::PublicWrite(write) => Some(write),
            CompiledExecutionBody::PublicRead(_) | CompiledExecutionBody::Direct(_) => None,
        }
    }

    pub(crate) fn direct_execution(&self) -> Option<&CompiledDirectExecution> {
        match &self.body {
            CompiledExecutionBody::Direct(direct) => Some(direct),
            CompiledExecutionBody::PublicRead(_) | CompiledExecutionBody::PublicWrite(_) => None,
        }
    }

    pub(crate) fn explain(&self) -> Option<&ExplainArtifacts> {
        self.explain.as_ref()
    }

    pub(crate) fn plain_explain(&self) -> Option<&ExplainArtifacts> {
        self.explain
            .as_ref()
            .filter(|explain| !explain.requires_execution())
    }

    pub(crate) fn analyzed_explain(&self) -> Option<&ExplainArtifacts> {
        self.explain
            .as_ref()
            .filter(|explain| explain.requires_execution())
    }
}
