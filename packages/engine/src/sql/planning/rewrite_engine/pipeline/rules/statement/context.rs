use sqlparser::ast::Statement;

use crate::engine::sql::planning::rewrite_engine::types::{
    MutationRow, PostprocessPlan, RewriteOutput, SchemaRegistration, UpdateValidationPlan,
};
use crate::engine::sql::planning::rewrite_engine::DetectedFileDomainChange;
use crate::{LixBackend, Value};

pub(crate) struct StatementContext<'a> {
    pub(crate) params: &'a [Value],
    pub(crate) writer_key: Option<&'a str>,
    pub(crate) backend: Option<&'a dyn LixBackend>,
    pub(crate) detected_file_domain_changes: &'a [DetectedFileDomainChange],
    pub(crate) side_effects: Vec<Statement>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) generated_params: Vec<Value>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
    pub(crate) postprocess: Option<PostprocessPlan>,
}

impl<'a> StatementContext<'a> {
    pub(crate) fn new_sync(params: &'a [Value], writer_key: Option<&'a str>) -> Self {
        Self {
            params,
            writer_key,
            backend: None,
            detected_file_domain_changes: &[],
            side_effects: Vec::new(),
            registrations: Vec::new(),
            generated_params: Vec::new(),
            mutations: Vec::new(),
            update_validations: Vec::new(),
            postprocess: None,
        }
    }

    pub(crate) fn new_backend(
        backend: &'a dyn LixBackend,
        params: &'a [Value],
        writer_key: Option<&'a str>,
        detected_file_domain_changes: &'a [DetectedFileDomainChange],
    ) -> Self {
        Self {
            params,
            writer_key,
            backend: Some(backend),
            detected_file_domain_changes,
            side_effects: Vec::new(),
            registrations: Vec::new(),
            generated_params: Vec::new(),
            mutations: Vec::new(),
            update_validations: Vec::new(),
            postprocess: None,
        }
    }

    pub(crate) fn take_output(&mut self, statements: Vec<Statement>) -> RewriteOutput {
        RewriteOutput {
            statements,
            params: std::mem::take(&mut self.generated_params),
            registrations: std::mem::take(&mut self.registrations),
            postprocess: self.postprocess.take(),
            mutations: std::mem::take(&mut self.mutations),
            update_validations: std::mem::take(&mut self.update_validations),
        }
    }
}
