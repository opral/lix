//! Prepare stage ownership.
//!
//! This stage owns compiler-side preparation of execution artifacts,
//! compiled-execution data structures, and public-surface prepared artifacts.

mod compile;
mod compiled;
mod compiler_metadata;
pub(crate) mod contracts;
pub(crate) mod dependency_spec;
pub(crate) mod derive_effects;
pub(crate) mod derive_requirements;
pub(crate) mod execution_program;
pub(crate) mod filesystem_insert_ids;
pub(crate) mod intent;
pub(crate) mod prepared_batch;
mod prepared_read;
pub(crate) mod preprocess;
pub(crate) mod public_surface;
pub(crate) mod script;

pub(crate) use compile::{
    compile_execution_from_template_instance_with_context, PreparationPolicy,
    SqlPreparationContext, SqlPreparationSeed,
};
pub(crate) use compiled::CompiledExecution;
pub(crate) use compiler_metadata::{
    load_sql_compiler_metadata, load_sql_compiler_metadata_with_reader,
    load_sql_compiler_metadata_with_reader_and_pending_view, SqlCompilerMetadata,
};
pub(crate) use contracts::planned_statement::UpdateValidationPlan;
pub(crate) use execution_program::{BoundStatementTemplateInstance, ExecutionProgram};
pub(crate) use prepared_read::{
    prepare_committed_read_program_in_transaction, prepare_committed_read_program_with_backend,
    prepare_public_read_artifact, CommittedReadProgramContext,
};
pub(crate) use public_surface::{
    build_public_write_execution, build_public_write_invariant_trace,
    finalize_public_write_execution, public_authoritative_write_error,
    public_write_preparation_error, statement_references_public_surface,
    try_prepare_public_read_with_registry_and_internal_access, PreparedPublicExecution,
    PreparedPublicRead, PreparedPublicWrite,
};
