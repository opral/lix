//! Prepare stage ownership.
//!
//! This stage owns compiler-side preparation of execution artifacts,
//! compiled-execution data structures, and public-surface prepared artifacts.

mod compile;
mod compiled;
pub(crate) mod contracts;
pub(crate) mod dependency_spec;
pub(crate) mod derive_effects;
pub(crate) mod derive_requirements;
pub(crate) mod execution_program;
pub(crate) mod intent;
pub(crate) mod preprocess;
mod public_surface;

pub(crate) use compile::{
    compile_execution_from_template_instance_with_context,
    prepared_execution_mutates_public_surface_registry, DefaultSqlPreparationContext,
    PreparationPolicy, SqlCompilerMetadata, SqlPreparationContext,
};
pub(crate) use compiled::{
    schema_registrations_for_compiled_execution, CompiledExecution, CompiledInternalExecution,
};
pub(crate) use public_surface::{
    apply_public_surface_registry_mutations, build_public_write_execution,
    build_public_write_invariant_trace, classify_public_execution_route_with_registry,
    finalize_public_write_execution, public_authoritative_write_error,
    public_surface_registry_mutations, public_write_preparation_error,
    read::prepared_public_read_contract, semantic_plan_effects_from_domain_changes,
    state_commit_stream_operation, statement_references_public_surface,
    try_prepare_public_read_with_registry_and_internal_access, PreparedPublicRead,
    PreparedPublicWrite, PublicExecutionRoute,
};
