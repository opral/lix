//! Executor stage ownership.
//!
//! This stage owns compile/prepare execution, compiled execution artifacts, and
//! public-surface execution.

mod compile;
mod compiled;
pub(crate) mod contracts;
pub(crate) mod dependency_spec;
pub(crate) mod derive_effects;
pub(crate) mod derive_requirements;
pub(crate) mod execute_prepared;
pub(crate) mod execution_program;
pub(crate) mod intent;
pub(crate) mod preprocess;
mod public_runtime;

pub(crate) use compile::{
    compile_execution_from_template_instance_with_backend,
    prepared_execution_mutates_public_surface_registry, PreparationPolicy,
};
pub(crate) use compiled::{
    schema_registrations_for_compiled_execution, CompiledExecution, CompiledInternalExecution,
};
pub(crate) use public_runtime::{
    apply_public_surface_registry_mutations, build_tracked_txn_unit,
    classify_public_execution_route_with_registry, execute_prepared_public_read,
    public_surface_registry_mutations, semantic_plan_effects_from_domain_changes,
    state_commit_stream_operation, statement_references_public_surface_with_backend,
    try_prepare_public_read_with_registry_and_internal_access, PreparedPublicRead,
    PreparedPublicWrite, PublicExecutionRoute, TrackedTxnUnit,
};
