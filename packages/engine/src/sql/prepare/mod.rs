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
mod metadata_reader;
mod pending_overlay;
pub(crate) mod prepared_batch;
mod prepared_read;
pub(crate) mod preprocess;
pub(crate) mod public_surface;
pub(crate) mod script;
pub(crate) mod statement_effects;

pub(crate) use compile::{
    compile_execution_from_bound_statement_with_context, CompilePolicy, SqlCompilerContext,
    SqlCompilerSeed,
};
pub(crate) use compiled::CompiledExecution;
pub(crate) use compiler_metadata::{
    load_sql_compiler_metadata, load_sql_compiler_metadata_with_reader,
    load_sql_compiler_metadata_with_reader_and_pending_overlay, SqlCompilerMetadata,
};
pub(crate) use execution_program::{BoundStatementInstance, StatementBatch};
pub(crate) use metadata_reader::SqlPreparationMetadataReader;
pub(crate) use pending_overlay::{
    SqlPreparationPendingOverlay, SqlPreparationPendingRow, SqlPreparationPendingStorage,
};
pub(crate) use prepared_read::{
    prepare_committed_read_batch_in_transaction, prepare_committed_read_batch_with_backend,
    prepare_public_read_artifact, CommittedReadContext,
};
pub(crate) use public_surface::{
    build_public_write_execution, build_public_write_invariant_trace,
    finalize_public_write_execution, public_authoritative_write_error,
    public_write_preparation_error, statement_references_public_surface,
    try_prepare_public_read_with_registry_and_internal_access, PublicPlan, PublicReadPlan,
    PublicWritePlan,
};
pub(crate) use statement_effects::{derive_statement_effects, StatementEffects};
