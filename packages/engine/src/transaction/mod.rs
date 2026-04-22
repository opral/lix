//! Transaction-scoped root API.
//!
//! `transaction` owns buffered write units, pending overlays, transaction-local
//! filesystem state, and backend transaction adapters. Callers outside the
//! transaction implementation should depend on `crate::transaction::{...}`
//! instead of reaching into transaction child modules directly.

mod backend;
pub(crate) mod buffered;
mod buffered_write_transaction;
mod checkpoint_labels;
mod commit_artifacts;
pub(crate) mod commit_idempotency;
mod compiler_state;
mod contracts;
mod deterministic_sequence;
pub(crate) mod filesystem;
#[cfg(test)]
mod live_state_write_transaction;
pub(crate) mod overlay;
#[cfg(test)]
mod overlay_read_context;
mod pending_overlay_public_reads;
pub(crate) mod pipeline;
mod prepared_artifacts;
mod prepared_step;
mod prepared_write;
mod validation_input;
mod write_batch;

pub(crate) use backend::{
    lookup_directory_id_by_path_in_transaction,
    normalize_sql_error_with_transaction_and_relation_names, TransactionExecutionBackend,
};
pub(crate) use buffered::{
    apply_schema_registrations_in_transaction, upsert_registered_schema_mirror_row_in_transaction,
    BufferedWriteCommandMetadata, BufferedWriteExecutionResult, BufferedWriteFlushClass,
    BufferedWriteSessionEffects, PlannedDirectWriteUnit, PublicWriteTxnUnit,
    RegisteredSchemaMirrorRow, TransactionWriteDelta,
};
pub(crate) use buffered_write_transaction::BufferedWriteTransaction;
pub(crate) use checkpoint_labels::{
    append_checkpoint_commit_label_fact_in_transaction, CheckpointCommitLabelWrite,
};
pub(crate) use commit_artifacts::{
    append_commit_idempotency_row, load_commit_change_snapshot_id_in_transaction,
    load_commit_idempotency_replay_in_transaction, PendingCommitLane, PendingCommitState,
};
pub(crate) use commit_idempotency::init_commit_idempotency_storage;
pub(crate) use compiler_state::{
    SessionCompilerCache, SessionCompilerCacheHandle, SessionCompilerState,
};
pub(crate) use contracts::{
    BufferedWriteExecutionInput, DeferredCommitEffects, PreparedWriteFunctionBindings,
    PublicCommitExecutionOutcome, TransactionCommitOutcome, WriteExecutionContext,
};
#[cfg(test)]
pub(crate) use contracts::{CommitOutcome, TransactionDelta, TransactionJournal};
pub(crate) use deterministic_sequence::{
    deterministic_sequence_key, ensure_runtime_sequence_initialized_in_transaction,
    persist_runtime_sequence_highest_seen_in_transaction, persist_runtime_sequence_in_transaction,
};
pub(crate) use filesystem::payload_change::FilesystemPayloadChange;
#[cfg(test)]
pub(crate) use filesystem::runtime::FilesystemTransactionFileState;
pub(crate) use filesystem::runtime::{
    binary_blob_writes_from_filesystem_state,
    compile_filesystem_finalization_from_state_in_transaction,
    compile_filesystem_transaction_state_from_state,
    filesystem_transaction_state_needs_exact_descriptors, merge_filesystem_transaction_state,
    persist_filesystem_payload_changes_in_transaction, resolve_binary_blob_writes_in_transaction,
    with_exact_filesystem_descriptors, BinaryBlobWrite, ExactFilesystemDescriptorState,
    FilesystemDescriptorState, FilesystemSemanticChange, FilesystemTransactionState,
    FILESYSTEM_FILE_SCHEMA_KEY,
};
pub(crate) use filesystem::state::filesystem_transaction_state_from_planned;
#[cfg(test)]
pub(crate) use live_state_write_transaction::LiveStateWriteTransaction;
pub(crate) use overlay::{
    PendingFilesystemFileView, PendingOverlay, PendingSemanticRow, PendingWriteOverlay,
};
#[cfg(test)]
pub(crate) use overlay_read_context::OverlayReadContext;
pub(crate) use pending_overlay_public_reads::{
    build_public_read_surface_registry_with_pending_overlay, execute_pending_overlay_public_read,
    execute_pending_overlay_public_read_in_transaction,
};
pub(crate) use pipeline::resolution::prepared_artifacts::SchemaProof;
#[cfg(test)]
pub(crate) use pipeline::resolution::resolve_write_plan_with_functions;
pub(crate) use pipeline::{
    ensure_function_bindings_for_write_scope, execute_parsed_statements_in_write_transaction,
    execute_statement_batch_with_write_transaction, prepared_write_function_bindings_for_execution,
    validate_commit_time_write, TransactionWriteSelectorResolver, WriteSelectorResolver,
};
pub(crate) use prepared_artifacts::{
    PreparedDirectWriteArtifact, PreparedPublicSurfaceRegistryEffect,
    PreparedPublicSurfaceRegistryMutation, PreparedPublicWrite, PreparedPublicWriteContract,
    PreparedPublicWriteExecution, PreparedPublicWriteMaterialization,
    PreparedPublicWritePlanArtifact, PreparedResolvedWritePartition, PreparedResolvedWritePlan,
    PreparedScalarReadArtifact, PreparedWriteArtifact, PreparedWriteStatement,
};
pub(crate) use prepared_step::{stage_prepared_write_statement, PreparedWriteStatementStager};
pub(crate) use prepared_write::{WriteCommand, WritePath, WriteResult};
pub(crate) use validation_input::{UpdateValidationInput, UpdateValidationInputRow};
pub(crate) use write_batch::{
    execute_write_batch_with_transaction, PersistenceStatementSink, WriteBatch,
};
