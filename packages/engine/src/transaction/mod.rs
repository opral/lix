//! Transaction-scoped root API.
//!
//! `transaction` owns buffered write units, pending overlays, transaction-local
//! filesystem state, and backend transaction adapters. Callers outside the
//! transaction implementation should depend on `crate::transaction::{...}`
//! instead of reaching into transaction child modules directly.

mod backend;
pub(crate) mod buffered;
mod contracts;
pub(crate) mod filesystem;
#[cfg(test)]
mod live_state_write_transaction;
mod observe_tick;
pub(crate) mod overlay;
pub(crate) mod pipeline;
mod prepared_step;
mod prepared_write;
#[cfg(test)]
mod read_context;
mod unit;
mod write_batch;

pub(crate) use crate::contracts::TransactionCommitOutcome;
pub(crate) use crate::execution::WriteBatch;
pub(crate) use backend::{
    lookup_directory_id_by_path_in_transaction,
    normalize_sql_error_with_transaction_and_relation_names, TransactionExecutionBackend,
};
pub(crate) use buffered::{
    apply_schema_registrations_in_transaction, BufferedWriteCommandMetadata,
    BufferedWriteExecutionResult, BufferedWriteFlushClass, BufferedWriteSessionEffects,
    PlannedDirectWriteUnit, PlannedPublicUntrackedWriteUnit, TrackedTxnUnit, TransactionWriteDelta,
};
#[cfg(test)]
pub(crate) use contracts::{CommitOutcome, TransactionDelta, TransactionJournal};
pub(crate) use contracts::{DeferredCommitEffects, WriteExecutionContext};
#[cfg(test)]
pub(crate) use filesystem::runtime::FilesystemTransactionFileState;
pub(crate) use filesystem::runtime::{
    binary_blob_writes_from_filesystem_state, build_filesystem_payload_changes_insert,
    compile_filesystem_finalization_from_state_in_transaction,
    compile_filesystem_transaction_state_from_state,
    filesystem_transaction_state_needs_exact_descriptors, merge_filesystem_transaction_state,
    persist_filesystem_payload_changes_in_transaction, resolve_binary_blob_writes_in_transaction,
    with_exact_filesystem_descriptors, BinaryBlobWrite, ExactFilesystemDescriptorState,
    FilesystemDescriptorState, FilesystemSemanticChange, FilesystemTransactionState,
    FILESYSTEM_DESCRIPTOR_FILE_ID, FILESYSTEM_FILE_SCHEMA_KEY,
};
pub(crate) use filesystem::state::filesystem_transaction_state_from_planned;
#[cfg(test)]
pub(crate) use live_state_write_transaction::LiveStateWriteTransaction;
pub(crate) use observe_tick::append_observe_tick_in_transaction;
pub(crate) use overlay::{
    PendingFilesystemFileView, PendingOverlay, PendingSemanticRow, PendingSemanticStorage,
    PendingWriteOverlay,
};
pub(crate) use pipeline::resolution::prepared_artifacts::SchemaProof;
#[cfg(test)]
pub(crate) use pipeline::resolution::resolve_write_plan_with_functions;
pub(crate) use pipeline::{
    ensure_function_bindings_for_write_scope,
    execute_parsed_statements_in_borrowed_write_transaction,
    execute_parsed_statements_in_write_transaction, execute_statement_batch_with_write_transaction,
    prepared_write_function_bindings_for_execution, validate_commit_time_write, WriteResolveError,
    WriteSelectorResolver,
};
pub(crate) use prepared_step::{stage_prepared_write_statement, PreparedWriteStatementStager};
pub(crate) use prepared_write::{WriteCommand, WritePath, WriteResult};
#[cfg(test)]
pub(crate) use read_context::ReadContext;
pub(crate) use unit::{BorrowedBufferedWriteTransaction, BufferedWriteTransaction};
pub(crate) use write_batch::execute_write_batch_with_transaction;
