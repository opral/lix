//! Isolated transaction lifecycle over effective, tracked, and untracked state.

mod buffered_write_runner;
mod buffered_write_state;
mod commands;
mod contracts;
mod coordinator;
mod execution;
mod live_state_write_state;
mod overlay;
mod pending_view;
mod read_context;
pub(crate) mod sql_adapter;
#[cfg(test)]
mod tests;
mod write_plan;
mod write_runner;

pub use contracts::{
    CommitOutcome, TransactionCommitOutcome, TransactionDelta, TransactionJournal,
};
pub use execution::WriteTransaction;
pub(crate) use pending_view::PendingTransactionView;
pub use read_context::ReadContext;

pub(crate) use execution::BorrowedWriteTransaction;
pub(crate) use sql_adapter::{
    execute_bound_statement_template_instance_in_borrowed_write_transaction,
    execute_bound_statement_template_instance_in_write_transaction,
    execute_parsed_statements_in_borrowed_write_transaction,
    execute_parsed_statements_in_write_transaction, execute_with_options_in_write_transaction,
};
pub(crate) use write_plan::{
    PendingFilesystemOverlay, PendingRegisteredSchemaOverlay, PendingSemanticOverlay,
    PendingSemanticRow, PendingSemanticStorage, PendingWorkspaceWriterKeyOverlay,
    PlannedWriteDelta,
};
