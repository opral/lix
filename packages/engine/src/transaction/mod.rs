//! Isolated transaction lifecycle over effective, tracked, and untracked state.

mod buffered_write_execution;
mod buffered_write_state;
mod contracts;
mod coordinator;
mod execution;
mod live_state_write_state;
mod overlay;
mod read_context;
mod write_plan;
mod write_runner;

pub use contracts::{CommitOutcome, TransactionDelta, TransactionJournal};
pub use execution::WriteTransaction;
pub use read_context::ReadContext;

pub(crate) use buffered_write_execution::{
    execute_bound_statement_template_instance_in_borrowed_write_transaction,
    execute_bound_statement_template_instance_in_write_transaction,
    execute_parsed_statements_in_borrowed_write_transaction,
    execute_parsed_statements_in_write_transaction, execute_with_options_in_write_transaction,
};
pub(crate) use execution::execute_program_with_new_write_transaction;
pub(crate) use execution::BorrowedWriteTransaction;
pub(crate) use write_plan::{
    build_planned_write_delta, PendingFilesystemOverlay, PendingRegisteredSchemaOverlay,
    PendingSemanticOverlay, PendingSemanticRow, PendingSemanticStorage, PlannedWriteDelta,
};
pub(crate) use write_runner::execute_planned_write_delta;
