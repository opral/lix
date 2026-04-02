//! Isolated transaction lifecycle over effective, tracked, and untracked state.

#[cfg(test)]
mod tests;

pub use crate::write_runtime::{
    CommitOutcome, ReadContext, TransactionCommitOutcome, TransactionDelta, TransactionJournal,
    WriteTransaction,
};
