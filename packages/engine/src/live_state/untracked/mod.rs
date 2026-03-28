//! Typed access to live untracked rows.

mod contracts;
mod read;
mod write;

pub use contracts::{
    BatchUntrackedRowRequest, ExactUntrackedRowRequest, UntrackedReadView, UntrackedRow,
    UntrackedScanRequest, UntrackedWriteBatch, UntrackedWriteOperation, UntrackedWriteParticipant,
    UntrackedWriteRow,
};
pub use read::{load_exact_row_with_backend, load_exact_rows_with_backend, scan_rows_with_backend};
pub(crate) use read::{load_exact_row_with_executor, scan_rows_with_executor};

#[cfg(test)]
mod tests;
