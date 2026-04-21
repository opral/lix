//! Typed access to live rows in the untracked visibility lane.

mod contracts;
mod read;
mod write;

#[cfg(test)]
pub(crate) use contracts::BatchUntrackedRowRequest;
#[cfg(test)]
pub(crate) use contracts::UntrackedReadView;
pub(crate) use contracts::{ExactUntrackedRowRequest, UntrackedRow, UntrackedScanRequest};
#[cfg(test)]
pub(crate) use read::load_exact_rows_with_backend;
pub(crate) use read::{load_exact_row_with_backend, scan_rows_with_backend};
pub(crate) use read::{load_exact_row_with_executor, scan_rows_with_executor};
pub(crate) use read::{scan_rows_with_backend_limit, scan_rows_with_executor_limit};
pub(crate) use write::apply_write_batch_in_transaction;

#[cfg(test)]
mod tests;
