//! Engine session boundary.
//!
//! Transaction invariant: a session has one execution lease. Parent-handle
//! calls use it for implicit single-statement execution; explicit transactions
//! hold it until commit or rollback. Session feature submodules should enter
//! write flows through the centralized session helpers rather than opening
//! `Transaction` directly or using session-level read helpers inside writes.
//!
//! MVP boundary: session close can cancel queued or pre-boundary writes until
//! the storage commit point-of-no-return. After that point, close waits for
//! commit completion. Crash persistence is provider-defined.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

mod checkpoint;
mod context;
mod create_branch;
mod execute;
mod gc;
pub(crate) mod idempotency;
mod media_upload;
mod merge;
pub(crate) mod observe;
mod switch_branch;
mod transaction;
mod undo_redo;

pub use crate::common::{
    ExecuteStatementMetadata, MutationIdentity, RequestBlobSpliceProvenance, VerifiedRequestBlob,
};
pub use checkpoint::CreateCheckpointReceipt;
pub use context::SessionContext;
pub(crate) use context::SessionMode;
pub use create_branch::{CreateBranchOptions, CreateBranchReceipt};
pub use execute::{
    CoherentReadBatch, ExecuteBatchStatement, ExecuteOptions, ExecuteResult, ExecutionDisposition,
    FileRead, Row, RowRef, TryFromValue,
};
pub use idempotency::ExecuteIdempotency;
pub(crate) use idempotency::{
    EXECUTE_IDEMPOTENCY_RECEIPT_SPACE, ExecuteIdempotencyReceipt, encode_receipt,
};
pub use media_upload::{FILE_UPLOAD_PART_BYTES, FileUploadProgress};
pub use merge::{
    MergeBranchOptions, MergeBranchOutcome, MergeBranchPreview, MergeBranchPreviewOptions,
    MergeBranchReceipt, MergeChangeStats, MergeConflict, MergeConflictChangeKind,
    MergeConflictKind, MergeConflictSide,
};
pub use observe::{ObserveEvent, ObserveEvents};
pub use switch_branch::{SwitchBranchOptions, SwitchBranchReceipt};
pub use transaction::SessionTransaction;
pub use undo_redo::{RedoReceipt, UndoReceipt};

/// Zero-cost adapter for futures that rustc cannot prove `Send` because an
/// opaque async call contains higher-ranked references. Construction is unsafe:
/// callers must verify that every value retained across suspension is `Send`.
#[repr(transparent)]
pub(super) struct AssumeSendFuture<F>(F);

impl<F> AssumeSendFuture<F> {
    pub(super) unsafe fn new(future: F) -> Self {
        Self(future)
    }
}

// SAFETY: `AssumeSendFuture::new` is private and unsafe; each call site must
// establish the wrapped future's complete suspension state is movable.
unsafe impl<F> Send for AssumeSendFuture<F> {}

impl<F> Future for AssumeSendFuture<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: projecting through the transparent wrapper does not move F.
        unsafe { self.map_unchecked_mut(|wrapped| &mut wrapped.0) }.poll(context)
    }
}
