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
pub(crate) mod media_upload;
mod merge;
pub(crate) mod observe;
mod switch_branch;
mod transaction;
mod undo_redo;

pub(crate) use media_upload::stage_reclaimable_upload_receipts;
#[cfg(feature = "storage-benches")]
pub(crate) use merge::{MergeCommitsForBench, analyze_merge_for_bench};
// Owner facade for the storage-space registry (`crate::storage_spaces`),
// which is compiled in every configuration.
pub(crate) use media_upload::{UPLOAD_MANIFEST_LEAF_SPACE, UPLOAD_STATE_SPACE};

pub(crate) use crate::common::ExecuteStatementMetadata;
#[cfg(feature = "server-protocol")]
pub(crate) use crate::common::VerifiedRequestBlob;
pub use checkpoint::CreateCheckpointReceipt;
pub(crate) use context::SessionBranch;
pub use context::SessionContext;
pub use create_branch::{CreateBranchOptions, CreateBranchReceipt};
pub use execute::{
    CoherentReadBatch, ExecuteBatchStatement, ExecuteOptions, ExecuteResult, Row, RowRef,
    TryFromValue,
};
pub(crate) use execute::{ExecutionDisposition, FileRead};
pub(crate) use idempotency::ExecuteIdempotency;
pub(crate) use idempotency::{
    EXECUTE_IDEMPOTENCY_RECEIPT_SPACE, ExecuteIdempotencyReceipt, encode_receipt,
};
pub(crate) use media_upload::FileUploadProgress;
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
pub(crate) struct AssumeSendFuture<F>(F);

impl<F> AssumeSendFuture<F> {
    pub(crate) unsafe fn new(future: F) -> Self {
        Self(future)
    }
}

// SAFETY: `AssumeSendFuture::new` is private and unsafe; each call site must
// establish the wrapped future's complete suspension state is movable. Every
// current call site is pinned by a compile-time proof; see
// `session::execute::assume_send_future_proofs`.
//
// `F::Output: Send` is not part of that obligation, but is required here so the
// wrapper can never launder a non-`Send` *result* onto another thread.
unsafe impl<F> Send for AssumeSendFuture<F>
where
    F: Future,
    F::Output: Send,
{
}

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

/// A storage adapter whose `Read<'a>` genuinely borrows `'a`.
///
/// `Memory::Read<'a> = MemoryRead` is lifetime-independent, so proofs written
/// against `Memory` alone exercise the easy case: the higher-ranked obstruction
/// that forces `AssumeSendFuture` collapses before rustc ever gets there. The
/// shipping RocksDB adapter is `Read<'a> = RocksDBRead<'a>`, and
/// `FilesystemStorage` borrows in both `Read` and `Write`. This adapter
/// reproduces that shape over `Memory`'s storage so the `Send` proofs cover the
/// configuration that actually ships.
#[cfg(test)]
pub(crate) mod borrowing_proof_storage {
    use std::future::Future;

    use crate::storage_adapter::{
        Memory, MemoryRead, MemoryWrite, Storage, StorageBeginScanOptions, StorageError,
        StorageGetManyRequest, StorageGetManyResult, StorageKeyRange, StorageRead,
        StorageReadOptions, StorageScanCursor, StorageSpace, StorageWriteOptions,
    };

    /// Read handle carrying a real `'a` borrow of the owning storage.
    pub(crate) struct BorrowingRead<'a> {
        inner: MemoryRead,
        _borrow: &'a Memory,
    }

    impl StorageRead for BorrowingRead<'_> {
        fn snapshot_cache_key(&self) -> Option<u128> {
            self.inner.snapshot_cache_key()
        }

        fn get_many(
            &self,
            requests: &[StorageGetManyRequest<'_>],
        ) -> impl Future<Output = Result<StorageGetManyResult, StorageError>> + Send {
            self.inner.get_many(requests)
        }

        fn begin_scan(
            &self,
            space: StorageSpace,
            range: StorageKeyRange,
            opts: StorageBeginScanOptions,
        ) -> impl Future<Output = Result<StorageScanCursor<'_>, StorageError>> + Send {
            self.inner.begin_scan(space, range, opts)
        }
    }

    #[derive(Clone, Default)]
    pub(crate) struct BorrowingStorage(Memory);

    impl Storage for BorrowingStorage {
        type Read<'a>
            = BorrowingRead<'a>
        where
            Self: 'a;

        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        fn begin_read(
            &self,
            opts: StorageReadOptions,
        ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
            async move {
                Ok(BorrowingRead {
                    inner: self.0.begin_read(opts).await?,
                    _borrow: &self.0,
                })
            }
        }

        fn begin_write(
            &self,
            opts: StorageWriteOptions,
        ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
            self.0.begin_write(opts)
        }
    }
}
