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

mod change_proposal;
mod checkpoint;
mod context;
mod create_branch;
mod execute;
mod gc;
pub(crate) mod idempotency;
mod merge;
pub(crate) mod observe;
mod switch_branch;
mod transaction;

pub use crate::common::{
    ExecuteStatementMetadata, MutationIdentity, RequestBlobSpliceProvenance, VerifiedRequestBlob,
};
pub use change_proposal::{
    AcceptChangeProposalReceipt, ChangeProposal, ChangeProposalDiff, ChangeProposalState,
    CreateChangeProposalOptions,
};
pub use checkpoint::CreateCheckpointReceipt;
pub use context::SessionContext;
pub(crate) use context::{SessionMode, load_workspace_branch_id_from_index};
pub use create_branch::{CreateBranchOptions, CreateBranchReceipt};
pub use execute::{
    CoherentReadBatch, ExecuteBatchStatement, ExecuteOptions, ExecuteResult, ExecutionDisposition,
    Row, RowRef, TryFromValue,
};
pub use idempotency::ExecuteIdempotency;
pub(crate) use idempotency::{
    EXECUTE_IDEMPOTENCY_RECEIPT_SPACE, ExecuteIdempotencyReceipt, encode_receipt,
};
pub(crate) use merge::branch_diff_from_readers;
pub use merge::{
    BranchDiff, BranchDiffChangeKind, BranchDiffEntry, BranchDiffOptions, MergeBranchOptions,
    MergeBranchOutcome, MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    MergeChangeStats, MergeConflict, MergeConflictChangeKind, MergeConflictKind, MergeConflictSide,
};
pub use observe::{ObserveEvent, ObserveEvents};
pub use switch_branch::{SwitchBranchOptions, SwitchBranchReceipt};
pub use transaction::SessionTransaction;
