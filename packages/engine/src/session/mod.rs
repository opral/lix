//! Engine session boundary.
//!
//! Transaction invariant: a session has one execution lease. Parent-handle
//! calls use it for implicit single-statement execution; explicit transactions
//! hold it until commit or rollback. Session feature submodules should enter
//! write flows through the centralized session helpers rather than opening
//! `Transaction` directly or using session-level read helpers inside writes.
//!
//! MVP boundary: session close can cancel queued or pre-boundary writes until
//! the durable commit point-of-no-return. After that point, close waits for
//! commit completion. Durability itself is the backend's responsibility.

mod context;
mod create_branch;
mod execute;
mod merge;
pub(crate) mod observe;
mod switch_branch;
mod transaction;

pub use context::SessionContext;
pub(crate) use context::{SessionMode, WORKSPACE_BRANCH_KEY};
pub use create_branch::{CreateBranchOptions, CreateBranchReceipt};
pub use execute::{CoherentReadBatch, ExecuteResult, Row, RowRef, TryFromValue};
pub use merge::{
    MergeBranchOptions, MergeBranchOutcome, MergeBranchPreview, MergeBranchPreviewOptions,
    MergeBranchReceipt, MergeChangeStats, MergeConflict, MergeConflictChangeKind,
    MergeConflictKind, MergeConflictSide,
};
pub use observe::{ObserveEvent, ObserveEvents};
pub use switch_branch::{SwitchBranchOptions, SwitchBranchReceipt};
pub use transaction::SessionTransaction;
