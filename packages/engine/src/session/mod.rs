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
mod create_version;
mod execute;
mod merge;
mod switch_version;
mod transaction;

pub use context::SessionContext;
pub(crate) use context::{SessionMode, WORKSPACE_VERSION_KEY};
pub use create_version::{CreateVersionOptions, CreateVersionReceipt};
pub use execute::{ExecuteResult, Row, RowRef, TryFromValue};
pub use merge::{
    MergeChangeStats, MergeConflict, MergeConflictChangeKind, MergeConflictKind, MergeConflictSide,
    MergeVersionOptions, MergeVersionOutcome, MergeVersionPreview, MergeVersionPreviewOptions,
    MergeVersionReceipt,
};
pub use switch_version::{SwitchVersionOptions, SwitchVersionReceipt};
pub use transaction::SessionTransaction;
