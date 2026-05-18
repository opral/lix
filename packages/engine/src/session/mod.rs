//! Engine session boundary.
//!
//! Transaction invariant: a session has one execution lease. Parent-handle
//! calls use it for implicit single-statement execution; explicit transactions
//! hold it until commit or rollback. Session APIs must not open `Transaction`
//! directly or use session-level read helpers inside write flows.

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
