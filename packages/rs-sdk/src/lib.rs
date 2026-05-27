//! Rust SDK for Lix.
//!
//! The public API mirrors `@lix-js/sdk`: `open_lix()` opens the workspace
//! session, and the returned [`Lix`] handle owns the small application-facing
//! surface.

mod lix;

pub use lix::{open_lix, open_lix_with_backend, Lix, LixTransaction, OpenLixOptions};
pub use lix_engine::{
    Backend, BackendError, BackendRangeScan, BackendRead, BackendWrite, CommitResult,
    CoreProjection, CreateBranchOptions, CreateBranchReceipt as CreateBranchResult, ExecuteResult,
    GetOptions, InMemoryBackend, InMemoryRangeScan, InMemoryRead, InMemoryWrite, Key, KeyRange,
    LixError, LixNotice, MergeBranchOptions, MergeBranchOutcome, MergeBranchPreview,
    MergeBranchPreviewOptions, MergeBranchReceipt as MergeBranchResult, MergeChangeStats,
    MergeConflict, MergeConflictChangeKind, MergeConflictKind, MergeConflictSide, PointVisitor,
    ProjectedValueRef, PutBatch, ReadOptions, Row, ScanOptions, ScanResult, ScanVisitor,
    SqlQueryResult, StoredValue, SwitchBranchOptions, SwitchBranchReceipt as SwitchBranchResult,
    TryFromValue, Value, WriteOptions, WriteStats,
};
