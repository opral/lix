//! Rust SDK for Lix.
//!
//! The public API mirrors `@lix-js/sdk`: `open_lix()` opens the workspace
//! session, and the returned [`Lix`] handle owns the small application-facing
//! surface.

mod lix;
#[cfg(feature = "sqlite")]
mod sqlite_backend;

pub use lix::{Lix, LixTransaction, OpenLixOptions, open_lix, open_lix_with_backend};
pub use lix_engine::{
    Backend, BackendConformanceReport, BackendConformanceResult, BackendConformanceStatus,
    BackendConformanceTest, BackendError, BackendFactory, BackendFixture, BackendRangeScan,
    BackendRead, BackendTestConfig, BackendWrite, CommitResult, CoreProjection,
    CreateBranchOptions, CreateBranchReceipt, CreateBranchReceipt as CreateBranchResult,
    ExecuteResult, GetOptions, InMemoryBackend, InMemoryRangeScan, InMemoryRead, InMemoryWrite,
    Key, KeyRange, LixError, LixNotice, MergeBranchOptions, MergeBranchOutcome, MergeBranchPreview,
    MergeBranchPreviewOptions, MergeBranchReceipt, MergeBranchReceipt as MergeBranchResult,
    MergeChangeStats, MergeConflict, MergeConflictChangeKind, MergeConflictKind, MergeConflictSide,
    PointVisitor, ProjectedValueRef, PutBatch, ReadOptions, Row, ScanOptions, ScanResult,
    ScanVisitor, SqlQueryResult, StoredValue, SwitchBranchOptions, SwitchBranchReceipt,
    SwitchBranchReceipt as SwitchBranchResult, TryFromValue, Value, WriteOptions, WriteStats,
    run_backend_conformance,
};
#[cfg(feature = "sqlite")]
pub use sqlite_backend::{
    SQLITE_FORMAT_VERSION, SqliteBackend, SqliteBackendFactory, SqliteBackendFixture,
    SqliteBackendOptions,
};
