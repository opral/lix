//! Rust SDK for Lix.
//!
//! The public API mirrors `@lix-js/sdk`: `open_lix()` opens the workspace
//! session, and the returned [`Lix`] handle owns the small application-facing
//! surface.

mod lix;

pub use lix::{open_lix, open_lix_with_backend, Lix, LixTransaction, OpenLixOptions};
pub use lix_engine::{
    Backend, BackendCapabilities, BackendError, BackendRangeScan, BackendRead, BackendWrite,
    CommitResult, CoreProjection, CreateVersionOptions,
    CreateVersionReceipt as CreateVersionResult, DurableWriteGuard, DurableWriteLock,
    ExecuteResult, GetOptions, InMemoryBackend, InMemoryRangeScan, InMemoryRead, InMemoryWrite,
    Key, KeyRange, LixError, LixNotice, MergeChangeStats, MergeConflict, MergeConflictChangeKind,
    MergeConflictKind, MergeConflictSide, MergeVersionOptions, MergeVersionOutcome,
    MergeVersionPreview, MergeVersionPreviewOptions, MergeVersionReceipt as MergeVersionResult,
    PointVisitor, ProjectedValueRef, PutBatch, ReadOptions, Row, ScanOptions, ScanResult,
    ScanVisitor, SqlQueryResult, StoredValue, SwitchVersionOptions,
    SwitchVersionReceipt as SwitchVersionResult, TryFromValue, Value, WriteConcurrency,
    WriteOptions, WriteStats,
};
