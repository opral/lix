//! Rust SDK for Lix.
//!
//! The public API mirrors `@lix-js/sdk`: `open_lix()` opens the workspace
//! session, and the returned [`Lix`] handle owns the small application-facing
//! surface.

#[cfg(feature = "default_wasm_runtime")]
mod default_wasm_runtime;
#[cfg(all(not(target_family = "wasm"), feature = "fs_backend"))]
mod filesystem;
mod lix;
#[cfg(feature = "sqlite")]
mod sqlite_backend;

#[cfg(all(not(target_family = "wasm"), feature = "fs_backend"))]
pub use filesystem::{FsBackend, FsBackendOpenOptions};
pub use lix::{Lix, LixTransaction, OpenLixOptions, open_lix, open_lix_with_backend};
pub use lix_engine::wasm::{
    WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime,
};
pub use lix_engine::{
    Backend, BackendConformanceReport, BackendConformanceResult, BackendConformanceStatus,
    BackendConformanceTest, BackendError, BackendFactory, BackendFixture, BackendRead,
    BackendTestConfig, BackendWrite, CommitResult, CoreProjection, CreateBranchOptions,
    CreateBranchReceipt, CreateBranchReceipt as CreateBranchResult, ExecuteOptions, ExecuteResult,
    GetManyResult, GetOptions, InMemoryBackend, InMemoryRead, InMemoryWrite, Key, KeyRange,
    LixError, LixNotice, MAX_SCAN_PAGE_ROWS, MergeBranchOptions, MergeBranchOutcome,
    MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    MergeBranchReceipt as MergeBranchResult, MergeChangeStats, MergeConflict,
    MergeConflictChangeKind, MergeConflictKind, MergeConflictSide, ObserveEvent, ObserveEvents,
    ProjectedValue, PutBatch, ReadEntry, ReadOptions, Row, ScanChunk, ScanOptions, SpaceId,
    SqlQueryResult, StoredValue, SwitchBranchOptions, SwitchBranchReceipt,
    SwitchBranchReceipt as SwitchBranchResult, TryFromValue, Value, WriteOptions, WriteStats,
    run_backend_conformance,
};
#[cfg(feature = "sqlite")]
pub use sqlite_backend::{
    SQLITE_FORMAT_VERSION, SqliteBackend, SqliteBackendFactory, SqliteBackendFixture,
    SqliteBackendOptions,
};
