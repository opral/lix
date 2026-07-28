//! Rust SDK for Lix.
//!
//! The public API mirrors `@lix-js/sdk`: `open_lix()` opens the workspace
//! session, and the returned [`Lix`] handle owns the small application-facing
//! surface.

#![cfg_attr(test, allow(clippy::cast_possible_truncation))]

mod client_state;
#[cfg(feature = "default_wasm_runtime")]
mod default_wasm_runtime;
#[cfg(all(not(target_family = "wasm"), feature = "local_filesystem"))]
mod filesystem;
mod lix;
#[cfg(feature = "sqlite")]
mod sqlite;

pub use client_state::ClientState;
#[cfg(all(not(target_family = "wasm"), feature = "local_filesystem"))]
pub use filesystem::{LocalFilesystem, LocalFilesystemOpenOptions};
pub use lix::{
    Lix, LixTransaction, OpenLixOptions, open_lix, open_lix_with_storage,
    open_lix_with_storage_and_plugin_v2_resource_limits, open_lix_with_telemetry,
};
pub use lix_engine::telemetry::{
    CallbackTelemetrySink, CompletedTelemetrySpan, TelemetryAttribute, TelemetrySink,
    TelemetrySpanEnd, TelemetrySpanHandle, TelemetrySpanKind, TelemetrySpanStart,
    TelemetrySpanStatus, TelemetryValue, TracingTelemetrySink,
};
/// Host-side contract for supplying a custom Component API v2 runtime through
/// [`OpenLixOptions::with_wasm_runtime`]. This is the engine/embedding boundary,
/// not a plugin-authoring SDK.
pub use lix_engine::wasm::v2::*;
pub use lix_engine::wasm::{WasmLimits, WasmRuntime};
pub use lix_engine::{
    Blob, CommitResult, CoreProjection, CreateBranchOptions, CreateBranchReceipt,
    CreateBranchReceipt as CreateBranchResult, CreateCheckpointReceipt,
    CreateCheckpointReceipt as CreateCheckpointResult, ExecuteBatchStatement, ExecuteIdempotency,
    ExecuteOptions, ExecuteResult, ExecuteStatementMetadata, ExecutionDisposition,
    GLOBAL_BRANCH_ID, GetManyRequest, GetManyResult, GetOptions, Key, KeyRange, LixError,
    LixNotice, MAX_SCAN_PAGE_ROWS, Memory, MemoryRead, MemoryWrite, MergeBranchOptions,
    MergeBranchOutcome, MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    MergeBranchReceipt as MergeBranchResult, MergeChangeStats, MergeConflict,
    MergeConflictChangeKind, MergeConflictKind, MergeConflictSide, MutationIdentity, ObserveEvent,
    ObserveEvents, ProjectedValue, PutBatch, ReadDurability, ReadEntry, ReadOptions,
    RequestBlobSpliceProvenance, Row, ScanChunk, ScanOptions, SpaceId, SqlQueryResult,
    SqlScriptPlan, SqlScriptStatement, Storage, StorageConformanceReport, StorageConformanceResult,
    StorageConformanceStatus, StorageConformanceTest, StorageError, StorageFactory, StorageFixture,
    StorageRead, StorageTestConfig, StorageWrite, StoredValue, SwitchBranchOptions,
    SwitchBranchReceipt, SwitchBranchReceipt as SwitchBranchResult, TryFromValue, Value,
    VerifiedRequestBlob, WireValue, WriteOptions, WriteStats, parse_sql_script,
    run_storage_conformance,
};
#[cfg(feature = "sqlite")]
pub use sqlite::{SQLITE_FORMAT_VERSION, SQLite, SQLiteFactory, SQLiteFixture, SQLiteOptions};
