//! Rust SDK for Lix.
//!
//! The public API mirrors `@lix-js/sdk`: `open_lix()` opens the workspace
//! session, and the returned [`Lix`] handle owns the small application-facing
//! surface.

#[cfg(feature = "default_wasm_runtime")]
mod default_wasm_runtime;
#[cfg(all(not(target_family = "wasm"), feature = "local_filesystem"))]
mod filesystem;
mod lix;
#[cfg(feature = "sqlite")]
mod sqlite;

#[cfg(all(not(target_family = "wasm"), feature = "local_filesystem"))]
pub use filesystem::{LocalFilesystem, LocalFilesystemOpenOptions};
pub use lix::{
    Lix, LixTransaction, OpenLixOptions, open_lix, open_lix_with_storage, open_lix_with_telemetry,
};
pub use lix_engine::telemetry::{
    CallbackTelemetrySink, CompletedTelemetrySpan, TelemetryAttribute, TelemetrySink,
    TelemetrySpanEnd, TelemetrySpanHandle, TelemetrySpanKind, TelemetrySpanStart,
    TelemetrySpanStatus, TelemetryValue, TracingTelemetrySink,
};
pub use lix_engine::wasm::v2::{WasmComponentV2Factory, WasmTransitionCounters};
pub use lix_engine::wasm::{
    WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime,
};
pub use lix_engine::{
    CommitResult, CoreProjection, CreateBranchOptions, CreateBranchReceipt,
    CreateBranchReceipt as CreateBranchResult, ExecuteBatchStatement, ExecuteOptions,
    ExecuteResult, ExecuteStatementMetadata, GetManyResult, GetOptions, Key, KeyRange, LixError,
    LixNotice, MAX_SCAN_PAGE_ROWS, Memory, MemoryRead, MemoryWrite, MergeBranchOptions,
    MergeBranchOutcome, MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    MergeBranchReceipt as MergeBranchResult, MergeChangeStats, MergeConflict,
    MergeConflictChangeKind, MergeConflictKind, MergeConflictSide, MutationIdentity, ObserveEvent,
    ObserveEvents, ProjectedValue, PutBatch, ReadEntry, ReadOptions, RequestBlobSpliceProvenance,
    Row, ScanChunk, ScanOptions, SpaceId, SqlQueryResult, SqlScriptPlan, SqlScriptStatement,
    Storage, StorageConformanceReport, StorageConformanceResult, StorageConformanceStatus,
    StorageConformanceTest, StorageError, StorageFactory, StorageFixture, StorageRead,
    StorageTestConfig, StorageWrite, StoredValue, SwitchBranchOptions, SwitchBranchReceipt,
    SwitchBranchReceipt as SwitchBranchResult, TryFromValue, Value, WireValue, WriteOptions,
    WriteStats, parse_sql_script, run_storage_conformance,
};

/// Returns the SDK's Wasmtime runtime for the large-file profiling harness.
///
/// This deliberately exists only behind the non-default internal
/// `__profile_wasm_memory`
/// feature. The harness wraps it to sweep diagnostic memory ceilings without
/// changing the production 64 MiB policy.
#[cfg(all(feature = "default_wasm_runtime", feature = "__profile_wasm_memory"))]
#[doc(hidden)]
pub fn profiling_default_wasm_runtime() -> Result<std::sync::Arc<dyn WasmRuntime>, LixError> {
    default_wasm_runtime::runtime()
}
#[cfg(feature = "sqlite")]
pub use sqlite::{SQLITE_FORMAT_VERSION, SQLite, SQLiteFactory, SQLiteFixture, SQLiteOptions};
