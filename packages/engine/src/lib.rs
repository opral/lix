//! Lix engine runtime.
//!
//! MVP transaction boundary:
//! - Engine/session APIs coordinate session lifecycle and commit boundaries.
//! - Explicit transactions serialize with implicit session writes on the same
//!   handle. The MVP does not promise multi-branch snapshot isolation across
//!   concurrent sessions beyond each storage read snapshot.
//! - `SessionContext::close()` is a lifecycle boundary. It waits for in-flight
//!   reads, rejects live explicit transactions, cancels queued or pre-boundary
//!   writes, and waits once a commit has entered the storage point-of-no-return.
//! - Crash durability is provider-defined. The MVP does not add an engine WAL,
//!   fsync policy, or recovery protocol above storage commits.
//! - `storage` and `storage_adapter` are low-level surfaces. Code that bypasses
//!   `Engine`/`SessionContext` also bypasses session lifecycle accounting and
//!   relies on storage-provided serialization.

#![cfg_attr(
    test,
    allow(
        clippy::cast_possible_truncation,
        clippy::cloned_ref_to_slice_refs,
        clippy::large_futures,
        clippy::redundant_clone,
        clippy::suspicious_operation_groupings,
        clippy::useless_vec
    )
)]

mod binary_cas;
pub(crate) mod branch;
pub(crate) mod catalog;
pub(crate) mod cel;
#[allow(unused_imports)]
pub mod changelog;
pub(crate) mod checkpoint;
pub(crate) mod collection_generation;
pub(crate) mod columnar_row_group;
pub(crate) mod commit_graph;
mod common;
pub(crate) mod compression;
pub(crate) mod domain;
pub mod engine;
pub(crate) mod entity_pk;
pub(crate) mod filesystem;
pub(crate) mod functions;
pub(crate) mod gc;
pub(crate) mod init;
pub(crate) mod json_store;
pub(crate) mod live_state;
pub(crate) mod observe_coordinator;
pub(crate) mod observe_invalidation;
pub(crate) mod plugin;
mod prepared_dml;
mod schema;
pub mod session;
pub(crate) mod sql2;
#[cfg(feature = "storage-benches")]
mod sql_profile;
mod sql_telemetry;
pub mod storage;
#[allow(unused_imports)]
pub mod storage_adapter;
#[cfg(feature = "storage-benches")]
pub mod storage_bench;
pub(crate) mod storage_codec;
pub mod telemetry;
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) mod test_support;
#[cfg(feature = "storage-benches")]
pub mod tracked_state;
#[cfg(not(feature = "storage-benches"))]
pub(crate) mod tracked_state;
#[cfg(feature = "storage-benches")]
pub mod transaction;
#[cfg(not(feature = "storage-benches"))]
pub(crate) mod transaction;
pub(crate) mod undo_redo;
pub mod wasm;

pub use schema::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};

pub use common::LixError;
pub use common::{Blob, LixNotice, NullableKeyFilter, SharedStr, SqlQueryResult, Value};
pub use common::{BranchId, CanonicalPluginKey, CanonicalSchemaKey, EntityPk, FileId};
pub use common::{LixPath, validate_lix_path_segment};
pub use common::{WireQueryResult, WireValue};
pub(crate) use common::{parse_row_metadata, parse_row_metadata_value, serialize_row_metadata};
pub use engine::{Engine, EngineOptions};
pub use init::InitReceipt;
pub use prepared_dml::{PreparedDmlParameterBatch, PreparedDmlValueRef};
pub use session::{
    CoherentReadBatch, ExecuteBatchStatement, ExecuteIdempotency, ExecuteOptions, ExecuteResult,
    ExecuteStatementMetadata, ExecutionDisposition, FILE_UPLOAD_PART_BYTES, FileRead,
    FileUploadProgress, MutationIdentity, ObserveEvent, ObserveEvents, RequestBlobSpliceProvenance,
    Row, RowRef, TryFromValue, VerifiedRequestBlob,
};
pub use session::{
    CreateBranchOptions, CreateBranchReceipt, CreateCheckpointReceipt, MergeBranchOptions,
    MergeBranchOutcome, MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    MergeChangeStats, MergeConflict, MergeConflictChangeKind, MergeConflictKind, MergeConflictSide,
    RedoReceipt, SessionContext, SessionTransaction, SwitchBranchOptions, SwitchBranchReceipt,
    UndoReceipt,
};
#[cfg(feature = "storage-benches")]
pub use sql_profile::SqlReadProfile;
pub use sql2::{SqlScriptPlan, SqlScriptStatement, parse_sql_script};
pub use storage::conformance::{
    ConformanceReport as StorageConformanceReport, ConformanceResult as StorageConformanceResult,
    ConformanceStatus as StorageConformanceStatus, ConformanceTest as StorageConformanceTest,
    StorageFactory, StorageFixture, StorageTestConfig, run_storage_conformance,
};
pub use storage::{
    CommitResult, CoreProjection, GetManyRequest, GetManyResult, GetOptions, Key, KeyRange,
    MAX_SCAN_PAGE_ROWS, Memory, MemoryFactory, MemoryFixture, MemoryRead, MemoryWrite, Prefix,
    ProjectedValue, PutBatch, PutEntry, ReadConsistency, ReadDurability, ReadEntry, ReadOptions,
    ScanChunk, ScanOptions, SnapshotRef, SpaceId, Storage, StorageError, StorageRead, StorageSpace,
    StorageWrite, StoredValue, ValueSemantics, WriteOptions, WriteStats,
};

/// Reserved high UUID sentinel for repository-global state.
pub const GLOBAL_BRANCH_ID: &str = "ffffffff-ffff-7fff-bfff-ffffffffffff";

/// Fixed author for engine-owned initialization and maintenance changes.
pub const SYSTEM_ACCOUNT_ID: &str = "00000000-0000-7000-8000-000000000001";

/// Fixed author used when a host opens a session without an authenticated account.
pub const ANONYMOUS_ACCOUNT_ID: &str = "00000000-0000-7000-8000-000000000002";
