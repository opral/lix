//! Lix engine runtime.
//!
//! MVP transaction boundary:
//! - Engine/session APIs coordinate session lifecycle and commit boundaries.
//! - Explicit transactions serialize with implicit session writes on the same
//!   handle. The MVP does not promise multi-branch snapshot isolation across
//!   concurrent sessions beyond each backend read snapshot.
//! - `SessionContext::close()` is a lifecycle boundary. It waits for in-flight
//!   reads, rejects live explicit transactions, cancels queued or pre-boundary
//!   writes, and waits once a commit has entered the durable point-of-no-return.
//! - Crash durability is delegated to the backend. The MVP does not add an
//!   engine WAL, fsync policy, or recovery protocol above backend commits.
//! - `backend` and `storage` are low-level surfaces. Code that bypasses
//!   `Engine`/`SessionContext` also bypasses session lifecycle accounting and
//!   relies on backend-provided serialization.

pub mod backend;
mod binary_cas;
pub(crate) mod branch;
pub(crate) mod catalog;
pub(crate) mod cel;
#[allow(dead_code, unused_imports)]
pub mod changelog;
pub(crate) mod commit_graph;
mod common;
pub(crate) mod domain;
pub mod engine;
pub(crate) mod entity_pk;
pub(crate) mod functions;
pub(crate) mod init;
#[allow(dead_code)]
pub(crate) mod json_store;
pub(crate) mod live_state;
mod schema;
pub mod session;
pub(crate) mod sql2;
#[allow(dead_code, unused_imports)]
pub mod storage;
#[cfg(feature = "storage-benches")]
pub mod storage_bench;
#[cfg_attr(feature = "storage-benches", allow(dead_code))]
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
pub(crate) mod untracked_state;
pub mod wasm;

pub use schema::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};

pub use backend::conformance::{
    run_backend_conformance, BackendFactory, BackendFixture, BackendTestConfig,
    ConformanceReport as BackendConformanceReport, ConformanceResult as BackendConformanceResult,
    ConformanceStatus as BackendConformanceStatus, ConformanceTest as BackendConformanceTest,
};
pub use backend::{
    get_many as backend_get_many, visit_range as backend_visit_range, Backend, BackendCapabilities,
    BackendError, BackendRangeScan, BackendRead, BackendWrite, BufferedRangeScan, CommitResult,
    CoreProjection, Durability, GetManyResult, GetOptions, InMemoryBackend, InMemoryBackendFactory,
    InMemoryBackendFixture, InMemoryRangeScan, InMemoryRead, InMemoryScanVisitResult,
    InMemoryWrite, Key, Key as BackendKey, KeyRange, KeyRef as BackendKeyRef, PointVisitor,
    Prefix as BackendPrefix, ProjectedValue, ProjectedValueRef, PutBatch, PutEntry,
    ReadConsistency, ReadEntry, ReadOptions, ScanChunk, ScanOptions, ScanResult, ScanVisitor,
    SnapshotRef, SpaceId, StoredValue, Value as BackendValue, WriteConcurrency, WriteOptions,
    WriteStats,
};
pub use common::LixError;
pub(crate) use common::{parse_row_metadata, parse_row_metadata_value, serialize_row_metadata};
pub use common::{BranchId, CanonicalPluginKey, CanonicalSchemaKey, EntityPk, FileId};
pub use common::{LixNotice, NullableKeyFilter, SqlQueryResult, Value, WriteReceipt};
pub use common::{WireQueryResult, WireValue};
pub use engine::Engine;
pub use init::InitReceipt;
pub use session::{
    CreateBranchOptions, CreateBranchReceipt, MergeBranchOptions, MergeBranchOutcome,
    MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt, MergeChangeStats,
    MergeConflict, MergeConflictChangeKind, MergeConflictKind, MergeConflictSide, SessionContext,
    SessionTransaction, SwitchBranchOptions, SwitchBranchReceipt,
};
pub use session::{ExecuteResult, Row, RowRef, TryFromValue};

pub(crate) const GLOBAL_BRANCH_ID: &str = "global";
