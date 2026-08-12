//! Rust SDK for Lix.
//!
//! Embedded version control for files and data.
//!
//! # Quick start
//!
//! ```no_run
//! # async fn example() -> Result<(), lix::LixError> {
//! let lix = lix::open_lix().await?;
//! # Ok(())
//! # }
//! ```
//!
//! [`open_lix`] opens an in-memory workspace. Add a storage adapter with
//! [`OpenLixBuilder::with_storage`] when persistence is needed.

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

// Let implementation modules use the same `lix::...` paths as external
// consumers now that the former engine and SDK share one crate.
extern crate self as lix;

pub(crate) mod account;
mod binary_cas;
pub(crate) mod branch;
pub(crate) mod catalog;
pub(crate) mod cel;
#[cfg(feature = "storage-benches")]
pub mod changelog;
#[cfg(not(feature = "storage-benches"))]
pub(crate) mod changelog;
pub(crate) mod checkpoint;
mod client_state;
pub(crate) mod collection_generation;
pub(crate) mod columnar_row_group;
pub(crate) mod commit_graph;
mod common;
pub(crate) mod compression;
#[cfg(feature = "default_wasm_runtime")]
mod default_wasm_runtime;
pub(crate) mod domain;
pub(crate) mod e46_probe;
mod engine;
pub(crate) mod entity_columnar;
pub(crate) mod entity_pk;
pub(crate) mod filesystem;
pub(crate) mod functions;
pub(crate) mod gc;
mod handle;
#[cfg(test)]
mod hot_index_aging_probe;
#[cfg(test)]
mod hot_row_tombstone_probe;
pub(crate) mod init;
pub(crate) mod json_store;
pub(crate) mod hot_state;
/// The declared module layer order and the test that enforces it. Test-only:
/// it contains no engine code, just the layering artifact and its guard.
#[cfg(test)]
mod module_layers;
pub(crate) mod observe_coordinator;
pub(crate) mod observe_invalidation;
pub(crate) mod order_preserving_key;
pub(crate) mod plugin;
mod plugin_arena;
mod plugin_layout;
mod plugin_wire;
mod prepared_dml;
// A `pub` view of `storage_spaces`, which is itself unconditional. This module
// stays gated on its own merits rather than mirroring the registry's: a build
// with neither `cfg(test)` nor `storage-benches` has no consumer for the
// handles, and publishing them there would widen the public surface for
// nothing.
#[cfg(any(test, feature = "storage-benches"))]
pub mod registered_spaces;
mod schema;
mod session;
pub(crate) mod sql2;
#[cfg(feature = "storage-benches")]
mod sql_profile;
mod sql_telemetry;
pub mod storage;
#[cfg(feature = "storage-benches")]
pub mod storage_adapter;
#[cfg(not(feature = "storage-benches"))]
pub(crate) mod storage_adapter;
#[cfg(feature = "storage-benches")]
pub mod storage_bench;
pub(crate) mod storage_codec;
// Unconditional: `StorageSpace::mutable`/`::immutable` check the id they are
// given against this registry, and those constructors exist in every build.
// A guard that is only compiled when a test feature is on is not a guard on
// the shipped crate.
pub(crate) mod storage_spaces;
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
pub(crate) mod transaction_types;
pub(crate) mod undo_redo;
pub mod wasm;

/// Advanced integration APIs for storage adapters and host runtimes.
///
/// Most applications should use [`open_lix`] and [`Lix`] instead. This module
/// is intentionally separate so the everyday SDK surface stays focused on a
/// workspace handle, while storage adapters can still compose with the
/// initialized engine they need to provide host behavior.
pub mod integration {
    pub use crate::engine::{Engine, EngineOptions};
    pub use crate::init::InitReceipt;
    pub use crate::session::SessionContext;
}

pub use client_state::ClientState;
#[cfg(feature = "default_wasm_runtime")]
#[doc(hidden)]
pub use default_wasm_runtime::runtime as default_wasm_runtime;
pub use handle::{Lix, LixTransaction, OpenLixBuilder, open_lix};

pub use schema::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};

pub use common::LixError;
pub use common::{Blob, Json, LixNotice, NullableKeyFilter, SharedStr, SqlQueryResult, Value};
pub use common::{BranchId, CanonicalPluginKey, CanonicalSchemaKey, EntityPk, FileId};
pub use common::{LixPath, validate_lix_path_segment};
pub use common::{WireQueryResult, WireValue};
pub(crate) use common::{parse_row_metadata, parse_row_metadata_value, serialize_row_metadata};
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
    RedoReceipt, SessionTransaction, SwitchBranchOptions, SwitchBranchReceipt, UndoReceipt,
};
#[cfg(feature = "storage-benches")]
pub use sql_profile::SqlReadProfile;
pub use sql2::{SqlScriptPlan, SqlScriptStatement, parse_sql_script};
pub use storage::Memory;

/// Reserved high UUID sentinel for repository-global state.
pub const GLOBAL_BRANCH_ID: &str = "ffffffff-ffff-7fff-bfff-ffffffffffff";

/// Fixed author for engine-owned initialization and maintenance changes.
pub const SYSTEM_ACCOUNT_ID: &str = "00000000-0000-7000-8000-000000000001";

/// Fixed author used when a host opens a session without an authenticated account.
pub const ANONYMOUS_ACCOUNT_ID: &str = "00000000-0000-7000-8000-000000000002";
