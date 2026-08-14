#![recursion_limit = "256"]

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

pub mod plugin;

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2")))]
macro_rules! engine_surface {
    ($($item:item)*) => {
        $($item)*
    };
}

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2")))]
engine_surface! {
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
pub(crate) mod commit_graph;
mod common;
pub(crate) mod compression;
pub(crate) mod domain;
mod engine;
pub(crate) mod row_pk;
pub(crate) mod filesystem;
pub(crate) mod forktree;
pub(crate) mod functions;
pub(crate) mod gc;
mod handle;
pub(crate) mod init;
pub(crate) mod json_store;
pub(crate) mod observe_coordinator;
pub(crate) mod observe_invalidation;
#[cfg(not(target_family = "wasm"))]
mod background_task;
mod prepared_dml;
mod schema;
mod native_row;
#[cfg(feature = "server-protocol")]
pub mod server_protocol;
mod session;
pub(crate) mod sql2;
#[cfg(feature = "storage-benches")]
mod sql_profile;
mod sql_telemetry;
pub(crate) mod state;
pub mod storage;
#[cfg(feature = "storage-benches")]
pub mod storage_adapter;
#[cfg(not(feature = "storage-benches"))]
pub(crate) mod storage_adapter;
#[cfg(feature = "storage-benches")]
pub mod storage_bench;
pub(crate) mod storage_codec;
pub mod telemetry;
#[cfg(feature = "storage-benches")]
pub mod transaction;
#[cfg(not(feature = "storage-benches"))]
pub(crate) mod transaction;
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
pub use plugin::runtime::default::runtime as default_wasm_runtime;
pub use handle::{Lix, LixTransaction, OpenLixBuilder, open_lix};

pub use schema::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};

/// PostgreSQL-derived Lix Schema v1 model and validation API.
pub use lix_schema as schema_v1;

pub use common::LixError;
pub use common::{Blob, Json, LixNotice, NullableKeyFilter, SharedStr, SqlQueryResult, Value};
pub use common::{BranchId, CanonicalPluginKey, CanonicalSchemaKey, RowPk, FileId};
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
}
