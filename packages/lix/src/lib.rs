#![recursion_limit = "256"]
// The hard-cut Rust SDK keeps implementation-only subsystems available inside
// the crate while intentionally no longer exporting them. Some are exercised
// only by feature-gated integrations and maintenance tooling.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

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
//! [`open_lix`] opens an in-memory repository. Add a storage adapter with
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
pub(crate) mod account;
mod binary_cas;
pub(crate) mod branch;
#[cfg(not(target_family = "wasm"))]
mod background_task;
pub(crate) mod catalog;
#[cfg(feature = "storage-benches")]
pub mod changelog;
#[cfg(not(feature = "storage-benches"))]
pub(crate) mod changelog;
pub(crate) mod checkpoint;
pub(crate) mod collection_generation;
pub(crate) mod columnar_row_group;
pub(crate) mod commit_graph;
mod common;
pub(crate) mod compression;
pub(crate) mod domain;
mod engine;
pub(crate) mod row_columnar;
pub(crate) mod row_pk;
pub(crate) mod filesystem;
pub(crate) mod functions;
pub(crate) mod gc;
mod handle;
#[cfg(test)]
mod hot_index_aging_probe;
#[cfg(test)]
mod hot_row_tombstone_probe;
// Calls `SessionContext::execute_profiled`, which is `storage-benches`-only, so
// the module needs the same gate. Without it `cargo check -p lix --tests` --
// cfg(test) with the feature off, which is what a plain `cargo test -p lix`
// builds -- fails while `--all-features` and clippy stay green.
#[cfg(all(test, feature = "storage-benches"))]
mod json_predicate_pushdown_probe;
pub(crate) mod hot_state;
pub(crate) mod init;
pub(crate) mod json_store;
/// The declared module layer order and the test that enforces it. Test-only:
/// it contains no engine code, just the layering artifact and its guard.
#[cfg(test)]
mod module_layers;
pub(crate) mod observe_coordinator;
pub(crate) mod observe_invalidation;
pub(crate) mod order_preserving_key;
mod prepared_dml;
// A `pub` view of `storage_spaces`, which is itself unconditional. This module
// stays gated on its own merits rather than mirroring the registry's: a build
// with neither `cfg(test)` nor `storage-benches` has no consumer for the
// handles, and publishing them there would widen the public surface for
// nothing.
#[cfg(any(test, feature = "storage-benches"))]
pub mod registered_spaces;
mod schema;
#[cfg(any(feature = "server-protocol", feature = "server-protocol-client"))]
pub mod server_protocol;
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

#[cfg(feature = "default_wasm_runtime")]
#[doc(hidden)]
pub use plugin::runtime::default::runtime as default_wasm_runtime;
pub use handle::{
    ExecuteBatchBuilder, ExecuteBuilder, Lix, LixTransaction, OpenAnotherSessionBuilder,
    OpenLixBuilder, TransactionExecuteBuilder, open_lix,
};

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
pub(crate) use prepared_dml::{PreparedDmlParameterBatch, PreparedDmlValueRef};
pub use session::{
    CreateBranchOptions, CreateBranchReceipt, CreateCheckpointReceipt, MergeBranchOptions,
    MergeBranchOutcome, MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    MergeChangeStats, MergeConflict, MergeConflictChangeKind, MergeConflictKind, MergeConflictSide,
    RedoReceipt, SessionTransaction, SwitchBranchOptions, SwitchBranchReceipt, UndoReceipt,
};
pub use session::{
    ExecuteBatchStatement, ExecuteResult, ObserveEvent, ObserveEvents, Row, RowRef, TryFromValue,
};
#[doc(hidden)]
pub use session::CoherentReadBatch;
pub(crate) use session::{
    ExecuteIdempotency, ExecuteStatementMetadata, ExecutionDisposition, FileRead,
    FileUploadProgress,
};
#[cfg(feature = "server-protocol")]
pub(crate) use session::VerifiedRequestBlob;
#[cfg(feature = "storage-benches")]
pub(crate) use sql_profile::SqlReadProfile;
pub use sql2::{SqlScriptPlan, SqlScriptStatement, parse_sql_script};
pub use storage::Memory;

/// Reserved high UUID sentinel for repository-global state.
pub const GLOBAL_BRANCH_ID: &str = "ffffffff-ffff-7fff-bfff-ffffffffffff";

/// Fixed author for engine-owned initialization and maintenance changes.
pub const SYSTEM_ACCOUNT_ID: &str = "00000000-0000-7000-8000-000000000001";

/// Fixed author used when a host opens a session without an authenticated account.
pub const ANONYMOUS_ACCOUNT_ID: &str = "00000000-0000-7000-8000-000000000002";

// Keep engine-level verification inside the crate. These tests deliberately
// exercise implementation details that are not part of the Rust SDK.
#[cfg(test)]
#[macro_use]
#[path = "../tests/integration/support/mod.rs"]
mod support;
#[cfg(test)]
#[path = "../tests/integration/main.rs"]
mod integration_tests;
}
