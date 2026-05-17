pub mod backend;
mod binary_cas;
pub(crate) mod catalog;
pub(crate) mod cel;
pub(crate) mod commit_graph;
#[allow(dead_code, unused_imports)]
pub(crate) mod commit_store;
mod common;
pub(crate) mod domain;
pub mod engine;
pub(crate) mod entity_identity;
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
pub(crate) mod tracked_state;
pub mod transaction;
pub(crate) mod untracked_state;
pub(crate) mod version;
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
pub use common::LixError;
pub(crate) use common::{parse_row_metadata, parse_row_metadata_value, serialize_row_metadata};
pub use common::{CanonicalPluginKey, CanonicalSchemaKey, EntityId, FileId, VersionId};
pub use common::{LixNotice, NullableKeyFilter, SqlQueryResult, Value, WriteReceipt};
pub use common::{WireQueryResult, WireValue};
pub use engine::Engine;
pub use init::InitReceipt;
#[cfg(feature = "storage-benches")]
pub use session::optimization9_sql2_bench;
pub use session::{
    CreateVersionOptions, CreateVersionReceipt, MergeChangeStats, MergeConflict,
    MergeConflictChangeKind, MergeConflictKind, MergeConflictSide, MergeVersionOptions,
    MergeVersionOutcome, MergeVersionPreview, MergeVersionPreviewOptions, MergeVersionReceipt,
    SessionContext, SessionTransaction, SwitchVersionOptions, SwitchVersionReceipt,
};
pub use session::{ExecuteResult, Row, RowRef, TryFromValue};

pub(crate) const GLOBAL_VERSION_ID: &str = "global";
