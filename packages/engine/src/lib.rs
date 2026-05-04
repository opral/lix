mod backend;
mod binary_cas;
pub(crate) mod cel;
pub(crate) mod changelog;
pub(crate) mod commit_graph;
mod common;
pub mod engine;
pub(crate) mod entity_identity;
pub(crate) mod functions;
pub(crate) mod init;
#[allow(dead_code)]
pub(crate) mod json_store;
pub(crate) mod live_state;
mod schema;
pub(crate) mod schema_registry;
pub mod session;
pub(crate) mod sql2;
#[allow(dead_code, unused_imports)]
pub(crate) mod storage;
#[cfg(feature = "storage-benches")]
pub mod storage_bench;
#[cfg(test)]
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

pub use backend::{
    Backend, BackendKvGetBatch, BackendKvGetBatchGroup, BackendKvGetEntry, BackendKvGetGroup,
    BackendKvGetProjection, BackendKvGetRequest, BackendKvPut, BackendKvScanBatch,
    BackendKvScanProjection, BackendKvScanRange, BackendKvScanRequest, BackendKvScanRow,
    BackendKvWriteBatch, BackendKvWriteGroup, BackendKvWriteStats, BackendReadTransaction,
    BackendWriteTransaction,
};
pub use common::LixError;
pub(crate) use common::{
    parse_row_metadata, serialize_row_metadata, validate_row_metadata, RowMetadata,
};
pub use common::{
    CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId, VersionId,
};
pub use common::{LixNotice, NullableKeyFilter, SqlQueryResult, Value, WriteReceipt};
pub use common::{WireQueryResult, WireValue};
pub use engine::Engine;
pub use init::InitReceipt;
pub use session::{
    CreateVersionOptions, CreateVersionReceipt, MergeVersionOptions, MergeVersionOutcome,
    MergeVersionReceipt, SessionContext, SwitchVersionOptions, SwitchVersionReceipt,
};
pub use session::{ExecuteResult, Row, RowRef, TryFromValue};

pub(crate) const GLOBAL_VERSION_ID: &str = "global";
