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
pub(crate) mod live_state;
mod schema;
pub(crate) mod schema_registry;
pub mod session;
pub(crate) mod sql2;
#[cfg(test)]
pub(crate) mod test_support;
pub(crate) mod tracked_state;
pub mod transaction;
pub(crate) mod untracked_state;
pub(crate) mod version_ref;
pub mod wasm;

#[cfg(test)]
mod tests;

pub use schema::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};

pub use backend::TransactionBeginMode;
pub use backend::{KvPair, KvScanRange, LixBackend, LixBackendTransaction};
pub use common::LixError;
pub use common::{
    CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId, VersionId,
};
pub use common::{NullableKeyFilter, SqlQueryResult, Value, WriteReceipt};
pub use common::{WireQueryResult, WireValue};
pub use engine::Engine;
pub use init::InitReceipt;
pub use session::{
    CreateVersionOptions, CreateVersionReceipt, MergeVersionOptions, MergeVersionOutcome,
    MergeVersionReceipt, SessionContext, SwitchVersionOptions, SwitchVersionReceipt,
};
pub use session::{ExecuteResult, Row, RowRef, TryFromValue};

pub(crate) const GLOBAL_VERSION_ID: &str = "global";
