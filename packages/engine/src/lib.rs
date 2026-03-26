mod account;
mod api;
mod backend;
mod boot;
pub(crate) mod canonical;
mod canonical_json;
mod cel;
mod change_view;
mod deterministic_mode;
mod engine;
mod errors;
mod filesystem;
mod functions;
mod identity;
mod init;
mod key_value;
pub mod live_state;
mod lix;
mod observe;
mod plugin;
mod schema;
pub mod session;
pub(crate) mod sql;
mod sql_support;
pub(crate) mod state;
#[cfg(test)]
mod test_support;
pub mod transaction;
mod types;
mod undo_redo;
mod version;
mod wasm_runtime;
pub mod wire;

pub use schema::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};

pub use backend::execute_auto_transactional;
pub use backend::execute_statement_with_backend;
pub use backend::prepared::{
    collapse_prepared_batch_for_dialect, PreparedBatch, PreparedStatement,
};
pub use backend::LixBackend;
pub use backend::LixBackendTransaction;
pub use backend::SqlDialect;
pub use canonical_json::CanonicalJson;
#[doc(hidden)]
pub use engine::{boot, BootArgs};
pub use engine::{BootAccount, BootKeyValue, Engine, ExecuteOptions};
pub use errors::LixError;
pub use identity::{
    CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId, VersionId,
};
pub use live_state::{
    apply_live_state_rebuild_plan, live_state_rebuild_plan, rebuild_live_state,
    LatestVisibleWinnerDebugRow, LiveStateApplyReport, LiveStateRebuildDebugMode,
    LiveStateRebuildDebugTrace, LiveStateRebuildPlan, LiveStateRebuildReport,
    LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning, LiveStateWrite,
    LiveStateWriteOp, ScopeWinnerDebugRow, StageStat, TraversedCommitDebugRow,
    TraversedEdgeDebugRow, VersionAncestryDebugRow, VersionHeadDebugRow,
};
pub use lix::{InitResult, Lix, LixConfig};
pub use observe::{ObserveEvent, ObserveEvents, ObserveEventsOwned, ObserveQuery};
pub use session::{OpenSessionOptions, Session, SessionTransaction};
pub use state::checkpoint::CreateCheckpointResult;
pub use state::image::{ImageChunkReader, ImageChunkWriter};
pub use state::stream::{
    StateCommitStream, StateCommitStreamBatch, StateCommitStreamChange, StateCommitStreamFilter,
    StateCommitStreamOperation,
};
pub use types::{ExecuteResult, QueryResult, Value};
pub use undo_redo::{RedoOptions, RedoResult, UndoOptions, UndoResult};
pub use version::{
    CreateVersionOptions, CreateVersionResult, ExpectedVersionHeads, MergeOutcome,
    MergeVersionOptions, MergeVersionResult,
};
pub use wasm_runtime::{NoopWasmRuntime, WasmComponentInstance, WasmLimits, WasmRuntime};
