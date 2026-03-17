mod account;
mod api;
mod backend;
mod boot;
mod canonical_json;
mod cel;
mod deterministic_mode;
mod engine;
mod errors;
mod filesystem;
mod functions;
mod init;
mod key_value;
mod lix;
mod observe;
mod plugin;
mod schema;
pub(crate) mod sql;
pub(crate) mod state;
mod transaction;
mod types;
mod version;
mod wasm_runtime;
pub mod wire;

pub use schema::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};

pub use backend::execute_statement_with_backend;
pub use backend::LixBackend;
pub use backend::LixTransaction;
pub use backend::SqlDialect;
pub use canonical_json::CanonicalJson;
#[doc(hidden)]
pub use engine::{boot, BootArgs};
pub use engine::{BootAccount, BootKeyValue, Engine, EngineTransaction, ExecuteOptions};
pub use errors::LixError;
pub use lix::{InitResult, Lix, LixConfig};
pub use observe::{ObserveEvent, ObserveEvents, ObserveEventsOwned, ObserveQuery};
pub use sql::execution::contracts::prepared_statement::{
    collapse_prepared_batch_for_dialect, PreparedBatch, PreparedStatement,
};
pub use state::checkpoint::CreateCheckpointResult;
pub use state::image::{ImageChunkReader, ImageChunkWriter};
pub use state::materialization::{
    apply_live_state_rebuild_plan, live_state_rebuild_plan, rebuild_live_state,
    LatestVisibleWinnerDebugRow, LiveStateApplyReport, LiveStateRebuildDebugMode,
    LiveStateRebuildDebugTrace, LiveStateRebuildPlan, LiveStateRebuildReport,
    LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning, LiveStateWrite,
    LiveStateWriteOp, ScopeWinnerDebugRow, StageStat, TraversedCommitDebugRow,
    TraversedEdgeDebugRow, VersionAncestryDebugRow, VersionHeadDebugRow,
};
pub use state::stream::{
    StateCommitStream, StateCommitStreamBatch, StateCommitStreamChange, StateCommitStreamFilter,
    StateCommitStreamOperation,
};
pub use types::{ExecuteResult, QueryResult, Value};
pub use version::{CreateVersionOptions, CreateVersionResult};
pub use wasm_runtime::{NoopWasmRuntime, WasmComponentInstance, WasmLimits, WasmRuntime};
