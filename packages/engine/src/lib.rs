mod account;
mod api;
mod backend;
mod boot;
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

pub use backend::LixBackend;
pub use backend::LixTransaction;
pub use backend::SqlDialect;
#[doc(hidden)]
pub use engine::{boot, BootArgs};
pub use engine::{
    BootAccount, BootKeyValue, Engine, EngineConfig, EngineTransaction, EngineTransactionFuture,
    ExecuteOptions, OpenOrInitResult,
};
pub use errors::{ErrorCode, LixError};
pub use lix::{InitResult, Lix, LixConfig};
pub use observe::{observe_owned, ObserveEvent, ObserveEvents, ObserveEventsOwned, ObserveQuery};
pub use state::checkpoint::CreateCheckpointResult;
pub use state::commit::{
    generate_commit, ChangeRow, DomainChangeInput, GenerateCommitArgs, GenerateCommitResult,
    MaterializedStateRow, VersionInfo, VersionSnapshot,
};
pub use state::materialization::{
    apply_materialization_plan, materialization_plan, materialize, LatestVisibleWinnerDebugRow,
    MaterializationApplyReport, MaterializationDebugMode, MaterializationDebugTrace,
    MaterializationPlan, MaterializationReport, MaterializationRequest, MaterializationScope,
    MaterializationWarning, MaterializationWrite, MaterializationWriteOp, ScopeWinnerDebugRow,
    StageStat, TraversedCommitDebugRow, TraversedEdgeDebugRow, VersionAncestryDebugRow,
    VersionPointerDebugRow,
};
pub use state::snapshot::{SnapshotChunkReader, SnapshotChunkWriter};
pub use state::stream::{
    StateCommitStream, StateCommitStreamBatch, StateCommitStreamChange, StateCommitStreamFilter,
    StateCommitStreamOperation,
};
pub use types::{ExecuteResult, QueryResult, Value};
pub use version::{CreateVersionOptions, CreateVersionResult};
pub use wasm_runtime::{NoopWasmRuntime, WasmComponentInstance, WasmLimits, WasmRuntime};
pub use wire::{WireQueryResult, WireValue};
