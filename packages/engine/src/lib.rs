mod account;
mod backend;
mod boot;
mod cel;
mod deterministic_mode;
mod engine;
mod error;
mod error_classification;
mod errors;
mod filesystem;
mod functions;
mod init;
mod key_value;
mod observe;
mod plugin;
#[path = "sql/execution/runtime_post_commit.rs"]
mod runtime_post_commit;
#[path = "sql/execution/runtime_sql_effects.rs"]
mod runtime_sql_effects;
mod schema;
pub(crate) mod sql;
pub(crate) mod state;
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
pub use engine::{
    boot, init_lix, BootAccount, BootArgs, BootKeyValue, Engine, EngineTransaction,
    EngineTransactionFuture, ExecuteOptions, InitLixArgs, InitLixResult,
};
pub use error::LixError;
pub use errors::ErrorCode;
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
