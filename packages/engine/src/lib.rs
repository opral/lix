mod annotations;
mod api;
mod backend;
mod binary_cas;
mod boot;
pub(crate) mod canonical;
mod canonical_json;
mod change_view;
pub(crate) mod checkpoint;
pub(crate) mod contracts;
mod engine;
mod error;
mod errors;
pub(crate) mod execution_effects;
pub(crate) mod explain_output;
mod filesystem_materialization;
mod identity;
mod init;
pub(crate) mod live_schema_access;
pub mod live_state;
mod lix;
mod paths;
pub(crate) mod projections;
pub(crate) mod read_runtime;
mod replay_cursor;
mod runtime;
mod schema;
pub mod session;
pub(crate) mod sql;
#[cfg(test)]
mod test_support;
mod text;
pub mod transaction;
mod transaction_mode;
mod types;
mod version;
pub mod wire;
pub(crate) mod write_runtime;

pub mod image {
    pub use crate::backend::{ImageChunkReader, ImageChunkWriter};
}

pub mod streams {
    pub use crate::runtime::streams::{
        StateCommitStream, StateCommitStreamBatch, StateCommitStreamChange,
        StateCommitStreamFilter, StateCommitStreamOperation,
    };
}

pub mod wasm {
    pub use crate::runtime::wasm::{
        NoopWasmRuntime, WasmComponentInstance, WasmLimits, WasmRuntime,
    };
}

pub use schema::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};

pub use backend::prepared::{
    collapse_prepared_batch_for_dialect, PreparedBatch, PreparedStatement,
};
pub use backend::LixBackend;
pub use backend::LixBackendTransaction;
pub use backend::SqlDialect;
pub use canonical_json::CanonicalJson;
pub use checkpoint::CreateCheckpointResult;
pub use contracts::artifacts::CommittedVersionFrontier;
pub use contracts::artifacts::ExecuteOptions;
#[doc(hidden)]
pub use engine::{boot, BootArgs};
pub use engine::{BootKeyValue, Engine};
pub use error::LixError;
pub use identity::{
    CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId, VersionId,
};
pub use live_state::{
    apply_live_state_rebuild_plan, live_state_rebuild_plan, rebuild_live_state,
    LatestVisibleWinnerDebugRow, LiveStateApplyReport, LiveStateRebuildDebugMode,
    LiveStateRebuildDebugTrace, LiveStateRebuildPlan, LiveStateRebuildReport,
    LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning, LiveStateWrite,
    LiveStateWriteOp, ScopeWinnerDebugRow, StageStat, TraversedCommitDebugRow,
    TraversedEdgeDebugRow, VersionHeadDebugRow,
};
pub use lix::{InitResult, Lix, LixConfig};
pub use replay_cursor::ReplayCursor;
pub use session::observe::{ObserveEvent, ObserveEvents, ObserveEventsOwned, ObserveQuery};
pub use session::undo_redo::{RedoOptions, RedoResult, UndoOptions, UndoResult};
pub use session::{OpenSessionOptions, Session, SessionTransaction};
#[doc(hidden)]
pub use sql::binder::{delay_broad_binding_for_test, BroadBindingDelayForTestGuard};
#[doc(hidden)]
pub use sql::routing::{delay_broad_routing_for_test, BroadRoutingDelayForTestGuard};
pub use transaction_mode::TransactionMode;
pub use types::{ExecuteResult, QueryResult, Value};
pub use version::{
    CreateVersionOptions, CreateVersionResult, ExpectedVersionHeads, MergeOutcome,
    MergeVersionOptions, MergeVersionResult,
};
