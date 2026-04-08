mod api;
mod backend;
mod binary_cas;
mod boot;
mod common;
pub(crate) mod canonical;
pub(crate) mod contracts;
mod engine;
pub(crate) mod execution;
mod filesystem_payload_sql;
mod filesystem_projection_sql;
mod init;
pub mod live_state;
mod lix;
pub(crate) mod projections;
mod public_surface_source_sql;
mod runtime;
mod schema;
pub mod session;
pub(crate) mod sql;
#[cfg(test)]
mod test_support;
pub(crate) mod version_state;

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
pub use canonical::json::CanonicalJson;
pub use session::checkpoint_ops::CreateCheckpointResult;
pub use common::error::LixError;
pub use common::identity::{
    CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId, VersionId,
};
pub use common::types::{ExecuteResult, QueryResult, Value};
pub use common::wire::{WireQueryResult, WireValue};
pub use contracts::artifacts::CommittedVersionFrontier;
pub use contracts::artifacts::ExecuteOptions;
#[doc(hidden)]
pub use engine::{boot, BootArgs};
pub use engine::{BootKeyValue, Engine};
pub use live_state::{
    apply_live_state_rebuild_plan, live_state_rebuild_plan, rebuild_live_state,
    LatestVisibleWinnerDebugRow, LiveStateApplyReport, LiveStateRebuildDebugMode,
    LiveStateRebuildDebugTrace, LiveStateRebuildPlan, LiveStateRebuildReport,
    LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning, LiveStateWrite,
    LiveStateWriteOp, ScopeWinnerDebugRow, StageStat, TraversedCommitDebugRow,
    TraversedEdgeDebugRow, VersionHeadDebugRow,
};
pub use lix::{InitResult, Lix, LixConfig};
pub use contracts::ReplayCursor;
pub use session::observe::{ObserveEvent, ObserveEvents, ObserveEventsOwned, ObserveQuery};
pub use session::{OpenSessionOptions, Session, SessionTransaction};
#[doc(hidden)]
pub use sql::binder::{delay_broad_binding_for_test, BroadBindingDelayForTestGuard};
#[doc(hidden)]
pub use sql::routing::{delay_broad_routing_for_test, BroadRoutingDelayForTestGuard};
pub use sql::common::dialect::SqlDialect;
pub use contracts::transaction_mode::TransactionMode;
pub use session::version_ops::{
    CreateVersionOptions, CreateVersionResult, ExpectedVersionHeads, MergeOutcome,
    MergeVersionOptions, MergeVersionResult, RedoOptions, RedoResult, UndoOptions, UndoResult,
};
