mod api;
mod backend;
mod binary_cas;
pub(crate) mod canonical;
pub(crate) mod catalog;
mod common;
pub(crate) mod contracts;
mod diagnostics;
pub(crate) mod execution;
mod init;
pub mod live_state;
mod runtime;
mod schema;
pub mod session;
pub(crate) mod sql;
#[cfg(test)]
mod test_support;

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

pub use api::BootKeyValue;
pub use api::{InitResult, Lix, LixConfig};
pub use backend::{LixBackend, LixBackendTransaction, PreparedBatch, PreparedStatement};
pub use canonical::CanonicalJson;
pub use common::LixError;
pub use common::SqlDialect;
pub use common::{
    CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId, VersionId,
};
pub use common::{ExecuteResult, QueryResult, Value};
pub use common::{WireQueryResult, WireValue};
pub use contracts::{CommittedVersionFrontier, ExecuteOptions, ReplayCursor, TransactionMode};
pub use live_state::{
    LatestVisibleWinnerDebugRow, LiveStateApplyReport, LiveStateRebuildDebugMode,
    LiveStateRebuildDebugTrace, LiveStateRebuildPlan, LiveStateRebuildReport,
    LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning, LiveStateWrite,
    LiveStateWriteOp, ScopeWinnerDebugRow, StageStat, TraversedCommitDebugRow,
    TraversedEdgeDebugRow, VersionHeadDebugRow,
};
pub use session::checkpoint_ops::CreateCheckpointResult;
pub use session::observe::{ObserveEvent, ObserveEvents, ObserveEventsOwned, ObserveQuery};
pub use session::version_ops::{
    CreateVersionOptions, CreateVersionResult, ExpectedVersionHeads, MergeOutcome,
    MergeVersionOptions, MergeVersionResult, RedoOptions, RedoResult, UndoOptions, UndoResult,
};
pub use session::{AdditionalSessionOptions, Session, SessionTransaction};
#[doc(hidden)]
pub use sql::binder::{delay_broad_binding_for_test, BroadBindingDelayForTestGuard};
pub use sql::prepare::prepared_batch::collapse_prepared_batch_for_dialect;
#[doc(hidden)]
pub use sql::prepare::public_surface::routing::{
    delay_broad_routing_for_test, BroadRoutingDelayForTestGuard,
};
