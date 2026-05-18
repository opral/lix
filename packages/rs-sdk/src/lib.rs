//! Rust SDK for Lix.
//!
//! The public API mirrors `@lix-js/sdk`: `open_lix()` opens the workspace
//! session, and the returned [`Lix`] handle owns the small application-facing
//! surface.

mod in_memory_backend;
mod lix;

pub use lix::{open_lix, Lix, LixTransaction, OpenLixOptions};
pub use lix_engine::{
    Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup, BackendKvGetGroup,
    BackendKvGetRequest, BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest,
    BackendKvValueBatch, BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch,
    BackendKvWriteGroup, BackendKvWriteOp, BackendKvWriteStats, BackendReadTransaction,
    BackendWriteTransaction, BytePage, BytePageBuilder, CreateVersionOptions,
    CreateVersionReceipt as CreateVersionResult, ExecuteResult, LixError, LixNotice,
    MergeChangeStats, MergeConflict, MergeConflictChangeKind, MergeConflictKind, MergeConflictSide,
    MergeVersionOptions, MergeVersionOutcome, MergeVersionPreview, MergeVersionPreviewOptions,
    MergeVersionReceipt as MergeVersionResult, Row, SqlQueryResult, SwitchVersionOptions,
    SwitchVersionReceipt as SwitchVersionResult, TryFromValue, Value,
};
