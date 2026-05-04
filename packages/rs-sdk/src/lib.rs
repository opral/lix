//! Rust SDK for Lix.
//!
//! The public API mirrors `@lix-js/sdk`: `open_lix()` opens the workspace
//! session, and the returned [`Lix`] handle owns the small application-facing
//! surface.

mod in_memory_backend;
mod lix;

pub use lix::{open_lix, Lix, OpenLixOptions};
pub use lix_engine::{
    Backend, BackendTransaction, CreateVersionOptions, CreateVersionReceipt as CreateVersionResult,
    ExecuteResult, KvPair, KvScanRange, LixError, LixNotice, MergeVersionOptions,
    MergeVersionOutcome, MergeVersionReceipt as MergeVersionResult, Row, SqlQueryResult,
    SwitchVersionOptions, SwitchVersionReceipt as SwitchVersionResult, TransactionBeginMode,
    TryFromValue, Value,
};
