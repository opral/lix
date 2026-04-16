//! Rust SDK for Lix.
//!
//! # Errors and hints
//!
//! Every fallible call returns [`LixError`], a structured error carrying a
//! machine-readable `code`, a human-readable `description`, and an optional
//! `hint` suggesting how to recover. The hint is intended to be rendered
//! alongside the primary message — CLIs typically print it as
//! `hint: <text>`; a UI might show it as secondary text.
//!
//! ```
//! use lix_rs_sdk::LixError;
//!
//! let err = LixError::new("LIX_ERROR_FOO", "something went wrong")
//!     .with_hint("try the fix");
//!
//! assert_eq!(err.code, "LIX_ERROR_FOO");
//! assert_eq!(err.hint(), Some("try the fix"));
//! ```

mod backend;
mod wasmtime_runtime;

pub use backend::sqlite::SqliteBackend;
pub use lix_engine::wasm::{WasmComponentInstance, WasmLimits, WasmRuntime};
pub use lix_engine::{
    collapse_prepared_batch_for_dialect, BootKeyValue, CreateCheckpointResult,
    CreateVersionOptions, CreateVersionResult, ExecuteOptions, ExecuteResult, InitResult, Lix,
    LixBackend, LixBackendTransaction, LixConfig, LixError, MergeOutcome, MergeVersionOptions,
    MergeVersionResult, ObserveEvent, ObserveEventsOwned, ObserveQuery, PreparedBatch,
    PreparedStatement, QueryResult, RedoOptions, RedoResult, SqlDialect, TransactionBeginMode,
    UndoOptions, UndoResult, Value,
};
pub use wasmtime_runtime::WasmtimeRuntime;
