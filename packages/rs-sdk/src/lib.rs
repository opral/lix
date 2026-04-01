mod backend;
mod wasmtime_runtime;

pub use backend::sqlite::SqliteBackend;
pub use lix_engine::wasm::{WasmComponentInstance, WasmLimits, WasmRuntime};
pub use lix_engine::{
    collapse_prepared_batch_for_dialect, BootKeyValue, CreateCheckpointResult,
    CreateVersionOptions, CreateVersionResult, ExecuteOptions, ExecuteResult, InitResult, Lix,
    LixBackend, LixBackendTransaction, LixConfig, LixError, MergeOutcome, MergeVersionOptions,
    MergeVersionResult, ObserveEvent, ObserveEventsOwned, ObserveQuery, PreparedBatch,
    PreparedStatement, QueryResult, RedoOptions, RedoResult, SqlDialect, TransactionMode,
    UndoOptions, UndoResult, Value,
};
pub use wasmtime_runtime::WasmtimeRuntime;
