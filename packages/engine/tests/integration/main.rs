#![recursion_limit = "256"]

#[macro_use]
mod support;

mod branching;
mod code_structure;
mod commit_graph;
mod correlated_live_state_perf;
mod durable_function_fast_path;
mod engine;
mod exact_file_read_benchmark;
mod execute_batch_benchmark;
mod fs_api;
mod json_pointer_crud_storage;
mod native_file_read_storage;
mod native_file_upsert_storage;
mod observe;
mod observe_mutation_revision;
mod plugin_registry_perf;
mod sql;
mod storage;
mod storage_accounting;
mod transaction;
