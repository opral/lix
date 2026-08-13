#![recursion_limit = "256"]

#[macro_use]
mod support;

mod branching;
mod checkpoint_gc;
mod code_structure;
mod commit_graph;
mod constraint_fuzz;
mod correlated_hot_state_perf;
mod corruption_fuzz;
mod durable_function_fast_path;
mod engine;
mod execute_batch_benchmark;
mod filesystem_fuzz;
mod fs_api;
mod json_pointer_crud_storage;
mod merge_fuzz;
mod observe;
mod observe_mutation_revision;
mod physical_plan_cache;
mod pooled_session_reuse;
mod sql;
mod storage_accounting;
mod transaction;
mod version_control_model_fuzz;
