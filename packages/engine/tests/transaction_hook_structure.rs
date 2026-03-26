use std::fs;
use std::path::PathBuf;

fn read_engine_source(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(relative);
    fs::read_to_string(path).expect("source file should be readable")
}

#[test]
fn sql_execution_uses_transaction_module_for_write_orchestration() {
    let source = read_engine_source("sql/execution/execution_program.rs");
    assert!(
        source.contains("use crate::transaction::{"),
        "execution_program.rs should import transaction-owned write orchestration"
    );
    assert!(
        !source.contains("sql::execution::write_txn_plan"),
        "execution_program.rs should not import SQL-owned write txn plan code"
    );
    assert!(
        !source.contains("sql::execution::write_txn_runner"),
        "execution_program.rs should not import SQL-owned write txn runner code"
    );
}

#[test]
fn sql_execution_module_no_longer_owns_write_txn_modules() {
    let source = read_engine_source("sql/execution/mod.rs");
    assert!(
        !source.contains("mod write_txn_plan"),
        "sql/execution/mod.rs should not compile a SQL-owned write txn plan module"
    );
    assert!(
        !source.contains("mod write_txn_runner"),
        "sql/execution/mod.rs should not compile a SQL-owned write txn runner module"
    );
    assert!(
        !source.contains("mod transaction_exec"),
        "sql/execution/mod.rs should not compile the removed raw transaction orchestration module"
    );
    assert!(
        !PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src/sql/execution/transaction_exec.rs")
            .exists(),
        "transaction_exec.rs should be deleted once transaction orchestration lives under transaction/"
    );
}

#[test]
fn transaction_module_uses_sql_adapter_boundary_for_sql_facing_write_paths() {
    let source = read_engine_source("transaction/mod.rs");
    assert!(
        source.contains("mod sql_adapter;"),
        "transaction/mod.rs should compile the transaction-owned sql_adapter boundary"
    );
    assert!(
        !source.contains("mod buffered_write_execution;"),
        "transaction/mod.rs should not compile the removed monolithic buffered_write_execution module"
    );
    assert!(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src/transaction/sql_adapter/mod.rs")
            .exists(),
        "transaction/sql_adapter/mod.rs should exist for the SQL-to-transaction adapter boundary"
    );
}

#[test]
fn core_transaction_files_do_not_import_sql_modules_directly() {
    let core_files = [
        "transaction/buffered_write_runner.rs",
        "transaction/buffered_write_state.rs",
        "transaction/commands.rs",
        "transaction/contracts.rs",
        "transaction/coordinator.rs",
        "transaction/execution.rs",
        "transaction/live_state_write_state.rs",
        "transaction/overlay.rs",
        "transaction/read_context.rs",
        "transaction/write_plan.rs",
        "transaction/write_runner.rs",
    ];

    for relative in core_files {
        let source = read_engine_source(relative);
        assert!(
            !source.contains("crate::sql::"),
            "{relative} should depend on SQL only through transaction/sql_adapter/*"
        );
    }
}

#[test]
fn engine_transaction_api_targets_transaction_module_not_legacy_wrapper() {
    let source = read_engine_source("engine.rs");
    assert!(
        source.contains("WriteTransaction"),
        "engine.rs should target the transaction module for the engine write boundary"
    );
    assert!(
        source.contains("WriteTransaction::new_buffered_write("),
        "engine.rs should construct buffered-write transactions through the transaction module"
    );
    assert!(
        source.contains(".commit_buffered_write("),
        "engine.rs should commit through the transaction-owned buffered-write lifecycle"
    );
    assert!(
        source.contains(".rollback_buffered_write()"),
        "engine.rs should roll back through the transaction-owned buffered-write lifecycle"
    );
    assert!(
        !PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src/transaction_legacy.rs")
            .exists(),
        "transaction_legacy.rs should be deleted once the engine-facing wrapper is folded into engine.rs"
    );
    assert!(
        !read_engine_source("lib.rs").contains("mod transaction_legacy;"),
        "lib.rs should not compile the removed transaction_legacy module"
    );
}

#[test]
fn engine_module_no_longer_owns_write_transaction_commit_choreography() {
    let source = read_engine_source("engine.rs");
    assert!(
        !source.contains("prepare_execution_context_for_write_transaction_commit"),
        "engine.rs should not own write transaction flush/finalize choreography"
    );
}

#[test]
fn sql_execution_backend_entrypoint_delegates_to_transaction_module() {
    let source = read_engine_source("sql/execution/execution_program.rs");
    assert!(
        source.contains("execute_program_with_new_write_transaction"),
        "execution_program.rs should delegate backend execution lifecycle to the transaction module"
    );
    assert!(
        !source.contains("begin_write_unit().await?"),
        "execution_program.rs should not begin backend transactions directly for the active engine write path"
    );
}

#[test]
fn execution_context_no_longer_owns_buffered_write_state() {
    let source = read_engine_source("sql/execution/execution_program.rs");
    let context_struct_region = source
        .split("impl ExecutionContext")
        .next()
        .expect("ExecutionContext impl should exist");
    assert!(
        !source.contains("buffered_write_journal"),
        "ExecutionContext should not own the buffered write journal"
    );
    assert!(
        !source.contains("pub(crate) pending_public_commit_session"),
        "ExecutionContext should not own pending public commit session state"
    );
    assert!(
        !source.contains("pending_public_commit_session: None"),
        "ExecutionContext should not own pending public commit session state"
    );
    for needle in [
        "pub(crate) active_version_changed",
        "pub(crate) installed_plugins_cache_invalidation_pending",
        "pub(crate) public_surface_registry_dirty",
        "pub(crate) pending_state_commit_stream_changes",
        "pub(crate) observe_tick_already_emitted",
    ] {
        assert!(
            !context_struct_region.contains(needle),
            "ExecutionContext should not own commit-time effect field `{needle}`"
        );
    }
}

#[test]
fn execution_program_is_a_thin_client_for_adapter_runtime() {
    let source = read_engine_source("sql/execution/execution_program.rs");
    for needle in [
        "struct CompiledExecution",
        "enum CompiledExecutionBody",
        "struct CompiledExecutionStep",
        "struct SqlExecutionOutcome",
        "fn execute_compiled_execution_step_with_transaction",
        "fn execute_internal_execution_with_transaction",
    ] {
        assert!(
            !source.contains(needle),
            "execution_program.rs should not own adapter runtime item `{needle}`"
        );
    }

    let adapter_source = read_engine_source("transaction/sql_adapter/runtime.rs");
    assert!(
        adapter_source.contains("struct CompiledExecution"),
        "transaction/sql_adapter/runtime.rs should own compiled execution runtime types"
    );
    assert!(
        adapter_source.contains("fn execute_compiled_execution_step_with_transaction"),
        "transaction/sql_adapter/runtime.rs should own compiled step execution"
    );
}

#[test]
fn pending_transaction_view_is_transaction_owned() {
    let shared_path_source = read_engine_source("sql/execution/shared_path.rs");
    assert!(
        !shared_path_source.contains("struct PendingTransactionView"),
        "shared_path.rs should not define PendingTransactionView once transaction owns pending visibility"
    );

    let transaction_mod_source = read_engine_source("transaction/mod.rs");
    assert!(
        transaction_mod_source.contains("mod pending_view;"),
        "transaction/mod.rs should compile a transaction-owned pending_view module"
    );

    let pending_view_source = read_engine_source("transaction/pending_view.rs");
    assert!(
        pending_view_source.contains("struct PendingTransactionView"),
        "transaction/pending_view.rs should own PendingTransactionView"
    );
}

#[test]
fn schema_registration_and_commit_effects_are_transaction_owned() {
    let coordinator_source = read_engine_source("transaction/coordinator.rs");
    assert!(
        coordinator_source.contains("register_schema_in_transaction("),
        "coordinator.rs should own live-state schema registration application"
    );

    for relative in [
        "transaction/sql_adapter/runtime.rs",
        "transaction/sql_adapter/planned_write_runner.rs",
    ] {
        let source = read_engine_source(relative);
        assert!(
            !source.contains("register_schema_in_transaction("),
            "{relative} should not call register_schema_in_transaction directly"
        );
    }

    let engine_source = read_engine_source("engine.rs");
    assert!(
        engine_source.contains("apply_transaction_commit_outcome"),
        "engine.rs should apply a transaction-owned commit outcome"
    );
    assert!(
        !engine_source.contains("finalize_committed_execution_context"),
        "engine.rs should not finalize commits from ExecutionContext state"
    );
}

#[test]
fn init_and_plugin_paths_use_transaction_owned_write_entrypoints() {
    let init_source = read_engine_source("init/seed.rs");
    assert!(
        init_source.contains("BorrowedWriteTransaction"),
        "init/seed.rs should route its borrowed backend transaction through a transaction-owned wrapper"
    );
    assert!(
        init_source.contains("execute_parsed_statements_in_borrowed_write_transaction"),
        "init/seed.rs should execute writes through the transaction module"
    );

    let plugin_source = read_engine_source("plugin/install.rs");
    assert!(
        plugin_source.contains("WriteTransaction::new_buffered_write("),
        "plugin/install.rs should use the transaction-owned buffered write lifecycle"
    );
    assert!(
        plugin_source.contains("execute_with_options_in_write_transaction"),
        "plugin/install.rs should execute statements through the transaction module"
    );
}

#[test]
fn internal_vtable_runtime_no_longer_uses_legacy_parallel_contracts() {
    let planned_statement_source =
        read_engine_source("sql/execution/contracts/planned_statement.rs");
    assert!(
        !planned_statement_source.contains("InternalStatePlan"),
        "planned_statement.rs should not carry InternalStatePlan"
    );
    assert!(
        !planned_statement_source.contains("internal_state"),
        "planned_statement.rs should not carry the legacy internal_state contract"
    );
    assert!(
        !planned_statement_source.contains("internal_mutation"),
        "planned_statement.rs should not carry the removed internal_mutation compatibility contract"
    );

    let internal_mod_source = read_engine_source("sql/internal/mod.rs");
    for forbidden in [
        "InternalStatePlan",
        "PostprocessPlan",
        "internal_state_plan_from_postprocess",
        "InternalMutationPlan",
    ] {
        assert!(
            !internal_mod_source.contains(forbidden),
            "sql/internal/mod.rs should not define legacy internal-vtable runtime contract `{forbidden}`"
        );
    }

    let compat_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/sql/compat");
    assert!(
        !compat_dir.exists(),
        "legacy sql/compat state-vtable normalization layer should stay removed"
    );
}

#[test]
fn transaction_runtime_uses_normal_internal_execution_not_postprocess_callbacks() {
    let runtime_source = read_engine_source("transaction/sql_adapter/runtime.rs");
    assert!(
        !runtime_source.contains("execute_internal_postprocess_with_transaction"),
        "transaction runtime should not call the removed postprocess callback path"
    );
    assert!(
        !runtime_source.contains("postprocess:"),
        "transaction runtime should not carry a postprocess field"
    );
    assert!(
        !runtime_source.contains("execute_internal_mutation_with_transaction"),
        "transaction runtime should not preserve a second internal_mutation execution path"
    );
    assert!(
        runtime_source.contains("execute_prepared_with_transaction"),
        "transaction runtime should execute internal compatibility statements through normal prepared execution"
    );

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/sql/internal");
    assert!(
        !root.join("followup.rs").exists(),
        "sql/internal/followup.rs should be removed"
    );
    assert!(
        !root.join("postprocess.rs").exists(),
        "sql/internal/postprocess.rs should be removed"
    );
    assert!(
        !root.join("mutation_plan.rs").exists(),
        "sql/internal/mutation_plan.rs should be removed once internal compatibility syntax is normalization-only"
    );
    assert!(
        !root.join("mutation_runtime.rs").exists(),
        "sql/internal/mutation_runtime.rs should be removed once internal compatibility syntax is normalization-only"
    );
}

#[test]
fn planned_write_runner_is_split_by_apply_owner() {
    let adapter_mod_source = read_engine_source("transaction/sql_adapter/mod.rs");
    assert!(
        adapter_mod_source.contains("mod tracked_apply;"),
        "transaction/sql_adapter/mod.rs should compile a tracked_apply module"
    );
    assert!(
        adapter_mod_source.contains("mod internal_apply;"),
        "transaction/sql_adapter/mod.rs should compile an internal_apply module"
    );

    let runner_source = read_engine_source("transaction/sql_adapter/planned_write_runner.rs");
    assert!(
        runner_source.contains("run_public_tracked_append_txn_with_transaction("),
        "planned_write_runner.rs should delegate tracked append apply"
    );
    assert!(
        runner_source.contains("run_internal_write_txn_with_transaction("),
        "planned_write_runner.rs should delegate internal apply"
    );
    for forbidden in [
        "append_tracked_with_pending_public_session(",
        "execute_internal_execution_with_transaction(",
        "validate_commit_time_write(",
        "persist_filesystem_payload_domain_changes_direct(",
    ] {
        assert!(
            !runner_source.contains(forbidden),
            "planned_write_runner.rs should not own `{forbidden}` after the split"
        );
    }
}
