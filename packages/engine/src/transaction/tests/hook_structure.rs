use std::fs;
use std::path::PathBuf;

fn read_engine_source(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(relative);
    fs::read_to_string(path).expect("source file should be readable")
}

#[test]
fn sql_execution_uses_write_runtime_for_write_orchestration() {
    let compiler_source = read_engine_source("sql/prepare/execution_program.rs");
    let runtime_source = read_engine_source("write_runtime/sql_adapter/execute.rs");
    assert!(
        !compiler_source.contains("use crate::write_runtime::{"),
        "execution_program.rs should stay compiler-only once write orchestration moves to write_runtime"
    );
    assert!(
        !compiler_source.contains("use crate::transaction::{"),
        "execution_program.rs should not import transaction-owned write orchestration directly"
    );
    assert!(
        !compiler_source.contains("use crate::engine::{"),
        "execution_program.rs should not import engine-owned side-effect state directly"
    );
    assert!(
        !compiler_source.contains("sql::prepare::write_txn_plan"),
        "execution_program.rs should not import SQL-owned write txn plan code"
    );
    assert!(
        !compiler_source.contains("sql::prepare::write_txn_runner"),
        "execution_program.rs should not import SQL-owned write txn runner code"
    );
    assert!(
        runtime_source.contains("use crate::write_runtime::{"),
        "write_runtime/sql_adapter/execute.rs should own the write-runtime seam after Phase G"
    );
}

#[test]
fn sql_execution_module_no_longer_owns_write_txn_modules() {
    let source = read_engine_source("sql/prepare/mod.rs");
    assert!(
        !source.contains("mod write_txn_plan"),
        "sql/prepare/mod.rs should not compile a SQL-owned write txn plan module"
    );
    assert!(
        !source.contains("mod write_txn_runner"),
        "sql/prepare/mod.rs should not compile a SQL-owned write txn runner module"
    );
    assert!(
        !source.contains("mod transaction_exec"),
        "sql/prepare/mod.rs should not compile the removed raw transaction orchestration module"
    );
    assert!(
        !PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src/sql/prepare/transaction_exec.rs")
            .exists(),
        "transaction_exec.rs should be deleted once transaction orchestration lives under transaction/"
    );
}

#[test]
fn write_runtime_exists_as_real_owner_directory() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    assert!(
        root.join("write_runtime").is_dir(),
        "write_runtime/ should exist as a real owner directory"
    );
    assert!(
        root.join("write_runtime/mod.rs").exists(),
        "write_runtime/mod.rs should exist as the real owner module"
    );
    assert!(
        !root.join("write_runtime.rs").exists(),
        "the legacy top-level write_runtime.rs shim file should be gone after Phase A"
    );

    let lib_source = read_engine_source("lib.rs");
    assert!(
        lib_source.contains("pub(crate) mod write_runtime;"),
        "lib.rs should wire the write_runtime/ directory as the write owner"
    );
}

#[test]
fn write_runtime_owns_sql_adapter_boundary_for_sql_facing_write_paths() {
    let source = read_engine_source("transaction/mod.rs");
    assert!(
        !source.contains("crate::write_runtime::sql_adapter::"),
        "transaction/mod.rs should stop re-exporting the sql_adapter boundary once session targets write_runtime directly"
    );
    assert!(
        !source.contains("mod buffered_write_runner;"),
        "transaction/mod.rs should not compile buffered write core files once write_runtime owns them"
    );
    assert!(
        !source.contains("mod execution;"),
        "transaction/mod.rs should not compile write execution core files once write_runtime owns them"
    );
    assert!(
        !source.contains("mod overlay;"),
        "transaction/mod.rs should not compile overlay core files once write_runtime owns them"
    );

    let adapter_mod_source = read_engine_source("write_runtime/sql_adapter/mod.rs");
    assert!(
        adapter_mod_source.contains("mod tracked_apply;"),
        "write_runtime/sql_adapter/mod.rs should compile the tracked_apply adapter split"
    );
    assert!(
        adapter_mod_source.contains("mod internal_apply;"),
        "write_runtime/sql_adapter/mod.rs should compile the internal_apply adapter split"
    );
    assert!(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src/write_runtime/sql_adapter/mod.rs")
            .exists(),
        "write_runtime/sql_adapter/mod.rs should exist for the runtime-owned SQL bridge"
    );
    assert!(
        !PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src/transaction/sql_adapter/mod.rs")
            .exists(),
        "transaction/sql_adapter/mod.rs should be deleted once the adapter moves under write_runtime/"
    );
}

#[test]
fn core_write_runtime_files_do_not_import_sql_modules_directly() {
    let core_files = [
        "write_runtime/buffered/buffered_write_runner.rs",
        "write_runtime/buffered/buffered_write_state.rs",
        "write_runtime/buffered/commands.rs",
        "write_runtime/contracts.rs",
        "write_runtime/buffered/coordinator.rs",
        "write_runtime/execution.rs",
        "write_runtime/buffered/live_state_write_state.rs",
        "write_runtime/overlay/pending_write_overlay.rs",
        "write_runtime/read_context.rs",
        "write_runtime/buffered/write_plan.rs",
        "write_runtime/buffered/write_runner.rs",
    ];

    for relative in core_files {
        let source = read_engine_source(relative);
        assert!(
            !source.contains("crate::sql::"),
            "{relative} should depend on SQL only through write_runtime/sql_adapter/*"
        );
    }
}

#[test]
fn write_runtime_owns_buffered_write_core_files() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");

    for required in [
        "write_runtime/contracts.rs",
        "write_runtime/execution.rs",
        "write_runtime/read_context.rs",
        "write_runtime/buffered/buffered_write_runner.rs",
        "write_runtime/buffered/buffered_write_state.rs",
        "write_runtime/buffered/commands.rs",
        "write_runtime/buffered/coordinator.rs",
        "write_runtime/buffered/live_state_write_state.rs",
        "write_runtime/buffered/write_plan.rs",
        "write_runtime/buffered/write_runner.rs",
        "write_runtime/overlay/pending_view.rs",
        "write_runtime/overlay/pending_write_overlay.rs",
    ] {
        assert!(
            root.join(required).exists(),
            "{required} should exist under write_runtime/ after Phase B"
        );
    }

    for removed in [
        "transaction/contracts.rs",
        "transaction/execution.rs",
        "transaction/read_context.rs",
        "transaction/buffered_write_runner.rs",
        "transaction/buffered_write_state.rs",
        "transaction/commands.rs",
        "transaction/coordinator.rs",
        "transaction/live_state_write_state.rs",
        "transaction/write_plan.rs",
        "transaction/write_runner.rs",
        "transaction/pending_view.rs",
        "transaction/overlay.rs",
    ] {
        assert!(
            !root.join(removed).exists(),
            "{removed} should be deleted once write_runtime owns the buffered core"
        );
    }
}

#[test]
fn session_runtime_api_targets_write_runtime_for_write_lifecycle() {
    let source = read_engine_source("session/mod.rs");
    assert!(
        source.contains("use crate::write_runtime::"),
        "session/mod.rs should import its write lifecycle from write_runtime directly"
    );
    assert!(
        source.contains("execute_parsed_statements_in_write_transaction("),
        "session/mod.rs should execute statements through write_runtime::sql_adapter"
    );
    assert!(
        !source.contains("use crate::transaction::{"),
        "session/mod.rs should not import write lifecycle types from the transitional transaction barrel"
    );
    assert!(
        !source.contains("crate::transaction::execute_parsed_statements_in_write_transaction"),
        "session/mod.rs should not call the transitional transaction-barrel adapter entrypoint"
    );
    assert!(
        source.contains("begin_write_unit().await?"),
        "session/mod.rs should acquire backend write units through the shared engine owner"
    );
    assert!(
        source.contains("WriteTransaction::new_buffered_write("),
        "session/mod.rs should construct buffered-write transactions through write_runtime"
    );
    assert!(
        source.contains(".commit_buffered_write("),
        "session/mod.rs should commit through the write-runtime-owned buffered-write lifecycle"
    );
    assert!(
        source.contains(".rollback_buffered_write()"),
        "session/mod.rs should roll back through the write-runtime-owned buffered-write lifecycle"
    );

    let engine_source = read_engine_source("engine.rs");
    assert!(
        !engine_source.contains(".commit_buffered_write("),
        "engine.rs should not own runtime buffered-write commit choreography anymore"
    );
    assert!(
        !engine_source.contains(".rollback_buffered_write()"),
        "engine.rs should not own runtime buffered-write rollback choreography anymore"
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
fn session_owns_runtime_context_state_and_construction() {
    let engine_source = read_engine_source("engine.rs");
    let engine_runtime_region = engine_source
        .split("#[cfg(test)]")
        .next()
        .expect("engine.rs should contain a pre-test runtime region");
    for forbidden in [
        "active_version_id: RwLock<String>",
        "active_account_ids: RwLock<Vec<String>>",
        "new_execution_context(",
        "begin_transaction_with_options(",
        "apply_transaction_commit_outcome(",
    ] {
        assert!(
            !engine_runtime_region.contains(forbidden),
            "engine.rs should not own session runtime item `{forbidden}`"
        );
    }

    let session_source = read_engine_source("session/mod.rs");
    for required in [
        "active_version_id: RwLock<String>",
        "active_account_ids: RwLock<Vec<String>>",
        "pub(crate) fn new_execution_context(",
        "pub async fn begin_transaction_with_options(",
        "pub(crate) async fn apply_transaction_commit_outcome(",
    ] {
        assert!(
            session_source.contains(required),
            "session/mod.rs should own runtime item `{required}`"
        );
    }
}

#[test]
fn sql_execution_program_requires_caller_owned_write_transaction() {
    let compiler_source = read_engine_source("sql/prepare/execution_program.rs");
    let runtime_source = read_engine_source("write_runtime/sql_adapter/execute.rs");
    assert!(
        !compiler_source.contains("execute_execution_program_with_write_transaction"),
        "execution_program.rs should stop exposing write-runtime execution entrypoints after Phase G"
    );
    assert!(
        runtime_source.contains("execute_execution_program_with_write_transaction"),
        "write_runtime/sql_adapter/execute.rs should expose the caller-owned write-transaction entrypoint"
    );
    assert!(
        runtime_source.contains("execute_bound_statement_template_instance_in_write_transaction"),
        "write_runtime/sql_adapter/execute.rs should delegate bound statement execution to the transaction module"
    );
    assert!(
        !runtime_source.contains("execute_program_with_new_write_transaction"),
        "write-runtime execution should not reconstruct a separate runtime-owned write lifecycle internally"
    );
    assert!(
        !runtime_source.contains("begin_write_unit().await?"),
        "write_runtime/sql_adapter/execute.rs should not begin backend transactions directly for the session-owned runtime path"
    );

    let session_source = read_engine_source("session/mod.rs");
    assert!(
        session_source.contains("execute_execution_program_with_write_transaction"),
        "session/mod.rs should own the runtime execution choreography above sql/prepare"
    );
}

#[test]
fn execution_context_no_longer_owns_buffered_write_state() {
    let source = read_engine_source("sql/prepare/execution_program.rs");
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
    let source = read_engine_source("sql/prepare/execution_program.rs");
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

    let compiled_source = read_engine_source("sql/prepare/compiled.rs");
    assert!(
        compiled_source.contains("struct CompiledExecution"),
        "sql/prepare/compiled.rs should own neutral compiled execution types"
    );
    assert!(
        compiled_source.contains("enum CompiledExecutionBody"),
        "sql/prepare/compiled.rs should own the compiled execution body split"
    );
    assert!(
        !compiled_source.contains("crate::transaction::"),
        "sql/prepare/compiled.rs should not depend on transaction-owned contracts"
    );

    let adapter_source = read_engine_source("write_runtime/sql_adapter/runtime.rs");
    assert!(
        !adapter_source.contains("struct CompiledExecution {"),
        "write_runtime/sql_adapter/runtime.rs should not own neutral compiled execution types anymore"
    );
    assert!(
        adapter_source.contains("fn execute_compiled_execution_step_with_transaction"),
        "write_runtime/sql_adapter/runtime.rs should own compiled step execution"
    );

    let adapter_mod_source = read_engine_source("write_runtime/sql_adapter/mod.rs");
    assert!(
        !adapter_mod_source.contains("pub(crate) use crate::sql::prepare::compiled::"),
        "write_runtime/sql_adapter/mod.rs should not re-export the neutral compiled execution model"
    );
    assert!(
        !adapter_mod_source.contains("pub(crate) use crate::"),
        "write_runtime/sql_adapter/mod.rs should not serve as a crate-level compatibility barrel"
    );
}

#[test]
fn pending_transaction_view_is_write_runtime_owned() {
    let executor_compile_source = read_engine_source("sql/prepare/compile.rs");
    assert!(
        !executor_compile_source.contains("struct PendingTransactionView"),
        "executor compile ownership should not define PendingTransactionView once write_runtime owns pending visibility"
    );

    let overlay_mod_source = read_engine_source("write_runtime/overlay/mod.rs");
    assert!(
        overlay_mod_source.contains("mod pending_view;"),
        "write_runtime/overlay/mod.rs should compile the pending_view module"
    );

    let pending_view_source = read_engine_source("write_runtime/overlay/pending_view.rs");
    assert!(
        pending_view_source.contains("struct PendingTransactionView"),
        "write_runtime/overlay/pending_view.rs should own PendingTransactionView"
    );
}

#[test]
fn schema_registration_and_commit_effects_are_write_runtime_owned() {
    let coordinator_source = read_engine_source("write_runtime/buffered/coordinator.rs");
    assert!(
        coordinator_source.contains("register_schema_in_transaction("),
        "write_runtime/buffered/coordinator.rs should own live-state schema registration application"
    );

    for relative in [
        "write_runtime/sql_adapter/runtime.rs",
        "write_runtime/sql_adapter/planned_write_runner.rs",
    ] {
        let source = read_engine_source(relative);
        assert!(
            !source.contains("pub(crate) async fn register_schema_in_transaction("),
            "{relative} should not define schema registration application locally"
        );
    }

    let session_source = read_engine_source("session/mod.rs");
    assert!(
        session_source.contains("apply_transaction_commit_outcome"),
        "session/mod.rs should apply a write-runtime-owned commit outcome"
    );
    assert!(
        !session_source.contains("finalize_committed_execution_context"),
        "session/mod.rs should not finalize commits from ExecutionContext state"
    );

    let engine_source = read_engine_source("engine.rs");
    assert!(
        !engine_source.contains("apply_transaction_commit_outcome"),
        "engine.rs should not apply runtime transaction commit outcomes anymore"
    );
}

#[test]
fn commit_authoring_is_write_runtime_owned() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");

    let commit_mod_source = read_engine_source("write_runtime/commit/mod.rs");
    assert!(
        commit_mod_source.contains("pub(crate) mod create;"),
        "write_runtime/commit/mod.rs should compile commit authoring modules"
    );
    assert!(
        commit_mod_source.contains("pub(crate) use append::{"),
        "write_runtime/commit/mod.rs should re-export the write-runtime commit surface"
    );

    for required in [
        "write_runtime/commit/append.rs",
        "write_runtime/commit/create.rs",
        "write_runtime/commit/generate.rs",
        "write_runtime/commit/init.rs",
        "write_runtime/commit/pending.rs",
        "write_runtime/commit/preflight.rs",
        "write_runtime/commit/receipt.rs",
        "write_runtime/commit/types.rs",
    ] {
        assert!(
            root.join(required).exists(),
            "{required} should exist under write_runtime/commit/ after Phase C"
        );
    }

    for removed in [
        "commit/mod.rs",
        "commit/append.rs",
        "commit/create.rs",
        "commit/generate.rs",
        "commit/init.rs",
        "commit/pending.rs",
        "commit/preflight.rs",
        "commit/receipt.rs",
        "commit/types.rs",
    ] {
        assert!(
            !root.join(removed).exists(),
            "{removed} should be removed once commit authoring moves under write_runtime/commit/"
        );
    }

    let init_source = read_engine_source("init/run.rs");
    assert!(
        init_source.contains("use crate::write_runtime::commit;"),
        "init/run.rs should bootstrap commit tables through write_runtime::commit"
    );

    let merge_source = read_engine_source("version/merge_version.rs");
    assert!(
        merge_source.contains("use crate::write_runtime::commit::{"),
        "version/merge_version.rs should depend on the write-runtime commit owner"
    );

    let lib_source = read_engine_source("lib.rs");
    assert!(
        !lib_source.contains("mod commit;"),
        "lib.rs should not compile a top-level commit owner once commit lives under write_runtime/commit/"
    );
}

#[test]
fn init_and_plugin_paths_use_write_runtime_owned_write_entrypoints() {
    let init_source = read_engine_source("init/seed.rs");
    assert!(
        init_source.contains("BorrowedWriteTransaction"),
        "init/seed.rs should route its borrowed backend transaction through the write-runtime wrapper"
    );
    assert!(
        init_source.contains("execute_parsed_statements_in_borrowed_write_transaction"),
        "init/seed.rs should execute writes through write_runtime::sql_adapter"
    );

    let plugin_source = read_engine_source("runtime/plugin/install.rs");
    assert!(
        plugin_source.contains("WriteTransaction::new_buffered_write("),
        "plugin/install.rs should use the write-runtime-owned buffered write lifecycle"
    );
    assert!(
        plugin_source.contains("execute_with_options_in_write_transaction"),
        "plugin/install.rs should execute statements through write_runtime::sql_adapter"
    );
}

#[test]
fn internal_vtable_runtime_no_longer_uses_legacy_parallel_contracts() {
    let planned_statement_source = read_engine_source("sql/prepare/contracts/planned_statement.rs");
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
    let runtime_source = read_engine_source("write_runtime/sql_adapter/runtime.rs");
    assert!(
        !runtime_source.contains("execute_internal_postprocess_with_transaction"),
        "write-runtime sql adapter should not call the removed postprocess callback path"
    );
    assert!(
        !runtime_source.contains("postprocess:"),
        "write-runtime sql adapter should not carry a postprocess field"
    );
    assert!(
        !runtime_source.contains("execute_internal_mutation_with_transaction"),
        "write-runtime sql adapter should not preserve a second internal_mutation execution path"
    );
    assert!(
        runtime_source.contains("execute_prepared_with_transaction"),
        "write-runtime sql adapter should execute internal compatibility statements through normal prepared execution"
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
    let adapter_mod_source = read_engine_source("write_runtime/sql_adapter/mod.rs");
    assert!(
        adapter_mod_source.contains("mod tracked_apply;"),
        "write_runtime/sql_adapter/mod.rs should compile a tracked_apply module"
    );
    assert!(
        adapter_mod_source.contains("mod internal_apply;"),
        "write_runtime/sql_adapter/mod.rs should compile an internal_apply module"
    );

    let runner_source = read_engine_source("write_runtime/sql_adapter/planned_write_runner.rs");
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

#[test]
fn filesystem_write_resolution_lives_under_one_runtime_owner_area() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");

    assert!(
        root.join("write_runtime/resolve_write_plan/filesystem_writes.rs")
            .exists(),
        "filesystem_writes.rs should remain the runtime-owned resolver entrypoint"
    );
    assert!(
        root.join("write_runtime/resolve_write_plan/filesystem_writes/insert_planning.rs")
            .exists(),
        "insert planning should live under the filesystem_writes runtime owner area"
    );
    assert!(
        !root
            .join("write_runtime/resolve_write_plan/filesystem_insert_planning.rs")
            .exists(),
        "the standalone filesystem_insert_planning.rs owner split should be gone after Phase D"
    );

    let resolve_source = read_engine_source("write_runtime/resolve_write_plan.rs");
    assert!(
        resolve_source.contains("mod filesystem_writes;"),
        "resolve_write_plan.rs should compile the filesystem_writes runtime owner"
    );
    assert!(
        !resolve_source.contains("mod filesystem_insert_planning;"),
        "resolve_write_plan.rs should not compile a sibling filesystem_insert_planning module"
    );

    let filesystem_source =
        read_engine_source("write_runtime/resolve_write_plan/filesystem_writes.rs");
    assert!(
        filesystem_source.contains("mod insert_planning;"),
        "filesystem_writes.rs should compile its insert planning as an internal runtime submodule"
    );
}
