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
