use std::fs;
use std::path::PathBuf;

fn read_engine_source(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(relative);
    fs::read_to_string(path).expect("source file should be readable")
}

fn read_rs_sdk_source(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("rs-sdk")
        .join("src")
        .join(relative);
    fs::read_to_string(path).expect("rs-sdk source file should be readable")
}

#[test]
fn backend_substrate_is_top_level() {
    let lib_source = read_engine_source("lib.rs");
    assert!(
        lib_source.contains("mod backend;"),
        "lib.rs should compile the backend module"
    );
    assert!(
        lib_source.contains("mod sql_support;"),
        "lib.rs should compile the sql_support module"
    );

    for relative in [
        "backend/mod.rs",
        "backend/prepared.rs",
        "backend/program.rs",
        "backend/program_runner.rs",
        "read/mod.rs",
        "read/contracts.rs",
        "read/models.rs",
        "read/runtime.rs",
        "sql_support/mod.rs",
        "sql_support/binding.rs",
        "sql_support/placeholders.rs",
        "sql_support/text.rs",
        "filesystem/runtime.rs",
        "live_state/shared/snapshot_sql.rs",
    ] {
        assert!(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("src")
                .join(relative)
                .exists(),
            "{relative} should exist after the backend isolation cut"
        );
    }
}

#[test]
fn legacy_backend_substrate_paths_are_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    for relative in [
        "backend.rs",
        "execution_support.rs",
        "sql/execution/contracts/prepared_statement.rs",
        "sql/execution/write_program_runner.rs",
        "sql/execution/runtime_effects.rs",
        "sql/ast/utils.rs",
        "sql/common/placeholders.rs",
        "sql/storage/sql_text.rs",
        "sql/live_snapshot.rs",
        "state/internal/write_program.rs",
    ] {
        assert!(
            !root.join(relative).exists(),
            "{relative} should stay removed after backend isolation"
        );
    }
}

#[test]
fn sql_module_tree_no_longer_owns_backend_substrate() {
    let execution_mod = read_engine_source("sql/execution/mod.rs");
    for forbidden in [
        "pub(crate) mod runtime_effects;",
        "pub(crate) mod write_program_runner;",
    ] {
        assert!(
            !execution_mod.contains(forbidden),
            "sql/execution/mod.rs should not compile legacy substrate `{forbidden}`"
        );
    }

    let contracts_mod = read_engine_source("sql/execution/contracts/mod.rs");
    assert!(
        !contracts_mod.contains("pub(crate) mod prepared_statement;"),
        "sql/execution/contracts/mod.rs should not compile prepared_statement.rs"
    );

    let sql_mod = read_engine_source("sql/mod.rs");
    assert!(
        !sql_mod.contains("pub(crate) mod live_snapshot;"),
        "sql/mod.rs should not own live_snapshot.rs anymore"
    );
}

#[test]
fn backend_module_stays_sql_feature_blind() {
    for relative in [
        "backend/mod.rs",
        "backend/prepared.rs",
        "backend/program.rs",
        "backend/program_runner.rs",
    ] {
        let source = read_engine_source(relative);
        for forbidden in [
            "crate::sql::execution::",
            "crate::sql::public::",
            "crate::canonical::",
            "crate::transaction::",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative} should not depend on feature-layer module `{forbidden}`"
            );
        }
    }
}

#[test]
fn canonical_no_longer_depends_on_execution_support() {
    for relative in [
        "canonical/create_commit.rs",
        "canonical/change_log.rs",
        "canonical/apply.rs",
        "canonical/pending_session.rs",
        "canonical/graph_index.rs",
    ] {
        let source = read_engine_source(relative);
        assert!(
            !source.contains("execution_support"),
            "{relative} should not depend on execution_support"
        );
    }
}

#[test]
fn prepared_types_are_reexported_from_backend() {
    let lib_source = read_engine_source("lib.rs");
    assert!(
        lib_source.contains("pub use backend::prepared::"),
        "lib.rs should re-export prepared types from backend::prepared"
    );
}

#[test]
fn backend_transaction_modes_are_explicit_and_session_routing_uses_read_runtime() {
    let backend_source = read_engine_source("backend/mod.rs");
    assert!(
        backend_source.contains("pub enum TransactionMode"),
        "backend transaction modes should be explicit"
    );
    assert!(
        backend_source.contains("mode: TransactionMode"),
        "backend begin_transaction should require an explicit mode"
    );
    assert!(
        backend_source.contains("fn mode(&self) -> TransactionMode;"),
        "backend transactions should expose their chosen mode"
    );

    let session_source = read_engine_source("session/mod.rs");
    assert!(
        session_source.contains("begin_read_unit(transaction_mode)"),
        "session execution should have a committed read path"
    );
    assert!(
        session_source.contains("execute_execution_program_in_committed_read_transaction"),
        "session execution should route committed reads through read/runtime"
    );
}

#[test]
fn read_subsystem_owns_committed_read_runtime_and_models() {
    let runtime_read_source = read_engine_source("sql/public/runtime/read.rs");
    assert!(
        runtime_read_source.contains("crate::read::models::"),
        "sql/public/runtime/read.rs should consume committed read models through read/models"
    );

    let runtime_mod_source = read_engine_source("sql/public/runtime/mod.rs");
    assert!(
        runtime_mod_source.contains("crate::read::models::"),
        "sql/public/runtime/mod.rs should consume committed read models through read/models"
    );

    let transaction_runtime_source = read_engine_source("transaction/sql_adapter/runtime.rs");
    assert!(
        transaction_runtime_source
            .contains("execute_prepared_public_read_with_pending_transaction_view"),
        "pending-view reads should remain transaction-owned"
    );

    let read_runtime_source = read_engine_source("read/runtime.rs");
    assert!(
        !read_runtime_source.contains("execute_compiled_execution_step_with_transaction"),
        "committed read runtime should not delegate back into transaction runtime execution"
    );
    assert!(
        !read_runtime_source.contains("TransactionMode::Deferred"),
        "committed read runtime should not rely on deferred fallback for public reads"
    );
}

#[test]
fn sqlite_backend_keeps_nested_mode_failures_explicit() {
    let sqlite_backend_source = read_rs_sdk_source("backend/sqlite.rs");
    assert!(
        sqlite_backend_source.contains("cannot open a nested read/deferred transaction"),
        "sqlite backend should reject nested read/deferred mode requests explicitly"
    );
}
