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

fn read_repo_source(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(relative);
    fs::read_to_string(path).expect("repo source file should be readable")
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
        session_source.contains("prepare_committed_read_program")
            && session_source.contains("begin_read_unit(prepared_committed_read.transaction_mode)"),
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
        !read_runtime_source.contains("crate::transaction::sql_adapter::"),
        "committed read runtime should not import transaction-owned compiled/runtime helpers"
    );
    assert!(
        read_runtime_source.contains("committed_read_mode_from_prepared_public_read"),
        "committed read runtime should derive its routing mode from committed read classification"
    );

    let shared_path_source = read_engine_source("sql/execution/shared_path.rs");
    assert!(
        !shared_path_source.contains("crate::transaction::sql_adapter::"),
        "shared_path.rs should not depend on transaction-owned compiled execution types"
    );

    let read_models_source = read_engine_source("read/models.rs");
    for forbidden in [
        "crate::change_view::",
        "crate::filesystem::history::",
        "crate::state::history::",
    ] {
        assert!(
            !read_models_source.contains(forbidden),
            "read/models.rs should own committed read models instead of forwarding to `{forbidden}`"
        );
    }

    let filesystem_history_source = read_engine_source("filesystem/history/mod.rs");
    assert!(
        filesystem_history_source.contains("crate::read::models::filesystem_history::"),
        "filesystem/history/mod.rs should consume the read-owned filesystem history model"
    );
}

#[test]
fn sqlite_backend_keeps_nested_mode_failures_explicit() {
    let sqlite_backend_source = read_rs_sdk_source("backend/sqlite.rs");
    assert!(
        sqlite_backend_source.contains("cannot open a nested read/deferred transaction"),
        "sqlite backend should reject nested read/deferred mode requests explicitly"
    );
    assert!(
        sqlite_backend_source.contains("cannot open a nested write transaction"),
        "sqlite backend should reject nested write mode requests explicitly"
    );
    assert!(
        !sqlite_backend_source.contains("sp_auto_"),
        "sqlite backend should not synthesize implicit nested savepoints from begin_transaction(...)"
    );
}

#[test]
fn init_no_longer_bootstraps_legacy_timeline_tables() {
    let init_tables_source = read_engine_source("init/tables.rs");
    assert!(
        !init_tables_source.contains("lix_internal_entity_state_timeline_breakpoint"),
        "init/tables.rs should not create the removed timeline breakpoint table"
    );
    assert!(
        !init_tables_source.contains("lix_internal_timeline_status"),
        "init/tables.rs should not create the removed timeline status table"
    );
}

#[test]
fn backend_surface_only_exposes_explicit_mode_helpers() {
    let backend_source = read_engine_source("backend/mod.rs");
    assert!(
        !backend_source.contains("execute_auto_transactional"),
        "backend/mod.rs should not expose the legacy implicit deferred helper"
    );
    assert!(
        !backend_source.contains("execute_statement_with_backend"),
        "backend/mod.rs should not expose the legacy implicit statement helper"
    );
    assert!(
        !backend_source.contains("execute_with_transaction_mode"),
        "backend/mod.rs should not expose the legacy public transaction wrapper helper"
    );
    assert!(
        !backend_source.contains("execute_statement_with_transaction_mode"),
        "backend/mod.rs should not expose the legacy prepared-statement wrapper helper"
    );

    let lib_source = read_engine_source("lib.rs");
    assert!(
        !lib_source.contains("execute_auto_transactional"),
        "lib.rs should not re-export the legacy implicit deferred helper"
    );
    assert!(
        !lib_source.contains("execute_statement_with_backend"),
        "lib.rs should not re-export the legacy implicit statement helper"
    );
    assert!(
        !lib_source.contains("execute_with_transaction_mode"),
        "lib.rs should not re-export the legacy public transaction wrapper helper"
    );
    assert!(
        !lib_source.contains("execute_statement_with_transaction_mode"),
        "lib.rs should not re-export the legacy prepared-statement wrapper helper"
    );
}

#[test]
fn repo_wide_backend_wrappers_are_mode_aware() {
    for relative in [
        "packages/cli/src/commands/exp/git_replay.rs",
        "packages/engine/benches/lix_file_recursive_update.rs",
        "packages/engine/benches/lix_file_update.rs",
        "packages/engine/benches/lix_file_insert_history.rs",
        "packages/engine/benches/support/sqlite_backend.rs",
    ] {
        let source = read_repo_source(relative);
        assert!(
            !source.contains("async fn begin_transaction(&self)"),
            "{relative} should not expose zero-argument begin_transaction()"
        );
        assert!(
            source.contains("mode: TransactionMode")
                || source.contains("mode: lix_engine::TransactionMode"),
            "{relative} should thread explicit transaction modes"
        );
    }
}
