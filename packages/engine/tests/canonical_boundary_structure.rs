use std::fs;
use std::path::PathBuf;

fn read_engine_source(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(relative);
    fs::read_to_string(path).expect("source file should be readable")
}

#[test]
fn canonical_is_a_top_level_boundary() {
    let lib_source = read_engine_source("lib.rs");
    assert!(
        lib_source.contains("pub(crate) mod canonical;"),
        "lib.rs should compile the top-level canonical module"
    );

    let state_source = read_engine_source("state/mod.rs");
    assert!(
        !state_source.contains("mod commit;"),
        "state/mod.rs should no longer own the canonical commit implementation"
    );

    assert!(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src/canonical/mod.rs")
            .exists(),
        "src/canonical/mod.rs should exist once canonical is promoted"
    );
}

#[test]
fn shared_path_no_longer_owns_canonical_session_helpers() {
    let source = read_engine_source("sql/execution/shared_path.rs");
    for forbidden in [
        "struct PendingPublicCommitSession",
        "struct PublicCommitInvariantChecker",
        "fn build_pending_public_commit_session",
        "fn merge_public_domain_change_batch_into_pending_commit",
        "fn pending_session_matches_create_commit",
        "fn create_commit_error_to_lix_error",
    ] {
        assert!(
            !source.contains(forbidden),
            "shared_path.rs should not own canonical session helper `{forbidden}`"
        );
    }
}

#[test]
fn tracked_append_callers_use_canonical_append_entrypoint() {
    for relative in [
        "transaction/sql_adapter/tracked_apply.rs",
        "version/merge_version.rs",
        "undo_redo/undo.rs",
        "undo_redo/redo.rs",
    ] {
        let source = read_engine_source(relative);
        assert!(
            source.contains("append_tracked(")
                || source.contains("append_tracked_with_pending_public_session("),
            "{relative} should use a canonical append entrypoint"
        );
        assert!(
            !source.contains("= create_commit("),
            "{relative} should not stitch create_commit directly"
        );
    }
}

#[test]
fn legacy_internal_tracked_generation_helpers_are_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/sql/internal");
    assert!(
        !root.join("mutation_runtime.rs").exists(),
        "sql/internal/mutation_runtime.rs should be removed once internal syntax no longer owns tracked generation followups"
    );
    assert!(
        !root.join("vtable_write.rs").exists(),
        "sql/internal/vtable_write.rs should be removed once internal compatibility syntax stops owning tracked generation"
    );
}

#[test]
fn canonical_readers_and_graph_surfaces_exist_and_are_used() {
    let canonical_mod = read_engine_source("canonical/mod.rs");
    assert!(
        canonical_mod.contains("pub(crate) mod append;"),
        "canonical/mod.rs should expose canonical::append"
    );
    assert!(
        canonical_mod.contains("pub(crate) mod pending_session;"),
        "canonical/mod.rs should expose canonical::pending_session"
    );
    assert!(
        canonical_mod.contains("pub(crate) mod apply;"),
        "canonical/mod.rs should expose canonical::apply"
    );
    assert!(
        canonical_mod.contains("mod change_log;"),
        "canonical/mod.rs should compile canonical::change_log"
    );
    assert!(
        canonical_mod.contains("pub(crate) mod graph;"),
        "canonical/mod.rs should expose canonical::graph"
    );
    assert!(
        canonical_mod.contains("pub(crate) mod readers;"),
        "canonical/mod.rs should expose canonical::readers"
    );

    let effective_state_source =
        read_engine_source("sql/public/planner/semantics/effective_state_resolver.rs");
    assert!(
        effective_state_source.contains("crate::sql::public::services::state_reader::"),
        "effective_state_resolver should read committed state through the sql/public state_reader seam"
    );

    let runtime_source = read_engine_source("sql/public/runtime/mod.rs");
    assert!(
        runtime_source.contains("crate::sql::public::services::state_reader::"),
        "sql/public/runtime should use the sql/public state_reader seam for committed-state lookups"
    );

    let history_query_source = read_engine_source("state/history/query.rs");
    assert!(
        history_query_source.contains("crate::canonical::graph::"),
        "state/history/query.rs should use canonical::graph"
    );

    let merge_source = read_engine_source("version/merge_version.rs");
    assert!(
        merge_source.contains("crate::canonical::readers::"),
        "version/merge_version.rs should use canonical::readers"
    );

    let undo_source = read_engine_source("undo_redo/mod.rs");
    assert!(
        undo_source.contains("crate::canonical::readers::"),
        "undo_redo/mod.rs should use canonical::readers"
    );
}

#[test]
fn canonical_core_no_longer_imports_sql_modules_directly() {
    for relative in [
        "canonical/create_commit.rs",
        "canonical/change_log.rs",
        "canonical/apply.rs",
        "canonical/pending_session.rs",
        "canonical/graph_index.rs",
    ] {
        let source = read_engine_source(relative);
        assert!(
            !source.contains("crate::sql::"),
            "{relative} should depend on shared/canonical support modules, not crate::sql::*"
        );
    }
}

#[test]
fn sql_public_runtime_stays_on_sql_owned_write_intent() {
    let runtime_source = read_engine_source("sql/public/runtime/mod.rs");
    for forbidden in [
        "CreateCommitPreconditions",
        "CreateCommitExpectedHead",
        "CreateCommitWriteLane",
        "CreateCommitIdempotencyKey",
        "ProposedDomainChange",
        "create_commit_preconditions_for_public_write(",
    ] {
        assert!(
            !runtime_source.contains(forbidden),
            "sql/public/runtime should not construct canonical append protocol via `{forbidden}`"
        );
    }

    let domain_changes_source =
        read_engine_source("sql/public/planner/semantics/domain_changes.rs");
    assert!(
        domain_changes_source.contains("struct PublicDomainChange"),
        "domain_changes.rs should define a SQL-owned PublicDomainChange seam"
    );
}

#[test]
fn transaction_adapter_uses_applied_output_not_synthetic_generate_commit_result() {
    let runner_source = read_engine_source("transaction/sql_adapter/planned_write_runner.rs");
    let tracked_source = read_engine_source("transaction/sql_adapter/tracked_apply.rs");
    assert!(
        !runner_source.contains("GenerateCommitResult {"),
        "planned_write_runner should not rebuild synthetic GenerateCommitResult values"
    );
    assert!(
        tracked_source.contains(
            "mirror_public_registered_schema_bootstrap_rows(transaction, applied_output)"
        ),
        "tracked_apply.rs should mirror bootstrap rows from the canonical applied_output"
    );
    assert!(
        tracked_source.contains("append_tracked_with_pending_public_session("),
        "tracked_apply.rs should delegate pending-session append protocol through canonical::append"
    );
}

#[test]
fn pending_session_module_no_longer_owns_sql_specific_checker() {
    let pending_session_source = read_engine_source("canonical/pending_session.rs");
    for forbidden in [
        "struct PublicCommitInvariantChecker",
        "validate_commit_time_write(",
        "planned_write: &'a crate::sql::public::planner::ir::PlannedWrite",
        "DomainChangeBatch",
    ] {
        assert!(
            !pending_session_source.contains(forbidden),
            "canonical/pending_session.rs should not own SQL-specific helper `{forbidden}`"
        );
    }
}

#[test]
fn canonical_root_is_not_a_broad_compatibility_barrel() {
    let canonical_mod = read_engine_source("canonical/mod.rs");
    for forbidden in [
        "pub(crate) use append::append_tracked;",
        "pub(crate) use pending_session::",
        "pub(crate) use runtime::",
        "pub(crate) use graph_index::",
        "pub(crate) use state_source::",
    ] {
        assert!(
            !canonical_mod.contains(forbidden),
            "canonical/mod.rs should not re-export legacy compatibility surface `{forbidden}`"
        );
    }
}
