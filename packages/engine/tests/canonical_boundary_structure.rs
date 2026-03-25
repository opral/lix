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
        "transaction/sql_adapter/planned_write_runner.rs",
        "version/merge_version.rs",
        "undo_redo/undo.rs",
        "undo_redo/redo.rs",
    ] {
        let source = read_engine_source(relative);
        assert!(
            source.contains("append_tracked("),
            "{relative} should use the canonical append_tracked entrypoint"
        );
        assert!(
            !source.contains("= create_commit("),
            "{relative} should not stitch create_commit directly"
        );
    }
}

#[test]
fn internal_tracked_generation_delegates_into_canonical_helpers() {
    let followup_source = read_engine_source("state/internal/followup.rs");
    assert!(
        followup_source.contains("build_prepared_batch_from_domain_changes_with_executor("),
        "state/internal/followup.rs should delegate tracked generation through canonical helpers"
    );
    assert!(
        !followup_source.contains("generate_commit("),
        "state/internal/followup.rs should not generate canonical commits directly"
    );

    let vtable_source = read_engine_source("state/internal/vtable_write.rs");
    assert!(
        vtable_source.contains("generate_commit_result_from_domain_changes_with_executor("),
        "state/internal/vtable_write.rs should delegate tracked generation through canonical helpers"
    );
    assert!(
        !vtable_source.contains("generate_commit("),
        "state/internal/vtable_write.rs should not generate canonical commits directly"
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
        canonical_mod.contains("pub(crate) mod runtime;"),
        "canonical/mod.rs should expose canonical::runtime"
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
        effective_state_source.contains("crate::canonical::readers::"),
        "effective_state_resolver should read committed state through canonical::readers"
    );

    let runtime_source = read_engine_source("sql/public/runtime/mod.rs");
    assert!(
        runtime_source.contains("crate::canonical::readers::"),
        "sql/public/runtime should use canonical::readers for committed-state lookups"
    );

    let history_query_source = read_engine_source("state/history/query.rs");
    assert!(
        history_query_source.contains("crate::canonical::graph::"),
        "state/history/query.rs should use canonical::graph"
    );

    let history_timeline_source = read_engine_source("state/history/timeline.rs");
    assert!(
        history_timeline_source.contains("crate::canonical::graph::"),
        "state/history/timeline.rs should use canonical::graph"
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
        "canonical/pending_session.rs",
        "canonical/runtime.rs",
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
    assert!(
        !runner_source.contains("GenerateCommitResult {"),
        "planned_write_runner should not rebuild synthetic GenerateCommitResult values"
    );
    assert!(
        runner_source.contains("mirror_public_registered_schema_bootstrap_rows(transaction, applied_output)"),
        "planned_write_runner should mirror bootstrap rows from the canonical applied_output"
    );
    assert!(
        runner_source.contains("build_pending_public_commit_session("),
        "planned_write_runner should build pending sessions through the canonical helper"
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
