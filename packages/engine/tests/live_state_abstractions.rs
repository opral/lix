use std::fs;
use std::path::PathBuf;

fn read_engine_source(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(relative);
    fs::read_to_string(path).expect("source file should be readable")
}

#[test]
fn session_and_roots_modules_use_live_state_raw_reads() {
    let session_source = read_engine_source("live_state/session.rs");
    let roots_source = read_engine_source("live_state/roots.rs");
    assert!(
        session_source.contains("crate::live_state::raw"),
        "session helpers should build on live_state::raw"
    );
    assert!(
        roots_source.contains("crate::live_state::raw"),
        "root helpers should build on live_state::raw"
    );
    assert!(
        session_source.contains("RawStorage::Untracked"),
        "session helpers should read engine-owned rows from the untracked lane through raw"
    );
    assert!(
        roots_source.contains("RawStorage::Untracked"),
        "root helpers should read engine-owned rows from the untracked lane through raw"
    );
    for source in [&session_source, &roots_source] {
        assert!(
            !source.contains("crate::live_state::untracked::load_exact_row_with_backend"),
            "session/root helpers should not bypass raw with direct untracked exact-row reads"
        );
        assert!(
            !source.contains("crate::live_state::untracked::scan_rows_with_backend"),
            "session/root helpers should not bypass raw with direct untracked scans"
        );
    }
}

#[test]
fn non_write_consumers_use_live_state_facades_instead_of_lane_modules() {
    for relative in [
        "sql/execution/shared_path.rs",
        "canonical/state_source.rs",
        "sql/public/validation.rs",
        "live_state/materialize/plan.rs",
        "session/mod.rs",
        "undo_redo/mod.rs",
        "sql/public/planner/semantics/write_resolver.rs",
    ] {
        let source = read_engine_source(relative);
        assert!(
            !source.contains("live_state::tracked"),
            "{relative} should not import live_state::tracked directly for read abstraction work"
        );
        assert!(
            !source.contains("live_state::untracked"),
            "{relative} should not import live_state::untracked directly for read abstraction work"
        );
    }
}

#[test]
fn schema_registration_hides_legacy_bridge_fields() {
    let source = read_engine_source("live_state/mod.rs");
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        source.contains("enum SchemaRegistrationSource"),
        "SchemaRegistration should use an internal source model instead of public bridge fields"
    );
    assert!(
        source.contains("pub(crate) mod raw;"),
        "live_state should expose raw as an internal facade"
    );
    assert!(
        source.contains("pub mod session;"),
        "live_state should expose the session helper surface"
    );
    assert!(
        source.contains("pub mod roots;"),
        "live_state should expose the root helper surface"
    );
    assert!(
        !source.contains("pub mod system;"),
        "live_state should not reintroduce the vague system helper surface"
    );
    assert!(
        !source.contains("pub registered_snapshot"),
        "registered_snapshot should not be a public field on SchemaRegistration"
    );
    assert!(
        !source.contains("pub(crate) registered_snapshot"),
        "registered_snapshot should stay behind accessors/constructors"
    );
    assert!(
        !source.contains("pub layout_override"),
        "layout_override should not be a public field on SchemaRegistration"
    );
    assert!(
        !source.contains("pub(crate) layout_override"),
        "layout_override should stay behind internal accessors"
    );
    assert!(
        !manifest_dir.join("src/live_state/system.rs").exists(),
        "live_state/system.rs should stay removed after the session/roots split"
    );
}

#[test]
fn read_history_orchestrates_live_roots_through_the_owned_boundary() {
    let source = read_engine_source("read/history.rs");
    let roots_source = read_engine_source("live_state/roots.rs");
    assert!(
        source.contains("crate::live_state::roots::"),
        "read history should resolve live roots through live_state::roots"
    );
    assert!(
        source.contains("crate::live_state::session::"),
        "read history should be able to resolve live session facts through live_state::session"
    );
    assert!(
        !source.contains("untracked_live_table_name(\"lix_version_ref\")"),
        "read history should not inline live version-ref table naming"
    );
    assert!(
        source.contains("resolve_history_root_facts_with_backend("),
        "read history should consume typed live root facts instead of SQL fragments"
    );
    assert!(
        !source.contains("build_requested_root_commits_cte_sql("),
        "read history should not request root CTE fragments from live_state::roots"
    );
    assert!(
        !roots_source.contains("build_requested_root_commits_cte_sql("),
        "live_state::roots should not expose the old SQL-fragment builder"
    );
    assert!(
        !roots_source.contains("commit_by_version"),
        "live_state::roots should not depend on canonical CTE alias names"
    );
    assert!(
        !roots_source.contains("requested_commits AS"),
        "live_state::roots should not build canonical-history CTEs"
    );
    assert!(
        source.contains("crate::canonical::history::"),
        "read history should compose canonical history instead of owning lineage SQL directly"
    );
}

#[test]
fn filesystem_history_consumes_committed_history_via_read_models() {
    let source = read_engine_source("filesystem/history/mod.rs");
    assert!(
        source.contains("crate::read::models::"),
        "filesystem history should consume committed history through read::models"
    );
    assert!(
        !source.contains("crate::state::history::"),
        "filesystem history should not import state::history directly"
    );
}

#[test]
fn state_module_no_longer_owns_history_reader() {
    let state_mod = read_engine_source("state/mod.rs");
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !state_mod.contains("pub(crate) mod history;"),
        "state/mod.rs should not keep a state::history owner once read/canonical split it out"
    );
    assert!(
        !manifest_dir.join("src/state/history/mod.rs").exists(),
        "src/state/history/mod.rs should stay removed once read/* owns history orchestration"
    );
    assert!(
        !manifest_dir.join("src/state/history/query.rs").exists(),
        "src/state/history/query.rs should stay removed once read/* owns history orchestration"
    );
    assert!(
        !manifest_dir.join("src/state/history/types.rs").exists(),
        "src/state/history/types.rs should stay removed once read/* owns history orchestration"
    );
}

#[test]
fn live_state_storage_layout_is_the_only_live_layout_owner() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let schema_mod = read_engine_source("schema/mod.rs");
    let live_state_layout = read_engine_source("live_state/storage/layout.rs");

    assert!(
        !schema_mod.contains("mod live_layout;"),
        "schema/mod.rs must not wire a parallel live_layout module"
    );
    assert!(
        !schema_mod.contains("mod registry;"),
        "schema/mod.rs must not wire the old schema registry live-layout owner"
    );
    assert!(
        live_state_layout.contains("struct LiveTableLayout"),
        "live_state/storage/layout.rs should define the owned LiveTableLayout"
    );
    assert!(
        live_state_layout.contains("logical_live_snapshot_from_row_with_layout"),
        "snapshot reconstruction should live under live_state/storage/layout.rs"
    );
    assert!(
        !manifest_dir.join("src/schema/live_layout.rs").exists(),
        "schema/live_layout.rs should stay removed"
    );
    assert!(
        !manifest_dir.join("src/schema/registry.rs").exists(),
        "schema/registry.rs should stay removed"
    );
    assert!(
        !manifest_dir.join("src/schema/live_store.rs").exists(),
        "schema/live_store.rs should stay removed"
    );
    assert!(
        !manifest_dir.join("src/state/materialization").exists(),
        "dead state/materialization subtree should stay removed"
    );
}
