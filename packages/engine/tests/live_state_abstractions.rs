use std::fs;
use std::path::PathBuf;

fn read_engine_source(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(relative);
    fs::read_to_string(path).expect("source file should be readable")
}

#[test]
fn system_module_uses_raw_untracked_reads() {
    let source = read_engine_source("live_state/system.rs");
    assert!(
        source.contains("crate::live_state::raw"),
        "system helpers should build on live_state::raw"
    );
    assert!(
        source.contains("RawStorage::Untracked"),
        "system helpers should read engine-owned rows from the untracked lane through raw"
    );
    assert!(
        !source.contains("crate::live_state::untracked::load_exact_row_with_backend"),
        "system helpers should not bypass raw with direct untracked exact-row reads"
    );
    assert!(
        !source.contains("crate::live_state::untracked::scan_rows_with_backend"),
        "system helpers should not bypass raw with direct untracked scans"
    );
}

#[test]
fn non_write_consumers_use_raw_or_system_instead_of_lane_modules() {
    for relative in [
        "sql/execution/shared_path.rs",
        "canonical/state_source.rs",
        "sql/public/validation.rs",
        "live_state/materialize/plan.rs",
        "version/switch_version.rs",
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
    assert!(
        source.contains("enum SchemaRegistrationSource"),
        "SchemaRegistration should use an internal source model instead of public bridge fields"
    );
    assert!(
        source.contains("pub(crate) mod raw;"),
        "live_state should expose raw as an internal facade"
    );
    assert!(
        source.contains("pub mod system;"),
        "live_state should expose a dedicated system helper surface"
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
}
