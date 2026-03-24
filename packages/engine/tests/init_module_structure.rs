use std::fs;
use std::path::PathBuf;

fn read_engine_source(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(relative);
    fs::read_to_string(path).expect("source file should be readable")
}

#[test]
fn seed_module_does_not_own_table_creation() {
    let source = read_engine_source("init/seed.rs");
    assert!(
        !source.contains("ensure_schema_live_table("),
        "seed.rs should not create schema storage tables"
    );
    assert!(
        !source.contains("CREATE TABLE"),
        "seed.rs should not own DDL"
    );
}

#[test]
fn run_module_stays_on_legacy_live_state_path() {
    let source = read_engine_source("init/run.rs");
    assert!(
        source.contains("crate::state::live_state::"),
        "run.rs should stay on the legacy live_state path in this cleanup pass"
    );
    assert!(
        !source.contains("crate::live_state::"),
        "run.rs must not hook the new live_state module into init yet"
    );
}

#[test]
fn mod_module_is_wiring_only() {
    let source = read_engine_source("init/mod.rs");
    assert!(
        !source.contains("CREATE TABLE"),
        "init/mod.rs should not own DDL"
    );
    assert!(
        !source.contains("BEGIN IMMEDIATE"),
        "init/mod.rs should not own init orchestration"
    );
}

#[test]
fn restore_path_does_not_run_init_structure() {
    let source = read_engine_source("api.rs");
    assert!(
        !source.contains("create_backend_tables(self.backend.as_ref())"),
        "restore_from_image should not run init structure helpers"
    );
}
