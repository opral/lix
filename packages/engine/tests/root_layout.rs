use std::collections::BTreeSet;
use std::path::Path;

#[test]
fn root_rs_whitelist_matches_phase_a_layout_contract() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let actual = std::fs::read_dir(&src_root)
        .expect("engine src directory should exist")
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let is_root_rs = path.is_file() && path.extension().is_some_and(|ext| ext == "rs");
            is_root_rs.then(|| entry.file_name().to_string_lossy().into_owned())
        })
        .collect::<BTreeSet<_>>();

    // Phase A keeps only root entrypoints/test helpers plus files explicitly
    // deferred to later ownership cuts.
    let allowed = BTreeSet::from([
        "api.rs".to_string(),
        "boot.rs".to_string(),
        "engine.rs".to_string(),
        "execution_effects.rs".to_string(),
        "execution_runtime.rs".to_string(),
        "filesystem_materialization.rs".to_string(),
        "filesystem_payload_sql.rs".to_string(),
        "filesystem_projection_sql.rs".to_string(),
        "lib.rs".to_string(),
        "lix.rs".to_string(),
        "prepared_write_artifacts.rs".to_string(),
        "public_surface_source_sql.rs".to_string(),
        "read_pipeline.rs".to_string(),
        "state_selector_rows.rs".to_string(),
        "test_support.rs".to_string(),
        "transaction_execution.rs".to_string(),
        "transaction_mode.rs".to_string(),
        "write_pipeline.rs".to_string(),
    ]);

    let unexpected = actual.difference(&allowed).cloned().collect::<Vec<_>>();
    let missing = allowed.difference(&actual).cloned().collect::<Vec<_>>();

    assert!(
        unexpected.is_empty() && missing.is_empty(),
        "engine src root whitelist drifted.\nunexpected: {unexpected:?}\nmissing: {missing:?}"
    );
}
