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

    // Root now keeps only entrypoints and test helpers. Ownership cuts moved
    // the former filesystem/materialization shims under truthful module owners.
    let allowed = BTreeSet::from(["lib.rs".to_string(), "test_support.rs".to_string()]);

    let unexpected = actual.difference(&allowed).cloned().collect::<Vec<_>>();
    let missing = allowed.difference(&actual).cloned().collect::<Vec<_>>();

    assert!(
        unexpected.is_empty() && missing.is_empty(),
        "engine src root whitelist drifted.\nunexpected: {unexpected:?}\nmissing: {missing:?}"
    );
}
