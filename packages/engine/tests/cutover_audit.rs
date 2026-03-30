use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn manifest_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn source_path(relative: &str) -> PathBuf {
    manifest_dir().join(relative)
}

fn read_source(relative: &str) -> String {
    fs::read_to_string(source_path(relative))
        .unwrap_or_else(|error| panic!("failed to read '{relative}': {error}"))
}

fn collect_rs_files(root: &Path, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(root)
        .unwrap_or_else(|error| panic!("failed to read dir '{}': {error}", root.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|error| {
            panic!(
                "failed to read dir entry under '{}': {error}",
                root.display()
            )
        });
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

fn src_rs_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rs_files(&source_path("src"), &mut files);
    files.sort();
    files
}

fn relative_to_manifest(path: &Path) -> String {
    path.strip_prefix(manifest_dir())
        .unwrap_or(path)
        .display()
        .to_string()
}

fn is_live_state_path(relative: &str) -> bool {
    relative.starts_with("src/live_state/")
}

#[test]
fn local_version_head_cutover_rejects_storage_ordering_and_limits_version_ref_table_reads() {
    for relative in [
        "src/canonical/refs.rs",
        "src/canonical/version_state.rs",
        "src/live_state/filesystem_projection.rs",
        "src/sql/physical_plan/lowerer.rs",
    ] {
        let source = read_source(relative);
        assert!(
            !source.contains("ORDER BY created_at DESC, id DESC"),
            "{relative} must not define local version-head meaning by storage recency",
        );
    }

    let actual = src_rs_files()
        .into_iter()
        .filter_map(|path| {
            let source = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read '{}': {error}", path.display()));
            source
                .contains("tracked_relation_name(\"lix_version_ref\")")
                .then(|| relative_to_manifest(&path))
        })
        .collect::<BTreeSet<_>>();
    let expected = BTreeSet::from([
        "src/live_state/filesystem_projection.rs".to_string(),
        "src/live_state/public_read_sql.rs".to_string(),
    ]);
    assert_eq!(
        actual, expected,
        "replica-local version-ref table reads escaped the allowlist"
    );
}

#[test]
fn canonical_cutover_does_not_reintroduce_compat_mirrors_or_fallback_depth() {
    for relative in [
        "src/live_state/projection/mod.rs",
        "src/live_state/materialize/plan.rs",
        "src/sql/physical_plan/lowerer.rs",
    ] {
        let source = read_source(relative);
        for banned in [
            "legacy compatibility mirror",
            "compatibility mirror",
            "compat mirror",
        ] {
            assert!(
                !source.contains(banned),
                "{relative} must not describe a steady-state compat mirror dependency"
            );
        }
    }

    for relative in [
        "src/canonical/history.rs",
        "src/canonical/state_source.rs",
        "src/canonical/graph_sql.rs",
        "src/canonical/graph_index.rs",
    ] {
        let source = read_source(relative);
        for banned in [
            "2048",
            "CANONICAL_FALLBACK_MAX_COMMIT_DEPTH",
            "fallback depth",
        ] {
            assert!(
                !source.contains(banned),
                "{relative} must not contain legacy fallback-depth semantics"
            );
        }
    }

    let graph_sql = read_source("src/canonical/graph_sql.rs");
    let graph_sql_impl = graph_sql.split("#[cfg(test)]").next().unwrap_or(&graph_sql);
    assert!(
        graph_sql_impl.contains("FROM lix_internal_change"),
        "canonical graph seed must read canonical change rows",
    );
    assert!(
        graph_sql_impl.contains("LEFT JOIN lix_internal_snapshot"),
        "canonical graph seed must read canonical snapshots",
    );
    for banned in [
        "lix_internal_live_v1_lix_commit",
        "lix_internal_live_v1_lix_change_set",
        "lix_internal_live_v1_lix_change_set_element",
        "lix_internal_live_v1_lix_commit_edge",
    ] {
        assert!(
            !graph_sql_impl.contains(banned),
            "canonical graph seed must not depend on live commit-family mirrors: {banned}"
        );
    }
}

#[test]
fn projection_lifecycle_stays_behind_root_level_live_state_entrypoints() {
    let actual = src_rs_files()
        .into_iter()
        .filter_map(|path| {
            let relative = relative_to_manifest(&path);
            if is_live_state_path(&relative) {
                return None;
            }
            let source = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read '{}': {error}", path.display()));
            (source.contains("crate::live_state::projection::")
                || source.contains("use crate::live_state::projection")
                || source.contains("crate::live_state::projection::{"))
            .then_some(relative)
        })
        .collect::<BTreeSet<_>>();
    assert!(
        actual.is_empty(),
        "projection lifecycle escaped live_state/*: {actual:?}"
    );

    for (relative, required) in [
        (
            "src/canonical/append.rs",
            &["apply_commit_projections_best_effort_in_transaction("][..],
        ),
        (
            "src/canonical/pending_session.rs",
            &["apply_commit_projections_best_effort_in_transaction("][..],
        ),
        (
            "src/transaction/coordinator.rs",
            &[
                "mark_live_state_projection_ready_in_transaction(",
                "apply_canonical_receipt_in_transaction(",
            ][..],
        ),
        (
            "src/init/run.rs",
            &[
                "load_latest_live_state_replay_cursor_with_backend(",
                "mark_live_state_projection_ready_with_backend(",
            ][..],
        ),
        (
            "src/sql/executor/public_runtime/read.rs",
            &["load_live_state_projection_status_with_backend("][..],
        ),
        (
            "src/version/merge_version.rs",
            &["mark_live_state_projection_ready_without_replay_cursor_in_transaction("][..],
        ),
    ] {
        let source = read_source(relative);
        for needle in required {
            assert!(
                source.contains(needle),
                "{relative} must call the root-level live_state projection entrypoint '{needle}'",
            );
        }
    }
}
