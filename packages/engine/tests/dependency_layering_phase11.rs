use std::fs;
use std::path::{Path, PathBuf};

const READ_RUNTIME_DIR: &str = "src/read_runtime";
const READ_RUNTIME_MOD_FILE: &str = "src/read_runtime/mod.rs";
const READ_RUNTIME_ROWSET_FILE: &str = "src/read_runtime/rowset.rs";
const PUBLIC_READ_ARTIFACTS_FILE: &str = "src/sql/physical_plan/public_read_artifacts.rs";
const PHYSICAL_PLAN_MOD_FILE: &str = "src/sql/physical_plan/mod.rs";
const SPECIALIZED_PUBLIC_READ_FILE: &str = "src/sql/executor/public_runtime/read.rs";

#[test]
fn read_runtime_directory_does_not_import_compiler_ir() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut hits = Vec::new();
    collect_rs_files(&manifest_dir.join(READ_RUNTIME_DIR), &mut |path| {
        let text = fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        for forbidden in [
            "use crate::sql::binder::",
            "use crate::sql::logical_plan::",
            "use crate::sql::optimizer::",
            "use crate::sql::parser::",
            "use crate::sql::semantic_ir::",
        ] {
            if text.contains(forbidden) {
                hits.push(format!(
                    "{}: {}",
                    relative_display(manifest_dir, path),
                    forbidden
                ));
            }
        }
    });

    assert!(
        hits.is_empty(),
        "read_runtime should not import compiler IR directly outside the existing committed-read execution seam\nhits:\n{}",
        hits.join("\n"),
    );
}

#[test]
fn read_runtime_mod_does_not_redefine_rowset_semantics_locally() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let text = fs::read_to_string(manifest_dir.join(READ_RUNTIME_MOD_FILE))
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", READ_RUNTIME_MOD_FILE));

    assert!(
        text.contains("pub(crate) use rowset::execute_read_time_projection_read_with_backend;"),
        "read_runtime/mod.rs should delegate read-time projection execution to rowset.rs",
    );

    for forbidden in [
        "fn execute_read_time_projection_rows(",
        "fn read_time_projection_filter_matches_row(",
        "fn compare_read_time_projection_rows(",
        "fn sql_like_matches(",
    ] {
        assert!(
            !text.contains(forbidden),
            "read_runtime/mod.rs should not re-own rowset semantics after Phase B\nfile: {}\nforbidden: {}",
            READ_RUNTIME_MOD_FILE,
            forbidden,
        );
    }

    let rowset_text = fs::read_to_string(manifest_dir.join(READ_RUNTIME_ROWSET_FILE))
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", READ_RUNTIME_ROWSET_FILE));
    for required in [
        "fn execute_read_time_projection_rows(",
        "fn read_time_projection_filter_matches_row(",
        "fn compare_read_time_projection_rows(",
        "fn sql_like_matches(",
    ] {
        assert!(
            rowset_text.contains(required),
            "read_runtime/rowset.rs should remain the explicit bounded rowset owner\nfile: {}\nrequired: {}",
            READ_RUNTIME_ROWSET_FILE,
            required,
        );
    }
}

#[test]
fn compiler_owned_public_read_artifact_selection_lives_under_sql_physical_plan() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let artifacts_text = fs::read_to_string(manifest_dir.join(PUBLIC_READ_ARTIFACTS_FILE))
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", PUBLIC_READ_ARTIFACTS_FILE));
    let physical_plan_mod_text = fs::read_to_string(manifest_dir.join(PHYSICAL_PLAN_MOD_FILE))
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", PHYSICAL_PLAN_MOD_FILE));
    let executor_text = fs::read_to_string(manifest_dir.join(SPECIALIZED_PUBLIC_READ_FILE))
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", SPECIALIZED_PUBLIC_READ_FILE));

    assert!(
        artifacts_text.contains("pub(crate) async fn select_specialized_public_read_artifact("),
        "compiler-owned public-read artifact selection should live under sql/physical_plan\nfile: {}",
        PUBLIC_READ_ARTIFACTS_FILE,
    );
    assert!(
        artifacts_text.contains("try_compile_read_time_projection_read(")
            && artifacts_text.contains("lower_read_for_execution_with_layouts("),
        "public_read_artifacts.rs should own compiler-side route selection across read-time projection and lowered SQL paths\nfile: {}",
        PUBLIC_READ_ARTIFACTS_FILE,
    );
    assert!(
        physical_plan_mod_text.contains("select_specialized_public_read_artifact"),
        "sql/physical_plan/mod.rs should re-export the compiler-owned public-read selection seam\nfile: {}",
        PHYSICAL_PLAN_MOD_FILE,
    );
    assert!(
        executor_text.contains("select_specialized_public_read_artifact("),
        "executor orchestration should call the compiler-owned selection seam\nfile: {}",
        SPECIALIZED_PUBLIC_READ_FILE,
    );
    assert!(
        !executor_text.contains("try_compile_read_time_projection_read(")
            && !executor_text.contains("lower_read_for_execution_with_layouts("),
        "executor orchestration should not compile public-read artifacts locally\nfile: {}",
        SPECIALIZED_PUBLIC_READ_FILE,
    );
}

fn collect_rs_files(dir: &Path, visit: &mut impl FnMut(&PathBuf)) {
    let mut entries = fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", dir.display()))
        .map(|entry| entry.unwrap_or_else(|error| panic!("failed to read dir entry: {error}")))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, visit);
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            visit(&path);
        }
    }
}

fn relative_display(manifest_dir: &Path, path: &Path) -> String {
    path.strip_prefix(manifest_dir)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}
