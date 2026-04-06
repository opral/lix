use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn engine_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn contracts_root() -> PathBuf {
    engine_root().join("src/contracts")
}

fn rust_source_files(root: &Path) -> Vec<PathBuf> {
    fn visit(dir: &Path, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(dir).expect("directory should be readable") {
            let entry = entry.expect("directory entry should be readable");
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == "tests") {
                    continue;
                }
                visit(&path, files);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    visit(root, &mut files);
    files.sort();
    files
}

fn contract_source_files() -> Vec<PathBuf> {
    let files = rust_source_files(&contracts_root());
    assert!(
        !files.is_empty(),
        "src/contracts should contain Rust sources for Plan 30 boundary checks"
    );
    files
}

fn production_source_files() -> Vec<PathBuf> {
    rust_source_files(&engine_root().join("src"))
}

fn relative_source_path(path: &Path) -> String {
    path.strip_prefix(engine_root().join("src"))
        .expect("path should be inside src/")
        .display()
        .to_string()
}

fn read_production_source(path: &Path) -> String {
    strip_cfg_test_items(&fs::read_to_string(path).expect("source file should be readable"))
}

fn strip_cfg_test_items(source: &str) -> String {
    let lines: Vec<&str> = source.lines().collect();
    let mut output = String::new();
    let mut index = 0;

    while index < lines.len() {
        let line = lines[index];
        let trimmed = line.trim_start();
        if trimmed.starts_with("#[") && trimmed.contains("cfg(test)") {
            index += 1;
            while index < lines.len() && lines[index].trim_start().starts_with("#[") {
                index += 1;
            }
            skip_annotated_item(&lines, &mut index);
            continue;
        }

        output.push_str(line);
        output.push('\n');
        index += 1;
    }

    output
}

fn skip_annotated_item(lines: &[&str], index: &mut usize) {
    let mut brace_depth = 0i32;
    let mut saw_item_body = false;

    while *index < lines.len() {
        let line = lines[*index];
        brace_depth += brace_delta(line);
        saw_item_body |= line.contains('{') || line.trim_end().ends_with(';');
        *index += 1;

        if saw_item_body && brace_depth <= 0 {
            break;
        }
    }
}

fn brace_delta(line: &str) -> i32 {
    line.chars().fold(0, |count, ch| match ch {
        '{' => count + 1,
        '}' => count - 1,
        _ => count,
    })
}

fn find_matching_brace(source: &str, open_brace_index: usize) -> usize {
    let mut depth = 0i32;
    for (index, ch) in source.char_indices().skip(open_brace_index) {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return index;
                }
            }
            _ => {}
        }
    }

    panic!("expected matching brace in source");
}

fn surface_registry_public_method_names(source: &str) -> BTreeSet<String> {
    let needle = "impl SurfaceRegistry {";
    let mut methods = BTreeSet::new();
    let mut search_start = 0usize;

    while let Some(relative_start) = source[search_start..].find(needle) {
        let start = search_start + relative_start;
        let open_brace_index = start + needle.len() - 1;
        let end = find_matching_brace(source, open_brace_index);
        methods.extend(source[start..=end].lines().filter_map(|line| {
            let trimmed = line.trim_start();
            trimmed
                .strip_prefix("pub(crate) fn ")
                .or_else(|| trimmed.strip_prefix("pub fn "))
                .map(|rest| {
                    rest.split('(')
                        .next()
                        .expect("function signature should include name")
                        .trim()
                        .to_string()
                })
        }));
        search_start = end + 1;
    }

    assert!(
        !methods.is_empty(),
        "contracts/surface.rs should define at least one impl SurfaceRegistry block",
    );

    methods
}

fn engine_cycle_report() -> serde_json::Value {
    let output = Command::new("node")
        .arg("scripts/count-cyclic-dependencies.mjs")
        .arg("--json")
        .current_dir(engine_root())
        .output()
        .expect("engine cycle baseline script should run");

    assert!(
        output.status.success(),
        "engine cycle baseline script should succeed, stderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );

    serde_json::from_slice(&output.stdout)
        .expect("engine cycle baseline script should emit valid JSON")
}

#[test]
fn contracts_boundary_plan30_blocks_upward_owner_imports() {
    let banned_import_roots = [
        "crate::runtime::",
        "crate::filesystem::",
        "crate::workspace::",
        "crate::write_runtime::",
        "crate::read_runtime::",
        "crate::session::",
        "crate::live_state::",
        "crate::engine",
    ];

    for path in contract_source_files() {
        let source = read_production_source(&path);
        let label = relative_source_path(&path);
        for banned in banned_import_roots {
            assert!(
                !source.contains(banned),
                "{label} should stay downward-only and must not import {banned}",
            );
        }
    }
}

#[test]
fn contracts_boundary_plan30_blocks_backend_runtime_tokens() {
    let banned_tokens = [
        "LixBackend",
        "LixBackendTransaction",
        "Engine",
        "ExecutionContext",
        "shared_runtime(",
    ];

    for path in contract_source_files() {
        let source = read_production_source(&path);
        let label = relative_source_path(&path);
        for banned in banned_tokens {
            assert!(
                !source.contains(banned),
                "{label} should not contain backend/runtime token {banned}",
            );
        }
    }
}

#[test]
fn contracts_boundary_plan30_blocks_crate_root_type_aliases() {
    for path in contract_source_files() {
        let source = read_production_source(&path);
        let label = relative_source_path(&path);
        assert!(
            !source.contains("use crate::{"),
            "{label} should not use crate-root grouped imports because they hide the owning module",
        );
        assert!(
            !contains_crate_root_type_alias(&source),
            "{label} should not use crate-root type aliases like crate::LixError or crate::TransactionMode",
        );
    }
}

fn contains_crate_root_type_alias(source: &str) -> bool {
    let mut remainder = source;
    while let Some(index) = remainder.find("crate::") {
        let candidate = &remainder[index + "crate::".len()..];
        if candidate
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_uppercase())
        {
            return true;
        }
        remainder = candidate;
    }
    false
}

#[test]
fn contracts_boundary_plan30_blocks_backend_bootstrap_entrypoints() {
    for path in contract_source_files() {
        let source = read_production_source(&path);
        let label = relative_source_path(&path);
        assert!(
            !source.contains("bootstrap_with_backend"),
            "{label} should not expose backend bootstrap entrypoints once registry assembly lives with owners",
        );
    }
}

#[test]
fn contracts_boundary_plan30_blocks_surface_registry_inherent_bootstrap_api() {
    let contracts_surface = read_production_source(&contracts_root().join("surface.rs"));
    let schema_public_surfaces =
        read_production_source(&engine_root().join("src/schema/public_surfaces.rs"));

    for banned in [
        "fn with_builtin_surfaces(",
        "fn bootstrap_with_backend(",
        "fn register_builtin_static_surfaces(",
        "fn register_builtin_entity_surfaces_from_spec(",
        "fn register_dynamic_entity_surfaces(",
        "fn remove_dynamic_entity_surfaces_for_schema_key(",
        "fn replace_dynamic_entity_surfaces_from_stored_snapshot(",
    ] {
        assert!(
            !contracts_surface.contains(banned),
            "contracts/surface.rs should not define owner-shaped inherent SurfaceRegistry API {banned}",
        );
    }

    for banned in ["fn with_builtin_surfaces(", "fn bootstrap_with_backend("] {
        assert!(
            !schema_public_surfaces.contains(banned),
            "schema/public_surfaces.rs should not define owner-shaped inherent SurfaceRegistry API {banned}",
        );
    }

    assert!(
        !schema_public_surfaces.contains("impl SurfaceRegistry"),
        "schema/public_surfaces.rs should keep registry assembly as free functions rather than extending SurfaceRegistry inherently",
    );

    let allowed_methods = BTreeSet::from([
        "new".to_string(),
        "catalog_epoch".to_string(),
        "insert_descriptors".to_string(),
        "remove_descriptors_matching".to_string(),
        "advance_catalog_epoch".to_string(),
        "bind_relation_name".to_string(),
        "bind_object_name".to_string(),
        "public_surface_names".to_string(),
        "public_surface_columns".to_string(),
        "registered_schema_keys".to_string(),
        "registered_state_backed_schema_keys".to_string(),
        "registered_state_surface_schema_keys".to_string(),
    ]);
    assert_eq!(
        surface_registry_public_method_names(&contracts_surface),
        allowed_methods,
        "contracts/surface.rs should expose only pure SurfaceRegistry state/binding APIs after Phase I",
    );

    for path in production_source_files() {
        let source = read_production_source(&path);
        let label = relative_source_path(&path);
        for banned in [
            "SurfaceRegistry::with_builtin_surfaces(",
            "SurfaceRegistry::bootstrap_with_backend(",
            ".register_builtin_static_surfaces(",
            ".register_builtin_entity_surfaces_from_spec(",
            ".register_dynamic_entity_surfaces(",
            ".remove_dynamic_entity_surfaces_for_schema_key(",
            ".replace_dynamic_entity_surfaces_from_stored_snapshot(",
        ] {
            assert!(
                !source.contains(banned),
                "{label} should not call owner/bootstrap assembly through inherent SurfaceRegistry API {banned}",
            );
        }
    }
}

#[test]
fn contracts_boundary_plan30_blocks_legacy_owner_structs_in_contract_traits() {
    let traits_source = read_production_source(&contracts_root().join("traits.rs"));
    for banned in ["FilesystemTransactionFileState", "CanonicalCommitReceipt"] {
        assert!(
            !traits_source.contains(banned),
            "contracts/traits.rs should not mention owner-specific struct {banned} in public trait signatures",
        );
    }
}

#[test]
fn contracts_boundary_plan30_keeps_contracts_out_of_engine_sccs() {
    let report = engine_cycle_report();
    let modules = report["modules_analyzed"]
        .as_array()
        .expect("cycle report should list analyzed modules");
    assert!(
        modules
            .iter()
            .any(|module| module.as_str() == Some("contracts")),
        "cycle report should analyze the contracts top-level module",
    );

    let components = report["components"]
        .as_array()
        .expect("cycle report should list SCC components");
    for component in components {
        let members = component["modules"]
            .as_array()
            .expect("cycle component should list module names");
        assert!(
            !members
                .iter()
                .any(|module| module.as_str() == Some("contracts")),
            "contracts must stay out of every engine SCC; offending component: {component}",
        );
    }
}
