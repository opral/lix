use std::fs;
use std::path::{Path, PathBuf};

fn collect_rust_sources(root: &Path, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(root).expect("read_dir should succeed");
    for entry in entries {
        let entry = entry.expect("dir entry should be readable");
        let path = entry.path();
        let file_type = entry.file_type().expect("file type should be readable");
        if file_type.is_dir() {
            collect_rust_sources(&path, out);
            continue;
        }
        if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

fn read_runtime_engine_section() -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let engine_source = fs::read_to_string(root.join("src/engine.rs"))
        .expect("engine.rs should be readable");
    let boundary = engine_source
        .find("mod tests {")
        .expect("engine.rs should contain test module");
    engine_source[..boundary].to_string()
}

#[test]
fn guardrail_legacy_execute_directory_is_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/execute").exists(),
        "legacy src/execute directory must stay removed"
    );
}

#[test]
fn guardrail_engine_module_is_not_wired_to_legacy_execute_mod() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let engine_source = fs::read_to_string(root.join("src/engine.rs"))
        .expect("engine.rs should be readable");

    assert!(
        !engine_source.contains("[path = \"execute/mod.rs\"]"),
        "engine.rs must not wire the removed execute module"
    );
}

#[test]
fn guardrail_engine_runtime_section_excludes_legacy_sql_pipeline_imports() {
    let runtime_source = read_runtime_engine_section();
    for forbidden in [
        "preprocess_sql",
        "preprocess_parsed_statements_with_provider_and_detected_file_domain_changes",
        "is_query_only_statements",
        "parse_sql_statements",
        "coalesce_vtable_inserts_in_statement_list",
        "coalesce_lix_file_transaction_statements",
    ] {
        assert!(
            !runtime_source.contains(forbidden),
            "engine runtime section must not import legacy sql pipeline symbol: {forbidden}"
        );
    }
}

#[test]
fn guardrail_execute_entrypoints_route_through_sql2_api() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let engine_source = fs::read_to_string(root.join("src/engine.rs"))
        .expect("engine.rs should be readable");
    let sql2_api_source = fs::read_to_string(root.join("src/sql2/api.rs"))
        .expect("sql2/api.rs should be readable");

    assert!(
        engine_source.contains("[path = \"sql2/mod.rs\"]"),
        "engine module must wire sql2 runtime module"
    );
    assert!(
        sql2_api_source.contains("pub(crate) async fn execute_impl")
            && sql2_api_source.contains("self.execute_impl_sql2(sql, params, options, allow_internal_tables)"),
        "engine execute entrypoint must delegate to sql2"
    );
    assert!(
        sql2_api_source.contains("pub(crate) async fn execute_impl_sql2"),
        "sql2 api must expose execute_impl_sql2 entrypoint"
    );
}

#[test]
fn guardrail_forbids_string_matched_postprocess_fallback() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rust_sources(&root, &mut files);

    for file in files {
        let source = fs::read_to_string(&file).expect("source file should be readable");
        assert!(
            !source.contains("is_postprocess_multi_statement_error"),
            "string-matched postprocess fallback helper must not be reintroduced: {}",
            file.display()
        );
    }
}

#[test]
fn guardrail_sql2_runtime_forbids_direct_sql_runtime_imports() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/sql2");
    let mut files = Vec::new();
    collect_rust_sources(&root, &mut files);

    for file in files {
        let source = fs::read_to_string(&file).expect("source file should be readable");
        assert!(
            !source.contains("crate::sql::"),
            "sql2 runtime must not directly depend on crate::sql::*: {}",
            file.display()
        );
        assert!(
            !source.contains("contracts::legacy_sql"),
            "sql2 runtime must not depend on removed legacy_sql contracts: {}",
            file.display()
        );
    }
}

#[test]
fn guardrail_sql2_legacy_contract_adapter_directory_stays_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/sql2/contracts/legacy_sql").exists(),
        "sql2 legacy contract adapter directory must stay removed"
    );
}

#[test]
fn guardrail_sql2_runtime_forbids_legacy_bridge_usage() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/sql2");
    let mut files = Vec::new();
    collect_rust_sources(&root, &mut files);

    for file in files {
        let source = fs::read_to_string(&file).expect("source file should be readable");
        let uses_legacy_bridge =
            source.contains("use crate::sql as legacy_sql") || source.contains("legacy_sql::");
        assert!(
            !uses_legacy_bridge,
            "legacy bridge usage in sql2 runtime is no longer allowed: {}",
            file.display()
        );
    }
}

#[test]
fn guardrail_legacy_bridge_is_removed_and_references_are_forbidden() {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !crate_root.join("src/sql2/legacy_bridge.rs").exists(),
        "legacy bridge module must remain removed"
    );

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rust_sources(&root, &mut files);

    for file in files {
        let source = fs::read_to_string(&file).expect("source file should be readable");
        assert!(
            !source.contains("legacy_bridge::"),
            "legacy bridge reference must not be reintroduced: {}",
            file.display()
        );
    }
}

#[test]
fn guardrail_legacy_sql_bridge_alias_usage_is_forbidden() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rust_sources(&root, &mut files);

    for file in files {
        let source = fs::read_to_string(&file).expect("source file should be readable");
        let uses_legacy_sql_bridge =
            source.contains("use crate::sql as legacy_sql") || source.contains("legacy_sql::");
        assert!(
            !uses_legacy_sql_bridge,
            "legacy_sql alias bridge usage is no longer allowed: {}",
            file.display()
        );
    }
}

#[test]
fn guardrail_runtime_sql_imports_are_isolated_to_preprocess_runtime_module() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rust_sources(&root, &mut files);

    for file in files {
        let normalized = file.to_string_lossy().replace('\\', "/");
        if normalized.contains("/src/sql/") {
            continue;
        }

        let source = fs::read_to_string(&file).expect("source file should be readable");
        if !source.contains("crate::sql::") {
            continue;
        }

        assert!(
            normalized.ends_with("src/sql_preprocess_runtime.rs"),
            "runtime crate::sql imports must stay isolated to src/sql_preprocess_runtime.rs: {}",
            file.display()
        );
    }
}
