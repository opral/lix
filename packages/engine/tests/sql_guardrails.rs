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
    let engine_source =
        fs::read_to_string(root.join("src/engine.rs")).expect("engine.rs should be readable");
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
    let engine_source =
        fs::read_to_string(root.join("src/engine.rs")).expect("engine.rs should be readable");

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
fn guardrail_execute_entrypoints_route_through_sql_api() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let engine_source =
        fs::read_to_string(root.join("src/engine.rs")).expect("engine.rs should be readable");
    let sql_api_source =
        fs::read_to_string(root.join("src/sql/api.rs")).expect("sql/api.rs should be readable");

    assert!(
        engine_source.contains("[path = \"sql/mod.rs\"]"),
        "engine module must wire sql runtime module"
    );
    assert!(
        !sql_api_source.contains("pub(crate) async fn execute_impl("),
        "legacy execute_impl wrapper should stay removed"
    );
    assert!(
        sql_api_source.contains("pub async fn execute(")
            && sql_api_source.contains("self.execute_impl_sql(sql, params, options, false).await"),
        "public execute entrypoint must delegate to execute_impl_sql"
    );
    assert!(
        sql_api_source.contains("pub(crate) async fn execute_internal(")
            && sql_api_source.contains("self.execute_impl_sql(sql, params, options, true).await"),
        "internal execute entrypoint must delegate to execute_impl_sql"
    );
    assert!(
        sql_api_source.contains("pub(crate) async fn execute_impl_sql"),
        "engine execute entrypoint must delegate to sql"
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
fn guardrail_sql_runtime_forbids_legacy_sql2_imports() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/sql");
    let mut files = Vec::new();
    collect_rust_sources(&root, &mut files);

    for file in files {
        let source = fs::read_to_string(&file).expect("source file should be readable");
        assert!(
            !source.contains("crate::engine::sql2::") && !source.contains("crate::sql2::"),
            "sql runtime must not depend on removed sql2 module paths: {}",
            file.display()
        );
        assert!(
            !source.contains("contracts::legacy_sql"),
            "sql runtime must not depend on removed legacy_sql contracts: {}",
            file.display()
        );
    }
}

#[test]
fn guardrail_sql_legacy_contract_adapter_directory_stays_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/sql/contracts/legacy_sql").exists(),
        "sql legacy contract adapter directory must stay removed"
    );
}

#[test]
fn guardrail_sql_runtime_forbids_legacy_bridge_usage() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/sql");
    let mut files = Vec::new();
    collect_rust_sources(&root, &mut files);

    for file in files {
        let source = fs::read_to_string(&file).expect("source file should be readable");
        let uses_legacy_bridge =
            source.contains("use crate::sql as legacy_sql") || source.contains("legacy_sql::");
        assert!(
            !uses_legacy_bridge,
            "legacy bridge usage in sql runtime is no longer allowed: {}",
            file.display()
        );
    }
}

#[test]
fn guardrail_legacy_bridge_is_removed_and_references_are_forbidden() {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !crate_root.join("src/sql/legacy_bridge.rs").exists(),
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
fn guardrail_legacy_sql2_directory_stays_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/sql2").exists(),
        "legacy src/sql2 directory must stay removed"
    );
    assert!(
        root.join("src/sql").exists(),
        "src/sql runtime directory must exist"
    );
}

#[test]
fn guardrail_runtime_source_forbids_crate_sql_imports() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rust_sources(&root, &mut files);

    for file in files {
        let source = fs::read_to_string(&file).expect("source file should be readable");
        assert!(
            !source.contains("crate::sql::"),
            "runtime source must not import crate::sql::*: {}",
            file.display()
        );
    }
}

#[test]
fn guardrail_preprocess_uses_bind_once_path_for_placeholder_binding() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let preprocess_source = fs::read_to_string(root.join("src/sql/planning/preprocess.rs"))
        .expect("sql/planning/preprocess.rs should be readable");

    assert!(
        preprocess_source.contains("bind_statements_with_appended_params_once"),
        "preprocess should bind placeholders through bind_once helper"
    );
    assert!(
        !preprocess_source.contains("bind_sql_with_state_and_appended_params("),
        "preprocess should not bind placeholders directly through ast utils"
    );
    assert!(
        !preprocess_source.contains("PlaceholderState::new()"),
        "preprocess should not own placeholder state directly"
    );
}
