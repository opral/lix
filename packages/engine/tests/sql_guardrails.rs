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
        let is_transition_shim = file.ends_with(Path::new("src/sql/execution/shared_path.rs"));
        assert!(
            !source.contains("crate::engine::sql2::"),
            "sql runtime must not depend on removed engine::sql2 bridge paths: {}",
            file.display()
        );
        assert!(
            is_transition_shim || !source.contains("crate::sql2::"),
            "sql runtime must not depend on sql2 outside the shared_path transition shim: {}",
            file.display()
        );
        assert!(
            !source.contains("contracts::legacy_sql"),
            "sql runtime must not depend on removed legacy_sql contracts: {}",
            file.display()
        );
    }

    let shared_path_source = fs::read_to_string(root.join("execution/shared_path.rs"))
        .expect("shared_path.rs should be readable");
    assert!(
        shared_path_source.contains("prepare_sql2_read"),
        "shared_path transition shim should invoke sql2 read preparation during migration"
    );
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
fn guardrail_sql2_directory_exists_alongside_legacy_sql_runtime() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        root.join("src/sql2").exists(),
        "src/sql2 directory must exist for the semantic rewrite"
    );
    assert!(
        root.join("src/sql").exists(),
        "src/sql runtime directory must remain available during migration"
    );
}

#[test]
fn guardrail_duplicate_public_surface_registry_is_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/lix_table_registry.rs").exists(),
        "duplicate public surface registry must stay removed once sql2/catalog owns diagnostics"
    );
}

#[test]
fn guardrail_sql2_stays_isolated_from_legacy_rewrite_followup_and_classifier_modules() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/sql2");
    let mut files = Vec::new();
    collect_rust_sources(&root, &mut files);

    for file in files {
        let source = fs::read_to_string(&file).expect("source file should be readable");
        for forbidden in [
            "crate::engine::sql::planning::",
            "crate::engine::sql::ast::utils",
            "crate::sql::ast::utils",
            "crate::engine::sql::execution::followup",
            "crate::engine::sql::surfaces::registry",
            "rewrite_engine",
            "classify_statement(",
            "preprocess_with_surfaces_to_plan(",
        ] {
            assert!(
                !source.contains(forbidden),
                "sql2 must not depend on legacy rewrite/followup/classifier/planning code: {}",
                file.display()
            );
        }
    }
}

#[test]
fn guardrail_filesystem_public_surfaces_do_not_enter_legacy_surface_classifier() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let registry_source = fs::read_to_string(root.join("src/sql/surfaces/registry.rs"))
        .expect("registry.rs should be readable");

    assert!(
        !registry_source.contains("filesystem::planner::matches(statement)"),
        "filesystem public surfaces must not re-enter the legacy surface classifier"
    );
    assert!(
        !registry_source.contains("filesystem::lower::lowering_kind(statement)"),
        "filesystem public surfaces must not re-enter legacy filesystem lowering coverage"
    );
}

#[test]
fn guardrail_filesystem_public_surfaces_do_not_enter_legacy_query_rewrite() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let logical_views_source =
        fs::read_to_string(root.join(
            "src/sql/planning/rewrite_engine/pipeline/rules/query/canonical/logical_views.rs",
        ))
        .expect("logical_views.rs should be readable");

    assert!(
        !logical_views_source.contains("filesystem_views::rewrite_query"),
        "filesystem public surfaces must not re-enter legacy canonical query rewrite"
    );
}

#[test]
fn guardrail_legacy_filesystem_step_wrapper_is_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root
            .join("src/sql/planning/rewrite_engine/steps/filesystem_step.rs")
            .exists(),
        "legacy filesystem step wrapper must stay removed"
    );

    let canonical_source = fs::read_to_string(root.join(
        "src/sql/planning/rewrite_engine/pipeline/rules/statement/canonical/filesystem_write.rs",
    ))
    .expect("filesystem_write.rs should be readable");

    assert!(
        !canonical_source.contains("filesystem_step::"),
        "filesystem canonical write rule must call neutral filesystem runtime directly"
    );
}

#[test]
fn guardrail_vtable_read_stays_filesystem_blind() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let vtable_read_source =
        fs::read_to_string(root.join("src/sql/planning/rewrite_engine/steps/vtable_read.rs"))
            .expect("vtable_read.rs should be readable");

    for forbidden in ["lix_file", "lix_directory", "filesystem::"] {
        assert!(
            !vtable_read_source.contains(forbidden),
            "vtable_read must not reintroduce public filesystem bridge logic: {forbidden}"
        );
    }
}

#[test]
fn guardrail_legacy_filesystem_select_rewrite_is_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/filesystem/select_rewrite.rs").exists(),
        "legacy filesystem select rewrite must stay removed once sql2 owns filesystem reads"
    );

    let filesystem_mod_source =
        fs::read_to_string(root.join("src/filesystem/mod.rs")).expect("filesystem mod readable");
    assert!(
        !filesystem_mod_source.contains("select_rewrite"),
        "filesystem module tree must not re-export the removed select rewrite"
    );
}

#[test]
fn guardrail_dead_rewrite_engine_filesystem_coalescer_stays_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/sql/planning/rewrite_engine/rewrite.rs").exists(),
        "dead rewrite_engine filesystem coalescer must stay removed"
    );
}

#[test]
fn guardrail_dead_rewrite_engine_filesystem_analysis_stays_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/sql/planning/rewrite_engine/analysis.rs").exists(),
        "dead rewrite_engine filesystem analysis helper must stay removed"
    );
}

#[test]
fn guardrail_dead_canonical_filesystem_write_wrapper_stays_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/sql/planning/rewrite_engine/pipeline/rules/statement/canonical/filesystem_write.rs").exists(),
        "dead canonical filesystem write wrapper must stay removed"
    );
}

#[test]
fn guardrail_live_transaction_script_filesystem_coalescer_stays_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script_source = fs::read_to_string(root.join("src/sql/planning/script.rs"))
        .expect("script.rs should be readable");
    let scripts_source =
        fs::read_to_string(root.join("src/sql/scripts.rs")).expect("scripts.rs readable");
    let engine_source =
        fs::read_to_string(root.join("src/engine.rs")).expect("engine.rs should be readable");

    assert!(
        !script_source.contains("coalesce_lix_file_transaction_statements"),
        "live transaction-script filesystem coalescer must stay removed from script.rs"
    );
    assert!(
        !scripts_source.contains("coalesce_lix_file_transaction_statements"),
        "sql/scripts.rs must not call the removed lix_file transaction coalescer"
    );
    assert!(
        !engine_source.contains("coalesce_lix_file_transaction_statements"),
        "engine.rs tests must not pin the removed lix_file transaction coalescer"
    );
}

#[test]
fn guardrail_legacy_query_pipeline_context_and_validator_are_filesystem_blind() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let context_source = fs::read_to_string(
        root.join("src/sql/planning/rewrite_engine/pipeline/context.rs"),
    )
    .expect("context.rs should be readable");
    let validator_source = fs::read_to_string(
        root.join("src/sql/planning/rewrite_engine/pipeline/validator.rs"),
    )
    .expect("validator.rs should be readable");

    for forbidden in [
        "lix_file",
        "lix_file_by_version",
        "lix_file_history",
        "lix_file_history_by_version",
        "lix_directory",
        "lix_directory_by_version",
        "lix_directory_history",
        "FILESYSTEM_VIEW_NAMES",
        "references_any_filesystem_view",
    ] {
        assert!(
            !context_source.contains(forbidden),
            "legacy query rewrite context must not carry filesystem surface awareness: {forbidden}"
        );
        assert!(
            !validator_source.contains(forbidden),
            "legacy query rewrite validator must not carry filesystem surface awareness: {forbidden}"
        );
    }
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

#[test]
fn guardrail_side_effect_placeholder_advancement_is_ast_based() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let side_effects_source = fs::read_to_string(root.join("src/sql/side_effects.rs"))
        .expect("sql/side_effects.rs should be readable");

    assert!(
        side_effects_source.contains("advance_placeholder_state_for_statement_ast"),
        "side effects should advance placeholder state through AST visitor helper"
    );
    assert!(
        !side_effects_source.contains("bind_sql_with_state(&statement_sql"),
        "side effects must not rebind rendered statement SQL to advance placeholders"
    );
    assert!(
        !side_effects_source.contains("let statement_sql = statement.to_string();"),
        "side effects must not render statements to SQL text for placeholder advancement"
    );
}

#[test]
fn guardrail_live_filesystem_effects_do_not_carry_cache_invalidation_targets() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let effects_source =
        fs::read_to_string(root.join("src/sql/semantics/state_resolution/effects.rs"))
            .expect("effects.rs should be readable");

    assert!(
        !effects_source.contains("file_data_cache_invalidation_targets"),
        "live filesystem effects must not derive file-data-cache invalidation targets"
    );
    assert!(
        !effects_source.contains("file_path_cache_invalidation_targets"),
        "live filesystem effects must not derive file-path-cache invalidation targets"
    );
}

#[test]
fn guardrail_filesystem_data_only_updates_use_explicit_effect_only_rewrite() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let canonical_source = fs::read_to_string(
        root.join("src/sql/planning/rewrite_engine/pipeline/rules/statement/canonical/mod.rs"),
    )
    .expect("canonical statement rule source should be readable");
    let mutation_source = fs::read_to_string(root.join("src/filesystem/mutation_rewrite.rs"))
        .expect("mutation_rewrite.rs should be readable");

    assert!(
        canonical_source.contains("FilesystemUpdateRewrite::EffectOnly"),
        "canonical filesystem rewrite must branch on explicit effect-only updates"
    );
    assert!(
        mutation_source.contains("FilesystemUpdateRewrite::EffectOnly"),
        "filesystem mutation rewrite must emit explicit effect-only updates"
    );
    assert!(
        !canonical_source.contains("filesystem_noop_statement"),
        "canonical filesystem rewrite must not pattern-match fake no-op SQL"
    );
    assert!(
        !mutation_source.contains("failed to build filesystem no-op statement"),
        "filesystem mutation rewrite must not synthesize fake no-op SQL statements"
    );
}

#[test]
fn guardrail_live_filesystem_intent_path_has_no_plugin_detection_branch() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let side_effects_source = fs::read_to_string(root.join("src/sql/side_effects.rs"))
        .expect("side_effects.rs should be readable");
    let shared_path_source = fs::read_to_string(root.join("src/sql/execution/shared_path.rs"))
        .expect("shared_path.rs should be readable");
    let intent_source = fs::read_to_string(root.join("src/sql/execution/intent.rs"))
        .expect("intent.rs should be readable");

    assert!(
        !side_effects_source
            .contains("detect_file_changes_for_pending_writes_by_statement_with_backend"),
        "live filesystem side-effect collection must not retain a plugin detect branch"
    );
    assert!(
        !side_effects_source.contains("detect_file_changes_with_plugins_with_cache"),
        "live filesystem side-effect collection must not call plugin file detection"
    );
    assert!(
        !shared_path_source.contains("detect_plugin_file_changes"),
        "execution preparation must not thread filesystem plugin-detect options"
    );
    assert!(
        !shared_path_source.contains("allow_plugin_cache"),
        "execution preparation must not thread filesystem plugin-cache options"
    );
    assert!(
        !intent_source.contains("detect_plugin_file_changes"),
        "execution intent collection must not expose filesystem plugin-detect options"
    );
    assert!(
        !intent_source.contains("allow_plugin_cache"),
        "execution intent collection must not expose filesystem plugin-cache options"
    );
}
