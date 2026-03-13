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
fn guardrail_sql_runtime_forbids_legacy_public_lowering_imports() {
    let shared_path_source = fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/sql/execution/shared_path.rs"),
    )
    .expect("shared_path.rs should be readable");
    assert!(
        !shared_path_source.contains("crate::engine::sql2::"),
        "shared_path must not depend on removed engine::sql2 bridge paths"
    );
    assert!(
        shared_path_source.contains("prepare_public_execution_with_internal_access"),
        "shared_path must route public batches through the single public-lowering preparation entrypoint"
    );
    assert!(
        !shared_path_source.contains("prepare_sql2_read"),
        "shared_path must not reintroduce direct public-read bridge probing once public lowering owns dispatch"
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
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
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
fn guardrail_duplicate_public_surface_registry_is_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/lix_table_registry.rs").exists(),
        "duplicate public surface registry must stay removed once public lowering/catalog owns diagnostics"
    );
}

#[test]
fn guardrail_public_lowering_stays_isolated_from_legacy_rewrite_followup_and_classifier_modules() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/sql/public");
    let mut files = Vec::new();
    collect_rust_sources(&root, &mut files);

    for file in files {
        let source = fs::read_to_string(&file).expect("source file should be readable");
        for forbidden in [
            "crate::engine::sql::planning::",
            "crate::sql::ast::utils",
            "crate::engine::sql::execution::followup",
            "crate::engine::sql::surfaces",
            "rewrite_engine",
            "classify_statement(",
            "preprocess_with_surfaces_to_plan(",
        ] {
            assert!(
                !source.contains(forbidden),
                "public lowering must not depend on legacy rewrite/followup/classifier/planning code: {}",
                file.display()
            );
        }
    }
}

#[test]
fn guardrail_legacy_surface_registry_directory_is_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/sql/surfaces").exists(),
        "legacy sql/surfaces directory must stay removed"
    );
}

#[test]
fn guardrail_filesystem_public_surfaces_do_not_enter_legacy_query_rewrite() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/sql/planning/rewrite_engine").exists(),
        "legacy rewrite_engine directory must stay removed for migrated public reads"
    );
}

#[test]
fn guardrail_vtable_read_stays_filesystem_blind() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let vtable_read_source = fs::read_to_string(root.join("src/state/internal/vtable_read.rs"))
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
        "legacy filesystem select rewrite must stay removed once public lowering owns filesystem reads"
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
        !root.join("src/state/internal/rewrite.rs").exists(),
        "dead rewrite_engine filesystem coalescer must stay removed"
    );
}

#[test]
fn guardrail_dead_rewrite_engine_filesystem_analysis_stays_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/state/internal/analysis.rs").exists(),
        "dead rewrite_engine filesystem analysis helper must stay removed"
    );
}

#[test]
fn guardrail_dead_canonical_filesystem_write_wrapper_stays_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/state/internal/filesystem_write.rs").exists(),
        "dead canonical filesystem write wrapper must stay removed"
    );
}

#[test]
fn guardrail_legacy_canonical_statement_rewrite_is_filesystem_blind() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let canonical_mod_source =
        fs::read_to_string(root.join("src/state/internal/canonical_write.rs"))
            .expect("canonical_write.rs should be readable");

    for forbidden in [
        "mutation_rewrite::rewrite_insert(",
        "mutation_rewrite::rewrite_update(",
        "mutation_rewrite::rewrite_delete(",
        "mutation_rewrite::insert_side_effect_statements_with_backend(",
        "filesystem backend insert side-effect discovery failed",
        "filesystem backend insert rewrite failed",
        "filesystem/backend insert vtable lowering failed",
        "FilesystemUpdateRewrite",
    ] {
        assert!(
            !canonical_mod_source.contains(forbidden),
            "legacy canonical statement rewrite must not carry filesystem write branches: {forbidden}"
        );
    }
}

#[test]
fn guardrail_filesystem_insert_planning_stays_out_of_write_resolver() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let write_resolver =
        fs::read_to_string(root.join("src/sql/public/planner/semantics/write_resolver.rs"))
            .expect("write_resolver.rs should be readable");
    let filesystem_writes = fs::read_to_string(
        root.join("src/sql/public/planner/semantics/write_resolver/filesystem_writes.rs"),
    )
    .expect("filesystem_writes.rs should be readable");
    let filesystem_planning =
        fs::read_to_string(root.join("src/sql/public/planner/semantics/filesystem_planning.rs"))
            .expect("filesystem_planning.rs should be readable");

    for required in ["plan_directory_insert_batch(", "plan_file_insert_batch("] {
        assert!(
            filesystem_writes.contains(required),
            "filesystem_writes.rs must delegate filesystem insert planning via {required}"
        );
        assert!(
            !write_resolver.contains(required),
            "write_resolver.rs must not bypass filesystem_writes.rs for filesystem insert planning via {required}"
        );
    }

    for forbidden in [
        "PendingFilesystemInsertBatch",
        "resolve_file_insert_target(",
        "resolve_directory_insert_target(",
        "finalize_pending_directory_insert_batch(",
        "finalize_pending_file_insert_batch(",
        "ensure_parent_directories_for_insert_batch(",
        "lookup_directory_id_by_path_in_insert_batch(",
        "lookup_directory_path_by_id_in_insert_batch(",
        "ensure_no_file_at_directory_path_in_insert_batch(",
        "ensure_no_directory_at_file_path_in_insert_batch(",
    ] {
        assert!(
            !write_resolver.contains(forbidden),
            "write_resolver must not reintroduce filesystem insert planning helper {forbidden}"
        );
        assert!(
            filesystem_planning.contains(forbidden),
            "filesystem_planning.rs should own extracted helper {forbidden}"
        );
    }
}

#[test]
fn guardrail_generic_filesystem_queries_stay_out_of_write_resolver() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let write_resolver =
        fs::read_to_string(root.join("src/sql/public/planner/semantics/write_resolver.rs"))
            .expect("write_resolver.rs should be readable");
    let filesystem_queries =
        fs::read_to_string(root.join("src/sql/public/planner/semantics/filesystem_queries.rs"))
            .expect("filesystem_queries.rs should be readable");

    for forbidden_definition in [
        "async fn lookup_directory_id_by_path(",
        "async fn lookup_file_id_by_path(",
        "async fn lookup_directory_path_by_id(",
        "async fn load_directory_row_by_id(",
        "async fn load_directory_row_by_path(",
        "async fn load_file_row_by_id(",
        "async fn load_file_row_by_path(",
        "async fn load_directory_rows_under_path(",
        "async fn load_file_rows_under_path(",
        "async fn ensure_no_file_at_directory_path(",
        "async fn ensure_no_directory_at_file_path(",
        "struct DirectoryFilesystemRow",
        "struct FileFilesystemRow",
    ] {
        assert!(
            !write_resolver.contains(forbidden_definition),
            "write_resolver must not define extracted generic filesystem query helper {forbidden_definition}"
        );
        assert!(
            filesystem_queries.contains(forbidden_definition),
            "filesystem_queries.rs should own extracted helper {forbidden_definition}"
        );
    }
}

#[test]
fn guardrail_top_level_filesystem_write_coordination_stays_out_of_write_resolver() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let resolver_path = root.join("src/sql/public/planner/semantics/write_resolver.rs");
    let resolver_source =
        fs::read_to_string(&resolver_path).expect("write_resolver.rs should be readable");
    let filesystem_writes_path =
        root.join("src/sql/public/planner/semantics/write_resolver/filesystem_writes.rs");
    let filesystem_writes_source = fs::read_to_string(&filesystem_writes_path)
        .expect("filesystem_writes.rs should be readable");

    assert!(
        resolver_source.contains("use filesystem_writes::resolve_filesystem_write;"),
        "write_resolver should delegate top-level filesystem coordination through filesystem_writes.rs"
    );

    for forbidden in [
        "async fn resolve_filesystem_write(",
        "async fn resolve_directory_insert_write_plan(",
        "async fn resolve_existing_directory_write(",
        "async fn resolve_file_insert_write_plan(",
        "async fn resolve_existing_file_write(",
    ] {
        assert!(
            !resolver_source.contains(forbidden),
            "top-level filesystem write coordinator must stay out of write_resolver.rs: {forbidden}"
        );
        assert!(
            filesystem_writes_source.contains(forbidden),
            "filesystem_writes.rs should own extracted top-level filesystem coordinator helper: {forbidden}"
        );
    }
}

#[test]
fn guardrail_filesystem_helper_cluster_stays_out_of_write_resolver() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let resolver_path = root.join("src/sql/public/planner/semantics/write_resolver.rs");
    let resolver_source =
        fs::read_to_string(&resolver_path).expect("write_resolver.rs should be readable");
    let filesystem_writes_path =
        root.join("src/sql/public/planner/semantics/write_resolver/filesystem_writes.rs");
    let filesystem_writes_source = fs::read_to_string(&filesystem_writes_path)
        .expect("filesystem_writes.rs should be readable");

    for helper in [
        "async fn resolve_parent_directory_target(",
        "async fn resolve_missing_directory_rows(",
        "async fn resolve_file_update_target(",
        "fn file_descriptor_changed(",
        "async fn resolve_directory_update_target(",
        "struct ProposedDirectoryUpdate",
        "async fn resolve_directory_update_targets_batch(",
        "fn resolve_proposed_directory_path(",
        "async fn load_target_directory_rows_for_selector(",
        "async fn load_target_file_rows_for_selector(",
        "async fn assert_no_directory_cycle(",
        "fn directory_descriptor_row(",
        "fn file_descriptor_row(",
        "fn file_descriptor_tombstone_row(",
        "fn directory_descriptor_tombstone_row(",
        "fn binary_blob_ref_row(",
        "fn binary_blob_ref_tombstone_row(",
        "fn auto_directory_id(",
        "fn auto_file_id(",
    ] {
        assert!(
            !resolver_source.contains(helper),
            "filesystem helper cluster must stay out of write_resolver.rs: {helper}"
        );
        assert!(
            filesystem_writes_source.contains(helper),
            "filesystem_writes.rs should own extracted filesystem helper: {helper}"
        );
    }
}

#[test]
fn guardrail_effective_state_pushdown_policy_stays_in_surface_semantics() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let effective_state_resolver = fs::read_to_string(
        root.join("src/sql/public/planner/semantics/effective_state_resolver.rs"),
    )
    .expect("effective_state_resolver.rs should be readable");
    let surface_semantics =
        fs::read_to_string(root.join("src/sql/public/planner/semantics/surface_semantics.rs"))
            .expect("surface_semantics.rs should be readable");

    assert!(
        effective_state_resolver.contains("effective_state_pushdown_predicates("),
        "effective_state_resolver must delegate pushdown policy through surface_semantics"
    );

    for helper in [
        "fn state_predicate_is_pushdown_safe",
        "fn state_pushdown_column",
        "fn identifier_column_name",
        "fn constant_like_expr",
    ] {
        assert!(
            !effective_state_resolver.contains(helper),
            "effective_state_resolver must not redefine pushdown helper {helper}"
        );
        assert!(
            surface_semantics.contains(helper),
            "surface_semantics.rs should own extracted pushdown helper {helper}"
        );
    }
}

#[test]
fn guardrail_state_assignment_semantics_stay_out_of_write_resolver() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let write_resolver =
        fs::read_to_string(root.join("src/sql/public/planner/semantics/write_resolver.rs"))
            .expect("write_resolver.rs should be readable");
    let state_assignments =
        fs::read_to_string(root.join("src/sql/public/planner/semantics/state_assignments.rs"))
            .expect("state_assignments.rs should be readable");

    for required_use in [
        "assignments_from_payload(",
        "apply_state_assignments(",
        "apply_entity_state_assignments(",
        "build_state_insert_row(",
        "build_entity_insert_rows_from_assignments(",
        "ensure_identity_columns_preserved(",
    ] {
        assert!(
            write_resolver.contains(required_use),
            "write_resolver should delegate shared state semantics through {required_use}"
        );
    }

    for extracted_definition in [
        "fn assignments_from_payload(",
        "fn apply_state_assignments(",
        "fn build_state_insert_row(",
        "fn build_entity_insert_rows(",
        "fn ensure_identity_columns_preserved(",
        "fn apply_entity_state_assignments(",
    ] {
        assert!(
            !write_resolver.contains(extracted_definition),
            "write_resolver must not redefine shared state assignment helper {extracted_definition}"
        );
        assert!(
            state_assignments.contains(extracted_definition),
            "state_assignments.rs should own extracted helper {extracted_definition}"
        );
    }
}

#[test]
fn guardrail_exact_row_targeting_stays_shared_between_read_and_write() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let write_resolver =
        fs::read_to_string(root.join("src/sql/public/planner/semantics/write_resolver.rs"))
            .expect("write_resolver.rs should be readable");
    let effective_state_resolver = fs::read_to_string(
        root.join("src/sql/public/planner/semantics/effective_state_resolver.rs"),
    )
    .expect("effective_state_resolver.rs should be readable");
    let ir_mod = fs::read_to_string(root.join("src/sql/public/planner/ir/mod.rs"))
        .expect("planner ir mod should be readable");

    for required in [
        "CanonicalStateRowKey",
        "CanonicalStateSelector",
        "resolve_exact_effective_state_row(",
        "ExactEffectiveStateRowRequest {",
        "targets_single_effective_row(",
    ] {
        assert!(
            write_resolver.contains(required),
            "write_resolver must share canonical exact-row targeting through {required}"
        );
    }

    for required in [
        "ExactEffectiveStateRowRequest",
        "row_key: CanonicalStateRowKey",
        "resolve_exact_effective_state_row(",
    ] {
        assert!(
            effective_state_resolver.contains(required),
            "effective_state_resolver must share canonical exact-row targeting through {required}"
        );
    }

    assert!(
        ir_mod.contains("fn targets_single_effective_row(&self"),
        "CanonicalStateRowKey should remain the shared exact-row completeness check"
    );
}

#[test]
fn guardrail_filesystem_path_normalization_stays_in_filesystem_assignments() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let filesystem_assignments =
        fs::read_to_string(root.join("src/sql/public/planner/semantics/filesystem_assignments.rs"))
            .expect("filesystem_assignments.rs should be readable");
    let write_resolver =
        fs::read_to_string(root.join("src/sql/public/planner/semantics/write_resolver.rs"))
            .expect("write_resolver.rs should be readable");
    let filesystem_writes = fs::read_to_string(
        root.join("src/sql/public/planner/semantics/write_resolver/filesystem_writes.rs"),
    )
    .expect("filesystem_writes.rs should be readable");
    let filesystem_planning =
        fs::read_to_string(root.join("src/sql/public/planner/semantics/filesystem_planning.rs"))
            .expect("filesystem_planning.rs should be readable");
    let filesystem_queries =
        fs::read_to_string(root.join("src/sql/public/planner/semantics/filesystem_queries.rs"))
            .expect("filesystem_queries.rs should be readable");

    for helper in [
        "normalize_directory_path(",
        "normalize_path_segment(",
        "parse_file_path(",
    ] {
        assert!(
            filesystem_assignments.contains(helper),
            "filesystem_assignments.rs should own path normalization helper {helper}"
        );
        for (label, source) in [
            ("write_resolver.rs", &write_resolver),
            ("filesystem_writes.rs", &filesystem_writes),
            ("filesystem_planning.rs", &filesystem_planning),
            ("filesystem_queries.rs", &filesystem_queries),
        ] {
            assert!(
                !source.contains(helper),
                "{label} must not reintroduce filesystem path normalization helper {helper}"
            );
        }
    }
}

#[test]
fn guardrail_legacy_filesystem_mutation_rewrite_is_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/filesystem/mutation_rewrite.rs").exists(),
        "legacy filesystem mutation rewrite must stay removed once public lowering owns filesystem writes"
    );

    let filesystem_mod_source =
        fs::read_to_string(root.join("src/filesystem/mod.rs")).expect("filesystem mod readable");
    assert!(
        !filesystem_mod_source.contains("mutation_rewrite"),
        "filesystem module tree must not re-export the removed legacy mutation rewrite"
    );
}

#[test]
fn guardrail_sql_side_effects_stays_off_legacy_filesystem_update_detector() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let side_effects_source = fs::read_to_string(root.join("src/sql/execution/runtime_effects.rs"))
        .expect("runtime_effects readable");
    let intent_source =
        fs::read_to_string(root.join("src/sql/execution/intent.rs")).expect("intent readable");
    let shared_path_source = fs::read_to_string(root.join("src/sql/execution/shared_path.rs"))
        .expect("shared_path readable");

    for forbidden in [
        "mutation_rewrite::update_side_effects_with_backend(",
        "skip_legacy_filesystem_update_side_effect_detection",
        "collect_filesystem_update_detected_file_domain_changes_from_statements",
    ] {
        assert!(
            !side_effects_source.contains(forbidden),
            "runtime effects must not keep legacy filesystem update detector plumbing: {forbidden}"
        );
        assert!(
            !intent_source.contains(forbidden),
            "execution intent must not keep legacy filesystem update detector plumbing: {forbidden}"
        );
        assert!(
            !shared_path_source.contains(forbidden),
            "shared_path must not keep legacy filesystem update detector plumbing: {forbidden}"
        );
    }
}

#[test]
fn guardrail_legacy_query_pipeline_context_is_removed_and_validator_is_filesystem_blind() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !root.join("src/sql/planning/rewrite_engine").exists(),
        "legacy rewrite_engine directory should be removed"
    );
}

#[test]
fn guardrail_side_effect_placeholder_advancement_is_ast_based() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let ast_utils_source = fs::read_to_string(root.join("src/sql/ast/utils.rs"))
        .expect("sql_ast/utils.rs should be readable");

    assert!(
        ast_utils_source.contains("advance_placeholder_state_for_statement_ast"),
        "placeholder advancement should stay on the AST visitor helper"
    );
    assert!(
        !ast_utils_source.contains("bind_sql_with_state(&statement_sql"),
        "placeholder advancement must not rebind rendered statement SQL"
    );
    assert!(
        !ast_utils_source.contains("let statement_sql = statement.to_string();"),
        "placeholder advancement must not render statements to SQL text"
    );
}

#[test]
fn guardrail_live_filesystem_effects_do_not_carry_cache_invalidation_targets() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let effects_source =
        fs::read_to_string(root.join("src/sql/analysis/state_resolution/effects.rs"))
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
fn guardrail_filesystem_noop_sql_synthesis_stays_removed() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rust_sources(&root, &mut files);

    for file in files {
        let source = fs::read_to_string(&file).expect("source file should be readable");
        for forbidden in [
            "filesystem_noop_statement",
            "failed to build filesystem no-op statement",
        ] {
            assert!(
                !source.contains(forbidden),
                "filesystem runtime must not synthesize fake no-op SQL statements: {}",
                file.display()
            );
        }
    }
}

#[test]
fn guardrail_live_filesystem_intent_path_has_no_plugin_detection_branch() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let side_effects_source = fs::read_to_string(root.join("src/sql/execution/runtime_effects.rs"))
        .expect("runtime_effects.rs should be readable");
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
