use lix_engine::Value;
use serde_json::{Map, Value as JsonValue};

fn explain_json_payload(result: &lix_engine::ExecuteResult) -> &serde_json::Value {
    let Value::Json(explain_json) = &result.statements[0].rows[0][0] else {
        panic!("EXPLAIN (FORMAT JSON) should return a JSON payload");
    };
    explain_json
}

fn explain_text_rows(result: &lix_engine::ExecuteResult) -> Vec<(&str, &str)> {
    result.statements[0]
        .rows
        .iter()
        .map(|row| {
            let Value::Text(key) = &row[0] else {
                panic!("FORMAT TEXT should expose string explain keys");
            };
            let Value::Text(value) = &row[1] else {
                panic!("FORMAT TEXT should expose string explain values");
            };
            (key.as_str(), value.as_str())
        })
        .collect()
}

fn explain_text_section<'a>(result: &'a lix_engine::ExecuteResult, key: &str) -> &'a str {
    explain_text_rows(result)
        .into_iter()
        .find_map(|(candidate_key, value)| (candidate_key == key).then_some(value))
        .unwrap_or_else(|| panic!("FORMAT TEXT should include {key}"))
}

fn explain_stage_names(explain_json: &serde_json::Value) -> Vec<String> {
    explain_json
        .get("stage_timings")
        .and_then(|value| value.as_array())
        .expect("stage_timings should be a JSON array")
        .iter()
        .map(|timing| {
            timing
                .get("stage")
                .and_then(|value| value.as_str())
                .expect("each stage timing should expose a stage name")
                .to_string()
        })
        .collect()
}

fn json_object<'a>(value: &'a JsonValue, context: &str) -> &'a Map<String, JsonValue> {
    value
        .as_object()
        .unwrap_or_else(|| panic!("{context} should be a JSON object"))
}

fn json_array<'a>(value: &'a JsonValue, context: &str) -> &'a Vec<JsonValue> {
    value
        .as_array()
        .unwrap_or_else(|| panic!("{context} should be a JSON array"))
}

fn json_object_at<'a>(
    value: &'a JsonValue,
    key: &str,
    context: &str,
) -> &'a Map<String, JsonValue> {
    json_object(
        value
            .get(key)
            .unwrap_or_else(|| panic!("{context} should include {key}")),
        key,
    )
}

fn json_array_at<'a>(value: &'a JsonValue, key: &str, context: &str) -> &'a Vec<JsonValue> {
    json_array(
        value
            .get(key)
            .unwrap_or_else(|| panic!("{context} should include {key}")),
        key,
    )
}

fn json_bool_at(value: &JsonValue, key: &str, context: &str) -> bool {
    value
        .get(key)
        .and_then(JsonValue::as_bool)
        .unwrap_or_else(|| panic!("{context}.{key} should be a bool"))
}

fn assert_string_array(value: &JsonValue, expected: &[&str], context: &str) {
    let actual = json_array(value, context)
        .iter()
        .map(|value| {
            value
                .as_str()
                .unwrap_or_else(|| panic!("{context} items should be strings"))
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{context} should match the contract");
}

fn assert_object_keys(object: &Map<String, JsonValue>, expected: &[&str], context: &str) {
    let mut actual = object.keys().map(String::as_str).collect::<Vec<_>>();
    actual.sort_unstable();
    let mut expected = expected.to_vec();
    expected.sort_unstable();
    assert_eq!(actual, expected, "{context} keys should match the contract");
}

fn assert_stage_timings_contract(explain_json: &JsonValue, expected: &[&str]) {
    let timings = json_array_at(explain_json, "stage_timings", "explain_json");
    assert_eq!(
        timings.len(),
        expected.len(),
        "stage_timings length should match the expected compile stages"
    );

    for (timing, expected_stage) in timings.iter().zip(expected.iter().copied()) {
        let timing_object = json_object(timing, "stage_timing");
        assert_object_keys(timing_object, &["duration_us", "stage"], "stage_timing");
        assert_eq!(
            timing_object.get("stage").and_then(JsonValue::as_str),
            Some(expected_stage),
            "stage ordering should match execution order"
        );
        assert!(
            timing_object
                .get("duration_us")
                .and_then(JsonValue::as_u64)
                .is_some(),
            "stage_timing.duration_us should be a u64"
        );
    }
}

fn assert_missing_stage_names(explain_json: &JsonValue, missing: &[&str]) {
    let stage_names = explain_stage_names(explain_json);
    for stage in missing {
        assert!(
            !stage_names.iter().any(|candidate| candidate == stage),
            "stage_timings should omit {stage}"
        );
    }
}

fn assert_text_explain_contract(
    result: &lix_engine::ExecuteResult,
    required_keys: &[&str],
    required_markers: &[(&str, &[&str])],
) {
    assert_eq!(
        result.statements[0].columns,
        vec!["explain_key".to_string(), "explain_value".to_string()]
    );

    let rows = explain_text_rows(result);
    assert!(
        !rows.is_empty(),
        "FORMAT TEXT should return at least one explain section"
    );

    let keys = rows.iter().map(|(key, _)| *key).collect::<Vec<_>>();
    for required_key in required_keys {
        assert!(
            keys.iter().any(|candidate| candidate == required_key),
            "FORMAT TEXT should include {required_key}"
        );
    }

    for (key, value) in &rows {
        assert!(
            !value.trim().is_empty(),
            "FORMAT TEXT row {key} should contain human-readable text"
        );
        assert!(
            serde_json::from_str::<JsonValue>(value).is_err(),
            "FORMAT TEXT row {key} should not be raw JSON"
        );
    }

    for (key, markers) in required_markers {
        let value = rows
            .iter()
            .find_map(|(candidate_key, value)| (*candidate_key == *key).then_some(*value))
            .unwrap_or_else(|| panic!("FORMAT TEXT should include {key}"));
        for marker in *markers {
            assert!(
                value.contains(marker),
                "FORMAT TEXT row {key} should contain marker {marker}: {value}"
            );
        }
    }
}

fn assert_no_rust_debug_leaks(explain_json: &JsonValue) {
    let serialized = serde_json::to_string(explain_json).expect("explain JSON should serialize");
    for forbidden in [
        "Some(",
        "None",
        "PublicRead",
        "PublicWrite",
        "ReadWrite",
        "ReadOnly",
        "Default",
        "ByVersion",
        "History",
        "WorkingChanges",
        "SemanticAnalysis",
        "LogicalPlanning",
        "PhysicalPlanning",
        "CapabilityResolution",
        "ExecutorPreparation",
        "PushdownSupport",
        "SurfaceFamily",
        "SurfaceVariant",
        "SurfaceCapability",
        "PreparedPublicReadExecution",
        "InternalLogicalPlan",
    ] {
        assert!(
            !serialized.contains(forbidden),
            "explain JSON should not contain Rust debug fragment {forbidden}: {serialized}"
        );
    }
}

fn assert_explain_request_json(
    result: &lix_engine::ExecuteResult,
    expected_mode: &str,
    expected_format: &str,
) {
    assert_eq!(
        result.statements[0].columns,
        vec!["explain_json".to_string()],
        "FORMAT JSON should expose a single explain_json column"
    );
    let request = json_object_at(explain_json_payload(result), "request", "explain_json");
    assert_eq!(
        request.get("mode").and_then(JsonValue::as_str),
        Some(expected_mode)
    );
    assert_eq!(
        request.get("format").and_then(JsonValue::as_str),
        Some(expected_format)
    );
}

fn assert_explain_request_text(
    result: &lix_engine::ExecuteResult,
    expected_mode: &str,
    expected_format: &str,
) {
    assert_eq!(
        result.statements[0].columns,
        vec!["explain_key".to_string(), "explain_value".to_string()],
        "FORMAT TEXT should expose explain_key/explain_value rows"
    );
    let request = explain_text_section(result, "request");
    assert!(
        request.contains(&format!("mode: {expected_mode}")),
        "FORMAT TEXT request should contain mode: {expected_mode}: {request}"
    );
    assert!(
        request.contains(&format!("format: {expected_format}")),
        "FORMAT TEXT request should contain format: {expected_format}: {request}"
    );
}

fn assert_broad_public_read_statement_snapshot(
    broad_statement: &JsonValue,
    expected_primary_kind: &str,
    expected_primary_surface: &str,
    expected_join_kind: &str,
    expected_join_surface: &str,
) {
    let statement = json_object(broad_statement, "broad_statement");
    assert_eq!(
        statement.get("kind").and_then(JsonValue::as_str),
        Some("query")
    );
    let statement_details = statement
        .get("details")
        .map(|value| json_object(value, "broad_statement.details"))
        .expect("broad_statement should include details");
    let body = statement_details
        .get("body")
        .map(|value| json_object(value, "broad_statement.details.body"))
        .expect("broad_statement.details should include body");
    assert_eq!(body.get("kind").and_then(JsonValue::as_str), Some("select"));
    let body_details = body
        .get("details")
        .map(|value| json_object(value, "broad_statement.details.body.details"))
        .expect("broad_statement.details.body should include details");
    let from = body_details
        .get("from")
        .map(|value| json_array(value, "broad_statement.details.body.details.from"))
        .expect("broad_statement.details.body.details should include from");
    assert_eq!(
        from.len(),
        1,
        "broad select should expose one root relation"
    );

    let root = json_object(&from[0], "broad_statement.from[0]");
    let root_relation = root
        .get("relation")
        .map(|value| json_object(value, "broad_statement.from[0].relation"))
        .expect("broad root should include relation");
    assert_eq!(
        root_relation.get("kind").and_then(JsonValue::as_str),
        Some("table")
    );
    let root_relation_details = root_relation
        .get("details")
        .map(|value| json_object(value, "broad_statement.from[0].relation.details"))
        .expect("broad root relation should include details");
    let root_surface = root_relation_details
        .get("relation")
        .map(|value| json_object(value, "broad_statement.from[0].relation.details.relation"))
        .expect("broad root relation should include relation");
    assert_eq!(
        root_surface.get("kind").and_then(JsonValue::as_str),
        Some(expected_primary_kind)
    );
    let root_surface_details = root_surface
        .get("details")
        .map(|value| {
            json_object(
                value,
                "broad_statement.from[0].relation.details.relation.details",
            )
        })
        .expect("broad root public relation should include details");
    assert_eq!(
        root_surface_details
            .get("public_name")
            .and_then(JsonValue::as_str),
        Some(expected_primary_surface)
    );

    let joins = root
        .get("joins")
        .map(|value| json_array(value, "broad_statement.from[0].joins"))
        .expect("broad root should include joins");
    assert_eq!(joins.len(), 1, "broad root should expose one join");
    let join = json_object(&joins[0], "broad_statement.from[0].joins[0]");
    assert_eq!(
        join.get("operator").and_then(JsonValue::as_str),
        Some("join")
    );
    let join_relation = join
        .get("relation")
        .map(|value| json_object(value, "broad_statement.from[0].joins[0].relation"))
        .expect("broad join should include relation");
    assert_eq!(
        join_relation.get("kind").and_then(JsonValue::as_str),
        Some("table")
    );
    let join_relation_details = join_relation
        .get("details")
        .map(|value| json_object(value, "broad_statement.from[0].joins[0].relation.details"))
        .expect("broad join relation should include details");
    let join_surface = join_relation_details
        .get("relation")
        .map(|value| {
            json_object(
                value,
                "broad_statement.from[0].joins[0].relation.details.relation",
            )
        })
        .expect("broad join relation should include relation");
    assert_eq!(
        join_surface.get("kind").and_then(JsonValue::as_str),
        Some(expected_join_kind)
    );
    let join_surface_details = join_surface
        .get("details")
        .map(|value| {
            json_object(
                value,
                "broad_statement.from[0].joins[0].relation.details.relation.details",
            )
        })
        .expect("broad join public relation should include details");
    assert_eq!(
        join_surface_details
            .get("public_name")
            .and_then(JsonValue::as_str),
        Some(expected_join_surface)
    );
}

fn assert_broad_public_read_typed_statement_contract(explain_json: &JsonValue) {
    let semantic_statement = json_object_at(explain_json, "semantic_statement", "explain_json");
    let semantic_details = semantic_statement
        .get("details")
        .map(|value| json_object(value, "semantic_statement.details"))
        .expect("semantic_statement should include details");
    assert_broad_public_read_statement_snapshot(
        semantic_details
            .get("broad_statement")
            .expect("semantic_statement.details should include broad_statement"),
        "public",
        "lix_state",
        "public",
        "lix_state_by_version",
    );

    let logical_plan = json_object_at(explain_json, "logical_plan", "explain_json");
    let logical_details = logical_plan
        .get("details")
        .map(|value| json_object(value, "logical_plan.details"))
        .expect("logical_plan should include details");
    assert_broad_public_read_statement_snapshot(
        logical_details
            .get("broad_statement")
            .expect("logical_plan.details should include broad_statement"),
        "public",
        "lix_state",
        "public",
        "lix_state_by_version",
    );
    assert_broad_public_read_relation_summary(
        logical_details
            .get("broad_relation_summary")
            .expect("logical_plan.details should include broad_relation_summary"),
        &["lix_state", "lix_state_by_version"],
        &[],
    );

    let optimized_logical_plan =
        json_object_at(explain_json, "optimized_logical_plan", "explain_json");
    let optimized_details = optimized_logical_plan
        .get("details")
        .map(|value| json_object(value, "optimized_logical_plan.details"))
        .expect("optimized_logical_plan should include details");
    assert_broad_public_read_statement_snapshot(
        optimized_details
            .get("broad_statement")
            .expect("optimized_logical_plan.details should include broad_statement"),
        "lowered_public",
        "lix_state",
        "lowered_public",
        "lix_state_by_version",
    );
    assert_broad_public_read_relation_summary(
        optimized_details
            .get("broad_relation_summary")
            .expect("optimized_logical_plan.details should include broad_relation_summary"),
        &[],
        &["lix_state", "lix_state_by_version"],
    );
    assert_ne!(
        logical_details.get("broad_statement"),
        optimized_details.get("broad_statement"),
        "broad logical_plan and optimized_logical_plan should stay distinct when optimization rewrites the broad IR"
    );
    assert_ne!(
        logical_details.get("broad_relation_summary"),
        optimized_details.get("broad_relation_summary"),
        "broad logical summaries should differ when optimization rewrites public relations"
    );
    assert_broad_public_read_physical_execution_contract(explain_json);
}

fn assert_broad_public_read_relation_summary(
    summary: &JsonValue,
    expected_public_relations: &[&str],
    expected_lowered_public_relations: &[&str],
) {
    let summary = json_object(summary, "broad_relation_summary");
    assert_object_keys(
        summary,
        &[
            "cte_relations",
            "external_relations",
            "internal_relations",
            "lowered_public_relations",
            "public_relations",
        ],
        "broad_relation_summary",
    );
    assert_string_array(
        summary
            .get("public_relations")
            .expect("broad_relation_summary should include public_relations"),
        expected_public_relations,
        "broad_relation_summary.public_relations",
    );
    assert_string_array(
        summary
            .get("lowered_public_relations")
            .expect("broad_relation_summary should include lowered_public_relations"),
        expected_lowered_public_relations,
        "broad_relation_summary.lowered_public_relations",
    );
    assert_string_array(
        summary
            .get("internal_relations")
            .expect("broad_relation_summary should include internal_relations"),
        &[],
        "broad_relation_summary.internal_relations",
    );
    assert_string_array(
        summary
            .get("external_relations")
            .expect("broad_relation_summary should include external_relations"),
        &[],
        "broad_relation_summary.external_relations",
    );
    assert_string_array(
        summary
            .get("cte_relations")
            .expect("broad_relation_summary should include cte_relations"),
        &[],
        "broad_relation_summary.cte_relations",
    );
}

fn assert_broad_public_read_physical_execution_contract(explain_json: &JsonValue) {
    let logical_details = json_object_at(explain_json, "logical_plan", "explain_json")
        .get("details")
        .map(|value| json_object(value, "logical_plan.details"))
        .expect("logical_plan should include details");
    let optimized_details = json_object_at(explain_json, "optimized_logical_plan", "explain_json")
        .get("details")
        .map(|value| json_object(value, "optimized_logical_plan.details"))
        .expect("optimized_logical_plan should include details");
    assert!(
        logical_details.get("shell_statement_sql").is_none(),
        "logical broad artifacts should not expose physical shell SQL"
    );
    assert!(
        optimized_details.get("shell_statement_sql").is_none(),
        "optimized logical broad artifacts should not expose physical shell SQL"
    );
    assert!(
        logical_details.get("relation_render_nodes").is_none(),
        "logical broad artifacts should not expose terminal relation render nodes"
    );
    assert!(
        optimized_details.get("relation_render_nodes").is_none(),
        "optimized logical broad artifacts should not expose terminal relation render nodes"
    );

    let physical_details = json_object_at(explain_json, "physical_plan", "explain_json")
        .get("details")
        .map(|value| json_object(value, "physical_plan.details"))
        .expect("physical_plan should include details");
    assert_eq!(
        physical_details.get("kind").and_then(JsonValue::as_str),
        Some("lowered_sql")
    );
    let lowered_program = physical_details
        .get("details")
        .map(|value| json_object(value, "physical_plan.details.details"))
        .expect("physical_plan.details should include lowered_sql details");
    assert_object_keys(
        lowered_program,
        &["pushdown_decision", "result_columns", "statements"],
        "physical_plan.details.details",
    );

    let statements = json_array(
        lowered_program
            .get("statements")
            .expect("physical_plan.details.details should include statements"),
        "physical_plan.details.details.statements",
    );
    assert_eq!(statements.len(), 1);
    let statement = json_object(
        &statements[0],
        "physical_plan.details.details.statements[0]",
    );
    assert_object_keys(
        statement,
        &["bindings", "relation_render_nodes", "shell_statement_sql"],
        "physical_plan.details.details.statements[0]",
    );
    let shell_statement_sql = statement
        .get("shell_statement_sql")
        .and_then(JsonValue::as_str)
        .expect("physical broad plan should expose shell_statement_sql");
    assert!(
        shell_statement_sql.contains("__lix_lowered_relation_"),
        "physical broad shell should retain placeholder relation markers before executor SQL rendering"
    );

    let relation_render_nodes = json_array(
        statement
            .get("relation_render_nodes")
            .expect("physical broad plan should expose relation_render_nodes"),
        "physical_plan.details.details.statements[0].relation_render_nodes",
    );
    assert!(
        !relation_render_nodes.is_empty(),
        "physical broad plan should retain terminal relation render nodes"
    );
    let render_node = json_object(
        &relation_render_nodes[0],
        "physical_plan.details.details.statements[0].relation_render_nodes[0]",
    );
    assert_object_keys(
        render_node,
        &["alias", "placeholder_relation_name", "rendered_factor_sql"],
        "physical_plan.details.details.statements[0].relation_render_nodes[0]",
    );
    let rendered_factor_sql = render_node
        .get("rendered_factor_sql")
        .and_then(JsonValue::as_str)
        .expect("physical broad render node should expose rendered_factor_sql");
    assert!(
        rendered_factor_sql.contains("lix_internal_live_v1_lix_key_value"),
        "physical broad render nodes should carry lowered backend SQL fragments"
    );
}

fn assert_public_read_json_contract(explain_json: &JsonValue) {
    let request = json_object_at(explain_json, "request", "explain_json");
    assert_object_keys(request, &["format", "mode"], "request");
    assert_eq!(
        request.get("mode").and_then(JsonValue::as_str),
        Some("plan")
    );
    assert_eq!(
        request.get("format").and_then(JsonValue::as_str),
        Some("json")
    );

    let semantic_statement = json_object_at(explain_json, "semantic_statement", "explain_json");
    assert_object_keys(
        semantic_statement,
        &["details", "kind"],
        "semantic_statement",
    );
    assert_eq!(
        semantic_statement.get("kind").and_then(JsonValue::as_str),
        Some("public_read")
    );
    let semantic_details = semantic_statement
        .get("details")
        .map(|value| json_object(value, "semantic_statement.details"))
        .expect("semantic_statement should include details");
    let surface_bindings = json_array(
        semantic_details
            .get("surface_bindings")
            .expect("semantic_statement.details should include surface_bindings"),
        "surface_bindings",
    );
    assert_eq!(surface_bindings.len(), 1);
    let surface_binding = json_object(&surface_bindings[0], "surface_binding");
    assert_object_keys(
        surface_binding,
        &[
            "capability",
            "default_scope",
            "expose_version_id",
            "exposed_columns",
            "fixed_schema_key",
            "hidden_columns",
            "public_name",
            "read_freshness",
            "surface_family",
            "surface_variant",
            "visible_columns",
        ],
        "surface_binding",
    );
    assert_eq!(
        surface_binding
            .get("public_name")
            .and_then(JsonValue::as_str),
        Some("lix_state")
    );
    assert_eq!(
        surface_binding
            .get("surface_family")
            .and_then(JsonValue::as_str),
        Some("state")
    );
    assert_eq!(
        surface_binding
            .get("surface_variant")
            .and_then(JsonValue::as_str),
        Some("default")
    );
    assert_eq!(
        surface_binding
            .get("capability")
            .and_then(JsonValue::as_str),
        Some("read_write")
    );
    assert_eq!(
        surface_binding
            .get("read_freshness")
            .and_then(JsonValue::as_str),
        Some("requires_fresh_projection")
    );
    assert_eq!(
        surface_binding
            .get("default_scope")
            .and_then(JsonValue::as_str),
        Some("active_version")
    );
    assert!(!json_bool_at(
        &surface_bindings[0],
        "expose_version_id",
        "surface_binding"
    ));
    let effective_state_request = semantic_details
        .get("effective_state_request")
        .map(|value| json_object(value, "semantic_statement.details.effective_state_request"))
        .expect("semantic_statement.details should include effective_state_request");
    assert_object_keys(
        effective_state_request,
        &[
            "include_global_overlay",
            "include_tombstones",
            "include_untracked_overlay",
            "predicate_classes",
            "required_columns",
            "schema_set",
            "version_scope",
        ],
        "effective_state_request",
    );
    assert_eq!(
        effective_state_request
            .get("version_scope")
            .and_then(JsonValue::as_str),
        Some("active_version")
    );
    let effective_state_plan = semantic_details
        .get("effective_state_plan")
        .map(|value| json_object(value, "semantic_statement.details.effective_state_plan"))
        .expect("semantic_statement.details should include effective_state_plan");
    assert_object_keys(
        effective_state_plan,
        &[
            "overlay_lanes",
            "pushdown_safe_predicates",
            "residual_predicates",
            "state_source",
        ],
        "effective_state_plan",
    );
    assert_eq!(
        effective_state_plan
            .get("state_source")
            .and_then(JsonValue::as_str),
        Some("authoritative_committed")
    );

    let logical_plan = json_object_at(explain_json, "logical_plan", "explain_json");
    assert_eq!(
        logical_plan.get("kind").and_then(JsonValue::as_str),
        Some("public_read")
    );
    let logical_details = logical_plan
        .get("details")
        .map(|value| json_object(value, "logical_plan.details"))
        .expect("logical_plan should include details");
    let dependency_spec = logical_details
        .get("dependency_spec")
        .map(|value| json_object(value, "logical_plan.details.dependency_spec"))
        .expect("logical_plan.details should include dependency_spec");
    assert_object_keys(
        dependency_spec,
        &[
            "depends_on_active_version",
            "entity_ids",
            "file_ids",
            "include_untracked",
            "precision",
            "relations",
            "schema_keys",
            "session_dependencies",
            "version_ids",
            "writer_filter",
        ],
        "dependency_spec",
    );
    assert_eq!(
        dependency_spec.get("precision").and_then(JsonValue::as_str),
        Some("precise")
    );
    let optimized_logical_plan =
        json_object_at(explain_json, "optimized_logical_plan", "explain_json");
    assert_eq!(
        optimized_logical_plan
            .get("kind")
            .and_then(JsonValue::as_str),
        Some("public_read")
    );
    let physical_plan = json_object_at(explain_json, "physical_plan", "explain_json");
    assert_object_keys(physical_plan, &["details", "kind"], "physical_plan");
    assert_eq!(
        physical_plan.get("kind").and_then(JsonValue::as_str),
        Some("public_read")
    );
    let physical_details = physical_plan
        .get("details")
        .map(|value| json_object(value, "physical_plan.details"))
        .expect("physical_plan should include details");
    assert_object_keys(
        physical_details,
        &["details", "kind"],
        "physical_plan.details",
    );
    assert_eq!(
        physical_details.get("kind").and_then(JsonValue::as_str),
        Some("lowered_sql")
    );

    let executor_artifacts = json_object_at(explain_json, "executor_artifacts", "explain_json");
    assert!(
        executor_artifacts.get("surface_bindings").is_none(),
        "executor_artifacts should not duplicate typed surface bindings as top-level names"
    );
    let bound_public_leaves = json_array(
        executor_artifacts
            .get("bound_public_leaves")
            .expect("executor_artifacts should include bound_public_leaves"),
        "bound_public_leaves",
    );
    assert_eq!(bound_public_leaves.len(), 1);
    let bound_public_leaf = json_object(&bound_public_leaves[0], "bound_public_leaf");
    assert_object_keys(
        bound_public_leaf,
        &[
            "capability",
            "public_name",
            "requires_effective_state",
            "surface_family",
            "surface_variant",
        ],
        "bound_public_leaf",
    );
    assert_eq!(
        bound_public_leaf
            .get("public_name")
            .and_then(JsonValue::as_str),
        Some("lix_state")
    );
    assert_eq!(
        bound_public_leaf
            .get("surface_family")
            .and_then(JsonValue::as_str),
        Some("state")
    );
    assert_eq!(
        bound_public_leaf
            .get("surface_variant")
            .and_then(JsonValue::as_str),
        Some("default")
    );
    assert_eq!(
        bound_public_leaf
            .get("capability")
            .and_then(JsonValue::as_str),
        Some("read_write")
    );
    assert!(json_bool_at(
        &bound_public_leaves[0],
        "requires_effective_state",
        "bound_public_leaf"
    ));

    let pushdown = executor_artifacts
        .get("pushdown")
        .map(|value| json_object(value, "executor_artifacts.pushdown"))
        .expect("executor_artifacts should include pushdown");
    assert_object_keys(
        pushdown,
        &[
            "accepted_predicates",
            "rejected_predicates",
            "residual_predicates",
        ],
        "pushdown",
    );
    assert_eq!(
        pushdown
            .get("accepted_predicates")
            .and_then(JsonValue::as_array)
            .cloned(),
        Some(vec![JsonValue::String(
            "schema_key = 'lix_key_value'".to_string()
        )])
    );
    assert_eq!(
        pushdown
            .get("rejected_predicates")
            .and_then(JsonValue::as_array)
            .map(Vec::len),
        Some(0)
    );
    assert_eq!(
        pushdown
            .get("residual_predicates")
            .and_then(JsonValue::as_array)
            .map(Vec::len),
        Some(0)
    );

    let lowered_sql = executor_artifacts
        .get("lowered_sql")
        .and_then(JsonValue::as_array)
        .and_then(|values| values.first())
        .and_then(JsonValue::as_str)
        .expect("executor_artifacts.lowered_sql should expose the lowered query");
    assert!(!lowered_sql.starts_with("EXPLAIN "));
    assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));

    assert_no_rust_debug_leaks(explain_json);
}

fn assert_public_read_logical_strategy(
    explain_json: &JsonValue,
    plan_key: &str,
    expected_strategy: &str,
) {
    let plan = json_object_at(explain_json, plan_key, "explain_json");
    assert_eq!(
        plan.get("kind").and_then(JsonValue::as_str),
        Some("public_read")
    );
    let details = plan
        .get("details")
        .map(|value| json_object(value, "public_read_logical_plan.details"))
        .expect("public read logical plan should include details");
    assert_eq!(
        details.get("strategy").and_then(JsonValue::as_str),
        Some(expected_strategy),
        "{plan_key} should expose strategy {expected_strategy}"
    );
}

fn assert_entity_history_direct_plan_contract(explain_json: &JsonValue) {
    let optimized_plan = json_object_at(explain_json, "optimized_logical_plan", "explain_json");
    let optimized_details = optimized_plan
        .get("details")
        .map(|value| json_object(value, "optimized_logical_plan.details"))
        .expect("optimized_logical_plan should include details");
    let direct_plan = optimized_details
        .get("direct_plan")
        .map(|value| json_object(value, "optimized_logical_plan.details.direct_plan"))
        .expect("optimized_logical_plan should include a direct_plan");
    assert_eq!(
        direct_plan.get("kind").and_then(JsonValue::as_str),
        Some("entity_history")
    );
    let direct_details = direct_plan
        .get("details")
        .map(|value| json_object(value, "optimized_logical_plan.details.direct_plan.details"))
        .expect("direct_plan should include details");
    assert_object_keys(
        direct_details,
        &[
            "limit",
            "offset",
            "predicates",
            "projections",
            "request",
            "result_columns",
            "sort_keys",
            "surface_binding",
            "wildcard_columns",
            "wildcard_projection",
        ],
        "entity_history_direct_plan",
    );

    let predicates = json_array(
        direct_details
            .get("predicates")
            .expect("entity_history_direct_plan should include predicates"),
        "entity_history_direct_plan.predicates",
    );
    let predicate = predicates
        .iter()
        .find_map(|predicate| {
            let predicate = json_object(predicate, "entity_history_direct_plan.predicate");
            let field = predicate
                .get("field")
                .map(|value| json_object(value, "entity_history_direct_plan.predicate.field"))?;
            (field.get("kind").and_then(JsonValue::as_str) == Some("property")).then_some(predicate)
        })
        .expect("entity_history_direct_plan should include a property predicate");
    assert_object_keys(
        predicate,
        &["field", "operator", "value", "values"],
        "entity_history_direct_plan.predicate",
    );
    assert_eq!(
        predicate.get("operator").and_then(JsonValue::as_str),
        Some("is_not_null")
    );
    let predicate_field = predicate
        .get("field")
        .map(|value| json_object(value, "entity_history_direct_plan.predicate.field"))
        .expect("predicate should include field");
    assert_object_keys(
        predicate_field,
        &["details", "kind"],
        "entity_history_direct_plan.predicate.field",
    );
    assert_eq!(
        predicate_field.get("kind").and_then(JsonValue::as_str),
        Some("property")
    );
    assert_eq!(
        predicate_field.get("details").and_then(JsonValue::as_str),
        Some("key")
    );

    let projections = json_array(
        direct_details
            .get("projections")
            .expect("entity_history_direct_plan should include projections"),
        "entity_history_direct_plan.projections",
    );
    let projection = projections
        .iter()
        .find_map(|projection| {
            let projection = json_object(projection, "entity_history_direct_plan.projection");
            let field = projection
                .get("field")
                .map(|value| json_object(value, "entity_history_direct_plan.projection.field"))?;
            (field.get("kind").and_then(JsonValue::as_str) == Some("property")
                && field.get("details").and_then(JsonValue::as_str) == Some("key"))
            .then_some(projection)
        })
        .expect("entity_history_direct_plan should include a property projection for key");
    assert_object_keys(
        projection,
        &["field", "output_name"],
        "entity_history_direct_plan.projection",
    );
    let projection_field = projection
        .get("field")
        .map(|value| json_object(value, "entity_history_direct_plan.projection.field"))
        .expect("projection should include field");
    assert_eq!(
        projection_field.get("kind").and_then(JsonValue::as_str),
        Some("property")
    );
    assert_eq!(
        projection_field.get("details").and_then(JsonValue::as_str),
        Some("key")
    );

    let sort_keys = json_array(
        direct_details
            .get("sort_keys")
            .expect("entity_history_direct_plan should include sort_keys"),
        "entity_history_direct_plan.sort_keys",
    );
    let sort_key = sort_keys
        .iter()
        .find_map(|sort_key| {
            let sort_key = json_object(sort_key, "entity_history_direct_plan.sort_key");
            let field = sort_key
                .get("field")
                .map(|value| json_object(value, "entity_history_direct_plan.sort_key.field"))?;
            (field.get("kind").and_then(JsonValue::as_str) == Some("state")
                && field.get("details").and_then(JsonValue::as_str) == Some("depth"))
            .then_some(sort_key)
        })
        .expect("entity_history_direct_plan should include a state sort key for depth");
    assert_object_keys(
        sort_key,
        &["descending", "field", "output_name"],
        "entity_history_direct_plan.sort_key",
    );
    assert_eq!(
        sort_key.get("descending").and_then(JsonValue::as_bool),
        Some(false)
    );
    let sort_field = sort_key
        .get("field")
        .map(|value| json_object(value, "entity_history_direct_plan.sort_key.field"))
        .expect("sort_key should include field");
    assert_eq!(
        sort_field.get("kind").and_then(JsonValue::as_str),
        Some("state")
    );
    assert_eq!(
        sort_field.get("details").and_then(JsonValue::as_str),
        Some("depth")
    );
}

fn assert_public_read_physical_kind(explain_json: &JsonValue, expected_kind: &str) {
    let physical_plan = json_object_at(explain_json, "physical_plan", "explain_json");
    assert_eq!(
        physical_plan.get("kind").and_then(JsonValue::as_str),
        Some("public_read")
    );
    let physical_details = physical_plan
        .get("details")
        .map(|value| json_object(value, "physical_plan.details"))
        .expect("physical_plan should include details");
    assert_eq!(
        physical_details.get("kind").and_then(JsonValue::as_str),
        Some(expected_kind),
        "physical_plan should expose {expected_kind}"
    );
}

fn assert_lowered_sql_presence(explain_json: &JsonValue, expected_non_empty: bool) {
    let lowered_sql = json_object_at(explain_json, "executor_artifacts", "explain_json")
        .get("lowered_sql")
        .and_then(JsonValue::as_array)
        .expect("executor_artifacts.lowered_sql should be an array");
    if expected_non_empty {
        assert!(
            !lowered_sql.is_empty(),
            "executor_artifacts.lowered_sql should be populated"
        );
    } else {
        assert!(
            lowered_sql.is_empty(),
            "executor_artifacts.lowered_sql should be empty"
        );
    }
}

fn assert_public_write_json_contract(explain_json: &JsonValue) {
    let request = json_object_at(explain_json, "request", "explain_json");
    assert_object_keys(request, &["format", "mode"], "request");
    assert_eq!(
        request.get("mode").and_then(JsonValue::as_str),
        Some("plan")
    );
    assert_eq!(
        request.get("format").and_then(JsonValue::as_str),
        Some("json")
    );

    let semantic_statement = json_object_at(explain_json, "semantic_statement", "explain_json");
    assert_eq!(
        semantic_statement.get("kind").and_then(JsonValue::as_str),
        Some("public_write")
    );
    let logical_plan = json_object_at(explain_json, "logical_plan", "explain_json");
    assert_eq!(
        logical_plan.get("kind").and_then(JsonValue::as_str),
        Some("public_write")
    );
    let logical_details = logical_plan
        .get("details")
        .map(|value| json_object(value, "logical_plan.details"))
        .expect("public_write logical_plan should include details");
    let planned_write = logical_details
        .get("planned_write")
        .map(|value| json_object(value, "logical_plan.details.planned_write"))
        .expect("public_write logical_plan should include planned_write");
    let write_command = planned_write
        .get("command")
        .map(|value| json_object(value, "logical_plan.details.planned_write.command"))
        .expect("public_write logical_plan should include planned_write.command");
    assert_eq!(
        write_command
            .get("operation_kind")
            .and_then(JsonValue::as_str),
        Some("insert")
    );
    assert_eq!(
        write_command
            .get("requested_mode")
            .and_then(JsonValue::as_str),
        Some("auto")
    );
    let scope_proof = planned_write
        .get("scope_proof")
        .map(|value| json_object(value, "logical_plan.details.planned_write.scope_proof"))
        .expect("public_write logical_plan should include planned_write.scope_proof");
    assert_eq!(
        scope_proof.get("kind").and_then(JsonValue::as_str),
        Some("active_version")
    );
    let schema_proof = planned_write
        .get("schema_proof")
        .map(|value| json_object(value, "logical_plan.details.planned_write.schema_proof"))
        .expect("public_write logical_plan should include planned_write.schema_proof");
    assert_eq!(
        schema_proof.get("kind").and_then(JsonValue::as_str),
        Some("exact")
    );
    assert_eq!(
        planned_write
            .get("state_source")
            .and_then(JsonValue::as_str),
        Some("authoritative_committed")
    );
    let physical_plan = json_object_at(explain_json, "physical_plan", "explain_json");
    assert_eq!(
        physical_plan.get("kind").and_then(JsonValue::as_str),
        Some("public_write")
    );

    let executor_artifacts = json_object_at(explain_json, "executor_artifacts", "explain_json");
    assert!(
        executor_artifacts.get("surface_bindings").is_none(),
        "executor_artifacts should not duplicate typed surface bindings as top-level names"
    );
    assert!(
        executor_artifacts.get("write_phase_trace").is_none(),
        "plain public-write explain should not expose a static write phase trace shim"
    );

    let commit_preconditions = json_array(
        executor_artifacts
            .get("commit_preconditions")
            .expect("executor_artifacts should include commit_preconditions"),
        "commit_preconditions",
    );
    assert_eq!(commit_preconditions.len(), 1);
    let commit_precondition = json_object(&commit_preconditions[0], "commit_precondition");
    assert_object_keys(
        commit_precondition,
        &["expected_head", "idempotency_key", "write_lane"],
        "commit_precondition",
    );
    assert_eq!(
        commit_precondition
            .get("expected_head")
            .and_then(JsonValue::as_str),
        Some("current_head")
    );
    assert_eq!(
        commit_precondition
            .get("write_lane")
            .and_then(JsonValue::as_str),
        Some("active_version")
    );

    let idempotency_key = commit_precondition
        .get("idempotency_key")
        .and_then(JsonValue::as_str)
        .expect("commit_precondition.idempotency_key should be a string");
    let idempotency_json = serde_json::from_str::<JsonValue>(idempotency_key)
        .expect("idempotency_key should contain stable JSON");
    let idempotency_object = json_object(&idempotency_json, "idempotency_key");
    assert_object_keys(
        idempotency_object,
        &[
            "fingerprint",
            "lane",
            "operation",
            "partition_index",
            "surface",
        ],
        "idempotency_key",
    );
    assert_eq!(
        idempotency_object
            .get("surface")
            .and_then(JsonValue::as_str),
        Some("lix_file")
    );
    assert_eq!(
        idempotency_object
            .get("operation")
            .and_then(JsonValue::as_str),
        Some("insert")
    );
    assert_eq!(
        idempotency_object.get("lane").and_then(JsonValue::as_str),
        Some("active_version")
    );
    assert_eq!(
        idempotency_object
            .get("partition_index")
            .and_then(JsonValue::as_u64),
        Some(0)
    );
    assert!(
        idempotency_object
            .get("fingerprint")
            .and_then(JsonValue::as_str)
            .is_some(),
        "idempotency_key.fingerprint should be a string"
    );

    let domain_change_batches = json_array(
        executor_artifacts
            .get("domain_change_batches")
            .expect("executor_artifacts should include domain_change_batches"),
        "domain_change_batches",
    );
    assert_eq!(domain_change_batches.len(), 1);
    let domain_change_batch = json_object(&domain_change_batches[0], "domain_change_batch");
    assert_eq!(
        domain_change_batch
            .get("write_lane")
            .and_then(JsonValue::as_str),
        Some("active_version")
    );

    assert_no_rust_debug_leaks(explain_json);
}

simulation_test!(
    explain_text_surface_returns_sections_for_lix_state_query,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let result = engine
        .execute(
            "EXPLAIN SELECT COUNT(*) FROM lix_state WHERE file_id = 'missing' AND plugin_key = 'plugin_json'",
            &[],
        )
        .await
        .unwrap();

        assert_text_explain_contract(
            &result,
            &[
                "request",
                "semantic_statement",
                "logical_plan",
                "optimized_logical_plan",
                "physical_plan",
                "executor_artifacts",
                "stage_timings",
            ],
            &[
                ("request", &["mode: plan", "format: text"]),
                (
                    "semantic_statement",
                    &["kind: public_read", "surfaces: lix_state"],
                ),
                (
                    "logical_plan",
                    &["kind: public_read", "strategy: structured"],
                ),
                (
                    "physical_plan",
                    &["kind: public_read", "execution: lowered_sql"],
                ),
                ("stage_timings", &["parse:", "executor_preparation:"]),
            ],
        );
    }
);

simulation_test!(
    explain_public_read_json_matches_contract,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "EXPLAIN (FORMAT JSON) SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(
            result.statements[0].columns,
            vec!["explain_json".to_string()]
        );
        assert_public_read_json_contract(explain_json_payload(&result));
    }
);

simulation_test!(
    explain_specialized_lowered_public_read_stage_contract,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "EXPLAIN (FORMAT JSON) SELECT COUNT(*) FROM lix_state WHERE file_id = 'missing'",
                &[],
            )
            .await
            .unwrap();

        let explain_json = explain_json_payload(&result);
        assert_public_read_logical_strategy(explain_json, "logical_plan", "structured");
        assert_public_read_logical_strategy(explain_json, "optimized_logical_plan", "structured");
        assert_public_read_physical_kind(explain_json, "lowered_sql");
        assert_lowered_sql_presence(explain_json, true);
        assert_stage_timings_contract(
            explain_json,
            vec![
                "parse",
                "bind",
                "semantic_analysis",
                "logical_planning",
                "optimizer",
                "capability_resolution",
                "physical_planning",
                "executor_preparation",
            ]
            .as_slice(),
        );
    }
);

simulation_test!(
    explain_direct_history_public_read_omits_executor_preparation,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "EXPLAIN (FORMAT JSON) SELECT entity_id FROM lix_state_history WHERE root_commit_id = 'commit-1' ORDER BY depth ASC",
                &[],
            )
            .await
            .unwrap();

        let explain_json = explain_json_payload(&result);
        assert_public_read_logical_strategy(explain_json, "logical_plan", "structured");
        assert_public_read_logical_strategy(
            explain_json,
            "optimized_logical_plan",
            "direct_history",
        );
        assert_public_read_physical_kind(explain_json, "direct");
        assert_lowered_sql_presence(explain_json, false);
        assert_stage_timings_contract(
            explain_json,
            &[
                "parse",
                "bind",
                "semantic_analysis",
                "logical_planning",
                "optimizer",
                "physical_planning",
            ],
        );
        assert_missing_stage_names(
            explain_json,
            &["capability_resolution", "executor_preparation"],
        );
    }
);

simulation_test!(
    explain_direct_history_public_read_exposes_typed_nested_plan_artifacts,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "EXPLAIN (FORMAT JSON) \
                 SELECT key \
                 FROM lix_key_value_history \
                 WHERE root_commit_id = 'commit-1' AND key IS NOT NULL \
                 ORDER BY lixcol_depth ASC",
                &[],
            )
            .await
            .unwrap();

        let explain_json = explain_json_payload(&result);
        assert_public_read_logical_strategy(
            explain_json,
            "optimized_logical_plan",
            "direct_history",
        );
        assert_entity_history_direct_plan_contract(explain_json);
    }
);

simulation_test!(
    explain_broad_public_read_stage_contract,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "EXPLAIN (FORMAT JSON) \
                 SELECT s.schema_key, COUNT(*) \
                 FROM lix_state s \
                 JOIN lix_state_by_version sv ON sv.entity_id = s.entity_id \
                 WHERE s.schema_key = 'lix_key_value' AND sv.lixcol_version_id = 'main' \
                 GROUP BY s.schema_key \
                 ORDER BY s.schema_key",
                &[],
            )
            .await
            .unwrap();

        let explain_json = explain_json_payload(&result);
        assert_public_read_logical_strategy(explain_json, "logical_plan", "broad");
        assert_public_read_logical_strategy(explain_json, "optimized_logical_plan", "broad");
        assert_broad_public_read_typed_statement_contract(explain_json);
        assert_public_read_physical_kind(explain_json, "lowered_sql");
        assert_lowered_sql_presence(explain_json, true);
        assert_stage_timings_contract(
            explain_json,
            &[
                "parse",
                "bind",
                "logical_planning",
                "capability_resolution",
                "optimizer",
                "physical_planning",
                "executor_preparation",
            ],
        );
        assert_missing_stage_names(explain_json, &["semantic_analysis"]);
    }
);

simulation_test!(
    explain_broad_public_read_text_shows_optimization_delta,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "EXPLAIN (FORMAT TEXT) \
                 SELECT s.schema_key, COUNT(*) \
                 FROM lix_state s \
                 JOIN lix_state_by_version sv ON sv.entity_id = s.entity_id \
                 WHERE s.schema_key = 'lix_key_value' AND sv.lixcol_version_id = 'main' \
                 GROUP BY s.schema_key \
                 ORDER BY s.schema_key",
                &[],
            )
            .await
            .unwrap();

        assert_text_explain_contract(
            &result,
            &["logical_plan", "optimized_logical_plan"],
            &[
                (
                    "logical_plan",
                    &[
                        "kind: public_read",
                        "strategy: broad",
                        "broad_public_relations: lix_state, lix_state_by_version",
                        "broad_lowered_public_relations: (none)",
                    ],
                ),
                (
                    "optimized_logical_plan",
                    &[
                        "kind: public_read",
                        "strategy: broad",
                        "broad_public_relations: (none)",
                        "broad_lowered_public_relations: lix_state, lix_state_by_version",
                    ],
                ),
            ],
        );
    }
);

simulation_test!(
    internal_explain_omits_unmeasured_stages,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute("EXPLAIN (FORMAT JSON) SELECT 1", &[])
            .await
            .unwrap();

        let explain_json = explain_json_payload(&result);
        assert_stage_timings_contract(explain_json, &["parse", "logical_planning"]);
        assert_missing_stage_names(
            explain_json,
            &[
                "bind",
                "semantic_analysis",
                "optimizer",
                "capability_resolution",
                "physical_planning",
                "executor_preparation",
            ],
        );
        assert_no_rust_debug_leaks(explain_json);
    }
);

simulation_test!(
    explain_public_write_omits_placeholder_stages,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "EXPLAIN (FORMAT JSON) INSERT INTO lix_file (path, data) VALUES ('/timing-check.md', lix_text_encode('hello'))",
                &[],
            )
            .await
            .unwrap();

        let explain_json = explain_json_payload(&result);
        assert_stage_timings_contract(
            explain_json,
            &[
                "parse",
                "bind",
                "semantic_analysis",
                "logical_planning",
                "physical_planning",
            ],
        );
        assert_missing_stage_names(
            explain_json,
            &["optimizer", "capability_resolution", "executor_preparation"],
        );
        assert_public_write_json_contract(explain_json);
    }
);

simulation_test!(
    explain_public_write_update_contract_pins_target_set_proof_kind,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();
        let active_version_id = engine.active_version_id().await.unwrap();

        let result = engine
            .execute(
                &format!(
                    "EXPLAIN (FORMAT JSON) UPDATE lix_state_by_version \
                     SET snapshot_content = '{{\"value\":\"after\"}}' \
                     WHERE schema_key = 'lix_key_value' \
                       AND entity_id = 'entity-1' \
                       AND version_id = '{active_version_id}'"
                ),
                &[],
            )
            .await
            .unwrap();

        let explain_json = explain_json_payload(&result);
        let logical_plan = json_object_at(explain_json, "logical_plan", "explain_json");
        let logical_details = logical_plan
            .get("details")
            .map(|value| json_object(value, "logical_plan.details"))
            .expect("logical_plan should include details");
        let planned_write = logical_details
            .get("planned_write")
            .map(|value| json_object(value, "logical_plan.details.planned_write"))
            .expect("logical_plan.details should include planned_write");
        let write_command = planned_write
            .get("command")
            .map(|value| json_object(value, "logical_plan.details.planned_write.command"))
            .expect("planned_write should include command");
        assert_eq!(
            write_command
                .get("operation_kind")
                .and_then(JsonValue::as_str),
            Some("update")
        );
        assert_eq!(
            write_command
                .get("requested_mode")
                .and_then(JsonValue::as_str),
            Some("auto")
        );

        let scope_proof = planned_write
            .get("scope_proof")
            .map(|value| json_object(value, "logical_plan.details.planned_write.scope_proof"))
            .expect("planned_write should include scope_proof");
        assert_eq!(
            scope_proof.get("kind").and_then(JsonValue::as_str),
            Some("single_version")
        );
        assert_eq!(
            scope_proof.get("version").and_then(JsonValue::as_str),
            Some(active_version_id.as_str())
        );

        let schema_proof = planned_write
            .get("schema_proof")
            .map(|value| json_object(value, "logical_plan.details.planned_write.schema_proof"))
            .expect("planned_write should include schema_proof");
        assert_eq!(
            schema_proof.get("kind").and_then(JsonValue::as_str),
            Some("exact")
        );
        assert_eq!(
            schema_proof
                .get("schema_keys")
                .and_then(JsonValue::as_array)
                .cloned(),
            Some(vec![JsonValue::String("lix_key_value".to_string())])
        );

        let target_set_proof = planned_write
            .get("target_set_proof")
            .map(|value| json_object(value, "logical_plan.details.planned_write.target_set_proof"))
            .expect("planned_write should include target_set_proof");
        assert_eq!(
            target_set_proof.get("kind").and_then(JsonValue::as_str),
            Some("exact")
        );
        assert_eq!(
            target_set_proof
                .get("entity_ids")
                .and_then(JsonValue::as_array)
                .cloned(),
            Some(vec![JsonValue::String("entity-1".to_string())])
        );

        assert_no_rust_debug_leaks(explain_json);
    }
);

simulation_test!(
    explain_analyze_json_reports_runtime_metrics,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "EXPLAIN (ANALYZE, FORMAT JSON) SELECT COUNT(*) AS total FROM lix_state WHERE file_id = 'missing'",
                &[],
            )
            .await
            .unwrap();

        let explain_json = explain_json_payload(&result);
        assert_eq!(
            explain_json
                .get("request")
                .and_then(|value| value.get("mode"))
                .and_then(|value| value.as_str()),
            Some("analyze")
        );
        let analyzed_runtime = explain_json
            .get("analyzed_runtime")
            .and_then(|value| value.as_object())
            .expect("EXPLAIN ANALYZE should expose analyzed_runtime");
        assert_object_keys(
            analyzed_runtime,
            &[
                "execution_duration_us",
                "output_column_count",
                "output_columns",
                "output_row_count",
            ],
            "analyzed_runtime",
        );
        assert!(analyzed_runtime
            .get("execution_duration_us")
            .and_then(|value| value.as_u64())
            .is_some());
        assert_eq!(
            analyzed_runtime
                .get("output_row_count")
                .and_then(|value| value.as_u64()),
            Some(1)
        );
        assert_eq!(
            analyzed_runtime
                .get("output_column_count")
                .and_then(|value| value.as_u64()),
            Some(1)
        );
        assert_eq!(
            analyzed_runtime
                .get("output_columns")
                .and_then(|value| value.as_array())
                .cloned(),
            Some(vec![serde_json::Value::String("total".to_string())])
        );
        assert_no_rust_debug_leaks(explain_json);
    }
);

simulation_test!(
    explain_analyze_internal_read_only_query_reports_runtime_metrics,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute("EXPLAIN (ANALYZE, FORMAT JSON) SELECT 1 AS value", &[])
            .await
            .unwrap();

        assert_explain_request_json(&result, "analyze", "json");
        let explain_json = explain_json_payload(&result);
        assert_eq!(
            explain_json
                .get("semantic_statement")
                .and_then(|value| value.get("kind"))
                .and_then(JsonValue::as_str),
            Some("internal")
        );
        assert_eq!(
            explain_json
                .get("logical_plan")
                .and_then(|value| value.get("kind"))
                .and_then(JsonValue::as_str),
            Some("internal")
        );
        assert_eq!(
            explain_json
                .get("optimized_logical_plan")
                .and_then(|value| value.get("kind"))
                .and_then(JsonValue::as_str),
            Some("internal")
        );
        assert!(
            explain_json.get("physical_plan").is_none(),
            "internal analyzed explain should not invent a physical_plan section"
        );
        assert_stage_timings_contract(explain_json, &["parse", "logical_planning"]);
        assert_missing_stage_names(
            explain_json,
            &[
                "bind",
                "semantic_analysis",
                "optimizer",
                "capability_resolution",
                "physical_planning",
                "executor_preparation",
            ],
        );
        let analyzed_runtime = explain_json
            .get("analyzed_runtime")
            .and_then(|value| value.as_object())
            .expect("internal EXPLAIN ANALYZE should expose analyzed_runtime");
        assert_eq!(
            analyzed_runtime
                .get("output_row_count")
                .and_then(|value| value.as_u64()),
            Some(1)
        );
        assert_eq!(
            analyzed_runtime
                .get("output_column_count")
                .and_then(|value| value.as_u64()),
            Some(1)
        );
        assert_eq!(
            analyzed_runtime
                .get("output_columns")
                .and_then(|value| value.as_array())
                .cloned(),
            Some(vec![serde_json::Value::String("value".to_string())])
        );
        assert_no_rust_debug_leaks(explain_json);
    }
);

simulation_test!(
    plain_explain_omits_analyzed_runtime,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute("EXPLAIN (FORMAT JSON) SELECT 1 AS value", &[])
            .await
            .unwrap();

        let explain_json = explain_json_payload(&result);
        assert_eq!(
            explain_json
                .get("request")
                .and_then(|value| value.get("mode"))
                .and_then(|value| value.as_str()),
            Some("plan")
        );
        assert!(
            explain_json.get("analyzed_runtime").is_none(),
            "plain EXPLAIN should not expose analyzed runtime metrics"
        );
    }
);

simulation_test!(
    explain_launch_matrix_accepts_exact_supported_forms,
    simulations = [sqlite, postgres],
    |sim| async move {
        struct AcceptedCase<'a> {
            sql: &'a str,
            expected_mode: &'a str,
            expected_format: &'a str,
        }

        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let cases = [
            AcceptedCase {
                sql: "EXPLAIN SELECT 1 AS value",
                expected_mode: "plan",
                expected_format: "text",
            },
            AcceptedCase {
                sql: "EXPLAIN ANALYZE SELECT 1 AS value",
                expected_mode: "analyze",
                expected_format: "text",
            },
            AcceptedCase {
                sql: "EXPLAIN (FORMAT JSON) SELECT 1 AS value",
                expected_mode: "plan",
                expected_format: "json",
            },
            AcceptedCase {
                sql: "EXPLAIN (ANALYZE, FORMAT JSON) SELECT 1 AS value",
                expected_mode: "analyze",
                expected_format: "json",
            },
            AcceptedCase {
                sql: "EXPLAIN (ANALYZE FALSE, FORMAT JSON) SELECT 1 AS value",
                expected_mode: "plan",
                expected_format: "json",
            },
            AcceptedCase {
                sql: "EXPLAIN (FORMAT JSON, ANALYZE FALSE) SELECT 1 AS value",
                expected_mode: "plan",
                expected_format: "json",
            },
            AcceptedCase {
                sql: "EXPLAIN (ANALYZE TRUE, FORMAT TEXT) SELECT 1 AS value",
                expected_mode: "analyze",
                expected_format: "text",
            },
        ];

        for case in cases {
            let result = engine.execute(case.sql, &[]).await.unwrap_or_else(|error| {
                panic!(
                    "accepted EXPLAIN form should succeed: {} -> {}",
                    case.sql, error
                )
            });
            match case.expected_format {
                "json" => {
                    assert_explain_request_json(&result, case.expected_mode, case.expected_format)
                }
                "text" => {
                    assert_explain_request_text(&result, case.expected_mode, case.expected_format)
                }
                other => panic!("unsupported expected format {other}"),
            }
        }
    }
);

simulation_test!(
    explain_launch_matrix_rejects_exact_unsupported_forms,
    simulations = [sqlite, postgres],
    |sim| async move {
        struct RejectedCase<'a> {
            sql: &'a str,
            expected_error_fragment: &'a str,
        }

        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let cases = [
            RejectedCase {
                sql: "EXPLAIN ESTIMATE SELECT 1",
                expected_error_fragment: "unsupported EXPLAIN modifier ESTIMATE",
            },
            RejectedCase {
                sql: "EXPLAIN (ANALYZE, ANALYZE FALSE) SELECT 1",
                expected_error_fragment: "duplicate EXPLAIN option ANALYZE",
            },
            RejectedCase {
                sql: "EXPLAIN (FORMAT JSON, FORMAT TEXT) SELECT 1",
                expected_error_fragment: "duplicate EXPLAIN option FORMAT",
            },
            RejectedCase {
                sql: "EXPLAIN (COSTS TRUE) SELECT 1",
                expected_error_fragment: "unsupported EXPLAIN option COSTS",
            },
            RejectedCase {
                sql: "EXPLAIN (ANALYZE 'maybe') SELECT 1",
                expected_error_fragment: "invalid EXPLAIN option ANALYZE: expected TRUE or FALSE",
            },
            RejectedCase {
                sql: "EXPLAIN (FORMAT 1) SELECT 1",
                expected_error_fragment: "invalid EXPLAIN option FORMAT: expected TEXT or JSON",
            },
        ];

        for case in cases {
            let error = engine
                .execute(case.sql, &[])
                .await
                .expect_err("unsupported EXPLAIN launch form should be rejected explicitly");
            assert_eq!(error.code, "LIX_ERROR_UNKNOWN");
            assert!(
                error.description.contains(case.expected_error_fragment),
                "unexpected error for {}: {}",
                case.sql,
                error.description
            );
        }
    }
);

simulation_test!(
    plain_explain_public_write_stays_side_effect_free,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "EXPLAIN INSERT INTO lix_file (path, data) VALUES ('/explain-noop.md', lix_text_encode('hello'))",
                &[],
            )
            .await
            .unwrap();

        let count = engine
            .execute(
                "SELECT COUNT(*) FROM lix_file WHERE path = '/explain-noop.md'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(count.statements[0].rows[0][0], Value::Integer(0));
    }
);

simulation_test!(
    explain_analyze_rejects_public_writes,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let error = engine
        .execute(
            "EXPLAIN ANALYZE INSERT INTO lix_file (path, data) VALUES ('/analyze-write.md', lix_text_encode('hello'))",
            &[],
        )
        .await
        .expect_err("EXPLAIN ANALYZE over public writes should be rejected explicitly");

        assert_eq!(error.code, "LIX_ERROR_UNKNOWN");
        assert!(
            error
                .description
                .contains("EXPLAIN ANALYZE is not supported for public write statements yet"),
            "unexpected error: {}",
            error.description
        );
    }
);

simulation_test!(
    explain_format_text_returns_section_rows,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute("EXPLAIN (FORMAT TEXT) SELECT 1 AS value", &[])
            .await
            .unwrap();

        assert_text_explain_contract(
            &result,
            &[
                "request",
                "semantic_statement",
                "logical_plan",
                "optimized_logical_plan",
                "executor_artifacts",
                "stage_timings",
            ],
            &[
                ("request", &["mode: plan", "format: text"]),
                ("semantic_statement", &["kind: internal"]),
                (
                    "logical_plan",
                    &["kind: internal", "result_contract: select"],
                ),
                ("executor_artifacts", &["lowered_sql_statements: 1"]),
                ("stage_timings", &["parse:", "logical_planning:"]),
            ],
        );
    }
);

simulation_test!(
    explain_rejects_legacy_format_syntax,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let error = engine
            .execute("EXPLAIN FORMAT JSON SELECT 1", &[])
            .await
            .expect_err("legacy EXPLAIN FORMAT syntax should be rejected");

        assert_eq!(error.code, "LIX_ERROR_UNKNOWN");
        assert!(
            error
                .description
                .contains("legacy EXPLAIN FORMAT syntax is not supported"),
            "unexpected error: {}",
            error.description
        );
    }
);

simulation_test!(
    explain_rejects_describe_aliases,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let error = engine
            .execute("DESCRIBE SELECT 1", &[])
            .await
            .expect_err("DESCRIBE should be rejected for explain normalization");

        assert_eq!(error.code, "LIX_ERROR_UNKNOWN");
        assert!(
            error
                .description
                .contains("unsupported EXPLAIN alias DESCRIBE"),
            "unexpected error: {}",
            error.description
        );
    }
);

simulation_test!(
    explain_rejects_unsupported_format,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let error = engine
            .execute("EXPLAIN (FORMAT TREE) SELECT 1", &[])
            .await
            .expect_err("unsupported EXPLAIN format should be rejected");

        assert_eq!(error.code, "LIX_ERROR_UNKNOWN");
        assert!(
            error
                .description
                .contains("unsupported EXPLAIN FORMAT TREE"),
            "unexpected error: {}",
            error.description
        );
    }
);

simulation_test!(
    explain_rejects_unsupported_modifier,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let error = engine
            .execute("EXPLAIN VERBOSE SELECT 1", &[])
            .await
            .expect_err("unsupported EXPLAIN modifier should be rejected");

        assert_eq!(error.code, "LIX_ERROR_UNKNOWN");
        assert!(
            error
                .description
                .contains("unsupported EXPLAIN modifier VERBOSE"),
            "unexpected error: {}",
            error.description
        );
    }
);

simulation_test!(
    explain_rejects_query_plan_modifier,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let error = engine
            .execute("EXPLAIN QUERY PLAN SELECT 1", &[])
            .await
            .expect_err("EXPLAIN QUERY PLAN should be rejected");

        assert_eq!(error.code, "LIX_ERROR_UNKNOWN");
        assert!(
            error
                .description
                .contains("unsupported EXPLAIN modifier QUERY PLAN"),
            "unexpected error: {}",
            error.description
        );
    }
);
