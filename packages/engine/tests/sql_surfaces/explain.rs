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

fn assert_text_explain_contract(result: &lix_engine::ExecuteResult, required_keys: &[&str]) {
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

    for (key, value) in rows {
        serde_json::from_str::<JsonValue>(value).unwrap_or_else(|error| {
            panic!("FORMAT TEXT row {key} should contain JSON text: {error}")
        });
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

    let logical_plan = json_object_at(explain_json, "logical_plan", "explain_json");
    assert_eq!(
        logical_plan.get("kind").and_then(JsonValue::as_str),
        Some("public_read")
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
    assert_eq!(
        executor_artifacts
            .get("surface_bindings")
            .and_then(JsonValue::as_array)
            .cloned(),
        Some(vec![JsonValue::String("lix_state".to_string())])
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
    let physical_plan = json_object_at(explain_json, "physical_plan", "explain_json");
    assert_eq!(
        physical_plan.get("kind").and_then(JsonValue::as_str),
        Some("public_write")
    );

    let executor_artifacts = json_object_at(explain_json, "executor_artifacts", "explain_json");
    assert_eq!(
        executor_artifacts
            .get("surface_bindings")
            .and_then(JsonValue::as_array)
            .cloned(),
        Some(vec![JsonValue::String("lix_file".to_string())])
    );

    let write_phase_trace = json_array(
        executor_artifacts
            .get("write_phase_trace")
            .expect("executor_artifacts should include write_phase_trace"),
        "write_phase_trace",
    );
    assert!(
        write_phase_trace
            .iter()
            .any(|value| value.as_str() == Some("build_domain_change_batch")),
        "public write explain should expose the write phase trace"
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
    explain_public_read_stage_timings_follow_compile_order,
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
        assert_stage_timings_contract(
            explain_json,
            vec![
                "parse",
                "bind",
                "semantic_analysis",
                "optimizer",
                "logical_planning",
                "physical_planning",
            ]
            .as_slice(),
        );
        assert_missing_stage_names(explain_json, &["executor_preparation"]);
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
        assert_missing_stage_names(explain_json, &["optimizer", "executor_preparation"]);
        assert_public_write_json_contract(explain_json);
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
