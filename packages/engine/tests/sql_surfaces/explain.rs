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

const BROAD_PUBLIC_READ_STAGE_CONTRACT: &[&str] = &[
    "parse",
    "bind",
    "logical_planning",
    "capability_resolution",
    "routing",
    "physical_planning",
    "artifact_preparation",
];

fn broad_public_read_stage_contract_query() -> &'static str {
    "EXPLAIN (FORMAT JSON) \
     WITH latest AS ( \
       SELECT entity_id \
       FROM lix_state_by_version \
       WHERE lixcol_version_id = 'main' \
     ) \
     SELECT \
       s.schema_key, \
       (SELECT COUNT(*) FROM lix_file f WHERE f.id = 'file-stable-child') AS file_count \
     FROM lix_state s \
     JOIN lix_state_by_version sv ON sv.entity_id = s.entity_id \
     WHERE s.schema_key = 'lix_key_value' \
       AND EXISTS (SELECT 1 FROM latest) \
       AND s.entity_id IN (SELECT entity_id FROM latest) \
       AND sv.lixcol_version_id = 'main' \
     GROUP BY s.schema_key \
     HAVING COUNT(*) > 0 \
     ORDER BY s.schema_key \
     LIMIT 5"
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

fn explain_stage_duration_us(explain_json: &serde_json::Value, stage: &str) -> Option<u64> {
    explain_json
        .get("stage_timings")
        .and_then(|value| value.as_array())
        .and_then(|timings| {
            timings.iter().find_map(|timing| {
                let object = timing.as_object()?;
                (object.get("stage").and_then(JsonValue::as_str) == Some(stage))
                    .then(|| object.get("duration_us").and_then(JsonValue::as_u64))
                    .flatten()
            })
        })
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

fn assert_stage_duration_at_least(explain_json: &JsonValue, stage: &str, minimum_us: u64) {
    let duration = explain_stage_duration_us(explain_json, stage)
        .unwrap_or_else(|| panic!("stage_timings should include {stage}"));
    assert!(
        duration >= minimum_us,
        "{stage} should be at least {minimum_us}us, got {duration}us"
    );
}

fn assert_stage_duration_below(explain_json: &JsonValue, stage: &str, maximum_us: u64) {
    let duration = explain_stage_duration_us(explain_json, stage)
        .unwrap_or_else(|| panic!("stage_timings should include {stage}"));
    assert!(
        duration < maximum_us,
        "{stage} should stay below {maximum_us}us, got {duration}us"
    );
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
        "ArtifactPreparation",
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

fn assert_broad_relation_snapshot(
    relation: &JsonValue,
    expected_kind: &str,
    expected_name: &str,
    context: &str,
) {
    let relation = json_object(relation, context);
    assert_eq!(
        relation.get("kind").and_then(JsonValue::as_str),
        Some(expected_kind),
        "{context} should expose the expected relation kind"
    );
    let details = relation
        .get("details")
        .map(|value| json_object(value, &format!("{context}.details")))
        .expect("relation snapshot should include details");
    match expected_kind {
        "public" | "lowered_public" => assert_eq!(
            details.get("public_name").and_then(JsonValue::as_str),
            Some(expected_name),
            "{context} should expose the expected public name"
        ),
        "internal" | "external" | "cte" => assert_eq!(
            details.get("relation_name").and_then(JsonValue::as_str),
            Some(expected_name),
            "{context} should expose the expected relation name"
        ),
        _ => panic!("unsupported broad relation kind {expected_kind}"),
    }
}

fn assert_broad_alias_snapshot(alias: &JsonValue, expected_name: &str, context: &str) {
    let alias = json_object(alias, context);
    assert_eq!(
        alias.get("name").and_then(JsonValue::as_str),
        Some(expected_name),
        "{context} should expose the expected alias"
    );
}

fn assert_no_broad_public_read_fallbacks(value: &JsonValue, context: &str) {
    match value {
        JsonValue::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                assert_no_broad_public_read_fallbacks(item, &format!("{context}[{index}]"));
            }
        }
        JsonValue::Object(object) => {
            if object.get("kind").and_then(JsonValue::as_str) == Some("other") {
                panic!("{context} should not contain broad fallback snapshots: {value}");
            }
            for (key, value) in object {
                assert_no_broad_public_read_fallbacks(value, &format!("{context}.{key}"));
            }
        }
        _ => {}
    }
}

fn rust_item_section<'a>(source: &'a str, start_marker: &str, end_marker: &str) -> &'a str {
    let start = source
        .find(start_marker)
        .unwrap_or_else(|| panic!("source should contain {start_marker}"));
    let rest = &source[start..];
    let end = rest
        .find(end_marker)
        .unwrap_or_else(|| panic!("source should contain {end_marker} after {start_marker}"));
    &rest[..end]
}

fn broad_expr_kind<'a>(expr: &'a JsonValue, context: &str) -> &'a str {
    json_object(expr, context)
        .get("kind")
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| panic!("{context} should expose a broad expression kind"))
}

fn broad_expr_details<'a>(expr: &'a JsonValue, context: &str) -> &'a Map<String, JsonValue> {
    json_object(
        json_object(expr, context)
            .get("details")
            .unwrap_or_else(|| panic!("{context} should include details")),
        &format!("{context}.details"),
    )
}

fn assert_expr_compound_identifier(expr: &JsonValue, expected_parts: &[&str], context: &str) {
    assert_eq!(broad_expr_kind(expr, context), "compound_identifier");
    assert_string_array(
        broad_expr_details(expr, context)
            .get("parts")
            .expect("compound_identifier should include parts"),
        expected_parts,
        &format!("{context}.details.parts"),
    );
}

fn assert_expr_value(expr: &JsonValue, expected_value: &str, context: &str) {
    assert_eq!(broad_expr_kind(expr, context), "value");
    assert_eq!(
        broad_expr_details(expr, context)
            .get("value")
            .and_then(JsonValue::as_str),
        Some(expected_value)
    );
}

fn find_value_with_kind<'a>(value: &'a JsonValue, expected_kind: &str) -> Option<&'a JsonValue> {
    match value {
        JsonValue::Array(items) => items
            .iter()
            .find_map(|item| find_value_with_kind(item, expected_kind)),
        JsonValue::Object(object) => {
            if object.get("kind").and_then(JsonValue::as_str) == Some(expected_kind) {
                return Some(value);
            }
            object
                .values()
                .find_map(|child| find_value_with_kind(child, expected_kind))
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => None,
    }
}

fn assert_query_root_surface(
    query: &JsonValue,
    expected_kind: &str,
    expected_surface: &str,
    context: &str,
) {
    let query = json_object(query, context);
    let body = query
        .get("body")
        .map(|value| json_object(value, &format!("{context}.body")))
        .expect("query should include body");
    assert_eq!(body.get("kind").and_then(JsonValue::as_str), Some("select"));
    let body_details = body
        .get("details")
        .map(|value| json_object(value, &format!("{context}.body.details")))
        .expect("query body should include details");
    let from = json_array(
        body_details
            .get("from")
            .expect("query body should include from"),
        &format!("{context}.body.details.from"),
    );
    assert!(!from.is_empty(), "{context} should include a root relation");
    let root = json_object(&from[0], &format!("{context}.body.details.from[0]"));
    let relation = root
        .get("relation")
        .map(|value| json_object(value, &format!("{context}.body.details.from[0].relation")))
        .expect("root relation should include relation");
    let relation_details = relation
        .get("details")
        .map(|value| {
            json_object(
                value,
                &format!("{context}.body.details.from[0].relation.details"),
            )
        })
        .expect("root relation should include details");
    assert_broad_relation_snapshot(
        relation_details
            .get("relation")
            .expect("root relation details should include relation"),
        expected_kind,
        expected_surface,
        &format!("{context}.body.details.from[0].relation.details.relation"),
    );
}

fn assert_rich_broad_public_read_statement_snapshot(
    broad_statement: &JsonValue,
    expected_surface_kind: &str,
) {
    let statement = json_object(broad_statement, "broad_statement");
    assert_no_broad_public_read_fallbacks(broad_statement, "broad_statement");
    assert_eq!(
        statement.get("kind").and_then(JsonValue::as_str),
        Some("query")
    );
    let statement_details = statement
        .get("details")
        .map(|value| json_object(value, "broad_statement.details"))
        .expect("broad_statement should include details");
    let with = statement_details
        .get("with")
        .map(|value| json_object(value, "broad_statement.details.with"))
        .expect("broad_statement.details should include a WITH snapshot");
    assert_eq!(
        with.get("recursive").and_then(JsonValue::as_bool),
        Some(false)
    );
    let cte_tables = with
        .get("cte_tables")
        .map(|value| json_array(value, "broad_statement.details.with.cte_tables"))
        .expect("broad_statement.details.with should include cte_tables");
    assert_eq!(cte_tables.len(), 1, "broad query should expose one CTE");
    let cte = json_object(&cte_tables[0], "broad_statement.details.with.cte_tables[0]");
    assert_broad_alias_snapshot(
        cte.get("alias").expect("CTE snapshot should include alias"),
        "latest",
        "broad_statement.details.with.cte_tables[0].alias",
    );
    assert_query_root_surface(
        cte.get("query").expect("CTE snapshot should include query"),
        expected_surface_kind,
        "lix_state_by_version",
        "broad_statement.details.with.cte_tables[0].query",
    );

    let order_by = statement_details
        .get("order_by")
        .map(|value| json_object(value, "broad_statement.details.order_by"))
        .expect("broad_statement.details should include order_by");
    assert_eq!(
        order_by.get("kind").and_then(JsonValue::as_str),
        Some("expressions")
    );
    let order_by_details = order_by
        .get("details")
        .map(|value| json_object(value, "broad_statement.details.order_by.details"))
        .expect("order_by should include details");
    let order_by_expressions = order_by_details
        .get("expressions")
        .map(|value| {
            json_array(
                value,
                "broad_statement.details.order_by.details.expressions",
            )
        })
        .expect("order_by should include expressions");
    assert_eq!(order_by_expressions.len(), 1);
    let order_expr = json_object(
        &order_by_expressions[0],
        "broad_statement.details.order_by.details.expressions[0]",
    );
    assert_expr_compound_identifier(
        order_expr
            .get("expr")
            .expect("order_by expression should include expr"),
        &["s", "schema_key"],
        "broad_statement.details.order_by.details.expressions[0].expr",
    );

    let limit_clause = statement_details
        .get("limit_clause")
        .map(|value| json_object(value, "broad_statement.details.limit_clause"))
        .expect("broad_statement.details should include limit_clause");
    assert_eq!(
        limit_clause.get("kind").and_then(JsonValue::as_str),
        Some("limit_offset")
    );
    let limit_details = limit_clause
        .get("details")
        .map(|value| json_object(value, "broad_statement.details.limit_clause.details"))
        .expect("limit_clause should include details");
    let limit_expr = limit_details
        .get("limit")
        .expect("limit_clause should include limit");
    assert_expr_value(
        limit_expr,
        "5",
        "broad_statement.details.limit_clause.details.limit",
    );

    let body = statement_details
        .get("body")
        .map(|value| json_object(value, "broad_statement.details.body"))
        .expect("broad_statement.details should include body");
    assert_eq!(body.get("kind").and_then(JsonValue::as_str), Some("select"));
    let body_details = body
        .get("details")
        .map(|value| json_object(value, "broad_statement.details.body.details"))
        .expect("broad_statement.details.body should include details");
    let projection = body_details
        .get("projection")
        .map(|value| json_array(value, "broad_statement.details.body.details.projection"))
        .expect("broad select should include projection");
    assert_eq!(
        projection.len(),
        2,
        "broad select should expose two projection items"
    );
    let schema_key_projection = json_object(
        &projection[0],
        "broad_statement.details.body.details.projection[0]",
    );
    assert_eq!(
        schema_key_projection
            .get("kind")
            .and_then(JsonValue::as_str),
        Some("expr")
    );
    let schema_key_projection_details = schema_key_projection
        .get("details")
        .map(|value| {
            json_object(
                value,
                "broad_statement.details.body.details.projection[0].details",
            )
        })
        .expect("first projection should include details");
    assert_expr_compound_identifier(
        schema_key_projection_details
            .get("expr")
            .expect("first projection should include expr"),
        &["s", "schema_key"],
        "broad_statement.details.body.details.projection[0].details.expr",
    );

    let file_count_projection = json_object(
        &projection[1],
        "broad_statement.details.body.details.projection[1]",
    );
    assert_eq!(
        file_count_projection
            .get("kind")
            .and_then(JsonValue::as_str),
        Some("expr")
    );
    let file_count_projection_details = file_count_projection
        .get("details")
        .map(|value| {
            json_object(
                value,
                "broad_statement.details.body.details.projection[1].details",
            )
        })
        .expect("second projection should include details");
    assert_eq!(
        file_count_projection_details
            .get("alias")
            .and_then(JsonValue::as_str),
        Some("file_count")
    );
    let projection_expr = file_count_projection_details
        .get("expr")
        .expect("second projection should include expr");
    assert_eq!(
        broad_expr_kind(
            projection_expr,
            "broad_statement.details.body.details.projection[1].details.expr",
        ),
        "scalar_subquery"
    );
    assert_query_root_surface(
        broad_expr_details(
            projection_expr,
            "broad_statement.details.body.details.projection[1].details.expr",
        )
        .get("query")
        .expect("scalar_subquery should include query"),
        expected_surface_kind,
        "lix_file",
        "broad_statement.details.body.details.projection[1].details.expr.details.query",
    );

    let selection = body_details
        .get("selection")
        .expect("broad select should include selection");
    let exists_expr = find_value_with_kind(selection, "exists")
        .expect("selection should include an exists expression");
    assert_query_root_surface(
        broad_expr_details(
            exists_expr,
            "broad_statement.details.body.details.selection.exists",
        )
        .get("subquery")
        .expect("exists expression should include subquery"),
        "cte",
        "latest",
        "broad_statement.details.body.details.selection.exists.details.subquery",
    );
    let in_subquery_expr = find_value_with_kind(selection, "in_subquery")
        .expect("selection should include an in_subquery expression");
    let in_subquery_details = broad_expr_details(
        in_subquery_expr,
        "broad_statement.details.body.details.selection.in_subquery",
    );
    assert_expr_compound_identifier(
        in_subquery_details
            .get("expr")
            .expect("in_subquery should include expr"),
        &["s", "entity_id"],
        "broad_statement.details.body.details.selection.in_subquery.details.expr",
    );
    assert_query_root_surface(
        in_subquery_details
            .get("subquery")
            .expect("in_subquery should include subquery"),
        "cte",
        "latest",
        "broad_statement.details.body.details.selection.in_subquery.details.subquery",
    );

    let group_by = body_details
        .get("group_by")
        .map(|value| json_object(value, "broad_statement.details.body.details.group_by"))
        .expect("broad select should include group_by");
    assert_eq!(
        group_by.get("kind").and_then(JsonValue::as_str),
        Some("expressions")
    );
    let group_by_expressions = group_by
        .get("details")
        .map(|value| {
            json_object(
                value,
                "broad_statement.details.body.details.group_by.details",
            )
        })
        .expect("group_by should include details")
        .get("expressions")
        .map(|value| {
            json_array(
                value,
                "broad_statement.details.body.details.group_by.details.expressions",
            )
        })
        .expect("group_by should include expressions");
    assert_eq!(group_by_expressions.len(), 1);
    assert_expr_compound_identifier(
        &group_by_expressions[0],
        &["s", "schema_key"],
        "broad_statement.details.body.details.group_by.details.expressions[0]",
    );

    let having = body_details
        .get("having")
        .expect("broad select should include having");
    assert!(find_value_with_kind(having, "function").is_some());

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
    assert_broad_alias_snapshot(
        root_relation_details
            .get("alias")
            .expect("broad root relation should include alias"),
        "s",
        "broad_statement.from[0].relation.details.alias",
    );
    assert_broad_relation_snapshot(
        root_relation_details
            .get("relation")
            .expect("broad root relation should include relation"),
        expected_surface_kind,
        "lix_state",
        "broad_statement.from[0].relation.details.relation",
    );

    let joins = root
        .get("joins")
        .map(|value| json_array(value, "broad_statement.from[0].joins"))
        .expect("broad root should include joins");
    assert_eq!(joins.len(), 1, "broad root should expose one join");
    let join = json_object(&joins[0], "broad_statement.from[0].joins[0]");
    assert_eq!(join.get("global").and_then(JsonValue::as_bool), Some(false));
    let join_kind = join
        .get("kind")
        .map(|value| json_object(value, "broad_statement.from[0].joins[0].kind"))
        .expect("broad join should include kind");
    assert_eq!(
        join_kind.get("kind").and_then(JsonValue::as_str),
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
    assert_broad_alias_snapshot(
        join_relation_details
            .get("alias")
            .expect("broad join relation should include alias"),
        "sv",
        "broad_statement.from[0].joins[0].relation.details.alias",
    );
    assert_broad_relation_snapshot(
        join_relation_details
            .get("relation")
            .expect("broad join relation should include relation"),
        expected_surface_kind,
        "lix_state_by_version",
        "broad_statement.from[0].joins[0].relation.details.relation",
    );
    let join_constraint = json_object(
        join_kind
            .get("details")
            .and_then(|value| {
                json_object(value, "broad_statement.from[0].joins[0].kind.details")
                    .get("constraint")
            })
            .expect("broad join kind should include constraint"),
        "broad_statement.from[0].joins[0].kind.details.constraint",
    );
    assert_eq!(
        join_constraint.get("kind").and_then(JsonValue::as_str),
        Some("on")
    );
    let join_constraint_expr = broad_expr_details(
        join_constraint
            .get("details")
            .and_then(|value| {
                json_object(
                    value,
                    "broad_statement.from[0].joins[0].kind.details.constraint.details",
                )
                .get("expr")
            })
            .expect("join constraint should include expr"),
        "broad_statement.from[0].joins[0].kind.details.constraint.details.expr",
    );
    assert_eq!(
        join_constraint_expr.get("op").and_then(JsonValue::as_str),
        Some("=")
    );
}

fn assert_nested_only_broad_public_read_statement_snapshot(
    broad_statement: &JsonValue,
    expected_directory_kind: &str,
    expected_file_kind: &str,
) {
    let statement = json_object(broad_statement, "nested_broad_statement");
    assert_no_broad_public_read_fallbacks(broad_statement, "nested_broad_statement");
    assert_eq!(
        statement.get("kind").and_then(JsonValue::as_str),
        Some("query")
    );
    let statement_details = statement
        .get("details")
        .map(|value| json_object(value, "nested_broad_statement.details"))
        .expect("nested broad statement should include details");
    let body = statement_details
        .get("body")
        .map(|value| json_object(value, "nested_broad_statement.details.body"))
        .expect("nested broad statement should include body");
    let body_details = body
        .get("details")
        .map(|value| json_object(value, "nested_broad_statement.details.body.details"))
        .expect("nested broad body should include details");
    let from = body_details
        .get("from")
        .map(|value| json_array(value, "nested_broad_statement.details.body.details.from"))
        .expect("nested broad body should include from");
    assert!(
        from.is_empty(),
        "nested-only broad query should have no top-level FROM relations"
    );
    let projection = body_details
        .get("projection")
        .map(|value| {
            json_array(
                value,
                "nested_broad_statement.details.body.details.projection",
            )
        })
        .expect("nested broad body should include projection");
    assert_eq!(projection.len(), 3);

    let scalar_projection = json_object(
        &projection[0],
        "nested_broad_statement.details.body.details.projection[0]",
    );
    let scalar_projection_details = scalar_projection
        .get("details")
        .map(|value| {
            json_object(
                value,
                "nested_broad_statement.details.body.details.projection[0].details",
            )
        })
        .expect("first nested projection should include details");
    assert_eq!(
        scalar_projection_details
            .get("alias")
            .and_then(JsonValue::as_str),
        Some("parent_change_id")
    );
    let scalar_expr = scalar_projection_details
        .get("expr")
        .expect("first nested projection should include expr");
    assert_eq!(
        broad_expr_kind(
            scalar_expr,
            "nested_broad_statement.details.body.details.projection[0].details.expr",
        ),
        "scalar_subquery"
    );

    let exists_projection = json_object(
        &projection[1],
        "nested_broad_statement.details.body.details.projection[1]",
    );
    let exists_projection_details = exists_projection
        .get("details")
        .map(|value| {
            json_object(
                value,
                "nested_broad_statement.details.body.details.projection[1].details",
            )
        })
        .expect("second nested projection should include details");
    assert_eq!(
        exists_projection_details
            .get("alias")
            .and_then(JsonValue::as_str),
        Some("has_child_dir")
    );
    let exists_expr = exists_projection_details
        .get("expr")
        .expect("second nested projection should include expr");
    assert_eq!(
        broad_expr_kind(
            exists_expr,
            "nested_broad_statement.details.body.details.projection[1].details.expr",
        ),
        "exists"
    );

    let in_projection = json_object(
        &projection[2],
        "nested_broad_statement.details.body.details.projection[2]",
    );
    let in_projection_details = in_projection
        .get("details")
        .map(|value| {
            json_object(
                value,
                "nested_broad_statement.details.body.details.projection[2].details",
            )
        })
        .expect("third nested projection should include details");
    assert_eq!(
        in_projection_details
            .get("alias")
            .and_then(JsonValue::as_str),
        Some("has_file")
    );
    let in_expr = in_projection_details
        .get("expr")
        .expect("third nested projection should include expr");
    assert_eq!(
        broad_expr_kind(
            in_expr,
            "nested_broad_statement.details.body.details.projection[2].details.expr",
        ),
        "in_subquery"
    );

    assert_query_root_surface(
        broad_expr_details(
            scalar_expr,
            "nested_broad_statement.details.body.details.projection[0].details.expr",
        )
        .get("query")
        .expect("scalar_subquery should include query"),
        expected_directory_kind,
        "lix_directory",
        "nested_broad_statement.details.body.details.projection[0].details.expr.details.query",
    );
    assert_query_root_surface(
        broad_expr_details(
            exists_expr,
            "nested_broad_statement.details.body.details.projection[1].details.expr",
        )
        .get("subquery")
        .expect("exists should include subquery"),
        expected_directory_kind,
        "lix_directory",
        "nested_broad_statement.details.body.details.projection[1].details.expr.details.subquery",
    );
    assert_query_root_surface(
        broad_expr_details(
            in_expr,
            "nested_broad_statement.details.body.details.projection[2].details.expr",
        )
        .get("subquery")
        .expect("in_subquery should include subquery"),
        expected_file_kind,
        "lix_file",
        "nested_broad_statement.details.body.details.projection[2].details.expr.details.subquery",
    );
}

#[test]
fn broad_explain_snapshot_model_has_no_fallback_variants() {
    let explain_src = include_str!("../../src/sql/explain/mod.rs");
    let set_expr_enum = rust_item_section(
        explain_src,
        "pub(crate) enum ExplainBroadPublicReadSetExprSnapshot",
        "pub(crate) enum ExplainBroadSetOperationKind",
    );
    let table_factor_enum = rust_item_section(
        explain_src,
        "pub(crate) enum ExplainBroadPublicReadTableFactorSnapshot",
        "pub(crate) struct ExplainBroadPublicReadAliasSnapshot",
    );

    assert!(
        !set_expr_enum.contains("Other"),
        "accepted broad explain snapshot model must not reintroduce set-expression fallback variants"
    );
    assert!(
        !table_factor_enum.contains("Other"),
        "accepted broad explain snapshot model must not reintroduce table-factor fallback variants"
    );
    assert!(
        !explain_src.contains("ExplainBroadPublicReadSetExprSnapshot::Other"),
        "accepted broad explain serialization must not construct set-expression fallback snapshots"
    );
    assert!(
        !explain_src.contains("ExplainBroadPublicReadTableFactorSnapshot::Other"),
        "accepted broad explain serialization must not construct table-factor fallback snapshots"
    );
}

fn assert_broad_public_read_typed_statement_contract(explain_json: &JsonValue) {
    let semantic_statement = json_object_at(explain_json, "semantic_statement", "explain_json");
    let semantic_details = semantic_statement
        .get("details")
        .map(|value| json_object(value, "semantic_statement.details"))
        .expect("semantic_statement should include details");
    assert_rich_broad_public_read_statement_snapshot(
        semantic_details
            .get("broad_statement")
            .expect("semantic_statement.details should include broad_statement"),
        "public",
    );

    let logical_plan = json_object_at(explain_json, "logical_plan", "explain_json");
    let logical_details = logical_plan
        .get("details")
        .map(|value| json_object(value, "logical_plan.details"))
        .expect("logical_plan should include details");
    assert_rich_broad_public_read_statement_snapshot(
        logical_details
            .get("broad_statement")
            .expect("logical_plan.details should include broad_statement"),
        "public",
    );
    assert_broad_public_read_relation_summary(
        logical_details
            .get("broad_relation_summary")
            .expect("logical_plan.details should include broad_relation_summary"),
        &["lix_file", "lix_state", "lix_state_by_version"],
        &[],
        &["latest"],
    );

    let optimized_logical_plan =
        json_object_at(explain_json, "optimized_logical_plan", "explain_json");
    let optimized_details = optimized_logical_plan
        .get("details")
        .map(|value| json_object(value, "optimized_logical_plan.details"))
        .expect("optimized_logical_plan should include details");
    assert_rich_broad_public_read_statement_snapshot(
        optimized_details
            .get("broad_statement")
            .expect("optimized_logical_plan.details should include broad_statement"),
        "lowered_public",
    );
    assert_broad_public_read_relation_summary(
        optimized_details
            .get("broad_relation_summary")
            .expect("optimized_logical_plan.details should include broad_relation_summary"),
        &[],
        &["lix_file", "lix_state", "lix_state_by_version"],
        &["latest"],
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
    assert_broad_public_read_physical_execution_contract(
        explain_json,
        &[
            "lix_internal_live_v1_lix_key_value",
            "lix_internal_live_v1_lix_file_descriptor",
        ],
    );
}

fn assert_broad_public_read_relation_summary(
    summary: &JsonValue,
    expected_public_relations: &[&str],
    expected_lowered_public_relations: &[&str],
    expected_cte_relations: &[&str],
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
        expected_cte_relations,
        "broad_relation_summary.cte_relations",
    );
}

fn assert_broad_public_read_physical_execution_contract(
    explain_json: &JsonValue,
    expected_render_sql_markers: &[&str],
) {
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
        &["details", "kind"],
        "physical_plan.details.details.statements[0]",
    );
    assert_eq!(
        statement.get("kind").and_then(JsonValue::as_str),
        Some("final")
    );
    let statement_details = statement
        .get("details")
        .map(|value| json_object(value, "physical_plan.details.details.statements[0].details"))
        .expect("physical broad plan should expose final statement details");
    assert_object_keys(
        statement_details,
        &["bindings", "statement_sql"],
        "physical_plan.details.details.statements[0].details",
    );
    let statement_sql = statement_details
        .get("statement_sql")
        .and_then(JsonValue::as_str)
        .expect("physical broad plan should expose final statement_sql");
    assert!(
        !statement_sql.contains("__lix_lowered_relation_"),
        "physical broad statement should not retain placeholder relation markers"
    );
    for marker in expected_render_sql_markers {
        assert!(
            statement_sql.contains(marker),
            "physical broad final statement should include marker {marker}: {statement_sql}"
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
            "read_semantics",
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
            .get("read_semantics")
            .and_then(JsonValue::as_str),
        Some("workspace_effective")
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

    let compiled_artifacts = json_object_at(explain_json, "compiled_artifacts", "explain_json");
    assert!(
        compiled_artifacts.get("surface_bindings").is_none(),
        "compiled_artifacts should not duplicate typed surface bindings as top-level names"
    );
    let bound_public_leaves = json_array(
        compiled_artifacts
            .get("bound_public_leaves")
            .expect("compiled_artifacts should include bound_public_leaves"),
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

    let pushdown = compiled_artifacts
        .get("pushdown")
        .map(|value| json_object(value, "compiled_artifacts.pushdown"))
        .expect("compiled_artifacts should include pushdown");
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

    let lowered_sql = compiled_artifacts
        .get("lowered_sql")
        .and_then(JsonValue::as_array)
        .and_then(|values| values.first())
        .and_then(JsonValue::as_str)
        .expect("compiled_artifacts.lowered_sql should expose the lowered query");
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
    let lowered_sql = json_object_at(explain_json, "compiled_artifacts", "explain_json")
        .get("lowered_sql")
        .and_then(JsonValue::as_array)
        .expect("compiled_artifacts.lowered_sql should be an array");
    if expected_non_empty {
        assert!(
            !lowered_sql.is_empty(),
            "compiled_artifacts.lowered_sql should be populated"
        );
    } else {
        assert!(
            lowered_sql.is_empty(),
            "compiled_artifacts.lowered_sql should be empty"
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

    let compiled_artifacts = json_object_at(explain_json, "compiled_artifacts", "explain_json");
    assert!(
        compiled_artifacts.get("surface_bindings").is_none(),
        "compiled_artifacts should not duplicate typed surface bindings as top-level names"
    );
    assert!(
        compiled_artifacts.get("write_phase_trace").is_none(),
        "plain public-write explain should not expose a static write phase trace shim"
    );

    let commit_preconditions = json_array(
        compiled_artifacts
            .get("commit_preconditions")
            .expect("compiled_artifacts should include commit_preconditions"),
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

    let change_batches = json_array(
        compiled_artifacts
            .get("change_batches")
            .expect("compiled_artifacts should include change_batches"),
        "change_batches",
    );
    assert_eq!(change_batches.len(), 1);
    let change_batch = json_object(&change_batches[0], "change_batch");
    assert_eq!(
        change_batch.get("write_lane").and_then(JsonValue::as_str),
        Some("active_version")
    );

    assert_no_rust_debug_leaks(explain_json);
}

simulation_test!(
    explain_text_surface_returns_sections_for_lix_state_query,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
                "compiled_artifacts",
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
                ("stage_timings", &["parse:", "artifact_preparation:"]),
            ],
        );
    }
);

simulation_test!(
    explain_public_read_json_matches_contract,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
                "routing",
                "capability_resolution",
                "physical_planning",
                "artifact_preparation",
            ]
            .as_slice(),
        );
    }
);

simulation_test!(
    explain_direct_history_public_read_omits_artifact_preparation,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
                "routing",
                "physical_planning",
            ],
        );
        assert_missing_stage_names(
            explain_json,
            &["capability_resolution", "artifact_preparation"],
        );
    }
);

simulation_test!(
    explain_direct_history_public_read_exposes_typed_nested_plan_artifacts,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(broad_public_read_stage_contract_query(), &[])
            .await
            .unwrap();

        let explain_json = explain_json_payload(&result);
        assert_public_read_logical_strategy(explain_json, "logical_plan", "broad");
        assert_public_read_logical_strategy(explain_json, "optimized_logical_plan", "broad");
        assert_broad_public_read_typed_statement_contract(explain_json);
        assert_public_read_physical_kind(explain_json, "lowered_sql");
        assert_lowered_sql_presence(explain_json, true);
        assert_stage_timings_contract(explain_json, BROAD_PUBLIC_READ_STAGE_CONTRACT);
        assert_missing_stage_names(explain_json, &["semantic_analysis"]);
    }
);

simulation_test!(
    explain_broad_public_read_rejects_unsupported_values_branch_early,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let error = engine
            .execute(
                "EXPLAIN (FORMAT JSON) \
                 SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' \
                 UNION VALUES ('entity-1')",
                &[],
            )
            .await
            .expect_err("unsupported VALUES broad branch should fail during binding");

        assert_eq!(error.code, "LIX_ERROR_INVALID_INPUT");
        assert!(
            error
                .description
                .contains("broad public reads do not support VALUES query bodies"),
            "unexpected error: {}",
            error.description
        );
        assert!(
            error.description.contains("VALUES ('entity-1')"),
            "unexpected error: {}",
            error.description
        );
        assert!(
            !error.description.contains("legacy set-expression fallback"),
            "unsupported broad VALUES must fail before any fallback-lowering defense: {}",
            error.description
        );
    }
);

simulation_test!(
    explain_broad_public_read_rejects_unsupported_unnest_table_factor_early,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let error = engine
            .execute(
                "EXPLAIN (FORMAT JSON) \
                 SELECT s.entity_id \
                 FROM lix_state s \
                 JOIN UNNEST(items) AS expanded ON 1 = 1 \
                 WHERE s.schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect_err("unsupported UNNEST broad table factor should fail during binding");

        assert_eq!(error.code, "LIX_ERROR_INVALID_INPUT");
        assert!(
            error
                .description
                .contains("broad public reads do not support UNNEST table factors"),
            "unexpected error: {}",
            error.description
        );
        assert!(
            error.description.contains("UNNEST(items)"),
            "unexpected error: {}",
            error.description
        );
        assert!(
            !error.description.contains("legacy table-factor fallback"),
            "unsupported broad UNNEST must fail before any fallback-lowering defense: {}",
            error.description
        );
    }
);

simulation_test!(
    explain_broad_public_read_nested_subquery_optimization_delta,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "EXPLAIN (FORMAT JSON) \
                 SELECT \
                   (SELECT lixcol_change_id FROM lix_directory WHERE id = 'dir-stable-parent') AS parent_change_id, \
                   EXISTS (SELECT 1 FROM lix_directory WHERE id = 'dir-stable-child') AS has_child_dir, \
                   'file-stable-child' IN (SELECT id FROM lix_file WHERE path = '/hello.txt') AS has_file",
                &[],
            )
            .await
            .unwrap();

        let explain_json = explain_json_payload(&result);
        let logical_details = json_object_at(explain_json, "logical_plan", "explain_json")
            .get("details")
            .map(|value| json_object(value, "logical_plan.details"))
            .expect("logical_plan should include details");
        let optimized_details =
            json_object_at(explain_json, "optimized_logical_plan", "explain_json")
                .get("details")
                .map(|value| json_object(value, "optimized_logical_plan.details"))
                .expect("optimized_logical_plan should include details");

        assert_nested_only_broad_public_read_statement_snapshot(
            logical_details
                .get("broad_statement")
                .expect("logical_plan.details should include broad_statement"),
            "public",
            "public",
        );
        assert_nested_only_broad_public_read_statement_snapshot(
            optimized_details
                .get("broad_statement")
                .expect("optimized_logical_plan.details should include broad_statement"),
            "lowered_public",
            "lowered_public",
        );
        assert_broad_public_read_relation_summary(
            logical_details
                .get("broad_relation_summary")
                .expect("logical_plan.details should include broad_relation_summary"),
            &["lix_directory", "lix_file"],
            &[],
            &[],
        );
        assert_broad_public_read_relation_summary(
            optimized_details
                .get("broad_relation_summary")
                .expect("optimized_logical_plan.details should include broad_relation_summary"),
            &[],
            &["lix_directory", "lix_file"],
            &[],
        );
        assert_ne!(
            logical_details.get("broad_statement"),
            optimized_details.get("broad_statement"),
            "nested-only broad public-read explain should show routing deltas inside nested subqueries",
        );
        assert_ne!(
            logical_details.get("broad_relation_summary"),
            optimized_details.get("broad_relation_summary"),
            "nested-only broad relation summaries should reflect nested routed public relations",
        );
    }
);

simulation_test!(
    explain_broad_public_read_binding_delay_lands_in_bind_stage,
    simulations = [sqlite, postgres],
    |sim| async move {
        let delay = std::time::Duration::from_millis(150);
        let _binding_delay_guard = lix_engine::delay_broad_binding_for_test(delay);

        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(broad_public_read_stage_contract_query(), &[])
            .await
            .unwrap();

        let explain_json = explain_json_payload(&result);
        let threshold_us = (delay.as_micros() / 2) as u64;
        assert_stage_timings_contract(explain_json, BROAD_PUBLIC_READ_STAGE_CONTRACT);
        assert_missing_stage_names(explain_json, &["semantic_analysis"]);
        assert_stage_duration_at_least(explain_json, "bind", threshold_us);
        assert_stage_duration_below(explain_json, "logical_planning", threshold_us);
    }
);

simulation_test!(
    explain_broad_public_read_routing_delay_lands_in_routing_stage,
    simulations = [sqlite, postgres],
    |sim| async move {
        let delay = std::time::Duration::from_millis(150);
        let _routing_delay_guard = lix_engine::delay_broad_routing_for_test(delay);

        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(broad_public_read_stage_contract_query(), &[])
            .await
            .unwrap();

        let explain_json = explain_json_payload(&result);
        let threshold_us = (delay.as_micros() / 2) as u64;
        assert_stage_timings_contract(explain_json, BROAD_PUBLIC_READ_STAGE_CONTRACT);
        assert_missing_stage_names(explain_json, &["semantic_analysis"]);
        assert_stage_duration_at_least(explain_json, "routing", threshold_us);
        assert_stage_duration_below(explain_json, "physical_planning", threshold_us);
    }
);

simulation_test!(
    explain_broad_public_read_text_shows_optimization_delta,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
                "routing",
                "capability_resolution",
                "physical_planning",
                "artifact_preparation",
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            &["routing", "capability_resolution", "artifact_preparation"],
        );
        assert_public_write_json_contract(explain_json);
    }
);

simulation_test!(
    explain_public_write_update_contract_pins_target_set_proof_kind,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
                "routing",
                "capability_resolution",
                "physical_planning",
                "artifact_preparation",
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
                "compiled_artifacts",
                "stage_timings",
            ],
            &[
                ("request", &["mode: plan", "format: text"]),
                ("semantic_statement", &["kind: internal"]),
                (
                    "logical_plan",
                    &["kind: internal", "result_contract: select"],
                ),
                ("compiled_artifacts", &["lowered_sql_statements: 1"]),
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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
