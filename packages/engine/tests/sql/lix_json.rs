use lix_engine::{LixError, Value};
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    lix_json_expression_results_are_semantic_json,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let result = session
            .execute(
                "SELECT \
                lix_json('{\"name\":\"Ada\",\"tags\":[\"db\"]}') AS document, \
                lix_json(NULL) AS json_null, \
                lix_json_get('{\"name\":\"Ada\",\"tags\":[\"db\"]}', 'tags') AS tags, \
                lix_json_get('{\"name\":\"Ada\"}', 'missing') AS missing",
                &[],
            )
            .await
            .expect("select should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Json(json!({"name": "Ada", "tags": ["db"]})),
                Value::Json(json!(null)),
                Value::Json(json!(["db"])),
                Value::Null,
            ]],
        );
    }
);

simulation_test!(lix_json_get_uses_variadic_path_segments, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let result = session
            .execute(
                "SELECT lix_json_get_text('{\"user\":{\"names\":[\"Ada\"]}}', 'user', 'names', 0) AS name",
                &[],
            )
            .await
            .expect("select should succeed");

    assert_rows_eq(result, vec![vec![Value::Text("Ada".to_string())]]);
});

simulation_test!(lix_json_get_rejects_jsonpath_strings, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let error = session
        .execute(
            "SELECT lix_json_get_text('{\"path\":\"ok\"}', '$.path')",
            &[],
        )
        .await
        .expect_err("JSONPath-looking strings should fail loudly");

    assert_eq!(error.code, LixError::CODE_INVALID_JSON_PATH);
    assert!(
        error
            .hint()
            .is_some_and(|hint| hint.contains("lix_json_get_text")),
        "expected path segment hint: {error}"
    );
});
