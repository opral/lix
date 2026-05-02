use lix_engine::Value;
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
                lix_json_extract('{\"name\":\"Ada\",\"tags\":[\"db\"]}', 'tags') AS tags, \
                lix_json_extract('{\"name\":\"Ada\"}', 'missing') AS missing",
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
