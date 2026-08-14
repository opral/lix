use lix::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    row_history_reads_typed_rows_from_commit_graph,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_history_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"count\",\"type\":\"int8\",\"nullable\":false},{\"name\":\"active\",\"type\":\"boolean\",\"nullable\":false},{\"name\":\"meta\",\"type\":\"jsonb\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO engine_history_schema \
                 (lixcol_row_pk, id, count, active, meta, lixcol_untracked) \
                 VALUES (CAST('[\"history-row\"]' AS JSONB), 'history-row', 1, true, CAST('{\"source\":\"insert\"}' AS JSONB), false)",
                &[],
            )
            .await
            .expect("row insert should succeed");
        let first_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("first head should load")
            .expect("first head should exist");

        session
            .execute(
                "UPDATE engine_history_schema \
                 SET count = 2, active = false, meta = CAST('{\"source\":\"update\"}' AS JSONB) \
                 WHERE lixcol_row_pk = CAST('[\"history-row\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("row update should succeed");
        let second_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("second head should load")
            .expect("second head should exist");
        assert_ne!(first_commit_id, second_commit_id);

        let result = session
            .execute(
                &format!(
                    "SELECT id, count, active, meta, lixcol_row_pk, lixcol_observed_commit_id, lixcol_is_deleted, lixcol_depth \
                     FROM engine_history_schema_history('{second_commit_id}') \
                     WHERE lixcol_row_pk = CAST('[\"history-row\"]' AS JSONB) \
                     ORDER BY lixcol_depth"
                ),
                &[],
            )
            .await
            .expect("row history read should succeed");

        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("history-row".to_string()),
                    Value::Integer(2),
                    Value::Boolean(false),
                    Value::Json(json!({"source": "update"}).into()),
                    Value::Json(json!(["history-row"]).into()),
                    Value::Text(second_commit_id.clone()),
                    Value::Boolean(false),
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("history-row".to_string()),
                    Value::Integer(1),
                    Value::Boolean(true),
                    Value::Json(json!({"source": "insert"}).into()),
                    Value::Json(json!(["history-row"]).into()),
                    Value::Text(first_commit_id),
                    Value::Boolean(false),
                    Value::Integer(1),
                ],
            ],
        );
    }
);

simulation_test!(row_history_defaults_to_active_head, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_history_error_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

    session
        .execute(
            "INSERT INTO engine_history_error_schema \
                 (lixcol_row_pk, id, lixcol_untracked) \
                 VALUES (CAST('[\"history-default\"]' AS JSONB), 'history-default', false)",
            &[],
        )
        .await
        .expect("row insert should succeed");
    let result = session
        .execute(
            "SELECT id, lixcol_depth \
                 FROM engine_history_error_schema_history() \
                 WHERE id = 'history-default'",
            &[],
        )
        .await
        .expect("typed history should default to the active head");

    assert_rows_eq(
        result,
        vec![vec![
            Value::Text("history-default".to_string()),
            Value::Integer(0),
        ]],
    );
});

simulation_test!(row_history_rejects_retired_anchor_names, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_history_bare_error_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

    for retired in ["start_commit_id", "lixcol_start_commit_id"] {
        let error = session
            .execute(
                &format!(
                    "SELECT id \
                         FROM engine_history_bare_error_schema_history() \
                         WHERE {retired} = lix_active_branch_commit_id()"
                ),
                &[],
            )
            .await
            .expect_err("retired history anchor must fail");

        assert_eq!(error.code, lix::LixError::CODE_COLUMN_NOT_FOUND);
        assert!(
            error.to_string().contains(retired),
            "unexpected error: {error}"
        );
    }
});
