use lix_engine::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    entity_history_reads_typed_rows_from_commit_graph,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_history_schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"count\":{\"type\":\"integer\"},\"active\":{\"type\":\"boolean\"},\"meta\":{\"type\":\"object\"}},\"required\":[\"id\",\"count\",\"active\",\"meta\"],\"additionalProperties\":false}'),\
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
                 (lixcol_entity_pk, id, count, active, meta, lixcol_untracked) \
                 VALUES (lix_json('[\"history-entity\"]'), 'history-entity', 1, true, lix_json('{\"source\":\"insert\"}'), false)",
                &[],
            )
            .await
            .expect("entity insert should succeed");
        let first_commit_id = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("first head should load")
            .expect("first head should exist");

        session
            .execute(
                "UPDATE engine_history_schema \
                 SET count = 2, active = false, meta = lix_json('{\"source\":\"update\"}') \
                 WHERE lixcol_entity_pk = lix_json('[\"history-entity\"]')",
                &[],
            )
            .await
            .expect("entity update should succeed");
        let second_commit_id = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("second head should load")
            .expect("second head should exist");
        assert_ne!(first_commit_id, second_commit_id);

        let result = session
            .execute(
                &format!(
                    "SELECT id, count, active, meta, lixcol_entity_pk, lixcol_observed_commit_id, lixcol_start_commit_id, lixcol_depth \
                     FROM engine_history_schema_history \
                     WHERE lixcol_start_commit_id = '{second_commit_id}' \
                       AND lixcol_entity_pk = lix_json('[\"history-entity\"]') \
                     ORDER BY lixcol_depth"
                ),
                &[],
            )
            .await
            .expect("entity history read should succeed");

        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("history-entity".to_string()),
                    Value::Integer(2),
                    Value::Boolean(false),
                    Value::Json(json!({"source": "update"})),
                    Value::Json(json!(["history-entity"])),
                    Value::Text(second_commit_id.clone()),
                    Value::Text(second_commit_id.clone()),
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("history-entity".to_string()),
                    Value::Integer(1),
                    Value::Boolean(true),
                    Value::Json(json!({"source": "insert"})),
                    Value::Json(json!(["history-entity"])),
                    Value::Text(first_commit_id),
                    Value::Text(second_commit_id),
                    Value::Integer(1),
                ],
            ],
        );
    }
);

simulation_test!(
    entity_history_requires_lixcol_start_commit_id,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_history_error_schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        let error = session
            .execute("SELECT id FROM engine_history_error_schema_history", &[])
            .await
            .expect_err("typed history queries must provide start commit");

        assert_eq!(
            error.code,
            lix_engine::LixError::CODE_HISTORY_FILTER_REQUIRED
        );
        assert!(
            error
                .to_string()
                .contains("requires a lixcol_start_commit_id filter"),
            "unexpected error: {error}"
        );
        assert!(
            error
                .hint()
                .is_some_and(|hint| hint.contains("WHERE lixcol_start_commit_id")),
            "unexpected error: {error}"
        );
    }
);

simulation_test!(
    entity_history_rejects_bare_start_commit_id_filter,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_history_bare_error_schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        let error = session
            .execute(
                "SELECT id \
                 FROM engine_history_bare_error_schema_history \
                 WHERE start_commit_id = lix_active_version_commit_id()",
                &[],
            )
            .await
            .expect_err("typed history should only expose lixcol_start_commit_id");

        assert_eq!(error.code, lix_engine::LixError::CODE_COLUMN_NOT_FOUND);
        assert!(
            error.to_string().contains("start_commit_id"),
            "unexpected error: {error}"
        );
    }
);
