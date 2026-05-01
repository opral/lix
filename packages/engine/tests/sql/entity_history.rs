use lix_engine::Value;

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
                 lix_json('{\"x-lix-key\":\"engine2_history_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"count\":{\"type\":\"integer\"},\"active\":{\"type\":\"boolean\"},\"meta\":{\"type\":\"object\"}},\"required\":[\"id\",\"count\",\"active\",\"meta\"],\"additionalProperties\":false}'),\
                 true,\
                 true\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO engine2_history_schema \
                 (lixcol_entity_id, id, count, active, meta, lixcol_untracked) \
                 VALUES ('history-entity', 'history-entity', 1, true, lix_json('{\"source\":\"insert\"}'), false)",
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
                "UPDATE engine2_history_schema \
                 SET count = 2, active = false, meta = lix_json('{\"source\":\"update\"}') \
                 WHERE lixcol_entity_id = 'history-entity'",
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
                    "SELECT id, count, active, meta, lixcol_entity_id, lixcol_commit_id, lixcol_start_commit_id, lixcol_depth \
                     FROM engine2_history_schema_history \
                     WHERE lixcol_start_commit_id = '{second_commit_id}' \
                       AND lixcol_entity_id = 'history-entity' \
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
                    Value::Text("{\"source\":\"update\"}".to_string()),
                    Value::Text("history-entity".to_string()),
                    Value::Text(second_commit_id.clone()),
                    Value::Text(second_commit_id.clone()),
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("history-entity".to_string()),
                    Value::Integer(1),
                    Value::Boolean(true),
                    Value::Text("{\"source\":\"insert\"}".to_string()),
                    Value::Text("history-entity".to_string()),
                    Value::Text(first_commit_id),
                    Value::Text(second_commit_id),
                    Value::Integer(1),
                ],
            ],
        );
    }
);
