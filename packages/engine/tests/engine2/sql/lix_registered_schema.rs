use crate::simulation_test2;
use lix_engine::engine2::ExecuteResult;
use lix_engine::Value;

simulation_test2!(
    lix_registered_schema_insert_makes_schema_visible_to_lix_state,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim
            .open_main_session(&engine)
            .await
            .expect("main session should open");

        let register_schema_result = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine2_dummy_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             true,\
             true\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");
        assert_eq!(register_schema_result, ExecuteResult::AffectedRows(1));

        let insert_state_result = session
        .execute(
            "INSERT INTO lix_state (\
             entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version, global, untracked\
             ) VALUES (\
             'dummy-1', 'engine2_dummy_schema', NULL, NULL, lix_json('{\"id\":\"dummy-1\",\"name\":\"Dummy\"}'), '1', false, true\
             )",
            &[],
        )
        .await
        .expect("lix_state insert for registered schema should succeed");
        assert_eq!(insert_state_result, ExecuteResult::AffectedRows(1));

        let result = session
            .execute(
                "SELECT entity_id, schema_key, snapshot_content \
             FROM lix_state \
             WHERE schema_key = 'engine2_dummy_schema' AND entity_id = 'dummy-1'",
                &[],
            )
            .await
            .expect("lix_state read should succeed");
        let ExecuteResult::Rows(row_set) = result else {
            panic!("SELECT should return rows");
        };
        assert_eq!(row_set.len(), 1);
        assert_eq!(
            row_set.rows()[0].values(),
            &[
                Value::Text("dummy-1".to_string()),
                Value::Text("engine2_dummy_schema".to_string()),
                Value::Text("{\"id\":\"dummy-1\",\"name\":\"Dummy\"}".to_string()),
            ]
        );
    }
);

simulation_test2!(entity_by_version_expands_global_rows, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim
        .open_main_session(&engine)
        .await
        .expect("main session should open");

    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine2_overlay_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             true,\
             true\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

    session
            .execute(
                "INSERT INTO engine2_overlay_schema \
                 (lixcol_entity_id, id, name, lixcol_global, lixcol_untracked) \
                 VALUES ('entity-global-overlay', 'entity-global-overlay', 'Global Entity', true, false)",
                &[],
            )
            .await
            .expect("global entity insert should succeed");

    let result = session
        .execute(
            "SELECT id, name, lixcol_version_id, lixcol_global, lixcol_untracked \
                 FROM engine2_overlay_schema_by_version \
                 WHERE lixcol_entity_id = 'entity-global-overlay' \
                 ORDER BY lixcol_version_id",
            &[],
        )
        .await
        .expect("entity by-version read should succeed");
    assert_rows_eq(
        result,
        vec![
            vec![
                Value::Text("entity-global-overlay".to_string()),
                Value::Text("Global Entity".to_string()),
                Value::Text(sim.main_version_id().to_string()),
                Value::Boolean(true),
                Value::Boolean(false),
            ],
            vec![
                Value::Text("entity-global-overlay".to_string()),
                Value::Text("Global Entity".to_string()),
                Value::Text("global".to_string()),
                Value::Boolean(true),
                Value::Boolean(false),
            ],
        ],
    );
});

fn assert_rows_eq(result: ExecuteResult, expected: Vec<Vec<Value>>) {
    let ExecuteResult::Rows(row_set) = result else {
        panic!("SELECT should return rows");
    };
    let rows = row_set
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(rows, expected);
}
