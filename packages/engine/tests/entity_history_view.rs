mod support;

use lix_engine::Value;

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

async fn active_version_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let rows = engine
        .execute(
            "SELECT version_id FROM lix_active_version ORDER BY id LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected text active version id, got {other:?}"),
    }
}

async fn seed_key_value_row(
    engine: &support::simulation_test::SimulationEngine,
    key: &str,
    value: &str,
    version_id: &str,
) {
    let sql = format!(
        "INSERT INTO lix_internal_state_vtable (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{entity_id}', 'lix_key_value', 'lix', '{version_id}', 'lix', '{snapshot}', '1'\
         )",
        entity_id = key.replace('\'', "''"),
        version_id = version_id.replace('\'', "''"),
        snapshot = format!(
            "{{\"key\":\"{}\",\"value\":\"{}\"}}",
            key.replace('\"', "\\\""),
            value.replace('\"', "\\\"")
        )
        .replace('\'', "''"),
    );
    engine.execute(&sql, &[]).await.unwrap();
}

simulation_test!(
    lix_entity_history_select_projects_property_columns,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();
        let version_id = active_version_id(&engine).await;

        seed_key_value_row(&engine, "key-history", "value-history", &version_id).await;

        let rows = engine
            .execute(
                "SELECT key, value, lixcol_commit_id, lixcol_depth \
                 FROM lix_key_value_history \
                 WHERE key = 'key-history' \
                 ORDER BY lixcol_depth ASC",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.rows.clone());
        assert!(!rows.rows.is_empty());
        assert_text(&rows.rows[0][0], "key-history");
        assert_text(&rows.rows[0][1], "value-history");
        assert!(matches!(rows.rows[0][2], Value::Text(_)));
        assert!(matches!(rows.rows[0][3], Value::Integer(_)));
    }
);

simulation_test!(lix_entity_history_rejects_writes, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    let insert_err = engine
        .execute(
            "INSERT INTO lix_key_value_history (entity_id) VALUES ('x')",
            &[],
        )
        .await
        .expect_err("history insert should fail");
    assert_eq!(insert_err.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");

    let update_err = engine
        .execute("UPDATE lix_key_value_history SET entity_id = 'x'", &[])
        .await
        .expect_err("history update should fail");
    assert_eq!(update_err.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");

    let delete_err = engine
        .execute("DELETE FROM lix_key_value_history", &[])
        .await
        .expect_err("history delete should fail");
    assert_eq!(delete_err.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");
});
