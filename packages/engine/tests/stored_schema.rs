mod support;

use lix_engine::Value;

simulation_test!(stored_schema_registers_materialized_table, |sim| async move {
    let engine = sim
        .boot_simulated_engine()
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1.0.0\"}}'\
             )",
            &[],
        )
        .await
        .unwrap();

    let stored = engine
        .execute(
            "SELECT entity_id, schema_key, version_id, file_id, plugin_key, change_id, is_tombstone, created_at, updated_at, snapshot_content \
             FROM lix_internal_state_materialized_v1_lix_stored_schema \
             WHERE entity_id = 'test_schema~1.0.0'",
            &[],
        )
        .await
        .unwrap();

    sim.expect_deterministic(stored.rows.clone());
    assert_eq!(stored.rows.len(), 1);
    let row = &stored.rows[0];
    assert_eq!(row[0], Value::Text("test_schema~1.0.0".to_string()));
    assert_eq!(row[1], Value::Text("lix_stored_schema".to_string()));
    assert_eq!(row[2], Value::Text("global".to_string()));
    assert_eq!(row[3], Value::Text("lix".to_string()));
    assert_eq!(row[4], Value::Text("lix".to_string()));
    assert_eq!(row[5], Value::Text("schema".to_string()));
    assert_eq!(row[6], Value::Integer(0));
    assert_eq!(row[7], Value::Text("1970-01-01T00:00:00Z".to_string()));
    assert_eq!(row[8], Value::Text("1970-01-01T00:00:00Z".to_string()));
    assert_eq!(
        row[9],
        Value::Text(
            "{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1.0.0\"}}".to_string()
        )
    );

    let table_exists = engine
        .execute(
            "SELECT COUNT(*) FROM lix_internal_state_materialized_v1_test_schema",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(table_exists.rows[0][0], Value::Integer(0));
});
