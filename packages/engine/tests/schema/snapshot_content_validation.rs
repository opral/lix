use lix_engine::Value;

simulation_test!(allows_valid_snapshot, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");

    engine.initialize().await.unwrap();

    engine
        .register_schema(
            &serde_json::from_str::<serde_json::Value>(
                "{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"],\"additionalProperties\":false}",
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let result = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'test_schema', NULL, lix_active_version_id(), NULL, '{\"name\":\"Ada\"}', '1'\
             )", &[])
            .await;

    assert!(result.is_ok(), "{result:?}");

    let stored = engine
            .execute(
                "SELECT snapshot_content FROM lix_state_by_version \
             WHERE schema_key = 'test_schema' AND entity_id = 'entity-1' AND file_id IS NULL AND version_id = lix_active_version_id()", &[])
            .await
            .unwrap();

    assert_eq!(
        stored.statements[0].rows[0][0],
        Value::Text("{\"name\":\"Ada\"}".to_string())
    );
});

simulation_test!(rejects_invalid_snapshot, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");

    engine.initialize().await.unwrap();

    engine
        .register_schema(
            &serde_json::from_str::<serde_json::Value>(
                "{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"],\"additionalProperties\":false}",
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let result = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'test_schema', NULL, lix_active_version_id(), NULL, '{\"missing\":\"field\"}', '1'\
             )", &[])
            .await;

    let err = result.expect_err("expected validation error");
    assert!(
        err.to_string()
            .contains("snapshot_content does not match schema 'test_schema' (1)"),
        "unexpected error: {err}"
    );
});

simulation_test!(requires_registered_schema, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");

    engine.initialize().await.unwrap();

    let result = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'missing_schema', NULL, lix_active_version_id(), NULL, '{\"name\":\"Ada\"}', '1'\
             )", &[])
            .await;

    let err = result.expect_err("expected validation error");
    assert!(
        err.to_string()
            .contains("schema 'missing_schema' (1) is not stored"),
        "unexpected error: {err}"
    );
});

simulation_test!(rejects_invalid_update, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");

    engine.initialize().await.unwrap();

    engine
        .register_schema(
            &serde_json::from_str::<serde_json::Value>(
                "{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"],\"additionalProperties\":false}",
            )
            .unwrap(),
        )
        .await
        .unwrap();

    engine
            .execute(
                "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'test_schema', NULL, lix_active_version_id(), NULL, '{\"name\":\"Ada\"}', '1'\
             )", &[])
            .await
            .unwrap();

    let result = engine
            .execute(
                "UPDATE lix_state_by_version SET snapshot_content = '{\"missing\":\"field\"}' \
             WHERE entity_id = 'entity-1' AND schema_key = 'test_schema' AND file_id IS NULL AND version_id = lix_active_version_id()", &[])
            .await;

    let err = result.expect_err("expected validation error");
    assert!(
        err.to_string()
            .contains("snapshot_content does not match schema 'test_schema' (1)"),
        "unexpected error: {err}"
    );
});
