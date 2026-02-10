mod support;

use lix_engine::Value;

simulation_test!(allows_valid_snapshot, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

    let result = engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Ada\"}', '1'\
             )",
                &[],
            )
            .await;

    assert!(result.is_ok(), "{result:?}");

    let stored = engine
            .execute(
                "SELECT snapshot_content FROM lix_internal_state_vtable \
             WHERE schema_key = 'test_schema' AND entity_id = 'entity-1' AND file_id = 'file-1' AND version_id = 'version-1'",
                &[],
            )
            .await
            .unwrap();

    assert_eq!(
        stored.rows[0][0],
        Value::Text("{\"name\":\"Ada\"}".to_string())
    );
});

simulation_test!(rejects_invalid_snapshot, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

    let result = engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"missing\":\"field\"}', '1'\
             )",
                &[],
            )
            .await;

    let err = result.expect_err("expected validation error");
    assert!(
        err.to_string()
            .contains("snapshot_content does not match schema 'test_schema' (1)"),
        "unexpected error: {err}"
    );
});

simulation_test!(requires_stored_schema, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    let result = engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'missing_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Ada\"}', '1'\
             )",
                &[],
            )
            .await;

    let err = result.expect_err("expected validation error");
    assert!(
        err.to_string()
            .contains("schema 'missing_schema' (1) is not stored"),
        "unexpected error: {err}"
    );
});

simulation_test!(
    rejects_insert_with_mismatched_primary_key_entity_id,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"pk_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}}'\
             )",
            &[],
        )
        .await
        .unwrap();

        let result = engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'pk_schema', 'file-1', 'version-1', 'lix', '{\"id\":\"entity-2\",\"name\":\"Ada\"}', '1'\
             )",
            &[],
        )
        .await;

        let err = result.expect_err("expected entity_id consistency error");
        assert!(
            err.to_string()
                .contains("entity_id 'entity-1' is inconsistent for schema 'pk_schema' (1)"),
            "unexpected error: {err}"
        );
    }
);

simulation_test!(rejects_invalid_update, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

    engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Ada\"}', '1'\
             )",
                &[],
            )
            .await
            .unwrap();

    let result = engine
            .execute(
                "UPDATE lix_internal_state_vtable SET snapshot_content = '{\"missing\":\"field\"}' \
             WHERE entity_id = 'entity-1' AND schema_key = 'test_schema' AND file_id = 'file-1' AND version_id = 'version-1'",
                &[],
            )
            .await;

    let err = result.expect_err("expected validation error");
    assert!(
        err.to_string()
            .contains("snapshot_content does not match schema 'test_schema' (1)"),
        "unexpected error: {err}"
    );
});

simulation_test!(rejects_update_on_immutable_schema, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"immutable_schema\",\"x-lix-version\":\"1\",\"x-lix-immutable\":true,\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

    engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'immutable_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Ada\"}', '1'\
             )",
                &[],
            )
            .await
            .unwrap();

    let result = engine
            .execute(
                "UPDATE lix_internal_state_vtable SET snapshot_content = '{\"name\":\"Grace\"}' \
             WHERE entity_id = 'entity-1' AND schema_key = 'immutable_schema' AND file_id = 'file-1' AND version_id = 'version-1'",
                &[],
            )
            .await;

    let err = result.expect_err("expected immutability error");
    assert!(
        err.to_string()
            .contains("Schema 'immutable_schema' is immutable and cannot be updated."),
        "unexpected error: {err}"
    );
});

simulation_test!(
    rejects_update_with_mismatched_primary_key_entity_id,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"pk_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}}'\
             )",
            &[],
        )
        .await
        .unwrap();

        engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'pk_schema', 'file-1', 'version-1', 'lix', '{\"id\":\"entity-1\",\"name\":\"Ada\"}', '1'\
             )",
            &[],
        )
        .await
        .unwrap();

        let result = engine
        .execute(
            "UPDATE lix_internal_state_vtable SET snapshot_content = '{\"id\":\"entity-2\",\"name\":\"Ada\"}' \
             WHERE entity_id = 'entity-1' AND schema_key = 'pk_schema' AND file_id = 'file-1' AND version_id = 'version-1'",
            &[],
        )
        .await;

        let err = result.expect_err("expected entity_id consistency error");
        assert!(
            err.to_string()
                .contains("entity_id 'entity-1' is inconsistent for schema 'pk_schema' (1)"),
            "unexpected error: {err}"
        );
    }
);

simulation_test!(
    allows_composite_primary_key_entity_id_roundtrip,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"composite_pk_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\",\"/locale\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"locale\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"locale\",\"name\"],\"additionalProperties\":false}}'\
             )",
            &[],
        )
        .await
        .unwrap();

        let insert_result = engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1~en', 'composite_pk_schema', 'file-1', 'version-1', 'lix', '{\"id\":\"entity-1\",\"locale\":\"en\",\"name\":\"Ada\"}', '1'\
             )",
            &[],
        )
        .await;
        assert!(insert_result.is_ok(), "{insert_result:?}");

        let update_result = engine
        .execute(
            "UPDATE lix_internal_state_vtable SET snapshot_content = '{\"id\":\"entity-1\",\"locale\":\"en\",\"name\":\"Ada Lovelace\"}' \
             WHERE entity_id = 'entity-1~en' AND schema_key = 'composite_pk_schema' AND file_id = 'file-1' AND version_id = 'version-1'",
            &[],
        )
        .await;
        assert!(update_result.is_ok(), "{update_result:?}");
    }
);

simulation_test!(allows_delete_on_immutable_schema, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"immutable_schema\",\"x-lix-version\":\"1\",\"x-lix-immutable\":true,\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

    engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'immutable_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Ada\"}', '1'\
             )",
                &[],
            )
            .await
            .unwrap();

    let result = engine
            .execute(
                "DELETE FROM lix_internal_state_vtable \
             WHERE entity_id = 'entity-1' AND schema_key = 'immutable_schema' AND file_id = 'file-1' AND version_id = 'version-1'",
                &[],
            )
            .await;

    assert!(result.is_ok(), "{result:?}");

    let stored = engine
        .execute(
            "SELECT snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'immutable_schema' \
               AND entity_id = 'entity-1' \
               AND file_id = 'file-1' \
               AND version_id = 'version-1'",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(stored.rows.len(), 1);
    assert_eq!(stored.rows[0][0], Value::Null);
});
