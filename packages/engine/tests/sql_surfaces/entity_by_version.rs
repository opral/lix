use lix_engine::Value;

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

simulation_test!(
    lix_entity_by_version_insert_update_delete,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();
        let version_id = engine.active_version_id().await.unwrap();
        let version_id_sql = version_id.replace('\'', "''");

        engine
            .execute(
                "INSERT INTO lix_key_value_by_version (\
                 key, value, lixcol_file_id, lixcol_version_id, lixcol_plugin_key, lixcol_schema_version\
                 ) VALUES (\
                 'key-bv', 'value-insert', 'lix', $1, 'lix', '1'\
                 )", &[Value::Text(version_id.clone())])
            .await
            .unwrap();

        let inserted = engine
            .execute(
                "SELECT key, value, lixcol_version_id \
                 FROM lix_key_value_by_version \
                 WHERE key = 'key-bv' AND lixcol_version_id = $1",
                &[Value::Text(version_id.clone())],
            )
            .await
            .unwrap();
        sim.assert_deterministic(inserted.statements[0].rows.clone());
        assert_eq!(inserted.statements[0].rows.len(), 1);
        assert_text(&inserted.statements[0].rows[0][0], "key-bv");
        assert_text(&inserted.statements[0].rows[0][1], "value-insert");
        assert_text(&inserted.statements[0].rows[0][2], &version_id);

        engine
            .execute(
                &format!(
                    "UPDATE lix_key_value_by_version \
                     SET value = 'value-update' \
                     WHERE key = 'key-bv' AND lixcol_version_id = '{version_id}'",
                    version_id = version_id_sql
                ),
                &[],
            )
            .await
            .unwrap();

        let updated = engine
            .execute(
                &format!(
                    "SELECT value \
                     FROM lix_key_value_by_version \
                     WHERE key = 'key-bv' AND lixcol_version_id = '{version_id}'",
                    version_id = version_id_sql
                ),
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(updated.statements[0].rows.clone());
        assert_eq!(updated.statements[0].rows.len(), 1);
        assert_text(&updated.statements[0].rows[0][0], "value-update");

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_key_value_by_version \
                     WHERE key = 'key-bv' AND lixcol_version_id = '{version_id}'",
                    version_id = version_id_sql
                ),
                &[],
            )
            .await
            .unwrap();

        let deleted = engine
            .execute(
                &format!(
                    "SELECT key \
                     FROM lix_key_value_by_version \
                     WHERE key = 'key-bv' AND lixcol_version_id = '{version_id}'",
                    version_id = version_id_sql
                ),
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(deleted.statements[0].rows.clone());
        assert!(deleted.statements[0].rows.is_empty());
    }
);

simulation_test!(
    lix_entity_by_version_insert_requires_version_id,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let err = engine
            .execute(
                "INSERT INTO lix_key_value_by_version (\
                 key, value, lixcol_file_id, lixcol_plugin_key, lixcol_schema_version\
                 ) VALUES (\
                 'missing-version', 'x', 'lix', 'lix', '1'\
                 )",
                &[],
            )
            .await
            .expect_err("insert without version should fail");
        assert!(
            err.description.contains("requires lixcol_version_id")
                || err.description.contains("requires version_id"),
            "unexpected error: {}",
            err.description
        );
    }
);

simulation_test!(
    lix_entity_by_version_insert_on_conflict_do_update_is_supported,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();
        let version_id = engine.active_version_id().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value_by_version (\
                 key, value, lixcol_version_id, lixcol_untracked\
                 ) VALUES (\
                 'key-upsert-bv', 'value-a', $1, true\
                 )",
                &[Value::Text(version_id.clone())],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value_by_version (\
                 key, value, lixcol_version_id, lixcol_untracked\
                 ) VALUES (\
                 'key-upsert-bv', 'value-b', $1, true\
                 ) \
                 ON CONFLICT (key, lixcol_version_id) DO UPDATE \
                 SET value = 'value-b', lixcol_untracked = true",
                &[Value::Text(version_id.clone())],
            )
            .await
            .unwrap();

        let updated = engine
            .execute(
                "SELECT value, lixcol_untracked \
                 FROM lix_key_value_by_version \
                 WHERE key = 'key-upsert-bv' AND lixcol_version_id = $1",
                &[Value::Text(version_id)],
            )
            .await
            .unwrap();

        sim.assert_deterministic_normalized(updated.statements[0].rows.clone());
        assert_eq!(updated.statements[0].rows.len(), 1);
        assert_text(&updated.statements[0].rows[0][0], "value-b");
        assert!(
            matches!(
                updated.statements[0].rows[0][1],
                Value::Boolean(true) | Value::Integer(1)
            ),
            "expected true-like untracked marker, got {:?}",
            updated.statements[0].rows[0][1]
        );
    }
);

simulation_test!(
    lix_entity_by_version_insert_on_conflict_do_nothing_is_supported,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();
        let version_id = engine.active_version_id().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value_by_version (\
                 key, value, lixcol_version_id\
                 ) VALUES (\
                 'key-upsert-bv', 'value-a', $1\
                 )",
                &[Value::Text(version_id.clone())],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value_by_version (\
                 key, value, lixcol_version_id\
                 ) VALUES (\
                 'key-upsert-bv', 'value-b', $1\
                 ) \
                 ON CONFLICT (key, lixcol_version_id) DO NOTHING",
                &[Value::Text(version_id.clone())],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT value \
                 FROM lix_key_value_by_version \
                 WHERE key = 'key-upsert-bv' AND lixcol_version_id = $1",
                &[Value::Text(version_id)],
            )
            .await
            .unwrap();
        sim.assert_deterministic_normalized(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "value-a");
    }
);
