mod support;

use lix_engine::{ExecuteOptions, Value};

fn deterministic_uuid(counter: i64) -> String {
    let counter_bits = (counter as u64) & 0x0000_FFFF_FFFF_FFFF;
    format!("01920000-0000-7000-8000-{counter_bits:012x}")
}

fn insert_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "INSERT INTO lix_internal_state_vtable (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{key}', 'lix_key_value', 'lix', 'global', 'lix', '{{\"key\":\"{key}\",\"value\":{value_json}}}', '1'\
         )"
    )
}

fn assert_blob_text(value: &Value, expected: &str) {
    match value {
        Value::Blob(actual) => assert_eq!(actual.as_slice(), expected.as_bytes()),
        other => panic!("expected blob value '{expected}', got {other:?}"),
    }
}

async fn read_sequence_value(engine: &support::simulation_test::SimulationEngine) -> i64 {
    let sequence = engine
        .execute(
            "SELECT snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_key_value' \
               AND entity_id = 'lix_deterministic_sequence_number' \
               AND version_id = 'global' \
               AND snapshot_content IS NOT NULL",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(sequence.rows.len(), 1);
    let snapshot_content = match &sequence.rows[0][0] {
        Value::Text(value) => value,
        other => panic!("expected text snapshot_content, got {other:?}"),
    };
    let parsed: serde_json::Value = serde_json::from_str(snapshot_content).unwrap();
    parsed["value"]
        .as_i64()
        .expect("sequence value must be integer")
}

async fn insert_version(engine: &support::simulation_test::SimulationEngine, version_id: &str) {
    let sql = format!(
        "INSERT INTO lix_version (\
         id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
         ) VALUES (\
         '{version_id}', '{version_id}', 'global', 0, 'commit-{version_id}', 'working-{version_id}'\
         )",
    );
    engine.execute(&sql, &[]).await.unwrap();
}

simulation_test!(
    transaction_path_applies_insert_validation,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let error = engine
            .raw_engine()
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
                         'lix_stored_schema',\
                         '{\"value\":{\"x-lix-key\":\"tx_validation_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"],\"additionalProperties\":false}}'\
                         )",
                        &[],
                    )
                    .await?;

                    tx.execute(
                        "INSERT INTO lix_internal_state_vtable (\
                         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                         ) VALUES (\
                         'entity-1', 'tx_validation_schema', 'file-1', 'version-1', 'lix', '{\"missing\":\"field\"}', '1'\
                         )",
                        &[],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .expect_err("expected validation error");

        assert!(
            error
                .message
                .contains("snapshot_content does not match schema 'tx_validation_schema' (1)"),
            "unexpected error: {}",
            error.message
        );
    }
);

simulation_test!(
    committing_unchanged_transaction_does_not_overwrite_active_version_cache,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();
        insert_version(&engine, "version-tx-stale").await;

        let tx = engine.raw_engine().begin_transaction().await.unwrap();
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-tx-stale'",
                &[],
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let active = engine
            .execute(
                "SELECT version_id FROM lix_active_version ORDER BY id LIMIT 1",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(active.rows.len(), 1);
        assert_eq!(
            active.rows[0][0],
            Value::Text("version-tx-stale".to_string())
        );
    }
);

simulation_test!(
    dropping_open_transaction_panics,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let tx = engine.raw_engine().begin_transaction().await.unwrap();
        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(tx)));
        assert!(
            panic_result.is_err(),
            "expected drop without commit/rollback to panic"
        );
    }
);

simulation_test!(
    transaction_path_respects_deterministic_settings,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                &insert_key_value_sql("lix_deterministic_mode", "{\"enabled\":true}"),
                &[],
            )
            .await
            .unwrap();

        engine
            .raw_engine()
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    let first = tx
                        .execute("SELECT lix_uuid_v7(), lix_timestamp(), lix_uuid_v7()", &[])
                        .await?;
                    assert_eq!(first.rows.len(), 1);
                    assert_eq!(first.rows[0][0], Value::Text(deterministic_uuid(0)));
                    assert_eq!(
                        first.rows[0][1],
                        Value::Text("1970-01-01T00:00:00.001Z".to_string())
                    );
                    assert_eq!(first.rows[0][2], Value::Text(deterministic_uuid(2)));

                    let second = tx
                        .execute("SELECT lix_uuid_v7(), lix_timestamp(), lix_uuid_v7()", &[])
                        .await?;
                    assert_eq!(second.rows.len(), 1);
                    assert_eq!(second.rows[0][0], Value::Text(deterministic_uuid(3)));
                    assert_eq!(
                        second.rows[0][1],
                        Value::Text("1970-01-01T00:00:00.004Z".to_string())
                    );
                    assert_eq!(second.rows[0][2], Value::Text(deterministic_uuid(5)));
                    Ok(())
                })
            })
            .await
            .unwrap();

        assert_eq!(read_sequence_value(&engine).await, 5);
    }
);

simulation_test!(
    transaction_path_applies_multi_statement_postprocess_fallback,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('tx-sequential-fallback', '/tx-sequential-fallback.json', 'before')",
                &[],
            )
            .await
            .unwrap();

        engine
            .raw_engine()
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        "UPDATE lix_file SET data = 'after' WHERE id = 'tx-sequential-fallback'; \
                         SELECT 1",
                        &[],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let result = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'tx-sequential-fallback'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_blob_text(&result.rows[0][0], "after");
    }
);
