mod support;

use futures_util::FutureExt;
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

fn insert_many_key_values_sql(row_count: usize) -> String {
    let mut rows = String::new();
    for index in 0..row_count {
        if index > 0 {
            rows.push_str(", ");
        }
        rows.push_str(&format!(
            "('bulk-{index}', 'lix_key_value', 'lix', 'global', 'lix', '{{\"key\":\"bulk-{index}\",\"value\":\"value-{index}\"}}', '1')"
        ));
    }
    format!(
        "INSERT INTO lix_internal_state_vtable (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES {rows}"
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

simulation_test!(
    transaction_script_path_preprocesses_lix_file_statements,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "BEGIN; \
                 INSERT INTO lix_file (id, path, data) VALUES ('tx-script-preprocess', '/tx-script-preprocess.json', 'before'); \
                 COMMIT;",
                &[],
            )
            .await
            .expect("BEGIN/COMMIT script should preprocess lix_file view writes");

        let result = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'tx-script-preprocess'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_blob_text(&result.rows[0][0], "before");
    }
);

simulation_test!(
    transaction_path_handles_large_vtable_insert_batch_without_sqlite_variable_overflow,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let sql = insert_many_key_values_sql(4_000);
        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(&sql, &[]).await?;
                    Ok(())
                })
            })
            .await
            .expect("large vtable insert should not fail with SQL variable overflow");
    }
);

simulation_test!(
    transaction_path_rolls_back_when_callback_panics,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let panic_result = std::panic::AssertUnwindSafe(engine.transaction(
            ExecuteOptions::default(),
            |tx| {
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO lix_file (id, path, data) VALUES ('tx-panic-rollback', '/tx-panic-rollback.json', 'before')",
                        &[],
                    )
                    .await?;
                    panic!("intentional panic in transaction callback");
                    #[allow(unreachable_code)]
                    Ok::<(), lix_engine::LixError>(())
                })
            },
        ))
        .catch_unwind()
        .await;
        assert!(
            panic_result.is_err(),
            "expected transaction callback to panic"
        );

        let rows = engine
            .execute(
                "SELECT id FROM lix_file WHERE id = 'tx-panic-rollback'",
                &[],
            )
            .await
            .unwrap();
        assert!(rows.rows.is_empty(), "panic path should roll back writes");
    }
);
