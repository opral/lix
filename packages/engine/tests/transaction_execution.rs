mod support;

use futures_util::FutureExt;
use lix_engine::{CreateVersionOptions, ExecuteOptions, Value};

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

fn build_large_multi_statement_select_script_and_params(
    statement_count: usize,
    blob_size_bytes: usize,
) -> (String, Vec<Value>) {
    let mut sql = String::new();
    let mut params = Vec::with_capacity(statement_count);

    for index in 0..statement_count {
        if index > 0 {
            sql.push(' ');
        }
        sql.push_str("SELECT ?;");
        params.push(Value::Blob(vec![(index % 256) as u8; blob_size_bytes]));
    }
    (sql, params)
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

    assert_eq!(sequence.statements[0].rows.len(), 1);
    let snapshot_content = match &sequence.statements[0].rows[0][0] {
        Value::Text(value) => value,
        other => panic!("expected text snapshot_content, got {other:?}"),
    };
    let parsed: serde_json::Value = serde_json::from_str(snapshot_content).unwrap();
    parsed["value"]
        .as_i64()
        .expect("sequence value must be integer")
}

async fn active_version_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let rows = engine
        .execute(
            "SELECT version_id FROM lix_active_version ORDER BY id LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.statements[0].rows.len(), 1);
    match &rows.statements[0].rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected active version id text, got {other:?}"),
    }
}

async fn register_state_history_test_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_registered_schema',\
             '{\"value\":{\"x-lix-key\":\"tx_state_history_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"}},\"required\":[\"value\"],\"additionalProperties\":false}}'\
             )",
            &[],
        )
        .await
        .unwrap();
}

async fn active_commit_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let result = engine
        .execute(
            "SELECT v.commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.statements[0].rows.len(), 1);
    match &result.statements[0].rows[0][0] {
        Value::Text(text) => text.clone(),
        other => panic!("expected commit_id text, got {other:?}"),
    }
}

async fn public_commit_count(engine: &support::simulation_test::SimulationEngine) -> i64 {
    let rows = engine
        .execute("SELECT COUNT(*) FROM lix_commit", &[])
        .await
        .unwrap();
    assert_eq!(rows.statements[0].rows.len(), 1);
    match rows.statements[0].rows[0][0] {
        Value::Integer(value) => value,
        ref other => panic!("expected integer commit count, got {other:?}"),
    }
}

fn tx_dynamic_schema_snapshot_sql() -> String {
    serde_json::json!({
        "value": {
            "x-lix-key": "lix_tx_dynamic_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "x-lix-override-lixcols": {
                "lixcol_file_id": "\"lix\"",
                "lixcol_plugin_key": "\"lix\""
            },
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string" }
            },
            "required": ["id"],
            "additionalProperties": false
        }
    })
    .to_string()
    .replace('\'', "''")
}

fn insert_tx_dynamic_schema_sql() -> String {
    let registered_schema_snapshot = tx_dynamic_schema_snapshot_sql();
    format!(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         'lix_tx_dynamic_schema~1', 'lix_registered_schema', 'lix', 'global', 'lix', '{registered_schema_snapshot}', '1'\
         )"
    )
}

fn insert_tx_dynamic_schema_row_sql(version_id: &str) -> String {
    format!(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         'row-1', 'lix_tx_dynamic_schema', 'lix', '{version_id}', 'lix', '{{\"id\":\"row-1\",\"name\":\"hello\"}}', '1'\
         )"
    )
}

fn delete_tx_dynamic_schema_sql() -> &'static str {
    "DELETE FROM lix_state_by_version \
     WHERE entity_id = 'lix_tx_dynamic_schema~1' \
       AND schema_key = 'lix_registered_schema' \
       AND file_id = 'lix' \
       AND version_id = 'global'"
}

fn insert_key_value_state_row_sql(entity_id: &str, version_id: &str, value_json: &str) -> String {
    format!(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{entity_id}', 'lix_key_value', 'lix', '{version_id}', 'lix', '{{\"key\":\"{entity_id}\",\"value\":{value_json}}}', '1'\
         )"
    )
}

fn update_key_value_state_row_sql(entity_id: &str, value_json: &str) -> String {
    format!(
        "UPDATE lix_state_by_version \
         SET snapshot_content = '{{\"key\":\"{entity_id}\",\"value\":{value_json}}}' \
         WHERE entity_id = '{entity_id}' \
           AND schema_key = 'lix_key_value' \
           AND file_id = 'lix'"
    )
}

fn delete_key_value_state_row_sql(entity_id: &str) -> String {
    format!(
        "DELETE FROM lix_state_by_version \
         WHERE entity_id = '{entity_id}' \
           AND schema_key = 'lix_key_value' \
           AND file_id = 'lix'"
    )
}

async fn assert_tx_dynamic_schema_row_visible(engine: &support::simulation_test::SimulationEngine) {
    let result = engine
        .execute(
            "SELECT name \
             FROM lix_tx_dynamic_schema \
             WHERE id = 'row-1'",
            &[],
        )
        .await
        .expect("dynamic surface query should succeed");
    assert_eq!(result.statements[0].rows.len(), 1);
    assert_eq!(
        result.statements[0].rows[0][0],
        Value::Text("hello".to_string())
    );
}

async fn assert_tx_dynamic_schema_unknown_table(
    engine: &support::simulation_test::SimulationEngine,
) {
    let error = engine
        .execute(
            "SELECT name \
             FROM lix_tx_dynamic_schema \
             WHERE id = 'row-1'",
            &[],
        )
        .await
        .expect_err("dynamic surface should no longer be queryable");
    assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_TABLE");
    assert!(
        error.description.contains("lix_tx_dynamic_schema"),
        "unexpected error: {error:?}"
    );
}

simulation_test!(
    transaction_path_applies_insert_validation,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let error = engine

            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
                         'lix_registered_schema',\
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
                .description
                .contains("snapshot_content does not match schema 'tx_validation_schema' (1)"),
            "unexpected error: {}",
            error.description
        );
    }
);

simulation_test!(
    transaction_path_executes_direct_state_history_reads,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();
        register_state_history_test_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'tx-history-entity', 'tx_state_history_schema', 'f0', 'lix', '1', '{\"value\":\"initial\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();
        let root_commit_id = active_commit_id(&engine).await;

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                let sql = format!(
                    "SELECT depth, snapshot_content \
                     FROM lix_state_history \
                     WHERE entity_id = 'tx-history-entity' \
                       AND schema_key = 'tx_state_history_schema' \
                       AND root_commit_id = '{root_commit_id}' \
                     ORDER BY depth ASC"
                );
                Box::pin(async move {
                    let rows = tx.execute(&sql, &[]).await?;
                    let [statement] = rows.statements.as_slice() else {
                        panic!(
                            "state history transaction read: expected 1 statement result(s), got {}",
                            rows.statements.len()
                        );
                    };
                    assert_eq!(statement.rows.len(), 1);
                    assert_eq!(statement.rows[0][0], Value::Integer(0));
                    assert_eq!(
                        statement.rows[0][1],
                        Value::Text("{\"value\":\"initial\"}".to_string())
                    );
                    Ok(())
                })
            })
            .await
            .expect("transactional direct state-history read should succeed");
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
        engine.initialize().await.unwrap();

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
                    let [first] = first.statements.as_slice() else {
                        panic!(
                            "deterministic first query: expected 1 statement result(s), got {}",
                            first.statements.len()
                        );
                    };
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
                    let [second] = second.statements.as_slice() else {
                        panic!(
                            "deterministic second query: expected 1 statement result(s), got {}",
                            second.statements.len()
                        );
                    };
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
    transaction_path_public_registered_schema_write_updates_bootstrap_for_followup_dynamic_surface_use,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let active_version_id = active_version_id(&engine).await;
        let active_version_id_for_tx = active_version_id.clone();
        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(&insert_tx_dynamic_schema_sql(), &[]).await?;
                    tx.execute(
                        &insert_tx_dynamic_schema_row_sql(&active_version_id_for_tx),
                        &[],
                    )
                    .await?;

                    let in_transaction = tx
                        .execute(
                            "SELECT name \
                         FROM lix_tx_dynamic_schema \
                         WHERE id = 'row-1'",
                            &[],
                        )
                        .await?;
                    assert_eq!(in_transaction.statements[0].rows.len(), 1);
                    assert_eq!(
                        in_transaction.statements[0].rows[0][0],
                        Value::Text("hello".to_string())
                    );

                    Ok::<_, lix_engine::LixError>(())
                })
            })
            .await
            .unwrap();

        assert_tx_dynamic_schema_row_visible(&engine).await;
    }
);

simulation_test!(
    transaction_script_path_uses_dynamic_surface_after_schema_is_registered,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                access_to_internal: false,
                ..Default::default()
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let active_version_id = active_version_id(&engine).await;
        engine
            .execute(&insert_tx_dynamic_schema_sql(), &[])
            .await
            .unwrap();
        engine
            .execute(
                &format!(
                    "BEGIN; \
                     {}; \
                     COMMIT;",
                    insert_tx_dynamic_schema_row_sql(&active_version_id)
                ),
                &[],
            )
            .await
            .unwrap();

        assert_tx_dynamic_schema_row_visible(&engine).await;
    }
);

simulation_test!(
    transaction_path_registered_schema_tombstone_removes_followup_dynamic_surface_dispatch,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let active_version_id = active_version_id(&engine).await;
        engine
            .execute(&insert_tx_dynamic_schema_sql(), &[])
            .await
            .unwrap();
        engine
            .execute(&insert_tx_dynamic_schema_row_sql(&active_version_id), &[])
            .await
            .unwrap();
        assert_tx_dynamic_schema_row_visible(&engine).await;

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(delete_tx_dynamic_schema_sql(), &[]).await?;
                    Ok::<_, lix_engine::LixError>(())
                })
            })
            .await
            .unwrap();

        assert_tx_dynamic_schema_unknown_table(&engine).await;
    }
);

simulation_test!(
    transaction_script_path_registered_schema_tombstone_removes_followup_dynamic_surface_dispatch,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                access_to_internal: false,
                ..Default::default()
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let active_version_id = active_version_id(&engine).await;
        engine
            .execute(&insert_tx_dynamic_schema_sql(), &[])
            .await
            .unwrap();
        engine
            .execute(&insert_tx_dynamic_schema_row_sql(&active_version_id), &[])
            .await
            .unwrap();
        assert_tx_dynamic_schema_row_visible(&engine).await;

        engine
            .execute(
                &format!("BEGIN; {}; COMMIT;", delete_tx_dynamic_schema_sql()),
                &[],
            )
            .await
            .unwrap();

        assert_tx_dynamic_schema_unknown_table(&engine).await;
    }
);

simulation_test!(
    transaction_path_public_noop_filesystem_delete_stays_in_public_pipeline,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let commit_count_before = public_commit_count(&engine).await;

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute("DELETE FROM lix_file WHERE id = 'tx-public-noop-missing'", &[])
                        .await?;
                    tx.execute(
                        "INSERT INTO lix_file (id, path, data) \
                         VALUES ('tx-public-noop-file', '/tx-public-noop-file.md', lix_text_encode('after'))",
                        &[],
                    )
                    .await?;
                    Ok::<_, lix_engine::LixError>(())
                })
            })
            .await
            .expect("public noop followed by insert should stay in the public pipeline");

        let rows = engine
            .execute(
                "SELECT id, path, data FROM lix_file WHERE id = 'tx-public-noop-file'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_eq!(
            rows.statements[0].rows[0][0],
            Value::Text("tx-public-noop-file".to_string())
        );
        assert_eq!(
            rows.statements[0].rows[0][1],
            Value::Text("/tx-public-noop-file.md".to_string())
        );
        assert_blob_text(&rows.statements[0].rows[0][2], "after");
        assert_eq!(public_commit_count(&engine).await, commit_count_before + 1);
    }
);

simulation_test!(
    transaction_path_public_entity_like_update_sees_pending_rows,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let commit_count_before = public_commit_count(&engine).await;

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO lix_key_value (key, value) VALUES ('tx-pending-like-a', 'before-a')",
                        &[],
                    )
                    .await?;
                    tx.execute(
                        "INSERT INTO lix_key_value (key, value) VALUES ('tx-pending-like-b', 'before-b')",
                        &[],
                    )
                    .await?;
                    tx.execute(
                        "UPDATE lix_key_value SET value = $2 WHERE key LIKE $1",
                        &[
                            Value::Text("tx-pending-like-%".to_string()),
                            Value::Text("after".to_string()),
                        ],
                    )
                    .await?;
                    Ok::<_, lix_engine::LixError>(())
                })
            })
            .await
            .expect("pending entity rows should be selectable for public LIKE updates");

        let rows = engine
            .execute(
                "SELECT key, value FROM lix_key_value \
                 WHERE key LIKE 'tx-pending-like-%' \
                 ORDER BY key",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_eq!(
            rows.statements[0].rows[0],
            vec![
                Value::Text("tx-pending-like-a".to_string()),
                Value::Text("after".to_string()),
            ]
        );
        assert_eq!(
            rows.statements[0].rows[1],
            vec![
                Value::Text("tx-pending-like-b".to_string()),
                Value::Text("after".to_string()),
            ]
        );
        assert_eq!(public_commit_count(&engine).await, commit_count_before + 1);
    }
);

simulation_test!(
    tx_execute_multistmt_has_statement_barriers_for_file_bytes,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('tx-byte-barrier', '/tx-byte-barrier.md', lix_text_encode('before'))",
                &[],
            )
            .await
            .unwrap();

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    let rows = tx
                        .execute(
                            "UPDATE lix_file SET data = lix_text_encode('after') \
                             WHERE id = 'tx-byte-barrier'; \
                             SELECT data FROM lix_file WHERE id = 'tx-byte-barrier' LIMIT 1",
                            &[],
                        )
                        .await?;
                    assert_eq!(rows.statements.len(), 2);
                    assert_eq!(rows.statements[1].rows.len(), 1);
                    assert_blob_text(&rows.statements[1].rows[0][0], "after");
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
);

simulation_test!(
    tx_execute_multistmt_commit_pointer_visible_after_content_stmt,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('tx-commit-barrier', '/tx-commit-barrier.md', lix_text_encode('before'))",
                &[],
            )
            .await
            .unwrap();

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    let before = tx
                        .execute(
                            "SELECT v.commit_id \
                             FROM lix_active_version av \
                             JOIN lix_version v ON v.id = av.version_id \
                             ORDER BY av.id \
                             LIMIT 1",
                            &[],
                        )
                        .await?;
                    assert_eq!(before.statements.len(), 1);
                    assert_eq!(before.statements[0].rows.len(), 1);
                    let before_commit = match &before.statements[0].rows[0][0] {
                        Value::Text(value) => value.clone(),
                        other => panic!("expected commit id text, got {other:?}"),
                    };

                    let after = tx
                        .execute(
                            "UPDATE lix_file SET data = lix_text_encode('after') \
                             WHERE id = 'tx-commit-barrier'; \
                             SELECT v.commit_id \
                             FROM lix_active_version av \
                             JOIN lix_version v ON v.id = av.version_id \
                             ORDER BY av.id \
                             LIMIT 1",
                            &[],
                        )
                        .await?;
                    assert_eq!(after.statements.len(), 2);
                    assert_eq!(after.statements[1].rows.len(), 1);
                    let after_commit = match &after.statements[1].rows[0][0] {
                        Value::Text(value) => value.clone(),
                        other => panic!("expected commit id text, got {other:?}"),
                    };
                    assert_ne!(after_commit, before_commit);
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
);

simulation_test!(
    tx_execute_repeated_writes_flush_before_followup_read,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO lix_file (id, path, data) \
                         VALUES ('tx-repeat-a', '/tx-repeat-a.md', lix_text_encode('a'))",
                        &[],
                    )
                    .await?;
                    tx.execute(
                        "INSERT INTO lix_file (id, path, data) \
                         VALUES ('tx-repeat-b', '/tx-repeat-b.md', lix_text_encode('b'))",
                        &[],
                    )
                    .await?;

                    let count = tx
                        .execute(
                            "SELECT COUNT(*) \
                             FROM lix_file \
                             WHERE id IN ('tx-repeat-a', 'tx-repeat-b')",
                            &[],
                        )
                        .await?;
                    assert_eq!(count.statements.len(), 1);
                    assert_eq!(count.statements[0].rows.len(), 1);
                    assert_eq!(count.statements[0].rows[0][0], Value::Integer(2));
                    Ok::<_, lix_engine::LixError>(())
                })
            })
            .await
            .unwrap();
    }
);

simulation_test!(
    transaction_path_public_state_inserts_coalesce_into_single_commit,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let version_id = active_version_id(&engine).await;
        let before_commit_count = public_commit_count(&engine).await;

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                let version_id = version_id.clone();
                Box::pin(async move {
                    tx.execute(
                        &format!(
                            "INSERT INTO lix_state_by_version (\
                             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                             ) VALUES (\
                             'tx-state-a', 'lix_key_value', 'lix', '{version_id}', 'lix', '{{\"key\":\"tx-state-a\",\"value\":\"a\"}}', '1'\
                             )"
                        ),
                        &[],
                    )
                    .await?;
                    tx.execute(
                        &format!(
                            "INSERT INTO lix_state_by_version (\
                             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                             ) VALUES (\
                             'tx-state-b', 'lix_key_value', 'lix', '{version_id}', 'lix', '{{\"key\":\"tx-state-b\",\"value\":\"b\"}}', '1'\
                             )"
                        ),
                        &[],
                    )
                    .await?;
                    Ok::<_, lix_engine::LixError>(())
                })
            })
            .await
            .unwrap();

        let after_commit_count = public_commit_count(&engine).await;
        assert_eq!(after_commit_count - before_commit_count, 1);

        let rows = engine
            .execute(
                "SELECT value \
                 FROM lix_key_value \
                 WHERE key IN ('tx-state-a', 'tx-state-b') \
                 ORDER BY key",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_eq!(rows.statements[0].rows[0][0], Value::Text("a".to_string()));
        assert_eq!(rows.statements[0].rows[1][0], Value::Text("b".to_string()));
    }
);

simulation_test!(
    transaction_path_direct_history_read_does_not_split_buffered_public_commit,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();
        register_state_history_test_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'tx-history-midpoint', 'tx_state_history_schema', 'f0', 'lix', '1', '{\"value\":\"initial\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let root_commit_id = active_commit_id(&engine).await;
        let version_id = active_version_id(&engine).await;
        let before_commit_count = public_commit_count(&engine).await;

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                let version_id = version_id.clone();
                let history_sql = format!(
                    "SELECT depth, snapshot_content \
                     FROM lix_state_history \
                     WHERE entity_id = 'tx-history-midpoint' \
                       AND schema_key = 'tx_state_history_schema' \
                       AND root_commit_id = '{root_commit_id}' \
                     ORDER BY depth ASC"
                );
                Box::pin(async move {
                    tx.execute(
                        &format!(
                            "INSERT INTO lix_state_by_version (\
                             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                             ) VALUES (\
                             'tx-mid-a', 'lix_key_value', 'lix', '{version_id}', 'lix', '{{\"key\":\"tx-mid-a\",\"value\":\"a\"}}', '1'\
                             )"
                        ),
                        &[],
                    )
                    .await?;

                    let history_rows = tx.execute(&history_sql, &[]).await?;
                    assert_eq!(history_rows.statements.len(), 1);
                    assert_eq!(history_rows.statements[0].rows.len(), 1);
                    assert_eq!(history_rows.statements[0].rows[0][0], Value::Integer(0));
                    assert_eq!(
                        history_rows.statements[0].rows[0][1],
                        Value::Text("{\"value\":\"initial\"}".to_string())
                    );

                    tx.execute(
                        &format!(
                            "INSERT INTO lix_state_by_version (\
                             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                             ) VALUES (\
                             'tx-mid-b', 'lix_key_value', 'lix', '{version_id}', 'lix', '{{\"key\":\"tx-mid-b\",\"value\":\"b\"}}', '1'\
                             )"
                        ),
                        &[],
                    )
                    .await?;
                    Ok::<_, lix_engine::LixError>(())
                })
            })
            .await
            .unwrap();

        let after_commit_count = public_commit_count(&engine).await;
        assert_eq!(after_commit_count - before_commit_count, 1);

        let rows = engine
            .execute(
                "SELECT value \
                 FROM lix_key_value \
                 WHERE key IN ('tx-mid-a', 'tx-mid-b') \
                 ORDER BY key",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_eq!(rows.statements[0].rows[0][0], Value::Text("a".to_string()));
        assert_eq!(rows.statements[0].rows[1][0], Value::Text("b".to_string()));
    }
);

simulation_test!(
    transaction_path_exact_public_state_updates_coalesce_into_single_commit,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let version_id = active_version_id(&engine).await;
        engine
            .execute(
                &insert_key_value_state_row_sql("tx-update-a", &version_id, "\"before-a\""),
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                &insert_key_value_state_row_sql("tx-update-b", &version_id, "\"before-b\""),
                &[],
            )
            .await
            .unwrap();
        let before_commit_count = public_commit_count(&engine).await;

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        &update_key_value_state_row_sql("tx-update-a", "\"after-a\""),
                        &[],
                    )
                    .await?;
                    tx.execute(
                        &update_key_value_state_row_sql("tx-update-b", "\"after-b\""),
                        &[],
                    )
                    .await?;
                    Ok::<_, lix_engine::LixError>(())
                })
            })
            .await
            .unwrap();

        let after_commit_count = public_commit_count(&engine).await;
        assert_eq!(after_commit_count - before_commit_count, 1);

        let rows = engine
            .execute(
                "SELECT value \
                 FROM lix_key_value \
                 WHERE key IN ('tx-update-a', 'tx-update-b') \
                 ORDER BY key",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.statements[0].rows,
            vec![
                vec![Value::Text("after-a".to_string())],
                vec![Value::Text("after-b".to_string())],
            ]
        );
    }
);

simulation_test!(
    transaction_path_exact_public_state_update_and_delete_coalesce_into_single_commit,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let version_id = active_version_id(&engine).await;
        engine
            .execute(
                &insert_key_value_state_row_sql("tx-ud-a", &version_id, "\"before-a\""),
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                &insert_key_value_state_row_sql("tx-ud-b", &version_id, "\"before-b\""),
                &[],
            )
            .await
            .unwrap();
        let before_commit_count = public_commit_count(&engine).await;

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        &update_key_value_state_row_sql("tx-ud-a", "\"after-a\""),
                        &[],
                    )
                    .await?;
                    tx.execute(&delete_key_value_state_row_sql("tx-ud-b"), &[])
                        .await?;
                    Ok::<_, lix_engine::LixError>(())
                })
            })
            .await
            .unwrap();

        let after_commit_count = public_commit_count(&engine).await;
        assert_eq!(after_commit_count - before_commit_count, 1);

        let rows = engine
            .execute(
                "SELECT key, value \
                 FROM lix_key_value \
                 WHERE key IN ('tx-ud-a', 'tx-ud-b') \
                 ORDER BY key",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.statements[0].rows,
            vec![vec![
                Value::Text("tx-ud-a".to_string()),
                Value::Text("after-a".to_string()),
            ]]
        );
    }
);

simulation_test!(
    transaction_path_exact_filesystem_metadata_updates_coalesce_into_single_commit,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('tx-file-update-a', '/tx-file-update-a.md', lix_text_encode('a'))",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('tx-file-update-b', '/tx-file-update-b.md', lix_text_encode('b'))",
                &[],
            )
            .await
            .unwrap();

        let before_commit_count = public_commit_count(&engine).await;

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        "UPDATE lix_file SET metadata = '{\"tag\":\"after-a\"}' \
                         WHERE id = 'tx-file-update-a'",
                        &[],
                    )
                    .await?;
                    tx.execute(
                        "UPDATE lix_file SET metadata = '{\"tag\":\"after-b\"}' \
                         WHERE id = 'tx-file-update-b'",
                        &[],
                    )
                    .await?;
                    Ok::<_, lix_engine::LixError>(())
                })
            })
            .await
            .unwrap();

        let after_commit_count = public_commit_count(&engine).await;
        assert_eq!(after_commit_count - before_commit_count, 1);

        let rows = engine
            .execute(
                "SELECT id, metadata \
                 FROM lix_file \
                 WHERE id IN ('tx-file-update-a', 'tx-file-update-b') \
                 ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.statements[0].rows,
            vec![
                vec![
                    Value::Text("tx-file-update-a".to_string()),
                    Value::Text("{\"tag\":\"after-a\"}".to_string())
                ],
                vec![
                    Value::Text("tx-file-update-b".to_string()),
                    Value::Text("{\"tag\":\"after-b\"}".to_string())
                ],
            ]
        );
    }
);

simulation_test!(
    transaction_path_exact_filesystem_update_and_delete_coalesce_into_single_commit,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('tx-file-ud-a', '/tx-file-ud-a.md', lix_text_encode('a'))",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('tx-file-ud-b', '/tx-file-ud-b.md', lix_text_encode('b'))",
                &[],
            )
            .await
            .unwrap();

        let before_commit_count = public_commit_count(&engine).await;

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        "UPDATE lix_file SET metadata = '{\"tag\":\"after-a\"}' \
                         WHERE id = 'tx-file-ud-a'",
                        &[],
                    )
                    .await?;
                    tx.execute("DELETE FROM lix_file WHERE id = 'tx-file-ud-b'", &[])
                        .await?;
                    Ok::<_, lix_engine::LixError>(())
                })
            })
            .await
            .unwrap();

        let after_commit_count = public_commit_count(&engine).await;
        assert_eq!(after_commit_count - before_commit_count, 1);

        let rows = engine
            .execute(
                "SELECT id, metadata \
                 FROM lix_file \
                 WHERE id IN ('tx-file-ud-a', 'tx-file-ud-b') \
                 ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.statements[0].rows,
            vec![vec![
                Value::Text("tx-file-ud-a".to_string()),
                Value::Text("{\"tag\":\"after-a\"}".to_string())
            ],]
        );
    }
);

simulation_test!(
    transaction_path_filesystem_insert_then_path_update_coalesces_into_single_commit,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let before_commit_count = public_commit_count(&engine).await;

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO lix_file (id, path, data) \
                         VALUES ('tx-file-insert-then-path-update', '/tx-file-before.md', lix_text_encode('before'))",
                        &[],
                    )
                    .await?;
                    tx.execute(
                        "UPDATE lix_file SET path = '/tx-file-after.md' \
                         WHERE path = '/tx-file-before.md'",
                        &[],
                    )
                    .await?;
                    Ok::<_, lix_engine::LixError>(())
                })
            })
            .await
            .unwrap();

        let after_commit_count = public_commit_count(&engine).await;
        assert_eq!(after_commit_count - before_commit_count, 1);

        let rows = engine
            .execute(
                "SELECT path, data \
                 FROM lix_file \
                 WHERE id = 'tx-file-insert-then-path-update'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.statements[0].rows[0][0],
            Value::Text("/tx-file-after.md".to_string())
        );
        assert_blob_text(&rows.statements[0].rows[0][1], "before");
    }
);

simulation_test!(
    transaction_path_repeated_parameterized_filesystem_writes_preserve_pending_visibility,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let before_commit_count = public_commit_count(&engine).await;

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    let insert_sql = "INSERT INTO lix_file (id, path, data) VALUES ($1, $2, $3)";
                    tx.execute(
                        insert_sql,
                        &[
                            Value::Text("tx-template-a".to_string()),
                            Value::Text("/tx-template-a-before.md".to_string()),
                            Value::Blob(b"before-a".to_vec()),
                        ],
                    )
                    .await?;
                    tx.execute(
                        insert_sql,
                        &[
                            Value::Text("tx-template-b".to_string()),
                            Value::Text("/tx-template-b-before.md".to_string()),
                            Value::Blob(b"before-b".to_vec()),
                        ],
                    )
                    .await?;

                    let update_sql = "UPDATE lix_file SET path = $1, data = $2 WHERE id = $3";
                    tx.execute(
                        update_sql,
                        &[
                            Value::Text("/tx-template-a-after.md".to_string()),
                            Value::Blob(b"after-a".to_vec()),
                            Value::Text("tx-template-a".to_string()),
                        ],
                    )
                    .await?;
                    tx.execute(
                        update_sql,
                        &[
                            Value::Text("/tx-template-b-after.md".to_string()),
                            Value::Blob(b"after-b".to_vec()),
                            Value::Text("tx-template-b".to_string()),
                        ],
                    )
                    .await?;
                    tx.execute(
                        "DELETE FROM lix_file WHERE id = $1",
                        &[Value::Text("tx-template-b".to_string())],
                    )
                    .await?;

                    let pending = tx
                        .execute(
                            "SELECT id, path, data \
                             FROM lix_file \
                             WHERE id IN ($1, $2) \
                             ORDER BY id",
                            &[
                                Value::Text("tx-template-a".to_string()),
                                Value::Text("tx-template-b".to_string()),
                            ],
                        )
                        .await?;
                    assert_eq!(pending.statements[0].rows.len(), 1);
                    assert_eq!(
                        pending.statements[0].rows[0][0],
                        Value::Text("tx-template-a".to_string())
                    );
                    assert_eq!(
                        pending.statements[0].rows[0][1],
                        Value::Text("/tx-template-a-after.md".to_string())
                    );
                    assert_blob_text(&pending.statements[0].rows[0][2], "after-a");
                    Ok::<_, lix_engine::LixError>(())
                })
            })
            .await
            .unwrap();

        let after_commit_count = public_commit_count(&engine).await;
        assert_eq!(after_commit_count - before_commit_count, 1);

        let rows = engine
            .execute(
                "SELECT id, path, data \
                 FROM lix_file \
                 WHERE id IN ('tx-template-a', 'tx-template-b') \
                 ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_eq!(
            rows.statements[0].rows[0][0],
            Value::Text("tx-template-a".to_string())
        );
        assert_eq!(
            rows.statements[0].rows[0][1],
            Value::Text("/tx-template-a-after.md".to_string())
        );
        assert_blob_text(&rows.statements[0].rows[0][2], "after-a");
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
        engine.initialize().await.unwrap();

        engine
            .execute(
                "BEGIN; \
                 INSERT INTO lix_file (id, path, data) VALUES ('tx-script-preprocess', '/tx-script-preprocess.json', lix_text_encode('before')); \
                 COMMIT;", &[])
            .await
            .expect("BEGIN/COMMIT script should preprocess lix_file view writes");

        let result = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'tx-script-preprocess'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(result.statements[0].rows.len(), 1);
        assert_blob_text(&result.statements[0].rows[0][0], "before");
    }
);

simulation_test!(
    execute_allows_begin_commit_script_without_internal_access,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                access_to_internal: false,
                ..Default::default()
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "BEGIN; \
                 INSERT INTO lix_key_value (key, value) VALUES ('tx-public-begin-commit', 'ok'); \
                 COMMIT;",
                &[],
            )
            .await
            .expect("public execute should accept explicit BEGIN/COMMIT wrappers");

        let result = engine
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'tx-public-begin-commit' LIMIT 1",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(result.statements[0].rows.len(), 1);
        assert_eq!(
            result.statements[0].rows[0][0],
            Value::Text("ok".to_string())
        );
    }
);

simulation_test!(
    execute_rejects_unsupported_transaction_control_variants,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                access_to_internal: false,
                ..Default::default()
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let error = engine
            .execute("BEGIN IMMEDIATE;", &[])
            .await
            .expect_err("unsupported transaction modifiers should remain denied");
        assert_eq!(error.code, "LIX_ERROR_TRANSACTION_CONTROL_STATEMENT_DENIED");
    }
);

simulation_test!(
    execute_rejects_standalone_begin,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                access_to_internal: false,
                ..Default::default()
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let error = engine
            .execute("BEGIN;", &[])
            .await
            .expect_err("standalone BEGIN should be denied");
        assert_eq!(error.code, "LIX_ERROR_TRANSACTION_CONTROL_STATEMENT_DENIED");
    }
);

simulation_test!(
    execute_rejects_standalone_commit_and_rollback,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                access_to_internal: false,
                ..Default::default()
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let commit_error = engine
            .execute("COMMIT;", &[])
            .await
            .expect_err("standalone COMMIT should be denied");
        assert_eq!(
            commit_error.code,
            "LIX_ERROR_TRANSACTION_CONTROL_STATEMENT_DENIED"
        );

        let rollback_error = engine
            .execute("ROLLBACK;", &[])
            .await
            .expect_err("standalone ROLLBACK should be denied");
        assert_eq!(
            rollback_error.code,
            "LIX_ERROR_TRANSACTION_CONTROL_STATEMENT_DENIED"
        );

        engine
            .create_version(CreateVersionOptions::default())
            .await
            .expect("create_version should remain available without hidden SQL session state");
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
        engine.initialize().await.unwrap();

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
    transaction_script_path_handles_large_parameterized_batch_without_param_fanout_oom,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let (sql, params) = build_large_multi_statement_select_script_and_params(20, 500_000);
        engine
            .execute(&sql, &params)
            .await
            .expect("large parameterized multi-statement script should execute without OOM");
    }
);

simulation_test!(
    transaction_script_path_handles_parameterized_lix_file_update_with_prior_statement_params,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('tx-script-param-update', '/before.md', lix_text_encode('before'))", &[])
            .await
            .unwrap();

        engine
            .execute(
                "BEGIN; \
                 DELETE FROM lix_file WHERE id IN (?); \
                 INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?); \
                 UPDATE lix_file SET path = ?, data = ? WHERE id = ?; \
                 COMMIT;", &[
                    Value::Text("tx-script-delete-miss".to_string()),
                    Value::Text("tx-script-insert".to_string()),
                    Value::Text("/inserted.md".to_string()),
                    Value::Blob(b"inserted".to_vec()),
                    Value::Text("/after.md".to_string()),
                    Value::Blob(b"after".to_vec()),
                    Value::Text("tx-script-param-update".to_string()),
                ])
            .await
            .expect(
                "BEGIN/COMMIT transaction script with parameterized update should execute successfully",
            );

        let updated = engine
            .execute(
                "SELECT path, data FROM lix_file WHERE id = 'tx-script-param-update'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(updated.statements[0].rows.len(), 1);
        assert_eq!(
            updated.statements[0].rows[0][0],
            Value::Text("/after.md".to_string())
        );
        assert_blob_text(&updated.statements[0].rows[0][1], "after");
    }
);

simulation_test!(
    transaction_script_path_handles_parameterized_multi_row_lix_file_insert,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "BEGIN; \
                 INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?), (?, ?, ?); \
                 COMMIT;",
                &[
                    Value::Text("tx-script-batch-1".to_string()),
                    Value::Text("/batch-1.md".to_string()),
                    Value::Blob(b"batch-1".to_vec()),
                    Value::Text("tx-script-batch-2".to_string()),
                    Value::Text("/batch-2.md".to_string()),
                    Value::Blob(b"batch-2".to_vec()),
                ],
            )
            .await
            .expect(
                "BEGIN/COMMIT transaction script with parameterized multi-row insert should execute successfully",
            );

        let inserted = engine
            .execute(
                "SELECT id, path, data FROM lix_file WHERE id IN ('tx-script-batch-1', 'tx-script-batch-2') ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(inserted.statements[0].rows.len(), 2);
        assert_eq!(
            inserted.statements[0].rows[0][0],
            Value::Text("tx-script-batch-1".to_string())
        );
        assert_eq!(
            inserted.statements[0].rows[1][1],
            Value::Text("/batch-2.md".to_string())
        );
        assert_blob_text(&inserted.statements[0].rows[0][2], "batch-1");
        assert_blob_text(&inserted.statements[0].rows[1][2], "batch-2");
    }
);

simulation_test!(
    transaction_script_path_handles_single_parameterized_lix_file_update,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('tx-script-single-update', '/before.md', lix_text_encode('before'))", &[])
            .await
            .unwrap();

        engine
            .execute(
                "BEGIN; \
                 UPDATE lix_file SET path = ?, data = ? WHERE id = ?; \
                 COMMIT;",
                &[
                    Value::Text("/after.md".to_string()),
                    Value::Blob(b"after".to_vec()),
                    Value::Text("tx-script-single-update".to_string()),
                ],
            )
            .await
            .expect("single update transaction script should execute successfully");

        let updated = engine
            .execute(
                "SELECT path, data FROM lix_file WHERE id = 'tx-script-single-update'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(updated.statements[0].rows.len(), 1);
        assert_eq!(
            updated.statements[0].rows[0][0],
            Value::Text("/after.md".to_string())
        );
        assert_blob_text(&updated.statements[0].rows[0][1], "after");
    }
);

simulation_test!(
    transaction_script_path_binds_placeholder_variants_once_across_statements,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "BEGIN; \
                 SELECT ?; \
                 SELECT ?3; \
                 SELECT $2, ?; \
                 COMMIT;",
                &[
                    Value::Integer(11),
                    Value::Integer(22),
                    Value::Integer(33),
                    Value::Integer(44),
                ],
            )
            .await
            .expect("mixed placeholder transaction script should execute");

        assert_eq!(result.statements.len(), 3);
        assert_eq!(result.statements[0].rows, vec![vec![Value::Integer(11)]]);
        assert_eq!(result.statements[1].rows, vec![vec![Value::Integer(33)]]);
        assert_eq!(
            result.statements[2].rows,
            vec![vec![Value::Integer(22), Value::Integer(44)]]
        );
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
        engine.initialize().await.unwrap();

        let panic_result = std::panic::AssertUnwindSafe(engine.transaction(
            ExecuteOptions::default(),
            |tx| {
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO lix_file (id, path, data) VALUES ('tx-panic-rollback', '/tx-panic-rollback.json', lix_text_encode('before'))",
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
        assert!(
            rows.statements[0].rows.is_empty(),
            "panic path should roll back writes"
        );
    }
);
