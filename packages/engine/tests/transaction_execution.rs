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
                .description
                .contains("snapshot_content does not match schema 'tx_validation_schema' (1)"),
            "unexpected error: {}",
            error.description
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
    transaction_path_sql2_stored_schema_write_updates_bootstrap_for_followup_dynamic_surface_use,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let active_version_id = active_version_id(&engine).await;
        let stored_schema_snapshot = serde_json::json!({
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
        .replace('\'', "''");

        let active_version_id_for_tx = active_version_id.clone();
        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        &format!(
                            "INSERT INTO lix_state_by_version (\
                             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                             ) VALUES (\
                             'lix_tx_dynamic_schema~1', 'lix_stored_schema', 'lix', 'global', 'lix', '{stored_schema_snapshot}', '1'\
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
                             'row-1', 'lix_tx_dynamic_schema', 'lix', '{}', 'lix', '{{\"id\":\"row-1\",\"name\":\"hello\"}}', '1'\
                             )",
                            active_version_id_for_tx
                        ),
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

        let post_commit = engine
            .execute(
                &format!(
                    "SELECT name \
                     FROM lix_tx_dynamic_schema \
                     WHERE id = 'row-1'"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(post_commit.statements[0].rows.len(), 1);
        assert_eq!(
            post_commit.statements[0].rows[0][0],
            Value::Text("hello".to_string())
        );
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
    execute_rejects_non_wrapped_transaction_control_without_internal_access,
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
            .execute("ROLLBACK;", &[])
            .await
            .expect_err("non-wrapped transaction control should remain denied");
        assert_eq!(error.code, "LIX_ERROR_TRANSACTION_CONTROL_STATEMENT_DENIED");
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
