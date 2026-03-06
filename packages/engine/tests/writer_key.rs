mod support;

use lix_engine::{ExecuteOptions, LixError, Value};

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

fn assert_blob_text(value: &Value, expected: &str) {
    match value {
        Value::Blob(actual) => assert_eq!(actual.as_slice(), expected.as_bytes()),
        other => panic!("expected blob value '{expected}', got {other:?}"),
    }
}

fn assert_null(value: &Value) {
    match value {
        Value::Null => {}
        other => panic!("expected null value, got {other:?}"),
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
    assert_eq!(rows.statements[0].rows.len(), 1);
    match &rows.statements[0].rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected active version id as text, got {other:?}"),
    }
}

async fn register_writer_key_test_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"wk_writer_key_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
            &[],
        )
        .await
        .unwrap();
}

simulation_test!(
    untracked_writer_key_matches_materialized_writer_key,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();
        register_writer_key_test_schema(&engine).await;

        let version_id = active_version_id(&engine).await;

        engine
            .execute_with_options(
                &format!(
                    "INSERT INTO lix_internal_state_vtable (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) VALUES (\
                     'wk-tracked', 'wk_writer_key_schema', 'file-1', '{version_id}', 'lix', '{{\"key\":\"tracked\"}}', '1'\
                     )"
                ),
                &[],
                ExecuteOptions {
                    writer_key: Some("editor:both".to_string()),
                },
            )
            .await
            .unwrap();

        engine
            .execute_with_options(
                &format!(
                    "INSERT INTO lix_internal_state_vtable (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
                     ) VALUES (\
                     'wk-untracked', 'wk_writer_key_schema', 'file-1', '{version_id}', 'lix', '{{\"key\":\"untracked\"}}', '1', true\
                     )"
                ),
                &[],
                ExecuteOptions {
                    writer_key: Some("editor:both".to_string()),
                },
            )
            .await
            .unwrap();

        let materialized = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_internal_state_materialized_v1_wk_writer_key_schema \
                     WHERE entity_id = 'wk-tracked' \
                       AND version_id = '{version_id}' \
                       AND is_tombstone = 0 \
                     LIMIT 1"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(materialized.statements[0].rows.len(), 1);
        assert_text(&materialized.statements[0].rows[0][0], "editor:both");

        let untracked = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_internal_state_untracked \
                     WHERE entity_id = 'wk-untracked' \
                       AND version_id = '{version_id}' \
                     LIMIT 1"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(untracked.statements[0].rows.len(), 1);
        assert_text(&untracked.statements[0].rows[0][0], "editor:both");

        let view_rows = engine
            .execute(
                &format!(
                    "SELECT entity_id, writer_key \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'wk_writer_key_schema' \
                       AND version_id = '{version_id}' \
                     ORDER BY entity_id"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(view_rows.statements[0].rows.len(), 2);
        assert_text(&view_rows.statements[0].rows[0][0], "wk-tracked");
        assert_text(&view_rows.statements[0].rows[0][1], "editor:both");
        assert_text(&view_rows.statements[0].rows[1][0], "wk-untracked");
        assert_text(&view_rows.statements[0].rows[1][1], "editor:both");
    }
);

simulation_test!(
    writer_key_visible_in_file_and_state_views_for_execute_options,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();
        engine
            .execute(
                "SELECT writer_key FROM lix_internal_state_materialized_v1_lix_file_descriptor LIMIT 0", &[])
            .await
            .unwrap();

        engine
            .execute_with_options(
                "INSERT INTO lix_file (id, path, data) VALUES ('wk-file-1', '/wk-file-1.json', lix_text_encode('ignored'))",
                &[],
                ExecuteOptions {
                    writer_key: Some("editor:single".to_string()),
                },
            )
            .await
            .unwrap();

        let file_row = engine
            .execute(
                "SELECT lixcol_writer_key FROM lix_file WHERE id = 'wk-file-1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(file_row.statements[0].rows.len(), 1);
        assert_text(&file_row.statements[0].rows[0][0], "editor:single");

        let version_id = active_version_id(&engine).await;
        let state_row = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'lix_file_descriptor' \
                       AND entity_id = 'wk-file-1' \
                       AND version_id = '{version_id}'"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(state_row.statements[0].rows.len(), 1);
        assert_text(&state_row.statements[0].rows[0][0], "editor:single");
    }
);

simulation_test!(
    writer_key_is_inherited_by_all_statements_in_transaction,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine

            .transaction(
                ExecuteOptions {
                    writer_key: Some("editor:tx".to_string()),
                },
                |tx| {
                    Box::pin(async move {
                        tx.execute(
                            "INSERT INTO lix_file (id, path, data) VALUES ('wk-tx-1', '/wk-tx-1.json', lix_text_encode('ignored'))",
                            &[],
                        )
                        .await?;
                        tx.execute(
                            "INSERT INTO lix_file (id, path, data) VALUES ('wk-tx-2', '/wk-tx-2.json', lix_text_encode('ignored'))",
                            &[],
                        )
                        .await?;
                        Ok(())
                    })
                },
            )
            .await
            .unwrap();

        let version_id = active_version_id(&engine).await;
        let rows = engine
            .execute(
                &format!(
                    "SELECT entity_id, writer_key \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'lix_file_descriptor' \
                       AND version_id = '{version_id}' \
                       AND entity_id IN ('wk-tx-1', 'wk-tx-2') \
                     ORDER BY entity_id"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_text(&rows.statements[0].rows[0][0], "wk-tx-1");
        assert_text(&rows.statements[0].rows[0][1], "editor:tx");
        assert_text(&rows.statements[0].rows[1][0], "wk-tx-2");
        assert_text(&rows.statements[0].rows[1][1], "editor:tx");
    }
);

simulation_test!(
    update_without_writer_key_clears_writer_key,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute_with_options(
                "INSERT INTO lix_file (id, path, data) VALUES ('wk-clear-update', '/wk-clear-update.json', lix_text_encode('before'))",
                &[],
                ExecuteOptions {
                    writer_key: Some("editor:initial".to_string()),
                },
            )
            .await
            .unwrap();

        engine
            .execute(
                "UPDATE lix_internal_state_vtable \
                 SET metadata = '{\"source\":\"update\"}' \
                 WHERE schema_key = 'lix_file_descriptor' \
                   AND entity_id = 'wk-clear-update'",
                &[],
            )
            .await
            .unwrap();

        let version_id = active_version_id(&engine).await;
        let state_row = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'lix_file_descriptor' \
                       AND entity_id = 'wk-clear-update' \
                       AND version_id = '{version_id}'"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(state_row.statements[0].rows.len(), 1);
        assert_null(&state_row.statements[0].rows[0][0]);
    }
);

simulation_test!(
    delete_without_writer_key_clears_tombstone_writer_key,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute_with_options(
                "INSERT INTO lix_file (id, path, data) VALUES ('wk-clear-delete', '/wk-clear-delete.json', lix_text_encode('before'))",
                &[],
                ExecuteOptions {
                    writer_key: Some("editor:initial".to_string()),
                },
            )
            .await
            .unwrap();

        engine
            .execute("DELETE FROM lix_file WHERE id = 'wk-clear-delete'", &[])
            .await
            .unwrap();

        let version_id = active_version_id(&engine).await;
        let tombstone = engine
            .execute(
                &format!(
                    "SELECT writer_key, is_tombstone \
                     FROM lix_internal_state_materialized_v1_lix_file_descriptor \
                     WHERE entity_id = 'wk-clear-delete' \
                       AND version_id = '{version_id}' \
                     LIMIT 1"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(tombstone.statements[0].rows.len(), 1);
        assert_null(&tombstone.statements[0].rows[0][0]);
        assert_eq!(tombstone.statements[0].rows[0][1], Value::Integer(1));
    }
);

simulation_test!(
    transaction_rollback_discards_writer_key_tagged_writes,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let error = engine

            .transaction(
                ExecuteOptions {
                    writer_key: Some("editor:rollback".to_string()),
                },
                |tx| {
                    Box::pin(async move {
                        tx.execute(
                            "INSERT INTO lix_file (id, path, data) VALUES ('wk-rolled-back', '/wk-rolled-back.json', lix_text_encode('ignored'))",
                            &[],
                        )
                        .await?;
                        Err::<(), LixError>(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: "rollback test".to_string(),
                        })
                    })
                },
            )
            .await
            .expect_err("transaction should roll back on closure error");
        assert!(
            error.description.contains("rollback test"),
            "unexpected error: {}",
            error.description
        );

        let file_rows = engine
            .execute("SELECT id FROM lix_file WHERE id = 'wk-rolled-back'", &[])
            .await
            .unwrap();
        assert!(file_rows.statements[0].rows.is_empty());
    }
);

simulation_test!(
    transaction_file_writes_persist_payload_reads,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine

            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO lix_file (id, path, data) VALUES ('wk-tx-cache', '/wk-tx-cache.json', lix_text_encode('before'))",
                        &[],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        "UPDATE lix_file SET data = lix_text_encode('after') WHERE id = 'wk-tx-cache'",
                        &[],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let file_rows = engine
            .execute("SELECT data FROM lix_file WHERE id = 'wk-tx-cache'", &[])
            .await
            .unwrap();
        assert_eq!(file_rows.statements[0].rows.len(), 1);
        assert_blob_text(&file_rows.statements[0].rows[0][0], "after");
    }
);

simulation_test!(
    explicit_writer_key_update_is_preserved_in_followup_commit,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute_with_options(
                "INSERT INTO lix_file (id, path, data) VALUES ('wk-update-writer', '/wk-update-writer.json', lix_text_encode('ignored'))",
                &[],
                ExecuteOptions {
                    writer_key: Some("editor:initial".to_string()),
                },
            )
            .await
            .unwrap();

        engine
            .execute(
                "UPDATE lix_internal_state_vtable \
                 SET writer_key = 'editor:explicit-update' \
                 WHERE schema_key = 'lix_file_descriptor' \
                   AND entity_id = 'wk-update-writer'",
                &[],
            )
            .await
            .unwrap();

        let version_id = active_version_id(&engine).await;
        let state_row = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'lix_file_descriptor' \
                       AND entity_id = 'wk-update-writer' \
                       AND version_id = '{version_id}'"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(state_row.statements[0].rows.len(), 1);
        assert_text(
            &state_row.statements[0].rows[0][0],
            "editor:explicit-update",
        );
    }
);

simulation_test!(
    sql2_state_by_version_update_uses_current_execution_writer_key,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();
        register_writer_key_test_schema(&engine).await;

        let version_id = active_version_id(&engine).await;
        engine
            .execute_with_options(
                &format!(
                    "INSERT INTO lix_internal_state_vtable (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) VALUES (\
                     'wk-sql2-update', 'wk_writer_key_schema', 'file-1', '{version_id}', 'lix', '{{\"key\":\"before\"}}', '1'\
                     )"
                ),
                &[],
                ExecuteOptions {
                    writer_key: Some("editor:initial".to_string()),
                },
            )
            .await
            .unwrap();

        engine
            .execute_with_options(
                &format!(
                    "UPDATE lix_state_by_version \
                     SET snapshot_content = '{{\"key\":\"after\"}}' \
                     WHERE schema_key = 'wk_writer_key_schema' \
                       AND entity_id = 'wk-sql2-update' \
                       AND file_id = 'file-1' \
                       AND version_id = '{version_id}'"
                ),
                &[],
                ExecuteOptions {
                    writer_key: Some("editor:update".to_string()),
                },
            )
            .await
            .unwrap();

        let state_row = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'wk_writer_key_schema' \
                       AND entity_id = 'wk-sql2-update' \
                       AND file_id = 'file-1' \
                       AND version_id = '{version_id}'"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(state_row.statements[0].rows.len(), 1);
        assert_text(&state_row.statements[0].rows[0][0], "editor:update");
    }
);
