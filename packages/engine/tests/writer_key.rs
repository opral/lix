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
    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected active version id as text, got {other:?}"),
    }
}

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
                "SELECT writer_key FROM lix_internal_state_materialized_v1_lix_file_descriptor LIMIT 0",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute_with_options(
                "INSERT INTO lix_file (id, path, data) VALUES ('wk-file-1', '/wk-file-1.json', 'ignored')",
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
        assert_eq!(file_row.rows.len(), 1);
        assert_text(&file_row.rows[0][0], "editor:single");

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
        assert_eq!(state_row.rows.len(), 1);
        assert_text(&state_row.rows[0][0], "editor:single");
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
            .raw_engine()
            .transaction(
                ExecuteOptions {
                    writer_key: Some("editor:tx".to_string()),
                },
                |tx| {
                    Box::pin(async move {
                        tx.execute(
                            "INSERT INTO lix_file (id, path, data) VALUES ('wk-tx-1', '/wk-tx-1.json', 'ignored')",
                            &[],
                        )
                        .await?;
                        tx.execute(
                            "INSERT INTO lix_file (id, path, data) VALUES ('wk-tx-2', '/wk-tx-2.json', 'ignored')",
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
        assert_eq!(rows.rows.len(), 2);
        assert_text(&rows.rows[0][0], "wk-tx-1");
        assert_text(&rows.rows[0][1], "editor:tx");
        assert_text(&rows.rows[1][0], "wk-tx-2");
        assert_text(&rows.rows[1][1], "editor:tx");
    }
);

simulation_test!(
    writer_key_statement_override_works_inside_transaction,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .raw_engine()
            .transaction(
                ExecuteOptions {
                    writer_key: Some("editor:tx-default".to_string()),
                },
                |tx| {
                    Box::pin(async move {
                        tx.execute_with_options(
                            "INSERT INTO lix_file (id, path, data) VALUES ('wk-override-1', '/wk-override-1.json', 'ignored')",
                            &[],
                            ExecuteOptions {
                                writer_key: Some("editor:override".to_string()),
                            },
                        )
                        .await?;
                        tx.execute(
                            "INSERT INTO lix_file (id, path, data) VALUES ('wk-override-2', '/wk-override-2.json', 'ignored')",
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
                       AND entity_id IN ('wk-override-1', 'wk-override-2') \
                     ORDER BY entity_id"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_text(&rows.rows[0][0], "wk-override-1");
        assert_text(&rows.rows[0][1], "editor:override");
        assert_text(&rows.rows[1][0], "wk-override-2");
        assert_text(&rows.rows[1][1], "editor:tx-default");
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
                "INSERT INTO lix_file (id, path, data) VALUES ('wk-clear-update', '/wk-clear-update.json', 'before')",
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
        assert_eq!(state_row.rows.len(), 1);
        assert_null(&state_row.rows[0][0]);
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
                "INSERT INTO lix_file (id, path, data) VALUES ('wk-clear-delete', '/wk-clear-delete.json', 'before')",
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
        assert_eq!(tombstone.rows.len(), 1);
        assert_null(&tombstone.rows[0][0]);
        assert_eq!(tombstone.rows[0][1], Value::Integer(1));
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
            .raw_engine()
            .transaction(
                ExecuteOptions {
                    writer_key: Some("editor:rollback".to_string()),
                },
                |tx| {
                    Box::pin(async move {
                        tx.execute(
                            "INSERT INTO lix_file (id, path, data) VALUES ('wk-rolled-back', '/wk-rolled-back.json', 'ignored')",
                            &[],
                        )
                        .await?;
                        Err::<(), LixError>(LixError {
                            message: "rollback test".to_string(),
                        })
                    })
                },
            )
            .await
            .expect_err("transaction should roll back on closure error");
        assert!(
            error.message.contains("rollback test"),
            "unexpected error: {}",
            error.message
        );

        let file_rows = engine
            .execute("SELECT id FROM lix_file WHERE id = 'wk-rolled-back'", &[])
            .await
            .unwrap();
        assert!(file_rows.rows.is_empty());
    }
);

simulation_test!(
    transaction_file_writes_persist_file_data_cache,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .raw_engine()
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO lix_file (id, path, data) VALUES ('wk-tx-cache', '/wk-tx-cache.json', 'before')",
                        &[],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        engine
            .raw_engine()
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        "UPDATE lix_file SET data = 'after' WHERE id = 'wk-tx-cache'",
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
        assert_eq!(file_rows.rows.len(), 1);
        assert_blob_text(&file_rows.rows[0][0], "after");

        let version_id = active_version_id(&engine).await;
        let cache_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'wk-tx-cache' AND version_id = '{version_id}'"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(cache_rows.rows.len(), 1);
        assert_blob_text(&cache_rows.rows[0][0], "after");
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
                "INSERT INTO lix_file (id, path, data) VALUES ('wk-update-writer', '/wk-update-writer.json', 'ignored')",
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
        assert_eq!(state_row.rows.len(), 1);
        assert_text(&state_row.rows[0][0], "editor:explicit-update");
    }
);

simulation_test!(
    init_migrates_legacy_writer_key_columns,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");

        engine
            .raw_engine()
            .execute(
                "CREATE TABLE lix_internal_state_materialized_v1_legacy_schema (\
                 entity_id TEXT NOT NULL,\
                 schema_key TEXT NOT NULL,\
                 schema_version TEXT NOT NULL,\
                 file_id TEXT NOT NULL,\
                 version_id TEXT NOT NULL,\
                 plugin_key TEXT NOT NULL,\
                 snapshot_content TEXT,\
                 inherited_from_version_id TEXT,\
                 change_id TEXT NOT NULL,\
                 metadata TEXT,\
                 is_tombstone INTEGER NOT NULL DEFAULT 0,\
                 created_at TEXT NOT NULL,\
                 updated_at TEXT NOT NULL,\
                 PRIMARY KEY (entity_id, file_id, version_id)\
                 )",
                &[],
            )
            .await
            .unwrap();
        engine
            .raw_engine()
            .execute(
                "CREATE TABLE lix_internal_file_lixcol_cache (\
                 file_id TEXT NOT NULL,\
                 version_id TEXT NOT NULL,\
                 latest_change_id TEXT,\
                 latest_commit_id TEXT,\
                 created_at TEXT,\
                 updated_at TEXT,\
                 PRIMARY KEY (file_id, version_id)\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine.init().await.unwrap();

        engine
            .execute(
                "SELECT writer_key FROM lix_internal_state_materialized_v1_legacy_schema LIMIT 0",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "SELECT writer_key FROM lix_internal_file_lixcol_cache LIMIT 0",
                &[],
            )
            .await
            .unwrap();
    }
);
