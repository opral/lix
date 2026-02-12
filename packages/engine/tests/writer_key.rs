mod support;

use lix_engine::{ExecuteOptions, LixError, Value};

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
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
