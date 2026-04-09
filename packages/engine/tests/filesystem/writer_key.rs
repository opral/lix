use crate::support;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use lix_engine::wasm::NoopWasmRuntime;
use lix_engine::{boot, BootArgs, ExecuteOptions, LixError, Value};
use serde_json::json;

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

fn temp_sqlite_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lix-writer-key-{label}-{}-{nanos}.sqlite",
        std::process::id()
    ))
}

fn cleanup_sqlite_path(path: &Path) {
    let _ = std::fs::remove_file(path);
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    let shm = PathBuf::from(format!("{}-shm", path.display()));
    let journal = PathBuf::from(format!("{}-journal", path.display()));
    let _ = std::fs::remove_file(wal);
    let _ = std::fs::remove_file(shm);
    let _ = std::fs::remove_file(journal);
}

fn boot_sqlite_engine_at_path(path: &Path) -> Arc<lix_engine::Engine> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("sqlite test parent directory should be creatable");
    }
    let _ = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
        .expect("sqlite test file should be creatable");
    let mut args = BootArgs::new(
        support::simulations::sqlite_backend_with_filename(format!("sqlite://{}", path.display())),
        Arc::new(NoopWasmRuntime),
    );
    args.access_to_internal = true;
    Arc::new(boot(args))
}

async fn register_writer_key_test_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .register_schema(&json!({
            "x-lix-key": "wk_writer_key_schema",
            "x-lix-version": "1",
            "type": "object",
            "properties": {
                "key": { "type": "string" }
            },
            "required": ["key"],
            "additionalProperties": false
        }))
        .await
        .unwrap();
}

simulation_test!(
    tracked_reads_overlay_writer_key_while_untracked_rows_store_it,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();
        register_writer_key_test_schema(&engine).await;

        let version_id = engine.active_version_id().await.unwrap();

        engine
            .execute_with_options(
                &format!(
                    "INSERT INTO lix_state_by_version (\
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
                    "INSERT INTO lix_state_by_version (\
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

        let workspace_annotation = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_internal_writer_key \
                     WHERE version_id = '{version_id}' \
                       AND schema_key = 'wk_writer_key_schema' \
                       AND entity_id = 'wk-tracked' \
                       AND file_id = 'file-1' \
                     LIMIT 1"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(workspace_annotation.statements[0].rows.len(), 1);
        assert_text(
            &workspace_annotation.statements[0].rows[0][0],
            "editor:both",
        );

        let untracked = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_internal_live_v1_wk_writer_key_schema \
                     WHERE entity_id = 'wk-untracked' \
                       AND version_id = '{version_id}' \
                       AND untracked = true \
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
        engine.initialize().await.unwrap();
        engine
            .execute(
                "SELECT writer_key FROM lix_internal_live_v1_lix_file_descriptor LIMIT 0",
                &[],
            )
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

        let version_id = engine.active_version_id().await.unwrap();
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

        let workspace_annotation = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_internal_writer_key \
                     WHERE version_id = '{version_id}' \
                       AND schema_key = 'lix_file_descriptor' \
                       AND entity_id = 'wk-file-1' \
                       AND file_id = 'lix' \
                     LIMIT 1"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(workspace_annotation.statements[0].rows.len(), 1);
        assert_text(
            &workspace_annotation.statements[0].rows[0][0],
            "editor:single",
        );

        engine
            .rebuild_live_state(&lix_engine::LiveStateRebuildRequest {
                scope: lix_engine::LiveStateRebuildScope::Full,
                debug: lix_engine::LiveStateRebuildDebugMode::Off,
                debug_row_limit: 1,
            })
            .await
            .unwrap();

        let raw_tracked = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_internal_live_v1_lix_file_descriptor \
                     WHERE entity_id = 'wk-file-1' \
                       AND version_id = '{version_id}' \
                       AND is_tombstone = 0 \
                       AND untracked = false"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(raw_tracked.statements[0].rows.len(), 1);
        assert_null(&raw_tracked.statements[0].rows[0][0]);

        let rebuilt_file_row = engine
            .execute(
                "SELECT lixcol_writer_key FROM lix_file WHERE id = 'wk-file-1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rebuilt_file_row.statements[0].rows.len(), 1);
        assert_text(&rebuilt_file_row.statements[0].rows[0][0], "editor:single");

        let rebuilt_state_row = engine
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
        assert_eq!(rebuilt_state_row.statements[0].rows.len(), 1);
        assert_text(&rebuilt_state_row.statements[0].rows[0][0], "editor:single");

        let filtered_file_row = engine
            .execute(
                "SELECT id FROM lix_file WHERE lixcol_writer_key = 'editor:single'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(filtered_file_row.statements[0].rows.len(), 1);
        assert_text(&filtered_file_row.statements[0].rows[0][0], "wk-file-1");
    }
);

#[test]
fn writer_key_annotation_persists_across_engine_reopen_sqlite() {
    let path = temp_sqlite_path("persist-reopen");
    let path_for_thread = path.clone();
    let thread = std::thread::Builder::new()
        .name("writer_key_annotation_persists_across_engine_reopen_sqlite".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime should build");
            runtime.block_on(async move {
                let engine_a = boot_sqlite_engine_at_path(&path_for_thread);
                engine_a.initialize().await.expect("init should succeed");

                let session_a = engine_a
                    .open_session()
                    .await
                    .expect("workspace session should open");
                session_a
                    .execute_with_options(
                        "INSERT INTO lix_file (id, path, data) VALUES ('wk-reopen', '/wk-reopen.json', lix_text_encode('persist'))",
                        &[],
                        ExecuteOptions {
                            writer_key: Some("editor:persist".to_string()),
                        },
                    )
                    .await
                    .expect("writer-key insert should succeed");
                let version_id = session_a.active_version_id();

                drop(session_a);
                drop(engine_a);

                let engine_b = boot_sqlite_engine_at_path(&path_for_thread);
                let reopen_err = engine_b
                    .initialize()
                    .await
                    .expect_err("reopen init should report already initialized");
                assert_eq!(reopen_err.code, "LIX_ERROR_ALREADY_INITIALIZED");
                engine_b
                    .open_existing()
                    .await
                    .expect("open_existing should load workspace state");
                engine_b
                    .rebuild_live_state(&lix_engine::LiveStateRebuildRequest {
                        scope: lix_engine::LiveStateRebuildScope::Full,
                        debug: lix_engine::LiveStateRebuildDebugMode::Off,
                        debug_row_limit: 1,
                    })
                    .await
                    .expect("rebuild after reopen should succeed");

                let session_b = engine_b
                    .open_session()
                    .await
                    .expect("workspace session should reopen");

                let workspace_annotation = session_b
                    .execute(
                        &format!(
                            "SELECT writer_key \
                             FROM lix_internal_writer_key \
                             WHERE version_id = '{version_id}' \
                               AND schema_key = 'lix_file_descriptor' \
                               AND entity_id = 'wk-reopen' \
                               AND file_id = 'lix'"
                        ),
                        &[],
                    )
                    .await
                    .expect("workspace annotation query should succeed");
                assert_eq!(workspace_annotation.statements[0].rows.len(), 1);
                assert_text(
                    &workspace_annotation.statements[0].rows[0][0],
                    "editor:persist",
                );

                let raw_tracked = session_b
                    .execute(
                        &format!(
                            "SELECT writer_key \
                             FROM lix_internal_live_v1_lix_file_descriptor \
                             WHERE entity_id = 'wk-reopen' \
                               AND version_id = '{version_id}' \
                               AND is_tombstone = 0 \
                               AND untracked = false"
                        ),
                        &[],
                    )
                    .await
                    .expect("raw tracked query should succeed");
                assert_eq!(raw_tracked.statements[0].rows.len(), 1);
                assert_null(&raw_tracked.statements[0].rows[0][0]);

                let state_row = session_b
                    .execute(
                        &format!(
                            "SELECT writer_key \
                             FROM lix_state_by_version \
                             WHERE schema_key = 'lix_file_descriptor' \
                               AND entity_id = 'wk-reopen' \
                               AND version_id = '{version_id}'"
                        ),
                        &[],
                    )
                    .await
                    .expect("public state query should succeed");
                assert_eq!(state_row.statements[0].rows.len(), 1);
                assert_text(&state_row.statements[0].rows[0][0], "editor:persist");
            });
        })
        .expect("writer-key reopen thread should spawn");

    thread
        .join()
        .expect("writer-key reopen thread should not panic");
    cleanup_sqlite_path(&path);
}

simulation_test!(
    writer_key_is_inherited_by_all_statements_in_transaction,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

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

        let version_id = engine.active_version_id().await.unwrap();
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
        engine.initialize().await.unwrap();

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
                "UPDATE lix_state_by_version \
                 SET metadata = '{\"source\":\"update\"}' \
                 WHERE schema_key = 'lix_file_descriptor' \
                   AND entity_id = 'wk-clear-update' \
                   AND version_id = lix_active_version_id()",
                &[],
            )
            .await
            .unwrap();

        let version_id = engine.active_version_id().await.unwrap();
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

        let workspace_rows = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_internal_writer_key \
                     WHERE version_id = '{version_id}' \
                       AND schema_key = 'lix_file_descriptor' \
                       AND entity_id = 'wk-clear-update' \
                       AND file_id = 'lix'"
                ),
                &[],
            )
            .await
            .unwrap();
        assert!(
            workspace_rows.statements[0].rows.is_empty(),
            "workspace writer annotation should be cleared when no writer_key is supplied"
        );
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
        engine.initialize().await.unwrap();

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

        let version_id = engine.active_version_id().await.unwrap();
        let tombstone = engine
            .execute(
                &format!(
                    "SELECT writer_key, is_tombstone \
                     FROM lix_internal_live_v1_lix_file_descriptor \
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
        engine.initialize().await.unwrap();

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

        let version_id = engine.active_version_id().await.unwrap();
        let workspace_rows = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_internal_writer_key \
                     WHERE version_id = '{version_id}' \
                       AND schema_key = 'lix_file_descriptor' \
                       AND entity_id = 'wk-rolled-back' \
                       AND file_id = 'lix'"
                ),
                &[],
            )
            .await
            .unwrap();
        assert!(
            workspace_rows.statements[0].rows.is_empty(),
            "rolled-back transactions must not leave workspace writer annotations behind"
        );
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
        engine.initialize().await.unwrap();

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
        engine.initialize().await.unwrap();

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
                "UPDATE lix_state_by_version \
                 SET writer_key = 'editor:explicit-update' \
                 WHERE schema_key = 'lix_file_descriptor' \
                   AND entity_id = 'wk-update-writer' \
                   AND version_id = lix_active_version_id()",
                &[],
            )
            .await
            .unwrap();

        let version_id = engine.active_version_id().await.unwrap();
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
    public_state_by_version_update_uses_current_execution_writer_key,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();
        register_writer_key_test_schema(&engine).await;

        let version_id = engine.active_version_id().await.unwrap();
        engine
            .execute_with_options(
                &format!(
                    "INSERT INTO lix_state_by_version (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) VALUES (\
                     'wk-public-update', 'wk_writer_key_schema', 'file-1', '{version_id}', 'lix', '{{\"key\":\"before\"}}', '1'\
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
                       AND entity_id = 'wk-public-update' \
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
                       AND entity_id = 'wk-public-update' \
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
