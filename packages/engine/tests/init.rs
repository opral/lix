mod support;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lix_engine::{boot, BootAccount, BootArgs, NoopWasmRuntime};

const CHECKPOINT_LABEL_ID: &str = "lix_label_checkpoint";

fn text_value(value: &lix_engine::Value, field: &str) -> String {
    match value {
        lix_engine::Value::Text(value) => value.clone(),
        other => panic!("expected text value for {field}, got {other:?}"),
    }
}

fn i64_value(value: &lix_engine::Value, field: &str) -> i64 {
    match value {
        lix_engine::Value::Integer(value) => *value,
        lix_engine::Value::Text(value) => value.parse::<i64>().unwrap_or_else(|error| {
            panic!("expected i64 text for {field}, got '{value}': {error}")
        }),
        other => panic!("expected i64 value for {field}, got {other:?}"),
    }
}

fn assert_uuid_v7_like(value: &str, field: &str) {
    assert_eq!(
        value.len(),
        36,
        "expected {field} to be uuid-like, got {value}"
    );
    assert_eq!(
        value.chars().nth(14),
        Some('7'),
        "expected {field} to be uuid v7-like, got {value}"
    );
}

async fn active_version_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let result = engine
        .execute(
            "SELECT version_id \
             FROM lix_active_version \
             ORDER BY id \
             LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.statements[0].rows.len(), 1);
    text_value(
        &result.statements[0].rows[0][0],
        "lix_active_version.version_id",
    )
}

async fn global_version_commit_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let result = engine
        .execute(
            "SELECT lix_json_extract(snapshot_content, 'commit_id') AS commit_id \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_version_ref' \
               AND entity_id = 'global' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND snapshot_content IS NOT NULL \
             ORDER BY updated_at DESC, created_at DESC, change_id DESC \
             LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.statements[0].rows.len(), 1);
    text_value(
        &result.statements[0].rows[0][0],
        "lix_version_ref.commit_id",
    )
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

fn temp_sqlite_init_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lix-init-{label}-{}-{nanos}.sqlite",
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

#[test]
fn init_parallel_open_same_sqlite_path_avoids_raw_unique_conflicts() {
    const ATTEMPTS: usize = 30;
    const THREADS_PER_ATTEMPT: usize = 2;

    let mut failures = Vec::new();

    for attempt in 0..ATTEMPTS {
        let path = temp_sqlite_init_path(&format!("parallel-open-{attempt}"));
        let barrier = Arc::new(std::sync::Barrier::new(THREADS_PER_ATTEMPT + 1));
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(THREADS_PER_ATTEMPT);
        let mut handles = Vec::new();

        for worker in 0..THREADS_PER_ATTEMPT {
            let path_for_thread = path.clone();
            let barrier_for_thread = Arc::clone(&barrier);
            let result_tx = result_tx.clone();
            let handle = std::thread::Builder::new()
                .name(format!(
                    "init_parallel_open_same_sqlite_path_should_not_fail-{attempt}-{worker}"
                ))
                .stack_size(8 * 1024 * 1024)
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build tokio runtime");

                    barrier_for_thread.wait();

                    let result = runtime.block_on(async move {
                        let engine = boot_sqlite_engine_at_path(&path_for_thread);
                        engine.initialize().await.map_err(|err| err.to_string())
                    });

                    let _ = result_tx.send(result);
                })
                .expect("failed to spawn parallel init worker");
            handles.push(handle);
        }

        drop(result_tx);
        barrier.wait();

        let mut attempt_errors = Vec::new();
        for _ in 0..THREADS_PER_ATTEMPT {
            let received = result_rx
                .recv_timeout(Duration::from_secs(120))
                .expect("parallel init worker result should arrive");
            if let Err(err) = received {
                attempt_errors.push(err);
            }
        }

        for handle in handles {
            handle.join().expect("parallel init worker thread panicked");
        }

        cleanup_sqlite_path(&path);

        for error in attempt_errors {
            if error.contains("LIX_ERROR_ALREADY_INITIALIZED") {
                continue;
            }
            let lower = error.to_ascii_lowercase();
            if lower.contains("unique constraint failed")
                || lower.contains("unique constraint violation")
            {
                failures.push(format!(
                    "attempt {attempt}: raw unique conflict leaked instead of ALREADY_INITIALIZED: {error}"
                ));
                continue;
            }
            failures.push(format!("attempt {attempt}: unexpected init error: {error}"));
        }
    }

    assert!(
        failures.is_empty(),
        "parallel init on the same sqlite path produced invalid errors: {}",
        failures.join("\n")
    );
}

#[test]
fn init_reopen_preserves_working_changes_sqlite() {
    let path = temp_sqlite_init_path("working-changes");
    let path_for_thread = path.clone();
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name("init_reopen_preserves_working_changes_sqlite".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let run_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build tokio runtime");
                runtime.block_on(async move {
                    let engine_a = boot_sqlite_engine_at_path(&path_for_thread);
                    engine_a.initialize().await.expect("first init should succeed");

                    let session_a = engine_a
                        .open_workspace_session()
                        .await
                        .expect("workspace session should open after init");

                    session_a
                        .execute(
                            "INSERT INTO lix_file (path, data, metadata) \
                             VALUES ('/wc-reopen.md', lix_text_encode('hello'), NULL)", &[])
                        .await
                        .expect("file insert should succeed");

                    let file_result = session_a
                        .execute(
                            "SELECT id \
                             FROM lix_file \
                             WHERE path = '/wc-reopen.md' \
                             LIMIT 1", &[])
                        .await
                        .expect("file lookup should succeed");
                    let [file_result] = file_result.statements.as_slice() else {
                        panic!(
                            "file lookup query: expected 1 statement result(s), got {}",
                            file_result.statements.len()
                        );
                    };
                    assert_eq!(file_result.rows.len(), 1);
                    let file_id = text_value(&file_result.rows[0][0], "file_id");

                    let before = session_a
                        .execute(
                            "SELECT COUNT(*) \
                             FROM lix_working_changes \
                             WHERE schema_key = 'lix_file_descriptor' \
                               AND file_id = 'lix' \
                               AND entity_id = $1", &[lix_engine::Value::Text(file_id.clone())])
                        .await
                        .expect("working changes query before reopen should succeed");
                    let [before] = before.statements.as_slice() else {
                        panic!(
                            "working changes query before reopen: expected 1 statement result(s), got {}",
                            before.statements.len()
                        );
                    };
                    let before_count = i64_value(&before.rows[0][0], "working_changes_before");
                    assert!(
                        before_count > 0,
                        "expected working changes before reopen, got {before_count}"
                    );

                    drop(engine_a);

                    let engine_b = boot_sqlite_engine_at_path(&path_for_thread);
                    let reopen_init_err = engine_b
                        .initialize()
                        .await
                        .expect_err("reopen init should report already initialized");
                    assert_eq!(reopen_init_err.code, "LIX_ERROR_ALREADY_INITIALIZED");
                    engine_b
                        .open_existing()
                        .await
                        .expect("reopen open should load active version state");
                    let session_b = engine_b
                        .open_workspace_session()
                        .await
                        .expect("workspace session should open after reopen");

                    let after = session_b
                        .execute(
                            "SELECT COUNT(*) \
                             FROM lix_working_changes \
                             WHERE schema_key = 'lix_file_descriptor' \
                               AND file_id = 'lix' \
                               AND entity_id = $1", &[lix_engine::Value::Text(file_id.clone())])
                        .await
                        .expect("working changes query after reopen should succeed");
                    let [after] = after.statements.as_slice() else {
                        panic!(
                            "working changes query after reopen: expected 1 statement result(s), got {}",
                            after.statements.len()
                        );
                    };
                    let after_count = i64_value(&after.rows[0][0], "working_changes_after");

                    let tip_result = session_b
                        .execute(
                            "SELECT v.commit_id \
                             FROM lix_active_version av \
                             JOIN lix_version v ON v.id = av.version_id \
                             ORDER BY av.id \
                             LIMIT 1", &[])
                        .await
                        .expect("tip query should succeed");
                    let [tip_result] = tip_result.statements.as_slice() else {
                        panic!(
                            "tip query: expected 1 statement result(s), got {}",
                            tip_result.statements.len()
                        );
                    };
                    assert_eq!(tip_result.rows.len(), 1);
                    let head_commit_id = text_value(&tip_result.rows[0][0], "commit_id");

                    let after_rows = session_b
                        .execute(
                            "SELECT status, before_change_id, after_change_id \
                             FROM lix_working_changes \
                             WHERE schema_key = 'lix_file_descriptor' \
                               AND file_id = 'lix' \
                               AND entity_id = $1", &[lix_engine::Value::Text(file_id)])
                        .await
                        .expect("working row query after reopen should succeed");
                    let [after_rows] = after_rows.statements.as_slice() else {
                        panic!(
                            "working row query after reopen: expected 1 statement result(s), got {}",
                            after_rows.statements.len()
                        );
                    };
                    assert_eq!(after_rows.rows.len(), 1, "expected one working row after reopen");
                    assert_eq!(text_value(&after_rows.rows[0][0], "status"), "added");
                    assert_eq!(after_rows.rows[0][1], lix_engine::Value::Null);
                    assert!(
                        !text_value(&after_rows.rows[0][2], "after_change_id").is_empty(),
                        "expected non-empty after_change_id after reopen"
                    );

                    assert_eq!(
                        after_count, before_count,
                        "reopen changed working changes count (before={before_count}, after={after_count}, tip={head_commit_id})"
                    );
                });
            }));
            let _ = result_tx.send(run_result);
        })
        .expect("failed to spawn init reopen sqlite test thread");

    let recv_result = result_rx.recv_timeout(Duration::from_secs(120));
    cleanup_sqlite_path(&path);
    match recv_result {
        Ok(Ok(())) => {
            thread
                .join()
                .expect("init reopen sqlite test thread panicked");
        }
        Ok(Err(payload)) => {
            let _ = thread.join();
            std::panic::resume_unwind(payload);
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("init reopen sqlite test timed out after 120s");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            if let Err(payload) = thread.join() {
                std::panic::resume_unwind(payload);
            }
            panic!("init reopen sqlite test disconnected without result");
        }
    }
}

#[test]
fn reopen_after_bare_multi_statement_write_succeeds_sqlite() {
    let path = temp_sqlite_init_path("multi-stmt-reopen");
    let path_for_thread = path.clone();
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name("reopen_after_bare_multi_statement_write_succeeds_sqlite".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let run_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build tokio runtime");
                runtime.block_on(async move {
                    // --- First session: init + bare multi-statement write ---
                    let engine_a = boot_sqlite_engine_at_path(&path_for_thread);
                    engine_a.initialize().await.expect("init should succeed");

                    // Two INSERT statements separated by ; WITHOUT BEGIN/COMMIT.
                    // This is exactly what the CLI does when a user runs:
                    //   lix sql execute "INSERT ...; INSERT ...;"
                    let session_a = engine_a
                        .open_workspace_session()
                        .await
                        .expect("workspace session should open after init");

                    session_a
                        .execute(
                            "INSERT INTO lix_key_value (key, value) VALUES ('a', '\"1\"'); \
                             INSERT INTO lix_key_value (key, value) VALUES ('b', '\"2\"');",
                            &[],
                        )
                        .await
                        .expect("bare multi-statement write should succeed");

                    drop(engine_a);

                    // --- Second session: reopen must not fail ---
                    let engine_b = boot_sqlite_engine_at_path(&path_for_thread);
                    engine_b.open_existing().await.expect(
                        "reopen after bare multi-statement write must not fail \
                             with LIX_ERROR_LIVE_STATE_NOT_READY",
                    );
                    let session_b = engine_b
                        .open_workspace_session()
                        .await
                        .expect("workspace session should open after reopen");

                    // Verify both rows are actually readable.
                    let result = session_b
                        .execute(
                            "SELECT key, value FROM lix_key_value \
                             WHERE key IN ('a', 'b') ORDER BY key",
                            &[],
                        )
                        .await
                        .expect("select after reopen should succeed");
                    assert_eq!(
                        result.statements[0].rows.len(),
                        2,
                        "expected both inserted rows to be readable after reopen"
                    );
                });
            }));
            let _ = result_tx.send(run_result);
        })
        .expect("failed to spawn multi-statement reopen test thread");

    let recv_result = result_rx.recv_timeout(Duration::from_secs(120));
    cleanup_sqlite_path(&path);
    match recv_result {
        Ok(Ok(())) => {
            thread
                .join()
                .expect("multi-statement reopen test thread panicked");
        }
        Ok(Err(payload)) => {
            let _ = thread.join();
            std::panic::resume_unwind(payload);
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("multi-statement reopen test timed out after 120s");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            if let Err(payload) = thread.join() {
                std::panic::resume_unwind(payload);
            }
            panic!("multi-statement reopen test disconnected without result");
        }
    }
}

simulation_test!(init_creates_active_version_live_table, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.initialize().await.unwrap();

    let result = engine
        .execute(
            "SELECT 1 FROM lix_internal_live_v1_lix_active_version WHERE untracked = true LIMIT 1",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(result.statements[0].rows.clone());
});

simulation_test!(
    init_does_not_seed_runtime_active_version_rows,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        let result = engine
        .execute(
            "SELECT COUNT(*) FROM lix_internal_live_v1_lix_active_version WHERE untracked = true",
            &[],
        )
        .await
        .unwrap();

        assert_eq!(
            i64_value(&result.statements[0].rows[0][0], "active_version_row_count"),
            0
        );
    }
);

simulation_test!(
    init_does_not_seed_runtime_active_account_rows,
    |sim| async move {
        let mut boot_args = support::simulation_test::SimulationBootArgs::default();
        boot_args.active_account = Some(BootAccount {
            id: "account-bootstrap".to_string(),
            name: "Bootstrap Account".to_string(),
        });
        let engine = sim
            .boot_simulated_engine(Some(boot_args))
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        let result = engine
        .execute(
            "SELECT COUNT(*) FROM lix_internal_live_v1_lix_active_account WHERE untracked = true",
            &[],
        )
        .await
        .unwrap();

        assert_eq!(
            i64_value(&result.statements[0].rows[0][0], "active_account_row_count"),
            0
        );
    }
);

simulation_test!(init_creates_snapshot_table, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.initialize().await.unwrap();

    let result = engine
        .execute("SELECT 1 FROM lix_internal_snapshot LIMIT 1", &[])
        .await
        .unwrap();

    sim.assert_deterministic(result.statements[0].rows.clone());
});

simulation_test!(init_creates_change_table, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.initialize().await.unwrap();

    let result = engine
        .execute("SELECT 1 FROM lix_internal_change LIMIT 1", &[])
        .await
        .unwrap();

    sim.assert_deterministic(result.statements[0].rows.clone());
});

simulation_test!(init_inserts_no_content_snapshot, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.initialize().await.unwrap();

    let result = engine
        .execute(
            "SELECT content FROM lix_internal_snapshot WHERE id = 'no-content'",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(result.statements[0].rows.clone());
    assert_eq!(result.statements[0].rows.len(), 1);
    assert_eq!(result.statements[0].rows[0][0], lix_engine::Value::Null);
});

simulation_test!(
    init_creates_key_value_materialized_table,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "SELECT 1 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(result.statements[0].rows.clone());
    }
);

simulation_test!(init_seeds_key_value_schema_definition, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.initialize().await.unwrap();

    let result = engine
        .execute(
            "SELECT entity_id, snapshot_content \
             FROM lix_state_by_version \
             WHERE entity_id = 'lix_key_value~1' \
               AND schema_key = 'lix_registered_schema' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
             LIMIT 1",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(result.statements[0].rows.clone());
    assert_eq!(result.statements[0].rows.len(), 1);
    assert_eq!(
        result.statements[0].rows[0][0],
        lix_engine::Value::Text("lix_key_value~1".to_string())
    );

    let snapshot_content = match &result.statements[0].rows[0][1] {
        lix_engine::Value::Text(value) => value,
        other => panic!("expected text snapshot_content, got {other:?}"),
    };
    let parsed: serde_json::Value = serde_json::from_str(snapshot_content).unwrap();
    assert_eq!(parsed["value"]["x-lix-key"], "lix_key_value");
    assert_eq!(parsed["value"]["x-lix-version"], "1");
});

simulation_test!(init_seeds_builtin_schema_definitions, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.initialize().await.unwrap();

    let result = engine
        .execute(
            "SELECT entity_id, snapshot_content \
             FROM lix_state_by_version \
             WHERE entity_id IN (\
               'lix_registered_schema~1', \
               'lix_key_value~1', \
               'lix_change~1', \
               'lix_change_author~1', \
               'lix_change_set~1', \
               'lix_commit~1', \
               'lix_version_ref~1', \
               'lix_change_set_element~1', \
               'lix_commit_edge~1'\
             ) \
               AND schema_key = 'lix_registered_schema' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
             ORDER BY entity_id",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(result.statements[0].rows.clone());
    assert_eq!(result.statements[0].rows.len(), 9);

    let mut seen_schema_keys = BTreeSet::new();
    for row in &result.statements[0].rows {
        let entity_id = match &row[0] {
            lix_engine::Value::Text(value) => value.clone(),
            other => panic!("expected text entity_id, got {other:?}"),
        };
        let snapshot_content = match &row[1] {
            lix_engine::Value::Text(value) => value.clone(),
            other => panic!("expected text snapshot_content, got {other:?}"),
        };
        let parsed: serde_json::Value = serde_json::from_str(&snapshot_content).unwrap();
        let schema = parsed
            .get("value")
            .expect("registered schema snapshot_content must include value");
        let schema_key = schema
            .get("x-lix-key")
            .and_then(serde_json::Value::as_str)
            .expect("schema must include x-lix-key");
        let schema_version = schema
            .get("x-lix-version")
            .and_then(serde_json::Value::as_str)
            .expect("schema must include x-lix-version");
        let plugin_key_override = schema
            .get("x-lix-override-lixcols")
            .and_then(serde_json::Value::as_object)
            .and_then(|overrides| overrides.get("lixcol_plugin_key"))
            .and_then(serde_json::Value::as_str)
            .expect("schema must include lixcol_plugin_key override");

        assert_eq!(schema_version, "1");
        assert_eq!(plugin_key_override, "\"lix\"");
        assert_eq!(entity_id, format!("{schema_key}~{schema_version}"));
        seen_schema_keys.insert(schema_key.to_string());
    }

    assert_eq!(
        seen_schema_keys,
        BTreeSet::from([
            "lix_change".to_string(),
            "lix_change_author".to_string(),
            "lix_change_set".to_string(),
            "lix_change_set_element".to_string(),
            "lix_commit".to_string(),
            "lix_commit_edge".to_string(),
            "lix_key_value".to_string(),
            "lix_registered_schema".to_string(),
            "lix_version_ref".to_string(),
        ])
    );
});

simulation_test!(
    init_seeds_bootstrap_change_set_for_hidden_global_version_commit,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        let commit_id = global_version_commit_id(&engine).await;

        let change_set_result = engine
            .execute(
                "SELECT change_set_id \
             FROM lix_commit \
             WHERE id = $1 \
             LIMIT 1",
                &[lix_engine::Value::Text(commit_id)],
            )
            .await
            .unwrap();
        assert_eq!(change_set_result.statements[0].rows.len(), 1);
        let change_set_id = match &change_set_result.statements[0].rows[0][0] {
            lix_engine::Value::Text(value) => value.clone(),
            other => panic!("expected text change_set_id for commit, got {other:?}"),
        };
        assert_uuid_v7_like(&change_set_id, "bootstrap change_set_id");

        let change_set_exists = engine
            .execute(
                "SELECT 1 \
             FROM lix_change_set \
             WHERE id = $1 \
             LIMIT 1",
                &[lix_engine::Value::Text(change_set_id.clone())],
            )
            .await
            .unwrap();
        sim.assert_deterministic(change_set_exists.statements[0].rows.clone());
        assert_eq!(change_set_exists.statements[0].rows.len(), 1);
    }
);

simulation_test!(
    init_seeds_bootstrap_commit_and_change_set_with_uuid_ids,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");

        engine.initialize().await.unwrap();

        let commit_id = global_version_commit_id(&engine).await;
        sim.assert_deterministic(commit_id.clone());
        assert_uuid_v7_like(&commit_id, "bootstrap commit id");
        assert_ne!(
            commit_id, "00000000-0000-7000-8000-000000000002",
            "bootstrap commit id must not use the old sentinel"
        );

        let change_set_result = engine
            .execute(
                "SELECT change_set_id \
                 FROM lix_commit \
                 WHERE id = $1 \
                 LIMIT 1",
                &[lix_engine::Value::Text(commit_id.clone())],
            )
            .await
            .unwrap();
        sim.assert_deterministic(change_set_result.statements[0].rows.clone());
        assert_eq!(change_set_result.statements[0].rows.len(), 1);
        let change_set_id =
            text_value(&change_set_result.statements[0].rows[0][0], "change_set_id");
        assert_uuid_v7_like(&change_set_id, "bootstrap change_set_id");
        assert_ne!(
            change_set_id, "00000000-0000-7000-8000-000000000001",
            "bootstrap change set id must not use the old sentinel"
        );
    }
);

simulation_test!(
    init_seeds_main_version_and_global_checkpoint_pointers,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");

        engine.initialize().await.unwrap();

        let main_version_id = active_version_id(&engine).await;
        assert_ne!(main_version_id, "global");

        let main_version = engine
            .execute(
                "SELECT id, commit_id \
                 FROM lix_version \
                 WHERE id = $1 \
                 LIMIT 1",
                &[lix_engine::Value::Text(main_version_id.clone())],
            )
            .await
            .unwrap();
        sim.assert_deterministic(main_version.statements[0].rows.clone());
        assert_eq!(
            main_version.statements[0].rows.len(),
            1,
            "expected exactly one public main version row"
        );
        let main_commit_id = text_value(&main_version.statements[0].rows[0][1], "commit_id");
        let global_commit_id = global_version_commit_id(&engine).await;

        let baselines = engine
            .execute(
                "SELECT version_id, checkpoint_commit_id \
                 FROM lix_internal_last_checkpoint \
                 WHERE version_id IN ('global', $1) \
                 ORDER BY version_id",
                &[lix_engine::Value::Text(main_version_id.clone())],
            )
            .await
            .unwrap();
        sim.assert_deterministic(baselines.statements[0].rows.clone());
        assert_eq!(
            baselines.statements[0].rows.len(),
            2,
            "expected baseline pointer rows for global + main"
        );

        let version_records = [
            (main_version_id.clone(), main_commit_id),
            ("global".to_string(), global_commit_id),
        ];
        for (version_id, commit_id) in version_records {
            assert!(
                !commit_id.is_empty(),
                "version '{version_id}' must have commit_id"
            );

            let baseline_commit_id = baselines.statements[0]
                .rows
                .iter()
                .find_map(|row| {
                    if text_value(&row[0], "version_id") == version_id {
                        Some(text_value(&row[1], "checkpoint_commit_id"))
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| panic!("missing baseline pointer for version '{version_id}'"));

            assert_eq!(
                baseline_commit_id, commit_id,
                "seeded baseline must point to seeded tip commit for version '{version_id}'"
            );

            let commit_exists = engine
                .execute(
                    "SELECT COUNT(*) \
                     FROM lix_commit \
                     WHERE id = $1",
                    &[lix_engine::Value::Text(commit_id.clone())],
                )
                .await
                .unwrap();
            assert_eq!(
                i64_value(&commit_exists.statements[0].rows[0][0], "commit_count"),
                1,
                "commit '{commit_id}' must exist exactly once"
            );
        }

        let main_authoritative_tip = engine
            .execute(
                "SELECT lix_json_extract(snapshot_content, 'commit_id') AS commit_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_version_ref' \
                   AND entity_id = $1 \
                   AND untracked = true \
                   AND snapshot_content IS NOT NULL \
                 ORDER BY updated_at DESC, created_at DESC \
                 LIMIT 1",
                &[lix_engine::Value::Text(main_version_id.clone())],
            )
            .await
            .unwrap();
        assert_eq!(main_authoritative_tip.statements[0].rows.len(), 1);
        assert_eq!(
            text_value(
                &main_authoritative_tip.statements[0].rows[0][0],
                "commit_id"
            ),
            baselines.statements[0]
                .rows
                .iter()
                .find_map(|row| {
                    if text_value(&row[0], "version_id") == main_version_id {
                        Some(text_value(&row[1], "checkpoint_commit_id"))
                    } else {
                        None
                    }
                })
                .expect("main version checkpoint must exist"),
        );

        let global_authoritative_tip = engine
            .execute(
                "SELECT lix_json_extract(snapshot_content, 'commit_id') AS commit_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_version_ref' \
                   AND entity_id = 'global' \
                   AND untracked = true \
                   AND snapshot_content IS NOT NULL \
                 ORDER BY updated_at DESC, created_at DESC \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(global_authoritative_tip.statements[0].rows.len(), 1);
        assert_eq!(
            text_value(
                &global_authoritative_tip.statements[0].rows[0][0],
                "commit_id"
            ),
            baselines.statements[0]
                .rows
                .iter()
                .find_map(|row| {
                    if text_value(&row[0], "version_id") == "global" {
                        Some(text_value(&row[1], "checkpoint_commit_id"))
                    } else {
                        None
                    }
                })
                .expect("global checkpoint must exist"),
        );

        let second_init_err = engine
            .initialize()
            .await
            .expect_err("second init should return already initialized");
        assert_eq!(second_init_err.code, "LIX_ERROR_ALREADY_INITIALIZED");
    }
);

simulation_test!(
    init_second_call_returns_already_initialized,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();

        let err = engine
            .initialize()
            .await
            .expect_err("second init should return already initialized");
        assert_eq!(err.code, "LIX_ERROR_ALREADY_INITIALIZED");
    }
);

simulation_test!(
    init_seeds_checkpoint_label_in_global_lane,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "SELECT entity_id, snapshot_content \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_label' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND snapshot_content IS NOT NULL",
                &[],
            )
            .await
            .unwrap();

        let mut has_checkpoint = false;
        for row in &result.statements[0].rows {
            let row_entity_id = match &row[0] {
                lix_engine::Value::Text(value) => value.clone(),
                other => panic!("expected text entity_id for lix_label, got {other:?}"),
            };
            let snapshot_content = match &row[1] {
                lix_engine::Value::Text(value) => value,
                other => panic!("expected text snapshot_content for lix_label, got {other:?}"),
            };
            let parsed: serde_json::Value =
                serde_json::from_str(snapshot_content).expect("lix_label snapshot must be JSON");
            if parsed.get("name").and_then(serde_json::Value::as_str) == Some("checkpoint") {
                has_checkpoint = true;
                let label_id = parsed
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(row_entity_id.as_str())
                    .to_string();
                assert_eq!(label_id, CHECKPOINT_LABEL_ID);
                break;
            }
        }

        assert!(has_checkpoint, "expected checkpoint label in global lane");
        let global_commit_id = global_version_commit_id(&engine).await;

        let checkpoint_links = engine
            .execute(
                "SELECT entity_id \
                 FROM lix_entity_label \
                 WHERE entity_id = $1 \
                   AND schema_key = 'lix_commit' \
                   AND file_id = 'lix' \
                   AND label_id = $2",
                &[
                    lix_engine::Value::Text(global_commit_id),
                    lix_engine::Value::Text(CHECKPOINT_LABEL_ID.to_string()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            checkpoint_links.statements[0].rows.len(),
            1,
            "expected exactly one checkpoint label link for bootstrap commit"
        );

        let tracked = engine
            .execute(
                "SELECT lixcol_untracked \
                 FROM lix_label \
                 WHERE id = $1 \
                 LIMIT 1",
                &[lix_engine::Value::Text(CHECKPOINT_LABEL_ID.to_string())],
            )
            .await
            .unwrap();
        assert_eq!(tracked.statements[0].rows.len(), 1);
        assert_eq!(
            tracked.statements[0].rows[0][0],
            lix_engine::Value::Boolean(false)
        );
    }
);

simulation_test!(init_seeds_global_system_directories, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.initialize().await.unwrap();

    let result = engine
        .execute(
            "SELECT path, hidden, lixcol_untracked \
                 FROM lix_directory \
                 WHERE path IN ('/.lix/', '/.lix/app_data/', '/.lix/plugins/') \
                 ORDER BY path",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(result.statements[0].rows.clone());
    assert_eq!(result.statements[0].rows.len(), 3);
    assert_eq!(
        result.statements[0].rows[0][0],
        lix_engine::Value::Text("/.lix/".to_string())
    );
    let root_hidden = match &result.statements[0].rows[0][1] {
        lix_engine::Value::Boolean(value) => *value,
        lix_engine::Value::Text(value) => value == "true",
        _ => false,
    };
    assert!(
        root_hidden,
        "expected hidden=true for /.lix/, got {:?}",
        result.statements[0].rows[0][1]
    );
    assert_eq!(
        result.statements[0].rows[0][2],
        lix_engine::Value::Boolean(false)
    );
    assert_eq!(
        result.statements[0].rows[1][0],
        lix_engine::Value::Text("/.lix/app_data/".to_string())
    );
    let app_data_hidden = match &result.statements[0].rows[1][1] {
        lix_engine::Value::Boolean(value) => *value,
        lix_engine::Value::Text(value) => value == "true",
        _ => false,
    };
    assert!(
        app_data_hidden,
        "expected hidden=true for /.lix/app_data/, got {:?}",
        result.statements[0].rows[1][1]
    );
    assert_eq!(
        result.statements[0].rows[1][2],
        lix_engine::Value::Boolean(false)
    );
    assert_eq!(
        result.statements[0].rows[2][0],
        lix_engine::Value::Text("/.lix/plugins/".to_string())
    );
    let plugins_hidden = match &result.statements[0].rows[2][1] {
        lix_engine::Value::Boolean(value) => *value,
        lix_engine::Value::Text(value) => value == "true",
        _ => false,
    };
    assert!(
        plugins_hidden,
        "expected hidden=true for /.lix/plugins/, got {:?}",
        result.statements[0].rows[2][1]
    );
    assert_eq!(
        result.statements[0].rows[2][2],
        lix_engine::Value::Boolean(false)
    );
});

simulation_test!(
    init_seeds_global_system_directories_with_uuid_ids,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "SELECT id, path \
                 FROM lix_directory \
                 WHERE path IN ('/.lix/', '/.lix/app_data/', '/.lix/plugins/') \
                 ORDER BY path",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(result.statements[0].rows.len(), 3);
        for row in &result.statements[0].rows {
            let lix_engine::Value::Text(id) = &row[0] else {
                panic!(
                    "expected text id for seeded system directory, got {:?}",
                    row[0]
                );
            };
            assert!(
                !id.starts_with("dir:auto::"),
                "expected seeded system directory id to be uuid-based, got {id}"
            );
            assert_eq!(
                id.len(),
                36,
                "expected seeded system directory id to look like a UUID, got {id}"
            );
        }
    }
);
