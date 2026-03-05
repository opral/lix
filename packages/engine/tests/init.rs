mod support;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lix_engine::{boot, BootArgs, NoopWasmRuntime};

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

async fn global_pointer_commit_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let result = engine
        .execute(
            "SELECT lix_json_extract(snapshot_content, 'commit_id') AS commit_id \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_global_pointer' \
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
        "lix_global_pointer.commit_id",
    )
}

fn boot_sqlite_engine_at_path(path: &Path) -> lix_engine::Engine {
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
    boot(args)
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
                        engine.init().await.map_err(|err| err.to_string())
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
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            let run_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build tokio runtime");
                runtime.block_on(async move {
                    let engine_a = boot_sqlite_engine_at_path(&path_for_thread);
                    engine_a.init().await.expect("first init should succeed");

                    engine_a
                        .execute(
                            "INSERT INTO lix_file (path, data, metadata) \
                             VALUES ('/wc-reopen.md', lix_text_encode('hello'), NULL)", &[])
                        .await
                        .expect("file insert should succeed");

                    let file_result = engine_a
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

                    let before = engine_a
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
                        .init()
                        .await
                        .expect_err("reopen init should report already initialized");
                    assert_eq!(reopen_init_err.code, "LIX_ERROR_ALREADY_INITIALIZED");
                    engine_b
                        .open()
                        .await
                        .expect("reopen open should load active version state");

                    let after = engine_b
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

                    let tip_result = engine_b
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
                    let tip_commit_id = text_value(&tip_result.rows[0][0], "commit_id");

                    let after_rows = engine_b
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
                        "reopen changed working changes count (before={before_count}, after={after_count}, tip={tip_commit_id})"
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

simulation_test!(init_creates_untracked_table, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    let result = engine
        .execute("SELECT 1 FROM lix_internal_state_untracked LIMIT 1", &[])
        .await
        .unwrap();

    sim.assert_deterministic(result.statements[0].rows.clone());
});

simulation_test!(init_creates_snapshot_table, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

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

    engine.init().await.unwrap();

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

    engine.init().await.unwrap();

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

        engine.init().await.unwrap();

        let result = engine
            .execute(
                "SELECT 1 FROM lix_internal_state_vtable \
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

    engine.init().await.unwrap();

    let result = engine
        .execute(
            "SELECT entity_id, snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE entity_id = 'lix_key_value~1' \
               AND schema_key = 'lix_stored_schema' \
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

    engine.init().await.unwrap();

    let result = engine
        .execute(
            "SELECT entity_id, snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE entity_id IN (\
               'lix_stored_schema~1', \
               'lix_key_value~1', \
               'lix_change~1', \
               'lix_change_author~1', \
               'lix_change_set~1', \
               'lix_commit~1', \
               'lix_version_pointer~1', \
               'lix_change_set_element~1', \
               'lix_commit_edge~1'\
             ) \
               AND schema_key = 'lix_stored_schema' \
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
            .expect("stored schema snapshot_content must include value");
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
            "lix_stored_schema".to_string(),
            "lix_version_pointer".to_string(),
        ])
    );
});

simulation_test!(
    init_seeds_bootstrap_change_set_for_bootstrap_global_pointer_commit,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        let commit_id = global_pointer_commit_id(&engine).await;

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
        sim.assert_deterministic(change_set_result.statements[0].rows.clone());
        assert_eq!(change_set_result.statements[0].rows.len(), 1);
        let change_set_id = match &change_set_result.statements[0].rows[0][0] {
            lix_engine::Value::Text(value) => value.clone(),
            other => panic!("expected text change_set_id for commit, got {other:?}"),
        };

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
    init_seeds_main_version_and_global_checkpoint_pointers,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");

        engine.init().await.unwrap();

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
        let global_commit_id = global_pointer_commit_id(&engine).await;

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

        let second_init_err = engine
            .init()
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
        engine.init().await.unwrap();

        let err = engine
            .init()
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

        engine.init().await.unwrap();

        let result = engine
            .execute(
                "SELECT entity_id, snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_label' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND snapshot_content IS NOT NULL",
                &[],
            )
            .await
            .unwrap();

        let mut has_checkpoint = false;
        let mut checkpoint_label_id: Option<String> = None;
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
                checkpoint_label_id = Some(label_id);
                break;
            }
        }

        assert!(has_checkpoint, "expected checkpoint label in global lane");
        let checkpoint_label_id =
            checkpoint_label_id.expect("checkpoint label id should be present");
        let global_commit_id = global_pointer_commit_id(&engine).await;

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
                    lix_engine::Value::Text(checkpoint_label_id),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            checkpoint_links.statements[0].rows.len(),
            1,
            "expected exactly one checkpoint label link for bootstrap commit"
        );
    }
);

simulation_test!(init_seeds_global_system_directories, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();
    let active_version_id = active_version_id(&engine).await;

    let result = engine
        .execute(
            "SELECT path, hidden \
                 FROM lix_directory_by_version \
                 WHERE lixcol_version_id = $1 \
                   AND lixcol_global = true \
                   AND path IN ('/.lix/', '/.lix/app_data/', '/.lix/plugins/') \
                 ORDER BY path",
            &[lix_engine::Value::Text(active_version_id)],
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

    let active_result = engine
        .execute(
            "SELECT path, hidden \
                 FROM lix_directory \
                 WHERE path IN ('/.lix/', '/.lix/app_data/', '/.lix/plugins/') \
                 ORDER BY path",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(active_result.statements[0].rows.clone());
    assert_eq!(active_result.statements[0].rows.len(), 3);
    assert_eq!(
        active_result.statements[0].rows[0][0],
        lix_engine::Value::Text("/.lix/".to_string())
    );
    let active_root_hidden = match &active_result.statements[0].rows[0][1] {
        lix_engine::Value::Boolean(value) => *value,
        lix_engine::Value::Text(value) => value == "true",
        _ => false,
    };
    assert!(
        active_root_hidden,
        "expected hidden=true for /.lix/ in lix_directory, got {:?}",
        active_result.statements[0].rows[0][1]
    );
    assert_eq!(
        active_result.statements[0].rows[1][0],
        lix_engine::Value::Text("/.lix/app_data/".to_string())
    );
    let active_app_data_hidden = match &active_result.statements[0].rows[1][1] {
        lix_engine::Value::Boolean(value) => *value,
        lix_engine::Value::Text(value) => value == "true",
        _ => false,
    };
    assert!(
        active_app_data_hidden,
        "expected hidden=true for /.lix/app_data/ in lix_directory, got {:?}",
        active_result.statements[0].rows[1][1]
    );
    assert_eq!(
        active_result.statements[0].rows[2][0],
        lix_engine::Value::Text("/.lix/plugins/".to_string())
    );
    let active_plugins_hidden = match &active_result.statements[0].rows[2][1] {
        lix_engine::Value::Boolean(value) => *value,
        lix_engine::Value::Text(value) => value == "true",
        _ => false,
    };
    assert!(
        active_plugins_hidden,
        "expected hidden=true for /.lix/plugins/ in lix_directory, got {:?}",
        active_result.statements[0].rows[2][1]
    );
});
