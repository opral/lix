mod support;

use lix_engine::{boot, BootArgs, Engine, ExecuteOptions, NoopWasmRuntime};
use lix_engine::{ObserveQuery, Value};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn insert_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "INSERT INTO lix_internal_state_vtable (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{key}', 'lix_key_value', 'lix', 'global', 'lix', '{{\"key\":\"{key}\",\"value\":{value_json}}}', '1'\
         )"
    )
}

fn update_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "UPDATE lix_internal_state_vtable \
         SET snapshot_content = '{{\"key\":\"{key}\",\"value\":{value_json}}}' \
         WHERE entity_id = '{key}' \
           AND schema_key = 'lix_key_value' \
           AND file_id = 'lix' \
           AND version_id = 'global'"
    )
}

simulation_test!(
    observe_emits_initial_and_followup_rows,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let mut observed = engine
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' AND entity_id = ?1",
                vec![Value::Text("observe-key".to_string())],
            ))
            .expect("observe should succeed");

        let initial = observed
            .next()
            .await
            .expect("initial observe poll should succeed")
            .expect("initial observe event should exist");
        assert_eq!(initial.sequence, 0);
        assert!(initial.rows.rows.is_empty());
        assert_eq!(initial.state_commit_sequence, None);

        engine
            .execute(&insert_key_value_sql("observe-key", "\"v0\""), &[])
            .await
            .unwrap();

        let update = observed
            .next()
            .await
            .expect("follow-up observe poll should succeed")
            .expect("follow-up observe event should exist");
        assert_eq!(update.sequence, 1);
        assert!(update.state_commit_sequence.is_some());
        assert_eq!(update.rows.rows.len(), 1);
        assert_eq!(
            update.rows.rows[0][0],
            Value::Text("observe-key".to_string())
        );

        observed.close();
        let closed = observed
            .next()
            .await
            .expect("observe poll after close should succeed");
        assert!(closed.is_none());
    }
);

simulation_test!(
    observe_supports_multiple_subscribers,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let mut observed_a = engine
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' AND entity_id = ?1 \
                 ORDER BY entity_id",
                vec![Value::Text("observe-multi".to_string())],
            ))
            .expect("observe should succeed");
        let mut observed_b = engine
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' AND entity_id = ?1 \
                 ORDER BY entity_id",
                vec![Value::Text("observe-multi".to_string())],
            ))
            .expect("observe should succeed");

        let initial_a = observed_a.next().await.unwrap().unwrap();
        let initial_b = observed_b.next().await.unwrap().unwrap();
        assert!(initial_a.rows.rows.is_empty());
        assert!(initial_b.rows.rows.is_empty());

        engine
            .execute(&insert_key_value_sql("observe-multi", "\"v0\""), &[])
            .await
            .unwrap();

        let update_a = observed_a.next().await.unwrap().unwrap();
        let update_b = observed_b.next().await.unwrap().unwrap();

        assert_eq!(update_a.sequence, 1);
        assert_eq!(update_b.sequence, 1);
        assert_eq!(update_a.rows.rows.len(), 1);
        assert_eq!(update_b.rows.rows.len(), 1);
        assert_eq!(
            update_a.rows.rows[0][0],
            Value::Text("observe-multi".to_string())
        );
        assert_eq!(
            update_b.rows.rows[0][0],
            Value::Text("observe-multi".to_string())
        );
    }
);

simulation_test!(
    observe_skips_unrelated_commits_until_result_changes,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let mut observed = engine
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' AND entity_id = ?1 \
                 ORDER BY entity_id",
                vec![Value::Text("observe-target".to_string())],
            ))
            .expect("observe should succeed");

        let initial = observed.next().await.unwrap().unwrap();
        assert_eq!(initial.sequence, 0);
        assert!(initial.rows.rows.is_empty());

        engine
            .execute(&insert_key_value_sql("observe-unrelated", "\"v0\""), &[])
            .await
            .unwrap();
        engine
            .execute(&insert_key_value_sql("observe-target", "\"v1\""), &[])
            .await
            .unwrap();

        let update = observed.next().await.unwrap().unwrap();
        assert_eq!(update.sequence, 1);
        assert_eq!(update.rows.rows.len(), 1);
        assert_eq!(
            update.rows.rows[0][0],
            Value::Text("observe-target".to_string())
        );
    }
);

simulation_test!(
    observe_dedups_noop_result_reexecutions,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(&insert_key_value_sql("observe-dedup", "\"v0\""), &[])
            .await
            .unwrap();

        let mut observed = engine
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' \
                 ORDER BY entity_id",
                vec![],
            ))
            .expect("observe should succeed");

        let initial = observed.next().await.unwrap().unwrap();
        assert_eq!(initial.sequence, 0);
        assert_eq!(initial.rows.rows.len(), 1);

        engine
            .execute(&update_key_value_sql("observe-dedup", "\"v1\""), &[])
            .await
            .unwrap();
        engine
            .execute(&insert_key_value_sql("observe-dedup-2", "\"v0\""), &[])
            .await
            .unwrap();

        let next = observed.next().await.unwrap().unwrap();
        assert_eq!(next.sequence, 1);
        assert_eq!(next.rows.rows.len(), 2);
        assert_eq!(
            next.rows.rows[0][0],
            Value::Text("observe-dedup".to_string())
        );
        assert_eq!(
            next.rows.rows[1][0],
            Value::Text("observe-dedup-2".to_string())
        );
    }
);

simulation_test!(
    observe_preserves_commit_order_across_multiple_updates,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let mut observed = engine
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' \
                 ORDER BY entity_id",
                vec![],
            ))
            .expect("observe should succeed");

        let initial = observed.next().await.unwrap().unwrap();
        assert_eq!(initial.sequence, 0);
        assert!(initial.rows.rows.is_empty());

        engine
            .execute(&insert_key_value_sql("observe-order-a", "\"v0\""), &[])
            .await
            .unwrap();

        let first = observed.next().await.unwrap().unwrap();
        assert_eq!(first.sequence, 1);
        assert_eq!(first.rows.rows.len(), 1);

        engine
            .execute(&insert_key_value_sql("observe-order-b", "\"v0\""), &[])
            .await
            .unwrap();

        let second = observed.next().await.unwrap().unwrap();
        assert_eq!(second.sequence, 2);
        assert_eq!(second.rows.rows.len(), 2);
        assert!(
            first
                .state_commit_sequence
                .zip(second.state_commit_sequence)
                .is_some_and(|(left, right)| right > left),
            "expected monotonic state commit sequence order"
        );
    }
);

simulation_test!(
    observe_rejects_non_query_sql,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let result = engine.observe(ObserveQuery::new(
            "UPDATE lix_state SET schema_version = '1' WHERE 1 = 0",
            vec![],
        ));

        let error = match result {
            Ok(_) => panic!("observe should reject non-query SQL"),
            Err(error) => error,
        };
        assert!(
            error
                .message
                .contains("observe requires one or more SELECT statements"),
            "unexpected error message: {}",
            error.message
        );
    }
);

#[test]
fn observe_sqlite_detects_external_insert_without_local_commit_stream_event() {
    run_local_observe_sqlite_case(
        "observe_sqlite_detects_external_insert_without_local_commit_stream_event",
        || async {
            let path = temp_sqlite_observe_path("external-insert");

            let engine_a = boot_sqlite_engine_at_path(path.clone());
            let engine_b = boot_sqlite_engine_at_path(path.clone());

            engine_a.init().await.expect("engine_a init should succeed");
            engine_b.init().await.expect("engine_b init should succeed");

            let mut observed = engine_a
                .observe(ObserveQuery::new(
                    "SELECT path FROM lix_file WHERE path = '/observe-external.md'",
                    vec![],
                ))
                .expect("observe should succeed");

            let initial = observed
                .next()
                .await
                .expect("initial observe next should succeed")
                .expect("initial observe event should exist");
            assert!(initial.rows.rows.is_empty());

            engine_b
                .execute(
                    "INSERT INTO lix_file (path, data) VALUES ('/observe-external.md', lix_text_encode('hello'))",
                    &[],
                    ExecuteOptions::default(),
                )
                .await
                .expect("external insert should succeed");

            let update = tokio::time::timeout(Duration::from_secs(2), observed.next())
                .await
                .expect("observe next should not time out")
                .expect("observe next should succeed")
                .expect("observe update event should exist");
            assert_eq!(update.rows.rows.len(), 1);
            assert_eq!(
                update.rows.rows[0][0],
                Value::Text("/observe-external.md".to_string())
            );

            observed.close();
            drop(observed);
            drop(engine_b);
            drop(engine_a);
            cleanup_sqlite_path(&path);
        },
    );
}

#[test]
fn observe_sqlite_detects_external_untracked_state_insert() {
    run_local_observe_sqlite_case(
        "observe_sqlite_detects_external_untracked_state_insert",
        || async {
            let path = temp_sqlite_observe_path("external-untracked");

            let engine_a = boot_sqlite_engine_at_path(path.clone());
            let engine_b = boot_sqlite_engine_at_path(path.clone());

            engine_a.init().await.expect("engine_a init should succeed");
            engine_b.init().await.expect("engine_b init should succeed");

            let mut observed = engine_a
                .observe(ObserveQuery::new(
                    "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'observe-untracked-external' \
                   AND untracked = true",
                    vec![],
                ))
                .expect("observe should succeed");

            let initial = observed
                .next()
                .await
                .expect("initial observe next should succeed")
                .expect("initial observe event should exist");
            assert!(initial.rows.rows.is_empty());

            engine_b
                .execute(
                    "INSERT INTO lix_state (\
                 entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content, untracked\
                 ) VALUES (\
                 'observe-untracked-external', 'lix', 'lix_key_value', 'lix', '1', \
                 lix_json('{\"key\":\"observe-untracked-external\",\"value\":\"u1\"}'), true\
                 )",
                    &[],
                    ExecuteOptions::default(),
                )
                .await
                .expect("external untracked insert should succeed");

            let update = tokio::time::timeout(Duration::from_secs(2), observed.next())
                .await
                .expect("observe next should not time out")
                .expect("observe next should succeed")
                .expect("observe update event should exist");
            assert_eq!(update.rows.rows.len(), 1);
            assert_eq!(
                update.rows.rows[0][0],
                Value::Text("observe-untracked-external".to_string())
            );

            observed.close();
            drop(observed);
            drop(engine_b);
            drop(engine_a);
            cleanup_sqlite_path(&path);
        },
    );
}

fn boot_sqlite_engine_at_path(path: PathBuf) -> Engine {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("sqlite test parent directory should be creatable");
    }
    let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");
    boot(BootArgs::new(
        support::simulations::sqlite_backend_with_filename(format!(
            "sqlite://{}",
            path.to_string_lossy()
        )),
        Arc::new(NoopWasmRuntime),
    ))
}

fn temp_sqlite_observe_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lix-observe-{label}-{}-{nanos}.sqlite",
        std::process::id()
    ))
}

fn cleanup_sqlite_path(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let wal = PathBuf::from(format!("{}-wal", path.to_string_lossy()));
    let shm = PathBuf::from(format!("{}-shm", path.to_string_lossy()));
    let journal = PathBuf::from(format!("{}-journal", path.to_string_lossy()));
    let _ = std::fs::remove_file(wal);
    let _ = std::fs::remove_file(shm);
    let _ = std::fs::remove_file(journal);
}

fn run_local_observe_sqlite_case<F, Fut>(name: &'static str, case: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            let run_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build tokio runtime");
                runtime.block_on(case());
            }));
            let _ = result_tx.send(run_result);
        })
        .expect("failed to spawn observe sqlite test thread");

    match result_rx.recv_timeout(Duration::from_secs(120)) {
        Ok(Ok(())) => {
            thread.join().expect("observe sqlite test thread panicked");
        }
        Ok(Err(payload)) => {
            let _ = thread.join();
            std::panic::resume_unwind(payload);
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("observe sqlite case timed out after 120s: {name}");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            if let Err(payload) = thread.join() {
                std::panic::resume_unwind(payload);
            }
            panic!("observe sqlite case disconnected without result: {name}");
        }
    }
}
