#[path = "support/mod.rs"]
mod support;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lix_engine::{boot, BootArgs, NoopWasmRuntime, Value};

fn text_value(value: &Value, field: &str) -> String {
    match value {
        Value::Text(value) => value.clone(),
        other => panic!("expected text value for {field}, got {other:?}"),
    }
}

fn i64_value(value: &Value, field: &str) -> i64 {
    match value {
        Value::Integer(value) => *value,
        Value::Text(value) => value.parse::<i64>().unwrap_or_else(|error| {
            panic!("expected i64 text for {field}, got '{value}': {error}")
        }),
        other => panic!("expected i64 value for {field}, got {other:?}"),
    }
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

fn temp_sqlite_cutover_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lix-cutover-{label}-{}-{nanos}.sqlite",
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
fn reopen_from_canonical_journal_without_commit_family_live_mirrors_sqlite() {
    let path = temp_sqlite_cutover_path("reopen-without-live-mirrors");
    let path_for_thread = path.clone();
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name("reopen_from_canonical_journal_without_commit_family_live_mirrors_sqlite".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let run_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build tokio runtime");
                runtime.block_on(async move {
                    let engine_a = boot_sqlite_engine_at_path(&path_for_thread);
                    engine_a.initialize().await.expect("init should succeed");
                    let session_a = engine_a
                        .open_workspace_session()
                        .await
                        .expect("workspace session should open");

                    session_a
                        .execute(
                            "INSERT INTO lix_key_value (key, value) VALUES ('cutover-reopen-key', 'v1')",
                            &[],
                        )
                        .await
                        .expect("seed insert should succeed");
                    session_a
                        .create_checkpoint()
                        .await
                        .expect("first checkpoint should succeed");
                    session_a
                        .execute(
                            "UPDATE lix_key_value SET value = 'v2' WHERE key = 'cutover-reopen-key'",
                            &[],
                        )
                        .await
                        .expect("update should succeed");
                    session_a
                        .create_checkpoint()
                        .await
                        .expect("second checkpoint should succeed");

                    let version_before = session_a
                        .execute(
                            "SELECT id, commit_id \
                             FROM lix_version \
                             WHERE name = 'main' \
                             LIMIT 1",
                            &[],
                        )
                        .await
                        .expect("main version query should succeed");
                    assert_eq!(version_before.statements[0].rows.len(), 1);
                    let version_id = text_value(&version_before.statements[0].rows[0][0], "version_id");
                    let commit_id_before =
                        text_value(&version_before.statements[0].rows[0][1], "commit_id");

                    for table in [
                        "lix_internal_live_v1_lix_commit",
                        "lix_internal_live_v1_lix_change_set",
                        "lix_internal_live_v1_lix_change_set_element",
                        "lix_internal_live_v1_lix_commit_edge",
                    ] {
                        session_a
                            .execute(&format!("DROP TABLE IF EXISTS {table}"), &[])
                            .await
                            .unwrap_or_else(|error| {
                                panic!("dropping '{table}' should succeed: {}", error.description)
                            });
                    }

                    drop(session_a);
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
                        .expect("open_existing should succeed without live commit-family mirrors");
                    let session_b = engine_b
                        .open_workspace_session()
                        .await
                        .expect("workspace session after reopen should open");

                    let version_after = session_b
                        .execute(
                            "SELECT id, name, commit_id \
                             FROM lix_version \
                             WHERE name = 'main' \
                             LIMIT 1",
                            &[],
                        )
                        .await
                        .expect("main version query after reopen should succeed");
                    assert_eq!(version_after.statements[0].rows.len(), 1);
                    assert_eq!(
                        text_value(&version_after.statements[0].rows[0][0], "version_id_after"),
                        version_id
                    );
                    assert_eq!(
                        text_value(&version_after.statements[0].rows[0][1], "version_name"),
                        "main"
                    );
                    assert_eq!(
                        text_value(&version_after.statements[0].rows[0][2], "commit_id_after"),
                        commit_id_before
                    );

                    let history_rows = session_b
                        .execute(
                            "SELECT value, lixcol_depth \
                             FROM lix_key_value_history \
                             WHERE key = 'cutover-reopen-key' \
                             ORDER BY lixcol_depth DESC",
                            &[],
                        )
                        .await
                        .expect("history read after reopen should succeed");
                    assert_eq!(history_rows.statements[0].rows.len(), 2);
                    assert_eq!(text_value(&history_rows.statements[0].rows[0][0], "value_oldest"), "v1");
                    assert_eq!(i64_value(&history_rows.statements[0].rows[0][1], "depth_oldest"), 1);
                    assert_eq!(text_value(&history_rows.statements[0].rows[1][0], "value_latest"), "v2");
                    assert_eq!(i64_value(&history_rows.statements[0].rows[1][1], "depth_latest"), 0);

                    let graph_count = session_b
                        .execute("SELECT COUNT(*) FROM lix_internal_commit_graph_node", &[])
                        .await
                        .expect("commit graph query after reopen should succeed");
                    assert!(
                        i64_value(&graph_count.statements[0].rows[0][0], "commit_graph_count") >= 1,
                        "expected canonical graph rows after reopen"
                    );
                });
            }));
            let _ = result_tx.send(run_result);
        })
        .expect("failed to spawn cutover reopen sqlite test thread");

    let recv_result = result_rx.recv_timeout(Duration::from_secs(120));
    cleanup_sqlite_path(&path);
    match recv_result {
        Ok(Ok(())) => {
            thread
                .join()
                .expect("cutover reopen sqlite test thread panicked");
        }
        Ok(Err(payload)) => {
            let _ = thread.join();
            std::panic::resume_unwind(payload);
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("cutover reopen sqlite test timed out after 120s");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            if let Err(payload) = thread.join() {
                std::panic::resume_unwind(payload);
            }
            panic!("cutover reopen sqlite test disconnected without result");
        }
    }
}
