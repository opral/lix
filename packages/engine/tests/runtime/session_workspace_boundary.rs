use crate::support;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use lix_engine::wasm::NoopWasmRuntime;
use lix_engine::{
    AdditionalSessionOptions, CreateVersionOptions, ExecuteOptions, Lix, LixConfig, ObserveEvents,
    ObserveEventsOwned, ObserveQuery, Value,
};

fn run_with_large_stack<F, Fut>(factory: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name("session-workspace-boundary".to_string())
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime should build")
                .block_on(Box::pin(factory()));
        })
        .expect("session workspace thread should spawn")
        .join()
        .expect("session workspace thread should not panic");
}

fn temp_sqlite_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lix-session-{label}-{}-{nanos}.sqlite",
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

fn lix_config(path: &Path) -> LixConfig {
    LixConfig::new(
        support::simulations::sqlite_backend_with_filename(format!("sqlite://{}", path.display())),
        Arc::new(NoopWasmRuntime),
    )
}

fn boot_engine(path: &Path) -> Arc<Lix> {
    Arc::new(Lix::boot(lix_config(path)))
}

fn first_text(result: &lix_engine::ExecuteResult) -> String {
    match &result.statements[0].rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected first result cell to be text, got {other:?}"),
    }
}

fn first_text_query_result(result: &lix_engine::QueryResult) -> String {
    match &result.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected first query result cell to be text, got {other:?}"),
    }
}

fn first_i64(result: &lix_engine::ExecuteResult) -> i64 {
    match &result.statements[0].rows[0][0] {
        Value::Integer(value) => *value,
        Value::Text(value) => value
            .parse()
            .unwrap_or_else(|error| panic!("expected integer-like text, got parse error: {error}")),
        other => panic!("expected first result cell to be integer-like, got {other:?}"),
    }
}

fn first_i64_query_result(result: &lix_engine::QueryResult) -> i64 {
    match &result.rows[0][0] {
        Value::Integer(value) => *value,
        Value::Text(value) => value
            .parse()
            .unwrap_or_else(|error| panic!("expected integer-like text, got parse error: {error}")),
        other => panic!("expected first query result cell to be integer-like, got {other:?}"),
    }
}

fn first_string_vec(result: &lix_engine::ExecuteResult) -> Vec<String> {
    let raw = first_text(result);
    serde_json::from_str(&raw)
        .unwrap_or_else(|error| panic!("expected first result cell to be JSON array text: {error}"))
}

async fn next_observe_count(observed: &mut ObserveEventsOwned, label: &str) -> i64 {
    let event = tokio::time::timeout(std::time::Duration::from_secs(1), observed.next())
        .await
        .unwrap_or_else(|_| panic!("{label} next should not time out"))
        .unwrap_or_else(|error| panic!("{label} next should succeed: {error:?}"))
        .unwrap_or_else(|| panic!("{label} should emit an initial event"));
    match &event.rows.rows[0][0] {
        Value::Integer(value) => *value,
        Value::Text(value) => value
            .parse()
            .unwrap_or_else(|error| panic!("expected integer-like text, got parse error: {error}")),
        other => panic!("expected observe count cell to be integer-like, got {other:?}"),
    }
}

async fn next_owned_observe_text(observed: &mut ObserveEventsOwned, label: &str) -> String {
    let event = tokio::time::timeout(std::time::Duration::from_secs(1), observed.next())
        .await
        .unwrap_or_else(|_| panic!("{label} next should not time out"))
        .unwrap_or_else(|error| panic!("{label} next should succeed: {error:?}"))
        .unwrap_or_else(|| panic!("{label} should emit an event"));
    first_text_query_result(&event.rows)
}

async fn next_session_observe_text(observed: &mut ObserveEvents<'_>, label: &str) -> String {
    let event = tokio::time::timeout(std::time::Duration::from_secs(1), observed.next())
        .await
        .unwrap_or_else(|_| panic!("{label} next should not time out"))
        .unwrap_or_else(|error| panic!("{label} next should succeed: {error:?}"))
        .unwrap_or_else(|| panic!("{label} should emit an event"));
    first_text_query_result(&event.rows)
}

async fn next_session_observe_string_vec(
    observed: &mut ObserveEvents<'_>,
    label: &str,
) -> Vec<String> {
    serde_json::from_str(&next_session_observe_text(observed, label).await)
        .unwrap_or_else(|error| panic!("{label} should return a JSON string array: {error}"))
}

async fn next_owned_observe_string_vec(
    observed: &mut ObserveEventsOwned,
    label: &str,
) -> Vec<String> {
    serde_json::from_str(&next_owned_observe_text(observed, label).await)
        .unwrap_or_else(|error| panic!("{label} should return a JSON string array: {error}"))
}

async fn assert_no_owned_observe_event(observed: &mut ObserveEventsOwned, label: &str) {
    let result = tokio::time::timeout(std::time::Duration::from_millis(400), observed.next()).await;
    assert!(result.is_err(), "{label} should not emit another event");
}

async fn workspace_metadata_value_lix(lix: &Lix, key: &str) -> Option<String> {
    let result = lix
        .execute(
            "SELECT value \
             FROM lix_internal_workspace_metadata \
             WHERE key = $1 \
             LIMIT 1",
            &[Value::Text(key.to_string())],
        )
        .await
        .expect("workspace metadata query should succeed");
    result.statements[0]
        .rows
        .first()
        .and_then(|row| row.first())
        .map(|value| match value {
            Value::Text(value) => value.clone(),
            other => panic!("expected workspace metadata text value, got {other:?}"),
        })
}

#[test]
fn booted_lix_uses_workspace_backed_root_session() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("engine-root-session");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        let engine = boot_engine(&path);
        engine.initialize().await.expect("init should succeed");

        let workspace = Arc::clone(&engine);
        let version = workspace
            .create_version(CreateVersionOptions {
                name: Some("engine-root-session".to_string()),
                ..Default::default()
            })
            .await
            .expect("create_version should succeed");
        workspace
            .switch_version(version.id.clone())
            .await
            .expect("switch_version should succeed");
        workspace
            .set_active_account_ids(vec!["acct-root".to_string()])
            .await
            .expect("set_active_account_ids should succeed");

        assert_eq!(
            workspace_metadata_value_lix(workspace.as_ref(), "active_version_id").await,
            Some(version.id.clone())
        );
        assert_eq!(
            workspace_metadata_value_lix(workspace.as_ref(), "active_account_ids").await,
            Some(r#"["acct-root"]"#.to_string())
        );

        drop(workspace);

        let reopened_engine = boot_engine(&path);
        reopened_engine
            .initialize_if_needed()
            .await
            .expect("reopen initialize_if_needed should succeed");
        let reopened = Arc::clone(&reopened_engine);

        assert_eq!(
            reopened
                .active_version_id()
                .await
                .expect("reopened active_version_id should load"),
            version.id
        );
        assert_eq!(
            reopened
                .active_account_ids()
                .await
                .expect("reopened active_account_ids should load"),
            vec!["acct-root".to_string()]
        );

        drop(reopened);
        drop(reopened_engine);
        drop(engine);
        cleanup_sqlite_path(&path);
    });
}

#[test]
fn open_additional_session_snapshots_active_version_and_isolates_switches() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("snapshot");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        Lix::init(lix_config(&path))
            .await
            .expect("init should succeed");
        let lix = Lix::open(lix_config(&path))
            .await
            .expect("open should succeed");

        let version = lix
            .create_version(CreateVersionOptions {
                name: Some("session-branch".to_string()),
                ..Default::default()
            })
            .await
            .expect("create_version should succeed");
        lix.switch_version(version.id.clone())
            .await
            .expect("switch_version should succeed");

        let worker = lix
            .open_additional_session(AdditionalSessionOptions::default())
            .await
            .expect("open_additional_session should succeed");
        worker
            .switch_version("global".to_string())
            .await
            .expect("worker switch_version should succeed");

        let workspace_active = lix
            .execute("SELECT lix_active_version_id()", &[])
            .await
            .expect("workspace active query should succeed");
        let worker_active = worker
            .execute("SELECT lix_active_version_id()", &[])
            .await
            .expect("worker active query should succeed");

        assert_eq!(first_text(&workspace_active), version.id);
        assert_eq!(first_text(&worker_active), "global");

        drop(worker);
        drop(lix);
        cleanup_sqlite_path(&path);
    });
}

#[test]
fn additional_sessions_can_operate_against_different_active_versions_at_the_same_time() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("additional-sessions-different-versions");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        Lix::init(lix_config(&path))
            .await
            .expect("init should succeed");
        let lix = Lix::open(lix_config(&path))
            .await
            .expect("open should succeed");

        let feature_version = lix
            .create_version(CreateVersionOptions {
                name: Some("feature".to_string()),
                ..Default::default()
            })
            .await
            .expect("feature create_version should succeed");
        let release_version = lix
            .create_version(CreateVersionOptions {
                name: Some("release".to_string()),
                ..Default::default()
            })
            .await
            .expect("release create_version should succeed");

        let feature = lix
            .open_additional_session(AdditionalSessionOptions {
                active_version_id: Some(feature_version.id.clone()),
                ..Default::default()
            })
            .await
            .expect("feature open_additional_session should succeed");
        let release = lix
            .open_additional_session(AdditionalSessionOptions {
                active_version_id: Some(release_version.id.clone()),
                ..Default::default()
            })
            .await
            .expect("release open_additional_session should succeed");

        feature
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('shared-branch-key', 'feature')",
                &[],
            )
            .await
            .expect("feature insert should succeed");
        release
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('shared-branch-key', 'release')",
                &[],
            )
            .await
            .expect("release insert should succeed");

        let feature_value = feature
            .execute(
                "SELECT COALESCE((SELECT value FROM lix_key_value WHERE key = 'shared-branch-key' LIMIT 1), 'missing')",
                &[],
            )
            .await
            .expect("feature value query should succeed");
        let release_value = release
            .execute(
                "SELECT COALESCE((SELECT value FROM lix_key_value WHERE key = 'shared-branch-key' LIMIT 1), 'missing')",
                &[],
            )
            .await
            .expect("release value query should succeed");
        let workspace_value = lix
            .execute(
                "SELECT COALESCE((SELECT value FROM lix_key_value WHERE key = 'shared-branch-key' LIMIT 1), 'missing')",
                &[],
            )
            .await
            .expect("workspace value query should succeed");

        assert_eq!(feature.active_version_id(), feature_version.id);
        assert_eq!(release.active_version_id(), release_version.id);
        assert_eq!(first_text(&feature_value), "feature");
        assert_eq!(first_text(&release_value), "release");
        assert_eq!(first_text(&workspace_value), "missing");

        drop(release);
        drop(feature);
        drop(lix);
        cleanup_sqlite_path(&path);
    });
}

#[test]
fn workspace_backed_lix_reopens_on_persisted_active_version() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("reopen");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        Lix::init(lix_config(&path))
            .await
            .expect("init should succeed");
        let version_id = {
            let lix = Lix::open(lix_config(&path))
                .await
                .expect("open should succeed");
            let version = lix
                .create_version(CreateVersionOptions {
                    name: Some("workspace-reopen".to_string()),
                    ..Default::default()
                })
                .await
                .expect("create_version should succeed");
            lix.switch_version(version.id.clone())
                .await
                .expect("switch_version should succeed");
            assert_eq!(
                first_text(
                    &lix.execute("SELECT lix_active_version_id()", &[])
                        .await
                        .expect("active version query should succeed")
                ),
                version.id
            );
            version.id
        };

        let reopened = Lix::open(lix_config(&path))
            .await
            .expect("reopen should succeed");
        let active = reopened
            .execute("SELECT lix_active_version_id()", &[])
            .await
            .expect("active version query should succeed");
        assert_eq!(first_text(&active), version_id);

        drop(reopened);
        cleanup_sqlite_path(&path);
    });
}

#[test]
fn lix_forwards_workspace_session_selector_and_execute_apis() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("lix-workspace-forwards");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        Lix::init(lix_config(&path))
            .await
            .expect("init should succeed");
        let lix = Lix::open(lix_config(&path))
            .await
            .expect("open should succeed");

        let version = lix
            .create_version(CreateVersionOptions {
                name: Some("workspace-forwarded".to_string()),
                ..Default::default()
            })
            .await
            .expect("create_version should succeed");
        let version_id = version.id.clone();
        let accounts = vec!["acct-one".to_string(), "acct-two".to_string()];

        lix.switch_version(version_id.clone())
            .await
            .expect("switch_version should succeed");
        lix.set_active_account_ids(accounts.clone())
            .await
            .expect("set_active_account_ids should succeed");

        assert_eq!(
            lix.active_version_id()
                .await
                .expect("active_version_id should succeed"),
            version_id.clone()
        );
        assert_eq!(
            lix.active_account_ids()
                .await
                .expect("active_account_ids should succeed"),
            accounts
        );

        let version_query = lix
            .execute("SELECT lix_active_version_id()", &[])
            .await
            .expect("execute should succeed");
        let accounts_query = lix
            .execute_with_options(
                "SELECT lix_active_account_ids()",
                &[],
                ExecuteOptions::default(),
            )
            .await
            .expect("execute_with_options should succeed");

        assert_eq!(first_text(&version_query), version_id);
        assert_eq!(
            first_string_vec(&accounts_query),
            vec!["acct-one".to_string(), "acct-two".to_string()]
        );

        drop(lix);
        cleanup_sqlite_path(&path);
    });
}

#[test]
fn open_additional_session_snapshots_active_accounts_and_allows_explicit_overrides() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("active-accounts-snapshot");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        Lix::init(lix_config(&path))
            .await
            .expect("init should succeed");
        let lix = Lix::open(lix_config(&path))
            .await
            .expect("open should succeed");

        let seeded = lix
            .open_additional_session(AdditionalSessionOptions {
                active_account_ids: Some(vec!["acct-parent".to_string()]),
                ..Default::default()
            })
            .await
            .expect("seeded open_additional_session should succeed");
        let worker = seeded
            .open_additional_session(AdditionalSessionOptions::default())
            .await
            .expect("snapshot open_additional_session should succeed");
        let override_worker = seeded
            .open_additional_session(AdditionalSessionOptions {
                active_account_ids: Some(vec!["acct-override".to_string()]),
                ..Default::default()
            })
            .await
            .expect("open_additional_session override should succeed");

        let seeded_accounts = seeded
            .execute("SELECT lix_active_account_ids()", &[])
            .await
            .expect("seeded active account query should succeed");
        let worker_accounts = worker
            .execute("SELECT lix_active_account_ids()", &[])
            .await
            .expect("worker active account query should succeed");
        let override_accounts = override_worker
            .execute("SELECT lix_active_account_ids()", &[])
            .await
            .expect("override worker active account query should succeed");

        assert_eq!(
            first_string_vec(&seeded_accounts),
            vec!["acct-parent".to_string()]
        );
        assert_eq!(
            first_string_vec(&worker_accounts),
            vec!["acct-parent".to_string()]
        );
        assert_eq!(
            first_string_vec(&override_accounts),
            vec!["acct-override".to_string()]
        );

        drop(override_worker);
        drop(worker);
        drop(seeded);
        drop(lix);
        cleanup_sqlite_path(&path);
    });
}

#[test]
fn open_additional_session_inherits_workspace_session_defaults_without_overrides() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("additional-session-inherits");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        Lix::init(lix_config(&path))
            .await
            .expect("init should succeed");
        let lix = Lix::open(lix_config(&path))
            .await
            .expect("open should succeed");

        let version = lix
            .create_version(CreateVersionOptions {
                name: Some("workspace-defaults".to_string()),
                ..Default::default()
            })
            .await
            .expect("create_version should succeed");
        lix.switch_version(version.id.clone())
            .await
            .expect("switch_version should succeed");
        lix.set_active_account_ids(vec!["acct-root".to_string(), "acct-shadow".to_string()])
            .await
            .expect("set_active_account_ids should succeed");

        let session = lix
            .open_additional_session(AdditionalSessionOptions::default())
            .await
            .expect("open_additional_session should succeed");

        assert_eq!(session.active_version_id(), version.id);
        assert_eq!(
            session.active_account_ids(),
            vec!["acct-root".to_string(), "acct-shadow".to_string()]
        );

        drop(session);
        drop(lix);
        cleanup_sqlite_path(&path);
    });
}

#[test]
fn create_version_uses_the_calling_sessions_active_version_by_default() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("create-version-source");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        Lix::init(lix_config(&path))
            .await
            .expect("init should succeed");
        let lix = Lix::open(lix_config(&path))
            .await
            .expect("open should succeed");

        let workspace_version = lix
            .create_version(CreateVersionOptions {
                name: Some("workspace-base".to_string()),
                ..Default::default()
            })
            .await
            .expect("workspace create_version should succeed");
        lix.switch_version(workspace_version.id.clone())
            .await
            .expect("workspace switch_version should succeed");

        let worker = lix
            .open_additional_session(AdditionalSessionOptions::default())
            .await
            .expect("open_additional_session should succeed");
        worker
            .switch_version("global".to_string())
            .await
            .expect("worker switch_version should succeed");

        let worker_version = worker
            .create_version(CreateVersionOptions {
                name: Some("worker-child".to_string()),
                ..Default::default()
            })
            .await
            .expect("worker create_version should succeed");
        let workspace_child = lix
            .create_version(CreateVersionOptions {
                name: Some("workspace-child".to_string()),
                ..Default::default()
            })
            .await
            .expect("workspace create_version should succeed");

        assert_eq!(worker_version.parent_version_id, "global");
        assert_eq!(workspace_child.parent_version_id, workspace_version.id);

        drop(worker);
        drop(lix);
        cleanup_sqlite_path(&path);
    });
}

#[test]
fn create_checkpoint_uses_the_calling_sessions_active_version() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("checkpoint");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        Lix::init(lix_config(&path))
            .await
            .expect("init should succeed");
        let lix = Lix::open(lix_config(&path))
            .await
            .expect("open should succeed");

        let workspace_version = lix
            .create_version(CreateVersionOptions {
                name: Some("workspace-checkpoint".to_string()),
                ..Default::default()
            })
            .await
            .expect("create_version should succeed");
        lix.switch_version(workspace_version.id.clone())
            .await
            .expect("switch_version should succeed");
        lix.execute(
            "INSERT INTO lix_file (path, data) VALUES ('/workspace-checkpoint.txt', x'01')",
            &[],
        )
        .await
        .expect("workspace insert should succeed");
        let workspace_checkpoint = lix
            .create_checkpoint()
            .await
            .expect("workspace create_checkpoint should succeed");

        let worker = lix
            .open_additional_session(AdditionalSessionOptions::default())
            .await
            .expect("open_additional_session should succeed");
        let worker_version = worker
            .create_version(CreateVersionOptions {
                name: Some("worker-undo-redo".to_string()),
                ..Default::default()
            })
            .await
            .expect("worker create_version should succeed");
        worker
            .switch_version(worker_version.id.clone())
            .await
            .expect("worker switch_version should succeed");
        worker
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ('/global-checkpoint.txt', x'02')",
                &[],
            )
            .await
            .expect("worker insert should succeed");
        worker
            .create_checkpoint()
            .await
            .expect("worker create_checkpoint should succeed");

        let workspace_checkpoint_after = lix
            .execute(
                "SELECT checkpoint_commit_id \
                 FROM lix_internal_last_checkpoint \
                 WHERE version_id = $1 \
                 LIMIT 1",
                &[Value::Text(workspace_version.id.clone())],
            )
            .await
            .expect("workspace checkpoint query should succeed");

        assert_eq!(
            first_text(&workspace_checkpoint_after),
            workspace_checkpoint.id
        );

        drop(worker);
        drop(lix);
        cleanup_sqlite_path(&path);
    });
}

#[test]
fn undo_and_redo_default_to_the_calling_sessions_active_version() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("undo-redo");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        Lix::init(lix_config(&path))
            .await
            .expect("init should succeed");
        let lix = Lix::open(lix_config(&path))
            .await
            .expect("open should succeed");

        let workspace_version = lix
            .create_version(CreateVersionOptions {
                name: Some("workspace-undo-redo".to_string()),
                ..Default::default()
            })
            .await
            .expect("create_version should succeed");
        lix.switch_version(workspace_version.id.clone())
            .await
            .expect("switch_version should succeed");

        let worker = lix
            .open_additional_session(AdditionalSessionOptions::default())
            .await
            .expect("open_additional_session should succeed");
        let worker_version = worker
            .create_version(CreateVersionOptions {
                name: Some("worker-observe".to_string()),
                ..Default::default()
            })
            .await
            .expect("worker create_version should succeed");
        worker
            .switch_version(worker_version.id.clone())
            .await
            .expect("worker switch_version should succeed");

        worker
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ('/worker-only.txt', x'03')",
                &[],
            )
            .await
            .expect("worker insert should succeed");

        let worker_visible = worker
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_file_by_version \
                 WHERE path = '/worker-only.txt' \
                   AND lixcol_version_id = $1",
                &[Value::Text(worker_version.id.clone())],
            )
            .await
            .expect("worker count query should succeed");
        let workspace_visible = lix
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_file_by_version \
                 WHERE path = '/worker-only.txt' \
                   AND lixcol_version_id = $1",
                &[Value::Text(workspace_version.id.clone())],
            )
            .await
            .expect("workspace count query should succeed");
        assert_eq!(first_i64(&worker_visible), 1);
        assert_eq!(first_i64(&workspace_visible), 0);

        worker.undo().await.expect("worker undo should succeed");

        let worker_after_undo = worker
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_file_by_version \
                 WHERE path = '/worker-only.txt' \
                   AND lixcol_version_id = $1",
                &[Value::Text(worker_version.id.clone())],
            )
            .await
            .expect("worker count query should succeed");
        let workspace_after_undo = lix
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_file_by_version \
                 WHERE path = '/worker-only.txt' \
                   AND lixcol_version_id = $1",
                &[Value::Text(workspace_version.id.clone())],
            )
            .await
            .expect("workspace count query should succeed");
        assert_eq!(first_i64(&worker_after_undo), 0);
        assert_eq!(first_i64(&workspace_after_undo), 0);

        worker.redo().await.expect("worker redo should succeed");

        let worker_after_redo = worker
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_file_by_version \
                 WHERE path = '/worker-only.txt' \
                   AND lixcol_version_id = $1",
                &[Value::Text(worker_version.id.clone())],
            )
            .await
            .expect("worker count query should succeed");
        let workspace_after_redo = lix
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_file_by_version \
                 WHERE path = '/worker-only.txt' \
                   AND lixcol_version_id = $1",
                &[Value::Text(workspace_version.id.clone())],
            )
            .await
            .expect("workspace count query should succeed");
        assert_eq!(first_i64(&worker_after_redo), 1);
        assert_eq!(first_i64(&workspace_after_redo), 0);

        drop(worker);
        drop(lix);
        cleanup_sqlite_path(&path);
    });
}

#[test]
fn observe_initial_snapshot_is_session_scoped() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("observe");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        Lix::init(lix_config(&path))
            .await
            .expect("init should succeed");
        let lix = Lix::open(lix_config(&path))
            .await
            .expect("open should succeed");

        let workspace_version = lix
            .create_version(CreateVersionOptions {
                name: Some("workspace-observe".to_string()),
                ..Default::default()
            })
            .await
            .expect("create_version should succeed");
        lix.switch_version(workspace_version.id.clone())
            .await
            .expect("switch_version should succeed");

        let worker = lix
            .open_additional_session(AdditionalSessionOptions::default())
            .await
            .expect("open_additional_session should succeed");
        let worker_version = worker
            .create_version(CreateVersionOptions {
                name: Some("worker-observe".to_string()),
                ..Default::default()
            })
            .await
            .expect("worker create_version should succeed");
        worker
            .switch_version(worker_version.id.clone())
            .await
            .expect("worker switch_version should succeed");
        worker
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ('/observe-session.txt', x'04')",
                &[],
            )
            .await
            .expect("worker insert should succeed");

        let query = ObserveQuery::new(
            "SELECT COUNT(*) \
             FROM lix_file_by_version \
             WHERE path = '/observe-session.txt' \
               AND lixcol_version_id = lix_active_version_id()",
            Vec::new(),
        );
        let worker_direct = worker
            .execute(&query.sql, &[])
            .await
            .expect("worker direct query should succeed");
        let workspace_direct = lix
            .execute(&query.sql, &[])
            .await
            .expect("workspace direct query should succeed");
        assert_eq!(first_i64(&worker_direct), 1);
        assert_eq!(first_i64(&workspace_direct), 0);

        let mut worker_observed = worker
            .observe(query.clone())
            .expect("worker observe should succeed");
        let mut workspace_observed = lix
            .observe(query)
            .expect("workspace observe should succeed");

        let worker_observed_event =
            tokio::time::timeout(std::time::Duration::from_secs(1), worker_observed.next())
                .await
                .expect("worker_observed next should not time out")
                .expect("worker_observed next should succeed")
                .expect("worker_observed should emit an event");
        assert_eq!(first_i64_query_result(&worker_observed_event.rows), 1);
        assert_eq!(
            next_observe_count(&mut workspace_observed, "workspace_observed").await,
            0
        );

        worker_observed.close();
        workspace_observed.close();
        drop(worker_observed);
        drop(workspace_observed);
        drop(worker);
        drop(lix);
        cleanup_sqlite_path(&path);
    });
}

#[test]
fn extra_session_switch_version_refreshes_only_its_own_observes() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("observe-worker-switch-version");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        let engine = boot_engine(&path);
        engine.initialize().await.expect("init should succeed");

        let workspace = Arc::clone(&engine);
        let worker = workspace
            .open_additional_session(AdditionalSessionOptions::default())
            .await
            .expect("open_additional_session should succeed");
        let worker_version = worker
            .create_version(CreateVersionOptions {
                name: Some("worker-observe-switch".to_string()),
                ..Default::default()
            })
            .await
            .expect("worker create_version should succeed");

        let query = ObserveQuery::new("SELECT lix_active_version_id()", Vec::new());
        let mut worker_observed = worker
            .observe(query.clone())
            .expect("worker observe should succeed");
        let mut workspace_observed = workspace
            .observe(query)
            .expect("workspace observe should succeed");

        let initial_version_id = workspace
            .active_version_id()
            .await
            .expect("workspace active_version_id should load");
        assert_eq!(
            next_session_observe_text(&mut worker_observed, "worker_observed_initial").await,
            initial_version_id.clone()
        );
        assert_eq!(
            next_owned_observe_text(&mut workspace_observed, "workspace_observed_initial").await,
            initial_version_id
        );

        worker
            .switch_version(worker_version.id.clone())
            .await
            .expect("worker switch_version should succeed");

        assert_eq!(
            next_session_observe_text(&mut worker_observed, "worker_observed_after_switch").await,
            worker_version.id
        );
        assert_no_owned_observe_event(
            &mut workspace_observed,
            "workspace_observed_after_worker_switch",
        )
        .await;

        worker_observed.close();
        workspace_observed.close();
        drop(engine);
        cleanup_sqlite_path(&path);
    });
}

#[test]
fn extra_session_active_account_changes_refresh_only_its_own_observes() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("observe-worker-active-accounts");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        let engine = boot_engine(&path);
        engine.initialize().await.expect("init should succeed");

        let workspace = Arc::clone(&engine);
        let worker = workspace
            .open_additional_session(AdditionalSessionOptions::default())
            .await
            .expect("open_additional_session should succeed");

        let query = ObserveQuery::new("SELECT lix_active_account_ids()", Vec::new());
        let mut worker_observed = worker
            .observe(query.clone())
            .expect("worker observe should succeed");
        let mut workspace_observed = workspace
            .observe(query)
            .expect("workspace observe should succeed");

        assert_eq!(
            next_session_observe_string_vec(&mut worker_observed, "worker_accounts_initial").await,
            Vec::<String>::new()
        );
        assert_eq!(
            next_owned_observe_string_vec(&mut workspace_observed, "workspace_accounts_initial",)
                .await,
            Vec::<String>::new()
        );

        worker
            .set_active_account_ids(vec!["acct-worker".to_string(), "acct-shadow".to_string()])
            .await
            .expect("worker set_active_account_ids should succeed");

        assert_eq!(
            next_session_observe_string_vec(&mut worker_observed, "worker_accounts_after_set")
                .await,
            vec!["acct-worker".to_string(), "acct-shadow".to_string()]
        );
        assert_no_owned_observe_event(
            &mut workspace_observed,
            "workspace_accounts_after_worker_set",
        )
        .await;

        worker_observed.close();
        workspace_observed.close();
        drop(engine);
        cleanup_sqlite_path(&path);
    });
}

#[test]
fn workspace_reopen_restores_runtime_state_from_workspace_metadata() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("workspace-metadata-reopen");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        let version_id = {
            let engine = boot_engine(&path);
            engine.initialize().await.expect("init should succeed");
            let workspace = Arc::clone(&engine);
            let version = workspace
                .create_version(CreateVersionOptions {
                    name: Some("workspace-metadata-reopen".to_string()),
                    ..Default::default()
                })
                .await
                .expect("create_version should succeed");
            workspace
                .switch_version(version.id.clone())
                .await
                .expect("switch_version should succeed");
            workspace
                .set_active_account_ids(vec!["acct-persisted".to_string()])
                .await
                .expect("set_active_account_ids should succeed");

            assert_eq!(
                workspace_metadata_value_lix(workspace.as_ref(), "active_version_id").await,
                Some(version.id.clone())
            );
            assert_eq!(
                workspace_metadata_value_lix(workspace.as_ref(), "active_account_ids").await,
                Some(r#"["acct-persisted"]"#.to_string())
            );

            version.id
        };

        let reopened_engine = boot_engine(&path);
        reopened_engine
            .initialize_if_needed()
            .await
            .expect("reopen initialize_if_needed should succeed");
        let reopened = Arc::clone(&reopened_engine);

        let reopened_active_version_id = reopened
            .active_version_id()
            .await
            .expect("reopened active_version_id should load");
        assert_eq!(reopened_active_version_id, version_id);
        assert_eq!(
            reopened
                .active_account_ids()
                .await
                .expect("reopened active_account_ids should load"),
            vec!["acct-persisted".to_string()]
        );
        assert_eq!(
            workspace_metadata_value_lix(reopened.as_ref(), "active_version_id").await,
            Some(
                reopened
                    .active_version_id()
                    .await
                    .expect("reopened active_version_id should load again"),
            )
        );
        assert_eq!(
            workspace_metadata_value_lix(reopened.as_ref(), "active_account_ids").await,
            Some(r#"["acct-persisted"]"#.to_string())
        );

        drop(reopened);
        drop(reopened_engine);
        cleanup_sqlite_path(&path);
    });
}

#[test]
fn tracked_writes_use_the_calling_sessions_active_accounts() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("active-account-authors");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        Lix::init(lix_config(&path))
            .await
            .expect("init should succeed");
        let lix = Lix::open(lix_config(&path))
            .await
            .expect("open should succeed");

        let worker = lix
            .open_additional_session(AdditionalSessionOptions {
                active_account_ids: Some(vec!["acct-session".to_string()]),
                ..Default::default()
            })
            .await
            .expect("open_additional_session override should succeed");

        worker
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('session-account-author', 'ok')",
                &[],
            )
            .await
            .expect("tracked write should succeed");

        let authors = lix
            .execute(
                "SELECT account_id FROM lix_change_author ORDER BY account_id",
                &[],
            )
            .await
            .expect("change author query should succeed");
        let author_ids = authors.statements[0]
            .rows
            .iter()
            .map(|row| match &row[0] {
                Value::Text(value) => value.clone(),
                other => panic!("expected author account id text, got {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(author_ids, vec!["acct-session".to_string()]);

        drop(worker);
        drop(lix);
        cleanup_sqlite_path(&path);
    });
}
