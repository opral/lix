use crate::support;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use lix_engine::wasm::NoopWasmRuntime;
use lix_engine::{AdditionalSessionOptions, CreateVersionOptions, Lix, LixConfig, Value};
use support::simulation_test::SimulationArgs;

fn run_with_large_stack<F, Fut>(factory: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name("active-version".to_string())
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime should build")
                .block_on(Box::pin(factory()));
        })
        .expect("active version thread should spawn")
        .join()
        .expect("active version thread should not panic");
}

fn temp_sqlite_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lix-active-version-{label}-{}-{nanos}.sqlite",
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

fn first_text(result: &lix_engine::ExecuteResult) -> String {
    match &result.statements[0].rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected first result cell to be text, got {other:?}"),
    }
}

async fn workspace_metadata_value(
    engine: &support::simulation_test::SimulatedLix,
    key: &str,
) -> Option<String> {
    let result = engine
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
        .map(first_text_value)
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
        .map(first_text_value)
}

fn first_text_value(value: &Value) -> String {
    match value {
        Value::Text(value) => value.clone(),
        other => panic!("expected text value, got {other:?}"),
    }
}

fn first_bool_value(value: &Value) -> bool {
    match value {
        Value::Boolean(value) => *value,
        Value::Integer(value) => *value != 0,
        Value::Text(value) => matches!(value.as_str(), "1" | "true" | "TRUE"),
        other => panic!("expected boolean-compatible value, got {other:?}"),
    }
}

async fn insert_version(engine: &support::simulation_test::SimulatedLix, version_id: &str) {
    engine
        .create_version(CreateVersionOptions {
            id: Some(version_id.to_string()),
            name: Some(version_id.to_string()),
            source_version_id: None,
            hidden: false,
        })
        .await
        .expect("create_version should succeed");
}

async fn active_version_commit_id(engine: &support::simulation_test::SimulatedLix) -> String {
    let active = engine
        .execute(
            "SELECT commit_id \
             FROM lix_version \
             WHERE id = lix_active_version_id() \
             LIMIT 1",
            &[],
        )
        .await
        .expect("active version commit query should succeed");
    assert_eq!(active.statements[0].rows.len(), 1);
    first_text_value(&active.statements[0].rows[0][0])
}

async fn run_init_seeds_default_active_version_deterministic(sim: SimulationArgs) {
    let engine = sim
        .boot_simulated_lix_deterministic()
        .await
        .expect("boot_simulated_lix_deterministic should succeed");

    engine.initialize().await.expect("init should succeed");

    let active_version_id = first_text(
        &engine
            .execute("SELECT lix_active_version_id()", &[])
            .await
            .expect("active version id query should succeed"),
    );
    assert!(!active_version_id.is_empty());
    sim.assert_deterministic(active_version_id.clone());

    let version_name = first_text(
        &engine
            .execute(
                "SELECT name FROM lix_version WHERE id = $1 LIMIT 1",
                &[Value::Text(active_version_id.clone())],
            )
            .await
            .expect("version name query should succeed"),
    );
    assert_eq!(version_name, "main");

    let persisted = workspace_metadata_value(&engine, "active_version_id")
        .await
        .expect("workspace metadata should persist active version id");
    assert_eq!(persisted, active_version_id);
    sim.assert_deterministic(persisted);
}

simulation_test!(
    init_seeds_default_active_version_is_deterministic_across_backends,
    simulations = [sqlite, postgres, materialization],
    |sim| async move {
        run_init_seeds_default_active_version_deterministic(sim).await;
    }
);

simulation_test!(
    initialize_bootstrap_rows_are_backed_by_lix_change_rows,
    simulations = [sqlite, postgres, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let visible_bootstrap_rows = engine
            .execute(
                "SELECT schema_key, entity_id, change_id \
                 FROM lix_state_by_version \
                 WHERE untracked = true \
                   AND schema_key IN ('lix_version_ref', 'lix_registered_schema') \
                 ORDER BY schema_key, entity_id",
                &[],
            )
            .await
            .expect("bootstrap visible state query should succeed");
        assert!(
            !visible_bootstrap_rows.statements[0].rows.is_empty(),
            "expected bootstrap rows to be visible"
        );

        for row in &visible_bootstrap_rows.statements[0].rows {
            let schema_key = first_text_value(&row[0]);
            let entity_id = first_text_value(&row[1]);
            let change_id = first_text_value(&row[2]);
            assert!(
                !change_id.is_empty(),
                "bootstrap row {schema_key}/{entity_id} must expose a real change_id"
            );

            let backing_change = engine
                .execute(
                    "SELECT schema_key, entity_id, untracked \
                     FROM lix_change \
                     WHERE id = $1",
                    &[Value::Text(change_id.clone())],
                )
                .await
                .expect("backing lix_change query should succeed");
            assert_eq!(
                backing_change.statements[0].rows.len(),
                1,
                "expected exactly one backing change row for {schema_key}/{entity_id}"
            );
            assert_eq!(
                first_text_value(&backing_change.statements[0].rows[0][0]),
                schema_key
            );
            assert_eq!(
                first_text_value(&backing_change.statements[0].rows[0][1]),
                entity_id
            );
            assert!(
                first_bool_value(&backing_change.statements[0].rows[0][2]),
                "bootstrap row {schema_key}/{entity_id} should be backed by an untracked change"
            );
        }
    }
);

simulation_test!(
    switch_version_updates_runtime_function_and_workspace_metadata,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");
        insert_version(&engine, "version-switch-target").await;

        engine
            .switch_version("version-switch-target".to_string())
            .await
            .expect("switch_version should succeed");

        let active_version_id = first_text(
            &engine
                .execute("SELECT lix_active_version_id()", &[])
                .await
                .expect("active version id query should succeed"),
        );
        assert_eq!(active_version_id, "version-switch-target");

        let persisted = workspace_metadata_value(&engine, "active_version_id")
            .await
            .expect("workspace metadata should persist active version id");
        assert_eq!(persisted, "version-switch-target");
    }
);

simulation_test!(
    switch_version_preserves_runtime_metadata_across_multiple_hops,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let start_version_id = first_text(
            &engine
                .execute("SELECT lix_active_version_id()", &[])
                .await
                .expect("starting active version query should succeed"),
        );
        insert_version(&engine, "version-hop-alpha").await;
        insert_version(&engine, "version-hop-beta").await;

        for expected in [
            "version-hop-alpha",
            "version-hop-beta",
            start_version_id.as_str(),
        ] {
            engine
                .switch_version(expected.to_string())
                .await
                .expect("switch_version should succeed");

            let active_version_id = first_text(
                &engine
                    .execute("SELECT lix_active_version_id()", &[])
                    .await
                    .expect("active version query should succeed"),
            );
            assert_eq!(active_version_id, expected);

            let persisted = workspace_metadata_value(&engine, "active_version_id")
                .await
                .expect("workspace metadata should persist active version id");
            assert_eq!(persisted, expected);
        }
    }
);

simulation_test!(switch_version_rejects_invalid_inputs, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
    engine.initialize().await.expect("init should succeed");

    let empty = engine
        .switch_version("".to_string())
        .await
        .expect_err("empty version id should fail");
    assert!(empty.description.contains("non-empty"));

    let missing = engine
        .switch_version("missing-version-id".to_string())
        .await
        .expect_err("unknown version id should fail");
    assert!(missing.description.contains("does not exist"));
});

simulation_test!(
    tracked_write_moves_active_commit_id_off_global,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        let active_before = engine
            .execute(
                "SELECT lix_active_version_id(), commit_id \
                 FROM lix_version \
                 WHERE id = lix_active_version_id() \
                 LIMIT 1",
                &[],
            )
            .await
            .expect("active version query should succeed");
        assert_eq!(active_before.statements[0].rows.len(), 1);
        let active_version_id = first_text_value(&active_before.statements[0].rows[0][0]);
        assert_ne!(active_version_id, "global");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('working-pointer-regression', '1')",
                &[],
            )
            .await
            .expect("tracked write should succeed");

        let active_after = engine
            .execute(
                "SELECT lix_active_version_id(), commit_id \
                 FROM lix_version \
                 WHERE id = lix_active_version_id() \
                 LIMIT 1",
                &[],
            )
            .await
            .expect("active version query should succeed");
        assert_eq!(active_after.statements[0].rows.len(), 1);
        assert_eq!(
            first_text_value(&active_after.statements[0].rows[0][0]),
            active_version_id
        );
        assert_ne!(
            first_text_value(&active_after.statements[0].rows[0][1]),
            "global"
        );
    }
);

simulation_test!(
    content_only_update_moves_active_commit_pointer,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('version-api-content-only', '/version-api-content-only.md', lix_text_encode('before'))",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        let before_commit = active_version_commit_id(&engine).await;

        engine
            .execute(
                "UPDATE lix_file SET data = lix_text_encode('after') \
                 WHERE id = 'version-api-content-only'",
                &[],
            )
            .await
            .expect("content-only update should succeed");

        let after_commit = active_version_commit_id(&engine).await;
        assert_ne!(after_commit, before_commit);
    }
);

simulation_test!(
    active_version_surface_is_not_publicly_queryable,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let error = engine
            .execute("SELECT version_id FROM lix_active_version", &[])
            .await
            .expect_err("removed active version surface should not be queryable");
        assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_TABLE");
        assert!(error.description.contains("lix_active_version"));
    }
);

#[test]
fn additional_session_switch_does_not_mutate_workspace_active_version() {
    run_with_large_stack(|| async move {
        let path = temp_sqlite_path("additional-session-switch-isolation");
        let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");

        Lix::init(lix_config(&path))
            .await
            .expect("init should succeed");
        let lix = Lix::open(lix_config(&path))
            .await
            .expect("open should succeed");

        let version = lix
            .create_version(CreateVersionOptions {
                name: Some("workspace-active".to_string()),
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
        assert_eq!(
            workspace_metadata_value_lix(&lix, "active_version_id").await,
            Some(version.id.clone())
        );

        drop(worker);
        drop(lix);
        cleanup_sqlite_path(&path);
    });
}
