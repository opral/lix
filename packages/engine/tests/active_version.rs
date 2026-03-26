mod support;

use lix_engine::{CreateVersionOptions, Value};
use support::simulation_test::SimulationArgs;

fn first_text(result: &lix_engine::ExecuteResult) -> String {
    match &result.statements[0].rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected first result cell to be text, got {other:?}"),
    }
}

async fn workspace_metadata_value(
    engine: &support::simulation_test::SimulationEngine,
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

fn first_text_value(value: &Value) -> String {
    match value {
        Value::Text(value) => value.clone(),
        other => panic!("expected text value, got {other:?}"),
    }
}

async fn insert_version(engine: &support::simulation_test::SimulationEngine, version_id: &str) {
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

async fn run_init_seeds_default_active_version_deterministic(sim: SimulationArgs) {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");

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
    switch_version_updates_runtime_function_and_workspace_metadata,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
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

simulation_test!(switch_version_rejects_missing_version, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.expect("init should succeed");

    let error = engine
        .switch_version("version-nonexistent".to_string())
        .await
        .expect_err("missing version id should fail");
    assert!(error.description.contains("does not exist"));
});

simulation_test!(
    active_version_surface_is_not_publicly_queryable,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        let error = engine
            .execute("SELECT version_id FROM lix_active_version", &[])
            .await
            .expect_err("removed active version surface should not be queryable");
        assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_TABLE");
        assert!(error.description.contains("lix_active_version"));
    }
);
