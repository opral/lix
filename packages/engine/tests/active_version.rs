mod support;

use lix_engine::{BootKeyValue, Value};
use support::simulation_test::{
    default_simulations, run_simulation_test, SimulationArgs, SimulationBootArgs,
};

fn parse_snapshot(value: &Value) -> serde_json::Value {
    let text = match value {
        Value::Text(value) => value,
        other => panic!("expected text snapshot_content, got {other:?}"),
    };
    serde_json::from_str(text).expect("snapshot_content should be valid JSON")
}

async fn read_active_version_value(engine: &support::simulation_test::SimulationEngine) -> String {
    let row = engine
        .execute(
            "SELECT snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_active_version' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND untracked = 1 \
             ORDER BY updated_at DESC \
             LIMIT 1",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(row.rows.len(), 1);
    let snapshot = parse_snapshot(&row.rows[0][0]);
    snapshot["version_id"]
        .as_str()
        .expect("active version value should be a string")
        .to_string()
}

async fn read_active_version_view_row(
    engine: &support::simulation_test::SimulationEngine,
) -> (String, String) {
    let result = engine
        .execute("SELECT id, version_id FROM lix_active_version", &[])
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let id = match &result.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected text id, got {other:?}"),
    };
    let version_id = match &result.rows[0][1] {
        Value::Text(value) => value.clone(),
        other => panic!("expected text version_id, got {other:?}"),
    };
    (id, version_id)
}

fn deterministic_boot_args() -> SimulationBootArgs {
    SimulationBootArgs {
        key_values: vec![BootKeyValue {
            key: "lix_deterministic_mode".to_string(),
            value: serde_json::json!({ "enabled": true }),
            version_id: None,
        }],
    }
}

async fn run_init_seeds_default_active_version_deterministic(sim: SimulationArgs) {
    let engine = sim
        .boot_simulated_engine(Some(deterministic_boot_args()))
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    let row = engine
        .execute(
            "SELECT entity_id, snapshot_content, untracked \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_active_version' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND untracked = 1 \
             ORDER BY updated_at DESC \
             LIMIT 1",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(row.rows.len(), 1);
    let entity_id = match &row.rows[0][0] {
        Value::Text(value) => value,
        other => panic!("expected text entity_id, got {other:?}"),
    };
    let snapshot = parse_snapshot(&row.rows[0][1]);
    assert_eq!(snapshot["id"], entity_id.as_str());
    let active_version_id = snapshot["version_id"]
        .as_str()
        .expect("active version snapshot should include string version_id");
    assert!(!active_version_id.is_empty());
    assert_eq!(row.rows[0][2], Value::Integer(1));

    sim.assert_deterministic(entity_id.to_string());
    sim.assert_deterministic(active_version_id.to_string());

    let version = engine
        .execute(
            "SELECT name FROM lix_version WHERE id = $1",
            &[Value::Text(active_version_id.to_string())],
        )
        .await
        .unwrap();
    assert_eq!(version.rows.len(), 1);
    assert_eq!(version.rows[0][0], Value::Text("main".to_string()));
}

#[tokio::test]
async fn init_seeds_default_active_version_is_deterministic_across_backends() {
    run_simulation_test(
        default_simulations(),
        run_init_seeds_default_active_version_deterministic,
    )
    .await;
}

simulation_test!(init_seeds_default_active_version, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    let row = engine
        .execute(
            "SELECT entity_id, snapshot_content, untracked \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_active_version' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND untracked = 1 \
             ORDER BY updated_at DESC \
             LIMIT 1",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(row.rows.len(), 1);
    let entity_id = match &row.rows[0][0] {
        Value::Text(value) => value,
        other => panic!("expected text entity_id, got {other:?}"),
    };
    let snapshot = parse_snapshot(&row.rows[0][1]);
    assert_eq!(snapshot["id"], entity_id.as_str());
    let active_version_id = snapshot["version_id"]
        .as_str()
        .expect("active version snapshot should include string version_id");
    assert!(!active_version_id.is_empty());
    assert_eq!(row.rows[0][2], Value::Integer(1));

    let version = engine
        .execute(
            "SELECT name FROM lix_version WHERE id = $1",
            &[Value::Text(active_version_id.to_string())],
        )
        .await
        .unwrap();
    assert_eq!(version.rows.len(), 1);
    assert_eq!(version.rows[0][0], Value::Text("main".to_string()));
});

simulation_test!(
    active_version_view_select_reads_seeded_row,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let (id, version_id) = read_active_version_view_row(&engine).await;
        assert!(!id.is_empty());
        assert!(!version_id.is_empty());
    }
);

simulation_test!(
    active_version_view_update_switches_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let (before_id, _) = read_active_version_view_row(&engine).await;

        engine
            .execute(
                "UPDATE lix_active_version SET version_id = $1",
                &[Value::Text("global".to_string())],
            )
            .await
            .unwrap();

        let (after_id, after_version_id) = read_active_version_view_row(&engine).await;
        assert_eq!(after_id, before_id);
        assert_eq!(after_version_id, "global");
    }
);

simulation_test!(
    active_version_view_update_rejects_missing_version_fk,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let error = engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version_nonexistent'",
                &[],
            )
            .await
            .expect_err("missing version_id should violate active version FK");
        assert!(
            error.message.contains("Foreign key constraint violation"),
            "unexpected error message: {}",
            error.message
        );
    }
);

simulation_test!(
    active_version_view_update_allows_existing_version_fk,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute("UPDATE lix_active_version SET version_id = 'global'", &[])
            .await
            .unwrap();

        let (_, version_id) = read_active_version_view_row(&engine).await;
        assert_eq!(version_id, "global");
    }
);

simulation_test!(
    active_version_can_be_switched_via_vtable_update,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();
        engine
            .execute(
                "UPDATE lix_internal_state_vtable \
                 SET snapshot_content = '{\"id\":\"preserved\",\"version_id\":\"version-a\"}' \
                 WHERE untracked = 1 \
                   AND schema_key = 'lix_active_version' \
                   AND file_id = 'lix' \
                   AND version_id = 'global'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(read_active_version_value(&engine).await, "version-a");
    }
);

simulation_test!(
    active_version_can_be_updated_multiple_times_via_vtable_update,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
            .execute(
                "UPDATE lix_internal_state_vtable \
                 SET snapshot_content = '{\"id\":\"preserved\",\"version_id\":\"version-b\"}' \
                 WHERE untracked = 1 \
                   AND schema_key = 'lix_active_version' \
                   AND file_id = 'lix' \
                   AND version_id = 'global'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(read_active_version_value(&engine).await, "version-b");
    }
);
