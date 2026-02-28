mod support;

use lix_engine::Value;
use support::simulation_test::SimulationArgs;

fn assert_true_like(value: &Value) {
    match value {
        Value::Boolean(value) => assert!(*value),
        Value::Integer(value) => assert_eq!(*value, 1),
        Value::Text(value) => assert!(matches!(value.as_str(), "true" | "TRUE" | "1")),
        other => panic!("expected boolean-like true value, got {other:?}"),
    }
}

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
               AND untracked = true \
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

async fn run_init_seeds_default_active_version_deterministic(sim: SimulationArgs) {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");

    engine.init().await.unwrap();

    let row = engine
        .execute(
            "SELECT entity_id, snapshot_content, untracked \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_active_version' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND untracked = true \
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
    assert_true_like(&row.rows[0][2]);

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

simulation_test!(
    init_seeds_default_active_version_is_deterministic_across_backends,
    simulations = [sqlite, postgres, materialization],
    |sim| async move {
        run_init_seeds_default_active_version_deterministic(sim).await;
    }
);

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
               AND untracked = true \
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
    assert_true_like(&row.rows[0][2]);

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
            error.description.contains("Foreign key constraint violation"),
            "unexpected error message: {}",
            error.description
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
        let (active_id, _) = read_active_version_view_row(&engine).await;
        engine
            .execute(
                "UPDATE lix_internal_state_vtable \
                 SET snapshot_content = $1 \
                 WHERE untracked = true \
                   AND schema_key = 'lix_active_version' \
                   AND file_id = 'lix' \
                   AND version_id = 'global'",
                &[Value::Text(
                    serde_json::json!({
                        "id": active_id,
                        "version_id": "version-a"
                    })
                    .to_string(),
                )],
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
        let (active_id, _) = read_active_version_view_row(&engine).await;

        engine
            .execute(
                "UPDATE lix_internal_state_vtable \
                 SET snapshot_content = $1 \
                 WHERE untracked = true \
                   AND schema_key = 'lix_active_version' \
                   AND file_id = 'lix' \
                   AND version_id = 'global'",
                &[Value::Text(
                    serde_json::json!({
                        "id": active_id,
                        "version_id": "version-b"
                    })
                    .to_string(),
                )],
            )
            .await
            .unwrap();

        assert_eq!(read_active_version_value(&engine).await, "version-b");
    }
);
