use lix_engine::{BootAccount, BootKeyValue, Value};
use serde_json::json;

mod support;

use support::simulation_test::SimulationBootArgs;

simulation_test!(
    fresh_engine_initialization_populates_subsystem_owned_bootstrap_state,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        let commit_graph_count = scalar_count(
            &engine
                .execute("SELECT COUNT(*) FROM lix_internal_commit_graph_node", &[])
                .await
                .expect("commit graph query should succeed"),
        );
        assert!(
            commit_graph_count >= 1,
            "expected bootstrap commit graph rows, got {commit_graph_count}"
        );

        let checkpoint_count = scalar_count(
            &engine
                .execute("SELECT COUNT(*) FROM lix_internal_last_checkpoint", &[])
                .await
                .expect("last checkpoint query should succeed"),
        );
        assert!(
            checkpoint_count >= 1,
            "expected checkpoint rows after init, got {checkpoint_count}"
        );

        let builtin_schema_count = scalar_count(
            &engine
                .execute(
                    "SELECT COUNT(*) \
                     FROM lix_registered_schema",
                    &[],
                )
                .await
                .expect("registered schema query should succeed"),
        );
        assert!(
            builtin_schema_count >= 1,
            "expected builtin registered schema rows, got {builtin_schema_count}"
        );

        let checkpoint_label = engine
            .execute(
                "SELECT s.content \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.entity_id = 'lix_label_checkpoint' \
                   AND c.schema_key = 'lix_label' \
                 ORDER BY c.created_at DESC \
                 LIMIT 1",
                &[],
            )
            .await
            .expect("checkpoint label query should succeed");
        assert_eq!(
            first_result_rows(&checkpoint_label).as_slice(),
            vec![vec![Value::Text(
                "{\"id\":\"lix_label_checkpoint\",\"name\":\"checkpoint\"}".to_string(),
            )]]
        );
    }
);

simulation_test!(
    initialize_if_needed_preserves_subsystem_seed_rows,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(SimulationBootArgs {
                key_values: vec![BootKeyValue {
                    key: "plan3_boot_key".to_string(),
                    value: json!("seeded"),
                    lixcol_global: Some(true),
                    lixcol_untracked: Some(false),
                }],
                active_account: Some(BootAccount {
                    id: "plan3-account".to_string(),
                    name: "Plan 3".to_string(),
                }),
                ..SimulationBootArgs::default()
            }))
            .await
            .expect("boot_simulated_engine should succeed");

        let initialized = engine
            .initialize_if_needed()
            .await
            .expect("initialize_if_needed should initialize fresh engine");
        assert!(initialized, "fresh engine should initialize");

        let initialized_again = engine
            .initialize_if_needed()
            .await
            .expect("initialize_if_needed should be idempotent at engine level");
        assert!(
            !initialized_again,
            "initialized engine should not bootstrap a second time"
        );

        let boot_key = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = $1 \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL",
                &[Value::Text("plan3_boot_key".to_string())],
            )
            .await
            .expect("boot key query should succeed");
        assert_eq!(
            first_result_rows(&boot_key).len(),
            1,
            "boot key seed row should persist"
        );

        let account = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_account' \
                   AND entity_id = $1 \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL",
                &[Value::Text("plan3-account".to_string())],
            )
            .await
            .expect("boot account query should succeed");
        assert_eq!(
            first_result_rows(&account).len(),
            1,
            "boot account seed row should persist"
        );
    }
);

fn scalar_count(result: &lix_engine::ExecuteResult) -> i64 {
    match first_result_rows(result)
        .first()
        .and_then(|row| row.first())
    {
        Some(Value::Integer(value)) => *value,
        other => panic!("expected integer count row, got {other:?}"),
    }
}

fn first_result_rows(result: &lix_engine::ExecuteResult) -> &Vec<Vec<Value>> {
    &result
        .statements
        .first()
        .expect("expected one query result")
        .rows
}
