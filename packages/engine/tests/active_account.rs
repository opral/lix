mod support;

use lix_engine::{BootAccount, Value};
use support::simulation_test::SimulationBootArgs;

fn first_text(result: &lix_engine::ExecuteResult) -> String {
    match &result.statements[0].rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected first result cell to be text, got {other:?}"),
    }
}

fn first_string_vec(result: &lix_engine::ExecuteResult) -> Vec<String> {
    serde_json::from_str(&first_text(result))
        .unwrap_or_else(|error| panic!("expected JSON array text, got parse error: {error}"))
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
        .map(|value| match value {
            Value::Text(value) => value.clone(),
            other => panic!("expected text metadata value, got {other:?}"),
        })
}

simulation_test!(
    active_account_ids_function_reads_boot_account,
    simulations = [sqlite, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(SimulationBootArgs {
                active_account: Some(BootAccount {
                    id: "acct-boot".to_string(),
                    name: "Boot Account".to_string(),
                }),
                ..SimulationBootArgs::default()
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        let result = engine
            .execute("SELECT lix_active_account_ids()", &[])
            .await
            .expect("active account ids query should succeed");
        assert_eq!(first_string_vec(&result), vec!["acct-boot".to_string()]);
    }
);

simulation_test!(
    set_active_account_ids_updates_runtime_function_and_workspace_metadata,
    simulations = [sqlite, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .set_active_account_ids(vec!["acct-first".to_string(), "acct-second".to_string()])
            .await
            .expect("set_active_account_ids should succeed");

        let result = engine
            .execute("SELECT lix_active_account_ids()", &[])
            .await
            .expect("active account ids query should succeed");
        assert_eq!(
            first_string_vec(&result),
            vec!["acct-first".to_string(), "acct-second".to_string()]
        );

        let persisted = workspace_metadata_value(&engine, "active_account_ids")
            .await
            .expect("workspace metadata should persist active account ids");
        assert_eq!(persisted, r#"["acct-first","acct-second"]"#);
    }
);

simulation_test!(
    set_active_account_ids_can_clear_selection,
    simulations = [sqlite, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(SimulationBootArgs {
                active_account: Some(BootAccount {
                    id: "acct-clear".to_string(),
                    name: "Clear Me".to_string(),
                }),
                ..SimulationBootArgs::default()
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .set_active_account_ids(Vec::new())
            .await
            .expect("clearing active account ids should succeed");

        let result = engine
            .execute("SELECT lix_active_account_ids()", &[])
            .await
            .expect("active account ids query should succeed");
        assert_eq!(first_string_vec(&result), Vec::<String>::new());

        let persisted = workspace_metadata_value(&engine, "active_account_ids")
            .await
            .expect("workspace metadata should persist cleared active account ids");
        assert_eq!(persisted, "[]");
    }
);

simulation_test!(
    active_account_surface_is_not_publicly_queryable,
    simulations = [sqlite, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        let error = engine
            .execute("SELECT account_id FROM lix_active_account", &[])
            .await
            .expect_err("removed active account surface should not be queryable");
        assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_TABLE");
        assert!(error.description.contains("lix_active_account"));
    }
);
